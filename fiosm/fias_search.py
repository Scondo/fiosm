#!/usr/bin/python
# -*- coding: UTF-8 -*-

import melt
import mangledb
mangledb.InitMangle(False)
from config import *
way_only = frozenset((u'улица', u'проезд', u'проспект', u'переулок', u'шоссе',
                      u'тупик', u'бульвар', u'проулок', u'набережная', u'дорога'))
pl_only = frozenset((u'город', u'район', u'территория', u'городок', u'деревня', u'поселок'))


def Subareas(elem):
    '''Calculate subareas of relation by member role 'subarea'
    return dict with names as key and osmid as values
    '''
    if elem.osmid > 0:
        #applicable only to relation i.e. negative osmid
        return {}
    else:
        osmid = elem.osmid * (-1)
    cur = elem.conn.cursor()
    cur.execute("SELECT members FROM "+prefix+slim_rel_tbl+" WHERE id=%s",(osmid,))
    mem=cur.fetchone()
    if mem==None:
        return {}
    mem=mem[0]
    mem=zip(mem[1::2],mem[::2])#osm2pgsql use sequence to represent roles
    mem=[it[1] for it in mem if it[0]=='subarea' and (it[1][0]=='r' or it[1][0]=='w')]
    #relation stored with negative osmid 
    mem=[int(it[1:])*(-1 if it[0]=='r' else 1) for it in mem]
    
    res={}
    for id_a in mem:
        #using only valid polygons i.e. processed by osm2pgsql
        cur.execute('SELECT name FROM '+prefix+poly_table+' WHERE osm_id=%s ',(id_a,))
        name=cur.fetchone()
        if name:
            res[name[0]]=id_a
    return res

def FindCandidates(pgeom,elem,tbl=prefix+poly_table,addcond=""):
    '''Get elements that may be osm representation of elem
    That items must contain part of elem's name full or formal (this will be returned)
    and lies within polygon pgeom (polygon of parent territory)
    
    return ( [(name, osmid),..],formal)
    '''
    cur = elem.conn.cursor()
    formal=True
    name='%'+elem.formalname+'%'
    if pgeom==None:
        cur.execute("SELECT name, osm_id FROM "+tbl+" WHERE lower(name) LIKE lower(%s)"+addcond,(name,))
    else:
        cur.execute("SELECT name, osm_id FROM "+tbl+" WHERE lower(name) LIKE lower(%s) AND ST_Within(way,%s)"+addcond,(name,pgeom))
    res=cur.fetchall()
    if not res:
        if elem.offname==None or elem.formalname==elem.offname:
            return (None,None)
        name='%'+elem.offname+'%'
        if pgeom==None:
            cur.execute("SELECT name, osm_id FROM "+tbl+" WHERE lower(name) LIKE lower(%s)"+addcond,(name,))
        else:
            cur.execute("SELECT name, osm_id FROM "+tbl+" WHERE lower(name) LIKE lower(%s) AND ST_Within(way,%s)"+addcond,(name,pgeom))
        res=cur.fetchall()
        if res:
            formal=False
        else:
            return (None,None)
    
    return (res,formal)


def FindMangled(pgeom, elem, tbl=prefix + ways_table, addcond=""):
    '''Get osm representation of elem using name from streetmangler
    That items must lies within polygon pgeom (polygon of parent territory)

    return (name, osmid)
    '''
    if melt.mangledb.usable:
        mangl_n = melt.mangledb.db.CheckCanonicalForm(elem.shortname + " " + elem.formalname)
        if not mangl_n:
            return
    else:
        return
    cur = elem.conn.cursor()
    if pgeom == None:
        cur.execute("SELECT name, osm_id FROM " + tbl + " WHERE lower(name) = lower(%s)" + addcond, (mangl_n,))
    else:
        cur.execute("SELECT name, osm_id FROM " + tbl + " WHERE lower(name) = lower(%s) AND ST_Within(way,%s)" + addcond, (mangl_n, pgeom))
    return cur.fetchall()


def FindByKladr(elem, tbl=prefix + poly_table, addcond=""):
    cur = elem.conn.cursor()
    cur.execute("SELECT name, osm_id FROM " + tbl + """ WHERE "kladr:user" = %s""" + addcond, (elem.fias.code,))
    return cur.fetchall()

def FindAssocPlace(elem,pgeom):
    kladr = FindByKladr(elem)
    if kladr:
        elem.name = kladr[0][0]
        return kladr[0][1]
    (candidates,formal)=FindCandidates(pgeom,elem,prefix+poly_table," AND building ISNULL")
    if not candidates:
        return None
    for name in elem.names(formal):
        checked=[it[1] for it in candidates if it[0].lower()==name.lower()]
        if checked:
            elem.name = name
            return checked[0]

def FindAssocStreet(elem,pgeom):
    mangled = FindMangled(pgeom, elem, prefix + ways_table, " AND highway NOTNULL")
    if mangled:
        elem.name = mangled[0][0]
        return [it[1] for it in mangled]
    (candidates,formal)=FindCandidates(pgeom,elem,prefix+ways_table," AND highway NOTNULL")
    if not candidates:
        return None
    for name in elem.names(formal):
        checked=[it[1] for it in candidates if it[0].lower()==name.lower()]
        if checked:
            mangledb.AddMangleGuess(name)
            elem.name = name
            return checked


def AssocBuild(elem, point):
    '''Search and save building association for elem
    '''
    houses = elem.subO('not found_b')
    if not houses:
        return
    cur = elem.conn.cursor()
    if point:
        cur.execute("""SELECT osm_id, "addr:housenumber" FROM """ + prefix + point_table + """ WHERE "addr:street"=%s AND ST_Within(way,%s)""", (elem.name, elem.geom))
    else:
        cur.execute("""SELECT osm_id, "addr:housenumber" FROM """ + prefix + poly_table + """ WHERE "addr:street"=%s AND ST_Within(way,%s)""", (elem.name, elem.geom))
    osm_h = cur.fetchall()
    #Filtering of found is optimisation for updating and also remove POI with address
    #found_pre = set([h.onestr for h in elem.subO('found_b')])
    #osm_h = filter(lambda it: it[1] not in found_pre, osm_h)
    for hid, number in osm_h:
        for house in houses:
            if house.equal_to_str(number):
                assoc = melt.BuildAssoc(house.houseguid, hid, point)
                house.osm = assoc
                elem.session.add(assoc)
                houses.remove(house)
                break


def AssociateO(elem):
    '''Search and save association for all subelements of elem

    This function should work for elements with partitially associated subs
    as well as elements without associated subs
    '''
    if not elem.kind:
        return
    AssocBuild(elem, 0)
    AssocBuild(elem, 1)
    elem.subO('all', False)
    #run processing for found to parse their subs
    for sub in tuple(elem.subO('found', False)):
        AssociateO(melt.fias_AONode(sub))
    #find new elements for street if any
    for sub in tuple(elem.subO('street', False)):
        sub_ = melt.fias_AONode(sub)
        streets=FindAssocStreet(sub_,elem.geom)
        if streets<>None:
            pre = elem.session.query(melt.StreetAssoc).filter_by(ao_id=sub.f_id).all()
            pre = set([it.osm_way for it in pre])
            for street in streets:
                if street not in pre:
                    assoc = melt.StreetAssoc(sub.f_id, street)
                    elem.session.add(assoc)
            elem.session.commit()
            AssociateO(sub_)
    #search for new elements
    subareas = Subareas(elem)
    for sub in tuple(elem.subO('not found', False)):
        sub_ = melt.fias_AONode(sub)
        adm_id=None
        if subareas and sub_.fullname not in way_only:
            for name in sub_.names():
                if name in subareas:
                    adm_id = subareas.pop(name)
                    break

        if adm_id == None and sub_.fullname not in way_only:
            adm_id=FindAssocPlace(sub_,elem.geom)
        if not adm_id==None:
            assoc = melt.PlaceAssoc(sub.f_id, adm_id)
            elem.session.add(assoc)
            elem.child_found(sub, 'found')
            sub_.osmid = adm_id
            sub_.kind = 2
            AssociateO(sub_)
        elif sub_.fullname not in pl_only:
            streets=FindAssocStreet(sub_,elem.geom)
            if streets<>None:
                for street in streets:
                    assoc = melt.StreetAssoc(sub.f_id, street)
                    elem.session.add(assoc)
                elem.session.commit()
                elem.child_found(sub, 'street')
                sub_.kind = 1
                sub_.osmid = streets[0]
                AssociateO(sub_)
    elem.session.commit()
    #elem.stat('not found')
    #elem.stat('not found_b')


def AssocRegion(guid):
    region = melt.fias_AONode(guid)
    if not region.kind:
        adm_id = FindAssocPlace(region, None)
        if adm_id != None:
            assoc = melt.PlaceAssoc(region.f_id, adm_id)
            region.session.add(assoc)
            region.session.commit()
            region = melt.fias_AONode(guid, 2, adm_id)

    AssociateO(region)
    region.session.commit()
    return ":".join((region.name, str(region.kind)))


def fedobj():
    conn = melt.psycopg2.connect(melt.connstr)
    cur = conn.cursor()
    cur.execute("SELECT aoguid FROM fias_addr_obj f WHERE parentid is Null and livestatus")
    return [it[0] for it in cur.fetchall()]


def AssORoot():
    '''Associate and process federal subject (they have no parent id and no parent geom)
    '''
    print "Here we go!"
    for sub in fedobj():
        print AssocRegion(sub)


def AssORootM():
    '''Associate and process federal subject (they have no parent id and no parent geom)
    '''
    from multiprocessing import Pool
    pool = Pool()
    results = []
    for sub in fedobj():
        results.append(pool.apply_async(AssocRegion, (sub,)))

    while results:
        result = results.pop(0)
        print result.get()
        print len(results)


if __name__=="__main__":
    from deploy import AssocTableReCreate, AssocBTableReCreate, StatTableReCreate, AssocIdxCreate
    AssocTableReCreate()
    AssocBTableReCreate()
#    AssocTriggersReCreate()
    StatTableReCreate()
    AssORoot()
    AssocIdxCreate()
