#!/usr/bin/python
# -*- coding: UTF-8 -*-

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
import melt
from config import *
way_only = frozenset((u'улица', u'проезд', u'проспект', u'переулок', u'шоссе',
                      u'тупик', u'бульвар', u'проулок', u'набережная',
                      u'дорога', u'площадь'))
pl_only = frozenset((u'город', u'район', u'территория', u'городок',
                     u'деревня', u'поселок', u'квартал'))

point_r = None
poly_r = None
way_r = None

multichkcache = None


def MultiChk(conn):
    global multichkcache
    if multichkcache is None:
        multichkcache = set()
        cur = conn.cursor()
        cur.execute('SELECT column_name FROM information_schema.columns '
                    'WHERE table_name = %s', (prefix + poly_table, ))
        multichkcache = set([it[0] for it in cur.fetchall()])
    return multichkcache


def InitPointR(conn):
    """Init point restriction with already used buildings
    """
    global point_r
    cur = conn.cursor()
    # points are only buildings
    cur.execute('SELECT osm_build FROM ' + prefix + bld_aso_tbl + ' WHERE point=1')
    point_r = set([it[0] for it in cur.fetchall()])


def InitPolyR(conn):
    """Init poly restriction with already used buildings and places
    """
    global poly_r
    cur = conn.cursor()
    # points are only buildings
    cur.execute('SELECT osm_build FROM ' + prefix + bld_aso_tbl + ' WHERE point=0')
    poly_r = set([it[0] for it in cur.fetchall()])
    cur.execute('SELECT osm_admin FROM ' + prefix + pl_aso_tbl)
    poly_r.update([it[0] for it in cur.fetchall()])


def InitWayR(conn):
    """Init way restriction with already used streets
    """
    global way_r
    cur = conn.cursor()
    # points are only buildings
    cur.execute('SELECT osm_way FROM ' + prefix + way_aso_tbl)
    way_r = set([it[0] for it in cur.fetchall()])


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


def FindByName(pgeom, conn, name,
               tbl=prefix + ways_table,
               addcond="", name_tag="name"):
    '''Get osm representation of object 'name'
    That items must lies within polygon pgeom (polygon of parent territory)

    return [osmid]
    '''
    cur = conn.cursor()
    if pgeom is None:
        cur.execute("SELECT DISTINCT osm_id FROM " + tbl +
                    " WHERE lower(" + name_tag + ") = lower(%s)" + addcond,
                    (name,))
    else:
        cur.execute("SELECT DISTINCT osm_id FROM " + tbl +
                    " WHERE lower(" + name_tag + ") = lower(%s) "
                    "AND ST_Within(way,%s)" + addcond,
                    (name, pgeom))
    return cur.fetchall()


def FindByKladr(elem, tbl=prefix + poly_table, addcond=""):
    cur = elem.conn.cursor()
    cur.execute("SELECT name, osm_id FROM " + tbl + \
                """ WHERE "kladr:user" = %s""" + addcond, (elem.fias.code,))
    return cur.fetchall()


def FindAssocPlace(elem, pgeom):
    def check_adm(osmid):
        if poly_r is not None:
            return osmid not in poly_r
        return session.query(melt.PlaceAssoc).get(osmid) is None

    session = elem.session
    kladr = FindByKladr(elem, addcond=" AND building ISNULL")
    if kladr and check_adm(kladr[0][1]):
        elem.name = kladr[0][0]
        if poly_r is not None:
            poly_r.add(kladr[0][1])
        return kladr[0][1]
    for name in elem.names():
        for name_tag in ('name', 'place_name', '"name:ru"',
                         '"official_name"', '"official_name:ru"'):
            checked = FindByName(pgeom, elem.conn, name, prefix + poly_table,
                             " AND building ISNULL", name_tag)

            if len(checked) > 1 and 'boundary' in MultiChk(elem.conn):
                checked0 = FindByName(pgeom, elem.conn, name, prefix + poly_table,
                             " AND building ISNULL AND "
                             "boundary='administrative'", name_tag)
                if len(checked0) != 0:
                    checked = checked0

            if len(checked) > 1 and 'admin_level' in MultiChk(elem.conn):
                checked0 = FindByName(pgeom, elem.conn, name, prefix + poly_table,
                             " AND building ISNULL AND admin_level NOTNULL", name_tag)
                if len(checked0) != 0:
                    checked = checked0

            if len(checked) > 1 and 'place' in MultiChk(elem.conn):
                checked0 = FindByName(pgeom, elem.conn, name, prefix + poly_table,
                             " AND building ISNULL AND "
                             "place IN ('city', 'town', 'village', 'hamlet', "
                             "'suburb', 'quarter', 'neighbourhood')", name_tag)
                if len(checked0) != 0:
                    checked = checked0

            checked = [it[0] for it in checked]
            checked = filter(check_adm, checked)
            for osmid in checked:
                elem.name = name
                if poly_r is not None:
                    poly_r.add(osmid)
                    return osmid


def FindAssocStreet(elem, pgeom):
    def check_street(osmid):
        if way_r is not None:
            return osmid not in way_r
        return session.query(melt.StreetAssoc).get(osmid) is None

    session = elem.session
    for name in elem.names():
        checked = FindByName(pgeom, elem.conn, name, prefix + ways_table,
                             " AND highway NOTNULL")
        checked = [it[0] for it in checked]
        checked = filter(check_street, checked)
        if checked:
            elem.name = name
            if way_r is not None:
                way_r.update(checked)
            return checked


def AssocBuild(elem, point):
    '''Search and save building association for elem
    '''
    def check_bld(osmid):
        if point:
            if point_r is not None:
                return not (osmid in point_r)
        else:
            if poly_r is not None:
                return not (osmid in poly_r)
        return elem.session.query(melt.BuildAssoc).get((osmid, point)) is None

    houses = elem.subB('not found_b')
    if not houses:
        return
    cur = elem.conn.cursor()
    cur.execute('SELECT osm_id, "addr:housenumber" FROM ' +\
                prefix + (point_table if point else poly_table) + ' WHERE ' +\
                ('"addr:street"' if elem.kind == 1 else '"addr:place"') + '=%s'
                ' AND ST_Within(way,%s) AND "addr:housenumber" IS NOT NULL',
                (elem.name, elem.geom))
    osm_h = cur.fetchall()

    #Filtering of found is optimisation for updating
    #and also remove POI with address
    #found_pre = set([h.onestr for h in elem.subO('found_b')])
    #osm_h = filter(lambda it: it[1] not in found_pre, osm_h)
    for hid, number in osm_h:
        for house in houses:
            if house.equal_to_str(number) and check_bld(hid):
                assoc = melt.BuildAssoc(house.houseguid, hid, point)
                house.osm = assoc
                elem.session.add(assoc)
                houses.remove(house)
                if point:
                    if point_r is not None:
                        point_r.add(hid)
                else:
                    if poly_r is not None:
                        poly_r.add(hid)

                break


def AssociateO(elem):
    '''Search and save association for all subelements of elem

    This function should work for elements with partitially associated subs
    as well as elements without associated subs
    '''
    if not elem.kind:
        return
    #Precache subs list
    # practically it's 'not found', filling others subs as side-effect
    elem.subO('all', True)
    #run processing for found to parse their subs
    for sub in tuple(elem.subO('found', True)):
        AssociateO(melt.fias_AONode(sub))
    #find new elements for street if any
    for sub in tuple(elem.subO('street', True)):
        sub_ = melt.fias_AONode(sub)
        streets = FindAssocStreet(sub_, elem.geom)
        if streets is not None:
            pre = elem.session.query(melt.StreetAssoc).filter_by(ao_id=sub.f_id).all()
            pre = set([it.osm_way for it in pre])
            for street in streets:
                if street not in pre:
                    assoc = melt.StreetAssoc(sub.f_id, street)
                    elem.session.add(assoc)
            elem.session.commit()
        AssociateO(sub_)
    #search for new areas
    subareas = Subareas(elem)
    for sub in tuple(elem.subO('not found', True)):
        if sub.fullname in way_only:
            continue
        sub_ = melt.fias_AONode(sub)
        adm_id = None
        if subareas:
            for name in sub_.names():
                if name in subareas:
                    adm_id = subareas.pop(name)
                    break
        if adm_id is None:
            adm_id = FindAssocPlace(sub_, elem.geom)
        if not (adm_id is None):
            assoc = melt.PlaceAssoc(sub.f_id, adm_id)
            elem.session.add(assoc)
            elem.child_found(sub, 'found')
            sub_.osmid = adm_id
            sub_.kind = 2
            AssociateO(sub_)
    #search for new streets
    for sub in tuple(elem.subO('not found', False)):
        if sub.fullname in pl_only:
            continue
        sub_ = melt.fias_AONode(sub)
        streets = FindAssocStreet(sub_, elem.geom)
        if streets is not None:
            for street in streets:
                assoc = melt.StreetAssoc(sub.f_id, street)
                elem.session.add(assoc)
            elem.session.commit()
            elem.child_found(sub, 'street')
            sub_.kind = 1
            sub_.osmid = streets[0]
            AssociateO(sub_)
    #Search for buildings
    AssocBuild(elem, 0)
    AssocBuild(elem, 1)

    elem.stat('not found', 1)
    elem.stat('not found_b', 1)
    elem.session.commit()


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
    return u":".join((region.name, str(region.kind)))


def fedobj():
    conn = melt.psycopg2.connect(melt.psy_dsn)
    cur = conn.cursor()
    cur.execute("SELECT aoguid FROM fias_addr_obj WHERE (parentid is Null) and livestatus")
    res = cur.fetchall()
    return [it[0] for it in res]


def AssORoot():
    '''Associate and process federal subject (they have no parent id and no parent geom)
    '''

    logging.info("Here we go!")
    conn = melt.psycopg2.connect(melt.psy_dsn)
    InitPointR(conn)
    InitPolyR(conn)
    InitWayR(conn)
    logging.info("Restriction cached")
    for sub in fedobj():
        passed = AssocRegion(sub)
        logging.info(passed)


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
