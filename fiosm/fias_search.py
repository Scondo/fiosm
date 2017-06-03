#!/usr/bin/python
# -*- coding: UTF-8 -*-

import melt
from config import *
import config
import psycopg2
import ppygis
# import psycopg2.extensions
from collections import OrderedDict
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

way_only = frozenset((u'улица', u'проезд', u'проспект', u'переулок', u'шоссе',
                      u'тупик', u'бульвар', u'проулок', u'набережная',
                      u'дорога', u'площадь'))
pl_only = frozenset((u'город', u'район', u'территория', u'городок',
                     u'деревня', u'поселок', u'квартал'))


class AQuery(object):
    def __init__(self, poolsize=5):
        self.conns = []
        self.poolsize = poolsize
        for _ in range(poolsize):
            self.conns.append(psycopg2.connect(config.psy_dsn, async=True))
        self.cache = OrderedDict()
        self.curs = {}

    def getfreeconn(self):
        import select
        from psycopg2.extensions import POLL_OK, POLL_READ, POLL_WRITE
        timeout = 0
        while 1:
            for conn in self.conns:
                state = conn.poll()
                if state == POLL_OK:
                    return conn
                elif state == POLL_READ:
                    select.select([conn.fileno()], [], [], timeout)
                elif state == POLL_WRITE:
                    select.select([], [conn.fileno()], [], timeout)
                else:
                    raise conn.OperationalError("bad state from poll: %s" %
                                                state)

    def execute(self, query, params=None):
        if (query, params) in self.cache:
            return self.cache[(query, params)]
        conn = self.getfreeconn()
        cur = conn.cursor()
        self.curs[conn] = cur
        cur.execute(query, params)
        if len(self.cache) > self.poolsize:
            self.cache.popitem(False)
        res = FutureResult(cur)
        self.cache[(query, params)] = res
        return res

    def fetchall(self, query, params=None):
        return self.execute(query, params).fetchall()

    def fetchone(self, query, params=None):
        return self.execute(query, params).fetchone()


class FutureResult(object):
    def __init__(self, cur):
        self.cur = cur
        self.cacheone = None
        self.cacheall = None

    def fetchall(self):
        psycopg2.extras.wait_select(self.cur.connection)
        if self.cacheall is None:
            self.cacheall = self.cur.fetchall()
        return self.cacheall

    def fetchone(self):
        psycopg2.extras.wait_select(self.cur.connection)
        if self.cacheone is None:
            self.cacheone = self.cur.fetchone()
        return self.cacheone

_AQ = None


def AsyncQuery():
    global _AQ
    if _AQ is None:
        _AQ = AQuery()
    return _AQ

point_r = None
poly_r = None
way_r = None

multichkcache = None


def MultiChk():
    global multichkcache
    if multichkcache is None:
        q = AsyncQuery()
        cols = q.execute('SELECT column_name FROM information_schema.columns '
                         'WHERE table_name = %s', (prefix + poly_table, ))
        multichkcache = set([it[0] for it in cols.fetchall()])
    return multichkcache


def geom(AO):
    '''Geometry where buildings can be'''
    if not AO.guid:
        res = AsyncQuery().execute("SELECT NULL")
    elif (AO.kind == 2) and (AO.osmid is not None):
        # TODO: add to config if import allow multi-polygon (-G)
        res = AsyncQuery().execute("SELECT ST_Union(way) as way FROM " +
                                   config.prefix + config.poly_table +
                                   " WHERE osm_id=%s", (AO.osmid,))
    elif AO.kind == 1:
        res = AsyncQuery().execute("SELECT St_Buffer(ST_Union(w.way),2000) "
            "FROM " + config.prefix + config.ways_table + " w"
            ", " + config.prefix + config.way_aso_tbl + " s "
            "WHERE s.osm_way=w.osm_id AND s.ao_id=%s", (AO.f_id,))
    else:
        res = AsyncQuery().execute("SELECT NULL")
    return res


def InitPointR():
    """Init point restriction with already used buildings"""
    global point_r
    # points are only buildings
    points = AsyncQuery().execute('SELECT osm_build FROM ' +
                                  prefix + bld_aso_tbl + ' WHERE point=1')
    point_r = set([it[0] for it in points.fetchall()])


def InitPolyR():
    """Init poly restriction with already used buildings and places"""
    global poly_r
    # polygons are buildings and places
    poly = AsyncQuery().execute('SELECT osm_build FROM ' +
                                prefix + bld_aso_tbl + ' WHERE point=0')
    poly_r = set([it[0] for it in poly.fetchall()])
    poly = AsyncQuery().execute('SELECT osm_admin FROM ' +
                                prefix + pl_aso_tbl)
    poly_r.update([it[0] for it in poly.fetchall()])


def InitWayR():
    """Init way restriction with already used streets"""
    global way_r
    way = AsyncQuery().execute('SELECT osm_way FROM ' +
                               prefix + way_aso_tbl)
    way_r = set([it[0] for it in way.fetchall()])


def Subareas(elem):
    '''Calculate subareas of relation by member role 'subarea'
    return dict with names as key and osmid as values
    '''
    if elem.osmid > 0:
        # applicable only to relation i.e. negative osmid
        return {}
    else:
        osmid = elem.osmid * (-1)
    mem = AsyncQuery().fetchone("SELECT members FROM " +
                                prefix + slim_rel_tbl + " WHERE id=%s",
                                (osmid,))
    if mem is None:
        return {}
    mem = mem[0]
    mem = zip(mem[1::2], mem[::2])  # osm2pgsql use sequence to represent roles
    mem = [it[1] for it in mem if
           it[0] == 'subarea' and (it[1][0] == 'r' or it[1][0] == 'w')]
    # relation stored with negative osmid
    mem = [int(it[1:]) * (-1 if it[0] == 'r' else 1) for it in mem]
    res = {}
    for id_a in mem:
        # using only valid polygons i.e. processed by osm2pgsql
        name = AsyncQuery().fetchone('SELECT name FROM ' +
                                     prefix + poly_table + ' WHERE osm_id=%s ',
                                     (id_a,))
        if name:
            res[name[0]] = id_a
    return res


def FindByName(pgeom, name, tbl=prefix + ways_table,
               addcond="", name_tag="name"):
    '''Get osm representation of object 'name'
    That items must lies within polygon pgeom (polygon of parent territory)

    return [osmid]
    '''
    if pgeom is None:
        res = AsyncQuery().execute("SELECT DISTINCT osm_id FROM " + tbl +
                                   " WHERE lower(" + name_tag + ") = lower(%s)"
                                   "" + addcond, (name,))
    else:
        res = AsyncQuery().execute("SELECT DISTINCT osm_id FROM " + tbl +
                                   " WHERE lower(" + name_tag + ") = lower(%s) "
                                   "AND ST_Within(way,%s)" + addcond,
                                   (name, pgeom))
    return [it[0] for it in res.fetchall()]


def FindByKladr(elem, tbl=prefix + poly_table, addcond=""):
    res = AsyncQuery().execute("SELECT name, osm_id FROM " + tbl + ""
        """ WHERE "kladr:user" = %s""" + addcond, (elem.fias.code,))
    return res.fetchall()


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
            checked = FindByName(pgeom, name, prefix + poly_table,
                             " AND building ISNULL", name_tag)
            checked = filter(check_adm, checked)

            if len(checked) > 1 and 'boundary' in MultiChk():
                checked0 = FindByName(pgeom, name, prefix + poly_table,
                             " AND building ISNULL AND "
                             "boundary='administrative'", name_tag)
                checked0 = filter(check_adm, checked0)
                if len(checked0) != 0:
                    checked = checked0

            if len(checked) > 1 and 'admin_level' in MultiChk():
                checked0 = FindByName(pgeom, name, prefix + poly_table,
                             " AND building ISNULL AND admin_level NOTNULL", name_tag)
                checked0 = filter(check_adm, checked0)
                if len(checked0) != 0:
                    checked = checked0

            if len(checked) > 1 and 'place' in MultiChk():
                checked0 = FindByName(pgeom, name, prefix + poly_table,
                             " AND building ISNULL AND "
                             "place IN ('city', 'town', 'village', 'hamlet', "
                             "'suburb', 'quarter', 'neighbourhood')", name_tag)
                checked0 = filter(check_adm, checked0)
                if len(checked0) != 0:
                    checked = checked0

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
        checked = FindByName(pgeom, name, prefix + ways_table,
                             " AND highway NOTNULL")
        checked = filter(check_street, checked)
        if checked:
            elem.name = name
            if way_r is not None:
                way_r.update(checked)
            return checked


def AssocBuild(elem, point, pgeom):
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

    osm_h = AsyncQuery().\
        execute('SELECT osm_id, "addr:housenumber" FROM ' +
                prefix + (point_table if point else poly_table) + ' WHERE ' +
                ('"addr:street"' if elem.kind == 1 else '"addr:place"') + '=%s'
                ' AND ST_Within(way,%s) AND "addr:housenumber" IS NOT NULL',
                (elem.name, pgeom))
    houses = elem.subB('not found_b')
    if not houses:
        return
    osm_h = osm_h.fetchall()

    #Filtering of found is optimisation for updating
    #and also remove POI with address
    #found_pre = set([h.onestr for h in elem.subO('found_b')])
    #osm_h = filter(lambda it: it[1] not in found_pre, osm_h)
    for hid, number in osm_h:
        for house in houses:
            if house.equal_to_str(number) and check_bld(hid):
                assoc = melt.BuildAssoc(house.houseid, hid, point)
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
    geom_ = geom(elem)
    #Precache subs list
    # practically it's 'not found', filling others subs as side-effect
    elem.subO('all', True)
    #run processing for found to parse their subs
    for sub in tuple(elem.subO('found', True)):
        AssociateO(melt.fias_AONode(sub))
    #find new elements for street if any
    for sub in tuple(elem.subO('street', True)):
        sub_ = melt.fias_AONode(sub)
        streets = FindAssocStreet(sub_, geom_.fetchone()[0])
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
            adm_id = FindAssocPlace(sub_, geom_.fetchone()[0])
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
        streets = FindAssocStreet(sub_, geom_.fetchone()[0])
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
    AssocBuild(elem, 0, geom_.fetchone()[0])
    AssocBuild(elem, 1, geom_.fetchone()[0])

    # elem.stat('not found', 1)
    # elem.stat('not found_b', 1)
    # elem.session.commit()


def AssocRegion(guid):
    region = melt.fias_AONode(guid)
    logging.info(u"Start " + region.name)
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


def AssORoot():
    '''Associate and process federal subject (they have no parent id and no parent geom)
    '''

    logging.info("Here we go!")
    InitPointR()
    InitPolyR()
    InitWayR()
    logging.info("Restriction cached")
    for sub in AsyncQuery().fetchall("SELECT aoguid FROM fias_addr_obj "
                                     "WHERE (parentid is Null) and "
                                     "livestatus"):
        passed = AssocRegion(sub[0])
        logging.info(passed)
        AsyncQuery().execute("ANALYZE " + melt.BuildAssoc.__tablename__ + ";"
                             "ANALYZE " + melt.PlaceAssoc.__tablename__ + ";"
                             "ANALYZE " + melt.StreetAssoc.__tablename__ + ";")


def AssORootM():
    '''Associate and process federal subject (they have no parent id and no parent geom)
    '''
    from multiprocessing import Pool
    pool = Pool()
    results = []
    for sub in AsyncQuery().fetchall("SELECT aoguid FROM fias_addr_obj "
                                     "WHERE (parentid is Null) and "
                                     "livestatus"):
        results.append(pool.apply_async(AssocRegion, (sub[0],)))

    while results:
        result = results.pop(0)
        print result.get()
        print len(results)


if __name__ == "__main__":
    from deploy import AssocTableReCreate, AssocBTableReCreate, StatTableReCreate, AssocIdxCreate
    AssocTableReCreate()
    AssocBTableReCreate()
#    AssocTriggersReCreate()
    StatTableReCreate()
    AssORoot()
    AssocIdxCreate()
