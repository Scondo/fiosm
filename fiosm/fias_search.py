#!/usr/bin/python
# -*- coding: UTF-8 -*-

import melt
import config
import psycopg2
import ppygis
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from collections import OrderedDict
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

way_only = frozenset((u'улица', u'проезд', u'проспект', u'переулок', u'шоссе',
                      u'тупик', u'бульвар', u'проулок', u'набережная',
                      u'дорога', u'площадь', u'километр', u'аллея'))
pl_only = frozenset((u'город', u'район', u'территория', u'городок',
                     u'деревня', u'поселок', u'квартал'))


class AQuery(object):
    def __init__(self, poolsize=8):
        self.conns = []
        self.cachesize = poolsize * 2
        for _ in range(poolsize):
            self.conns.append(psycopg2.connect(config.psy_dsn, async=True))
        self.cache = OrderedDict()
        self.curs = {}

    def getfreeconn(self):
        import select
        from psycopg2.extensions import POLL_OK, POLL_READ, POLL_WRITE
        timeout = 0
        firstpass = True
        while 1:
            for conn in self.conns:
                state = conn.poll()
                if state == POLL_OK:
                    # Fetch all data bound to this connection
                    # for fut in self.cache.values():
                    #    if fut.cur.connection == conn:
                    #        fut.fetchall()
                    return conn
                elif firstpass:
                    pass
                elif state == POLL_READ:
                    select.select([conn.fileno()], [], [], timeout)
                elif state == POLL_WRITE:
                    select.select([], [conn.fileno()], [], timeout)
                else:
                    raise conn.OperationalError("bad state from poll: %s" %
                                                state)
            firstpass = False

    def execute(self, query, params=None):
        if (query, params) in self.cache:
            return self.cache[(query, params)]
        conn = self.getfreeconn()
        cur = conn.cursor()
        self.curs[conn] = cur
        cur.execute(query, params)
        if len(self.cache) > self.cachesize:
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
        if self.cacheall is None:
            psycopg2.extras.wait_select(self.cur.connection)
            self.cacheall = self.cur.fetchall()
        return self.cacheall

    def fetchone(self):
        if self.cacheall is not None:
            self.cacheone = self.cacheall[0]
        if self.cacheone is None:
            psycopg2.extras.wait_select(self.cur.connection)
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
                         'WHERE table_name = %s',
                         (config.prefix + config.poly_table, ))
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


def InitPointR(session):
    """Init point restriction with already used buildings"""
    global point_r
    # points are only buildings
    q = session.query(melt.BuildAssoc).filter_by(point=1)
    point_r = set([it.osm_build for it in q.all()])


def InitPolyR(session):
    """Init poly restriction with already used buildings and places"""
    global poly_r
    # polygons are buildings and places
    q = session.query(melt.BuildAssoc).filter_by(point=0)
    poly_r = set([it.osm_build for it in q.all()])
    q = session.query(melt.PlaceAssoc)
    poly_r.update([it.osm_admin for it in q.all()])


def InitWayR(session):
    """Init way restriction with already used streets"""
    global way_r
    q = session.query(melt.StreetAssoc)
    way_r = set([it.osm_way for it in q.all()])


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
                                config.prefix + config.slim_rel_tbl +
                                " WHERE id=%s", (osmid,))
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
                                     config.prefix + config.poly_table +
                                     ' WHERE osm_id=%s ', (id_a,))
        if name:
            res[name[0]] = id_a
    return res


def FindByName(pgeom, name, tbl=config.prefix + config.ways_table,
               addcond="", name_tag="name"):
    '''Get osm representation of object 'name'
    That items must lies within polygon pgeom (polygon of parent territory)

    return [osmid]
    '''
    if pgeom is None:
        res = AsyncQuery().execute("SELECT DISTINCT osm_id, ST_Area(way)"
                                   " FROM " + tbl + " "
                                   "WHERE lower(" + name_tag + ") = lower(%s)"
                                   " " + addcond + " "
                                   "ORDER BY ST_Area(way) DESC",
                                   (name,))
    else:
        res = AsyncQuery().execute("SELECT DISTINCT osm_id FROM " + tbl +
                                   " WHERE lower(" + name_tag + ") = lower(%s)"
                                   " AND ST_Within(way,%s)" + addcond,
                                   (name, pgeom))
    return res


def FindAssocPlace(elem, pgeom):
    session = elem.session

    def check_adm(osmid):
        if poly_r is not None:
            return osmid not in poly_r
        return session.query(melt.PlaceAssoc).get(osmid) is None

    def refine(id_list, addcond):
        return AsyncQuery().execute("SELECT DISTINCT osm_id FROM " +
                                    config.prefix + config.poly_table +
                                    " WHERE osm_id IN %s AND " + addcond,
                                    (tuple(id_list),)).fetchall()

    for name in elem.names():
        checked_all = [FindByName(pgeom, name,
                                  config.prefix + config.poly_table,
                                  " AND building ISNULL", name_tag)
                       for name_tag in ('"official_name:ru"',
                                        '"official_name"',
                                        '"name:ru"', 'place_name', 'name')]
        for checked in checked_all:
            checked = [it[0] for it in checked.fetchall() if check_adm(it[0])]

            if len(checked) > 1 and 'boundary' in MultiChk():
                checked0 = refine(checked, "boundary='administrative'")
                checked0 = [it[0] for it in checked0 if check_adm(it[0])]
                if len(checked0) != 0:
                    checked = checked0

            if len(checked) > 1 and 'admin_level' in MultiChk():
                checked0 = refine(checked, "admin_level NOTNULL")
                checked0 = [it[0] for it in checked0 if check_adm(it[0])]
                if len(checked0) != 0:
                    checked = checked0

            if len(checked) > 1 and 'place' in MultiChk():
                checked0 = refine(checked, "place IN ('city', 'town', "
                                  "'village', 'hamlet', 'suburb', 'quarter', "
                                  "'neighbourhood')")
                checked0 = [it[0] for it in checked0 if check_adm(it[0])]
                if len(checked0) != 0:
                    checked = checked0

            for osmid in checked:
                elem.name = name
                if poly_r is not None:
                    poly_r.add(osmid)
                return osmid


def FindAssocStreet(elem, pgeom):
    session = elem.session

    def check_street(osmid):
        if way_r is not None:
            return osmid not in way_r
        return session.query(melt.StreetAssoc).get(osmid) is None

    for name in elem.names():
        checked = FindByName(pgeom, name, config.prefix + config.ways_table,
                             " AND highway NOTNULL")
        checked = [it[0] for it in checked.fetchall() if check_street(it[0])]
        if checked:
            elem.name = name
            if way_r is not None:
                way_r.update(checked)
            return checked


def AssocBuild(elem, point, pgeom):
    '''Search and save building association for elem'''
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
                config.prefix +
                (config.point_table if point else config.poly_table) +
                ' WHERE ' +
                ('"addr:street"' if elem.kind == 1 else '"addr:place"') + '=%s'
                ' AND ST_Within(way,%s) AND "addr:housenumber" IS NOT NULL',
                (elem.name, pgeom))
    houses = elem.subB('not found_b')
    if not houses:
        return
    houses = {h.onestr: h for h in houses}
    osm_h = osm_h.fetchall()

    # Filtering of found is optimisation for updating
    # and also remove POI with address
    # TODO: check for this filter...
    # found_pre = set([h.onestr for h in elem.subO('found_b')])
    # osm_h = filter(lambda it: it[1] not in found_pre, osm_h)
    for hid, number in osm_h:
        if not check_bld(hid):
            continue
        fh = houses.get(number, None)
        if not fh:
            for house in houses.values():
                if house.equal_to_str(number):
                    fh = house
                    break
        if fh:
            assoc = melt.BuildAssoc(fh.houseid, hid, point)
            fh.osm = assoc
            elem.session.add(assoc)
            houses.pop(fh.onestr)
            if point:
                if point_r is not None:
                    point_r.add(hid)
            else:
                if poly_r is not None:
                    poly_r.add(hid)


def AssociateO(elem):
    '''Search and save association for all subelements of elem

    This function should work for elements with partitially associated subs
    as well as elements without associated subs
    '''
    if not elem.kind:
        return
    geom_ = geom(elem)
    # Precache subs list
    elem.subO('all', False)
    # run processing for found to parse their subs
    for sub in tuple(elem.subO('found', False)):
        AssociateO(melt.fias_AONode(sub))
    # find new elements for street if any
    for sub in tuple(elem.subO('street', False)):
        sub_ = melt.fias_AONode(sub)
        streets = FindAssocStreet(sub_, geom_.fetchone()[0])
        if streets is not None:
            pre = elem.session.query(melt.StreetAssoc).\
                filter_by(ao_id=sub.f_id).all()
            pre = set([it.osm_way for it in pre])
            for street in streets:
                if street not in pre:
                    assoc = melt.StreetAssoc(sub.f_id, street)
                    elem.session.add(assoc)
            elem.session.commit()
        AssociateO(sub_)
    # search for new areas
    subareas = Subareas(elem)
    for sub in tuple(elem.subO('not found', False)):
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
    # search for new streets
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
    # Search for buildings
    AssocBuild(elem, 0, geom_.fetchone()[0])
    AssocBuild(elem, 1, geom_.fetchone()[0])

    # elem.stat('not found', 1)
    # elem.stat('not found_b', 1)
    # elem.session.commit()


def AssocRegion(guid, session):
    region = melt.fias_AONode(guid, session=session)
    logging.info(u"Start " + region.name)
    if not region.kind:
        adm_id = FindAssocPlace(region, None)
        if adm_id is not None:
            assoc = melt.PlaceAssoc(region.f_id, adm_id)
            session.add(assoc)
            session.commit()
            region = melt.fias_AONode(guid, 2, adm_id)

    AssociateO(region)
    session.commit()
    return u":".join((region.name, str(region.kind)))


def AssORoot(session):
    '''Associate and process federal subject'''
    logging.info("Here we go!")
    InitPointR(session)
    InitPolyR(session)
    InitWayR(session)
    logging.info("Restriction cached")
    for (i, sub) in enumerate(session.query(melt.Addrobj).
                              filter_by(parentid=None, livestatus=True).all()):
        passed = AssocRegion(sub, session)
        logging.info(passed)
        logging.info(i)
        if i % 3 == 0:
            AsyncQuery().\
                execute("ANALYZE " + melt.BuildAssoc.__tablename__ + ";"
                        "ANALYZE " + melt.PlaceAssoc.__tablename__ + ";"
                        "ANALYZE " + melt.StreetAssoc.__tablename__ + ";")


if __name__ == "__main__":
    import argparse
    from deploy import AssocTableReCreate, AssocBTableReCreate, AssocIdxCreate
    from deploy import StatTableReCreate
    p = argparse.ArgumentParser()
    p.add_argument('--allnew', action='store_true')
    p.add_argument('region', nargs="?")
    args = p.parse_args()
    if args.allnew:
        AssocTableReCreate()
        AssocBTableReCreate()
#    AssocTriggersReCreate()
    StatTableReCreate()
    s = sessionmaker(expire_on_commit=False,
                     bind=create_engine(config.al_dsn,
                                        pool_size=2))()
    if args.region:
        q = s.query(melt.Addrobj).filter_by(parentid=None,
                                            livestatus=True,
                                            regioncode=args.region)
        AssocRegion(q.one())
    else:
        AssORoot(s)
    if args.allnew:
        AssocIdxCreate()
