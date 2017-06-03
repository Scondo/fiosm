#!/usr/bin/python
# -*- coding: UTF-8 -*-
from __future__ import division
import logging
import nice_street
import config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import relationship, backref, joinedload, subqueryload
from sqlalchemy import ForeignKey, Column, Integer, BigInteger, SmallInteger
from sqlalchemy import Table
from sqlalchemy.sql.expression import select
from fias_db import Base, Socrbase, House, Addrobj, GUID, FiasMeta


# with keys socr#aolev
socr_cache = {}


if config.use_osm:
    util_engine = create_engine(config.al_dsn, pool_size=2, pool_recycle=180)
    PolyTable = Table(config.prefix + config.poly_table, FiasMeta,
                      autoload=True, autoload_with=util_engine)
    WaysTable = Table(config.prefix + config.ways_table, FiasMeta,
                      autoload=True, autoload_with=util_engine)
    PointTable = Table(config.prefix + config.point_table, FiasMeta,
                       autoload=True, autoload_with=util_engine)
    del util_engine

    class Statistic(Base):
        __tablename__ = 'fiosm_stat'
        ao_id = Column(Integer, ForeignKey("fias_addr_obj.id"),
                       primary_key=True)
        ao_all = Column(Integer)
        found = Column(Integer)
        street = Column(Integer)
        all_b = Column(Integer)
        found_b = Column(Integer)
        all_r = Column(Integer)
        found_r = Column(Integer)
        street_r = Column(Integer)
        all_b_r = Column(Integer)
        found_b_r = Column(Integer)
        fias = relationship("Addrobj", backref=backref("stat", uselist=False),
                            uselist=False)

        @property
        def all(self):
            return self.ao_all

        def fromdic(self, dic):
            for it in dic.iteritems():
                setattr(self, 'ao_all' if it[0] == 'all' else it[0], it[1])

        def __init__(self, dic):
            self.fromdic(dic)

    class PlaceAssoc(Base):
        __tablename__ = config.prefix + config.pl_aso_tbl
        ao_id = Column(Integer, ForeignKey("fias_addr_obj.id"))
        osm_admin = Column(BigInteger, primary_key=True)
        fias = relationship("Addrobj", backref=backref("place", uselist=False),
                            uselist=False)

        def __init__(self, f_id, osmid):
            self.ao_id = f_id
            self.osm_admin = osmid

    class StreetAssoc(Base):
        __tablename__ = config.prefix + config.way_aso_tbl
        ao_id = Column(Integer, ForeignKey("fias_addr_obj.id"))
        osm_way = Column(BigInteger, primary_key=True)
        fias = relationship("Addrobj", backref=backref("street"),
                            uselist=False)

        def __init__(self, f_id, osmid):
            self.ao_id = f_id
            self.osm_way = osmid

    class BuildAssoc(Base):
        __tablename__ = config.prefix + config.bld_aso_tbl
        h_id = Column(GUID, ForeignKey("fias_house.houseid"),
                      primary_key=False, index=True)
        osm_build = Column(BigInteger, primary_key=True)
        point = Column(SmallInteger, primary_key=True)
        fias = relationship("House",
                            backref=backref("osm", uselist=False,
                                            lazy='joined'),
                            uselist=False, lazy='select')

        def __init__(self, h_id, osmid, point):
            self.osm_build = osmid
            self.point = point
            self.h_id = h_id


osm_base_link = u'http://www.openstreetmap.org/browse'


def osm_link(osm_id):
    if osm_id is None:
        return u''
    if osm_id < 0:
        osm_type = u'relation'
        osm_id = -1 * osm_id
    else:
        osm_type = u'way'
    return '/'.join((osm_base_link, osm_type, unicode(osm_id)))


def HouseOSMLink(self):
    if self.osm is None:
        return u''
    elif self.osm.point == 0:
        return osm_link(self.osm.osm_build)
    elif self.osm.point == 1:
        return '/'.join((osm_base_link, 'node', unicode(self.osm.osm_build)))


def HouseTXTKind(self):
    if self.osm is None:
        return u'нет в ОСМ'
    elif self.osm.point == 1:
        return u'точка'
    elif self.osm.point == 0:
        return u'полигон'
if config.use_osm:
    House.txtkind = property(HouseTXTKind)
    House.osmlink = property(HouseOSMLink)
else:
    House.txtkind = u'ОСМ отключен'
    House.osmlink = u''


class fias_AO(object):
    def __init__(self, guid=None, kind=None, osmid=None, parent=None,
                 session=None):
        if not guid:
            guid = None
        if isinstance(guid, Addrobj):
            self._fias = guid
            self._guid = None
        else:
            self._guid = guid
            self._fias = None
        if session is None:
            engine = create_engine(config.al_dsn, pool_size=2)
            Session = sessionmaker(expire_on_commit=False)
            self.session = Session(bind=engine)
        else:
            self.session = session
        self._osmid = osmid
        self._osmkind = None
        self.setkind(kind)
        self._parent = parent
        self._stat = {}
        self._subB = None  # cache all buildings

    @property
    def guid(self):
        if self._guid == None:
            self._guid = self.fias.aoguid
        return self._guid

    @property
    def f_id(self):
        return self.fias.id

    def calkind(self):
        if not config.use_osm:
            self._kind = 0
            return
        #'found'
        cid = self.fias.place
        if cid is not None:
            self._osmid = cid.osm_admin
            self._kind = 2
            return 2
        #'street':
        cid = self.fias.street
        if cid:
            self._osmid = cid[0].osm_way
            self._kind = 1
        else:
            self._kind = 0

    def getkind(self):
        if self.guid is None:
            return 2
        if self._kind == None:
            self.calkind()
        return self._kind

    def setkind(self, kind):
        if not type(kind) is int:
            if str(kind) == 'found':
                self._kind = 2
            elif str(kind) == 'street':
                self._kind = 1
            elif str(kind) == 'not found':
                self._kind = 0
            else:
                self._kind = None
        else:
            self._kind = kind

    kind = property(getkind, setkind, doc='''Basic type of object
        0-not found
        1-street
        2-found as area
        ''')

    @property
    def txtkind(self):
        if self.kind == 0:
            return u'нет в ОСМ'
        elif self.kind == 1:
            return u'улица'
        elif self.kind == 2:
            return u'территория'

    def getFiasData(self):
        if self._guid:
            self._fias = self.session.query(Addrobj).\
            filter_by(aoguid=self.guid).one()
        elif self._osmid and self._osmkind == 2:
            a = self.session.query(PlaceAssoc).\
            filter_by(osm_admin=self._osmid).first()
            self._fias = a.fias
        elif self._osmid and self._osmkind == 1:
            a = self.session.query(StreetAssoc).\
            filter_by(osm_way=self._osmid).first()
            self._fias = a.fias
        else:
            self._fias = Addrobj({'formalname': 'Россия'})

    @property
    def fias(self):
        if self._fias == None:
            self.getFiasData()
        return self._fias

    def __getattr__(self, name):
        if not name.startswith('_') and hasattr(self.fias, name):
            return getattr(self.fias, name)
        else:
            raise AttributeError

    @property
    def formalname(self):
        return self.fias.formalname

    @property
    def fullname(self):
        if self.fias.shortname is None:
            return u""
        key = u"#".join((self.fias.shortname, str(self.fias.aolevel)))
        if key not in socr_cache:
            res = self.session.query(Socrbase).\
            filter_by(scname=self.fias.shortname, level=self.fias.aolevel).\
            first()
            if res:
                socr_cache[key] = res.socrname.lower()
            else:
                socr_cache[key] = ""
        return socr_cache[key]

    def basenames(self):
        if self.fias.formalname is None:
            return (self.fias.offname,)
        if self.fias.offname is None:
            return (self.fias.formalname,)
        if self.fias.offname == self.fias.formalname:
            return (self.fias.offname,)
        return (self.fias.offname, self.fias.formalname)

    def names(self, formal=None):
        if formal is None:
            was = set()
            for name in nice_street.nice(self.formalname, self.fias.shortname,
                                    self.fullname, self.kind == 2):
                if name not in was:
                    yield name
                    was.add(name)
            if not(self.fias.formalname is None):
                for name in self.names(self.formalname):
                    if name not in was:
                        yield name
                        was.add(name)
            if not(self.fias.offname is None):
                for name in self.names(self.offname):
                    if name not in was:
                        yield name
                        was.add(name)
            if not was:
                yield ''
        else:
            uns = nice_street.unslash(formal)
            if uns != formal:
                yield self.fullname + " " + uns
                yield uns + " " + self.fullname
                yield uns
            yield self.fullname + " " + formal
            yield formal + " " + self.fullname
            yield formal

    @property
    def parentguid(self):
        if self._parent == None:
            if self.fias.parent is not None:
                self._parent = self.fias.parent.aoguid
        return self._parent

    @property
    def parent(self):
        if not hasattr(self, '_parentO'):
            self._parentO = fias_AO(self.parentguid)
            if self._fias is not None:
                self._parentO._fias = self.fias.parent
        return self._parentO

    @property
    def isok(self):
        return (self.fias != None) or (self.guid == None)

    def subB(self, typ):
        def only_best(bld_list):
            done = set()
            for one in bld_list:
                if one.houseguid in done:
                    continue
                like_me = filter(lambda it: it.houseguid == one.houseguid,
                                 bld_list)
                if len(like_me) == 1:
                    yield one
                else:
                    for fiFunc in (lambda it: it.updatedate >= one.updatedate,
                                   lambda it: it.startdate >= one.startdate,
                                   lambda it: it.enddate >= one.enddate,
                                   lambda it: it.divtype >= one.divtype,
                                   ):
                        like_me = filter(fiFunc, bld_list)
                        if len(like_me) == 1:
                            break
                    done.add(one.houseguid)
                    yield one

        if self._subB is None:
            q = self.session.query(House)
            _subB = q.filter_by(ao_id=self.f_id).all()
            guids = [it.houseguid for it in _subB]
            if len(guids) == len(set(guids)):
                self._subB = _subB
            else:
                self._subB = list(only_best(_subB))
        if typ == 'all_b':
            return self._subB
        elif typ == 'found_b':
            return filter(lambda h: h.osm is not None, self._subB)
        elif typ == 'not found_b':
            return filter(lambda h: h.osm is None, self._subB)

    def CalcAreaStat(self, typ):
        #check in pulled children
        if hasattr(self, '_subO') and\
        (typ in self._subO or (typ == 'all' and 'not found' in self._subO)):
                self._stat[typ] = len(self.subO(typ))
        elif typ in ('all', 'found', 'street'):
            if ('all' in self._stat and self._stat['all'] == 0) or\
            (self.kind == 0 and typ == 'found') or\
            (self.kind < 2 and typ == 'street'):
                self._stat[typ] = 0
            else:
                #make request
                q = self.session.query(Addrobj).filter_by(parentid=self.f_id,
                                                          livestatus=True)
                if typ == 'found':
                    q = q.join(PlaceAssoc)
                elif typ == 'street':
                    q = q.join(StreetAssoc).distinct(Addrobj.id)
                #otherwise - all
                self._stat[typ] = q.count()

        elif typ.endswith('_b'):
            #no buildings in country
            if self.guid == None:
                self._stat[typ] = 0
                return
            #all building children are easily available from all_b
            self._stat[typ] = len(self.subB(typ))

    def CalcRecStat(self, typ, savemode=1):
        res = 0
        for ao in self.subO('not found'):
            res += fias_AONode(ao).stat(typ, savemode)
        for ao in self.subO('found'):
            res += fias_AONode(ao).stat(typ, savemode)
        for ao in self.subO('street'):
            res += ao.stat(typ[:-2])
        self._stat[typ] = res + self.stat(typ[:-2])

    @property
    def stat_db_full(self):
        if not config.use_osm:
            return False
        stat = self.fias.stat
        if stat is None:
            return False
        if self.kind == 1:
            return True
        if stat.all_r is None or stat.all_b_r is None:
            return False
        return True

    def pullstatA(self):
        '''Pull stat info from statistic obj'''
        def getvalue(item):
            value = getattr(stat, item)
            if value is not None:
                self._stat[item] = value

        stat = self.fias.stat
        if stat is None:
            return {}
        for r in ('', '_r'):
            getvalue('all' + r)
            if self._stat.get('all', 0):
                getvalue('found' + r)
                getvalue('street' + r)
            getvalue('all_b' + r)
            if self._stat.get('all_b', 0):
                getvalue('found_b' + r)
        return self._stat

    def stat(self, typ, savemode=0):
        '''Statistic of childs for item'''
        if not config.use_osm:
            return 0
        #Calculable values
        (r, t0) = ('_r', typ[:-2]) if typ.endswith('_r') else ('', typ)
        (b, t0) = ('_b', t0[:-2]) if t0.endswith('_b') else ('', t0)
        if t0 != 'all' and self.stat('all' + b + r, savemode) == 0:
            return 0
        if t0 == 'all_found':
            return self.stat('found' + r) + self.stat('street' + r)
        if t0 == 'all_high':
            return 0.9 * self.stat('all' + b + r)
        if t0 == 'all_low':
            return 0.2 * self.stat('all' + b + r)
        if t0 == 'not found':
            #Try to pull saved stat
            if not (('all' + b + r) in self._stat) and self.guid != None:
                self.pullstatA()

            if r and ('all' + b not in self._stat):
                self.CalcRecStat(typ, savemode)
            return self.stat('all' + b + r) - (self.stat('found_b' + r) if b else self.stat('all_found' + r))
        #There no streets or buildings in root
        if self.guid == None and (typ == 'street' or typ.endswith('_b')):
            return 0
        #Try to pull saved stat
        if not (typ in self._stat) and self.guid != None:
            self.pullstatA()
        #If still no stat - calculate
        if typ not in self._stat:
            if r:
                self.CalcRecStat(typ, savemode)
            else:
                self.CalcAreaStat(typ)
            self.SaveAreaStat(savemode)
        if typ not in self._stat:
            logging.error("Not calculated stat " + typ)
            logging.error(self._stat.items())
            logging.error(self.guid)
        return self._stat[typ]

    def SaveAreaStat(self, mode=0):
        '''Save statistic to DB
        mode - 0 if have any slice,
        1 - if have all not recursive,
        2 - if have all
        '''
        if self.guid == None:
            return
        stat = self._stat
        a = ('all' in stat) and ('found' in stat) and ('street' in stat)
        b = ('all_b' in stat) and ('found_b' in stat)
        ar = ('all_r' in stat) and ('found_r' in stat) and ('street_r' in stat)
        br = ('all_b_r' in stat) and ('found_b_r' in stat)
        f = mode == 2 and a and b and ar and br
        statR = self.fias.stat
        if statR is None:
            # multithread backup
            self.session.query(Statistic).get(self.f_id)
        if statR is None:
            if a or b or ar or br:
                statR = Statistic({"ao_id": self.f_id})
                self.session.add(statR)
                self.fias.stat = statR
            else:
                return
        if (mode == 0 and a) or (mode == 1 and a and b) or f:
            statR.fromdic(stat)
        elif (mode == 0 and b) or (mode == 1 and a and b) or f:
            statR.fromdic(stat)
        elif (mode == 0 and ar) or (mode == 1 and ar and br) or f:
            statR.fromdic(stat)
        elif (mode == 0 and br) or (mode == 1 and ar and br) or f:
            statR.fromdic(stat)
        self.session.commit()

    @property
    def name(self):
        '''Name of object as on map'''
        if self.guid is None:
            return u"Россия"

        if not hasattr(self, '_name'):
            if self.kind:
                if self.kind == 2:
                    res = self.session.execute(
                        PolyTable.select(PolyTable.c.osm_id == self.osmid))
                    name = res.fetchone()
                elif self.kind == 1:
                    res = self.session.execute(
                        WaysTable.select(WaysTable.c.osm_id == self.osmid))
                    name = res.fetchone()
                else:
                    name = None
            else:
                name = None
            if name and name['name:ru']:
                self._name = name['name:ru']
            elif name and name['name']:
                self._name = name['name']
            else:
                self._name = self.names().next()
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def osmid(self):
        if self._osmid is None:
            if (self._kind == 0) or (self.guid is None):
                # Do not even try if not found or root
                return None
            self.calkind()
            #If kind other than 'not found' then we receive osmid
        return self._osmid

    @osmid.setter
    def osmid(self, value):
        self._osmid = value

    @property
    def osmlink(self):
        if self.osmid is None:
            return u''
        else:
            return osm_link(self.osmid)


class fias_AONode(fias_AO):
    def __init__(self, *args, **kwargs):
        if type(args[0]) == fias_AO:
            for it in args[0].__dict__.keys():
                self.__setattr__(it, args[0].__getattribute__(it))
            self.__class__ = fias_AONode
        else:
            fias_AO.__init__(self, *args, **kwargs)
        self._subO = {}

    def subO(self, typ, ao_stat=True, pullnames=None):
        '''List of subelements'''
        def get_names_(objs):
            if not objs:
                return
            ids = set([obj.osmid for obj in objs])
            tbl = PolyTable if typ == 'found' else WaysTable
            res = self.session.execute(select(
                [tbl.c.osm_id, tbl.c.name]).where(tbl.c.osm_id.in_(ids)))
            names = res.fetchall()
            names = dict(names)
            for obj in objs:
                obj._name = names[obj.osmid]
                logging.info(names)

        if typ in self._subO:
            if pullnames and typ in ('found', 'street'):
                get_names_(self._subO[typ])
            return self._subO[typ]
        if typ in ('not found', 'found', 'street'):
            q = self.session.query(Addrobj).filter_by(parentid=self.f_id,
                                                      livestatus=True)
            if ao_stat and config.use_osm:
                q = q.options(joinedload(Addrobj.stat))
            if typ == 'found' and config.use_osm:
                q = q.join(PlaceAssoc)
            elif typ == 'street' and config.use_osm:
                q = q.join(StreetAssoc).distinct(Addrobj.id)
            elif typ == 'not found' and config.use_osm:
                q = q.options(joinedload(Addrobj.street))
                q = q.options(joinedload(Addrobj.place))
                #q = q.filter(~Addrobj.street.any(), ~Addrobj.place.has())
            if typ == 'not found':
                self._subO['street'] = []
                self._subO['found'] = []
            self._subO[typ] = []
            for row in q.all():
                el = fias_AO(row.aoguid, None, None, self.guid, self.session)
                el._fias = row
                #    el._name = row['name']  # TODO :OSM name!
                if typ == 'not found' and config.use_osm:
                    if row.place is not None:
                        self._subO['found'].append(el)
                    elif row.street:
                        self._subO['street'].append(el)
                    else:
                        self._subO[typ].append(el)
                else:
                    self._subO[typ].append(el)
            return self._subO[typ]
        if typ == 'all':
            res = []
            res.extend(self.subO('not found', ao_stat, pullnames))
            res.extend(self.subO('found', ao_stat, pullnames))
            res.extend(self.subO('street', ao_stat, pullnames))
            return res
        if typ.endswith('_b'):
            return self.subB(typ)

    def child_found(self, child, typ):
        if 'not found' in self._subO:
            self._subO['not found'].remove(child)
        if typ in self._subO:
            self._subO[typ].append(child)
