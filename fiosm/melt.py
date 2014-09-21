#!/usr/bin/python
# -*- coding: UTF-8 -*-
from __future__ import division
from config import *
if use_osm:
    import psycopg2
    import ppygis
    import psycopg2.extensions
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)
else:
    psycopg2 = None
#import copy
import logging
import nice_street

socr_cache = {}
#with keys socr#aolev

typ_cond = {'all': '',
    'found': 'EXISTS(SELECT ao_id FROM ' + prefix + pl_aso_tbl + ' WHERE ao_id=f.id)',
    'street': 'EXISTS(SELECT ao_id FROM ' + prefix + way_aso_tbl + ' WHERE ao_id=f.id)',
    'not found': None
         }

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import relationship, backref, joinedload
from sqlalchemy import ForeignKey, Column, Integer, BigInteger, SmallInteger
from fias_db import Base, Socrbase, House, Addrobj, GUID

if use_osm:
    class Statistic(Base):
        __tablename__ = 'fiosm_stat'
        ao_id = Column(Integer, ForeignKey("fias_addr_obj.id"), primary_key=True)
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

        def fromdic(self, dic):
            for it in dic.iteritems():
                setattr(self, 'ao_all' if it[0] == 'all' else it[0], it[1])

        def __init__(self, dic):
            self.fromdic(dic)

    class PlaceAssoc(Base):
        __tablename__ = prefix + pl_aso_tbl
        ao_id = Column(Integer, ForeignKey("fias_addr_obj.id"))
        osm_admin = Column(BigInteger, primary_key=True)
        fias = relationship("Addrobj", backref=backref("place", uselist=False),
                            uselist=False)

        def __init__(self, f_id, osmid):
            self.ao_id = f_id
            self.osm_admin = osmid

    class StreetAssoc(Base):
        __tablename__ = prefix + way_aso_tbl
        ao_id = Column(Integer, ForeignKey("fias_addr_obj.id"))
        osm_way = Column(BigInteger, primary_key=True)
        fias = relationship("Addrobj", backref=backref("street"), uselist=False)

        def __init__(self, f_id, osmid):
            self.ao_id = f_id
            self.osm_way = osmid

    class BuildAssoc(Base):
        __tablename__ = prefix + bld_aso_tbl
        h_guid = Column(GUID, ForeignKey("fias_house.houseguid"),
                        primary_key=False, index=True)
        osm_build = Column(BigInteger, primary_key=True)
        point = Column(SmallInteger, primary_key=True)
        fias = relationship("House",
                            backref=backref("osm", uselist=False, lazy=False),
                            uselist=False, lazy='joined')

        def __init__(self, h_guid, osmid, point):
            self.osm_build = osmid
            self.point = point
            self.h_guid = h_guid


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
if use_osm:
    House.txtkind = property(HouseTXTKind)
    House.osmlink = property(HouseOSMLink)
else:
    House.txtkind = u'ОСМ отключен'
    House.osmlink = u''


class fias_AO(object):
    def __init__(self, guid=None, kind=None, osmid=None, parent=None,
                 conn=None, session=None):
        if not guid:
            guid = None
        if isinstance(guid, Addrobj):
            self._fias = guid
            self._guid = None
        else:
            self._guid = guid
            self._fias = None
        if conn == None and psycopg2:
            #Connection must not be global to be more thread-safe
            self.conn = psycopg2.connect(psy_dsn)
            self.conn.autocommit = True
        else:
            #Yet we can share connection to keep their number low
            self.conn = conn
        if session == None:
            engine = create_engine(al_dsn, pool_size=2)
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
        if not use_osm:
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
            nice = nice_street.nice(self.formalname, self.fias.shortname,
                                    self.fullname, self.kind == 2)
            yield nice[0]
            was = set((nice[0]))
            if len(nice) > 1:
                for name in self.names((nice[1])):
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
        else:
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
        if self._subB is None:
            q = self.session.query(House)
            if use_osm:
                q = q.options(joinedload(House.osm))
            self._subB = q.filter_by(ao_id=self.f_id).all()
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
        if not use_osm:
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
        stat = self.fias.stat
        if stat is None:
            return
        for item in ('all', 'found', 'street', 'all_b', 'found_b'):
            value = getattr(stat, item if item != 'all' else 'ao_all')
            if value != None:
                self._stat[item] = value
            value = getattr(stat, item + '_r')
            if value != None:
                self._stat[item + '_r'] = value

    def stat(self, typ, savemode=0):
        '''Statistic of childs for item'''
        if not use_osm:
            return 0
        #Calculable values
        (r, t0) = ('_r', typ[:-2]) if typ.endswith('_r') else ('', typ)
        (b, t0) = ('_b', t0[:-2]) if t0.endswith('_b') else ('', t0)
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
        if self.fias.stat is None:
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
            if self.conn and self.kind:
                cur = self.conn.cursor()
                if self.kind == 2:
                    cur.execute('SELECT name FROM ' + prefix + poly_table + \
                                '  WHERE osm_id=%s ', (self.osmid,))
                    name = cur.fetchone()
                elif self.kind == 1:
                    cur.execute('SELECT name FROM ' + prefix + ways_table + \
                                '  WHERE osm_id=%s ', (self.osmid,))
                    name = cur.fetchone()
                else:
                    name = None
            else:
                name = None
            if name:
                self._name = name[0]
            else:
                self._name = self.names().next()
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def osmid(self):
        if self._osmid == None:
            if self._kind == 0:
                return None
            #Do not even try if not found
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

    @property
    def geom(self):
        '''Geometry where buildings can be'''
        if not self.guid or not use_osm:
            return None
        if hasattr(self,'_geom'):
            return self._geom
        cur_ = self.conn.cursor()
        if (self.kind == 2) and (self.osmid is not None):
            #cur_.execute("SELECT way FROM "+prefix+poly_table+" WHERE osm_id=%s",(self.osmid,))
            # TODO: add to config if import allow multi-polygon (-G)
            cur_.execute("SELECT ST_Union(way) as way FROM " +\
                         prefix + poly_table + " WHERE osm_id=%s",
                         (self.osmid,))
            self._geom=cur_.fetchone()[0]
        elif self.kind==1:
            cur_.execute("SELECT St_Buffer(ST_Union(w.way),2000) FROM " + prefix + ways_table + " w, " + prefix + way_aso_tbl + " s WHERE s.osm_way=w.osm_id AND s.ao_id=%s", (self.f_id,))
            self._geom=cur_.fetchone()[0]
        else:
            return None
        return self._geom


class fias_AONode(fias_AO):
    def __init__(self, *args, **kwargs):
        if type(args[0]) == fias_AO:
            for it in args[0].__dict__.keys():
                self.__setattr__(it, args[0].__getattribute__(it))
                #setattr = copy(args[0])
            self.__class__ = fias_AONode
        else:
            fias_AO.__init__(self, *args, **kwargs)
        self._subO = {}

    def subO(self, typ, ao_stat=True):
        '''List of subelements'''
        if typ in self._subO:
            return self._subO[typ]
        if typ in ('not found', 'found', 'street'):
            q = self.session.query(Addrobj).filter_by(parentid=self.f_id,
                                                      livestatus=True)
            if ao_stat and use_osm:
                q = q.options(joinedload(Addrobj.stat))
            if typ == 'found' and use_osm:
                q = q.join(PlaceAssoc)
            elif typ == 'street' and use_osm:
                q = q.join(StreetAssoc).distinct(Addrobj.id)
            elif typ == 'not found' and use_osm:
                q = q.options(joinedload(Addrobj.street))
                q = q.options(joinedload(Addrobj.place))
                #q = q.filter(~Addrobj.street.any(), ~Addrobj.place.has())
            if typ == 'not found':
                self._subO['street'] = []
                self._subO['found'] = []
            self._subO[typ] = []
            for row in q.all():
                el = fias_AO(row.aoguid, None, None, self.guid, self.conn,
                             self.session)
                el._fias = row
                #    el._name = row['name']  # TODO :OSM name!
                if typ == 'not found' and use_osm:
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
            res.extend(self.subO('not found', ao_stat))
            res.extend(self.subO('found', ao_stat))
            res.extend(self.subO('street', ao_stat))
            return res
        if typ.endswith('_b'):
            return self.subB(typ)


    def child_found(self, child, typ):
        if 'not found' in self._subO:
            self._subO['not found'].remove(child)
        if typ in self._subO:
            self._subO[typ].append(child)
