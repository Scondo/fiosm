#!/usr/bin/python
# -*- coding: UTF-8 -*-
from __future__ import division
import psycopg2
import ppygis
#import psycopg2.extras
#import uuid
#psycopg2.extras.register_uuid()
import psycopg2.extensions
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)
#import copy
import logging

import mangledb
from config import *
stat_conn = None

socr_cache={}
#with keys socr#aolev

typ_cond = {'all': '',
    'found': 'EXISTS(SELECT ao_id FROM ' + prefix + pl_aso_tbl + ' WHERE ao_id=f.id)',
    'street': 'EXISTS(SELECT ao_id FROM ' + prefix + way_aso_tbl + ' WHERE ao_id=f.id)',
    'not found': None
         }

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, relationship, backref, joinedload, join
from sqlalchemy import ForeignKey, Column, Integer, BigInteger, SmallInteger
from fias_db import Base, Socrbase, House, Addrobj, UUID


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
    fias = relationship("Addrobj", backref=backref("stat", uselist=False), uselist=False)

    def fromdic(self, dic):
        for it in dic.iteritems():
            setattr(self, 'ao_all' if it[0] == 'all' else it[0], it[1])

    def __init__(self, dic):
        self.fromdic(dic)


class PlaceAssoc(Base):
    __tablename__ = prefix + pl_aso_tbl
    ao_id = Column(Integer, ForeignKey("fias_addr_obj.id"), primary_key=True)
    osm_admin = Column(BigInteger)
    fias = relationship("Addrobj", backref=backref("place", uselist=False), uselist=False)

    def __init__(self, f_id, osmid):
        self.ao_id = f_id
        self.osm_admin = osmid


class StreetAssoc(Base):
    __tablename__ = prefix + way_aso_tbl
    ao_id = Column(Integer, ForeignKey("fias_addr_obj.id"), primary_key=True)
    osm_way = Column(BigInteger, primary_key=True)
    fias = relationship("Addrobj", backref=backref("street"), uselist=False)

    def __init__(self, f_id, osmid):
        self.ao_id = f_id
        self.osm_way = osmid


class BuildAssoc(Base):
    __tablename__ = prefix + bld_aso_tbl
    h_guid = Column(UUID(as_uuid=True), ForeignKey("fias_house.houseguid"), primary_key=True)
    osm_build = Column(BigInteger)
    point = Column(SmallInteger)
    fias = relationship("House", backref=backref("osm", uselist=False, lazy=False), uselist=False, lazy='joined')

    def __init__(self, h_guid, osmid, point):
        self.h_guid = h_guid
        self.osm_build = osmid
        self.point = point

def HouseTXTKind(self):
    if self.osm is None:
        return u'нет в ОСМ'
    elif self.osm.point == 1:
        return u'точка'
    elif self.osm.point == 0:
        return u'полигон'
House.txtkind = property(HouseTXTKind)


class fias_AO(object):
    def __init__(self, guid=None, kind=None, osmid=None, parent=None, conn=None, session=None):
        if not guid:
            guid = None
        if conn == None:
            #Connection must not be global to be more thread-safe
            self.conn = psycopg2.connect(connstr)
            self.conn.autocommit = True
        else:
            #Yet we can share connection to keep ttheir number low
            self.conn = conn
        if session == None:
            engine = create_engine("postgresql://{user}:{pass}@{host}/{db}".format(**conn_par), pool_size=2)
            Session = sessionmaker(expire_on_commit=False)
            self.session = Session(bind=engine)
        else:
            self.session = session
        self._guid = guid
        self._osmid = osmid
        self._osmkind = None
        self.setkind(kind)
        self._parent = parent
        self._stat={}
        self._fias = None

    @property
    def guid(self):
        if self._guid == None:
            self._guid = self.fias.aoguid
        return self._guid

    @property
    def f_id(self):
        return self.fias.id

    def calkind(self):
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
    
    def setkind(self,kind):
        if not type(kind) is int:
            if str(kind)=='found':
                self._kind=2
            elif str(kind)=='street':
                self._kind=1
            elif str(kind)=='not found':
                self._kind=0
            else:
                self._kind=None
        else:
            self._kind=kind

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
            self._fias = self.session.query(Addrobj).filter_by(aoguid=self.guid).one()
        elif self._osmid and self._osmkind == 2:
            a = self.session.query(PlaceAssoc).filter_by(osm_admin=self._osmid).first()
            self._fias = a.fias
        elif self._osmid and self._osmkind == 1:
            a = self.session.query(StreetAssoc).filter_by(osm_way=self._osmid).first()
            self._fias = a.fias
        else:
            self._fias = Addrobj({'formalname': 'Россия'})

    @property
    def fias(self):
        if self._fias == None:
            self.getFiasData()
        return self._fias

    @property
    def offname(self):
#        if not hasattr(self,'_offname'):
#            self.getFiasData()
#        return self._offname
        return self.fias.offname

    @property
    def formalname(self):
#        if not hasattr(self,'_formalname'):
#            self.getFiasData()
#        return self._formalname
        return self.fias.formalname

    @property
    def shortname(self):
        #if not hasattr(self,'_shortname'):
        #    self.getFiasData()
        #return self._shortname
        return self.fias.shortname

    @property
    def fullname(self):
        if self.fias.shortname is None:
            return u""
        key = u"#".join((self.shortname, str(self.fias.aolevel)))
        if key not in socr_cache:
            res = self.session.query(Socrbase).filter_by(scname=self.shortname, level=self.fias.aolevel).first()
            if res:
                socr_cache[key] = res.socrname.lower()
            else:
                socr_cache[key] = ""
        return socr_cache[key]

    def names(self,formal=True):
        name_=self.formalname if formal else self.offname
        if mangledb.usable and self.kind!=2:
            mangled=mangledb.db.CheckCanonicalForm(self.shortname+" "+name_)
            if mangled:
                yield mangled[0]
        yield self.fullname+" "+name_
        yield name_+" "+self.fullname
        #As AMDmi3 promises
        #yield self.shortname+" "+name_
        #yield name_+" "+self.shortname
        yield name_

    @property
    def parentguid(self):
        if self._parent == None:
            if self.fias.parent is not None:
                self._parent = self.fias.parent.aoguid
        return self._parent

    @property
    def parent(self):
        if not hasattr(self,'_parentO'):
            self._parentO = fias_AO(self.parentguid)
            if self._fias is not None:
                self._parentO._fias = self.fias.parent
        return self._parentO

    @property
    def isok(self):
        return (self.fias != None) or (self.guid == None)

    def CalcAreaStat(self, typ):
        #check in pulled children
        if hasattr(self, '_subO') and typ in self._subO:
                self._stat[typ] = len(self._subO[typ])
        cur = self.conn.cursor()
        if typ in ('all','found','street'):
            if ('all' in self._stat and self._stat['all']==0) or (self.kind==0 and typ=='found') or (self.kind<2 and typ=='street'):
                self._stat[typ]=0
            else:
                cmpop = ' is ' if self.guid == None else ' = '
                #make request
                if typ == 'found':
                    cur.execute('SELECT COUNT(f.id) FROM fias_addr_obj f INNER JOIN ' + prefix + pl_aso_tbl + ' a ON f.id=a.ao_id WHERE parentid' + cmpop + '%s', (self.fias.id,))
                elif typ == 'street':
                    cur.execute('SELECT COUNT(DISTINCT f.id) FROM fias_addr_obj f INNER JOIN ' + prefix + way_aso_tbl + ' a ON f.id=a.ao_id WHERE parentid' + cmpop + '%s', (self.fias.id,))
                elif typ == 'all':
                    cur.execute('SELECT COUNT(id) FROM fias_addr_obj WHERE parentid' + cmpop + '%s', (self.fias.id,))
                self._stat[typ] = cur.fetchone()[0]

        elif typ.endswith('_b'):
            #no buildings in country
            if self.guid == None:
                self._stat[typ] = 0
                return
            #all building children are easily available from all_b
            if hasattr(self, 'subO'):
                self._stat[typ] = len(self.subO(typ))
                return
            #if not node
            if typ=='all_b':
                cur.execute("SELECT count(distinct(houseguid)) FROM fias_house WHERE ao_id=%s", (self.f_id,))
                self._stat['all_b'] = cur.fetchone()[0]
            elif typ=='found_b':
                if self.stat('all_b') == 0 or self.kind == 0:
                    self._stat['found_b']=0
                else:
                    cur.execute("SELECT count(distinct(f.houseguid)) FROM fias_house f, " + prefix + bld_aso_tbl + " o WHERE f.ao_id=%s AND f.f_id=o.f_id", (self.f_id,))
                    self._stat['found_b'] = cur.fetchone()[0]

    def CalcRecStat(self, typ, savemode=1):
        res = self.stat(typ[:-2])
        for ao in self.subO('found'):
            res += fias_AONode(ao).stat(typ, savemode)
        for ao in self.subO('street'):
            res += ao.stat(typ[:-2])
        for ao in self.subO('not found'):
            res += fias_AONode(ao).stat(typ, savemode)
        self._stat[typ] = res

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
        mode - 0 if have any slice, 1 - if have all not recursive, 2 - if have all
        '''
        if self.guid == None:
            return
        stat = self._stat
        a = ('all' in stat) and ('found' in stat) and ('street' in stat)
        b = ('all_b' in stat) and ('found_b' in stat)
        ar = ('all_r' in stat) and ('found_r' in stat) and ('street_r' in stat)
        br = ('all_b_r' in stat) and ('found_b_r' in stat)
        f = mode == 2 and a and b and ar and br
        statR = self.session.query(Statistic).get(self.f_id)
        if statR is None:
            if a or b or ar or br:
                statR = Statistic({"ao_id": self.f_id})
                self.session.add(statR)
            else:
                return
        if (mode == 0 and a) or (mode == 1 and a and b) or f:
            statR.fromdic(stat)
        if (mode == 0 and b) or (mode == 1 and a and b) or f:
            statR.fromdic(stat)
        if (mode == 0 and ar) or (mode == 1 and ar and br) or f:
            statR.fromdic(stat)
        if (mode == 0 and br) or (mode == 1 and ar and br) or f:
            statR.fromdic(stat)
        self.session.commit()

    @property
    def name(self):
        '''Name of object as on map'''
        if self.guid is None:
            return u"Россия"

        if not hasattr(self, '_name'):
            cur = self.conn.cursor()
            if self.kind == 2:
                cur.execute('SELECT name FROM ' + prefix + poly_table + '  WHERE osm_id=%s ', (self.osmid,))
                name = cur.fetchone()
            elif self.kind == 1:
                cur.execute('SELECT name FROM ' + prefix + ways_table + '  WHERE osm_id=%s ', (self.osmid,))
                name = cur.fetchone()
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
    def geom(self):
        '''Geometry where buildings can be'''
        if not self.guid:
            return None
        if hasattr(self,'_geom'):
            return self._geom
        cur_ = self.conn.cursor()
        if self.kind==2 and self.osmid<>None:
            cur_.execute("SELECT way FROM "+prefix+poly_table+" WHERE osm_id=%s",(self.osmid,))
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
            q = self.session.query(Addrobj).filter_by(parentid=self.f_id, livestatus=True)
            if ao_stat:
                q = q.options(joinedload(Addrobj.stat))
            if typ == 'found':
                q = q.join(PlaceAssoc)
            elif typ == 'street':
                q = q.join(StreetAssoc)
            elif typ == 'not found':
                q = q.options(joinedload(Addrobj.street))
                q = q.options(joinedload(Addrobj.place))
                #q = q.filter(~Addrobj.street.any(), ~Addrobj.place.has())
            if typ == 'not found':
                self._subO['street'] = []
                self._subO['found'] = []
            self._subO[typ] = []
            for row in q.all():
                el = fias_AO(row.aoguid, None, None, self.guid, self.conn, self.session)
                el._fias = row
                #    el._name = row['name']  # TODO :OSM name!
                if typ == 'not found':
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
            res.extend(self.subO('not found'))
            res.extend(self.subO('found'))
            res.extend(self.subO('street'))
            return res

        if typ == 'all_b':
            self._subO[typ] = self.session.query(House).filter_by(ao_id=self.f_id).all()
            return self._subO[typ]

        if typ == 'found_b':
            return filter(lambda h: h.osm is not None, self.subO('all_b'))
        elif typ == 'not found_b':
            return filter(lambda h: h.osm is None, self.subO('all_b'))

    def child_found(self, child, typ):
        if 'not found' in self._subO:
            self._subO['not found'].remove(child)
        if typ in self._subO:
            self._subO[typ].append(child)
