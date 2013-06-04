#!/usr/bin/python
# -*- coding: UTF-8 -*-
from __future__ import division
import psycopg2
import ppygis
import psycopg2.extras
import uuid
psycopg2.extras.register_uuid()
import psycopg2.extensions
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)
import copy
import logging

import mangledb
from config import *
stat_conn = None

socr_cache={}
#with keys socr#aolev

typ_cond = {'all': '',
    'found': 'EXISTS(SELECT aoguid FROM ' + prefix + pl_aso_tbl + ' WHERE aoguid=f.aoguid)',
    'street': 'EXISTS(SELECT aoguid FROM ' + prefix + way_aso_tbl + ' WHERE aoguid=f.aoguid)',
    'not found': None
         }

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, relationship, backref
from sqlalchemy import ForeignKey, Column, Integer, BigInteger, SmallInteger
from fias_db import Base, House, Addrobj


class BuildAssoc(Base):
    __tablename__ = prefix + bld_aso_tbl
    f_id = Column(Integer, ForeignKey("fias_house.f_id"))
    osm_build = Column(BigInteger, primary_key=True)
    point = Column(SmallInteger, primary_key=True)
    fias = relationship("House", backref=backref("osm", uselist=False), uselist=False, lazy='joined')


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
            Session = sessionmaker()
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

    def getguid(self):
        if not self._osmkind or self._osmid == None:
            return
        cur = self.conn.cursor()
        if self._osmkind == 2:
            #'found'
            cur.execute('SELECT aoguid FROM ' + prefix + pl_aso_tbl + ' WHERE osm_admin=%s', (self._osmid,))
        elif self._osmkind == 1:
            cur.execute('SELECT aoguid FROM ' + prefix + way_aso_tbl + ' WHERE aoguid=%s', (self._osmid,))
        else:
            return
        cid = cur.fetchone()
        if cid:
            self._guid = cid[0]

    @property
    def guid(self):
        if self._guid == None:
            self.getguid()
        return self._guid

    def calkind(self):
        cur = self.conn.cursor()
        #'found'
        cur.execute('SELECT osm_admin FROM '+prefix+pl_aso_tbl+' WHERE aoguid=%s',(self.guid,))
        cid=cur.fetchone()
        if cid:
            self._osmid=cid[0]
            self._kind=2
            return 2
        #'street':
        cur.execute('SELECT osm_way FROM '+prefix+way_aso_tbl+' WHERE aoguid=%s',(self.guid,))
        cid=cur.fetchone()
        if cid:
            self._osmid=cid[0]
            self._kind=1
        else:
            self._kind=0

    def getkind(self):
        if self.guid==None:
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
            
    def delkind(self):
        self._kind = None

    kind = property(getkind, setkind, delkind, '''Basic type of object
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
        #cur = self.conn.cursor()
        if self.guid:
            #cur.execute('SELECT parentguid, updatedate, postalcode, code, okato, oktmo, offname, formalname, shortname, aolevel FROM fias_addr_obj WHERE aoguid=%s',(self.guid,))
            #firow=cur.fetchone()
            self._fias = self.session.query(Addrobj).get(self.guid)
        else:
            #firow=[None,None,None,None,None,None,None,None,None,None]
            self._fias = Addrobj({'formalname': 'Россия'})
        if True:  # firow:
            self._parent = self._fias.parentguid  # firow[0]
            #self._fias={}
            #self._fias['updatedate']=firow[1]
            #self._fias['postalcode']=firow[2]
            #self._fias['kladr']=firow[3]
            #self._fias['okato']=firow[4]
            #self._fias['oktmo']=firow[5]
            self._offname = self._fias.offname  # firow[6]
            self._formalname = self._fias.formalname  # firow[7]
            self._shortname = self._fias.shortname  # firow[8]
            self._aolevel = self._fias.aolevel  # firow[9]
        #else:
        #    self._fias = None

    @property
    def fias(self):
        if self._fias == None:
            self.getFiasData()
        return self._fias

    @property
    def offname(self):
        if not hasattr(self,'_offname'):
            self.getFiasData()
        return self._offname
        #return self.fias.offname

    @property
    def formalname(self):
        if not hasattr(self,'_formalname'):
            self.getFiasData()
        return self._formalname
        #return self.fias.formalname

    @property
    def shortname(self):
        if not hasattr(self,'_shortname'):
            self.getFiasData()
        return self._shortname
        #return self.fias.shortname

    @property
    def fullname(self):
        key=u"#".join((self._shortname,str(self._aolevel)))
        if key not in socr_cache:
            cur = self.conn.cursor()
            cur.execute("""SELECT lower(socrname) FROM fias_socr_obj s
            WHERE scname=%s AND level=%s """, (self._shortname, self._aolevel))
            res = cur.fetchone()
            if res:
                socr_cache[key] = res[0]
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
    def parent(self):
        if not hasattr(self,'_parentO'):
            if self._parent == None:
                self.getFiasData()
            self._parentO = fias_AO(self._parent)
        return self._parentO

    @property
    def parentguid(self):
        if self._parent == None:
            self.getFiasData()
        return self._parent

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
                    cur.execute('SELECT COUNT(f.aoguid) FROM fias_addr_obj f INNER JOIN ' + prefix + pl_aso_tbl + ' a ON f.aoguid=a.aoguid WHERE parentguid' + cmpop + '%s', (self.guid,))
                elif typ == 'street':
                    cur.execute('SELECT COUNT(DISTINCT f.aoguid) FROM fias_addr_obj f INNER JOIN ' + prefix + way_aso_tbl + ' a ON f.aoguid=a.aoguid WHERE parentguid' + cmpop + '%s', (self.guid,))
                elif typ == 'all':
                    cur.execute('SELECT COUNT(aoguid) FROM fias_addr_obj WHERE parentguid' + cmpop + '%s', (self.guid,))
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
                cur.execute("SELECT count(distinct(f_id)) FROM fias_house WHERE aoguid=%s", (self.guid,))
                self._stat['all_b'] = cur.fetchone()[0]
            elif typ=='found_b':
                if self.stat('all_b') == 0 or self.kind == 0:
                    self._stat['found_b']=0
                else:
                    cur.execute("SELECT count(distinct(f.f_id)) FROM fias_house f, " + prefix + bld_aso_tbl + " o WHERE f.aoguid=%s AND f.f_id=o.f_id",(self.guid,))
                    self._stat['found_b'] = cur.fetchone()[0]

    def CalcRecStat(self, typ, savemode=1):
        res = self.stat(typ[:-2])
        for ao in self.subAO('found'):
            res += fias_AONode(ao).stat(typ, savemode)
        for ao in self.subAO('street'):
            res += ao.stat(typ[:-2])
        for ao in self.subAO('not found'):
            res += fias_AONode(ao).stat(typ, savemode)
        self._stat[typ] = res

    def pullstat(self, row=None):
        '''Pull stat info from row of dictionary-like cursor'''
        #self._stat = {}
        if row == None:
            if self.guid == None:
                return
            cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute('SELECT * FROM fiosm_stat WHERE aoguid=%s', (self.guid, ))
            row = cur.fetchone()
            if row == None:
                return

        for item in ('all', 'found', 'street', 'all_b', 'found_b'):
            value = row.get(item if item != 'all' else 'ao_all')
            if value != None:
                self._stat[item] = value
            value = row.get(item + '_r')
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
            self.pullstat()
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
        stat = copy.copy(self._stat)
        stat['guid'] = self.guid
        a = ('all' in stat) and ('found' in stat) and ('street' in stat)
        b = ('all_b' in stat) and ('found_b' in stat)
        ar = ('all_r' in stat) and ('found_r' in stat) and ('street_r' in stat)
        br = ('all_b_r' in stat) and ('found_b_r' in stat)
        f = mode == 2 and a and b and ar and br
        self._stat = {}
        #self.conn.autocommit = False
        stat_cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        stat_cur.execute('SELECT * FROM fiosm_stat WHERE aoguid=%s', (self.guid,))
        row = stat_cur.fetchone()
        if row:
            self.pullstat(row)
        elif a or b or ar or br:
            stat_cur.execute('INSERT INTO fiosm_stat (aoguid) VALUES (%s)', (self.guid,))
        else:
            del stat['guid']
            self._stat = stat
            return

        if (mode == 0 and a) or (mode == 1 and a and b) or f:
            if stat['all'] != self._stat.get('all') or stat['found'] != self._stat.get('found') or stat['street'] != self._stat.get('street'):
                stat_cur.execute('UPDATE fiosm_stat SET ao_all=%(all)s, found=%(found)s, street=%(street)s WHERE aoguid = %(guid)s', stat)
        if (mode == 0 and b) or (mode == 1 and a and b) or f:
            if stat['all_b'] != self._stat.get('all_b') or stat['found_b'] != self._stat.get('found_b'):
                stat_cur.execute('UPDATE fiosm_stat SET all_b=%(all_b)s, found_b=%(found_b)s WHERE aoguid = %(guid)s', stat)

        if (mode == 0 and ar) or (mode == 1 and ar and br) or f:
            if stat['all_r'] != self._stat.get('all_r') or stat['found_r'] != self._stat.get('found_r') or stat['street_r'] != self._stat.get('street_r'):
                stat_cur.execute('UPDATE fiosm_stat SET all_r=%(all_r)s, found_r=%(found_r)s, street_r=%(street_r)s WHERE aoguid = %(guid)s', stat)

        if (mode == 0 and br) or (mode == 1 and ar and br) or f:
            if stat['all_b_r'] != self._stat.get('all_b_r') or stat['found_b_r'] != self._stat.get('found_b_r'):
                stat_cur.execute('UPDATE fiosm_stat SET all_b_r=%(all_b_r)s, found_b_r=%(found_b_r)s WHERE aoguid = %(guid)s', stat)

        #self.conn.commit()
        #self.conn.autocommit = True
        del stat['guid']
        self._stat = stat

    @property
    def name(self):
        '''Name of object as on map'''
        if self.guid == None:
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
            cur_.execute("SELECT St_Buffer(ST_Union(w.way),2000) FROM "+prefix+ways_table+" w, "+prefix+way_aso_tbl+" s WHERE s.osm_way=w.osm_id AND s.aoguid=%s",(self.guid,))
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

    def subAO(self, typ, need_stat=True):
        if typ in self._subO:
            return self._subO[typ]

        #Voids by kind
        if self.kind != 2 and typ != 'not found':
            return []
        cmpop = ' is ' if self.guid == None else ' = '
        if need_stat:
            stat_join = "LEFT JOIN fiosm_stat s ON f.aoguid=s.aoguid"
            stat_sel = ", s.*"
        else:
            stat_join = ""
            stat_sel = ""

        fias_sel = "f.formalname, f.shortname, f.offname, f.aolevel"
        #make request
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        if typ == 'found':
            osmid = 'osm_admin'
            cur.execute('SELECT f.aoguid, ' + fias_sel + ', a.' + osmid + ', o.name' + stat_sel + ' FROM fias_addr_obj f INNER JOIN ' + prefix + pl_aso_tbl + ' a ON f.aoguid=a.aoguid INNER JOIN ' + prefix + poly_table + ' o ON a.' + osmid + '=o.osm_id ' + stat_join + ' WHERE parentguid' + cmpop + '%s', (self.guid,))
            kind = 2
        elif typ == 'street':
            kind = 1
            osmid = 'osm_way'
            cur.execute('SELECT DISTINCT ON(f.aoguid) f.aoguid,' + fias_sel + ', a.' + osmid + ', o.name' + stat_sel + ' FROM fias_addr_obj f INNER JOIN ' + prefix + way_aso_tbl + ' a ON f.aoguid=a.aoguid INNER JOIN ' + prefix + ways_table + ' o ON a.' + osmid + '=o.osm_id ' + stat_join + ' WHERE parentguid' + cmpop + '%s', (self.guid,))
        elif typ == 'not found':
            kind = 0
            osmid = None
            cur.execute('SELECT f.aoguid, ' + fias_sel + stat_sel + ' FROM fias_addr_obj f ' + stat_join + ' WHERE NOT(' + typ_cond['found'] + ' OR ' + typ_cond['street'] + ') AND parentguid' + cmpop + '%s', (self.guid,))
        else:
            return []
        self._subO[typ] = []
        for row in cur.fetchall():
            el = fias_AO(row[0], kind, row[osmid] if osmid else None, self.guid, self.conn, self.session)
            el._formalname = row['formalname']
            el._offname = row['offname']
            el._shortname = row['shortname']
            el._aolevel = row['aolevel']
            el.pullstat(row)
            if kind:
                el._name = row['name']
            self._subO[typ].append(el)
        return self._subO[typ]

    def subO(self, typ):
        '''List of subelements'''
        if typ in self._subO:
            return self._subO[typ]

        if typ in ('not found', 'found', 'street'):
            return self.subAO(typ)
        if typ == 'all':
            res = []
            res.extend(self.subO('found'))
            res.extend(self.subO('street'))
            res.extend(self.subO('not found'))
            return res

        if typ == 'all_b':
            self._subO[typ] = self.session.query(House).filter_by(aoguid=self.guid).all()
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
