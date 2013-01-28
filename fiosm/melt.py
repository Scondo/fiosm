#!/usr/bin/python
# -*- coding: UTF-8 -*-
from __future__ import division

import logging
import psycopg2
import ppygis
import psycopg2.extras
import uuid
psycopg2.extras.register_uuid()
import psycopg2.extensions
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

import mangledb
from config import *
conn=psycopg2.connect(connstr)
conn.autocommit=True
stat_conn=psycopg2.connect(connstr,async=True)
psycopg2.extras.wait_select(stat_conn)
stat_cur=stat_conn.cursor()

socr_cache={}
#with keys socr#aolev
    
typ_cond={}
def InitCond():
    global typ_cond,typ_b_cond
    typ_cond={'all':'',
          'found':'EXISTS(SELECT aoguid FROM '+prefix+pl_aso_tbl+' WHERE aoguid=fias_addr_obj.aoguid)',
          'street':'EXISTS(SELECT aoguid FROM '+prefix+way_aso_tbl+' WHERE aoguid=fias_addr_obj.aoguid)',
          'not found':None
         }

 
InitCond()
    
def GetAreaList(parentguid,typ,count=False):
    cur_=conn.cursor()
    if typ=='not found':
        type_cond="NOT("+" OR ".join(filter(None,typ_cond.values()))+")"
    elif typ_cond.has_key(typ):
        type_cond=typ_cond[typ]
#    else:
#        return []
    
    if type_cond<>"":
        type_cond=" AND "+type_cond
    what='Count(aoguid)' if count else 'aoguid'
    if parentguid=='' or (parentguid is None):
        cur_.execute("SELECT "+what+" FROM fias_addr_obj WHERE parentguid is Null"+type_cond)
    else:
        cur_.execute("SELECT "+what+" FROM fias_addr_obj WHERE parentguid=%s"+type_cond,(parentguid,))
    
    res=cur_.fetchone()
    while res:
        yield res[0]
        res=cur_.fetchone()

def SaveAreaStat(guid,stat):
    if guid==None :
        return
    psycopg2.extras.wait_select(stat_conn)
    stat['guid']=guid
    stat_cur.execute('''INSERT INTO fiosm_stat (ao_all, found, street, all_b, found_b,aoguid) 
    VALUES (%(all)s , %(found)s , %(street)s , %(all_b)s , %(found_b)s , %(guid)s) ''',stat)

class fias_AO(object):
    def __init__(self,guid,kind=None,osmid=None,parent=None):
        if guid=="":
            guid=None
        self.guid=guid
        self.setkind(kind)
        if osmid:
            self._osmid=osmid
        if parent:
            self._parent=parent
        self._subs={}

    
    def calkind(self):
        cur=conn.cursor()
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
        return self._kind
    
    def getkind(self):
        if self.guid==None:
            return 2    
        if not hasattr(self,'_kind') or self._kind==None:
            return self.calkind()
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
        del self._kind
        
    kind=property(getkind,setkind,delkind,'''Basic type of object
        0-not found
        1-street
        2-found as area
        ''')
    
    def getFiasData(self):
        cur=conn.cursor()
        if self.guid:
            cur.execute('SELECT parentguid, updatedate, postalcode, code, okato, oktmo, offname, formalname,shortname,aolevel FROM fias_addr_obj WHERE aoguid=%s',(self.guid,))
            firow=cur.fetchone()
        else:
            firow=[None,None,None,None,None,None,None,None,None]
        if firow:
            self._parent=firow[0]
            self._fias={}
            self._fias['updatedate']=firow[1]
            self._fias['postalcode']=firow[2]
            self._fias['kladr']=firow[3]
            self._fias['okato']=firow[4]
            self._fias['oktmo']=firow[5]
            self._offname=firow[6]
            self._formalname=firow[7]
            self._shortname=firow[8]
            self._aolevel=firow[9]
            self._is=True
        else:
            self._is=False
        
    @property
    def fias(self):
        if not hasattr(self,'_fias'):
            self.getFiasData()
        return self._fias
    @property
    def offname(self):
        if not hasattr(self,'_offname'):
            self.getFiasData()
        return self._offname
    @property
    def formalname(self):
        if not hasattr(self,'_formalname'):
            self.getFiasData()
        return self._formalname
    @property
    def shortname(self):
        if not hasattr(self,'_shortname'):
            self.getFiasData()
        return self._shortname
    
    @property
    def fullname(self):
        key=u"#".join((self._shortname,str(self._aolevel)))
        if key in socr_cache:
            return socr_cache[key]
        cur_=conn.cursor()
        cur_.execute("""SELECT lower(socrname) FROM fias_socr_obj s
        WHERE scname=%s AND level=%s """,(self._shortname,self._aolevel))
        res=cur_.fetchone()
        if res:
            socr_cache[key]=res[0]
            return res[0]
        
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
            if not hasattr(self,'_parent'):
                self.getFiasData()
            if self._parent==None:
                return self
            self._parentO=fias_AO(self._parent)
        return self._parentO
    
    @property
    def isok(self): 
        if not hasattr(self,'_is'):
            self.getFiasData()
        return self._is
    
    def CalcAreaStat(self,force=False):
  
        elem={}
        for typ_ in ('all','found','street'):
            if not force and elem.has_key(typ_):
                pass
            elif not force and self._subs.has_key(typ_):
                elem[typ_]=len(self._subs[typ_])
            elif (elem.has_key('all') and not elem['all']) or (self.kind==0 and typ_=='found') or (self.kind<2 and typ_=='street'):
                elem[typ_]=0
            else:
                elem[typ_]=GetAreaList(self.guid,typ_,True).next()

        cur_=conn.cursor()
        if self.guid==None or self.kind==0:
            elem['all_b']=0
        elif force or (not 'all_b' in elem):
            cur_.execute("SELECT count(distinct(houseguid)) FROM fias_house WHERE aoguid=%s",(self.guid,))
            elem['all_b']=cur_.fetchone()[0]
        
        if not elem['all_b']:
            elem['found_b']=0
        elif not force and 'found_b' in self._subs:
            elem['found_b']=len(self._subs['found_b'])
        elif force or not 'found_b' in elem:
            cur_.execute("SELECT count(distinct(f.houseguid)) FROM fias_house f, "+prefix+bld_aso_tbl+" o WHERE f.aoguid=%s AND f.houseguid=o.aoguid",(self.guid,))
            elem['found_b']=cur_.fetchone()[0]
        
        return elem

    @property
    def stat(self):
        '''Statistic of childs for item'''
        if hasattr(self,'stat_'):
            res=self.stat_
        else:
            if self.guid==None:
                res=None
            else:
                psycopg2.extras.wait_select(stat_conn)
                stat_cur.execute('SELECT ao_all, found, street, all_b, found_b FROM fiosm_stat WHERE aoguid=%s',(self.guid,))
                psycopg2.extras.wait_select(stat_conn)
                res=stat_cur.fetchone()
                
            if res==None:
                res=self.CalcAreaStat()
                SaveAreaStat(self.guid,res)
                self.stat_=res
            else:
                res={'all':res[0],
                 'found':res[1],
                 'street':res[2],
                 'all_b':res[3],
                 'found_b':res[4]
                 }
                self.stat_=res      
        res['not found']=res['all']-res['found']-res['street']
        res['not found_b']=res['all_b']-res['found_b']
        return res
    
    @property
    def statR(self):
        res={}
        for ao in self.subs('found'):
            res+=fias_AO(ao,2).statR()
        for ao in self.subs('street'):
            res+=fias_AO(ao,1).stat()
        for ao in self.subs('not found'):
            res+=fias_AO(ao,0).stat()
        res+=self.stat()
        return res
        
    @property
    def name(self):
        '''Name of object as on map'''
        if self.guid==None: 
            return u"Россия"
        if hasattr(self,'_name'):
            return self._name
        
        if self.kind==2:
            cur_=conn.cursor() 
            cur_.execute('SELECT name FROM '+prefix+poly_table+' o, '+prefix+pl_aso_tbl+' a WHERE a.aoguid=%s AND o.osm_id=a.osm_admin ',(self.guid,))
            name=cur_.fetchone()
            if name:
                self._name=name[0]
                return name[0]   
        self._name=self.names().next()
        return self._name

    @property
    def osmid(self):
        if not hasattr(self,'_osmid'):
            if not self.calkind():
                return None
            #and if we have kind other than 'not found' then we 
            #receive osmid while we calculate
        return self._osmid
          
    @property
    def geom(self):
        '''Geometry where buildings can be'''
        if not self.guid:
            return None
        if hasattr(self,'_geom'):
            return self._geom
        cur_=conn.cursor()
        if self.kind==2 and self.osmid<>None:
            cur_.execute("SELECT way FROM "+prefix+poly_table+" WHERE osm_id=%s",(self.osmid,))
            self._geom=cur_.fetchone()[0]
        elif self.kind==1:
            cur_.execute("SELECT St_Buffer(ST_Union(w.way),1000) FROM "+prefix+ways_table+" w, "+prefix+way_aso_tbl+" s WHERE s.osm_way=w.osm_id AND s.aoguid=%s",(self.guid,))
            self._geom=cur_.fetchone()[0]
        else:
            return None
        return self._geom
    
    def builds(self,point):
        point_=1 if point else 0
        cur_=conn.cursor()
        cur_.execute("SELECT o.osm_build, o.aoguid FROM fias_house f, "+prefix+bld_aso_tbl+" o WHERE f.houseguid=o.aogiid AND f.aoguid=%s AND o.point=%s",(self.guid,point_))  
        res={}
        for it in cur_.fetchall():
            res[it[0]]=it[1]
        return res
            
    def subs(self,typ_):
        '''Return set of subelements'''
        #cached value
        if self._subs.has_key(typ_):
            return self._subs[typ_]
        #calculable values
        if self.kind==0 and (typ_=='found_b' or typ_=='found' or typ_=='street'):
            return set()
        if self.kind==1 and (typ_=='found' or typ_=='street'):
            return set()
        if typ_=='not found_b':
            return self.subs('all_b')-self.subs('found_b')
        if typ_=='not found':
            return self.subs('all')-self.subs('found')-self.subs('street')
        #make request
        cur_=conn.cursor()
        if typ_=='all_b':
            cur_.execute("SELECT DISTINCT(houseguid) FROM fias_house WHERE aoguid=%s",(self.guid,))
            self._subs['all_b']=set([it[0] for it in cur_.fetchall()])
        elif typ_=='found_b':
            self._subs['found_b']=set(self.builds(1).values)+set(self.builds(0).values)
        elif typ_cond.has_key(typ_):
            self._subs[typ_]=set(GetAreaList(self.guid,typ_))
        
        return self._subs[typ_]
        
    def move_sub(self,guid,tgt):
        if tgt.endswith('_b') and typ_b_cond.has_key(tgt[:-2]):
            if self._subs.has_key('found_b'):
                self._subs['found_b'].discard(guid)

        if typ_cond.has_key(tgt):
            if self._subs.has_key('found'):
                self._subs['found'].discard(guid)
            if self._subs.has_key('street'):
                self._subs['street'].discard(guid)

        if self._subs.has_key(tgt):
            self._subs[tgt].add(guid)