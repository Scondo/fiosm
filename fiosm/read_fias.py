#!/usr/bin/python
from __future__ import division
from config import connstr

import psycopg2
#import uuid
#import psycopg2.extras
#psycopg2.extras.register_uuid()
import xml.parsers.expat
import datetime
#import threading

conn=psycopg2.connect(connstr)#,async=True)
conn.autocommit=True
#psycopg2.extras.wait_select(conn)
cur=conn.cursor()

upd=False

def addr_recreate():
    #psycopg2.extras.wait_select(conn)
    cur.execute("DROP TABLE IF EXISTS fias_addr_obj")
    #psycopg2.extras.wait_select(conn)
    cur.execute("""CREATE TABLE fias_addr_obj(
   formalname CHARACTER VARYING (120), aoguid UUID PRIMARY KEY, parentguid UUID,
   regioncode   CHARACTER (2), autocode CHARACTER (1), areacode CHARACTER (3),
   citycode     CHARACTER (3), ctarcode CHARACTER (3), placecode CHARACTER (3),
   streetcode   CHARACTER (4), extrcode CHARACTER (4), sextcode CHARACTER (3),
   offname      CHARACTER VARYING (120), postalcode integer,
ifnsfl smallint, terrifnsfl smallint, ifnsul smallint, terrifnsul smallint,
   okato bigint,  oktmo integer, updatedate date,
   shortname    CHARACTER VARYING (10),  aolevel      integer,
   aoid UUID, previd UUID, nextid UUID,
   code         CHARACTER VARYING (17), plaincode CHARACTER VARYING (15),
   actstatus    smallint,   centstatus   smallint,   operstatus   smallint,  currstatus  integer,
   startdate date,   enddate  date, normdoc UUID, livestatus   BOOLEAN);""")
    #cur.commit()
    
def socr_recreate():
    #psycopg2.extras.wait_select(conn)
    cur.execute("DROP TABLE IF EXISTS fias_socr_obj")
    #psycopg2.extras.wait_select(conn)
    cur.execute("""CREATE TABLE fias_socr_obj
(level integer, scname CHARACTER VARYING (10), socrname CHARACTER VARYING (50),
 kod_t_st   CHARACTER VARYING (4));

CREATE UNIQUE INDEX fias_socr_obj_level_scname_idx ON fias_socr_obj USING btree (level, scname);
    """)
    #conn.commit()

def house_recreate():
    #psycopg2.extras.wait_select(conn)
    cur.execute("DROP TABLE IF EXISTS fias_house")
    #psycopg2.extras.wait_select(conn)
    # , counter INTEGER
    cur.execute("""CREATE TABLE fias_house(
    postalcode integer,
ifnsfl smallint, terrifnsfl smallint, ifnsul smallint, terrifnsul smallint,
   okato bigint,  oktmo integer, updatedate date,
   housenum CHARACTER VARYING (20), eststatus smallint,
   buildnum CHARACTER VARYING (10), strucnum CHARACTER VARYING (10), strstatus smallint,
   houseguid UUID, aoguid UUID,houseid UUID,
   startdate date,   enddate  date,statstatus smallint, normdoc UUID);""")
    #conn.commit()
    
def dict_to_2str(attrib):
    fields=''
    values=''
    for k in attrib.keys():
        fields=fields+','+k
        values=values+', %('+k+')s'
    fields=fields[1:]
    values=values[1:]
    return (fields,values)

now_row=0
pushed_rows=set()     
def addr_obj_row(name,attrib):
    global now_row,pushed_rows,upd
    if name=='Object':# and attrib['AOID'] in buglist:
        #       ins_addr_row(attrib,False)
        #Simple Version
        if not 'NEXTID' in attrib:
            if 'AOGUID' in attrib:
                if upd or attrib['AOGUID'] in pushed_rows:
                    #psycopg2.extras.wait_select(conn)
                    conn.commit()
                    cur.execute("DELETE FROM fias_addr_obj WHERE aoguid=%s",(attrib['AOGUID'],))
                    conn.commit()
            else:    
                print "Missed AOGuid"
                print attrib
            (fields,values)=dict_to_2str(attrib)
            #psycopg2.extras.wait_select(conn)
            cur.execute("INSERT INTO fias_addr_obj ("+fields+") VALUES ("+values+")",attrib)
            if not upd:
                pushed_rows.add(attrib['AOGUID'])
        else:
            if upd:
                #psycopg2.extras.wait_select(conn)
                conn.commit()
                cur.execute("DELETE FROM fias_addr_obj WHERE aoid=%s",(attrib['AOID'],))
                conn.commit()
        now_row+=1
        if now_row % 10000 == 0:
            print now_row
            conn.commit()
            #print "rows pending: "+str(len(arow_pending))
           
def addr_drop_obsolete():
    #psycopg2.extras.wait_select(conn)
    cur.execute("DELETE FROM fias_addr_obj WHERE NOT (nextid is Null);")

def addr_create_indexes():
    #psycopg2.extras.wait_select(conn)
    cur.execute(#"""CREATE INDEX fias_addr_obj_aoguid_idx ON fias_addr_obj USING btree (aoguid);
"""CREATE INDEX ao_parent_idx ON fias_addr_obj USING btree (parentguid);
CLUSTER fias_addr_obj USING ao_parent_idx;
""")

def house_create_indexes():
    #psycopg2.extras.wait_select(conn)
    cur.execute(#"""CREATE INDEX fias_addr_obj_aoguid_idx ON fias_addr_obj USING btree (aoguid);
"""CREATE INDEX house_parent_idx ON fias_house USING btree (aoguid);
CLUSTER fias_house USING house_parent_idx;
""")
    
def socr_obj_row(name,attrib):
    if name=="AddressObjectType":
        (fields,values)=dict_to_2str(attrib)
        #psycopg2.extras.wait_select(conn)
        cur.execute("INSERT INTO fias_socr_obj ("+fields+") VALUES ("+values+")",attrib)

def house_row(name,attrib):
    global upd,now_row
    now_row+=1
    if now_row % 100000 == 0:
        print now_row
        conn.commit()

    if name=='House':
        #if 'HOUSEID' in attrib:
        #    del attrib['HOUSEID']
        if 'COUNTER' in attrib:
            del attrib['COUNTER']
        
        if upd and ('HOUSEID' in attrib):
            #psycopg2.extras.wait_select(conn)
            conn.commit()
            cur.execute("DELETE FROM fias_house WHERE houseid=%s",(attrib['HOUSEID'],))
            conn.commit()
        if 'ENDDATE' in attrib:
            ed=attrib['ENDDATE'].split('-')
            enddate=datetime.date(int(ed[0]),int(ed[1]),int(ed[2]))
            if enddate<datetime.date.today():
                return
        (fields,values)=dict_to_2str(attrib)
        #psycopg2.extras.wait_select(conn)
        cur.execute("INSERT INTO fias_house ("+fields+") VALUES ("+values+")",attrib)


arow_c=0

import argparse
if __name__=="__main__":
    parser=argparse.ArgumentParser(description="Reader of FIAS for FIOSM validator")
    parser.add_argument("--addrobj",type=file)
    parser.add_argument("--socrbase",type=file)
    parser.add_argument("--house",type=file)
    parser.add_argument("--upd",action='store_true')
    a=''
    #a=a+'--addrobj D:\\GisBuben\\AS_ADDROBJ_20121202.XML '
    a=a+'--socrbase D:\\GisBuben\\AS_SOCRBASE_20121202.XML '
    a=a+'--house D:\\GisBuben\\AS_HOUSE_20121202.XML '
    a=a.split()
    args=parser.parse_args(a)
    upd=args.upd
    if args.addrobj:
        p = xml.parsers.expat.ParserCreate()
        conn.autocommit=False
        if not args.upd:
            addr_recreate()
            conn.commit()
        p.StartElementHandler=addr_obj_row
        #p.StartElementHandler=arow_count
        p.ParseFile(args.addrobj)
        #arow_pend_proc()
        if not args.upd:
            conn.commit()
            addr_create_indexes()
        del p
        conn.commit()
        pushed_rows=set()
        conn.autocommit=True
    if args.socrbase:
        p = xml.parsers.expat.ParserCreate()
        socr_recreate()
        p.StartElementHandler=socr_obj_row
        p.ParseFile(args.socrbase)
        del p
    if args.house:
        p = xml.parsers.expat.ParserCreate()
        nom_row=0
        if not args.upd:
            house_recreate()
        conn.autocommit=False
        p.StartElementHandler=house_row
        p.ParseFile(args.house)
        del p
        if not args.upd:
            conn.commit()
            house_create_indexes()

        conn.commit()
        conn.autocommit=True
    #psycopg2.extras.wait_select(conn)