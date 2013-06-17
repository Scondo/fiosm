#!/usr/bin/python
# -*- coding: UTF8 -*-
from __future__ import division
import xml.parsers.expat
import datetime
import logging
from urllib import urlretrieve, urlcleanup
from os.path import exists
import rarfile
updating = ("normdoc", "addrobj", "socrbase", "house")
import fias_db
from fias_db import House, HouseFuture, Addrobj
import uuid
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
Session = sessionmaker()
session = Session()
upd = False
now_row = 0


class FiasFiles(object):
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(FiasFiles, cls).__new__(cls)
            self = cls.instance
            self.full_file = None
            self.full_ver = None
            self.full_temp = None
            self.fias_list = {}
            try:
                from pysimplesoap import client
                client.TIMEOUT = None
                fias = client.SoapClient(wsdl="http://fias.nalog.ru/WebServices/Public/DownloadService.asmx?WSDL", trace=False)
                fias_list_raw = fias.GetAllDownloadFileInfo()
                if fias_list_raw and 'GetAllDownloadFileInfoResult' in fias_list_raw:
                    for it in fias_list_raw['GetAllDownloadFileInfoResult']:
                        one = it['DownloadFileInfo']
                        ver = session.query(fias_db.Versions).get(one['VersionId'])
                        if ver is None:
                            ver = fias_db.Versions(one['VersionId'])
                            session.add(ver)
                        ver.dumpdate = datetime.datetime.strptime(one['TextVersion'][-10:], "%d.%m.%Y").date()
                        del one['VersionId']
                        self.fias_list[ver.ver] = one
                    session.commit()
            except:
                pass
        return cls.instance

    def maxver(self):
        return max(self.fias_list.iterkeys())

    def get_fullarch(self):
        if self.full_file == None or not exists(self.full_file):
            urlretrieve(self.fias_list[self.maxver()]['FiasCompleteXmlUrl'], self.full_file)
            self.full_ver = self.maxver()

    def get_fullfile(self, table):
        self.get_fullarch()
        arch = rarfile.RarFile(self.full_file)
        for filename in arch.namelist():
            if filename[3:].lower().startswith(table + '_'):
                fdate = datetime.datetime.strptime(filename.split("_")[2], "%Y%m%d").date()
                if self.full_ver is None:
                    rec = session.query(fias_db.Versions).filter_by(date=fdate).one()
                    if rec is None:
                        self.full_ver = 0  # TODO: search by dumpdate
                    else:
                        self.full_ver = rec.ver
                else:
                    try:
                        rec = session.query(fias_db.Versions).get(self.full_ver)
                        rec.date = fdate
                    except:
                        pass
                return (arch.open(filename), self.full_ver)

    def get_updfile(self, table, ver):
        archfile = urlretrieve(self.fias_list[ver]['FiasDeltaXmlUrl'])
        arch = rarfile.RarFile(archfile)
        for filename in arch.namelist():
            if filename.lower().beginswith(table):
                #Get and save date
                try:
                    rec = session.query(fias_db.Versions).get(ver)
                    rec.date = datetime.datetime.strptime(filename.split("_")[3], "%Y%m%d").date()
                except:
                    pass
                return arch.open(filename)

    def __del__(self):
        urlcleanup()


class GuidId(object):
    def __init__(self, guidn, record):
        self.cache = {}
        self.guidn = guidn
        if guidn in record.__table__.primary_key:
            self.fast = 1
        elif 'id' in record.__table__.primary_key:
            self.fast = 2
        else:
            self.fast = 0
        self.record = record
        self.local = None

    def chklocal(self):
        if session.query(self.record).count() == 0:
            self.local = True
            self.max = 0
        else:
            self.local = False

    def getrec(self, guid):
        if self.fast == 1:
            return session.query(self.record).get(guid)
        elif self.fast == 2 and guid in self.cache:
            return session.query(self.record).get(self.cache[guid])
        else:
            return session.query(self.record).filter_by(**{self.guidn: guid}).first()

    def getid(self, guid):
        if guid not in self.cache:
            if self.local:
                idO = None
            elif self.local == False:
                idO = self.getrec(guid)
            elif self.local is None:
                self.chklocal()
                return self.getid(guid)
            else:
                raise AssertionError('wrong cache state')

            if idO == None:
                idO = self.record({self.guidn: guid})
                session.add(idO)
                if self.local:
                    self.max += 1
                    idO.id = self.max
                else:
                    session.commit()
            self.cache[guid] = idO.id

        return self.cache[guid]

    def pushrec(self, dic, cache=True, cmp=None):
        guid = dic[self.guidn]
        if self.local and guid not in self.cache:
            self.max += 1
            dic["id"] = self.max
            idO = None
        elif self.local is None:
            self.chklocal()
            return self.pushrec(dic)
        else:
            idO = self.getrec(guid)

        if idO == None:
            idO = self.record(dic)
            session.add(idO)
            if not self.local:
                #In non-local DB we must commit to get id
                session.commit()
        else:
            if cmp is None or cmp(dic, idO):
                idO.fromdic(dic)
        if cache:
            self.cache[guid] = idO.id
        return idO.id


NormdocGuidId = GuidId('normdocid', fias_db.Normdoc)
AoGuidId = GuidId('aoguid', fias_db.Addrobj)


def addr_cmp(dic, rec):
    if rec.aoid is None:
        #Dummy always should be replaced
        return True
    if dic['STARTDATE'] > dic['ENDDATE']:
        logging.warn("Crazy dates")
        logging.warn(dic)
    if dic['STARTDATE'] > rec.startdate or dic['ENDDATE'] > rec.enddate:
        return True


def normdoc_row(name, attrib):
    global now_row
    if name == "NormativeDocument":
        now_row += 1
        if now_row % 100000 == 0:
            print now_row
            session.commit()
        docid = uuid.UUID(attrib.pop('NORMDOCID'))
        attrib['normdocid'] = docid
        NormdocGuidId.pushrec(attrib)


def socr_obj_row(name, attrib):
    if name == "AddressObjectType":
        socr = fias_db.Socrbase(attrib)
        session.add(socr)


def addr_obj_row(name, attrib):
    global now_row, pushed_rows, upd
    if name == 'Object':
        #if upd:
        #    session.query(fias_db.Addrobj).filter_by(aoid=attrib['AOID']).delete()

        if 'NEXTID' in attrib:
            attrib['LIVESTATUS'] = '0'
            #return
        #On first pass we can skip all dead, on update they will disable current
        if not upd and attrib.get('LIVESTATUS', '0') != '1':
            return

        if 'NORMDOC' in attrib:
            docid = uuid.UUID(attrib['NORMDOC'])
            attrib['NORMDOC'] = NormdocGuidId.getid(docid)
        ed = attrib.pop('ENDDATE').split('-')
        enddate = datetime.date(int(ed[0]), int(ed[1]), int(ed[2]))
        sd = attrib.pop('STARTDATE').split('-')
        startdate = datetime.date(int(sd[0]), int(sd[1]), int(sd[2]))
        attrib['ENDDATE'] = enddate
        attrib['STARTDATE'] = startdate
        attrib['aoguid'] = uuid.UUID(attrib.pop('AOGUID'))
        if 'PARENTGUID' in attrib:
            parentid = uuid.UUID(attrib.pop('PARENTGUID'))
            attrib['parentid'] = AoGuidId.getid(parentid)
        AoGuidId.pushrec(attrib, cmp=addr_cmp)

        now_row += 1
        if now_row % 20000 == 0:
            print now_row
            session.commit()

#HouseGuidId = GuidId('houseguid', HouseFMeta)

"""
def put_house(dic, sd, ed, guid):
    meta = {"STARTDATE": sd,
            "ENDDATE": ed,
            "houseguid": guid,
            "HOUSEID": dic.pop("HOUSEID")}
    f_id = HouseGuidId.pushrec(meta, False)
    if HouseGuidId.local:
        rec = None
    else:
        rec = session.query(fias_db.House).get(f_id)

    if rec == None:
        dic['f_id'] = f_id
        rec = fias_db.House(dic)
        session.add(rec)
    else:
        rec.fromdic(dic)
"""

def house_row(name, attrib):
    global upd, now_row
    if name == 'House':
        now_row += 1
        if now_row % 100000 == 0:
            print now_row
            session.commit()

        if upd:
            session.query(fias_db.House).filter_by(houseid=attrib['HOUSEID']).delete()

        ed = attrib.pop('ENDDATE').split('-')
        enddate = datetime.date(int(ed[0]), int(ed[1]), int(ed[2]))
        if enddate < datetime.date.today():
            return

        del attrib['COUNTER']
        if 'NORMDOC' in attrib:
                docid = uuid.UUID(attrib['NORMDOC'])
                attrib['NORMDOC'] = NormdocGuidId.getid(docid)
        sd = attrib.pop('STARTDATE').split('-')
        startdate = datetime.date(int(sd[0]), int(sd[1]), int(sd[2]))
        guid = uuid.UUID(attrib.pop("HOUSEGUID"))
        attrib['ENDDATE'] = enddate
        attrib['STARTDATE'] = startdate
        attrib['HOUSEGUID'] = guid
        if startdate > datetime.date.today():
            hous = fias_db.HouseFuture(attrib)
        else:
            attrib['ao_id'] = AoGuidId.getid(uuid.UUID(attrib.pop('AOGUID')))
            hous = fias_db.House(attrib)
        session.add(hous)


def UpdateTable(table, fil, engine=None):
    global upd, pushed_rows, now_row
    if fil == None:
        return
    p = xml.parsers.expat.ParserCreate()
    now_row = 0
    print "start import " + table
    if table == 'addrobj':
        if not upd:
            Addrobj.__table__.drop(engine)
            Addrobj.__table__.create(engine)
        p.StartElementHandler = addr_obj_row
    elif table == 'socrbase':
        session.query(fias_db.Socrbase).delete()
        p.StartElementHandler = socr_obj_row
    elif table == 'normdoc':
        p.StartElementHandler = normdoc_row
    elif table == 'house':
        if not upd:
            House.__table__.drop(engine)
            House.__table__.create(engine)
            HouseFuture.__table__.drop(engine)
            HouseFuture.__table__.create(engine)
        p.StartElementHandler = house_row
    p.ParseFile(fil)
    session.commit()
    print table + " imported"

import argparse
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reader of FIAS into database")
    parser.add_argument("--fullfile")
    parser.add_argument("--fullver", type=int)
    args = parser.parse_args()
    from config import conn_par
    engine = create_engine("postgresql://{user}:{pass}@{host}/{db}".format(**conn_par), echo=False)
    Session.configure(bind=engine)
    session = Session()
    fias_db.Base.metadata.create_all(engine)
    fias = FiasFiles()
    fias.full_file = args.fullfile
    fias.full_ver = args.fullver

    sess = Session()
    minver = None
    for tabl in updating:
        my = sess.query(fias_db.TableStatus).filter_by(tablename=tabl).first()
        if my == None:
            full = FiasFiles().get_fullfile(tabl)
            if full != None:
                upd = False
                UpdateTable(tabl, full[0], engine)
                my = fias_db.TableStatus(tabl, full[1])
                sess.add(my)
                sess.commit()

        if my != None and (minver == None or minver > my.ver):
            minver = my.ver
    upd = True
    for ver in range(minver + 1, FiasFiles().maxver() + 1):
        for tabl in updating:
            my = sess.query(fias_db.TableStatus).filter_by(tablename=tabl).first()
            if my.ver < ver:
                UpdateTable(tabl, FiasFiles().get_updfile(tabl, ver))
                my.ver = ver
                sess.commit()
