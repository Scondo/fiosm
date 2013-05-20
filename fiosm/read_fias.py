#!/usr/bin/python
# -*- coding: UTF8 -*-
from __future__ import division
import xml.parsers.expat
import datetime
from urllib import urlretrieve, urlcleanup
from os.path import exists
import rarfile
updating = ("normdoc", "addrobj", "socrbase", "house")
import fias_db
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
                        ver = one['VersionId']
                        del one['VersionId']
                        self.fias_list[ver] = one
            except:
                pass
        return cls.instance

    def maxver(self):
        return max(self.fias_list.iterkeys())

    def get_fullarch(self):
        if self.full_file == None or not exists(self.full_file):
            urlretrieve(self.fias_list[self.maxver()]['FiasCompleteXmlUrl'], self.full_file)
            self.full_ver = self.maxver()
            # TODO: Get GUID, date and save version

    def get_fullfile(self, table):
        self.get_fullarch()
        arch = rarfile.RarFile(self.full_file)
        for filename in arch.namelist():
            if filename[3:].lower().startswith(table + '_'):
                return (arch.open(filename), self.full_ver)

    def get_updfile(self, table, ver):
        archfile = urlretrieve(self.fias_list[ver]['FiasDeltaXmlUrl'])
        arch = rarfile.RarFile(archfile)
        for filename in arch.namelist():
            if filename.lower().beginswith(table):
                return arch.open(filename)

    def __del__(self):
        urlcleanup()


class GuidId(object):
    def __init__(self, guidn, record):
        self.cache = {}
        self.guidn = guidn
        if guidn in record.__table__.primary_key:
            self.fast = True
        else:
            self.fast = False
        self.record = record
        self.local = None

    def chklocal(self):
        if session.query(self.record).count() == 0:
            self.local = True
            self.max = 0
        else:
            self.local = False

    def getrec(self, guid):
        if self.fast:
            return session.query(self.record).get(guid)
        else:
            return session.query(self.record).filter_by(**{self.guidn: guid}).first()

    def getid(self, guid):
        if guid not in self.cache:
            if self.local:
                idO = None
            elif self.local == False:
                idO = self.getrec(guid)
            elif self.local == None:
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

    def pushrec(self, dic):
        guid = dic[self.guidn]
        if self.local:
            self.max += 1
            dic["id"] = self.max
            idO = None
        elif self.local == None:
            self.chklocal()
            return self.pushrec(dic)
        else:
            idO = self.getrec(guid)
        if idO == None:
            idO = self.record(dic)
            session.add(idO)
            if not self.local:
                session.commit()
        else:
            idO.fromrow(dic)
        self.cache[guid] = idO.id


NormdocGuidId = GuidId('normdocid', fias_db.Normdoc)


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
        if upd:
            session.query(fias_db.Addrobj).filter_by(aoid=attrib['AOID']).delete()

        if 'NEXTID' not in attrib and attrib.pop('LIVESTATUS', '0') == '1':
            if 'NORMDOC' in attrib:
                docid = uuid.UUID(attrib['NORMDOC'])
                attrib['NORMDOC'] = NormdocGuidId.getid(docid)
            aobj = fias_db.Addrobj(attrib)
            session.add(aobj)

        now_row += 1
        if now_row % 10000 == 0:
            print now_row
            session.commit()


def house_row(name, attrib):
    global upd, now_row
    if name == 'House':
        now_row += 1
        if now_row % 100000 == 0:
            print now_row
            session.commit()

        if 'COUNTER' in attrib:
            del attrib['COUNTER']
        if upd and ('HOUSEID' in attrib):
            session.query(fias_db.House).filter_by(houseid=attrib['HOUSEID']).delete()
        if 'ENDDATE' in attrib:
            ed = attrib['ENDDATE'].split('-')
            enddate = datetime.date(int(ed[0]), int(ed[1]), int(ed[2]))
            if enddate < datetime.date.today():
                return
        if 'NORMDOC' in attrib:
                docid = uuid.UUID(attrib['NORMDOC'])
                attrib['NORMDOC'] = NormdocGuidId.getid(docid)
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
            fias_db.Addrobj.__table__.drop(engine)
            fias_db.Addrobj.__table__.create(engine)
        p.StartElementHandler = addr_obj_row
    elif table == 'socrbase':
        session.query(fias_db.Socrbase).delete()
        p.StartElementHandler = socr_obj_row
    elif table == 'normdoc':
        p.StartElementHandler = normdoc_row
    elif table == 'house':
        if not upd:
            fias_db.House.__table__.drop(engine)
            fias_db.House.__table__.create(engine)
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
