#!/usr/bin/python
# -*- coding: UTF8 -*-
from __future__ import division
import xml.parsers.expat
import datetime
from datetime import date
import logging
from urllib import urlretrieve, urlcleanup
from urllib2 import URLError
from os.path import exists
import rarfile
import fias_db
from fias_db import House, Addrobj, Normdoc
from uuid import UUID
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def strpdate(string, fmt):
    return datetime.datetime.strptime(string, fmt).date()

today = date.today()

updating = ("normdoc", "addrobj", "socrbase", "house")
Session = sessionmaker()
session = Session()
upd = False
now_row = 0
now_row_ = 0
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

# Very dirty hack
def do_executemany_patched(self, cursor, statement, parameters, context=None):
    ins = False
    if statement[:6].lower() == 'insert':
        p = statement.find('VALUES')
        if p == -1:
            p = statement.find('values')
        if p != -1:
            records_list_template = ','.join(['%s' for t in parameters])
            statement_ = statement[:p + 7] + records_list_template
            cursor.execute(statement_, parameters)
            ins = True
    if not ins:
        cursor.executemany(statement, parameters)

from sqlalchemy.engine.default import DefaultDialect
DefaultDialect.do_executemany = do_executemany_patched

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
                from suds.client import Client
                fias = Client("http://fias.nalog.ru/WebServices/Public/DownloadService.asmx?WSDL")
                fias_list_raw = fias.service.GetAllDownloadFileInfo()
                if fias_list_raw:
                    for it in fias_list_raw.DownloadFileInfo:
                        ver = session.query(fias_db.Versions).\
                                    get(it.VersionId)
                        if ver is None:
                            ver = fias_db.Versions(it.VersionId)
                            session.add(ver)
                        dumpdate = it.TextVersion[-10:].split('.')
                        ver.dumpdate = date(int(dumpdate[2]),
                                            int(dumpdate[1]),
                                            int(dumpdate[0]))
                        self.fias_list[ver.ver] = it
                    session.commit()
            except URLError as e:
                logging.warn(e)
                pass
        return cls.instance

    def maxver(self):
        if self.fias_list:
            return max(self.fias_list.iterkeys())
        else:
            return 0

    def get_fullarch(self):
        if self.full_file == None or not exists(self.full_file):
            self.full_file = urlretrieve(
                            self.fias_list[self.maxver()].FiasCompleteXmlUrl,
                            self.full_file)[0]
            self.full_ver = self.maxver()

    def get_fullfile(self, table):
        self.get_fullarch()
        arch = rarfile.RarFile(self.full_file)
        for filename in arch.namelist():
            if filename[3:].lower().startswith(table + '_'):
                fdate = strpdate(filename.split("_")[2], "%Y%m%d")
                if self.full_ver is None:
                    rec = session.query(fias_db.Versions).\
                            filter_by(date=fdate).first()
                    if rec is None:
                        self.full_ver = 0  # TODO: search by dumpdate
                    else:
                        self.full_ver = rec.ver
                else:
                    try:
                        rec = session.query(fias_db.Versions).\
                                        get(self.full_ver)
                        rec.date = fdate
                    except:
                        pass
                return (arch.open(filename), self.full_ver)

    def get_updfile(self, table, ver):
        archfile = urlretrieve(self.fias_list[ver].FiasDeltaXmlUrl)
        arch = rarfile.RarFile(archfile)
        for filename in arch.namelist():
            if filename.lower().beginswith(table):
                #Get and save date
                try:
                    rec = session.query(fias_db.Versions).get(ver)
                    rec.date = strpdate(filename.split("_")[3], "%Y%m%d")
                except:
                    pass
                return arch.open(filename)

    def __del__(self):
        urlcleanup()


class GuidId(object):
    def __init__(self, guidn, record, use_objcache=False):
        self.cache = {}
        if use_objcache:
            self.objcache = {}
            self.objcache_ = {}
        else:
            self.objcache = None
        self.guidn = guidn
        if guidn in record.__table__.primary_key:
            self.fast = 1
        elif 'id' in record.__table__.primary_key:
            self.fast = 2
        else:
            self.fast = 0
        self.record = record
        self.local = None

    def flush_cache(self):
        self.objcache_ = self.objcache
        self.objcache = {}

    def chklocal(self):
        if session.query(self.record).count() == 0:
            self.local = True
            self.max = 0
        else:
            self.local = False

    def __getrec(self, guid_int):
        if not (self.objcache is None):
            if guid_int in self.objcache:
                return self.objcache[guid_int]
            if guid_int in self.objcache_:
                return self.objcache_[guid_int]
        if self.fast == 1:
            return session.query(self.record).get(UUID(int=guid_int))
        elif self.fast == 2 and guid_int in self.cache:
            return session.query(self.record).get(self.cache[guid_int])
        else:
            return session.query(self.record).\
                filter_by(**{self.guidn: UUID(int=guid_int)}).first()

    def getid(self, guid, create=True):
        if isinstance(guid, UUID):
            guid_int = guid.int
        elif isinstance(guid, basestring):
            guid = UUID(guid)
            guid_int = guid.int
        elif isinstance(guid, long):
            guid_int = guid
            guid = UUID(int=guid_int)
        elif guid is None:
            return None
        else:
            raise TypeError('Use Guid')

        if guid_int not in self.cache:
            if self.local:
                idO = None
            elif self.local is False:
                idO = self.__getrec(guid_int)
            elif self.local is None:  # not initialized
                self.chklocal()  # initialize local state
                return self.getid(guid)  # re-call self
            else:
                raise AssertionError('wrong cache state')

            if idO is None:
                if not create:
                    return None
                idO = self.record()
                setattr(idO, self.guidn, guid)
                session.add(idO)
                if self.local:
                    self.max += 1
                    idO.id = self.max
                else:
                    session.commit()
            self.cache[guid_int] = idO.id

        return self.cache[guid_int]

    def pushrec(self, dic, comp=None):
        guid = dic[self.guidn]
        if self.local and (guid.int not in self.cache):
            self.max += 1
            dic["id"] = self.max
            idO = None
        elif self.local is None:
            self.chklocal()
            return self.pushrec(dic)
        else:
            idO = self.__getrec(guid.int)

        if idO is None:
            idO = self.record(dic)
            session.add(idO)
            if not self.local:
                #In non-local DB we must commit to get id
                session.commit()
        else:
            if (comp is None) or comp(dic, idO):
                del dic[self.guidn]
                idO.fromdic(dic)
        self.cache[guid.int] = idO.id
        if not (self.objcache is None):
            self.objcache[guid.int] = idO


NormdocGuidId = GuidId('normdocid', fias_db.Normdoc)
AoGuidId = GuidId('aoguid', fias_db.Addrobj, True)


def addr_cmp(dic, rec):
    if rec.aoid is None:
        #Dummy always should be replaced
        return True
    if dic['parentid'] != rec.parentid:
        # force save new records
        # if it could be referenced
        session.commit()
    if dic['AOID'] == rec.aoid:
        #New rec is always better
        return True
    if dic['STARTDATE'] > rec.startdate:
        return True
    return False


def normdoc_row(name, attrib):
    global now_row, now_row_
    if name == "NormativeDocument":
        now_row_ += 1
        if now_row_ == 100000:
            now_row = now_row + now_row_
            now_row_ = 0
            logging.info(now_row)
            session.flush()
        attrib['normdocid'] = UUID(attrib.pop('NORMDOCID'))
        NormdocGuidId.pushrec(attrib)


def socr_obj_row(name, attrib):
    if name == "AddressObjectType":
        socr = fias_db.Socrbase(attrib)
        session.add(socr)


def addr_obj_row(name, attrib):
    global upd, now_row, now_row_
    if name == 'Object':
        if 'NEXTID' in attrib:
            #If one have successor - it's dead
            attrib['LIVESTATUS'] = '0'
        if not upd and attrib.get('LIVESTATUS', '0') != '1':
            #On first pass we can skip all dead,
            #on update they will disable current
            return

        if 'NORMDOC' in attrib:
            attrib['NORMDOC'] = NormdocGuidId.getid(UUID(attrib['NORMDOC']))
        ed = attrib.pop('ENDDATE').split('-')
        attrib['ENDDATE'] = date(int(ed[0]), int(ed[1]), int(ed[2]))
        sd = attrib.pop('STARTDATE').split('-')
        attrib['STARTDATE'] = date(int(sd[0]), int(sd[1]), int(sd[2]))
        attrib['aoguid'] = UUID(attrib.pop('AOGUID'))
        attrib['parentid'] = AoGuidId.getid(attrib.pop('PARENTGUID', None))

        AoGuidId.pushrec(attrib, comp=addr_cmp)

        now_row_ += 1
        if now_row_ == 100000:
            now_row = now_row + now_row_
            now_row_ = 0
            logging.info(now_row)
            AoGuidId.flush_cache()
            session.flush()

# Keys are guids, values are update dates
removed_hous = {}
# Keys are guids, values are records
pushed_hous = {}
h_cache = {}
broken_house = frozenset((UUID('ea1e5154-7588-4220-8691-6b63bb93c3d4').int,
                          ))


def house_row(name, attrib):
    global upd, now_row, now_row_, h_cache, pushed_hous, removed_hous

    if name == 'House':
        now_row_ += 1
        if now_row_ == 250000:
            now_row = now_row + now_row_
            now_row_ = 0
            logging.info((now_row, len(h_cache),
                          len(pushed_hous), len(removed_hous)))
            h_cache = {}
            session.flush()
        if upd:
            session.query(House).filter_by(houseid=attrib['HOUSEID']).delete()

        del attrib['COUNTER']
        ed = attrib.pop('ENDDATE').split('-')
        enddate = date(int(ed[0]), int(ed[1]), int(ed[2]))
        sd = attrib.pop('STARTDATE').split('-')
        startdate = date(int(sd[0]), int(sd[1]), int(sd[2]))
        ud = attrib.pop('UPDATEDATE').split('-')
        updatedate = date(int(ud[0]), int(ud[1]), int(ud[2]))
        guid = UUID(attrib.pop("HOUSEGUID"))
        guid_i = guid.int
        rec = pushed_hous.get(guid_i, None)
        strange = startdate >= enddate
        if (not strange) and (enddate < today) and\
                (rec is None or rec.updatedate <= updatedate):
            removed_hous[guid_i] = updatedate
            if rec is not None:
                pushed_hous.pop(guid_i)
                session.delete(rec)
            return
        if guid_i in removed_hous:
            if updatedate <= removed_hous[guid_i]:
                return
            else:
                removed_hous.pop(guid_i)

        normdoc = attrib.pop('NORMDOC', None)
        attrib['ao_id'] = AoGuidId.getid(attrib.pop('AOGUID'), False)
        if attrib['ao_id'] is None:
            return

        if (strange or (startdate >= today) or upd) and (rec is None):
            # If house is 'future' check if that already in DB
            # Other houses should not be in DB:
            # 'past' houses are skipped and current is only one
            if guid_i in h_cache:
                rec = h_cache[guid_i]
            else:
                rec = session.query(House).get(guid)

        if rec is None:
            rec = fias_db.House(attrib)
            rec.houseguid = guid
            session.add(rec)
        else:
            if updatedate > rec.updatedate or\
                    (updatedate == rec.updatedate) and (enddate > rec.enddate):
                attrib.setdefault('IFNSFL', None)
                attrib.setdefault('TERRIFNSFL', None)
                attrib.setdefault('IFNSUL', None)
                attrib.setdefault('TERRIFNSUL', None)

                attrib.setdefault('POSTALCODE', None)
                attrib.setdefault('OKTMO', None)
                attrib.setdefault('OKATO', None)

                attrib.setdefault('HOUSENUM', None)
                attrib.setdefault('BUILDNUM', None)
                attrib.setdefault('STRUCNUM', None)
                rec.fromdic(attrib)
            else:
                return
        rec.enddate = enddate
        rec.startdate = startdate
        rec.updatedate = updatedate
        rec.normdoc = NormdocGuidId.getid(normdoc)

        if (startdate >= today) or strange or (guid_i in broken_house):
            pushed_hous[guid_i] = rec
        else:
            pushed_hous.pop(guid_i, None)
            h_cache[guid_i] = rec


def UpdateTable(table, fil, engine=None):
    global upd, pushed_rows, now_row
    if fil is None:
        return
    p = xml.parsers.expat.ParserCreate()
    now_row = 0
    now_row_ = 0
    logging.info("start import " + table)
    if table == 'addrobj':
        if not upd:
            Addrobj.__table__.drop(engine)
            Addrobj.__table__.create(engine)
        p.StartElementHandler = addr_obj_row
    elif table == 'socrbase':
        session.query(fias_db.Socrbase).delete()
        p.StartElementHandler = socr_obj_row
    elif table == 'normdoc':
        if not upd:
            Normdoc.__table__.drop(engine)
            Normdoc.__table__.create(engine)
        p.StartElementHandler = normdoc_row
    elif table == 'house':
        if not upd:
            House.__table__.drop(engine)
            House.__table__.create(engine)
        p.StartElementHandler = house_row
    p.ParseFile(fil)
    AoGuidId.objcache = {}
    AoGuidId.objcache_ = {}
    session.commit()
    session.expunge_all()
    if (not upd) and (engine.name == 'postgresql'):
        logging.info("Index and cluster")
        if table == 'house':
            engine.execute("CREATE INDEX parent ON fias_house "
                           "USING btree (ao_id)")
            engine.execute("CLUSTER fias_house USING parent")
        elif table == 'addrobj':
            engine.execute("CLUSTER fias_addr_obj "
                           "USING ix_fias_addr_obj_parentid")
    logging.info(table + " imported")


import argparse
if __name__ == "__main__":
    parser = argparse.ArgumentParser(\
                            description="Reader of FIAS into database")
    parser.add_argument("--fullfile")
    parser.add_argument("--forcenew", action='store_true')
    parser.add_argument("--fullver", type=int)
    args = parser.parse_args()
    from config import al_dsn
    engine = create_engine(al_dsn,
                           echo=False,
                           paramstyle='format',
                           implicit_returning=False,
                           #execution_options={'stream_results': True}
                           #isolation_level='AUTOCOMMIT',
                           #poolclass=NullPool,
                           )
    Session.configure(bind=engine)
    session = Session(expire_on_commit=False)
    if args.forcenew:
        try:
            fias_db.TableStatus.__table__.drop(engine)
        except:
            pass
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
            my = sess.query(fias_db.TableStatus).\
                filter_by(tablename=tabl).first()
            if my.ver < ver:
                UpdateTable(tabl, FiasFiles().get_updfile(tabl, ver))
                my.ver = ver
                sess.commit()
