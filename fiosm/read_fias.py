#!/usr/bin/python
# -*- coding: UTF8 -*-
from __future__ import division
import xml.parsers.expat
import datetime
from datetime import date
import logging
import urllib
from urllib import urlretrieve
from urllib2 import URLError
from os.path import exists
from os import remove
import rarfile
import fias_db
from fias_db import House, HouseInt, Addrobj, Normdoc
from uuid import UUID
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.functions import func
import progressbar


#urllib._urlopener = urllib.FancyURLopener(
#    proxies={'http': 'http://192.168.67.252:3128'})
pbar = None
syncrar = False


def loadProgress(bl, blsize, size):
    dldsize = min(bl * blsize, size)
    p = float(dldsize) / size
    pbar.update(p)


def strpdate(string, fmt):
    return datetime.datetime.strptime(string, fmt).date()

today = date.today()

updating = ("normdoc", "addrobj", "socrbase", "houseint", "house")
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

import multiprocessing
import io


class AsyncRarServ(multiprocessing.Process):
    def __init__(self, *args, **kwargs):
        self.src = kwargs.pop('src')
        self.filename = kwargs.pop('filename')
        multiprocessing.Process.__init__(self, *args, **kwargs)
        self.queue = multiprocessing.Queue(10)
        self.fin = multiprocessing.Event()
        self.go = multiprocessing.Event()
        self.size = multiprocessing.queues.SimpleQueue()

    def run(self):
        rar = rarfile.RarFile(self.src)
        rared = rar.open(self.filename)
        self.size.put(rared.inf.file_size)
        self.go.set()
        while rared.remain > 0:
            line = rared.read(102400)
            self.queue.put(line)
        self.fin.set()
        rared.close()
        rar.close()


class AsyncRarCli(io.IOBase):
    def __init__(self, src, filename):
        self.srv = AsyncRarServ(src=src, filename=filename)
        self.buf = bytearray()
        self.srv.start()
        self.passed = 0
        self._size = None

    def read(self, n=None):
        if not self.buf:
            while self.srv.queue.empty():
                if self.srv.fin.wait(0.1):
                    if self.srv.queue.empty():
                        self.srv.join()
                        return self.buf
                    else:
                        # recursive to fetch remain data
                        return self.read(n)
            self.buf = self.srv.queue.get()
        res = self.buf[:n]
        self.buf = self.buf[n:]
        self.passed += len(res)
        return bytes(res)

    def tell(self):
        return self.passed

    def size(self):
        if self._size is None:
            if self.srv.go.wait(10):
                self._size = self.srv.size.get()
            else:
                raise ValueError('File not opened')
        return self._size

    def readinto(self, b):
        z = self.read(len(b))
        z = bytearray(z)
        b[:] = z[:]

    def readable(self):
        return True

    def writable(self):
        return False


class FiasFiles(object):
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(FiasFiles, cls).__new__(cls)
            self = cls.instance
            self.full_file = None
            self.upd_files = {}
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

    def get_fullarch(self, ver=None):
        if ver is None:
            ver = self.maxver()
        global pbar
        if self.full_file is None or not exists(self.full_file):
            pbar = progressbar.ProgressBar(widgets=['Download: ',
                progressbar.Bar(), ' ', progressbar.ETA()], maxval=1.0).start()
            self.full_file = urlretrieve(
                self.fias_list[ver].FiasCompleteXmlUrl,
                self.full_file, reporthook=loadProgress)[0]
            self.full_ver = ver
            pbar.finish()
            try:
                arch = rarfile.RarFile(self.full_file)
                logging.info('Testing full file...')
                arch.testrar()
            except rarfile.Error:
                # Try get previous version
                logging.warn('Full file version ' + str(ver) + ' is broken')
                remove(self.full_file)
                logging.warn('Trying full file version ' + str(ver - 1))
                self.get_fullarch(ver - 1)

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
                if syncrar:
                    return (arch.open(filename), self.full_ver)
                else:
                    filo = AsyncRarCli(self.full_file, filename)
                    return (filo, self.full_ver)

    def get_updfile(self, table, ver):
        global pbar
        if ver not in self.upd_files:
            pbar = progressbar.ProgressBar(widgets=['Download: ',
                progressbar.Bar(), ' ', progressbar.ETA()], maxval=1.0).start()
            self.upd_files[ver] = urlretrieve(
                self.fias_list[ver].FiasDeltaXmlUrl,
                reporthook=loadProgress)[0]
            pbar.finish()
        arch = rarfile.RarFile(self.upd_files[ver])
        for filename in arch.namelist():
            if filename[3:].lower().startswith(table):
                # Get and save date
                try:
                    rec = session.query(fias_db.Versions).get(ver)
                    rec.date = strpdate(filename.split("_")[3], "%Y%m%d")
                except:
                    pass
                if syncrar:
                    return arch.open(filename)
                else:
                    return AsyncRarCli(self.upd_files[ver], filename)

    def __del__(self):
        urllib.urlcleanup()


class GuidId(object):
    def __init__(self, guidn, record, use_objcache=False):
        self.cache = {}
        if use_objcache:
            self.objcache = {}
            self.objcache_ = {}
        else:
            self.objcache = None
        self.guidn = guidn
        self.record = record
        self.local = None

    def flush_cache(self):
        if len(self.objcache) > 10000:
            self.objcache_ = self.objcache
            self.objcache = {}

    def chklocal(self):
        if session.query(self.record).count() == 0:
            self.local = True
            self.max = 0
        else:
            self.local = False
            self.max = session.query(func.max(self.record.id)).scalar()

    def __getrec(self, guid_int):
        if not (self.objcache is None):
            if guid_int in self.objcache:
                return self.objcache[guid_int]
            if guid_int in self.objcache_:
                return self.objcache_[guid_int]
        cond = {self.guidn: UUID(int=guid_int)}
        return session.query(self.record).filter_by(**cond).first()

    def precache(self, guid_list=[]):
        guid_list = set([UUID(it) for it in guid_list if it])
        guid_list = [it for it in guid_list if it not in self.objcache]
        q = session.query(self.record)
        q = q.filter(getattr(self.record, self.guidn).in_(guid_list))
        for obj in q.all():
            guid_int = getattr(obj, self.guidn).int
            if guid_int not in self.objcache:
                self.objcache[guid_int] = obj

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
                self.max += 1
                idO.id = self.max
                session.flush() # force save new id
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
            # Non-local or present
            idO = self.__getrec(guid.int)

        if idO is None:
            if not self.local:
                # Non-local and new
                self.max += 1
                dic["id"] = self.max
            idO = self.record(dic)
            session.add(idO)
        else:
            if (comp is None) or comp(dic, idO):
                del dic[self.guidn]
                idO.fromdic(dic)
        self.cache[guid.int] = idO.id
        if not (self.objcache is None):
            self.objcache[guid.int] = idO
        try:
            session.flush()
        except:
            print sorted(dic.items())
            print sorted(idO.asdic().items())
            raise


AoGuidId = GuidId('aoguid', fias_db.Addrobj, True)


def addr_cmp(dic, rec):
    if rec.aoid is None:
        #Dummy always should be replaced
        return True
    if dic['AOID'] == rec.aoid:
        #New rec is always better
        return True
    if dic['UPDATEDATE'] > rec.updatedate:
        return True
    if dic['STARTDATE'] > rec.startdate:
        return True
    return False

count_max = 0
count_tag = None
pre_list = []
pre_att = ''
tag4tbl = {'addrobj': 'Object',
           'house': 'House',
           'normdoc': 'NormativeDocument'}


def count_row(name, attrib):
    global count_max
    if name == count_tag:
        count_max += 1
        if pre_att:
            pre_list.append(attrib.get(pre_att, None))


def normdoc_row(name, attrib):
    global upd, now_row, now_row_
    if name == "NormativeDocument":
        now_row_ += 1
        if now_row_ == 100000:
            now_row = now_row + now_row_
            now_row_ = 0
            # logging.info(now_row)
            session.flush()
        attrib['normdocid'] = UUID(attrib.pop('NORMDOCID'))
        if upd:
            pbar.update(now_row + now_row_)
            old_doc = session.query(Normdoc).get(attrib['normdocid'])
            if old_doc:  # May be update will be better???
                session.delete(old_doc)
                session.flush()
        rec = Normdoc(attrib)
        session.add(rec)


def socr_obj_row(name, attrib):
    if name == "AddressObjectType":
        socr = fias_db.Socrbase(attrib)
        session.add(socr)


def addr_obj_row(name, attrib):
    global upd, now_row, now_row_, pre_list
    if name == 'Object':
        if upd:
            pbar.update(now_row + now_row_)
            if now_row_ % 100 == 0:
                AoGuidId.precache(pre_list[:100])
                pre_list = pre_list[100:]
        if 'NEXTID' in attrib:
            #If one have successor - it's dead
            attrib['LIVESTATUS'] = '0'
        if not upd and attrib.get('LIVESTATUS', '0') != '1':
            #On first pass we can skip all dead,
            #on update they will disable current
            return

        if 'NORMDOC' in attrib:
            attrib['NORMDOC'] = UUID(attrib['NORMDOC'])
        d = attrib.pop('ENDDATE').split('-')
        attrib['ENDDATE'] = date(int(d[0]), int(d[1]), int(d[2]))
        d = attrib.pop('STARTDATE').split('-')
        attrib['STARTDATE'] = date(int(d[0]), int(d[1]), int(d[2]))
        d = attrib.pop('UPDATEDATE').split('-')
        attrib['UPDATEDATE'] = date(int(d[0]), int(d[1]), int(d[2]))
        attrib['aoguid'] = UUID(attrib.pop('AOGUID'))
        attrib['parentid'] = AoGuidId.getid(attrib.pop('PARENTGUID', None))

        AoGuidId.pushrec(attrib, comp=addr_cmp)

        now_row_ += 1
        if now_row_ == 100000:
            now_row = now_row + now_row_
            now_row_ = 0
            # logging.info(now_row)
            AoGuidId.flush_cache()
            session.flush()

# Keys are guid integers, values are update dates ordinal
passed_houses = {}
# Keys are guids, values are records
pushed_hous = {}
broken_house = frozenset((UUID('ea1e5154-7588-4220-8691-6b63bb93c3d4').int,
                          UUID('feed6431-5e39-4ba0-9ecf-02f1ec55910e').int,
                          UUID('f477c26f-2d14-468f-b3f0-f7399a3c2de5').int,
                          UUID('2831d031-8ea7-4ff1-82df-50693cc94320').int,
                          UUID('9002160a-2cf9-4574-9988-b6c840d3fab2').int,
                          ))


def is_h_better(rec, attrib, sdate, edate, udate):
    if rec.updatedate < udate:
        return True
    elif rec.updatedate > udate:
        return False

    if rec.startdate < sdate:
        return True
    elif rec.startdate > sdate:
        return False

    if rec.enddate < edate:
            return True
    elif rec.enddate > edate:
            return False

    if rec.divtype == 0 and attrib.get('DIVTYPE', '0') != '0':
            return True
    elif rec.divtype != 0 and attrib.get('DIVTYPE', '0') == '0':
            return False
    return True


def house_row(name, attrib):
    global upd, now_row, now_row_, pushed_hous, passed_houses
    if name == 'House':
        now_row_ += 1
        if now_row_ == 250000:
            now_row = now_row + now_row_
            now_row_ = 0
            # logging.info((now_row, len(pushed_hous) len(removed_hous)))
            session.flush()
        if upd:
            pbar.update(now_row + now_row_)
            old_h = session.query(House).\
                filter_by(houseid=attrib['HOUSEID']).first()
            if old_h:
                session.delete(old_h)
                session.flush()

        del attrib['COUNTER']
        ed = attrib.pop('ENDDATE').split('-')
        enddate = date(*[int(it) for it in ed])
        sd = attrib.pop('STARTDATE').split('-')
        startdate = date(int(sd[0]), int(sd[1]), int(sd[2]))
        ud = attrib.pop('UPDATEDATE').split('-')
        updatedate = date(int(ud[0]), int(ud[1]), int(ud[2]))
        updatedateo = updatedate.toordinal()
        guid = UUID(attrib.pop("HOUSEGUID"))
        guid_i = guid.int
        if passed_houses.get(guid_i, 0) > updatedateo:
            return
        else:
            strange = (passed_houses.get(guid_i, 0) == updatedateo)
        strange = strange or startdate >= enddate
        rec = pushed_hous.get(guid_i, None)
        if (not strange) and (enddate < today) and\
                (rec is None or rec.updatedate <= updatedate):
            # remove is kind of pass
            passed_houses[guid_i] = updatedateo
            if rec is not None:
                pushed_hous.pop(guid_i)
                session.flush() #!!!
                session.delete(rec)
            return

        normdoc = attrib.pop('NORMDOC', None)
        attrib['ao_id'] = AoGuidId.getid(attrib.pop('AOGUID'), False)
        if attrib['ao_id'] is None:
            return

        if (strange or (startdate >= today) or upd or
                (guid_i in passed_houses)) and (rec is None):
            # If house is 'future' check if that already in DB
            # Other houses should not be in DB:
            # 'past' houses are skipped and current is only one
            rec = session.query(House).get(guid)

        if rec is None:
            rec = fias_db.House(attrib)
            rec.houseguid = guid
            session.add(rec)
        else:
            if is_h_better(rec, attrib, startdate, enddate, updatedate):
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
        if normdoc:
            rec.normdoc = UUID(normdoc)
        passed_houses[guid_i] = updatedateo
        if strange or (guid_i in broken_house):
            pushed_hous[guid_i] = rec
        else:
            pushed_hous.pop(guid_i, None)


def houseint_row(name, attrib):
    global upd, pushed_hous, passed_houses

    if name == 'HouseInterval':
        if upd:
            session.query(HouseInt).filter_by(
                houseintid=attrib['HOUSEINTID']).delete()

        del attrib['COUNTER']
        ed = attrib.pop('ENDDATE').split('-')
        enddate = date(int(ed[0]), int(ed[1]), int(ed[2]))
        sd = attrib.pop('STARTDATE').split('-')
        startdate = date(int(sd[0]), int(sd[1]), int(sd[2]))
        ud = attrib.pop('UPDATEDATE').split('-')
        updatedate = date(int(ud[0]), int(ud[1]), int(ud[2]))
        updatedateo = updatedate.toordinal()
        guid = UUID(attrib.pop("INTGUID"))
        guid_i = guid.int
        if passed_houses.get(guid_i, 0) > updatedateo:
            return
        else:
            strange = (passed_houses.get(guid_i, 0) == updatedateo)
        strange = strange or (startdate >= enddate)
        rec = pushed_hous.get(guid_i, None)

        if (not strange) and (enddate < today) and\
                (rec is None or rec.updatedate <= updatedate):
            if rec is not None:
                session.flush()
                pushed_hous.pop(guid_i)
                session.flush()
                session.delete(rec)
            return

        normdoc = attrib.pop('NORMDOC', None)
        attrib['ao_id'] = AoGuidId.getid(attrib.pop('AOGUID'), False)
        if attrib['ao_id'] is None:
            return

        if (strange or (startdate >= today) or upd or
                (guid_i in passed_houses)) and (rec is None):
            # If house is 'future' check if that already in DB
            # Other houses should not be in DB:
            # 'past' houses are skipped and current is only one
            rec = session.query(HouseInt).get(guid)

        if rec is None:
            rec = fias_db.HouseInt(attrib)
            rec.intguid = guid
            session.add(rec)
        else:
            if is_h_better(rec, attrib,
                           startdate, enddate, updatedate):
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
        if normdoc:
            rec.normdoc = UUID(normdoc)
        passed_houses[guid_i] = updatedateo
        if strange:
            pushed_hous[guid_i] = rec
        else:
            pushed_hous.pop(guid_i, None)


def UpdateTable(table, fil, engine=None):
    global upd, pushed_rows, now_row, now_row_, pbar
    if fil is None:
        return
    p = xml.parsers.expat.ParserCreate()
    now_row = 0
    now_row_ = 0
    logging.info("start import " + table)
    if count_max:
        p_widgets = ['Updating: ', progressbar.SimpleProgress(), ' ',
             progressbar.Bar(marker=progressbar.RotatingMarker()),
             ' ', progressbar.ETA()]
        pbar = progressbar.ProgressBar(widgets=p_widgets,
                                       maxval=count_max).start()
    else:
        p_widgets = ['Load/update: ',
             progressbar.Bar(marker=progressbar.RotatingMarker()),
             ' ', progressbar.ETA()]
        pbar = progressbar.ProgressBar(widgets=p_widgets,
                                       maxval=fil.size() + 1).start()

        class PBReader(object):
            def __init__(self, orig):
                self.orig = orig

            def read(self, n):
                pbar.update(self.orig.tell())
                return self.orig.read(n)
        fil = PBReader(fil)
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
    elif table == 'houseint':
        if not upd:
            HouseInt.__table__.drop(engine)
            HouseInt.__table__.create(engine)
        p.StartElementHandler = houseint_row
    p.ParseFile(fil)
    AoGuidId.flush_cache()
    removed_hous = {}
    pushed_hous = {}
    session.commit()
    session.expunge_all()
    pbar.finish()
    if (not upd) and (engine.name == 'postgresql'):
        logging.info("Index and cluster")
        if table == 'house':
            engine.execute("CREATE INDEX parent ON fias_house "
                           "USING btree (ao_id)")
            engine.execute("CREATE INDEX idx_houseid ON fias_house "
                           "USING btree (houseid)")
            engine.execute("CLUSTER fias_house USING parent")
        elif table == 'addrobj':
            engine.execute("CLUSTER fias_addr_obj "
                           "USING ix_fias_addr_obj_parentid")
    logging.info(table + " imported")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Reader of FIAS into database")
    parser.add_argument("--fullfile")
    parser.add_argument("--forcenew", action='store_true')
    parser.add_argument("--onlyfull", action='store_true')
    parser.add_argument("--fullver", type=int)
    parser.add_argument("--syncrar", action='store_true')
    args = parser.parse_args()
    syncrar = args.syncrar
    from config import al_dsn, use_osm
    engine = create_engine(al_dsn,
                           echo=False,
                           paramstyle='format',
                           implicit_returning=False)
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
        if my is None:  # Load table is absent
            full = FiasFiles().get_fullfile(tabl)
            if use_osm:
                # Clean dependency
                try:
                    if tabl == "addrobj":
                        engine.execute("DROP TABLE public.fiosm_stat")
                        engine.execute("DROP TABLE "
                                       "public.planet_osm_fias_place")
                        engine.execute("DROP TABLE "
                                       "public.planet_osm_fias_street")
                    elif tabl == "house" or tabl == "houseint":
                        engine.execute("DROP TABLE "
                                       "public.planet_osm_fias_build")
                except:
                    pass
            if full is not None:
                upd = False
                UpdateTable(tabl, full[0], engine)
                my = fias_db.TableStatus(tabl, full[1])
                sess.add(my)
                sess.commit()
        # Load or was present before - determine min ver for futher update
        if my is not None and (minver is None or minver > my.ver):
            minver = my.ver

    if not args.onlyfull:
        upd = True
        for ver in range(minver + 1, FiasFiles().maxver() + 1):
            logging.info("Update " + str(ver) + " of " + str(FiasFiles().maxver()))
            for tabl in updating:
                my = sess.query(fias_db.TableStatus).\
                    filter_by(tablename=tabl).first()
                if my.ver < ver:
                    count_max = 0
                    count_tag = tag4tbl.get(tabl, None)
                    pre_list = []
                    if count_tag:
                        if tabl == 'addrobj':
                            pre_att = 'AOGUID'
                        else:
                            pre_att = ''
                        p = xml.parsers.expat.ParserCreate()
                        p.StartElementHandler = count_row
                        p.ParseFile(FiasFiles().get_updfile(tabl, ver))
                    UpdateTable(tabl, FiasFiles().get_updfile(tabl, ver))
                    my.ver = ver
                    sess.commit()
