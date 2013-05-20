'''
Created on 18.05.2013

@author: scond_000
'''
from sqlalchemy import Sequence, Column, Integer, BigInteger, SmallInteger, String, Date
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()


class FiasRow(object):
    def fromdic(self, dic):
        for it in dic.iteritems():
            setattr(self, it[0].lower(), it[1])

    def __init__(self, dic):
        self.fromdic(dic)


class Socrbase(FiasRow, Base):
    __tablename__ = 'fias_socr_obj'
    level = Column(SmallInteger, primary_key=True)
    scname = Column(String(10), primary_key=True, default="")
    socrname = Column(String(50))
    kod_t_st = Column(String(4))


class Normdoc(FiasRow, Base):
    __tablename__ = 'fias_normdoc'
    id = Column(Integer, Sequence('inner_id'))
    normdocid = Column(UUID(as_uuid=True), primary_key=True)
    docname = Column(String)
    docdate = Column(Date)
    docnum = Column(String(20))
    doctype = Column(Integer)
    docimgid = Column(Integer)


class Addrobj(FiasRow, Base):
    __tablename__ = 'fias_addr_obj'
    aoguid = Column(UUID, primary_key=True)
    parentguid = Column(UUID, index=True)
    aoid = Column(UUID)
    previd = Column(UUID)
    #nextid = Column(UUID)  #=None
    startdate = Column(Date)
    enddate = Column(Date)

    formalname = Column(String(120))
    offname = Column(String(120))
    shortname = Column(String(10))
    aolevel = Column(SmallInteger)
    postalcode = Column(Integer)
    #KLADE
    regioncode = Column(String(2))
    autocode = Column(String(1))
    areacode = Column(String(3))
    citycode = Column(String(3))
    ctarcode = Column(String(3))
    placecode = Column(String(3))
    streetcode = Column(String(4))
    extrcode = Column(String(4))
    sextcode = Column(String(3))
    #KLADR
    code = Column(String(17))
    plaincode = Column(String(15))
    #NALOG
    ifnsfl = Column(SmallInteger)
    terrifnsfl = Column(SmallInteger)
    ifnsul = Column(SmallInteger)
    terrifnsul = Column(SmallInteger)
    okato = Column(BigInteger)
    oktmo = Column(Integer)
    updatedate = Column(Date)
    actstatus = Column(SmallInteger)
    centstatus = Column(SmallInteger)
    operstatus = Column(SmallInteger)
    currstatus = Column(SmallInteger)
    normdoc = Column(Integer)
    #livestatus = Column(Boolean)  #=True


class House(FiasRow, Base):
    __tablename__ = 'fias_house'
    postalcode = Column(Integer)
    ifnsfl = Column(SmallInteger)
    terrifnsfl = Column(SmallInteger)
    ifnsul = Column(SmallInteger)
    terrifnsul = Column(SmallInteger)
    okato = Column(BigInteger)
    oktmo = Column(Integer)
    updatedate = Column(Date)
    housenum = Column(String(20))
    eststatus = Column(SmallInteger)
    buildnum = Column(String(10))
    strucnum = Column(String(10))
    strstatus = Column(SmallInteger)
    houseguid = Column(UUID, primary_key=True)
    houseid = Column(UUID)
    aoguid = Column(UUID, index=False)
    startdate = Column(Date)
    enddate = Column(Date)
    statstatus = Column(SmallInteger)
    normdoc = Column(Integer)


class Versions(Base):
    __tablename__ = "fias_versions"
    ver = Column(Integer, primary_key=True)
    date = Column(Date)


class TableStatus(Base):
    __tablename__ = "fias_upd_stat"
    ver = Column(Integer)
    tablename = Column(String, primary_key=True)

    def __init__(self, name, ver):
        self.tablename = name
        self.ver = ver
