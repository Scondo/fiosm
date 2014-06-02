# -*- coding: UTF-8 -*-
'''
Created on 18.05.2013

@author: scond_000
'''
from sqlalchemy import Integer, BigInteger, SmallInteger, String, Date, Boolean
from sqlalchemy import Sequence, Column, ForeignKey
from sqlalchemy.orm.properties import ColumnProperty
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import deferred, relationship
from sqlalchemy.orm import object_mapper
from datetime import date
Base = declarative_base()


class FiasRow(object):
    def fromdic(self, dic):
        for it in dic.iteritems():
            setattr(self, it[0].lower(), it[1])

    def __init__(self, dic=None):
        if dic is not None:
            self.fromdic(dic)

    def collist(self):
        for attr in object_mapper(self).attrs:
            if isinstance(attr, ColumnProperty):
                yield attr.key

    def asdic(self, collist=None, withnone=True):
        res = {}
        if collist is None:
            collist = self.collist()
        for col in collist:
            val = getattr(self, col)
            if val is not None or withnone:
                res[col] = val
        return res


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
    aoguid = Column(UUID(as_uuid=True))
    id = Column(Integer, primary_key=True)
    parentid = Column(Integer, ForeignKey('fias_addr_obj.id'), index=True)
    parent = relationship("Addrobj", remote_side=[id], uselist=False)
    aoid = deferred(Column(UUID(as_uuid=True)))
    previd = deferred(Column(UUID(as_uuid=True)))
    nextid = deferred(Column(UUID(as_uuid=True)))
    startdate = deferred(Column(Date, default=date(1900, 1, 1)))
    enddate = deferred(Column(Date, default=date(2100, 1, 1)))

    formalname = Column(String(120))
    offname = Column(String(120))
    shortname = Column(String(10))
    aolevel = Column(SmallInteger)
    #KLADE
    regioncode = deferred(Column(String(2)))
    autocode = deferred(Column(String(1)))
    areacode = deferred(Column(String(3)))
    citycode = deferred(Column(String(3)))
    ctarcode = deferred(Column(String(3)))
    placecode = deferred(Column(String(3)))
    streetcode = deferred(Column(String(4)))
    extrcode = deferred(Column(String(4)))
    sextcode = deferred(Column(String(3)))
    #KLADR
    code = Column(String(17))
    plaincode = deferred(Column(String(15)))
    #NALOG
    postalcode = deferred(Column(Integer))
    ifnsfl = deferred(Column(SmallInteger))
    terrifnsfl = deferred(Column(SmallInteger))
    ifnsul = deferred(Column(SmallInteger))
    terrifnsul = deferred(Column(SmallInteger))
    okato = deferred(Column(BigInteger))
    oktmo = deferred(Column(String(11)))

    updatedate = deferred(Column(Date))
    actstatus = deferred(Column(SmallInteger))
    centstatus = deferred(Column(SmallInteger))
    operstatus = deferred(Column(SmallInteger))
    currstatus = deferred(Column(SmallInteger))
    normdoc = deferred(Column(Integer))
    livestatus = Column(Boolean, index=True)


class House(FiasRow, Base):
    __tablename__ = 'fias_house'
    houseguid = Column(UUID(as_uuid=True), primary_key=True)
    houseid = Column(UUID(as_uuid=True))
    startdate = Column(Date)
    enddate = Column(Date)

    #f_id = Column(Integer, primary_key=True)
    postalcode = deferred(Column(Integer))
    ifnsfl = deferred(Column(SmallInteger))
    terrifnsfl = deferred(Column(SmallInteger))
    ifnsul = deferred(Column(SmallInteger))
    terrifnsul = deferred(Column(SmallInteger))
    okato = deferred(Column(BigInteger))
    oktmo = deferred(Column(Integer))
    updatedate = deferred(Column(Date))
    housenum = Column(String(20))
    eststatus = Column(SmallInteger)
    buildnum = Column(String(10))
    strucnum = Column(String(10))
    strstatus = Column(SmallInteger)
    ao_id = Column(Integer, index=False)
    statstatus = Column(SmallInteger)
    normdoc = deferred(Column(Integer))

    def makeonestr(self, space=u' '):
        _str = u''
        if self.housenum:
            _str = _str + self.housenum + space
        if self.buildnum:
            _str = _str + u'к' + self.buildnum + space
        if self.strucnum:
            _str = _str + u'с' + self.strucnum + space
        return _str[:-1]

    @property
    def onestr(self):
        return self.makeonestr()

    @property
    def name(self):
        return self.onestr

    def equal_to_str(self, guess):
        if self.onestr.lower() == guess.lower():
            return True
        if self.makeonestr('').lower() == guess.lower():
            return True
        return False


class Versions(FiasRow, Base):
    __tablename__ = "fias_versions"
    ver = Column(Integer, primary_key=True)
    date = Column(Date)
    dumpdate = Column(Date)

    def __init__(self, ver):
        self.ver = ver


class TableStatus(Base):
    __tablename__ = "fias_upd_stat"
    ver = Column(Integer)
    tablename = Column(String, primary_key=True)

    def __init__(self, name, ver):
        self.tablename = name
        self.ver = ver
