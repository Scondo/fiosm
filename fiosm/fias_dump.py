'''
Created on 17.09.2013

@author: Scondo
'''
from datetime import date
import datetime
today = date.today()
import logging
import fias_db
from uuid import UUID
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import class_mapper, defer, undefer
Session = sessionmaker()
session = Session()
upd = False
now_row = 0
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
import cPickle as pickle
import argparse
from inspect import isclass


def obj_by_tabln(tabln):
    for obj in fias_db.__dict__.values():
        if isclass(obj) and issubclass(obj, fias_db.FiasRow)\
        and obj != fias_db.FiasRow and obj.__tablename__ == tabln:
            return obj


def strip_guids(mydic, guidlist):
    for guid in guidlist:
        if guid in mydic:
            mydic[guid] = mydic[guid].int


def wrap_guids(mydic, guidlist):
    for guid in guidlist:
        if guid in mydic:
            mydic[guid] = UUID(int=mydic[guid])


def strip_dates(mydic, datelist):
    for guid in datelist:
        if guid in mydic:
            mydic[guid] = mydic[guid].toordinal()


def wrap_dates(mydic, datelist):
    for guid in datelist:
        if guid in mydic:
            mydic[guid] = date.fromordinal(mydic[guid])


if __name__ == "__main__":
    pars = argparse.ArgumentParser(\
                description='Dump tool for FIAS part of fiosm')
    pars.add_argument('mode', choices=['dump', 'check', 'load'])
    pars.add_argument('target_file')
    args = pars.parse_args()
    from config import al_dsn
    engine = create_engine(al_dsn, echo=False, implicit_returning=False)
    Session.configure(bind=engine)
    session = Session()
    if args.mode == 'dump':
        tgt = open(args.target_file, "wb")
        pi = pickle.Pickler(tgt, protocol=-1)
        li = session.query(fias_db.Versions).all()
        li = [it.asdic() for it in li]
        pi.dump(li)
        for obj in fias_db.__dict__.values():
            #if issubclass(obj, fias_db.FiasRow) and obj != fias_db.FiasRow:
            if obj in (fias_db.Socrbase, fias_db.Normdoc, fias_db.Addrobj, fias_db.House):
                tbln = obj.__tablename__
                mapr = class_mapper(obj)
                pk_names = [mapr.get_property_by_column(col).key\
                    for col in mapr.primary_key]
                collist = [attr.key for attr in mapr.column_attrs]
                guids = [attr.key for attr in mapr.column_attrs \
                         if isinstance(attr.columns[0].type, fias_db.UUID)]
                dates = [attr.key for attr in mapr.column_attrs \
                         if isinstance(attr.columns[0].type, fias_db.Date)]
                v = session.query(fias_db.TableStatus).\
                filter_by(tablename=tbln).first()
                q = session.query(obj).options(*[undefer(col) for col in collist])
                n = q.count()
                #Pickle tablename, version and number of records
                #as header of section
                pi.dump((tbln, v, n))
                for it in q.yield_per(10000):
                    val = it.asdic(collist, False)
                    for guid in guids:
                        if guid in val:
                            val[guid] = val[guid].int
                    #strip_guids(val, guids)
                    for guid in dates:
                        if guid in val:
                            val[guid] = val[guid].toordinal()
#                    strip_dates(val, dates)
                    pi.dump(val)
                    # Saving raw data instead of references allow
                    # better compression (and saving from memory overflow)
                    pi.clear_memo()
        tgt.close()
    elif args.mode == 'check':
        src = open(args.target_file, "rb")
        upi = pickle.Unpickler(src)
        li1 = session.query(fias_db.Versions).all()
        li1 = [it.asdic() for it in li1]
        li = upi.load()
        assert li == li1
        while True:
            missed = dict()
            passed = dict()
            try:
                (tbln, v, n) = upi.load()
            except EOFError:
                break
            v1 = session.query(fias_db.TableStatus).\
                filter_by(tablename=tbln).first()
            assert v == v1
            obj = obj_by_tabln(tbln)
            mapr = class_mapper(obj)
            pk_names = [mapr.get_property_by_column(col).key\
                    for col in mapr.primary_key]
            collist = [attr.key for attr in mapr.column_attrs]
            guids = [attr.key for attr in mapr.column_attrs \
                         if isinstance(attr.columns[0].type, fias_db.UUID)]
            dates = [attr.key for attr in mapr.column_attrs \
                         if isinstance(attr.columns[0].type, fias_db.Date)]
            #Simple check TODO: remove after full check
            q = session.query(obj)
            n1 = q.count()
            q = session.query(obj).options(*[undefer(col) for col in collist])
            gen = q.yield_per(1000).__iter__()
            assert n == n1
            for i in range(0, n):
                one = upi.load()
                wrap_guids(one, guids)
                wrap_dates(one, dates)
                pk = tuple([one[pk_] for pk_ in pk_names])
                #one1 = q.get(pk).asdic(collist, False)
                if pk in missed:
                    assert missed[pk] == one
                    del missed[pk]
                else:
                    passed[pk] = one
                one_ = gen.next()
                pk = tuple(mapr.primary_key_from_instance(one_))
                if pk in passed:
                    assert passed[pk] == one_.asdic(collist, False)
                    del passed[pk]
                else:
                    missed[pk] = one_.asdic(collist, False)

        src.close()
        print "OK"
