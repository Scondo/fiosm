'''
Created on 28.07.2013

@author: scond_000
'''
from config import al_dsn
import fias_db
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
engine = create_engine(al_dsn)
session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)
return_limit = 100
import uuid
from pyramid_rpc.xmlrpc import xmlrpc_method


def add_req_arg(func):
    def f1(request, *args):
        return func(*args)
    return f1


import melt  # TODO: check melt for no-osm safe


@xmlrpc_method(endpoint='xmlrpc')
def GetBuildingProperties(request, guid, proplist=[]):
    if not proplist:
        return {}
    session = Session()
    one = session.query(fias_db.House).get(uuid.UUID(guid))
    res = {}
    for prop in proplist:
        res[prop] = getattr(one, prop, '')
        if res[prop] is None:
            res[prop] = ''
        elif isinstance(res[prop], (int, long)):
            res[prop] = str(res[prop])
    return res


@xmlrpc_method(endpoint='xmlrpc')
def GetBuildings(request, ao_guid):
    session = Session()
    #one = session.query(fias_db.Addrobj).get(aid)
    one = melt.fias_AO(ao_guid, session=session)
    res = []
    for bld in one.subB('all_b'):
        res.append({'guid': str(bld.houseguid),
                    'onestr': bld.onestr
                    })
    return res


@xmlrpc_method(endpoint='xmlrpc')
def GetAreaProperties(request, guid, propnames):
    session = Session()
    #one = session.query(fias_db.Addrobj).get(aid)
    one = melt.fias_AO(guid, session=session)
    res = {}
    for prop in propnames:
        res[prop] = getattr(one, prop)
    return res


@xmlrpc_method(endpoint='xmlrpc')
def GetAreaProperty(request, guid, propname):
    session = Session()
    #one = session.query(fias_db.Addrobj).get(aid)
    one = melt.fias_AO(guid, session=session)
    return getattr(one, propname)


@xmlrpc_method(endpoint='xmlrpc')
def GetAreas(request, filter_by={}, name_like=None):
    """Get data for FIAS address objects filtered by 'filter_by' struct
    """
    session = Session()
    if filter_by is None:
        filter_by = {'parentid': None}
    cols = set(fias_db.Addrobj({'id': -1}).collist())
    for key in filter_by:
        if key not in cols:
            del filter_by[key]
        if filter_by[key] == '':
            filter_by[key] = 0
    filter_by['livestatus'] = True
    q = session.query(fias_db.Addrobj).filter_by(**filter_by)
    if name_like:
        q = q.filter(fias_db.Addrobj.formalname.like(name_like))
    if q.count() > return_limit:
        return "Too big list"
    else:
        res = []
        for one in q.all():
            ao = melt.fias_AO(one, session=session)
            one_ = dict(formalname=one.formalname,
                        shortname=one.shortname,
                        aolevel=one.aolevel,
                        code=one.code,
                    okato=str(one.okato) if one.okato else '',
                    postalcode=str(one.postalcode) if one.postalcode else '',
                        name=ao.name,
                        fullname=ao.fullname,
                        aoguid=str(one.aoguid)
                        )
            res.append(one_)
        return res


if __name__ == "__main__":
    from DocXMLRPCServer import DocXMLRPCServer
    serv = DocXMLRPCServer(("127.0.0.1", 8080), allow_none=True)
    serv.register_function(GetAreas)
    serv.register_function(GetAreaProperty)
    serv.register_function(GetBuildings)
    serv.register_function(GetBuildingProperties)
    serv.register_introspection_functions()
    serv.serve_forever()
