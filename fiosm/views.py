#!/usr/bin/python
# -*- coding: UTF-8 -*-

from pyramid.view import view_config
from pyramid.httpexceptions import HTTPBadRequest, HTTPNotFound
import melt
import fias_db
import uuid
import logging
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import create_engine
from config import al_dsn
from pyramid.events import subscriber
from pyramid.events import BeforeRender
engine = create_engine(al_dsn, pool_size=2)
Session = scoped_session(sessionmaker(bind=engine))


@subscriber(BeforeRender)
def add_global(event):
    Session.remove()

off_border = 100


def node4guid(guid=None):
    if guid == 'None' or not guid:
        guid = None
    else:
        # Make check for malformed guid
        try:
            guid = uuid.UUID(guid)
        except ValueError:
            raise HTTPBadRequest()
    return melt.fias_AONode(guid, session=Session())


@view_config(route_name='json_full', renderer='json',
             http_cache=(3600, {'public': True}))
def json_full_view(request):
    # Basic info
    myself = node4guid(request.matchdict["guid"])
    if myself.guid is not None:
        res = myself.fias.asdic(None, False)
        res['fullname'] = myself.fullname
        res['parentguid'] = str(myself.parentguid)
        if myself.fias.parent is None:
            res['parentname'] = u'Россия'
        else:
            res['parentname'] = myself.parent.name
    else:
        res = dict()
    return res


@view_config(route_name='json_subo', renderer='json',
             http_cache=(3600, {'public': True}))
def json_subo_view(request):
    # Basic info
    myself = node4guid(request.matchdict["guid"])
    res = [{'guid': it.guid, 'name': it.name,
            'kind': it.kind, 'osmid': it.osmid}
           for it in myself.subO('all', False, True)]
    return res


@view_config(route_name='json_subb', renderer='json',
             http_cache=(3600, {'public': True}))
def json_subb_view(request):
    # Basic info
    myself = node4guid(request.matchdict["guid"])

    def bld2dict(bld):
        res = {'name': bld.name}
        res['guid'] = bld.houseguid
        if bld.osm:
            res['osmid'] = bld.osm.osm_build
            res['point'] = bld.osm.point
        return res

    res = [bld2dict(it) for it in myself.subB('all_b')]
    return res


@view_config(route_name='json_info', renderer='json',
             http_cache=(3600, {'public': True}))
def json_info_view(request):
    # Basic info
    myself = node4guid(request.matchdict["guid"])
    res = dict(guid=myself.guid)
    res['osmid'] = myself.osmid
    res['kind'] = myself.kind
    res['name'] = myself.name
    res['stat'] = dict()
    for stat_k in ('all', 'found', 'street',
                   'all_b', 'found_b'):
        res['stat'][stat_k] = myself.stat(stat_k)
        if myself.stat_db_full:
            res['stat'][stat_k + '_r'] = myself.stat(stat_k + '_r')
    return res


@view_config(route_name='json_build', renderer='json',
             http_cache=(3600, {'public': True}))
def json_build_view(request):
    build_text = request.matchdict["bld"]
    # Search by guid
    try:
        bld_guid = uuid.UUID(build_text)
        build = Session().query(fias_db.House).\
            filter_by(houseguid=bld_guid).one()
    except ValueError:
        build = None
    # Search by AO and string
    if build is None:
        AO = node4guid(request.matchdict["guid"])
        all_bld = AO.subB('all_b')
        my = [it for it in all_bld if it.equal_to_str(build_text)]
        if my:
            build = my[0]
    # Split by ~
    if build is None:
        raise HTTPNotFound()
    return build.asdic()


@view_config(route_name='details', renderer='templates/details.pt')
def details_view(request):
    #Make check for malformed guid
    try:
        guid = uuid.UUID(request.matchdict["guid"])
    except ValueError:
        raise HTTPBadRequest()
    myself = melt.fias_AONode(guid)
    statlink = request.route_url('found0', guid=guid, typ='all')
    return {"myself": myself, "statlink": statlink, "name": myself.name}


@view_config(route_name='foundroot', renderer='templates/found.pt')
@view_config(route_name='found', renderer='templates/found.pt')
@view_config(route_name='foundbase_', renderer='templates/found.pt')
@view_config(route_name='foundbase', renderer='templates/found.pt')
@view_config(route_name='foundroot0', renderer='templates/found.pt')
@view_config(route_name='found0', renderer='templates/found.pt')
@view_config(route_name='rest_found', renderer='templates/rest_substat.pt')
def found_view(request):
    guid = request.matchdict.get("guid")
    typ = request.matchdict.get("typ", "all")
    if request.matched_route.name == 'rest_json_list':
        typ = request.GET.get('kind', 'all')
    bld = typ.endswith('_b')
    if typ not in ('all', 'found', 'street', 'not found',
                   'all_b', 'found_b', 'not found_b'):
        raise HTTPBadRequest()
    if not guid or guid == 'None':
        #root is ok
        guid = None
    else:
        #Make check for malformed guid
        try:
            guid = uuid.UUID(guid)
        except ValueError:
            raise HTTPBadRequest()
    #Make check for area exist
    myself = melt.fias_AONode(guid)
    if guid and not myself.isok:
        raise HTTPNotFound()

    if bld:
        alist = myself.subB(typ)
        alist.sort(key=lambda el: el.onestr)
    else:
        alist = myself.subO(typ, not('rest' in request.url))
        alist.sort(key=lambda el: el.offname)


    fullstat = guid is not None and myself.stat_db_full
    fullstat = fullstat or all([it.stat_db_full for it in myself.subO('all')])

    offset = int(request.matchdict.get("offset", 0))
    myself.need_more = (len(alist) > (off_border * 1.5)
                        and len(alist) > off_border + offset)
    if 'rest' in request.url:
        request.response.content_type = 'text/xml'
        myself.need_more = False
    if (offset or myself.need_more):
        myself.offlinks = True
        alist = alist[offset:offset + off_border]
    else:
        myself.offlinks = False

    def links(self, typ_l):
        if typ_l in ('all', 'found', 'street', 'not found',
                     'all_b', 'found_b', 'not found_b'):
            return request.route_url('found0', guid=self.guid, typ=typ_l)
        elif typ_l == 'details':
            return request.route_url('details', guid=self.guid, kind='ao')
        elif typ_l == 'top':
            if self.parent.guid:
                return request.route_url('found0', guid=self.parentguid,
                                         typ='all')
            else:
                return request.route_url('foundroot0', typ='all')
        elif typ_l == "prev":
            return request.route_url('found', guid=self.guid, typ=typ,
                                     offset=max(0, offset - off_border))
        elif typ_l == "next":
            return request.route_url('found', guid=self.guid, typ=typ,
                                     offset=min(self.stat(typ) - 1,
                                                offset + off_border))
    return {"list": alist, "myself": myself, "links": links,
            'bld': bld, 'fullstat': fullstat}


@view_config(route_name='rest_buildings', renderer='templates/rest_buildings.pt')
def rest_buildings_view(request):
    #Make check for malformed guid
    try:
        ao_guid = uuid.UUID(request.matchdict["ao_guid"])
    except ValueError:
        raise HTTPBadRequest()
    myself = melt.fias_AO(ao_guid)
    request.response.content_type = 'text/xml'
    return {"list": myself.subB('all_b')}
