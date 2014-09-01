from pyramid.view import view_config
from pyramid.httpexceptions import HTTPBadRequest, HTTPNotFound

import melt
import uuid
off_border = 100
import logging


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
    fullstat = guid is not None and myself.stat_db_full
    fullstat = fullstat or all([it.stat_db_full for it in myself.subO('all')])

    if bld:
        alist = myself.subB(typ)
        alist.sort(key=lambda el: el.onestr)
    else:
        alist = myself.subO(typ)
        alist.sort(key=lambda el: el.offname)
    offset = int(request.matchdict.get("offset", 0))
    if not bld and (offset or len(alist) > (off_border * 1.5)):
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
    if 'rest' in request.url:
        request.response.content_type = 'text/xml'
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
