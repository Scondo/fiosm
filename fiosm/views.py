from pyramid.view import view_config
from pyramid.httpexceptions import HTTPBadRequest

import melt
import uuid


@view_config(route_name='details', renderer='templates/details.pt')
def details_view(request):
    
    guid=request.matchdict["guid"]
    #Make check for malformed guid!!!
    try:
        guid=uuid.UUID(guid)
    except ValueError:
        raise HTTPBadRequest()
    myself=melt.fias_AONode(guid)
    statlink = request.route_url('found0', guid=guid, typ='all')
    return {"fias":myself.fias,"statlink":statlink,"name":myself.name}


@view_config(route_name='found', renderer='templates/found.pt')
def found_view(request):
    def add_links(elem):
        elem.link={}
        for typ_ in ('all','found','street','not found'):#fiosm.typ_cond.keys():
            elem.link[typ_] = request.route_url('found0', guid=elem.guid, typ=typ_)

    guid=request.matchdict["guid"]
    typ=request.matchdict['typ']
    if not melt.typ_cond.has_key(typ):
        raise HTTPBadRequest()
    #Make check for malformed guid
    try:
        guid=uuid.UUID(guid)
    except ValueError:
        if not guid:
            guid=None
        else:
            raise HTTPBadRequest()
    #Make check for area exist
    myself=melt.fias_AONode(guid)
    if not myself.isok:
        raise HTTPBadRequest()
    add_links(myself)
    if myself.parent.guid:
        myself.link['top'] = request.route_url('found0', guid=myself.parent.guid, typ='all')
    else:
        myself.link['top'] = ''

    myself.link['details']=request.route_url('details', guid=myself.guid, kind='ao')

    alist = myself.subO(typ)
    alist.sort(key=lambda el: el.offname)

    if request.matchdict["offset"] or len(alist) > 150:
        offset = int(request.matchdict['offset'])
        myself.link["offlinks"] = True
        myself.link["prev"] = request.route_url('found', guid=guid, typ=typ, offset=max(0, offset - 100))
        myself.link["next"] = request.route_url('found', guid=guid, typ=typ, offset=min(len(alist) - 1, offset + 100))
        alist = alist[offset:offset + 100]
    else:
        myself.link["offlinks"] = False

    for el in alist:
        add_links(el)
    return {"list": alist, "myself": myself}


#Some defaults
@view_config(route_name='foundbase', renderer='templates/found.pt')
def foundbase_view(request):
    request.matchdict["typ"] = "all"
    return foundroot0_view(request)


@view_config(route_name='foundbase_', renderer='templates/found.pt')
def foundbase2_view(request):
    return foundbase_view(request)


@view_config(route_name='foundroot', renderer='templates/found.pt')
def foundroot_view(request):
    request.matchdict["guid"] = ""
    return found_view(request)


@view_config(route_name='foundroot0', renderer='templates/found.pt')
def foundroot0_view(request):
    request.matchdict["guid"] = ""
    request.matchdict["offset"] = 0
    return found_view(request)


@view_config(route_name='found0', renderer='templates/found.pt')
def found0_view(request):
    request.matchdict["offset"] = 0
    return found_view(request)
