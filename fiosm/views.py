from pyramid.view import view_config
from pyramid.httpexceptions import HTTPBadRequest, HTTPNotFound

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
    guid = request.matchdict["guid"]
    typ = request.matchdict["typ"]
    if typ not in ('all', 'found', 'street', 'not found'):
        raise HTTPBadRequest()
    if not guid:
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

    alist = myself.subO(typ)
    alist.sort(key=lambda el: el.offname)

    if request.matchdict["offset"] or len(alist) > 150:
        offset = int(request.matchdict['offset'])
        myself.offlinks = True
        alist = alist[offset:offset + 100]
    else:
        myself.offlinks = False

    def links(self, typ_l):
        if typ_l in ('all', 'found', 'street', 'not found'):
            return request.route_url('found0', guid=self.guid, typ=typ_l)
        elif typ_l == 'details':
            return request.route_url('details', guid=self.guid, kind='ao')
        elif typ_l == 'top':
            if self.parent.guid:
                return request.route_url('found0', guid=self.parent.guid, typ='all')
            else:
                return request.route_url('foundroot0', typ='all')
        elif typ_l == "prev":
            return request.route_url('found', guid=self.guid, typ=typ, offset=max(0, offset - 100))
        elif typ_l == "next":
            return request.route_url('found', guid=self.guid, typ=typ, offset=min(self.stat(typ) - 1, offset + 100))

    return {"list": alist, "myself": myself, "links": links}


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
