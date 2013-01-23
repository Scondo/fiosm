from pyramid.view import view_config
from pyramid.httpexceptions import HTTPBadRequest

#this is demo
@view_config(route_name='home', renderer='templates/mytemplate.pt')
def my_view(request):
    return {'project':'fiosm'}

#import psycopg2
import melt
import uuid
#conn=psycopg2.connect("dbname=osm user=osm password=osm host=192.168.56.101")

@view_config(route_name='foundbase', renderer='templates/found.pt')
def foundbase_view(request):
    request.matchdict["guid"]=""
    request.matchdict["typ"]="all"
    return found_view(request)

@view_config(route_name='foundroot', renderer='templates/found.pt')
def foundroot_view(request):
    request.matchdict["guid"]=""
    return found_view(request)

@view_config(route_name='details', renderer='templates/details.pt')
def details_view(request):
    
    guid=request.matchdict["guid"]
    #Make check for malformed guid!!!
    try:
        guid=uuid.UUID(guid)
    except ValueError:
        pass
    myself=melt.fias_AO(guid)
    statlink=request.route_url('found', guid=guid, typ='all')
    return {"fias":myself.fias,"statlink":statlink,"name":myself.name}


@view_config(route_name='found', renderer='templates/found.pt')
def found_view(request):
    def add_links(elem):
        elem.link={}
        for typ_ in ('all','found','street','not found'):#fiosm.typ_cond.keys():
            elem.link[typ_]=request.route_url('found', guid=elem.guid, typ=typ_)

    #cur_=conn.cursor()
    guid=request.matchdict["guid"]
    typ=request.matchdict['typ']
    if not melt.typ_cond.has_key(typ):
        raise HTTPBadRequest()
    #Make check for malformed guid!!!
    try:
        guid=uuid.UUID(guid)
    except ValueError:
        pass
    #Make check for area exist
    myself=melt.fias_AO(guid)
    if not myself.isok:
        pass
    add_links(myself)
    if myself.parent.guid:
        myself.link['top']=request.route_url('found', guid=myself.parent, typ='all')
    else:
        myself.link['top']=''

    myself.link['details']=request.route_url('details', guid=myself.guid, kind='ao')
   
       
    agen=myself.subs(typ) #melt.GetAreaList(guid,typ)
    #alist=[makerow(_guid for _guid in agen)]
    alist=[]
    for _guid in agen:
        el=melt.fias_AO(_guid,parent=guid)
        el.kind=typ
        add_links(el)
        alist.append(el)
    return {'project':'fiosm',"list":alist, "myself":myself}