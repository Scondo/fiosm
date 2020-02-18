from pyramid.config import Configurator
from pyramid.renderers import JSON
from datetime import datetime, date
from uuid import UUID


def main(global_config, **settings):
    """ This function returns a Pyramid WSGI application.
    """
    config = Configurator(settings=settings)
    json_renderer = JSON()

    def datetime_adapter(obj, request):
        return obj.isoformat()

    def guid_adapter(obj, request):
        return str(obj)

    json_renderer.add_adapter(datetime, datetime_adapter)
    json_renderer.add_adapter(date, datetime_adapter)
    json_renderer.add_adapter(UUID, guid_adapter)
    config.add_renderer('json', json_renderer)
    config.include('pyramid_chameleon')
    config.add_static_view('static', 'static', cache_max_age=3600)
    config.add_route('foundbase_', '/')
    config.add_route('foundbase', 'found')
    config.add_route('found0', 'found/{guid}/{typ}')
    config.add_route('foundroot0', 'found//{typ}')
    config.add_route('found', 'found/{guid}/{typ}/{offset}')
    config.add_route('foundroot', 'found//{typ}/{offset}')
    config.add_route('details', 'details/{kind}/{guid}')
    # REST API
    config.add_route('rest_buildings', 'rest/buildings/{ao_guid}')
    config.add_route('rest_found', 'rest/list/{guid}/{typ}')
    # XML-RPC API
    config.include('pyramid_rpc.xmlrpc')
    config.add_xmlrpc_endpoint('xmlrpc', '/xmlrpc')
    # JSON API
    config.add_route('json_info', 'json/{guid}')
    config.add_route('json_full', 'json/{guid}/full')
    config.add_route('json_subo', 'json/{guid}/subo')
    config.add_route('json_subb', 'json/{guid}/subb')
    config.add_route('json_build', 'json/{guid}/bld/{bld}')
    config.add_route('json_search', 'json/search/obj')
    config.scan()
    return config.make_wsgi_app()
