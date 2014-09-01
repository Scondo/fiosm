from pyramid.config import Configurator


def main(global_config, **settings):
    """ This function returns a Pyramid WSGI application.
    """
    config = Configurator(settings=settings)
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

    config.scan()
    return config.make_wsgi_app()
