from pyramid.config import Configurator

def main(global_config, **settings):
    """ This function returns a Pyramid WSGI application.
    """
    config = Configurator(settings=settings)
    config.add_static_view('static', 'static', cache_max_age=3600)
    config.add_route('foundbase_', '/')
    config.add_route('foundbase','found')
    config.add_route('found','found/{guid}/{typ}')
    config.add_route('foundroot','found//{typ}')
    config.add_route('details','details/{kind}/{guid}')
    config.scan()
    return config.make_wsgi_app()
