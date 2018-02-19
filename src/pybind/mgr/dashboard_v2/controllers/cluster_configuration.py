import cherrypy
from ..tools import ApiController, RESTController, AuthRequired


@ApiController('cluster_conf')
@AuthRequired()
class ClusterConfiguration(RESTController):
    def list(self, service=None, level=None):
        levels = ['basic', 'advanced', 'developer']
        if level is not None:
            assert level in levels

        options = self.mgr.get("config_options")['options']

        if service is not None:
            options = [o for o in options if service in o['services']]

        if level is not None:
            options = [
                o for o in options
                if levels.index(o['level']) <= levels.index(level)
            ]

        return options

    def get(self, name):
        for option in self.mgr.get('config_options')['options']:
            if option['name'] == name:
                return option

        raise cherrypy.HTTPError(404)
