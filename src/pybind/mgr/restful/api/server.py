from pecan import expose
from pecan.rest import RestController

from restful import module
from restful.decorators import auth


class ServerFqdn(RestController):
    def __init__(self, fqdn):
        self.fqdn = fqdn


    @expose(template='json')
    @auth
    def get(self, **kwargs):
        """
        Show the information for the server fqdn
        """
        return module.instance.get_server(self.fqdn)



class Server(RestController):
    @expose(template='json')
    @auth
    def get(self, **kwargs):
        """
        Show the information for all the servers
        """
        return module.instance.list_servers()


    @expose()
    def _lookup(self, fqdn, *remainder):
        return ServerFqdn(fqdn), remainder
