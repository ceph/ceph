from pecan import expose, response
from pecan.rest import RestController

from restful import context
from restful.decorators import auth


class MonName(RestController):
    def __init__(self, name):
        self.name = name


    @expose(template='json')
    @auth
    def get(self, **kwargs):
        """
        Show the information for the monitor name
        """
        mon = filter(
            lambda x: x['name'] == self.name,
            context.instance.get_mons()
        )

        if len(mon) != 1:
            response.status = 500
            return {'message': 'Failed to identify the monitor node "%s"' % self.name}

        return mon[0]



class Mon(RestController):
    @expose(template='json')
    @auth
    def get(self, **kwargs):
        """
        Show the information for all the monitors
        """
        return context.instance.get_mons()


    @expose()
    def _lookup(self, name, *remainder):
        return MonName(name), remainder
