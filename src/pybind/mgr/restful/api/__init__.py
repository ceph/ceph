from pecan import expose
from pecan.rest import RestController

from .config import Config
from .crush import Crush
from .doc import Doc
from .mon import Mon
from .osd import Osd
from .pool import Pool
from .request import Request
from .server import Server


class Root(RestController):
    config = Config()
    crush = Crush()
    doc = Doc()
    mon = Mon()
    osd = Osd()
    pool = Pool()
    request = Request()
    server = Server()

    @expose(template='json')
    def get(self, **kwargs):
        """
        Show the basic information for the REST API
        This includes values like api version or auth method
        """
        return {
            'api_version': 1,
            'auth':
                'Use "ceph restful create-key <key>" to create a key pair, '
                'pass it as HTTP Basic auth to authenticate',
            'doc': 'See /doc endpoint',
            'info': "Ceph Manager RESTful API server",
        }
