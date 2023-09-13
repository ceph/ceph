import logging

import cherrypy

from ..exceptions import ScopeNotValid
from ..security import Scope
from ._base_controller import BaseController
from ._helpers import generate_controller_routes

logger = logging.getLogger(__name__)


class Router(object):
    def __init__(self, path, base_url=None, security_scope=None, secure=True):
        if security_scope and not Scope.valid_scope(security_scope):
            raise ScopeNotValid(security_scope)
        self.path = path
        self.base_url = base_url
        self.security_scope = security_scope
        self.secure = secure

        if self.path and self.path[0] != "/":
            self.path = "/" + self.path

        if self.base_url is None:
            self.base_url = ""
        elif self.base_url == "/":
            self.base_url = ""

        if self.base_url == "" and self.path == "":
            self.base_url = "/"

    def __call__(self, cls):
        cls._routed = True
        cls._cp_path_ = "{}{}".format(self.base_url, self.path)
        cls._security_scope = self.security_scope

        config = {
            'tools.dashboard_exception_handler.on': True,
            'tools.authenticate.on': self.secure,
        }
        if not hasattr(cls, '_cp_config'):
            cls._cp_config = {}
        cls._cp_config.update(config)
        return cls

    @classmethod
    def generate_routes(cls, url_prefix):
        controllers = BaseController.load_controllers()
        logger.debug("controllers=%r", controllers)

        mapper = cherrypy.dispatch.RoutesDispatcher()

        parent_urls = set()

        endpoint_list = []
        for ctrl in controllers:
            inst = ctrl()
            for endpoint in ctrl.endpoints():
                endpoint.inst = inst
                endpoint_list.append(endpoint)

        endpoint_list = sorted(endpoint_list, key=lambda e: e.url)
        for endpoint in endpoint_list:
            parent_urls.add(generate_controller_routes(endpoint, mapper,
                                                       "{}".format(url_prefix)))

        logger.debug("list of parent paths: %s", parent_urls)
        return mapper, parent_urls
