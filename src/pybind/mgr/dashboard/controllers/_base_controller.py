import inspect
import json
import logging
from functools import wraps
from typing import ClassVar, List, Optional, Type
from urllib.parse import unquote

import cherrypy

from ..plugins import PLUGIN_MANAGER
from ..services.auth import AuthManager, JwtManager
from ..tools import get_request_body_params
from ._helpers import _get_function_params
from ._version import APIVersion

logger = logging.getLogger(__name__)


class BaseController:
    """
    Base class for all controllers providing API endpoints.
    """

    _registry: ClassVar[List[Type['BaseController']]] = []
    _routed = False

    def __init_subclass__(cls, skip_registry: bool = False, **kwargs) -> None:
        super().__init_subclass__(**kwargs)  # type: ignore
        if not skip_registry:
            BaseController._registry.append(cls)

    @classmethod
    def load_controllers(cls):
        import importlib
        from pathlib import Path

        path = Path(__file__).parent
        logger.debug('Controller import path: %s', path)
        modules = [
            f.stem for f in path.glob('*.py') if
            not f.name.startswith('_') and f.is_file() and not f.is_symlink()]
        logger.debug('Controller files found: %r', modules)

        for module in modules:
            importlib.import_module(f'{__package__}.{module}')

        # pylint: disable=protected-access
        controllers = [
            controller for controller in BaseController._registry if
            controller._routed
        ]

        for clist in PLUGIN_MANAGER.hook.get_controllers() or []:
            controllers.extend(clist)

        return controllers

    class Endpoint:
        """
        An instance of this class represents an endpoint.
        """

        def __init__(self, ctrl, func):
            self.ctrl = ctrl
            self.inst = None
            self.func = func

            if not self.config['proxy']:
                setattr(self.ctrl, func.__name__, self.function)

        @property
        def config(self):
            func = self.func
            while not hasattr(func, '_endpoint'):
                if hasattr(func, "__wrapped__"):
                    func = func.__wrapped__
                else:
                    return None
            return func._endpoint  # pylint: disable=protected-access

        @property
        def function(self):
            # pylint: disable=protected-access
            return self.ctrl._request_wrapper(self.func, self.method,
                                              self.config['json_response'],
                                              self.config['xml'],
                                              self.config['version'])

        @property
        def method(self):
            return self.config['method']

        @property
        def proxy(self):
            return self.config['proxy']

        @property
        def url(self):
            ctrl_path = self.ctrl.get_path()
            if ctrl_path == "/":
                ctrl_path = ""
            if self.config['path'] is not None:
                url = "{}{}".format(ctrl_path, self.config['path'])
            else:
                url = "{}/{}".format(ctrl_path, self.func.__name__)

            ctrl_path_params = self.ctrl.get_path_param_names(
                self.config['path'])
            path_params = [p['name'] for p in self.path_params
                           if p['name'] not in ctrl_path_params]
            path_params = ["{{{}}}".format(p) for p in path_params]
            if path_params:
                url += "/{}".format("/".join(path_params))

            return url

        @property
        def action(self):
            return self.func.__name__

        @property
        def path_params(self):
            ctrl_path_params = self.ctrl.get_path_param_names(
                self.config['path'])
            func_params = _get_function_params(self.func)

            if self.method in ['GET', 'DELETE']:
                assert self.config['path_params'] is None

                return [p for p in func_params if p['name'] in ctrl_path_params
                        or (p['name'] not in self.config['query_params']
                            and p['required'])]

            # elif self.method in ['POST', 'PUT']:
            return [p for p in func_params if p['name'] in ctrl_path_params
                    or p['name'] in self.config['path_params']]

        @property
        def query_params(self):
            if self.method in ['GET', 'DELETE']:
                func_params = _get_function_params(self.func)
                path_params = [p['name'] for p in self.path_params]
                return [p for p in func_params if p['name'] not in path_params]

            # elif self.method in ['POST', 'PUT']:
            func_params = _get_function_params(self.func)
            return [p for p in func_params
                    if p['name'] in self.config['query_params']]

        @property
        def body_params(self):
            func_params = _get_function_params(self.func)
            path_params = [p['name'] for p in self.path_params]
            query_params = [p['name'] for p in self.query_params]
            return [p for p in func_params
                    if p['name'] not in path_params
                    and p['name'] not in query_params]

        @property
        def group(self):
            return self.ctrl.__name__

        @property
        def is_api(self):
            # changed from hasattr to getattr: some ui-based api inherit _api_endpoint
            return getattr(self.ctrl, '_api_endpoint', False)

        @property
        def is_secure(self):
            return self.ctrl._cp_config['tools.authenticate.on']  # pylint: disable=protected-access

        def __repr__(self):
            return "Endpoint({}, {}, {})".format(self.url, self.method,
                                                 self.action)

    def __init__(self):
        logger.info('Initializing controller: %s -> %s',
                    self.__class__.__name__, self._cp_path_)  # type: ignore
        super().__init__()

    def _has_permissions(self, permissions, scope=None):
        if not self._cp_config['tools.authenticate.on']:  # type: ignore
            raise Exception("Cannot verify permission in non secured "
                            "controllers")

        if not isinstance(permissions, list):
            permissions = [permissions]

        if scope is None:
            scope = getattr(self, '_security_scope', None)
        if scope is None:
            raise Exception("Cannot verify permissions without scope security"
                            " defined")
        username = JwtManager.LOCAL_USER.username
        return AuthManager.authorize(username, scope, permissions)

    @classmethod
    def get_path_param_names(cls, path_extension=None):
        if path_extension is None:
            path_extension = ""
        full_path = cls._cp_path_[1:] + path_extension  # type: ignore
        path_params = []
        for step in full_path.split('/'):
            param = None
            if not step:
                continue
            if step[0] == ':':
                param = step[1:]
            elif step[0] == '{' and step[-1] == '}':
                param, _, _ = step[1:-1].partition(':')
            if param:
                path_params.append(param)
        return path_params

    @classmethod
    def get_path(cls):
        return cls._cp_path_  # type: ignore

    @classmethod
    def endpoints(cls):
        """
        This method iterates over all the methods decorated with ``@endpoint``
        and creates an Endpoint object for each one of the methods.

        :return: A list of endpoint objects
        :rtype: list[BaseController.Endpoint]
        """
        result = []
        for _, func in inspect.getmembers(cls, predicate=callable):
            if hasattr(func, '_endpoint'):
                result.append(cls.Endpoint(cls, func))
        return result

    @staticmethod
    def get_client_version():
        try:
            client_version = APIVersion.from_mime_type(
                cherrypy.request.headers['Accept'])
        except Exception:
            raise cherrypy.HTTPError(
                415, "Unable to find version in request header")
        return client_version

    @staticmethod
    def _request_wrapper(func, method, json_response, xml,  # pylint: disable=unused-argument
                         version: Optional[APIVersion]):
        # pylint: disable=too-many-branches
        @wraps(func)
        def inner(*args, **kwargs):
            client_version = None
            for key, value in kwargs.items():
                if isinstance(value, str):
                    kwargs[key] = unquote(value)

            # Process method arguments.
            params = get_request_body_params(cherrypy.request)
            kwargs.update(params)

            if version is not None:
                client_version = BaseController.get_client_version()

                if version.supports(client_version):
                    ret = func(*args, **kwargs)
                else:
                    raise cherrypy.HTTPError(
                        415,
                        f"Incorrect version: endpoint is '{version!s}', "
                        f"client requested '{client_version!s}'"
                    )

            else:
                ret = func(*args, **kwargs)

            if isinstance(ret, bytes):
                ret = ret.decode('utf-8')

            if xml:
                cherrypy.response.headers['Content-Type'] = (version.to_mime_type(subtype='xml')
                                                             if version else 'application/xml')
                return ret.encode('utf8')
            if json_response:
                cherrypy.response.headers['Content-Type'] = (version.to_mime_type(subtype='json')
                                                             if version else 'application/json')
                ret = json.dumps(ret).encode('utf8')
            return ret
        return inner

    @property
    def _request(self):
        return self.Request(cherrypy.request)

    class Request(object):
        def __init__(self, cherrypy_req):
            self._creq = cherrypy_req

        @property
        def scheme(self):
            return self._creq.scheme

        @property
        def host(self):
            base = self._creq.base
            base = base[len(self.scheme)+3:]
            return base[:base.find(":")] if ":" in base else base

        @property
        def port(self):
            base = self._creq.base
            base = base[len(self.scheme)+3:]
            default_port = 443 if self.scheme == 'https' else 80
            return int(base[base.find(":")+1:]) if ":" in base else default_port

        @property
        def path_info(self):
            return self._creq.path_info
