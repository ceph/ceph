# -*- coding: utf-8 -*-
# pylint: disable=protected-access
from __future__ import absolute_import

import collections
import importlib
import inspect
import json
import os
import pkgutil
import sys

import cherrypy
from six import add_metaclass

from .. import logger
from ..settings import Settings
from ..tools import Session, wraps, getargspec, TaskManager
from ..exceptions import ViewCacheNoDataException, DashboardException
from ..services.exception import serialize_dashboard_exception


class Controller(object):
    def __init__(self, path, base_url=""):
        self.path = path
        self.base_url = base_url

    def __call__(self, cls):
        cls._cp_controller_ = True
        if self.base_url:
            cls._cp_path_ = "{}/{}".format(self.base_url, self.path)
        else:
            cls._cp_path_ = self.path
        config = {
            'tools.sessions.on': True,
            'tools.sessions.name': Session.NAME,
            'tools.session_expire_at_browser_close.on': True,
            'tools.dashboard_exception_handler.on': True
        }
        if not hasattr(cls, '_cp_config'):
            cls._cp_config = {}
        if 'tools.authenticate.on' not in cls._cp_config:
            config['tools.authenticate.on'] = False
        cls._cp_config.update(config)
        return cls


class ApiController(Controller):
    def __init__(self, path, version=1):
        if version == 1:
            base_url = "api"
        else:
            base_url = "api/v" + str(version)
        super(ApiController, self).__init__(path, base_url)
        self.version = version

    def __call__(self, cls):
        cls = super(ApiController, self).__call__(cls)
        cls._api_version = self.version
        return cls


def AuthRequired(enabled=True):
    def decorate(cls):
        if not hasattr(cls, '_cp_config'):
            cls._cp_config = {
                'tools.authenticate.on': enabled
            }
        else:
            cls._cp_config['tools.authenticate.on'] = enabled
        return cls
    return decorate


def load_controllers():
    # setting sys.path properly when not running under the mgr
    controllers_dir = os.path.dirname(os.path.realpath(__file__))
    dashboard_dir = os.path.dirname(controllers_dir)
    mgr_dir = os.path.dirname(dashboard_dir)
    logger.debug("LC: controllers_dir=%s", controllers_dir)
    logger.debug("LC: dashboard_dir=%s", dashboard_dir)
    logger.debug("LC: mgr_dir=%s", mgr_dir)
    if mgr_dir not in sys.path:
        sys.path.append(mgr_dir)

    controllers = []
    mods = [mod for _, mod, _ in pkgutil.iter_modules([controllers_dir])]
    logger.debug("LC: mods=%s", mods)
    for mod_name in mods:
        mod = importlib.import_module('.controllers.{}'.format(mod_name),
                                      package='dashboard')
        for _, cls in mod.__dict__.items():
            # Controllers MUST be derived from the class BaseController.
            if inspect.isclass(cls) and issubclass(cls, BaseController) and \
                    hasattr(cls, '_cp_controller_'):
                if cls._cp_path_.startswith(':'):
                    # invalid _cp_path_ value
                    logger.error("Invalid url prefix '%s' for controller '%s'",
                                 cls._cp_path_, cls.__name__)
                    continue
                controllers.append(cls)

    return controllers


ENDPOINT_MAP = collections.defaultdict(list)


def generate_controller_routes(ctrl_class, mapper, base_url):
    inst = ctrl_class()
    endp_base_urls = set()

    for endpoint in ctrl_class.endpoints():
        conditions = dict(method=endpoint.methods) if endpoint.methods else None
        endp_url = endpoint.url
        if '/' in endp_url:
            endp_base_urls.add(endp_url[:endp_url.find('/')])
        else:
            endp_base_urls.add(endp_url)
        url = "{}/{}".format(base_url, endp_url)

        logger.debug("Mapped [%s] to %s:%s restricted to %s",
                     url, ctrl_class.__name__, endpoint.action,
                     endpoint.methods)

        ENDPOINT_MAP[endpoint.url].append(endpoint)

        name = ctrl_class.__name__ + ":" + endpoint.action
        mapper.connect(name, url, controller=inst, action=endpoint.action,
                       conditions=conditions)

        # adding route with trailing slash
        name += "/"
        url += "/"
        mapper.connect(name, url, controller=inst, action=endpoint.action,
                       conditions=conditions)

    return endp_base_urls


def generate_routes(url_prefix):
    mapper = cherrypy.dispatch.RoutesDispatcher()
    ctrls = load_controllers()

    parent_urls = set()
    for ctrl in ctrls:
        parent_urls.update(generate_controller_routes(ctrl, mapper,
                                                      "{}".format(url_prefix)))

    logger.debug("list of parent paths: %s", parent_urls)
    return mapper, parent_urls


def json_error_page(status, message, traceback, version):
    cherrypy.response.headers['Content-Type'] = 'application/json'
    return json.dumps(dict(status=status, detail=message, traceback=traceback,
                           version=version))


class Task(object):
    def __init__(self, name, metadata, wait_for=5.0, exception_handler=None):
        self.name = name
        if isinstance(metadata, list):
            self.metadata = dict([(e[1:-1], e) for e in metadata])
        else:
            self.metadata = metadata
        self.wait_for = wait_for
        self.exception_handler = exception_handler

    def _gen_arg_map(self, func, args, kwargs):
        # pylint: disable=deprecated-method
        arg_map = {}
        if sys.version_info > (3, 0):  # pylint: disable=no-else-return
            sig = inspect.signature(func)
            arg_list = [a for a in sig.parameters]
        else:
            sig = getargspec(func)
            arg_list = [a for a in sig.args]

        for idx, arg in enumerate(arg_list):
            if idx < len(args):
                arg_map[arg] = args[idx]
            else:
                if arg in kwargs:
                    arg_map[arg] = kwargs[arg]
            if arg in arg_map:
                # This is not a type error. We are using the index here.
                arg_map[idx] = arg_map[arg]

        return arg_map

    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            arg_map = self._gen_arg_map(func, args, kwargs)
            md = {}
            for k, v in self.metadata.items():
                if isinstance(v, str) and v and v[0] == '{' and v[-1] == '}':
                    param = v[1:-1]
                    try:
                        pos = int(param)
                        md[k] = arg_map[pos]
                    except ValueError:
                        md[k] = arg_map[v[1:-1]]
                else:
                    md[k] = v
            task = TaskManager.run(self.name, md, func, args, kwargs,
                                   exception_handler=self.exception_handler)
            try:
                status, value = task.wait(self.wait_for)
            except Exception as ex:
                if task.ret_value:
                    # exception was handled by task.exception_handler
                    if 'status' in task.ret_value:
                        status = task.ret_value['status']
                    else:
                        status = getattr(ex, 'status', 500)
                    cherrypy.response.status = status
                    return task.ret_value
                raise ex
            if status == TaskManager.VALUE_EXECUTING:
                cherrypy.response.status = 202
                return {'name': self.name, 'metadata': md}
            return value
        return wrapper


def _get_function_params(func):
    """
    Retrieves the list of parameters declared in function.
    Each parameter is represented as dict with keys:
      * name (str): the name of the parameter
      * required (bool): whether the parameter is required or not
      * default (obj): the parameter's default value
    """
    fspec = getargspec(func)

    func_params = []
    nd = len(fspec.args) if not fspec.defaults else -len(fspec.defaults)
    for param in fspec.args[1:nd]:
        func_params.append({'name': param, 'required': True})

    if fspec.defaults:
        for param, val in zip(fspec.args[nd:], fspec.defaults):
            func_params.append({
                'name': param,
                'required': False,
                'default': val
            })

    return func_params


class BaseController(object):
    """
    Base class for all controllers providing API endpoints.
    """

    class Endpoint(object):
        """
        An instance of this class represents an endpoint.
        """
        def __init__(self, ctrl, func, methods=None):
            self.ctrl = ctrl
            self.func = self._unwrap(func)
            if methods is None:
                methods = []
            self.methods = methods

        @classmethod
        def _unwrap(cls, func):
            while hasattr(func, "__wrapped__"):
                func = func.__wrapped__
            return func

        @property
        def url(self):
            ctrl_path_params = self.ctrl.get_path_param_names()
            if self.func.__name__ != '__call__':
                url = "{}/{}".format(self.ctrl.get_path(), self.func.__name__)
            else:
                url = self.ctrl.get_path()
            path_params = [
                p['name'] for p in _get_function_params(self.func)
                if p['required'] and p['name'] not in ctrl_path_params]
            path_params = ["{{{}}}".format(p) for p in path_params]
            if path_params:
                url += "/{}".format("/".join(path_params))
            return url

        @property
        def action(self):
            return self.func.__name__

        @property
        def path_params(self):
            return [p for p in _get_function_params(self.func) if p['required']]

        @property
        def query_params(self):
            return [p for p in _get_function_params(self.func)
                    if not p['required']]

        @property
        def body_params(self):
            return []

        @property
        def group(self):
            return self.ctrl.__name__

        @property
        def is_api(self):
            return hasattr(self.ctrl, '_api_version')

        @property
        def is_secure(self):
            return self.ctrl._cp_config['tools.authenticate.on']

        def __repr__(self):
            return "Endpoint({}, {}, {})".format(self.url, self.methods,
                                                 self.action)

    def __init__(self):
        logger.info('Initializing controller: %s -> /%s',
                    self.__class__.__name__, self._cp_path_)

    @classmethod
    def get_path_param_names(cls):
        path_params = []
        for step in cls._cp_path_.split('/'):
            param = None
            if step[0] == ':':
                param = step[1:]
            elif step[0] == '{' and step[-1] == '}':
                param, _, _ = step[1:-1].partition(':')
            if param:
                path_params.append(param)
        return path_params

    @classmethod
    def get_path(cls):
        return cls._cp_path_

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
            if hasattr(func, 'exposed') and func.exposed:
                result.append(cls.Endpoint(cls, func))
        return result


class RESTController(BaseController):
    """
    Base class for providing a RESTful interface to a resource.

    To use this class, simply derive a class from it and implement the methods
    you want to support.  The list of possible methods are:

    * list()
    * bulk_set(data)
    * create(data)
    * bulk_delete()
    * get(key)
    * set(data, key)
    * delete(key)

    Test with curl:

    curl -H "Content-Type: application/json" -X POST \
         -d '{"username":"xyz","password":"xyz"}'  http://127.0.0.1:8080/foo
    curl http://127.0.0.1:8080/foo
    curl http://127.0.0.1:8080/foo/0

    """

    # resource id parameter for using in get, set, and delete methods
    # should be overriden by subclasses.
    # to specify a composite id (two parameters) use '/'. e.g., "param1/param2".
    # If subclasses don't override this property we try to infer the structure
    # of the resourse ID.
    RESOURCE_ID = None

    _method_mapping = collections.OrderedDict([
        ('list', {'method': 'GET', 'resource': False, 'status': 200}),
        ('create', {'method': 'POST', 'resource': False, 'status': 201}),
        ('bulk_set', {'method': 'PUT', 'resource': False, 'status': 200}),
        ('bulk_delete', {'method': 'DELETE', 'resource': False, 'status': 204}),
        ('get', {'method': 'GET', 'resource': True, 'status': 200}),
        ('delete', {'method': 'DELETE', 'resource': True, 'status': 204}),
        ('set', {'method': 'PUT', 'resource': True, 'status': 200})
    ])

    class RESTEndpoint(BaseController.Endpoint):
        def __init__(self, ctrl, func):
            if func.__name__ in ctrl._method_mapping:
                methods = [ctrl._method_mapping[func.__name__]['method']]
                status = ctrl._method_mapping[func.__name__]['status']
            elif hasattr(func, "_resource_method_"):
                methods = func._resource_method_
                status = 200
            elif hasattr(func, "_collection_method_"):
                methods = func._collection_method_
                status = 200
            else:
                assert False

            wrapper = ctrl._rest_request_wrapper(func, status)
            setattr(ctrl, func.__name__, wrapper)

            super(RESTController.RESTEndpoint, self).__init__(
                ctrl, func, methods)

        def get_resource_id_params(self):
            if self.func.__name__ in self.ctrl._method_mapping:
                if self.ctrl._method_mapping[self.func.__name__]['resource']:
                    resource_id_params = self.ctrl.infer_resource_id()
                    if resource_id_params:
                        return resource_id_params

            if hasattr(self.func, '_resource_method_'):
                resource_id_params = self.ctrl.infer_resource_id()
                if resource_id_params:
                    return resource_id_params

            return []

        @property
        def url(self):
            url = self.ctrl.get_path()

            res_id_params = self.get_resource_id_params()
            if res_id_params:
                res_id_params = ["{{{}}}".format(p) for p in res_id_params]
                url += "/{}".format("/".join(res_id_params))

            if hasattr(self.func, "_collection_method_") \
               or hasattr(self.func, "_resource_method_"):
                url += "/{}".format(self.func.__name__)
            return url

        @property
        def path_params(self):
            params = [{'name': p, 'required': True}
                      for p in self.ctrl.get_path_param_names()]
            params.extend([{'name': p, 'required': True}
                           for p in self.get_resource_id_params()])
            return params

        @property
        def query_params(self):
            path_params_names = [p['name'] for p in self.path_params]
            if 'GET' in self.methods or 'DELETE' in self.methods:
                return [p for p in _get_function_params(self.func)
                        if p['name'] not in path_params_names]
            return []

        @property
        def body_params(self):
            path_params_names = [p['name'] for p in self.path_params]
            if 'POST' in self.methods or 'PUT' in self.methods:
                return [p for p in _get_function_params(self.func)
                        if p['name'] not in path_params_names]
            return []

    @classmethod
    def infer_resource_id(cls):
        if cls.RESOURCE_ID is not None:
            return cls.RESOURCE_ID.split('/')
        for k, v in cls._method_mapping.items():
            func = getattr(cls, k, None)
            while hasattr(func, "__wrapped__"):
                func = func.__wrapped__
            if v['resource'] and func:
                path_params = cls.get_path_param_names()
                params = _get_function_params(func)
                return [p['name'] for p in params
                        if p['required'] and p['name'] not in path_params]
        return None

    @classmethod
    def endpoints(cls):
        result = []
        for _, val in inspect.getmembers(cls, predicate=callable):
            if val.__name__ in cls._method_mapping:
                result.append(cls.RESTEndpoint(cls, val))
            elif hasattr(val, "_collection_method_") \
                    or hasattr(val, "_resource_method_"):
                result.append(cls.RESTEndpoint(cls, val))
            elif hasattr(val, 'exposed') and val.exposed:
                result.append(cls.Endpoint(cls, val))
        return result

    @classmethod
    def _rest_request_wrapper(cls, func, status_code):
        @wraps(func)
        def wrapper(*vpath, **params):
            method = func
            if cherrypy.request.method not in ['GET', 'DELETE']:
                method = RESTController._takes_json(method)

            method = RESTController._returns_json(method)

            cherrypy.response.status = status_code

            return method(*vpath, **params)
        if not hasattr(wrapper, '__wrapped__'):
            wrapper.__wrapped__ = func
        return wrapper

    @staticmethod
    def _function_args(func):
        return getargspec(func).args[1:]

    @staticmethod
    def _takes_json(func):
        def inner(*args, **kwargs):
            if cherrypy.request.headers.get('Content-Type', '') == \
                    'application/x-www-form-urlencoded':
                return func(*args, **kwargs)

            content_length = int(cherrypy.request.headers['Content-Length'])
            body = cherrypy.request.body.read(content_length)
            if not body:
                return func(*args, **kwargs)

            try:
                data = json.loads(body.decode('utf-8'))
            except Exception as e:
                raise cherrypy.HTTPError(400, 'Failed to decode JSON: {}'
                                         .format(str(e)))

            kwargs.update(data.items())
            return func(*args, **kwargs)
        return inner

    @staticmethod
    def _returns_json(func):
        def inner(*args, **kwargs):
            cherrypy.response.headers['Content-Type'] = 'application/json'
            ret = func(*args, **kwargs)
            return json.dumps(ret).encode('utf8')
        return inner

    @staticmethod
    def resource(methods=None):
        if not methods:
            methods = ['GET']

        def _wrapper(func):
            func._resource_method_ = methods
            return func
        return _wrapper

    @staticmethod
    def collection(methods=None):
        if not methods:
            methods = ['GET']

        def _wrapper(func):
            func._collection_method_ = methods
            return func
        return _wrapper
