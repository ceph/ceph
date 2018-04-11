# -*- coding: utf-8 -*-
# pylint: disable=W0212
from __future__ import absolute_import

import collections
from datetime import datetime, timedelta
import fnmatch
import importlib
import inspect
import json
import os
import pkgutil
import sys
import time
import threading
import types  # pylint: disable=import-error

import cherrypy
from six import add_metaclass

from .. import logger
from ..settings import Settings
from ..tools import Session


def ApiController(path):
    def decorate(cls):
        cls._cp_controller_ = True
        cls._cp_path_ = path
        config = {
            'tools.sessions.on': True,
            'tools.sessions.name': Session.NAME,
            'tools.session_expire_at_browser_close.on': True
        }
        if not hasattr(cls, '_cp_config'):
            cls._cp_config = {}
        if 'tools.authenticate.on' not in cls._cp_config:
            config['tools.authenticate.on'] = False
        cls._cp_config.update(config)
        return cls
    return decorate


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


def generate_controller_routes(ctrl_class, mapper, base_url):
    inst = ctrl_class()
    for methods, url_suffix, action, params in ctrl_class.endpoints():
        if not url_suffix:
            name = ctrl_class.__name__
            url = "{}/{}".format(base_url, ctrl_class._cp_path_)
        else:
            name = "{}:{}".format(ctrl_class.__name__, url_suffix)
            url = "{}/{}/{}".format(base_url, ctrl_class._cp_path_, url_suffix)

        if params:
            for param in params:
                url = "{}/:{}".format(url, param)

        conditions = dict(method=methods) if methods else None

        logger.debug("Mapping [%s] to %s:%s restricted to %s",
                     url, ctrl_class.__name__, action, methods)
        mapper.connect(name, url, controller=inst, action=action,
                       conditions=conditions)

        # adding route with trailing slash
        name += "/"
        url += "/"
        mapper.connect(name, url, controller=inst, action=action,
                       conditions=conditions)


def generate_routes(url_prefix):
    mapper = cherrypy.dispatch.RoutesDispatcher()
    ctrls = load_controllers()
    for ctrl in ctrls:
        generate_controller_routes(ctrl, mapper, "{}/api".format(url_prefix))

    mapper.connect(ApiRoot.__name__, "{}/api".format(url_prefix),
                   controller=ApiRoot("{}/api".format(url_prefix),
                                      ctrls))
    return mapper


def json_error_page(status, message, traceback, version):
    cherrypy.response.headers['Content-Type'] = 'application/json'
    return json.dumps(dict(status=status, detail=message, traceback=traceback,
                           version=version))


class ApiRoot(object):

    _cp_config = {
        'tools.sessions.on': True,
        'tools.authenticate.on': True
    }

    def __init__(self, base_url, ctrls):
        self.base_url = base_url
        self.ctrls = ctrls

    def __call__(self):
        tpl = """API Endpoints:<br>
        <ul>
        {lis}
        </ul>
        """
        endpoints = ['<li><a href="{}/{}">{}</a></li>'
                     .format(self.base_url, ctrl._cp_path_, ctrl.__name__) for
                     ctrl in self.ctrls]
        return tpl.format(lis='\n'.join(endpoints))


# pylint: disable=too-many-locals
def browsable_api_view(meth):
    def wrapper(self, *vpath, **kwargs):
        assert isinstance(self, BaseController)
        if not Settings.ENABLE_BROWSABLE_API:
            return meth(self, *vpath, **kwargs)
        if 'text/html' not in cherrypy.request.headers.get('Accept', ''):
            return meth(self, *vpath, **kwargs)
        if '_method' in kwargs:
            cherrypy.request.method = kwargs.pop('_method').upper()
        if '_raw' in kwargs:
            kwargs.pop('_raw')
            return meth(self, *vpath, **kwargs)

        template = """
        <html>
        <h1>Browsable API</h1>
        {docstring}
        <h2>Request</h2>
        <p>{method} {breadcrump}</p>
        <h2>Response</h2>
        <p>Status: {status_code}<p>
        <pre>{reponse_headers}</pre>
        <form action="/api/{path}/{vpath}" method="get">
        <input type="hidden" name="_raw" value="true" />
        <button type="submit">GET raw data</button>
        </form>
        <h2>Data</h2>
        <pre>{data}</pre>
        {create_form}
        <h2>Note</h2>
        <p>Please note that this API is not an official Ceph REST API to be
        used by third-party applications. It's primary purpose is to serve
        the requirements of the Ceph Dashboard and is subject to change at
        any time. Use at your own risk.</p>
        """

        create_form_template = """
        <h2>Create Form</h2>
        <form action="/api/{path}/{vpath}" method="post">
        {fields}<br>
        <input type="hidden" name="_method" value="post" />
        <button type="submit">Create</button>
        </form>
        """

        try:
            data = meth(self, *vpath, **kwargs)
        except Exception as e:  # pylint: disable=broad-except
            except_template = """
            <h2>Exception: {etype}: {tostr}</h2>
            <pre>{trace}</pre>
            Params: {kwargs}
            """
            import traceback
            tb = sys.exc_info()[2]
            cherrypy.response.headers['Content-Type'] = 'text/html'
            data = except_template.format(
                etype=e.__class__.__name__,
                tostr=str(e),
                trace='\n'.join(traceback.format_tb(tb)),
                kwargs=kwargs
            )

        if cherrypy.response.headers['Content-Type'] == 'application/json':
            data = json.dumps(json.loads(data), indent=2, sort_keys=True)

        try:
            create = getattr(self, 'create')
            f_args = RESTController._function_args(create)
            input_fields = ['{name}:<input type="text" name="{name}">'.format(name=name) for name in
                            f_args]
            create_form = create_form_template.format(
                fields='<br>'.join(input_fields),
                path=self._cp_path_,
                vpath='/'.join(vpath)
            )
        except AttributeError:
            create_form = ''

        def mk_breadcrump(elems):
            return '/'.join([
                '<a href="/{}">{}</a>'.format('/'.join(elems[0:i+1]), e)
                for i, e in enumerate(elems)
            ])

        cherrypy.response.headers['Content-Type'] = 'text/html'
        return template.format(
            docstring='<pre>{}</pre>'.format(self.__doc__) if self.__doc__ else '',
            method=cherrypy.request.method,
            path=self._cp_path_,
            vpath='/'.join(vpath),
            breadcrump=mk_breadcrump(['api', self._cp_path_] + list(vpath)),
            status_code=cherrypy.response.status,
            reponse_headers='\n'.join(
                '{}: {}'.format(k, v) for k, v in cherrypy.response.headers.items()),
            data=data,
            create_form=create_form
        )

    wrapper.exposed = True
    if hasattr(meth, '_cp_config'):
        wrapper._cp_config = meth._cp_config
    return wrapper


class BaseControllerMeta(type):
    def __new__(mcs, name, bases, dct):
        new_cls = type.__new__(mcs, name, bases, dct)

        for a_name, thing in new_cls.__dict__.items():
            if isinstance(thing, (types.FunctionType, types.MethodType))\
                    and getattr(thing, 'exposed', False):

                # @cherrypy.tools.json_out() is incompatible with our browsable_api_view decorator.
                cp_config = getattr(thing, '_cp_config', {})
                if not cp_config.get('tools.json_out.on', False):
                    setattr(new_cls, a_name, browsable_api_view(thing))
        return new_cls


@add_metaclass(BaseControllerMeta)
class BaseController(object):
    """
    Base class for all controllers providing API endpoints.
    """

    def __init__(self):
        logger.info('Initializing controller: %s -> /api/%s',
                    self.__class__.__name__, self._cp_path_)

    @classmethod
    def _parse_function_args(cls, func):
        # pylint: disable=deprecated-method
        if sys.version_info > (3, 0):  # pylint: disable=no-else-return
            sig = inspect.signature(func)
            cargs = [k for k, v in sig.parameters.items()
                     if k != 'self' and v.default is inspect.Parameter.empty and
                     (v.kind == inspect.Parameter.POSITIONAL_ONLY or
                      v.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD)]
        else:
            args = inspect.getargspec(func)
            nd = len(args.args) if not args.defaults else -len(args.defaults)
            cargs = args.args[1:nd]

        # filter out controller path params
        for idx, step in enumerate(cls._cp_path_.split('/')):
            if step[0] == ':':
                param = step[1:]
                if param not in cargs:
                    raise Exception("function '{}' does not have the"
                                    " positional argument '{}' in the {} "
                                    "position".format(func, param, idx))
                cargs.remove(param)
        return cargs

    @classmethod
    def endpoints(cls):
        result = []

        def isfunction(m):
            return inspect.isfunction(m) or inspect.ismethod(m)

        for attr, val in inspect.getmembers(cls, predicate=isfunction):
            if (hasattr(val, 'exposed') and val.exposed):
                args = cls._parse_function_args(val)
                suffix = attr
                action = attr
                if attr == '__call__':
                    suffix = None
                result.append(([], suffix, action, args))
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

    _method_mapping = collections.OrderedDict([
        (('GET', False), ('list', 200)),
        (('PUT', False), ('bulk_set', 200)),
        (('PATCH', False), ('bulk_set', 200)),
        (('POST', False), ('create', 201)),
        (('DELETE', False), ('bulk_delete', 204)),
        (('GET', True), ('get', 200)),
        (('PUT', True), ('set', 200)),
        (('PATCH', True), ('set', 200)),
        (('DELETE', True), ('delete', 204)),
    ])

    @classmethod
    def endpoints(cls):

        def isfunction(m):
            return inspect.isfunction(m) or inspect.ismethod(m)

        result = []
        for attr, val in inspect.getmembers(cls, predicate=isfunction):
            if hasattr(val, 'exposed') and val.exposed and \
                    attr != '_collection' and attr != '_element':
                result.append(([], attr, attr, cls._parse_function_args(val)))

        methods = []
        for k, v in cls._method_mapping.items():
            if not k[1] and hasattr(cls, v[0]):
                methods.append(k[0])
        if methods:
            result.append((methods, None, '_collection', []))
        methods = []
        args = []
        for k, v in cls._method_mapping.items():
            if k[1] and hasattr(cls, v[0]):
                methods.append(k[0])
                if not args:
                    args = cls._parse_function_args(getattr(cls, v[0]))
        if methods:
            result.append((methods, None, '_element', args))

        return result

    @cherrypy.expose
    def _collection(self, *vpath, **params):
        return self._rest_request(False, *vpath, **params)

    @cherrypy.expose
    def _element(self, *vpath, **params):
        return self._rest_request(True, *vpath, **params)

    def _rest_request(self, is_element, *vpath, **params):
        method_name, status_code = self._method_mapping[
            (cherrypy.request.method, is_element)]
        method = getattr(self, method_name, None)

        if cherrypy.request.method not in ['GET', 'DELETE']:
            method = RESTController._takes_json(method)

        method = RESTController._returns_json(method)

        cherrypy.response.status = status_code

        return method(*vpath, **params)

    @staticmethod
    def args_from_json(func):
        func._args_from_json_ = True
        return func

    @staticmethod
    def _function_args(func):
        if sys.version_info > (3, 0):  # pylint: disable=no-else-return
            return list(inspect.signature(func).parameters.keys())
        else:
            return inspect.getargspec(func).args[1:]  # pylint: disable=deprecated-method

    # pylint: disable=W1505
    @staticmethod
    def _takes_json(func):
        def inner(*args, **kwargs):
            if cherrypy.request.headers.get('Content-Type',
                                            '') == 'application/x-www-form-urlencoded':
                if hasattr(func, '_args_from_json_'):  # pylint: disable=no-else-return
                    return func(*args, **kwargs)
                else:
                    return func(kwargs)

            content_length = int(cherrypy.request.headers['Content-Length'])
            body = cherrypy.request.body.read(content_length)
            if not body:
                return func(*args, **kwargs)

            try:
                data = json.loads(body.decode('utf-8'))
            except Exception as e:
                raise cherrypy.HTTPError(400, 'Failed to decode JSON: {}'
                                         .format(str(e)))
            if hasattr(func, '_args_from_json_'):
                kwargs.update(data.items())
                return func(*args, **kwargs)

            kwargs['data'] = data
            return func(*args, **kwargs)
        return inner

    @staticmethod
    def _returns_json(func):
        def inner(*args, **kwargs):
            cherrypy.response.headers['Content-Type'] = 'application/json'
            ret = func(*args, **kwargs)
            return json.dumps(ret).encode('utf8')
        return inner
