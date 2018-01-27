# -*- coding: utf-8 -*-
# pylint: disable=W0212
from __future__ import absolute_import

import importlib
import inspect
import json
import os
import pkgutil
import sys

import six
import cherrypy


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
            cls._cp_config = dict(cls._cp_config_default)
            config['tools.authenticate.on'] = False
        else:
            cls._cp_config.update(cls._cp_config_default)
            if 'tools.authenticate.on' not in cls._cp_config:
                config['tools.authenticate.on'] = False
        cls._cp_config.update(config)
        return cls
    return decorate


def AuthRequired(enabled=True):
    def decorate(cls):
        if not hasattr(cls, '_cp_config'):
            cls._cp_config = dict(cls._cp_config_default)
            cls._cp_config = {
                'tools.authenticate.on': enabled
            }
        else:
            cls._cp_config.update(cls._cp_config_default)
            cls._cp_config['tools.authenticate.on'] = enabled
        return cls
    return decorate


def load_controllers(mgrmodule):
    # setting sys.path properly when not running under the mgr
    dashboard_dir = os.path.dirname(os.path.realpath(__file__))
    mgr_dir = os.path.dirname(dashboard_dir)
    if mgr_dir not in sys.path:
        sys.path.append(mgr_dir)

    controllers = []
    ctrls_path = '{}/controllers'.format(dashboard_dir)
    mods = [mod for _, mod, _ in pkgutil.iter_modules([ctrls_path])]
    for mod_name in mods:
        mod = importlib.import_module('.controllers.{}'.format(mod_name),
                                      package='dashboard_v2')
        for _, cls in mod.__dict__.items():
            # Controllers MUST be derived from the class BaseController.
            if isinstance(cls, BaseControllerMeta) and \
                    hasattr(cls, '_cp_controller_'):
                cls.mgr = mgrmodule
                controllers.append(cls)

    return controllers


def json_error_page(status, message, traceback, version):
    cherrypy.response.headers['Content-Type'] = 'application/json'
    return json.dumps(dict(status=status, detail=message, traceback=traceback,
                           version=version))


class BaseControllerMeta(type):
    @property
    def mgr(cls):
        """
        :return: Returns the MgrModule instance of this Ceph dashboard module.
        """
        return cls._mgr_module

    @mgr.setter
    def mgr(cls, value):
        """
        :param value: The MgrModule instance of the Ceph dashboard module.
        """
        cls._mgr_module = value

    @property
    def logger(cls):
        """
        :return: Returns the logger belonging to the Ceph dashboard module.
        """
        return cls.mgr.log


class BaseController(six.with_metaclass(BaseControllerMeta, object)):
    """
    Base class for all controllers providing API endpoints.
    """
    _mgr_module = None

    _cp_config_default = {
        'request.error_page': {'default': json_error_page},
    }

    @property
    def mgr(self):
        """
        :return: Returns the MgrModule instance of this Ceph module.
        """
        return self._mgr_module

    @property
    def logger(self):
        """
        :return: Returns the logger belonging to the Ceph dashboard module.
        """
        return self.mgr.log


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

    def _not_implemented(self, obj_key, detail_route_name):
        if detail_route_name:
            try:
                methods = getattr(getattr(self, detail_route_name), 'detail_route_methods')
            except AttributeError:
                raise cherrypy.NotFound()
        else:
            methods = [method
                       for ((method, _is_element), (meth, _))
                       in self._method_mapping.items()
                       if _is_element == obj_key is not None and hasattr(self, meth)]
        cherrypy.response.headers['Allow'] = ','.join(methods)
        raise cherrypy.HTTPError(405, 'Method not implemented.')

    _method_mapping = {
        ('GET', False): ('list', 200),
        ('PUT', False): ('bulk_set', 200),
        ('PATCH', False): ('bulk_set', 200),
        ('POST', False): ('create', 201),
        ('DELETE', False): ('bulk_delete', 204),
        ('GET', True): ('get', 200),
        ('PUT', True): ('set', 200),
        ('PATCH', True): ('set', 200),
        ('DELETE', True): ('delete', 204),
    }

    def _get_method(self, obj_key, detail_route_name):
        if detail_route_name:
            try:
                method = getattr(self, detail_route_name)
                if not getattr(method, 'detail_route'):
                    self._not_implemented(obj_key, detail_route_name)
                if cherrypy.request.method not in getattr(method, 'detail_route_methods'):
                    self._not_implemented(obj_key, detail_route_name)
                return method, 200
            except AttributeError:
                self._not_implemented(obj_key, detail_route_name)
        else:
            method_name, status_code = self._method_mapping[
                (cherrypy.request.method, obj_key is not None)]
            method = getattr(self, method_name, None)
            if not method:
                self._not_implemented(obj_key, detail_route_name)
            return method, status_code

    @cherrypy.expose
    def default(self, *vpath, **params):
        cherrypy.config.update({
            'error_page.default': json_error_page})
        obj_key, detail_route_name = self.split_vpath(vpath)
        method, status_code = self._get_method(obj_key, detail_route_name)

        if cherrypy.request.method not in ['GET', 'DELETE']:
            method = RESTController._takes_json(method)

        if cherrypy.request.method != 'DELETE':
            method = RESTController._returns_json(method)

        cherrypy.response.status = status_code

        obj_key_args = [obj_key] if obj_key else []
        return method(*obj_key_args, **params)

    @staticmethod
    def args_from_json(func):
        func._args_from_json_ = True
        return func

    # pylint: disable=W1505
    @staticmethod
    def _takes_json(func):
        def inner(*args, **kwargs):
            content_length = int(cherrypy.request.headers['Content-Length'])
            body = cherrypy.request.body.read(content_length)
            if not body:
                raise cherrypy.HTTPError(400, 'Empty body. Content-Length={}'
                                         .format(content_length))
            try:
                data = json.loads(body.decode('utf-8'))
            except Exception as e:
                raise cherrypy.HTTPError(400, 'Failed to decode JSON: {}'
                                         .format(str(e)))
            if hasattr(func, '_args_from_json_'):
                if sys.version_info > (3, 0):
                    f_args = list(inspect.signature(func).parameters.keys())
                else:
                    f_args = inspect.getargspec(func).args[1:]
                n_args = []
                for arg in args:
                    n_args.append(arg)
                for arg in f_args:
                    if arg in data:
                        n_args.append(data[arg])
                        data.pop(arg)
                kwargs.update(data)
                return func(*n_args, **kwargs)

            return func(data, *args, **kwargs)
        return inner

    @staticmethod
    def _returns_json(func):
        def inner(*args, **kwargs):
            cherrypy.response.headers['Content-Type'] = 'application/json'
            ret = func(*args, **kwargs)
            return json.dumps(ret).encode('utf8')
        return inner

    @staticmethod
    def split_vpath(vpath):
        if not vpath:
            return None, None
        if len(vpath) == 1:
            return vpath[0], None
        return vpath[0], vpath[1]


def detail_route(methods):
    def decorator(func):
        func.detail_route = True
        func.detail_route_methods = [m.upper() for m in methods]
        return func
    return decorator


class Session(object):
    """
    This class contains all relevant settings related to cherrypy.session.
    """
    NAME = 'session_id'

    # The keys used to store the information in the cherrypy.session.
    USERNAME = '_username'
    TS = '_ts'
    EXPIRE_AT_BROWSER_CLOSE = '_expire_at_browser_close'

    # The default values.
    DEFAULT_EXPIRE = 1200.0


class SessionExpireAtBrowserCloseTool(cherrypy.Tool):
    """
    A CherryPi Tool which takes care that the cookie does not expire
    at browser close if the 'Keep me logged in' checkbox was selected
    on the login page.
    """
    def __init__(self):
        cherrypy.Tool.__init__(self, 'before_finalize', self._callback)

    def _callback(self):
        # Shall the cookie expire at browser close?
        expire_at_browser_close = cherrypy.session.get(
            Session.EXPIRE_AT_BROWSER_CLOSE, True)
        if expire_at_browser_close:
            # Get the cookie and its name.
            cookie = cherrypy.response.cookie
            name = cherrypy.request.config.get(
                'tools.sessions.name', Session.NAME)
            # Make the cookie a session cookie by purging the
            # fields 'expires' and 'max-age'.
            if name in cookie:
                del cookie[name]['expires']
                del cookie[name]['max-age']
