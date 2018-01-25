# -*- coding: utf-8 -*-
# pylint: disable=W0212
from __future__ import absolute_import

import importlib
import inspect
import json
import os
import pkgutil
import sys

import cherrypy


def ApiController(path):
    def decorate(cls):
        cls._cp_controller_ = True
        cls._cp_path_ = path
        if not hasattr(cls, '_cp_config'):
            cls._cp_config = {
                'tools.sessions.on': True,
                'tools.autenticate.on': False
            }
        else:
            cls._cp_config['tools.sessions.on'] = True
            if 'tools.autenticate.on' not in cls._cp_config:
                cls._cp_config['tools.autenticate.on'] = False
        return cls
    return decorate


def AuthRequired(enabled=True):
    def decorate(cls):
        if not hasattr(cls, '_cp_config'):
            cls._cp_config = {
                'tools.autenticate.on': enabled
            }
        else:
            cls._cp_config['tools.autenticate.on'] = enabled
        return cls
    return decorate


def load_controllers(mgrmodule):
    # setting sys.path properly when not running under the mgr
    dashboard_dir = os.path.dirname(os.path.realpath(__file__))
    mgr_dir = os.path.dirname(dashboard_dir)
    if mgr_dir not in sys.path:
        sys.path.append(mgr_dir)

    controllers = []
    ctrls_path = "{}/controllers".format(dashboard_dir)
    mods = [mod for _, mod, _ in pkgutil.iter_modules([ctrls_path])]
    for mod_name in mods:
        mod = importlib.import_module('.controllers.{}'.format(mod_name),
                                      package='dashboard_v2')
        for _, cls in mod.__dict__.items():
            if isinstance(cls, type) and hasattr(cls, '_cp_controller_'):
                # found controller
                cls._mgr_module_ = mgrmodule
                controllers.append(cls)

    return controllers


def load_controller(mgrmodule, cls):
    ctrls = load_controllers(mgrmodule)
    for ctrl in ctrls:
        if ctrl.__name__ == cls:
            return ctrl
    raise Exception("Controller class '{}' not found".format(cls))


def _json_error_page(status, message, traceback, version):
    return json.dumps(dict(status=status, detail=message, traceback=traceback,
                           version=version))


class RESTController(object):
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

    _cp_config = {
        'request.error_page': {'default': _json_error_page},
    }

    def _not_implemented(self, is_element):
        methods = [method
                   for ((method, _is_element), (meth, _))
                   in self._method_mapping.items()
                   if _is_element == is_element and hasattr(self, meth)]
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

    @cherrypy.expose
    def default(self, *vpath, **params):
        cherrypy.config.update({
            'error_page.default': _json_error_page})
        is_element = len(vpath) > 0

        (method_name, status_code) = self._method_mapping[
            (cherrypy.request.method, is_element)]
        method = getattr(self, method_name, None)
        if not method:
            self._not_implemented(is_element)

        if cherrypy.request.method not in ['GET', 'DELETE']:
            method = RESTController._takes_json(method)

        if cherrypy.request.method != 'DELETE':
            method = RESTController._returns_json(method)

        cherrypy.response.status = status_code

        return method(*vpath, **params)

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
            cherrypy.serving.response.headers['Content-Type'] = \
                    'application/json'
            ret = func(*args, **kwargs)
            return json.dumps(ret).encode('utf8')
        return inner
