import collections
import inspect
from functools import wraps
from typing import Optional

import cherrypy

from ..security import Permission
from ._base_controller import BaseController
from ._endpoint import Endpoint
from ._helpers import _get_function_params
from ._permissions import _set_func_permissions
from ._version import APIVersion


class RESTController(BaseController, skip_registry=True):
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
    * singleton_set(data)
    * delete(key)

    Test with curl:

    curl -H "Content-Type: application/json" -X POST \
         -d '{"username":"xyz","password":"xyz"}'  https://127.0.0.1:8443/foo
    curl https://127.0.0.1:8443/foo
    curl https://127.0.0.1:8443/foo/0

    """

    # resource id parameter for using in get, set, and delete methods
    # should be overridden by subclasses.
    # to specify a composite id (two parameters) use '/'. e.g., "param1/param2".
    # If subclasses don't override this property we try to infer the structure
    # of the resource ID.
    RESOURCE_ID: Optional[str] = None

    _permission_map = {
        'GET': Permission.READ,
        'POST': Permission.CREATE,
        'PUT': Permission.UPDATE,
        'DELETE': Permission.DELETE
    }

    _method_mapping = collections.OrderedDict([
        ('list', {'method': 'GET', 'resource': False, 'status': 200, 'version': APIVersion.DEFAULT}),  # noqa E501 #pylint: disable=line-too-long
        ('create', {'method': 'POST', 'resource': False, 'status': 201, 'version': APIVersion.DEFAULT}),  # noqa E501 #pylint: disable=line-too-long
        ('bulk_set', {'method': 'PUT', 'resource': False, 'status': 200, 'version': APIVersion.DEFAULT}),  # noqa E501 #pylint: disable=line-too-long
        ('bulk_delete', {'method': 'DELETE', 'resource': False, 'status': 204, 'version': APIVersion.DEFAULT}),  # noqa E501 #pylint: disable=line-too-long
        ('get', {'method': 'GET', 'resource': True, 'status': 200, 'version': APIVersion.DEFAULT}),
        ('delete', {'method': 'DELETE', 'resource': True, 'status': 204, 'version': APIVersion.DEFAULT}),  # noqa E501 #pylint: disable=line-too-long
        ('set', {'method': 'PUT', 'resource': True, 'status': 200, 'version': APIVersion.DEFAULT}),
        ('singleton_set', {'method': 'PUT', 'resource': False, 'status': 200, 'version': APIVersion.DEFAULT})  # noqa E501 #pylint: disable=line-too-long
    ])

    @classmethod
    def infer_resource_id(cls):
        if cls.RESOURCE_ID is not None:
            return cls.RESOURCE_ID.split('/')
        for k, v in cls._method_mapping.items():
            func = getattr(cls, k, None)
            while hasattr(func, "__wrapped__"):
                assert func
                func = func.__wrapped__
            if v['resource'] and func:
                path_params = cls.get_path_param_names()
                params = _get_function_params(func)
                return [p['name'] for p in params
                        if p['required'] and p['name'] not in path_params]
        return None

    @classmethod
    def endpoints(cls):
        result = super().endpoints()
        res_id_params = cls.infer_resource_id()

        for name, func in inspect.getmembers(cls, predicate=callable):
            endpoint_params = {
                'no_resource_id_params': False,
                'status': 200,
                'method': None,
                'query_params': None,
                'path': '',
                'version': APIVersion.DEFAULT,
                'sec_permissions': hasattr(func, '_security_permissions'),
                'permission': None,
            }
            if name in cls._method_mapping:
                cls._update_endpoint_params_method_map(
                    func, res_id_params, endpoint_params, name=name)

            elif hasattr(func, "__collection_method__"):
                cls._update_endpoint_params_collection_map(func, endpoint_params)

            elif hasattr(func, "__resource_method__"):
                cls._update_endpoint_params_resource_method(
                    res_id_params, endpoint_params, func)

            else:
                continue

            if endpoint_params['no_resource_id_params']:
                raise TypeError("Could not infer the resource ID parameters for"
                                " method {} of controller {}. "
                                "Please specify the resource ID parameters "
                                "using the RESOURCE_ID class property"
                                .format(func.__name__, cls.__name__))

            if endpoint_params['method'] in ['GET', 'DELETE']:
                params = _get_function_params(func)
                if res_id_params is None:
                    res_id_params = []
                if endpoint_params['query_params'] is None:
                    endpoint_params['query_params'] = [p['name'] for p in params  # type: ignore
                                                       if p['name'] not in res_id_params]

            func = cls._status_code_wrapper(func, endpoint_params['status'])
            endp_func = Endpoint(endpoint_params['method'], path=endpoint_params['path'],
                                 query_params=endpoint_params['query_params'],
                                 version=endpoint_params['version'])(func)  # type: ignore
            if endpoint_params['permission']:
                _set_func_permissions(endp_func, [endpoint_params['permission']])
            result.append(cls.Endpoint(cls, endp_func))

        return result

    @classmethod
    def _update_endpoint_params_resource_method(cls, res_id_params, endpoint_params, func):
        if not res_id_params:
            endpoint_params['no_resource_id_params'] = True
        else:
            path_params = ["{{{}}}".format(p) for p in res_id_params]
            endpoint_params['path'] += "/{}".format("/".join(path_params))
            if func.__resource_method__['path']:
                endpoint_params['path'] += func.__resource_method__['path']
            else:
                endpoint_params['path'] += "/{}".format(func.__name__)
        endpoint_params['status'] = func.__resource_method__['status']
        endpoint_params['method'] = func.__resource_method__['method']
        endpoint_params['version'] = func.__resource_method__['version']
        endpoint_params['query_params'] = func.__resource_method__['query_params']
        if not endpoint_params['sec_permissions']:
            endpoint_params['permission'] = cls._permission_map[endpoint_params['method']]

    @classmethod
    def _update_endpoint_params_collection_map(cls, func, endpoint_params):
        if func.__collection_method__['path']:
            endpoint_params['path'] = func.__collection_method__['path']
        else:
            endpoint_params['path'] = "/{}".format(func.__name__)
        endpoint_params['status'] = func.__collection_method__['status']
        endpoint_params['method'] = func.__collection_method__['method']
        endpoint_params['query_params'] = func.__collection_method__['query_params']
        endpoint_params['version'] = func.__collection_method__['version']
        if not endpoint_params['sec_permissions']:
            endpoint_params['permission'] = cls._permission_map[endpoint_params['method']]

    @classmethod
    def _update_endpoint_params_method_map(cls, func, res_id_params, endpoint_params, name=None):
        meth = cls._method_mapping[func.__name__ if not name else name]  # type: dict

        if meth['resource']:
            if not res_id_params:
                endpoint_params['no_resource_id_params'] = True
            else:
                path_params = ["{{{}}}".format(p) for p in res_id_params]
                endpoint_params['path'] += "/{}".format("/".join(path_params))

        endpoint_params['status'] = meth['status']
        endpoint_params['method'] = meth['method']
        if hasattr(func, "__method_map_method__"):
            endpoint_params['version'] = func.__method_map_method__['version']
        if not endpoint_params['sec_permissions']:
            endpoint_params['permission'] = cls._permission_map[endpoint_params['method']]

    @classmethod
    def _status_code_wrapper(cls, func, status_code):
        @wraps(func)
        def wrapper(*vpath, **params):
            cherrypy.response.status = status_code
            return func(*vpath, **params)

        return wrapper

    @staticmethod
    def Resource(method=None, path=None, status=None, query_params=None,  # noqa: N802
                 version: Optional[APIVersion] = APIVersion.DEFAULT):
        if not method:
            method = 'GET'

        if status is None:
            status = 200

        def _wrapper(func):
            func.__resource_method__ = {
                'method': method,
                'path': path,
                'status': status,
                'query_params': query_params,
                'version': version
            }
            return func
        return _wrapper

    @staticmethod
    def MethodMap(resource=False, status=None,
                  version: Optional[APIVersion] = APIVersion.DEFAULT):  # noqa: N802

        if status is None:
            status = 200

        def _wrapper(func):
            func.__method_map_method__ = {
                'resource': resource,
                'status': status,
                'version': version
            }
            return func
        return _wrapper

    @staticmethod
    def Collection(method=None, path=None, status=None, query_params=None,  # noqa: N802
                   version: Optional[APIVersion] = APIVersion.DEFAULT):
        if not method:
            method = 'GET'

        if status is None:
            status = 200

        def _wrapper(func):
            func.__collection_method__ = {
                'method': method,
                'path': path,
                'status': status,
                'query_params': query_params,
                'version': version
            }
            return func
        return _wrapper
