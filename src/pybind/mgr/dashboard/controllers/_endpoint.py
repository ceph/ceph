from typing import Optional

from ._helpers import _get_function_params
from ._version import APIVersion


class Endpoint:

    def __init__(self, method=None, path=None, path_params=None, query_params=None,  # noqa: N802
                 json_response=True, proxy=False, xml=False,
                 version: Optional[APIVersion] = APIVersion.DEFAULT):
        if method is None:
            method = 'GET'
        elif not isinstance(method, str) or \
                method.upper() not in ['GET', 'POST', 'DELETE', 'PUT', 'PATCH']:
            raise TypeError("Possible values for method are: 'GET', 'POST', "
                            "'DELETE', 'PUT', 'PATCH'")

        method = method.upper()

        if method in ['GET', 'DELETE']:
            if path_params is not None:
                raise TypeError("path_params should not be used for {} "
                                "endpoints. All function params are considered"
                                " path parameters by default".format(method))

        if path_params is None:
            if method in ['POST', 'PUT', 'PATCH']:
                path_params = []

        if query_params is None:
            query_params = []

        self.method = method
        self.path = path
        self.path_params = path_params
        self.query_params = query_params
        self.json_response = json_response
        self.proxy = proxy
        self.xml = xml
        self.version = version

    def __call__(self, func):
        if self.method in ['POST', 'PUT', 'PATCH']:
            func_params = _get_function_params(func)
            for param in func_params:
                if param['name'] in self.path_params and not param['required']:
                    raise TypeError("path_params can only reference "
                                    "non-optional function parameters")

        if func.__name__ == '__call__' and self.path is None:
            e_path = ""
        else:
            e_path = self.path

        if e_path is not None:
            e_path = e_path.strip()
            if e_path and e_path[0] != "/":
                e_path = "/" + e_path
            elif e_path == "/":
                e_path = ""

        func._endpoint = {
            'method': self.method,
            'path': e_path,
            'path_params': self.path_params,
            'query_params': self.query_params,
            'json_response': self.json_response,
            'proxy': self.proxy,
            'xml': self.xml,
            'version': self.version
        }
        return func


def Proxy(path=None):  # noqa: N802
    if path is None:
        path = ""
    elif path == "/":
        path = ""
    path += "/{path:.*}"
    return Endpoint(path=path, proxy=True)
