from functools import wraps

import cherrypy

from ..tools import TaskManager
from ._helpers import _get_function_params


class Task:
    def __init__(self, name, metadata, wait_for=5.0, exception_handler=None):
        self.name = name
        if isinstance(metadata, list):
            self.metadata = {e[1:-1]: e for e in metadata}
        else:
            self.metadata = metadata
        self.wait_for = wait_for
        self.exception_handler = exception_handler

    def _gen_arg_map(self, func, args, kwargs):
        arg_map = {}
        params = _get_function_params(func)

        args = args[1:]  # exclude self
        for idx, param in enumerate(params):
            if idx < len(args):
                arg_map[param['name']] = args[idx]
            else:
                if param['name'] in kwargs:
                    arg_map[param['name']] = kwargs[param['name']]
                else:
                    assert not param['required'], "{0} is required".format(param['name'])
                    arg_map[param['name']] = param['default']

            if param['name'] in arg_map:
                # This is not a type error. We are using the index here.
                arg_map[idx+1] = arg_map[param['name']]

        return arg_map

    def _get_metadata(self, arg_map):
        metadata = {}
        for k, v in self.metadata.items():
            if isinstance(v, str) and v and v[0] == '{' and v[-1] == '}':
                param = v[1:-1]
                try:
                    pos = int(param)
                    metadata[k] = arg_map[pos]
                except ValueError:
                    if param.find('.') == -1:
                        metadata[k] = arg_map[param]
                    else:
                        path = param.split('.')
                        metadata[k] = arg_map[path[0]]
                        for i in range(1, len(path)):
                            metadata[k] = metadata[k][path[i]]
            else:
                metadata[k] = v
        return metadata

    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            arg_map = self._gen_arg_map(func, args, kwargs)
            metadata = self._get_metadata(arg_map)

            task = TaskManager.run(self.name, metadata, func, args, kwargs,
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
                return {'name': self.name, 'metadata': metadata}
            return value
        return wrapper
