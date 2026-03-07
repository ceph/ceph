import inspect
import collections

if not hasattr(inspect, 'getargspec'):
    ArgSpec = collections.namedtuple('ArgSpec', ['args', 'varargs', 'keywords', 'defaults'])
    def _getargspec_patch(func):
        full_spec = inspect.getfullargspec(func)
        return ArgSpec(full_spec.args, full_spec.varargs, full_spec.varkw, full_spec.defaults)
    inspect.getargspec = _getargspec_patch
