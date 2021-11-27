import collections
from itertools import islice
import json
import logging
import re
from functools import wraps
from textwrap import wrap
from typing import Any, Callable, Dict, Sequence, TypeVar

import cherrypy
from ceph_argparse import ArgumentFormat  # pylint: disable=import-error

from ..exceptions import DashboardException
from ..tools import getargspec

logger = logging.getLogger(__name__)


ENDPOINT_MAP = collections.defaultdict(list)  # type: dict


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


def generate_controller_routes(endpoint, mapper, base_url):
    inst = endpoint.inst
    ctrl_class = endpoint.ctrl

    if endpoint.proxy:
        conditions = None
    else:
        conditions = dict(method=[endpoint.method])

    # base_url can be empty or a URL path that starts with "/"
    # we will remove the trailing "/" if exists to help with the
    # concatenation with the endpoint url below
    if base_url.endswith("/"):
        base_url = base_url[:-1]

    endp_url = endpoint.url

    if endp_url.find("/", 1) == -1:
        parent_url = "{}{}".format(base_url, endp_url)
    else:
        parent_url = "{}{}".format(base_url, endp_url[:endp_url.find("/", 1)])

    # parent_url might be of the form "/.../{...}" where "{...}" is a path parameter
    # we need to remove the path parameter definition
    parent_url = re.sub(r'(?:/\{[^}]+\})$', '', parent_url)
    if not parent_url:  # root path case
        parent_url = "/"

    url = "{}{}".format(base_url, endp_url)

    logger.debug("Mapped [%s] to %s:%s restricted to %s",
                 url, ctrl_class.__name__, endpoint.action,
                 endpoint.method)

    ENDPOINT_MAP[endpoint.url].append(endpoint)

    name = ctrl_class.__name__ + ":" + endpoint.action
    mapper.connect(name, url, controller=inst, action=endpoint.action,
                   conditions=conditions)

    # adding route with trailing slash
    name += "/"
    url += "/"
    mapper.connect(name, url, controller=inst, action=endpoint.action,
                   conditions=conditions)

    return parent_url


def json_error_page(status, message, traceback, version):
    cherrypy.response.headers['Content-Type'] = 'application/json'
    return json.dumps(dict(status=status, detail=message, traceback=traceback,
                           version=version))


def allow_empty_body(func):  # noqa: N802
    """
    The POST/PUT request methods decorated with ``@allow_empty_body``
    are allowed to send empty request body.
    """
    # pylint: disable=protected-access
    try:
        func._cp_config['tools.json_in.force'] = False
    except (AttributeError, KeyError):
        func._cp_config = {'tools.json_in.force': False}
    return func


def validate_ceph_type(validations, component=''):
    def decorator(func):
        @wraps(func)
        def validate_args(*args, **kwargs):
            input_values = kwargs
            for key, ceph_type in validations:
                try:
                    ceph_type.valid(input_values[key])
                except ArgumentFormat as e:
                    raise DashboardException(msg=e,
                                             code='ceph_type_not_valid',
                                             component=component)
            return func(*args, **kwargs)
        return validate_args
    return decorator


T = TypeVar('T')

def paginated(func: Callable[..., Sequence[T]]) -> Callable[..., Sequence[T]]:
    """
    Paginate the output of decorated method

    >>> paginated(range)(5)
    [0,1,2,3,4]

    >>> paginated(range)(5, offset="0")
    [0,1,2,3,4]

    >>> paginated(range)(5, limit="0")
    [0,1,2,3,4]

    >>> paginated(range)(5, offset="0", limit="0")
    [0,1,2,3,4]

    >>> paginated(range)(5, offset="1", limit="0")
    [1,2,3,4]

    >>> paginated(range)(5, offset="1", limit="2")
    [1,2]

    >>> paginated(range)(5, offset="4", limit="2")
    [4]

    >>> paginated(range)(5, offset="5", limit="2")
    []
    """
    @wraps(func)
    def wrapper(*args, offset: int = 0, limit: int = 0, **kwargs) -> Sequence[T]:
        """
        Input and output arguments default to a backward-compatible behaviour

        Args:
            offset (int): default: 0. Item/row count, not pages, starting with 0
            limit (int): default: 0. Item/row count. 0 means no limit
        Returns:
            Iterable<T>: an iterable with equal (or less) amount of items requested
            (e.g.: in case there less remaining items).

        Additionally, the total number of items returned from the unpaginated
        method is returned as an HTTP Header `X-Total-Count`. The purpose of
        this interface is not to break the existing response format and keep
        the current API version of the unpaginated endpoints.
        """
        ret = func(*args, **kwargs)
        start = int(offset)
        limit = int(limit)
        if offset or limit:
            end = start + limit if limit > 0 else None
            cherrypy.response.headers['X-Total-Count'] = len(ret)
            return list(islice(ret, start, end))
        return ret
    return wrapper


def dot_getitem(_dict: Dict[str, Any], dot_key):
    """
    Support JS dot-like access to nested objects

    >>> sort.strip({'osd': {'host': {'name': 'example.com'}}}, 'osd.host.name')
    'example.com'
    """
    for key in dot_key.split('.'):
        _dict = _dict[key]
    return _dict


def sorting(func: Callable[..., Sequence[T]]) -> Callable[..., Sequence[T]]:
    """
    Sort the output of the decorated method

    >>> sorting(iter)([dict(id=1, value="foo"), dict(id=2, value="bar")])
    [{'id': 1, 'value': 'foo'}, {'id': 2, 'value': 'bar'}]

    >>> sorting(iter)([dict(id=1, value="foo"), dict(id=2, value="bar")], sort="id")
    [{'id': 1, 'value': 'foo'}, {'id': 2, 'value': 'bar'}]

    >>> sorting(iter)([dict(id=1, value="foo"), dict(id=2, value="bar")], sort="-id")
    [{'id': 2, 'value': 'bar'}, {'id': 1, 'value': 'foo'}]

    >>> sorting(iter)([dict(id=1, value="foo"), dict(id=2, value="bar")], sort="value")
    [{'id': 2, 'value': 'bar'}, {'id': 1, 'value': 'foo'}]

    >>> sorting(iter)([dict(id=1, value="foo"), dict(id=2, value="bar")], sort="-value")
    [{'id': 1, 'value': 'foo'}, {'id': 2, 'value': 'bar'}]
    """
    @wraps(func)
    def wrapper(*args, sort: str = "", **kwargs) -> Sequence[T]:
        """
        Input and output arguments default to a backward-compatible behaviour

        Args:
            sort (string): default: "". Format: "[-]<field_name>".

        Returns:
            Iterable<T>: an iterable sorted according to the 'field_name' in
            lexicographically descending order if the 'field_name' is prefixed
            with '-'; otherwise ascending.
        """
        ret = func(*args, **kwargs)
        if sort:
            desc = sort.startswith('-')
            field = sort.strip('-')
            return sorted(ret, key=lambda i: dot_getitem(i, field), reverse=desc)
        return ret
    return wrapper


def search_in_dict(obj: Dict[Any, Any], text: str, ignore_case=False):
    """
    >>> search(dict(a=1,b="Bar_Foo_Bar"),"Fo")
    True
    >>> search(dict(a=1,b="Bar_Foo_Bar"),"fo")
    False
    >>> search(dict(a=1,b="Bar_Foo_Bar"),"fo", ingore_case=True)
    True
    """
    return any(
        text.lower() in str(value).lower() if ignore_case else text in str(value)
        for value in obj.values()
        )


def searching(func: Callable[..., Sequence[T]]) -> Callable[..., Sequence[T]]:
    """
    Search text in the output of the decorated method

    >>> searching(iter)([dict(id=1, value="foo"), dict(id=2, value="bar")], search="bar")
    [dict(id=2, value="bar")]
    """
    @wraps(func)
    def wrapper(*args, search: str = "", **kwargs) -> Sequence[T]:
        ret = func(*args, **kwargs)
        if search:
            ret = filter(lambda x: search_in_dict(x, search), ret)
        return ret
    return wrapper


def with_server_timing(getter):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            ret, metric = func(*args, **kwargs)
            cherrypy.response.headers['Server-Timing'] = getter(ret, metric)
            return ret
        return wrapper
    return decorator
