import collections
import json
import logging
import re
from functools import wraps
from urllib.parse import unquote

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


def _decode_path_params(url, proxy):
    """Locate raw whole-segment parameters using Routes' parsed components."""
    from routes.route import Route

    positions = {}
    separators = 0
    for part in Route(None, url or '/').routelist:
        if isinstance(part, dict):
            positions[part['name']] = separators
        else:
            separators += part.count('/')

    def decode(environ, result):
        path = environ['REQUEST_URI'].partition('?')[0]
        # CherryPy routes slash-only URLs to the root proxy endpoint.
        if proxy and not path.strip('/'):
            path = '/'
        segments = path.split('/', separators)
        # Reject separator changes outside a proxy's final wildcard.
        if (len(segments) != separators + 1 or any(not part for part in segments[1:-1])
                or (not proxy and '/' in segments[-1])):
            return False
        try:
            values = [unquote(part, errors='strict') for part in segments]
        except UnicodeDecodeError:
            raise cherrypy.HTTPError(400, 'URL path must be UTF-8')
        result.update((name, values[index]) for name, index in positions.items())
        return True

    return decode


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
    conditions = dict(conditions or {}, function=_decode_path_params(url, endpoint.proxy))

    logger.debug("Mapped [%s] to %s:%s restricted to %s",
                 url, ctrl_class.__name__, endpoint.action,
                 endpoint.method)

    ENDPOINT_MAP[endpoint.url].append(endpoint)

    name = ctrl_class.__name__ + ":" + endpoint.action
    mapper.connect(name, url, controller=inst, action=endpoint.action,
                   conditions=conditions)

    # A proxy's final wildcard already includes trailing slashes.
    if not endpoint.proxy:
        name += "/"
        url += "/"
        conditions = dict(conditions, function=_decode_path_params(url, endpoint.proxy))
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
