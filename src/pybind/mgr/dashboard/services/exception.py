# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json
from contextlib import contextmanager
import logging
import six

import cherrypy

import rbd
import rados

from ..services.ceph_service import SendCommandError
from ..exceptions import ViewCacheNoDataException, DashboardException
from ..tools import wraps


logger = logging.getLogger('exception')


if six.PY2:
    # Monkey-patch a __call__ method into @contextmanager to make
    # it compatible to Python 3

    # pylint: disable=no-name-in-module,ungrouped-imports
    from contextlib import GeneratorContextManager

    def init(self, *args):
        if len(args) == 1:
            self.gen = args[0]
        elif len(args) == 3:
            self.func, self.args, self.kwargs = args
        else:
            raise TypeError()

    def enter(self):
        if hasattr(self, 'func'):
            self.gen = self.func(*self.args, **self.kwargs)
        try:
            return self.gen.next()
        except StopIteration:
            raise RuntimeError("generator didn't yield")

    def call(self, f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            with self:
                return f(*args, **kwargs)

        return wrapper

    GeneratorContextManager.__init__ = init
    GeneratorContextManager.__enter__ = enter
    GeneratorContextManager.__call__ = call

    # pylint: disable=function-redefined
    def contextmanager(func):  # noqa: F811

        @wraps(func)
        def helper(*args, **kwds):
            return GeneratorContextManager(func, args, kwds)

        return helper


def serialize_dashboard_exception(e, include_http_status=False, task=None):
    """
    :type e: Exception
    :param include_http_status: Used for Tasks, where the HTTP status code is not available.
    """
    from ..tools import ViewCache
    if isinstance(e, ViewCacheNoDataException):
        return {'status': ViewCache.VALUE_NONE, 'value': None}

    out = dict(detail=str(e))
    try:
        out['code'] = e.code
    except AttributeError:
        pass
    component = getattr(e, 'component', None)
    out['component'] = component if component else None
    if include_http_status:
        out['status'] = getattr(e, 'status', 500)
    if task:
        out['task'] = dict(name=task.name, metadata=task.metadata)
    return out


def dashboard_exception_handler(handler, *args, **kwargs):
    try:
        with handle_rados_error(component=None):  # make the None controller the fallback.
            return handler(*args, **kwargs)
    # Don't catch cherrypy.* Exceptions.
    except (ViewCacheNoDataException, DashboardException) as e:
        logger.exception('Dashboard Exception')
        cherrypy.response.headers['Content-Type'] = 'application/json'
        cherrypy.response.status = getattr(e, 'status', 400)
        return json.dumps(serialize_dashboard_exception(e)).encode('utf-8')


@contextmanager
def handle_rbd_error():
    try:
        yield
    except rbd.OSError as e:
        raise DashboardException(e, component='rbd')
    except rbd.Error as e:
        raise DashboardException(e, component='rbd', code=e.__class__.__name__)


@contextmanager
def handle_rados_error(component):
    try:
        yield
    except rados.OSError as e:
        raise DashboardException(e, component=component)
    except rados.Error as e:
        raise DashboardException(e, component=component, code=e.__class__.__name__)


@contextmanager
def handle_send_command_error(component):
    try:
        yield
    except SendCommandError as e:
        raise DashboardException(e, component=component)


@contextmanager
def handle_orchestrator_error(component):
    try:
        yield
    except RuntimeError as e:
        # how to catch remote error e.g. NotImplementedError ?
        raise DashboardException(e, component=component)
