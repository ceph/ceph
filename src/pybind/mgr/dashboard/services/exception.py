# -*- coding: utf-8 -*-

import json
import logging
from contextlib import contextmanager

import cephfs
import cherrypy
import rados
import rbd
from orchestrator import OrchestratorError

from ..exceptions import DashboardException, ViewCacheNoDataException
from ..rest_client import RequestException
from ..services.ceph_service import SendCommandError

logger = logging.getLogger('exception')


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
        out['status'] = getattr(e, 'status', 500)  # type: ignore
    if task:
        out['task'] = dict(name=task.name, metadata=task.metadata)  # type: ignore
    return out


# pylint: disable=broad-except
def dashboard_exception_handler(handler, *args, **kwargs):
    try:
        with handle_rados_error(component=None):  # make the None controller the fallback.
            return handler(*args, **kwargs)
    # pylint: disable=try-except-raise
    except (cherrypy.HTTPRedirect, cherrypy.NotFound, cherrypy.HTTPError):
        raise
    except (ViewCacheNoDataException, DashboardException) as error:
        logger.exception('Dashboard Exception')
        cherrypy.response.headers['Content-Type'] = 'application/json'
        cherrypy.response.status = getattr(error, 'status', 400)
        return json.dumps(serialize_dashboard_exception(error)).encode('utf-8')
    except Exception as error:
        logger.exception('Internal Server Error')
        raise error


@contextmanager
def handle_cephfs_error():
    try:
        yield
    except cephfs.OSError as e:
        raise DashboardException(e, component='cephfs') from e


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
    except OrchestratorError as e:
        raise DashboardException(e, component=component)


@contextmanager
def handle_request_error(component):
    try:
        yield
    except RequestException as e:
        if e.content:
            content = json.loads(e.content)
            content_message = content.get('message')
            if content_message:
                raise DashboardException(
                    msg=content_message, component=component)
        raise DashboardException(e=e, component=component)


@contextmanager
def handle_error(component, http_status_code=None):
    try:
        yield
    except Exception as e:  # pylint: disable=broad-except
        raise DashboardException(e, component=component, http_status_code=http_status_code)


@contextmanager
def handle_custom_error(component, http_status_code=None, exceptions=()):
    try:
        yield
    except exceptions as e:
        raise DashboardException(e, component=component, http_status_code=http_status_code)
