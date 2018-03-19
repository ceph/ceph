# -*- coding: utf-8 -*-
# pylint: disable=W0212
from __future__ import absolute_import

import collections
import datetime
import importlib
import inspect
import json
import os
import pkgutil
import sys
import time
import threading

import cherrypy

from . import logger


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
            cls._cp_config = {}
        if 'tools.authenticate.on' not in cls._cp_config:
            config['tools.authenticate.on'] = False
        cls._cp_config.update(config)
        return cls
    return decorate


def AuthRequired(enabled=True):
    def decorate(cls):
        if not hasattr(cls, '_cp_config'):
            cls._cp_config = {
                'tools.authenticate.on': enabled
            }
        else:
            cls._cp_config['tools.authenticate.on'] = enabled
        return cls
    return decorate


def load_controllers():
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
                                      package='dashboard')
        for _, cls in mod.__dict__.items():
            # Controllers MUST be derived from the class BaseController.
            if inspect.isclass(cls) and issubclass(cls, BaseController) and \
                    hasattr(cls, '_cp_controller_'):
                controllers.append(cls)

    return controllers


def json_error_page(status, message, traceback, version):
    cherrypy.response.headers['Content-Type'] = 'application/json'
    return json.dumps(dict(status=status, detail=message, traceback=traceback,
                           version=version))


class BaseController(object):
    """
    Base class for all controllers providing API endpoints.
    """


class RequestLoggingTool(cherrypy.Tool):
    def __init__(self):
        cherrypy.Tool.__init__(self, 'before_handler', self.request_begin,
                               priority=95)

    def _setup(self):
        cherrypy.Tool._setup(self)
        cherrypy.request.hooks.attach('on_end_request', self.request_end,
                                      priority=5)
        cherrypy.request.hooks.attach('after_error_response', self.request_error,
                                      priority=5)

    def _get_user(self):
        if hasattr(cherrypy.serving, 'session'):
            return cherrypy.session.get(Session.USERNAME)
        return None

    def request_begin(self):
        req = cherrypy.request
        user = self._get_user()
        if user:
            logger.debug("[%s:%s] [%s] [%s] %s", req.remote.ip,
                         req.remote.port, req.method, user, req.path_info)
        else:
            logger.debug("[%s:%s] [%s] %s", req.remote.ip,
                         req.remote.port, req.method, req.path_info)

    def request_error(self):
        self._request_log(logger.error)
        logger.error(cherrypy.response.body)

    def request_end(self):
        status = cherrypy.response.status[:3]
        if status in ["401"]:
            # log unauthorized accesses
            self._request_log(logger.warning)
        else:
            self._request_log(logger.info)

    def _format_bytes(self, num):
        units = ['B', 'K', 'M', 'G']

        format_str = "{:.0f}{}"
        for i, unit in enumerate(units):
            div = 2**(10*i)
            if num < 2**(10*(i+1)):
                if num % div == 0:
                    format_str = "{}{}"
                else:
                    div = float(div)
                    format_str = "{:.1f}{}"
                return format_str.format(num/div, unit[0])

        # content-length bigger than 1T!! return value in bytes
        return "{}B".format(num)

    def _request_log(self, logger_fn):
        req = cherrypy.request
        res = cherrypy.response
        lat = time.time() - res.time
        user = self._get_user()
        status = res.status[:3] if isinstance(res.status, str) else res.status
        if 'Content-Length' in res.headers:
            length = self._format_bytes(res.headers['Content-Length'])
        else:
            length = self._format_bytes(0)
        if user:
            logger_fn("[%s:%s] [%s] [%s] [%s] [%s] [%s] %s", req.remote.ip,
                      req.remote.port, req.method, status,
                      "{0:.3f}s".format(lat), user, length, req.path_info)
        else:
            logger_fn("[%s:%s] [%s] [%s] [%s] [%s] %s", req.remote.ip,
                      req.remote.port, req.method, status,
                      "{0:.3f}s".format(lat), length, req.path_info)


# pylint: disable=too-many-instance-attributes
class ViewCache(object):
    VALUE_OK = 0
    VALUE_STALE = 1
    VALUE_NONE = 2
    VALUE_EXCEPTION = 3

    class GetterThread(threading.Thread):
        def __init__(self, view, fn, args, kwargs):
            super(ViewCache.GetterThread, self).__init__()
            self._view = view
            self.event = threading.Event()
            self.fn = fn
            self.args = args
            self.kwargs = kwargs

        # pylint: disable=broad-except
        def run(self):
            try:
                t0 = time.time()
                val = self.fn(*self.args, **self.kwargs)
                t1 = time.time()
            except Exception as ex:
                logger.exception("Error while calling fn=%s ex=%s", self.fn,
                                 str(ex))
                self._view.value = None
                self._view.value_when = None
                self._view.getter_thread = None
                self._view.exception = ex
            else:
                with self._view.lock:
                    self._view.latency = t1 - t0
                    self._view.value = val
                    self._view.value_when = datetime.datetime.now()
                    self._view.getter_thread = None
                    self._view.exception = None

            self.event.set()

    class RemoteViewCache(object):
        # Return stale data if
        STALE_PERIOD = 1.0

        def __init__(self, timeout):
            self.getter_thread = None
            # Consider data within 1s old to be sufficiently fresh
            self.timeout = timeout
            self.event = threading.Event()
            self.value_when = None
            self.value = None
            self.latency = 0
            self.exception = None
            self.lock = threading.Lock()

        def run(self, fn, args, kwargs):
            """
            If data less than `stale_period` old is available, return it
            immediately.
            If an attempt to fetch data does not complete within `timeout`, then
            return the most recent data available, with a status to indicate that
            it is stale.

            Initialization does not count towards the timeout, so the first call
            on one of these objects during the process lifetime may be slower
            than subsequent calls.

            :return: 2-tuple of value status code, value
            """
            with self.lock:
                now = datetime.datetime.now()
                if self.value_when and now - self.value_when < datetime.timedelta(
                        seconds=self.STALE_PERIOD):
                    return ViewCache.VALUE_OK, self.value

                if self.getter_thread is None:
                    self.getter_thread = ViewCache.GetterThread(self, fn, args,
                                                                kwargs)
                    self.getter_thread.start()

                ev = self.getter_thread.event

            success = ev.wait(timeout=self.timeout)

            with self.lock:
                if success:
                    # We fetched the data within the timeout
                    if self.exception:
                        # execution raised an exception
                        return ViewCache.VALUE_EXCEPTION, self.exception
                    return ViewCache.VALUE_OK, self.value
                elif self.value_when is not None:
                    # We have some data, but it doesn't meet freshness requirements
                    return ViewCache.VALUE_STALE, self.value
                # We have no data, not even stale data
                return ViewCache.VALUE_NONE, None

    def __init__(self, timeout=5):
        self.timeout = timeout
        self.cache_by_args = {}

    def __call__(self, fn):
        def wrapper(*args, **kwargs):
            rvc = self.cache_by_args.get(args, None)
            if not rvc:
                rvc = ViewCache.RemoteViewCache(self.timeout)
                self.cache_by_args[args] = rvc
            return rvc.run(fn, args, kwargs)
        return wrapper


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

    def _not_implemented(self, is_sub_path):
        methods = [method
                   for ((method, _is_element), (meth, _))
                   in self._method_mapping.items()
                   if _is_element == is_sub_path is not None and hasattr(self, meth)]
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

    def _get_method(self, vpath):
        is_sub_path = bool(len(vpath))
        try:
            method_name, status_code = self._method_mapping[
                (cherrypy.request.method, is_sub_path)]
        except KeyError:
            self._not_implemented(is_sub_path)
        method = getattr(self, method_name, None)
        if not method:
            self._not_implemented(is_sub_path)
        return method, status_code

    @cherrypy.expose
    def default(self, *vpath, **params):
        method, status_code = self._get_method(vpath)

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
        logger.debug("expire at browser close: %s", expire_at_browser_close)
        if expire_at_browser_close:
            # Get the cookie and its name.
            cookie = cherrypy.response.cookie
            name = cherrypy.request.config.get(
                'tools.sessions.name', Session.NAME)
            # Make the cookie a session cookie by purging the
            # fields 'expires' and 'max-age'.
            logger.debug("expire at browser close: removing 'expires' and 'max-age'")
            if name in cookie:
                del cookie[name]['expires']
                del cookie[name]['max-age']


class NotificationQueue(threading.Thread):
    _ALL_TYPES_ = '__ALL__'
    _listeners = collections.defaultdict(set)
    _lock = threading.Lock()
    _cond = threading.Condition()
    _queue = collections.deque()
    _running = False
    _instance = None

    def __init__(self):
        super(NotificationQueue, self).__init__()

    @classmethod
    def start_queue(cls):
        with cls._lock:
            if cls._instance:
                # the queue thread is already running
                return
            cls._running = True
            cls._instance = NotificationQueue()
        logger.debug("starting notification queue")
        cls._instance.start()

    @classmethod
    def stop(cls):
        with cls._lock:
            if not cls._instance:
                # the queue thread was not started
                return
            instance = cls._instance
            cls._instance = None
            cls._running = False
        with cls._cond:
            cls._cond.notify()
        logger.debug("waiting for notification queue to finish")
        instance.join()
        logger.debug("notification queue stopped")

    @classmethod
    def register(cls, func, types=None):
        """Registers function to listen for notifications

        If the second parameter `types` is omitted, the function in `func`
        parameter will be called for any type of notifications.

        Args:
            func (function): python function ex: def foo(val)
            types (str|list): the single type to listen, or a list of types
        """
        with cls._lock:
            if not types:
                cls._listeners[cls._ALL_TYPES_].add(func)
                return
            if isinstance(types, str):
                cls._listeners[types].add(func)
            elif isinstance(types, list):
                for typ in types:
                    cls._listeners[typ].add(func)
            else:
                raise Exception("types param is neither a string nor a list")

    @classmethod
    def new_notification(cls, notify_type, notify_value):
        cls._queue.append((notify_type, notify_value))
        with cls._cond:
            cls._cond.notify()

    @classmethod
    def notify_listeners(cls, events):
        for ev in events:
            notify_type, notify_value = ev
            with cls._lock:
                listeners = list(cls._listeners[notify_type])
                listeners.extend(cls._listeners[cls._ALL_TYPES_])
            for listener in listeners:
                listener(notify_value)

    def run(self):
        logger.debug("notification queue started")
        while self._running:
            private_buffer = []
            logger.debug("NQ: processing queue: %s", len(self._queue))
            try:
                while True:
                    private_buffer.append(self._queue.popleft())
            except IndexError:
                pass
            self.notify_listeners(private_buffer)
            with self._cond:
                self._cond.wait(1.0)
        # flush remaining events
        logger.debug("NQ: flush remaining events: %s", len(self._queue))
        self.notify_listeners(self._queue)
        self._queue.clear()
        logger.debug("notification queue finished")
