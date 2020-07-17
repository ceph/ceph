# -*- coding: utf-8 -*-
from __future__ import absolute_import

import inspect
import json
import ipaddress
import logging

import collections
from datetime import datetime, timedelta
from distutils.util import strtobool
import fnmatch
import time
import threading
import urllib

import cherrypy

from . import mgr
from .exceptions import ViewCacheNoDataException
from .settings import Settings
from .services.auth import JwtManager

try:
    from typing import Any, AnyStr, Callable, DefaultDict, Deque,\
        Dict, List, Set, Tuple, Union  # noqa pylint: disable=unused-import
except ImportError:
    pass  # For typing only


class RequestLoggingTool(cherrypy.Tool):
    def __init__(self):
        cherrypy.Tool.__init__(self, 'before_handler', self.request_begin,
                               priority=10)
        self.logger = logging.getLogger('request')

    def _setup(self):
        cherrypy.Tool._setup(self)
        cherrypy.request.hooks.attach('on_end_request', self.request_end,
                                      priority=5)
        cherrypy.request.hooks.attach('after_error_response', self.request_error,
                                      priority=5)

    def request_begin(self):
        req = cherrypy.request
        user = JwtManager.get_username()
        # Log the request.
        self.logger.debug('[%s:%s] [%s] [%s] %s', req.remote.ip, req.remote.port,
                          req.method, user, req.path_info)
        # Audit the request.
        if Settings.AUDIT_API_ENABLED and req.method not in ['GET']:
            url = build_url(req.remote.ip, scheme=req.scheme,
                            port=req.remote.port)
            msg = '[DASHBOARD] from=\'{}\' path=\'{}\' method=\'{}\' ' \
                'user=\'{}\''.format(url, req.path_info, req.method, user)
            if Settings.AUDIT_API_LOG_PAYLOAD:
                params = dict(req.params or {}, **get_request_body_params(req))
                # Hide sensitive data like passwords, secret keys, ...
                # Extend the list of patterns to search for if necessary.
                # Currently parameters like this are processed:
                # - secret_key
                # - user_password
                # - new_passwd_to_login
                keys = []
                for key in ['password', 'passwd', 'secret']:
                    keys.extend([x for x in params.keys() if key in x])
                for key in keys:
                    params[key] = '***'
                msg = '{} params=\'{}\''.format(msg, json.dumps(params))
            mgr.cluster_log('audit', mgr.CLUSTER_LOG_PRIO_INFO, msg)

    def request_error(self):
        self._request_log(self.logger.error)
        self.logger.error(cherrypy.response.body)

    def request_end(self):
        status = cherrypy.response.status[:3]
        if status in ["401", "403"]:
            # log unauthorized accesses
            self._request_log(self.logger.warning)
        else:
            self._request_log(self.logger.info)

    def _format_bytes(self, num):
        units = ['B', 'K', 'M', 'G']

        if isinstance(num, str):
            try:
                num = int(num)
            except ValueError:
                return "n/a"

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
        user = JwtManager.get_username()
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
            logger_fn("[%s:%s] [%s] [%s] [%s] [%s] [%s] %s", req.remote.ip,
                      req.remote.port, req.method, status,
                      "{0:.3f}s".format(lat), length, getattr(req, 'unique_id', '-'), req.path_info)


# pylint: disable=too-many-instance-attributes
class ViewCache(object):
    VALUE_OK = 0
    VALUE_STALE = 1
    VALUE_NONE = 2

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
            t0 = 0.0
            t1 = 0.0
            try:
                t0 = time.time()
                self._view.logger.debug("starting execution of %s", self.fn)
                val = self.fn(*self.args, **self.kwargs)
                t1 = time.time()
            except Exception as ex:
                with self._view.lock:
                    self._view.logger.exception("Error while calling fn=%s ex=%s", self.fn,
                                                str(ex))
                    self._view.value = None
                    self._view.value_when = None
                    self._view.getter_thread = None
                    self._view.exception = ex
            else:
                with self._view.lock:
                    self._view.latency = t1 - t0
                    self._view.value = val
                    self._view.value_when = datetime.now()
                    self._view.getter_thread = None
                    self._view.exception = None

            self._view.logger.debug("execution of %s finished in: %s", self.fn,
                                    t1 - t0)
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
            self.logger = logging.getLogger('viewcache')

        def reset(self):
            with self.lock:
                self.value_when = None
                self.value = None

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
                now = datetime.now()
                if self.value_when and now - self.value_when < timedelta(
                        seconds=self.STALE_PERIOD):
                    return ViewCache.VALUE_OK, self.value

                if self.getter_thread is None:
                    self.getter_thread = ViewCache.GetterThread(self, fn, args,
                                                                kwargs)
                    self.getter_thread.start()
                else:
                    self.logger.debug("getter_thread still alive for: %s", fn)

                ev = self.getter_thread.event

            success = ev.wait(timeout=self.timeout)

            with self.lock:
                if success:
                    # We fetched the data within the timeout
                    if self.exception:
                        # execution raised an exception
                        # pylint: disable=raising-bad-type
                        raise self.exception
                    return ViewCache.VALUE_OK, self.value
                if self.value_when is not None:
                    # We have some data, but it doesn't meet freshness requirements
                    return ViewCache.VALUE_STALE, self.value
                # We have no data, not even stale data
                raise ViewCacheNoDataException()

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
        wrapper.reset = self.reset  # type: ignore
        return wrapper

    def reset(self):
        for _, rvc in self.cache_by_args.items():
            rvc.reset()


class NotificationQueue(threading.Thread):
    _ALL_TYPES_ = '__ALL__'
    _listeners = collections.defaultdict(set)  # type: DefaultDict[str, Set[Tuple[int, Callable]]]
    _lock = threading.Lock()
    _cond = threading.Condition()
    _queue = collections.deque()  # type: Deque[Tuple[str, Any]]
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
        cls.logger = logging.getLogger('notification_queue')  # type: ignore
        cls.logger.debug("starting notification queue")  # type: ignore
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
        cls.logger.debug("waiting for notification queue to finish")  # type: ignore
        instance.join()
        cls.logger.debug("notification queue stopped")  # type: ignore

    @classmethod
    def _registered_handler(cls, func, n_types):
        for _, reg_func in cls._listeners[n_types]:
            if reg_func == func:
                return True
        return False

    @classmethod
    def register(cls, func, n_types=None, priority=1):
        """Registers function to listen for notifications

        If the second parameter `n_types` is omitted, the function in `func`
        parameter will be called for any type of notifications.

        Args:
            func (function): python function ex: def foo(val)
            n_types (str|list): the single type to listen, or a list of types
            priority (int): the priority level (1=max, +inf=min)
        """
        with cls._lock:
            if not n_types:
                n_types = [cls._ALL_TYPES_]
            elif isinstance(n_types, str):
                n_types = [n_types]
            elif not isinstance(n_types, list):
                raise Exception("n_types param is neither a string nor a list")
            for ev_type in n_types:
                if not cls._registered_handler(func, ev_type):
                    cls._listeners[ev_type].add((priority, func))
                    cls.logger.debug(  # type: ignore
                        "function %s was registered for events of type %s",
                        func, ev_type
                    )

    @classmethod
    def deregister(cls, func, n_types=None):
        # type: (Callable, Union[str, list, None]) -> None
        """Removes the listener function from this notification queue

        If the second parameter `n_types` is omitted, the function is removed
        from all event types, otherwise the function is removed only for the
        specified event types.

        Args:
            func (function): python function
            n_types (str|list): the single event type, or a list of event types
        """
        with cls._lock:
            if not n_types:
                n_types = list(cls._listeners.keys())
            elif isinstance(n_types, str):
                n_types = [n_types]
            elif not isinstance(n_types, list):
                raise Exception("n_types param is neither a string nor a list")
            for ev_type in n_types:
                listeners = cls._listeners[ev_type]
                to_remove = None
                for pr, fn in listeners:
                    if fn == func:
                        to_remove = (pr, fn)
                        break
                if to_remove:
                    listeners.discard(to_remove)
                    cls.logger.debug(  # type: ignore
                        "function %s was deregistered for events of type %s",
                        func, ev_type
                    )

    @classmethod
    def new_notification(cls, notify_type, notify_value):
        # type: (str, Any) -> None
        with cls._cond:
            cls._queue.append((notify_type, notify_value))
            cls._cond.notify()

    @classmethod
    def _notify_listeners(cls, events):
        for ev in events:
            notify_type, notify_value = ev
            with cls._lock:
                listeners = list(cls._listeners[notify_type])
                listeners.extend(cls._listeners[cls._ALL_TYPES_])
            listeners.sort(key=lambda lis: lis[0])
            for listener in listeners:
                listener[1](notify_value)

    def run(self):
        self.logger.debug("notification queue started")  # type: ignore
        while self._running:
            private_buffer = []
            self.logger.debug("processing queue: %s", len(self._queue))  # type: ignore
            try:
                while True:
                    private_buffer.append(self._queue.popleft())
            except IndexError:
                pass
            self._notify_listeners(private_buffer)
            with self._cond:
                while self._running and not self._queue:
                    self._cond.wait()
        # flush remaining events
        self.logger.debug("flush remaining events: %s", len(self._queue))  # type: ignore
        self._notify_listeners(self._queue)
        self._queue.clear()
        self.logger.debug("notification queue finished")  # type: ignore


# pylint: disable=too-many-arguments, protected-access
class TaskManager(object):
    FINISHED_TASK_SIZE = 10
    FINISHED_TASK_TTL = 60.0

    VALUE_DONE = "done"
    VALUE_EXECUTING = "executing"

    _executing_tasks = set()  # type: Set[Task]
    _finished_tasks = []  # type: List[Task]
    _lock = threading.Lock()

    _task_local_data = threading.local()

    @classmethod
    def init(cls):
        cls.logger = logging.getLogger('taskmgr')  # type: ignore
        NotificationQueue.register(cls._handle_finished_task, 'cd_task_finished')

    @classmethod
    def _handle_finished_task(cls, task):
        cls.logger.info("finished %s", task)  # type: ignore
        with cls._lock:
            cls._executing_tasks.remove(task)
            cls._finished_tasks.append(task)

    @classmethod
    def run(cls, name, metadata, fn, args=None, kwargs=None, executor=None,
            exception_handler=None):
        if not args:
            args = []
        if not kwargs:
            kwargs = {}
        if not executor:
            executor = ThreadedExecutor()
        task = Task(name, metadata, fn, args, kwargs, executor,
                    exception_handler)
        with cls._lock:
            if task in cls._executing_tasks:
                cls.logger.debug("task already executing: %s", task)  # type: ignore
                for t in cls._executing_tasks:
                    if t == task:
                        return t
            cls.logger.debug("created %s", task)  # type: ignore
            cls._executing_tasks.add(task)
        cls.logger.info("running %s", task)  # type: ignore
        task._run()
        return task

    @classmethod
    def current_task(cls):
        """
        Returns the current task object.
        This method should only be called from a threaded task operation code.
        """
        return cls._task_local_data.task

    @classmethod
    def _cleanup_old_tasks(cls, task_list):
        """
        The cleanup rule is: maintain the FINISHED_TASK_SIZE more recent
        finished tasks, and the rest is maintained up to the FINISHED_TASK_TTL
        value.
        """
        now = datetime.now()
        for idx, t in enumerate(task_list):
            if idx < cls.FINISHED_TASK_SIZE:
                continue
            if now - datetime.fromtimestamp(t[1].end_time) > \
                    timedelta(seconds=cls.FINISHED_TASK_TTL):
                del cls._finished_tasks[t[0]]

    @classmethod
    def list(cls, name_glob=None):
        executing_tasks = []
        finished_tasks = []
        with cls._lock:
            for task in cls._executing_tasks:
                if not name_glob or fnmatch.fnmatch(task.name, name_glob):
                    executing_tasks.append(task)
            for idx, task in enumerate(cls._finished_tasks):
                if not name_glob or fnmatch.fnmatch(task.name, name_glob):
                    finished_tasks.append((idx, task))
            finished_tasks.sort(key=lambda t: t[1].end_time, reverse=True)
            cls._cleanup_old_tasks(finished_tasks)
        executing_tasks.sort(key=lambda t: t.begin_time, reverse=True)
        return executing_tasks, [t[1] for t in finished_tasks]

    @classmethod
    def list_serializable(cls, ns_glob=None):
        ex_t, fn_t = cls.list(ns_glob)
        return [{
            'name': t.name,
            'metadata': t.metadata,
            'begin_time': "{}Z".format(datetime.fromtimestamp(t.begin_time).isoformat()),
            'progress': t.progress
        } for t in ex_t if t.begin_time], [{
            'name': t.name,
            'metadata': t.metadata,
            'begin_time': "{}Z".format(datetime.fromtimestamp(t.begin_time).isoformat()),
            'end_time': "{}Z".format(datetime.fromtimestamp(t.end_time).isoformat()),
            'duration': t.duration,
            'progress': t.progress,
            'success': not t.exception,
            'ret_value': t.ret_value if not t.exception else None,
            'exception': t.ret_value if t.exception and t.ret_value else (
                {'detail': str(t.exception)} if t.exception else None)
        } for t in fn_t]


# pylint: disable=protected-access
class TaskExecutor(object):
    def __init__(self):
        self.logger = logging.getLogger('taskexec')
        self.task = None

    def init(self, task):
        self.task = task

    # pylint: disable=broad-except
    def start(self):
        self.logger.debug("executing task %s", self.task)
        try:
            self.task.fn(*self.task.fn_args, **self.task.fn_kwargs)  # type: ignore
        except Exception as ex:
            self.logger.exception("Error while calling %s", self.task)
            self.finish(None, ex)

    def finish(self, ret_value, exception):
        if not exception:
            self.logger.debug("successfully finished task: %s", self.task)
        else:
            self.logger.debug("task finished with exception: %s", self.task)
        self.task._complete(ret_value, exception)  # type: ignore


# pylint: disable=protected-access
class ThreadedExecutor(TaskExecutor):
    def __init__(self):
        super(ThreadedExecutor, self).__init__()
        self._thread = threading.Thread(target=self._run)

    def start(self):
        self._thread.start()

    # pylint: disable=broad-except
    def _run(self):
        TaskManager._task_local_data.task = self.task
        try:
            self.logger.debug("executing task %s", self.task)
            val = self.task.fn(*self.task.fn_args, **self.task.fn_kwargs)  # type: ignore
        except Exception as ex:
            self.logger.exception("Error while calling %s", self.task)
            self.finish(None, ex)
        else:
            self.finish(val, None)


class Task(object):
    def __init__(self, name, metadata, fn, args, kwargs, executor,
                 exception_handler=None):
        self.name = name
        self.metadata = metadata
        self.fn = fn
        self.fn_args = args
        self.fn_kwargs = kwargs
        self.executor = executor
        self.ex_handler = exception_handler
        self.running = False
        self.event = threading.Event()
        self.progress = None
        self.ret_value = None
        self.begin_time = None
        self.end_time = None
        self.duration = 0
        self.exception = None
        self.logger = logging.getLogger('task')
        self.lock = threading.Lock()

    def __hash__(self):
        return hash((self.name, tuple(sorted(self.metadata.items()))))

    def __eq__(self, other):
        return self.name == other.name and self.metadata == other.metadata

    def __str__(self):
        return "Task(ns={}, md={})" \
               .format(self.name, self.metadata)

    def __repr__(self):
        return str(self)

    def _run(self):
        NotificationQueue.register(self._handle_task_finished, 'cd_task_finished', 100)
        with self.lock:
            assert not self.running
            self.executor.init(self)
            self.set_progress(0, in_lock=True)
            self.begin_time = time.time()
            self.running = True
        self.executor.start()

    def _complete(self, ret_value, exception=None):
        now = time.time()
        if exception and self.ex_handler:
            # pylint: disable=broad-except
            try:
                ret_value = self.ex_handler(exception, task=self)
            except Exception as ex:
                exception = ex
        with self.lock:
            assert self.running, "_complete cannot be called before _run"
            self.end_time = now
            self.ret_value = ret_value
            self.exception = exception
            self.duration = now - self.begin_time  # type: ignore
            if not self.exception:
                self.set_progress(100, True)
        NotificationQueue.new_notification('cd_task_finished', self)
        self.logger.debug("execution of %s finished in: %s s", self,
                          self.duration)

    def _handle_task_finished(self, task):
        if self == task:
            NotificationQueue.deregister(self._handle_task_finished)
            self.event.set()

    def wait(self, timeout=None):
        with self.lock:
            assert self.running, "wait cannot be called before _run"
            ev = self.event

        success = ev.wait(timeout=timeout)
        with self.lock:
            if success:
                # the action executed within the timeout
                if self.exception:
                    # pylint: disable=raising-bad-type
                    # execution raised an exception
                    raise self.exception
                return TaskManager.VALUE_DONE, self.ret_value
            # the action is still executing
            return TaskManager.VALUE_EXECUTING, None

    def inc_progress(self, delta, in_lock=False):
        if not isinstance(delta, int) or delta < 0:
            raise Exception("Progress delta value must be a positive integer")
        if not in_lock:
            self.lock.acquire()
        prog = self.progress + delta  # type: ignore
        self.progress = prog if prog <= 100 else 100
        if not in_lock:
            self.lock.release()

    def set_progress(self, percentage, in_lock=False):
        if not isinstance(percentage, int) or percentage < 0 or percentage > 100:
            raise Exception("Progress value must be in percentage "
                            "(0 <= percentage <= 100)")
        if not in_lock:
            self.lock.acquire()
        self.progress = percentage
        if not in_lock:
            self.lock.release()


def build_url(host, scheme=None, port=None):
    """
    Build a valid URL. IPv6 addresses specified in host will be enclosed in brackets
    automatically.

    >>> build_url('example.com', 'https', 443)
    'https://example.com:443'

    >>> build_url(host='example.com', port=443)
    '//example.com:443'

    >>> build_url('fce:9af7:a667:7286:4917:b8d3:34df:8373', port=80, scheme='http')
    'http://[fce:9af7:a667:7286:4917:b8d3:34df:8373]:80'

    :param scheme: The scheme, e.g. http, https or ftp.
    :type scheme: str
    :param host: Consisting of either a registered name (including but not limited to
                 a hostname) or an IP address.
    :type host: str
    :type port: int
    :rtype: str
    """
    try:
        ipaddress.IPv6Address(host)
        netloc = '[{}]'.format(host)
    except ValueError:
        netloc = host
    if port:
        netloc += ':{}'.format(port)
    pr = urllib.parse.ParseResult(
        scheme=scheme if scheme else '',
        netloc=netloc,
        path='',
        params='',
        query='',
        fragment='')
    return pr.geturl()


def prepare_url_prefix(url_prefix):
    """
    return '' if no prefix, or '/prefix' without slash in the end.
    """
    url_prefix = urllib.parse.urljoin('/', url_prefix)
    return url_prefix.rstrip('/')


def dict_contains_path(dct, keys):
    """
    Tests whether the keys exist recursively in `dictionary`.

    :type dct: dict
    :type keys: list
    :rtype: bool
    """
    if keys:
        if not isinstance(dct, dict):
            return False
        key = keys.pop(0)
        if key in dct:
            dct = dct[key]
            return dict_contains_path(dct, keys)
        return False
    return True


def dict_get(obj, path, default=None):
    """
    Get the value at any depth of a nested object based on the path
    described by `path`. If path doesn't exist, `default` is returned.
    """
    current = obj
    for part in path.split('.'):
        if not isinstance(current, dict):
            return default
        if part not in current.keys():
            return default
        current = current.get(part, {})
    return current


def getargspec(func):
    try:
        while True:
            func = func.__wrapped__
    except AttributeError:
        pass
    # pylint: disable=deprecated-method
    return inspect.getfullargspec(func)


def str_to_bool(val):
    """
    Convert a string representation of truth to True or False.

    >>> str_to_bool('true') and str_to_bool('yes') and str_to_bool('1') and str_to_bool(True)
    True

    >>> str_to_bool('false') and str_to_bool('no') and str_to_bool('0') and str_to_bool(False)
    False

    >>> str_to_bool('xyz')
    Traceback (most recent call last):
        ...
    ValueError: invalid truth value 'xyz'

    :param val: The value to convert.
    :type val: str|bool
    :rtype: bool
    """
    if isinstance(val, bool):
        return val
    return bool(strtobool(val))


def json_str_to_object(value):  # type: (AnyStr) -> Any
    """
    It converts a JSON valid string representation to object.

    >>> result = json_str_to_object('{"a": 1}')
    >>> result == {'a': 1}
    True
    """
    if value == '':
        return value

    try:
        # json.loads accepts binary input from version >=3.6
        value = value.decode('utf-8')  # type: ignore
    except AttributeError:
        pass

    return json.loads(value)


def partial_dict(orig, keys):  # type: (Dict, List[str]) -> Dict
    """
    It returns Dict containing only the selected keys of original Dict.

    >>> partial_dict({'a': 1, 'b': 2}, ['b'])
    {'b': 2}
    """
    return {k: orig[k] for k in keys}


def get_request_body_params(request):
    """
    Helper function to get parameters from the request body.
    :param request The CherryPy request object.
    :type request: cherrypy.Request
    :return: A dictionary containing the parameters.
    :rtype: dict
    """
    params = {}  # type: dict
    if request.method not in request.methods_with_bodies:
        return params

    content_type = request.headers.get('Content-Type', '')
    if content_type in ['application/json', 'text/javascript']:
        if not hasattr(request, 'json'):
            raise cherrypy.HTTPError(400, 'Expected JSON body')
        if isinstance(request.json, str):
            params.update(json.loads(request.json))
        else:
            params.update(request.json)

    return params


def find_object_in_list(key, value, iterable):
    """
    Get the first occurrence of an object within a list with
    the specified key/value.

    >>> find_object_in_list('name', 'bar', [{'name': 'foo'}, {'name': 'bar'}])
    {'name': 'bar'}

    >>> find_object_in_list('name', 'xyz', [{'name': 'foo'}, {'name': 'bar'}]) is None
    True

    >>> find_object_in_list('foo', 'bar', [{'xyz': 4815162342}]) is None
    True

    >>> find_object_in_list('foo', 'bar', []) is None
    True

    :param key: The name of the key.
    :param value: The value to search for.
    :param iterable: The list to process.
    :return: Returns the found object or None.
    """
    for obj in iterable:
        if key in obj and obj[key] == value:
            return obj
    return None
