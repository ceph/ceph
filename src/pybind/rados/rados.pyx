# cython: embedsignature=True, binding=True
"""
This module is a thin wrapper around librados.

Error codes from librados are turned into exceptions that subclass
:class:`Error`. Almost all methods may raise :class:`Error(the base class of all rados exceptions), :class:`PermissionError`
(the base class of all rados exceptions), :class:`PermissionError`
and :class:`IOError`, in addition to those documented for the
method.
"""
# Copyright 2011 Josh Durgin
# Copyright 2011, Hannu Valtonen <hannu.valtonen@ormod.com>
# Copyright 2015 Hector Martin <marcan@marcan.st>
# Copyright 2016 Mehdi Abaakouk <sileht@redhat.com>

from cpython cimport PyObject, ref
from cpython.pycapsule cimport *
from libc cimport errno
from libc.stdint cimport *
from libc.stdlib cimport malloc, realloc, free
IF BUILD_DOC:
    include "mock_rados.pxi"
ELSE:
    from c_rados cimport *

import threading
import time

from datetime import datetime, timedelta
from functools import partial, wraps
from itertools import chain
from typing import Callable, Dict, List, Optional, Sequence, Tuple, Union

cdef extern from "Python.h":
    # These are in cpython/string.pxd, but use "object" types instead of
    # PyObject*, which invokes assumptions in cpython that we need to
    # legitimately break to implement zero-copy string buffers in Ioctx.read().
    # This is valid use of the Python API and documented as a special case.
    PyObject *PyBytes_FromStringAndSize(char *v, Py_ssize_t len) except NULL
    char* PyBytes_AsString(PyObject *string) except NULL
    int _PyBytes_Resize(PyObject **string, Py_ssize_t newsize) except -1
    void PyEval_InitThreads()

LIBRADOS_OP_FLAG_EXCL = _LIBRADOS_OP_FLAG_EXCL
LIBRADOS_OP_FLAG_FAILOK = _LIBRADOS_OP_FLAG_FAILOK
LIBRADOS_OP_FLAG_FADVISE_RANDOM = _LIBRADOS_OP_FLAG_FADVISE_RANDOM
LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL = _LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL
LIBRADOS_OP_FLAG_FADVISE_WILLNEED = _LIBRADOS_OP_FLAG_FADVISE_WILLNEED
LIBRADOS_OP_FLAG_FADVISE_DONTNEED = _LIBRADOS_OP_FLAG_FADVISE_DONTNEED
LIBRADOS_OP_FLAG_FADVISE_NOCACHE = _LIBRADOS_OP_FLAG_FADVISE_NOCACHE

LIBRADOS_CMPXATTR_OP_EQ = _LIBRADOS_CMPXATTR_OP_EQ
LIBRADOS_CMPXATTR_OP_NE = _LIBRADOS_CMPXATTR_OP_NE
LIBRADOS_CMPXATTR_OP_GT = _LIBRADOS_CMPXATTR_OP_GT
LIBRADOS_CMPXATTR_OP_GTE = _LIBRADOS_CMPXATTR_OP_GTE
LIBRADOS_CMPXATTR_OP_LT = _LIBRADOS_CMPXATTR_OP_LT
LIBRADOS_CMPXATTR_OP_LTE = _LIBRADOS_CMPXATTR_OP_LTE

LIBRADOS_SNAP_HEAD = _LIBRADOS_SNAP_HEAD

LIBRADOS_OPERATION_NOFLAG = _LIBRADOS_OPERATION_NOFLAG
LIBRADOS_OPERATION_BALANCE_READS = _LIBRADOS_OPERATION_BALANCE_READS
LIBRADOS_OPERATION_LOCALIZE_READS = _LIBRADOS_OPERATION_LOCALIZE_READS
LIBRADOS_OPERATION_ORDER_READS_WRITES = _LIBRADOS_OPERATION_ORDER_READS_WRITES
LIBRADOS_OPERATION_IGNORE_CACHE = _LIBRADOS_OPERATION_IGNORE_CACHE
LIBRADOS_OPERATION_SKIPRWLOCKS = _LIBRADOS_OPERATION_SKIPRWLOCKS
LIBRADOS_OPERATION_IGNORE_OVERLAY = _LIBRADOS_OPERATION_IGNORE_OVERLAY

LIBRADOS_ALL_NSPACES = _LIBRADOS_ALL_NSPACES.decode('utf-8')

LIBRADOS_CREATE_EXCLUSIVE = _LIBRADOS_CREATE_EXCLUSIVE
LIBRADOS_CREATE_IDEMPOTENT = _LIBRADOS_CREATE_IDEMPOTENT

MAX_ERRNO = _MAX_ERRNO

ANONYMOUS_AUID = 0xffffffffffffffff
ADMIN_AUID = 0


class Error(Exception):
    """ `Error` class, derived from `Exception` """
    def __init__(self, message, errno=None):
        super(Exception, self).__init__(message)
        self.errno = errno

    def __str__(self):
        msg = super(Exception, self).__str__()
        if self.errno is None:
            return msg
        return '[errno {0}] {1}'.format(self.errno, msg)

    def __reduce__(self):
        return (self.__class__, (self.message, self.errno))

class InvalidArgumentError(Error):
    def __init__(self, message, errno=None):
        super(InvalidArgumentError, self).__init__(
            "RADOS invalid argument (%s)" % message, errno)

class ExtendMismatch(Error):
    def __init__(self, message, errno, offset):
        super().__init__(
             "object content does not match (%s)" % message, errno)
        self.offset = offset

class OSError(Error):
    """ `OSError` class, derived from `Error` """
    pass

class InterruptedOrTimeoutError(OSError):
    """ `InterruptedOrTimeoutError` class, derived from `OSError` """
    def __init__(self, message, errno=None):
        super(InterruptedOrTimeoutError, self).__init__(
            "RADOS interrupted or timeout (%s)" % message, errno)


class PermissionError(OSError):
    """ `PermissionError` class, derived from `OSError` """
    def __init__(self, message, errno=None):
        super(PermissionError, self).__init__(
            "RADOS permission error (%s)" % message, errno)


class PermissionDeniedError(OSError):
    """ deal with EACCES related. """
    def __init__(self, message, errno=None):
        super(PermissionDeniedError, self).__init__(
                "RADOS permission denied (%s)" % message, errno)


class ObjectNotFound(OSError):
    """ `ObjectNotFound` class, derived from `OSError` """
    def __init__(self, message, errno=None):
        super(ObjectNotFound, self).__init__(
                "RADOS object not found (%s)" % message, errno)


class NoData(OSError):
    """ `NoData` class, derived from `OSError` """
    def __init__(self, message, errno=None):
        super(NoData, self).__init__(
                "RADOS no data (%s)" % message, errno)


class ObjectExists(OSError):
    """ `ObjectExists` class, derived from `OSError` """
    def __init__(self, message, errno=None):
        super(ObjectExists, self).__init__(
                "RADOS object exists (%s)" % message, errno)


class ObjectBusy(OSError):
    """ `ObjectBusy` class, derived from `IOError` """
    def __init__(self, message, errno=None):
        super(ObjectBusy, self).__init__(
                "RADOS object busy (%s)" % message, errno)


class IOError(OSError):
    """ `ObjectBusy` class, derived from `OSError` """
    def __init__(self, message, errno=None):
        super(IOError, self).__init__(
                "RADOS I/O error (%s)" % message, errno)


class NoSpace(OSError):
    """ `NoSpace` class, derived from `OSError` """
    def __init__(self, message, errno=None):
        super(NoSpace, self).__init__(
                "RADOS no space (%s)" % message, errno)

class NotConnected(OSError):
    """ `NotConnected` class, derived from `OSError` """
    def __init__(self, message, errno=None):
        super(NotConnected, self).__init__(
                "RADOS not connected (%s)" % message, errno)

class RadosStateError(Error):
    """ `RadosStateError` class, derived from `Error` """
    def __init__(self, message, errno=None):
        super(RadosStateError, self).__init__(
                "RADOS rados state (%s)" % message, errno)


class IoctxStateError(Error):
    """ `IoctxStateError` class, derived from `Error` """
    def __init__(self, message, errno=None):
        super(IoctxStateError, self).__init__(
                "RADOS Ioctx state error (%s)" % message, errno)


class ObjectStateError(Error):
    """ `ObjectStateError` class, derived from `Error` """
    def __init__(self, message, errno=None):
        super(ObjectStateError, self).__init__(
                "RADOS object state error (%s)" % message, errno)


class LogicError(Error):
    """ `` class, derived from `Error` """
    def __init__(self, message, errno=None):
        super(LogicError, self).__init__(
            "RADOS logic error (%s)" % message, errno)


class TimedOut(OSError):
    """ `TimedOut` class, derived from `OSError` """
    def __init__(self, message, errno=None):
        super(TimedOut, self).__init__(
                "RADOS timed out (%s)" % message, errno)


class InProgress(Error):
    """ `InProgress` class, derived from `Error` """
    def __init__(self, message, errno=None):
        super(InProgress, self).__init__(
                "RADOS in progress error (%s)" % message, errno)


class IsConnected(Error):
    """ `IsConnected` class, derived from `Error` """
    def __init__(self, message, errno=None):
        super(IsConnected, self).__init__(
                "RADOS is connected error (%s)" % message, errno)


class ConnectionShutdown(OSError):
    """ `ConnectionShutdown` class, derived from `OSError` """
    def __init__(self, message, errno=None):
        super(ConnectionShutdown, self).__init__(
                "RADOS connection was shutdown (%s)" % message, errno)


IF UNAME_SYSNAME == "FreeBSD":
    cdef errno_to_exception = {
        errno.EPERM     : PermissionError,
        errno.ENOENT    : ObjectNotFound,
        errno.EIO       : IOError,
        errno.ENOSPC    : NoSpace,
        errno.EEXIST    : ObjectExists,
        errno.EBUSY     : ObjectBusy,
        errno.ENOATTR   : NoData,
        errno.EINTR     : InterruptedOrTimeoutError,
        errno.ETIMEDOUT : TimedOut,
        errno.EACCES    : PermissionDeniedError,
        errno.EINPROGRESS : InProgress,
        errno.EISCONN   : IsConnected,
        errno.EINVAL    : InvalidArgumentError,
        errno.ENOTCONN  : NotConnected,
        errno.ESHUTDOWN : ConnectionShutdown,
    }
ELSE:
    cdef errno_to_exception = {
        errno.EPERM     : PermissionError,
        errno.ENOENT    : ObjectNotFound,
        errno.EIO       : IOError,
        errno.ENOSPC    : NoSpace,
        errno.EEXIST    : ObjectExists,
        errno.EBUSY     : ObjectBusy,
        errno.ENODATA   : NoData,
        errno.EINTR     : InterruptedOrTimeoutError,
        errno.ETIMEDOUT : TimedOut,
        errno.EACCES    : PermissionDeniedError,
        errno.EINPROGRESS : InProgress,
        errno.EISCONN   : IsConnected,
        errno.EINVAL    : InvalidArgumentError,
        errno.ENOTCONN  : NotConnected,
        errno.ESHUTDOWN : ConnectionShutdown,
    }


cdef make_ex(ret: int, msg: str):
    """
    Translate a librados return code into an exception.

    :param ret: the return code
    :type ret: int
    :param msg: the error message to use
    :type msg: str
    :returns: a subclass of :class:`Error`
    """
    ret = abs(ret)
    if ret in errno_to_exception:
        return errno_to_exception[ret](msg, errno=ret)
    elif ret > MAX_ERRNO:
         offset = ret - MAX_ERRNO
         return ExtendMismatch(msg, ret, offset)
    else:
        return OSError(msg, errno=ret)


def cstr(val, name, encoding="utf-8", opt=False) -> Optional[bytes]:
    """
    Create a byte string from a Python string

    :param basestring val: Python string
    :param str name: Name of the string parameter, for exceptions
    :param str encoding: Encoding to use
    :param bool opt: If True, None is allowed
    :raises: :class:`InvalidArgument`
    """
    if opt and val is None:
        return None
    if isinstance(val, bytes):
        return val
    elif isinstance(val, str):
        return val.encode(encoding)
    else:
        raise TypeError('%s must be a string' % name)


def cstr_list(list_str, name, encoding="utf-8"):
    return [cstr(s, name) for s in list_str]


def decode_cstr(val, encoding="utf-8") -> Optional[str]:
    """
    Decode a byte string into a Python string.

    :param bytes val: byte string
    """
    if val is None:
        return None

    return val.decode(encoding)


def flatten_dict(d, name):
    items = chain.from_iterable(d.items())
    return cstr(''.join(i + '\0' for i in items), name)


cdef char* opt_str(s) except? NULL:
    if s is None:
        return NULL
    return s


cdef void* realloc_chk(void* ptr, size_t size) except NULL:
    cdef void *ret = realloc(ptr, size)
    if ret == NULL:
        raise MemoryError("realloc failed")
    return ret


cdef size_t * to_csize_t_array(list_int):
    cdef size_t *ret = <size_t *>malloc(len(list_int) * sizeof(size_t))
    if ret == NULL:
        raise MemoryError("malloc failed")
    for i in range(len(list_int)):
        ret[i] = <size_t>list_int[i]
    return ret


cdef char ** to_bytes_array(list_bytes):
    cdef char **ret = <char **>malloc(len(list_bytes) * sizeof(char *))
    if ret == NULL:
        raise MemoryError("malloc failed")
    for i in range(len(list_bytes)):
        ret[i] = <char *>list_bytes[i]
    return ret

cdef int __monitor_callback(void *arg, const char *line, const char *who,
                             uint64_t sec, uint64_t nsec, uint64_t seq,
                             const char *level, const char *msg) with gil:
    cdef object cb_info = <object>arg
    cb_info[0](cb_info[1], line, who, sec, nsec, seq, level, msg)
    return 0

cdef int __monitor_callback2(void *arg, const char *line, const char *channel,
                             const char *who,
                             const char *name,
                             uint64_t sec, uint64_t nsec, uint64_t seq,
                             const char *level, const char *msg) with gil:
    cdef object cb_info = <object>arg
    cb_info[0](cb_info[1], line, channel, name, who, sec, nsec, seq, level, msg)
    return 0


class Version(object):
    """ Version information """
    def __init__(self, major, minor, extra):
        self.major = major
        self.minor = minor
        self.extra = extra

    def __str__(self):
        return "%d.%d.%d" % (self.major, self.minor, self.extra)


cdef class Rados(object):
    """This class wraps librados functions"""
    # NOTE(sileht): attributes declared in .pyd

    def __init__(self, *args, **kwargs):
        PyEval_InitThreads()
        self.__setup(*args, **kwargs)

    NO_CONF_FILE = -1
    "special value that indicates no conffile should be read when creating a mount handle"
    DEFAULT_CONF_FILES = -2
    "special value that indicates the default conffiles should be read when creating a mount handle"

    def __setup(self,
                rados_id: Optional[str] = None,
                name: Optional[str] = None,
                clustername: Optional[str] = None,
                conf_defaults: Optional[Dict[str, str]] = None,
                conffile: Union[str, int, None] = NO_CONF_FILE,
                conf: Optional[Dict[str, str]] = None,
                flags: int = 0,
                context: object = None):
        self.monitor_callback = None
        self.monitor_callback2 = None
        self.parsed_args = []
        self.conf_defaults = conf_defaults
        self.conffile = conffile
        self.rados_id = rados_id

        if rados_id and name:
            raise Error("Rados(): can't supply both rados_id and name")
        elif rados_id:
            name = 'client.' + rados_id
        elif name is None:
            name = 'client.admin'
        if clustername is None:
            clustername = ''

        name_raw = cstr(name, 'name')
        clustername_raw = cstr(clustername, 'clustername')
        cdef:
            char *_name = name_raw
            char *_clustername = clustername_raw
            int _flags = flags
            int ret

        if context:
            # Unpack void* (aka rados_config_t) from capsule
            rados_config = <rados_config_t> PyCapsule_GetPointer(context, NULL)
            with nogil:
                ret = rados_create_with_context(&self.cluster, rados_config)
        else:
            with nogil:
                ret = rados_create2(&self.cluster, _clustername, _name, _flags)
        if ret != 0:
            raise Error("rados_initialize failed with error code: %d" % ret)

        self.state = "configuring"
        # order is important: conf_defaults, then conffile, then conf
        if conf_defaults:
            for key, value in conf_defaults.items():
                self.conf_set(key, value)
        if conffile in (self.NO_CONF_FILE, None):
            pass
        elif conffile in (self.DEFAULT_CONF_FILES, ''):
            self.conf_read_file(None)
        else:
            self.conf_read_file(conffile)
        if conf:
            for key, value in conf.items():
                self.conf_set(key, value)

    def get_addrs(self):
        """
        Get associated client addresses with this RADOS session.
        """
        self.require_state("configuring", "connected")

        cdef:
            char* addrs = NULL

        try:

            with nogil:
                ret = rados_getaddrs(self.cluster, &addrs)
            if ret:
                raise make_ex(ret, "error calling getaddrs")

            return decode_cstr(addrs)
        finally:
            free(addrs)

    def require_state(self, *args):
        """
        Checks if the Rados object is in a special state

        :raises: :class:`RadosStateError`
        """
        if self.state in args:
            return
        raise RadosStateError("You cannot perform that operation on a \
Rados object in state %s." % self.state)

    def shutdown(self):
        """
        Disconnects from the cluster.  Call this explicitly when a
        Rados.connect()ed object is no longer used.
        """
        if self.state != "shutdown":
            with nogil:
                rados_shutdown(self.cluster)
            self.state = "shutdown"

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type_, value, traceback):
        self.shutdown()
        return False

    def version(self) -> Version:
        """
        Get the version number of the ``librados`` C library.

        :returns: a tuple of ``(major, minor, extra)`` components of the
                  librados version
        """
        cdef int major = 0
        cdef int minor = 0
        cdef int extra = 0
        with nogil:
            rados_version(&major, &minor, &extra)
        return Version(major, minor, extra)

    def conf_read_file(self, path: Optional[str] = None):
        """
        Configure the cluster handle using a Ceph config file.

        :param path: path to the config file
        """
        self.require_state("configuring", "connected")
        path_raw = cstr(path, 'path', opt=True)
        cdef:
            char *_path = opt_str(path_raw)
        with nogil:
            ret = rados_conf_read_file(self.cluster, _path)
        if ret != 0:
            raise make_ex(ret, "error calling conf_read_file")

    def conf_parse_argv(self, args: Sequence[str]):
        """
        Parse known arguments from args, and remove; returned
        args contain only those unknown to ceph
        """
        self.require_state("configuring", "connected")
        if not args:
            return

        cargs = cstr_list(args, 'args')
        cdef:
            int _argc = len(args)
            char **_argv = to_bytes_array(cargs)
            char **_remargv = NULL

        try:
            _remargv = <char **>malloc(_argc * sizeof(char *))
            with nogil:
                ret = rados_conf_parse_argv_remainder(self.cluster, _argc,
                                                      <const char**>_argv,
                                                      <const char**>_remargv)
            if ret:
                raise make_ex(ret, "error calling conf_parse_argv_remainder")

            # _remargv was allocated with fixed argc; collapse return
            # list to eliminate any missing args
            retargs = [decode_cstr(a) for a in _remargv[:_argc]
                       if a != NULL]
            self.parsed_args = args
            return retargs
        finally:
            free(_argv)
            free(_remargv)

    def conf_parse_env(self, var: Optional[str] = 'CEPH_ARGS'):
        """
        Parse known arguments from an environment variable, normally
        CEPH_ARGS.
        """
        self.require_state("configuring", "connected")
        if not var:
            return

        var_raw = cstr(var, 'var')
        cdef:
            char *_var = var_raw
        with nogil:
            ret = rados_conf_parse_env(self.cluster, _var)
        if ret != 0:
            raise make_ex(ret, "error calling conf_parse_env")

    def conf_get(self, option: str) -> Optional[str]:
        """
        Get the value of a configuration option

        :param option: which option to read

        :returns: value of the option or None
        :raises: :class:`TypeError`
        """
        self.require_state("configuring", "connected")
        option_raw = cstr(option, 'option')
        cdef:
            char *_option = option_raw
            size_t length = 20
            char *ret_buf = NULL

        try:
            while True:
                ret_buf = <char *>realloc_chk(ret_buf, length)
                with nogil:
                    ret = rados_conf_get(self.cluster, _option, ret_buf, length)
                if ret == 0:
                    return decode_cstr(ret_buf)
                elif ret == -errno.ENAMETOOLONG:
                    length = length * 2
                elif ret == -errno.ENOENT:
                    return None
                else:
                    raise make_ex(ret, "error calling conf_get")
        finally:
            free(ret_buf)

    def conf_set(self, option: str, val: str):
        """
        Set the value of a configuration option

        :param option: which option to set
        :param option: value of the option

        :raises: :class:`TypeError`, :class:`ObjectNotFound`
        """
        self.require_state("configuring", "connected")
        option_raw = cstr(option, 'option')
        val_raw = cstr(val, 'val')
        cdef:
            char *_option = option_raw
            char *_val = val_raw

        with nogil:
            ret = rados_conf_set(self.cluster, _option, _val)
        if ret != 0:
            raise make_ex(ret, "error calling conf_set")

    def ping_monitor(self, mon_id: str):
        """
        Ping a monitor to assess liveness

        May be used as a simply way to assess liveness, or to obtain
        information about the monitor in a simple way even in the
        absence of quorum.

        :param mon_id: the ID portion of the monitor's name (i.e., mon.<ID>)
        :returns: the string reply from the monitor
        """

        self.require_state("configuring", "connected")

        mon_id_raw = cstr(mon_id, 'mon_id')
        cdef:
            char *_mon_id = mon_id_raw
            size_t outstrlen = 0
            char *outstr

        with nogil:
            ret = rados_ping_monitor(self.cluster, _mon_id, &outstr, &outstrlen)

        if ret != 0:
            raise make_ex(ret, "error calling ping_monitor")

        if outstrlen:
            my_outstr = outstr[:outstrlen]
            rados_buffer_free(outstr)
            return decode_cstr(my_outstr)

    def connect(self, timeout: int = 0):
        """
        Connect to the cluster.  Use shutdown() to release resources.

        :param timeout: Any supplied timeout value is currently ignored.
        """
        self.require_state("configuring")
        # NOTE(sileht): timeout was supported by old python API,
        # but this is not something available in C API, so ignore
        # for now and remove it later
        with nogil:
            ret = rados_connect(self.cluster)
        if ret != 0:
            raise make_ex(ret, "error connecting to the cluster")
        self.state = "connected"

    def get_instance_id(self) -> int:
        """
        Get a global id for current instance
        """
        self.require_state("connected")
        with nogil:
            ret = rados_get_instance_id(self.cluster)
        return ret;

    def get_cluster_stats(self) -> Dict[str, int]:
        """
        Read usage info about the cluster

        This tells you total space, space used, space available, and number
        of objects. These are not updated immediately when data is written,
        they are eventually consistent.
        :returns: contains the following keys:

            - ``kb`` (int) - total space

            - ``kb_used`` (int) - space used

            - ``kb_avail`` (int) - free space available

            - ``num_objects`` (int) - number of objects

        """
        cdef:
            rados_cluster_stat_t stats

        with nogil:
            ret = rados_cluster_stat(self.cluster, &stats)

        if ret < 0:
            raise make_ex(
                ret, "Rados.get_cluster_stats(%s): get_stats failed" % self.rados_id)
        return {'kb': stats.kb,
                'kb_used': stats.kb_used,
                'kb_avail': stats.kb_avail,
                'num_objects': stats.num_objects}

    def pool_exists(self, pool_name: str) -> bool:
        """
        Checks if a given pool exists.

        :param pool_name: name of the pool to check

        :raises: :class:`TypeError`, :class:`Error`
        :returns: whether the pool exists, false otherwise.
        """
        self.require_state("connected")

        pool_name_raw = cstr(pool_name, 'pool_name')
        cdef:
            char *_pool_name = pool_name_raw

        with nogil:
            ret = rados_pool_lookup(self.cluster, _pool_name)
        if ret >= 0:
            return True
        elif ret == -errno.ENOENT:
            return False
        else:
            raise make_ex(ret, "error looking up pool '%s'" % pool_name)

    def pool_lookup(self, pool_name: str) -> int:
        """
        Returns a pool's ID based on its name.

        :param pool_name: name of the pool to look up

        :raises: :class:`TypeError`, :class:`Error`
        :returns: pool ID, or None if it doesn't exist
        """
        self.require_state("connected")
        pool_name_raw = cstr(pool_name, 'pool_name')
        cdef:
            char *_pool_name = pool_name_raw

        with nogil:
            ret = rados_pool_lookup(self.cluster, _pool_name)
        if ret >= 0:
            return int(ret)
        elif ret == -errno.ENOENT:
            return None
        else:
            raise make_ex(ret, "error looking up pool '%s'" % pool_name)

    def pool_reverse_lookup(self, pool_id: int) -> Optional[str]:
        """
        Returns a pool's name based on its ID.

        :param pool_id: ID of the pool to look up

        :raises: :class:`TypeError`, :class:`Error`
        :returns: pool name, or None if it doesn't exist
        """
        self.require_state("connected")
        cdef:
            int64_t _pool_id = pool_id
            size_t size = 512
            char *name = NULL

        try:
            while True:
                name = <char *>realloc_chk(name, size)
                with nogil:
                    ret = rados_pool_reverse_lookup(self.cluster, _pool_id, name, size)
                if ret >= 0:
                    break
                elif ret != -errno.ERANGE and size <= 4096:
                    size *= 2
                elif ret == -errno.ENOENT:
                    return None
                elif ret < 0:
                    raise make_ex(ret, "error reverse looking up pool '%s'" % pool_id)

            return decode_cstr(name)

        finally:
            free(name)

    def create_pool(self, pool_name: str,
                    crush_rule: Optional[int] = None,
                    auid: Optional[int] = None):
        """
        Create a pool:
        - with default settings: if crush_rule=None and auid=None
        - with a specific CRUSH rule: crush_rule given
        - with a specific auid: auid given
        - with a specific CRUSH rule and auid: crush_rule and auid given       
 
        :param pool_name: name of the pool to create
        :param crush_rule: rule to use for placement in the new pool
        :param auid: id of the owner of the new pool

        :raises: :class:`TypeError`, :class:`Error`
        """
        self.require_state("connected")

        pool_name_raw = cstr(pool_name, 'pool_name')
        cdef:
            char *_pool_name = pool_name_raw
            uint8_t _crush_rule
            uint64_t _auid

        if crush_rule is None and auid is None:
            with nogil:
                ret = rados_pool_create(self.cluster, _pool_name)
        elif crush_rule is not None and auid is None:
            _crush_rule = crush_rule
            with nogil:
                ret = rados_pool_create_with_crush_rule(self.cluster, _pool_name, _crush_rule)
        elif crush_rule is None and auid is not None:
            _auid = auid
            with nogil:
                ret = rados_pool_create_with_auid(self.cluster, _pool_name, _auid)
        else:
            _crush_rule = crush_rule
            _auid = auid
            with nogil:
                ret = rados_pool_create_with_all(self.cluster, _pool_name, _auid, _crush_rule)
        if ret < 0:
            raise make_ex(ret, "error creating pool '%s'" % pool_name)

    def get_pool_base_tier(self, pool_id: int) -> int:
        """
        Get base pool

        :returns: base pool, or pool_id if tiering is not configured for the pool
        """
        self.require_state("connected")
        cdef:
            int64_t base_tier = 0
            int64_t _pool_id = pool_id

        with nogil:
            ret = rados_pool_get_base_tier(self.cluster, _pool_id, &base_tier)
        if ret < 0:
            raise make_ex(ret, "get_pool_base_tier(%d)" % pool_id)
        return int(base_tier)

    def delete_pool(self, pool_name: str):
        """
        Delete a pool and all data inside it.

        The pool is removed from the cluster immediately,
        but the actual data is deleted in the background.

        :param pool_name: name of the pool to delete

        :raises: :class:`TypeError`, :class:`Error`
        """
        self.require_state("connected")

        pool_name_raw = cstr(pool_name, 'pool_name')
        cdef:
            char *_pool_name = pool_name_raw

        with nogil:
            ret = rados_pool_delete(self.cluster, _pool_name)
        if ret < 0:
            raise make_ex(ret, "error deleting pool '%s'" % pool_name)

    def get_inconsistent_pgs(self, pool_id: int) -> List[str]:
        """
        List inconsistent placement groups in the given pool

        :param pool_id: ID of the pool in which PGs are listed
        :returns: inconsistent placement groups
        """
        self.require_state("connected")
        cdef:
            int64_t pool = pool_id
            size_t size = 512
            char *pgs = NULL

        try:
            while True:
                pgs = <char *>realloc_chk(pgs, size);
                with nogil:
                    ret = rados_inconsistent_pg_list(self.cluster, pool,
                                                     pgs, size)
                if ret > <int>size:
                    size *= 2
                elif ret >= 0:
                    break
                else:
                    raise make_ex(ret, "error calling inconsistent_pg_list")
            return [pg for pg in decode_cstr(pgs[:ret]).split('\0') if pg]
        finally:
            free(pgs)

    def list_pools(self) -> List[str]:
        """
        Gets a list of pool names.

        :returns: list of pool names.
        """
        self.require_state("connected")
        cdef:
            size_t size = 512
            char *c_names = NULL

        try:
            while True:
                c_names = <char *>realloc_chk(c_names, size)
                with nogil:
                    ret = rados_pool_list(self.cluster, c_names, size)
                if ret > <int>size:
                    size *= 2
                elif ret >= 0:
                    break
            return [name for name in decode_cstr(c_names[:ret]).split('\0')
                    if name]
        finally:
            free(c_names)

    def get_fsid(self) -> str:
        """
        Get the fsid of the cluster as a hexadecimal string.

        :raises: :class:`Error`
        :returns: cluster fsid
        """
        self.require_state("connected")
        cdef:
            char *ret_buf = NULL
            size_t buf_len = 64

        try:
            while True:
                ret_buf = <char *>realloc_chk(ret_buf, buf_len)
                with nogil:
                    ret = rados_cluster_fsid(self.cluster, ret_buf, buf_len)
                if ret == -errno.ERANGE:
                    buf_len = buf_len * 2
                elif ret < 0:
                    raise make_ex(ret, "error getting cluster fsid")
                else:
                    break
            return decode_cstr(ret_buf)
        finally:
            free(ret_buf)

    def open_ioctx(self, ioctx_name: str) -> Ioctx:
        """
        Create an io context

        The io context allows you to perform operations within a particular
        pool.

        :param ioctx_name: name of the pool

        :raises: :class:`TypeError`, :class:`Error`
        :returns: Rados Ioctx object
        """
        self.require_state("connected")
        ioctx_name_raw = cstr(ioctx_name, 'ioctx_name')
        cdef:
            rados_ioctx_t ioctx
            char *_ioctx_name = ioctx_name_raw
        with nogil:
            ret = rados_ioctx_create(self.cluster, _ioctx_name, &ioctx)
        if ret < 0:
            raise make_ex(ret, "error opening pool '%s'" % ioctx_name)
        io = Ioctx(self, ioctx_name)
        io.io = ioctx
        return io

    def open_ioctx2(self, pool_id: int) -> Ioctx:
        """
        Create an io context

        The io context allows you to perform operations within a particular
        pool.

        :param pool_id: ID of the pool

        :raises: :class:`TypeError`, :class:`Error`
        :returns: Rados Ioctx object
        """
        self.require_state("connected")
        cdef:
            rados_ioctx_t ioctx
            int64_t _pool_id = pool_id
        with nogil:
            ret = rados_ioctx_create2(self.cluster, _pool_id, &ioctx)
        if ret < 0:
            raise make_ex(ret, "error opening pool id '%s'" % pool_id)
        io = Ioctx(self, str(pool_id))
        io.io = ioctx
        return io

    def mon_command(self,
                    cmd: str,
                    inbuf: bytes,
                    timeout: int = 0,
                    target: Optional[Union[str, int]] = None) -> Tuple[int, bytes, str]:
        """
        Send a command to the mon.

        mon_command[_target](cmd, inbuf, outbuf, outbuflen, outs, outslen)

        :param cmd: JSON formatted string.
        :param inbuf: optional string.
        :param timeout: This parameter is ignored.
        :param target: name or rank of a specific mon. Optional
        :return: (int ret, string outbuf, string outs)

        Example:

        >>> import json
        >>> c = Rados(conffile='/etc/ceph/ceph.conf')
        >>> c.connect()
        >>> cmd = json.dumps({"prefix": "osd safe-to-destroy", "ids": ["2"], "format": "json"})
        >>> c.mon_command(cmd, b'')
        """
        # NOTE(sileht): timeout is ignored because C API doesn't provide
        # timeout argument, but we keep it for backward compat with old python binding
        self.require_state("connected")
        cmds = [cstr(cmd, 'cmd')]

        if isinstance(target, int):
        # NOTE(sileht): looks weird but test_monmap_dump pass int
            target = str(target)

        target_raw = cstr(target, 'target', opt=True)

        cdef:
            char *_target = opt_str(target_raw)
            char **_cmd = to_bytes_array(cmds)
            size_t _cmdlen = len(cmds)

            char *_inbuf = inbuf
            size_t _inbuf_len = len(inbuf)

            char *_outbuf
            size_t _outbuf_len
            char *_outs
            size_t _outs_len

        try:
            if target_raw:
                with nogil:
                    ret = rados_mon_command_target(self.cluster, _target,
                                                <const char **>_cmd, _cmdlen,
                                                <const char*>_inbuf, _inbuf_len,
                                                &_outbuf, &_outbuf_len,
                                                &_outs, &_outs_len)
            else:
                with nogil:
                    ret = rados_mon_command(self.cluster,
                                            <const char **>_cmd, _cmdlen,
                                            <const char*>_inbuf, _inbuf_len,
                                            &_outbuf, &_outbuf_len,
                                            &_outs, &_outs_len)

            my_outs = decode_cstr(_outs[:_outs_len])
            my_outbuf = _outbuf[:_outbuf_len]
            if _outs_len:
                rados_buffer_free(_outs)
            if _outbuf_len:
                rados_buffer_free(_outbuf)
            return (ret, my_outbuf, my_outs)
        finally:
            free(_cmd)

    def osd_command(self,
                    osdid: int,
                    cmd: str,
                    inbuf: bytes,
                    timeout: int = 0) -> Tuple[int, bytes, str]:
        """
        osd_command(osdid, cmd, inbuf, outbuf, outbuflen, outs, outslen)

        :return: (int ret, string outbuf, string outs)
        """
        # NOTE(sileht): timeout is ignored because C API doesn't provide
        # timeout argument, but we keep it for backward compat with old python binding
        self.require_state("connected")

        cmds = [cstr(cmd, 'cmd')]

        cdef:
            int _osdid = osdid
            char **_cmd = to_bytes_array(cmds)
            size_t _cmdlen = len(cmds)

            char *_inbuf = inbuf
            size_t _inbuf_len = len(inbuf)

            char *_outbuf
            size_t _outbuf_len
            char *_outs
            size_t _outs_len

        try:
            with nogil:
                ret = rados_osd_command(self.cluster, _osdid,
                                        <const char **>_cmd, _cmdlen,
                                        <const char*>_inbuf, _inbuf_len,
                                        &_outbuf, &_outbuf_len,
                                        &_outs, &_outs_len)

            my_outs = decode_cstr(_outs[:_outs_len])
            my_outbuf = _outbuf[:_outbuf_len]
            if _outs_len:
                rados_buffer_free(_outs)
            if _outbuf_len:
                rados_buffer_free(_outbuf)
            return (ret, my_outbuf, my_outs)
        finally:
            free(_cmd)

    def mgr_command(self,
                    cmd: str,
                    inbuf: bytes,
                    timeout: int = 0,
                    target: Optional[str] = None) -> Tuple[int, str, bytes]:
        """
        :return: (int ret, string outbuf, string outs)
        """
        # NOTE(sileht): timeout is ignored because C API doesn't provide
        # timeout argument, but we keep it for backward compat with old python binding
        self.require_state("connected")

        cmds = [cstr(cmd, 'cmd')]
        target_raw = cstr(target, 'target', opt=True)

        cdef:
            char *_target = opt_str(target_raw)

            char **_cmd = to_bytes_array(cmds)
            size_t _cmdlen = len(cmds)

            char *_inbuf = inbuf
            size_t _inbuf_len = len(inbuf)

            char *_outbuf
            size_t _outbuf_len
            char *_outs
            size_t _outs_len

        try:
            if target_raw is not None:
                with nogil:
                    ret = rados_mgr_command_target(self.cluster,
		                            <const char*>_target,
                                            <const char **>_cmd, _cmdlen,
                                            <const char*>_inbuf, _inbuf_len,
                                            &_outbuf, &_outbuf_len,
                                            &_outs, &_outs_len)
            else:
                with nogil:
                    ret = rados_mgr_command(self.cluster,
                                            <const char **>_cmd, _cmdlen,
                                            <const char*>_inbuf, _inbuf_len,
                                            &_outbuf, &_outbuf_len,
                                            &_outs, &_outs_len)

            my_outs = decode_cstr(_outs[:_outs_len])
            my_outbuf = _outbuf[:_outbuf_len]
            if _outs_len:
                rados_buffer_free(_outs)
            if _outbuf_len:
                rados_buffer_free(_outbuf)
            return (ret, my_outbuf, my_outs)
        finally:
            free(_cmd)

    def pg_command(self,
                   pgid: str,
                   cmd: str,
                   inbuf: bytes,
                   timeout: int = 0) -> Tuple[int, bytes, str]:
        """
        pg_command(pgid, cmd, inbuf, outbuf, outbuflen, outs, outslen)

        :return: (int ret, string outbuf, string outs)
        """
        # NOTE(sileht): timeout is ignored because C API doesn't provide
        # timeout argument, but we keep it for backward compat with old python binding
        self.require_state("connected")

        pgid_raw = cstr(pgid, 'pgid')
        cmds = [cstr(cmd, 'cmd')]

        cdef:
            char *_pgid = pgid_raw
            char **_cmd = to_bytes_array(cmds)
            size_t _cmdlen = len(cmds)

            char *_inbuf = inbuf
            size_t _inbuf_len = len(inbuf)

            char *_outbuf
            size_t _outbuf_len
            char *_outs
            size_t _outs_len

        try:
            with nogil:
                ret = rados_pg_command(self.cluster, _pgid,
                                       <const char **>_cmd, _cmdlen,
                                       <const char *>_inbuf, _inbuf_len,
                                       &_outbuf, &_outbuf_len,
                                       &_outs, &_outs_len)

            my_outs = decode_cstr(_outs[:_outs_len])
            my_outbuf = _outbuf[:_outbuf_len]
            if _outs_len:
                rados_buffer_free(_outs)
            if _outbuf_len:
                rados_buffer_free(_outbuf)
            return (ret, my_outbuf, my_outs)
        finally:
            free(_cmd)

    def wait_for_latest_osdmap(self) -> int:
        self.require_state("connected")
        with nogil:
            ret = rados_wait_for_latest_osdmap(self.cluster)
        return ret

    def blocklist_add(self, client_address: str, expire_seconds: int = 0):
        """
        Blocklist a client from the OSDs

        :param client_address: client address
        :param expire_seconds: number of seconds to blocklist

        :raises: :class:`Error`
        """
        self.require_state("connected")
        client_address_raw = cstr(client_address, 'client_address')
        cdef:
            uint32_t _expire_seconds = expire_seconds
            char *_client_address = client_address_raw

        with nogil:
            ret = rados_blocklist_add(self.cluster, _client_address, _expire_seconds)
        if ret < 0:
            raise make_ex(ret, "error blocklisting client '%s'" % client_address)

    def monitor_log(self, level: str,
                    callback: Optional[Callable[[object, str, str, str, int, int, int, str, str], None]] = None,
                    arg: Optional[object] = None):
        if level not in MONITOR_LEVELS:
            raise LogicError("invalid monitor level " + level)
        if callback is not None and not callable(callback):
            raise LogicError("callback must be a callable function or None")

        level_raw = cstr(level, 'level')
        cdef char *_level = level_raw

        if callback is None:
            with nogil:
                r = rados_monitor_log(self.cluster, <const char*>_level, NULL, NULL)
            self.monitor_callback = None
            self.monitor_callback2 = None
            return

        cb = (callback, arg)
        cdef PyObject* _arg = <PyObject*>cb
        with nogil:
            r = rados_monitor_log(self.cluster, <const char*>_level,
                                  <rados_log_callback_t>&__monitor_callback, _arg)

        if r:
            raise make_ex(r, 'error calling rados_monitor_log')
        # NOTE(sileht): Prevents the callback method from being garbage collected
        self.monitor_callback = cb
        self.monitor_callback2 = None

    def monitor_log2(self, level: str,
                     callback: Optional[Callable[[object, str, str, str, str, int, int, int, str, str], None]] = None,
                     arg: Optional[object] = None):
        if level not in MONITOR_LEVELS:
            raise LogicError("invalid monitor level " + level)
        if callback is not None and not callable(callback):
            raise LogicError("callback must be a callable function or None")

        level_raw = cstr(level, 'level')
        cdef char *_level = level_raw

        if callback is None:
            with nogil:
                r = rados_monitor_log2(self.cluster, <const char*>_level, NULL, NULL)
            self.monitor_callback = None
            self.monitor_callback2 = None
            return

        cb = (callback, arg)
        cdef PyObject* _arg = <PyObject*>cb
        with nogil:
            r = rados_monitor_log2(self.cluster, <const char*>_level,
                                  <rados_log_callback2_t>&__monitor_callback2, _arg)

        if r:
            raise make_ex(r, 'error calling rados_monitor_log')
        # NOTE(sileht): Prevents the callback method from being garbage collected
        self.monitor_callback = None
        self.monitor_callback2 = cb

    def service_daemon_register(self, service: str, daemon: str, metadata: Dict[str, str]):
        """
        :param str service: service name (e.g. "rgw")
        :param str daemon: daemon name (e.g. "gwfoo")
        :param dict metadata: static metadata about the register daemon
               (e.g., the version of Ceph, the kernel version.)
        """
        service_raw = cstr(service, 'service')
        daemon_raw = cstr(daemon, 'daemon')
        metadata_dict = flatten_dict(metadata, 'metadata')
        cdef:
            char *_service = service_raw
            char *_daemon = daemon_raw
            char *_metadata = metadata_dict

        with nogil:
            ret = rados_service_register(self.cluster, _service, _daemon, _metadata)
        if ret != 0:
            raise make_ex(ret, "error calling service_register()")

    def service_daemon_update(self, status: Dict[str, str]):
        status_dict = flatten_dict(status, 'status')
        cdef:
            char *_status = status_dict

        with nogil:
            ret = rados_service_update_status(self.cluster, _status)
        if ret != 0:
            raise make_ex(ret, "error calling service_daemon_update()")


cdef class OmapIterator(object):
    """Omap iterator"""

    cdef public Ioctx ioctx
    cdef rados_omap_iter_t ctx

    def __cinit__(self, Ioctx ioctx):
        self.ioctx = ioctx

    def __iter__(self):
        return self

    def __next__(self):
        """
        Get the next key-value pair in the object
        :returns: next rados.OmapItem
        """
        cdef:
            char *key_ = NULL
            char *val_ = NULL
            size_t len_

        with nogil:
            ret = rados_omap_get_next(self.ctx, &key_, &val_, &len_)

        if ret != 0:
            raise make_ex(ret, "error iterating over the omap")
        if key_ == NULL:
            raise StopIteration()
        key = decode_cstr(key_)
        val = None
        if val_ != NULL:
            val = val_[:len_]
        return (key, val)

    def __dealloc__(self):
        with nogil:
            rados_omap_get_end(self.ctx)


cdef class ObjectIterator(object):
    """rados.Ioctx Object iterator"""

    cdef rados_list_ctx_t ctx

    cdef public object ioctx

    def __cinit__(self, Ioctx ioctx):
        self.ioctx = ioctx

        with nogil:
            ret = rados_nobjects_list_open(ioctx.io, &self.ctx)
        if ret < 0:
            raise make_ex(ret, "error iterating over the objects in ioctx '%s'"
                          % self.ioctx.name)

    def __iter__(self):
        return self

    def __next__(self):
        """
        Get the next object name and locator in the pool

        :raises: StopIteration
        :returns: next rados.Ioctx Object
        """
        cdef:
            const char *key_ = NULL
            const char *locator_ = NULL
            const char *nspace_ = NULL
            size_t key_size_ = 0
            size_t locator_size_ = 0
            size_t nspace_size_ = 0

        with nogil:
            ret = rados_nobjects_list_next2(self.ctx, &key_, &locator_, &nspace_,
                                            &key_size_, &locator_size_, &nspace_size_)

        if ret < 0:
            raise StopIteration()

        key = decode_cstr(key_[:key_size_])
        locator = decode_cstr(locator_[:locator_size_]) if locator_ != NULL else None
        nspace = decode_cstr(nspace_[:nspace_size_]) if nspace_ != NULL else None
        return Object(self.ioctx, key, locator, nspace)

    def __dealloc__(self):
        with nogil:
            rados_nobjects_list_close(self.ctx)


cdef class XattrIterator(object):
    """Extended attribute iterator"""

    cdef rados_xattrs_iter_t it
    cdef char* _oid

    cdef public Ioctx ioctx
    cdef public object oid

    def __cinit__(self, Ioctx ioctx, oid):
        self.ioctx = ioctx
        self.oid = cstr(oid, 'oid')
        self._oid = self.oid

        with nogil:
            ret = rados_getxattrs(ioctx.io,  self._oid, &self.it)
        if ret != 0:
            raise make_ex(ret, "Failed to get rados xattrs for object %r" % oid)

    def __iter__(self):
        return self

    def __next__(self):
        """
        Get the next xattr on the object

        :raises: StopIteration
        :returns: pair - of name and value of the next Xattr
        """
        cdef:
            const char *name_ = NULL
            const char *val_ = NULL
            size_t len_ = 0

        with nogil:
            ret = rados_getxattrs_next(self.it, &name_, &val_, &len_)
        if ret != 0:
            raise make_ex(ret, "error iterating over the extended attributes \
in '%s'" % self.oid)
        if name_ == NULL:
            raise StopIteration()
        name = decode_cstr(name_)
        val = val_[:len_]
        return (name, val)

    def __dealloc__(self):
        with nogil:
            rados_getxattrs_end(self.it)


cdef class SnapIterator(object):
    """Snapshot iterator"""

    cdef public Ioctx ioctx

    cdef rados_snap_t *snaps
    cdef int max_snap
    cdef int cur_snap

    def __cinit__(self, Ioctx ioctx):
        self.ioctx = ioctx
        # We don't know how big a buffer we need until we've called the
        # function. So use the exponential doubling strategy.
        cdef int num_snaps = 10
        while True:
            self.snaps = <rados_snap_t*>realloc_chk(self.snaps,
                                                    num_snaps *
                                                    sizeof(rados_snap_t))

            with nogil:
                ret = rados_ioctx_snap_list(ioctx.io, self.snaps, num_snaps)
            if ret >= 0:
                self.max_snap = ret
                break
            elif ret != -errno.ERANGE:
                raise make_ex(ret, "error calling rados_snap_list for \
ioctx '%s'" % self.ioctx.name)
            num_snaps = num_snaps * 2
        self.cur_snap = 0

    def __iter__(self) -> 'SnapIterator':
        return self

    def __next__(self) -> 'Snap':
        """
        Get the next Snapshot

        :raises: :class:`Error`, StopIteration
        :returns: next snapshot
        """
        if self.cur_snap >= self.max_snap:
            raise StopIteration

        cdef:
            rados_snap_t snap_id = self.snaps[self.cur_snap]
            int name_len = 10
            char *name = NULL

        try:
            while True:
                name = <char *>realloc_chk(name, name_len)
                with nogil:
                    ret = rados_ioctx_snap_get_name(self.ioctx.io, snap_id, name, name_len)
                if ret == 0:
                    break
                elif ret != -errno.ERANGE:
                    raise make_ex(ret, "rados_snap_get_name error")
                else:
                    name_len = name_len * 2

            snap = Snap(self.ioctx, decode_cstr(name[:name_len]).rstrip('\0'), snap_id)
            self.cur_snap = self.cur_snap + 1
            return snap
        finally:
            free(name)


cdef class Snap(object):
    """Snapshot object"""
    cdef public Ioctx ioctx
    cdef public object name

    # NOTE(sileht): old API was storing the ctypes object
    # instead of the value ....
    cdef public rados_snap_t snap_id

    def __cinit__(self, Ioctx ioctx, object name, rados_snap_t snap_id):
        self.ioctx = ioctx
        self.name = name
        self.snap_id = snap_id

    def __str__(self):
        return "rados.Snap(ioctx=%s,name=%s,snap_id=%d)" \
            % (str(self.ioctx), self.name, self.snap_id)

    def get_timestamp(self) -> float:
        """
        Find when a snapshot in the current pool occurred

        :raises: :class:`Error`
        :returns: the data and time the snapshot was created
        """
        cdef time_t snap_time

        with nogil:
            ret = rados_ioctx_snap_get_stamp(self.ioctx.io, self.snap_id, &snap_time)
        if ret != 0:
            raise make_ex(ret, "rados_ioctx_snap_get_stamp error")
        return datetime.fromtimestamp(snap_time)

# https://github.com/cython/cython/issues/1370
unicode = str

cdef class Completion(object):
    """completion object"""

    cdef public:
         Ioctx ioctx
         object oncomplete
         object onsafe

    cdef:
         rados_callback_t complete_cb
         rados_callback_t safe_cb
         rados_completion_t rados_comp
         PyObject* buf

    def __cinit__(self, Ioctx ioctx, object oncomplete, object onsafe):
        self.oncomplete = oncomplete
        self.onsafe = onsafe
        self.ioctx = ioctx

    def is_safe(self) -> bool:
        """
        Is an asynchronous operation safe?

        This does not imply that the safe callback has finished.

        :returns: True if the operation is safe
        """
        return self.is_complete()

    def is_complete(self) -> bool:
        """
        Has an asynchronous operation completed?

        This does not imply that the safe callback has finished.

        :returns: True if the operation is completed
        """
        with nogil:
            ret = rados_aio_is_complete(self.rados_comp)
        return ret == 1

    def wait_for_safe(self):
        """
        Wait for an asynchronous operation to be marked safe

        wait_for_safe() is an alias of wait_for_complete()  since Luminous
        """
        self.wait_for_complete()

    def wait_for_complete(self):
        """
        Wait for an asynchronous operation to complete

        This does not imply that the complete callback has finished.
        """
        with nogil:
            rados_aio_wait_for_complete(self.rados_comp)

    def wait_for_safe_and_cb(self):
        """
        Wait for an asynchronous operation to be marked safe and for
        the safe callback to have returned
        """
        return self.wait_for_complete_and_cb()

    def wait_for_complete_and_cb(self):
        """
        Wait for an asynchronous operation to complete and for the
        complete callback to have returned

        :returns:  whether the operation is completed
        """
        with nogil:
            ret = rados_aio_wait_for_complete_and_cb(self.rados_comp)
        return ret

    def get_return_value(self) -> int:
        """
        Get the return value of an asychronous operation

        The return value is set when the operation is complete or safe,
        whichever comes first.

        :returns: return value of the operation
        """
        with nogil:
            ret = rados_aio_get_return_value(self.rados_comp)
        return ret

    def __dealloc__(self):
        """
        Release a completion

        Call this when you no longer need the completion. It may not be
        freed immediately if the operation is not acked and committed.
        """
        ref.Py_XDECREF(self.buf)
        self.buf = NULL
        if self.rados_comp != NULL:
            with nogil:
                rados_aio_release(self.rados_comp)
                self.rados_comp = NULL

    def _complete(self):
        self.oncomplete(self)
        if self.onsafe:
            self.onsafe(self)
        self._cleanup()

    def _cleanup(self):
        with self.ioctx.lock:
            if self.oncomplete:
                self.ioctx.complete_completions.remove(self)
            if self.onsafe:
                self.ioctx.safe_completions.remove(self)


class OpCtx(object):
    def __enter__(self):
        return self.create()

    def __exit__(self, type, msg, traceback):
        self.release()


cdef class WriteOp(object):
    cdef rados_write_op_t write_op

    def create(self):
        with nogil:
            self.write_op = rados_create_write_op()
        return self

    def release(self):
        with nogil:
            rados_release_write_op(self.write_op)

    def new(self, exclusive: Optional[int] = None):
        """
        Create the object.
        """

        cdef:
            int _exclusive = exclusive

        with nogil:
            rados_write_op_create(self.write_op, _exclusive, NULL)


    def remove(self):
        """
        Remove object.
        """
        with nogil:
            rados_write_op_remove(self.write_op)

    def set_flags(self, flags: int = LIBRADOS_OPERATION_NOFLAG):
        """
        Set flags for the last operation added to this write_op.
        :para flags: flags to apply to the last operation
        """

        cdef:
            int _flags = flags

        with nogil:
            rados_write_op_set_flags(self.write_op, _flags)

    def set_xattr(self, xattr_name: str, xattr_value: bytes):
        """
        Set an extended attribute on an object.
        :param xattr_name: name of the xattr
        :param xattr_value: buffer to set xattr to
        """
        xattr_name_raw = cstr(xattr_name, 'xattr_name')
        cdef:
            char *_xattr_name = xattr_name_raw
            char *_xattr_value = xattr_value
            size_t _xattr_value_len = len(xattr_value)
        with nogil:
            rados_write_op_setxattr(self.write_op, _xattr_name, _xattr_value, _xattr_value_len)

    def rm_xattr(self, xattr_name: str):
        """  
        Removes an extended attribute on from an object.
        :param xattr_name: name of the xattr to remove
        """
        xattr_name_raw = cstr(xattr_name, 'xattr_name')
        cdef:
            char *_xattr_name = xattr_name_raw
        with nogil:
            rados_write_op_rmxattr(self.write_op, _xattr_name)

    def append(self, to_write: bytes):
        """
        Append data to an object synchronously
        :param to_write: data to write
        """

        cdef:
            char *_to_write = to_write
            size_t length = len(to_write)

        with nogil:
            rados_write_op_append(self.write_op, _to_write, length)

    def write_full(self, to_write: bytes):
        """
        Write whole object, atomically replacing it.
        :param to_write: data to write
        """

        cdef:
            char *_to_write = to_write
            size_t length = len(to_write)

        with nogil:
            rados_write_op_write_full(self.write_op, _to_write, length)

    def write(self, to_write: bytes, offset: int = 0):
        """
        Write to offset.
        :param to_write: data to write
        :param offset: byte offset in the object to begin writing at
        """

        cdef:
            char *_to_write = to_write
            size_t length = len(to_write)
            uint64_t _offset = offset

        with nogil:
            rados_write_op_write(self.write_op, _to_write, length, _offset)

    def assert_version(self, version: int):
        """
        Check if object's version is the expected one.
        :param version: expected version of the object
        :param type: int
        """
        cdef:
            uint64_t _version = version

        with nogil:
            rados_write_op_assert_version(self.write_op, _version)

    def zero(self, offset: int, length: int):
        """
        Zero part of an object.
        :param offset: byte offset in the object to begin writing at
        :param offset: number of zero to write
        """

        cdef:
            size_t _length = length
            uint64_t _offset = offset

        with nogil:
            rados_write_op_zero(self.write_op, _length, _offset)

    def truncate(self, offset: int):
        """
        Truncate an object.
        :param offset: byte offset in the object to begin truncating at
        """

        cdef:
            uint64_t _offset = offset

        with nogil:
            rados_write_op_truncate(self.write_op,  _offset)

    def execute(self, cls: str, method: str, data: bytes):
        """
        Execute an OSD class method on an object
        
        :param cls: name of the object class
        :param method: name of the method
        :param data: input data
        """

        cls_raw = cstr(cls, 'cls')
        method_raw = cstr(method, 'method')
        cdef:
            char *_cls = cls_raw
            char *_method = method_raw
            char *_data = data
            size_t _data_len = len(data)

        with nogil:
            rados_write_op_exec(self.write_op, _cls, _method, _data, _data_len, NULL)

    def writesame(self, to_write: bytes, write_len: int, offset: int = 0):
        """
        Write the same buffer multiple times
        :param to_write: data to write
        :param write_len: total number of bytes to write
        :param offset: byte offset in the object to begin writing at
        """
        cdef:
            char *_to_write = to_write
            size_t _data_len = len(to_write)
            size_t _write_len = write_len
            uint64_t _offset = offset
        with nogil:
             rados_write_op_writesame(self.write_op, _to_write, _data_len, _write_len, _offset)

    def cmpext(self, cmp_buf: bytes, offset: int = 0):
        """
        Ensure that given object range (extent) satisfies comparison
        :param cmp_buf: buffer containing bytes to be compared with object contents
        :param offset: object byte offset at which to start the comparison
        """
        cdef:
            char *_cmp_buf = cmp_buf
            size_t _cmp_buf_len = len(cmp_buf)
            uint64_t _offset = offset
        with nogil:
            rados_write_op_cmpext(self.write_op, _cmp_buf, _cmp_buf_len, _offset, NULL)

    def omap_cmp(self, key: str, val: str, cmp_op: int = LIBRADOS_CMPXATTR_OP_EQ):
        """
        Ensure that an omap key value satisfies comparison
        :param key: omap key whose associated value is evaluated for comparison
        :param val: value to compare with
        :param cmp_op: comparison operator, one of LIBRADOS_CMPXATTR_OP_EQ (1),
            LIBRADOS_CMPXATTR_OP_GT (3), or LIBRADOS_CMPXATTR_OP_LT (5).
        """
        key_raw = cstr(key, 'key')
        val_raw = cstr(val, 'val')
        cdef:
            char *_key = key_raw
            char *_val = val_raw
            size_t _val_len = len(val)
            uint8_t _comparison_operator = cmp_op
        with nogil:
            rados_write_op_omap_cmp(self.write_op, _key, _comparison_operator, _val, _val_len, NULL)

class WriteOpCtx(WriteOp, OpCtx):
    """write operation context manager"""


cdef class ReadOp(object):
    cdef rados_read_op_t read_op

    def create(self):
        with nogil:
            self.read_op = rados_create_read_op()
        return self

    def release(self):
        with nogil:
            rados_release_read_op(self.read_op)

    def cmpext(self, cmp_buf: bytes, offset: int = 0):
        """
        Ensure that given object range (extent) satisfies comparison
        :param cmp_buf: buffer containing bytes to be compared with object contents
        :param offset: object byte offset at which to start the comparison
        """
        cdef:
            char *_cmp_buf = cmp_buf
            size_t _cmp_buf_len = len(cmp_buf)
            uint64_t _offset = offset
        with nogil:
            rados_read_op_cmpext(self.read_op, _cmp_buf, _cmp_buf_len, _offset, NULL)

    def set_flags(self, flags: int = LIBRADOS_OPERATION_NOFLAG):
        """
        Set flags for the last operation added to this read_op.
        :para flags: flags to apply to the last operation
        """

        cdef:
            int _flags = flags

        with nogil:
            rados_read_op_set_flags(self.read_op, _flags)


class ReadOpCtx(ReadOp, OpCtx):
    """read operation context manager"""


cdef void __watch_callback(void *_arg, int64_t _notify_id, uint64_t _cookie,
                           uint64_t _notifier_id, void *_data,
                           size_t _data_len) with gil:
    """
    Watch callback
    """
    cdef object watch = <object>_arg
    data = None
    if _data != NULL:
        data = (<char *>_data)[:_data_len]
    watch._callback(_notify_id, _notifier_id, _cookie, data)

cdef void __watch_error_callback(void *_arg, uint64_t _cookie,
                                 int _error) with gil:
    """
    Watch error callback
    """
    cdef object watch = <object>_arg
    watch._error_callback(_cookie, _error)


cdef class Watch(object):
    """watch object"""

    cdef:
        object id
        Ioctx ioctx
        object oid
        object callback
        object error_callback

    def __cinit__(self, Ioctx ioctx, object oid, object callback,
                  object error_callback, object timeout):
        self.id = 0
        self.ioctx = ioctx.dup()
        self.oid = cstr(oid, 'oid')
        self.callback = callback
        self.error_callback = error_callback

        if timeout is None:
            timeout = 0

        cdef:
            char *_oid = self.oid
            uint64_t _cookie;
            uint32_t _timeout = timeout;
            void *_args = <PyObject*>self

        with nogil:
            ret = rados_watch3(self.ioctx.io, _oid, &_cookie,
                               <rados_watchcb2_t>&__watch_callback,
                               <rados_watcherrcb_t>&__watch_error_callback,
                               _timeout, _args)
        if ret < 0:
            raise make_ex(ret, "watch error")

        self.id = int(_cookie);

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        self.close()
        return False

    def __dealloc__(self):
        if self.id == 0:
            return
        self.ioctx.rados.require_state("connected")
        self.close()

    def _callback(self, notify_id, notifier_id, watch_id, data):
        replay = self.callback(notify_id, notifier_id, watch_id, data)

        cdef:
            rados_ioctx_t _io = <rados_ioctx_t>self.ioctx.io
            char *_obj = self.oid
            int64_t _notify_id = notify_id
            uint64_t _cookie = watch_id
            char *_replay = NULL
            int _replay_len = 0

        if replay is not None:
            replay = cstr(replay, 'replay')
            _replay = replay
            _replaylen = len(replay)

        with nogil:
            rados_notify_ack(_io, _obj, _notify_id, _cookie, _replay,
                             _replaylen)

    def _error_callback(self, watch_id, error):
        if self.error_callback is None:
            return
        self.error_callback(watch_id, error)

    def get_id(self) -> int:
        return self.id

    def check(self):
        """
        Check on watch validity.

        :raises: :class:`Error`
        :returns: timedelta since last confirmed valid
        """
        self.ioctx.require_ioctx_open()

        cdef:
            uint64_t _cookie = self.id

        with nogil:
            ret = rados_watch_check(self.ioctx.io, _cookie)
        if ret < 0:
            raise make_ex(ret, "check error")

        return timedelta(milliseconds=ret)

    def close(self):
        """
        Unregister an interest in an object.

        :raises: :class:`Error`
        """
        if self.id == 0:
            return

        self.ioctx.require_ioctx_open()

        cdef:
            uint64_t _cookie = self.id

        with nogil:
            ret = rados_unwatch2(self.ioctx.io, _cookie)
        if ret < 0 and ret != -errno.ENOENT:
            raise make_ex(ret, "unwatch error")
        self.id = 0

        with nogil:
            cluster = rados_ioctx_get_cluster(self.ioctx.io)
            ret = rados_watch_flush(cluster);
        if ret < 0:
            raise make_ex(ret, "watch_flush error")

        self.ioctx.close()


cdef int __aio_complete_cb(rados_completion_t completion, void *args) with gil:
    """
    Callback to oncomplete() for asynchronous operations
    """
    cdef object cb = <object>args
    cb._complete()
    return 0

cdef class Ioctx(object):
    """rados.Ioctx object"""
    # NOTE(sileht): attributes declared in .pyd

    def __init__(self, rados, name):
        self.rados = rados
        self.name = name
        self.state = "open"

        self.locator_key = ""
        self.nspace = ""
        self.lock = threading.Lock()
        self.safe_completions = []
        self.complete_completions = []

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        self.close()
        return False

    def __dealloc__(self):
        self.close()

    def __track_completion(self, completion_obj):
        if completion_obj.oncomplete:
            with self.lock:
                self.complete_completions.append(completion_obj)
        if completion_obj.onsafe:
            with self.lock:
                self.safe_completions.append(completion_obj)

    def __get_completion(self,
                         oncomplete: Callable[[Completion], None],
                         onsafe: Callable[[Completion], None]):
        """
        Constructs a completion to use with asynchronous operations

        :param oncomplete: what to do when the write is safe and complete in memory
            on all replicas
        :param onsafe:  what to do when the write is safe and complete on storage
            on all replicas

        :raises: :class:`Error`
        :returns: completion object
        """

        completion_obj = Completion(self, oncomplete, onsafe)

        cdef:
            rados_callback_t complete_cb = NULL
            rados_completion_t completion
            PyObject* p_completion_obj= <PyObject*>completion_obj

        if oncomplete:
            complete_cb = <rados_callback_t>&__aio_complete_cb

        with nogil:
            ret = rados_aio_create_completion2(p_completion_obj, complete_cb,
                                              &completion)
        if ret < 0:
            raise make_ex(ret, "error getting a completion")

        completion_obj.rados_comp = completion
        return completion_obj

    def dup(self):
        """
        Duplicate IoCtx
        """

        ioctx = self.rados.open_ioctx2(self.get_pool_id())
        ioctx.set_namespace(self.get_namespace())
        return ioctx

    def aio_stat(self,
                 object_name: str,
                 oncomplete: Callable[[Completion, Optional[int], Optional[time.struct_time]], None]) -> Completion:
        """
        Asynchronously get object stats (size/mtime)

        oncomplete will be called with the returned size and mtime
        as well as the completion:

        oncomplete(completion, size, mtime)

        :param object_name: the name of the object to get stats from
        :param oncomplete: what to do when the stat is complete

        :raises: :class:`Error`
        :returns: completion object
        """

        object_name_raw = cstr(object_name, 'object_name')

        cdef:
            Completion completion
            char *_object_name = object_name_raw
            uint64_t psize
            time_t pmtime

        def oncomplete_(completion_v):
            cdef Completion _completion_v = completion_v
            return_value = _completion_v.get_return_value()
            if return_value >= 0:
                return oncomplete(_completion_v, psize, time.localtime(pmtime))
            else:
                return oncomplete(_completion_v, None, None)

        completion = self.__get_completion(oncomplete_, None)
        self.__track_completion(completion)
        with nogil:
            ret = rados_aio_stat(self.io, _object_name, completion.rados_comp,
                                 &psize, &pmtime)

        if ret < 0:
            completion._cleanup()
            raise make_ex(ret, "error stating %s" % object_name)
        return completion

    def aio_write(self, object_name: str, to_write: bytes, offset: int = 0,
                  oncomplete: Optional[Callable[[Completion], None]] = None,
                  onsafe: Optional[Callable[[Completion], None]] = None) -> Completion:
        """
        Write data to an object asynchronously

        Queues the write and returns.

        :param object_name: name of the object
        :param to_write: data to write
        :param offset: byte offset in the object to begin writing at
        :param oncomplete: what to do when the write is safe and complete in memory
            on all replicas
        :param onsafe:  what to do when the write is safe and complete on storage
            on all replicas

        :raises: :class:`Error`
        :returns: completion object
        """

        object_name_raw = cstr(object_name, 'object_name')

        cdef:
            Completion completion
            char* _object_name = object_name_raw
            char* _to_write = to_write
            size_t size = len(to_write)
            uint64_t _offset = offset

        completion = self.__get_completion(oncomplete, onsafe)
        self.__track_completion(completion)
        with nogil:
            ret = rados_aio_write(self.io, _object_name, completion.rados_comp,
                                _to_write, size, _offset)
        if ret < 0:
            completion._cleanup()
            raise make_ex(ret, "error writing object %s" % object_name)
        return completion

    def aio_write_full(self, object_name: str, to_write: bytes,
                  oncomplete: Optional[Callable] = None,
                  onsafe: Optional[Callable] = None) -> Completion:
        """
        Asynchronously write an entire object

        The object is filled with the provided data. If the object exists,
        it is atomically truncated and then written.
        Queues the write and returns.

        :param object_name: name of the object
        :param to_write: data to write
        :param oncomplete: what to do when the write is safe and complete in memory
            on all replicas
        :param onsafe:  what to do when the write is safe and complete on storage
            on all replicas

        :raises: :class:`Error`
        :returns: completion object
        """

        object_name_raw = cstr(object_name, 'object_name')

        cdef:
            Completion completion
            char* _object_name = object_name_raw
            char* _to_write = to_write
            size_t size = len(to_write)

        completion = self.__get_completion(oncomplete, onsafe)
        self.__track_completion(completion)
        with nogil:
            ret = rados_aio_write_full(self.io, _object_name,
                                    completion.rados_comp,
                                    _to_write, size)
        if ret < 0:
            completion._cleanup()
            raise make_ex(ret, "error writing object %s" % object_name)
        return completion

    def aio_writesame(self, object_name: str, to_write: bytes,
                      write_len: int, offset: int = 0,
                      oncomplete: Optional[Callable] = None) -> Completion:
        """    
        Asynchronously write the same buffer multiple times

        :param object_name: name of the object
        :param to_write: data to write
        :param write_len: total number of bytes to write
        :param offset: byte offset in the object to begin writing at
        :param oncomplete: what to do when the writesame is safe and 
            complete in memory on all replicas
        :raises: :class:`Error`
        :returns: completion object
        """

        object_name_raw = cstr(object_name, 'object_name')

        cdef:
            Completion completion
            char* _object_name = object_name_raw
            char* _to_write = to_write
            size_t _data_len = len(to_write)
            size_t _write_len = write_len
            uint64_t _offset = offset

        completion = self.__get_completion(oncomplete, None)
        self.__track_completion(completion)
        with nogil:
            ret = rados_aio_writesame(self.io, _object_name, completion.rados_comp, 
                                       _to_write, _data_len, _write_len, _offset)

        if ret < 0:
            completion._cleanup()
            raise make_ex(ret, "error writing object %s" % object_name)
        return completion

    def aio_append(self, object_name: str, to_append: bytes,
                  oncomplete: Optional[Callable] = None,
                  onsafe: Optional[Callable] = None) -> Completion:
        """
        Asynchronously append data to an object

        Queues the write and returns.

        :param object_name: name of the object
        :param to_append: data to append
        :param offset: byte offset in the object to begin writing at
        :param oncomplete: what to do when the write is safe and complete in memory
            on all replicas
        :param onsafe:  what to do when the write is safe and complete on storage
            on all replicas

        :raises: :class:`Error`
        :returns: completion object
        """
        object_name_raw = cstr(object_name, 'object_name')

        cdef:
            Completion completion
            char* _object_name = object_name_raw
            char* _to_append = to_append
            size_t size = len(to_append)

        completion = self.__get_completion(oncomplete, onsafe)
        self.__track_completion(completion)
        with nogil:
            ret = rados_aio_append(self.io, _object_name,
                                completion.rados_comp,
                                _to_append, size)
        if ret < 0:
            completion._cleanup()
            raise make_ex(ret, "error appending object %s" % object_name)
        return completion

    def aio_flush(self):
        """
        Block until all pending writes in an io context are safe

        :raises: :class:`Error`
        """
        with nogil:
            ret = rados_aio_flush(self.io)
        if ret < 0:
            raise make_ex(ret, "error flushing")

    def aio_cmpext(self, object_name: str, cmp_buf: bytes, offset: int = 0,
                  oncomplete: Optional[Callable] = None) -> Completion:
        """
        Asynchronously compare an on-disk object range with a buffer
        :param object_name: the name of the object
        :param cmp_buf: buffer containing bytes to be compared with object contents
        :param offset: object byte offset at which to start the comparison
        :param oncomplete: what to do when the write is safe and complete in memory
            on all replicas

        :raises: :class:`TypeError`
        returns: 0 - on success, negative error code on failure,
                 (-MAX_ERRNO - mismatch_off) on mismatch
        """
        object_name_raw = cstr(object_name, 'object_name')

        cdef:
            Completion completion
            char* _object_name = object_name_raw
            char* _cmp_buf = cmp_buf
            size_t _cmp_buf_len = len(cmp_buf)
            uint64_t _offset = offset

        completion = self.__get_completion(oncomplete, None)
        self.__track_completion(completion)

        with nogil:
            ret = rados_aio_cmpext(self.io, _object_name, completion.rados_comp,
                                   _cmp_buf, _cmp_buf_len, _offset)

        if ret < 0:
            completion._cleanup()
            raise make_ex(ret, "failed to compare %s" % object_name)
        return completion

    def aio_rmxattr(self, object_name: str, xattr_name: str,
                    oncomplete: Optional[Callable] = None) -> Completion:
        """
        Asynchronously delete an extended attribute from an object

        :param object_name: the name of the object to remove xattr from
        :param xattr_name: which extended attribute to remove
        :param oncomplete: what to do when the rmxattr completes

        :raises: :class:`Error`
        :returns: completion object
        """
        object_name_raw = cstr(object_name, 'object_name')
        xattr_name_raw = cstr(xattr_name , 'xattr_name')

        cdef:
            Completion completion
            char* _object_name = object_name_raw
            char* _xattr_name = xattr_name_raw

        completion = self.__get_completion(oncomplete, None)
        self.__track_completion(completion)
        with nogil:
            ret = rados_aio_rmxattr(self.io, _object_name,
                                    completion.rados_comp, _xattr_name)

        if ret < 0:
            completion._cleanup()
            raise make_ex(ret, "Failed to remove xattr %r" % xattr_name)
        return completion

    def aio_read(self, object_name: str, length: int, offset: int,
                  oncomplete: Optional[Callable] = None) -> Completion:
        """
        Asynchronously read data from an object

        oncomplete will be called with the returned read value as
        well as the completion:

        oncomplete(completion, data_read)

        :param object_name: name of the object to read from
        :param length: the number of bytes to read
        :param offset: byte offset in the object to begin reading from
        :param oncomplete: what to do when the read is complete

        :raises: :class:`Error`
        :returns: completion object
        """

        object_name_raw = cstr(object_name, 'object_name')

        cdef:
            Completion completion
            char* _object_name = object_name_raw
            uint64_t _offset = offset

            char *ref_buf
            size_t _length = length

        def oncomplete_(completion_v):
            cdef Completion _completion_v = completion_v
            return_value = _completion_v.get_return_value()
            if return_value > 0 and return_value != length:
                _PyBytes_Resize(&_completion_v.buf, return_value)
            return oncomplete(_completion_v, <object>_completion_v.buf if return_value >= 0 else None)

        completion = self.__get_completion(oncomplete_, None)
        completion.buf = PyBytes_FromStringAndSize(NULL, length)
        ret_buf = PyBytes_AsString(completion.buf)
        self.__track_completion(completion)
        with nogil:
            ret = rados_aio_read(self.io, _object_name, completion.rados_comp,
                                ret_buf, _length, _offset)
        if ret < 0:
            completion._cleanup()
            raise make_ex(ret, "error reading %s" % object_name)
        return completion

    def aio_execute(self, object_name: str, cls: str, method: str,
                    data: bytes, length: int = 8192,
                    oncomplete: Optional[Callable[[Completion, bytes], None]] = None,
                    onsafe: Optional[Callable[[Completion, bytes], None]] = None) -> Completion:
        """
        Asynchronously execute an OSD class method on an object.

        oncomplete and onsafe will be called with the data returned from
        the plugin as well as the completion:

        oncomplete(completion, data)
        onsafe(completion, data)

        :param object_name: name of the object
        :param cls: name of the object class
        :param method: name of the method
        :param data: input data
        :param length: size of output buffer in bytes (default=8192)
        :param oncomplete: what to do when the execution is complete
        :param onsafe:  what to do when the execution is safe and complete

        :raises: :class:`Error`
        :returns: completion object
        """

        object_name_raw = cstr(object_name, 'object_name')
        cls_raw = cstr(cls, 'cls')
        method_raw = cstr(method, 'method')
        cdef:
            Completion completion
            char *_object_name = object_name_raw
            char *_cls = cls_raw
            char *_method = method_raw
            char *_data = data
            size_t _data_len = len(data)

            char *ref_buf
            size_t _length = length

        def oncomplete_(completion_v):
            cdef Completion _completion_v = completion_v
            return_value = _completion_v.get_return_value()
            if return_value > 0 and return_value != length:
                _PyBytes_Resize(&_completion_v.buf, return_value)
            return oncomplete(_completion_v, <object>_completion_v.buf if return_value >= 0 else None)

        def onsafe_(completion_v):
            cdef Completion _completion_v = completion_v
            return_value = _completion_v.get_return_value()
            return onsafe(_completion_v, <object>_completion_v.buf if return_value >= 0 else None)

        completion = self.__get_completion(oncomplete_ if oncomplete else None, onsafe_ if onsafe else None)
        completion.buf = PyBytes_FromStringAndSize(NULL, length)
        ret_buf = PyBytes_AsString(completion.buf)
        self.__track_completion(completion)
        with nogil:
            ret = rados_aio_exec(self.io, _object_name, completion.rados_comp,
                                 _cls, _method, _data, _data_len, ret_buf, _length)
        if ret < 0:
            completion._cleanup()
            raise make_ex(ret, "error executing %s::%s on %s" % (cls, method, object_name))
        return completion

    def aio_setxattr(self, object_name: str, xattr_name: str, xattr_value: bytes,
                     oncomplete: Optional[Callable] = None) -> Completion:
        """
        Asynchronously set an extended attribute on an object

        :param object_name: the name of the object to set xattr to
        :param xattr_name: which extended attribute to set
        :param xattr_value: the value of the  extended attribute
        :param oncomplete: what to do when the setxttr completes

        :raises: :class:`Error`
        :returns: completion object
        """
        object_name_raw = cstr(object_name, 'object_name')
        xattr_name_raw = cstr(xattr_name , 'xattr_name')

        cdef:
            Completion completion
            char* _object_name = object_name_raw
            char* _xattr_name = xattr_name_raw
            char* _xattr_value = xattr_value
            size_t xattr_value_len = len(xattr_value)

        completion = self.__get_completion(oncomplete, None)
        self.__track_completion(completion)
        with nogil:
            ret = rados_aio_setxattr(self.io, _object_name,
                               completion.rados_comp,
                               _xattr_name, _xattr_value, xattr_value_len)

        if ret < 0:
            completion._cleanup()
            raise make_ex(ret, "Failed to set xattr %r" % xattr_name)
        return completion

    def aio_remove(self, object_name: str,
                  oncomplete: Optional[Callable] = None,
                  onsafe: Optional[Callable] = None) -> Completion:
        """
        Asynchronously remove an object

        :param object_name: name of the object to remove
        :param oncomplete: what to do when the remove is safe and complete in memory
            on all replicas
        :param onsafe:  what to do when the remove is safe and complete on storage
            on all replicas

        :raises: :class:`Error`
        :returns: completion object
        """
        object_name_raw = cstr(object_name, 'object_name')

        cdef:
            Completion completion
            char* _object_name = object_name_raw

        completion = self.__get_completion(oncomplete, onsafe)
        self.__track_completion(completion)
        with nogil:
            ret = rados_aio_remove(self.io, _object_name,
                                completion.rados_comp)
        if ret < 0:
            completion._cleanup()
            raise make_ex(ret, "error removing %s" % object_name)
        return completion

    def require_ioctx_open(self):
        """
        Checks if the rados.Ioctx object state is 'open'

        :raises: IoctxStateError
        """
        if self.state != "open":
            raise IoctxStateError("The pool is %s" % self.state)

    def set_locator_key(self, loc_key: str):
        """
        Set the key for mapping objects to pgs within an io context.

        The key is used instead of the object name to determine which
        placement groups an object is put in. This affects all subsequent
        operations of the io context - until a different locator key is
        set, all objects in this io context will be placed in the same pg.

        :param loc_key: the key to use as the object locator, or NULL to discard
            any previously set key

        :raises: :class:`TypeError`
        """
        self.require_ioctx_open()
        cloc_key = cstr(loc_key, 'loc_key')
        cdef char *_loc_key = cloc_key
        with nogil:
            rados_ioctx_locator_set_key(self.io, _loc_key)
        self.locator_key = loc_key

    def get_locator_key(self) -> str:
        """
        Get the locator_key of context

        :returns: locator_key
        """
        return self.locator_key

    def set_read(self, snap_id: int):
        """
        Set the snapshot for reading objects.

        To stop to read from snapshot, use set_read(LIBRADOS_SNAP_HEAD)

        :param snap_id: the snapshot Id

        :raises: :class:`TypeError`
        """
        self.require_ioctx_open()
        cdef rados_snap_t _snap_id = snap_id
        with nogil:
            rados_ioctx_snap_set_read(self.io, _snap_id)

    def set_namespace(self, nspace: str):
        """
        Set the namespace for objects within an io context.

        The namespace in addition to the object name fully identifies
        an object. This affects all subsequent operations of the io context
        - until a different namespace is set, all objects in this io context
        will be placed in the same namespace.

        :param nspace: the namespace to use, or None/"" for the default namespace

        :raises: :class:`TypeError`
        """
        self.require_ioctx_open()
        if nspace is None:
            nspace = ""
        cnspace = cstr(nspace, 'nspace')
        cdef char *_nspace = cnspace
        with nogil:
            rados_ioctx_set_namespace(self.io, _nspace)
        self.nspace = nspace

    def get_namespace(self) -> str:
        """
        Get the namespace of context

        :returns: namespace
        """
        return self.nspace

    def close(self):
        """
        Close a rados.Ioctx object.

        This just tells librados that you no longer need to use the io context.
        It may not be freed immediately if there are pending asynchronous
        requests on it, but you should not use an io context again after
        calling this function on it.
        """
        if self.state == "open":
            self.require_ioctx_open()
            with nogil:
                rados_ioctx_destroy(self.io)
            self.state = "closed"


    def write(self, key: str, data: bytes, offset: int = 0):
        """
        Write data to an object synchronously

        :param key: name of the object
        :param data: data to write
        :param offset: byte offset in the object to begin writing at

        :raises: :class:`TypeError`
        :raises: :class:`LogicError`
        :returns: int - 0 on success
        """
        self.require_ioctx_open()

        key_raw = cstr(key, 'key')
        cdef:
            char *_key = key_raw
            char *_data = data
            size_t length = len(data)
            uint64_t _offset = offset

        with nogil:
            ret = rados_write(self.io, _key, _data, length, _offset)
        if ret == 0:
            return ret
        elif ret < 0:
            raise make_ex(ret, "Ioctx.write(%s): failed to write %s"
                          % (self.name, key))
        else:
            raise LogicError("Ioctx.write(%s): rados_write \
returned %d, but should return zero on success." % (self.name, ret))

    def write_full(self, key: str, data: bytes):
        """
        Write an entire object synchronously.

        The object is filled with the provided data. If the object exists,
        it is atomically truncated and then written.

        :param key: name of the object
        :param data: data to write

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: int - 0 on success
        """
        self.require_ioctx_open()
        key_raw = cstr(key, 'key')
        cdef:
            char *_key = key_raw
            char *_data = data
            size_t length = len(data)

        with nogil:
            ret = rados_write_full(self.io, _key, _data, length)
        if ret == 0:
            return ret
        elif ret < 0:
            raise make_ex(ret, "Ioctx.write_full(%s): failed to write %s"
                          % (self.name, key))
        else:
            raise LogicError("Ioctx.write_full(%s): rados_write_full \
returned %d, but should return zero on success." % (self.name, ret))

    def writesame(self, key: str, data: bytes, write_len: int, offset: int = 0):
        """
        Write the same buffer multiple times
        :param key: name of the object
        :param data: data to write
        :param write_len: total number of bytes to write
        :param offset: byte offset in the object to begin writing at

        :raises: :class:`TypeError`
        :raises: :class:`LogicError`
        """
        self.require_ioctx_open()

        key_raw = cstr(key, 'key')
        cdef:
            char *_key = key_raw
            char *_data = data
            size_t _data_len = len(data)
            size_t _write_len = write_len
            uint64_t _offset = offset

        with nogil:
            ret = rados_writesame(self.io, _key, _data, _data_len, _write_len, _offset)
        if ret < 0:
            raise make_ex(ret, "Ioctx.writesame(%s): failed to write %s"
                           % (self.name, key))
        assert(ret == 0)

    def append(self, key: str, data: bytes):
        """
        Append data to an object synchronously

        :param key: name of the object
        :param data: data to write

        :raises: :class:`TypeError`
        :raises: :class:`LogicError`
        :returns: int - 0 on success
        """
        self.require_ioctx_open()
        key_raw = cstr(key, 'key')
        cdef:
            char *_key = key_raw
            char *_data = data
            size_t length = len(data)

        with nogil:
            ret = rados_append(self.io, _key, _data, length)
        if ret == 0:
            return ret
        elif ret < 0:
            raise make_ex(ret, "Ioctx.append(%s): failed to append %s"
                          % (self.name, key))
        else:
            raise LogicError("Ioctx.append(%s): rados_append \
returned %d, but should return zero on success." % (self.name, ret))

    def read(self, key: str, length: int = 8192, offset: int = 0) -> bytes:
        """
        Read data from an object synchronously

        :param key: name of the object
        :param length: the number of bytes to read (default=8192)
        :param offset: byte offset in the object to begin reading at

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: data read from object
        """
        self.require_ioctx_open()
        key_raw = cstr(key, 'key')
        cdef:
            char *_key = key_raw
            char *ret_buf
            uint64_t _offset = offset
            size_t _length = length
            PyObject* ret_s = NULL

        ret_s = PyBytes_FromStringAndSize(NULL, length)
        try:
            ret_buf = PyBytes_AsString(ret_s)
            with nogil:
                ret = rados_read(self.io, _key, ret_buf, _length, _offset)
            if ret < 0:
                raise make_ex(ret, "Ioctx.read(%s): failed to read %s" % (self.name, key))

            if ret != length:
                _PyBytes_Resize(&ret_s, ret)

            return <object>ret_s
        finally:
            # We DECREF unconditionally: the cast to object above will have
            # INCREFed if necessary. This also takes care of exceptions,
            # including if _PyString_Resize fails (that will free the string
            # itself and set ret_s to NULL, hence XDECREF).
            ref.Py_XDECREF(ret_s)

    def execute(self, key: str, cls: str, method: str, data: bytes, length: int = 8192) -> Tuple[int, object]:
        """
        Execute an OSD class method on an object.

        :param key: name of the object
        :param cls: name of the object class
        :param method: name of the method
        :param data: input data
        :param length: size of output buffer in bytes (default=8192)

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: (ret, method output)
        """
        self.require_ioctx_open()

        key_raw = cstr(key, 'key')
        cls_raw = cstr(cls, 'cls')
        method_raw = cstr(method, 'method')
        cdef:
            char *_key = key_raw
            char *_cls = cls_raw
            char *_method = method_raw
            char *_data = data
            size_t _data_len = len(data)

            char *ref_buf
            size_t _length = length
            PyObject* ret_s = NULL

        ret_s = PyBytes_FromStringAndSize(NULL, length)
        try:
            ret_buf = PyBytes_AsString(ret_s)
            with nogil:
                ret = rados_exec(self.io, _key, _cls, _method, _data,
                                 _data_len, ret_buf, _length)
            if ret < 0:
                raise make_ex(ret, "Ioctx.read(%s): failed to read %s" % (self.name, key))

            if ret != length:
                _PyBytes_Resize(&ret_s, ret)

            return ret, <object>ret_s
        finally:
            # We DECREF unconditionally: the cast to object above will have
            # INCREFed if necessary. This also takes care of exceptions,
            # including if _PyString_Resize fails (that will free the string
            # itself and set ret_s to NULL, hence XDECREF).
            ref.Py_XDECREF(ret_s)

    def get_stats(self) -> Dict[str, int]:
        """
        Get pool usage statistics

        :returns: dict contains the following keys:

            - ``num_bytes`` (int) - size of pool in bytes

            - ``num_kb`` (int) - size of pool in kbytes

            - ``num_objects`` (int) - number of objects in the pool

            - ``num_object_clones`` (int) - number of object clones

            - ``num_object_copies`` (int) - number of object copies

            - ``num_objects_missing_on_primary`` (int) - number of objets
                missing on primary

            - ``num_objects_unfound`` (int) - number of unfound objects

            - ``num_objects_degraded`` (int) - number of degraded objects

            - ``num_rd`` (int) - bytes read

            - ``num_rd_kb`` (int) - kbytes read

            - ``num_wr`` (int) - bytes written

            - ``num_wr_kb`` (int) - kbytes written
        """
        self.require_ioctx_open()
        cdef rados_pool_stat_t stats
        with nogil:
            ret = rados_ioctx_pool_stat(self.io, &stats)
        if ret < 0:
            raise make_ex(ret, "Ioctx.get_stats(%s): get_stats failed" % self.name)
        return {'num_bytes': stats.num_bytes,
                'num_kb': stats.num_kb,
                'num_objects': stats.num_objects,
                'num_object_clones': stats.num_object_clones,
                'num_object_copies': stats.num_object_copies,
                "num_objects_missing_on_primary": stats.num_objects_missing_on_primary,
                "num_objects_unfound": stats.num_objects_unfound,
                "num_objects_degraded": stats.num_objects_degraded,
                "num_rd": stats.num_rd,
                "num_rd_kb": stats.num_rd_kb,
                "num_wr": stats.num_wr,
                "num_wr_kb": stats.num_wr_kb}

    def remove_object(self, key: str) -> bool:
        """
        Delete an object

        This does not delete any snapshots of the object.

        :param key: the name of the object to delete

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: True on success
        """
        self.require_ioctx_open()
        key_raw = cstr(key, 'key')
        cdef:
            char *_key = key_raw

        with nogil:
            ret = rados_remove(self.io, _key)
        if ret < 0:
            raise make_ex(ret, "Failed to remove '%s'" % key)
        return True

    def trunc(self, key: str, size: int) -> int:
        """
        Resize an object

        If this enlarges the object, the new area is logically filled with
        zeroes. If this shrinks the object, the excess data is removed.

        :param key: the name of the object to resize
        :param size: the new size of the object in bytes

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: 0 on success, otherwise raises error
        """

        self.require_ioctx_open()
        key_raw = cstr(key, 'key')
        cdef:
            char *_key = key_raw
            uint64_t _size = size

        with nogil:
            ret = rados_trunc(self.io, _key, _size)
        if ret < 0:
            raise make_ex(ret, "Ioctx.trunc(%s): failed to truncate %s" % (self.name, key))
        return ret
   
    def cmpext(self, key: str, cmp_buf: bytes, offset: int = 0) -> int:
        '''
        Compare an on-disk object range with a buffer
        :param key: the name of the object
        :param cmp_buf: buffer containing bytes to be compared with object contents
        :param offset: object byte offset at which to start the comparison
        
        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: 0 - on success, negative error code on failure,
                 (-MAX_ERRNO - mismatch_off) on mismatch
        '''
        self.require_ioctx_open()
        key_raw = cstr(key, 'key')
        cdef:
             char *_key = key_raw
             char *_cmp_buf = cmp_buf
             size_t _cmp_buf_len = len(cmp_buf)
             uint64_t _offset = offset
        with nogil:
            ret = rados_cmpext(self.io, _key, _cmp_buf, _cmp_buf_len, _offset)
        assert ret < -MAX_ERRNO or ret == 0, "Ioctx.cmpext(%s): failed to compare %s" % (self.name, key)        
        return ret

    def stat(self, key: str) -> Tuple[int, time.struct_time]:
        """
        Get object stats (size/mtime)

        :param key: the name of the object to get stats from

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: (size,timestamp)
        """
        self.require_ioctx_open()

        key_raw = cstr(key, 'key')
        cdef:
            char *_key = key_raw
            uint64_t psize
            time_t pmtime

        with nogil:
            ret = rados_stat(self.io, _key, &psize, &pmtime)
        if ret < 0:
            raise make_ex(ret, "Failed to stat %r" % key)
        return psize, time.localtime(pmtime)

    def get_xattr(self, key: str, xattr_name: str) -> bytes:
        """
        Get the value of an extended attribute on an object.

        :param key: the name of the object to get xattr from
        :param xattr_name: which extended attribute to read

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: value of the xattr
        """
        self.require_ioctx_open()

        key_raw = cstr(key, 'key')
        xattr_name_raw = cstr(xattr_name, 'xattr_name')
        cdef:
            char *_key = key_raw
            char *_xattr_name = xattr_name_raw
            size_t ret_length = 4096
            char *ret_buf = NULL

        try:
            while ret_length < 4096 * 1024 * 1024:
                ret_buf = <char *>realloc_chk(ret_buf, ret_length)
                with nogil:
                    ret = rados_getxattr(self.io, _key, _xattr_name, ret_buf, ret_length)
                if ret == -errno.ERANGE:
                    ret_length *= 2
                elif ret < 0:
                    raise make_ex(ret, "Failed to get xattr %r" % xattr_name)
                else:
                    break
            return ret_buf[:ret]
        finally:
            free(ret_buf)

    def get_xattrs(self, oid: str) -> XattrIterator:
        """
        Start iterating over xattrs on an object.

        :param oid: the name of the object to get xattrs from

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: XattrIterator
        """
        self.require_ioctx_open()
        return XattrIterator(self, oid)

    def set_xattr(self, key: str, xattr_name: str, xattr_value: bytes) -> bool:
        """
        Set an extended attribute on an object.

        :param key: the name of the object to set xattr to
        :param xattr_name: which extended attribute to set
        :param xattr_value: the value of the  extended attribute

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: True on success, otherwise raise an error
        """
        self.require_ioctx_open()

        key_raw = cstr(key, 'key')
        xattr_name_raw = cstr(xattr_name, 'xattr_name')
        cdef:
            char *_key = key_raw
            char *_xattr_name = xattr_name_raw
            char *_xattr_value = xattr_value
            size_t _xattr_value_len = len(xattr_value)

        with nogil:
            ret = rados_setxattr(self.io, _key, _xattr_name,
                                 _xattr_value, _xattr_value_len)
        if ret < 0:
            raise make_ex(ret, "Failed to set xattr %r" % xattr_name)
        return True

    def rm_xattr(self, key: str, xattr_name: str) -> bool:
        """
        Removes an extended attribute on from an object.

        :param key: the name of the object to remove xattr from
        :param xattr_name: which extended attribute to remove

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: True on success, otherwise raise an error
        """
        self.require_ioctx_open()

        key_raw = cstr(key, 'key')
        xattr_name_raw = cstr(xattr_name, 'xattr_name')
        cdef:
            char *_key = key_raw
            char *_xattr_name = xattr_name_raw

        with nogil:
            ret = rados_rmxattr(self.io, _key, _xattr_name)
        if ret < 0:
            raise make_ex(ret, "Failed to delete key %r xattr %r" %
                          (key, xattr_name))
        return True

    def notify(self, obj: str, msg: str = '', timeout_ms: int = 5000) -> bool:
        """
        Send a rados notification to an object.

        :param obj: the name of the object to notify
        :param msg: optional message to send in the notification
        :param timeout_ms: notify timeout (in ms)

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: True on success, otherwise raise an error
        """
        self.require_ioctx_open()

        msglen = len(msg)
        obj_raw = cstr(obj, 'obj')
        msg_raw = cstr(msg, 'msg')
        cdef:
            char *_obj = obj_raw
            char *_msg = msg_raw
            int _msglen = msglen
            uint64_t _timeout_ms = timeout_ms

        with nogil:
            ret = rados_notify2(self.io, _obj, _msg, _msglen, _timeout_ms,
                                NULL, NULL)
        if ret < 0:
            raise make_ex(ret, "Failed to notify %r" % (obj))
        return True

    def aio_notify(self, obj: str,
                   oncomplete: Callable[[Completion, int, Optional[List], Optional[List]], None],
                   msg: str = '', timeout_ms: int = 5000) -> Completion:
        """
        Asynchronously send a rados notification to an object
        """
        self.require_ioctx_open()

        msglen = len(msg)
        obj_raw = cstr(obj, 'obj')
        msg_raw = cstr(msg, 'msg')

        cdef:
            Completion completion
            char *_obj = obj_raw
            char *_msg = msg_raw
            int _msglen = msglen
            uint64_t _timeout_ms = timeout_ms
            char *reply
            size_t replylen = 0

        def oncomplete_(completion_v):
            cdef:
                Completion _completion_v = completion_v
                notify_ack_t *acks = NULL
                notify_timeout_t *timeouts = NULL
                size_t nr_acks
                size_t nr_timeouts
            return_value = _completion_v.get_return_value()
            if return_value == 0:
                return_value = rados_decode_notify_response(reply, replylen, &acks, &nr_acks, &timeouts, &nr_timeouts)
                rados_buffer_free(reply)
            if return_value == 0:
                ack_list = [(ack.notifier_id, ack.cookie, '' if not ack.payload_len \
                                                             else ack.payload[:ack.payload_len]) for ack in acks[:nr_acks]]
                timeout_list = [(timeout.notifier_id, timeout.cookie) for timeout in timeouts[:nr_timeouts]]
                rados_free_notify_response(acks, nr_acks, timeouts)
                return oncomplete(_completion_v, 0, ack_list, timeout_list)
            else:
                return oncomplete(_completion_v, return_value, None, None)

        completion = self.__get_completion(oncomplete_, None)
        self.__track_completion(completion)
        with nogil:
            ret = rados_aio_notify(self.io, _obj, completion.rados_comp,
                                   _msg, _msglen, _timeout_ms, &reply, &replylen)
        if ret < 0:
            completion._cleanup()
            raise make_ex(ret, "aio_notify error: %s" % obj)
        return completion

    def watch(self, obj: str,
              callback: Callable[[int, str, int, bytes], None],
              error_callback: Optional[Callable[[int], None]] = None,
              timeout: Optional[int] = None) -> Watch:
        """
        Register an interest in an object.

        :param obj: the name of the object to notify
        :param callback: what to do when a notify is received on this object
        :param error_callback: what to do when the watch session encounters an error
        :param timeout: how many seconds the connection will keep after disconnection

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: watch_id - internal id assigned to this watch
        """
        self.require_ioctx_open()

        return Watch(self, obj, callback, error_callback, timeout)

    def list_objects(self) -> ObjectIterator:
        """
        Get ObjectIterator on rados.Ioctx object.

        :returns: ObjectIterator
        """
        self.require_ioctx_open()
        return ObjectIterator(self)

    def list_snaps(self) -> SnapIterator:
        """
        Get SnapIterator on rados.Ioctx object.

        :returns: SnapIterator
        """
        self.require_ioctx_open()
        return SnapIterator(self)

    def get_pool_id(self) -> int:
        """
        Get pool id

        :returns: int - pool id
        """
        with nogil:
            ret = rados_ioctx_get_id(self.io)
        return ret;

    def get_pool_name(self) -> str:
        """
        Get pool name

        :returns: pool name
        """
        cdef:
            int name_len = 10
            char *name = NULL

        try:
            while True:
                name = <char *>realloc_chk(name, name_len)
                with nogil:
                    ret = rados_ioctx_get_pool_name(self.io, name, name_len)
                if ret > 0:
                    break
                elif ret != -errno.ERANGE:
                    raise make_ex(ret, "get pool name error")
                else:
                    name_len = name_len * 2

            return decode_cstr(name)
        finally:
            free(name)


    def create_snap(self, snap_name: str):
        """
        Create a pool-wide snapshot

        :param snap_name: the name of the snapshot

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        """
        self.require_ioctx_open()
        snap_name_raw = cstr(snap_name, 'snap_name')
        cdef char *_snap_name = snap_name_raw

        with nogil:
            ret = rados_ioctx_snap_create(self.io, _snap_name)
        if ret != 0:
            raise make_ex(ret, "Failed to create snap %s" % snap_name)

    def remove_snap(self, snap_name: str):
        """
        Removes a pool-wide snapshot

        :param snap_name: the name of the snapshot

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        """
        self.require_ioctx_open()
        snap_name_raw = cstr(snap_name, 'snap_name')
        cdef char *_snap_name = snap_name_raw

        with nogil:
            ret = rados_ioctx_snap_remove(self.io, _snap_name)
        if ret != 0:
            raise make_ex(ret, "Failed to remove snap %s" % snap_name)

    def lookup_snap(self, snap_name: str) -> Snap:
        """
        Get the id of a pool snapshot

        :param snap_name: the name of the snapshot to lookop

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: Snap - on success
        """
        self.require_ioctx_open()
        csnap_name = cstr(snap_name, 'snap_name')
        cdef:
            char *_snap_name = csnap_name
            rados_snap_t snap_id

        with nogil:
            ret = rados_ioctx_snap_lookup(self.io, _snap_name, &snap_id)
        if ret != 0:
            raise make_ex(ret, "Failed to lookup snap %s" % snap_name)
        return Snap(self, snap_name, int(snap_id))

    def snap_rollback(self, oid: str, snap_name: str):
        """
        Rollback an object to a snapshot

        :param oid: the name of the object
        :param snap_name: the name of the snapshot

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        """
        self.require_ioctx_open()
        oid_raw = cstr(oid, 'oid')
        snap_name_raw = cstr(snap_name, 'snap_name')
        cdef:
            char *_oid = oid_raw
            char *_snap_name = snap_name_raw

        with nogil:
            ret = rados_ioctx_snap_rollback(self.io, _oid, _snap_name)
        if ret != 0:
            raise make_ex(ret, "Failed to rollback %s" % oid)

    def create_self_managed_snap(self):
        """
        Creates a self-managed snapshot

        :returns: snap id on success

        :raises: :class:`Error`
        """
        self.require_ioctx_open()
        cdef:
            rados_snap_t _snap_id
        with nogil:
            ret = rados_ioctx_selfmanaged_snap_create(self.io, &_snap_id)
        if ret != 0:
            raise make_ex(ret, "Failed to create self-managed snapshot")
        return int(_snap_id)

    def remove_self_managed_snap(self, snap_id: int):
        """
        Removes a self-managed snapshot

        :param snap_id: the name of the snapshot

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        """
        self.require_ioctx_open()
        cdef:
            rados_snap_t _snap_id = snap_id
        with nogil:
            ret = rados_ioctx_selfmanaged_snap_remove(self.io, _snap_id)
        if ret != 0:
            raise make_ex(ret, "Failed to remove self-managed snapshot")

    def set_self_managed_snap_write(self, snaps: Sequence[Union[int, str]]):
        """
        Updates the write context to the specified self-managed
        snapshot ids.

        :param snaps: all associated self-managed snapshot ids

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        """
        self.require_ioctx_open()
        sorted_snaps = []
        snap_seq = 0
        if snaps:
            sorted_snaps = sorted([int(x) for x in snaps], reverse=True)
            snap_seq = sorted_snaps[0]

        cdef:
            rados_snap_t _snap_seq = snap_seq
            rados_snap_t *_snaps = NULL
            int _num_snaps = len(sorted_snaps)
        try:
            _snaps = <rados_snap_t *>malloc(_num_snaps * sizeof(rados_snap_t))
            for i in range(len(sorted_snaps)):
                _snaps[i] = sorted_snaps[i]
            with nogil:
                ret = rados_ioctx_selfmanaged_snap_set_write_ctx(self.io,
                                                                 _snap_seq,
                                                                 _snaps,
                                                                 _num_snaps)
            if ret != 0:
                raise make_ex(ret, "Failed to update snapshot write context")
        finally:
            free(_snaps)

    def rollback_self_managed_snap(self, oid: str, snap_id: int):
        """
        Rolls an specific object back to a self-managed snapshot revision

        :param oid: the name of the object
        :param snap_id: the name of the snapshot

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        """
        self.require_ioctx_open()
        oid_raw = cstr(oid, 'oid')
        cdef:
            char *_oid = oid_raw
            rados_snap_t _snap_id = snap_id
        with nogil:
            ret = rados_ioctx_selfmanaged_snap_rollback(self.io, _oid, _snap_id)
        if ret != 0:
            raise make_ex(ret, "Failed to rollback %s" % oid)

    def get_last_version(self) -> int:
        """
        Return the version of the last object read or written to.

        This exposes the internal version number of the last object read or
        written via this io context

        :returns: version of the last object used
        """
        self.require_ioctx_open()
        with nogil:
            ret = rados_get_last_version(self.io)
        return int(ret)

    def create_write_op(self) -> WriteOp:
        """
        create write operation object.
        need call release_write_op after use
        """
        return WriteOp().create()

    def create_read_op(self) -> ReadOp:
        """
        create read operation object.
        need call release_read_op after use
        """
        return ReadOp().create()

    def release_write_op(self, write_op):
        """
        release memory alloc by create_write_op
        """
        write_op.release()

    def release_read_op(self, read_op: ReadOp):
        """
        release memory alloc by create_read_op
        :para read_op: read_op object
        """
        read_op.release()

    def set_omap(self, write_op: WriteOp, keys: Sequence[str], values: Sequence[bytes]):
        """
        set keys values to write_op
        :para write_op: write_operation object
        :para keys: a tuple of keys
        :para values: a tuple of values
        """

        if len(keys) != len(values):
            raise Error("Rados(): keys and values must have the same number of items")

        keys = cstr_list(keys, 'keys')
        values = cstr_list(values, 'values')
        lens = [len(v) for v in values]
        cdef:
            WriteOp _write_op = write_op
            size_t key_num = len(keys)
            char **_keys = to_bytes_array(keys)
            char **_values = to_bytes_array(values)
            size_t *_lens = to_csize_t_array(lens)

        try:
            with nogil:
                rados_write_op_omap_set(_write_op.write_op,
                                        <const char**>_keys,
                                        <const char**>_values,
                                        <const size_t*>_lens, key_num)
        finally:
            free(_keys)
            free(_values)
            free(_lens)

    def operate_write_op(self,
                         write_op: WriteOp,
                         oid: str,
                         mtime: int = 0,
                         flags: int = LIBRADOS_OPERATION_NOFLAG):
        """
        execute the real write operation
        :para write_op: write operation object
        :para oid: object name
        :para mtime: the time to set the mtime to, 0 for the current time
        :para flags: flags to apply to the entire operation
        """

        oid_raw = cstr(oid, 'oid')
        cdef:
            WriteOp _write_op = write_op
            char *_oid = oid_raw
            time_t _mtime = mtime
            int _flags = flags

        with nogil:
            ret = rados_write_op_operate(_write_op.write_op, self.io, _oid, &_mtime, _flags)
        if ret != 0:
            raise make_ex(ret, "Failed to operate write op for oid %s" % oid)

    def operate_aio_write_op(self, write_op: WriteOp, oid: str,
                             oncomplete: Optional[Callable[[Completion], None]] = None,
                             onsafe: Optional[Callable[[Completion], None]] = None,
                             mtime: int = 0,
                             flags: int = LIBRADOS_OPERATION_NOFLAG) -> Completion:
        """
        execute the real write operation asynchronously
        :para write_op: write operation object
        :para oid: object name
        :param oncomplete: what to do when the remove is safe and complete in memory
            on all replicas
        :param onsafe:  what to do when the remove is safe and complete on storage
            on all replicas
        :para mtime: the time to set the mtime to, 0 for the current time
        :para flags: flags to apply to the entire operation

        :raises: :class:`Error`
        :returns: completion object
        """

        oid_raw = cstr(oid, 'oid')
        cdef:
            WriteOp _write_op = write_op
            char *_oid = oid_raw
            Completion completion
            time_t _mtime = mtime
            int _flags = flags

        completion = self.__get_completion(oncomplete, onsafe)
        self.__track_completion(completion)

        with nogil:
            ret = rados_aio_write_op_operate(_write_op.write_op, self.io, completion.rados_comp, _oid,
                                             &_mtime, _flags)
        if ret != 0:
            completion._cleanup()
            raise make_ex(ret, "Failed to operate aio write op for oid %s" % oid)
        return completion

    def operate_read_op(self, read_op: ReadOp, oid: str, flag: int = LIBRADOS_OPERATION_NOFLAG):
        """
        execute the real read operation
        :para read_op: read operation object
        :para oid: object name
        :para flag: flags to apply to the entire operation
        """
        oid_raw = cstr(oid, 'oid')
        cdef:
            ReadOp _read_op = read_op
            char *_oid = oid_raw
            int _flag = flag

        with nogil:
            ret = rados_read_op_operate(_read_op.read_op, self.io, _oid, _flag)
        if ret != 0:
            raise make_ex(ret, "Failed to operate read op for oid %s" % oid)

    def operate_aio_read_op(self, read_op: ReadOp, oid: str,
                            oncomplete: Optional[Callable[[Completion], None]] = None,
                            onsafe: Optional[Callable[[Completion], None]] = None,
                            flag: int = LIBRADOS_OPERATION_NOFLAG) -> Completion:
        """
        execute the real read operation
        :para read_op: read operation object
        :para oid: object name
        :param oncomplete: what to do when the remove is safe and complete in memory
            on all replicas
        :param onsafe:  what to do when the remove is safe and complete on storage
            on all replicas
        :para flag: flags to apply to the entire operation
        """
        oid_raw = cstr(oid, 'oid')
        cdef:
            ReadOp _read_op = read_op
            char *_oid = oid_raw
            Completion completion
            int _flag = flag

        completion = self.__get_completion(oncomplete, onsafe)
        self.__track_completion(completion)

        with nogil:
            ret = rados_aio_read_op_operate(_read_op.read_op, self.io, completion.rados_comp, _oid, _flag)
        if ret != 0:
            completion._cleanup()
            raise make_ex(ret, "Failed to operate aio read op for oid %s" % oid)
        return completion

    def get_omap_vals(self,
                      read_op: ReadOp,
                      start_after: str,
                      filter_prefix: str,
                      max_return: int) -> Tuple[OmapIterator, int]:
        """
        get the omap values
        :para read_op: read operation object
        :para start_after: list keys starting after start_after
        :para filter_prefix: list only keys beginning with filter_prefix
        :para max_return: list no more than max_return key/value pairs
        :returns: an iterator over the requested omap values, return value from this action
        """

        start_after_raw = cstr(start_after, 'start_after') if start_after else None
        filter_prefix_raw = cstr(filter_prefix, 'filter_prefix') if filter_prefix else None
        cdef:
            char *_start_after = opt_str(start_after_raw)
            char *_filter_prefix = opt_str(filter_prefix_raw)
            ReadOp _read_op = read_op
            rados_omap_iter_t iter_addr = NULL
            int _max_return = max_return

        with nogil:
            rados_read_op_omap_get_vals2(_read_op.read_op, _start_after, _filter_prefix,
                                         _max_return, &iter_addr, NULL, NULL)
        it = OmapIterator(self)
        it.ctx = iter_addr
        return it, 0   # 0 is meaningless; there for backward-compat

    def get_omap_keys(self, read_op: ReadOp, start_after: str, max_return: int) -> Tuple[OmapIterator, int]:
        """
        get the omap keys
        :para read_op: read operation object
        :para start_after: list keys starting after start_after
        :para max_return: list no more than max_return key/value pairs
        :returns: an iterator over the requested omap values, return value from this action
        """
        start_after_raw = cstr(start_after, 'start_after') if start_after else None
        cdef:
            char *_start_after = opt_str(start_after_raw)
            ReadOp _read_op = read_op
            rados_omap_iter_t iter_addr = NULL
            int _max_return = max_return

        with nogil:
            rados_read_op_omap_get_keys2(_read_op.read_op, _start_after,
                                         _max_return, &iter_addr, NULL, NULL)
        it = OmapIterator(self)
        it.ctx = iter_addr
        return it, 0   # 0 is meaningless; there for backward-compat

    def get_omap_vals_by_keys(self, read_op: ReadOp, keys: Sequence[str]) -> Tuple[OmapIterator, int]:
        """
        get the omap values by keys
        :para read_op: read operation object
        :para keys: input key tuple
        :returns: an iterator over the requested omap values, return value from this action
        """
        keys = cstr_list(keys, 'keys')
        cdef:
            ReadOp _read_op = read_op
            rados_omap_iter_t iter_addr
            char **_keys = to_bytes_array(keys)
            size_t key_num = len(keys)

        try:
            with nogil:
                rados_read_op_omap_get_vals_by_keys(_read_op.read_op,
                                                    <const char**>_keys,
                                                    key_num, &iter_addr, NULL)
            it = OmapIterator(self)
            it.ctx = iter_addr
            return it, 0   # 0 is meaningless; there for backward-compat
        finally:
            free(_keys)

    def remove_omap_keys(self, write_op: WriteOp, keys: Sequence[str]):
        """
        remove omap keys specifiled
        :para write_op: write operation object
        :para keys: input key tuple
        """

        keys = cstr_list(keys, 'keys')
        cdef:
            WriteOp _write_op = write_op
            size_t key_num = len(keys)
            char **_keys = to_bytes_array(keys)

        try:
            with nogil:
                rados_write_op_omap_rm_keys(_write_op.write_op, <const char**>_keys, key_num)
        finally:
            free(_keys)

    def clear_omap(self, write_op: WriteOp):
        """
        Remove all key/value pairs from an object
        :para write_op: write operation object
        """

        cdef:
            WriteOp _write_op = write_op

        with nogil:
            rados_write_op_omap_clear(_write_op.write_op)

    def remove_omap_range2(self, write_op: WriteOp, key_begin: str, key_end: str):
        """
        Remove key/value pairs from an object whose keys are in the range
        [key_begin, key_end)
        :param write_op: write operation object
        :param key_begin: the lower bound of the key range to remove
        :param key_end: the upper bound of the key range to remove
        """
        key_begin_raw = cstr(key_begin, 'key_begin')
        key_end_raw = cstr(key_end, 'key_end')
        cdef:
            WriteOp _write_op = write_op
            char* _key_begin = key_begin_raw
            size_t key_begin_len = len(key_begin)
            char* _key_end = key_end_raw
            size_t key_end_len = len(key_end)
        with nogil:
            rados_write_op_omap_rm_range2(_write_op.write_op, _key_begin, key_begin_len,
                                           _key_end, key_end_len)

    def lock_exclusive(self, key: str, name: str, cookie: str, desc: str = "",
                       duration: Optional[int] = None,
                       flags: int = 0):

        """
        Take an exclusive lock on an object

        :param key: name of the object
        :param name: name of the lock
        :param cookie: cookie of the lock
        :param desc: description of the lock
        :param duration: duration of the lock in seconds
        :param flags: flags

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        """
        self.require_ioctx_open()

        key_raw = cstr(key, 'key')
        name_raw = cstr(name, 'name')
        cookie_raw = cstr(cookie, 'cookie')
        desc_raw = cstr(desc, 'desc')

        cdef:
            char* _key = key_raw
            char* _name = name_raw
            char* _cookie = cookie_raw
            char* _desc = desc_raw
            uint8_t _flags = flags
            timeval _duration

        if duration is None:
            with nogil:
                ret = rados_lock_exclusive(self.io, _key, _name, _cookie, _desc,
                                           NULL, _flags)
        else:
            _duration.tv_sec = duration
            with nogil:
                ret = rados_lock_exclusive(self.io, _key, _name, _cookie, _desc,
                                           &_duration, _flags)

        if ret < 0:
            raise make_ex(ret, "Ioctx.rados_lock_exclusive(%s): failed to set lock %s on %s" % (self.name, name, key))

    def lock_shared(self, key: str, name: str, cookie: str, tag: str, desc: str = "",
                    duration: Optional[int] = None,
                    flags: int = 0):

        """
        Take a shared lock on an object

        :param key: name of the object
        :param name: name of the lock
        :param cookie: cookie of the lock
        :param tag: tag of the lock
        :param desc: description of the lock
        :param duration: duration of the lock in seconds
        :param flags: flags

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        """
        self.require_ioctx_open()

        key_raw = cstr(key, 'key')
        tag_raw = cstr(tag, 'tag')
        name_raw = cstr(name, 'name')
        cookie_raw = cstr(cookie, 'cookie')
        desc_raw = cstr(desc, 'desc')

        cdef:
            char* _key = key_raw
            char* _tag = tag_raw
            char* _name = name_raw
            char* _cookie = cookie_raw
            char* _desc = desc_raw
            uint8_t _flags = flags
            timeval _duration

        if duration is None:
            with nogil:
                ret = rados_lock_shared(self.io, _key, _name, _cookie, _tag, _desc,
                                        NULL, _flags)
        else:
            _duration.tv_sec = duration
            with nogil:
                ret = rados_lock_shared(self.io, _key, _name, _cookie, _tag, _desc,
                                        &_duration, _flags)
        if ret < 0:
            raise make_ex(ret, "Ioctx.rados_lock_exclusive(%s): failed to set lock %s on %s" % (self.name, name, key))

    def unlock(self, key: str, name: str, cookie: str):

        """
        Release a shared or exclusive lock on an object

        :param key: name of the object
        :param name: name of the lock
        :param cookie: cookie of the lock

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        """
        self.require_ioctx_open()

        key_raw = cstr(key, 'key')
        name_raw = cstr(name, 'name')
        cookie_raw = cstr(cookie, 'cookie')

        cdef:
            char* _key = key_raw
            char* _name = name_raw
            char* _cookie = cookie_raw

        with nogil:
            ret = rados_unlock(self.io, _key, _name, _cookie)
        if ret < 0:
            raise make_ex(ret, "Ioctx.rados_lock_exclusive(%s): failed to set lock %s on %s" % (self.name, name, key))

    def set_osdmap_full_try(self):
        """
        Set global osdmap_full_try label to true
        """
        with nogil:
            rados_set_pool_full_try(self.io)

    def unset_osdmap_full_try(self):
        """
        Unset
        """
        with nogil:
            rados_unset_pool_full_try(self.io)

    def application_enable(self, app_name: str, force: bool = False):
        """
        Enable an application on an OSD pool

        :param app_name: application name
        :type app_name: str
        :param force: False if only a single app should exist per pool
        :type expire_seconds: boool

        :raises: :class:`Error`
        """
        app_name_raw = cstr(app_name, 'app_name')
        cdef:
            char *_app_name = app_name_raw
            int _force = (1 if force else 0)

        with nogil:
            ret = rados_application_enable(self.io, _app_name, _force)
        if ret < 0:
            raise make_ex(ret, "error enabling application")

    def application_list(self) -> List[str]:
        """
        Returns a list of enabled applications

        :returns: list of app name string
        """
        cdef:
            size_t length = 128
            char *apps = NULL

        try:
            while True:
                apps = <char *>realloc_chk(apps, length)
                with nogil:
                    ret = rados_application_list(self.io, apps, &length)
                if ret == 0:
                    return [decode_cstr(app) for app in
                                apps[:length].split(b'\0') if app]
                elif ret == -errno.ENOENT:
                    return None
                elif ret == -errno.ERANGE:
                    pass
                else:
                    raise make_ex(ret, "error listing applications")
        finally:
            free(apps)

    def application_metadata_get(self, app_name: str, key: str) -> str:
        """
        Gets application metadata on an OSD pool for the given key

        :param app_name: application name
        :type app_name: str
        :param key: metadata key
        :type key: str
        :returns: str - metadata value

        :raises: :class:`Error`
        """

        app_name_raw = cstr(app_name, 'app_name')
        key_raw = cstr(key, 'key')
        cdef:
            char *_app_name = app_name_raw
            char *_key = key_raw
            size_t size = 129
            char *value = NULL
            int ret
        try:
            while True:
                value = <char *>realloc_chk(value, size)
                with nogil:
                    ret = rados_application_metadata_get(self.io, _app_name,
                                                         _key, value, &size)
                if ret != -errno.ERANGE:
                    break
            if ret == -errno.ENOENT:
                raise KeyError('no metadata %s for application %s' % (key, _app_name))
            elif ret != 0:
                raise make_ex(ret, 'error getting metadata %s for application %s' %
                              (key, _app_name))
            return decode_cstr(value)
        finally:
            free(value)

    def application_metadata_set(self, app_name: str, key: str, value: str):
        """
        Sets application metadata on an OSD pool

        :param app_name: application name
        :type app_name: str
        :param key: metadata key
        :type key: str
        :param value: metadata value
        :type value: str

        :raises: :class:`Error`
        """
        app_name_raw = cstr(app_name, 'app_name')
        key_raw = cstr(key, 'key')
        value_raw = cstr(value, 'value')
        cdef:
            char *_app_name = app_name_raw
            char *_key = key_raw
            char *_value = value_raw

        with nogil:
            ret = rados_application_metadata_set(self.io, _app_name, _key,
                                                 _value)
        if ret < 0:
            raise make_ex(ret, "error setting application metadata")

    def application_metadata_remove(self, app_name: str, key: str):
        """
        Remove application metadata from an OSD pool

        :param app_name: application name
        :type app_name: str
        :param key: metadata key
        :type key: str

        :raises: :class:`Error`
        """
        app_name_raw = cstr(app_name, 'app_name')
        key_raw = cstr(key, 'key')
        cdef:
            char *_app_name = app_name_raw
            char *_key = key_raw

        with nogil:
            ret = rados_application_metadata_remove(self.io, _app_name, _key)
        if ret < 0:
            raise make_ex(ret, "error removing application metadata")

    def application_metadata_list(self, app_name: str) -> List[Tuple[str, str]]:
        """
        Returns a list of enabled applications

        :param app_name: application name
        :type app_name: str
        :returns: list of key/value tuples
        """
        app_name_raw = cstr(app_name, 'app_name')
        cdef:
            char *_app_name = app_name_raw
            size_t key_length = 128
            size_t val_length = 128
            char *c_keys = NULL
            char *c_vals = NULL

        try:
            while True:
                c_keys = <char *>realloc_chk(c_keys, key_length)
                c_vals = <char *>realloc_chk(c_vals, val_length)
                with nogil:
                    ret = rados_application_metadata_list(self.io, _app_name,
                                                          c_keys, &key_length,
                                                          c_vals, &val_length)
                if ret == 0:
                    keys = [decode_cstr(key) for key in
                                c_keys[:key_length].split(b'\0')]
                    vals = [decode_cstr(val) for val in
                                c_vals[:val_length].split(b'\0')]
                    return list(zip(keys, vals))[:-1]
                elif ret == -errno.ERANGE:
                    pass
                else:
                    raise make_ex(ret, "error listing application metadata")
        finally:
            free(c_keys)
            free(c_vals)

    def alignment(self) -> int:
        """
        Returns pool alignment

        :returns:
            Number of alignment bytes required by the current pool, or None if
            alignment is not required.
        """
        cdef:
            int requires = 0
            uint64_t _alignment

        with nogil:
            ret = rados_ioctx_pool_requires_alignment2(self.io, &requires)
        if ret != 0:
            raise make_ex(ret, "error checking alignment")

        alignment = None
        if requires:
            with nogil:
                ret = rados_ioctx_pool_required_alignment2(self.io, &_alignment)
            if ret != 0:
                raise make_ex(ret, "error querying alignment")
            alignment = _alignment
        return alignment


def set_object_locator(func):
    def retfunc(self, *args, **kwargs):
        if self.locator_key is not None:
            old_locator = self.ioctx.get_locator_key()
            self.ioctx.set_locator_key(self.locator_key)
            retval = func(self, *args, **kwargs)
            self.ioctx.set_locator_key(old_locator)
            return retval
        else:
            return func(self, *args, **kwargs)
    return retfunc


def set_object_namespace(func):
    def retfunc(self, *args, **kwargs):
        if self.nspace is None:
            raise LogicError("Namespace not set properly in context")
        old_nspace = self.ioctx.get_namespace()
        self.ioctx.set_namespace(self.nspace)
        retval = func(self, *args, **kwargs)
        self.ioctx.set_namespace(old_nspace)
        return retval
    return retfunc


class Object(object):
    """Rados object wrapper, makes the object look like a file"""
    def __init__(self, ioctx, key, locator_key=None, nspace=None):
        self.key = key
        self.ioctx = ioctx
        self.offset = 0
        self.state = "exists"
        self.locator_key = locator_key
        self.nspace = "" if nspace is None else nspace

    def __str__(self):
        return "rados.Object(ioctx=%s,key=%s,nspace=%s,locator=%s)" % \
            (str(self.ioctx), self.key, "--default--"
             if self.nspace is "" else self.nspace, self.locator_key)

    def require_object_exists(self):
        if self.state != "exists":
            raise ObjectStateError("The object is %s" % self.state)

    @set_object_locator
    @set_object_namespace
    def read(self, length=1024 * 1024):
        self.require_object_exists()
        ret = self.ioctx.read(self.key, length, self.offset)
        self.offset += len(ret)
        return ret

    @set_object_locator
    @set_object_namespace
    def write(self, string_to_write):
        self.require_object_exists()
        ret = self.ioctx.write(self.key, string_to_write, self.offset)
        if ret == 0:
            self.offset += len(string_to_write)
        return ret

    @set_object_locator
    @set_object_namespace
    def remove(self):
        self.require_object_exists()
        self.ioctx.remove_object(self.key)
        self.state = "removed"

    @set_object_locator
    @set_object_namespace
    def stat(self) -> Tuple[int, time.struct_time]:
        self.require_object_exists()
        return self.ioctx.stat(self.key)

    def seek(self, position: int):
        self.require_object_exists()
        self.offset = position

    @set_object_locator
    @set_object_namespace
    def get_xattr(self, xattr_name: str) -> bytes:
        self.require_object_exists()
        return self.ioctx.get_xattr(self.key, xattr_name)

    @set_object_locator
    @set_object_namespace
    def get_xattrs(self) -> XattrIterator:
        self.require_object_exists()
        return self.ioctx.get_xattrs(self.key)

    @set_object_locator
    @set_object_namespace
    def set_xattr(self, xattr_name: str, xattr_value: bytes) -> bool:
        self.require_object_exists()
        return self.ioctx.set_xattr(self.key, xattr_name, xattr_value)

    @set_object_locator
    @set_object_namespace
    def rm_xattr(self, xattr_name: str) -> bool:
        self.require_object_exists()
        return self.ioctx.rm_xattr(self.key, xattr_name)

MONITOR_LEVELS = [
    "debug",
    "info",
    "warn", "warning",
    "err", "error",
    "sec",
    ]


class MonitorLog(object):
    # NOTE(sileht): Keep this class for backward compat
    # method moved to Rados.monitor_log()
    """
    For watching cluster log messages.  Instantiate an object and keep
    it around while callback is periodically called.  Construct with
    'level' to monitor 'level' messages (one of MONITOR_LEVELS).
    arg will be passed to the callback.

    callback will be called with:
        arg (given to __init__)
        line (the full line, including timestamp, who, level, msg)
        who (which entity issued the log message)
        timestamp_sec (sec of a struct timespec)
        timestamp_nsec (sec of a struct timespec)
        seq (sequence number)
        level (string representing the level of the log message)
        msg (the message itself)
    callback's return value is ignored
    """
    def __init__(self, cluster, level, callback, arg):
        self.level = level
        self.callback = callback
        self.arg = arg
        self.cluster = cluster
        self.cluster.monitor_log(level, callback, arg)

