# cython: embedsignature=True
"""
This module is a thin wrapper around librbd.

It currently provides all the synchronous methods of librbd that do
not use callbacks.

Error codes from librbd are turned into exceptions that subclass
:class:`Error`. Almost all methods may raise :class:`Error`
(the base class of all rbd exceptions), :class:`PermissionError`
and :class:`IOError`, in addition to those documented for the
method.
"""
# Copyright 2011 Josh Durgin
# Copyright 2015 Hector Martin <marcan@marcan.st>

import cython
import json
import sys

from cpython cimport PyObject, ref, exc
from libc cimport errno
from libc.stdint cimport *
from libc.stdlib cimport malloc, realloc, free
from libc.string cimport strdup, memset
cimport libcpp

try:
    from collections.abc import Iterable
except ImportError:
    from collections import Iterable
from datetime import datetime
import errno
from itertools import chain
import time

IF BUILD_DOC:
    include "mock_rbd.pxi"
ELSE:
    from c_rbd cimport *
    cimport rados


cdef extern from "Python.h":
    # These are in cpython/string.pxd, but use "object" types instead of
    # PyObject*, which invokes assumptions in cpython that we need to
    # legitimately break to implement zero-copy string buffers in Image.read().
    # This is valid use of the Python API and documented as a special case.
    PyObject *PyBytes_FromStringAndSize(char *v, Py_ssize_t len) except NULL
    char* PyBytes_AsString(PyObject *string) except NULL
    int _PyBytes_Resize(PyObject **string, Py_ssize_t newsize) except -1

cdef extern from "<errno.h>" nogil:
    enum:
        _ECANCELED "ECANCELED"


ECANCELED = _ECANCELED

RBD_FEATURE_LAYERING = _RBD_FEATURE_LAYERING
RBD_FEATURE_STRIPINGV2 = _RBD_FEATURE_STRIPINGV2
RBD_FEATURE_EXCLUSIVE_LOCK = _RBD_FEATURE_EXCLUSIVE_LOCK
RBD_FEATURE_OBJECT_MAP = _RBD_FEATURE_OBJECT_MAP
RBD_FEATURE_FAST_DIFF = _RBD_FEATURE_FAST_DIFF
RBD_FEATURE_DEEP_FLATTEN = _RBD_FEATURE_DEEP_FLATTEN
RBD_FEATURE_JOURNALING = _RBD_FEATURE_JOURNALING
RBD_FEATURE_DATA_POOL = _RBD_FEATURE_DATA_POOL
RBD_FEATURE_OPERATIONS = _RBD_FEATURE_OPERATIONS
RBD_FEATURE_MIGRATING = _RBD_FEATURE_MIGRATING
RBD_FEATURE_NON_PRIMARY = _RBD_FEATURE_NON_PRIMARY

RBD_FEATURES_INCOMPATIBLE = _RBD_FEATURES_INCOMPATIBLE
RBD_FEATURES_RW_INCOMPATIBLE = _RBD_FEATURES_RW_INCOMPATIBLE
RBD_FEATURES_MUTABLE = _RBD_FEATURES_MUTABLE
RBD_FEATURES_SINGLE_CLIENT = _RBD_FEATURES_SINGLE_CLIENT
RBD_FEATURES_ALL = _RBD_FEATURES_ALL

RBD_OPERATION_FEATURE_CLONE_PARENT = _RBD_OPERATION_FEATURE_CLONE_PARENT
RBD_OPERATION_FEATURE_CLONE_CHILD = _RBD_OPERATION_FEATURE_CLONE_CHILD
RBD_OPERATION_FEATURE_GROUP = _RBD_OPERATION_FEATURE_GROUP
RBD_OPERATION_FEATURE_SNAP_TRASH = _RBD_OPERATION_FEATURE_SNAP_TRASH

RBD_FLAG_OBJECT_MAP_INVALID = _RBD_FLAG_OBJECT_MAP_INVALID
RBD_FLAG_FAST_DIFF_INVALID = _RBD_FLAG_FAST_DIFF_INVALID

RBD_MIRROR_MODE_DISABLED = _RBD_MIRROR_MODE_DISABLED
RBD_MIRROR_MODE_IMAGE = _RBD_MIRROR_MODE_IMAGE
RBD_MIRROR_MODE_POOL = _RBD_MIRROR_MODE_POOL

RBD_MIRROR_PEER_DIRECTION_RX = _RBD_MIRROR_PEER_DIRECTION_RX
RBD_MIRROR_PEER_DIRECTION_TX = _RBD_MIRROR_PEER_DIRECTION_TX
RBD_MIRROR_PEER_DIRECTION_RX_TX = _RBD_MIRROR_PEER_DIRECTION_RX_TX

RBD_MIRROR_IMAGE_MODE_JOURNAL = _RBD_MIRROR_IMAGE_MODE_JOURNAL
RBD_MIRROR_IMAGE_MODE_SNAPSHOT = _RBD_MIRROR_IMAGE_MODE_SNAPSHOT

RBD_MIRROR_IMAGE_DISABLING = _RBD_MIRROR_IMAGE_DISABLING
RBD_MIRROR_IMAGE_ENABLED = _RBD_MIRROR_IMAGE_ENABLED
RBD_MIRROR_IMAGE_DISABLED = _RBD_MIRROR_IMAGE_DISABLED

MIRROR_IMAGE_STATUS_STATE_UNKNOWN = _MIRROR_IMAGE_STATUS_STATE_UNKNOWN
MIRROR_IMAGE_STATUS_STATE_ERROR = _MIRROR_IMAGE_STATUS_STATE_ERROR
MIRROR_IMAGE_STATUS_STATE_SYNCING = _MIRROR_IMAGE_STATUS_STATE_SYNCING
MIRROR_IMAGE_STATUS_STATE_STARTING_REPLAY = _MIRROR_IMAGE_STATUS_STATE_STARTING_REPLAY
MIRROR_IMAGE_STATUS_STATE_REPLAYING = _MIRROR_IMAGE_STATUS_STATE_REPLAYING
MIRROR_IMAGE_STATUS_STATE_STOPPING_REPLAY = _MIRROR_IMAGE_STATUS_STATE_STOPPING_REPLAY
MIRROR_IMAGE_STATUS_STATE_STOPPED = _MIRROR_IMAGE_STATUS_STATE_STOPPED

RBD_LOCK_MODE_EXCLUSIVE = _RBD_LOCK_MODE_EXCLUSIVE
RBD_LOCK_MODE_SHARED = _RBD_LOCK_MODE_SHARED

RBD_IMAGE_OPTION_FORMAT = _RBD_IMAGE_OPTION_FORMAT
RBD_IMAGE_OPTION_FEATURES = _RBD_IMAGE_OPTION_FEATURES
RBD_IMAGE_OPTION_ORDER = _RBD_IMAGE_OPTION_ORDER
RBD_IMAGE_OPTION_STRIPE_UNIT = _RBD_IMAGE_OPTION_STRIPE_UNIT
RBD_IMAGE_OPTION_STRIPE_COUNT = _RBD_IMAGE_OPTION_STRIPE_COUNT
RBD_IMAGE_OPTION_DATA_POOL = _RBD_IMAGE_OPTION_DATA_POOL
RBD_IMAGE_OPTION_FLATTEN = _RBD_IMAGE_OPTION_FLATTEN
RBD_IMAGE_OPTION_CLONE_FORMAT = _RBD_IMAGE_OPTION_CLONE_FORMAT

RBD_SNAP_NAMESPACE_TYPE_USER = _RBD_SNAP_NAMESPACE_TYPE_USER
RBD_SNAP_NAMESPACE_TYPE_GROUP = _RBD_SNAP_NAMESPACE_TYPE_GROUP
RBD_SNAP_NAMESPACE_TYPE_TRASH = _RBD_SNAP_NAMESPACE_TYPE_TRASH
RBD_SNAP_NAMESPACE_TYPE_MIRROR = _RBD_SNAP_NAMESPACE_TYPE_MIRROR

RBD_SNAP_MIRROR_STATE_PRIMARY = _RBD_SNAP_MIRROR_STATE_PRIMARY
RBD_SNAP_MIRROR_STATE_PRIMARY_DEMOTED = _RBD_SNAP_MIRROR_STATE_PRIMARY_DEMOTED
RBD_SNAP_MIRROR_STATE_NON_PRIMARY = _RBD_SNAP_MIRROR_STATE_NON_PRIMARY
RBD_SNAP_MIRROR_STATE_NON_PRIMARY_DEMOTED = _RBD_SNAP_MIRROR_STATE_NON_PRIMARY_DEMOTED

RBD_GROUP_IMAGE_STATE_ATTACHED = _RBD_GROUP_IMAGE_STATE_ATTACHED
RBD_GROUP_IMAGE_STATE_INCOMPLETE = _RBD_GROUP_IMAGE_STATE_INCOMPLETE

RBD_GROUP_SNAP_STATE_INCOMPLETE = _RBD_GROUP_SNAP_STATE_INCOMPLETE
RBD_GROUP_SNAP_STATE_COMPLETE = _RBD_GROUP_SNAP_STATE_COMPLETE

RBD_IMAGE_MIGRATION_STATE_UNKNOWN = _RBD_IMAGE_MIGRATION_STATE_UNKNOWN
RBD_IMAGE_MIGRATION_STATE_ERROR = _RBD_IMAGE_MIGRATION_STATE_ERROR
RBD_IMAGE_MIGRATION_STATE_PREPARING = _RBD_IMAGE_MIGRATION_STATE_PREPARING
RBD_IMAGE_MIGRATION_STATE_PREPARED = _RBD_IMAGE_MIGRATION_STATE_PREPARED
RBD_IMAGE_MIGRATION_STATE_EXECUTING = _RBD_IMAGE_MIGRATION_STATE_EXECUTING
RBD_IMAGE_MIGRATION_STATE_EXECUTED = _RBD_IMAGE_MIGRATION_STATE_EXECUTED
RBD_IMAGE_MIGRATION_STATE_ABORTING = _RBD_IMAGE_MIGRATION_STATE_ABORTING

RBD_CONFIG_SOURCE_CONFIG = _RBD_CONFIG_SOURCE_CONFIG
RBD_CONFIG_SOURCE_POOL = _RBD_CONFIG_SOURCE_POOL
RBD_CONFIG_SOURCE_IMAGE = _RBD_CONFIG_SOURCE_IMAGE

RBD_POOL_STAT_OPTION_IMAGES = _RBD_POOL_STAT_OPTION_IMAGES
RBD_POOL_STAT_OPTION_IMAGE_PROVISIONED_BYTES = _RBD_POOL_STAT_OPTION_IMAGE_PROVISIONED_BYTES
RBD_POOL_STAT_OPTION_IMAGE_MAX_PROVISIONED_BYTES = _RBD_POOL_STAT_OPTION_IMAGE_MAX_PROVISIONED_BYTES
RBD_POOL_STAT_OPTION_IMAGE_SNAPSHOTS = _RBD_POOL_STAT_OPTION_IMAGE_SNAPSHOTS
RBD_POOL_STAT_OPTION_TRASH_IMAGES = _RBD_POOL_STAT_OPTION_TRASH_IMAGES
RBD_POOL_STAT_OPTION_TRASH_PROVISIONED_BYTES = _RBD_POOL_STAT_OPTION_TRASH_PROVISIONED_BYTES
RBD_POOL_STAT_OPTION_TRASH_MAX_PROVISIONED_BYTES = _RBD_POOL_STAT_OPTION_TRASH_MAX_PROVISIONED_BYTES
RBD_POOL_STAT_OPTION_TRASH_SNAPSHOTS = _RBD_POOL_STAT_OPTION_TRASH_SNAPSHOTS

RBD_SNAP_CREATE_SKIP_QUIESCE = _RBD_SNAP_CREATE_SKIP_QUIESCE
RBD_SNAP_CREATE_IGNORE_QUIESCE_ERROR = _RBD_SNAP_CREATE_IGNORE_QUIESCE_ERROR

RBD_SNAP_REMOVE_UNPROTECT = _RBD_SNAP_REMOVE_UNPROTECT
RBD_SNAP_REMOVE_FLATTEN = _RBD_SNAP_REMOVE_FLATTEN
RBD_SNAP_REMOVE_FORCE = _RBD_SNAP_REMOVE_FORCE

RBD_ENCRYPTION_FORMAT_LUKS1 = _RBD_ENCRYPTION_FORMAT_LUKS1
RBD_ENCRYPTION_FORMAT_LUKS2 = _RBD_ENCRYPTION_FORMAT_LUKS2
RBD_ENCRYPTION_FORMAT_LUKS = _RBD_ENCRYPTION_FORMAT_LUKS
RBD_ENCRYPTION_ALGORITHM_AES128 = _RBD_ENCRYPTION_ALGORITHM_AES128
RBD_ENCRYPTION_ALGORITHM_AES256 = _RBD_ENCRYPTION_ALGORITHM_AES256

RBD_WRITE_ZEROES_FLAG_THICK_PROVISION = _RBD_WRITE_ZEROES_FLAG_THICK_PROVISION

class Error(Exception):
    pass


class OSError(Error):
    """ `OSError` class, derived from `Error` """
    def __init__(self, message, errno=None):
        super(OSError, self).__init__(message)
        self.errno = errno

    def __str__(self):
        msg = super(OSError, self).__str__()
        if self.errno is None:
            return msg
        return '[errno {0}] {1}'.format(self.errno, msg)

    def __reduce__(self):
        return (self.__class__, (self.message, self.errno))

class PermissionError(OSError):
    def __init__(self, message, errno=None):
        super(PermissionError, self).__init__(
                "RBD permission error (%s)" % message, errno)


class ImageNotFound(OSError):
    def __init__(self, message, errno=None):
        super(ImageNotFound, self).__init__(
                "RBD image not found (%s)" % message, errno)


class ObjectNotFound(OSError):
    def __init__(self, message, errno=None):
        super(ObjectNotFound, self).__init__(
                "RBD object not found (%s)" % message, errno)


class ImageExists(OSError):
    def __init__(self, message, errno=None):
        super(ImageExists, self).__init__(
                "RBD image already exists (%s)" % message, errno)


class ObjectExists(OSError):
    def __init__(self, message, errno=None):
        super(ObjectExists, self).__init__(
                "RBD object already exists (%s)" % message, errno)


class IOError(OSError):
    def __init__(self, message, errno=None):
        super(IOError, self).__init__(
                "RBD I/O error (%s)" % message, errno)


class NoSpace(OSError):
    def __init__(self, message, errno=None):
        super(NoSpace, self).__init__(
                "RBD insufficient space available (%s)" % message, errno)


class IncompleteWriteError(OSError):
    def __init__(self, message, errno=None):
        super(IncompleteWriteError, self).__init__(
               "RBD incomplete write (%s)" % message, errno)


class InvalidArgument(OSError):
    def __init__(self, message, errno=None):
        super(InvalidArgument, self).__init__(
                "RBD invalid argument (%s)" % message, errno)


class LogicError(OSError):
    def __init__(self, message, errno=None):
        super(LogicError, self).__init__(
                "RBD logic error (%s)" % message, errno)


class ReadOnlyImage(OSError):
    def __init__(self, message, errno=None):
        super(ReadOnlyImage, self).__init__(
                "RBD read-only image (%s)" % message, errno)


class ImageBusy(OSError):
    def __init__(self, message, errno=None):
        super(ImageBusy, self).__init__(
                "RBD image is busy (%s)" % message, errno)


class ImageHasSnapshots(OSError):
    def __init__(self, message, errno=None):
        super(ImageHasSnapshots, self).__init__(
                "RBD image has snapshots (%s)" % message, errno)


class FunctionNotSupported(OSError):
    def __init__(self, message, errno=None):
        super(FunctionNotSupported, self).__init__(
                "RBD function not supported (%s)" % message, errno)


class ArgumentOutOfRange(OSError):
    def __init__(self, message, errno=None):
        super(ArgumentOutOfRange, self).__init__(
                "RBD arguments out of range (%s)" % message, errno)


class ConnectionShutdown(OSError):
    def __init__(self, message, errno=None):
        super(ConnectionShutdown, self).__init__(
                "RBD connection was shutdown (%s)" % message, errno)


class Timeout(OSError):
    def __init__(self, message, errno=None):
        super(Timeout, self).__init__(
                "RBD operation timeout (%s)" % message, errno)


class DiskQuotaExceeded(OSError):
    def __init__(self, message, errno=None):
        super(DiskQuotaExceeded, self).__init__(
                "RBD disk quota exceeded (%s)" % message, errno)

class OperationNotSupported(OSError):
    def __init__(self, message, errno=None):
        super(OperationNotSupported, self).__init__(
                "RBD operation not supported (%s)" % message, errno)

class OperationCanceled(OSError):
    def __init__(self, message, errno=None):
        super(OperationCanceled, self).__init__(
                "RBD operation canceled (%s)" % message, errno)

cdef errno_to_exception = {
    errno.EPERM      : PermissionError,
    errno.ENOENT     : ImageNotFound,
    errno.EIO        : IOError,
    errno.ENOSPC     : NoSpace,
    errno.EEXIST     : ImageExists,
    errno.EINVAL     : InvalidArgument,
    errno.EROFS      : ReadOnlyImage,
    errno.EBUSY      : ImageBusy,
    errno.ENOTEMPTY  : ImageHasSnapshots,
    errno.ENOSYS     : FunctionNotSupported,
    errno.EDOM       : ArgumentOutOfRange,
    errno.ESHUTDOWN  : ConnectionShutdown,
    errno.ETIMEDOUT  : Timeout,
    errno.EDQUOT     : DiskQuotaExceeded,
    errno.EOPNOTSUPP : OperationNotSupported,
    ECANCELED        : OperationCanceled,
}

cdef group_errno_to_exception = {
    errno.EPERM      : PermissionError,
    errno.ENOENT     : ObjectNotFound,
    errno.EIO        : IOError,
    errno.ENOSPC     : NoSpace,
    errno.EEXIST     : ObjectExists,
    errno.EINVAL     : InvalidArgument,
    errno.EROFS      : ReadOnlyImage,
    errno.EBUSY      : ImageBusy,
    errno.ENOTEMPTY  : ImageHasSnapshots,
    errno.ENOSYS     : FunctionNotSupported,
    errno.EDOM       : ArgumentOutOfRange,
    errno.ESHUTDOWN  : ConnectionShutdown,
    errno.ETIMEDOUT  : Timeout,
    errno.EDQUOT     : DiskQuotaExceeded,
    errno.EOPNOTSUPP : OperationNotSupported,
    ECANCELED        : OperationCanceled,
}

cdef make_ex(ret, msg, exception_map=errno_to_exception):
    """
    Translate a librbd return code into an exception.

    :param ret: the return code
    :type ret: int
    :param msg: the error message to use
    :type msg: str
    :returns: a subclass of :class:`Error`
    """
    ret = abs(ret)
    if ret in exception_map:
        return exception_map[ret](msg, errno=ret)
    else:
        return OSError(msg, errno=ret)


IF BUILD_DOC:
    cdef rados_t convert_rados(rados) nogil:
        return <rados_t>0

    cdef rados_ioctx_t convert_ioctx(ioctx) nogil:
        return <rados_ioctx_t>0
ELSE:
    cdef rados_t convert_rados(rados.Rados rados) except? NULL:
        return <rados_t>rados.cluster

    cdef rados_ioctx_t convert_ioctx(rados.Ioctx ioctx) except? NULL:
        return <rados_ioctx_t>ioctx.io

cdef int progress_callback(uint64_t offset, uint64_t total, void* ptr) with gil:
    return (<object>ptr)(offset, total)

cdef int no_op_progress_callback(uint64_t offset, uint64_t total, void* ptr):
    return 0

def cstr(val, name, encoding="utf-8", opt=False):
    """
    Create a byte string from a Python string

    :param basestring val: Python string
    :param str name: Name of the string parameter, for exceptions
    :param str encoding: Encoding to use
    :param bool opt: If True, None is allowed
    :rtype: bytes
    :raises: :class:`InvalidArgument`
    """
    if opt and val is None:
        return None
    if isinstance(val, bytes):
        return val
    elif isinstance(val, str):
        return val.encode(encoding)
    else:
        raise InvalidArgument('%s must be a string' % name)

def decode_cstr(val, encoding="utf-8"):
    """
    Decode a byte string into a Python string.

    :param bytes val: byte string
    :rtype: str or None
    """
    if val is None:
        return None

    return val.decode(encoding)


cdef char* opt_str(s) except? NULL:
    if s is None:
        return NULL
    return s

cdef void* realloc_chk(void* ptr, size_t size) except NULL:
    cdef void *ret = realloc(ptr, size)
    if ret == NULL:
        raise MemoryError("realloc failed")
    return ret

RBD_MIRROR_PEER_ATTRIBUTE_NAME_MON_HOST = decode_cstr(_RBD_MIRROR_PEER_ATTRIBUTE_NAME_MON_HOST)
RBD_MIRROR_PEER_ATTRIBUTE_NAME_KEY = decode_cstr(_RBD_MIRROR_PEER_ATTRIBUTE_NAME_KEY)

cdef class Completion

cdef void __aio_complete_cb(rbd_completion_t completion, void *args) with gil:
    """
    Callback to oncomplete() for asynchronous operations
    """
    cdef Completion cb = <Completion>args
    cb._complete()


cdef class Completion(object):
    """completion object"""

    cdef:
        object image
        object oncomplete
        rbd_completion_t rbd_comp
        PyObject* buf
        bint persisted
        object exc_info

    def __cinit__(self, image, object oncomplete):
        self.oncomplete = oncomplete
        self.image = image
        self.persisted = False

    def is_complete(self):
        """
        Has an asynchronous operation completed?

        This does not imply that the callback has finished.

        :returns: True if the operation is completed
        """
        with nogil:
            ret = rbd_aio_is_complete(self.rbd_comp)
        return ret == 1

    def wait_for_complete_and_cb(self):
        """
        Wait for an asynchronous operation to complete

        This method waits for the callback to execute, if one was provided.
        It will also re-raise any exceptions raised by the callback. You
        should call this to "reap" asynchronous completions and ensure that
        any exceptions in the callbacks are handled, as an exception internal
        to this module may have occurred.
        """
        with nogil:
            rbd_aio_wait_for_complete(self.rbd_comp)

        if self.exc_info:
            raise self.exc_info[0], self.exc_info[1], self.exc_info[2]

    def get_return_value(self):
        """
        Get the return value of an asynchronous operation

        The return value is set when the operation is complete.

        :returns: int - return value of the operation
        """
        with nogil:
            ret = rbd_aio_get_return_value(self.rbd_comp)
        return ret

    def __dealloc__(self):
        """
        Release a completion

        This is automatically called when the completion object is freed.
        """
        ref.Py_XDECREF(self.buf)
        self.buf = NULL
        if self.rbd_comp != NULL:
            with nogil:
                rbd_aio_release(self.rbd_comp)
                self.rbd_comp = NULL

    cdef void _complete(self):
        try:
            self.__unpersist()
            if self.oncomplete:
                self.oncomplete(self)
        # In the event that something raises an exception during the next 2
        # lines of code, we will not be able to catch it, and this may result
        # in the app not noticing a failed callback. However, this should only
        # happen in extreme circumstances (OOM, etc.). KeyboardInterrupt
        # should not be a problem because the callback thread from librbd
        # ought to have SIGINT blocked.
        except:
            self.exc_info = sys.exc_info()

    cdef __persist(self):
        if self.oncomplete is not None and not self.persisted:
            # Increment our own reference count to make sure the completion
            # is not freed until the callback is called. The completion is
            # allowed to be freed if there is no callback.
            ref.Py_INCREF(self)
            self.persisted = True

    cdef __unpersist(self):
        if self.persisted:
            ref.Py_DECREF(self)
            self.persisted = False


class RBD(object):
    """
    This class wraps librbd CRUD functions.
    """
    def version(self):
        """
        Get the version number of the ``librbd`` C library.

        :returns: a tuple of ``(major, minor, extra)`` components of the
                  librbd version
        """
        cdef int major = 0
        cdef int minor = 0
        cdef int extra = 0
        rbd_version(&major, &minor, &extra)
        return (major, minor, extra)

    def create(self, ioctx, name, size, order=None, old_format=False,
               features=None, stripe_unit=None, stripe_count=None,
               data_pool=None):
        """
        Create an rbd image.

        :param ioctx: the context in which to create the image
        :type ioctx: :class:`rados.Ioctx`
        :param name: what the image is called
        :type name: str
        :param size: how big the image is in bytes
        :type size: int
        :param order: the image is split into (2**order) byte objects
        :type order: int
        :param old_format: whether to create an old-style image that
                           is accessible by old clients, but can't
                           use more advanced features like layering.
        :type old_format: bool
        :param features: bitmask of features to enable
        :type features: int
        :param stripe_unit: stripe unit in bytes (default None to let librbd decide)
        :type stripe_unit: int
        :param stripe_count: objects to stripe over before looping
        :type stripe_count: int
        :param data_pool: optional separate pool for data blocks
        :type data_pool: str
        :raises: :class:`ImageExists`
        :raises: :class:`TypeError`
        :raises: :class:`InvalidArgument`
        :raises: :class:`FunctionNotSupported`
        """
        name = cstr(name, 'name')
        data_pool = cstr(data_pool, 'data_pool', opt=True)
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            char *_name = name
            uint64_t _size = size
            int _order = 0
            rbd_image_options_t opts
        if order is not None:
            _order = order
        if old_format:
            if (features or
                ((stripe_unit is not None) and stripe_unit != 0) or
                ((stripe_count is not None) and stripe_count != 0) or
                data_pool):
                raise InvalidArgument('format 1 images do not support feature '
                                      'masks, non-default striping, nor data '
                                      'pool')
            with nogil:
                ret = rbd_create(_ioctx, _name, _size, &_order)
        else:
            rbd_image_options_create(&opts)
            try:
                rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_FORMAT,
                                             1 if old_format else 2)
                if features is not None:
                    rbd_image_options_set_uint64(opts,
                                                 RBD_IMAGE_OPTION_FEATURES,
                                                 features)
                if order is not None:
                    rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_ORDER,
                                                 _order)
                if stripe_unit is not None:
                    rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_STRIPE_UNIT,
                                                 stripe_unit)
                if stripe_count is not None:
                    rbd_image_options_set_uint64(opts,
                                                 RBD_IMAGE_OPTION_STRIPE_COUNT,
                                                 stripe_count)
                if data_pool is not None:
                    rbd_image_options_set_string(opts,
                                                 RBD_IMAGE_OPTION_DATA_POOL,
                                                 data_pool)
                with nogil:
                    ret = rbd_create4(_ioctx, _name, _size, opts)
            finally:
                rbd_image_options_destroy(opts)
        if ret < 0:
            raise make_ex(ret, 'error creating image')

    def clone(self, p_ioctx, p_name, p_snapname, c_ioctx, c_name,
              features=None, order=None, stripe_unit=None, stripe_count=None,
              data_pool=None, clone_format=None):
        """
        Clone a parent rbd snapshot into a COW sparse child.

        :param p_ioctx: the parent context that represents the parent snap
        :type ioctx: :class:`rados.Ioctx`
        :param p_name: the parent image name
        :type name: str
        :param p_snapname: the parent image snapshot name
        :type name: str
        :param c_ioctx: the child context that represents the new clone
        :type ioctx: :class:`rados.Ioctx`
        :param c_name: the clone (child) name
        :type name: str
        :param features: bitmask of features to enable; if set, must include layering
        :type features: int
        :param order: the image is split into (2**order) byte objects
        :type order: int
        :param stripe_unit: stripe unit in bytes (default None to let librbd decide)
        :type stripe_unit: int
        :param stripe_count: objects to stripe over before looping
        :type stripe_count: int
        :param data_pool: optional separate pool for data blocks
        :type data_pool: str
        :param clone_format: 1 (requires a protected snapshot), 2 (requires mimic+ clients)
        :type clone_format: int
        :raises: :class:`TypeError`
        :raises: :class:`InvalidArgument`
        :raises: :class:`ImageExists`
        :raises: :class:`FunctionNotSupported`
        :raises: :class:`ArgumentOutOfRange`
        """
        p_snapname = cstr(p_snapname, 'p_snapname')
        p_name = cstr(p_name, 'p_name')
        c_name = cstr(c_name, 'c_name')
        data_pool = cstr(data_pool, 'data_pool', opt=True)
        cdef:
            rados_ioctx_t _p_ioctx = convert_ioctx(p_ioctx)
            rados_ioctx_t _c_ioctx = convert_ioctx(c_ioctx)
            char *_p_name = p_name
            char *_p_snapname = p_snapname
            char *_c_name = c_name
            rbd_image_options_t opts

        rbd_image_options_create(&opts)
        try:
            if features is not None:
                rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_FEATURES,
                                             features)
            if order is not None:
                rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_ORDER,
                                             order)
            if stripe_unit is not None:
                rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_STRIPE_UNIT,
                                             stripe_unit)
            if stripe_count is not None:
                rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_STRIPE_COUNT,
                                             stripe_count)
            if data_pool is not None:
                rbd_image_options_set_string(opts, RBD_IMAGE_OPTION_DATA_POOL,
                                             data_pool)
            if clone_format is not None:
                rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_CLONE_FORMAT,
                                             clone_format)
            with nogil:
                ret = rbd_clone3(_p_ioctx, _p_name, _p_snapname,
                                 _c_ioctx, _c_name, opts)
        finally:
            rbd_image_options_destroy(opts)
        if ret < 0:
            raise make_ex(ret, 'error creating clone')

    def list(self, ioctx):
        """
        List image names.

        :param ioctx: determines which RADOS pool is read
        :type ioctx: :class:`rados.Ioctx`
        :returns: list -- a list of image names
        """
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            size_t size = 512
            char *c_names = NULL
        try:
            while True:
                c_names = <char *>realloc_chk(c_names, size)
                with nogil:
                    ret = rbd_list(_ioctx, c_names, &size)
                if ret >= 0:
                    break
                elif ret != -errno.ERANGE:
                    raise make_ex(ret, 'error listing images')
            return [decode_cstr(name) for name in c_names[:ret].split(b'\0')
                    if name]
        finally:
            free(c_names)

    def list2(self, ioctx):
        """
        Iterate over the images in the pool.

        :param ioctx: determines which RADOS pool the image is in
        :type ioctx: :class:`rados.Ioctx`
        :returns: :class:`ImageIterator`
        """
        return ImageIterator(ioctx)

    def remove(self, ioctx, name, on_progress=None):
        """
        Delete an RBD image. This may take a long time, since it does
        not return until every object that comprises the image has
        been deleted. Note that all snapshots must be deleted before
        the image can be removed. If there are snapshots left,
        :class:`ImageHasSnapshots` is raised. If the image is still
        open, or the watch from a crashed client has not expired,
        :class:`ImageBusy` is raised.

        :param ioctx: determines which RADOS pool the image is in
        :type ioctx: :class:`rados.Ioctx`
        :param name: the name of the image to remove
        :type name: str
        :param on_progress: optional progress callback function
        :type on_progress: callback function
        :raises: :class:`ImageNotFound`, :class:`ImageBusy`,
                 :class:`ImageHasSnapshots`
        """
        name = cstr(name, 'name')
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            char *_name = name
            librbd_progress_fn_t _prog_cb = &no_op_progress_callback
            void *_prog_arg = NULL
        if on_progress:
            _prog_cb = &progress_callback
            _prog_arg = <void *>on_progress
        with nogil:
            ret = rbd_remove_with_progress(_ioctx, _name, _prog_cb, _prog_arg)
        if ret != 0:
            raise make_ex(ret, 'error removing image')

    def rename(self, ioctx, src, dest):
        """
        Rename an RBD image.

        :param ioctx: determines which RADOS pool the image is in
        :type ioctx: :class:`rados.Ioctx`
        :param src: the current name of the image
        :type src: str
        :param dest: the new name of the image
        :type dest: str
        :raises: :class:`ImageNotFound`, :class:`ImageExists`
        """
        src = cstr(src, 'src')
        dest = cstr(dest, 'dest')
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            char *_src = src
            char *_dest = dest
        with nogil:
            ret = rbd_rename(_ioctx, _src, _dest)
        if ret != 0:
            raise make_ex(ret, 'error renaming image')

    def trash_move(self, ioctx, name, delay=0):
        """
        Move an RBD image to the trash.

        :param ioctx: determines which RADOS pool the image is in
        :type ioctx: :class:`rados.Ioctx`
        :param name: the name of the image to remove
        :type name: str
        :param delay: time delay in seconds before the image can be deleted
                      from trash
        :type delay: int
        :raises: :class:`ImageNotFound`
        """
        name = cstr(name, 'name')
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            char *_name = name
            uint64_t _delay = delay
        with nogil:
            ret = rbd_trash_move(_ioctx, _name, _delay)
        if ret != 0:
            raise make_ex(ret, 'error moving image to trash')

    def trash_purge(self, ioctx, expire_ts=None, threshold=-1):
        """
        Delete RBD images from trash in bulk.

        By default it removes images with deferment end time less than now.

        The timestamp is configurable, e.g. delete images that have expired a
        week ago.

        If the threshold is used it deletes images until X% pool usage is met.

        :param ioctx: determines which RADOS pool the image is in
        :type ioctx: :class:`rados.Ioctx`
        :param expire_ts: timestamp for images to be considered as expired (UTC)
        :type expire_ts: datetime
        :param threshold: percentage of pool usage to be met (0 to 1)
        :type threshold: float
        """
        if expire_ts:
            expire_epoch_ts = time.mktime(expire_ts.timetuple())
        else:
            expire_epoch_ts = 0

        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            time_t _expire_ts = expire_epoch_ts
            float _threshold = threshold
        with nogil:
            ret = rbd_trash_purge(_ioctx, _expire_ts, _threshold)
        if ret != 0:
            raise make_ex(ret, 'error purging images from trash')

    def trash_remove(self, ioctx, image_id, force=False, on_progress=None):
        """
        Delete an RBD image from trash. If image deferment time has not
        expired :class:`PermissionError` is raised.

        :param ioctx: determines which RADOS pool the image is in
        :type ioctx: :class:`rados.Ioctx`
        :param image_id: the id of the image to remove
        :type image_id: str
        :param force: force remove even if deferment time has not expired
        :type force: bool
        :param on_progress: optional progress callback function
        :type on_progress: callback function
        :raises: :class:`ImageNotFound`, :class:`PermissionError`
        """
        image_id = cstr(image_id, 'image_id')
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            char *_image_id = image_id
            int _force = force
            librbd_progress_fn_t _prog_cb = &no_op_progress_callback
            void *_prog_arg = NULL
        if on_progress:
            _prog_cb = &progress_callback
            _prog_arg = <void *>on_progress
        with nogil:
            ret = rbd_trash_remove_with_progress(_ioctx, _image_id, _force,
                                                 _prog_cb, _prog_arg)
        if ret != 0:
            raise make_ex(ret, 'error deleting image from trash')

    def trash_get(self, ioctx, image_id):
        """
        Retrieve RBD image info from trash.

        :param ioctx: determines which RADOS pool the image is in
        :type ioctx: :class:`rados.Ioctx`
        :param image_id: the id of the image to restore
        :type image_id: str
        :returns: dict - contains the following keys:

            * ``id`` (str) - image id

            * ``name`` (str) - image name

            * ``source`` (str) - source of deletion

            * ``deletion_time`` (datetime) - time of deletion

            * ``deferment_end_time`` (datetime) - time that an image is allowed
              to be removed from trash

        :raises: :class:`ImageNotFound`
        """
        image_id = cstr(image_id, 'image_id')
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            char *_image_id = image_id
            rbd_trash_image_info_t c_info
        with nogil:
            ret = rbd_trash_get(_ioctx, _image_id, &c_info)
        if ret != 0:
            raise make_ex(ret, 'error retrieving image from trash')

        __source_string = ['USER', 'MIRRORING', 'MIGRATION', 'REMOVING']
        info = {
            'id'          : decode_cstr(c_info.id),
            'name'        : decode_cstr(c_info.name),
            'source'      : __source_string[c_info.source],
            'deletion_time' : datetime.utcfromtimestamp(c_info.deletion_time),
            'deferment_end_time' : datetime.utcfromtimestamp(c_info.deferment_end_time)
            }
        rbd_trash_get_cleanup(&c_info)
        return info

    def trash_list(self, ioctx):
        """
        List all entries from trash.

        :param ioctx: determines which RADOS pool the image is in
        :type ioctx: :class:`rados.Ioctx`
        :returns: :class:`TrashIterator`
        """
        return TrashIterator(ioctx)

    def trash_restore(self, ioctx, image_id, name):
        """
        Restore an RBD image from trash.

        :param ioctx: determines which RADOS pool the image is in
        :type ioctx: :class:`rados.Ioctx`
        :param image_id: the id of the image to restore
        :type image_id: str
        :param name: the new name of the restored image
        :type name: str
        :raises: :class:`ImageNotFound`
        """
        image_id = cstr(image_id, 'image_id')
        name = cstr(name, 'name')
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            char *_image_id = image_id
            char *_name = name
        with nogil:
            ret = rbd_trash_restore(_ioctx, _image_id, _name)
        if ret != 0:
            raise make_ex(ret, 'error restoring image from trash')

    def migration_prepare(self, ioctx, image_name, dest_ioctx, dest_image_name,
                          features=None, order=None, stripe_unit=None, stripe_count=None,
                          data_pool=None, clone_format=None, flatten=False):
        """
        Prepare an RBD image migration.

        :param ioctx: determines which RADOS pool the image is in
        :type ioctx: :class:`rados.Ioctx`
        :param image_name: the current name of the image
        :type src: str
        :param dest_ioctx: determines which pool to migration into
        :type dest_ioctx: :class:`rados.Ioctx`
        :param dest_image_name: the name of the destination image (may be the same image)
        :type dest_image_name: str
        :param features: bitmask of features to enable; if set, must include layering
        :type features: int
        :param order: the image is split into (2**order) byte objects
        :type order: int
        :param stripe_unit: stripe unit in bytes (default None to let librbd decide)
        :type stripe_unit: int
        :param stripe_count: objects to stripe over before looping
        :type stripe_count: int
        :param data_pool: optional separate pool for data blocks
        :type data_pool: str
        :param clone_format: if the source image is a clone, which clone format
                             to use for the destination image
        :type clone_format: int
        :param flatten: if the source image is a clone, whether to flatten the
                        destination image or make it a clone of the same parent
        :type flatten: bool
        :raises: :class:`TypeError`
        :raises: :class:`InvalidArgument`
        :raises: :class:`ImageExists`
        :raises: :class:`FunctionNotSupported`
        :raises: :class:`ArgumentOutOfRange`
        """
        image_name = cstr(image_name, 'image_name')
        dest_image_name = cstr(dest_image_name, 'dest_image_name')
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            char *_image_name = image_name
            rados_ioctx_t _dest_ioctx = convert_ioctx(dest_ioctx)
            char *_dest_image_name = dest_image_name
            rbd_image_options_t opts

        rbd_image_options_create(&opts)
        try:
            if features is not None:
                rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_FEATURES,
                                             features)
            if order is not None:
                rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_ORDER,
                                             order)
            if stripe_unit is not None:
                rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_STRIPE_UNIT,
                                             stripe_unit)
            if stripe_count is not None:
                rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_STRIPE_COUNT,
                                             stripe_count)
            if data_pool is not None:
                rbd_image_options_set_string(opts, RBD_IMAGE_OPTION_DATA_POOL,
                                             data_pool)
            if clone_format is not None:
                rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_CLONE_FORMAT,
                                             clone_format)
            rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_FLATTEN, flatten)
            with nogil:
                ret = rbd_migration_prepare(_ioctx, _image_name, _dest_ioctx,
                                            _dest_image_name, opts)
        finally:
            rbd_image_options_destroy(opts)
        if ret < 0:
            raise make_ex(ret, 'error migrating image %s' % (image_name))

    def migration_prepare_import(self, source_spec, dest_ioctx, dest_image_name,
                          features=None, order=None, stripe_unit=None,
                          stripe_count=None, data_pool=None):
        """
        Prepare an RBD image migration.

        :param source_spec: JSON-encoded source-spec
        :type source_spec: str
        :param dest_ioctx: determines which pool to migration into
        :type dest_ioctx: :class:`rados.Ioctx`
        :param dest_image_name: the name of the destination image (may be the same image)
        :type dest_image_name: str
        :param features: bitmask of features to enable; if set, must include layering
        :type features: int
        :param order: the image is split into (2**order) byte objects
        :type order: int
        :param stripe_unit: stripe unit in bytes (default None to let librbd decide)
        :type stripe_unit: int
        :param stripe_count: objects to stripe over before looping
        :type stripe_count: int
        :param data_pool: optional separate pool for data blocks
        :type data_pool: str
        :raises: :class:`TypeError`
        :raises: :class:`InvalidArgument`
        :raises: :class:`ImageExists`
        :raises: :class:`FunctionNotSupported`
        :raises: :class:`ArgumentOutOfRange`
        """
        source_spec = cstr(source_spec, 'source_spec')
        dest_image_name = cstr(dest_image_name, 'dest_image_name')
        cdef:
            char *_source_spec = source_spec
            rados_ioctx_t _dest_ioctx = convert_ioctx(dest_ioctx)
            char *_dest_image_name = dest_image_name
            rbd_image_options_t opts

        rbd_image_options_create(&opts)
        try:
            if features is not None:
                rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_FEATURES,
                                             features)
            if order is not None:
                rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_ORDER,
                                             order)
            if stripe_unit is not None:
                rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_STRIPE_UNIT,
                                             stripe_unit)
            if stripe_count is not None:
                rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_STRIPE_COUNT,
                                             stripe_count)
            if data_pool is not None:
                rbd_image_options_set_string(opts, RBD_IMAGE_OPTION_DATA_POOL,
                                             data_pool)
            with nogil:
                ret = rbd_migration_prepare_import(_source_spec, _dest_ioctx,
                                                   _dest_image_name, opts)
        finally:
            rbd_image_options_destroy(opts)
        if ret < 0:
            raise make_ex(ret, 'error migrating image %s' % (source_spec))

    def migration_execute(self, ioctx, image_name, on_progress=None):
        """
        Execute a prepared RBD image migration.

        :param ioctx: determines which RADOS pool the image is in
        :type ioctx: :class:`rados.Ioctx`
        :param image_name: the name of the image
        :type image_name: str
        :param on_progress: optional progress callback function
        :type on_progress: callback function
        :raises: :class:`ImageNotFound`
        """
        image_name = cstr(image_name, 'image_name')
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            char *_image_name = image_name
            librbd_progress_fn_t _prog_cb = &no_op_progress_callback
            void *_prog_arg = NULL
        if on_progress:
            _prog_cb = &progress_callback
            _prog_arg = <void *>on_progress
        with nogil:
            ret = rbd_migration_execute_with_progress(_ioctx, _image_name,
                                                      _prog_cb, _prog_arg)
        if ret != 0:
            raise make_ex(ret, 'error aborting migration')

    def migration_commit(self, ioctx, image_name, on_progress=None):
        """
        Commit an executed RBD image migration.

        :param ioctx: determines which RADOS pool the image is in
        :type ioctx: :class:`rados.Ioctx`
        :param image_name: the name of the image
        :type image_name: str
        :param on_progress: optional progress callback function
        :type on_progress: callback function
        :raises: :class:`ImageNotFound`
        """
        image_name = cstr(image_name, 'image_name')
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            char *_image_name = image_name
            librbd_progress_fn_t _prog_cb = &no_op_progress_callback
            void *_prog_arg = NULL
        if on_progress:
            _prog_cb = &progress_callback
            _prog_arg = <void *>on_progress
        with nogil:
            ret = rbd_migration_commit_with_progress(_ioctx, _image_name,
                                                     _prog_cb, _prog_arg)
        if ret != 0:
            raise make_ex(ret, 'error aborting migration')

    def migration_abort(self, ioctx, image_name, on_progress=None):
        """
        Cancel a previously started but interrupted migration.

        :param ioctx: determines which RADOS pool the image is in
        :type ioctx: :class:`rados.Ioctx`
        :param image_name: the name of the image
        :type image_name: str
        :param on_progress: optional progress callback function
        :type on_progress: callback function
        :raises: :class:`ImageNotFound`
        """
        image_name = cstr(image_name, 'image_name')
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            char *_image_name = image_name
            librbd_progress_fn_t _prog_cb = &no_op_progress_callback
            void *_prog_arg = NULL
        if on_progress:
            _prog_cb = &progress_callback
            _prog_arg = <void *>on_progress
        with nogil:
            ret = rbd_migration_abort_with_progress(_ioctx, _image_name,
                                                    _prog_cb, _prog_arg)
        if ret != 0:
            raise make_ex(ret, 'error aborting migration')

    def migration_status(self, ioctx, image_name):
        """
        Return RBD image migration status.

        :param ioctx: determines which RADOS pool the image is in
        :type ioctx: :class:`rados.Ioctx`
        :param image_name: the name of the image
        :type image_name: str
        :returns: dict - contains the following keys:

            * ``source_pool_id`` (int) - source image pool id

            * ``source_pool_namespace`` (str) - source image pool namespace

            * ``source_image_name`` (str) - source image name

            * ``source_image_id`` (str) - source image id

            * ``dest_pool_id`` (int) - destination image pool id

            * ``dest_pool_namespace`` (str) - destination image pool namespace

            * ``dest_image_name`` (str) - destination image name

            * ``dest_image_id`` (str) - destination image id

            * ``state`` (int) - current migration state

            * ``state_description`` (str) - migration state description

        :raises: :class:`ImageNotFound`
        """
        image_name = cstr(image_name, 'image_name')
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            char *_image_name = image_name
            rbd_image_migration_status_t c_status
        with nogil:
            ret = rbd_migration_status(_ioctx, _image_name, &c_status,
                                       sizeof(c_status))
        if ret != 0:
            raise make_ex(ret, 'error getting migration status')

        status = {
            'source_pool_id'        : c_status.source_pool_id,
            'source_pool_namespace' : decode_cstr(c_status.source_pool_namespace),
            'source_image_name'     : decode_cstr(c_status.source_image_name),
            'source_image_id'       : decode_cstr(c_status.source_image_id),
            'dest_pool_id'          : c_status.source_pool_id,
            'dest_pool_namespace'   : decode_cstr(c_status.dest_pool_namespace),
            'dest_image_name'       : decode_cstr(c_status.dest_image_name),
            'dest_image_id'         : decode_cstr(c_status.dest_image_id),
            'state'                 : c_status.state,
            'state_description'     : decode_cstr(c_status.state_description)
         }

        rbd_migration_status_cleanup(&c_status)

        return status

    def mirror_site_name_get(self, rados):
        """
        Get the local cluster's friendly site name

        :param rados: cluster connection
        :type rados: :class: rados.Rados
        :returns: str - local site name
        """
        cdef:
            rados_t _rados = convert_rados(rados)
            char *_site_name = NULL
            size_t _max_size = 512
        try:
            while True:
                _site_name = <char *>realloc_chk(_site_name, _max_size)
                with nogil:
                    ret = rbd_mirror_site_name_get(_rados, _site_name,
                                                   &_max_size)
                if ret >= 0:
                    break
                elif ret != -errno.ERANGE:
                    raise make_ex(ret, 'error getting site name')
            return decode_cstr(_site_name)
        finally:
            free(_site_name)

    def mirror_site_name_set(self, rados, site_name):
        """
        Set the local cluster's friendly site name

        :param rados: cluster connection
        :type rados: :class: rados.Rados
        :param site_name: friendly site name
        :type str:
        """
        site_name = cstr(site_name, 'site_name')
        cdef:
            rados_t _rados = convert_rados(rados)
            char *_site_name = site_name
        with nogil:
            ret = rbd_mirror_site_name_set(_rados, _site_name)
        if ret != 0:
            raise make_ex(ret, 'error setting mirror site name')

    def mirror_mode_get(self, ioctx):
        """
        Get pool mirror mode.

        :param ioctx: determines which RADOS pool is read
        :type ioctx: :class:`rados.Ioctx`
        :returns: int - pool mirror mode
        """
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            rbd_mirror_mode_t mirror_mode
        with nogil:
            ret = rbd_mirror_mode_get(_ioctx, &mirror_mode)
        if ret != 0:
            raise make_ex(ret, 'error getting mirror mode')
        return mirror_mode

    def mirror_mode_set(self, ioctx, mirror_mode):
        """
        Set pool mirror mode.

        :param ioctx: determines which RADOS pool is written
        :type ioctx: :class:`rados.Ioctx`
        :param mirror_mode: mirror mode to set
        :type mirror_mode: int
        """
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            rbd_mirror_mode_t _mirror_mode = mirror_mode
        with nogil:
            ret = rbd_mirror_mode_set(_ioctx, _mirror_mode)
        if ret != 0:
            raise make_ex(ret, 'error setting mirror mode')

    def mirror_uuid_get(self, ioctx):
        """
        Get pool mirror uuid

        :param ioctx: determines which RADOS pool is read
        :type ioctx: :class:`rados.Ioctx`
        :returns: ste - pool mirror uuid
        """
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            char *_uuid = NULL
            size_t _max_size = 512
        try:
            while True:
                _uuid = <char *>realloc_chk(_uuid, _max_size)
                with nogil:
                    ret = rbd_mirror_uuid_get(_ioctx, _uuid, &_max_size)
                if ret >= 0:
                    break
                elif ret != -errno.ERANGE:
                    raise make_ex(ret, 'error retrieving mirror uuid')
            return decode_cstr(_uuid)
        finally:
            free(_uuid)

    def mirror_peer_bootstrap_create(self, ioctx):
        """
        Creates a new RBD mirroring bootstrap token for an
        external cluster.

        :param ioctx: determines which RADOS pool is written
        :type ioctx: :class:`rados.Ioctx`
        :returns: str - bootstrap token
        """
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            char *_token = NULL
            size_t _max_size = 512
        try:
            while True:
                _token = <char *>realloc_chk(_token, _max_size)
                with nogil:
                    ret = rbd_mirror_peer_bootstrap_create(_ioctx, _token,
                                                           &_max_size)
                if ret >= 0:
                    break
                elif ret != -errno.ERANGE:
                    raise make_ex(ret, 'error creating bootstrap token')
            return decode_cstr(_token)
        finally:
            free(_token)

    def mirror_peer_bootstrap_import(self, ioctx, direction, token):
        """
        Import a bootstrap token from an external cluster to
        auto-configure the mirror peer.

        :param ioctx: determines which RADOS pool is written
        :type ioctx: :class:`rados.Ioctx`
        :param direction: mirror peer direction
        :type direction: int
        :param token: bootstrap token
        :type token: str
        """
        token = cstr(token, 'token')
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            rbd_mirror_peer_direction_t _direction = direction
            char *_token = token
        with nogil:
            ret = rbd_mirror_peer_bootstrap_import(_ioctx, _direction, _token)
        if ret != 0:
            raise make_ex(ret, 'error importing bootstrap token')

    def mirror_peer_add(self, ioctx, site_name, client_name,
                        direction=RBD_MIRROR_PEER_DIRECTION_RX_TX):
        """
        Add mirror peer.

        :param ioctx: determines which RADOS pool is used
        :type ioctx: :class:`rados.Ioctx`
        :param site_name: mirror peer site name
        :type site_name: str
        :param client_name: mirror peer client name
        :type client_name: str
        :param direction: the direction of the mirroring
        :type direction: int
        :returns: str - peer uuid
        """
        site_name = cstr(site_name, 'site_name')
        client_name = cstr(client_name, 'client_name')
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            char *_uuid = NULL
            size_t _uuid_max_length = 512
            rbd_mirror_peer_direction_t _direction = direction
            char *_site_name = site_name
            char *_client_name = client_name
        try:
            _uuid = <char *>realloc_chk(_uuid, _uuid_max_length)
            with nogil:
                ret = rbd_mirror_peer_site_add(_ioctx, _uuid, _uuid_max_length,
                                               _direction, _site_name,
                                               _client_name)
            if ret != 0:
                raise make_ex(ret, 'error adding mirror peer')
            return decode_cstr(_uuid)
        finally:
            free(_uuid)

    def mirror_peer_remove(self, ioctx, uuid):
        """
        Remove mirror peer.

        :param ioctx: determines which RADOS pool is used
        :type ioctx: :class:`rados.Ioctx`
        :param uuid: peer uuid
        :type uuid: str
        """
        uuid = cstr(uuid, 'uuid')
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            char *_uuid = uuid
        with nogil:
            ret = rbd_mirror_peer_site_remove(_ioctx, _uuid)
        if ret != 0:
            raise make_ex(ret, 'error removing mirror peer')

    def mirror_peer_list(self, ioctx):
        """
        Iterate over the peers of a pool.

        :param ioctx: determines which RADOS pool is read
        :type ioctx: :class:`rados.Ioctx`
        :returns: :class:`MirrorPeerIterator`
        """
        return MirrorPeerIterator(ioctx)

    def mirror_peer_set_client(self, ioctx, uuid, client_name):
        """
        Set mirror peer client name

        :param ioctx: determines which RADOS pool is written
        :type ioctx: :class:`rados.Ioctx`
        :param uuid: uuid of the mirror peer
        :type uuid: str
        :param client_name: client name of the mirror peer to set
        :type client_name: str
        """
        uuid = cstr(uuid, 'uuid')
        client_name = cstr(client_name, 'client_name')
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            char *_uuid = uuid
            char *_client_name = client_name
        with nogil:
            ret = rbd_mirror_peer_site_set_client_name(_ioctx, _uuid,
                                                       _client_name)
        if ret != 0:
            raise make_ex(ret, 'error setting mirror peer client name')

    def mirror_peer_set_name(self, ioctx, uuid, site_name):
        """
        Set mirror peer site name

        :param ioctx: determines which RADOS pool is written
        :type ioctx: :class:`rados.Ioctx`
        :param uuid: uuid of the mirror peer
        :type uuid: str
        :param site_name: site name of the mirror peer to set
        :type site_name: str
        """
        uuid = cstr(uuid, 'uuid')
        site_name = cstr(site_name, 'site_name')
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            char *_uuid = uuid
            char *_site_name = site_name
        with nogil:
            ret = rbd_mirror_peer_site_set_name(_ioctx, _uuid, _site_name)
        if ret != 0:
            raise make_ex(ret, 'error setting mirror peer site name')

    def mirror_peer_set_cluster(self, ioctx, uuid, cluster_name):
        self.mirror_peer_set_name(ioctx, uuid, cluster_name)

    def mirror_peer_get_attributes(self, ioctx, uuid):
        """
        Get optional mirror peer attributes

        :param ioctx: determines which RADOS pool is written
        :type ioctx: :class:`rados.Ioctx`
        :param uuid: uuid of the mirror peer
        :type uuid: str

        :returns: dict - contains the following keys:

            * ``mon_host`` (str) - monitor addresses

            * ``key`` (str) - CephX key
        """
        uuid = cstr(uuid, 'uuid')
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            char *_uuid = uuid
            char *_keys = NULL
            char *_vals = NULL
            size_t _keys_size = 512
            size_t _vals_size = 512
            size_t _count = 0
        try:
            while True:
                _keys = <char *>realloc_chk(_keys, _keys_size)
                _vals = <char *>realloc_chk(_vals, _vals_size)
                with nogil:
                    ret = rbd_mirror_peer_site_get_attributes(
                        _ioctx, _uuid, _keys, &_keys_size, _vals, &_vals_size,
                        &_count)
                if ret >= 0:
                    break
                elif ret != -errno.ERANGE:
                    raise make_ex(ret, 'error getting mirror peer attributes')
            keys = [decode_cstr(x) for x in _keys[:_keys_size].split(b'\0')[:-1]]
            vals = [decode_cstr(x) for x in _vals[:_vals_size].split(b'\0')[:-1]]
            return dict(zip(keys, vals))
        finally:
            free(_keys)
            free(_vals)

    def mirror_peer_set_attributes(self, ioctx, uuid, attributes):
        """
        Set optional mirror peer attributes

        :param ioctx: determines which RADOS pool is written
        :type ioctx: :class:`rados.Ioctx`
        :param uuid: uuid of the mirror peer
        :type uuid: str
        :param attributes: 'mon_host' and 'key' attributes
        :type attributes: dict
        """
        uuid = cstr(uuid, 'uuid')
        keys_str = b'\0'.join([cstr(x[0], 'key') for x in attributes.items()])
        vals_str = b'\0'.join([cstr(x[1], 'val') for x in attributes.items()])
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            char *_uuid = uuid
            char *_keys = keys_str
            char *_vals = vals_str
            size_t _count = len(attributes)

        with nogil:
            ret = rbd_mirror_peer_site_set_attributes(_ioctx, _uuid, _keys,
                                                      _vals, _count)
        if ret != 0:
            raise make_ex(ret, 'error setting mirror peer attributes')

    def mirror_image_status_list(self, ioctx):
        """
        Iterate over the mirror image statuses of a pool.

        :param ioctx: determines which RADOS pool is read
        :type ioctx: :class:`rados.Ioctx`
        :returns: :class:`MirrorImageStatusIterator`
        """
        return MirrorImageStatusIterator(ioctx)

    def mirror_image_status_summary(self, ioctx):
        """
        Get mirror image status summary of a pool.

        :param ioctx: determines which RADOS pool is read
        :type ioctx: :class:`rados.Ioctx`
        :returns: list - a list of (state, count) tuples
        """
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            rbd_mirror_image_status_state_t *states = NULL
            int *counts = NULL
            size_t maxlen = 32
        try:
            states = <rbd_mirror_image_status_state_t *>realloc_chk(states,
                sizeof(rbd_mirror_image_status_state_t) * maxlen)
            counts = <int *>realloc_chk(counts, sizeof(int) * maxlen)
            with nogil:
                ret = rbd_mirror_image_status_summary(_ioctx, states, counts,
                                                      &maxlen)
            if ret < 0:
                raise make_ex(ret, 'error getting mirror image status summary')
            return [(states[i], counts[i]) for i in range(maxlen)]
        finally:
            free(states)
            free(counts)

    def mirror_image_instance_id_list(self, ioctx):
        """
        Iterate over the mirror image instance ids of a pool.

        :param ioctx: determines which RADOS pool is read
        :type ioctx: :class:`rados.Ioctx`
        :returns: :class:`MirrorImageInstanceIdIterator`
        """
        return MirrorImageInstanceIdIterator(ioctx)

    def mirror_image_info_list(self, ioctx, mode_filter=None):
        """
        Iterate over the mirror image instance ids of a pool.

        :param ioctx: determines which RADOS pool is read
        :param mode_filter: list images in this image mirror mode
        :type ioctx: :class:`rados.Ioctx`
        :returns: :class:`MirrorImageInfoIterator`
        """
        return MirrorImageInfoIterator(ioctx, mode_filter)

    def pool_metadata_get(self, ioctx, key):
        """
        Get pool metadata for the given key.

        :param ioctx: determines which RADOS pool is read
        :type ioctx: :class:`rados.Ioctx`
        :param key: metadata key
        :type key: str
        :returns: str - metadata value
        """
        key = cstr(key, 'key')
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            char *_key = key
            size_t size = 4096
            char *value = NULL
            int ret
        try:
            while True:
                value = <char *>realloc_chk(value, size)
                with nogil:
                    ret = rbd_pool_metadata_get(_ioctx, _key, value, &size)
                if ret != -errno.ERANGE:
                    break
            if ret == -errno.ENOENT:
                raise KeyError('no metadata %s' % (key))
            if ret != 0:
                raise make_ex(ret, 'error getting metadata %s' % (key))
            return decode_cstr(value)
        finally:
            free(value)

    def pool_metadata_set(self, ioctx, key, value):
        """
        Set pool metadata for the given key.

        :param ioctx: determines which RADOS pool is read
        :type ioctx: :class:`rados.Ioctx`
        :param key: metadata key
        :type key: str
        :param value: metadata value
        :type value: str
        """
        key = cstr(key, 'key')
        value = cstr(value, 'value')
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            char *_key = key
            char *_value = value
        with nogil:
            ret = rbd_pool_metadata_set(_ioctx, _key, _value)

        if ret != 0:
            raise make_ex(ret, 'error setting metadata %s' % (key))

    def pool_metadata_remove(self, ioctx, key):
        """
        Remove pool metadata for the given key.

        :param ioctx: determines which RADOS pool is read
        :type ioctx: :class:`rados.Ioctx`
        :param key: metadata key
        :type key: str
        :returns: str - metadata value
        """
        key = cstr(key, 'key')
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            char *_key = key
        with nogil:
            ret = rbd_pool_metadata_remove(_ioctx, _key)

        if ret == -errno.ENOENT:
            raise KeyError('no metadata %s' % (key))
        if ret != 0:
            raise make_ex(ret, 'error removing metadata %s' % (key))

    def pool_metadata_list(self, ioctx):
        """
        List pool metadata.

        :returns: :class:`PoolMetadataIterator`
        """
        return PoolMetadataIterator(ioctx)

    def config_list(self, ioctx):
        """
        List pool-level config overrides.

        :returns: :class:`ConfigPoolIterator`
        """
        return ConfigPoolIterator(ioctx)

    def config_get(self, ioctx, key):
        """
        Get a pool-level configuration override.

        :param ioctx: determines which RADOS pool is read
        :type ioctx: :class:`rados.Ioctx`
        :param key: key
        :type key: str
        :returns: str - value
        """
        conf_key = 'conf_' + key
        conf_key = cstr(conf_key, 'key')
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            char *_key = conf_key
            size_t size = 4096
            char *value = NULL
            int ret
        try:
            while True:
                value = <char *>realloc_chk(value, size)
                with nogil:
                    ret = rbd_pool_metadata_get(_ioctx, _key, value, &size)
                if ret != -errno.ERANGE:
                    break
            if ret == -errno.ENOENT:
                raise KeyError('no config %s for pool %s' % (key, ioctx.get_pool_name()))
            if ret != 0:
                raise make_ex(ret, 'error getting config %s for pool %s' %
                             (key, ioctx.get_pool_name()))
            return decode_cstr(value)
        finally:
            free(value)

    def config_set(self, ioctx, key, value):
        """
        Get a pool-level configuration override.

        :param ioctx: determines which RADOS pool is read
        :type ioctx: :class:`rados.Ioctx`
        :param key: key
        :type key: str
        :param value: value
        :type value: str
        """
        conf_key = 'conf_' + key
        conf_key = cstr(conf_key, 'key')
        value = cstr(value, 'value')
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            char *_key = conf_key
            char *_value = value
        with nogil:
            ret = rbd_pool_metadata_set(_ioctx, _key, _value)

        if ret != 0:
            raise make_ex(ret, 'error setting config %s for pool %s' %
                          (key, ioctx.get_pool_name()))

    def config_remove(self, ioctx, key):
        """
        Remove a pool-level configuration override.

        :param ioctx: determines which RADOS pool is read
        :type ioctx: :class:`rados.Ioctx`
        :param key: key
        :type key: str
        :returns: str - value
        """
        conf_key = 'conf_' + key
        conf_key = cstr(conf_key, 'key')
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            char *_key = conf_key
        with nogil:
            ret = rbd_pool_metadata_remove(_ioctx, _key)

        if ret == -errno.ENOENT:
            raise KeyError('no config %s for pool %s' %
                           (key, ioctx.get_pool_name()))
        if ret != 0:
            raise make_ex(ret, 'error removing config %s for pool %s' %
                          (key, ioctx.get_pool_name()))

    def group_create(self, ioctx, name):
        """
        Create a group.

        :param ioctx: determines which RADOS pool is used
        :type ioctx: :class:`rados.Ioctx`
        :param name: the name of the group
        :type name: str
        :raises: :class:`ObjectExists`
        :raises: :class:`InvalidArgument`
        :raises: :class:`FunctionNotSupported`
        """
        name = cstr(name, 'name')
        cdef:
            char *_name = name
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
        with nogil:
            ret = rbd_group_create(_ioctx, _name)
        if ret != 0:
            raise make_ex(ret, 'error creating group %s' % name, group_errno_to_exception)

    def group_remove(self, ioctx, name):
        """
        Delete an RBD group. This may take a long time, since it does
        not return until every image in the group has been removed
        from the group.

        :param ioctx: determines which RADOS pool the group is in
        :type ioctx: :class:`rados.Ioctx`
        :param name: the name of the group to remove
        :type name: str
        :raises: :class:`ObjectNotFound`
        :raises: :class:`InvalidArgument`
        :raises: :class:`FunctionNotSupported`
        """
        name = cstr(name, 'name')
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            char *_name = name
        with nogil:
            ret = rbd_group_remove(_ioctx, _name)
        if ret != 0:
            raise make_ex(ret, 'error removing group', group_errno_to_exception)

    def group_list(self, ioctx):
        """
        List groups.

        :param ioctx: determines which RADOS pool is read
        :type ioctx: :class:`rados.Ioctx`
        :returns: list -- a list of groups names
        :raises: :class:`FunctionNotSupported`
        """
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            size_t size = 512
            char *c_names = NULL
        try:
            while True:
                c_names = <char *>realloc_chk(c_names, size)
                with nogil:
                    ret = rbd_group_list(_ioctx, c_names, &size)
                if ret >= 0:
                    break
                elif ret != -errno.ERANGE:
                    raise make_ex(ret, 'error listing groups', group_errno_to_exception)
            return [decode_cstr(name) for name in c_names[:ret].split(b'\0')
                    if name]
        finally:
            free(c_names)

    def group_rename(self, ioctx, src, dest):
        """
        Rename an RBD group.

        :param ioctx: determines which RADOS pool the group is in
        :type ioctx: :class:`rados.Ioctx`
        :param src: the current name of the group
        :type src: str
        :param dest: the new name of the group
        :type dest: str
        :raises: :class:`ObjectExists`
        :raises: :class:`ObjectNotFound`
        :raises: :class:`InvalidArgument`
        :raises: :class:`FunctionNotSupported`
        """
        src = cstr(src, 'src')
        dest = cstr(dest, 'dest')
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            char *_src = src
            char *_dest = dest
        with nogil:
            ret = rbd_group_rename(_ioctx, _src, _dest)
        if ret != 0:
            raise make_ex(ret, 'error renaming group')

    def namespace_create(self, ioctx, name):
        """
        Create an RBD namespace within a pool

        :param ioctx: determines which RADOS pool
        :type ioctx: :class:`rados.Ioctx`
        :param name: namespace name
        :type name: str
        """
        name = cstr(name, 'name')
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            const char *_name = name
        with nogil:
            ret = rbd_namespace_create(_ioctx, _name)
        if ret != 0:
            raise make_ex(ret, 'error creating namespace')

    def namespace_remove(self, ioctx, name):
        """
        Remove an RBD namespace from a pool

        :param ioctx: determines which RADOS pool
        :type ioctx: :class:`rados.Ioctx`
        :param name: namespace name
        :type name: str
        """
        name = cstr(name, 'name')
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            const char *_name = name
        with nogil:
            ret = rbd_namespace_remove(_ioctx, _name)
        if ret != 0:
            raise make_ex(ret, 'error removing namespace')

    def namespace_exists(self, ioctx, name):
        """
        Verifies if a namespace exists within a pool

        :param ioctx: determines which RADOS pool
        :type ioctx: :class:`rados.Ioctx`
        :param name: namespace name
        :type name: str
        :returns: bool - true if namespace exists
        """
        name = cstr(name, 'name')
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            const char *_name = name
            libcpp.bool _exists = False
        with nogil:
            ret = rbd_namespace_exists(_ioctx, _name, &_exists)
        if ret != 0:
            raise make_ex(ret, 'error verifying namespace')
        return _exists

    def namespace_list(self, ioctx):
        """
        List all namespaces within a pool

        :param ioctx: determines which RADOS pool
        :type ioctx: :class:`rados.Ioctx`
        :returns: list - collection of namespace names
        """
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            char *_names = NULL
            size_t _size = 512
        try:
            while True:
                _names = <char *>realloc_chk(_names, _size)
                with nogil:
                    ret = rbd_namespace_list(_ioctx, _names, &_size)
                if ret >= 0:
                    break
                elif ret != -errno.ERANGE:
                    raise make_ex(ret, 'error listing namespaces')
            return [decode_cstr(name) for name in _names[:_size].split(b'\0')
                    if name]
        finally:
            free(_names)

    def pool_init(self, ioctx, force):
        """
        Initialize an RBD pool
        :param ioctx: determines which RADOS pool
        :type ioctx: :class:`rados.Ioctx`
        :param force: force init
        :type force: bool
        """
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            bint _force = force
        with nogil:
            ret = rbd_pool_init(_ioctx, _force)
        if ret != 0:
            raise make_ex(ret, 'error initializing pool')

    def pool_stats_get(self, ioctx):
        """
        Return RBD pool stats

        :param ioctx: determines which RADOS pool
        :type ioctx: :class:`rados.Ioctx`
        :returns: dict - contains the following keys:

            * ``image_count`` (int) - image count

            * ``image_provisioned_bytes`` (int) - image total HEAD provisioned bytes

            * ``image_max_provisioned_bytes`` (int) - image total max provisioned bytes

            * ``image_snap_count`` (int) - image snap count

            * ``trash_count`` (int) - trash image count

            * ``trash_provisioned_bytes`` (int) - trash total HEAD provisioned bytes

            * ``trash_max_provisioned_bytes`` (int) - trash total max provisioned bytes

            * ``trash_snap_count`` (int) - trash snap count

        """
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            uint64_t _image_count = 0
            uint64_t _image_provisioned_bytes = 0
            uint64_t _image_max_provisioned_bytes = 0
            uint64_t _image_snap_count = 0
            uint64_t _trash_count = 0
            uint64_t _trash_provisioned_bytes = 0
            uint64_t _trash_max_provisioned_bytes = 0
            uint64_t _trash_snap_count = 0
            rbd_pool_stats_t _stats

        rbd_pool_stats_create(&_stats)
        rbd_pool_stats_option_add_uint64(_stats, RBD_POOL_STAT_OPTION_IMAGES,
                                         &_image_count)
        rbd_pool_stats_option_add_uint64(_stats, RBD_POOL_STAT_OPTION_IMAGE_PROVISIONED_BYTES,
                                         &_image_provisioned_bytes)
        rbd_pool_stats_option_add_uint64(_stats, RBD_POOL_STAT_OPTION_IMAGE_MAX_PROVISIONED_BYTES,
                                         &_image_max_provisioned_bytes)
        rbd_pool_stats_option_add_uint64(_stats, RBD_POOL_STAT_OPTION_IMAGE_SNAPSHOTS,
                                         &_image_snap_count)
        rbd_pool_stats_option_add_uint64(_stats, RBD_POOL_STAT_OPTION_TRASH_IMAGES,
                                         &_trash_count)
        rbd_pool_stats_option_add_uint64(_stats, RBD_POOL_STAT_OPTION_TRASH_PROVISIONED_BYTES,
                                         &_trash_provisioned_bytes)
        rbd_pool_stats_option_add_uint64(_stats, RBD_POOL_STAT_OPTION_TRASH_MAX_PROVISIONED_BYTES,
                                         &_trash_max_provisioned_bytes)
        rbd_pool_stats_option_add_uint64(_stats, RBD_POOL_STAT_OPTION_TRASH_SNAPSHOTS,
                                         &_trash_snap_count)
        try:
            with nogil:
                ret = rbd_pool_stats_get(_ioctx, _stats)
            if ret != 0:
                raise make_ex(ret, 'error retrieving pool stats')
        else:
            return {'image_count': _image_count,
                    'image_provisioned_bytes': _image_provisioned_bytes,
                    'image_max_provisioned_bytes': _image_max_provisioned_bytes,
                    'image_snap_count': _image_snap_count,
                    'trash_count': _trash_count,
                    'trash_provisioned_bytes': _trash_provisioned_bytes,
                    'trash_max_provisioned_bytes': _trash_max_provisioned_bytes,
                    'trash_snap_count': _trash_snap_count}
        finally:
            rbd_pool_stats_destroy(_stats)

    def features_to_string(self, features):
        """
        Convert features bitmask to str.

        :param features: feature bitmask
        :type features: int
        :returns: str - the features str of the image
        :raises: :class:`InvalidArgument`
        """
        cdef:
            int ret = -errno.ERANGE
            uint64_t _features = features
            size_t size = 1024
            char *str_features = NULL
        try:
            while ret == -errno.ERANGE:
                str_features =  <char *>realloc_chk(str_features, size)
                with nogil:
                    ret = rbd_features_to_string(_features, str_features, &size)

            if ret != 0:
                raise make_ex(ret, 'error converting features bitmask to str')
            return decode_cstr(str_features)
        finally:
            free(str_features)

    def features_from_string(self, str_features):
        """
        Get features bitmask from str, if str_features is empty, it will return
        RBD_FEATURES_DEFAULT.

        :param str_features: feature str
        :type str_features: str
        :returns: int - the features bitmask of the image
        :raises: :class:`InvalidArgument`
        """
        str_features = cstr(str_features, 'str_features')
        cdef:
            const char *_str_features = str_features
            uint64_t features
        with nogil:
            ret = rbd_features_from_string(_str_features, &features)
        if ret != 0:
            raise make_ex(ret, 'error getting features bitmask from str')
        return features

    def aio_open_image(self, oncomplete, ioctx, name=None, snapshot=None,
                       read_only=False, image_id=None):
        """
        Asynchronously open the image at the given snapshot.
        Specify either name or id, otherwise :class:`InvalidArgument` is raised.

        oncomplete will be called with the created Image object as
        well as the completion:

        oncomplete(completion, image)

        If a snapshot is specified, the image will be read-only, unless
        :func:`Image.set_snap` is called later.

        If read-only mode is used, metadata for the :class:`Image`
        object (such as which snapshots exist) may become obsolete. See
        the C api for more details.

        To clean up from opening the image, :func:`Image.close` or
        :func:`Image.aio_close` should be called.

        :param oncomplete: what to do when open is complete
        :type oncomplete: completion
        :param ioctx: determines which RADOS pool the image is in
        :type ioctx: :class:`rados.Ioctx`
        :param name: the name of the image
        :type name: str
        :param snapshot: which snapshot to read from
        :type snapshot: str
        :param read_only: whether to open the image in read-only mode
        :type read_only: bool
        :param image_id: the id of the image
        :type image_id: str
        :returns: :class:`Completion` - the completion object
        """

        image = Image(ioctx, name, snapshot, read_only, image_id, oncomplete)
        comp, image._open_completion = image._open_completion, None
        return comp

cdef class MirrorPeerIterator(object):
    """
    Iterator over mirror peer info for a pool.

    Yields a dictionary containing information about a peer.

    Keys are:

    * ``uuid`` (str) - uuid of the peer

    * ``direction`` (int) - direction enum

    * ``site_name`` (str) - cluster name of the peer

    * ``mirror_uuid`` (str) - mirror uuid of the peer

    * ``client_name`` (str) - client name of the peer
    """

    cdef:
        rbd_mirror_peer_site_t *peers
        int num_peers

    def __init__(self, ioctx):
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
        self.peers = NULL
        self.num_peers = 10
        while True:
            self.peers = <rbd_mirror_peer_site_t *>realloc_chk(
                self.peers, self.num_peers * sizeof(rbd_mirror_peer_site_t))
            with nogil:
                ret = rbd_mirror_peer_site_list(_ioctx, self.peers,
                                                &self.num_peers)
            if ret < 0:
                if ret == -errno.ERANGE:
                    continue
                self.num_peers = 0
                raise make_ex(ret, 'error listing peers')
            break

    def __iter__(self):
        for i in range(self.num_peers):
            yield {
                'uuid'         : decode_cstr(self.peers[i].uuid),
                'direction'    : int(self.peers[i].direction),
                'site_name'    : decode_cstr(self.peers[i].site_name),
                'cluster_name' : decode_cstr(self.peers[i].site_name),
                'mirror_uuid'  : decode_cstr(self.peers[i].mirror_uuid),
                'client_name'  : decode_cstr(self.peers[i].client_name),
                }

    def __dealloc__(self):
        if self.peers:
            rbd_mirror_peer_site_list_cleanup(self.peers, self.num_peers)
            free(self.peers)

cdef class MirrorImageStatusIterator(object):
    """
    Iterator over mirror image status for a pool.

    Yields a dictionary containing mirror status of an image.

    Keys are:

        * ``name`` (str) - mirror image name

        * ``id`` (str) - mirror image id

        * ``info`` (dict) - mirror image info

        * ``state`` (int) - status mirror state

        * ``description`` (str) - status description

        * ``last_update`` (datetime) - last status update time

        * ``up`` (bool) - is mirroring agent up

        * ``remote_statuses`` (array) -

        *   ``mirror uuid`` (str) - remote mirror uuid

        *   ``state`` (int) - status mirror state

        *   ``description`` (str) - status description

        *   ``last_update`` (datetime) - last status update time

        *   ``up`` (bool) - is mirroring agent up
    """

    cdef:
        rados_ioctx_t ioctx
        size_t max_read
        char *last_read
        char **image_ids
        rbd_mirror_image_site_status_t *s_status
        rbd_mirror_image_global_status_t *images
        size_t size

    def __init__(self, ioctx):
        self.ioctx = convert_ioctx(ioctx)
        self.max_read = 1024
        self.last_read = strdup("")
        self.image_ids = <char **>realloc_chk(NULL,
            sizeof(char *) * self.max_read)
        self.images = <rbd_mirror_image_global_status_t *>realloc_chk(NULL,
            sizeof(rbd_mirror_image_global_status_t) * self.max_read)
        self.size = 0
        self.get_next_chunk()


    def __iter__(self):
        while self.size > 0:
            for i in range(self.size):
                local_status = None
                site_statuses = []

                for x in range(self.images[i].site_statuses_count):
                    s_status = &self.images[i].site_statuses[x]
                    site_status = {
                        'state'       : s_status.state,
                        'description' : decode_cstr(s_status.description),
                        'last_update' : datetime.utcfromtimestamp(s_status.last_update),
                        'up'          : s_status.up,
                        }
                    mirror_uuid = decode_cstr(s_status.mirror_uuid)
                    if mirror_uuid == '':
                        local_status = site_status
                    else:
                        site_status['mirror_uuid'] = mirror_uuid
                        site_statuses.append(site_status)

                status = {
                    'name'        : decode_cstr(self.images[i].name),
                    'id'          : decode_cstr(self.image_ids[i]),
                    'info'        : {
                        'global_id' : decode_cstr(self.images[i].info.global_id),
                        'state'     : self.images[i].info.state,
                        # primary isn't added here because it is unknown (always
                        # false, see XXX in Mirror::image_global_status_list())
                        },
                    'remote_statuses': site_statuses,
                    }
                if local_status:
                    status.update(local_status)
                yield status
            if self.size < self.max_read:
                break
            self.get_next_chunk()

    def __dealloc__(self):
        rbd_mirror_image_global_status_list_cleanup(self.image_ids, self.images,
                                                    self.size)
        if self.last_read:
            free(self.last_read)
        if self.image_ids:
            free(self.image_ids)
        if self.images:
            free(self.images)

    def get_next_chunk(self):
        if self.size > 0:
            rbd_mirror_image_global_status_list_cleanup(self.image_ids,
                                                        self.images,
                                                        self.size)
            self.size = 0
        with nogil:
            ret = rbd_mirror_image_global_status_list(self.ioctx,
                                                      self.last_read,
                                                      self.max_read,
                                                      self.image_ids,
                                                      self.images, &self.size)
        if ret < 0:
            raise make_ex(ret, 'error listing mirror images status')
        if self.size > 0:
            last_read = cstr(self.image_ids[self.size - 1], 'last_read')
            free(self.last_read)
            self.last_read = strdup(last_read)
        else:
            free(self.last_read)
            self.last_read = strdup("")

cdef class MirrorImageInstanceIdIterator(object):
    """
    Iterator over mirror image instance id for a pool.

    Yields ``(image_id, instance_id)`` tuple.
    """

    cdef:
        rados_ioctx_t ioctx
        size_t max_read
        char *last_read
        char **image_ids
        char **instance_ids
        size_t size

    def __init__(self, ioctx):
        self.ioctx = convert_ioctx(ioctx)
        self.max_read = 1024
        self.last_read = strdup("")
        self.image_ids = <char **>realloc_chk(NULL,
            sizeof(char *) * self.max_read)
        self.instance_ids = <char **>realloc_chk(NULL,
            sizeof(char *) * self.max_read)
        self.size = 0
        self.get_next_chunk()

    def __iter__(self):
        while self.size > 0:
            for i in range(self.size):
                yield (decode_cstr(self.image_ids[i]),
                       decode_cstr(self.instance_ids[i]))
            if self.size < self.max_read:
                break
            self.get_next_chunk()

    def __dealloc__(self):
        rbd_mirror_image_instance_id_list_cleanup(self.image_ids,
                                                  self.instance_ids, self.size)
        if self.last_read:
            free(self.last_read)
        if self.image_ids:
            free(self.image_ids)
        if self.instance_ids:
            free(self.instance_ids)

    def get_next_chunk(self):
        if self.size > 0:
            rbd_mirror_image_instance_id_list_cleanup(self.image_ids,
                                                      self.instance_ids,
                                                      self.size)
            self.size = 0
        with nogil:
            ret = rbd_mirror_image_instance_id_list(self.ioctx, self.last_read,
                                                    self.max_read,
                                                    self.image_ids,
                                                    self.instance_ids,
                                                    &self.size)
        if ret < 0:
            raise make_ex(ret, 'error listing mirror images instance ids')
        if self.size > 0:
            last_read = cstr(self.image_ids[self.size - 1], 'last_read')
            free(self.last_read)
            self.last_read = strdup(last_read)
        else:
            free(self.last_read)
            self.last_read = strdup("")

cdef class MirrorImageInfoIterator(object):
    """
    Iterator over mirror image info for a pool.

    Yields ``(image_id, info)`` tuple.
    """

    cdef:
        rados_ioctx_t ioctx
        rbd_mirror_image_mode_t mode_filter
        rbd_mirror_image_mode_t *mode_filter_ptr
        size_t max_read
        char *last_read
        char **image_ids
        rbd_mirror_image_info_t *info_entries
        rbd_mirror_image_mode_t *mode_entries
        size_t size

    def __init__(self, ioctx, mode_filter):
        self.ioctx = convert_ioctx(ioctx)
        if mode_filter is not None:
            self.mode_filter = mode_filter
            self.mode_filter_ptr = &self.mode_filter
        else:
            self.mode_filter_ptr = NULL
        self.max_read = 1024
        self.last_read = strdup("")
        self.image_ids = <char **>realloc_chk(NULL,
            sizeof(char *) * self.max_read)
        self.info_entries = <rbd_mirror_image_info_t *>realloc_chk(NULL,
            sizeof(rbd_mirror_image_info_t) * self.max_read)
        self.mode_entries = <rbd_mirror_image_mode_t *>realloc_chk(NULL,
            sizeof(rbd_mirror_image_mode_t) * self.max_read)
        self.size = 0
        self.get_next_chunk()

    def __iter__(self):
        while self.size > 0:
            for i in range(self.size):
                yield (decode_cstr(self.image_ids[i]),
                       {
                           'mode'      : int(self.mode_entries[i]),
                           'global_id' : decode_cstr(self.info_entries[i].global_id),
                           'state'     : int(self.info_entries[i].state),
                           'primary'   : self.info_entries[i].primary,
                       })
            if self.size < self.max_read:
                break
            self.get_next_chunk()

    def __dealloc__(self):
        rbd_mirror_image_info_list_cleanup(self.image_ids, self.info_entries,
                                           self.size)
        if self.last_read:
            free(self.last_read)
        if self.image_ids:
            free(self.image_ids)
        if self.info_entries:
            free(self.info_entries)
        if self.mode_entries:
            free(self.mode_entries)

    def get_next_chunk(self):
        if self.size > 0:
            rbd_mirror_image_info_list_cleanup(self.image_ids,
                                               self.info_entries, self.size)
            self.size = 0
        with nogil:
            ret = rbd_mirror_image_info_list(self.ioctx, self.mode_filter_ptr,
                                             self.last_read, self.max_read,
                                             self.image_ids, self.mode_entries,
                                             self.info_entries, &self.size)
        if ret < 0:
            raise make_ex(ret, 'error listing mirror image info')
        if self.size > 0:
            last_read = cstr(self.image_ids[self.size - 1], 'last_read')
            free(self.last_read)
            self.last_read = strdup(last_read)
        else:
            free(self.last_read)
            self.last_read = strdup("")

cdef class PoolMetadataIterator(object):
    """
    Iterator over pool metadata list.

    Yields ``(key, value)`` tuple.

    * ``key`` (str) - metadata key
    * ``value`` (str) - metadata value
    """

    cdef:
        rados_ioctx_t ioctx
        char *last_read
        uint64_t max_read
        object next_chunk

    def __init__(self, ioctx):
        self.ioctx = convert_ioctx(ioctx)
        self.last_read = strdup("")
        self.max_read = 32
        self.get_next_chunk()

    def __iter__(self):
        while len(self.next_chunk) > 0:
            for pair in self.next_chunk:
                yield pair
            if len(self.next_chunk) < self.max_read:
                break
            self.get_next_chunk()

    def __dealloc__(self):
        if self.last_read:
            free(self.last_read)

    def get_next_chunk(self):
        cdef:
            char *c_keys = NULL
            size_t keys_size = 4096
            char *c_vals = NULL
            size_t vals_size = 4096
        try:
            while True:
                c_keys = <char *>realloc_chk(c_keys, keys_size)
                c_vals = <char *>realloc_chk(c_vals, vals_size)
                with nogil:
                    ret = rbd_pool_metadata_list(self.ioctx, self.last_read,
                                                 self.max_read, c_keys,
                                                 &keys_size, c_vals, &vals_size)
                if ret >= 0:
                    break
                elif ret != -errno.ERANGE:
                    raise make_ex(ret, 'error listing metadata')
            keys = [decode_cstr(key) for key in
                        c_keys[:keys_size].split(b'\0') if key]
            vals = [decode_cstr(val) for val in
                        c_vals[:vals_size].split(b'\0') if val]
            if len(keys) > 0:
                last_read = cstr(keys[-1], 'last_read')
                free(self.last_read)
                self.last_read = strdup(last_read)
            self.next_chunk = list(zip(keys, vals))
        finally:
            free(c_keys)
            free(c_vals)

cdef class ConfigPoolIterator(object):
    """
    Iterator over pool-level overrides for a pool.

    Yields a dictionary containing information about an override.

    Keys are:

    * ``name`` (str) - override name

    * ``value`` (str) - override value

    * ``source`` (str) - override source
    """

    cdef:
        rbd_config_option_t *options
        int num_options

    def __init__(self, ioctx):
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
        self.options = NULL
        self.num_options = 32
        while True:
            self.options = <rbd_config_option_t *>realloc_chk(
                self.options, self.num_options * sizeof(rbd_config_option_t))
            with nogil:
                ret = rbd_config_pool_list(_ioctx, self.options, &self.num_options)
            if ret < 0:
                if ret == -errno.ERANGE:
                    continue
                self.num_options = 0
                raise make_ex(ret, 'error listing config options')
            break

    def __iter__(self):
        for i in range(self.num_options):
            yield {
                'name'   : decode_cstr(self.options[i].name),
                'value'  : decode_cstr(self.options[i].value),
                'source' : self.options[i].source,
                }

    def __dealloc__(self):
        if self.options:
            rbd_config_pool_list_cleanup(self.options, self.num_options)
            free(self.options)

cdef int diff_iterate_cb(uint64_t offset, size_t length, int write, void *cb) \
    except? -9000 with gil:
    # Make sure that if we wound up with an exception from a previous callback,
    # we stop calling back (just in case librbd ever fails to bail out on the
    # first negative return, as older versions did)
    if exc.PyErr_Occurred():
        return -9000
    ret = (<object>cb)(offset, length, bool(write))
    if ret is None:
        return 0
    return ret

cdef class Group(object):
    """
    This class represents an RBD group. It is used to interact with
    snapshots and images members.
    """

    cdef object name
    cdef char *_name
    cdef object ioctx

    cdef rados_ioctx_t _ioctx

    def __init__(self, ioctx, name):
        name = cstr(name, 'name')
        self.name = name

        self._ioctx = convert_ioctx(ioctx)
        self._name = name

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        return False

    def add_image(self, image_ioctx, image_name):
        """
        Add an image to a group.

        :param image_ioctx: determines which RADOS pool the image belongs to.
        :type ioctx: :class:`rados.Ioctx`
        :param name: the name of the image to add
        :type name: str

        :raises: :class:`ObjectNotFound`
        :raises: :class:`ObjectExists`
        :raises: :class:`InvalidArgument`
        :raises: :class:`FunctionNotSupported`
        """
        image_name = cstr(image_name, 'image_name')
        cdef:
            rados_ioctx_t _image_ioctx = convert_ioctx(image_ioctx)
            char *_image_name = image_name
        with nogil:
            ret = rbd_group_image_add(self._ioctx, self._name, _image_ioctx, _image_name)
        if ret != 0:
            raise make_ex(ret, 'error adding image to group', group_errno_to_exception)

    def remove_image(self, image_ioctx, image_name):
        """
        Remove an image from a group.

        :param image_ioctx: determines which RADOS pool the image belongs to.
        :type ioctx: :class:`rados.Ioctx`
        :param name: the name of the image to remove
        :type name: str

        :raises: :class:`ObjectNotFound`
        :raises: :class:`InvalidArgument`
        :raises: :class:`FunctionNotSupported`
        """
        image_name = cstr(image_name, 'image_name')
        cdef:
            rados_ioctx_t _image_ioctx = convert_ioctx(image_ioctx)
            char *_image_name = image_name
        with nogil:
            ret = rbd_group_image_remove(self._ioctx, self._name, _image_ioctx, _image_name)
        if ret != 0:
            raise make_ex(ret, 'error removing image from group', group_errno_to_exception)


    def list_images(self):
        """
        Iterate over the images of a group.

        :returns: :class:`GroupImageIterator`
        """
        return GroupImageIterator(self)

    def create_snap(self, snap_name, flags=0):
        """
        Create a snapshot for the group.

        :param snap_name: the name of the snapshot to create
        :param flags: create snapshot flags
        :type name: str

        :raises: :class:`ObjectNotFound`
        :raises: :class:`ObjectExists`
        :raises: :class:`InvalidArgument`
        :raises: :class:`FunctionNotSupported`
        """
        snap_name = cstr(snap_name, 'snap_name')
        cdef:
            char *_snap_name = snap_name
            uint32_t _flags = flags
        with nogil:
            ret = rbd_group_snap_create2(self._ioctx, self._name, _snap_name,
                                         _flags)
        if ret != 0:
            raise make_ex(ret, 'error creating group snapshot', group_errno_to_exception)

    def remove_snap(self, snap_name):
        """
        Remove a snapshot from the group.

        :param snap_name: the name of the snapshot to remove
        :type name: str

        :raises: :class:`ObjectNotFound`
        :raises: :class:`InvalidArgument`
        :raises: :class:`FunctionNotSupported`
        """
        snap_name = cstr(snap_name, 'snap_name')
        cdef:
            char *_snap_name = snap_name
        with nogil:
            ret = rbd_group_snap_remove(self._ioctx, self._name, _snap_name)
        if ret != 0:
            raise make_ex(ret, 'error removing group snapshot', group_errno_to_exception)

    def rename_snap(self, old_snap_name, new_snap_name):
        """
        Rename group's snapshot.

        :raises: :class:`ObjectNotFound`
        :raises: :class:`ObjectExists`
        :raises: :class:`InvalidArgument`
        :raises: :class:`FunctionNotSupported`
        """

        old_snap_name = cstr(old_snap_name, 'old_snap_name')
        new_snap_name = cstr(new_snap_name, 'new_snap_name')
        cdef:
            char *_old_snap_name = old_snap_name
            char *_new_snap_name = new_snap_name
        with nogil:
            ret = rbd_group_snap_rename(self._ioctx, self._name, _old_snap_name,
                                        _new_snap_name)
        if ret != 0:
            raise make_ex(ret, 'error renaming group snapshot',
                          group_errno_to_exception)

    def list_snaps(self):
        """
        Iterate over the images of a group.

        :returns: :class:`GroupSnapIterator`
        """
        return GroupSnapIterator(self)

    def rollback_to_snap(self, name):
        """
        Rollback group to snapshot.

        :param name: the group snapshot to rollback to
        :type name: str
        :raises: :class:`ObjectNotFound`
        :raises: :class:`IOError`
        """
        name = cstr(name, 'name')
        cdef char *_name = name
        with nogil:
            ret = rbd_group_snap_rollback(self._ioctx, self._name, _name)
        if ret != 0:
            raise make_ex(ret, 'error rolling back group to snapshot', group_errno_to_exception)

def requires_not_closed(f):
    def wrapper(self, *args, **kwargs):
        self.require_not_closed()
        return f(self, *args, **kwargs)

    return wrapper

cdef class Image(object):
    """
    This class represents an RBD image. It is used to perform I/O on
    the image and interact with snapshots.

    **Note**: Any method of this class may raise :class:`ImageNotFound`
    if the image has been deleted.
    """
    cdef rbd_image_t image
    cdef bint closed
    cdef object name
    cdef object ioctx
    cdef rados_ioctx_t _ioctx
    cdef Completion _open_completion

    def __init__(self, ioctx, name=None, snapshot=None,
                 read_only=False, image_id=None, _oncomplete=None):
        """
        Open the image at the given snapshot.
        Specify either name or id, otherwise :class:`InvalidArgument` is raised.

        If a snapshot is specified, the image will be read-only, unless
        :func:`Image.set_snap` is called later.

        If read-only mode is used, metadata for the :class:`Image`
        object (such as which snapshots exist) may become obsolete. See
        the C api for more details.

        To clean up from opening the image, :func:`Image.close` should
        be called.  For ease of use, this is done automatically when
        an :class:`Image` is used as a context manager (see :pep:`343`).

        :param ioctx: determines which RADOS pool the image is in
        :type ioctx: :class:`rados.Ioctx`
        :param name: the name of the image
        :type name: str
        :param snapshot: which snapshot to read from
        :type snapshot: str
        :param read_only: whether to open the image in read-only mode
        :type read_only: bool
        :param image_id: the id of the image
        :type image_id: str
        """
        name = cstr(name, 'name', opt=True)
        image_id = cstr(image_id, 'image_id', opt=True)
        snapshot = cstr(snapshot, 'snapshot', opt=True)
        self.closed = True
        if name is not None and image_id is not None:
            raise InvalidArgument("only need to specify image name or image id")
        elif name is None and image_id is None:
            raise InvalidArgument("image name or image id was not specified")
        elif name is not None:
            self.name = name
        else:
            self.name = image_id
        # Keep around a reference to the ioctx, so it won't get deleted
        self.ioctx = ioctx
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            char *_name = opt_str(name)
            char *_image_id = opt_str(image_id)
            char *_snapshot = opt_str(snapshot)
            cdef Completion completion

        if _oncomplete:
            def oncomplete(completion_v):
                cdef Completion _completion_v = completion_v
                return_value = _completion_v.get_return_value()
                if return_value == 0:
                    self.closed = False
                    if name is None:
                        self.name = self.get_name()
                return _oncomplete(_completion_v, self)

            completion = self.__get_completion(oncomplete)
            try:
                completion.__persist()
                if read_only:
                    with nogil:
                        if name is not None:
                            ret = rbd_aio_open_read_only(
                                _ioctx, _name, &self.image, _snapshot,
                                completion.rbd_comp)
                        else:
                            ret = rbd_aio_open_by_id_read_only(
                                _ioctx, _image_id, &self.image, _snapshot,
                                completion.rbd_comp)
                else:
                    with nogil:
                        if name is not None:
                            ret = rbd_aio_open(
                                _ioctx, _name, &self.image, _snapshot,
                                completion.rbd_comp)
                        else:
                            ret = rbd_aio_open_by_id(
                                _ioctx, _image_id, &self.image, _snapshot,
                                completion.rbd_comp)
                if ret != 0:
                    raise make_ex(ret, 'error opening image %s at snapshot %s' %
                                  (self.name, snapshot))
            except:
                completion.__unpersist()
                raise

            self._open_completion = completion
            return

        if read_only:
            with nogil:
                if name is not None:
                    ret = rbd_open_read_only(_ioctx, _name, &self.image, _snapshot)
                else:
                    ret = rbd_open_by_id_read_only(_ioctx, _image_id, &self.image, _snapshot)
        else:
            with nogil:
                if name is not None:
                    ret = rbd_open(_ioctx, _name, &self.image, _snapshot)
                else:
                    ret = rbd_open_by_id(_ioctx, _image_id, &self.image, _snapshot)
        if ret != 0:
            raise make_ex(ret, 'error opening image %s at snapshot %s' % (self.name, snapshot))
        self.closed = False
        if name is None:
            self.name = self.get_name()

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        """
        Closes the image. See :func:`close`
        """
        self.close()
        return False

    def __get_completion(self, oncomplete):
        """
        Constructs a completion to use with asynchronous operations

        :param oncomplete: callback for the completion

        :raises: :class:`Error`
        :returns: completion object
        """

        completion_obj = Completion(self, oncomplete)

        cdef:
            rbd_completion_t completion
            PyObject* p_completion_obj= <PyObject*>completion_obj

        with nogil:
            ret = rbd_aio_create_completion(p_completion_obj, __aio_complete_cb,
                                            &completion)
        if ret < 0:
            raise make_ex(ret, "error getting a completion")

        completion_obj.rbd_comp = completion
        return completion_obj

    def require_not_closed(self):
        """
        Checks if the Image is not closed

        :raises: :class:`InvalidArgument`
        """
        if self.closed:
            raise InvalidArgument("image is closed")

    def close(self):
        """
        Release the resources used by this image object.

        After this is called, this object should not be used.
        """
        if not self.closed:
            self.closed = True
            with nogil:
                ret = rbd_close(self.image)
            if ret < 0:
                raise make_ex(ret, 'error while closing image %s' % (
                              self.name,))

    @requires_not_closed
    def aio_close(self, oncomplete):
        """
        Asynchronously close the image.

        After this is called, this object should not be used.

        :param oncomplete: what to do when close is complete
        :type oncomplete: completion
        :returns: :class:`Completion` - the completion object
        """
        cdef Completion completion = self.__get_completion(oncomplete)
        self.closed = True
        try:
            completion.__persist()
            with nogil:
                ret = rbd_aio_close(self.image, completion.rbd_comp)
            if ret < 0:
                raise make_ex(ret, 'error while closing image %s' %
                              self.name)
        except:
            completion.__unpersist()
            raise
        return completion

    def __dealloc__(self):
        self.close()

    def __repr__(self):
        return "rbd.Image(ioctx, %r)" % self.name

    @requires_not_closed
    def resize(self, size, allow_shrink=True):
        """
        Change the size of the image, allow shrink.

        :param size: the new size of the image
        :type size: int
        :param allow_shrink: permit shrinking
        :type allow_shrink: bool
        """
        old_size = self.size()
        if old_size == size:
            return
        if not allow_shrink and old_size > size:
            raise InvalidArgument("error allow_shrink is False but old_size > new_size")
        cdef:
            uint64_t _size = size
            bint _allow_shrink = allow_shrink
            librbd_progress_fn_t prog_cb = &no_op_progress_callback
        with nogil:
            ret = rbd_resize2(self.image, _size, _allow_shrink, prog_cb, NULL)
        if ret < 0:
            raise make_ex(ret, 'error resizing image %s' % self.name)

    @requires_not_closed
    def stat(self):
        """
        Get information about the image. Currently parent pool and
        parent name are always -1 and ''.

        :returns: dict - contains the following keys:

            * ``size`` (int) - the size of the image in bytes

            * ``obj_size`` (int) - the size of each object that comprises the
              image

            * ``num_objs`` (int) - the number of objects in the image

            * ``order`` (int) - log_2(object_size)

            * ``block_name_prefix`` (str) - the prefix of the RADOS objects used
              to store the image

            * ``parent_pool`` (int) - deprecated

            * ``parent_name``  (str) - deprecated

            See also :meth:`format` and :meth:`features`.

        """
        cdef rbd_image_info_t info
        with nogil:
            ret = rbd_stat(self.image, &info, sizeof(info))
        if ret != 0:
            raise make_ex(ret, 'error getting info for image %s' % self.name)
        return {
            'size'              : info.size,
            'obj_size'          : info.obj_size,
            'num_objs'          : info.num_objs,
            'order'             : info.order,
            'block_name_prefix' : decode_cstr(info.block_name_prefix),
            'parent_pool'       : info.parent_pool,
            'parent_name'       : info.parent_name
            }

    @requires_not_closed
    def get_name(self):
        """
        Get the RBD image name

        :returns: str - image name
        """
        cdef:
            int ret = -errno.ERANGE
            size_t size = 64
            char *image_name = NULL
        try:
            while ret == -errno.ERANGE:
                image_name =  <char *>realloc_chk(image_name, size)
                with nogil:
                    ret = rbd_get_name(self.image, image_name, &size)

            if ret != 0:
                raise make_ex(ret, 'error getting name for image %s' % self.name)
            return decode_cstr(image_name)
        finally:
            free(image_name)

    @requires_not_closed
    def id(self):
        """
        Get the RBD v2 internal image id

        :returns: str - image id
        """
        cdef:
            int ret = -errno.ERANGE
            size_t size = 32
            char *image_id = NULL
        try:
            while ret == -errno.ERANGE and size <= 4096:
                image_id =  <char *>realloc_chk(image_id, size)
                with nogil:
                    ret = rbd_get_id(self.image, image_id, size)
                if ret == -errno.ERANGE:
                    size *= 2

            if ret != 0:
                raise make_ex(ret, 'error getting id for image %s' % self.name)
            return decode_cstr(image_id)
        finally:
            free(image_id)

    @requires_not_closed
    def block_name_prefix(self):
        """
        Get the RBD block name prefix

        :returns: str - block name prefix
        """
        cdef:
            int ret = -errno.ERANGE
            size_t size = 32
            char *prefix = NULL
        try:
            while ret == -errno.ERANGE and size <= 4096:
                prefix =  <char *>realloc_chk(prefix, size)
                with nogil:
                    ret = rbd_get_block_name_prefix(self.image, prefix, size)
                if ret == -errno.ERANGE:
                    size *= 2

            if ret != 0:
                raise make_ex(ret, 'error getting block name prefix for image %s' % self.name)
            return decode_cstr(prefix)
        finally:
            free(prefix)

    @requires_not_closed
    def data_pool_id(self):
        """
        Get the pool id of the pool where the data of this RBD image is stored.

        :returns: int - the pool id
        """
        with nogil:
            ret = rbd_get_data_pool_id(self.image)
        if ret < 0:
            raise make_ex(ret, 'error getting data pool id for image %s' % self.name)
        return ret

    @requires_not_closed
    def get_parent_image_spec(self):
        """
        Get spec of the cloned image's parent

        :returns: dict - contains the following keys:
            * ``pool_name`` (str) - parent pool name
            * ``pool_namespace`` (str) - parent pool namespace
            * ``image_name`` (str) - parent image name
            * ``snap_name`` (str) - parent snapshot name

        :raises: :class:`ImageNotFound` if the image doesn't have a parent
        """
        cdef:
            rbd_linked_image_spec_t parent_spec
            rbd_snap_spec_t snap_spec
        with nogil:
            ret = rbd_get_parent(self.image, &parent_spec, &snap_spec)
        if ret != 0:
            raise make_ex(ret, 'error getting parent info for image %s' % self.name)

        result = {'pool_name': decode_cstr(parent_spec.pool_name),
                  'pool_namespace': decode_cstr(parent_spec.pool_namespace),
                  'image_name': decode_cstr(parent_spec.image_name),
                  'snap_name': decode_cstr(snap_spec.name)}

        rbd_linked_image_spec_cleanup(&parent_spec)
        rbd_snap_spec_cleanup(&snap_spec)
        return result

    @requires_not_closed
    def parent_info(self):
        """
        Deprecated. Use `get_parent_image_spec` instead.

        Get information about a cloned image's parent (if any)

        :returns: tuple - ``(pool name, image name, snapshot name)`` components
                  of the parent image
        :raises: :class:`ImageNotFound` if the image doesn't have a parent
        """
        parent = self.get_parent_image_spec()
        return (parent['pool_name'], parent['image_name'], parent['snap_name'])

    @requires_not_closed
    def parent_id(self):
        """
        Get image id of a cloned image's parent (if any)

        :returns: str - the parent id
        :raises: :class:`ImageNotFound` if the image doesn't have a parent
        """
        cdef:
            rbd_linked_image_spec_t parent_spec
            rbd_snap_spec_t snap_spec
        with nogil:
            ret = rbd_get_parent(self.image, &parent_spec, &snap_spec)
        if ret != 0:
            raise make_ex(ret, 'error getting parent info for image %s' % self.name)

        result = decode_cstr(parent_spec.image_id)

        rbd_linked_image_spec_cleanup(&parent_spec)
        rbd_snap_spec_cleanup(&snap_spec)
        return result

    @requires_not_closed
    def migration_source_spec(self):
        """
        Get migration source spec (if any)

        :returns: dict
        :raises: :class:`ImageNotFound` if the image is not migration destination
        """
        cdef:
            size_t size = 512
            char *spec = NULL
        try:
            while True:
                spec = <char *>realloc_chk(spec, size)
                with nogil:
                    ret = rbd_get_migration_source_spec(self.image, spec, &size)
                if ret >= 0:
                    break
                elif ret != -errno.ERANGE:
                    raise make_ex(ret, 'error retrieving migration source')
            return json.loads(decode_cstr(spec))
        finally:
            free(spec)

    @requires_not_closed
    def old_format(self):
        """
        Find out whether the image uses the old RBD format.

        :returns: bool - whether the image uses the old RBD format
        """
        cdef uint8_t old
        with nogil:
            ret = rbd_get_old_format(self.image, &old)
        if ret != 0:
            raise make_ex(ret, 'error getting old_format for image %s' % (self.name))
        return old != 0

    @requires_not_closed
    def size(self):
        """
        Get the size of the image. If open to a snapshot, returns the
        size of that snapshot.

        :returns: int - the size of the image in bytes
        """
        cdef uint64_t image_size
        with nogil:
            ret = rbd_get_size(self.image, &image_size)
        if ret != 0:
            raise make_ex(ret, 'error getting size for image %s' % (self.name))
        return image_size

    @requires_not_closed
    def features(self):
        """
        Get the features bitmask of the image.

        :returns: int - the features bitmask of the image
        """
        cdef uint64_t features
        with nogil:
            ret = rbd_get_features(self.image, &features)
        if ret != 0:
            raise make_ex(ret, 'error getting features for image %s' % (self.name))
        return features

    @requires_not_closed
    def update_features(self, features, enabled):
        """
        Update the features bitmask of the image by enabling/disabling
        a single feature.  The feature must support the ability to be
        dynamically enabled/disabled.

        :param features: feature bitmask to enable/disable
        :type features: int
        :param enabled: whether to enable/disable the feature
        :type enabled: bool
        :raises: :class:`InvalidArgument`
        """
        cdef:
            uint64_t _features = features
            uint8_t _enabled = bool(enabled)
        with nogil:
            ret = rbd_update_features(self.image, _features, _enabled)
        if ret != 0:
            raise make_ex(ret, 'error updating features for image %s' %
                               (self.name))

    @requires_not_closed
    def op_features(self):
        """
        Get the op features bitmask of the image.

        :returns: int - the op features bitmask of the image
        """
        cdef uint64_t op_features
        with nogil:
            ret = rbd_get_op_features(self.image, &op_features)
        if ret != 0:
            raise make_ex(ret, 'error getting op features for image %s' % (self.name))
        return op_features

    @requires_not_closed
    def overlap(self):
        """
        Get the number of overlapping bytes between the image and its parent
        image. If open to a snapshot, returns the overlap between the snapshot
        and the parent image.

        :returns: int - the overlap in bytes
        :raises: :class:`ImageNotFound` if the image doesn't have a parent
        """
        cdef uint64_t overlap
        with nogil:
            ret = rbd_get_overlap(self.image, &overlap)
        if ret != 0:
            raise make_ex(ret, 'error getting overlap for image %s' % (self.name))
        return overlap

    @requires_not_closed
    def flags(self):
        """
        Get the flags bitmask of the image.

        :returns: int - the flags bitmask of the image
        """
        cdef uint64_t flags
        with nogil:
            ret = rbd_get_flags(self.image, &flags)
        if ret != 0:
            raise make_ex(ret, 'error getting flags for image %s' % (self.name))
        return flags

    @requires_not_closed
    def group(self):
        """
        Get information about the image's group.

        :returns: dict - contains the following keys:

            * ``pool`` (int) - id of the group pool

            * ``name`` (str) - name of the group

        """
        cdef rbd_group_info_t info
        with nogil:
            ret = rbd_get_group(self.image, &info, sizeof(info))
        if ret != 0:
            raise make_ex(ret, 'error getting group for image %s' % self.name)
        result = {
            'pool' : info.pool,
            'name' : decode_cstr(info.name)
            }
        rbd_group_info_cleanup(&info, sizeof(info))
        return result

    @requires_not_closed
    def is_exclusive_lock_owner(self):
        """
        Get the status of the image exclusive lock.

        :returns: bool - true if the image is exclusively locked
        """
        cdef int owner
        with nogil:
            ret = rbd_is_exclusive_lock_owner(self.image, &owner)
        if ret != 0:
            raise make_ex(ret, 'error getting lock status for image %s' % (self.name))
        return owner == 1

    @requires_not_closed
    def copy(self, dest_ioctx, dest_name, features=None, order=None,
             stripe_unit=None, stripe_count=None, data_pool=None):
        """
        Copy the image to another location.

        :param dest_ioctx: determines which pool to copy into
        :type dest_ioctx: :class:`rados.Ioctx`
        :param dest_name: the name of the copy
        :type dest_name: str
        :param features: bitmask of features to enable; if set, must include layering
        :type features: int
        :param order: the image is split into (2**order) byte objects
        :type order: int
        :param stripe_unit: stripe unit in bytes (default None to let librbd decide)
        :type stripe_unit: int
        :param stripe_count: objects to stripe over before looping
        :type stripe_count: int
        :param data_pool: optional separate pool for data blocks
        :type data_pool: str
        :raises: :class:`TypeError`
        :raises: :class:`InvalidArgument`
        :raises: :class:`ImageExists`
        :raises: :class:`FunctionNotSupported`
        :raises: :class:`ArgumentOutOfRange`
        """
        dest_name = cstr(dest_name, 'dest_name')
        data_pool = cstr(data_pool, 'data_pool', opt=True)
        cdef:
            rados_ioctx_t _dest_ioctx = convert_ioctx(dest_ioctx)
            char *_dest_name = dest_name
            rbd_image_options_t opts

        rbd_image_options_create(&opts)
        try:
            if features is not None:
                rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_FEATURES,
                                             features)
            if order is not None:
                rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_ORDER,
                                             order)
            if stripe_unit is not None:
                rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_STRIPE_UNIT,
                                             stripe_unit)
            if stripe_count is not None:
                rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_STRIPE_COUNT,
                                             stripe_count)
            if data_pool is not None:
                rbd_image_options_set_string(opts, RBD_IMAGE_OPTION_DATA_POOL,
                                             data_pool)
            with nogil:
                ret = rbd_copy3(self.image, _dest_ioctx, _dest_name, opts)
        finally:
            rbd_image_options_destroy(opts)
        if ret < 0:
            raise make_ex(ret, 'error copying image %s to %s' % (self.name, dest_name))

    @requires_not_closed
    def deep_copy(self, dest_ioctx, dest_name, features=None, order=None,
                  stripe_unit=None, stripe_count=None, data_pool=None,
                  clone_format=None, flatten=False):
        """
        Deep copy the image to another location.

        :param dest_ioctx: determines which pool to copy into
        :type dest_ioctx: :class:`rados.Ioctx`
        :param dest_name: the name of the copy
        :type dest_name: str
        :param features: bitmask of features to enable; if set, must include layering
        :type features: int
        :param order: the image is split into (2**order) byte objects
        :type order: int
        :param stripe_unit: stripe unit in bytes (default None to let librbd decide)
        :type stripe_unit: int
        :param stripe_count: objects to stripe over before looping
        :type stripe_count: int
        :param data_pool: optional separate pool for data blocks
        :type data_pool: str
        :param clone_format: if the source image is a clone, which clone format
                             to use for the destination image
        :type clone_format: int
        :param flatten: if the source image is a clone, whether to flatten the
                        destination image or make it a clone of the same parent
        :type flatten: bool
        :raises: :class:`TypeError`
        :raises: :class:`InvalidArgument`
        :raises: :class:`ImageExists`
        :raises: :class:`FunctionNotSupported`
        :raises: :class:`ArgumentOutOfRange`
        """
        dest_name = cstr(dest_name, 'dest_name')
        data_pool = cstr(data_pool, 'data_pool', opt=True)
        cdef:
            rados_ioctx_t _dest_ioctx = convert_ioctx(dest_ioctx)
            char *_dest_name = dest_name
            rbd_image_options_t opts

        rbd_image_options_create(&opts)
        try:
            if features is not None:
                rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_FEATURES,
                                             features)
            if order is not None:
                rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_ORDER,
                                             order)
            if stripe_unit is not None:
                rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_STRIPE_UNIT,
                                             stripe_unit)
            if stripe_count is not None:
                rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_STRIPE_COUNT,
                                             stripe_count)
            if data_pool is not None:
                rbd_image_options_set_string(opts, RBD_IMAGE_OPTION_DATA_POOL,
                                             data_pool)
            if clone_format is not None:
                rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_CLONE_FORMAT,
                                             clone_format)
            rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_FLATTEN, flatten)
            with nogil:
                ret = rbd_deep_copy(self.image, _dest_ioctx, _dest_name, opts)
        finally:
            rbd_image_options_destroy(opts)
        if ret < 0:
            raise make_ex(ret, 'error deep copying image %s to %s' % (self.name, dest_name))

    @requires_not_closed
    def list_snaps(self):
        """
        Iterate over the snapshots of an image.

        :returns: :class:`SnapIterator`
        """
        return SnapIterator(self)

    @requires_not_closed
    def create_snap(self, name, flags=0):
        """
        Create a snapshot of the image.

        :param name: the name of the snapshot
        :type name: str
        :raises: :class:`ImageExists`, :class:`InvalidArgument`
        """
        name = cstr(name, 'name')
        cdef:
            char *_name = name
            uint32_t _flags = flags
            librbd_progress_fn_t prog_cb = &no_op_progress_callback
        with nogil:
            ret = rbd_snap_create2(self.image, _name, _flags, prog_cb, NULL)
        if ret != 0:
            raise make_ex(ret, 'error creating snapshot %s from %s' % (name, self.name))

    @requires_not_closed
    def rename_snap(self, srcname, dstname):
        """
        rename a snapshot of the image.

        :param srcname: the src name of the snapshot
        :type srcname: str
        :param dstname: the dst name of the snapshot
        :type dstname: str
        :raises: :class:`ImageExists`
        """
        srcname = cstr(srcname, 'srcname')
        dstname = cstr(dstname, 'dstname')
        cdef:
            char *_srcname = srcname
            char *_dstname = dstname
        with nogil:
            ret = rbd_snap_rename(self.image, _srcname, _dstname)
        if ret != 0:
            raise make_ex(ret, 'error renaming snapshot of %s from %s to %s' % (self.name, srcname, dstname))

    @requires_not_closed
    def remove_snap(self, name):
        """
        Delete a snapshot of the image.

        :param name: the name of the snapshot
        :type name: str
        :raises: :class:`IOError`, :class:`ImageBusy`, :class:`ImageNotFound`
        """
        name = cstr(name, 'name')
        cdef char *_name = name
        with nogil:
            ret = rbd_snap_remove(self.image, _name)
        if ret != 0:
            raise make_ex(ret, 'error removing snapshot %s from %s' % (name, self.name))

    @requires_not_closed
    def remove_snap2(self, name, flags):
        """
        Delete a snapshot of the image.

        :param name: the name of the snapshot
        :param flags: the flags for removal
        :type name: str
        :raises: :class:`IOError`, :class:`ImageBusy`
        """
        self.require_not_closed()

        name = cstr(name, 'name')
        cdef:
            char *_name = name
            uint32_t _flags = flags
            librbd_progress_fn_t prog_cb = &no_op_progress_callback
        with nogil:
            ret = rbd_snap_remove2(self.image, _name, _flags, prog_cb, NULL)
        if ret != 0:
            raise make_ex(ret, 'error removing snapshot %s from %s with flags %lx' % (name, self.name, flags))

    @requires_not_closed
    def remove_snap_by_id(self, snap_id):
        """
        Delete a snapshot of the image by its id.

        :param id: the id of the snapshot
        :type name: int
        :raises: :class:`IOError`, :class:`ImageBusy`
        """
        cdef:
            uint64_t _snap_id = snap_id
        with nogil:
            ret = rbd_snap_remove_by_id(self.image, _snap_id)
        if ret != 0:
            raise make_ex(ret, 'error removing snapshot %s from %s' % (snap_id, self.name))

    @requires_not_closed
    def rollback_to_snap(self, name):
        """
        Revert the image to its contents at a snapshot. This is a
        potentially expensive operation, since it rolls back each
        object individually.

        :param name: the snapshot to rollback to
        :type name: str
        :raises: :class:`IOError`
        """
        name = cstr(name, 'name')
        cdef char *_name = name
        with nogil:
            ret = rbd_snap_rollback(self.image, _name)
        if ret != 0:
            raise make_ex(ret, 'error rolling back image %s to snapshot %s' % (self.name, name))

    @requires_not_closed
    def protect_snap(self, name):
        """
        Mark a snapshot as protected. This means it can't be deleted
        until it is unprotected.

        :param name: the snapshot to protect
        :type name: str
        :raises: :class:`IOError`, :class:`ImageNotFound`
        """
        name = cstr(name, 'name')
        cdef char *_name = name
        with nogil:
            ret = rbd_snap_protect(self.image, _name)
        if ret != 0:
            raise make_ex(ret, 'error protecting snapshot %s@%s' % (self.name, name))

    @requires_not_closed
    def unprotect_snap(self, name):
        """
        Mark a snapshot unprotected. This allows it to be deleted if
        it was protected.

        :param name: the snapshot to unprotect
        :type name: str
        :raises: :class:`IOError`, :class:`ImageNotFound`
        """
        name = cstr(name, 'name')
        cdef char *_name = name
        with nogil:
            ret = rbd_snap_unprotect(self.image, _name)
        if ret != 0:
            raise make_ex(ret, 'error unprotecting snapshot %s@%s' % (self.name, name))

    @requires_not_closed
    def is_protected_snap(self, name):
        """
        Find out whether a snapshot is protected from deletion.

        :param name: the snapshot to check
        :type name: str
        :returns: bool - whether the snapshot is protected
        :raises: :class:`IOError`, :class:`ImageNotFound`
        """
        name = cstr(name, 'name')
        cdef:
            char *_name = name
            int is_protected
        with nogil:
            ret = rbd_snap_is_protected(self.image, _name, &is_protected)
        if ret != 0:
            raise make_ex(ret, 'error checking if snapshot %s@%s is protected' % (self.name, name))
        return is_protected == 1

    @requires_not_closed
    def snap_exists(self, name):
        """
        Find out whether a snapshot is exists.

        :param name: the snapshot to check
        :type name: str
        :returns: bool - whether the snapshot is exists
        """
        name = cstr(name, 'name')
        cdef:
            char *_name = name
            libcpp.bool _exists = False
        with nogil:
            ret = rbd_snap_exists(self.image, _name, &_exists)
        if ret != 0:
            raise make_ex(ret, 'error getting snapshot exists for %s' % self.name)
        return _exists

    @requires_not_closed
    def get_snap_limit(self):
        """
        Get the snapshot limit for an image.

        :returns: int - the snapshot limit for an image
        """
        cdef:
            uint64_t limit
        with nogil:
            ret = rbd_snap_get_limit(self.image, &limit)
        if ret != 0:
            raise make_ex(ret, 'error getting snapshot limit for %s' % self.name)
        return limit

    @requires_not_closed
    def set_snap_limit(self, limit):
        """
        Set the snapshot limit for an image.

        :param limit: the new limit to set
        """
        cdef:
            uint64_t _limit = limit
        with nogil:
            ret = rbd_snap_set_limit(self.image, _limit)
        if ret != 0:
            raise make_ex(ret, 'error setting snapshot limit for %s' % self.name)
        return ret

    @requires_not_closed
    def get_snap_timestamp(self, snap_id):
        """
        Get the snapshot timestamp for an image.
        :param snap_id: the snapshot id of a snap shot
        :returns: datetime - the snapshot timestamp for an image
        """
        cdef:
            timespec timestamp
            uint64_t _snap_id = snap_id
        with nogil:
            ret = rbd_snap_get_timestamp(self.image, _snap_id, &timestamp)
        if ret != 0:
            raise make_ex(ret, 'error getting snapshot timestamp for image: %s, snap_id: %d' % (self.name, snap_id))
        return datetime.utcfromtimestamp(timestamp.tv_sec)

    @requires_not_closed
    def remove_snap_limit(self):
        """
        Remove the snapshot limit for an image, essentially setting
        the limit to the maximum size allowed by the implementation.
        """
        with nogil:
            ret = rbd_snap_set_limit(self.image, UINT64_MAX)
        if ret != 0:
            raise make_ex(ret, 'error removing snapshot limit for %s' % self.name)
        return ret

    @requires_not_closed
    def set_snap(self, name):
        """
        Set the snapshot to read from. Writes will raise ReadOnlyImage
        while a snapshot is set. Pass None to unset the snapshot
        (reads come from the current image) , and allow writing again.

        :param name: the snapshot to read from, or None to unset the snapshot
        :type name: str or None
        """
        name = cstr(name, 'name', opt=True)
        cdef char *_name = opt_str(name)
        with nogil:
            ret = rbd_snap_set(self.image, _name)
        if ret != 0:
            raise make_ex(ret, 'error setting image %s to snapshot %s' % (self.name, name))

    @requires_not_closed
    def set_snap_by_id(self, snap_id):
        """
        Set the snapshot to read from. Writes will raise ReadOnlyImage
        while a snapshot is set. Pass None to unset the snapshot
        (reads come from the current image) , and allow writing again.

        :param snap_id: the snapshot to read from, or None to unset the snapshot
        :type snap_id: int
        """
        if not snap_id:
            snap_id = _LIBRADOS_SNAP_HEAD
        cdef int64_t _snap_id = snap_id
        with nogil:
            ret = rbd_snap_set_by_id(self.image, _snap_id)
        if ret != 0:
            raise make_ex(ret, 'error setting image %s to snapshot %d' % (self.name, snap_id))

    @requires_not_closed
    def snap_get_name(self, snap_id):
        """
        Get snapshot name by id.

        :param snap_id: the snapshot id
        :type snap_id: int
        :returns: str - snapshot name
        :raises: :class:`ImageNotFound`
        """
        cdef:
            int ret = -errno.ERANGE
            int64_t _snap_id = snap_id
            size_t size = 512
            char *image_name = NULL
        try:
            while ret == -errno.ERANGE:
                image_name =  <char *>realloc_chk(image_name, size)
                with nogil:
                    ret = rbd_snap_get_name(self.image, _snap_id, image_name, &size)

            if ret != 0:
                raise make_ex(ret, 'error snap_get_name.')
            return decode_cstr(image_name)
        finally:
            free(image_name)

    @requires_not_closed
    def snap_get_id(self, snap_name):
        """
        Get snapshot id by name.

        :param snap_name: the snapshot name
        :type snap_name: str
        :returns: int - snapshot id
        :raises: :class:`ImageNotFound`
        """
        snap_name = cstr(snap_name, 'snap_name')
        cdef:
            const char *_snap_name = snap_name
            uint64_t snap_id
        with nogil:
            ret = rbd_snap_get_id(self.image, _snap_name, &snap_id)
        if ret != 0:
            raise make_ex(ret, 'error snap_get_id.')
        return snap_id

    @requires_not_closed
    def read(self, offset, length, fadvise_flags=0):
        """
        Read data from the image. Raises :class:`InvalidArgument` if
        part of the range specified is outside the image.

        :param offset: the offset to start reading at
        :type offset: int
        :param length: how many bytes to read
        :type length: int
        :param fadvise_flags: fadvise flags for this read
        :type fadvise_flags: int
        :returns: str - the data read
        :raises: :class:`InvalidArgument`, :class:`IOError`
        """

        # This usage of the Python API allows us to construct a string
        # that librbd directly reads into, avoiding an extra copy. Although
        # strings are normally immutable, this usage is explicitly supported
        # for freshly created string objects.
        cdef:
            char *ret_buf
            uint64_t _offset = offset
            size_t _length = length
            int _fadvise_flags = fadvise_flags
            PyObject* ret_s = NULL
        ret_s = PyBytes_FromStringAndSize(NULL, length)
        try:
            ret_buf = PyBytes_AsString(ret_s)
            with nogil:
                ret = rbd_read2(self.image, _offset, _length, ret_buf,
                                _fadvise_flags)
            if ret < 0:
                raise make_ex(ret, 'error reading %s %ld~%ld' % (self.name, offset, length))

            if ret != <ssize_t>length:
                _PyBytes_Resize(&ret_s, ret)

            return <object>ret_s
        finally:
            # We DECREF unconditionally: the cast to object above will have
            # INCREFed if necessary. This also takes care of exceptions,
            # including if _PyString_Resize fails (that will free the string
            # itself and set ret_s to NULL, hence XDECREF).
            ref.Py_XDECREF(ret_s)

    @requires_not_closed
    def diff_iterate(self, offset, length, from_snapshot, iterate_cb,
                     include_parent = True, whole_object = False):
        """
        Iterate over the changed extents of an image.

        This will call iterate_cb with three arguments:

        (offset, length, exists)

        where the changed extent starts at offset bytes, continues for
        length bytes, and is full of data (if exists is True) or zeroes
        (if exists is False).

        If from_snapshot is None, it is interpreted as the beginning
        of time and this generates all allocated extents.

        The end version is whatever is currently selected (via set_snap)
        for the image.

        iterate_cb may raise an exception, which will abort the diff and will be
        propagated to the caller.

        Raises :class:`InvalidArgument` if from_snapshot is after
        the currently set snapshot.

        Raises :class:`ImageNotFound` if from_snapshot is not the name
        of a snapshot of the image.

        :param offset: start offset in bytes
        :type offset: int
        :param length: size of region to report on, in bytes
        :type length: int
        :param from_snapshot: starting snapshot name, or None
        :type from_snapshot: str or None
        :param iterate_cb: function to call for each extent
        :type iterate_cb: function acception arguments for offset,
                           length, and exists
        :param include_parent: True if full history diff should include parent
        :type include_parent: bool
        :param whole_object: True if diff extents should cover whole object
        :type whole_object: bool
        :raises: :class:`InvalidArgument`, :class:`IOError`,
                 :class:`ImageNotFound`
        """
        from_snapshot = cstr(from_snapshot, 'from_snapshot', opt=True)
        cdef:
            char *_from_snapshot = opt_str(from_snapshot)
            uint64_t _offset = offset, _length = length
            uint8_t _include_parent = include_parent
            uint8_t _whole_object = whole_object
        with nogil:
            ret = rbd_diff_iterate2(self.image, _from_snapshot, _offset,
                                    _length, _include_parent, _whole_object,
                                    &diff_iterate_cb, <void *>iterate_cb)
        if ret < 0:
            msg = 'error generating diff from snapshot %s' % from_snapshot
            raise make_ex(ret, msg)

    @requires_not_closed
    def write(self, data, offset, fadvise_flags=0):
        """
        Write data to the image. Raises :class:`InvalidArgument` if
        part of the write would fall outside the image.

        :param data: the data to be written
        :type data: bytes
        :param offset: where to start writing data
        :type offset: int
        :param fadvise_flags: fadvise flags for this write
        :type fadvise_flags: int
        :returns: int - the number of bytes written
        :raises: :class:`IncompleteWriteError`, :class:`LogicError`,
                 :class:`InvalidArgument`, :class:`IOError`
        """
        if not isinstance(data, bytes):
            raise TypeError('data must be a byte string')
        cdef:
            uint64_t _offset = offset, length = len(data)
            char *_data = data
            int _fadvise_flags = fadvise_flags
        with nogil:
            ret = rbd_write2(self.image, _offset, length, _data, _fadvise_flags)

        if ret == <ssize_t>length:
            return ret
        elif ret < 0:
            raise make_ex(ret, "error writing to %s" % self.name)
        elif ret < <ssize_t>length:
            raise IncompleteWriteError("Wrote only %ld out of %ld bytes" % (ret, length))
        else:
            raise LogicError("logic error: rbd_write(%s) \
returned %d, but %d was the maximum number of bytes it could have \
written." % (self.name, ret, length))

    @requires_not_closed
    def discard(self, offset, length):
        """
        Trim the range from the image. It will be logically filled
        with zeroes.
        """
        cdef uint64_t _offset = offset, _length = length
        with nogil:
            ret = rbd_discard(self.image, _offset, _length)
        if ret < 0:
            msg = 'error discarding region %d~%d' % (offset, length)
            raise make_ex(ret, msg)

    @requires_not_closed
    def write_zeroes(self, offset, length, zero_flags = 0):
        """
        Zero the range from the image. By default it will attempt to
        discard/unmap as much space as possible but any unaligned
        extent segments will still be zeroed.
        """
        cdef:
            uint64_t _offset = offset, _length = length
            int _zero_flags = zero_flags
        with nogil:
            ret = rbd_write_zeroes(self.image, _offset, _length,
                                   _zero_flags, 0)
        if ret < 0:
            msg = 'error zeroing region %d~%d' % (offset, length)
            raise make_ex(ret, msg)

    @requires_not_closed
    def flush(self):
        """
        Block until all writes are fully flushed if caching is enabled.
        """
        with nogil:
            ret = rbd_flush(self.image)
        if ret < 0:
            raise make_ex(ret, 'error flushing image')

    @requires_not_closed
    def invalidate_cache(self):
        """
        Drop any cached data for the image.
        """
        with nogil:
            ret = rbd_invalidate_cache(self.image)
        if ret < 0:
            raise make_ex(ret, 'error invalidating cache')

    @requires_not_closed
    def stripe_unit(self):
        """
        Return the stripe unit used for the image.
        """
        cdef uint64_t stripe_unit
        with nogil:
            ret = rbd_get_stripe_unit(self.image, &stripe_unit)
        if ret != 0:
            raise make_ex(ret, 'error getting stripe unit for image %s' % (self.name))
        return stripe_unit

    @requires_not_closed
    def stripe_count(self):
        """
        Return the stripe count used for the image.
        """
        cdef uint64_t stripe_count
        with nogil:
            ret = rbd_get_stripe_count(self.image, &stripe_count)
        if ret != 0:
            raise make_ex(ret, 'error getting stripe count for image %s' % (self.name))
        return stripe_count

    @requires_not_closed
    def create_timestamp(self):
        """
        Return the create timestamp for the image.
        """
        cdef:
            timespec timestamp
        with nogil:
            ret = rbd_get_create_timestamp(self.image, &timestamp)
        if ret != 0:
            raise make_ex(ret, 'error getting create timestamp for image: %s' % (self.name))
        return datetime.utcfromtimestamp(timestamp.tv_sec)

    @requires_not_closed
    def access_timestamp(self):
        """
        Return the access timestamp for the image.
        """
        cdef:
            timespec timestamp
        with nogil:
            ret = rbd_get_access_timestamp(self.image, &timestamp)
        if ret != 0:
            raise make_ex(ret, 'error getting access timestamp for image: %s' % (self.name))
        return datetime.fromtimestamp(timestamp.tv_sec)

    @requires_not_closed
    def modify_timestamp(self):
        """
        Return the modify timestamp for the image.
        """
        cdef:
            timespec timestamp
        with nogil:
            ret = rbd_get_modify_timestamp(self.image, &timestamp)
        if ret != 0:
            raise make_ex(ret, 'error getting modify timestamp for image: %s' % (self.name))
        return datetime.fromtimestamp(timestamp.tv_sec)

    @requires_not_closed
    def flatten(self, on_progress=None):
        """
        Flatten clone image (copy all blocks from parent to child)
        :param on_progress: optional progress callback function
        :type on_progress: callback function
        """
        cdef:
            librbd_progress_fn_t _prog_cb = &no_op_progress_callback
            void *_prog_arg = NULL
        if on_progress:
            _prog_cb = &progress_callback
            _prog_arg = <void *>on_progress
        with nogil:
            ret = rbd_flatten_with_progress(self.image, _prog_cb, _prog_arg)
        if ret < 0:
            raise make_ex(ret, "error flattening %s" % self.name)

    @requires_not_closed
    def sparsify(self, sparse_size):
        """
        Reclaim space for zeroed image extents
        """
        cdef:
            size_t _sparse_size = sparse_size
        with nogil:
            ret = rbd_sparsify(self.image, _sparse_size)
        if ret < 0:
            raise make_ex(ret, "error sparsifying %s" % self.name)

    @requires_not_closed
    def rebuild_object_map(self):
        """
        Rebuild the object map for the image HEAD or currently set snapshot
        """
        cdef librbd_progress_fn_t prog_cb = &no_op_progress_callback
        with nogil:
            ret = rbd_rebuild_object_map(self.image, prog_cb, NULL)
        if ret < 0:
            raise make_ex(ret, "error rebuilding object map %s" % self.name)

    @requires_not_closed
    def list_children(self):
        """
        List children of the currently set snapshot (set via set_snap()).

        :returns: list - a list of (pool name, image name) tuples
        """
        cdef:
            rbd_linked_image_spec_t *children = NULL
            size_t num_children = 10

        try:
            while True:
                children = <rbd_linked_image_spec_t*>realloc_chk(
                    children, num_children * sizeof(rbd_linked_image_spec_t))
                with nogil:
                    ret = rbd_list_children3(self.image, children,
                                             &num_children)
                if ret >= 0:
                    break
                elif ret != -errno.ERANGE:
                    raise make_ex(ret, 'error listing children.')

            return [(decode_cstr(x.pool_name), decode_cstr(x.image_name)) for x
                    in children[:num_children] if not x.trash]
        finally:
            if children:
                rbd_linked_image_spec_list_cleanup(children, num_children)
                free(children)

    @requires_not_closed
    def list_children2(self):
        """
        Iterate over the children of the image or its snapshot.

        :returns: :class:`ChildIterator`
        """
        return ChildIterator(self)

    @requires_not_closed
    def list_descendants(self):
        """
        Iterate over the descendants of the image.

        :returns: :class:`ChildIterator`
        """
        return ChildIterator(self, True)

    @requires_not_closed
    def list_lockers(self):
        """
        List clients that have locked the image and information
        about the lock.

        :returns: dict - contains the following keys:

                  * ``tag`` - the tag associated with the lock (every
                    additional locker must use the same tag)
                  * ``exclusive`` - boolean indicating whether the
                     lock is exclusive or shared
                  * ``lockers`` - a list of (client, cookie, address)
                    tuples
        """
        cdef:
            size_t clients_size = 512, cookies_size = 512
            size_t addrs_size = 512, tag_size = 512
            int exclusive = 0
            char *c_clients = NULL
            char *c_cookies = NULL
            char *c_addrs = NULL
            char *c_tag = NULL

        try:
            while True:
                c_clients = <char *>realloc_chk(c_clients, clients_size)
                c_cookies = <char *>realloc_chk(c_cookies, cookies_size)
                c_addrs = <char *>realloc_chk(c_addrs, addrs_size)
                c_tag = <char *>realloc_chk(c_tag, tag_size)
                with nogil:
                    ret = rbd_list_lockers(self.image, &exclusive,
                                           c_tag, &tag_size,
                                           c_clients, &clients_size,
                                           c_cookies, &cookies_size,
                                           c_addrs, &addrs_size)
                if ret >= 0:
                    break
                elif ret != -errno.ERANGE:
                    raise make_ex(ret, 'error listing images')
            if ret == 0:
                return []
            clients = map(decode_cstr, c_clients[:clients_size - 1].split(b'\0'))
            cookies = map(decode_cstr, c_cookies[:cookies_size - 1].split(b'\0'))
            addrs = map(decode_cstr, c_addrs[:addrs_size - 1].split(b'\0'))
            return {
                'tag'       : decode_cstr(c_tag),
                'exclusive' : exclusive == 1,
                'lockers'   : list(zip(clients, cookies, addrs)),
                }
        finally:
            free(c_clients)
            free(c_cookies)
            free(c_addrs)
            free(c_tag)

    @requires_not_closed
    def lock_acquire(self, lock_mode):
        """
        Acquire a managed lock on the image.

        :param lock_mode: lock mode to set
        :type lock_mode: int
        :raises: :class:`ImageBusy` if the lock could not be acquired
        """
        cdef:
            rbd_lock_mode_t _lock_mode = lock_mode
        with nogil:
            ret = rbd_lock_acquire(self.image, _lock_mode)
        if ret < 0:
            raise make_ex(ret, 'error acquiring lock on image')

    @requires_not_closed
    def lock_release(self):
        """
        Release a managed lock on the image that was previously acquired.
        """
        with nogil:
            ret = rbd_lock_release(self.image)
        if ret < 0:
            raise make_ex(ret, 'error releasing lock on image')

    @requires_not_closed
    def lock_get_owners(self):
        """
        Iterate over the lock owners of an image.

        :returns: :class:`LockOwnerIterator`
        """
        return LockOwnerIterator(self)

    @requires_not_closed
    def lock_break(self, lock_mode, lock_owner):
        """
        Break the image lock held by a another client.

        :param lock_owner: the owner of the lock to break
        :type lock_owner: str
        """
        lock_owner = cstr(lock_owner, 'lock_owner')
        cdef:
            rbd_lock_mode_t _lock_mode = lock_mode
            char *_lock_owner = lock_owner
        with nogil:
            ret = rbd_lock_break(self.image, _lock_mode, _lock_owner)
        if ret < 0:
            raise make_ex(ret, 'error breaking lock on image')

    @requires_not_closed
    def lock_exclusive(self, cookie):
        """
        Take an exclusive lock on the image.

        :raises: :class:`ImageBusy` if a different client or cookie locked it
                 :class:`ImageExists` if the same client and cookie locked it
        """
        cookie = cstr(cookie, 'cookie')
        cdef char *_cookie = cookie
        with nogil:
            ret = rbd_lock_exclusive(self.image, _cookie)
        if ret < 0:
            raise make_ex(ret, 'error acquiring exclusive lock on image')

    @requires_not_closed
    def lock_shared(self, cookie, tag):
        """
        Take a shared lock on the image. The tag must match
        that of the existing lockers, if any.

        :raises: :class:`ImageBusy` if a different client or cookie locked it
                 :class:`ImageExists` if the same client and cookie locked it
        """
        cookie = cstr(cookie, 'cookie')
        tag = cstr(tag, 'tag')
        cdef:
            char *_cookie = cookie
            char *_tag = tag
        with nogil:
            ret = rbd_lock_shared(self.image, _cookie, _tag)
        if ret < 0:
            raise make_ex(ret, 'error acquiring shared lock on image')

    @requires_not_closed
    def unlock(self, cookie):
        """
        Release a lock on the image that was locked by this rados client.
        """
        cookie = cstr(cookie, 'cookie')
        cdef char *_cookie = cookie
        with nogil:
            ret = rbd_unlock(self.image, _cookie)
        if ret < 0:
            raise make_ex(ret, 'error unlocking image')

    @requires_not_closed
    def break_lock(self, client, cookie):
        """
        Release a lock held by another rados client.
        """
        client = cstr(client, 'client')
        cookie = cstr(cookie, 'cookie')
        cdef:
            char *_client = client
            char *_cookie = cookie
        with nogil:
            ret = rbd_break_lock(self.image, _client, _cookie)
        if ret < 0:
            raise make_ex(ret, 'error unlocking image')

    @requires_not_closed
    def mirror_image_enable(self, mode=RBD_MIRROR_IMAGE_MODE_JOURNAL):
        """
        Enable mirroring for the image.
        """
        cdef rbd_mirror_image_mode_t c_mode = mode
        with nogil:
            ret = rbd_mirror_image_enable2(self.image, c_mode)
        if ret < 0:
            raise make_ex(ret, 'error enabling mirroring for image %s' % self.name)

    @requires_not_closed
    def mirror_image_disable(self, force):
        """
        Disable mirroring for the image.

        :param force: force disabling
        :type force: bool
        """
        cdef bint c_force = force
        with nogil:
            ret = rbd_mirror_image_disable(self.image, c_force)
        if ret < 0:
            raise make_ex(ret, 'error disabling mirroring for image %s' % self.name)

    @requires_not_closed
    def mirror_image_promote(self, force):
        """
        Promote the image to primary for mirroring.

        :param force: force promoting
        :type force: bool
        """
        cdef bint c_force = force
        with nogil:
            ret = rbd_mirror_image_promote(self.image, c_force)
        if ret < 0:
            raise make_ex(ret, 'error promoting image %s to primary' % self.name)

    @requires_not_closed
    def mirror_image_demote(self):
        """
        Demote the image to secondary for mirroring.
        """
        with nogil:
            ret = rbd_mirror_image_demote(self.image)
        if ret < 0:
            raise make_ex(ret, 'error demoting image %s to secondary' % self.name)

    @requires_not_closed
    def mirror_image_resync(self):
        """
        Flag the image to resync.
        """
        with nogil:
            ret = rbd_mirror_image_resync(self.image)
        if ret < 0:
            raise make_ex(ret, 'error to resync image %s' % self.name)

    @requires_not_closed
    def mirror_image_create_snapshot(self, flags=0):
        """
        Create mirror snapshot.

        :param flags: create snapshot flags
        :type flags: int
        :returns: int - the snapshot Id
        """
        cdef:
            uint32_t _flags = flags
            uint64_t snap_id
        with nogil:
            ret = rbd_mirror_image_create_snapshot2(self.image, _flags,
                                                    &snap_id)
        if ret < 0:
            raise make_ex(ret, 'error creating mirror snapshot for image %s' %
                          self.name)
        return snap_id

    @requires_not_closed
    def aio_mirror_image_create_snapshot(self, flags, oncomplete):
        """
        Asynchronously create mirror snapshot.

        Raises :class:`InvalidArgument` if the image is not in mirror
        snapshot mode.

        oncomplete will be called with the created snap ID as
        well as the completion:

        oncomplete(completion, snap_id)

        :param flags: create snapshot flags
        :type flags: int
        :param oncomplete: what to do when the read is complete
        :type oncomplete: completion
        :returns: :class:`Completion` - the completion object
        :raises: :class:`InvalidArgument`
        """
        cdef:
            uint32_t _flags = flags
            Completion completion

        def oncomplete_(completion_v):
            cdef Completion _completion_v = completion_v
            return_value = _completion_v.get_return_value()
            snap_id = <object>(<uint64_t *>_completion_v.buf)[0] \
                if return_value >= 0 else None
            return oncomplete(_completion_v, snap_id)

        completion = self.__get_completion(oncomplete_)
        completion.buf = PyBytes_FromStringAndSize(NULL, sizeof(uint64_t))
        try:
            completion.__persist()
            with nogil:
                ret = rbd_aio_mirror_image_create_snapshot(self.image, _flags,
                                                           <uint64_t *>completion.buf,
                                                           completion.rbd_comp)
            if ret < 0:
                raise make_ex(ret, 'error creating mirror snapshot for image %s' %
                              self.name)
        except:
            completion.__unpersist()
            raise

        return completion

    @requires_not_closed
    def mirror_image_get_info(self):
        """
        Get mirror info for the image.

        :returns: dict - contains the following keys:

            * ``global_id`` (str) - image global id

            * ``state`` (int) - mirror state

            * ``primary`` (bool) - is image primary
        """
        cdef rbd_mirror_image_info_t c_info
        with nogil:
            ret = rbd_mirror_image_get_info(self.image, &c_info, sizeof(c_info))
        if ret != 0:
            raise make_ex(ret, 'error getting mirror info for image %s' % self.name)
        info = {
            'global_id' : decode_cstr(c_info.global_id),
            'state'     : int(c_info.state),
            'primary'   : c_info.primary,
            }
        rbd_mirror_image_get_info_cleanup(&c_info)
        return info

    @requires_not_closed
    def aio_mirror_image_get_info(self,  oncomplete):
        """
         Asynchronously get mirror info for the image.

        oncomplete will be called with the returned info as
        well as the completion:

        oncomplete(completion, info)

        :param oncomplete: what to do when get info is complete
        :type oncomplete: completion
        :returns: :class:`Completion` - the completion object
        """
        cdef:
            Completion completion

        def oncomplete_(completion_v):
            cdef:
                Completion _completion_v = completion_v
                rbd_mirror_image_info_t *c_info
            return_value = _completion_v.get_return_value()
            if return_value == 0:
                c_info = <rbd_mirror_image_info_t *>_completion_v.buf
                info = {
                    'global_id' : decode_cstr(c_info[0].global_id),
                    'state'     : int(c_info[0].state),
                    'primary'   : c_info[0].primary,
                }
                rbd_mirror_image_get_info_cleanup(c_info)
            else:
                info = None
            return oncomplete(_completion_v, info)

        completion = self.__get_completion(oncomplete_)
        completion.buf = PyBytes_FromStringAndSize(
            NULL, sizeof(rbd_mirror_image_info_t))
        try:
            completion.__persist()
            with nogil:
                ret = rbd_aio_mirror_image_get_info(
                    self.image, <rbd_mirror_image_info_t *>completion.buf,
                    sizeof(rbd_mirror_image_info_t), completion.rbd_comp)
            if ret != 0:
                raise make_ex(
                    ret, 'error getting mirror info for image %s' % self.name)
        except:
            completion.__unpersist()
            raise

        return completion

    @requires_not_closed
    def mirror_image_get_mode(self):
        """
        Get mirror mode for the image.

        :returns: int - mirror mode
        """
        cdef rbd_mirror_image_mode_t c_mode
        with nogil:
            ret = rbd_mirror_image_get_mode(self.image, &c_mode)
        if ret != 0:
            raise make_ex(ret, 'error getting mirror mode for image %s' % self.name)
        return int(c_mode)

    @requires_not_closed
    def aio_mirror_image_get_mode(self,  oncomplete):
        """
         Asynchronously get mirror mode for the image.

        oncomplete will be called with the returned mode as
        well as the completion:

        oncomplete(completion, mode)

        :param oncomplete: what to do when get info is complete
        :type oncomplete: completion
        :returns: :class:`Completion` - the completion object
        """
        cdef:
            Completion completion

        def oncomplete_(completion_v):
            cdef Completion _completion_v = completion_v
            return_value = _completion_v.get_return_value()
            mode = int((<rbd_mirror_image_mode_t *>_completion_v.buf)[0]) \
                if return_value >= 0 else None
            return oncomplete(_completion_v, mode)

        completion = self.__get_completion(oncomplete_)
        completion.buf = PyBytes_FromStringAndSize(
            NULL, sizeof(rbd_mirror_image_mode_t))
        try:
            completion.__persist()
            with nogil:
                ret = rbd_aio_mirror_image_get_mode(
                    self.image, <rbd_mirror_image_mode_t *>completion.buf,
                    completion.rbd_comp)
            if ret != 0:
                raise make_ex(
                    ret, 'error getting mirror mode for image %s' % self.name)
        except:
            completion.__unpersist()
            raise

        return completion

    @requires_not_closed
    def mirror_image_get_status(self):
        """
        Get mirror status for the image.

        :returns: dict - contains the following keys:

            * ``name`` (str) - mirror image name

            * ``id`` (str) - mirror image id

            * ``info`` (dict) - mirror image info

            * ``state`` (int) - status mirror state

            * ``description`` (str) - status description

            * ``last_update`` (datetime) - last status update time

            * ``up`` (bool) - is mirroring agent up

            * ``remote_statuses`` (array) -

            *   ``mirror_uuid`` (str) - remote mirror uuid

            *   ``state`` (int) - status mirror state

            *   ``description`` (str) - status description

            *   ``last_update`` (datetime) - last status update time

            *   ``up`` (bool) - is mirroring agent up
        """
        cdef:
            rbd_mirror_image_site_status_t *s_status
            rbd_mirror_image_global_status_t c_status
        try:
            with nogil:
                ret = rbd_mirror_image_get_global_status(self.image, &c_status,
                                                         sizeof(c_status))
            if ret != 0:
                raise make_ex(ret, 'error getting mirror status for image %s' % self.name)

            local_status = None
            site_statuses = []
            for i in range(c_status.site_statuses_count):
                s_status = &c_status.site_statuses[i]
                site_status = {
                    'state'       : s_status.state,
                    'description' : decode_cstr(s_status.description),
                    'last_update' : datetime.utcfromtimestamp(s_status.last_update),
                    'up'          : s_status.up,
                    }
                mirror_uuid = decode_cstr(s_status.mirror_uuid)
                if mirror_uuid == '':
                    local_status = site_status
                else:
                    site_status['mirror_uuid'] = mirror_uuid
                    site_statuses.append(site_status)
            status = {
                'name': decode_cstr(c_status.name),
                'id'  : self.id(),
                'info': {
                    'global_id' : decode_cstr(c_status.info.global_id),
                    'state'     : int(c_status.info.state),
                    'primary'   : c_status.info.primary,
                    },
                'remote_statuses': site_statuses,
                }
            if local_status:
                status.update(local_status)
        finally:
            rbd_mirror_image_global_status_cleanup(&c_status)
        return status

    @requires_not_closed
    def mirror_image_get_instance_id(self):
        """
        Get mirror instance id for the image.

        :returns: str - instance id
        """
        cdef:
            int ret = -errno.ERANGE
            size_t size = 32
            char *instance_id = NULL
        try:
            while ret == -errno.ERANGE and size <= 4096:
                instance_id =  <char *>realloc_chk(instance_id, size)
                with nogil:
                    ret = rbd_mirror_image_get_instance_id(self.image,
                                                           instance_id, &size)
            if ret != 0:
                raise make_ex(ret,
                              'error getting mirror instance id for image %s' %
                              self.name)
            return decode_cstr(instance_id)
        finally:
            free(instance_id)

    @requires_not_closed
    def aio_read(self, offset, length, oncomplete, fadvise_flags=0):
        """
        Asynchronously read data from the image

        Raises :class:`InvalidArgument` if part of the range specified is
        outside the image.

        oncomplete will be called with the returned read value as
        well as the completion:

        oncomplete(completion, data_read)

        :param offset: the offset to start reading at
        :type offset: int
        :param length: how many bytes to read
        :type length: int
        :param oncomplete: what to do when the read is complete
        :type oncomplete: completion
        :param fadvise_flags: fadvise flags for this read
        :type fadvise_flags: int
        :returns: :class:`Completion` - the completion object
        :raises: :class:`InvalidArgument`, :class:`IOError`
        """
        cdef:
            char *ret_buf
            uint64_t _offset = offset
            size_t _length = length
            int _fadvise_flags = fadvise_flags
            Completion completion

        def oncomplete_(completion_v):
            cdef Completion _completion_v = completion_v
            return_value = _completion_v.get_return_value()
            if return_value > 0 and return_value != length:
                _PyBytes_Resize(&_completion_v.buf, return_value)
            return oncomplete(_completion_v, <object>_completion_v.buf if return_value >= 0 else None)

        completion = self.__get_completion(oncomplete_)
        completion.buf = PyBytes_FromStringAndSize(NULL, length)
        ret_buf = PyBytes_AsString(completion.buf)
        try:
            completion.__persist()
            with nogil:
                ret = rbd_aio_read2(self.image, _offset, _length, ret_buf,
                                    completion.rbd_comp, _fadvise_flags)
            if ret < 0:
                raise make_ex(ret, 'error reading %s %ld~%ld' %
                              (self.name, offset, length))
        except:
            completion.__unpersist()
            raise

        return completion

    @requires_not_closed
    def aio_write(self, data, offset, oncomplete, fadvise_flags=0):
        """
        Asynchronously write data to the image

        Raises :class:`InvalidArgument` if part of the write would fall outside
        the image.

        oncomplete will be called with the completion:

        oncomplete(completion)

        :param data: the data to be written
        :type data: bytes
        :param offset: the offset to start writing at
        :type offset: int
        :param oncomplete: what to do when the write is complete
        :type oncomplete: completion
        :param fadvise_flags: fadvise flags for this write
        :type fadvise_flags: int
        :returns: :class:`Completion` - the completion object
        :raises: :class:`InvalidArgument`, :class:`IOError`
        """
        cdef:
            uint64_t _offset = offset
            char *_data = data
            size_t _length = len(data)
            int _fadvise_flags = fadvise_flags
            Completion completion

        completion = self.__get_completion(oncomplete)
        try:
            completion.__persist()
            with nogil:
                ret = rbd_aio_write2(self.image, _offset, _length, _data,
                                     completion.rbd_comp, _fadvise_flags)
            if ret < 0:
                raise make_ex(ret, 'error writing %s %ld~%ld' %
                              (self.name, offset, _length))
        except:
            completion.__unpersist()
            raise

        return completion

    @requires_not_closed
    def aio_discard(self, offset, length, oncomplete):
        """
        Asynchronously trim the range from the image. It will be logically
        filled with zeroes.
        """
        cdef:
            uint64_t _offset = offset
            size_t _length = length
            Completion completion

        completion = self.__get_completion(oncomplete)
        try:
            completion.__persist()
            with nogil:
                ret = rbd_aio_discard(self.image, _offset, _length,
                                     completion.rbd_comp)
            if ret < 0:
                raise make_ex(ret, 'error discarding %s %ld~%ld' %
                              (self.name, offset, _length))
        except:
            completion.__unpersist()
            raise

        return completion

    @requires_not_closed
    def aio_write_zeroes(self, offset, length, oncomplete, zero_flags = 0):
        """
        Asynchronously Zero the range from the image. By default it will attempt
        to discard/unmap as much space as possible but any unaligned extent
        segments will still be zeroed.
        """
        cdef:
            uint64_t _offset = offset
            size_t _length = length
            int _zero_flags = zero_flags
            Completion completion

        completion = self.__get_completion(oncomplete)
        try:
            completion.__persist()
            with nogil:
                ret = rbd_aio_write_zeroes(self.image, _offset, _length,
                                           completion.rbd_comp, _zero_flags, 0)
            if ret < 0:
                raise make_ex(ret, 'error zeroing %s %ld~%ld' %
                              (self.name, offset, length))
        except:
            completion.__unpersist()
            raise

        return completion

    @requires_not_closed
    def aio_flush(self, oncomplete):
        """
        Asynchronously wait until all writes are fully flushed if caching is
        enabled.
        """
        cdef Completion completion = self.__get_completion(oncomplete)
        try:
            completion.__persist()
            with nogil:
                ret = rbd_aio_flush(self.image, completion.rbd_comp)
            if ret < 0:
                raise make_ex(ret, 'error flushing')
        except:
            completion.__unpersist()
            raise

        return completion

    @requires_not_closed
    def metadata_get(self, key):
        """
        Get image metadata for the given key.

        :param key: metadata key
        :type key: str
        :returns: str - metadata value
        """
        key = cstr(key, 'key')
        cdef:
            char *_key = key
            size_t size = 4096
            char *value = NULL
            int ret
        try:
            while True:
                value = <char *>realloc_chk(value, size)
                with nogil:
                    ret = rbd_metadata_get(self.image, _key, value, &size)
                if ret != -errno.ERANGE:
                    break
            if ret == -errno.ENOENT:
                raise KeyError('no metadata %s for image %s' % (key, self.name))
            if ret != 0:
                raise make_ex(ret, 'error getting metadata %s for image %s' %
                              (key, self.name))
            return decode_cstr(value)
        finally:
            free(value)

    @requires_not_closed
    def metadata_set(self, key, value):
        """
        Set image metadata for the given key.

        :param key: metadata key
        :type key: str
        :param value: metadata value
        :type value: str
        """
        key = cstr(key, 'key')
        value = cstr(value, 'value')
        cdef:
            char *_key = key
            char *_value = value
        with nogil:
            ret = rbd_metadata_set(self.image, _key, _value)

        if ret != 0:
            raise make_ex(ret, 'error setting metadata %s for image %s' %
                          (key, self.name))

    @requires_not_closed
    def metadata_remove(self, key):
        """
        Remove image metadata for the given key.

        :param key: metadata key
        :type key: str
        """
        key = cstr(key, 'key')
        cdef:
            char *_key = key
        with nogil:
            ret = rbd_metadata_remove(self.image, _key)

        if ret == -errno.ENOENT:
            raise KeyError('no metadata %s for image %s' % (key, self.name))
        if ret != 0:
            raise make_ex(ret, 'error removing metadata %s for image %s' %
                          (key, self.name))

    @requires_not_closed
    def metadata_list(self):
        """
        List image metadata.

        :returns: :class:`MetadataIterator`
        """
        return MetadataIterator(self)

    @requires_not_closed
    def watchers_list(self):
        """
        List image watchers.

        :returns: :class:`WatcherIterator`
        """
        return WatcherIterator(self)

    @requires_not_closed
    def config_list(self):
        """
        List image-level config overrides.

        :returns: :class:`ConfigImageIterator`
        """
        return ConfigImageIterator(self)

    @requires_not_closed
    def config_set(self, key, value):
        """
        Set an image-level configuration override.

        :param key: key
        :type key: str
        :param value: value
        :type value: str
        """
        conf_key = 'conf_' + key
        conf_key = cstr(conf_key, 'key')
        value = cstr(value, 'value')
        cdef:
            char *_key = conf_key
            char *_value = value
        with nogil:
            ret = rbd_metadata_set(self.image, _key, _value)

        if ret != 0:
            raise make_ex(ret, 'error setting config %s for image %s' %
                          (key, self.name))

    @requires_not_closed
    def config_get(self, key):
        """
        Get an image-level configuration override.

        :param key: key
        :type key: str
        :returns: str - value
        """
        conf_key = 'conf_' + key
        conf_key = cstr(conf_key, 'key')
        cdef:
            char *_key = conf_key
            size_t size = 4096
            char *value = NULL
            int ret
        try:
            while True:
                value = <char *>realloc_chk(value, size)
                with nogil:
                    ret = rbd_metadata_get(self.image, _key, value, &size)
                if ret != -errno.ERANGE:
                    break
            if ret == -errno.ENOENT:
                raise KeyError('no config %s for image %s' % (key, self.name))
            if ret != 0:
                raise make_ex(ret, 'error getting config %s for image %s' %
                              (key, self.name))
            return decode_cstr(value)
        finally:
            free(value)

    @requires_not_closed
    def config_remove(self, key):
        """
        Remove an image-level configuration override.

        :param key: key
        :type key: str
        """
        conf_key = 'conf_' + key
        conf_key = cstr(conf_key, 'key')
        cdef:
            char *_key = conf_key
        with nogil:
            ret = rbd_metadata_remove(self.image, _key)

        if ret == -errno.ENOENT:
            raise KeyError('no config %s for image %s' % (key, self.name))
        if ret != 0:
            raise make_ex(ret, 'error removing config %s for image %s' %
                          (key, self.name))

    @requires_not_closed
    def snap_get_namespace_type(self, snap_id):
        """
        Get the snapshot namespace type.
        :param snap_id: the snapshot id of a snap shot
        :type key: int
        """
        cdef:
            rbd_snap_namespace_type_t namespace_type
            uint64_t _snap_id = snap_id
        with nogil:
            ret = rbd_snap_get_namespace_type(self.image, _snap_id, &namespace_type)
        if ret != 0:
            raise make_ex(ret, 'error getting snapshot namespace type for image: %s, snap_id: %d' % (self.name, snap_id))

        return namespace_type

    @requires_not_closed
    def snap_get_group_namespace(self, snap_id):
        """
        get the group namespace details.
        :param snap_id: the snapshot id of the group snapshot
        :type key: int
        :returns: dict - contains the following keys:

            * ``pool`` (int) - pool id

            * ``name`` (str) - group name

            * ``snap_name`` (str) - group snap name
        """
        cdef:
            rbd_snap_group_namespace_t group_namespace
            uint64_t _snap_id = snap_id
        with nogil:
            ret = rbd_snap_get_group_namespace(self.image, _snap_id,
                                               &group_namespace,
                                               sizeof(rbd_snap_group_namespace_t))
        if ret != 0:
            raise make_ex(ret, 'error getting snapshot group namespace for image: %s, snap_id: %d' % (self.name, snap_id))

        info = {
                'pool' : group_namespace.group_pool,
                'name' : decode_cstr(group_namespace.group_name),
                'snap_name' : decode_cstr(group_namespace.group_snap_name)
            }
        rbd_snap_group_namespace_cleanup(&group_namespace,
                                         sizeof(rbd_snap_group_namespace_t))
        return info

    @requires_not_closed
    def snap_get_trash_namespace(self, snap_id):
        """
        get the trash namespace details.
        :param snap_id: the snapshot id of the trash snapshot
        :type key: int
        :returns: dict - contains the following keys:

            * ``original_name`` (str) - original snap name
        """
        cdef:
            uint64_t _snap_id = snap_id
            size_t _size = 512
            char *_name = NULL
        try:
            while True:
                _name = <char*>realloc_chk(_name, _size);
                with nogil:
                    ret = rbd_snap_get_trash_namespace(self.image, _snap_id,
                                                       _name, _size)
                if ret >= 0:
                    break
                elif ret != -errno.ERANGE:
                    raise make_ex(ret, 'error getting snapshot trash '
                                       'namespace image: %s, snap_id: %d' % (self.name, snap_id))
            return {
                    'original_name' : decode_cstr(_name)
                }
        finally:
            free(_name)

    @requires_not_closed
    def snap_get_mirror_namespace(self, snap_id):
        """
        get the mirror namespace details.
        :param snap_id: the snapshot id of the mirror snapshot
        :type key: int
        :returns: dict - contains the following keys:

            * ``state`` (int) - the snapshot state

            * ``mirror_peer_uuids`` (list) - mirror peer uuids

            * ``complete`` (bool) - True if snapshot is complete

            * ``primary_mirror_uuid`` (str) - primary mirror uuid

            * ``primary_snap_id`` (int) - primary snapshot Id

            *  ``last_copied_object_number`` (int) - last copied object number
        """
        cdef:
            rbd_snap_mirror_namespace_t sn
            uint64_t _snap_id = snap_id
        with nogil:
            ret = rbd_snap_get_mirror_namespace(
                self.image, _snap_id, &sn,
                sizeof(rbd_snap_mirror_namespace_t))
        if ret != 0:
            raise make_ex(ret, 'error getting snapshot mirror '
                               'namespace for image: %s, snap_id: %d' %
                               (self.name, snap_id))
        uuids = []
        cdef char *p = sn.mirror_peer_uuids
        for i in range(sn.mirror_peer_uuids_count):
            uuid = decode_cstr(p)
            uuids.append(uuid)
            p += len(uuid) + 1
        info = {
                'state' : sn.state,
                'mirror_peer_uuids' : uuids,
                'complete' : sn.complete,
                'primary_mirror_uuid' : decode_cstr(sn.primary_mirror_uuid),
                'primary_snap_id' : sn.primary_snap_id,
                'last_copied_object_number' : sn.last_copied_object_number,
            }
        rbd_snap_mirror_namespace_cleanup(
            &sn, sizeof(rbd_snap_mirror_namespace_t))
        return info

    @requires_not_closed
    def encryption_format(self, format, passphrase,
                          cipher_alg=RBD_ENCRYPTION_ALGORITHM_AES256):
        passphrase = cstr(passphrase, "passphrase")
        cdef rbd_encryption_format_t _format = format
        cdef rbd_encryption_luks1_format_options_t _luks1_opts
        cdef rbd_encryption_luks2_format_options_t _luks2_opts
        cdef char* _passphrase = passphrase

        if (format == RBD_ENCRYPTION_FORMAT_LUKS1):
            _luks1_opts.alg = cipher_alg
            _luks1_opts.passphrase = _passphrase
            _luks1_opts.passphrase_size = len(passphrase)
            with nogil:
                ret = rbd_encryption_format(self.image, _format, &_luks1_opts,
                                            sizeof(_luks1_opts))
            if ret != 0:
                raise make_ex(
                    ret,
                    'error formatting image %s with format luks1' % self.name)
        elif (format == RBD_ENCRYPTION_FORMAT_LUKS2):
            _luks2_opts.alg = cipher_alg
            _luks2_opts.passphrase = _passphrase
            _luks2_opts.passphrase_size = len(passphrase)
            with nogil:
                ret = rbd_encryption_format(self.image, _format, &_luks2_opts,
                                            sizeof(_luks2_opts))
            if ret != 0:
                raise make_ex(
                    ret,
                    'error formatting image %s with format luks2' % self.name)
        else:
            raise make_ex(-errno.ENOTSUP, 'Unsupported encryption format')

    @requires_not_closed
    def encryption_load(self, format, passphrase):
        passphrase = cstr(passphrase, "passphrase")
        cdef rbd_encryption_format_t _format = format
        cdef rbd_encryption_luks1_format_options_t _luks1_opts
        cdef rbd_encryption_luks2_format_options_t _luks2_opts
        cdef rbd_encryption_luks_format_options_t _luks_opts
        cdef char* _passphrase = passphrase

        if (format == RBD_ENCRYPTION_FORMAT_LUKS1):
            _luks1_opts.passphrase = _passphrase
            _luks1_opts.passphrase_size = len(passphrase)
            with nogil:
                ret = rbd_encryption_load(self.image, _format, &_luks1_opts,
                                          sizeof(_luks1_opts))
            if ret != 0:
                raise make_ex(
                    ret,
                    ('error loading encryption on image %s '
                     'with format luks1') % self.name)
        elif (format == RBD_ENCRYPTION_FORMAT_LUKS2):
            _luks2_opts.passphrase = _passphrase
            _luks2_opts.passphrase_size = len(passphrase)
            with nogil:
                ret = rbd_encryption_load(self.image, _format, &_luks2_opts,
                                          sizeof(_luks2_opts))
            if ret != 0:
                raise make_ex(
                    ret,
                    ('error loading encryption on image %s '
                     'with format luks2') % self.name)
        elif (format == RBD_ENCRYPTION_FORMAT_LUKS):
            _luks_opts.passphrase = _passphrase
            _luks_opts.passphrase_size = len(passphrase)
            with nogil:
                ret = rbd_encryption_load(self.image, _format, &_luks_opts,
                                          sizeof(_luks_opts))
            if ret != 0:
                raise make_ex(
                    ret,
                    ('error loading encryption on image %s '
                     'with format luks') % self.name)
        else:
            raise make_ex(-errno.ENOTSUP, 'Unsupported encryption format')

    @requires_not_closed
    def encryption_load2(self, specs):
        cdef rbd_encryption_spec_t *_specs
        cdef rbd_encryption_luks1_format_options_t* _luks1_opts
        cdef rbd_encryption_luks2_format_options_t* _luks2_opts
        cdef rbd_encryption_luks_format_options_t* _luks_opts
        cdef size_t spec_count = len(specs)

        _specs = <rbd_encryption_spec_t *>malloc(len(specs) *
                                                 sizeof(rbd_encryption_spec_t))
        if _specs == NULL:
            raise MemoryError("malloc failed")

        memset(<void *>_specs, 0, len(specs) * sizeof(rbd_encryption_spec_t))
        try:
            for i in range(len(specs)):
                format, passphrase = specs[i]
                passphrase = cstr(passphrase, "specs[%d][1]" % i)
                _specs[i].format = format
                if (format == RBD_ENCRYPTION_FORMAT_LUKS1):
                    _luks1_opts = <rbd_encryption_luks1_format_options_t *>malloc(
                        sizeof(rbd_encryption_luks1_format_options_t))
                    if _luks1_opts == NULL:
                        raise MemoryError("malloc failed")
                    _luks1_opts.passphrase = passphrase
                    _luks1_opts.passphrase_size = len(passphrase)
                    _specs[i].opts = <rbd_encryption_options_t> _luks1_opts
                    _specs[i].opts_size = sizeof(
                    rbd_encryption_luks1_format_options_t)
                elif (format == RBD_ENCRYPTION_FORMAT_LUKS2):
                    _luks2_opts = <rbd_encryption_luks2_format_options_t *>malloc(
                        sizeof(rbd_encryption_luks2_format_options_t))
                    if _luks2_opts == NULL:
                        raise MemoryError("malloc failed")
                    _luks2_opts.passphrase = passphrase
                    _luks2_opts.passphrase_size = len(passphrase)
                    _specs[i].opts = <rbd_encryption_options_t> _luks2_opts
                    _specs[i].opts_size = sizeof(
                    rbd_encryption_luks2_format_options_t)
                elif (format == RBD_ENCRYPTION_FORMAT_LUKS):
                    _luks_opts = <rbd_encryption_luks_format_options_t *>malloc(
                        sizeof(rbd_encryption_luks_format_options_t))
                    if _luks_opts == NULL:
                        raise MemoryError("malloc failed")
                    _luks_opts.passphrase = passphrase
                    _luks_opts.passphrase_size = len(passphrase)
                    _specs[i].opts = <rbd_encryption_options_t> _luks_opts
                    _specs[i].opts_size = sizeof(
                    rbd_encryption_luks_format_options_t)
                else:
                    raise make_ex(
                        -errno.ENOTSUP,
                        'specs[%d][1]: Unsupported encryption format' % i)
            with nogil:
                ret = rbd_encryption_load2(self.image, _specs, spec_count)
            if ret != 0:
                raise make_ex(
                    ret,
                    'error loading encryption on image %s' % self.name)
        finally:
            for i in range(len(specs)):
                if _specs[i].opts != NULL:
                    free(_specs[i].opts)
            free(_specs)


cdef class ImageIterator(object):
    """
    Iterator over RBD images in a pool

    Yields a dictionary containing information about the images

    Keys are:

    * ``id`` (str) - image id

    * ``name`` (str) - image name
    """
    cdef rados_ioctx_t ioctx
    cdef rbd_image_spec_t *images
    cdef size_t num_images

    def __init__(self, ioctx):
        self.ioctx = convert_ioctx(ioctx)
        self.images = NULL
        self.num_images = 1024
        while True:
            self.images = <rbd_image_spec_t*>realloc_chk(
                self.images, self.num_images * sizeof(rbd_image_spec_t))
            with nogil:
                ret = rbd_list2(self.ioctx, self.images, &self.num_images)
            if ret >= 0:
                break
            elif ret == -errno.ERANGE:
                self.num_images *= 2
            else:
                raise make_ex(ret, 'error listing images.')

    def __iter__(self):
        for i in range(self.num_images):
            yield {
                'id'   : decode_cstr(self.images[i].id),
                'name' : decode_cstr(self.images[i].name)
                }

    def __dealloc__(self):
        if self.images:
            rbd_image_spec_list_cleanup(self.images, self.num_images)
            free(self.images)


cdef class LockOwnerIterator(object):
    """
    Iterator over managed lock owners for an image

    Yields a dictionary containing information about the image's lock

    Keys are:

    * ``mode`` (int) - active lock mode

    * ``owner`` (str) - lock owner name
    """

    cdef:
        rbd_lock_mode_t lock_mode
        char **lock_owners
        size_t num_lock_owners
        object image

    def __init__(self, Image image):
        image.require_not_closed()

        self.image = image
        self.lock_owners = NULL
        self.num_lock_owners = 8
        while True:
            self.lock_owners = <char**>realloc_chk(self.lock_owners,
                                                   self.num_lock_owners *
                                                   sizeof(char*))
            with nogil:
                ret = rbd_lock_get_owners(image.image, &self.lock_mode,
                                          self.lock_owners,
                                          &self.num_lock_owners)
            if ret >= 0:
                break
            elif ret == -errno.ENOENT:
                self.num_lock_owners = 0
                break
            elif ret != -errno.ERANGE:
                raise make_ex(ret, 'error listing lock owners for image %s' % image.name)

    def __iter__(self):
        for i in range(self.num_lock_owners):
            yield {
                'mode'  : int(self.lock_mode),
                'owner' : decode_cstr(self.lock_owners[i]),
                }

    def __dealloc__(self):
        if self.lock_owners:
            rbd_lock_get_owners_cleanup(self.lock_owners, self.num_lock_owners)
            free(self.lock_owners)

cdef class MetadataIterator(object):
    """
    Iterator over metadata list for an image.

    Yields ``(key, value)`` tuple.

    * ``key`` (str) - metadata key
    * ``value`` (str) - metadata value
    """

    cdef:
        cdef object image
        rbd_image_t c_image
        char *last_read
        uint64_t max_read
        object next_chunk

    def __init__(self, Image image):
        image.require_not_closed()

        self.image = image
        self.c_image = image.image
        self.last_read = strdup("")
        self.max_read = 32
        self.get_next_chunk()

    def __iter__(self):
        while len(self.next_chunk) > 0:
            for pair in self.next_chunk:
                yield pair
            if len(self.next_chunk) < self.max_read:
                break
            self.get_next_chunk()

    def __dealloc__(self):
        if self.last_read:
            free(self.last_read)

    def get_next_chunk(self):
        self.image.require_not_closed()

        cdef:
            char *c_keys = NULL
            size_t keys_size = 4096
            char *c_vals = NULL
            size_t vals_size = 4096
        try:
            while True:
                c_keys = <char *>realloc_chk(c_keys, keys_size)
                c_vals = <char *>realloc_chk(c_vals, vals_size)
                with nogil:
                    ret = rbd_metadata_list(self.c_image, self.last_read,
                                            self.max_read, c_keys, &keys_size,
                                            c_vals, &vals_size)
                if ret >= 0:
                    break
                elif ret != -errno.ERANGE:
                    raise make_ex(ret, 'error listing metadata for image %s' %
                                  self.image.name)
            keys = [decode_cstr(key) for key in
                        c_keys[:keys_size].split(b'\0') if key]
            vals = [decode_cstr(val) for val in
                        c_vals[:vals_size].split(b'\0') if val]
            if len(keys) > 0:
                last_read = cstr(keys[-1], 'last_read')
                free(self.last_read)
                self.last_read = strdup(last_read)
            self.next_chunk = list(zip(keys, vals))
        finally:
            free(c_keys)
            free(c_vals)

cdef class SnapIterator(object):
    """
    Iterator over snapshot info for an image.

    Yields a dictionary containing information about a snapshot.

    Keys are:

    * ``id`` (int) - numeric identifier of the snapshot

    * ``size`` (int) - size of the image at the time of snapshot (in bytes)

    * ``name`` (str) - name of the snapshot

    * ``namespace`` (int) - enum for snap namespace

    * ``group`` (dict) - optional for group namespace snapshots

    * ``trash`` (dict) - optional for trash namespace snapshots

    * ``mirror`` (dict) - optional for mirror namespace snapshots
    """

    cdef rbd_snap_info_t *snaps
    cdef int num_snaps
    cdef object image

    def __init__(self, Image image):
        image.require_not_closed()

        self.image = image
        self.snaps = NULL
        self.num_snaps = 10
        while True:
            self.snaps = <rbd_snap_info_t*>realloc_chk(self.snaps,
                                                       self.num_snaps *
                                                       sizeof(rbd_snap_info_t))
            with nogil:
                ret = rbd_snap_list(image.image, self.snaps, &self.num_snaps)
            if ret >= 0:
                self.num_snaps = ret
                break
            elif ret != -errno.ERANGE:
                raise make_ex(ret, 'error listing snapshots for image %s' % image.name)

    def __iter__(self):
        for i in range(self.num_snaps):
            s = {
                'id'   : self.snaps[i].id,
                'size' : self.snaps[i].size,
                'name' : decode_cstr(self.snaps[i].name),
                'namespace' : self.image.snap_get_namespace_type(self.snaps[i].id)
                }
            if s['namespace'] == RBD_SNAP_NAMESPACE_TYPE_GROUP:
                try:
                    group = self.image.snap_get_group_namespace(self.snaps[i].id)
                except:
                    group = None
                s['group'] = group
            elif s['namespace'] == RBD_SNAP_NAMESPACE_TYPE_TRASH:
                try:
                    trash = self.image.snap_get_trash_namespace(self.snaps[i].id)
                except:
                    trash = None
                s['trash'] = trash
            elif s['namespace'] == RBD_SNAP_NAMESPACE_TYPE_MIRROR:
                try:
                    mirror = self.image.snap_get_mirror_namespace(
                        self.snaps[i].id)
                except:
                    mirror = None
                s['mirror'] = mirror
            yield s

    def __dealloc__(self):
        if self.snaps:
            rbd_snap_list_end(self.snaps)
            free(self.snaps)

cdef class TrashIterator(object):
    """
    Iterator over trash entries.

    Yields a dictionary containing trash info of an image.

    Keys are:

        * `id` (str) - image id

        * `name` (str) - image name

        * `source` (str) - source of deletion

        * `deletion_time` (datetime) - time of deletion

        * `deferment_end_time` (datetime) - time that an image is allowed to be
                                            removed from trash
    """

    cdef:
        rados_ioctx_t ioctx
        size_t num_entries
        rbd_trash_image_info_t *entries

    def __init__(self, ioctx):
        self.ioctx = convert_ioctx(ioctx)
        self.num_entries = 1024
        self.entries = NULL
        while True:
            self.entries = <rbd_trash_image_info_t*>realloc_chk(self.entries,
                                                                self.num_entries *
                                                                sizeof(rbd_trash_image_info_t))
            with nogil:
                ret = rbd_trash_list(self.ioctx, self.entries, &self.num_entries)
            if ret >= 0:
                self.num_entries = ret
                break
            elif ret != -errno.ERANGE:
                raise make_ex(ret, 'error listing trash entries')

    __source_string = ['USER', 'MIRRORING']

    def __iter__(self):
        for i in range(self.num_entries):
            yield {
                'id'          : decode_cstr(self.entries[i].id),
                'name'        : decode_cstr(self.entries[i].name),
                'source'      : TrashIterator.__source_string[self.entries[i].source],
                'deletion_time' : datetime.utcfromtimestamp(self.entries[i].deletion_time),
                'deferment_end_time' : datetime.utcfromtimestamp(self.entries[i].deferment_end_time)
                }

    def __dealloc__(self):
        rbd_trash_list_cleanup(self.entries, self.num_entries)
        if self.entries:
            free(self.entries)

cdef class ChildIterator(object):
    """
    Iterator over child info for the image or its snapshot.

    Yields a dictionary containing information about a child.

    Keys are:

    * ``pool`` (str) - name of the pool

    * ``pool_namespace`` (str) - namespace of the pool

    * ``image`` (str) - name of the child

    * ``id`` (str) - id of the child

    * ``trash`` (bool) - True if child is in trash bin
    """

    cdef rbd_linked_image_spec_t *children
    cdef size_t num_children
    cdef object image

    def __init__(self, Image image, descendants=False):
        image.require_not_closed()

        self.image = image
        self.children = NULL
        self.num_children = 10
        while True:
            self.children = <rbd_linked_image_spec_t*>realloc_chk(
                self.children, self.num_children * sizeof(rbd_linked_image_spec_t))
            if descendants:
                with nogil:
                    ret = rbd_list_descendants(image.image, self.children,
                                               &self.num_children)
            else:
                with nogil:
                    ret = rbd_list_children3(image.image, self.children,
                                             &self.num_children)
            if ret >= 0:
                break
            elif ret != -errno.ERANGE:
                raise make_ex(ret, 'error listing children.')

    def __iter__(self):
        for i in range(self.num_children):
            yield {
                'pool'           : decode_cstr(self.children[i].pool_name),
                'pool_namespace' : decode_cstr(self.children[i].pool_namespace),
                'image'          : decode_cstr(self.children[i].image_name),
                'id'             : decode_cstr(self.children[i].image_id),
                'trash'          : self.children[i].trash
                }

    def __dealloc__(self):
        if self.children:
            rbd_linked_image_spec_list_cleanup(self.children, self.num_children)
            free(self.children)

cdef class WatcherIterator(object):
    """
    Iterator over watchers of an image.

    Yields a dictionary containing information about a watcher.

    Keys are:

    * ``addr`` (str) - address of the watcher

    * ``id`` (int) - id of the watcher

    * ``cookie`` (int) - the watcher's cookie
    """

    cdef rbd_image_watcher_t *watchers
    cdef size_t num_watchers
    cdef object image

    def __init__(self, Image image):
        image.require_not_closed()

        self.image = image
        self.watchers = NULL
        self.num_watchers = 10
        while True:
            self.watchers = <rbd_image_watcher_t*>realloc_chk(self.watchers,
                                                              self.num_watchers *
                                                              sizeof(rbd_image_watcher_t))
            with nogil:
                ret = rbd_watchers_list(image.image, self.watchers, &self.num_watchers)
            if ret >= 0:
                break
            elif ret != -errno.ERANGE:
                raise make_ex(ret, 'error listing watchers.')

    def __iter__(self):
        for i in range(self.num_watchers):
            yield {
                'addr'   : decode_cstr(self.watchers[i].addr),
                'id'     : self.watchers[i].id,
                'cookie' : self.watchers[i].cookie
                }

    def __dealloc__(self):
        if self.watchers:
            rbd_watchers_list_cleanup(self.watchers, self.num_watchers)
            free(self.watchers)

cdef class ConfigImageIterator(object):
    """
    Iterator over image-level overrides for an image.

    Yields a dictionary containing information about an override.

    Keys are:

    * ``name`` (str) - override name

    * ``value`` (str) - override value

    * ``source`` (str) - override source
    """

    cdef:
        rbd_config_option_t *options
        int num_options

    def __init__(self, Image image):
        image.require_not_closed()

        self.options = NULL
        self.num_options = 32
        while True:
            self.options = <rbd_config_option_t *>realloc_chk(
                self.options, self.num_options * sizeof(rbd_config_option_t))
            with nogil:
                ret = rbd_config_image_list(image.image, self.options,
                                            &self.num_options)
            if ret < 0:
                if ret == -errno.ERANGE:
                    continue
                self.num_options = 0
                raise make_ex(ret, 'error listing config options')
            break

    def __iter__(self):
        for i in range(self.num_options):
            yield {
                'name'   : decode_cstr(self.options[i].name),
                'value'  : decode_cstr(self.options[i].value),
                'source' : self.options[i].source,
                }

    def __dealloc__(self):
        if self.options:
            rbd_config_image_list_cleanup(self.options, self.num_options)
            free(self.options)

cdef class GroupImageIterator(object):
    """
    Iterator over image info for a group.

    Yields a dictionary containing information about an image.

    Keys are:

    * ``name`` (str) - name of the image

    * ``pool`` (int) - id of the pool this image belongs to

    * ``state`` (int) - state of the image
    """

    cdef rbd_group_image_info_t *images
    cdef size_t num_images
    cdef object group

    def __init__(self, Group group):
        self.group = group
        self.images = NULL
        self.num_images = 10
        while True:
            self.images = <rbd_group_image_info_t*>realloc_chk(self.images,
                                                               self.num_images *
                                                               sizeof(rbd_group_image_info_t))
            with nogil:
                ret = rbd_group_image_list(group._ioctx, group._name,
                                           self.images,
                                           sizeof(rbd_group_image_info_t),
                                           &self.num_images)

            if ret >= 0:
                break
            elif ret != -errno.ERANGE:
                raise make_ex(ret, 'error listing images for group %s' % group.name, group_errno_to_exception)

    def __iter__(self):
        for i in range(self.num_images):
            yield {
                'name'  : decode_cstr(self.images[i].name),
                'pool'  : self.images[i].pool,
                'state' : self.images[i].state,
                }

    def __dealloc__(self):
        if self.images:
            rbd_group_image_list_cleanup(self.images,
                                         sizeof(rbd_group_image_info_t),
                                         self.num_images)
            free(self.images)

cdef class GroupSnapIterator(object):
    """
    Iterator over snaps specs for a group.

    Yields a dictionary containing information about a snapshot.

    Keys are:

    * ``name`` (str) - name of the snapshot

    * ``state`` (int) - state of the snapshot
    """

    cdef rbd_group_snap_info_t *snaps
    cdef size_t num_snaps
    cdef object group

    def __init__(self, Group group):
        self.group = group
        self.snaps = NULL
        self.num_snaps = 10
        while True:
            self.snaps = <rbd_group_snap_info_t*>realloc_chk(self.snaps,
                                                             self.num_snaps *
                                                             sizeof(rbd_group_snap_info_t))
            with nogil:
                ret = rbd_group_snap_list(group._ioctx, group._name, self.snaps,
                                          sizeof(rbd_group_snap_info_t),
                                          &self.num_snaps)

            if ret >= 0:
                break
            elif ret != -errno.ERANGE:
                raise make_ex(ret, 'error listing snapshots for group %s' % group.name, group_errno_to_exception)

    def __iter__(self):
        for i in range(self.num_snaps):
            yield {
                'name'  : decode_cstr(self.snaps[i].name),
                'state' : self.snaps[i].state,
                }

    def __dealloc__(self):
        if self.snaps:
            rbd_group_snap_list_cleanup(self.snaps,
                                        sizeof(rbd_group_snap_info_t),
                                        self.num_snaps)
            free(self.snaps)
