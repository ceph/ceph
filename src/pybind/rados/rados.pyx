# cython: embedsignature=True
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

import sys
import threading
import time

try:
    from collections.abc import Callable
except ImportError:
    from collections import Callable
from datetime import datetime
from functools import partial, wraps
from itertools import chain

# Are we running Python 2.x
if sys.version_info[0] < 3:
    str_type = basestring
else:
    str_type = str


cdef extern from "Python.h":
    # These are in cpython/string.pxd, but use "object" types instead of
    # PyObject*, which invokes assumptions in cpython that we need to
    # legitimately break to implement zero-copy string buffers in Ioctx.read().
    # This is valid use of the Python API and documented as a special case.
    PyObject *PyBytes_FromStringAndSize(char *v, Py_ssize_t len) except NULL
    char* PyBytes_AsString(PyObject *string) except NULL
    int _PyBytes_Resize(PyObject **string, Py_ssize_t newsize) except -1
    void PyEval_InitThreads()


cdef extern from "time.h":
    ctypedef long int time_t
    ctypedef long int suseconds_t


cdef extern from "sys/time.h":
    cdef struct timeval:
        time_t tv_sec
        suseconds_t tv_usec


cdef extern from "rados/rados_types.h" nogil:
    cdef char* _LIBRADOS_ALL_NSPACES "LIBRADOS_ALL_NSPACES"


cdef extern from "rados/librados.h" nogil:
    enum:
        _LIBRADOS_OP_FLAG_EXCL "LIBRADOS_OP_FLAG_EXCL"
        _LIBRADOS_OP_FLAG_FAILOK "LIBRADOS_OP_FLAG_FAILOK"
        _LIBRADOS_OP_FLAG_FADVISE_RANDOM "LIBRADOS_OP_FLAG_FADVISE_RANDOM"
        _LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL "LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL"
        _LIBRADOS_OP_FLAG_FADVISE_WILLNEED "LIBRADOS_OP_FLAG_FADVISE_WILLNEED"
        _LIBRADOS_OP_FLAG_FADVISE_DONTNEED "LIBRADOS_OP_FLAG_FADVISE_DONTNEED"
        _LIBRADOS_OP_FLAG_FADVISE_NOCACHE "LIBRADOS_OP_FLAG_FADVISE_NOCACHE"


    enum:
        _LIBRADOS_OPERATION_NOFLAG "LIBRADOS_OPERATION_NOFLAG"
        _LIBRADOS_OPERATION_BALANCE_READS "LIBRADOS_OPERATION_BALANCE_READS"
        _LIBRADOS_OPERATION_LOCALIZE_READS "LIBRADOS_OPERATION_LOCALIZE_READS"
        _LIBRADOS_OPERATION_ORDER_READS_WRITES "LIBRADOS_OPERATION_ORDER_READS_WRITES"
        _LIBRADOS_OPERATION_IGNORE_CACHE "LIBRADOS_OPERATION_IGNORE_CACHE"
        _LIBRADOS_OPERATION_SKIPRWLOCKS "LIBRADOS_OPERATION_SKIPRWLOCKS"
        _LIBRADOS_OPERATION_IGNORE_OVERLAY "LIBRADOS_OPERATION_IGNORE_OVERLAY"
        _LIBRADOS_CREATE_EXCLUSIVE "LIBRADOS_CREATE_EXCLUSIVE"
        _LIBRADOS_CREATE_IDEMPOTENT "LIBRADOS_CREATE_IDEMPOTENT"

    cdef uint64_t _LIBRADOS_SNAP_HEAD "LIBRADOS_SNAP_HEAD"

    ctypedef void* rados_xattrs_iter_t
    ctypedef void* rados_omap_iter_t
    ctypedef void* rados_list_ctx_t
    ctypedef uint64_t rados_snap_t
    ctypedef void *rados_write_op_t
    ctypedef void *rados_read_op_t
    ctypedef void *rados_completion_t
    ctypedef void (*rados_callback_t)(rados_completion_t cb, void *arg)
    ctypedef void (*rados_log_callback_t)(void *arg, const char *line, const char *who,
                                          uint64_t sec, uint64_t nsec, uint64_t seq, const char *level, const char *msg)
    ctypedef void (*rados_log_callback2_t)(void *arg, const char *line, const char *channel, const char *who, const char *name,
                                          uint64_t sec, uint64_t nsec, uint64_t seq, const char *level, const char *msg)


    cdef struct rados_cluster_stat_t:
        uint64_t kb
        uint64_t kb_used
        uint64_t kb_avail
        uint64_t num_objects

    cdef struct rados_pool_stat_t:
        uint64_t num_bytes
        uint64_t num_kb
        uint64_t num_objects
        uint64_t num_object_clones
        uint64_t num_object_copies
        uint64_t num_objects_missing_on_primary
        uint64_t num_objects_unfound
        uint64_t num_objects_degraded
        uint64_t num_rd
        uint64_t num_rd_kb
        uint64_t num_wr
        uint64_t num_wr_kb

    void rados_buffer_free(char *buf)

    void rados_version(int *major, int *minor, int *extra)
    int rados_create2(rados_t *pcluster, const char *const clustername,
                      const char * const name, uint64_t flags)
    int rados_create_with_context(rados_t *cluster, rados_config_t cct)
    int rados_connect(rados_t cluster)
    void rados_shutdown(rados_t cluster)
    uint64_t rados_get_instance_id(rados_t cluster)
    int rados_conf_read_file(rados_t cluster, const char *path)
    int rados_conf_parse_argv_remainder(rados_t cluster, int argc, const char **argv, const char **remargv)
    int rados_conf_parse_env(rados_t cluster, const char *var)
    int rados_conf_set(rados_t cluster, char *option, const char *value)
    int rados_conf_get(rados_t cluster, char *option, char *buf, size_t len)

    int rados_ioctx_pool_stat(rados_ioctx_t io, rados_pool_stat_t *stats)
    int64_t rados_pool_lookup(rados_t cluster, const char *pool_name)
    int rados_pool_reverse_lookup(rados_t cluster, int64_t id, char *buf, size_t maxlen)
    int rados_pool_create(rados_t cluster, const char *pool_name)
    int rados_pool_create_with_crush_rule(rados_t cluster, const char *pool_name, uint8_t crush_rule_num)
    int rados_pool_get_base_tier(rados_t cluster, int64_t pool, int64_t *base_tier)
    int rados_pool_list(rados_t cluster, char *buf, size_t len)
    int rados_pool_delete(rados_t cluster, const char *pool_name)
    int rados_inconsistent_pg_list(rados_t cluster, int64_t pool, char *buf, size_t len)

    int rados_cluster_stat(rados_t cluster, rados_cluster_stat_t *result)
    int rados_cluster_fsid(rados_t cluster, char *buf, size_t len)
    int rados_blacklist_add(rados_t cluster, char *client_address, uint32_t expire_seconds)
    int rados_application_enable(rados_ioctx_t io, const char *app_name,
                                 int force)
    void rados_set_osdmap_full_try(rados_ioctx_t io)
    void rados_unset_osdmap_full_try(rados_ioctx_t io)
    int rados_application_list(rados_ioctx_t io, char *values,
                             size_t *values_len)
    int rados_application_metadata_get(rados_ioctx_t io, const char *app_name,
                                       const char *key, char *value,
                                       size_t *value_len)
    int rados_application_metadata_set(rados_ioctx_t io, const char *app_name,
                                       const char *key, const char *value)
    int rados_application_metadata_remove(rados_ioctx_t io,
                                          const char *app_name, const char *key)
    int rados_application_metadata_list(rados_ioctx_t io,
                                        const char *app_name, char *keys,
                                        size_t *key_len, char *values,
                                        size_t *value_len)
    int rados_ping_monitor(rados_t cluster, const char *mon_id, char **outstr, size_t *outstrlen)
    int rados_mon_command(rados_t cluster, const char **cmd, size_t cmdlen,
                          const char *inbuf, size_t inbuflen,
                          char **outbuf, size_t *outbuflen,
                          char **outs, size_t *outslen)
    int rados_mgr_command(rados_t cluster, const char **cmd, size_t cmdlen,
                          const char *inbuf, size_t inbuflen,
                          char **outbuf, size_t *outbuflen,
                          char **outs, size_t *outslen)
    int rados_mon_command_target(rados_t cluster, const char *name, const char **cmd, size_t cmdlen,
                                 const char *inbuf, size_t inbuflen,
                                 char **outbuf, size_t *outbuflen,
                                 char **outs, size_t *outslen)
    int rados_osd_command(rados_t cluster, int osdid, const char **cmd, size_t cmdlen,
                          const char *inbuf, size_t inbuflen,
                          char **outbuf, size_t *outbuflen,
                          char **outs, size_t *outslen)
    int rados_pg_command(rados_t cluster, const char *pgstr, const char **cmd, size_t cmdlen,
                         const char *inbuf, size_t inbuflen,
                         char **outbuf, size_t *outbuflen,
                         char **outs, size_t *outslen)
    int rados_monitor_log(rados_t cluster, const char *level, rados_log_callback_t cb, void *arg)
    int rados_monitor_log2(rados_t cluster, const char *level, rados_log_callback2_t cb, void *arg)

    int rados_wait_for_latest_osdmap(rados_t cluster)

    int rados_service_register(rados_t cluster, const char *service, const char *daemon, const char *metadata_dict)
    int rados_service_update_status(rados_t cluster, const char *status_dict)

    int rados_ioctx_create(rados_t cluster, const char *pool_name, rados_ioctx_t *ioctx)
    int rados_ioctx_create2(rados_t cluster, int64_t pool_id, rados_ioctx_t *ioctx)
    void rados_ioctx_destroy(rados_ioctx_t io)
    void rados_ioctx_locator_set_key(rados_ioctx_t io, const char *key)
    void rados_ioctx_set_namespace(rados_ioctx_t io, const char * nspace)

    uint64_t rados_get_last_version(rados_ioctx_t io)
    int rados_stat(rados_ioctx_t io, const char *o, uint64_t *psize, time_t *pmtime)
    int rados_write(rados_ioctx_t io, const char *oid, const char *buf, size_t len, uint64_t off)
    int rados_write_full(rados_ioctx_t io, const char *oid, const char *buf, size_t len)
    int rados_append(rados_ioctx_t io, const char *oid, const char *buf, size_t len)
    int rados_read(rados_ioctx_t io, const char *oid, char *buf, size_t len, uint64_t off)
    int rados_remove(rados_ioctx_t io, const char *oid)
    int rados_trunc(rados_ioctx_t io, const char *oid, uint64_t size)
    int rados_getxattr(rados_ioctx_t io, const char *o, const char *name, char *buf, size_t len)
    int rados_setxattr(rados_ioctx_t io, const char *o, const char *name, const char *buf, size_t len)
    int rados_rmxattr(rados_ioctx_t io, const char *o, const char *name)
    int rados_getxattrs(rados_ioctx_t io, const char *oid, rados_xattrs_iter_t *iter)
    int rados_getxattrs_next(rados_xattrs_iter_t iter, const char **name, const char **val, size_t *len)
    void rados_getxattrs_end(rados_xattrs_iter_t iter)

    int rados_nobjects_list_open(rados_ioctx_t io, rados_list_ctx_t *ctx)
    int rados_nobjects_list_next(rados_list_ctx_t ctx, const char **entry, const char **key, const char **nspace)
    void rados_nobjects_list_close(rados_list_ctx_t ctx)

    int rados_ioctx_pool_requires_alignment2(rados_ioctx_t io, int * requires)
    int rados_ioctx_pool_required_alignment2(rados_ioctx_t io, uint64_t * alignment)

    int rados_ioctx_snap_rollback(rados_ioctx_t io, const char * oid, const char * snapname)
    int rados_ioctx_snap_create(rados_ioctx_t io, const char * snapname)
    int rados_ioctx_snap_remove(rados_ioctx_t io, const char * snapname)
    int rados_ioctx_snap_lookup(rados_ioctx_t io, const char * name, rados_snap_t * id)
    int rados_ioctx_snap_get_name(rados_ioctx_t io, rados_snap_t id, char * name, int maxlen)
    void rados_ioctx_snap_set_read(rados_ioctx_t io, rados_snap_t snap)
    int rados_ioctx_snap_list(rados_ioctx_t io, rados_snap_t * snaps, int maxlen)
    int rados_ioctx_snap_get_stamp(rados_ioctx_t io, rados_snap_t id, time_t * t)

    int rados_ioctx_selfmanaged_snap_create(rados_ioctx_t io,
                                            rados_snap_t *snapid)
    int rados_ioctx_selfmanaged_snap_remove(rados_ioctx_t io,
                                            rados_snap_t snapid)
    int rados_ioctx_selfmanaged_snap_set_write_ctx(rados_ioctx_t io,
                                                   rados_snap_t snap_seq,
                                                   rados_snap_t *snap,
                                                   int num_snaps)
    int rados_ioctx_selfmanaged_snap_rollback(rados_ioctx_t io, const char *oid,
                                              rados_snap_t snapid)

    int rados_lock_exclusive(rados_ioctx_t io, const char * oid, const char * name,
                             const char * cookie, const char * desc,
                             timeval * duration, uint8_t flags)
    int rados_lock_shared(rados_ioctx_t io, const char * o, const char * name,
                          const char * cookie, const char * tag, const char * desc,
                          timeval * duration, uint8_t flags)
    int rados_unlock(rados_ioctx_t io, const char * o, const char * name, const char * cookie)

    rados_write_op_t rados_create_write_op()
    void rados_release_write_op(rados_write_op_t write_op)

    rados_read_op_t rados_create_read_op()
    void rados_release_read_op(rados_read_op_t read_op)

    int rados_aio_create_completion(void * cb_arg, rados_callback_t cb_complete, rados_callback_t cb_safe, rados_completion_t * pc)
    void rados_aio_release(rados_completion_t c)
    int rados_aio_stat(rados_ioctx_t io, const char *oid, rados_completion_t completion, uint64_t *psize, time_t *pmtime)
    int rados_aio_write(rados_ioctx_t io, const char * oid, rados_completion_t completion, const char * buf, size_t len, uint64_t off)
    int rados_aio_append(rados_ioctx_t io, const char * oid, rados_completion_t completion, const char * buf, size_t len)
    int rados_aio_write_full(rados_ioctx_t io, const char * oid, rados_completion_t completion, const char * buf, size_t len)
    int rados_aio_remove(rados_ioctx_t io, const char * oid, rados_completion_t completion)
    int rados_aio_read(rados_ioctx_t io, const char * oid, rados_completion_t completion, char * buf, size_t len, uint64_t off)
    int rados_aio_flush(rados_ioctx_t io)

    int rados_aio_get_return_value(rados_completion_t c)
    int rados_aio_wait_for_complete_and_cb(rados_completion_t c)
    int rados_aio_wait_for_safe_and_cb(rados_completion_t c)
    int rados_aio_wait_for_complete(rados_completion_t c)
    int rados_aio_wait_for_safe(rados_completion_t c)
    int rados_aio_is_complete(rados_completion_t c)
    int rados_aio_is_safe(rados_completion_t c)

    int rados_exec(rados_ioctx_t io, const char * oid, const char * cls, const char * method,
                   const char * in_buf, size_t in_len, char * buf, size_t out_len)
    int rados_aio_exec(rados_ioctx_t io, const char * oid, rados_completion_t completion, const char * cls, const char * method,
                       const char * in_buf, size_t in_len, char * buf, size_t out_len)

    int rados_write_op_operate(rados_write_op_t write_op, rados_ioctx_t io, const char * oid, time_t * mtime, int flags)
    int rados_aio_write_op_operate(rados_write_op_t write_op, rados_ioctx_t io, rados_completion_t completion, const char *oid, time_t *mtime, int flags)
    void rados_write_op_omap_set(rados_write_op_t write_op, const char * const* keys, const char * const* vals, const size_t * lens, size_t num)
    void rados_write_op_omap_rm_keys(rados_write_op_t write_op, const char * const* keys, size_t keys_len)
    void rados_write_op_omap_clear(rados_write_op_t write_op)
    void rados_write_op_set_flags(rados_write_op_t write_op, int flags)

    void rados_write_op_create(rados_write_op_t write_op, int exclusive, const char *category)
    void rados_write_op_append(rados_write_op_t write_op, const char *buffer, size_t len)
    void rados_write_op_write_full(rados_write_op_t write_op, const char *buffer, size_t len)
    void rados_write_op_assert_version(rados_write_op_t write_op, uint64_t ver)
    void rados_write_op_write(rados_write_op_t write_op, const char *buffer, size_t len, uint64_t offset)
    void rados_write_op_remove(rados_write_op_t write_op)
    void rados_write_op_truncate(rados_write_op_t write_op, uint64_t offset)
    void rados_write_op_zero(rados_write_op_t write_op, uint64_t offset, uint64_t len)

    void rados_read_op_omap_get_vals2(rados_read_op_t read_op, const char * start_after, const char * filter_prefix, uint64_t max_return, rados_omap_iter_t * iter, unsigned char *pmore, int * prval)
    void rados_read_op_omap_get_keys2(rados_read_op_t read_op, const char * start_after, uint64_t max_return, rados_omap_iter_t * iter, unsigned char *pmore, int * prval)
    void rados_read_op_omap_get_vals_by_keys(rados_read_op_t read_op, const char * const* keys, size_t keys_len, rados_omap_iter_t * iter, int * prval)
    int rados_read_op_operate(rados_read_op_t read_op, rados_ioctx_t io, const char * oid, int flags)
    int rados_aio_read_op_operate(rados_read_op_t read_op, rados_ioctx_t io, rados_completion_t completion, const char *oid, int flags)
    void rados_read_op_set_flags(rados_read_op_t read_op, int flags)
    int rados_omap_get_next(rados_omap_iter_t iter, const char * const* key, const char * const* val, size_t * len)
    void rados_omap_get_end(rados_omap_iter_t iter)
    int rados_notify2(rados_ioctx_t io, const char * o, const char *buf, int buf_len, uint64_t timeout_ms, char **reply_buffer, size_t *reply_buffer_len)


LIBRADOS_OP_FLAG_EXCL = _LIBRADOS_OP_FLAG_EXCL
LIBRADOS_OP_FLAG_FAILOK = _LIBRADOS_OP_FLAG_FAILOK
LIBRADOS_OP_FLAG_FADVISE_RANDOM = _LIBRADOS_OP_FLAG_FADVISE_RANDOM
LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL = _LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL
LIBRADOS_OP_FLAG_FADVISE_WILLNEED = _LIBRADOS_OP_FLAG_FADVISE_WILLNEED
LIBRADOS_OP_FLAG_FADVISE_DONTNEED = _LIBRADOS_OP_FLAG_FADVISE_DONTNEED
LIBRADOS_OP_FLAG_FADVISE_NOCACHE = _LIBRADOS_OP_FLAG_FADVISE_NOCACHE

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
    pass

class OSError(Error):
    """ `OSError` class, derived from `Error` """
    pass

class InterruptedOrTimeoutError(OSError):
    """ `InterruptedOrTimeoutError` class, derived from `OSError` """
    pass


class PermissionError(OSError):
    """ `PermissionError` class, derived from `OSError` """
    pass


class PermissionDeniedError(OSError):
    """ deal with EACCES related. """
    pass


class ObjectNotFound(OSError):
    """ `ObjectNotFound` class, derived from `OSError` """
    pass


class NoData(OSError):
    """ `NoData` class, derived from `OSError` """
    pass


class ObjectExists(OSError):
    """ `ObjectExists` class, derived from `OSError` """
    pass


class ObjectBusy(OSError):
    """ `ObjectBusy` class, derived from `IOError` """
    pass


class IOError(OSError):
    """ `ObjectBusy` class, derived from `OSError` """
    pass


class NoSpace(OSError):
    """ `NoSpace` class, derived from `OSError` """
    pass


class RadosStateError(Error):
    """ `RadosStateError` class, derived from `Error` """
    pass


class IoctxStateError(Error):
    """ `IoctxStateError` class, derived from `Error` """
    pass


class ObjectStateError(Error):
    """ `ObjectStateError` class, derived from `Error` """
    pass


class LogicError(Error):
    """ `` class, derived from `Error` """
    pass


class TimedOut(OSError):
    """ `TimedOut` class, derived from `OSError` """
    pass


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
        errno.EINVAL    : InvalidArgumentError,
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
        errno.EINVAL    : InvalidArgumentError,
    }


cdef make_ex(ret, msg):
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
    else:
        return OSError(msg, errno=ret)


# helper to specify an optional argument, where in addition to `cls`, `None`
# is also acceptable
def opt(cls):
    return (cls, None)


# validate argument types of an instance method
# kwargs is an un-ordered dict, so use args instead
def requires(*types):
    def is_type_of(v, t):
        if t is None:
            return v is None
        else:
            return isinstance(v, t)

    def check_type(val, arg_name, arg_type):
        if isinstance(arg_type, tuple):
            if any(is_type_of(val, t) for t in arg_type):
                return
            type_names = ' or '.join('None' if t is None else t.__name__
                                     for t in arg_type)
            raise TypeError('%s must be %s' % (arg_name, type_names))
        else:
            if is_type_of(val, arg_type):
                return
            assert(arg_type is not None)
            raise TypeError('%s must be %s' % (arg_name, arg_type.__name__))

    def wrapper(f):
        # FIXME(sileht): this stop with
        # AttributeError: 'method_descriptor' object has no attribute '__module__'
        # @wraps(f)
        def validate_func(*args, **kwargs):
            # ignore the `self` arg
            pos_args = zip(args[1:], types)
            named_args = ((kwargs[name], (name, spec)) for name, spec in types
                          if name in kwargs)
            for arg_val, (arg_name, arg_type) in chain(pos_args, named_args):
                check_type(arg_val, arg_name, arg_type)
            return f(*args, **kwargs)
        return validate_func
    return wrapper


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
    elif isinstance(val, unicode):
        return val.encode(encoding)
    else:
        raise TypeError('%s must be a string' % name)


def cstr_list(list_str, name, encoding="utf-8"):
    return [cstr(s, name) for s in list_str]


def decode_cstr(val, encoding="utf-8"):
    """
    Decode a byte string into a Python string.

    :param bytes val: byte string
    :rtype: unicode or None
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


cdef size_t * to_csize_t_array(list_int):
    cdef size_t *ret = <size_t *>malloc(len(list_int) * sizeof(size_t))
    if ret == NULL:
        raise MemoryError("malloc failed")
    for i in xrange(len(list_int)):
        ret[i] = <size_t>list_int[i]
    return ret


cdef char ** to_bytes_array(list_bytes):
    cdef char **ret = <char **>malloc(len(list_bytes) * sizeof(char *))
    if ret == NULL:
        raise MemoryError("malloc failed")
    for i in xrange(len(list_bytes)):
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

    @requires(('rados_id', opt(str_type)), ('name', opt(str_type)), ('clustername', opt(str_type)),
              ('conffile', opt(str_type)))
    def __setup(self, rados_id=None, name=None, clustername=None,
                conf_defaults=None, conffile=None, conf=None, flags=0,
                context=None):
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

        name = cstr(name, 'name')
        clustername = cstr(clustername, 'clustername')
        cdef:
            char *_name = name
            char *_clustername = clustername
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
        if conffile is not None:
            # read the default conf file when '' is given
            if conffile == '':
                conffile = None
            self.conf_read_file(conffile)
        if conf:
            for key, value in conf.items():
                self.conf_set(key, value)

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

    def version(self):
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

    @requires(('path', opt(str_type)))
    def conf_read_file(self, path=None):
        """
        Configure the cluster handle using a Ceph config file.

        :param path: path to the config file
        :type path: str
        """
        self.require_state("configuring", "connected")
        path = cstr(path, 'path', opt=True)
        cdef:
            char *_path = opt_str(path)
        with nogil:
            ret = rados_conf_read_file(self.cluster, _path)
        if ret != 0:
            raise make_ex(ret, "error calling conf_read_file")

    def conf_parse_argv(self, args):
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

    def conf_parse_env(self, var='CEPH_ARGS'):
        """
        Parse known arguments from an environment variable, normally
        CEPH_ARGS.
        """
        self.require_state("configuring", "connected")
        if not var:
            return

        var = cstr(var, 'var')
        cdef:
            char *_var = var
        with nogil:
            ret = rados_conf_parse_env(self.cluster, _var)
        if ret != 0:
            raise make_ex(ret, "error calling conf_parse_env")

    @requires(('option', str_type))
    def conf_get(self, option):
        """
        Get the value of a configuration option

        :param option: which option to read
        :type option: str

        :returns: str - value of the option or None
        :raises: :class:`TypeError`
        """
        self.require_state("configuring", "connected")
        option = cstr(option, 'option')
        cdef:
            char *_option = option
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

    @requires(('option', str_type), ('val', str_type))
    def conf_set(self, option, val):
        """
        Set the value of a configuration option

        :param option: which option to set
        :type option: str
        :param option: value of the option
        :type option: str

        :raises: :class:`TypeError`, :class:`ObjectNotFound`
        """
        self.require_state("configuring", "connected")
        option = cstr(option, 'option')
        val = cstr(val, 'val')
        cdef:
            char *_option = option
            char *_val = val

        with nogil:
            ret = rados_conf_set(self.cluster, _option, _val)
        if ret != 0:
            raise make_ex(ret, "error calling conf_set")

    def ping_monitor(self, mon_id):
        """
        Ping a monitor to assess liveness

        May be used as a simply way to assess liveness, or to obtain
        information about the monitor in a simple way even in the
        absence of quorum.

        :param mon_id: the ID portion of the monitor's name (i.e., mon.<ID>)
        :type mon_id: str
        :returns: the string reply from the monitor
        """

        self.require_state("configuring", "connected")

        mon_id = cstr(mon_id, 'mon_id')
        cdef:
            char *_mon_id = mon_id
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

    def connect(self, timeout=0):
        """
        Connect to the cluster.  Use shutdown() to release resources.
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

    def get_instance_id(self):
        """
        Get a global id for current instance
        """
        self.require_state("connected")
        with nogil:
            ret = rados_get_instance_id(self.cluster)
        return ret;

    def get_cluster_stats(self):
        """
        Read usage info about the cluster

        This tells you total space, space used, space available, and number
        of objects. These are not updated immediately when data is written,
        they are eventually consistent.

        :returns: dict - contains the following keys:

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

    @requires(('pool_name', str_type))
    def pool_exists(self, pool_name):
        """
        Checks if a given pool exists.

        :param pool_name: name of the pool to check
        :type pool_name: str

        :raises: :class:`TypeError`, :class:`Error`
        :returns: bool - whether the pool exists, false otherwise.
        """
        self.require_state("connected")

        pool_name = cstr(pool_name, 'pool_name')
        cdef:
            char *_pool_name = pool_name

        with nogil:
            ret = rados_pool_lookup(self.cluster, _pool_name)
        if ret >= 0:
            return True
        elif ret == -errno.ENOENT:
            return False
        else:
            raise make_ex(ret, "error looking up pool '%s'" % pool_name)

    @requires(('pool_name', str_type))
    def pool_lookup(self, pool_name):
        """
        Returns a pool's ID based on its name.

        :param pool_name: name of the pool to look up
        :type pool_name: str

        :raises: :class:`TypeError`, :class:`Error`
        :returns: int - pool ID, or None if it doesn't exist
        """
        self.require_state("connected")
        pool_name = cstr(pool_name, 'pool_name')
        cdef:
            char *_pool_name = pool_name

        with nogil:
            ret = rados_pool_lookup(self.cluster, _pool_name)
        if ret >= 0:
            return int(ret)
        elif ret == -errno.ENOENT:
            return None
        else:
            raise make_ex(ret, "error looking up pool '%s'" % pool_name)

    @requires(('pool_id', int))
    def pool_reverse_lookup(self, pool_id):
        """
        Returns a pool's name based on its ID.

        :param pool_id: ID of the pool to look up
        :type pool_id: int

        :raises: :class:`TypeError`, :class:`Error`
        :returns: string - pool name, or None if it doesn't exist
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

    @requires(('pool_name', str_type), ('crush_rule', opt(int)))
    def create_pool(self, pool_name, crush_rule=None):
        """
        Create a pool:
        - with default settings: if crush_rule=None
        - with a specific CRUSH rule: crush_rule given

        :param pool_name: name of the pool to create
        :type pool_name: str
        :param crush_rule: rule to use for placement in the new pool
        :type crush_rule: int

        :raises: :class:`TypeError`, :class:`Error`
        """
        self.require_state("connected")

        pool_name = cstr(pool_name, 'pool_name')
        cdef:
            char *_pool_name = pool_name
            uint8_t _crush_rule

        if crush_rule is None:
            with nogil:
                ret = rados_pool_create(self.cluster, _pool_name)
        else:
            _crush_rule = crush_rule
            with nogil:
                ret = rados_pool_create_with_crush_rule(self.cluster, _pool_name, _crush_rule)
        if ret < 0:
            raise make_ex(ret, "error creating pool '%s'" % pool_name)

    @requires(('pool_id', int))
    def get_pool_base_tier(self, pool_id):
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

    @requires(('pool_name', str_type))
    def delete_pool(self, pool_name):
        """
        Delete a pool and all data inside it.

        The pool is removed from the cluster immediately,
        but the actual data is deleted in the background.

        :param pool_name: name of the pool to delete
        :type pool_name: str

        :raises: :class:`TypeError`, :class:`Error`
        """
        self.require_state("connected")

        pool_name = cstr(pool_name, 'pool_name')
        cdef:
            char *_pool_name = pool_name

        with nogil:
            ret = rados_pool_delete(self.cluster, _pool_name)
        if ret < 0:
            raise make_ex(ret, "error deleting pool '%s'" % pool_name)

    @requires(('pool_id', int))
    def get_inconsistent_pgs(self, pool_id):
        """
        List inconsistent placement groups in the given pool

        :param pool_id: ID of the pool in which PGs are listed
        :type pool_id: int
        :returns: list - inconsistent placement groups
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

    def list_pools(self):
        """
        Gets a list of pool names.

        :returns: list - of pool names.
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

    def get_fsid(self):
        """
        Get the fsid of the cluster as a hexadecimal string.

        :raises: :class:`Error`
        :returns: str - cluster fsid
        """
        self.require_state("connected")
        cdef:
            char *ret_buf
            size_t buf_len = 37
            PyObject* ret_s = NULL

        ret_s = PyBytes_FromStringAndSize(NULL, buf_len)
        try:
            ret_buf = PyBytes_AsString(ret_s)
            with nogil:
                ret = rados_cluster_fsid(self.cluster, ret_buf, buf_len)
            if ret < 0:
                raise make_ex(ret, "error getting cluster fsid")
            if ret != <int>buf_len:
                _PyBytes_Resize(&ret_s, ret)
            return <object>ret_s
        finally:
            # We DECREF unconditionally: the cast to object above will have
            # INCREFed if necessary. This also takes care of exceptions,
            # including if _PyString_Resize fails (that will free the string
            # itself and set ret_s to NULL, hence XDECREF).
            ref.Py_XDECREF(ret_s)

    @requires(('ioctx_name', str_type))
    def open_ioctx(self, ioctx_name):
        """
        Create an io context

        The io context allows you to perform operations within a particular
        pool.

        :param ioctx_name: name of the pool
        :type ioctx_name: str

        :raises: :class:`TypeError`, :class:`Error`
        :returns: Ioctx - Rados Ioctx object
        """
        self.require_state("connected")
        ioctx_name = cstr(ioctx_name, 'ioctx_name')
        cdef:
            rados_ioctx_t ioctx
            char *_ioctx_name = ioctx_name
        with nogil:
            ret = rados_ioctx_create(self.cluster, _ioctx_name, &ioctx)
        if ret < 0:
            raise make_ex(ret, "error opening pool '%s'" % ioctx_name)
        io = Ioctx(ioctx_name)
        io.io = ioctx
        return io

    @requires(('pool_id', int))
    def open_ioctx2(self, pool_id):
        """
        Create an io context

        The io context allows you to perform operations within a particular
        pool.

        :param pool_id: ID of the pool
        :type pool_id: int

        :raises: :class:`TypeError`, :class:`Error`
        :returns: Ioctx - Rados Ioctx object
        """
        self.require_state("connected")
        cdef:
            rados_ioctx_t ioctx
            int64_t _pool_id = pool_id
        with nogil:
            ret = rados_ioctx_create2(self.cluster, _pool_id, &ioctx)
        if ret < 0:
            raise make_ex(ret, "error opening pool id '%s'" % pool_id)
        io = Ioctx(str(pool_id))
        io.io = ioctx
        return io

    def mon_command(self, cmd, inbuf, timeout=0, target=None):
        """
        mon_command[_target](cmd, inbuf, outbuf, outbuflen, outs, outslen)
        returns (int ret, string outbuf, string outs)
        """
        # NOTE(sileht): timeout is ignored because C API doesn't provide
        # timeout argument, but we keep it for backward compat with old python binding

        self.require_state("connected")
        cmd = cstr_list(cmd, 'c')

        if isinstance(target, int):
        # NOTE(sileht): looks weird but test_monmap_dump pass int
            target = str(target)

        target = cstr(target, 'target', opt=True)
        inbuf = cstr(inbuf, 'inbuf')

        cdef:
            char *_target = opt_str(target)
            char **_cmd = to_bytes_array(cmd)
            size_t _cmdlen = len(cmd)

            char *_inbuf = inbuf
            size_t _inbuf_len = len(inbuf)

            char *_outbuf
            size_t _outbuf_len
            char *_outs
            size_t _outs_len

        try:
            if target:
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

    def osd_command(self, osdid, cmd, inbuf, timeout=0):
        """
        osd_command(osdid, cmd, inbuf, outbuf, outbuflen, outs, outslen)
        returns (int ret, string outbuf, string outs)
        """
        # NOTE(sileht): timeout is ignored because C API doesn't provide
        # timeout argument, but we keep it for backward compat with old python binding
        self.require_state("connected")

        cmd = cstr_list(cmd, 'cmd')
        inbuf = cstr(inbuf, 'inbuf')

        cdef:
            int _osdid = osdid
            char **_cmd = to_bytes_array(cmd)
            size_t _cmdlen = len(cmd)

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

    def mgr_command(self, cmd, inbuf, timeout=0):
        """
        returns (int ret, string outbuf, string outs)
        """
        # NOTE(sileht): timeout is ignored because C API doesn't provide
        # timeout argument, but we keep it for backward compat with old python binding
        self.require_state("connected")

        cmd = cstr_list(cmd, 'cmd')
        inbuf = cstr(inbuf, 'inbuf')

        cdef:
            char **_cmd = to_bytes_array(cmd)
            size_t _cmdlen = len(cmd)

            char *_inbuf = inbuf
            size_t _inbuf_len = len(inbuf)

            char *_outbuf
            size_t _outbuf_len
            char *_outs
            size_t _outs_len

        try:
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

    def pg_command(self, pgid, cmd, inbuf, timeout=0):
        """
        pg_command(pgid, cmd, inbuf, outbuf, outbuflen, outs, outslen)
        returns (int ret, string outbuf, string outs)
        """
        # NOTE(sileht): timeout is ignored because C API doesn't provide
        # timeout argument, but we keep it for backward compat with old python binding
        self.require_state("connected")

        pgid = cstr(pgid, 'pgid')
        cmd = cstr_list(cmd, 'cmd')
        inbuf = cstr(inbuf, 'inbuf')

        cdef:
            char *_pgid = pgid
            char **_cmd = to_bytes_array(cmd)
            size_t _cmdlen = len(cmd)

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

    def wait_for_latest_osdmap(self):
        self.require_state("connected")
        with nogil:
            ret = rados_wait_for_latest_osdmap(self.cluster)
        return ret

    def blacklist_add(self, client_address, expire_seconds=0):
        """
        Blacklist a client from the OSDs

        :param client_address: client address
        :type client_address: str
        :param expire_seconds: number of seconds to blacklist
        :type expire_seconds: int

        :raises: :class:`Error`
        """
        self.require_state("connected")
        client_address =  cstr(client_address, 'client_address')
        cdef:
            uint32_t _expire_seconds = expire_seconds
            char *_client_address = client_address

        with nogil:
            ret = rados_blacklist_add(self.cluster, _client_address, _expire_seconds)
        if ret < 0:
            raise make_ex(ret, "error blacklisting client '%s'" % client_address)

    def monitor_log(self, level, callback, arg):
        if level not in MONITOR_LEVELS:
            raise LogicError("invalid monitor level " + level)
        if callback is not None and not callable(callback):
            raise LogicError("callback must be a callable function or None")

        level = cstr(level, 'level')
        cdef char *_level = level

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

    def monitor_log2(self, level, callback, arg):
        if level not in MONITOR_LEVELS:
            raise LogicError("invalid monitor level " + level)
        if callback is not None and not callable(callback):
            raise LogicError("callback must be a callable function or None")

        level = cstr(level, 'level')
        cdef char *_level = level

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

    @requires(('service', str_type), ('daemon', str_type), ('metadata', dict))
    def service_daemon_register(self, service, daemon, metadata):
        """
        :param str service: service name (e.g. "rgw")
        :param str daemon: daemon name (e.g. "gwfoo")
        :param dict metadata: static metadata about the register daemon
               (e.g., the version of Ceph, the kernel version.)
        """
        service = cstr(service, 'service')
        daemon = cstr(daemon, 'daemon')
        metadata_dict = '\0'.join(chain.from_iterable(metadata.items()))
        metadata_dict += '\0'
        cdef:
            char *_service = service
            char *_daemon = daemon
            char *_metadata = metadata_dict

        with nogil:
            ret = rados_service_register(self.cluster, _service, _daemon, _metadata)
        if ret != 0:
            raise make_ex(ret, "error calling service_register()")

    @requires(('metadata', dict))
    def service_daemon_update(self, status):
        status_dict = '\0'.join(chain.from_iterable(status.items()))
        status_dict += '\0'
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

        with nogil:
            ret = rados_nobjects_list_next(self.ctx, &key_, &locator_, &nspace_)

        if ret < 0:
            raise StopIteration()

        key = decode_cstr(key_)
        locator = decode_cstr(locator_) if locator_ != NULL else None
        nspace = decode_cstr(nspace_) if nspace_ != NULL else None
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

    def __iter__(self):
        return self

    def __next__(self):
        """
        Get the next Snapshot

        :raises: :class:`Error`, StopIteration
        :returns: Snap - next snapshot
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

    def get_timestamp(self):
        """
        Find when a snapshot in the current pool occurred

        :raises: :class:`Error`
        :returns: datetime - the data and time the snapshot was created
        """
        cdef time_t snap_time

        with nogil:
            ret = rados_ioctx_snap_get_stamp(self.ioctx.io, self.snap_id, &snap_time)
        if ret != 0:
            raise make_ex(ret, "rados_ioctx_snap_get_stamp error")
        return datetime.fromtimestamp(snap_time)


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

    def is_safe(self):
        """
        Is an asynchronous operation safe?

        This does not imply that the safe callback has finished.

        :returns: True if the operation is safe
        """
        with nogil:
            ret = rados_aio_is_safe(self.rados_comp)
        return ret == 1

    def is_complete(self):
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

        This does not imply that the safe callback has finished.
        """
        with nogil:
            rados_aio_wait_for_safe(self.rados_comp)

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
        with nogil:
            rados_aio_wait_for_safe_and_cb(self.rados_comp)

    def wait_for_complete_and_cb(self):
        """
        Wait for an asynchronous operation to complete and for the
        complete callback to have returned

        :returns:  whether the operation is completed
        """
        with nogil:
            ret = rados_aio_wait_for_complete_and_cb(self.rados_comp)
        return ret

    def get_return_value(self):
        """
        Get the return value of an asychronous operation

        The return value is set when the operation is complete or safe,
        whichever comes first.

        :returns: int - return value of the operation
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
        with self.ioctx.lock:
            if self.oncomplete:
                self.ioctx.complete_completions.remove(self)

    def _safe(self):
        self.onsafe(self)
        with self.ioctx.lock:
            if self.onsafe:
                self.ioctx.safe_completions.remove(self)

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

    @requires(('exclusive', opt(int)))
    def new(self, exclusive=None):
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

    @requires(('flags', int))
    def set_flags(self, flags=LIBRADOS_OPERATION_NOFLAG):
        """
        Set flags for the last operation added to this write_op.
        :para flags: flags to apply to the last operation
        :type flags: int
        """

        cdef:
            int _flags = flags

        with nogil:
            rados_write_op_set_flags(self.write_op, _flags)

    @requires(('to_write', bytes))
    def append(self, to_write):
        """
        Append data to an object synchronously
        :param to_write: data to write
        :type to_write: bytes
        """

        cdef:
            char *_to_write = to_write
            size_t length = len(to_write)

        with nogil:
            rados_write_op_append(self.write_op, _to_write, length)

    @requires(('to_write', bytes))
    def write_full(self, to_write):
        """
        Write whole object, atomically replacing it.
        :param to_write: data to write
        :type to_write: bytes
        """

        cdef:
            char *_to_write = to_write
            size_t length = len(to_write)

        with nogil:
            rados_write_op_write_full(self.write_op, _to_write, length)

    @requires(('to_write', bytes), ('offset', int))
    def write(self, to_write, offset=0):
        """
        Write to offset.
        :param to_write: data to write
        :type to_write: bytes
        :param offset: byte offset in the object to begin writing at
        :type offset: int
        """

        cdef:
            char *_to_write = to_write
            size_t length = len(to_write)
            uint64_t _offset = offset

        with nogil:
            rados_write_op_write(self.write_op, _to_write, length, _offset)

    @requires(('version', int))
    def assert_version(self, version):
        """
        Check if object's version is the expected one.
        :param version: expected version of the object
        :param type: int
        """
        cdef:
            uint64_t _version = version

        with nogil:
            rados_write_op_assert_version(self.write_op, _version)

    @requires(('offset', int), ('length', int))
    def zero(self, offset, length):
        """
        Zero part of an object.
        :param offset: byte offset in the object to begin writing at
        :type offset: int
        :param offset: number of zero to write
        :type offset: int
        """

        cdef:
            size_t _length = length
            uint64_t _offset = offset

        with nogil:
            rados_write_op_zero(self.write_op, _length, _offset)

    @requires(('offset', int))
    def truncate(self, offset):
        """
        Truncate an object.
        :param offset: byte offset in the object to begin truncating at
        :type offset: int
        """

        cdef:
            uint64_t _offset = offset

        with nogil:
            rados_write_op_truncate(self.write_op,  _offset)


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

    @requires(('flags', int))
    def set_flags(self, flags=LIBRADOS_OPERATION_NOFLAG):
        """
        Set flags for the last operation added to this read_op.
        :para flags: flags to apply to the last operation
        :type flags: int
        """

        cdef:
            int _flags = flags

        with nogil:
            rados_read_op_set_flags(self.read_op, _flags)


class ReadOpCtx(ReadOp, OpCtx):
    """read operation context manager"""


cdef int __aio_safe_cb(rados_completion_t completion, void *args) with gil:
    """
    Callback to onsafe() for asynchronous operations
    """
    cdef object cb = <object>args
    cb._safe()
    return 0


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

    def __init__(self, name):
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

    def __get_completion(self, oncomplete, onsafe):
        """
        Constructs a completion to use with asynchronous operations

        :param oncomplete: what to do when the write is safe and complete in memory
            on all replicas
        :type oncomplete: completion
        :param onsafe:  what to do when the write is safe and complete on storage
            on all replicas
        :type onsafe: completion

        :raises: :class:`Error`
        :returns: completion object
        """

        completion_obj = Completion(self, oncomplete, onsafe)

        cdef:
            rados_callback_t complete_cb = NULL
            rados_callback_t safe_cb = NULL
            rados_completion_t completion
            PyObject* p_completion_obj= <PyObject*>completion_obj

        if oncomplete:
            complete_cb = <rados_callback_t>&__aio_complete_cb
        if onsafe:
            safe_cb = <rados_callback_t>&__aio_safe_cb

        with nogil:
            ret = rados_aio_create_completion(p_completion_obj, complete_cb, safe_cb,
                                              &completion)
        if ret < 0:
            raise make_ex(ret, "error getting a completion")

        completion_obj.rados_comp = completion
        return completion_obj

    @requires(('object_name', str_type), ('oncomplete', opt(Callable)))
    def aio_stat(self, object_name, oncomplete):
        """
        Asynchronously get object stats (size/mtime)

        oncomplete will be called with the returned size and mtime
        as well as the completion:

        oncomplete(completion, size, mtime)

        :param object_name: the name of the object to get stats from
        :type object_name: str
        :param oncomplete: what to do when the stat is complete
        :type oncomplete: completion

        :raises: :class:`Error`
        :returns: completion object
        """

        object_name = cstr(object_name, 'object_name')

        cdef:
            Completion completion
            char *_object_name = object_name
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

    @requires(('object_name', str_type), ('to_write', bytes), ('offset', int),
              ('oncomplete', opt(Callable)), ('onsafe', opt(Callable)))
    def aio_write(self, object_name, to_write, offset=0,
                  oncomplete=None, onsafe=None):
        """
        Write data to an object asynchronously

        Queues the write and returns.

        :param object_name: name of the object
        :type object_name: str
        :param to_write: data to write
        :type to_write: bytes
        :param offset: byte offset in the object to begin writing at
        :type offset: int
        :param oncomplete: what to do when the write is safe and complete in memory
            on all replicas
        :type oncomplete: completion
        :param onsafe:  what to do when the write is safe and complete on storage
            on all replicas
        :type onsafe: completion

        :raises: :class:`Error`
        :returns: completion object
        """

        object_name = cstr(object_name, 'object_name')

        cdef:
            Completion completion
            char* _object_name = object_name
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

    @requires(('object_name', str_type), ('to_write', bytes), ('oncomplete', opt(Callable)),
              ('onsafe', opt(Callable)))
    def aio_write_full(self, object_name, to_write,
                       oncomplete=None, onsafe=None):
        """
        Asynchronously write an entire object

        The object is filled with the provided data. If the object exists,
        it is atomically truncated and then written.
        Queues the write and returns.

        :param object_name: name of the object
        :type object_name: str
        :param to_write: data to write
        :type to_write: str
        :param oncomplete: what to do when the write is safe and complete in memory
            on all replicas
        :type oncomplete: completion
        :param onsafe:  what to do when the write is safe and complete on storage
            on all replicas
        :type onsafe: completion

        :raises: :class:`Error`
        :returns: completion object
        """

        object_name = cstr(object_name, 'object_name')

        cdef:
            Completion completion
            char* _object_name = object_name
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

    @requires(('object_name', str_type), ('to_append', bytes), ('oncomplete', opt(Callable)),
              ('onsafe', opt(Callable)))
    def aio_append(self, object_name, to_append, oncomplete=None, onsafe=None):
        """
        Asynchronously append data to an object

        Queues the write and returns.

        :param object_name: name of the object
        :type object_name: str
        :param to_append: data to append
        :type to_append: str
        :param offset: byte offset in the object to begin writing at
        :type offset: int
        :param oncomplete: what to do when the write is safe and complete in memory
            on all replicas
        :type oncomplete: completion
        :param onsafe:  what to do when the write is safe and complete on storage
            on all replicas
        :type onsafe: completion

        :raises: :class:`Error`
        :returns: completion object
        """
        object_name = cstr(object_name, 'object_name')

        cdef:
            Completion completion
            char* _object_name = object_name
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

    @requires(('object_name', str_type), ('length', int), ('offset', int),
              ('oncomplete', opt(Callable)))
    def aio_read(self, object_name, length, offset, oncomplete):
        """
        Asynchronously read data from an object

        oncomplete will be called with the returned read value as
        well as the completion:

        oncomplete(completion, data_read)

        :param object_name: name of the object to read from
        :type object_name: str
        :param length: the number of bytes to read
        :type length: int
        :param offset: byte offset in the object to begin reading from
        :type offset: int
        :param oncomplete: what to do when the read is complete
        :type oncomplete: completion

        :raises: :class:`Error`
        :returns: completion object
        """

        object_name = cstr(object_name, 'object_name')

        cdef:
            Completion completion
            char* _object_name = object_name
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

    @requires(('object_name', str_type), ('cls', str_type), ('method', str_type),
              ('data', bytes), ('length', int),
              ('oncomplete', opt(Callable)), ('onsafe', opt(Callable)))
    def aio_execute(self, object_name, cls, method, data,
                    length=8192, oncomplete=None, onsafe=None):
        """
        Asynchronously execute an OSD class method on an object.

        oncomplete and onsafe will be called with the data returned from
        the plugin as well as the completion:

        oncomplete(completion, data)
        onsafe(completion, data)

        :param object_name: name of the object
        :type object_name: str
        :param cls: name of the object class
        :type cls: str
        :param method: name of the method
        :type method: str
        :param data: input data
        :type data: bytes
        :param length: size of output buffer in bytes (default=8192)
        :type length: int
        :param oncomplete: what to do when the execution is complete
        :type oncomplete: completion
        :param onsafe:  what to do when the execution is safe and complete
        :type onsafe: completion

        :raises: :class:`Error`
        :returns: completion object
        """

        object_name = cstr(object_name, 'object_name')
        cls = cstr(cls, 'cls')
        method = cstr(method, 'method')
        cdef:
            Completion completion
            char *_object_name = object_name
            char *_cls = cls
            char *_method = method
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

    @requires(('object_name', str_type), ('oncomplete', opt(Callable)), ('onsafe', opt(Callable)))
    def aio_remove(self, object_name, oncomplete=None, onsafe=None):
        """
        Asynchronously remove an object

        :param object_name: name of the object to remove
        :type object_name: str
        :param oncomplete: what to do when the remove is safe and complete in memory
            on all replicas
        :type oncomplete: completion
        :param onsafe:  what to do when the remove is safe and complete on storage
            on all replicas
        :type onsafe: completion

        :raises: :class:`Error`
        :returns: completion object
        """
        object_name = cstr(object_name, 'object_name')

        cdef:
            Completion completion
            char* _object_name = object_name

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

    @requires(('loc_key', str_type))
    def set_locator_key(self, loc_key):
        """
        Set the key for mapping objects to pgs within an io context.

        The key is used instead of the object name to determine which
        placement groups an object is put in. This affects all subsequent
        operations of the io context - until a different locator key is
        set, all objects in this io context will be placed in the same pg.

        :param loc_key: the key to use as the object locator, or NULL to discard
            any previously set key
        :type loc_key: str

        :raises: :class:`TypeError`
        """
        self.require_ioctx_open()
        cloc_key = cstr(loc_key, 'loc_key')
        cdef char *_loc_key = cloc_key
        with nogil:
            rados_ioctx_locator_set_key(self.io, _loc_key)
        self.locator_key = loc_key

    def get_locator_key(self):
        """
        Get the locator_key of context

        :returns: locator_key
        """
        return self.locator_key

    @requires(('snap_id', long))
    def set_read(self, snap_id):
        """
        Set the snapshot for reading objects.

        To stop to read from snapshot, use set_read(LIBRADOS_SNAP_HEAD)

        :param snap_id: the snapshot Id
        :type snap_id: int

        :raises: :class:`TypeError`
        """
        self.require_ioctx_open()
        cdef rados_snap_t _snap_id = snap_id
        with nogil:
            rados_ioctx_snap_set_read(self.io, _snap_id)

    @requires(('nspace', str_type))
    def set_namespace(self, nspace):
        """
        Set the namespace for objects within an io context.

        The namespace in addition to the object name fully identifies
        an object. This affects all subsequent operations of the io context
        - until a different namespace is set, all objects in this io context
        will be placed in the same namespace.

        :param nspace: the namespace to use, or None/"" for the default namespace
        :type nspace: str

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

    def get_namespace(self):
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


    @requires(('key', str_type), ('data', bytes))
    def write(self, key, data, offset=0):
        """
        Write data to an object synchronously

        :param key: name of the object
        :type key: str
        :param data: data to write
        :type data: bytes
        :param offset: byte offset in the object to begin writing at
        :type offset: int

        :raises: :class:`TypeError`
        :raises: :class:`LogicError`
        :returns: int - 0 on success
        """
        self.require_ioctx_open()

        key = cstr(key, 'key')
        cdef:
            char *_key = key
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

    @requires(('key', str_type), ('data', bytes))
    def write_full(self, key, data):
        """
        Write an entire object synchronously.

        The object is filled with the provided data. If the object exists,
        it is atomically truncated and then written.

        :param key: name of the object
        :type key: str
        :param data: data to write
        :type data: bytes

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: int - 0 on success
        """
        self.require_ioctx_open()
        key = cstr(key, 'key')
        cdef:
            char *_key = key
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

    @requires(('key', str_type), ('data', bytes))
    def append(self, key, data):
        """
        Append data to an object synchronously

        :param key: name of the object
        :type key: str
        :param data: data to write
        :type data: bytes

        :raises: :class:`TypeError`
        :raises: :class:`LogicError`
        :returns: int - 0 on success
        """
        self.require_ioctx_open()
        key = cstr(key, 'key')
        cdef:
            char *_key = key
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

    @requires(('key', str_type))
    def read(self, key, length=8192, offset=0):
        """
        Read data from an object synchronously

        :param key: name of the object
        :type key: str
        :param length: the number of bytes to read (default=8192)
        :type length: int
        :param offset: byte offset in the object to begin reading at
        :type offset: int

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: str - data read from object
        """
        self.require_ioctx_open()
        key = cstr(key, 'key')
        cdef:
            char *_key = key
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

    @requires(('key', str_type), ('cls', str_type), ('method', str_type), ('data', bytes))
    def execute(self, key, cls, method, data, length=8192):
        """
        Execute an OSD class method on an object.

        :param key: name of the object
        :type key: str
        :param cls: name of the object class
        :type cls: str
        :param method: name of the method
        :type method: str
        :param data: input data
        :type data: bytes
        :param length: size of output buffer in bytes (default=8192)
        :type length: int

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: (ret, method output)
        """
        self.require_ioctx_open()

        key = cstr(key, 'key')
        cls = cstr(cls, 'cls')
        method = cstr(method, 'method')
        cdef:
            char *_key = key
            char *_cls = cls
            char *_method = method
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

    def get_stats(self):
        """
        Get pool usage statistics

        :returns: dict - contains the following keys:

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

    @requires(('key', str_type))
    def remove_object(self, key):
        """
        Delete an object

        This does not delete any snapshots of the object.

        :param key: the name of the object to delete
        :type key: str

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: bool - True on success
        """
        self.require_ioctx_open()
        key = cstr(key, 'key')
        cdef:
            char *_key = key

        with nogil:
            ret = rados_remove(self.io, _key)
        if ret < 0:
            raise make_ex(ret, "Failed to remove '%s'" % key)
        return True

    @requires(('key', str_type))
    def trunc(self, key, size):
        """
        Resize an object

        If this enlarges the object, the new area is logically filled with
        zeroes. If this shrinks the object, the excess data is removed.

        :param key: the name of the object to resize
        :type key: str
        :param size: the new size of the object in bytes
        :type size: int

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: int - 0 on success, otherwise raises error
        """

        self.require_ioctx_open()
        key = cstr(key, 'key')
        cdef:
            char *_key = key
            uint64_t _size = size

        with nogil:
            ret = rados_trunc(self.io, _key, _size)
        if ret < 0:
            raise make_ex(ret, "Ioctx.trunc(%s): failed to truncate %s" % (self.name, key))
        return ret

    @requires(('key', str_type))
    def stat(self, key):
        """
        Get object stats (size/mtime)

        :param key: the name of the object to get stats from
        :type key: str

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: (size,timestamp)
        """
        self.require_ioctx_open()

        key = cstr(key, 'key')
        cdef:
            char *_key = key
            uint64_t psize
            time_t pmtime

        with nogil:
            ret = rados_stat(self.io, _key, &psize, &pmtime)
        if ret < 0:
            raise make_ex(ret, "Failed to stat %r" % key)
        return psize, time.localtime(pmtime)

    @requires(('key', str_type), ('xattr_name', str_type))
    def get_xattr(self, key, xattr_name):
        """
        Get the value of an extended attribute on an object.

        :param key: the name of the object to get xattr from
        :type key: str
        :param xattr_name: which extended attribute to read
        :type xattr_name: str

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: str - value of the xattr
        """
        self.require_ioctx_open()

        key = cstr(key, 'key')
        xattr_name = cstr(xattr_name, 'xattr_name')
        cdef:
            char *_key = key
            char *_xattr_name = xattr_name
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

    @requires(('oid', str_type))
    def get_xattrs(self, oid):
        """
        Start iterating over xattrs on an object.

        :param oid: the name of the object to get xattrs from
        :type oid: str

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: XattrIterator
        """
        self.require_ioctx_open()
        return XattrIterator(self, oid)

    @requires(('key', str_type), ('xattr_name', str_type), ('xattr_value', bytes))
    def set_xattr(self, key, xattr_name, xattr_value):
        """
        Set an extended attribute on an object.

        :param key: the name of the object to set xattr to
        :type key: str
        :param xattr_name: which extended attribute to set
        :type xattr_name: str
        :param xattr_value: the value of the  extended attribute
        :type xattr_value: bytes

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: bool - True on success, otherwise raise an error
        """
        self.require_ioctx_open()

        key = cstr(key, 'key')
        xattr_name = cstr(xattr_name, 'xattr_name')
        cdef:
            char *_key = key
            char *_xattr_name = xattr_name
            char *_xattr_value = xattr_value
            size_t _xattr_value_len = len(xattr_value)

        with nogil:
            ret = rados_setxattr(self.io, _key, _xattr_name,
                                 _xattr_value, _xattr_value_len)
        if ret < 0:
            raise make_ex(ret, "Failed to set xattr %r" % xattr_name)
        return True

    @requires(('key', str_type), ('xattr_name', str_type))
    def rm_xattr(self, key, xattr_name):
        """
        Removes an extended attribute on from an object.

        :param key: the name of the object to remove xattr from
        :type key: str
        :param xattr_name: which extended attribute to remove
        :type xattr_name: str

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: bool - True on success, otherwise raise an error
        """
        self.require_ioctx_open()

        key = cstr(key, 'key')
        xattr_name = cstr(xattr_name, 'xattr_name')
        cdef:
            char *_key = key
            char *_xattr_name = xattr_name

        with nogil:
            ret = rados_rmxattr(self.io, _key, _xattr_name)
        if ret < 0:
            raise make_ex(ret, "Failed to delete key %r xattr %r" %
                          (key, xattr_name))
        return True

    @requires(('obj', str_type), ('msg', str_type), ('timeout_ms', int))
    def notify(self, obj, msg='', timeout_ms=5000):
        """
        Send a rados notification to an object.

        :param obj: the name of the object to notify
        :type obj: str
        :param msg: optional message to send in the notification
        :type msg: str
        :param timeout_ms: notify timeout (in ms)
        :type timeout_ms: int

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: bool - True on success, otherwise raise an error
        """
        self.require_ioctx_open()

        msglen = len(msg)
        obj = cstr(obj, 'obj')
        msg = cstr(msg, 'msg')
        cdef:
            char *_obj = obj
            char *_msg = msg
            int _msglen = msglen
            uint64_t _timeout_ms = timeout_ms

        with nogil:
            ret = rados_notify2(self.io, _obj, _msg, _msglen, _timeout_ms,
                                NULL, NULL)
        if ret < 0:
            raise make_ex(ret, "Failed to notify %r" % (obj))
        return True

    def list_objects(self):
        """
        Get ObjectIterator on rados.Ioctx object.

        :returns: ObjectIterator
        """
        self.require_ioctx_open()
        return ObjectIterator(self)

    def list_snaps(self):
        """
        Get SnapIterator on rados.Ioctx object.

        :returns: SnapIterator
        """
        self.require_ioctx_open()
        return SnapIterator(self)

    @requires(('snap_name', str_type))
    def create_snap(self, snap_name):
        """
        Create a pool-wide snapshot

        :param snap_name: the name of the snapshot
        :type snap_name: str

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        """
        self.require_ioctx_open()
        snap_name = cstr(snap_name, 'snap_name')
        cdef char *_snap_name = snap_name

        with nogil:
            ret = rados_ioctx_snap_create(self.io, _snap_name)
        if ret != 0:
            raise make_ex(ret, "Failed to create snap %s" % snap_name)

    @requires(('snap_name', str_type))
    def remove_snap(self, snap_name):
        """
        Removes a pool-wide snapshot

        :param snap_name: the name of the snapshot
        :type snap_name: str

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        """
        self.require_ioctx_open()
        snap_name = cstr(snap_name, 'snap_name')
        cdef char *_snap_name = snap_name

        with nogil:
            ret = rados_ioctx_snap_remove(self.io, _snap_name)
        if ret != 0:
            raise make_ex(ret, "Failed to remove snap %s" % snap_name)

    @requires(('snap_name', str_type))
    def lookup_snap(self, snap_name):
        """
        Get the id of a pool snapshot

        :param snap_name: the name of the snapshot to lookop
        :type snap_name: str

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

    @requires(('oid', str_type), ('snap_name', str_type))
    def snap_rollback(self, oid, snap_name):
        """
        Rollback an object to a snapshot

        :param oid: the name of the object
        :type oid: str
        :param snap_name: the name of the snapshot
        :type snap_name: str

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        """
        self.require_ioctx_open()
        oid = cstr(oid, 'oid')
        snap_name = cstr(snap_name, 'snap_name')
        cdef:
            char *_snap_name = snap_name
            char *_oid = oid

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

    @requires(('snap_id', int))
    def remove_self_managed_snap(self, snap_id):
        """
        Removes a self-managed snapshot

        :param snap_id: the name of the snapshot
        :type snap_id: int

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

    def set_self_managed_snap_write(self, snaps):
        """
        Updates the write context to the specified self-managed
        snapshot ids.

        :param snaps: all associated self-managed snapshot ids
        :type snaps: list

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

    @requires(('oid', str_type), ('snap_id', int))
    def rollback_self_managed_snap(self, oid, snap_id):
        """
        Rolls an specific object back to a self-managed snapshot revision

        :param oid: the name of the object
        :type oid: str
        :param snap_id: the name of the snapshot
        :type snap_id: int

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        """
        self.require_ioctx_open()
        oid = cstr(oid, 'oid')
        cdef:
            char *_oid = oid
            rados_snap_t _snap_id = snap_id
        with nogil:
            ret = rados_ioctx_selfmanaged_snap_rollback(self.io, _oid, _snap_id)
        if ret != 0:
            raise make_ex(ret, "Failed to rollback %s" % oid)

    def get_last_version(self):
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

    def create_write_op(self):
        """
        create write operation object.
        need call release_write_op after use
        """
        return WriteOp().create()

    def create_read_op(self):
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

    def release_read_op(self, read_op):
        """
        release memory alloc by create_read_op
        :para read_op: read_op object
        :type: int
        """
        read_op.release()

    @requires(('write_op', WriteOp), ('keys', tuple), ('values', tuple))
    def set_omap(self, write_op, keys, values):
        """
        set keys values to write_op
        :para write_op: write_operation object
        :type write_op: WriteOp
        :para keys: a tuple of keys
        :type keys: tuple
        :para values: a tuple of values
        :type values: tuple
        """

        if len(keys) != len(values):
            raise Error("Rados(): keys and values must have the same number of items")

        keys = cstr_list(keys, 'keys')
        cdef:
            WriteOp _write_op = write_op
            size_t key_num = len(keys)
            char **_keys = to_bytes_array(keys)
            char **_values = to_bytes_array(values)
            size_t *_lens = to_csize_t_array([len(v) for v in values])

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

    @requires(('write_op', WriteOp), ('oid', str_type), ('mtime', opt(int)), ('flags', opt(int)))
    def operate_write_op(self, write_op, oid, mtime=0, flags=LIBRADOS_OPERATION_NOFLAG):
        """
        execute the real write operation
        :para write_op: write operation object
        :type write_op: WriteOp
        :para oid: object name
        :type oid: str
        :para mtime: the time to set the mtime to, 0 for the current time
        :type mtime: int
        :para flags: flags to apply to the entire operation
        :type flags: int
        """

        oid = cstr(oid, 'oid')
        cdef:
            WriteOp _write_op = write_op
            char *_oid = oid
            time_t _mtime = mtime
            int _flags = flags

        with nogil:
            ret = rados_write_op_operate(_write_op.write_op, self.io, _oid, &_mtime, _flags)
        if ret != 0:
            raise make_ex(ret, "Failed to operate write op for oid %s" % oid)

    @requires(('write_op', WriteOp), ('oid', str_type), ('oncomplete', opt(Callable)), ('onsafe', opt(Callable)), ('mtime', opt(int)), ('flags', opt(int)))
    def operate_aio_write_op(self, write_op, oid, oncomplete=None, onsafe=None, mtime=0, flags=LIBRADOS_OPERATION_NOFLAG):
        """
        execute the real write operation asynchronously
        :para write_op: write operation object
        :type write_op: WriteOp
        :para oid: object name
        :type oid: str
        :param oncomplete: what to do when the remove is safe and complete in memory
            on all replicas
        :type oncomplete: completion
        :param onsafe:  what to do when the remove is safe and complete on storage
            on all replicas
        :type onsafe: completion
        :para mtime: the time to set the mtime to, 0 for the current time
        :type mtime: int
        :para flags: flags to apply to the entire operation
        :type flags: int

        :raises: :class:`Error`
        :returns: completion object
        """

        oid = cstr(oid, 'oid')
        cdef:
            WriteOp _write_op = write_op
            char *_oid = oid
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

    @requires(('read_op', ReadOp), ('oid', str_type), ('flag', opt(int)))
    def operate_read_op(self, read_op, oid, flag=LIBRADOS_OPERATION_NOFLAG):
        """
        execute the real read operation
        :para read_op: read operation object
        :type read_op: ReadOp
        :para oid: object name
        :type oid: str
        :para flag: flags to apply to the entire operation
        :type flag: int
        """
        oid = cstr(oid, 'oid')
        cdef:
            ReadOp _read_op = read_op
            char *_oid = oid
            int _flag = flag

        with nogil:
            ret = rados_read_op_operate(_read_op.read_op, self.io, _oid, _flag)
        if ret != 0:
            raise make_ex(ret, "Failed to operate read op for oid %s" % oid)

    @requires(('read_op', ReadOp), ('oid', str_type), ('oncomplete', opt(Callable)), ('onsafe', opt(Callable)), ('flag', opt(int)))
    def operate_aio_read_op(self, read_op, oid, oncomplete=None, onsafe=None, flag=LIBRADOS_OPERATION_NOFLAG):
        """
        execute the real read operation
        :para read_op: read operation object
        :type read_op: ReadOp
        :para oid: object name
        :type oid: str
        :param oncomplete: what to do when the remove is safe and complete in memory
            on all replicas
        :type oncomplete: completion
        :param onsafe:  what to do when the remove is safe and complete on storage
            on all replicas
        :type onsafe: completion
        :para flag: flags to apply to the entire operation
        :type flag: int
        """
        oid = cstr(oid, 'oid')
        cdef:
            ReadOp _read_op = read_op
            char *_oid = oid
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

    @requires(('read_op', ReadOp), ('start_after', str_type), ('filter_prefix', str_type), ('max_return', int))
    def get_omap_vals(self, read_op, start_after, filter_prefix, max_return):
        """
        get the omap values
        :para read_op: read operation object
        :type read_op: ReadOp
        :para start_after: list keys starting after start_after
        :type start_after: str
        :para filter_prefix: list only keys beginning with filter_prefix
        :type filter_prefix: str
        :para max_return: list no more than max_return key/value pairs
        :type max_return: int
        :returns: an iterator over the requested omap values, return value from this action
        """

        start_after = cstr(start_after, 'start_after') if start_after else None
        filter_prefix = cstr(filter_prefix, 'filter_prefix') if filter_prefix else None
        cdef:
            char *_start_after = opt_str(start_after)
            char *_filter_prefix = opt_str(filter_prefix)
            ReadOp _read_op = read_op
            rados_omap_iter_t iter_addr = NULL
            int _max_return = max_return

        with nogil:
            rados_read_op_omap_get_vals2(_read_op.read_op, _start_after, _filter_prefix,
                                         _max_return, &iter_addr, NULL, NULL)
        it = OmapIterator(self)
        it.ctx = iter_addr
        return it, 0   # 0 is meaningless; there for backward-compat

    @requires(('read_op', ReadOp), ('start_after', str_type), ('max_return', int))
    def get_omap_keys(self, read_op, start_after, max_return):
        """
        get the omap keys
        :para read_op: read operation object
        :type read_op: ReadOp
        :para start_after: list keys starting after start_after
        :type start_after: str
        :para max_return: list no more than max_return key/value pairs
        :type max_return: int
        :returns: an iterator over the requested omap values, return value from this action
        """
        start_after = cstr(start_after, 'start_after') if start_after else None
        cdef:
            char *_start_after = opt_str(start_after)
            ReadOp _read_op = read_op
            rados_omap_iter_t iter_addr = NULL
            int _max_return = max_return

        with nogil:
            rados_read_op_omap_get_keys2(_read_op.read_op, _start_after,
                                         _max_return, &iter_addr, NULL, NULL)
        it = OmapIterator(self)
        it.ctx = iter_addr
        return it, 0   # 0 is meaningless; there for backward-compat

    @requires(('read_op', ReadOp), ('keys', tuple))
    def get_omap_vals_by_keys(self, read_op, keys):
        """
        get the omap values by keys
        :para read_op: read operation object
        :type read_op: ReadOp
        :para keys: input key tuple
        :type keys: tuple
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

    @requires(('write_op', WriteOp), ('keys', tuple))
    def remove_omap_keys(self, write_op, keys):
        """
        remove omap keys specifiled
        :para write_op: write operation object
        :type write_op: WriteOp
        :para keys: input key tuple
        :type keys: tuple
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

    @requires(('write_op', WriteOp))
    def clear_omap(self, write_op):
        """
        Remove all key/value pairs from an object
        :para write_op: write operation object
        :type write_op: WriteOp
        """

        cdef:
            WriteOp _write_op = write_op

        with nogil:
            rados_write_op_omap_clear(_write_op.write_op)

    @requires(('key', str_type), ('name', str_type), ('cookie', str_type), ('desc', str_type),
              ('duration', opt(int)), ('flags', int))
    def lock_exclusive(self, key, name, cookie, desc="", duration=None, flags=0):

        """
        Take an exclusive lock on an object

        :param key: name of the object
        :type key: str
        :param name: name of the lock
        :type name: str
        :param cookie: cookie of the lock
        :type cookie: str
        :param desc: description of the lock
        :type desc: str
        :param duration: duration of the lock in seconds
        :type duration: int
        :param flags: flags
        :type flags: int

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        """
        self.require_ioctx_open()

        key = cstr(key, 'key')
        name = cstr(name, 'name')
        cookie = cstr(cookie, 'cookie')
        desc = cstr(desc, 'desc')

        cdef:
            char* _key = key
            char* _name = name
            char* _cookie = cookie
            char* _desc = desc
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

    @requires(('key', str_type), ('name', str_type), ('cookie', str_type), ('tag', str_type),
              ('desc', str_type), ('duration', opt(int)), ('flags', int))
    def lock_shared(self, key, name, cookie, tag, desc="", duration=None, flags=0):

        """
        Take a shared lock on an object

        :param key: name of the object
        :type key: str
        :param name: name of the lock
        :type name: str
        :param cookie: cookie of the lock
        :type cookie: str
        :param tag: tag of the lock
        :type tag: str
        :param desc: description of the lock
        :type desc: str
        :param duration: duration of the lock in seconds
        :type duration: int
        :param flags: flags
        :type flags: int

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        """
        self.require_ioctx_open()

        key = cstr(key, 'key')
        tag = cstr(tag, 'tag')
        name = cstr(name, 'name')
        cookie = cstr(cookie, 'cookie')
        desc = cstr(desc, 'desc')

        cdef:
            char* _key = key
            char* _tag = tag
            char* _name = name
            char* _cookie = cookie
            char* _desc = desc
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

    @requires(('key', str_type), ('name', str_type), ('cookie', str_type))
    def unlock(self, key, name, cookie):

        """
        Release a shared or exclusive lock on an object

        :param key: name of the object
        :type key: str
        :param name: name of the lock
        :type name: str
        :param cookie: cookie of the lock
        :type cookie: str

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        """
        self.require_ioctx_open()

        key = cstr(key, 'key')
        name = cstr(name, 'name')
        cookie = cstr(cookie, 'cookie')

        cdef:
            char* _key = key
            char* _name = name
            char* _cookie = cookie

        with nogil:
            ret = rados_unlock(self.io, _key, _name, _cookie)
        if ret < 0:
            raise make_ex(ret, "Ioctx.rados_lock_exclusive(%s): failed to set lock %s on %s" % (self.name, name, key))

    def set_osdmap_full_try(self):
        """
        Set global osdmap_full_try label to true
        """
        with nogil:
            rados_set_osdmap_full_try(self.io)

    def unset_osdmap_full_try(self):
        """
        Unset
        """
        with nogil:
            rados_unset_osdmap_full_try(self.io)

    def application_enable(self, app_name, force=False):
        """
        Enable an application on an OSD pool

        :param app_name: application name
        :type app_name: str
        :param force: False if only a single app should exist per pool
        :type expire_seconds: boool

        :raises: :class:`Error`
        """
        app_name =  cstr(app_name, 'app_name')
        cdef:
            char *_app_name = app_name
            int _force = (1 if force else 0)

        with nogil:
            ret = rados_application_enable(self.io, _app_name, _force)
        if ret < 0:
            raise make_ex(ret, "error enabling application")

    def application_list(self):
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

    def application_metadata_set(self, app_name, key, value):
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
        app_name =  cstr(app_name, 'app_name')
        key =  cstr(key, 'key')
        value =  cstr(value, 'value')
        cdef:
            char *_app_name = app_name
            char *_key = key
            char *_value = value

        with nogil:
            ret = rados_application_metadata_set(self.io, _app_name, _key,
                                                 _value)
        if ret < 0:
            raise make_ex(ret, "error setting application metadata")

    def application_metadata_remove(self, app_name, key):
        """
        Remove application metadata from an OSD pool

        :param app_name: application name
        :type app_name: str
        :param key: metadata key
        :type key: str

        :raises: :class:`Error`
        """
        app_name =  cstr(app_name, 'app_name')
        key =  cstr(key, 'key')
        cdef:
            char *_app_name = app_name
            char *_key = key

        with nogil:
            ret = rados_application_metadata_remove(self.io, _app_name, _key)
        if ret < 0:
            raise make_ex(ret, "error removing application metadata")

    def application_metadata_list(self, app_name):
        """
        Returns a list of enabled applications

        :param app_name: application name
        :type app_name: str
        :returns: list of key/value tuples
        """
        app_name =  cstr(app_name, 'app_name')
        cdef:
            char *_app_name = app_name
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
                    return zip(keys, vals)[:-1]
                elif ret == -errno.ERANGE:
                    pass
                else:
                    raise make_ex(ret, "error listing application metadata")
        finally:
            free(c_keys)
            free(c_vals)

    def alignment(self):
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
    def stat(self):
        self.require_object_exists()
        return self.ioctx.stat(self.key)

    def seek(self, position):
        self.require_object_exists()
        self.offset = position

    @set_object_locator
    @set_object_namespace
    def get_xattr(self, xattr_name):
        self.require_object_exists()
        return self.ioctx.get_xattr(self.key, xattr_name)

    @set_object_locator
    @set_object_namespace
    def get_xattrs(self):
        self.require_object_exists()
        return self.ioctx.get_xattrs(self.key)

    @set_object_locator
    @set_object_namespace
    def set_xattr(self, xattr_name, xattr_value):
        self.require_object_exists()
        return self.ioctx.set_xattr(self.key, xattr_name, xattr_value)

    @set_object_locator
    @set_object_namespace
    def rm_xattr(self, xattr_name):
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

