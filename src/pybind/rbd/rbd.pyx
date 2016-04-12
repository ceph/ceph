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

from cpython cimport PyObject, ref, exc
from libc cimport errno
from libc.stdint cimport *
from libc.stdlib cimport realloc, free

from collections import Iterable

cimport rados


cdef extern from "Python.h":
    # These are in cpython/string.pxd, but use "object" types instead of
    # PyObject*, which invokes assumptions in cpython that we need to
    # legitimately break to implement zero-copy string buffers in Image.read().
    # This is valid use of the Python API and documented as a special case.
    PyObject *PyBytes_FromStringAndSize(char *v, Py_ssize_t len) except NULL
    char* PyBytes_AsString(PyObject *string) except NULL
    int _PyBytes_Resize(PyObject **string, Py_ssize_t newsize) except -1

ctypedef int (*librbd_progress_fn_t)(uint64_t offset, uint64_t total, void* ptr)

cdef extern from "rbd/librbd.h" nogil:
    enum:
        _RBD_FEATURE_LAYERING "RBD_FEATURE_LAYERING"
        _RBD_FEATURE_STRIPINGV2 "RBD_FEATURE_STRIPINGV2"
        _RBD_FEATURE_EXCLUSIVE_LOCK "RBD_FEATURE_EXCLUSIVE_LOCK"
        _RBD_FEATURE_OBJECT_MAP "RBD_FEATURE_OBJECT_MAP"
        _RBD_FEATURE_FAST_DIFF "RBD_FEATURE_FAST_DIFF"
        _RBD_FEATURE_DEEP_FLATTEN "RBD_FEATURE_DEEP_FLATTEN"
        _RBD_FEATURE_JOURNALING "RBD_FEATURE_JOURNALING"

        _RBD_FEATURES_INCOMPATIBLE "RBD_FEATURES_INCOMPATIBLE"
        _RBD_FEATURES_RW_INCOMPATIBLE "RBD_FEATURES_RW_INCOMPATIBLE"
        _RBD_FEATURES_MUTABLE "RBD_FEATURES_MUTABLE"
        _RBD_FEATURES_SINGLE_CLIENT "RBD_FEATURES_SINGLE_CLIENT"
        _RBD_FEATURES_ALL "RBD_FEATURES_ALL"

        _RBD_FLAG_OBJECT_MAP_INVALID "RBD_FLAG_OBJECT_MAP_INVALID"
        _RBD_FLAG_FAST_DIFF_INVALID "RBD_FLAG_FAST_DIFF_INVALID"

        _RBD_IMAGE_OPTION_FORMAT "RBD_IMAGE_OPTION_FORMAT"
        _RBD_IMAGE_OPTION_FEATURES "RBD_IMAGE_OPTION_FEATURES"
        _RBD_IMAGE_OPTION_ORDER "RBD_IMAGE_OPTION_ORDER"
        _RBD_IMAGE_OPTION_STRIPE_UNIT "RBD_IMAGE_OPTION_STRIPE_UNIT"
        _RBD_IMAGE_OPTION_STRIPE_COUNT "RBD_IMAGE_OPTION_STRIPE_COUNT"

        RBD_MAX_BLOCK_NAME_SIZE
        RBD_MAX_IMAGE_NAME_SIZE

    ctypedef void* rados_ioctx_t
    ctypedef void* rbd_image_t
    ctypedef void* rbd_image_options_t

    ctypedef struct rbd_image_info_t:
        uint64_t size
        uint64_t obj_size
        uint64_t num_objs
        int order
        char block_name_prefix[RBD_MAX_BLOCK_NAME_SIZE]
        uint64_t parent_pool
        char parent_name[RBD_MAX_IMAGE_NAME_SIZE]

    ctypedef struct rbd_snap_info_t:
        uint64_t id
        uint64_t size
        char *name

    void rbd_version(int *major, int *minor, int *extra)

    void rbd_image_options_create(rbd_image_options_t* opts)
    void rbd_image_options_destroy(rbd_image_options_t opts)
    int rbd_image_options_set_string(rbd_image_options_t opts, int optname,
                                     const char* optval)
    int rbd_image_options_set_uint64(rbd_image_options_t opts, int optname,
                                     uint64_t optval)
    int rbd_image_options_get_string(rbd_image_options_t opts, int optname,
                                     char* optval, size_t maxlen)
    int rbd_image_options_get_uint64(rbd_image_options_t opts, int optname,
                                     uint64_t* optval)
    int rbd_image_options_unset(rbd_image_options_t opts, int optname)
    void rbd_image_options_clear(rbd_image_options_t opts)
    int rbd_image_options_is_empty(rbd_image_options_t opts)

    int rbd_list(rados_ioctx_t io, char *names, size_t *size)
    int rbd_create(rados_ioctx_t io, const char *name, uint64_t size,
                   int *order)
    int rbd_create4(rados_ioctx_t io, const char *name, uint64_t size,
                    rbd_image_options_t opts)
    int rbd_clone3(rados_ioctx_t p_ioctx, const char *p_name,
                   const char *p_snapname, rados_ioctx_t c_ioctx,
                   const char *c_name, rbd_image_options_t c_opts)
    int rbd_remove(rados_ioctx_t io, const char *name)
    int rbd_rename(rados_ioctx_t src_io_ctx, const char *srcname,
                   const char *destname)
    int rbd_open(rados_ioctx_t io, const char *name,
                 rbd_image_t *image, const char *snap_name)
    int rbd_open_read_only(rados_ioctx_t io, const char *name,
                           rbd_image_t *image, const char *snap_name)
    int rbd_close(rbd_image_t image)
    int rbd_resize(rbd_image_t image, uint64_t size)
    int rbd_stat(rbd_image_t image, rbd_image_info_t *info, size_t infosize)
    int rbd_get_old_format(rbd_image_t image, uint8_t *old)
    int rbd_get_size(rbd_image_t image, uint64_t *size)
    int rbd_get_features(rbd_image_t image, uint64_t *features)
    int rbd_update_features(rbd_image_t image, uint64_t features,
                            uint8_t enabled)
    int rbd_get_stripe_unit(rbd_image_t image, uint64_t *stripe_unit)
    int rbd_get_stripe_count(rbd_image_t image, uint64_t *stripe_count)
    int rbd_get_overlap(rbd_image_t image, uint64_t *overlap)
    int rbd_get_parent_info(rbd_image_t image,
                            char *parent_poolname, size_t ppoolnamelen,
                            char *parent_name, size_t pnamelen,
                            char *parent_snapname, size_t psnapnamelen)
    int rbd_get_flags(rbd_image_t image, uint64_t *flags)
    int rbd_is_exclusive_lock_owner(rbd_image_t image, int *is_owner)
    ssize_t rbd_read2(rbd_image_t image, uint64_t ofs, size_t len,
                      char *buf, int op_flags)
    ssize_t rbd_write2(rbd_image_t image, uint64_t ofs, size_t len,
                       const char *buf, int op_flags)
    int rbd_discard(rbd_image_t image, uint64_t ofs, uint64_t len)
    int rbd_copy3(rbd_image_t src, rados_ioctx_t dest_io_ctx,
                  const char *destname, rbd_image_options_t dest_opts)
    int rbd_snap_list(rbd_image_t image, rbd_snap_info_t *snaps,
                      int *max_snaps)
    void rbd_snap_list_end(rbd_snap_info_t *snaps)
    int rbd_snap_create(rbd_image_t image, const char *snapname)
    int rbd_snap_remove(rbd_image_t image, const char *snapname)
    int rbd_snap_rollback(rbd_image_t image, const char *snapname)
    int rbd_snap_rename(rbd_image_t image, const char *snapname,
                        const char* dstsnapsname)
    int rbd_snap_protect(rbd_image_t image, const char *snap_name)
    int rbd_snap_unprotect(rbd_image_t image, const char *snap_name)
    int rbd_snap_is_protected(rbd_image_t image, const char *snap_name,
                              int *is_protected)
    int rbd_snap_set(rbd_image_t image, const char *snapname)
    int rbd_flatten(rbd_image_t image)
    int rbd_rebuild_object_map(rbd_image_t image, librbd_progress_fn_t cb,
                               void *cbdata)
    ssize_t rbd_list_children(rbd_image_t image, char *pools, size_t *pools_len,
                              char *images, size_t *images_len)
    ssize_t rbd_list_lockers(rbd_image_t image, int *exclusive,
                             char *tag, size_t *tag_len,
                             char *clients, size_t *clients_len,
                             char *cookies, size_t *cookies_len,
                             char *addrs, size_t *addrs_len)
    int rbd_lock_exclusive(rbd_image_t image, const char *cookie)
    int rbd_lock_shared(rbd_image_t image, const char *cookie,
                        const char *tag)
    int rbd_unlock(rbd_image_t image, const char *cookie)
    int rbd_break_lock(rbd_image_t image, const char *client,
                       const char *cookie)

    # We use -9000 to propagate Python exceptions. We use except? to make sure
    # things still work as intended if -9000 happens to be a valid errno value
    # somewhere.
    int rbd_diff_iterate2(rbd_image_t image, const char *fromsnapname,
                         uint64_t ofs, uint64_t len,
                         uint8_t include_parent, uint8_t whole_object,
                         int (*cb)(uint64_t, size_t, int, void *)
                             nogil except? -9000,
                         void *arg) except? -9000

    int rbd_flush(rbd_image_t image)
    int rbd_invalidate_cache(rbd_image_t image)


RBD_FEATURE_LAYERING = _RBD_FEATURE_LAYERING
RBD_FEATURE_STRIPINGV2 = _RBD_FEATURE_STRIPINGV2
RBD_FEATURE_EXCLUSIVE_LOCK = _RBD_FEATURE_EXCLUSIVE_LOCK
RBD_FEATURE_OBJECT_MAP = _RBD_FEATURE_OBJECT_MAP
RBD_FEATURE_FAST_DIFF = _RBD_FEATURE_FAST_DIFF
RBD_FEATURE_DEEP_FLATTEN = _RBD_FEATURE_DEEP_FLATTEN
RBD_FEATURE_JOURNALING = _RBD_FEATURE_JOURNALING

RBD_FEATURES_INCOMPATIBLE = _RBD_FEATURES_INCOMPATIBLE
RBD_FEATURES_RW_INCOMPATIBLE = _RBD_FEATURES_RW_INCOMPATIBLE
RBD_FEATURES_MUTABLE = _RBD_FEATURES_MUTABLE
RBD_FEATURES_SINGLE_CLIENT = _RBD_FEATURES_SINGLE_CLIENT
RBD_FEATURES_ALL = _RBD_FEATURES_ALL

RBD_FLAG_OBJECT_MAP_INVALID = _RBD_FLAG_OBJECT_MAP_INVALID

RBD_IMAGE_OPTION_FORMAT = _RBD_IMAGE_OPTION_FORMAT
RBD_IMAGE_OPTION_FEATURES = _RBD_IMAGE_OPTION_FEATURES
RBD_IMAGE_OPTION_ORDER = _RBD_IMAGE_OPTION_ORDER
RBD_IMAGE_OPTION_STRIPE_UNIT = _RBD_IMAGE_OPTION_STRIPE_UNIT
RBD_IMAGE_OPTION_STRIPE_COUNT = _RBD_IMAGE_OPTION_STRIPE_COUNT


class Error(Exception):
    pass


class PermissionError(Error):
    pass


class ImageNotFound(Error):
    pass


class ImageExists(Error):
    pass


class IOError(Error):
    pass


class NoSpace(Error):
    pass


class IncompleteWriteError(Error):
    pass


class InvalidArgument(Error):
    pass


class LogicError(Error):
    pass


class ReadOnlyImage(Error):
    pass


class ImageBusy(Error):
    pass


class ImageHasSnapshots(Error):
    pass


class FunctionNotSupported(Error):
    pass


class ArgumentOutOfRange(Error):
    pass


class ConnectionShutdown(Error):
    pass


class Timeout(Error):
    pass


cdef errno_to_exception = {
    errno.EPERM     : PermissionError,
    errno.ENOENT    : ImageNotFound,
    errno.EIO       : IOError,
    errno.ENOSPC    : NoSpace,
    errno.EEXIST    : ImageExists,
    errno.EINVAL    : InvalidArgument,
    errno.EROFS     : ReadOnlyImage,
    errno.EBUSY     : ImageBusy,
    errno.ENOTEMPTY : ImageHasSnapshots,
    errno.ENOSYS    : FunctionNotSupported,
    errno.EDOM      : ArgumentOutOfRange,
    errno.ESHUTDOWN : ConnectionShutdown,
    errno.ETIMEDOUT : Timeout,
}

cdef make_ex(ret, msg):
    """
    Translate a librbd return code into an exception.

    :param ret: the return code
    :type ret: int
    :param msg: the error message to use
    :type msg: str
    :returns: a subclass of :class:`Error`
    """
    ret = abs(ret)
    if ret in errno_to_exception:
        return errno_to_exception[ret](msg)
    else:
        return Error(msg + (": error code %d" % ret))


cdef rados_ioctx_t convert_ioctx(rados.Ioctx ioctx) except? NULL:
    return <rados_ioctx_t>ioctx.io

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
    elif isinstance(val, unicode):
        return val.encode(encoding)
    else:
        raise InvalidArgument('%s must be a string' % name)

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

    def create(self, ioctx, name, size, order=None, old_format=True,
               features=0, stripe_unit=0, stripe_count=0):
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
        :param stripe_unit: stripe unit in bytes (default 0 for object size)
        :type stripe_unit: int
        :param stripe_count: objects to stripe over before looping
        :type stripe_count: int
        :raises: :class:`ImageExists`
        :raises: :class:`TypeError`
        :raises: :class:`InvalidArgument`
        :raises: :class:`FunctionNotSupported`
        """
        name = cstr(name, 'name')
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            char *_name = name
            uint64_t _size = size
            int _order = 0
            rbd_image_options_t opts
        if order is not None:
            _order = order
        if old_format:
            if features != 0 or stripe_unit != 0 or stripe_count != 0:
                raise InvalidArgument('format 1 images do not support feature'
                                      ' masks or non-default striping')
            with nogil:
                ret = rbd_create(_ioctx, _name, _size, &_order)
        else:
            rbd_image_options_create(&opts)
            try:
                rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_FORMAT,
                                             1 if old_format else 2)
                rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_FEATURES,
                                             features)
                rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_ORDER,
                                             _order)
                rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_STRIPE_UNIT,
                                             stripe_unit)
                rbd_image_options_set_uint64(opts,
                                             RBD_IMAGE_OPTION_STRIPE_COUNT,
                                             stripe_count)
                with nogil:
                    ret = rbd_create4(_ioctx, _name, _size, opts)
            finally:
                rbd_image_options_destroy(opts)
        if ret < 0:
            raise make_ex(ret, 'error creating image')

    def clone(self, p_ioctx, p_name, p_snapname, c_ioctx, c_name,
              features=0, order=None, stripe_unit=0, stripe_count=0):
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
        :param stripe_unit: stripe unit in bytes (default 0 for object size)
        :type stripe_unit: int
        :param stripe_count: objects to stripe over before looping
        :type stripe_count: int
        :raises: :class:`TypeError`
        :raises: :class:`InvalidArgument`
        :raises: :class:`ImageExists`
        :raises: :class:`FunctionNotSupported`
        :raises: :class:`ArgumentOutOfRange`
        """
        p_snapname = cstr(p_snapname, 'p_snapname')
        p_name = cstr(p_name, 'p_name')
        c_name = cstr(c_name, 'c_name')
        cdef:
            rados_ioctx_t _p_ioctx = convert_ioctx(p_ioctx)
            rados_ioctx_t _c_ioctx = convert_ioctx(c_ioctx)
            char *_p_name = p_name
            char *_p_snapname = p_snapname
            char *_c_name = c_name
            rbd_image_options_t opts
        if order is None:
            order = 0

        rbd_image_options_create(&opts)
        try:
            rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_FEATURES,
                                         features)
            rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_ORDER,
                                         order)
            rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_STRIPE_UNIT,
                                         stripe_unit)
            rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_STRIPE_COUNT,
                                         stripe_count)
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
            return [decode_cstr(name) for name in c_names[:ret].split('\0')
                    if name]
        finally:
            free(c_names)

    def remove(self, ioctx, name):
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
        :raises: :class:`ImageNotFound`, :class:`ImageBusy`,
                 :class:`ImageHasSnapshots`
        """
        name = cstr(name, 'name')
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            char *_name = name
        with nogil:
            ret = rbd_remove(_ioctx, _name)
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

    def __init__(self, ioctx, name, snapshot=None, read_only=False):
        """
        Open the image at the given snapshot.
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
        :type snaphshot: str
        :param read_only: whether to open the image in read-only mode
        :type read_only: bool
        """
        name = cstr(name, 'name')
        snapshot = cstr(snapshot, 'snapshot', opt=True)
        self.closed = True
        self.name = name
        # Keep around a reference to the ioctx, so it won't get deleted
        self.ioctx = ioctx
        cdef:
            rados_ioctx_t _ioctx = convert_ioctx(ioctx)
            char *_name = name
            char *_snapshot = opt_str(snapshot)
        if read_only:
            with nogil:
                ret = rbd_open_read_only(_ioctx, _name, &self.image, _snapshot)
        else:
            with nogil:
                ret = rbd_open(_ioctx, _name, &self.image, _snapshot)
        if ret != 0:
            raise make_ex(ret, 'error opening image %s at snapshot %s' % (name, snapshot))
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        """
        Closes the image. See :func:`close`
        """
        self.close()
        return False

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

    def __dealloc__(self):
        self.close()

    def __repr__(self):
        return "rbd.Image(ioctx, %r)" % self.name

    def resize(self, size):
        """
        Change the size of the image.

        :param size: the new size of the image
        :type size: int
        """
        cdef uint64_t _size = size
        with nogil:
            ret = rbd_resize(self.image, _size)
        if ret < 0:
            raise make_ex(ret, 'error resizing image %s' % (self.name,))

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
            raise make_ex(ret, 'error getting info for image %s' % (self.name,))
        return {
            'size'              : info.size,
            'obj_size'          : info.obj_size,
            'num_objs'          : info.num_objs,
            'order'             : info.order,
            'block_name_prefix' : decode_cstr(info.block_name_prefix),
            'parent_pool'       : info.parent_pool,
            'parent_name'       : info.parent_name
            }

    def parent_info(self):
        """
        Get information about a cloned image's parent (if any)

        :returns: tuple - ``(pool name, image name, snapshot name)`` components
                  of the parent image
        :raises: :class:`ImageNotFound` if the image doesn't have a parent
        """
        cdef:
            int ret = -errno.ERANGE
            size_t size = 8
            char *pool = NULL
            char *name = NULL
            char *snapname = NULL
        try:
            while ret == -errno.ERANGE and size <= 4096:
                pool = <char *>realloc_chk(pool, size)
                name = <char *>realloc_chk(name, size)
                snapname = <char *>realloc_chk(snapname, size)
                with nogil:
                    ret = rbd_get_parent_info(self.image, pool, size, name,
                                              size, snapname, size)
                if ret == -errno.ERANGE:
                    size *= 2

            if ret != 0:
                raise make_ex(ret, 'error getting parent info for image %s' % (self.name,))
            return (decode_cstr(pool), decode_cstr(name), decode_cstr(snapname))
        finally:
            free(pool)
            free(name)
            free(snapname)

    def old_format(self):
        """
        Find out whether the image uses the old RBD format.

        :returns: bool - whether the image uses the old RBD format
        """
        cdef uint8_t old
        with nogil:
            ret = rbd_get_old_format(self.image, &old)
        if ret != 0:
            raise make_ex(ret, 'error getting old_format for image' % (self.name))
        return old != 0

    def size(self):
        """
        Get the size of the image. If open to a snapshot, returns the
        size of that snapshot.

        :returns: the size of the image in bytes
        """
        cdef uint64_t image_size
        with nogil:
            ret = rbd_get_size(self.image, &image_size)
        if ret != 0:
            raise make_ex(ret, 'error getting size for image' % (self.name))
        return image_size

    def features(self):
        """
        Gets the features bitmask of the image.

        :returns: int - the features bitmask of the image
        """
        cdef uint64_t features
        with nogil:
            ret = rbd_get_features(self.image, &features)
        if ret != 0:
            raise make_ex(ret, 'error getting features for image' % (self.name))
        return features

    def update_features(self, features, enabled):
        """
        Updates the features bitmask of the image by enabling/disabling
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

    def overlap(self):
        """
        Gets the number of overlapping bytes between the image and its parent
        image. If open to a snapshot, returns the overlap between the snapshot
        and the parent image.

        :returns: int - the overlap in bytes
        :raises: :class:`ImageNotFound` if the image doesn't have a parent
        """
        cdef uint64_t overlap
        with nogil:
            ret = rbd_get_overlap(self.image, &overlap)
        if ret != 0:
            raise make_ex(ret, 'error getting overlap for image' % (self.name))
        return overlap

    def flags(self):
        """
        Gets the flags bitmask of the image.

        :returns: int - the flags bitmask of the image
        """
        cdef uint64_t flags
        with nogil:
            ret = rbd_get_flags(self.image, &flags)
        if ret != 0:
            raise make_ex(ret, 'error getting flags for image' % (self.name))
        return flags

    def is_exclusive_lock_owner(self):
        """
        Gets the status of the image exclusive lock.

        :returns: bool - true if the image is exclusively locked
        """
        cdef int owner
        with nogil:
            ret = rbd_is_exclusive_lock_owner(self.image, &owner)
        if ret != 0:
            raise make_ex(ret, 'error getting lock status for image' % (self.name))
        return owner == 1

    def copy(self, dest_ioctx, dest_name, features=0, order=None, stripe_unit=0,
             stripe_count=0):
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
        :param stripe_unit: stripe unit in bytes (default 0 for object size)
        :type stripe_unit: int
        :param stripe_count: objects to stripe over before looping
        :type stripe_count: int
        :raises: :class:`TypeError`
        :raises: :class:`InvalidArgument`
        :raises: :class:`ImageExists`
        :raises: :class:`FunctionNotSupported`
        :raises: :class:`ArgumentOutOfRange`
        """
        if order is None:
            order = 0
        dest_name = cstr(dest_name, 'dest_name')
        cdef:
            rados_ioctx_t _dest_ioctx = convert_ioctx(dest_ioctx)
            char *_dest_name = dest_name
            rbd_image_options_t opts

        rbd_image_options_create(&opts)
        try:
            rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_FEATURES,
                                         features)
            rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_ORDER,
                                         order)
            rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_STRIPE_UNIT,
                                         stripe_unit)
            rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_STRIPE_COUNT,
                                         stripe_count)
            with nogil:
                ret = rbd_copy3(self.image, _dest_ioctx, _dest_name, opts)
        finally:
            rbd_image_options_destroy(opts)
        if ret < 0:
            raise make_ex(ret, 'error copying image %s to %s' % (self.name, dest_name))

    def list_snaps(self):
        """
        Iterate over the snapshots of an image.

        :returns: :class:`SnapIterator`
        """
        return SnapIterator(self)

    def create_snap(self, name):
        """
        Create a snapshot of the image.

        :param name: the name of the snapshot
        :type name: str
        :raises: :class:`ImageExists`
        """
        name = cstr(name, 'name')
        cdef char *_name = name
        with nogil:
            ret = rbd_snap_create(self.image, _name)
        if ret != 0:
            raise make_ex(ret, 'error creating snapshot %s from %s' % (name, self.name))

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

    def remove_snap(self, name):
        """
        Delete a snapshot of the image.

        :param name: the name of the snapshot
        :type name: str
        :raises: :class:`IOError`, :class:`ImageBusy`
        """
        name = cstr(name, 'name')
        cdef char *_name = name
        with nogil:
            ret = rbd_snap_remove(self.image, _name)
        if ret != 0:
            raise make_ex(ret, 'error removing snapshot %s from %s' % (name, self.name))

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

            if ret != length:
                _PyBytes_Resize(&ret_s, ret)

            return <object>ret_s
        finally:
            # We DECREF unconditionally: the cast to object above will have
            # INCREFed if necessary. This also takes care of exceptions,
            # including if _PyString_Resize fails (that will free the string
            # itself and set ret_s to NULL, hence XDECREF).
            ref.Py_XDECREF(ret_s)

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

        if ret == length:
            return ret
        elif ret < 0:
            raise make_ex(ret, "error writing to %s" % (self.name,))
        elif ret < length:
            raise IncompleteWriteError("Wrote only %ld out of %ld bytes" % (ret, length))
        else:
            raise LogicError("logic error: rbd_write(%s) \
returned %d, but %d was the maximum number of bytes it could have \
written." % (self.name, ret, length))

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

    def flush(self):
        """
        Block until all writes are fully flushed if caching is enabled.
        """
        with nogil:
            ret = rbd_flush(self.image)
        if ret < 0:
            raise make_ex(ret, 'error flushing image')

    def invalidate_cache(self):
        """
        Drop any cached data for the image.
        """
        with nogil:
            ret = rbd_invalidate_cache(self.image)
        if ret < 0:
            raise make_ex(ret, 'error invalidating cache')

    def stripe_unit(self):
        """
        Returns the stripe unit used for the image.
        """
        cdef uint64_t stripe_unit
        with nogil:
            ret = rbd_get_stripe_unit(self.image, &stripe_unit)
        if ret != 0:
            raise make_ex(ret, 'error getting stripe unit for image' % (self.name))
        return stripe_unit

    def stripe_count(self):
        """
        Returns the stripe count used for the image.
        """
        cdef uint64_t stripe_count
        with nogil:
            ret = rbd_get_stripe_count(self.image, &stripe_count)
        if ret != 0:
            raise make_ex(ret, 'error getting stripe count for image' % (self.name))
        return stripe_count

    def flatten(self):
        """
        Flatten clone image (copy all blocks from parent to child)
        """
        with nogil:
            ret = rbd_flatten(self.image)
        if ret < 0:
            raise make_ex(ret, "error flattening %s" % self.name)

    def rebuild_object_map(self):
        """
        Rebuilds the object map for the image HEAD or currently set snapshot
        """
        cdef librbd_progress_fn_t prog_cb = &no_op_progress_callback
        with nogil:
            ret = rbd_rebuild_object_map(self.image, prog_cb, NULL)
        if ret < 0:
            raise make_ex(ret, "error rebuilding object map %s" % self.name)

    def list_children(self):
        """
        List children of the currently set snapshot (set via set_snap()).

        :returns: list - a list of (pool name, image name) tuples
        """
        cdef:
            size_t pools_size = 512, images_size = 512
            char *c_pools = NULL
            char *c_images = NULL
        try:
            while True:
                c_pools = <char *>realloc_chk(c_pools, pools_size)
                c_images = <char *>realloc_chk(c_images, images_size)
                with nogil:
                    ret = rbd_list_children(self.image, c_pools, &pools_size,
                                            c_images, &images_size)
                if ret >= 0:
                    break
                elif ret != -errno.ERANGE:
                    raise make_ex(ret, 'error listing images')
            if ret == 0:
                return []
            pools = map(decode_cstr, c_pools[:pools_size - 1].split('\0'))
            images = map(decode_cstr, c_images[:images_size - 1].split('\0'))
            return list(zip(pools, images))
        finally:
            free(c_pools)
            free(c_images)

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
            clients = map(decode_cstr, c_clients[:clients_size - 1].split('\0'))
            cookies = map(decode_cstr, c_cookies[:cookies_size - 1].split('\0'))
            addrs = map(decode_cstr, c_addrs[:addrs_size - 1].split('\0'))
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


cdef class SnapIterator(object):
    """
    Iterator over snapshot info for an image.

    Yields a dictionary containing information about a snapshot.

    Keys are:

    * ``id`` (int) - numeric identifier of the snapshot

    * ``size`` (int) - size of the image at the time of snapshot (in bytes)

    * ``name`` (str) - name of the snapshot
    """

    cdef rbd_snap_info_t *snaps
    cdef int num_snaps

    def __init__(self, Image image):
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
                raise make_ex(ret, 'error listing snapshots for image %s' % (image.name,))

    def __iter__(self):
        for i in range(self.num_snaps):
            yield {
                'id'   : self.snaps[i].id,
                'size' : self.snaps[i].size,
                'name' : decode_cstr(self.snaps[i].name),
                }

    def __dealloc__(self):
        if self.snaps:
            rbd_snap_list_end(self.snaps)
            free(self.snaps)
