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
from collections import Iterable
from ctypes import CDLL, c_char, c_char_p, c_size_t, c_void_p, c_int, \
    create_string_buffer, byref, Structure, c_uint64, c_int64, c_uint8, \
    CFUNCTYPE
from ctypes.util import find_library
import ctypes
import errno
import sys

from rados import cstr, decode_cstr

ANONYMOUS_AUID = 0xffffffffffffffff
ADMIN_AUID = 0

RBD_FEATURE_LAYERING = 1
RBD_FEATURE_STRIPINGV2 = 2
RBD_FEATURE_EXCLUSIVE_LOCK = 4
RBD_FEATURE_OBJECT_MAP = 8
RBD_FEATURE_FAST_DIFF = 16
RBD_FEATURE_DEEP_FLATTEN = 32
RBD_FEATURE_JOURNALING = 64

RBD_FEATURES_ALL = (RBD_FEATURE_LAYERING       |
                    RBD_FEATURE_STRIPINGV2     |
                    RBD_FEATURE_EXCLUSIVE_LOCK |
                    RBD_FEATURE_OBJECT_MAP     |
                    RBD_FEATURE_FAST_DIFF      |
                    RBD_FEATURE_DEEP_FLATTEN   |
                    RBD_FEATURE_JOURNALING)

# features that make an image inaccessible for read or write by
# clients that don't understand them
RBD_FEATURES_INCOMPATIBLE = (RBD_FEATURE_LAYERING |
                             RBD_FEATURE_STRIPINGV2)

# features that make an image unwritable by clients that don't
# understand them
RBD_FEATURES_RW_INCOMPATIBLE = (RBD_FEATURES_INCOMPATIBLE  |
                                RBD_FEATURE_EXCLUSIVE_LOCK |
                                RBD_FEATURE_OBJECT_MAP     |
                                RBD_FEATURE_FAST_DIFF      |
                                RBD_FEATURE_DEEP_FLATTEN   |
                                RBD_FEATURE_JOURNALING)

# features that may be dynamically enabled or disabled
RBD_FEATURES_MUTABLE = (RBD_FEATURE_EXCLUSIVE_LOCK |
                        RBD_FEATURE_OBJECT_MAP     |
                        RBD_FEATURE_FAST_DIFF      |
                        RBD_FEATURE_JOURNALING)

# features that only work when used with a single client
# using the image for writes
RBD_FEATURES_SINGLE_CLIENT = (RBD_FEATURE_EXCLUSIVE_LOCK |
                              RBD_FEATURE_OBJECT_MAP     |
                              RBD_FEATURE_FAST_DIFF      |
                              RBD_FEATURE_JOURNALING)

RBD_FLAG_OBJECT_MAP_INVALID = 1

RBD_IMAGE_OPTION_FORMAT = 0
RBD_IMAGE_OPTION_FEATURES = 1
RBD_IMAGE_OPTION_ORDER = 2
RBD_IMAGE_OPTION_STRIPE_UNIT = 3
RBD_IMAGE_OPTION_STRIPE_COUNT = 4


# Are we running Python 2.x
_python2 = sys.hexversion < 0x03000000


if _python2:
    str_type = basestring
else:
    str_type = str


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


def make_ex(ret, msg):
    """
    Translate a librbd return code into an exception.

    :param ret: the return code
    :type ret: int
    :param msg: the error message to use
    :type msg: str
    :returns: a subclass of :class:`Error`
    """
    errors = {
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
    ret = abs(ret)
    if ret in errors:
        return errors[ret](msg)
    else:
        return Error(msg + (": error code %d" % ret))


class rbd_image_info_t(Structure):
    _fields_ = [("size", c_uint64),
                ("obj_size", c_uint64),
                ("num_objs", c_uint64),
                ("order", c_int),
                ("block_name_prefix", c_char * 24),
                ("parent_pool", c_int64),
                ("parent_name", c_char * 96)]


class rbd_snap_info_t(Structure):
    _fields_ = [("id", c_uint64),
                ("size", c_uint64),
                ("name", c_char_p)]


def load_librbd():
    """
    Load the librbd shared library.
    """
    librbd_path = find_library('rbd')
    if librbd_path:
        return CDLL(librbd_path)

    # try harder, find_library() doesn't search LD_LIBRARY_PATH
    # in addition, it doesn't seem work on centos 6.4 (see e46d2ca067b5)
    try:
        return CDLL('librbd.so.1')
    except OSError as e:
        raise EnvironmentError("Unable to load librbd: %s" % e)


class RBD(object):
    """
    This class wraps librbd CRUD functions.
    """
    def __init__(self):
        self.librbd = load_librbd()

    def version(self):
        """
        Get the version number of the ``librbd`` C library.

        :returns: a tuple of ``(major, minor, extra)`` components of the
                  librbd version
        """
        major = c_int(0)
        minor = c_int(0)
        extra = c_int(0)
        self.librbd.rbd_version(byref(major), byref(minor), byref(extra))
        return (major.value, minor.value, extra.value)

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
        if order is None:
            order = 0
        if not isinstance(name, str_type):
            raise TypeError('name must be a string')
        if old_format:
            if features != 0 or stripe_unit != 0 or stripe_count != 0:
                raise InvalidArgument('format 1 images do not support feature'
                                      ' masks or non-default striping')
            ret = self.librbd.rbd_create(ioctx.io, cstr(name),
                                         c_uint64(size),
                                         byref(c_int(order)))
        else:
            if not hasattr(self.librbd, 'rbd_create2'):
                raise FunctionNotSupported('installed version of librbd does'
                                           ' not support format 2 images')
            has_create4 = hasattr(self.librbd, 'rbd_create4')
            has_create3 = hasattr(self.librbd, 'rbd_create3')
            if (stripe_unit != 0 or stripe_count != 0) and not has_create3:
                raise FunctionNotSupported('installed version of librbd does'
                                           ' not support stripe unit or count')
            if has_create4:
                format = old_format and 1 or 2
                opts = c_void_p()
                self.librbd.rbd_image_options_create(byref(opts))
                self.librbd.rbd_image_options_set_uint64(opts,
                                                         RBD_IMAGE_OPTION_FORMAT,
                                                         c_uint64(format))
                self.librbd.rbd_image_options_set_uint64(opts,
                                                         RBD_IMAGE_OPTION_FEATURES,
                                                         c_uint64(features))
                self.librbd.rbd_image_options_set_uint64(opts,
                                                         RBD_IMAGE_OPTION_ORDER,
                                                         c_uint64(order))
                self.librbd.rbd_image_options_set_uint64(opts,
                                                         RBD_IMAGE_OPTION_STRIPE_UNIT,
                                                         c_uint64(stripe_unit))
                self.librbd.rbd_image_options_set_uint64(opts,
                                                         RBD_IMAGE_OPTION_STRIPE_COUNT,
                                                         c_uint64(stripe_count))
                ret = self.librbd.rbd_create4(ioctx.io, cstr(name),
                                              c_uint64(size), opts)
                self.librbd.rbd_image_options_get_uint64(opts,
                                                         RBD_IMAGE_OPTION_ORDER,
                                                         byref(c_uint64(order)))
                self.librbd.rbd_image_options_destroy(opts)
            elif has_create3:
                ret = self.librbd.rbd_create3(ioctx.io, cstr(name),
                                              c_uint64(size),
                                              c_uint64(features),
                                              byref(c_int(order)),
                                              c_uint64(stripe_unit),
                                              c_uint64(stripe_count))
            else:
                ret = self.librbd.rbd_create2(ioctx.io, cstr(name),
                                              c_uint64(size),
                                              c_uint64(features),
                                              byref(c_int(order)))
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
        if order is None:
            order = 0
        if not isinstance(p_snapname, str_type) or not isinstance(p_name, str_type):
            raise TypeError('parent name and snapname must be strings')
        if not isinstance(c_name, str_type):
            raise TypeError('child name must be a string')

        has_clone3 = hasattr(self.librbd, 'rbd_clone3')
        if (stripe_unit != 0 or stripe_count != 0) and not has_clone3:
            raise FunctionNotSupported('installed version of librbd does'
                                       ' not support stripe unit or count')
        if has_clone3:
            opts = c_void_p()
            self.librbd.rbd_image_options_create(byref(opts))
            self.librbd.rbd_image_options_set_uint64(opts,
                                                     RBD_IMAGE_OPTION_FEATURES,
                                                     c_uint64(features))
            self.librbd.rbd_image_options_set_uint64(opts,
                                                     RBD_IMAGE_OPTION_ORDER,
                                                     c_uint64(order))
            self.librbd.rbd_image_options_set_uint64(opts,
                                                     RBD_IMAGE_OPTION_STRIPE_UNIT,
                                                     c_uint64(stripe_unit))
            self.librbd.rbd_image_options_set_uint64(opts,
                                                     RBD_IMAGE_OPTION_STRIPE_COUNT,
                                                     c_uint64(stripe_count))
            ret = self.librbd.rbd_clone3(p_ioctx.io, cstr(p_name),
                                         cstr(p_snapname),
                                         c_ioctx.io, cstr(c_name),
                                         opts)
            self.librbd.rbd_image_options_get_uint64(opts,
                                                     RBD_IMAGE_OPTION_ORDER,
                                                     byref(c_uint64(order)))
            self.librbd.rbd_image_options_destroy(opts)
        else:
            ret = self.librbd.rbd_clone(p_ioctx.io, cstr(p_name),
                                        cstr(p_snapname),
                                        c_ioctx.io, cstr(c_name),
                                        c_uint64(features),
                                        byref(c_int(order)))
        if ret < 0:
            raise make_ex(ret, 'error creating clone')

    def list(self, ioctx):
        """
        List image names.

        :param ioctx: determines which RADOS pool is read
        :type ioctx: :class:`rados.Ioctx`
        :returns: list -- a list of image names
        """
        size = c_size_t(512)
        while True:
            c_names = create_string_buffer(size.value)
            ret = self.librbd.rbd_list(ioctx.io, byref(c_names), byref(size))
            if ret >= 0:
                break
            elif ret != -errno.ERANGE:
                raise make_ex(ret, 'error listing images')

        return [decode_cstr(name) for name in c_names.raw.split(b'\0') if len(name) > 0]

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
        if not isinstance(name, str_type):
            raise TypeError('name must be a string')
        ret = self.librbd.rbd_remove(ioctx.io, cstr(name))
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
        if not isinstance(src, str_type) or not isinstance(dest, str_type):
            raise TypeError('src and dest must be strings')
        ret = self.librbd.rbd_rename(ioctx.io, cstr(src), cstr(dest))
        if ret != 0:
            raise make_ex(ret, 'error renaming image')


class Image(object):
    """
    This class represents an RBD image. It is used to perform I/O on
    the image and interact with snapshots.

    **Note**: Any method of this class may raise :class:`ImageNotFound`
    if the image has been deleted.
    """

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
        self.closed = True
        self.librbd = load_librbd()
        self.image = c_void_p()
        self.name = name
        if not isinstance(name, str_type):
            raise TypeError('name must be a string')
        if snapshot is not None and not isinstance(snapshot, str_type):
            raise TypeError('snapshot must be a string or None')
        if read_only:
            if not hasattr(self.librbd, 'rbd_open_read_only'):
                raise FunctionNotSupported('installed version of librbd does '
                                           'not support open in read-only mode')
            ret = self.librbd.rbd_open_read_only(ioctx.io, cstr(name),
                                                 byref(self.image),
                                                 cstr(snapshot))
        else:
            ret = self.librbd.rbd_open(ioctx.io, cstr(name),
                                       byref(self.image), cstr(snapshot))
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
            ret = self.librbd.rbd_close(self.image)
            if ret < 0:
                raise make_ex(ret, 'error while closing image %s' % (
                              self.name,))

    def __del__(self):
        self.close()

    def __str__(self):
        s = "rbd.Image(" + dict.__repr__(self.__dict__) + ")"
        return s

    def resize(self, size):
        """
        Change the size of the image.

        :param size: the new size of the image
        :type size: int
        """
        ret = self.librbd.rbd_resize(self.image, c_uint64(size))
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
        info = rbd_image_info_t()
        ret = self.librbd.rbd_stat(self.image, byref(info), ctypes.sizeof(info))
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
        ret = -errno.ERANGE
        size = 8
        while ret == -errno.ERANGE and size <= 4096:
            pool = create_string_buffer(size)
            name = create_string_buffer(size)
            snapname = create_string_buffer(size)
            ret = self.librbd.rbd_get_parent_info(self.image, byref(pool),
                                                  c_size_t(size),
                                                  byref(name),
                                                  c_size_t(size),
                                                  byref(snapname),
                                                  c_size_t(size))
            if ret == -errno.ERANGE:
                size *= 2

        if ret != 0:
            raise make_ex(ret, 'error getting parent info for image %s' % (self.name,))
        return (decode_cstr(pool.value),
                decode_cstr(name.value),
                decode_cstr(snapname.value))

    def old_format(self):
        """
        Find out whether the image uses the old RBD format.

        :returns: bool - whether the image uses the old RBD format
        """
        old = c_uint8()
        ret = self.librbd.rbd_get_old_format(self.image, byref(old))
        if ret != 0:
            raise make_ex(ret, 'error getting old_format for image' % (self.name))
        return old.value != 0

    def size(self):
        """
        Get the size of the image. If open to a snapshot, returns the
        size of that snapshot.

        :returns: the size of the image in bytes
        """
        image_size = c_uint64()
        ret = self.librbd.rbd_get_size(self.image, byref(image_size))
        if ret != 0:
            raise make_ex(ret, 'error getting size for image' % (self.name))
        return image_size.value

    def features(self):
        """
        Gets the features bitmask of the image.

        :returns: int - the features bitmask of the image
        """
        features = c_uint64()
        ret = self.librbd.rbd_get_features(self.image, byref(features))
        if ret != 0:
            raise make_ex(ret, 'error getting features for image' % (self.name))
        return features.value

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
        ret = self.librbd.rbd_update_features(self.image, c_uint64(features),
                                              c_uint8(enabled));
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
        overlap = c_uint64()
        ret = self.librbd.rbd_get_overlap(self.image, byref(overlap))
        if ret != 0:
            raise make_ex(ret, 'error getting overlap for image' % (self.name))
        return overlap.value

    def flags(self):
        """
        Gets the flags bitmask of the image.

        :returns: int - the flags bitmask of the image
        """
        flags = c_uint64()
        ret = self.librbd.rbd_get_flags(self.image, byref(flags))
        if ret != 0:
            raise make_ex(ret, 'error getting flags for image' % (self.name))
        return flags.value

    def is_exclusive_lock_owner(self):
        """
        Gets the status of the image exclusive lock.

        :returns: bool - true if the image is exclusively locked
        """
        owner = c_int()
        ret = self.librbd.rbd_is_exclusive_lock_owner(self.image, byref(owner))
        if ret != 0:
            raise make_ex(ret, 'error getting lock status for image' % (self.name))
        return owner.value == 1

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
        if not isinstance(dest_name, str_type):
            raise TypeError('dest_name must be a string')
        has_copy3 = hasattr(self.librbd, 'rbd_copy3')
        if (stripe_unit != 0 or stripe_count != 0) and not has_copy3:
            raise FunctionNotSupported('installed version of librbd does'
                                       ' not support stripe unit or count')
        if has_copy3:
            opts = c_void_p()
            self.librbd.rbd_image_options_create(byref(opts))
            self.librbd.rbd_image_options_set_uint64(opts,
                                                     RBD_IMAGE_OPTION_FEATURES,
                                                     c_uint64(features))
            self.librbd.rbd_image_options_set_uint64(opts,
                                                     RBD_IMAGE_OPTION_ORDER,
                                                     c_uint64(order))
            self.librbd.rbd_image_options_set_uint64(opts,
                                                     RBD_IMAGE_OPTION_STRIPE_UNIT,
                                                     c_uint64(stripe_unit))
            self.librbd.rbd_image_options_set_uint64(opts,
                                                     RBD_IMAGE_OPTION_STRIPE_COUNT,
                                                     c_uint64(stripe_count))
            ret = self.librbd.rbd_copy3(self.image, dest_ioctx.io,
                                        cstr(dest_name), opts)
            self.librbd.rbd_image_options_get_uint64(opts,
                                                     RBD_IMAGE_OPTION_ORDER,
                                                     byref(c_uint64(order)))
            self.librbd.rbd_image_options_destroy(opts)
        else:
            ret = self.librbd.rbd_copy(self.image, dest_ioctx.io, cstr(dest_name))
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
        if not isinstance(name, str_type):
            raise TypeError('name must be a string')
        ret = self.librbd.rbd_snap_create(self.image, cstr(name))
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
        if not isinstance(srcname, str_type):
            raise TypeError('src name must be a string')
        if not isinstance(dstname, str_type):
            raise TypeError('dst name must be a string')
        ret = self.librbd.rbd_snap_rename(self.image, cstr(srcname), cstr(dstname))
        if ret != 0:
            raise make_ex(ret, 'error renaming snapshot of %s from %s to %s' % (self.name, srcname, dstname))

    def remove_snap(self, name):
        """
        Delete a snapshot of the image.

        :param name: the name of the snapshot
        :type name: str
        :raises: :class:`IOError`, :class:`ImageBusy`
        """
        if not isinstance(name, str_type):
            raise TypeError('name must be a string')
        ret = self.librbd.rbd_snap_remove(self.image, cstr(name))
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
        if not isinstance(name, str_type):
            raise TypeError('name must be a string')
        ret = self.librbd.rbd_snap_rollback(self.image, cstr(name))
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
        if not isinstance(name, str_type):
            raise TypeError('name must be a string')
        ret = self.librbd.rbd_snap_protect(self.image, cstr(name))
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
        if not isinstance(name, str_type):
            raise TypeError('name must be a string')
        ret = self.librbd.rbd_snap_unprotect(self.image, cstr(name))
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
        if not isinstance(name, str_type):
            raise TypeError('name must be a string')
        is_protected = c_int()
        ret = self.librbd.rbd_snap_is_protected(self.image, cstr(name),
                                                byref(is_protected))
        if ret != 0:
            raise make_ex(ret, 'error checking if snapshot %s@%s is protected' % (self.name, name))
        return is_protected.value == 1

    def set_snap(self, name):
        """
        Set the snapshot to read from. Writes will raise ReadOnlyImage
        while a snapshot is set. Pass None to unset the snapshot
        (reads come from the current image) , and allow writing again.

        :param name: the snapshot to read from, or None to unset the snapshot
        :type name: str or None
        """
        if name is not None and not isinstance(name, str_type):
            raise TypeError('name must be a string')
        ret = self.librbd.rbd_snap_set(self.image, cstr(name))
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
        ret_buf = create_string_buffer(length)
        if fadvise_flags == 0:
            ret = self.librbd.rbd_read(self.image, c_uint64(offset),
                                       c_size_t(length), byref(ret_buf))
        else:
            ret = self.librbd.rbd_read2(self.image, c_uint64(offset),
                                        c_size_t(length), byref(ret_buf),
                                        c_int(fadvise_flags))
        if ret < 0:
            raise make_ex(ret, 'error reading %s %ld~%ld' % (self.image, offset, length))

        return ctypes.string_at(ret_buf, ret)

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
        if from_snapshot is not None and not isinstance(from_snapshot, str_type):
            raise TypeError('client must be a string')

        RBD_DIFF_CB = CFUNCTYPE(c_int, c_uint64, c_size_t, c_int, c_void_p)
        cb_holder = DiffIterateCB(iterate_cb)
        cb = RBD_DIFF_CB(cb_holder.callback)
        ret = self.librbd.rbd_diff_iterate2(self.image,
                                            cstr(from_snapshot),
                                            c_uint64(offset),
                                            c_uint64(length),
                                            c_uint8(include_parent),
                                            c_uint8(whole_object),
                                            cb,
                                            c_void_p(None))
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
        length = len(data)

        if fadvise_flags == 0:
            ret = self.librbd.rbd_write(self.image, c_uint64(offset),
                                        c_size_t(length), c_char_p(data))
        else:
            ret = self.librbd.rbd_write2(self.image, c_uint64(offset),
                                         c_size_t(length), c_char_p(data),
                                         c_int(fadvise_flags))

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
        ret = self.librbd.rbd_discard(self.image,
                                      c_uint64(offset),
                                      c_uint64(length))
        if ret < 0:
            msg = 'error discarding region %d~%d' % (offset, length)
            raise make_ex(ret, msg)

    def flush(self):
        """
        Block until all writes are fully flushed if caching is enabled.
        """
        ret = self.librbd.rbd_flush(self.image)
        if ret < 0:
            raise make_ex(ret, 'error flushing image')

    def invalidate_cache(self):
        """
        Drop any cached data for the image.
        """
        ret = self.librbd.rbd_invalidate_cache(self.image)
        if ret < 0:
            raise make_ex(ret, 'error invalidating cache')

    def stripe_unit(self):
        """
        Returns the stripe unit used for the image.
        """
        stripe_unit = c_uint64()
        ret = self.librbd.rbd_get_stripe_unit(self.image, byref(stripe_unit))
        if ret != 0:
            raise make_ex(ret, 'error getting stripe unit for image' % (self.name))
        return stripe_unit.value

    def stripe_count(self):
        """
        Returns the stripe count used for the image.
        """
        stripe_count = c_uint64()
        ret = self.librbd.rbd_get_stripe_count(self.image, byref(stripe_count))
        if ret != 0:
            raise make_ex(ret, 'error getting stripe count for image' % (self.name))
        return stripe_count.value

    def flatten(self):
        """
        Flatten clone image (copy all blocks from parent to child)
        """
        ret = self.librbd.rbd_flatten(self.image)
        if ret < 0:
            raise make_ex(ret, "error flattening %s" % self.name)

    def list_children(self):
        """
        List children of the currently set snapshot (set via set_snap()).

        :returns: list - a list of (pool name, image name) tuples
        """
        pools_size = c_size_t(512)
        images_size = c_size_t(512)
        while True:
            c_pools = create_string_buffer(pools_size.value)
            c_images = create_string_buffer(images_size.value)
            ret = self.librbd.rbd_list_children(self.image,
                                                byref(c_pools),
                                                byref(pools_size),
                                                byref(c_images),
                                                byref(images_size))
            if ret >= 0:
                break
            elif ret != -errno.ERANGE:
                raise make_ex(ret, 'error listing images')
        if ret == 0:
            return []
        pools = map(decode_cstr, c_pools.raw[:pools_size.value - 1].split(b'\0'))
        images = map(decode_cstr, c_images.raw[:images_size.value - 1].split(b'\0'))
        return list(zip(pools, images))

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
        clients_size = c_size_t(512)
        cookies_size = c_size_t(512)
        addrs_size = c_size_t(512)
        tag_size = c_size_t(512)
        exclusive = c_int(0)

        while True:
            c_clients = create_string_buffer(clients_size.value)
            c_cookies = create_string_buffer(cookies_size.value)
            c_addrs = create_string_buffer(addrs_size.value)
            c_tag = create_string_buffer(tag_size.value)
            ret = self.librbd.rbd_list_lockers(self.image,
                                               byref(exclusive),
                                               byref(c_tag),
                                               byref(tag_size),
                                               byref(c_clients),
                                               byref(clients_size),
                                               byref(c_cookies),
                                               byref(cookies_size),
                                               byref(c_addrs),
                                               byref(addrs_size))
            if ret >= 0:
                break
            elif ret != -errno.ERANGE:
                raise make_ex(ret, 'error listing images')
        if ret == 0:
            return []
        clients = [client.decode("utf-8") for client in c_clients.raw[:clients_size.value - 1].split(b'\0')]
        cookies = [cookie.decode("utf-8") for cookie in c_cookies.raw[:cookies_size.value - 1].split(b'\0')]
        addrs = [addr.decode("utf-8") for addr in c_addrs.raw[:addrs_size.value - 1].split(b'\0')]
        return {
            'tag'       : decode_cstr(c_tag),
            'exclusive' : exclusive.value == 1,
            'lockers'   : list(zip(clients, cookies, addrs)),
            }

    def lock_exclusive(self, cookie):
        """
        Take an exclusive lock on the image.

        :raises: :class:`ImageBusy` if a different client or cookie locked it
                 :class:`ImageExists` if the same client and cookie locked it
        """
        if not isinstance(cookie, str_type):
            raise TypeError('cookie must be a string')
        ret = self.librbd.rbd_lock_exclusive(self.image, cstr(cookie))
        if ret < 0:
            raise make_ex(ret, 'error acquiring exclusive lock on image')

    def lock_shared(self, cookie, tag):
        """
        Take a shared lock on the image. The tag must match
        that of the existing lockers, if any.

        :raises: :class:`ImageBusy` if a different client or cookie locked it
                 :class:`ImageExists` if the same client and cookie locked it
        """
        if not isinstance(cookie, str_type):
            raise TypeError('cookie must be a string')
        if not isinstance(tag, str_type):
            raise TypeError('tag must be a string')
        ret = self.librbd.rbd_lock_shared(self.image, cstr(cookie),
                                          cstr(tag))
        if ret < 0:
            raise make_ex(ret, 'error acquiring shared lock on image')

    def unlock(self, cookie):
        """
        Release a lock on the image that was locked by this rados client.
        """
        if not isinstance(cookie, str_type):
            raise TypeError('cookie must be a string')
        ret = self.librbd.rbd_unlock(self.image, cstr(cookie))
        if ret < 0:
            raise make_ex(ret, 'error unlocking image')

    def break_lock(self, client, cookie):
        """
        Release a lock held by another rados client.
        """
        if not isinstance(client, str_type):
            raise TypeError('client must be a string')
        if not isinstance(cookie, str_type):
            raise TypeError('cookie must be a string')
        ret = self.librbd.rbd_break_lock(self.image, cstr(client),
                                         cstr(cookie))
        if ret < 0:
            raise make_ex(ret, 'error unlocking image')


class DiffIterateCB(object):
    def __init__(self, cb):
        self.cb = cb

    def callback(self, offset, length, exists, unused):
        self.cb(offset, length, exists == 1)
        return 0


class SnapIterator(Iterable):
    """
    Iterator over snapshot info for an image.

    Yields a dictionary containing information about a snapshot.

    Keys are:

    * ``id`` (int) - numeric identifier of the snapshot

    * ``size`` (int) - size of the image at the time of snapshot (in bytes)

    * ``name`` (str) - name of the snapshot
    """
    def __init__(self, image):
        self.librbd = image.librbd
        num_snaps = c_int(10)
        while True:
            self.snaps = (rbd_snap_info_t * num_snaps.value)()
            ret = self.librbd.rbd_snap_list(image.image, byref(self.snaps),
                                            byref(num_snaps))
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

    def __del__(self):
        self.librbd.rbd_snap_list_end(self.snaps)
