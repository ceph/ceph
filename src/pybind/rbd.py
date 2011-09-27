"""librbd Python ctypes wrapper
Copyright 2011, Josh Durgin
"""
from ctypes import CDLL, c_char, c_char_p, c_size_t, c_void_p, c_int, \
    create_string_buffer, byref, Structure, c_uint64
import ctypes
import errno

ANONYMOUS_AUID = 0xffffffffffffffff
ADMIN_AUID = 0

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

def make_ex(ret, msg):
    ret = abs(ret)
    if (ret == errno.EPERM):
        return PermissionError(msg)
    elif (ret == errno.ENOENT):
        return ImageNotFound(msg)
    elif (ret == errno.EIO):
        return IOError(msg)
    elif (ret == errno.ENOSPC):
        return NoSpace(msg)
    elif (ret == errno.EEXIST):
        return ImageExists(msg)
    elif (ret == errno.EINVAL):
        return InvalidArgument(msg)
    else:
        return Error(msg + (": error code %d" % ret))

class rbd_image_info_t(Structure):
    _fields_ = [("size", c_uint64),
                ("obj_size", c_uint64),
                ("num_objs", c_uint64),
                ("order", c_int),
                ("block_name_prefix", c_char * 24),
                ("parent_pool", c_int),
                ("parent_name", c_char * 96)]

class rbd_snap_info_t(Structure):
    _fields_ = [("id", c_uint64),
                ("size", c_uint64),
                ("name", c_char_p)]

class RBD(object):
    """librbd python wrapper"""
    def __init__(self):
        self.librbd = CDLL('librbd.so.1')

    def version(self):
        major = c_int(0)
        minor = c_int(0)
        extra = c_int(0)
        self.librbd.rbd_version(byref(major), byref(minor), byref(extra))
        return (major.value, minor.value, extra.value)

    def create(self, ioctx, name, size, order=None):
        """
        Create an rbd image. Size is in bytes.
        The image is split into (1 << order) byte objects.
        """
        if order is None:
            order = 0
        if not isinstance(name, str):
            raise TypeError('name must be a string')
        ret = self.librbd.rbd_create(ioctx.io, c_char_p(name), c_uint64(size),
                                     byref(c_int(order)))
        if ret < 0:
            raise make_ex(ret, 'error creating image')

    def list(self, ioctx):
        size = c_size_t(512)
        while True:
            c_names = create_string_buffer(size.value)
            ret = self.librbd.rbd_list(ioctx.io, byref(c_names), byref(size))
            if ret >= 0:
                break
            elif ret != -errno.ERANGE:
                raise make_ex(ret, 'error listing images')
        return c_names.raw.rstrip('\0').split('\0')

    def remove(self, ioctx, name):
        if not isinstance(name, str):
            raise TypeError('name must be a string')
        ret = self.librbd.rbd_remove(ioctx.io, c_char_p(name))
        if ret != 0:
            raise make_ex(ret, 'error removing image')

    def rename(self, ioctx, src, dest):
        if not isinstance(src, str) or not isinstance(dest, str):
            raise TypeError('src and dest must be strings')
        ret = self.librbd.rbd_rename(ioctx.io, c_char_p(src), c_char_p(dest))
        if ret != 0:
            raise make_ex(ret, 'error renaming image')

class Image(object):
    """librbd python wrapper"""

    def __init__(self, ioctx, name, snapshot=None):
        self.librbd = CDLL('librbd.so.1')
        self.image = c_void_p()
        self.name = name
        self.closed = False
        if not isinstance(name, str):
            raise TypeError('name must be a string')
        if snapshot is not None and not isinstance(snapshot, str):
            raise TypeError('snapshot must be a string or None')
        ret = self.librbd.rbd_open(ioctx.io, c_char_p(name),
                                   byref(self.image), c_char_p(snapshot))
        if ret != 0:
            raise make_ex(ret, 'error opening image %s at snapshot %s' % (name, snapshot))

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        self.close()
        return False

    def close(self):
        if not self.closed:
            self.closed = True
            self.librbd.rbd_close(self.image)

    def __del__(self):
        self.close()

    def resize(self, size):
        ret = self.librbd.rbd_resize(self.image, c_uint64(size))
        if ret < 0:
            raise make_ex(ret, 'error resizing image %s' % (self.name,))

    def stat(self):
        info = rbd_image_info_t()
        ret = self.librbd.rbd_stat(self.image, byref(info), ctypes.sizeof(info))
        if ret != 0:
            raise make_ex(ret, 'error getting info for image %s' % (self.name,))
        return {
            'size'              : info.size,
            'obj_size'          : info.obj_size,
            'num_objs'          : info.num_objs,
            'order'             : info.order,
            'block_name_prefix' : info.block_name_prefix,
            'parent_pool'       : info.parent_pool,
            'parent_name'       : info.parent_name,
            }

    def copy(self, dest_ioctx, dest_name):
        if not isinstance(dest_name, str):
            raise TypeError('dest_name must be a string')
        ret = self.librbd.rbd_copy(self.image, dest_ioctx.io, c_char_p(dest_name))
        if ret < 0:
            raise make_ex(ret, 'error copying image %s to %s' % (self.name, dest_name))
        return ret

    def list_snaps(self):
        return SnapIterator(self)

    def create_snap(self, name):
        if not isinstance(name, str):
            raise TypeError('name must be a string')
        ret = self.librbd.rbd_snap_create(self.image, c_char_p(name))
        if ret != 0:
            raise make_ex(ret, 'error creating snapshot %s from %s' % (name, self.name))

    def remove_snap(self, name):
        if not isinstance(name, str):
            raise TypeError('name must be a string')
        ret = self.librbd.rbd_snap_remove(self.image, c_char_p(name))
        if ret != 0:
            raise make_ex(ret, 'error removing snapshot %s from %s' % (name, self.name))

    def rollback_to_snap(self, name):
        if not isinstance(name, str):
            raise TypeError('name must be a string')
        ret = self.librbd.rbd_snap_rollback(self.image, c_char_p(name))
        if ret != 0:
            raise make_ex(ret, 'error rolling back image %s to snapshot %s' % (self.name, name))

    def set_snap(self, name):
        if not isinstance(name, str):
            raise TypeError('name must be a string')
        ret = self.librbd.rbd_snap_set(self.image, c_char_p(name))
        if ret != 0:
            raise make_ex(ret, 'error setting image %s to snapshot %s' % (self.name, name))

    def read(self, offset, length):
        ret_buf = create_string_buffer(length)
        ret = self.librbd.rbd_read(self.image, c_uint64(offset),
                                   c_size_t(length), byref(ret_buf))
        if ret < 0:
            raise make_ex(ret, 'error reading %s %ld~%ld' % (self.image, offset, length))
        return ctypes.string_at(ret_buf, ret)

    def write(self, data, offset):
        if not isinstance(data, str):
            raise TypeError('data must be a string')
        length = len(data)
        ret = self.librbd.rbd_write(self.image, c_uint64(offset),
                                    c_size_t(length), c_char_p(data))
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

class SnapIterator(object):
    """Snapshot iterator"""
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
        for i in xrange(self.num_snaps):
            yield {
                'id'   : self.snaps[i].id,
                'size' : self.snaps[i].size,
                'name' : self.snaps[i].name,
                }

    def __del__(self):
        self.librbd.rbd_snap_list_end(self.snaps)
