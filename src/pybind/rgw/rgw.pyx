"""
This module is a thin wrapper around rgw_file.
"""


from cpython cimport PyObject, ref, exc, array
from libc.stdint cimport *
from libc.stdlib cimport malloc, realloc, free
from cstat cimport stat
cimport libcpp

IF BUILD_DOC:
    include "mock_rgw.pxi"
ELSE:
    from c_rgw cimport *
    cimport rados

from collections import namedtuple
from datetime import datetime
import errno


cdef extern from "Python.h":
    # These are in cpython/string.pxd, but use "object" types instead of
    # PyObject*, which invokes assumptions in cpython that we need to
    # legitimately break to implement zero-copy string buffers in Image.read().
    # This is valid use of the Python API and documented as a special case.
    PyObject *PyBytes_FromStringAndSize(char *v, Py_ssize_t len) except NULL
    char* PyBytes_AsString(PyObject *string) except NULL
    int _PyBytes_Resize(PyObject **string, Py_ssize_t newsize) except -1
    void PyEval_InitThreads()


class Error(Exception):
    pass


class OSError(Error):
    """ `OSError` class, derived from `Error` """
    def __init__(self, errno, strerror):
        self.errno = errno
        self.strerror = strerror

    def __str__(self):
        return '[Errno {0}] {1}'.format(self.errno, self.strerror)


class PermissionError(OSError):
    pass


class ObjectNotFound(OSError):
    pass


class NoData(Error):
    pass


class ObjectExists(Error):
    pass


class IOError(OSError):
    pass


class NoSpace(Error):
    pass


class InvalidValue(Error):
    pass


class OperationNotSupported(Error):
    pass


class IncompleteWriteError(Error):
    pass


class LibCephFSStateError(Error):
    pass

class WouldBlock(Error):
    pass

class OutOfRange(Error):
    pass

IF UNAME_SYSNAME == "FreeBSD":
    cdef errno_to_exception =  {
        errno.EPERM      : PermissionError,
        errno.ENOENT     : ObjectNotFound,
        errno.EIO        : IOError,
        errno.ENOSPC     : NoSpace,
        errno.EEXIST     : ObjectExists,
        errno.ENOATTR    : NoData,
        errno.EINVAL     : InvalidValue,
        errno.EOPNOTSUPP : OperationNotSupported,
        errno.ERANGE     : OutOfRange,
        errno.EWOULDBLOCK: WouldBlock,
    }
ELSE:
    cdef errno_to_exception =  {
        errno.EPERM      : PermissionError,
        errno.ENOENT     : ObjectNotFound,
        errno.EIO        : IOError,
        errno.ENOSPC     : NoSpace,
        errno.EEXIST     : ObjectExists,
        errno.ENODATA    : NoData,
        errno.EINVAL     : InvalidValue,
        errno.EOPNOTSUPP : OperationNotSupported,
        errno.ERANGE     : OutOfRange,
        errno.EWOULDBLOCK: WouldBlock,
    }


cdef class FileHandle(object):
    cdef rgw_file_handle *handler


StatResult = namedtuple('StatResult',
                        ["st_dev", "st_ino", "st_mode", "st_nlink", "st_uid",
                         "st_gid", "st_rdev", "st_size", "st_blksize",
                         "st_blocks", "st_atime", "st_mtime", "st_ctime"])


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
        raise TypeError('%s must be a string' % name)


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
        return errno_to_exception[ret](ret, msg)
    else:
        return Error(msg + (": error code %d" % ret))


cdef bint readdir_cb(const char *name, void *arg, uint64_t offset, stat *st, uint32_t st_mask, uint32_t flags) \
except? -9000 with gil:
    if exc.PyErr_Occurred():
        return False
    (<object>arg)(name, offset, flags)
    return True


class LibCephFSStateError(Error):
    pass


cdef class LibRGWFS(object):
    """librgwfs python wrapper"""

    cdef public object state
    cdef public object uid
    cdef public object key
    cdef public object secret
    cdef librgw_t cluster
    cdef rgw_fs *fs

    def require_state(self, *args):
        if self.state in args:
            return
        raise LibCephFSStateError("You cannot perform that operation on a "
                                  "RGWFS object in state %s." % (self.state))

    def __cinit__(self, uid, key, secret):
        PyEval_InitThreads()
        self.state = "umounted"
        ret = librgw_create(&self.cluster, 0, NULL)
        if ret != 0:
            raise make_ex(ret, "error calling librgw_create")
        self.uid = cstr(uid, "uid")
        self.key = cstr(key, "key")
        self.secret = cstr(secret, "secret")

    def shutdown(self):
        """
        Unmount and destroy the ceph mount handle.
        """
        if self.state in ["mounted"]:
            with nogil:
                ret = rgw_umount(self.fs, 0);
            if ret != 0:
                raise make_ex(ret, "error calling rgw_unmount")
            self.state = "shutdown"

    def __enter__(self):
        self.mount()
        return self

    def __exit__(self, type_, value, traceback):
        self.shutdown()

    def __dealloc__(self):
        self.shutdown()

    def version(self):
        """
        Get the version number of the ``librgwfile`` C library.

        :returns: a tuple of ``(major, minor, extra)`` components of the
                  libcephfs version
        """
        cdef:
            int major = 0
            int minor = 0
            int extra = 0
        with nogil:
            rgwfile_version(&major, &minor, &extra)
        return (major, minor, extra)

    def mount(self):
        self.require_state("umounted")
        cdef:
            char *_uid = self.uid
            char *_key = self.key
            char *_secret = self.secret
        with nogil:
            ret = rgw_mount(self.cluster, <const char*>_uid, <const char*>_key,
                            <const char*>_secret, &self.fs, 0)
        if ret != 0:
            raise make_ex(ret, "error calling rgw_mount")
        self.state = "mounted"
        dir_handler = FileHandle()
        dir_handler.handler = <rgw_file_handle*>self.fs.root_fh
        return dir_handler

    def unmount(self):
        self.require_state("mounted")
        with nogil:
            ret = rgw_umount(self.fs, 0)
        if ret != 0:
            raise make_ex(ret, "error calling rgw_umount")
        self.state = "umounted"

    def statfs(self):
        self.require_state("mounted")
        cdef:
            rgw_statvfs statbuf

        with nogil:
            ret = rgw_statfs(self.fs, <rgw_file_handle*>self.fs.root_fh, &statbuf, 0)
        if ret < 0:
            raise make_ex(ret, "statfs failed")
        cdef uint64_t[:] fsid = statbuf.f_fsid
        return {'f_bsize': statbuf.f_bsize,
                'f_frsize': statbuf.f_frsize,
                'f_blocks': statbuf.f_blocks,
                'f_bfree': statbuf.f_bfree,
                'f_bavail': statbuf.f_bavail,
                'f_files': statbuf.f_files,
                'f_ffree': statbuf.f_ffree,
                'f_favail': statbuf.f_favail,
                'f_fsid': fsid,
                'f_flag': statbuf.f_flag,
                'f_namemax': statbuf.f_namemax}


    def create(self, FileHandle dir_handler, filename, flags = 0):
        self.require_state("mounted")

        if not isinstance(flags, int):
            raise TypeError("flags must be an integer")

        filename = cstr(filename, 'filename')

        cdef:
            rgw_file_handle *_dir_handler = <rgw_file_handle*>dir_handler.handler
            rgw_file_handle *_file_handler
            int _flags = flags
            char* _filename = filename
            stat statbuf

        with nogil:
            ret = rgw_create(self.fs, _dir_handler, _filename, &statbuf, 0,
                             &_file_handler, 0, _flags)
        if ret < 0:
            raise make_ex(ret, "error in create '%s'" % filename)
        with nogil:
            ret = rgw_open(self.fs, _file_handler, 0, _flags)
        if ret < 0:
            raise make_ex(ret, "error in open '%s'" % filename)

        file_handler = FileHandle()
        file_handler.handler = _file_handler
        return file_handler

    def mkdir(self, FileHandle dir_handler, dirname, flags = 0):
        self.require_state("mounted")
        dirname = cstr(dirname, 'dirname')
        new_dir_handler = FileHandle()
        cdef:
            rgw_file_handle *_dir_handler = <rgw_file_handle*>dir_handler.handler
            rgw_file_handle *_new_dir_handler
            char* _dirname = dirname
            int _flags = flags
            stat statbuf
        with nogil:
            ret = rgw_mkdir(self.fs, _dir_handler, _dirname, &statbuf,
                            0, &_new_dir_handler, _flags)
        if ret < 0:
            raise make_ex(ret, "error in mkdir '%s'" % dirname)
        new_dir_handler.handler = _new_dir_handler
        return new_dir_handler

    def rename(self, FileHandle src_handler, src_name, FileHandle dst_handler, dst_name, flags = 0):
        self.require_state("mounted")

        src_name = cstr(src_name, 'src_name')
        dst_name = cstr(dst_name, 'dst_name')

        cdef:
            rgw_file_handle *_src_dir_handler = <rgw_file_handle*>src_handler.handler
            rgw_file_handle *_dst_dir_handler = <rgw_file_handle*>dst_handler.handler
            char* _src_name = src_name
            char* _dst_name = dst_name
            int _flags = flags

        with nogil:
            ret = rgw_rename(self.fs, _src_dir_handler, _src_name,
                             _dst_dir_handler, _dst_name, _flags)
        if ret < 0:
            raise make_ex(ret, "error in rename '%s' to '%s'" % (src_name,
                                                                 dst_name))
        return ret

    def unlink(self, FileHandle handler, name, flags = 0):
        self.require_state("mounted")
        name = cstr(name, 'name')
        cdef:
            rgw_file_handle *_handler = <rgw_file_handle*>handler.handler
            int _flags = flags
            char* _name = name
        with nogil:
            ret = rgw_unlink(self.fs, _handler, _name, _flags)
        if ret < 0:
            raise make_ex(ret, "error in unlink")
        return ret

    def readdir(self, FileHandle dir_handler, iterate_cb, offset, flags = 0):
        self.require_state("mounted")

        cdef:
            rgw_file_handle *_dir_handler = <rgw_file_handle*>dir_handler.handler
            uint64_t _offset = offset
            libcpp.bool _eof
            uint32_t _flags = flags
        with nogil:
            ret = rgw_readdir(self.fs, _dir_handler, &_offset, &readdir_cb,
                              <void *>iterate_cb, &_eof, _flags)
        if ret < 0:
            raise make_ex(ret, "error in readdir")

        return (_offset, _eof)

    def fstat(self, FileHandle file_handler):
        self.require_state("mounted")

        cdef:
            rgw_file_handle *_file_handler = <rgw_file_handle*>file_handler.handler
            stat statbuf

        with nogil:
            ret = rgw_getattr(self.fs, _file_handler, &statbuf, 0)
        if ret < 0:
            raise make_ex(ret, "error in getattr")
        return StatResult(st_dev=statbuf.st_dev, st_ino=statbuf.st_ino,
                          st_mode=statbuf.st_mode, st_nlink=statbuf.st_nlink,
                          st_uid=statbuf.st_uid, st_gid=statbuf.st_gid,
                          st_rdev=statbuf.st_rdev, st_size=statbuf.st_size,
                          st_blksize=statbuf.st_blksize,
                          st_blocks=statbuf.st_blocks,
                          st_atime=datetime.fromtimestamp(statbuf.st_atime),
                          st_mtime=datetime.fromtimestamp(statbuf.st_mtime),
                          st_ctime=datetime.fromtimestamp(statbuf.st_ctime))

    def opendir(self, FileHandle dir_handler, dirname, flags = 0):
        self.require_state("mounted")

        if not isinstance(flags, int):
            raise TypeError("flags must be an integer")

        dirname = cstr(dirname, 'dirname')

        cdef:
            rgw_file_handle *_dir_handler = <rgw_file_handle*>dir_handler.handler
            rgw_file_handle *_file_handler
            int _flags = flags
            char* _dirname = dirname
            stat st
            uint32_t st_mask = 0

        with nogil:
            ret = rgw_lookup(self.fs, _dir_handler, _dirname,
                             &_file_handler, &st, st_mask, _flags)
        if ret < 0:
            raise make_ex(ret, "error in open '%s'" % dirname)

        file_handler = FileHandle()
        file_handler.handler = _file_handler
        return file_handler

    def open(self, FileHandle dir_handler, filename, flags = 0):
        self.require_state("mounted")

        if not isinstance(flags, int):
            raise TypeError("flags must be an integer")

        filename = cstr(filename, 'filename')

        cdef:
            rgw_file_handle *_dir_handler = <rgw_file_handle*>dir_handler.handler
            rgw_file_handle *_file_handler
            int _flags = flags
            char* _filename = filename
            stat st
            uint32_t st_mask = 0

        with nogil:
            ret = rgw_lookup(self.fs, _dir_handler, _filename,
                             &_file_handler, &st, st_mask, _flags)
        if ret < 0:
            raise make_ex(ret, "error in open '%s'" % filename)
        with nogil:
            ret = rgw_open(self.fs, _file_handler, 0, _flags)
        if ret < 0:
            raise make_ex(ret, "error in open '%s'" % filename)

        file_handler = FileHandle()
        file_handler.handler = _file_handler
        return file_handler

    def close(self, FileHandle file_handler, flags = 0):
        self.require_state("mounted")
        cdef:
            rgw_file_handle *_file_handler = <rgw_file_handle*>file_handler.handler
            int _flags = flags
        with nogil:
            ret = rgw_close(self.fs, _file_handler, _flags)
        if ret < 0:
            raise make_ex(ret, "error in close")

    def read(self, FileHandle file_handler, offset, l, flags = 0):
        self.require_state("mounted")
        if not isinstance(offset, int):
            raise TypeError('offset must be an int')
        if not isinstance(l, int):
            raise TypeError('l must be an int')
        cdef:
            rgw_file_handle *_file_handler = <rgw_file_handle*>file_handler.handler
            int64_t _offset = offset
            size_t _length = l
            size_t _got
            int _flags = flags

            char *ret_buf
            PyObject* ret_s = NULL

        ret_s = PyBytes_FromStringAndSize(NULL, _length)
        try:
            ret_buf = PyBytes_AsString(ret_s)
            with nogil:
                ret = rgw_read(self.fs, _file_handler, _offset, _length,
                               &_got, ret_buf, _flags)
            if ret < 0:
                raise make_ex(ret, "error in read")

            if _got < _length:
                _PyBytes_Resize(&ret_s, _got)

            return <object>ret_s
        finally:
            # We DECREF unconditionally: the cast to object above will have
            # INCREFed if necessary. This also takes care of exceptions,
            # including if _PyString_Resize fails (that will free the string
            # itself and set ret_s to NULL, hence XDECREF).
            ref.Py_XDECREF(ret_s)


    def write(self, FileHandle file_handler, offset, buf, flags = 0):
        self.require_state("mounted")
        if not isinstance(buf, bytes):
            raise TypeError('buf must be a bytes')
        if not isinstance(offset, int):
            raise TypeError('offset must be an int')

        cdef:
            rgw_file_handle *_file_handler = <rgw_file_handle*>file_handler.handler
            char *_data = buf
            int64_t _offset = offset

            size_t length = len(buf)
            int _flags = flags
            size_t _written

        with nogil:
            ret = rgw_write(self.fs, _file_handler, _offset, length, &_written,
                            _data, _flags)
        if ret < 0:
            raise make_ex(ret, "error in write")
        return ret


    def fsync(self, FileHandle handler, flags = 0):
        self.require_state("mounted")

        cdef:
            rgw_file_handle *_file_handler = <rgw_file_handle*>handler.handler
            int _flags = flags
        with nogil:
            ret = rgw_fsync(self.fs, _file_handler, _flags)

        if ret < 0:
            raise make_ex(ret, "fsync failed")
