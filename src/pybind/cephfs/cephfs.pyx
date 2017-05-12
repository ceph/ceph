"""
This module is a thin wrapper around libcephfs.
"""

from cpython cimport PyObject, ref, exc
from libc cimport errno
from libc.stdint cimport *
from libc.stdlib cimport malloc, realloc, free

cimport rados

from collections import namedtuple
from datetime import datetime
import errno
import os
import sys

# Are we running Python 2.x
_python2 = sys.hexversion < 0x03000000

if _python2:
    str_type = basestring
else:
    str_type = str


cdef extern from "Python.h":
    # These are in cpython/string.pxd, but use "object" types instead of
    # PyObject*, which invokes assumptions in cpython that we need to
    # legitimately break to implement zero-copy string buffers in Image.read().
    # This is valid use of the Python API and documented as a special case.
    PyObject *PyBytes_FromStringAndSize(char *v, Py_ssize_t len) except NULL
    char* PyBytes_AsString(PyObject *string) except NULL
    int _PyBytes_Resize(PyObject **string, Py_ssize_t newsize) except -1
    void PyEval_InitThreads()


cdef extern from "sys/statvfs.h":
    cdef struct statvfs:
        unsigned long int f_bsize
        unsigned long int f_frsize
        unsigned long int f_blocks
        unsigned long int f_bfree
        unsigned long int f_bavail
        unsigned long int f_files
        unsigned long int f_ffree
        unsigned long int f_favail
        unsigned long int f_fsid
        unsigned long int f_flag
        unsigned long int f_namemax
        unsigned long int f_padding[32]


cdef extern from "dirent.h":
    cdef struct dirent:
        long int d_ino
        unsigned long int d_off
        unsigned short int d_reclen
        unsigned char d_type
        char d_name[256]


cdef extern from "time.h":
    ctypedef long int time_t


cdef extern from "sys/types.h":
    ctypedef unsigned long mode_t


cdef extern from "sys/stat.h":
    cdef struct stat:
        unsigned long st_dev
        unsigned long st_ino
        unsigned long st_nlink
        unsigned int st_mode
        unsigned int st_uid
        unsigned int st_gid
        int __pad0
        unsigned long st_rdev
        long int st_size
        long int st_blksize
        long int st_blocks
        time_t st_atime
        time_t st_mtime
        time_t st_ctime


cdef extern from "cephfs/libcephfs.h" nogil:
    cdef struct ceph_mount_info:
        pass

    cdef struct ceph_dir_result:
        pass

    ctypedef void* rados_t

    const char *ceph_version(int *major, int *minor, int *patch)

    int ceph_create(ceph_mount_info **cmount, const char * const id)
    int ceph_create_from_rados(ceph_mount_info **cmount, rados_t cluster)
    int ceph_init(ceph_mount_info *cmount)
    void ceph_shutdown(ceph_mount_info *cmount)

    int ceph_conf_read_file(ceph_mount_info *cmount, const char *path_list)
    int ceph_conf_parse_argv(ceph_mount_info *cmount, int argc, const char **argv)
    int ceph_conf_get(ceph_mount_info *cmount, const char *option, char *buf, size_t len)
    int ceph_conf_set(ceph_mount_info *cmount, const char *option, const char *value)

    int ceph_mount(ceph_mount_info *cmount, const char *root)
    int ceph_fstat(ceph_mount_info *cmount, int fd, stat *stbuf)
    int ceph_stat(ceph_mount_info *cmount, const char *path, stat *stbuf)
    int ceph_statfs(ceph_mount_info *cmount, const char *path, statvfs *stbuf)

    int ceph_mds_command(ceph_mount_info *cmount, const char *mds_spec, const char **cmd, size_t cmdlen,
                         const char *inbuf, size_t inbuflen, char **outbuf, size_t *outbuflen,
                         char **outs, size_t *outslen)
    int ceph_rename(ceph_mount_info *cmount, const char *from_, const char *to)
    int ceph_unlink(ceph_mount_info *cmount, const char *path)
    int ceph_symlink(ceph_mount_info *cmount, const char *existing, const char *newname)
    int ceph_setxattr(ceph_mount_info *cmount, const char *path, const char *name,
                      const void *value, size_t size, int flags)
    int ceph_getxattr(ceph_mount_info *cmount, const char *path, const char *name,
                      void *value, size_t size)
    int ceph_write(ceph_mount_info *cmount, int fd, const char *buf, int64_t size, int64_t offset)
    int ceph_read(ceph_mount_info *cmount, int fd, char *buf, int64_t size, int64_t offset)
    int ceph_flock(ceph_mount_info *cmount, int fd, int operation, uint64_t owner)
    int ceph_close(ceph_mount_info *cmount, int fd)
    int ceph_open(ceph_mount_info *cmount, const char *path, int flags, mode_t mode)
    int ceph_mkdir(ceph_mount_info *cmount, const char *path, mode_t mode)
    int ceph_mkdirs(ceph_mount_info *cmount, const char *path, mode_t mode)
    int ceph_closedir(ceph_mount_info *cmount, ceph_dir_result *dirp)
    int ceph_opendir(ceph_mount_info *cmount, const char *name, ceph_dir_result **dirpp)
    int ceph_chdir(ceph_mount_info *cmount, const char *path)
    dirent * ceph_readdir(ceph_mount_info *cmount, ceph_dir_result *dirp)
    int ceph_rmdir(ceph_mount_info *cmount, const char *path)
    const char* ceph_getcwd(ceph_mount_info *cmount)
    int ceph_sync_fs(ceph_mount_info *cmount)
    int ceph_fsync(ceph_mount_info *cmount, int fd, int syncdataonly)
    int ceph_conf_parse_argv(ceph_mount_info *cmount, int argc, const char **argv)
    void ceph_buffer_free(char *buf)



class Error(Exception):
    pass


class PermissionError(Error):
    pass


class ObjectNotFound(Error):
    pass


class NoData(Error):
    pass


class ObjectExists(Error):
    pass


class IOError(Error):
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
        return errno_to_exception[ret](msg)
    else:
        return Error(msg + (": error code %d" % ret))


class DirEntry(namedtuple('DirEntry',
               ['d_ino', 'd_off', 'd_reclen', 'd_type', 'd_name'])):
    DT_DIR = 0x4
    DT_REG = 0xA
    DT_LNK = 0xC
    def is_dir(self):
        return self.d_type == self.DT_DIR

    def is_symbol_file(self):
        return self.d_type == self.DT_LNK

    def is_file(self):
        return self.d_type == self.DT_REG

StatResult = namedtuple('StatResult',
                        ["st_dev", "st_ino", "st_mode", "st_nlink", "st_uid",
                         "st_gid", "st_rdev", "st_size", "st_blksize",
                         "st_blocks", "st_atime", "st_mtime", "st_ctime"])

cdef class DirResult(object):
    cdef ceph_dir_result *handler


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


cdef char ** to_bytes_array(list_bytes):
    cdef char **ret = <char **>malloc(len(list_bytes) * sizeof(char *))
    if ret == NULL:
        raise MemoryError("malloc failed")
    for i in xrange(len(list_bytes)):
        ret[i] = <char *>list_bytes[i]
    return ret


cdef void* realloc_chk(void* ptr, size_t size) except NULL:
    cdef void *ret = realloc(ptr, size)
    if ret == NULL:
        raise MemoryError("realloc failed")
    return ret


cdef class LibCephFS(object):
    """libcephfs python wrapper"""

    cdef public object state
    cdef ceph_mount_info *cluster

    def require_state(self, *args):
        if self.state in args:
            return
        raise LibCephFSStateError("You cannot perform that operation on a "
                                  "CephFS object in state %s." % (self.state))

    def __cinit__(self, conf=None, conffile=None, auth_id=None, rados_inst=None):
        PyEval_InitThreads()
        self.state = "uninitialized"
        if rados_inst is not None:
            if auth_id is not None or conffile is not None or conf is not None:
                raise make_ex(errno.EINVAL,
                              "May not pass RADOS instance as well as other configuration")

            self.create_with_rados(rados_inst)
        else:
            self.create(conf, conffile, auth_id)

    def create_with_rados(self, rados.Rados rados_inst):
        cdef int ret
        with nogil:
            ret = ceph_create_from_rados(&self.cluster, rados_inst.cluster)
        if ret != 0:
            raise Error("libcephfs_initialize failed with error code: %d" % ret)
        self.state = "configuring"

    def create(self, conf=None, conffile=None, auth_id=None):
        if conf is not None and not isinstance(conf, dict):
            raise TypeError("conf must be dict or None")
        cstr(conffile, 'configfile', opt=True)
        auth_id = cstr(auth_id, 'configfile', opt=True)

        cdef:
            char* _auth_id = opt_str(auth_id)
            int ret

        with nogil:
            ret = ceph_create(&self.cluster, <const char*>_auth_id)
        if ret != 0:
            raise Error("libcephfs_initialize failed with error code: %d" % ret)

        self.state = "configuring"
        if conffile is not None:
            # read the default conf file when '' is given
            if conffile == '':
                conffile = None
            self.conf_read_file(conffile)
        if conf is not None:
            for key, value in conf.iteritems():
                self.conf_set(key, value)

    def conf_read_file(self, conffile=None):
        conffile = cstr(conffile, 'conffile', opt=True)
        cdef:
            char *_conffile = opt_str(conffile)
        with nogil:
            ret = ceph_conf_read_file(self.cluster, <const char*>_conffile)
        if ret != 0:
            raise make_ex(ret, "error calling conf_read_file")

    def conf_parse_argv(self, argv):
        self.require_state("configuring")
        cargv = cstr_list(argv, 'argv')
        cdef:
            int _argc = len(argv)
            char **_argv = to_bytes_array(cargv)

        try:
            with nogil:
                ret = ceph_conf_parse_argv(self.cluster, _argc,
                                           <const char **>_argv)
            if ret != 0:
                raise make_ex(ret, "error calling conf_parse_argv")
        finally:
            free(_argv)

    def shutdown(self):
        """
        Unmount and destroy the ceph mount handle.
        """
        if self.state in ["initialized", "mounted"]:
            with nogil:
                ceph_shutdown(self.cluster)
            self.state = "shutdown"

    def __enter__(self):
        self.mount()
        return self

    def __exit__(self, type_, value, traceback):
        self.shutdown()
        return False

    def __dealloc__(self):
        self.shutdown()

    def version(self):
        """
        Get the version number of the ``libcephfs`` C library.

        :returns: a tuple of ``(major, minor, extra)`` components of the
                  libcephfs version
        """
        cdef:
            int major = 0
            int minor = 0
            int extra = 0
        with nogil:
            ceph_version(&major, &minor, &extra)
        return (major, minor, extra)

    def conf_get(self, option):
        self.require_state("configuring", "initialized", "mounted")

        option = cstr(option, 'option')
        cdef:
            char *_option = option
            size_t length = 20
            char *ret_buf = NULL

        try:
            while True:
                ret_buf = <char *>realloc_chk(ret_buf, length)
                with nogil:
                    ret = ceph_conf_get(self.cluster, _option, ret_buf, length)
                if (ret == 0):
                    return decode_cstr(ret_buf)
                elif (ret == -errno.ENAMETOOLONG):
                    length = length * 2
                elif (ret == -errno.ENOENT):
                    return None
                else:
                    raise make_ex(ret, "error calling conf_get")
        finally:
            free(ret_buf)

    def conf_set(self, option, val):
        self.require_state("configuring", "initialized", "mounted")

        option = cstr(option, 'option')
        val = cstr(val, 'val')
        cdef:
            char *_option = option
            char *_val = val

        with nogil:
            ret = ceph_conf_set(self.cluster, _option, _val)
        if (ret != 0):
            raise make_ex(ret, "error calling conf_set")

    def init(self):
        self.require_state("configuring")
        with nogil:
            ret = ceph_init(self.cluster)
        if ret != 0:
            raise make_ex(ret, "error calling ceph_init")
        self.state = "initialized"

    def mount(self):
        if self.state == "configuring":
            self.init()
        self.require_state("initialized")
        with nogil:
            ret = ceph_mount(self.cluster, "/")
        if ret != 0:
            raise make_ex(ret, "error calling ceph_mount")
        self.state = "mounted"

    def statfs(self, path):
        self.require_state("mounted")
        path = cstr(path, 'path')
        cdef:
            char* _path = path
            statvfs statbuf

        with nogil:
            ret = ceph_statfs(self.cluster, _path, &statbuf)
        if ret < 0:
            raise make_ex(ret, "statfs failed: %s" % path)
        return {'f_bsize': statbuf.f_bsize,
                'f_frsize': statbuf.f_frsize,
                'f_blocks': statbuf.f_blocks,
                'f_bfree': statbuf.f_bfree,
                'f_bavail': statbuf.f_bavail,
                'f_files': statbuf.f_files,
                'f_ffree': statbuf.f_ffree,
                'f_favail': statbuf.f_favail,
                'f_fsid': statbuf.f_fsid,
                'f_flag': statbuf.f_flag,
                'f_namemax': statbuf.f_namemax}

    def sync_fs(self):
        self.require_state("mounted")
        with nogil:
            ret = ceph_sync_fs(self.cluster)
        if ret < 0:
            raise make_ex(ret, "sync_fs failed")

    def fsync(self, int fd, int syncdataonly):
        self.require_state("mounted")
        with nogil:
            ret = ceph_fsync(self.cluster, fd, syncdataonly)
        if ret < 0:
            raise make_ex(ret, "fsync failed")

    def getcwd(self):
        self.require_state("mounted")
        with nogil:
            ret = ceph_getcwd(self.cluster)
        return ret

    def chdir(self, path):
        self.require_state("mounted")

        path = cstr(path, 'path')
        cdef char* _path = path
        with nogil:
            ret = ceph_chdir(self.cluster, _path)
        if ret < 0:
            raise make_ex(ret, "chdir failed")

    def opendir(self, path):
        self.require_state("mounted")

        path = cstr(path, 'path')
        cdef:
            char* _path = path
            ceph_dir_result *dir_handler
        with nogil:
            ret = ceph_opendir(self.cluster, _path, &dir_handler);
        if ret < 0:
            raise make_ex(ret, "opendir failed")
        d = DirResult()
        d.handler = dir_handler
        return d

    def readdir(self, DirResult dir_handler):
        self.require_state("mounted")

        cdef ceph_dir_result *_dir_handler = dir_handler.handler
        with nogil:
            dirent = ceph_readdir(self.cluster, _dir_handler)
        if not dirent:
            return None

        return DirEntry(d_ino=dirent.d_ino,
                        d_off=dirent.d_off,
                        d_reclen=dirent.d_reclen,
                        d_type=dirent.d_type,
                        d_name=dirent.d_name)

    def closedir(self, DirResult dir_handler):
        self.require_state("mounted")
        cdef:
            ceph_dir_result *_dir_handler = dir_handler.handler

        with nogil:
            ret = ceph_closedir(self.cluster, _dir_handler)
        if ret < 0:
            raise make_ex(ret, "closedir failed")

    def mkdir(self, path, mode):
        self.require_state("mounted")
        path = cstr(path, 'path')
        if not isinstance(mode, int):
            raise TypeError('mode must be an int')
        cdef:
            char* _path = path
            int _mode = mode
        with nogil:
            ret = ceph_mkdir(self.cluster, _path, _mode)
        if ret < 0:
            raise make_ex(ret, "error in mkdir '%s'" % path)

    def mkdirs(self, path, mode):
        self.require_state("mounted")
        path = cstr(path, 'path')
        if not isinstance(mode, int):
            raise TypeError('mode must be an int')
        cdef:
            char* _path = path
            int _mode = mode

        with nogil:
            ret = ceph_mkdirs(self.cluster, _path, _mode)
        if ret < 0:
            raise make_ex(ret, "error in mkdirs '%s'" % path)

    def rmdir(self, path):
        self.require_state("mounted")
        path = cstr(path, 'path')
        cdef char* _path = path
        ret = ceph_rmdir(self.cluster, _path)
        if ret < 0:
            raise make_ex(ret, "error in rmdir '%s'" % path)

    def open(self, path, flags, mode=0):
        self.require_state("mounted")
        path = cstr(path, 'path')

        if not isinstance(mode, int):
            raise TypeError('mode must be an int')
        if isinstance(flags, basestring):
            flags = cstr(flags, 'flags')
            cephfs_flags = 0
            if flags == '':
                cephfs_flags = os.O_RDONLY
            else:
                access_flags = 0;
                for c in flags:
                    if c == 'r':
                        access_flags = 1;
                    elif c == 'w':
                        access_flags = 2;
                        cephfs_flags |= os.O_TRUNC | os.O_CREAT
                    elif access_flags > 0 and c == '+':
                        access_flags = 3;
                    else:
                        raise make_ex(errno.EOPNOTSUPP,
                                      "open flags doesn't support %s" % c)

                if access_flags == 1:
                    cephfs_flags |= os.O_RDONLY;
                elif access_flags == 2:
                    cephfs_flags |= os.O_WRONLY;
                else:
                    cephfs_flags |= os.O_RDWR;

        elif isinstance(flags, int):
            cephfs_flags = flags
        else:
            raise TypeError("flags must be a string or an integer")

        cdef:
            char* _path = path
            int _flags = cephfs_flags
            int _mode = mode

        with nogil:
            ret = ceph_open(self.cluster, _path, _flags, _mode)
        if ret < 0:
            raise make_ex(ret, "error in open '%s'" % path)
        return ret

    def close(self, fd):
        self.require_state("mounted")
        if not isinstance(fd, int):
            raise TypeError('fd must be an int')
        cdef int _fd = fd
        with nogil:
            ret = ceph_close(self.cluster, _fd)
        if ret < 0:
            raise make_ex(ret, "error in close")

    def read(self, fd, offset, l):
        self.require_state("mounted")
        if not isinstance(offset, int):
            raise TypeError('offset must be an int')
        if not isinstance(l, int):
            raise TypeError('l must be an int')
        if not isinstance(fd, int):
            raise TypeError('fd must be an int')
        cdef:
            int _fd = fd
            int64_t _offset = offset
            int64_t _length = l

            char *ret_buf
            PyObject* ret_s = NULL

        ret_s = PyBytes_FromStringAndSize(NULL, _length)
        try:
            ret_buf = PyBytes_AsString(ret_s)
            with nogil:
                ret = ceph_read(self.cluster, _fd, ret_buf, _length, _offset)
            if ret < 0:
                raise make_ex(ret, "error in read")

            if ret != _length:
                _PyBytes_Resize(&ret_s, ret)

            return <object>ret_s
        finally:
            # We DECREF unconditionally: the cast to object above will have
            # INCREFed if necessary. This also takes care of exceptions,
            # including if _PyString_Resize fails (that will free the string
            # itself and set ret_s to NULL, hence XDECREF).
            ref.Py_XDECREF(ret_s)

    def write(self, fd, buf, offset):
        self.require_state("mounted")
        if not isinstance(fd, int):
            raise TypeError('fd must be an int')
        if not isinstance(buf, bytes):
            raise TypeError('buf must be a bytes')
        if not isinstance(offset, int):
            raise TypeError('offset must be an int')

        cdef:
            int _fd = fd
            char *_data = buf
            int64_t _offset = offset

            size_t length = len(buf)

        with nogil:
            ret = ceph_write(self.cluster, _fd, _data, length, _offset)
        if ret < 0:
            raise make_ex(ret, "error in write")
        return ret

    def flock(self, fd, operation, owner):
        self.require_state("mounted")
        if not isinstance(fd, int):
            raise TypeError('fd must be an int')
        if not isinstance(operation, int):
            raise TypeError('operation must be an int')

        cdef:
            int _fd = fd
            int _op = operation
            uint64_t _owner = owner

        with nogil:
            ret = ceph_flock(self.cluster, _fd, _op, _owner)
        if ret < 0:
            raise make_ex(ret, "error in write")
        return ret

    def getxattr(self, path, name, size=255):
        self.require_state("mounted")

        path = cstr(path, 'path')
        name = cstr(name, 'name')

        cdef:
            char* _path = path
            char* _name = name

            size_t ret_length = size
            char *ret_buf = NULL

        try:
            ret_buf = <char *>realloc_chk(ret_buf, ret_length)
            with nogil:
                ret = ceph_getxattr(self.cluster, _path, _name, ret_buf,
                                    ret_length)

            if ret < 0:
                raise make_ex(ret, "error in getxattr")

            return ret_buf[:ret]
        finally:
            free(ret_buf)

    def setxattr(self, path, name, value, flags):
        self.require_state("mounted")

        name = cstr(name, 'name')
        path = cstr(path, 'path')
        if not isinstance(flags, int):
            raise TypeError('flags must be a int')
        if not isinstance(value, bytes):
            raise TypeError('value must be a bytes')

        cdef:
            char *_path = path
            char *_name = name
            char *_value = value
            size_t _value_len = len(value)
            int _flags = flags

        with nogil:
            ret = ceph_setxattr(self.cluster, _path, _name,
                                _value, _value_len, _flags)
        if ret < 0:
            raise make_ex(ret, "error in setxattr")

    def stat(self, path):
        self.require_state("mounted")
        path = cstr(path, 'path')

        cdef:
            char* _path = path
            stat statbuf

        with nogil:
            ret = ceph_stat(self.cluster, _path, &statbuf)
        if ret < 0:
            raise make_ex(ret, "error in stat: %s" % path)
        return StatResult(st_dev=statbuf.st_dev, st_ino=statbuf.st_ino,
                          st_mode=statbuf.st_mode, st_nlink=statbuf.st_nlink,
                          st_uid=statbuf.st_uid, st_gid=statbuf.st_gid,
                          st_rdev=statbuf.st_rdev, st_size=statbuf.st_size,
                          st_blksize=statbuf.st_blksize,
                          st_blocks=statbuf.st_blocks,
                          st_atime=datetime.fromtimestamp(statbuf.st_atime),
                          st_mtime=datetime.fromtimestamp(statbuf.st_mtime),
                          st_ctime=datetime.fromtimestamp(statbuf.st_ctime))

    def fstat(self, fd):
        self.require_state("mounted")
        if not isinstance(fd, int):
            raise TypeError('fd must be an int')

        cdef:
            int _fd = fd
            stat statbuf

        with nogil:
            ret = ceph_fstat(self.cluster, _fd, &statbuf)
        if ret < 0:
            raise make_ex(ret, "error in fsat")
        return StatResult(st_dev=statbuf.st_dev, st_ino=statbuf.st_ino,
                          st_mode=statbuf.st_mode, st_nlink=statbuf.st_nlink,
                          st_uid=statbuf.st_uid, st_gid=statbuf.st_gid,
                          st_rdev=statbuf.st_rdev, st_size=statbuf.st_size,
                          st_blksize=statbuf.st_blksize,
                          st_blocks=statbuf.st_blocks,
                          st_atime=datetime.fromtimestamp(statbuf.st_atime),
                          st_mtime=datetime.fromtimestamp(statbuf.st_mtime),
                          st_ctime=datetime.fromtimestamp(statbuf.st_ctime))

    def symlink(self, existing, newname):
        self.require_state("mounted")
        existing = cstr(existing, 'existing')
        newname = cstr(newname, 'newname')
        cdef:
            char* _existing = existing
            char* _newname = newname

        with nogil:
            ret = ceph_symlink(self.cluster, _existing, _newname)
        if ret < 0:
            raise make_ex(ret, "error in symlink")

    def unlink(self, path):
        self.require_state("mounted")
        path = cstr(path, 'path')
        cdef char* _path = path
        with nogil:
            ret = ceph_unlink(self.cluster, _path)
        if ret < 0:
            raise make_ex(ret, "error in unlink: %s" % path)

    def rename(self, src, dst):
        self.require_state("mounted")

        src = cstr(src, 'source')
        dst = cstr(dst, 'destination')

        cdef:
            char* _src = src
            char* _dst = dst

        with nogil:
            ret = ceph_rename(self.cluster, _src, _dst)
        if ret < 0:
            raise make_ex(ret, "error in rename '%s' to '%s'" % (src, dst))

    def mds_command(self, mds_spec, args, input_data):
        """
        :return 3-tuple of output status int, output status string, output data
        """
        mds_spec = cstr(mds_spec, 'mds_spec')
        args = cstr_list(args, 'args')
        input_data = cstr(input_data, 'input_data')

        cdef:
            char *_mds_spec = opt_str(mds_spec)
            char **_cmd = to_bytes_array(args)
            size_t _cmdlen = len(args)

            char *_inbuf = input_data
            size_t _inbuf_len = len(input_data)

            char *_outbuf
            size_t _outbuf_len
            char *_outs
            size_t _outs_len

        try:
            with nogil:
                ret = ceph_mds_command(self.cluster, _mds_spec,
                                       <const char **>_cmd, _cmdlen,
                                       <const char*>_inbuf, _inbuf_len,
                                       &_outbuf, &_outbuf_len,
                                       &_outs, &_outs_len)
            if ret == 0:
                my_outs = decode_cstr(_outs[:_outs_len])
                my_outbuf = _outbuf[:_outbuf_len]
                if _outs_len:
                    ceph_buffer_free(_outs)
                if _outbuf_len:
                    ceph_buffer_free(_outbuf)
                return (ret, my_outbuf, my_outs)
            else:
                return (ret, b"", "")
        finally:
            free(_cmd)

