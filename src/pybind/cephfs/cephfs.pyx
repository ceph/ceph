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
import time

# Are we running Python 2.x
if sys.version_info[0] < 3:
    str_type = basestring
else:
    str_type = str

AT_NO_ATTR_SYNC = 0x4000
AT_SYMLINK_NOFOLLOW = 0x100
cdef int AT_SYMLINK_NOFOLLOW_CDEF = AT_SYMLINK_NOFOLLOW
CEPH_STATX_BASIC_STATS = 0x7ff
cdef int CEPH_STATX_BASIC_STATS_CDEF = CEPH_STATX_BASIC_STATS
CEPH_STATX_MODE = 0x1
CEPH_STATX_NLINK = 0x2
CEPH_STATX_UID = 0x4
CEPH_STATX_GID = 0x8
CEPH_STATX_RDEV = 0x10
CEPH_STATX_ATIME = 0x20
CEPH_STATX_MTIME = 0x40
CEPH_STATX_CTIME = 0x80
CEPH_STATX_INO = 0x100
CEPH_STATX_SIZE = 0x200
CEPH_STATX_BLOCKS = 0x400
CEPH_STATX_BTIME = 0x800
CEPH_STATX_VERSION = 0x1000

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


IF UNAME_SYSNAME == "FreeBSD" or UNAME_SYSNAME == "Darwin":
    cdef extern from "dirent.h":
        cdef struct dirent:
            long int d_ino
            unsigned short int d_reclen
            unsigned char d_type
            char d_name[256]
ELSE:
    cdef extern from "dirent.h":
        cdef struct dirent:
            long int d_ino
            unsigned long int d_off
            unsigned short int d_reclen
            unsigned char d_type
            char d_name[256]


cdef extern from "time.h":
    ctypedef long int time_t

cdef extern from "time.h":
    cdef struct timespec:
        time_t      tv_sec
        long int    tv_nsec

cdef extern from "sys/types.h":
    ctypedef unsigned long mode_t

cdef extern from "<utime.h>":
    cdef struct utimbuf:
        time_t actime
        time_t modtime

cdef extern from "sys/time.h":
    cdef struct timeval:
        long tv_sec
        long tv_usec

cdef extern from "cephfs/ceph_ll_client.h":
    cdef struct statx "ceph_statx":
        uint32_t    stx_mask
        uint32_t    stx_blksize
        uint32_t    stx_nlink
        uint32_t    stx_uid
        uint32_t    stx_gid
        uint16_t    stx_mode
        uint64_t    stx_ino
        uint64_t    stx_size
        uint64_t    stx_blocks
        uint64_t    stx_dev
        uint64_t    stx_rdev
        timespec    stx_atime
        timespec    stx_ctime
        timespec    stx_mtime
        timespec    stx_btime
        uint64_t    stx_version

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

    int ceph_getaddrs(ceph_mount_info* cmount, char** addrs)
    int ceph_conf_read_file(ceph_mount_info *cmount, const char *path_list)
    int ceph_conf_parse_argv(ceph_mount_info *cmount, int argc, const char **argv)
    int ceph_conf_get(ceph_mount_info *cmount, const char *option, char *buf, size_t len)
    int ceph_conf_set(ceph_mount_info *cmount, const char *option, const char *value)

    int ceph_mount(ceph_mount_info *cmount, const char *root)
    int ceph_select_filesystem(ceph_mount_info *cmount, const char *fs_name)
    int ceph_unmount(ceph_mount_info *cmount)
    int ceph_abort_conn(ceph_mount_info *cmount)
    uint64_t ceph_get_instance_id(ceph_mount_info *cmount)
    int ceph_fstatx(ceph_mount_info *cmount, int fd, statx *stx, unsigned want, unsigned flags)
    int ceph_statx(ceph_mount_info *cmount, const char *path, statx *stx, unsigned want, unsigned flags)
    int ceph_statfs(ceph_mount_info *cmount, const char *path, statvfs *stbuf)

    int ceph_mds_command(ceph_mount_info *cmount, const char *mds_spec, const char **cmd, size_t cmdlen,
                         const char *inbuf, size_t inbuflen, char **outbuf, size_t *outbuflen,
                         char **outs, size_t *outslen)
    int ceph_rename(ceph_mount_info *cmount, const char *from_, const char *to)
    int ceph_link(ceph_mount_info *cmount, const char *existing, const char *newname)
    int ceph_unlink(ceph_mount_info *cmount, const char *path)
    int ceph_symlink(ceph_mount_info *cmount, const char *existing, const char *newname)
    int ceph_readlink(ceph_mount_info *cmount, const char *path, char *buf, int64_t size)
    int ceph_setxattr(ceph_mount_info *cmount, const char *path, const char *name,
                      const void *value, size_t size, int flags)
    int ceph_fsetxattr(ceph_mount_info *cmount, int fd, const char *name,
                       const void *value, size_t size, int flags)
    int ceph_lsetxattr(ceph_mount_info *cmount, const char *path, const char *name,
                       const void *value, size_t size, int flags)
    int ceph_getxattr(ceph_mount_info *cmount, const char *path, const char *name,
                      void *value, size_t size)
    int ceph_fgetxattr(ceph_mount_info *cmount, int fd, const char *name,
                       void *value, size_t size)
    int ceph_lgetxattr(ceph_mount_info *cmount, const char *path, const char *name,
                       void *value, size_t size)
    int ceph_removexattr(ceph_mount_info *cmount, const char *path, const char *name)
    int ceph_fremovexattr(ceph_mount_info *cmount, int fd, const char *name)
    int ceph_lremovexattr(ceph_mount_info *cmount, const char *path, const char *name)
    int ceph_listxattr(ceph_mount_info *cmount, const char *path, char *list, size_t size)
    int ceph_flistxattr(ceph_mount_info *cmount, int fd, char *list, size_t size)
    int ceph_llistxattr(ceph_mount_info *cmount, const char *path, char *list, size_t size)
    int ceph_write(ceph_mount_info *cmount, int fd, const char *buf, int64_t size, int64_t offset)
    int ceph_read(ceph_mount_info *cmount, int fd, char *buf, int64_t size, int64_t offset)
    int ceph_flock(ceph_mount_info *cmount, int fd, int operation, uint64_t owner)
    int ceph_close(ceph_mount_info *cmount, int fd)
    int ceph_open(ceph_mount_info *cmount, const char *path, int flags, mode_t mode)
    int ceph_mkdir(ceph_mount_info *cmount, const char *path, mode_t mode)
    int ceph_mkdirs(ceph_mount_info *cmount, const char *path, mode_t mode)
    int ceph_closedir(ceph_mount_info *cmount, ceph_dir_result *dirp)
    int ceph_opendir(ceph_mount_info *cmount, const char *name, ceph_dir_result **dirpp)
    void ceph_rewinddir(ceph_mount_info *cmount, ceph_dir_result *dirp)
    int ceph_chdir(ceph_mount_info *cmount, const char *path)
    dirent * ceph_readdir(ceph_mount_info *cmount, ceph_dir_result *dirp)
    int ceph_rmdir(ceph_mount_info *cmount, const char *path)
    const char* ceph_getcwd(ceph_mount_info *cmount)
    int ceph_sync_fs(ceph_mount_info *cmount)
    int ceph_fsync(ceph_mount_info *cmount, int fd, int syncdataonly)
    int ceph_conf_parse_argv(ceph_mount_info *cmount, int argc, const char **argv)
    int ceph_chmod(ceph_mount_info *cmount, const char *path, mode_t mode)
    int ceph_fchmod(ceph_mount_info *cmount, int fd, mode_t mode)
    int ceph_chown(ceph_mount_info *cmount, const char *path, int uid, int gid)
    int ceph_lchown(ceph_mount_info *cmount, const char *path, int uid, int gid)
    int ceph_fchown(ceph_mount_info *cmount, int fd, int uid, int gid)
    int64_t ceph_lseek(ceph_mount_info *cmount, int fd, int64_t offset, int whence)
    void ceph_buffer_free(char *buf)
    mode_t ceph_umask(ceph_mount_info *cmount, mode_t mode)
    int ceph_utime(ceph_mount_info *cmount, const char *path, utimbuf *buf)
    int ceph_futime(ceph_mount_info *cmount, int fd, utimbuf *buf)
    int ceph_utimes(ceph_mount_info *cmount, const char *path, timeval times[2])
    int ceph_lutimes(ceph_mount_info *cmount, const char *path, timeval times[2])
    int ceph_futimes(ceph_mount_info *cmount, int fd, timeval times[2])
    int ceph_futimens(ceph_mount_info *cmount, int fd, timespec times[2])


class Error(Exception):
    def get_error_code(self):
        return 1


class LibCephFSStateError(Error):
    pass


class OSError(Error):
    def __init__(self, errno, strerror):
        super(OSError, self).__init__(errno, strerror)
        self.errno = errno
        self.strerror = "%s: %s" % (strerror, os.strerror(errno))

    def __str__(self):
        return '{} [Errno {}]'.format(self.strerror, self.errno)

    def get_error_code(self):
        return self.errno


class PermissionError(OSError):
    pass


class ObjectNotFound(OSError):
    pass


class NoData(OSError):
    pass


class ObjectExists(OSError):
    pass


class IOError(OSError):
    pass


class NoSpace(OSError):
    pass


class InvalidValue(OSError):
    pass


class OperationNotSupported(OSError):
    pass


class WouldBlock(OSError):
    pass


class OutOfRange(OSError):
    pass


class ObjectNotEmpty(OSError):
    pass

class NotDirectory(OSError):
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
        errno.ENOTEMPTY  : ObjectNotEmpty,
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
        errno.ENOTEMPTY  : ObjectNotEmpty,
        errno.ENOTDIR    : NotDirectory
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
        return errno_to_exception[ret](ret, msg)
    else:
        return Error(msg + ': {} [Errno {:d}]'.format(os.strerror(ret), ret))


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
    cdef LibCephFS lib
    cdef ceph_dir_result* handle

# Bug in older Cython instances prevents this from being a static method.
#    @staticmethod
#    cdef create(LibCephFS lib, ceph_dir_result* handle):
#        d = DirResult()
#        d.lib = lib
#        d.handle = handle
#        return d

    def __dealloc__(self):
        self.close()

    def __enter__(self):
        if not self.handle:
            raise make_ex(errno.EBADF, "dir is not open")
        self.lib.require_state("mounted")
        with nogil:
            ceph_rewinddir(self.lib.cluster, self.handle)
        return self

    def __exit__(self, type_, value, traceback):
        self.close()
        return False

    def readdir(self):
        self.lib.require_state("mounted")

        with nogil:
            dirent = ceph_readdir(self.lib.cluster, self.handle)
        if not dirent:
            return None

        IF UNAME_SYSNAME == "FreeBSD" or UNAME_SYSNAME == "Darwin":
            return DirEntry(d_ino=dirent.d_ino,
                            d_off=0,
                            d_reclen=dirent.d_reclen,
                            d_type=dirent.d_type,
                            d_name=dirent.d_name)
        ELSE:
            return DirEntry(d_ino=dirent.d_ino,
                            d_off=dirent.d_off,
                            d_reclen=dirent.d_reclen,
                            d_type=dirent.d_type,
                            d_name=dirent.d_name)

    def close(self):
        if self.handle:
            self.lib.require_state("mounted")
            with nogil:
                ret = ceph_closedir(self.lib.cluster, self.handle)
            if ret < 0:
                raise make_ex(ret, "closedir failed")
            self.handle = NULL

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
    else:
        try:
            v = val.encode(encoding)
        except:
            raise TypeError('%s must be encodeable as a bytearray' % name)
        assert isinstance(v, bytes)
        return v

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

cdef timeval to_timeval(t):
    """
    return timeval equivalent from time
    """
    tt = int(t)
    cdef timeval buf = timeval(tt, (t - tt) * 1000000)
    return buf

cdef timespec to_timespec(t):
    """
    return timespec equivalent from time
    """
    tt = int(t)
    cdef timespec buf = timespec(tt, (t - tt) * 1000000000)
    return buf

cdef char* opt_str(s) except? NULL:
    if s is None:
        return NULL
    return s


cdef char ** to_bytes_array(list_bytes):
    cdef char **ret = <char **>malloc(len(list_bytes) * sizeof(char *))
    if ret == NULL:
        raise MemoryError("malloc failed")
    for i in range(len(list_bytes)):
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
        """Create a libcephfs wrapper

        :param conf dict opt: settings overriding the default ones and conffile
        :param conffile str opt: the path to ceph.conf to override the default settings
        :auth_id str opt: the id used to authenticate the client entity
        :rados_inst Rados opt: a rados.Rados instance
        """
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
        """
        Create a mount handle for interacting with Ceph.  All libcephfs
        functions operate on a mount info handle.
        
        :param conf dict opt: settings overriding the default ones and conffile
        :param conffile str opt: the path to ceph.conf to override the default settings
        :auth_id str opt: the id used to authenticate the client entity
        """
        if conf is not None and not isinstance(conf, dict):
            raise TypeError("conf must be dict or None")
        cstr(conffile, 'configfile', opt=True)
        auth_id = cstr(auth_id, 'auth_id', opt=True)

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

    def get_addrs(self):
        """
        Get associated client addresses with this RADOS session.
        """
        self.require_state("mounted")

        cdef:
            char* addrs = NULL

        try:

            with nogil:
                ret = ceph_getaddrs(self.cluster, &addrs)
            if ret:
                raise make_ex(ret, "error calling getaddrs")

            return decode_cstr(addrs)
        finally:
            ceph_buffer_free(addrs)


    def conf_read_file(self, conffile=None):
        """
        Load the ceph configuration from the specified config file.
         
        :param conffile str opt: the path to ceph.conf to override the default settings
        """
        conffile = cstr(conffile, 'conffile', opt=True)
        cdef:
            char *_conffile = opt_str(conffile)
        with nogil:
            ret = ceph_conf_read_file(self.cluster, <const char*>_conffile)
        if ret != 0:
            raise make_ex(ret, "error calling conf_read_file")

    def conf_parse_argv(self, argv):
        """
        Parse the command line arguments and load the configuration parameters.
        
        :param argv: the argument list
        """
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
        """
        Gets the configuration value as a string.
        
        :param option: the config option to get
        """
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

    def conf_set(self, option, val):
        """
        Sets a configuration value from a string.
        
        :param option: the configuration option to set
        :param value: the value of the configuration option to set
        """
        self.require_state("configuring", "initialized", "mounted")

        option = cstr(option, 'option')
        val = cstr(val, 'val')
        cdef:
            char *_option = option
            char *_val = val

        with nogil:
            ret = ceph_conf_set(self.cluster, _option, _val)
        if ret != 0:
            raise make_ex(ret, "error calling conf_set")

    def init(self):
        """
        Initialize the filesystem client (but do not mount the filesystem yet)
        """
        self.require_state("configuring")
        with nogil:
            ret = ceph_init(self.cluster)
        if ret != 0:
            raise make_ex(ret, "error calling ceph_init")
        self.state = "initialized"

    def mount(self, mount_root=None, filesystem_name=None):
        """
        Perform a mount using the path for the root of the mount.
        """
        if self.state == "configuring":
            self.init()
        self.require_state("initialized")

        # Configure which filesystem to mount if one was specified
        if filesystem_name is None:
            filesystem_name = b""
        else:
            filesystem_name = cstr(filesystem_name, 'filesystem_name')
        cdef:
            char *_filesystem_name = filesystem_name
        if filesystem_name:
            with nogil:
                ret = ceph_select_filesystem(self.cluster,
                        _filesystem_name)
            if ret != 0:
                raise make_ex(ret, "error calling ceph_select_filesystem")

        # Prepare mount_root argument, default to "/"
        root = b"/" if mount_root is None else mount_root
        cdef:
            char *_mount_root = root

        with nogil:
            ret = ceph_mount(self.cluster, _mount_root)
        if ret != 0:
            raise make_ex(ret, "error calling ceph_mount")
        self.state = "mounted"

    def unmount(self):
        """
        Unmount a mount handle.
        """
        self.require_state("mounted")
        with nogil:
            ret = ceph_unmount(self.cluster)
        if ret != 0:
            raise make_ex(ret, "error calling ceph_unmount")
        self.state = "initialized"

    def abort_conn(self):
        """
        Abort mds connections.
        """
        self.require_state("mounted")
        with nogil:
            ret = ceph_abort_conn(self.cluster)
        if ret != 0:
            raise make_ex(ret, "error calling ceph_abort_conn")
        self.state = "initialized"

    def get_instance_id(self):
        """
        Get a global id for current instance
        """
        self.require_state("initialized", "mounted")
        with nogil:
            ret = ceph_get_instance_id(self.cluster)
        return ret;

    def statfs(self, path):
        """
        Perform a statfs on the ceph file system.  This call fills in file system wide statistics
        into the passed in buffer.
        
        :param path: any path within the mounted filesystem
        """
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
        """
        Synchronize all filesystem data to persistent media
        """
        self.require_state("mounted")
        with nogil:
            ret = ceph_sync_fs(self.cluster)
        if ret < 0:
            raise make_ex(ret, "sync_fs failed")

    def fsync(self, int fd, int syncdataonly):
        """
        Synchronize an open file to persistent media.
        
        :param fd: the file descriptor of the file to sync.
        :param syncdataonly: a boolean whether to synchronize metadata and data (0)
                             or just data (1).
        """
        self.require_state("mounted")
        with nogil:
            ret = ceph_fsync(self.cluster, fd, syncdataonly)
        if ret < 0:
            raise make_ex(ret, "fsync failed")

    def getcwd(self):
        """
        Get the current working directory.
        
        :rtype the path to the current working directory
        """
        self.require_state("mounted")
        with nogil:
            ret = ceph_getcwd(self.cluster)
        return ret

    def chdir(self, path):
        """
        Change the current working directory.
        
        :param path the path to the working directory to change into.
        """
        self.require_state("mounted")

        path = cstr(path, 'path')
        cdef char* _path = path
        with nogil:
            ret = ceph_chdir(self.cluster, _path)
        if ret < 0:
            raise make_ex(ret, "chdir failed")

    def opendir(self, path):
        """
        Open the given directory.
        
        :param path: the path name of the directory to open.  Must be either an absolute path
                     or a path relative to the current working directory.
        :rtype handle: the open directory stream handle
        """
        self.require_state("mounted")

        path = cstr(path, 'path')
        cdef:
            char* _path = path
            ceph_dir_result* handle
        with nogil:
            ret = ceph_opendir(self.cluster, _path, &handle);
        if ret < 0:
            raise make_ex(ret, "opendir failed")
        d = DirResult()
        d.lib = self
        d.handle = handle
        return d

    def readdir(self, DirResult handle):
        """
        Get the next entry in an open directory.
        
        :param handle: the open directory stream handle
        :rtype dentry: the next directory entry or None if at the end of the
                       directory (or the directory is empty.  This pointer
                       should not be freed by the caller, and is only safe to
                       access between return and the next call to readdir or
                       closedir.
        """
        self.require_state("mounted")

        return handle.readdir()

    def closedir(self, DirResult handle):
        """
        Close the open directory.
        
        :param handle: the open directory stream handle
        """
        self.require_state("mounted")

        return handle.close()

    def mkdir(self, path, mode):
        """
        Create a directory.
 
        :param path: the path of the directory to create.  This must be either an
                     absolute path or a relative path off of the current working directory.
        :param mode: the permissions the directory should have once created.
        """

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
            raise make_ex(ret, "error in mkdir {}".format(path.decode('utf-8')))

    def chmod(self, path, mode) :
        """
        Change directory mode.

        :param path: the path of the directory to create.  This must be either an
                     absolute path or a relative path off of the current working directory.
        :param mode: the permissions the directory should have once created.
        """
        self.require_state("mounted")
        path = cstr(path, 'path')
        if not isinstance(mode, int):
            raise TypeError('mode must be an int')
        cdef:
            char* _path = path
            int _mode = mode
        with nogil:
            ret = ceph_chmod(self.cluster, _path, _mode)
        if ret < 0:
            raise make_ex(ret, "error in chmod {}".format(path.decode('utf-8')))

    def fchmod(self, fd, mode) :
        """
        Change file mode based on fd.
        :param fd: the file descriptor of the file to change file mode
        :param mode: the permissions to be set.
        """
        self.require_state("mounted")
        if not isinstance(fd, int):
            raise TypeError('fd must be an int')
        if not isinstance(mode, int):
            raise TypeError('mode must be an int')
        cdef:
            int _fd = fd
            int _mode = mode
        with nogil:
            ret = ceph_fchmod(self.cluster, _fd, _mode)
        if ret < 0:
            raise make_ex(ret, "error in fchmod")

    def chown(self, path, uid, gid, follow_symlink=True):
        """
        Change directory ownership

        :param path: the path of the directory to change.
        :param uid: the uid to set
        :param gid: the gid to set
        :param follow_symlink: perform the operation on the target file if @path
                               is a symbolic link (default)
        """
        self.require_state("mounted")
        path = cstr(path, 'path')
        if not isinstance(uid, int):
            raise TypeError('uid must be an int')
        elif not isinstance(gid, int):
            raise TypeError('gid must be an int')

        cdef:
            char* _path = path
            int _uid = uid
            int _gid = gid
        if follow_symlink:
            with nogil:
                ret = ceph_chown(self.cluster, _path, _uid, _gid)
        else:
            with nogil:
                ret = ceph_lchown(self.cluster, _path, _uid, _gid)
        if ret < 0:
            raise make_ex(ret, "error in chown {}".format(path.decode('utf-8')))

    def lchown(self, path, uid, gid):
        """
        Change ownership of a symbolic link
        :param path: the path of the symbolic link to change.
        :param uid: the uid to set
        :param gid: the gid to set
        """
        self.chown(path, uid, gid, follow_symlink=False)

    def fchown(self, fd, uid, gid):
        """
        Change file ownership
        :param fd: the file descriptor of the file to change ownership
        :param uid: the uid to set
        :param gid: the gid to set
        """
        self.require_state("mounted")
        if not isinstance(fd, int):
            raise TypeError('fd must be an int')
        if not isinstance(uid, int):
            raise TypeError('uid must be an int')
        elif not isinstance(gid, int):
            raise TypeError('gid must be an int')

        cdef:
            int _fd = fd
            int _uid = uid
            int _gid = gid
        with nogil:
            ret = ceph_fchown(self.cluster, _fd, _uid, _gid)
        if ret < 0:
            raise make_ex(ret, "error in fchown")

    def mkdirs(self, path, mode):
        """
        Create multiple directories at once.

        :param path: the full path of directories and sub-directories that should
                     be created.
        :param mode: the permissions the directory should have once created
        """
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
            raise make_ex(ret, "error in mkdirs {}".format(path.decode('utf-8')))

    def rmdir(self, path):
        """
        Remove a directory.
         
        :param path: the path of the directory to remove.
        """
        self.require_state("mounted")
        path = cstr(path, 'path')
        cdef char* _path = path
        ret = ceph_rmdir(self.cluster, _path)
        if ret < 0:
            raise make_ex(ret, "error in rmdir {}".format(path.decode('utf-8')))

    def open(self, path, flags, mode=0):
        """
        Create and/or open a file.
         
        :param path: the path of the file to open.  If the flags parameter includes O_CREAT,
                     the file will first be created before opening.
        :param flags: set of option masks that control how the file is created/opened.
        :param mode: the permissions to place on the file if the file does not exist and O_CREAT
                     is specified in the flags.
        """
        self.require_state("mounted")
        path = cstr(path, 'path')

        if not isinstance(mode, int):
            raise TypeError('mode must be an int')
        if isinstance(flags, str):
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
            raise make_ex(ret, "error in open {}".format(path.decode('utf-8')))
        return ret

    def close(self, fd):
        """
        Close the open file.
        
        :param fd: the file descriptor referring to the open file.
        """

        self.require_state("mounted")
        if not isinstance(fd, int):
            raise TypeError('fd must be an int')
        cdef int _fd = fd
        with nogil:
            ret = ceph_close(self.cluster, _fd)
        if ret < 0:
            raise make_ex(ret, "error in close")

    def read(self, fd, offset, l):
        """
        Read data from the file.
 
        :param fd: the file descriptor of the open file to read from.
        :param offset: the offset in the file to read from.  If this value is negative, the
                       function reads from the current offset of the file descriptor.
        :param l: the flag to indicate what type of seeking to perform
        """     
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
        """
        Write data to a file.
       
        :param fd: the file descriptor of the open file to write to
        :param buf: the bytes to write to the file
        :param offset: the offset of the file write into.  If this value is negative, the
                       function writes to the current offset of the file descriptor.
        """
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
        """
        Apply or remove an advisory lock.
        
        :param fd: the open file descriptor to change advisory lock.
        :param operation: the advisory lock operation to be performed on the file
        :param owner: the user-supplied owner identifier (an arbitrary integer)
        """
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

    def getxattr(self, path, name, size=255, follow_symlink=True):
        """
         Get an extended attribute.
         
         :param path: the path to the file
         :param name: the name of the extended attribute to get
         :param size: the size of the pre-allocated buffer
        """ 
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
            if follow_symlink:
                with nogil:
                    ret = ceph_getxattr(self.cluster, _path, _name, ret_buf,
                                        ret_length)
            else:
                with nogil:
                    ret = ceph_lgetxattr(self.cluster, _path, _name, ret_buf,
                                         ret_length)

            if ret < 0:
                raise make_ex(ret, "error in getxattr")

            return ret_buf[:ret]
        finally:
            free(ret_buf)

    def fgetxattr(self, fd, name, size=255):
        """
         Get an extended attribute given the fd of a file.

         :param fd: the open file descriptor referring to the file
         :param name: the name of the extended attribute to get
         :param size: the size of the pre-allocated buffer
        """
        self.require_state("mounted")

        if not isinstance(fd, int):
            raise TypeError('fd must be an int')
        name = cstr(name, 'name')

        cdef:
            int _fd = fd
            char* _name = name

            size_t ret_length = size
            char *ret_buf = NULL

        try:
            ret_buf = <char *>realloc_chk(ret_buf, ret_length)
            with nogil:
                ret = ceph_fgetxattr(self.cluster, _fd, _name, ret_buf,
                                    ret_length)

            if ret < 0:
                raise make_ex(ret, "error in fgetxattr")

            return ret_buf[:ret]
        finally:
            free(ret_buf)

    def lgetxattr(self, path, name, size=255):
        """
         Get an extended attribute without following symbolic links. This
         function is identical to ceph_getxattr, but if the path refers to
         a symbolic link, we get the extended attributes of the symlink
         rather than the attributes of the file it points to.

         :param path: the path to the file
         :param name: the name of the extended attribute to get
         :param size: the size of the pre-allocated buffer
        """

        return self.getxattr(path, name, size=size, follow_symlink=False)

    def setxattr(self, path, name, value, flags, follow_symlink=True):
        """
        Set an extended attribute on a file.

       :param path: the path to the file.
       :param name: the name of the extended attribute to set.
       :param value: the bytes of the extended attribute value
       """
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

        if follow_symlink:
            with nogil:
                ret = ceph_setxattr(self.cluster, _path, _name,
                                    _value, _value_len, _flags)
        else:
            with nogil:
                ret = ceph_lsetxattr(self.cluster, _path, _name,
                                    _value, _value_len, _flags)

        if ret < 0:
            raise make_ex(ret, "error in setxattr")

    def fsetxattr(self, fd, name, value, flags):
        """
        Set an extended attribute on a file.

        :param fd: the open file descriptor referring to the file.
        :param name: the name of the extended attribute to set.
        :param value: the bytes of the extended attribute value
        """
        self.require_state("mounted")

        name = cstr(name, 'name')
        if not isinstance(fd, int):
            raise TypeError('fd must be an int')
        if not isinstance(flags, int):
            raise TypeError('flags must be a int')
        if not isinstance(value, bytes):
            raise TypeError('value must be a bytes')

        cdef:
            int _fd = fd
            char *_name = name
            char *_value = value
            size_t _value_len = len(value)
            int _flags = flags

        with nogil:
            ret = ceph_fsetxattr(self.cluster, _fd, _name,
                                 _value, _value_len, _flags)
        if ret < 0:
            raise make_ex(ret, "error in fsetxattr")

    def lsetxattr(self, path, name, value, flags):
        """
        Set an extended attribute on a file but do not follow symbolic link.

        :param path: the path to the file.
        :param name: the name of the extended attribute to set.
        :param value: the bytes of the extended attribute value
        """

        self.setxattr(path, name, value, flags, follow_symlink=False)

    def removexattr(self, path, name, follow_symlink=True):
        """
        Remove an extended attribute of a file.

        :param path: path of the file.
        :param name: name of the extended attribute to remove.
        """
        self.require_state("mounted")

        name = cstr(name, 'name')
        path = cstr(path, 'path')

        cdef:
            char *_path = path
            char *_name = name

        if follow_symlink:
            with nogil:
                ret = ceph_removexattr(self.cluster, _path, _name)
        else:
            with nogil:
                ret = ceph_lremovexattr(self.cluster, _path, _name)

        if ret < 0:
            raise make_ex(ret, "error in removexattr")

    def fremovexattr(self, fd, name):
        """
        Remove an extended attribute of a file.

        :param fd: the open file descriptor referring to the file.
        :param name: name of the extended attribute to remove.
        """
        self.require_state("mounted")

        if not isinstance(fd, int):
            raise TypeError('fd must be an int')
        name = cstr(name, 'name')

        cdef:
            int _fd = fd
            char *_name = name

        with nogil:
            ret = ceph_fremovexattr(self.cluster, _fd, _name)
        if ret < 0:
            raise make_ex(ret, "error in fremovexattr")

    def lremovexattr(self, path, name):
        """
        Remove an extended attribute of a file but do not follow symbolic link.

        :param path: path of the file.
        :param name: name of the extended attribute to remove.
        """
        self.removexattr(path, name, follow_symlink=False)

    def listxattr(self, path, size=65536, follow_symlink=True):
        """
        List the extended attribute keys set on a file.

        :param path: path of the file.
        :param size: the size of list buffer to be filled with extended attribute keys.
        """
        self.require_state("mounted")

        path = cstr(path, 'path')

        cdef:
            char *_path = path
            char *ret_buf = NULL
            size_t ret_length = size

        try:
            ret_buf = <char *>realloc_chk(ret_buf, ret_length)
            if follow_symlink:
                with nogil:
                    ret = ceph_listxattr(self.cluster, _path, ret_buf, ret_length)
            else:
                with nogil:
                    ret = ceph_llistxattr(self.cluster, _path, ret_buf, ret_length)

            if ret < 0:
                raise make_ex(ret, "error in listxattr")

            return ret, ret_buf[:ret]
        finally:
            free(ret_buf)

    def flistxattr(self, fd, size=65536):
        """
        List the extended attribute keys set on a file.

        :param fd: the open file descriptor referring to the file.
        :param size: the size of list buffer to be filled with extended attribute keys.
        """
        self.require_state("mounted")

        if not isinstance(fd, int):
            raise TypeError('fd must be an int')

        cdef:
            int _fd = fd
            char *ret_buf = NULL
            size_t ret_length = size

        try:
            ret_buf = <char *>realloc_chk(ret_buf, ret_length)
            with nogil:
                ret = ceph_flistxattr(self.cluster, _fd, ret_buf, ret_length)

            if ret < 0:
                raise make_ex(ret, "error in flistxattr")

            return ret, ret_buf[:ret]
        finally:
            free(ret_buf)

    def llistxattr(self, path, size=65536):
        """
        List the extended attribute keys set on a file but do not follow symbolic link.

        :param path: path of the file.
        :param size: the size of list buffer to be filled with extended attribute keys.
        """

        return self.listxattr(path, size=size, follow_symlink=False)

    def stat(self, path, follow_symlink=True):
        """
        Get a file's extended statistics and attributes.
        
        :param path: the file or directory to get the statistics of.
        """
        self.require_state("mounted")
        path = cstr(path, 'path')

        cdef:
            char* _path = path
            statx stx

        if follow_symlink:
            with nogil:
                ret = ceph_statx(self.cluster, _path, &stx,
                                 CEPH_STATX_BASIC_STATS_CDEF, 0)
        else:
            with nogil:
                ret = ceph_statx(self.cluster, _path, &stx,
                                 CEPH_STATX_BASIC_STATS_CDEF, AT_SYMLINK_NOFOLLOW_CDEF)

        if ret < 0:
            raise make_ex(ret, "error in stat: {}".format(path.decode('utf-8')))
        return StatResult(st_dev=stx.stx_dev, st_ino=stx.stx_ino,
                          st_mode=stx.stx_mode, st_nlink=stx.stx_nlink,
                          st_uid=stx.stx_uid, st_gid=stx.stx_gid,
                          st_rdev=stx.stx_rdev, st_size=stx.stx_size,
                          st_blksize=stx.stx_blksize,
                          st_blocks=stx.stx_blocks,
                          st_atime=datetime.fromtimestamp(stx.stx_atime.tv_sec),
                          st_mtime=datetime.fromtimestamp(stx.stx_mtime.tv_sec),
                          st_ctime=datetime.fromtimestamp(stx.stx_ctime.tv_sec))

    def lstat(self, path):
        """
        Get a file's extended statistics and attributes. When file's a
        symbolic link, return the informaion of the link itself rather
        than that of the file it points too.

        :param path: the file or directory to get the statistics of.
        """
        return self.stat(path, follow_symlink=False)

    def fstat(self, fd):
        """
        Get an open file's extended statistics and attributes.

        :param fd: the file descriptor of the file to get statistics of.
         """
        self.require_state("mounted")
        if not isinstance(fd, int):
            raise TypeError('fd must be an int')

        cdef:
            int _fd = fd
            statx stx

        with nogil:
            ret = ceph_fstatx(self.cluster, _fd, &stx,
                              CEPH_STATX_BASIC_STATS_CDEF, 0)
        if ret < 0:
            raise make_ex(ret, "error in fsat")
        return StatResult(st_dev=stx.stx_dev, st_ino=stx.stx_ino,
                          st_mode=stx.stx_mode, st_nlink=stx.stx_nlink,
                          st_uid=stx.stx_uid, st_gid=stx.stx_gid,
                          st_rdev=stx.stx_rdev, st_size=stx.stx_size,
                          st_blksize=stx.stx_blksize,
                          st_blocks=stx.stx_blocks,
                          st_atime=datetime.fromtimestamp(stx.stx_atime.tv_sec),
                          st_mtime=datetime.fromtimestamp(stx.stx_mtime.tv_sec),
                          st_ctime=datetime.fromtimestamp(stx.stx_ctime.tv_sec))
    
    def statx(self, path, mask, flag):
        """
        Get a file's extended statistics and attributes.

        :param path: the file or directory to get the statistics of.
        :param mask: want bitfield of CEPH_STATX_* flags showing designed attributes.
        :param flag: bitfield that can be used to set AT_* modifier flags (only AT_NO_ATTR_SYNC and AT_SYMLINK_NOFOLLOW)
        """

        self.require_state("mounted")
        path = cstr(path, 'path')
        if not isinstance(mask, int):
            raise TypeError('flag must be a int')
        if not isinstance(flag, int):
            raise TypeError('flag must be a int')

        cdef:
            char* _path = path
            statx stx
            int _mask = mask
            int _flag = flag
            dict_result = dict()

        with nogil:
            ret = ceph_statx(self.cluster, _path, &stx, _mask, _flag)
        if ret < 0:
            raise make_ex(ret, "error in stat: %s" % path)

        if (_mask & CEPH_STATX_MODE):
            dict_result["mode"] = stx.stx_mode
        if (_mask & CEPH_STATX_NLINK):
            dict_result["nlink"] = stx.stx_nlink
        if (_mask & CEPH_STATX_UID):
            dict_result["uid"] = stx.stx_uid
        if (_mask & CEPH_STATX_GID):
            dict_result["gid"] = stx.stx_gid
        if (_mask & CEPH_STATX_RDEV):
            dict_result["rdev"] = stx.stx_rdev
        if (_mask & CEPH_STATX_ATIME):
            dict_result["atime"] = datetime.fromtimestamp(stx.stx_atime.tv_sec)
        if (_mask & CEPH_STATX_MTIME):
            dict_result["mtime"] = datetime.fromtimestamp(stx.stx_mtime.tv_sec)
        if (_mask & CEPH_STATX_CTIME):
            dict_result["ctime"] = datetime.fromtimestamp(stx.stx_ctime.tv_sec)
        if (_mask & CEPH_STATX_INO):
            dict_result["ino"] = stx.stx_ino
        if (_mask & CEPH_STATX_SIZE):
            dict_result["size"] = stx.stx_size
        if (_mask & CEPH_STATX_BLOCKS):
            dict_result["blocks"] = stx.stx_blocks
        if (_mask & CEPH_STATX_BTIME):
            dict_result["btime"] = datetime.fromtimestamp(stx.stx_btime.tv_sec)
        if (_mask & CEPH_STATX_VERSION):
            dict_result["version"] = stx.stx_version

        return dict_result

    def symlink(self, existing, newname):
        """
        Creates a symbolic link.
       
        :param existing: the path to the existing file/directory to link to.
        :param newname: the path to the new file/directory to link from.
        """
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
    
    def link(self, existing, newname):
        """
        Create a link.
        
        :param existing: the path to the existing file/directory to link to.
        :param newname: the path to the new file/directory to link from.
        """

        self.require_state("mounted")
        existing = cstr(existing, 'existing')
        newname = cstr(newname, 'newname')
        cdef:
            char* _existing = existing
            char* _newname = newname
        
        with nogil:
            ret = ceph_link(self.cluster, _existing, _newname)
        if ret < 0:
            raise make_ex(ret, "error in link")    
    
    def readlink(self, path, size):
        """
        Read a symbolic link.
      
        :param path: the path to the symlink to read
        :param size: the length of the buffer
        :rtype buf: buffer to hold the path of the file that the symlink points to.
        """
        self.require_state("mounted")
        path = cstr(path, 'path')

        cdef:
            char* _path = path
            int64_t _size = size
            char *buf = NULL

        try:
            buf = <char *>realloc_chk(buf, _size)
            with nogil:
                ret = ceph_readlink(self.cluster, _path, buf, _size)
            if ret < 0:
                raise make_ex(ret, "error in readlink")
            return buf
        finally:
            free(buf)

    def unlink(self, path):
        """
        Removes a file, link, or symbolic link.  If the file/link has multiple links to it, the
        file will not disappear from the namespace until all references to it are removed.
        
        :param path: the path of the file or link to unlink.
        """
        self.require_state("mounted")
        path = cstr(path, 'path')
        cdef char* _path = path
        with nogil:
            ret = ceph_unlink(self.cluster, _path)
        if ret < 0:
            raise make_ex(ret, "error in unlink: {}".format(path.decode('utf-8')))

    def rename(self, src, dst):
        """
        Rename a file or directory.
        
        :param src: the path to the existing file or directory.
        :param dst: the new name of the file or directory.
        """
        
        self.require_state("mounted")

        src = cstr(src, 'source')
        dst = cstr(dst, 'destination')

        cdef:
            char* _src = src
            char* _dst = dst

        with nogil:
            ret = ceph_rename(self.cluster, _src, _dst)
        if ret < 0:
            raise make_ex(ret, "error in rename {} to {}".format(src.decode(
                          'utf-8'), dst.decode('utf-8')))

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

            char *_outbuf = NULL
            size_t _outbuf_len = 0
            char *_outs = NULL
            size_t _outs_len = 0

        try:
            with nogil:
                ret = ceph_mds_command(self.cluster, _mds_spec,
                                       <const char **>_cmd, _cmdlen,
                                       <const char*>_inbuf, _inbuf_len,
                                       &_outbuf, &_outbuf_len,
                                       &_outs, &_outs_len)
            my_outs = decode_cstr(_outs[:_outs_len])
            my_outbuf = _outbuf[:_outbuf_len]
            if _outs_len:
                ceph_buffer_free(_outs)
            if _outbuf_len:
                ceph_buffer_free(_outbuf)
            return (ret, my_outbuf, my_outs)
        finally:
            free(_cmd)

    def umask(self, mode) :
        self.require_state("mounted")
        cdef:
            mode_t _mode = mode
        with nogil:
            ret = ceph_umask(self.cluster, _mode)
        if ret < 0:
            raise make_ex(ret, "error in umask")
        return ret

    def lseek(self, fd, offset, whence):
        """
        Set the file's current position.
 
        :param fd: the file descriptor of the open file to read from.
        :param offset: the offset in the file to read from.  If this value is negative, the
                       function reads from the current offset of the file descriptor.
        :param whence: the flag to indicate what type of seeking to performs:SEEK_SET, SEEK_CUR, SEEK_END
        """     
        self.require_state("mounted")
        if not isinstance(fd, int):
            raise TypeError('fd must be an int')
        if not isinstance(offset, int):
            raise TypeError('offset must be an int')
        if not isinstance(whence, int):
            raise TypeError('whence must be an int')

        cdef:
            int _fd = fd
            int64_t _offset = offset
            int64_t _whence = whence

        with nogil:
            ret = ceph_lseek(self.cluster, _fd, _offset, _whence)

        if ret < 0:
            raise make_ex(ret, "error in lseek")

        return ret      

    def utime(self, path, times=None):
        """
        Set access and modification time for path

        :param path: file path for which timestamps have to be changed
        :param times: if times is not None, it must be a tuple (atime, mtime)
        """

        self.require_state("mounted")
        path = cstr(path, 'path')
        if times:
            if not isinstance(times, tuple):
                raise TypeError('times must be a tuple')
            if not isinstance(times[0], int):
                raise TypeError('atime must be an int')
            if not isinstance(times[1], int):
                raise TypeError('mtime must be an int')
        actime = modtime = int(time.time())
        if times:
            actime = times[0]
            modtime = times[1]

        cdef:
            char *pth = path
            utimbuf buf = utimbuf(actime, modtime)
        with nogil:
            ret = ceph_utime(self.cluster, pth, &buf)
        if ret < 0:
            raise make_ex(ret, "error in utime {}".format(path.decode('utf-8')))

    def futime(self, fd, times=None):
        """
        Set access and modification time for a file pointed by descriptor

        :param fd: file descriptor of the open file
        :param times: if times is not None, it must be a tuple (atime, mtime)
        """

        self.require_state("mounted")
        if not isinstance(fd, int):
            raise TypeError('fd must be an int')
        if times:
            if not isinstance(times, tuple):
                raise TypeError('times must be a tuple')
            if not isinstance(times[0], int):
                raise TypeError('atime must be an int')
            if not isinstance(times[1], int):
                raise TypeError('mtime must be an int')
        actime = modtime = int(time.time())
        if times:
            actime = times[0]
            modtime = times[1]

        cdef:
            int _fd = fd
            utimbuf buf = utimbuf(actime, modtime)
        with nogil:
            ret = ceph_futime(self.cluster, _fd, &buf)
        if ret < 0:
            raise make_ex(ret, "error in futime")

    def utimes(self, path, times=None, follow_symlink=True):
        """
        Set access and modification time for path

        :param path: file path for which timestamps have to be changed
        :param times: if times is not None, it must be a tuple (atime, mtime)
        :param follow_symlink: perform the operation on the target file if @path
                               is a symbolic link (default)
        """

        self.require_state("mounted")
        path = cstr(path, 'path')
        if times:
            if not isinstance(times, tuple):
                raise TypeError('times must be a tuple')
            if not isinstance(times[0], (int, float)):
                raise TypeError('atime must be an int or a float')
            if not isinstance(times[1], (int, float)):
                raise TypeError('mtime must be an int or a float')
        actime = modtime = time.time()
        if times:
            actime = float(times[0])
            modtime = float(times[1])

        cdef:
            char *pth = path
            timeval *buf = [to_timeval(actime), to_timeval(modtime)]
        if follow_symlink:
            with nogil:
                ret = ceph_utimes(self.cluster, pth, buf)
        else:
            with nogil:
                ret = ceph_lutimes(self.cluster, pth, buf)
        if ret < 0:
            raise make_ex(ret, "error in utimes {}".format(path.decode('utf-8')))

    def lutimes(self, path, times=None):
        """
        Set access and modification time for a file. If the file is a symbolic
        link do not follow to the target.

        :param path: file path for which timestamps have to be changed
        :param times: if times is not None, it must be a tuple (atime, mtime)
        """
        self.utimes(path, times=times, follow_symlink=False)

    def futimes(self, fd, times=None):
        """
        Set access and modification time for a file pointer by descriptor

        :param fd: file descriptor of the open file
        :param times: if times is not None, it must be a tuple (atime, mtime)
        """

        self.require_state("mounted")
        if not isinstance(fd, int):
            raise TypeError('fd must be an int')
        if times:
            if not isinstance(times, tuple):
                raise TypeError('times must be a tuple')
            if not isinstance(times[0], (int, float)):
                raise TypeError('atime must be an int or a float')
            if not isinstance(times[1], (int, float)):
                raise TypeError('mtime must be an int or a float')
        actime = modtime = time.time()
        if times:
            actime = float(times[0])
            modtime = float(times[1])

        cdef:
            int _fd = fd
            timeval *buf = [to_timeval(actime), to_timeval(modtime)]
        with nogil:
                ret = ceph_futimes(self.cluster, _fd, buf)
        if ret < 0:
            raise make_ex(ret, "error in futimes")

    def futimens(self, fd, times=None):
        """
        Set access and modification time for a file pointer by descriptor

        :param fd: file descriptor of the open file
        :param times: if times is not None, it must be a tuple (atime, mtime)
        """

        self.require_state("mounted")
        if not isinstance(fd, int):
            raise TypeError('fd must be an int')
        if times:
            if not isinstance(times, tuple):
                raise TypeError('times must be a tuple')
            if not isinstance(times[0], (int, float)):
                raise TypeError('atime must be an int or a float')
            if not isinstance(times[1], (int, float)):
                raise TypeError('mtime must be an int or a float')
        actime = modtime = time.time()
        if times:
            actime = float(times[0])
            modtime = float(times[1])

        cdef:
            int _fd = fd
            timespec *buf = [to_timespec(actime), to_timespec(modtime)]
        with nogil:
                ret = ceph_futimens(self.cluster, _fd, buf)
        if ret < 0:
            raise make_ex(ret, "error in futimens")
