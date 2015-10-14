"""
This module is a thin wrapper around libcephfs.
"""
from ctypes import CDLL, c_char_p, c_size_t, c_void_p, c_int, c_long, c_uint, c_ulong, \
    c_ushort, create_string_buffer, byref, Structure, pointer, c_char, POINTER, \
    c_uint8, c_int64
from ctypes.util import find_library
from collections import namedtuple
import errno
import os


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


def make_ex(ret, msg):
    """
    Translate a libcephfs return code into an exception.

    :param ret: the return code
    :type ret: int
    :param msg: the error message to use
    :type msg: str
    :returns: a subclass of :class:`Error`
    """

    errors = {
        errno.EPERM     : PermissionError,
        errno.ENOENT    : ObjectNotFound,
        errno.EIO       : IOError,
        errno.ENOSPC    : NoSpace,
        errno.EEXIST    : ObjectExists,
        errno.ENODATA   : NoData,
        errno.EINVAL    : InvalidValue,
        errno.EOPNOTSUPP: OperationNotSupported,
        }
    ret = abs(ret)
    if ret in errors:
        return errors[ret](msg)
    else:
        return Error(msg + (": error code %d" % ret))


class cephfs_statvfs(Structure):
    _fields_ = [("f_bsize", c_ulong),
                ("f_frsize", c_ulong),
                ("f_blocks", c_ulong),
                ("f_bfree", c_ulong),
                ("f_bavail", c_ulong),
                ("f_files", c_ulong),
                ("f_ffree", c_ulong),
                ("f_favail", c_ulong),
                ("f_fsid", c_ulong),
                ("f_flag", c_ulong),
                ("f_namemax", c_ulong),
                ("f_padding", c_ulong*32)]


class cephfs_dirent(Structure):
    _fields_ = [("d_ino", c_long),
                ("d_off", c_ulong),
                ("d_reclen", c_ushort),
                ("d_type", c_uint8),
                ("d_name", c_char*256)]

# struct timespec {
#   long int tv_sec;
#   long int tv_nsec;
# }
class cephfs_timespec(Structure):
    _fields_ = [('tv_sec', c_long),
                ('tv_nsec', c_long)]


# struct stat {
#   unsigned long st_dev;
#   unsigned long st_ino;
#   unsigned long st_nlink;
#   unsigned int st_mode;
#   unsigned int st_uid;
#   unsigned int st_gid;
#   int __pad0;
#   unsigned long st_rdev;
#   long int st_size;
#   long int st_blksize;
#   long int st_blocks;
#   struct timespec st_atim;
#   struct timespec st_mtim;
#   struct timespec st_ctim;
#   long int __unused[3];
# };
class cephfs_stat(Structure):
    _fields_ = [('st_dev', c_ulong),            # ID of device containing file
                ('st_ino', c_ulong),            # inode number
                ('st_nlink', c_ulong),          # number of hard links
                ('st_mode', c_uint),            # protection
                ('st_uid', c_uint),             # user ID of owner
                ('st_gid', c_uint),             # group ID of owner
                ('__pad0', c_int),
                ('st_rdev', c_ulong),           # device ID (if special file)
                ('st_size', c_long),            # total size, in bytes
                ('st_blksize', c_long),         # blocksize for file system I/O
                ('st_blocks', c_long),          # num of 512B blocks allocated
                ('st_atime', cephfs_timespec),  # time of last access
                ('st_mtime', cephfs_timespec),  # time of last modification
                ('st_ctime', cephfs_timespec),  # time of last status change
                ('__unused1', c_long),
                ('__unused2', c_long),
                ('__unused3', c_long)]


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

def load_libcephfs():
    """
    Load the libcephfs shared library.
    """
    libcephfs_path = find_library('cephfs')
    if libcephfs_path:
        return CDLL(libcephfs_path)

    # try harder, find_library() doesn't search LD_LIBRARY_PATH
    # in addition, it doesn't seem work on centos 6.4 (see e46d2ca067b5)
    try:
        return CDLL('libcephfs.so.1')
    except OSError as e:
        raise EnvironmentError("Unable to load libcephfs: %s" % e)


class LibCephFS(object):
    """libcephfs python wrapper"""
    def require_state(self, *args):
        for a in args:
            if self.state == a:
                return
        raise LibCephFSStateError("You cannot perform that operation on a "
                                  "CephFS object in state %s." % (self.state))

    def __init__(self, conf=None, conffile=None, auth_id=None, rados_inst=None):
        self.libcephfs = load_libcephfs()
        self.cluster = c_void_p()

        self.state = "uninitialized"
        if rados_inst is not None:
            return self.create_with_rados(rados_inst)
        else:
            return self.create(conf, conffile, auth_id)

    def create_with_rados(self, rados_inst):
        ret = self.libcephfs.ceph_create_from_rados(
                byref(self.cluster),
                rados_inst.cluster)
        if ret != 0:
            raise Error("libcephfs_initialize failed with error code: %d" % ret)
        self.state = "configuring"

    def create(self, conf=None, conffile=None, auth_id=None, rados_inst=None):
        if conffile is not None and not isinstance(conffile, basestring):
            raise TypeError('conffile must be a string or None')

        if auth_id is not None and not isinstance(auth_id, basestring):
            raise TypeError('auth_id must be a string or None')

        ret = self.libcephfs.ceph_create(byref(self.cluster),
                c_char_p(auth_id) if auth_id else c_char_p(0))
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
        if conffile is not None and not isinstance(conffile, basestring):
            raise TypeError('conffile param must be a string')
        ret = self.libcephfs.ceph_conf_read_file(self.cluster, c_char_p(conffile))
        if ret != 0:
            raise make_ex(ret, "error calling conf_read_file")

    def conf_parse_argv(self, argv):
        self.require_state("configuring")
        c_argv = (c_char_p * len(argv))(*argv)
        ret = self.libcephfs.ceph_conf_parse_argv(self.cluster, len(argv),
                                                  c_argv)
        if ret != 0:
            raise make_ex(ret, "error calling conf_parse_argv")

    def shutdown(self):
        """
        Unmount and destroy the ceph mount handle.
        """
        if self.state != "shutdown":
            self.libcephfs.ceph_shutdown(self.cluster)
            self.state = "shutdown"

    def __enter__(self):
        self.mount()
        return self

    def __exit__(self, type_, value, traceback):
        self.shutdown()
        return False

    def __del__(self):
        self.shutdown()

    def version(self):
        """
        Get the version number of the ``libcephfs`` C library.

        :returns: a tuple of ``(major, minor, extra)`` components of the
                  libcephfs version
        """
        major = c_int(0)
        minor = c_int(0)
        extra = c_int(0)
        self.libcephfs.ceph_version(byref(major), byref(minor), byref(extra))
        return (major.value, minor.value, extra.value)

    def conf_get(self, option):
        self.require_state("configuring", "initialized", "mounted")
        if not isinstance(option, basestring):
            raise TypeError('option must be a string')
        length = 20
        while True:
            ret_buf = create_string_buffer(length)
            ret = self.libcephfs.ceph_conf_get(self.cluster, option,
                                               ret_buf, c_size_t(length))
            if ret == 0:
                return ret_buf.value
            elif ret == -errno.ENAMETOOLONG:
                length = length * 2
            elif ret == -errno.ENOENT:
                return None
            else:
                raise make_ex(ret, "error calling conf_get")

    def conf_set(self, option, val):
        self.require_state("configuring", "initialized", "mounted")
        if not isinstance(option, basestring):
            raise TypeError('option must be a string')
        if not isinstance(val, basestring):
            raise TypeError('val must be a string')
        ret = self.libcephfs.ceph_conf_set(self.cluster, c_char_p(option),
                                           c_char_p(val))
        if ret != 0:
            raise make_ex(ret, "error calling conf_set")

    def init(self):
        self.require_state("configuring")
        ret = self.libcephfs.ceph_init(self.cluster)
        if ret != 0:
            raise make_ex(ret, "error calling ceph_init")
        self.state = "initialized"

    def mount(self):
        if self.state == "configuring":
            self.init()
        self.require_state("initialized")
        ret = self.libcephfs.ceph_mount(self.cluster, "/")
        if ret != 0:
            raise make_ex(ret, "error calling ceph_mount")
        self.state = "mounted"

    def statfs(self, path):
        if not isinstance(path, basestring):
            raise TypeError('path must be a string')
        self.require_state("mounted")
        statbuf = cephfs_statvfs()
        ret = self.libcephfs.ceph_statfs(self.cluster, c_char_p(path), byref(statbuf))
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
        ret = self.libcephfs.ceph_sync_fs(self.cluster)
        if ret < 0:
            raise make_ex(ret, "sync_fs failed")

    def getcwd(self):
        self.require_state("mounted")
        self.libcephfs.ceph_getcwd.restype = c_char_p
        return self.libcephfs.ceph_getcwd(self.cluster)

    def chdir(self, path):
        self.require_state("mounted")
        ret = self.libcephfs.ceph_chdir(self.cluster, c_char_p(path))
        if ret < 0:
            raise make_ex(ret, "chdir failed")

    def opendir(self, path):
        self.require_state("mounted")
        if not isinstance(path, basestring):
            raise TypeError('path must be a string')
        dir_handler = c_void_p()
        ret = self.libcephfs.ceph_opendir(self.cluster, c_char_p(path),
                                          pointer(dir_handler));
        if ret < 0:
            raise make_ex(ret, "opendir failed")
        return dir_handler

    def readdir(self, dir_handler):
        self.require_state("mounted")
        self.libcephfs.ceph_readdir.restype = POINTER(cephfs_dirent)
        while True:
            dirent = self.libcephfs.ceph_readdir(self.cluster, dir_handler)
            if not dirent:
                return None

            return DirEntry(d_ino=dirent.contents.d_ino,
                            d_off=dirent.contents.d_off,
                            d_reclen=dirent.contents.d_reclen,
                            d_type=dirent.contents.d_type,
                            d_name=dirent.contents.d_name)

    def closedir(self, dir_handler):
        self.require_state("mounted")
        ret = self.libcephfs.ceph_closedir(self.cluster, dir_handler)
        if ret < 0:
            raise make_ex(ret, "closedir failed")

    def mkdir(self, path, mode):
        self.require_state("mounted")
        if not isinstance(path, basestring):
            raise TypeError('path must be a string')
        ret = self.libcephfs.ceph_mkdir(self.cluster, c_char_p(path), c_int(mode))
        if ret < 0:
            raise make_ex(ret, "error in mkdir '%s'" % path)

    def mkdirs(self, path, mode):
        self.require_state("mounted")
        if not isinstance(path, basestring):
            raise TypeError('path must be a string')
        if not isinstance(mode, basestring):
            raise TypeError('mode must be an int')
        ret = self.libcephfs.ceph_mkdir(self.cluster, c_char_p(path), c_int(mode))
        if ret < 0:
            raise make_ex(ret, "error in mkdirs '%s'" % path)

    def rmdir(self, path):
        self.require_state("mounted")
        if not isinstance(path, basestring):
            raise TypeError('path must be a string')
        ret = self.libcephfs.ceph_rmdir(self.cluster, c_char_p(path))
        if ret < 0:
            raise make_ex(ret, "error in rmdir '%s'" % path)

    def open(self, path, flags, mode=0):
        self.require_state("mounted")
        if not isinstance(path, basestring):
            raise TypeError('path must be a string')
        if not isinstance(flags, basestring):
            raise TypeError('flags must be a string')
        if not isinstance(mode, int):
            raise TypeError('mode must be an int')
        cephfs_flags = 0
        if flags == '':
            cephfs_flags = os.O_RDONLY
        else:
            for c in flags:
                if c == 'r':
                    cephfs_flags |= os.O_RDONLY
                elif c == 'w':
                    cephfs_flags |= os.O_WRONLY | os.O_TRUNC | os.O_CREAT
                elif c == '+':
                    cephfs_flags |= os.O_RDWR
                else:
                    raise OperationNotSupported(
                        "open flags doesn't support %s" % c)

        ret = self.libcephfs.ceph_open(self.cluster, c_char_p(path),
                                       c_int(cephfs_flags), c_int(mode))
        if ret < 0:
            raise make_ex(ret, "error in open '%s'" % path)
        return ret

    def close(self, fd):
        self.require_state("mounted")
        ret = self.libcephfs.ceph_close(self.cluster, c_int(fd))
        if ret < 0:
            raise make_ex(ret, "error in close")

    def read(self, fd, offset, l):
        self.require_state("mounted")
        if not isinstance(offset, int):
            raise TypeError('path must be an int')
        if not isinstance(l, int):
            raise TypeError('path must be an int')

        buf = create_string_buffer(l)
        ret = self.libcephfs.ceph_read(self.cluster, c_int(fd),
                                       buf, c_int64(l), c_int64(offset))
        if ret < 0:
            raise make_ex(ret, "error in close")
        return buf.value

    def write(self, fd, buf, offset):
        self.require_state("mounted")
        if not isinstance(buf, basestring):
            raise TypeError('buf must be a string')
        if not isinstance(offset, int):
            raise TypeError('offset must be an int')

        ret = self.libcephfs.ceph_write(self.cluster, c_int(fd),
                                        c_char_p(buf), c_int64(len(buf)),
                                        c_int64(offset))
        if ret < 0:
            raise make_ex(ret, "error in close")
        return ret

    def getxattr(self, path, name):
        if not isinstance(path, basestring):
            raise TypeError('path must be a string')
        if not isinstance(name, basestring):
            raise TypeError('name must be a string')

        self.require_state("mounted")
        l = 255
        buf = create_string_buffer(l)
        actual_l = self.libcephfs.ceph_getxattr(self.cluster, path, name, buf, c_int(l))
        if actual_l > l:
            buf = create_string_buffer(actual_)
            self.libcephfs.ceph_getxattr(path, name, new_buf, actual_l)
        return buf.value

    def setxattr(self, path, name, value, flags):
        if not isinstance(path, basestring):
            raise TypeError('path must be a string')
        if not isinstance(name, basestring):
            raise TypeError('name must be a string')
        if not isinstance(value, basestring):
            raise TypeError('value must be a string')
        self.require_state("mounted")
        ret = self.libcephfs.ceph_setxattr(self.cluster, c_char_p(path),
                                           c_char_p(name), c_char_p(value),
                                           c_size_t(len(value)), c_int(flags))
        if ret < 0:
            raise make_ex(ret, "error in setxattr")

    def stat(self, path):
        self.require_state("mounted")
        if not isinstance(path, basestring):
            raise TypeError('path must be a string')
        statbuf = cephfs_stat()
        ret = self.libcephfs.ceph_stat(self.cluster, c_char_p(path),
                                       byref(statbuf))
        if ret < 0:
            raise make_ex(ret, "error in stat: %s" % path)
        return StatResult(st_dev=statbuf.st_dev, st_ino=statbuf.st_ino,
                          st_mode=statbuf.st_mode, st_nlink=statbuf.st_nlink,
                          st_uid=statbuf.st_uid, st_gid=statbuf.st_gid,
                          st_rdev=statbuf.st_rdev, st_size=statbuf.st_size,
                          st_blksize=statbuf.st_blksize,
                          st_blocks=statbuf.st_blocks,
                          st_atime=statbuf.st_atime, st_mtime=statbuf.st_mtime,
                          st_ctime=statbuf.st_ctime)

    def unlink(self, path):
        self.require_state("mounted")
        ret = self.libcephfs.ceph_unlink(
            self.cluster,
            c_char_p(path))
        if ret < 0:
            raise make_ex(ret, "error in unlink: %s" % path)

    def rename(self, src, dst):
        self.require_state("mounted")
        if not isinstance(src, basestring) or not isinstance(dst, basestring):
            raise TypeError('source and destination must be a string')
        ret = self.libcephfs.ceph_rename(self.cluster, c_char_p(src), c_char_p(dst))
        if ret < 0:
            raise make_ex(ret, "error in rename '%s' to '%s'" % (src, dst))

    def mds_command(self, mds_spec, args, input_data):
        """
        :return 3-tuple of output status int, output status string, output data
        """

        cmdarr = (c_char_p * len(args))(*args)

        outbufp = pointer(pointer(c_char()))
        outbuflen = c_long()
        outsp = pointer(pointer(c_char()))
        outslen = c_long()

        ret = self.libcephfs.ceph_mds_command(self.cluster, c_char_p(mds_spec),
                                              cmdarr, len(args),
                                              c_char_p(input_data),
                                              len(input_data), outbufp,
                                              byref(outbuflen), outsp,
                                              byref(outslen))

        my_outbuf = outbufp.contents[:(outbuflen.value)]
        my_outs = outsp.contents[:(outslen.value)]
        if outbuflen.value:
            self.libcephfs.ceph_buffer_free(outbufp.contents)
        if outslen.value:
            self.libcephfs.ceph_buffer_free(outsp.contents)

        return (ret, my_outbuf, my_outs)
