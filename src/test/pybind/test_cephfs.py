# vim: expandtab smarttab shiftwidth=4 softtabstop=4
from nose.tools import assert_raises, assert_equal, assert_greater, with_setup
import cephfs as libcephfs
import fcntl
import os
import time
import stat
from datetime import datetime

cephfs = None

def setup_module():
    global cephfs
    cephfs = libcephfs.LibCephFS(conffile='')
    cephfs.mount()

def teardown_module():
    global cephfs
    cephfs.shutdown()

def setup_test():
    d = cephfs.opendir(b"/")
    dent = cephfs.readdir(d)
    while dent:
        if (dent.d_name not in [b".", b".."]):
            if dent.is_dir():
                cephfs.rmdir(b"/" + dent.d_name)
            else:
                cephfs.unlink(b"/" + dent.d_name)

        dent = cephfs.readdir(d)

    cephfs.closedir(d)

    cephfs.chdir(b"/")

@with_setup(setup_test)
def test_conf_get():
    fsid = cephfs.conf_get("fsid")
    assert(len(fsid) > 0)

@with_setup(setup_test)
def test_version():
    cephfs.version()

@with_setup(setup_test)
def test_fstat():
    fd = cephfs.open(b'file-1', 'w', 0o755)
    stat = cephfs.fstat(fd)
    assert(len(stat) == 13)
    cephfs.close(fd)

@with_setup(setup_test)
def test_statfs():
    stat = cephfs.statfs(b'/')
    assert(len(stat) == 11)

@with_setup(setup_test)
def test_statx():
    stat = cephfs.statx(b'/', libcephfs.CEPH_STATX_MODE, 0)
    assert('mode' in stat.keys())
    stat = cephfs.statx(b'/', libcephfs.CEPH_STATX_BTIME, 0)
    assert('btime' in stat.keys())
    
    fd = cephfs.open(b'file-1', 'w', 0o755)
    cephfs.write(fd, b"1111", 0)
    cephfs.close(fd)
    cephfs.symlink(b'file-1', b'file-2')
    stat = cephfs.statx(b'file-2', libcephfs.CEPH_STATX_MODE | libcephfs.CEPH_STATX_BTIME, libcephfs.AT_SYMLINK_NOFOLLOW)
    assert('mode' in stat.keys())
    assert('btime' in stat.keys())
    cephfs.unlink(b'file-2')
    cephfs.unlink(b'file-1')

@with_setup(setup_test)
def test_syncfs():
    stat = cephfs.sync_fs()

@with_setup(setup_test)
def test_fsync():
    fd = cephfs.open(b'file-1', 'w', 0o755)
    cephfs.write(fd, b"asdf", 0)
    stat = cephfs.fsync(fd, 0)
    cephfs.write(fd, b"qwer", 0)
    stat = cephfs.fsync(fd, 1)
    cephfs.close(fd)
    #sync on non-existing fd (assume fd 12345 is not exists)
    assert_raises(libcephfs.Error, cephfs.fsync, 12345, 0)

@with_setup(setup_test)
def test_directory():
    cephfs.mkdir(b"/temp-directory", 0o755)
    cephfs.mkdirs(b"/temp-directory/foo/bar", 0o755)
    cephfs.chdir(b"/temp-directory")
    assert_equal(cephfs.getcwd(), b"/temp-directory")
    cephfs.rmdir(b"/temp-directory/foo/bar")
    cephfs.rmdir(b"/temp-directory/foo")
    cephfs.rmdir(b"/temp-directory")
    assert_raises(libcephfs.ObjectNotFound, cephfs.chdir, b"/temp-directory")

@with_setup(setup_test)
def test_walk_dir():
    cephfs.chdir(b"/")
    dirs = [b"dir-1", b"dir-2", b"dir-3"]
    for i in dirs:
        cephfs.mkdir(i, 0o755)
    handler = cephfs.opendir(b"/")
    d = cephfs.readdir(handler)
    dirs += [b".", b".."]
    while d:
        assert(d.d_name in dirs)
        dirs.remove(d.d_name)
        d = cephfs.readdir(handler)
    assert(len(dirs) == 0)
    dirs = [b"/dir-1", b"/dir-2", b"/dir-3"]
    for i in dirs:
        cephfs.rmdir(i)
    cephfs.closedir(handler)

@with_setup(setup_test)
def test_xattr():
    assert_raises(libcephfs.OperationNotSupported, cephfs.setxattr, "/", "key", b"value", 0)
    cephfs.setxattr("/", "user.key", b"value", 0)
    assert_equal(b"value", cephfs.getxattr("/", "user.key"))

    cephfs.setxattr("/", "user.big", b"x" * 300, 0)

    # Default size is 255, get ERANGE
    assert_raises(libcephfs.OutOfRange, cephfs.getxattr, "/", "user.big")

    # Pass explicit size, and we'll get the value
    assert_equal(300, len(cephfs.getxattr("/", "user.big", 300)))

    cephfs.removexattr("/", "user.key")
    # user.key is already removed
    assert_raises(libcephfs.NoData, cephfs.getxattr, "/", "user.key")

    # user.big is only listed
    ret_val, ret_buff = cephfs.listxattr("/")
    assert_equal(9, ret_val)
    assert_equal("user.big\x00", ret_buff.decode('utf-8'))

@with_setup(setup_test)
def test_fxattr():
    fd = cephfs.open(b'/file-fxattr', 'w', 0o755)
    assert_raises(libcephfs.OperationNotSupported, cephfs.fsetxattr, fd, "key", b"value", 0)
    assert_raises(TypeError, cephfs.fsetxattr, "fd", "user.key", b"value", 0)
    assert_raises(TypeError, cephfs.fsetxattr, fd, "user.key", "value", 0)
    assert_raises(TypeError, cephfs.fsetxattr, fd, "user.key", b"value", "0")
    cephfs.fsetxattr(fd, "user.key", b"value", 0)
    assert_equal(b"value", cephfs.fgetxattr(fd, "user.key"))

    cephfs.fsetxattr(fd, "user.big", b"x" * 300, 0)

    # Default size is 255, get ERANGE
    assert_raises(libcephfs.OutOfRange, cephfs.fgetxattr, fd, "user.big")

    # Pass explicit size, and we'll get the value
    assert_equal(300, len(cephfs.fgetxattr(fd, "user.big", 300)))

    cephfs.fremovexattr(fd, "user.key")
    # user.key is already removed
    assert_raises(libcephfs.NoData, cephfs.fgetxattr, fd, "user.key")

    # user.big is only listed
    ret_val, ret_buff = cephfs.flistxattr(fd)
    assert_equal(9, ret_val)
    assert_equal("user.big\x00", ret_buff.decode('utf-8'))
    cephfs.close(fd)
    cephfs.unlink(b'/file-fxattr')

@with_setup(setup_test)
def test_rename():
    cephfs.mkdir(b"/a", 0o755)
    cephfs.mkdir(b"/a/b", 0o755)
    cephfs.rename(b"/a", b"/b")
    cephfs.stat(b"/b/b")
    cephfs.rmdir(b"/b/b")
    cephfs.rmdir(b"/b")

@with_setup(setup_test)
def test_open():
    assert_raises(libcephfs.ObjectNotFound, cephfs.open, b'file-1', 'r')
    assert_raises(libcephfs.ObjectNotFound, cephfs.open, b'file-1', 'r+')
    fd = cephfs.open(b'file-1', 'w', 0o755)
    cephfs.write(fd, b"asdf", 0)
    cephfs.close(fd)
    fd = cephfs.open(b'file-1', 'r', 0o755)
    assert_equal(cephfs.read(fd, 0, 4), b"asdf")
    cephfs.close(fd)
    fd = cephfs.open(b'file-1', 'r+', 0o755)
    cephfs.write(fd, b"zxcv", 4)
    assert_equal(cephfs.read(fd, 4, 8), b"zxcv")
    cephfs.close(fd)
    fd = cephfs.open(b'file-1', 'w+', 0o755)
    assert_equal(cephfs.read(fd, 0, 4), b"")
    cephfs.write(fd, b"zxcv", 4)
    assert_equal(cephfs.read(fd, 4, 8), b"zxcv")
    cephfs.close(fd)
    fd = cephfs.open(b'file-1', os.O_RDWR, 0o755)
    cephfs.write(fd, b"asdf", 0)
    assert_equal(cephfs.read(fd, 0, 4), b"asdf")
    cephfs.close(fd)
    assert_raises(libcephfs.OperationNotSupported, cephfs.open, b'file-1', 'a')
    cephfs.unlink(b'file-1')

@with_setup(setup_test)
def test_link():
    fd = cephfs.open(b'file-1', 'w', 0o755)
    cephfs.write(fd, b"1111", 0)
    cephfs.close(fd)
    cephfs.link(b'file-1', b'file-2')
    fd = cephfs.open(b'file-2', 'r', 0o755)
    assert_equal(cephfs.read(fd, 0, 4), b"1111")
    cephfs.close(fd)
    fd = cephfs.open(b'file-2', 'r+', 0o755)
    cephfs.write(fd, b"2222", 4)
    cephfs.close(fd)
    fd = cephfs.open(b'file-1', 'r', 0o755)
    assert_equal(cephfs.read(fd, 0, 8), b"11112222")
    cephfs.close(fd)
    cephfs.unlink(b'file-2')

@with_setup(setup_test)
def test_symlink():
    fd = cephfs.open(b'file-1', 'w', 0o755)
    cephfs.write(fd, b"1111", 0)
    cephfs.close(fd)
    cephfs.symlink(b'file-1', b'file-2')
    fd = cephfs.open(b'file-2', 'r', 0o755)
    assert_equal(cephfs.read(fd, 0, 4), b"1111")
    cephfs.close(fd)
    fd = cephfs.open(b'file-2', 'r+', 0o755)
    cephfs.write(fd, b"2222", 4)
    cephfs.close(fd)
    fd = cephfs.open(b'file-1', 'r', 0o755)
    assert_equal(cephfs.read(fd, 0, 8), b"11112222")
    cephfs.close(fd)
    cephfs.unlink(b'file-2')

@with_setup(setup_test)
def test_readlink():
    fd = cephfs.open(b'/file-1', 'w', 0o755)
    cephfs.write(fd, b"1111", 0)
    cephfs.close(fd)
    cephfs.symlink(b'/file-1', b'/file-2')
    d = cephfs.readlink(b"/file-2",100)
    assert_equal(d, b"/file-1")
    cephfs.unlink(b'/file-2')
    cephfs.unlink(b'/file-1')

@with_setup(setup_test)
def test_delete_cwd():
    assert_equal(b"/", cephfs.getcwd())

    cephfs.mkdir(b"/temp-directory", 0o755)
    cephfs.chdir(b"/temp-directory")
    cephfs.rmdir(b"/temp-directory")

    # getcwd gives you something stale here: it remembers the path string
    # even when things are unlinked.  It's up to the caller to find out
    # whether it really still exists
    assert_equal(b"/temp-directory", cephfs.getcwd())

@with_setup(setup_test)
def test_flock():
    fd = cephfs.open(b'file-1', 'w', 0o755)

    cephfs.flock(fd, fcntl.LOCK_EX, 123);
    fd2 = cephfs.open(b'file-1', 'w', 0o755)

    assert_raises(libcephfs.WouldBlock, cephfs.flock, fd2,
                  fcntl.LOCK_EX | fcntl.LOCK_NB, 456);
    cephfs.close(fd2)

    cephfs.close(fd)

@with_setup(setup_test)
def test_mount_unmount():
    test_directory()
    cephfs.unmount()
    cephfs.mount()
    test_open()

@with_setup(setup_test)
def test_mount_root():
    cephfs.mkdir(b"/mount-directory", 0o755)
    cephfs.unmount()
    cephfs.mount(mount_root = b"/mount-directory")

    assert_raises(libcephfs.Error, cephfs.mount, mount_root = b"/nowhere")
    cephfs.unmount()
    cephfs.mount()

@with_setup(setup_test)
def test_utime():
    fd = cephfs.open(b'/file-1', 'w', 0o755)
    cephfs.write(fd, b'0000', 0)
    cephfs.close(fd)

    stx_pre = cephfs.statx(b'/file-1', libcephfs.CEPH_STATX_ATIME | libcephfs.CEPH_STATX_MTIME, 0)

    time.sleep(1)
    cephfs.utime(b'/file-1')

    stx_post = cephfs.statx(b'/file-1', libcephfs.CEPH_STATX_ATIME | libcephfs.CEPH_STATX_MTIME, 0)

    assert_greater(stx_post['atime'], stx_pre['atime'])
    assert_greater(stx_post['mtime'], stx_pre['mtime'])

    atime_pre = int(time.mktime(stx_pre['atime'].timetuple()))
    mtime_pre = int(time.mktime(stx_pre['mtime'].timetuple()))

    cephfs.utime(b'/file-1', (atime_pre, mtime_pre))
    stx_post = cephfs.statx(b'/file-1', libcephfs.CEPH_STATX_ATIME | libcephfs.CEPH_STATX_MTIME, 0)

    assert_equal(stx_post['atime'], stx_pre['atime'])
    assert_equal(stx_post['mtime'], stx_pre['mtime'])

    cephfs.unlink(b'/file-1')

@with_setup(setup_test)
def test_futime():
    fd = cephfs.open(b'/file-1', 'w', 0o755)
    cephfs.write(fd, b'0000', 0)

    stx_pre = cephfs.statx(b'/file-1', libcephfs.CEPH_STATX_ATIME | libcephfs.CEPH_STATX_MTIME, 0)

    time.sleep(1)
    cephfs.futime(fd)

    stx_post = cephfs.statx(b'/file-1', libcephfs.CEPH_STATX_ATIME | libcephfs.CEPH_STATX_MTIME, 0)

    assert_greater(stx_post['atime'], stx_pre['atime'])
    assert_greater(stx_post['mtime'], stx_pre['mtime'])

    atime_pre = int(time.mktime(stx_pre['atime'].timetuple()))
    mtime_pre = int(time.mktime(stx_pre['mtime'].timetuple()))

    cephfs.futime(fd, (atime_pre, mtime_pre))
    stx_post = cephfs.statx(b'/file-1', libcephfs.CEPH_STATX_ATIME | libcephfs.CEPH_STATX_MTIME, 0)

    assert_equal(stx_post['atime'], stx_pre['atime'])
    assert_equal(stx_post['mtime'], stx_pre['mtime'])

    cephfs.close(fd)
    cephfs.unlink(b'/file-1')

@with_setup(setup_test)
def test_utimes():
    fd = cephfs.open(b'/file-1', 'w', 0o755)
    cephfs.write(fd, b'0000', 0)
    cephfs.close(fd)

    stx_pre = cephfs.statx(b'/file-1', libcephfs.CEPH_STATX_ATIME | libcephfs.CEPH_STATX_MTIME, 0)

    time.sleep(1)
    cephfs.utimes(b'/file-1')

    stx_post = cephfs.statx(b'/file-1', libcephfs.CEPH_STATX_ATIME | libcephfs.CEPH_STATX_MTIME, 0)

    assert_greater(stx_post['atime'], stx_pre['atime'])
    assert_greater(stx_post['mtime'], stx_pre['mtime'])

    atime_pre = time.mktime(stx_pre['atime'].timetuple())
    mtime_pre = time.mktime(stx_pre['mtime'].timetuple())

    cephfs.utimes(b'/file-1', (atime_pre, mtime_pre))
    stx_post = cephfs.statx(b'/file-1', libcephfs.CEPH_STATX_ATIME | libcephfs.CEPH_STATX_MTIME, 0)

    assert_equal(stx_post['atime'], stx_pre['atime'])
    assert_equal(stx_post['mtime'], stx_pre['mtime'])

    cephfs.unlink(b'/file-1')

@with_setup(setup_test)
def test_lutimes():
    fd = cephfs.open(b'/file-1', 'w', 0o755)
    cephfs.write(fd, b'0000', 0)
    cephfs.close(fd)

    cephfs.symlink(b'/file-1', b'/file-2')

    stx_pre_t = cephfs.statx(b'/file-1', libcephfs.CEPH_STATX_ATIME | libcephfs.CEPH_STATX_MTIME, 0)
    stx_pre_s = cephfs.statx(b'/file-2', libcephfs.CEPH_STATX_ATIME | libcephfs.CEPH_STATX_MTIME, libcephfs.AT_SYMLINK_NOFOLLOW)

    time.sleep(1)
    cephfs.lutimes(b'/file-2')

    stx_post_t = cephfs.statx(b'/file-1', libcephfs.CEPH_STATX_ATIME | libcephfs.CEPH_STATX_MTIME, 0)
    stx_post_s = cephfs.statx(b'/file-2', libcephfs.CEPH_STATX_ATIME | libcephfs.CEPH_STATX_MTIME, libcephfs.AT_SYMLINK_NOFOLLOW)

    assert_equal(stx_post_t['atime'], stx_pre_t['atime'])
    assert_equal(stx_post_t['mtime'], stx_pre_t['mtime'])

    assert_greater(stx_post_s['atime'], stx_pre_s['atime'])
    assert_greater(stx_post_s['mtime'], stx_pre_s['mtime'])

    atime_pre = time.mktime(stx_pre_s['atime'].timetuple())
    mtime_pre = time.mktime(stx_pre_s['mtime'].timetuple())

    cephfs.lutimes(b'/file-2', (atime_pre, mtime_pre))
    stx_post_s = cephfs.statx(b'/file-2', libcephfs.CEPH_STATX_ATIME | libcephfs.CEPH_STATX_MTIME, libcephfs.AT_SYMLINK_NOFOLLOW)

    assert_equal(stx_post_s['atime'], stx_pre_s['atime'])
    assert_equal(stx_post_s['mtime'], stx_pre_s['mtime'])

    cephfs.unlink(b'/file-2')
    cephfs.unlink(b'/file-1')

@with_setup(setup_test)
def test_futimes():
    fd = cephfs.open(b'/file-1', 'w', 0o755)
    cephfs.write(fd, b'0000', 0)

    stx_pre = cephfs.statx(b'/file-1', libcephfs.CEPH_STATX_ATIME | libcephfs.CEPH_STATX_MTIME, 0)

    time.sleep(1)
    cephfs.futimes(fd)

    stx_post = cephfs.statx(b'/file-1', libcephfs.CEPH_STATX_ATIME | libcephfs.CEPH_STATX_MTIME, 0)

    assert_greater(stx_post['atime'], stx_pre['atime'])
    assert_greater(stx_post['mtime'], stx_pre['mtime'])

    atime_pre = time.mktime(stx_pre['atime'].timetuple())
    mtime_pre = time.mktime(stx_pre['mtime'].timetuple())

    cephfs.futimes(fd, (atime_pre, mtime_pre))
    stx_post = cephfs.statx(b'/file-1', libcephfs.CEPH_STATX_ATIME | libcephfs.CEPH_STATX_MTIME, 0)

    assert_equal(stx_post['atime'], stx_pre['atime'])
    assert_equal(stx_post['mtime'], stx_pre['mtime'])

    cephfs.close(fd)
    cephfs.unlink(b'/file-1')

@with_setup(setup_test)
def test_futimens():
    fd = cephfs.open(b'/file-1', 'w', 0o755)
    cephfs.write(fd, b'0000', 0)

    stx_pre = cephfs.statx(b'/file-1', libcephfs.CEPH_STATX_ATIME | libcephfs.CEPH_STATX_MTIME, 0)

    time.sleep(1)
    cephfs.futimens(fd)

    stx_post = cephfs.statx(b'/file-1', libcephfs.CEPH_STATX_ATIME | libcephfs.CEPH_STATX_MTIME, 0)

    assert_greater(stx_post['atime'], stx_pre['atime'])
    assert_greater(stx_post['mtime'], stx_pre['mtime'])

    atime_pre = time.mktime(stx_pre['atime'].timetuple())
    mtime_pre = time.mktime(stx_pre['mtime'].timetuple())

    cephfs.futimens(fd, (atime_pre, mtime_pre))
    stx_post = cephfs.statx(b'/file-1', libcephfs.CEPH_STATX_ATIME | libcephfs.CEPH_STATX_MTIME, 0)

    assert_equal(stx_post['atime'], stx_pre['atime'])
    assert_equal(stx_post['mtime'], stx_pre['mtime'])

    cephfs.close(fd)
    cephfs.unlink(b'/file-1')

@with_setup(setup_test)
def test_fchmod():
    fd = cephfs.open(b'/file-fchmod', 'w', 0o655)
    st = cephfs.statx(b'/file-fchmod', libcephfs.CEPH_STATX_MODE, 0)
    mode = st["mode"] | stat.S_IXUSR
    cephfs.fchmod(fd, mode)
    st = cephfs.statx(b'/file-fchmod', libcephfs.CEPH_STATX_MODE, 0)
    assert_equal(st["mode"] & stat.S_IRWXU, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    assert_raises(TypeError, cephfs.fchmod, "/file-fchmod", stat.S_IXUSR)
    assert_raises(TypeError, cephfs.fchmod, fd, "stat.S_IXUSR")
    cephfs.close(fd)
    cephfs.unlink(b'/file-fchmod')

@with_setup(setup_test)
def test_fchown():
    fd = cephfs.open(b'/file-fchown', 'w', 0o655)
    uid = os.getuid()
    gid = os.getgid()
    assert_raises(TypeError, cephfs.fchown, b'/file-fchown', uid, gid)
    assert_raises(TypeError, cephfs.fchown, fd, "uid", "gid")
    cephfs.fchown(fd, uid, gid)
    st = cephfs.statx(b'/file-fchown', libcephfs.CEPH_STATX_UID | libcephfs.CEPH_STATX_GID, 0)
    assert_equal(st["uid"], uid)
    assert_equal(st["gid"], gid)
    cephfs.fchown(fd, 9999, 9999)
    st = cephfs.statx(b'/file-fchown', libcephfs.CEPH_STATX_UID | libcephfs.CEPH_STATX_GID, 0)
    assert_equal(st["uid"], 9999)
    assert_equal(st["gid"], 9999)
    cephfs.close(fd)
    cephfs.unlink(b'/file-fchown')
