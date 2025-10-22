# vim: expandtab shiftwidth=4 softtabstop=4
import collections
collections.Callable = collections.abc.Callable
from assertions import assert_raises, assert_equal, assert_not_equal, assert_greater
import cephfs as libcephfs
import fcntl
import os
import pytest
import random
import time
import stat
import uuid
import json
from datetime import datetime
from subprocess import getoutput as get_cmd_output


cephfs = None


def setup_module():
    global cephfs
    cephfs = libcephfs.LibCephFS(conffile='')

    username = get_cmd_output('id -un')
    uid = get_cmd_output(f'id -u {username}')
    gid = get_cmd_output(f'id -g {username}')
    cephfs.conf_set('client_mount_uid', uid)
    cephfs.conf_set('client_mount_gid', gid)
    cephfs.mount()

    cephfs.conf_set('client_permissions' , 'false')
    cephfs.chown('/', int(uid), int(gid))
    cephfs.conf_set('client_permissions' , 'true')

def teardown_module():
    global cephfs
    cephfs.shutdown()

def purge_dir(path, is_snap = False):
    print(b"Purge " + path)
    d = cephfs.opendir(path)
    if (not path.endswith(b"/")):
        path = path + b"/"
    dent = cephfs.readdir(d)
    while dent:
        if (dent.d_name not in [b".", b".."]):
            print(path + dent.d_name)
            if dent.is_dir():
                if (not is_snap):
                    try:
                        snappath = path + dent.d_name + b"/.snap"
                        cephfs.stat(snappath)
                        purge_dir(snappath, True)
                    except:
                        pass
                    purge_dir(path + dent.d_name, False)
                    cephfs.rmdir(path + dent.d_name)
                else:
                    print("rmsnap on {} snap {}".format(path, dent.d_name))
                    cephfs.rmsnap(path, dent.d_name);
            else:
                cephfs.unlink(path + dent.d_name)
        dent = cephfs.readdir(d)
    cephfs.closedir(d)

@pytest.fixture
def testdir():
    purge_dir(b"/")

    cephfs.chdir(b"/")
    _, ret_buf = cephfs.listxattr("/")
    print(f'ret_buf={ret_buf}')
    xattrs = ret_buf.decode('utf-8').split('\x00')
    for xattr in xattrs[:-1]:
        cephfs.removexattr("/", xattr)

def test_conf_get(testdir):
    fsid = cephfs.conf_get("fsid")
    assert(len(fsid) > 0)

def test_version():
    cephfs.version()

def test_fstat(testdir):
    fd = cephfs.open(b'file-1', 'w', 0o755)
    stat = cephfs.fstat(fd)
    assert(len(stat) == 13)
    cephfs.close(fd)

def test_statfs(testdir):
    stat = cephfs.statfs(b'/')
    assert(len(stat) == 11)

def test_statx(testdir):
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

def test_syncfs(testdir):
    stat = cephfs.sync_fs()

def test_fsync(testdir):
    fd = cephfs.open(b'file-1', 'w', 0o755)
    cephfs.write(fd, b"asdf", 0)
    stat = cephfs.fsync(fd, 0)
    cephfs.write(fd, b"qwer", 0)
    stat = cephfs.fsync(fd, 1)
    cephfs.close(fd)
    #sync on non-existing fd (assume fd 12345 is not exists)
    assert_raises(libcephfs.Error, cephfs.fsync, 12345, 0)

def test_directory(testdir):
    cephfs.mkdir(b"/temp-directory", 0o755)
    cephfs.mkdirs(b"/temp-directory/foo/bar", 0o755)
    cephfs.chdir(b"/temp-directory")
    assert_equal(cephfs.getcwd(), b"/temp-directory")
    cephfs.rmdir(b"/temp-directory/foo/bar")
    cephfs.rmdir(b"/temp-directory/foo")
    cephfs.rmdir(b"/temp-directory")
    assert_raises(libcephfs.ObjectNotFound, cephfs.chdir, b"/temp-directory")

def test_walk_dir(testdir):
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

def test_xattr(testdir):
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

def test_ceph_mirror_xattr(testdir):
    def gen_mirror_xattr():
        cluster_id = str(uuid.uuid4())
        fs_id = random.randint(1, 10)
        mirror_xattr = f'cluster_id={cluster_id} fs_id={fs_id}'
        return mirror_xattr.encode('utf-8')

    mirror_xattr_enc_1 = gen_mirror_xattr()

    # mirror xattr is only allowed on root
    cephfs.mkdir('/d0', 0o755)
    assert_raises(libcephfs.InvalidValue, cephfs.setxattr,
                  '/d0', 'ceph.mirror.info', mirror_xattr_enc_1, os.XATTR_CREATE)
    cephfs.rmdir('/d0')

    cephfs.setxattr('/', 'ceph.mirror.info', mirror_xattr_enc_1, os.XATTR_CREATE)
    assert_equal(mirror_xattr_enc_1, cephfs.getxattr('/', 'ceph.mirror.info'))

    # setting again with XATTR_CREATE should fail
    assert_raises(libcephfs.ObjectExists, cephfs.setxattr,
                  '/', 'ceph.mirror.info', mirror_xattr_enc_1, os.XATTR_CREATE)

    # ceph.mirror.info should not show up in listing
    ret_val, _ = cephfs.listxattr("/")
    assert_equal(0, ret_val)

    mirror_xattr_enc_2 = gen_mirror_xattr()

    cephfs.setxattr('/', 'ceph.mirror.info', mirror_xattr_enc_2, os.XATTR_REPLACE)
    assert_equal(mirror_xattr_enc_2, cephfs.getxattr('/', 'ceph.mirror.info'))

    cephfs.removexattr('/', 'ceph.mirror.info')
    # ceph.mirror.info is already removed
    assert_raises(libcephfs.NoData, cephfs.getxattr, '/', 'ceph.mirror.info')
    # removing again should throw error
    assert_raises(libcephfs.NoData, cephfs.removexattr, "/", "ceph.mirror.info")

    # check mirror info xattr format
    assert_raises(libcephfs.InvalidValue, cephfs.setxattr, '/', 'ceph.mirror.info', b"unknown", 0)

def test_fxattr(testdir):
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

def test_rename(testdir):
    cephfs.mkdir(b"/a", 0o755)
    cephfs.mkdir(b"/a/b", 0o755)
    cephfs.rename(b"/a", b"/b")
    cephfs.stat(b"/b/b")
    cephfs.rmdir(b"/b/b")
    cephfs.rmdir(b"/b")

def test_open(testdir):
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

def test_link(testdir):
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

def test_symlink(testdir):
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

def test_readlink(testdir):
    fd = cephfs.open(b'/file-1', 'w', 0o755)
    cephfs.write(fd, b"1111", 0)
    cephfs.close(fd)
    cephfs.symlink(b'/file-1', b'/file-2')
    d = cephfs.readlink(b"/file-2",100)
    assert_equal(d, b"/file-1")
    cephfs.unlink(b'/file-2')
    cephfs.unlink(b'/file-1')

def test_delete_cwd(testdir):
    assert_equal(b"/", cephfs.getcwd())

    cephfs.mkdir(b"/temp-directory", 0o755)
    cephfs.chdir(b"/temp-directory")
    cephfs.rmdir(b"/temp-directory")

    # getcwd gives you something stale here: it remembers the path string
    # even when things are unlinked.  It's up to the caller to find out
    # whether it really still exists
    assert_equal(b"/temp-directory", cephfs.getcwd())

def test_flock(testdir):
    fd = cephfs.open(b'file-1', 'w', 0o755)

    cephfs.flock(fd, fcntl.LOCK_EX, 123);
    fd2 = cephfs.open(b'file-1', 'w', 0o755)

    assert_raises(libcephfs.WouldBlock, cephfs.flock, fd2,
                  fcntl.LOCK_EX | fcntl.LOCK_NB, 456);
    cephfs.close(fd2)

    cephfs.close(fd)

def test_mount_unmount(testdir):
    test_directory(testdir)
    cephfs.unmount()
    cephfs.mount()
    test_open(testdir)

def test_lxattr(testdir):
    fd = cephfs.open(b'/file-lxattr', 'w', 0o755)
    cephfs.close(fd)
    cephfs.setxattr(b"/file-lxattr", "user.key", b"value", 0)
    cephfs.symlink(b"/file-lxattr", b"/file-sym-lxattr")
    assert_equal(b"value", cephfs.getxattr(b"/file-sym-lxattr", "user.key"))
    assert_raises(libcephfs.NoData, cephfs.lgetxattr, b"/file-sym-lxattr", "user.key")

    cephfs.lsetxattr(b"/file-sym-lxattr", "trusted.key-sym", b"value-sym", 0)
    assert_equal(b"value-sym", cephfs.lgetxattr(b"/file-sym-lxattr", "trusted.key-sym"))
    cephfs.lsetxattr(b"/file-sym-lxattr", "trusted.big", b"x" * 300, 0)

    # Default size is 255, get ERANGE
    assert_raises(libcephfs.OutOfRange, cephfs.lgetxattr, b"/file-sym-lxattr", "trusted.big")

    # Pass explicit size, and we'll get the value
    assert_equal(300, len(cephfs.lgetxattr(b"/file-sym-lxattr", "trusted.big", 300)))

    cephfs.lremovexattr(b"/file-sym-lxattr", "trusted.key-sym")
    # trusted.key-sym is already removed
    assert_raises(libcephfs.NoData, cephfs.lgetxattr, b"/file-sym-lxattr", "trusted.key-sym")

    # trusted.big is only listed
    ret_val, ret_buff = cephfs.llistxattr(b"/file-sym-lxattr")
    assert_equal(12, ret_val)
    assert_equal("trusted.big\x00", ret_buff.decode('utf-8'))
    cephfs.unlink(b'/file-lxattr')
    cephfs.unlink(b'/file-sym-lxattr')

def test_mount_root(testdir):
    cephfs.mkdir(b"/mount-directory", 0o755)
    cephfs.unmount()
    cephfs.mount(mount_root = b"/mount-directory")

    assert_raises(libcephfs.Error, cephfs.mount, mount_root = b"/nowhere")
    cephfs.unmount()
    cephfs.mount()

def test_utime(testdir):
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

def test_futime(testdir):
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

def test_utimes(testdir):
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

def test_lutimes(testdir):
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

def test_futimes(testdir):
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

def test_futimens(testdir):
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

def test_lchmod(testdir):
    fd = cephfs.open(b'/file-1', 'w', 0o755)
    cephfs.write(fd, b'0000', 0)
    cephfs.close(fd)

    cephfs.symlink(b'/file-1', b'/file-2')

    stx_pre_t = cephfs.statx(b'/file-1', libcephfs.CEPH_STATX_MODE, 0)
    stx_pre_s = cephfs.statx(b'/file-2', libcephfs.CEPH_STATX_MODE, libcephfs.AT_SYMLINK_NOFOLLOW)

    time.sleep(1)
    cephfs.lchmod(b'/file-2', 0o400)

    stx_post_t = cephfs.statx(b'/file-1', libcephfs.CEPH_STATX_MODE, 0)
    stx_post_s = cephfs.statx(b'/file-2', libcephfs.CEPH_STATX_MODE, libcephfs.AT_SYMLINK_NOFOLLOW)

    assert_equal(stx_post_t['mode'], stx_pre_t['mode'])
    assert_not_equal(stx_post_s['mode'], stx_pre_s['mode'])
    stx_post_s_perm_bits = stx_post_s['mode'] & ~stat.S_IFMT(stx_post_s["mode"])
    assert_equal(stx_post_s_perm_bits, 0o400)

    cephfs.unlink(b'/file-2')
    cephfs.unlink(b'/file-1')

def test_fchmod(testdir):
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

def test_truncate(testdir):
    fd = cephfs.open(b'/file-truncate', 'w', 0o755)
    cephfs.write(fd, b"1111", 0)
    cephfs.truncate(b'/file-truncate', 0)
    stat = cephfs.fsync(fd, 0)
    st = cephfs.statx(b'/file-truncate', libcephfs.CEPH_STATX_SIZE, 0)
    assert_equal(st["size"], 0)
    cephfs.close(fd)
    cephfs.unlink(b'/file-truncate')

def test_ftruncate(testdir):
    fd = cephfs.open(b'/file-ftruncate', 'w', 0o755)
    cephfs.write(fd, b"1111", 0)
    assert_raises(TypeError, cephfs.ftruncate, b'/file-ftruncate', 0)
    cephfs.ftruncate(fd, 0)
    stat = cephfs.fsync(fd, 0)
    st = cephfs.fstat(fd)
    assert_equal(st.st_size, 0)
    cephfs.close(fd)
    cephfs.unlink(b'/file-ftruncate')

def test_fallocate(testdir):
    fd = cephfs.open(b'/file-fallocate', 'w', 0o755)
    assert_raises(TypeError, cephfs.fallocate, b'/file-fallocate', 0, 10)
    assert_raises(libcephfs.OperationNotSupported, cephfs.fallocate, fd, 0, 10)
    stat = cephfs.fsync(fd, 0)
    st = cephfs.fstat(fd)
    assert_equal(st.st_size, 0)
    cephfs.close(fd)
    cephfs.unlink(b'/file-fallocate')

def test_mknod(testdir):
    mode = stat.S_IFIFO | stat.S_IRUSR | stat.S_IWUSR
    cephfs.mknod(b'/file-fifo', mode)
    st = cephfs.statx(b'/file-fifo', libcephfs.CEPH_STATX_MODE, 0)
    assert_equal(st["mode"] & mode, mode)
    cephfs.unlink(b'/file-fifo')

def test_lazyio(testdir):
    fd = cephfs.open(b'/file-lazyio', 'w', 0o755)
    assert_raises(TypeError, cephfs.lazyio, "fd", 1)
    assert_raises(TypeError, cephfs.lazyio, fd, "1")
    cephfs.lazyio(fd, 1)
    cephfs.write(fd, b"1111", 0)
    assert_raises(TypeError, cephfs.lazyio_propagate, "fd", 0, 4)
    assert_raises(TypeError, cephfs.lazyio_propagate, fd, "0", 4)
    assert_raises(TypeError, cephfs.lazyio_propagate, fd, 0, "4")
    cephfs.lazyio_propagate(fd, 0, 4)
    st = cephfs.fstat(fd)
    assert_equal(st.st_size, 4)
    cephfs.write(fd, b"2222", 4)
    assert_raises(TypeError, cephfs.lazyio_synchronize, "fd", 0, 8)
    assert_raises(TypeError, cephfs.lazyio_synchronize, fd, "0", 8)
    assert_raises(TypeError, cephfs.lazyio_synchronize, fd, 0, "8")
    cephfs.lazyio_synchronize(fd, 0, 8)
    st = cephfs.fstat(fd)
    assert_equal(st.st_size, 8)
    cephfs.close(fd)
    cephfs.unlink(b'/file-lazyio')

def test_replication(testdir):
    fd = cephfs.open(b'/file-rep', 'w', 0o755)
    assert_raises(TypeError, cephfs.get_file_replication, "fd")
    l_dict = cephfs.get_layout(fd)
    assert('pool_name' in l_dict.keys())
    cnt = cephfs.get_file_replication(fd)
    get_rep_cnt_cmd = "ceph osd pool get " + l_dict["pool_name"] + " size"
    s=os.popen(get_rep_cnt_cmd).read().strip('\n')
    size=int(s.split(" ")[-1])
    assert_equal(cnt, size)
    cnt = cephfs.get_path_replication(b'/file-rep')
    assert_equal(cnt, size)
    cephfs.close(fd)
    cephfs.unlink(b'/file-rep')

def test_caps(testdir):
    fd = cephfs.open(b'/file-caps', 'w', 0o755)
    timeout = cephfs.get_cap_return_timeout()
    assert_equal(timeout, 300)
    fd_caps = cephfs.debug_get_fd_caps(fd)
    file_caps = cephfs.debug_get_file_caps(b'/file-caps')
    assert_equal(fd_caps, file_caps)
    cephfs.close(fd)
    cephfs.unlink(b'/file-caps')

def test_setuuid(testdir):
    ses_id_uid = uuid.uuid1()
    ses_id_str = str(ses_id_uid)
    cephfs.set_uuid(ses_id_str)

def test_session_timeout(testdir):
    assert_raises(TypeError, cephfs.set_session_timeout, "300")
    cephfs.set_session_timeout(300)

def test_readdirops(testdir):
    cephfs.chdir(b"/")
    dirs = [b"dir-1", b"dir-2", b"dir-3"]
    for i in dirs:
        cephfs.mkdir(i, 0o755)
    handler = cephfs.opendir(b"/")
    d1 = cephfs.readdir(handler)
    d2 = cephfs.readdir(handler)
    d3 = cephfs.readdir(handler)
    offset_d4 = cephfs.telldir(handler)
    d4 = cephfs.readdir(handler)
    cephfs.rewinddir(handler)
    d = cephfs.readdir(handler)
    assert_equal(d.d_name, d1.d_name)
    cephfs.seekdir(handler, offset_d4)
    d = cephfs.readdir(handler)
    assert_equal(d.d_name, d4.d_name)
    dirs += [b".", b".."]
    cephfs.rewinddir(handler)
    d = cephfs.readdir(handler)
    while d:
        assert(d.d_name in dirs)
        dirs.remove(d.d_name)
        d = cephfs.readdir(handler)
    assert(len(dirs) == 0)
    dirs = [b"/dir-1", b"/dir-2", b"/dir-3"]
    for i in dirs:
        cephfs.rmdir(i)
    cephfs.closedir(handler)

def test_preadv_pwritev():
    fd = cephfs.open(b'file-1', 'w', 0o755)
    cephfs.pwritev(fd, [b"asdf", b"zxcvb"], 0)
    cephfs.close(fd)
    fd = cephfs.open(b'file-1', 'r', 0o755)
    buf = [bytearray(i) for i in [4, 5]]
    cephfs.preadv(fd, buf, 0)
    assert_equal([b"asdf", b"zxcvb"], list(buf))
    cephfs.close(fd)
    cephfs.unlink(b'file-1')

def test_get_layout(testdir):
    fd = cephfs.open(b'file-get-layout', 'w', 0o755)
    cephfs.write(fd, b"1111", 0)
    assert_raises(TypeError, cephfs.get_layout, "fd")
    l_dict = cephfs.get_layout(fd)
    assert('stripe_unit' in l_dict.keys())
    assert('stripe_count' in l_dict.keys())
    assert('object_size' in l_dict.keys())
    assert('pool_id' in l_dict.keys())
    assert('pool_name' in l_dict.keys())

    cephfs.close(fd)
    cephfs.unlink(b'file-get-layout')

def test_get_default_pool(testdir):
    dp_dict = cephfs.get_default_pool()
    assert('pool_id' in dp_dict.keys())
    assert('pool_name' in dp_dict.keys())

def test_get_pool(testdir):
    dp_dict = cephfs.get_default_pool()
    assert('pool_id' in dp_dict.keys())
    assert('pool_name' in dp_dict.keys())
    assert_equal(cephfs.get_pool_id(dp_dict["pool_name"]), dp_dict["pool_id"])
    get_rep_cnt_cmd = "ceph osd pool get " + dp_dict["pool_name"] + " size"
    s=os.popen(get_rep_cnt_cmd).read().strip('\n')
    size=int(s.split(" ")[-1])
    assert_equal(cephfs.get_pool_replication(dp_dict["pool_id"]), size)

def test_disk_quota_exceeeded_error(testdir):
    cephfs.mkdir("/dir-1", 0o755)
    cephfs.setxattr("/dir-1", "ceph.quota.max_bytes", b"5", 0)
    fd = cephfs.open(b'/dir-1/file-1', 'w', 0o755)
    assert_raises(libcephfs.DiskQuotaExceeded, cephfs.write, fd, b"abcdeghiklmnopqrstuvwxyz", 0)
    cephfs.close(fd)
    cephfs.unlink(b"/dir-1/file-1")

def test_empty_snapshot_info(testdir):
    cephfs.mkdir("/dir-1", 0o755)

    # snap without metadata
    cephfs.mkdir("/dir-1/.snap/snap0", 0o755)
    snap_info = cephfs.snap_info("/dir-1/.snap/snap0")
    assert_equal(snap_info["metadata"], {})
    assert_greater(snap_info["id"], 0)
    cephfs.rmdir("/dir-1/.snap/snap0")

    # remove directory
    cephfs.rmdir("/dir-1")

def test_snapshot_info(testdir):
    cephfs.mkdir("/dir-1", 0o755)

    # snap with custom metadata
    md = {"foo": "bar", "zig": "zag", "abcdefg": "12345"}
    cephfs.mksnap("/dir-1", "snap0", 0o755, metadata=md)
    snap_info = cephfs.snap_info("/dir-1/.snap/snap0")
    assert_equal(snap_info["metadata"]["foo"], md["foo"])
    assert_equal(snap_info["metadata"]["zig"], md["zig"])
    assert_equal(snap_info["metadata"]["abcdefg"], md["abcdefg"])
    assert_greater(snap_info["id"], 0)
    cephfs.rmsnap("/dir-1", "snap0")

    # remove directory
    cephfs.rmdir("/dir-1")

def test_set_mount_timeout_post_mount(testdir):
    assert_raises(libcephfs.LibCephFSStateError, cephfs.set_mount_timeout, 5)

def test_set_mount_timeout(testdir):
    cephfs.unmount()
    cephfs.set_mount_timeout(5)
    cephfs.mount()

def test_set_mount_timeout_lt0(testdir):
    cephfs.unmount()
    assert_raises(libcephfs.InvalidValue, cephfs.set_mount_timeout, -5)
    cephfs.mount()

def test_snapdiff(testdir):
    cephfs.mkdir("/snapdiff_test", 0o755)
    fd = cephfs.open('/snapdiff_test/file-1', 'w', 0o755)
    cephfs.write(fd, b"1111", 0)
    cephfs.close(fd)
    fd = cephfs.open('/snapdiff_test/file-2', 'w', 0o755)
    cephfs.write(fd, b"2222", 0)
    cephfs.close(fd)
    cephfs.mksnap("/snapdiff_test", "snap1", 0o755)
    fd = cephfs.open('/snapdiff_test/file-1', 'w', 0o755)
    cephfs.write(fd, b"1222", 0)
    cephfs.close(fd)
    cephfs.unlink('/snapdiff_test/file-2')
    cephfs.mksnap("/snapdiff_test", "snap2", 0o755)
    snap1id = cephfs.snap_info(b"/snapdiff_test/.snap/snap1")['id']
    snap2id = cephfs.snap_info(b"/snapdiff_test/.snap/snap2")['id']
    diff = cephfs.opensnapdiff(b"/snapdiff_test", b"/", b"snap2", b"snap1")
    cnt = 0
    e = diff.readdir()
    while e is not None:
        if (e.d_name == b"file-1"):
            cnt = cnt + 1
            assert_equal(snap2id, e.d_snapid)
        elif (e.d_name == b"file-2"):
            cnt = cnt + 1
            assert_equal(snap1id, e.d_snapid)
        elif (e.d_name != b"." and e.d_name != b".."):
            cnt = cnt + 1
        e = diff.readdir()
    assert_equal(cnt, 2)
    diff.close()

    # remove directory
    purge_dir(b"/snapdiff_test");

def test_single_target_command():
    command = {'prefix': u'session ls', 'format': 'json'}
    mds_spec  = "a"
    inbuf = b''
    ret, outbl, outs = cephfs.mds_command(mds_spec, json.dumps(command), inbuf)
    if outbl:
        session_map = json.loads(outbl)
    # Standby MDSs will return -38
    assert(ret == 0 or ret == -38)

def test_multi_target_command():
    mds_get_command = {'prefix': 'status', 'format': 'json'}
    inbuf = b''
    ret, outbl, outs = cephfs.mds_command('*', json.dumps(mds_get_command), inbuf)
    print(outbl)
    mds_status = json.loads(outbl)
    print(mds_status)

    command = {'prefix': u'session ls', 'format': 'json'}
    mds_spec  = "*"
    inbuf = b''

    ret, outbl, outs = cephfs.mds_command(mds_spec, json.dumps(command), inbuf)
    # Standby MDSs will return -38
    assert(ret == 0 or ret == -38)
    print(outbl)
    session_map = json.loads(outbl)

    if isinstance(mds_status, list): # if multi target command result
        for mds_sessions in session_map:
            assert(list(mds_sessions.keys())[0].startswith('mds.'))


class TestUnlinkat:

    def test_unlinkat_regfile_fd_of_root(self, testdir):
        regfilename = 'file1'

        fd = cephfs.open(regfilename, 'w', 0o755)
        cephfs.write(fd, b"abcd", 0)
        cephfs.close(fd)

        fd = cephfs.open('/', os.O_RDONLY | os.O_DIRECTORY, 0o755)
        cephfs.unlinkat(fd, './' + regfilename, 0)
        cephfs.close(fd)

    def test_unlinkat_dir_fd_of_root(self, testdir):
        dirname = 'dir1'

        cephfs.mkdir(dirname, 0o755)
        fd = cephfs.open('/', os.O_RDONLY | os.O_DIRECTORY, 0o755)
        # AT_REMOVEDIR = 0x200
        cephfs.unlinkat(fd, dirname, 0x200)
        cephfs.close(fd)

    def test_unlinkat_regfile_fd_of_nonroot(self, testdir):
        dir1_name = 'dir1'
        regfile_name = 'file1'
        regfile_path = 'dir1/file1'

        cephfs.mkdir(dir1_name, 0o755)
        fd = cephfs.open(regfile_path, 'w', 0o755)
        cephfs.write(fd, b"abcd", 0)
        cephfs.close(fd)

        fd = cephfs.open(dir1_name, os.O_RDONLY | os.O_DIRECTORY, 0o755)
        cephfs.unlinkat(fd, regfile_name, 0)
        cephfs.close(fd)

    def test_unlinkat_dir_fd_of_nonroot(self, testdir):
        dir1_name = 'dir1'
        dir2_name = 'dir2'
        dir2_path = 'dir1/dir2'

        cephfs.mkdir(dir1_name, 0o755)
        cephfs.mkdir(dir2_path, 0o755)
        fd = cephfs.open('dir1', os.O_RDONLY | os.O_DIRECTORY, 0o755)
        cephfs.unlinkat(fd, dir2_name, libcephfs.AT_REMOVEDIR)
        cephfs.close(fd)

    def test_unlinkat_regfile_AT_FDCWD(self, testdir):
        regfilename = 'file1'

        fd = cephfs.open(regfilename, 'w', 0o755)
        cephfs.write(fd, b"abcd", 0)
        cephfs.close(fd)
        cephfs.unlinkat(libcephfs.AT_FDCWD, regfilename, 0)

    def test_unlinkat_dir_AT_FDCWD(self, testdir):
        dirname = 'dir1'

        cephfs.mkdir(dirname, 0o755)
        cephfs.unlinkat(libcephfs.AT_FDCWD, dirname, libcephfs.AT_REMOVEDIR)

    def test_unlinkat_for_opened_dir(self, testdir):
        dirname = 'dir1'

        cephfs.mkdir(dirname, 0o755)
        fd = cephfs.open('/', os.O_RDONLY | os.O_DIRECTORY, 0o755)
        fh = cephfs.opendir(dirname)
        cephfs.unlinkat(fd, dirname, libcephfs.AT_REMOVEDIR)
        cephfs.close(fd)


class TestFdopendir:
    '''
    Tests for libcephfs's fdopendir().
    '''

    def test_fdopendir_for_dir_at_CWD(self, testdir):
        dirname = 'dir1'
        cephfs.mkdir(dirname, 0o755)

        fd = cephfs.open(dirname, os.O_RDONLY | os.O_DIRECTORY, 0o755)
        dh = cephfs.fdopendir(fd)

        dh.close()

    def test_fdopendir_for_dir_not_at_CWD(self, testdir):
        dirname = 'dir1/dir2/dir3'
        cephfs.mkdir('dir1', 0o755)
        cephfs.mkdir('dir1/dir2', 0o755)
        cephfs.mkdir(dirname, 0o755)

        fd = cephfs.open(dirname, os.O_RDONLY | os.O_DIRECTORY, 0o755)
        dh = cephfs.fdopendir(fd)

        dh.close()


class TestWithRootUser:

    def setup_method(self):
        cephfs.unmount()
        cephfs.conf_set('client_mount_uid', '0')
        cephfs.conf_set('client_mount_gid', '0')
        cephfs.mount()

        cephfs.conf_set('client_permissions' , 'false')
        cephfs.chown('/', 0, 0)
        cephfs.conf_set('client_permissions' , 'true')

    def teardown_method(self):
        cephfs.unmount()

        username = get_cmd_output('id -un')
        uid = get_cmd_output(f'id -u {username}')
        gid = get_cmd_output(f'id -g {username}')

        cephfs.conf_set('client_mount_uid', uid)
        cephfs.conf_set('client_mount_gid', gid)
        cephfs.mount()

        cephfs.conf_set('client_permissions' , 'false')
        cephfs.chown('/', int(uid), int(gid))
        cephfs.conf_set('client_permissions' , 'true')

    def test_chown(self, testdir):
        fd = cephfs.open('file1', 'w', 0o755)
        cephfs.write(fd, b'abcd', 0)
        cephfs.close(fd)

        uid = 1012
        gid = 1012
        cephfs.chown('file1', uid, gid)
        st = cephfs.stat(b'/file1')
        assert_equal(st.st_uid, uid)
        assert_equal(st.st_gid, gid)

    def test_chown_change_uid_but_not_gid(self, testdir):
        fd = cephfs.open('file1', 'w', 0o755)
        cephfs.write(fd, b'abcd', 0)
        cephfs.close(fd)

        st1 = cephfs.stat(b'/file1')

        uid = 1012
        cephfs.chown('file1', uid, -1)
        st2 = cephfs.stat(b'/file1')
        assert_equal(st2.st_uid, uid)

        # ensure that gid is unchaged.
        assert_equal(st1.st_gid, st2.st_gid)

    def test_chown_change_gid_but_not_uid(self, testdir):
        fd = cephfs.open('file1', 'w', 0o755)
        cephfs.write(fd, b'abcd', 0)
        cephfs.close(fd)

        st1 = cephfs.stat(b'/file1')

        gid = 1012
        cephfs.chown('file1', -1, gid)
        st2 = cephfs.stat(b'/file1')
        assert_equal(st2.st_gid, gid)

        # ensure that uid is unchaged.
        assert_equal(st1.st_uid, st2.st_uid)

    def test_lchown(self, testdir):
        fd = cephfs.open('file1', 'w', 0o755)
        cephfs.write(fd, b'abcd', 0)
        cephfs.close(fd)
        cephfs.symlink('file1', 'slink1')

        uid = 1012
        gid = 1012
        cephfs.lchown('slink1', uid, gid)
        st = cephfs.lstat(b'/slink1')
        assert_equal(st.st_uid, uid)
        assert_equal(st.st_gid, gid)

    def test_lchown_change_uid_but_not_gid(self, testdir):
        fd = cephfs.open('file2', 'w', 0o755)
        cephfs.write(fd, b'abcd', 0)
        cephfs.close(fd)
        cephfs.symlink('file2', 'slink2')

        st1 = cephfs.lstat(b'slink2')

        uid = 1012
        cephfs.lchown('slink2', uid, -1)
        st2 = cephfs.lstat(b'/slink2')
        assert_equal(st2.st_uid, uid)

        # ensure that gid is unchaged.
        assert_equal(st1.st_gid, st2.st_gid)

    def test_lchown_change_gid_but_not_uid(self, testdir):
        fd = cephfs.open('file3', 'w', 0o755)
        cephfs.write(fd, b'abcd', 0)
        cephfs.close(fd)
        cephfs.symlink('file3', 'slink3')

        st1 = cephfs.lstat(b'slink3')

        gid = 1012
        cephfs.lchown('slink3', -1, gid)
        st2 = cephfs.lstat(b'/slink3')
        assert_equal(st2.st_gid, gid)

        # ensure that uid is unchaged.
        assert_equal(st1.st_uid, st2.st_uid)

    def test_fchown(self, testdir):
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

    def test_fchown_change_uid_but_not_gid(self, testdir):
        fd = cephfs.open('file1', 'w', 0o755)
        cephfs.write(fd, b'abcd', 0)

        st1 = cephfs.stat(b'/file1')

        uid = 1012
        cephfs.fchown(fd, uid, -1)
        st2 = cephfs.stat(b'/file1')
        assert_equal(st2.st_uid, uid)

        # ensure that uid is unchanged.
        assert_equal(st1.st_gid, st2.st_gid)

        cephfs.close(fd)

    def test_fchown_change_gid_but_not_uid(self, testdir):
        fd = cephfs.open('file1', 'w', 0o755)
        cephfs.write(fd, b'abcd', 0)

        st1 = cephfs.stat(b'/file1')

        gid = 1012
        cephfs.chown('file1', -1, gid)
        st2 = cephfs.stat(b'/file1')
        assert_equal(st2.st_gid, gid)

        # ensure that gid is unchanged.
        assert_equal(st1.st_uid, st2.st_uid)

        cephfs.close(fd)

    def test_setattrx(self, testdir):
        fd = cephfs.open(b'file-setattrx', 'w', 0o655)
        cephfs.write(fd, b"1111", 0)
        cephfs.close(fd)
        st = cephfs.statx(b'file-setattrx', libcephfs.CEPH_STATX_MODE, 0)
        mode = st["mode"] | stat.S_IXUSR
        assert_raises(TypeError, cephfs.setattrx, b'file-setattrx', "dict", 0, 0)

        time.sleep(1)
        statx_dict = dict()
        statx_dict["mode"] = mode
        statx_dict["uid"] = 9999
        statx_dict["gid"] = 9999
        dt = datetime.now()
        statx_dict["mtime"] = dt
        statx_dict["atime"] = dt
        statx_dict["ctime"] = dt
        statx_dict["size"] = 10
        statx_dict["btime"] = dt
        cephfs.setattrx(b'file-setattrx', statx_dict, libcephfs.CEPH_SETATTR_MODE |
                                                      libcephfs.CEPH_SETATTR_UID |
                                                      libcephfs.CEPH_SETATTR_GID |
                                                      libcephfs.CEPH_SETATTR_MTIME |
                                                      libcephfs.CEPH_SETATTR_ATIME |
                                                      libcephfs.CEPH_SETATTR_CTIME |
                                                      libcephfs.CEPH_SETATTR_SIZE |
                                                      libcephfs.CEPH_SETATTR_BTIME, 0)
        st1 = cephfs.statx(b'file-setattrx', libcephfs.CEPH_STATX_MODE |
                                             libcephfs.CEPH_STATX_UID |
                                             libcephfs.CEPH_STATX_GID |
                                             libcephfs.CEPH_STATX_MTIME |
                                             libcephfs.CEPH_STATX_ATIME |
                                             libcephfs.CEPH_STATX_CTIME |
                                             libcephfs.CEPH_STATX_SIZE |
                                             libcephfs.CEPH_STATX_BTIME, 0)
        assert_equal(mode, st1["mode"])
        assert_equal(9999, st1["uid"])
        assert_equal(9999, st1["gid"])
        assert_equal(int(dt.timestamp()), int(st1["mtime"].timestamp()))
        assert_equal(int(dt.timestamp()), int(st1["atime"].timestamp()))
        assert_equal(int(dt.timestamp()), int(st1["ctime"].timestamp()))
        assert_equal(int(dt.timestamp()), int(st1["btime"].timestamp()))
        assert_equal(10, st1["size"])
        cephfs.unlink(b'file-setattrx')

    def test_fsetattrx(self, testdir):
        fd = cephfs.open(b'file-fsetattrx', 'w', 0o655)
        cephfs.write(fd, b"1111", 0)
        st = cephfs.statx(b'file-fsetattrx', libcephfs.CEPH_STATX_MODE, 0)
        mode = st["mode"] | stat.S_IXUSR
        assert_raises(TypeError, cephfs.fsetattrx, fd, "dict", 0, 0)

        time.sleep(1)
        statx_dict = dict()
        statx_dict["mode"] = mode
        statx_dict["uid"] = 9999
        statx_dict["gid"] = 9999
        dt = datetime.now()
        statx_dict["mtime"] = dt
        statx_dict["atime"] = dt
        statx_dict["ctime"] = dt
        statx_dict["size"] = 10
        statx_dict["btime"] = dt
        cephfs.fsetattrx(fd, statx_dict, libcephfs.CEPH_SETATTR_MODE |
                                         libcephfs.CEPH_SETATTR_UID |
                                         libcephfs.CEPH_SETATTR_GID |
                                         libcephfs.CEPH_SETATTR_MTIME |
                                         libcephfs.CEPH_SETATTR_ATIME |
                                         libcephfs.CEPH_SETATTR_CTIME |
                                         libcephfs.CEPH_SETATTR_SIZE |
                                         libcephfs.CEPH_SETATTR_BTIME)
        st1 = cephfs.statx(b'file-fsetattrx', libcephfs.CEPH_STATX_MODE |
                                              libcephfs.CEPH_STATX_UID |
                                              libcephfs.CEPH_STATX_GID |
                                              libcephfs.CEPH_STATX_MTIME |
                                              libcephfs.CEPH_STATX_ATIME |
                                              libcephfs.CEPH_STATX_CTIME |
                                              libcephfs.CEPH_STATX_SIZE |
                                              libcephfs.CEPH_STATX_BTIME, 0)
        assert_equal(mode, st1["mode"])
        assert_equal(9999, st1["uid"])
        assert_equal(9999, st1["gid"])
        assert_equal(int(dt.timestamp()), int(st1["mtime"].timestamp()))
        assert_equal(int(dt.timestamp()), int(st1["atime"].timestamp()))
        assert_equal(int(dt.timestamp()), int(st1["ctime"].timestamp()))
        assert_equal(int(dt.timestamp()), int(st1["btime"].timestamp()))
        assert_equal(10, st1["size"])
        cephfs.close(fd)
        cephfs.unlink(b'file-fsetattrx')


class TestRmtree:
    '''
    Test rmtree() method of CephFS python bindings.
    '''

    def test_rmtree_on_regfile(self, testdir):
        should_cancel = lambda: False

        fd = cephfs.open(f'/file1', 'w', 0o755)
        cephfs.write(fd, b'abcd', 0)
        cephfs.close(fd)

        # Errors are not expected from the call to this method. Therefore, set
        # suppress_errors to False so that tests abort as soon as any errors
        # occur.
        cephfs.rmtree('/file1', should_cancel, suppress_errors=False)
        assert_raises(libcephfs.ObjectNotFound, cephfs.stat, 'file1')

    def test_rmtree_on_regfile_no_perms(self, testdir):
        should_cancel = lambda: False

        fd = cephfs.open(f'/file1', 'w', 0o755)
        cephfs.write(fd, b'abcd', 0)
        cephfs.close(fd)

        cephfs.chmod('/file1', 0o000)
        # Errors are not expected from the call to this method. Therefore, set
        # suppress_errors to False so that tests abort as soon as any errors
        # occur.
        cephfs.rmtree('/file1', should_cancel, suppress_errors=False)
        assert_raises(libcephfs.ObjectNotFound, cephfs.stat, 'file1')

    def test_rmtree_on_symlink(self, testdir):
        should_cancel = lambda: False

        fd = cephfs.open(f'/file1', 'w', 0o755)
        cephfs.write(fd, b'abcd', 0)
        cephfs.close(fd)
        cephfs.symlink('file1', '/slink1')

        # Errors are not expected from the call to this method. Therefore, set
        # suppress_errors to False so that tests abort as soon as any errors
        # occur.
        cephfs.rmtree('/slink1', should_cancel, suppress_errors=False)
        assert_raises(libcephfs.ObjectNotFound, cephfs.stat, 'slink1')
        cephfs.stat('file1')

    def test_rmtree_on_symlink_no_perms(self, testdir):
        should_cancel = lambda: False

        fd = cephfs.open(f'/file1', 'w', 0o755)
        cephfs.write(fd, b'abcd', 0)
        cephfs.close(fd)
        cephfs.symlink('file1', '/slink1')

        cephfs.chmod('/file1', 0o000)
        cephfs.chmod('/slink1', 0o000)
        # Errors are not expected from the call to this method. Therefore, set
        # suppress_errors to False so that tests abort as soon as any errors
        # occur.
        cephfs.rmtree('/slink1', should_cancel, suppress_errors=False)
        assert_raises(libcephfs.ObjectNotFound, cephfs.stat, 'slink1')
        cephfs.stat('file1')

    def test_rmtree_when_tree_contains_only_regfiles(self, testdir):
        '''
        Test rmtree() successfully deletes the entire file hierarchy that contains
        only regular files.
        '''
        should_cancel = lambda: False

        cephfs.mkdir('dir1', 0o755)
        for i in range(1, 6):
            fd = cephfs.open(f'/dir1/file{i}', 'w', 0o755)
            cephfs.write(fd, b'abcd', 0)
            cephfs.close(fd)

        # Errors are not expected from the call to this method. Therefore, set
        # suppress_errors to False so that tests abort as soon as any errors
        # occur.
        cephfs.rmtree('dir1', should_cancel, suppress_errors=False)
        assert_raises(libcephfs.ObjectNotFound, cephfs.stat, 'dir1')

    def test_rmtree_when_tree_contains_dirs_and_regfiles(self, testdir):
        '''
        Test that rmtree() successfully deletes the entire file hierarchy that
        contains only directories and regular files.
        '''
        should_cancel = lambda: False

        cephfs.mkdir('dir2', 0o755)
        for i in range(1, 6):
            cephfs.mkdir(f'/dir2/dir2{i}', 0o755)
            for j in range(1, 6):
                fd = cephfs.open(f'/dir2/dir2{i}/file{j}', 'w', 0o755)
                cephfs.write(fd, b'abcd', 0)
                cephfs.close(fd)

        # Errors are not expected from the call to this method. Therefore, set
        # suppress_errors to False so that tests abort as soon as any errors
        # occur.
        cephfs.rmtree('dir2', should_cancel, suppress_errors=False)
        assert_raises(libcephfs.ObjectNotFound, cephfs.stat, 'dir2')

    def test_rmtree_when_tree_contains_dirs_regfiles_and_symlinks(self, testdir):
        '''
        Test that rmtree() successfully deletes entire file hierarchy that
        contains directories, regular files as well as symbolic links.
        '''
        should_cancel = lambda: False

        cephfs.mkdir('dir3', 0o755)
        for i in range(1, 6):
            fd = cephfs.open(f'/dir3/file{i}', 'w', 0o755)
            cephfs.write(fd, b'abcd', 0)
            cephfs.close(fd)

            file_name = f'/dir3/file{i}'.encode('utf-8')
            slink_name = f'slink{i}'.encode('utf-8')
            cephfs.symlink(file_name, slink_name)

        # Errors are not expected from the call to this method. Therefore, set
        # suppress_errors to False so that tests abort as soon as any errors
        # occur.
        cephfs.rmtree('dir3', should_cancel, suppress_errors=False)
        assert_raises(libcephfs.ObjectNotFound, cephfs.stat, 'dir3')

    def test_rmtree_when_path_has_trailing_slash(self, testdir):
        '''
        Test rmtree() successfully deletes the entire file hierarchy when path
        passed to rmtree() ends with a slash
        '''
        should_cancel = lambda: False

        cephfs.mkdir('dir1', 0o755)
        for i in range(1, 6):
            fd = cephfs.open(f'/dir1/file{i}', 'w', 0o755)
            cephfs.write(fd, b'abcd', 0)
            cephfs.close(fd)

        # Errors are not expected from the call to this method. Therefore, set
        # suppress_errors to False so that tests abort as soon as any errors
        # occur.
        cephfs.rmtree('dir1/', should_cancel, suppress_errors=False)
        assert_raises(libcephfs.ObjectNotFound, cephfs.stat, 'dir1')

    def test_rmtree_when_symlink_points_to_parent_dir(self, testdir):
        '''
        Test that rmtree() successfully deletes entire file hierarchy that
        contains directories, regular files as well as symbolic links.
        '''
        should_cancel = lambda: False

        cephfs.mkdir('dir3', 0o755)
        cephfs.mkdir('dir3/dir4', 0o755)
        cephfs.symlink('../dir4', 'dir3/dir4/slink1')

        # Errors are not expected from the call to this method. Therefore, set
        # suppress_errors to False so that tests abort as soon as any errors
        # occur.
        cephfs.rmtree('dir3', should_cancel, suppress_errors=False)
        assert_raises(libcephfs.ObjectNotFound, cephfs.stat, 'dir3')

    def test_rmtree_when_tree_contains_only_empty_dirs(self, testdir):
        '''
        Test that rmtree() successfully deletes entire file hierarchy that contains
        only empty directories.
        '''
        should_cancel = lambda: False

        cephfs.mkdir('dir4', 0o755)
        for i in range(1, 6):
            cephfs.mkdir(f'/dir4/dir4{i}', 0o755)

        # Errors are not expected from the call to this method. Therefore, set
        # suppress_errors to False so that tests abort as soon as any errors
        # occur.
        cephfs.rmtree('dir4', should_cancel, suppress_errors=False)
        assert_raises(libcephfs.ObjectNotFound, cephfs.stat, 'dir4')

    def test_rmtree_when_root_is_empty_dir(self, testdir):
        '''
        Test that rmtree() successfully deletes entire file hierarchy when it is
        only an empty directory.
        '''
        should_cancel = lambda: False

        cephfs.mkdir('dir5', 0o755)

        # Errors are not expected from the call to this method. Therefore, set
        # suppress_errors to False so that tests abort as soon as any errors
        # occur.
        cephfs.rmtree('dir5', should_cancel, suppress_errors=False)
        assert_raises(libcephfs.ObjectNotFound, cephfs.stat, 'dir5')

    def test_rmtree_no_perm_on_nonroot_dir_suppress_errors(self, testdir):
        '''
        Test that rmtree() successfully deletes the entire file hierarchy except the
        branch where permission for one of the (non-root) directories is not
        granted.
        '''
        should_cancel = lambda: False

        cephfs.mkdir('dir1', 0o755)

        cephfs.mkdir('dir1/dir2', 0o755)
        for i in range(1, 6):
            fd = cephfs.open(f'/dir1/dir2/file{i}', 'w', 0o755)
            cephfs.write(fd, b'abcd', 0)
            cephfs.close(fd)

        cephfs.mkdir('dir1/dir3', 0o755)
        for i in range(1, 6):
            fd = cephfs.open(f'/dir1/dir3/file{i}', 'w', 0o755)
            cephfs.write(fd, b'abcd', 0)
            cephfs.close(fd)

        cephfs.mkdir('dir1/dir4', 0o755)
        for i in range(1, 6):
            fd = cephfs.open(f'/dir1/dir4/file{i}', 'w', 0o755)
            cephfs.write(fd, b'abcd', 0)
            cephfs.close(fd)

        cephfs.mkdir('dir1/dir5', 0o755)
        for i in range(1, 6):
            fd = cephfs.open(f'/dir1/dir4/file{i}', 'w', 0o755)
            cephfs.write(fd, b'abcd', 0)
            cephfs.close(fd)

        cephfs.mkdir('dir1/dir6', 0o755)
        for i in range(1, 6):
            fd = cephfs.open(f'/dir1/dir4/file{i}', 'w', 0o755)
            cephfs.write(fd, b'abcd', 0)
            cephfs.close(fd)

        # actual test
        cephfs.chmod('/dir1/dir4', 0o000)
        # Errors are expected from call to this method. Set suppress_errors to
        # True to confirm that this argument works.
        cephfs.rmtree('dir1', should_cancel, suppress_errors=True)
        # ensure /dir1/dir3 wasn't deleted
        cephfs.stat('dir1/dir4')
        cephfs.chmod('/dir1/dir4', 0o755)
        for i in range(1, 6):
            cephfs.stat(f'dir1/dir4/file{i}')

        # cleanup
        cephfs.rmtree('dir1', should_cancel)
        assert_raises(libcephfs.ObjectNotFound, cephfs.stat, 'dir1')

    def test_rmtree_no_perm_on_nonroot_dir_dont_suppress_errors(self, testdir):
        '''
        Test that rmtree() successfully deletes the entire file hierarchy except the
        branch where permission for one of the (non-root) directories is not
        granted.
        '''
        should_cancel = lambda: False

        cephfs.mkdir('dir1', 0o755)

        cephfs.mkdir('dir1/dir2', 0o755)
        for i in range(1, 6):
            fd = cephfs.open(f'/dir1/dir2/file{i}', 'w', 0o755)
            cephfs.write(fd, b'abcd', 0)
            cephfs.close(fd)

        cephfs.mkdir('dir1/dir3', 0o755)
        for i in range(1, 6):
            fd = cephfs.open(f'/dir1/dir3/file{i}', 'w', 0o755)
            cephfs.write(fd, b'abcd', 0)
            cephfs.close(fd)

        cephfs.mkdir('dir1/dir4', 0o755)
        for i in range(1, 6):
            fd = cephfs.open(f'/dir1/dir4/file{i}', 'w', 0o755)
            cephfs.write(fd, b'abcd', 0)
            cephfs.close(fd)

        # actual test
        cephfs.chmod('/dir1/dir3', 0o000)
        # Errors are not expected from the call to this method. Therefore, set
        # suppress_errors to False so that tests abort as soon as any errors
        # occur.
        assert_raises(libcephfs.PermissionDenied, cephfs.rmtree, 'dir1',
                      should_cancel, suppress_errors=False)
        # ensure /dir1/dir3 wasn't deleted
        cephfs.stat('dir1/dir3')

        # cleanup
        cephfs.chmod('/dir1/dir3', 0o755)
        cephfs.rmtree('dir1', should_cancel)
        assert_raises(libcephfs.ObjectNotFound, cephfs.stat, 'dir1')

    def test_rmtree_no_perm_on_root_suppress_errors(self, testdir):
        '''
        Test rmtree() exits when permission is not granted for the root of the file
        hierarchy.
        '''
        should_cancel = lambda: False

        cephfs.mkdir('dir1', 0o755)
        for i in range(1, 6):
            fd = cephfs.open(f'/dir1/file{i}', 'w', 0o755)
            cephfs.write(fd, b'abcd', 0)
            cephfs.close(fd)

        cephfs.chmod('/dir1', 0o000)
        # Errors are expected from call to this method. Set suppress_errors to
        # True to confirm that this argument works.
        cephfs.rmtree('dir1', should_cancel, suppress_errors=True)
        # ensure /dir1 wasn't deleted
        cephfs.stat('dir1')

        # cleanup
        cephfs.chmod('/dir1', 0o755)
        cephfs.rmtree('dir1', should_cancel)
        assert_raises(libcephfs.ObjectNotFound, cephfs.stat, 'dir1')

    def test_rmtree_no_perm_on_root_dont_suppress_errors(self, testdir):
        '''
        Test rmtree() exits when permission is not granted for the root of the file
        hierarchy.
        '''
        should_cancel = lambda: False

        cephfs.mkdir('dir1', 0o755)
        for i in range(1, 6):
            fd = cephfs.open(f'/dir1/file{i}', 'w', 0o755)
            cephfs.write(fd, b'abcd', 0)
            cephfs.close(fd)

        cephfs.chmod('/dir1', 0o000)
        # Errors are not expected from the call to this method. Therefore, set
        # suppress_errors to False so that tests abort as soon as any errors
        # occur.
        assert_raises(libcephfs.PermissionDenied, cephfs.rmtree, 'dir1',
                      should_cancel, suppress_errors=False)
        # ensure /dir1 wasn't deleted
        cephfs.stat('dir1')

        # cleanup
        cephfs.chmod('/dir1', 0o755)
        cephfs.rmtree('dir1', should_cancel)
        assert_raises(libcephfs.ObjectNotFound, cephfs.stat, 'dir1')

    def test_rmtree_on_tree_with_snaps(self, testdir):
        '''
        Test that rmtree() successfully deletes the entire file hierarchy except
        the branch where one of the directories contains one or many snapshots.
        '''
        should_cancel = lambda: False

        cephfs.mkdir('dir1', 0o755)
        cephfs.mkdir('dir1/dir2', 0o755)
        for i in range(1, 6):
            fd = cephfs.open(f'/dir1/dir2/file{i}', 'w', 0o755)
            cephfs.write(fd, b'abcd', 0)
            cephfs.close(fd)
        cephfs.mksnap('/dir1/dir2', 'snap1', 0o755)

        # Errors are not expected from the call to this method. Therefore, set
        # suppress_errors to False so that tests abort as soon as any errors
        # occur.
        cephfs.rmtree('dir1', should_cancel, suppress_errors=False)
        # ensure dir1 wasn't deleted
        cephfs.stat('dir1')

        # cleanup
        cephfs.rmsnap('/dir1/dir2', 'snap1')
        cephfs.rmtree('dir1', should_cancel)
        assert_raises(libcephfs.ObjectNotFound, cephfs.stat, 'dir1')

    def test_rmtree_on_tree_with_snaps_on_root(self, testdir):
        '''
        Test that rmtree() successfully deletes the entire file hierarchy except
        the branch where one of the directories contains one or many snapshots.
        '''
        should_cancel = lambda: False

        cephfs.mkdir('dir1', 0o755)
        for i in range(1, 6):
            fd = cephfs.open(f'/dir1/file{i}', 'w', 0o755)
            cephfs.write(fd, b'abcd', 0)
            cephfs.close(fd)
        cephfs.mksnap('/dir1', 'snap1', 0o755)

        # Errors are not expected from the call to this method. Therefore, set
        # suppress_errors to False so that tests abort as soon as any errors
        # occur.
        cephfs.rmtree('dir1', should_cancel, suppress_errors=False)
        # ensure dir1 wasn't deleted
        cephfs.stat('dir1')

        # cleanup
        cephfs.rmsnap('/dir1', 'snap1')
        cephfs.rmtree('dir1', should_cancel)
        assert_raises(libcephfs.ObjectNotFound, cephfs.stat, 'dir1')

    def get_file_count(self, dir_path):
        '''
        Return the number of files present in the given directory.
        '''
        i = 0
        with cephfs.opendir(dir_path) as dir_handle:
            de = cephfs.readdir(dir_handle)
            while de:
                if de.d_name not in (b'.', b'..'):
                    i += 1
                de = cephfs.readdir(dir_handle)
        return i

    def test_rmtree_aborts_when_should_cancel_is_true(self, testdir):
        '''
        Test that rmtree() stops deleting the file hierarchy when the return
        value of "should_cancel" becomes True.
        '''
        from threading import Event, Thread
        cancel_flag = Event()
        def should_cancel():
            time.sleep(0.1)
            return cancel_flag.is_set()

        # NOTE: this method is just a wrapper to provide an appropriate location
        # to catch the exception OpCanceled. If left uncaught the test passes
        # but pytest fails citing this exception.
        def rmtree(path, should_cancel, suppress_error=False):
            assert_raises(libcephfs.OpCanceled, cephfs.rmtree, path,
                          should_cancel, suppress_error)

        cephfs.mkdir('dir6', 0o755)
        for i in range(1, 101):
            fd = cephfs.open(f'/dir6/file{i}', 'w', 0o755)
            cephfs.write(fd, b'abcd', 0)
            cephfs.close(fd)

        # Errors are not expected from the call to this method. Therefore, set
        # suppress_errors to False so that tests abort as soon as any errors
        # occur.
        Thread(target=rmtree, args=('dir6', should_cancel, False)).start()
        time.sleep(1)

        # this will change return value of should_cancel and therefore halt
        # execution of rmtree()
        cancel_flag.set()
        # give a little time for cephs.rmtree() to catch the raised exception
        # due to cancel flag.
        time.sleep(0.1)
        # ensure dir6 wasn't deleted
        cephfs.stat('dir6')
        # ensure that deletion had begun but hadn't finished and was halted
        file_count = self.get_file_count('dir6')
        assert file_count > 0 and file_count < 100

        # ensure that deletion has made no progress since it was halted
        time.sleep(2)
        file_count = self.get_file_count('dir6')
        assert file_count > 0 and file_count < 100

        # cleanup

        # clear flag so that coming call to rmtree() doesn't cancel.
        cancel_flag.clear()
        cephfs.rmtree('dir6', should_cancel)
        assert_raises(libcephfs.ObjectNotFound, cephfs.stat, 'dir1')

    def test_rmtree_on_a_200_dir_broad_tree(self, testdir):
        '''
        Test that rmtree() successfully deletes a file hierarchy with 200
        subdirectories on the same level.
        '''
        should_cancel = lambda: False

        cephfs.mkdir('dir1', 0o755)
        cephfs.chdir('dir1')
        for i in range(1, 201):
            dirname = f'dir{i}'
            cephfs.mkdir(dirname, 0o755)
        cephfs.chdir('/')

        # Errors are not expected from the call to this method. Therefore, set
        # suppress_errors to False so that tests abort as soon as any errors
        # occur.
        cephfs.rmtree('dir1', should_cancel, suppress_errors=False)
        assert_raises(libcephfs.ObjectNotFound, cephfs.stat, 'dir1')

    def test_rmtree_on_a_2k_dir_broad_tree(self, testdir):
        '''
        Test that rmtree() successfully deletes a file hierarchy with 2000
        subdirectories on the same level.
        '''
        should_cancel = lambda: False

        cephfs.mkdir('dir1', 0o755)
        cephfs.chdir('dir1')
        for i in range(1, 2001):
            dirname = f'dir{i}'
            cephfs.mkdir(dirname, 0o755)
        cephfs.chdir('/')

        # Errors are not expected from the call to this method. Therefore, set
        # suppress_errors to False so that tests abort as soon as any errors
        # occur.
        cephfs.rmtree('dir1', should_cancel, suppress_errors=False)
        assert_raises(libcephfs.ObjectNotFound, cephfs.stat, 'dir1')

    def test_rmtree_on_a_200_dir_deep_tree(self, testdir):
        '''
        Test that rmtree() successfully deletes a file hierarchy with 2000
        levels.
        '''
        should_cancel = lambda: False

        for i in range(1, 201):
            dirname = f'dir{i}'
            cephfs.mkdir(dirname, 0o755)
            cephfs.chdir(dirname)
        cephfs.chdir('/')

        # Errors are not expected from the call to this method. Therefore, set
        # suppress_errors to False so that tests abort as soon as any errors
        # occur.
        cephfs.rmtree('dir1', should_cancel, suppress_errors=False)
        assert_raises(libcephfs.ObjectNotFound, cephfs.stat, 'dir1')

    def test_rmtree_on_a_2k_dir_deep_tree(self, testdir):
        '''
        Test that rmtree() successfully deletes a file hierarchy with 2000
        levels.
        '''
        should_cancel = lambda: False

        for i in range(1, 2001):
            dirname = f'dir{i}'
            cephfs.mkdir(dirname, 0o755)
            cephfs.chdir(dirname)
        cephfs.chdir('/')

        # Errors are not expected from the call to this method. Therefore, set
        # suppress_errors to False so that tests abort as soon as any errors
        # occur.
        cephfs.rmtree('dir1', should_cancel, suppress_errors=False)
        assert_raises(libcephfs.ObjectNotFound, cephfs.stat, 'dir1')

class TestFcopyfile:
    '''
    Tests for fcopyfile() method of CephFS Python bindings.
    '''

    PERMS = 0o755

    def get_perm_bits_from_stat_mode(self, mode):
        mode_oct = oct(mode)
        # get only last three digits, rest is not relevant
        perm_bits_str = mode_oct[-3:]
        # convert from str to octal literal value
        perms = int(perm_bits_str, 8)

        return perms

    def test_with_dir(self, testdir):
        '''
        Test that fcopyfile() create a new dir with expected mode.
        '''
        cephfs.mkdir('dir1', self.PERMS)

        cephfs.fcopyfile('dir1', 'dir2', self.PERMS)

        stat_result = cephfs.stat('dir2')
        perms = self.get_perm_bits_from_stat_mode(stat_result.st_mode)
        assert perms == self.PERMS

    def test_with_symlink(self, testdir):
        '''
        Test that fcopyfile() creates a new symlink with same size, mode and
        pointing to the same file.
        '''
        fd = cephfs.open(b'file1', 'w', self.PERMS)
        SIZE = 1000
        cephfs.write(fd, b'1' * SIZE, 0)
        cephfs.close(fd)
        cephfs.symlink('file1', 'slink1')

        cephfs.fcopyfile('slink1', 'slink2', self.PERMS)
        stat_result = cephfs.stat('slink2')
        assert stat_result.st_size == SIZE

        perms = self.get_perm_bits_from_stat_mode(stat_result.st_mode)
        assert perms == self.PERMS

        lstat_result1 = cephfs.stat('slink1')
        lstat_result2 = cephfs.stat('slink2')
        assert lstat_result1.st_size == lstat_result2.st_size

        path1 = cephfs.readlink('slink1', 4096)
        path2 = cephfs.readlink('slink2', 4096)
        assert path1 == path2

    def _write_file_and_test_fcopyfile(self, testdir, SIZE):
        fd = cephfs.open(b'file1', 'w', self.PERMS)
        cephfs.write(fd, b'1' * SIZE, 0)
        cephfs.close(fd)

        cephfs.fcopyfile('file1', 'file2', self.PERMS)

        stat_result = cephfs.stat('file2')
        assert stat_result.st_size == SIZE

        perms = self.get_perm_bits_from_stat_mode(stat_result.st_mode)
        assert perms == self.PERMS

    def test_with_regfile_of_size_0_5MB(self, testdir):
        '''
        Test that fcopyfile() copies a 0.5 MB file and then verifies its size and
        mode.
        '''
        self._write_file_and_test_fcopyfile(testdir, int(0.5 * 1000 * 1000))

    def test_with_regfile_of_size_2MB(self, testdir):
        '''
        Test that fcopyfile() copies a 2 MB file and then verifies its size and
        mode.
        '''
        self._write_file_and_test_fcopyfile(testdir, int(2 * 1000 * 1000))

    def test_with_regfile_of_size_1MB(self, testdir):
        '''
        Test that fcopyfile() copies a 1 MB file and then verifies its size and
        mode.
        '''
        self._write_file_and_test_fcopyfile(testdir, int(1000 * 1000))

    def test_with_regfile_of_size_2MiB(self, testdir):
        '''
        Test that fcopyfile() copies a 2 MiB file and then verifies its size and
        mode.
        '''
        self._write_file_and_test_fcopyfile(testdir, int(2 * 1024 * 1024))

    def test_with_regfile_of_size_1_15MB(self, testdir):
        '''
        Test that fcopyfile() copies a 1.15 MB file and then verifies its size and
        mode.
        '''
        self._write_file_and_test_fcopyfile(testdir, int(1.15 * 1000 * 1000))

    def test_with_regfile_of_size_1_5MB(self, testdir):
        '''
        Test that fcopyfile() copies a 1.5 MB file and then verifies its size and
        mode.
        '''
        self._write_file_and_test_fcopyfile(testdir, int(1.5 * 1000 * 1000))

    def test_with_regfile_of_size_1GB(self, testdir):
        '''
        Test that fcopyfile() copies a 1 GB file and then verifies its size and
        mode.
        '''
        self._write_file_and_test_fcopyfile(testdir, int(1 * 1000 * 1000 * 1000))
