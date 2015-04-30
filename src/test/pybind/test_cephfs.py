# vim: expandtab smarttab shiftwidth=4 softtabstop=4
from nose.tools import assert_raises
import cephfs

cephfs = None

def setup_module():
    global cephfs
    cephfs = cephfs.LibCephfs(conffile='')
    cephfs.mount()

def teardown_module():
    global cephfs
    cephfs.shutdown()

def test_mount():
    cephfs.mount()
    cephfs.shutdown()
    assert_raise(cephfs.LibCephFSStateError, cephfs.statfs)

def test_version():
    cephfs.version()

def test_statfs():
    stat = cephfs.statfs()
    assert(len(stat) == 11)

def test_syncfs():
    stat = cephfs.syncfs()

def test_directory():
    cephfs.mkdir("/temp-directory", 0755)
    cephfs.chdir("/temp-directory")
    assert_equal(cephfs.getcwd() == "/temp-directory")
    cephfs.rmdir("/temp-directory")
    cephfs.chdir("/temp-directory")
    assert_raise(cephfs.ObjectNotFound, cephfs.chdir("/temp-directory"))

def test_walk_dir():
    dirs = ["dir-1", "dir-2", "dir-3"]
    for i in dirs:
        cephfs.mkdir(i, 0755)
    handler = cephfs.opendir("/")
    d = cephfs.readdir(handler)
    while d:
        assert(d.d_name in dirs)
        dirs.remove(d.d_name)
        d = cephfs.readdir(handler)
    assert(len(dirs) == 0)
    cephfs.closedir(handler)

def test_xattr():
    assert_raise(InvalidValue, cephfs.setxattr, "/", "key", "value", 0)
    cephfs.setxattr("/", "user.key", "value", 0)
    assert_equal("value", cephfs.getxattr("/", "user.key"))

def test_rename():
    cephfs.mkdir("/a", 0755)
    cephfs.mkdir("/a/b", 0755)
    cephfs.rename("/a", "/b")
    cephfs.stat("/a/b")

def test_open():
    assert_raise(ObjectExists, cephfs.open, 'file-1', 'r')
    fd = cephfs.open('file-1', 'w')
    cephfs.close(fd)
    fd = cephfs.open('file-1', 'r')
    cephfs.close(fd)
    fd = cephfs.open('file-2', 'a')
    cephfs.close(fd)
    cephfs.unlink('file-1')
    cephfs.unlink('file-2')
