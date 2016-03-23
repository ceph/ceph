# vim: expandtab smarttab shiftwidth=4 softtabstop=4
from nose.tools import assert_raises, assert_equal
import cephfs as libcephfs

cephfs = None


def setup_module():
    global cephfs
    cephfs = libcephfs.LibCephFS(conffile='')
    cephfs.mount()


def teardown_module():
    global cephfs
    cephfs.shutdown()


def test_conf_get():
    fsid = cephfs.conf_get("fsid")
    assert(fsid != "")


def test_version():
    cephfs.version()


def test_statfs():
    stat = cephfs.statfs('/')
    assert(len(stat) == 11)


def test_syncfs():
    stat = cephfs.sync_fs()


def test_directory():
    cephfs.mkdir("/temp-directory", 0755)
    cephfs.mkdirs("/temp-directory/foo/bar", 0755)
    cephfs.chdir("/temp-directory")
    assert_equal(cephfs.getcwd(), "/temp-directory")
    cephfs.rmdir("/temp-directory/foo/bar")
    cephfs.rmdir("/temp-directory/foo")
    cephfs.rmdir("/temp-directory")
    assert_raises(libcephfs.ObjectNotFound, cephfs.chdir, "/temp-directory")


def test_walk_dir():
    cephfs.chdir("/")
    dirs = ["dir-1", "dir-2", "dir-3"]
    for i in dirs:
        cephfs.mkdir(i, 0755)
    handler = cephfs.opendir("/")
    d = cephfs.readdir(handler)
    dirs += [".", ".."]
    while d:
        assert(d.d_name in dirs)
        dirs.remove(d.d_name)
        d = cephfs.readdir(handler)
    assert(len(dirs) == 0)
    dirs = ["/dir-1", "/dir-2", "/dir-3"]
    for i in dirs:
        cephfs.rmdir(i)
    cephfs.closedir(handler)


def test_xattr():
    assert_raises(libcephfs.OperationNotSupported, cephfs.setxattr, "/", "key", "value", 0)
    cephfs.setxattr("/", "user.key", "value", 0)
    assert_equal("value", cephfs.getxattr("/", "user.key"))
    cephfs.setxattr("/", "user.big", "" * 300, 0)
    # NOTE(sileht): this actually doesn't work, cephfs returns errno -34
    # when we retrieve big xattr value, at least on my setup
    # The old python binding was not checking the return code and was
    # returning a empty string in that case.
    assert_equal(300, len(cephfs.getxattr("/", "user.big")))


def test_rename():
    cephfs.mkdir("/a", 0755)
    cephfs.mkdir("/a/b", 0755)
    cephfs.rename("/a", "/b")
    cephfs.stat("/b/b")
    cephfs.rmdir("/b/b")
    cephfs.rmdir("/b")


def test_open():
    assert_raises(libcephfs.ObjectNotFound, cephfs.open, 'file-1', 'r')
    assert_raises(libcephfs.ObjectNotFound, cephfs.open, 'file-1', 'r+')
    fd = cephfs.open('file-1', 'w')
    cephfs.write(fd, "asdf", 0)
    cephfs.close(fd)
    fd = cephfs.open('file-1', 'r')
    assert_equal(cephfs.read(fd, 0, 4), "asdf")
    cephfs.close(fd)
    fd = cephfs.open('file-1', 'r+')
    cephfs.write(fd, "zxcv", 4)
    assert_equal(cephfs.read(fd, 4, 8), "zxcv")
    cephfs.close(fd)
    fd = cephfs.open('file-1', 'w+')
    assert_equal(cephfs.read(fd, 0, 4), "")
    cephfs.write(fd, "zxcv", 4)
    assert_equal(cephfs.read(fd, 4, 8), "zxcv")
    cephfs.close(fd)
    assert_raises(libcephfs.OperationNotSupported, cephfs.open, 'file-1', 'a')
    cephfs.unlink('file-1')


def test_symlink():
    fd = cephfs.open('file-1', 'w')
    cephfs.write(fd, "1111", 0)
    cephfs.close(fd)
    cephfs.symlink('file-1', 'file-2')
    fd = cephfs.open('file-2', 'r')
    assert_equal(cephfs.read(fd, 0, 4), "1111")
    cephfs.close(fd)
    fd = cephfs.open('file-2', 'r+')
    cephfs.write(fd, "2222", 4)
    cephfs.close(fd)
    fd = cephfs.open('file-1', 'r')
    assert_equal(cephfs.read(fd, 0, 8), "11112222")
    cephfs.close(fd)
    cephfs.unlink('file-2')
