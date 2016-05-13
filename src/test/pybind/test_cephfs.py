# vim: expandtab smarttab shiftwidth=4 softtabstop=4
from nose.tools import assert_raises, assert_equal, with_setup
import cephfs as libcephfs
import fcntl

cephfs = None

def setup_module():
    global cephfs
    cephfs = libcephfs.LibCephFS(conffile='')
    cephfs.mount()

def teardown_module():
    global cephfs
    cephfs.shutdown()

def setup_test():
    d = cephfs.opendir("/")
    dent = cephfs.readdir(d)
    while dent:
        if (dent.d_name not in [".", ".."]):
            if dent.is_dir():
                cephfs.rmdir("/{0}".format(dent.d_name))
            else:
                cephfs.unlink("/{0}".format(dent.d_name))

        dent = cephfs.readdir(d)

    cephfs.closedir(d)

    cephfs.chdir("/")

@with_setup(setup_test)
def test_conf_get():
    fsid = cephfs.conf_get("fsid")
    assert(fsid != "")

@with_setup(setup_test)
def test_version():
    cephfs.version()

@with_setup(setup_test)
def test_statfs():
    stat = cephfs.statfs('/')
    assert(len(stat) == 11)

@with_setup(setup_test)
def test_syncfs():
    stat = cephfs.sync_fs()

@with_setup(setup_test)
def test_fsync():
    fd = cephfs.open('file-1', 'w', 0755)
    cephfs.write(fd, "asdf", 0)
    stat = cephfs.fsync(fd, 0)
    cephfs.write(fd, "qwer", 0)
    stat = cephfs.fsync(fd, 1)
    cephfs.close(fd)
    #sync on non-existing fd (assume fd 12345 is not exists)
    assert_raises(libcephfs.Error, cephfs.fsync, 12345, 0)

@with_setup(setup_test)
def test_directory():
    cephfs.mkdir("/temp-directory", 0755)
    cephfs.mkdirs("/temp-directory/foo/bar", 0755)
    cephfs.chdir("/temp-directory")
    assert_equal(cephfs.getcwd(), "/temp-directory")
    cephfs.rmdir("/temp-directory/foo/bar")
    cephfs.rmdir("/temp-directory/foo")
    cephfs.rmdir("/temp-directory")
    assert_raises(libcephfs.ObjectNotFound, cephfs.chdir, "/temp-directory")

@with_setup(setup_test)
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

@with_setup(setup_test)
def test_xattr():
    assert_raises(libcephfs.OperationNotSupported, cephfs.setxattr, "/", "key", "value", 0)
    cephfs.setxattr("/", "user.key", "value", 0)
    assert_equal("value", cephfs.getxattr("/", "user.key"))

    cephfs.setxattr("/", "user.big", "x" * 300, 0)

    # Default size is 255, get ERANGE
    assert_raises(libcephfs.OutOfRange, cephfs.getxattr, "/", "user.big")

    # Pass explicit size, and we'll get the value
    assert_equal(300, len(cephfs.getxattr("/", "user.big", 300)))


@with_setup(setup_test)
def test_rename():
    cephfs.mkdir("/a", 0755)
    cephfs.mkdir("/a/b", 0755)
    cephfs.rename("/a", "/b")
    cephfs.stat("/b/b")
    cephfs.rmdir("/b/b")
    cephfs.rmdir("/b")

@with_setup(setup_test)
def test_open():
    assert_raises(libcephfs.ObjectNotFound, cephfs.open, 'file-1', 'r')
    assert_raises(libcephfs.ObjectNotFound, cephfs.open, 'file-1', 'r+')
    fd = cephfs.open('file-1', 'w', 0755)
    cephfs.write(fd, "asdf", 0)
    cephfs.close(fd)
    fd = cephfs.open('file-1', 'r', 0755)
    assert_equal(cephfs.read(fd, 0, 4), "asdf")
    cephfs.close(fd)
    fd = cephfs.open('file-1', 'r+', 0755)
    cephfs.write(fd, "zxcv", 4)
    assert_equal(cephfs.read(fd, 4, 8), "zxcv")
    cephfs.close(fd)
    fd = cephfs.open('file-1', 'w+', 0755)
    assert_equal(cephfs.read(fd, 0, 4), "")
    cephfs.write(fd, "zxcv", 4)
    assert_equal(cephfs.read(fd, 4, 8), "zxcv")
    cephfs.close(fd)
    assert_raises(libcephfs.OperationNotSupported, cephfs.open, 'file-1', 'a')
    cephfs.unlink('file-1')

@with_setup(setup_test)
def test_symlink():
    fd = cephfs.open('file-1', 'w', 0755)
    cephfs.write(fd, "1111", 0)
    cephfs.close(fd)
    cephfs.symlink('file-1', 'file-2')
    fd = cephfs.open('file-2', 'r', 0755)
    assert_equal(cephfs.read(fd, 0, 4), "1111")
    cephfs.close(fd)
    fd = cephfs.open('file-2', 'r+', 0755)
    cephfs.write(fd, "2222", 4)
    cephfs.close(fd)
    fd = cephfs.open('file-1', 'r', 0755)
    assert_equal(cephfs.read(fd, 0, 8), "11112222")
    cephfs.close(fd)
    cephfs.unlink('file-2')

@with_setup(setup_test)
def test_delete_cwd():
    assert_equal("/", cephfs.getcwd())

    cephfs.mkdir("/temp-directory", 0755)
    cephfs.chdir("/temp-directory")
    cephfs.rmdir("/temp-directory")

    # getcwd gives you something stale here: it remembers the path string
    # even when things are unlinked.  It's up to the caller to find out
    # whether it really still exists
    assert_equal("/temp-directory", cephfs.getcwd())

@with_setup(setup_test)
def test_flock():
    fd = cephfs.open('file-1', 'w', 0755)

    cephfs.flock(fd, fcntl.LOCK_EX, 123);
    fd2 = cephfs.open('file-1', 'w', 0755)

    assert_raises(libcephfs.WouldBlock, cephfs.flock, fd2,
                  fcntl.LOCK_EX | fcntl.LOCK_NB, 456);
    cephfs.close(fd2)

    cephfs.close(fd)
