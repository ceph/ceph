# vim: expandtab smarttab shiftwidth=4 softtabstop=4
import pytest
from assertions import assert_raises, assert_equal
import rgw as librgwfs

rgwfs = None
root_handler = None
root_dir_handler = None


def setup_module():
    global rgwfs
    global root_handler
    rgwfs = librgwfs.LibRGWFS("testid", "", "")
    root_handler = rgwfs.mount()


def teardown_module():
    global rgwfs
    rgwfs.shutdown()


@pytest.fixture
def testdir():
    global root_dir_handler

    names = []

    try:
        root_dir_handler = rgwfs.opendir(root_handler, b"bucket", 0)
    except Exception:
        root_dir_handler = rgwfs.mkdir(root_handler, b"bucket", 0)

    def cb(name, offset, flags):
        names.append(name)
    rgwfs.readdir(root_dir_handler, cb, 0, 0)
    for name in names:
        rgwfs.unlink(root_dir_handler, name, 0)


def test_version():
    rgwfs.version()


def test_fstat(testdir):
    stat = rgwfs.fstat(root_dir_handler)
    assert(len(stat) == 13)
    file_handler = rgwfs.create(root_dir_handler, b'file-1', 0)
    stat = rgwfs.fstat(file_handler)
    assert(len(stat) == 13)
    rgwfs.close(file_handler)


def test_statfs(testdir):
    stat = rgwfs.statfs()
    assert(len(stat) == 11)


def test_fsync(testdir):
    fd = rgwfs.create(root_dir_handler, b'file-1', 0)
    rgwfs.write(fd, 0, b"asdf")
    rgwfs.fsync(fd, 0)
    rgwfs.write(fd, 4, b"qwer")
    rgwfs.fsync(fd, 1)
    rgwfs.close(fd)


def test_directory(testdir):
    dir_handler = rgwfs.mkdir(root_dir_handler, b"temp-directory", 0)
    rgwfs.close(dir_handler)
    rgwfs.unlink(root_dir_handler, b"temp-directory")


def test_walk_dir(testdir):
    dirs = [b"dir-1", b"dir-2", b"dir-3"]
    handles = []
    for i in dirs:
        d = rgwfs.mkdir(root_dir_handler, i, 0)
        handles.append(d)
    entries = []

    def cb(name, offset):
        entries.append((name, offset))

    offset, eof = rgwfs.readdir(root_dir_handler, cb, 0)

    for i in handles:
        rgwfs.close(i)

    for name, _ in entries:
        assert(name in dirs)
        rgwfs.unlink(root_dir_handler, name)


def test_rename(testdir):
    file_handler = rgwfs.create(root_dir_handler, b"a", 0)
    rgwfs.close(file_handler)
    rgwfs.rename(root_dir_handler, b"a", root_dir_handler, b"b")
    file_handler = rgwfs.open(root_dir_handler, b"b", 0)
    rgwfs.fstat(file_handler)
    rgwfs.close(file_handler)
    rgwfs.unlink(root_dir_handler, b"b")


def test_open(testdir):
    assert_raises(librgwfs.ObjectNotFound, rgwfs.open,
                  root_dir_handler, b'file-1', 0)
    assert_raises(librgwfs.ObjectNotFound, rgwfs.open,
                  root_dir_handler, b'file-1', 0)
    fd = rgwfs.create(root_dir_handler, b'file-1', 0)
    rgwfs.write(fd, 0, b"asdf")
    rgwfs.close(fd)
    fd = rgwfs.open(root_dir_handler, b'file-1', 0)
    assert_equal(rgwfs.read(fd, 0, 4), b"asdf")
    rgwfs.close(fd)
    fd = rgwfs.open(root_dir_handler, b'file-1', 0)
    rgwfs.write(fd, 0, b"aaaazxcv")
    rgwfs.close(fd)
    fd = rgwfs.open(root_dir_handler, b'file-1', 0)
    assert_equal(rgwfs.read(fd, 4, 4), b"zxcv")
    rgwfs.close(fd)
    fd = rgwfs.open(root_dir_handler, b'file-1', 0)
    assert_equal(rgwfs.read(fd, 0, 4), b"aaaa")
    rgwfs.close(fd)
    rgwfs.unlink(root_dir_handler, b"file-1")


def test_mount_unmount(testdir):
    global root_handler
    global root_dir_handler
    test_directory()
    rgwfs.close(root_dir_handler)
    rgwfs.close(root_handler)
    rgwfs.unmount()
    root_handler = rgwfs.mount()
    root_dir_handler = rgwfs.opendir(root_handler, b"bucket", 0)
    test_open()
