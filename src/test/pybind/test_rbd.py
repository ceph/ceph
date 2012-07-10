import random
import struct
import os

from nose import with_setup
from nose.tools import eq_ as eq, assert_raises
from rados import Rados
from rbd import (RBD, Image, ImageNotFound, InvalidArgument, ImageExists,
                 ImageBusy, ImageHasSnapshots, RBD_FEATURE_LAYERING)


rados = None
ioctx = None
IMG_NAME = 'foo'
IMG_SIZE = 8 << 20 # 8 MiB
IMG_ORDER = 22 # 4 MiB objects

def setUp():
    global rados
    rados = Rados(conffile='')
    rados.connect()
    assert rados.pool_exists('rbd')
    global ioctx
    ioctx = rados.open_ioctx('rbd')

def tearDown():
    global ioctx
    ioctx.__del__()
    global rados
    rados.shutdown()

def create_image():
    global features
    features = os.getenv("RBD_FEATURES")
    if features is not None:
        features = int(features)
        RBD().create(ioctx, IMG_NAME, IMG_SIZE, IMG_ORDER, old_format=False,
                     features=int(features))
    else:
        RBD().create(ioctx, IMG_NAME, IMG_SIZE, IMG_ORDER, old_format=True)

def remove_image():
    RBD().remove(ioctx, IMG_NAME)
    
def test_version():
    RBD().version()

def test_create():
    create_image()
    remove_image()

def test_context_manager():
    with Rados(conffile='') as cluster:
        with cluster.open_ioctx('rbd') as ioctx:
            RBD().create(ioctx, IMG_NAME, IMG_SIZE)
            with Image(ioctx, IMG_NAME) as image:
                data = rand_data(256)
                image.write(data, 0)
                read = image.read(0, 256)
            RBD().remove(ioctx, IMG_NAME)
            eq(data, read)

def test_remove_dne():
    assert_raises(ImageNotFound, remove_image)

def test_list_empty():
    eq([], RBD().list(ioctx))

@with_setup(create_image, remove_image)
def test_list():
    eq([IMG_NAME], RBD().list(ioctx))

@with_setup(create_image, remove_image)
def test_rename():
    rbd = RBD()
    rbd.rename(ioctx, IMG_NAME, IMG_NAME + '2')
    eq([IMG_NAME + '2'], rbd.list(ioctx))
    rbd.rename(ioctx, IMG_NAME + '2', IMG_NAME)
    eq([IMG_NAME], rbd.list(ioctx))

def rand_data(size):
    l = [random.Random().getrandbits(64) for _ in xrange(size/8)]
    return struct.pack((size/8)*'Q', *l)

def check_stat(info, size, order):
    assert 'block_name_prefix' in info
    eq(info['size'], size)
    eq(info['order'], order)
    eq(info['num_objs'], size / (1 << order))
    eq(info['obj_size'], 1 << order)

class TestImage(object):

    def setUp(self):
        self.rbd = RBD()
        create_image()
        self.image = Image(ioctx, IMG_NAME)

    def tearDown(self):
        self.image.close()
        remove_image()

    def test_stat(self):
        info = self.image.stat()
        check_stat(info, IMG_SIZE, IMG_ORDER)

    def test_write(self):
        data = rand_data(256)
        self.image.write(data, 0)

    def test_read(self):
        data = self.image.read(0, 20)
        eq(data, '\0' * 20)

    def test_large_write(self):
        data = rand_data(IMG_SIZE)
        self.image.write(data, 0)

    def test_large_read(self):
        data = self.image.read(0, IMG_SIZE)
        eq(data, '\0' * IMG_SIZE)

    def test_write_read(self):
        data = rand_data(256)
        offset = 50
        self.image.write(data, offset)
        read = self.image.read(offset, 256)
        eq(data, read)

    def test_read_bad_offset(self):
        assert_raises(InvalidArgument, self.image.read, IMG_SIZE + 1, IMG_SIZE)

    def test_resize(self):
        new_size = IMG_SIZE * 2
        self.image.resize(new_size)
        info = self.image.stat()
        check_stat(info, new_size, IMG_ORDER)

    def test_resize_down(self):
        new_size = IMG_SIZE / 2
        data = rand_data(256)
        self.image.write(data, IMG_SIZE / 2);
        self.image.resize(new_size)
        self.image.resize(IMG_SIZE)
        read = self.image.read(IMG_SIZE / 2, 256)
        eq('\0' * 256, read)

    def test_resize_bytes(self):
        new_size = IMG_SIZE / 2 - 5
        data = rand_data(256)
        self.image.write(data, IMG_SIZE / 2 - 10);
        self.image.resize(new_size)
        self.image.resize(IMG_SIZE)
        read = self.image.read(IMG_SIZE / 2 - 10, 5)
        eq(data[:5], read)
        read = self.image.read(IMG_SIZE / 2 - 5, 251)
        eq('\0' * 251, read)

    def test_copy(self):
        global ioctx
        data = rand_data(256)
        self.image.write(data, 256)
        self.image.copy(ioctx, IMG_NAME + '2')
        assert_raises(ImageExists, self.image.copy, ioctx, IMG_NAME + '2')
        copy = Image(ioctx, IMG_NAME + '2')
        copy_data = copy.read(256, 256)
        copy.close()
        self.rbd.remove(ioctx, IMG_NAME + '2')
        eq(data, copy_data)

    def test_create_snap(self):
        global ioctx
        self.image.create_snap('snap1')
        read = self.image.read(0, 256)
        eq(read, '\0' * 256)
        data = rand_data(256)
        self.image.write(data, 0)
        read = self.image.read(0, 256)
        eq(read, data)
        at_snapshot = Image(ioctx, IMG_NAME, 'snap1')
        snap_data = at_snapshot.read(0, 256)
        at_snapshot.close()
        eq(snap_data, '\0' * 256)
        self.image.remove_snap('snap1')

    def test_list_snaps(self):
        eq([], list(self.image.list_snaps()))
        self.image.create_snap('snap1')
        eq(['snap1'], map(lambda snap: snap['name'], self.image.list_snaps()))
        self.image.create_snap('snap2')
        eq(['snap1', 'snap2'], map(lambda snap: snap['name'], self.image.list_snaps()))
        self.image.remove_snap('snap1')
        self.image.remove_snap('snap2')

    def test_remove_snap(self):
        eq([], list(self.image.list_snaps()))
        self.image.create_snap('snap1')
        eq(['snap1'], map(lambda snap: snap['name'], self.image.list_snaps()))
        self.image.remove_snap('snap1')
        eq([], list(self.image.list_snaps()))

    def test_remove_with_snap(self):
        self.image.create_snap('snap1')
        assert_raises(ImageHasSnapshots, remove_image)
        self.image.remove_snap('snap1')

    def test_remove_with_watcher(self):
        assert_raises(ImageBusy, remove_image)

    def test_rollback_to_snap(self):
        self.image.write('\0' * 256, 0)
        self.image.create_snap('snap1')
        read = self.image.read(0, 256)
        eq(read, '\0' * 256)
        data = rand_data(256)
        self.image.write(data, 0)
        read = self.image.read(0, 256)
        eq(read, data)
        self.image.rollback_to_snap('snap1')
        read = self.image.read(0, 256)
        eq(read, '\0' * 256)
        self.image.remove_snap('snap1')

    def test_rollback_to_snap_sparse(self):
        self.image.create_snap('snap1')
        read = self.image.read(0, 256)
        eq(read, '\0' * 256)
        data = rand_data(256)
        self.image.write(data, 0)
        read = self.image.read(0, 256)
        eq(read, data)
        self.image.rollback_to_snap('snap1')
        read = self.image.read(0, 256)
        eq(read, '\0' * 256)
        self.image.remove_snap('snap1')

    def test_rollback_with_resize(self):
        read = self.image.read(0, 256)
        eq(read, '\0' * 256)
        data = rand_data(256)
        self.image.write(data, 0)
        self.image.create_snap('snap1')
        read = self.image.read(0, 256)
        eq(read, data)
        new_size = IMG_SIZE * 2
        self.image.resize(new_size)
        check_stat(self.image.stat(), new_size, IMG_ORDER)
        self.image.write(data, new_size - 256)
        self.image.create_snap('snap2')
        read = self.image.read(new_size - 256, 256)
        eq(read, data)
        self.image.rollback_to_snap('snap1')
        check_stat(self.image.stat(), IMG_SIZE, IMG_ORDER)
        assert_raises(InvalidArgument, self.image.read, new_size - 256, 256)
        self.image.rollback_to_snap('snap2')
        check_stat(self.image.stat(), new_size, IMG_ORDER)
        read = self.image.read(new_size - 256, 256)
        eq(read, data)
        self.image.remove_snap('snap1')
        self.image.remove_snap('snap2')

    def test_set_snap(self):
        self.image.write('\0' * 256, 0)
        self.image.create_snap('snap1')
        read = self.image.read(0, 256)
        eq(read, '\0' * 256)
        data = rand_data(256)
        self.image.write(data, 0)
        read = self.image.read(0, 256)
        eq(read, data)
        self.image.set_snap('snap1')
        read = self.image.read(0, 256)
        eq(read, '\0' * 256)
        self.image.remove_snap('snap1')

    def test_set_no_snap(self):
        self.image.write('\0' * 256, 0)
        self.image.create_snap('snap1')
        read = self.image.read(0, 256)
        eq(read, '\0' * 256)
        data = rand_data(256)
        self.image.write(data, 0)
        read = self.image.read(0, 256)
        eq(read, data)
        self.image.set_snap('snap1')
        read = self.image.read(0, 256)
        eq(read, '\0' * 256)
        self.image.set_snap(None)
        read = self.image.read(0, 256)
        eq(read, data)
        self.image.remove_snap('snap1')

    def test_set_snap_sparse(self):
        self.image.create_snap('snap1')
        read = self.image.read(0, 256)
        eq(read, '\0' * 256)
        data = rand_data(256)
        self.image.write(data, 0)
        read = self.image.read(0, 256)
        eq(read, data)
        self.image.set_snap('snap1')
        read = self.image.read(0, 256)
        eq(read, '\0' * 256)
        self.image.remove_snap('snap1')

    def test_many_snaps(self):
        num_snaps = 200
        for i in xrange(num_snaps):
            self.image.create_snap(str(i))
        snaps = sorted(self.image.list_snaps(),
                       key=lambda snap: int(snap['name']))
        eq(len(snaps), num_snaps)
        for i, snap in enumerate(snaps):
            eq(snap['size'], IMG_SIZE)
            eq(snap['name'], str(i))
        for i in xrange(num_snaps):
            self.image.remove_snap(str(i))

    def test_set_snap_deleted(self):
        self.image.write('\0' * 256, 0)
        self.image.create_snap('snap1')
        read = self.image.read(0, 256)
        eq(read, '\0' * 256)
        data = rand_data(256)
        self.image.write(data, 0)
        read = self.image.read(0, 256)
        eq(read, data)
        self.image.set_snap('snap1')
        self.image.remove_snap('snap1')
        assert_raises(ImageNotFound, self.image.read, 0, 256)
        self.image.set_snap(None)
        read = self.image.read(0, 256)
        eq(read, data)

    def test_set_snap_recreated(self):
        self.image.write('\0' * 256, 0)
        self.image.create_snap('snap1')
        read = self.image.read(0, 256)
        eq(read, '\0' * 256)
        data = rand_data(256)
        self.image.write(data, 0)
        read = self.image.read(0, 256)
        eq(read, data)
        self.image.set_snap('snap1')
        self.image.remove_snap('snap1')
        self.image.create_snap('snap1')
        assert_raises(ImageNotFound, self.image.read, 0, 256)
        self.image.set_snap(None)
        read = self.image.read(0, 256)
        eq(read, data)
        self.image.remove_snap('snap1')

    def test_clone(self):
        if features is None or (features & RBD_FEATURE_LAYERING) == 0:
            return 0
        self.image.create_snap('snap1')
        RBD().clone(ioctx, IMG_NAME, 'snap1', ioctx, 'clone', features)
        with Image(ioctx, 'clone') as clone:
            image_info = self.image.stat()
            clone_info = clone.stat()
            eq(clone_info['size'], image_info['size'])
            eq(clone_info['size'], clone.overlap())
        RBD().remove(ioctx, 'clone')
        self.image.remove_snap('snap1')

    def test_clone_resize(self):
        if features is None or (features & RBD_FEATURE_LAYERING) == 0:
            return 0
        self.image.create_snap('snap1')
        RBD().clone(ioctx, IMG_NAME, 'snap1', ioctx, 'clone', features)
        with Image(ioctx, 'clone') as clone:
            image_info = self.image.stat()
            clone_info = clone.stat()
            eq(clone_info['size'], image_info['size'])
            eq(clone_info['size'], clone.overlap())

            clone.resize(IMG_SIZE / 2)
            image_info = self.image.stat()
            clone_info = clone.stat()
            eq(clone_info['size'], IMG_SIZE / 2)
            eq(image_info['size'], IMG_SIZE)
            eq(clone.overlap(), IMG_SIZE / 2)

            clone.resize(IMG_SIZE * 2)
            image_info = self.image.stat()
            clone_info = clone.stat()
            eq(clone_info['size'], IMG_SIZE * 2)
            eq(image_info['size'], IMG_SIZE)
            eq(clone.overlap(), IMG_SIZE / 2)

        RBD().remove(ioctx, 'clone')
        self.image.remove_snap('snap1')
