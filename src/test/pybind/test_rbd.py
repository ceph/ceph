# vim: expandtab smarttab shiftwidth=4 softtabstop=4
import functools
import socket
import os
import time

from nose import with_setup, SkipTest
from nose.tools import eq_ as eq, assert_raises
from rados import (Rados,
                   LIBRADOS_OP_FLAG_FADVISE_DONTNEED,
                   LIBRADOS_OP_FLAG_FADVISE_NOCACHE,
                   LIBRADOS_OP_FLAG_FADVISE_RANDOM)
from rbd import (RBD, Image, ImageNotFound, InvalidArgument, ImageExists,
                 ImageBusy, ImageHasSnapshots, ReadOnlyImage,
                 FunctionNotSupported, ArgumentOutOfRange,
                 RBD_FEATURE_LAYERING, RBD_FEATURE_STRIPINGV2,
                 RBD_FEATURE_EXCLUSIVE_LOCK)

rados = None
ioctx = None
features = None
image_idx = 0
image_name = None
pool_idx = 0
pool_name = None
IMG_SIZE = 8 << 20  # 8 MiB
IMG_ORDER = 22  # 4 MiB objects


def setup_module():
    global rados
    rados = Rados(conffile='')
    rados.connect()
    global pool_name
    pool_name = get_temp_pool_name()
    rados.create_pool(pool_name)
    global ioctx
    ioctx = rados.open_ioctx(pool_name)
    global features
    features = os.getenv("RBD_FEATURES")
    features = int(features) if features is not None else 61


def teardown_module():
    global ioctx
    ioctx.__del__()
    global rados
    rados.delete_pool(pool_name)
    rados.shutdown()


def get_temp_pool_name():
    global pool_idx
    pool_idx += 1
    return "test-rbd-api-" + socket.gethostname() + '-' + str(os.getpid()) + \
           '-' + str(pool_idx)


def get_temp_image_name():
    global image_idx
    image_idx += 1
    return "image" + str(image_idx)


def create_image():
    global image_name
    image_name = get_temp_image_name()
    if features is not None:
        RBD().create(ioctx, image_name, IMG_SIZE, IMG_ORDER, old_format=False,
                     features=int(features))
    else:
        RBD().create(ioctx, image_name, IMG_SIZE, IMG_ORDER, old_format=True)


def remove_image():
    if image_name is not None:
        RBD().remove(ioctx, image_name)


def require_new_format():
    def wrapper(fn):
        def _require_new_format(*args, **kwargs):
            global features
            if features is None:
                raise SkipTest
            return fn(*args, **kwargs)
        return functools.wraps(fn)(_require_new_format)
    return wrapper


def require_features(required_features):
    def wrapper(fn):
        def _require_features(*args, **kwargs):
            global features
            if features is None:
                raise SkipTest
            for feature in required_features:
                if feature & features != feature:
                    raise SkipTest
            return fn(*args, **kwargs)
        return functools.wraps(fn)(_require_features)
    return wrapper


def blacklist_features(blacklisted_features):
    def wrapper(fn):
        def _blacklist_features(*args, **kwargs):
            global features
            for feature in blacklisted_features:
                if features is not None and feature & features == feature:
                    raise SkipTest
            return fn(*args, **kwargs)
        return functools.wraps(fn)(_blacklist_features)
    return wrapper


def test_version():
    RBD().version()


def test_create():
    create_image()
    remove_image()


def check_default_params(format, order=None, features=None, stripe_count=None,
                         stripe_unit=None, exception=None):
    global rados
    global ioctx
    orig_vals = {}
    for k in ['rbd_default_format', 'rbd_default_order', 'rbd_default_features',
              'rbd_default_stripe_count', 'rbd_default_stripe_unit']:
        orig_vals[k] = rados.conf_get(k)
    try:
        rados.conf_set('rbd_default_format', str(format))
        if order is not None:
            rados.conf_set('rbd_default_order', str(order or 0))
        if features is not None:
            rados.conf_set('rbd_default_features', str(features or 0))
        if stripe_count is not None:
            rados.conf_set('rbd_default_stripe_count', str(stripe_count or 0))
        if stripe_unit is not None:
            rados.conf_set('rbd_default_stripe_unit', str(stripe_unit or 0))
        image_name = get_temp_image_name()
        if exception is None:
            RBD().create(ioctx, image_name, IMG_SIZE)
            try:
                with Image(ioctx, image_name) as image:
                    eq(format == 1, image.old_format())

                    expected_order = int(rados.conf_get('rbd_default_order'))
                    actual_order = image.stat()['order']
                    eq(expected_order, actual_order)

                    expected_features = features
                    if expected_features is None or format == 1:
                        expected_features = 0 if format == 1 else 61
                    eq(expected_features, image.features())

                    expected_stripe_count = stripe_count
                    if not expected_stripe_count or format == 1 or \
                            features & RBD_FEATURE_STRIPINGV2 == 0:
                        expected_stripe_count = 1
                    eq(expected_stripe_count, image.stripe_count())

                    expected_stripe_unit = stripe_unit
                    if not expected_stripe_unit or format == 1 or \
                            features & RBD_FEATURE_STRIPINGV2 == 0:
                        expected_stripe_unit = 1 << actual_order
                    eq(expected_stripe_unit, image.stripe_unit())
            finally:
                RBD().remove(ioctx, image_name)
        else:
            assert_raises(exception, RBD().create, ioctx, image_name, IMG_SIZE)
    finally:
        for k, v in orig_vals.items():
            rados.conf_set(k, v)


def test_create_defaults():
    # basic format 1 and 2
    check_default_params(1)
    check_default_params(2)
    # invalid order
    check_default_params(1, 0, exception=ArgumentOutOfRange)
    check_default_params(2, 0, exception=ArgumentOutOfRange)
    check_default_params(1, 11, exception=ArgumentOutOfRange)
    check_default_params(2, 11, exception=ArgumentOutOfRange)
    check_default_params(1, 65, exception=ArgumentOutOfRange)
    check_default_params(2, 65, exception=ArgumentOutOfRange)
    # striping and features are ignored for format 1
    check_default_params(1, 20, 0, 1, 1)
    check_default_params(1, 20, 3, 1, 1)
    check_default_params(1, 20, 0, 0, 0)
    # striping is ignored if stripingv2 is not set
    check_default_params(2, 20, 0, 1, 1 << 20)
    check_default_params(2, 20, RBD_FEATURE_LAYERING, 1, 1 << 20)
    check_default_params(2, 20, 0, 0, 0)
    # striping with stripingv2 is fine
    check_default_params(2, 20, RBD_FEATURE_STRIPINGV2, 1, 1 << 16)
    check_default_params(2, 20, RBD_FEATURE_STRIPINGV2, 10, 1 << 20)
    check_default_params(2, 20, RBD_FEATURE_STRIPINGV2, 10, 1 << 16)
    check_default_params(2, 20, RBD_FEATURE_STRIPINGV2, 0, 0)
    # make sure invalid combinations of stripe unit and order are still invalid
    check_default_params(2, 22, RBD_FEATURE_STRIPINGV2, 10, 1 << 50, exception=InvalidArgument)
    check_default_params(2, 22, RBD_FEATURE_STRIPINGV2, 10, 100, exception=InvalidArgument)
    check_default_params(2, 22, RBD_FEATURE_STRIPINGV2, 0, 1, exception=InvalidArgument)
    check_default_params(2, 22, RBD_FEATURE_STRIPINGV2, 1, 0, exception=InvalidArgument)
    # 0 stripe unit and count are still ignored
    check_default_params(2, 22, RBD_FEATURE_STRIPINGV2, 0, 0)


def test_context_manager():
    with Rados(conffile='') as cluster:
        with cluster.open_ioctx(pool_name) as ioctx:
            image_name = get_temp_image_name()
            RBD().create(ioctx, image_name, IMG_SIZE)
            with Image(ioctx, image_name) as image:
                data = rand_data(256)
                image.write(data, 0)
                read = image.read(0, 256)
            RBD().remove(ioctx, image_name)
            eq(data, read)


def test_open_read_only():
    with Rados(conffile='') as cluster:
        with cluster.open_ioctx(pool_name) as ioctx:
            image_name = get_temp_image_name()
            RBD().create(ioctx, image_name, IMG_SIZE)
            data = rand_data(256)
            with Image(ioctx, image_name) as image:
                image.write(data, 0)
                image.create_snap('snap')
            with Image(ioctx, image_name, read_only=True) as image:
                read = image.read(0, 256)
                eq(data, read)
                assert_raises(ReadOnlyImage, image.write, data, 0)
                assert_raises(ReadOnlyImage, image.create_snap, 'test')
                assert_raises(ReadOnlyImage, image.remove_snap, 'snap')
                assert_raises(ReadOnlyImage, image.rollback_to_snap, 'snap')
                assert_raises(ReadOnlyImage, image.protect_snap, 'snap')
                assert_raises(ReadOnlyImage, image.unprotect_snap, 'snap')
                assert_raises(ReadOnlyImage, image.unprotect_snap, 'snap')
                assert_raises(ReadOnlyImage, image.flatten)
            with Image(ioctx, image_name) as image:
                image.remove_snap('snap')
            RBD().remove(ioctx, image_name)
            eq(data, read)


def test_open_dne():
    for i in range(100):
        image_name = get_temp_image_name()
        assert_raises(ImageNotFound, Image, ioctx, image_name + 'dne')
        assert_raises(ImageNotFound, Image, ioctx, image_name, 'snap')


def test_open_readonly_dne():
    for i in range(100):
        image_name = get_temp_image_name()
        assert_raises(ImageNotFound, Image, ioctx, image_name + 'dne',
                      read_only=True)
        assert_raises(ImageNotFound, Image, ioctx, image_name, 'snap',
                      read_only=True)


def test_remove_dne():
    assert_raises(ImageNotFound, remove_image)


def test_list_empty():
    eq([], RBD().list(ioctx))


@with_setup(create_image, remove_image)
def test_list():
    eq([image_name], RBD().list(ioctx))


@with_setup(create_image, remove_image)
def test_rename():
    rbd = RBD()
    image_name2 = get_temp_image_name()
    rbd.rename(ioctx, image_name, image_name2)
    eq([image_name2], rbd.list(ioctx))
    rbd.rename(ioctx, image_name2, image_name)
    eq([image_name], rbd.list(ioctx))


def rand_data(size):
    return os.urandom(size)


def check_stat(info, size, order):
    assert 'block_name_prefix' in info
    eq(info['size'], size)
    eq(info['order'], order)
    eq(info['num_objs'], size // (1 << order))
    eq(info['obj_size'], 1 << order)


class TestImage(object):

    def setUp(self):
        self.rbd = RBD()
        create_image()
        self.image = Image(ioctx, image_name)

    def tearDown(self):
        self.image.close()
        remove_image()

    @require_new_format()
    @blacklist_features([RBD_FEATURE_EXCLUSIVE_LOCK])
    def test_update_features(self):
        features = self.image.features()
        self.image.update_features(RBD_FEATURE_EXCLUSIVE_LOCK, True)
        eq(features | RBD_FEATURE_EXCLUSIVE_LOCK, self.image.features())

    @require_features([RBD_FEATURE_STRIPINGV2])
    def test_create_with_params(self):
        global features
        image_name = get_temp_image_name()
        order = 20
        stripe_unit = 1 << 20
        stripe_count = 10
        self.rbd.create(ioctx, image_name, IMG_SIZE, order,
                        False, features, stripe_unit, stripe_count)
        image = Image(ioctx, image_name)
        info = image.stat()
        check_stat(info, IMG_SIZE, order)
        eq(image.features(), features)
        eq(image.stripe_unit(), stripe_unit)
        eq(image.stripe_count(), stripe_count)
        image.close()
        RBD().remove(ioctx, image_name)

    def test_invalidate_cache(self):
        self.image.write(b'abc', 0)
        eq(b'abc', self.image.read(0, 3))
        self.image.invalidate_cache()
        eq(b'abc', self.image.read(0, 3))

    def test_stat(self):
        info = self.image.stat()
        check_stat(info, IMG_SIZE, IMG_ORDER)

    def test_flags(self):
        flags = self.image.flags()
        eq(0, flags)

    def test_image_auto_close(self):
        image = Image(ioctx, image_name)

    def test_write(self):
        data = rand_data(256)
        self.image.write(data, 0)

    def test_write_with_fadvise_flags(self):
        data = rand_data(256)
        self.image.write(data, 0, LIBRADOS_OP_FLAG_FADVISE_DONTNEED)
        self.image.write(data, 0, LIBRADOS_OP_FLAG_FADVISE_NOCACHE)

    def test_read(self):
        data = self.image.read(0, 20)
        eq(data, b'\0' * 20)

    def test_read_with_fadvise_flags(self):
        data = self.image.read(0, 20, LIBRADOS_OP_FLAG_FADVISE_DONTNEED)
        eq(data, b'\0' * 20)
        data = self.image.read(0, 20, LIBRADOS_OP_FLAG_FADVISE_RANDOM)
        eq(data, b'\0' * 20)

    def test_large_write(self):
        data = rand_data(IMG_SIZE)
        self.image.write(data, 0)

    def test_large_read(self):
        data = self.image.read(0, IMG_SIZE)
        eq(data, b'\0' * IMG_SIZE)

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

    def test_size(self):
        eq(IMG_SIZE, self.image.size())
        self.image.create_snap('snap1')
        new_size = IMG_SIZE * 2
        self.image.resize(new_size)
        eq(new_size, self.image.size())
        self.image.create_snap('snap2')
        self.image.set_snap('snap2')
        eq(new_size, self.image.size())
        self.image.set_snap('snap1')
        eq(IMG_SIZE, self.image.size())
        self.image.set_snap(None)
        eq(new_size, self.image.size())
        self.image.remove_snap('snap1')
        self.image.remove_snap('snap2')

    def test_resize_down(self):
        new_size = IMG_SIZE // 2
        data = rand_data(256)
        self.image.write(data, IMG_SIZE // 2)
        self.image.resize(new_size)
        self.image.resize(IMG_SIZE)
        read = self.image.read(IMG_SIZE // 2, 256)
        eq(b'\0' * 256, read)

    def test_resize_bytes(self):
        new_size = IMG_SIZE // 2 - 5
        data = rand_data(256)
        self.image.write(data, IMG_SIZE // 2 - 10)
        self.image.resize(new_size)
        self.image.resize(IMG_SIZE)
        read = self.image.read(IMG_SIZE // 2 - 10, 5)
        eq(data[:5], read)
        read = self.image.read(IMG_SIZE // 2 - 5, 251)
        eq(b'\0' * 251, read)

    def _test_copy(self, features=None, order=None, stripe_unit=None,
                   stripe_count=None):
        global ioctx
        data = rand_data(256)
        self.image.write(data, 256)
        image_name = get_temp_image_name()
        if features is None:
            self.image.copy(ioctx, image_name)
        elif order is None:
            self.image.copy(ioctx, image_name, features)
        elif stripe_unit is None:
            self.image.copy(ioctx, image_name, features, order)
        elif stripe_count is None:
            self.image.copy(ioctx, image_name, features, order, stripe_unit)
        else:
            self.image.copy(ioctx, image_name, features, order, stripe_unit,
                            stripe_count)
        assert_raises(ImageExists, self.image.copy, ioctx, image_name)
        copy = Image(ioctx, image_name)
        copy_data = copy.read(256, 256)
        copy.close()
        self.rbd.remove(ioctx, image_name)
        eq(data, copy_data)

    def test_copy(self):
        self._test_copy()

    def test_copy2(self):
        self._test_copy(self.image.features(), self.image.stat()['order'])

    @require_features([RBD_FEATURE_STRIPINGV2])
    def test_copy3(self):
        global features
        self._test_copy(features, self.image.stat()['order'],
                        self.image.stripe_unit(), self.image.stripe_count())

    def test_create_snap(self):
        global ioctx
        self.image.create_snap('snap1')
        read = self.image.read(0, 256)
        eq(read, b'\0' * 256)
        data = rand_data(256)
        self.image.write(data, 0)
        read = self.image.read(0, 256)
        eq(read, data)
        at_snapshot = Image(ioctx, image_name, 'snap1')
        snap_data = at_snapshot.read(0, 256)
        at_snapshot.close()
        eq(snap_data, b'\0' * 256)
        self.image.remove_snap('snap1')

    def test_list_snaps(self):
        eq([], list(self.image.list_snaps()))
        self.image.create_snap('snap1')
        eq(['snap1'], [snap['name'] for snap in self.image.list_snaps()])
        self.image.create_snap('snap2')
        eq(['snap1', 'snap2'], [snap['name'] for snap in self.image.list_snaps()])
        self.image.remove_snap('snap1')
        self.image.remove_snap('snap2')

    def test_list_snaps_iterator_auto_close(self):
        self.image.create_snap('snap1')
        self.image.list_snaps()
        self.image.remove_snap('snap1')

    def test_remove_snap(self):
        eq([], list(self.image.list_snaps()))
        self.image.create_snap('snap1')
        eq(['snap1'], [snap['name'] for snap in self.image.list_snaps()])
        self.image.remove_snap('snap1')
        eq([], list(self.image.list_snaps()))

    def test_rename_snap(self):
        eq([], list(self.image.list_snaps()))
        self.image.create_snap('snap1')
        eq(['snap1'], [snap['name'] for snap in self.image.list_snaps()])
        self.image.rename_snap("snap1", "snap1-rename")
        eq(['snap1-rename'], [snap['name'] for snap in self.image.list_snaps()])
        self.image.remove_snap('snap1-rename')
        eq([], list(self.image.list_snaps()))

    @require_features([RBD_FEATURE_LAYERING])
    def test_protect_snap(self):
        self.image.create_snap('snap1')
        assert(not self.image.is_protected_snap('snap1'))
        self.image.protect_snap('snap1')
        assert(self.image.is_protected_snap('snap1'))
        assert_raises(ImageBusy, self.image.remove_snap, 'snap1')
        self.image.unprotect_snap('snap1')
        assert(not self.image.is_protected_snap('snap1'))
        self.image.remove_snap('snap1')
        assert_raises(ImageNotFound, self.image.unprotect_snap, 'snap1')
        assert_raises(ImageNotFound, self.image.is_protected_snap, 'snap1')

    @require_features([RBD_FEATURE_EXCLUSIVE_LOCK])
    def test_remove_with_exclusive_lock(self):
        assert_raises(ImageBusy, remove_image)

    @blacklist_features([RBD_FEATURE_EXCLUSIVE_LOCK])
    def test_remove_with_snap(self):
        self.image.create_snap('snap1')
        assert_raises(ImageHasSnapshots, remove_image)
        self.image.remove_snap('snap1')

    @blacklist_features([RBD_FEATURE_EXCLUSIVE_LOCK])
    def test_remove_with_watcher(self):
        data = rand_data(256)
        self.image.write(data, 0)
        assert_raises(ImageBusy, remove_image)
        read = self.image.read(0, 256)
        eq(read, data)

    def test_rollback_to_snap(self):
        self.image.write(b'\0' * 256, 0)
        self.image.create_snap('snap1')
        read = self.image.read(0, 256)
        eq(read, b'\0' * 256)
        data = rand_data(256)
        self.image.write(data, 0)
        read = self.image.read(0, 256)
        eq(read, data)
        self.image.rollback_to_snap('snap1')
        read = self.image.read(0, 256)
        eq(read, b'\0' * 256)
        self.image.remove_snap('snap1')

    def test_rollback_to_snap_sparse(self):
        self.image.create_snap('snap1')
        read = self.image.read(0, 256)
        eq(read, b'\0' * 256)
        data = rand_data(256)
        self.image.write(data, 0)
        read = self.image.read(0, 256)
        eq(read, data)
        self.image.rollback_to_snap('snap1')
        read = self.image.read(0, 256)
        eq(read, b'\0' * 256)
        self.image.remove_snap('snap1')

    def test_rollback_with_resize(self):
        read = self.image.read(0, 256)
        eq(read, b'\0' * 256)
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
        self.image.write(b'\0' * 256, 0)
        self.image.create_snap('snap1')
        read = self.image.read(0, 256)
        eq(read, b'\0' * 256)
        data = rand_data(256)
        self.image.write(data, 0)
        read = self.image.read(0, 256)
        eq(read, data)
        self.image.set_snap('snap1')
        read = self.image.read(0, 256)
        eq(read, b'\0' * 256)
        self.image.remove_snap('snap1')

    def test_set_no_snap(self):
        self.image.write(b'\0' * 256, 0)
        self.image.create_snap('snap1')
        read = self.image.read(0, 256)
        eq(read, b'\0' * 256)
        data = rand_data(256)
        self.image.write(data, 0)
        read = self.image.read(0, 256)
        eq(read, data)
        self.image.set_snap('snap1')
        read = self.image.read(0, 256)
        eq(read, b'\0' * 256)
        self.image.set_snap(None)
        read = self.image.read(0, 256)
        eq(read, data)
        self.image.remove_snap('snap1')

    def test_set_snap_sparse(self):
        self.image.create_snap('snap1')
        read = self.image.read(0, 256)
        eq(read, b'\0' * 256)
        data = rand_data(256)
        self.image.write(data, 0)
        read = self.image.read(0, 256)
        eq(read, data)
        self.image.set_snap('snap1')
        read = self.image.read(0, 256)
        eq(read, b'\0' * 256)
        self.image.remove_snap('snap1')

    def test_many_snaps(self):
        num_snaps = 200
        for i in range(num_snaps):
            self.image.create_snap(str(i))
        snaps = sorted(self.image.list_snaps(),
                       key=lambda snap: int(snap['name']))
        eq(len(snaps), num_snaps)
        for i, snap in enumerate(snaps):
            eq(snap['size'], IMG_SIZE)
            eq(snap['name'], str(i))
        for i in range(num_snaps):
            self.image.remove_snap(str(i))

    def test_set_snap_deleted(self):
        self.image.write(b'\0' * 256, 0)
        self.image.create_snap('snap1')
        read = self.image.read(0, 256)
        eq(read, b'\0' * 256)
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
        self.image.write(b'\0' * 256, 0)
        self.image.create_snap('snap1')
        read = self.image.read(0, 256)
        eq(read, b'\0' * 256)
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

    def test_lock_unlock(self):
        assert_raises(ImageNotFound, self.image.unlock, '')
        self.image.lock_exclusive('')
        assert_raises(ImageExists, self.image.lock_exclusive, '')
        assert_raises(ImageBusy, self.image.lock_exclusive, 'test')
        assert_raises(ImageExists, self.image.lock_shared, '', '')
        assert_raises(ImageBusy, self.image.lock_shared, 'foo', '')
        self.image.unlock('')

    def test_list_lockers(self):
        eq([], self.image.list_lockers())
        self.image.lock_exclusive('test')
        lockers = self.image.list_lockers()
        eq(1, len(lockers['lockers']))
        _, cookie, _ = lockers['lockers'][0]
        eq(cookie, 'test')
        eq('', lockers['tag'])
        assert lockers['exclusive']
        self.image.unlock('test')
        eq([], self.image.list_lockers())

        num_shared = 10
        for i in range(num_shared):
            self.image.lock_shared(str(i), 'tag')
        lockers = self.image.list_lockers()
        eq('tag', lockers['tag'])
        assert not lockers['exclusive']
        eq(num_shared, len(lockers['lockers']))
        cookies = sorted(map(lambda x: x[1], lockers['lockers']))
        for i in range(num_shared):
            eq(str(i), cookies[i])
            self.image.unlock(str(i))
        eq([], self.image.list_lockers())

    def test_diff_iterate(self):
        check_diff(self.image, 0, IMG_SIZE, None, [])
        self.image.write(b'a' * 256, 0)
        check_diff(self.image, 0, IMG_SIZE, None, [(0, 256, True)])
        self.image.write(b'b' * 256, 256)
        check_diff(self.image, 0, IMG_SIZE, None, [(0, 512, True)])
        self.image.discard(128, 256)
        check_diff(self.image, 0, IMG_SIZE, None, [(0, 512, True)])

        self.image.create_snap('snap1')
        self.image.discard(0, 1 << IMG_ORDER)
        self.image.create_snap('snap2')
        self.image.set_snap('snap2')
        check_diff(self.image, 0, IMG_SIZE, 'snap1', [(0, 512, False)])
        self.image.remove_snap('snap1')
        self.image.remove_snap('snap2')


def check_diff(image, offset, length, from_snapshot, expected):
    extents = []

    def cb(offset, length, exists):
        extents.append((offset, length, exists))
    image.diff_iterate(0, IMG_SIZE, None, cb)
    eq(extents, expected)


class TestClone(object):

    @require_features([RBD_FEATURE_LAYERING])
    def setUp(self):
        global ioctx
        global features
        self.rbd = RBD()
        create_image()
        self.image = Image(ioctx, image_name)
        data = rand_data(256)
        self.image.write(data, IMG_SIZE // 2)
        self.image.create_snap('snap1')
        global features
        self.image.protect_snap('snap1')
        self.clone_name = get_temp_image_name()
        self.rbd.clone(ioctx, image_name, 'snap1', ioctx, self.clone_name,
                       features)
        self.clone = Image(ioctx, self.clone_name)

    def tearDown(self):
        global ioctx
        self.clone.close()
        self.rbd.remove(ioctx, self.clone_name)
        self.image.unprotect_snap('snap1')
        self.image.remove_snap('snap1')
        self.image.close()
        remove_image()

    @require_features([RBD_FEATURE_STRIPINGV2])
    def test_with_params(self):
        global features
        self.image.create_snap('snap2')
        self.image.protect_snap('snap2')
        clone_name2 = get_temp_image_name()
        self.rbd.clone(ioctx, image_name, 'snap2', ioctx, clone_name2,
                       features, self.image.stat()['order'],
                       self.image.stripe_unit(), self.image.stripe_count())
        self.rbd.remove(ioctx, clone_name2)
        self.image.unprotect_snap('snap2')
        self.image.remove_snap('snap2')

    def test_unprotected(self):
        self.image.create_snap('snap2')
        global features
        clone_name2 = get_temp_image_name()
        assert_raises(InvalidArgument, self.rbd.clone, ioctx, image_name,
                      'snap2', ioctx, clone_name2, features)
        self.image.remove_snap('snap2')

    def test_unprotect_with_children(self):
        global features
        # can't remove a snapshot that has dependent clones
        assert_raises(ImageBusy, self.image.remove_snap, 'snap1')

        # validate parent info of clone created by TestClone.setUp
        (pool, image, snap) = self.clone.parent_info()
        eq(pool, pool_name)
        eq(image, image_name)
        eq(snap, 'snap1')

        # create a new pool...
        pool_name2 = get_temp_pool_name()
        rados.create_pool(pool_name2)
        other_ioctx = rados.open_ioctx(pool_name2)

        # ...with a clone of the same parent
        other_clone_name = get_temp_image_name()
        self.rbd.clone(ioctx, image_name, 'snap1', other_ioctx,
                       other_clone_name, features)
        self.other_clone = Image(other_ioctx, other_clone_name)
        # validate its parent info
        (pool, image, snap) = self.other_clone.parent_info()
        eq(pool, pool_name)
        eq(image, image_name)
        eq(snap, 'snap1')

        # can't unprotect snap with children
        assert_raises(ImageBusy, self.image.unprotect_snap, 'snap1')

        # 2 children, check that cannot remove the parent snap
        assert_raises(ImageBusy, self.image.remove_snap, 'snap1')

        # close and remove other pool's clone
        self.other_clone.close()
        self.rbd.remove(other_ioctx, other_clone_name)

        # check that we cannot yet remove the parent snap
        assert_raises(ImageBusy, self.image.remove_snap, 'snap1')

        other_ioctx.close()
        rados.delete_pool(pool_name2)

        # unprotect, remove parent snap happen in cleanup, and should succeed

    def test_stat(self):
        image_info = self.image.stat()
        clone_info = self.clone.stat()
        eq(clone_info['size'], image_info['size'])
        eq(clone_info['size'], self.clone.overlap())

    def test_resize_stat(self):
        self.clone.resize(IMG_SIZE // 2)
        image_info = self.image.stat()
        clone_info = self.clone.stat()
        eq(clone_info['size'], IMG_SIZE // 2)
        eq(image_info['size'], IMG_SIZE)
        eq(self.clone.overlap(), IMG_SIZE // 2)

        self.clone.resize(IMG_SIZE * 2)
        image_info = self.image.stat()
        clone_info = self.clone.stat()
        eq(clone_info['size'], IMG_SIZE * 2)
        eq(image_info['size'], IMG_SIZE)
        eq(self.clone.overlap(), IMG_SIZE // 2)

    def test_resize_io(self):
        parent_data = self.image.read(IMG_SIZE // 2, 256)
        self.image.resize(0)
        self.clone.resize(IMG_SIZE // 2 + 128)
        child_data = self.clone.read(IMG_SIZE // 2, 128)
        eq(child_data, parent_data[:128])
        self.clone.resize(IMG_SIZE)
        child_data = self.clone.read(IMG_SIZE // 2, 256)
        eq(child_data, parent_data[:128] + (b'\0' * 128))
        self.clone.resize(IMG_SIZE // 2 + 1)
        child_data = self.clone.read(IMG_SIZE // 2, 1)
        eq(child_data, parent_data[0:1])
        self.clone.resize(0)
        self.clone.resize(IMG_SIZE)
        child_data = self.clone.read(IMG_SIZE // 2, 256)
        eq(child_data, b'\0' * 256)

    def test_read(self):
        parent_data = self.image.read(IMG_SIZE // 2, 256)
        child_data = self.clone.read(IMG_SIZE // 2, 256)
        eq(child_data, parent_data)

    def test_write(self):
        parent_data = self.image.read(IMG_SIZE // 2, 256)
        new_data = rand_data(256)
        self.clone.write(new_data, IMG_SIZE // 2 + 256)
        child_data = self.clone.read(IMG_SIZE // 2 + 256, 256)
        eq(child_data, new_data)
        child_data = self.clone.read(IMG_SIZE // 2, 256)
        eq(child_data, parent_data)
        parent_data = self.image.read(IMG_SIZE // 2 + 256, 256)
        eq(parent_data, b'\0' * 256)

    def check_children(self, expected):
        actual = self.image.list_children()
        # dedup for cache pools until
        # http://tracker.ceph.com/issues/8187 is fixed
        deduped = set([(pool_name, image[1]) for image in actual])
        eq(deduped, set(expected))

    def test_list_children(self):
        global ioctx
        global features
        self.image.set_snap('snap1')
        self.check_children([(pool_name, self.clone_name)])
        self.clone.close()
        self.rbd.remove(ioctx, self.clone_name)
        eq(self.image.list_children(), [])

        clone_name = get_temp_image_name() + '_'
        expected_children = []
        for i in range(10):
            self.rbd.clone(ioctx, image_name, 'snap1', ioctx,
                           clone_name + str(i), features)
            expected_children.append((pool_name, clone_name + str(i)))
            self.check_children(expected_children)

        for i in range(10):
            self.rbd.remove(ioctx, clone_name + str(i))
            expected_children.pop(0)
            self.check_children(expected_children)

        eq(self.image.list_children(), [])
        self.rbd.clone(ioctx, image_name, 'snap1', ioctx, self.clone_name,
                       features)
        self.check_children([(pool_name, self.clone_name)])
        self.clone = Image(ioctx, self.clone_name)

    def test_flatten_errors(self):
        # test that we can't flatten a non-clone
        assert_raises(InvalidArgument, self.image.flatten)

        # test that we can't flatten a snapshot
        self.clone.create_snap('snap2')
        self.clone.set_snap('snap2')
        assert_raises(ReadOnlyImage, self.clone.flatten)
        self.clone.remove_snap('snap2')

    def check_flatten_with_order(self, new_order):
        global ioctx
        global features
        clone_name2 = get_temp_image_name()
        self.rbd.clone(ioctx, image_name, 'snap1', ioctx, clone_name2,
                       features, new_order)
        # with Image(ioctx, 'clone2') as clone:
        clone2 = Image(ioctx, clone_name2)
        clone2.flatten()
        eq(clone2.overlap(), 0)
        clone2.close()
        self.rbd.remove(ioctx, clone_name2)

        # flatten after resizing to non-block size
        self.rbd.clone(ioctx, image_name, 'snap1', ioctx, clone_name2,
                       features, new_order)
        with Image(ioctx, clone_name2) as clone:
            clone.resize(IMG_SIZE // 2 - 1)
            clone.flatten()
            eq(0, clone.overlap())
        self.rbd.remove(ioctx, clone_name2)

        # flatten after resizing to non-block size
        self.rbd.clone(ioctx, image_name, 'snap1', ioctx, clone_name2,
                       features, new_order)
        with Image(ioctx, clone_name2) as clone:
            clone.resize(IMG_SIZE // 2 + 1)
            clone.flatten()
            eq(clone.overlap(), 0)
        self.rbd.remove(ioctx, clone_name2)

    def test_flatten_basic(self):
        self.check_flatten_with_order(IMG_ORDER)

    def test_flatten_smaller_order(self):
        self.check_flatten_with_order(IMG_ORDER - 2)

    def test_flatten_larger_order(self):
        self.check_flatten_with_order(IMG_ORDER + 2)

    def test_flatten_drops_cache(self):
        global ioctx
        global features
        clone_name2 = get_temp_image_name()
        self.rbd.clone(ioctx, image_name, 'snap1', ioctx, clone_name2,
                       features, IMG_ORDER)
        with Image(ioctx, clone_name2) as clone:
            with Image(ioctx, clone_name2) as clone2:
                # cache object non-existence
                data = clone.read(IMG_SIZE // 2, 256)
                clone2_data = clone2.read(IMG_SIZE // 2, 256)
                eq(data, clone2_data)
                clone.flatten()
                assert_raises(ImageNotFound, clone.parent_info)
                assert_raises(ImageNotFound, clone2.parent_info)
                after_flatten = clone.read(IMG_SIZE // 2, 256)
                eq(data, after_flatten)
                after_flatten = clone2.read(IMG_SIZE // 2, 256)
                eq(data, after_flatten)
        self.rbd.remove(ioctx, clone_name2)

    def test_flatten_multi_level(self):
        self.clone.create_snap('snap2')
        self.clone.protect_snap('snap2')
        clone_name3 = get_temp_image_name()
        self.rbd.clone(ioctx, self.clone_name, 'snap2', ioctx, clone_name3,
                       features)
        self.clone.flatten()
        with Image(ioctx, clone_name3) as clone3:
            clone3.flatten()
        self.clone.unprotect_snap('snap2')
        self.clone.remove_snap('snap2')
        self.rbd.remove(ioctx, clone_name3)

    def test_resize_flatten_multi_level(self):
        self.clone.create_snap('snap2')
        self.clone.protect_snap('snap2')
        clone_name3 = get_temp_image_name()
        self.rbd.clone(ioctx, self.clone_name, 'snap2', ioctx, clone_name3,
                       features)
        self.clone.resize(1)
        orig_data = self.image.read(0, 256)
        with Image(ioctx, clone_name3) as clone3:
            clone3_data = clone3.read(0, 256)
            eq(orig_data, clone3_data)
        self.clone.flatten()
        with Image(ioctx, clone_name3) as clone3:
            clone3_data = clone3.read(0, 256)
            eq(orig_data, clone3_data)
        self.rbd.remove(ioctx, clone_name3)
        self.clone.unprotect_snap('snap2')
        self.clone.remove_snap('snap2')


class TestExclusiveLock(object):

    @require_features([RBD_FEATURE_EXCLUSIVE_LOCK])
    def setUp(self):
        global rados2
        rados2 = Rados(conffile='')
        rados2.connect()
        global ioctx2
        ioctx2 = rados2.open_ioctx(pool_name)
        create_image()

    def tearDown(self):
        remove_image()
        global ioctx2
        ioctx2.__del__()
        global rados2
        rados2.shutdown()

    def test_ownership(self):
        with Image(ioctx, image_name) as image1, Image(ioctx2, image_name) as image2:
            image1.write(b'0'*256, 0)
            eq(image1.is_exclusive_lock_owner(), True)
            eq(image2.is_exclusive_lock_owner(), False)

    def test_snapshot_leadership(self):
        with Image(ioctx, image_name) as image:
            image.create_snap('snap')
            eq(image.is_exclusive_lock_owner(), True)
        try:
            with Image(ioctx, image_name) as image:
                image.write(b'0'*256, 0)
                eq(image.is_exclusive_lock_owner(), True)
                image.set_snap('snap')
                eq(image.is_exclusive_lock_owner(), False)
            with Image(ioctx, image_name, snapshot='snap') as image:
                eq(image.is_exclusive_lock_owner(), False)
        finally:
            with Image(ioctx, image_name) as image:
                image.remove_snap('snap')

    def test_read_only_leadership(self):
        with Image(ioctx, image_name, read_only=True) as image:
            eq(image.is_exclusive_lock_owner(), False)

    def test_follower_flatten(self):
        with Image(ioctx, image_name) as image:
            image.create_snap('snap')
            image.protect_snap('snap')
        try:
            RBD().clone(ioctx, image_name, 'snap', ioctx, 'clone', features)
            with Image(ioctx, 'clone') as image1, Image(ioctx2, 'clone') as image2:
                data = rand_data(256)
                image1.write(data, 0)
                image2.flatten()
                assert_raises(ImageNotFound, image1.parent_info)
                parent = True
                for x in range(30):
                    try:
                        image2.parent_info()
                    except ImageNotFound:
                        parent = False
                        break
                eq(False, parent)
        finally:
            RBD().remove(ioctx, 'clone')
            with Image(ioctx, image_name) as image:
                image.unprotect_snap('snap')
                image.remove_snap('snap')

    def test_follower_resize(self):
        with Image(ioctx, image_name) as image1, Image(ioctx2, image_name) as image2:
            image1.write(b'0'*256, 0)
            for new_size in [IMG_SIZE * 2, IMG_SIZE // 2]:
                image2.resize(new_size)
                eq(new_size, image1.size())
                for x in range(30):
                    if new_size == image2.size():
                        break
                    time.sleep(1)
                eq(new_size, image2.size())

    def test_follower_snap_create(self):
        with Image(ioctx, image_name) as image1, Image(ioctx2, image_name) as image2:
            image2.create_snap('snap1')
            image1.remove_snap('snap1')

    def test_follower_snap_rollback(self):
        with Image(ioctx, image_name) as image1, Image(ioctx2, image_name) as image2:
            image1.create_snap('snap')
            try:
                assert_raises(ReadOnlyImage, image2.rollback_to_snap, 'snap')
                image1.rollback_to_snap('snap')
            finally:
                image1.remove_snap('snap')

    def test_follower_discard(self):
        with Image(ioctx, image_name) as image1, Image(ioctx2, image_name) as image2:
            data = rand_data(256)
            image1.write(data, 0)
            image2.discard(0, 256)
            eq(image1.is_exclusive_lock_owner(), False)
            eq(image2.is_exclusive_lock_owner(), True)
            read = image2.read(0, 256)
            eq(256 * b'\0', read)

    def test_follower_write(self):
        with Image(ioctx, image_name) as image1, Image(ioctx2, image_name) as image2:
            data = rand_data(256)
            image1.write(data, 0)
            image2.write(data, IMG_SIZE // 2)
            eq(image1.is_exclusive_lock_owner(), False)
            eq(image2.is_exclusive_lock_owner(), True)
            for offset in [0, IMG_SIZE // 2]:
                read = image2.read(offset, 256)
                eq(data, read)
