# vim: expandtab smarttab shiftwidth=4 softtabstop=4
import base64
import copy
import errno
import functools
import json
import socket
import os
import time
import sys

from datetime import datetime, timedelta
from nose import with_setup, SkipTest
from nose.tools import eq_ as eq, assert_raises, assert_not_equal
from rados import (Rados,
                   LIBRADOS_OP_FLAG_FADVISE_DONTNEED,
                   LIBRADOS_OP_FLAG_FADVISE_NOCACHE,
                   LIBRADOS_OP_FLAG_FADVISE_RANDOM)
from rbd import (RBD, Group, Image, ImageNotFound, InvalidArgument, ImageExists,
                 ImageBusy, ImageHasSnapshots, ReadOnlyImage,
                 FunctionNotSupported, ArgumentOutOfRange,
                 ECANCELED, OperationCanceled,
                 DiskQuotaExceeded, ConnectionShutdown, PermissionError,
                 RBD_FEATURE_LAYERING, RBD_FEATURE_STRIPINGV2,
                 RBD_FEATURE_EXCLUSIVE_LOCK, RBD_FEATURE_JOURNALING,
                 RBD_FEATURE_DEEP_FLATTEN, RBD_FEATURE_FAST_DIFF,
                 RBD_FEATURE_OBJECT_MAP,
                 RBD_MIRROR_MODE_DISABLED, RBD_MIRROR_MODE_IMAGE,
                 RBD_MIRROR_MODE_POOL, RBD_MIRROR_IMAGE_ENABLED,
                 RBD_MIRROR_IMAGE_DISABLED, MIRROR_IMAGE_STATUS_STATE_UNKNOWN,
                 RBD_MIRROR_IMAGE_MODE_JOURNAL, RBD_MIRROR_IMAGE_MODE_SNAPSHOT,
                 RBD_LOCK_MODE_EXCLUSIVE, RBD_OPERATION_FEATURE_GROUP,
                 RBD_SNAP_NAMESPACE_TYPE_TRASH,
                 RBD_SNAP_NAMESPACE_TYPE_MIRROR,
                 RBD_IMAGE_MIGRATION_STATE_PREPARED, RBD_CONFIG_SOURCE_CONFIG,
                 RBD_CONFIG_SOURCE_POOL, RBD_CONFIG_SOURCE_IMAGE,
                 RBD_MIRROR_PEER_ATTRIBUTE_NAME_MON_HOST,
                 RBD_MIRROR_PEER_ATTRIBUTE_NAME_KEY,
                 RBD_MIRROR_PEER_DIRECTION_RX, RBD_MIRROR_PEER_DIRECTION_RX_TX,
                 RBD_SNAP_REMOVE_UNPROTECT, RBD_SNAP_MIRROR_STATE_PRIMARY,
                 RBD_SNAP_MIRROR_STATE_PRIMARY_DEMOTED,
                 RBD_SNAP_CREATE_SKIP_QUIESCE,
                 RBD_SNAP_CREATE_IGNORE_QUIESCE_ERROR)

rados = None
ioctx = None
features = None
image_idx = 0
group_idx = 0
snap_idx = 0
image_name = None
group_name = None
snap_name = None
pool_idx = 0
pool_name = None
IMG_SIZE = 8 << 20 # 8 MiB
IMG_ORDER = 22 # 4 MiB objects

os.environ["RBD_FORCE_ALLOW_V1"] = "1"

def setup_module():
    global rados
    rados = Rados(conffile='')
    rados.connect()
    global pool_name
    pool_name = get_temp_pool_name()
    rados.create_pool(pool_name)
    global ioctx
    ioctx = rados.open_ioctx(pool_name)
    RBD().pool_init(ioctx, True)
    global features
    features = os.getenv("RBD_FEATURES")
    features = int(features) if features is not None else 61

def teardown_module():
    global ioctx
    ioctx.close()
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

def get_temp_group_name():
    global group_idx
    group_idx += 1
    return "group" + str(group_idx)

def get_temp_snap_name():
    global snap_idx
    snap_idx += 1
    return "snap" + str(snap_idx)

def create_image():
    global image_name
    image_name = get_temp_image_name()
    if features is not None:
        RBD().create(ioctx, image_name, IMG_SIZE, IMG_ORDER, old_format=False,
                     features=int(features))
    else:
        RBD().create(ioctx, image_name, IMG_SIZE, IMG_ORDER, old_format=True)
    return image_name

def remove_image():
    if image_name is not None:
        RBD().remove(ioctx, image_name)

def create_group():
    global group_name
    group_name = get_temp_group_name()
    RBD().group_create(ioctx, group_name)

def remove_group():
    if group_name is not None:
        RBD().group_remove(ioctx, group_name)

def rename_group():
    new_group_name = "new" + group_name
    RBD().group_rename(ioctx, group_name, new_group_name)

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
        feature_data_pool = 0
        datapool = rados.conf_get('rbd_default_data_pool')
        if not len(datapool) == 0:
            feature_data_pool = 128
        image_name = get_temp_image_name()
        if exception is None:
            RBD().create(ioctx, image_name, IMG_SIZE, old_format=(format == 1))
            try:
                with Image(ioctx, image_name) as image:
                    eq(format == 1, image.old_format())

                    expected_order = int(rados.conf_get('rbd_default_order'))
                    actual_order = image.stat()['order']
                    eq(expected_order, actual_order)

                    expected_features = features
                    if format == 1:
                        expected_features = 0
                    elif expected_features is None:
                        expected_features = 61 | feature_data_pool
                    else:
                        expected_features |= feature_data_pool
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
    check_default_params(2, 20, 0, 0, 0)
    # make sure invalid combinations of stripe unit and order are still invalid
    check_default_params(2, 22, RBD_FEATURE_STRIPINGV2, 10, 1 << 50, exception=InvalidArgument)
    check_default_params(2, 22, RBD_FEATURE_STRIPINGV2, 10, 100, exception=InvalidArgument)
    check_default_params(2, 22, RBD_FEATURE_STRIPINGV2, 0, 1, exception=InvalidArgument)
    check_default_params(2, 22, RBD_FEATURE_STRIPINGV2, 1, 0, exception=InvalidArgument)
    # 0 stripe unit and count are still ignored
    check_default_params(2, 22, 0, 0, 0)

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

@require_new_format()
def test_open_by_id():
    with Rados(conffile='') as cluster:
        with cluster.open_ioctx(pool_name) as ioctx:
            image_name = get_temp_image_name()
            RBD().create(ioctx, image_name, IMG_SIZE)
            with Image(ioctx, image_name) as image:
                image_id = image.id()
            with Image(ioctx, image_id=image_id) as image:
                eq(image.get_name(), image_name)
            RBD().remove(ioctx, image_name)

def test_remove_dne():
    assert_raises(ImageNotFound, remove_image)

def test_list_empty():
    eq([], RBD().list(ioctx))

@with_setup(create_image, remove_image)
def test_list():
    eq([image_name], RBD().list(ioctx))

    with Image(ioctx, image_name) as image:
        image_id = image.id()
    eq([{'id': image_id, 'name': image_name}], list(RBD().list2(ioctx)))

@with_setup(create_image)
def test_remove_with_progress():
    d = {'received_callback': False}
    def progress_cb(current, total):
        d['received_callback'] = True
        return 0

    RBD().remove(ioctx, image_name, on_progress=progress_cb)
    eq(True, d['received_callback'])

@with_setup(create_image)
def test_remove_canceled():
    def progress_cb(current, total):
        return -ECANCELED

    assert_raises(OperationCanceled, RBD().remove, ioctx, image_name,
                  on_progress=progress_cb)

@with_setup(create_image, remove_image)
def test_rename():
    rbd = RBD()
    image_name2 = get_temp_image_name()
    rbd.rename(ioctx, image_name, image_name2)
    eq([image_name2], rbd.list(ioctx))
    rbd.rename(ioctx, image_name2, image_name)
    eq([image_name], rbd.list(ioctx))

def test_pool_metadata():
    rbd = RBD()
    metadata = list(rbd.pool_metadata_list(ioctx))
    eq(len(metadata), 0)
    assert_raises(KeyError, rbd.pool_metadata_get, ioctx, "key1")
    rbd.pool_metadata_set(ioctx, "key1", "value1")
    rbd.pool_metadata_set(ioctx, "key2", "value2")
    value = rbd.pool_metadata_get(ioctx, "key1")
    eq(value, "value1")
    value = rbd.pool_metadata_get(ioctx, "key2")
    eq(value, "value2")
    metadata = list(rbd.pool_metadata_list(ioctx))
    eq(len(metadata), 2)
    rbd.pool_metadata_remove(ioctx, "key1")
    metadata = list(rbd.pool_metadata_list(ioctx))
    eq(len(metadata), 1)
    eq(metadata[0], ("key2", "value2"))
    rbd.pool_metadata_remove(ioctx, "key2")
    assert_raises(KeyError, rbd.pool_metadata_remove, ioctx, "key2")
    metadata = list(rbd.pool_metadata_list(ioctx))
    eq(len(metadata), 0)

    N = 65
    for i in range(N):
        rbd.pool_metadata_set(ioctx, "key" + str(i), "X" * 1025)
    metadata = list(rbd.pool_metadata_list(ioctx))
    eq(len(metadata), N)
    for i in range(N):
        rbd.pool_metadata_remove(ioctx, "key" + str(i))
        metadata = list(rbd.pool_metadata_list(ioctx))
        eq(len(metadata), N - i - 1)

def test_config_list():
    rbd = RBD()

    for option in rbd.config_list(ioctx):
        eq(option['source'], RBD_CONFIG_SOURCE_CONFIG)

    rbd.pool_metadata_set(ioctx, "conf_rbd_cache", "true")

    for option in rbd.config_list(ioctx):
        if option['name'] == "rbd_cache":
            eq(option['source'], RBD_CONFIG_SOURCE_POOL)
        else:
            eq(option['source'], RBD_CONFIG_SOURCE_CONFIG)

    rbd.pool_metadata_remove(ioctx, "conf_rbd_cache")

    for option in rbd.config_list(ioctx):
        eq(option['source'], RBD_CONFIG_SOURCE_CONFIG)

def test_pool_config_set_and_get_and_remove():
    rbd = RBD()

    for option in rbd.config_list(ioctx):
        eq(option['source'], RBD_CONFIG_SOURCE_CONFIG)

    rbd.config_set(ioctx, "rbd_request_timed_out_seconds", "100")
    new_value = rbd.config_get(ioctx, "rbd_request_timed_out_seconds")
    eq(new_value, "100")
    rbd.config_remove(ioctx, "rbd_request_timed_out_seconds")

    for option in rbd.config_list(ioctx):
        eq(option['source'], RBD_CONFIG_SOURCE_CONFIG)

def test_namespaces():
    rbd = RBD()

    eq(False, rbd.namespace_exists(ioctx, 'ns1'))
    eq([], rbd.namespace_list(ioctx))
    assert_raises(ImageNotFound, rbd.namespace_remove, ioctx, 'ns1')

    rbd.namespace_create(ioctx, 'ns1')
    eq(True, rbd.namespace_exists(ioctx, 'ns1'))

    assert_raises(ImageExists, rbd.namespace_create, ioctx, 'ns1')
    eq(['ns1'], rbd.namespace_list(ioctx))
    rbd.namespace_remove(ioctx, 'ns1')
    eq([], rbd.namespace_list(ioctx))

@require_new_format()
def test_pool_stats():
    rbd = RBD()

    try:
        image1 = create_image()
        image2 = create_image()
        image3 = create_image()
        image4 = create_image()
        with Image(ioctx, image4) as image:
            image.create_snap('snap')
            image.resize(0)

        stats = rbd.pool_stats_get(ioctx)
        eq(stats['image_count'], 4)
        eq(stats['image_provisioned_bytes'], 3 * IMG_SIZE)
        eq(stats['image_max_provisioned_bytes'], 4 * IMG_SIZE)
        eq(stats['image_snap_count'], 1)
        eq(stats['trash_count'], 0)
        eq(stats['trash_provisioned_bytes'], 0)
        eq(stats['trash_max_provisioned_bytes'], 0)
        eq(stats['trash_snap_count'], 0)
    finally:
        rbd.remove(ioctx, image1)
        rbd.remove(ioctx, image2)
        rbd.remove(ioctx, image3)
        with Image(ioctx, image4) as image:
            image.remove_snap('snap')
        rbd.remove(ioctx, image4)

def rand_data(size):
    return os.urandom(size)

def check_stat(info, size, order):
    assert 'block_name_prefix' in info
    eq(info['size'], size)
    eq(info['order'], order)
    eq(info['num_objs'], size // (1 << order))
    eq(info['obj_size'], 1 << order)

@require_new_format()
def test_features_to_string():
    rbd = RBD()
    features = RBD_FEATURE_DEEP_FLATTEN | RBD_FEATURE_EXCLUSIVE_LOCK | RBD_FEATURE_FAST_DIFF \
               | RBD_FEATURE_LAYERING | RBD_FEATURE_OBJECT_MAP
    expected_features_string = "deep-flatten,exclusive-lock,fast-diff,layering,object-map"
    features_string = rbd.features_to_string(features)
    eq(expected_features_string, features_string)

    features = RBD_FEATURE_LAYERING
    features_string = rbd.features_to_string(features)
    eq(features_string, "layering")

    features = 16777216
    assert_raises(InvalidArgument, rbd.features_to_string, features)

@require_new_format()
def test_features_from_string():
    rbd = RBD()
    features_string = "deep-flatten,exclusive-lock,fast-diff,layering,object-map"
    expected_features_bitmask = RBD_FEATURE_DEEP_FLATTEN | RBD_FEATURE_EXCLUSIVE_LOCK | RBD_FEATURE_FAST_DIFF \
                                | RBD_FEATURE_LAYERING | RBD_FEATURE_OBJECT_MAP
    features = rbd.features_from_string(features_string)
    eq(expected_features_bitmask, features)

    features_string = "layering"
    features = rbd.features_from_string(features_string)
    eq(features, RBD_FEATURE_LAYERING)

class TestImage(object):

    def setUp(self):
        self.rbd = RBD()
        create_image()
        self.image = Image(ioctx, image_name)

    def tearDown(self):
        self.image.close()
        remove_image()
        self.image = None

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

    @require_new_format()
    def test_id(self):
        assert_not_equal(b'', self.image.id())

    def test_block_name_prefix(self):
        assert_not_equal(b'', self.image.block_name_prefix())

    def test_create_timestamp(self):
        timestamp = self.image.create_timestamp()
        assert_not_equal(0, timestamp.year)
        assert_not_equal(1970, timestamp.year)

    def test_access_timestamp(self):
        timestamp = self.image.access_timestamp()
        assert_not_equal(0, timestamp.year)
        assert_not_equal(1970, timestamp.year)

    def test_modify_timestamp(self):
        timestamp = self.image.modify_timestamp()
        assert_not_equal(0, timestamp.year)
        assert_not_equal(1970, timestamp.year)

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

    def test_use_after_close(self):
        self.image.close()
        assert_raises(InvalidArgument, self.image.stat)

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

    def test_resize_allow_shrink_False(self):
        new_size = IMG_SIZE * 2
        self.image.resize(new_size)
        info = self.image.stat()
        check_stat(info, new_size, IMG_ORDER)
        assert_raises(InvalidArgument, self.image.resize, IMG_SIZE, False)

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
        self.image.write(data, IMG_SIZE // 2);
        self.image.resize(new_size)
        self.image.resize(IMG_SIZE)
        read = self.image.read(IMG_SIZE // 2, 256)
        eq(b'\0' * 256, read)

    def test_resize_bytes(self):
        new_size = IMG_SIZE // 2 - 5
        data = rand_data(256)
        self.image.write(data, IMG_SIZE // 2 - 10);
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

    def test_deep_copy(self):
        global ioctx
        global features
        self.image.write(b'a' * 256, 0)
        self.image.create_snap('snap1')
        self.image.write(b'b' * 256, 0)
        dst_name = get_temp_image_name()
        self.image.deep_copy(ioctx, dst_name, features=features,
                             order=self.image.stat()['order'],
                             stripe_unit=self.image.stripe_unit(),
                             stripe_count=self.image.stripe_count(),
                             data_pool=None)
        self.image.remove_snap('snap1')
        with Image(ioctx, dst_name, 'snap1') as copy:
            copy_data = copy.read(0, 256)
            eq(b'a' * 256, copy_data)
        with Image(ioctx, dst_name) as copy:
            copy_data = copy.read(0, 256)
            eq(b'b' * 256, copy_data)
            copy.remove_snap('snap1')
        self.rbd.remove(ioctx, dst_name)

    @require_features([RBD_FEATURE_LAYERING])
    def test_deep_copy_clone(self):
        global ioctx
        global features
        self.image.write(b'a' * 256, 0)
        self.image.create_snap('snap1')
        self.image.write(b'b' * 256, 0)
        self.image.protect_snap('snap1')
        clone_name = get_temp_image_name()
        dst_name = get_temp_image_name()
        self.rbd.clone(ioctx, image_name, 'snap1', ioctx, clone_name)
        with Image(ioctx, clone_name) as child:
            child.create_snap('snap1')
            child.deep_copy(ioctx, dst_name, features=features,
                            order=self.image.stat()['order'],
                            stripe_unit=self.image.stripe_unit(),
                            stripe_count=self.image.stripe_count(),
                            data_pool=None)
            child.remove_snap('snap1')

        with Image(ioctx, dst_name) as copy:
            copy_data = copy.read(0, 256)
            eq(b'a' * 256, copy_data)
            copy.remove_snap('snap1')
        self.rbd.remove(ioctx, dst_name)
        self.rbd.remove(ioctx, clone_name)
        self.image.unprotect_snap('snap1')
        self.image.remove_snap('snap1')

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

    def test_create_snap_exists(self):
        self.image.create_snap('snap1')
        assert_raises(ImageExists, self.image.create_snap, 'snap1')
        self.image.remove_snap('snap1')

    def test_create_snap_flags(self):
        self.image.create_snap('snap1', 0)
        self.image.remove_snap('snap1')
        self.image.create_snap('snap1', RBD_SNAP_CREATE_SKIP_QUIESCE)
        self.image.remove_snap('snap1')
        self.image.create_snap('snap1', RBD_SNAP_CREATE_IGNORE_QUIESCE_ERROR)
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

    def test_remove_snap_not_found(self):
        assert_raises(ImageNotFound, self.image.remove_snap, 'snap1')

    @require_features([RBD_FEATURE_LAYERING])
    def test_remove_snap2(self):
        self.image.create_snap('snap1')
        self.image.protect_snap('snap1')
        assert(self.image.is_protected_snap('snap1'))
        self.image.remove_snap2('snap1', RBD_SNAP_REMOVE_UNPROTECT)
        eq([], list(self.image.list_snaps()))

    def test_remove_snap_by_id(self):
        eq([], list(self.image.list_snaps()))
        self.image.create_snap('snap1')
        eq(['snap1'], [snap['name'] for snap in self.image.list_snaps()])
        for snap in self.image.list_snaps():
            snap_id = snap["id"]
        self.image.remove_snap_by_id(snap_id)
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

    def test_snap_exists(self):
        self.image.create_snap('snap1')
        eq(self.image.snap_exists('snap1'), True)
        self.image.remove_snap('snap1')
        eq(self.image.snap_exists('snap1'), False)

    def test_snap_timestamp(self):
        self.image.create_snap('snap1')
        eq(['snap1'], [snap['name'] for snap in self.image.list_snaps()])
        for snap in self.image.list_snaps():
            snap_id = snap["id"]
        time = self.image.get_snap_timestamp(snap_id)
        assert_not_equal(b'', time.year)
        assert_not_equal(0, time.year)
        assert_not_equal(time.year, '1970')
        self.image.remove_snap('snap1')

    def test_limit_snaps(self):
        self.image.set_snap_limit(2)
        eq(2, self.image.get_snap_limit())
        self.image.create_snap('snap1')
        self.image.create_snap('snap2')
        assert_raises(DiskQuotaExceeded, self.image.create_snap, 'snap3')
        self.image.remove_snap_limit()
        self.image.create_snap('snap3')

        self.image.remove_snap('snap1')
        self.image.remove_snap('snap2')
        self.image.remove_snap('snap3')

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
        assert_raises(ReadOnlyImage, self.image.write, data, 0)
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
        assert_raises(ReadOnlyImage, self.image.write, data, 0)
        self.image.set_snap(None)
        read = self.image.read(0, 256)
        eq(read, data)
        self.image.remove_snap('snap1')

    def test_set_snap_by_id(self):
        self.image.write(b'\0' * 256, 0)
        self.image.create_snap('snap1')
        read = self.image.read(0, 256)
        eq(read, b'\0' * 256)
        data = rand_data(256)
        self.image.write(data, 0)
        read = self.image.read(0, 256)
        eq(read, data)
        snaps = list(self.image.list_snaps())
        self.image.set_snap_by_id(snaps[0]['id'])
        read = self.image.read(0, 256)
        eq(read, b'\0' * 256)
        assert_raises(ReadOnlyImage, self.image.write, data, 0)
        self.image.set_snap_by_id(None)
        read = self.image.read(0, 256)
        eq(read, data)
        self.image.remove_snap('snap1')

    def test_snap_get_name(self):
        eq([], list(self.image.list_snaps()))
        self.image.create_snap('snap1')
        self.image.create_snap('snap2')
        self.image.create_snap('snap3')

        for snap in self.image.list_snaps():
            expected_snap_name = self.image.snap_get_name(snap['id'])
            eq(expected_snap_name, snap['name'])
        self.image.remove_snap('snap1')
        self.image.remove_snap('snap2')
        self.image.remove_snap('snap3')
        eq([], list(self.image.list_snaps()))

        assert_raises(ImageNotFound, self.image.snap_get_name, 1)

    def test_snap_get_id(self):
        eq([], list(self.image.list_snaps()))
        self.image.create_snap('snap1')
        self.image.create_snap('snap2')
        self.image.create_snap('snap3')

        for snap in self.image.list_snaps():
            expected_snap_id = self.image.snap_get_id(snap['name'])
            eq(expected_snap_id, snap['id'])
        self.image.remove_snap('snap1')
        self.image.remove_snap('snap2')
        self.image.remove_snap('snap3')
        eq([], list(self.image.list_snaps()))

        assert_raises(ImageNotFound, self.image.snap_get_id, 'snap1')

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
        assert_raises(ReadOnlyImage, self.image.write, data, 0)
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

    def test_aio_read(self):
        # this is a list so that the local cb() can modify it
        retval = [None]
        def cb(_, buf):
            retval[0] = buf

        # test1: success case
        comp = self.image.aio_read(0, 20, cb)
        comp.wait_for_complete_and_cb()
        eq(retval[0], b'\0' * 20)
        eq(comp.get_return_value(), 20)
        eq(sys.getrefcount(comp), 2)

        # test2: error case
        retval[0] = 1
        comp = self.image.aio_read(IMG_SIZE, 20, cb)
        comp.wait_for_complete_and_cb()
        eq(None, retval[0])
        assert(comp.get_return_value() < 0)
        eq(sys.getrefcount(comp), 2)

    def test_aio_write(self):
        retval = [None]
        def cb(comp):
            retval[0] = comp.get_return_value()

        data = rand_data(256)
        comp = self.image.aio_write(data, 256, cb)
        comp.wait_for_complete_and_cb()
        eq(retval[0], 0)
        eq(comp.get_return_value(), 0)
        eq(sys.getrefcount(comp), 2)
        eq(self.image.read(256, 256), data)

    def test_aio_discard(self):
        retval = [None]
        def cb(comp):
            retval[0] = comp.get_return_value()

        data = rand_data(256)
        self.image.write(data, 0)
        comp = self.image.aio_discard(0, 256, cb)
        comp.wait_for_complete_and_cb()
        eq(retval[0], 0)
        eq(comp.get_return_value(), 0)
        eq(sys.getrefcount(comp), 2)
        eq(self.image.read(256, 256), b'\0' * 256)

    def test_aio_flush(self):
        retval = [None]
        def cb(comp):
            retval[0] = comp.get_return_value()

        comp = self.image.aio_flush(cb)
        comp.wait_for_complete_and_cb()
        eq(retval[0], 0)
        eq(sys.getrefcount(comp), 2)

    def test_metadata(self):
        metadata = list(self.image.metadata_list())
        eq(len(metadata), 0)
        assert_raises(KeyError, self.image.metadata_get, "key1")
        self.image.metadata_set("key1", "value1")
        self.image.metadata_set("key2", "value2")
        value = self.image.metadata_get("key1")
        eq(value, "value1")
        value = self.image.metadata_get("key2")
        eq(value, "value2")
        metadata = list(self.image.metadata_list())
        eq(len(metadata), 2)
        self.image.metadata_remove("key1")
        metadata = list(self.image.metadata_list())
        eq(len(metadata), 1)
        eq(metadata[0], ("key2", "value2"))
        self.image.metadata_remove("key2")
        assert_raises(KeyError, self.image.metadata_remove, "key2")
        metadata = list(self.image.metadata_list())
        eq(len(metadata), 0)

        N = 65
        for i in range(N):
            self.image.metadata_set("key" + str(i), "X" * 1025)
        metadata = list(self.image.metadata_list())
        eq(len(metadata), N)
        for i in range(N):
            self.image.metadata_remove("key" + str(i))
            metadata = list(self.image.metadata_list())
            eq(len(metadata), N - i - 1)

    def test_watchers_list(self):
        watchers = list(self.image.watchers_list())
        # The image is open (in r/w mode) from setup, so expect there to be one
        # watcher.
        eq(len(watchers), 1)

    def test_config_list(self):
        with Image(ioctx, image_name) as image:
            for option in image.config_list():
                eq(option['source'], RBD_CONFIG_SOURCE_CONFIG)

            image.metadata_set("conf_rbd_cache", "true")

            for option in image.config_list():
                if option['name'] == "rbd_cache":
                    eq(option['source'], RBD_CONFIG_SOURCE_IMAGE)
                else:
                    eq(option['source'], RBD_CONFIG_SOURCE_CONFIG)

            image.metadata_remove("conf_rbd_cache")

            for option in image.config_list():
                eq(option['source'], RBD_CONFIG_SOURCE_CONFIG)

    def test_image_config_set_and_get_and_remove(self):
        with Image(ioctx, image_name) as image:
            for option in image.config_list():
                eq(option['source'], RBD_CONFIG_SOURCE_CONFIG)

            image.config_set("rbd_request_timed_out_seconds", "100")
            modify_value = image.config_get("rbd_request_timed_out_seconds")
            eq(modify_value, '100')

            image.config_remove("rbd_request_timed_out_seconds")

            for option in image.config_list():
                eq(option['source'], RBD_CONFIG_SOURCE_CONFIG)

    def test_sparsify(self):
        assert_raises(InvalidArgument, self.image.sparsify, 16)
        self.image.sparsify(4096)

class TestImageId(object):

    def setUp(self):
        self.rbd = RBD()
        create_image()
        self.image = Image(ioctx, image_name)
        self.image2 = Image(ioctx, None, None, False, self.image.id())

    def tearDown(self):
        self.image.close()
        self.image2.close()
        remove_image()
        self.image = None
        self.image2 = None

    def test_read(self):
        data = self.image2.read(0, 20)
        eq(data, b'\0' * 20)

    def test_write(self):
        data = rand_data(256)
        self.image2.write(data, 0)

    def test_resize(self):
        new_size = IMG_SIZE * 2
        self.image2.resize(new_size)
        info = self.image2.stat()
        check_stat(info, new_size, IMG_ORDER)

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

    def _test_with_params(self, features=None, order=None, stripe_unit=None,
                          stripe_count=None):
        self.image.create_snap('snap2')
        self.image.protect_snap('snap2')
        clone_name2 = get_temp_image_name()
        if features is None:
            self.rbd.clone(ioctx, image_name, 'snap2', ioctx, clone_name2)
        elif order is None:
            self.rbd.clone(ioctx, image_name, 'snap2', ioctx, clone_name2,
                           features)
        elif stripe_unit is None:
            self.rbd.clone(ioctx, image_name, 'snap2', ioctx, clone_name2,
                           features, order)
        elif stripe_count is None:
            self.rbd.clone(ioctx, image_name, 'snap2', ioctx, clone_name2,
                           features, order, stripe_unit)
        else:
            self.rbd.clone(ioctx, image_name, 'snap2', ioctx, clone_name2,
                           features, order, stripe_unit, stripe_count)
        self.rbd.remove(ioctx, clone_name2)
        self.image.unprotect_snap('snap2')
        self.image.remove_snap('snap2')

    def test_with_params(self):
        self._test_with_params()

    def test_with_params2(self):
        global features
        self._test_with_params(features, self.image.stat()['order'])

    @require_features([RBD_FEATURE_STRIPINGV2])
    def test_with_params3(self):
        global features
        self._test_with_params(features, self.image.stat()['order'],
                               self.image.stripe_unit(),
                               self.image.stripe_count())

    def test_stripe_unit_and_count(self):
        global features
        global ioctx
        image_name = get_temp_image_name()
        RBD().create(ioctx, image_name, IMG_SIZE, IMG_ORDER, old_format=False,
                     features=int(features), stripe_unit=1048576, stripe_count=8)
        image = Image(ioctx, image_name)
        image.create_snap('snap1')
        image.protect_snap('snap1')
        clone_name = get_temp_image_name()
        RBD().clone(ioctx, image_name, 'snap1', ioctx, clone_name)
        clone = Image(ioctx, clone_name)

        eq(1048576, clone.stripe_unit())
        eq(8, clone.stripe_count())

        clone.close()
        RBD().remove(ioctx, clone_name)
        image.unprotect_snap('snap1')
        image.remove_snap('snap1')
        image.close()
        RBD().remove(ioctx, image_name)


    def test_unprotected(self):
        self.image.create_snap('snap2')
        global features
        clone_name2 = get_temp_image_name()
        rados.conf_set("rbd_default_clone_format", "1")
        assert_raises(InvalidArgument, self.rbd.clone, ioctx, image_name,
                      'snap2', ioctx, clone_name2, features)
        rados.conf_set("rbd_default_clone_format", "auto")
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
        eq(self.image.id(), self.clone.parent_id())

        # create a new pool...
        pool_name2 = get_temp_pool_name()
        rados.create_pool(pool_name2)
        other_ioctx = rados.open_ioctx(pool_name2)
        other_ioctx.application_enable('rbd')

        # ...with a clone of the same parent
        other_clone_name = get_temp_image_name()
        rados.conf_set("rbd_default_clone_format", "1")
        self.rbd.clone(ioctx, image_name, 'snap1', other_ioctx,
                       other_clone_name, features)
        rados.conf_set("rbd_default_clone_format", "auto")
        self.other_clone = Image(other_ioctx, other_clone_name)
        # validate its parent info
        (pool, image, snap) = self.other_clone.parent_info()
        eq(pool, pool_name)
        eq(image, image_name)
        eq(snap, 'snap1')
        eq(self.image.id(), self.other_clone.parent_id())

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

    def check_children2(self, expected):
        actual = [{k:v for k,v in x.items() if k in expected[0]} \
                  for x in self.image.list_children2()]
        eq(actual, expected)

    def check_descendants(self, expected):
        eq(list(self.image.list_descendants()), expected)

    def get_image_id(self, ioctx, name):
        with Image(ioctx, name) as image:
            return image.id()

    def test_list_children(self):
        global ioctx
        global features
        self.image.set_snap('snap1')
        self.check_children([(pool_name, self.clone_name)])
        self.check_children2(
            [{'pool': pool_name, 'pool_namespace': '',
              'image': self.clone_name, 'trash': False,
              'id': self.get_image_id(ioctx, self.clone_name)}])
        self.check_descendants(
            [{'pool': pool_name, 'pool_namespace': '',
              'image': self.clone_name, 'trash': False,
              'id': self.get_image_id(ioctx, self.clone_name)}])
        self.clone.close()
        self.rbd.remove(ioctx, self.clone_name)
        eq(self.image.list_children(), [])
        eq(list(self.image.list_children2()), [])
        eq(list(self.image.list_descendants()), [])

        clone_name = get_temp_image_name() + '_'
        expected_children = []
        expected_children2 = []
        for i in range(10):
            self.rbd.clone(ioctx, image_name, 'snap1', ioctx,
                           clone_name + str(i), features)
            expected_children.append((pool_name, clone_name + str(i)))
            expected_children2.append(
                {'pool': pool_name, 'pool_namespace': '',
                 'image': clone_name + str(i), 'trash': False,
                 'id': self.get_image_id(ioctx, clone_name + str(i))})
            self.check_children(expected_children)
            self.check_children2(expected_children2)
            self.check_descendants(expected_children2)

        image6_id = self.get_image_id(ioctx, clone_name + str(5))
        RBD().trash_move(ioctx, clone_name + str(5), 0)
        expected_children.remove((pool_name, clone_name + str(5)))
        for item in expected_children2:
          for k, v in item.items():
            if v == image6_id:
              item["trash"] = True
        self.check_children(expected_children)
        self.check_children2(expected_children2)
        self.check_descendants(expected_children2)

        RBD().trash_restore(ioctx, image6_id, clone_name + str(5))
        expected_children.append((pool_name, clone_name + str(5)))
        for item in expected_children2:
          for k, v in item.items():
            if v == image6_id:
              item["trash"] = False
        self.check_children(expected_children)
        self.check_children2(expected_children2)
        self.check_descendants(expected_children2)

        for i in range(10):
            self.rbd.remove(ioctx, clone_name + str(i))
            expected_children.remove((pool_name, clone_name + str(i)))
            expected_children2.pop(0)
            self.check_children(expected_children)
            self.check_children2(expected_children2)
            self.check_descendants(expected_children2)

        eq(self.image.list_children(), [])
        eq(list(self.image.list_children2()), [])
        self.rbd.clone(ioctx, image_name, 'snap1', ioctx, self.clone_name,
                       features)
        self.check_children([(pool_name, self.clone_name)])
        self.check_children2(
            [{'pool': pool_name, 'pool_namespace': '',
              'image': self.clone_name, 'trash': False,
              'id': self.get_image_id(ioctx, self.clone_name)}])
        self.check_descendants(
            [{'pool': pool_name, 'pool_namespace': '',
              'image': self.clone_name, 'trash': False,
              'id': self.get_image_id(ioctx, self.clone_name)}])
        self.clone = Image(ioctx, self.clone_name)

    def test_flatten_errors(self):
        # test that we can't flatten a non-clone
        assert_raises(InvalidArgument, self.image.flatten)

        # test that we can't flatten a snapshot
        self.clone.create_snap('snap2')
        self.clone.set_snap('snap2')
        assert_raises(ReadOnlyImage, self.clone.flatten)
        self.clone.remove_snap('snap2')

    def check_flatten_with_order(self, new_order, stripe_unit=None,
                                 stripe_count=None):
        global ioctx
        global features
        clone_name2 = get_temp_image_name()
        self.rbd.clone(ioctx, image_name, 'snap1', ioctx, clone_name2,
                       features, new_order, stripe_unit, stripe_count)
        #with Image(ioctx, 'clone2') as clone:
        clone2 = Image(ioctx, clone_name2)
        clone2.flatten()
        eq(clone2.overlap(), 0)
        clone2.close()
        self.rbd.remove(ioctx, clone_name2)

        # flatten after resizing to non-block size
        self.rbd.clone(ioctx, image_name, 'snap1', ioctx, clone_name2,
                       features, new_order, stripe_unit, stripe_count)
        with Image(ioctx, clone_name2) as clone:
            clone.resize(IMG_SIZE // 2 - 1)
            clone.flatten()
            eq(0, clone.overlap())
        self.rbd.remove(ioctx, clone_name2)

        # flatten after resizing to non-block size
        self.rbd.clone(ioctx, image_name, 'snap1', ioctx, clone_name2,
                       features, new_order, stripe_unit, stripe_count)
        with Image(ioctx, clone_name2) as clone:
            clone.resize(IMG_SIZE // 2 + 1)
            clone.flatten()
            eq(clone.overlap(), 0)
        self.rbd.remove(ioctx, clone_name2)

    def test_flatten_basic(self):
        self.check_flatten_with_order(IMG_ORDER)

    def test_flatten_smaller_order(self):
        self.check_flatten_with_order(IMG_ORDER - 2, 1048576, 1)

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
                assert_raises(ImageNotFound, clone.parent_id)
                assert_raises(ImageNotFound, clone2.parent_id)
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

    def test_flatten_with_progress(self):
        d = {'received_callback': False}
        def progress_cb(current, total):
            d['received_callback'] = True
            return 0

        global ioctx
        global features
        clone_name = get_temp_image_name()
        self.rbd.clone(ioctx, image_name, 'snap1', ioctx, clone_name,
                       features, 0)
        with Image(ioctx, clone_name) as clone:
            clone.flatten(on_progress=progress_cb)
        self.rbd.remove(ioctx, clone_name)
        eq(True, d['received_callback'])

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

    def test_trash_snapshot(self):
        self.image.create_snap('snap2')
        global features
        clone_name = get_temp_image_name()
        rados.conf_set("rbd_default_clone_format", "2")
        self.rbd.clone(ioctx, image_name, 'snap2', ioctx, clone_name, features)
        rados.conf_set("rbd_default_clone_format", "auto")

        self.image.remove_snap('snap2')

        snaps = [s for s in self.image.list_snaps() if s['name'] != 'snap1']
        eq([RBD_SNAP_NAMESPACE_TYPE_TRASH], [s['namespace'] for s in snaps])
        eq([{'original_name' : 'snap2'}], [s['trash'] for s in snaps])

        self.rbd.remove(ioctx, clone_name)
        eq([], [s for s in self.image.list_snaps() if s['name'] != 'snap1'])

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
        ioctx2.close()
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
                assert_raises(ImageNotFound, image1.parent_id)
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
                image2.resize(new_size);
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
        global rados
        with Image(ioctx, image_name) as image1, Image(ioctx2, image_name) as image2:
            data = rand_data(256)
            image1.write(data, 0)
            image2.discard(0, 256)
            eq(image1.is_exclusive_lock_owner(), False)
            eq(image2.is_exclusive_lock_owner(), True)
            read = image2.read(0, 256)
            if rados.conf_get('rbd_skip_partial_discard') == 'false':
                eq(256 * b'\0', read)
            else:
                eq(data, read)

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
    def test_acquire_release_lock(self):
        with Image(ioctx, image_name) as image:
            image.lock_acquire(RBD_LOCK_MODE_EXCLUSIVE)
            image.lock_release()

    def test_break_lock(self):
        blacklist_rados = Rados(conffile='')
        blacklist_rados.connect()
        try:
            blacklist_ioctx = blacklist_rados.open_ioctx(pool_name)
            try:
                rados2.conf_set('rbd_blacklist_on_break_lock', 'true')
                with Image(ioctx2, image_name) as image, \
                     Image(blacklist_ioctx, image_name) as blacklist_image:

                    lock_owners = list(image.lock_get_owners())
                    eq(0, len(lock_owners))

                    blacklist_image.lock_acquire(RBD_LOCK_MODE_EXCLUSIVE)
                    assert_raises(ReadOnlyImage, image.lock_acquire,
                                  RBD_LOCK_MODE_EXCLUSIVE)
                    lock_owners = list(image.lock_get_owners())
                    eq(1, len(lock_owners))
                    eq(RBD_LOCK_MODE_EXCLUSIVE, lock_owners[0]['mode'])
                    image.lock_break(RBD_LOCK_MODE_EXCLUSIVE,
                                     lock_owners[0]['owner'])

                    assert_raises(ConnectionShutdown,
                                  blacklist_image.is_exclusive_lock_owner)

                    blacklist_rados.wait_for_latest_osdmap()
                    data = rand_data(256)
                    assert_raises(ConnectionShutdown,
                                  blacklist_image.write, data, 0)

                    image.lock_acquire(RBD_LOCK_MODE_EXCLUSIVE)

                    try:
                        blacklist_image.close()
                    except ConnectionShutdown:
                        pass
            finally:
                blacklist_ioctx.close()
        finally:
            blacklist_rados.shutdown()

class TestMirroring(object):

    @staticmethod
    def check_info(info, global_id, state, primary=None):
        eq(global_id, info['global_id'])
        eq(state, info['state'])
        if primary is not None:
            eq(primary, info['primary'])

    def setUp(self):
        self.rbd = RBD()
        self.initial_mirror_mode = self.rbd.mirror_mode_get(ioctx)
        self.rbd.mirror_mode_set(ioctx, RBD_MIRROR_MODE_POOL)
        create_image()
        self.image = Image(ioctx, image_name)

    def tearDown(self):
        self.image.close()
        remove_image()
        self.rbd.mirror_mode_set(ioctx, self.initial_mirror_mode)

    def test_uuid(self):
        mirror_uuid = self.rbd.mirror_uuid_get(ioctx)
        assert(mirror_uuid)

    def test_site_name(self):
        site_name = "us-west-1"
        self.rbd.mirror_site_name_set(rados, site_name)
        eq(site_name, self.rbd.mirror_site_name_get(rados))
        self.rbd.mirror_site_name_set(rados, "")
        eq(rados.get_fsid(), self.rbd.mirror_site_name_get(rados))

    def test_mirror_peer_bootstrap(self):
        eq([], list(self.rbd.mirror_peer_list(ioctx)))

        self.rbd.mirror_mode_set(ioctx, RBD_MIRROR_MODE_DISABLED)
        assert_raises(InvalidArgument, self.rbd.mirror_peer_bootstrap_create,
                      ioctx);

        self.rbd.mirror_mode_set(ioctx, RBD_MIRROR_MODE_POOL)
        token_b64 = self.rbd.mirror_peer_bootstrap_create(ioctx)
        token = base64.b64decode(token_b64)
        token_dict = json.loads(token)
        eq(sorted(['fsid', 'client_id', 'key', 'mon_host']),
            sorted(list(token_dict.keys())))

        # requires different cluster
        assert_raises(InvalidArgument, self.rbd.mirror_peer_bootstrap_import,
            ioctx, RBD_MIRROR_PEER_DIRECTION_RX, token_b64)

    def test_mirror_peer(self):
        eq([], list(self.rbd.mirror_peer_list(ioctx)))
        site_name = "test_site"
        client_name = "test_client"
        uuid = self.rbd.mirror_peer_add(ioctx, site_name, client_name,
                                        direction=RBD_MIRROR_PEER_DIRECTION_RX_TX)
        assert(uuid)
        peer = {
            'uuid' : uuid,
            'direction': RBD_MIRROR_PEER_DIRECTION_RX_TX,
            'site_name' : site_name,
            'cluster_name' : site_name,
            'mirror_uuid': '',
            'client_name' : client_name,
            }
        eq([peer], list(self.rbd.mirror_peer_list(ioctx)))
        cluster_name = "test_cluster1"
        self.rbd.mirror_peer_set_cluster(ioctx, uuid, cluster_name)
        client_name = "test_client1"
        self.rbd.mirror_peer_set_client(ioctx, uuid, client_name)
        peer = {
            'uuid' : uuid,
            'direction': RBD_MIRROR_PEER_DIRECTION_RX_TX,
            'site_name' : cluster_name,
            'cluster_name' : cluster_name,
            'mirror_uuid': '',
            'client_name' : client_name,
            }
        eq([peer], list(self.rbd.mirror_peer_list(ioctx)))

        attribs = {
            RBD_MIRROR_PEER_ATTRIBUTE_NAME_MON_HOST: 'host1',
            RBD_MIRROR_PEER_ATTRIBUTE_NAME_KEY: 'abc'
            }
        self.rbd.mirror_peer_set_attributes(ioctx, uuid, attribs)
        eq(attribs, self.rbd.mirror_peer_get_attributes(ioctx, uuid))

        self.rbd.mirror_peer_remove(ioctx, uuid)
        eq([], list(self.rbd.mirror_peer_list(ioctx)))

    @require_features([RBD_FEATURE_EXCLUSIVE_LOCK,
                       RBD_FEATURE_JOURNALING])
    def test_mirror_image(self):

        self.rbd.mirror_mode_set(ioctx, RBD_MIRROR_MODE_IMAGE)
        self.image.mirror_image_disable(True)
        info = self.image.mirror_image_get_info()
        self.check_info(info, '', RBD_MIRROR_IMAGE_DISABLED, False)

        self.image.mirror_image_enable()
        info = self.image.mirror_image_get_info()
        global_id = info['global_id']
        self.check_info(info, global_id, RBD_MIRROR_IMAGE_ENABLED, True)

        self.rbd.mirror_mode_set(ioctx, RBD_MIRROR_MODE_POOL)
        fail = False
        try:
            self.image.mirror_image_disable(True)
        except InvalidArgument:
            fail = True
        eq(True, fail) # Fails because of mirror mode pool

        self.image.mirror_image_demote()
        info = self.image.mirror_image_get_info()
        self.check_info(info, global_id, RBD_MIRROR_IMAGE_ENABLED, False)

        entries = dict(self.rbd.mirror_image_info_list(ioctx))
        info['mode'] = RBD_MIRROR_IMAGE_MODE_JOURNAL;
        eq(info, entries[self.image.id()])

        self.image.mirror_image_resync()

        self.image.mirror_image_promote(True)
        info = self.image.mirror_image_get_info()
        self.check_info(info, global_id, RBD_MIRROR_IMAGE_ENABLED, True)

        entries = dict(self.rbd.mirror_image_info_list(ioctx))
        info['mode'] = RBD_MIRROR_IMAGE_MODE_JOURNAL;
        eq(info, entries[self.image.id()])

        fail = False
        try:
            self.image.mirror_image_resync()
        except InvalidArgument:
            fail = True
        eq(True, fail) # Fails because it is primary

        status = self.image.mirror_image_get_status()
        eq(image_name, status['name'])
        eq(False, status['up'])
        eq(MIRROR_IMAGE_STATUS_STATE_UNKNOWN, status['state'])
        info = status['info']
        self.check_info(info, global_id, RBD_MIRROR_IMAGE_ENABLED, True)

    @require_features([RBD_FEATURE_EXCLUSIVE_LOCK,
                       RBD_FEATURE_JOURNALING])
    def test_mirror_image_status(self):
        info = self.image.mirror_image_get_info()
        global_id = info['global_id']
        state = info['state']
        primary = info['primary']

        status = self.image.mirror_image_get_status()
        eq(image_name, status['name'])
        eq(False, status['up'])
        eq(MIRROR_IMAGE_STATUS_STATE_UNKNOWN, status['state'])
        eq([], status['remote_statuses'])
        info = status['info']
        self.check_info(info, global_id, state, primary)

        images = list(self.rbd.mirror_image_status_list(ioctx))
        eq(1, len(images))
        status = images[0]
        eq(image_name, status['name'])
        eq(False, status['up'])
        eq(MIRROR_IMAGE_STATUS_STATE_UNKNOWN, status['state'])
        info = status['info']
        self.check_info(info, global_id, state)

        states = self.rbd.mirror_image_status_summary(ioctx)
        eq([(MIRROR_IMAGE_STATUS_STATE_UNKNOWN, 1)], states)

        assert_raises(ImageNotFound, self.image.mirror_image_get_instance_id)
        instance_ids = list(self.rbd.mirror_image_instance_id_list(ioctx))
        eq(0, len(instance_ids))

        N = 65
        for i in range(N):
            self.rbd.create(ioctx, image_name + str(i), IMG_SIZE, IMG_ORDER,
                            old_format=False, features=int(features))
        images = list(self.rbd.mirror_image_status_list(ioctx))
        eq(N + 1, len(images))
        for i in range(N):
            self.rbd.remove(ioctx, image_name + str(i))

    def test_mirror_image_create_snapshot(self):
        assert_raises(InvalidArgument, self.image.mirror_image_create_snapshot)

        peer1_uuid = self.rbd.mirror_peer_add(ioctx, "cluster1", "client")
        peer2_uuid = self.rbd.mirror_peer_add(ioctx, "cluster2", "client")
        self.rbd.mirror_mode_set(ioctx, RBD_MIRROR_MODE_IMAGE)
        self.image.mirror_image_disable(False)
        self.image.mirror_image_enable(RBD_MIRROR_IMAGE_MODE_SNAPSHOT)
        mode = self.image.mirror_image_get_mode()
        eq(RBD_MIRROR_IMAGE_MODE_SNAPSHOT, mode)

        snaps = list(self.image.list_snaps())
        eq(1, len(snaps))
        snap = snaps[0]
        eq(snap['namespace'], RBD_SNAP_NAMESPACE_TYPE_MIRROR)
        eq(RBD_SNAP_MIRROR_STATE_PRIMARY, snap['mirror']['state'])

        info = self.image.mirror_image_get_info()
        eq(True, info['primary'])
        entries = dict(
            self.rbd.mirror_image_info_list(ioctx,
                                            RBD_MIRROR_IMAGE_MODE_SNAPSHOT))
        info['mode'] = RBD_MIRROR_IMAGE_MODE_SNAPSHOT;
        eq(info, entries[self.image.id()])

        snap_id = self.image.mirror_image_create_snapshot(
            RBD_SNAP_CREATE_SKIP_QUIESCE)

        snaps = list(self.image.list_snaps())
        eq(2, len(snaps))
        snap = snaps[0]
        eq(snap['namespace'], RBD_SNAP_NAMESPACE_TYPE_MIRROR)
        eq(RBD_SNAP_MIRROR_STATE_PRIMARY, snap['mirror']['state'])
        snap = snaps[1]
        eq(snap['id'], snap_id)
        eq(snap['namespace'], RBD_SNAP_NAMESPACE_TYPE_MIRROR)
        eq(RBD_SNAP_MIRROR_STATE_PRIMARY, snap['mirror']['state'])
        eq(sorted([peer1_uuid, peer2_uuid]),
           sorted(snap['mirror']['mirror_peer_uuids']))

        eq(RBD_SNAP_NAMESPACE_TYPE_MIRROR,
           self.image.snap_get_namespace_type(snap_id))
        mirror_snap = self.image.snap_get_mirror_namespace(snap_id)
        eq(mirror_snap, snap['mirror'])

        self.image.mirror_image_demote()

        assert_raises(InvalidArgument, self.image.mirror_image_create_snapshot)

        snaps = list(self.image.list_snaps())
        eq(3, len(snaps))
        snap = snaps[0]
        eq(snap['namespace'], RBD_SNAP_NAMESPACE_TYPE_MIRROR)
        snap = snaps[1]
        eq(snap['id'], snap_id)
        eq(snap['namespace'], RBD_SNAP_NAMESPACE_TYPE_MIRROR)
        snap = snaps[2]
        eq(snap['namespace'], RBD_SNAP_NAMESPACE_TYPE_MIRROR)
        eq(RBD_SNAP_MIRROR_STATE_PRIMARY_DEMOTED, snap['mirror']['state'])
        eq(sorted([peer1_uuid, peer2_uuid]),
           sorted(snap['mirror']['mirror_peer_uuids']))

        self.rbd.mirror_peer_remove(ioctx, peer1_uuid)
        self.rbd.mirror_peer_remove(ioctx, peer2_uuid)
        self.image.mirror_image_promote(False)

class TestTrash(object):

    def setUp(self):
        global rados2
        rados2 = Rados(conffile='')
        rados2.connect()
        global ioctx2
        ioctx2 = rados2.open_ioctx(pool_name)

    def tearDown(self):
        global ioctx2
        ioctx2.close()
        global rados2
        rados2.shutdown()

    def test_move(self):
        create_image()
        with Image(ioctx, image_name) as image:
            image_id = image.id()

        RBD().trash_move(ioctx, image_name, 1000)
        RBD().trash_remove(ioctx, image_id, True)

    def test_purge(self):
        create_image()
        with Image(ioctx, image_name) as image:
            image_name1 = image_name
            image_id1 = image.id()

        create_image()
        with Image(ioctx, image_name) as image:
            image_name2 = image_name
            image_id2 = image.id()

        RBD().trash_move(ioctx, image_name1, 0)
        RBD().trash_move(ioctx, image_name2, 1000)
        RBD().trash_purge(ioctx, datetime.now())

        entries = list(RBD().trash_list(ioctx))
        eq([image_id2], [x['id'] for x in entries])
        RBD().trash_remove(ioctx, image_id2, True)

    def test_remove_denied(self):
        create_image()
        with Image(ioctx, image_name) as image:
            image_id = image.id()

        RBD().trash_move(ioctx, image_name, 1000)
        assert_raises(PermissionError, RBD().trash_remove, ioctx, image_id)
        RBD().trash_remove(ioctx, image_id, True)

    def test_remove(self):
        create_image()
        with Image(ioctx, image_name) as image:
            image_id = image.id()

        RBD().trash_move(ioctx, image_name, 0)
        RBD().trash_remove(ioctx, image_id)

    def test_remove_with_progress(self):
        d = {'received_callback': False}
        def progress_cb(current, total):
            d['received_callback'] = True
            return 0

        create_image()
        with Image(ioctx, image_name) as image:
            image_id = image.id()

        RBD().trash_move(ioctx, image_name, 0)
        RBD().trash_remove(ioctx, image_id, on_progress=progress_cb)
        eq(True, d['received_callback'])

    def test_get(self):
        create_image()
        with Image(ioctx, image_name) as image:
            image_id = image.id()

        RBD().trash_move(ioctx, image_name, 1000)

        info = RBD().trash_get(ioctx, image_id)
        eq(image_id, info['id'])
        eq(image_name, info['name'])
        eq('USER', info['source'])
        assert(info['deferment_end_time'] > info['deletion_time'])

        RBD().trash_remove(ioctx, image_id, True)

    def test_list(self):
        create_image()
        with Image(ioctx, image_name) as image:
            image_id1 = image.id()
            image_name1 = image_name
        RBD().trash_move(ioctx, image_name, 1000)

        create_image()
        with Image(ioctx, image_name) as image:
            image_id2 = image.id()
            image_name2 = image_name
        RBD().trash_move(ioctx, image_name, 1000)

        entries = list(RBD().trash_list(ioctx))
        for e in entries:
            if e['id'] == image_id1:
                eq(e['name'], image_name1)
            elif e['id'] == image_id2:
                eq(e['name'], image_name2)
            else:
                assert False
            eq(e['source'], 'USER')
            assert e['deferment_end_time'] > e['deletion_time']

        RBD().trash_remove(ioctx, image_id1, True)
        RBD().trash_remove(ioctx, image_id2, True)

    def test_restore(self):
        create_image()
        with Image(ioctx, image_name) as image:
            image_id = image.id()
        RBD().trash_move(ioctx, image_name, 1000)
        RBD().trash_restore(ioctx, image_id, image_name)
        remove_image()

def test_create_group():
    create_group()
    remove_group()

def test_rename_group():
    create_group()
    if group_name is not None:
        rename_group()
        eq(["new" + group_name], RBD().group_list(ioctx))
        RBD().group_remove(ioctx, "new" + group_name)
    else:
        remove_group()

def test_list_groups_empty():
    eq([], RBD().group_list(ioctx))

@with_setup(create_group, remove_group)
def test_list_groups():
    eq([group_name], RBD().group_list(ioctx))

@with_setup(create_group)
def test_list_groups_after_removed():
    remove_group()
    eq([], RBD().group_list(ioctx))

class TestGroups(object):

    def setUp(self):
        global snap_name
        self.rbd = RBD()
        create_image()
        self.image_names = [image_name]
        self.image = Image(ioctx, image_name)

        create_group()
        snap_name = get_temp_snap_name()
        self.group = Group(ioctx, group_name)

    def tearDown(self):
        remove_group()
        self.image = None
        for name in self.image_names:
            RBD().remove(ioctx, name)

    def test_group_image_add(self):
        self.group.add_image(ioctx, image_name)

    def test_group_image_list_empty(self):
        eq([], list(self.group.list_images()))

    def test_group_image_list(self):
        eq([], list(self.group.list_images()))
        self.group.add_image(ioctx, image_name)
        eq([image_name], [img['name'] for img in self.group.list_images()])

    def test_group_image_list_move_to_trash(self):
        eq([], list(self.group.list_images()))
        with Image(ioctx, image_name) as image:
            image_id = image.id()
        self.group.add_image(ioctx, image_name)
        eq([image_name], [img['name'] for img in self.group.list_images()])
        RBD().trash_move(ioctx, image_name, 0)
        eq([], list(self.group.list_images()))
        RBD().trash_restore(ioctx, image_id, image_name)

    def test_group_image_many_images(self):
        eq([], list(self.group.list_images()))
        self.group.add_image(ioctx, image_name)

        for x in range(0, 20):
            create_image()
            self.image_names.append(image_name)
            self.group.add_image(ioctx, image_name)

        self.image_names.sort()
        answer = [img['name'] for img in self.group.list_images()]
        answer.sort()
        eq(self.image_names, answer)

    def test_group_image_remove(self):
        eq([], list(self.group.list_images()))
        self.group.add_image(ioctx, image_name)
        with Image(ioctx, image_name) as image:
            eq(RBD_OPERATION_FEATURE_GROUP,
               image.op_features() & RBD_OPERATION_FEATURE_GROUP)
            group = image.group()
            eq(group_name, group['name'])

        eq([image_name], [img['name'] for img in self.group.list_images()])
        self.group.remove_image(ioctx, image_name)
        eq([], list(self.group.list_images()))
        with Image(ioctx, image_name) as image:
            eq(0, image.op_features() & RBD_OPERATION_FEATURE_GROUP)

    def test_group_snap(self):
        global snap_name
        eq([], list(self.group.list_snaps()))
        self.group.create_snap(snap_name)
        eq([snap_name], [snap['name'] for snap in self.group.list_snaps()])

        for snap in self.image.list_snaps():
            eq(rbd.RBD_SNAP_NAMESPACE_TYPE_GROUP, snap['namespace'])
            info = snap['group']
            eq(group_name, info['group_name'])
            eq(snap_name, info['group_snap_name'])

        self.group.remove_snap(snap_name)
        eq([], list(self.group.list_snaps()))

    def test_group_snap_list_many(self):
        global snap_name
        eq([], list(self.group.list_snaps()))
        snap_names = []
        for x in range(0, 20):
            snap_names.append(snap_name)
            self.group.create_snap(snap_name)
            snap_name = get_temp_snap_name()

        snap_names.sort()
        answer = [snap['name'] for snap in self.group.list_snaps()]
        answer.sort()
        eq(snap_names, answer)

    def test_group_snap_namespace(self):
        global snap_name
        eq([], list(self.group.list_snaps()))
        self.group.add_image(ioctx, image_name)
        self.group.create_snap(snap_name)
        eq(1, len([snap['name'] for snap in self.image.list_snaps()]))
        self.group.remove_image(ioctx, image_name)
        self.group.remove_snap(snap_name)
        eq([], list(self.group.list_snaps()))

    def test_group_snap_rename(self):
        global snap_name
        new_snap_name = "new" + snap_name

        eq([], list(self.group.list_snaps()))
        self.group.create_snap(snap_name)
        eq([snap_name], [snap['name'] for snap in self.group.list_snaps()])
        self.group.rename_snap(snap_name, new_snap_name)
        eq([new_snap_name], [snap['name'] for snap in self.group.list_snaps()])
        self.group.remove_snap(new_snap_name)
        eq([], list(self.group.list_snaps()))

    def test_group_snap_rollback(self):
        eq([], list(self.group.list_images()))
        self.group.add_image(ioctx, image_name)
        with Image(ioctx, image_name) as image:
            image.write(b'\0' * 256, 0)
            read = image.read(0, 256)
            eq(read, b'\0' * 256)

        global snap_name
        eq([], list(self.group.list_snaps()))
        self.group.create_snap(snap_name)
        eq([snap_name], [snap['name'] for snap in self.group.list_snaps()])

        with Image(ioctx, image_name) as image:
            data = rand_data(256)
            image.write(data, 0)
            read = image.read(0, 256)
            eq(read, data)

        self.group.rollback_to_snap(snap_name)
        with Image(ioctx, image_name) as image:
            read = image.read(0, 256)
            eq(read, b'\0' * 256)

        self.group.remove_image(ioctx, image_name)
        eq([], list(self.group.list_images()))
        self.group.remove_snap(snap_name)
        eq([], list(self.group.list_snaps()))

@with_setup(create_image, remove_image)
def test_rename():
    rbd = RBD()
    image_name2 = get_temp_image_name()

class TestMigration(object):

    def test_migration(self):
        create_image()
        RBD().migration_prepare(ioctx, image_name, ioctx, image_name, features=63,
                                order=23, stripe_unit=1<<23, stripe_count=1,
                                data_pool=None)

        status = RBD().migration_status(ioctx, image_name)
        eq(image_name, status['source_image_name'])
        eq(image_name, status['dest_image_name'])
        eq(RBD_IMAGE_MIGRATION_STATE_PREPARED, status['state'])

        RBD().migration_execute(ioctx, image_name)
        RBD().migration_commit(ioctx, image_name)
        remove_image()

    def test_migration_with_progress(self):
        d = {'received_callback': False}
        def progress_cb(current, total):
            d['received_callback'] = True
            return 0

        create_image()
        RBD().migration_prepare(ioctx, image_name, ioctx, image_name, features=63,
                                order=23, stripe_unit=1<<23, stripe_count=1,
                                data_pool=None)
        RBD().migration_execute(ioctx, image_name, on_progress=progress_cb)
        eq(True, d['received_callback'])
        d['received_callback'] = False

        RBD().migration_commit(ioctx, image_name, on_progress=progress_cb)
        eq(True, d['received_callback'])
        remove_image()

    def test_migrate_abort(self):
        create_image()
        RBD().migration_prepare(ioctx, image_name, ioctx, image_name, features=63,
                                order=23, stripe_unit=1<<23, stripe_count=1,
                                data_pool=None)
        RBD().migration_abort(ioctx, image_name)
        remove_image()

    def test_migrate_abort_with_progress(self):
        d = {'received_callback': False}
        def progress_cb(current, total):
            d['received_callback'] = True
            return 0

        create_image()
        RBD().migration_prepare(ioctx, image_name, ioctx, image_name, features=63,
                                order=23, stripe_unit=1<<23, stripe_count=1,
                                data_pool=None)
        RBD().migration_abort(ioctx, image_name, on_progress=progress_cb)
        eq(True, d['received_callback'])
        remove_image()
