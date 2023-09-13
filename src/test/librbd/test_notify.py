#!/usr/bin/python3
import os
import sys
import time

from rados import Rados
from rbd import (RBD,
                 Image,
                 ImageNotFound,
                 RBD_FEATURE_EXCLUSIVE_LOCK,
                 RBD_FEATURE_LAYERING,
                 RBD_FEATURE_OBJECT_MAP,
                 RBD_FEATURE_FAST_DIFF,
                 RBD_FLAG_OBJECT_MAP_INVALID)

POOL_NAME='rbd'
PARENT_IMG_NAME='test_notify_parent'
CLONE_IMG_NAME='test_notify_clone'
CLONE_IMG_RENAME='test_notify_clone2'
IMG_SIZE = 16 << 20
IMG_ORDER = 20

def delete_image(ioctx, img_name):
    image = Image(ioctx, img_name)
    for snap in image.list_snaps():
        snap_name = snap['name']
        print("removing snapshot: %s@%s" % (img_name, snap_name))
        if image.is_protected_snap(snap_name):
            image.unprotect_snap(snap_name)
        image.remove_snap(snap_name)
    image.close()
    print("removing image: %s" % img_name)
    RBD().remove(ioctx, img_name)

def safe_delete_image(ioctx, img_name):
    try:
        delete_image(ioctx, img_name)
    except ImageNotFound:
        pass

def get_features():
    features = os.getenv("RBD_FEATURES")
    if features is not None:
        features = int(features)
    else:
        features = int(RBD_FEATURE_EXCLUSIVE_LOCK | RBD_FEATURE_LAYERING |
                       RBD_FEATURE_OBJECT_MAP | RBD_FEATURE_FAST_DIFF)
    assert((features & RBD_FEATURE_EXCLUSIVE_LOCK) != 0)
    assert((features & RBD_FEATURE_LAYERING) != 0)
    assert((features & RBD_FEATURE_OBJECT_MAP) != 0)
    assert((features & RBD_FEATURE_FAST_DIFF) != 0)
    return features

def master(ioctx):
    print("starting master")
    safe_delete_image(ioctx, CLONE_IMG_RENAME)
    safe_delete_image(ioctx, CLONE_IMG_NAME)
    safe_delete_image(ioctx, PARENT_IMG_NAME)

    features = get_features()
    RBD().create(ioctx, PARENT_IMG_NAME, IMG_SIZE, IMG_ORDER, old_format=False,
                 features=features)
    with Image(ioctx, PARENT_IMG_NAME) as image:
        image.create_snap('snap1')
        image.protect_snap('snap1')

    features = features & ~(RBD_FEATURE_FAST_DIFF)
    RBD().clone(ioctx, PARENT_IMG_NAME, 'snap1', ioctx, CLONE_IMG_NAME,
                features=features)
    with Image(ioctx, CLONE_IMG_NAME) as image:
        print("acquiring exclusive lock")
        offset = 0
        data = os.urandom(512)
        while offset < IMG_SIZE:
            image.write(data, offset)
            offset += (1 << IMG_ORDER)
        image.write(b'1', IMG_SIZE - 1)
        assert(image.is_exclusive_lock_owner())

        print("waiting for slave to complete")
        while image.is_exclusive_lock_owner():
            time.sleep(5)

    safe_delete_image(ioctx, CLONE_IMG_RENAME)
    safe_delete_image(ioctx, CLONE_IMG_NAME)
    delete_image(ioctx, PARENT_IMG_NAME)
    print("finished")

def slave(ioctx):
    print("starting slave")

    while True:
        try:
            with Image(ioctx, CLONE_IMG_NAME) as image:
                if (image.list_lockers() != [] and
                    image.read(IMG_SIZE - 1, 1) == b'1'):
                    break
        except Exception:
            pass

    print("detected master")

    print("rename")
    RBD().rename(ioctx, CLONE_IMG_NAME, CLONE_IMG_RENAME);

    with Image(ioctx, CLONE_IMG_RENAME) as image:
        print("flatten")
        image.flatten()
        assert(not image.is_exclusive_lock_owner())

        print("resize")
        image.resize(IMG_SIZE // 2)
        assert(not image.is_exclusive_lock_owner())
        assert(image.stat()['size'] == IMG_SIZE // 2)

        print("create_snap")
        image.create_snap('snap1')
        assert(not image.is_exclusive_lock_owner())
        assert(any(snap['name'] == 'snap1'
                   for snap in image.list_snaps()))

        print("protect_snap")
        image.protect_snap('snap1')
        assert(not image.is_exclusive_lock_owner())
        assert(image.is_protected_snap('snap1'))

        print("unprotect_snap")
        image.unprotect_snap('snap1')
        assert(not image.is_exclusive_lock_owner())
        assert(not image.is_protected_snap('snap1'))

        print("rename_snap")
        image.rename_snap('snap1', 'snap1-new')
        assert(not image.is_exclusive_lock_owner())
        assert(any(snap['name'] == 'snap1-new'
                   for snap in image.list_snaps()))

        print("remove_snap")
        image.remove_snap('snap1-new')
        assert(not image.is_exclusive_lock_owner())
        assert(list(image.list_snaps()) == [])

        if 'RBD_DISABLE_UPDATE_FEATURES' not in os.environ:
            print("update_features")
            assert((image.features() & RBD_FEATURE_OBJECT_MAP) != 0)
            image.update_features(RBD_FEATURE_OBJECT_MAP, False)
            assert(not image.is_exclusive_lock_owner())
            assert((image.features() & RBD_FEATURE_OBJECT_MAP) == 0)
            image.update_features(RBD_FEATURE_OBJECT_MAP, True)
            assert(not image.is_exclusive_lock_owner())
            assert((image.features() & RBD_FEATURE_OBJECT_MAP) != 0)
            assert((image.flags() & RBD_FLAG_OBJECT_MAP_INVALID) != 0)
        else:
            print("skipping update_features")

        print("rebuild object map")
        image.rebuild_object_map()
        assert(not image.is_exclusive_lock_owner())
        assert((image.flags() & RBD_FLAG_OBJECT_MAP_INVALID) == 0)

        print("write")
        data = os.urandom(512)
        image.write(data, 0)
        assert(image.is_exclusive_lock_owner())

    print("finished")

def main():
    if len(sys.argv) != 2 or sys.argv[1] not in ['master', 'slave']:
        print("usage: %s: [master/slave]" % sys.argv[0])
        sys.exit(2)

    rados = Rados(conffile='')
    rados.connect()
    ioctx = rados.open_ioctx(POOL_NAME)
    if sys.argv[1] == 'master':
        master(ioctx)
    else:
        slave(ioctx)
    rados.shutdown()

if __name__ == "__main__":
    main()
