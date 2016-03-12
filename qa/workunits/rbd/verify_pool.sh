#!/bin/sh -ex

POOL_NAME=rbd_test_validate_pool
PG_NUM=100

tear_down () {
  ceph osd pool delete $POOL_NAME $POOL_NAME --yes-i-really-really-mean-it || true
}

set_up () {
  tear_down
  ceph osd pool create $POOL_NAME $PG_NUM
  ceph osd pool mksnap $POOL_NAME snap
}

trap tear_down EXIT HUP INT
set_up

# creating an image in a pool-managed snapshot pool should fail
rbd create --pool $POOL_NAME --size 1 foo && exit 1 || true

# should succeed if images already exist in the pool
rados --pool $POOL_NAME create rbd_directory
rbd create --pool $POOL_NAME --size 1 foo

echo OK
