#!/usr/bin/env bash

set -ex

KEYRING=$(mktemp)
trap cleanup EXIT ERR HUP INT QUIT

cleanup() {
    (ceph auth del client.mon_read || true) >/dev/null 2>&1
    (ceph auth del client.mon_write || true) >/dev/null 2>&1

    rm -f $KEYRING
}

expect_false()
{
	set -x
	if "$@"; then return 1; else return 0; fi
}

create_pool_op() {
  ID=$1
  POOL=$2

  cat << EOF | CEPH_ARGS="-k $KEYRING" python3
import rados

cluster = rados.Rados(conffile="", rados_id="${ID}")
cluster.connect()
cluster.create_pool("${POOL}")
EOF
}

delete_pool_op() {
  ID=$1
  POOL=$2

  cat << EOF | CEPH_ARGS="-k $KEYRING" python3
import rados

cluster = rados.Rados(conffile="", rados_id="${ID}")
cluster.connect()
cluster.delete_pool("${POOL}")
EOF
}

create_pool_snap_op() {
  ID=$1
  POOL=$2
  SNAP=$3

  cat << EOF | CEPH_ARGS="-k $KEYRING" python3
import rados

cluster = rados.Rados(conffile="", rados_id="${ID}")
cluster.connect()
ioctx = cluster.open_ioctx("${POOL}")

ioctx.create_snap("${SNAP}")
EOF
}

remove_pool_snap_op() {
  ID=$1
  POOL=$2
  SNAP=$3

  cat << EOF | CEPH_ARGS="-k $KEYRING" python3
import rados

cluster = rados.Rados(conffile="", rados_id="${ID}")
cluster.connect()
ioctx = cluster.open_ioctx("${POOL}")

ioctx.remove_snap("${SNAP}")
EOF
}

test_pool_op()
{
    ceph auth get-or-create client.mon_read mon 'allow r' >> $KEYRING
    ceph auth get-or-create client.mon_write mon 'allow *' >> $KEYRING

    expect_false create_pool_op mon_read pool1
    create_pool_op mon_write pool1

    expect_false create_pool_snap_op mon_read pool1 snap1
    create_pool_snap_op mon_write pool1 snap1

    expect_false remove_pool_snap_op mon_read pool1 snap1
    remove_pool_snap_op mon_write pool1 snap1

    expect_false delete_pool_op mon_read pool1
    delete_pool_op mon_write pool1
}

key=`ceph auth get-or-create-key client.poolaccess1 mon 'allow r' osd 'allow *'`
rados --id poolaccess1 --key $key -p rbd ls

key=`ceph auth get-or-create-key client.poolaccess2 mon 'allow r' osd 'allow * pool=nopool'`
expect_false rados --id poolaccess2 --key $key -p rbd ls

key=`ceph auth get-or-create-key client.poolaccess3 mon 'allow r' osd 'allow rw pool=nopool'`
expect_false rados --id poolaccess3 --key $key -p rbd ls

test_pool_op

echo OK
