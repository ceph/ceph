#!/usr/bin/env bash

set -ex

KEYRING=$(mktemp)
trap cleanup EXIT ERR HUP INT QUIT

cleanup() {
    (ceph auth del client.mon_read || true) >/dev/null 2>&1
    (ceph auth del client.mon_write || true) >/dev/null 2>&1
    (ceph auth del client.live_nocaps || true) >/dev/null 2>&1
    (ceph auth del client.live_othercaps || true) >/dev/null 2>&1
    (ceph tell mon.\* config unset auth_service_ticket_ttl || true) >/dev/null 2>&1

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

# Write as a client that gets EPERM, grant it osd caps for the pool while it
# keeps running, and wait for its writes to succeed.
grant_to_live_client_op() {
  ID=$1
  POOL=$2

  cat << EOF | CEPH_ARGS="-k $KEYRING" timeout 600 python3
import os, subprocess, sys, time
import rados

cluster = rados.Rados(conffile="", rados_id="${ID}")
cluster.connect()
ioctx = cluster.open_ioctx("${POOL}")
try:
    ioctx.write_full("${ID}", b"x")
    sys.exit("${ID}: write succeeded before the osd caps were granted")
except rados.PermissionError:
    pass

env = dict(os.environ)
del env["CEPH_ARGS"]
subprocess.check_call(["ceph", "auth", "caps", "client.${ID}",
                       "mon", "allow r", "osd", "allow rw pool=${POOL}"], env=env)
granted = time.time()
while True:
    try:
        ioctx.write_full("${ID}", b"x")
        break
    except rados.PermissionError:
        if time.time() - granted > 300:
            sys.exit("${ID}: still EPERM 300 s after the osd caps were granted")
        time.sleep(2)
print("${ID}: write succeeded %d s after the osd caps were granted" %
      (time.time() - granted))
EOF
}

test_grant_to_live_client()
{
    # Clients renew their tickets 3/4 of the way through the ticket's
    # lifetime, so with 120 s tickets the grant reaches them in about 90 s.
    ceph tell mon.\* config set auth_service_ticket_ttl 120

    ceph auth get-or-create client.live_nocaps mon 'allow r' >> $KEYRING
    grant_to_live_client_op live_nocaps rbd

    ceph auth get-or-create client.live_othercaps mon 'allow r' \
        osd 'allow rw pool=nopool' >> $KEYRING
    grant_to_live_client_op live_othercaps rbd

    ceph tell mon.\* config unset auth_service_ticket_ttl
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

# with no osd caps at all, ops must fail with EPERM instead of hanging
key=`ceph auth get-or-create-key client.poolaccess4 mon 'allow r'`
out=$(timeout 120 rados --id poolaccess4 --key $key -p rbd ls 2>&1) && exit 1
echo "$out" | grep 'Operation not permitted'

test_grant_to_live_client
test_pool_op

echo OK
