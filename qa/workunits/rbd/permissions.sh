#!/usr/bin/env bash
set -ex

IMAGE_FEATURES="layering,exclusive-lock,object-map,fast-diff"

clone_v2_enabled() {
    image_spec=$1
    rbd info $image_spec | grep "clone-parent"
}

create_pools() {
    ceph osd pool create images 32
    rbd pool init images
    ceph osd pool create volumes 32
    rbd pool init volumes
}

delete_pools() {
    (ceph osd pool delete images images --yes-i-really-really-mean-it || true) >/dev/null 2>&1
    (ceph osd pool delete volumes volumes --yes-i-really-really-mean-it || true) >/dev/null 2>&1

}

recreate_pools() {
    delete_pools
    create_pools
}

delete_users() {
    (ceph auth del client.volumes || true) >/dev/null 2>&1
    (ceph auth del client.images || true) >/dev/null 2>&1

    (ceph auth del client.snap_none || true) >/dev/null 2>&1
    (ceph auth del client.snap_all || true) >/dev/null 2>&1
    (ceph auth del client.snap_pool || true) >/dev/null 2>&1
    (ceph auth del client.snap_profile_all || true) >/dev/null 2>&1
    (ceph auth del client.snap_profile_pool || true) >/dev/null 2>&1

    (ceph auth del client.mon_write || true) >/dev/null 2>&1
}

create_users() {
    ceph auth get-or-create client.volumes \
	mon 'profile rbd' \
	osd 'profile rbd pool=volumes, profile rbd-read-only pool=images' \
	mgr 'profile rbd pool=volumes, profile rbd-read-only pool=images' >> $KEYRING
    ceph auth get-or-create client.images mon 'profile rbd' osd 'profile rbd pool=images' >> $KEYRING

    ceph auth get-or-create client.snap_none mon 'allow r' >> $KEYRING
    ceph auth get-or-create client.snap_all mon 'allow r' osd 'allow w' >> $KEYRING
    ceph auth get-or-create client.snap_pool mon 'allow r' osd 'allow w pool=images' >> $KEYRING
    ceph auth get-or-create client.snap_profile_all mon 'allow r' osd 'profile rbd' >> $KEYRING
    ceph auth get-or-create client.snap_profile_pool mon 'allow r' osd 'profile rbd pool=images' >> $KEYRING

    ceph auth get-or-create client.mon_write mon 'allow *' >> $KEYRING
}

expect() {

  set +e

  local expected_ret=$1
  local ret

  shift
  cmd=$@

  eval $cmd
  ret=$?

  set -e

  if [[ $ret -ne $expected_ret ]]; then
    echo "ERROR: running \'$cmd\': expected $expected_ret got $ret"
    return 1
  fi

  return 0
}

test_images_access() {
    rbd -k $KEYRING --id images create --image-format 2 --image-feature $IMAGE_FEATURES -s 1 images/foo
    rbd -k $KEYRING --id images snap create images/foo@snap
    rbd -k $KEYRING --id images snap protect images/foo@snap
    rbd -k $KEYRING --id images snap unprotect images/foo@snap
    rbd -k $KEYRING --id images snap protect images/foo@snap
    rbd -k $KEYRING --id images export images/foo@snap - >/dev/null
    expect 16 rbd -k $KEYRING --id images snap rm images/foo@snap

    rbd -k $KEYRING --id volumes clone --image-feature $IMAGE_FEATURES images/foo@snap volumes/child

    if ! clone_v2_enabled images/foo; then
        expect 16 rbd -k $KEYRING --id images snap unprotect images/foo@snap
    fi

    expect 1 rbd -k $KEYRING --id volumes snap unprotect images/foo@snap
    expect 1 rbd -k $KEYRING --id images flatten volumes/child
    rbd -k $KEYRING --id volumes flatten volumes/child
    expect 1 rbd -k $KEYRING --id volumes snap unprotect images/foo@snap
    rbd -k $KEYRING --id images snap unprotect images/foo@snap

    expect 39 rbd -k $KEYRING --id images rm images/foo
    rbd -k $KEYRING --id images snap rm images/foo@snap
    rbd -k $KEYRING --id images rm images/foo
    rbd -k $KEYRING --id volumes rm volumes/child
}

test_volumes_access() {
    rbd -k $KEYRING --id images create --image-format 2 --image-feature $IMAGE_FEATURES -s 1 images/foo
    rbd -k $KEYRING --id images snap create images/foo@snap
    rbd -k $KEYRING --id images snap protect images/foo@snap

    # commands that work with read-only access
    rbd -k $KEYRING --id volumes info images/foo@snap
    rbd -k $KEYRING --id volumes snap ls images/foo
    rbd -k $KEYRING --id volumes export images/foo - >/dev/null
    rbd -k $KEYRING --id volumes cp images/foo volumes/foo_copy
    rbd -k $KEYRING --id volumes rm volumes/foo_copy
    rbd -k $KEYRING --id volumes children images/foo@snap
    rbd -k $KEYRING --id volumes lock list images/foo

    # commands that fail with read-only access
    expect 1 rbd -k $KEYRING --id volumes resize -s 2 images/foo --allow-shrink
    expect 1 rbd -k $KEYRING --id volumes snap create images/foo@2
    expect 1 rbd -k $KEYRING --id volumes snap rollback images/foo@snap
    expect 1 rbd -k $KEYRING --id volumes snap remove images/foo@snap
    expect 1 rbd -k $KEYRING --id volumes snap purge images/foo
    expect 1 rbd -k $KEYRING --id volumes snap unprotect images/foo@snap
    expect 1 rbd -k $KEYRING --id volumes flatten images/foo
    expect 1 rbd -k $KEYRING --id volumes lock add images/foo test
    expect 1 rbd -k $KEYRING --id volumes lock remove images/foo test locker
    expect 1 rbd -k $KEYRING --id volumes ls rbd

    # create clone and snapshot
    rbd -k $KEYRING --id volumes clone --image-feature $IMAGE_FEATURES images/foo@snap volumes/child
    rbd -k $KEYRING --id volumes snap create volumes/child@snap1
    rbd -k $KEYRING --id volumes snap protect volumes/child@snap1
    rbd -k $KEYRING --id volumes snap create volumes/child@snap2

    # make sure original snapshot stays protected
    if clone_v2_enabled images/foo; then
        rbd -k $KEYRING --id volumes flatten volumes/child
        rbd -k $KEYRING --id volumes snap rm volumes/child@snap2
        rbd -k $KEYRING --id volumes snap unprotect volumes/child@snap1
    else
        expect 16 rbd -k $KEYRING --id images snap unprotect images/foo@snap
        rbd -k $KEYRING --id volumes flatten volumes/child
        expect 16 rbd -k $KEYRING --id images snap unprotect images/foo@snap
        rbd -k $KEYRING --id volumes snap rm volumes/child@snap2
        expect 16 rbd -k $KEYRING --id images snap unprotect images/foo@snap
        expect 2 rbd -k $KEYRING --id volumes snap rm volumes/child@snap2
        rbd -k $KEYRING --id volumes snap unprotect volumes/child@snap1
        expect 16 rbd -k $KEYRING --id images snap unprotect images/foo@snap
    fi

    # clean up
    rbd -k $KEYRING --id volumes snap rm volumes/child@snap1
    rbd -k $KEYRING --id images snap unprotect images/foo@snap
    rbd -k $KEYRING --id images snap rm images/foo@snap
    rbd -k $KEYRING --id images rm images/foo
    rbd -k $KEYRING --id volumes rm volumes/child
}

create_self_managed_snapshot() {
  ID=$1
  POOL=$2

  cat << EOF | CEPH_ARGS="-k $KEYRING" python3
import rados

with rados.Rados(conffile="", rados_id="${ID}") as cluster:
  ioctx = cluster.open_ioctx("${POOL}")

  snap_id = ioctx.create_self_managed_snap()
  print ("Created snap id {}".format(snap_id))
EOF
}

remove_self_managed_snapshot() {
  ID=$1
  POOL=$2

  cat << EOF | CEPH_ARGS="-k $KEYRING" python3
import rados

with rados.Rados(conffile="", rados_id="mon_write") as cluster1, \
     rados.Rados(conffile="", rados_id="${ID}") as cluster2:
  ioctx1 = cluster1.open_ioctx("${POOL}")

  snap_id = ioctx1.create_self_managed_snap()
  print ("Created snap id {}".format(snap_id))

  ioctx2 = cluster2.open_ioctx("${POOL}")

  ioctx2.remove_self_managed_snap(snap_id)
  print ("Removed snap id {}".format(snap_id))
EOF
}

test_remove_self_managed_snapshots() {
    # Ensure users cannot create self-managed snapshots w/o permissions
    expect 1 create_self_managed_snapshot snap_none images
    expect 1 create_self_managed_snapshot snap_none volumes

    create_self_managed_snapshot snap_all images
    create_self_managed_snapshot snap_all volumes

    create_self_managed_snapshot snap_pool images
    expect 1 create_self_managed_snapshot snap_pool volumes

    create_self_managed_snapshot snap_profile_all images
    create_self_managed_snapshot snap_profile_all volumes

    create_self_managed_snapshot snap_profile_pool images
    expect 1 create_self_managed_snapshot snap_profile_pool volumes

    # Ensure users cannot delete self-managed snapshots w/o permissions
    expect 1 remove_self_managed_snapshot snap_none images
    expect 1 remove_self_managed_snapshot snap_none volumes

    remove_self_managed_snapshot snap_all images
    remove_self_managed_snapshot snap_all volumes

    remove_self_managed_snapshot snap_pool images
    expect 1 remove_self_managed_snapshot snap_pool volumes

    remove_self_managed_snapshot snap_profile_all images
    remove_self_managed_snapshot snap_profile_all volumes

    remove_self_managed_snapshot snap_profile_pool images
    expect 1 remove_self_managed_snapshot snap_profile_pool volumes
}

test_rbd_support() {
    # read-only commands should work on both pools
    ceph -k $KEYRING --id volumes rbd perf image stats volumes
    ceph -k $KEYRING --id volumes rbd perf image stats images

    # read/write commands should only work on 'volumes'
    rbd -k $KEYRING --id volumes create --image-format 2 --image-feature $IMAGE_FEATURES -s 1 volumes/foo
    ceph -k $KEYRING --id volumes rbd task add remove volumes/foo
    expect 13 ceph -k $KEYRING --id volumes rbd task add remove images/foo
}

cleanup() {
    rm -f $KEYRING
}

KEYRING=$(mktemp)
trap cleanup EXIT ERR HUP INT QUIT

delete_users
create_users

recreate_pools
test_images_access

recreate_pools
test_volumes_access

test_remove_self_managed_snapshots

test_rbd_support

delete_pools
delete_users

echo OK
exit 0
