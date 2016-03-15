#!/bin/bash -ex

IMAGE_FEATURES="layering,exclusive-lock,object-map,fast-diff"

create_pools() {
    ceph osd pool create images 100
    ceph osd pool create volumes 100
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
}

create_users() {
    ceph auth get-or-create client.volumes mon 'allow r' osd 'allow class-read object_prefix rbd_children, allow r class-read pool images, allow rwx pool volumes' >> $KEYRING
    ceph auth get-or-create client.images mon 'allow r' osd 'allow class-read object_prefix rbd_children, allow rwx pool images' >> $KEYRING
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
    expect 16 rbd -k $KEYRING --id images snap unprotect images/foo@snap
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
    expect 16 rbd -k $KEYRING --id images snap unprotect images/foo@snap
    rbd -k $KEYRING --id volumes flatten volumes/child
    expect 16 rbd -k $KEYRING --id images snap unprotect images/foo@snap
    rbd -k $KEYRING --id volumes snap rm volumes/child@snap2
    expect 16 rbd -k $KEYRING --id images snap unprotect images/foo@snap
    expect 2 rbd -k $KEYRING --id volumes snap rm volumes/child@snap2
    rbd -k $KEYRING --id volumes snap unprotect volumes/child@snap1
    expect 16 rbd -k $KEYRING --id images snap unprotect images/foo@snap

    # clean up
    rbd -k $KEYRING --id volumes snap rm volumes/child@snap1
    rbd -k $KEYRING --id images snap unprotect images/foo@snap
    rbd -k $KEYRING --id images snap rm images/foo@snap
    rbd -k $KEYRING --id images rm images/foo
    rbd -k $KEYRING --id volumes rm volumes/child
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

delete_pools
delete_users

echo OK
exit 0
