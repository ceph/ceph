#!/usr/bin/env bash

# This is a test for https://tracker.ceph.com/issues/40481.
#
# An osdmap with 60000 slots encodes to ~16M, of which the ignored portion
# is ~13M.  However in-memory osdmap is larger than ~3M: in-memory osd_addr
# array for 60000 OSDs is ~8M because of sockaddr_storage.
#
# Set mon_max_osd = 60000 in ceph.conf.

set -ex

function expect_false() {
    if "$@"; then return 1; else return 0; fi
}

function run_test() {
    local dev

    # initially tiny, grow via incrementals
    dev=$(sudo rbd map img)
    for max in 8 60 600 6000 60000; do
        ceph osd setmaxosd $max
        expect_false sudo rbd map wait_for/latest_osdmap
        xfs_io -c 'pwrite -w 0 12M' $DEV
    done
    ceph osd getcrushmap -o /dev/stdout | ceph osd setcrushmap -i /dev/stdin
    expect_false sudo rbd map wait_for/latest_osdmap
    xfs_io -c 'pwrite -w 0 12M' $DEV
    sudo rbd unmap $dev

    # initially huge, shrink via incrementals
    dev=$(sudo rbd map img)
    for max in 60000 6000 600 60 8; do
        ceph osd setmaxosd $max
        expect_false sudo rbd map wait_for/latest_osdmap
        xfs_io -c 'pwrite -w 0 12M' $DEV
    done
    ceph osd getcrushmap -o /dev/stdout | ceph osd setcrushmap -i /dev/stdin
    expect_false sudo rbd map wait_for/latest_osdmap
    xfs_io -c 'pwrite -w 0 12M' $DEV
    sudo rbd unmap $dev
}

rbd create --size 12M img
run_test
# repeat with primary affinity (adds an extra array)
ceph osd primary-affinity osd.0 0.5
run_test

echo OK
