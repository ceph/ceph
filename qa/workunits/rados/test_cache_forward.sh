#!/usr/bin/env bash

set -ex

expect_false()
{
	set -x
	if "$@"; then return 1; else return 0; fi
}

# create pools, set up tier relationship
ceph osd pool create pool 16 16
ceph osd pool application enable pool rados

# stat non-existent object doesn't hang.
expect_false rados -p pool stat doesnotexist

# create newpool and overlay
ceph osd pool create newpool 8 8
ceph osd pool application enable newpool rados
ceph osd tier add newpool pool --force-nonempty
ceph osd tier cache-mode pool forward --yes-i-really-mean-it
ceph osd tier set-overlay newpool pool

# stat non-existent object still doesn't hang.
expect_false rados -p pool stat doesnotexist

# cleanup
ceph osd tier remove-overlay newpool
ceph osd tier remove newpool pool
ceph osd pool delete newpool newpool --yes-i-really-really-mean-it
ceph osd pool delete pool pool --yes-i-really-really-mean-it

echo OK
