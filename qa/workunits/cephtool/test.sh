#!/bin/sh -x

set -e

ceph status
ceph -s
ceph quorum_status

ceph osd dump
ceph osd tree
ceph pg dump
ceph mon dump
ceph mds dump

ceph tell osd.0 version
! ceph tell osd.9999 version 
! ceph tell osd.foo version

ceph osd reweight 0 0.9
! ceph osd reweight 0 -1
ceph osd reweight 0 1

for s in pg_num pgp_num size min_size crash_replay_interval crush_ruleset; do
	ceph osd pool get data $s
done

ceph osd pool get data size | grep 'size: 2'
ceph osd pool set data size 3
ceph osd pool get data size | grep 'size: 3'
ceph osd pool set data size 2

ceph osd pool get rbd crush_ruleset | grep 'crush_ruleset: 2'

for id in `ceph osd ls` ; do
	ceph tell osd.$id version
done

echo OK

