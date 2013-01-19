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

for id in `ceph osd ls` ; do
	ceph tell osd.$id version
done

echo OK

