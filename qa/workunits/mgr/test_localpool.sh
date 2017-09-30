#!/bin/sh -ex

ceph config-key set mgr/localpool/subtree host
ceph config-key set mgr/localpool/failure_domain osd
ceph mgr module enable localpool

while ! ceph osd pool ls | grep '^by-host-'
do
    sleep 5
done

ceph mgr module disable localpool
for p in `ceph osd pool ls | grep '^by-host-'`
do
    ceph osd pool rm $p $p --yes-i-really-really-mean-it
done

ceph config-key rm mgr/localpool/subtree
ceph config-key rm mgr/localpool/failure_domain

echo OK
