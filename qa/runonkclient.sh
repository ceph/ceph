#!/bin/bash -x

mkdir -p testspace
/bin/mount -t ceph $1 testspace

./runallonce.sh testspace

/bin/umount testspace