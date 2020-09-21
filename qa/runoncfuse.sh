#!/usr/bin/env bash
set -x

mkdir -p testspace
ceph-fuse testspace -m $1

./runallonce.sh testspace
killall ceph-fuse
