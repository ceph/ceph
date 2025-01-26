#!/usr/bin/env bash

# test setfattr remove, and check values of vxattr
# after remove for vxattr, where possible.

set -ex

mkdir -p dir

#ceph.dir.pin test, def val -1, reset val -1
getfattr -n ceph.dir.pin dir | grep 'ceph.dir.pin="-1"'
setfattr -n ceph.dir.pin dir 2>&1 | grep "setfattr: dir: Invalid argument"
setfattr -n ceph.dir.pin -v 1 dir
getfattr -n ceph.dir.pin dir | grep 'ceph.dir.pin="1"'
setfattr -x ceph.dir.pin dir
getfattr -n ceph.dir.pin dir | grep 'ceph.dir.pin="-1"'

#TODO: Once test machines support getfattr for vxattr, uncomment getfattr below
#see: https://lists.ceph.io/hyperkitty/list/ceph-users@ceph.io/thread/EZL3POLMQLMMNBPAJ2QQ2BAKH44VUNJU/#JJNRRYLUKUAUN5HIL5A7Q4N63OCLWQXF
#for further detail

#ceph.dir.pin.distributed, def val 0, reset val 0
#getfattr -n ceph.dir.pin.distributed dir | grep 'ceph.dir.pin.distributed="0"'
setfattr -n ceph.dir.pin.distributed dir 2>&1 | grep "setfattr: dir: Invalid argument"
setfattr -n ceph.dir.pin.distributed -v 1 dir
#getfattr -n ceph.dir.pin.distributed dir | grep 'ceph.dir.pin.distributed="1"'
setfattr -x ceph.dir.pin.distributed dir
#getfattr -n ceph.dir.pin.distributed dir | grep 'ceph.dir.pin.distributed="0"'

#ceph.dir.pin.random def val 0, reset val 0
#getfattr -n ceph.dir.pin.random dir | grep 'ceph.dir.pin.random="0"'
setfattr -n ceph.dir.pin.random dir 2>&1 | grep "setfattr: dir: Invalid argument"
setfattr -n ceph.dir.pin.random -v 0.01 dir
#getfattr -n ceph.dir.pin.random dir | grep 'ceph.dir.pin.random="0.01"'
setfattr -x ceph.dir.pin.random dir
#getfattr -n ceph.dir.pin.random dir | grep 'ceph.dir.pin.random="0"'

#ceph.quota, def value 0, reset val 0
setfattr -n ceph.quota.max_bytes dir 2>&1 | grep "setfattr: dir: Invalid argument"
setfattr -n ceph.quota.max_bytes -v 100000000 dir
#getfattr -n ceph.quota.max_bytes dir | grep 'ceph.quota.max_bytes="100000000"'
setfattr -x ceph.quota.max_bytes dir
setfattr -n ceph.quota.max_files dir 2>&1 | grep "setfattr: dir: Invalid argument"
setfattr -n ceph.quota.max_files -v 10000 dir
#getfattr -n ceph.quota.max_files dir | grep 'ceph.quota.max_files="10000"'
setfattr -x ceph.quota.max_files dir

rmdir dir

echo OK

