#!/bin/sh -ex

$CEPH_TOOL mds tell 0 injectargs '--mds-bal-interval 0'
$CEPH_TOOL mds tell 1 injectargs '--mds-bal-interval 0'
$CEPH_TOOL mds tell 2 injectargs '--mds-bal-interval 0'
$CEPH_TOOL mds tell 3 injectargs '--mds-bal-interval 0'
#$CEPH_TOOL mds tell 4 injectargs '--mds-bal-interval 0'

mkdir -p ./a/a
mkdir -p ./b/b
mkdir -p ./c/c
mkdir -p ./d/d

mount_dir=`df . | grep -o " /.*" | grep -o "/.*"`
cur_dir=`pwd`
ceph_dir=${cur_dir##$mount_dir}
$CEPH_TOOL mds tell 0 export_dir $ceph_dir/b 1
$CEPH_TOOL mds tell 0 export_dir $ceph_dir/c 2
$CEPH_TOOL mds tell 0 export_dir $ceph_dir/d 3
sleep 5

