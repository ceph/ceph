#!/bin/sh -ex

./ceph mds tell 0 injectargs '--mds-bal-interval 0'
./ceph mds tell 1 injectargs '--mds-bal-interval 0'
./ceph mds tell 2 injectargs '--mds-bal-interval 0'
./ceph mds tell 3 injectargs '--mds-bal-interval 0'
#./ceph mds tell 4 injectargs '--mds-bal-interval 0'

mkdir -p mnt/a/a
mkdir -p mnt/b/b
mkdir -p mnt/c/c
mkdir -p mnt/d/d

./ceph mds tell 0 export_dir /b 1
./ceph mds tell 0 export_dir /c 2
./ceph mds tell 0 export_dir /d 3
sleep 5

