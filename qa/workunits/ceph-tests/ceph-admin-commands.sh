#!/bin/sh -e

#check ceph health
ceph -s
#list pools
rados lspools
#lisr rbd images
ceph osd pool create rbd 128 128
rbd ls
#check that the monitors work
ceph osd set nodown
ceph osd unset nodown

exit 0
