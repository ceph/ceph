#!/bin/sh -e

#check ceph health
ceph -s
#list pools
rados lspools
#lisr rbd images
rbd ls
#check that the monitors work
ceph osd set nodown
ceph osd unset nodown

exit 0
