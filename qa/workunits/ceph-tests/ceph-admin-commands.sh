#!/bin/sh -ex

ceph -s
rados lspools
rbd ls
# check that the monitors work
ceph osd set nodown
ceph osd unset nodown

exit 0
