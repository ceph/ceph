#!/usr/bin/env bash

set -e

touch foo.$$
rados mkpool foo.$$
ceph fs add_data_pool cephfs foo.$$
setfattr -n ceph.file.layout.pool -v foo.$$ foo.$$

# cleanup
rm foo.$$
ceph fs rm_data_pool cephfs foo.$$
rados rmpool foo.$$ foo.$$ --yes-i-really-really-mean-it

echo OK
