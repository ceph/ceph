#!/bin/bash

set -e

touch foo.$$
rados mkpool foo.$$
ceph mds add_data_pool foo.$$
setfattr -n ceph.file.layout.pool -v foo.$$ foo.$$

# cleanup
rm foo.$$
ceph mds remove_data_pool foo.$$
rados rmpool foo.$$ foo.$$ --yes-i-really-really-mean-it

echo OK
