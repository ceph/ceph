#!/bin/bash

set -e

touch foo.$$
rados mkpool foo.$$
poolid=$(ceph osd dump | grep "^pool" | awk '{print $2}' | tail -n 1)
ceph mds add_data_pool ${poolid}
setfattr -n ceph.file.layout.pool -v foo.$$ foo.$$

# cleanup
rados rmpool foo.$$ foo.$$ --yes-i-really-really-mean-it 
rm foo.$$
