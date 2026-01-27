#!/usr/bin/env bash

set -e

touch foo.$$
ceph osd pool create foo.$$ 8
ceph fs add_data_pool cephfs foo.$$
setfattr -n ceph.file.layout.pool -v foo.$$ foo.$$

# cleanup
rm foo.$$

echo OK
