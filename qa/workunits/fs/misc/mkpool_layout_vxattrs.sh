#!/usr/bin/env bash

#do check to see if cwd has fscrypt configured
getfattr -n ceph.fscrypt.auth . > /dev/null 2>&1
rval=$?

if [ $rval == "0" ] ; then
    echo This directory has fscrypt enabled and layout changing is not supported, skipping!
    exit 0
fi

set -e

touch foo.$$
ceph osd pool create foo.$$ 8
ceph fs add_data_pool cephfs foo.$$
setfattr -n ceph.file.layout.pool -v foo.$$ foo.$$

# cleanup
rm foo.$$

echo OK
