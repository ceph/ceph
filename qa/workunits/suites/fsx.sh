#!/bin/sh -x

set -e

git clone https://git.ceph.com/xfstests-dev.git
cd xfstests-dev
# This sha1 is the latest master head and works well for our tests.
git checkout 0e5c12dfd008efc2848c98108c9237487e91ef35
make -j4
cd ..
cp xfstests-dev/ltp/fsx .

OPTIONS="-z"  # don't use zero range calls; not supported by cephfs

./fsx $OPTIONS  1MB -N 50000 -p 10000 -l 1048576
./fsx $OPTIONS  10MB -N 50000 -p 10000 -l 10485760
./fsx $OPTIONS 100MB -N 50000 -p 10000 -l 104857600
