#!/bin/sh -x

set -e

git clone https://git.ceph.com/xfstests-dev.git
cd xfstests-dev
make -j4
cd ..
cp xfstests-dev/ltp/fsx .

OPTIONS="-z"  # don't use zero range calls; not supported by cephfs

./fsx $OPTIONS  1MB -N 50000 -p 10000 -l 1048576
./fsx $OPTIONS  10MB -N 50000 -p 10000 -l 10485760
./fsx $OPTIONS 100MB -N 50000 -p 10000 -l 104857600
