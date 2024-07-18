#!/bin/sh -x

set -e

git clone https://git.ceph.com/xfstests-dev.git
cd xfstests-dev
git checkout 12973fc04fd10d4af086901e10ffa8e48866b735
make -j4
cd ..
cp xfstests-dev/ltp/fsx .

OPTIONS="-z"  # don't use zero range calls; not supported by cephfs

./fsx $OPTIONS  1MB -N 50000 -p 10000 -l 1048576
./fsx $OPTIONS  10MB -N 50000 -p 10000 -l 10485760
./fsx $OPTIONS 100MB -N 50000 -p 10000 -l 104857600
