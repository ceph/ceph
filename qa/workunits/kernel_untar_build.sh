#!/bin/bash

set -e

wget -O linux.tar.gz http://download.ceph.com/qa/linux-4.17.tar.gz

mkdir t
cd t
tar xzf ../linux.tar.gz
cd linux*
make defconfig
make -j`grep -c processor /proc/cpuinfo`
cd ..
if ! rm -rv linux* ; then
    echo "uh oh rm -r failed, it left behind:"
    find .
    exit 1
fi
cd ..
rm -rv t linux*
