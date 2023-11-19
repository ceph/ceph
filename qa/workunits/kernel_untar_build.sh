#!/usr/bin/env bash

set -ex

wget -O linux.tar.xz http://download.ceph.com/qa/linux-6.5.11.tar.xz

mkdir t
cd t
tar xJf ../linux.tar.xz
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
