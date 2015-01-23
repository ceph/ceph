#!/bin/bash

set -e

#wget -q http://ceph.com/qa/linux-2.6.33.tar.bz2
wget -q http://ceph.com/qa/linux-3.2.9.tar.bz2

mkdir t
cd t
tar jxvf ../linux*.bz2
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
