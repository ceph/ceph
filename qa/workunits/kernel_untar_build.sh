#!/bin/bash

set -e

wget http://ceph.newdream.net/qa/linux-2.6.33.tar.bz2
mkdir t
cd t
tar jxvf ../linux*
cd linux*
make defconfig
make -j`grep -c processor /proc/cpuinfo`
cd ..
rm -r linux*
cd ..
rm -r t linux*
