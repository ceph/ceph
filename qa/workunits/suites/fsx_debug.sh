#!/bin/sh -x

p() {
 echo "$*" > /sys/kernel/debug/dynamic_debug/control
}

echo 9 > /proc/sysrq-trigger
p 'module ceph +p'
p 'module libceph +p'
p 'module rbd +p'

set -e

git clone git://ceph.newdream.net/git/xfstests.git
make -C xfstests
cp xfstests/ltp/fsx .

./fsx   1MB -N 50000 -p 10000 -l 1048576
./fsx  10MB -N 50000 -p 10000 -l 10485760
./fsx 100MB -N 50000 -p 10000 -l 104857600

echo 9 > /proc/sysrq-trigger
p 'module ceph +p'
p 'module libceph +p'
p 'module rbd +p'