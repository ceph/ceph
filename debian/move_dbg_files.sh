#!/bin/sh

for f in ceph ceph-fuse libcrush1 librados1 libceph1 radosgw
do
    echo moving $f unstripped binaries into $f-dbg
    mkdir -p debian/$f-dbg/usr/lib
    mv debian/$f/usr/lib/debug debian/$f-dbg/usr/lib
done
