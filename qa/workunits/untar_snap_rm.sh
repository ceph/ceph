#!/bin/sh

set -e

tarball=linux-2.6.33.tar.bz2
dir=linux-2.6.33

wget http://ceph.newdream.net/qa/$tarball
tar jxvf $tarball
mkdir .snap/k
rm -rv $dir
cp -av .snap/k .
rmdir .snap/k
rm -rv k
rm $tarball