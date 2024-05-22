#!/bin/sh

set -ex

do_tarball() {
    wget http://download.ceph.com/qa/$1
    tar xvf$2 $1
    mkdir .snap/k
    sync
    rm -rv $3
    cp -av .snap/k .
    rmdir .snap/k
    rm -rv k
    rm $1
}

do_tarball coreutils_8.5.orig.tar.gz z coreutils-8.5
do_tarball linux-2.6.33.tar.bz2 j linux-2.6.33
