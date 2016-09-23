#!/bin/bash

set -e

T=$(mktemp -d tmp.XXXXXX)

(
    cd "$T"
    wget -q http://download.ceph.com/qa/linux-4.0.5.tar.xz

    tar Jxvf linux*.xz
    cd linux*
    make defconfig
    make -j`grep -c processor /proc/cpuinfo`
    cd ..
    if ! rm -rv linux* ; then
        echo "uh oh rm -r failed, it left behind:"
        find .
        exit 1
    fi
)

rm -rf "$T"
