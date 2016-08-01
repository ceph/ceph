#!/bin/sh -xve
NPROC=`sysctl -n hw.ncpu`

if [ x"$1"x = x"--deps"x ]; then
    # we need bash first otherwise almost nothing will work
    sudo pkg install bash
    if [ ! -L /bin/bash ]; then
        echo linking /bin/bash to /usr/local/bin/bash
        ln -s /usr/local/bin/bash /bin/bash
    fi
    sudo ./install-deps.sh
fi
. ./autogen_freebsd.sh
./autogen.sh
./configure ${CONFIGURE_FLAGS}
( cd src/gmock/gtest; patch < /usr/ports/devel/googletest/files/patch-bsd-defines )
gmake -j$NPROC ENABLE_GIT_VERSION=OFF
gmake -j$NPROC check ENABLE_GIT_VERSION=OFF CEPH_BUFFER_NO_BENCH=yes

