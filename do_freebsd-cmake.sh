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
rm -rf build && ./do_cmake.sh "$*" \
	-D CMAKE_BUILD_TYPE=Debug \
	-D ENABLE_GIT_VERSION=OFF \
	-D WITH_BLKID=OFF \
	-D WITH_FUSE=OFF \
	-D WITH_RBD=OFF \
	-D WITH_XFS=OFF \
	-D WITH_KVS=OFF \
	-D WITH_MANPAGE=OFF \
	-D WITH_LIBCEPHFS=OFF \
	-D WITH_CEPHFS=OFF \
	-D WITH_RADOSGW=OFF \
	2>&1 | tee cmake.log 

cd build 
gmake -j$NPROC V=1 VERBOSE=1 | tee build.log 2>&1
gmake -j$NPROC check CEPH_BUFFER_NO_BENCH=yes | tee check.log 2>&1

