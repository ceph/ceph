#!/bin/sh -xve
export NPROC=`sysctl -n hw.ncpu`

if [ x"$1"x = x"--deps"x ]; then
    sudo ./install-deps.sh
fi

if [ x"$CEPH_DEV"x != xx ]; then
    BUILDOPTS="$BUILDOPTS V=1 VERBOSE=1"
    CXX_FLAGS_DEBUG="-DCEPH_DEV"
    C_FLAGS_DEBUG="-DCEPH_DEV"
fi

#   To test with a new release Clang, use with cmake:
#	-D CMAKE_CXX_COMPILER="/usr/local/bin/clang++-devel" \
#	-D CMAKE_C_COMPILER="/usr/local/bin/clang-devel" \

rm -rf build && ./do_cmake.sh "$*" \
	-D CMAKE_BUILD_TYPE=Debug \
	-D CMAKE_CXX_FLAGS_DEBUG="$CXX_FLAGS_DEBUG -O0 -g" \
	-D CMAKE_C_FLAGS_DEBUG="$C_FLAGS_DEBUG -O0 -g" \
	-D ENABLE_GIT_VERSION=OFF \
	-D WITH_SYSTEM_BOOST=ON \
	-D WITH_LTTNG=OFF \
	-D WITH_BLKID=OFF \
	-D WITH_BLUESTORE=OFF \
	-D WITH_FUSE=ON \
	-D WITH_KRBD=OFF \
	-D WITH_XFS=OFF \
	-D WITH_KVS=OFF \
	-D CEPH_MAN_DIR=man \
	-D WITH_LIBCEPHFS=OFF \
	-D WITH_CEPHFS=OFF \
	-D WITH_EMBEDDED=OFF \
	-D WITH_MGR=YES \
	-D WITH_SPDK=OFF \
	2>&1 | tee cmake.log

echo start building 
date
(cd build; gmake -j$NPROC $BUILDOPTS )
(cd build; gmake -j$NPROC $BUILDOPTS ceph-disk)
(cd build; gmake -j$NPROC $BUILDOPTS ceph-detect-init)

echo start testing 
date
(cd build; ctest -j$NPROC || ctest --rerun-failed --output-on-failure)

