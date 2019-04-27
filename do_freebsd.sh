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
COMPILE_FLAGS="-O0 -g"
COMPILE_FLAGS="${COMPILE_FLAGS} -fuse-ld=/usr/local/bin/ld -Wno-unused-command-line-argument"
CMAKE_CXX_FLAGS_DEBUG="$CXX_FLAGS_DEBUG $COMPILE_FLAGS"
CMAKE_C_FLAGS_DEBUG="$C_FLAGS_DEBUG $COMPILE_FLAGS"

#
#   On FreeBSD we need to preinstall all the tools that are required for building
#   dashboard, because versions fetched are not working on FreeBSD.
 

if [ -d build ]; then
    mv build build.remove
    rm -f build.remove &
fi

./do_cmake.sh "$*" \
	-D WITH_CCACHE=ON \
	-D CMAKE_BUILD_TYPE=Debug \
	-D CMAKE_CXX_FLAGS_DEBUG="$CXX_FLAGS_DEBUG" \
	-D CMAKE_C_FLAGS_DEBUG="$C_FLAGS_DEBUG" \
	-D ENABLE_GIT_VERSION=OFF \
	-D WITH_RADOSGW_AMQP_ENDPOINT=OFF \
	-D WITH_SYSTEM_BOOST=ON \
	-D WITH_SYSTEM_NPM=ON \
	-D WITH_LTTNG=OFF \
	-D WITH_BABELTRACE=OFF \
	-D WITH_SEASTAR=OFF \
	-D WITH_BLKID=OFF \
	-D WITH_FUSE=ON \
	-D WITH_KRBD=OFF \
	-D WITH_XFS=OFF \
	-D WITH_KVS=ON \
	-D CEPH_MAN_DIR=man \
	-D WITH_LIBCEPHFS=OFF \
	-D WITH_CEPHFS=OFF \
	-D WITH_MGR=YES \
	-D WITH_RDMA=OFF \
	-D WITH_SPDK=OFF \
	2>&1 | tee cmake.log

echo start building 
date
(cd build; gmake -j$NPROC $BUILDOPTS )

echo start testing 
date
# And remove cores leftover from previous runs
sudo rm -rf /tmp/cores.*
(cd build; ctest -j$NPROC || ctest --rerun-failed --output-on-failure)

