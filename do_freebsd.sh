#!/bin/sh -xve
NPROC=`sysctl -n hw.ncpu`

# we need bash first otherwise almost nothing will work
if [ ! -L /bin/bash ]; then
    echo install bash and link /bin/bash to /usr/local/bin/bash
    echo Run:
    echo     sudo pkg install bash
    echo     ln -s /usr/local/bin/bash /bin/bash
    exit 1
fi
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
	-D CMAKE_CXX_FLAGS_DEBUG="$CXX_FLAGS_DEBUG -O0 -g" \
	-D CMAKE_C_FLAGS_DEBUG="$C_FLAGS_DEBUG -O0 -g" \
	-D ENABLE_GIT_VERSION=OFF \
	-D WITH_SYSTEM_BOOST=ON \
	-D WITH_SYSTEM_NPM=ON \
	-D WITH_LTTNG=OFF \
	-D WITH_BABELTRACE=OFF \
	-D WITH_BLKID=OFF \
	-D WITH_FUSE=ON \
	-D WITH_KRBD=OFF \
	-D WITH_XFS=OFF \
	-D WITH_KVS=OFF \
	-D CEPH_MAN_DIR=man \
	-D WITH_LIBCEPHFS=OFF \
	-D WITH_CEPHFS=OFF \
	-D WITH_MGR=YES \
	2>&1 | tee cmake.log

echo start building 
date
(cd build; gmake -j$NPROC $BUILDOPTS )
(cd build; gmake -j$NPROC $BUILDOPTS ceph-disk)
(cd build; gmake -j$NPROC $BUILDOPTS ceph-detect-init)

echo start testing 
date
# And remove cores leftover from previous runs
sudo rm -rf /tmp/cores.*
(cd build; ctest -j$NPROC || ctest --rerun-failed --output-on-failure)

