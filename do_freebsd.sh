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
 

[ -z "$BUILD_DIR" ] && BUILD_DIR=build

echo Keeping the old build
if [ -d ${BUILD_DIR}.old ]; then
    sudo mv ${BUILD_DIR}.old ${BUILD_DIR}.del
    sudo rm -rf ${BUILD_DIR}.del &
fi
if [ -d ${BUILD_DIR} ]; then
    sudo mv ${BUILD_DIR} ${BUILD_DIR}.old
fi

mkdir ${BUILD_DIR}
./do_cmake.sh "$*" \
	-D WITH_CCACHE=ON \
	-D CMAKE_BUILD_TYPE=Debug \
	-D CMAKE_CXX_FLAGS_DEBUG="$CMAKE_CXX_FLAGS_DEBUG" \
	-D CMAKE_C_FLAGS_DEBUG="$CMAKE_C_FLAGS_DEBUG" \
	-D ENABLE_GIT_VERSION=OFF \
	-D WITH_RADOSGW_AMQP_ENDPOINT=OFF \
	-D WITH_RADOSGW_KAFKA_ENDPOINT=OFF \
	-D WITH_SYSTEMD=OFF \
	-D WITH_SYSTEM_BOOST=ON \
	-D WITH_SYSTEM_NPM=ON \
	-D WITH_LTTNG=OFF \
	-D WITH_BABELTRACE=OFF \
	-D WITH_CRIMSON=OFF \
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
	-D WITH_JAEGER=OFF \
	2>&1 | tee cmake.log

echo -n "start building: "; date
printenv

cd ${BUILD_DIR}
  gmake -j$CPUS V=1 VERBOSE=1 
  gmake tests 
  echo -n "start testing: "; date ;
  ctest -j $CPUS || RETEST=1

echo "Testing result, retest: = " $RETEST

if [ $RETEST -eq 1 ]; then
    # make sure no leftovers are there
    killall ceph-osd || true
    killall ceph-mgr || true
    killall ceph-mds || true
    killall ceph-mon || true
    # clean up after testing
    rm -rf td/* /tmp/td src/test/td/* || true
    rm -rf /tmp/ceph-asok.* || true
    rm -rf /tmp/cores.* || true
    rm -rf /tmp/*.core || true

    ctest --output-on-failure --rerun-failed
fi

STATUS=$?

# cleanup after the fact
rm -rf /tmp/tmp* /tmp/foo /tmp/pip* /tmp/big* /tmp/pymp* $TMPDIR || true

echo -n "Ended: "; date 

return $STATUS

