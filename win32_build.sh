#!/usr/bin/env bash

set -e

SCRIPT_DIR="$(dirname "$BASH_SOURCE")"
SCRIPT_DIR="$(realpath "$SCRIPT_DIR")"

num_vcpus=$(( $(lscpu -p | tail -1 | cut -d "," -f 1) + 1 ))

CEPH_DIR="${CEPH_DIR:-$SCRIPT_DIR}"
BUILD_DIR="${BUILD_DIR:-${CEPH_DIR}/build}"
DEPS_DIR="${DEPS_DIR:-$CEPH_DIR/build.deps}"

CLEAN_BUILD=${CLEAN_BUILD:-}
SKIP_BUILD=${SKIP_BUILD:-}
NUM_WORKERS=${NUM_WORKERS:-$num_vcpus}
NINJA_BUILD=${NINJA_BUILD:-}
DEV_BUILD=${DEV_BUILD:-}

# We'll have to be explicit here since auto-detecting doesn't work
# properly when cross compiling.
ALLOCATOR=${ALLOCATOR:-libc}
# Debug builds don't work with MINGW for the time being, failing with
# can't close <file>: File too big
# -Wa,-mbig-obj does not help.
CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE:-RelWithDebInfo}

depsSrcDir="$DEPS_DIR/src"
depsToolsetDir="$DEPS_DIR/mingw"

lz4Dir="${depsToolsetDir}/lz4"
sslDir="${depsToolsetDir}/openssl"
curlDir="${depsToolsetDir}/curl"
boostDir="${depsToolsetDir}/boost"
zlibDir="${depsToolsetDir}/zlib"
backtraceDir="${depsToolsetDir}/backtrace"
snappyDir="${depsToolsetDir}/snappy"
winLibDir="${depsToolsetDir}/windows/lib"
if [[ -n $NINJA_BUILD ]]; then
    generatorUsed="Ninja"
else
    generatorUsed="Unix Makefiles"
fi

pyVersion=`python -c "import sys; print('%d.%d' % (sys.version_info.major, sys.version_info.minor))"`

depsDirs="$lz4Dir;$curlDir;$sslDir;$boostDir;$zlibDir;$backtraceDir;$snappyDir"
depsDirs+=";$winLibDir"

# That's actually a dll, we may want to rename the file.
lz4Lib="${lz4Dir}/lib/liblz4.so.1.9.2"
lz4Include="${lz4Dir}/lib"
curlLib="${curlDir}/lib/libcurl.dll.a"
curlInclude="${curlDir}/include"

if [[ -n $CLEAN_BUILD ]]; then
    echo "Cleaning up build dir: $BUILD_DIR"
    rm -rf $BUILD_DIR
fi

if [[ ! -d $DEPS_DIR ]]; then
    echo "Preparing dependencies: $DEPS_DIR"
    NUM_WORKERS=$NUM_WORKERS DEPS_DIR=$DEPS_DIR \
        "$SCRIPT_DIR/win32_deps_build.sh"
fi

mkdir -p $BUILD_DIR
cd $BUILD_DIR

# We'll need to cross compile Boost.Python before enabling
# "WITH_MGR".
echo "Generating solution. Log: ${BUILD_DIR}/cmake.log"

# This isn't propagated to some of the subprojects, we'll use an env variable
# for now.
export CMAKE_PREFIX_PATH=$depsDirs

if [[ -n $DEV_BUILD ]]; then
  echo "Dev build enabled."
  echo "Git versioning will be disabled."
  ENABLE_GIT_VERSION="OFF"
else
  ENABLE_GIT_VERSION="ON"
fi

# As opposed to Linux, Windows shared libraries can't have unresolved
# symbols. Until we fix the dependencies (which are either unspecified
# or circular), we'll have to stick to static linking.
cmake -D CMAKE_PREFIX_PATH=$depsDirs \
      -D CMAKE_TOOLCHAIN_FILE="$CEPH_DIR/cmake/toolchains/mingw32.cmake" \
      -D WITH_PYTHON2=OFF -D WITH_PYTHON3=ON \
      -D MGR_PYTHON_VERSION=$pyVersion \
      -D WITH_RDMA=OFF -D WITH_OPENLDAP=OFF \
      -D WITH_GSSAPI=OFF -D WITH_FUSE=OFF -D WITH_XFS=OFF \
      -D WITH_BLUESTORE=OFF -D WITH_LEVELDB=OFF \
      -D WITH_LTTNG=OFF -D WITH_BABELTRACE=OFF \
      -D WITH_SYSTEM_BOOST=ON -D WITH_MGR=OFF \
      -D WITH_LIBCEPHFS=OFF -D WITH_KRBD=OFF -D WITH_RADOSGW=OFF \
      -D ENABLE_SHARED=OFF -D WITH_RBD=ON -D BUILD_GMOCK=OFF \
      -D WITH_CEPHFS=OFF -D WITH_MANPAGE=OFF \
      -D WITH_MGR_DASHBOARD_FRONTEND=OFF -D WITH_SYSTEMD=OFF -D WITH_TESTS=OFF \
      -D LZ4_INCLUDE_DIR=$lz4Include -D LZ4_LIBRARY=$lz4Lib \
      -D Backtrace_Header="$backtraceDir/include/backtrace.h" \
      -D Backtrace_INCLUDE_DIR="$backtraceDir/include" \
      -D Backtrace_LIBRARY="$backtraceDir/lib/libbacktrace.dll.a" \
      -D Boost_THREADAPI="pthread" \
      -D ENABLE_GIT_VERSION=$ENABLE_GIT_VERSION \
      -D ALLOCATOR="$ALLOCATOR" -D CMAKE_BUILD_TYPE=$CMAKE_BUILD_TYPE \
      -G "$generatorUsed" \
      $CEPH_DIR  2>&1 | tee "${BUILD_DIR}/cmake.log"

if [[ -z $SKIP_BUILD ]]; then
    echo "Building using $NUM_WORKERS workers. Log: ${BUILD_DIR}/build.log"
    echo "NINJA_BUILD = $NINJA_BUILD"

    # We're currently limitting the build scope to the rados/rbd binaries.
    if [[ -n $NINJA_BUILD ]]; then
        cd $BUILD_DIR
        ninja -v rados.exe rbd.exe compressor | tee "${BUILD_DIR}/build.log"
    else
        cd $BUILD_DIR/src/tools
        make -j $NUM_WORKERS 2>&1 | tee "${BUILD_DIR}/build.log"

        cd $BUILD_DIR/src/compressor
        make -j $NUM_WORKERS 2>&1 | tee -a "${BUILD_DIR}/build.log"
    fi
fi
