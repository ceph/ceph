#!/usr/bin/env bash

set -e
set -o pipefail

SCRIPT_DIR="$(dirname "$BASH_SOURCE")"
SCRIPT_DIR="$(realpath "$SCRIPT_DIR")"

num_vcpus=$(nproc)

CEPH_DIR="${CEPH_DIR:-$SCRIPT_DIR}"
BUILD_DIR="${BUILD_DIR:-${CEPH_DIR}/build}"
DEPS_DIR="${DEPS_DIR:-$CEPH_DIR/build.deps}"
ZIP_DEST="${ZIP_DEST:-$BUILD_DIR/ceph.zip}"

CLEAN_BUILD=${CLEAN_BUILD:-}
SKIP_BUILD=${SKIP_BUILD:-}
# Usefull when packaging existing binaries.
SKIP_CMAKE=${SKIP_CMAKE:-}
SKIP_DLL_COPY=${SKIP_DLL_COPY:-}
SKIP_TESTS=${SKIP_TESTS:-}
NUM_WORKERS=${NUM_WORKERS:-$num_vcpus}
DEV_BUILD=${DEV_BUILD:-}
BUILD_ZIP=${BUILD_ZIP:-}
# By default, we'll build release binaries with debug symbols attached.
# If BUILD_ZIP and STRIP_ZIPPED are enabled, we'll strip the binaries
# that we're going to archive.
# Unfortunately we cannot use pdb symbols when cross compiling. cv2pdb
# well as llvm rely on mspdb*.dll in order to support this proprietary format.
STRIP_ZIPPED=${STRIP_ZIPPED:-}

# We'll have to be explicit here since auto-detecting doesn't work
# properly when cross compiling.
ALLOCATOR=${ALLOCATOR:-libc}
# Debug builds don't work with MINGW for the time being, failing with
# can't close <file>: File too big
# -Wa,-mbig-obj does not help.
CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE:-RelWithDebInfo}

binDir="$BUILD_DIR/bin"
strippedBinDir="$BUILD_DIR/bin_stripped"
depsSrcDir="$DEPS_DIR/src"
depsToolsetDir="$DEPS_DIR/mingw"

generatorUsed="Unix Makefiles"
lz4Dir="${depsToolsetDir}/lz4"
sslDir="${depsToolsetDir}/openssl"
curlDir="${depsToolsetDir}/curl"
boostDir="${depsToolsetDir}/boost"
zlibDir="${depsToolsetDir}/zlib"
backtraceDir="${depsToolsetDir}/libbacktrace"
snappyDir="${depsToolsetDir}/snappy"
winLibDir="${depsToolsetDir}/windows/lib"
wnbdSrcDir="${depsSrcDir}/wnbd"
wnbdLibDir="${depsToolsetDir}/wnbd/lib"

depsDirs="$lz4Dir;$curlDir;$sslDir;$boostDir;$zlibDir;$backtraceDir;$snappyDir"
depsDirs+=";$winLibDir"

lz4Lib="${lz4Dir}/lib/dll/liblz4-1.dll"
lz4Include="${lz4Dir}/lib"
curlLib="${curlDir}/lib/libcurl.dll.a"
curlInclude="${curlDir}/include"

if [[ -n $CLEAN_BUILD ]]; then
    echo "Cleaning up build dir: $BUILD_DIR"
    rm -rf $BUILD_DIR
    rm -rf $DEPS_DIR
fi

if [[ ! -f ${depsToolsetDir}/completed ]]; then
    echo "Preparing dependencies: $DEPS_DIR"
    NUM_WORKERS=$NUM_WORKERS DEPS_DIR=$DEPS_DIR \
        "$SCRIPT_DIR/win32_deps_build.sh"
fi

mkdir -p $BUILD_DIR
cd $BUILD_DIR

if [[ -z $SKIP_CMAKE ]]; then
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
      -D Backtrace_INCLUDE_DIR="$backtraceDir/include" \
      -D Backtrace_LIBRARY="$backtraceDir/lib/libbacktrace.dll.a" \
      -D ENABLE_GIT_VERSION=$ENABLE_GIT_VERSION \
      -D ALLOCATOR="$ALLOCATOR" -D CMAKE_BUILD_TYPE=$CMAKE_BUILD_TYPE \
      -D WNBD_INCLUDE_DIRS="$wnbdSrcDir/include" \
      -D WNBD_LIBRARIES="$wnbdLibDir/libwnbd.a" \
      -G "$generatorUsed" \
      $CEPH_DIR  2>&1 | tee "${BUILD_DIR}/cmake.log"
fi # [[ -z $SKIP_CMAKE ]]

if [[ -z $SKIP_BUILD ]]; then
    echo "Building using $NUM_WORKERS workers. Log: ${BUILD_DIR}/build.log"
    echo "" > "${BUILD_DIR}/build.log"

    # We're going to use an associative array having subdirectories as keys
    # and targets as values.
    declare -A make_targets
    make_targets["src/tools"]="ceph-conf ceph_radosacl ceph_scratchtool rados"
    make_targets["src/tools/immutable_object_cache"]="all"
    make_targets["src/tools/rbd"]="all"
    make_targets["src/tools/rbd_wnbd"]="all"
    make_targets["src/compressor"]="all"

    for target_subdir in "${!make_targets[@]}"; do
      echo "Building $target_subdir: ${make_targets[$target_subdir]}" | tee -a "${BUILD_DIR}/build.log"
      make -j $NUM_WORKERS -C $target_subdir ${make_targets[$target_subdir]} 2>&1 | tee -a "${BUILD_DIR}/build.log"
    done
fi

if [[ -z $SKIP_DLL_COPY ]]; then
    # Hopefully this path will be the same across distros.
    # This depends on the thread library, we're currently using posix.
    mingwVersion=`x86_64-w64-mingw32-c++-posix -dumpversion`
    mingwTargetLibDir="/usr/lib/gcc/x86_64-w64-mingw32/$mingwVersion"
    required_dlls=(
        $zlibDir/zlib1.dll
        $lz4Dir/lib/dll/liblz4-1.dll
        $sslDir/bin/libcrypto-1_1-x64.dll
        $sslDir/bin/libssl-1_1-x64.dll
        $mingwTargetLibDir/libstdc++-6.dll
        $mingwTargetLibDir/libgcc_s_seh-1.dll
        /usr/x86_64-w64-mingw32/lib/libwinpthread-1.dll)
    echo "Copying required dlls to $binDir."
    cp ${required_dlls[@]} $binDir
fi

if [[ -n $BUILD_ZIP ]]; then
    if [[ -n $STRIP_ZIPPED ]]; then
        rm -rf $strippedBinDir
        cp -r $binDir $strippedBinDir
        echo "Stripping debug symbols from $strippedBinDir binaries."
        x86_64-w64-mingw32-strip $strippedBinDir/*.exe \
                                 $strippedBinDir/*.dll
    fi
    echo "Building zip archive $ZIP_DEST."
    zip -r $ZIP_DEST $strippedBinDir
fi
