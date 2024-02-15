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
SKIP_BINDIR_CLEAN=${SKIP_BINDIR_CLEAN:-}
# Use Ninja's default, it might be useful when having few cores.
NUM_WORKERS_DEFAULT=$(( $num_vcpus + 2 ))
NUM_WORKERS=${NUM_WORKERS:-$NUM_WORKERS_DEFAULT}
DEV_BUILD=${DEV_BUILD:-}
# Unless SKIP_ZIP is set, we're preparing an archive that contains the Ceph
# binaries, debug symbols as well as the required DLLs.
SKIP_ZIP=${SKIP_ZIP:-}
# By default, we'll move the debug symbols to separate files located in the
# ".debug" directory. If "EMBEDDED_DBG_SYM" is set, the debug symbols will
# remain embedded in the binaries.
#
# Unfortunately we cannot use pdb symbols when cross compiling. cv2pdb
# well as llvm rely on mspdb*.dll in order to support this proprietary format.
EMBEDDED_DBG_SYM=${EMBEDDED_DBG_SYM:-}
# Allow for OS specific customizations through the OS flag.
# Valid options are currently "ubuntu", "suse", and "rhel".

OS=${OS}
if [[ -z $OS ]]; then
    source /etc/os-release
    case "$ID" in
    opensuse*|suse|sles)
        OS="suse"
        ;;
    rhel|centos)
        OS="rhel"
        ;;
    ubuntu)
        OS="ubuntu"
        ;;
    *)
        echo "Unsupported Linux distro $ID."
        echo "only SUSE, Ubuntu and RHEL are supported."
        echo "Set the OS environment variable to override."
        exit 1
        ;;
    esac
fi

# The main advantages of mingw-llvm:
# * not affected by the libstdc++/winpthread rw lock bugs
# * can generate pdb debug symbols, which are compatible with WinDBG
TOOLCHAIN=${TOOLCHAIN:-"mingw-llvm"}

case "$TOOLCHAIN" in
    mingw-llvm)
        echo "Using mingw-llvm."
        USE_MINGW_LLVM=1
        ;;
    mingw-gcc)
        echo "Using mingw-gcc"
        ;;
    *)
        echo "Unsupported toolchain: $TOOLCHAIN."
        echo "Allowed toolchains: mingw-llvm or mingw-gcc."
esac


# We'll have to be explicit here since auto-detecting doesn't work
# properly when cross compiling.
ALLOCATOR=${ALLOCATOR:-libc}
# Debug builds don't work with MINGW for the time being, failing with
# can't close <file>: File too big
# -Wa,-mbig-obj does not help.
CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE:-}
if [[ -z $CMAKE_BUILD_TYPE ]]; then
  # By default, we're building release binaries with minimal debug information.
  export CFLAGS="$CFLAGS -g1"
  export CXXFLAGS="$CXXFLAGS -g1"
  CMAKE_BUILD_TYPE=Release
fi

ENABLE_SHARED=${ENABLE_SHARED:-ON}

binDir="$BUILD_DIR/bin"
strippedBinDir="$BUILD_DIR/bin_stripped"
# GDB will look for this directory by default.
dbgDirname=".debug"
dbgSymbolDir="$strippedBinDir/${dbgDirname}"
depsSrcDir="$DEPS_DIR/src"
depsToolsetDir="$DEPS_DIR/mingw"

cmakeGenerator="Ninja"
lz4Dir="${depsToolsetDir}/lz4"
sslDir="${depsToolsetDir}/openssl"
boostDir="${depsToolsetDir}/boost"
zlibDir="${depsToolsetDir}/zlib"
backtraceDir="${depsToolsetDir}/libbacktrace"
snappyDir="${depsToolsetDir}/snappy"
winLibDir="${depsToolsetDir}/windows/lib"
wnbdSrcDir="${depsSrcDir}/wnbd"
wnbdLibDir="${depsToolsetDir}/wnbd/lib"
dokanSrcDir="${depsSrcDir}/dokany"
dokanLibDir="${depsToolsetDir}/dokany/lib"

depsDirs="$lz4Dir;$sslDir;$boostDir;$zlibDir;$backtraceDir;$snappyDir"
depsDirs+=";$winLibDir"

# Cmake recommends using CMAKE_PREFIX_PATH instead of link_directories.
# Still, some library dependencies may not include the full path (e.g. Boost
# sets the "-lz" flag through INTERFACE_LINK_LIBRARIES), which is why
# we have to ensure that the linker will be able to locate them.
linkDirs="$zlibDir/lib"

lz4Lib="${lz4Dir}/lib/dll/liblz4-1.dll"
lz4Include="${lz4Dir}/lib"

if [[ -n $CLEAN_BUILD ]]; then
    echo "Cleaning up build dir: $BUILD_DIR"
    rm -rf $BUILD_DIR
    rm -rf $DEPS_DIR
fi
if [[ -z $SKIP_BINDIR_CLEAN ]]; then
    echo "Cleaning up bin dir: $binDir"
    rm -rf $binDir
fi

mkdir -p $BUILD_DIR
cd $BUILD_DIR

if [[ ! -f ${depsToolsetDir}/completed ]]; then
    echo "Preparing dependencies: $DEPS_DIR. Log: ${BUILD_DIR}/build_deps.log"
    NUM_WORKERS=$NUM_WORKERS \
        DEPS_DIR=$DEPS_DIR \
        OS="$OS" \
        ENABLE_SHARED=$ENABLE_SHARED \
        USE_MINGW_LLVM=$USE_MINGW_LLVM \
            "$SCRIPT_DIR/win32_deps_build.sh" | tee "${BUILD_DIR}/build_deps.log"
fi

# Due to distribution specific mingw settings, the mingw.cmake file
# must be built prior to running cmake.
MINGW_CMAKE_FILE="$BUILD_DIR/mingw32.cmake"
MINGW_POSIX_FLAGS=1
source "$SCRIPT_DIR/mingw_conf.sh"

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
  WITH_CEPH_DEBUG_MUTEX="ON"
else
  ENABLE_GIT_VERSION="ON"
  WITH_CEPH_DEBUG_MUTEX="OFF"
fi

# As opposed to Linux, Windows shared libraries can't have unresolved
# symbols. Until we fix the dependencies (which are either unspecified
# or circular), we'll have to stick to static linking.
cmake -D CMAKE_PREFIX_PATH=$depsDirs \
      -D MINGW_LINK_DIRECTORIES="$linkDirs" \
      -D CMAKE_TOOLCHAIN_FILE="$MINGW_CMAKE_FILE" \
      -D WITH_FMT_HEADER_ONLY=ON \
      -D WITH_LIBCEPHSQLITE=OFF \
      -D WITH_QATLIB=OFF -D WITH_QATZIP=OFF \
      -D WITH_RDMA=OFF -D WITH_OPENLDAP=OFF \
      -D WITH_GSSAPI=OFF -D WITH_XFS=OFF \
      -D WITH_FUSE=OFF -D WITH_DOKAN=ON \
      -D WITH_BLUESTORE=OFF -D WITH_LEVELDB=OFF \
      -D WITH_LTTNG=OFF -D WITH_BABELTRACE=OFF -D WITH_JAEGER=OFF \
      -D WITH_SYSTEM_BOOST=ON -D WITH_MGR=OFF -D WITH_KVS=OFF \
      -D WITH_LIBCEPHFS=ON -D WITH_KRBD=OFF -D WITH_RADOSGW=OFF \
      -D ENABLE_SHARED=$ENABLE_SHARED -D WITH_RBD=ON -D BUILD_GMOCK=ON \
      -D WITH_CEPHFS=OFF -D WITH_MANPAGE=OFF \
      -D WITH_MGR_DASHBOARD_FRONTEND=OFF -D WITH_SYSTEMD=OFF -D WITH_TESTS=ON \
      -D LZ4_INCLUDE_DIR=$lz4Include -D LZ4_LIBRARY=$lz4Lib \
      -D Backtrace_INCLUDE_DIR="$backtraceDir/include" \
      -D Backtrace_LIBRARY="$backtraceDir/lib/libbacktrace.a" \
      -D ENABLE_GIT_VERSION=$ENABLE_GIT_VERSION \
      -D ALLOCATOR="$ALLOCATOR" -D CMAKE_BUILD_TYPE=$CMAKE_BUILD_TYPE \
      -D WNBD_INCLUDE_DIRS="$wnbdSrcDir/include" \
      -D WNBD_LIBRARIES="$wnbdLibDir/libwnbd.a" \
      -D WITH_CEPH_DEBUG_MUTEX=$WITH_CEPH_DEBUG_MUTEX \
      -D DOKAN_INCLUDE_DIRS="$dokanSrcDir/dokan" \
      -D DOKAN_LIBRARIES="$dokanLibDir/libdokan.a" \
      -G "$cmakeGenerator" \
      $CEPH_DIR  2>&1 | tee "${BUILD_DIR}/cmake.log"
fi # [[ -z $SKIP_CMAKE ]]

if [[ -z $SKIP_BUILD ]]; then
    echo "Building using $NUM_WORKERS workers. Log: ${BUILD_DIR}/build.log"
    echo "" > "${BUILD_DIR}/build.log"

    cd $BUILD_DIR
    ninja_targets="rados rbd rbd-wnbd "
    ninja_targets+=" ceph-conf ceph-immutable-object-cache"
    ninja_targets+=" cephfs ceph-dokan"
    # TODO: do we actually need the ceph compression libs?
    ninja_targets+=" compressor ceph_lz4 ceph_snappy ceph_zlib ceph_zstd"
    if [[ -z $SKIP_TESTS ]]; then
      ninja_targets+=" tests ceph_radosacl ceph_scratchtool "
      ninja_targets+=`ninja -t targets | grep ceph_test | cut -d ":" -f 1 | grep -v exe`
    fi

    ninja -v $ninja_targets 2>&1 | tee "${BUILD_DIR}/build.log"
fi

if [[ -z $SKIP_DLL_COPY ]]; then
    # To adjust mingw paths, see 'mingw_conf.sh'.
    required_dlls=(
        $zlibDir/zlib1.dll
        $lz4Dir/lib/dll/liblz4-1.dll
        $sslDir/bin/libcrypto-1_1-x64.dll
        $sslDir/bin/libssl-1_1-x64.dll
        $mingwLibpthreadDir/libwinpthread-1.dll)
    if [[ $ENABLE_SHARED == "ON" ]]; then
        required_dlls+=(
            $boostDir/lib/*.dll
        )
    fi
    if [[ -n $USE_MINGW_LLVM ]]; then
        required_dlls+=(
            $mingwTargetLibDir/libc++.dll
            $mingwTargetLibDir/libunwind.dll)
    else
        required_dlls+=(
            $mingwTargetLibDir/libstdc++-6.dll
            $mingwTargetLibDir/libssp*.dll
            $mingwTargetLibDir/libgcc_s_seh-1.dll)
    fi
    echo "Copying required dlls to $binDir."
    cp ${required_dlls[@]} $binDir
fi

if [[ -z $SKIP_ZIP ]]; then
    # Use a temp directory, in order to create a clean zip file
    ZIP_TMPDIR=$(mktemp -d win_binaries.XXXXX)
    if [[ -z $EMBEDDED_DBG_SYM ]]; then
        echo "Extracting debug symbols from binaries."
        rm -rf $strippedBinDir; mkdir $strippedBinDir
        rm -rf $dbgSymbolDir; mkdir $dbgSymbolDir
        # Strip files individually, to save time and space
        for file in $binDir/*.exe $binDir/*.dll; do
            dbgFilename=$(basename $file).debug
            dbgFile="$dbgSymbolDir/$dbgFilename"
            strippedFile="$strippedBinDir/$(basename $file)"

            echo "Copying debug symbols: $dbgFile"
            $MINGW_OBJCOPY --only-keep-debug $file $dbgFile
            $MINGW_STRIP --strip-debug --strip-unneeded -o $strippedFile $file
            $MINGW_OBJCOPY --remove-section .gnu_debuglink $strippedFile
            $MINGW_OBJCOPY --add-gnu-debuglink=$dbgFile $strippedFile
        done
        # Copy any remaining files to the stripped directory
        for file in $binDir/*; do
            [[ ! -f $strippedBinDir/$(basename $file) ]] && \
                cp $file $strippedBinDir
        done
        ln -s $strippedBinDir $ZIP_TMPDIR/ceph
    else
        ln -s $binDir $ZIP_TMPDIR/ceph
    fi
    echo "Building zip archive $ZIP_DEST."
    # Include the README file in the archive
    ln -s $CEPH_DIR/README.windows.rst $ZIP_TMPDIR/ceph/README.windows.rst
    cd $ZIP_TMPDIR
    [[ -f $ZIP_DEST ]] && rm $ZIP_DEST
    zip -r $ZIP_DEST ceph
    cd -
    rm -rf $ZIP_TMPDIR/ceph/README.windows.rst $ZIP_TMPDIR
    echo -e '\n  WIN32 files zipped to: '$ZIP_DEST'\n'
fi
