#!/usr/bin/env bash

set -e

SCRIPT_DIR="$(dirname "$BASH_SOURCE")"
SCRIPT_DIR="$(realpath "$SCRIPT_DIR")"

num_vcpus=$(nproc)
NUM_WORKERS=${NUM_WORKERS:-$num_vcpus}

DEPS_DIR="${DEPS_DIR:-$SCRIPT_DIR/build.deps}"
depsSrcDir="$DEPS_DIR/src"
depsToolsetDir="$DEPS_DIR/mingw"

lz4SrcDir="${depsSrcDir}/lz4"
lz4Dir="${depsToolsetDir}/lz4"
lz4Tag="v1.9.2"
sslTag="OpenSSL_1_1_1c"
sslDir="${depsToolsetDir}/openssl"
sslSrcDir="${depsSrcDir}/openssl"

# For now, we'll keep the version number within the file path when not using git.
boostUrl="https://boostorg.jfrog.io/artifactory/main/release/1.82.0/source/boost_1_82_0.tar.gz"
boostSrcDir="${depsSrcDir}/boost_1_82_0"
boostDir="${depsToolsetDir}/boost"
zlibDir="${depsToolsetDir}/zlib"
zlibSrcDir="${depsSrcDir}/zlib"
backtraceDir="${depsToolsetDir}/libbacktrace"
backtraceSrcDir="${depsSrcDir}/libbacktrace"
snappySrcDir="${depsSrcDir}/snappy"
snappyDir="${depsToolsetDir}/snappy"
snappyTag="1.1.9"
# Additional Windows libraries, which aren't provided by Mingw
winLibDir="${depsToolsetDir}/windows/lib"

wnbdUrl="https://github.com/cloudbase/wnbd"
wnbdTag="main"
wnbdSrcDir="${depsSrcDir}/wnbd"
wnbdLibDir="${depsToolsetDir}/wnbd/lib"

dokanUrl="https://github.com/dokan-dev/dokany"
dokanTag="v2.0.5.1000"
dokanSrcDir="${depsSrcDir}/dokany"
dokanLibDir="${depsToolsetDir}/dokany/lib"

# Allow for OS specific customizations through the OS flag (normally
# passed through from win32_build).
# Valid options are currently "ubuntu", "rhel", and "suse".
OS=${OS:-"ubuntu"}

function _make() {
  make -j $NUM_WORKERS $@
}

if [[ -d $DEPS_DIR ]]; then
    echo "Cleaning up dependency build dir: $DEPS_DIR"
    rm -rf $DEPS_DIR
fi

mkdir -p $DEPS_DIR
mkdir -p $depsToolsetDir
mkdir -p $depsSrcDir

echo "Installing required packages."
case "$OS" in
    rhel)
        # pkgconf needs https://bugzilla.redhat.com/show_bug.cgi?id=1975416
        sudo yum -y --setopt=skip_missing_names_on_install=False install \
            mingw64-gcc-c++ \
            cmake \
            pkgconf \
            python3-devel \
            autoconf \
            libtool \
            ninja-build \
            zip \
            python3-PyYAML \
            gcc \
            diffutils \
            patch \
            wget \
            perl \
            git-core
        ;;
    ubuntu)
        sudo apt-get update
        sudo env DEBIAN_FRONTEND=noninteractive apt-get -y install \
            mingw-w64 g++ cmake pkg-config \
            python3-dev python3-yaml \
                autoconf libtool ninja-build wget zip \
                git
        ;;
    suse)
        for PKG in mingw64-cross-gcc-c++ mingw64-libgcc_s_seh1 mingw64-libstdc++6 \
                cmake pkgconf python3-devel autoconf libtool ninja zip \
                python3-PyYAML \
                gcc patch wget git; do
            rpm -q $PKG >/dev/null || zypper -n install $PKG
        done
        ;;
esac

MINGW_CMAKE_FILE="$DEPS_DIR/mingw.cmake"
source "$SCRIPT_DIR/mingw_conf.sh"

echo "Building zlib."
cd $depsSrcDir
if [[ ! -d $zlibSrcDir ]]; then
    git clone --depth 1 https://github.com/madler/zlib
fi
cd $zlibSrcDir
# Apparently the configure script is broken...
sed -e s/"PREFIX = *$"/"PREFIX = ${MINGW_PREFIX}"/ -i win32/Makefile.gcc
_make -f win32/Makefile.gcc
_make BINARY_PATH=$zlibDir \
     INCLUDE_PATH=$zlibDir/include \
     LIBRARY_PATH=$zlibDir/lib \
     SHARED_MODE=1 \
     -f win32/Makefile.gcc install

echo "Building lz4."
cd $depsToolsetDir
if [[ ! -d $lz4Dir ]]; then
    git clone --branch $lz4Tag --depth 1 https://github.com/lz4/lz4
    cd $lz4Dir
fi
cd $lz4Dir
_make BUILD_STATIC=no CC=${MINGW_CC%-posix*} \
      DLLTOOL=${MINGW_DLLTOOL} \
      WINDRES=${MINGW_WINDRES} \
      TARGET_OS=Windows_NT

echo "Building OpenSSL."
cd $depsSrcDir
if [[ ! -d $sslSrcDir ]]; then
    git clone --branch $sslTag --depth 1 https://github.com/openssl/openssl
    cd $sslSrcDir
fi
cd $sslSrcDir
mkdir -p $sslDir
CROSS_COMPILE="${MINGW_PREFIX}" ./Configure \
    mingw64 shared --prefix=$sslDir --libdir="$sslDir/lib"
_make depend
_make
_make install_sw

echo "Building boost."
cd $depsSrcDir
if [[ ! -d $boostSrcDir ]]; then
    echo "Downloading boost."
    wget -qO- $boostUrl | tar xz
fi

cd $boostSrcDir
echo "using gcc : mingw32 : ${MINGW_CXX} ;" > user-config.jam

# Workaround for https://github.com/boostorg/thread/issues/156
# Older versions of mingw provided a different pthread lib.
sed -i 's/lib$(libname)GC2.a/lib$(libname).a/g' ./libs/thread/build/Jamfile.v2
sed -i 's/mthreads/pthreads/g' ./tools/build/src/tools/gcc.jam
sed -i 's/pthreads/mthreads/g' ./tools/build/src/tools/gcc.jam

export PTW32_INCLUDE=${PTW32Include}
export PTW32_LIB=${PTW32Lib}

echo "Patching boost."
# Fix getting Windows page size
# TODO: send this upstream and maybe use a fork until it merges.
# Meanwhile, we might consider moving those to ceph/cmake/modules/BuildBoost.cmake.
# This cmake module will first have to be updated to support Mingw though.
patch -N boost/thread/pthread/thread_data.hpp <<EOL
--- boost/thread/pthread/thread_data.hpp        2019-10-11 15:26:15.678703586 +0300
+++ boost/thread/pthread/thread_data.hpp.new    2019-10-11 15:26:07.321463698 +0300
@@ -32,6 +32,10 @@
 # endif
 #endif

+#if defined(_WIN32)
+#include <windows.h>
+#endif
+
 #include <pthread.h>
 #include <unistd.h>

@@ -54,6 +58,10 @@
           if (size==0) return;
 #ifdef BOOST_THREAD_USES_GETPAGESIZE
           std::size_t page_size = getpagesize();
+#elif _WIN32
+          SYSTEM_INFO system_info;
+          ::GetSystemInfo (&system_info);
+          std::size_t page_size = system_info.dwPageSize;
 #else
           std::size_t page_size = ::sysconf( _SC_PAGESIZE);
 #endif
EOL

./bootstrap.sh

./b2 install --user-config=user-config.jam toolset=gcc-mingw32 \
    target-os=windows release \
    link=static,shared \
    threadapi=win32 --prefix=$boostDir \
    address-model=64 architecture=x86 \
    binary-format=pe abi=ms -j $NUM_WORKERS \
    -sZLIB_INCLUDE=$zlibDir/include -sZLIB_LIBRARY_PATH=$zlibDir/lib \
    --without-python --without-mpi --without-log --without-wave

echo "Building libbacktrace."
cd $depsSrcDir
if [[ ! -d $backtraceSrcDir ]]; then
    git clone --depth 1 https://github.com/ianlancetaylor/libbacktrace
fi
mkdir -p $backtraceSrcDir/build
cd $backtraceSrcDir/build
../configure --prefix=$backtraceDir --exec-prefix=$backtraceDir \
             --host ${MINGW_BASE} --enable-host-shared \
             --libdir="$backtraceDir/lib"
_make LDFLAGS="-no-undefined"
_make install

echo "Building snappy."
cd $depsSrcDir
if [[ ! -d $snappySrcDir ]]; then
    git clone --branch $snappyTag --depth 1 https://github.com/google/snappy
    cd $snappySrcDir
fi
mkdir -p $snappySrcDir/build
cd $snappySrcDir/build

cmake -DCMAKE_INSTALL_PREFIX=$snappyDir \
      -DCMAKE_BUILD_TYPE=Release \
      -DBUILD_SHARED_LIBS=ON \
      -DSNAPPY_BUILD_TESTS=OFF \
      -DSNAPPY_BUILD_BENCHMARKS=OFF \
      -DCMAKE_TOOLCHAIN_FILE=$MINGW_CMAKE_FILE \
      ../
_make
_make install

cmake -DCMAKE_INSTALL_PREFIX=$snappyDir \
      -DCMAKE_BUILD_TYPE=Release \
      -DBUILD_SHARED_LIBS=OFF \
      -DSNAPPY_BUILD_TESTS=OFF \
      -DCMAKE_TOOLCHAIN_FILE=$MINGW_CMAKE_FILE \
      ../
_make
_make install

echo "Generating mswsock.lib."
# mswsock.lib is not provided by mingw, so we'll have to generate
# it.
mkdir -p $winLibDir
cat > $winLibDir/mswsock.def <<EOF
LIBRARY MSWSOCK.DLL
EXPORTS
AcceptEx@32
EnumProtocolsA@12
EnumProtocolsW@12
GetAcceptExSockaddrs@32
GetAddressByNameA@40
GetAddressByNameW@40
GetNameByTypeA@12
GetNameByTypeW@12
GetServiceA@28
GetServiceW@28
GetTypeByNameA@8
GetTypeByNameW@8
MigrateWinsockConfiguration@12
NPLoadNameSpaces@12
SetServiceA@24
SetServiceW@24
TransmitFile@28
WSARecvEx@16
dn_expand@20
getnetbyname@4
inet_network@4
rcmd@24
rexec@24rresvport@4
s_perror@8sethostname@8
EOF

$MINGW_DLLTOOL -d $winLibDir/mswsock.def \
               -l $winLibDir/libmswsock.a

echo "Fetching libwnbd."
cd $depsSrcDir
if [[ ! -d $wnbdSrcDir ]]; then
    git clone --branch $wnbdTag --depth 1 $wnbdUrl
fi
cd $wnbdSrcDir
mkdir -p $wnbdLibDir
$MINGW_DLLTOOL -d $wnbdSrcDir/libwnbd/libwnbd.def \
               -D libwnbd.dll \
               -l $wnbdLibDir/libwnbd.a

echo "Fetching dokany."
cd $depsSrcDir
if [[ ! -d $dokanSrcDir ]]; then
    git clone --branch $dokanTag --depth 1 $dokanUrl
fi

mkdir -p $dokanLibDir
$MINGW_DLLTOOL -d $dokanSrcDir/dokan/dokan.def \
               -l $dokanLibDir/libdokan.a

# That's probably the easiest way to deal with the dokan imports.
# dokan.h is defined in both ./dokan and ./sys while both are using
# sys/public.h without the "sys" prefix.
cp $dokanSrcDir/sys/public.h $dokanSrcDir/dokan

echo "Finished building Ceph dependencies."
touch $depsToolsetDir/completed
