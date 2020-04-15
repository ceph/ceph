#!/usr/bin/env bash

set -e

SCRIPT_DIR="$(dirname "$BASH_SOURCE")"
SCRIPT_DIR="$(realpath "$SCRIPT_DIR")"

num_vcpus=$(( $(lscpu -p | tail -1 | cut -d "," -f 1) + 1 ))
NUM_WORKERS=${NUM_WORKERS:-$num_vcpus}

DEPS_DIR="${DEPS_DIR:-$SCRIPT_DIR/build.deps}"
depsSrcDir="$DEPS_DIR/src"
depsToolsetDir="$DEPS_DIR/mingw"

lz4SrcDir="${depsSrcDir}/lz4"
lz4Dir="${depsToolsetDir}/lz4"
lz4Tag="v1.9.2"
sslVersion="1.1.1c"
sslDir="${depsToolsetDir}/openssl"
sslSrcDir="${depsSrcDir}/openssl-${sslVersion}"

curlTag="curl-7_66_0"
curlSrcDir="${depsSrcDir}/curl"
curlDir="${depsToolsetDir}/curl"

# For now, we'll keep the version number within the file path when not using git.
boostUrl="https://dl.bintray.com/boostorg/release/1.70.0/source/boost_1_70_0.tar.gz"
boostSrcDir="${depsSrcDir}/boost_1_70_0"
boostDir="${depsToolsetDir}/boost"
zlibDir="${depsToolsetDir}/zlib"
zlibSrcDir="${depsSrcDir}/zlib"
backtraceDir="${depsToolsetDir}/backtrace"
backtraceSrcDir="${depsSrcDir}/backtrace"
snappySrcDir="${depsSrcDir}/snappy"
snappyDir="${depsToolsetDir}/snappy"
snappyTag="1.1.7"
# Additional Windows libraries, which aren't provided by Mingw
winLibDir="${depsToolsetDir}/windows/lib"


MINGW_PREFIX="x86_64-w64-mingw32-"

function _make() {
  make -j $NUM_WORKERS $@
}

mkdir -p $DEPS_DIR
mkdir -p $depsToolsetDir
mkdir -p $depsSrcDir

MINGW_CMAKE_FILE="$DEPS_DIR/mingw.cmake"
cat > $MINGW_CMAKE_FILE <<EOL
set(CMAKE_SYSTEM_NAME Windows)
set(TOOLCHAIN_PREFIX x86_64-w64-mingw32)

# We'll need to use posix threads in order to use
# C++11 features, such as std::thread.
set(CMAKE_C_COMPILER \${TOOLCHAIN_PREFIX}-gcc-posix)
set(CMAKE_CXX_COMPILER \${TOOLCHAIN_PREFIX}-g++-posix)
set(CMAKE_RC_COMPILER \${TOOLCHAIN_PREFIX}-windres)

set(CMAKE_FIND_ROOT_PATH /usr/\${TOOLCHAIN_PREFIX} /usr/lib/gcc/\${TOOLCHAIN_PREFIX}/7.3-posix)
set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY BOTH)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE BOTH)
EOL

sudo apt-get -y install mingw-w64 cmake pkg-config python3-dev python3-pip \
                autoconf libtool ninja-build
sudo python3 -m pip install cython

cd $depsSrcDir
if [[ ! -d $zlibDir ]]; then
    git clone https://github.com/madler/zlib
fi
cd zlib
# Apparently the configure script is broken...
sed -e s/"PREFIX ="/"PREFIX = x86_64-w64-mingw32-"/ -i win32/Makefile.gcc
_make -f win32/Makefile.gcc
_make BINARY_PATH=$zlibDir \
     INCLUDE_PATH=$zlibDir/include \
     LIBRARY_PATH=$zlibDir/lib \
     SHARED_MODE=1 \
     -f win32/Makefile.gcc install

cd $depsToolsetDir
if [[ ! -d $lz4Dir ]]; then
    git clone https://github.com/lz4/lz4
fi
cd lz4
git checkout $lz4Tag
_make BUILD_STATIC=no CC=x86_64-w64-mingw32-gcc \
      DLLTOOL=x86_64-w64-mingw32-dlltool OS=Windows_NT

cd $depsSrcDir
if [[ ! -d $sslSrcDir ]]; then
    curl "https://www.openssl.org/source/openssl-${sslVersion}.tar.gz" | tar xz
fi
cd $sslSrcDir
mkdir -p $sslDir
CROSS_COMPILE="x86_64-w64-mingw32-" ./Configure \
    mingw64 shared --prefix=$sslDir
_make depend
_make
_make install

cd $depsSrcDir
if [[ ! -d $curlSrcDir ]]; then
    git clone https://github.com/curl/curl
fi
cd $curlSrcDir
git checkout $curlTag
./buildconf
./configure --prefix=$curlDir --with-ssl=$sslDir --with-zlib=$zlibDir \
            --host=x86_64-w64-mingw32
_make
_make install


cd $depsSrcDir
if [[ ! -d $boostSrcDir ]]; then
    wget -qO- $boostUrl | tar xz
fi

cd $boostSrcDir
echo "using gcc : mingw32 : x86_64-w64-mingw32-g++-posix ;" > user-config.jam

# Workaround for https://github.com/boostorg/thread/issues/156
# Older versions of mingw provided a different pthread lib.
sed -i 's/lib$(libname)GC2.a/lib$(libname).a/g' ./libs/thread/build/Jamfile.v2
sed -i 's/mthreads/pthreads/g' ./tools/build/src/tools/gcc.py
sed -i 's/mthreads/pthreads/g' ./tools/build/src/tools/gcc.jam

sed -i 's/pthreads/mthreads/g' ./tools/build/src/tools/gcc.py
sed -i 's/pthreads/mthreads/g' ./tools/build/src/tools/gcc.jam

export PTW32_INCLUDE=/usr/share/mingw-w64/include
export PTW32_LIB=/usr/x86_64-w64-mingw32/lib

# Fix getting Windows page size
# TODO: send this upstream and maybe use a fork until it merges.
# Meanwhile, we might consider moving those to ceph/cmake/modules/BuildBoost.cmake.
# This cmake module will first have to be updated to support Mingw though.
cat | patch -N boost/thread/pthread/thread_data.hpp <<EOL
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

# Use pthread if requested
cat | patch -N boost/asio/detail/thread.hpp <<EOL
--- boost/asio/detail/thread.hpp        2019-10-11 16:26:11.191094656 +0300
+++ boost/asio/detail/thread.hpp.new    2019-10-11 16:26:03.310542438 +0300
@@ -19,6 +19,8 @@

 #if !defined(BOOST_ASIO_HAS_THREADS)
 # include <boost/asio/detail/null_thread.hpp>
+#elif defined(BOOST_ASIO_HAS_PTHREADS)
+# include <boost/asio/detail/posix_thread.hpp>
 #elif defined(BOOST_ASIO_WINDOWS)
 # if defined(UNDER_CE)
 #  include <boost/asio/detail/wince_thread.hpp>
@@ -27,8 +29,6 @@
 # else
 #  include <boost/asio/detail/win_thread.hpp>
 # endif
-#elif defined(BOOST_ASIO_HAS_PTHREADS)
-# include <boost/asio/detail/posix_thread.hpp>
 #elif defined(BOOST_ASIO_HAS_STD_THREAD)
 # include <boost/asio/detail/std_thread.hpp>
 #else
@@ -41,6 +41,8 @@

 #if !defined(BOOST_ASIO_HAS_THREADS)
 typedef null_thread thread;
+#elif defined(BOOST_ASIO_HAS_PTHREADS)
+typedef posix_thread thread;
 #elif defined(BOOST_ASIO_WINDOWS)
 # if defined(UNDER_CE)
 typedef wince_thread thread;
@@ -49,8 +51,6 @@
 # else
 typedef win_thread thread;
 # endif
-#elif defined(BOOST_ASIO_HAS_PTHREADS)
-typedef posix_thread thread;
 #elif defined(BOOST_ASIO_HAS_STD_THREAD)
 typedef std_thread thread;
 #endif
EOL

# Unix socket support for Windows is currently disabled by Boost.
# https://github.com/huangqinjin/asio/commit/d27a8ad1870
cat | patch -N boost/asio/detail/socket_types.hpp <<EOL
--- boost/asio/detail/socket_types.hpp       2019-11-29 16:50:58.647125797 +0000
+++ boost/asio/detail/socket_types.hpp.new   2020-01-13 11:45:05.015104678 +0000
@@ -200,6 +200,8 @@
 typedef ipv6_mreq in6_mreq_type;
 typedef sockaddr_in6 sockaddr_in6_type;
 typedef sockaddr_storage sockaddr_storage_type;
+struct sockaddr_un_type { u_short sun_family;
+  char sun_path[108]; }; /* copy from afunix.h */
 typedef addrinfo addrinfo_type;
 # endif
 typedef ::linger linger_type;
EOL
cat | patch -N boost/asio/detail/config.hpp <<EOL
--- boost/asio/detail/config.hpp       2019-11-29 16:50:58.691126211 +0000
+++ boost/asio/detail/config.hpp.new   2020-01-13 13:09:17.966771750 +0000
@@ -1142,13 +1142,9 @@
 // UNIX domain sockets.
 #if !defined(BOOST_ASIO_HAS_LOCAL_SOCKETS)
 # if !defined(BOOST_ASIO_DISABLE_LOCAL_SOCKETS)
-#  if !defined(BOOST_ASIO_WINDOWS) \\
-  && !defined(BOOST_ASIO_WINDOWS_RUNTIME) \\
-  && !defined(__CYGWIN__)
+#  if !defined(BOOST_ASIO_WINDOWS_RUNTIME)
 #   define BOOST_ASIO_HAS_LOCAL_SOCKETS 1
-#  endif // !defined(BOOST_ASIO_WINDOWS)
-         //   && !defined(BOOST_ASIO_WINDOWS_RUNTIME)
-         //   && !defined(__CYGWIN__)
+#  endif // !defined(BOOST_ASIO_WINDOWS_RUNTIME)
 # endif // !defined(BOOST_ASIO_DISABLE_LOCAL_SOCKETS)
 #endif // !defined(BOOST_ASIO_HAS_LOCAL_SOCKETS)
EOL

./bootstrap.sh

./b2 install --user-config=user-config.jam toolset=gcc-mingw32 \
    target-os=windows release \
    threadapi=pthread --prefix=$boostDir \
    address-model=64 architecture=x86 \
    binary-format=pe abi=ms -j $NUM_WORKERS \
    -sZLIB_INCLUDE=$zlibDir/include -sZLIB_LIBRARY_PATH=$zlibDir/lib \
    --without-python --without-mpi

cd $depsSrcDir
if [[ ! -d $backtraceSrcDir ]]; then
    git clone https://github.com/ianlancetaylor/libbacktrace
fi
mkdir libbacktrace/build
cd libbacktrace/build
../configure --prefix=$backtraceDir --exec-prefix=$backtraceDir \
             --host x86_64-w64-mingw32 --enable-host-shared
_make LDFLAGS="-no-undefined"
_make install
cp $backtraceDir/lib/libbacktrace.a $backtraceDir/lib/libbacktrace.dll.a

cd $depsSrcDir
if [[ ! -d $snappySrcDir ]]; then
    git clone https://github.com/google/snappy
fi
mkdir -p snappy/build
cd snappy && git checkout $snappyTag
cd build

cmake -DCMAKE_INSTALL_PREFIX=$snappyDir \
      -DCMAKE_BUILD_TYPE=Release \
      -DBUILD_SHARED_LIBS=ON \
      -DSNAPPY_BUILD_TESTS=OFF \
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

x86_64-w64-mingw32-dlltool -d $winLibDir/mswsock.def \
                           -l $winLibDir/libmswsock.a
