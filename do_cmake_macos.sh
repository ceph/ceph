#!/bin/bash
# Configure Ceph for a native macOS (Darwin/arm64) build.
#
# Companion to do_freebsd.sh.  Points cmake at the keg-only homebrew packages,
# selects the virtualenv interpreter that carries Cython, and turns off what
# either cannot build here or needs more than the base dependencies.
#
# Enabled: libceph-common, librados, librbd, libcephfs, the mon, the OSD on
# memstore, and the command line and offline tools.
#
# Disabled but working: the mgr, the MDS and the RGW.  Each wants extra
# packages and options rather than source changes; doc/dev/macos.rst says
# which.  Pass -DWITH_MGR=ON, -DWITH_CEPHFS=ON or -DWITH_RADOSGW=ON to turn
# them back on.
#
# Disabled because it does not build: BlueStore, which needs libaio or
# io_uring and the Linux block layer.
set -e

BUILD_DIR=${BUILD_DIR:-build}
BREW=$(brew --prefix)
SRC_DIR=$(cd "$(dirname "$0")" && pwd)

# src/pybind unconditionally requires Cython, so a venv holding it is a hard
# build dependency even though we do not need the python bindings themselves.
PYTHON=${PYTHON:-$SRC_DIR/.venv/bin/python}
if [ ! -x "$PYTHON" ]; then
    echo "no python at $PYTHON -- run: uv venv --python 3.12 .venv && uv pip install --python .venv/bin/python cython pyyaml" >&2
    exit 1
fi
PYTHON_VERSION=$("$PYTHON" -c 'import sys; print("%d.%d" % sys.version_info[:2])')

# Homebrew keg-only packages are not on the default search path.
CMAKE_PREFIX_PATH="$BREW/opt/openssl@3"
for pkg in boost nss nspr krb5 snappy lz4 zstd sqlite libevent utf8proc c-ares icu4c; do
    CMAKE_PREFIX_PATH="$CMAKE_PREFIX_PATH;$BREW/opt/$pkg"
done

mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

cmake -G Ninja \
    -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_PREFIX_PATH="$CMAKE_PREFIX_PATH" \
    -DOPENSSL_ROOT_DIR="$BREW/opt/openssl@3" \
    -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
    -DWITH_CCACHE=ON \
    -DENABLE_GIT_VERSION=OFF \
    -DWITH_SYSTEM_BOOST=ON \
    -DWITH_PYTHON3="$PYTHON_VERSION" \
    -DPython3_EXECUTABLE="$PYTHON" \
    -DPython3_FIND_VIRTUALENV=FIRST \
    \
    `# --- daemons / backends that cannot build on Darwin ---` \
    -DWITH_BLUESTORE=OFF \
    -DWITH_BLUEFS=OFF \
    -DWITH_CRIMSON=OFF \
    -DWITH_MGR=OFF \
    -DWITH_RADOSGW=OFF \
    -DWITH_CEPHFS=OFF \
    -DWITH_LIBCEPHSQLITE=OFF \
    -DWITH_KVS=OFF \
    -DWITH_NVMEOF_GATEWAY_MONITOR_CLIENT=OFF \
    \
    `# --- Linux-only plumbing ---` \
    -DWITH_KRBD=OFF \
    -DWITH_XFS=OFF \
    -DWITH_RDMA=OFF \
    -DWITH_SPDK=OFF \
    -DWITH_DPDK=OFF \
    -DWITH_SYSTEMD=OFF \
    -DWITH_SELINUX=OFF \
    -DWITH_LTTNG=OFF \
    -DWITH_BABELTRACE=OFF \
    -DWITH_BLKIN=OFF \
    -DWITH_JAEGER=OFF \
    -DWITH_BREAKPAD=OFF \
    -DWITH_QATLIB=OFF \
    -DWITH_QATZIP=OFF \
    \
    `# --- client libraries that do build on Darwin ---` \
    -DWITH_RBD=ON \
    -DWITH_LIBCEPHFS=ON \
    \
    `# --- trim the build surface while porting ---` \
    -DWITH_FUSE=OFF \
    -DWITH_OPENLDAP=OFF \
    -DWITH_GSSAPI=OFF \
    -DWITH_MANPAGE=OFF \
    -DWITH_TESTS=OFF \
    -DWITH_CATCH2=OFF \
    "$@" \
    .. 2>&1 | tee cmake.log
