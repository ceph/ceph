#!/usr/bin/env bash
set -ex

if [ -d .git ]; then
    git submodule update --init --recursive --recommend-shallow
fi

: ${BUILD_DIR:=build}
: ${CEPH_GIT_DIR:=..}

if [ -e $BUILD_DIR ]; then
    echo "'$BUILD_DIR' dir already exists; either rm -rf '$BUILD_DIR' and re-run, or set BUILD_DIR env var to a different directory name"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/cmake/presets/detect_host.sh"

ARGS="${ARGS} -GNinja"
bootstrap_local_presets "${SCRIPT_DIR}/cmake/presets"
write_host_json "${SCRIPT_DIR}/cmake/presets/host.json"
ARGS+="$(build_host_cmake_args)"
if [ -n "${WITH_SCCACHE:-}" ]; then
    echo "enabling sccache"
elif [ -n "${WITH_CCACHE:-}" ]; then
    echo "enabling ccache"
fi

mkdir $BUILD_DIR
cd $BUILD_DIR

# Only set CMAKE variable if not already set by user/environment.
# This allows users to override with a custom cmake binary via environment variable.
# Priority order: cmake 4.x+ (if available) -> cmake3 -> cmake (fallback)
if [ -z "${CMAKE}" ]; then
  if type cmake > /dev/null 2>&1 && cmake --version | grep -qE 'cmake version [4-9]\.'; then
      CMAKE=cmake
  elif type cmake3 > /dev/null 2>&1; then
      CMAKE=cmake3
  else
      CMAKE=cmake
  fi
fi
${CMAKE} $ARGS "$@" $CEPH_GIT_DIR || exit 1
set +x

# minimal config to find plugins
cat <<EOF > ceph.conf
[global]
plugin dir = lib
erasure code dir = lib
EOF

echo done.

if [[ ! "$ARGS $@" =~ "-DCMAKE_BUILD_TYPE" ]]; then
    if [ -d ../.git ]; then
        printf "
****
WARNING: do_cmake.sh now creates debug builds by default if .git exists.
Performance may be severely affected. Please use -DCMAKE_BUILD_TYPE=RelWithDebInfo
if a performance sensitive build is required.
****
"
    else
        printf "
****
WARNING: do_cmake.sh now creates RelWithDebInfo builds by default when .git is absent.
****
"
    fi
fi

