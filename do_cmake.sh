#!/usr/bin/env bash
set -x

git submodule update --init --recursive

[ -z "$BUILD_DIR" ] && BUILD_DIR=build

if [ -e $BUILD_DIR ]; then
    echo "'$BUILD_DIR' dir already exists; either rm -rf '$BUILD_DIR' and re-run, or set BUILD_DIR env var to a different directory name"
    exit 1
fi

PYBUILD="2"
if [ -r /etc/os-release ]; then
  source /etc/os-release
  case "$ID" in
      fedora)
          if [ "$VERSION_ID" -ge "29" ] ; then
              PYBUILD="3.7"
          fi
          ;;
      rhel|centos)
          MAJOR_VER=$(echo "$VERSION_ID" | sed -e 's/\..*$//')
          if [ "$MAJOR_VER" -ge "8" ] ; then
              PYBUILD="3.6"
          fi
          ;;
      opensuse*|suse|sles)
          PYBUILD="3"
          ARGS+=" -DWITH_RADOSGW_AMQP_ENDPOINT=OFF"
          ;;
  esac
elif [ "$(uname)" == FreeBSD ] ; then
  PYBUILD="3"
  ARGS+=" -DWITH_RADOSGW_AMQP_ENDPOINT=OFF"
else
  echo Unknown release
  exit 1
fi

if [[ "$PYBUILD" =~ ^3(\..*)?$ ]] ; then
    ARGS+=" -DWITH_PYTHON2=OFF -DWITH_PYTHON3=${PYBUILD} -DMGR_PYTHON_VERSION=${PYBUILD}"
fi

if type ccache > /dev/null 2>&1 ; then
    echo "enabling ccache"
    ARGS+=" -DWITH_CCACHE=ON"
fi

mkdir $BUILD_DIR
cd $BUILD_DIR
if type cmake3 > /dev/null 2>&1 ; then
    CMAKE=cmake3
else
    CMAKE=cmake
fi
${CMAKE} $ARGS "$@" .. || exit 1

# minimal config to find plugins
cat <<EOF > ceph.conf
[global]
plugin dir = lib
erasure code dir = lib
EOF

echo done.
cat <<EOF

****
WARNING: do_cmake.sh now creates debug builds by default. Performance
may be severely affected. Please use -DCMAKE_BUILD_TYPE=RelWithDebInfo
if a performance sensitive build is required.
****
EOF
