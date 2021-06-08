#!/usr/bin/env bash
set -ex

if [ -d .git ]; then
    git submodule update --init --recursive
fi

: ${BUILD_DIR:=build}
: ${CEPH_GIT_DIR:=..}

if [ -e $BUILD_DIR ]; then
    echo "'$BUILD_DIR' dir already exists; either rm -rf '$BUILD_DIR' and re-run, or set BUILD_DIR env var to a different directory name"
    exit 1
fi

PYBUILD="3"
ARGS="-GNinja"
if [ -r /etc/os-release ]; then
  source /etc/os-release
  case "$ID" in
      fedora)
          if [ "$VERSION_ID" -ge "35" ] ; then
            PYBUILD="3.10"
          elif [ "$VERSION_ID" -ge "33" ] ; then
            PYBUILD="3.9"
          elif [ "$VERSION_ID" -ge "32" ] ; then
            PYBUILD="3.8"
          else
            PYBUILD="3.7"
          fi
          ;;
      rhel|centos)
          MAJOR_VER=$(echo "$VERSION_ID" | sed -e 's/\..*$//')
          if [ "$MAJOR_VER" -ge "9" ] ; then
              PYBUILD="3.9"
          elif [ "$MAJOR_VER" -ge "8" ] ; then
              PYBUILD="3.6"
          fi
          ;;
      opensuse*|suse|sles)
          PYBUILD="3"
          ARGS+=" -DWITH_RADOSGW_AMQP_ENDPOINT=OFF"
          ARGS+=" -DWITH_RADOSGW_KAFKA_ENDPOINT=OFF"
          ;;
  esac
elif [ "$(uname)" == FreeBSD ] ; then
  PYBUILD="3"
  ARGS+=" -DWITH_RADOSGW_AMQP_ENDPOINT=OFF"
  ARGS+=" -DWITH_RADOSGW_KAFKA_ENDPOINT=OFF"
else
  echo Unknown release
  exit 1
fi

ARGS+=" -DWITH_PYTHON3=${PYBUILD}"

if type ccache > /dev/null 2>&1 ; then
    echo "enabling ccache"
    ARGS+=" -DWITH_CCACHE=ON"
fi

if [[ ! "$ARGS $@" =~ "-DBOOST_J" ]] ; then
    ncpu=$(getconf _NPROCESSORS_ONLN 2>&1)
    [ -n "$ncpu" -a "$ncpu" -gt 1 ] && ARGS+=" -DBOOST_J=$(expr $ncpu / 2)"
fi

mkdir $BUILD_DIR
cd $BUILD_DIR
if type cmake3 > /dev/null 2>&1 ; then
    CMAKE=cmake3
else
    CMAKE=cmake
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
  cat <<EOF

****
WARNING: do_cmake.sh now creates debug builds by default. Performance
may be severely affected. Please use -DCMAKE_BUILD_TYPE=RelWithDebInfo
if a performance sensitive build is required.
****
EOF
fi

