#!/usr/bin/env bash
set -ex

if [ -d .git ]; then
    git submodule update --init --recursive --progress
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
          if [ "$VERSION_ID" -ge "39" ] ; then
            PYBUILD="3.12"
          else
            # Fedora 37 and above
            PYBUILD="3.11"
          fi
          ;;
      rocky|rhel|centos)
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
      ubuntu)
          MAJOR_VER=$(echo "$VERSION_ID" | sed -e 's/\..*$//')
          if [ "$MAJOR_VER" -ge "22" ] ; then
              PYBUILD="3.10"
          fi
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

cxx_compiler="g++"
c_compiler="gcc"
# 20 is used for more future-proof
for i in $(seq 20 -1 11); do
  if type -t gcc-$i > /dev/null; then
    cxx_compiler="g++-$i"
    c_compiler="gcc-$i"
    break
  fi
done
ARGS+=" -DCMAKE_CXX_COMPILER=$cxx_compiler"
ARGS+=" -DCMAKE_C_COMPILER=$c_compiler"

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

