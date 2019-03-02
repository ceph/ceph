#!/bin/sh -x
git submodule update --init --recursive
if test -e build; then
    echo 'build dir already exists; rm -rf build and re-run'
    exit 1
fi

PYBUILD="2"
if [ -r /etc/os-release ]; then
  source /etc/os-release
  case "$ID" in
      fedora)
          if [ "$VERSION_ID" -ge "29" ] ; then
              PYBUILD="3"
          fi
          ;;
      rhel|centos)
          MAJOR_VER=$(echo "$VERSION_ID" | sed -e 's/\..*$//')
          if [ "$MAJOR_VER" -ge "8" ] ; then
              PYBUILD="3"
          fi
          ;;
      opensuse*|suse|sles)
          PYBUILD="3"
          WITH_RADOSGW_AMQP_ENDPOINT="OFF"
          ;;
  esac
elif [ "$(uname)" == FreeBSD ] ; then
  PYBUILD="3"
  WITH_RADOSGW_AMQP_ENDPOINT="OFF"
else
  echo Unknown release
  exit 1
fi

if [ "$PYBUILD" = "3" ] ; then
    ARGS="$ARGS -DWITH_PYTHON2=OFF -DWITH_PYTHON3=ON -DMGR_PYTHON_VERSION=3"
fi

if type ccache > /dev/null 2>&1 ; then
    echo "enabling ccache"
    ARGS="$ARGS -DWITH_CCACHE=ON"
fi
if [ -n "$WITH_RADOSGW_AMQP_ENDPOINT" ] ; then
    ARGS="$ARGS -DWITH_RADOSGW_AMQP_ENDPOINT=$WITH_RADOSGW_AMQP_ENDPOINT"
fi

mkdir build
cd build
if type cmake3 > /dev/null 2>&1 ; then
    CMAKE=cmake3
else
    CMAKE=cmake
fi
${CMAKE} -DCMAKE_BUILD_TYPE=Debug $ARGS "$@" .. || exit 1

# minimal config to find plugins
cat <<EOF > ceph.conf
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
