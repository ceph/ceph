#!/bin/sh -x
git submodule update --init --recursive
if test -e build; then
    echo 'build dir already exists; rm -rf build and re-run'
    exit 1
fi

ARGS=""
if which ccache ; then
    echo "enabling ccache"
    ARGS="$ARGS -DWITH_CCACHE=ON"
fi

mkdir build
cd build
NPROC=${NPROC:-$(nproc)}
cmake -DBOOST_J=$NPROC $ARGS "$@" ..

# minimal config to find plugins
cat <<EOF > ceph.conf
plugin dir = lib
erasure code dir = lib
EOF

# give vstart a unique mon port to start with
while [ true ]
do
  PORT=`echo $(( RANDOM % 1000 + 40000 ))`
  ss | awk '{print $5}' | awk -F : '{print $2}' | grep ^[0-9] | grep ${PORT}
  if [ "$?" -ne 0 ]
  then
    echo ${PORT} > .ceph_port
    break
  fi
done

echo done.
