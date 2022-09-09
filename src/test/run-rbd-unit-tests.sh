#!/usr/bin/env bash
set -x

# increase local port range in case we run out of local port
# due to large amount of TIME_WAIT connections.
sudo sysctl -w net.ipv4.ip_local_port_range="1024 65535"

# this should be run from the src directory in the ceph.git

source $(dirname $0)/detect-build-env-vars.sh
PATH="$CEPH_BIN:$PATH"

function run_with_feature() {
  local feature=$1
  shift
  if [ -z $feature ]; then
    unset RBD_FEATURES
  else
    export RBD_FEATURES=$feature
  fi
  if ! unittest_librbd; then
    sudo netstat -tnpW
    exit 1
  fi
}

if [ $# = 0 ]; then
  # mimic the old behaviour
  TESTS='0 1 61 109 127'
  run_with_feature
elif [ $# = 1 -a "${1}" = N ] ; then
  # new style no feature request
  run_with_feature
else
  TESTS="$*"
fi

for i in ${TESTS}
do
  run_with_feature $i
done

echo OK
