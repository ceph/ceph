#!/bin/bash

set -ex

tox_version="$(tox --version | cut -d' ' -f1)"

oldest_tox="$(cat <<EOF | sort -V | head -n 1
2.9.0
$tox_version
EOF
)"

echo $oldest_tox

if [ ! "$oldest_tox" == "2.9.0" ] ; then
  rm -rf tox-venv
  python3 -m venv tox-venv
  . ./tox-venv/bin/activate
  pip install tox
fi

status=0

run_tox_1(){
    echo $1
    out="$(tox -c $1)"
    status=$?
    if [ $status != 0 ] ; then
      echo "$out"
    fi
    return $status
}

export -f run_tox_1

tests="
src/python-common/tox.ini
src/cephadm/tox.ini
src/pybind/mgr/tox.ini
qa/tox.ini
src/pybind/mgr/dashboard/tox.ini
"

echo "$tests" | xargs -P 0 -n 1 -I {} bash -c 'run_tox_1 "$@"' _ {}

