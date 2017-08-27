#!/usr/bin/env bash

set -ex

wget http://download.ceph.com/qa/wac.c
gcc -o wac wac.c
set +e
timeout 5m ./wac -l 65536 -n 64 -r wac-test
RET=$?
set -e
[[ $RET -eq 124 ]]
echo OK
