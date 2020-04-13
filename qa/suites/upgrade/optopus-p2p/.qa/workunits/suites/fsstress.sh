#!/bin/bash

set -ex

mkdir -p fsstress
pushd fsstress
wget -q -O ltp-full.tgz http://download.ceph.com/qa/ltp-full-20091231.tgz
tar xzf ltp-full.tgz
pushd ltp-full-20091231/testcases/kernel/fs/fsstress
make
BIN=$(readlink -f fsstress)
popd
popd

T=$(mktemp -d -p .)
"$BIN" -d "$T" -l 1 -n 1000 -p 10 -v
rm -rf -- "$T"
