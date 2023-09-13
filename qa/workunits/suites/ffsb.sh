#!/usr/bin/env bash

set -ex

mydir=`dirname $0`

# try it again if the clone is slow and the second time
trap -- 'retry' EXIT
retry() {
    rm -rf ffsb
    # double the timeout value
    timeout 3600 git clone https://git.ceph.com/ffsb.git --depth 1
}
rm -rf ffsb
timeout 1800 git clone https://git.ceph.com/ffsb.git --depth 1
trap - EXIT

cd ffsb
./configure
make
cd ..
mkdir tmp
cd tmp

for f in $mydir/*.ffsb
do
    ../ffsb/ffsb $f
done
cd ..
rm -r tmp ffsb*

