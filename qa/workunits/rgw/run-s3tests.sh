#!/usr/bin/env bash
set -ex

# run s3-tests from current directory. assume working
# ceph environment (radosgw-admin in path) and rgw on localhost:8000
# (the vstart default).

branch=$1
[ -z "$1" ] && branch=master
port=$2
[ -z "$2" ] && port=8000   # this is vstart's default

##

[ -z "$BUILD_DIR" ] && BUILD_DIR=build

if [ -e CMakeCache.txt ]; then
    BIN_PATH=$PWD/bin
elif [ -e $root_path/../${BUILD_DIR}/CMakeCache.txt ]; then
    cd $root_path/../${BUILD_DIR}
    BIN_PATH=$PWD/bin
fi
PATH=$PATH:$BIN_PATH

dir=tmp.s3-tests.$$

# clone and bootstrap
mkdir $dir
cd $dir
git clone https://github.com/ceph/s3-tests
cd s3-tests
git checkout ceph-$branch
S3TEST_CONF=s3tests.conf.SAMPLE tox -- -m "not fails_on_rgw and not sse_s3 and not lifecycle_expiration and not test_of_sts and not webidentity_test" -v

cd ../..
rm -rf $dir

echo OK.

