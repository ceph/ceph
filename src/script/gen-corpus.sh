#!/usr/bin/env bash
# -*- mode:sh; tab-width:4; sh-basic-offset:4; indent-tabs-mode:nil -*-
# vim: softtabstop=4 shiftwidth=4 expandtab

set -ex

function get_jobs() {
    local jobs=$(nproc)
    if [ $jobs -ge 8 ] ; then
        echo 8
    else
        echo $jobs
    fi
}

[ -z "$BUILD_DIR" ] && BUILD_DIR=build

function build() {
    local encode_dump_path=$1
    shift

    ./do_cmake.sh \
        -DWITH_MGR_DASHBOARD_FRONTEND=OFF \
        -DWITH_DPDK=OFF \
        -DWITH_SPDK=OFF \
        -DCMAKE_CXX_FLAGS="-DENCODE_DUMP_PATH=${encode_dump_path}"
    cd ${BUILD_DIR}
    cmake --build . -- -j$(get_jobs)
}

function run() {
    MON=3 MGR=2 OSD=3 MDS=3 RGW=1 ../src/vstart.sh -n -x

    local old_path="$PATH"
    export PATH="bin:$PATH"
    ceph osd pool create mypool
    rados -p mypool bench 10 write -b 123
    ceph osd out 0
    ceph osd in 0
    init-ceph restart osd.1
    for f in ../qa/workunits/cls/*.sh ; do
        $f
    done
    ../qa/workunits/rados/test.sh
    ceph_test_librbd
    ceph_test_libcephfs
    init-ceph restart mds.a
    ../qa/workunits/rgw/run-s3tests.sh
    PATH="$old_path"

    ../src/stop.sh
}

function import_corpus() {
    local encode_dump_path=$1
    shift
    local version=$1
    shift

    # import the corpus
    ../src/test/encoding/import.sh \
        ${encode_dump_path} \
        ${version} \
        ../ceph-object-corpus/archive
    ../src/test/encoding/import-generated.sh \
        ../ceph-object-corpus/archive
    # prune it
    pushd ../ceph-object-corpus
    bin/prune-archive.sh
    popd
}

function verify() {
    ctest -R readable.sh
}

function commit_and_push() {
    local version=$1
    shift

    pushd ../ceph-object-corpus
    git checkout -b wip-${version}
    git add archive/${version}
    git commit --signoff --message=${version}
    git remote add cc git@github.com:ceph/ceph-object-corpus.git
    git push cc wip-${version}
    popd
}

encode_dump_path=$(mktemp -d)
build $encode_dump_path
echo "generating corpus objects.."
run
version=$(bin/ceph-dencoder version)
echo "importing corpus. it may take over 30 minutes.."
import_corpus $encode_dump_path $version
echo "verifying imported corpus.."
verify
echo "all good, pushing to remote repo.."
commit_and_push ${version}
rm -rf encode_dump_path
