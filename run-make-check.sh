#!/usr/bin/env bash
#
# Ceph distributed storage system
#
# Copyright (C) 2014 Red Hat <contact@redhat.com>
#
# Author: Loic Dachary <loic@dachary.org>
#
#  This library is free software; you can redistribute it and/or
#  modify it under the terms of the GNU Lesser General Public
#  License as published by the Free Software Foundation; either
#  version 2.1 of the License, or (at your option) any later version.
#

#
# To just look at what this script will do, run it like this:
#
# $ DRY_RUN=echo ./run-make-check.sh
#

source src/script/run-make.sh

set -e

function in_jenkins() {
    test -n "$JENKINS_HOME"
}

function run() {
    # to prevent OSD EMFILE death on tests, make sure ulimit >= 1024
    $DRY_RUN ulimit -n $(ulimit -Hn)
    if [ $(ulimit -n) -lt 1024 ];then
        echo "***ulimit -n too small, better bigger than 1024 for test***"
        return 1
    fi

    # increase the aio-max-nr, which is by default 65536. we could reach this
    # limit while running seastar tests and bluestore tests.
    local m=16
    if [ $(nproc) -gt $m ]; then
        m=$(nproc)
    fi
    $DRY_RUN sudo /sbin/sysctl -q -w fs.aio-max-nr=$((65536 * $(nproc)))

    CHECK_MAKEOPTS=${CHECK_MAKEOPTS:-$DEFAULT_MAKEOPTS}
    if in_jenkins; then
        if ! ctest $CHECK_MAKEOPTS --no-compress-output --output-on-failure --test-output-size-failed 1024000 -T Test; then
            # do not return failure, as the jenkins publisher will take care of this
            rm -fr ${TMPDIR:-/tmp}/ceph-asok.*
        fi
    else
        if ! $DRY_RUN ctest $CHECK_MAKEOPTS --output-on-failure; then
            rm -fr ${TMPDIR:-/tmp}/ceph-asok.*
            return 1
        fi
    fi
}

function main() {
    if [[ $EUID -eq 0 ]] ; then
        echo "For best results, run this script as a normal user configured"
        echo "with the ability to run commands as root via sudo."
    fi
    echo -n "Checking hostname sanity... "
    if $DRY_RUN hostname --fqdn >/dev/null 2>&1 ; then
        echo "OK"
    else
        echo "NOT OK"
        echo "Please fix 'hostname --fqdn', otherwise 'make check' will fail"
        return 1
    fi
    # uses run-make.sh to install-deps
    FOR_MAKE_CHECK=1 prepare
    local cxx_compiler=g++
    local c_compiler=gcc
    for i in $(seq 14 -1 10); do
        if type -t clang-$i > /dev/null; then
            cxx_compiler="clang++-$i"
            c_compiler="clang-$i"
            break
        fi
    done
    # Init defaults after deps are installed.
    local cmake_opts
    cmake_opts+=" -DCMAKE_CXX_COMPILER=$cxx_compiler -DCMAKE_C_COMPILER=$c_compiler"
    cmake_opts+=" -DCMAKE_CXX_FLAGS_DEBUG=-Werror"
    cmake_opts+=" -DENABLE_GIT_VERSION=OFF"
    cmake_opts+=" -DWITH_GTEST_PARALLEL=ON"
    cmake_opts+=" -DWITH_FIO=ON"
    cmake_opts+=" -DWITH_CEPHFS_SHELL=ON"
    cmake_opts+=" -DWITH_GRAFANA=ON"
    cmake_opts+=" -DWITH_SPDK=ON"
    if [ $WITH_SEASTAR ]; then
        cmake_opts+=" -DWITH_SEASTAR=ON"
    fi
    if [ $WITH_ZBD ]; then
        cmake_opts+=" -DWITH_ZBD=ON"
    fi
    if [ $WITH_RBD_RWL ]; then
        cmake_opts+=" -DWITH_RBD_RWL=ON"
    fi
    cmake_opts+=" -DWITH_RBD_SSD_CACHE=ON"
    configure "$cmake_opts" "$@"
    build tests
    echo "make check: successful build on $(git rev-parse HEAD)"
    FOR_MAKE_CHECK=1 run
}

main "$@"
