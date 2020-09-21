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

function run() {
    # to prevent OSD EMFILE death on tests, make sure ulimit >= 1024
    $DRY_RUN ulimit -n $(ulimit -Hn)
    if [ $(ulimit -n) -lt 1024 ];then
        echo "***ulimit -n too small, better bigger than 1024 for test***"
        return 1
    fi

    # increase the aio-max-nr, which is by default 65536. we could reach this
    # limit while running seastar tests and bluestore tests.
    $DRY_RUN sudo /sbin/sysctl -q -w fs.aio-max-nr=$((65536 * 16))

    CHECK_MAKEOPTS=${CHECK_MAKEOPTS:-$DEFAULT_MAKEOPTS}
    if ! $DRY_RUN ctest $CHECK_MAKEOPTS --output-on-failure; then
        rm -fr ${TMPDIR:-/tmp}/ceph-asok.*
        return 1
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
    FOR_MAKE_CHECK=1 prepare
    # Init defaults after deps are installed.
    local cmake_opts=" -DWITH_PYTHON3=3 -DWITH_GTEST_PARALLEL=ON -DWITH_FIO=ON -DWITH_CEPHFS_SHELL=ON -DWITH_SPDK=ON -DENABLE_GIT_VERSION=OFF"
    if [ $WITH_SEASTAR ]; then
        cmake_opts+=" -DWITH_SEASTAR=ON"
    fi
    configure $cmake_opts $@
    build tests && echo "make check: successful build on $(git rev-parse HEAD)"
    run
}

main "$@"
