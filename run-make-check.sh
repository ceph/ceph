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

function gen_ctest_resource_file() {
    local file_name=$(mktemp /tmp/ctest-resource-XXXXXX)
    local max_cpuid=$(($(nproc) - 1))
    jq -n '$ARGS.positional | map({id:., slots:1}) | {cpus:.} | {version: {major:1, minor:0}, local:[.]}' \
        --args $(seq 0 $max_cpuid) > $file_name
    echo "$file_name"
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
    local procs="$(($(get_processors) * 2))"
    if [ "${procs}" -gt $m ]; then
        m="${procs}"
    fi
    local aiomax="$((65536 * procs))"
    if [ "$(/sbin/sysctl -n fs.aio-max-nr )" -lt "${aiomax}" ]; then
        $DRY_RUN sudo /sbin/sysctl -q -w fs.aio-max-nr="${aiomax}"
    fi

    CHECK_MAKEOPTS=${CHECK_MAKEOPTS:-$DEFAULT_MAKEOPTS}
    CTEST_RESOURCE_FILE=$(gen_ctest_resource_file)
    CHECK_MAKEOPTS+=" --resource-spec-file ${CTEST_RESOURCE_FILE}"
    if in_jenkins; then
        if ! ctest $CHECK_MAKEOPTS --no-compress-output --output-on-failure --test-output-size-failed 1024000 -T Test; then
            # do not return failure, as the jenkins publisher will take care of this
            rm -fr ${TMPDIR:-/tmp}/ceph-asok.* ${CTEST_RESOURCE_FILE}
        fi
    else
        if ! $DRY_RUN ctest $CHECK_MAKEOPTS --output-on-failure; then
            rm -fr ${TMPDIR:-/tmp}/ceph-asok.* ${CTEST_RESOURCE_FILE}
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
    local start_prepare=$(date +%s)
    echo "run-make-check: start make check"
    FOR_MAKE_CHECK=1 prepare
    local end_prepare=$(date +%s)
    echo "run-make-check: finish prepare, duration $(($end_prepare - $start_prepare)) sec"
    configure "$@"
    local end_configure=$(date +%s)
    echo "run-make-check: finish configure, duration $(($end_configure - $end_prepare)) sec"
    in_jenkins && echo "CI_DEBUG: Running 'build tests'"
    build tests
    local end_build=$(date +%s)
    echo "run-make-check: finish build, duration $(($end_build - $end_configure)) sec"
    echo "make check: successful build on $(git rev-parse HEAD)"
    FOR_MAKE_CHECK=1 run
    local end_makecheck=$(date +%s)
    echo "run-make-check: finish tests, duration $(($end_makecheck - $end_build)) sec"
    echo "run-make-check: finish make check, duration $(($end_makecheck - $start_prepare)) sec"
}

if [ "$0" = "$BASH_SOURCE" ]; then
    main "$@"
fi
