#!/bin/bash
#
# Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
#
# Author: Loic Dachary <loic@dachary.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#
function setup() {
    local dir=$1
    teardown $dir
    mkdir $dir
}

function teardown() {
    local dir=$1
    kill_daemons $dir
    rm -fr $dir
}

function run_mon() {
    local dir=$1
    shift
    local id=$1
    shift
    dir+=/$id
    
    ./ceph-mon \
        --id $id \
        --mkfs \
        --mon-data=$dir --run-dir=$dir \
        "$@"

    ./ceph-mon \
        --id $id \
        --paxos-propose-interval=0.1 \
        --osd-pool-default-erasure-code-directory=.libs \
        --debug-mon 20 \
        --debug-ms 20 \
        --debug-paxos 20 \
        --mon-advanced-debug-mode \
        --chdir= \
        --mon-data=$dir \
        --log-file=$dir/log \
        --mon-cluster-log-file=$dir/log \
        --run-dir=$dir \
        --pid-file=$dir/pidfile \
        "$@"
}

function kill_daemons() {
    local dir=$1
    for pidfile in $(find $dir | grep pidfile) ; do
        for try in 0 1 1 1 2 3 ; do
            kill -9 $(cat $pidfile 2> /dev/null) 2> /dev/null || break
            sleep $try
        done
    done
}

function call_TEST_functions() {
    local dir=$1
    shift
    local id=$2
    shift

    setup $dir || return 1
    run_mon $dir $id "$@"
    SHARE_MON_FUNCTIONS=${SHARE_MON_FUNCTIONS:-$(set | sed -n -e 's/^\(SHARE_MON_TEST_[0-9a-z_]*\) .*/\1/p')}
    for TEST_function in $SHARE_MON_FUNCTIONS ; do
        if ! $TEST_function $dir $id ; then
            cat $dir/$id/log
            return 1
        fi
    done
    teardown $dir || return 1

    FUNCTIONS=${FUNCTIONS:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for TEST_function in $FUNCTIONS ; do
        setup $dir || return 1
        $TEST_function $dir || return 1
        teardown $dir || return 1
    done
}

function main() {
    local dir=$1

    export PATH=:$PATH # make sure program from sources are prefered

    PS4='${FUNCNAME[0]}: $LINENO: '
    export CEPH_CONF=/dev/null
    unset CEPH_ARGS

    set -x
    setup $dir || return 1
    local code
    if run $dir ; then
        code=0
    else
        code=1
    fi
    teardown $dir || return 1
    return $code
}
