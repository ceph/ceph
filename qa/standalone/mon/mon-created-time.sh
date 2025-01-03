#!/usr/bin/env bash
#
# Copyright (C) 2015 SUSE LINUX GmbH
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
source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7125" # git grep '\<7125\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function TEST_mon_created_time() {
    local dir=$1

    run_mon $dir a || return 1

    ceph mon dump || return 1

    if test "$(ceph mon dump 2>/dev/null | sed -n '/created/p' | awk '{print $NF}')"x = ""x ; then
        return 1
    fi

    if test "$(ceph mon dump 2>/dev/null | sed -n '/created/p' | awk '{print $NF}')"x = "0.000000"x ; then
        return 1
    fi
}

main mon-created-time "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/mon/mon-created-time.sh"
# End:
