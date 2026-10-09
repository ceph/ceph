#!/usr/bin/env bash
#
# Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
# Copyright (C) 2014, 2015 Red Hat <contact@redhat.com>
#
# Author: Loic Dachary <loic@dachary.org>
# Author: Sage Weil <sage@redhat.com>
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

    export CEPH_MON="127.0.0.1:7111" # git grep '\<7111\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function TEST_copy_from() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    create_rbd_pool || return 1

    # success
    rados -p rbd put foo $(which rados)
    rados -p rbd cp foo foo2
    rados -p rbd stat foo2

    # failure
    ceph tell osd.\* injectargs -- --osd-debug-inject-copyfrom-error
    ! rados -p rbd cp foo foo3
    ! rados -p rbd stat foo3

    # success again
    ceph tell osd.\* injectargs -- --no-osd-debug-inject-copyfrom-error
    ! rados -p rbd cp foo foo3
    rados -p rbd stat foo3
}

# copy_from onto an object that has omap, from a source without omap, while
# the replicas are down: log-based recovery must not leave them with the old
# omap.
function TEST_copy_from_over_omap_recovers_omap() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for id in 0 1 2 ; do
        run_osd $dir $id || return 1
    done
    create_pool test 1 1 || return 1
    ceph osd pool set test size 3 --yes-i-really-mean-it || return 1
    ceph osd pool set test min_size 1 || return 1
    wait_for_clean || return 1

    rados -p test put tgt $(which rados) || return 1
    rados -p test setomapheader tgt header || return 1
    for i in $(seq 1 10) ; do
        rados -p test setomapval tgt key$i val$i || return 1
    done
    echo source > $dir/src
    rados -p test put src $dir/src || return 1

    local pg=$(get_pg test tgt)
    local primary=$(get_primary test tgt)
    local replicas=$(ceph pg map $pg -f json | jq -r ".acting[] | select(. != $primary)")
    ceph osd set noout || return 1
    for id in $replicas ; do
        kill_daemons $dir TERM osd.$id || return 1
    done
    ceph osd down $replicas || return 1
    for i in $(seq 1 60) ; do
        test "$(ceph pg map $pg -f json | jq -c .acting)" = "[$primary]" && break
        sleep 1
    done
    test "$(ceph pg map $pg -f json | jq -c .acting)" = "[$primary]" || return 1

    rados -p test cp src tgt || return 1
    test -z "$(rados -p test listomapkeys tgt)" || return 1

    for id in $replicas ; do
        activate_osd $dir $id || return 1
    done
    ceph osd unset noout || return 1
    wait_for_clean || return 1

    pg_deep_scrub $pg || return 1
    rados list-inconsistent-obj $pg | jq '.inconsistents'
    test "$(rados list-inconsistent-obj $pg | jq '.inconsistents | length')" = 0 || return 1
}

main osd-copy-from "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/osd/osd-bench.sh"
# End:
