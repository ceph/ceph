#!/bin/bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7113" # git grep '\<7113\>' : there must be only one
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

function TEST_osd_df() {
    local dir=$1
    setup $dir || return 1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1
    run_osd $dir 4 || return 1
    run_osd $dir 5 || return 1

    # normal case
    ceph osd df --f json-pretty | grep osd.0 || return 1
    ceph osd df --f json-pretty | grep osd.1 || return 1
    ceph osd df --f json-pretty | grep osd.2 || return 1
    ceph osd df --f json-pretty | grep osd.3 || return 1
    ceph osd df --f json-pretty | grep osd.4 || return 1
    ceph osd df --f json-pretty | grep osd.5 || return 1

    # filter by device class
    osd_class=$(ceph osd crush get-device-class 0)
    ceph osd df class $osd_class --f json-pretty | grep 'osd.0' || return 1
    # post-nautilus we require filter-type no more
    ceph osd df $osd_class --f json-pretty | grep 'osd.0' || return 1
    ceph osd crush rm-device-class 0 || return 1
    ceph osd crush set-device-class aaa 0 || return 1
    ceph osd df aaa --f json-pretty | grep 'osd.0' || return 1
    ceph osd df aaa --f json-pretty | grep 'osd.1' && return 1
    # reset osd.1's device class
    ceph osd crush rm-device-class 0 || return 1
    ceph osd crush set-device-class $osd_class 0 || return 1

    # filter by crush node
    ceph osd df osd.0 --f json-pretty | grep osd.0 || return 1
    ceph osd df osd.0 --f json-pretty | grep osd.1 && return 1
    ceph osd crush move osd.0 root=default host=foo || return 1
    ceph osd crush move osd.1 root=default host=foo || return 1
    ceph osd crush move osd.2 root=default host=foo || return 1
    ceph osd crush move osd.3 root=default host=bar || return 1
    ceph osd crush move osd.4 root=default host=bar || return 1
    ceph osd crush move osd.5 root=default host=bar || return 1
    ceph osd df tree foo --f json-pretty | grep foo || return 1
    ceph osd df tree foo --f json-pretty | grep bar && return 1
    ceph osd df foo --f json-pretty | grep osd.0 || return 1
    ceph osd df foo --f json-pretty | grep osd.1 || return 1
    ceph osd df foo --f json-pretty | grep osd.2 || return 1
    ceph osd df foo --f json-pretty | grep osd.3 && return 1
    ceph osd df foo --f json-pretty | grep osd.4 && return 1
    ceph osd df foo --f json-pretty | grep osd.5 && return 1
    ceph osd df tree bar --f json-pretty | grep bar || return 1
    ceph osd df tree bar --f json-pretty | grep foo && return 1
    ceph osd df bar --f json-pretty | grep osd.0 && return 1
    ceph osd df bar --f json-pretty | grep osd.1 && return 1
    ceph osd df bar --f json-pretty | grep osd.2 && return 1
    ceph osd df bar --f json-pretty | grep osd.3 || return 1
    ceph osd df bar --f json-pretty | grep osd.4 || return 1
    ceph osd df bar --f json-pretty | grep osd.5 || return 1

    # filter by pool
    ceph osd crush rm-device-class all || return 1
    ceph osd crush set-device-class nvme 0 1 3 4 || return 1
    ceph osd crush rule create-replicated nvme-rule default host nvme || return 1
    ceph osd pool create nvme-pool 12 12 nvme-rule || return 1
    ceph osd df nvme-pool --f json-pretty | grep osd.0 || return 1
    ceph osd df nvme-pool --f json-pretty | grep osd.1 || return 1
    ceph osd df nvme-pool --f json-pretty | grep osd.2 && return 1
    ceph osd df nvme-pool --f json-pretty | grep osd.3 || return 1
    ceph osd df nvme-pool --f json-pretty | grep osd.4 || return 1
    ceph osd df nvme-pool --f json-pretty | grep osd.5 && return 1

    teardown $dir || return 1
}

main osd-df "$@"
