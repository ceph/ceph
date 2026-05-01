#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7159" # git grep '\<7159\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    #
    # Disable auto-class, so we can inject device class manually below
    #
    CEPH_ARGS+="--osd-class-update-on-start=false "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function TEST_stretch_replicated() {
    local dir=$1
    run_mon $dir a || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1

    ceph osd crush add-bucket dc1 datacenter
    ceph osd crush add-bucket dc2 datacenter
    ceph osd crush move dc1 root=default
    ceph osd crush move dc2 root=default
    ceph osd tree
    ceph osd crush rule create-stretch-replicated
    ceph osd crush rule create-stretch-replicated 2>&1 | grep "Error EINVAL: zone dc1 has only 0 items of type host" || return 1

    ceph osd crush add-bucket host1 host
    ceph osd crush move host1 datacenter=dc1
    ceph osd crush rule create-stretch-replicated 2>&1 | grep "Error EINVAL: zone dc1 has only 1 items of type host" || return 1

    ceph osd crush add-bucket host2 host
    ceph osd crush move host2 datacenter=dc1
    ceph osd crush rule create-stretch-replicated 2>&1 | grep "Error EINVAL: host host1 has no OSDs" || return 1

    ceph osd crush set osd.0 1.0 host=host1
    ceph osd crush rule create-stretch-replicated 2>&1 | grep "Error EINVAL: host host2 has no OSDs" || return 1

    ceph osd crush set osd.1 1.0 host=host2
    ceph osd crush rule create-stretch-replicated 2>&1 | grep "Error EINVAL: zone dc2 has only 0 items of type host" || return 1

    ceph osd crush add-bucket host3 host
    ceph osd crush move host3 datacenter=dc2
    ceph osd crush rule create-stretch-replicated 2>&1 | grep "Error EINVAL: zone dc2 has only 1 items of type host" || return 1

    ceph osd crush add-bucket host4 host
    ceph osd crush move host4 datacenter=dc2
    ceph osd crush rule create-stretch-replicated 2>&1 | grep "Error EINVAL: host host3 has no OSDs" || return 1


    ceph osd crush set osd.2 1.0 host=host3
    ceph osd crush rule create-stretch-replicated 2>&1 | grep "Error EINVAL: host host4 has no OSDs" || return 1

    ceph osd crush set osd.3 1.0 host=host4

    ceph osd crush rule create-stretch-replicated || return 1

    ceph osd crush rule dump stretch_rule | jq '.steps[1].op' | grep "choose_firstn" || return 1
    ceph osd crush rule dump stretch_rule | jq '.steps[1].num' | grep "0" || return 1
    ceph osd crush rule dump stretch_rule | jq '.steps[1].type' | grep "datacenter" || return 1

    ceph osd crush rule dump stretch_rule | jq '.steps[2].op' | grep "chooseleaf_firstn" || return 1
    ceph osd crush rule dump stretch_rule | jq '.steps[2].num' | grep "2" || return 1
    ceph osd crush rule dump stretch_rule | jq '.steps[2].type' | grep "host" || return 1
}

main crush-stretch "$@"
