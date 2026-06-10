#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON_A="127.0.0.1:7159" # git grep '\<7159\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7160" # git grep '\<7160\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7161" # git grep '\<7161\>' : there must be only one
    export CEPH_MON="$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"
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

# Test create-stretch-replicated fails when insufficient number of zones, hosts, and osds
function TEST_stretch_replicated() {
    local dir=$1
    run_mon $dir a --public-addr=$CEPH_MON_A || return 1
    run_mon $dir b --public-addr=$CEPH_MON_B || return 1
    run_mon $dir c --public-addr=$CEPH_MON_C || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1

    ceph osd crush add-bucket dc1 datacenter
    ceph osd crush add-bucket dc2 datacenter
    ceph osd crush move dc1 root=default
    ceph osd crush move dc2 root=default

    ceph osd crush rule create-stretch-replicated 2>&1 | grep "Error EINVAL: zone dc1 has only 0 items of type host" || return 1

    ceph osd crush add-bucket host1 host
    ceph osd crush move host1 datacenter=dc1
    ceph osd crush rule create-stretch-replicated 2>&1 | grep "Error EINVAL: zone dc1 has only 1 items of type host" || return 1

    ceph osd crush add-bucket host2 host
    ceph osd crush move host2 datacenter=dc1
    ceph osd crush rule create-stretch-replicated 2>&1 | grep "Error EINVAL: zone dc1 does not have 2 hosts with at least one OSD" || return 1

    ceph osd crush set osd.0 1.0 host=host1
    ceph osd crush rule create-stretch-replicated 2>&1 | grep "Error EINVAL: zone dc1 does not have 2 hosts with at least one OSD" || return 1

    ceph osd crush set osd.1 1.0 host=host2
    ceph osd crush rule create-stretch-replicated 2>&1 | grep "Error EINVAL: zone dc2 has only 0 items of type host" || return 1

    ceph osd crush add-bucket host3 host
    ceph osd crush move host3 datacenter=dc2
    ceph osd crush rule create-stretch-replicated 2>&1 | grep "Error EINVAL: zone dc2 has only 1 items of type host" || return 1

    ceph osd crush add-bucket host4 host
    ceph osd crush move host4 datacenter=dc2
    ceph osd crush rule create-stretch-replicated 2>&1 | grep "Error EINVAL: zone dc2 does not have 2 hosts with at least one OSD" || return 1


    ceph osd crush set osd.2 1.0 host=host3
    ceph osd crush rule create-stretch-replicated 2>&1 | grep "Error EINVAL: zone dc2 does not have 2 hosts with at least one OSD" || return 1

    ceph osd crush set osd.3 1.0 host=host4

    ceph osd crush rule create-stretch-replicated || return 1

    ceph osd crush rule dump stretch_replica_rule | jq '.steps[1].op' | grep "choose_firstn" || return 1
    ceph osd crush rule dump stretch_replica_rule | jq '.steps[1].num' | grep "0" || return 1
    ceph osd crush rule dump stretch_replica_rule | jq '.steps[1].type' | grep "datacenter" || return 1

    ceph osd crush rule dump stretch_replica_rule | jq '.steps[2].op' | grep "chooseleaf_firstn" || return 1
    ceph osd crush rule dump stretch_replica_rule | jq '.steps[2].num' | grep "2" || return 1
    ceph osd crush rule dump stretch_replica_rule | jq '.steps[2].type' | grep "host" || return 1
}

# Test create-erasure fails when insufficient number of zones, hosts, and osds
function TEST_stretch_ec() {
    local dir=$1
    run_mon $dir a --public-addr=$CEPH_MON_A || return 1
    run_mon $dir b --public-addr=$CEPH_MON_B || return 1
    run_mon $dir c --public-addr=$CEPH_MON_C || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1
    run_osd $dir 4 || return 1
    run_osd $dir 5 || return 1

    ceph osd crush add-bucket dc1 datacenter
    ceph osd crush add-bucket dc2 datacenter
    ceph osd crush move dc1 root=default
    ceph osd crush move dc2 root=default

    ceph osd erasure-code-profile set stretch_ec_profile plugin=jerasure k=2 m=1 crush-num-osd-failure-domains=2
    ceph osd crush rule create-erasure stretch_erasurecode_rule stretch_ec_profile --num-zones 2
    ceph osd crush rule create-erasure stretch_erasurecode_rule stretch_ec_profile --num-zones 2  2>&1 | grep "Error EINVAL: zone dc1 has only 0 items of type host" || return 1

    ceph osd crush add-bucket host1 host
    ceph osd crush move host1 datacenter=dc1
    ceph osd crush rule create-erasure stretch_erasurecode_rule stretch_ec_profile --num-zones 2 2>&1 | grep "Error EINVAL: zone dc1 has only 1 items of type host" || return 1

    ceph osd crush add-bucket host2 host
    ceph osd crush move host2 datacenter=dc1
    ceph osd crush rule create-erasure stretch_erasurecode_rule stretch_ec_profile --num-zones 2
    ceph osd crush rule create-erasure stretch_erasurecode_rule stretch_ec_profile --num-zones 2 2>&1 | grep "Error EINVAL: zone dc1 has only 2 items of type host" || return 1

    ceph osd crush add-bucket host3 host
    ceph osd crush move host3 datacenter=dc1
    ceph osd crush rule create-erasure stretch_erasurecode_rule stretch_ec_profile --num-zones 2
    ceph osd crush rule create-erasure stretch_erasurecode_rule stretch_ec_profile --num-zones 2 2>&1 | grep "Error EINVAL: zone dc1 does not have 3 hosts with at least one OSD" || return 1

    ceph osd crush set osd.0 1.0 host=host1
    ceph osd crush rule create-erasure stretch_erasurecode_rule stretch_ec_profile --num-zones 2 2>&1 | grep "Error EINVAL: zone dc1 does not have 3 hosts with at least one OSD" || return 1

    ceph osd crush set osd.1 1.0 host=host2
    ceph osd crush rule create-erasure stretch_erasurecode_rule stretch_ec_profile --num-zones 2 2>&1 | grep "Error EINVAL: zone dc1 does not have 3 hosts with at least one OSD" || return 1

    ceph osd crush set osd.2 1.0 host=host3
    ceph osd crush rule create-erasure stretch_erasurecode_rule stretch_ec_profile --num-zones 2 2>&1 | grep "Error EINVAL: zone dc2 has only 0 items of type host" || return 1

    ceph osd crush add-bucket host4 host
    ceph osd crush move host4 datacenter=dc2
    ceph osd crush rule create-erasure stretch_erasurecode_rule stretch_ec_profile --num-zones 2 2>&1 | grep "Error EINVAL: zone dc2 has only 1 items of type host" || return 1

    ceph osd crush add-bucket host5 host
    ceph osd crush move host5 datacenter=dc2
    ceph osd crush rule create-erasure stretch_erasurecode_rule stretch_ec_profile --num-zones 2 2>&1 | grep "Error EINVAL: zone dc2 has only 2 items of type host" || return 1

    ceph osd crush add-bucket host6 host
    ceph osd crush move host6 datacenter=dc2
    ceph osd crush rule create-erasure stretch_erasurecode_rule stretch_ec_profile --num-zones 2 2>&1 | grep "Error EINVAL: zone dc2 does not have 3 hosts with at least one OSD" || return 1

    ceph osd crush set osd.3 1.0 host=host4
    ceph osd crush rule create-erasure stretch_erasurecode_rule stretch_ec_profile --num-zones 2 2>&1 | grep "Error EINVAL: zone dc2 does not have 3 hosts with at least one OSD" || return 1

    ceph osd crush set osd.4 1.0 host=host5
    ceph osd crush rule create-erasure stretch_erasurecode_rule stretch_ec_profile --num-zones 2 2>&1 | grep "Error EINVAL: zone dc2 does not have 3 hosts with at least one OSD" || return 1

    ceph osd crush set osd.5 1.0 host=host6

    ceph osd crush rule create-erasure stretch_erasurecode_rule stretch_ec_profile --num-zones 2 || return 1

    ceph osd crush rule dump stretch_erasurecode_rule | jq '.steps[3].op' | grep "choose_firstn" || return 1
    ceph osd crush rule dump stretch_erasurecode_rule | jq '.steps[3].num' | grep "0" || return 1
    ceph osd crush rule dump stretch_erasurecode_rule | jq '.steps[3].type' | grep "datacenter" || return 1

    ceph osd crush rule dump stretch_erasurecode_rule | jq '.steps[4].op' | grep "chooseleaf_indep" || return 1
    ceph osd crush rule dump stretch_erasurecode_rule | jq '.steps[4].num' | grep "3" || return 1
    ceph osd crush rule dump stretch_erasurecode_rule | jq '.steps[4].type' | grep "host" || return 1
}

# # Test osd pool create for stretch EC fails when insufficient number of zones, hosts, and osds
function TEST_pool_create_stretch_ec() {
    local dir=$1
    run_mon $dir a --public-addr=$CEPH_MON_A || return 1
    run_mon $dir b --public-addr=$CEPH_MON_B || return 1
    run_mon $dir c --public-addr=$CEPH_MON_C || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1
    run_osd $dir 4 || return 1
    run_osd $dir 5 || return 1

    # Set monitor locations for stretch mode validation
    ceph mon set_location a datacenter=dc1
    ceph mon set_location b datacenter=dc2
    ceph mon set_location c datacenter=arbiter

    ceph osd crush add-bucket dc1 datacenter
    ceph osd crush add-bucket dc2 datacenter
    ceph osd crush move dc1 root=default
    ceph osd crush move dc2 root=default

    ceph mon set_location a datacenter=dc1
    ceph mon set_location b datacenter=dc2
    ceph mon set_location c datacenter=dc3

    ceph osd erasure-code-profile set stretch_ec_profile plugin=jerasure k=2 m=1 crush-num-osd-failure-domains=2
    ceph osd pool create data0 erasure stretch_ec_profile --num-zones 2 2>&1 | grep "Error EINVAL: zone dc1 has only 0 items of type host" || return 1

    ceph osd crush add-bucket host1 host
    ceph osd crush move host1 datacenter=dc1
    ceph osd pool create data0 erasure stretch_ec_profile --num-zones 2 2>&1 | grep "Error EINVAL: zone dc1 has only 1 items of type host" || return 1

    ceph osd crush add-bucket host2 host
    ceph osd crush move host2 datacenter=dc1
    ceph osd pool create data0 erasure stretch_ec_profile --num-zones 2 2>&1 | grep "Error EINVAL: zone dc1 has only 2 items of type host" || return 1

    ceph osd crush add-bucket host3 host
    ceph osd crush move host3 datacenter=dc1
    
    ceph osd pool create data0 erasure stretch_ec_profile --num-zones 2 2>&1 | grep "Error EINVAL: zone dc1 does not have 3 hosts with at least one OSD" || return 1

    ceph osd crush set osd.0 1.0 host=host1
    ceph osd pool create data0 erasure stretch_ec_profile --num-zones 2 2>&1 | grep "Error EINVAL: zone dc1 does not have 3 hosts with at least one OSD" || return 1

    ceph osd crush set osd.1 1.0 host=host2
    ceph osd pool create data0 erasure stretch_ec_profile --num-zones 2 2>&1 | grep "Error EINVAL: zone dc1 does not have 3 hosts with at least one OSD" || return 1

    ceph osd crush set osd.2 1.0 host=host3
    ceph osd pool create data0 erasure stretch_ec_profile --num-zones 2 2>&1 | grep "Error EINVAL: zone dc2 has only 0 items of type host" || return 1

    ceph osd crush add-bucket host4 host
    ceph osd crush move host4 datacenter=dc2
    ceph osd pool create data0 erasure stretch_ec_profile --num-zones 2 2>&1 | grep "Error EINVAL: zone dc2 has only 1 items of type host" || return 1

    ceph osd crush add-bucket host5 host
    ceph osd crush move host5 datacenter=dc2
    ceph osd pool create data0 erasure stretch_ec_profile --num-zones 2 2>&1 | grep "Error EINVAL: zone dc2 has only 2 items of type host" || return 1

    ceph osd crush add-bucket host6 host
    ceph osd crush move host6 datacenter=dc2
    ceph osd pool create data0 erasure stretch_ec_profile --num-zones 2 2>&1 | grep "Error EINVAL: zone dc2 does not have 3 hosts with at least one OSD" || return 1

    ceph osd crush set osd.3 1.0 host=host4
    ceph osd pool create data0 erasure stretch_ec_profile --num-zones 2 2>&1 | grep "Error EINVAL: zone dc2 does not have 3 hosts with at least one OSD" || return 1

    ceph osd crush set osd.4 1.0 host=host5
    ceph osd pool create data0 erasure stretch_ec_profile --num-zones 2 2>&1 | grep "Error EINVAL: zone dc2 does not have 3 hosts with at least one OSD" || return 1

    ceph osd crush set osd.5 1.0 host=host6

    ceph osd pool create data0 erasure stretch_ec_profile --num-zones 2 || return 1

    ceph osd crush rule dump data0 | jq '.steps[3].op' | grep "choose_firstn" || return 1
    ceph osd crush rule dump data0 | jq '.steps[3].num' | grep "0" || return 1
    ceph osd crush rule dump data0 | jq '.steps[3].type' | grep "datacenter" || return 1

    ceph osd crush rule dump data0 | jq '.steps[4].op' | grep "chooseleaf_indep" || return 1
    ceph osd crush rule dump data0 | jq '.steps[4].num' | grep "3" || return 1
    ceph osd crush rule dump data0 | jq '.steps[4].type' | grep "host" || return 1
}

# Test osd pool create for stretch replica fails when insufficient number of zones, hosts, and osds
function TEST_pool_create_stretch_replica() {
    local dir=$1
    run_mon $dir a --public-addr=$CEPH_MON_A || return 1
    run_mon $dir b --public-addr=$CEPH_MON_B || return 1
    run_mon $dir c --public-addr=$CEPH_MON_C || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1
    run_osd $dir 4 || return 1
    run_osd $dir 5 || return 1

    # Set monitor locations for stretch mode validation
    ceph mon set_location a datacenter=dc1
    ceph mon set_location b datacenter=dc2
    ceph mon set_location c datacenter=arbiter

    ceph osd crush add-bucket dc1 datacenter
    ceph osd crush add-bucket dc2 datacenter
    ceph osd crush move dc1 root=default
    ceph osd crush move dc2 root=default

    ceph osd pool create data0 --num-zones 2 2>&1 | grep "Error EINVAL: zone dc1 has only 0 items of type host" || return 1

    ceph osd crush add-bucket host1 host
    ceph osd crush move host1 datacenter=dc1
    ceph osd pool create data0 --num-zones 2 2>&1 | grep "Error EINVAL: zone dc1 has only 1 items of type host" || return 1

    ceph osd crush add-bucket host2 host
    ceph osd crush move host2 datacenter=dc1
    ceph osd pool create data0 --num-zones 2 2>&1 | grep "Error EINVAL: zone dc1 does not have 2 hosts with at least one OSD" || return 1

    ceph osd crush set osd.0 1.0 host=host1
    ceph osd pool create data0 --num-zones 2 2>&1 | grep "Error EINVAL: zone dc1 does not have 2 hosts with at least one OSD" || return 1

    ceph osd crush set osd.1 1.0 host=host2
    ceph osd pool create data0 --num-zones 2 2>&1 | grep "Error EINVAL: zone dc2 has only 0 items of type host" || return 1

    ceph osd crush add-bucket host3 host
    ceph osd crush move host3 datacenter=dc2
    ceph osd pool create data0 --num-zones 2 2>&1 | grep "Error EINVAL: zone dc2 has only 1 items of type host" || return 1

    ceph osd crush add-bucket host4 host
    ceph osd crush move host4 datacenter=dc2
    ceph osd pool create data0 --num-zones 2 2>&1 | grep "Error EINVAL: zone dc2 does not have 2 hosts with at least one OSD" || return 1


    ceph osd crush set osd.2 1.0 host=host3
    ceph osd pool create data0 --num-zones 2 2>&1 | grep "Error EINVAL: zone dc2 does not have 2 hosts with at least one OSD" || return 1

    ceph osd crush set osd.3 1.0 host=host4

    ceph osd pool create data0 --num-zones 2 || return 1

    ceph osd crush rule dump data0 | jq '.steps[1].op' | grep "choose_firstn" || return 1
    ceph osd crush rule dump data0 | jq '.steps[1].num' | grep "0" || return 1
    ceph osd crush rule dump data0 | jq '.steps[1].type' | grep "datacenter" || return 1

    ceph osd crush rule dump data0 | jq '.steps[2].op' | grep "chooseleaf_firstn" || return 1
    ceph osd crush rule dump data0 | jq '.steps[2].num' | grep "2" || return 1
    ceph osd crush rule dump data0 | jq '.steps[2].type' | grep "host" || return 1
}

# Test create stretch crush rule fails for device classes
function TEST_stretch_replicated_with_class() {
    local dir=$1
    run_mon $dir a --public-addr=$CEPH_MON_A || return 1
    run_mon $dir b --public-addr=$CEPH_MON_B || return 1
    run_mon $dir c --public-addr=$CEPH_MON_C || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1
    run_osd $dir 4 || return 1
    run_osd $dir 5 || return 1
    run_osd $dir 6 || return 1
    run_osd $dir 7 || return 1

    ceph osd crush add-bucket dc1 datacenter
    ceph osd crush add-bucket dc2 datacenter
    ceph osd crush add-bucket host1 host
    ceph osd crush add-bucket host2 host
    ceph osd crush add-bucket host3 host
    ceph osd crush add-bucket host4 host
    ceph osd crush add-bucket host5 host
    ceph osd crush add-bucket host6 host
    ceph osd crush move dc1 root=default
    ceph osd crush move dc2 root=default
    ceph osd crush move host1 datacenter=dc1
    ceph osd crush move host2 datacenter=dc1
    ceph osd crush move host3 datacenter=dc2
    ceph osd crush move host4 datacenter=dc2
    ceph osd crush move host5 datacenter=dc1
    ceph osd crush move host6 datacenter=dc2
    ceph osd crush set osd.0 1.0 host=host1
    ceph osd crush set osd.1 1.0 host=host2
    ceph osd crush set osd.2 1.0 host=host3
    ceph osd crush set osd.3 1.0 host=host4
    ceph osd crush set osd.4 1.0 host=host5
    ceph osd crush set osd.5 1.0 host=host6

    ceph osd crush rm-device-class osd.0 osd.1 osd.2 osd.3 osd.4 osd.5
    ceph osd crush set-device-class ssd osd.0 osd.1 osd.2 osd.3
    ceph osd crush set-device-class hdd osd.4 osd.5

    ceph osd crush rule create-stretch-replicated --rule-name=stretch_ssd --class=ssd || return 1
    ceph osd crush rule create-stretch-replicated --rule-name=stretch_hdd --class=hdd 2>&1 | grep "Error EINVAL: zone dc1~hdd does not have 2 hosts with at least one OSD" || return 1
    ceph osd crush add-bucket host7 host
    ceph osd crush add-bucket host8 host
    ceph osd crush move host7 datacenter=dc1
    ceph osd crush move host8 datacenter=dc2

    ceph osd crush rm-device-class osd.6 osd.7
    ceph osd crush set-device-class hdd osd.6 osd.7

    ceph osd crush set osd.6 1.0 host=host7
    ceph osd crush set osd.7 1.0 host=host8

    ceph osd crush rule create-stretch-replicated --rule-name=stretch_hdd --class=hdd || return 1
}

function TEST_stretch_ec_with_class() {
    local dir=$1
    run_mon $dir a --public-addr=$CEPH_MON_A || return 1
    run_mon $dir b --public-addr=$CEPH_MON_B || return 1
    run_mon $dir c --public-addr=$CEPH_MON_C || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1
    run_osd $dir 4 || return 1
    run_osd $dir 5 || return 1
    run_osd $dir 6 || return 1
    run_osd $dir 7 || return 1
    run_osd $dir 8 || return 1
    run_osd $dir 9 || return 1
    run_osd $dir 10 || return 1
    run_osd $dir 11 || return 1

    ceph osd erasure-code-profile set stretch_ec_ssd plugin=jerasure k=2 m=1 crush-num-osd-failure-domains=2 crush-device-class=ssd
    ceph osd erasure-code-profile set stretch_ec_hdd plugin=jerasure k=2 m=1 crush-num-osd-failure-domains=2 crush-device-class=hdd

    ceph osd crush add-bucket dc1 datacenter
    ceph osd crush add-bucket dc2 datacenter
    ceph osd crush add-bucket host1 host
    ceph osd crush add-bucket host2 host
    ceph osd crush add-bucket host3 host
    ceph osd crush add-bucket host4 host
    ceph osd crush add-bucket host5 host
    ceph osd crush add-bucket host6 host
    ceph osd crush add-bucket host7 host
    ceph osd crush add-bucket host8 host
    ceph osd crush add-bucket host9 host
    ceph osd crush add-bucket host10 host

    ceph osd crush move dc1 root=default
    ceph osd crush move dc2 root=default
    ceph osd crush move host1 datacenter=dc1
    ceph osd crush move host2 datacenter=dc1
    ceph osd crush move host3 datacenter=dc1
    ceph osd crush move host4 datacenter=dc2
    ceph osd crush move host5 datacenter=dc2
    ceph osd crush move host6 datacenter=dc2
    ceph osd crush move host7 datacenter=dc1
    ceph osd crush move host8 datacenter=dc1
    ceph osd crush move host9 datacenter=dc2
    ceph osd crush move host10 datacenter=dc2

    ceph mon set_location a datacenter=dc1
    ceph mon set_location b datacenter=dc2
    ceph mon set_location c datacenter=arbiter

    ceph osd crush set osd.0 1.0 host=host1
    ceph osd crush set osd.1 1.0 host=host2
    ceph osd crush set osd.2 1.0 host=host3
    ceph osd crush set osd.3 1.0 host=host4
    ceph osd crush set osd.4 1.0 host=host5
    ceph osd crush set osd.5 1.0 host=host6
    ceph osd crush set osd.6 1.0 host=host7
    ceph osd crush set osd.7 1.0 host=host8
    ceph osd crush set osd.8 1.0 host=host9
    ceph osd crush set osd.9 1.0 host=host10

    ceph osd crush rm-device-class osd.0 osd.1 osd.2 osd.3 osd.4 osd.5 osd.6 osd.7 osd.8 osd.9 
    ceph osd crush set-device-class ssd osd.0 osd.1 osd.2 osd.3 osd.4 osd.5
    ceph osd crush set-device-class hdd osd.6 osd.7 osd.8 osd.9

    ceph osd crush rule create-erasure stretch_ssd stretch_ec_ssd --num-zones 2 || return 1

    ceph osd crush rule create-erasure stretch_hdd stretch_ec_hdd --num-zones 2 2>&1 | grep "Error EINVAL: zone dc1~hdd does not have 3 hosts with at least one OSD" || return 1

    ceph osd crush add-bucket host11 host
    ceph osd crush add-bucket host12 host
    ceph osd crush move host11 datacenter=dc1
    ceph osd crush move host12 datacenter=dc2

    ceph osd crush rm-device-class osd.10 osd.11
    ceph osd crush set-device-class hdd osd.10 osd.11

    ceph osd crush set osd.10 1.0 host=host11
    ceph osd crush set osd.11 1.0 host=host12

    ceph osd crush rule create-erasure stretch_hdd stretch_ec_hdd --num-zones 2 || return 1
}

# Test adding a pool with different sites than other stretch mode pools
function TEST_stretch_diff_sites() {
    local dir=$1
    run_mon $dir a --public-addr=$CEPH_MON_A || return 1
    run_mon $dir b --public-addr=$CEPH_MON_B || return 1
    run_mon $dir c --public-addr=$CEPH_MON_C || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1
    run_osd $dir 4 || return 1
    run_osd $dir 5 || return 1
    run_osd $dir 6 || return 1
    run_osd $dir 7 || return 1

    ceph osd crush add-bucket dc1 datacenter
    ceph osd crush add-bucket dc2 datacenter
    ceph osd crush add-bucket host1 host
    ceph osd crush add-bucket host2 host
    ceph osd crush add-bucket host3 host
    ceph osd crush add-bucket host4 host
    ceph osd crush add-bucket host5 host
    ceph osd crush add-bucket host6 host

    ceph osd crush move dc1 root=default
    ceph osd crush move dc2 root=default
    ceph osd crush move host1 datacenter=dc1
    ceph osd crush move host2 datacenter=dc1
    ceph osd crush move host3 datacenter=dc2
    ceph osd crush move host4 datacenter=dc2
    ceph osd crush move host5 datacenter=dc1
    ceph osd crush move host6 datacenter=dc2

    ceph osd crush set osd.0 1.0 host=host1
    ceph osd crush set osd.1 1.0 host=host2
    ceph osd crush set osd.2 1.0 host=host3
    ceph osd crush set osd.3 1.0 host=host4
    ceph osd crush set osd.4 1.0 host=host5
    ceph osd crush set osd.5 1.0 host=host6

    ceph osd crush rm-device-class osd.0 osd.1 osd.2 osd.3 osd.4 osd.5
    ceph osd crush set-device-class ssd osd.0 osd.1 osd.2 osd.3
    ceph osd crush set-device-class hdd osd.4 osd.5

    ceph osd pool create data0
    ceph osd pool create data1

    ceph osd crush rule create-stretch-replicated --rule-name=stretch_ssd --class=ssd
    ceph mon set election_strategy connectivity

    ceph mon set_location a datacenter=dc1
    ceph mon set_location b datacenter=dc2
    ceph mon set_location c datacenter=arbiter

    ceph mon enable_stretch_mode c stretch_ssd datacenter

    # create pool with crush rule with different sites
    ceph osd crush add-bucket dc3 datacenter

    ceph osd getcrushmap -o $dir/crushmap.bin || return 1
    crushtool -d $dir/crushmap.bin -o $dir/crushmap.txt || return 1
    cat >> $dir/crushmap.txt <<EOF
    rule bad_rule {
        id 4
        type replicated
        step take dc1
        step chooseleaf firstn 2 type host
        step emit
        step take dc3
        step chooseleaf firstn 2 type host
        step emit
    }
EOF
    crushtool -c $dir/crushmap.txt -o $dir/crushmap.new.bin || return 1
    ceph osd setcrushmap -i $dir/crushmap.new.bin || return 1

    ceph osd pool create data2 --rule bad_rule --num-zones 2 2>&1 | grep "CRUSH rule 4 uses different datacenter buckets than configured for stretch mode" || return 1
}

function TEST_stretch_replica_device_class_pools() {
    local dir=$1
    run_mon $dir a --public-addr=$CEPH_MON_A || return 1
    run_mon $dir b --public-addr=$CEPH_MON_B || return 1
    run_mon $dir c --public-addr=$CEPH_MON_C || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1
    run_osd $dir 4 || return 1
    run_osd $dir 5 || return 1
    run_osd $dir 6 || return 1
    run_osd $dir 7 || return 1


    ceph osd crush add-bucket dc1 datacenter
    ceph osd crush add-bucket dc2 datacenter
    ceph osd crush add-bucket host1 host
    ceph osd crush add-bucket host2 host
    ceph osd crush add-bucket host3 host
    ceph osd crush add-bucket host4 host
    ceph osd crush add-bucket host5 host
    ceph osd crush add-bucket host6 host
    ceph osd crush add-bucket host7 host
    ceph osd crush add-bucket host8 host

    ceph osd crush move dc1 root=default
    ceph osd crush move dc2 root=default
    ceph osd crush move host1 datacenter=dc1
    ceph osd crush move host2 datacenter=dc1
    ceph osd crush move host3 datacenter=dc2
    ceph osd crush move host4 datacenter=dc2
    ceph osd crush move host5 datacenter=dc1
    ceph osd crush move host6 datacenter=dc2
    ceph osd crush move host7 datacenter=dc1
    ceph osd crush move host8 datacenter=dc2

    ceph osd crush set osd.0 1.0 host=host1
    ceph osd crush set osd.1 1.0 host=host2
    ceph osd crush set osd.2 1.0 host=host3
    ceph osd crush set osd.3 1.0 host=host4


    ceph osd crush rm-device-class osd.0 osd.1 osd.2 osd.3 osd.4 osd.5 osd.6 osd.7
    ceph osd crush set-device-class ssd osd.0 osd.1 osd.2 osd.3
    ceph osd crush set-device-class hdd osd.4 osd.5 osd.6 osd.7

    ceph osd crush set osd.4 1.0 host=host5
    ceph osd crush set osd.5 1.0 host=host6
    ceph osd crush set osd.6 1.0 host=host7
    ceph osd crush set osd.7 1.0 host=host8

    ceph mon set_location a datacenter=dc1
    ceph mon set_location b datacenter=dc2
    ceph mon set_location c datacenter=arbiter

    # Create pools using different device classes
    ceph osd crush rule create-stretch-replicated --rule-name=stretch_ssd --class=ssd || return 1

    ceph osd crush rule create-stretch-replicated --rule-name=stretch_hdd --class=hdd || return 1

    ceph osd pool create pool_ssd || return 1

    ceph mon set election_strategy connectivity

    ceph mon enable_stretch_mode c stretch_ssd datacenter

    ceph osd pool create data0 replicated --rule stretch_ssd --num-zones 2 || return 1

    ceph osd pool create pool_hdd replicated --rule stretch_hdd --num-zones 2 || return 1

    ceph osd pool get pool_ssd crush_rule | grep "stretch_ssd" || return 1
    ceph osd pool get pool_hdd crush_rule | grep "stretch_hdd" || return 1

    ceph osd pool ls | grep "pool_ssd" || return 1
    ceph osd pool ls | grep "pool_hdd" || return 1
}

function TEST_stretch_ec_stretch_set() {
    local dir=$1
    run_mon $dir a --public-addr=$CEPH_MON_A || return 1
    run_mon $dir b --public-addr=$CEPH_MON_B || return 1
    run_mon $dir c --public-addr=$CEPH_MON_C || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1
    run_osd $dir 4 || return 1
    run_osd $dir 5 || return 1
    run_osd $dir 6 || return 1
    run_osd $dir 7 || return 1
    run_osd $dir 8 || return 1
    run_osd $dir 9 || return 1
    run_osd $dir 10 || return 1
    run_osd $dir 11 || return 1

    ceph osd crush add-bucket dc1 datacenter
    ceph osd crush add-bucket dc2 datacenter
    ceph osd crush add-bucket host1 host
    ceph osd crush add-bucket host2 host
    ceph osd crush add-bucket host3 host
    ceph osd crush add-bucket host4 host
    ceph osd crush add-bucket host5 host
    ceph osd crush add-bucket host6 host
    ceph osd crush add-bucket host7 host
    ceph osd crush add-bucket host8 host
    ceph osd crush add-bucket host9 host
    ceph osd crush add-bucket host10 host
    ceph osd crush add-bucket host11 host
    ceph osd crush add-bucket host12 host

    ceph osd crush move dc1 root=default
    ceph osd crush move dc2 root=default
    ceph osd crush move host1 datacenter=dc1
    ceph osd crush move host2 datacenter=dc1
    ceph osd crush move host3 datacenter=dc1
    ceph osd crush move host4 datacenter=dc1
    ceph osd crush move host5 datacenter=dc1
    ceph osd crush move host6 datacenter=dc1
    ceph osd crush move host7 datacenter=dc2
    ceph osd crush move host8 datacenter=dc2
    ceph osd crush move host9 datacenter=dc2
    ceph osd crush move host10 datacenter=dc2
    ceph osd crush move host11 datacenter=dc2
    ceph osd crush move host12 datacenter=dc2

    ceph osd crush set osd.0 1.0 host=host1
    ceph osd crush set osd.1 1.0 host=host2
    ceph osd crush set osd.2 1.0 host=host3
    ceph osd crush set osd.3 1.0 host=host4
    ceph osd crush set osd.4 1.0 host=host5
    ceph osd crush set osd.5 1.0 host=host6
    ceph osd crush set osd.6 1.0 host=host7
    ceph osd crush set osd.7 1.0 host=host8
    ceph osd crush set osd.8 1.0 host=host9
    ceph osd crush set osd.9 1.0 host=host10
    ceph osd crush set osd.10 1.0 host=host11
    ceph osd crush set osd.11 1.0 host=host12

    ceph mon set_location a datacenter=dc4
    ceph mon set_location b datacenter=dc1
    ceph mon set_location c datacenter=dc2

    ceph mon set election_strategy connectivity

    ceph osd erasure-code-profile set stretch_ec_profile plugin=jerasure k=4 m=2 crush-num-osd-failure-domains=2

    ceph osd crush rule create-erasure stretch_ec stretch_ec_profile 2

    ceph osd pool create data0 erasure --erasure_code_profile=stretch_ec_profile || return 1
    ceph osd pool create data1 erasure --erasure_code_profile=stretch_ec_profile || return 1

    ceph osd pool set data0 allow_ec_optimizations true
    ceph osd pool set data1 allow_ec_optimizations true

    ceph osd pool stretch set data0 2 6 datacenter stretch_ec 12 4 --yes-i-really-mean-it || return 1
    ceph osd pool stretch set data1 2 6 datacenter stretch_ec 12 4 --yes-i-really-mean-it || return 1
}

function TEST_stretch_diff_bucket_barrier() {
    local dir=$1
    run_mon $dir a --public-addr=$CEPH_MON_A || return 1
    run_mon $dir b --public-addr=$CEPH_MON_B || return 1
    run_mon $dir c --public-addr=$CEPH_MON_C || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1
    run_osd $dir 4 || return 1
    run_osd $dir 5 || return 1
    run_osd $dir 6 || return 1
    run_osd $dir 7 || return 1

    ceph osd crush add-bucket z1 zone
    ceph osd crush add-bucket z2 zone
    ceph osd crush add-bucket host1 host
    ceph osd crush add-bucket host2 host
    ceph osd crush add-bucket host3 host
    ceph osd crush add-bucket host4 host
    ceph osd crush add-bucket host5 host
    ceph osd crush add-bucket host6 host
    ceph osd crush add-bucket host7 host
    ceph osd crush add-bucket host8 host

    ceph osd crush move z1 root=default
    ceph osd crush move z2 root=default

    ceph osd crush move host1 zone=z1
    ceph osd crush move host2 zone=z1
    ceph osd crush move host3 zone=z2
    ceph osd crush move host4 zone=z2
    ceph osd crush move host5 zone=z1
    ceph osd crush move host6 zone=z2
    ceph osd crush move host7 zone=z1
    ceph osd crush move host8 zone=z2

    ceph osd crush set osd.0 1.0 host=host1
    ceph osd crush set osd.1 1.0 host=host2
    ceph osd crush set osd.2 1.0 host=host3
    ceph osd crush set osd.3 1.0 host=host4
    ceph osd crush set osd.4 1.0 host=host5
    ceph osd crush set osd.5 1.0 host=host6
    ceph osd crush set osd.6 1.0 host=host7
    ceph osd crush set osd.7 1.0 host=host8

    ceph osd crush rm-device-class osd.0 osd.1 osd.2 osd.3 osd.4 osd.5 osd.6 osd.7
    ceph osd crush set-device-class ssd osd.0 osd.1 osd.2 osd.3 osd.4 osd.5 osd.6 osd.7

    ceph mon set_location a zone=z1
    ceph mon set_location b zone=z2
    ceph mon set_location c zone=arbiter


    ceph osd crush rule create-stretch-replicated --rule-name=stretch_zone --zone-failure-domain=zone --class=ssd  || return 1

    ceph osd pool create pool_zone || return 1

    ceph mon set election_strategy connectivity

    ceph mon enable_stretch_mode c stretch_zone zone

    # Wait for stretch mode to be fully committed before testing validation
    ceph mon dump | grep "stretch_mode_enabled 1" || return 1

    # Try to create pool with datacenter failure domain when only zones exist in CRUSH
    ceph osd pool create pool_dc replicated --zone-failure-domain=datacenter --num-zones 2 --class=ssd 2>&1 | grep "Error EINVAL: number of zones 0 for type datacenter is less than num_failure_domains 2" || return 1
}

main crush-stretch "$@"
