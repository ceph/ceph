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
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
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

# Test osd pool create for stretch replica builds its rule from the root and
# failure domains it is not given in the osd_pool_default_* options
function TEST_pool_create_stretch_replica_option_defaults() {
    local dir=$1
    run_mon $dir a --public-addr=$CEPH_MON_A || return 1
    run_mon $dir b --public-addr=$CEPH_MON_B || return 1
    run_mon $dir c --public-addr=$CEPH_MON_C || return 1
    for osd in 0 1 2 3; do
        run_osd $dir $osd || return 1
    done
    ceph osd crush add-bucket other root || return 1
    for dc in 1 2; do
        ceph osd crush add-bucket room$dc room || return 1
        ceph osd crush move room$dc root=other || return 1
    done
    ceph osd crush set osd.0 1.0 host=h1 room=room1 root=other || return 1
    ceph osd crush set osd.1 1.0 host=h1 room=room1 root=other || return 1
    ceph osd crush set osd.2 1.0 host=h2 room=room2 root=other || return 1
    ceph osd crush set osd.3 1.0 host=h2 room=room2 root=other || return 1
    ceph mon set_location a room=room1 || return 1
    ceph mon set_location b room=room2 || return 1
    ceph mon set_location c room=arbiter || return 1

    ceph config set global osd_pool_default_root other || return 1
    ceph config set global osd_pool_default_zone_failure_domain room || return 1
    ceph config set global osd_pool_default_osd_failure_domain osd || return 1
    ceph osd pool create from_options --num_zones 2 --replica 2 || return 1
    ceph osd crush rule dump from_options > $dir/rule.json || return 1
    jq -e '.steps[] | select(.op == "take" and .item_name == "other")' $dir/rule.json || return 1
    jq -e '.steps[] | select(.type == "room")' $dir/rule.json || return 1
    jq -e '.steps[] | select(.type == "osd")' $dir/rule.json || return 1
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

    ceph osd crush add-bucket dc1 datacenter || return 1
    ceph osd crush add-bucket dc2 datacenter || return 1
    ceph osd crush move dc1 root=default || return 1
    ceph osd crush move dc2 root=default || return 1

    ceph osd erasure-code-profile set stretch_ec_profile plugin=jerasure k=2 m=1 crush-num-osd-failure-domains=2 || return 1
    ceph osd crush rule create-erasure stretch_erasurecode_rule stretch_ec_profile --num_zones 2 2>&1 | grep "Error EINVAL: zone dc1 has only 0 items of type host" || return 1

    ceph osd crush add-bucket host1 host || return 1
    ceph osd crush move host1 datacenter=dc1 || return 1
    ceph osd crush rule create-erasure stretch_erasurecode_rule stretch_ec_profile --num_zones 2 2>&1 | grep "Error EINVAL: zone dc1 has only 1 items of type host" || return 1

    ceph osd crush add-bucket host2 host || return 1
    ceph osd crush move host2 datacenter=dc1 || return 1
    ceph osd crush rule create-erasure stretch_erasurecode_rule stretch_ec_profile --num_zones 2 2>&1 | grep "Error EINVAL: zone dc1 has only 2 items of type host" || return 1

    ceph osd crush add-bucket host3 host || return 1
    ceph osd crush move host3 datacenter=dc1 || return 1
    ceph osd crush rule create-erasure stretch_erasurecode_rule stretch_ec_profile --num_zones 2 2>&1 | grep "Error EINVAL: zone dc1 does not have 3 hosts with at least one OSD" || return 1

    ceph osd crush set osd.0 1.0 host=host1 || return 1
    ceph osd crush rule create-erasure stretch_erasurecode_rule stretch_ec_profile --num_zones 2 2>&1 | grep "Error EINVAL: zone dc1 does not have 3 hosts with at least one OSD" || return 1

    ceph osd crush set osd.1 1.0 host=host2 || return 1
    ceph osd crush rule create-erasure stretch_erasurecode_rule stretch_ec_profile --num_zones 2 2>&1 | grep "Error EINVAL: zone dc1 does not have 3 hosts with at least one OSD" || return 1

    ceph osd crush set osd.2 1.0 host=host3 || return 1
    ceph osd crush rule create-erasure stretch_erasurecode_rule stretch_ec_profile --num_zones 2 2>&1 | grep "Error EINVAL: zone dc2 has only 0 items of type host" || return 1

    ceph osd crush add-bucket host4 host || return 1
    ceph osd crush move host4 datacenter=dc2 || return 1
    ceph osd crush rule create-erasure stretch_erasurecode_rule stretch_ec_profile --num_zones 2 2>&1 | grep "Error EINVAL: zone dc2 has only 1 items of type host" || return 1

    ceph osd crush add-bucket host5 host || return 1
    ceph osd crush move host5 datacenter=dc2 || return 1
    ceph osd crush rule create-erasure stretch_erasurecode_rule stretch_ec_profile --num_zones 2 2>&1 | grep "Error EINVAL: zone dc2 has only 2 items of type host" || return 1

    ceph osd crush add-bucket host6 host || return 1
    ceph osd crush move host6 datacenter=dc2 || return 1
    ceph osd crush rule create-erasure stretch_erasurecode_rule stretch_ec_profile --num_zones 2 2>&1 | grep "Error EINVAL: zone dc2 does not have 3 hosts with at least one OSD" || return 1

    ceph osd crush set osd.3 1.0 host=host4 || return 1
    ceph osd crush rule create-erasure stretch_erasurecode_rule stretch_ec_profile --num_zones 2 2>&1 | grep "Error EINVAL: zone dc2 does not have 3 hosts with at least one OSD" || return 1

    ceph osd crush set osd.4 1.0 host=host5 || return 1
    ceph osd crush rule create-erasure stretch_erasurecode_rule stretch_ec_profile --num_zones 2 2>&1 | grep "Error EINVAL: zone dc2 does not have 3 hosts with at least one OSD" || return 1

    ceph osd crush set osd.5 1.0 host=host6 || return 1

    ceph osd crush rule create-erasure stretch_erasurecode_rule stretch_ec_profile --num_zones 2 || return 1

    ceph osd crush rule dump stretch_erasurecode_rule | jq '.steps[3].op' | grep "choose_firstn" || return 1
    ceph osd crush rule dump stretch_erasurecode_rule | jq '.steps[3].num' | grep "2" || return 1
    ceph osd crush rule dump stretch_erasurecode_rule | jq '.steps[3].type' | grep "datacenter" || return 1

    ceph osd crush rule dump stretch_erasurecode_rule | jq '.steps[4].op' | grep "chooseleaf_indep" || return 1
    ceph osd crush rule dump stretch_erasurecode_rule | jq '.steps[4].num' | grep "3" || return 1
    ceph osd crush rule dump stretch_erasurecode_rule | jq '.steps[4].type' | grep "host" || return 1
}

# Test osd pool create for stretch EC fails when insufficient number of zones, hosts, and osds
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
    ceph mon set_location a datacenter=dc1 || return 1
    ceph mon set_location b datacenter=dc2 || return 1
    ceph mon set_location c datacenter=arbiter || return 1

    ceph osd crush add-bucket dc1 datacenter || return 1
    ceph osd crush add-bucket dc2 datacenter || return 1
    ceph osd crush move dc1 root=default || return 1
    ceph osd crush move dc2 root=default || return 1

    ceph osd pool create data0 erasure --num_zones 2 --k 2 --m 1 2>&1 | grep "Error EINVAL: zone dc1 has only 0 items of type host" || return 1

    ceph osd crush add-bucket host1 host || return 1
    ceph osd crush move host1 datacenter=dc1 || return 1
    ceph osd pool create data0 erasure --num_zones 2 --k 2 --m 1 2>&1 | grep "Error EINVAL: zone dc1 has only 1 items of type host" || return 1

    ceph osd crush add-bucket host2 host || return 1
    ceph osd crush move host2 datacenter=dc1 || return 1
    ceph osd pool create data0 erasure --num_zones 2 --k 2 --m 1 2>&1 | grep "Error EINVAL: zone dc1 has only 2 items of type host" || return 1

    ceph osd crush add-bucket host3 host || return 1
    ceph osd crush move host3 datacenter=dc1 || return 1

    # The zone weights are checked before the rule is built, so the OSDs
    # join with weight 0 until both zones have them.
    
    ceph osd pool create data0 erasure --num_zones 2 --k 2 --m 1 2>&1 | grep "Error EINVAL: zone dc1 does not have 3 hosts with at least one OSD" || return 1

    ceph osd crush set osd.0 0 host=host1 || return 1
    ceph osd pool create data0 erasure --num_zones 2 --k 2 --m 1 2>&1 | grep "Error EINVAL: zone dc1 does not have 3 hosts with at least one OSD" || return 1

    ceph osd crush set osd.1 0 host=host2 || return 1
    ceph osd pool create data0 erasure --num_zones 2 --k 2 --m 1 2>&1 | grep "Error EINVAL: zone dc1 does not have 3 hosts with at least one OSD" || return 1

    ceph osd crush set osd.2 0 host=host3 || return 1
    ceph osd pool create data0 erasure --num_zones 2 --k 2 --m 1 2>&1 | grep "Error EINVAL: zone dc2 has only 0 items of type host" || return 1

    ceph osd crush add-bucket host4 host || return 1
    ceph osd crush move host4 datacenter=dc2 || return 1
    ceph osd pool create data0 erasure --num_zones 2 --k 2 --m 1 2>&1 | grep "Error EINVAL: zone dc2 has only 1 items of type host" || return 1

    ceph osd crush add-bucket host5 host || return 1
    ceph osd crush move host5 datacenter=dc2 || return 1
    ceph osd pool create data0 erasure --num_zones 2 --k 2 --m 1 2>&1 | grep "Error EINVAL: zone dc2 has only 2 items of type host" || return 1

    ceph osd crush add-bucket host6 host || return 1
    ceph osd crush move host6 datacenter=dc2 || return 1
    ceph osd pool create data0 erasure --num_zones 2 --k 2 --m 1 2>&1 | grep "Error EINVAL: zone dc2 does not have 3 hosts with at least one OSD" || return 1

    ceph osd crush set osd.3 0 host=host4 || return 1
    ceph osd pool create data0 erasure --num_zones 2 --k 2 --m 1 2>&1 | grep "Error EINVAL: zone dc2 does not have 3 hosts with at least one OSD" || return 1

    ceph osd crush set osd.4 0 host=host5 || return 1
    ceph osd pool create data0 erasure --num_zones 2 --k 2 --m 1 2>&1 | grep "Error EINVAL: zone dc2 does not have 3 hosts with at least one OSD" || return 1

    ceph osd crush set osd.5 0 host=host6 || return 1
    for osd in 0 1 2 3 4 5; do
        ceph osd crush reweight osd.$osd 1.0 || return 1
    done
    ceph osd pool create data0 erasure --num_zones 2 --k 2 --m 1 || return 1

    ceph osd crush rule dump data0 | jq '.steps[3].op' | grep "choose_firstn" || return 1
    ceph osd crush rule dump data0 | jq '.steps[3].num' | grep "2" || return 1
    ceph osd crush rule dump data0 | jq '.steps[3].type' | grep "datacenter" || return 1

    ceph osd crush rule dump data0 | jq '.steps[4].op' | grep "chooseleaf_indep" || return 1
    ceph osd crush rule dump data0 | jq '.steps[4].num' | grep "3" || return 1
    ceph osd crush rule dump data0 | jq '.steps[4].type' | grep "host" || return 1

    ceph osd erasure-code-profile set osd_failure_domain_profile \
        plugin=jerasure k=2 m=1 crush-osd-failure-domain=osd || return 1
    ceph osd pool create data_osd_failure_domain erasure --num_zones 2 \
        --erasure_code_profile=osd_failure_domain_profile || return 1
    ceph osd crush rule dump data_osd_failure_domain | \
        jq -r '.steps[4].type' | grep '^osd$' || return 1
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
    ceph mon set_location a datacenter=dc1 || return 1
    ceph mon set_location b datacenter=dc2 || return 1
    ceph mon set_location c datacenter=arbiter || return 1

    ceph osd crush add-bucket dc1 datacenter || return 1
    ceph osd crush add-bucket dc2 datacenter || return 1
    ceph osd crush move dc1 root=default || return 1
    ceph osd crush move dc2 root=default || return 1

    ceph osd pool create data0 --num_zones 2 --replica 2 2>&1 | grep "Error EINVAL: zone dc1 has only 0 items of type host" || return 1

    ceph osd crush add-bucket host1 host || return 1
    ceph osd crush move host1 datacenter=dc1 || return 1
    ceph osd pool create data0 --num_zones 2 --replica 2 2>&1 | grep "Error EINVAL: zone dc1 has only 1 items of type host" || return 1

    ceph osd crush add-bucket host2 host || return 1
    ceph osd crush move host2 datacenter=dc1 || return 1
    ceph osd pool create data0 --num_zones 2 --replica 2 2>&1 | grep "Error EINVAL: zone dc1 does not have 2 hosts with at least one OSD" || return 1

    # The zone weights are checked before the rule is built, so the OSDs
    # join with weight 0 until both zones have them.
    ceph osd crush set osd.0 0 host=host1 || return 1
    ceph osd pool create data0 --num_zones 2 --replica 2 2>&1 | grep "Error EINVAL: zone dc1 does not have 2 hosts with at least one OSD" || return 1

    ceph osd crush set osd.1 0 host=host2 || return 1
    ceph osd pool create data0 --num_zones 2 --replica 2 2>&1 | grep "Error EINVAL: zone dc2 has only 0 items of type host" || return 1

    ceph osd crush add-bucket host3 host || return 1
    ceph osd crush move host3 datacenter=dc2 || return 1
    ceph osd pool create data0 --num_zones 2 --replica 2 2>&1 | grep "Error EINVAL: zone dc2 has only 1 items of type host" || return 1

    ceph osd crush add-bucket host4 host || return 1
    ceph osd crush move host4 datacenter=dc2 || return 1
    ceph osd pool create data0 --num_zones 2 --replica 2 2>&1 | grep "Error EINVAL: zone dc2 does not have 2 hosts with at least one OSD" || return 1


    ceph osd crush set osd.2 0 host=host3 || return 1
    ceph osd pool create data0 --num_zones 2 --replica 2 2>&1 | grep "Error EINVAL: zone dc2 does not have 2 hosts with at least one OSD" || return 1

    ceph osd crush set osd.3 0 host=host4 || return 1
    for osd in 0 1 2 3; do
        ceph osd crush reweight osd.$osd 1.0 || return 1
    done

    ceph osd pool create data0 --num_zones 2 --replica 2 || return 1
    # replica (2) copies in each of the 2 zones
    test "$(ceph osd pool get data0 size -f json | jq .size)" = 4 || return 1
    test "$(ceph osd pool get data0 min_size -f json | jq .min_size)" = 1 || return 1

    ceph osd crush rule dump data0 | jq '.steps[1].op' | grep "choose_firstn" || return 1
    ceph osd crush rule dump data0 | jq '.steps[1].num' | grep "2" || return 1
    ceph osd crush rule dump data0 | jq '.steps[1].type' | grep "datacenter" || return 1

    ceph osd crush rule dump data0 | jq '.steps[2].op' | grep "chooseleaf_firstn" || return 1
    ceph osd crush rule dump data0 | jq '.steps[2].num' | grep "2" || return 1
    ceph osd crush rule dump data0 | jq '.steps[2].type' | grep "host" || return 1
}

# Test osd pool create for stretch replica fails for device classes with
# insufficient hosts
function TEST_pool_create_stretch_replica_with_class() {
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

    ceph osd crush add-bucket dc1 datacenter || return 1
    ceph osd crush add-bucket dc2 datacenter || return 1
    ceph osd crush add-bucket host1 host || return 1
    ceph osd crush add-bucket host2 host || return 1
    ceph osd crush add-bucket host3 host || return 1
    ceph osd crush add-bucket host4 host || return 1
    ceph osd crush add-bucket host5 host || return 1
    ceph osd crush add-bucket host6 host || return 1
    ceph osd crush move dc1 root=default || return 1
    ceph osd crush move dc2 root=default || return 1
    ceph osd crush move host1 datacenter=dc1 || return 1
    ceph osd crush move host2 datacenter=dc1 || return 1
    ceph osd crush move host3 datacenter=dc2 || return 1
    ceph osd crush move host4 datacenter=dc2 || return 1
    ceph osd crush move host5 datacenter=dc1 || return 1
    ceph osd crush move host6 datacenter=dc2 || return 1
    ceph osd crush set osd.0 1.0 host=host1 || return 1
    ceph osd crush set osd.1 1.0 host=host2 || return 1
    ceph osd crush set osd.2 1.0 host=host3 || return 1
    ceph osd crush set osd.3 1.0 host=host4 || return 1
    ceph osd crush set osd.4 1.0 host=host5 || return 1
    ceph osd crush set osd.5 1.0 host=host6 || return 1

    ceph osd crush rm-device-class osd.0 osd.1 osd.2 osd.3 osd.4 osd.5 || return 1
    ceph osd crush set-device-class ssd osd.0 osd.1 osd.2 osd.3 || return 1
    ceph osd crush set-device-class hdd osd.4 osd.5 || return 1

    ceph mon set_location a datacenter=dc1 || return 1
    ceph mon set_location b datacenter=dc2 || return 1
    ceph mon set_location c datacenter=arbiter || return 1

    ceph osd pool create stretch_ssd --num_zones 2 --replica 2 --class ssd || return 1
    ceph osd pool create stretch_hdd --num_zones 2 --replica 2 --class hdd 2>&1 | grep "Error EINVAL: zone dc1~hdd does not have 2 hosts with at least one OSD (osd_pool_default_zone_failure_domain, osd_pool_default_osd_failure_domain, osd_pool_default_root)" || return 1
    ceph osd crush add-bucket host7 host || return 1
    ceph osd crush add-bucket host8 host || return 1
    ceph osd crush move host7 datacenter=dc1 || return 1
    ceph osd crush move host8 datacenter=dc2 || return 1

    ceph osd crush rm-device-class osd.6 osd.7 || return 1
    ceph osd crush set-device-class hdd osd.6 osd.7 || return 1

    ceph osd crush set osd.6 1.0 host=host7 || return 1
    ceph osd crush set osd.7 1.0 host=host8 || return 1

    ceph osd pool create stretch_hdd --num_zones 2 --replica 2 --class hdd || return 1
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

    ceph osd erasure-code-profile set stretch_ec_ssd plugin=jerasure k=2 m=1 crush-num-osd-failure-domains=2 crush-device-class=ssd || return 1
    ceph osd erasure-code-profile set stretch_ec_hdd plugin=jerasure k=2 m=1 crush-num-osd-failure-domains=2 crush-device-class=hdd || return 1

    ceph osd crush add-bucket dc1 datacenter || return 1
    ceph osd crush add-bucket dc2 datacenter || return 1
    ceph osd crush add-bucket host1 host || return 1
    ceph osd crush add-bucket host2 host || return 1
    ceph osd crush add-bucket host3 host || return 1
    ceph osd crush add-bucket host4 host || return 1
    ceph osd crush add-bucket host5 host || return 1
    ceph osd crush add-bucket host6 host || return 1
    ceph osd crush add-bucket host7 host || return 1
    ceph osd crush add-bucket host8 host || return 1
    ceph osd crush add-bucket host9 host || return 1
    ceph osd crush add-bucket host10 host || return 1

    ceph osd crush move dc1 root=default || return 1
    ceph osd crush move dc2 root=default || return 1
    ceph osd crush move host1 datacenter=dc1 || return 1
    ceph osd crush move host2 datacenter=dc1 || return 1
    ceph osd crush move host3 datacenter=dc1 || return 1
    ceph osd crush move host4 datacenter=dc2 || return 1
    ceph osd crush move host5 datacenter=dc2 || return 1
    ceph osd crush move host6 datacenter=dc2 || return 1
    ceph osd crush move host7 datacenter=dc1 || return 1
    ceph osd crush move host8 datacenter=dc1 || return 1
    ceph osd crush move host9 datacenter=dc2 || return 1
    ceph osd crush move host10 datacenter=dc2 || return 1

    ceph mon set_location a datacenter=dc1 || return 1
    ceph mon set_location b datacenter=dc2 || return 1
    ceph mon set_location c datacenter=arbiter || return 1

    ceph osd crush set osd.0 1.0 host=host1 || return 1
    ceph osd crush set osd.1 1.0 host=host2 || return 1
    ceph osd crush set osd.2 1.0 host=host3 || return 1
    ceph osd crush set osd.3 1.0 host=host4 || return 1
    ceph osd crush set osd.4 1.0 host=host5 || return 1
    ceph osd crush set osd.5 1.0 host=host6 || return 1
    ceph osd crush set osd.6 1.0 host=host7 || return 1
    ceph osd crush set osd.7 1.0 host=host8 || return 1
    ceph osd crush set osd.8 1.0 host=host9 || return 1
    ceph osd crush set osd.9 1.0 host=host10 || return 1

    ceph osd crush rm-device-class osd.0 osd.1 osd.2 osd.3 osd.4 osd.5 osd.6 osd.7 osd.8 osd.9 || return 1
    ceph osd crush set-device-class ssd osd.0 osd.1 osd.2 osd.3 osd.4 osd.5 || return 1
    ceph osd crush set-device-class hdd osd.6 osd.7 osd.8 osd.9 || return 1

    ceph osd crush rule create-erasure stretch_ssd stretch_ec_ssd --num_zones 2 || return 1

    ceph osd crush rule create-erasure stretch_hdd stretch_ec_hdd --num_zones 2 2>&1 | grep "Error EINVAL: zone dc1~hdd does not have 3 hosts with at least one OSD" || return 1

    ceph osd crush add-bucket host11 host || return 1
    ceph osd crush add-bucket host12 host || return 1
    ceph osd crush move host11 datacenter=dc1 || return 1
    ceph osd crush move host12 datacenter=dc2 || return 1

    ceph osd crush rm-device-class osd.10 osd.11 || return 1
    ceph osd crush set-device-class hdd osd.10 osd.11 || return 1

    ceph osd crush set osd.10 1.0 host=host11 || return 1
    ceph osd crush set osd.11 1.0 host=host12 || return 1

    ceph osd crush rule create-erasure stretch_hdd stretch_ec_hdd --num_zones 2 || return 1
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

    ceph osd crush add-bucket dc1 datacenter || return 1
    ceph osd crush add-bucket dc2 datacenter || return 1
    ceph osd crush add-bucket host1 host || return 1
    ceph osd crush add-bucket host2 host || return 1
    ceph osd crush add-bucket host3 host || return 1
    ceph osd crush add-bucket host4 host || return 1
    ceph osd crush add-bucket host5 host || return 1
    ceph osd crush add-bucket host6 host || return 1

    ceph osd crush move dc1 root=default || return 1
    ceph osd crush move dc2 root=default || return 1
    ceph osd crush move host1 datacenter=dc1 || return 1
    ceph osd crush move host2 datacenter=dc1 || return 1
    ceph osd crush move host3 datacenter=dc2 || return 1
    ceph osd crush move host4 datacenter=dc2 || return 1
    ceph osd crush move host5 datacenter=dc1 || return 1
    ceph osd crush move host6 datacenter=dc2 || return 1

    ceph osd crush set osd.0 1.0 host=host1 || return 1
    ceph osd crush set osd.1 1.0 host=host2 || return 1
    ceph osd crush set osd.2 1.0 host=host3 || return 1
    ceph osd crush set osd.3 1.0 host=host4 || return 1
    ceph osd crush set osd.4 1.0 host=host5 || return 1
    ceph osd crush set osd.5 1.0 host=host6 || return 1

    ceph osd crush rm-device-class osd.0 osd.1 osd.2 osd.3 osd.4 osd.5 || return 1
    ceph osd crush set-device-class ssd osd.0 osd.1 osd.2 osd.3 || return 1
    ceph osd crush set-device-class hdd osd.4 osd.5 || return 1

    ceph osd pool create data0 || return 1
    ceph osd pool create data1 || return 1

    add_stretch_rule $dir stretch_ssd default datacenter 2 host 2 ssd || return 1
    ceph mon set election_strategy connectivity || return 1

    ceph mon set_location a datacenter=dc1 || return 1
    ceph mon set_location b datacenter=dc2 || return 1
    ceph mon set_location c datacenter=arbiter || return 1

    ceph mon enable_stretch_mode c stretch_ssd datacenter || return 1

    # create pool with crush rule with different sites
    ceph osd crush add-bucket dc3 datacenter || return 1

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

    # pool create checks the cluster for stretch mode before the rule
    ceph osd pool create data2 --rule bad_rule --num_zones 2 2>&1 | grep "there are 3datacenter's in the cluster but stretch mode currently only works with 2" || return 1

    # the same check when a pool is set to two zones
    ceph osd pool create data3 --num_zones 1 || return 1
    ceph osd pool set data3 num_zones 2 --crush_rule bad_rule 2>&1 | grep "CRUSH rule 4 uses different datacenter buckets than configured for stretch mode" || return 1
    test "$(ceph osd pool get data3 num_zones -f json | jq .num_zones)" = 1 || return 1
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


    ceph osd crush add-bucket dc1 datacenter || return 1
    ceph osd crush add-bucket dc2 datacenter || return 1
    ceph osd crush add-bucket host1 host || return 1
    ceph osd crush add-bucket host2 host || return 1
    ceph osd crush add-bucket host3 host || return 1
    ceph osd crush add-bucket host4 host || return 1
    ceph osd crush add-bucket host5 host || return 1
    ceph osd crush add-bucket host6 host || return 1
    ceph osd crush add-bucket host7 host || return 1
    ceph osd crush add-bucket host8 host || return 1

    ceph osd crush move dc1 root=default || return 1
    ceph osd crush move dc2 root=default || return 1
    ceph osd crush move host1 datacenter=dc1 || return 1
    ceph osd crush move host2 datacenter=dc1 || return 1
    ceph osd crush move host3 datacenter=dc2 || return 1
    ceph osd crush move host4 datacenter=dc2 || return 1
    ceph osd crush move host5 datacenter=dc1 || return 1
    ceph osd crush move host6 datacenter=dc2 || return 1
    ceph osd crush move host7 datacenter=dc1 || return 1
    ceph osd crush move host8 datacenter=dc2 || return 1

    ceph osd crush set osd.0 1.0 host=host1 || return 1
    ceph osd crush set osd.1 1.0 host=host2 || return 1
    ceph osd crush set osd.2 1.0 host=host3 || return 1
    ceph osd crush set osd.3 1.0 host=host4 || return 1


    ceph osd crush rm-device-class osd.0 osd.1 osd.2 osd.3 osd.4 osd.5 osd.6 osd.7 || return 1
    ceph osd crush set-device-class ssd osd.0 osd.1 osd.2 osd.3 || return 1
    ceph osd crush set-device-class hdd osd.4 osd.5 osd.6 osd.7 || return 1

    ceph osd crush set osd.4 1.0 host=host5 || return 1
    ceph osd crush set osd.5 1.0 host=host6 || return 1
    ceph osd crush set osd.6 1.0 host=host7 || return 1
    ceph osd crush set osd.7 1.0 host=host8 || return 1

    ceph mon set_location a datacenter=dc1 || return 1
    ceph mon set_location b datacenter=dc2 || return 1
    ceph mon set_location c datacenter=arbiter || return 1

    # Create pools using different device classes
    add_stretch_rule $dir stretch_ssd default datacenter 2 host 2 ssd || return 1

    add_stretch_rule $dir stretch_hdd default datacenter 2 host 2 hdd || return 1

    ceph osd pool create pool_ssd || return 1

    ceph mon set election_strategy connectivity || return 1

    ceph mon enable_stretch_mode c stretch_ssd datacenter || return 1

    ceph osd pool create data0 replicated --rule stretch_ssd --num_zones 2 || return 1

    ceph osd pool create pool_hdd replicated --rule stretch_hdd --num_zones 2 || return 1

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
    # wait_for_clean reads the PGs from the manager
    run_mgr $dir x || return 1
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

    ceph osd crush add-bucket dc1 datacenter || return 1
    ceph osd crush add-bucket dc2 datacenter || return 1
    ceph osd crush add-bucket host1 host || return 1
    ceph osd crush add-bucket host2 host || return 1
    ceph osd crush add-bucket host3 host || return 1
    ceph osd crush add-bucket host4 host || return 1
    ceph osd crush add-bucket host5 host || return 1
    ceph osd crush add-bucket host6 host || return 1
    ceph osd crush add-bucket host7 host || return 1
    ceph osd crush add-bucket host8 host || return 1
    ceph osd crush add-bucket host9 host || return 1
    ceph osd crush add-bucket host10 host || return 1
    ceph osd crush add-bucket host11 host || return 1
    ceph osd crush add-bucket host12 host || return 1

    ceph osd crush move dc1 root=default || return 1
    ceph osd crush move dc2 root=default || return 1
    ceph osd crush move host1 datacenter=dc1 || return 1
    ceph osd crush move host2 datacenter=dc1 || return 1
    ceph osd crush move host3 datacenter=dc1 || return 1
    ceph osd crush move host4 datacenter=dc1 || return 1
    ceph osd crush move host5 datacenter=dc1 || return 1
    ceph osd crush move host6 datacenter=dc1 || return 1
    ceph osd crush move host7 datacenter=dc2 || return 1
    ceph osd crush move host8 datacenter=dc2 || return 1
    ceph osd crush move host9 datacenter=dc2 || return 1
    ceph osd crush move host10 datacenter=dc2 || return 1
    ceph osd crush move host11 datacenter=dc2 || return 1
    ceph osd crush move host12 datacenter=dc2 || return 1

    ceph osd crush set osd.0 1.0 host=host1 || return 1
    ceph osd crush set osd.1 1.0 host=host2 || return 1
    ceph osd crush set osd.2 1.0 host=host3 || return 1
    ceph osd crush set osd.3 1.0 host=host4 || return 1
    ceph osd crush set osd.4 1.0 host=host5 || return 1
    ceph osd crush set osd.5 1.0 host=host6 || return 1
    ceph osd crush set osd.6 1.0 host=host7 || return 1
    ceph osd crush set osd.7 1.0 host=host8 || return 1
    ceph osd crush set osd.8 1.0 host=host9 || return 1
    ceph osd crush set osd.9 1.0 host=host10 || return 1
    ceph osd crush set osd.10 1.0 host=host11 || return 1
    ceph osd crush set osd.11 1.0 host=host12 || return 1

    ceph mon set_location a datacenter=dc4 || return 1
    ceph mon set_location b datacenter=dc1 || return 1
    ceph mon set_location c datacenter=dc2 || return 1

    ceph mon set election_strategy connectivity || return 1

    ceph osd erasure-code-profile set stretch_ec_profile plugin=jerasure k=4 m=2 crush-num-osd-failure-domains=2 || return 1

    ceph osd crush rule create-erasure stretch_ec stretch_ec_profile 2 || return 1

    ceph osd pool create data0 erasure --erasure_code_profile=stretch_ec_profile || return 1
    ceph osd pool create data1 erasure --erasure_code_profile=stretch_ec_profile || return 1

    ceph osd pool set data0 allow_ec_optimizations true || return 1
    ceph osd pool set data1 allow_ec_optimizations true || return 1

    # an erasure coded pool is stretched with num_zones, not stretch set
    ceph osd pool stretch set data0 2 2 datacenter stretch_ec 12 8 2>&1 |
        grep "not supported for EC pools" || return 1
    ceph osd pool set data0 num_zones 2 || return 1
    ceph osd pool set data1 num_zones 2 || return 1
    for pool in data0 data1; do
        test "$(ceph osd pool get $pool size | awk '{print $2}')" = 12 || return 1
    done
    ceph osd dump | grep "^stretch_mode_enabled true" || return 1
    wait_for_clean || return 1
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

    ceph osd crush add-bucket z1 zone || return 1
    ceph osd crush add-bucket z2 zone || return 1
    ceph osd crush add-bucket host1 host || return 1
    ceph osd crush add-bucket host2 host || return 1
    ceph osd crush add-bucket host3 host || return 1
    ceph osd crush add-bucket host4 host || return 1
    ceph osd crush add-bucket host5 host || return 1
    ceph osd crush add-bucket host6 host || return 1
    ceph osd crush add-bucket host7 host || return 1
    ceph osd crush add-bucket host8 host || return 1

    ceph osd crush move z1 root=default || return 1
    ceph osd crush move z2 root=default || return 1

    ceph osd crush move host1 zone=z1 || return 1
    ceph osd crush move host2 zone=z1 || return 1
    ceph osd crush move host3 zone=z2 || return 1
    ceph osd crush move host4 zone=z2 || return 1
    ceph osd crush move host5 zone=z1 || return 1
    ceph osd crush move host6 zone=z2 || return 1
    ceph osd crush move host7 zone=z1 || return 1
    ceph osd crush move host8 zone=z2 || return 1

    ceph osd crush set osd.0 1.0 host=host1 || return 1
    ceph osd crush set osd.1 1.0 host=host2 || return 1
    ceph osd crush set osd.2 1.0 host=host3 || return 1
    ceph osd crush set osd.3 1.0 host=host4 || return 1
    ceph osd crush set osd.4 1.0 host=host5 || return 1
    ceph osd crush set osd.5 1.0 host=host6 || return 1
    ceph osd crush set osd.6 1.0 host=host7 || return 1
    ceph osd crush set osd.7 1.0 host=host8 || return 1


    ceph osd crush rm-device-class osd.0 osd.1 osd.2 osd.3 osd.4 osd.5 osd.6 osd.7 || return 1
    ceph osd crush set-device-class ssd osd.0 osd.1 osd.2 osd.3 osd.4 osd.5 osd.6 osd.7 || return 1

    ceph mon set_location a zone=z1 || return 1
    ceph mon set_location b zone=z2 || return 1
    ceph mon set_location c zone=arbiter || return 1


    add_stretch_rule $dir stretch_zone default zone 2 host 2 ssd || return 1

    ceph osd pool create pool_zone || return 1

    ceph mon set election_strategy connectivity || return 1

    ceph mon enable_stretch_mode c stretch_zone zone || return 1
    
    # Wait for stretch mode to be fully committed before testing validation
    ceph mon dump | grep "stretch_mode_enabled 1" || return 1

    # the stretch checks come before the rule is built
    ceph osd pool create pool_dc replicated --zone_failure_domain=datacenter --num_zones 2 --class=ssd 2>&1 | grep "there are 0datacenter's in the cluster but stretch mode currently only works with 2" || return 1

}

# Test that unstretching the last pool cleanly disables stretch mode globally and clears leader memory
# across all transition states: Healthy, Degraded, and Recovery.
# Wait until the output of a command matches a pattern
function wait_for_output() {
    local pattern=$1
    shift
    for ((i=0; i < 60; i++)); do
        "$@" 2>/dev/null | grep -q "$pattern" && return 0
        sleep 1
    done
    return 1
}

# Stretch mode is left in the OSDMap and the MonMap, whose change follows
function stretch_mode_left() {
    ceph osd dump | grep "^stretch_mode_enabled false" || return 1
    for ((i=0; i < 60; i++)); do
        ceph mon dump 2>/dev/null | grep -q "stretch_mode_enabled" || return 0
        sleep 1
    done
    return 1
}

function TEST_stretch_unstretch_clean() {
    local dir=$1
    run_mon $dir a --public-addr=$CEPH_MON_A || return 1
    run_mon $dir b --public-addr=$CEPH_MON_B || return 1
    run_mon $dir c --public-addr=$CEPH_MON_C || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1

    ceph mon set_location a datacenter=dc1 || return 1
    ceph mon set_location b datacenter=dc2 || return 1
    ceph mon set_location c datacenter=arbiter || return 1

    ceph osd crush add-bucket dc1 datacenter || return 1
    ceph osd crush add-bucket dc2 datacenter || return 1
    ceph osd crush move dc1 root=default || return 1
    ceph osd crush move dc2 root=default || return 1

    ceph osd crush add-bucket host1 host || return 1
    ceph osd crush move host1 datacenter=dc1 || return 1
    ceph osd crush set osd.0 1.0 host=host1 || return 1

    ceph osd crush add-bucket host2 host || return 1
    ceph osd crush move host2 datacenter=dc1 || return 1
    ceph osd crush set osd.1 1.0 host=host2 || return 1

    ceph osd crush add-bucket host3 host || return 1
    ceph osd crush move host3 datacenter=dc2 || return 1
    ceph osd crush set osd.2 1.0 host=host3 || return 1

    ceph osd crush add-bucket host4 host || return 1
    ceph osd crush move host4 datacenter=dc2 || return 1
    ceph osd crush set osd.3 1.0 host=host4 || return 1

    # ==========================================
    # 1. Test Unstretching from Healthy Stretch Mode
    # ==========================================
    ceph osd pool create data_healthy --num_zones 2 --replica 2 || return 1

    # healthy stretch mode
    ceph osd dump | grep "^stretch_mode_enabled true" || return 1
    ceph osd dump | grep "^degraded_stretch_mode 0" || return 1

    # Unstretch the pool
    ceph osd pool set data_healthy num_zones 1 || return 1
    stretch_mode_left || return 1

    # ==========================================
    # 2. Test Unstretching from Degraded Stretch Mode
    # ==========================================
    ceph osd pool create data_degraded --num_zones 2 --replica 2 || return 1

    # Simulate zone failure by killing monitor b and OSDs in dc2
    kill_daemons $dir TERM mon.b
    kill_daemons $dir TERM osd.2
    kill_daemons $dir TERM osd.3
    ceph osd down 2 3 || return 1

    wait_for_output "^degraded_stretch_mode 1" ceph osd dump || return 1

    # Unstretch the pool while in degraded stretch mode
    ceph osd pool set data_degraded num_zones 1 || return 1
    stretch_mode_left || return 1
    # the leader's degraded and recovering state went with it
    ceph osd force_healthy_stretch_mode --yes-i-really-mean-it 2>&1 | grep "the cluster is not in recovery stretch mode" || return 1

    # Restart monitor and wait for quorum of 3 monitors to form first
    activate_mon $dir b || return 1
    wait_for_quorum 60 3 || return 1

    # Now restart the OSDs in dc2
    activate_osd $dir 2 || return 1
    activate_osd $dir 3 || return 1
    ceph osd crush set osd.2 1.0 host=host3 || return 1
    ceph osd crush set osd.3 1.0 host=host4 || return 1
    sleep 5
    ceph osd tree || return 1
    # ==========================================
    # 3. Test Unstretching from Recovery Stretch Mode
    # ==========================================
    ceph osd pool create data_recovery --num_zones 2 --replica 2 || return 1

    # Recovery stretch mode is entered from degraded stretch mode, and only
    # with a monitor in quorum in every zone: lose dc2, then bring back
    # mon.b but not the OSDs
    kill_daemons $dir TERM mon.b
    kill_daemons $dir TERM osd.2
    kill_daemons $dir TERM osd.3
    ceph osd down 2 3 || return 1
    wait_for_output "^degraded_stretch_mode 1" ceph osd dump || return 1
    activate_mon $dir b || return 1
    wait_for_quorum 60 3 || return 1

    ceph osd force_recovery_stretch_mode --yes-i-really-mean-it || return 1
    wait_for_output "^recovering_stretch_mode 1" ceph osd dump || return 1

    # Unstretch the pool while in recovery stretch mode
    ceph osd pool set data_recovery num_zones 1 || return 1
    stretch_mode_left || return 1
    ceph osd force_healthy_stretch_mode --yes-i-really-mean-it 2>&1 | grep "the cluster is not in recovery stretch mode" || return 1

    # Restore dc2 OSDs so teardown can clean up normally
    activate_osd $dir 2 || return 1
    activate_osd $dir 3 || return 1
}

main crush-stretch "$@"
