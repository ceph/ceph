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

# osd pool create --num_zones 2 builds the pool's rule from --root,
# --zone_failure_domain and --osd_failure_domain, and takes any it is not
# given from osd_pool_default_root, osd_pool_default_zone_failure_domain and
# osd_pool_default_osd_failure_domain. This test sets these options to
# other (a second root, holding rooms room1 and room2), room and osd, and
# creates replicated pool from_options with only --num_zones 2 and
# --replica 2. Its rule, from_options, takes other and has steps of type
# room and osd.
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

# osd crush rule create-erasure <rule> <profile> --num_zones 2 makes an
# erasure rule over two zones, of type datacenter by default, that places
# k+m shards in each zone, each on a different host. It is refused until
# each zone has k+m hosts with at least one OSD. This test uses profile
# stretch_ec_profile (k=2 m=1) and datacenters dc1 and dc2, which start
# empty. It adds three hosts to dc1, then an OSD to each of them, then does
# the same for dc2, and runs the command after each step: it is refused,
# naming the zone and what it lacks, until both zones have 3 hosts with an
# OSD. The rule then chooses 2 datacenters, and 3 hosts in each.
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

    ceph osd erasure-code-profile set stretch_ec_profile plugin=jerasure k=2 m=1 || return 1
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

# osd pool create <pool> erasure --num_zones 2 makes an erasure coded pool
# over two zones, of type datacenter by default, on a new rule named after
# the pool. It is refused until each zone has k+m hosts with at least one
# OSD. This test creates data0 with --k 2 --m 1 while it adds three hosts
# to datacenter dc1, then an OSD to each, then does the same for dc2: each
# attempt is refused, naming the zone and what it lacks. The OSDs join with
# weight 0, so that the check of the zones' weights, which comes first,
# passes, and get weight 1 once both zones have them. data0 is then
# created, and its rule chooses 2 datacenters, and 3 hosts in each. Pool
# data_osd_failure_domain, from a profile with crush-osd-failure-domain=osd,
# gets a rule that chooses OSDs instead of hosts in each datacenter.
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

    # Set monitor locations, which pools over zones need
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

# osd pool create <pool> --num_zones 2 --replica 2 makes a replicated pool
# with 2 copies in each of two zones, of type datacenter by default, on a
# new rule named after the pool. It is refused until each zone has 2 hosts
# with at least one OSD. This test creates data0 while it adds two hosts to
# datacenter dc1, then an OSD to each, then does the same for dc2: each
# attempt is refused, naming the zone and what it lacks. The OSDs join with
# weight 0, so that the check of the zones' weights, which comes first,
# passes, and get weight 1 once both zones have them. data0 is then created
# with size 4 and min_size 1, and its rule chooses 2 datacenters, and 2
# hosts in each.
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

    # Set monitor locations, which pools over zones need
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

# osd pool create --num_zones 2 --class <class> places a pool over two zones
# on the OSDs of that device class only, so it needs enough hosts of the
# class in each zone. In this test datacenters dc1 and dc2 each have 2 hosts
# of ssd OSDs and 1 of hdd OSDs. Replicated pool stretch_ssd, with
# --replica 2 --class ssd, is created. stretch_hdd, with --class hdd, is
# refused, as dc1~hdd (the hdd OSDs of dc1) does not have 2 hosts with an
# OSD; the message names the osd_pool_default_* options that gave the rest
# of the placement. With a second host of hdd OSDs added to each
# datacenter, stretch_hdd is created.
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

# osd crush rule create-erasure <rule> <profile> --num_zones 2, with a
# profile of a device class (crush-device-class), places the shards on OSDs
# of that class only, so it needs k+m hosts of the class in each zone. In
# this test datacenters dc1 and dc2 each have 3 hosts of ssd OSDs and 2 of
# hdd OSDs, and profiles stretch_ec_ssd and stretch_ec_hdd are k=2 m=1 in
# ssd and in hdd OSDs. Rule stretch_ssd is created. stretch_hdd is refused,
# as dc1 has only 2 hosts with an hdd OSD. With a third host of hdd OSDs
# added to each datacenter, stretch_hdd is created.
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

    ceph osd erasure-code-profile set stretch_ec_ssd plugin=jerasure k=2 m=1 crush-device-class=ssd || return 1
    ceph osd erasure-code-profile set stretch_ec_hdd plugin=jerasure k=2 m=1 crush-device-class=hdd || return 1

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

# In global stretch mode a pool can go over zones only on a rule over the
# two datacenters that global stretch mode uses. This test enables it with
# mon enable_stretch_mode and rule stretch_ssd, which chooses datacenters
# dc1 and dc2, and 2 hosts of ssd OSDs in each. It then adds a datacenter
# bucket, dc3, outside the root, and bad_rule, which takes dc1 and dc3.
# osd pool create data2 --rule bad_rule --num_zones 2 is refused because of
# dc3, as global stretch mode needs exactly two datacenter buckets; this
# check comes before that of the rule. osd pool set data3 num_zones 2
# --crush_rule bad_rule is refused, as bad_rule uses datacenter buckets
# other than those of global stretch mode, and data3 stays in one zone.
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

    # pool create checks the cluster's zones before the rule
    ceph osd pool create data2 --rule bad_rule --num_zones 2 2>&1 | grep "there are 3datacenter's in the cluster but stretch mode currently only works with 2" || return 1

    # the same check when a pool is set to two zones
    ceph osd pool create data3 --num_zones 1 || return 1
    ceph osd pool set data3 num_zones 2 --crush_rule bad_rule 2>&1 | grep "CRUSH rule 4 uses different datacenter buckets than configured for stretch mode" || return 1
    test "$(ceph osd pool get data3 num_zones -f json | jq .num_zones)" = 1 || return 1
}

# In global stretch mode, pools over zones can use rules of different device
# classes over the same two datacenters. This test makes rules stretch_ssd
# and stretch_hdd, which choose datacenters dc1 and dc2, and 2 hosts in
# each, of ssd and of hdd OSDs. mon enable_stretch_mode with stretch_ssd
# moves pool pool_ssd, created before, to stretch_ssd. Pools data0, on
# stretch_ssd, and pool_hdd, on stretch_hdd, are then created with
# --num_zones 2, and pool_hdd keeps stretch_hdd.
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

# osd pool stretch set, which makes a replicated pool an individual stretch
# pool, is refused for an erasure coded pool: osd pool set num_zones 2
# places one over two zones instead. This test has 6 hosts in each of
# datacenters dc1 and dc2, and erasure coded pools data0 and data1 from
# profile stretch_ec_profile (k=4 m=2), with allow_ec_optimizations set.
# osd pool stretch set data0, with stretch_ec, a rule over 2 zones made by
# osd crush rule create-erasure, is refused. osd pool set num_zones 2 then
# gives each pool size 12, 6 shards in each zone, and sets the cluster
# stretch flags. The PGs then go clean.
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

# Global stretch mode needs exactly two buckets of the zone type of each
# pool over zones. This test enables it with mon enable_stretch_mode, zones
# z1 and z2 of bucket type zone, and rule stretch_zone (2 hosts of ssd OSDs
# in each zone), and checks that the monmap shows it. Replicated pool
# pool_dc, created with --num_zones 2 and --zone_failure_domain datacenter,
# is then refused, as there are no datacenter buckets. The refusal comes
# from the check of global stretch mode, before the pool's rule is built.
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
    
    # Wait for global stretch mode to be committed before testing validation
    ceph mon dump | grep "stretch_mode_enabled 1" || return 1

    # the stretch checks come before the rule is built
    ceph osd pool create pool_dc replicated --zone_failure_domain=datacenter --num_zones 2 --class=ssd 2>&1 | grep "there are 0datacenter's in the cluster but stretch mode currently only works with 2" || return 1

}

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

# The cluster stretch flags are cleared in the OSDMap, and in the monmap,
# whose change follows
function stretch_mode_left() {
    ceph osd dump | grep "^stretch_mode_enabled false" || return 1
    for ((i=0; i < 60; i++)); do
        ceph mon dump 2>/dev/null | grep -q "stretch_mode_enabled" || return 0
        sleep 1
    done
    return 1
}

# When the last pool over zones goes back to one zone, the cluster stretch
# flags are cleared, in the OSDMap and then in the monmap, and the leader
# drops any degraded or recovering state. This test, with datacenters dc1
# and dc2, sets num_zones 1 on a new replicated pool, created with
# --num_zones 2 --replica 2, three times: while the cluster is healthy;
# while degraded_stretch_mode is set, with the monitor and OSDs of dc2
# down; and while recovering_stretch_mode is set, after the monitor of dc2
# is back and osd force_recovery_stretch_mode is run. Each time the flags
# are cleared. After the last two, osd force_healthy_stretch_mode is
# refused, as the cluster is not recovering.
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

    # the cluster stretch flags set, not degraded or recovering
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

    # Unstretch the pool while in degraded mode
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

    # Recovery mode is entered from degraded mode, and only
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

    # Unstretch the pool while in recovery mode
    ceph osd pool set data_recovery num_zones 1 || return 1
    stretch_mode_left || return 1
    ceph osd force_healthy_stretch_mode --yes-i-really-mean-it 2>&1 | grep "the cluster is not in recovery stretch mode" || return 1

    # Restore dc2 OSDs so teardown can clean up normally
    activate_osd $dir 2 || return 1
    activate_osd $dir 3 || return 1
}

main crush-stretch "$@"
