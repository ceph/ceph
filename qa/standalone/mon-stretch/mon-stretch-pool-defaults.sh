#!/usr/bin/env bash
#
# ceph osd pool default set and get, and how ceph osd pool create uses the
# defaults they manage.

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7181" # git grep '\<7181\>' : there must be only one
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

function default_field() {
    ceph osd pool default get -f json | jq -r ".$1.$2"
}

function pool_field() {
    ceph osd pool ls detail -f json | jq --arg p $1 ".[]|select(.pool_name==\$p)|.$2"
}

# Test get shows every default with its value, option and source
function TEST_get_shows_every_default() {
    local dir=$1
    run_mon $dir a || return 1

    ceph osd pool default get || return 1
    local param
    for param in pool_type num_zones rule zone_failure_domain \
                 osd_failure_domain root class replica min_size \
                 erasure_code_profile k m pg_num pgp_num autoscale_mode \
                 bulk crimson ; do
        test "$(ceph osd pool default get -f json | jq "has(\"$param\")")" = true || return 1
    done
    test "$(default_field pool_type value)" = replicated || return 1
    test "$(default_field num_zones value)" = 1 || return 1
    test "$(default_field num_zones source)" = default || return 1
    test "$(default_field rule value)" = none || return 1
    test "$(default_field zone_failure_domain value)" = datacenter || return 1
    test "$(default_field osd_failure_domain value)" = host || return 1
    test "$(default_field root value)" = default || return 1
    # run_mon sets these on the command line
    test "$(default_field autoscale_mode value)" = off || return 1
    test "$(default_field autoscale_mode source)" = cmdline || return 1
}

# Test set writes the global section and get shows where values come from
function TEST_set_writes_global_values() {
    local dir=$1
    run_mon $dir a || return 1

    ceph osd pool default set --replica 2 --min_size 1 --pg_num 16 || return 1
    test "$(ceph config get mon osd_pool_default_replica)" = 2 || return 1
    test "$(default_field replica value)" = 2 || return 1
    test "$(default_field replica option)" = osd_pool_default_replica || return 1
    test "$(default_field replica source)" = global || return 1
    test "$(default_field min_size value)" = 1 || return 1
    test "$(default_field pg_num value)" = 16 || return 1
    # parameters that are not given are left alone
    ceph osd pool default set --pg_num 32 || return 1
    test "$(default_field replica value)" = 2 || return 1
    ceph osd pool default set --bulk || return 1
    test "$(default_field bulk value)" = true || return 1
    ceph osd pool default set --bulk=false || return 1
    test "$(default_field bulk value)" = false || return 1
}

# Test the legacy --size sets the replicas per zone
function TEST_set_legacy_size() {
    local dir=$1
    run_mon $dir a || return 1

    ceph osd pool default set --size 4 || return 1
    test "$(ceph config get mon osd_pool_default_replica)" = 4 || return 1
    expect_failure $dir "cannot specify both 'size' and 'replica'" \
        ceph osd pool default set --size 3 --replica 3 || return 1
    # the legacy size gives the copies of a single-zone pool
    ceph osd pool default set --size 3 --num_zones 1 || return 1
    expect_failure $dir "cannot specify 'size' with num_zones > 1" \
        ceph osd pool default set --size 3 --num_zones 2 || return 1
}

# Test replica 0 shows the legacy osd_pool_default_size as its option
function TEST_get_replica_from_legacy_size() {
    local dir=$1
    run_mon $dir a || return 1

    ceph config set global osd_pool_default_size 2 || return 1
    test "$(default_field replica value)" = 2 || return 1
    test "$(default_field replica option)" = osd_pool_default_size || return 1
}

# Test the checks that ceph osd pool create would make
function TEST_set_checks() {
    local dir=$1
    run_mon $dir a || return 1

    expect_failure $dir "is not a CRUSH bucket type" \
        ceph osd pool default set --zone_failure_domain nosuchtype || return 1
    expect_failure $dir "is not a CRUSH bucket type" \
        ceph osd pool default set --osd_failure_domain nosuchtype || return 1
    expect_failure $dir "does not exist" \
        ceph osd pool default set --root nosuchroot || return 1
    expect_failure $dir "does not exist" \
        ceph osd pool default set --class nosuchclass || return 1
    expect_failure $dir "does not exist" \
        ceph osd pool default set --rule nosuchrule || return 1
    expect_failure $dir "cannot specify both crush rule" \
        ceph osd pool default set --rule replicated_rule --root default || return 1
    expect_failure $dir "pool min_size must be between 1 and replica, which is set to 2" \
        ceph osd pool default set --replica 2 --min_size 3 || return 1
    expect_failure $dir "'pgp_num' must be greater than 0 and lower or equal than 'pg_num'" \
        ceph osd pool default set --pg_num 8 --pgp_num 16 || return 1
    expect_failure $dir "data loss" \
        ceph osd pool default set --replica 1 || return 1
    ceph osd pool default set --replica 1 --yes-i-really-mean-it || return 1
    expect_failure $dir "set-allow-crimson" \
        ceph osd pool default set --crimson || return 1
    # nothing was written by the failures
    test "$(default_field zone_failure_domain value)" = datacenter || return 1
    test "$(default_field rule value)" = none || return 1
}

# Test num_zones 2 needs a cluster that can be stretched
function TEST_set_num_zones_needs_stretchable_cluster() {
    local dir=$1
    run_mon $dir a || return 1

    expect_failure $dir "Failed to validate monitor stretch mode" \
        ceph osd pool default set --num_zones 2 --zone_failure_domain zone || return 1
    test "$(default_field num_zones value)" = 1 || return 1
}

# Test the rule default is written as its id, and none returns to generated rules
function TEST_set_rule() {
    local dir=$1
    run_mon $dir a || return 1

    ceph osd pool default set --rule replicated_rule || return 1
    test "$(ceph config get mon osd_pool_default_crush_rule)" = 0 || return 1
    test "$(default_field rule value)" = replicated_rule || return 1
    ceph osd pool default set --rule none || return 1
    test "$(ceph config get mon osd_pool_default_crush_rule)" = -1 || return 1
}

# Test k, m and a profile all write the default erasure code profile
function TEST_set_erasure_code_profile() {
    local dir=$1
    run_mon $dir a || return 1

    ceph osd pool default set --k 4 --m 3 || return 1
    test "$(default_field k value)" = 4 || return 1
    test "$(default_field m value)" = 3 || return 1
    ceph config get mon osd_pool_default_erasure_code_profile | grep -q 'k=4' || return 1

    ceph osd erasure-code-profile set myprofile plugin=isa k=3 m=2 || return 1
    ceph osd pool default set --erasure_code_profile myprofile || return 1
    test "$(default_field k value)" = 3 || return 1
    test "$(default_field m value)" = 2 || return 1
    # a copy, so the default stays when the profile goes
    ceph osd erasure-code-profile rm myprofile || return 1
    test "$(default_field k value)" = 3 || return 1

    expect_failure $dir "cannot specify both erasure_code_profile and k/m parameters" \
        ceph osd pool default set --erasure_code_profile default --k 2 || return 1
    expect_failure $dir "does not exist" \
        ceph osd pool default set --erasure_code_profile nosuchprofile || return 1
}

# Test values that another source overrides are refused
function TEST_set_refuses_overridden_values() {
    local dir=$1
    run_mon $dir a || return 1

    # run_mon sets this one on the command line
    expect_failure $dir "local configuration of mon.a" \
        ceph osd pool default set --autoscale_mode on || return 1
    ceph config set mon osd_pool_default_replica 3 || return 1
    expect_failure $dir "section mon of the configuration database" \
        ceph osd pool default set --replica 2 || return 1
    ceph config rm mon osd_pool_default_replica || return 1
    ceph osd pool default set --replica 2 || return 1
}

# Test a later override raises POOL_DEFAULT_OVERRIDDEN
function TEST_health_warns_of_override() {
    local dir=$1
    run_mon $dir a || return 1

    ceph osd pool default set --replica 2 || return 1
    ceph health detail | grep -q POOL_DEFAULT_OVERRIDDEN && return 1
    ceph config set mon osd_pool_default_replica 3 || return 1
    local i
    for i in $(seq 1 30); do
        ceph health detail | grep -q POOL_DEFAULT_OVERRIDDEN && break
        sleep 1
    done
    ceph health detail | grep "osd_pool_default_replica is set in section mon" || return 1
    ceph config rm mon osd_pool_default_replica || return 1
    for i in $(seq 1 30); do
        ceph health detail | grep -q POOL_DEFAULT_OVERRIDDEN || return 0
        sleep 1
    done
    return 1
}

# Test pool create takes the defaults
function TEST_pool_create_uses_defaults() {
    local dir=$1
    run_mon $dir a || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1

    ceph osd pool default set --replica 2 --pg_num 4 || return 1
    ceph osd pool create rep || return 1
    test "$(pool_field rep size)" = 2 || return 1
    test "$(pool_field rep pg_num)" = 4 || return 1
    test "$(ceph osd pool get rep num_zones -f json | jq .num_zones)" = 1 || return 1
    # the legacy --size still gives the size of a single-zone pool
    ceph osd pool create rep3 --size 3 || return 1
    test "$(pool_field rep3 size)" = 3 || return 1
}

main mon-stretch-pool-defaults "$@"

# Local Variables:
# compile-command: "cd ../../../build ; make -j4 && ../qa/run-standalone.sh mon-stretch-pool-defaults.sh"
# End:
