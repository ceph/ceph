#!/usr/bin/env bash
#
# ceph mon enable_stretch_mode and disable_stretch_mode once the upgrade is
# committed: they change every pool as ceph osd pool set num_zones does and
# set the pool creation defaults.

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON_A="127.0.0.1:7182" # git grep '\<7182\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7183" # git grep '\<7183\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7184" # git grep '\<7184\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function pool_field() {
    ceph osd pool ls detail -f json | jq --arg p $1 ".[]|select(.pool_name==\$p)|.$2"
}

# the EC shards that cannot become primary, raw shards 1 to k-1 in each zone
function pool_nonprimary_shards() {
    ceph osd pool ls detail -f json | jq -r --arg p $1 ".[]|select(.pool_name==\$p)|.nonprimary_shards"
}

function pool_num_zones() {
    ceph osd pool get $1 num_zones -f json | jq .num_zones
}

function default_value() {
    ceph osd pool default get -f json | jq -r ".$1.value"
}

# zones iris (mon.a) and pze (mon.b) of two hosts each, tiebreaker mon.c,
# and a replicated rule that takes 2 hosts in each zone
function setup_zones() {
    local dir=$1

    run_mon $dir a --public-addr $CEPH_MON_A || return 1
    run_mon $dir b --public-addr $CEPH_MON_B || return 1
    run_mon $dir c --public-addr $CEPH_MON_C || return 1
    wait_for_quorum 300 3 || return 1
    local zone host
    for zone in iris pze; do
        ceph osd crush add-bucket $zone zone || return 1
        ceph osd crush move $zone root=default || return 1
    done
    for host in 2 3 4 5; do
        ceph osd crush add-bucket node-$host host || return 1
    done
    ceph osd crush move node-2 zone=iris || return 1
    ceph osd crush move node-3 zone=iris || return 1
    ceph osd crush move node-4 zone=pze || return 1
    ceph osd crush move node-5 zone=pze || return 1
    ceph mon set_location a zone=iris host=node-2 || return 1
    ceph mon set_location b zone=pze host=node-4 || return 1
    ceph mon set_location c zone=arbiter host=node-1 || return 1

    ceph osd getcrushmap > $dir/crushmap || return 1
    crushtool --decompile $dir/crushmap > $dir/crushmap.txt || return 1
    sed 's/^# end crush map$//' $dir/crushmap.txt > $dir/crushmap_modified.txt || return 1
    cat >> $dir/crushmap_modified.txt << EOF
rule stretch_rule {
        id 1
        type replicated
        step take iris
        step chooseleaf firstn 2 type host
        step emit
        step take pze
        step chooseleaf firstn 2 type host
        step emit
}

# end crush map
EOF
    crushtool --compile $dir/crushmap_modified.txt -o $dir/crushmap.bin || return 1
    ceph osd setcrushmap -i $dir/crushmap.bin || return 1
}

# three OSDs in each zone
function setup_osds() {
    local dir=$1

    run_mgr $dir x || return 1
    local osd
    for osd in 0 1 2 3 4 5; do
        run_osd $dir $osd || return 1
    done
    ceph osd crush move osd.0 host=node-2 || return 1
    ceph osd crush move osd.1 host=node-2 || return 1
    ceph osd crush move osd.2 host=node-3 || return 1
    ceph osd crush move osd.3 host=node-4 || return 1
    ceph osd crush move osd.4 host=node-4 || return 1
    ceph osd crush move osd.5 host=node-5 || return 1
    ceph osd crush remove $(hostname -s) || return 1
}

function create_pools() {
    ceph osd pool create rep 8 8 replicated || return 1
    ceph osd erasure-code-profile set ec21 plugin=isa k=2 m=1 \
        crush-failure-domain=osd || return 1
    ceph osd pool create ec 8 8 erasure ec21 || return 1
    ceph osd pool set ec allow_ec_optimizations true || return 1
}

# Test enabling stretches every pool and sets the stretch defaults
function TEST_enable_stretches_every_pool() {
    local dir=$1
    setup_zones $dir || return 1
    setup_osds $dir || return 1
    create_pools || return 1
    test "$(pool_nonprimary_shards ec)" = "{1}" || return 1
    wait_for_clean || return 1

    ceph mon enable_stretch_mode c stretch_rule zone || return 1

    test "$(pool_num_zones rep)" = 2 || return 1
    test "$(pool_field rep size)" = 4 || return 1
    test "$(pool_field rep crush_rule)" = 1 || return 1
    test "$(pool_field rep peering_crush_bucket_count)" = 2 || return 1
    test "$(pool_num_zones ec)" = 2 || return 1
    test "$(pool_field ec size)" = 6 || return 1
    test "$(pool_nonprimary_shards ec)" = "{1,4}" || return 1
    ceph osd crush rule ls | grep -q '^ec-stretch$' || return 1
    test "$(default_value num_zones)" = 2 || return 1
    test "$(default_value replica)" = 2 || return 1
    test "$(default_value zone_failure_domain)" = zone || return 1
    test "$(ceph mon dump -f json | jq .stretch_mode)" = true || return 1
    test "$(ceph mon dump -f json | jq -r .tiebreaker_mon)" = c || return 1
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = true || return 1
    wait_for_clean || return 1

    # it can be run again
    ceph mon enable_stretch_mode c stretch_rule zone || return 1
    test "$(pool_field rep size)" = 4 || return 1

    # a new pool takes the stretch defaults
    ceph osd pool create newrep 8 8 || return 1
    test "$(pool_num_zones newrep)" = 2 || return 1
    test "$(pool_field newrep size)" = 4 || return 1
    test "$(pool_field newrep crush_rule)" != 1 || return 1

    # a default rule that suits stretched pools replaces a generated one
    ceph osd pool default set --rule stretch_rule || return 1
    ceph osd pool create newrep2 8 8 || return 1
    test "$(pool_num_zones newrep2)" = 2 || return 1
    test "$(pool_field newrep2 crush_rule)" = 1 || return 1
    wait_for_clean || return 1
}

# Test disabling unstretches every pool and sets the local defaults
function TEST_disable_unstretches_every_pool() {
    local dir=$1
    setup_zones $dir || return 1
    setup_osds $dir || return 1
    create_pools || return 1
    ceph mon enable_stretch_mode c stretch_rule zone || return 1
    wait_for_clean || return 1

    expect_failure $dir "yes-i-really-mean-it" \
        ceph mon disable_stretch_mode || return 1
    ceph mon disable_stretch_mode --yes-i-really-mean-it || return 1

    test "$(pool_num_zones rep)" = 1 || return 1
    test "$(pool_field rep size)" = 3 || return 1
    test "$(pool_field rep peering_crush_bucket_count)" = 0 || return 1
    test "$(pool_num_zones ec)" = 1 || return 1
    test "$(pool_field ec size)" = 3 || return 1
    test "$(pool_nonprimary_shards ec)" = "{1}" || return 1
    test "$(default_value num_zones)" = 1 || return 1
    test "$(default_value replica)" = 3 || return 1
    test "$(ceph mon dump -f json | jq .stretch_mode)" = false || return 1
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = false || return 1
    wait_for_clean || return 1

    expect_failure $dir "already disabled" \
        ceph mon disable_stretch_mode --yes-i-really-mean-it || return 1
}

# Test disable resets the defaults after the last pool has left stretch mode
function TEST_disable_resets_defaults_without_stretch_mode() {
    local dir=$1
    setup_zones $dir || return 1
    setup_osds $dir || return 1
    create_pools || return 1
    ceph mon enable_stretch_mode c stretch_rule zone || return 1
    wait_for_clean || return 1

    # unstretching every pool leaves stretch mode but not its defaults
    for pool in $(ceph osd pool ls); do
        ceph osd pool set $pool num_zones 1 || return 1
    done
    for i in $(seq 60); do
        test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = false && break
        sleep 1
    done
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = false || return 1
    test "$(default_value num_zones)" = 2 || return 1

    ceph mon disable_stretch_mode --yes-i-really-mean-it || return 1
    test "$(default_value num_zones)" = 1 || return 1
    test "$(default_value replica)" = 3 || return 1
    expect_failure $dir "already disabled" \
        ceph mon disable_stretch_mode --yes-i-really-mean-it || return 1
}

# Test a legacy EC pool makes enabling fail without changing anything
function TEST_enable_refuses_legacy_ec_pool() {
    local dir=$1
    setup_zones $dir || return 1
    setup_osds $dir || return 1
    ceph osd pool create rep 8 8 replicated || return 1
    # new EC pools are legacy EC pools by default
    ceph osd erasure-code-profile set ec21 plugin=isa k=2 m=1 \
        crush-failure-domain=osd || return 1
    ceph osd pool create oldec 8 8 erasure ec21 || return 1

    expect_failure $dir "'oldec' is a legacy EC pool" \
        ceph mon enable_stretch_mode c stretch_rule zone || return 1
    test "$(pool_num_zones rep)" = 1 || return 1
    test "$(default_value num_zones)" = 1 || return 1
    test "$(ceph mon dump -f json | jq .stretch_mode)" = false || return 1

    # the same refusal for a single pool
    expect_failure $dir "allow_ec_optimizations true" \
        ceph osd pool set oldec num_zones 2 --zone_failure_domain zone || return 1
}

# Test the checks that need no OSDs
function TEST_enable_checks() {
    local dir=$1
    setup_zones $dir || return 1

    expect_failure $dir "unrecognized crush rule" \
        ceph mon enable_stretch_mode c nosuchrule zone || return 1
    expect_failure $dir "is not a valid crush bucket type" \
        ceph mon enable_stretch_mode c stretch_rule nosuchtype || return 1
    ceph osd crush rule create-erasure ecrule || return 1
    expect_failure $dir "is not a replicated rule" \
        ceph mon enable_stretch_mode c ecrule zone || return 1

    # the next ceph osd pool create must work with the defaults that result
    ceph config set global osd_pool_default_pg_num 100000 || return 1
    expect_failure $dir "(osd_pool_default_pg_num)" \
        ceph mon enable_stretch_mode c stretch_rule zone || return 1
    ceph config rm global osd_pool_default_pg_num || return 1

    # and must take effect
    ceph config set mon osd_pool_default_num_zones 1 || return 1
    expect_failure $dir "section mon of the configuration database" \
        ceph mon enable_stretch_mode c stretch_rule zone || return 1
    ceph config rm mon osd_pool_default_num_zones || return 1

    test "$(ceph mon dump -f json | jq .stretch_mode)" = false || return 1
    test "$(default_value num_zones)" = 1 || return 1
}

main mon-stretch-enable-disable "$@"

# Local Variables:
# compile-command: "cd ../../../build ; make -j4 && ../qa/run-standalone.sh mon-stretch-enable-disable.sh"
# End:
