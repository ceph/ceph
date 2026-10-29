#!/usr/bin/env bash
#
# ceph mon enable_stretch_mode and disable_stretch_mode once the upgrade is
# committed: they change every pool as ceph osd pool set num_zones does and
# set the pool creation defaults.

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

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
# six OSDs, and a Manager unless the second argument is nomgr
function setup_osds() {
    local dir=$1
    local mgr=${2:-mgr}

    if [ "$mgr" = mgr ]; then
        run_mgr $dir x || return 1
    fi
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

function pool_rule_name() {
    ceph osd pool get $1 crush_rule -f json | jq -r .crush_rule
}

# Test enabling without a CRUSH rule gives each pool a rule generated for it,
# and that disabling removes those rules
function TEST_enable_generates_rules() {
    local dir=$1
    setup_zones $dir || return 1
    setup_osds $dir || return 1
    ceph osd pool create rep 8 8 replicated --num_zones 1 || return 1
    ceph osd erasure-code-profile set ec21 plugin=isa k=2 m=1 \
        crush-failure-domain=osd || return 1
    ceph osd pool create ec 8 8 erasure ec21 || return 1
    ceph osd pool set ec allow_ec_optimizations true || return 1
    # over two zones already, with another replica count
    ceph osd pool create two 8 8 replicated --num_zones 2 --replica 1 \
        --zone_failure_domain zone || return 1

    ceph mon enable_stretch_mode --dividing_bucket zone || return 1
    test "$(pool_num_zones rep)" = 2 || return 1
    test "$(pool_field rep size)" = 4 || return 1
    test "$(pool_rule_name rep)" = rep || return 1
    ceph osd crush rule dump rep | \
        jq -e '[.steps[] | select(.op == "choose_firstn") | .type] == ["zone"]' || return 1
    test "$(pool_field ec size)" = 6 || return 1
    test "$(pool_rule_name ec)" = ec-stretch || return 1
    test "$(pool_field two size)" = 4 || return 1
    test "$(pool_rule_name two)" = two-replica-2 || return 1
    wait_for_clean || return 1

    # a rule given to the command is still given to the replicated pools
    ceph osd pool create later 8 8 replicated --num_zones 1 || return 1
    ceph mon enable_stretch_mode c stretch_rule zone || return 1
    test "$(pool_rule_name later)" = stretch_rule || return 1

    ceph mon disable_stretch_mode --yes-i-really-mean-it || return 1
    local rule
    for rule in rep ec-stretch two-replica-2; do
        ! ceph osd crush rule ls | grep -qx $rule || return 1
    done
    wait_for_clean || return 1
}

# Test enabling with no pools enters stretch mode in the MonMap and sets the
# defaults, and the first pool over two zones sets the cluster stretch flags
function TEST_enable_without_pools() {
    local dir=$1
    setup_zones $dir || return 1
    test -z "$(ceph osd pool ls)" || return 1

    ceph mon enable_stretch_mode c stretch_rule zone || return 1
    test "$(ceph mon dump -f json | jq .stretch_mode)" = true || return 1
    test "$(ceph mon dump -f json | jq -r .tiebreaker_mon)" = c || return 1
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = false || return 1
    test "$(default_value num_zones)" = 2 || return 1
    test "$(default_value zone_failure_domain)" = zone || return 1

    # no individual stretch pool meanwhile
    ceph osd pool create one 8 8 replicated --num_zones 1 || return 1
    expect_failure $dir "cannot be used while stretch mode is enabled" \
        ceph osd pool stretch set one 2 2 zone replicated_rule 4 2 || return 1
    ceph osd pool rm one one --yes-i-really-really-mean-it || return 1

    # disabling and enabling again work while no pool has two zones
    ceph mon disable_stretch_mode --yes-i-really-mean-it || return 1
    test "$(ceph mon dump -f json | jq .stretch_mode)" = false || return 1
    test "$(default_value num_zones)" = 1 || return 1
    ceph mon enable_stretch_mode c stretch_rule zone || return 1
    ceph mon enable_stretch_mode c stretch_rule zone || return 1
    test "$(ceph mon dump -f json | jq .stretch_mode)" = true || return 1
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = false || return 1

    # the first pool over two zones must divide them as the tiebreaker was
    # chosen for: two regions hold the zones, but no Monitor is in one, so
    # stretch mode by region would crash the Monitors. No Manager yet, which
    # would create .mgr over the zones first.
    setup_osds $dir nomgr || return 1
    local region
    for region in 1 2; do
        ceph osd crush add-bucket region$region region || return 1
        ceph osd crush move region$region root=default || return 1
    done
    ceph osd crush move iris region=region1 || return 1
    ceph osd crush move pze region=region2 || return 1
    expect_failure $dir "monitor stretch mode" \
        ceph osd pool create other 8 8 replicated \
        --zone_failure_domain region || return 1
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = false || return 1

    # the first pool over two zones sets the cluster stretch flags
    ceph osd pool create rep 8 8 replicated || return 1
    test "$(pool_num_zones rep)" = 2 || return 1
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = true || return 1
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_bucket_count)" = 2 || return 1
    local zone_type=$(ceph osd crush dump | jq '.types[] | select(.name == "zone") | .type_id')
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_bucket)" = "$zone_type" || return 1
    test "$(ceph mon dump -f json | jq -r .tiebreaker_mon)" = c || return 1
    run_mgr $dir x || return 1
    wait_for_clean || return 1
}

# Test Rook's pool create in a stretch cluster: the stretch rule and --size 4,
# before and after enable_stretch_mode, whatever osd_pool_default_size is
function TEST_size_of_defaults_in_stretch_mode() {
    local dir=$1
    setup_zones $dir || return 1
    setup_osds $dir || return 1
    ceph osd pool create before 8 replicated stretch_rule --size 4 || return 1
    test "$(pool_field before size)" = 4 || return 1

    ceph mon enable_stretch_mode c stretch_rule zone || return 1
    test "$(pool_num_zones before)" = 2 || return 1
    ceph osd pool create rook 8 replicated stretch_rule --size 4 || return 1
    test "$(pool_num_zones rook)" = 2 || return 1
    test "$(pool_field rook size)" = 4 || return 1
    test "$(pool_field rook crush_rule)" = 1 || return 1
    # Rook re-applies a user's osd_pool_default_size on every reconcile
    ceph config set global osd_pool_default_size 3 || return 1
    ceph osd pool create rook2 8 replicated stretch_rule --size 4 || return 1
    test "$(pool_field rook2 size)" = 4 || return 1
    test "$(pool_field rook2 replica)" = 2 || return 1
    expect_failure $dir "unless it is a multiple of num_zones 2" \
        ceph osd pool create other 8 replicated stretch_rule --size 5 || return 1
    expect_failure $dir "cannot specify 'size' with num_zones > 1" \
        ceph osd pool create other 8 replicated stretch_rule --size 4 \
        --num_zones 2 || return 1
    wait_for_clean || return 1
}

# Test a pool created through librados takes the pool creation defaults, as
# ceph osd pool create <pool> does: in stretch mode it is stretched
function TEST_librados_pool_takes_defaults() {
    local dir=$1
    setup_zones $dir || return 1
    setup_osds $dir || return 1
    librados_create_pool $dir before || return 1
    test "$(pool_num_zones before)" = 1 || return 1
    test "$(pool_field before size)" = 3 || return 1

    ceph mon enable_stretch_mode c stretch_rule zone || return 1
    librados_create_pool $dir during || return 1
    test "$(pool_num_zones during)" = 2 || return 1
    test "$(pool_field during size)" = 4 || return 1
    test "$(pool_field during crush_rule)" != 1 || return 1
    # the default rule, when it suits the pool
    ceph osd pool default set --rule stretch_rule || return 1
    librados_create_pool $dir withrule || return 1
    test "$(pool_field withrule crush_rule)" = 1 || return 1
    ceph osd pool default set --rule none || return 1

    # a create that fails removes the rule it generated
    ceph config set global mon_max_pg_per_osd 1 || return 1
    ! librados_create_pool $dir toomany || return 1
    ! ceph osd pool ls | grep -qx toomany || return 1
    ! ceph osd crush rule ls | grep -qx toomany || return 1
    ceph config rm global mon_max_pg_per_osd || return 1
    wait_for_clean || return 1
}

# Test enabling checks that its rule places a pool already over two zones
# with its new share in each zone
function TEST_enable_checks_rule_of_pools_over_zones() {
    local dir=$1
    setup_zones $dir || return 1
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
    add_stretch_rule $dir three_per_zone default zone 2 osd 3 || return 1
    ceph osd pool create rep3 --num_zones 2 --replica 3 \
        --zone_failure_domain zone --osd_failure_domain osd || return 1
    local rule=$(pool_field rep3 crush_rule)

    ! ceph mon enable_stretch_mode c three_per_zone zone > $dir/out 2>&1 || return 1
    grep "pool 'rep3': crush rule three_per_zone does not place 2 items in each zone" \
        $dir/out || return 1
    test "$(pool_field rep3 size)" = 6 || return 1
    test "$(pool_field rep3 crush_rule)" = "$rule" || return 1

    # nor does it change the size of a pool with nosizechange
    ceph osd pool set rep3 nosizechange true || return 1
    ! ceph mon enable_stretch_mode c stretch_rule zone > $dir/out 2>&1 || return 1
    grep "pool 'rep3': pool size change is disabled" $dir/out || return 1
    ceph osd pool set rep3 nosizechange false || return 1

    ceph mon enable_stretch_mode c stretch_rule zone || return 1
    test "$(pool_field rep3 size)" = 4 || return 1
}

# Test enable_stretch_mode picks the monitor outside both zones as the
# tiebreaker when none is given; the rule and bucket are then given by name
function TEST_enable_without_tiebreaker() {
    local dir=$1
    setup_zones $dir || return 1
    setup_osds $dir || return 1
    ceph osd pool create rep 8 8 replicated || return 1

    ! ceph mon enable_stretch_mode stretch_rule zone > $dir/out 2>&1 || return 1
    grep "missing required parameter dividing_bucket" $dir/out || return 1
    ceph mon enable_stretch_mode --new_crush_rule stretch_rule \
        --dividing_bucket zone || return 1
    test "$(ceph mon dump -f json | jq .stretch_mode)" = true || return 1
    test "$(ceph mon dump -f json | jq -r .tiebreaker_mon)" = c || return 1
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = true || return 1
    test "$(pool_num_zones rep)" = 2 || return 1
}

# Test disabling unstretches every pool and sets the local defaults
function TEST_disable_unstretches_every_pool() {
    local dir=$1
    setup_zones $dir || return 1
    setup_osds $dir || return 1
    create_pools || return 1
    ceph mon enable_stretch_mode c stretch_rule zone || return 1
    # a pool confined to one zone, with the stretched replica default
    ceph osd pool create local 8 8 --num_zones 1 || return 1
    test "$(pool_field local size)" = 2 || return 1
    wait_for_clean || return 1

    expect_failure $dir "yes-i-really-mean-it" \
        ceph mon disable_stretch_mode || return 1
    ceph mon disable_stretch_mode --yes-i-really-mean-it || return 1
    # only pools over zones change
    test "$(pool_num_zones local)" = 1 || return 1
    test "$(pool_field local size)" = 2 || return 1

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

    # a repeat succeeds, as the monitors' own retry does
    ceph mon disable_stretch_mode --yes-i-really-mean-it 2> $dir/out || return 1
    grep -q "stretch mode is disabled" $dir/out || return 1
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
        ceph osd pool set $pool num_zones 1 --yes-i-really-mean-it || return 1
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
    # a repeat succeeds, as the monitors' own retry does
    ceph mon disable_stretch_mode --yes-i-really-mean-it 2> $dir/out || return 1
    grep -q "stretch mode is disabled" $dir/out || return 1
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
