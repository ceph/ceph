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

# the EC shards that cannot become primary, raw shards 1 to k-1 in each zone
function pool_nonprimary_shards() {
    ceph osd pool ls detail -f json | jq -r --arg p $1 ".[]|select(.pool_name==\$p)|.nonprimary_shards"
}

function pool_num_zones() {
    ceph osd pool get $1 num_zones -f json | jq .num_zones
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

# mon enable_stretch_mode <tiebreaker> <rule> <zone type> turns on global
# stretch mode: it moves every pool over two zones, as osd pool set
# num_zones 2 does, and sets the pool creation defaults to 2 copies in each
# of two zones of that type. This test gives it tiebreaker c (the monitor
# in neither zone), stretch_rule (2 hosts in each of the zones iris and
# pze) and type zone. The pools are rep (replicated) and ec (erasure coded,
# k=2 m=1). rep goes to stretch_rule with 4 copies, and ec to a new rule,
# ec-stretch, with 3 shards in each zone, of which shards 1 and 4 cannot
# become primary. The monmap records global stretch mode with c as the
# tiebreaker, and the cluster stretch flags are set. The command can be
# run again.
# A new pool, newrep, follows the new defaults with 4 copies on a rule made
# for it; after osd pool default set --rule stretch_rule, newrep2 is put on
# stretch_rule.
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

# mon enable_stretch_mode with no rule gives each pool a new rule over two
# zones with the placement of the pool's own rule. This test runs it with
# only --dividing_bucket zone on rep (replicated, in one zone), ec (erasure
# coded, k=2 m=1) and two (replicated, already over two zones with 1 copy
# in each). rep gets 4 copies on rule rep, which chooses the zones and then
# OSDs, as replicated_rule chooses OSDs here; ec gets 6 shards on
# ec-stretch; and two gets 2 copies in each zone on two-replica-2.
# Pool later, replicated, is then created in one zone; mon
# enable_stretch_mode run again with stretch_rule moves it to stretch_rule.
# mon disable_stretch_mode then leaves none of the rules rep, ec-stretch
# and two-replica-2.
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
    # the zones, then the OSDs that the default rule chooses
    ceph osd crush rule dump rep | \
        jq -e '[.steps[] | select(.op == "choose_firstn") | .type] == ["zone", "osd"]' || return 1
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

# mon enable_stretch_mode with no pools turns on global stretch mode in the
# monmap, with c as the tiebreaker, and sets the pool creation defaults to
# two zones of type zone. The cluster stretch flags stay unset until a pool
# is over two zones. Meanwhile osd pool stretch set, which stretches a
# single pool, is refused, and mon disable_stretch_mode and then mon
# enable_stretch_mode, run twice, work.
# OSDs are then added, and the zones iris and pze put in two regions. A pool
# over zones of type region is refused, as the monitors were divided by
# zone, and the flags stay unset. The next pool, rep, is created over the
# two zones by the defaults and sets the cluster stretch flags, with zone as
# their bucket type, 2 zones and c as the tiebreaker.
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
    # cluster stretch flags by region would crash the Monitors. No Manager
    # yet.
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

# Rook creates each pool of a stretch cluster with osd pool create <pool>
# <pg_num> replicated stretch_rule --size 4. Pool before is created that
# way before mon enable_stretch_mode, which then moves it over two zones.
# After it, pool rook is created over two zones on stretch_rule with 4
# copies. rook2, created after osd_pool_default_size is set to 3, as Rook
# re-applies a user's osd_pool_default_size, still gets 4 copies, 2 in each
# zone. --size 5, not a multiple of the two zones, is refused, and so is
# --size with --num_zones 2.
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

# A pool that a librados client such as RGW creates takes the pool creation
# defaults, as one from osd pool create <pool> does. Before mon
# enable_stretch_mode, pool before is in one zone with 3 copies. After it,
# pool during is over two zones with 4 copies on a rule made for it, and,
# after osd pool default set --rule stretch_rule, pool withrule is on
# stretch_rule (2 hosts in each zone). A create that fails, with
# mon_max_pg_per_osd set to 1, leaves no pool toomany and no rule of that
# name.
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

# mon enable_stretch_mode gives every replicated pool 2 copies in each zone
# on the rule it is given, so the rule must place a pool already over two
# zones that way. This test creates rep3, a replicated pool over two zones
# with 3 copies in each. The command is refused with three_per_zone, a rule
# of 3 OSDs in each zone, and rep3 keeps its 6 copies and its rule. With
# nosizechange set on rep3 it is refused with stretch_rule (2 hosts in each
# zone) too. Once nosizechange is unset, it gives rep3 4 copies.
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

# mon enable_stretch_mode with no tiebreaker picks the monitor in neither
# zone, c. Its arguments are positional, so mon enable_stretch_mode
# stretch_rule zone is refused, as dividing_bucket is missing. With
# --new_crush_rule stretch_rule --dividing_bucket zone it turns on global
# stretch mode with c as the tiebreaker, sets the cluster stretch flags and
# moves pool rep over two zones.
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

# mon disable_stretch_mode turns off global stretch mode: it moves every
# pool over two zones back to one zone, as osd pool set num_zones 1 does,
# and sets the pool creation defaults back to one zone of 3 copies. It
# needs --yes-i-really-mean-it. This test runs mon enable_stretch_mode on
# rep (replicated) and ec (erasure coded, k=2 m=1), and then creates local
# in one zone, with 2 copies, the replica default of global stretch mode.
# The disable moves rep back to 3 copies and ec to 3 shards, takes global
# stretch mode out of the monmap and clears the cluster stretch flags;
# local is left as it was. Run again, the command succeeds, as the
# monitors' own retry needs.
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

# mon disable_stretch_mode moves a replicated pool whose rule has a device
# class to a rule of its own in one zone, of the same class. Here every OSD
# is ssd, and rep is created over two zones, 2 copies in each, with --class
# ssd; global stretch mode is never enabled. The command moves rep to one
# zone on rep-single-zone, which takes the ssd OSDs of the default root.
function TEST_disable_keeps_placement_of_rule() {
    local dir=$1
    setup_zones $dir || return 1
    setup_osds $dir || return 1
    ceph osd crush rm-device-class $(seq 0 5) || return 1
    ceph osd crush set-device-class ssd $(seq 0 5) || return 1
    ceph osd pool create rep 8 8 replicated --num_zones 2 --replica 2 \
        --zone_failure_domain zone --class ssd || return 1

    ceph mon disable_stretch_mode --yes-i-really-mean-it || return 1
    test "$(pool_num_zones rep)" = 1 || return 1
    test "$(pool_rule_name rep)" = rep-single-zone || return 1
    test "$(ceph osd crush rule dump rep-single-zone | jq -r '.steps[0].item_name')" = default~ssd || return 1
}

# When osd pool set num_zones 1 has moved every pool to one zone, the
# cluster stretch flags are cleared, but the pool creation defaults that mon
# enable_stretch_mode set stay at two zones. This test does that to rep
# (replicated) and ec (erasure coded, k=2 m=1) after the enable. mon
# disable_stretch_mode then still sets the defaults back to one zone of 3
# copies, and succeeds when run again.
function TEST_disable_resets_defaults_without_stretch_mode() {
    local dir=$1
    setup_zones $dir || return 1
    setup_osds $dir || return 1
    create_pools || return 1
    ceph mon enable_stretch_mode c stretch_rule zone || return 1
    wait_for_clean || return 1

    # unstretching every pool clears the cluster stretch flags but not the
    # defaults
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

# An erasure coded pool without allow_ec_optimizations, a legacy erasure
# coded pool, cannot be over two zones. With
# oldec such a pool, mon enable_stretch_mode is refused and changes
# nothing: rep, a replicated pool, stays in one zone, the pool creation
# defaults stay at one zone and the monmap is not in global stretch mode.
# osd pool set oldec num_zones 2 is refused too, asking for
# allow_ec_optimizations.
function TEST_enable_refuses_legacy_ec_pool() {
    local dir=$1
    setup_zones $dir || return 1
    setup_osds $dir || return 1
    ceph osd pool create rep 8 8 replicated || return 1
    ceph config set global osd_pool_default_flag_ec_optimizations false || return 1
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

# mon enable_stretch_mode checks its arguments and the configuration before
# it changes anything. With no OSDs, it is refused for nosuchrule and
# nosuchtype, which do not exist, and for ecrule, an erasure rule. It is
# refused while osd_pool_default_pg_num is 100000, as the next osd pool
# create must work with the defaults it sets, and while
# osd_pool_default_num_zones is set in the mon section of the configuration
# database, where it would override them. Global stretch mode stays off and
# the default num_zones stays 1.
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
