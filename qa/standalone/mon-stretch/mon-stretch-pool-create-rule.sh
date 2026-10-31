#!/usr/bin/env bash
#
# Checks of osd pool create for a pool over two zones: its CRUSH rule must
# place the pool's copies in both zones, its profile must support FastEC,
# and no individual stretch pool may exist.
#

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON_A="127.0.0.1:7304" # git grep '\<7304\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7305" # git grep '\<7305\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7306" # git grep '\<7306\>' : there must be only one
    export CEPH_MON="$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"
    run_stretch_tests $dir "$@"
}

function TEST_pool_create_rule_must_span_zones() {
    local dir=$1

    two_zone_cluster $dir 2 || return 1

    # replicated_rule chooses hosts anywhere under the root
    ! ceph osd pool create bad --num_zones 2 --replica 2 --rule replicated_rule \
        > $dir/out 2>&1 || return 1
    grep "does not place the pool in 2 buckets of type datacenter" $dir/out || return 1
    ! ceph osd pool ls | grep -qx bad || return 1
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = false || return 1

    add_stretch_rule $dir two_dc default datacenter 2 host 2 || return 1
    ceph osd pool create good --num_zones 2 --replica 2 --rule two_dc || return 1
    test "$(pool_field good num_zones)" = 2 || return 1
}

# osd pool set crush_rule checks the rule of a pool over two zones as create does
function TEST_pool_set_rule_must_span_zones() {
    local dir=$1

    two_zone_cluster $dir 2 || return 1
    ceph osd pool create p --num_zones 2 --replica 2 || return 1
    local rule=$(pool_field p crush_rule)

    ! ceph osd pool set p crush_rule replicated_rule > $dir/out 2>&1 || return 1
    grep "does not place the pool in 2 buckets of type datacenter" $dir/out || return 1
    add_stretch_rule $dir one_per_dc default datacenter 2 host 1 || return 1
    ! ceph osd pool set p crush_rule one_per_dc > $dir/out 2>&1 || return 1
    grep "does not place 2 items in each datacenter" $dir/out || return 1
    test "$(pool_field p crush_rule)" = "$rule" || return 1

    add_stretch_rule $dir two_per_dc default datacenter 2 host 2 || return 1
    ceph osd pool set p crush_rule two_per_dc || return 1
}

# min_size of a pool over two zones counts the copies in each zone
function TEST_pool_create_min_size_per_zone() {
    local dir=$1

    two_zone_cluster $dir 2 || return 1

    ceph osd pool create s --num_zones 2 --replica 2 --min_size 2 || return 1
    test "$(pool_field s min_size)" = 2 || return 1
    test "$(pool_field s size)" = 4 || return 1
    ! ceph osd pool create s2 --num_zones 2 --replica 2 --min_size 3 \
        > $dir/out 2>&1 || return 1
    grep "pool min_size must be between 1 and replica" $dir/out || return 1
    ! ceph osd pool ls | grep -qx s2 || return 1
}

# A rule that already has the pool's name, left by a pool that was renamed,
# must also place a pool over two zones in both zones
function TEST_pool_create_existing_rule_must_span_zones() {
    local dir=$1

    two_zone_cluster $dir 3 || return 1

    ceph osd pool create e erasure --k 2 --m 1 || return 1
    ceph osd pool rename e e_old || return 1
    ! ceph osd pool create e erasure --k 2 --m 1 --num_zones 2 \
        > $dir/out 2>&1 || return 1
    grep "crush rule e does not place the pool in 2 buckets of type datacenter" \
        $dir/out || return 1
    ! ceph osd pool ls | grep -qx e || return 1

    ceph osd crush rule create-replicated r default host || return 1
    ! ceph osd pool create r --num_zones 2 --replica 2 > $dir/out 2>&1 || return 1
    grep "crush rule r does not place the pool in 2 buckets of type datacenter" \
        $dir/out || return 1
    ! ceph osd pool ls | grep -qx r || return 1
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = false || return 1
}

# A rule left with the pool's name by an earlier pool over two zones must
# place this pool's own share of shards in each zone
function TEST_pool_create_existing_rule_zone_size() {
    local dir=$1

    two_zone_cluster $dir 4 || return 1

    ceph osd pool create e erasure --k 2 --m 2 --num_zones 2 || return 1
    ceph osd pool rename e e_old || return 1
    ! ceph osd pool create e erasure --k 2 --m 1 --num_zones 2 \
        > $dir/out 2>&1 || return 1
    grep "crush rule e does not place 3 items in each datacenter" $dir/out || return 1
    ! ceph osd pool ls | grep -qx e || return 1
    ceph osd pool create f erasure --k 2 --m 1 --num_zones 2 || return 1
}

# A pool over two zones needs an erasure code profile that supports FastEC,
# and a refused create leaves no pool and stretch mode off
function TEST_pool_create_zones_need_fast_ec() {
    local dir=$1

    two_zone_cluster $dir 3 || return 1

    ceph osd erasure-code-profile set cauchy plugin=jerasure \
        technique=cauchy_good k=2 m=1 || return 1
    ! ceph osd pool create c erasure --erasure_code_profile cauchy --num_zones 2 \
        > $dir/out 2>&1 || return 1
    grep "Multi-zone erasure coded pools require FastEC support" $dir/out || return 1
    ! ceph osd pool ls | grep -qx c || return 1

    ceph osd erasure-code-profile set lrc plugin=lrc k=2 m=1 l=3 || return 1
    ! ceph osd pool create l erasure --erasure_code_profile lrc --num_zones 2 \
        > $dir/out 2>&1 || return 1
    grep "LRC erasure code does not support stretch" $dir/out || return 1
    ! ceph osd pool ls | grep -qx l || return 1

    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = false || return 1
    test "$(ceph mon dump -f json | jq .stretch_mode)" = false || return 1
}

# An individual stretch pool ('osd pool stretch set' while stretch mode is
# off) and stretch mode cannot be combined
function TEST_individual_stretch_pool_blocks_stretch_mode() {
    local dir=$1

    two_zone_cluster $dir 2 || return 1
    add_stretch_rule $dir two_dc default datacenter 2 host 2 || return 1
    ceph osd pool create ind 8 8 replicated two_dc || return 1
    ceph osd pool stretch set ind 2 2 datacenter two_dc 4 2 || return 1

    ! ceph osd pool create p --num_zones 2 --replica 2 > $dir/out 2>&1 || return 1
    grep "individual stretch pools and stretch mode cannot be combined" $dir/out || return 1
    ! ceph osd pool ls | grep -qx p || return 1
    ceph osd pool create q --replica 2 || return 1
    ! ceph osd pool set q num_zones 2 > $dir/out 2>&1 || return 1
    grep "individual stretch pools and stretch mode cannot be combined" $dir/out || return 1
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = false || return 1
    test "$(pool_field ind peering_crush_bucket_count)" = 2 || return 1
    ceph mon set election_strategy connectivity || return 1
    ! ceph mon enable_stretch_mode c two_dc datacenter > $dir/out 2>&1 || return 1
    grep "individual stretch pools and stretch mode cannot be combined" $dir/out || return 1
    test "$(ceph mon dump -f json | jq .stretch_mode)" = false || return 1
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = false || return 1

    ceph osd pool stretch unset ind two_dc 3 2 || return 1
    ceph osd pool create p --num_zones 2 --replica 2 || return 1
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = true || return 1
}

# osd pool stretch unset on an individual stretch pool, while stretch mode is
# off, keeps the monitors' settings and leaves the pool with no mandatory zone
function TEST_individual_stretch_pool_to_one_zone() {
    local dir=$1

    two_zone_cluster $dir 2 || return 1
    ceph mon set election_strategy disallow || return 1
    ceph mon add disallowed_leader c || return 1
    add_stretch_rule $dir two_dc default datacenter 2 host 2 || return 1
    ceph osd pool create ind 8 8 replicated two_dc || return 1
    ceph osd pool stretch set ind 2 2 datacenter two_dc 4 2 || return 1
    test "$(pool_field ind num_zones)" = 1 || return 1

    ceph osd pool stretch unset ind two_dc 4 2 || return 1
    ceph mon dump | grep '^disallowed_leaders' | grep -q c || return 1
    test "$(pool_field ind peering_crush_bucket_mandatory_member)" = $CRUSH_ITEM_NONE || return 1
}

# What osd pool create gives an EC pool over two zones
function TEST_pool_create_two_zone_ec_fields() {
    local dir=$1

    two_zone_cluster $dir 4 || return 1

    ceph osd pool create e erasure --k 2 --m 2 --num_zones 2 || return 1
    test "$(pool_field e size)" = 8 || return 1
    test "$(pool_field e min_size)" = 3 || return 1
    test "$(pool_field e num_zones)" = 2 || return 1
    test "$(pool_field e peering_crush_bucket_count)" = 2 || return 1
    test "$(pool_field e peering_crush_bucket_target)" = 2 || return 1
    test "$(ceph osd pool ls detail -f json |
        jq -r '.[]|select(.pool_name=="e")|.nonprimary_shards')" = "{1,5}" || return 1
    # FastEC's stripe unit of 16 KiB in each of the k=2 data chunks
    test "$(pool_field e stripe_width)" = $((2 * 16384)) || return 1
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = true || return 1
    wait_for_clean || return 1
    # each zone's 4 shards are in one datacenter (dc1 is osd.0-3), and the
    # two zones in different ones
    local pg up
    for pg in $(ceph pg ls-by-pool e -f json | jq -r '.pg_stats[].pgid'); do
        up=$(ceph pg map $pg -f json | jq -c '.up')
        test "$(echo $up | jq -c '[(.[0:4]|map(. < 4)|unique), (.[4:8]|map(. < 4)|unique)]')" \
            = "[[true],[false]]" -o \
            "$(echo $up | jq -c '[(.[0:4]|map(. < 4)|unique), (.[4:8]|map(. < 4)|unique)]')" \
            = "[[false],[true]]" || return 1
    done
}

# A pool over two zones joins stretch mode as it is: the zone weights are
# checked only when stretch mode is entered
function TEST_join_stretch_mode_with_uneven_zone_weights() {
    local dir=$1

    two_zone_cluster $dir 3 || return 1
    ceph osd pool create first --num_zones 2 --replica 2 || return 1
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = true || return 1

    ceph osd crush reweight osd.0 0.1 || return 1
    ceph osd crush reweight osd.1 0.1 || return 1
    ceph osd pool create second --num_zones 2 --replica 2 || return 1
    test "$(pool_field second num_zones)" = 2 || return 1
    ceph osd pool create third 8 || return 1
    ceph osd pool set third num_zones 2 --replica 2 || return 1
    test "$(pool_field third num_zones)" = 2 || return 1
}

main mon-stretch-pool-create-rule "$@"
