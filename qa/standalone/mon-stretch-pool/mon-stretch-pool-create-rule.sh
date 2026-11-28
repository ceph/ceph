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

# osd pool create <pool> --num_zones 2 --rule <rule> needs a rule that
# places the pool in both zones, here the datacenters dc1 and dc2.
# replicated_rule, which places copies anywhere under the root, is refused
# for pool bad, which is not created, and the cluster stretch flags stay
# off. two_dc, which chooses 2 datacenters and 2 hosts in each, is accepted
# for pool good, which is then over two zones.
function TEST_pool_create_rule_must_span_zones() {
    local dir=$1

    two_zone_cluster $dir 2 || return 1

    # replicated_rule chooses OSDs anywhere under the root
    ! ceph osd pool create bad --num_zones 2 --replica 2 --rule replicated_rule \
        > $dir/out 2>&1 || return 1
    grep "does not place the pool in 2 buckets of type datacenter" $dir/out || return 1
    ! ceph osd pool ls | grep -qx bad || return 1
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = false || return 1

    add_stretch_rule $dir two_dc default datacenter 2 host 2 || return 1
    ceph osd pool create good --num_zones 2 --replica 2 --rule two_dc || return 1
    test "$(pool_field good num_zones)" = 2 || return 1
}

# osd pool set <pool> crush_rule <rule> checks the rule of a pool over two
# zones as osd pool create does. Pool p is created over the datacenters dc1
# and dc2 with --num_zones 2 --replica 2. replicated_rule (copies anywhere
# under the root) is refused, as it does not place p in two datacenters, and
# so is one_per_dc (one host in each datacenter), as it does not place 2
# copies in each; p keeps its rule. two_per_dc (2 hosts in each datacenter)
# is accepted.
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

# For a pool over two zones, osd pool create --min_size counts the copies in
# each zone, as --replica does. Pool s, created over the datacenters dc1 and
# dc2 with --num_zones 2 --replica 2 --min_size 2, gets size 4 and min_size
# 2. s2, with --min_size 3, is refused, as min_size cannot be more than
# replica, and is not created.
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

# osd pool create <pool> --num_zones 2 uses the rule that has the pool's
# name, if there is one, and refuses it unless it places the pool in both
# zones, the datacenters dc1 and dc2. Erasure coded pool e is created in one
# zone, with a rule named e, and renamed e_old, which keeps the rule.
# Creating e again with --num_zones 2 is refused, as rule e does not place it
# in two datacenters. Replicated pool r is refused the same way with rule r,
# made by create-replicated over hosts under the root. Neither pool is
# created, and the cluster stretch flags stay off.
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

# The rule that has the pool's name, when osd pool create --num_zones 2 finds
# one, must also place the new pool's number of shards in each zone, the
# datacenters dc1 and dc2. Erasure coded pool e is created over two zones
# with k=2 m=2, so its rule, e, places 4 shards in each datacenter, and is
# renamed e_old. Creating e again over two zones with k=2 m=1, which needs 3
# shards in each, is refused and leaves no pool. Pool f, created the same
# way under a name that has no rule, is created.
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

# An erasure coded pool over two zones needs a profile that supports FastEC.
# osd pool create --num_zones 2 refuses pool c, with profile cauchy
# (jerasure with technique cauchy_good), and pool l, with profile lrc (the
# LRC plugin), each with its own message. Neither pool is created, and the
# cluster stretch flags, in the OSDMap and the monmap, stay off.
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

# osd pool stretch set makes an individual stretch pool: a pool given
# stretch settings of its own while the cluster stretch flags are off. It
# cannot be combined with pools over zones or with global stretch mode.
# With pool ind on two_dc (2 hosts in each of the datacenters dc1 and dc2)
# made an individual stretch pool, osd pool create p --num_zones 2, osd pool
# set q num_zones 2 and mon enable_stretch_mode are each refused. ind keeps
# its settings and the cluster stretch flags stay off. After osd pool
# stretch unset ind, p is created over two zones and sets the cluster
# stretch flags.
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

# osd pool stretch unset, for an individual stretch pool while the cluster
# stretch flags are off, must leave the monitors' settings alone and the
# pool with no mandatory zone. The monitors use election strategy disallow,
# with c a disallowed leader. Pool ind, on two_dc (2 hosts in each of the
# datacenters dc1 and dc2), is made an individual stretch pool, which keeps
# num_zones 1, and is then unset: c is still a disallowed leader, and ind's
# peering_crush_bucket_mandatory_member is none.
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

# osd pool create <pool> erasure --num_zones 2 makes a pool with k+m shards
# in each zone, the datacenters dc1 and dc2. Pool e, with k=2 m=2, gets size
# 8, min_size 3, num_zones 2, a peering_crush_bucket_count and
# peering_crush_bucket_target of 2, shards 1 and 5 as its nonprimary_shards,
# and a stripe width of 32 KiB, FastEC's 16 KiB stripe unit in each of the 2
# data chunks. It sets the cluster stretch flags. Once its PGs are clean, the
# first 4 shards of every PG are in one datacenter and the last 4 in the
# other.
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

# The zones' CRUSH weights are checked when the cluster stretch flags are
# set, and not when more pools go over the zones later. Pool first, created
# with --num_zones 2 over the datacenters dc1 and dc2, sets the flags. osd.0
# and osd.1, two of dc1's 3 OSDs, are then reweighted to 0.1, so the zones'
# weights are far apart. Pool second is still created over two zones, and
# pool third, created in one zone, still moves to two with osd pool set
# third num_zones 2.
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
