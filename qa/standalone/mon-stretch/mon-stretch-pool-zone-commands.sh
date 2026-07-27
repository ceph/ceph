#!/usr/bin/env bash
#
# Changes to existing pools over zones: the pool set refusals, the limits
# of num_zones, and which rules and profiles stay or go.
#

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    export CEPH_MON_A="127.0.0.1:7350" # git grep '\<7350\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7351" # git grep '\<7351\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7352" # git grep '\<7352\>' : there must be only one
    export CEPH_MON="$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"
    run_stretch_tests "$@"
}

# CEPH_ARGS for a test that runs mon.a only
function one_mon_args() {
    echo "${CEPH_ARGS/--mon-host=$CEPH_MON /--mon-host=$CEPH_MON_A }"
}

# size and replica of pools over zones, and min_size of an EC pool over zones
function TEST_pool_set_on_two_zones() {
    local dir=$1
    two_zone_cluster $dir || return 1

    ceph osd pool create r --num_zones 2 --replica 2 || return 1
    expect_failure $dir "cannot set 'size' for stretch pools (num_zones=2); use 'replica' parameter instead" \
        ceph osd pool set r size 3 || return 1
    test "$(pool_field r size)" = 4 || return 1
    ceph osd pool set r nosizechange true || return 1
    expect_failure $dir "pool size change is disabled; you must unset nosizechange flag for the pool first" \
        ceph osd pool set r replica 3 || return 1
    test "$(pool_field r size)" = 4 || return 1
    ceph osd pool set r nosizechange false || return 1
    ceph osd pool set r replica 3 || return 1
    test "$(pool_field r size)" = 6 || return 1

    ceph osd pool create e erasure --num_zones 2 --k 2 --m 1 || return 1
    expect_failure $dir "replica is only applicable to replicated pools" \
        ceph osd pool set e replica 2 || return 1
    # min_size counts the shards in each zone
    expect_failure $dir "pool min_size must be between k=2 and k+m=3 (inclusively)" \
        ceph osd pool set e min_size 4 || return 1
    ceph osd pool set e min_size 3 || return 1
    test "$(pool_field e min_size)" = 3 || return 1
}

# A PG has at most 128 OSDs: num_zones 2 is refused for a pool of more than 64
# shards. A rule generated for so many shards is refused for the zones' size
# first, so the pool is given a rule over two zones.
function TEST_num_zones_osd_limit() {
    local dir=$1
    two_zone_cluster $dir || return 1

    ceph osd erasure-code-profile set wide plugin=jerasure k=60 m=5 \
        crush-failure-domain=osd || return 1
    ceph osd pool create wide 1 1 erasure wide || return 1
    ceph osd pool set wide allow_ec_optimizations true || return 1
    ceph osd erasure-code-profile set small plugin=jerasure k=2 m=1 \
        crush-failure-domain=osd || return 1
    ceph osd crush rule create-erasure two_zones small 2 || return 1
    expect_failure $dir "a PG can have at most 128 OSDs" \
        ceph osd pool set wide num_zones 2 --crush_rule two_zones || return 1
    test "$(pool_field wide size)" = 65 || return 1
    test "$(pool_field wide num_zones)" = 1 || return 1
    rule_exists two_zones || return 1
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = false || return 1
}

# A pool over zones is a FastEC pool, which a default profile with a stripe
# unit that is not a multiple of 4096 cannot give, even with
# --yes-i-really-mean-it; nothing is left behind
function TEST_odd_stripe_unit_default_over_zones() {
    local dir=$1
    two_zone_cluster $dir || return 1

    ceph osd erasure-code-profile set default plugin=isa k=2 m=1 \
        stripe_unit=2048 --force --yes-i-really-mean-it || return 1
    expect_failure $dir "stripe_unit should be a multiple of 4096 bytes" \
        ceph osd pool create e erasure --num_zones 2 || return 1
    expect_failure $dir "a pool over zones is a FastEC pool" \
        ceph osd pool create e erasure --num_zones 2 --yes-i-really-mean-it || return 1
    ! ceph osd pool ls | grep -qx e || return 1
    ! ceph osd erasure-code-profile ls | grep -qx e-k2-m1 || return 1
    ! rule_exists e || return 1
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = false || return 1
}

# pool default set changes only the defaults: no pool, and not stretch mode
function TEST_default_set_num_zones_changes_nothing() {
    local dir=$1
    two_zone_cluster $dir || return 1

    ceph osd pool create old || return 1
    ceph osd pool default set --num_zones 2 --replica 2 || return 1
    test "$(pool_field old num_zones)" = 1 || return 1
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = false || return 1
    test "$(ceph mon dump -f json | jq .stretch_mode)" = false || return 1
    # the next pool create follows them
    ceph osd pool create new || return 1
    test "$(pool_field new num_zones)" = 2 || return 1
    test "$(pool_field new size)" = 4 || return 1
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = true || return 1
    test "$(pool_field old num_zones)" = 1 || return 1
}

# create-erasure takes the zone type of a rule over zones from the profile,
# else from osd_pool_default_zone_failure_domain, as pool creation does: a
# profile has crush-zone-failure-domain only if it is given
function TEST_create_erasure_zone_type() {
    local dir=$1
    two_zone_cluster $dir || return 1

    ceph osd erasure-code-profile set small plugin=jerasure k=2 m=1 \
        crush-failure-domain=osd || return 1
    ! ceph osd erasure-code-profile get small | grep crush-zone-failure-domain || return 1
    ceph config set mon osd_pool_default_zone_failure_domain host || return 1
    expect_failure $dir "for type host is not equal to num_failure_domains 2" \
        ceph osd crush rule create-erasure by_option small 2 || return 1
    ceph config set mon osd_pool_default_zone_failure_domain datacenter || return 1
    ceph osd crush rule create-erasure by_option small 2 || return 1
    test "$(rule_zone_type by_option)" = datacenter || return 1

    ceph osd erasure-code-profile set dcs plugin=jerasure k=2 m=1 \
        crush-failure-domain=osd crush-zone-failure-domain=datacenter || return 1
    ceph config set mon osd_pool_default_zone_failure_domain host || return 1
    ceph osd crush rule create-erasure by_profile dcs 2 || return 1
    test "$(rule_zone_type by_profile)" = datacenter || return 1
}

# num_zones takes the zone type of a pool in one zone from
# --zone_failure_domain, else from osd_pool_default_zone_failure_domain, not
# from an erasure coded pool's profile
function TEST_set_num_zones_erasure_zone_type() {
    local dir=$1
    two_zone_cluster $dir || return 1

    ceph osd erasure-code-profile set hosts plugin=isa k=2 m=1 \
        crush-failure-domain=osd crush-zone-failure-domain=host || return 1
    ceph osd pool create e erasure hosts || return 1
    ceph osd pool set e allow_ec_optimizations true || return 1
    ceph osd pool set e num_zones 2 || return 1
    test "$(rule_zone_type e-stretch)" = datacenter || return 1
}

# num_zones keeps the placement of the pool's rule, not that of the defaults
# or of an erasure coded pool's profile; the command line changes it
function TEST_num_zones_placement_from_rule() {
    local dir=$1
    two_zone_cluster $dir || return 1
    ceph osd crush rm-device-class $(seq 0 5) || return 1
    ceph osd crush set-device-class ssd $(seq 0 5) || return 1

    ceph osd pool create r 4 --class ssd --osd_failure_domain osd || return 1
    ceph osd pool set r num_zones 2 --replica 2 || return 1
    test "$(rule_takes r)" = default~ssd || return 1
    test "$(rule_zone_type r)" = datacenter,osd || return 1
    ceph osd pool set r num_zones 1 --root dc1 || return 1
    test "$(rule_takes r-single-zone)" = dc1~ssd || return 1
    test "$(rule_leaf_type r-single-zone)" = osd || return 1

    ceph osd erasure-code-profile set plain plugin=isa k=2 m=1 \
        crush-failure-domain=host || return 1
    ceph osd erasure-code-profile set ssds plugin=isa k=2 m=1 \
        crush-failure-domain=osd crush-device-class=ssd || return 1
    ceph osd crush rule create-erasure ssd_osds ssds || return 1
    ceph osd pool create e 4 4 erasure plain ssd_osds || return 1
    ceph osd pool set e allow_ec_optimizations true || return 1
    ceph osd pool set e num_zones 2 || return 1
    test "$(rule_takes e-stretch)" = default~ssd || return 1
    test "$(rule_leaf_type e-stretch)" = osd || return 1
    ceph osd pool set e num_zones 1 --no-class --yes-i-really-mean-it || return 1
    test "$(rule_takes e-single-zone)" = default || return 1
    test "$(rule_leaf_type e-single-zone)" = osd || return 1
}

# A pool whose rule's placement cannot be read changes num_zones only with
# --crush_rule, or with all of --root, --osd_failure_domain and --class or
# --no-class
function TEST_num_zones_rule_not_read() {
    local dir=$1
    two_zone_cluster $dir || return 1
    ceph osd getcrushmap > $dir/crushmap || return 1
    crushtool --decompile $dir/crushmap > $dir/crushmap.txt || return 1
    cat >> $dir/crushmap.txt << EOF
rule two_per_host {
        id 10
        type replicated
        step take default
        step choose firstn 0 type host
        step chooseleaf firstn 2 type osd
        step emit
}
EOF
    crushtool --compile $dir/crushmap.txt -o $dir/crushmap.bin || return 1
    ceph osd setcrushmap -i $dir/crushmap.bin || return 1
    ceph osd pool create r 4 --rule two_per_host || return 1
    ceph osd pool create r2 4 --rule two_per_host || return 1

    expect_failure $dir "pool 'r': cannot read the placement of its CRUSH rule: rule 'two_per_host' does not take the root and choose OSDs" \
        ceph osd pool set r num_zones 2 --replica 2 --root default \
        --osd_failure_domain host || return 1
    test "$(pool_field r num_zones)" = 1 || return 1
    ceph osd pool set r num_zones 2 --replica 2 --root default \
        --osd_failure_domain host --no-class || return 1
    test "$(rule_takes r)" = default || return 1
    test "$(rule_leaf_type r)" = host || return 1
    ceph osd pool set r2 num_zones 2 --replica 2 --crush_rule r || return 1
    test "$(ceph osd pool get r2 crush_rule -f json | jq -r .crush_rule)" = r || return 1
}

# --root confines a pool to a subtree, and --class to a device class, also
# when replica changes
function TEST_root_and_class_rules() {
    local dir=$1
    two_zone_cluster $dir || return 1
    ceph osd crush rm-device-class $(seq 0 5) || return 1
    ceph osd crush set-device-class ssd $(seq 0 5) || return 1

    ceph osd pool create e erasure --k 2 --m 1 --root dc1 --num_zones 1 || return 1
    test "$(rule_takes e)" = dc1 || return 1
    ceph osd pool create r --num_zones 2 --replica 2 --class ssd || return 1
    test "$(rule_takes r)" = default~ssd || return 1
    ceph osd pool set r replica 3 || return 1
    test "$(rule_takes r-replica-3)" = default~ssd || return 1
}

# A pool reduced to one zone gets a rule of the placement that num_zones 1
# is given (--root, --osd_failure_domain, --class), for replicated and
# erasure coded pools; --no-class drops a default class
function TEST_shrink_to_one_zone_placement() {
    local dir=$1
    two_zone_cluster $dir || return 1
    ceph osd crush rm-device-class $(seq 0 5) || return 1
    ceph osd crush set-device-class ssd $(seq 0 5) || return 1

    for pool in r_root r_host r_cls r_plain; do
        ceph osd pool create $pool 4 --num_zones 2 --replica 2 || return 1
    done
    ceph osd pool set r_root num_zones 1 --root dc1 --yes-i-really-mean-it || return 1
    test "$(rule_takes r_root-single-zone)" = dc1 || return 1
    test "$(ceph osd pool get r_root crush_rule -f json | jq -r .crush_rule)" = r_root-single-zone || return 1
    # the default rule here chooses OSDs, so host needs a rule of its own
    ceph osd pool set r_host num_zones 1 --osd_failure_domain host --yes-i-really-mean-it || return 1
    test "$(rule_leaf_type r_host-single-zone)" = host || return 1
    ceph osd pool set r_cls num_zones 1 --class ssd --yes-i-really-mean-it || return 1
    test "$(rule_takes r_cls-single-zone)" = default~ssd || return 1
    # with the cluster's default placement the pool takes the default rule
    ceph osd pool set r_plain num_zones 1 --yes-i-really-mean-it || return 1
    ! rule_exists r_plain-single-zone || return 1

    for pool in e_root e_cls; do
        ceph osd pool create $pool 4 erasure --num_zones 2 --k 2 --m 1 || return 1
    done
    ceph osd pool set e_root num_zones 1 --root dc2 --yes-i-really-mean-it || return 1
    test "$(rule_takes e_root-single-zone)" = dc2 || return 1
    ceph osd pool set e_cls num_zones 1 --class ssd --yes-i-really-mean-it || return 1
    test "$(rule_takes e_cls-single-zone)" = default~ssd || return 1

    ceph config set global osd_pool_default_class ssd || return 1
    for pool in r_nc r_both; do
        ceph osd pool create $pool 4 --num_zones 2 --replica 2 || return 1
    done
    ceph osd pool set r_nc num_zones 1 --no-class --yes-i-really-mean-it || return 1
    test "$(rule_takes $(ceph osd pool get r_nc crush_rule -f json | jq -r .crush_rule))" = default || return 1
    expect_failure $dir "cannot specify both --class and --no-class" \
        ceph osd pool set r_both num_zones 1 --class ssd --no-class --yes-i-really-mean-it || return 1
    # an EC pool whose own profile carries the default class drops it too
    ceph osd pool default set --pool_type erasure --class ssd || return 1
    ceph osd pool default set --pool_type replicated || return 1
    ceph osd pool create e_nc 4 erasure --num_zones 2 --k 2 --m 1 || return 1
    ceph osd erasure-code-profile get e_nc-k2-m1 | grep -q 'crush-device-class=ssd' || return 1
    ceph osd pool set e_nc num_zones 1 --no-class --yes-i-really-mean-it || return 1
    test "$(rule_takes e_nc-single-zone)" = default || return 1
    ceph osd pool create e_both 4 erasure --num_zones 2 --k 2 --m 1 || return 1
    expect_failure $dir "cannot specify both --class and --no-class" \
        ceph osd pool set e_both num_zones 1 --class ssd --no-class --yes-i-really-mean-it || return 1
    # a rule built from a profile still takes the profile's class
    ceph osd erasure-code-profile set p_ssd k=2 m=1 crush-device-class=ssd \
        crush-failure-domain=osd || return 1
    ceph osd crush rule create-erasure r_ssd p_ssd || return 1
    test "$(rule_takes r_ssd)" = default~ssd || return 1
    ceph config rm global osd_pool_default_class || return 1
}

# A rule goes when the change of a pool's rule leaves it unused, and stays
# while another pool uses it
function TEST_rule_lifetime_on_pool_changes() {
    local dir=$1
    two_zone_cluster $dir || return 1
    ceph osd pool default set --pool_type erasure --k 2 --m 1 || return 1
    ceph osd pool default set --pool_type replicated || return 1

    ceph osd pool create e1 erasure || return 1
    ceph osd pool create e2 erasure --rule e1 || return 1
    for pool in e1 e2; do
        ceph osd pool set $pool allow_ec_optimizations true || return 1
        test "$(ceph osd pool get $pool crush_rule -f json | jq -r .crush_rule)" = e1 || return 1
    done
    ceph osd pool set e1 num_zones 2 || return 1
    rule_exists e1-stretch || return 1
    rule_exists e1 || return 1
    ceph osd pool set e2 num_zones 2 || return 1
    ! rule_exists e1 || return 1
    ceph osd pool set e1 num_zones 1 --yes-i-really-mean-it || return 1
    ! rule_exists e1-stretch || return 1
    rule_exists e1-single-zone || return 1

    ceph osd pool create r --num_zones 2 --replica 2 || return 1
    rule_exists r || return 1
    ceph osd pool set r replica 3 || return 1
    rule_exists r-replica-3 || return 1
    ! rule_exists r || return 1
}

# The rule osd_pool_default_crush_rule names is kept, and replicated_rule
# goes with its last pool; with no default replicated rule, single-zone
# replicated pools cannot be created
function TEST_default_replicated_rule_lifetime() {
    local dir=$1
    local -x CEPH_ARGS="$(one_mon_args)"
    run_mon $dir a --public-addr $CEPH_MON_A || return 1

    ceph osd crush rule create-replicated r default osd || return 1
    ceph osd crush rule create-replicated r2 default osd || return 1
    ceph osd pool default set --rule r || return 1
    ceph osd pool create q --rule replicated_rule || return 1
    ceph osd pool create p || return 1
    test "$(ceph osd pool get p crush_rule -f json | jq -r .crush_rule)" = r || return 1
    ceph osd pool set q crush_rule r || return 1
    ! rule_exists replicated_rule || return 1
    ceph osd pool set p crush_rule r2 || return 1
    ceph osd pool set q crush_rule r2 || return 1
    rule_exists r || return 1

    ceph config set mon osd_pool_default_crush_rule 99 || return 1
    expect_failure $dir "No suitable CRUSH rule exists" \
        ceph osd pool create s || return 1
    ! ceph osd pool ls | grep -qx s || return 1
}

# With mon_fake_pool_delete a deleted pool is only renamed, and keeps its
# profile and rule
function TEST_fake_pool_delete_keeps_profile_and_rule() {
    local dir=$1
    local -x CEPH_ARGS="$(one_mon_args)"
    run_mon $dir a --public-addr $CEPH_MON_A --mon_fake_pool_delete=true || return 1

    ceph osd pool create e erasure --k 2 --m 1 || return 1
    local id=$(ceph osd pool ls detail -f json | jq '.[]|select(.pool_name=="e")|.pool_id')
    # this flag, unlike --yes-i-really-really-mean-it, leaves the deletion
    # to mon_fake_pool_delete
    ceph osd pool rm e e --yes-i-really-really-mean-it-not-faking || return 1
    test "$(pool_field e.$id.DELETED erasure_code_profile)" = '"e-k2-m1"' || return 1
    ceph osd erasure-code-profile ls | grep -qx e-k2-m1 || return 1
    rule_exists e || return 1
}

# An error of building the rule of a pool over zones names the defaults that
# gave its values
function TEST_rule_errors_name_defaults() {
    local dir=$1
    two_zone_cluster $dir 2 || return 1

    expect_failure $dir "requires at least 3 hosts (osd_pool_default_size, osd_pool_default_zone_failure_domain, osd_pool_default_osd_failure_domain, osd_pool_default_root)" \
        ceph osd pool create p --num_zones 2 || return 1
    expect_failure $dir "requires at least 3 hosts (osd_pool_default_zone_failure_domain, osd_pool_default_osd_failure_domain, osd_pool_default_root)" \
        ceph osd pool create p --num_zones 2 --replica 3 || return 1
    ! ceph osd pool ls | grep -qx p || return 1
}

main mon-stretch-pool-zone-commands "$@"
