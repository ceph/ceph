#!/usr/bin/env bash
#
# Existing CRUSH rules of unusual shapes for pools over zones, in pool
# creation, num_zones and enable_stretch_mode, and an erasure coded pool over
# zones given an existing erasure rule: each works or is refused with its
# reason, and nothing is left behind.

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    export CEPH_MON_A="127.0.0.1:7661" # git grep '\<7661\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7662" # git grep '\<7662\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7663" # git grep '\<7663\>' : there must be only one
    export CEPH_MON="$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"
    run_stretch_tests "$@"
}

# add the rules read from stdin to the CRUSH map
function add_rules() {
    local dir=$1
    cat > $dir/rules.txt || return 1
    ceph osd getcrushmap > $dir/crushmap || return 1
    crushtool --decompile $dir/crushmap > $dir/crushmap.txt || return 1
    cat $dir/rules.txt >> $dir/crushmap.txt || return 1
    crushtool --compile $dir/crushmap.txt -o $dir/crushmap.bin || return 1
    ceph osd setcrushmap -i $dir/crushmap.bin || return 1
}

function pool_exists() {
    ceph osd pool ls | grep -qx "$1"
}

# S5: rules that take z2 before z1 place the first copies or shards in z2;
# pools on them go to two zones, back to one, and into global stretch mode
function TEST_zones_in_opposite_order() {
    local dir=$1
    local -x CEPH_ARGS="$CEPH_ARGS"
    zone_cluster $dir zone || return 1
    # a pool created on a rule takes the default zone type
    ceph config set global osd_pool_default_zone_failure_domain zone || return 1
    add_rules $dir << EOF || return 1
rule rev2 {
        id 10
        type replicated
        step take z2
        step chooseleaf firstn 2 type host
        step emit
        step take z1
        step chooseleaf firstn 2 type host
        step emit
}
rule ec_rev {
        id 11
        type erasure
        step take z2
        step chooseleaf indep 4 type host
        step emit
        step take z1
        step chooseleaf indep 4 type host
        step emit
}
EOF
    ceph osd erasure-code-profile set p22 plugin=isa k=2 m=2 \
        crush-failure-domain=host || return 1
    ceph osd pool create r 8 8 replicated rev2 --num_zones 2 --replica 2 || return 1
    ceph osd pool create e 8 8 erasure p22 || return 1
    ceph osd pool set e allow_ec_optimizations true || return 1
    ceph osd pool set e num_zones 2 --crush_rule ec_rev --zone_failure_domain zone || return 1
    rules_are replicated_rule rev2 ec_rev || return 1
    wait_for_clean || return 1
    check_two_zones r zone 2 || return 1
    check_two_zones e zone 4 || return 1
    # the first zone of every PG is z2
    ceph pg ls-by-pool e -f json | jq -e '[.pg_stats[].acting[0:4][] | select(. < 4)] == []' || return 1
    write_pools $dir r e || return 1
    check_without_zone $dir 1 r e || return 1
    wait_for_stretch_state 0 0 || return 1

    ceph mon enable_stretch_mode c rev2 zone || return 1
    test "$(pool_rule .mgr)" = rev2 || return 1
    wait_for_clean || return 1
    check_pools $dir r e || return 1
    # rev2's placement is the stock one, which takes the default replicated
    # rule
    ceph mon disable_stretch_mode --yes-i-really-mean-it || return 1
    rules_are replicated_rule e-single-zone || return 1
    test "$(pool_rule r)" = replicated_rule || return 1
    wait_for_clean || return 1
    check_pools $dir r e || return 1
}

# S5: zones under a bucket that is not the root: a rule that chooses them
# from it, and one that takes each zone, read that bucket as the root
function TEST_zones_under_a_bucket() {
    local dir=$1
    zone_cluster $dir zone || return 1
    ceph config set global osd_pool_default_zone_failure_domain zone || return 1
    ceph osd crush add-bucket site region || return 1
    ceph osd crush move site root=default || return 1
    ceph osd crush move z1 region=site || return 1
    ceph osd crush move z2 region=site || return 1
    add_hand_rules $dir zone || return 1
    add_rules $dir << EOF || return 1
rule site2 {
        id 20
        type replicated
        step take site
        step choose firstn 2 type zone
        step chooseleaf firstn 2 type host
        step emit
}
EOF
    ceph osd pool create rsite 8 || return 1
    ceph osd pool create rtake 8 || return 1
    ceph osd pool set rsite num_zones 2 --replica 2 --crush_rule site2 \
        --zone_failure_domain zone || return 1
    ceph osd pool set rtake num_zones 2 --replica 2 --crush_rule take2 \
        --zone_failure_domain zone || return 1
    ceph osd pool create rgen 8 --num_zones 2 --replica 2 --root site \
        --zone_failure_domain zone || return 1
    test "$(rule_takes rgen)" = site || return 1
    wait_for_clean || return 1
    for pool in rsite rtake rgen; do
        check_two_zones $pool zone 2 || return 1
    done
    write_pools $dir rsite rtake rgen || return 1

    ceph osd pool set rsite num_zones 1 || return 1
    test "$(rule_takes rsite-single-zone)" = site || return 1
    ceph osd pool set rtake num_zones 1 || return 1
    test "$(rule_takes rtake-single-zone)" = site || return 1
    test "$(rule_leaf_type rtake-single-zone)" = host || return 1
    rules_are replicated_rule choose2 class2 ec_take ec_choose mixed2 hosts_any \
        per_zone_one rsite-single-zone rtake-single-zone rgen || return 1

    # site2 went with rsite
    add_rules $dir << EOF || return 1
rule site2 {
        id 20
        type replicated
        step take site
        step choose firstn 2 type zone
        step chooseleaf firstn 2 type host
        step emit
}
EOF
    ceph mon enable_stretch_mode c site2 zone || return 1
    test "$(pool_rule rgen)" = rgen || return 1
    for pool in .mgr rsite rtake; do
        test "$(pool_rule $pool)" = site2 || return 1
    done
    wait_for_clean || return 1
    check_pools $dir rsite rtake rgen || return 1
}

# S5: z2 has 3 hosts and z1 4. With the zones' weights more than 10% apart
# stretch mode refuses a pool over them; with the weights evened out a pool
# over zones gets at most 3 copies or shards in each zone, and a larger one
# is refused, with no pool, profile or rule left behind
function TEST_uneven_hosts() {
    local dir=$1 osd
    local -x CEPH_ARGS="$CEPH_ARGS"
    zone_cluster $dir zone || return 1
    ceph osd crush add-bucket spare root || return 1
    ceph osd crush move host7 root=spare || return 1
    ceph osd out osd.7 || return 1
    expect_failure $dir "differing weights" \
        ceph osd pool create rep 8 --num_zones 2 --replica 3 --zone_failure_domain zone \
        --osd_failure_domain host || return 1
    ! pool_exists rep || return 1
    rules_are replicated_rule || return 1
    for osd in 4 5 6; do
        ceph osd crush reweight osd.$osd 1.3333 || return 1
    done
    ceph osd pool create rep 8 --num_zones 2 --replica 3 --zone_failure_domain zone \
        --osd_failure_domain host || return 1
    expect_failure $dir "zone z2 has only 3 items of type host; replica 4 per zone requires at least 4 hosts" \
        ceph osd pool create rep4 8 --num_zones 2 --replica 4 --zone_failure_domain zone \
        --osd_failure_domain host || return 1
    ! pool_exists rep4 || return 1
    expect_failure $dir "zone z2 has only 3 items of type host" \
        ceph osd pool create e22 8 erasure --k 2 --m 2 --num_zones 2 \
        --zone_failure_domain zone --osd_failure_domain host || return 1
    ! pool_exists e22 || return 1
    ! ceph osd erasure-code-profile ls | grep -qx e22-k2-m2 || return 1
    ceph osd pool create e21 8 erasure --k 2 --m 1 --num_zones 2 \
        --zone_failure_domain zone --osd_failure_domain host || return 1
    rules_are replicated_rule rep e21 || return 1
    wait_for_clean || return 1
    check_two_zones rep zone 3 || return 1
    check_two_zones e21 zone 3 || return 1
    write_pools $dir rep e21 || return 1
    check_without_zone $dir 2 rep e21 || return 1
    check_without_zone $dir 1 rep e21 || return 1
}

# S5: zones named like bucket types: --root names the bucket, and the bucket
# type of that name, which has no buckets here, is refused as a zone type
function TEST_zone_names_like_types() {
    local dir=$1
    zone_cluster $dir zone || return 1
    ceph osd crush rename-bucket z1 rack || return 1
    ceph osd crush rename-bucket z2 room || return 1
    ceph mon set_location a zone=rack || return 1
    ceph mon set_location b zone=room || return 1
    ceph osd pool create r 8 --num_zones 2 --replica 2 --zone_failure_domain zone || return 1
    ceph osd pool create e 8 erasure --k 2 --m 2 --num_zones 2 \
        --zone_failure_domain zone || return 1
    expect_failure $dir "there are 0rack's in the cluster" \
        ceph osd pool create r2 8 --num_zones 2 --replica 2 --zone_failure_domain rack || return 1
    ! pool_exists r2 || return 1
    wait_for_clean || return 1
    write_pools $dir r e || return 1
    ceph osd pool set r num_zones 1 --root rack || return 1
    test "$(rule_takes r-single-zone)" = rack || return 1
    ceph osd pool set e num_zones 1 --root room --yes-i-really-mean-it || return 1
    test "$(rule_takes e-single-zone)" = room || return 1
    rules_are replicated_rule r-single-zone e-single-zone || return 1
    wait_for_clean || return 1
    check_one_zone r 1 || return 1
    check_one_zone e 2 || return 1
    check_pools $dir r e || return 1
}

# S5: MSR rules cannot be read, and the placement check does not count their
# zones: a pool on one changes num_zones with the whole placement given, and
# one over zones is refused as a rule over zones
function TEST_msr_rules() {
    local dir=$1
    zone_cluster $dir zone || return 1
    ceph osd set-require-min-compat-client squid || return 1
    add_rules $dir << EOF || return 1
rule msr_one {
        id 10
        type msr_indep
        step take default
        step choosemsr 4 type host
        step choosemsr 1 type osd
        step emit
}
rule msr_two {
        id 11
        type msr_indep
        step take default
        step choosemsr 2 type zone
        step choosemsr 4 type host
        step choosemsr 1 type osd
        step emit
}
EOF
    ceph osd erasure-code-profile set p22 plugin=isa k=2 m=2 \
        crush-failure-domain=host || return 1
    ceph osd pool create e 8 8 erasure p22 msr_one || return 1
    ceph osd pool set e allow_ec_optimizations true || return 1
    wait_for_clean || return 1
    write_pools $dir e || return 1
    expect_failure $dir "pool 'e': cannot read the placement of its CRUSH rule: rule 'msr_one' does not take the root and choose OSDs" \
        ceph osd pool set e num_zones 2 --zone_failure_domain zone || return 1
    expect_failure $dir "crush rule msr_two does not place the pool in 2 buckets of type zone" \
        ceph osd pool set e num_zones 2 --crush_rule msr_two --zone_failure_domain zone || return 1
    test "$(pool_field e num_zones)" = 1 || return 1
    rules_are replicated_rule msr_one msr_two || return 1
    ceph osd pool set e num_zones 2 --zone_failure_domain zone --root default \
        --osd_failure_domain host --no-class || return 1
    rules_are replicated_rule msr_two e-stretch || return 1
    wait_for_clean || return 1
    check_two_zones e zone 4 || return 1
    check_pools $dir e || return 1
}

# S6: ceph osd pool set crush_rule gives an erasure coded pool over zones an
# erasure rule that places k+m shards in each zone, chosen or taken, and its
# old rule goes; a replicated rule, a rule of another count and a rule in one
# zone are refused. The same holds in global stretch mode.
function TEST_ec_pool_given_existing_rule() {
    local dir=$1
    zone_cluster $dir zone || return 1
    add_hand_rules $dir zone || return 1
    add_rules $dir << EOF || return 1
rule ec_three {
        id 20
        type erasure
        step take default
        step choose indep 2 type zone
        step chooseleaf indep 3 type host
        step emit
}
EOF
    ceph osd erasure-code-profile set p22 plugin=isa k=2 m=2 \
        crush-failure-domain=host crush-zone-failure-domain=zone || return 1
    ceph osd crush rule create-erasure one_zone p22 || return 1
    ceph osd pool create e 8 erasure p22 --num_zones 2 || return 1
    local hand="take2 choose2 class2 ec_take ec_choose mixed2 hosts_any per_zone_one"
    rules_are replicated_rule $hand ec_three one_zone e || return 1
    wait_for_clean || return 1
    write_pools $dir e || return 1

    ceph osd pool set e crush_rule ec_choose || return 1
    rules_are replicated_rule $hand ec_three one_zone || return 1
    wait_for_clean || return 1
    check_two_zones e zone 4 || return 1
    check_pools $dir e || return 1
    expect_failure $dir "type does not match pool" \
        ceph osd pool set e crush_rule choose2 || return 1
    expect_failure $dir "crush rule ec_three does not place 4 items in each zone" \
        ceph osd pool set e crush_rule ec_three || return 1
    expect_failure $dir "crush rule one_zone does not place the pool in 2 buckets of type zone" \
        ceph osd pool set e crush_rule one_zone || return 1
    test "$(pool_rule e)" = ec_choose || return 1
    rules_are replicated_rule $hand ec_three one_zone || return 1

    ceph mon enable_stretch_mode --dividing_bucket zone || return 1
    ceph osd pool set e crush_rule ec_take || return 1
    rules_are replicated_rule take2 choose2 class2 ec_take mixed2 hosts_any \
        per_zone_one ec_three one_zone .mgr || return 1
    wait_for_clean || return 1
    check_two_zones e zone 4 || return 1
    check_pools $dir e || return 1
}

main mon-stretch-pool-existing-rule-cases "$@"
