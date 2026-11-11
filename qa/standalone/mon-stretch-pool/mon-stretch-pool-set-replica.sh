#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON_A="127.0.0.1:7219" # git grep '\<7219\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7224" # git grep '\<7224\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7225" # git grep '\<7225\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "

    export BASE_CEPH_ARGS=$CEPH_ARGS

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        # each test adds monitors to the --mon-host it is given
        CEPH_ARGS="$BASE_CEPH_ARGS --mon-host=$CEPH_MON_A"
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function assert_pool_rule() {
    local poolname=$1
    local expected_name=$2
    local expected_replica=$3
    local expected_min_size=$4
    local expected_size=$((2 * expected_replica))

    local pool_rule_name
    pool_rule_name=$(ceph osd pool get "$poolname" crush_rule |
        awk '/crush_rule:/ { print $2 }') || return 1
    test "$pool_rule_name" = "$expected_name" || {
        echo "pool uses CRUSH rule $pool_rule_name; expected $expected_name"
        return 1
    }

    local actual_replica
    actual_replica=$(ceph osd crush rule dump "$expected_name" --format json |
        jq -r '[.steps[] | select(.op == "chooseleaf_firstn") | .num][0]') || return 1
    test "$actual_replica" = "$expected_replica" || {
        echo "$expected_name chooses $actual_replica replicas per zone; expected $expected_replica"
        return 1
    }

    local actual_size
    actual_size=$(ceph osd pool get "$poolname" size |
        awk '/size:/ { print $2 }') || return 1
    test "$actual_size" = "$expected_size" || {
        echo "pool size is $actual_size; expected $expected_size"
        return 1
    }

    local actual_min_size
    actual_min_size=$(ceph osd pool get "$poolname" min_size |
        awk '/min_size:/ { print $2 }') || return 1
    test "$actual_min_size" = "$expected_min_size" || {
        echo "pool min_size is $actual_min_size; expected $expected_min_size"
        return 1
    }
}

# osd pool set <pool> num_zones and osd pool set <pool> replica change the
# number of zones and of copies in each, on rules named after the pool. The
# zones are zone-a and zone-b, of type zone, with 3 hosts each.
# num_zones 2 refuses a --replica of 0, 128 or 256, and num_zones 3 is
# refused. With --replica 2, replicated pool replica_name_test gets a rule
# named after it with 2 copies in each zone, size 4 and min_size 1; min_size
# counts the copies in each zone, so 3 is refused and 2 accepted. num_zones
# 2 again changes nothing. replica 3 moves the pool to <pool>-replica-3, a
# rule that already exists with 3 hosts in each zone, and replica 2 to a new
# rule, <pool>-replica-2. The rule made for pool renamed goes when the pool,
# renamed renamed2, is deleted. num_zones 1 refuses --replica 200, needs
# --yes-i-really-mean-it for --replica 1 and is refused while nosizechange is
# set; then the pool is on replicated_rule with size 3 and min_size 2, and no
# peering zones. An erasure coded pool of k=2 m=1 is refused num_zones 2
# until allow_ec_optimizations is set; then it gets <pool>-stretch, size 6,
# and with num_zones 1 <pool>-single-zone, which has no step choosing zones.
function TEST_pool_replica_crush_rule_name() {
    local dir=$1
    local poolname=replica_name_test

    run_mon $dir a --public-addr "$CEPH_MON_A" \
        --osd_pool_default_min_size=0 || return 1
    wait_for_quorum 300 1 || return 1

    run_mon $dir b --public-addr "$CEPH_MON_B" \
        --osd_pool_default_min_size=0 || return 1
    CEPH_ARGS="$BASE_CEPH_ARGS --mon-host=$CEPH_MON_A,$CEPH_MON_B"
    wait_for_quorum 300 2 || return 1

    run_mon $dir c --public-addr "$CEPH_MON_C" \
        --osd_pool_default_min_size=0 || return 1
    CEPH_ARGS="$BASE_CEPH_ARGS --mon-host=$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"
    wait_for_quorum 300 3 || return 1

    for osd in $(seq 0 5); do
        run_osd $dir "$osd" || return 1
    done

    for zone in zone-a zone-b; do
        ceph osd crush add-bucket "$zone" zone || return 1
        ceph osd crush move "$zone" root=default || return 1
    done
    for host in zone-a-host-1 zone-a-host-2 zone-a-host-3; do
        ceph osd crush add-bucket "$host" host || return 1
        ceph osd crush move "$host" zone=zone-a || return 1
    done
    for host in zone-b-host-1 zone-b-host-2 zone-b-host-3; do
        ceph osd crush add-bucket "$host" host || return 1
        ceph osd crush move "$host" zone=zone-b || return 1
    done
    for osd in $(seq 0 2); do
        ceph osd crush move "osd.$osd" host=zone-a-host-$((osd + 1)) || return 1
    done
    for osd in $(seq 3 5); do
        ceph osd crush move "osd.$osd" host=zone-b-host-$((osd - 2)) || return 1
    done

    ceph mon set_location a zone=zone-a || return 1
    ceph mon set_location b zone=zone-b || return 1
    ceph mon set_location c zone=arbiter || return 1

    ceph osd pool create "$poolname" 8 8 replicated || return 1
    for replica in 0 128 256; do
        ! ceph osd pool set "$poolname" num_zones 2 \
            --replica "$replica" --zone_failure_domain zone \
            --osd_failure_domain host > "$dir/error.txt" 2>&1 || return 1
        grep -E 'not in range|between 1 and 10|maximum supported value' \
            "$dir/error.txt" || return 1
    done
    ! ceph osd pool set "$poolname" num_zones 3 \
        > "$dir/error.txt" 2>&1 || return 1
    grep 'num_zones must be 1 or 2' "$dir/error.txt" || return 1
    ceph osd pool set "$poolname" num_zones 2 \
        --replica 2 --zone_failure_domain zone --osd_failure_domain host || return 1
    # With osd_pool_default_min_size=0, min_size is replica - floor(replica / 2).
    # This gives min_size 1 for replica 2 and 2 for replica 3.
    assert_pool_rule "$poolname" "$poolname" 2 1 || return 1

    # min_size counts the copies in each zone
    ! ceph osd pool set "$poolname" min_size 3 > "$dir/error.txt" 2>&1 || return 1
    grep 'must be between 1 and replica, which is set to 2' "$dir/error.txt" || return 1
    ceph osd pool set "$poolname" min_size 2 || return 1
    assert_pool_rule "$poolname" "$poolname" 2 2 || return 1
    ceph osd pool set "$poolname" min_size 1 || return 1

    ceph osd pool set "$poolname" num_zones 2 || return 1
    assert_pool_rule "$poolname" "$poolname" 2 1 || return 1
    ! ceph osd pool set "$poolname" num_zones 3 \
        > "$dir/error.txt" 2>&1 || return 1
    grep 'num_zones must be 1 or 2' "$dir/error.txt" || return 1
    assert_pool_rule "$poolname" "$poolname" 2 1 || return 1

    # Exercise reuse of a committed rule, rather than only rule creation.
    add_stretch_rule $dir "$poolname-replica-3" default zone 2 host 3 || return 1
    ceph osd pool set "$poolname" replica 3 || return 1
    assert_pool_rule "$poolname" "$poolname-replica-3" 3 2 || return 1

    ceph osd pool set "$poolname" replica 2 || return 1
    assert_pool_rule "$poolname" "$poolname-replica-2" 2 1 || return 1

    # a generated rule goes with its pool, whatever the pool is called now
    ceph osd pool create renamed 8 8 replicated || return 1
    ceph osd pool set renamed num_zones 2 \
        --replica 2 --zone_failure_domain zone --osd_failure_domain host || return 1
    ceph osd crush rule ls | grep -qx renamed || return 1
    ceph osd pool rename renamed renamed2 || return 1
    ceph osd pool delete renamed2 renamed2 --yes-i-really-really-mean-it || return 1
    ! ceph osd crush rule ls | grep -qx renamed || return 1

    ceph osd pool set "$poolname" num_zones 2 || return 1
    assert_pool_rule "$poolname" "$poolname-replica-2" 2 1 || return 1

    ! ceph osd pool set "$poolname" num_zones 1 --replica 200 \
        > "$dir/error.txt" 2>&1 || return 1
    grep 'replica must be between 1 and 10' "$dir/error.txt" || return 1
    # one copy needs the same confirmation as osd pool set size 1
    ! ceph osd pool set "$poolname" num_zones 1 --replica 1 \
        > "$dir/error.txt" 2>&1 || return 1
    grep 'yes-i-really-mean-it' "$dir/error.txt" || return 1
    # num_zones changes the size, which nosizechange forbids
    ceph osd pool set "$poolname" nosizechange true || return 1
    ! ceph osd pool set "$poolname" num_zones 1 > "$dir/error.txt" 2>&1 || return 1
    grep 'unset nosizechange flag' "$dir/error.txt" || return 1
    ceph osd pool set "$poolname" nosizechange false || return 1
    ceph osd pool set "$poolname" num_zones 1 || return 1
    ceph osd pool get "$poolname" all --format json > "$dir/pool.json" || return 1
    jq -e '.num_zones == 1 and .replica == 3 and .size == 3 and
        .min_size == 2 and .crush_rule == "replicated_rule"' \
        "$dir/pool.json" || return 1
    ceph osd dump --format json > "$dir/osd.json" || return 1
    jq -e --arg pool "$poolname" '.pools[] | select(.pool_name == $pool) |
        .peering_crush_bucket_count == 0 and .peering_crush_bucket_target == 0' \
        "$dir/osd.json" || return 1
    ceph osd pool set "$poolname" num_zones 1 || return 1
    ceph osd crush rule dump replicated_rule --format json > "$dir/rule.json" || return 1

    local ec_poolname=replica_ec_transition_test
    ceph config set global osd_pool_default_flag_ec_optimizations false || return 1
    ceph osd pool create "$ec_poolname" --pool_type erasure --k 2 --m 1 \
        --pg_num 8 --pgp_num 8 \
        --zone_failure_domain zone --osd_failure_domain host || return 1
    # a legacy EC pool must be converted to FastEC first
    expect_failure $dir "is a legacy EC pool" \
        ceph osd pool set "$ec_poolname" num_zones 2 \
        --zone_failure_domain zone || return 1
    ceph osd pool set "$ec_poolname" allow_ec_optimizations true || return 1
    ceph osd pool set "$ec_poolname" num_zones 2 \
        --zone_failure_domain zone || return 1
    ceph osd pool get "$ec_poolname" all --format json > "$dir/ec-pool.json" || return 1
    jq -e --arg rule "$ec_poolname-stretch" \
        '.num_zones == 2 and .size == 6 and .min_size == 2 and
        .crush_rule == $rule' "$dir/ec-pool.json" || return 1
    ceph osd pool set "$ec_poolname" num_zones 2 || return 1
    ceph osd pool set "$ec_poolname" num_zones 1 --yes-i-really-mean-it || return 1
    ceph osd pool get "$ec_poolname" all --format json > "$dir/ec-pool.json" || return 1
    jq -e --arg rule "$ec_poolname-single-zone" \
        '.num_zones == 1 and .size == 3 and .min_size == 2 and
        .crush_rule == $rule' "$dir/ec-pool.json" || return 1
    ceph osd crush rule dump "$ec_poolname-single-zone" --format json \
        > "$dir/ec-rule.json" || return 1
    jq -e '[.steps[] | select(.op == "choose_firstn" or .op == "choose_indep")] |
        length == 0' \
        "$dir/ec-rule.json" || return 1
}

# osd pool set <pool> replica, for a pool in one zone, sets its size, with
# the limits of osd pool set size: replica 11 is refused, and replica 1 needs
# --yes-i-really-mean-it, though run_mon allows pools of one copy. With it,
# size and min_size are 1. The reply to replica 2 has its two parts
# separated ("size set to 2; set pool ...").
function TEST_pool_replica_limits() {
    local dir=$1

    run_mon $dir a --public-addr "$CEPH_MON_A" || return 1
    wait_for_quorum 300 1 || return 1

    ceph osd pool create single 8 8 replicated || return 1
    # replica of a single-zone pool is its size: the same limits apply
    ! ceph osd pool set single replica 11 > "$dir/error.txt" 2>&1 || return 1
    grep 'must be between 1 and 10' "$dir/error.txt" || return 1
    # run_mon allows pools of one copy: one still needs --yes-i-really-mean-it
    ! ceph osd pool set single replica 1 > "$dir/error.txt" 2>&1 || return 1
    grep 'yes-i-really-mean-it' "$dir/error.txt" || return 1
    ceph osd pool set single replica 1 --yes-i-really-mean-it || return 1
    test "$(ceph osd pool get single size | awk '{print $2}')" = 1 || return 1
    # as with size, min_size is reset to its default for the new size
    test "$(ceph osd pool get single min_size | awk '{print $2}')" = 1 || return 1
    # the reply's parts are separated
    ceph osd pool set single replica 2 > "$dir/out.txt" 2>&1 || return 1
    grep -F "size set to 2; set pool" "$dir/out.txt" || return 1
}

# osd pool set <pool> replica resets min_size to its default for the new
# size, as osd pool set <pool> size does. Pool single, with min_size set to
# 1, gets min_size 2 from size 4, and, with min_size set to 1 again,
# min_size 3 from replica 5.
function TEST_pool_size_resets_min_size() {
    local dir=$1

    run_mon $dir a --public-addr "$CEPH_MON_A" || return 1
    wait_for_quorum 300 1 || return 1

    ceph osd pool create single 8 8 replicated || return 1
    ceph osd pool set single min_size 1 || return 1
    ceph osd pool set single size 4 || return 1
    test "$(ceph osd pool get single min_size | awk '{print $2}')" = 2 || return 1
    ceph osd pool set single min_size 1 || return 1
    ceph osd pool set single replica 5 || return 1
    test "$(ceph osd pool get single min_size | awk '{print $2}')" = 3 || return 1
}

# --replica, --crush_rule, --zone_failure_domain, --osd_failure_domain,
# --root, --class and --no-class are options of osd pool set <pool>
# num_zones. Any other variable would ignore them, so they are refused with
# it: osd pool set single size 2 is refused with each of them, and with
# --min_size, which osd pool set does not take, and the size is unchanged.
# osd pool set single num_zones 1 --replica 2 --root default is accepted.
function TEST_pool_set_num_zones_options_only() {
    local dir=$1

    run_mon $dir a --public-addr "$CEPH_MON_A" || return 1
    wait_for_quorum 300 1 || return 1

    ceph osd pool create single 8 8 replicated || return 1
    local size=$(ceph osd pool get single size | awk '{print $2}')
    local opt
    for opt in "--replica 2" "--crush_rule replicated_rule" \
               "--zone_failure_domain host" "--osd_failure_domain osd" \
               "--root default" "--class ssd" "--no-class"; do
        ! ceph osd pool set single size 2 $opt > "$dir/error.txt" 2>&1 || return 1
        grep "only for 'ceph osd pool set <pool> num_zones'" "$dir/error.txt" || return 1
    done
    ! ceph osd pool set single size 2 --min_size 1 > "$dir/error.txt" 2>&1 || return 1
    test "$(ceph osd pool get single size | awk '{print $2}')" = $size || return 1
    ceph osd pool set single num_zones 1 --replica 2 --root default || return 1
}

# osd pool set <pool> replica, for a pool over two zones, reads the
# placement of the pool's rule to make a rule with the new number of copies
# in each zone. It can read a rule that takes each zone in turn, as the
# rules written for global stretch mode do: pool p, on two_take (2 hosts in
# each of the zones zone-a and zone-b), moves with replica 1 to p-replica-1,
# with 1 copy in each zone, size 2 and min_size 1. It cannot read
# unlike_zones (2 hosts in zone-a, 2 OSDs in zone-b), so replica 1 is
# refused for pool q, whose size stays 4.
function TEST_pool_replica_two_take_rule() {
    local dir=$1

    run_mon $dir a --public-addr "$CEPH_MON_A" || return 1
    run_mon $dir b --public-addr "$CEPH_MON_B" || return 1
    run_mon $dir c --public-addr "$CEPH_MON_C" || return 1
    CEPH_ARGS="$BASE_CEPH_ARGS --mon-host=$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"
    wait_for_quorum 300 3 || return 1
    # not i, which wait_for_osd sets
    local zone host
    for zone in zone-a zone-b; do
        ceph osd crush add-bucket $zone zone || return 1
        ceph osd crush move $zone root=default || return 1
        for host in $zone-host-1 $zone-host-2; do
            ceph osd crush add-bucket $host host || return 1
            ceph osd crush move $host zone=$zone || return 1
        done
    done
    local osd=0
    for zone in zone-a zone-b; do
        for host in $zone-host-1 $zone-host-2; do
            run_osd $dir $osd || return 1
            ceph osd crush move osd.$osd host=$host || return 1
            osd=$((osd + 1))
        done
    done
    ceph mon set_location a zone=zone-a || return 1
    ceph mon set_location b zone=zone-b || return 1
    ceph mon set_location c zone=arbiter || return 1
    ceph osd getcrushmap > $dir/crushmap || return 1
    crushtool --decompile $dir/crushmap > $dir/crushmap.txt || return 1
    sed 's/^# end crush map$//' $dir/crushmap.txt > $dir/crushmap_modified.txt || return 1
    cat >> $dir/crushmap_modified.txt << EOF
rule two_take {
        id 1
        type replicated
        step take zone-a
        step chooseleaf firstn 2 type host
        step emit
        step take zone-b
        step chooseleaf firstn 2 type host
        step emit
}
rule unlike_zones {
        id 2
        type replicated
        step take zone-a
        step chooseleaf firstn 2 type host
        step emit
        step take zone-b
        step chooseleaf firstn 2 type osd
        step emit
}

# end crush map
EOF
    crushtool --compile $dir/crushmap_modified.txt -o $dir/crushmap.bin || return 1
    ceph osd setcrushmap -i $dir/crushmap.bin || return 1
    ceph osd pool create p 8 8 replicated || return 1
    ceph osd pool set p num_zones 2 --crush_rule two_take --replica 2 \
        --zone_failure_domain zone || return 1

    ceph osd pool set p replica 1 || return 1
    assert_pool_rule p p-replica-1 1 1 || return 1

    ceph osd pool create q 8 8 replicated || return 1
    ceph osd pool set q num_zones 2 --crush_rule unlike_zones --replica 2 \
        --zone_failure_domain zone || return 1
    ! ceph osd pool set q replica 1 > "$dir/error.txt" 2>&1 || return 1
    grep "pool 'q': cannot read the placement of its CRUSH rule to change its replica" \
        "$dir/error.txt" || return 1
    test "$(ceph osd pool get q size | awk '{print $2}')" = 4 || return 1
}

main mon-stretch-pool-set-replica "$@"
