#!/usr/bin/env bash

set -euo pipefail

scenario=${POOL_FIELD_SCENARIO:?POOL_FIELD_SCENARIO must be set}
zone_type=${POOL_FIELD_ZONE_TYPE:-datacenter}
pool=pool_fields_upgrade
rule=pool_fields_upgrade_rule
tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT

pool_map=$(ceph osd pool ls detail -f json)

# the commit records itself in the OSDMap
if ! ceph osd dump -f json | jq -r .flags | grep -q stretch_zones; then
    echo "the upgrade commit did not set the OSDMap flag stretch_zones"
    exit 1
fi

# the objects of pool p as the setup workunit wrote them
check_objects() {
    local p=$1 i
    for i in {1..10}; do
        rados -p "$p" get "upgrade-obj$i" "$tmpdir/got"
        cmp "$tmpdir/got" <(seq "$((i * 1000))" | sed "s/^/$p /")
    done
}

wait_for_clean() {
    local i
    for i in $(seq 180); do
        ceph pg stat -f json | jq -e '.pg_summary | .num_pgs > 0 and
            ([.num_pg_by_state[] | select(.name == "active+clean") | .num] |
             add) == .num_pgs' > /dev/null && return 0
        sleep 5
    done
    ceph pg stat
    return 1
}

rule_of() {
    ceph osd pool get "$1" crush_rule -f json | jq -r .crush_rule
}

has_rule() {
    ceph osd crush rule ls | grep -qx "$1"
}

# command "$@" must fail with expected in its error
expect_refusal() {
    local expected=$1
    shift
    if "$@" 2> "$tmpdir/err"; then
        echo "not refused: $*"
        exit 1
    fi
    if ! grep -q -- "$expected" "$tmpdir/err"; then
        echo "refused without '$expected': $*"
        cat "$tmpdir/err"
        exit 1
    fi
}

no_rule() {
    if has_rule "$1"; then
        echo "rule $1 was not removed"
        exit 1
    fi
}

check_pool_fields() {
    local zones=$1 replica=$2 size=$3 min_size=$4
    if ! jq -e \
        --arg pool "$pool" \
        --argjson zones "$zones" \
        --argjson replica "$replica" \
        --argjson size "$size" \
        --argjson min_size "$min_size" \
        'any(.[]; .pool_name == $pool and .num_zones == $zones and
            .replica == $replica and .size == $size and .min_size == $min_size)' \
        <<< "$pool_map"; then
        jq --arg pool "$pool" '.[] | select(.pool_name == $pool) |
            {pool_name, num_zones, replica, size, min_size}' <<< "$pool_map"
        exit 1
    fi
}

case "$scenario" in
    2az)
        check_pool_fields 2 2 4 1
        # the pool keeps its rule, and its zones the bucket type it had
        test "$(rule_of "$pool")" = "$rule"
        type_id=$(ceph osd crush dump -f json |
            jq --arg t "$zone_type" '.types[] | select(.name == $t) | .type_id')
        jq -e --arg pool "$pool" --argjson t "$type_id" \
            'any(.[]; .pool_name == $pool and .peering_crush_bucket_barrier == $t)' \
            <<< "$pool_map"
        test "$(ceph config get mon osd_pool_default_zone_failure_domain)" = "$zone_type"
        wait_for_clean
        check_objects "$pool"
        ;;
    ec)
        # the commit changes no erasure coded pool: each keeps its rule and
        # placement, and the plain pools get copies of the default profile
        test "$(rule_of ec_fast)" = ec_fast
        test "$(rule_of ec_shared1)" = erasure-code
        test "$(rule_of ec_shared2)" = erasure-code
        test "$(rule_of ec_hand)" = ec_hand_rule
        ceph osd pool get ec_fast erasure_code_profile | grep -qx 'erasure_code_profile: fast22'
        ceph osd pool get ec_shared1 erasure_code_profile | grep -q 'erasure_code_profile: ec_shared1-'
        wait_for_clean
        for p in ec_fast ec_shared1 ec_shared2 ec_legacy ec_hand; do
            check_objects "$p"
        done

        # then num_zones 2 refuses the legacy EC pool, and for ec_hand,
        # whose rule's placement cannot be read in one zone, needs the
        # placement given; each rule goes with its last pool
        expect_refusal "is a legacy EC pool" \
            ceph osd pool set ec_legacy num_zones 2
        ceph osd pool set ec_fast num_zones 2
        no_rule ec_fast
        ceph osd pool set ec_shared1 num_zones 2
        has_rule erasure-code
        ceph osd pool set ec_shared2 num_zones 2
        no_rule erasure-code
        expect_refusal "cannot read the placement of its CRUSH rule" \
            ceph osd pool set ec_hand num_zones 2
        ceph osd pool set ec_hand num_zones 2 --root default \
            --osd_failure_domain host --no-class
        no_rule ec_hand_rule
        for p in ec_fast ec_shared1 ec_shared2 ec_hand; do
            test "$(ceph osd pool get "$p" num_zones -f json | jq .num_zones)" = 2
            test "$(rule_of "$p")" = "$p-stretch"
        done
        wait_for_clean
        for p in ec_fast ec_shared1 ec_shared2 ec_legacy ec_hand; do
            check_objects "$p"
        done
        ;;
    3az)
        # A stretch pool that was created before the upgrade decodes
        # as num_zones=1 with replica=size by design. The pool keeps
        # its stretch peering fields unchanged.
        check_pool_fields 1 6 6 3

        # Verify the pool stretch mode peering fields survived the upgrade.
        crush_rule_id=$(ceph osd crush rule dump "$rule" --format=json 2>/dev/null \
            | python3 -c 'import sys,json; print(json.load(sys.stdin)["rule_id"])')
        if ! jq -e \
            --arg pool "$pool" \
            --argjson rule_id "$crush_rule_id" \
            'any(.[]; .pool_name == $pool
                and .is_stretch_pool == true
                and .peering_crush_bucket_count == 2
                and .peering_crush_bucket_target == 3
                and .crush_rule == $rule_id)' \
            <<< "$pool_map"; then
            echo "3az: pool stretch mode peering fields did not survive upgrade"
            jq --arg pool "$pool" '.[] | select(.pool_name == $pool) |
                {pool_name, is_stretch_pool, peering_crush_bucket_count,
                 peering_crush_bucket_target, crush_rule}' <<< "$pool_map"
            exit 1
        fi
        ;;
    rules)
        # the commit removes no rule, moves no pool and keeps the weight-sets
        for r in class_hosts erasure-code osds rook_stretch per_zone_one spare; do
            has_rule "$r"
        done
        test "$(rule_of rcls1)" = class_hosts
        test "$(rule_of e1)" = erasure-code
        test "$(rule_of eosd)" = osds
        test "$(rule_of rrook1)" = rook_stretch
        test "$(rule_of runread)" = per_zone_one
        ceph osd crush weight-set ls | grep -qx '(compat)'
        ceph osd crush weight-set ls | grep -qx rcls1
        ceph osd crush weight-set ls | grep -qx rrook1
        all="rcls1 rcls2 e1 e2 eosd rrook1 rrook2 runread"
        wait_for_clean
        for p in $all; do
            check_objects "$p"
        done

        # then num_zones 2 keeps or replaces each rule, or is refused, as its
        # placement allows, and each rule goes with its last pool
        ceph osd pool set rcls1 num_zones 2 --replica 2
        has_rule class_hosts
        ceph osd pool set rcls2 num_zones 2 --replica 2
        no_rule class_hosts
        ceph osd pool set e1 num_zones 2
        has_rule erasure-code
        expect_refusal "is a legacy EC pool" ceph osd pool set e2 num_zones 2
        ceph osd pool set e2 allow_ec_optimizations true
        ceph osd pool set e2 num_zones 2
        no_rule erasure-code
        ceph osd pool set eosd num_zones 2
        no_rule osds
        expect_refusal "cannot read the placement of its CRUSH rule" \
            ceph osd pool set rrook1 num_zones 2 --replica 2
        ceph osd pool set rrook1 num_zones 2 --replica 2 --crush_rule rook_stretch
        ceph osd pool set rrook2 num_zones 2 --replica 2 --root default \
            --osd_failure_domain host --no-class
        has_rule rook_stretch
        expect_refusal "cannot read the placement of its CRUSH rule" \
            ceph osd pool set runread num_zones 2 --replica 2
        ceph osd pool set runread num_zones 2 --replica 2 --root default \
            --osd_failure_domain host --no-class
        no_rule per_zone_one
        has_rule spare
        for p in $all; do
            test "$(ceph osd pool get "$p" num_zones -f json | jq .num_zones)" = 2
        done
        wait_for_clean
        for p in $all; do
            check_objects "$p"
        done
        ;;
    *)
        echo "unknown pool field scenario: $scenario"
        exit 1
        ;;
esac