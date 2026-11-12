#!/usr/bin/env bash

# The second half of the upgrade test that pool-fields-upgrade-setup.sh
# starts, run for the same POOL_FIELD_SCENARIO after the upgrade and
# osd require-osd-release vampire, the upgrade commit. The commit must set
# the OSDMap flag stretch_zones. Then each scenario checks what the commit
# did to its pools and rules and, except in 3az, reads back the objects
# that the setup wrote once the PGs are clean. In ec and rules, osd pool
# set num_zones 2 then moves each pool over two zones, and the objects are
# read back again.

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
        # the commit makes the pool in global stretch mode a pool over two
        # zones of 2 copies each: num_zones 2, replica 2, size 4, min_size 1
        check_pool_fields 2 2 4 1
        # the pool keeps its rule, and its zones the bucket type they had,
        # which is also the value of osd_pool_default_zone_failure_domain
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
        # the commit moves no erasure coded pool to another rule; ec_fast
        # keeps its profile, and ec_shared1, of the default profile, now has
        # a profile of its own, named after it
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

        # then osd pool set num_zones 2 moves each pool over two zones, to a
        # rule of its own, <pool>-stretch. It is refused for ec_legacy, a
        # legacy erasure coded pool, and for ec_hand, whose rule's placement
        # cannot be read, until --root, --osd_failure_domain and --no-class
        # give the whole placement. A rule is removed when its last pool
        # leaves it.
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
        # A pool that osd pool stretch set made before the upgrade decodes
        # as num_zones 1 with replica equal to size, by design: num_zones 1,
        # replica 6, size 6, min_size 3.
        check_pool_fields 1 6 6 3

        # It keeps its rule and the peering_crush_bucket fields that osd
        # pool stretch set gave it.
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
            echo "3az: the individual stretch pool's peering fields did not survive upgrade"
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

        # then osd pool set num_zones 2, with --replica 2 for the replicated
        # pools, moves each pool over two zones. The pools on class_hosts,
        # erasure-code and osds need nothing more, but e2, a legacy erasure
        # coded pool, is refused until allow_ec_optimizations is set. The
        # placement of rook_stretch and per_zone_one cannot be read, so
        # rrook1 and runread are refused until given a rule (rrook1:
        # rook_stretch itself) or the whole placement (runread, and rrook2).
        # A rule is removed when its last pool leaves it; rook_stretch, still
        # used by rrook1, and spare, which no pool uses, stay.
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