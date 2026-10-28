#!/usr/bin/env bash

set -euo pipefail

scenario=${POOL_FIELD_SCENARIO:?POOL_FIELD_SCENARIO must be set}
pool=pool_fields_upgrade
rule=pool_fields_upgrade_rule

pool_map=$(ceph osd pool ls detail -f json)

# the commit records itself in the OSDMap
if ! ceph osd dump -f json | jq -r .flags | grep -q stretch_zones; then
    echo "the upgrade commit did not set the OSDMap flag stretch_zones"
    exit 1
fi

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
    *)
        echo "unknown pool field scenario: $scenario"
        exit 1
        ;;
esac