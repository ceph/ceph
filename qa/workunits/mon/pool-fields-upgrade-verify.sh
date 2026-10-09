#!/usr/bin/env bash

set -euo pipefail

scenario=${POOL_FIELD_SCENARIO:?POOL_FIELD_SCENARIO must be set}
pool=pool_fields_upgrade

case "$scenario" in
    2az)
        zones=2
        replica=2
        size=4
        min_size=1
        ;;
    3az)
        zones=3
        replica=2
        size=6
        min_size=1
        ;;
    *)
        echo "unknown pool field scenario: $scenario"
        exit 1
        ;;
esac

pool_map=$(ceph osd pool ls detail -f json)
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