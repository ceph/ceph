#!/bin/bash

. $(dirname $0)/../../standalone/ceph-helpers.sh

set -x

_cmp_json_field() {
    local a=$1
    local b=$2
    local field=$3
    local name=$4
    local av
    local bv
    av=$(echo "$a" | jq -c "$field") || return 1
    bv=$(echo "$b" | jq -c "$field") || return 1
    if [ "$av" != "$bv" ]; then
        echo "Mismatch for $name at $field: base=$av vs offset=$bv"
        return 1
    fi
}

_run_pg_json() {
    local pgid=$1
    local subcmd=$2
    local offset_json=$3
    if [ -n "$offset_json" ]; then
        ceph --format json pg "$pgid" "$subcmd" "$offset_json" | awk 'BEGIN{p=0} /^[[:space:]]*[\[{]/{p=1} p{print}'
    else
        ceph --format json pg "$pgid" "$subcmd" | awk 'BEGIN{p=0} /^[[:space:]]*[\[{]/{p=1} p{print}'
    fi
}

_wait_for_pg_query_ok() {
    local pgid=$1
    local timeout=${2:-60}
    local deadline=$(( $(date +%s) + timeout ))

    while [ $(date +%s) -lt $deadline ]; do
        local q
        q=$(_run_pg_json "$pgid" query 2>/dev/null || true)
        if [ -n "$q" ]; then
            echo "$q" | jq -e . >/dev/null 2>&1 && return 0
        fi
        sleep 1
    done

    echo "Timed out waiting for pg $pgid query to succeed"
    ceph -s || true
    return 1
}

_get_any_pgid() {
    ceph pg dump pgs_brief --format json | jq -r '.pg_stats[0].pgid'
}

test_pg_subcommands() {
    [ "$(ceph osd metadata 0 | jq -r '.osd_type')" = "crimson" ] || return 0

    local pgid
    pgid=$(_get_any_pgid) || return 1
    [ -n "$pgid" ] || return 1

    _wait_for_pg_query_ok "$pgid" 120 || return 1

    local qjson
    qjson=$(_run_pg_json "$pgid" query) || return 1
    echo "$qjson" | jq -e . >/dev/null || return 1

    local ujson ujson_offset
    ujson=$(_run_pg_json "$pgid" list_unfound) || return 1
    echo "$ujson" | jq -e . >/dev/null || return 1

    ujson_offset=$(_run_pg_json "$pgid" list_unfound '{}') || return 1
    echo "$ujson_offset" | jq -e . >/dev/null || return 1

    _cmp_json_field "$ujson" "$ujson_offset" '(.missing.num_missing // .num_missing // 0)' "list_unfound.num_missing" || return 1
    _cmp_json_field "$ujson" "$ujson_offset" '(.missing.num_unfound // .num_unfound // 0)' "list_unfound.num_unfound" || return 1
}

test_pg_subcommands || { echo "test_pg_subcommands failed"; exit 1; }

echo "OK"
