#!/usr/bin/env bash
source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

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
        echo "Mismatch for $name at $field: classic=$av vs crimson=$bv"
        return 1
    fi
}

_assert_pg_json_sane() {
    local pgid=$1
    local json=$2
    local kind=$3

    echo "$json" | jq -e . >/dev/null || {
        echo "Invalid JSON output ($kind): $json"
        return 1
    }

    if [ "$kind" = "query" ]; then
        echo "$json" | jq -e --arg pgid "$pgid" '(.pgid == $pgid) or (.pg.pgid == $pgid) or (.info.pgid == $pgid)' >/dev/null || {
            echo "Query output missing pgid ($pgid): $json"
            return 1
        }
    fi
}

_wait_for_pg_query_ok() {
    local pgid=$1
    local timeout=${2:-60}
    local deadline=$(( $(date +%s) + timeout ))
    local had_xtrace=0
    case "$-" in
      *x*) had_xtrace=1; set +x ;;
    esac
    while [ $(date +%s) -lt $deadline ]; do
        local q
        q=$(_run_pg_json "$pgid" query 2>/dev/null || true)
        if [ -n "$q" ]; then
          if _assert_pg_json_sane "$pgid" "$q" query >/dev/null 2>&1; then
            if [ $had_xtrace -eq 1 ]; then
              set -x
            fi
            return 0
          fi
        fi
        sleep 1
    done
    if [ $had_xtrace -eq 1 ]; then
      set -x
    fi
    echo "Timed out waiting for pg $pgid query to succeed"
    ceph -s || true
    return 1
}

# Run a subcommand and return JSON
_run_pg_json() {
    local pgid=$1
    local subcmd=$2
    ceph --format json pg $pgid $subcmd | awk 'BEGIN{p=0} /^[[:space:]]*[\[{]/{p=1} p{print}'
}

# Save JSON outputs for a given OSD type
_save_outputs() {
    local pgid=$1
    local outdir=$2
    local qjson ljson ujson
    qjson=$(_run_pg_json $pgid query) || return 1
    _assert_pg_json_sane "$pgid" "$qjson" query || return 1
    ljson=$(_run_pg_json $pgid log) || return 1
    _assert_pg_json_sane "$pgid" "$ljson" log || return 1
    ujson=$(_run_pg_json $pgid list_unfound) || return 1
    _assert_pg_json_sane "$pgid" "$ujson" list_unfound || return 1
    echo "$qjson" > "$outdir/query.json"
    echo "$ljson" > "$outdir/log.json"
    echo "$ujson" > "$outdir/list_unfound.json"
}

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7195" # git grep '\<7195\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    export OUTDIR=${TMPDIR:-/tmp}/pg-subcommands-$$
    rm -rf "$OUTDIR"
    mkdir -p "$OUTDIR"
    trap 'rm -rf "$OUTDIR"' EXIT

    local funcs=${@:-TEST_a_classic_save_outputs TEST_b_crimson_save_and_compare}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function TEST_a_classic_save_outputs() {
    local dir=$1

    run_mon $dir a --osd_pool_default_size=1 --mon_allow_pool_size_one=true \
      --osd_pool_default_crimson=false || return 1
    run_mgr $dir x || return 1
    
    # Run classic OSD first
    echo "=== Running Classic OSD ==="
    run_osd $dir 0 || return 1

    local poolname=foo
    create_pool $poolname 1 1 || return 1

    local objname=obj-$$
    local pgid
    pgid=$(get_pg $poolname $objname) || return 1

    _wait_for_pg_query_ok $pgid 60 || return 1

    mkdir -p "$OUTDIR/classic"
    _save_outputs $pgid "$OUTDIR/classic" || return 1

    delete_pool $poolname
}

function TEST_b_crimson_save_and_compare() {
    local dir=$1

    run_mon $dir a --osd_pool_default_size=1 --mon_allow_pool_size_one=true \
      --osd_pool_default_crimson=true || return 1
    run_mgr $dir x || return 1

    echo "=== Running Crimson OSD ==="
    run_crimson_osd $dir 0 || return 1

    local poolname=foo
    create_pool $poolname 1 1 || return 1
    ceph osd pool ls detail --format json |
      jq -e --arg pool "$poolname" '.[] | select(.pool_name == $pool) | (.flags_names // [] | index("crimson"))' >/dev/null || return 1

    local objname=obj-$$
    local pgid
    pgid=$(get_pg $poolname $objname) || return 1

    _wait_for_pg_query_ok $pgid 60 || return 1

    mkdir -p "$OUTDIR/crimson"
    _save_outputs $pgid "$OUTDIR/crimson" || return 1

    echo "=== Comparing Classic vs Crimson Outputs ==="
    local classic_query crimson_query classic_unfound crimson_unfound
    classic_query=$(cat "$OUTDIR/classic/query.json")
    crimson_query=$(cat "$OUTDIR/crimson/query.json")
    classic_unfound=$(cat "$OUTDIR/classic/list_unfound.json")
    crimson_unfound=$(cat "$OUTDIR/crimson/list_unfound.json")

    _cmp_json_field "$classic_query" "$crimson_query" '(.pgid // .pg.pgid // .info.pgid)' "query.pgid" || return 1
    _cmp_json_field "$classic_unfound" "$crimson_unfound" '(.missing.num_missing // .num_missing // 0)' "list_unfound.num_missing" || return 1
    _cmp_json_field "$classic_unfound" "$crimson_unfound" '(.missing.num_unfound // .num_unfound // 0)' "list_unfound.num_unfound" || return 1

    delete_pool $poolname
}

# Run cd build && ../qa/run-standalone.sh osd/pg-subcommands.sh
main pg-subcommands "$@"
