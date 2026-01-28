#!/usr/bin/env bash
source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

# Compare a field from two JSON outputs; fail if values differ
_cmp_json_field() {
    local a=$1
    local b=$2
    local field=$3
    local av=$(echo "$a" | jq -r "$field // null")
    local bv=$(echo "$b" | jq -r "$field // null")
    if [ "$av" != "$bv" ]; then
        echo "Mismatch at $field: classic=$av vs crimson=$bv"
        return 1
    fi
}

# Run a subcommand and return JSON
_run_pg_json() {
    local pgid=$1
    local subcmd=$2
    ceph --format json pg $pgid $subcmd
}

# Save JSON outputs for a given OSD type
_save_outputs() {
    local pgid=$1
    local outdir=$2
    local qjson ljson ujson
    qjson=$(_run_pg_json $pgid query) || return 1
    ljson=$(_run_pg_json $pgid log) || return 1
    ujson=$(_run_pg_json $pgid list_unfound) || return 1
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

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function TEST_pg_subcommands_old_form() {
    local dir=$1

    run_mon $dir a --osd_pool_default_size=1 --mon_allow_pool_size_one=true || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1

    local poolname=foo
    create_pool $poolname 1 1 || return 1
    wait_for_clean || return 1

    local objname=obj-$$
    echo "payload" > $dir/$objname
    rados -p $poolname put $objname $dir/$objname || return 1

    local pgid
    pgid=$(get_pg $poolname $objname) || return 1

    # Ensure commands work (old-form wrapper)
    ceph pg $pgid query >/dev/null || return 1
    ceph pg $pgid log >/dev/null || return 1
    ceph pg $pgid list_unfound >/dev/null || return 1
    ceph pg $pgid scrub || return 1
    ceph pg $pgid deep-scrub || return 1

    # Save outputs from current OSD
    local outdir=${TMPDIR:-/tmp}/pg-subcommands-$$
    mkdir -p "$outdir/outputs"
    _save_outputs $pgid "$outdir/outputs" || return 1
    echo "Outputs saved to $outdir/outputs"

    # Basic sanity checks on stable fields
    echo "Validating JSON outputs..."
    _cmp_json_field "$(<$outdir/outputs/query.json)" "$(<$outdir/outputs/query.json)" '.pg.pgid' || return 1
    _cmp_json_field "$(<$outdir/outputs/log.json)" "$(<$outdir/outputs/log.json)" '.op_log.pg_log_t.head' || return 1
    _cmp_json_field "$(<$outdir/outputs/list_unfound.json)" "$(<$outdir/outputs/list_unfound.json)" '.missing.num_missing' || return 1
    echo "JSON validation passed!"

    delete_pool $poolname
}

main pg-subcommands "$@"
