#!/usr/bin/env bash
#
# Functional test for the erasure-coded "dynamic object size" feature: EC picks
# a per-object chunk size from the object's size hint, stashes it in the
# object_info, and honours it thereafter (write/read/recovery/scrub).
#
source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7132" # git grep '\<7132\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    # optimized EC requires bluestore for ec_overwrites scrubbing
    CEPH_ARGS+="--osd-scrub-during-recovery=true "

    setup $dir || return 1
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for id in $(seq 0 6) ; do
        run_osd $dir $id || return 1
    done
    create_rbd_pool || return 1
    wait_for_clean || return 1

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        $func $dir || return 1
    done

    teardown $dir || return 1
}

# Create a k=4 m=2 optimized EC pool with the dynamic-object-size feature.
function create_dynamic_ec_pool() {
    local poolname=$1
    local max_chunk=${2:-1048576}

    ceph osd erasure-code-profile set dynprofile \
        k=4 m=2 crush-failure-domain=osd || return 1
    create_pool $poolname 12 12 erasure dynprofile || return 1
    ceph osd pool set $poolname allow_ec_overwrites true || return 1
    ceph osd pool set $poolname allow_ec_optimizations true || return 1
    ceph osd pool set $poolname dynamic_object_size true || return 1
    ceph osd pool set $poolname ec_dynamic_max_chunk_size $max_chunk || return 1
    wait_for_clean || return 1
}

# The flag and property must round-trip and the flag must be set-once.
function TEST_dynamic_object_size_pool_config() {
    local dir=$1
    local poolname=dynpool_cfg

    create_dynamic_ec_pool $poolname || return 1

    ceph osd pool get $poolname dynamic_object_size | grep -q "true" || return 1
    ceph osd pool get $poolname ec_dynamic_max_chunk_size | \
        grep -q "1048576" || return 1

    # Set-once: disabling must be rejected.
    ! ceph osd pool set $poolname dynamic_object_size false || return 1

    delete_pool $poolname || return 1
    ceph osd erasure-code-profile rm dynprofile || return 1
}

# Write objects of several sizes (with size hints), read them back and verify
# integrity, then deep-scrub and confirm the pool stays consistent. This
# exercises the choose/stash-on-write and honour-on-read/scrub paths.
function TEST_dynamic_object_size_write_read_scrub() {
    local dir=$1
    local poolname=dynpool_io

    create_dynamic_ec_pool $poolname || return 1

    # A range of sizes: below the default stripe, within one dynamic chunk, and
    # spanning multiple stripes at the max chunk size.
    for size in 4096 200000 1048576 5242880 ; do
        local obj="obj_$size"
        dd if=/dev/urandom of=$dir/$obj bs=$size count=1 2>/dev/null || return 1
        # --object-size gives EC the expected-object-size hint.
        rados --pool $poolname put $obj $dir/$obj \
            --object-size $size || return 1
        rados --pool $poolname get $obj $dir/$obj.check || return 1
        cmp $dir/$obj $dir/$obj.check || return 1

        # Overwrite part of the object and re-verify: the stashed chunk size
        # must continue to be honoured for reads after modification.
        dd if=/dev/urandom of=$dir/patch bs=4096 count=1 2>/dev/null || return 1
        rados --pool $poolname put $obj $dir/patch --offset 4096 || return 1
        dd if=$dir/patch of=$dir/$obj bs=4096 count=1 seek=1 conv=notrunc \
            2>/dev/null || return 1
        rados --pool $poolname get $obj $dir/$obj.check2 || return 1
        cmp $dir/$obj $dir/$obj.check2 || return 1
    done

    # Deep-scrub every PG in the pool and confirm no inconsistencies.
    local poolid=$(ceph osd pool ls detail -f json | \
        jq ".[] | select(.pool_name==\"$poolname\") | .pool")
    for pgid in $(ceph pg ls-by-pool $poolname -f json | \
                  jq -r '.pg_stats[].pgid') ; do
        pg_deep_scrub $pgid || return 1
    done
    ceph pg dump pgs -f json | \
        jq -e '.pg_stats[] | select(.state | test("inconsistent")) | .pgid' \
        && return 1

    delete_pool $poolname || return 1
    ceph osd erasure-code-profile rm dynprofile || return 1
}

# The chosen chunk size must be stashed in the object_info and be immutable.
function TEST_dynamic_object_size_stashed_in_oi() {
    local dir=$1
    local poolname=dynpool_oi

    create_dynamic_ec_pool $poolname || return 1

    local obj=big_obj
    dd if=/dev/urandom of=$dir/$obj bs=1048576 count=1 2>/dev/null || return 1
    rados --pool $poolname put $obj $dir/$obj --object-size 1048576 || return 1

    # Find a shard/OSD holding the object and dump its object_info offline.
    local poolid=$(ceph osd pool ls detail -f json | \
        jq ".[] | select(.pool_name==\"$poolname\") | .pool")
    local osd_id=$(ceph osd map $poolname $obj -f json | jq '.up[0]')

    ceph osd set noup || return 1
    kill_daemons $dir TERM osd.$osd_id || return 1

    # The dumped object_info includes ec_chunk_size; it must be non-zero.
    local oi=$(ceph-objectstore-tool --data-path $dir/$osd_id \
        --op list "$obj" | head -1)
    ceph-objectstore-tool --data-path $dir/$osd_id "$oi" dump | \
        jq -e '.info.ec_chunk_size > 0' || return 1

    activate_osd $dir $osd_id || return 1
    ceph osd unset noup || return 1
    wait_for_clean || return 1

    delete_pool $poolname || return 1
    ceph osd erasure-code-profile rm dynprofile || return 1
}

main test-erasure-code-dynamic-object-size "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && \
#   test/erasure-code/test-erasure-code-dynamic-object-size.sh"
# End:
