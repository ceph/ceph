#!/usr/bin/env bash
#
# The copies that out-of-band transfers make in the OSDs. The OSDs run
# with libceph_test_copycount preloaded, which counts the bytes each
# process copies with memcpy and memmove in calls of 4 KiB or more. Each
# test moves one object of SIZE bytes out of band over the libfabric tcp
# provider, on 127.0.0.1, and compares the bytes the three OSDs copied
# meanwhile with the budget of that path:
#
#   read, BlueStore reads into the arena (osd_oob_direct_read=nocache),
#   replicated or erasure-coded with balanced reads      no copy
#   read, erasure-coded, the primary gathers the other
#   data shard into the arena (oob_cluster_transport)    1/k of the object
#   read, osd_oob_direct_read=off                        the whole object
#   range read from an offset that is not block-aligned,
#   BlueStore reads into the arena, replicated or
#   erasure-coded with balanced reads                    no copy
#   pulled write (OSD-direct PUT)                        the whole object,
#                                                        on top of what a
#                                                        plain write copies
#
# A test fails when its OSDs copy more than its budget allows, and prints
# the places that copied, so that a change that adds a copy names itself.
# The off case also has a floor: it must copy the object once, which
# shows that the counting works. A test that lowers a budget, because a
# copy went away, lowers it here.
#
# A copy budget allows SLACK bytes on top, for the copies that a busy
# OSD makes without any client I/O. Reads are cold: each test drops the
# OSDs' caches before it reads.
#
# A balanced read of an erasure-coded object goes to its shard OSDs only
# when they can serve it. Otherwise they answer EAGAIN, the client sends
# the read to the primary, and the primary copies the whole object. Three
# things make them refuse, so the tests rule all three out:
#
# - A PG reads into the arena only with the optimized EC code, and a PG
#   of a pool that gets allow_ec_optimizations after its creation keeps
#   the legacy code until its next interval change. The pools have EC
#   optimizations from their creation
#   (osd_pool_default_flag_ec_optimizations).
# - A shard serves a read of an object only once it knows that the last
#   write of the object committed on every shard, which a later write to
#   the PG tells it. A small write reaches only the shards it changes,
#   so each test writes a second object of whole stripes (1 MiB) into
#   the (only) PG of the pool after the object it reads.
# - A shard serves reads only while it holds a read lease from the
#   primary, and a new PG gets its first lease a moment after it is
#   clean. A test with balanced reads reads the second object until the
#   shards serve it.
#
source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

SIZE=16777216
SLACK=1048576

function run() {
    local dir=$1
    shift

    # a build without WITH_OOB_OFI has nothing to test, unless the run
    # says it needs the tests (CEPH_OOB_REQUIRED): a skip then fails
    if ! type ceph_test_osd_oob_read > /dev/null 2>&1 ; then
        echo "ceph_test_osd_oob_read not found (built without WITH_OOB_OFI?)"
        test -z "$CEPH_OOB_REQUIRED"
        return
    fi
    # the build tree first, then where the ceph-test package puts it
    COPYCOUNT_LIB=
    local lib
    for lib in $(pwd)/${CEPH_LIB:-lib} /usr/lib64/ceph /usr/lib/ceph \
               /usr/lib/$(uname -m)-linux-gnu/ceph ; do
        if test -f $lib/libceph_test_copycount.so ; then
            COPYCOUNT_LIB=$lib/libceph_test_copycount.so
            break
        fi
    done
    if test -z "$COPYCOUNT_LIB" ; then
        echo "libceph_test_copycount.so not found"
        test -z "$CEPH_OOB_REQUIRED"
        return
    fi

    export CEPH_MON="127.0.0.1:7162" # git grep '\<7162\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

##
# Run a mon, a mgr and three OSDs that serve out-of-band transfers over
# the libfabric tcp provider, with the copy counter preloaded in the OSDs
# and their debug logs off. The remaining arguments go to each OSD.
#
function run_copy_cluster() {
    local dir=$1
    shift

    run_mon $dir a --osd_pool_default_flag_ec_optimizations=true || return 1
    run_mgr $dir x || return 1
    mkdir -p $(pwd)/$dir/copycount || return 1
    for id in 0 1 2 ; do
        (
            export LD_PRELOAD=$COPYCOUNT_LIB
            export COPYCOUNT_DIR=$(pwd)/$dir/copycount
            export COPYCOUNT_DEPTH=6
            run_osd $dir $id \
                --oob_client_transports=tcp \
                --oob_memory=268435456 \
                --debug_osd=0/0 --debug_ms=0/0 --debug_monc=0/0 \
                --debug_bluestore=0/0 --debug_bdev=0/0 --debug_bluefs=0/0 \
                --debug_rocksdb=0/0 \
                "$@"
        ) || return 1
    done
    for id in 0 1 2 ; do
        test -f $(osd_copy_file $dir $id) || return 1
    done
}

function osd_copy_file() {
    local dir=$1
    local id=$2
    echo $(pwd)/$dir/copycount/copycount.$(cat $dir/osd.$id.pid)
}

# the counts of the three OSDs, one JSON object: bytes, calls, and the
# bytes and calls of each place that copied
function osd_copies() {
    local dir=$1
    python3 -c '
import json, struct, sys
total = {"bytes": 0, "calls": 0, "sites": {}}
for f in sys.argv[1:]:
    d = open(f, "rb").read()
    magic, mn, b, c, dropped = struct.unpack_from("8s4Q", d, 0)
    assert magic == b"COPYCNT2", f
    total["bytes"] += b
    total["calls"] += c
    for i in range(256):
        sb, sc, w = struct.unpack_from("2Q240s", d, 40 + 256 * i)
        if sc:
            w = w.split(b"\0")[0].decode()
            s = total["sites"].setdefault(w, [0, 0])
            s[0] += sb
            s[1] += sc
print(json.dumps(total))
' $(osd_copy_file $dir 0) $(osd_copy_file $dir 1) $(osd_copy_file $dir 2)
}

# what the OSDs copied between two osd_copies: "<bytes> <calls>", and the
# places that copied on stderr, largest first, each with its call stack
# (addr2line names the functions when it can)
function copies_between() {
    python3 -c '
import json, os, subprocess, sys
a, b = json.loads(sys.argv[1]), json.loads(sys.argv[2])
osd_bin, lib_dir = sys.argv[3], sys.argv[4]
print(b["bytes"] - a["bytes"], b["calls"] - a["calls"])
def name(frame):
    obj, _, off = frame.rpartition("+")
    path = osd_bin if obj == "ceph-osd" else os.path.join(lib_dir, obj)
    try:
        out = subprocess.run(["addr2line", "-f", "-C", "-e", path, off],
                             capture_output=True, text=True).stdout.split("\n")
    except OSError:
        return frame
    f = out[0].split("(")[0] if out and out[0] not in ("", "??") else ""
    return "%s (%s)" % (f, frame) if f else frame
sites = []
for w, (sb, sc) in b["sites"].items():
    ab, ac = a["sites"].get(w, [0, 0])
    if sb - ab:
        sites.append((sb - ab, sc - ac, w))
for sb, sc, w in sorted(sites, reverse=True)[:10]:
    print("  copied %d bytes in %d calls:" % (sb, sc), file=sys.stderr)
    for frame in w.split(" < "):
        print("      " + name(frame), file=sys.stderr)
' "$1" "$2" "$(type -p ceph-osd)" "$(pwd)/${CEPH_LIB:-lib}"
}

# replica and shard reads that the OSDs served, summed
function replica_reads_served() {
    local total=0
    for id in 0 1 2 ; do
        local v=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.$id) \
            perf dump osd | jq -r ".osd.replica_read_served")
        total=$((total + v))
    done
    echo $total
}

# read **oid** of **size** bytes with balanced reads until a shard OSD
# serves the read: up to 10 s
function wait_for_shard_reads() {
    local pool=$1
    local size=$2
    local oid=$3
    for i in $(seq 50) ; do
        local before=$(replica_reads_served)
        ceph_test_osd_oob_read get $pool $size --balance $oid > /dev/null || return 1
        test $(replica_reads_served) -gt $before && return 0
        sleep 0.2
    done
    echo "no shard OSD served a balanced read of $oid" >&2
    return 1
}

function cache_drop() {
    for id in 0 1 2 ; do
        ceph tell osd.$id cache drop > /dev/null || return 1
    done
}

function bluestore_stat() {
    local field=$1
    local total=0
    for id in 0 1 2 ; do
        local v=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.$id) \
            perf dump bluestore | jq -r ".bluestore.$field")
        total=$((total + v))
    done
    echo $total
}

function ofi_total() {
    local field=$1
    local total=0
    for id in 0 1 2 ; do
        local v=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.$id) \
            ofi status | jq -r ".$field")
        total=$((total + v))
    done
    echo $total
}

# a replicated pool of one PG, on all three OSDs
function create_repl_pool() {
    local pool=$1
    create_pool $pool 1 1 || return 1
    wait_for_clean || return 1
}

# an erasure-coded pool of one PG, k=2 m=1, with EC optimizations from
# its creation
function create_ec_pool_k2m1() {
    local pool=$1
    ceph osd erasure-code-profile set copyprofile k=2 m=1 \
        crush-failure-domain=osd || return 1
    create_pool $pool 1 1 erasure copyprofile || return 1
    ceph osd pool get $pool allow_ec_optimizations | grep -q true || return 1
    ceph osd pool set $pool allow_ec_overwrites true || return 1
    wait_for_clean || return 1
}

##
# Write **oid** in **pool**, drop the caches, read **len** bytes of it out
# of band with the remaining arguments to ceph_test_osd_oob_read get, and
# check that the OSDs placed all of them. Prints what the OSDs did for
# the read alone: "<bytes copied> <copy calls> <bytes BlueStore read into
# the arena> <bytes written from the arena> <bytes BlueStore copied into
# the arena>", and the places that copied on stderr.
#
function cold_read_copies() {
    local dir=$1
    local pool=$2
    local oid=$3
    local len=$4
    shift 4

    ceph_test_osd_oob_read write $pool $SIZE $oid >&2 || return 1
    ceph_test_osd_oob_read write $pool 1048576 $oid.commit >&2 || return 1
    if [[ " $* " == *" --balance "* ]] ; then
        wait_for_shard_reads $pool 1048576 $oid.commit || return 1
    fi
    cache_drop || return 1
    local direct=$(bluestore_stat read_into_direct_bytes)
    local bcopied=$(bluestore_stat read_into_copied_bytes)
    local staged=$(ofi_total bytes_staged)
    local before=$(osd_copies $dir)
    local out
    out=$(ceph_test_osd_oob_read get $pool $len "$@" $oid) || return 1
    echo "$out" >&2
    local after=$(osd_copies $dir)
    echo "$out" | grep -q "^get $oid: r=0 bytes=$len inline=0 .* data=ok crc=ok" || return 1
    local copied
    copied=$(copies_between "$before" "$after") || return 1
    echo "$copied $(( $(bluestore_stat read_into_direct_bytes) - direct ))" \
        "$(( $(ofi_total bytes_staged) - staged ))" \
        "$(( $(bluestore_stat read_into_copied_bytes) - bcopied ))"
}

# check_budget <bytes> <low> <high>: low <= bytes <= high
function check_budget() {
    local bytes=$1
    local low=$2
    local high=$3
    echo "copied $bytes bytes, budget $low to $high" >&2
    test $bytes -ge $low -a $bytes -le $high
}

function TEST_repl_read_into_copies_nothing() {
    local dir=$1
    run_copy_cluster $dir --osd_oob_direct_read=nocache || return 1
    create_repl_pool rbd || return 1

    local copied calls direct staged bcopied
    local out
    out=$(cold_read_copies $dir rbd obj $SIZE) || return 1
    read copied calls direct staged bcopied <<< "$out"
    check_budget $copied 0 $SLACK || return 1
    # BlueStore read the object into the arena, and the NIC took it from
    # there
    test $bcopied = 0 || return 1
    test $direct = $SIZE || return 1
    test $staged = $SIZE || return 1
}

function TEST_repl_staged_read_copies_once() {
    local dir=$1
    run_copy_cluster $dir --osd_oob_direct_read=off || return 1
    create_repl_pool rbd || return 1

    local copied calls direct staged bcopied
    local out
    out=$(cold_read_copies $dir rbd obj $SIZE) || return 1
    read copied calls direct staged bcopied <<< "$out"
    # the read lands in a bufferlist, which the OSD copies into its
    # staging buffer: once, and the counter sees it
    check_budget $copied $((SIZE - SLACK)) $((SIZE + SLACK)) || return 1
}

function TEST_ec_balanced_read_into_copies_nothing() {
    local dir=$1
    run_copy_cluster $dir --osd_oob_direct_read=nocache || return 1
    create_ec_pool_k2m1 ecpool || return 1

    local copied calls direct staged bcopied
    local out
    out=$(cold_read_copies $dir ecpool obj $SIZE --balance) || return 1
    read copied calls direct staged bcopied <<< "$out"
    # each shard OSD reads its own shard into its arena and writes it
    # from there
    check_budget $copied 0 $SLACK || return 1
    test $bcopied = 0 || return 1
    test $direct = $SIZE || return 1
    test $staged = $SIZE || return 1
}

# A range read whose ends are not block-aligned: the OSD reads the whole
# blocks that hold the range into its arena buffer, so that BlueStore
# reads every block straight into the buffer and copies no part of one,
# and the OSD hands out the range from there. The copy budget cannot
# tell the end blobs apart from the slack, so the tests also check that
# BlueStore copied nothing into the arena.
RANGE_OFFSET=1000
RANGE_LEN=$((SIZE - 8192))

function TEST_repl_range_read_into_copies_nothing() {
    local dir=$1
    run_copy_cluster $dir --osd_oob_direct_read=nocache || return 1
    create_repl_pool rbd || return 1

    local copied calls direct staged bcopied
    local out
    out=$(cold_read_copies $dir rbd obj $RANGE_LEN \
        --offset $RANGE_OFFSET) || return 1
    read copied calls direct staged bcopied <<< "$out"
    check_budget $copied 0 $SLACK || return 1
    echo "BlueStore read $direct bytes into the arena for a range of" \
        "$RANGE_LEN, and copied $bcopied" >&2
    test $bcopied = 0 || return 1
    test $direct -ge $RANGE_LEN || return 1
    test $staged = $RANGE_LEN || return 1
}

function TEST_ec_balanced_range_read_into_copies_nothing() {
    local dir=$1
    run_copy_cluster $dir --osd_oob_direct_read=nocache || return 1
    create_ec_pool_k2m1 ecpool || return 1

    local copied calls direct staged bcopied
    local out
    out=$(cold_read_copies $dir ecpool obj $RANGE_LEN --balance \
        --offset $RANGE_OFFSET) || return 1
    read copied calls direct staged bcopied <<< "$out"
    # each shard OSD reads the whole blocks of its shard that hold its
    # part of the range into its arena
    check_budget $copied 0 $SLACK || return 1
    echo "BlueStore read $direct bytes into the arena for a range of" \
        "$RANGE_LEN, and copied $bcopied" >&2
    test $bcopied = 0 || return 1
    test $direct -ge $RANGE_LEN || return 1
    test $staged = $RANGE_LEN || return 1
}

function TEST_ec_gather_read_into_copies_gathered_shard_once() {
    local dir=$1
    run_copy_cluster $dir --osd_oob_direct_read=nocache \
        --oob_cluster_transport=tcp || return 1
    create_ec_pool_k2m1 ecpool || return 1

    local copied calls direct staged bcopied
    local out
    out=$(cold_read_copies $dir ecpool obj $SIZE) || return 1
    read copied calls direct staged bcopied <<< "$out"
    # the primary reads its own shard into the arena, and copies the
    # shard that the other data OSD pushed into its gather window into
    # the arena once (take_pushed): half of the object with k=2
    check_budget $copied 0 $((SIZE / 2 + SLACK)) || return 1
}

function TEST_ec_gather_range_read_into_copies_gathered_shard_once() {
    local dir=$1
    run_copy_cluster $dir --osd_oob_direct_read=nocache \
        --oob_cluster_transport=tcp || return 1
    create_ec_pool_k2m1 ecpool || return 1

    local copied calls direct staged bcopied
    local out
    out=$(cold_read_copies $dir ecpool obj $RANGE_LEN \
        --offset $RANGE_OFFSET) || return 1
    read copied calls direct staged bcopied <<< "$out"
    # as for the whole object: the shard that the other data OSD pushed,
    # once. Each shard's sub-read reads the whole blocks that hold its
    # extents, so that BlueStore copies none of them
    check_budget $copied 0 $((SIZE / 2 + SLACK)) || return 1
    echo "BlueStore read $direct bytes into the arena for a range of" \
        "$RANGE_LEN, and copied $bcopied" >&2
    test $bcopied = 0 || return 1
    test $direct -ge $RANGE_LEN || return 1
}

function TEST_pulled_write_copies_once_more_than_a_plain_write() {
    local dir=$1
    run_copy_cluster $dir || return 1
    create_repl_pool rbd || return 1

    # a plain write of the same size, with the data in the message: the
    # copies that every replicated write makes (BlueStore aligns the data
    # that a replica received, and the primary's transaction decode
    # copies its data)
    local before=$(osd_copies $dir)
    ceph_test_osd_oob_read write rbd $SIZE plain >&2 || return 1
    local after=$(osd_copies $dir)
    local plain
    plain=$(copies_between "$before" "$after") || return 1
    echo "a plain write copied ${plain% *} bytes" >&2

    before=$(osd_copies $dir)
    local out
    out=$(ceph_test_osd_oob_read put rbd $SIZE pulled) || return 1
    echo "$out" >&2
    after=$(osd_copies $dir)
    echo "$out" | grep -q "^put pulled: r=0 bytes=$SIZE .* crc=ok" || return 1
    local copied
    copied=$(copies_between "$before" "$after") || return 1
    # on top of that, the primary pulls the payload into its staging
    # buffer and copies it into the write's bufferlist once
    check_budget ${copied% *} 0 $((${plain% *} + SIZE + SLACK)) || return 1
}

main osd-oob-copies "$@"

# Local Variables:
# compile-command: "cd ../../../build ; make -j4 && ../qa/run-standalone.sh osd-oob-copies.sh"
# End:
