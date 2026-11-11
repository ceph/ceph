#!/usr/bin/env bash
#
# Deep scrub and repair of an erasure coded pool over two zones, whose zones
# hold a copy of each shard.
#

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    export CEPH_MON_A="127.0.0.1:7353" # git grep '\<7353\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7354" # git grep '\<7354\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7355" # git grep '\<7355\>' : there must be only one
    export CEPH_MON="$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"
    run_stretch_tests "$@"
}

# the shards with errors in the inconsistent object of a PG
function inconsistent_shards() {
    rados list-inconsistent-obj $1 |
        jq -c '[.inconsistents[].shards[]|select(.errors|length > 0)|.shard]|sort'
}

# Deep scrub of an erasure coded pool over two zones must find a shard
# whose copy in one zone differs from that in the other, and repair must
# fix it. The test creates pool ec, of one PG, over the datacenters dc1 and
# dc2 with the given profile, so shards 0-2 are in one zone and their
# copies, shards 3-5, in the other. It writes an object, corrupts shard 4,
# the copy of shard 1, and deep scrubs the PG: the shards reported with
# errors must be those expected. After repair, a second deep scrub finds no
# inconsistency, shard 4 holds its old bytes again, and the object reads
# back as written.
# The tests run this with an isa and a jerasure profile, both k=2 m=1.
function scrub_and_repair_zone_copy() {
    local dir=$1
    local profile=$2
    local expected=$3

    ceph osd pool create ec 1 1 erasure $profile --num_zones 2 || return 1
    wait_for_clean || return 1
    dd if=/dev/urandom of=$dir/ORIGINAL bs=65536 count=1 || return 1
    rados -p ec put obj $dir/ORIGINAL || return 1
    local pg=$(get_pg ec obj)
    # shard 4 is the fifth in the acting set by shard, which pg query shows;
    # not named osd, which the helpers set
    local victim=$(ceph pg $pg query | jq '.acting[4]')

    objectstore_tool $dir $victim obj get-bytes $dir/SHARD || return 1
    dd if=/dev/urandom of=$dir/CORRUPT bs=$(stat -c %s $dir/SHARD) count=1 || return 1
    objectstore_tool $dir $victim obj set-bytes $dir/CORRUPT || return 1

    pg_deep_scrub $pg || return 1
    rados list-inconsistent-obj $pg --format=json-pretty
    test "$(inconsistent_shards $pg)" = "$expected" || return 1

    repair $pg || return 1
    pg_deep_scrub $pg || return 1
    test "$(rados list-inconsistent-obj $pg | jq '.inconsistents|length')" = 0 || return 1
    objectstore_tool $dir $victim obj get-bytes $dir/REPAIRED || return 1
    cmp $dir/SHARD $dir/REPAIRED || return 1
    rados -p ec get obj $dir/COPY || return 1
    cmp $dir/ORIGINAL $dir/COPY || return 1
}

# isa can turn the CRCs of the data shards into those of the parity shard
# and back, so scrub can tell which zone's shards agree with each other:
# only the corrupt copy, shard 4, is reported.
function TEST_scrub_repairs_corrupt_zone_copy() {
    local dir=$1
    two_zone_cluster $dir || return 1

    ceph osd erasure-code-profile set isa21 plugin=isa k=2 m=1 || return 1
    scrub_and_repair_zone_copy $dir isa21 "[4]" || return 1
}

# jerasure with technique reed_sol_van cannot turn CRCs that way, so
# nothing says which of the two different copies is good: both, shards 1
# and 4, are reported.
function TEST_scrub_repairs_both_copies_without_crc_decode() {
    local dir=$1
    two_zone_cluster $dir || return 1

    ceph osd erasure-code-profile set jer21 plugin=jerasure k=2 m=1 \
        technique=reed_sol_van || return 1
    scrub_and_repair_zone_copy $dir jer21 "[1,4]" || return 1
}

main mon-stretch-ec-scrub "$@"
