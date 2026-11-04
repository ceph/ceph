#!/usr/bin/env bash
#
# A soak of S3 GETs whose data the OSDs write out of band into client
# memory, over libfabric, with ceph_test_rgw_ofi_soak.
#
# The client puts its object set over plain HTTP, then GETs the objects
# for SOAK_DURATION seconds, whole and in ranges, into windows of its
# memory. It checks that the bytes a GET asked for hold the object's data,
# that no byte outside them changed, that the OSDs' CRC matches, and that
# no write lands in a window after the GET's response, or, for a GET that
# failed without a response, after the fence (SOAK_FENCE_MS). See
# src/test/rgw/ofi/ofi_s3_soak.cc.
#
# A client that dies of SIGKILL, as when rgw_thrash kills the clients,
# starts again until the time is up. Any other failure ends the workunit.
#
# Environment (see s3-oob-common.sh for RGW_ENDPOINT, OOB_PROVIDER,
# OOB_DOMAIN and OOB_NODE):
#   OOB_MODE        strict: every GET gets a response, with no HTTP error,
#                   and only the GETs whose token is too small for their
#                   range fall back to HTTP (default); mixed: GETs may fail
#                   or fall back, as while daemons restart, but no GET may
#                   break a window, and some data must go out of band
#   SOAK_DURATION   seconds to run (default 300)
#   SOAK_BUCKET     the bucket (default s3oob-soak)
#   SOAK_THREADS    concurrent GETs (default 2)
#   SOAK_WINDOWS    windows (default 8)
#   SOAK_WINDOW     window size in bytes (default 64 MiB)
#   SOAK_FENCE_MS   how long after a failed GET ended writes may still
#                   land: the pools' rdma_delivery_lease plus
#                   rdma_delivery_drain plus osd_oob_cutoff_late_tolerance
#                   (default 9000), plus the longest stop of an OSD where
#                   a thrasher stops them
#   SOAK_BIG        1 adds an object of 100 MiB (default 0)
#   SOAK_ARGS       more options of ceph_test_rgw_ofi_soak run
#
# Against a vstart cluster with a gateway, from the build directory:
#
#   ../src/vstart.sh -n -x --rgw_port 8000 \
#       -o 'oob_client_transports = tcp' -o 'oob_network = 127.0.0.0/8' \
#       -o 'oob_memory = 268435456'
#   PATH=$PWD/bin:$PATH CEPH_CONF=$PWD/ceph.conf SOAK_DURATION=60 \
#       RGW_ENDPOINT=http://localhost:8000 \
#       ../qa/workunits/rgw/run-s3-oob-soak.sh

set -ex

: ${OOB_MODE:=strict}
: ${SOAK_DURATION:=300}
: ${SOAK_BUCKET:=s3oob-soak}
: ${SOAK_THREADS:=2}
: ${SOAK_WINDOWS:=8}
: ${SOAK_WINDOW:=67108864}
: ${SOAK_FENCE_MS:=9000}
: ${SOAK_BIG:=0}

. "$(dirname "$0")/s3-oob-common.sh"

if ! type ceph_test_rgw_ofi_soak > /dev/null 2>&1; then
    echo "ceph_test_rgw_ofi_soak not found: the build has no WITH_OOB_OFI"
    exit 1
fi
case "$OOB_MODE" in
    strict|mixed) ;;
    *) echo "OOB_MODE is strict or mixed, not $OOB_MODE"; exit 1 ;;
esac

s3oob_user

work=$(mktemp -d)
# When the workunit fails, keep the manifest and the full output of each
# run: in the job's archive under teuthology, else where they are.
finish() {
    local rc=$? dest
    if [ $rc -eq 0 ]; then
        rm -rf "$work"
        return
    fi
    dest=$work
    if [ -n "$TESTDIR" ] && [ -d "$TESTDIR/archive" ]; then
        if dest=$(mktemp -d "$TESTDIR/archive/s3-oob-soak.XXXX") &&
               cp -a "$work"/. "$dest"/; then
            rm -rf "$work"
        else
            dest=$work
        fi
    fi
    echo "the outputs of the runs are in $dest"
}
trap finish EXIT

# the object set, over HTTP; a gateway that rgw_thrash restarts can fail
# a PUT, so try again
put_args=(--crc)
if [ "$SOAK_BIG" = 1 ]; then
    put_args+=(--big)
fi
for try in $(seq 1 10); do
    if ceph_test_rgw_ofi_soak put "$RGW_ENDPOINT" "$SOAK_BUCKET" \
           $access $secret "$work/manifest" "${put_args[@]}"; then
        break
    fi
    if [ $try = 10 ]; then
        echo "could not put the object set"
        exit 1
    fi
    sleep 10
done
cat "$work/manifest"

# the counters of each run: those of its FINAL line, or of its last STATS
# line when it was killed
total_ops=0 total_placed=0 total_http=0 total_small=0 total_errors=0
total_suspect=0 total_crc=0
field() {
    local v
    v=$(sed -n "s/.* $1=\([0-9]*\).*/\1/p" <<< "$2")
    echo ${v:-0}
}
# the GETs of http= whose token was too small for their range, which the
# line prints as "(small-token N)"
small_tokens() {
    local v
    v=$(sed -n "s/.* (small-token \([0-9]*\)).*/\1/p" <<< "$1")
    echo ${v:-0}
}

end=$(( $(date +%s) + SOAK_DURATION ))
runs=0 kills=0
while :; do
    left=$(( end - $(date +%s) ))
    if [ $runs -gt 0 ] && [ $left -le 0 ]; then
        break
    fi
    [ $left -gt 0 ] || left=1
    runs=$((runs + 1))
    out=$work/run.$runs.out
    rc=0
    ceph_test_rgw_ofi_soak run "$OOB_PROVIDER" "$OOB_DOMAIN" "$OOB_NODE" \
        "$RGW_ENDPOINT" "$SOAK_BUCKET" $access $secret "$work/manifest" \
        --duration $left --threads $SOAK_THREADS --windows $SOAK_WINDOWS \
        --window-size $SOAK_WINDOW --fence-ms $SOAK_FENCE_MS \
        --seed $(( $(date +%s) + runs )) --stats-s 10 \
        $SOAK_ARGS > "$out" 2>&1 || rc=$?
    tail -n 40 "$out"
    if grep -q '^VIOLATION' "$out"; then
        echo "run $runs: the client found violations"
        grep '^VIOLATION' "$out" | head -n 20
        exit 1
    fi
    if [ $rc = 137 ]; then
        # SIGKILL, as rgw_thrash kills the clients
        kills=$((kills + 1))
    elif [ $rc != 0 ]; then
        echo "run $runs: ceph_test_rgw_ofi_soak exited $rc"
        exit 1
    fi
    final=$(grep -E '^(FINAL|STATS) ' "$out" | tail -n 1)
    total_ops=$((total_ops + $(field ops "$final")))
    total_placed=$((total_placed + $(field placed "$final")))
    total_http=$((total_http + $(field http "$final")))
    total_small=$((total_small + $(small_tokens "$final")))
    total_errors=$((total_errors + $(field errors "$final")))
    total_suspect=$((total_suspect + $(field suspect "$final")))
    total_crc=$((total_crc + $(field crc-checked "$final")))
done

echo "soak: $runs runs ($kills killed), $total_ops GETs, $total_placed" \
     "placed ($total_crc with the OSDs' CRC checked), $total_http over" \
     "HTTP ($total_small with a token too small), $total_errors failed," \
     "$total_suspect words suspect"

if [ $total_ops -eq 0 ]; then
    echo "no GET finished"
    exit 1
fi
if [ $total_placed -eq 0 ]; then
    echo "the OSDs placed no GET"
    exit 1
fi
if [ "$OOB_MODE" = strict ] && [ $total_errors -ne 0 ]; then
    echo "GETs failed"
    exit 1
fi
if [ "$OOB_MODE" = strict ] && [ $total_http -gt $total_small ]; then
    echo "$((total_http - total_small)) GETs fell back to HTTP with a token" \
         "that was large enough"
    exit 1
fi

echo OK
