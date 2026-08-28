#!/usr/bin/env bash
#
# Tier-2 op scheduler isolation benchmark: drives real rados bench
# workloads through a single-OSD vstart cluster, cycling osd_op_queue
# across schedulers.  The tier-1 in-process companion is
# ceph_bench_op_scheduler; this script validates that dispatch-level
# isolation survives the full OSD pipeline with BlueStore underneath.
#
# Run from a built build directory:
#   ../src/test/osd/vstart_bench_scheduler.sh
#
# Tunables (env):
#   SCHEDULERS      default "wpq mclock_scheduler bfq"
#   SESSIONS        aggressor session counts, default "1 4 16"
#   SECS            victim measurement window per cell, default 12
#   BLOCK           op size in bytes, default 65536
#   AGGR_QD         queue depth per aggressor session, default 8
#   VICTIM_QD       victim queue depth in share cells, default 16
#   THROTTLE_BYTES  bluestore throttle, default 4194304; 0 disables
#
# THROTTLE_BYTES bounds BlueStore's in-flight window so that, on fast
# devices, contention surfaces at the op queue where the scheduler can
# act on it.  Without it a fast NVMe absorbs the offered load below
# the scheduler and every scheduler degenerates to FIFO passthrough --
# the same reason the mclock tuning guide recommends constraining
# bluestore_throttle_bytes.
#
# Scenarios per scheduler:
#   share_k<K>   victim (rbd pool) saturating vs K saturating rgw
#                sessions; isolation == victim MB/s insensitive to K
#   latency_qd1  victim at queue depth 1 (latency probe) vs 8
#                saturating sessions; isolation == bounded avg/max lat
#
# Results: CSV + per-cell dump_op_pq_state snapshots under
# ./scheduler-isolation-results/

set -uo pipefail

BUILD_DIR=${BUILD_DIR:-$PWD}
SCHEDULERS=${SCHEDULERS:-"wpq mclock_scheduler bfq"}
SESSIONS=${SESSIONS:-"1 4 16"}
SECS=${SECS:-12}
BLOCK=${BLOCK:-65536}
AGGR_QD=${AGGR_QD:-8}
# The victim must be able to consume its fair share: by Little's law
# it needs QD >= share * op_rate * latency, or it self-caps below any
# scheduler's allocation and every scheduler looks identical.
VICTIM_QD=${VICTIM_QD:-64}
THROTTLE_BYTES=${THROTTLE_BYTES:-4194304}

cd "$BUILD_DIR"
CEPH=./bin/ceph
RADOS=./bin/rados
if [ ! -x "$CEPH" ] || [ ! -x "$RADOS" ]; then
    echo "run from a built build directory (./bin/ceph missing)" >&2
    exit 1
fi

RESULTS_DIR=$BUILD_DIR/scheduler-isolation-results
CSV=$RESULTS_DIR/results.csv
mkdir -p "$RESULTS_DIR"
echo "scheduler,cell,victim_mbps,victim_avg_lat_s,victim_max_lat_s,aggr_mbps_total,victim_share" > "$CSV"

THROTTLE_OPTS=()
if [ "$THROTTLE_BYTES" -gt 0 ]; then
    THROTTLE_OPTS=(-o "bluestore_throttle_bytes = $THROTTLE_BYTES"
		   -o "bluestore_throttle_deferred_bytes = $THROTTLE_BYTES")
fi

start_cluster() {
    local sched=$1
    ../src/stop.sh >/dev/null 2>&1 || true
    if ! MON=1 OSD=1 MGR=1 MDS=0 RGW=0 ../src/vstart.sh -n --without-dashboard \
        -o "osd_op_queue = $sched" \
        -o "osd_op_num_shards = 1" \
        -o "osd_pool_default_size = 1" \
        -o "osd_pool_default_min_size = 1" \
        -o "mon_allow_pool_size_one = true" \
        -o "mon_allow_pool_delete = true" \
        "${THROTTLE_OPTS[@]}" \
        > "$RESULTS_DIR/vstart_$sched.log" 2>&1; then
        echo "vstart failed for $sched, see $RESULTS_DIR/vstart_$sched.log" >&2
        exit 1
    fi
    $CEPH osd set noscrub >/dev/null 2>&1
    $CEPH osd set nodeep-scrub >/dev/null 2>&1
    local active
    active=$($CEPH daemon osd.0 config get osd_op_queue 2>/dev/null |
                 python3 -c 'import json,sys; print(json.load(sys.stdin)["osd_op_queue"])')
    if [ "$active" != "$sched" ]; then
        echo "expected osd_op_queue=$sched, got '$active'" >&2
        exit 1
    fi
}

wait_clean() {
    for _ in $(seq 90); do
        if $CEPH pg stat --format json 2>/dev/null | python3 -c '
import json, sys
states = json.load(sys.stdin)["pg_summary"]["num_pg_by_state"]
total = sum(s["num"] for s in states)
clean = sum(s["num"] for s in states if s["name"] == "active+clean")
sys.exit(0 if total > 0 and total == clean else 1)'; then
            return 0
        fi
        sleep 1
    done
    echo "timed out waiting for active+clean pgs" >&2
    return 1
}

make_pools() {
    $CEPH osd pool create victim 32 >/dev/null 2>&1
    $CEPH osd pool create aggr 32 >/dev/null 2>&1
    $CEPH osd pool application enable victim rbd >/dev/null 2>&1
    $CEPH osd pool application enable aggr rgw >/dev/null 2>&1
    wait_clean
}

destroy_pools() {
    $CEPH osd pool rm victim victim --yes-i-really-really-mean-it >/dev/null 2>&1
    $CEPH osd pool rm aggr aggr --yes-i-really-really-mean-it >/dev/null 2>&1
}

# run_cell <scheduler> <aggr_sessions> <victim_qd> <label>
run_cell() {
    local sched=$1 k=$2 victim_qd=$3 label=$4
    local tmp="$RESULTS_DIR/${sched}_${label}"
    mkdir -p "$tmp"
    make_pools

    local pids=()
    for i in $(seq 1 "$k"); do
        $RADOS -p aggr bench $((SECS + 25)) write -b "$BLOCK" -t "$AGGR_QD" \
            --no-cleanup > "$tmp/aggr_$i.log" 2>&1 &
        pids+=($!)
    done
    sleep 4  # let the aggressor backlog build

    $RADOS -p victim bench "$SECS" write -b "$BLOCK" -t "$victim_qd" \
        --no-cleanup > "$tmp/victim.log" 2>&1

    # snapshot scheduler state while the aggressors are still running
    $CEPH daemon osd.0 dump_op_pq_state > "$tmp/pq_state.json" 2>/dev/null || true

    kill "${pids[@]}" >/dev/null 2>&1 || true
    wait >/dev/null 2>&1 || true

    local vbw vavg vmax abw share
    vbw=$(awk '/^Bandwidth \(MB\/sec\)/ {print $3}' "$tmp/victim.log")
    vavg=$(awk '/^Average Latency\(s\)/ {print $3}' "$tmp/victim.log")
    vmax=$(awk '/^Max latency\(s\)/ {print $3}' "$tmp/victim.log")
    # killed benches never print a final summary; use each aggressor's
    # last per-second progress line ("avg MB/s" column).  Approximate:
    # their window is slightly wider than the victim's.
    abw=$(for f in "$tmp"/aggr_*.log; do
	      awk '$1 ~ /^[0-9]+$/ && $5 ~ /^[0-9.]+$/ {last=$5}
		   END {if (last) print last}' "$f"
	  done | awk '{s += $1} END {printf "%.1f", s}')
    share=$(awk -v v="${vbw:-0}" -v a="${abw:-0}" \
		'BEGIN {t = v + a; printf "%.3f", (t > 0 ? v / t : 0)}')

    echo "$sched,$label,${vbw:-0},${vavg:-0},${vmax:-0},${abw:-0},$share" >> "$CSV"
    printf '  %-16s %-12s victim %8s MB/s  share %-6s avg %8ss  max %8ss  (aggr ~%s MB/s)\n' \
        "$sched" "$label" "${vbw:-?}" "$share" "${vavg:-?}" "${vmax:-?}" "${abw:-?}"

    destroy_pools
}

echo "device: single vstart OSD, 1 op shard, ${BLOCK}B writes"
for sched in $SCHEDULERS; do
    echo "=== $sched ==="
    start_cluster "$sched"
    for k in $SESSIONS; do
        run_cell "$sched" "$k" "$VICTIM_QD" "share_k$k"
    done
    run_cell "$sched" 8 1 "latency_qd1"
done
../src/stop.sh >/dev/null 2>&1 || true

echo
echo "results: $CSV"
column -s, -t < "$CSV"
