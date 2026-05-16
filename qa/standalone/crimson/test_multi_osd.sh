#!/bin/bash
set -euo pipefail

# test_multi_osd.sh — drive a single fio bench against an already-up
# crimson cluster (presumed brought up by start_multi_osd.sh).
# Generates the fio job, runs fio in the background, periodically
# dumps cluster health + per-OSD seastore_waf counters to stdout and
# a log, watchdog-kills fio if the counters stall, then builds the
# waf_report and tears the cluster down.

usage() {
    cat <<EOF
Usage: $0 --jobs N --size SIZE --iosize SIZE --nrfiles N [options] [BASE_DIR]

Mandatory:
  --jobs N           fio numjobs (one writer per OSD is typical)
  --size SIZE        fio per-job address space (e.g. 16g, 256m)
  --iosize SIZE      fio io_size per job (total writes; e.g. 60g).
                     Total cluster writes = jobs x iosize; bench stops there.
                     Total cluster consumed address space = jobs x size.

Optional fio knobs:
  --bs SIZE          fio block size                  (default: 1M)
  --iodepth N        fio iodepth per job             (default: 32)
  --rw PATTERN       fio rw type                     (default: randwrite)

Optional infra knobs:
  --pool NAME        target rados pool               (default: waf-test)
  --period SECONDS   monitor poll interval           (default: 5)
  --stall-multi N    consecutive identical counter samples that count
                     as a stall and trigger fio kill  (default: 3,
                     so default stall window = 3 x 5 = 15 s)
  --no-teardown      leave cluster running after bench completes
  BASE_DIR           cluster base dir                (default: <repo>/build/dev)

Exit codes:
  0   fio completed cleanly, report built
  1   infrastructure failure (missing cluster / pool / asok)
  2   fio killed by the stall watchdog
  3   fio killed because an OSD went down mid-run
  >3  fio exited non-zero for some other reason (signal or fio error)
EOF
    exit 1
}

# ---- arg parsing ----
JOBS=""; SIZE=""; IOSIZE=""
BS=1M; IODEPTH=32; RW=randwrite
POOL=waf-test
PERIOD=5
STALL_MULTI=15
DO_TEARDOWN=0
RATE=""
BASE_DIR=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --jobs)         JOBS="$2"; shift 2 ;;
        --size)         SIZE="$2"; shift 2 ;;
        --iosize)       IOSIZE="$2"; shift 2 ;;
        --bs)           BS="$2"; shift 2 ;;
        --iodepth)      IODEPTH="$2"; shift 2 ;;
        --rw)           RW="$2"; shift 2 ;;
        --pool)         POOL="$2"; shift 2 ;;
        --period)       PERIOD="$2"; shift 2 ;;
        --stall-multi)  STALL_MULTI="$2"; shift 2 ;;
        --teardown)     DO_TEARDOWN=1; shift ;;
        --no-teardown)  DO_TEARDOWN=0; shift ;;
        --rate)         RATE="$2"; shift 2 ;;
        -h|--help)      usage ;;
        --) shift; break ;;
        -*) echo "Unknown flag: $1" >&2; usage ;;
        *) [ -z "$BASE_DIR" ] && BASE_DIR="$1" || usage ; shift ;;
    esac
done

missing=()
[ -z "$JOBS" ]    && missing+=("--jobs")
[ -z "$SIZE" ]    && missing+=("--size")
[ -z "$IOSIZE" ]  && missing+=("--iosize")
if [ "${#missing[@]}" -gt 0 ]; then
    echo "Error: missing required: ${missing[*]}" >&2
    usage
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CEPH_ROOT="$(realpath "$SCRIPT_DIR/../../..")"
CEPH_BUILD="$CEPH_ROOT/build"
[ -z "$BASE_DIR" ] && BASE_DIR="$CEPH_BUILD/dev"
BASE_DIR="$(realpath -m "$BASE_DIR")"

# ---- preflight: cluster + pool must exist ----
CEPH="$CEPH_BUILD/bin/ceph -c $CEPH_BUILD/ceph.conf"

if [ ! -x "$CEPH_BUILD/bin/ceph" ] || [ ! -f "$CEPH_BUILD/ceph.conf" ]; then
    echo "Error: $CEPH_BUILD/bin/ceph or ceph.conf missing — run start_multi_osd.sh first" >&2
    exit 1
fi
if ! $CEPH -s >/dev/null 2>&1; then
    echo "Error: cluster at $CEPH_BUILD/ceph.conf is not responsive — run start_multi_osd.sh first" >&2
    exit 1
fi
if ! $CEPH osd pool ls 2>/dev/null | grep -qx "$POOL"; then
    echo "Error: pool '$POOL' does not exist on this cluster" >&2
    exit 1
fi

NUM_OSDS=$($CEPH osd stat -f json 2>/dev/null | jq '.num_osds')
if ! [[ "$NUM_OSDS" =~ ^[1-9][0-9]*$ ]]; then
    echo "Error: could not determine OSD count from cluster" >&2
    exit 1
fi

# ---- output paths ----
OUT_DIR="$BASE_DIR/waf_bench"
mkdir -p "$OUT_DIR/asok"
MONITOR_LOG="$OUT_DIR/monitor.log"
FIO_JOB="$OUT_DIR/fio.job"
FIO_STDOUT="$OUT_DIR/fio.stdout.log"
FIO_JSON="$OUT_DIR/fio.json"
: > "$MONITOR_LOG"

log() {
    # Tee one line to stdout and monitor.log with a timestamp prefix.
    local line="$*"
    local ts
    ts=$(date +"%H:%M:%S")
    printf '%s %s\n' "$ts" "$line" | tee -a "$MONITOR_LOG"
}

# ---- calculate nrfiles to match object size to bs ----
size_to_bytes() {
    local val="${1%[KkMmGgTt]}"
    local unit="${1##*[0-9]}"
    case "${unit^^}" in
        K) echo $((val * 1024)) ;;
        M) echo $((val * 1024 * 1024)) ;;
        G) echo $((val * 1024 * 1024 * 1024)) ;;
        T) echo $((val * 1024 * 1024 * 1024 * 1024)) ;;
        *) echo "$val" ;;
    esac
}
SIZE_BYTES=$(size_to_bytes "$SIZE")
BS_BYTES=$(size_to_bytes "$BS")
NRFILES=$(( SIZE_BYTES / BS_BYTES ))
if [ "$NRFILES" -lt 1 ]; then NRFILES=1; fi

# ---- step 4: write fio job ----
cat > "$FIO_JOB" <<EOF
# Generated by test_multi_osd.sh on $(date)
[global]
ioengine=rados
clientname=admin
pool=$POOL
conf=$CEPH_BUILD/ceph.conf
rw=$RW
bs=$BS
iodepth=$IODEPTH
numjobs=$JOBS
nrfiles=$NRFILES
randrepeat=0
norandommap=1
file_service_type=random
log_avg_msec=1000
write_bw_log=fio_write_bw
write_iops_log=fio_write_iops
write_lat_log=fio_write_lat
per_job_logs=1

[waf-write]
size=$SIZE
io_size=$IOSIZE
EOF

if [ -n "$RATE" ]; then
    echo "rate=$RATE" >> "$FIO_JOB"
fi

log "[bench] generated $FIO_JOB"
log "[bench]   jobs=$JOBS size=$SIZE iosize=$IOSIZE nrfiles=$NRFILES (${BS} objects) rate=${RATE:-unlimited}"
log "[bench]   bs=$BS iodepth=$IODEPTH rw=$RW pool=$POOL osds=$NUM_OSDS"
log "[bench]   monitor: period=${PERIOD}s stall-watchdog=${STALL_MULTI} samples (~$((PERIOD * STALL_MULTI))s window)"

# ---- step 5: launch fio in the background ----
export CEPH_KEYRING="$CEPH_BUILD/keyring"
# `exec` so the backgrounded subshell is *replaced* by fio itself — otherwise
# $! is the subshell's pid and the watchdog's kill / wait would target the
# wrapper, leaving the real fio process orphaned and still running.
(
    cd "$OUT_DIR"
    exec fio --output-format=json --output="$FIO_JSON" "$FIO_JOB" >"$FIO_STDOUT" 2>&1
) &
FIO_PID=$!
log "[bench] fio launched pid=$FIO_PID"

# ---- step 6+7: monitor loop with stall watchdog ----
WATCHDOG_FIRED=0
OSD_DOWN=0
PREV_SIG=""
STABLE=0

# Per-OSD consecutive asok-failure counters. We can't trust `ceph osd dump`
# for liveness in a 2-OSD standalone cluster: mon needs mon_osd_min_down_reporters
# (default 2) peer reports to mark an OSD down, but with N=2 only one peer is
# left to report, and ceph-run keeps restarting the dead daemon fast enough to
# trickle heartbeats — so up=2 stays true even while the OSD is wedged in
# replay/crash-loop. Direct asok responsiveness is the ground truth.
declare -ga OSD_FAIL
for _i in $(seq 0 $((NUM_OSDS - 1))); do OSD_FAIL[_i]=0; done
OSD_FAIL_THRESHOLD=2   # ~2 * PERIOD seconds of asok unresponsiveness => crashed

monitor_iter() {
    # Per-OSD seastore_waf counters (totals across the cluster).
    local agg_user=0 agg_dev=0 osd_block=""
    local i u w cnt
    for i in $(seq 0 $((NUM_OSDS - 1))); do
        cnt=$(timeout 5 $CEPH daemon "osd.$i" perfcounters_dump seastore_waf 2>/dev/null || true)
        if [ -z "$cnt" ]; then
            OSD_FAIL[i]=$((${OSD_FAIL[i]} + 1))
        else
            OSD_FAIL[i]=0
        fi
        u=$(echo "$cnt" | jq -r '.seastore_waf.bytes_user_written   // 0' 2>/dev/null || echo 0)
        w=$(echo "$cnt" | jq -r '.seastore_waf.bytes_device_written // 0' 2>/dev/null || echo 0)
        [[ "$u" =~ ^[0-9]+$ ]] || u=0
        [[ "$w" =~ ^[0-9]+$ ]] || w=0
        agg_user=$((agg_user + u))
        agg_dev=$((agg_dev + w))
        osd_block+="    osd.$i: user=$(printf '%15d' "$u")  device=$(printf '%15d' "$w")"$'\n'
    done

    local health
    health=$(timeout 5 $CEPH health 2>/dev/null | head -1 || echo "health: unknown")

    local df_block
    df_block=$(timeout 5 $CEPH osd df -f json 2>/dev/null \
        | jq -r '.nodes[] | select(.type=="osd") | "    osd.\(.id): pgs=\(.pgs) used_kb=\(.kb_used) (used \(.utilization | floor)%)"' \
        2>/dev/null || true)

    log "[monitor] $health"
    if [ -n "$df_block" ]; then printf '%s\n' "$df_block" | tee -a "$MONITOR_LOG"; fi
    printf '%s' "$osd_block" | tee -a "$MONITOR_LOG"
    log "    AGG  : user=$(printf '%15d' "$agg_user")  device=$(printf '%15d' "$agg_dev")"

    # OSD liveness watchdog: bail as soon as any OSD's asok has been
    # unresponsive for OSD_FAIL_THRESHOLD consecutive samples. That means the
    # daemon is either dead or stuck in mkfs/replay; either way, fio is
    # wedged and waiting out the full stall window is wasted time.
    local i_dn
    for i_dn in $(seq 0 $((NUM_OSDS - 1))); do
        if [ "${OSD_FAIL[i_dn]}" -ge "$OSD_FAIL_THRESHOLD" ]; then
            log "[monitor] OSD DOWN — osd.$i_dn asok unresponsive for ${OSD_FAIL[i_dn]} samples; killing fio pid=$FIO_PID"
            kill -9 "$FIO_PID" 2>/dev/null || true
            OSD_DOWN=1
            return 1
        fi
    done

    # Stall watchdog: key on aggregate user_written. That counter
    # advances exactly when fio submits new writes; once fio is wedged
    # (waiting on completions that aren't coming back from librados),
    # user_written freezes. We deliberately ignore device_written —
    # it can trickle by a few KB per poll due to GC/metadata flushes
    # even when fio has made no further submissions, which would mask
    # a real wedge.
    local sig="${agg_user}"
    if [ -n "$PREV_SIG" ] && [ "$sig" = "$PREV_SIG" ]; then
        STABLE=$((STABLE + 1))
        log "[monitor] perfcounters unchanged (${STABLE}/${STALL_MULTI})"
        if [ "$STABLE" -ge "$STALL_MULTI" ]; then
            log "[monitor] WATCHDOG fired — killing fio pid=$FIO_PID"
            kill -9 "$FIO_PID" 2>/dev/null || true
            WATCHDOG_FIRED=1
            return 1
        fi
    else
        STABLE=0
    fi
    PREV_SIG="$sig"
    return 0
}

# Main monitor loop. Exits on fio death OR watchdog kill.
while kill -0 "$FIO_PID" 2>/dev/null; do
    sleep "$PERIOD"
    if ! kill -0 "$FIO_PID" 2>/dev/null; then break; fi
    monitor_iter || break
done

# Wait for fio to fully exit, capture rc safely (it may have been signal-killed).
set +e
wait "$FIO_PID"
FIO_RC=$?
set -e
log "[bench] fio exit rc=$FIO_RC (watchdog_fired=$WATCHDOG_FIRED osd_down=$OSD_DOWN)"

# ---- step 8: capture final asok counters + build report ----
log "[report] waiting 11s for OSDs to flush final 10s WAF perf counter timers..."
sleep 11

log "[report] capturing final seastore_waf snapshots"
rm -f "$OUT_DIR/asok"/*.json "$OUT_DIR/asok"/*.err
for i in $(seq 0 $((NUM_OSDS - 1))); do
    if ! timeout 10 $CEPH daemon "osd.$i" perfcounters_dump seastore_waf \
            >"$OUT_DIR/asok/osd.$i.seastore_waf.json" 2>"$OUT_DIR/asok/osd.$i.err"; then
        log "    osd.$i: asok dump failed (see $OUT_DIR/asok/osd.$i.err)"
    fi
done

log "[report] building waf_report.txt"
if WAF_NUM_OSDS="$NUM_OSDS" WAF_OUT_DIR="$OUT_DIR" \
   WAF_FIO_JSON="$FIO_JSON" WAF_REPORT="$OUT_DIR/waf_report.txt" \
   python3 "$SCRIPT_DIR/waf_report.py" 2>&1 | tee -a "$MONITOR_LOG"; then
    :
else
    log "[report] waf_report.py exited non-zero"
fi

echo
echo "=== waf_report.txt ==="
cat "$OUT_DIR/waf_report.txt" 2>/dev/null || echo "(report not produced)"
echo "======================"
echo
log "[bench] full monitor log: $MONITOR_LOG"

# ---- step 9: teardown (unless --no-teardown) ----
# Teardown deletes BASE_DIR (incl. $MONITOR_LOG), so switch to plain
# echo from here on — log() would tee to a file that's about to vanish.
if [ "$DO_TEARDOWN" = "1" ]; then
    echo "[teardown] stopping cluster + reclaiming devices"
    pkill -9 -f 'fio.*fio\.job'              2>/dev/null || true
    pkill -9 -f crimson-osd                  2>/dev/null || true
    pkill -9 -f 'ceph-run.*crimson-osd'      2>/dev/null || true
    sleep 1
    pkill -9 -f 'ceph-mon -i'                2>/dev/null || true
    pkill -9 -f 'ceph-mgr -i'                2>/dev/null || true
    sleep 2
    "$SCRIPT_DIR/setup_osd_emul.sh" --teardown "$BASE_DIR" >/dev/null 2>&1 || true
    echo "[teardown] done"
else
    log "[teardown] skipped (--no-teardown); cluster left running"
fi

# ---- exit code ----
if [ "$WATCHDOG_FIRED" = "1" ]; then
    exit 2
fi
if [ "$OSD_DOWN" = "1" ]; then
    exit 3
fi
if [ "$FIO_RC" -ne 0 ]; then
    exit "$FIO_RC"
fi
exit 0
