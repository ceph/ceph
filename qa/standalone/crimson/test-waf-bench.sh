#!/bin/bash
# test-waf-bench.sh — qa/standalone end-to-end driver for the crimson
# SeaStore Write Amplification benchmark. Wires the four building
# blocks already in this directory into a single setup → bring-up →
# bench → teardown run, asserts the aggregate WAF stays under
# --waf-max (default 10) as a sanity gate, and exits non-zero on any
# step's failure or on the WAF assertion firing.
#
# Usage:
#   test-waf-bench.sh [--num-osds N] [--size-gb G] [--backing MODE]
#                     [--runtime SECONDS] [--fio-size SIZE]
#                     [--nrfiles N] [--waf-max VALUE]
#
# Defaults are tuned for a fast CI-style run on a single machine.
# This script is destructive of any crimson cluster already running
# in the implicit BASE_DIR ($CEPH_BUILD_ROOT/build/dev or
# $TMPDIR/waf-bench-XXXX if outside a ceph build tree).
set -euo pipefail

# ---- arg defaults ----
NUM_OSDS=2
SIZE_GB=4
BACKING=memory
RUNTIME=60
FIO_SIZE=256m
NRFILES=32
WAF_MAX=10

usage() {
    cat <<EOF >&2
Usage: $(basename "$0") [options]

Options:
  --num-osds N      crimson-osd instances to bring up (default: ${NUM_OSDS})
  --size-gb G       per-OSD device size in GiB (default: ${SIZE_GB}, min 4)
  --backing MODE    memory | file | auto (default: ${BACKING})
  --runtime SEC     fio time_based runtime (default: ${RUNTIME})
  --fio-size SIZE   fio per-job total size, divided by --nrfiles into
                    individual rados objects (default: ${FIO_SIZE})
  --nrfiles N       rados objects per fio job (default: ${NRFILES})
  --waf-max VALUE   maximum acceptable aggregate WAF; the script exits
                    non-zero if the measured aggregate WAF is >= VALUE
                    (default: ${WAF_MAX}; pass 0 to disable the gate)

Exits 0 on a clean run, non-zero on any failure (bring-up, bench,
teardown, WAF assertion). The aggregate WAF for the run is printed
and also lands in \$BASE_DIR/waf_bench/waf_report.txt.
EOF
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --num-osds)  NUM_OSDS="$2"; shift 2 ;;
        --size-gb)   SIZE_GB="$2"; shift 2 ;;
        --backing)   BACKING="$2"; shift 2 ;;
        --runtime)   RUNTIME="$2"; shift 2 ;;
        --fio-size)  FIO_SIZE="$2"; shift 2 ;;
        --nrfiles)   NRFILES="$2"; shift 2 ;;
        --waf-max)   WAF_MAX="$2"; shift 2 ;;
        -h|--help)   usage ;;
        *) echo "Unknown flag: $1" >&2; usage ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CEPH_ROOT="$(realpath "$SCRIPT_DIR/../../..")"

# Prefer the in-tree build dir so the operator inherits the existing
# vstart conventions; fall back to a tmp dir if there is no build tree
# (the bench can't run without a built crimson-osd, but the message is
# clearer if it fails on a missing binary than on a missing BASE_DIR).
if [ -x "$CEPH_ROOT/build/bin/crimson-osd" ]; then
    BASE_DIR="$CEPH_ROOT/build/dev"
else
    BASE_DIR="$(mktemp -d -t waf-bench-XXXX)"
fi

cleanup() {
    # Best-effort: stop_multi_osd.sh would normally do this, but its
    # presence is not guaranteed in older trees and we want the
    # teardown to be robust to having reached only a partial bring-up.
    pkill -9 -f crimson-osd      2>/dev/null || true
    pkill -9 -f "ceph-mon -i a"  2>/dev/null || true
    pkill -9 -f "ceph-mgr -i x"  2>/dev/null || true
    pkill -9 -f ceph-run         2>/dev/null || true
    sleep 1
    if [ -d "$BASE_DIR" ]; then
        # Reuse setup_osd_emul.sh --teardown so memory- and file-mode
        # devices are reclaimed via the same path that allocated them.
        "$SCRIPT_DIR/setup_osd_emul.sh" --teardown "$BASE_DIR" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

echo "[test-waf-bench] config: num_osds=$NUM_OSDS size_gb=$SIZE_GB backing=$BACKING"
echo "[test-waf-bench]         runtime=${RUNTIME}s fio_size=$FIO_SIZE nrfiles=$NRFILES"
echo "[test-waf-bench] base_dir=$BASE_DIR"

# Make sure we're starting from a clean slate so this is idempotent
# against operators who ran a prior bench in the same tree.
cleanup
mkdir -p "$BASE_DIR"

# ---- step 1: setup + bring-up ----
echo "[test-waf-bench] starting $NUM_OSDS-OSD crimson cluster"
"$SCRIPT_DIR/start_multi_osd.sh" --backing="$BACKING" "$NUM_OSDS" "$SIZE_GB" "$BASE_DIR"

# ---- step 2: bench ----
echo "[test-waf-bench] running waf_bench"
"$SCRIPT_DIR/run_waf_bench.sh" \
    --runtime "$RUNTIME" --size "$FIO_SIZE" --nrfiles "$NRFILES" \
    "$NUM_OSDS" "$BASE_DIR"

REPORT="$BASE_DIR/waf_bench/waf_report.txt"
if [ ! -s "$REPORT" ]; then
    echo "[test-waf-bench] FAIL: report file not produced at $REPORT" >&2
    exit 1
fi

# Surface the aggregate WAF line and a single-line FIO summary so the
# caller can see at a glance what the run measured; full report stays
# under $BASE_DIR/waf_bench/.
echo "[test-waf-bench] aggregate result:"
grep -E "^  WAF " "$REPORT" || true
grep -E "^           TOTAL " "$REPORT" || true

# ---- step 3: WAF < $WAF_MAX sanity gate ----
# Pull the aggregate WAF the formatter computed (sum(d)/sum(u) across
# all OSDs that produced an asok dump, the same number the report
# header surfaces). A missing line means waf_report.py couldn't find
# usable counters — usually that the build is WITH_SEASTORE_WAF_COUNTERS=OFF
# or that the cluster lost an OSD mid-bench; either way, fail loudly
# rather than silently passing the gate.
AGG_LINE=$(grep -E "^  WAF " "$REPORT" || true)
if [ -z "$AGG_LINE" ]; then
    echo "[test-waf-bench] FAIL: no aggregate WAF in $REPORT" >&2
    echo "                (build with -DWITH_SEASTORE_WAF_COUNTERS=ON and confirm" >&2
    echo "                 the cluster stayed healthy through the run)" >&2
    exit 1
fi
WAF_VALUE=$(echo "$AGG_LINE" | awk -F': ' '{print $2}' | tr -d ' ')

# Compare in awk for float-safe arithmetic; bash builtins are
# integer-only and 'bc' isn't universally available.
if [ "$WAF_MAX" = "0" ]; then
    echo "[test-waf-bench] WAF gate disabled (--waf-max 0); measured WAF=$WAF_VALUE"
else
    GATE_RESULT=$(awk -v w="$WAF_VALUE" -v m="$WAF_MAX" 'BEGIN { print (w < m) ? "PASS" : "FAIL" }')
    if [ "$GATE_RESULT" != "PASS" ]; then
        echo "[test-waf-bench] FAIL: aggregate WAF=$WAF_VALUE >= --waf-max=$WAF_MAX" >&2
        echo "                See $REPORT for the per-OSD breakdown." >&2
        exit 1
    fi
    echo "[test-waf-bench] WAF gate: $WAF_VALUE < $WAF_MAX  (PASS)"
fi

echo "[test-waf-bench] PASS"
