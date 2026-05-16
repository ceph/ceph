#!/bin/bash
set -euo pipefail

# run_waf_bench.sh — drive waf_bench.fio against an already-running
# crimson cluster (typically the one start_multi_osd.sh brought up),
# capture FIO output, query each OSD's seastore_waf perf counters, and
# emit a per-run report. The report formatter and optional plotting
# step land in subsequent commits in the addendum; this commit just
# wires the pool/image setup, the FIO invocation, and basic teardown.

usage() {
    cat <<EOF
Usage: $0 <NUM_OSDS> <BASE_DIR> [--runtime <seconds>] [--size <fio_size>]
                                [--nrfiles <N>] [--pool <name>]

Required:
  NUM_OSDS     number of crimson-osd instances the cluster brought up
               (used as fio numjobs and to enumerate asok sockets later)
  BASE_DIR     same BASE_DIR start_multi_osd.sh used; the FIO logs and
               run report land under \$BASE_DIR/waf_bench/

Optional:
  --runtime    fio time_based runtime in seconds (default 120)
  --size       fio per-job total size (default 256m, divided across
               --nrfiles rados objects)
  --nrfiles    rados objects per fio job (default 32). Per-object size
               (size/nrfiles) MUST be <= seastore_default_max_object_size
               (16 MiB default); the script errors out otherwise.
  --pool       rados pool name (default waf-test)

Note: uses fio's rados ioengine, not rbd, because crimson rejects
librbd image creation with EOPNOTSUPP. See waf_bench.fio header.
EOF
    exit 1
}

# ---- arg parsing ----
RUNTIME=120
SIZE=256m
NRFILES=32
POOL=waf-test
POSITIONAL=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --runtime)  RUNTIME="$2"; shift 2 ;;
        --size)     SIZE="$2"; shift 2 ;;
        --nrfiles)  NRFILES="$2"; shift 2 ;;
        --pool)     POOL="$2"; shift 2 ;;
        --) shift; break ;;
        -*) echo "Unknown flag: $1" >&2; usage ;;
        *) POSITIONAL+=("$1"); shift ;;
    esac
done
set -- "${POSITIONAL[@]:-}"

[[ $# -eq 2 ]] || usage
NUM_OSDS="$1"
BASE_DIR="$(realpath "$2")"

if ! [[ "$NUM_OSDS" =~ ^[1-9][0-9]*$ ]]; then
    echo "Error: NUM_OSDS must be a positive integer (got '$NUM_OSDS')" >&2
    exit 1
fi
if [ ! -d "$BASE_DIR" ]; then
    echo "Error: BASE_DIR '$BASE_DIR' does not exist (cluster not running?)" >&2
    exit 1
fi
if ! [[ "$NRFILES" =~ ^[1-9][0-9]*$ ]]; then
    echo "Error: --nrfiles must be a positive integer (got '$NRFILES')" >&2
    exit 1
fi

# ---- per-object size guard ----
# fio rados engine maps each "file" to one rados object; per-object
# capacity = size/nrfiles. Crimson seastore aborts with
# `ceph_assert(size <= max_object_size)` in object_data_handler.cc when
# a single object exceeds seastore_default_max_object_size (16 MiB
# default). Reject configurations that would trip that assert before
# we even start fio.
parse_size_bytes() {
    local s="$1"
    local n="${s%[KMGTkmgt]}"
    local unit="${s:${#n}}"
    if ! [[ "$n" =~ ^[0-9]+$ ]]; then
        echo "Error: --size '$s' is not a number with optional K/M/G/T suffix" >&2
        return 1
    fi
    case "${unit,,}" in
        ""|b) echo "$n" ;;
        k)    echo "$((n * 1024)) " ;;
        m)    echo "$((n * 1024 * 1024)) " ;;
        g)    echo "$((n * 1024 * 1024 * 1024)) " ;;
        t)    echo "$((n * 1024 * 1024 * 1024 * 1024)) " ;;
        *)    echo "Error: --size '$s' has unsupported unit '$unit'" >&2; return 1 ;;
    esac
}
SIZE_BYTES=$(parse_size_bytes "$SIZE") || exit 1
SIZE_BYTES=${SIZE_BYTES// /}
PER_OBJ_BYTES=$(( SIZE_BYTES / NRFILES ))
MAX_OBJ_BYTES=$(( 16 * 1024 * 1024 ))    # seastore_default_max_object_size
if [ "$PER_OBJ_BYTES" -gt "$MAX_OBJ_BYTES" ]; then
    cat <<EOF >&2
Error: per-object size (size/nrfiles) = $PER_OBJ_BYTES B exceeds
       seastore_default_max_object_size = $MAX_OBJ_BYTES B (16 MiB).
       The OSD will abort on the first write past that cap with
       ceph_assert(size <= max_object_size) in object_data_handler.cc.

       Either reduce --size (currently $SIZE) or raise --nrfiles
       (currently $NRFILES) so that size/nrfiles <= 16M. Example:
       --size 256m --nrfiles 32  (8 MiB per object).
EOF
    exit 1
fi

OUT_DIR="$BASE_DIR/waf_bench"
mkdir -p "$OUT_DIR"

# ---- locate ceph build tree ----
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CEPH_ROOT=$(realpath "$SCRIPT_DIR/../../..")
CEPH_BIN="$CEPH_ROOT/build/bin"
CEPH_CONF="$CEPH_ROOT/build/ceph.conf"

if [ ! -x "$CEPH_BIN/ceph" ] || [ ! -f "$CEPH_CONF" ]; then
    echo "Error: ceph/ceph.conf not found under $CEPH_ROOT/build" >&2
    echo "       Run start_multi_osd.sh first to bring the cluster up." >&2
    exit 1
fi

if ! command -v fio >/dev/null 2>&1; then
    echo "Error: fio is not installed; this benchmark requires fio>=3.x with the rados ioengine." >&2
    exit 1
fi

# ---- pool setup ----
echo "[waf_bench] Creating pool '$POOL' (idempotent)"
"$CEPH_BIN/ceph" -c "$CEPH_CONF" osd pool create "$POOL" 8 8 >/dev/null 2>&1 \
    || echo "[waf_bench] Pool '$POOL' already exists or creation skipped"

# fio's rados ioengine writes objects directly into the pool; no image
# pre-creation step is needed (and crimson currently rejects rbd image
# create with EOPNOTSUPP, see waf_bench.fio header).

# ---- run fio ----
FIO_JOB="$SCRIPT_DIR/waf_bench.fio"
FIO_JSON="$OUT_DIR/fio.json"
FIO_STDOUT="$OUT_DIR/fio.stdout.log"

echo "[waf_bench] Running fio job_file=$FIO_JOB num_jobs=$NUM_OSDS runtime=${RUNTIME}s size=$SIZE nrfiles=$NRFILES (per-obj $((PER_OBJ_BYTES / 1024 / 1024)) MiB)"
echo "[waf_bench] FIO logs and JSON output: $OUT_DIR"

# Per-job logs are written to cwd by fio, so cd into OUT_DIR for the run.
(
    cd "$OUT_DIR"
    export WAF_CLIENT=admin
    export WAF_POOL="$POOL"
    export WAF_CONF="$CEPH_CONF"
    export WAF_NUM_JOBS="$NUM_OSDS"
    export WAF_RUNTIME="$RUNTIME"
    export WAF_SIZE="$SIZE"
    export WAF_NRFILES="$NRFILES"
    # fio's rados ioengine has its own `conf=` option (passed through
    # waf_bench.fio's WAF_CONF) — librados ignores CEPH_CONF env when
    # the engine supplies an explicit config path. Set CEPH_KEYRING
    # so the auth handshake finds the vstart-generated keyring.
    export CEPH_KEYRING="$CEPH_ROOT/build/keyring"
    fio --output-format=json --output="$FIO_JSON" "$FIO_JOB" \
        2>&1 | tee "$FIO_STDOUT"
)
FIO_RC=${PIPESTATUS[0]}

if [ "$FIO_RC" -ne 0 ]; then
    echo "[waf_bench] fio exited non-zero ($FIO_RC); see $FIO_STDOUT" >&2
    # Continue anyway — the report formatter handles a missing/partial
    # fio JSON without crashing, so the operator can still see the WAF
    # snapshot from the asok side. Exit code propagated at the end.
fi

# ---- per-OSD seastore_waf snapshot via asok ----
echo "[waf_bench] Capturing seastore_waf perf counters from each OSD"
ASOK_DUMP_DIR="$OUT_DIR/asok"
mkdir -p "$ASOK_DUMP_DIR"
for i in $(seq 0 $((NUM_OSDS - 1))); do
    OSD_DUMP="$ASOK_DUMP_DIR/osd.$i.seastore_waf.json"
    if "$CEPH_BIN/ceph" -c "$CEPH_CONF" daemon "osd.$i" \
            perfcounters_dump seastore_waf > "$OSD_DUMP" 2>"$ASOK_DUMP_DIR/osd.$i.err"; then
        echo "[waf_bench]   osd.$i -> $OSD_DUMP"
    else
        echo "[waf_bench]   osd.$i asok failed; see $ASOK_DUMP_DIR/osd.$i.err" >&2
        # Leave the .err file in place; the formatter copes with missing files.
    fi
done

# ---- emit waf_report.txt ----
REPORT="$OUT_DIR/waf_report.txt"
echo "[waf_bench] Building $REPORT"
WAF_NUM_OSDS="$NUM_OSDS" \
WAF_OUT_DIR="$OUT_DIR" \
WAF_FIO_JSON="$FIO_JSON" \
WAF_REPORT="$REPORT" \
python3 "$SCRIPT_DIR/waf_report.py"
REPORT_RC=$?
if [ "$REPORT_RC" -ne 0 ]; then
    echo "[waf_bench] report formatter exited $REPORT_RC; see stderr above" >&2
    exit "$REPORT_RC"
fi

echo "[waf_bench] Done. Report: $REPORT"
echo "[waf_bench] Per-OSD asok dumps: $ASOK_DUMP_DIR"

# ---- optional WAF-over-time plot ----
# Per spec: only attempt if matplotlib is importable; skip cleanly
# otherwise (no error). The plot script itself handles the
# absent-tool case and exits 0, so we don't gate on availability
# here -- just on whether python3 can run the script at all.
PLOT_PNG="$OUT_DIR/waf_over_time.png"
if command -v python3 >/dev/null 2>&1; then
    WAF_NUM_OSDS="$NUM_OSDS" \
    WAF_BASE_DIR="$BASE_DIR" \
    WAF_PLOT_OUT="$PLOT_PNG" \
    python3 "$SCRIPT_DIR/waf_plot.py" || true  # best-effort
    if [ -s "$PLOT_PNG" ]; then
        echo "[waf_bench] WAF-over-time plot: $PLOT_PNG"
    fi
else
    echo "[waf_bench] python3 absent; skipping WAF-over-time plot"
fi

# Propagate fio's exit code so callers (CI tasks etc.) see a real failure
# instead of "0 because the report wrote".
exit "$FIO_RC"
