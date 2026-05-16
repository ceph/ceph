#!/bin/bash
set -euo pipefail

usage() {
    cat <<EOF
Usage: $0 [options] <NUM_OSDS> <SIZE_GB> <BASE_DIR>

Brings up an isolated crimson cluster end-to-end:
  0. preflight  — refuse to start if leftover state is present
  1. devices    — null_blk or loop-backed emulated devices
  2. vstart     — mon, mgr, N crimson-osds, wait for up+active
  3. pool       — pre-create a workload pool (size, pg_num configurable)
  4. balancer   — enable upmap balancer and wait for osd df to converge

Options:
  --backing=memory|file|auto   (default: auto; passed to setup_osd_emul.sh)
  --no-preflight               skip step 0
  --no-pool                    skip step 3 (no workload pool is created)
  --pool NAME                  workload pool name (default: waf-test)
  --pool-pg N                  pool pg_num (default: 256)
  --pool-size N                pool replication size + min_size (default: 1)
  --no-balancer                skip step 4
  --balancer-timeout SECONDS   max time waiting for df convergence (default: 300)
  --balancer-interval SECONDS  poll interval (default: 5)
  --balancer-stable N          consecutive identical samples required (default: 3)

Exits 0 only after every requested stage completes — for the balancer
stage that means "osd df pg counts unchanged across --balancer-stable
consecutive samples." Returns non-zero on preflight failure, on
bring-up timeout, or on balancer-convergence timeout.
EOF
    exit 1
}

# ---- arg parsing ----
BACKING_ARG=""
DO_PREFLIGHT=1
DO_POOL=1
DO_BALANCER=1
POOL_NAME=waf-test
POOL_PG=256
POOL_SIZE=1
BAL_TIMEOUT=300
BAL_INTERVAL=5
BAL_STABLE=3
POSITIONAL=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --backing=*)              BACKING_ARG="$1"; shift ;;
        --backing)                BACKING_ARG="--backing=${2:-}"; shift 2 || usage ;;
        --no-preflight)           DO_PREFLIGHT=0; shift ;;
        --no-pool)                DO_POOL=0; shift ;;
        --pool)                   POOL_NAME="$2"; shift 2 ;;
        --pool-pg)                POOL_PG="$2"; shift 2 ;;
        --pool-size)              POOL_SIZE="$2"; shift 2 ;;
        --no-balancer)            DO_BALANCER=0; shift ;;
        --balancer-timeout)       BAL_TIMEOUT="$2"; shift 2 ;;
        --balancer-interval)      BAL_INTERVAL="$2"; shift 2 ;;
        --balancer-stable)        BAL_STABLE="$2"; shift 2 ;;
        -h|--help)                usage ;;
        --) shift; break ;;
        -*) echo "Unknown flag: $1" >&2; usage ;;
        *)  POSITIONAL+=("$1"); shift ;;
    esac
done
set -- "${POSITIONAL[@]:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CEPH_ROOT="$(realpath "$SCRIPT_DIR/../../..")"
CEPH_BUILD="$CEPH_ROOT/build"

do_cleanup() {
    echo "[cleanup] Killing processes..."
    pkill -9 -f 'fio|crimson-osd|ceph-run' || true
    sleep 1
    pkill -9 -f 'ceph-mon -i|ceph-mgr -i' || true
    sleep 2
    echo "[cleanup] Removing null_blk devices..."
    for n in /sys/kernel/config/nullb/nullb*; do
        [ -d "$n" ] || continue
        echo 0 | sudo tee "$n/power" >/dev/null
        sudo rmdir "$n"
    done
    echo "[cleanup] Removing loop devices..."
    for d in /dev/loop[0-9]*; do
        losetup -a 2>/dev/null | grep -q "^$d:" && sudo losetup -d "$d" || true
    done
    if [ -d "$CEPH_BUILD/dev" ]; then
        echo "[cleanup] Removing $CEPH_BUILD/dev..."
        rm -rf "$CEPH_BUILD/dev"
    fi
    echo "[cleanup] Done."
}

if [[ $# -ne 3 ]]; then usage; fi

# Every invocation starts from a clean slate.
do_cleanup

NUM_OSDS="$1"
SIZE_GB="$2"
BASE_DIR="$(realpath -m "$3")"

# ---- step 0: preflight ----
preflight() {
    local bad=()

    if pgrep -f crimson-osd >/dev/null 2>&1; then
        bad+=("crimson-osd process(es) running: $(pgrep -af crimson-osd | head -3 | sed 's/^/      /')")
    fi
    if pgrep -f 'ceph-mon -i' >/dev/null 2>&1; then
        bad+=("ceph-mon process(es) running")
    fi
    if pgrep -f 'ceph-mgr -i' >/dev/null 2>&1; then
        bad+=("ceph-mgr process(es) running")
    fi
    if pgrep -f 'ceph-run.*crimson-osd' >/dev/null 2>&1; then
        bad+=("ceph-run wrapper(s) running")
    fi
    if pgrep -x fio >/dev/null 2>&1; then
        bad+=("fio process(es) running")
    fi

    if [ -d /sys/kernel/config/nullb ]; then
        local nullbs
        nullbs=$(find /sys/kernel/config/nullb -mindepth 1 -maxdepth 1 -type d ! -name features -printf '%f ' 2>/dev/null)
        if [ -n "$nullbs" ]; then
            bad+=("leftover null_blk devices: $nullbs")
        fi
    fi

    # Any loop device pointing at a 'backing.img' under the build tree is
    # a leftover from a prior file-backed run.
    local loops
    loops=$(losetup -a 2>/dev/null | grep -E "/dev/loop[0-9]+:.*backing\.img" || true)
    if [ -n "$loops" ]; then
        bad+=("leftover loop device(s): $(echo "$loops" | head -3 | tr '\n' ';')")
    fi

    if [ -e "$BASE_DIR" ]; then
        bad+=("BASE_DIR already exists: $BASE_DIR")
    fi

    if [ ${#bad[@]} -gt 0 ]; then
        echo "[preflight] FAIL: refusing to start with stale state present:" >&2
        local b
        for b in "${bad[@]}"; do
            echo "  - $b" >&2
        done
        cat >&2 <<EOF

This indicates the unconditional cleanup at the top of $0 failed to
remove some residue. Investigate manually, then re-run.
Or re-run with --no-preflight to bypass these checks (not recommended).
EOF
        exit 1
    fi

    echo "[preflight] OK — no leftover processes, devices, or BASE_DIR."
}

# ---- step 4 helpers: balancer + DF convergence ----
osd_df_signature() {
    # Compact, deterministic representation of per-OSD PG counts. Used as
    # the convergence key — when this string is stable across $BAL_STABLE
    # samples, we declare the balancer done.
    "$CEPH_BUILD/bin/ceph" -c "$CEPH_BUILD/ceph.conf" osd df -f json 2>/dev/null \
        | jq -c '.nodes | map(select(.type=="osd")) | map({id, pgs}) | sort_by(.id)'
}

wait_for_df_convergence() {
    local timeout="$1" interval="$2" need_stable="$3"
    local prev="" cur stable=0 elapsed=0
    echo "[balancer] waiting for osd df pg counts to converge"
    echo "[balancer]   timeout=${timeout}s  interval=${interval}s  need_stable=${need_stable}"

    while [ "$elapsed" -lt "$timeout" ]; do
        cur=$(osd_df_signature)
        if [ -z "$cur" ] || [ "$cur" = "null" ]; then
            sleep "$interval"; elapsed=$((elapsed + interval))
            continue
        fi
        if [ "$cur" = "$prev" ]; then
            stable=$((stable + 1))
            echo "[balancer]   stable ${stable}/${need_stable}: ${cur}"
            if [ "$stable" -ge "$need_stable" ]; then
                echo "[balancer] converged after ${elapsed}s — exiting cleanly."
                return 0
            fi
        else
            stable=1
            echo "[balancer]   sample: ${cur}"
        fi
        prev="$cur"
        sleep "$interval"
        elapsed=$((elapsed + interval))
    done

    echo "[balancer] FAIL: pg counts did not converge within ${timeout}s." >&2
    echo "          Last sample: ${prev:-<none>}" >&2
    return 1
}

# =========================================================================
# Step 0 — preflight (refuse to start on stale state)
# =========================================================================
if [ "$DO_PREFLIGHT" = "1" ]; then
    preflight
else
    echo "[preflight] skipped (--no-preflight)"
fi

# =========================================================================
# Step 1 — devices (setup_osd_emul.sh) + vstart cluster bring-up
# =========================================================================
if [ -n "$BACKING_ARG" ]; then
    "$SCRIPT_DIR/setup_osd_emul.sh" "$BACKING_ARG" "$NUM_OSDS" "$SIZE_GB" "$BASE_DIR"
else
    "$SCRIPT_DIR/setup_osd_emul.sh" "$NUM_OSDS" "$SIZE_GB" "$BASE_DIR"
fi

export CEPH_DEV_DIR="$BASE_DIR"
export CEPH_OUT_DIR="$BASE_DIR/out"

cd "$CEPH_BUILD"

# Snapshot the device_path / backing_mode markers — vstart's OSD mkfs
# step wipes $BASE_DIR/osd$i out from under us; we restore them after
# bring-up so the teardown path can still find them.
declare -a SNAP_DEV SNAP_MODE
DEVS=""
for i in $(seq 0 $((NUM_OSDS - 1))); do
    SNAP_DEV[$i]=$(cat "$BASE_DIR/osd$i/device_path")
    if [ -f "$BASE_DIR/osd$i/backing_mode" ]; then
        SNAP_MODE[$i]=$(cat "$BASE_DIR/osd$i/backing_mode")
    else
        SNAP_MODE[$i]=""
    fi
    if [ -z "$DEVS" ]; then DEVS="${SNAP_DEV[$i]}"; else DEVS="$DEVS,${SNAP_DEV[$i]}"; fi
done

echo "[vstart] starting MON/MGR + $NUM_OSDS crimson-osd"
mkdir "$CEPH_OUT_DIR"
MON=1 MGR=1 OSD="$NUM_OSDS" MDS=0 ../src/vstart.sh -n -x --without-dashboard \
    --crimson --seastore --seastore-devs "$DEVS" --seastore-device-size "${SIZE_GB}G" >> "$CEPH_OUT_DIR/vstart.log" 2>&1

for i in $(seq 0 $((NUM_OSDS - 1))); do
    OSD_DIR="$BASE_DIR/osd$i"
    mkdir -p "$OSD_DIR"
    echo "${SNAP_DEV[$i]}" > "$OSD_DIR/device_path"
    if [ -n "${SNAP_MODE[$i]}" ]; then
        echo "${SNAP_MODE[$i]}" > "$OSD_DIR/backing_mode"
    fi
done

echo "[vstart] capturing OSD pids"
for i in $(seq 0 $((NUM_OSDS - 1))); do
    OSD_DIR="$BASE_DIR/osd$i"
    T=60
    while [ ! -f "$CEPH_OUT_DIR/osd.$i.pid" ] && [ $T -gt 0 ]; do
        sleep 1
        T=$((T - 1))
    done
    if [ -f "$CEPH_OUT_DIR/osd.$i.pid" ]; then
        cp "$CEPH_OUT_DIR/osd.$i.pid" "$OSD_DIR/crimson-osd.pid"
        echo "[vstart]   osd.$i pid=$(cat "$OSD_DIR/crimson-osd.pid")"
    else
        echo "[vstart]   WARN: osd.$i pid file not found"
    fi
done

echo "[vstart] waiting for all OSDs up+active"
TIMEOUT=120
while [[ $TIMEOUT -gt 0 ]]; do
    STATUS=$(./bin/ceph -c ceph.conf osd stat -f json)
    UP=$(echo "$STATUS" | jq '.num_up_osds')
    IN=$(echo "$STATUS" | jq '.num_in_osds')
    TOTAL=$(echo "$STATUS" | jq '.num_osds')
    echo "[vstart]   total=$TOTAL up=$UP in=$IN"
    if [[ "$UP" -eq "$NUM_OSDS" ]] && [[ "$IN" -eq "$NUM_OSDS" ]]; then
        echo "[vstart] all OSDs up+active"
        break
    fi
    sleep 2
    TIMEOUT=$((TIMEOUT - 2))
done
if [[ $TIMEOUT -le 0 ]]; then
    echo "[vstart] FAIL: timeout waiting for OSDs to become up+active" >&2
    exit 1
fi

# =========================================================================
# Step 3 — pool create + size override
# =========================================================================
if [ "$DO_POOL" = "1" ]; then
    echo "[pool] creating '$POOL_NAME' (pg_num=$POOL_PG, size=$POOL_SIZE)"
    ./bin/ceph -c ceph.conf osd pool create "$POOL_NAME" "$POOL_PG" "$POOL_PG" replicated >/dev/null
    ./bin/ceph -c ceph.conf osd pool set    "$POOL_NAME" size     "$POOL_SIZE" --yes-i-really-mean-it >/dev/null
    ./bin/ceph -c ceph.conf osd pool set    "$POOL_NAME" min_size "$POOL_SIZE" >/dev/null
    # Surface the effective parameters so the operator can see them.
    local_size=$(./bin/ceph -c ceph.conf osd pool get "$POOL_NAME" size      2>&1 | awk '/^size:/{print $2}')
    local_min=$(./bin/ceph -c ceph.conf osd pool get  "$POOL_NAME" min_size  2>&1 | awk '/^min_size:/{print $2}')
    local_pg=$(./bin/ceph -c ceph.conf osd pool get   "$POOL_NAME" pg_num    2>&1 | awk '/^pg_num:/{print $2}')
    echo "[pool]   $POOL_NAME: size=$local_size min_size=$local_min pg_num=$local_pg"
else
    echo "[pool] skipped (--no-pool)"
fi

# =========================================================================
# Step 4 — balancer + wait for osd df convergence
# =========================================================================
if [ "$DO_BALANCER" = "1" ]; then
    echo "[balancer] enabling upmap mode"
    ./bin/ceph -c ceph.conf balancer mode upmap >/dev/null 2>&1 || true
    ./bin/ceph -c ceph.conf balancer on        >/dev/null 2>&1 || true

    if ! wait_for_df_convergence "$BAL_TIMEOUT" "$BAL_INTERVAL" "$BAL_STABLE"; then
        exit 1
    fi
else
    echo "[balancer] skipped (--no-balancer)"
fi

echo ""
echo "[done] cluster ready."
./bin/ceph -c ceph.conf osd df 2>/dev/null | grep -v -E "^\*\*\*|WARNING|^$" || true
