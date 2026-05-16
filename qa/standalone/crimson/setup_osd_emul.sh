#!/bin/bash
set -euo pipefail

usage() {
    cat <<EOF
Usage: $0 [--backing=memory|file|auto] <NUM_OSDS> <SIZE_GB> <BASE_DIR>
       $0 --teardown <BASE_DIR>

Options:
  --backing=auto    (default) memory if total <= 75% of MemTotal, else file
  --backing=memory  force null_blk memory-backed mode
  --backing=file    force loop device over sparse file
EOF
    exit 1
}

# ---- Argument parsing ----
BACKING="auto"
POSITIONAL=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --backing=*)
            BACKING="${1#--backing=}"
            shift
            ;;
        --backing)
            BACKING="${2:-}"
            shift 2 || usage
            ;;
        *)
            POSITIONAL+=("$1")
            shift
            ;;
    esac
done
set -- "${POSITIONAL[@]:-}"

case "$BACKING" in
    memory|file|auto) ;;
    *) echo "Error: --backing must be memory, file, or auto (got '$BACKING')" >&2; exit 1 ;;
esac

# ---- Teardown path ----
if [[ "${1:-}" == "--teardown" ]]; then
    if [[ $# -ne 2 ]]; then
        usage
    fi
    BASE_DIR="$2"
    echo "Teardown mode for BASE_DIR=$BASE_DIR"

    if [ -d "$BASE_DIR" ]; then
        for osd_dir in "$BASE_DIR"/osd*; do
            [ -d "$osd_dir" ] || continue
            DEV_PATH=""
            [ -f "$osd_dir/device_path" ] && DEV_PATH=$(cat "$osd_dir/device_path")
            MODE=""
            [ -f "$osd_dir/backing_mode" ] && MODE=$(cat "$osd_dir/backing_mode")

            # Infer mode from device path if marker is missing (legacy dirs).
            if [ -z "$MODE" ] && [ -n "$DEV_PATH" ]; then
                case "$DEV_PATH" in
                    /dev/nullb*) MODE="memory" ;;
                    /dev/loop*)  MODE="file" ;;
                esac
            fi

            case "$MODE" in
                memory)
                    DEV_NAME=$(basename "$DEV_PATH")
                    CONFIG_DIR="/sys/kernel/config/nullb/$DEV_NAME"
                    if [ -d "$CONFIG_DIR" ]; then
                        echo 0 | sudo tee "$CONFIG_DIR/power" >/dev/null
                        sudo rmdir "$CONFIG_DIR"
                        echo "Removed null_blk device $DEV_NAME"
                    fi
                    ;;
                file)
                    if [ -n "$DEV_PATH" ] && [ -b "$DEV_PATH" ]; then
                        if sudo losetup -d "$DEV_PATH" 2>/dev/null; then
                            echo "Detached loop device $DEV_PATH"
                        fi
                    fi
                    if [ -f "$osd_dir/backing.img" ]; then
                        rm -f "$osd_dir/backing.img"
                        echo "Removed sparse file $osd_dir/backing.img"
                    fi
                    ;;
                "")
                    echo "Warning: $osd_dir has no backing_mode and no device_path; skipping device cleanup" >&2
                    ;;
                *)
                    echo "Warning: $osd_dir has unknown backing_mode '$MODE'; skipping device cleanup" >&2
                    ;;
            esac
        done
        rm -rf "$BASE_DIR"
    fi
    exit 0
fi

if [[ $# -ne 3 ]]; then
    usage
fi

NUM_OSDS="$1"
SIZE_GB="$2"
BASE_DIR="$3"

if ! [[ "$NUM_OSDS" =~ ^[0-9]+$ ]] || ! [[ "$SIZE_GB" =~ ^[0-9]+$ ]]; then
    echo "Error: NUM_OSDS and SIZE_GB must be integers." >&2
    exit 1
fi

# Empirical minimum: SeaStore's SegmentCleaner aborts under sustained
# writes (async_cleaner.cc: "device size setting is too small") when
# the per-OSD device is smaller than this. Bisected against a 90 s
# steady-state run of waf_bench.fio (4 KiB randwrite, iodepth=64,
# nrfiles=32, 4 OSDs):
#   -  3 GiB: passes one 90 s bench with HEALTH_OK
#   -  2 GiB: takes out 3 of 4 OSDs with out-of-space aborts
# We enforce 4 GiB rather than the bisected 3 GiB boundary to keep a
# small safety margin for slightly longer or heavier workloads. The
# threshold is the SAME for memory- and file-backed modes because the
# constraint is on segment headroom, not the backing technology.
MIN_SIZE_GB=4
if [ "$SIZE_GB" -lt "$MIN_SIZE_GB" ]; then
    cat <<EOF >&2
Error: SIZE_GB=$SIZE_GB is below the enforced minimum of $MIN_SIZE_GB GiB.

SeaStore's SegmentCleaner aborts mid-bench under sustained writes
when the per-OSD device is too small:
  src/crimson/os/seastore/async_cleaner.cc:
    abort(seastore device size setting is too small)

The minimum was bisected against a 90 s waf_bench.fio steady-state
run (4 KiB randwrite, iodepth=64, nrfiles=32, 4 OSDs):
  -  3 GiB: PASS (HEALTH_OK, no aborts)
  -  2 GiB: FAIL (3-of-4 OSDs aborted, HEALTH_WARN)
We round up from 3 to 4 GiB to keep a small safety margin.

Recommended: 20 GiB per OSD for comfortable headroom under longer
benches.
EOF
    exit 1
fi

# ---- Backing-mode selection ----
TOTAL_BYTES=$(( NUM_OSDS * SIZE_GB * 1024 * 1024 * 1024 ))
MEM_KB=$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)
MEM_BYTES=$(( MEM_KB * 1024 ))
MEM_THRESHOLD_BYTES=$(( MEM_BYTES * 3 / 4 ))

human() {
    awk -v b="$1" 'BEGIN {
        if (b >= 1024^4) printf "%.2f TiB", b/(1024^4);
        else if (b >= 1024^3) printf "%.1f GiB", b/(1024^3);
        else printf "%.0f MiB", b/(1024^2);
    }'
}

if [ "$BACKING" = "auto" ]; then
    if [ "$TOTAL_BYTES" -le "$MEM_THRESHOLD_BYTES" ]; then
        EFFECTIVE_BACKING="memory"
        REL_OP="<="
    else
        EFFECTIVE_BACKING="file"
        REL_OP=">"
    fi
else
    EFFECTIVE_BACKING="$BACKING"
    if [ "$TOTAL_BYTES" -le "$MEM_THRESHOLD_BYTES" ]; then
        REL_OP="<="
    else
        REL_OP=">"
    fi
fi

echo "[setup] Backing: $EFFECTIVE_BACKING (total $(human $TOTAL_BYTES) $REL_OP 75% of $(human $MEM_BYTES) MemTotal = $(human $MEM_THRESHOLD_BYTES))"

check_null_blk() {
    # Check if module is available
    if ! modinfo null_blk >/dev/null 2>&1; then
        cat <<EOF >&2
ERROR: null_blk kernel module is not available on this system.

This script requires null_blk to emulate block devices. To install:
  Ubuntu/Debian:  sudo apt install linux-modules-extra-\$(uname -r)
  RHEL/CentOS:    ensure kernel-modules-extra is installed
  Custom kernel:  rebuild with CONFIG_BLK_DEV_NULL_BLK=m

Then verify:
  sudo modprobe null_blk && lsmod | grep null_blk

This script does NOT fall back to alternative emulation backends.
Re-run after null_blk is installed and loadable.
EOF
        exit 1
    fi

    if ! grep -q "^null_blk" /proc/modules; then
        if ! sudo modprobe null_blk >/dev/null 2>&1; then
            cat <<EOF >&2
ERROR: null_blk kernel module is not available on this system.

This script requires null_blk to emulate block devices. To install:
  Ubuntu/Debian:  sudo apt install linux-modules-extra-\$(uname -r)
  RHEL/CentOS:    ensure kernel-modules-extra is installed
  Custom kernel:  rebuild with CONFIG_BLK_DEV_NULL_BLK=m

Then verify:
  sudo modprobe null_blk && lsmod | grep null_blk

This script does NOT fall back to alternative emulation backends.
Re-run after null_blk is installed and loadable.
EOF
            exit 1
        fi
    fi
}

check_losetup() {
    if ! command -v losetup >/dev/null 2>&1; then
        cat <<EOF >&2
ERROR: losetup not found (util-linux). File-backed mode requires losetup.
       Install util-linux and re-run.
EOF
        exit 1
    fi
    mkdir -p "$BASE_DIR"
    AVAIL_BYTES=$(df --output=avail -B1 "$BASE_DIR" | tail -n 1 | tr -d ' ')
    NEED_BYTES=$(( TOTAL_BYTES * 105 / 100 ))
    if [ "$AVAIL_BYTES" -lt "$NEED_BYTES" ]; then
        cat <<EOF >&2
ERROR: insufficient free space at $BASE_DIR for file-backed mode.
       required (with 5% headroom): $(human "$NEED_BYTES")
       available:                   $(human "$AVAIL_BYTES")
       Suggest: re-run with <BASE_DIR> on a larger filesystem.
EOF
        exit 1
    fi
    echo "[setup] Sparse file dir: $BASE_DIR (free: $(human "$AVAIL_BYTES"))"
}

if [ "$EFFECTIVE_BACKING" = "memory" ]; then
    check_null_blk
else
    check_losetup
fi

echo "Setting up $NUM_OSDS device(s) of ${SIZE_GB}GB ($EFFECTIVE_BACKING mode)..."

for ((i=0; i<NUM_OSDS; i++)); do
    OSD_DIR="$BASE_DIR/osd$i"
    mkdir -p "$OSD_DIR"

    if [ "$EFFECTIVE_BACKING" = "memory" ]; then
        DEV_NAME="nullb$i"
        DEV_PATH="/dev/$DEV_NAME"
        CONFIG_DIR="/sys/kernel/config/nullb/$DEV_NAME"

        # Clean up existing instance if any
        if [ -d "$CONFIG_DIR" ]; then
            echo 0 | sudo tee "$CONFIG_DIR/power" >/dev/null
            sudo rmdir "$CONFIG_DIR"
        fi

        sudo mkdir -p "$CONFIG_DIR"
        echo "$((SIZE_GB * 1024))" | sudo tee "$CONFIG_DIR/size" >/dev/null
        echo 4096 | sudo tee "$CONFIG_DIR/blocksize" >/dev/null
        echo 0 | sudo tee "$CONFIG_DIR/queue_mode" >/dev/null
        echo 0 | sudo tee "$CONFIG_DIR/irqmode" >/dev/null
        echo 1 | sudo tee "$CONFIG_DIR/memory_backed" >/dev/null
        echo 1 | sudo tee "$CONFIG_DIR/power" >/dev/null

        udevadm settle || sleep 1

        if [ ! -b "$DEV_PATH" ]; then
            echo "Error: Device $DEV_PATH was not created." >&2
            exit 1
        fi

        sudo chmod 666 "$DEV_PATH"
        dd if=/dev/zero of="$DEV_PATH" bs=1M count=100 conv=fsync
        echo "$DEV_PATH" > "$OSD_DIR/device_path"
        echo "memory" > "$OSD_DIR/backing_mode"
        echo "Initialized $DEV_PATH for OSD $i in $OSD_DIR"
    else
        # File-backed: sparse image + loop device with direct I/O.
        # Force a 4 KiB logical sector size — seastore aborts on mount
        # with "block_size >= laddr_t::UNIT_SIZE" (i.e. >= 4096) and
        # losetup defaults to 512.
        IMG="$OSD_DIR/backing.img"
        truncate -s "${SIZE_GB}G" "$IMG"
        DEV_PATH=$(sudo losetup --find --show --direct-io=on --sector-size 4096 "$IMG")
        if [ ! -b "$DEV_PATH" ]; then
            echo "Error: losetup did not return a usable device for $IMG" >&2
            exit 1
        fi
        sudo chmod 666 "$DEV_PATH"
        echo "$DEV_PATH" > "$OSD_DIR/device_path"
        echo "file" > "$OSD_DIR/backing_mode"
        echo "Initialized $DEV_PATH (sparse $IMG) for OSD $i in $OSD_DIR"
    fi
done

echo "Setup complete."
