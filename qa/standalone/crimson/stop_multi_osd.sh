#!/bin/bash
set -euo pipefail

usage() {
    echo "Usage: $0 <BASE_DIR>"
    exit 1
}

if [[ $# -ne 1 ]]; then
    usage
fi

BASE_DIR="$(realpath "$1")"
# Capture script directory at the start
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 1. Stop all crimson-osd instances using pid files
echo "Stopping all crimson-osd instances..."
for pid_file in "$BASE_DIR"/osd*/crimson-osd.pid; do
    if [[ -f "$pid_file" ]]; then
        pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            echo "Sending SIGTERM to process $pid..."
            kill -15 "$pid"
        fi
    fi
done

# Wait for them to exit
echo "Waiting for crimson-osd instances to exit..."
for pid_file in "$BASE_DIR"/osd*/crimson-osd.pid; do
    if [[ -f "$pid_file" ]]; then
        pid=$(cat "$pid_file")
        while kill -0 "$pid" 2>/dev/null; do
            sleep 1
        done
        rm -f "$pid_file"
    fi
done

# 2. Stop vstart cluster
echo "Stopping vstart MON/MGR cluster..."
CEPH_ROOT=$(realpath "$SCRIPT_DIR/../../..")
cd "$CEPH_ROOT/build"
../src/stop.sh || true

# 3. Teardown emulated block devices
echo "Tearing down emulated devices..."
"$SCRIPT_DIR/setup_osd_emul.sh" --teardown "$BASE_DIR"

echo "Teardown complete."