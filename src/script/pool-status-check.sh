#!/bin/bash

LOG_DIR="/var/log/ceph_pool_status"
PID_FILE="/var/run/ceph_pool_status.pid"

mkdir -p "$LOG_DIR"

# Handle stop flag
if [[ "$1" == "--stop" ]]; then
    if [[ -f "$PID_FILE" ]]; then
        PID=$(cat "$PID_FILE")
        echo "Stopping ceph_pool_status process with PID $PID..."
        kill "$PID" 2>/dev/null

        # Force-kill if still running
        sleep 1
        if kill -0 "$PID" 2>/dev/null; then
            echo "Force killing..."
            kill -9 "$PID"
        fi

        rm -f "$PID_FILE"
        echo "Stopped."
    else
        echo "No PID file found. Script is not running."
    fi
    exit 0
fi

# Prevent multiple instances
if [[ -f "$PID_FILE" ]]; then
    echo "Script already running (PID $(cat "$PID_FILE")). Exiting."
    exit 1
fi

# Save current PID
echo $$ > "$PID_FILE"

# Main loop
while true
do
    LOG_FILE="$LOG_DIR/pool_status_$(date +%Y-%m-%d).log"

    {
        echo "===== $(date '+%Y-%m-%d %H:%M:%S') ====="
        ceph -s --format json
        ceph osd pool availability-status --format json-pretty
        echo ""
    } >> "$LOG_FILE" 2>&1

    sleep 600
done
