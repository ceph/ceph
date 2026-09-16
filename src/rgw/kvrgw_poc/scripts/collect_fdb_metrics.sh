#!/bin/bash
#
# Ceph - scalable distributed file system
#
# Author: Gabriel BenHanokh <gbenhano@redhat.com>
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1, as published by the Free Software
# Foundation.  See file COPYING.
#
#!/usr/bin/env bash
set -euo pipefail

OUTDIR="${1:?Usage: $0 <output_dir> [interval_sec]}"
INTERVAL="${2:-2}"
mkdir -p "$OUTDIR"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FDB_CLI="${ROOT}/third_party/fdb/usr/bin/fdbcli"
CLUSTER_FILE="${ROOT}/.fdb/fdb.cluster"

FDB_PIDS=$(pgrep -f 'fdbserver.*127.0.0.1:45' | tr '\n' ',' | sed 's/,$//')
if [[ -z "$FDB_PIDS" ]]; then
  echo "ERROR: no fdbserver processes found" >&2
  exit 1
fi
echo "Collecting metrics for fdbserver PIDs: $FDB_PIDS"
echo "Output: $OUTDIR  Interval: ${INTERVAL}s"

NVME_DEVS="nvme4n1,nvme8n1,nvme9n1,nvme1n1,nvme7n1"

pidstat -u -p "$FDB_PIDS" "$INTERVAL" > "$OUTDIR/pidstat_cpu.log" 2>&1 &
PID_CPU=$!

pidstat -d -p "$FDB_PIDS" "$INTERVAL" > "$OUTDIR/pidstat_io.log" 2>&1 &
PID_IO=$!

iostat -x -d "$INTERVAL" $NVME_DEVS > "$OUTDIR/iostat.log" 2>&1 &
PID_IOSTAT=$!

(
  while true; do
    for pid in $(echo "$FDB_PIDS" | tr ',' ' '); do
      port=$(cat /proc/$pid/cmdline 2>/dev/null | tr '\0' '\n' | grep -oE '45[0-9][0-9]' | head -1 || echo "?")
      rss=$(awk '/VmRSS/{print $2}' /proc/$pid/status 2>/dev/null || echo "0")
      echo "$(date +%s) pid=$pid port=$port rss_kb=$rss"
    done
    sleep "$INTERVAL"
  done
) > "$OUTDIR/memory.log" 2>&1 &
PID_MEM=$!

(
  while true; do
    echo "--- $(date +%s) ---"
    "$FDB_CLI" -C "$CLUSTER_FILE" --exec "status json" 2>/dev/null || echo '{"error":"fdbcli failed"}'
    sleep "$INTERVAL"
  done
) > "$OUTDIR/fdb_status.log" 2>&1 &
PID_FDB=$!

echo "$PID_CPU $PID_IO $PID_IOSTAT $PID_MEM $PID_FDB" > "$OUTDIR/collector.pids"
echo "Collectors started. To stop: kill \$(cat $OUTDIR/collector.pids)"
echo "Waiting... (Ctrl+C or kill this script to stop)"

trap "kill $PID_CPU $PID_IO $PID_IOSTAT $PID_MEM $PID_FDB 2>/dev/null; echo 'Collectors stopped.'" EXIT
wait
