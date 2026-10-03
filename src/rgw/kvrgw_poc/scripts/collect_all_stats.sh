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
#!/bin/bash
set -euo pipefail

OUTDIR="${1:?Usage: $0 <output-dir>}"
INTERVAL="${2:-2}"
FDB_CLI="/home/gbenhano/kv_poc/third_party/fdb/usr/bin/fdbcli"
CLUSTER="--cluster-file /home/gbenhano/kv_poc/.fdb/fdb.cluster"

FDB_CONTAINERS=$(docker ps --format "{{.Names}}" | grep fdb | sort)
KVRGW_BACKEND_PIDS=$(ps aux | grep kv-rgw-backend | grep -v grep | awk '{print $2}' | sort || true)
KVRGW_FRONTEND_PIDS=$(ps aux | grep kv-rgw-frontend | grep -v grep | awk '{print $2}' | sort)

# --- CSV headers ---

# 1) FDB cluster stats
echo "timestamp,commits_hz,conflicts_hz,reads_hz,writes_hz,bytes_read_hz,bytes_written_hz,storage_bytes_used,started_hz,log_queue_bytes" \
  > "$OUTDIR/fdb_cluster.csv"

# 2) FDB per-process stats (storage + log disk latency, queue, roles)
echo "timestamp,address,role,cpu_usage,memory_used_bytes,memory_limit_bytes,disk_busy,disk_reads_hz,disk_writes_hz,queue_disk_bytes,input_bytes_hz,durable_bytes_hz" \
  > "$OUTDIR/fdb_processes.csv"

# 3) Docker container stats (CPU%, MEM usage, MEM limit, Net I/O, Block I/O)
echo "timestamp,container,cpu_pct,mem_usage_bytes,mem_limit_bytes,net_in_bytes,net_out_bytes,block_in_bytes,block_out_bytes" \
  > "$OUTDIR/docker_stats.csv"

# 4) Host disk I/O (iostat for fdb volumes)
echo "timestamp,device,r_per_sec,w_per_sec,rkB_per_sec,wkB_per_sec,await_ms,util_pct" \
  > "$OUTDIR/host_iostat.csv"

# 5) Host memory
echo "timestamp,total_kb,used_kb,free_kb,available_kb,cached_kb,buffers_kb" \
  > "$OUTDIR/host_memory.csv"

# 6) KVRGW process stats (CPU%, RSS)
echo "timestamp,pid,process,cpu_pct,rss_kb,vsz_kb" \
  > "$OUTDIR/kvrgw_processes.csv"

parse_bytes() {
  local val="$1"
  val="${val//,/}"
  case "$val" in
    *GiB) echo "${val%GiB} * 1073741824" | bc | cut -d. -f1 ;;
    *MiB) echo "${val%MiB} * 1048576" | bc | cut -d. -f1 ;;
    *KiB) echo "${val%KiB} * 1024" | bc | cut -d. -f1 ;;
    *kB)  echo "${val%kB} * 1000" | bc | cut -d. -f1 ;;
    *B)   echo "${val%B}" ;;
    *)    echo "$val" ;;
  esac
}

while true; do
  TS=$(date +%s)

  # 1) FDB cluster + per-process stats
  STATUS=$($FDB_CLI $CLUSTER --exec "status json" 2>/dev/null || echo "")
  if [ -n "$STATUS" ]; then
    echo "$STATUS" | python3 -c "
import sys,json
try:
  d=json.load(sys.stdin)
  c=d['cluster']['workload']
  s=d['cluster']['data']
  lq=0
  for p in d['cluster'].get('processes',{}).values():
    for r in p.get('roles',[]):
      if r.get('role')=='log':
        lq+=r.get('queue_disk_used_bytes',0)
  print(f'${TS},{c[\"transactions\"][\"committed\"][\"hz\"]:.1f},{c[\"transactions\"][\"conflicted\"][\"hz\"]:.1f},{c[\"operations\"][\"reads\"][\"hz\"]:.1f},{c[\"operations\"][\"writes\"][\"hz\"]:.1f},{c[\"bytes\"][\"read\"][\"hz\"]:.0f},{c[\"bytes\"][\"written\"][\"hz\"]:.0f},{s[\"total_disk_used_bytes\"]},{c[\"transactions\"][\"started\"][\"hz\"]:.1f},{lq}')
except: pass
" >> "$OUTDIR/fdb_cluster.csv"

    echo "$STATUS" | python3 -c "
import sys,json
try:
  d=json.load(sys.stdin)
  for addr, p in d['cluster'].get('processes',{}).items():
    cpu=p.get('cpu',{}).get('usage_cores',0)
    mem=p.get('memory',{}).get('used_bytes',0)
    mlim=p.get('memory',{}).get('limit_bytes',0)
    for r in p.get('roles',[]):
      role=r.get('role','unknown')
      db=r.get('disk',{}).get('busy',0)
      dr=r.get('disk',{}).get('reads',{}).get('hz',0)
      dw=r.get('disk',{}).get('writes',{}).get('hz',0)
      qd=r.get('queue_disk_used_bytes',r.get('stored_bytes',0))
      ib=r.get('input_bytes',{}).get('hz',0)
      dub=r.get('durable_bytes',{}).get('hz',0)
      print(f'${TS},{addr},{role},{cpu:.3f},{mem},{mlim},{db:.4f},{dr:.1f},{dw:.1f},{qd},{ib:.0f},{dub:.0f}')
except: pass
" >> "$OUTDIR/fdb_processes.csv"
  fi

  # 2) Docker container stats
  docker stats --no-stream --format "{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.NetIO}},{{.BlockIO}}" 2>/dev/null | grep fdb | while IFS=, read -r name cpu mem net blk; do
    cpu="${cpu//%/}"
    mem_used=$(echo "$mem" | awk -F'/' '{print $1}' | xargs)
    mem_limit=$(echo "$mem" | awk -F'/' '{print $2}' | xargs)
    net_in=$(echo "$net" | awk -F'/' '{print $1}' | xargs)
    net_out=$(echo "$net" | awk -F'/' '{print $2}' | xargs)
    blk_in=$(echo "$blk" | awk -F'/' '{print $1}' | xargs)
    blk_out=$(echo "$blk" | awk -F'/' '{print $2}' | xargs)
    echo "$TS,$name,$cpu,$(parse_bytes "$mem_used"),$(parse_bytes "$mem_limit"),$(parse_bytes "$net_in"),$(parse_bytes "$net_out"),$(parse_bytes "$blk_in"),$(parse_bytes "$blk_out")"
  done >> "$OUTDIR/docker_stats.csv"

  # 3) Host iostat (NVMe devices for FDB)
  iostat -dx 1 1 2>/dev/null | awk -v ts="$TS" '/^nvme/ {print ts","$1","$2","$3","$4","$5","$10","$NF}' >> "$OUTDIR/host_iostat.csv"

  # 4) Host memory
  awk -v ts="$TS" '
    /^MemTotal:/{t=$2} /^MemFree:/{f=$2} /^MemAvailable:/{a=$2}
    /^Cached:/{c=$2} /^Buffers:/{b=$2}
    END{print ts","t","t-f","f","a","c","b}
  ' /proc/meminfo >> "$OUTDIR/host_memory.csv"

  # 5) KVRGW process stats
  for pid in $KVRGW_BACKEND_PIDS; do
    if [ -d "/proc/$pid" ]; then
      ps -p "$pid" -o pid=,pcpu=,rss=,vsz= 2>/dev/null | awk -v ts="$TS" '{print ts","$1",backend,"$2","$3","$4}' >> "$OUTDIR/kvrgw_processes.csv"
    fi
  done
  for pid in $KVRGW_FRONTEND_PIDS; do
    if [ -d "/proc/$pid" ]; then
      ps -p "$pid" -o pid=,pcpu=,rss=,vsz= 2>/dev/null | awk -v ts="$TS" '{print ts","$1",frontend,"$2","$3","$4}' >> "$OUTDIR/kvrgw_processes.csv"
    fi
  done

  sleep "$INTERVAL"
done
