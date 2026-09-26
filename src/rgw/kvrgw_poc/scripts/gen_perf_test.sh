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

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG="${1:?Usage: gen_perf_test.sh <Perf-Test-XXX.txt>}"
OUTPUT_NAME="$(basename "${CONFIG}" .txt).md"
OUTPUT="${ROOT}/${OUTPUT_NAME}"

if [[ ! -f "${CONFIG}" ]]; then
  echo "ERROR: ${CONFIG} not found" >&2; exit 1
fi

get_field() {
  local key="$1"
  local val
  val=$(grep -v '^\s*#' "${CONFIG}" | grep -m1 "^${key}:" | sed "s/^${key}:[[:space:]]*//" | sed 's/[[:space:]]*$//' || true)
  echo "$val"
}

require_field() {
  local key="$1"
  local val
  val=$(get_field "$key")
  if [[ -z "$val" ]]; then
    echo "ERROR: required field '${key}' missing in ${CONFIG}" >&2; exit 1
  fi
  echo "$val"
}

require_int() {
  local key="$1"
  local val
  val=$(require_field "$key")
  if ! [[ "$val" =~ ^[0-9]+$ ]]; then
    echo "ERROR: '${key}' must be a Non-negative Integer, got '${val}'" >&2; exit 1
  fi
  echo "$val"
}

require_int_positive() {
  local key="$1"
  local val
  val=$(require_int "$key")
  if [[ "$val" -eq 0 ]]; then
    echo "ERROR: '${key}' must be > 0" >&2; exit 1
  fi
  echo "$val"
}

instances=$(require_int_positive "instances")
mode=$(require_field "mode")
clean=$(require_field "clean")
batch_size=$(require_int_positive "batch_size")
batch_timeout_us=$(require_int_positive "batch_timeout_us")
batch_threads=$(require_int_positive "batch_threads")
buckets=$(require_int_positive "buckets")
burst=$(require_int_positive "burst")
tiers=$(require_field "tiers")
sim_disk_write_us=$(require_int "sim_disk_write_us")
sim_disk_read_us=$(require_int "sim_disk_read_us")
sample_interval=$(require_int_positive "sample_interval")
fdb_stats=$(require_field "fdb_stats")
kvrgw_stats=$(require_field "kvrgw_stats")
version_state=$(require_field "version_state")
workload=$(require_field "workload")

if [[ "$version_state" != "none" && "$version_state" != "versioned" && "$version_state" != "suspended" ]]; then
  echo "ERROR: 'version_state' must be 'none', 'versioned', or 'suspended', got '${version_state}'" >&2; exit 1
fi

if [[ "$workload" != "put" && "$workload" != "put-overwrite" && "$workload" != "get" ]]; then
  echo "ERROR: 'workload' must be 'put', 'put-overwrite', or 'get', got '${workload}'" >&2; exit 1
fi

producers=""
consumers=""
max_futures=""
concurrency=""
if [[ "$workload" == "get" ]]; then
  producers=$(require_int_positive "producers")
  consumers=$(require_int_positive "consumers")
  max_futures=$(require_int_positive "max_futures")
  if (( max_futures < 1 || max_futures > 1024 )); then
    echo "ERROR: 'max_futures' must be in 1..1024, got '${max_futures}'" >&2; exit 1
  fi
else
  concurrency=$(require_int_positive "concurrency")
fi

base_files=""
overwrite_count=""
duration=""
count=""
source=""
if [[ "$workload" == "put-overwrite" ]]; then
  base_files=$(require_int_positive "base_files")
  overwrite_count=$(require_int_positive "overwrite_count")
  if [[ "$base_files" -eq 0 ]]; then
    echo "ERROR: 'base_files' must be > 0" >&2; exit 1
  fi
  if (( base_files % concurrency != 0 )); then
    echo "ERROR: base_files (${base_files}) must be divisible by concurrency (${concurrency})" >&2
    exit 1
  fi
  if (( base_files % buckets != 0 )); then
    echo "ERROR: base_files (${base_files}) must be divisible by buckets (${buckets})" >&2
    exit 1
  fi
elif [[ "$workload" == "put" ]]; then
    duration=$(require_int_positive "duration")
elif [[ "$workload" == "get" ]]; then
  if [[ "$clean" != "no" ]]; then
    echo "ERROR: workload get requires clean: no" >&2; exit 1
  fi
  source=$(require_field "source")
  duration=$(get_field "duration")
  count=$(get_field "count")
  if [[ -n "$duration" && -n "$count" ]]; then
    echo "ERROR: get stop mode is mutually exclusive: set duration or count, not both" >&2
    exit 1
  fi
  if [[ -n "$duration" ]]; then
    if ! [[ "$duration" =~ ^[1-9][0-9]*$ ]]; then
      echo "ERROR: 'duration' must be > 0, got '${duration}'" >&2; exit 1
    fi
  fi
  if [[ -n "$count" ]]; then
    if ! [[ "$count" =~ ^[1-9][0-9]*$ ]]; then
      echo "ERROR: 'count' must be > 0, got '${count}'" >&2; exit 1
    fi
  fi
fi

prefix_len=""
suffix_len="0"
tag_count="0"
tag_name_base=""
tag_data_size=""
if [[ "$workload" == "put" || "$workload" == "put-overwrite" ]]; then
  prefix_len=$(require_int_positive "prefix_len")
  if (( prefix_len < 1 || prefix_len > 768 )); then
    echo "ERROR: 'prefix_len' must be in 1..768, got '${prefix_len}'" >&2; exit 1
  fi
  suffix_len=$(get_field "suffix_len")
  if [[ -z "$suffix_len" ]]; then
    suffix_len=0
  fi
  if ! [[ "$suffix_len" =~ ^[0-9]+$ ]]; then
    echo "ERROR: 'suffix_len' must be an integer, got '${suffix_len}'" >&2; exit 1
  fi
  if (( suffix_len < 0 || suffix_len > 768 )); then
    echo "ERROR: 'suffix_len' must be in 0..768, got '${suffix_len}'" >&2; exit 1
  fi
  if (( prefix_len + suffix_len > 768 )); then
    echo "ERROR: prefix_len + suffix_len must be <= 768, got ${prefix_len}+${suffix_len}" >&2; exit 1
  fi
  tag_count=$(get_field "tag_count")
  if [[ -z "$tag_count" ]]; then
    tag_count=0
  fi
  if ! [[ "$tag_count" =~ ^[0-9]+$ ]]; then
    echo "ERROR: 'tag_count' must be an integer, got '${tag_count}'" >&2; exit 1
  fi
  if (( tag_count < 0 || tag_count > 10 )); then
    echo "ERROR: 'tag_count' must be in 0..10, got '${tag_count}'" >&2; exit 1
  fi
  if (( tag_count > 0 )); then
    tag_name_base=$(require_field "tag_name_base")
    tag_data_size=$(require_int_positive "tag_data_size")
    if (( tag_data_size < 1 || tag_data_size > 256 )); then
      echo "ERROR: 'tag_data_size' must be in 1..256, got '${tag_data_size}'" >&2; exit 1
    fi
  fi
fi

# #7: Optional tier thresholds with defaults
max_inline=$(get_field "max_inline")
max_kv_store=$(get_field "max_kv_store")
[[ -n "$max_inline" ]] || max_inline=256
[[ -n "$max_kv_store" ]] || max_kv_store=4096
if ! [[ "$max_inline" =~ ^[0-9]+$ ]]; then
  echo "ERROR: 'max_inline' must be a positive integer, got '${max_inline}'" >&2; exit 1
fi
if ! [[ "$max_kv_store" =~ ^[0-9]+$ ]]; then
  echo "ERROR: 'max_kv_store' must be a positive integer, got '${max_kv_store}'" >&2; exit 1
fi

# #21: Validate tiers values are numeric
IFS=',' read -ra tier_vals <<< "$tiers"
for tv in "${tier_vals[@]}"; do
  if ! [[ "$tv" =~ ^[0-9]+$ ]]; then
    echo "ERROR: tiers value '${tv}' is not a positive integer (tiers=${tiers})" >&2; exit 1
  fi
done

if [[ "$mode" != "perf" && "$mode" != "normal" ]]; then
  echo "ERROR: 'mode' must be 'perf' or 'normal', got '${mode}'" >&2; exit 1
fi
if [[ "$clean" != "yes" && "$clean" != "no" ]]; then
  echo "ERROR: 'clean' must be 'yes' or 'no', got '${clean}'" >&2; exit 1
fi
if [[ "$fdb_stats" != "full" && "$fdb_stats" != "none" ]]; then
  echo "ERROR: 'fdb_stats' must be 'full' or 'none', got '${fdb_stats}'" >&2; exit 1
fi
if [[ "$kvrgw_stats" != "full" && "$kvrgw_stats" != "none" ]]; then
  echo "ERROR: 'kvrgw_stats' must be 'full' or 'none', got '${kvrgw_stats}'" >&2; exit 1
fi

{
cat <<EOF
# Perf Test Configuration

Generated from: $(basename "${CONFIG}")
Generated at: $(date -u +%Y-%m-%dT%H:%M:%SZ)

## Test Parameters

| Parameter | Value |
|-----------|-------|
| instances | ${instances} |
| mode | ${mode} |
| clean | ${clean} |
| workload | ${workload} |
EOF
if [[ "$workload" == "get" ]]; then
cat <<EOF
| producers | ${producers} |
| consumers | ${consumers} |
| max_futures | ${max_futures} |
EOF
else
cat <<EOF
| concurrency | ${concurrency} |
EOF
fi
cat <<EOF
| buckets | ${buckets} |
| burst | ${burst} |
| tiers | ${tiers} |
| duration | ${duration} |
| version_state | ${version_state} |
| max_inline | ${max_inline} |
| max_kv_store | ${max_kv_store} |
EOF

if [[ "$workload" == "put" || "$workload" == "put-overwrite" ]]; then
cat <<EOF
| prefix_len | ${prefix_len} |
| suffix_len | ${suffix_len} |
| tag_count | ${tag_count} |
EOF
  if (( tag_count > 0 )); then
cat <<EOF
| tag_name_base | ${tag_name_base} |
| tag_data_size | ${tag_data_size} |
EOF
  fi
fi

if [[ "$workload" == "put-overwrite" ]]; then
cat <<EOF
| base_files | ${base_files} |
| overwrite_count | ${overwrite_count} |
EOF
fi
if [[ "$workload" == "get" ]]; then
cat <<EOF
| source | ${source} |
EOF
  if [[ -n "$count" ]]; then
cat <<EOF
| count | ${count} |
EOF
  fi
fi

cat <<EOF

## Batch Configuration

| Parameter | Value |
|-----------|-------|
| batch_size | ${batch_size} |
| batch_timeout_us | ${batch_timeout_us} |
| batch_threads | ${batch_threads} |

## Simulated Disk Latency

| Parameter | Value |
|-----------|-------|
| sim_disk_write_us | ${sim_disk_write_us} |
| sim_disk_read_us | ${sim_disk_read_us} |

## Stats Collection

| Parameter | Value |
|-----------|-------|
| sample_interval | ${sample_interval}s |
| fdb_stats | ${fdb_stats} |
| kvrgw_stats | ${kvrgw_stats} |
EOF

if [[ "$fdb_stats" == "full" ]]; then
cat <<'EOF'

### FDB Stats (full)

- Transaction rates: txn/sec, reads/sec, writes/sec, conflict/sec
- Per-process CPU utilization (SS, log, proxy)
- Per-process memory usage (RSS, FDB internal)
- Disk latency (read/write per SS)
- Log queue size / durability lag
- Log growth (current size, delta KB/MB, delta %)
- Storage queue size
- Data distribution (bytes stored per SS, key-range balance)
EOF
fi

if [[ "$kvrgw_stats" == "full" ]]; then
cat <<'EOF'

### KVRGW Stats (full)

**Latency (per op type):**
PutObject, GetObject, HeadObject, DeleteObject, DeleteMulti, DeleteBucket,
CreateBucket, ListBuckets, ListObjects, ListObjVersions, DeleteObjVersion,
CopyObject, PutObjTagging, GetObjTagging, DelObjTagging, BucketExists,
PutBucketPolicy, GetBucketPolicy, DelBucketPolicy, PutBucketVer, GetBucketVer

Each with: count, avg_total_us, avg_fdb_us, fdb_pct, avg_get_us, avg_put_us,
avg_del_us, avg_commit_us, avg_scan_us, avg_disk_us

**Batch stats:**
batch_commits, entries_batched, conflict_pushbacks,
avg/min/max batch_size, avg queue_size, avg/min/max wait_us

**Transaction health:**
txn_retries, txn_hard_failures, txn_max_retries_exceeded

**Error stats:**
Per-error-code counters (all KvrgwErrorCode values, non-zero only)

**Host stats:**
Per-process CPU/IO (pidstat), disk utilization (iostat), process memory
EOF
fi

cat <<EOF

## Perf Driver Commands

Per instance:
\`\`\`
set-batch size=${batch_size} timeout=${batch_timeout_us} threads=${batch_threads}
EOF
if [[ "$workload" != "get" ]]; then
cat <<EOF
create-buckets buckets=${buckets} mode=${version_state}
EOF
fi
if [[ "$workload" == "put-overwrite" ]]; then
cat <<EOF
put-overwrite c=${concurrency} tiers=${tiers} base_files=${base_files} overwrite_count=${overwrite_count}
\`\`\`
EOF
elif [[ "$workload" == "get" ]]; then
  get_cmd="get-test producers=${producers} consumers=${consumers} max_futures=${max_futures}"
  if [[ -n "$duration" ]]; then
    get_cmd="${get_cmd} duration=${duration}"
  elif [[ -n "$count" ]]; then
    get_cmd="${get_cmd} count=${count}"
  fi
  if [[ "$version_state" == "versioned" ]]; then
    get_cmd="${get_cmd} --all-versions"
  fi
cat <<EOF
${get_cmd}
\`\`\`
EOF
else
cat <<EOF
put c=${concurrency} tiers=${tiers} duration=${duration} burst=${burst}
\`\`\`
EOF
fi
} > "${OUTPUT}"

echo "Generated: ${OUTPUT}"
