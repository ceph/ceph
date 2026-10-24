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

if [[ $# -lt 1 ]]; then
  echo "Usage: $(basename "$0") <raw_file.csv> [description]"
  exit 1
fi

INPUT="$1"
DESC="${2:-replay of $(basename "$1")}"

if [[ ! -f "${INPUT}" ]]; then
  echo "ERROR: file not found: ${INPUT}" >&2
  exit 1
fi

get_header() {
  grep "^# ${1}:" "${INPUT}" | head -1 | sed "s/^# ${1}: *//"
}

INSTANCES=$(get_header "rgw_instances")
CONC=$(get_header "rgw_producer_threads")
BATCH_SIZE=$(get_header "rgw_batch_size")
BATCH_TIMEOUT=$(get_header "rgw_batch_timeout_us")
BATCH_THREADS=$(get_header "rgw_batch_threads")
OBJ_SIZE=$(get_header "object_size_bytes")
BUCKETS=$(get_header "buckets_per_instance")
BURST=$(get_header "rgw_burst_size")
DURATION=$(get_header "test_duration_sec")
SIM_WRITE=$(get_header "sim_disk_write_us")
SIM_READ=$(get_header "sim_disk_read_us")
SAMPLE=$(get_header "sample_interval_sec")
OUTDIR=$(dirname "${INPUT}")

echo "=== Replaying: $(basename "${INPUT}") ==="
echo "  Original: $(get_header "description")"
echo "  Original commit: $(get_header "git_commit")"
echo "  New description: ${DESC}"
echo "  Config: ${INSTANCES} instances, c=${CONC}, batch=${BATCH_SIZE}, ${OBJ_SIZE}B, ${DURATION}s"
echo ""

exec "${ROOT}/scripts/run_perf_test.sh" \
  --description "${DESC}" \
  --instances "${INSTANCES}" \
  --concurrency "${CONC}" \
  --batch-size "${BATCH_SIZE}" \
  --batch-timeout "${BATCH_TIMEOUT}" \
  --batch-threads "${BATCH_THREADS}" \
  --object-size "${OBJ_SIZE}" \
  --buckets "${BUCKETS}" \
  --burst-size "${BURST:-1}" \
  --duration "${DURATION}" \
  --sim-write-us "${SIM_WRITE}" \
  --sim-read-us "${SIM_READ}" \
  --sample-interval "${SAMPLE}" \
  --output-dir "${OUTDIR}"
