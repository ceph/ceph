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

if [[ $# -lt 1 ]]; then
  echo "Usage: $(basename "$0") <raw_file.csv>"
  exit 1
fi

INPUT="$1"
if [[ ! -f "${INPUT}" ]]; then
  echo "ERROR: file not found: ${INPUT}" >&2
  exit 1
fi

echo "=== Test Report: $(basename "${INPUT}") ==="
echo ""

echo "--- Configuration ---"
grep "^#" "${INPUT}" | grep -v "^# columns:" | grep -v "^# final_" | sed 's/^# /  /'
echo ""

INSTANCES=$(grep "^# rgw_instances:" "${INPUT}" | sed 's/^# rgw_instances: *//')

echo "--- Final Results ---"
INST_COUNT=$(grep "^# rgw_instances:" "${INPUT}" | sed 's/^# rgw_instances: *//')
for i in $(seq 0 $((INST_COUNT-1))); do
  PERF_LINE=$(grep "^# final_instance_${i}:" "${INPUT}" | sed 's/^# final_instance_[0-9]*: *//' || true)
  BATCH_LINE=$(grep "^# final_batch_${i}:" "${INPUT}" | sed 's/^# final_batch_[0-9]*: *\(perf> \)\?//' || true)
  if [[ -z "${PERF_LINE}" ]]; then continue; fi
  echo "  Instance ${i}:"
  for kv in $(echo "${PERF_LINE}" | tr ' ' '\n' | grep '='); do
    k=$(echo "${kv}" | cut -d= -f1)
    v=$(echo "${kv}" | cut -d= -f2)
    printf "    %-28s %s\n" "${k}" "${v}"
  done
  if [[ -n "${BATCH_LINE}" ]]; then
    echo "  Batch stats:"
    for kv in $(echo "${BATCH_LINE}" | tr ' ' '\n' | grep '='); do
      k=$(echo "${kv}" | cut -d= -f1)
      v=$(echo "${kv}" | cut -d= -f2)
      printf "    %-28s %s\n" "${k}" "${v}"
    done
  fi
  echo ""
done
if ! grep -q "^# final_instance_" "${INPUT}" 2>/dev/null; then
  echo "  (no final results in file)"
  echo ""
fi

DATA_ROWS=$(grep -c '^[0-9]' "${INPUT}" 2>/dev/null || echo "0")
echo "--- Time Series (${DATA_ROWS} samples) ---"

if [[ "${DATA_ROWS}" -gt 0 ]]; then
  python3 -c "
import sys, csv

rows = []
with open('${INPUT}') as f:
    for line in f:
        if line.startswith('#'): continue
        parts = line.strip().split(',')
        if len(parts) < 11: continue
        rows.append([float(x) for x in parts])

if not rows:
    print('  (no data)')
    sys.exit(0)

n = len(rows)
instances = ${INSTANCES:-1}

def col_stats(idx):
    vals = [r[idx] for r in rows if idx < len(r)]
    if not vals: return 0,0,0
    return min(vals), sum(vals)/len(vals), max(vals)

print('  {:>25}  {:>8}  {:>8}  {:>8}'.format('Metric', 'Min', 'Avg', 'Max'))
print('  ' + '-'*57)

labels = [
    (1, 'FDB txn_committed/sec'),
    (2, 'FDB reads/sec'),
    (3, 'FDB writes/sec'),
    (4, 'FDB conflicts/sec'),
    (5, 'SS CPU avg'),
    (6, 'SS CPU max'),
    (7, 'Log CPU max'),
    (8, 'Proxy CPU max'),
    (9, 'Queue MB'),
    (10, 'Durability lag (s)'),
]

for idx, label in labels:
    lo, avg, hi = col_stats(idx)
    fmt = '{:.0f}' if idx <= 4 else '{:.3f}' if idx <= 8 else '{:.1f}'
    print('  {:>25}  {:>8}  {:>8}  {:>8}'.format(label, fmt.format(lo), fmt.format(avg), fmt.format(hi)))

for i in range(instances):
    base = 11 + i * 6
    if base >= len(rows[0]): break
    print()
    print('  Instance {}:'.format(i))
    inst_labels = [
        (base, 'entries/sec'),
        (base+1, 'commits/sec'),
        (base+2, 'interval_wait_us'),
        (base+3, 'interval_queue_size'),
        (base+4, 'interval_latency_us'),
        (base+5, 'errors'),
    ]
    for idx, label in inst_labels:
        lo, avg, hi = col_stats(idx)
        print('  {:>25}  {:>8.0f}  {:>8.0f}  {:>8.0f}'.format(label, lo, avg, hi))

# Aggregate IOPS across instances
total_entries = [sum(r[11 + i*6] for i in range(instances) if 11+i*6 < len(r)) for r in rows]
if total_entries:
    print()
    print('  {:>25}  {:>8.0f}  {:>8.0f}  {:>8.0f}'.format(
        'AGGREGATE entries/sec',
        min(total_entries), sum(total_entries)/len(total_entries), max(total_entries)))
" 2>/dev/null || echo "  (failed to parse data)"
fi
