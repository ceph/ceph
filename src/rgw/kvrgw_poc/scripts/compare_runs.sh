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

if [[ $# -lt 2 ]]; then
  echo "Usage: $(basename "$0") <run1.csv> <run2.csv>"
  exit 1
fi

A="$1"
B="$2"

if [[ ! -f "${A}" || ! -f "${B}" ]]; then
  echo "ERROR: files not found" >&2
  exit 1
fi

get_h() { grep "^# ${2}:" "$1" | head -1 | sed "s/^# ${2}: *//" ; }

echo "=== Run Comparison ==="
echo ""
echo "  Run A: $(get_h "${A}" description) [$(get_h "${A}" git_commit)]"
echo "  Run B: $(get_h "${B}" description) [$(get_h "${B}" git_commit)]"
echo ""

echo "--- Config Differences ---"
diff <(grep "^#" "${A}" | grep -v "^# columns:" | grep -v "^# final_" | grep -v "^# test_start:" | grep -v "^# description:" | grep -v "^# git_commit:" | grep -v "^# based_on:" | sort) \
     <(grep "^#" "${B}" | grep -v "^# columns:" | grep -v "^# final_" | grep -v "^# test_start:" | grep -v "^# description:" | grep -v "^# git_commit:" | grep -v "^# based_on:" | sort) \
     && echo "  (identical)" || true
echo ""

INST_A=$(get_h "${A}" rgw_instances)
INST_B=$(get_h "${B}" rgw_instances)

echo "--- Performance Comparison ---"
python3 -c "
import sys

def load(path, instances):
    rows = []
    with open(path) as f:
        for line in f:
            if line.startswith('#'): continue
            parts = line.strip().split(',')
            if len(parts) < 11: continue
            rows.append([float(x) for x in parts])
    return rows

def avg_col(rows, idx):
    vals = [r[idx] for r in rows if idx < len(r)]
    return sum(vals)/len(vals) if vals else 0

ra = load('${A}', ${INST_A:-1})
rb = load('${B}', ${INST_B:-1})

if not ra or not rb:
    print('  (insufficient data)')
    sys.exit(0)

inst_a = ${INST_A:-1}
inst_b = ${INST_B:-1}

def agg_entries(rows, inst):
    return [sum(r[11+i*6] for i in range(inst) if 11+i*6 < len(r)) for r in rows]

ea = agg_entries(ra, inst_a)
eb = agg_entries(rb, inst_b)
iops_a = sum(ea)/len(ea) if ea else 0
iops_b = sum(eb)/len(eb) if eb else 0
delta_pct = ((iops_b - iops_a) / iops_a * 100) if iops_a > 0 else 0

metrics = [
    ('Aggregate IOPS', iops_a, iops_b),
    ('FDB txn/sec', avg_col(ra,1), avg_col(rb,1)),
    ('FDB reads/sec', avg_col(ra,2), avg_col(rb,2)),
    ('FDB writes/sec', avg_col(ra,3), avg_col(rb,3)),
    ('SS CPU avg', avg_col(ra,5), avg_col(rb,5)),
    ('SS CPU max', avg_col(ra,6), avg_col(rb,6)),
    ('Log CPU max', avg_col(ra,7), avg_col(rb,7)),
    ('Proxy CPU max', avg_col(ra,8), avg_col(rb,8)),
    ('Queue MB', avg_col(ra,9), avg_col(rb,9)),
]

print('  {:>25}  {:>10}  {:>10}  {:>8}'.format('Metric', 'Run A', 'Run B', 'Delta'))
print('  ' + '-'*57)
for label, va, vb in metrics:
    d = ((vb - va) / va * 100) if va > 0 else 0
    sign = '+' if d >= 0 else ''
    fmt = '{:.0f}' if va > 100 else '{:.3f}' if va < 1 else '{:.1f}'
    print('  {:>25}  {:>10}  {:>10}  {:>7}{}'.format(
        label, fmt.format(va), fmt.format(vb), sign, '{:.1f}%'.format(d)))
" 2>/dev/null || echo "  (failed to parse data)"

echo ""
echo "--- Final Results ---"
echo "  Run A:"
grep "^# final_" "${A}" | sed 's/^# /    /' || echo "    (none)"
echo "  Run B:"
grep "^# final_" "${B}" | sed 's/^# /    /' || echo "    (none)"
