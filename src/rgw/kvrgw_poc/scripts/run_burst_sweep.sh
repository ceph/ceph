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
OUTDIR="${ROOT}/perf-results/csv"

for BURST in 1 3 6 9; do
  echo ""
  echo "========================================"
  echo "  burst=${BURST} — reload + test"
  echo "========================================"

  "${ROOT}/scripts/run_perf_test.sh" \
    --description "burst sweep burst=${BURST}" \
    --instances 3 \
    --concurrency 128 \
    --batch-size 10 \
    --batch-timeout 1000 \
    --batch-threads 16 \
    --object-size 8192 \
    --buckets 1 \
    --burst-size "${BURST}" \
    --duration 60 \
    --output-dir "${OUTDIR}"
done

echo ""
echo "=== ALL BURST SWEEP TESTS COMPLETE ==="
echo ""
echo "=== Results ==="
for f in $(ls -t "${OUTDIR}"/raw_*.csv | head -4 | tac); do
  echo ""
  "${ROOT}/scripts/display_test.sh" "${f}"
done
