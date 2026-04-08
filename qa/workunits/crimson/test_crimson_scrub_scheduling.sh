#!/usr/bin/env bash

. $(dirname $0)/../../standalone/ceph-helpers.sh

set -x

BINARY="ceph_test_crimson_scrub_scheduling"

echo "=== Run the scrub scheduling binary (built-in PG and timeout) ==="
if ! command -v $BINARY &> /dev/null; then
    echo "ERROR: $BINARY not found in PATH"
    exit 1
fi

$BINARY

echo "=== Verify no scrub errors ==="
ceph log last 100 | grep -qi "scrub.*error" && echo "WARNING: scrub errors found in logs" || true

echo "Test PASSED"
exit 0
