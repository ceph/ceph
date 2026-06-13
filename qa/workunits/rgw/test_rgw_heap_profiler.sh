#!/usr/bin/env bash
# -*- mode:shell-script; tab-width:8; sh-basic-offset:2; indent-tabs-mode:nil -*-
#
# Test RGW tcmalloc heap profiler via admin socket (ceph daemon).
# Run after vstart.sh or in teuthology with at least one RGW running.

set -e

TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

if ! command -v ceph >/dev/null 2>&1; then
  echo "ceph not in PATH; skipping test."
  exit 0
fi

get_rgw_socket() {
  if [ -n "$RGW_ADMIN_SOCKET" ]; then
    echo "$RGW_ADMIN_SOCKET"
    return
  fi
  local socket
  for dir in ./run /var/run/ceph; do
    socket=$(ls "${dir}"/ceph-client.rgw.*.asok 2>/dev/null | head -1)
    if [ -n "$socket" ]; then
      echo "$socket"
      return
    fi
  done
  echo ""
}

RGW_SOCKET=$(get_rgw_socket)
if [ -z "$RGW_SOCKET" ]; then
  echo "No RGW admin socket found; is RGW running? Set RGW_ADMIN_SOCKET if needed."
  exit 1
fi
echo "Using RGW admin socket: $RGW_SOCKET"

# --- Test 1: heap stats (does not require profiler to be running) ---
echo "=== Test: heap stats ==="
set +e
ceph daemon "$RGW_SOCKET" heap stats > "$TMPDIR/stats.out" 2>&1
rc=$?
set -e

if [ $rc -ne 0 ]; then
  if grep -q "not using tcmalloc" "$TMPDIR/stats.out"; then
    echo "tcmalloc not in use; skipping remaining tests."
    exit 0
  fi
  echo "heap stats failed with rc=$rc:"
  cat "$TMPDIR/stats.out"
  exit $rc
fi

grep -q "tcmalloc heap stats" "$TMPDIR/stats.out" ||
  grep -q "MALLOC:" "$TMPDIR/stats.out" ||
  fail "heap stats output missing expected content"
echo "heap stats OK"

# --- Test 2: start_profiler ---
echo "=== Test: heap start_profiler ==="
ceph daemon "$RGW_SOCKET" heap start_profiler > "$TMPDIR/start.out" 2>&1
grep -q "started profiler" "$TMPDIR/start.out" ||
  fail "start_profiler did not report success"
echo "start_profiler OK"

# --- Test 3: heap dump (requires profiler running) ---
echo "=== Test: heap dump ==="
ceph daemon "$RGW_SOCKET" heap dump > "$TMPDIR/dump.out" 2>&1
grep -q "dumping heap profile" "$TMPDIR/dump.out" ||
  fail "heap dump did not report expected output"
echo "heap dump OK"

# --- Test 4: stop_profiler ---
echo "=== Test: heap stop_profiler ==="
ceph daemon "$RGW_SOCKET" heap stop_profiler > "$TMPDIR/stop.out" 2>&1
grep -q "stopped profiler" "$TMPDIR/stop.out" ||
  fail "stop_profiler did not report success"
echo "stop_profiler OK"

# --- Test 5: heap release ---
echo "=== Test: heap release ==="
ceph daemon "$RGW_SOCKET" heap release > "$TMPDIR/release.out" 2>&1
grep -q "releasing free RAM" "$TMPDIR/release.out" ||
  fail "heap release did not report expected output"
echo "heap release OK"

# --- Test 6: get_release_rate ---
echo "=== Test: heap get_release_rate ==="
ceph daemon "$RGW_SOCKET" heap get_release_rate > "$TMPDIR/getrate.out" 2>&1
grep -q "release rate" "$TMPDIR/getrate.out" ||
  fail "get_release_rate did not report expected output"
echo "get_release_rate OK"

echo ""
echo "All RGW heap profiler tests passed."
exit 0
