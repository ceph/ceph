#!/usr/bin/env bash
# -*- mode:shell-script; tab-width:8; sh-basic-offset:2; indent-tabs-mode:nil -*-
#
# Test RGW tcmalloc heap profiler via admin socket (ceph daemon).
# Run after vstart.sh or in teuthology with at least one RGW running.

set -e

TEST_TMPDIR=$(mktemp -d)
PROFILER_RUNNING=0

cleanup() {
  # If we're exiting (e.g. via set -e or fail()) while the profiler is
  # still on, stop it so we don't leave profiling enabled in this shared
  # RGW daemon after the test exits.
  if [ "$PROFILER_RUNNING" = "1" ]; then
    rgw_admin heap stop_profiler >/dev/null 2>&1 || true
  fi
  rm -rf "$TEST_TMPDIR"
}
trap cleanup EXIT

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
  # In teuthology, qa/tasks/rgw.py starts radosgw as "-n client.$CEPH_ID",
  # and the workunit runner exports CEPH_ID for the client role running this
  # script; qa/tasks/admin_socket.py resolves that daemon's socket to
  # /var/run/ceph/ceph-client.$CEPH_ID.asok, which won't match the
  # "client.rgw.*" glob below (that pattern is for vstart.sh runs).
  if [ -n "$CEPH_ID" ] && [ -S "/var/run/ceph/ceph-client.${CEPH_ID}.asok" ]; then
    echo "/var/run/ceph/ceph-client.${CEPH_ID}.asok"
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

# radosgw runs under sudo in teuthology (see qa/tasks/rgw.py), so its admin
# socket there is root-owned; qa/tasks/admin_socket.py accordingly invokes
# "ceph --admin-daemon" via sudo. Detect that case and route our commands
# through sudo too, while keeping direct access for user-owned sockets
# (e.g. a local vstart.sh run) so this also works without sudo configured.
if [ -r "$RGW_SOCKET" ] && [ -w "$RGW_SOCKET" ]; then
  NEED_SUDO=""
else
  NEED_SUDO="sudo"
fi

rgw_admin() {
  $NEED_SUDO ceph daemon "$RGW_SOCKET" "$@"
}

# --- Test 1: heap stats (does not require profiler to be running) ---
echo "=== Test: heap stats ==="
set +e
rgw_admin heap stats > "$TEST_TMPDIR/stats.out" 2>&1
rc=$?
set -e

if grep -q "not using tcmalloc" "$TEST_TMPDIR/stats.out"; then
  echo "tcmalloc not in use; skipping remaining tests."
  exit 0
fi

if [ $rc -ne 0 ]; then
  echo "heap stats failed with rc=$rc:"
  cat "$TEST_TMPDIR/stats.out"
  exit $rc
fi

grep -q "tcmalloc heap stats" "$TEST_TMPDIR/stats.out" ||
  grep -q "MALLOC:" "$TEST_TMPDIR/stats.out" ||
  fail "heap stats output missing expected content"
echo "heap stats OK"

# --- Test 2: start_profiler ---
echo "=== Test: heap start_profiler ==="
rgw_admin heap start_profiler > "$TEST_TMPDIR/start.out" 2>&1
grep -q "started profiler" "$TEST_TMPDIR/start.out" ||
  fail "start_profiler did not report success"
PROFILER_RUNNING=1
echo "start_profiler OK"

# --- Test 3: heap dump (requires profiler running) ---
echo "=== Test: heap dump ==="
rgw_admin heap dump > "$TEST_TMPDIR/dump.out" 2>&1
grep -q "dumping heap profile" "$TEST_TMPDIR/dump.out" ||
  fail "heap dump did not report expected output"
echo "heap dump OK"

# --- Test 4: stop_profiler ---
echo "=== Test: heap stop_profiler ==="
rgw_admin heap stop_profiler > "$TEST_TMPDIR/stop.out" 2>&1
grep -q "stopped profiler" "$TEST_TMPDIR/stop.out" ||
  fail "stop_profiler did not report success"
PROFILER_RUNNING=0
echo "stop_profiler OK"

# --- Test 5: heap release ---
echo "=== Test: heap release ==="
rgw_admin heap release > "$TEST_TMPDIR/release.out" 2>&1
grep -q "releasing free RAM" "$TEST_TMPDIR/release.out" ||
  fail "heap release did not report expected output"
echo "heap release OK"

# --- Test 6: get_release_rate ---
echo "=== Test: heap get_release_rate ==="
rgw_admin heap get_release_rate > "$TEST_TMPDIR/getrate.out" 2>&1
grep -q "release rate" "$TEST_TMPDIR/getrate.out" ||
  fail "get_release_rate did not report expected output"
echo "get_release_rate OK"

# --- Test 7: set_release_rate (round-trips the rate read above back to
# itself, so the value-forwarding/argument-parsing path is exercised
# without actually changing daemon state) ---
echo "=== Test: heap set_release_rate ==="
current_rate=$(sed -n 's/.*release rate: *\([^[:space:]]*\).*/\1/p' "$TEST_TMPDIR/getrate.out")
if [ -z "$current_rate" ]; then
  fail "could not parse current release rate from get_release_rate output"
fi
rgw_admin heap set_release_rate "$current_rate" > "$TEST_TMPDIR/setrate.out" 2>&1
grep -q "release rate changed" "$TEST_TMPDIR/setrate.out" ||
  fail "set_release_rate did not report expected output"
echo "set_release_rate OK"

echo ""
echo "All RGW heap profiler tests passed."
exit 0
