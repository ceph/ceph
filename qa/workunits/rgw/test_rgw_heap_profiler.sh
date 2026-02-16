#!/usr/bin/env bash
# -*- mode:shell-script; tab-width:8; sh-basic-offset:2; indent-tabs-mode:nil -*-
# Test RGW tcmalloc heap profiler via admin socket (ceph daemon).
# Run after vstart.sh or in teuthology with at least one RGW.
# Requires: ceph in PATH, cluster with RGW running.

set -e

if ! command -v ceph >/dev/null 2>&1; then
  echo "ceph not in PATH; run this script from a vstart or teuthology environment."
  exit 0
fi

# Find the admin socket for the RGW daemon.
# In a vstart environment, sockets are typically in ./run/.
get_rgw_socket() {
  if [ -n "$RGW_ADMIN_SOCKET" ]; then
    echo "$RGW_ADMIN_SOCKET"
    return
  fi
  # look for an rgw admin socket in common locations
  local socket
  for dir in ./run /var/run/ceph; do
    socket=$(ls "${dir}"/ceph-client.rgw.*.asok 2>/dev/null | head -1)
    if [ -n "$socket" ]; then
      echo "$socket"
      return
    fi
  done
  # fallback: try to find via ceph daemon
  echo ""
}

RGW_SOCKET=$(get_rgw_socket)
if [ -z "$RGW_SOCKET" ]; then
  echo "No RGW admin socket found; is RGW running? Set RGW_ADMIN_SOCKET if needed."
  exit 1
fi
echo "Using RGW admin socket: $RGW_SOCKET"

# Test heap stats (does not require profiler running)
echo "--- heap stats ---"
set +e
ceph daemon "$RGW_SOCKET" heap stats 2>&1 | tee /tmp/rgw_heap_stats.$$
out=$?
set -e
if [ $out -ne 0 ]; then
  if grep -q "not using tcmalloc\|tcmalloc not enabled\|unable to get value" /tmp/rgw_heap_stats.$$; then
    echo "RGW heap profiler not available (tcmalloc not in use or command not supported); skip."
    rm -f /tmp/rgw_heap_stats.$$
    exit 0
  fi
  cat /tmp/rgw_heap_stats.$$
  rm -f /tmp/rgw_heap_stats.$$
  exit $out
fi
if ! grep -q "tcmalloc heap stats\|MALLOC:" /tmp/rgw_heap_stats.$$; then
  echo "Unexpected output from heap stats:"
  cat /tmp/rgw_heap_stats.$$
  rm -f /tmp/rgw_heap_stats.$$
  exit 1
fi
echo "heap stats OK"
rm -f /tmp/rgw_heap_stats.$$

# Optional: start profiler, dump, stop, release
echo "--- heap start_profiler ---"
ceph daemon "$RGW_SOCKET" heap start_profiler 2>&1 || true
echo "--- heap dump ---"
ceph daemon "$RGW_SOCKET" heap dump 2>&1 || true
echo "--- heap stop_profiler ---"
ceph daemon "$RGW_SOCKET" heap stop_profiler 2>&1 || true
echo "--- heap release ---"
ceph daemon "$RGW_SOCKET" heap release 2>&1 || true

echo "RGW heap profiler test passed."
exit 0
