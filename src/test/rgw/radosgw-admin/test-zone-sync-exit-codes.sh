#!/bin/bash
# Exit-code tests for radosgw-admin zone metadata/data sync commands.
#
# Usage:
#   ./test-zone-sync-exit-codes.sh
#   RGW_ADMIN=/path/to/radosgw-admin ./test-zone-sync-exit-codes.sh
#   CEPH_CONF=/path/to/ceph.conf ./test-zone-sync-exit-codes.sh
#
# Run from the build directory:
#   cd /path/to/ceph/build && bash ../src/test/rgw/radosgw-admin/test-zone-sync-exit-codes.sh

RGW_ADMIN="${RGW_ADMIN:-./bin/radosgw-admin}"
export CEPH_CONF="${CEPH_CONF:-./ceph.conf}"
export CEPH_ARGS="--log-to-stderr=false${CEPH_ARGS:+ ${CEPH_ARGS}}"
PASS=0
FAIL=0
SKIP=0

filter() {
  grep -v "^[0-9]\{4\}-" | \
  grep -v "^did not load config" | \
  grep -v "^unable to get monitor" | \
  grep -v "^failed to fetch mon config"
}

cluster_running() { pgrep -x radosgw > /dev/null 2>&1; }

check_cluster() {
  local desc="$1" expected_exit="$2"
  shift 2
  shift

  if ! cluster_running; then
    echo "SKIP [$desc]: no cluster running"
    SKIP=$((SKIP+1))
    return
  fi

  local tmpfile; tmpfile=$(mktemp)
  "$RGW_ADMIN" "$@" >"$tmpfile" 2>&1
  local exit_code=$?

  if [ "$exit_code" = "$expected_exit" ]; then
    echo "PASS [$desc]"
    PASS=$((PASS+1))
  else
    echo "FAIL [$desc]: expected exit $expected_exit, got $exit_code"
    echo "     output: $(filter <"$tmpfile")"
    FAIL=$((FAIL+1))
  fi
  rm -f "$tmpfile"
}

zone_get_field() {
  local field="$1"
  "$RGW_ADMIN" zone get --format json 2>/dev/null | python3 -c "
import json, sys
try:
    print(json.load(sys.stdin)['$field'])
except Exception:
    sys.exit(1)
" 2>/dev/null
}

echo "=== zone metadata/data sync (cluster) ==="

check_cluster "metadata sync status: --source-zone not supported" 22 -- \
  metadata sync status --source-zone z1
check_cluster "metadata sync init: --source-zone not supported" 22 -- \
  metadata sync init --source-zone z1
check_cluster "metadata sync run: --source-zone not supported" 22 -- \
  metadata sync run --source-zone z1
check_cluster "metadata sync status: --source-zone-id not supported" 22 -- \
  metadata sync status --source-zone-id 00000000-0000-0000-0000-000000000001

check_cluster "data sync status: missing --source-zone" 22 -- data sync status
check_cluster "data sync init: missing --source-zone" 22 -- data sync init
check_cluster "data sync run: missing --source-zone" 22 -- data sync run

if cluster_running; then
  _local_zone_name=$(zone_get_field name)
  _local_zone_id=$(zone_get_field id)
  if [ -n "$_local_zone_name" ] && [ -n "$_local_zone_id" ]; then
    check_cluster "data sync status: local zone by name" 22 -- \
      data sync status --source-zone "$_local_zone_name"
    check_cluster "data sync status: local zone by id" 22 -- \
      data sync status --source-zone-id "$_local_zone_id"
    check_cluster "data sync init: local zone by name" 22 -- \
      data sync init --source-zone "$_local_zone_name"
    check_cluster "data sync run: local zone by name" 22 -- \
      data sync run --source-zone "$_local_zone_name"
  else
    echo "SKIP [data sync: local zone tests]: could not read zone get"
    SKIP=$((SKIP+5))
  fi

  if "$RGW_ADMIN" sync status 2>/dev/null | grep -q "no sync (zone is master)"; then
    check_cluster "metadata sync status: rejected on meta master" 22 -- \
      metadata sync status
  else
    echo "SKIP [metadata sync status on meta master]: zone is not metadata master"
    SKIP=$((SKIP+1))
  fi
fi

echo ""
echo "========================================"
echo "Results: $PASS passed, $FAIL failed, $SKIP skipped"
echo "========================================"
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
