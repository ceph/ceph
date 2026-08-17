#!/bin/bash
# Tests for cross-cutting global handling: ceph global flags are stripped by
# rgw_global_init before radosgw-admin parses its own flags.
#
# Usage:
#   ./test-globals.sh
#   RGW_ADMIN=/path/to/radosgw-admin ./test-globals.sh
#   CEPH_CONF=/path/to/ceph.conf ./test-globals.sh
#
# Run from the build directory:
#   cd /path/to/ceph/build && bash /path/to/test-globals.sh

RGW_ADMIN="${RGW_ADMIN:-./bin/radosgw-admin}"
export CEPH_CONF="${CEPH_CONF:-./ceph.conf}"
# Keep ceph log lines off stderr so they cannot interleave with the messages
# the tests grep. rgw_global_init consumes --log-to-stderr before the flag loop runs.
export CEPH_ARGS="--log-to-stderr=false${CEPH_ARGS:+ ${CEPH_ARGS}}"
PASS=0
FAIL=0
SKIP=0

# Filter out noisy ceph log lines and config-not-found lines
filter() {
  grep -v "^[0-9]\{4\}-" | \
  grep -v "^did not load config" | \
  grep -v "^unable to get monitor" | \
  grep -v "^failed to fetch mon config"
}

# --no-mon-config skips monitor connection so check() runs without a cluster.
_run() { "$RGW_ADMIN" --no-mon-config "$@"; }

cluster_running() { pgrep -x radosgw > /dev/null 2>&1; }

# check "desc" expected_exit "expected_msg_or_empty" command args
check() {
  local desc="$1" expected_exit="$2" expected_msg="$3"
  shift 3
  local tmpfile; tmpfile=$(mktemp)
  _run "$@" >"$tmpfile" 2>&1
  local exit_code=$?
  local output; output=$(filter <"$tmpfile")
  rm -f "$tmpfile"

  local ok=1
  if [ "$exit_code" != "$expected_exit" ]; then
    echo "FAIL [$desc]: expected exit $expected_exit, got $exit_code"
    echo "     output: $output"
    ok=0
  fi
  if [ -n "$expected_msg" ] && ! echo "$output" | grep -qF -- "$expected_msg"; then
    echo "FAIL [$desc]: expected message not found: $expected_msg"
    echo "     output: $output"
    ok=0
  fi
  [ "$ok" = "1" ] && { echo "PASS [$desc]"; PASS=$((PASS+1)); } || FAIL=$((FAIL+1))
}

# check_cluster "desc" expected_exit "expected_msg_or_empty" -- command args
check_cluster() {
  local desc="$1" expected_exit="$2" expected_msg="$3"
  shift 3
  shift  # skip --

  if ! cluster_running; then
    echo "SKIP [$desc]: no cluster running"
    SKIP=$((SKIP+1))
    return
  fi

  local tmpfile; tmpfile=$(mktemp)
  "$RGW_ADMIN" "$@" >"$tmpfile" 2>&1
  local exit_code=$?
  local output; output=$(filter <"$tmpfile")
  rm -f "$tmpfile"

  local ok=1
  if [ "$exit_code" != "$expected_exit" ]; then
    echo "FAIL [$desc]: expected exit $expected_exit, got $exit_code"
    echo "     output: $output"
    ok=0
  fi
  if [ -n "$expected_msg" ] && ! echo "$output" | grep -qF -- "$expected_msg"; then
    echo "FAIL [$desc]: expected message not found: $expected_msg"
    echo "     output: $output"
    ok=0
  fi
  [ "$ok" = "1" ] && { echo "PASS [$desc]"; PASS=$((PASS+1)); } || FAIL=$((FAIL+1))
}

# ============================================================
# ceph global flags are consumed by rgw_global_init before radosgw-admin walks
# its own flags, so they never reach that loop.
#
# ceph strips these on two paths. --cluster and --no-config-file are read first,
# because they decide which ceph.conf to read, or whether to read one at all.
# -d, --rgw-zone and --debug-rgw are read after that file is loaded.
# Both paths are covered, in space and = form, each with a no-value flag, and
# with the global placed both before and after the command.
#
# A consumed global lets the command run, so these rows need a cluster.
# object shard prints the shard number for the object, here "shard": 10.
# bucket list repeats the same flags on a second command and prints [].
# Each shows the command reached its handler.
# ============================================================
echo ""
echo "=== ceph globals stripped before the flag loop (with cluster) ==="
check_cluster "global --cluster (space) stripped"                 0 '"shard": 10' -- \
  bucket object shard --object foo --num-shards 11 --cluster ceph
check_cluster "global --cluster=ceph (= form) stripped"           0 '"shard": 10' -- \
  bucket object shard --object foo --num-shards 11 --cluster=ceph
check_cluster "global --no-config-file (no value) stripped"       0 '"shard": 10' -- \
  bucket object shard --object foo --num-shards 11 --no-config-file
check_cluster "global -d (no value) stripped"                     0 '"shard": 10' -- \
  bucket object shard --object foo --num-shards 11 -d
check_cluster "global --rgw-zone (space) stripped"                0 '"shard": 10' -- \
  bucket object shard --object foo --num-shards 11 --rgw-zone default
check_cluster "global --rgw-zone=default (= form) stripped"       0 '"shard": 10' -- \
  bucket object shard --object foo --num-shards 11 --rgw-zone=default
check_cluster "global --debug-rgw (space) stripped"               0 '"shard": 10' -- \
  bucket object shard --object foo --num-shards 11 --debug-rgw 5
check_cluster "global --debug-rgw=5 (= form) stripped"            0 '"shard": 10' -- \
  bucket object shard --object foo --num-shards 11 --debug-rgw=5
check_cluster "global --rgw-zone default before command stripped" 0 '"shard": 10' -- \
  --rgw-zone default bucket object shard --object foo --num-shards 11
check_cluster "global --rgw-zone default (space) on bucket list"        0 '[]' -- bucket list --rgw-zone default
check_cluster "global --rgw-zone=default (= form) on bucket list"       0 '[]' -- bucket list --rgw-zone=default
check_cluster "global --debug-rgw 5 (space) on bucket list"             0 '[]' -- bucket list --debug-rgw 5
check_cluster "global --rgw-zone default before command on bucket list" 0 '[]' -- --rgw-zone default bucket list

# ============================================================
# These rows pair a global with an unknown flag. The error names the unknown flag,
# not the global.
# ============================================================
echo ""
echo "=== ceph globals alongside an unknown flag (no cluster) ==="
check "global --cluster with an unknown flag"  22 'ERROR: invalid flag --rgw-banana' \
  bucket object shard --object foo --num-shards 11 --cluster ceph --rgw-banana x
check "global --rgw-zone with an unknown flag" 22 'ERROR: invalid flag --rgw-banana' \
  bucket object shard --object foo --num-shards 11 --rgw-zone default --rgw-banana x

# ============================================================
echo ""
echo "========================================"
echo "Results: $PASS passed, $FAIL failed, $SKIP skipped"
[ "$SKIP" -gt 0 ] && echo "(some tests require a running cluster)"
echo "========================================"
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
