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
# Keep ceph log lines off stderr so they cannot interleave with the output printed
# when a test fails. rgw_global_init consumes --log-to-stderr before the flags
# are read.
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

# check "desc" expected_exit args
check() {
  local desc="$1" expected_exit="$2"
  shift 2
  local tmpfile; tmpfile=$(mktemp)
  _run "$@" >"$tmpfile" 2>&1
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

# check_cluster "desc" expected_exit -- args
check_cluster() {
  local desc="$1" expected_exit="$2"
  shift 2
  shift  # skip --

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

# ============================================================
# ceph global flags are consumed by rgw_global_init before radosgw-admin reads
# its own flags, so they never reach them.
#
# ceph strips these on two paths. --cluster and --no-config-file are read first,
# because they decide which ceph.conf to read, or whether to read one at all.
# -d, --rgw-zone and --debug-rgw are read after that file is loaded.
# Both paths are covered, in space and = form, each with a no-value flag, and
# with the global placed both before and after the command.
#
# A consumed global lets the command run, so these rows need a cluster.
# Exit 0 shows the command reached its handler.
# ============================================================
echo ""
echo "=== ceph globals stripped before radosgw-admin's own flags (with cluster) ==="
check_cluster "global --cluster (space) stripped"                 0 -- \
  bucket object shard --object foo --num-shards 11 --cluster ceph
check_cluster "global --cluster=ceph (= form) stripped"           0 -- \
  bucket object shard --object foo --num-shards 11 --cluster=ceph
check_cluster "global --no-config-file (no value) stripped"       0 -- \
  bucket object shard --object foo --num-shards 11 --no-config-file
check_cluster "global -d (no value) stripped"                     0 -- \
  bucket object shard --object foo --num-shards 11 -d
check_cluster "global --rgw-zone (space) stripped"                0 -- \
  bucket object shard --object foo --num-shards 11 --rgw-zone default
check_cluster "global --rgw-zone=default (= form) stripped"       0 -- \
  bucket object shard --object foo --num-shards 11 --rgw-zone=default
check_cluster "global --debug-rgw (space) stripped"               0 -- \
  bucket object shard --object foo --num-shards 11 --debug-rgw 5
check_cluster "global --debug-rgw=5 (= form) stripped"            0 -- \
  bucket object shard --object foo --num-shards 11 --debug-rgw=5
check_cluster "global --rgw-zone default before command stripped" 0 -- \
  --rgw-zone default bucket object shard --object foo --num-shards 11

# the same globals again on a second command, so the stripping is not specific
# to one command.
check_cluster "global --rgw-zone default (space) on bucket list"        0 -- bucket list --rgw-zone default
check_cluster "global --rgw-zone=default (= form) on bucket list"       0 -- bucket list --rgw-zone=default
check_cluster "global --debug-rgw 5 (space) on bucket list"             0 -- bucket list --debug-rgw 5
check_cluster "global --rgw-zone default before command on bucket list" 0 -- --rgw-zone default bucket list

# ============================================================
# These rows pair a global with an unknown flag. The unknown flag is what
# fails, so the exit code is the same as without the global.
# ============================================================
echo ""
echo "=== ceph globals alongside an unknown flag (no cluster) ==="
check "global --cluster with an unknown flag"  22 \
  bucket object shard --object foo --num-shards 11 --cluster ceph --rgw-banana x
check "global --rgw-zone with an unknown flag" 22 \
  bucket object shard --object foo --num-shards 11 --rgw-zone default --rgw-banana x

# ============================================================
echo ""
echo "========================================"
echo "Results: $PASS passed, $FAIL failed, $SKIP skipped"
[ "$SKIP" -gt 0 ] && echo "(some tests require a running cluster)"
echo "========================================"
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
