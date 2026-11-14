#!/bin/bash
# Shared helpers for radosgw-admin exit-code tests.
#
# Sourced by test-*-exit-codes.sh under this directory. See also PR #71155
# (test-bucket-exit-codes.sh, test-script-exit-codes.sh, test-globals.sh).

RGW_ADMIN="${RGW_ADMIN:-${CEPH_BIN:-./bin}/radosgw-admin}"
export CEPH_CONF="${CEPH_CONF:-${CEPH_BUILD_DIR:-.}/ceph.conf}"
# Keep ceph log lines off stderr so they cannot interleave with the output printed
# when a test fails. rgw_global_init consumes --log-to-stderr before the flags
# are read.
export CEPH_ARGS="--log-to-stderr=false${CEPH_ARGS:+ ${CEPH_ARGS}}"
PASS=0
FAIL=0
SKIP=0

# Filter out noisy ceph log lines (timestamped) and config-not-found lines
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
# Runs against a real cluster. Skips if no cluster is running.
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

report_results() {
  echo ""
  echo "========================================"
  echo "Results: $PASS passed, $FAIL failed, $SKIP skipped"
  [ "$SKIP" -gt 0 ] && echo "(some tests require a running cluster)"
  echo "========================================"
  [ "$FAIL" -eq 0 ] && exit 0 || exit 1
}
