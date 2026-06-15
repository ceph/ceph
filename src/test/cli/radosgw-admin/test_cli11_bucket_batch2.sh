#!/bin/bash
# CLI11 migration tests, batch 2: bucket layout, bucket chown,
# bucket limit check, bucket logging info/list/flush
#
# Usage:
#   ./test_cli11_bucket_batch2.sh
#   RGW_ADMIN=/path/to/radosgw-admin ./test_cli11_bucket_batch2.sh
#   CEPH_CONF=/path/to/ceph.conf ./test_cli11_bucket_batch2.sh
#
# TESTS-FIRST: these tests were written BEFORE migrating the commands to
# CLI11. Every expectation below encodes the LEGACY (pre-CLI11) behavior,
# verified against the legacy code paths:
#   - stray args:     "ERROR: Unrecognized argument: 'x'" + expected list, exit 1
#                     (SimpleCmd::find_command)
#   - unknown flag:   "ERROR: invalid flag --x", exit 22 (legacy arg loop)
#   - missing value:  "Option --x requires an argument.", exit 1 (ceph_argparse)
#   - handler errors: per-command messages, exit 22 / 2 (after driver init)
#
# !!! KNOWN REGRESSION on the rgw-cli11-migration branch !!!
# The CLI11 'bucket' subcommand (require_subcommand) currently swallows all
# UNMIGRATED bucket subcommands:
#   bucket layout / chown / logging ...  -> "A subcommand is required", exit 106
#   bucket limit check                   -> mis-parses as 'bucket check' with a
#                                           stray 'limit': "ERROR: unexpected
#                                           argument: 'limit'", exit 22
# So most parse-level tests below FAIL on the branch today. They pass against
# a main build, and define the target behavior for the migration.
#
# Expectations marked "post-migration:" are the ones that will deliberately
# change when the command is migrated, following the convention established
# by the first 6 migrated commands (list/stats/link/unlink/check/rm):
#   - missing option value -> exit 114, "--x: 1 required TEXT missing"
#   - stray positional     -> exit 22,  "ERROR: unexpected argument: 'x'"
#   - unknown flag         -> unchanged (exit 22, "ERROR: invalid flag --x")
#
# Test types:
#   check()        - no cluster needed: verifies exit code + exact message
#   check_cluster()- cluster needed: verifies command output and exit code
#
# Run from the build directory:
#   cd /path/to/ceph/build && bash /path/to/test_cli11_bucket_batch2.sh

RGW_ADMIN="${RGW_ADMIN:-./bin/radosgw-admin}"
export CEPH_CONF="${CEPH_CONF:-./ceph.conf}"
PASS=0
FAIL=0
SKIP=0

# Error message constants (legacy)
ERR_UNKNOWN_CMD="ERROR: Unknown command"
ERR_BUCKET_NOT_SPECIFIED="ERROR: bucket not specified"          # layout, logging *
ERR_BUCKET_NAME_NOT_SPECIFIED="ERROR: bucket name not specified" # chown (different wording!)
# chown on a nonexistent bucket: RGWBucket::init fails, main prints
# "failure: <cpp_strerror>: <err_msg>"
ERR_CHOWN_NO_BUCKET="failure: (2) No such file or directory: failed to fetch bucket info for bucket="
# logging list/flush on a bucket without logging: error message but exit 0 (!)
ERR_NO_LOGGING="does not have logging enabled"

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
echo "=== bucket layout (parse level, no cluster) ==="
# ============================================================

# stray positional args
# post-migration: exit 22, "ERROR: unexpected argument: 'x'"
check "layout: stray after"   1 "Command not found: bucket layout strayarg" \
  bucket layout strayarg
check "layout: stray before"  1 "ERROR: Unrecognized argument: 'foo'" \
  foo bucket layout
check "layout: stray between" 1 "ERROR: Unrecognized argument: 'extra'" \
  bucket extra layout

# unknown flag (same message and exit code post-migration)
check "layout: unrecognized flag" 22 "ERROR: invalid flag --fakeflag" \
  bucket layout --fakeflag

# missing option value
# post-migration: exit 114, "--bucket: 1 required TEXT missing"
check "layout: --bucket missing value"    1 "Option --bucket requires an argument." \
  bucket layout --bucket
check "layout: --bucket-id missing value" 1 "Option --bucket-id requires an argument." \
  bucket layout --bucket-id
check "layout: --tenant missing value"    1 "Option --tenant requires an argument." \
  bucket layout --tenant
check "layout: --format missing value"    1 "Option --format requires an argument." \
  bucket layout --format

# ============================================================
echo ""
echo "=== bucket chown (parse level, no cluster) ==="
# ============================================================

check "chown: stray after"   1 "Command not found: bucket chown strayarg" \
  bucket chown strayarg
check "chown: stray between" 1 "ERROR: Unrecognized argument: 'extra'" \
  bucket extra chown

check "chown: unrecognized flag" 22 "ERROR: invalid flag --fakeflag" \
  bucket chown --fakeflag

check "chown: --bucket missing value" 1 "Option --bucket requires an argument." \
  bucket chown --bucket
check "chown: --uid missing value"    1 "Option --uid requires an argument." \
  bucket chown --uid
check "chown: --marker missing value" 1 "Option --marker requires an argument." \
  bucket chown --marker

# ============================================================
echo ""
echo "=== bucket limit check (parse level, no cluster) ==="
# ============================================================

# 'bucket limit' is an internal node in the legacy command tree
check "limit (incomplete command)" 1 "$ERR_UNKNOWN_CMD" \
  bucket limit
check "limit check: stray after" 1 "Command not found: bucket limit check strayarg" \
  bucket limit check strayarg

check "limit check: unrecognized flag" 22 "ERROR: invalid flag --fakeflag" \
  bucket limit check --fakeflag

check "limit check: --uid missing value" 1 "Option --uid requires an argument." \
  bucket limit check --uid

# ============================================================
echo ""
echo "=== bucket logging (parse level, no cluster) ==="
# ============================================================

# 'bucket logging' is an internal node: Unknown command + expected list
check "logging (incomplete command)"    1 "$ERR_UNKNOWN_CMD" \
  bucket logging
check "logging: unknown subcommand"     1 "ERROR: Unrecognized argument: 'banana'" \
  bucket logging banana

check "logging info: stray after"  1 "Command not found: bucket logging info strayarg" \
  bucket logging info strayarg
check "logging list: stray after"  1 "Command not found: bucket logging list strayarg" \
  bucket logging list strayarg
check "logging flush: stray after" 1 "Command not found: bucket logging flush strayarg" \
  bucket logging flush strayarg

check "logging info: unrecognized flag"  22 "ERROR: invalid flag --fakeflag" \
  bucket logging info --fakeflag
check "logging list: unrecognized flag"  22 "ERROR: invalid flag --fakeflag" \
  bucket logging list --fakeflag
check "logging flush: unrecognized flag" 22 "ERROR: invalid flag --fakeflag" \
  bucket logging flush --fakeflag

check "logging info: --bucket missing value"  1 "Option --bucket requires an argument." \
  bucket logging info --bucket
check "logging list: --bucket missing value"  1 "Option --bucket requires an argument." \
  bucket logging list --bucket
check "logging flush: --bucket missing value" 1 "Option --bucket requires an argument." \
  bucket logging flush --bucket

# ============================================================
echo ""
echo "=== handler-level errors (cluster) ==="
# ============================================================
# These run after driver init, so they need a cluster. The validation and
# messages live in the command handlers and are unchanged by the migration.

# bucket layout
check_cluster "layout: missing --bucket" 22 "$ERR_BUCKET_NOT_SPECIFIED" -- \
  bucket layout
# init_bucket failure is SILENT for layout: no message, exit ENOENT
check_cluster "layout: nonexistent bucket (silent exit 2)" 2 "" -- \
  bucket layout --bucket cli11-no-such-bucket

# bucket chown — note the different message wording vs layout
check_cluster "chown: missing --bucket" 22 "$ERR_BUCKET_NAME_NOT_SPECIFIED" -- \
  bucket chown
check_cluster "chown: nonexistent bucket" 2 "$ERR_CHOWN_NO_BUCKET" -- \
  bucket chown --bucket cli11-no-such-bucket --uid cli11_no_such_user
# NOTE (do not test, mutating): legacy 'bucket chown --bucket <real>' without
# --uid performs the chown with an EMPTY owner — no up-front validation
# (rgw_chown_bucket_and_objects). Worth raising with mentors during migration.

# bucket limit check — iterates all users when no --uid given
check_cluster "limit check: no args (all users)" 0 "" -- \
  bucket limit check
check_cluster "limit check: --warnings-only" 0 "" -- \
  bucket limit check --warnings-only
check_cluster "limit check: nonexistent --uid (empty listing, exit 0)" 0 "" -- \
  bucket limit check --uid cli11_no_such_user

# bucket logging info/list/flush
check_cluster "logging info: missing --bucket"  22 "$ERR_BUCKET_NOT_SPECIFIED" -- \
  bucket logging info
check_cluster "logging list: missing --bucket"  22 "$ERR_BUCKET_NOT_SPECIFIED" -- \
  bucket logging list
check_cluster "logging flush: missing --bucket" 22 "$ERR_BUCKET_NOT_SPECIFIED" -- \
  bucket logging flush

# init_bucket failure is SILENT: no message, exit ENOENT
check_cluster "logging info: nonexistent bucket (silent exit 2)"  2 "" -- \
  bucket logging info --bucket cli11-no-such-bucket
check_cluster "logging list: nonexistent bucket (silent exit 2)"  2 "" -- \
  bucket logging list --bucket cli11-no-such-bucket
check_cluster "logging flush: nonexistent bucket (silent exit 2)" 2 "" -- \
  bucket logging flush --bucket cli11-no-such-bucket

# ============================================================
echo ""
echo "=== integration: real bucket (cluster) ==="
# ============================================================
# Creates a test user and bucket, exercises the read paths, then cleans up.
# Skipped automatically if no cluster is running.

_test_uid="cli11_batch2_user"
_test_bucket="cli11-batch2-bucket"
_test_display="CLI11 Batch2 Test User"
_n_integration=9

if cluster_running; then
  # Create a test user (legacy command, not yet CLI11-migrated)
  "$RGW_ADMIN" user create --uid "$_test_uid" --display-name "$_test_display" \
    >/dev/null 2>&1

  if command -v aws >/dev/null 2>&1; then
    # Get credentials for the test user
    _access_key=$("$RGW_ADMIN" user info --uid "$_test_uid" 2>/dev/null | \
      python3 -c "import sys,json; d=json.load(sys.stdin); print(d['keys'][0]['access_key'])" 2>/dev/null)
    _secret_key=$("$RGW_ADMIN" user info --uid "$_test_uid" 2>/dev/null | \
      python3 -c "import sys,json; d=json.load(sys.stdin); print(d['keys'][0]['secret_key'])" 2>/dev/null)
    _rgw_endpoint="http://localhost:8000"

    if [ -n "$_access_key" ] && [ -n "$_secret_key" ]; then
      # Create the test bucket
      AWS_ACCESS_KEY_ID="$_access_key" \
      AWS_SECRET_ACCESS_KEY="$_secret_key" \
      aws --endpoint-url "$_rgw_endpoint" \
        s3 mb "s3://$_test_bucket" >/dev/null 2>&1

      # bucket layout: dumps the bucket's index layout as JSON
      check_cluster "integration: bucket layout" 0 "current_index" -- \
        bucket layout --bucket "$_test_bucket"
      check_cluster "integration: bucket layout --format json" 0 "current_index" -- \
        bucket layout --bucket "$_test_bucket" --format json

      # bucket chown: chown to the (already-owning) test user. Progress goes
      # to stderr ("0 objects processed in ...") unless BucketOwnerEnforced
      # short-circuits the object loop, so only the exit code is asserted.
      check_cluster "integration: bucket chown" 0 "" -- \
        bucket chown --bucket "$_test_bucket" --uid "$_test_uid"

      # bucket limit check for the test user: JSON with user_id + buckets
      check_cluster "integration: limit check --uid" 0 "user_id" -- \
        bucket limit check --uid "$_test_uid"
      check_cluster "integration: limit check --uid --warnings-only" 0 "" -- \
        bucket limit check --uid "$_test_uid" --warnings-only

      # bucket logging on a bucket WITHOUT logging configured:
      # info is silent (exit 0, no output); list and flush print an error
      # message but still exit 0 (legacy quirk, kept verbatim)
      check_cluster "integration: logging info (no logging, silent)" 0 "" -- \
        bucket logging info --bucket "$_test_bucket"
      check_cluster "integration: logging list (no logging, msg + exit 0)" 0 "$ERR_NO_LOGGING" -- \
        bucket logging list --bucket "$_test_bucket"
      check_cluster "integration: logging flush (no logging, msg + exit 0)" 0 "$ERR_NO_LOGGING" -- \
        bucket logging flush --bucket "$_test_bucket"

      # tenant flag passthrough: empty tenant is the default, exit 0
      check_cluster "integration: bucket layout --tenant ''" 0 "current_index" -- \
        bucket layout --bucket "$_test_bucket" --tenant ""
    else
      echo "SKIP [integration]: could not get credentials for test user"
      SKIP=$((SKIP+_n_integration))
    fi
  else
    echo "SKIP [integration]: aws CLI not available (needed to create test bucket)"
    SKIP=$((SKIP+_n_integration))
  fi

  # Cleanup: remove the test user and its buckets
  "$RGW_ADMIN" user rm --uid "$_test_uid" --purge-data >/dev/null 2>&1
fi

# ============================================================
echo ""
echo "========================================"
echo "Results: $PASS passed, $FAIL failed, $SKIP skipped"
[ "$SKIP" -gt 0 ] && echo "(some tests require a running cluster or aws CLI)"
echo "========================================"
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
