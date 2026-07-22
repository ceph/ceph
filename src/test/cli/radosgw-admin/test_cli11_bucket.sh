#!/bin/bash
# CLI11 migration tests for bucket commands
#
# Usage:
#   ./test_cli11_bucket.sh
#   RGW_ADMIN=/path/to/radosgw-admin ./test_cli11_bucket.sh
#   CEPH_CONF=/path/to/ceph.conf ./test_cli11_bucket.sh
#
# Test types:
#   check()        - no cluster needed: verifies exit code + exact message
#   check_warns()  - cluster needed: verifies warning messages appear before driver init
#                    usage: check_warns "desc" exit_code "error_msg_or_empty" "msg1" "msg2" ... -- command args
#   check_help()   - no cluster needed: verifies exit 0 for --cli11-help positions
#   check_cluster()- cluster needed: verifies command output and exit code
#
# Run from the build directory:
#   cd /path/to/ceph/build && bash /path/to/test_cli11_bucket.sh

RGW_ADMIN="${RGW_ADMIN:-./bin/radosgw-admin}"
export CEPH_CONF="${CEPH_CONF:-./ceph.conf}"
# Route dout/derr log lines off stderr so async cluster logs (e.g. "ERROR:
# obj.oid is empty") can't interleave mid-line with the cerr messages we grep.
# Consumed by rgw_global_init, never seen by CLI11 (app.parse reads argv).
export CEPH_ARGS="--log-to-stderr=false${CEPH_ARGS:+ ${CEPH_ARGS}}"
PASS=0
FAIL=0
SKIP=0

# Warning message constants
WARN_BUCKET_POS="Warning: --bucket/-b should appear after the subcommand"
WARN_BUCKET_DUP="Warning: --bucket/-b specified multiple times, using last value"
WARN_BUCKETID_POS="Warning: --bucket-id should appear after the subcommand"
WARN_BUCKETID_DUP="Warning: --bucket-id specified multiple times, using last value"
WARN_UID_DUP="Warning: --uid/-i specified multiple times, using last value"
WARN_TENANT_DUP="Warning: --tenant specified multiple times, using last value"
WARN_FIX_POS="Warning: --fix should appear after the subcommand"
WARN_FORMAT_POS="Warning: --format should appear after the subcommand"
WARN_FORMAT_DUP="Warning: --format specified multiple times, using last value"
WARN_MARKER_POS="Warning: --marker should appear after the subcommand"
WARN_MARKER_DUP="Warning: --marker specified multiple times, using last value"
WARN_MAXENT_POS="Warning: --max-entries should appear after the subcommand"
WARN_PURGE_POS="Warning: --purge-objects should appear after the subcommand"
WARN_PURGE_DUP="Warning: --purge-objects specified multiple times, using last value"
WARN_BYPASS_POS="Warning: --bypass-gc should appear after the subcommand"
WARN_INCONSISTENT_POS="Warning: --inconsistent-index should appear after the subcommand"
WARN_YIRMI_POS="Warning: --yes-i-really-mean-it should appear after the subcommand"
WARN_YIRMI_DUP="Warning: --yes-i-really-mean-it specified multiple times, using last value"
WARN_SHOWRESTORE_POS="Warning: --show-restore-stats should appear after the subcommand"
WARN_SHOWRESTORE_DUP="Warning: --show-restore-stats specified multiple times, using last value"
WARN_REMOVE_BAD_POS="Warning: --remove-bad should appear after the subcommand"
WARN_CHECKHEAD_POS="Warning: --check-head-obj-locator should appear after the subcommand"
WARN_ALLOW_UNORDERED_POS="Warning: --allow-unordered should appear after the subcommand"
WARN_OBJVER_POS="Warning: --object-version should appear after the subcommand"
WARN_NEWNAME_POS="Warning: --bucket-new-name should appear after the subcommand"
WARN_BUCKETID_DUP="Warning: --bucket-id specified multiple times, using last value"
WARN_FIX_DUP="Warning: --fix specified multiple times, using last value"
WARN_CHECK_OBJECTS_POS="Warning: --check-objects should appear after the subcommand"
WARN_MAX_IOS_POS="Warning: --max-concurrent-ios should appear after the subcommand"
WARN_MAX_IOS_DUP="Warning: --max-concurrent-ios specified multiple times, using last value"
# radoslist flags
WARN_ORPHAN_POS="Warning: --orphan-stale-secs should appear after the subcommand"
WARN_ORPHAN_DUP="Warning: --orphan-stale-secs specified multiple times, using last value"
WARN_RGW_OBJ_FS_POS="Warning: --rgw-obj-fs should appear after the subcommand"
WARN_RGW_OBJ_FS_DUP="Warning: --rgw-obj-fs specified multiple times, using last value"
WARN_WARNINGS_POS="Warning: --warnings-only should appear after the subcommand"
WARN_DUMP_KEYS_POS="Warning: --dump-keys should appear after the subcommand"
WARN_HIDE_PROGRESS_POS="Warning: --hide-progress should appear after the subcommand"
WARN_DUMP_KEYS_DUP="Warning: --dump-keys specified multiple times, using last value"
WARN_HIDE_PROGRESS_DUP="Warning: --hide-progress specified multiple times, using last value"
# rewrite-specific flags (date flags warn with both aliases joined by '/')
WARN_STARTDATE_POS="Warning: --start-date/--start-time should appear after the subcommand"
WARN_STARTDATE_DUP="Warning: --start-date/--start-time specified multiple times, using last value"
WARN_ENDDATE_POS="Warning: --end-date/--end-time should appear after the subcommand"
WARN_ENDDATE_DUP="Warning: --end-date/--end-time specified multiple times, using last value"
WARN_MINRW_POS="Warning: --min-rewrite-size should appear after the subcommand"
WARN_MINRW_DUP="Warning: --min-rewrite-size specified multiple times, using last value"
WARN_MAXRW_POS="Warning: --max-rewrite-size should appear after the subcommand"
WARN_MINRWSTRIPE_POS="Warning: --min-rewrite-stripe-size should appear after the subcommand"
# set-min-shards / object-shard shared flag
WARN_NUM_SHARDS_POS="Warning: --num-shards should appear after the subcommand"
WARN_NUM_SHARDS_DUP="Warning: --num-shards specified multiple times, using last value"
# object-shard flag
WARN_OBJECT_POS="Warning: --object/-o should appear after the subcommand"
WARN_OBJECT_DUP="Warning: --object/-o specified multiple times, using last value"
# shard-objects flags
WARN_SHARD_ID_POS="Warning: --shard-id should appear after the subcommand"
WARN_SHARD_ID_DUP="Warning: --shard-id specified multiple times, using last value"
WARN_PREFIX_POS="Warning: --prefix should appear after the subcommand"
WARN_PREFIX_DUP="Warning: --prefix specified multiple times, using last value"

# Error message constants
# Legacy behavior: missing --bucket/--uid is not validated up front; the op
# layer fails with raw EINVAL messages, and bucket rm ignores failures
# entirely (see TODOs in radosgw-admin.cc). Tests for these need a cluster.
ERR_FETCH_BUCKET="failure: (22) Invalid argument: failed to fetch bucket info for bucket="
# chown on a nonexistent bucket: RGWBucket::init fails, main prints exit 2
ERR_CHOWN_NO_BUCKET="failure: (2) No such file or directory: failed to fetch bucket info for bucket="
ERR_REQUIRES_USER="failure: (22) Invalid argument: requires user or account id"
ERR_EINVAL="failure: (22) Invalid argument"
ERR_SUBCOMMAND="A subcommand is required"
# logging list/flush on a bucket without logging: error message but exit 0 (legacy quirk)
ERR_NO_LOGGING="does not have logging enabled"
# legacy message spells the flag with an underscore (pre-migration typo, kept verbatim)
ERR_INCONSISTENT="using --inconsistent_index can corrupt the bucket index"

# Filter out noisy ceph log lines and config-not-found lines
filter() {
  grep -v "^[0-9]\{4\}-" | \
  grep -v "^did not load config" | \
  grep -v "^unable to get monitor" | \
  grep -v "^failed to fetch mon config"
}

# --no-mon-config skips monitor connection so check()/check_help() run without a cluster.
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

# check_warns "desc" expected_exit "error_msg_or_empty" "warn1" "warn2" ... -- command args
check_warns() {
  local desc="$1" expected_exit="$2" error_msg="$3"
  shift 3
  local msgs=()
  while [ "$1" != "--" ]; do
    msgs+=("$1")
    shift
  done
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
  for msg in "${msgs[@]}"; do
    if ! echo "$output" | grep -qF -- "$msg"; then
      echo "FAIL [$desc]: expected message not found: $msg"
      echo "     output: $output"
      ok=0
    fi
  done
  if [ "$exit_code" != "$expected_exit" ]; then
    echo "FAIL [$desc]: expected exit $expected_exit, got $exit_code"
    echo "     output: $output"
    ok=0
  fi
  if [ -n "$error_msg" ] && ! echo "$output" | grep -qF -- "$error_msg"; then
    echo "FAIL [$desc]: expected error not found: $error_msg"
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

check_help() {
  local desc="$1"; shift
  _run "$@" >/dev/null 2>&1
  local exit_code=$?
  if [ "$exit_code" = "0" ]; then
    echo "PASS [$desc]"
    PASS=$((PASS+1))
  else
    echo "FAIL [$desc]: expected exit 0, got $exit_code"
    FAIL=$((FAIL+1))
  fi
}

check_help_content() {
  local desc="$1" expected_content="$2"; shift 2
  local output
  output=$(_run "$@" 2>&1)
  local exit_code=$?
  if [ "$exit_code" != "0" ]; then
    echo "FAIL [$desc]: expected exit 0, got $exit_code"
    FAIL=$((FAIL+1))
    return
  fi
  if ! echo "$output" | grep -qF -- "$expected_content"; then
    echo "FAIL [$desc]: expected '$expected_content' in help output"
    echo "     output: $(echo "$output" | head -3)"
    FAIL=$((FAIL+1))
  else
    echo "PASS [$desc]"
    PASS=$((PASS+1))
  fi
}

# ============================================================
echo "=== bucket (bare) ==="
# ============================================================

check "bare bucket"          1 "$ERR_SUBCOMMAND" bucket
check "bare buckets (alias)" 1 "$ERR_SUBCOMMAND" buckets
check "unknown subcommand"   1 "$ERR_SUBCOMMAND" bucket banana

# ============================================================
echo ""
echo "=== buckets alias (non-list commands) ==="
# ============================================================

# no-cluster: alias works for all subcommands, not just list
check "buckets stats: stray arg"    1 "ERROR: unexpected argument: 'strayarg'" \
  buckets stats strayarg
# legacy: missing --bucket is not validated up front; rm silently exits 0,
# link/unlink fail in the op layer (cluster needed)
check_cluster "buckets rm: missing --bucket (silent exit 0)" 0 "" -- \
  buckets rm
check_cluster "buckets link: missing --bucket" 22 "$ERR_FETCH_BUCKET" -- \
  buckets link --uid testuser
check_cluster "buckets unlink: missing --bucket" 22 "$ERR_EINVAL" -- \
  buckets unlink --uid testuser
check "buckets check: stray arg"    1 "ERROR: unexpected argument: 'strayarg'" \
  buckets check strayarg
check "buckets list: unrecognized flag"    22 "ERROR: invalid flag --fakeflag" \
  buckets list --fakeflag
check "buckets link: stray after flags"    1 "ERROR: unexpected argument: 'strayarg'" \
  buckets link --bucket mybucket --uid testuser strayarg
check "buckets link: unrecognized flag"    22 "ERROR: invalid flag --fakeflag" \
  buckets link --bucket mybucket --uid testuser --fakeflag
check "buckets unlink: stray after flags"  1 "ERROR: unexpected argument: 'strayarg'" \
  buckets unlink --bucket mybucket --uid testuser strayarg
check "buckets unlink: unrecognized flag"  22 "ERROR: invalid flag --fakeflag" \
  buckets unlink --bucket mybucket --uid testuser --fakeflag
check "buckets rm: stray after flags"      1 "ERROR: unexpected argument: 'strayarg'" \
  buckets rm --bucket mybucket strayarg
check "buckets rm: unrecognized flag"      22 "ERROR: invalid flag --fakeflag" \
  buckets rm --bucket mybucket --fakeflag
check "buckets check: unrecognized flag"   22 "ERROR: invalid flag --fakeflag" \
  buckets check --fakeflag

# ============================================================
echo ""
echo "=== bucket list ==="
# ============================================================

# stray positional args
check "list: stray after flags"              1 "ERROR: unexpected argument: 'strayarg'" \
  bucket list strayarg
check "list: stray before bucket"            1 "ERROR: unexpected argument: 'foo'" \
  foo bucket list
check "list: stray between bucket and list"  1 "ERROR: unexpected argument: 'extra'" \
  bucket extra list

# unrecognized flag
check "list: unrecognized flag" 22 "ERROR: invalid flag --fakeflag" \
  bucket list --fakeflag

# flags in wrong position: check() catches these when combined with a required-arg error.
# Pure wrong-position (no error) tests are in check_warns below.

# missing option value
check "list: --bucket missing value"         1 "--bucket: 1 required TEXT missing" \
  bucket list --bucket
check "list: --uid missing value"            1 "--uid: 1 required TEXT missing" \
  bucket list --uid
check "list: --bucket-id missing value"      1 "--bucket-id: 1 required TEXT missing" \
  bucket list --bucket-id
check "list: --format missing value"         1 "--format: 1 required TEXT missing" \
  bucket list --format
check "list: --max-entries missing value"    1 "--max-entries: 1 required INT missing" \
  bucket list --max-entries
# out-of-int-range value rejected by the strict base-10 setter (strict_strtol's
# int range check), same shape as CLI11's own overflow rejection
check "list: --max-entries out of int range" 22 "Could not convert: --max-entries = 5000000000" \
  bucket list --max-entries 5000000000
check "list: --marker missing value"         1 "--marker: 1 required TEXT missing" \
  bucket list --marker
check "list: --object-version missing value" 1 "--object-version: 1 required TEXT missing" \
  bucket list --object-version

# ============================================================
echo ""
echo "=== flags: underscore vs dash spelling (normalize_cli11_tokens rewrite) ==="
# ============================================================
# Legacy accepts '_' as well as '-' in long option names. CLI11 only knows
# the dash form, so normalize_cli11_tokens() rewrites a recognized flag's
# NAME to dashes before parsing. Each pair below proves the underscore
# spelling now behaves identically to the dash spelling, in both the space
# form (value is the next token) and the '=' form (value glued on).

# space form: value reaches CLI11's own strict-int check either way
check "list: --max-entries space form (dash)"       22 "Could not convert: --max-entries = banana" \
  bucket list --max-entries banana
check "list: --max_entries space form (underscore)" 22 "Could not convert: --max-entries = banana" \
  bucket list --max_entries banana

# '=' form
check "list: --max-entries= form (dash)"       22 "Could not convert: --max-entries = banana" \
  bucket list --max-entries=banana
check "list: --max_entries= form (underscore)" 22 "Could not convert: --max-entries = banana" \
  bucket list --max_entries=banana

# a value that itself looks like a flag (starts with --, contains '_') must
# never be touched by the rewrite: only the flag NAME is rewritten, never
# the value. --bucket has no underscore in its own name, so nothing here
# is rewritten; the literal value is used as the bucket name.
check "list: --bucket=--hello_world (glued value untouched)" 2 \
  "ERROR: could not init bucket: (2) No such file or directory" \
  bucket list --bucket=--hello_world
check "list: --bucket --hello_world (space-form value untouched)" 2 \
  "ERROR: could not init bucket: (2) No such file or directory" \
  bucket list --bucket --hello_world
check "list: -b=--hello_world (short flag, glued value untouched)" 2 \
  "ERROR: could not init bucket: (2) No such file or directory" \
  bucket list -b=--hello_world

# --bucket-id/--bucket_id: the flag NAME is rewritten (underscore -> dash),
# but the flag-like glued/space-form VALUE is still passed through as-is.
check "list: --bucket-id=--hello_world (dash, glued value untouched)" 0 '"demo"' \
  bucket list --bucket-id=--hello_world
check "list: --bucket_id=--hello_world (underscore, glued value untouched)" 0 '"demo"' \
  bucket list --bucket_id=--hello_world
check "list: --bucket-id --hello_world (dash, space-form value untouched)" 0 '"demo"' \
  bucket list --bucket-id --hello_world
check "list: --bucket_id --hello_world (underscore, space-form value untouched)" 0 '"demo"' \
  bucket list --bucket_id --hello_world

# unknown flag keeps the user's own spelling (not rewritten, not recognized)
check "list: unrecognized underscore flag: value strands" 1 \
  "ERROR: unexpected argument: '1'" \
  bucket list --banana_flag 1

# valueless (binary) flag: underscore and dash spellings both succeed
check_cluster "rm: --purge-objects (dash)"       0 "" -- \
  bucket rm --bucket=cli11-no-such-bucket --purge-objects
check_cluster "rm: --purge_objects (underscore)" 0 "" -- \
  bucket rm --bucket=cli11-no-such-bucket --purge_objects

# ============================================================
echo ""
echo "=== bucket stats ==="
# ============================================================

check "stats: stray after flags"             1 "ERROR: unexpected argument: 'strayarg'" \
  bucket stats strayarg
check "stats: stray before bucket"           1 "ERROR: unexpected argument: 'foo'" \
  foo bucket stats
check "stats: stray between bucket and stats" 1 "ERROR: unexpected argument: 'extra'" \
  bucket extra stats

check "stats: unrecognized flag" 22 "ERROR: invalid flag --fakeflag" \
  bucket stats --fakeflag

check "stats: --bucket missing value"      1 "--bucket: 1 required TEXT missing" \
  bucket stats --bucket
check "stats: --bucket-id missing value"   1 "--bucket-id: 1 required TEXT missing" \
  bucket stats --bucket-id
check "stats: --format missing value"      1 "--format: 1 required TEXT missing" \
  bucket stats --format
check "stats: --max-entries missing value" 1 "--max-entries: 1 required INT missing" \
  bucket stats --max-entries
check "stats: --marker missing value"      1 "--marker: 1 required TEXT missing" \
  bucket stats --marker

# ============================================================
echo ""
echo "=== bucket layout ==="
# ============================================================

check "layout: stray after flags"              1 "ERROR: unexpected argument: 'strayarg'" \
  bucket layout strayarg
check "layout: stray before bucket"            1 "ERROR: unexpected argument: 'foo'" \
  foo bucket layout
check "layout: stray between bucket and layout" 1 "ERROR: unexpected argument: 'extra'" \
  bucket extra layout

check "layout: unrecognized flag" 22 "ERROR: invalid flag --fakeflag" \
  bucket layout --fakeflag

check "layout: --bucket missing value"    1 "--bucket: 1 required TEXT missing" \
  bucket layout --bucket
check "layout: --bucket-id missing value" 1 "--bucket-id: 1 required TEXT missing" \
  bucket layout --bucket-id
check "layout: --tenant missing value"    1 "--tenant: 1 required TEXT missing" \
  bucket layout --tenant
check "layout: --format missing value"    1 "--format: 1 required TEXT missing" \
  bucket layout --format

# handler-level (cluster): bucket_name.empty() is checked inside the action,
# nonexistent bucket fails init_bucket silently with exit 2 (legacy quirk kept)
check_cluster "layout: missing --bucket" 22 "ERROR: bucket not specified" -- \
  bucket layout
check_cluster "layout: nonexistent bucket (silent exit 2)" 2 "" -- \
  bucket layout --bucket cli11-no-such-bucket

# wrong-position warnings (flag before the leaf subcommand). --bucket/--bucket-id/
# --format warn then fail on the nonexistent bucket (exit 2); --tenant trips the
# global "no user ID" check (exit 22) before reaching the bucket.
check_warns "layout: --bucket before subcommand"    2 "" "$WARN_BUCKET_POS" -- \
  bucket --bucket cli11-no-such-bucket layout
check_warns "layout: --bucket-id before subcommand" 2 "" "$WARN_BUCKETID_POS" -- \
  bucket --bucket-id x layout --bucket cli11-no-such-bucket
check_warns "layout: --format before subcommand"    2 "" "$WARN_FORMAT_POS" -- \
  bucket --format json layout --bucket cli11-no-such-bucket
check_warns "layout: --tenant before subcommand"    22 "ERROR: --tenant is set, but there's no user ID" -- \
  bucket --tenant t layout --bucket cli11-no-such-bucket

# duplicate-flag warnings (flag specified twice; last value wins)
check_warns "layout: duplicate --bucket"    2 "" "$WARN_BUCKET_DUP" -- \
  bucket layout --bucket a --bucket cli11-no-such-bucket
check_warns "layout: duplicate --bucket-id" 2 "" "$WARN_BUCKETID_DUP" -- \
  bucket layout --bucket-id a --bucket-id b --bucket cli11-no-such-bucket
check_warns "layout: duplicate --format"    2 "" "$WARN_FORMAT_DUP" -- \
  bucket layout --format json --format json --bucket cli11-no-such-bucket
check_warns "layout: duplicate --tenant"    22 "ERROR: --tenant is set, but there's no user ID" "$WARN_TENANT_DUP" -- \
  bucket layout --tenant a --tenant b --bucket cli11-no-such-bucket

# ============================================================
echo ""
echo "=== bucket chown ==="
# ============================================================

# stray positional args
check "chown: stray after flags"               1 "ERROR: unexpected argument: 'strayarg'" \
  bucket chown strayarg
check "chown: stray before bucket"             1 "ERROR: unexpected argument: 'foo'" \
  foo bucket chown
check "chown: stray between bucket and chown"  1 "ERROR: unexpected argument: 'extra'" \
  bucket extra chown

check "chown: unrecognized flag" 22 "ERROR: invalid flag --fakeflag" \
  bucket chown --fakeflag

# missing option value
check "chown: --bucket missing value"          1 "--bucket: 1 required TEXT missing" \
  bucket chown --bucket
check "chown: --uid missing value"             1 "--uid: 1 required TEXT missing" \
  bucket chown --uid
check "chown: --marker missing value"          1 "--marker: 1 required TEXT missing" \
  bucket chown --marker
check "chown: --tenant missing value"          1 "--tenant: 1 required TEXT missing" \
  bucket chown --tenant
check "chown: --bucket-new-name missing value" 1 "--bucket-new-name: 1 required TEXT missing" \
  bucket chown --bucket-new-name

# --bucket-id is NOT a chown option (the handler never read it); the flag is
# swallowed and its value trips the stray-positional check (exit 22).
check_warns "chown: --bucket-id swallowed+warned (not a chown option)" 22 "ERROR: bucket name not specified" \
  "Warning: --bucket-id is not a valid option for 'bucket chown'" -- \
  bucket chown --bucket-id x

# handler-level (cluster): bucket_name.empty() is checked inside the action
# (note the "bucket name not specified" wording differs from layout); a
# nonexistent bucket fails RGWBucket::init with exit 2
check_cluster "chown: missing --bucket" 22 "ERROR: bucket name not specified" -- \
  bucket chown
check_cluster "chown: nonexistent bucket (exit 2)" 2 "$ERR_CHOWN_NO_BUCKET" -- \
  bucket chown --bucket cli11-no-such-bucket --uid cli11_no_such_user

# wrong-position warnings (flag before the leaf subcommand); all warn then fail on
# the nonexistent bucket (exit 2). --uid is supplied, so --tenant does NOT trip the
# global "no user ID" check here.
check_warns "chown: --bucket before subcommand"          2 "" "$WARN_BUCKET_POS" -- \
  bucket --bucket cli11-no-such-bucket chown --uid cli11_no_such_user
check_warns "chown: --uid before subcommand"             2 "" -- \
  bucket --uid cli11_no_such_user chown --bucket cli11-no-such-bucket
check_warns "chown: --marker before subcommand"          2 "" "$WARN_MARKER_POS" -- \
  bucket --marker m chown --bucket cli11-no-such-bucket --uid cli11_no_such_user
check_warns "chown: --tenant before subcommand"          2 "" -- \
  bucket --tenant t chown --bucket cli11-no-such-bucket --uid cli11_no_such_user
check_warns "chown: --bucket-new-name before subcommand" 2 "" "$WARN_NEWNAME_POS" -- \
  bucket --bucket-new-name nn chown --bucket cli11-no-such-bucket --uid cli11_no_such_user

# duplicate-flag warnings (flag specified twice; last value wins)
check_warns "chown: duplicate --bucket" 2 "" "$WARN_BUCKET_DUP" -- \
  bucket chown --bucket a --bucket cli11-no-such-bucket --uid cli11_no_such_user
check_warns "chown: duplicate --uid"    2 "" "$WARN_UID_DUP" -- \
  bucket chown --uid a --uid cli11_no_such_user --bucket cli11-no-such-bucket

# ============================================================
echo ""
echo "=== bucket limit check ==="
# ============================================================

# 'bucket limit' is an internal node: it requires the 'check' subcommand
check "limit (incomplete command)" 1 "$ERR_SUBCOMMAND" \
  bucket limit

check "limit check: stray after" 1 "ERROR: unexpected argument: 'strayarg'" \
  bucket limit check strayarg

check "limit check: unrecognized flag" 22 "ERROR: invalid flag --fakeflag" \
  bucket limit check --fakeflag

check "limit check: --uid missing value" 1 "--uid: 1 required TEXT missing" \
  bucket limit check --uid

# handler-level (cluster): no --uid iterates all users; all paths exit 0
check_cluster "limit check: no args (all users)" 0 "" -- \
  bucket limit check
check_cluster "limit check: --warnings-only" 0 "" -- \
  bucket limit check --warnings-only
check_cluster "limit check: nonexistent --uid (empty listing, exit 0)" 0 "" -- \
  bucket limit check --uid cli11_no_such_user

# wrong-position warnings (flag before the 'check' leaf); all paths exit 0
check_warns "limit check: --uid before subcommand"           0 "" -- \
  bucket --uid cli11_no_such_user limit check
check_warns "limit check: --warnings-only before subcommand" 0 "" "$WARN_WARNINGS_POS" -- \
  bucket --warnings-only limit check

# duplicate-flag warning (flag specified twice; last value wins)
check_warns "limit check: duplicate --uid" 0 "" "$WARN_UID_DUP" -- \
  bucket limit check --uid a --uid cli11_no_such_user

# ============================================================
echo ""
echo "=== bucket logging (info/list/flush) ==="
# ============================================================

# 'bucket logging' is an internal node (require_subcommand): both an incomplete
# command and an unknown subcommand report "A subcommand is required", exit 106
check "logging (incomplete command)" 1 "$ERR_SUBCOMMAND" \
  bucket logging
check "logging: unknown subcommand"  1 "$ERR_SUBCOMMAND" \
  bucket logging banana

# stray positional args
check "logging info: stray after"   1 "ERROR: unexpected argument: 'strayarg'" \
  bucket logging info strayarg
check "logging list: stray after"   1 "ERROR: unexpected argument: 'strayarg'" \
  bucket logging list strayarg
check "logging flush: stray after"  1 "ERROR: unexpected argument: 'strayarg'" \
  bucket logging flush strayarg
check "logging info: stray before"  1 "ERROR: unexpected argument: 'foo'" \
  foo bucket logging info

# unrecognized flag
check "logging info: unrecognized flag"  22 "ERROR: invalid flag --fakeflag" \
  bucket logging info --fakeflag
check "logging list: unrecognized flag"  22 "ERROR: invalid flag --fakeflag" \
  bucket logging list --fakeflag
check "logging flush: unrecognized flag" 22 "ERROR: invalid flag --fakeflag" \
  bucket logging flush --fakeflag

# missing option value (flush has no --format)
check "logging info: --bucket missing value"    1 "--bucket: 1 required TEXT missing" \
  bucket logging info --bucket
check "logging info: --bucket-id missing value" 1 "--bucket-id: 1 required TEXT missing" \
  bucket logging info --bucket-id
check "logging info: --tenant missing value"    1 "--tenant: 1 required TEXT missing" \
  bucket logging info --tenant
check "logging info: --format missing value"    1 "--format: 1 required TEXT missing" \
  bucket logging info --format
check "logging list: --format missing value"    1 "--format: 1 required TEXT missing" \
  bucket logging list --format
check "logging flush: --bucket missing value"   1 "--bucket: 1 required TEXT missing" \
  bucket logging flush --bucket

# handler-level (cluster): bucket_name.empty() is checked inside the action;
# a nonexistent bucket fails init_bucket silently with exit 2
check_cluster "logging info: missing --bucket"  22 "ERROR: bucket not specified" -- \
  bucket logging info
check_cluster "logging list: missing --bucket"  22 "ERROR: bucket not specified" -- \
  bucket logging list
check_cluster "logging flush: missing --bucket" 22 "ERROR: bucket not specified" -- \
  bucket logging flush
check_cluster "logging info: nonexistent bucket (silent exit 2)"  2 "" -- \
  bucket logging info --bucket cli11-no-such-bucket
check_cluster "logging list: nonexistent bucket (silent exit 2)"  2 "" -- \
  bucket logging list --bucket cli11-no-such-bucket
check_cluster "logging flush: nonexistent bucket (silent exit 2)" 2 "" -- \
  bucket logging flush --bucket cli11-no-such-bucket

# wrong-position warnings (flag before the leaf subcommand). --bucket/--bucket-id/
# --format warn then fail on the nonexistent bucket (exit 2); --tenant trips the
# global "no user ID" check (exit 22) before reaching the bucket.
check_warns "logging info: --bucket before subcommand"    2 "" "$WARN_BUCKET_POS" -- \
  bucket --bucket cli11-no-such-bucket logging info
check_warns "logging info: --bucket-id before subcommand" 2 "" "$WARN_BUCKETID_POS" -- \
  bucket --bucket-id x logging info --bucket cli11-no-such-bucket
check_warns "logging list: --format before subcommand"    2 "" "$WARN_FORMAT_POS" -- \
  bucket --format json logging list --bucket cli11-no-such-bucket
check_warns "logging info: --tenant before subcommand"    22 "ERROR: --tenant is set, but there's no user ID" -- \
  bucket --tenant t logging info --bucket cli11-no-such-bucket

# duplicate-flag warnings (flag specified twice; last value wins)
check_warns "logging info: duplicate --bucket" 2 "" "$WARN_BUCKET_DUP" -- \
  bucket logging info --bucket a --bucket cli11-no-such-bucket
check_warns "logging list: duplicate --tenant" 22 "ERROR: --tenant is set, but there's no user ID" "$WARN_TENANT_DUP" -- \
  bucket logging list --tenant a --tenant b --bucket cli11-no-such-bucket

# ============================================================
echo ""
echo "=== bucket rewrite ==="
# ============================================================

# stray positional args
check "rewrite: stray after flags"               1 "ERROR: unexpected argument: 'strayarg'" \
  bucket rewrite strayarg
check "rewrite: stray before bucket"             1 "ERROR: unexpected argument: 'foo'" \
  foo bucket rewrite
check "rewrite: stray between bucket and rewrite" 1 "ERROR: unexpected argument: 'extra'" \
  bucket extra rewrite

check "rewrite: unrecognized flag" 22 "ERROR: invalid flag --fakeflag" \
  bucket rewrite --fakeflag

# missing option value (parse-level, exit 114). The size flags are bound to a
# string sink (legacy atoll compat) so they behave like any other TEXT option
# here; the date flags report the canonical name even when the alias is used.
check "rewrite: --bucket missing value"                1 "--bucket: 1 required TEXT missing" \
  bucket rewrite --bucket
check "rewrite: --bucket-id missing value"             1 "--bucket-id: 1 required TEXT missing" \
  bucket rewrite --bucket-id
check "rewrite: --tenant missing value"                1 "--tenant: 1 required TEXT missing" \
  bucket rewrite --tenant
check "rewrite: --format missing value"                1 "--format: 1 required TEXT missing" \
  bucket rewrite --format
check "rewrite: --start-date missing value"            1 "--start-date: 1 required TEXT missing" \
  bucket rewrite --start-date
check "rewrite: --start-time missing value (alias)"    1 "--start-date: 1 required TEXT missing" \
  bucket rewrite --start-time
check "rewrite: --end-date missing value"              1 "--end-date: 1 required TEXT missing" \
  bucket rewrite --end-date
check "rewrite: --end-time missing value (alias)"      1 "--end-date: 1 required TEXT missing" \
  bucket rewrite --end-time
check "rewrite: --min-rewrite-size missing value"        1 "--min-rewrite-size: 1 required TEXT missing" \
  bucket rewrite --min-rewrite-size
check "rewrite: --max-rewrite-size missing value"        1 "--max-rewrite-size: 1 required TEXT missing" \
  bucket rewrite --max-rewrite-size
check "rewrite: --min-rewrite-stripe-size missing value" 1 "--min-rewrite-stripe-size: 1 required TEXT missing" \
  bucket rewrite --min-rewrite-stripe-size

# handler-level (cluster): bucket_name.empty() is checked inside the action;
# a nonexistent bucket fails init_bucket with exit 2 (legacy quirk kept)
check_cluster "rewrite: missing --bucket" 22 "ERROR: bucket not specified" -- \
  bucket rewrite
check_cluster "rewrite: nonexistent bucket (exit 2)" 2 "ERROR: could not init bucket" -- \
  bucket rewrite --bucket cli11-no-such-bucket

# atoll compat: malformed size values are ACCEPTED at parse (not rejected like a
# strict integer would be) and only later become 0 via atoll. We assert parse
# acceptance by reaching init_bucket (exit 2), not a parse error (exit 22).
check_cluster "rewrite: --min-rewrite-size=abc accepted (atoll compat)" 2 "ERROR: could not init bucket" -- \
  bucket rewrite --bucket cli11-no-such-bucket --min-rewrite-size=abc
check_cluster "rewrite: --max-rewrite-size=abc accepted (atoll compat)" 2 "ERROR: could not init bucket" -- \
  bucket rewrite --bucket cli11-no-such-bucket --max-rewrite-size=abc
check_cluster "rewrite: --min-rewrite-stripe-size=abc accepted (atoll compat)" 2 "ERROR: could not init bucket" -- \
  bucket rewrite --bucket cli11-no-such-bucket --min-rewrite-stripe-size=abc

# wrong-position warnings (flag before the leaf subcommand); all warn then fail
# on the nonexistent bucket (exit 2). --tenant trips the global "no user ID"
# check (exit 22) before reaching the bucket.
check_warns "rewrite: --bucket before subcommand"     2 "" "$WARN_BUCKET_POS" -- \
  bucket --bucket cli11-no-such-bucket rewrite
check_warns "rewrite: --bucket-id before subcommand"  2 "" "$WARN_BUCKETID_POS" -- \
  bucket --bucket-id x rewrite --bucket cli11-no-such-bucket
check_warns "rewrite: --format before subcommand"     2 "" "$WARN_FORMAT_POS" -- \
  bucket --format json rewrite --bucket cli11-no-such-bucket
check_warns "rewrite: --start-date before subcommand" 2 "" "$WARN_STARTDATE_POS" -- \
  bucket --start-date 2020-01-01 rewrite --bucket cli11-no-such-bucket
check_warns "rewrite: --end-date before subcommand"   2 "" "$WARN_ENDDATE_POS" -- \
  bucket --end-date 2020-01-01 rewrite --bucket cli11-no-such-bucket
check_warns "rewrite: --min-rewrite-size before subcommand"        2 "" "$WARN_MINRW_POS" -- \
  bucket --min-rewrite-size 1 rewrite --bucket cli11-no-such-bucket
check_warns "rewrite: --max-rewrite-size before subcommand"        2 "" "$WARN_MAXRW_POS" -- \
  bucket --max-rewrite-size 1 rewrite --bucket cli11-no-such-bucket
check_warns "rewrite: --min-rewrite-stripe-size before subcommand" 2 "" "$WARN_MINRWSTRIPE_POS" -- \
  bucket --min-rewrite-stripe-size 1 rewrite --bucket cli11-no-such-bucket
check_warns "rewrite: --tenant before subcommand"     22 "ERROR: --tenant is set, but there's no user ID" -- \
  bucket --tenant t rewrite --bucket cli11-no-such-bucket

# duplicate-flag warnings (flag specified twice; last value wins)
check_warns "rewrite: duplicate --bucket"            2 "" "$WARN_BUCKET_DUP" -- \
  bucket rewrite --bucket a --bucket cli11-no-such-bucket
check_warns "rewrite: duplicate --start-date"        2 "" "$WARN_STARTDATE_DUP" -- \
  bucket rewrite --start-date 2020-01-01 --start-date 2021-01-01 --bucket cli11-no-such-bucket
check_warns "rewrite: duplicate --min-rewrite-size"  2 "" "$WARN_MINRW_DUP" -- \
  bucket rewrite --min-rewrite-size 1 --min-rewrite-size 2 --bucket cli11-no-such-bucket
check_warns "rewrite: duplicate --tenant"            22 "ERROR: --tenant is set, but there's no user ID" "$WARN_TENANT_DUP" -- \
  bucket rewrite --tenant a --tenant b --bucket cli11-no-such-bucket

# multi-warning combinations (2 and 3 warnings)
check_warns "rewrite: --bucket + --min-rewrite-size before (2 pos warnings)" 2 "" \
  "$WARN_BUCKET_POS" "$WARN_MINRW_POS" -- \
  bucket --bucket cli11-no-such-bucket --min-rewrite-size 1 rewrite
check_warns "rewrite: pos + duplicate --bucket (2 warns)"                    2 "" \
  "$WARN_BUCKET_POS" "$WARN_BUCKET_DUP" -- \
  bucket --bucket a rewrite --bucket cli11-no-such-bucket
check_warns "rewrite: --start-date + --end-date + --tenant before (2 warns; --tenant global, no warn)" 22 \
  "ERROR: --tenant is set, but there's no user ID" \
  "$WARN_STARTDATE_POS" "$WARN_ENDDATE_POS" -- \
  bucket --start-date 2020-01-01 --end-date 2021-01-01 --tenant t rewrite --bucket cli11-no-such-bucket

# ============================================================
echo ""
echo "=== bucket set-min-shards ==="
# ============================================================

# stray positional args
check "set-min-shards: stray after flags"            1 "ERROR: unexpected argument: 'strayarg'" \
  bucket set-min-shards strayarg
check "set-min-shards: stray before bucket"          1 "ERROR: unexpected argument: 'foo'" \
  foo bucket set-min-shards
check "set-min-shards: stray between bucket and leaf" 1 "ERROR: unexpected argument: 'extra'" \
  bucket extra set-min-shards

check "set-min-shards: unrecognized flag" 22 "ERROR: invalid flag --fakeflag" \
  bucket set-min-shards --fakeflag
# Unrelated flags differ by TYPE, not relatedness (both --fix and --max-entries
# are registered on the shared root via add_multilevel_*). A value option in
# SPACE form leaks its value token as a stray positional (exit 22) — the known
# global space-form-value divergence; legacy parsed-and-ignored it. The =form
# binds the value and is accepted instead (see the cluster cases below).
check_warns "set-min-shards: unrelated --max-entries 5 swallowed+warned (space form)" 234 "ERROR: bucket not specified" \
  "Warning: --max-entries is not a valid option for 'bucket set-min-shards'" -- \
  bucket set-min-shards --max-entries 5

# missing option value (parse-level, exit 114)
check "set-min-shards: --bucket missing value"     1 "--bucket: 1 required TEXT missing" \
  bucket set-min-shards --bucket
check "set-min-shards: --bucket-id missing value"  1 "--bucket-id: 1 required TEXT missing" \
  bucket set-min-shards --bucket-id
check "set-min-shards: --tenant missing value"     1 "--tenant: 1 required TEXT missing" \
  bucket set-min-shards --tenant
check "set-min-shards: --num-shards missing value" 1 "--num-shards: 1 required INT missing" \
  bucket set-min-shards --num-shards
# --num-shards is a strict CLI11 integer; a non-numeric value is rejected at
# parse (exit 104), where legacy would emit its own strict_strtol error
check "set-min-shards: --num-shards non-integer"   22 "Could not convert: --num-shards = abc" \
  bucket set-min-shards --num-shards abc

# handler-level (cluster): validations live inside cli11_action and run after
# driver init. Order: bucket empty -> num-shards specified -> num-shards >= 1.
# Each returns -EINVAL (shell exit 234), faithful to the legacy handler.
check_cluster "set-min-shards: missing --bucket"            234 "ERROR: bucket not specified" -- \
  bucket set-min-shards --num-shards 11
check_cluster "set-min-shards: --num-shards not specified"  234 "ERROR: --num-shards not specified" -- \
  bucket set-min-shards --bucket cli11-no-such-bucket
check_cluster "set-min-shards: --num-shards < 1"            234 "ERROR: --num-shards must be at least 1" -- \
  bucket set-min-shards --bucket cli11-no-such-bucket --num-shards 0
# valid args but nonexistent bucket: init_bucket fails (exit 2, no handler message)
check_cluster "set-min-shards: nonexistent bucket (exit 2)" 2 "" -- \
  bucket set-min-shards --bucket cli11-no-such-bucket --num-shards 11
# The three unrelated-flag cases side by side (identical args, only the flag
# differs): a binary flag (--fix, expected(0,1)) takes 0 values -> accepted,
# exit 2; a value option in =form binds its value -> accepted, exit 2; the same
# value option in SPACE form leaks its value as a stray positional -> exit 22
# (the known global space-form-value divergence; legacy parsed-and-ignored it).
check_cluster "set-min-shards: unrelated binary flag --fix accepted (exit 2)" 2 "" -- \
  bucket set-min-shards --fix --bucket cli11-no-such-bucket --num-shards 11
check_cluster "set-min-shards: unrelated value flag --max-entries=5 (=form, exit 2)" 2 "" -- \
  bucket set-min-shards --max-entries=5 --bucket cli11-no-such-bucket --num-shards 11
check_warns "set-min-shards: unrelated --max-entries 5 swallowed+warned (space form, +bucket+num-shards)" 2 "" \
  "Warning: --max-entries is not a valid option for 'bucket set-min-shards'" -- \
  bucket set-min-shards --max-entries 5 --bucket cli11-no-such-bucket --num-shards 11

# wrong-position warnings (flag before the leaf subcommand). The value still
# flows in via the shared option, so with a valid --num-shards and a nonexistent
# bucket they warn then fail at init_bucket (exit 2). --tenant trips the global
# "no user ID" check (exit 22).
check_warns "set-min-shards: --bucket before subcommand"     2 "" "$WARN_BUCKET_POS" -- \
  bucket --bucket cli11-no-such-bucket set-min-shards --num-shards 11
check_warns "set-min-shards: -b before subcommand (short)"   2 "" "$WARN_BUCKET_POS" -- \
  bucket -b cli11-no-such-bucket set-min-shards --num-shards 11
check_warns "set-min-shards: --bucket-id before subcommand"  2 "" "$WARN_BUCKETID_POS" -- \
  bucket --bucket-id x set-min-shards --bucket cli11-no-such-bucket --num-shards 11
check_warns "set-min-shards: --num-shards before subcommand" 2 "" "$WARN_NUM_SHARDS_POS" -- \
  bucket --num-shards 11 set-min-shards --bucket cli11-no-such-bucket
check_warns "set-min-shards: --tenant before subcommand"     22 "ERROR: --tenant is set, but there's no user ID" -- \
  bucket --tenant t set-min-shards --bucket cli11-no-such-bucket --num-shards 11

# duplicate-flag warnings (flag specified twice; last value wins)
check_warns "set-min-shards: duplicate --bucket"     2 "" "$WARN_BUCKET_DUP" -- \
  bucket set-min-shards --bucket a --bucket cli11-no-such-bucket --num-shards 11
check_warns "set-min-shards: duplicate --num-shards" 2 "" "$WARN_NUM_SHARDS_DUP" -- \
  bucket set-min-shards --bucket cli11-no-such-bucket --num-shards 11 --num-shards 12
check_warns "set-min-shards: duplicate --tenant"     22 "ERROR: --tenant is set, but there's no user ID" "$WARN_TENANT_DUP" -- \
  bucket set-min-shards --tenant a --tenant b --bucket cli11-no-such-bucket --num-shards 11

# multi-warning combinations (2 and 3 warnings)
check_warns "set-min-shards: --bucket + --num-shards before (2 pos warnings)" 2 "" \
  "$WARN_BUCKET_POS" "$WARN_NUM_SHARDS_POS" -- \
  bucket --bucket cli11-no-such-bucket --num-shards 11 set-min-shards
check_warns "set-min-shards: pos + duplicate --bucket (2 warns)"             2 "" \
  "$WARN_BUCKET_POS" "$WARN_BUCKET_DUP" -- \
  bucket --bucket a set-min-shards --bucket cli11-no-such-bucket --num-shards 11
check_warns "set-min-shards: --bucket + --num-shards + --tenant before (2 warns; --tenant global, no warn)" 22 \
  "ERROR: --tenant is set, but there's no user ID" \
  "$WARN_BUCKET_POS" "$WARN_NUM_SHARDS_POS" -- \
  bucket --bucket cli11-no-such-bucket --num-shards 11 --tenant t set-min-shards

# ============================================================
echo ""
echo "=== bucket object shard ==="
# ============================================================

# stray positional args
check "object shard: stray after flags"              1 "ERROR: unexpected argument: 'stray'" \
  bucket object shard stray
check "object shard: stray before bucket"            1 "ERROR: unexpected argument: 'foo'" \
  foo bucket object shard
check "object shard: stray between object and shard" 1 "ERROR: unexpected argument: 'extra'" \
  bucket object extra shard
check "object shard: stray word after leaf (banana)"  1 "ERROR: unexpected argument: 'banana'" \
  bucket object shard banana

# unknown subcommand under the 'object' node: require_subcommand fires (exit 106)
check "object: unknown subcommand (banana)"           1 "A subcommand is required" \
  bucket object banana
check "object: no subcommand"                         1 "A subcommand is required" \
  bucket object

check "object shard: unrecognized flag" 22 "ERROR: invalid flag --fakeflag" \
  bucket object shard --fakeflag
# Unrelated flags differ by TYPE (both registered on the shared root). A value
# option in SPACE form leaks its value as a stray positional (exit 22); the =form
# binds the value and is accepted instead (see the cluster cases below).
check_warns "object shard: unrelated --max-entries 5 swallowed+warned (space form)" 0 "" \
  "Warning: --max-entries is not a valid option for 'bucket object shard'" '"shard": 10' -- \
  bucket object shard --object foo --num-shards 11 --max-entries 5

# missing option value (parse-level, exit 114)
check "object shard: --object missing value"     1 "--object: 1 required TEXT missing" \
  bucket object shard --object
check "object shard: --num-shards missing value" 1 "--num-shards: 1 required INT missing" \
  bucket object shard --num-shards
# --num-shards is a strict CLI11 integer; a non-numeric value is rejected at parse (exit 104)
check "object shard: --num-shards non-integer"   22 "Could not convert: --num-shards = abc" \
  bucket object shard --object foo --num-shards abc

# strict base-10 parsing, faithful to legacy strict_strtol: a leading 0 does not
# switch to octal ("010" = 10, "08" = 8) and hex is rejected. CLI11's default
# integer binding would auto-detect the base (010 -> 8, 0x10 -> 16, 08 -> error).
# Object "bar" maps to shard 8 of 10 and shard 4 of 8 (probe-verified).
check "object shard: --num-shards hex rejected" 22 "Could not convert: --num-shards = 0x10" \
  bucket object shard --object bar --num-shards 0x10
check_cluster "object shard: --num-shards 010 parses as decimal 10" 0 '"shard": 8' -- \
  bucket object shard --object bar --num-shards 010
check_cluster "object shard: --num-shards 08 parses as 8" 0 '"shard": 4' -- \
  bucket object shard --object bar --num-shards 08
check_warns "object shard: --num-shards 010 before subcommand (base-10 + warn)" 0 "" \
  "Warning: --num-shards should appear after the subcommand" '"shard": 8' -- \
  bucket --num-shards 010 object shard --object bar

# handler-level (cluster): validations live inside cli11_action and run after
# driver init. The handler returns a positive EINVAL (shell exit 22), faithful
# to the legacy handler (note: this differs from set-min-shards' -EINVAL/234).
check_cluster "object shard: missing object (only --num-shards)" 22 "ERROR: num-shards and object must be specified." -- \
  bucket object shard --num-shards 11
check_cluster "object shard: missing num-shards (only --object)" 22 "ERROR: num-shards and object must be specified." -- \
  bucket object shard --object foo
check_cluster "object shard: non-positive num-shards"           22 "ERROR: non-positive value supplied for num-shards: 0" -- \
  bucket object shard --object foo --num-shards 0

# unrelated flags alongside valid args: a binary flag (--fix) takes 0 values ->
# accepted; a value option in =form binds its value -> accepted; both still
# compute the shard (foo % 11 -> 10).
check_cluster "object shard: unrelated binary flag --fix accepted"        0 '"shard": 10' -- \
  bucket object shard --object foo --num-shards 11 --fix
check_cluster "object shard: unrelated value flag --max-entries=5 (=form)" 0 '"shard": 10' -- \
  bucket object shard --object foo --num-shards 11 --max-entries=5

# wrong-position warnings (flag before the leaf subcommand). The value still
# flows in via the shared option, so the shard is still computed (exit 0).
check_warns "object shard: --object before subcommand"     0 "" "$WARN_OBJECT_POS" -- \
  bucket --object foo object shard --num-shards 11
check_warns "object shard: -o before subcommand (short)"   0 "" "$WARN_OBJECT_POS" -- \
  bucket -o foo object shard --num-shards 11
check_warns "object shard: --num-shards before subcommand" 0 "" "$WARN_NUM_SHARDS_POS" -- \
  bucket --num-shards 11 object shard --object foo
check_warns "object shard: --format before subcommand"     0 "" "$WARN_FORMAT_POS" -- \
  bucket --format xml object shard --object foo --num-shards 11

# duplicate-flag warnings (flag specified twice; last value wins)
check_warns "object shard: duplicate --object"     0 "" "$WARN_OBJECT_DUP" -- \
  bucket object shard --object a --object foo --num-shards 11
check_warns "object shard: duplicate --num-shards" 0 "" "$WARN_NUM_SHARDS_DUP" -- \
  bucket object shard --object foo --num-shards 4 --num-shards 11

# multi-warning combinations (2 and 3 warnings)
check_warns "object shard: --object + --num-shards before (2 pos warnings)" 0 "" \
  "$WARN_OBJECT_POS" "$WARN_NUM_SHARDS_POS" -- \
  bucket --object foo --num-shards 11 object shard
check_warns "object shard: pos + duplicate --object (2 warns)"             0 "" \
  "$WARN_OBJECT_POS" "$WARN_OBJECT_DUP" -- \
  bucket --object a object shard --object foo --num-shards 11
check_warns "object shard: --object + --num-shards + --format before (3 warns)" 0 "" \
  "$WARN_OBJECT_POS" "$WARN_NUM_SHARDS_POS" "$WARN_FORMAT_POS" -- \
  bucket --object foo --num-shards 11 --format xml object shard

# ============================================================
echo ""
echo "=== bucket shard objects ==="
# ============================================================

# stray positional args
check "shard objects: stray after flags"               1 "ERROR: unexpected argument: 'stray'" \
  bucket shard objects stray
check "shard objects: stray before bucket"             1 "ERROR: unexpected argument: 'foo'" \
  foo bucket shard objects
check "shard objects: stray between shard and objects" 1 "ERROR: unexpected argument: 'extra'" \
  bucket shard extra objects
check "shard objects: stray word after leaf (banana)"  1 "ERROR: unexpected argument: 'banana'" \
  bucket shard objects banana

# unknown subcommand under the 'shard' node: require_subcommand fires (exit 106)
check "shard: unknown subcommand (banana)"             1 "A subcommand is required" \
  bucket shard banana
check "shard: no subcommand"                           1 "A subcommand is required" \
  bucket shard

check "shard objects: unrecognized flag" 22 "ERROR: invalid flag --fakeflag" \
  bucket shard objects --fakeflag
# value option in SPACE form leaks its value as a stray positional (exit 22); the
# =form binds the value and is accepted instead (see the cluster cases below).
check_warns "shard objects: unrelated --max-entries 5 swallowed+warned (space form)" 0 "" \
  "Warning: --max-entries is not a valid option for 'bucket shard objects'" '"objs"' -- \
  bucket shard objects --num-shards 4 --max-entries 5

# missing option value (parse-level, exit 114)
check "shard objects: --num-shards missing value" 1 "--num-shards: 1 required INT missing" \
  bucket shard objects --num-shards
check "shard objects: --shard-id missing value"   1 "--shard-id: 1 required INT missing" \
  bucket shard objects --shard-id
check "shard objects: --prefix missing value"     1 "--prefix: 1 required TEXT missing" \
  bucket shard objects --prefix
# --num-shards / --shard-id are strict CLI11 integers; a non-numeric value is
# rejected at parse (exit 104)
check "shard objects: --num-shards non-integer"   22 "Could not convert: --num-shards = abc" \
  bucket shard objects --num-shards abc
check "shard objects: --shard-id non-integer"     22 "Could not convert: --shard-id = abc" \
  bucket shard objects --shard-id abc --num-shards 4

# handler-level (cluster): validations live inside cli11_action and run after
# driver init. The handler returns a positive EINVAL (shell exit 22).
check_cluster "shard objects: missing --num-shards"     22 "ERROR: num-shards must be specified." -- \
  bucket shard objects
check_cluster "shard objects: --shard-id >= num-shards" 22 "ERROR: shard-id must be less than num-shards." -- \
  bucket shard objects --num-shards 4 --shard-id 5

# unrelated flags alongside valid args: binary flag (--fix) takes 0 values ->
# accepted; value option in =form binds -> accepted; both still list objs.
check_cluster "shard objects: unrelated binary flag --fix accepted"        0 '"objs"' -- \
  bucket shard objects --num-shards 4 --fix
check_cluster "shard objects: unrelated value flag --max-entries=5 (=form)" 0 '"objs"' -- \
  bucket shard objects --num-shards 4 --max-entries=5

# wrong-position warnings (flag before the leaf subcommand). The value still
# flows in via the shared option, so the objects are still listed (exit 0).
check_warns "shard objects: --num-shards before subcommand" 0 "" "$WARN_NUM_SHARDS_POS" -- \
  bucket --num-shards 4 shard objects
check_warns "shard objects: --shard-id before subcommand"   0 "" "$WARN_SHARD_ID_POS" -- \
  bucket --shard-id 1 shard objects --num-shards 4
check_warns "shard objects: --prefix before subcommand"     0 "" "$WARN_PREFIX_POS" -- \
  bucket --prefix myobj shard objects --num-shards 4
check_warns "shard objects: --format before subcommand"     0 "" "$WARN_FORMAT_POS" -- \
  bucket --format xml shard objects --num-shards 4

# duplicate-flag warnings (flag specified twice; last value wins)
check_warns "shard objects: duplicate --num-shards" 0 "" "$WARN_NUM_SHARDS_DUP" -- \
  bucket shard objects --num-shards 4 --num-shards 8
check_warns "shard objects: duplicate --shard-id"   0 "" "$WARN_SHARD_ID_DUP" -- \
  bucket shard objects --num-shards 4 --shard-id 0 --shard-id 1
check_warns "shard objects: duplicate --prefix"     0 "" "$WARN_PREFIX_DUP" -- \
  bucket shard objects --num-shards 4 --prefix a --prefix myobj

# multi-warning combinations (2 and 3 warnings)
check_warns "shard objects: --num-shards + --shard-id before (2 pos warnings)" 0 "" \
  "$WARN_NUM_SHARDS_POS" "$WARN_SHARD_ID_POS" -- \
  bucket --num-shards 4 --shard-id 1 shard objects
check_warns "shard objects: --num-shards + --shard-id + --prefix before (3 warns)" 0 "" \
  "$WARN_NUM_SHARDS_POS" "$WARN_SHARD_ID_POS" "$WARN_PREFIX_POS" -- \
  bucket --num-shards 4 --shard-id 1 --prefix myobj shard objects

# ============================================================
echo ""
echo "=== bucket resync encrypted multipart ==="
# ============================================================

# stray positional args
check "resync: stray after leaf (banana)"          1 "ERROR: unexpected argument: 'banana'" \
  bucket resync encrypted multipart banana
check "resync: stray before bucket"                1 "ERROR: unexpected argument: 'foo'" \
  foo bucket resync encrypted multipart
check "resync: stray between resync and encrypted"  1 "ERROR: unexpected argument: 'x'" \
  bucket resync x encrypted multipart
check "resync: stray between encrypted and multipart" 1 "ERROR: unexpected argument: 'x'" \
  bucket resync encrypted x multipart

# unknown subcommand under the resync / encrypted nodes (require_subcommand -> 106)
check "resync: unknown subcommand (banana)"           1 "A subcommand is required" \
  bucket resync banana
check "resync encrypted: unknown subcommand (banana)" 1 "A subcommand is required" \
  bucket resync encrypted banana
check "resync: no subcommand"                         1 "A subcommand is required" \
  bucket resync

check "resync: unrecognized flag" 22 "ERROR: invalid flag --fakeflag" \
  bucket resync encrypted multipart --fakeflag
# a value option in SPACE form leaks its value as a stray positional (exit 22)
check_warns "resync: unrelated --max-entries 5 swallowed+warned (space form)" 2 "" \
  "Warning: --max-entries is not a valid option for 'bucket resync encrypted multipart'" -- \
  bucket resync encrypted multipart --bucket cli11chk --max-entries 5 --yes-i-really-mean-it

# missing option value (parse-level, exit 114)
check "resync: --bucket missing value"    1 "--bucket: 1 required TEXT missing" \
  bucket resync encrypted multipart --bucket
check "resync: --bucket-id missing value" 1 "--bucket-id: 1 required TEXT missing" \
  bucket resync encrypted multipart --bucket-id
check "resync: --tenant missing value"    1 "--tenant: 1 required TEXT missing" \
  bucket resync encrypted multipart --tenant
check "resync: --marker missing value"    1 "--marker: 1 required TEXT missing" \
  bucket resync encrypted multipart --marker

# handler-level (cluster). empty bucket -> EINVAL (exit 22). Real-bucket EPERM and
# success cases live in the integration section (need a bucket that exists).
check_cluster "resync: bucket not specified" 22 "ERROR: bucket not specified" -- \
  bucket resync encrypted multipart
# valid args, nonexistent bucket: init_bucket fails (exit 2, no handler message)
check_cluster "resync: nonexistent bucket (exit 2)" 2 "" -- \
  bucket resync encrypted multipart --bucket cli11-no-such-bucket --yes-i-really-mean-it
# unrelated flags alongside valid args: a binary flag (--fix) takes 0 values ->
# accepted; a value option in =form binds -> accepted; both proceed to init_bucket
# (which fails on the nonexistent bucket, exit 2) rather than being rejected at parse.
check_cluster "resync: unrelated binary flag --fix accepted (exit 2)" 2 "" -- \
  bucket resync encrypted multipart --fix --bucket cli11-no-such-bucket --yes-i-really-mean-it
check_cluster "resync: unrelated value flag --max-entries=5 (=form, exit 2)" 2 "" -- \
  bucket resync encrypted multipart --max-entries=5 --bucket cli11-no-such-bucket --yes-i-really-mean-it

# wrong-position warnings (flag before the leaf). Value still flows via the shared
# option, so with a nonexistent bucket they warn then fail at init_bucket (exit 2).
# --tenant trips the global "no user ID" check (exit 22).
check_warns "resync: --bucket before subcommand"  2 "" "$WARN_BUCKET_POS" -- \
  bucket --bucket cli11-no-such-bucket resync encrypted multipart --yes-i-really-mean-it
check_warns "resync: -b before subcommand (short)" 2 "" "$WARN_BUCKET_POS" -- \
  bucket -b cli11-no-such-bucket resync encrypted multipart --yes-i-really-mean-it
check_warns "resync: --bucket-id before subcommand" 2 "" "$WARN_BUCKETID_POS" -- \
  bucket --bucket-id x resync encrypted multipart --bucket cli11-no-such-bucket --yes-i-really-mean-it
check_warns "resync: --marker before subcommand"   2 "" "$WARN_MARKER_POS" -- \
  bucket --marker m resync encrypted multipart --bucket cli11-no-such-bucket --yes-i-really-mean-it
check_warns "resync: --yes-i-really-mean-it before subcommand" 2 "" "$WARN_YIRMI_POS" -- \
  bucket --yes-i-really-mean-it resync encrypted multipart --bucket cli11-no-such-bucket
check_warns "resync: --format before subcommand"   2 "" "$WARN_FORMAT_POS" -- \
  bucket --format json resync encrypted multipart --bucket cli11-no-such-bucket --yes-i-really-mean-it
check_warns "resync: --tenant before subcommand"   22 "ERROR: --tenant is set, but there's no user ID" -- \
  bucket --tenant t resync encrypted multipart --bucket cli11-no-such-bucket --yes-i-really-mean-it

# duplicate-flag warnings
check_warns "resync: duplicate --bucket" 2 "" "$WARN_BUCKET_DUP" -- \
  bucket resync encrypted multipart --bucket a --bucket cli11-no-such-bucket --yes-i-really-mean-it
check_warns "resync: duplicate --marker" 2 "" "$WARN_MARKER_DUP" -- \
  bucket resync encrypted multipart --bucket cli11-no-such-bucket --marker a --marker b --yes-i-really-mean-it
check_warns "resync: duplicate --yes-i-really-mean-it" 2 "" "$WARN_YIRMI_DUP" -- \
  bucket resync encrypted multipart --bucket cli11-no-such-bucket --yes-i-really-mean-it --yes-i-really-mean-it
check_warns "resync: duplicate --tenant" 22 "ERROR: --tenant is set, but there's no user ID" "$WARN_TENANT_DUP" -- \
  bucket resync encrypted multipart --tenant a --tenant b --bucket cli11-no-such-bucket --yes-i-really-mean-it

# multi-warning combinations (2 and 3 warnings)
check_warns "resync: --bucket + --marker before (2 pos warnings)" 2 "" \
  "$WARN_BUCKET_POS" "$WARN_MARKER_POS" -- \
  bucket --bucket cli11-no-such-bucket --marker m resync encrypted multipart --yes-i-really-mean-it
check_warns "resync: --bucket + --marker + --yes-i-really-mean-it before (3 warns)" 2 "" \
  "$WARN_BUCKET_POS" "$WARN_MARKER_POS" "$WARN_YIRMI_POS" -- \
  bucket --bucket cli11-no-such-bucket --marker m --yes-i-really-mean-it resync encrypted multipart

# ============================================================
echo ""
echo "=== bucket radoslist (+ 'bucket rados list' alias) ==="
# ============================================================
# 'bucket radoslist' and 'bucket rados list' are two entry points to the same
# command (shared registration). The radoslist block below is the full coverage;
# the rados-list block after it confirms the alias path is wired identically.

# stray positional args
check "radoslist: stray after leaf (banana)" 1 "ERROR: unexpected argument: 'banana'" \
  bucket radoslist banana
check "radoslist: stray before bucket"       1 "ERROR: unexpected argument: 'foo'" \
  foo bucket radoslist

check "radoslist: unrecognized flag" 22 "ERROR: invalid flag --fakeflag" \
  bucket radoslist --fakeflag
# a value option in SPACE form leaks its value as a stray positional (exit 22)
check_warns "radoslist: unrelated --max-entries 5 swallowed+warned (space form)" 0 "" \
  "Warning: --max-entries is not a valid option for 'bucket radoslist'" -- \
  bucket radoslist --bucket cli11chk --max-entries 5

# missing option value (parse-level, exit 114)
check "radoslist: --bucket missing value"            1 "--bucket: 1 required TEXT missing" \
  bucket radoslist --bucket
check "radoslist: --tenant missing value"            1 "--tenant: 1 required TEXT missing" \
  bucket radoslist --tenant
check "radoslist: --max-concurrent-ios missing value" 1 "--max-concurrent-ios: 1 required INT missing" \
  bucket radoslist --max-concurrent-ios
check "radoslist: --orphan-stale-secs missing value" 1 "--orphan-stale-secs: 1 required UINT missing" \
  bucket radoslist --orphan-stale-secs
check "radoslist: --rgw-obj-fs missing value"        1 "--rgw-obj-fs: 1 required TEXT missing" \
  bucket radoslist --rgw-obj-fs
# --max-concurrent-ios (int) / --orphan-stale-secs (uint) are strict CLI11 numbers;
# a non-numeric value is rejected at parse (exit 104)
check "radoslist: --max-concurrent-ios non-integer" 22 "Could not convert: --max-concurrent-ios = abc" \
  bucket radoslist --max-concurrent-ios abc
check "radoslist: --orphan-stale-secs non-integer"  22 "Could not convert: --orphan-stale-secs = abc" \
  bucket radoslist --orphan-stale-secs abc
# a negative value wraps into the uint64, matching legacy (uint64_t)strict_strtoll
check_cluster "radoslist: --orphan-stale-secs -5 accepted (wraps like legacy)" 0 "" -- \
  bucket radoslist --bucket cli11chk --orphan-stale-secs -5
# strict base-10 on the uint64 branch too: hex rejected, "08" accepted as 8
# (CLI11's default binding would accept 0x10 as 16 and reject 08 as bad octal)
check "radoslist: --orphan-stale-secs hex rejected" 22 "Could not convert: --orphan-stale-secs = 0x10" \
  bucket radoslist --orphan-stale-secs 0x10
check_cluster "radoslist: --orphan-stale-secs 08 accepted as 8" 0 "" -- \
  bucket radoslist --bucket cli11chk --orphan-stale-secs 08
check "radoslist: --orphan-stale-secs out of range" 22 "Could not convert: --orphan-stale-secs = 99999999999999999999" \
  bucket radoslist --orphan-stale-secs 99999999999999999999

# cluster: readonly command, lists rados objects backing the bucket (exit 0).
check_cluster "radoslist: --bucket (lists, exit 0)" 0 "" -- \
  bucket radoslist --bucket cli11chk
# unrelated flags alongside valid args: binary flag (--fix) takes 0 values ->
# accepted; value option in =form binds -> accepted; both still exit 0.
check_cluster "radoslist: unrelated binary flag --fix accepted"        0 "" -- \
  bucket radoslist --bucket cli11chk --fix
check_cluster "radoslist: unrelated value flag --max-entries=5 (=form)" 0 "" -- \
  bucket radoslist --bucket cli11chk --max-entries=5

# wrong-position warnings (flag before the leaf subcommand). Value still flows in
# via the shared option, so radoslist still runs (exit 0). --tenant trips the
# global "no user ID" check (exit 22).
check_warns "radoslist: --bucket before subcommand"   0 "" "$WARN_BUCKET_POS" -- \
  bucket --bucket cli11chk radoslist
check_warns "radoslist: -b before subcommand (short)" 0 "" "$WARN_BUCKET_POS" -- \
  bucket -b cli11chk radoslist
check_warns "radoslist: --max-concurrent-ios before subcommand" 0 "" "$WARN_MAX_IOS_POS" -- \
  bucket --max-concurrent-ios 16 radoslist --bucket cli11chk
check_warns "radoslist: --orphan-stale-secs before subcommand" 0 "" "$WARN_ORPHAN_POS" -- \
  bucket --orphan-stale-secs 100 radoslist --bucket cli11chk
check_warns "radoslist: --rgw-obj-fs before subcommand" 0 "" "$WARN_RGW_OBJ_FS_POS" -- \
  bucket --rgw-obj-fs ":" radoslist --bucket cli11chk
check_warns "radoslist: --yes-i-really-mean-it before subcommand" 0 "" "$WARN_YIRMI_POS" -- \
  bucket --yes-i-really-mean-it radoslist --bucket cli11chk
check_warns "radoslist: --tenant before subcommand" 22 "ERROR: --tenant is set, but there's no user ID" -- \
  bucket --tenant t radoslist --bucket cli11chk

# duplicate-flag warnings (flag specified twice; last value wins)
check_warns "radoslist: duplicate --bucket"             0 "" "$WARN_BUCKET_DUP" -- \
  bucket radoslist --bucket a --bucket cli11chk
check_warns "radoslist: duplicate --max-concurrent-ios" 0 "" "$WARN_MAX_IOS_DUP" -- \
  bucket radoslist --bucket cli11chk --max-concurrent-ios 8 --max-concurrent-ios 16
check_warns "radoslist: duplicate --orphan-stale-secs"  0 "" "$WARN_ORPHAN_DUP" -- \
  bucket radoslist --bucket cli11chk --orphan-stale-secs 1 --orphan-stale-secs 2
check_warns "radoslist: duplicate --rgw-obj-fs"         0 "" "$WARN_RGW_OBJ_FS_DUP" -- \
  bucket radoslist --bucket cli11chk --rgw-obj-fs a --rgw-obj-fs b

# multi-warning combinations (2 and 3 warnings)
check_warns "radoslist: --bucket + --max-concurrent-ios before (2 pos warnings)" 0 "" \
  "$WARN_BUCKET_POS" "$WARN_MAX_IOS_POS" -- \
  bucket --bucket cli11chk --max-concurrent-ios 16 radoslist
check_warns "radoslist: --bucket + --max-concurrent-ios + --orphan-stale-secs before (3 warns)" 0 "" \
  "$WARN_BUCKET_POS" "$WARN_MAX_IOS_POS" "$WARN_ORPHAN_POS" -- \
  bucket --bucket cli11chk --max-concurrent-ios 16 --orphan-stale-secs 100 radoslist

# ---- 'bucket rados list' alias: same command via the rados node ----
# stray + nesting under the 'rados' node
check "rados list: stray after leaf (banana)" 1 "ERROR: unexpected argument: 'banana'" \
  bucket rados list banana
check "rados list: stray between rados and list" 1 "ERROR: unexpected argument: 'extra'" \
  bucket rados extra list
# unknown subcommand under the 'rados' node (require_subcommand -> 106)
check "rados: unknown subcommand (banana)" 1 "A subcommand is required" \
  bucket rados banana
check "rados: no subcommand"               1 "A subcommand is required" \
  bucket rados
# parse error + warn parity with the radoslist entry point
check "rados list: --max-concurrent-ios non-integer" 22 "Could not convert: --max-concurrent-ios = abc" \
  bucket rados list --max-concurrent-ios abc
check_warns "rados list: --bucket before subcommand"  0 "" "$WARN_BUCKET_POS" -- \
  bucket --bucket cli11chk rados list
check_warns "rados list: duplicate --rgw-obj-fs"      0 "" "$WARN_RGW_OBJ_FS_DUP" -- \
  bucket rados list --bucket cli11chk --rgw-obj-fs a --rgw-obj-fs b
check_cluster "rados list: --bucket (lists, exit 0)"  0 "" -- \
  bucket rados list --bucket cli11chk

# ============================================================
echo ""
echo "=== bucket link ==="
# ============================================================

# missing required — legacy: no up-front validation, op layer fails (cluster needed);
# the op checks for an empty user id before fetching bucket info
check_cluster "link: missing --bucket"        22 "$ERR_FETCH_BUCKET" -- \
  bucket link --uid testuser
check_cluster "link: missing --uid"           22 "$ERR_REQUIRES_USER" -- \
  bucket link --bucket mybucket
check_cluster "link: missing both"            22 "$ERR_REQUIRES_USER" -- \
  bucket link

# wrong position + missing required (warnings fire, then op-layer error)
check_warns "link: --bucket before bucket, missing --uid"  22 "$ERR_REQUIRES_USER" "$WARN_BUCKET_POS" -- \
  --bucket mybucket bucket link
check_warns "link: --uid before bucket, missing --bucket"  22 "$ERR_FETCH_BUCKET" -- \
  --uid testuser bucket link
check_warns "link: --bucket + --uid before bucket (1 warn; --uid global, fails on nonexistent)" 2 "" \
  "$WARN_BUCKET_POS" -- \
  --bucket mybucket --uid testuser bucket link

# stray positional args
check "link: stray after flags"              1 "ERROR: unexpected argument: 'strayarg'" \
  bucket link --bucket mybucket --uid testuser strayarg
check "link: stray before bucket"            1 "ERROR: unexpected argument: 'foo'" \
  foo bucket link --bucket mybucket --uid testuser
check "link: stray between bucket and link"  1 "ERROR: unexpected argument: 'extra'" \
  bucket extra link --bucket mybucket --uid testuser

# unrecognized flag
check "link: unrecognized flag" 22 "ERROR: invalid flag --fakeflag" \
  bucket link --bucket mybucket --uid testuser --fakeflag

check "link: --bucket missing value"          1 "--bucket: 1 required TEXT missing" \
  bucket link --bucket
check "link: --uid missing value"             1 "--uid: 1 required TEXT missing" \
  bucket link --uid
check "link: --bucket-id missing value"       1 "--bucket-id: 1 required TEXT missing" \
  bucket link --bucket-id
check "link: --bucket-new-name missing value" 1 "--bucket-new-name: 1 required TEXT missing" \
  bucket link --bucket-new-name

# ============================================================
echo ""
echo "=== bucket unlink ==="
# ============================================================

# missing required — legacy: no up-front validation, op layer fails with a
# bare EINVAL (cluster needed)
check_cluster "unlink: missing --bucket"    22 "$ERR_EINVAL" -- \
  bucket unlink --uid testuser
check_cluster "unlink: missing --uid"       22 "$ERR_EINVAL" -- \
  bucket unlink --bucket mybucket
check_cluster "unlink: missing both"        22 "$ERR_EINVAL" -- \
  bucket unlink

check_warns "unlink: --bucket before bucket, missing --uid"   22 "$ERR_EINVAL" "$WARN_BUCKET_POS" -- \
  --bucket mybucket bucket unlink
check_warns "unlink: --uid before bucket, missing --bucket"   22 "$ERR_EINVAL" -- \
  --uid testuser bucket unlink

check "unlink: stray after flags"              1 "ERROR: unexpected argument: 'strayarg'" \
  bucket unlink --bucket mybucket --uid testuser strayarg
check "unlink: stray before bucket"            1 "ERROR: unexpected argument: 'foo'" \
  foo bucket unlink --bucket mybucket --uid testuser
check "unlink: stray between bucket and unlink" 1 "ERROR: unexpected argument: 'extra'" \
  bucket extra unlink --bucket mybucket --uid testuser

check "unlink: unrecognized flag" 22 "ERROR: invalid flag --fakeflag" \
  bucket unlink --bucket mybucket --uid testuser --fakeflag

check "unlink: --bucket missing value" 1 "--bucket: 1 required TEXT missing" \
  bucket unlink --bucket
check "unlink: --uid missing value"    1 "--uid: 1 required TEXT missing" \
  bucket unlink --uid

# ============================================================
echo ""
echo "=== bucket rm ==="
# ============================================================

# legacy: rm ignores the op's return value, so missing --bucket silently
# exits 0 (see TODO in radosgw-admin.cc); cluster needed to reach the op
check_cluster "rm: missing --bucket (silent exit 0)" 0 "" -- \
  bucket rm
check_warns "rm: --purge-objects before bucket, missing --bucket (warns, silent exit 0)" 0 "" \
  "$WARN_PURGE_POS" -- \
  --purge-objects bucket rm

check "rm: stray after flags"              1 "ERROR: unexpected argument: 'strayarg'" \
  bucket rm --bucket mybucket strayarg
check "rm: stray before bucket"            1 "ERROR: unexpected argument: 'foo'" \
  foo bucket rm --bucket mybucket
check "rm: stray between bucket and rm"    1 "ERROR: unexpected argument: 'extra'" \
  bucket extra rm --bucket mybucket

check "rm: unrecognized flag" 22 "ERROR: invalid flag --fakeflag" \
  bucket rm --bucket mybucket --fakeflag

check "rm: --bucket missing value" 1 "--bucket: 1 required TEXT missing" \
  bucket rm --bucket

# --inconsistent-index without --yes-i-really-mean-it is caught in cli11_action (needs cluster)
check_cluster "rm: --inconsistent-index without --yes-i-really-mean-it" 1 "$ERR_INCONSISTENT" -- \
  bucket rm --bucket nonexistent_cli11_test --inconsistent-index

# ============================================================
echo ""
echo "=== bucket rm (remove alias) ==="
# ============================================================

check_cluster "remove: missing --bucket (silent exit 0)" 0 "" -- \
  bucket remove
check "remove: stray after flags"   1 "ERROR: unexpected argument: 'strayarg'" \
  bucket remove --bucket mybucket strayarg
check "remove: stray before bucket"             1 "ERROR: unexpected argument: 'foo'" \
  foo bucket remove --bucket mybucket
check "remove: stray between bucket and remove" 1 "ERROR: unexpected argument: 'extra'" \
  bucket extra remove --bucket mybucket
check "remove: unrecognized flag"   22 "ERROR: invalid flag --fakeflag" \
  bucket remove --bucket mybucket --fakeflag
check "remove: --bucket missing value" 1 "--bucket: 1 required TEXT missing" \
  bucket remove --bucket

# ============================================================
echo ""
echo "=== bucket check ==="
# ============================================================

check "check: stray after flags"              1 "ERROR: unexpected argument: 'strayarg'" \
  bucket check strayarg
check "check: stray before bucket"            1 "ERROR: unexpected argument: 'foo'" \
  foo bucket check
check "check: stray between bucket and check" 1 "ERROR: unexpected argument: 'extra'" \
  bucket extra check

check "check: unrecognized flag" 22 "ERROR: invalid flag --fakeflag" \
  bucket check --fakeflag

check "check: --bucket missing value" 1 "--bucket: 1 required TEXT missing" \
  bucket check --bucket
check "check: --max-concurrent-ios missing value" 1 "--max-concurrent-ios: 1 required INT missing" \
  bucket check --max-concurrent-ios

# --check-head-obj-locator without --bucket is caught in cli11_action (needs cluster)
check_cluster "check: --check-head-obj-locator without --bucket" 22 "ERROR: need to specify bucket name" -- \
  bucket check --check-head-obj-locator

# ============================================================
echo ""
echo "=== bucket check olh ==="
# ============================================================

check "check olh: stray after flags"               1 "ERROR: unexpected argument: 'strayarg'" \
  bucket check olh strayarg
check "check olh: stray before bucket"             1 "ERROR: unexpected argument: 'foo'" \
  foo bucket check olh
check "check olh: stray between bucket and check"  1 "ERROR: unexpected argument: 'extra'" \
  bucket extra check olh
check "check olh: stray between check and olh"     1 "ERROR: unexpected argument: 'extra'" \
  bucket check extra olh

check "check olh: unrecognized flag" 22 "ERROR: invalid flag --fakeflag" \
  bucket check olh --fakeflag
check "check olh: --max-concurrent-ios missing value" 1 "--max-concurrent-ios: 1 required INT missing" \
  bucket check olh --max-concurrent-ios
check "check olh: --bucket missing value"             1 "--bucket: 1 required TEXT missing" \
  bucket check olh --bucket

# ============================================================
echo ""
echo "=== bucket check unlinked ==="
# ============================================================

check "check unlinked: stray after flags"                1 "ERROR: unexpected argument: 'strayarg'" \
  bucket check unlinked strayarg
check "check unlinked: stray before bucket"              1 "ERROR: unexpected argument: 'foo'" \
  foo bucket check unlinked
check "check unlinked: stray between bucket and check"   1 "ERROR: unexpected argument: 'extra'" \
  bucket extra check unlinked
check "check unlinked: stray between check and unlinked" 1 "ERROR: unexpected argument: 'extra'" \
  bucket check extra unlinked

check "check unlinked: unrecognized flag" 22 "ERROR: invalid flag --fakeflag" \
  bucket check unlinked --fakeflag
check "check unlinked: --max-concurrent-ios missing value" 1 "--max-concurrent-ios: 1 required INT missing" \
  bucket check unlinked --max-concurrent-ios
check "check unlinked: --bucket missing value"             1 "--bucket: 1 required TEXT missing" \
  bucket check unlinked --bucket

# ============================================================
echo ""
echo "=== bucket list: wrong-position warnings (cluster) ==="
# ============================================================

# flag before bucket
# --bucket with nonexistent name: warning fires, then init_bucket fails (exit 2)
check_warns "list: --bucket/-b before bucket"    2 "ERROR: could not init bucket" "$WARN_BUCKET_POS" -- \
  --bucket nonexistent_cli11_test bucket list
check_warns "list: -b (short) before bucket"     2 "ERROR: could not init bucket" "$WARN_BUCKET_POS" -- \
  -b nonexistent_cli11_test bucket list
# --tenant without --uid: warning fires, then legacy check rejects it (exit 22)
check_warns "list: --tenant before bucket"       22 "ERROR: --tenant is set, but there's no user ID" -- \
  --tenant mytenant bucket list
# flags that don't affect success: warning fires, command succeeds (exit 0)
check_warns "list: --format before bucket"       0 "" "$WARN_FORMAT_POS" -- \
  --format json bucket list
check_warns "list: --max-entries before bucket"  0 "" "$WARN_MAXENT_POS" -- \
  --max-entries 10 bucket list
check_warns "list: --marker before bucket"       0 "" "$WARN_MARKER_POS" -- \
  --marker somemarker bucket list

# flag between bucket and list
check_warns "list: --bucket between bucket and list"   2 "ERROR: could not init bucket" "$WARN_BUCKET_POS" -- \
  bucket --bucket nonexistent_cli11_test list
check_warns "list: --tenant between bucket and list"   22 "ERROR: --tenant is set, but there's no user ID" -- \
  bucket --tenant mytenant list
check_warns "list: --format between bucket and list"   0 "" "$WARN_FORMAT_POS" -- \
  bucket --format json list
check_warns "list: --max-entries between bucket and list" 0 "" "$WARN_MAXENT_POS" -- \
  bucket --max-entries 10 list
check_warns "list: --marker between bucket and list"   0 "" "$WARN_MARKER_POS" -- \
  bucket --marker somemarker list

# duplicate flags (same level)
check_warns "list: duplicate --bucket same level"  2 "ERROR: could not init bucket" "$WARN_BUCKET_DUP" -- \
  bucket list --bucket nonexistent1_cli11_test --bucket nonexistent2_cli11_test
check_warns "list: duplicate --tenant same level"  22 "ERROR: --tenant is set, but there's no user ID" "$WARN_TENANT_DUP" -- \
  bucket list --tenant foo --tenant bar
check_warns "list: duplicate --format same level"  0 "" "$WARN_FORMAT_DUP" -- \
  bucket list --format json --format xml
# --uid is registered on bucket list (to filter by owner)
# legacy returns -ENOENT from main for an unknown user, so the exit code is 254
check_warns "list: --uid before bucket"           254 "ERROR: could not find user" -- \
  --uid testuser_cli11_test bucket list
check_warns "list: --bucket-id before bucket"     0 "" "$WARN_BUCKETID_POS" -- \
  --bucket-id nonexistent_id_cli11_test bucket list
check_warns "list: --object-version before bucket" 0 "" "$WARN_OBJVER_POS" -- \
  --object-version somever bucket list
check_warns "list: --allow-unordered before bucket" 0 "" "$WARN_ALLOW_UNORDERED_POS" -- \
  --allow-unordered bucket list

# duplicate cross level (position + duplicate)
check_warns "list: duplicate --bucket cross level"  2 "ERROR: could not init bucket" "$WARN_BUCKET_POS" "$WARN_BUCKET_DUP" -- \
  --bucket nonexistent1_cli11_test bucket list --bucket nonexistent2_cli11_test
check_warns "list: duplicate --tenant cross level"  22 "ERROR: --tenant is set, but there's no user ID" "$WARN_TENANT_DUP" -- \
  --tenant foo bucket list --tenant bar

# ============================================================
echo ""
echo "=== bucket stats: wrong-position warnings (cluster) ==="
# ============================================================

check_warns "stats: --bucket before bucket"            2 "" "$WARN_BUCKET_POS" -- \
  --bucket nonexistent_cli11_test bucket stats
check_warns "stats: --tenant before bucket"            22 "ERROR: --tenant is set, but there's no user ID" -- \
  --tenant mytenant bucket stats
check_warns "stats: --bucket between bucket and stats" 2 "" "$WARN_BUCKET_POS" -- \
  bucket --bucket nonexistent_cli11_test stats
check_warns "stats: duplicate --bucket"                2 "" "$WARN_BUCKET_DUP" -- \
  bucket stats --bucket nonexistent1_cli11_test --bucket nonexistent2_cli11_test

# stats-specific flags in wrong position
check_warns "stats: --show-restore-stats before bucket"        0 "" "$WARN_SHOWRESTORE_POS" -- \
  --show-restore-stats bucket stats
check_warns "stats: --show-restore-stats between bucket/stats" 0 "" "$WARN_SHOWRESTORE_POS" -- \
  bucket --show-restore-stats stats
check_warns "stats: duplicate --show-restore-stats"            0 "" "$WARN_SHOWRESTORE_DUP" -- \
  bucket stats --show-restore-stats --show-restore-stats
# legacy returns -ENOENT from main for an unknown bucket id, so the exit code is 254
check_warns "stats: --bucket-id before bucket"                 254 "failure: no such bucket id" "$WARN_BUCKETID_POS" -- \
  --bucket-id nonexistent_id_cli11_test bucket stats
check_warns "stats: duplicate --bucket-id"                     254 "failure: no such bucket id" "$WARN_BUCKETID_DUP" -- \
  bucket stats --bucket-id id1_cli11_test --bucket-id id2_cli11_test
check_warns "stats: --max-entries before bucket"               0 "" "$WARN_MAXENT_POS" -- \
  --max-entries 10 bucket stats
check_warns "stats: --marker before bucket"                    0 "" "$WARN_MARKER_POS" -- \
  --marker foo bucket stats
check_warns "stats: --format before bucket"                    0 "" "$WARN_FORMAT_POS" -- \
  --format json bucket stats
check_warns "stats: --format between bucket and stats"         0 "" "$WARN_FORMAT_POS" -- \
  bucket --format json stats
check_warns "stats: duplicate --format"                        0 "" "$WARN_FORMAT_DUP" -- \
  bucket stats --format json --format xml

# stats multi-warning combinations
check_warns "stats: --show-restore-stats + --tenant before (1 warn; --tenant global, no warn)" 22 \
  "ERROR: --tenant is set, but there's no user ID" \
  "$WARN_SHOWRESTORE_POS" -- \
  --show-restore-stats --tenant foo bucket stats
check_warns "stats: --bucket + --show-restore-stats before (2 warns)" 2 "" \
  "$WARN_BUCKET_POS" "$WARN_SHOWRESTORE_POS" -- \
  --bucket nonexistent_cli11_test --show-restore-stats bucket stats

# ============================================================
echo ""
echo "=== bucket link: wrong-position warnings (cluster) ==="
# ============================================================

# With correct flags but wrong position, these commands will fail (no such bucket/user),
# but warnings appear before the action runs.
check_warns "link: --bucket before bucket (warns, then fails)"  2 "" "$WARN_BUCKET_POS" -- \
  --bucket nonexistent_cli11_test bucket link --uid testuser_cli11_test
check_warns "link: --uid before bucket (warns, then fails)"     2 "" -- \
  --uid testuser_cli11_test bucket link --bucket nonexistent_cli11_test
check_warns "link: duplicate --bucket"  2 "" "$WARN_BUCKET_DUP" -- \
  bucket link --bucket foo --bucket nonexistent_cli11_test --uid testuser_cli11_test
check_warns "link: duplicate --uid"     2 "" "$WARN_UID_DUP" -- \
  bucket link --uid foo --uid testuser_cli11_test --bucket nonexistent_cli11_test

# link-specific flags in wrong position
check_warns "link: --bucket-new-name before bucket"  2 "" "$WARN_NEWNAME_POS" -- \
  --bucket-new-name newname bucket link --bucket nonexistent_cli11_test --uid testuser_cli11_test
check_warns "link: --bucket-id before bucket"        2 "" "$WARN_BUCKETID_POS" -- \
  --bucket-id someid_cli11_test bucket link --bucket nonexistent_cli11_test --uid testuser_cli11_test
check_warns "link: --tenant before bucket"           2 "" -- \
  --tenant foo bucket link --bucket nonexistent_cli11_test --uid testuser_cli11_test
check_warns "link: --bucket + --uid + --tenant before (1 pos warning; --tenant and --uid global, no warn)" 2 "" \
  "$WARN_BUCKET_POS" -- \
  --bucket nonexistent_cli11_test --uid testuser_cli11_test --tenant foo bucket link

# ============================================================
echo ""
echo "=== bucket unlink: wrong-position warnings (cluster) ==="
# ============================================================

check_warns "unlink: --bucket before bucket (warns, then fails)"  2 "" "$WARN_BUCKET_POS" -- \
  --bucket nonexistent_cli11_test bucket unlink --uid testuser_cli11_test
check_warns "unlink: --uid before bucket (warns, then fails)"     2 "" -- \
  --uid testuser_cli11_test bucket unlink --bucket nonexistent_cli11_test
check_warns "unlink: duplicate --bucket"  2 "" "$WARN_BUCKET_DUP" -- \
  bucket unlink --bucket foo --bucket nonexistent_cli11_test --uid testuser_cli11_test
check_warns "unlink: --tenant before bucket"  2 "" -- \
  --tenant foo bucket unlink --bucket nonexistent_cli11_test --uid testuser_cli11_test
check_warns "unlink: duplicate --uid"         2 "" "$WARN_UID_DUP" -- \
  bucket unlink --uid foo --uid testuser_cli11_test --bucket nonexistent_cli11_test
check_warns "unlink: --bucket + --uid before (1 pos warning; --uid global, no warn)" 2 "" \
  "$WARN_BUCKET_POS" -- \
  --bucket nonexistent_cli11_test --uid testuser_cli11_test bucket unlink

# ============================================================
echo ""
echo "=== bucket rm: wrong-position warnings (cluster) ==="
# ============================================================

check_warns "rm: --bucket before bucket (warns, fails on nonexistent)"  0 "" "$WARN_BUCKET_POS" -- \
  --bucket nonexistent_cli11_test bucket rm
check_warns "rm: duplicate --bucket"  0 "" "$WARN_BUCKET_DUP" -- \
  bucket rm --bucket foo --bucket nonexistent_cli11_test
check_warns "rm: duplicate --tenant"  22 "ERROR: --tenant is set, but there's no user ID" "$WARN_TENANT_DUP" -- \
  bucket rm --bucket nonexistent_cli11_test --tenant foo --tenant bar

# rm-specific flags in wrong position
check_warns "rm: --purge-objects before bucket"              0 "" "$WARN_PURGE_POS" -- \
  --purge-objects bucket rm --bucket nonexistent_cli11_test
check_warns "rm: --bypass-gc before bucket"                  0 "" "$WARN_BYPASS_POS" -- \
  --bypass-gc bucket rm --bucket nonexistent_cli11_test
check_warns "rm: --inconsistent-index before bucket"         1 "$ERR_INCONSISTENT" "$WARN_INCONSISTENT_POS" -- \
  --inconsistent-index bucket rm --bucket nonexistent_cli11_test
check_warns "rm: --yes-i-really-mean-it + --inconsistent-index before" 0 "" \
  "$WARN_YIRMI_POS" "$WARN_INCONSISTENT_POS" -- \
  --yes-i-really-mean-it --inconsistent-index bucket rm --bucket nonexistent_cli11_test
check_warns "rm: --tenant before bucket"                     22 "ERROR: --tenant is set, but there's no user ID" -- \
  --tenant foo bucket rm --bucket nonexistent_cli11_test
check_warns "rm: --purge-objects between bucket and rm"      0 "" "$WARN_PURGE_POS" -- \
  bucket --purge-objects rm --bucket nonexistent_cli11_test
check_warns "rm: duplicate --purge-objects"                  0 "" "$WARN_PURGE_DUP" -- \
  bucket rm --bucket nonexistent_cli11_test --purge-objects --purge-objects

# rm multi-warning combinations
check_warns "rm: --purge-objects + --bypass-gc before (2 pos warnings)" 0 "" \
  "$WARN_PURGE_POS" "$WARN_BYPASS_POS" -- \
  --purge-objects --bypass-gc bucket rm --bucket nonexistent_cli11_test
check_warns "rm: --bucket + --purge-objects before (2 pos warnings)"    0 "" \
  "$WARN_BUCKET_POS" "$WARN_PURGE_POS" -- \
  --bucket nonexistent_cli11_test --purge-objects bucket rm
check_warns "rm: --bucket + --tenant + --purge-objects before (2 warns + exit 22; --tenant global, no warn)" 22 \
  "ERROR: --tenant is set, but there's no user ID" \
  "$WARN_BUCKET_POS" "$WARN_PURGE_POS" -- \
  --bucket nonexistent_cli11_test --tenant foo --purge-objects bucket rm
check_warns "rm: 4 pos warnings + inconsistent error" 1 "$ERR_INCONSISTENT" \
  "$WARN_BUCKET_POS" "$WARN_PURGE_POS" "$WARN_BYPASS_POS" "$WARN_INCONSISTENT_POS" -- \
  --bucket nonexistent_cli11_test --purge-objects --bypass-gc --inconsistent-index bucket rm
check_warns "rm: pos + duplicate + tenant (2 warns; --tenant global, no warn)" 22 \
  "ERROR: --tenant is set, but there's no user ID" \
  "$WARN_BUCKET_POS" "$WARN_BUCKET_DUP" -- \
  --bucket foo --tenant bar bucket rm --bucket nonexistent_cli11_test

# ============================================================
echo ""
echo "=== bucket check: wrong-position warnings (cluster) ==="
# ============================================================

check_warns "check: --bucket before bucket"            0 "" "$WARN_BUCKET_POS" -- \
  --bucket nonexistent_cli11_test bucket check
check_warns "check: --fix before bucket"               0 "" "$WARN_FIX_POS" -- \
  --fix bucket check
check_warns "check: --bucket between bucket and check" 0 "" "$WARN_BUCKET_POS" -- \
  bucket --bucket nonexistent_cli11_test check
check_warns "check: --fix between bucket and check"    0 "" "$WARN_FIX_POS" -- \
  bucket --fix check
check_warns "check: duplicate --bucket"                0 "" "$WARN_BUCKET_DUP" -- \
  bucket check --bucket nonexistent1_cli11_test --bucket nonexistent2_cli11_test
check_warns "check: duplicate --tenant"                22 "ERROR: --tenant is set, but there's no user ID" "$WARN_TENANT_DUP" -- \
  bucket check --tenant foo --tenant bar
check_warns "check: duplicate --bucket cross level"    0 "" "$WARN_BUCKET_POS" "$WARN_BUCKET_DUP" -- \
  --bucket nonexistent1_cli11_test bucket check --bucket nonexistent2_cli11_test

# check-specific flags in wrong position
check_warns "check: --remove-bad before bucket"             0 "" "$WARN_REMOVE_BAD_POS" -- \
  --remove-bad bucket check
check_warns "check: --remove-bad between bucket and check"  0 "" "$WARN_REMOVE_BAD_POS" -- \
  bucket --remove-bad check
check_warns "check: --check-head-obj-locator before bucket" 22 "ERROR: need to specify bucket name" "$WARN_CHECKHEAD_POS" -- \
  --check-head-obj-locator bucket check
check_warns "check: --tenant before bucket"                 22 "ERROR: --tenant is set, but there's no user ID" -- \
  --tenant foo bucket check
check_warns "check: duplicate --fix"                        0 "" "$WARN_FIX_DUP" -- \
  bucket check --fix --fix
# take_last: with explicit values the last one wins (and still warns)
check_warns "check: duplicate --fix value (last value wins)" 0 "" "$WARN_FIX_DUP" -- \
  bucket check --fix=true --fix=false

# binary-flag backward-compat (--fix pilot, add_multilevel_binary_flag).
# All forms resolve to a bool and exit 0; with no --bucket the check is a silent
# no-op, so these assert PARSE-level handling (value consumed vs left as a stray,
# and the warn-and-accept warning). A real stray like "zzz" gives exit 22
# "unexpected argument", so exit 0 here means the token was consumed by --fix.
check "check: --fix (bare)"                                 0 "" \
  bucket check --fix
check "check: --fix=true"                                   0 "" \
  bucket check --fix=true
check "check: --fix true (space, bool consumed)"            0 "" \
  bucket check --fix true
check "check: --fix=false"                                  0 "" \
  bucket check --fix=false
check "check: --fix false (space, bool consumed)"           0 "" \
  bucket check --fix false
# invalid value: warn-and-accept -> treated as set, exit 0 (legacy stored a
# truthy -EINVAL silently; we keep the truthy result and add a warning).
check "check: --fix=banana (warn-and-accept)"               0 "Warning: invalid value 'banana' for --fix, treating as set" \
  bucket check --fix=banana
# "--fix banana" (space + non-bool) DIVERGES from legacy, which left banana as a
# stray (exit 1); expected(0,1) consumes it, so it matches the "=banana" form.
check "check: --fix banana (space non-bool, warn-accept)"   0 "Warning: invalid value 'banana' for --fix, treating as set" \
  bucket check --fix banana
# parse-safety on an UNMIGRATED command: must not throw (old add_flag(int) gave
# exit 104). Legacy ignores --fix here; we stay exit 0. (A spurious warning is
# printed for an invalid value only -- known minor divergence, exit code right.)
check "gc list --fix=banana (unmigrated stays parse-safe)"  0 "" \
  gc list --fix=banana

# Binary-flag rollout: one "=banana" per migrated flag confirms it uses
# add_multilevel_binary_flag (warn-and-accept, correct flag name) and is treated
# as SET. The exit matches legacy's truthy -EINVAL outcome (verified bare ==
# =true == =banana for each flag); the warning is the one intended addition.
check "list: --allow-unordered=banana"          0 "Warning: invalid value 'banana' for --allow-unordered, treating as set" \
  bucket list --allow-unordered=banana
check "stats: --show-restore-stats=banana"      0 "Warning: invalid value 'banana' for --show-restore-stats, treating as set" \
  bucket stats --show-restore-stats=banana
check "check: --remove-bad=banana"              0 "Warning: invalid value 'banana' for --remove-bad, treating as set" \
  bucket check --remove-bad=banana
check "check: --check-objects=banana"           0 "Warning: invalid value 'banana' for --check-objects, treating as set" \
  bucket check --check-objects=banana
# set -> locator path needs a bucket name -> exit 22 (same as bare/=true)
check "check: --check-head-obj-locator=banana"  22 "Warning: invalid value 'banana' for --check-head-obj-locator, treating as set" \
  bucket check --check-head-obj-locator=banana
check "rm: --purge-objects=banana"              0 "Warning: invalid value 'banana' for --purge-objects, treating as set" \
  bucket rm --purge-objects=banana
check "rm: --bypass-gc=banana"                  0 "Warning: invalid value 'banana' for --bypass-gc, treating as set" \
  bucket rm --bypass-gc=banana
# set -> corrupt-index guard fires (requires --yes-i-really-mean-it) -> exit 1
check "rm: --inconsistent-index=banana"         1 "Warning: invalid value 'banana' for --inconsistent-index, treating as set" \
  bucket rm --inconsistent-index=banana
check "rm: --yes-i-really-mean-it=banana"       0 "Warning: invalid value 'banana' for --yes-i-really-mean-it, treating as set" \
  bucket rm --yes-i-really-mean-it=banana

# check multi-warning combinations
check_warns "check: --fix + --remove-bad before (2 pos warnings)"       0 "" \
  "$WARN_FIX_POS" "$WARN_REMOVE_BAD_POS" -- \
  --fix --remove-bad bucket check
check_warns "check: --fix + --remove-bad + --tenant before (2 warnings; --tenant global, no warn)" 22 \
  "ERROR: --tenant is set, but there's no user ID" \
  "$WARN_FIX_POS" "$WARN_REMOVE_BAD_POS" -- \
  --fix --remove-bad --tenant foo bucket check
check_warns "check: pos + duplicate --bucket (2 warns)"                  0 "" \
  "$WARN_BUCKET_POS" "$WARN_BUCKET_DUP" -- \
  --bucket nonexistent1_cli11_test bucket check --bucket nonexistent2_cli11_test

# new check flags in wrong position (variable not set at callback time, cli11_action runs normally)
check_warns "check: --check-objects before bucket"       0 "" "$WARN_CHECK_OBJECTS_POS" -- \
  --check-objects bucket check
check_warns "check: --max-concurrent-ios before bucket"  0 "" "$WARN_MAX_IOS_POS" -- \
  --max-concurrent-ios 5 bucket check
check_warns "check: duplicate --max-concurrent-ios"      0 "" "$WARN_MAX_IOS_DUP" -- \
  bucket check --max-concurrent-ios 5 --max-concurrent-ios 10

# ============================================================
echo ""
echo "=== bucket check olh/unlinked: wrong-position warnings (cluster) ==="
# ============================================================

# --bucket in wrong position for olh/unlinked means it appears before "bucket" or between "bucket" and "check"
# (between "check" and "olh/unlinked" is the correct place for bucket_check's --bucket)
check_warns "check olh: --bucket before bucket"             0 "" "$WARN_BUCKET_POS" -- \
  --bucket nonexistent_cli11_test bucket check olh
check_warns "check olh: --bucket between bucket and check"  0 "" "$WARN_BUCKET_POS" -- \
  bucket --bucket nonexistent_cli11_test check olh
check_warns "check olh: --tenant before bucket"             22 "ERROR: --tenant is set, but there's no user ID" -- \
  --tenant foo bucket check olh
check_warns "check olh: --bucket + --tenant before (1 warn; --tenant global, no warn)" 22 "ERROR: --tenant is set, but there's no user ID" \
  "$WARN_BUCKET_POS" -- \
  --bucket nonexistent_cli11_test --tenant foo bucket check olh

# olh-specific new flags in wrong position
check_warns "check olh: --fix before bucket"             0 "" "$WARN_FIX_POS" -- \
  --fix bucket check olh
check_warns "check olh: --dump-keys before bucket"       0 "" "$WARN_DUMP_KEYS_POS" -- \
  --dump-keys bucket check olh
check_warns "check olh: --hide-progress before bucket"   0 "" "$WARN_HIDE_PROGRESS_POS" -- \
  --hide-progress bucket check olh
check_warns "check olh: --max-concurrent-ios before bucket" 0 "" "$WARN_MAX_IOS_POS" -- \
  --max-concurrent-ios 5 bucket check olh
check_warns "check olh: --dump-keys + --hide-progress before (2 warns)" 0 "" \
  "$WARN_DUMP_KEYS_POS" "$WARN_HIDE_PROGRESS_POS" -- \
  --dump-keys --hide-progress bucket check olh

# Multilevel registration: these flags are accepted at every level, not just at
# root. Away from its canonical command a flag gets a position warning but still
# works (exit 0). --fix is a "bucket check" flag, so "after check" is canonical
# (no position warning); --dump-keys/--hide-progress belong to olh, so "after
# check" still warns. --fix represents the 10 binary flags (same helper).
check_warns "check olh: --fix after bucket"             0 "" "$WARN_FIX_POS" -- \
  bucket --fix check olh
check_cluster "check olh: --fix after check (canonical, accepted)" 0 "" -- \
  bucket check --fix olh
check_warns "check olh: --dump-keys after bucket"       0 "" "$WARN_DUMP_KEYS_POS" -- \
  bucket --dump-keys check olh
check_warns "check olh: --dump-keys after check"        0 "" "$WARN_DUMP_KEYS_POS" -- \
  bucket check --dump-keys olh
check_warns "check olh: --hide-progress after bucket"   0 "" "$WARN_HIDE_PROGRESS_POS" -- \
  bucket --hide-progress check olh
check_warns "check olh: --hide-progress after check"    0 "" "$WARN_HIDE_PROGRESS_POS" -- \
  bucket check --hide-progress olh
# duplicate -> take_last keeps the last value, with a dup warning
check_warns "check olh: duplicate --dump-keys (last value wins)"     0 "" "$WARN_DUMP_KEYS_DUP" -- \
  bucket check olh --dump-keys=true --dump-keys=false
check_warns "check olh: duplicate --hide-progress (last value wins)" 0 "" "$WARN_HIDE_PROGRESS_DUP" -- \
  bucket check olh --hide-progress=true --hide-progress=false
# dup and position warnings are independent: --fix duplicated at its canonical
# level warns about the duplicate but not about position
check_warns "check olh: duplicate --fix at canonical level (dup only)" 0 "" "$WARN_FIX_DUP" -- \
  bucket check --fix=true --fix=false olh

check_warns "check unlinked: --bucket before bucket"             0 "" "$WARN_BUCKET_POS" -- \
  --bucket nonexistent_cli11_test bucket check unlinked
check_warns "check unlinked: --bucket between bucket and check"  0 "" "$WARN_BUCKET_POS" -- \
  bucket --bucket nonexistent_cli11_test check unlinked
check_warns "check unlinked: --tenant before bucket"             22 "ERROR: --tenant is set, but there's no user ID" -- \
  --tenant foo bucket check unlinked

# unlinked-specific new flags in wrong position
check_warns "check unlinked: --fix before bucket"             0 "" "$WARN_FIX_POS" -- \
  --fix bucket check unlinked
check_warns "check unlinked: --dump-keys before bucket"       0 "" "$WARN_DUMP_KEYS_POS" -- \
  --dump-keys bucket check unlinked
check_warns "check unlinked: --hide-progress before bucket"   0 "" "$WARN_HIDE_PROGRESS_POS" -- \
  --hide-progress bucket check unlinked
check_warns "check unlinked: --max-concurrent-ios before bucket" 0 "" "$WARN_MAX_IOS_POS" -- \
  --max-concurrent-ios 5 bucket check unlinked

# ============================================================
echo ""
echo "=== bucket remove alias: wrong-position warnings (cluster) ==="
# ============================================================

check_warns "remove: --bucket before bucket"           0 "" "$WARN_BUCKET_POS" -- \
  --bucket nonexistent_cli11_test bucket remove
check_warns "remove: --purge-objects before bucket"    0 "" "$WARN_PURGE_POS" -- \
  --purge-objects bucket remove --bucket nonexistent_cli11_test
check_warns "remove: --tenant before bucket"           22 "ERROR: --tenant is set, but there's no user ID" -- \
  --tenant foo bucket remove --bucket nonexistent_cli11_test
check_warns "remove: duplicate --bucket"               0 "" "$WARN_BUCKET_DUP" -- \
  bucket remove --bucket foo --bucket nonexistent_cli11_test
check_warns "remove: --bucket + --purge-objects before (2 pos warnings)" 0 "" \
  "$WARN_BUCKET_POS" "$WARN_PURGE_POS" -- \
  --bucket nonexistent_cli11_test --purge-objects bucket remove
check_warns "remove: --inconsistent-index before (without yes)" 1 "$ERR_INCONSISTENT" "$WARN_INCONSISTENT_POS" -- \
  --inconsistent-index bucket remove --bucket nonexistent_cli11_test

# ============================================================
echo ""
echo "=== short flags in correct position (cluster) ==="
# ============================================================

# -b accepted as --bucket, -i accepted as --uid in correct position
check_cluster "list: -b correct position (nonexistent)" 2 "ERROR: could not init bucket" -- \
  bucket list -b nonexistent_cli11_test
check_cluster "link: -b and -i correct position (nonexistent)" 2 "" -- \
  bucket link -b nonexistent_cli11_test -i nonexistent_user_cli11_test
check_cluster "unlink: -b and -i correct position (nonexistent)" 2 "" -- \
  bucket unlink -b nonexistent_cli11_test -i nonexistent_user_cli11_test
check_cluster "rm: -b correct position (nonexistent)" 0 "" -- \
  bucket rm -b nonexistent_cli11_test
check_cluster "stats: -b correct position (nonexistent)" 2 "" -- \
  bucket stats -b nonexistent_cli11_test
check_cluster "check: -b correct position" 0 "" -- \
  bucket check -b nonexistent_cli11_test

# ============================================================
echo ""
echo "=== --cli11-help positions ==="
# ============================================================

# root
check_help "cli11-help root"           --cli11-help

# bucket
check_help "cli11-help before bucket"  --cli11-help bucket
check_help "cli11-help after bucket"   bucket --cli11-help

# bucket list
check_help "cli11-help bucket list"         --cli11-help bucket list
check_help "cli11-help list after bucket"   bucket --cli11-help list
check_help "cli11-help after list"          bucket list --cli11-help

# bucket stats
check_help "cli11-help bucket stats"        --cli11-help bucket stats
check_help "cli11-help stats after bucket"  bucket --cli11-help stats
check_help "cli11-help after stats"         bucket stats --cli11-help

# bucket link
check_help "cli11-help bucket link"         --cli11-help bucket link
check_help "cli11-help link after bucket"   bucket --cli11-help link
check_help "cli11-help after link"          bucket link --cli11-help

# bucket unlink
check_help "cli11-help bucket unlink"       --cli11-help bucket unlink
check_help "cli11-help unlink after bucket" bucket --cli11-help unlink
check_help "cli11-help after unlink"        bucket unlink --cli11-help

# bucket rm
check_help "cli11-help bucket rm"           --cli11-help bucket rm
check_help "cli11-help rm after bucket"     bucket --cli11-help rm
check_help "cli11-help after rm"            bucket rm --cli11-help

# bucket check
check_help "cli11-help bucket check"        --cli11-help bucket check
check_help "cli11-help check after bucket"  bucket --cli11-help check
check_help "cli11-help after check"         bucket check --cli11-help

# bucket check olh
check_help "cli11-help bucket check olh"         --cli11-help bucket check olh
check_help "cli11-help olh after check"          bucket check --cli11-help olh
check_help "cli11-help after olh"                bucket check olh --cli11-help

# bucket check unlinked
check_help "cli11-help bucket check unlinked"    --cli11-help bucket check unlinked
check_help "cli11-help unlinked after check"     bucket check --cli11-help unlinked
check_help "cli11-help after unlinked"           bucket check unlinked --cli11-help

# bucket remove alias (same app as rm, same help)
check_help "cli11-help bucket remove"            --cli11-help bucket remove
check_help "cli11-help remove after bucket"      bucket --cli11-help remove
check_help "cli11-help after remove"             bucket remove --cli11-help

# buckets alias (same app as bucket, same help)
check_help "cli11-help buckets"                  buckets --cli11-help
check_help "cli11-help buckets list"             buckets list --cli11-help
check_help "cli11-help buckets rm"               buckets rm --cli11-help
check_help "cli11-help buckets check"            buckets check --cli11-help

# bucket logging (nested) — the logging node and each leaf, at several positions
check_help "cli11-help bucket logging"             --cli11-help bucket logging
check_help "cli11-help logging after bucket"       bucket --cli11-help logging
check_help "cli11-help bucket logging info"        --cli11-help bucket logging info
check_help "cli11-help logging info after bucket"  bucket logging --cli11-help info
check_help "cli11-help after logging info"         bucket logging info --cli11-help
check_help "cli11-help bucket logging list"        --cli11-help bucket logging list
check_help "cli11-help after logging list"         bucket logging list --cli11-help
check_help "cli11-help bucket logging flush"       --cli11-help bucket logging flush
check_help "cli11-help after logging flush"        bucket logging flush --cli11-help

# bucket layout
check_help "cli11-help bucket layout"            --cli11-help bucket layout
check_help "cli11-help layout after bucket"      bucket --cli11-help layout
check_help "cli11-help after layout"             bucket layout --cli11-help

# bucket chown
check_help "cli11-help bucket chown"             --cli11-help bucket chown
check_help "cli11-help chown after bucket"       bucket --cli11-help chown
check_help "cli11-help after chown"              bucket chown --cli11-help

# bucket limit check (nested: limit node + check leaf)
check_help "cli11-help bucket limit"             --cli11-help bucket limit
check_help "cli11-help limit after bucket"       bucket --cli11-help limit
check_help "cli11-help bucket limit check"       --cli11-help bucket limit check
check_help "cli11-help limit check after bucket" bucket limit --cli11-help check
check_help "cli11-help after limit check"        bucket limit check --cli11-help

# bucket rewrite
check_help "cli11-help bucket rewrite"           --cli11-help bucket rewrite
check_help "cli11-help rewrite after bucket"     bucket --cli11-help rewrite
check_help "cli11-help after rewrite"            bucket rewrite --cli11-help

# bucket set-min-shards
check_help "cli11-help bucket set-min-shards"        --cli11-help bucket set-min-shards
check_help "cli11-help set-min-shards after bucket"  bucket --cli11-help set-min-shards
check_help "cli11-help after set-min-shards"         bucket set-min-shards --cli11-help

# bucket object shard
check_help "cli11-help bucket object shard"          --cli11-help bucket object shard
check_help "cli11-help object shard after bucket"    bucket --cli11-help object shard
check_help "cli11-help after object shard"           bucket object shard --cli11-help
check_help "cli11-help bucket object (node)"         bucket object --cli11-help

# bucket shard objects (+ alias 'shard object', + node 'shard')
check_help "cli11-help bucket shard objects"         --cli11-help bucket shard objects
check_help "cli11-help shard objects after bucket"   bucket --cli11-help shard objects
check_help "cli11-help after shard objects"          bucket shard objects --cli11-help
check_help "cli11-help after shard object (alias)"   bucket shard object --cli11-help
check_help "cli11-help bucket shard (node)"          bucket shard --cli11-help

# bucket resync encrypted multipart (3-level nesting)
check_help "cli11-help bucket resync encrypted multipart" --cli11-help bucket resync encrypted multipart
check_help "cli11-help resync after bucket"               bucket --cli11-help resync encrypted multipart
check_help "cli11-help after resync multipart"            bucket resync encrypted multipart --cli11-help
check_help "cli11-help bucket resync (node)"              bucket resync --cli11-help
check_help "cli11-help bucket resync encrypted (node)"    bucket resync encrypted --cli11-help

# bucket radoslist (+ 'bucket rados list' alias, + 'rados' node)
check_help "cli11-help bucket radoslist"            --cli11-help bucket radoslist
check_help "cli11-help radoslist after bucket"      bucket --cli11-help radoslist
check_help "cli11-help after radoslist"             bucket radoslist --cli11-help
check_help "cli11-help bucket rados (node)"         bucket rados --cli11-help
check_help "cli11-help after rados list (alias)"    bucket rados list --cli11-help

# ============================================================
echo ""
echo "=== --cli11-help content verification ==="
# ============================================================

# bucket subcommand: lists all subcommands
check_help_content "help content bucket: list"          "list"          bucket --cli11-help
check_help_content "help content bucket: stats"         "stats"         bucket --cli11-help
check_help_content "help content bucket: link"          "link"          bucket --cli11-help
check_help_content "help content bucket: unlink"        "unlink"        bucket --cli11-help
check_help_content "help content bucket: rm"            "rm"            bucket --cli11-help
check_help_content "help content bucket: check"         "check"         bucket --cli11-help

# bucket list: all flags present
check_help_content "help content list: --bucket"        "--bucket"            bucket list --cli11-help
check_help_content "help content list: -b short"        "-b"                  bucket list --cli11-help
check_help_content "help content list: --allow-unordered" "--allow-unordered" bucket list --cli11-help
check_help_content "help content list: --max-entries"   "--max-entries"       bucket list --cli11-help
check_help_content "help content list: --marker"        "--marker"            bucket list --cli11-help
check_help_content "help content list: --format"        "--format"            bucket list --cli11-help
check_help_content "help content list: --object-version" "--object-version"   bucket list --cli11-help
check_help_content "help content list: description"     "list buckets"        bucket list --cli11-help

# bucket stats: all flags present
check_help_content "help content stats: --bucket"           "--bucket"            bucket stats --cli11-help
check_help_content "help content stats: --show-restore-stats" "--show-restore-stats" bucket stats --cli11-help
check_help_content "help content stats: --format"           "--format"            bucket stats --cli11-help
check_help_content "help content stats: --max-entries"      "--max-entries"       bucket stats --cli11-help
check_help_content "help content stats: description"        "returns bucket statistics" bucket stats --cli11-help

# bucket link: all flags present
check_help_content "help content link: --bucket"        "--bucket"          bucket link --cli11-help
check_help_content "help content link: --uid"           "--uid"             bucket link --cli11-help
check_help_content "help content link: -i short"        "-i"                bucket link --cli11-help
check_help_content "help content link: --bucket-new-name" "--bucket-new-name" bucket link --cli11-help
check_help_content "help content link: --bucket-id"     "--bucket-id"       bucket link --cli11-help
check_help_content "help content link: description"     "link bucket"       bucket link --cli11-help

# bucket unlink: all flags present
check_help_content "help content unlink: --bucket"      "--bucket"          bucket unlink --cli11-help
check_help_content "help content unlink: --uid"         "--uid"             bucket unlink --cli11-help
check_help_content "help content unlink: description"   "unlink bucket"     bucket unlink --cli11-help

# bucket rm: all flags present
check_help_content "help content rm: --bucket"              "--bucket"               bucket rm --cli11-help
check_help_content "help content rm: --purge-objects"       "--purge-objects"        bucket rm --cli11-help
check_help_content "help content rm: --bypass-gc"           "--bypass-gc"            bucket rm --cli11-help
check_help_content "help content rm: --inconsistent-index"  "--inconsistent-index"   bucket rm --cli11-help
check_help_content "help content rm: --yes-i-really-mean-it" "--yes-i-really-mean-it" bucket rm --cli11-help
check_help_content "help content rm: description"           "remove bucket"          bucket rm --cli11-help

# bucket check: all flags and subcommands present
check_help_content "help content check: --bucket"              "--bucket"               bucket check --cli11-help
check_help_content "help content check: --fix"                 "--fix"                  bucket check --cli11-help
check_help_content "help content check: --remove-bad"          "--remove-bad"           bucket check --cli11-help
check_help_content "help content check: --check-head-obj-locator" "--check-head-obj-locator" bucket check --cli11-help
check_help_content "help content check: --check-objects"       "--check-objects"        bucket check --cli11-help
check_help_content "help content check: --max-concurrent-ios"  "--max-concurrent-ios"   bucket check --cli11-help
check_help_content "help content check: olh subcommand"        "olh"                    bucket check --cli11-help
check_help_content "help content check: unlinked subcommand"   "unlinked"               bucket check --cli11-help

# bucket check olh: all flags
check_help_content "help content check olh: --bucket"          "--bucket"               bucket check olh --cli11-help
check_help_content "help content check olh: --fix"             "--fix"                  bucket check olh --cli11-help
check_help_content "help content check olh: --max-concurrent-ios" "--max-concurrent-ios" bucket check olh --cli11-help
check_help_content "help content check olh: --dump-keys"       "--dump-keys"            bucket check olh --cli11-help
check_help_content "help content check olh: --hide-progress"   "--hide-progress"        bucket check olh --cli11-help

# bucket check unlinked: all flags
check_help_content "help content check unlinked: --bucket"     "--bucket"               bucket check unlinked --cli11-help
check_help_content "help content check unlinked: --fix"        "--fix"                  bucket check unlinked --cli11-help
check_help_content "help content check unlinked: --dump-keys"  "--dump-keys"            bucket check unlinked --cli11-help
check_help_content "help content check unlinked: --hide-progress" "--hide-progress"     bucket check unlinked --cli11-help

# remove alias: same flags as rm
check_help_content "help content remove: --purge-objects"      "--purge-objects"        bucket remove --cli11-help
check_help_content "help content remove: --bypass-gc"          "--bypass-gc"            bucket remove --cli11-help

# buckets alias: same as bucket
check_help_content "help content buckets list: --allow-unordered" "--allow-unordered"   buckets list --cli11-help
check_help_content "help content buckets rm: --purge-objects"     "--purge-objects"      buckets rm --cli11-help
check_help_content "help content buckets check: --fix"            "--fix"                buckets check --cli11-help

# bucket layout: all flags + description
check_help_content "help content layout: --bucket"     "--bucket"                 bucket layout --cli11-help
check_help_content "help content layout: -b short"     "-b"                       bucket layout --cli11-help
check_help_content "help content layout: --bucket-id"  "--bucket-id"              bucket layout --cli11-help
check_help_content "help content layout: --tenant"     "--tenant"                 bucket layout --cli11-help
check_help_content "help content layout: --format"     "--format"                 bucket layout --cli11-help
check_help_content "help content layout: description"  "show the bucket's layout" bucket layout --cli11-help

# bucket chown: all flags + description
check_help_content "help content chown: --bucket"          "--bucket"               bucket chown --cli11-help
check_help_content "help content chown: -b short"          "-b"                     bucket chown --cli11-help
check_help_content "help content chown: --uid"             "--uid"                  bucket chown --cli11-help
check_help_content "help content chown: -i short"          "-i"                     bucket chown --cli11-help
check_help_content "help content chown: --marker"          "--marker"               bucket chown --cli11-help
check_help_content "help content chown: --tenant"          "--tenant"               bucket chown --cli11-help
check_help_content "help content chown: --bucket-new-name" "--bucket-new-name"      bucket chown --cli11-help
check_help_content "help content chown: description"       "update its object ACLs" bucket chown --cli11-help

# bucket limit check: all flags + description
check_help_content "help content limit check: --uid"           "--uid"                      bucket limit check --cli11-help
check_help_content "help content limit check: -i short"        "-i"                         bucket limit check --cli11-help
check_help_content "help content limit check: --warnings-only" "--warnings-only"            bucket limit check --cli11-help
check_help_content "help content limit check: description"     "show bucket sharding stats" bucket limit check --cli11-help

# bucket set-min-shards: all flags present
check_help_content "help content set-min-shards: --bucket"      "--bucket"      bucket set-min-shards --cli11-help
check_help_content "help content set-min-shards: -b short"      "-b"            bucket set-min-shards --cli11-help
check_help_content "help content set-min-shards: --bucket-id"   "--bucket-id"   bucket set-min-shards --cli11-help
check_help_content "help content set-min-shards: --tenant"      "--tenant"      bucket set-min-shards --cli11-help
check_help_content "help content set-min-shards: --num-shards"  "--num-shards"  bucket set-min-shards --cli11-help
check_help_content "help content set-min-shards: --num-shards REQUIRED" "<num-shards> REQUIRED" bucket set-min-shards --cli11-help
check_help_content "help content set-min-shards: description"   "minimum number of shards that dynamic resharding" bucket set-min-shards --cli11-help

# bucket object shard: all flags present
check_help_content "help content object shard: --object"      "--object"      bucket object shard --cli11-help
check_help_content "help content object shard: -o short"      "-o"            bucket object shard --cli11-help
check_help_content "help content object shard: --object REQUIRED"     "<object> REQUIRED"     bucket object shard --cli11-help
check_help_content "help content object shard: --num-shards" "--num-shards"  bucket object shard --cli11-help
check_help_content "help content object shard: --num-shards REQUIRED" "<num-shards> REQUIRED" bucket object shard --cli11-help
check_help_content "help content object shard: --format"     "--format"      bucket object shard --cli11-help
check_help_content "help content object shard: description"   "show the shard index a given object maps to" bucket object shard --cli11-help
check_help_content "help content object (node): description"  "bucket object commands" bucket object --cli11-help

# bucket shard objects: all flags present
check_help_content "help content shard objects: --num-shards" "--num-shards"  bucket shard objects --cli11-help
check_help_content "help content shard objects: --num-shards REQUIRED" "<num-shards> REQUIRED" bucket shard objects --cli11-help
check_help_content "help content shard objects: --shard-id"   "--shard-id"    bucket shard objects --cli11-help
check_help_content "help content shard objects: --prefix"     "--prefix"      bucket shard objects --cli11-help
check_help_content "help content shard objects: --format"     "--format"      bucket shard objects --cli11-help
check_help_content "help content shard objects: description"  "show sample object names that map to bucket shards" bucket shard objects --cli11-help
check_help_content "help content shard (node): description"   "bucket shard commands" bucket shard --cli11-help

# bucket resync encrypted multipart: all flags present
check_help_content "help content resync: --bucket"      "--bucket"      bucket resync encrypted multipart --cli11-help
check_help_content "help content resync: -b short"      "-b"            bucket resync encrypted multipart --cli11-help
check_help_content "help content resync: --bucket REQUIRED" "<bucket> REQUIRED" bucket resync encrypted multipart --cli11-help
check_help_content "help content resync: --bucket-id"   "--bucket-id"   bucket resync encrypted multipart --cli11-help
check_help_content "help content resync: --tenant"      "--tenant"      bucket resync encrypted multipart --cli11-help
check_help_content "help content resync: --marker"      "--marker"      bucket resync encrypted multipart --cli11-help
check_help_content "help content resync: --yes-i-really-mean-it" "--yes-i-really-mean-it" bucket resync encrypted multipart --cli11-help
check_help_content "help content resync: --format"      "--format"      bucket resync encrypted multipart --cli11-help
check_help_content "help content resync: leaf description"   "repair replication of encrypted multipart uploads" bucket resync encrypted multipart --cli11-help
check_help_content "help content resync (node): description" "bucket resync commands" bucket resync --cli11-help
check_help_content "help content resync encrypted (node): description" "encrypted multipart resync commands" bucket resync encrypted --cli11-help

# bucket radoslist: all flags present (descriptions from usage())
check_help_content "help content radoslist: --bucket"             "--bucket"             bucket radoslist --cli11-help
check_help_content "help content radoslist: -b short"             "-b"                   bucket radoslist --cli11-help
check_help_content "help content radoslist: --tenant"             "--tenant"             bucket radoslist --cli11-help
check_help_content "help content radoslist: --max-concurrent-ios" "--max-concurrent-ios" bucket radoslist --cli11-help
check_help_content "help content radoslist: --orphan-stale-secs"  "--orphan-stale-secs"  bucket radoslist --cli11-help
check_help_content "help content radoslist: --rgw-obj-fs"         "--rgw-obj-fs"         bucket radoslist --cli11-help
check_help_content "help content radoslist: --yes-i-really-mean-it" "--yes-i-really-mean-it" bucket radoslist --cli11-help
check_help_content "help content radoslist: description"  "list rados objects backing bucket's objects" bucket radoslist --cli11-help
check_help_content "help content radoslist: max-concurrent-ios desc" "maximum concurrent ios for bucket operations (default: 32)" bucket radoslist --cli11-help
# alias entry point + node descriptions
check_help_content "help content rados list (alias): description" "list rados objects backing bucket's objects" bucket rados list --cli11-help
check_help_content "help content rados list (alias): --rgw-obj-fs" "--rgw-obj-fs" bucket rados list --cli11-help
check_help_content "help content rados (node): description" "bucket rados commands" bucket rados --cli11-help
# the --max-concurrent-ios description swap also landed on check/olh/unlinked
check_help_content "help content check: max-concurrent-ios desc (swapped)" "maximum concurrent ios for bucket operations (default: 32)" bucket check --cli11-help

# --bucket marked "<bucket> REQUIRED" in help for the commands that error on a
# missing bucket (consistency with link/unlink/rm/chown/rewrite)
check_help_content "help content layout: --bucket REQUIRED"        "<bucket> REQUIRED" bucket layout --cli11-help
check_help_content "help content logging info: --bucket REQUIRED"  "<bucket> REQUIRED" bucket logging info --cli11-help
check_help_content "help content logging list: --bucket REQUIRED"  "<bucket> REQUIRED" bucket logging list --cli11-help
check_help_content "help content logging flush: --bucket REQUIRED" "<bucket> REQUIRED" bucket logging flush --cli11-help
check_help_content "help content set-min-shards: --bucket REQUIRED" "<bucket> REQUIRED" bucket set-min-shards --cli11-help

# ============================================================
echo ""
echo "=== functional: format and flag options (cluster) ==="
# ============================================================

# bucket list: format and ordering flags
check_cluster "functional: bucket list --allow-unordered"      0 "" -- \
  bucket list --allow-unordered
check_cluster "functional: bucket list --format json"          0 "" -- \
  bucket list --format json
check_cluster "functional: bucket list --format xml"           0 "" -- \
  bucket list --format xml
check_cluster "functional: bucket list --max-entries 5"        0 "" -- \
  bucket list --max-entries 5
check_cluster "functional: bucket list --allow-unordered + --max-entries" 0 "" -- \
  bucket list --allow-unordered --max-entries 10
check_cluster "functional: buckets list --allow-unordered (alias)" 0 "" -- \
  buckets list --allow-unordered

# bucket stats: format and restore-stats
check_cluster "functional: bucket stats --format json"         0 "" -- \
  bucket stats --format json
check_cluster "functional: bucket stats --format xml"          0 "" -- \
  bucket stats --format xml
check_cluster "functional: bucket stats --max-entries 5"       0 "" -- \
  bucket stats --max-entries 5
check_cluster "functional: bucket stats --show-restore-stats"  0 "" -- \
  bucket stats --show-restore-stats
check_cluster "functional: buckets stats --format json (alias)" 0 "" -- \
  buckets stats --format json

# bucket check: new flags
check_cluster "functional: bucket check --check-objects"           0 "" -- \
  bucket check --check-objects
check_cluster "functional: bucket check --max-concurrent-ios"      0 "" -- \
  bucket check --max-concurrent-ios 4

# bucket check olh/unlinked: no --bucket (global scan) and new flags
check_cluster "functional: bucket check olh (no --bucket)"        0 "" -- \
  bucket check olh
check_cluster "functional: bucket check olh --fix"                0 "" -- \
  bucket check olh --fix
check_cluster "functional: bucket check olh --dump-keys"          0 "" -- \
  bucket check olh --dump-keys
check_cluster "functional: bucket check olh --hide-progress"      0 "" -- \
  bucket check olh --hide-progress
check_cluster "functional: bucket check olh --max-concurrent-ios" 0 "" -- \
  bucket check olh --max-concurrent-ios 4
check_cluster "functional: bucket check unlinked (no --bucket)"   0 "" -- \
  bucket check unlinked
check_cluster "functional: bucket check unlinked --fix"           0 "" -- \
  bucket check unlinked --fix
check_cluster "functional: bucket check unlinked --dump-keys"     0 "" -- \
  bucket check unlinked --dump-keys
check_cluster "functional: bucket check unlinked --hide-progress" 0 "" -- \
  bucket check unlinked --hide-progress
# --dump-keys/--hide-progress migrated to the binary-flag form, so a value
# suffix now warn-and-accepts (=banana -> warn + set), like the other binary
# flags above. olh/unlinked are cluster commands, hence check_warns (not the
# no-cluster "check" =banana block).
check_warns "check olh: --dump-keys=banana (warn-accept)"          0 "" \
  "Warning: invalid value 'banana' for --dump-keys, treating as set" -- \
  bucket check olh --dump-keys=banana
check_warns "check olh: --hide-progress=banana (warn-accept)"      0 "" \
  "Warning: invalid value 'banana' for --hide-progress, treating as set" -- \
  bucket check olh --hide-progress=banana
check_warns "check unlinked: --dump-keys=banana (warn-accept)"     0 "" \
  "Warning: invalid value 'banana' for --dump-keys, treating as set" -- \
  bucket check unlinked --dump-keys=banana
check_warns "check unlinked: --hide-progress=banana (warn-accept)" 0 "" \
  "Warning: invalid value 'banana' for --hide-progress, treating as set" -- \
  bucket check unlinked --hide-progress=banana
# Valid bool value forms (=true/=false and the space form) are consumed cleanly
# (exit 0, no stray), like the other binary flags. olh is the representative:
# --dump-keys/--hide-progress share one registration and var across olh and
# unlinked, so value parsing is identical (per-subcommand execution covered above).
check_cluster "check olh: --dump-keys=true"              0 "" -- \
  bucket check olh --dump-keys=true
check_cluster "check olh: --dump-keys true (space)"      0 "" -- \
  bucket check olh --dump-keys true
check_cluster "check olh: --dump-keys=false"             0 "" -- \
  bucket check olh --dump-keys=false
check_cluster "check olh: --dump-keys false (space)"     0 "" -- \
  bucket check olh --dump-keys false
check_cluster "check olh: --hide-progress=true"          0 "" -- \
  bucket check olh --hide-progress=true
check_cluster "check olh: --hide-progress true (space)"  0 "" -- \
  bucket check olh --hide-progress true
check_cluster "check olh: --hide-progress=false"         0 "" -- \
  bucket check olh --hide-progress=false
check_cluster "check olh: --hide-progress false (space)" 0 "" -- \
  bucket check olh --hide-progress false
check_cluster "functional: buckets check olh (alias, no --bucket)" 0 "" -- \
  buckets check olh
check_cluster "functional: buckets check unlinked (alias)"         0 "" -- \
  buckets check unlinked

# ============================================================
echo ""
echo "=== '=' token normalization (empty '=' and short-flag '=') ==="
# ============================================================
# CLI11's tokenizer collapses "--name=" into a bare "--name" (missing value)
# and captures "-i=value" as "=value"; normalize_cli11_tokens() recovers both
# before parsing so '='-form values behave like the legacy parser.

# int flag: "" fails strict_strtol (legacy exits 22; CLI11 conversion error)
check "empty-= on int flag" 22 "Could not convert" bucket list --max-entries=
# uid/bucket-id: rewritten to explicit "" values, then rejected by the CLI11-side
# per-value empty checks (same message and exit code as legacy's special cases)
check "empty-= on --uid"       1 "no value for uid"       bucket list --uid=
check "empty-= on -i"          1 "no value for uid"       bucket list -i=
check "empty-= on --bucket-id" 1 "no value for bucket-id" bucket stats --bucket-id=
# mid-line: "" is the value; the next word strays (the collapsed flag must not eat it)
check "empty-= mid-line strays next word" 1 "unexpected argument: 'foo'" \
  bucket list --bucket= foo
# unknown flag: not rewritten (arity unknown), legacy rejects it
check "empty-= on unknown flag" 22 "invalid flag --banana=" bucket list --banana=
# glued short flag (i.e. -Xvalue, no '='): CLI11 accepts it as "-i banana"
# (mentor decision: supported, and stays supported once legacy is gone); today
# the transitional legacy loop still rejects the original token with its
# unknown-flag error
check "glued short flag rejected" 22 "invalid flag -ibanana" bucket list -ibanana

# ============================================================
echo ""
echo "=== empty-value checks (CLI11-side) and glued short flags ==="
# ============================================================
# Legacy's empty-value special cases for -i/--uid and --bucket-id run as
# CLI11 per-value checks (same message and exit code; the legacy loop still
# backstops lines CLI11 never sees). The '='-forms are covered above; these
# are the space forms, where CLI11 consumes the empty token as the value.

check "empty space-form -i"          1 "no value for uid"       bucket list -i ""
check "empty space-form --uid"       1 "no value for uid"       bucket list --uid ""
# uid emptiness is judged after tenant$user parsing, like legacy: an empty
# user-id part rejects even when a tenant is present
check "parse-empty uid: -i '\$'"     1 "no value for uid"       bucket list -i '$'
check "parse-empty uid: -i 'tenant\$'" 1 "no value for uid"     bucket list -i 'tenant$'
check "empty space-form --bucket-id" 1 "no value for bucket-id" bucket stats --bucket-id ""
# the check rides every copy of the option: pre-command tokens are collected
# by the hidden root/ancestor copies, not the leaf's
check "empty --bucket-id before command (root copy)" 1 "no value for bucket-id" \
  --bucket-id "" bucket stats
check "empty --bucket-id mid-tree (bucket copy)"     1 "no value for bucket-id" \
  bucket --bucket-id "" stats
# the check runs per occurrence, like legacy's loop: an empty value errors
# whether or not another occurrence supplies a valid one
check "empty -i then valid -i" 1 "no value for uid" bucket list -i "" -i slides
check "valid -i then empty -i" 1 "no value for uid" bucket list -i slides -i ""

# glued short values (i.e. -Xvalue): CLI11 accepts them natively (mentor
# decision: supported, and they keep working once legacy is gone); the
# rejections below all come from the transitional legacy loop, which scans
# the original tokens.
# "-ibanana" above covers -i; -b and -o are the other CLI11-owned shorts
# that take values
check "glued short -b rejected" 22 "invalid flag -bdemo" bucket stats -bdemo
check "glued short -o rejected" 22 "invalid flag -oxyz"  bucket object shard -oxyz
# unknown short glued: same loop error (ownership makes no difference here)
check "unknown glued short rejected" 22 "invalid flag -xfoo" bucket list -xfoo
# left-to-right: the first glued token is the one reported by the loop's scan
check "first glued token wins"  22 "invalid flag -ibanana" bucket list -ibanana -bdemo
# flag position is what matters, not position relative to the command words
check "glued short before command rejected" 22 "invalid flag -ibanana" -ibanana bucket list
# help wins: CLI11 parses the glued token natively and help returns before
# the legacy loop runs
check "glued short + --cli11-help wins" 0 "" bucket list -ibanana --cli11-help

# ============================================================
echo ""
echo "=== integration: bucket list and stats (cluster) ==="
# ============================================================

# bucket list with no args lists all buckets (may be empty) — always succeeds
check_cluster "integration: bucket list (all)"  0 "" -- \
  bucket list

# bucket list via buckets alias
check_cluster "integration: buckets list (alias)" 0 "" -- \
  buckets list

# bucket list for a nonexistent bucket errors
check_cluster "integration: bucket list nonexistent" 2 "ERROR: could not init bucket" -- \
  bucket list --bucket nonexistent_cli11_test_xyz

# bucket stats with no args lists all bucket stats — always succeeds
check_cluster "integration: bucket stats (all)"  0 "" -- \
  bucket stats

# bucket stats for a nonexistent bucket
check_cluster "integration: bucket stats nonexistent" 2 "" -- \
  bucket stats --bucket nonexistent_cli11_test_xyz

# a glued-short-looking token in value position is a value, never rejected
# (both parsers consume it: it's a bucket named "-ibanana", which doesn't exist)
check_cluster "integration: glued token in value position is a value" 2 "" -- \
  bucket stats --bucket -ibanana

# bucket check with no args runs index check — always succeeds (even with 0 buckets)
check_cluster "integration: bucket check (all)"  0 "" -- \
  bucket check

# buckets alias works for non-list subcommands too
check_cluster "integration: buckets stats (alias)"  0 "" -- \
  buckets stats
check_cluster "integration: buckets check (alias)"  0 "" -- \
  buckets check

# --bucket-id without --bucket triggers rgw_find_bucket_by_id path
# legacy returns -ENOENT from main for an unknown bucket id, so the exit code is 254
check_cluster "integration: bucket stats --bucket-id nonexistent" 254 "failure: no such bucket id" -- \
  bucket stats --bucket-id nonexistent_id_cli11_test

# --inconsistent-index + --yes-i-really-mean-it suppresses the warning and proceeds
check_cluster "integration: rm --inconsistent-index --yes-i-really-mean-it (nonexistent)" 0 "" -- \
  bucket rm --bucket nonexistent_cli11_test --inconsistent-index --yes-i-really-mean-it

# ============================================================
echo ""
echo "=== integration: full bucket lifecycle (cluster) ==="
# ============================================================
# Creates a test user and bucket, runs link/unlink/rm, then cleans up.
# Skipped automatically if no cluster is running.

_test_uid="cli11_bucket_test_user"
_test_bucket="cli11-bucket-test"
_test_display="CLI11 Bucket Test User"

if cluster_running; then
  # Create a test user (legacy command, not yet CLI11-migrated)
  "$RGW_ADMIN" user create --uid "$_test_uid" --display-name "$_test_display" \
    >/dev/null 2>&1

  # Create a bucket via the S3 API using radosgw-admin bucket link on a
  # freshly created bucket. Since bucket creation requires S3 API access,
  # we use radosgw-admin to create the bucket directly by linking it.
  # Note: 'bucket link' links an existing RADOS bucket to a user. The bucket
  # must have been created first via S3 PUT bucket. We use a workaround here:
  # create the bucket listing entry directly via radosgw-admin if possible,
  # or skip the link/unlink/rm lifecycle tests.

  # Check if aws CLI is available to create the bucket
  _aws_available=0
  if command -v aws >/dev/null 2>&1; then
    _aws_available=1
  fi

  if [ "$_aws_available" = "1" ]; then
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

      # bucket list: lists objects in the test bucket (empty)
      check_cluster "integration: bucket list (named, empty)" 0 "" -- \
        bucket list --bucket "$_test_bucket"

      # short flags -b and -i work the same as --bucket and --uid
      check_cluster "integration: bucket list -b (short flag)" 0 "" -- \
        bucket list -b "$_test_bucket"
      check_cluster "integration: bucket stats -b (short flag)" 0 "" -- \
        bucket stats -b "$_test_bucket"

      # bucket stats: returns stats for the test bucket
      check_cluster "integration: bucket stats (named)" 0 "" -- \
        bucket stats --bucket "$_test_bucket"

      # '=' normalization shapes that execute (cluster):
      # string flag with empty '=': "" value like legacy, command runs
      check_cluster "integration: empty-= on string flag runs" 0 "" -- \
        bucket stats --bucket=
      # empty-= must not eat a following flag
      check_cluster "integration: empty-= then flag parses normally" 0 "" -- \
        bucket list --bucket= --max-entries 7
      # binary flag empty-= keeps its silent-set behavior (not rewritten)
      check_cluster "integration: binary flag empty-= unchanged" 0 "" -- \
        bucket list --bucket "$_test_bucket" --allow-unordered=
      # -i=<uid>: CLI11 captures the value correctly (lists the user's bucket)
      check_cluster "integration: -i=uid captures value" 0 "$_test_bucket" -- \
        bucket list -i="$_test_uid"
      # value position: a token after a value-taking flag is its value, even
      # if it looks like a flag (both parsers consume it; handler then fails)
      check_cluster "integration: flag value may look like a flag" 2 "" -- \
        bucket stats --bucket --max-entries
      check_cluster "lifecycle: bucket list --allow-unordered" 0 "" -- \
        bucket list --allow-unordered --bucket "$_test_bucket"
      check_cluster "lifecycle: bucket list --format json" 0 "" -- \
        bucket list --format json --bucket "$_test_bucket"
      check_cluster "lifecycle: bucket stats --show-restore-stats" 0 "" -- \
        bucket stats --show-restore-stats --bucket "$_test_bucket"
      check_cluster "lifecycle: bucket stats --format json" 0 "" -- \
        bucket stats --format json --bucket "$_test_bucket"

      # bucket layout: dumps the bucket's layout as JSON (index/log generations)
      check_cluster "integration: bucket layout" 0 "current_index" -- \
        bucket layout --bucket "$_test_bucket"
      check_cluster "integration: bucket layout --format json" 0 "current_index" -- \
        bucket layout --bucket "$_test_bucket" --format json
      check_cluster "integration: bucket layout --tenant ''" 0 "current_index" -- \
        bucket layout --bucket "$_test_bucket" --tenant ""

      # bucket rewrite: rewrites all objects in the bucket (empty bucket -> empty
      # "objects" array, exit 0). Exercises -b, --format, the size flags, the
      # date aliases, and the atoll-compat path on a REAL bucket.
      check_cluster "integration: bucket rewrite" 0 "objects" -- \
        bucket rewrite --bucket "$_test_bucket"
      check_cluster "integration: bucket rewrite -b (short flag)" 0 "objects" -- \
        bucket rewrite -b "$_test_bucket"
      check_cluster "integration: bucket rewrite --format json" 0 "objects" -- \
        bucket rewrite --bucket "$_test_bucket" --format json
      check_cluster "integration: bucket rewrite --min-rewrite-size (numeric)" 0 "objects" -- \
        bucket rewrite --bucket "$_test_bucket" --min-rewrite-size 1
      check_cluster "integration: bucket rewrite --min-rewrite-size=abc (atoll -> 0)" 0 "objects" -- \
        bucket rewrite --bucket "$_test_bucket" --min-rewrite-size=abc
      check_cluster "integration: bucket rewrite --start-time/--end-time (aliases)" 0 "objects" -- \
        bucket rewrite --bucket "$_test_bucket" --start-time 2000-01-01 --end-time 2100-01-01
      # bad date is parsed AFTER init_bucket, so on a real bucket it reaches the
      # date check and fails with EINVAL (exit 22)
      check_cluster "integration: bucket rewrite bad --start-date (exit 22)" 22 "ERROR: failed to parse start date" -- \
        bucket rewrite --bucket "$_test_bucket" --start-date notadate

      # status assertions: with a small (<4MB) object present, the per-object
      # "status" field proves the filters actually work. Default min (4MB) skips
      # it; atoll("abc")=0 disables the min filter so it is rewritten (Success);
      # a past --end-time filters it out by date (Skipped).
      echo "rewrite-status-probe" > /tmp/cli11_rw_small.txt
      AWS_ACCESS_KEY_ID="$_access_key" \
      AWS_SECRET_ACCESS_KEY="$_secret_key" \
      aws --endpoint-url "$_rgw_endpoint" \
        s3 cp /tmp/cli11_rw_small.txt "s3://$_test_bucket/" >/dev/null 2>&1

      check_cluster "integration: rewrite small obj Skipped (default 4M min)"        0 '"status": "Skipped"' -- \
        bucket rewrite --bucket "$_test_bucket"
      check_cluster "integration: rewrite --min-rewrite-size=abc Success (atoll->0)" 0 '"status": "Success"' -- \
        bucket rewrite --bucket "$_test_bucket" --min-rewrite-size=abc
      check_cluster "integration: rewrite --min-rewrite-size 1 Success"             0 '"status": "Success"' -- \
        bucket rewrite --bucket "$_test_bucket" --min-rewrite-size 1
      check_cluster "integration: rewrite past --end-time Skipped (date filter)"    0 '"status": "Skipped"' -- \
        bucket rewrite --bucket "$_test_bucket" --min-rewrite-size 1 --end-time 2000-01-01

      # remove the probe object so later lifecycle tests see an empty bucket
      AWS_ACCESS_KEY_ID="$_access_key" \
      AWS_SECRET_ACCESS_KEY="$_secret_key" \
      aws --endpoint-url "$_rgw_endpoint" \
        s3 rm "s3://$_test_bucket/cli11_rw_small.txt" >/dev/null 2>&1
      rm -f /tmp/cli11_rw_small.txt

      # bucket chown: chown to the (already-owning) test user — a no-op ownership
      # change that still exercises the full chown path; exit 0, no output
      check_cluster "integration: bucket chown" 0 "" -- \
        bucket chown --bucket "$_test_bucket" --uid "$_test_uid"

      # bucket limit check for the test user: JSON with user_id + buckets
      check_cluster "integration: limit check --uid" 0 "user_id" -- \
        bucket limit check --uid "$_test_uid"
      check_cluster "integration: limit check --uid --warnings-only" 0 "" -- \
        bucket limit check --uid "$_test_uid" --warnings-only

      # bucket set-min-shards: set the dynamic-resharding minimum on the (Normal)
      # test bucket; succeeds with no output (exit 0). Assert the value actually
      # changed by reading it back via bucket layout. Exercises -b, the =form,
      # and an empty --tenant.
      check_cluster "integration: bucket set-min-shards (num 7)" 0 "" -- \
        bucket set-min-shards --bucket "$_test_bucket" --num-shards 7
      check_cluster "integration: set-min-shards effect (layout shows 7)" 0 '"min_num_shards": 7' -- \
        bucket layout --bucket "$_test_bucket"
      check_cluster "integration: bucket set-min-shards -b --num-shards=9 (short + =form)" 0 "" -- \
        bucket set-min-shards -b "$_test_bucket" --num-shards=9
      check_cluster "integration: set-min-shards effect (layout shows 9)" 0 '"min_num_shards": 9' -- \
        bucket layout --bucket "$_test_bucket"
      check_cluster "integration: bucket set-min-shards --tenant '' (empty)" 0 "" -- \
        bucket set-min-shards --bucket "$_test_bucket" --num-shards 11 --tenant ""

      # bucket object shard: pure computation (no bucket needed), but runs after
      # driver init so a cluster is required. Deterministic: foo % 11 -> 10,
      # any object % 1 -> 0. Exercises -o short form, =form, and --format xml.
      check_cluster "integration: object shard (foo/11 -> 10)" 0 '"shard": 10' -- \
        bucket object shard --object foo --num-shards 11
      check_cluster "integration: object shard (foo/1 -> 0)"   0 '"shard": 0' -- \
        bucket object shard --object foo --num-shards 1
      check_cluster "integration: object shard -o --num-shards=11 (short + =form)" 0 '"shard": 10' -- \
        bucket object shard -o foo --num-shards=11
      check_cluster "integration: object shard --format xml" 0 "<obj_shard><shard>10</shard></obj_shard>" -- \
        bucket object shard --object foo --num-shards 11 --format xml

      # bucket shard objects: pure computation (no bucket needed), runs after
      # driver init. Deterministic sample object names per shard; --shard-id
      # picks one shard; --prefix changes the name prefix (default "obj").
      # Exercises the 'shard object' alias, =form, --prefix "", and --format xml.
      check_cluster "integration: shard objects (num 4, lists objs)" 0 '"obj00000000000000000000"' -- \
        bucket shard objects --num-shards 4
      check_cluster "integration: shard objects (num 1 -> single obj)" 0 '"obj00000000000000000000"' -- \
        bucket shard objects --num-shards 1
      check_cluster "integration: shard objects --shard-id 1" 0 '"obj": "obj00000000000000000004"' -- \
        bucket shard objects --num-shards 4 --shard-id 1
      check_cluster "integration: shard object (alias) --shard-id=1 (=form)" 0 '"obj": "obj00000000000000000004"' -- \
        bucket shard object --num-shards 4 --shard-id=1
      check_cluster "integration: shard objects --prefix myobj" 0 '"myobj00000000000000000000"' -- \
        bucket shard objects --num-shards 4 --prefix myobj
      check_cluster "integration: shard objects --prefix '' (engaged empty)" 0 '"00000000000000000000"' -- \
        bucket shard objects --num-shards 4 --prefix ""
      check_cluster "integration: shard objects --format xml" 0 "<shard_obj><obj>obj00000000000000000010</obj></shard_obj>" -- \
        bucket shard objects --num-shards 4 --shard-id 0 --format xml

      # bucket resync encrypted multipart: a repair op. On a non-replicated single-zone
      # cluster it needs --yes-i-really-mean-it; without it -> EPERM (exit 1). With it,
      # runs and emits the "modified" report (exit 0, idempotent on a normal bucket).
      # The binary flag's =false / space-form 'false' both leave it unset -> EPERM.
      check_cluster "integration: resync without --yes (EPERM)" 1 "This command is only necessary for replicated buckets." -- \
        bucket resync encrypted multipart --bucket "$_test_bucket"
      check_cluster "integration: resync --yes-i-really-mean-it (success)" 0 '"bucket_id"' -- \
        bucket resync encrypted multipart --bucket "$_test_bucket" --yes-i-really-mean-it
      check_cluster "integration: resync --yes-i-really-mean-it=false (=form -> EPERM)" 1 "This command is only necessary" -- \
        bucket resync encrypted multipart --bucket "$_test_bucket" --yes-i-really-mean-it=false

      # bucket radoslist: read-only, lists the rados objects backing the bucket
      # (exit 0). Exercises both entry points (radoslist + 'rados list' alias),
      # the -b short form, and the --rgw-obj-fs field separator.
      check_cluster "integration: radoslist --bucket" 0 "" -- \
        bucket radoslist --bucket "$_test_bucket"
      check_cluster "integration: radoslist -b (short)" 0 "" -- \
        bucket radoslist -b "$_test_bucket"
      check_cluster "integration: radoslist --rgw-obj-fs" 0 "" -- \
        bucket radoslist --bucket "$_test_bucket" --rgw-obj-fs ":"
      check_cluster "integration: rados list --bucket (alias)" 0 "" -- \
        bucket rados list --bucket "$_test_bucket"

      # bucket logging on a bucket WITHOUT logging configured: info is silent
      # (exit 0, no output); list and flush print an error but still exit 0
      # (legacy quirk, kept verbatim)
      check_cluster "integration: logging info (no logging, silent)" 0 "" -- \
        bucket logging info --bucket "$_test_bucket"
      check_cluster "integration: logging list (no logging, msg + exit 0)" 0 "$ERR_NO_LOGGING" -- \
        bucket logging list --bucket "$_test_bucket"
      check_cluster "integration: logging flush (no logging, msg + exit 0)" 0 "$ERR_NO_LOGGING" -- \
        bucket logging flush --bucket "$_test_bucket"

      # bucket unlink: unlink the bucket from the user
      check_cluster "integration: bucket unlink" 0 "" -- \
        bucket unlink --bucket "$_test_bucket" --uid "$_test_uid"

      # re-link using short flags -b and -i
      check_cluster "integration: bucket link -b -i (short flags)" 0 "" -- \
        bucket link -b "$_test_bucket" -i "$_test_uid"

      # bucket unlink using short flags
      check_cluster "integration: bucket unlink -b -i (short flags)" 0 "" -- \
        bucket unlink -b "$_test_bucket" -i "$_test_uid"

      # bucket link: re-link for remaining tests
      check_cluster "integration: bucket link" 0 "" -- \
        bucket link --bucket "$_test_bucket" --uid "$_test_uid"

      # bucket check: check the bucket index, with and without --fix
      check_cluster "integration: bucket check (named)" 0 "" -- \
        bucket check --bucket "$_test_bucket"
      check_cluster "lifecycle: bucket check --fix" 0 "" -- \
        bucket check --fix --bucket "$_test_bucket"

      # bucket check olh and unlinked with named bucket and new flags
      check_cluster "integration: bucket check olh (named)" 0 "" -- \
        bucket check olh --bucket "$_test_bucket"
      check_cluster "lifecycle: bucket check olh --fix (named)" 0 "" -- \
        bucket check olh --fix --bucket "$_test_bucket"
      check_cluster "lifecycle: bucket check olh --dump-keys (named)" 0 "" -- \
        bucket check olh --dump-keys --bucket "$_test_bucket"
      check_cluster "integration: bucket check unlinked (named)" 0 "" -- \
        bucket check unlinked --bucket "$_test_bucket"
      check_cluster "lifecycle: bucket check unlinked --fix (named)" 0 "" -- \
        bucket check unlinked --fix --bucket "$_test_bucket"
      check_cluster "lifecycle: bucket check unlinked --dump-keys (named)" 0 "" -- \
        bucket check unlinked --dump-keys --bucket "$_test_bucket"

      # bucket rm: remove the test bucket (it's empty, so no --purge-objects needed)
      check_cluster "integration: bucket rm" 0 "" -- \
        bucket rm --bucket "$_test_bucket"

      # bucket rm via 'remove' alias — re-create then remove
      AWS_ACCESS_KEY_ID="$_access_key" \
      AWS_SECRET_ACCESS_KEY="$_secret_key" \
      aws --endpoint-url "$_rgw_endpoint" \
        s3 mb "s3://$_test_bucket" >/dev/null 2>&1

      check_cluster "integration: bucket remove (alias for rm)" 0 "" -- \
        bucket remove --bucket "$_test_bucket"

      # Re-create to test --purge-objects (bucket is empty, so purge is a no-op)
      AWS_ACCESS_KEY_ID="$_access_key" \
      AWS_SECRET_ACCESS_KEY="$_secret_key" \
      aws --endpoint-url "$_rgw_endpoint" \
        s3 mb "s3://$_test_bucket" >/dev/null 2>&1

      check_cluster "lifecycle: bucket rm --purge-objects (empty bucket)" 0 "" -- \
        bucket rm --purge-objects --bucket "$_test_bucket"
    else
      echo "SKIP [integration: lifecycle tests]: could not get credentials for test user"
      SKIP=$((SKIP+39))
    fi
  else
    echo "SKIP [integration: lifecycle tests]: aws CLI not available (needed to create test bucket)"
    SKIP=$((SKIP+34))
  fi

  # Cleanup: remove the test user
  "$RGW_ADMIN" user rm --uid "$_test_uid" --purge-data >/dev/null 2>&1
fi

# NOTE: ceph-global stripping and --cli11-help footer tests live in
# test_cli11_globals.sh (they are cross-cutting, not bucket-specific).

# ============================================================
echo ""
echo "========================================"
echo "Results: $PASS passed, $FAIL failed, $SKIP skipped"
[ "$SKIP" -gt 0 ] && echo "(some tests require a running cluster or aws CLI)"
echo "========================================"
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
