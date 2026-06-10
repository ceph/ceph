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
PASS=0
FAIL=0
SKIP=0

# Warning message constants
WARN_BUCKET_POS="Warning: --bucket/-b should appear after the subcommand"
WARN_BUCKET_DUP="Warning: --bucket/-b specified multiple times, using last value"
WARN_BUCKETID_POS="Warning: --bucket-id should appear after the subcommand"
WARN_BUCKETID_DUP="Warning: --bucket-id specified multiple times, using last value"
WARN_UID_POS="Warning: --uid/-i should appear after the subcommand"
WARN_UID_DUP="Warning: --uid/-i specified multiple times, using last value"
WARN_TENANT_POS="Warning: --tenant should appear after the subcommand"
WARN_TENANT_DUP="Warning: --tenant specified multiple times, using last value"
WARN_FIX_POS="Warning: --fix should appear after the subcommand"
WARN_FORMAT_POS="Warning: --format should appear after the subcommand"
WARN_FORMAT_DUP="Warning: --format specified multiple times, using last value"
WARN_MARKER_POS="Warning: --marker should appear after the subcommand"
WARN_MAXENT_POS="Warning: --max-entries should appear after the subcommand"
WARN_PURGE_POS="Warning: --purge-objects should appear after the subcommand"
WARN_PURGE_DUP="Warning: --purge-objects specified multiple times, using last value"
WARN_BYPASS_POS="Warning: --bypass-gc should appear after the subcommand"
WARN_INCONSISTENT_POS="Warning: --inconsistent-index should appear after the subcommand"
WARN_YIRMI_POS="Warning: --yes-i-really-mean-it should appear after the subcommand"
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
WARN_DUMP_KEYS_POS="Warning: --dump-keys should appear after the subcommand"
WARN_HIDE_PROGRESS_POS="Warning: --hide-progress should appear after the subcommand"

# Error message constants
# Legacy behavior: missing --bucket/--uid is not validated up front; the op
# layer fails with raw EINVAL messages, and bucket rm ignores failures
# entirely (see TODOs in radosgw-admin.cc). Tests for these need a cluster.
ERR_FETCH_BUCKET="failure: (22) Invalid argument: failed to fetch bucket info for bucket="
ERR_REQUIRES_USER="failure: (22) Invalid argument: requires user or account id"
ERR_EINVAL="failure: (22) Invalid argument"
ERR_SUBCOMMAND="A subcommand is required"
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

check "bare bucket"          106 "$ERR_SUBCOMMAND" bucket
check "bare buckets (alias)" 106 "$ERR_SUBCOMMAND" buckets
check "unknown subcommand"   106 "$ERR_SUBCOMMAND" bucket banana

# ============================================================
echo ""
echo "=== buckets alias (non-list commands) ==="
# ============================================================

# no-cluster: alias works for all subcommands, not just list
check "buckets stats: stray arg"    22 "ERROR: unexpected argument: 'strayarg'" \
  buckets stats strayarg
# legacy: missing --bucket is not validated up front; rm silently exits 0,
# link/unlink fail in the op layer (cluster needed)
check_cluster "buckets rm: missing --bucket (silent exit 0)" 0 "" -- \
  buckets rm
check_cluster "buckets link: missing --bucket" 22 "$ERR_FETCH_BUCKET" -- \
  buckets link --uid testuser
check_cluster "buckets unlink: missing --bucket" 22 "$ERR_EINVAL" -- \
  buckets unlink --uid testuser
check "buckets check: stray arg"    22 "ERROR: unexpected argument: 'strayarg'" \
  buckets check strayarg
check "buckets list: unrecognized flag"    22 "ERROR: invalid flag --fakeflag" \
  buckets list --fakeflag
check "buckets link: stray after flags"    22 "ERROR: unexpected argument: 'strayarg'" \
  buckets link --bucket mybucket --uid testuser strayarg
check "buckets link: unrecognized flag"    22 "ERROR: invalid flag --fakeflag" \
  buckets link --bucket mybucket --uid testuser --fakeflag
check "buckets unlink: stray after flags"  22 "ERROR: unexpected argument: 'strayarg'" \
  buckets unlink --bucket mybucket --uid testuser strayarg
check "buckets unlink: unrecognized flag"  22 "ERROR: invalid flag --fakeflag" \
  buckets unlink --bucket mybucket --uid testuser --fakeflag
check "buckets rm: stray after flags"      22 "ERROR: unexpected argument: 'strayarg'" \
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
check "list: stray after flags"              22 "ERROR: unexpected argument: 'strayarg'" \
  bucket list strayarg
check "list: stray before bucket"            22 "ERROR: unexpected argument: 'foo'" \
  foo bucket list
check "list: stray between bucket and list"  22 "ERROR: unexpected argument: 'extra'" \
  bucket extra list

# unrecognized flag
check "list: unrecognized flag" 22 "ERROR: invalid flag --fakeflag" \
  bucket list --fakeflag

# flags in wrong position: check() catches these when combined with a required-arg error.
# Pure wrong-position (no error) tests are in check_warns below.

# missing option value
check "list: --bucket missing value"         114 "--bucket: 1 required TEXT missing" \
  bucket list --bucket
check "list: --uid missing value"            114 "--uid: 1 required TEXT missing" \
  bucket list --uid
check "list: --bucket-id missing value"      114 "--bucket-id: 1 required TEXT missing" \
  bucket list --bucket-id
check "list: --format missing value"         114 "--format: 1 required TEXT missing" \
  bucket list --format
check "list: --max-entries missing value"    114 "--max-entries: 1 required INT missing" \
  bucket list --max-entries
check "list: --marker missing value"         114 "--marker: 1 required TEXT missing" \
  bucket list --marker
check "list: --object-version missing value" 114 "--object-version: 1 required TEXT missing" \
  bucket list --object-version

# ============================================================
echo ""
echo "=== bucket stats ==="
# ============================================================

check "stats: stray after flags"             22 "ERROR: unexpected argument: 'strayarg'" \
  bucket stats strayarg
check "stats: stray before bucket"           22 "ERROR: unexpected argument: 'foo'" \
  foo bucket stats
check "stats: stray between bucket and stats" 22 "ERROR: unexpected argument: 'extra'" \
  bucket extra stats

check "stats: unrecognized flag" 22 "ERROR: invalid flag --fakeflag" \
  bucket stats --fakeflag

check "stats: --bucket missing value"      114 "--bucket: 1 required TEXT missing" \
  bucket stats --bucket
check "stats: --bucket-id missing value"   114 "--bucket-id: 1 required TEXT missing" \
  bucket stats --bucket-id
check "stats: --format missing value"      114 "--format: 1 required TEXT missing" \
  bucket stats --format
check "stats: --max-entries missing value" 114 "--max-entries: 1 required INT missing" \
  bucket stats --max-entries
check "stats: --marker missing value"      114 "--marker: 1 required TEXT missing" \
  bucket stats --marker

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
check_warns "link: --uid before bucket, missing --bucket"  22 "$ERR_FETCH_BUCKET" "$WARN_UID_POS" -- \
  --uid testuser bucket link
check_warns "link: both before bucket (warns x2, fails on nonexistent)" 2 "" \
  "$WARN_BUCKET_POS" "$WARN_UID_POS" -- \
  --bucket mybucket --uid testuser bucket link

# stray positional args
check "link: stray after flags"              22 "ERROR: unexpected argument: 'strayarg'" \
  bucket link --bucket mybucket --uid testuser strayarg
check "link: stray before bucket"            22 "ERROR: unexpected argument: 'foo'" \
  foo bucket link --bucket mybucket --uid testuser
check "link: stray between bucket and link"  22 "ERROR: unexpected argument: 'extra'" \
  bucket extra link --bucket mybucket --uid testuser

# unrecognized flag
check "link: unrecognized flag" 22 "ERROR: invalid flag --fakeflag" \
  bucket link --bucket mybucket --uid testuser --fakeflag

check "link: --bucket missing value"          114 "--bucket: 1 required TEXT missing" \
  bucket link --bucket
check "link: --uid missing value"             114 "--uid: 1 required TEXT missing" \
  bucket link --uid
check "link: --bucket-id missing value"       114 "--bucket-id: 1 required TEXT missing" \
  bucket link --bucket-id
check "link: --bucket-new-name missing value" 114 "--bucket-new-name: 1 required TEXT missing" \
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
check_warns "unlink: --uid before bucket, missing --bucket"   22 "$ERR_EINVAL" "$WARN_UID_POS" -- \
  --uid testuser bucket unlink

check "unlink: stray after flags"              22 "ERROR: unexpected argument: 'strayarg'" \
  bucket unlink --bucket mybucket --uid testuser strayarg
check "unlink: stray before bucket"            22 "ERROR: unexpected argument: 'foo'" \
  foo bucket unlink --bucket mybucket --uid testuser
check "unlink: stray between bucket and unlink" 22 "ERROR: unexpected argument: 'extra'" \
  bucket extra unlink --bucket mybucket --uid testuser

check "unlink: unrecognized flag" 22 "ERROR: invalid flag --fakeflag" \
  bucket unlink --bucket mybucket --uid testuser --fakeflag

check "unlink: --bucket missing value" 114 "--bucket: 1 required TEXT missing" \
  bucket unlink --bucket
check "unlink: --uid missing value"    114 "--uid: 1 required TEXT missing" \
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

check "rm: stray after flags"              22 "ERROR: unexpected argument: 'strayarg'" \
  bucket rm --bucket mybucket strayarg
check "rm: stray before bucket"            22 "ERROR: unexpected argument: 'foo'" \
  foo bucket rm --bucket mybucket
check "rm: stray between bucket and rm"    22 "ERROR: unexpected argument: 'extra'" \
  bucket extra rm --bucket mybucket

check "rm: unrecognized flag" 22 "ERROR: invalid flag --fakeflag" \
  bucket rm --bucket mybucket --fakeflag

check "rm: --bucket missing value" 114 "--bucket: 1 required TEXT missing" \
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
check "remove: stray after flags"   22 "ERROR: unexpected argument: 'strayarg'" \
  bucket remove --bucket mybucket strayarg
check "remove: stray before bucket"             22 "ERROR: unexpected argument: 'foo'" \
  foo bucket remove --bucket mybucket
check "remove: stray between bucket and remove" 22 "ERROR: unexpected argument: 'extra'" \
  bucket extra remove --bucket mybucket
check "remove: unrecognized flag"   22 "ERROR: invalid flag --fakeflag" \
  bucket remove --bucket mybucket --fakeflag
check "remove: --bucket missing value" 114 "--bucket: 1 required TEXT missing" \
  bucket remove --bucket

# ============================================================
echo ""
echo "=== bucket check ==="
# ============================================================

check "check: stray after flags"              22 "ERROR: unexpected argument: 'strayarg'" \
  bucket check strayarg
check "check: stray before bucket"            22 "ERROR: unexpected argument: 'foo'" \
  foo bucket check
check "check: stray between bucket and check" 22 "ERROR: unexpected argument: 'extra'" \
  bucket extra check

check "check: unrecognized flag" 22 "ERROR: invalid flag --fakeflag" \
  bucket check --fakeflag

check "check: --bucket missing value" 114 "--bucket: 1 required TEXT missing" \
  bucket check --bucket
check "check: --max-concurrent-ios missing value" 114 "--max-concurrent-ios: 1 required INT missing" \
  bucket check --max-concurrent-ios

# --check-head-obj-locator without --bucket is caught in cli11_action (needs cluster)
check_cluster "check: --check-head-obj-locator without --bucket" 22 "ERROR: need to specify bucket name" -- \
  bucket check --check-head-obj-locator

# ============================================================
echo ""
echo "=== bucket check olh ==="
# ============================================================

check "check olh: stray after flags"               22 "ERROR: unexpected argument: 'strayarg'" \
  bucket check olh strayarg
check "check olh: stray before bucket"             22 "ERROR: unexpected argument: 'foo'" \
  foo bucket check olh
check "check olh: stray between bucket and check"  22 "ERROR: unexpected argument: 'extra'" \
  bucket extra check olh
check "check olh: stray between check and olh"     22 "ERROR: unexpected argument: 'extra'" \
  bucket check extra olh

check "check olh: unrecognized flag" 22 "ERROR: invalid flag --fakeflag" \
  bucket check olh --fakeflag
check "check olh: --max-concurrent-ios missing value" 114 "--max-concurrent-ios: 1 required INT missing" \
  bucket check olh --max-concurrent-ios
check "check olh: --bucket missing value"             114 "--bucket: 1 required TEXT missing" \
  bucket check olh --bucket

# ============================================================
echo ""
echo "=== bucket check unlinked ==="
# ============================================================

check "check unlinked: stray after flags"                22 "ERROR: unexpected argument: 'strayarg'" \
  bucket check unlinked strayarg
check "check unlinked: stray before bucket"              22 "ERROR: unexpected argument: 'foo'" \
  foo bucket check unlinked
check "check unlinked: stray between bucket and check"   22 "ERROR: unexpected argument: 'extra'" \
  bucket extra check unlinked
check "check unlinked: stray between check and unlinked" 22 "ERROR: unexpected argument: 'extra'" \
  bucket check extra unlinked

check "check unlinked: unrecognized flag" 22 "ERROR: invalid flag --fakeflag" \
  bucket check unlinked --fakeflag
check "check unlinked: --max-concurrent-ios missing value" 114 "--max-concurrent-ios: 1 required INT missing" \
  bucket check unlinked --max-concurrent-ios
check "check unlinked: --bucket missing value"             114 "--bucket: 1 required TEXT missing" \
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
check_warns "list: --tenant before bucket"       22 "ERROR: --tenant is set, but there's no user ID" "$WARN_TENANT_POS" -- \
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
check_warns "list: --tenant between bucket and list"   22 "ERROR: --tenant is set, but there's no user ID" "$WARN_TENANT_POS" -- \
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
check_warns "list: --uid before bucket"           254 "ERROR: could not find user" "$WARN_UID_POS" -- \
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
check_warns "list: duplicate --tenant cross level"  22 "ERROR: --tenant is set, but there's no user ID" "$WARN_TENANT_POS" "$WARN_TENANT_DUP" -- \
  --tenant foo bucket list --tenant bar

# ============================================================
echo ""
echo "=== bucket stats: wrong-position warnings (cluster) ==="
# ============================================================

check_warns "stats: --bucket before bucket"            2 "" "$WARN_BUCKET_POS" -- \
  --bucket nonexistent_cli11_test bucket stats
check_warns "stats: --tenant before bucket"            22 "ERROR: --tenant is set, but there's no user ID" "$WARN_TENANT_POS" -- \
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
check_warns "stats: --show-restore-stats + --tenant before (2 warns)" 22 \
  "ERROR: --tenant is set, but there's no user ID" \
  "$WARN_SHOWRESTORE_POS" "$WARN_TENANT_POS" -- \
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
check_warns "link: --uid before bucket (warns, then fails)"     2 "" "$WARN_UID_POS" -- \
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
check_warns "link: --tenant before bucket"           2 "" "$WARN_TENANT_POS" -- \
  --tenant foo bucket link --bucket nonexistent_cli11_test --uid testuser_cli11_test
check_warns "link: --bucket + --uid + --tenant before (3 pos warnings)" 2 "" \
  "$WARN_BUCKET_POS" "$WARN_UID_POS" "$WARN_TENANT_POS" -- \
  --bucket nonexistent_cli11_test --uid testuser_cli11_test --tenant foo bucket link

# ============================================================
echo ""
echo "=== bucket unlink: wrong-position warnings (cluster) ==="
# ============================================================

check_warns "unlink: --bucket before bucket (warns, then fails)"  2 "" "$WARN_BUCKET_POS" -- \
  --bucket nonexistent_cli11_test bucket unlink --uid testuser_cli11_test
check_warns "unlink: --uid before bucket (warns, then fails)"     2 "" "$WARN_UID_POS" -- \
  --uid testuser_cli11_test bucket unlink --bucket nonexistent_cli11_test
check_warns "unlink: duplicate --bucket"  2 "" "$WARN_BUCKET_DUP" -- \
  bucket unlink --bucket foo --bucket nonexistent_cli11_test --uid testuser_cli11_test
check_warns "unlink: --tenant before bucket"  2 "" "$WARN_TENANT_POS" -- \
  --tenant foo bucket unlink --bucket nonexistent_cli11_test --uid testuser_cli11_test
check_warns "unlink: duplicate --uid"         2 "" "$WARN_UID_DUP" -- \
  bucket unlink --uid foo --uid testuser_cli11_test --bucket nonexistent_cli11_test
check_warns "unlink: --bucket + --uid before (2 pos warnings)" 2 "" \
  "$WARN_BUCKET_POS" "$WARN_UID_POS" -- \
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
check_warns "rm: --tenant before bucket"                     22 "ERROR: --tenant is set, but there's no user ID" "$WARN_TENANT_POS" -- \
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
check_warns "rm: --bucket + --tenant + --purge-objects before (3 warns + exit 22)" 22 \
  "ERROR: --tenant is set, but there's no user ID" \
  "$WARN_BUCKET_POS" "$WARN_TENANT_POS" "$WARN_PURGE_POS" -- \
  --bucket nonexistent_cli11_test --tenant foo --purge-objects bucket rm
check_warns "rm: 4 pos warnings + inconsistent error" 1 "$ERR_INCONSISTENT" \
  "$WARN_BUCKET_POS" "$WARN_PURGE_POS" "$WARN_BYPASS_POS" "$WARN_INCONSISTENT_POS" -- \
  --bucket nonexistent_cli11_test --purge-objects --bypass-gc --inconsistent-index bucket rm
check_warns "rm: pos + duplicate + tenant (3 warns)" 22 \
  "ERROR: --tenant is set, but there's no user ID" \
  "$WARN_BUCKET_POS" "$WARN_BUCKET_DUP" "$WARN_TENANT_POS" -- \
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
check_warns "check: --tenant before bucket"                 22 "ERROR: --tenant is set, but there's no user ID" "$WARN_TENANT_POS" -- \
  --tenant foo bucket check
check_warns "check: duplicate --fix"                        0 "" "$WARN_FIX_DUP" -- \
  bucket check --fix --fix

# check multi-warning combinations
check_warns "check: --fix + --remove-bad before (2 pos warnings)"       0 "" \
  "$WARN_FIX_POS" "$WARN_REMOVE_BAD_POS" -- \
  --fix --remove-bad bucket check
check_warns "check: --fix + --remove-bad + --tenant before (3 warnings)" 22 \
  "ERROR: --tenant is set, but there's no user ID" \
  "$WARN_FIX_POS" "$WARN_REMOVE_BAD_POS" "$WARN_TENANT_POS" -- \
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
check_warns "check olh: --tenant before bucket"             22 "ERROR: --tenant is set, but there's no user ID" "$WARN_TENANT_POS" -- \
  --tenant foo bucket check olh
check_warns "check olh: --bucket + --tenant before (2 warns)" 22 "ERROR: --tenant is set, but there's no user ID" \
  "$WARN_BUCKET_POS" "$WARN_TENANT_POS" -- \
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

check_warns "check unlinked: --bucket before bucket"             0 "" "$WARN_BUCKET_POS" -- \
  --bucket nonexistent_cli11_test bucket check unlinked
check_warns "check unlinked: --bucket between bucket and check"  0 "" "$WARN_BUCKET_POS" -- \
  bucket --bucket nonexistent_cli11_test check unlinked
check_warns "check unlinked: --tenant before bucket"             22 "ERROR: --tenant is set, but there's no user ID" "$WARN_TENANT_POS" -- \
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
check_warns "remove: --tenant before bucket"           22 "ERROR: --tenant is set, but there's no user ID" "$WARN_TENANT_POS" -- \
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
check_cluster "functional: buckets check olh (alias, no --bucket)" 0 "" -- \
  buckets check olh
check_cluster "functional: buckets check unlinked (alias)"         0 "" -- \
  buckets check unlinked

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
      check_cluster "lifecycle: bucket list --allow-unordered" 0 "" -- \
        bucket list --allow-unordered --bucket "$_test_bucket"
      check_cluster "lifecycle: bucket list --format json" 0 "" -- \
        bucket list --format json --bucket "$_test_bucket"
      check_cluster "lifecycle: bucket stats --show-restore-stats" 0 "" -- \
        bucket stats --show-restore-stats --bucket "$_test_bucket"
      check_cluster "lifecycle: bucket stats --format json" 0 "" -- \
        bucket stats --format json --bucket "$_test_bucket"

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
      SKIP=$((SKIP+23))
    fi
  else
    echo "SKIP [integration: lifecycle tests]: aws CLI not available (needed to create test bucket)"
    SKIP=$((SKIP+23))
  fi

  # Cleanup: remove the test user
  "$RGW_ADMIN" user rm --uid "$_test_uid" --purge-data >/dev/null 2>&1
fi

# ============================================================
echo ""
echo "========================================"
echo "Results: $PASS passed, $FAIL failed, $SKIP skipped"
[ "$SKIP" -gt 0 ] && echo "(some tests require a running cluster or aws CLI)"
echo "========================================"
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
