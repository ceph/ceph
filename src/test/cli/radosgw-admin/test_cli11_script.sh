#!/bin/bash
# CLI11 migration tests for script put/get/rm/remove
#
# Usage:
#   ./test_cli11_script.sh
#   RGW_ADMIN=/path/to/radosgw-admin ./test_cli11_script.sh
#   CEPH_CONF=/path/to/ceph.conf ./test_cli11_script.sh
#
# Test types:
#   check()       - no cluster needed: verifies exit code + exact message
#   check_warns() - cluster needed: verifies warning messages appear before driver init
#                   usage: check_warns "desc" exit_code "error_msg_or_empty" "msg1" "msg2" ... -- command args
#   check_help()  - no cluster needed: verifies exit 0 for --cli11-help positions
#
# Run from the build directory:
#   cd /path/to/ceph/build && /path/to/test_cli11_script.sh

RGW_ADMIN="${RGW_ADMIN:-./bin/radosgw-admin}"
export CEPH_CONF="${CEPH_CONF:-./ceph.conf}"
PASS=0
FAIL=0
SKIP=0

# Full warning/error message constants
WARN_CTX_POS="Warning: --context should appear after the subcommand"
WARN_INFILE_POS="Warning: --infile should appear after the subcommand"
WARN_CTX_DUP="Warning: --context specified multiple times, using last value"
WARN_INFILE_DUP="Warning: --infile specified multiple times, using last value"
WARN_TENANT_DUP="Warning: --tenant specified multiple times, using last value"

ERR_CTX_MISSING="ERROR: context was not provided (via --context)"
ERR_INFILE_MISSING="ERROR: infile was not provided (via --infile)"
ERR_CTX_VALUE="--context: 1 required TEXT missing"
ERR_INFILE_VALUE="--infile: 1 required TEXT missing"
ERR_SUBCOMMAND="A subcommand is required"

# Filter out noisy ceph log lines (timestamped) and config-not-found lines
filter() {
  grep -v "^[0-9]\{4\}-" | \
  grep -v "^did not load config" | \
  grep -v "^unable to get monitor" | \
  grep -v "^failed to fetch mon config"
}

# --no-mon-config skips monitor connection so check()/check_help() run without a cluster.
# check_warns() still needs a cluster because warnings fire before driver init which hangs without one.
_run() { "$RGW_ADMIN" --no-mon-config "$@"; }

# Detect if a cluster is running (needed for check_warns tests)
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
# expected_exit: 0 if warnings only, non-zero if also an error
# error_msg: empty string if no error expected
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
# Runs against a real cluster. Skips if no cluster is running.
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

# ============================================================
echo "=== script put ==="
# ============================================================

# missing required
check "put: missing --context"        22 "$ERR_CTX_MISSING" \
  script put --infile /dev/null
check "put: missing --infile"         22 "$ERR_INFILE_MISSING" \
  script put --context prerequest
check "put: missing both"             22 "$ERR_CTX_MISSING" \
  script put

# missing option value
check "put: --context missing value" 1 "$ERR_CTX_VALUE" \
  script put --context
check "put: --infile missing value"  1 "$ERR_INFILE_VALUE" \
  script put --infile

# flag before subcommand (1 warning)
check_warns "put: --context before script"  0 "" "$WARN_CTX_POS" -- \
  --context prerequest script put --infile /dev/null
check_warns "put: --infile before script"   0 "" "$WARN_INFILE_POS" -- \
  --infile /dev/null script put --context prerequest
check_warns "put: --tenant before script"   0 "" -- \
  --tenant mytenant script put --context prerequest --infile /dev/null

# flag between script and put (1 warning)
check_warns "put: --context between script and put"  0 "" "$WARN_CTX_POS" -- \
  script --context prerequest put --infile /dev/null
check_warns "put: --infile between script and put"   0 "" "$WARN_INFILE_POS" -- \
  script --infile /dev/null put --context prerequest
check_warns "put: --tenant between script and put"   0 "" -- \
  script --tenant mytenant put --context prerequest --infile /dev/null

# duplicate flags same level (1 warning)
check_warns "put: duplicate --context same level"  0 "" "$WARN_CTX_DUP" -- \
  script put --context prerequest --context background --infile /dev/null
check_warns "put: duplicate --infile same level"   0 "" "$WARN_INFILE_DUP" -- \
  script put --context prerequest --infile /dev/null --infile /dev/null
check_warns "put: duplicate --tenant same level"   0 "" "$WARN_TENANT_DUP" -- \
  script put --context prerequest --infile /dev/null --tenant foo --tenant bar

# duplicate flags cross level (2 warnings: position + duplicate)
check_warns "put: duplicate --context cross level"  0 "" "$WARN_CTX_POS" "$WARN_CTX_DUP" -- \
  --context prerequest script put --context background --infile /dev/null
check_warns "put: duplicate --infile cross level"   0 "" "$WARN_INFILE_POS" "$WARN_INFILE_DUP" -- \
  --infile /dev/null script put --context prerequest --infile /dev/null

# duplicate --infile and --tenant same level (2 warnings)
check_warns "put: duplicate --infile and --tenant"  0 "" "$WARN_INFILE_DUP" "$WARN_TENANT_DUP" -- \
  script put --context prerequest --infile /dev/null --infile /dev/null --tenant foo --tenant bar

# 3 warnings: position + context dup + infile dup
check_warns "put: 3 warnings (position + context dup + infile dup)"  0 "" \
  "$WARN_CTX_POS" "$WARN_CTX_DUP" "$WARN_INFILE_DUP" -- \
  --context prerequest script put --context background --infile /dev/null --infile /dev/null

# 3 warnings + error: position + context dup + tenant dup + missing --infile
check_warns "put: 3 warnings + missing --infile"  22 "$ERR_INFILE_MISSING" \
  "$WARN_CTX_POS" "$WARN_CTX_DUP" "$WARN_TENANT_DUP" -- \
  --context prerequest --tenant foo script put --context background --tenant bar

# 4 warnings: position + context dup + infile dup + tenant dup
check_warns "put: 4 warnings (position + context dup + infile dup + tenant dup)"  0 "" \
  "$WARN_CTX_POS" "$WARN_INFILE_POS" "$WARN_CTX_DUP" "$WARN_INFILE_DUP" "$WARN_TENANT_DUP" -- \
  --context prerequest --infile /dev/null --tenant foo script put --context prerequest --infile /dev/null --tenant bar

# 4 warnings + error: position + context dup + infile dup + tenant dup + background+tenant conflict
check_warns "put: 4 warnings + background+tenant error"  22 "ERROR: cannot specify tenant in background context" \
  "$WARN_CTX_POS" "$WARN_INFILE_POS" "$WARN_CTX_DUP" "$WARN_INFILE_DUP" "$WARN_TENANT_DUP" -- \
  --context background --infile /dev/null --tenant foo script put --context background --infile /dev/null --tenant bar

# stray positional args
check "put: stray after flags"           1 "ERROR: unexpected argument: 'strayarg'" \
  script put --context prerequest --infile /dev/null strayarg
check "put: stray before script"         1 "ERROR: unexpected argument: 'foo'" \
  foo script put --context prerequest --infile /dev/null
check "put: stray between script and put" 1 "ERROR: unexpected argument: 'extra'" \
  script extra put --context prerequest --infile /dev/null

# unrecognized flag
check "put: unrecognized flag"  22 "ERROR: invalid flag --fakeflag" \
  script put --context prerequest --infile /dev/null --fakeflag

# ============================================================
echo ""
echo "=== script get ==="
# ============================================================

# missing required
check "get: missing --context"        22 "$ERR_CTX_MISSING" \
  script get

# missing option value
check "get: --context missing value" 1 "$ERR_CTX_VALUE" \
  script get --context

# flag before subcommand (1 warning)
check_warns "get: --context before script"  0 "" "$WARN_CTX_POS" -- \
  --context prerequest script get
check_warns "get: --tenant before script"   0 "" -- \
  --tenant mytenant script get --context prerequest

# flag between script and get (1 warning)
check_warns "get: --context between script and get"  0 "" "$WARN_CTX_POS" -- \
  script --context prerequest get
check_warns "get: --tenant between script and get"   0 "" -- \
  script --tenant mytenant get --context prerequest

# duplicate flags same level (1 warning)
check_warns "get: duplicate --context same level"  0 "" "$WARN_CTX_DUP" -- \
  script get --context prerequest --context background
check_warns "get: duplicate --tenant same level"   0 "" "$WARN_TENANT_DUP" -- \
  script get --context prerequest --tenant foo --tenant bar

# duplicate --context and --tenant same level (2 warnings)
check_warns "get: duplicate --context and --tenant"  0 "" "$WARN_CTX_DUP" "$WARN_TENANT_DUP" -- \
  script get --context prerequest --context background --tenant foo --tenant bar

# duplicate --context cross level (2 warnings: position + duplicate)
check_warns "get: duplicate --context cross level"  0 "" "$WARN_CTX_POS" "$WARN_CTX_DUP" -- \
  --context prerequest script get --context background

# stray positional args
check "get: stray after flags"           1 "ERROR: unexpected argument: 'strayarg'" \
  script get --context prerequest strayarg
check "get: stray before script"         1 "ERROR: unexpected argument: 'foo'" \
  foo script get --context prerequest
check "get: stray between script and get" 1 "ERROR: unexpected argument: 'extra'" \
  script extra get --context prerequest

# unrecognized flag
check "get: unrecognized flag"  22 "ERROR: invalid flag --fakeflag" \
  script get --context prerequest --fakeflag

# --infile not registered on get: swallowed via fallthrough + warned (backward compat)
check_warns "get: --infile swallowed+warned (not registered)"  0 "" \
  "Warning: --infile is not a valid option for 'script get'" -- \
  script get --infile /dev/null --context prerequest

# ============================================================
echo ""
echo "=== script rm ==="
# ============================================================

# missing required
check "rm: missing --context"         22 "$ERR_CTX_MISSING" \
  script rm

# missing option value
check "rm: --context missing value"  1 "$ERR_CTX_VALUE" \
  script rm --context

# flag before subcommand (1 warning)
check_warns "rm: --context before script"  0 "" "$WARN_CTX_POS" -- \
  --context prerequest script rm
check_warns "rm: --tenant before script"   0 "" -- \
  --tenant mytenant script rm --context prerequest

# flag between script and rm (1 warning)
check_warns "rm: --context between script and rm"  0 "" "$WARN_CTX_POS" -- \
  script --context prerequest rm
check_warns "rm: --tenant between script and rm"   0 "" -- \
  script --tenant mytenant rm --context prerequest

# duplicate flags same level (1 warning)
check_warns "rm: duplicate --context same level"  0 "" "$WARN_CTX_DUP" -- \
  script rm --context prerequest --context background
check_warns "rm: duplicate --tenant same level"   0 "" "$WARN_TENANT_DUP" -- \
  script rm --context prerequest --tenant foo --tenant bar

# duplicate cross level (2 warnings: position + duplicate)
check_warns "rm: duplicate --context cross level"  0 "" "$WARN_CTX_POS" "$WARN_CTX_DUP" -- \
  --context prerequest script rm --context background
check_warns "rm: duplicate --tenant cross level"   0 "" "$WARN_TENANT_DUP" -- \
  --tenant foo script rm --context prerequest --tenant bar

# stray positional args
check "rm: stray after flags"           1 "ERROR: unexpected argument: 'strayarg'" \
  script rm --context prerequest strayarg
check "rm: stray before script"         1 "ERROR: unexpected argument: 'foo'" \
  foo script rm --context prerequest
check "rm: stray between script and rm" 1 "ERROR: unexpected argument: 'extra'" \
  script extra rm --context prerequest

# unrecognized flag
check "rm: unrecognized flag"  22 "ERROR: invalid flag --fakeflag" \
  script rm --context prerequest --fakeflag

# ============================================================
echo ""
echo "=== script remove (alias for rm) ==="
# ============================================================

# missing required
check "remove: missing --context"         22 "$ERR_CTX_MISSING" \
  script remove

# missing option value
check "remove: --context missing value"  1 "$ERR_CTX_VALUE" \
  script remove --context

# flag before subcommand (1 warning)
check_warns "remove: --context before script"  0 "" "$WARN_CTX_POS" -- \
  --context prerequest script remove
check_warns "remove: --tenant before script"   0 "" -- \
  --tenant mytenant script remove --context prerequest

# flag between script and remove (1 warning)
check_warns "remove: --context between script and remove"  0 "" "$WARN_CTX_POS" -- \
  script --context prerequest remove
check_warns "remove: --tenant between script and remove"   0 "" -- \
  script --tenant mytenant remove --context prerequest

# duplicate flags same level (1 warning)
check_warns "remove: duplicate --context same level"  0 "" "$WARN_CTX_DUP" -- \
  script remove --context prerequest --context background
check_warns "remove: duplicate --tenant same level"   0 "" "$WARN_TENANT_DUP" -- \
  script remove --context prerequest --tenant foo --tenant bar

# duplicate cross level (2 warnings: position + duplicate)
check_warns "remove: duplicate --context cross level"  0 "" "$WARN_CTX_POS" "$WARN_CTX_DUP" -- \
  --context prerequest script remove --context background
check_warns "remove: duplicate --tenant cross level"   0 "" "$WARN_TENANT_DUP" -- \
  --tenant foo script remove --context prerequest --tenant bar

# stray positional args
check "remove: stray after flags"              1 "ERROR: unexpected argument: 'strayarg'" \
  script remove --context prerequest strayarg
check "remove: stray before script"            1 "ERROR: unexpected argument: 'foo'" \
  foo script remove --context prerequest
check "remove: stray between script and remove" 1 "ERROR: unexpected argument: 'extra'" \
  script extra remove --context prerequest

# unrecognized flag
check "remove: unrecognized flag"  22 "ERROR: invalid flag --fakeflag" \
  script remove --context prerequest --fakeflag

# ============================================================
echo ""
echo "=== script put: callback validation errors (no cluster needed) ==="
# ============================================================

# invalid context string
check "put: invalid context string" 22 "ERROR: invalid script context: invalid_ctx" \
  script put --context invalid_ctx --infile /dev/null

# ============================================================
echo ""
echo "=== script get: callback validation errors (no cluster needed) ==="
# ============================================================

check "get: invalid context string" 22 "ERROR: invalid script context: invalid_ctx" \
  script get --context invalid_ctx

# ============================================================
echo ""
echo "=== script rm: callback validation errors (no cluster needed) ==="
# ============================================================

check "rm: invalid context string" 22 "ERROR: invalid script context: invalid_ctx" \
  script rm --context invalid_ctx

# invalid lua syntax
_bad_lua=$(mktemp /tmp/bad_lua_XXXXXX.lua)
echo "this is not valid lua !!!" > "$_bad_lua"
check "put: invalid lua syntax" 22 "has error:" \
  script put --context prerequest --infile "$_bad_lua"
rm -f "$_bad_lua"

# non-existent infile
check "put: file not found" 2 "ERROR: failed to read script" \
  script put --context prerequest --infile /tmp/cli11_test_nonexistent_xyz

# background context with tenant
check "put: background context with tenant" 22 \
  "ERROR: cannot specify tenant in background context" \
  script put --context background --tenant foo --infile /dev/null

# ============================================================
echo ""
echo "=== integration: put/get/rm full cycle ==="
# ============================================================

_script_file=$(mktemp /tmp/cli11_test_XXXXXX.lua)
_SCRIPT_CONTENT='function handle(input) return "cli11-test-ok" end'
echo "$_SCRIPT_CONTENT" > "$_script_file"

check_cluster "integration: put prerequest script"    0 "" -- \
  script put --context prerequest --infile "$_script_file"
check_cluster "integration: get prerequest script"    0 "$_SCRIPT_CONTENT" -- \
  script get --context prerequest
check_cluster "integration: get different context"    0 "no script exists for context: postrequest" -- \
  script get --context postrequest
check_cluster "integration: rm prerequest script"     0 "" -- \
  script rm --context prerequest
check_cluster "integration: get after rm"             0 "no script exists for context: prerequest" -- \
  script get --context prerequest
check_cluster "integration: rm non-existent (silent)" 0 "" -- \
  script rm --context prerequest

# same cycle using remove alias
check_cluster "integration: put for remove alias"     0 "" -- \
  script put --context prerequest --infile "$_script_file"
check_cluster "integration: get before remove"        0 "$_SCRIPT_CONTENT" -- \
  script get --context prerequest
check_cluster "integration: remove prerequest script" 0 "" -- \
  script remove --context prerequest
check_cluster "integration: get after remove"         0 "no script exists for context: prerequest" -- \
  script get --context prerequest
check_cluster "integration: remove non-existent (silent)" 0 "" -- \
  script remove --context prerequest

# tenant isolation
check_cluster "integration: put with tenant"          0 "" -- \
  script put --context prerequest --tenant testenant --infile "$_script_file"
check_cluster "integration: get with same tenant"     0 "$_SCRIPT_CONTENT" -- \
  script get --context prerequest --tenant testenant
check_cluster "integration: get without tenant"       0 "no script exists for context: prerequest" -- \
  script get --context prerequest
check_cluster "integration: rm with tenant"           0 "" -- \
  script rm --context prerequest --tenant testenant
check_cluster "integration: get after tenant rm"      0 "no script exists for context: prerequest" -- \
  script get --context prerequest --tenant testenant

# background context (no tenant allowed)
check_cluster "integration: put background script"    0 "" -- \
  script put --context background --infile "$_script_file"
check_cluster "integration: get background script"    0 "$_SCRIPT_CONTENT" -- \
  script get --context background
check_cluster "integration: rm background script"     0 "" -- \
  script rm --context background
check_cluster "integration: get after background rm"  0 "no script exists for context: background" -- \
  script get --context background

rm -f "$_script_file"

# ============================================================
echo ""
echo "=== CLI11 rejection ==="
# ============================================================

check "bare script"         1 "$ERR_SUBCOMMAND" script
check "unknown subcommand"  1 "$ERR_SUBCOMMAND" script banana
check "reversed put script" 1 "$ERR_SUBCOMMAND" put script

# ============================================================
echo ""
echo "=== --cli11-help positions ==="
# ============================================================

check_help "cli11-help root"             --cli11-help
check_help "cli11-help before script"    --cli11-help script
check_help "cli11-help after script"     script --cli11-help
check_help "cli11-help script put"       --cli11-help script put
check_help "cli11-help put after script" script --cli11-help put
check_help "cli11-help after put"        script put --cli11-help
check_help "cli11-help script get"       --cli11-help script get
check_help "cli11-help get after script" script --cli11-help get
check_help "cli11-help after get"        script get --cli11-help
check_help "cli11-help script rm"        --cli11-help script rm
check_help "cli11-help rm after script"  script --cli11-help rm
check_help "cli11-help after rm"         script rm --cli11-help

# ============================================================
echo ""
echo "========================================"
echo "Results: $PASS passed, $FAIL failed, $SKIP skipped"
[ "$SKIP" -gt 0 ] && echo "(skipped tests require a running cluster)"
echo "========================================"
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
