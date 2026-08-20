#!/bin/bash
# Exit-code tests for radosgw-admin script commands
#
# Usage:
#   ./test-script-exit-codes.sh
#   RGW_ADMIN=/path/to/radosgw-admin ./test-script-exit-codes.sh
#   CEPH_CONF=/path/to/ceph.conf ./test-script-exit-codes.sh
#
# Test types:
#   check()        - no cluster needed; runs with --no-mon-config
#   check_cluster()- needs a running cluster, SKIPs when there is none
#
# Both verify the exit code, and when a non-empty message is given, that the
# output contains it as a substring.
#
# Run from the build directory:
#   cd /path/to/ceph/build && bash /path/to/test-script-exit-codes.sh

RGW_ADMIN="${RGW_ADMIN:-./bin/radosgw-admin}"
export CEPH_CONF="${CEPH_CONF:-./ceph.conf}"
# Keep ceph log lines off stderr so they cannot interleave with the messages
# the tests grep. rgw_global_init consumes --log-to-stderr before the flag loop runs.
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

# ============================================================
echo "=== script put ==="
# ============================================================

# missing required
check_cluster "put: missing --context"        22 "ERROR: context was not provided (via --context)" -- \
  script put --infile /dev/null
check_cluster "put: missing --infile"         22 "ERROR: infile was not provided (via --infile)" -- \
  script put --context prerequest
check_cluster "put: missing both"             22 "ERROR: context was not provided (via --context)" -- \
  script put

# missing option value
check "put: --context missing value" 1 "Option --context requires an argument." \
  script put --context
check "put: --infile missing value"  1 "Option --infile requires an argument." \
  script put --infile

# flag before script
check_cluster "put: --context before script"  0 "" -- \
  --context prerequest script put --infile /dev/null
check_cluster "put: --infile before script"   0 "" -- \
  --infile /dev/null script put --context prerequest
check_cluster "put: --tenant before script"   0 "" -- \
  --tenant mytenant script put --context prerequest --infile /dev/null

# flag between script and put
check_cluster "put: --context between script and put"  0 "" -- \
  script --context prerequest put --infile /dev/null
check_cluster "put: --infile between script and put"   0 "" -- \
  script --infile /dev/null put --context prerequest
check_cluster "put: --tenant between script and put"   0 "" -- \
  script --tenant mytenant put --context prerequest --infile /dev/null

# the same flag given twice, both after put
check_cluster "put: duplicate --context after put"  0 "" -- \
  script put --context prerequest --context background --infile /dev/null
check_cluster "put: duplicate --infile after put"   0 "" -- \
  script put --context prerequest --infile /dev/null --infile /dev/null
check_cluster "put: duplicate --tenant after put"   0 "" -- \
  script put --context prerequest --infile /dev/null --tenant foo --tenant bar

# once before script and again after put
check_cluster "put: --context before script and again after put"  0 "" -- \
  --context prerequest script put --context background --infile /dev/null
check_cluster "put: --infile before script and again after put"   0 "" -- \
  --infile /dev/null script put --context prerequest --infile /dev/null

# two flags duplicated at once, both after put
check_cluster "put: duplicate --infile and --tenant after put"  0 "" -- \
  script put --context prerequest --infile /dev/null --infile /dev/null --tenant foo --tenant bar

# two or three flags at once, before script and again after put
check_cluster "put: --context before script and again after put, --infile twice after put"  0 "" -- \
  --context prerequest script put --context background --infile /dev/null --infile /dev/null
check_cluster "put: --context, --infile and --tenant before script and again after put"  0 "" -- \
  --context prerequest --infile /dev/null --tenant foo script put --context prerequest --infile /dev/null --tenant bar

# the same shapes, with a missing or conflicting value
check_cluster "put: --context and --tenant before script and again after put, no --infile"  22 "ERROR: infile was not provided (via --infile)" -- \
  --context prerequest --tenant foo script put --context background --tenant bar
check_cluster "put: all three before script and again after put, --tenant with background context"  22 "ERROR: cannot specify tenant in background context" -- \
  --context background --infile /dev/null --tenant foo script put --context background --infile /dev/null --tenant bar

# stray positional args
check "put: stray after flags"           1 "Command not found: script put strayarg" \
  script put --context prerequest --infile /dev/null strayarg
check "put: stray before script"         1 "ERROR: Unrecognized argument: 'foo'" \
  foo script put --context prerequest --infile /dev/null
check "put: stray between script and put" 1 "ERROR: Unrecognized argument: 'extra'" \
  script extra put --context prerequest --infile /dev/null

# unrecognized flag
check "put: unrecognized flag"  22 "ERROR: invalid flag --fakeflag" \
  script put --context prerequest --infile /dev/null --fakeflag

# the message goes on to list the valid contexts, so it is matched up to the name
check_cluster "put: invalid context string" 22 "ERROR: invalid script context: invalid_ctx" -- \
  script put --context invalid_ctx --infile /dev/null

# invalid lua syntax
_bad_lua=$(mktemp /tmp/rgw_script_test_bad_XXXXXX.lua)
echo "this is not valid lua !!!" > "$_bad_lua"
check_cluster "put: invalid lua syntax" 22 "has error:" -- \
  script put --context prerequest --infile "$_bad_lua"
rm -f "$_bad_lua"

# non-existent infile
check_cluster "put: file not found" 2 "ERROR: failed to read script" -- \
  script put --context prerequest --infile /tmp/rgw_script_test_nonexistent_xyz

# background context with tenant
check_cluster "put: --tenant with background context" 22 \
  "ERROR: cannot specify tenant in background context" -- \
  script put --context background --tenant foo --infile /dev/null

# ============================================================
echo ""
echo "=== script get ==="
# ============================================================

# missing required
check_cluster "get: missing --context"        22 "ERROR: context was not provided (via --context)" -- \
  script get

# missing option value
check "get: --context missing value" 1 "Option --context requires an argument." \
  script get --context

# flag before script
check_cluster "get: --context before script"  0 "" -- \
  --context prerequest script get
check_cluster "get: --tenant before script"   0 "" -- \
  --tenant mytenant script get --context prerequest

# flag between script and get
check_cluster "get: --context between script and get"  0 "" -- \
  script --context prerequest get
check_cluster "get: --tenant between script and get"   0 "" -- \
  script --tenant mytenant get --context prerequest

# the same flag given twice, both after get
check_cluster "get: duplicate --context after get"  0 "" -- \
  script get --context prerequest --context background
check_cluster "get: duplicate --tenant after get"   0 "" -- \
  script get --context prerequest --tenant foo --tenant bar

# both flags duplicated after get: the asserted message names the second value of each
check_cluster "get: duplicate --context and --tenant after get"  0 "no script exists for context: background in tenant: bar" -- \
  script get --context prerequest --context background --tenant foo --tenant bar

# once before script and again after get
check_cluster "get: --context before script and again after get"  0 "" -- \
  --context prerequest script get --context background

# stray positional args
check "get: stray after flags"           1 "Command not found: script get strayarg" \
  script get --context prerequest strayarg
check "get: stray before script"         1 "ERROR: Unrecognized argument: 'foo'" \
  foo script get --context prerequest
check "get: stray between script and get" 1 "ERROR: Unrecognized argument: 'extra'" \
  script extra get --context prerequest
check "get: script twice in a row before get" 1 "ERROR: Unrecognized argument: 'script'" \
  script script get --context prerequest

# unrecognized flag
check "get: unrecognized flag"  22 "ERROR: invalid flag --fakeflag" \
  script get --context prerequest --fakeflag

# --infile is accepted here and makes no difference
check_cluster "get: --infile is accepted and ignored"  0 "" -- \
  script get --infile /dev/null --context prerequest

# the message goes on to list the valid contexts, so it is matched up to the name
check_cluster "get: invalid context string" 22 "ERROR: invalid script context: invalid_ctx" -- \
  script get --context invalid_ctx

# ============================================================
echo ""
echo "=== script rm ==="
# ============================================================

# missing required
check_cluster "rm: missing --context"         22 "ERROR: context was not provided (via --context)" -- \
  script rm

# missing option value
check "rm: --context missing value"  1 "Option --context requires an argument." \
  script rm --context

# flag before script
check_cluster "rm: --context before script"  0 "" -- \
  --context prerequest script rm
check_cluster "rm: --tenant before script"   0 "" -- \
  --tenant mytenant script rm --context prerequest

# flag between script and rm
check_cluster "rm: --context between script and rm"  0 "" -- \
  script --context prerequest rm
check_cluster "rm: --tenant between script and rm"   0 "" -- \
  script --tenant mytenant rm --context prerequest

# the same flag given twice, both after rm
check_cluster "rm: duplicate --context after rm"  0 "" -- \
  script rm --context prerequest --context background
check_cluster "rm: duplicate --tenant after rm"   0 "" -- \
  script rm --context prerequest --tenant foo --tenant bar

# once before script and again after rm
check_cluster "rm: --context before script and again after rm"  0 "" -- \
  --context prerequest script rm --context background
check_cluster "rm: --tenant before script and again after rm"   0 "" -- \
  --tenant foo script rm --context prerequest --tenant bar

# stray positional args
check "rm: stray after flags"           1 "Command not found: script rm strayarg" \
  script rm --context prerequest strayarg
check "rm: stray before script"         1 "ERROR: Unrecognized argument: 'foo'" \
  foo script rm --context prerequest
check "rm: stray between script and rm" 1 "ERROR: Unrecognized argument: 'extra'" \
  script extra rm --context prerequest

# unrecognized flag
check "rm: unrecognized flag"  22 "ERROR: invalid flag --fakeflag" \
  script rm --context prerequest --fakeflag

# the message goes on to list the valid contexts, so it is matched up to the name
check_cluster "rm: invalid context string" 22 "ERROR: invalid script context: invalid_ctx" -- \
  script rm --context invalid_ctx

# ============================================================
echo ""
echo "=== script remove (alias for rm) ==="
# ============================================================

# missing required
check_cluster "remove: missing --context"         22 "ERROR: context was not provided (via --context)" -- \
  script remove

# missing option value
check "remove: --context missing value"  1 "Option --context requires an argument." \
  script remove --context

# flag before script
check_cluster "remove: --context before script"  0 "" -- \
  --context prerequest script remove
check_cluster "remove: --tenant before script"   0 "" -- \
  --tenant mytenant script remove --context prerequest

# flag between script and remove
check_cluster "remove: --context between script and remove"  0 "" -- \
  script --context prerequest remove
check_cluster "remove: --tenant between script and remove"   0 "" -- \
  script --tenant mytenant remove --context prerequest

# the same flag given twice, both after remove
check_cluster "remove: duplicate --context after remove"  0 "" -- \
  script remove --context prerequest --context background
check_cluster "remove: duplicate --tenant after remove"   0 "" -- \
  script remove --context prerequest --tenant foo --tenant bar

# once before script and again after remove
check_cluster "remove: --context before script and again after remove"  0 "" -- \
  --context prerequest script remove --context background
check_cluster "remove: --tenant before script and again after remove"   0 "" -- \
  --tenant foo script remove --context prerequest --tenant bar

# stray positional args
check "remove: stray after flags"              1 "Command not found: script remove strayarg" \
  script remove --context prerequest strayarg
check "remove: stray before script"            1 "ERROR: Unrecognized argument: 'foo'" \
  foo script remove --context prerequest
check "remove: stray between script and remove" 1 "ERROR: Unrecognized argument: 'extra'" \
  script extra remove --context prerequest

# unrecognized flag
check "remove: unrecognized flag"  22 "ERROR: invalid flag --fakeflag" \
  script remove --context prerequest --fakeflag

# ============================================================
echo ""
echo "=== integration: put/get/rm full cycle ==="
# ============================================================
# Each row builds on the one before it: put a script, read it back, remove it,
# then read it again. Same cycle for the remove alias, a tenant, and background.

_script_file=$(mktemp /tmp/rgw_script_test_XXXXXX.lua)
_SCRIPT_CONTENT='function handle(input) return "script-test-ok" end'
echo "$_SCRIPT_CONTENT" > "$_script_file"

check_cluster "integration: put prerequest script"    0 "" -- \
  script put --context prerequest --infile "$_script_file"
check_cluster "integration: get prerequest script"    0 "$_SCRIPT_CONTENT" -- \
  script get --context prerequest
check_cluster "integration: get postrequest finds nothing"    0 "no script exists for context: postrequest" -- \
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
check_cluster "integration: get without tenant finds nothing"       0 "no script exists for context: prerequest" -- \
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
echo "=== script: no subcommand, and unrecognized command words ==="
# ============================================================

check "bare script"         1 'ERROR: Unknown command' script
check "unknown subcommand"  1 "ERROR: Unrecognized argument: 'banana'" script banana
check "reversed put script" 1 "ERROR: Unrecognized argument: 'put'" put script

# ============================================================
echo ""
echo "========================================"
echo "Results: $PASS passed, $FAIL failed, $SKIP skipped"
[ "$SKIP" -gt 0 ] && echo "(skipped tests require a running cluster)"
echo "========================================"
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
