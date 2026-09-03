#!/bin/bash
# Exit-code tests for radosgw-admin bucket commands
#
# Usage:
#   ./test-bucket-exit-codes.sh
#   RGW_ADMIN=/path/to/radosgw-admin ./test-bucket-exit-codes.sh
#   CEPH_CONF=/path/to/ceph.conf ./test-bucket-exit-codes.sh
#
# Test types:
#   check()        - no cluster needed; runs with --no-mon-config
#   check_cluster()- needs a running cluster, SKIPs when there is none
#
# Both verify the exit code.
#
# Run from the build directory:
#   cd /path/to/ceph/build && bash /path/to/test-bucket-exit-codes.sh

RGW_ADMIN="${RGW_ADMIN:-./bin/radosgw-admin}"
export CEPH_CONF="${CEPH_CONF:-./ceph.conf}"
# Route dout/derr log lines off stderr so async cluster logs (e.g. "ERROR:
# obj.oid is empty") can't interleave mid-line with the output printed when a test fails.
# rgw_global_init consumes --log-to-stderr before the flags are read.
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
echo "=== bucket (bare) ==="
# ============================================================

# no arguments at all: the usage text, before any command is resolved
check "no arguments" 1
check "bare bucket" 1 bucket
check "bare buckets (alias)" 1 buckets
check "unknown subcommand" 1 bucket banana

# ============================================================
echo ""
echo "=== buckets alias (non-list commands) ==="
# ============================================================

# 'buckets' is only accepted for list. Every other subcommand is unrecognized.
# When a line also has a bad flag, the flag error comes first (exit 22).
check "buckets stats: stray arg" 1 buckets stats strayarg
check "buckets rm: unrecognized subcommand" 1 buckets rm
check "buckets link: unrecognized subcommand" 1 buckets link --uid testuser
check "buckets unlink: unrecognized subcommand" 1 buckets unlink --uid testuser
check "buckets check: stray arg" 1 buckets check strayarg
check "buckets list: unrecognized flag" 22 buckets list --fakeflag
check "buckets link: stray after flags" 1 buckets link --bucket mybucket --uid testuser strayarg
check "buckets link: unrecognized flag" 22 buckets link --bucket mybucket --uid testuser --fakeflag
check "buckets unlink: stray after flags" 1 buckets unlink --bucket mybucket --uid testuser strayarg
check "buckets unlink: unrecognized flag" 22 buckets unlink --bucket mybucket --uid testuser --fakeflag
check "buckets rm: stray after flags" 1 buckets rm --bucket mybucket strayarg
check "buckets rm: unrecognized flag" 22 buckets rm --bucket mybucket --fakeflag
check "buckets check: unrecognized flag" 22 buckets check --fakeflag

# ============================================================
echo ""
echo "=== bucket list ==="
# ============================================================

# stray positional args
check "list: stray after flags" 1 bucket list strayarg
check "list: stray before bucket" 1 foo bucket list
check "list: stray between bucket and list" 1 bucket extra list

# unrecognized flag
check "list: unrecognized flag" 22 bucket list --fakeflag

# missing option value
# Leaving the value out is the way to check that an option is defined as
# taking a value: a value-taking option should reject the command when its
# value is missing. These rows also verify that each option is defined for
# the command where it is expected to be valid. The same option therefore
# gets a row under each command where it is expected to be valid. In CLI11,
# for example, an option that is not registered for the command may result
# in an error or a warning.
check "list: --bucket missing value" 1 bucket list --bucket
check "list: --uid missing value" 1 bucket list --uid
check "list: --bucket-id missing value" 1 bucket list --bucket-id
check "list: --format missing value" 1 bucket list --format
check "list: --max-entries missing value" 1 bucket list --max-entries
# out-of-int-range value rejected by the strict base-10 setter (strict_strtol's
# int range check)
check "list: --max-entries out of int range" 22 bucket list --max-entries 5000000000
check "list: --marker missing value" 1 bucket list --marker
check "list: --object-version missing value" 1 bucket list --object-version

# ============================================================
echo ""
echo "=== flags: underscore vs dash spelling ==="
# ============================================================
# Long option names accept '_' as well as '-'. Each pair below proves the
# underscore spelling behaves identically to the dash spelling, in both the
# space form (value is the next token) and the '=' form (value glued on).

# space form: the value reaches the int check either way
check "list: --max-entries space form (dash)" 22 bucket list --max-entries banana
check "list: --max_entries space form (underscore)" 22 bucket list --max_entries banana

# '=' form
check "list: --max-entries= form (dash)" 22 bucket list --max-entries=banana
check "list: --max_entries= form (underscore)" 22 bucket list --max_entries=banana

# both spellings give the identical error, so both are accepted. --bucket_id is
# itself a real flag, but here it is taken as a value and quoted verbatim
check "list: --max-entries=--bucket_id (dash, flag-shaped value preserved)" 22 bucket list --max-entries=--bucket_id
check "list: --max_entries=--bucket_id (underscore, flag-shaped value preserved)" 22 bucket list --max_entries=--bucket_id
check "list: --max-entries --bucket_id (dash, space-form value preserved)" 22 bucket list --max-entries --bucket_id
check "list: --max_entries --bucket_id (underscore, space-form value preserved)" 22 bucket list --max_entries --bucket_id

# A flag-shaped token is taken as the flag's VALUE, not parsed as a flag:
# --bucket reaches bucket init (exit 2) rather than being left without a value
# (exit 1).
check_cluster "list: --bucket=--hello_world (flag-shaped value taken)" 2 -- bucket list --bucket=--hello_world
check_cluster "list: --bucket --hello_world (flag-shaped value taken, space form)" 2 -- bucket list --bucket --hello_world
check_cluster "list: -b=--hello_world (short flag, glued value taken)" 2 -- bucket list -b=--hello_world

# --bucket-id/--bucket_id: both spellings are accepted, and the
# flag-shaped token is taken as its VALUE — exit 0, not the exit 1 the flag
# would give if that token were parsed as a flag, leaving it without a value.
# Without --bucket the id is ignored, so the listing runs and its content says
# nothing about the id; only the exit code carries the result.
check_cluster "list: --bucket-id=--hello_world (dash, flag-shaped value taken)" 0 -- bucket list --bucket-id=--hello_world
check_cluster "list: --bucket_id=--hello_world (underscore, flag-shaped value taken)" 0 -- bucket list --bucket_id=--hello_world
check_cluster "list: --bucket-id --hello_world (dash, space form, value taken)" 0 -- bucket list --bucket-id --hello_world
check_cluster "list: --bucket_id --hello_world (underscore, space form, value taken)" 0 -- bucket list --bucket_id --hello_world

# an unknown flag is quoted in the error exactly as the user typed it;
# it is rejected by name before its value is looked at
check "list: unrecognized underscore flag rejected by name" 22 bucket list --banana_flag 1

# valueless (binary) flag: underscore and dash spellings both succeed
check_cluster "rm: --purge-objects (dash)" 0 -- bucket rm --bucket=no-such-bucket --purge-objects
check_cluster "rm: --purge_objects (underscore)" 0 -- bucket rm --bucket=no-such-bucket --purge_objects

# Underscore spellings are accepted silently; the command still fails on the
# integer value, so it runs cluster-free.
check "list: underscore --bucket_id repeated, --max_entries invalid" 22 \
  bucket list --bucket_id x --bucket_id y --max_entries banana

check "list: --max_entries given twice, last value invalid" 22 \
  bucket list --max_entries 1 --max_entries banana

# The two tests above hit the parse-error (failure) path. This one hits the
# success path: the underscore spelling parses and the command runs
check_cluster "list: underscore spelling on success path" 0 -- bucket list --max_entries 100

# ============================================================
echo ""
echo "=== bucket stats ==="
# ============================================================

check "stats: stray after flags" 1 bucket stats strayarg
check "stats: stray before bucket" 1 foo bucket stats
check "stats: stray between bucket and stats" 1 bucket extra stats

check "stats: unrecognized flag" 22 bucket stats --fakeflag

check "stats: --bucket missing value" 1 bucket stats --bucket
check "stats: --bucket-id missing value" 1 bucket stats --bucket-id
check "stats: --format missing value" 1 bucket stats --format
check "stats: --max-entries missing value" 1 bucket stats --max-entries
check "stats: --marker missing value" 1 bucket stats --marker

# ============================================================
echo ""
echo "=== bucket layout ==="
# ============================================================

check "layout: stray after flags" 1 bucket layout strayarg
check "layout: stray before bucket" 1 foo bucket layout
check "layout: stray between bucket and layout" 1 bucket extra layout

check "layout: unrecognized flag" 22 bucket layout --fakeflag

check "layout: --bucket missing value" 1 bucket layout --bucket
check "layout: --bucket-id missing value" 1 bucket layout --bucket-id
check "layout: --tenant missing value" 1 bucket layout --tenant
check "layout: --format missing value" 1 bucket layout --format

# handler-level (cluster): bucket_name.empty() is checked inside the action,
# nonexistent bucket fails init_bucket silently with exit 2
check_cluster "layout: missing --bucket" 22 -- bucket layout
check_cluster "layout: nonexistent bucket (silent exit 2)" 2 -- bucket layout --bucket no-such-bucket

# flags before the leaf subcommand. --bucket/--bucket-id/--format fail on the
# nonexistent bucket (exit 2, no message); --tenant trips the global
# "no user ID" check (exit 22) before reaching the bucket.
check_cluster "layout: --bucket before subcommand (silent exit 2)" 2 -- bucket --bucket no-such-bucket layout
check_cluster "layout: --bucket-id before subcommand (silent exit 2)" 2 -- bucket --bucket-id x layout --bucket no-such-bucket
check_cluster "layout: --format before subcommand (silent exit 2)" 2 -- bucket --format json layout --bucket no-such-bucket
check_cluster "layout: --tenant before subcommand" 22 -- bucket --tenant t layout --bucket no-such-bucket

# the same flag given twice
check_cluster "layout: duplicate --bucket (silent exit 2)" 2 -- bucket layout --bucket a --bucket no-such-bucket
check_cluster "layout: duplicate --bucket-id (silent exit 2)" 2 -- bucket layout --bucket-id a --bucket-id b --bucket no-such-bucket
check_cluster "layout: duplicate --format (silent exit 2)" 2 -- bucket layout --format json --format json --bucket no-such-bucket
check_cluster "layout: duplicate --tenant" 22 -- bucket layout --tenant a --tenant b --bucket no-such-bucket

# ============================================================
echo ""
echo "=== bucket chown ==="
# ============================================================

# stray positional args
check "chown: stray after flags" 1 bucket chown strayarg
check "chown: stray before bucket" 1 foo bucket chown
check "chown: stray between bucket and chown" 1 bucket extra chown

check "chown: unrecognized flag" 22 bucket chown --fakeflag

# missing option value
check "chown: --bucket missing value" 1 bucket chown --bucket
check "chown: --uid missing value" 1 bucket chown --uid
check "chown: --marker missing value" 1 bucket chown --marker
check "chown: --tenant missing value" 1 bucket chown --tenant
check "chown: --bucket-new-name missing value" 1 bucket chown --bucket-new-name

# --bucket-id is NOT a chown option (the handler never read it); the flag is
# swallowed and its value trips the stray-positional check (exit 22).
check_cluster "chown: --bucket-id swallowed (not a chown option)" 22 -- bucket chown --bucket-id x

# handler-level (cluster): bucket_name.empty() is checked inside the action
# (note the "bucket name not specified" wording differs from layout); a
# nonexistent bucket fails RGWBucket::init with exit 2
check_cluster "chown: missing --bucket" 22 -- bucket chown
check_cluster "chown: nonexistent bucket (exit 2)" 2 -- bucket chown --bucket no-such-bucket --uid no_such_user

# flags before the leaf subcommand; all fail on the nonexistent bucket (exit 2).
# --uid is supplied, so --tenant does NOT trip the global "no user ID" check here.
check_cluster "chown: --bucket before subcommand" 2 -- bucket --bucket no-such-bucket chown --uid no_such_user
check_cluster "chown: --uid before subcommand" 2 -- bucket --uid no_such_user chown --bucket no-such-bucket
check_cluster "chown: --marker before subcommand" 2 -- bucket --marker m chown --bucket no-such-bucket --uid no_such_user
check_cluster "chown: --tenant before subcommand" 2 -- bucket --tenant t chown --bucket no-such-bucket --uid no_such_user
check_cluster "chown: --bucket-new-name before subcommand" 2 -- bucket --bucket-new-name nn chown --bucket no-such-bucket --uid no_such_user

# the same flag given twice
check_cluster "chown: duplicate --bucket" 2 -- bucket chown --bucket a --bucket no-such-bucket --uid no_such_user
check_cluster "chown: duplicate --uid" 2 -- bucket chown --uid a --uid no_such_user --bucket no-such-bucket

# ============================================================
echo ""
echo "=== bucket limit check ==="
# ============================================================

# 'bucket limit' is an internal node: it requires the 'check' subcommand
check "limit (incomplete command)" 1 bucket limit

check "limit check: stray after" 1 bucket limit check strayarg

check "limit check: unrecognized flag" 22 bucket limit check --fakeflag

check "limit check: --uid missing value" 1 bucket limit check --uid

# handler-level (cluster): no --uid iterates all users; all paths exit 0
check_cluster "limit check: no args (all users)" 0 -- bucket limit check
check_cluster "limit check: --warnings-only" 0 -- bucket limit check --warnings-only
check_cluster "limit check: nonexistent --uid (empty listing, exit 0)" 0 -- bucket limit check --uid no_such_user

# flags before the 'check' leaf; all paths exit 0
check_cluster "limit check: --uid before subcommand" 0 -- bucket --uid no_such_user limit check
check_cluster "limit check: --warnings-only before subcommand" 0 -- bucket --warnings-only limit check

# the same flag given twice
check_cluster "limit check: duplicate --uid" 0 -- bucket limit check --uid a --uid no_such_user

# ============================================================
echo ""
echo "=== bucket logging (info/list/flush) ==="
# ============================================================

# 'bucket logging' is not a command on its own; it needs a subcommand.
# Missing subcommand or unknown subcommand, both fail.
check "logging (incomplete command)" 1 bucket logging
check "logging: unknown subcommand" 1 bucket logging banana

# stray positional args
check "logging info: stray after" 1 bucket logging info strayarg
check "logging list: stray after" 1 bucket logging list strayarg
check "logging flush: stray after" 1 bucket logging flush strayarg
check "logging info: stray before" 1 foo bucket logging info

# unrecognized flag
check "logging info: unrecognized flag" 22 bucket logging info --fakeflag
check "logging list: unrecognized flag" 22 bucket logging list --fakeflag
check "logging flush: unrecognized flag" 22 bucket logging flush --fakeflag

# missing option value (flush has no --format)
check "logging info: --bucket missing value" 1 bucket logging info --bucket
check "logging info: --bucket-id missing value" 1 bucket logging info --bucket-id
check "logging info: --tenant missing value" 1 bucket logging info --tenant
check "logging info: --format missing value" 1 bucket logging info --format
check "logging list: --format missing value" 1 bucket logging list --format
check "logging flush: --bucket missing value" 1 bucket logging flush --bucket

# handler-level (cluster): bucket_name.empty() is checked inside the action;
# a nonexistent bucket fails init_bucket silently with exit 2
check_cluster "logging info: missing --bucket" 22 -- bucket logging info
check_cluster "logging list: missing --bucket" 22 -- bucket logging list
check_cluster "logging flush: missing --bucket" 22 -- bucket logging flush
check_cluster "logging info: nonexistent bucket (silent exit 2)" 2 -- bucket logging info --bucket no-such-bucket
check_cluster "logging list: nonexistent bucket (silent exit 2)" 2 -- bucket logging list --bucket no-such-bucket
check_cluster "logging flush: nonexistent bucket (silent exit 2)" 2 -- bucket logging flush --bucket no-such-bucket

# flags before the leaf subcommand. --bucket/--bucket-id/--format fail on the
# nonexistent bucket (exit 2, no message); --tenant trips the global
# "no user ID" check (exit 22) before reaching the bucket.
check_cluster "logging info: --bucket before subcommand (silent exit 2)" 2 -- bucket --bucket no-such-bucket logging info
check_cluster "logging info: --bucket-id before subcommand (silent exit 2)" 2 -- bucket --bucket-id x logging info --bucket no-such-bucket
check_cluster "logging list: --format before subcommand (silent exit 2)" 2 -- bucket --format json logging list --bucket no-such-bucket
check_cluster "logging info: --tenant before subcommand" 22 -- bucket --tenant t logging info --bucket no-such-bucket

# the same flag given twice
check_cluster "logging info: duplicate --bucket (silent exit 2)" 2 -- bucket logging info --bucket a --bucket no-such-bucket
check_cluster "logging list: duplicate --tenant" 22 -- bucket logging list --tenant a --tenant b --bucket no-such-bucket

# ============================================================
echo ""
echo "=== bucket rewrite ==="
# ============================================================

# stray positional args
check "rewrite: stray after flags" 1 bucket rewrite strayarg
check "rewrite: stray before bucket" 1 foo bucket rewrite
check "rewrite: stray between bucket and rewrite" 1 bucket extra rewrite

check "rewrite: unrecognized flag" 22 bucket rewrite --fakeflag

# missing option value (parse-level, exit 1). The size flags behave like any
# other text option here; the date flags name whichever spelling was used.
check "rewrite: --bucket missing value" 1 bucket rewrite --bucket
check "rewrite: --bucket-id missing value" 1 bucket rewrite --bucket-id
check "rewrite: --tenant missing value" 1 bucket rewrite --tenant
check "rewrite: --format missing value" 1 bucket rewrite --format
check "rewrite: --start-date missing value" 1 bucket rewrite --start-date
check "rewrite: --start-time missing value (alias)" 1 bucket rewrite --start-time
check "rewrite: --end-date missing value" 1 bucket rewrite --end-date
check "rewrite: --end-time missing value (alias)" 1 bucket rewrite --end-time
check "rewrite: --min-rewrite-size missing value" 1 bucket rewrite --min-rewrite-size
check "rewrite: --max-rewrite-size missing value" 1 bucket rewrite --max-rewrite-size
check "rewrite: --min-rewrite-stripe-size missing value" 1 bucket rewrite --min-rewrite-stripe-size

# handler-level (cluster): bucket_name.empty() is checked inside the action;
# a nonexistent bucket fails init_bucket with exit 2
check_cluster "rewrite: missing --bucket" 22 -- bucket rewrite
check_cluster "rewrite: nonexistent bucket (exit 2)" 2 -- bucket rewrite --bucket no-such-bucket

# the size flags use atoll, so a malformed value is ACCEPTED at parse (not
# rejected the way a strict integer would be) and quietly becomes 0. We assert
# acceptance by reaching init_bucket (exit 2), not a parse error (exit 22).
check_cluster "rewrite: --min-rewrite-size=abc accepted (atoll)" 2 -- bucket rewrite --bucket no-such-bucket --min-rewrite-size=abc
check_cluster "rewrite: --max-rewrite-size=abc accepted (atoll)" 2 -- bucket rewrite --bucket no-such-bucket --max-rewrite-size=abc
check_cluster "rewrite: --min-rewrite-stripe-size=abc accepted (atoll)" 2 -- bucket rewrite --bucket no-such-bucket --min-rewrite-stripe-size=abc

# flags before the leaf subcommand; all fail on the nonexistent bucket (exit 2).
# --tenant trips the global "no user ID" check (exit 22) before reaching the bucket.
check_cluster "rewrite: --bucket before subcommand" 2 -- bucket --bucket no-such-bucket rewrite
check_cluster "rewrite: --bucket-id before subcommand" 2 -- bucket --bucket-id x rewrite --bucket no-such-bucket
check_cluster "rewrite: --format before subcommand" 2 -- bucket --format json rewrite --bucket no-such-bucket
check_cluster "rewrite: --start-date before subcommand" 2 -- bucket --start-date 2020-01-01 rewrite --bucket no-such-bucket
check_cluster "rewrite: --end-date before subcommand" 2 -- bucket --end-date 2020-01-01 rewrite --bucket no-such-bucket
check_cluster "rewrite: --min-rewrite-size before subcommand" 2 -- bucket --min-rewrite-size 1 rewrite --bucket no-such-bucket
check_cluster "rewrite: --max-rewrite-size before subcommand" 2 -- bucket --max-rewrite-size 1 rewrite --bucket no-such-bucket
check_cluster "rewrite: --min-rewrite-stripe-size before subcommand" 2 -- bucket --min-rewrite-stripe-size 1 rewrite --bucket no-such-bucket
check_cluster "rewrite: --tenant before subcommand" 22 -- bucket --tenant t rewrite --bucket no-such-bucket

# the same flag given twice
check_cluster "rewrite: duplicate --bucket" 2 -- bucket rewrite --bucket a --bucket no-such-bucket
check_cluster "rewrite: duplicate --start-date" 2 -- bucket rewrite --start-date 2020-01-01 --start-date 2021-01-01 --bucket no-such-bucket
check_cluster "rewrite: duplicate --min-rewrite-size" 2 -- bucket rewrite --min-rewrite-size 1 --min-rewrite-size 2 --bucket no-such-bucket
check_cluster "rewrite: duplicate --tenant" 22 -- bucket rewrite --tenant a --tenant b --bucket no-such-bucket

# two or three flags at once: before the subcommand, or before and duplicated
check_cluster "rewrite: --bucket + --min-rewrite-size before" 2 -- bucket --bucket no-such-bucket --min-rewrite-size 1 rewrite
check_cluster "rewrite: pos + duplicate --bucket" 2 -- bucket --bucket a rewrite --bucket no-such-bucket
check_cluster "rewrite: --start-date + --end-date + --tenant before" 22 -- bucket --start-date 2020-01-01 --end-date 2021-01-01 --tenant t rewrite --bucket no-such-bucket

# ============================================================
echo ""
echo "=== bucket set-min-shards ==="
# ============================================================

# stray positional args
check "set-min-shards: stray after flags" 1 bucket set-min-shards strayarg
check "set-min-shards: stray before bucket" 1 foo bucket set-min-shards
check "set-min-shards: stray between bucket and leaf" 1 bucket extra set-min-shards

check "set-min-shards: unrecognized flag" 22 bucket set-min-shards --fakeflag
# Unrelated flags are parsed and ignored whatever their type: the command
# proceeds and fails for its own reason (here, no --bucket). The exit code is
# 234 because -EINVAL comes back negative, as the block below spells out.
check_cluster "set-min-shards: unrelated --max-entries 5 swallowed (space form)" 234 -- bucket set-min-shards --max-entries 5

# missing option value (parse-level, exit 1)
check "set-min-shards: --bucket missing value" 1 bucket set-min-shards --bucket
check "set-min-shards: --bucket-id missing value" 1 bucket set-min-shards --bucket-id
check "set-min-shards: --tenant missing value" 1 bucket set-min-shards --tenant
check "set-min-shards: --num-shards missing value" 1 bucket set-min-shards --num-shards
# --num-shards is parsed as an integer; a non-numeric value is rejected
check "set-min-shards: --num-shards non-integer" 22 bucket set-min-shards --num-shards abc

# handler-level (cluster): these validations run after driver init.
# Order: bucket empty -> num-shards specified -> num-shards >= 1.
# Each returns -EINVAL (shell exit 234).
check_cluster "set-min-shards: missing --bucket" 234 -- bucket set-min-shards --num-shards 11
check_cluster "set-min-shards: --num-shards not specified" 234 -- bucket set-min-shards --bucket no-such-bucket
check_cluster "set-min-shards: --num-shards < 1" 234 -- bucket set-min-shards --bucket no-such-bucket --num-shards 0
# valid args but nonexistent bucket: init_bucket fails (exit 2, no message)
check_cluster "set-min-shards: nonexistent bucket (silent exit 2)" 2 -- bucket set-min-shards --bucket no-such-bucket --num-shards 11
# The three unrelated-flag cases side by side (identical args, only the flag
# differs): a binary flag, a value option in =form, and the same option in
# space form. All three are ignored, so all three exit 2 on the missing bucket.
check_cluster "set-min-shards: unrelated binary flag --fix accepted (silent exit 2)" 2 -- bucket set-min-shards --fix --bucket no-such-bucket --num-shards 11
check_cluster "set-min-shards: unrelated value flag --max-entries=5 (=form, exit 2) (silent exit 2)" 2 -- bucket set-min-shards --max-entries=5 --bucket no-such-bucket --num-shards 11
check_cluster "set-min-shards: unrelated --max-entries 5 swallowed (space form, +bucket+num-shards) (silent exit 2)" 2 -- bucket set-min-shards --max-entries 5 --bucket no-such-bucket --num-shards 11

# flags before the leaf subcommand. The value still reaches the command, so with
# a valid --num-shards and a nonexistent bucket they fail at init_bucket
# (exit 2, no message). --tenant trips the global "no user ID" check
# (exit 22).
check_cluster "set-min-shards: --bucket before subcommand (silent exit 2)" 2 -- bucket --bucket no-such-bucket set-min-shards --num-shards 11
check_cluster "set-min-shards: -b before subcommand (short) (silent exit 2)" 2 -- bucket -b no-such-bucket set-min-shards --num-shards 11
check_cluster "set-min-shards: --bucket-id before subcommand (silent exit 2)" 2 -- bucket --bucket-id x set-min-shards --bucket no-such-bucket --num-shards 11
check_cluster "set-min-shards: --num-shards before subcommand (silent exit 2)" 2 -- bucket --num-shards 11 set-min-shards --bucket no-such-bucket
check_cluster "set-min-shards: --tenant before subcommand" 22 -- bucket --tenant t set-min-shards --bucket no-such-bucket --num-shards 11

# the same flag given twice
check_cluster "set-min-shards: duplicate --bucket (silent exit 2)" 2 -- bucket set-min-shards --bucket a --bucket no-such-bucket --num-shards 11
check_cluster "set-min-shards: duplicate --num-shards (silent exit 2)" 2 -- bucket set-min-shards --bucket no-such-bucket --num-shards 11 --num-shards 12
check_cluster "set-min-shards: duplicate --tenant" 22 -- bucket set-min-shards --tenant a --tenant b --bucket no-such-bucket --num-shards 11

# two or three flags at once: before the subcommand, or before and duplicated
check_cluster "set-min-shards: --bucket + --num-shards before (silent exit 2)" 2 -- bucket --bucket no-such-bucket --num-shards 11 set-min-shards
check_cluster "set-min-shards: pos + duplicate --bucket (silent exit 2)" 2 -- bucket --bucket a set-min-shards --bucket no-such-bucket --num-shards 11
check_cluster "set-min-shards: --bucket + --num-shards + --tenant before" 22 -- bucket --bucket no-such-bucket --num-shards 11 --tenant t set-min-shards

# ============================================================
echo ""
echo "=== bucket object shard ==="
# ============================================================

# stray positional args
check "object shard: stray after flags" 1 bucket object shard stray
check "object shard: stray before bucket" 1 foo bucket object shard
check "object shard: stray between object and shard" 1 bucket object extra shard
check "object shard: stray word after leaf (banana)" 1 bucket object shard banana

# 'bucket object' is not a command on its own; it needs a subcommand.
# Missing subcommand or unknown subcommand, both fail.
check "object: unknown subcommand (banana)" 1 bucket object banana
check "object: no subcommand" 1 bucket object

check "object shard: unrecognized flag" 22 bucket object shard --fakeflag
# --max-entries is a real flag that this command does not use; it is parsed and ignored
check_cluster "object shard: unrelated --max-entries 5 swallowed (space form)" 0 -- bucket object shard --object foo --num-shards 11 --max-entries 5

# missing option value (parse-level, exit 1)
check "object shard: --object missing value" 1 bucket object shard --object
check "object shard: --num-shards missing value" 1 bucket object shard --num-shards
# --num-shards is parsed as an integer; a non-numeric value is rejected
check "object shard: --num-shards non-integer" 22 bucket object shard --object foo --num-shards abc

# strict base-10 parsing: a leading 0 does not switch to octal ("010" = 10,
# "08" = 8) and hex is rejected.
# Object "bar" maps to shard 8 of 10 and shard 4 of 8.
check "object shard: --num-shards hex rejected" 22 bucket object shard --object bar --num-shards 0x10
check_cluster "object shard: --num-shards 010 parses as decimal 10" 0 -- bucket object shard --object bar --num-shards 010
check_cluster "object shard: --num-shards 08 parses as 8" 0 -- bucket object shard --object bar --num-shards 08
check_cluster "object shard: --num-shards 010 before subcommand (base-10)" 0 -- bucket --num-shards 010 object shard --object bar

# handler-level (cluster): these validations run after driver init. The handler
# returns a positive EINVAL (shell exit 22) - note this differs from
# set-min-shards' -EINVAL/234.
check_cluster "object shard: missing object (only --num-shards)" 22 -- bucket object shard --num-shards 11
check_cluster "object shard: missing num-shards (only --object)" 22 -- bucket object shard --object foo
check_cluster "object shard: non-positive num-shards" 22 -- bucket object shard --object foo --num-shards 0

# unrelated flags alongside valid args: a binary flag (--fix) takes 0 values ->
# accepted; a value option in =form binds its value -> accepted; both still
# compute the shard (foo % 11 -> 10).
check_cluster "object shard: unrelated binary flag --fix accepted" 0 -- bucket object shard --object foo --num-shards 11 --fix
check_cluster "object shard: unrelated value flag --max-entries=5 (=form)" 0 -- bucket object shard --object foo --num-shards 11 --max-entries=5

# flags before the leaf subcommand. The value still reaches the command, so the
# shard is still computed (exit 0).
check_cluster "object shard: --object before subcommand" 0 -- bucket --object foo object shard --num-shards 11
check_cluster "object shard: -o before subcommand (short)" 0 -- bucket -o foo object shard --num-shards 11
check_cluster "object shard: --num-shards before subcommand" 0 -- bucket --num-shards 11 object shard --object foo
check_cluster "object shard: --format before subcommand" 0 -- bucket --format xml object shard --object foo --num-shards 11

# the same flag given twice
check_cluster "object shard: duplicate --object" 0 -- bucket object shard --object a --object foo --num-shards 11
check_cluster "object shard: duplicate --num-shards" 0 -- bucket object shard --object foo --num-shards 4 --num-shards 11

# two or three flags at once: before the subcommand, or before and duplicated
check_cluster "object shard: --object + --num-shards before" 0 -- bucket --object foo --num-shards 11 object shard
check_cluster "object shard: pos + duplicate --object" 0 -- bucket --object a object shard --object foo --num-shards 11
check_cluster "object shard: --object + --num-shards + --format before" 0 -- bucket --object foo --num-shards 11 --format xml object shard

# ============================================================
echo ""
echo "=== bucket shard objects ==="
# ============================================================

# stray positional args
check "shard objects: stray after flags" 1 bucket shard objects stray
check "shard objects: stray before bucket" 1 foo bucket shard objects
check "shard objects: stray between shard and objects" 1 bucket shard extra objects
check "shard objects: stray word after leaf (banana)" 1 bucket shard objects banana

# 'bucket shard' is not a command on its own; it needs a subcommand.
# Missing subcommand or unknown subcommand, both fail.
check "shard: unknown subcommand (banana)" 1 bucket shard banana
check "shard: no subcommand" 1 bucket shard

check "shard objects: unrecognized flag" 22 bucket shard objects --fakeflag
# --max-entries is a real flag that this command does not use; it is parsed and ignored
check_cluster "shard objects: unrelated --max-entries 5 swallowed (space form)" 0 -- bucket shard objects --num-shards 4 --max-entries 5

# missing option value (parse-level, exit 1)
check "shard objects: --num-shards missing value" 1 bucket shard objects --num-shards
check "shard objects: --shard-id missing value" 1 bucket shard objects --shard-id
check "shard objects: --prefix missing value" 1 bucket shard objects --prefix
# --num-shards / --shard-id are parsed as integers; a non-numeric value is rejected
check "shard objects: --num-shards non-integer" 22 bucket shard objects --num-shards abc
check "shard objects: --shard-id non-integer" 22 bucket shard objects --shard-id abc --num-shards 4

# handler-level (cluster): these validations run after driver init. The handler
# returns exit 22.
check_cluster "shard objects: missing --num-shards" 22 -- bucket shard objects
check_cluster "shard objects: --shard-id >= num-shards" 22 -- bucket shard objects --num-shards 4 --shard-id 5

# unrelated flags alongside valid args: binary flag (--fix) takes 0 values ->
# accepted; value option in =form binds -> accepted; both still list objs.
check_cluster "shard objects: unrelated binary flag --fix accepted" 0 -- bucket shard objects --num-shards 4 --fix
check_cluster "shard objects: unrelated value flag --max-entries=5 (=form)" 0 -- bucket shard objects --num-shards 4 --max-entries=5

# flags before the leaf subcommand. The value still reaches the command, so the
# objects are still listed (exit 0).
check_cluster "shard objects: --num-shards before subcommand" 0 -- bucket --num-shards 4 shard objects
check_cluster "shard objects: --shard-id before subcommand" 0 -- bucket --shard-id 1 shard objects --num-shards 4
check_cluster "shard objects: --prefix before subcommand" 0 -- bucket --prefix myobj shard objects --num-shards 4
check_cluster "shard objects: --format before subcommand" 0 -- bucket --format xml shard objects --num-shards 4

# the same flag given twice
check_cluster "shard objects: duplicate --num-shards" 0 -- bucket shard objects --num-shards 4 --num-shards 8
check_cluster "shard objects: duplicate --shard-id" 0 -- bucket shard objects --num-shards 4 --shard-id 0 --shard-id 1
check_cluster "shard objects: duplicate --prefix" 0 -- bucket shard objects --num-shards 4 --prefix a --prefix myobj

# two or three flags at once: before the subcommand, or before and duplicated
check_cluster "shard objects: --num-shards + --shard-id before" 0 -- bucket --num-shards 4 --shard-id 1 shard objects
check_cluster "shard objects: --num-shards + --shard-id + --prefix before" 0 -- bucket --num-shards 4 --shard-id 1 --prefix myobj shard objects

# ============================================================
echo ""
echo "=== bucket resync encrypted multipart ==="
# ============================================================

# stray positional args
check "resync: stray after leaf (banana)" 1 bucket resync encrypted multipart banana
check "resync: stray before bucket" 1 foo bucket resync encrypted multipart
check "resync: stray between resync and encrypted" 1 bucket resync x encrypted multipart
check "resync: stray between encrypted and multipart" 1 bucket resync encrypted x multipart

# 'bucket resync' and 'bucket resync encrypted' are not commands on their own;
# they need a subcommand. Missing subcommand or unknown subcommand, both fail.
check "resync: unknown subcommand (banana)" 1 bucket resync banana
check "resync encrypted: unknown subcommand (banana)" 1 bucket resync encrypted banana
check "resync: no subcommand" 1 bucket resync

check "resync: unrecognized flag" 22 bucket resync encrypted multipart --fakeflag
# --max-entries is a real flag that this command does not use; it is parsed and ignored
check_cluster "resync: unrelated --max-entries 5 swallowed (space form) (silent exit 2)" 2 -- bucket resync encrypted multipart --bucket chk --max-entries 5 --yes-i-really-mean-it

# missing option value (parse-level, exit 1)
check "resync: --bucket missing value" 1 bucket resync encrypted multipart --bucket
check "resync: --bucket-id missing value" 1 bucket resync encrypted multipart --bucket-id
check "resync: --tenant missing value" 1 bucket resync encrypted multipart --tenant
check "resync: --marker missing value" 1 bucket resync encrypted multipart --marker

# in the space form the binary flag takes no value, so a non-bool word is left
# on the line and read as a command word
check "resync: --yes-i-really-mean-it banana (space form, non-bool)" 1 bucket resync encrypted multipart --bucket no-such-bucket --yes-i-really-mean-it banana

# handler-level (cluster). empty bucket -> exit 22. Real-bucket EPERM and
# success cases live in the integration section (need a bucket that exists).
check_cluster "resync: bucket not specified" 22 -- bucket resync encrypted multipart
# valid args, nonexistent bucket: init_bucket fails (exit 2, no message)
check_cluster "resync: nonexistent bucket (silent exit 2)" 2 -- bucket resync encrypted multipart --bucket no-such-bucket --yes-i-really-mean-it
# unrelated flags alongside valid args: a binary flag (--fix) takes 0 values ->
# accepted; a value option in =form binds -> accepted; both proceed to init_bucket
# (which fails on the nonexistent bucket, exit 2) rather than being rejected at parse.
check_cluster "resync: unrelated binary flag --fix accepted (silent exit 2)" 2 -- bucket resync encrypted multipart --fix --bucket no-such-bucket --yes-i-really-mean-it
check_cluster "resync: unrelated value flag --max-entries=5 (=form, exit 2) (silent exit 2)" 2 -- bucket resync encrypted multipart --max-entries=5 --bucket no-such-bucket --yes-i-really-mean-it

# flags before the leaf. The value still reaches the command, so with a
# nonexistent bucket they fail at init_bucket (exit 2, no message).
# --tenant trips the global "no user ID" check (exit 22).
check_cluster "resync: --bucket before subcommand (silent exit 2)" 2 -- bucket --bucket no-such-bucket resync encrypted multipart --yes-i-really-mean-it
check_cluster "resync: -b before subcommand (short) (silent exit 2)" 2 -- bucket -b no-such-bucket resync encrypted multipart --yes-i-really-mean-it
check_cluster "resync: --bucket-id before subcommand (silent exit 2)" 2 -- bucket --bucket-id x resync encrypted multipart --bucket no-such-bucket --yes-i-really-mean-it
check_cluster "resync: --marker before subcommand (silent exit 2)" 2 -- bucket --marker m resync encrypted multipart --bucket no-such-bucket --yes-i-really-mean-it
check_cluster "resync: --yes-i-really-mean-it before subcommand (silent exit 2)" 2 -- bucket --yes-i-really-mean-it resync encrypted multipart --bucket no-such-bucket
check_cluster "resync: --format before subcommand (silent exit 2)" 2 -- bucket --format json resync encrypted multipart --bucket no-such-bucket --yes-i-really-mean-it
check_cluster "resync: --tenant before subcommand" 22 -- bucket --tenant t resync encrypted multipart --bucket no-such-bucket --yes-i-really-mean-it

# the same flag given twice
check_cluster "resync: duplicate --bucket (silent exit 2)" 2 -- bucket resync encrypted multipart --bucket a --bucket no-such-bucket --yes-i-really-mean-it
check_cluster "resync: duplicate --marker (silent exit 2)" 2 -- bucket resync encrypted multipart --bucket no-such-bucket --marker a --marker b --yes-i-really-mean-it
check_cluster "resync: duplicate --yes-i-really-mean-it (silent exit 2)" 2 -- bucket resync encrypted multipart --bucket no-such-bucket --yes-i-really-mean-it --yes-i-really-mean-it
check_cluster "resync: duplicate --tenant" 22 -- bucket resync encrypted multipart --tenant a --tenant b --bucket no-such-bucket --yes-i-really-mean-it

# two or three flags at once: before the subcommand, or before and duplicated
check_cluster "resync: --bucket + --marker before (silent exit 2)" 2 -- bucket --bucket no-such-bucket --marker m resync encrypted multipart --yes-i-really-mean-it
check_cluster "resync: --bucket + --marker + --yes-i-really-mean-it before (silent exit 2)" 2 -- bucket --bucket no-such-bucket --marker m --yes-i-really-mean-it resync encrypted multipart

# ============================================================
echo ""
echo "=== bucket radoslist (+ 'bucket rados list' alias) ==="
# ============================================================
# 'bucket radoslist' and 'bucket rados list' are two entry points to the same
# command. The radoslist block below is the full coverage; the rados-list block
# after it confirms the second spelling behaves identically.

# stray positional args
check "radoslist: stray after leaf (banana)" 1 bucket radoslist banana
check "radoslist: stray before bucket" 1 foo bucket radoslist

check "radoslist: unrecognized flag" 22 bucket radoslist --fakeflag
# --max-entries is a real flag that this command does not use; it is parsed and ignored
check_cluster "radoslist: unrelated --max-entries 5 swallowed (space form)" 0 -- bucket radoslist --bucket chk --max-entries 5

# missing option value (parse-level, exit 1)
check "radoslist: --bucket missing value" 1 bucket radoslist --bucket
check "radoslist: --tenant missing value" 1 bucket radoslist --tenant
check "radoslist: --max-concurrent-ios missing value" 1 bucket radoslist --max-concurrent-ios
check "radoslist: --orphan-stale-secs missing value" 1 bucket radoslist --orphan-stale-secs
check "radoslist: --rgw-obj-fs missing value" 1 bucket radoslist --rgw-obj-fs
# --max-concurrent-ios (int) / --orphan-stale-secs (uint) are parsed as numbers;
# a non-numeric value is rejected
check "radoslist: --max-concurrent-ios non-integer" 22 bucket radoslist --max-concurrent-ios abc
check "radoslist: --orphan-stale-secs non-integer" 22 bucket radoslist --orphan-stale-secs abc
# a negative value parses fine and wraps when cast to uint64, so it is accepted
check_cluster "radoslist: --orphan-stale-secs -5 accepted (negative value wraps)" 0 -- bucket radoslist --bucket chk --orphan-stale-secs -5
# --orphan-stale-secs is always read as base 10, like --max-concurrent-ios above.
# "0x10" is rejected; "08" is accepted, which octal would not be.
check "radoslist: --orphan-stale-secs hex rejected" 22 bucket radoslist --orphan-stale-secs 0x10
check_cluster "radoslist: --orphan-stale-secs 08 accepted (not octal)" 0 -- bucket radoslist --bucket chk --orphan-stale-secs 08
check "radoslist: --orphan-stale-secs out of range" 22 bucket radoslist --orphan-stale-secs 99999999999999999999

# cluster: readonly command, lists rados objects backing the bucket (exit 0).
check_cluster "radoslist: --bucket (lists, exit 0)" 0 -- bucket radoslist --bucket chk
# unrelated flags alongside valid args: binary flag (--fix) takes 0 values ->
# accepted; value option in =form binds -> accepted; both still exit 0.
check_cluster "radoslist: unrelated binary flag --fix accepted" 0 -- bucket radoslist --bucket chk --fix
check_cluster "radoslist: unrelated value flag --max-entries=5 (=form)" 0 -- bucket radoslist --bucket chk --max-entries=5

# flags before the leaf subcommand. The value still reaches the command, so
# radoslist still runs (exit 0). --tenant trips the global "no user ID" check
# (exit 22).
check_cluster "radoslist: --bucket before subcommand" 0 -- bucket --bucket chk radoslist
check_cluster "radoslist: -b before subcommand (short)" 0 -- bucket -b chk radoslist
check_cluster "radoslist: --max-concurrent-ios before subcommand" 0 -- bucket --max-concurrent-ios 16 radoslist --bucket chk
check_cluster "radoslist: --orphan-stale-secs before subcommand" 0 -- bucket --orphan-stale-secs 100 radoslist --bucket chk
check_cluster "radoslist: --rgw-obj-fs before subcommand" 0 -- bucket --rgw-obj-fs ":" radoslist --bucket chk
check_cluster "radoslist: --yes-i-really-mean-it before subcommand" 0 -- bucket --yes-i-really-mean-it radoslist --bucket chk
check_cluster "radoslist: --tenant before subcommand" 22 -- bucket --tenant t radoslist --bucket chk

# the same flag given twice
check_cluster "radoslist: duplicate --bucket" 0 -- bucket radoslist --bucket a --bucket chk
check_cluster "radoslist: duplicate --max-concurrent-ios" 0 -- bucket radoslist --bucket chk --max-concurrent-ios 8 --max-concurrent-ios 16
check_cluster "radoslist: duplicate --orphan-stale-secs" 0 -- bucket radoslist --bucket chk --orphan-stale-secs 1 --orphan-stale-secs 2
check_cluster "radoslist: duplicate --rgw-obj-fs" 0 -- bucket radoslist --bucket chk --rgw-obj-fs a --rgw-obj-fs b

# two or three flags at once: before the subcommand, or before and duplicated
check_cluster "radoslist: --bucket + --max-concurrent-ios before" 0 -- bucket --bucket chk --max-concurrent-ios 16 radoslist
check_cluster "radoslist: --bucket + --max-concurrent-ios + --orphan-stale-secs before" 0 -- bucket --bucket chk --max-concurrent-ios 16 --orphan-stale-secs 100 radoslist

# ---- 'bucket rados list' alias: same command via the rados node ----
# stray + nesting under the 'rados' node
check "rados list: stray after leaf (banana)" 1 bucket rados list banana
check "rados list: stray between rados and list" 1 bucket rados extra list
# 'bucket rados' is not a command on its own; it needs a subcommand.
# Missing subcommand or unknown subcommand, both fail.
check "rados: unknown subcommand (banana)" 1 bucket rados banana
check "rados: no subcommand" 1 bucket rados
# a bad integer, a flag before the subcommand, a duplicate, and the working case
check "rados list: --max-concurrent-ios non-integer" 22 bucket rados list --max-concurrent-ios abc
check_cluster "rados list: --bucket before subcommand" 0 -- bucket --bucket chk rados list
check_cluster "rados list: duplicate --rgw-obj-fs" 0 -- bucket rados list --bucket chk --rgw-obj-fs a --rgw-obj-fs b
check_cluster "rados list: --bucket (lists, exit 0)" 0 -- bucket rados list --bucket chk

# ============================================================
echo ""
echo "=== bucket link ==="
# ============================================================

# the missing flag is not caught up front; it fails inside the op, so a cluster
# is needed to see it. The op checks for an empty user id before fetching bucket info.
check_cluster "link: missing --bucket" 22 -- bucket link --uid testuser
check_cluster "link: missing --uid" 22 -- bucket link --bucket mybucket
check_cluster "link: missing both" 22 -- bucket link

# out of position and missing a required flag: the op layer reports the error
check_cluster "link: --bucket before bucket, missing --uid" 22 -- --bucket mybucket bucket link
check_cluster "link: --uid before bucket, missing --bucket" 22 -- --uid testuser bucket link
check_cluster "link: --bucket + --uid before bucket (fails on nonexistent)" 2 -- --bucket mybucket --uid testuser bucket link

# stray positional args
check "link: stray after flags" 1 bucket link --bucket mybucket --uid testuser strayarg
check "link: stray before bucket" 1 foo bucket link --bucket mybucket --uid testuser
check "link: stray between bucket and link" 1 bucket extra link --bucket mybucket --uid testuser

# unrecognized flag
check "link: unrecognized flag" 22 bucket link --bucket mybucket --uid testuser --fakeflag

check "link: --bucket missing value" 1 bucket link --bucket
check "link: --uid missing value" 1 bucket link --uid
check "link: --bucket-id missing value" 1 bucket link --bucket-id
check "link: --bucket-new-name missing value" 1 bucket link --bucket-new-name

# ============================================================
echo ""
echo "=== bucket unlink ==="
# ============================================================

# the missing flag is not caught up front; it fails inside the op, so a
# cluster is needed to see it
check_cluster "unlink: missing --bucket" 22 -- bucket unlink --uid testuser
check_cluster "unlink: missing --uid" 22 -- bucket unlink --bucket mybucket
check_cluster "unlink: missing both" 22 -- bucket unlink

check_cluster "unlink: --bucket before bucket, missing --uid" 22 -- --bucket mybucket bucket unlink
check_cluster "unlink: --uid before bucket, missing --bucket" 22 -- --uid testuser bucket unlink

check "unlink: stray after flags" 1 bucket unlink --bucket mybucket --uid testuser strayarg
check "unlink: stray before bucket" 1 foo bucket unlink --bucket mybucket --uid testuser
check "unlink: stray between bucket and unlink" 1 bucket extra unlink --bucket mybucket --uid testuser

check "unlink: unrecognized flag" 22 bucket unlink --bucket mybucket --uid testuser --fakeflag

check "unlink: --bucket missing value" 1 bucket unlink --bucket
check "unlink: --uid missing value" 1 bucket unlink --uid

# ============================================================
echo ""
echo "=== bucket rm ==="
# ============================================================

# rm ignores the op's return value, so missing --bucket silently exits 0;
# a cluster is needed to reach the op
check_cluster "rm: missing --bucket (silent exit 0)" 0 -- bucket rm
check_cluster "rm: --purge-objects before bucket, missing --bucket (silent exit 0)" 0 -- --purge-objects bucket rm

check "rm: stray after flags" 1 bucket rm --bucket mybucket strayarg
check "rm: stray before bucket" 1 foo bucket rm --bucket mybucket
check "rm: stray between bucket and rm" 1 bucket extra rm --bucket mybucket

check "rm: unrecognized flag" 22 bucket rm --bucket mybucket --fakeflag

check "rm: --bucket missing value" 1 bucket rm --bucket

# the message spells the flag with an underscore, unlike the flag itself
check_cluster "rm: --inconsistent-index without --yes-i-really-mean-it" 1 -- bucket rm --bucket nonexistent_test --inconsistent-index

# ============================================================
echo ""
echo "=== bucket rm (remove alias) ==="
# ============================================================

check_cluster "remove: missing --bucket (silent exit 0)" 0 -- bucket remove
check "remove: stray after flags" 1 bucket remove --bucket mybucket strayarg
check "remove: stray before bucket" 1 foo bucket remove --bucket mybucket
check "remove: stray between bucket and remove" 1 bucket extra remove --bucket mybucket
check "remove: unrecognized flag" 22 bucket remove --bucket mybucket --fakeflag
check "remove: --bucket missing value" 1 bucket remove --bucket

# ============================================================
echo ""
echo "=== bucket check ==="
# ============================================================

check "check: stray after flags" 1 bucket check strayarg
check "check: stray before bucket" 1 foo bucket check
check "check: stray between bucket and check" 1 bucket extra check

check "check: unrecognized flag" 22 bucket check --fakeflag

check "check: --bucket missing value" 1 bucket check --bucket
check "check: --max-concurrent-ios missing value" 1 bucket check --max-concurrent-ios

# --check-head-obj-locator without --bucket is caught after driver init (needs cluster)
check_cluster "check: --check-head-obj-locator without --bucket" 22 -- bucket check --check-head-obj-locator

# ============================================================
echo ""
echo "=== bucket check olh ==="
# ============================================================

check "check olh: stray after flags" 1 bucket check olh strayarg
check "check olh: stray before bucket" 1 foo bucket check olh
check "check olh: stray between bucket and check" 1 bucket extra check olh
check "check olh: stray between check and olh" 1 bucket check extra olh

check "check olh: unrecognized flag" 22 bucket check olh --fakeflag
check "check olh: --max-concurrent-ios missing value" 1 bucket check olh --max-concurrent-ios
check "check olh: --bucket missing value" 1 bucket check olh --bucket

# ============================================================
echo ""
echo "=== bucket check unlinked ==="
# ============================================================

check "check unlinked: stray after flags" 1 bucket check unlinked strayarg
check "check unlinked: stray before bucket" 1 foo bucket check unlinked
check "check unlinked: stray between bucket and check" 1 bucket extra check unlinked
check "check unlinked: stray between check and unlinked" 1 bucket check extra unlinked

check "check unlinked: unrecognized flag" 22 bucket check unlinked --fakeflag
check "check unlinked: --max-concurrent-ios missing value" 1 bucket check unlinked --max-concurrent-ios
check "check unlinked: --bucket missing value" 1 bucket check unlinked --bucket

# ============================================================
echo ""
echo "=== bucket sync (checkpoint/info/status/markers/init/run/disable/enable) ==="
# ============================================================

# 'bucket sync' is not a command on its own; it needs a subcommand.
# Missing subcommand or unknown subcommand, both fail.
check "sync (incomplete command)" 1 bucket sync
check "sync: unknown subcommand" 1 bucket sync banana
check "sync: empty subcommand" 1 bucket sync ""
check "sync: repeated command word" 1 bucket sync sync status

# stray positional args
check "sync status: stray between sync and status" 1 bucket sync x status
check "sync status: stray between bucket and sync" 1 bucket extra sync status
check "sync status: stray before bucket" 1 foo bucket sync status
check "sync status: empty stray word" 1 bucket sync status ""
check "sync checkpoint: stray after" 1 bucket sync checkpoint strayarg
check "sync info: stray after" 1 bucket sync info strayarg
check "sync status: stray after" 1 bucket sync status strayarg
check "sync markers: stray after" 1 bucket sync markers strayarg
check "sync init: stray after" 1 bucket sync init strayarg
check "sync run: stray after" 1 bucket sync run strayarg
check "sync disable: stray after" 1 bucket sync disable strayarg
check "sync enable: stray after" 1 bucket sync enable strayarg

# unrecognized flag, on every leaf
check "sync checkpoint: unrecognized flag" 22 bucket sync checkpoint --fakeflag
check "sync info: unrecognized flag" 22 bucket sync info --fakeflag
check "sync status: unrecognized flag" 22 bucket sync status --fakeflag
check "sync markers: unrecognized flag" 22 bucket sync markers --fakeflag
check "sync init: unrecognized flag" 22 bucket sync init --fakeflag
check "sync run: unrecognized flag" 22 bucket sync run --fakeflag
check "sync disable: unrecognized flag" 22 bucket sync disable --fakeflag
check "sync enable: unrecognized flag" 22 bucket sync enable --fakeflag

# missing option value (parse-level, exit 1). Each leaf takes its own set of
# flags, so each is checked on the leaf that uses it rather than on one leaf
# standing in for the rest.
check "sync status: --bucket missing value" 1 bucket sync status --bucket
check "sync status: --bucket-id missing value" 1 bucket sync status --bucket-id
check "sync status: --tenant missing value" 1 bucket sync status --tenant
check "sync status: --format missing value" 1 bucket sync status --format
check "sync status: --source-zone missing value" 1 bucket sync status --source-zone
check "sync status: --source-bucket missing value" 1 bucket sync status --source-bucket
check "sync checkpoint: --bucket missing value" 1 bucket sync checkpoint --bucket
check "sync checkpoint: --source-bucket missing value" 1 bucket sync checkpoint --source-bucket
check "sync checkpoint: --timeout-sec missing value" 1 bucket sync checkpoint --timeout-sec
check "sync checkpoint: --retry-delay-ms missing value" 1 bucket sync checkpoint --retry-delay-ms
check "sync info: --bucket missing value" 1 bucket sync info --bucket
check "sync info: --bucket-id missing value" 1 bucket sync info --bucket-id
check "sync markers: --source-zone missing value" 1 bucket sync markers --source-zone
check "sync markers: --bucket missing value" 1 bucket sync markers --bucket
check "sync init: --source-bucket missing value" 1 bucket sync init --source-bucket
check "sync run: --source-zone missing value" 1 bucket sync run --source-zone
check "sync disable: --bucket missing value" 1 bucket sync disable --bucket
check "sync disable: --tenant missing value" 1 bucket sync disable --tenant
check "sync enable: --bucket missing value" 1 bucket sync enable --bucket

# --extra-info is a binary flag: it takes the next token only when that token is
# a bool, so anything else is left behind as a stray. init and run are the two
# leaves that use it, so both are checked.
check "sync init: --extra-info banana (left as stray)" 1 bucket sync init --extra-info banana
check "sync run: --extra-info banana (left as stray)" 1 bucket sync run --extra-info banana

# handler-level (cluster). The leaves check different things and in a different
# order: checkpoint/info/status/disable/enable want a bucket, while
# markers/init/run want a source zone first and only then a bucket.
# A --source-zone that does not exist also prints a warning of its own; the
# rows below assert the error that follows it.
check_cluster "sync checkpoint: missing --bucket" 22 -- bucket sync checkpoint
check_cluster "sync info: missing --bucket" 22 -- bucket sync info
check_cluster "sync status: missing --bucket" 22 -- bucket sync status
check_cluster "sync disable: missing --bucket" 22 -- bucket sync disable
check_cluster "sync enable: missing --bucket" 22 -- bucket sync enable
check_cluster "sync markers: missing --source-zone" 22 -- bucket sync markers
check_cluster "sync init: missing --source-zone" 22 -- bucket sync init
check_cluster "sync run: missing --source-zone" 22 -- bucket sync run
# with a source zone, the same three then report the missing bucket
check_cluster "sync markers: --source-zone but no --bucket" 22 -- bucket sync markers --source-zone z1
check_cluster "sync init: --source-zone but no --bucket" 22 -- bucket sync init --source-zone z1
check_cluster "sync run: --source-zone but no --bucket" 22 -- bucket sync run --source-zone z1
# --extra-info does take a bool, and no stray is reported: the command gets as
# far as its own missing source zone
check_cluster "sync init: --extra-info true (bool consumed)" 22 -- bucket sync init --extra-info true
# an empty --bucket= value counts as no bucket at all
check_cluster "sync status: --bucket= empty" 22 -- bucket sync status --bucket=

# valid args, nonexistent bucket. All eight fail to load the bucket and exit 2,
# but they report it three different ways: checkpoint/info/status say nothing at
# all (exit 2, no message), markers/init/run print "could not init
# bucket", and disable/enable name the bucket they could not read.
check_cluster "sync checkpoint: nonexistent bucket (silent exit 2)" 2 -- bucket sync checkpoint --bucket no-such-bucket
check_cluster "sync info: nonexistent bucket (silent exit 2)" 2 -- bucket sync info --bucket no-such-bucket
check_cluster "sync status: nonexistent bucket (silent exit 2)" 2 -- bucket sync status --bucket no-such-bucket
check_cluster "sync markers: nonexistent bucket" 2 -- bucket sync markers --source-zone z1 --bucket no-such-bucket
check_cluster "sync init: nonexistent bucket" 2 -- bucket sync init --source-zone z1 --bucket no-such-bucket
check_cluster "sync run: nonexistent bucket" 2 -- bucket sync run --source-zone z1 --bucket no-such-bucket
check_cluster "sync disable: nonexistent bucket" 2 -- bucket sync disable --bucket no-such-bucket
check_cluster "sync enable: nonexistent bucket" 2 -- bucket sync enable --bucket no-such-bucket
# --bucket-id is accepted as well, and does not change the outcome
check_cluster "sync info: with --bucket-id (silent exit 2)" 2 -- bucket sync info --bucket no-such-bucket --bucket-id nosuchid

# unrelated flags alongside valid args: a binary flag (--fix) takes 0 values ->
# accepted; a value option binds in either form -> accepted. None of the three
# changes the outcome, so all three still fail on the nonexistent bucket.
check_cluster "sync status: unrelated binary flag --fix accepted (silent exit 2)" 2 -- bucket sync status --fix --bucket no-such-bucket
check_cluster "sync status: unrelated value flag --max-entries=5 (=form) (silent exit 2)" 2 -- bucket sync status --max-entries=5 --bucket no-such-bucket
check_cluster "sync status: unrelated --max-entries 5 swallowed (space form) (silent exit 2)" 2 -- bucket sync status --bucket no-such-bucket --max-entries 5
# --timeout-sec and --retry-delay-ms are read leniently, so a non-numeric value
# is accepted rather than rejected, and the command still fails on the bucket
check_cluster "sync checkpoint: --timeout-sec non-integer accepted (silent exit 2)" 2 -- bucket sync checkpoint --bucket no-such-bucket --timeout-sec abc
check_cluster "sync checkpoint: --retry-delay-ms non-integer accepted (silent exit 2)" 2 -- bucket sync checkpoint --bucket no-such-bucket --retry-delay-ms abc

# flags before the leaf subcommand. The value still reaches the command, so a
# nonexistent bucket still fails: status silently (exit 2, no message),
# markers with its message. --tenant trips the global "no user ID" check
# (exit 22).
check_cluster "sync status: --bucket before subcommand (silent exit 2)" 2 -- bucket --bucket no-such-bucket sync status
check_cluster "sync status: -b before subcommand (short) (silent exit 2)" 2 -- bucket -b no-such-bucket sync status
check_cluster "sync status: --bucket-id before subcommand (silent exit 2)" 2 -- bucket --bucket-id x sync status --bucket no-such-bucket
check_cluster "sync status: --format before subcommand (silent exit 2)" 2 -- bucket --format json sync status --bucket no-such-bucket
check_cluster "sync markers: --source-zone before subcommand" 2 -- bucket --source-zone z1 sync markers --bucket no-such-bucket
check_cluster "sync status: --tenant before subcommand" 22 -- bucket --tenant t sync status --bucket no-such-bucket

# the same flag given twice
check_cluster "sync status: duplicate --bucket (silent exit 2)" 2 -- bucket sync status --bucket a --bucket no-such-bucket
check_cluster "sync disable: duplicate --bucket" 2 -- bucket sync disable --bucket a --bucket no-such-bucket
check_cluster "sync init: duplicate --source-zone" 2 -- bucket sync init --source-zone a --source-zone z1 --bucket no-such-bucket
check_cluster "sync status: duplicate --tenant" 22 -- bucket sync status --tenant a --tenant b --bucket no-such-bucket

# two flags at once: before the subcommand, or before and duplicated
check_cluster "sync markers: --source-zone + --bucket before" 2 -- bucket --source-zone z1 --bucket no-such-bucket sync markers
check_cluster "sync status: pos + duplicate --bucket (silent exit 2)" 2 -- bucket --bucket a sync status --bucket no-such-bucket

# ============================================================
echo ""
echo "=== bucket reshard (+ 'reshard bucket' alias) ==="
# ============================================================
# 'bucket reshard' and 'reshard bucket' are two entry points to the same
# command. The reshard block below is the full coverage; the alias rows repeat
# every flag on the second spelling, and one row per validation to show it
# gives the same errors.

# stray positional args
check "reshard: stray after flags" 1 bucket reshard strayarg
check "reshard: stray before bucket" 1 foo bucket reshard
check "reshard: stray between bucket and leaf" 1 bucket extra reshard
check "reshard bucket (alias): stray after flags" 1 reshard bucket strayarg
check "reshard bucket (alias): stray between reshard and bucket" 1 reshard extra bucket

check "reshard: unrecognized flag" 22 bucket reshard --fakeflag
check "reshard bucket (alias): unrecognized flag" 22 reshard bucket --fakeflag

# missing option value (parse-level, exit 1)
check "reshard: --bucket missing value" 1 bucket reshard --bucket
check "reshard: --bucket-id missing value" 1 bucket reshard --bucket-id
check "reshard: --tenant missing value" 1 bucket reshard --tenant
check "reshard: --num-shards missing value" 1 bucket reshard --num-shards
check "reshard: --max-entries missing value" 1 bucket reshard --max-entries

# --num-shards and --max-entries are parsed as integers; a non-numeric value is
# rejected in either form, and an empty value is reported the same way
check "reshard: --num-shards non-integer" 22 bucket reshard --num-shards abc
check "reshard: --num-shards=abc (=form)" 22 bucket reshard --num-shards=abc
check "reshard: --num-shards empty value" 22 bucket reshard --num-shards ""
check "reshard: --max-entries non-integer" 22 bucket reshard --max-entries=abc
check "reshard bucket (alias): --num-shards=abc" 22 reshard bucket --num-shards=abc

# the alias takes the same flags. One row per flag, so a flag that reached only
# one of the two spellings would show up here rather than pass unnoticed.
check "reshard bucket (alias): --bucket missing value" 1 reshard bucket --bucket
check "reshard bucket (alias): --bucket-id missing value" 1 reshard bucket --bucket-id
check "reshard bucket (alias): --tenant missing value" 1 reshard bucket --tenant
check "reshard bucket (alias): --num-shards missing value" 1 reshard bucket --num-shards
check "reshard bucket (alias): --max-entries missing value" 1 reshard bucket --max-entries
check "reshard bucket (alias): --max-entries non-integer" 22 reshard bucket --max-entries=abc
check "reshard bucket (alias): --yes-i-really-mean-it banana (left as stray)" 1 reshard bucket --bucket no-such-bucket --num-shards 4 --yes-i-really-mean-it banana

# handler-level (cluster): these validations run after driver init.
# Order: bucket empty -> num-shards specified -> num-shards <= max -> num-shards
# >= 0 -> the bucket exists.
#
# The exit codes here are 234 and 254 rather than the usual 22 and 2. reshard
# hands the errno back still negative, and the shell keeps only the low 8 bits:
#     -EINVAL = -22  ->  234
#     -ENOENT =  -2  ->  254
# So the number is different but the error behind it is the same one.
check_cluster "reshard: missing --bucket" 234 -- bucket reshard
check_cluster "reshard: missing --bucket, --num-shards given" 234 -- bucket reshard --num-shards 4
check_cluster "reshard: --num-shards not specified" 234 -- bucket reshard --bucket no-such-bucket
check_cluster "reshard: --num-shards above the maximum" 234 -- bucket reshard --bucket no-such-bucket --num-shards 99999999
check_cluster "reshard: --num-shards negative" 234 -- bucket reshard --bucket no-such-bucket --num-shards -1
# valid args but nonexistent bucket: the bucket is not found (-ENOENT)
check_cluster "reshard: nonexistent bucket" 254 -- bucket reshard --bucket no-such-bucket --num-shards 4
check_cluster "reshard: --num-shards 0 is accepted" 254 -- bucket reshard --bucket no-such-bucket --num-shards 0
check_cluster "reshard bucket (alias): nonexistent bucket" 254 -- reshard bucket --bucket no-such-bucket --num-shards 4
# the alias takes flags out of position and repeated flags the same way
check_cluster "reshard bucket (alias): --bucket between reshard and bucket" 254 -- reshard --bucket no-such-bucket bucket --num-shards 4
check_cluster "reshard bucket (alias): -b (short)" 254 -- reshard bucket -b no-such-bucket --num-shards 4
check_cluster "reshard bucket (alias): duplicate --bucket" 254 -- reshard bucket --bucket a --bucket no-such-bucket --num-shards 4
check_cluster "reshard bucket (alias): unrelated binary flag --fix accepted" 254 -- reshard bucket --fix --bucket no-such-bucket --num-shards 4
check_cluster "reshard bucket (alias): --yes-i-really-mean-it false (bool consumed)" 254 -- reshard bucket --bucket no-such-bucket --num-shards 4 --yes-i-really-mean-it false
# and gives the same errors, in the same order
check_cluster "reshard bucket (alias): missing --bucket" 234 -- reshard bucket
check_cluster "reshard bucket (alias): --num-shards not specified" 234 -- reshard bucket --bucket no-such-bucket
check_cluster "reshard bucket (alias): --tenant" 22 -- reshard bucket --tenant t --bucket no-such-bucket --num-shards 4

# --yes-i-really-mean-it is a binary flag: it takes the next token only when that
# token is a bool, so a bool is consumed and anything else is left as a stray
check_cluster "reshard: --yes-i-really-mean-it false (bool consumed)" 254 -- bucket reshard --bucket no-such-bucket --num-shards 4 --yes-i-really-mean-it false
check "reshard: --yes-i-really-mean-it banana (left as stray)" 1 bucket reshard --bucket no-such-bucket --num-shards 4 --yes-i-really-mean-it banana

# unrelated flags alongside valid args: a binary flag, a value option in =form,
# and the same option in space form. All three are ignored, so all three still
# fail on the nonexistent bucket.
check_cluster "reshard: unrelated binary flag --fix accepted" 254 -- bucket reshard --fix --bucket no-such-bucket --num-shards 4
check_cluster "reshard: unrelated value flag --max-entries=5 (=form)" 254 -- bucket reshard --max-entries=5 --bucket no-such-bucket --num-shards 4
check_cluster "reshard: unrelated --max-entries 5 swallowed (space form)" 254 -- bucket reshard --max-entries 5 --bucket no-such-bucket --num-shards 4

# flags before the leaf subcommand. The value still reaches the command, so with
# a valid --num-shards, a nonexistent bucket still fails.
# --tenant trips the global "no user ID" check (exit 22).
check_cluster "reshard: --bucket before subcommand" 254 -- bucket --bucket no-such-bucket reshard --num-shards 4
check_cluster "reshard: -b before subcommand (short)" 254 -- bucket -b no-such-bucket reshard --num-shards 4
check_cluster "reshard: --num-shards before subcommand" 254 -- bucket --num-shards 4 reshard --bucket no-such-bucket
check_cluster "reshard: --bucket-id before subcommand" 254 -- bucket --bucket-id x reshard --bucket no-such-bucket --num-shards 4
check_cluster "reshard: --yes-i-really-mean-it before subcommand" 254 -- bucket --yes-i-really-mean-it reshard --bucket no-such-bucket --num-shards 4
check_cluster "reshard: --format before subcommand" 254 -- bucket --format json reshard --bucket no-such-bucket --num-shards 4
check_cluster "reshard: --tenant before subcommand" 22 -- bucket --tenant t reshard --bucket no-such-bucket --num-shards 4

# the same flag given twice
check_cluster "reshard: duplicate --bucket" 254 -- bucket reshard --bucket a --bucket no-such-bucket --num-shards 4
check_cluster "reshard: duplicate --num-shards" 254 -- bucket reshard --bucket no-such-bucket --num-shards 2 --num-shards 4
check_cluster "reshard: duplicate --yes-i-really-mean-it" 254 -- bucket reshard --bucket no-such-bucket --num-shards 4 --yes-i-really-mean-it --yes-i-really-mean-it
check_cluster "reshard: duplicate --tenant" 22 -- bucket reshard --tenant a --tenant b --bucket no-such-bucket --num-shards 4

# two or three flags at once: before the subcommand, or before and duplicated
check_cluster "reshard: --bucket + --num-shards before" 254 -- bucket --bucket no-such-bucket --num-shards 4 reshard
check_cluster "reshard: pos + duplicate --bucket" 254 -- bucket --bucket a reshard --bucket no-such-bucket --num-shards 4
check_cluster "reshard: --bucket + --num-shards + --tenant before" 22 -- bucket --bucket no-such-bucket --num-shards 4 --tenant t reshard


# ============================================================
echo ""
echo "=== bucket list: flags out of position, and repeated flags (cluster) ==="
# ============================================================

# flag before bucket
# --bucket with nonexistent name: init_bucket fails (exit 2)
check_cluster "list: --bucket/-b before bucket" 2 -- --bucket nonexistent_test bucket list
check_cluster "list: -b (short) before bucket" 2 -- -b nonexistent_test bucket list
# --tenant without --uid is rejected (exit 22)
check_cluster "list: --tenant before bucket" 22 -- --tenant mytenant bucket list
# flags that don't affect success: command succeeds (exit 0)
check_cluster "list: --format before bucket" 0 -- --format json bucket list
check_cluster "list: --max-entries before bucket" 0 -- --max-entries 10 bucket list
check_cluster "list: --marker before bucket" 0 -- --marker somemarker bucket list

# flag between bucket and list
check_cluster "list: --bucket between bucket and list" 2 -- bucket --bucket nonexistent_test list
check_cluster "list: --tenant between bucket and list" 22 -- bucket --tenant mytenant list
check_cluster "list: --format between bucket and list" 0 -- bucket --format json list
check_cluster "list: --max-entries between bucket and list" 0 -- bucket --max-entries 10 list
check_cluster "list: --marker between bucket and list" 0 -- bucket --marker somemarker list

# the same flag twice
check_cluster "list: duplicate --bucket, both after the command" 2 -- bucket list --bucket nonexistent1_test --bucket nonexistent2_test
check_cluster "list: duplicate --tenant, both after the command" 22 -- bucket list --tenant foo --tenant bar
check_cluster "list: duplicate --format, both after the command" 0 -- bucket list --format json --format xml
# --uid filters bucket list by owner
# an unknown user gives -ENOENT, so the exit code is 254
check_cluster "list: --uid before bucket" 254 -- --uid testuser_test bucket list
check_cluster "list: --bucket-id before bucket" 0 -- --bucket-id nonexistent_id_test bucket list
check_cluster "list: --object-version before bucket" 0 -- --object-version somever bucket list
check_cluster "list: --allow-unordered before bucket" 0 -- --allow-unordered bucket list

# out of position and duplicated at once
check_cluster "list: duplicate --bucket, one before the command" 2 -- --bucket nonexistent1_test bucket list --bucket nonexistent2_test
check_cluster "list: duplicate --tenant, one before the command" 22 -- --tenant foo bucket list --tenant bar

# ============================================================
echo ""
echo "=== bucket stats: flags out of position, and repeated flags (cluster) ==="
# ============================================================

check_cluster "stats: --bucket before bucket" 2 -- --bucket nonexistent_test bucket stats
check_cluster "stats: --tenant before bucket" 22 -- --tenant mytenant bucket stats
check_cluster "stats: --bucket between bucket and stats" 2 -- bucket --bucket nonexistent_test stats
check_cluster "stats: duplicate --bucket" 2 -- bucket stats --bucket nonexistent1_test --bucket nonexistent2_test

# stats-specific flags out of position
check_cluster "stats: --show-restore-stats before bucket" 0 -- --show-restore-stats bucket stats
check_cluster "stats: --show-restore-stats between bucket/stats" 0 -- bucket --show-restore-stats stats
check_cluster "stats: duplicate --show-restore-stats" 0 -- bucket stats --show-restore-stats --show-restore-stats
# an unknown bucket id gives -ENOENT, so the exit code is 254
check_cluster "stats: --bucket-id before bucket" 254 -- --bucket-id nonexistent_id_test bucket stats
check_cluster "stats: duplicate --bucket-id" 254 -- bucket stats --bucket-id id1_test --bucket-id id2_test
check_cluster "stats: --max-entries before bucket" 0 -- --max-entries 10 bucket stats
check_cluster "stats: --marker before bucket" 0 -- --marker foo bucket stats
check_cluster "stats: --format before bucket" 0 -- --format json bucket stats
check_cluster "stats: --format between bucket and stats" 0 -- bucket --format json stats
check_cluster "stats: duplicate --format" 0 -- bucket stats --format json --format xml

# stats: several flags out of position at once
check_cluster "stats: --show-restore-stats + --tenant before" 22 -- --show-restore-stats --tenant foo bucket stats
check_cluster "stats: --bucket + --show-restore-stats before" 2 -- --bucket nonexistent_test --show-restore-stats bucket stats

# ============================================================
echo ""
echo "=== bucket link: flags out of position, and repeated flags (cluster) ==="
# ============================================================

# The flags are correct but out of position. The commands still fail for their
# own reason (no such bucket/user).
check_cluster "link: --bucket before bucket (then fails)" 2 -- --bucket nonexistent_test bucket link --uid testuser_test
check_cluster "link: --uid before bucket (then fails)" 2 -- --uid testuser_test bucket link --bucket nonexistent_test
check_cluster "link: duplicate --bucket" 2 -- bucket link --bucket foo --bucket nonexistent_test --uid testuser_test
check_cluster "link: duplicate --uid" 2 -- bucket link --uid foo --uid testuser_test --bucket nonexistent_test

# link-specific flags out of position
check_cluster "link: --bucket-new-name before bucket" 2 -- --bucket-new-name newname bucket link --bucket nonexistent_test --uid testuser_test
check_cluster "link: --bucket-id before bucket" 2 -- --bucket-id someid_test bucket link --bucket nonexistent_test --uid testuser_test
check_cluster "link: --tenant before bucket" 2 -- --tenant foo bucket link --bucket nonexistent_test --uid testuser_test
check_cluster "link: --bucket + --uid + --tenant before" 2 -- --bucket nonexistent_test --uid testuser_test --tenant foo bucket link

# ============================================================
echo ""
echo "=== bucket unlink: flags out of position, and repeated flags (cluster) ==="
# ============================================================

check_cluster "unlink: --bucket before bucket (then fails)" 2 -- --bucket nonexistent_test bucket unlink --uid testuser_test
check_cluster "unlink: --uid before bucket (then fails)" 2 -- --uid testuser_test bucket unlink --bucket nonexistent_test
check_cluster "unlink: duplicate --bucket" 2 -- bucket unlink --bucket foo --bucket nonexistent_test --uid testuser_test
check_cluster "unlink: --tenant before bucket" 2 -- --tenant foo bucket unlink --bucket nonexistent_test --uid testuser_test
check_cluster "unlink: duplicate --uid" 2 -- bucket unlink --uid foo --uid testuser_test --bucket nonexistent_test
check_cluster "unlink: --bucket + --uid before" 2 -- --bucket nonexistent_test --uid testuser_test bucket unlink

# ============================================================
echo ""
echo "=== bucket rm: flags out of position, and repeated flags (cluster) ==="
# ============================================================

check_cluster "rm: --bucket before bucket (nonexistent bucket, silent exit 0)" 0 -- --bucket nonexistent_test bucket rm
check_cluster "rm: duplicate --bucket" 0 -- bucket rm --bucket foo --bucket nonexistent_test
check_cluster "rm: duplicate --tenant" 22 -- bucket rm --bucket nonexistent_test --tenant foo --tenant bar

# rm-specific flags out of position
check_cluster "rm: --purge-objects before bucket" 0 -- --purge-objects bucket rm --bucket nonexistent_test
check_cluster "rm: --bypass-gc before bucket" 0 -- --bypass-gc bucket rm --bucket nonexistent_test
check_cluster "rm: --inconsistent-index before bucket" 1 -- --inconsistent-index bucket rm --bucket nonexistent_test
check_cluster "rm: --yes-i-really-mean-it + --inconsistent-index before" 0 -- --yes-i-really-mean-it --inconsistent-index bucket rm --bucket nonexistent_test
check_cluster "rm: --tenant before bucket" 22 -- --tenant foo bucket rm --bucket nonexistent_test
check_cluster "rm: --purge-objects between bucket and rm" 0 -- bucket --purge-objects rm --bucket nonexistent_test
check_cluster "rm: duplicate --purge-objects" 0 -- bucket rm --bucket nonexistent_test --purge-objects --purge-objects

# rm: several flags out of position at once
check_cluster "rm: --purge-objects + --bypass-gc before" 0 -- --purge-objects --bypass-gc bucket rm --bucket nonexistent_test
check_cluster "rm: --bucket + --purge-objects before" 0 -- --bucket nonexistent_test --purge-objects bucket rm
check_cluster "rm: --bucket + --tenant + --purge-objects before (exit 22)" 22 -- --bucket nonexistent_test --tenant foo --purge-objects bucket rm
check_cluster "rm: 4 flags before the subcommand + inconsistent error" 1 -- --bucket nonexistent_test --purge-objects --bypass-gc --inconsistent-index bucket rm
check_cluster "rm: pos + duplicate + tenant" 22 -- --bucket foo --tenant bar bucket rm --bucket nonexistent_test

# ============================================================
echo ""
echo "=== bucket check: flags out of position, and repeated flags (cluster) ==="
# ============================================================

check_cluster "check: --bucket before bucket" 0 -- --bucket nonexistent_test bucket check
check_cluster "check: --fix before bucket" 0 -- --fix bucket check
check_cluster "check: --bucket between bucket and check" 0 -- bucket --bucket nonexistent_test check
check_cluster "check: --fix between bucket and check" 0 -- bucket --fix check
check_cluster "check: duplicate --bucket" 0 -- bucket check --bucket nonexistent1_test --bucket nonexistent2_test
check_cluster "check: duplicate --tenant" 22 -- bucket check --tenant foo --tenant bar
check_cluster "check: duplicate --bucket, one before the command" 0 -- --bucket nonexistent1_test bucket check --bucket nonexistent2_test

# check-specific flags out of position
check_cluster "check: --remove-bad before bucket" 0 -- --remove-bad bucket check
check_cluster "check: --remove-bad between bucket and check" 0 -- bucket --remove-bad check
check_cluster "check: --check-head-obj-locator before bucket" 22 -- --check-head-obj-locator bucket check
check_cluster "check: --tenant before bucket" 22 -- --tenant foo bucket check
check_cluster "check: duplicate --fix" 0 -- bucket check --fix --fix
# a flag given twice with explicit values is accepted
check_cluster "check: duplicate --fix=value (accepted)" 0 -- bucket check --fix=true --fix=false

# --fix is a binary flag: bare, =true, =false and the space form all work.
# With no --bucket the check is a silent no-op, so these test parsing only:
# whether the value was consumed or left behind as a stray word.
# A real stray like "zzz" gives exit 1, so exit 0 here means --fix took the token.
check_cluster "check: --fix (bare)" 0 -- bucket check --fix
check_cluster "check: --fix=true" 0 -- bucket check --fix=true
check_cluster "check: --fix true (space, bool consumed)" 0 -- bucket check --fix true
check_cluster "check: --fix=false" 0 -- bucket check --fix=false
check_cluster "check: --fix false (space, bool consumed)" 0 -- bucket check --fix false
# a non-bool value stores -EINVAL in the flag. That is non-zero, so the flag
# counts as set and the command exits 0, with no message.
check_cluster "check: --fix=banana (accepted)" 0 -- bucket check --fix=banana
# gc list ignores --fix, so an invalid value does not affect the exit code
check_cluster "gc list --fix=banana (parse-safe)" 0 -- gc list --fix=banana

# one "=banana" per binary flag: a non-bool value is accepted and the flag is
# treated as set. bare, =true and =banana all give the same result.
check_cluster "list: --allow-unordered=banana" 0 -- bucket list --allow-unordered=banana
check_cluster "stats: --show-restore-stats=banana" 0 -- bucket stats --show-restore-stats=banana
check_cluster "check: --remove-bad=banana" 0 -- bucket check --remove-bad=banana
check_cluster "check: --check-objects=banana" 0 -- bucket check --check-objects=banana
# set -> locator path needs a bucket name -> exit 22 (same as bare/=true)
check_cluster "check: --check-head-obj-locator=banana" 22 -- bucket check --check-head-obj-locator=banana
check_cluster "rm: --purge-objects=banana" 0 -- bucket rm --purge-objects=banana
check_cluster "rm: --bypass-gc=banana" 0 -- bucket rm --bypass-gc=banana
# set -> corrupt-index guard fires (requires --yes-i-really-mean-it) -> exit 1
check_cluster "rm: --inconsistent-index=banana" 1 -- bucket rm --inconsistent-index=banana
check_cluster "rm: --yes-i-really-mean-it=banana" 0 -- bucket rm --yes-i-really-mean-it=banana

# check: several flags out of position at once
check_cluster "check: --fix + --remove-bad before" 0 -- --fix --remove-bad bucket check
check_cluster "check: --fix + --remove-bad + --tenant before" 22 -- --fix --remove-bad --tenant foo bucket check

# check flags before the subcommand: accepted, the command runs normally
check_cluster "check: --check-objects before bucket" 0 -- --check-objects bucket check
check_cluster "check: --max-concurrent-ios before bucket" 0 -- --max-concurrent-ios 5 bucket check
check_cluster "check: duplicate --max-concurrent-ios" 0 -- bucket check --max-concurrent-ios 5 --max-concurrent-ios 10

# ============================================================
echo ""
echo "=== bucket check olh/unlinked: flags out of position, and repeated flags (cluster) ==="
# ============================================================

# --bucket out of position for olh/unlinked: before "bucket", or between
# "bucket" and "check"
check_cluster "check olh: --bucket before bucket" 0 -- --bucket nonexistent_test bucket check olh
check_cluster "check olh: --bucket between bucket and check" 0 -- bucket --bucket nonexistent_test check olh
check_cluster "check olh: --tenant before bucket" 22 -- --tenant foo bucket check olh
check_cluster "check olh: --bucket + --tenant before" 22 -- --bucket nonexistent_test --tenant foo bucket check olh

# olh-specific flags out of position
check_cluster "check olh: --fix before bucket" 0 -- --fix bucket check olh
check_cluster "check olh: --dump-keys before bucket" 0 -- --dump-keys bucket check olh
check_cluster "check olh: --hide-progress before bucket" 0 -- --hide-progress bucket check olh
check_cluster "check olh: --max-concurrent-ios before bucket" 0 -- --max-concurrent-ios 5 bucket check olh
check_cluster "check olh: --dump-keys + --hide-progress before" 0 -- --dump-keys --hide-progress bucket check olh

# the same flags again, now between the command words instead of before them
check_cluster "check olh: --fix after bucket" 0 -- bucket --fix check olh
check_cluster "check olh: --fix after check (accepted)" 0 -- bucket check --fix olh
check_cluster "check olh: --dump-keys after bucket" 0 -- bucket --dump-keys check olh
check_cluster "check olh: --dump-keys after check" 0 -- bucket check --dump-keys olh
check_cluster "check olh: --hide-progress after bucket" 0 -- bucket --hide-progress check olh
check_cluster "check olh: --hide-progress after check" 0 -- bucket check --hide-progress olh
# the =form is rejected at the first flag, so the second is never reached
check "check olh: duplicate --dump-keys (rejected at the first)" 22 bucket check olh --dump-keys=true --dump-keys=false
check "check olh: duplicate --hide-progress (rejected at the first)" 22 bucket check olh --hide-progress=true --hide-progress=false
# --fix twice, between the command words
check_cluster "check olh: duplicate --fix between command words" 0 -- bucket check --fix=true --fix=false olh

check_cluster "check unlinked: --bucket before bucket" 0 -- --bucket nonexistent_test bucket check unlinked
check_cluster "check unlinked: --bucket between bucket and check" 0 -- bucket --bucket nonexistent_test check unlinked
check_cluster "check unlinked: --tenant before bucket" 22 -- --tenant foo bucket check unlinked

# unlinked-specific flags out of position
check_cluster "check unlinked: --fix before bucket" 0 -- --fix bucket check unlinked
check_cluster "check unlinked: --dump-keys before bucket" 0 -- --dump-keys bucket check unlinked
check_cluster "check unlinked: --hide-progress before bucket" 0 -- --hide-progress bucket check unlinked
check_cluster "check unlinked: --max-concurrent-ios before bucket" 0 -- --max-concurrent-ios 5 bucket check unlinked

# ============================================================
echo ""
echo "=== bucket remove alias: flags out of position, and repeated flags (cluster) ==="
# ============================================================

check_cluster "remove: --bucket before bucket" 0 -- --bucket nonexistent_test bucket remove
check_cluster "remove: --purge-objects before bucket" 0 -- --purge-objects bucket remove --bucket nonexistent_test
check_cluster "remove: --tenant before bucket" 22 -- --tenant foo bucket remove --bucket nonexistent_test
check_cluster "remove: duplicate --bucket" 0 -- bucket remove --bucket foo --bucket nonexistent_test
check_cluster "remove: --bucket + --purge-objects before" 0 -- --bucket nonexistent_test --purge-objects bucket remove
check_cluster "remove: --inconsistent-index before (without yes)" 1 -- --inconsistent-index bucket remove --bucket nonexistent_test

# ============================================================
echo ""
echo "=== short flags in correct position (cluster) ==="
# ============================================================

# -b accepted as --bucket, -i accepted as --uid in correct position
check_cluster "list: -b correct position (nonexistent)" 2 -- bucket list -b nonexistent_test
check_cluster "link: -b and -i correct position (nonexistent)" 2 -- bucket link -b nonexistent_test -i nonexistent_user_test
check_cluster "unlink: -b and -i correct position (nonexistent)" 2 -- bucket unlink -b nonexistent_test -i nonexistent_user_test
check_cluster "rm: -b correct position (nonexistent)" 0 -- bucket rm -b nonexistent_test
check_cluster "stats: -b correct position (nonexistent)" 2 -- bucket stats -b nonexistent_test
check_cluster "check: -b correct position" 0 -- bucket check -b nonexistent_test

# ============================================================
echo ""
echo "=== functional: format and flag options (cluster) ==="
# ============================================================

# bucket list: format and ordering flags
check_cluster "functional: bucket list --allow-unordered" 0 -- bucket list --allow-unordered
check_cluster "functional: bucket list --format json" 0 -- bucket list --format json
check_cluster "functional: bucket list --format xml" 0 -- bucket list --format xml
check_cluster "functional: bucket list --max-entries 5" 0 -- bucket list --max-entries 5
check_cluster "functional: bucket list --allow-unordered + --max-entries" 0 -- bucket list --allow-unordered --max-entries 10
check_cluster "functional: buckets list --allow-unordered (alias)" 0 -- buckets list --allow-unordered

# bucket stats: format and restore-stats
check_cluster "functional: bucket stats --format json" 0 -- bucket stats --format json
check_cluster "functional: bucket stats --format xml" 0 -- bucket stats --format xml
check_cluster "functional: bucket stats --max-entries 5" 0 -- bucket stats --max-entries 5
check_cluster "functional: bucket stats --show-restore-stats" 0 -- bucket stats --show-restore-stats
check "functional: buckets stats --format json (alias)" 1 buckets stats --format json

# bucket check: new flags
check_cluster "functional: bucket check --check-objects" 0 -- bucket check --check-objects
check_cluster "functional: bucket check --max-concurrent-ios" 0 -- bucket check --max-concurrent-ios 4

# bucket check olh/unlinked: no --bucket (global scan) and new flags
check_cluster "functional: bucket check olh (no --bucket)" 0 -- bucket check olh
check_cluster "functional: bucket check olh --fix" 0 -- bucket check olh --fix
check_cluster "functional: bucket check olh --dump-keys" 0 -- bucket check olh --dump-keys
check_cluster "functional: bucket check olh --hide-progress" 0 -- bucket check olh --hide-progress
check_cluster "functional: bucket check olh --max-concurrent-ios" 0 -- bucket check olh --max-concurrent-ios 4
check_cluster "functional: bucket check unlinked (no --bucket)" 0 -- bucket check unlinked
check_cluster "functional: bucket check unlinked --fix" 0 -- bucket check unlinked --fix
check_cluster "functional: bucket check unlinked --dump-keys" 0 -- bucket check unlinked --dump-keys
check_cluster "functional: bucket check unlinked --hide-progress" 0 -- bucket check unlinked --hide-progress
# --dump-keys/--hide-progress take no value, so a =value suffix is rejected as
# an unknown flag - unlike the binary flags above, which accept it.
check "check olh: --dump-keys=banana (rejected)" 22 bucket check olh --dump-keys=banana
check "check olh: --hide-progress=banana (rejected)" 22 bucket check olh --hide-progress=banana
check "check unlinked: --dump-keys=banana (rejected)" 22 bucket check unlinked --dump-keys=banana
check "check unlinked: --hide-progress=banana (rejected)" 22 bucket check unlinked --hide-progress=banana
# --dump-keys and --hide-progress take no value.
# Unlike the other binary flags, these two do not accept =true / =false.
# With =value the whole token is unrecognized and rejected.
# With a space the flag is taken and the value is left as a command word.
# Both flags are shared by 'check olh' and 'check unlinked', so olh covers both.
check "check olh: --dump-keys=true" 22 bucket check olh --dump-keys=true
check "check olh: --dump-keys true (space)" 1 bucket check olh --dump-keys true
check "check olh: --dump-keys=false" 22 bucket check olh --dump-keys=false
check "check olh: --dump-keys false (space)" 1 bucket check olh --dump-keys false
check "check olh: --hide-progress=true" 22 bucket check olh --hide-progress=true
check "check olh: --hide-progress true (space)" 1 bucket check olh --hide-progress true
check "check olh: --hide-progress=false" 22 bucket check olh --hide-progress=false
check "check olh: --hide-progress false (space)" 1 bucket check olh --hide-progress false
check "functional: buckets check olh (alias, no --bucket)" 1 buckets check olh
check "functional: buckets check unlinked (alias)" 1 buckets check unlinked

# ============================================================
echo ""
echo "=== '=' token normalization (empty '=' and short-flag '=') ==="
# ============================================================
# '=' forms: "--name=" passes an empty value through to the flag's own
# parsing, and "-i=value" splits the value off the short flag.

# int flag: an empty value fails to parse
check "empty-= on int flag" 22 bucket list --max-entries=
# uid/bucket-id: an empty value is rejected by their own per-value checks
check "empty-= on --uid" 1 bucket list --uid=
check "empty-= on -i" 1 bucket list -i=
check "empty-= on --bucket-id" 1 bucket stats --bucket-id=
# non-empty short-flag '=': the value is split off the flag, so the message
# names the user without a leading '=' (uncaught by -b=, which never echoes).
# an unknown user gives -ENOENT, so the exit code is 254
check_cluster "non-empty -= on -i (value split off the flag)" 254 -- bucket list -i=nosuchuser
# mid-line: "" is the value; the next word strays (the collapsed flag must not eat it)
check "empty-= mid-line strays next word" 1 bucket list --bucket= foo
# unknown flag with an empty '=': rejected by name
check "empty-= on unknown flag" 22 bucket list --banana=
# glued short flags (i.e. -Xvalue, no '=') are rejected as unknown flags
check "glued short flag rejected" 22 bucket list -ibanana

# ============================================================
echo ""
echo "=== empty-value checks and glued short flags ==="
# ============================================================
# Empty-value special cases for -i/--uid and --bucket-id. The '='-forms are
# covered above; these are the space forms.

check "empty space-form -i" 1 bucket list -i ""
check "empty space-form --uid" 1 bucket list --uid ""
# uid emptiness is judged after tenant$user parsing: an empty
# user-id part rejects even when a tenant is present
check "parse-empty uid: -i '\$'" 1 bucket list -i '$'
check "parse-empty uid: -i 'tenant\$'" 1 bucket list -i 'tenant$'
check "empty space-form --bucket-id" 1 bucket stats --bucket-id ""
# the check runs wherever the flag appears, before the command words or
# between them
check "empty --bucket-id before the command words" 1 --bucket-id "" bucket stats
check "empty --bucket-id between the command words" 1 bucket --bucket-id "" stats
# the check runs per occurrence: an empty value errors
# whether or not another occurrence supplies a valid one
check "empty -i then valid -i" 1 bucket list -i "" -i slides
check "valid -i then empty -i" 1 bucket list -i slides -i ""

# glued short values (i.e. -Xvalue) are rejected. "-ibanana" above covers -i;
# -b and -o are the other shorts that take values.
check "glued short -b rejected" 22 bucket stats -bdemo
check "glued short -o rejected" 22 bucket object shard -oxyz
# an unknown short flag glued to a value is rejected the same way
check "unknown glued short rejected" 22 bucket list -xfoo
# flags are read left to right, so the first glued token is the one reported
check "first glued token wins" 22 bucket list -ibanana -bdemo
# flag position is what matters, not position relative to the command words
check "glued short before command rejected" 22 -ibanana bucket list

# ============================================================
echo ""
echo "=== flags that eat command words, stray tokens ==="
# ============================================================
# A flag that takes a value consumes the next token whatever it is. When that
# token is a command word it is eaten, and the command is resolved from the
# words that remain.

# --access-key: value consumed, command runs, user init fails on the key
check_cluster "list: --access-key consumed" 22 -- bucket --access-key foo list
check_cluster "list: --access-key twice" 22 -- bucket --access-key foo list --access-key foo2
check "list: unknown flag after --access-key" 22 bucket --access-key foo --backet name list
check_cluster "list: --access-key with misplaced --bucket" 22 -- bucket --access-key foo --bucket name list

# eaten command word: whatever survives is parsed as the command
check_cluster "stats: eaten 'list', sibling command survives" 22 -- bucket --access-key list stats
check_cluster "stats: eaten 'list', underscore spelling" 22 -- bucket --access_key list stats
check_cluster "list: eaten 'list', duplicate survives" 22 -- bucket --access-key list list
check "bucket: eaten 'list', stray word survives" 1 bucket --access-key list banana
# --format takes the next word as its value, so no command word is left
check "bucket: --format eats the command word" 1 bucket --format list

# unknown flags are rejected by name
check "list: unknown flag before --max-entries" 22 bucket list --banana --max-entries=5
check "list: unknown flag before --bucket" 22 bucket list --banana --bucket bananana
# the int value is rejected before the unknown flag is reached
check "list: unknown flag with unparsable int value" 22 bucket list --banana --max-entries=abc
# unknown command: --bucket takes '--banana', leaving 'banana' unresolvable
check "unknown command: flag takes the next token" 1 banana --bucket --banana
check "unknown command: unparsable int value" 22 banana --max-entries=abc --banana

# single-dash spellings are not flags
check "list: -uid rejected" 22 bucket list -uid u1 extra
check "list: -uid=u1 rejected" 22 bucket list -uid=u1 extra
# the same spellings without the trailing stray word
check "list: -uid rejected (alone)" 22 bucket list -uid u1
check "list: -uid=u1 rejected (alone)" 22 bucket list -uid=u1
# stray words after a consumed flag value
check "list: stray word after --uid value" 1 bucket list --uid u1 extra
check "list: stray word after --uid= value" 1 bucket list --uid=u1 extra
check "list: repeated command word" 1 bucket list list
check "list: 'bucket' repeated" 1 bucket bucket list
check "list: 'bucket' repeated twice" 1 bucket bucket bucket list
check "list: 'bucket' repeated, alias first" 1 buckets bucket list
check "list: 'bucket' repeated after the command" 1 bucket list bucket list
check "logging list: repeated command word" 1 bucket logging logging list
check "list: empty stray word" 1 bucket list ""

# binary flag, space form: the value is consumed only when it is an exact
# bool, so anything else is left behind as a stray
check "list: --allow-unordered banana (left as stray)" 1 bucket list --allow-unordered banana
check "list: --allow-unordered '' (left as stray)" 1 bucket list --allow-unordered ""
check_cluster "list: --allow-unordered 1 (bool consumed)" 0 -- bucket list --allow-unordered 1

# empty values are consumed like any other value
check_cluster "list: --access-key '' consumed" 0 -- bucket --access-key "" list
check_cluster "list: --bucket '' lists all buckets" 0 -- bucket list --bucket ""

# commands outside the bucket set, and unknown command words
check_cluster "user info: no --uid or --access-key" 22 -- user info
check "unknown command" 1 banana list
check "unknown command repeated" 1 banana banana list
# a subcommand word on its own is not a command
check "subcommand word alone" 1 list

# the same flags work on commands outside the bucket tree
check_cluster "reshard list: --max-entries=5" 0 -- reshard list --max-entries=5
check "reshard list: --max-entries=abc rejected" 22 reshard list --max-entries=abc
check_cluster "reshard list: --bucket takes the next token" 0 -- reshard list --bucket --banana
check_cluster "reshard list: --bucket name" 0 -- reshard list --bucket name

# ============================================================
echo ""
echo "=== which token a flag takes as its value ==="
# ============================================================
# In the space form a binary flag takes the next token only when it is exactly
# true, 1, false or 0; otherwise only the flag is consumed and the token stays
# on the line, where it is used as a command word if it can be, and reported if
# not.

# a command word is never taken as a binary flag's value
check_cluster "list: --fix before the command" 0 -- bucket --fix list
check_cluster "check: --fix before the command" 0 -- bucket --fix check --bucket demo
check_cluster "list: --fix before any command word" 0 -- --fix bucket list
check_cluster "list: --fix before the command, bool value" 0 -- bucket --fix true list
check_cluster "list: --allow-unordered before the command" 0 -- bucket --allow-unordered list
check_cluster "list: --allow-unordered, bool value" 0 -- bucket --allow-unordered true list
check_cluster "list: --fix with a bool after the command" 0 -- bucket list --fix true
# in the '=' form a binary flag's value belongs to the token, so it is always
# consumed; a value that is not true/1/false/0 is accepted and nothing is left
# behind
check_cluster "list: --fix=banana before the command" 0 -- bucket --fix=banana list

# a non-bool word is left on the line, so it is reported as a stray
check "list: --fix before the command leaves banana as a stray argument" 1 bucket --fix banana list
check "list: --allow-unordered leaves banana as a stray argument" 1 bucket --allow-unordered banana list
check "list: --fix after the command leaves banana as a stray" 1 bucket list --fix banana
check "check: --fix leaves banana as a stray argument" 1 bucket check --fix banana
# --fix takes no value wherever it sits, so the repeated 'list' stays on the
# line and is reported there
check "bucket: --fix before the command, 'list' repeated" 1 bucket --fix list list
check "list: --fix leaves a repeated command word 'list' as a stray" 1 bucket list --fix list
check "check: --fix leaves 'list' as a stray argument" 1 bucket check --fix list

# --fix and --allow-unordered take no value, so a flag-shaped token is left on
# the line and rejected by name
check "list: --fix does not take a long flag" 22 bucket list --fix --banana
check "list: --fix does not take a short flag" 22 bucket list --fix -x
# a lone dash after a binary flag is left on the line and rejected as an invalid
# flag
check "list: --fix leaves a lone dash" 22 bucket list --fix -
check "check: --fix leaves a lone dash" 22 bucket check --fix -
check "bucket: --fix leaves a lone dash before the command" 22 bucket --fix - list
check "bucket: --allow-unordered leaves a lone dash" 22 bucket --allow-unordered - list
check "list: --allow-unordered leaves a lone dash" 22 bucket list --allow-unordered -
check "list: --fix leaves a negative number" 22 bucket list --fix -5

# a flag that requires a value takes the next token whatever it is
check_cluster "stats: --format takes 'list', 'stats' is the command" 1 -- bucket --format list stats
check_cluster "stats: --bucket takes the command word" 2 -- bucket --bucket list stats
check_cluster "list: --bucket takes a lone dash" 2 -- bucket --bucket - list
check "bucket: --bucket takes a lone dash, no command left" 1 bucket --bucket -
check_cluster "list: --bucket takes --max-entries as its value" 2 -- bucket list --bucket --max-entries
check "list: --max-entries takes --bucket as its value" 22 bucket list --max-entries --bucket
# a binary flag leaves the following flag alone, so it is the one left short
check "list: --fix leaves --bucket without a value" 1 bucket list --fix --bucket
check "check: --fix leaves --bucket without a value" 1 bucket check --fix --bucket

# --categories takes its value whatever it is; when that value is a command
# word the command is parsed from what remains
check_cluster "list: --categories takes its value" 0 -- bucket --categories foo list
check_cluster "list: --categories after the command" 0 -- bucket list --categories foo
check "bucket: --categories takes the command word" 1 bucket --categories list
check_cluster "stats: --categories takes 'list', 'stats' survives" 0 -- bucket --categories list stats

# a flag the bucket commands do not use still takes the next token as its
# value, even when that token is flag-shaped. --access-key is checked and
# fails; nothing checks --secret-key, so the same line runs to completion.
check_cluster "list: --access-key takes a bare word as its value" 22 -- bucket list --access-key banana
check_cluster "list: --secret-key takes a bare word as its value" 0 -- bucket list --secret-key banana
check_cluster "list: --access-key takes --bucket as its value" 22 -- bucket list --access-key --bucket
check_cluster "list: --secret-key takes --bucket as its value" 0 -- bucket list --secret-key --bucket
check_cluster "stats: --access-key takes --bucket as its value" 22 -- bucket stats --access-key --bucket
check_cluster "stats: --secret-key takes --bucket as its value" 0 -- bucket stats --secret-key --bucket
check "list: --access-key takes --bucket, 'demo' is left as a command word" 1 bucket list --access-key --bucket demo
check "list: --secret-key takes --bucket, 'demo' is left as a command word" 1 bucket list --secret-key --bucket demo

# with no command word at all, only the flags are read
check "unknown flag with no command" 22 --banana
check "--fix leaves a lone dash with no command" 22 --fix -
check "lone dash with no command" 22 -
# a known flag is consumed, so what is left is the missing command word:
# exit 1, not the 22 an unknown flag gives
check "known flag with no command" 1 --fix
check "known flag takes the unknown token, no command left" 1 --bucket --banana

# ============================================================
echo ""
echo "=== two errors on one line ==="
# ============================================================
# These lines carry two errors at once: a missing subcommand and a bad flag.
# Repairing either one leaves the other, so each error is pinned on its own below.

check "bucket: unknown flag and no subcommand" 22 bucket --banana
check "bucket: lone dash and no subcommand" 22 bucket -
check "bucket: --fix leaves a dash and no subcommand" 22 bucket --fix -
check "bucket: --allow-unordered leaves a dash, no subcommand" 22 bucket --allow-unordered -
check "list: unknown flag and --bucket without a value" 22 bucket list --banana --bucket

# each of those errors on its own. Removing a token is not enough to isolate a
# missing subcommand -- the command word has to be supplied. The bare 'bucket'
# half is pinned in the bucket (bare) section, and 'bucket list --bucket' in
# the bucket list section.
check "bucket: --fix and no subcommand" 1 bucket --fix
check "check: lone dash on a complete command" 22 bucket check -
check "bucket: lone dash before the command word" 22 bucket - list
check "list: lone dash after the command" 22 bucket list -
check "list: unknown flag on a complete command" 22 bucket list --banana

# ============================================================
echo ""
echo "=== 'bucket' as an ordinary word ==="
# ============================================================
# Besides naming the bucket commands, the word 'bucket' ends a command name,
# names a metadata section, and can be any flag's value. These rows pin that it
# is treated as an ordinary word in each of those positions. Most are paired
# with the same line using a different word, and the two results match.

# 'bucket' also ends a command name: 'reshard bucket' is the alias form of 'bucket reshard'
# --num-shards is checked after the bucket name, so this one got into the
# handler. reshard hands -EINVAL back negative, so the exit code is 234
check_cluster "reshard bucket: --num-shards not specified" 234 -- reshard bucket --bucket demo

# 'bucket' as a metadata section name. Every verb that takes a bare section
# name reaches its handler with it. 'metadata put' is left out on purpose: it
# reads stdin, so a row for it would block the suite.
check_cluster "metadata list bucket" 0 -- metadata list bucket
check_cluster "metadata list bucket, json format" 0 -- metadata list bucket --format json
check_cluster "metadata get bucket" 22 -- metadata get bucket
check_cluster "metadata rm bucket" 22 -- metadata rm bucket
check_cluster "metadata list bucket.instance" 0 -- metadata list bucket.instance
check_cluster "metadata list user" 0 -- metadata list user

# 'bucket' as a flag's value, on commands outside the bucket family
check_cluster "user info: --access-key bucket" 22 -- user info --access-key bucket
check_cluster "user info: --access-key banana" 22 -- user info --access-key banana
check_cluster "period get: --period bucket" 2 -- period get --period bucket
check_cluster "period get: --period banana" 2 -- period get --period banana
check_cluster "user create: --display-name bucket" 22 -- user create --display-name bucket
check_cluster "user create: --display-name banana" 22 -- user create --display-name banana
# the same shape on a command that SUCCEEDS: the word is taken as the value and
# the command still runs to completion
check_cluster "gc list: --period bucket" 0 -- gc list --period bucket
check_cluster "gc list: --period banana" 0 -- gc list --period banana

# 'bucket' as a flag's value on the bucket commands themselves, before and
# after the command words
check_cluster "list: --access-key bucket" 22 -- bucket list --access-key bucket
check_cluster "stats: --access-key bucket" 22 -- bucket stats --access-key bucket
check_cluster "list: --access-key bucket before the command words" 22 -- --access-key bucket bucket list
check_cluster "list: --access-key script before the command words" 22 -- --access-key script bucket list
check "bucket: --access-key bucket, no subcommand" 1 bucket --access-key bucket

# a flag the command does use takes the word as its value first
check_cluster "user info: --bucket bucket" 22 -- user info --bucket bucket
check_cluster "metadata list: --bucket bucket" 0 -- metadata list --bucket bucket
check_cluster "user info: --uid bucket" 22 -- user info --uid bucket

# a global flag before the command words
check_cluster "metadata list bucket: --tenant before the command" 22 -- --tenant t metadata list bucket

# ============================================================
echo ""
echo "=== integration: bucket list and stats (cluster) ==="
# ============================================================

# bucket list with no args lists all buckets (may be empty) — always succeeds
check_cluster "integration: bucket list (all)" 0 -- bucket list

# bucket list via buckets alias
check_cluster "integration: buckets list (alias)" 0 -- buckets list

# bucket list for a nonexistent bucket errors
check_cluster "integration: bucket list nonexistent" 2 -- bucket list --bucket nonexistent_test_xyz

# bucket stats with no args lists all bucket stats — always succeeds
check_cluster "integration: bucket stats (all)" 0 -- bucket stats

# bucket stats for a nonexistent bucket
check_cluster "integration: bucket stats nonexistent" 2 -- bucket stats --bucket nonexistent_test_xyz

# a glued-short-looking token in value position is a value, never rejected
# (it is taken as a bucket named "-ibanana", which does not exist)
check_cluster "integration: glued token in value position is a value" 2 -- bucket stats --bucket -ibanana

# bucket check with no args runs index check — always succeeds (even with 0 buckets)
check_cluster "integration: bucket check (all)" 0 -- bucket check

# 'buckets' is only accepted for list; stats and check are unrecognized
check "integration: buckets stats (alias)" 1 buckets stats
check "integration: buckets check (alias)" 1 buckets check

# --bucket-id without --bucket triggers rgw_find_bucket_by_id path
# an unknown bucket id gives -ENOENT, so the exit code is 254
check_cluster "integration: bucket stats --bucket-id nonexistent" 254 -- bucket stats --bucket-id nonexistent_id_test

# --inconsistent-index + --yes-i-really-mean-it suppresses the warning and proceeds
check_cluster "integration: rm --inconsistent-index --yes-i-really-mean-it (nonexistent)" 0 -- bucket rm --bucket nonexistent_test --inconsistent-index --yes-i-really-mean-it

# ============================================================
echo ""
echo "=== integration: full bucket lifecycle (cluster) ==="
# ============================================================
# Creates a test user and bucket, runs link/unlink/rm, then cleans up.
# Skipped automatically if no cluster is running.

_test_uid="bucket_test_user"
_test_bucket="bucket-test"
_test_display="Bucket Test User"

if cluster_running; then
  # Create a test user
  "$RGW_ADMIN" user create --uid "$_test_uid" --display-name "$_test_display" \
    >/dev/null 2>&1

  check_cluster "integration: bucket list --uid (owner with no buckets)" 0 -- bucket list --uid "$_test_uid"

  # The rows below need a bucket that exists: 'bucket link' only links one
  # that is already there, so the aws CLI creates it, and they are skipped
  # when it is not installed.
  _aws_available=0
  if command -v aws >/dev/null 2>&1; then
    _aws_available=1
  fi

  # Nothing in this block runs unless the aws CLI and the credentials are there.
  # The SKIP lines below stand in for all of its rows, so update them together.
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
      check_cluster "integration: bucket list (named, empty)" 0 -- bucket list --bucket "$_test_bucket"

      # short flags -b and -i work the same as --bucket and --uid
      check_cluster "integration: bucket list -b (short flag)" 0 -- bucket list -b "$_test_bucket"
      check_cluster "integration: bucket stats -b (short flag)" 0 -- bucket stats -b "$_test_bucket"

      # bucket stats: returns stats for the test bucket
      check_cluster "integration: bucket stats (named)" 0 -- bucket stats --bucket "$_test_bucket"

      # '=' normalization shapes that execute (cluster):
      # string flag with empty '=': the value is "", command runs
      check_cluster "integration: empty-= on string flag runs" 0 -- bucket stats --bucket=
      # empty-= must not eat a following flag
      check_cluster "integration: empty-= then flag parses normally" 0 -- bucket list --bucket= --max-entries 7
      # binary flag empty-= keeps its silent-set behavior
      check_cluster "integration: binary flag empty-= unchanged" 0 -- bucket list --bucket "$_test_bucket" --allow-unordered=
      # -i=<uid>: the value is captured correctly (lists the user's bucket)
      check_cluster "integration: -i=uid captures value" 0 -- bucket list -i="$_test_uid"
      # value position: a token after a value-taking flag is its value, even
      # if it looks like a flag (the handler then fails)
      check_cluster "integration: flag value may look like a flag" 2 -- bucket stats --bucket --max-entries
      check_cluster "lifecycle: bucket list --allow-unordered" 0 -- bucket list --allow-unordered --bucket "$_test_bucket"
      check_cluster "lifecycle: bucket list --format json" 0 -- bucket list --format json --bucket "$_test_bucket"
      check_cluster "lifecycle: bucket stats --show-restore-stats" 0 -- bucket stats --show-restore-stats --bucket "$_test_bucket"
      check_cluster "lifecycle: bucket stats --format json" 0 -- bucket stats --format json --bucket "$_test_bucket"

      # bucket layout: dumps the bucket's layout as JSON (index/log generations)
      check_cluster "integration: bucket layout" 0 -- bucket layout --bucket "$_test_bucket"
      check_cluster "integration: bucket layout --format json" 0 -- bucket layout --bucket "$_test_bucket" --format json
      check_cluster "integration: bucket layout --tenant ''" 0 -- bucket layout --bucket "$_test_bucket" --tenant ""

      # bucket rewrite: rewrites all objects in the bucket (empty bucket -> empty
      # "objects" array, exit 0). Exercises -b, --format, the size flags, the
      # date aliases, and the atoll path on a REAL bucket.
      check_cluster "integration: bucket rewrite" 0 -- bucket rewrite --bucket "$_test_bucket"
      check_cluster "integration: bucket rewrite -b (short flag)" 0 -- bucket rewrite -b "$_test_bucket"
      check_cluster "integration: bucket rewrite --format json" 0 -- bucket rewrite --bucket "$_test_bucket" --format json
      check_cluster "integration: bucket rewrite --min-rewrite-size (numeric)" 0 -- bucket rewrite --bucket "$_test_bucket" --min-rewrite-size 1
      check_cluster "integration: bucket rewrite --min-rewrite-size=abc (atoll -> 0)" 0 -- bucket rewrite --bucket "$_test_bucket" --min-rewrite-size=abc
      check_cluster "integration: bucket rewrite --start-time/--end-time (aliases)" 0 -- bucket rewrite --bucket "$_test_bucket" --start-time 2000-01-01 --end-time 2100-01-01
      # bad date is parsed AFTER init_bucket, so on a real bucket it reaches the
      # date check and fails with exit 22
      check_cluster "integration: bucket rewrite bad --start-date (exit 22)" 22 -- bucket rewrite --bucket "$_test_bucket" --start-date notadate

      # status assertions: with a small (<4MB) object present, the per-object
      # "status" field proves the filters actually work. Default min (4MB) skips
      # it; atoll("abc")=0 disables the min filter so it is rewritten (Success);
      # a past --end-time filters it out by date (Skipped).
      echo "rewrite-status-probe" > /tmp/rw_small.txt
      AWS_ACCESS_KEY_ID="$_access_key" \
      AWS_SECRET_ACCESS_KEY="$_secret_key" \
      aws --endpoint-url "$_rgw_endpoint" \
        s3 cp /tmp/rw_small.txt "s3://$_test_bucket/" >/dev/null 2>&1

      check_cluster "integration: rewrite small obj Skipped (default 4M min)" 0 -- bucket rewrite --bucket "$_test_bucket"
      check_cluster "integration: rewrite --min-rewrite-size=abc Success (atoll->0)" 0 -- bucket rewrite --bucket "$_test_bucket" --min-rewrite-size=abc
      check_cluster "integration: rewrite --min-rewrite-size 1 Success" 0 -- bucket rewrite --bucket "$_test_bucket" --min-rewrite-size 1
      check_cluster "integration: rewrite past --end-time Skipped (date filter)" 0 -- bucket rewrite --bucket "$_test_bucket" --min-rewrite-size 1 --end-time 2000-01-01

      # remove the probe object so later lifecycle tests see an empty bucket
      AWS_ACCESS_KEY_ID="$_access_key" \
      AWS_SECRET_ACCESS_KEY="$_secret_key" \
      aws --endpoint-url "$_rgw_endpoint" \
        s3 rm "s3://$_test_bucket/rw_small.txt" >/dev/null 2>&1
      rm -f /tmp/rw_small.txt

      # bucket chown: chown to the (already-owning) test user — a no-op ownership
      # change that still exercises the full chown path; exit 0, no output
      check_cluster "integration: bucket chown" 0 -- bucket chown --bucket "$_test_bucket" --uid "$_test_uid"

      # bucket limit check for the test user: JSON with user_id + buckets
      check_cluster "integration: limit check --uid" 0 -- bucket limit check --uid "$_test_uid"
      check_cluster "integration: limit check --uid --warnings-only" 0 -- bucket limit check --uid "$_test_uid" --warnings-only

      # bucket set-min-shards: set the dynamic-resharding minimum on the (Normal)
      # test bucket; succeeds with no output (exit 0). Assert the value actually
      # changed by reading it back via bucket layout. Exercises -b, the =form,
      # and an empty --tenant.
      check_cluster "integration: bucket set-min-shards (num 7)" 0 -- bucket set-min-shards --bucket "$_test_bucket" --num-shards 7
      check_cluster "integration: set-min-shards effect (layout shows 7)" 0 -- bucket layout --bucket "$_test_bucket"
      check_cluster "integration: bucket set-min-shards -b --num-shards=9 (short + =form)" 0 -- bucket set-min-shards -b "$_test_bucket" --num-shards=9
      check_cluster "integration: set-min-shards effect (layout shows 9)" 0 -- bucket layout --bucket "$_test_bucket"
      check_cluster "integration: bucket set-min-shards --tenant '' (empty)" 0 -- bucket set-min-shards --bucket "$_test_bucket" --num-shards 11 --tenant ""

      # bucket object shard: pure computation (no bucket needed), but runs after
      # driver init so a cluster is required. Deterministic: foo % 11 -> 10,
      # any object % 1 -> 0. Exercises -o short form, =form, and --format xml.
      check_cluster "integration: object shard (foo/11 -> 10)" 0 -- bucket object shard --object foo --num-shards 11
      check_cluster "integration: object shard (foo/1 -> 0)" 0 -- bucket object shard --object foo --num-shards 1
      check_cluster "integration: object shard -o --num-shards=11 (short + =form)" 0 -- bucket object shard -o foo --num-shards=11
      check_cluster "integration: object shard --format xml" 0 -- bucket object shard --object foo --num-shards 11 --format xml

      # bucket shard objects: pure computation (no bucket needed), runs after
      # driver init. Deterministic sample object names per shard; --shard-id
      # picks one shard; --prefix changes the name prefix (default "obj").
      # Exercises the 'shard object' alias, =form, --prefix "", and --format xml.
      check_cluster "integration: shard objects (num 4, lists objs)" 0 -- bucket shard objects --num-shards 4
      check_cluster "integration: shard objects (num 1 -> single obj)" 0 -- bucket shard objects --num-shards 1
      check_cluster "integration: shard objects --shard-id 1" 0 -- bucket shard objects --num-shards 4 --shard-id 1
      check_cluster "integration: shard object (alias) --shard-id=1 (=form)" 0 -- bucket shard object --num-shards 4 --shard-id=1
      check_cluster "integration: shard objects --prefix myobj" 0 -- bucket shard objects --num-shards 4 --prefix myobj
      check_cluster "integration: shard objects --prefix '' (engaged empty)" 0 -- bucket shard objects --num-shards 4 --prefix ""
      check_cluster "integration: shard objects --format xml" 0 -- bucket shard objects --num-shards 4 --shard-id 0 --format xml

      # bucket resync encrypted multipart: a repair op. On a non-replicated single-zone
      # cluster it needs --yes-i-really-mean-it; without it -> EPERM (exit 1). With it,
      # runs and emits the "modified" report (exit 0, idempotent on a normal bucket).
      # The binary flag's =false / space-form 'false' both leave it unset -> EPERM.
      check_cluster "integration: resync without --yes (EPERM)" 1 -- bucket resync encrypted multipart --bucket "$_test_bucket"
      check_cluster "integration: resync --yes-i-really-mean-it (success)" 0 -- bucket resync encrypted multipart --bucket "$_test_bucket" --yes-i-really-mean-it
      check_cluster "integration: resync --yes-i-really-mean-it=false (=form -> EPERM)" 1 -- bucket resync encrypted multipart --bucket "$_test_bucket" --yes-i-really-mean-it=false
      check_cluster "integration: resync --yes-i-really-mean-it false (space form -> EPERM)" 1 -- bucket resync encrypted multipart --bucket "$_test_bucket" --yes-i-really-mean-it false
      # a value that is not true/1/false/0 still leaves the flag set, so =banana
      # runs the repair where =false refuses it
      check_cluster "integration: resync --yes-i-really-mean-it=banana (=form, non-bool)" 0 -- bucket resync encrypted multipart --bucket "$_test_bucket" --yes-i-really-mean-it=banana

      # bucket radoslist: read-only, lists the rados objects backing the bucket
      # (exit 0). Exercises both entry points (radoslist + 'rados list' alias),
      # the -b short form, and the --rgw-obj-fs field separator.
      check_cluster "integration: radoslist --bucket" 0 -- bucket radoslist --bucket "$_test_bucket"
      check_cluster "integration: radoslist -b (short)" 0 -- bucket radoslist -b "$_test_bucket"
      check_cluster "integration: radoslist --rgw-obj-fs" 0 -- bucket radoslist --bucket "$_test_bucket" --rgw-obj-fs ":"
      check_cluster "integration: rados list --bucket (alias)" 0 -- bucket rados list --bucket "$_test_bucket"

      # bucket logging on a bucket WITHOUT logging configured: info is silent
      # (exit 0, no output); list and flush print an error but still exit 0
      check_cluster "integration: logging info (no logging, silent)" 0 -- bucket logging info --bucket "$_test_bucket"
      check_cluster "integration: logging list (no logging, msg + exit 0)" 0 -- bucket logging list --bucket "$_test_bucket"
      check_cluster "integration: logging flush (no logging, msg + exit 0)" 0 -- bucket logging flush --bucket "$_test_bucket"

      # bucket unlink: unlink the bucket from the user
      check_cluster "integration: bucket unlink" 0 -- bucket unlink --bucket "$_test_bucket" --uid "$_test_uid"

      # re-link using short flags -b and -i
      check_cluster "integration: bucket link -b -i (short flags)" 0 -- bucket link -b "$_test_bucket" -i "$_test_uid"

      # bucket unlink using short flags
      check_cluster "integration: bucket unlink -b -i (short flags)" 0 -- bucket unlink -b "$_test_bucket" -i "$_test_uid"

      # bucket link: re-link for remaining tests
      check_cluster "integration: bucket link" 0 -- bucket link --bucket "$_test_bucket" --uid "$_test_uid"

      # bucket check: check the bucket index, with and without --fix
      check_cluster "integration: bucket check (named)" 0 -- bucket check --bucket "$_test_bucket"
      check_cluster "lifecycle: bucket check --fix" 0 -- bucket check --fix --bucket "$_test_bucket"

      # bucket check olh and unlinked with named bucket and new flags
      check_cluster "integration: bucket check olh (named)" 0 -- bucket check olh --bucket "$_test_bucket"
      check_cluster "lifecycle: bucket check olh --fix (named)" 0 -- bucket check olh --fix --bucket "$_test_bucket"
      check_cluster "lifecycle: bucket check olh --dump-keys (named)" 0 -- bucket check olh --dump-keys --bucket "$_test_bucket"
      check_cluster "integration: bucket check unlinked (named)" 0 -- bucket check unlinked --bucket "$_test_bucket"
      check_cluster "lifecycle: bucket check unlinked --fix (named)" 0 -- bucket check unlinked --fix --bucket "$_test_bucket"
      check_cluster "lifecycle: bucket check unlinked --dump-keys (named)" 0 -- bucket check unlinked --dump-keys --bucket "$_test_bucket"

      # bucket sync on a real bucket. This is a single-zone cluster, so nothing
      # is replicated: info and checkpoint report that sync is disabled, and the
      # three leaves that need a source zone fail to resolve the zone name.
      # That failure warns first and errors after; these rows assert the error,
      # except the status row, which only warns.
      check_cluster "integration: sync info" 0 -- bucket sync info --bucket "$_test_bucket"
      check_cluster "integration: sync info -b (short)" 0 -- bucket sync info -b "$_test_bucket"
      check_cluster "integration: sync status" 0 -- bucket sync status --bucket "$_test_bucket"
      check_cluster "integration: sync status --format json" 0 -- bucket sync status --bucket "$_test_bucket" --format json
      check_cluster "integration: sync status --source-zone (unknown zone)" 0 -- bucket sync status --bucket "$_test_bucket" --source-zone z1
      check_cluster "integration: sync checkpoint" 0 -- bucket sync checkpoint --bucket "$_test_bucket"
      check_cluster "integration: sync checkpoint --timeout-sec --retry-delay-ms" 0 -- bucket sync checkpoint --bucket "$_test_bucket" --timeout-sec 1 --retry-delay-ms 10
      check_cluster "integration: sync markers (unknown source zone)" 22 -- bucket sync markers --source-zone z1 --bucket "$_test_bucket"
      check_cluster "integration: sync init (unknown source zone)" 22 -- bucket sync init --source-zone z1 --bucket "$_test_bucket"
      check_cluster "integration: sync run (unknown source zone)" 22 -- bucket sync run --source-zone z1 --bucket "$_test_bucket"
      # --source-bucket names a second bucket, which is looked up as well
      check_cluster "integration: sync init --source-bucket nonexistent" 2 -- bucket sync init --source-zone z1 --source-bucket no-such-bucket --bucket "$_test_bucket"
      # disable and enable both succeed without printing anything
      check_cluster "integration: sync disable" 0 -- bucket sync disable --bucket "$_test_bucket"
      check_cluster "integration: sync enable" 0 -- bucket sync enable --bucket "$_test_bucket"

      # bucket reshard on a real bucket. Resharding up needs nothing extra;
      # resharding to the same or fewer shards needs --yes-i-really-mean-it.
      # The test bucket starts at the default 11 index shards. A refusal exits
      # 234, because reshard hands -EINVAL back negative.
      check_cluster "integration: reshard down without --yes" 234 -- bucket reshard --bucket "$_test_bucket" --num-shards 1
      check_cluster "integration: reshard up" 0 -- bucket reshard --bucket "$_test_bucket" --num-shards 23
      check_cluster "integration: reshard down with --yes-i-really-mean-it" 0 -- bucket reshard --bucket "$_test_bucket" --num-shards 5 --yes-i-really-mean-it
      check_cluster "integration: reshard bucket (alias)" 0 -- reshard bucket --bucket "$_test_bucket" --num-shards 9
      check_cluster "integration: reshard -b (short)" 0 -- bucket reshard -b "$_test_bucket" --num-shards 13
      check_cluster "integration: reshard --max-entries" 0 -- bucket reshard --bucket "$_test_bucket" --num-shards 17 --max-entries 10
      # --format is accepted but this command reports its progress as plain text
      # either way, so the output is the same as the rows above
      check_cluster "integration: reshard --format json (output unchanged)" 0 -- bucket reshard --bucket "$_test_bucket" --num-shards 19 --format json
      check_cluster "integration: reshard --yes-i-really-mean-it=false (=form)" 234 -- bucket reshard --bucket "$_test_bucket" --num-shards 5 --yes-i-really-mean-it=false

      # bucket rm: remove the test bucket (it's empty, so no --purge-objects needed)
      check_cluster "integration: bucket rm" 0 -- bucket rm --bucket "$_test_bucket"

      # bucket rm via 'remove' alias — re-create then remove
      AWS_ACCESS_KEY_ID="$_access_key" \
      AWS_SECRET_ACCESS_KEY="$_secret_key" \
      aws --endpoint-url "$_rgw_endpoint" \
        s3 mb "s3://$_test_bucket" >/dev/null 2>&1

      check_cluster "integration: bucket remove (alias for rm)" 0 -- bucket remove --bucket "$_test_bucket"

      # Re-create to test --purge-objects (bucket is empty, so purge is a no-op)
      AWS_ACCESS_KEY_ID="$_access_key" \
      AWS_SECRET_ACCESS_KEY="$_secret_key" \
      aws --endpoint-url "$_rgw_endpoint" \
        s3 mb "s3://$_test_bucket" >/dev/null 2>&1

      check_cluster "lifecycle: bucket rm --purge-objects (empty bucket)" 0 -- bucket rm --purge-objects --bucket "$_test_bucket"
    else
      echo "SKIP [integration: lifecycle tests]: could not get credentials for test user"
      SKIP=$((SKIP+94))
    fi
  else
    echo "SKIP [integration: lifecycle tests]: aws CLI not available (needed to create test bucket)"
    SKIP=$((SKIP+94))
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
