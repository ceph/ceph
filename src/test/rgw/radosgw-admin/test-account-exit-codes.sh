#!/bin/bash
# Exit-code tests for radosgw-admin account commands (account.cc module).
#
# Usage:
#   ./test-account-exit-codes.sh
#   RGW_ADMIN=/path/to/radosgw-admin ./test-account-exit-codes.sh
#   CEPH_CONF=/path/to/ceph.conf ./test-account-exit-codes.sh
#
# Test types:
#   check()        - no cluster needed; runs with --no-mon-config
#   check_cluster()- needs a running cluster, SKIPs when there is none
#
# Both verify the exit code.
#
# Run from the build directory:
#   cd /path/to/ceph/build && bash /path/to/test-account-exit-codes.sh

. "`dirname $0`/test-radosgw-admin-exit-codes-common.sh"

# ============================================================
echo "=== account (bare) ==="
# ============================================================

# no arguments at all: the usage text, before any command is resolved
check "bare account" 1 account
check "unknown subcommand" 1 account banana
check "stray before account" 1 foo account list
check "stray after account" 1 account list foo

# ============================================================
echo ""
echo "=== account create ==="
# ============================================================

check "create: unrecognized flag" 22 account create --fakeflag
check "create: stray after flags" 1 account create --email x@y.com strayarg
check "create: stray before account" 1 foo account create

# missing option value
# Leaving the value out is the way to check that an option is defined as
# taking a value: a value-taking option should reject the command when its
# value is missing.
check "create: --account-name missing value" 1 account create --account-name
check "create: --account-id missing value" 1 account create --account-id
check "create: --email missing value" 1 account create --email
check "create: --max-users missing value" 1 account create --max-users
check "create: --max-roles missing value" 1 account create --max-roles
check "create: --max-groups missing value" 1 account create --max-groups
check "create: --max-access-keys missing value" 1 account create --max-access-keys
check "create: --max-buckets missing value" 1 account create --max-buckets
check "create: --format missing value" 1 account create --format

check "create: --max-users invalid int" 22 account create --max-users banana

# account id must be 20 bytes, start with RGW, and end with digits
check_cluster "create: --account-id wrong length" 22 -- \
  account create --account-id badid
check_cluster "create: --account-id too short" 22 -- \
  account create --account-id RGW123
check_cluster "create: --account-name contains $" 22 -- \
  account create --account-name 'bad$name'
check_cluster "create: --account-name contains :" 22 -- \
  account create --account-name 'bad:name'

# name and id are optional; the command auto-generates an id when omitted
check_cluster "create: no identity flags" 0 -- account create
check_cluster "create: --email only" 0 -- \
  account create --email "exit-code-test-$RANDOM@example.com"

# ============================================================
echo ""
echo "=== account get ==="
# ============================================================

check "get: unrecognized flag" 22 account get --fakeflag
check "get: stray after flags" 1 account get strayarg
check "get: stray before account" 1 foo account get
check "get: stray between account and get" 1 account extra get

check "get: --account-name missing value" 1 account get --account-name
check "get: --account-id missing value" 1 account get --account-id
check "get: --email missing value" 1 account get --email
check "get: --format missing value" 1 account get --format

check_cluster "get: no identity flags" 22 -- account get
check_cluster "get: nonexistent account" 2 -- \
  account get --account-name no-such-account-$RANDOM

# ============================================================
echo ""
echo "=== account modify ==="
# ============================================================

check "modify: unrecognized flag" 22 account modify --fakeflag
check "modify: stray after flags" 1 account modify strayarg

check "modify: --account-name missing value" 1 account modify --account-name
check "modify: --account-id missing value" 1 account modify --account-id
check "modify: --email missing value" 1 account modify --email
check "modify: --max-users missing value" 1 account modify --max-users
check "modify: --max-roles missing value" 1 account modify --max-roles

check "modify: --max-roles invalid int" 22 \
  account modify --max-roles banana

check_cluster "modify: no identity flags" 22 -- account modify
check_cluster "modify: nonexistent account" 2 -- \
  account modify --account-name no-such-account-$RANDOM

# ============================================================
echo ""
echo "=== account rm ==="
# ============================================================

check "rm: unrecognized flag" 22 account rm --fakeflag
check "rm: stray after flags" 1 account rm strayarg

check "rm: --account-name missing value" 1 account rm --account-name
check "rm: --account-id missing value" 1 account rm --account-id
check "rm: --email missing value" 1 account rm --email

check_cluster "rm: no identity flags" 22 -- account rm
check_cluster "rm: nonexistent account" 2 -- \
  account rm --account-name no-such-account-$RANDOM

# ============================================================
echo ""
echo "=== account stats ==="
# ============================================================

check "stats: unrecognized flag" 22 account stats --fakeflag
check "stats: stray after flags" 1 account stats strayarg

check "stats: --account-name missing value" 1 account stats --account-name
check "stats: --account-id missing value" 1 account stats --account-id
check "stats: --format missing value" 1 account stats --format

# stats requires an account identity, even when only asking for sync flags
check_cluster "stats: no identity flags" 22 -- account stats
check_cluster "stats: --sync-stats without identity" 22 -- account stats --sync-stats
check_cluster "stats: --reset-stats without identity" 22 -- account stats --reset-stats
check_cluster "stats: nonexistent account" 2 -- \
  account stats --account-name no-such-account-$RANDOM

# ============================================================
echo ""
echo "=== account list ==="
# ============================================================

check "list: unrecognized flag" 22 account list --fakeflag
check "list: stray after flags" 1 account list strayarg
check "list: stray before account" 1 foo account list
check "list: stray between account and list" 1 account extra list

check "list: --max-entries missing value" 1 account list --max-entries
check "list: --marker missing value" 1 account list --marker
check "list: --format missing value" 1 account list --format

check "list: --max-entries invalid int" 22 account list --max-entries banana
check "list: --max-entries out of int range" 22 \
  account list --max-entries 5000000000

check_cluster "list: --max-entries negative" 22 -- account list --max-entries -1
check_cluster "list: default" 0 -- account list
check_cluster "list: --max-entries 10" 0 -- account list --max-entries 10
check_cluster "list: --format json" 0 -- --format json account list

# ============================================================
echo ""
echo "=== flags: underscore vs dash spelling ==="
# ============================================================
# Long option names accept '_' as well as '-'. Each pair below proves the
# underscore spelling behaves identically to the dash spelling.

check "list: --max-entries space form (dash)" 22 account list --max-entries banana
check "list: --max_entries space form (underscore)" 22 account list --max_entries banana
check "list: --max-entries= form (dash)" 22 account list --max-entries=banana
check "list: --max_entries= form (underscore)" 22 account list --max_entries=banana

check_cluster "rm: --purge-data (dash)" 2 -- \
  account rm --account-name no-such-account --purge-data
check_cluster "rm: --purge_data (underscore)" 2 -- \
  account rm --account-name no-such-account --purge_data

check_cluster "list: underscore spelling on success path" 0 -- \
  account list --max_entries 100

# ============================================================
echo ""
echo "=== integration: account lifecycle ==="
# ============================================================
# Create, read, modify, stats, list, and remove a real account on a live cluster.

if cluster_running; then
  _test_acct="exit-code-acct-$$"

  check_cluster "integration: create" 0 -- \
    account create --account-name "$_test_acct"
  check_cluster "integration: get" 0 -- \
    account get --account-name "$_test_acct"
  check_cluster "integration: modify" 0 -- \
    account modify --account-name "$_test_acct" --email "${_test_acct}@example.com"
  check_cluster "integration: stats" 0 -- \
    account stats --account-name "$_test_acct"
  check_cluster "integration: list" 0 -- account list
  check_cluster "integration: rm --purge-data" 0 -- \
    account rm --account-name "$_test_acct" --purge-data
  check_cluster "integration: get after rm" 2 -- \
    account get --account-name "$_test_acct"
else
  echo "SKIP [integration: account lifecycle]: no cluster running"
  SKIP=$((SKIP+1))
fi

report_results
