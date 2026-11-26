#!/bin/bash
# Exit-code tests for radosgw-admin user commands (user.cc module).
#
# Usage:
#   ./test-user-exit-codes.sh
#   RGW_ADMIN=/path/to/radosgw-admin ./test-user-exit-codes.sh
#   CEPH_CONF=/path/to/ceph.conf ./test-user-exit-codes.sh
#
# Test types:
#   check()         - no cluster needed; runs with --no-mon-config
#   check_cluster() - needs a running cluster, SKIPs when there is none
#
# Both verify the exit code.
#
# Run from the build directory:
#   cd /path/to/ceph/build && bash /path/to/test-user-exit-codes.sh

. "`dirname $0`/test-radosgw-admin-exit-codes-common.sh"

# ============================================================
echo "=== user (bare) ==="
# ============================================================

check "bare user" 1 user
check "unknown subcommand" 1 user banana
check "stray before user" 1 foo user list
check "stray after user" 1 user list foo

# ============================================================
echo ""
echo "=== user info ==="
# ============================================================

check "info: unrecognized flag" 22 user info --fakeflag
check "info: stray after flags" 1 user info --uid u strayarg
check "info: --uid missing value" 1 user info --uid
check "info: --access-key missing value" 1 user info --access-key
check_cluster "info: no identity flags" 22 -- user info

# ============================================================
echo ""
echo "=== user create ==="
# ============================================================

check "create: unrecognized flag" 22 user create --fakeflag
check "create: --uid missing value" 1 user create --uid
check "create: --display-name missing value" 1 user create --display-name
check "create: --email missing value" 1 user create --email
check "create: --max-buckets invalid int" 22 user create --uid u --max-buckets banana
check_cluster "create: no display-name" 22 -- user create --uid u-no-display

# ============================================================
echo ""
echo "=== user modify / enable / suspend ==="
# ============================================================

check "modify: unrecognized flag" 22 user modify --fakeflag
check_cluster "modify: no uid" 22 -- user modify --email x@y.com
check_cluster "enable: no uid" 22 -- user enable
check_cluster "suspend: no uid" 22 -- user suspend

# ============================================================
echo ""
echo "=== user rm / rename ==="
# ============================================================

check "rm: unrecognized flag" 22 user rm --fakeflag
check_cluster "rm: no uid" 22 -- user rm
check_cluster "rename: no uid" 22 -- user rename --new-uid newname
check "rename: --new-uid missing value" 1 user rename --uid u --new-uid

# ============================================================
echo ""
echo "=== user stats ==="
# ============================================================

check "stats: unrecognized flag" 22 user stats --fakeflag
check_cluster "stats: no uid" 22 -- user stats
check_cluster "stats: --sync-stats and --reset-stats" 22 -- \
  user stats --uid u --sync-stats --reset-stats
check_cluster "stats: --reset-stats with bucket" 22 -- \
  user stats --uid u --reset-stats --bucket b

check_cluster "stats: nonexistent user" 2 -- user stats --uid "no-such-user-$RANDOM"

# ============================================================
echo ""
echo "=== user list ==="
# ============================================================

check "list: unrecognized flag" 22 user list --fakeflag
check "list: --max-entries missing value" 1 user list --max-entries
check "list: --max-entries invalid int" 22 user list --max-entries banana
check "list: --max-entries out of int range" 22 user list --max-entries 5000000000

check_cluster "list: default" 0 -- user list
check_cluster "list: --max-entries 5" 0 -- user list --max-entries 5

# ============================================================
echo ""
echo "=== user policy ==="
# ============================================================

check_cluster "policy attach: no uid" 22 -- user policy attach --policy-arn arn:x
check_cluster "policy attach: empty arn" 22 -- user policy attach --uid u
check_cluster "policy detach: no uid" 22 -- user policy detach --policy-arn arn:x
check_cluster "policy list: no uid" 22 -- user policy list attached

# ============================================================
echo ""
echo "=== subuser / key / caps ==="
# ============================================================

check_cluster "subuser create: no uid" 22 -- subuser create --subuser s:u
check_cluster "key create: no uid" 22 -- key create
check_cluster "caps add: no uid" 22 -- caps add --caps "buckets=*"

# ============================================================
echo ""
echo "=== flags: underscore vs dash spelling ==="
# ============================================================

check "list: --max-entries space form (dash)" 22 user list --max-entries banana
check "list: --max_entries space form (underscore)" 22 user list --max_entries banana

check_cluster "list: underscore spelling on success path" 0 -- \
  user list --max_entries 100

# ============================================================
echo ""
echo "=== integration: user lifecycle ==="
# ============================================================

if cluster_running; then
  _uid="exit-code-user-$$"

  check_cluster "integration: create" 0 -- \
    user create --uid "$_uid" --display-name "$_uid"
  check_cluster "integration: info" 0 -- user info --uid "$_uid"
  check_cluster "integration: modify" 0 -- \
    user modify --uid "$_uid" --email "${_uid}@example.com"
  check_cluster "integration: stats" 0 -- user stats --uid "$_uid"
  check_cluster "integration: list" 0 -- user list
  check_cluster "integration: rm" 0 -- user rm --uid "$_uid"
  check_cluster "integration: info after rm" 22 -- user info --uid "$_uid"
else
  echo "SKIP [integration: user lifecycle]: no cluster running"
  SKIP=$((SKIP+1))
fi

report_results
