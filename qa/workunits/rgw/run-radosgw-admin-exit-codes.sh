#!/usr/bin/env bash
#
# Run the radosgw-admin command-line suites from src/test/rgw/radosgw-admin
# against the cluster teuthology started. See the README there.
#
# The suites default to a vstart layout (./bin/radosgw-admin, ./ceph.conf,
# radosgw on localhost:8000). Point them at the installed tool, the
# teuthology cluster and the radosgw the rgw task started instead.

set -ex

mydir=$(dirname "$0")
suitedir=${CEPH_ROOT:-$mydir/../../..}/src/test/rgw/radosgw-admin

export RGW_ADMIN=${RGW_ADMIN:-radosgw-admin}
export CEPH_CONF=${CEPH_CONF:-/etc/ceph/ceph.conf}

# the rgw task stores the radosgw endpoint in ${TESTDIR}/url_file
if [ -z "$RGW_ENDPOINT" ] && [ -f "${TESTDIR}/url_file" ]; then
  RGW_ENDPOINT=$(cat "${TESTDIR}/url_file")
  export RGW_ENDPOINT
fi

# the bucket suite creates its test bucket with the aws CLI, which the
# install task puts on the node (see qa/suites/rgw/tools/tasks.yaml)
aws --version

# run every suite even when an earlier one fails, so the log has all of them
failed=0
for suite in test-bucket-exit-codes.sh test-script-exit-codes.sh test-globals.sh; do
  bash "$suitedir/$suite" || failed=1
done

[ "$failed" -eq 0 ] && echo OK.
exit $failed
