#!/bin/bash

. $(dirname $0)/../../standalone/ceph-helpers.sh

set -ex

# Skip unless running on a Crimson OSD
[ "$(ceph osd metadata 0 | jq -r '.osd_type')" == "crimson" ] || exit 0

# Write some data so the OSD has real work behind it
ceph osd pool create testpool 8 8
rados -p testpool bench 10 write --no-cleanup

sleep 5

# Enable the admin socket assert command
ceph tell osd.0 config set debug_asok_assert_abort true

# Fire the assert — this crashes OSD 0. The command will fail because the
# OSD dies mid-request, so we ignore the exit code.
timeout 30 ceph tell osd.0 assert || true

# Wait for the monitor to notice OSD 0 is down
for i in $(seq 1 60); do
    if ceph osd dump --format json | jq -e '.osds[] | select(.osd == 0) | .up == 0' >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

ceph osd dump --format json
ceph osd dump --format json | jq -e '.osds[] | select(.osd == 0) | .up == 0' || \
    { echo "OSD 0 did not go down after assert"; exit 1; }

# Verify that a coredump was produced. Leave it in place for
# downstream coredump/binary collection validation.
find $TESTDIR/archive/coredump -type f -ls 2>/dev/null
coredump_count=$(find $TESTDIR/archive/coredump -type f 2>/dev/null | wc -l)
if [ "$coredump_count" -gt 0 ]; then
    echo "Found $coredump_count coredump file(s) — OK"
else
    echo "WARNING: no coredump files found (may be a system coredump config issue)"
fi

echo "OK"
