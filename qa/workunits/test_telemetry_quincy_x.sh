#!/bin/bash -ex

# Opt in to new collections right away to avoid "TELEMETRY_CHANGED"
# health warning (see https://tracker.ceph.com/issues/64458).
# Currently, no new collections between latest quincy and reef (dev)

# For quincy, the last_opt_revision remains at 1 since last_opt_revision
# was phased out for fresh installs of quincy.
LAST_OPT_REVISION=$(ceph config get mgr mgr/telemetry/last_opt_revision)
if [ $LAST_OPT_REVISION -ne 1 ]; then
    echo "last_opt_revision is incorrect"
    exit 1
fi

# Check the warning:
ceph -s

COLLECTIONS=$(ceph telemetry collection ls)
NEW_COLLECTIONS=("perf_perf" "basic_mds_metadata" "basic_pool_usage"
                 "basic_rook_v01" "perf_memory_metrics" "basic_pool_options_bluestore")
for col in ${NEW_COLLECTIONS[@]}; do
    if ! [[ $COLLECTIONS == *$col* ]];
    then
        echo "COLLECTIONS does not contain" "'"$col"'."
	exit 1
    fi
done

#Run preview commands
ceph telemetry preview
ceph telemetry preview-device
ceph telemetry preview-all

# Run show commands
ceph telemetry show
ceph telemetry show-device
ceph telemetry show

# Opt out
ceph telemetry off

echo OK
