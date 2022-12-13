#!/bin/bash -ex

# Set up ident details for cluster
ceph config set mgr mgr/telemetry/channel_ident true
ceph config set mgr mgr/telemetry/organization 'ceph-qa'
ceph config set mgr mgr/telemetry/description 'upgrade test cluster'


#Run preview commands
ceph telemetry preview
ceph telemetry preview-device
ceph telemetry preview-all

# Assert that new collections are available
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

# Opt-in
ceph telemetry on --license sharing-1-0

# Enable perf channel
ceph telemetry enable channel perf

# For quincy, the last_opt_revision remains at 1 since last_opt_revision
# was phased out for fresh installs of quincy.
LAST_OPT_REVISION=$(ceph config get mgr mgr/telemetry/last_opt_revision)
if [ $LAST_OPT_REVISION -ne 1 ]; then
    echo "last_opt_revision is incorrect"
    exit 1
fi

# Run show commands
ceph telemetry show
ceph telemetry show-device
ceph telemetry show-all

echo OK
