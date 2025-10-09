#!/bin/bash -ex

# Set up ident details for cluster
ceph config set mgr mgr/telemetry/channel_ident true
ceph config set mgr mgr/telemetry/organization 'ceph-qa'
ceph config set mgr mgr/telemetry/description 'upgrade test cluster'

#Run preview commands
ceph telemetry preview
ceph telemetry preview-device
ceph telemetry preview-all

# Opt in to new collections right away to avoid "TELEMETRY_CHANGED"
# warning (see https://tracker.ceph.com/issues/64458)
ceph telemetry on --license sharing-1-0
ceph telemetry enable channel perf

# The last_opt_revision remains at 1 since last_opt_revision
# was phased out for fresh installs of quincy.
LAST_OPT_REVISION=$(ceph config get mgr mgr/telemetry/last_opt_revision)
if [ $LAST_OPT_REVISION -ne 1 ]; then
    echo "last_opt_revision is incorrect"
    exit 1
fi

# Check the warning:
ceph -s

# Verify collections
REPORTED_COLLECTIONS=$(ceph telemetry collection ls)
NUM_REPORTED_COLLECTIONS=$(echo "$REPORTED_COLLECTIONS" | awk '/^NAME/ {flag=1; next} flag' | wc -l)
KNOWN_COLLECTIONS=("basic_base" "basic_mds_metadata" "basic_pool_flags" "basic_pool_options_bluestore"
                   "basic_pool_usage" "basic_rook_v01" "basic_usage_by_class" "crash_base"
                   "device_base" "ident_base" "perf_memory_metrics" "perf_perf")

if ! [[ $NUM_REPORTED_COLLECTIONS == "${#KNOWN_COLLECTIONS[@]}" ]];
then
    echo "Number of reported collections ($NUM_REPORTED_COLLECTIONS) does not match KNOWN_COLLECTIONS ("${#KNOWN_COLLECTIONS[@]}")."
    exit 1
fi

for col in ${KNOWN_COLLECTIONS[@]}; do
    if ! [[ $REPORTED_COLLECTIONS == *$col* ]];
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
ceph telemetry show-all

echo OK
