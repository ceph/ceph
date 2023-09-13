#!/bin/bash -ex

# Assert that we're still opted in
LAST_OPT_REVISION=$(ceph config get mgr mgr/telemetry/last_opt_revision)
if [ $LAST_OPT_REVISION -ne 3 ]; then
    echo "last_opt_revision is incorrect"
    exit 1
fi

# Check the warning:
STATUS=$(ceph -s)
if ! [[ $STATUS == *"Telemetry requires re-opt-in"* ]]
then
    echo "STATUS does not contain re-opt-in warning"
    exit 1
fi

# Check new collections
COLLECTIONS=$(ceph telemetry collection ls)
NEW_COLLECTIONS=("perf_perf" "basic_mds_metadata" "basic_pool_usage" "basic_rook_v01" "perf_memory_metrics")
for col in ${NEW_COLLECTIONS[@]}; do
    if ! [[ $COLLECTIONS == *$col* ]];
    then
        echo "COLLECTIONS does not contain" "'"$col"'."
	exit 1
    fi
done

# Run preview commands
ceph telemetry preview
ceph telemetry preview-device
ceph telemetry preview-all

# Opt in to new collections
ceph telemetry on --license sharing-1-0
ceph telemetry enable channel perf

# Check the warning:
timeout=60
STATUS=$(ceph -s)
until [[ $STATUS != *"Telemetry requires re-opt-in"* ]] || [ $timeout -le 0 ]; do
    STATUS=$(ceph -s)
    sleep 1
    timeout=$(( timeout - 1 ))
done
if [ $timeout -le 0 ]; then
    echo "STATUS should not contain re-opt-in warning at this point"
    exit 1
fi

# Run show commands
ceph telemetry show
ceph telemetry show-device
ceph telemetry show

# Opt out
ceph telemetry off

echo OK
