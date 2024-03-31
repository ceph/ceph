#!/bin/bash -ex

# Set up ident details for cluster
ceph config set mgr mgr/telemetry/channel_ident true
ceph config set mgr mgr/telemetry/organization 'ceph-qa'
ceph config set mgr mgr/telemetry/description 'upgrade test cluster'

# Opt-in
ceph telemetry on --license sharing-1-0

# Check last_opt_revision
LAST_OPT_REVISION=$(ceph config get mgr mgr/telemetry/last_opt_revision)
if [ $LAST_OPT_REVISION -ne 3 ]; then
    echo "last_opt_revision is incorrect."
    exit 1
fi

# Check reports
ceph telemetry show
ceph telemetry show-device
ceph telemetry show-all

echo OK
