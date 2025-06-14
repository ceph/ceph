#!/bin/bash

# Set up orchestrator...
# This will create CEPHADM_STRAY_HOST warnings, but we just want to be able to run an orch command.
ceph mgr module enable cephadm
ceph orch set backend cephadm

# Check cluster status
ceph -s

# Delay loading on the balancer module
ceph config set mgr mgr_module_load_delay_name balancer

# Test if an excess loading time is properly handled
echo "Testing with module load delay of 10000 ms..."
ceph config set mgr mgr_module_load_delay 10000

output=$(ceph mgr fail; ceph orch status 2>&1)
echo "$output"
if [[ "$output" == *"Error ENOTSUP: Warning: due to ceph-mgr restart, some PG states may not be up to date"* ]]; then
    echo "FAIL: Excess loading time was not properly supported."
    exit 1
else
    echo "PASS: Excess loading time was properly supported."
fi
