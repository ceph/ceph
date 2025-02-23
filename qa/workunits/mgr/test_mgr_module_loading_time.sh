#!/bin/bash

# Set up orchestrator...
# This will create CEPHADM_STRAY_HOST warnings, but we just want to be able to run an orch command.
ceph mgr module enable cephadm
ceph orch set backend cephadm

# Check cluster status
ceph -s

# Run balancer for a bit to add upmap entires to the osdmap
# (helps detect balancer scalability issues that affect module
# loading time, i.e. https://tracker.ceph.com/issues/68657)
ceph osd set-require-min-compat-client reef
ceph balancer mode upmap-read
for i in {1..5}; do
    ceph balancer on
    sleep 1
done
ceph osd dump

# Delay loading on the balancer module
ceph config set mgr mgr_module_load_delay_name balancer

# Test case 1: Delay exceeds max load time
echo "Testing with module load delay of 6 seconds..."
ceph config set mgr mgr_module_load_delay 6

output=$(ceph mgr fail; ceph orch status 2>&1)
echo "$output"
if [[ "$output" == *"Error ETIMEDOUT: Warning: due to ceph-mgr restart, some PG states may not be up to date"* ]]; then
    echo "PASS: Expected command timeout occurred."
else
    echo "FAIL: Command did not fail as expected."
    exit 1
fi

# Test case 2: Delay within acceptable load time
echo "Testing with module load delay of 4 seconds..."
ceph config set mgr mgr_module_load_delay 4

for i in $(seq 1 100); do
    output=$(ceph mgr fail && ceph orch status 2>&1)
    echo "$output"
    if [[ "$output" == *"Error ETIMEDOUT: Warning: due to ceph-mgr restart, some PG states may not be up to date"* ]]; then
        echo "FAIL: Command failed with acceptable delay in iteration $i."
        exit 1
    fi
    sleep 1
done
echo "PASS: All commands succeeded with acceptable delay."

# Test case 3: No injected delay; testing real module loading time
echo "Testing with module load delay of 0 seconds..."
ceph config set mgr mgr_module_load_delay 0

for i in $(seq 1 100); do
    output=$(ceph mgr fail && ceph orch status 2>&1)
    echo "$output"
    if [[ "$output" == *"Error ETIMEDOUT: Warning: due to ceph-mgr restart, some PG states may not be up to date"* ]]; then
        echo "FAIL: Command unexpectedly timed out in iteration $i."
	exit 1
    fi
    sleep 1
done

echo "PASS: All tests completed successfully."
