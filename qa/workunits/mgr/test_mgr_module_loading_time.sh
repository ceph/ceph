#!/bin/bash

# This script tests how the mgr handles different module loading times post failover.
# The motivation is this tracker ticket: https://tracker.ceph.com/issues/71631

# To run this script on a vstart cluter, use the following command:
#   cd ceph/build
#   ../qa/workunits/mgr/test_mgr_module_loading_time.sh --vstart

vstart=0
if [ "$1" = "--vstart" ]; then
    vstart=1
fi

ceph="ceph"
if [ $vstart -eq 1 ]; then
    ceph="./bin/ceph"
fi


# This will create CEPHADM_STRAY_HOST warnings, but we just want to be able to run an orch command.
echo "Enabling cephadm module..."
"$ceph" mgr module enable cephadm
"$ceph" orch set backend cephadm

echo "Checking cluster status..."
"$ceph" -s

# ------ Test 1 ------
echo "Test 1: Test normal module loading behavior without any injected delays"

echo "Ensure that no module is set for a load delay..."
"$ceph" config set mgr mgr_module_load_delay_name ""

echo "Test 1: Ensure that there is no injected load delay..."
"$ceph" config set mgr mgr_module_load_delay 0

"$ceph" mgr fail
orch_status_output=$("$ceph" orch status 2>&1)

echo "$orch_status_output"
if [[ "$orch_status_output" == *"Backend: cephadm"* ]]; then
    echo "PASS: orch command succeeded during normal behavior."
elif [[ "$orch_status_output" == *"Error ENOTSUP: Module 'orchestrator' is not enabled/loaded"* ]]; then
    echo "FAIL: orch command failed during normal behavior."
    exit 1
else
    echo "FAIL: Unexpected error in orch command during normal behavior."
    echo "$orch_status_output"
    exit 1
fi

echo "Ensure health detail DOES NOT warn about any modules that failed initialization..."
health=$("$ceph" health detail 2>&1)
if [[ "$health" == *"Module failed to initialize"* ]]; then
    echo "FAIL: One or more modules failed to initialize during small delay."
    echo "$health"
    exit 1
fi

echo "Verify that mgr is active..."
stat=$("$ceph" -s 2>&1)
if [[ "$stat" != *"active, since"* ]]; then
    echo "FAIL: Mgr should be in 'active' state."
    echo "$stat"
    exit 1
fi

# ------ Test 2 ------
echo "Select balancer module to receive loading delays..."
"$ceph" config set mgr mgr_module_load_delay_name balancer

echo "Test 2: Inject small delay (10000 ms) that should not exceed max loading retries"
"$ceph" config set mgr mgr_module_load_delay 10000

"$ceph" mgr fail
orch_status_output=$("$ceph" orch status 2>&1)

echo "$orch_status_output"
if [[ "$orch_status_output" == *"Backend: cephadm"* ]]; then
    echo "PASS: orch command succeeded during small delay."
elif [[ "$orch_status_output" == *"Error ENOTSUP: Module 'orchestrator' is not enabled/loaded"* ]]; then
    echo "FAIL: orch command failed during small delay."
    exit 1
else
    echo "FAIL: Unexpected error in orch command during small delay."
    echo "$orch_status_output"
    exit 1
fi

echo "Ensure health detail DOES NOT warn about any modules that failed initialization..."
health=$("$ceph" health detail 2>&1)
if [[ "$health" == *"Module failed to initialize"* ]]; then
    echo "FAIL: One or more modules failed to initialize during small delay."
    echo "$health"
    exit 1
fi

echo "Verify that mgr is active..."
stat=$("$ceph" -s 2>&1)
if [[ "$stat" != *"active, since"* ]]; then
    echo "FAIL: Mgr should be in 'active' state."
    echo "$stat"
    exit 1
fi

# ------ Test 3 ------
echo "Test 3: Inject large delay (10000000000 ms) that exceeds max loading retries and emits cluster error"
"$ceph" config set mgr mgr_module_load_delay 10000000000

"$ceph" mgr fail
orch_status_output=$("$ceph" orch status 2>&1)

echo "$orch_status_output"
if [[ "$orch_status_output" == *"Error ENOTSUP: Module 'orchestrator' is not enabled/loaded"* ]]; then
    echo "PASS: orch command failed during large delay as expected."
else
    echo "FAIL: Unexpected error in orch command during large delay."
    echo "$orch_status_output"
    exit 1
fi

echo "Ensure health detail DOES warn about any modules that failed initialization..."
health=$("$ceph" health detail 2>&1)
if [[ "$health" == *"Module failed to initialize"* ]]; then
    echo "PASS: Cluster properly issued error about modules that failed to initialize."
    echo "$health"
else
    echo "FAIL: Cluster did not properly issue error about modules that failed to initialize."
    echo "$health"
    exit 1
fi

echo "Verify that mgr is active..."
stat=$("$ceph" -s 2>&1)
if [[ "$stat" != *"active, since"* ]]; then
    echo "FAIL: Mgr should be in 'active' state."
    echo "$stat"
    exit 1
fi

# ----- Test 4 -----
echo "Test 4: Disable the problematic module and confirm that the health error goes away"

echo "Disabling the balancer module..."
"$ceph" mgr module force disable balancer --yes-i-really-mean-it

echo "Sleeping for 10 seconds to allow the health error to clear up..."
sleep 10

echo "Ensure health detail no longer warns about any modules that failed initialization..."
health=$("$ceph" health detail 2>&1)
if [[ "$health" == *"Module failed to initialize"* ]]; then
    echo "FAIL: One or more modules failed to initialize despite problem module being disabled."
    echo "$health"
    exit 1
fi

echo "Verify that mgr is active..."
stat=$("$ceph" -s 2>&1)
if [[ "$stat" != *"active, since"* ]]; then
    echo "FAIL: Mgr should be in 'active' state."
    echo "$stat"
    exit 1
fi

echo "All tests passed."
