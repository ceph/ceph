#!/bin/bash

# This script tests how the mgr handles different module loading times post failover.
# The motivation is this tracker ticket: https://tracker.ceph.com/issues/71631

# To run this script on a vstart cluter, use the following command:
#   cd ceph/build
#   ../qa/workunits/mgr/test_mgr_module_loading_time.sh --vstart
#
#   (assumes there is no existing vstart cluster)

vstart=0
if [ "$1" = "--vstart" ]; then
    vstart=1
fi

ceph="ceph"
if [ $vstart -eq 1 ]; then
    ceph="./bin/ceph"
    MGR=2 ../src/vstart.sh --debug --new -x --localhost --bluestore --without-dashboard
    echo "Wait for cluster to settle..."
    sleep 20

    "$ceph" mgr module enable nvmeof # simply to match the active modules in the corresponding teuthology cluster
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

echo "Check mgr_status to ensure 'pending_modules' is empty..."
expected='[]'
mgr_status=$("$ceph" tell mgr mgr_status | jq -c '.pending_modules')
if [[ "$mgr_status" == "$expected" ]]; then
    echo "PASS: No modules are pending."
else
    echo "FAIL: Some modules are pending when they shouldn't be."
    echo "Expected: $expected"
    echo "Actual:   $mgr_status"
    exit 1
fi

# ------ Test 2 ------
echo "Select balancer module to receive loading delays..."
"$ceph" config set mgr mgr_module_load_delay_name balancer

echo "Test 2: Inject small delay (10000 ms) that should not exceed max loading expiration"
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

echo "Check mgr_status to ensure 'pending_modules' is empty..."
expected='[]'
mgr_status=$("$ceph" tell mgr mgr_status | jq -c '.pending_modules')
if [[ "$mgr_status" == "$expected" ]]; then
    echo "PASS: No modules are pending."
else
    echo "FAIL: Some modules are pending when there shouldn't be."
    echo "Expected: $expected"
    echo "Actual:   $mgr_status"
    exit 1
fi

# ------ Test 3 ------
echo "Test 3: Inject large delay (10000000000 ms) that exceeds max loading expiration and emits cluster error"
"$ceph" config set mgr mgr_module_load_delay 10000000000

"$ceph" mgr fail
orch_status_output=$("$ceph" orch status 2>&1)

echo "$orch_status_output"
if [[ "$orch_status_output" == *"Error ETIMEDOUT: Module 'orchestrator' did not initialize in time"* ]]; then
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

echo "Check mgr_status to ensure 'pending_modules' is populated with modules we expect..."
expected='["balancer","cephadm","crash","devicehealth","iostat","nfs","nvmeof","orchestrator","pg_autoscaler","progress","rbd_support","status","telemetry","volumes"]'
mgr_status=$("$ceph" tell mgr mgr_status | jq -c '.pending_modules')
if [[ "$mgr_status" == "$expected" ]]; then
    echo "PASS: Expected modules are pending."
else
    echo "FAIL: Expected output does not match actual."
    echo "Expected: $expected"
    echo "Actual:   $mgr_status"
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

echo "Check mgr_status to ensure 'pending_modules' is empty..."
expected='[]'
mgr_status=$("$ceph" tell mgr mgr_status | jq -c '.pending_modules')
if [[ "$mgr_status" == "$expected" ]]; then
    echo "PASS: No modules are pending."
else
    echo "FAIL: Some modules are pending when there shouldn't be."
    echo "Expected: $expected"
    echo "Actual:   $mgr_status"
    exit 1
fi

echo "Re-enabling the balancer module..."
"$ceph" mgr module enable balancer

# Give the health error a bit of time to clear
sleep 10

# ----- Test 5 -----
# Regression test for https://tracker.ceph.com/issues/81666
#
# If mgr_module_load_expiration elapses before Mgr::init() has reached
# PyModuleRegistry::active_start(), MgrStandby::send_beacon() marks the mgr as
# available and calls PyModuleRegistry::get_services() while active_modules is
# still null. That trips ceph_assert(active_modules) and aborts the mgr.
#
# mgr_init_delay is a dev-only option that exists ONLY to reproduce this: it
# makes Mgr::init() sleep before starting the python modules. We pair it with a
# shorter mgr_module_load_expiration so the expiration elapses inside that window.
echo "Test 5: Let the load expiration (20000 ms default value) elapse before the mgr starts its modules (init delay 25000 ms)"

echo "Clearing module load delays so only the init delay and expiration are under test..."
"$ceph" config set mgr mgr_module_load_delay_name ""
"$ceph" config set mgr mgr_module_load_delay 0

"$ceph" config get mgr mgr_module_load_expiration # for debug purposes, should be default value
"$ceph" config set mgr mgr_init_delay 25000

"$ceph" mgr fail

# mgr_status is registered at the very end of Mgr::init(), so it only answers
# once the active mgr is alive and has finished initializing. Poll until it
# answers. If the active mgr crashed, the mon only drops it from `ceph -s`
# after mgr_beacon_grace passes, at which point `ceph -s` reports
# "no daemons active"; treat that as a failure instead of waiting forever.
echo "Waiting for the active mgr to finish initializing (up to 120 seconds)..."
alive=0
no_active_mgr=0
deadline=$((SECONDS + 120))
while [ $SECONDS -lt $deadline ]; do
    if timeout 3 "$ceph" tell mgr mgr_status >/dev/null 2>&1; then
        alive=1
        break
    fi
    if timeout 10 "$ceph" -s 2>&1 | grep -q "no daemons active"; then
        no_active_mgr=1
        break
    fi
    sleep 1
done

echo "Restoring default config..."
"$ceph" config set mgr mgr_init_delay 0

if [ $alive -eq 0 ]; then
    if [ $no_active_mgr -eq 1 ]; then
        echo "FAIL: Test 5: no active mgr in 'ceph -s'; the mgr(s) likely crashed (see tracker 81666)."
    else
        echo "FAIL: Test 5: the active mgr never finished initializing (see tracker 81666)."
    fi
    "$ceph" -s
    exit 1
fi
echo "PASS: Mgr survived its load expiration elapsing before its modules were started."

"$ceph" -s

echo "All tests passed."
