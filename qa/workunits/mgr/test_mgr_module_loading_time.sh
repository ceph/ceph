#!/bin/bash

setup_cephadm() {
    # This will create CEPHADM_STRAY_HOST warnings, but we just want to be able to run an orch command.
    echo "Enabling cephadm module..."
    ceph mgr module enable cephadm
    ceph orch set backend cephadm
}

check_cluster_status() {
    echo "Checking cluster status..."
    ceph -s
}

set_balancer_delay() {
    echo "Setting balancer module load delay..."
    ceph config set mgr mgr_module_load_delay_name balancer
    ceph config set mgr mgr_module_load_delay 10000
}

test_loading_time() {
    echo "Testing with module load delay of 10000 ms..."
    ceph mgr fail

    local orch_status_output
    if ! orch_status_output=$(ceph orch status 2>&1); then
        echo "FAIL: 'ceph orch status' failed to run:"
        echo "$orch_status_output"
        exit 1
    fi

    echo "$orch_status_output"

    if [[ "$orch_status_output" == *"Backend: cephadm"* ]]; then
        echo "PASS: Excess loading time was properly supported."
    elif [[ "$orch_status_output" == *"Error ENOTSUP: Warning: due to ceph-mgr restart, some PG states may not be up to date"* ]]; then
        echo "FAIL: Excess loading time was not properly supported."
        exit 1
    else
        echo "FAIL: Unexpected error in 'ceph orch status':"
        echo "$orch_status_output"
        exit 1
    fi
}

main() {
    setup_cephadm || return 1
    check_cluster_status || return 1
    set_balancer_delay || return 1
    test_loading_time || return 1
}

main "$@"
