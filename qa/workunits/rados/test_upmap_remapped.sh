#!/usr/bin/env bash
# vim: expandtab shiftwidth=4 softtabstop=4
set -ex

# Integration test to demonstrate the usage of src/tools/contrib/upmap-remapped.py
#
# Verifies that the script can resolve PGs stuck in "backfill_wait+remapped"
# or "backfill+remapped" caused by adding cluster capacity. The script should
# return the cluster to "active+clean" without moving any data.

vstart=false

function print_help {
    cat <<EOM >&2

Help:

   test_upmap_remapped.sh --help

Usage:
   test_upmap_remapped.sh {-vstart}

The test may be run as-is for teuthology tests, but you may add the --vstart
flag to run the test on a vstart cluster.

EOM
}

function start_vstart_cluster {
    echo "Setting up a test cluster..."
    cd ~/ceph/build

    # Use vstart binaries
    export CEPH_ROOT=$(cd .. && pwd)
    export CEPH_LIB=$(pwd)/lib
    export CEPH_PLUGIN_DIR=$(pwd)/lib
    export PATH=$(pwd)/bin:$PATH
    export LD_LIBRARY_PATH=$(pwd)/lib:$LD_LIBRARY_PATH
    export PYTHONPATH=$(pwd)/lib/cython_modules/lib.3:$CEPH_ROOT/src/pybind:$PYTHONPATH

    ninja -j$(nproc) vstart
    OSD=10 ../src/vstart.sh --debug --new -x --localhost --bluestore --osds-per-host 5
    
    echo "Waiting for cluster to settle..."
    wait_for_clean || return 1
}

function shut_down_cluster {
    if [[ "$vstart" == "true" ]]; then
        echo "Shutting down test cluster..."
        ../src/stop.sh
    fi
}

# Usage: wait_for_clean [timeout_seconds]  (default: 120)
function wait_for_clean {
    local timeout=${1:-120}
    local wait_time=0
    local ceph_status
    local num_pgs_total
    local num_pgs_clean

    while (( wait_time < timeout )); do
        ceph_status=$(ceph -s -f json 2>/dev/null)
        num_pgs_total=$(echo "$ceph_status" | jq '.pgmap.num_pgs')
        num_pgs_clean=$(echo "$ceph_status" | jq '[.pgmap.pgs_by_state[] | select(.state_name == "active+clean") | .count] | add // 0')

        if (( num_pgs_total > 0 && num_pgs_clean == num_pgs_total )); then
            return 0
        fi

        sleep 1
        wait_time=$((wait_time + 1))
    done

    echo "ERROR: Cluster timed out waiting for \"active+clean\" PGs."
    ceph -s
    return 1
}

function wait_for_stuck_backfill {
    local wait_time=0
    local ceph_status
    local num_pgs_backfill_wait
    local num_pgs_backfilling

    while (( wait_time < 400 )); do
        ceph_status=$(ceph -s -f json 2>/dev/null)
        num_pgs_backfill_wait=$(echo "$ceph_status" | jq '[.pgmap.pgs_by_state[] | select(.state_name | test("remapped\\+backfill_wait")) | .count] | add // 0')
        num_pgs_backfilling=$(echo "$ceph_status" | jq '[.pgmap.pgs_by_state[] | select(.state_name | test("remapped\\+backfilling")) | .count] | add // 0')
        num_pgs_degraded=$(echo "$ceph_status" | jq '[.pgmap.pgs_by_state[] | select(.state_name | test("degraded")) | .count] | add // 0')
        num_pgs_recovering=$(echo "$ceph_status" | jq '[.pgmap.pgs_by_state[] | select(.state_name | test("recovering")) | .count] | add // 0')

        if (( num_pgs_backfill_wait > 0 &&
              num_pgs_backfilling > 0 &&
              num_pgs_degraded == 0 &&
              num_pgs_recovering == 0 )); then
            break
        fi

        sleep 1
        wait_time=$((wait_time + 1))
    done

    sleep 60 # give a bit more time to make sure cluster is truly stuck
    ceph_status=$(ceph -s -f json 2>/dev/null)
    num_pgs_backfill_wait=$(echo "$ceph_status" | jq '[.pgmap.pgs_by_state[] | select(.state_name | test("remapped\\+backfill_wait")) | .count] | add // 0')
    num_pgs_backfilling=$(echo "$ceph_status" | jq '[.pgmap.pgs_by_state[] | select(.state_name | test("remapped\\+backfilling")) | .count] | add // 0')
    num_pgs_degraded=$(echo "$ceph_status" | jq '[.pgmap.pgs_by_state[] | select(.state_name | test("degraded")) | .count] | add // 0')
    num_pgs_recovering=$(echo "$ceph_status" | jq '[.pgmap.pgs_by_state[] | select(.state_name | test("recovering")) | .count] | add // 0')

    if (( num_pgs_backfill_wait > 0 &&
          num_pgs_backfilling > 0 &&
          num_pgs_degraded == 0 &&
          num_pgs_recovering == 0 )); then
        echo "PGs are stuck in backfill_wait/backfilling, but none are recovering or degraded."
        return 0
    fi

    echo "ERROR: Cluster timed out waiting for PGs stuck backfilling."
    ceph -s
    return 1
}

function TEST_upmap_remapped_add_capacity {
    echo "Use the upmap-remapped script after adding cluster capacity"

    # Change CRUSH weight of first 5 OSDs to 0 to simulate only one host (workaround for not being able to add another host)
    for id in 0 1 2 3 4; do
        ceph osd crush reweight osd.$id 0 || return 1
    done
    wait_for_clean || return 1

    # Create a test pool and write data
    ceph osd pool create testpool 128 || return 1
    dd if=/dev/urandom of=/tmp/bigfile bs=1M count=50 || return 1
    for i in $(seq 1 500); do
      rados -p testpool put obj$i /tmp/bigfile || return 1
    done

    # Wait for "active+clean"
    wait_for_clean || return 1

    # Prevent CRUSH from reacting to adding capacity
    ceph osd set norebalance

    # "Add" a host
    # Change first 5 OSDs back to a CRUSH weight of 1
    for id in 0 1 2 3 4; do
        ceph osd crush reweight osd.$id 1 || return 1
    done

    # Wait for cluster to be stuck in prolonged backfill state
    wait_for_stuck_backfill || return 1

    # Check that the target_max_misplaced_objects threshold was exceeded
    local misplaced_objects=$(ceph balancer status | jq '(.optimize_result) | test("^Too many objects .* are misplaced; try again later$")')
    if [ ! "$misplaced_objects" == "true" ]; then
        echo "ERROR: Balancer does not report too many misplaced objects."
        return 1
    fi

    # Retrieve script and check dry run
    wget https://raw.githubusercontent.com/ceph/ceph/refs/heads/main/src/tools/contrib/upmap-remapped.py
    chmod u+x upmap-remapped.py
    local dry_run=$(./upmap-remapped.py)
    if ! grep -q '^ceph osd pg-upmap-items ' <<< "$dry_run"; then
        echo "ERROR: upmap-remapped.py did not suggest any upmap changes."
        return 1
    fi
    
    # Execute script (up to 3 times if needed)
    local num_script_runs=0
    local clean=1
    while (( $num_script_runs < 3 )); do
        ./upmap-remapped.py | sh || return 1
        if wait_for_clean; then
            clean=0
            break
        fi
        echo "Cluster is still not clean. We need to run the script again."
        num_script_runs=$((num_script_runs + 1))
    done

    if [[ $clean != 0 ]]; then
        echo "ERROR: Cluster never became 'active+clean' after running upmap-remapped.py"
        return 1
    fi

    # Print status for debugging
    ceph -s

    # Remove no-rebalance flag and do final clean check
    ceph osd unset norebalance || return 1
    wait_for_clean || return 1

    # Cleanup
    rm upmap-remapped.py
    ceph osd pool rm testpool testpool --yes-i-really-really-mean-it

    return 0
}

#-------------- END -------------


main() {
    if [ "$1" == "--help" ]; then
        print_help
        exit
    fi

    if [ "$1" == "--vstart" ]; then
        vstart=true
    fi

    if [ "$vstart" == "true" ]; then
        start_vstart_cluster
    fi

    # ---- RUN TESTS ----
    TEST_upmap_remapped_add_capacity || return 1
    # -------------------

    if [ "$vstart" == "true" ]; then
        shut_down_cluster
    fi

    echo "PASS"
}

main "$@"

