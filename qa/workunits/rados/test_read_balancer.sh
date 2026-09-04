#!/usr/bin/env bash
# vim: expandtab shiftwidth=4 softtabstop=4
set -ex

# This workunit is to be used as a basis for testing read balancer functionality of both the 'online'
# mgr module and the 'offline' CLI commands.

# See https://docs.ceph.com/en/latest/rados/operations/read-balancer/ for more information.

vstart=false
ceph="ceph"

function print_help {
    cat <<EOM >&2

Help:

   test_read_balancer.sh --help

Usage:
   test_read_balancer.sh {-vstart}

The test may be run as-is for teuthology tests, but you may add the --vstart
flag to run the test on a vstart cluster.

EOM
}

function start_vstart_cluster {
    echo "Setting up a test cluster..."
    cd ~/ceph/build
    ninja -j$(nproc) vstart
    OSD=4 ../src/vstart.sh --debug --new -x --localhost --bluestore
}

function shut_down_cluster {
    if [[ "$vstart" == "true" ]]; then
        echo "Shutting down test cluster..."
        ../src/stop.sh
    fi
}

function TEST_read_balancer_bulk_flag {
    # Tests bug from https://tracker.ceph.com/issues/76731
    echo "TEST 1: Read balancer interaction with pg_autoscaler 'bulk' flag"

    # Enable the read balancer
    "$ceph" osd set-require-min-compat-client reef || return 1
    "$ceph" balancer mode upmap-read || return 1

    # Restrict upmap_max_deviation (balancer will apply more mappings this way)
    "$ceph" config set mgr mgr/balancer/upmap_max_deviation 1 || return 1

    # Create a new, empty pool
    "$ceph" osd pool create foo 32 32 || return 1
    "$ceph" osd pool application enable foo test || return 1 
    pool_id=$("$ceph" osd pool ls detail -f json | jq '.[] | select(.pool_name == "foo")'.pool_id)

    # Turn autoscaler on for that pool
    "$ceph" osd pool set foo pg_autoscale_mode on || return 1

    # Give some pool time to scale up and stabilize after creation
    orig_pg_num=$("$ceph" osd pool ls detail -f json | jq '.[] | select(.pool_name == "foo")'.pg_num)
    time_elapsed_s=0
    while (( time_elapsed_s <= 300 )); do
        if (( orig_pg_num == 32)); then
            break
        fi

        echo "Pool foo needs more time to scale up pgs (actual: $orig_pg_num pgs | expected: 32 pgs)..."
        orig_pg_num=$("$ceph" osd pool ls detail -f json | jq '.[] | select(.pool_name == "foo")'.pg_num)

        time_elapsed_s=$((time_elapsed_s + 5))
        sleep 5
        echo "Waited $time_elapsed_s s for pool foo to scale up..."
    done

    test "$orig_pg_num" -eq 32 || { echo "ERROR: Pool foo failed to scale up to desired pg_num after it was created." >&2; return 1; }

    # Adjust dynamic threshold (necessary to allow the test pool to scale up; if threshold is
    # too high, the variance between the current pg_num and recommended pg_num
    # is not large enough to trigger the autoscaler.)
    "$ceph" osd pool set threshold 1.0 || return 1

    # Restrict the balancer to only pool "foo" (ensures it isn't unable to add mappings due to other
    # pools scaling up or down)
    "$ceph" config set mgr mgr/balancer/pool_ids $pool_id || return 1

    # Add the bulk flag
    "$ceph" osd pool set foo bulk true || return 1

    # Wait for pg_num to scale up more with bulk flag
    pg_num_expected=$("$ceph" osd pool autoscale-status -f json | jq '.[] | select(.pool_name == "foo")'.pg_num_final)
    pg_num_actual=$("$ceph" osd pool ls detail -f json | jq '.[] | select(.pool_name == "foo")'.pg_num)
    time_elapsed_s=0
    while (( time_elapsed_s <= 300 )); do
        if (( pg_num_expected <= orig_pg_num )); then
            echo "Autoscaler status doesn't reflect bulked pool value yet..."
            pg_num_expected=$("$ceph" osd pool autoscale-status -f json | jq '.[] | select(.pool_name == "foo")'.pg_num_final)
        else
            if (( pg_num_expected == pg_num_actual )); then
                break
            fi

            next_pg_num_actual=$("$ceph" osd pool ls detail -f json | jq '.[] | select(.pool_name == "foo")'.pg_num)
            if (( next_pg_num_actual > pg_num_actual )); then
                echo "Making progress on scaling up..."
            else
                echo "Not making progress on scaling up..."
            fi
            pg_num_actual=$next_pg_num_actual
            echo "actual: $pg_num_actual pgs | expected: $pg_num_expected pgs"
        fi

        time_elapsed_s=$((time_elapsed_s + 5))
        sleep 5
        echo "Waited $time_elapsed_s s for pool foo to scale up..."

    done

    [[ $pg_num_expected -gt $orig_pg_num ]] || { echo "ERROR: Autoscaler never set higher pg value." >&2; return 1; }
    [[ $pg_num_expected -eq $pg_num_actual ]] || { echo "ERROR: Pool foo failed to scale up in time." >&2; return 1; }


    # Confirm that pool foo has pg_upmap_primary mappings
    time_elapsed_s=0
    primary_mappings=""

    while (( time_elapsed_s <= 300 )); do
        primary_mappings=$("$ceph" osd dump | grep 'pg_upmap_primary' | grep "${pool_id}\." || true)

        # Run the balancer if there are no primary mappings
        if [[ -z "$primary_mappings" ]]; then
            "$ceph" balancer on || return 1
        fi

        # Exit early if we do have mappings
        if [[ -n "$primary_mappings" ]]; then
            break
        fi

        time_elapsed_s=$((time_elapsed_s + 5))
        sleep 5
        echo "Waited $time_elapsed_s s for the balancer to apply pg_upmap_primary mappings..."
    done

    test -n "$primary_mappings" || { echo "ERROR: No pg_upmap_primary mappings were created for pool foo." >&2; return 1; }

    # Turn balancer off to control variables (we don't want it to add mappings back just yet)
    "$ceph" balancer off || return 1

    # Remove the bulk flag
    "$ceph" osd pool set foo bulk false || return 1

    # Wait for pg_num to scale down (allow a generous 2 hrs)
    pg_num_expected=$("$ceph" osd pool autoscale-status -f json | jq '.[] | select(.pool_name == "foo")'.pg_num_final)
    pg_num_actual=$("$ceph" osd pool ls detail -f json | jq '.[] | select(.pool_name == "foo")'.pg_num)
    time_elapsed_s=0
    while (( time_elapsed_s <= 7200 )); do
        if (( pg_num_expected != orig_pg_num )); then
            echo "Autoscaler status doesn't reflect lower (no bulk) pool value yet..."
            pg_num_expected=$("$ceph" osd pool autoscale-status -f json | jq '.[] | select(.pool_name == "foo")'.pg_num_final)
        else
            if (( pg_num_expected == pg_num_actual )); then
                break
            fi

            next_pg_num_actual=$("$ceph" osd pool ls detail -f json | jq '.[] | select(.pool_name == "foo")'.pg_num)
            echo "pg_num went from $pg_num_actual to $next_pg_num_actual ..."
            if (( next_pg_num_actual < pg_num_actual )); then
                echo "Making progress on scaling down..."
            else
                echo "Not making progress on scaling down..."
            fi
            pg_num_actual=$next_pg_num_actual
            echo "actual: $pg_num_actual pgs | expected: $pg_num_expected pgs"
        fi

        time_elapsed_s=$((time_elapsed_s + 5))
        sleep 5
        echo "Waited $time_elapsed_s s for pool foo to scale down..."

    done

    [[ $pg_num_expected -eq $orig_pg_num ]] || { echo "ERROR: Autoscaler never reverted to original pg value." >&2; return 1; }
    [[ $pg_num_expected -eq $pg_num_actual ]] || { echo "ERROR: Pool foo failed to scale down in time." >&2; return 1; }

    # Confirm that pg_upmap_primary mappings are cleared for pool foo
    time_elapsed_s=0
    primary_mappings=""

    while (( time_elapsed_s <= 300 )); do
        primary_mappings=$("$ceph" osd dump | grep 'pg_upmap_primary' | grep "${pool_id}\." || true)

        # Exit early if the mappings are gone
        if [[ -z "$primary_mappings" ]]; then
            break
        fi

        time_elapsed_s=$((time_elapsed_s + 5))
        sleep 5
        echo "Waited $time_elapsed_s s for the pg_upmap_primary mappings to clear..."
    done

    test -z "$primary_mappings" || { echo "ERROR: The pg_upmap_primary mappings for pool foo were not cleared in time." >&2; return 1; }
}

function TEST_read_balancer_cli {
    echo "TEST 2: Verify 'ceph osd rm-pg-upmap-primary-all'"

    # Make sure the read balance is enabled
    "$ceph" osd set-require-min-compat-client reef || return 1
    "$ceph" balancer mode upmap-read || return 1

    # Turn balancer back on for every pool
    "$ceph" balancer on || return 1
    "$ceph" config set mgr mgr/balancer/pool_ids "" || return 1

    # Create more pools
    "$ceph" osd pool create foo_2 32 32 || return 1
    "$ceph" osd pool application enable foo_2 test || return 1

    "$ceph" osd pool create foo_3 32 32 || return 1
    "$ceph" osd pool application enable foo_3 test || return 1

    echo "Let new pools settle..."
    sleep 30

    # Confirm that multiple pools have pg_upmap_primary mappings
    time_elapsed_s=0
    prim_pools=""
    num_prim_pools=0
    while (( time_elapsed_s <= 300 )); do
        prim_pools="$(
            "$ceph" osd dump -f json |
            jq -r '.pg_upmap_primaries[]?.pgid | split(".")[0]' |
            sort -u || true
        )"
        num_prim_pools=$(echo "$prim_pools" | grep -c .) || true

        # Break if at least two pools have pg_upmap_primary mappings
        if [[ $num_prim_pools -ge 2 ]]; then
            echo "2 or more pools have pg_upmap_primary mappings."
            break
        fi

        echo "Not enough pools have mappings. Running the balancer..."
        "$ceph" balancer on || return 1

        time_elapsed_s=$((time_elapsed_s + 5))
        sleep 5
        echo "Waited $time_elapsed_s s for the balancer to apply pg_upmap_primary mappings..."
    done

    test "$num_prim_pools" -ge 2 || { echo "Not enough pools have pg_upmap_primary mappings." >&2; return 1; }

    # Turn balancer off to freeze mapping state
    "$ceph" balancer off || return 1 

    # Choose random pool
    rand_pool_id=$(
        echo "$prim_pools" |
        shuf -n 1
    )
    [[ "$rand_pool_id" -ge 1 ]] || { echo "ERROR: Invalid pool id $rand_pool_id." >&2; return 1; }

    rand_pool_name="$(
        "$ceph" osd lspools -f json |
        jq -r --argjson poolid "$rand_pool_id" '.[] | select(.poolnum == $poolid) | .poolname'
    )"
    [[ -n "$rand_pool_name" ]] || { echo "ERROR: Unable to extract pool name." >&2; return 1; }

    # Remove prims for random pool only
    "$ceph" osd rm-pg-upmap-primary-all "$rand_pool_name" || return 1
    sleep 10 # shouldn't take longer than this to clear mappings

    # Check each pool for existance of prim mappings
    for pool_id in $prim_pools; do
        primary_mappings=$("$ceph" osd dump | grep 'pg_upmap_primary' | grep "${pool_id}\." || true)

        if [[ $pool_id -eq $rand_pool_id ]]; then
            [[ -z "$primary_mappings" ]] || { echo "ERROR: Pool $pool_id (random pool) should not have any pg_upmap_primary mappings: $primary_mappings" >&2; return 1; }
        else
            [[ -n "$primary_mappings" ]] || { echo "ERROR: Pool $pool_id should have pg_upmap_primary_mappings." >&2; return 1; }
        fi    
    done

    echo "Pool $rand_pool_id was properly cleared of mappings, and all other pools were preserved."

    # Now remove all mappings
    "$ceph" osd rm-pg-upmap-primary-all || return 1
    sleep 10

    # Verify all mappings are gone
    primary_mappings=$("$ceph" osd dump | grep 'pg_upmap_primary' || true)
    test -z "$primary_mappings" || { echo "ERROR: Not all pg_upmap_primary mappings were cleared: $primary_mappings" >&2; return 1; }
}


#-------------- END -------------


main() {
    if [ "$1" == "--help" ]; then
        print_help
        exit
    fi

    if [ "$1" == "--vstart" ]; then
        vstart=true
        ceph="./bin/ceph"
    fi

    if $vstart; then
        start_vstart_cluster
    fi

    # ---- RUN TESTS ----
    TEST_read_balancer_bulk_flag
    TEST_read_balancer_cli
    # -------------------

    if $vstart; then
        shut_down_cluster
    fi

    echo "PASS"
}

main "$@"
