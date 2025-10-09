#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7146" # git grep '\<7146\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    CEPH_ARGS+="--debug_disable_randomized_ping=true "
    CEPH_ARGS+="--debug_heartbeat_testing_span=5 "
    CEPH_ARGS+="--osd_heartbeat_interval=1 "
    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function TEST_network_ping_test1() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1

    sleep 5

    create_pool foo 16

    # write some objects
    timeout 20 rados bench -p foo 10 write -b 4096 --no-cleanup || return 1

    # Get 1 cycle worth of ping data "1 minute"
    sleep 10
    flush_pg_stats

    CEPH_ARGS='' ceph daemon $(get_asok_path osd.0) dump_osd_network | tee $dir/json
    test "$(cat $dir/json | jq '.entries | length')" = "0" || return 1
    test "$(cat $dir/json | jq '.threshold')" = "1000" || return 1

    CEPH_ARGS='' ceph daemon $(get_asok_path mgr.x) dump_osd_network | tee $dir/json
    test "$(cat $dir/json | jq '.entries | length')" = "0" || return 1
    test "$(cat $dir/json | jq '.threshold')" = "1000" || return 1

    CEPH_ARGS='' ceph daemon $(get_asok_path osd.0) dump_osd_network 0 | tee $dir/json
    test "$(cat $dir/json | jq '.entries | length')" = "4" || return 1
    test "$(cat $dir/json | jq '.threshold')" = "0" || return 1

    CEPH_ARGS='' ceph daemon $(get_asok_path mgr.x) dump_osd_network 0 | tee $dir/json
    test "$(cat $dir/json | jq '.entries | length')" = "12" || return 1
    test "$(cat $dir/json | jq '.threshold')" = "0" || return 1

    # Wait another 4 cycles to get "5 minute interval"
    sleep 20
    flush_pg_stats
    CEPH_ARGS='' ceph daemon $(get_asok_path osd.0) dump_osd_network | tee $dir/json
    test "$(cat $dir/json | jq '.entries | length')" = "0" || return 1
    test "$(cat $dir/json | jq '.threshold')" = "1000" || return 1

    CEPH_ARGS='' ceph daemon $(get_asok_path mgr.x) dump_osd_network | tee $dir/json
    test "$(cat $dir/json | jq '.entries | length')" = "0" || return 1
    test "$(cat $dir/json | jq '.threshold')" = "1000" || return 1

    CEPH_ARGS='' ceph daemon $(get_asok_path osd.0) dump_osd_network 0 | tee $dir/json
    test "$(cat $dir/json | jq '.entries | length')" = "4" || return 1
    test "$(cat $dir/json | jq '.threshold')" = "0" || return 1

    CEPH_ARGS='' ceph daemon $(get_asok_path mgr.x) dump_osd_network 0 | tee $dir/json
    test "$(cat $dir/json | jq '.entries | length')" = "12" || return 1
    test "$(cat $dir/json | jq '.threshold')" = "0" || return 1


    # Wait another 10 cycles to get "15 minute interval"
    sleep 50
    flush_pg_stats
    CEPH_ARGS='' ceph daemon $(get_asok_path osd.0) dump_osd_network | tee $dir/json
    test "$(cat $dir/json | jq '.entries | length')" = "0" || return 1
    test "$(cat $dir/json | jq '.threshold')" = "1000" || return 1

    CEPH_ARGS='' ceph daemon $(get_asok_path mgr.x) dump_osd_network | tee $dir/json
    test "$(cat $dir/json | jq '.entries | length')" = "0" || return 1
    test "$(cat $dir/json | jq '.threshold')" = "1000" || return 1

    CEPH_ARGS='' ceph daemon $(get_asok_path osd.0) dump_osd_network 0 | tee $dir/json
    test "$(cat $dir/json | jq '.entries | length')" = "4" || return 1
    test "$(cat $dir/json | jq '.threshold')" = "0" || return 1

    CEPH_ARGS='' ceph daemon $(get_asok_path mgr.x) dump_osd_network 0 | tee $dir/json
    test "$(cat $dir/json | jq '.entries | length')" = "12" || return 1
    test "$(cat $dir/json | jq '.threshold')" = "0" || return 1

    # Just check the threshold output matches the input
    CEPH_ARGS='' ceph daemon $(get_asok_path mgr.x) dump_osd_network 99 | tee $dir/json
    test "$(cat $dir/json | jq '.threshold')" = "99" || return 1
    CEPH_ARGS='' ceph daemon $(get_asok_path osd.0) dump_osd_network 98 | tee $dir/json
    test "$(cat $dir/json | jq '.threshold')" = "98" || return 1

    rm -f $dir/json
}

# Test setting of mon_warn_on_slow_ping_time very low to
# get health warning
function TEST_network_ping_test2() {
    local dir=$1

    export CEPH_ARGS
    export EXTRA_OPTS=" --mon_warn_on_slow_ping_time=0.001"
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1

    sleep 5
    ceph osd crush add-bucket dc1 datacenter
    ceph osd crush add-bucket dc2 datacenter
    ceph osd crush add-bucket dc3 datacenter
    ceph osd crush add-bucket rack1 rack
    ceph osd crush add-bucket rack2 rack
    ceph osd crush add-bucket rack3 rack
    ceph osd crush add-bucket host1 host
    ceph osd crush add-bucket host2 host
    ceph osd crush add-bucket host3 host
    ceph osd crush move dc1 root=default
    ceph osd crush move dc2 root=default
    ceph osd crush move dc3 root=default
    ceph osd crush move rack1 datacenter=dc1
    ceph osd crush move rack2 datacenter=dc2
    ceph osd crush move rack3 datacenter=dc3
    ceph osd crush move host1 rack=rack1
    ceph osd crush move host2 rack=rack2
    ceph osd crush move host3 rack=rack3
    ceph osd crush set osd.0 1.0 host=host1
    ceph osd crush set osd.1 1.0 host=host2
    ceph osd crush set osd.2 1.0 host=host3
    ceph osd crush rule create-simple myrule default host firstn

    create_pool foo 16 16 replicated myrule

    # write some objects
    timeout 20 rados bench -p foo 10 write -b 4096 --no-cleanup || return 1

    # Get at least 1 cycle of ping data (this test runs with 5 second cycles of 1 second pings)
    sleep 10
    flush_pg_stats

    ceph health | tee $dir/health
    grep -q "Slow OSD heartbeats" $dir/health || return 1

    ceph health detail | tee $dir/health
    grep -q "OSD_SLOW_PING_TIME_BACK" $dir/health || return 1
    grep -q "OSD_SLOW_PING_TIME_FRONT" $dir/health || return 1
    grep -q "Slow OSD heartbeats on front from osd[.][0-2] [[]dc[1-3],rack[1-3][]] \
to osd[.][0-2] [[]dc[1-3],rack[1-3][]]" $dir/health || return 1
    rm -f $dir/health
}

main network-ping "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && ../qa/run-standalone.sh network-ping.sh"
# End:
