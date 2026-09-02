#!/usr/bin/env bash
#
# Copyright (C) 2015 Intel <contact@intel.com.com>
# Copyright (C) 2014, 2015 Red Hat <contact@redhat.com>
# Copyright (C) 2024 Red Hat <contact@redhat.com> - Crimson adaptation
#
# Crimson adaptation: Based on osd-unexpected-clone.sh
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    # Parse --debug and --noremove flags (may appear anywhere in the argument list).
    # --debug:    append --debug to CRIMSON_EXTRA_OPTS so only run_crimson_osd
    #             picks it up.  Must NOT go into EXTRA_OPTS because run_mon,
    #             run_mgr, etc. pass EXTRA_OPTS to ceph-mon/ceph-mgr which do
    #             not accept a bare --debug flag (they require --debug-<subsystem>).
    # --noremove: set NOREMOVE=1 so teardown() in ceph-helpers.sh skips "rm -fr $dir".
    local new_args=()
    for arg in "$@"; do
        case "$arg" in
            --debug)    CRIMSON_EXTRA_OPTS+=" --debug" ;;
            --noremove) NOREMOVE=1 ;;
            *)          new_args+=("$arg") ;;
        esac
    done
    set -- "${new_args[@]}"

    export CEPH_MON="127.0.0.1:7148" # git grep '\<7148\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    CEPH_ARGS+="--ms-bind-msgr2=true --ms-bind-msgr1=false "
    # Critical: Mark pools as crimson-compatible by default
    CEPH_ARGS+="--osd_pool_default_crimson=true "
    # Disable PG autoscale for crimson (not supported yet)
    CEPH_ARGS+="--osd_pool_default_pg_autoscale_mode=off "

    export -n CEPH_CLI_TEST_DUP_COMMAND
    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}

    local passed=()
    local failed=()
    for func in $funcs ; do
        if ! setup $dir ; then
            echo "SETUP FAILED for $func — skipping"
            failed+=("$func (setup failed)")
            continue
        fi
        if $func $dir ; then
            passed+=("$func")
        else
            echo "FAILED: $func"
            failed+=("$func")
        fi
        teardown $dir || true
    done

    echo ""
    echo "======== TEST RESULTS ========"
    echo "PASSED (${#passed[@]}):"
    for f in "${passed[@]}"; do echo "  [PASS] $f"; done
    echo "FAILED (${#failed[@]}):"
    for f in "${failed[@]}"; do echo "  [FAIL] $f"; done
    echo "=============================="

    if [ ${#failed[@]} -ne 0 ]; then
        return 1
    fi
}

function apply_crimson_config() {
    # Apply critical configurations that vstart.sh sets via config assimilate-conf
    # These are needed for proper Crimson operation and peering
    ceph config set mon mon_osd_reporter_subtree_level osd || return 1
    ceph config set mon mon_data_avail_warn 2 || return 1
    ceph config set mon mon_data_avail_crit 1 || return 1
    ceph config set mon mon_allow_pool_delete true || return 1
    ceph config set mon mon_allow_pool_size_one true || return 1
    ceph config set osd osd_scrub_load_threshold 2000 || return 1
    ceph config set osd osd_debug_op_order true || return 1
    ceph config set osd osd_debug_misdirected_ops true || return 1

    return 0
}

function TEST_recover_unexpected() {
    local dir=$1

    run_mon $dir a || return 1
    apply_crimson_config || return 1
    run_mgr $dir x || return 1
    run_crimson_osd $dir 0 --osd_objectstore=seastore || return 1
    run_crimson_osd $dir 1 --osd_objectstore=seastore || return 1
    run_crimson_osd $dir 2 --osd_objectstore=seastore || return 1

    ceph osd pool create foo 1
    rados -p foo put foo /etc/passwd
    rados -p foo mksnap snap
    rados -p foo put foo /etc/group

    wait_for_clean || return 1

    local osd=$(get_primary foo foo)

    # Stop all OSDs to use crimson-objectstore-tool
    kill_daemons $dir KILL osd || return 1

    # Use crimson-objectstore-tool directly for all operations
    JSON=`crimson-objectstore-tool --data-path $dir/$osd --op list foo | grep snapid.:1`
    echo "JSON is $JSON"
    rm -f $dir/_ $dir/data
    crimson-objectstore-tool --data-path $dir/$osd "$JSON" get-attr _ > $dir/_ || return 1
    crimson-objectstore-tool --data-path $dir/$osd "$JSON" get-bytes $dir/data || return 1

    # Restart OSDs
    run_crimson_osd $dir 0 --osd_objectstore=seastore || return 1
    run_crimson_osd $dir 1 --osd_objectstore=seastore || return 1
    run_crimson_osd $dir 2 --osd_objectstore=seastore || return 1

    wait_for_clean || return 1

    rados -p foo rmsnap snap

    sleep 5

    # Stop OSDs again for modifications
    kill_daemons $dir KILL osd || return 1

    crimson-objectstore-tool --data-path $dir/$osd "$JSON" set-bytes $dir/data || return 1
    crimson-objectstore-tool --data-path $dir/$osd "$JSON" set-attr _ $dir/_ || return 1

    # Restart OSDs
    run_crimson_osd $dir 0 --osd_objectstore=seastore || return 1
    run_crimson_osd $dir 1 --osd_objectstore=seastore || return 1
    run_crimson_osd $dir 2 --osd_objectstore=seastore || return 1

    wait_for_clean || return 1

    sleep 5

    ceph pg repair 1.0 || return 1

    sleep 10

    ceph log last

    # make sure osds are still up
    timeout 60 ceph tell osd.0 version || return 1
    timeout 60 ceph tell osd.1 version || return 1
    timeout 60 ceph tell osd.2 version || return 1
}


main osd-unexpected-clone-crimson "$@"

# Local Variables:
# compile-command: "cd build ; make -j4 && \
#    ../qa/run-standalone.sh osd-unexpected-clone-crimson.sh"
# End: