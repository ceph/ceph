#!/usr/bin/env bash
#
# The OSD records the purged snaps of all pools in its 'purged_snaps' meta
# object: from the new OSDMaps, and - for the epochs it has not recorded
# (e.g. after 'reset_purged_snaps_last') - from the monitor, while booting.
# Check that no purge is lost on either path.
#
# osd.0 holds no PGs (its crush weight is 0) and only records: all the PGs
# are on osd.1, so that snaps are purged whether osd.0 is up or not.

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

POOL=test

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7149" # git grep '\<7149\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function setup_cluster() {
    local dir=$1

    run_mon $dir a --osd_pool_default_size=1 --mon_allow_pool_size_one=true || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    ceph osd crush reweight osd.0 0 || return 1
    create_pool $POOL 8 8 || return 1
    ceph osd pool application enable $POOL rados || return 1
    wait_for_clean || return 1
}

function pool_id() {
    ceph osd dump -f json | jq ".pools[] | select(.pool_name == \"$POOL\") | .pool"
}

# create a pool snapshot, remove it, and wait for the monitor to mark it
# purged. Outputs the snap id.
function purge_snap() {
    local name=$1

    ceph osd pool mksnap $POOL $name >&2 || return 1
    local snap=$(rados -p $POOL lssnap | awk -v name=$name '$2 == name { print $1 }')
    test -n "$snap" || return 1
    ceph osd pool rmsnap $POOL $name >&2 || return 1
    for ((i=0; i < $TIMEOUT; i++)); do
        if [ "$(ceph osd dump -f json | jq '.removed_snaps_queue | length')" = 0 ]; then
            echo $snap
            return 0
        fi
        sleep 1
    done
    return 1
}

# wait for osd.$id to have the cluster's current OSDMap
function wait_for_osd_epoch() {
    local id=$1

    local epoch=$(ceph osd dump -f json | jq .epoch)
    for ((i=0; i < $TIMEOUT; i++)); do
        if [ "$(ceph tell osd.$id status | jq -r .newest_map)" -ge "$epoch" ]; then
            return 0
        fi
        sleep 1
    done
    return 1
}

function restart_osd() {
    local dir=$1
    local id=$2

    kill_daemons $dir TERM osd.$id >&2 || return 1
    activate_osd $dir $id >&2 || return 1
    wait_for_osd up $id >&2 || return 1
}

# fail unless osd.$id has a purged_snaps row (of the pool) containing each
# of the snaps. Restarts the OSD.
function check_purged() {
    local dir=$1
    local id=$2
    shift 2

    local poolid=$(pool_id)
    kill_daemons $dir TERM osd.$id >&2 || return 1
    # each row's value: the pool (int64), and the interval [begin, end)
    # of purged snaps (two snapid_t), all little-endian
    local rows=$(
        for key in $(_objectstore_tool_nodown $dir $id --pgid meta \
                       purged_snaps list-omap | grep "^PSN__${poolid}_"); do
            _objectstore_tool_nodown $dir $id --pgid meta \
              purged_snaps get-omap $key | od -An -t u8 -j 8 -N 16
        done)
    echo "osd.$id purged_snaps rows of pool $poolid: $rows" >&2
    activate_osd $dir $id >&2 || return 1
    wait_for_osd up $id >&2 || return 1

    local snap
    for snap in "$@"; do
        echo "$rows" | awk -v snap=$snap '
            NF == 2 && $1 <= snap && snap < $2 { found = 1 }
            END { exit !found }' || {
            echo "osd.$id did not record purged snap $snap" >&2
            return 1
        }
    done
}

# the purged snaps are recorded from the OSDMaps
function TEST_purged_snaps_from_osdmaps() {
    local dir=$1

    setup_cluster $dir || return 1
    local snap1=$(purge_snap s1)
    test -n "$snap1" || return 1
    wait_for_osd_epoch 0 || return 1
    check_purged $dir 0 $snap1 || return 1
}

# after fetching the purged snaps from the monitor (when booting), the OSD
# goes on recording them from the OSDMaps
function TEST_purged_snaps_after_sync() {
    local dir=$1

    setup_cluster $dir || return 1
    local snap1=$(purge_snap s1)
    test -n "$snap1" || return 1
    wait_for_osd_epoch 0 || return 1

    ceph tell osd.0 reset_purged_snaps_last || return 1
    restart_osd $dir 0 || return 1
    local snap2=$(purge_snap s2)
    test -n "$snap2" || return 1
    wait_for_osd_epoch 0 || return 1
    check_purged $dir 0 $snap1 $snap2 || return 1
}

# snaps purged while the OSD is down (in epochs newer than its OSDMap) are
# recorded once it is back
function TEST_purged_snaps_while_down() {
    local dir=$1

    setup_cluster $dir || return 1
    ceph tell osd.0 reset_purged_snaps_last || return 1
    kill_daemons $dir TERM osd.0 >&2 || return 1
    ceph osd down 0
    local snap1=$(purge_snap s1)
    test -n "$snap1" || return 1

    activate_osd $dir 0 >&2 || return 1
    wait_for_osd up 0 || return 1
    wait_for_osd_epoch 0 || return 1
    check_purged $dir 0 $snap1 || return 1

    # and the next ones, from the OSDMaps
    local snap2=$(purge_snap s2)
    test -n "$snap2" || return 1
    wait_for_osd_epoch 0 || return 1
    check_purged $dir 0 $snap1 $snap2 || return 1
}

main osd-purged-snaps "$@"

# Local Variables:
# compile-command: "cd ../../../build ; make -j4 && \
#    ../qa/run-standalone.sh osd-purged-snaps.sh"
# End:
