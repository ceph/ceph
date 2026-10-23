#!/usr/bin/env bash
#
# A replica of a pool being migrated must not serve an object's old data once
# a write to the object in the target pool has been acknowledged.

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7158" # git grep '\<7158\>' : there must be only one
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

function TEST_replica_read_after_migrated_write() {
    local dir=$1
    local pool=mig
    local nobj=8
    local osd_args="--osd_crush_update_on_start=false"

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for id in 0 1 2 3 ; do
        run_osd $dir $id $osd_args || return 1
    done
    ceph config set global enable_experimental_unrecoverable_data_corrupting_features poolmigration || return 1
    ceph osd set-require-min-compat-client umbrella || return 1
    ceph osd pool set noautoscale || return 1

    # One host per OSD, so that a localized read picks the replica. The source
    # is on osd.0 and osd.1, the target on osd.2 and osd.3, so a write to the
    # target never waits for the source's replica.
    for id in 0 1 2 3 ; do
        ceph osd crush add-bucket h$id host || return 1
        ceph osd crush move h$id root=default || return 1
        ceph osd crush set osd.$id 1.0 root=default host=h$id || return 1
        ceph osd crush rm-device-class osd.$id || return 1
    done
    ceph osd crush set-device-class src osd.0 osd.1 || return 1
    ceph osd crush set-device-class tgt osd.2 osd.3 || return 1
    ceph osd crush rule create-replicated src-rep default host src || return 1
    ceph osd crush rule create-replicated tgt-rep default host tgt || return 1
    ceph osd pool create $pool 1 1 replicated --rule src-rep --size 2 || return 1
    wait_for_clean || return 1

    echo v1 > $dir/v1
    echo v2 > $dir/v2
    for i in $(seq 0 $((nobj - 1))) ; do
        rados -p $pool put obj$i $dir/v1 || return 1
    done
    local primary=$(get_primary $pool obj0)
    local replica=$(get_not_primary $pool obj0)

    # Every message from another OSD, the migration's deletes among them,
    # reaches the replica up to 8s late. A long read lease stops the late lease
    # renewals from bouncing its reads.
    kill_daemons $dir TERM osd.$replica || return 1
    activate_osd $dir $replica $osd_args --ms_inject_delay_type=osd \
        --ms_inject_delay_probability=1 --ms_inject_delay_max=8 || return 1
    wait_for_clean || return 1
    test "$(get_primary $pool obj0)" = $primary || return 1
    ceph tell 'osd.*' config set osd_pool_default_read_lease_ratio 4 || return 1
    # a new map makes the PG take the new lease
    ceph osd pool application enable $pool rados || return 1

    ceph osd pool migrate $pool --pool_type replicated --size 2 --rule tgt-rep || return 1

    # As soon as the primary deletes a migrated object, write it, which the
    # primary redirects to the target, and read it back from the replica.
    local checked="" stale=0 obj
    for t in $(seq 1 1200) ; do
        for obj in $(grep -ao 'consider_updating_migration_watermark deleting object [^ ]*:::obj[0-9]*:head' \
                         $dir/osd.$primary.log | sed -e 's/.*::://' -e 's/:head//') ; do
            case " $checked " in
                *" $obj "*) continue ;;
            esac
            checked+=" $obj"
            rados -p $pool put $obj $dir/v2 || return 1
            rados --rados_replica_read_policy=localize --crush_location=host=h$replica \
                -p $pool get $obj $dir/got || return 1
            if ! cmp -s $dir/v2 $dir/got ; then
                echo "$obj: the replica returned $(cat $dir/got) after v2 was written"
                stale=$((stale + 1))
            fi
        done
        test $(echo $checked | wc -w) = $nobj && break
        sleep 0.2
    done
    echo "checked:$checked"
    test $(echo $checked | wc -w) = $nobj || return 1
    test $stale = 0 || return 1
}

main osd-pool-migration-replica-read "$@"
