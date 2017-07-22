#!/bin/bash

source $(dirname $0)/../detect-build-env-vars.sh
source $CEPH_ROOT/qa/workunits/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7146" # git grep '\<7146\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    # avoid running out of fds in rados bench
    CEPH_ARGS+="--filestore_wbthrottle_xfs_ios_hard_limit=900 "
    CEPH_ARGS+="--filestore_wbthrottle_btrfs_ios_hard_limit=900 "
    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function TEST_filestore_to_bluestore() {
    local dir=$1

    local flimit=$(ulimit -n)
    if [ $flimit -lt 1536 ]; then
        echo "Low open file limit ($flimit), test may fail. Increase to 1536 or higher and retry if that happens."
    fi

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    osd_pid=$(cat $dir/osd.0.pid)
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1

    sleep 5

    ceph osd pool create foo 16

    # write some objects
    rados bench -p foo 10 write -b 4096 --no-cleanup || return 1

    # kill
    while kill $osd_pid; do sleep 1 ; done
    ceph osd down 0

    mv $dir/0 $dir/0.old || return 1
    mkdir $dir/0 || return 1
    ofsid=$(cat $dir/0.old/fsid)
    echo "osd fsid $ofsid"
    O=$CEPH_ARGS
    CEPH_ARGS+="--log-file $dir/cot.log --log-max-recent 0 "
    ceph-objectstore-tool --type bluestore --data-path $dir/0 --fsid $ofsid \
			  --op mkfs || return 1
    ceph-objectstore-tool --data-path $dir/0.old --target-data-path $dir/0 \
			  --op dup || return 1
    CEPH_ARGS=$O

    run_osd_bluestore $dir 0 || return 1

    while ! ceph osd stat | grep '3 up' ; do sleep 1 ; done
    ceph osd metadata 0 | grep bluestore || return 1

    ceph osd scrub 0

    # give it some time
    sleep 15
    # and make sure mon is sync'ed
    flush_pg_stats

    wait_for_clean || return 1
}

main osd-dup "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/osd/osd-dup.sh"
# End:
