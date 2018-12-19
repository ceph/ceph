#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

[ `uname` = FreeBSD ] && exit 0

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7146" # git grep '\<7146\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    CEPH_ARGS+="--bluestore_block_size=2147483648 "
    CEPH_ARGS+="--bluestore_block_db_create=true "
    CEPH_ARGS+="--bluestore_block_db_size=1073741824 "
    CEPH_ARGS+="--bluestore_block_wal_size=536870912 "
    CEPH_ARGS+="--bluestore_bluefs_min=536870912 "
    CEPH_ARGS+="--bluestore_bluefs_min_free=536870912 "
    CEPH_ARGS+="--bluestore_block_wal_create=true "
    CEPH_ARGS+="--bluestore_fsck_on_mount=true "
    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function TEST_bluestore() {
    local dir=$1

    local flimit=$(ulimit -n)
    if [ $flimit -lt 1536 ]; then
        echo "Low open file limit ($flimit), test may fail. Increase to 1536 or higher and retry if that happens."
    fi

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd_bluestore $dir 0 || return 1
    osd_pid0=$(cat $dir/osd.0.pid)
    run_osd_bluestore $dir 1 || return 1
    osd_pid1=$(cat $dir/osd.1.pid)
    run_osd_bluestore $dir 2 || return 1
    osd_pid2=$(cat $dir/osd.2.pid)
    run_osd_bluestore $dir 3 || return 1
    osd_pid3=$(cat $dir/osd.3.pid)

    sleep 5

    create_pool foo 16

    # write some objects
    timeout 60 rados bench -p foo 30 write -b 4096 --no-cleanup #|| return 1

    echo "after bench"

    # kill
    while kill $osd_pid0; do sleep 1 ; done
    ceph osd down 0
    while kill $osd_pid1; do sleep 1 ; done
    ceph osd down 1
    while kill $osd_pid2; do sleep 1 ; done
    ceph osd down 2
    while kill $osd_pid3; do sleep 1 ; done
    ceph osd down 3

    # expand slow devices
    ceph-bluestore-tool --path $dir/0 fsck || return 1
    ceph-bluestore-tool --path $dir/1 fsck || return 1
    ceph-bluestore-tool --path $dir/2 fsck || return 1
    ceph-bluestore-tool --path $dir/3 fsck || return 1

    truncate $dir/0/block -s 4294967296 # 4GB
    ceph-bluestore-tool --path $dir/0 bluefs-bdev-expand || return 1
    truncate $dir/1/block -s 4311744512 # 4GB + 16MB
    ceph-bluestore-tool --path $dir/1 bluefs-bdev-expand || return 1
    truncate $dir/2/block -s 4295099392 # 4GB + 129KB
    ceph-bluestore-tool --path $dir/2 bluefs-bdev-expand || return 1
    truncate $dir/3/block -s 4293918720 # 4GB - 1MB
    ceph-bluestore-tool --path $dir/3 bluefs-bdev-expand || return 1

    # slow, DB, WAL -> slow, DB
    ceph-bluestore-tool --path $dir/0 fsck || return 1
    ceph-bluestore-tool --path $dir/1 fsck || return 1
    ceph-bluestore-tool --path $dir/2 fsck || return 1
    ceph-bluestore-tool --path $dir/3 fsck || return 1

    ceph-bluestore-tool --path $dir/0 bluefs-bdev-sizes

    ceph-bluestore-tool --path $dir/0 \
      --devs-source $dir/0/block.wal \
      --dev-target $dir/0/block.db \
      --command bluefs-bdev-migrate || return 1

    ceph-bluestore-tool --path $dir/0 fsck || return 1

    # slow, DB, WAL -> slow, WAL
    ceph-bluestore-tool --path $dir/1 \
      --devs-source $dir/1/block.db \
      --dev-target $dir/1/block \
      --command bluefs-bdev-migrate || return 1

    ceph-bluestore-tool --path $dir/1 fsck || return 1

    # slow, DB, WAL -> slow
    ceph-bluestore-tool --path $dir/2 \
      --devs-source $dir/2/block.wal \
      --devs-source $dir/2/block.db \
      --dev-target $dir/2/block \
      --command bluefs-bdev-migrate || return 1

    ceph-bluestore-tool --path $dir/2 fsck || return 1

    # slow, DB, WAL -> slow, WAL (negative case)
    ceph-bluestore-tool --path $dir/3 \
      --devs-source $dir/3/block.db \
      --dev-target $dir/3/block.wal \
      --command bluefs-bdev-migrate

    # Migration to WAL is unsupported
    if [ $? -eq 0 ]; then
        return 1
    fi
    ceph-bluestore-tool --path $dir/3 fsck || return 1

    # slow, DB, WAL -> slow, DB (WAL to slow then slow to DB)
    ceph-bluestore-tool --path $dir/3 \
      --devs-source $dir/3/block.wal \
      --dev-target $dir/3/block \
      --command bluefs-bdev-migrate || return 1

    ceph-bluestore-tool --path $dir/3 fsck || return 1

    ceph-bluestore-tool --path $dir/3 \
      --devs-source $dir/3/block \
      --dev-target $dir/3/block.db \
      --command bluefs-bdev-migrate || return 1

    ceph-bluestore-tool --path $dir/3 fsck || return 1

    run_osd_bluestore $dir 0 || return 1
    osd_pid0=$(cat $dir/osd.0.pid)
    run_osd_bluestore $dir 1 || return 1
    osd_pid1=$(cat $dir/osd.1.pid)
    run_osd_bluestore $dir 2 || return 1
    osd_pid2=$(cat $dir/osd.2.pid)
    run_osd_bluestore $dir 3 || return 1
    osd_pid3=$(cat $dir/osd.3.pid)

    wait_for_clean || return 1

    # write some objects
    timeout 60 rados bench -p foo 30 write -b 4096 --no-cleanup #|| return 1

    # kill
    while kill $osd_pid0; do sleep 1 ; done
    ceph osd down 0
    while kill $osd_pid1; do sleep 1 ; done
    ceph osd down 1
    while kill $osd_pid2; do sleep 1 ; done
    ceph osd down 2
    while kill $osd_pid3; do sleep 1 ; done
    ceph osd down 3

    # slow, DB -> slow, DB, WAL
    ceph-bluestore-tool --path $dir/0 fsck || return 1

    dd if=/dev/zero  of=$dir/0/wal count=512 bs=1M
    ceph-bluestore-tool --path $dir/0 \
      --dev-target $dir/0/wal \
      --command bluefs-bdev-new-wal || return 1

    ceph-bluestore-tool --path $dir/0 fsck || return 1

    # slow, WAL -> slow, DB, WAL
    ceph-bluestore-tool --path $dir/1 fsck || return 1

    dd if=/dev/zero  of=$dir/1/db count=1024 bs=1M
    ceph-bluestore-tool --path $dir/1 \
      --dev-target $dir/1/db \
      --command bluefs-bdev-new-db || return 1

    ceph-bluestore-tool --path $dir/1 \
      --devs-source $dir/1/block \
      --dev-target $dir/1/block.db \
      --command bluefs-bdev-migrate || return 1

    ceph-bluestore-tool --path $dir/1 fsck || return 1

    # slow -> slow, DB, WAL
    ceph-bluestore-tool --path $dir/2 fsck || return 1

    ceph-bluestore-tool --path $dir/2 \
      --command bluefs-bdev-new-db || return 1

    ceph-bluestore-tool --path $dir/2 \
      --command bluefs-bdev-new-wal || return 1

    ceph-bluestore-tool --path $dir/2 \
      --devs-source $dir/2/block \
      --dev-target $dir/2/block.db \
      --command bluefs-bdev-migrate || return 1

    ceph-bluestore-tool --path $dir/2 fsck || return 1

    # slow, DB -> slow, WAL
    ceph-bluestore-tool --path $dir/3 fsck || return 1

    ceph-bluestore-tool --path $dir/3 \
      --command bluefs-bdev-new-wal || return 1

    ceph-bluestore-tool --path $dir/3 \
      --devs-source $dir/3/block.db \
      --dev-target $dir/3/block \
      --command bluefs-bdev-migrate || return 1

    ceph-bluestore-tool --path $dir/3 fsck || return 1

    run_osd_bluestore $dir 0 || return 1
    osd_pid0=$(cat $dir/osd.0.pid)
    run_osd_bluestore $dir 1 || return 1
    osd_pid1=$(cat $dir/osd.1.pid)
    run_osd_bluestore $dir 2 || return 1
    osd_pid2=$(cat $dir/osd.2.pid)
    run_osd_bluestore $dir 3 || return 1
    osd_pid3=$(cat $dir/osd.3.pid)

    # write some objects
    timeout 60 rados bench -p foo 30 write -b 4096 --no-cleanup #|| return 1

    # kill
    while kill $osd_pid0; do sleep 1 ; done
    ceph osd down 0
    while kill $osd_pid1; do sleep 1 ; done
    ceph osd down 1
    while kill $osd_pid2; do sleep 1 ; done
    ceph osd down 2
    while kill $osd_pid3; do sleep 1 ; done
    ceph osd down 3

    # slow, DB1, WAL -> slow, DB2, WAL
    ceph-bluestore-tool --path $dir/0 fsck || return 1

    dd if=/dev/zero  of=$dir/0/db2 count=1024 bs=1M
    ceph-bluestore-tool --path $dir/0 \
      --devs-source $dir/0/block.db \
      --dev-target $dir/0/db2 \
      --command bluefs-bdev-migrate || return 1

    ceph-bluestore-tool --path $dir/0 fsck || return 1

    # slow, DB, WAL1 -> slow, DB, WAL2

    dd if=/dev/zero  of=$dir/0/wal2 count=512 bs=1M
    ceph-bluestore-tool --path $dir/0 \
      --devs-source $dir/0/block.wal \
      --dev-target $dir/0/wal2 \
      --command bluefs-bdev-migrate || return 1
    rm -rf $dir/0/wal

    ceph-bluestore-tool --path $dir/0 fsck || return 1

    # slow, DB + WAL -> slow, DB2 -> slow
    ceph-bluestore-tool --path $dir/1 fsck || return 1

    dd if=/dev/zero  of=$dir/1/db2 count=1024 bs=1M
    ceph-bluestore-tool --path $dir/1 \
      --devs-source $dir/1/block.db \
      --devs-source $dir/1/block.wal \
      --dev-target $dir/1/db2 \
      --command bluefs-bdev-migrate || return 1

    rm -rf $dir/1/db

    ceph-bluestore-tool --path $dir/1 fsck || return 1

    ceph-bluestore-tool --path $dir/1 \
      --devs-source $dir/1/block.db \
      --dev-target $dir/1/block \
      --command bluefs-bdev-migrate || return 1

    rm -rf $dir/1/db2

    ceph-bluestore-tool --path $dir/1 fsck || return 1

    # slow -> slow, DB (negative case)
    ceph-objectstore-tool --type bluestore --data-path $dir/2 \
			  --op fsck --no-mon-config || return 1

    dd if=/dev/zero  of=$dir/2/db2 count=1024 bs=1M
    ceph-bluestore-tool --path $dir/2 \
      --devs-source $dir/2/block \
      --dev-target $dir/2/db2 \
      --command bluefs-bdev-migrate

    # Migration from slow-only to new device is unsupported
    if [ $? -eq 0 ]; then
        return 1
    fi
    ceph-bluestore-tool --path $dir/2 fsck || return 1

    # slow + DB + WAL -> slow, DB2
    dd if=/dev/zero  of=$dir/2/db2 count=1024 bs=1M

    ceph-bluestore-tool --path $dir/2 \
      --devs-source $dir/2/block \
      --devs-source $dir/2/block.db \
      --devs-source $dir/2/block.wal \
      --dev-target $dir/2/db2 \
      --command bluefs-bdev-migrate || return 1

    ceph-bluestore-tool --path $dir/2 fsck || return 1

    # slow + WAL -> slow2, WAL2
    dd if=/dev/zero  of=$dir/3/wal2 count=1024 bs=1M

    ceph-bluestore-tool --path $dir/3 \
      --devs-source $dir/3/block \
      --devs-source $dir/3/block.wal \
      --dev-target $dir/3/wal2 \
      --command bluefs-bdev-migrate || return 1

    ceph-bluestore-tool --path $dir/3 fsck || return 1

    run_osd_bluestore $dir 0 || return 1
    osd_pid0=$(cat $dir/osd.0.pid)
    run_osd_bluestore $dir 1 || return 1
    osd_pid1=$(cat $dir/osd.1.pid)
    run_osd_bluestore $dir 2 || return 1
    osd_pid2=$(cat $dir/osd.2.pid)
    run_osd_bluestore $dir 3 || return 1
    osd_pid3=$(cat $dir/osd.3.pid)

    # write some objects
    timeout 60 rados bench -p foo 30 write -b 4096 --no-cleanup #|| return 1

    wait_for_clean || return 1
}

main osd-bluefs-volume-ops "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/osd/osd-bluefs-volume-ops.sh"
# End:
