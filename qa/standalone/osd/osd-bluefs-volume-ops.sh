#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

[ `uname` = FreeBSD ] && exit 0

function run() {
    local dir=$1
    shift

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
    export CEPH_MON="127.0.0.1:7146" # git grep '\<7146\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    CEPH_ARGS+="--bluestore_block_size=2147483648 "
    CEPH_ARGS+="--bluestore_block_db_create=true "
    CEPH_ARGS+="--bluestore_block_db_size=1073741824 "
    CEPH_ARGS+="--bluestore_block_wal_size=536870912 "
    CEPH_ARGS+="--bluestore_block_wal_create=true "
    CEPH_ARGS+="--bluestore_fsck_on_mount=true "

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    osd_pid0=$(cat $dir/osd.0.pid)
    run_osd $dir 1 || return 1
    osd_pid1=$(cat $dir/osd.1.pid)
    run_osd $dir 2 || return 1
    osd_pid2=$(cat $dir/osd.2.pid)
    run_osd $dir 3 || return 1
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

    activate_osd $dir 0 || return 1
    osd_pid0=$(cat $dir/osd.0.pid)
    activate_osd $dir 1 || return 1
    osd_pid1=$(cat $dir/osd.1.pid)
    activate_osd $dir 2 || return 1
    osd_pid2=$(cat $dir/osd.2.pid)
    activate_osd $dir 3 || return 1
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

    activate_osd $dir 0 || return 1
    osd_pid0=$(cat $dir/osd.0.pid)
    activate_osd $dir 1 || return 1
    osd_pid1=$(cat $dir/osd.1.pid)
    activate_osd $dir 2 || return 1
    osd_pid2=$(cat $dir/osd.2.pid)
    activate_osd $dir 3 || return 1
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

    activate_osd $dir 0 || return 1
    osd_pid0=$(cat $dir/osd.0.pid)
    activate_osd $dir 1 || return 1
    osd_pid1=$(cat $dir/osd.1.pid)
    activate_osd $dir 2 || return 1
    osd_pid2=$(cat $dir/osd.2.pid)
    activate_osd $dir 3 || return 1
    osd_pid3=$(cat $dir/osd.3.pid)

    # write some objects
    timeout 60 rados bench -p foo 30 write -b 4096 --no-cleanup #|| return 1

    wait_for_clean || return 1
}

function TEST_bluestore2() {
    local dir=$1

    local flimit=$(ulimit -n)
    if [ $flimit -lt 1536 ]; then
        echo "Low open file limit ($flimit), test may fail. Increase to 1536 or higher and retry if that happens."
    fi
    export CEPH_MON="127.0.0.1:7146" # git grep '\<7146\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    CEPH_ARGS+="--bluestore_block_size=4294967296 "
    CEPH_ARGS+="--bluestore_block_db_create=true "
    CEPH_ARGS+="--bluestore_block_db_size=1073741824 "
    CEPH_ARGS+="--bluestore_block_wal_create=false "
    CEPH_ARGS+="--bluestore_fsck_on_mount=true "
    CEPH_ARGS+="--osd_pool_default_size=1 "
    CEPH_ARGS+="--osd_pool_default_min_size=1 "
    CEPH_ARGS+="--bluestore_debug_enforce_settings=ssd "

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    osd_pid0=$(cat $dir/osd.0.pid)

    sleep 5
    create_pool foo 16

    retry = 0
    while [[ $retry -le 5 ]]; do
      # write some objects
      timeout 60 rados bench -p foo 10 write --write-omap --no-cleanup #|| return 1

      #give RocksDB some time to cooldown and put files to slow level(s)
      sleep 10

      db_used=$( ceph tell osd.0 perf dump bluefs | jq ".bluefs.db_used_bytes" )
      spilled_over=$( ceph tell osd.0 perf dump bluefs | jq ".bluefs.slow_used_bytes" )
      ((retry+=1))
      test $spilled_over -eq 0 || break
    done
    test $spilled_over -gt 0 || return 1

    while kill $osd_pid0; do sleep 1 ; done
    ceph osd down 0

    ceph-bluestore-tool --path $dir/0 \
      --devs-source $dir/0/block.db \
      --dev-target $dir/0/block \
      --command bluefs-bdev-migrate || return 1

    ceph-bluestore-tool --path $dir/0 \
      --command bluefs-bdev-sizes || return 1

    ceph-bluestore-tool --path $dir/0 \
      --command fsck || return 1

    activate_osd $dir 0 || return 1
    osd_pid0=$(cat $dir/osd.0.pid)

    wait_for_clean || return 1
}

function TEST_bluestore_expand() {
    local dir=$1

    local flimit=$(ulimit -n)
    if [ $flimit -lt 1536 ]; then
        echo "Low open file limit ($flimit), test may fail. Increase to 1536 or higher and retry if that happens."
    fi
    export CEPH_MON="127.0.0.1:7146" # git grep '\<7146\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    CEPH_ARGS+="--bluestore_block_size=4294967296 "
    CEPH_ARGS+="--bluestore_block_db_create=true "
    CEPH_ARGS+="--bluestore_block_db_size=1073741824 "
    CEPH_ARGS+="--bluestore_block_wal_create=false "
    CEPH_ARGS+="--bluestore_fsck_on_mount=true "
    CEPH_ARGS+="--osd_pool_default_size=1 "
    CEPH_ARGS+="--osd_pool_default_min_size=1 "
    CEPH_ARGS+="--bluestore_debug_enforce_settings=ssd "

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    osd_pid0=$(cat $dir/osd.0.pid)

    sleep 5
    create_pool foo 16

    # write some objects
    timeout 60 rados bench -p foo 30 write -b 4096 --no-cleanup #|| return 1
    sleep 5
    
    total_space_before=$( ceph tell osd.0 perf dump bluefs | jq ".bluefs.slow_total_bytes" )
    free_space_before=`ceph tell osd.0 bluestore bluefs device info | grep "BDEV_SLOW" -A 2 | grep free | cut -d':' -f 2 | cut -d"," -f 1 | cut -d' ' -f 2`
    
    # kill
    while kill $osd_pid0; do sleep 1 ; done
    ceph osd down 0

    # destage allocation to file before expand (in case fast-shutdown skipped that step)
    ceph-bluestore-tool --log-file $dir/bluestore_tool.log --path $dir/0 allocmap || return 1

    # expand slow devices
    ceph-bluestore-tool --log-file $dir/bluestore_tool.log --path $dir/0 fsck || return 1

    requested_space=4294967296 # 4GB
    truncate $dir/0/block -s $requested_space
    ceph-bluestore-tool --log-file $dir/bluestore_tool.log --path $dir/0 bluefs-bdev-expand || return 1

    # slow, DB, WAL -> slow, DB
    ceph-bluestore-tool --log-file $dir/bluestore_tool.log --path $dir/0 fsck || return 1

    # compare allocation-file with RocksDB state
    ceph-bluestore-tool --log-file $dir/bluestore_tool.log --path $dir/0 qfsck || return 1

    ceph-bluestore-tool --log-file $dir/bluestore_tool.log --path $dir/0 bluefs-bdev-sizes
    
    activate_osd $dir 0 || return 1
    osd_pid0=$(cat $dir/osd.0.pid)

    wait_for_clean || return 1
    
    total_space_after=$( ceph tell osd.0 perf dump bluefs | jq ".bluefs.slow_total_bytes" )
    free_space_after=`ceph tell osd.0 bluestore bluefs device info | grep "BDEV_SLOW" -A 2 | grep free | cut -d':' -f 2 | cut -d"," -f 1 | cut -d' ' -f 2`

    if [$total_space_after != $requested_space]; then
	echo "total_space_after = $total_space_after"
	echo "requested_space   = $requested_space"
	return 1;
    fi

    total_space_added=$((total_space_after - total_space_before))
    free_space_added=$((free_space_after - free_space_before))

    let new_used_space=($total_space_added - $free_space_added)
    echo $new_used_space
    # allow upto 128KB to be consumed
    if [ $new_used_space -gt 131072 ]; then
	echo "total_space_added = $total_space_added"
	echo "free_space_added  = $free_space_added"
	return 1;
    fi
    
    # kill
    while kill $osd_pid0; do sleep 1 ; done
    ceph osd down 0

    ceph-bluestore-tool --log-file $dir/bluestore_tool.log --path $dir/0 qfsck || return 1
}

main osd-bluefs-volume-ops "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/osd/osd-bluefs-volume-ops.sh"
# End:
