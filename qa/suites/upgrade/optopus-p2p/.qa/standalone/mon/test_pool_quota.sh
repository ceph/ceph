#!/usr/bin/env bash

#
# Generic pool quota test
#

# Includes


source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:17108" # git grep '\<17108\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        $func $dir || return 1
    done
}

function TEST_pool_quota() {
    local dir=$1
    setup $dir || return 1

    run_mon $dir a || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1

    local poolname=testquota
    create_pool $poolname 20
    local objects=`ceph df detail | grep -w $poolname|awk '{print $3}'`
    local bytes=`ceph df detail | grep -w $poolname|awk '{print $4}'`

    echo $objects
    echo $bytes
    if [ $objects != 'N/A' ] || [ $bytes != 'N/A' ] ;
      then
      return 1
    fi

    ceph osd pool set-quota  $poolname   max_objects 1000
    ceph osd pool set-quota  $poolname  max_bytes 1024

    objects=`ceph df detail | grep -w $poolname|awk '{print $3}'`
    bytes=`ceph df detail | grep -w $poolname|awk '{print $4}'`
   
    if [ $objects != '1000' ] || [ $bytes != '1K' ] ;
      then
      return 1
    fi

    ceph osd pool delete  $poolname $poolname  --yes-i-really-really-mean-it
    teardown $dir || return 1
}

main testpoolquota
