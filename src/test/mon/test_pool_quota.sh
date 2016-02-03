#!/bin/bash 

#
# Generic pool quota test
#

# Includes
source ../qa/workunits/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:17108"
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
   
    local poolname=testquoa
    ceph osd  pool create $poolname 20
    local objects=`ceph df detail | grep -w $poolname|awk '{print $4}'`
    local bytes=`ceph df detail | grep -w $poolname|awk '{print $5}'`

    echo $objects
    echo $bytes
    if [ $objects != 'N/A' ] || [ $bytes != 'N/A' ] ;
      then
      return 1
    fi

    ceph osd pool set-quota  $poolname   max_objects 1000
    ceph osd pool set-quota  $poolname  max_bytes 1024

    objects=`ceph df detail | grep -w $poolname|awk '{print $4}'`
    bytes=`ceph df detail | grep -w $poolname|awk '{print $5}'`
   
     if [ $objects != '1000' ] || [ $bytes != '1024' ] ;
       then
       return 1
     fi

     ceph osd pool delete  $poolname $poolname  --yes-i-really-really-mean-it
     teardown $dir || return 1
}

main testpoolquota
