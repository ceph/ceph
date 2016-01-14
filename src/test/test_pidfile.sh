#!/bin/bash 

#
# test pidfile here 
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

function TEST_without_pidfile() {
    local dir=$1
    setup $dir
    local RUNID=`uuidgen`
    run_mon $dir a --pid-file= --daemonize=$RUNID || { teardown_unexist_pidfile $dir; return 1; } 
    run_osd $dir 0 --pid-file= --daemonize=$RUNID || { teardown_unexist_pidfile $dir; return 1; }
    teardown_unexist_pidfile $dir $RUNID || return 1
}

function teardown_unexist_pidfile() {
    local dir=$1
    shift
    local RUNID=$1
    shift
    local delays=${4:-0 0 1 1 1 2 3 5 5 5 10 10 20 60 60 60 120}
    local pids=$(ps aux|awk "/cep[h].*$RUNID.*/ {print \$2}")
    local status=0
    for i in $pids ; do
        local kill_complete=false
        for try in $delays ; do  
            if kill $i 2> /dev/null ; then
                kill_complete=false
                sleep $try
            else
                kill_complete=true
                break
            fi
       done
       if ! $kill_complete ; then
            status=1
       fi   
    done
    if [ $(stat -f -c '%T' .) == "btrfs" ]; then
         __teardown_btrfs $dir
    fi
    rm -fr $dir
    return $status
}

function TEST_contend_pidfile() {
    local dir=$1
    setup $dir 
 
    run_mon $dir a 
    run_mon $dir a 2>&1 | grep "failed to lock pidfile" || return 1

    run_osd $dir 0 
    run_osd $dir 0 2>&1 | grep "failed to lock pidfile" || return 1
    teardown $dir || return 1
}

main pidfile
