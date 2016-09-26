#!/bin/bash
#
# Copyright (C) 2016 Piotr Dałek <git@predictor.org.pl>
# Copyright (C) 2014, 2015 Red Hat <contact@redhat.com>
#
# Author: Piotr Dałek <git@predictor.org.pl>
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

source $(dirname $0)/../detect-build-env-vars.sh
source $CEPH_ROOT/qa/workunits/ceph-helpers.sh

function run() {
    local dir=$1
    shift
    rm -f $dir/*.pid
    export CEPH_MON="127.0.0.1:7126" # git grep '\<7126\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    
    echo "Ensuring old behavior is there..."
    test_fast_kill $dir && (echo "OSDs died too early!" ; return 1)

    CEPH_ARGS+="--osd-fast-fail-on-connection-refused=true"
    OLD_ARGS=$CEPH_ARGS

    CEPH_ARGS+="--ms_type=simple"
    echo "Testing simple msgr..."
    test_fast_kill $dir || return 1

    CEPH_ARGS=$OLD_ARGS"--ms_type=async"
    echo "Testing async msgr..."
    test_fast_kill $dir || return 1

    return 0

}

function test_fast_kill() {
   # create cluster with 3 osds
   setup $dir || return 1
   run_mon $dir a --osd_pool_default_size=3 || return 1
   for oi in {0..2}; do
     run_osd $dir $oi || return 1
     pids[$oi]=$(cat $dir/osd.$oi.pid)
   done

   # make some objects so osds to ensure connectivity between osds
   rados -p rbd bench 10 write -b 4096 --max-objects 128 --no-cleanup
   sleep 1

   killid=0
   previd=0

   # kill random osd and see if 1 sec after, the osd count decreased.
   for i in {1..2}; do
     while [ $killid -eq $previd ]; do
        killid=${pids[$RANDOM%${#pids[@]}]}
     done
     previd=$killid

     kill -9 $killid
     sleep 1

     down_osds=$(ceph osd tree | grep -c down)
     if [ $down_osds -lt $i ]; then
        echo Killed the OSD, yet it is not marked down
        ceph osd tree
        teardown $dir
        return 1
     elif [ $down_osds -gt $i ]; then
        echo Too many \($down_osds\) osds died!
        teardown $dir
        return 1
     fi
     
   done
   pkill -SIGTERM rados
   teardown $dir || return 1
}

main osd-fast-mark-down "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/osd/osd-fast-mark-down.sh"
# End:
