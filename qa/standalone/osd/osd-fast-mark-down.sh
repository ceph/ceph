#!/usr/bin/env bash
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

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
MAX_PROPAGATION_TIME=30

function run() {
    local dir=$1
    shift
    rm -f $dir/*.pid
    export CEPH_MON="127.0.0.1:7126" # git grep '\<7126\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "

    OLD_ARGS=$CEPH_ARGS
    CEPH_ARGS+="--osd-fast-fail-on-connection-refused=false "
    echo "Ensuring old behavior is there..."
    test_fast_kill $dir && (echo "OSDs died too early! Old behavior doesn't work." ; return 1)

    CEPH_ARGS=$OLD_ARGS"--osd-fast-fail-on-connection-refused=true "
    OLD_ARGS=$CEPH_ARGS

    CEPH_ARGS=$OLD_ARGS"--ms_type=async --mon-host=$CEPH_MON"
    echo "Testing async msgr..."
    test_fast_kill $dir || return 1

    return 0

}

function test_fast_kill() {
   # create cluster with 3 osds
   setup $dir || return 1
   run_mon $dir a --osd_pool_default_size=3 || return 1
   run_mgr $dir x || return 1
   for oi in {0..2}; do
     run_osd $dir $oi || return 1
     pids[$oi]=$(cat $dir/osd.$oi.pid)
   done

   create_rbd_pool || return 1

   # make some objects so osds to ensure connectivity between osds
   timeout 20 rados -p rbd bench 10 write -b 4096 --max-objects 128 --no-cleanup || return 1
   sleep 1

   killid=0
   previd=0

   # kill random osd and see if after max MAX_PROPAGATION_TIME, the osd count decreased.
   for i in {1..2}; do
     while [ $killid -eq $previd ]; do
        killid=${pids[$RANDOM%${#pids[@]}]}
     done
     previd=$killid

     kill -9 $killid
     time_left=$MAX_PROPAGATION_TIME
     down_osds=0

     while [ $time_left -gt 0 ]; do
       sleep 1
       time_left=$[$time_left - 1];

       grep -m 1 -c -F "ms_handle_refused" $dir/osd.*.log > /dev/null
       if [ $? -ne 0 ]; then
         continue
       fi

       down_osds=$(ceph osd tree | grep -c down)
       if [ $down_osds -lt $i ]; then
         # osds not marked down yet, try again in a second
         continue
       elif [ $down_osds -gt $i ]; then
         echo Too many \($down_osds\) osds died!
         return 1
       else
         break
       fi
     done

     if [ $down_osds -lt $i ]; then
        echo Killed the OSD, yet it is not marked down
        ceph osd tree
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
