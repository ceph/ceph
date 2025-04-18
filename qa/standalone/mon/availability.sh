#!/usr/bin/env bash
#
# Copyright (C) 2024 IBM <contact@ibm.com>
#
# Author: Shraddha Agrawal <shraddhaag@ibm.com> 
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

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7124" # git grep '\<7124\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function TEST_availablity_score() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    
    ceph config set osd osd_recovery_delay_start 10000
    ceph config get osd.* osd_recovery_delay_start
    ceph osd pool create foo 64
    ceph osd pool set foo size 2 --yes-i-really-mean-it 

    WAIT_FOR_CLEAN_TIMEOUT=60 wait_for_clean
    ceph osd pool stats

    ceph -s 
    ceph health | grep HEALTH_OK || return 1
    ceph osd pool availability-status
    AVAILABILITY_STATUS=$(ceph osd pool availability-status | grep -w "foo")
    SCORE=$(echo "$AVAILABILITY_STATUS" | awk '{print $7}')
    IS_AVAILABLE=$(echo "$AVAILABILITY_STATUS" | awk '{print $8}')
    if [ $IS_AVAILABLE -ne 1 ]; then
      echo "Failed: Pool is not available in availabilty status"
    fi

    # write some objects
    for i in $(seq 1 10); do
      rados --pool foo put object_id$i /etc/group;
    done

    # kill OSD 0 
    kill_daemons $dir TERM osd.0 >&2 < /dev/null || return 1
    sleep 10
    ceph -s
    ceph osd pool availability-status

    #write more objects 
    for i in $(seq 1 20); do
      rados --pool foo put object_id$i /etc/group;
    done

    # bring osd 0 back up 
    activate_osd $dir 0 || return 1
    ceph -s
    ceph osd pool availability-status

    # kill osd 1
    kill_daemons $dir TERM osd.1 >&2 < /dev/null || return 1
    ceph -s
    ceph osd pool availability-status

    # wait for 10 seconds so availability score is refreshed
    # check ceph heath and availability score
    sleep 10
    ceph -s
    ceph osd pool availability-status
    AVAILABILITY_STATUS=$(ceph osd pool availability-status | grep -w "foo")
    IS_AVAILABLE=$(echo "$AVAILABILITY_STATUS" | awk '{print $8}')
    NEW_SCORE=$(echo "$AVAILABILITY_STATUS" | awk '{print $7}')
    if [ $IS_AVAILABLE -ne 0 ]; then
      echo "Failed: Pool is available in availabilty status when unfound objects present"
      return 1
    fi
    if (( $(echo "$NEW_SCORE >= $SCORE" | bc -l) )); then
      echo "Failed: Availability score for the pool did not drop"
      return 1
    fi

    echo "TEST PASSED"
    return 0
}

main availability "$@"
