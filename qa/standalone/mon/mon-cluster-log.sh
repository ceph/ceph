#!/usr/bin/env bash
#
# Copyright (C) 2022 Red Hat <contact@redhat.com>
#
# Author: Prashant D <pdhange@redhat.com>
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

    export CEPH_MON="127.0.0.1:7156" # git grep '\<7156\>' : there must be only one
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

function TEST_cluster_log_level() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1

    ceph config set mon.a mon_cluster_log_level debug
    ceph osd pool create replicated1 8 8
    ceph osd pool set replicated1 size 1 --yes-i-really-mean-it
    ceph osd pool set replicated1 min_size 1

    WAIT_FOR_CLEAN_TIMEOUT=60 wait_for_clean
    ERRORS=0
    truncate $dir/log -s 0
    ceph pg deep-scrub 1.0
    search_str="cluster [[]DBG[]] 1.0 deep-scrub"
    TIMEOUT=60 wait_for_string $dir/log "$search_str"
    grep -q "$search_str" $dir/log
    return_code=$?
    if [ $return_code -ne 0 ]; then
      echo "Failed : Could not find DBG log in the cluster log file"
      ERRORS=$(($ERRORS + 1))
    fi

    ceph osd down 0
    TIMEOUT=20 wait_for_osd up 0 || return 1
    grep -q "cluster [[]INF[]] osd.0.*boot" $dir/log
    return_code=$?
    if [ $return_code -ne 0 ]; then
      echo "Failed : Could not find INF log in the cluster log file"
      ERRORS=$(($ERRORS + 1))
    fi

    ceph config set mon.a mon_cluster_log_level info
    ceph pg deep-scrub 1.1
    search_str="cluster [[]DBG[]] 1.1 deep-scrub"
    TIMEOUT=60 wait_for_string $dir/log "$search_str"
    grep -q "$search_str" $dir/log
    return_code=$?
    if [ $return_code -eq 0 ]; then
      echo "Failed : Found DBG log in the cluster log file"
      ERRORS=$(($ERRORS + 1))
    fi

    ceph config set mon.a mon_cluster_log_level warn
    ceph osd set noup
    ceph osd down osd.0
    ceph osd unset noup
    TIMEOUT=60 wait_for_osd up 0 || return 1
    search_str="cluster [[]WRN[]] Health check failed: noup flag(s) set (OSDMAP_FLAGS)"
    grep -q "$search_str" $dir/log
    return_code=$?
    if [ $return_code -ne 0 ]; then
      echo "Failed : No WRN entries found in the cluster log file"
      ERRORS=$(($ERRORS + 1))
    fi

    ceph osd out 0
    ceph osd in 0
    WAIT_FOR_CLEAN_TIMEOUT=60 wait_for_clean
    search_str="cluster [[]INF[]] Client client.admin marked osd.0 out, while it was still marked up"
    ceph log last 1000 | grep -q "$search_str" || return 1
    TIMEOUT=60 wait_for_string $dir/log "$search_str"
    grep -q "$search_str" $dir/log
    return_code=$?
    if [ $return_code -eq 0 ]; then
      echo "Failed : Found INF log in the cluster log file"
      ERRORS=$(($ERRORS + 1))
    fi

    if [ $ERRORS -gt 0 ]; then
        echo "TEST FAILED WITH $ERRORS ERRORS"
        return 1
    fi

    echo "TEST PASSED"
    return 0
}

function TEST_journald_cluster_log_level() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1

    ceph config set mon.a mon_cluster_log_level debug
    ceph osd pool create replicated1 8 8
    ceph osd pool set replicated1 size 1 --yes-i-really-mean-it
    ceph osd pool set replicated1 min_size 1

    WAIT_FOR_CLEAN_TIMEOUT=60 wait_for_clean
    ERRORS=0
    ceph config set mon.a mon_cluster_log_to_journald true

    ceph pg deep-scrub 1.0
    search_str="1.0 deep-scrub"
    TIMEOUT=60
    sleep $TIMEOUT
    journalctl _COMM=ceph-mon CEPH_CHANNEL=cluster PRIORITY=7 --output=json-pretty --since "60 seconds ago" |jq '.MESSAGE' > $dir/journal.log
    grep -q "$search_str" $dir/journal.log
    return_code=$?
    if [ $return_code -ne 0 ]; then
      echo "Failed : Could not find DBG log in the journalctl log file"
      ERRORS=$(($ERRORS + 1))
    fi

    ceph osd down 0
    TIMEOUT=20 wait_for_osd up 0 || return 1
    search_str="osd.0.*boot"
    journalctl _COMM=ceph-mon CEPH_CHANNEL=cluster PRIORITY=6 --output=json-pretty --since "60 seconds ago" |jq '.MESSAGE' > $dir/journal.log
    grep -q "$search_str" $dir/journal.log
    return_code=$?
    if [ $return_code -ne 0 ]; then
      echo "Failed : Could not find INF log in the journalctl log file"
      ERRORS=$(($ERRORS + 1))
    fi

    ceph config set mon.a mon_cluster_log_level info
    ceph pg deep-scrub 1.1
    TIMEOUT=60
    sleep $TIMEOUT
    search_str="1.1 deep-scrub"
    journalctl _COMM=ceph-mon CEPH_CHANNEL=cluster PRIORITY=7 --output=json-pretty --since "60 seconds ago" |jq '.MESSAGE' > $dir/journal.log
    grep -q "$search_str" $dir/journal.log
    return_code=$?
    if [ $return_code -eq 0 ]; then
      echo "Failed : Found $clog_entries DBG log entries in the journalctl log file"
      ERRORS=$(($ERRORS + 1))
    fi

    ceph config set mon.a mon_cluster_log_level warn
    ceph osd set noup
    ceph osd down osd.0
    ceph osd unset noup
    TIMEOUT=60 wait_for_osd up 0 || return 1
    search_str="Health check failed: noup flag(s) set (OSDMAP_FLAGS)"
    journalctl _COMM=ceph-mon CEPH_CHANNEL=cluster PRIORITY=4 --output=json-pretty --since "60 seconds ago" |jq '.MESSAGE' > $dir/journal.log
    grep -q "$search_str" $dir/journal.log
    return_code=$?
    if [ $return_code -ne 0 ]; then
      echo "Failed : No WRN entries found in the journalctl log file"
      ERRORS=$(($ERRORS + 1))
    fi

    ceph osd out 0
    ceph osd in 0
    WAIT_FOR_CLEAN_TIMEOUT=60 wait_for_clean
    search_str="Client client.admin marked osd.0 out, while it was still marked up"
    ceph log last | grep -q "$search_str" || return 1
    journalctl _COMM=ceph-mon CEPH_CHANNEL=cluster PRIORITY=6 --output=json-pretty --since "60 seconds ago" |jq '.MESSAGE' > $dir/journal.log
    grep -q "$search_str" $dir/journal.log
    return_code=$?
    if [ $return_code -eq 0 ]; then
      echo "Failed : Found $clog_entries INF log entries in the journalctl log file"
      ERRORS=$(($ERRORS + 1))
    fi

    if [ $ERRORS -gt 0 ]; then
        echo "TEST FAILED WITH $ERRORS ERRORS"
        return 1
    fi

    echo "TEST PASSED"
    return 0
}

main mon-cluster-log "$@"
