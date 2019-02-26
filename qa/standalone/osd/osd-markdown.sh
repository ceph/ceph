#!/usr/bin/env bash
#
# Copyright (C) 2015 Intel <contact@intel.com.com>
# Copyright (C) 2014, 2015 Red Hat <contact@redhat.com>
#
# Author: Xiaoxi Chen <xiaoxi.chen@intel.com>
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

    export CEPH_MON="127.0.0.1:7108" # git grep '\<7108\>' : there must be only one
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

function markdown_N_impl() {
  markdown_times=$1
  total_time=$2
  sleeptime=$3
  for i in `seq 1 $markdown_times`
  do
    # check the OSD is UP
    ceph osd tree
    ceph osd tree | grep osd.0 |grep up || return 1
    # mark the OSD down.
    # override any dup setting in the environment to ensure we do this
    # exactly once (modulo messenger failures, at least; we can't *actually*
    # provide exactly-once semantics for mon commands).
    CEPH_CLI_TEST_DUP_COMMAND=0 ceph osd down 0
    sleep $sleeptime
  done
}


function TEST_markdown_exceed_maxdown_count() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1

    create_rbd_pool || return 1

    # 3+1 times within 300s, osd should stay dead on the 4th time
    local count=3
    local sleeptime=10
    local period=300
    ceph tell osd.0 injectargs '--osd_max_markdown_count '$count'' || return 1
    ceph tell osd.0 injectargs '--osd_max_markdown_period '$period'' || return 1

    markdown_N_impl $(($count+1)) $period $sleeptime
    # down N+1 times ,the osd.0 should die
    ceph osd tree | grep down | grep osd.0 || return 1
}

function TEST_markdown_boot() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1

    create_rbd_pool || return 1

    # 3 times within 120s, should stay up
    local count=3
    local sleeptime=10
    local period=120
    ceph tell osd.0 injectargs '--osd_max_markdown_count '$count'' || return 1
    ceph tell osd.0 injectargs '--osd_max_markdown_period '$period'' || return 1

    markdown_N_impl $count $period $sleeptime
    #down N times, osd.0 should be up
    sleep 15  # give osd plenty of time to notice and come back up
    ceph osd tree | grep up | grep osd.0 || return 1
}

function TEST_markdown_boot_exceed_time() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1

    create_rbd_pool || return 1

    # 3+1 times, but over 40s, > 20s, so should stay up
    local count=3
    local period=20
    local sleeptime=10
    ceph tell osd.0 injectargs '--osd_max_markdown_count '$count'' || return 1
    ceph tell osd.0 injectargs '--osd_max_markdown_period '$period'' || return 1

    markdown_N_impl $(($count+1)) $period $sleeptime
    sleep 15  # give osd plenty of time to notice and come back up
    ceph osd tree | grep up | grep osd.0 || return 1
}

main osd-markdown "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/osd/osd-bench.sh"
# End:
