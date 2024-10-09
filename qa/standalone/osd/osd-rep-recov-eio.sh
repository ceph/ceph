#!/usr/bin/env bash
#
# Copyright (C) 2017 Red Hat <contact@redhat.com>
#
#
# Author: Kefu Chai <kchai@redhat.com>
# Author: David Zafman <dzafman@redhat.com>
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

warnings=10

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7140" # git grep '\<7140\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "


    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
	# set warning amount in case default changes
        run_mon $dir a --mon_osd_warn_num_repaired=$warnings || return 1
	run_mgr $dir x || return 1
	ceph osd pool create foo 8 || return 1

        $func $dir || return 1
        teardown $dir || return 1
    done
}

function setup_osds() {
    local count=$1
    shift
    local type=$1

    for id in $(seq 0 $(expr $count - 1)) ; do
        run_osd${type} $dir $id || return 1
    done
    wait_for_clean || return 1
}

function get_state() {
    local pgid=$1
    local sname=state
    ceph --format json pg dump pgs 2>/dev/null | \
        jq -r ".pg_stats | .[] | select(.pgid==\"$pgid\") | .$sname"
}

function rados_put() {
    local dir=$1
    local poolname=$2
    local objname=${3:-SOMETHING}

    for marker in AAA BBB CCCC DDDD ; do
        printf "%*s" 1024 $marker
    done > $dir/ORIGINAL
    #
    # get and put an object, compare they are equal
    #
    rados --pool $poolname put $objname $dir/ORIGINAL || return 1
}

function rados_get() {
    local dir=$1
    local poolname=$2
    local objname=${3:-SOMETHING}
    local expect=${4:-ok}

    #
    # Expect a failure to get object
    #
    if [ $expect = "fail" ];
    then
        ! rados --pool $poolname get $objname $dir/COPY
        return
    fi
    #
    # Expect hang trying to get object
    #
    if [ $expect = "hang" ];
    then
        timeout 5 rados --pool $poolname get $objname $dir/COPY
        test "$?" = "124"
        return
    fi
    #
    # get an object, compare with $dir/ORIGINAL
    #
    rados --pool $poolname get $objname $dir/COPY || return 1
    diff $dir/ORIGINAL $dir/COPY || return 1
    rm $dir/COPY
}

function rados_get_data() {
    local inject=$1
    shift
    local dir=$1

    local poolname=pool-rep
    local objname=obj-$inject-$$
    local pgid=$(get_pg $poolname $objname)

    rados_put $dir $poolname $objname || return 1
    inject_$inject rep data $poolname $objname $dir 0 || return 1
    rados_get $dir $poolname $objname || return 1

    wait_for_clean
    COUNT=$(ceph pg $pgid query | jq '.info.stats.stat_sum.num_objects_repaired')
    test "$COUNT" = "1" || return 1
    flush_pg_stats
    COUNT=$(ceph pg dump --format=json-pretty | jq ".pg_map.osd_stats_sum.num_shards_repaired")
    test "$COUNT" = "1" || return 1

    local object_osds=($(get_osds $poolname $objname))
    local primary=${object_osds[0]}
    local bad_peer=${object_osds[1]}
    inject_$inject rep data $poolname $objname $dir 0 || return 1
    inject_$inject rep data $poolname $objname $dir 1 || return 1
    # Force primary to pull from the bad peer, so we can repair it too!
    set_config osd $primary osd_debug_feed_pullee $bad_peer || return 1
    rados_get $dir $poolname $objname || return 1

    # Wait until automatic repair of bad peer is done
    wait_for_clean || return 1

    inject_$inject rep data $poolname $objname $dir 0 || return 1
    inject_$inject rep data $poolname $objname $dir 2 || return 1
    rados_get $dir $poolname $objname || return 1

    wait_for_clean
    COUNT=$(ceph pg $pgid query | jq '.info.stats.stat_sum.num_objects_repaired')
    test "$COUNT" = "3" || return 1
    flush_pg_stats
    COUNT=$(ceph pg dump --format=json-pretty | jq ".pg_map.osd_stats_sum.num_shards_repaired")
    test "$COUNT" = "4" || return 1

    inject_$inject rep data $poolname $objname $dir 0 || return 1
    inject_$inject rep data $poolname $objname $dir 1 || return 1
    inject_$inject rep data $poolname $objname $dir 2 || return 1
    rados_get $dir $poolname $objname hang || return 1

    wait_for_clean
    # After hang another repair couldn't happen, so count stays the same
    COUNT=$(ceph pg $pgid query | jq '.info.stats.stat_sum.num_objects_repaired')
    test "$COUNT" = "3" || return 1
    flush_pg_stats
    COUNT=$(ceph pg dump --format=json-pretty | jq ".pg_map.osd_stats_sum.num_shards_repaired")
    test "$COUNT" = "4" || return 1
}

function TEST_rados_get_with_eio() {
    local dir=$1

    setup_osds 4 || return 1

    local poolname=pool-rep
    create_pool $poolname 1 1 || return 1
    wait_for_clean || return 1
    rados_get_data eio $dir || return 1

    delete_pool $poolname
}

function TEST_rados_repair_warning() {
    local dir=$1
    local OBJS=$(expr $warnings + 1)

    setup_osds 4 || return 1

    local poolname=pool-rep
    create_pool $poolname 1 1 || return 1
    wait_for_clean || return 1

    local poolname=pool-rep
    local objbase=obj-warn
    local inject=eio

   for i in $(seq 1 $OBJS)
    do
      rados_put $dir $poolname ${objbase}-$i || return 1
      inject_$inject rep data $poolname ${objbase}-$i $dir 0 || return 1
      rados_get $dir $poolname ${objbase}-$i || return 1
    done
    local pgid=$(get_pg $poolname ${objbase}-1)

    local object_osds=($(get_osds $poolname ${objbase}-1))
    local primary=${object_osds[0]}
    local bad_peer=${object_osds[1]}

    wait_for_clean
    COUNT=$(ceph pg $pgid query | jq '.info.stats.stat_sum.num_objects_repaired')
    test "$COUNT" = "$OBJS" || return 1
    flush_pg_stats
    COUNT=$(ceph pg dump --format=json-pretty | jq ".pg_map.osd_stats_sum.num_shards_repaired")
    test "$COUNT" = "$OBJS" || return 1

    ceph health | grep -q "Too many repaired reads on 1 OSDs" || return 1
    ceph health detail | grep -q "osd.$primary had $OBJS reads repaired" || return 1

    ceph health mute OSD_TOO_MANY_REPAIRS
    set -o pipefail
    # Should mute this
    ceph health | $(! grep -q "Too many repaired reads on 1 OSDs") || return 1
    set +o pipefail

    for i in $(seq 1 $OBJS)
     do
       inject_$inject rep data $poolname ${objbase}-$i $dir 0 || return 1
       inject_$inject rep data $poolname ${objbase}-$i $dir 1 || return 1
       # Force primary to pull from the bad peer, so we can repair it too!
       set_config osd $primary osd_debug_feed_pullee $bad_peer || return 1
       rados_get $dir $poolname ${objbase}-$i || return 1
    done

    wait_for_clean
    COUNT=$(ceph pg $pgid query | jq '.info.stats.stat_sum.num_objects_repaired')
    test "$COUNT" = "$(expr $OBJS \* 2)" || return 1
    flush_pg_stats
    COUNT=$(ceph pg dump --format=json-pretty | jq ".pg_map.osd_stats_sum.num_shards_repaired")
    test "$COUNT" = "$(expr $OBJS \* 3)" || return 1

    # Give mon a chance to notice additional OSD and unmute
    # The default tick time is 5 seconds
    CHECKTIME=10
    LOOPS=0
    while(true)
    do
      sleep 1
      if ceph health | grep -q "Too many repaired reads on 2 OSDs"
      then
	      break
      fi
      LOOPS=$(expr $LOOPS + 1)
      if test "$LOOPS" = "$CHECKTIME"
      then
	      echo "Too many repaired reads not seen after $CHECKTIME seconds"
	      return 1
      fi
    done
    ceph health detail | grep -q "osd.$primary had $(expr $OBJS \* 2) reads repaired" || return 1
    ceph health detail | grep -q "osd.$bad_peer had $OBJS reads repaired" || return 1

    delete_pool $poolname
}

# Test backfill with unfound object
function TEST_rep_backfill_unfound() {
    local dir=$1
    local objname=myobject
    local lastobj=300
    # Must be between 1 and $lastobj
    local testobj=obj250

    export CEPH_ARGS
    CEPH_ARGS+=' --osd_min_pg_log_entries=5 --osd_max_pg_log_entries=10'
    setup_osds 3 || return 1

    local poolname=test-pool
    create_pool $poolname 1 1 || return 1
    wait_for_clean || return 1

    ceph pg dump pgs

    rados_put $dir $poolname $objname || return 1

    local -a initial_osds=($(get_osds $poolname $objname))
    local last_osd=${initial_osds[-1]}
    kill_daemons $dir TERM osd.${last_osd} 2>&2 < /dev/null || return 1
    ceph osd down ${last_osd} || return 1
    ceph osd out ${last_osd} || return 1

    ceph pg dump pgs

    dd if=/dev/urandom of=${dir}/ORIGINAL bs=1024 count=4
    for i in $(seq 1 $lastobj)
    do
      rados --pool $poolname put obj${i} $dir/ORIGINAL || return 1
    done

    inject_eio rep data $poolname $testobj $dir 0 || return 1
    inject_eio rep data $poolname $testobj $dir 1 || return 1

    activate_osd $dir ${last_osd} || return 1
    ceph osd in ${last_osd} || return 1

    sleep 15

    for tmp in $(seq 1 360); do
      state=$(get_state 2.0)
      echo $state | grep backfill_unfound
      if [ "$?" = "0" ]; then
        break
      fi
      echo "$state "
      sleep 1
    done

    ceph pg dump pgs
    ceph pg 2.0 list_unfound | grep -q $testobj || return 1

    # Command should hang because object is unfound
    timeout 5 rados -p $poolname get $testobj $dir/CHECK
    test $? = "124" || return 1

    ceph pg 2.0 mark_unfound_lost delete

    wait_for_clean || return 1

    for i in $(seq 1 $lastobj)
    do
      if [ obj${i} = "$testobj" ]; then
        # Doesn't exist anymore
        ! rados -p $poolname get $testobj $dir/CHECK || return 1
      else
        rados --pool $poolname get obj${i} $dir/CHECK || return 1
        diff -q $dir/ORIGINAL $dir/CHECK || return 1
      fi
    done

    rm -f ${dir}/ORIGINAL ${dir}/CHECK

    delete_pool $poolname
}

# Test recovery with unfound object
function TEST_rep_recovery_unfound() {
    local dir=$1
    local objname=myobject
    local lastobj=100
    # Must be between 1 and $lastobj
    local testobj=obj75

    setup_osds 3 || return 1

    local poolname=test-pool
    create_pool $poolname 1 1 || return 1
    wait_for_clean || return 1

    ceph pg dump pgs

    rados_put $dir $poolname $objname || return 1

    local -a initial_osds=($(get_osds $poolname $objname))
    local last_osd=${initial_osds[-1]}
    kill_daemons $dir TERM osd.${last_osd} 2>&2 < /dev/null || return 1
    ceph osd down ${last_osd} || return 1
    ceph osd out ${last_osd} || return 1

    ceph pg dump pgs

    dd if=/dev/urandom of=${dir}/ORIGINAL bs=1024 count=4
    for i in $(seq 1 $lastobj)
    do
      rados --pool $poolname put obj${i} $dir/ORIGINAL || return 1
    done

    inject_eio rep data $poolname $testobj $dir 0 || return 1
    inject_eio rep data $poolname $testobj $dir 1 || return 1

    activate_osd $dir ${last_osd} || return 1
    ceph osd in ${last_osd} || return 1

    sleep 15

    for tmp in $(seq 1 100); do
      state=$(get_state 2.0)
      echo $state | grep -v recovering
      if [ "$?" = "0" ]; then
        break
      fi
      echo "$state "
      sleep 1
    done

    ceph pg dump pgs
    ceph pg 2.0 list_unfound | grep -q $testobj || return 1

    # Command should hang because object is unfound
    timeout 5 rados -p $poolname get $testobj $dir/CHECK
    test $? = "124" || return 1

    ceph pg 2.0 mark_unfound_lost delete

    wait_for_clean || return 1

    for i in $(seq 1 $lastobj)
    do
      if [ obj${i} = "$testobj" ]; then
        # Doesn't exist anymore
        ! rados -p $poolname get $testobj $dir/CHECK || return 1
      else
        rados --pool $poolname get obj${i} $dir/CHECK || return 1
        diff -q $dir/ORIGINAL $dir/CHECK || return 1
      fi
    done

    rm -f ${dir}/ORIGINAL ${dir}/CHECK

    delete_pool $poolname
}

main osd-rep-recov-eio.sh "$@"

# Local Variables:
# compile-command: "cd ../../../build ; make -j4 && ../qa/run-standalone.sh osd-rep-recov-eio.sh"
# End:
