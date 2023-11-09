#!/usr/bin/env bash
# -*- mode:text; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
# vim: ts=8 sw=2 smarttab
#
# test the handling of a corrupted SnapMapper DB by Scrub

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/scrub/scrub-helpers.sh

function run() {
  local dir=$1
  shift

  export CEPH_MON="127.0.0.1:7144" # git grep '\<7144\>' : there must be only one
  export CEPH_ARGS
  CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
  CEPH_ARGS+="--mon-host=$CEPH_MON "

  export -n CEPH_CLI_TEST_DUP_COMMAND
  local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
  for func in $funcs ; do
    setup $dir || return 1
    $func $dir || return 1
    teardown $dir || return 1
  done
}

# one clone & multiple snaps (according to the number of parameters)
function make_a_clone()
{
  #turn off '-x' (but remember previous state)
  local saved_echo_flag=${-//[^x]/}
  set +x
  local pool=$1
  local obj=$2
  echo $RANDOM | rados -p $pool put $obj - || return 1
  shift 2
  for snap in $@ ; do
    rados -p $pool mksnap $snap || return 1
  done
  if [[ -n "$saved_echo_flag" ]]; then set -x; fi
}

function TEST_truncated_sna_record() {
    local dir=$1
    local -A cluster_conf=(
        ['osds_num']="3" 
        ['pgs_in_pool']="4"
        ['pool_name']="test"
    )

    local extr_dbg=3
    (( extr_dbg > 1 )) && echo "Dir: $dir"
    standard_scrub_cluster $dir cluster_conf
    ceph tell osd.* config set osd_stats_update_period_not_scrubbing "1"
    ceph tell osd.* config set osd_stats_update_period_scrubbing "1"

    local osdn=${cluster_conf['osds_num']}
    local poolid=${cluster_conf['pool_id']}
    local poolname=${cluster_conf['pool_name']}
    local objname="objxxx"

    # create an object and clone it
    make_a_clone $poolname $objname snap01 snap02 || return 1
    make_a_clone $poolname $objname snap13 || return 1
    make_a_clone $poolname $objname snap24 snap25 || return 1
    echo $RANDOM | rados -p $poolname put $objname - || return 1

    #identify the PG and the primary OSD
    local pgid=`ceph --format=json-pretty osd map $poolname $objname | jq -r '.pgid'`
    local osd=`ceph --format=json-pretty osd map $poolname $objname | jq -r '.up[0]'`
    echo "pgid is $pgid (primary: osd.$osd)"
    # turn on the publishing of test data in the 'scrubber' section of 'pg query' output
    set_query_debug $pgid

    # verify the existence of these clones
    (( extr_dbg >= 1 )) && rados --format json-pretty -p $poolname listsnaps $objname

    # scrub the PG
    ceph pg $pgid deep-scrub || return 1

    # we aren't just waiting for the scrub to terminate, but also for the
    # logs to be published
    sleep 3
    ceph pg dump pgs
    until grep -a -q -- "event: --^^^^---- ScrubFinished" $dir/osd.$osd.log ; do
        sleep 0.2
    done

    ceph pg dump pgs
    ceph osd set noscrub || return 1
    ceph osd set nodeep-scrub || return 1
    sleep 5
    grep -a -q -v "ERR" $dir/osd.$osd.log || return 1

    # kill the OSDs
    kill_daemons $dir TERM osd || return 1

    (( extr_dbg >= 2 )) && ceph-kvstore-tool bluestore-kv $dir/0 dump "p"
    (( extr_dbg >= 2 )) && ceph-kvstore-tool bluestore-kv $dir/2 dump "p" | grep -a SNA_
    (( extr_dbg >= 2 )) && grep -a SNA_ /tmp/oo2.dump
    (( extr_dbg >= 2 )) && ceph-kvstore-tool bluestore-kv $dir/2 dump p 2> /dev/null
    local num_sna_b4=`ceph-kvstore-tool bluestore-kv $dir/$osd dump p 2> /dev/null | grep -a -e 'SNA_[0-9]_000000000000000[0-9]_000000000000000' \
            | awk -e '{print $2;}' | wc -l`

    for sdn in $(seq 0 $(expr $osdn - 1))
    do
        kvdir=$dir/$sdn
        echo "corrupting the SnapMapper DB of osd.$sdn (db: $kvdir)"
        (( extr_dbg >= 3 )) && ceph-kvstore-tool bluestore-kv $kvdir dump "p"

        # truncate the 'mapping' (SNA_) entry corresponding to the snap13 clone
        KY=`ceph-kvstore-tool bluestore-kv $kvdir dump p 2> /dev/null | grep -a -e 'SNA_[0-9]_0000000000000003_000000000000000' \
            | awk -e '{print $2;}'`
        (( extr_dbg >= 1 )) && echo "SNA key: $KY" | cat -v

        tmp_fn1=`mktemp -p /tmp --suffix="_the_val"`
        (( extr_dbg >= 1 )) && echo "Value dumped in: $tmp_fn1"
        ceph-kvstore-tool bluestore-kv $kvdir get p "$KY" out $tmp_fn1 2> /dev/null
        (( extr_dbg >= 2 )) && od -xc $tmp_fn1

        NKY=${KY:0:-30}
        ceph-kvstore-tool bluestore-kv $kvdir rm "p" "$KY" 2> /dev/null
        ceph-kvstore-tool bluestore-kv $kvdir set "p" "$NKY" in $tmp_fn1 2> /dev/null

        (( extr_dbg >= 1 )) || rm $tmp_fn1
    done

    orig_osd_args=" ${cluster_conf['osd_args']}"
    orig_osd_args=" $(echo $orig_osd_args)"
    (( extr_dbg >= 2 )) && echo "Copied OSD args: /$orig_osd_args/ /${orig_osd_args:1}/"
    for sdn in $(seq 0 $(expr $osdn - 1))
    do
      CEPH_ARGS="$CEPH_ARGS $orig_osd_args" activate_osd $dir $sdn
    done
    sleep 1

    for sdn in $(seq 0 $(expr $osdn - 1))
    do
      timeout 60 ceph tell osd.$sdn version
    done
    rados --format json-pretty -p $poolname listsnaps $objname

    # when scrubbing now - we expect the scrub to emit a cluster log ERR message regarding SnapMapper internal inconsistency
    ceph osd unset nodeep-scrub || return 1
    ceph osd unset noscrub || return 1

    # what is the primary now?
    local cur_prim=`ceph --format=json-pretty osd map $poolname $objname | jq -r '.up[0]'`
    ceph pg dump pgs
    sleep 2
    ceph pg $pgid deep-scrub || return 1
    sleep 5
    ceph pg dump pgs
    (( extr_dbg >= 1 )) && grep -a "ERR" $dir/osd.$cur_prim.log
    grep -a -q "ERR" $dir/osd.$cur_prim.log || return 1

    # but did we fix the snap issue? let's try scrubbing again

    local prev_err_cnt=`grep -a "ERR" $dir/osd.$cur_prim.log | wc -l`
    echo "prev count: $prev_err_cnt"

    # scrub again. No errors expected this time
    ceph pg $pgid deep-scrub || return 1
    sleep 5
    ceph pg dump pgs
    (( extr_dbg >= 1 )) && grep -a "ERR" $dir/osd.$cur_prim.log
    local current_err_cnt=`grep -a "ERR" $dir/osd.$cur_prim.log | wc -l`
    (( extr_dbg >= 1 )) && echo "current count: $current_err_cnt"
    (( current_err_cnt == prev_err_cnt )) || return 1
    kill_daemons $dir TERM osd || return 1
    kvdir=$dir/$cur_prim
    (( extr_dbg >= 2 )) && ceph-kvstore-tool bluestore-kv $kvdir dump p 2> /dev/null | grep -a -e 'SNA_[0-9]_' \
            | awk -e '{print $2;}'
    local num_sna_full=`ceph-kvstore-tool bluestore-kv $kvdir dump p 2> /dev/null | grep -a -e 'SNA_[0-9]_000000000000000[0-9]_000000000000000' \
            | awk -e '{print $2;}' | wc -l`
    (( num_sna_full == num_sna_b4 )) || return 1
    return 0
}


main osd-mapper "$@"
