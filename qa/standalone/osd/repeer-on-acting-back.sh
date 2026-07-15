#!/usr/bin/env bash
#
# Copyright (C) 2020  ZTE Corporation <contact@zte.com.cn>
#
# Author: xie xingguo <xie.xingguo@zte.com.cn>
# Author: Yan Jun <yan.jun8@zte.com.cn>
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

    export poolname=test
    export testobjects=100
    export loglen=12
    export trim=$(expr $loglen / 2)
    export CEPH_MON="127.0.0.1:7115" # git grep '\<7115\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    # so we will not force auth_log_shard to be acting_primary
    CEPH_ARGS+="--osd_force_auth_primary_missing_objects=1000000 "
    # use small pg_log settings, so we always do backfill instead of recovery
    CEPH_ARGS+="--osd_min_pg_log_entries=$loglen --osd_max_pg_log_entries=$loglen --osd_pg_log_trim_min=$trim "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}


function TEST_repeer_on_down_acting_member_coming_back() {
    local dir=$1

    local num_osds=6
    local osds="$(seq 0 $(expr $num_osds - 1))"
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for i in $osds
    do
      run_osd $dir $i || return 1
    done

    create_pool $poolname 1 1
    ceph osd pool set $poolname size 3
    ceph osd pool set $poolname min_size 2
    local poolid=$(ceph pg dump pools -f json | jq '.pool_stats' | jq '.[].poolid')
    local pgid=$poolid.0

    # enable required feature-bits for upmap
    ceph osd set-require-min-compat-client luminous
    # reset up to [1,2,3]
    ceph osd pg-upmap $pgid 1 2 3 || return 1

    flush_pg_stats || return 1
    wait_for_clean || return 1

    echo "writing initial objects"
    local dummyfile=$(file_with_random_data)
    # write a bunch of objects
    for i in $(seq 1 $testobjects)
    do
      rados -p $poolname put existing_$i $dummyfile || return 1
    done
    rm -f $dummyfile

    WAIT_FOR_CLEAN_TIMEOUT=20 wait_for_clean

    # reset up to [1,4,5]
    ceph osd pg-upmap $pgid 1 4 5 || return 1

    timeout=2
    start=$(date +%s)

    while true; do
      if ceph pg "$pgid" query | jq '.acting' | grep -qw 2; then
        echo "OSD.2 found in acting set"
        break
      fi
      now=$(date +%s)
      if [ $((now - start)) -ge $timeout ]; then
        echo "Timed out waiting for OSD.2 in acting set"
        return 1
      fi
      sleep 0.1
    done

    # kill osd.2
    kill_daemons $dir KILL osd.2 || return 1
    ceph osd down osd.2

    # again, wait for peering to complete
    sleep 2

    # osd.2 should have been moved out from acting set
    ceph pg $pgid query | jq '.acting' | grep 2 && return 1

    # bring up osd.2
    activate_osd $dir 2 || return 1
    wait_for_osd up 2

    # again, wait for peering to complete
    sleep 2

    pgjson=$(ceph pg $pgid query)
    state=$(echo "$pgjson" | jq -r '.state')

    if [[ "$state" == *"recovery"* ]]; then
      # recovery in progress → osd.2 must be in acting
      if echo "$pgjson" | jq '.acting' | grep 2; then
        echo "Recovery in progress and OSD.2 is in acting set"
        return 0
      else
        echo "Recovery in progress but OSD.2 is NOT in acting set"
        return 1
      fi
    else
      echo "Recovery finished (no need for OSD.2 in acting)"
      return 0
    fi

    WAIT_FOR_CLEAN_TIMEOUT=20 wait_for_clean

    if ! grep -Eiq "requesting pg[_ ]temp change" $(find $dir -name '*osd*log')
    then
            echo failure
            return 1
    fi
    echo "success"

    delete_pool $poolname
    kill_daemons $dir || return 1
}

function TEST_ceph_pg_repeer_on_simple_ec_and_rep_pools() {
  local dir=$1
  local pools=1
  local OSDS=3

  run_mon $dir a || return 1
  run_mgr $dir x || return 1
  export CEPH_ARGS

  for osd in $(seq 0 $(expr $OSDS - 1))
  do
    run_osd $dir $osd || return 1
  done

  #setup erasure coded pool
  EC_POOL_NAME="ecpool"
  METADATA_POOL_NAME="ecpool_metadata"

  ceph osd erasure-code-profile set ec-prof plugin=isa k=2 m=1 crush-failure-domain=osd
  
  ceph osd pool create $EC_POOL_NAME erasure ec-prof || return 1
  ceph osd pool set $EC_POOL_NAME allow_ec_overwrites true || return 1
  ceph osd pool application enable $EC_POOL_NAME rbd || return 1

  #create a metadata pool for the ec pool
  ceph osd pool create $METADATA_POOL_NAME replicated || return 1
  rbd pool init $METADATA_POOL_NAME || return 1

  #create an rbd image on the ec pool
  rbd create -s 10G --data-pool $EC_POOL_NAME $METADATA_POOL_NAME/vol0 || return 1
  
  #create a replicated pool now
  REP_POOL_NAME="reppool"

  ceph osd pool create $REP_POOL_NAME replicated || return 1
  rbd pool init $REP_POOL_NAME || return 1

  #create an rbd image on the replicated pool
  rbd create -s 10G $REP_POOL_NAME/vol0 || return 1

  # Perform some I/O on both images
  rbd bench -p $REP_POOL_NAME --image vol0 --io-size 1K --io-threads 1 --io-total 10000K --io-pattern rand --io-type readwrite || return 1
  rbd bench -p $METADATA_POOL_NAME --image vol0 --io-size 1K --io-threads 1 --io-total 10000K --io-pattern rand --io-type readwrite || return 1

  #Grab a list of all the pgs in the ec pool and the rep pool
  ec_pgs=$(ceph pg ls-by-pool $EC_POOL_NAME | grep [0-9]. | awk '{print $1}')
  rep_pgs=$(ceph pg ls-by-pool $REP_POOL_NAME | grep [0-9]. | awk '{print $1}')

  ec_pgs_list=( $ec_pgs )
  rep_pgs_list=( $rep_pgs )

  echo ${#ec_pgs_list[@]}
  echo ${ec_pgs_list[@]}

  echo ${#rep_pgs_list[@]}
  echo ${rep_pgs_list[@]}

  #Now pick 2 pgs from each pool at random and then cause them to repeer
  ceph pg repeer ${ec_pgs_list[ $RANDOM % ${#ec_pgs_list[@]} ]} || return 1
  ceph pg repeer ${ec_pgs_list[ $RANDOM % ${#ec_pgs_list[@]} ]} || return 1
  ceph pg repeer ${rep_pgs_list[ $RANDOM % ${#rep_pgs_list[@]} ]} || return 1
  ceph pg repeer ${rep_pgs_list[ $RANDOM % ${#rep_pgs_list[@]} ]} || return 1

}

main repeer-on-acting-back "$@"

# Local Variables:
# compile-command: "make -j4 && ../qa/run-standalone.sh repeer-on-acting-back.sh"
# End:
