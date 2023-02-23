#!/usr/bin/env bash
#
# Copyright (C) 2013, 2014 Cloudwatt <libre.licensing@cloudwatt.com>
# Copyright (C) 2014, 2015 Red Hat <contact@redhat.com>
#
# Author: Loic Dachary <loic@dachary.org>
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

    export CEPH_MON="127.0.0.1:7105" # git grep '\<7105\>' : there must be only one
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    export CEPH_ARGS

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

# Before http://tracker.ceph.com/issues/8307 the invalid profile was created
function TEST_erasure_invalid_profile() {
    local dir=$1
    run_mon $dir a || return 1
    local poolname=pool_erasure
    local notaprofile=not-a-valid-erasure-code-profile
    ! ceph osd pool create $poolname 12 12 erasure $notaprofile || return 1
    ! ceph osd erasure-code-profile ls | grep $notaprofile || return 1
}

function TEST_erasure_crush_rule() {
    local dir=$1
    run_mon $dir a || return 1
    #
    # choose the crush rule used with an erasure coded pool
    #
    local crush_rule=myrule
    ! ceph osd crush rule ls | grep $crush_rule || return 1
    ceph osd crush rule create-erasure $crush_rule
    ceph osd crush rule ls | grep $crush_rule
    local poolname
    poolname=pool_erasure1
    ! ceph --format json osd dump | grep '"crush_rule":1' || return 1
    ceph osd pool create $poolname 12 12 erasure default $crush_rule
    ceph --format json osd dump | grep '"crush_rule":1' || return 1
    #
    # a crush rule by the name of the pool is implicitly created
    #
    poolname=pool_erasure2
    ceph osd erasure-code-profile set myprofile
    ceph osd pool create $poolname 12 12 erasure myprofile
    ceph osd crush rule ls | grep $poolname || return 1
    #
    # a non existent crush rule given in argument is an error
    # http://tracker.ceph.com/issues/9304
    #
    poolname=pool_erasure3
    ! ceph osd pool create $poolname 12 12 erasure myprofile INVALIDRULE || return 1
}

function TEST_erasure_code_profile_default() {
    local dir=$1
    run_mon $dir a || return 1
    ceph osd erasure-code-profile rm default || return 1
    ! ceph osd erasure-code-profile ls | grep default || return 1
    ceph osd pool create $poolname 12 12 erasure default
    ceph osd erasure-code-profile ls | grep default || return 1
}

function TEST_erasure_crush_stripe_unit() {
    local dir=$1
    # the default stripe unit is used to initialize the pool
    run_mon $dir a --public-addr $CEPH_MON
    stripe_unit=$(ceph-conf --show-config-value osd_pool_erasure_code_stripe_unit)
    eval local $(ceph osd erasure-code-profile get myprofile | grep k=)
    stripe_width = $((stripe_unit * k))
    ceph osd pool create pool_erasure 12 12 erasure
    ceph --format json osd dump | tee $dir/osd.json
    grep '"stripe_width":'$stripe_width $dir/osd.json > /dev/null || return 1
}

function TEST_erasure_crush_stripe_unit_padded() {
    local dir=$1
    # setting osd_pool_erasure_code_stripe_unit modifies the stripe_width
    # and it is padded as required by the default plugin
    profile+=" plugin=jerasure"
    profile+=" technique=reed_sol_van"
    k=4
    profile+=" k=$k"
    profile+=" m=2"
    actual_stripe_unit=2048
    desired_stripe_unit=$((actual_stripe_unit - 1))
    actual_stripe_width=$((actual_stripe_unit * k))
    run_mon $dir a \
        --osd_pool_erasure_code_stripe_unit $desired_stripe_unit \
        --osd_pool_default_erasure_code_profile "$profile" || return 1
    ceph osd pool create pool_erasure 12 12 erasure
    ceph osd dump | tee $dir/osd.json
    grep "stripe_width $actual_stripe_width" $dir/osd.json > /dev/null || return 1
}

function TEST_erasure_code_pool() {
    local dir=$1
    run_mon $dir a || return 1
    ceph --format json osd dump > $dir/osd.json
    local expected='"erasure_code_profile":"default"'
    ! grep "$expected" $dir/osd.json || return 1
    ceph osd pool create erasurecodes 12 12 erasure
    ceph --format json osd dump | tee $dir/osd.json
    grep "$expected" $dir/osd.json > /dev/null || return 1

    ceph osd pool create erasurecodes 12 12 erasure 2>&1 | \
        grep 'already exists' || return 1
    ceph osd pool create erasurecodes 12 12 2>&1 | \
        grep 'cannot change to type replicated' || return 1
}

function TEST_replicated_pool_with_rule() {
    local dir=$1
    run_mon $dir a
    local rule=rule0
    local root=host1
    ceph osd crush add-bucket $root host
    local failure_domain=osd
    local poolname=mypool
    ceph osd crush rule create-simple $rule $root $failure_domain || return 1
    ceph osd crush rule ls | grep $rule
    ceph osd pool create $poolname 12 12 replicated $rule || return 1
    rule_id=`ceph osd crush rule dump $rule | grep "rule_id" | awk -F[' ':,] '{print $4}'`
    ceph osd pool get $poolname crush_rule  2>&1 | \
        grep "crush_rule: $rule_id" || return 1
    #non-existent crush rule
    ceph osd pool create newpool 12 12 replicated non-existent 2>&1 | \
        grep "doesn't exist" || return 1
}

function TEST_erasure_code_pool_lrc() {
    local dir=$1
    run_mon $dir a || return 1

    ceph osd erasure-code-profile set LRCprofile \
             plugin=lrc \
             mapping=DD_ \
             layers='[ [ "DDc", "" ] ]' || return 1

    ceph --format json osd dump > $dir/osd.json
    local expected='"erasure_code_profile":"LRCprofile"'
    local poolname=erasurecodes
    ! grep "$expected" $dir/osd.json || return 1
    ceph osd pool create $poolname 12 12 erasure LRCprofile
    ceph --format json osd dump | tee $dir/osd.json
    grep "$expected" $dir/osd.json > /dev/null || return 1
    ceph osd crush rule ls | grep $poolname || return 1
}

function TEST_replicated_pool() {
    local dir=$1
    run_mon $dir a || return 1
    ceph osd pool create replicated 12 12 replicated replicated_rule || return 1
    ceph osd pool create replicated 12 12 replicated replicated_rule 2>&1 | \
        grep 'already exists' || return 1
    # default is replicated
    ceph osd pool create replicated1 12 12 || return 1
    # default is replicated, pgp_num = pg_num
    ceph osd pool create replicated2 12 || return 1
    ceph osd pool create replicated 12 12 erasure 2>&1 | \
        grep 'cannot change to type erasure' || return 1
}

function TEST_no_pool_delete() {
    local dir=$1
    run_mon $dir a || return 1
    ceph osd pool create foo 1 || return 1
    ceph tell mon.a injectargs -- --no-mon-allow-pool-delete || return 1
    ! ceph osd pool delete foo foo --yes-i-really-really-mean-it || return 1
    ceph tell mon.a injectargs -- --mon-allow-pool-delete || return 1
    ceph osd pool delete foo foo --yes-i-really-really-mean-it || return 1
}

function TEST_utf8_cli() {
    local dir=$1
    run_mon $dir a || return 1
    # Hopefully it's safe to include literal UTF-8 characters to test
    # the fix for http://tracker.ceph.com/issues/7387.  If it turns out
    # to not be OK (when is the default encoding *not* UTF-8?), maybe
    # the character '黄' can be replaced with the escape $'\xe9\xbb\x84'
    OLDLANG="$LANG"
    export LANG=en_US.UTF-8
    ceph osd pool create 黄 16 || return 1
    ceph osd lspools 2>&1 | \
        grep "黄" || return 1
    ceph -f json-pretty osd dump | \
        python3 -c "import json; import sys; json.load(sys.stdin)" || return 1
    ceph osd pool delete 黄 黄 --yes-i-really-really-mean-it
    export LANG="$OLDLANG"
}

function check_pool_priority() {
    local dir=$1
    shift
    local pools=$1
    shift
    local spread="$1"
    shift
    local results="$1"

    setup $dir || return 1

    EXTRA_OPTS="--debug_allow_any_pool_priority=true"
    export EXTRA_OPTS
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1

    # Add pool 0 too
    for i in $(seq 0 $pools)
    do
      num=$(expr $i + 1)
      ceph osd pool create test${num} 1 1
    done

    wait_for_clean || return 1
    for i in $(seq 0 $pools)
    do
	num=$(expr $i + 1)
	ceph osd pool set test${num} recovery_priority $(expr $i \* $spread)
    done

    #grep "recovery_priority.*pool set" out/mon.a.log

    bin/ceph osd dump

    # Restart everything so mon converts the priorities
    kill_daemons
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    activate_osd $dir 0 || return 1
    activate_osd $dir 1 || return 1
    activate_osd $dir 2 || return 1
    sleep 5

    grep convert $dir/mon.a.log
    ceph osd dump

    pos=1
    for i in $(ceph osd dump | grep ^pool | sed 's/.*recovery_priority //' | awk '{ print $1 }')
    do
      result=$(echo $results | awk "{ print \$${pos} }")
      # A value of 0 is an unset value so sed/awk gets "pool"
      if test $result = "0"
      then
        result="pool"
      fi
      test "$result" = "$i" || return 1
      pos=$(expr $pos + 1)
    done
}

function TEST_pool_pos_only_prio() {
   local dir=$1
   check_pool_priority $dir 20 5 "0 0 1 1 2 2 3 3 4 4 5 5 6 6 7 7 8 8 9 9 10" || return 1
}

function TEST_pool_neg_only_prio() {
   local dir=$1
   check_pool_priority $dir 20 -5 "0 0 -1 -1 -2 -2 -3 -3 -4 -4 -5 -5 -6 -6 -7 -7 -8 -8 -9 -9 -10" || return 1
}

function TEST_pool_both_prio() {
   local dir=$1
   check_pool_priority $dir 20 "5 - 50" "-10 -9 -8 -7 -6 -5 -4 -3 -2 -1 0 1 2 3 4 5 6 7 8 9 10" || return 1
}

function TEST_pool_both_prio_no_neg() {
   local dir=$1
   check_pool_priority $dir 20 "2 - 4" "-4 -2 0 0 1 1 2 2 3 3 4 5 5 6 6 7 7 8 8 9 10" || return 1
}

function TEST_pool_both_prio_no_pos() {
   local dir=$1
   check_pool_priority $dir 20 "2 - 36" "-10 -9 -8 -8 -7 -7 -6 -6 -5 -5 -4 -3 -3 -2 -2 -1 -1 0 0 2 4" || return 1
}


main osd-pool-create "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/mon/osd-pool-create.sh"
# End:
