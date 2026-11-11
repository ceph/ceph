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
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    export CEPH_ARGS

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

# osd pool get <pool> <option> is refused for an option that was never set
# on the pool, with a message saying so, and gives the value once it is
# set. This test gets compression_mode from replicated pool p before and
# after setting it to aggressive. It also expects num_zones 1 for p, which
# was created without --num_zones.
function TEST_pool_get_unset_option() {
    local dir=$1
    run_mon $dir a || return 1
    ceph osd pool create p 8 || return 1
    ! ceph osd pool get p compression_mode > $dir/out 2>&1 || return 1
    grep "option 'compression_mode' is not set on pool 'p'" $dir/out || return 1
    ceph osd pool set p compression_mode aggressive || return 1
    ceph osd pool get p compression_mode | grep aggressive || return 1
    ceph osd pool get p num_zones | grep 'num_zones: 1' || return 1
}

# A CRUSH rule is removed when its last pool leaves it, by being deleted or
# by osd pool set crush_rule, whether the rule was made by hand or
# generated for the pool. The default rule, replicated_rule, is kept. This
# test puts replicated pools p and p2 on op_rule, made by
# osd crush rule create-replicated: op_rule stays when p is deleted and
# goes with p2. Pool r is moved from r-ssd, also made by hand, to
# replicated_rule, and r-ssd goes. Deleting r leaves replicated_rule.
# Deleting erasure coded pool e removes the rule generated for it.
function TEST_pool_rule_removed_when_unused() {
    local dir=$1
    run_mon $dir a || return 1
    ceph osd crush rule create-replicated op_rule default host || return 1
    ceph osd pool create p 8 8 replicated op_rule || return 1
    ceph osd pool create p2 8 8 replicated op_rule || return 1
    # kept while another pool uses it
    ceph osd pool delete p p --yes-i-really-really-mean-it || return 1
    ceph osd crush rule ls | grep -qx op_rule || return 1
    # removed with its last pool, whoever created it
    ceph osd pool delete p2 p2 --yes-i-really-really-mean-it || return 1
    ! ceph osd crush rule ls | grep -qx op_rule || return 1
    # and when its last pool is given another rule
    ceph osd crush rule create-replicated r-ssd default host || return 1
    ceph osd pool create r 8 8 replicated r-ssd || return 1
    ceph osd pool set r crush_rule replicated_rule || return 1
    ! ceph osd crush rule ls | grep -qx r-ssd || return 1
    # the default rule stays
    ceph osd pool delete r r --yes-i-really-really-mean-it || return 1
    ceph osd crush rule ls | grep -qx replicated_rule || return 1
    # a rule generated for an erasure coded pool goes with the pool
    ceph osd pool create e 8 8 erasure || return 1
    local rule=$(ceph osd pool get e crush_rule | awk '{print $2}')
    ceph osd pool delete e e --yes-i-really-really-mean-it || return 1
    ! ceph osd crush rule ls | grep -qx "$rule" || return 1
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

# An osd pool create that fails removes the rule, and the erasure code
# profile, that it generated for the pool, but not a rule of the same name
# made by hand. This test creates badpool from lrcprofile (lrc plugin) with
# --crimson, which is refused as a crimson pool needs EC optimizations and
# lrc does not support them: no rule badpool is left. With rule badpool
# made by osd crush rule create-erasure, the same refused create leaves it.
# Erasure coded pool bigec, with no profile and 1024 PGs, is refused for
# exceeding the PG limit, and leaves no rule bigec and no profile bigec-*.
function TEST_failed_pool_create_removes_its_rule() {
    local dir=$1
    run_mon $dir a --enable_experimental_unrecoverable_data_corrupting_features=crimson || return 1
    ceph osd set-allow-crimson --yes-i-really-mean-it || return 1
    # crimson needs EC optimizations, which lrc does not support
    ceph osd erasure-code-profile set lrcprofile plugin=lrc k=2 m=1 l=3 || return 1
    ! ceph osd pool create badpool 12 12 erasure lrcprofile --crimson || return 1
    ! ceph osd crush rule ls | grep -qx badpool || return 1
    ceph osd crush rule create-erasure badpool lrcprofile || return 1
    ! ceph osd pool create badpool 12 12 erasure lrcprofile --crimson || return 1
    ceph osd crush rule ls | grep -qx badpool || return 1
    # the PG limit is checked with the rule
    ! ceph osd pool create bigec 1024 1024 erasure > $dir/out 2>&1 || return 1
    grep "exceeds the" $dir/out || return 1
    ! ceph osd crush rule ls | grep -qx bigec || return 1
    ! ceph osd erasure-code-profile ls | grep -q '^bigec-' || return 1
}

# osd pool create refuses an erasure coded pool whose stripe_unit is not a
# multiple of 4096 unless given --yes-i-really-mean-it, also when the
# stripe_unit comes from the default profile, and the refused create leaves
# nothing behind. This test sets the default profile to isa k=2 m=1 with
# stripe_unit=2048, which osd erasure-code-profile set accepts only with
# --force, and creates erasure coded pool e with no profile. That is
# refused, and leaves no pool e, profile e-k2-m1 or rule e.
function TEST_default_profile_odd_stripe_unit() {
    local dir=$1
    run_mon $dir a || return 1
    ceph osd erasure-code-profile set default plugin=isa k=2 m=1 \
        stripe_unit=2048 --force --yes-i-really-mean-it || return 1
    expect_failure $dir "stripe_unit should be a multiple of 4096 bytes" \
        ceph osd pool create e 8 8 erasure || return 1
    ! ceph osd pool ls | grep -qx e || return 1
    ! ceph osd erasure-code-profile ls | grep -qx e-k2-m1 || return 1
    ! ceph osd crush rule ls | grep -qx e || return 1
}

# With --yes-i-really-mean-it, osd pool create accepts an erasure coded pool
# whose stripe_unit is not a multiple of 4096. This test makes profile odd,
# isa k=2 m=1 with stripe_unit=2048 (set with --force), and creates pool
# named on it: refused without the flag, created with it. It then gives the
# default profile the same values and creates pool generated, with no
# profile, with the flag. Both pools get a stripe width of 4096: 2 data
# chunks of 2048.
function TEST_odd_stripe_unit_yes_i_really_mean_it() {
    local dir=$1
    run_mon $dir a || return 1
    ceph osd erasure-code-profile set odd plugin=isa k=2 m=1 \
        stripe_unit=2048 --force || return 1
    expect_failure $dir "only ceph osd pool create --yes-i-really-mean-it" \
        ceph osd pool create named 8 8 erasure odd || return 1
    ceph osd pool create named 8 8 erasure odd --yes-i-really-mean-it || return 1
    ceph osd erasure-code-profile set default plugin=isa k=2 m=1 \
        stripe_unit=2048 --force --yes-i-really-mean-it || return 1
    ceph osd pool create generated 8 8 erasure --yes-i-really-mean-it || return 1
    for pool in named generated; do
        test "$(ceph osd dump -f json |
            jq ".pools[] | select(.pool_name == \"$pool\") | .stripe_width")" = 4096 || return 1
    done
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
    profile+=" plugin=isa"
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
    ceph osd pool create erasurecodes 12 12 erasure
    # a copy of the default profile of the pool's own
    ceph osd pool get erasurecodes erasure_code_profile | \
        grep 'erasure_code_profile: erasurecodes-k2-m2$' || return 1
    test "$(ceph osd erasure-code-profile get erasurecodes-k2-m2 -f json | jq -r .plugin)" = \
        "$(ceph osd erasure-code-profile get default -f json | jq -r .plugin)" || return 1
    ceph --format json osd dump | tee $dir/osd.json
    ! grep '"erasure_code_profile":"default"' $dir/osd.json || return 1
    test "$(ceph osd pool get erasurecodes crush_rule -f json | jq -r .crush_rule)" = \
        erasurecodes || return 1

    ceph osd pool create erasurecodes 12 12 erasure 2>&1 | \
        grep 'already exists' || return 1
    ceph osd pool create erasurecodes 12 12 2>&1 | \
        grep 'cannot change to type replicated' || return 1
}

# osd pool create --pool_type erasure without a profile gives the pool a
# profile of its own, <pool>-k<k>-m<m>: the default profile with the --k,
# --m and CRUSH options given. This test creates km_crush with --k 2 --m 1,
# --zone_failure_domain rack and --osd_failure_domain osd, which differ
# from the default profile's, and expects profile km_crush-k2-m1 to have
# crush-zone-failure-domain=rack and crush-failure-domain=osd. The pool can
# then be deleted and created again with the same options.
function TEST_erasure_code_pool_km_crush_params() {
    local dir=$1
    run_mon $dir a || return 1
    # the default profile uses crush-failure-domain=host, so the crush
    # parameters given here differ from it and must end up in the
    # auto-created profile
    local poolname=km_crush
    local profile=$poolname-k2-m1
    ceph osd pool create $poolname --pool_type erasure --k 2 --m 1 \
        --pg_num 8 --pgp_num 8 \
        --zone_failure_domain rack --osd_failure_domain osd || return 1
    ceph osd erasure-code-profile get $profile | tee $dir/profile.txt
    grep '^crush-failure-domain=osd$' $dir/profile.txt || return 1
    grep '^crush-zone-failure-domain=rack$' $dir/profile.txt || return 1
    # the profile goes with the pool, and is made again for the new one
    ceph osd pool delete $poolname $poolname --yes-i-really-really-mean-it || return 1
    ceph osd pool create $poolname --pool_type erasure --k 2 --m 1 \
        --pg_num 8 --pgp_num 8 \
        --zone_failure_domain rack --osd_failure_domain osd || return 1
}

# osd pool create --pool_type erasure with CRUSH options but no --k or --m
# takes k and m from the default profile. This test creates crush_no_km
# with only --osd_failure_domain osd, and expects a profile of its own,
# crush_no_km-k2-m2, with crush-failure-domain=osd, and a rule of its own,
# crush_no_km.
function TEST_erasure_code_pool_crush_params_without_km() {
    local dir=$1
    run_mon $dir a || return 1
    # k and m come from the default profile, and the pool's own profile and
    # rule record the crush parameters
    ceph osd pool create crush_no_km --pool_type erasure \
        --pg_num 8 --pgp_num 8 --osd_failure_domain osd || return 1
    ceph osd erasure-code-profile get crush_no_km-k2-m2 | tee $dir/profile.txt
    grep '^crush-failure-domain=osd$' $dir/profile.txt || return 1
    test "$(ceph osd pool get crush_no_km crush_rule -f json | jq -r .crush_rule)" = \
        crush_no_km || return 1
}

# osd pool create for an erasure coded pool with no profile makes a profile
# for it, <pool>-k<k>-m<m>. If a profile of that name exists with other
# contents, the create is refused, and the message says to remove it or to
# name it with --erasure_code_profile. This test makes taken-k2-m2 by hand
# (isa k=2 m=2, failure domain osd) and creates pool taken: refused, and
# then created when given --erasure_code_profile taken-k2-m2.
function TEST_erasure_code_pool_profile_name_taken() {
    local dir=$1
    run_mon $dir a || return 1
    ceph osd erasure-code-profile set taken-k2-m2 plugin=isa k=2 m=2 \
        crush-failure-domain=osd || return 1
    ! ceph osd pool create taken --pool_type erasure --pg_num 8 \
        2> $dir/err.txt || return 1
    grep "EC profile 'taken-k2-m2' already exists with different contents; remove it with ceph osd erasure-code-profile rm taken-k2-m2, or use it with --erasure_code_profile taken-k2-m2" \
        $dir/err.txt || return 1
    ceph osd pool create taken --pool_type erasure --pg_num 8 \
        --erasure_code_profile taken-k2-m2 || return 1
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

# A create resent before the first one is committed replies once the pool
# exists
function TEST_replicated_pool_create_resent() {
    local dir=$1
    run_mon $dir a || return 1
    ceph tell mon.\* config set paxos_propose_interval 5 || return 1
    # an OSDMap change just committed delays the next proposal
    ceph osd set noout || return 1

    timeout 60 ceph osd pool create resent 4 4 replicated &
    local pid=$!
    sleep 1
    timeout 60 ceph osd pool create resent 4 4 replicated || return 1
    ceph osd pool get resent size || return 1
    wait $pid || return 1
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

function TEST_osd_pool_create_migrate_from() {
  local dir=$1

  setup $dir || return 1
  run_mon $dir a || return 1
  run_mgr $dir x || return 1
  for id in $(seq 0 2); do
    run_osd $dir $id || return 1
  done
  create_rbd_pool || return 1
  wait_for_clean || return 1

  SRC_POOL_NAME="ecpool"
  REPPOOL_NAME="metadatapool"

  #setup the cluster 
  #create an erasure code profile
  ceph osd erasure-code-profile set ec-prof plugin=isa k=2 m=1 crush-failure-domain=osd

  #create an erasure coded pool
  ceph osd pool create $SRC_POOL_NAME erasure ec-prof || return 1
  ceph osd pool set $SRC_POOL_NAME allow_ec_overwrites true || return 1
  #rbd pool init $SRC_POOL_NAME || return 1
  ceph osd pool application enable $SRC_POOL_NAME rbd || return 1

  #create a replicated pool
  ceph osd pool create $REPPOOL_NAME replicated || return 1
  rbd pool init $REPPOOL_NAME || return 1

  #create an rbd image with data pool set to the erasure coded pool
  rbd create -s 10G --data-pool $SRC_POOL_NAME $REPPOOL_NAME/vol0 || return 1

  #write some data to the image
  rbd bench -p $REPPOOL_NAME --image vol0 --io-size 1K --io-threads 1 --io-total 10000K --io-pattern rand --io-type write || return 1

  #pool migration is experimental and needs min compat client vampire
  ceph config set global enable_experimental_unrecoverable_data_corrupting_features poolmigration || return 1
  ceph osd set-require-min-compat-client vampire || return 1

  #disable autoscaling
  ceph osd pool set noautoscale || return 1

  #resize the ec pool to have small number of pgs
  ceph osd pool set $SRC_POOL_NAME pg_num 2 || return 1

  #Wait some of the PGs to be merged
  sleep 10  

  #migrate the pool, the new pool takes over the name and the old pool is
  #renamed to .migrate-1
  ceph osd pool migrate $SRC_POOL_NAME || return 1
  TGT_POOL_NAME=$SRC_POOL_NAME
  SRC_POOL_NAME=".migrate-1"
   
  #reenable autoscaling
  ceph osd pool unset noautoscale || return 1

  #check that changing the pg_num on either pool is blocked
  ceph osd pool set $SRC_POOL_NAME pg_num 10
  ceph osd pool set $TGT_POOL_NAME pg_num 10

  #Let the message reach the logs
  sleep 2
  
  grep "pool pg_num change is disabled; you must unset nopgchange flag for the pool first" $dir/mon.a.log || return 1

  #check that the daemonServer was blocked from changing the pg_num on the two
  #pools involved in the migration
  SRC_POOL_ID=$(ceph osd dump | grep " '$SRC_POOL_NAME' " | awk '{print $2}')
  TGT_POOL_ID=$(ceph osd dump | grep " '$TGT_POOL_NAME' " | awk '{print $2}')
  echo "Source pool id: $SRC_POOL_ID"
  echo "Target pool id: $TGT_POOL_ID"
  
  grep "Pool $SRC_POOL_ID has the flag NOPGCHANGE so will be skipped" $dir/mgr.x.log || return 1
  grep "Pool $TGT_POOL_ID has the flag NOPGCHANGE so will be skipped" $dir/mgr.x.log || return 1

  teardown $dir || return 1
}

# osd pool set <pool> crush_rule <rule> must not give a pool a rule that a
# pending update removes, as the rule's last pool has just left it: the
# pool would be on a rule that is gone. This test moves pool a, the only
# pool on race_rule, to replicated_rule, and one second later, while a
# paxos_propose_interval of 5 seconds keeps that move pending, gives pool b
# race_rule. b's rule exists afterwards: either b got race_rule and the
# rule was kept, or the change was refused as race_rule does not exist and
# b is still on replicated_rule.
function TEST_pool_set_rule_racing_its_removal() {
    local dir=$1

    run_mon $dir a || return 1
    ceph osd crush rule create-replicated race_rule default host || return 1
    ceph osd pool create a 8 8 replicated race_rule || return 1
    ceph osd pool create b 8 8 replicated || return 1
    ceph tell mon.\* config set paxos_propose_interval 5 || return 1
    # an OSDMap change just committed delays the next proposal
    ceph osd set noout || return 1

    timeout 60 ceph osd pool set a crush_rule replicated_rule &
    local pid=$!
    sleep 1
    timeout 60 ceph osd pool set b crush_rule race_rule > $dir/out 2>&1
    wait $pid || return 1
    cat $dir/out
    timeout 60 ceph osd pool get b crush_rule > $dir/rule || return 1
    local rule=$(awk '{print $2}' $dir/rule)
    ceph osd crush rule ls | grep -qx "$rule" || return 1
    if [ "$rule" = replicated_rule ]; then
        grep "does not exist" $dir/out || return 1
    fi
}

# osd crush rule create-simple <rule> succeeds without a change when the
# rule exists. While a pending update removes the rule with its last pool,
# it must wait for that update and create the rule again. This test deletes
# pool a, the only pool on race_rule, and one second later, while a
# paxos_propose_interval of 5 seconds keeps the deletion pending, runs
# osd crush rule create-simple race_rule again. race_rule exists
# afterwards.
function TEST_crush_rule_create_simple_racing_its_removal() {
    local dir=$1

    run_mon $dir a || return 1
    ceph osd crush rule create-simple race_rule default host || return 1
    ceph osd pool create a 8 8 replicated race_rule || return 1
    ceph tell mon.\* config set paxos_propose_interval 5 || return 1
    # an OSDMap change just committed delays the next proposal
    ceph osd set noout || return 1

    timeout 60 ceph osd pool rm a a --yes-i-really-really-mean-it &
    local pid=$!
    sleep 1
    timeout 60 ceph osd crush rule create-simple race_rule default host || return 1
    wait $pid || return 1
    ceph osd crush rule ls | grep -qx race_rule || return 1
}

# osd pool default set --rule <rule> sets the rule that osd pool create
# gives replicated pools by default. It must not take a rule that a pending
# update removes with its last pool, or replicated pools would have no
# default rule left. This test deletes pool a, the only pool on race_rule,
# and one second later, while a paxos_propose_interval of 5 seconds keeps
# the deletion pending, runs osd pool default set --rule race_rule. That is
# refused as race_rule does not exist, race_rule is gone, and pool p can
# then be created with the defaults.
function TEST_pool_default_rule_racing_its_removal() {
    local dir=$1

    run_mon $dir a || return 1
    ceph osd crush rule create-replicated race_rule default host || return 1
    ceph osd pool create a 8 8 replicated race_rule || return 1
    ceph tell mon.\* config set paxos_propose_interval 5 || return 1
    ceph osd set noout || return 1

    timeout 60 ceph osd pool rm a a --yes-i-really-really-mean-it &
    local pid=$!
    sleep 1
    ! timeout 60 ceph osd pool default set --rule race_rule > $dir/out 2>&1 || return 1
    wait $pid || return 1
    grep "does not exist" $dir/out || return 1
    ! ceph osd crush rule ls | grep -qx race_rule || return 1
    timeout 60 ceph osd pool create p 8 || return 1
}

# osd pool set <pool> num_zones checks the rule and the replicas it is
# given as osd pool create does. This test gives replicated pool r
# num_zones 2 with one_zone, a rule that does not place the pool in 2
# buckets of type datacenter, and with --replica 11, above the limit of 10:
# both are refused. Erasure coded pool e is given num_zones 2 with nosuch,
# a rule that does not exist: refused, and no rule nosuch is made.
function TEST_pool_set_num_zones_checks() {
    local dir=$1

    run_mon $dir a || return 1
    ceph osd crush rule create-replicated one_zone default host || return 1
    ceph osd pool create r 8 8 replicated || return 1
    ! ceph osd pool set r num_zones 2 --crush_rule one_zone > $dir/out 2>&1 || return 1
    grep "does not place the pool in 2 buckets of type datacenter" $dir/out || return 1
    ! ceph osd pool set r num_zones 2 --replica 11 > $dir/out 2>&1 || return 1
    grep "replica must be between 1 and 10" $dir/out || return 1

    ceph osd pool create e 8 8 erasure --k 2 --m 1 || return 1
    ceph osd pool set e allow_ec_optimizations true || return 1
    ! ceph osd pool set e num_zones 2 --crush_rule nosuch > $dir/out 2>&1 || return 1
    grep "specified rule nosuch doesn't exist" $dir/out || return 1
    ! ceph osd crush rule ls | grep -qx nosuch || return 1
}

# osd pool create --min_size for an erasure coded pool of the lrc plugin
# must be between k and the pool's size, which counts the local parity
# chunks lrc adds. This test uses profile lrcp, k=4 m=2 l=3, 8 chunks in
# all: --min_size 9 is refused, as not between 4 and 8, and --min_size 5
# is accepted.
function TEST_lrc_min_size() {
    local dir=$1

    run_mon $dir a || return 1
    ceph osd erasure-code-profile set lrcp plugin=lrc k=4 m=2 l=3 \
        crush-failure-domain=osd || return 1
    ! ceph osd pool create l 8 8 erasure lrcp --min_size 9 > $dir/out 2>&1 || return 1
    grep "pool min_size must be between 4 and 8" $dir/out || return 1
    ceph osd pool create l 8 8 erasure lrcp --min_size 5 || return 1
    test "$(ceph osd pool get l min_size | awk '{print $2}')" = 5 || return 1
}

# A pool deleted in the same update that moves it to another rule takes
# both rules with it when it was their only pool: the one it left and the
# one it was given. This test moves pool p from r1 to r2, and one second
# later, while a paxos_propose_interval of 5 seconds keeps the move
# pending, deletes p. Neither r1 nor r2 is left.
function TEST_pool_delete_after_rule_change() {
    local dir=$1

    run_mon $dir a || return 1
    ceph osd crush rule create-replicated r1 default host || return 1
    ceph osd crush rule create-replicated r2 default host || return 1
    ceph osd pool create p 8 8 replicated r1 || return 1
    ceph tell mon.\* config set paxos_propose_interval 5 || return 1
    ceph osd set noout || return 1

    timeout 60 ceph osd pool set p crush_rule r2 &
    local pid=$!
    sleep 1
    timeout 60 ceph osd pool rm p p --yes-i-really-really-mean-it || return 1
    wait $pid || return 1
    ! ceph osd crush rule ls | grep -qx r1 || return 1
    ! ceph osd crush rule ls | grep -qx r2 || return 1
}

function pool_field() {
  local pool=$1
  local field=$2
  ceph osd dump --format=json | jq -r ".pools[] | select(.pool_name == \"$pool\") | .$field"
}

function run_mon_for_pool_migration() {
    local dir=$1
    run_mon $dir a || return 1
    ceph config set global enable_experimental_unrecoverable_data_corrupting_features poolmigration || return 1
    ceph osd set-require-min-compat-client vampire || return 1
}

function rule_of() {
    ceph osd pool get $1 crush_rule -f json | jq -r .crush_rule
}

function profile_of() {
    ceph osd pool get $1 erasure_code_profile -f json | jq -r .erasure_code_profile
}

# the OSDs create the PGs of a pool only while it has the creating flag
function creating() {
    pool_field $1 flags_names | tr , '\n' | grep -qx creating
}

function TEST_pool_migrate_copies_pool() {
    local dir=$1
    run_mon_for_pool_migration $dir || return 1
    ceph osd erasure-code-profile set mprofile k=2 m=1 crush-failure-domain=osd || return 1
    ceph osd pool create mec 8 8 erasure mprofile || return 1
    ceph osd pool create mrep 4 4 replicated || return 1
    ceph osd pool set mrep size 2 || return 1

    # a migration with no options copies the pool, and must be confirmed
    expect_failure $dir "pointless migration" \
        ceph osd pool migrate mec || return 1
    ceph osd pool ls | grep -qx mec || return 1
    ! ceph osd pool ls --show-all | grep -q 'migrate-' || return 1

    ceph osd pool migrate mec --yes-i-really-mean-it || return 1
    test "$(pool_field mec type)" = 3 || return 1
    test "$(pool_field mec pg_num)" = 8 || return 1
    test "$(pool_field mec size)" = 3 || return 1
    test "$(pool_field mec num_zones)" = 1 || return 1
    test "$(pool_field .migrate-1 migration_target)" = "$(pool_field mec pool)" || return 1

    ceph osd pool migrate mrep --yes-i-really-mean-it || return 1
    test "$(pool_field mrep type)" = 1 || return 1
    test "$(pool_field mrep pg_num)" = 4 || return 1
    test "$(pool_field mrep size)" = 2 || return 1
}

function TEST_pool_migrate_takes_create_arguments() {
    local dir=$1
    run_mon_for_pool_migration $dir || return 1
    ceph osd pool create mec erasure --k 2 --m 1 --osd_failure_domain osd || return 1
    ceph osd pool create mrep 4 4 replicated || return 1

    ceph osd pool migrate mec --pool_type erasure --k 4 --m 2 --osd_failure_domain osd \
        --enable_ec_optimizations || return 1
    test "$(profile_of mec)" = mec-k4-m2 || return 1
    test "$(pool_field mec size)" = 6 || return 1

    # a migration that the checks of osd pool create refuse changes nothing
    expect_failure $dir "num_zones must be from 1 to" \
        ceph osd pool migrate mrep --num_zones 3 || return 1
    ceph osd pool create later 4 4 replicated || return 1
    ceph osd pool ls | grep -qx mrep || return 1
    test "$(ceph osd pool ls --show-all | grep -c 'migrate-')" = 1 || return 1
}

function TEST_pool_migrate_gives_target_own_rule_and_profile() {
    local dir=$1
    run_mon_for_pool_migration $dir || return 1
    ceph osd erasure-code-profile set pcopy k=2 m=1 crush-failure-domain=osd || return 1
    ceph osd pool create mcopy 4 4 erasure pcopy || return 1
    test "$(rule_of mcopy)" = mcopy || return 1
    local rule=$(pool_field mcopy crush_rule)
    local steps=$(ceph osd crush rule dump mcopy -f json | jq -c 'del(.rule_id, .rule_name)')

    # the source keeps its rule and profile under new names, and the target
    # has copies named after it
    ceph osd pool migrate mcopy --yes-i-really-mean-it || return 1
    creating mcopy || return 1
    test "$(pool_field .migrate-1 crush_rule)" = $rule || return 1
    test "$(rule_of .migrate-1)" = mcopy.migrate-1 || return 1
    test "$(profile_of .migrate-1)" = pcopy.migrate-1 || return 1
    test "$(pool_field mcopy crush_rule)" != $rule || return 1
    test "$(rule_of mcopy)" = mcopy || return 1
    test "$(profile_of mcopy)" = mcopy-k2-m1 || return 1
    test "$(ceph osd crush rule dump mcopy -f json | jq -c 'del(.rule_id, .rule_name)')" = "$steps" || return 1
    test "$(ceph osd erasure-code-profile get mcopy-k2-m1 -f json | jq -cS .)" = \
        "$(ceph osd erasure-code-profile get pcopy.migrate-1 -f json | jq -cS .)" || return 1
    ! ceph osd erasure-code-profile ls | grep -qx pcopy || return 1

    # a new profile needs a new rule
    ceph osd pool create mnew erasure --k 2 --m 1 --osd_failure_domain osd || return 1
    rule=$(pool_field mnew crush_rule)
    ceph osd pool migrate mnew --pool_type erasure --k 3 --m 1 --osd_failure_domain osd \
        --enable_ec_optimizations || return 1
    creating mnew || return 1
    test "$(rule_of mnew)" = mnew || return 1
    test "$(profile_of mnew)" = mnew-k3-m1 || return 1
    test "$(pool_field mnew crush_rule)" != $rule || return 1
    test "$(pool_field .migrate-2 crush_rule)" = $rule || return 1
    test "$(rule_of .migrate-2)" = mnew.migrate-2 || return 1
    test "$(profile_of .migrate-2)" = mnew-k2-m1.migrate-2 || return 1

    ceph osd crush rule create-replicated myrule default osd || return 1
    ceph osd pool create mrep 4 4 replicated myrule || return 1
    rule=$(pool_field mrep crush_rule)
    ceph osd pool migrate mrep --yes-i-really-mean-it || return 1
    creating mrep || return 1
    test "$(rule_of mrep)" = mrep || return 1
    test "$(pool_field .migrate-3 crush_rule)" = $rule || return 1
    test "$(rule_of .migrate-3)" = myrule.migrate-3 || return 1
}

function TEST_pool_migrate_keeps_shared_rule_and_profile() {
    local dir=$1
    run_mon_for_pool_migration $dir || return 1
    ceph osd erasure-code-profile set pshared k=2 m=1 crush-failure-domain=osd || return 1
    ceph osd crush rule create-erasure rshared pshared || return 1
    ceph osd pool create mshared 4 4 erasure pshared rshared || return 1
    ceph osd pool create other 4 4 erasure pshared rshared || return 1
    ceph osd pool create mdefault 4 4 replicated || return 1
    local default_rule=$(rule_of mdefault)
    local steps=$(ceph osd crush rule dump $default_rule -f json | jq -c 'del(.rule_id, .rule_name)')

    # the target has copies of the rules and profiles that keep their names
    ceph osd pool migrate mshared --yes-i-really-mean-it || return 1
    test "$(rule_of .migrate-1)" = rshared || return 1
    test "$(profile_of .migrate-1)" = pshared || return 1
    test "$(rule_of mshared)" = mshared || return 1
    test "$(profile_of mshared)" = mshared-k2-m1 || return 1
    ceph osd pool migrate mdefault --yes-i-really-mean-it || return 1
    test "$(rule_of .migrate-2)" = $default_rule || return 1
    test "$(rule_of mdefault)" = mdefault || return 1
    test "$(ceph osd crush rule dump mdefault -f json | jq -c 'del(.rule_id, .rule_name)')" = "$steps" || return 1

    # and of the defaults for a new pool type
    ceph osd pool create mtoec 4 4 replicated || return 1
    ceph osd pool migrate mtoec --pool_type erasure --enable_ec_optimizations || return 1
    test "$(rule_of mtoec)" = mtoec || return 1
    profile_of mtoec | grep -qx 'mtoec-k[0-9]*-m[0-9]*' || return 1
    ceph osd pool create mtorep 4 4 erasure pshared rshared || return 1
    ceph osd pool migrate mtorep --pool_type replicated || return 1
    test "$(rule_of mtorep)" = mtorep || return 1

    # a rule or profile that the command names is shared, and keeps its name
    ceph osd erasure-code-profile set pown k=2 m=1 crush-failure-domain=osd || return 1
    ceph osd pool create mown 4 4 erasure pown || return 1
    ceph osd pool migrate mown --pool_type erasure --erasure_code_profile pown --rule mown \
        --enable_ec_optimizations || return 1
    for pool in mown .migrate-5 ; do
        test "$(rule_of $pool)" = mown || return 1
        test "$(profile_of $pool)" = pown || return 1
    done
    ! ceph osd crush rule ls | grep -q migrate || return 1
    ! ceph osd erasure-code-profile ls | grep -q migrate || return 1
}

function TEST_pool_migrate_name_clash() {
    local dir=$1
    run_mon_for_pool_migration $dir || return 1
    ceph osd erasure-code-profile set pclash k=2 m=1 crush-failure-domain=osd || return 1
    ceph osd pool create mclash 4 4 erasure pclash || return 1

    # the name for the source's rule is taken
    ceph osd crush rule create-erasure mclash.migrate-1 pclash || return 1
    expect_failure $dir "EEXIST.*rule 'mclash.migrate-1'" \
        ceph osd pool migrate mclash --yes-i-really-mean-it || return 1
    ceph osd crush rule rm mclash.migrate-1 || return 1
    # the name for the source's profile is taken
    ceph osd erasure-code-profile set pclash.migrate-1 k=2 m=1 crush-failure-domain=osd || return 1
    expect_failure $dir "EEXIST.*profile 'pclash.migrate-1'" \
        ceph osd pool migrate mclash --yes-i-really-mean-it || return 1
    ceph osd erasure-code-profile rm pclash.migrate-1 || return 1
    # the name for the target's profile is taken, with other contents
    ceph osd erasure-code-profile set mclash-k2-m1 k=2 m=1 crush-failure-domain=host || return 1
    expect_failure $dir "EEXIST.*'mclash-k2-m1'" \
        ceph osd pool migrate mclash --yes-i-really-mean-it || return 1
    ceph osd erasure-code-profile rm mclash-k2-m1 || return 1
    # the name for the target's rule is another pool's rule
    ceph osd pool create mrep 4 4 replicated || return 1
    ceph osd crush rule create-erasure mrep pclash || return 1
    ceph osd pool create other 4 4 erasure pclash mrep || return 1
    expect_failure $dir "EEXIST.*rule 'mrep'" \
        ceph osd pool migrate mrep --yes-i-really-mean-it || return 1

    # the failures renamed nothing, and left nothing behind
    ceph osd pool create later 4 4 replicated || return 1
    ! ceph osd pool ls --show-all | grep -q migrate || return 1
    test "$(rule_of mclash)" = mclash || return 1
    test "$(profile_of mclash)" = pclash || return 1
    test "$(rule_of mrep)" = "$(rule_of later)" || return 1
    ! ceph osd erasure-code-profile ls | grep -q '^mclash-' || return 1
}

function TEST_pool_migrate_changes_profile_contents() {
    local dir=$1
    run_mon_for_pool_migration $dir || return 1
    ceph osd pool create mec erasure --k 2 --m 1 --osd_failure_domain osd || return 1
    local before=$(ceph osd erasure-code-profile get mec-k2-m1 -f json | jq -cS .)

    # the target's profile has the name of the source's, with other contents
    ceph osd pool migrate mec --pool_type erasure --k 2 --m 1 --osd_failure_domain host \
        --enable_ec_optimizations || return 1
    test "$(profile_of mec)" = mec-k2-m1 || return 1
    ceph osd erasure-code-profile get mec-k2-m1 | grep -q 'failure-domain=host' || return 1
    ceph osd crush rule dump mec -f json | jq -e '[.steps[] | select(.type == "host")] | length > 0' || return 1
    test "$(profile_of .migrate-1)" = mec-k2-m1.migrate-1 || return 1
    test "$(ceph osd erasure-code-profile get mec-k2-m1.migrate-1 -f json | jq -cS .)" = "$before" || return 1
    test "$(rule_of .migrate-1)" = mec.migrate-1 || return 1

    # a failure once the source's profile has its new name gives it back the old one
    ceph osd pool create mundo erasure --k 2 --m 1 --osd_failure_domain osd || return 1
    before=$(ceph osd erasure-code-profile get mundo-k2-m1 -f json | jq -cS .)
    ceph osd crush rule create-replicated mundo.migrate-2 default osd || return 1
    expect_failure $dir "EEXIST.*rule 'mundo.migrate-2'" \
        ceph osd pool migrate mundo --pool_type erasure --k 2 --m 1 --osd_failure_domain host \
        --enable_ec_optimizations || return 1
    ceph osd pool ls | grep -qx mundo || return 1
    test "$(profile_of mundo)" = mundo-k2-m1 || return 1
    test "$(ceph osd erasure-code-profile get mundo-k2-m1 -f json | jq -cS .)" = "$before" || return 1
    ! ceph osd erasure-code-profile ls | grep -q '^mundo-k2-m1.migrate' || return 1
}

# The monitors run a command again after an election: a migration of a pool
# that is being migrated to the layout it asks for changes nothing, also when
# its options give the target its own rule and erasure code profile
function TEST_pool_migrate_again_to_layout() {
    local dir=$1
    run_mon_for_pool_migration $dir || return 1
    ceph osd pool create mrep 4 4 replicated || return 1
    ceph osd pool create mec 4 4 erasure --k 2 --m 1 --osd_failure_domain osd \
        --enable_ec_optimizations || return 1

    ceph osd pool migrate mrep --pool_type replicated --size 2 || return 1
    local target=$(pool_field mrep pool)
    ceph osd pool migrate mrep --pool_type replicated --size 2 || return 1
    test "$(pool_field mrep pool)" = $target || return 1
    expect_failure $dir "ongoing migration" \
        ceph osd pool migrate mrep --pool_type replicated --size 3 || return 1
    expect_failure $dir "ongoing migration" \
        ceph osd pool migrate .migrate-1 --pool_type replicated --size 2 || return 1

    ceph osd pool migrate mec --pool_type erasure --k 4 --m 2 --osd_failure_domain osd \
        --enable_ec_optimizations || return 1
    target=$(pool_field mec pool)
    ceph osd pool migrate mec --pool_type erasure --k 4 --m 2 --osd_failure_domain osd \
        --enable_ec_optimizations || return 1
    test "$(pool_field mec pool)" = $target || return 1
    expect_failure $dir "ongoing migration" \
        ceph osd pool migrate mec --pool_type erasure --k 4 --m 2 --osd_failure_domain host \
        --enable_ec_optimizations || return 1
    test "$(ceph osd pool ls --show-all | grep -c 'migrate-')" = 2 || return 1
}

main osd-pool-create "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/mon/osd-pool-create.sh"
# End:
