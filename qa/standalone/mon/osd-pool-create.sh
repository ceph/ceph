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

# The next proposal must not pick up the pool of a create that failed
function TEST_failed_pool_create_leaves_no_pool() {
    local dir=$1
    run_mon $dir a --enable_experimental_unrecoverable_data_corrupting_features=crimson || return 1
    ceph osd set-allow-crimson --yes-i-really-mean-it || return 1
    # crimson needs EC optimizations, which lrc does not support
    ceph osd erasure-code-profile set lrcprofile plugin=lrc k=2 m=1 l=3 || return 1
    ! ceph osd pool create badpool 12 12 erasure lrcprofile --crimson || return 1
    timeout 60 ceph osd pool create goodpool 12 || return 1
    ceph osd pool ls | grep -qx goodpool || return 1
    ! ceph osd pool ls | grep -q badpool || return 1
}

# A pool create that fails removes the rule and the profile it generated; a
# rule of that name made by hand stays
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
    ! ceph osd crush rule ls | grep -qx erasure-code || return 1
    ! ceph osd erasure-code-profile ls | grep -q '^bigec-' || return 1
}

# A default profile with a stripe unit that is not a multiple of 4096, which
# only --force accepts, does not serve the pools that name no profile, and the
# refused create leaves nothing behind
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

# --yes-i-really-mean-it creates a pool with such a stripe unit, from a named
# profile or from the default one
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
        erasure-code || return 1

    ceph osd pool create erasurecodes 12 12 erasure 2>&1 | \
        grep 'already exists' || return 1
    ceph osd pool create erasurecodes 12 12 2>&1 | \
        grep 'cannot change to type replicated' || return 1
}

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
    # recreating the pool reuses the profile
    ceph osd pool delete $poolname $poolname --yes-i-really-really-mean-it || return 1
    ceph osd pool create $poolname --pool_type erasure --k 2 --m 1 \
        --pg_num 8 --pgp_num 8 \
        --zone_failure_domain rack --osd_failure_domain osd || return 1
}

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


# A pool given a rule in the same update that removes the rule must not get
# it: the pool would have a rule that is gone
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

# A rule created by name while the deletion of its last pool is pending exists
# afterwards
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

# A default rule named in the update that removes the rule must not be taken:
# replicated pools would have no default rule left
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

# osd pool set num_zones checks the rule and the replicas as pool create does
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

# The min_size of an LRC pool is checked against its chunks
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

# A pool deleted in the update that gives it another rule takes that rule
# with it
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

main osd-pool-create "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/mon/osd-pool-create.sh"
# End:
