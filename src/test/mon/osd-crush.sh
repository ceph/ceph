#!/bin/bash
#
# Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
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
source $CEPH_ROOT/qa/workunits/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7104" # git grep '\<7104\>' : there must be only one
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

function TEST_crush_rule_create_simple() {
    local dir=$1

    run_mon $dir a || return 1

    ceph --format xml osd crush rule dump replicated_ruleset | \
        egrep '<op>take</op><item>[^<]+</item><item_name>default</item_name>' | \
        grep '<op>choose_firstn</op><num>0</num><type>osd</type>' || return 1
    local ruleset=ruleset0
    local root=host1
    ceph osd crush add-bucket $root host
    local failure_domain=osd
    ceph osd crush rule create-simple $ruleset $root $failure_domain || return 1
    ceph osd crush rule create-simple $ruleset $root $failure_domain 2>&1 | \
        grep "$ruleset already exists" || return 1
    ceph --format xml osd crush rule dump $ruleset | \
        egrep '<op>take</op><item>[^<]+</item><item_name>'$root'</item_name>' | \
        grep '<op>choose_firstn</op><num>0</num><type>'$failure_domain'</type>' || return 1
    ceph osd crush rule rm $ruleset || return 1
}

function TEST_crush_rule_dump() {
    local dir=$1

    run_mon $dir a || return 1

    local ruleset=ruleset1
    ceph osd crush rule create-erasure $ruleset || return 1
    local expected
    expected="<rule_name>$ruleset</rule_name>"
    ceph --format xml osd crush rule dump $ruleset | grep $expected || return 1
    expected='"rule_name": "'$ruleset'"'
    ceph osd crush rule dump | grep "$expected" || return 1
    ! ceph osd crush rule dump non_existent_ruleset || return 1
    ceph osd crush rule rm $ruleset || return 1
}

function TEST_crush_rule_rm() {
    local ruleset=erasure2

    run_mon $dir a || return 1

    ceph osd crush rule create-erasure $ruleset default || return 1
    ceph osd crush rule ls | grep $ruleset || return 1
    ceph osd crush rule rm $ruleset || return 1
    ! ceph osd crush rule ls | grep $ruleset || return 1
}

function TEST_crush_rule_create_erasure() {
    local dir=$1

    run_mon $dir a || return 1
    # should have at least one OSD
    run_osd $dir 0 || return 1

    local ruleset=ruleset3
    #
    # create a new ruleset with the default profile, implicitly
    #
    ceph osd crush rule create-erasure $ruleset || return 1
    ceph osd crush rule create-erasure $ruleset 2>&1 | \
        grep "$ruleset already exists" || return 1
    ceph --format xml osd crush rule dump $ruleset | \
        egrep '<op>take</op><item>[^<]+</item><item_name>default</item_name>' | \
        grep '<op>chooseleaf_indep</op><num>0</num><type>host</type>' || return 1
    ceph osd crush rule rm $ruleset || return 1
    ! ceph osd crush rule ls | grep $ruleset || return 1
    #
    # create a new ruleset with the default profile, explicitly
    #
    ceph osd crush rule create-erasure $ruleset default || return 1
    ceph osd crush rule ls | grep $ruleset || return 1
    ceph osd crush rule rm $ruleset || return 1
    ! ceph osd crush rule ls | grep $ruleset || return 1
    #
    # create a new ruleset and the default profile, implicitly
    #
    ceph osd erasure-code-profile rm default || return 1
    ! ceph osd erasure-code-profile ls | grep default || return 1
    ceph osd crush rule create-erasure $ruleset || return 1
    CEPH_ARGS='' ceph --admin-daemon $dir/ceph-mon.a.asok log flush || return 1
    grep 'profile set default' $dir/mon.a.log || return 1
    ceph osd erasure-code-profile ls | grep default || return 1
    ceph osd crush rule rm $ruleset || return 1
    ! ceph osd crush rule ls | grep $ruleset || return 1
    #
    # verify that if the crushmap contains a bugous ruleset,
    # it will prevent the creation of a pool.
    #
    local crushtool_path_old=`ceph-conf --show-config-value crushtool`
    ceph tell mon.\* injectargs --crushtool "false"

    expect_failure $dir "Error EINVAL" \
        ceph osd pool create mypool 1 1 erasure || return 1
}

function check_ruleset_id_match_rule_id() {
    local rule_name=$1
    rule_id=`ceph osd crush rule dump $rule_name | grep "\"rule_id\":" | awk -F ":|," '{print int($2)}'`
    ruleset_id=`ceph osd crush rule dump $rule_name | grep "\"ruleset\":"| awk -F ":|," '{print int($2)}'`
    test $ruleset_id = $rule_id || return 1
}

function generate_manipulated_rules() {
    local dir=$1
    ceph osd crush add-bucket $root host
    ceph osd crush rule create-simple test_rule1 $root osd firstn || return 1
    ceph osd crush rule create-simple test_rule2 $root osd firstn || return 1
    ceph osd getcrushmap -o $dir/original_map
    crushtool -d $dir/original_map -o $dir/decoded_original_map
    #manipulate the rulesets , to make the rule_id != ruleset_id
    sed -i 's/ruleset 0/ruleset 3/' $dir/decoded_original_map
    sed -i 's/ruleset 2/ruleset 0/' $dir/decoded_original_map
    sed -i 's/ruleset 1/ruleset 2/' $dir/decoded_original_map

    crushtool -c $dir/decoded_original_map -o $dir/new_map
    ceph osd setcrushmap -i $dir/new_map

    ceph osd crush rule dump
}

function TEST_crush_ruleset_match_rule_when_creating() {
    local dir=$1

    run_mon $dir a || return 1

    local root=host1

    generate_manipulated_rules $dir

    ceph osd crush rule create-simple special_rule_simple $root osd firstn || return 1

    ceph osd crush rule dump
    #show special_rule_simple has same rule_id and ruleset_id
    check_ruleset_id_match_rule_id special_rule_simple || return 1
}

function TEST_add_ruleset_failed() {
    local dir=$1

    run_mon $dir a || return 1

    local root=host1

    ceph osd crush add-bucket $root host
    ceph osd crush rule create-simple test_rule1 $root osd firstn || return 1
    ceph osd crush rule create-simple test_rule2 $root osd firstn || return 1
    ceph osd getcrushmap > $dir/crushmap || return 1
    crushtool --decompile $dir/crushmap > $dir/crushmap.txt || return 1
    for i in $(seq 3 255)
        do
            cat <<EOF
rule test_rule$i {
	ruleset $i
	type replicated
	min_size 1
	max_size 10
	step take $root
	step choose firstn 0 type osd
	step emit
}
EOF
    done >> $dir/crushmap.txt
    crushtool --compile $dir/crushmap.txt -o $dir/crushmap || return 1
    ceph osd setcrushmap -i $dir/crushmap  || return 1
    ceph osd crush rule create-simple test_rule_nospace $root osd firstn 2>&1 | grep "Error ENOSPC" || return 1

}

function TEST_crush_rename_bucket() {
    local dir=$1

    run_mon $dir a || return 1

    ceph osd crush add-bucket host1 host
    ! ceph osd tree | grep host2 || return 1
    ceph osd crush rename-bucket host1 host2 || return 1
    ceph osd tree | grep host2 || return 1
    ceph osd crush rename-bucket host1 host2 || return 1 # idempotency
    ceph osd crush rename-bucket nonexistent something 2>&1 | grep "Error ENOENT" || return 1
}

function TEST_crush_reject_empty() {
    local dir=$1
    run_mon $dir a || return 1
    # should have at least one OSD
    run_osd $dir 0 || return 1

    local empty_map=$dir/empty_map
    :> $empty_map.txt
    crushtool -c $empty_map.txt -o $empty_map.map || return 1
    expect_failure $dir "Error EINVAL" \
        ceph osd setcrushmap -i $empty_map.map || return 1
}

function TEST_crush_tree() {
    local dir=$1
    run_mon $dir a || return 1

    ceph osd crush tree --format=xml | \
        $XMLSTARLET val -e -r $CEPH_ROOT/src/test/mon/osd-crush-tree.rng - || return 1
}

# NB: disable me if i am too time consuming
function TEST_crush_repair_faulty_crushmap() {
    local dir=$1
    fsid=$(uuidgen)
    MONA=127.0.0.1:7113 # git grep '\<7113\>' : there must be only one
    MONB=127.0.0.1:7114 # git grep '\<7114\>' : there must be only one
    MONC=127.0.0.1:7115 # git grep '\<7115\>' : there must be only one
    CEPH_ARGS_orig=$CEPH_ARGS
    CEPH_ARGS="--fsid=$fsid --auth-supported=none "
    CEPH_ARGS+="--mon-initial-members=a,b,c "
    CEPH_ARGS+="--mon-host=$MONA,$MONB,$MONC "
    run_mon $dir a --public-addr $MONA || return 1
    run_mon $dir b --public-addr $MONB || return 1
    run_mon $dir c --public-addr $MONC || return 1

    local empty_map=$dir/empty_map
    :> $empty_map.txt
    crushtool -c $empty_map.txt -o $empty_map.map || return 1

    local crushtool_path_old=`ceph-conf --show-config-value crushtool`
    ceph tell mon.\* injectargs --crushtool "true"
    
    
    #import empty crushmap should failture.because the default pool rbd use the rule
    ceph osd setcrushmap -i $empty_map.map  2>&1|grep "Error EINVAL:  the crush rule no"|| return 1

    #remove the default pool rbd
    ceph osd pool delete rbd rbd --yes-i-really-really-mean-it || return 1

    #now it can be successful to set the empty crush map
    ceph osd setcrushmap -i $empty_map.map || return 1

    # should be an empty crush map without any buckets
    ! test $(ceph osd crush dump --format=xml | \
           $XMLSTARLET sel -t -m "//buckets/bucket" -v .) || return 1
    # bring them down, the "ceph" commands will try to hunt for other monitor in
    # vain, after mon.a is offline
    kill_daemons $dir || return 1
    # rewrite the monstore with the good crush map,
    $CEPH_ROOT/src/tools/ceph-monstore-update-crush.sh --rewrite $dir/a || return 1

    run_mon $dir a --public-addr $MONA || return 1
    run_mon $dir b --public-addr $MONB || return 1
    run_mon $dir c --public-addr $MONC || return 1
    # the buckets are back
    test $(ceph osd crush dump --format=xml | \
           $XMLSTARLET sel -t -m "//buckets/bucket" -v .) || return 1
    CEPH_ARGS=$CEPH_ARGS_orig
}

main osd-crush "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/mon/osd-crush.sh"
# End:
