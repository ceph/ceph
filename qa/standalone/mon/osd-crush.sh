#!/usr/bin/env bash
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
source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7104" # git grep '\<7104\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | ${SED} -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function TEST_crush_rule_create_simple() {
    local dir=$1

    run_mon $dir a || return 1

    ceph --format xml osd crush rule dump replicated_rule | \
        egrep '<op>take</op><item>[^<]+</item><item_name>default</item_name>' | \
        grep '<op>choose_firstn</op><num>0</num><type>osd</type>' || return 1
    local rule=rule0
    local root=host1
    ceph osd crush add-bucket $root host
    local failure_domain=osd
    ceph osd crush rule create-simple $rule $root $failure_domain || return 1
    ceph osd crush rule create-simple $rule $root $failure_domain 2>&1 | \
        grep "$rule already exists" || return 1
    ceph --format xml osd crush rule dump $rule | \
        egrep '<op>take</op><item>[^<]+</item><item_name>'$root'</item_name>' | \
        grep '<op>choose_firstn</op><num>0</num><type>'$failure_domain'</type>' || return 1
    ceph osd crush rule rm $rule || return 1
}

function TEST_crush_rule_dump() {
    local dir=$1

    run_mon $dir a || return 1

    local rule=rule1
    ceph osd crush rule create-erasure $rule || return 1
    test $(ceph --format json osd crush rule dump $rule | \
           jq ".rule_name == \"$rule\"") == true || return 1
    test $(ceph --format json osd crush rule dump | \
           jq "map(select(.rule_name == \"$rule\")) | length == 1") == true || return 1
    ! ceph osd crush rule dump non_existent_rule || return 1
    ceph osd crush rule rm $rule || return 1
}

function TEST_crush_rule_rm() {
    local rule=erasure2

    run_mon $dir a || return 1

    ceph osd crush rule create-erasure $rule default || return 1
    ceph osd crush rule ls | grep $rule || return 1
    ceph osd crush rule rm $rule || return 1
    ! ceph osd crush rule ls | grep $rule || return 1
}

function TEST_crush_rule_create_erasure() {
    local dir=$1

    run_mon $dir a || return 1
    # should have at least one OSD
    run_osd $dir 0 || return 1

    local rule=rule3
    #
    # create a new rule with the default profile, implicitly
    #
    ceph osd crush rule create-erasure $rule || return 1
    ceph osd crush rule create-erasure $rule 2>&1 | \
        grep "$rule already exists" || return 1
    ceph --format xml osd crush rule dump $rule | \
        egrep '<op>take</op><item>[^<]+</item><item_name>default</item_name>' | \
        grep '<op>chooseleaf_indep</op><num>0</num><type>host</type>' || return 1
    ceph osd crush rule rm $rule || return 1
    ! ceph osd crush rule ls | grep $rule || return 1
    #
    # create a new rule with the default profile, explicitly
    #
    ceph osd crush rule create-erasure $rule default || return 1
    ceph osd crush rule ls | grep $rule || return 1
    ceph osd crush rule rm $rule || return 1
    ! ceph osd crush rule ls | grep $rule || return 1
    #
    # create a new rule and the default profile, implicitly
    #
    ceph osd erasure-code-profile rm default || return 1
    ! ceph osd erasure-code-profile ls | grep default || return 1
    ceph osd crush rule create-erasure $rule || return 1
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path mon.a) log flush || return 1
    grep 'profile set default' $dir/mon.a.log || return 1
    ceph osd erasure-code-profile ls | grep default || return 1
    ceph osd crush rule rm $rule || return 1
    ! ceph osd crush rule ls | grep $rule || return 1
}

function TEST_add_rule_failed() {
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
	id $i
	type replicated
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
    ceph osd tree
    ! ceph osd tree | grep host2 || return 1
    ceph osd crush rename-bucket host1 host2 || return 1
    ceph osd tree
    ceph osd tree | grep host2 || return 1
    ceph osd crush rename-bucket host1 host2 || return 1 # idempotency
    ceph osd crush rename-bucket nonexistent something 2>&1 | grep "Error ENOENT" || return 1
}

function TEST_crush_ls_node() {
    local dir=$1
    run_mon $dir a || return 1
    ceph osd crush add-bucket default1 root
    ceph osd crush add-bucket host1 host
    ceph osd crush move host1 root=default1
    ceph osd crush ls default1 | grep host1 || return 1
    ceph osd crush ls default2 2>&1 | grep "Error ENOENT" || return 1
}

function TEST_crush_reject_empty() {
    local dir=$1
    run_mon $dir a --osd_pool_default_size=1 --mon_allow_pool_size_one=true || return 1
    # should have at least one OSD
    run_osd $dir 0 || return 1
    create_rbd_pool || return 1

    local empty_map=$dir/empty_map
    :> $empty_map.txt
    crushtool -c $empty_map.txt -o $empty_map.map || return 1
    expect_failure $dir "Error EINVAL" \
        ceph osd setcrushmap -i $empty_map.map || return 1
}

main osd-crush "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/mon/osd-crush.sh"
# End:
