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

    export CEPH_MON="127.0.0.1:7102" # git grep '\<7102\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        $func $dir || return 1
    done
}

TEST_POOL=rbd

function TEST_osd_pool_get_set() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a || return 1
    create_pool $TEST_POOL 8

    local flag
    for flag in nodelete nopgchange nosizechange write_fadvise_dontneed noscrub nodeep-scrub; do
	ceph osd pool set $TEST_POOL $flag 0 || return 1
	! ceph osd dump | grep 'pool ' | grep $flag || return 1
	ceph osd pool set $TEST_POOL $flag 1 || return 1
	ceph osd dump | grep 'pool ' | grep $flag || return 1
	ceph osd pool set $TEST_POOL $flag false || return 1
	! ceph osd dump | grep 'pool ' | grep $flag || return 1
	ceph osd pool set $TEST_POOL $flag false || return 1
        # check that setting false twice does not toggle to true (bug)
	! ceph osd dump | grep 'pool ' | grep $flag || return 1
	ceph osd pool set $TEST_POOL $flag true || return 1
	ceph osd dump | grep 'pool ' | grep $flag || return 1
	# cleanup
	ceph osd pool set $TEST_POOL $flag 0 || return 1
    done

    local size=$(ceph osd pool get $TEST_POOL size|awk '{print $2}')
    local min_size=$(ceph osd pool get $TEST_POOL min_size|awk '{print $2}')
    local expected_min_size=$(expr $size - $size / 2)
    if [ $min_size -ne $expected_min_size ]; then
	echo "default min_size is wrong: expected $expected_min_size, got $min_size"
	return 1
    fi

    ceph osd pool set $TEST_POOL scrub_min_interval 123456 || return 1
    ceph osd dump | grep 'pool ' | grep 'scrub_min_interval 123456' || return 1
    ceph osd pool set $TEST_POOL scrub_min_interval 0 || return 1
    ceph osd dump | grep 'pool ' | grep 'scrub_min_interval' && return 1
    ceph osd pool set $TEST_POOL scrub_max_interval 123456 || return 1
    ceph osd dump | grep 'pool ' | grep 'scrub_max_interval 123456' || return 1
    ceph osd pool set $TEST_POOL scrub_max_interval 0 || return 1
    ceph osd dump | grep 'pool ' | grep 'scrub_max_interval' && return 1
    ceph osd pool set $TEST_POOL deep_scrub_interval 123456 || return 1
    ceph osd dump | grep 'pool ' | grep 'deep_scrub_interval 123456' || return 1
    ceph osd pool set $TEST_POOL deep_scrub_interval 0 || return 1
    ceph osd dump | grep 'pool ' | grep 'deep_scrub_interval' && return 1

    #replicated pool size restrict in 1 and 10
    ! ceph osd pool set $TEST_POOL 11 || return 1
    #replicated pool min_size must be between in 1 and size
    ! ceph osd pool set $TEST_POOL min_size $(expr $size + 1) || return 1
    ! ceph osd pool set $TEST_POOL min_size 0 || return 1

    local ecpool=erasepool
    create_pool $ecpool 12 12 erasure default || return 1
    #erasue pool size=k+m, min_size=k
    local size=$(ceph osd pool get $ecpool size|awk '{print $2}')
    local min_size=$(ceph osd pool get $ecpool min_size|awk '{print $2}')
    local k=$(expr $min_size - 1)  # default min_size=k+1
    #erasure pool size can't change
    ! ceph osd pool set $ecpool size  $(expr $size + 1) || return 1
    #erasure pool min_size must be between in k and size
    ceph osd pool set $ecpool min_size $(expr $k + 1) || return 1
    ! ceph osd pool set $ecpool min_size $(expr $k - 1) || return 1
    ! ceph osd pool set $ecpool min_size $(expr $size + 1) || return 1

    teardown $dir || return 1
}

function TEST_mon_add_to_single_mon() {
    local dir=$1

    fsid=$(uuidgen)
    MONA=127.0.0.1:7117 # git grep '\<7117\>' : there must be only one
    MONB=127.0.0.1:7118 # git grep '\<7118\>' : there must be only one
    CEPH_ARGS_orig=$CEPH_ARGS
    CEPH_ARGS="--fsid=$fsid --auth-supported=none "
    CEPH_ARGS+="--mon-initial-members=a "
    CEPH_ARGS+="--mon-host=$MONA "

    setup $dir || return 1
    run_mon $dir a --public-addr $MONA || return 1
    # wait for the quorum
    timeout 120 ceph -s > /dev/null || return 1
    run_mon $dir b --public-addr $MONB || return 1
    teardown $dir || return 1

    setup $dir || return 1
    run_mon $dir a --public-addr $MONA || return 1
    # without the fix of #5454, mon.a will assert failure at seeing the MMonJoin
    # from mon.b
    run_mon $dir b --public-addr $MONB || return 1
    # make sure mon.b get's it's join request in first, then
    sleep 2
    # wait for the quorum
    timeout 120 ceph -s > /dev/null || return 1
    ceph mon dump
    ceph mon dump -f json-pretty
    local num_mons
    num_mons=$(ceph mon dump --format=json 2>/dev/null | jq ".mons | length") || return 1
    [ $num_mons == 2 ] || return 1
    # no reason to take more than 120 secs to get this submitted
    timeout 120 ceph mon add b $MONB || return 1
    teardown $dir || return 1
}

function TEST_no_segfault_for_bad_keyring() {
    local dir=$1
    setup $dir || return 1
    # create a client.admin key and add it to ceph.mon.keyring
    ceph-authtool --create-keyring $dir/ceph.mon.keyring --gen-key -n mon. --cap mon 'allow *'
    ceph-authtool --create-keyring $dir/ceph.client.admin.keyring --gen-key -n client.admin --cap mon 'allow *'
    ceph-authtool $dir/ceph.mon.keyring --import-keyring $dir/ceph.client.admin.keyring
    CEPH_ARGS_TMP="--fsid=$(uuidgen) --mon-host=127.0.0.1:7102 --auth-supported=cephx "
    CEPH_ARGS_orig=$CEPH_ARGS
    CEPH_ARGS="$CEPH_ARGS_TMP --keyring=$dir/ceph.mon.keyring "
    run_mon $dir a
    # create a bad keyring and make sure no segfault occurs when using the bad keyring
    echo -e "[client.admin]\nkey = BQAUlgtWoFePIxAAQ9YLzJSVgJX5V1lh5gyctg==" > $dir/bad.keyring
    CEPH_ARGS="$CEPH_ARGS_TMP --keyring=$dir/bad.keyring"
    ceph osd dump 2> /dev/null
    # 139(11|128) means segfault and core dumped
    [ $? -eq 139 ] && return 1
    CEPH_ARGS=$CEPH_ARGS_orig
    teardown $dir || return 1
}

function TEST_mon_features() {
    local dir=$1
    setup $dir || return 1

    fsid=$(uuidgen)
    MONA=127.0.0.1:7127 # git grep '\<7127\>' ; there must be only one
    MONB=127.0.0.1:7128 # git grep '\<7128\>' ; there must be only one
    MONC=127.0.0.1:7129 # git grep '\<7129\>' ; there must be only one
    CEPH_ARGS_orig=$CEPH_ARGS
    CEPH_ARGS="--fsid=$fsid --auth-supported=none "
    CEPH_ARGS+="--mon-host=$MONA,$MONB,$MONC "
    CEPH_ARGS+="--mon-debug-no-initial-persistent-features "
    CEPH_ARGS+="--mon-debug-no-require-squid "

    run_mon $dir a --public-addr $MONA || return 1
    run_mon $dir b --public-addr $MONB || return 1
    timeout 120 ceph -s > /dev/null || return 1

    # expect monmap to contain 3 monitors (a, b, and c)
    jqinput="$(ceph quorum_status --format=json 2>/dev/null)"
    jq_success "$jqinput" '.monmap.mons | length == 3' || return 1
    # quorum contains two monitors
    jq_success "$jqinput" '.quorum | length == 2' || return 1
    # quorum's monitor features contain kraken, luminous, mimic, nautilus,
    # octopus, pacific, quincy, reef
    jqfilter='.features.quorum_mon[]|select(. == "kraken")'
    jq_success "$jqinput" "$jqfilter" "kraken" || return 1
    jqfilter='.features.quorum_mon[]|select(. == "luminous")'
    jq_success "$jqinput" "$jqfilter" "luminous" || return 1
    jqfilter='.features.quorum_mon[]|select(. == "mimic")'
    jq_success "$jqinput" "$jqfilter" "mimic" || return 1
    jqfilter='.features.quorum_mon[]|select(. == "nautilus")'
    jq_success "$jqinput" "$jqfilter" "nautilus" || return 1
    jqfilter='.features.quorum_mon[]|select(. == "octopus")'
    jq_success "$jqinput" "$jqfilter" "octopus" || return 1
    jqfilter='.features.quorum_mon[]|select(. == "pacific")'
    jq_success "$jqinput" "$jqfilter" "pacific" || return 1
    jqfilter='.features.quorum_mon[]|select(. == "quincy")'
    jq_success "$jqinput" "$jqfilter" "quincy" || return 1
    jqfilter='.features.quorum_mon[]|select(. == "reef")'
    jq_success "$jqinput" "$jqfilter" "reef" || return 1
    jqfilter='.features.quorum_mon[]|select(. == "squid")'
    jq_success "$jqinput" "$jqfilter" "squid" || return 1

    # monmap must have no persistent features set, because we
    # don't currently have a quorum made out of all the monitors
    # in the monmap.
    jqfilter='.monmap.features.persistent | length == 0'
    jq_success "$jqinput" "$jqfilter" || return 1

    # nor do we have any optional features, for that matter.
    jqfilter='.monmap.features.optional | length == 0'
    jq_success "$jqinput" "$jqfilter" || return 1

    # validate 'mon feature ls'

    jqinput="$(ceph mon feature ls --format=json 2>/dev/null)"
    # k l m n o p q r are supported
    jqfilter='.all.supported[] | select(. == "kraken")'
    jq_success "$jqinput" "$jqfilter" "kraken" || return 1
    jqfilter='.all.supported[] | select(. == "luminous")'
    jq_success "$jqinput" "$jqfilter" "luminous" || return 1
    jqfilter='.all.supported[] | select(. == "mimic")'
    jq_success "$jqinput" "$jqfilter" "mimic" || return 1
    jqfilter='.all.supported[] | select(. == "nautilus")'
    jq_success "$jqinput" "$jqfilter" "nautilus" || return 1
    jqfilter='.all.supported[] | select(. == "octopus")'
    jq_success "$jqinput" "$jqfilter" "octopus" || return 1
    jqfilter='.all.supported[] | select(. == "pacific")'
    jq_success "$jqinput" "$jqfilter" "pacific" || return 1
    jqfilter='.all.supported[] | select(. == "quincy")'
    jq_success "$jqinput" "$jqfilter" "quincy" || return 1
    jqfilter='.all.supported[] | select(. == "reef")'
    jq_success "$jqinput" "$jqfilter" "reef" || return 1
    jqfilter='.all.supported[] | select(. == "squid")'
    jq_success "$jqinput" "$jqfilter" "squid" || return 1

    # start third monitor
    run_mon $dir c --public-addr $MONC || return 1

    wait_for_quorum 300 3 || return 1

    timeout 300 ceph -s > /dev/null || return 1

    jqinput="$(ceph quorum_status --format=json 2>/dev/null)"
    # expect quorum to have all three monitors
    jqfilter='.quorum | length == 3'
    jq_success "$jqinput" "$jqfilter" || return 1

    # quorum's monitor features should have p now too
    jqfilter='.features.quorum_mon[]|select(. == "pacific")'
    jq_success "$jqinput" "$jqfilter" "pacific" || return 1

    # persistent too
    jqfilter='.monmap.features.persistent[]|select(. == "kraken")'
    jq_success "$jqinput" "$jqfilter" "kraken" || return 1
    jqfilter='.monmap.features.persistent[]|select(. == "luminous")'
    jq_success "$jqinput" "$jqfilter" "luminous" || return 1
    jqfilter='.monmap.features.persistent[]|select(. == "mimic")'
    jq_success "$jqinput" "$jqfilter" "mimic" || return 1
    jqfilter='.monmap.features.persistent[]|select(. == "osdmap-prune")'
    jq_success "$jqinput" "$jqfilter" "osdmap-prune" || return 1
    jqfilter='.monmap.features.persistent[]|select(. == "nautilus")'
    jq_success "$jqinput" "$jqfilter" "nautilus" || return 1
    jqfilter='.monmap.features.persistent[]|select(. == "octopus")'
    jq_success "$jqinput" "$jqfilter" "octopus" || return 1
    jqfilter='.monmap.features.persistent[]|select(. == "pacific")'
    jq_success "$jqinput" "$jqfilter" "pacific" || return 1
    jqfilter='.monmap.features.persistent[]|select(. == "elector-pinging")'
    jq_success "$jqinput" "$jqfilter" "elector-pinging" || return 1
    jqfilter='.monmap.features.persistent | length == 11'
    jq_success "$jqinput" "$jqfilter" || return 1
    jqfilter='.monmap.features.persistent[]|select(. == "quincy")'
    jq_success "$jqinput" "$jqfilter" "quincy" || return 1
    jqfilter='.monmap.features.persistent[]|select(. == "reef")'
    jq_success "$jqinput" "$jqfilter" "reef" || return 1
    jqfilter='.monmap.features.persistent[]|select(. == "squid")'
    jq_success "$jqinput" "$jqfilter" "squid" || return 1

    CEPH_ARGS=$CEPH_ARGS_orig
    # that's all folks. thank you for tuning in.
    teardown $dir || return 1
}

main misc "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/mon/misc.sh"
# End:
