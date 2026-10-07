#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON_A="127.0.0.1:7150" # git grep '\<7150\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7151" # git grep '\<7151\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7152" # git grep '\<7152\>' : there must be only one
    export CEPH_MON_D="127.0.0.1:7153" # git grep '\<7153\>' : there must be only one
    export CEPH_MON_E="127.0.0.1:7154" # git grep '\<7154\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
    export ORIG_CEPH_ARGS="$CEPH_ARGS"

    local funcs=${@:-$(set | ${SED} -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        kill_daemons $dir KILL || return 1
        teardown $dir || return 1
    done
}

function TEST_1_mon_checks() {
    local dir=$1

    CEPH_ARGS="$ORIG_CEPH_ARGS --mon-host=$CEPH_MON_A "

    run_mon $dir a --public-addr=$CEPH_MON_A || return 1

    ceph mon ok-to-stop dne || return 1
    ! ceph mon ok-to-stop a || return 1

    ! ceph mon ok-to-add-offline || return 1

    ! ceph mon ok-to-rm a || return 1
    ceph mon ok-to-rm dne || return 1
}

function TEST_2_mons_checks() {
    local dir=$1

    CEPH_ARGS="$ORIG_CEPH_ARGS --mon-host=$CEPH_MON_A,$CEPH_MON_B "

    run_mon $dir a --public-addr=$CEPH_MON_A || return 1
    run_mon $dir b --public-addr=$CEPH_MON_B || return 1

    ceph mon ok-to-stop dne || return 1
    ! ceph mon ok-to-stop a || return 1
    ! ceph mon ok-to-stop b || return 1
    ! ceph mon ok-to-stop a b || return 1

    ceph mon ok-to-add-offline || return 1

    ceph mon ok-to-rm a || return 1
    ceph mon ok-to-rm b || return 1
    ceph mon ok-to-rm dne || return 1
}

function TEST_3_mons_checks() {
    local dir=$1

    CEPH_ARGS="$ORIG_CEPH_ARGS --mon-host=$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C "

    run_mon $dir a --public-addr=$CEPH_MON_A || return 1
    run_mon $dir b --public-addr=$CEPH_MON_B || return 1
    run_mon $dir c --public-addr=$CEPH_MON_C || return 1
    wait_for_quorum 60 3

    ceph mon ok-to-stop dne || return 1
    ceph mon ok-to-stop a || return 1
    ceph mon ok-to-stop b || return 1
    ceph mon ok-to-stop c || return 1
    ! ceph mon ok-to-stop a b || return 1
    ! ceph mon ok-to-stop b c || return 1
    ! ceph mon ok-to-stop a b c || return 1

    ceph mon ok-to-add-offline || return 1

    ceph mon ok-to-rm a || return 1
    ceph mon ok-to-rm b || return 1
    ceph mon ok-to-rm c || return 1

    kill_daemons $dir KILL mon.b
    wait_for_quorum 60 2

    ! ceph mon ok-to-stop a || return 1
    ceph mon ok-to-stop b || return 1
    ! ceph mon ok-to-stop c || return 1

    ! ceph mon ok-to-add-offline || return 1

    ! ceph mon ok-to-rm a || return 1
    ceph mon ok-to-rm b || return 1
    ! ceph mon ok-to-rm c || return 1
}

function TEST_4_mons_checks() {
    local dir=$1

    CEPH_ARGS="$ORIG_CEPH_ARGS --mon-host=$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C,$CEPH_MON_D "

    run_mon $dir a --public-addr=$CEPH_MON_A || return 1
    run_mon $dir b --public-addr=$CEPH_MON_B || return 1
    run_mon $dir c --public-addr=$CEPH_MON_C || return 1
    run_mon $dir d --public-addr=$CEPH_MON_D || return 1
    wait_for_quorum 60 4

    ceph mon ok-to-stop dne || return 1
    ceph mon ok-to-stop a || return 1
    ceph mon ok-to-stop b || return 1
    ceph mon ok-to-stop c || return 1
    ceph mon ok-to-stop d || return 1
    ! ceph mon ok-to-stop a b || return 1
    ! ceph mon ok-to-stop c d || return 1

    ceph mon ok-to-add-offline || return 1

    ceph mon ok-to-rm a || return 1
    ceph mon ok-to-rm b || return 1
    ceph mon ok-to-rm c || return 1

    kill_daemons $dir KILL mon.a
    wait_for_quorum 60 3

    ceph mon ok-to-stop a || return 1
    ! ceph mon ok-to-stop b || return 1
    ! ceph mon ok-to-stop c || return 1
    ! ceph mon ok-to-stop d || return 1

    ceph mon ok-to-add-offline || return 1

    ceph mon ok-to-rm a || return 1
    ceph mon ok-to-rm b || return 1
    ceph mon ok-to-rm c || return 1
    ceph mon ok-to-rm d || return 1
}

function TEST_5_mons_checks() {
    local dir=$1

    CEPH_ARGS="$ORIG_CEPH_ARGS --mon-host=$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C,$CEPH_MON_D,$CEPH_MON_E "

    run_mon $dir a --public-addr=$CEPH_MON_A || return 1
    run_mon $dir b --public-addr=$CEPH_MON_B || return 1
    run_mon $dir c --public-addr=$CEPH_MON_C || return 1
    run_mon $dir d --public-addr=$CEPH_MON_D || return 1
    run_mon $dir e --public-addr=$CEPH_MON_E || return 1
    wait_for_quorum 60 5

    ceph mon ok-to-stop dne || return 1
    ceph mon ok-to-stop a || return 1
    ceph mon ok-to-stop b || return 1
    ceph mon ok-to-stop c || return 1
    ceph mon ok-to-stop d || return 1
    ceph mon ok-to-stop e || return 1
    ceph mon ok-to-stop a b || return 1
    ceph mon ok-to-stop c d || return 1
    ! ceph mon ok-to-stop a b c || return 1

    ceph mon ok-to-add-offline || return 1

    ceph mon ok-to-rm a || return 1
    ceph mon ok-to-rm b || return 1
    ceph mon ok-to-rm c || return 1
    ceph mon ok-to-rm d || return 1
    ceph mon ok-to-rm e || return 1

    kill_daemons $dir KILL mon.a
    wait_for_quorum 60 4

    ceph mon ok-to-stop a || return 1
    ceph mon ok-to-stop b || return 1
    ceph mon ok-to-stop c || return 1
    ceph mon ok-to-stop d || return 1
    ceph mon ok-to-stop e || return 1

    ceph mon ok-to-add-offline || return 1

    ceph mon ok-to-rm a || return 1
    ceph mon ok-to-rm b || return 1
    ceph mon ok-to-rm c || return 1
    ceph mon ok-to-rm d || return 1
    ceph mon ok-to-rm e || return 1

    kill_daemons $dir KILL mon.e
    wait_for_quorum 60 3

    ceph mon ok-to-stop a || return 1
    ! ceph mon ok-to-stop b || return 1
    ! ceph mon ok-to-stop c || return 1
    ! ceph mon ok-to-stop d || return 1
    ceph mon ok-to-stop e || return 1

    ! ceph mon ok-to-add-offline || return 1

    ceph mon ok-to-rm a || return 1
    ! ceph mon ok-to-rm b || return 1
    ! ceph mon ok-to-rm c || return 1
    ! ceph mon ok-to-rm d || return 1
    ceph mon ok-to-rm e || return 1
}

function TEST_0_mds() {
    local dir=$1

    CEPH_ARGS="$ORIG_CEPH_ARGS --mon-host=$CEPH_MON_A "

    run_mon $dir a --public-addr=$CEPH_MON_A || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_mds $dir a || return 1

    ceph osd pool create meta 1 || return 1
    ceph osd pool create data 1 || return 1
    ceph fs new myfs meta data || return 1
    sleep 5

    ! ceph mds ok-to-stop a || return 1
    ! ceph mds ok-to-stop a dne || return 1
    ceph mds ok-to-stop dne || return 1

    run_mds $dir b || return 1
    sleep 5

    ceph mds ok-to-stop a || return 1
    ceph mds ok-to-stop b || return 1
    ! ceph mds ok-to-stop a b || return 1
    ceph mds ok-to-stop a dne1 dne2 || return 1
    ceph mds ok-to-stop b dne || return 1
    ! ceph mds ok-to-stop a b dne || return 1
    ceph mds ok-to-stop dne1 dne2 || return 1

    kill_daemons $dir KILL mds.a
}

function TEST_0_osd() {
    local dir=$1

    CEPH_ARGS="$ORIG_CEPH_ARGS --mon-host=$CEPH_MON_A "

    run_mon $dir a --public-addr=$CEPH_MON_A || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1

    ceph osd erasure-code-profile set ec-profile m=2 k=2 crush-failure-domain=osd || return 1
    ceph osd pool create ec erasure ec-profile || return 1

    wait_for_clean || return 1

    # with min_size 3, we can stop only 1 osd
    ceph osd pool set ec min_size 3 || return 1
    wait_for_clean || return 1

    ceph osd ok-to-stop 0 || return 1
    ceph osd ok-to-stop 1 || return 1
    ceph osd ok-to-stop 2 || return 1
    ceph osd ok-to-stop 3 || return 1
    ! ceph osd ok-to-stop 0 1 || return 1
    ! ceph osd ok-to-stop 2 3 || return 1
    ceph osd ok-to-stop 0 --max 2 | grep '[0]' || return 1
    ceph osd ok-to-stop 1 --max 2 | grep '[1]' || return 1

    # with min_size 2 we can stop 1 osds
    ceph osd pool set ec min_size 2 || return 1
    wait_for_clean || return 1

    ceph osd ok-to-stop 0 1 || return 1
    ceph osd ok-to-stop 2 3 || return 1
    ! ceph osd ok-to-stop 0 1 2 || return 1
    ! ceph osd ok-to-stop 1 2 3 || return 1

    ceph osd ok-to-stop 0 --max 2 | grep '[0,1]' || return 1
    ceph osd ok-to-stop 0 --max 20 | grep '[0,1]' || return 1
    ceph osd ok-to-stop 2 --max 2 | grep '[2,3]' || return 1
    ceph osd ok-to-stop 2 --max 20 | grep '[2,3]' || return 1

    # we should get the same result with one of the osds already down
    kill_daemons $dir TERM osd.0 || return 1
    ceph osd down 0 || return 1
    wait_for_peered || return 1

    ceph osd ok-to-stop 0 || return 1
    ceph osd ok-to-stop 0 1 || return 1
    ! ceph osd ok-to-stop 0 1 2 || return 1
    ! ceph osd ok-to-stop 1 2 3 || return 1
}

# Wait until the mgr's PGMap - what `osd ok-to-stop` and `osd ok-to-upgrade`
# read, as opposed to the mon's OSDMap that get_osds() reads - reports PG
# pgid with the given acting set (space separated) and a state containing
# the given string, or until a timeout of $WAIT_FOR_CLEAN_TIMEOUT seconds.
function wait_for_pg_acting() {
    local pgid=$1
    local acting="$2"
    local state=${3:-active}
    local -a delays=($(get_timeout_delays $WAIT_FOR_CLEAN_TIMEOUT .1))
    local -i loop=0

    flush_pg_stats || return 1
    while true ; do
        ceph --format json pg dump pgs 2>/dev/null | \
            jq -e --arg pgid "$pgid" --arg acting "$acting" --arg state "$state" \
               '.pg_stats[] | select(.pgid == $pgid)
                | select((.acting | map(tostring) | join(" ")) == $acting)
                | select(.state | contains($state))' > /dev/null && return 0
        (( loop >= ${#delays[*]} )) && return 1
        sleep ${delays[$loop]}
        loop+=1
    done
}

# _check_offlines_pgs() decides whether a PG is affected by the OSDs being
# checked while walking its acting set. A regression (cf54988c504) made that
# decision depend on the *last* member of the acting set only: a PG whose
# last acting OSD was not in the set was skipped altogether, and
# `osd ok-to-stop` answered "safe" for sets that leave the PG below
# min_size. Pin the acting order of a single PG with pg-upmap so that the
# set under test never contains the last acting OSD, and check that the
# answer is still "unsafe" - for a clean PG (acting) and for a degraded one
# (avail_no_missing).
function TEST_0_osd_last_acting_outside_set() {
    local dir=$1
    local poolname=test

    CEPH_ARGS="$ORIG_CEPH_ARGS --mon-host=$CEPH_MON_A "

    run_mon $dir a --public-addr=$CEPH_MON_A || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1

    # one PG, 3 copies on 3 OSDs, min_size 2: no two OSDs can ever be
    # stopped together, one always can
    create_pool $poolname 1 1 || return 1
    ceph osd pool set $poolname size 3 || return 1
    ceph osd pool set $poolname min_size 2 || return 1
    wait_for_clean || return 1

    # pin the acting order to [0, 1, 2]: osd.2 is the last acting member
    local pgid=$(get_pg $poolname obj)
    ceph osd set-require-min-compat-client luminous || return 1
    ceph osd pg-upmap $pgid 0 1 2 || return 1
    wait_for_clean || return 1
    wait_for_pg_acting $pgid "0 1 2" clean || return 1

    ceph osd ok-to-stop 0 || return 1
    ceph osd ok-to-stop 1 || return 1
    ceph osd ok-to-stop 2 || return 1
    # the last acting member (osd.2) is in the set: always evaluated
    ! ceph osd ok-to-stop 1 2 || return 1
    ! ceph osd ok-to-stop 0 2 || return 1
    # the last acting member is NOT in the set: the regression skipped the
    # PG and answered "safe" although one copy would remain
    ! ceph osd ok-to-stop 0 1 || return 1
    ! ceph osd ok-to-stop 0 1 2 || return 1
    # growing the set from osd.0 must not pick up osd.1 either
    local res=$(ceph osd ok-to-stop 0 --max 3 --format=json)
    test $(echo $res | jq '.ok_to_stop') = true || return 1
    test $(echo $res | jq '.osds | length') -eq 1 || return 1

    # the same for a degraded PG, where the walk is over avail_no_missing.
    # A PG is only flagged degraded when it has degraded objects: write some
    # first, or with osd.2 down it would only be undersized and the walk
    # would be over acting again.
    local i
    for i in $(seq 1 10); do
        rados -p $poolname put obj$i /etc/group || return 1
    done
    wait_for_clean || return 1

    kill_daemons $dir TERM osd.2 || return 1
    ceph osd down 2 || return 1
    wait_for_pg_acting $pgid "0 1" degraded || return 1
    # avail_no_missing lists the primary first, then the peers by OSD id:
    # with osd.0 primary it is [0, 1] and osd.1 is the last member
    test "$(ceph --format json pg dump pgs 2>/dev/null | \
        jq -r --arg pgid $pgid '.pg_stats[] | select(.pgid == $pgid)
                                | .avail_no_missing | join(" ")')" = "0 1" || return 1

    ceph osd ok-to-stop 2 || return 1
    ! ceph osd ok-to-stop 1 || return 1
    # osd.1 (last) not in the set: must still be refused
    ! ceph osd ok-to-stop 0 || return 1
    ! ceph osd ok-to-stop 0 2 || return 1
}


main ok-to-stop "$@"
