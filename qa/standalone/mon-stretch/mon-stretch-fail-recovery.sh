#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
function run() {
    local dir=$1
    shift

    export CEPH_MON_A="127.0.0.1:7139" # git grep '\<7139\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7141" # git grep '\<7141\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7142" # git grep '\<7142\>' : there must be only one
    export CEPH_MON_D="127.0.0.1:7143" # git grep '\<7143\>' : there must be only one
    export CEPH_MON_E="127.0.0.1:7144" # git grep '\<7144\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "

    export BASE_CEPH_ARGS=$CEPH_ARGS
    CEPH_ARGS+="--mon-host=$CEPH_MON_A"

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}
TEST_stretched_cluster_failover_add_three_osds(){
    local dir=$1
    local OSDS=8
    setup $dir || return 1

    run_mon $dir a --public-addr $CEPH_MON_A || return 1
    wait_for_quorum 300 1 || return 1

    run_mon $dir b --public-addr $CEPH_MON_B || return 1
    CEPH_ARGS="$BASE_CEPH_ARGS --mon-host=$CEPH_MON_A,$CEPH_MON_B"
    wait_for_quorum 300 2 || return 1

    run_mon $dir c --public-addr $CEPH_MON_C || return 1
    CEPH_ARGS="$BASE_CEPH_ARGS --mon-host=$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"
    wait_for_quorum 300 3 || return 1

    run_mon $dir d --public-addr $CEPH_MON_D || return 1
    CEPH_ARGS="$BASE_CEPH_ARGS --mon-host=$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C,$CEPH_MON_D"
    wait_for_quorum 300 4 || return 1

    run_mon $dir e --public-addr $CEPH_MON_E || return 1
    CEPH_ARGS="$BASE_CEPH_ARGS --mon-host=$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C,$CEPH_MON_D,$CEPH_MON_E"
    wait_for_quorum 300 5 || return 1

    ceph mon set election_strategy connectivity
    ceph mon add disallowed_leader e

    run_mgr $dir x || return 1
    run_mgr $dir y || return 1
    run_mgr $dir z || return 1

    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd || return 1
    done
    
    for zone in iris pze
    do
      ceph osd crush add-bucket $zone zone
      ceph osd crush move $zone root=default
    done


    ceph osd crush add-bucket node-2 host
    ceph osd crush add-bucket node-3 host
    ceph osd crush add-bucket node-4 host
    ceph osd crush add-bucket node-5 host

    ceph osd crush move node-2 zone=iris
    ceph osd crush move node-3 zone=iris
    ceph osd crush move node-4 zone=pze
    ceph osd crush move node-5 zone=pze

    ceph osd crush move osd.0 host=node-2
    ceph osd crush move osd.1 host=node-2
    ceph osd crush move osd.2 host=node-3
    ceph osd crush move osd.3 host=node-3
    ceph osd crush move osd.4 host=node-4
    ceph osd crush move osd.5 host=node-4
    ceph osd crush move osd.6 host=node-5
    ceph osd crush move osd.7 host=node-5
    
    ceph mon set_location a zone=iris host=node-2
    ceph mon set_location b zone=iris host=node-3
    ceph mon set_location c zone=pze host=node-4
    ceph mon set_location d zone=pze  host=node-5

    hostname=$(hostname -s)
    ceph osd crush remove $hostname || return 1
    ceph osd getcrushmap > crushmap || return 1
    crushtool --decompile crushmap > crushmap.txt || return 1
    sed 's/^# end crush map$//' crushmap.txt > crushmap_modified.txt || return 1
    cat >> crushmap_modified.txt << EOF
rule stretch_rule {
        id 1
        type replicated
        min_size 1
        max_size 10
        step take iris
        step chooseleaf firstn 2 type host
        step emit
        step take pze
        step chooseleaf firstn 2 type host
        step emit
}

# end crush map
EOF

    crushtool --compile crushmap_modified.txt -o crushmap.bin || return 1
    ceph osd setcrushmap -i crushmap.bin  || return 1
    local stretched_poolname=stretched_rbdpool
    ceph osd pool create $stretched_poolname 32 32 stretch_rule || return 1
    ceph osd pool set $stretched_poolname size 4 || return 1

    sleep 3

    ceph mon set_location e zone=arbiter host=node-1
    ceph mon enable_stretch_mode e stretch_rule zone

    kill_daemons $dir KILL mon.c || return 1
    kill_daemons $dir KILL mon.d || return 1

    kill_daemons $dir KILL osd.4 || return 1
    kill_daemons $dir KILL osd.5 || return 1
    kill_daemons $dir KILL osd.6 || return 1
    kill_daemons $dir KILL osd.7 || return 1

    ceph -s

    sleep 3

    run_osd $dir 8 || return 1
    run_osd $dir 9 || return 1
    run_osd $dir 10 || return 1

    ceph -s

    sleep 3

    teardown $dir || return 1
}
main mon-stretch-fail-recovery "$@"