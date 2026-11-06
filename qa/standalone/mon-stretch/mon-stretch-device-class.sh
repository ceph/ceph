#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
function run() {
    local dir=$1
    shift

    export CEPH_MON_A="127.0.0.1:7157" # git grep '\<7157\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7158" # git grep '\<7158\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7159" # git grep '\<7159\>' : there must be only one
    export CEPH_MON_D="127.0.0.1:7160" # git grep '\<7160\>' : there must be only one
    export CEPH_MON_E="127.0.0.1:7161" # git grep '\<7161\>' : there must be only one
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

# Stretch mode resolves an OSD's zone through the pool's CRUSH rule, so a rule
# pinned to a device class reports the shadow bucket "iris~ssd" while degraded
# stretch mode requires the real "iris". Check that we refuse such a rule
# instead of accepting it and failing much later, when a zone is lost.
TEST_stretch_mode_rejects_device_class_rule() {
    local dir=$1
    local OSDS=4
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
    ceph osd crush move osd.1 host=node-3
    ceph osd crush move osd.2 host=node-4
    ceph osd crush move osd.3 host=node-5

    ceph mon set_location a zone=iris host=node-2
    ceph mon set_location b zone=iris host=node-3
    ceph mon set_location c zone=pze host=node-4
    ceph mon set_location d zone=pze host=node-5

    # pin every OSD to one class, so that the shadow tree spans both zones and
    # a device class rule is a plausible thing for someone to write
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      ceph osd crush rm-device-class osd.$osd || return 1
      ceph osd crush set-device-class ssd osd.$osd || return 1
    done

    hostname=$(hostname -s)
    ceph osd crush remove $hostname || return 1
    ceph osd getcrushmap > crushmap || return 1
    crushtool --decompile crushmap > crushmap.txt || return 1
    sed 's/^# end crush map$//' crushmap.txt > crushmap_modified.txt || return 1
    cat >> crushmap_modified.txt << EOF
rule stretch_rule {
        id 1
        type replicated
        step take iris
        step chooseleaf firstn 2 type host
        step emit
        step take pze
        step chooseleaf firstn 2 type host
        step emit
}
rule stretch_rule_ssd {
        id 2
        type replicated
        step take iris class ssd
        step chooseleaf firstn 2 type host
        step emit
        step take pze class ssd
        step chooseleaf firstn 2 type host
        step emit
}
# end crush map
EOF

    crushtool --compile crushmap_modified.txt -o crushmap.bin || return 1
    ceph osd setcrushmap -i crushmap.bin || return 1

    local stretched_poolname=stretched_rbdpool
    # leave size/min_size alone: enabling stretch mode sets them, and pools that
    # start out non-default are refused before the rule is ever looked at
    ceph osd pool create $stretched_poolname 32 32 stretch_rule || return 1

    ceph mon set_location e zone=arbiter host=node-1 || return 1

    # a device class rule is not a valid rule to enter stretch mode with
    expect_failure $dir "does not support" \
        ceph mon enable_stretch_mode e stretch_rule_ssd zone || return 1

    # ... and refusing it must leave stretch mode off, not half-enabled
    ! ceph osd dump | grep -q "stretch_mode_enabled true" || return 1

    # while the same command with a class-free rule still works
    ceph mon enable_stretch_mode e stretch_rule zone || return 1
    ceph osd dump | grep -q "stretch_mode_enabled true" || return 1

    # nor can a device class be smuggled in by editing the rule the pool
    # already uses and injecting the map
    sed -e 's/step take iris$/step take iris class ssd/' \
        -e 's/step take pze$/step take pze class ssd/' \
        crushmap_modified.txt > crushmap_classed.txt || return 1
    crushtool --compile crushmap_classed.txt -o crushmap_classed.bin || return 1
    expect_failure $dir "does not support" \
        ceph osd setcrushmap -i crushmap_classed.bin || return 1

    # moving a stretch pool onto a device class rule is refused as well, and the
    # complaint names both the class and the pool's own peering barrier
    expect_failure $dir "class ssd" \
        ceph osd pool set $stretched_poolname crush_rule stretch_rule_ssd || return 1
    expect_failure $dir "peer if a zone were lost" \
        ceph osd pool set $stretched_poolname crush_rule stretch_rule_ssd || return 1
    ceph osd pool get $stretched_poolname crush_rule | grep -q stretch_rule$ || return 1

    # but an operator who insists can still do it
    ceph osd pool set $stretched_poolname crush_rule stretch_rule_ssd \
        --yes-i-really-mean-it || return 1
    ceph osd pool get $stretched_poolname crush_rule | grep -q stretch_rule_ssd || return 1

    # with the pool already on a classed rule, further map changes have to keep
    # being accepted, or there is no editing the cluster back out of this state
    sed 's/^# end crush map$//' crushmap_modified.txt > crushmap_benign.txt || return 1
    cat >> crushmap_benign.txt << EOF
rule unrelated_rule {
        id 9
        type replicated
        step take default
        step chooseleaf firstn 0 type host
        step emit
}
# end crush map
EOF
    crushtool --compile crushmap_benign.txt -o crushmap_benign.bin || return 1
    ceph osd setcrushmap -i crushmap_benign.bin || return 1

    # "osd pool stretch set" makes a pool a stretch pool as well, and while the
    # cluster is in stretch mode it gets the same treatment
    expect_failure $dir "does not support" \
        ceph osd pool stretch set $stretched_poolname 2 2 zone \
        stretch_rule_ssd 4 2 || return 1

    # and a pool created now must not land on the classed rule either
    expect_failure $dir "does not support" \
        ceph osd pool create newpool 8 8 stretch_rule_ssd || return 1

    teardown $dir || return 1
}
main mon-stretch-device-class "$@"
