#!/usr/bin/env bash
#
# Per-pool num_zones before and at the upgrade commit: until
# require_osd_release reaches umbrella the monitors keep the behaviour of
# earlier releases, and the commit converts the pool creation defaults of a
# cluster in stretch mode.

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON_A="127.0.0.1:7185" # git grep '\<7185\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7186" # git grep '\<7186\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7187" # git grep '\<7187\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C "
    CEPH_ARGS+="--mon-debug-no-require-umbrella "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function pool_field() {
    ceph osd pool ls detail -f json | jq --arg p $1 ".[]|select(.pool_name==\$p)|.$2"
}

function pool_num_zones() {
    ceph osd pool get $1 num_zones -f json | jq .num_zones
}

function run_mons() {
    local dir=$1
    shift

    run_mon $dir a --public-addr $CEPH_MON_A "$@" || return 1
    run_mon $dir b --public-addr $CEPH_MON_B "$@" || return 1
    run_mon $dir c --public-addr $CEPH_MON_C "$@" || return 1
    wait_for_quorum 300 3 || return 1
    test "$(ceph osd dump -f json | jq -r .require_osd_release)" = tentacle || return 1
}

# Test what waits for the commit
function TEST_commands_wait_for_commit() {
    local dir=$1
    run_mons $dir || return 1

    expect_failure $dir "require-osd-release umbrella" \
        ceph osd pool create early 8 8 || return 1
    expect_failure $dir "errno 1\]" \
        python3 -c "
import rados
with rados.Rados(conffile='$dir/ceph.conf') as r:
    r.create_pool('early')
" || return 1
    # the defaults can always be shown
    ceph osd pool default get || return 1
    expect_failure $dir "require-osd-release umbrella" \
        ceph osd pool default set --replica 2 || return 1

    # the debug option lets single-zone pools through for tests
    ceph config set mon mon_debug_allow_pool_create_before_commit true || return 1
    ceph osd pool create early 8 8 || return 1
    expect_failure $dir "cannot be created until" \
        ceph osd pool create early2 8 8 --num_zones 2 --zone_failure_domain host || return 1
    expect_failure $dir "require-osd-release umbrella" \
        ceph osd pool set early num_zones 1 || return 1
    ceph osd erasure-code-profile set ec21 plugin=isa k=2 m=1 || return 1
    ceph osd pool create ecearly 8 8 erasure ec21 || return 1
    expect_failure $dir "cannot be stretched until" \
        ceph osd pool stretch set ecearly 2 2 host replicated_rule 6 2 || return 1
    ceph config rm mon mon_debug_allow_pool_create_before_commit || return 1

    ceph osd require-osd-release umbrella --yes-i-really-mean-it || return 1
    ceph osd pool create late 8 8 || return 1
    ceph osd pool default get || return 1
    ceph osd pool set late num_zones 1 || return 1
}

# zones iris (mon.a, osd.0, osd.1) and pze (mon.b, osd.2, osd.3), tiebreaker mon.c
function stretchable_cluster() {
    local dir=$1

    run_mons $dir --mon-debug-allow-pool-create-before-commit=true || return 1
    local zone host osd
    for zone in iris pze; do
        ceph osd crush add-bucket $zone zone || return 1
        ceph osd crush move $zone root=default || return 1
    done
    for host in 2 3 4 5; do
        ceph osd crush add-bucket node-$host host || return 1
    done
    ceph osd crush move node-2 zone=iris || return 1
    ceph osd crush move node-3 zone=iris || return 1
    ceph osd crush move node-4 zone=pze || return 1
    ceph osd crush move node-5 zone=pze || return 1
    ceph mon set_location a zone=iris host=node-2 || return 1
    ceph mon set_location b zone=pze host=node-4 || return 1
    ceph mon set_location c zone=arbiter host=node-1 || return 1
    ceph mon set election_strategy connectivity || return 1
    # so that enabling stretch mode does not cause an election
    ceph mon add disallowed_leader c || return 1
    run_mgr $dir x || return 1
    for osd in 0 1 2 3; do
        run_osd $dir $osd || return 1
    done
    ceph osd crush move osd.0 host=node-2 || return 1
    ceph osd crush move osd.1 host=node-3 || return 1
    ceph osd crush move osd.2 host=node-4 || return 1
    ceph osd crush move osd.3 host=node-5 || return 1
    ceph osd crush remove $(hostname -s) || return 1

    ceph osd getcrushmap > $dir/crushmap || return 1
    crushtool --decompile $dir/crushmap > $dir/crushmap.txt || return 1
    sed 's/^# end crush map$//' $dir/crushmap.txt > $dir/crushmap_modified.txt || return 1
    cat >> $dir/crushmap_modified.txt << EOF
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

# end crush map
EOF
    crushtool --compile $dir/crushmap_modified.txt -o $dir/crushmap.bin || return 1
    ceph osd setcrushmap -i $dir/crushmap.bin || return 1
}

# Test enable_stretch_mode behaves as on main before the commit, and the
# commit turns its stretch mode into pool creation defaults
function TEST_legacy_stretch_mode_then_commit() {
    local dir=$1
    stretchable_cluster $dir || return 1
    ceph osd pool create rep 8 8 replicated || return 1
    ceph osd erasure-code-profile set ec21 plugin=isa k=2 m=1 \
        crush-failure-domain=osd || return 1
    ceph osd pool create ec 8 8 erasure ec21 || return 1
    wait_for_clean || return 1

    # as on main: no EC pools in stretch mode
    expect_failure $dir "stretched pools must be replicated" \
        ceph mon enable_stretch_mode c stretch_rule zone || return 1
    ceph osd pool rm ec ec --yes-i-really-really-mean-it || return 1

    ceph mon enable_stretch_mode c stretch_rule zone || return 1
    test "$(pool_field rep size)" = 4 || return 1
    test "$(pool_field rep crush_rule)" = 1 || return 1
    test "$(ceph mon dump -f json | jq .stretch_mode)" = true || return 1
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = true || return 1
    expect_failure $dir "already engaged" \
        ceph mon enable_stretch_mode c stretch_rule zone || return 1
    wait_for_clean || return 1

    ceph osd require-osd-release umbrella || return 1
    test "$(ceph config get mon osd_pool_default_num_zones)" = 2 || return 1
    test "$(ceph config get mon osd_pool_default_replica)" = 2 || return 1
    test "$(ceph config get mon osd_pool_default_zone_failure_domain)" = zone || return 1
    test "$(pool_num_zones rep)" = 2 || return 1
    test "$(pool_field rep size)" = 4 || return 1
    test "$(pool_field rep min_size)" = 1 || return 1

    # after the commit a new pool takes the converted defaults
    ceph osd pool create newrep 8 8 || return 1
    test "$(pool_num_zones newrep)" = 2 || return 1
    test "$(pool_field newrep size)" = 4 || return 1
    wait_for_clean || return 1

    # and disable_stretch_mode follows the pools
    ceph mon disable_stretch_mode --yes-i-really-mean-it || return 1
    test "$(pool_num_zones rep)" = 1 || return 1
    test "$(pool_field rep size)" = 3 || return 1
    test "$(ceph config get mon osd_pool_default_num_zones)" = 1 || return 1
    test "$(ceph config get mon osd_pool_default_replica)" = 3 || return 1
    wait_for_clean || return 1
}

# Test the commit leaves the defaults of a cluster without stretch mode alone
function TEST_commit_without_stretch_mode() {
    local dir=$1
    run_mons $dir || return 1

    ceph osd require-osd-release umbrella --yes-i-really-mean-it || return 1
    test "$(ceph osd pool default get -f json | jq -r .num_zones.source)" = default || return 1
    test "$(ceph osd pool default get -f json | jq -r .replica.source)" = default || return 1
}

main mon-stretch-upgrade "$@"

# Local Variables:
# compile-command: "cd ../../../build ; make -j4 && ../qa/run-standalone.sh mon-stretch-upgrade.sh"
# End:
