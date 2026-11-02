#!/usr/bin/env bash
#
# Per-pool num_zones before and at the upgrade commit: until
# require_osd_release reaches vampire, whose update sets the OSDMap flag
# stretch_zones, the monitors keep the behavior of earlier releases; the commit
# converts the pools and pool creation defaults of a cluster in stretch mode.

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON_A="127.0.0.1:7185" # git grep '\<7185\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7186" # git grep '\<7186\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7187" # git grep '\<7187\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C "
    CEPH_ARGS+="--mon-debug-no-require-vampire "

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

function committed() {
    ceph osd dump -f json | jq -r .flags | grep -q stretch_zones
}

# expect pool $1 to have num_zones $2, replica $3, size $4 and min_size $5
function expect_pool() {
    local got=$(ceph osd pool ls detail -f json | jq -c --arg p $1 \
        '.[]|select(.pool_name==$p)|[.num_zones,.replica,.size,.min_size]')
    if [ "$got" != "[$2,$3,$4,$5]" ]; then
        echo "pool $1: [num_zones,replica,size,min_size] is $got, expected [$2,$3,$4,$5]"
        return 1
    fi
}

# start an existing monitor again, as run_mon does after --mkfs
function restart_mon() {
    local dir=$1
    local id=$2
    shift 2

    ceph-mon \
        --id $id \
        --osd-failsafe-full-ratio=.99 \
        --mon-osd-full-ratio=.99 \
        --mon-data-avail-crit=1 \
        --mon-data-avail-warn=5 \
        --paxos-propose-interval=0.1 \
        --osd-crush-chooseleaf-type=0 \
        $EXTRA_OPTS \
        --debug-mon 20 \
        --debug-ms 20 \
        --debug-paxos 20 \
        --chdir= \
        --mon-data=$dir/$id \
        --log-file=$dir/\$name.log \
        --admin-socket=$(get_asok_path) \
        --mon-cluster-log-file=$dir/log \
        --run-dir=$dir \
        --pid-file=$dir/\$name.pid \
        --mon-allow-pool-delete \
        --mon-allow-pool-size-one \
        --osd-pool-default-pg-autoscale-mode off \
        --mon-osd-backfillfull-ratio .99 \
        --mon-warn-on-insecure-global-id-reclaim-allowed=false \
        "$@" || return 1
}

function wait_for_stretch_state() {
    local degraded=$1
    local recovering=$2
    for i in $(seq 1 120); do
        local s=$(ceph osd dump -f json | jq -c '.stretch_mode|[.degraded_stretch_mode,.recovering_stretch_mode]')
        if [ "$s" == "[$degraded,$recovering]" ]; then
            return 0
        fi
        sleep 2
    done
    return 1
}

function run_mons() {
    local dir=$1
    shift

    run_mon $dir a --public-addr $CEPH_MON_A "$@" || return 1
    run_mon $dir b --public-addr $CEPH_MON_B "$@" || return 1
    run_mon $dir c --public-addr $CEPH_MON_C "$@" || return 1
    wait_for_quorum 300 3 || return 1
    test "$(ceph osd dump -f json | jq -r .require_osd_release)" = umbrella || return 1
}

# Test what waits for the commit: what earlier releases cannot express
function TEST_commands_wait_for_commit() {
    local dir=$1
    run_mons $dir || return 1
    ! committed || return 1

    ceph osd pool create early 8 8 || return 1
    # librados creates pools as earlier releases do, for example for RGW
    librados_create_pool $dir rados_early || return 1
    test "$(pool_field rados_early size)" = 3 || return 1
    # the defaults can always be shown
    ceph osd pool default get || return 1
    expect_failure $dir "require-osd-release vampire" \
        ceph osd pool default set --replica 2 || return 1

    expect_failure $dir "cannot be created until" \
        ceph osd pool create early2 8 8 --num_zones 2 --zone_failure_domain host || return 1
    # over the default two zones too, a pool is created in one zone, with its
    # legacy size
    ceph config set mon osd_pool_default_num_zones 2 || return 1
    ceph osd pool create early3 8 8 --size 4 || return 1
    test "$(pool_num_zones early3)" = 1 || return 1
    test "$(pool_field early3 size)" = 4 || return 1
    ceph config rm mon osd_pool_default_num_zones || return 1
    expect_failure $dir "require-osd-release vampire" \
        ceph osd pool set early num_zones 1 || return 1
    ceph osd erasure-code-profile set ec21 plugin=isa k=2 m=1 || return 1
    ceph osd pool create ecearly 8 8 erasure ec21 || return 1
    expect_failure $dir "not supported for EC pools" \
        ceph osd pool stretch set ecearly 2 2 host replicated_rule 6 2 || return 1

    ceph osd require-osd-release vampire --yes-i-really-mean-it || return 1
    committed || return 1
    ceph osd pool create late 8 8 || return 1
    ceph osd pool default get || return 1
    ceph osd pool set late num_zones 1 || return 1
}

# zones iris (mon.a, osd.0, osd.1) and pze (mon.b, osd.2, osd.3), tiebreaker
# mon.c
function stretchable_cluster() {
    local dir=$1

    run_mons $dir || return 1
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
    # keep restarted OSDs in their zones
    ceph config set osd osd_crush_update_on_start false || return 1

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

# Test enable_stretch_mode and pool creation in stretch mode behave as on main
# before the commit, and the commit turns its stretch mode into pools over two
# zones and pool creation defaults
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

    # unlike main, an individual stretch pool is not taken over
    ceph osd pool create ind 8 8 replicated || return 1
    ceph osd pool stretch set ind 2 2 zone replicated_rule 4 2 || return 1
    expect_failure $dir "legacy individual stretch pool" \
        ceph mon enable_stretch_mode c stretch_rule zone || return 1
    test "$(pool_num_zones ind)" = 1 || return 1
    test "$(ceph mon dump -f json | jq .stretch_mode)" = false || return 1
    ceph osd pool stretch unset ind replicated_rule 3 2 || return 1

    ceph mon enable_stretch_mode c stretch_rule zone || return 1
    test "$(pool_field ind size)" = 4 || return 1
    expect_pool rep 1 4 4 2 || return 1
    test "$(pool_field rep crush_rule)" = 1 || return 1
    test "$(ceph mon dump -f json | jq .stretch_mode)" = true || return 1
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = true || return 1
    ceph mon enable_stretch_mode c stretch_rule zone > $dir/out 2>&1 || return 1
    grep -q "already engaged" $dir/out || return 1
    # created as main creates it in stretch mode
    ceph osd pool create early 8 8 || return 1
    expect_pool early 1 4 4 2 || return 1
    test "$(pool_field early crush_rule)" = 1 || return 1
    test "$(pool_field early peering_crush_bucket_count)" = 2 || return 1
    # the options of pools over zones wait for the commit
    expect_failure $dir "cannot be created until" \
        ceph osd pool create early2 --num_zones 2 --replica 2 || return 1
    wait_for_clean || return 1
    rados -p rep put obj $dir/crushmap.bin || return 1
    local resend=$(pool_field rep last_force_op_resend | tr -d '"')

    ceph osd require-osd-release vampire || return 1
    committed || return 1
    test "$(ceph config get mon osd_pool_default_num_zones)" = 2 || return 1
    test "$(ceph config get mon osd_pool_default_size)" = 2 || return 1
    test "$(ceph config get mon osd_pool_default_zone_failure_domain)" = zone || return 1
    expect_pool rep 2 2 4 1 || return 1
    expect_pool early 2 2 4 1 || return 1
    # clients resend, as the peering rules change
    test "$(pool_field rep last_force_op_resend | tr -d '"')" -gt "$resend" || return 1
    # tools that apply the settings again keep working
    ceph osd pool set rep size 4 || return 1
    expect_failure $dir "use 'replica'" ceph osd pool set rep size 6 || return 1
    wait_for_clean || return 1
    rados -p rep get obj $dir/obj || return 1
    cmp $dir/crushmap.bin $dir/obj || return 1

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
    test "$(ceph config get mon osd_pool_default_size)" = 3 || return 1
    wait_for_clean || return 1
}

# Test librados creates pools before the commit as earlier releases do, also
# in stretch mode, and the commit gives them their zones
function TEST_librados_pool_before_commit() {
    local dir=$1
    stretchable_cluster $dir || return 1
    librados_create_pool $dir rep || return 1
    wait_for_clean || return 1

    ceph mon enable_stretch_mode c stretch_rule zone || return 1
    librados_create_pool $dir early || return 1
    expect_pool early 1 4 4 2 || return 1

    ceph osd require-osd-release vampire || return 1
    expect_pool early 2 2 4 1 || return 1
    librados_create_pool $dir late || return 1
    test "$(pool_num_zones late)" = 2 || return 1
    test "$(pool_field late size)" = 4 || return 1
}

# Test the commit gives a pool that uses the default profile a copy of it
function TEST_commit_copies_default_profile() {
    local dir=$1
    run_mons $dir || return 1

    ceph osd pool create ecdefault 8 8 erasure || return 1
    test "$(pool_field ecdefault erasure_code_profile)" = '"default"' || return 1
    ceph osd require-osd-release vampire --yes-i-really-mean-it || return 1
    test "$(pool_field ecdefault erasure_code_profile)" = '"ecdefault-k2-m2"' || return 1
    test "$(ceph osd erasure-code-profile get ecdefault-k2-m2 -f json)" = \
        "$(ceph osd erasure-code-profile get default -f json)" || return 1
}

# Test the commit's copies of the default profile follow the pools of its
# OSDMap update: a pool created before the commit in that update gets one, a
# pool deleted in it none, and a pool created after it waits for the commit
function TEST_commit_copies_default_profile_in_its_update() {
    local dir=$1
    run_mons $dir || return 1

    ceph osd pool create gone 8 8 erasure || return 1
    ceph tell mon.\* config set paxos_propose_interval 5 || return 1
    ceph osd pool rm gone gone --yes-i-really-really-mean-it &
    local pid_rm=$!
    ceph osd pool create fresh 8 8 erasure &
    local pid_fresh=$!
    sleep 1
    ceph osd require-osd-release vampire --yes-i-really-mean-it &
    local pid_commit=$!
    sleep 1
    ceph osd pool create late 8 8 erasure || return 1
    wait $pid_rm || return 1
    wait $pid_fresh || return 1
    wait $pid_commit || return 1

    test "$(pool_field fresh erasure_code_profile)" = '"fresh-k2-m2"' || return 1
    test "$(pool_field late erasure_code_profile)" = '"late-k2-m2"' || return 1
    ! ceph osd erasure-code-profile ls | grep -q '^gone-' || return 1
}

# Test a legacy stretch mode min_size that the zones do not divide rounds up
# per zone at the commit, so the pool never takes writes with fewer copies
# than asked for
function TEST_legacy_stretch_mode_min_size_rounds_up() {
    local dir=$1
    stretchable_cluster $dir || return 1
    ceph config set mon mon_stretch_pool_min_size 3 || return 1
    ceph osd pool create rep 8 8 replicated || return 1

    ceph mon enable_stretch_mode c stretch_rule zone || return 1
    expect_pool rep 1 4 4 3 || return 1
    ceph osd require-osd-release vampire || return 1
    expect_pool rep 2 2 4 2 || return 1
}

# Test a commit while degraded: before it the monitors halve min_size as
# earlier releases do; at it min_size per zone comes from
# mon_stretch_pool_min_size, and a pool created while degraded gets the
# surviving zone as its mandatory member, so writes go on in that zone
function TEST_degraded_commit() {
    local dir=$1
    stretchable_cluster $dir || return 1
    ceph osd pool create rep 8 8 replicated || return 1
    ceph mon enable_stretch_mode c stretch_rule zone || return 1
    wait_for_clean || return 1

    kill_daemons $dir KILL mon.b || return 1
    kill_daemons $dir KILL osd.2 || return 1
    kill_daemons $dir KILL osd.3 || return 1
    ceph osd down osd.2 osd.3
    wait_for_stretch_state 1 0 || return 1
    expect_pool rep 1 4 4 1 || return 1
    ceph osd pool create early 8 8 || return 1
    expect_pool early 1 4 4 1 || return 1
    test "$(pool_field early peering_crush_bucket_count)" = 1 || return 1

    ceph osd require-osd-release vampire || return 1
    local iris=$(ceph osd crush dump -f json | jq '.buckets[]|select(.name=="iris")|.id')
    for pool in rep early; do
        expect_pool $pool 2 2 4 1 || return 1
        test "$(pool_field $pool peering_crush_bucket_mandatory_member)" = $iris || return 1
        timeout 120 rados -p $pool put obj $dir/crushmap.bin || return 1
    done

    restart_mon $dir b --public-addr $CEPH_MON_B || return 1
    wait_for_quorum 300 3 || return 1
    activate_osd $dir 2 || return 1
    activate_osd $dir 3 || return 1
    wait_for_stretch_state 0 0 || return 1
    # no longer restored, as the pools count min_size per zone
    for pool in rep early; do
        expect_pool $pool 2 2 4 1 || return 1
    done
    wait_for_clean || return 1
    for pool in rep early; do
        rados -p $pool get obj $dir/obj || return 1
        cmp $dir/crushmap.bin $dir/obj || return 1
    done
}

# Test the commit leaves the defaults of a cluster without stretch mode alone
# Test the warning for a pool of stretch mode that the commit cannot split
# over two zones: before the commit, while it can be changed, and after it
function TEST_commit_warns_of_unconverted_pools() {
    local dir=$1
    stretchable_cluster $dir || return 1
    ceph osd pool create rep 8 8 replicated || return 1
    ceph osd pool create odd 8 8 replicated || return 1
    ceph mon enable_stretch_mode c stretch_rule zone || return 1
    ceph osd pool set odd size 3 || return 1
    wait_for_health "will keep one zone when the upgrade is committed" || return 1
    ceph health detail > $dir/health || return 1
    grep -q "^\[WRN\] STRETCH_MODE_POOL_NOT_CONVERTED: 1 stretch pool(s)" $dir/health || return 1
    grep -q "pool 'odd' has size 3" $dir/health || return 1
    ! grep -q "pool 'rep'" $dir/health || return 1

    ceph osd require-osd-release vampire || return 1
    expect_pool rep 2 2 4 1 || return 1
    expect_pool odd 1 3 3 2 || return 1
    wait_for_health "kept one zone when the upgrade was committed" || return 1
    ceph health detail > $dir/health || return 1
    grep -q "pool 'odd' has size 3" $dir/health || return 1
    ! grep -q "pool 'rep'" $dir/health || return 1

    ceph mon disable_stretch_mode --yes-i-really-mean-it || return 1
    test "$(pool_num_zones rep)" = 1 || return 1
    test "$(pool_field odd peering_crush_bucket_count)" = 2 || return 1
}

function TEST_commit_without_stretch_mode() {
    local dir=$1
    run_mons $dir || return 1

    ceph osd require-osd-release vampire --yes-i-really-mean-it || return 1
    test "$(ceph osd pool default get -f json | jq -r .num_zones.source)" = default || return 1
    test "$(ceph osd pool default get -f json | jq -r .replica.source)" = default || return 1
}

main mon-stretch-upgrade "$@"

# Local Variables:
# compile-command: "cd ../../../build ; make -j4 && ../qa/run-standalone.sh mon-stretch-upgrade.sh"
# End:
