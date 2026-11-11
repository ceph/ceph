#!/usr/bin/env bash
#
# Per-pool num_zones before and at the upgrade commit: until
# require_osd_release reaches vampire, whose update sets the OSDMap flag
# stretch_zones, the monitors keep the behavior of earlier releases; the commit
# converts the pools and pool creation defaults of a cluster in global
# stretch mode.

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

function run_mons() {
    local dir=$1
    shift

    run_mon $dir a --public-addr $CEPH_MON_A "$@" || return 1
    run_mon $dir b --public-addr $CEPH_MON_B "$@" || return 1
    run_mon $dir c --public-addr $CEPH_MON_C "$@" || return 1
    wait_for_quorum 300 3 || return 1
    test "$(ceph osd dump -f json | jq -r .require_osd_release)" = umbrella || return 1
}

# Until osd require-osd-release vampire, the upgrade commit, the monitors
# refuse what earlier releases cannot express and otherwise act as those
# releases do. Before the commit, with monitors only: a pool created
# through librados, as RGW creates its pools, gets 3 copies; osd pool
# default get works but osd pool default set is refused; osd pool create
# with --num_zones 2 is refused, and with osd_pool_default_num_zones 2 set
# the pool is created in one zone with the size it is given; osd pool set
# <pool> num_zones 1 is refused; and osd pool stretch set is still refused
# for an erasure coded pool. The commit sets the stretch_zones flag and adds
# crush-zone-failure-domain=datacenter and crush-root=default to the
# default erasure code profile. After it, a new pool can be set to
# num_zones 1.
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

    # the default profile gets the placement of erasure coded pools at the
    # commit
    ! ceph osd erasure-code-profile get default | grep -q crush-zone-failure-domain || return 1
    ceph osd require-osd-release vampire --yes-i-really-mean-it || return 1
    committed || return 1
    ceph osd erasure-code-profile get default | \
        grep -qx crush-zone-failure-domain=datacenter || return 1
    ceph osd erasure-code-profile get default | grep -qx crush-root=default || return 1
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
    # so that mon enable_stretch_mode does not cause an election
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

# Before the upgrade commit, mon enable_stretch_mode and pool creates in
# global stretch mode act as in earlier releases; the commit turns that
# global stretch mode into pools over two zones and pool creation defaults.
# The zones are iris and pze, of type zone, with 2 OSDs each, and
# stretch_rule takes 2 hosts in each. mon enable_stretch_mode is refused
# while there is an erasure coded pool, and while pool ind is an individual
# stretch pool (osd pool stretch set). It then gives the replicated pools 4
# copies on stretch_rule, and so does osd pool create of pool early; a
# create with --num_zones 2 is refused. osd require-osd-release vampire sets
# the pool creation defaults to two zones of type zone with 2 copies in
# each, makes rep and early pools over two zones of 2 copies each, with
# min_size 1, and makes clients resend their ops. osd pool set rep size 4
# still works, size 6 is refused, and an object written to rep before the
# commit reads back. A pool created after the commit is over two zones, and
# mon disable_stretch_mode moves rep to one zone of 3 copies and resets the
# defaults.
function TEST_legacy_stretch_mode_then_commit() {
    local dir=$1
    stretchable_cluster $dir || return 1
    ceph osd pool create rep 8 8 replicated || return 1
    ceph osd erasure-code-profile set ec21 plugin=isa k=2 m=1 \
        crush-failure-domain=osd || return 1
    ceph osd pool create ec 8 8 erasure ec21 || return 1
    wait_for_clean || return 1

    # as on main: no erasure coded pools in global stretch mode
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
    # created as main creates it in global stretch mode
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
    ceph osd erasure-code-profile get default | \
        grep -qx crush-zone-failure-domain=zone || return 1
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

# Before the upgrade commit, a pool created through librados, as RGW
# creates its pools, gets what earlier releases give it, in global stretch
# mode too, and the commit puts it over two zones. The zones are iris and
# pze, with 2 OSDs each. After mon enable_stretch_mode with stretch_rule (2
# hosts in each zone), pool early, created through librados, gets size 4
# and min_size 2. osd require-osd-release vampire makes early a pool over
# two zones of 2 copies each, and pool late, created through librados after
# the commit, is over two zones with size 4.
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

# The upgrade commit adds crush-zone-failure-domain and crush-root to the
# default erasure code profile, but a pool created on the default profile
# must keep the profile it was created with, so the commit first gives the
# pool a copy of it. Here erasure coded pool ecdefault is created on the
# default profile (k=2 m=2), with monitors only. After osd
# require-osd-release vampire, ecdefault uses ecdefault-k2-m2, which is the
# default profile as it was, and the default profile has
# crush-zone-failure-domain=datacenter.
function TEST_commit_copies_default_profile() {
    local dir=$1
    run_mons $dir || return 1

    ceph osd pool create ecdefault 8 8 erasure || return 1
    test "$(pool_field ecdefault erasure_code_profile)" = '"default"' || return 1
    local before="$(ceph osd erasure-code-profile get default -f json)"
    ceph osd require-osd-release vampire --yes-i-really-mean-it || return 1
    test "$(pool_field ecdefault erasure_code_profile)" = '"ecdefault-k2-m2"' || return 1
    test "$(ceph osd erasure-code-profile get ecdefault-k2-m2 -f json)" = \
        "$before" || return 1
    test "$(ceph osd erasure-code-profile get default -f json | \
            jq -r '."crush-zone-failure-domain"')" = datacenter || return 1
}

# The upgrade commit's copies of the default erasure code profile must
# follow the pools of the OSDMap update that the commit is in. With
# proposals delayed, the test sends, one after another, the removal of
# erasure coded pool gone, the create of erasure coded pool fresh on the
# default profile, osd require-osd-release vampire and the create of
# erasure coded pool late. fresh, created before the commit, gets a copy,
# fresh-k2-m2; gone, removed, gets none; and late, created after the
# commit, gets late-k2-m2.
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

# The upgrade commit turns a min_size of global stretch mode, which counts
# the copies in both zones, into a min_size in each zone, rounding up when
# the two zones do not divide it, so that the pool never takes writes with
# fewer copies than asked for. With mon_stretch_pool_min_size 3, mon
# enable_stretch_mode gives replicated pool rep 4 copies and min_size 3.
# After osd require-osd-release vampire, rep is over two zones of 2 copies
# each, with min_size 2.
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

# The upgrade commit while the cluster is in degraded global stretch mode.
# Replicated pool rep has 4 copies on stretch_rule (2 hosts in each of the
# zones iris and pze). With pze's monitor and OSDs down, the monitors halve
# rep's min_size to 1, as earlier releases do, and pool early, created
# then, gets the same and peers in one zone. osd require-osd-release
# vampire makes both pools over two zones of 2 copies each, with min_size 1
# in each zone (mon_stretch_pool_min_size, 2, split between the zones), and
# makes iris, the zone that is up, the one they must peer in, so both take
# a write. When pze comes back, the cluster returns to normal mode,
# min_size stays 1, as it now counts each zone, and the objects read back
# once the PGs are clean.
function TEST_degraded_commit() {
    local dir=$1
    stretchable_cluster $dir || return 1
    ceph osd pool create rep 8 8 replicated || return 1
    ceph mon enable_stretch_mode c stretch_rule zone || return 1
    wait_for_clean || return 1

    kill_daemons $dir KILL mon.b || return 1
    kill_daemons $dir KILL osd.2 || return 1
    kill_daemons $dir KILL osd.3 || return 1
    ceph osd down osd.2 osd.3 || return 1
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

    activate_mon $dir b --public-addr $CEPH_MON_B || return 1
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

# STRETCH_MODE_POOL_NOT_CONVERTED warns, before the upgrade commit and
# after it, of each pool of global stretch mode that the commit cannot put
# over two zones. mon enable_stretch_mode gives replicated pools rep and
# odd 4 copies on stretch_rule (2 hosts in each of the zones iris and pze);
# odd is then set to size 3, which two zones cannot divide, so the warning
# names odd and not rep. Pool fix, created after the enable, is warned of
# too once moved to replicated_rule, and no longer once moved back to
# stretch_rule. After osd require-osd-release vampire, rep and fix are over
# two zones of 2 copies each, odd keeps one zone and size 3, and the
# warning says that odd kept one zone. mon disable_stretch_mode is refused
# until osd pool set odd num_zones 2 --replica 2 puts odd over two zones,
# which clears the warning; it then moves rep and odd to one zone.
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

    # a new rule still applies before the commit, and lets it give fix zones
    ceph osd pool create fix 8 8 replicated || return 1
    ceph osd pool set fix crush_rule replicated_rule || return 1
    wait_for_health "2 stretch pool(s) will keep one zone" || return 1
    ceph osd pool set fix crush_rule stretch_rule || return 1
    wait_for_health "1 stretch pool(s) will keep one zone" || return 1

    ceph osd require-osd-release vampire || return 1
    expect_pool rep 2 2 4 1 || return 1
    expect_pool fix 2 2 4 1 || return 1
    expect_pool odd 1 3 3 2 || return 1
    wait_for_health "kept one zone when the upgrade was committed" || return 1
    ceph health detail > $dir/health || return 1
    grep -q "pool 'odd' has size 3" $dir/health || return 1
    ! grep -q "pool 'rep'" $dir/health || return 1
    ! grep -q "pool 'fix'" $dir/health || return 1

    # disabling would leave odd stretched, so it waits until odd has zones
    expect_failure $dir "pool(s) 'odd' kept one zone when the upgrade was committed" \
        ceph mon disable_stretch_mode --yes-i-really-mean-it || return 1
    ceph osd pool set odd num_zones 2 --replica 2 || return 1
    expect_pool odd 2 2 4 1 || return 1
    test "$(pool_field odd peering_crush_bucket_count)" = 2 || return 1
    local i
    for i in $(seq 1 30); do
        ! ceph health detail | grep -q STRETCH_MODE_POOL_NOT_CONVERTED && break
        sleep 1
    done
    ! ceph health detail | grep -q STRETCH_MODE_POOL_NOT_CONVERTED || return 1

    ceph mon disable_stretch_mode --yes-i-really-mean-it || return 1
    test "$(pool_num_zones rep)" = 1 || return 1
    test "$(pool_num_zones odd)" = 1 || return 1
    test "$(pool_field odd peering_crush_bucket_count)" = 0 || return 1
}

# osd pool set <pool> num_zones 1 turns a pool that kept one zone at the
# upgrade commit into an ordinary pool in one zone. Replicated pool odd,
# set to size 3 in global stretch mode, keeps one zone at osd
# require-osd-release vampire. osd pool set odd crush_rule replicated_rule
# is refused, as odd would still peer over both zones, and odd stays on
# stretch_rule. osd pool set odd num_zones 1 keeps its 3 copies, puts it on
# replicated_rule, as mon disable_stretch_mode would, and stops it peering
# over the zones, which clears STRETCH_MODE_POOL_NOT_CONVERTED. Pool rep,
# over two zones, keeps the OSDMap's stretch flag set, and mon
# disable_stretch_mode then moves it to one zone.
function TEST_unconverted_pool_to_one_zone() {
    local dir=$1
    stretchable_cluster $dir || return 1
    ceph osd pool create rep 8 8 replicated || return 1
    ceph osd pool create odd 8 8 replicated || return 1
    ceph mon enable_stretch_mode c stretch_rule zone || return 1
    ceph osd pool set odd size 3 || return 1
    ceph osd require-osd-release vampire || return 1
    expect_pool odd 1 3 3 2 || return 1
    wait_for_health "kept one zone when the upgrade was committed" || return 1

    # a new rule alone would leave odd peering over both zones
    expect_failure $dir "is still stretched" \
        ceph osd pool set odd crush_rule replicated_rule || return 1
    test "$(pool_field odd crush_rule)" = 1 || return 1

    # 3 replicas on the default rule, as disable_stretch_mode gives
    ceph osd pool set odd num_zones 1 || return 1
    expect_pool odd 1 3 3 2 || return 1
    test "$(pool_field odd peering_crush_bucket_count)" = 0 || return 1
    test "$(pool_field odd crush_rule)" = 0 || return 1
    local i
    for i in $(seq 1 30); do
        ! ceph health detail | grep -q STRETCH_MODE_POOL_NOT_CONVERTED && break
        sleep 1
    done
    ! ceph health detail | grep -q STRETCH_MODE_POOL_NOT_CONVERTED || return 1
    # rep keeps the OSDMap's stretch flag set
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = true || return 1

    ceph mon disable_stretch_mode --yes-i-really-mean-it || return 1
    test "$(pool_num_zones rep)" = 1 || return 1
}

# Without global stretch mode, osd require-osd-release vampire leaves the
# pool creation defaults alone: num_zones and replica keep their built-in
# defaults.
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
