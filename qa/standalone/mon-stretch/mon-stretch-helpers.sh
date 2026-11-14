# Helpers for the qa/standalone/mon-stretch tests; sourced, not run.

# CRUSH_ITEM_NONE: the peering_crush_bucket_mandatory_member of a pool with none
CRUSH_ITEM_NONE=2147483647

# Run the TEST_ functions given (or all of them) with a fresh cluster each.
# The caller exports CEPH_MON (the --mon-host list) and, for each monitor it
# starts with run_mon --public-addr, CEPH_MON_A, CEPH_MON_B and CEPH_MON_C.
function run_stretch_tests() {
    local dir=$1
    shift

    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    CEPH_ARGS+="--mon_stretch_recovery_min_wait=5 "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
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
    ceph osd dump -f json | jq '.stretch_mode'
    ceph -s
    return 1
}

function pool_field() {
    ceph osd pool ls detail -f json | jq --arg p $1 ".[]|select(.pool_name==\$p)|.$2"
}

# create a pool as librados clients such as RGW do
function librados_create_pool() {
    local dir=$1
    python3 -c "
import rados
with rados.Rados(conffile='$dir/ceph.conf') as r:
    r.create_pool('$2')
"
}

# monitors a in dc1, b in dc2 and c as arbiter, a manager, and the given
# number of OSDs (default 3) in each of dc1 and dc2, on a host each
function two_zone_cluster() {
    local dir=$1
    local osds_per_dc=${2:-3}
    local osds=$(seq 0 $((2 * osds_per_dc - 1)))

    run_mon $dir a --public-addr $CEPH_MON_A || return 1
    run_mon $dir b --public-addr $CEPH_MON_B || return 1
    run_mon $dir c --public-addr $CEPH_MON_C || return 1
    wait_for_quorum 300 3 || return 1
    run_mgr $dir x || return 1
    for osd in $osds; do
        run_osd $dir $osd || return 1
    done

    ceph mon set_location a datacenter=dc1 || return 1
    ceph mon set_location b datacenter=dc2 || return 1
    ceph mon set_location c datacenter=arbiter || return 1
    for dc in dc1 dc2; do
        ceph osd crush add-bucket $dc datacenter || return 1
        ceph osd crush move $dc root=default || return 1
    done
    for osd in $osds; do
        ceph osd crush add-bucket host$osd host || return 1
        ceph osd crush move host$osd datacenter=dc$((osd / osds_per_dc + 1)) || return 1
        ceph osd crush set osd.$osd 1.0 host=host$osd || return 1
    done
    ceph config set osd osd_crush_update_on_start false || return 1
}

# a stretch EC pool data0 over dc1 and dc2, with dc2's monitor and OSDs
# down and the cluster in degraded stretch mode
function ec_stretch_cluster_without_dc2() {
    local dir=$1

    two_zone_cluster $dir || return 1
    ceph osd pool create data0 erasure --num_zones 2 --k 2 --m 1 || return 1
    wait_for_clean || return 1

    kill_daemons $dir KILL mon.b || return 1
    for osd in 3 4 5; do
        kill_daemons $dir KILL osd.$osd || return 1
    done
    ceph osd down osd.3 osd.4 osd.5 || return 1
    wait_for_stretch_state 1 0 || return 1
}

function rule_exists() {
    ceph osd crush rule ls | grep -qx "$1"
}

function rule_takes() {
    ceph osd crush rule dump $1 | jq -r '[.steps[]|select(.op=="take")|.item_name]|join(",")'
}

function rule_zone_type() {
    ceph osd crush rule dump $1 |
        jq -r '[.steps[]|select(.op=="choose_firstn")|.type]|join(",")'
}

function rule_leaf_type() {
    ceph osd crush rule dump $1 |
        jq -r '[.steps[]|select(.op|startswith("choose"))|.type]|last'
}

# write count objects named prefix<i> to pool, keeping what was written
function write_objects() {
    local dir=$1 pool=$2 prefix=$3 count=$4 i
    for i in $(seq 0 $((count - 1))); do
        dd if=/dev/urandom of=$dir/$prefix$i bs=4k count=$((1 + i % 8)) status=none
        rados -p $pool put $prefix$i $dir/$prefix$i || return 1
    done
}

function check_objects() {
    local dir=$1 pool=$2 prefix=$3 count=$4 i
    for i in $(seq 0 $((count - 1))); do
        timeout 120 rados -p $pool get $prefix$i $dir/got || return 1
        cmp $dir/got $dir/$prefix$i || return 1
    done
}

# the number of PGs of pool that are not active
function inactive_pgs() {
    local pool=$1
    flush_pg_stats > /dev/null || return 1
    ceph pg ls-by-pool $pool -f json |
        jq '[.pg_stats[] | select(.state | startswith("active") | not)] | length'
}

# no PG of pool inactive
function wait_for_active() {
    local pool=$1 i
    for i in $(seq 1 150); do
        test "$(inactive_pgs $pool)" = 0 && return 0
        sleep 2
    done
    ceph pg ls-by-pool $pool
    return 1
}

# Monitors a in zone z1, b in z2 and c the tiebreaker, a manager, and 4 OSDs
# in each zone on a host each: osd.0-3 in z1 and osd.4-7 in z2. The zones are
# buckets of type ztype under the root, or with deep each under a region of
# its own. osd.3 and osd.7 are of class hdd, the others ssd.
function zone_cluster() {
    local dir=$1 ztype=$2 deep=$3 osd z

    run_mon $dir a --public-addr $CEPH_MON_A || return 1
    run_mon $dir b --public-addr $CEPH_MON_B || return 1
    run_mon $dir c --public-addr $CEPH_MON_C || return 1
    wait_for_quorum 300 3 || return 1
    run_mgr $dir x || return 1
    for osd in $(seq 0 7); do
        run_osd $dir $osd || return 1
    done

    ceph mon set_location a $ztype=z1 || return 1
    ceph mon set_location b $ztype=z2 || return 1
    ceph mon set_location c $ztype=arbiter || return 1
    for z in 1 2; do
        ceph osd crush add-bucket z$z $ztype || return 1
        if [ -n "$deep" ]; then
            ceph osd crush add-bucket region$z region || return 1
            ceph osd crush move region$z root=default || return 1
            ceph osd crush move z$z region=region$z || return 1
        else
            ceph osd crush move z$z root=default || return 1
        fi
    done
    for osd in $(seq 0 7); do
        ceph osd crush add-bucket host$osd host || return 1
        ceph osd crush move host$osd $ztype=z$((osd / 4 + 1)) || return 1
        ceph osd crush set osd.$osd 1.0 host=host$osd || return 1
    done
    ceph osd crush remove $(hostname -s) || return 1
    ceph osd crush rm-device-class $(seq 0 7) || return 1
    ceph osd crush set-device-class ssd 0 1 2 4 5 6 || return 1
    ceph osd crush set-device-class hdd 3 7 || return 1
    ceph config set osd osd_crush_update_on_start false || return 1
    # the Manager creates .mgr when a module first needs it, which would
    # change the pools and rules under a test
    ceph osd pool create .mgr 1 1 --yes-i-really-mean-it || return 1
    ceph osd pool application enable .mgr mgr || return 1
}

# the CRUSH rules are exactly those given
function rules_are() {
    local want=$(printf '%s\n' "$@" | LC_ALL=C sort | xargs)
    local have=$(ceph osd crush rule ls | LC_ALL=C sort | xargs)
    test "$have" = "$want" || {
        echo "rules: $have; expected: $want"
        return 1
    }
}

function pool_rule() {
    ceph osd pool get $1 crush_rule -f json | jq -r .crush_rule
}

function type_id() {
    ceph osd crush dump | jq --arg t $1 '.types[]|select(.name==$t)|.type_id'
}

# pool is over two zones of type ztype, with per_zone OSDs of each PG in z1
# and in z2, and those of the first zone first
function check_two_zones() {
    local pool=$1 ztype=$2 per_zone=$3
    test "$(pool_field $pool size)" = $((2 * per_zone)) || return 1
    test "$(pool_field $pool peering_crush_bucket_count)" = 2 || return 1
    test "$(pool_field $pool peering_crush_bucket_barrier)" = $(type_id $ztype) || return 1
    flush_pg_stats > /dev/null || return 1
    ceph pg ls-by-pool $pool -f json | jq -e --argjson n $per_zone '
        [.pg_stats[].acting |
         [(map(select(. < 4)) | length), (map(select(. >= 4 and . < 8)) | length),
          (.[0:$n] | map(. < 4) | unique | length)]] |
        unique == [[$n, $n, 1]]' > /dev/null || {
        ceph pg ls-by-pool $pool
        return 1
    }
}

# pool is in zone z alone (1: osd.0-3, 2: osd.4-7)
function check_one_zone() {
    local pool=$1 z=$2
    local first=$(((z - 1) * 4))
    test "$(pool_field $pool peering_crush_bucket_count)" = 0 || return 1
    test "$(pool_field $pool peering_crush_bucket_barrier)" = 0 || return 1
    flush_pg_stats > /dev/null || return 1
    ceph pg ls-by-pool $pool -f json | jq -e --argjson f $first '
        [.pg_stats[].acting[] | select(. < $f or . >= $f + 4)] == []' > /dev/null || {
        ceph pg ls-by-pool $pool
        return 1
    }
}

# every OSD of pool's PGs is of class ssd
function check_ssd() {
    local pool=$1
    flush_pg_stats > /dev/null || return 1
    ceph pg ls-by-pool $pool -f json | jq -e --argjson ssd "$(ceph osd crush class ls-osd ssd -f json)" '
        [.pg_stats[].acting[]] - $ssd == []' > /dev/null
}

function zone_mon() {
    test $1 = 1 && echo a || echo b
}

function zone_osds() {
    seq $((($1 - 1) * 4)) $(($1 * 4 - 1))
}

# the --mon-host list without zone z's monitor
function other_mons() {
    test $1 = 1 && echo $CEPH_MON_B,$CEPH_MON_C || echo $CEPH_MON_A,$CEPH_MON_C
}

# kill zone z's monitor and OSDs; the clients stop trying the monitor
function kill_zone() {
    local dir=$1 z=$2 osd
    kill_daemons $dir KILL mon.$(zone_mon $z) || return 1
    for osd in $(zone_osds $z); do
        kill_daemons $dir KILL osd.$osd || return 1
    done
    CEPH_ARGS=${CEPH_ARGS/--mon-host=$CEPH_MON /--mon-host=$(other_mons $z) }
    ceph osd down $(zone_osds $z) || return 1
}

function restore_zone() {
    local dir=$1 z=$2 osd addr=$CEPH_MON_A
    test $z = 1 || addr=$CEPH_MON_B
    activate_mon $dir $(zone_mon $z) --public-addr $addr || return 1
    CEPH_ARGS=${CEPH_ARGS/--mon-host=$(other_mons $z) /--mon-host=$CEPH_MON }
    wait_for_quorum 300 3 || return 1
    for osd in $(zone_osds $z); do
        activate_osd $dir $osd || return 1
    done
    wait_for_clean || return 1
}

function write_pools() {
    local dir=$1 pool
    shift
    for pool in "$@"; do
        write_objects $dir $pool $pool 10 || return 1
    done
}

function check_pools() {
    local dir=$1 pool
    shift
    for pool in "$@"; do
        wait_for_active $pool || return 1
        check_objects $dir $pool $pool 10 || return 1
    done
}

# the pools over zones can be read with either zone lost (degraded stretch
# mode), and healthy stretch mode comes back with the zone
function check_each_zone_alone() {
    local dir=$1 z
    shift
    for z in 1 2; do
        kill_zone $dir $z || return 1
        wait_for_stretch_state 1 0 || return 1
        check_pools $dir "$@" || return 1
        restore_zone $dir $z || return 1
        wait_for_stretch_state 0 0 || return 1
    done
}

# the pools in one zone can be read with the other zone, z, lost
function check_without_zone() {
    local dir=$1 z=$2
    shift 2
    kill_zone $dir $z || return 1
    check_pools $dir "$@" || return 1
    restore_zone $dir $z || return 1
}

function stretch_mode_enabled() {
    ceph osd dump -f json | jq -e .stretch_mode.stretch_mode_enabled > /dev/null
}

# hand-written rules over zones z1 and z2 of type ztype: take2 takes each zone,
# choose2 chooses them, class2 chooses them in class ssd, ec_take and ec_choose
# are erasure rules of each kind, and mixed2 has different steps after each
# take; and rules in one zone: hosts_any, and per_zone_one, which puts a copy
# in each zone
function add_hand_rules() {
    local dir=$1 ztype=$2
    ceph osd getcrushmap > $dir/crushmap || return 1
    crushtool --decompile $dir/crushmap > $dir/crushmap.txt || return 1
    cat >> $dir/crushmap.txt << EOF
rule take2 {
        id 10
        type replicated
        step take z1
        step chooseleaf firstn 2 type host
        step emit
        step take z2
        step chooseleaf firstn 2 type host
        step emit
}
rule choose2 {
        id 11
        type replicated
        step take default
        step choose firstn 2 type $ztype
        step chooseleaf firstn 2 type host
        step emit
}
rule class2 {
        id 12
        type replicated
        step take default class ssd
        step choose firstn 2 type $ztype
        step chooseleaf firstn 2 type host
        step emit
}
rule ec_take {
        id 13
        type erasure
        step set_chooseleaf_tries 5
        step set_choose_tries 100
        step take z1
        step chooseleaf indep 4 type host
        step emit
        step take z2
        step chooseleaf indep 4 type host
        step emit
}
rule ec_choose {
        id 14
        type erasure
        step set_chooseleaf_tries 5
        step set_choose_tries 100
        step take default
        step choose indep 2 type $ztype
        step chooseleaf indep 4 type host
        step emit
}
rule mixed2 {
        id 15
        type replicated
        step take z1
        step chooseleaf firstn 2 type host
        step emit
        step take z2
        step chooseleaf firstn 2 type osd
        step emit
}
rule hosts_any {
        id 16
        type replicated
        step take default
        step chooseleaf firstn 0 type host
        step emit
}
rule per_zone_one {
        id 17
        type replicated
        step take default
        step choose firstn 0 type $ztype
        step chooseleaf firstn 1 type host
        step emit
}
EOF
    crushtool --compile $dir/crushmap.txt -o $dir/crushmap.bin || return 1
    ceph osd setcrushmap -i $dir/crushmap.bin || return 1
}
