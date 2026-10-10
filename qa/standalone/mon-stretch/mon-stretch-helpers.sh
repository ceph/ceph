# Helpers for the qa/standalone/mon-stretch tests; sourced, not run.

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
    ceph osd pool create data0 erasure --num-zones 2 --k 2 --m 1 || return 1
    wait_for_clean || return 1

    kill_daemons $dir KILL mon.b || return 1
    for osd in 3 4 5; do
        kill_daemons $dir KILL osd.$osd || return 1
    done
    ceph osd down osd.3 osd.4 osd.5 || return 1
    wait_for_stretch_state 1 0 || return 1
}

# global stretch mode over the datacenters, with pool stretched on a stretch rule
function enable_global_stretch_mode() {
    ceph osd crush rule create-stretch-replicated --rule-name=stretch_rule || return 1
    ceph osd pool create stretched 8 8 replicated stretch_rule || return 1
    ceph mon set election_strategy connectivity || return 1
    # the command can be resent after the election it causes
    ceph mon enable_stretch_mode c stretch_rule datacenter
    for i in $(seq 1 30); do
        test "$(ceph mon dump -f json | jq .global_stretch_mode)" = true && return 0
        sleep 1
    done
    return 1
}
