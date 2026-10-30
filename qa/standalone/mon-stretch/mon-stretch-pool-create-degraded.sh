#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON_A="127.0.0.1:7290" # git grep '\<7290\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7291" # git grep '\<7291\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7292" # git grep '\<7292\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C "
    CEPH_ARGS+="--mon_stretch_recovery_min_wait=5 "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
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
    ceph osd dump -f json | jq '.stretch_mode'
    ceph -s
    return 1
}

function pool_field() {
    ceph osd pool ls detail -f json | jq --arg p $1 ".[]|select(.pool_name==\$p)|.$2"
}

# A pool created in degraded stretch mode must end up like the other stretch
# pools once the cluster is healthy again.
function TEST_pool_create_in_degraded_stretch_mode() {
    local dir=$1

    run_mon $dir a --public-addr $CEPH_MON_A || return 1
    run_mon $dir b --public-addr $CEPH_MON_B || return 1
    run_mon $dir c --public-addr $CEPH_MON_C || return 1
    wait_for_quorum 300 3 || return 1
    ceph mon set election_strategy connectivity || return 1
    ceph mon add disallowed_leader c || return 1
    run_mgr $dir x || return 1
    for osd in 0 1 2 3; do
        run_osd $dir $osd || return 1
    done

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
    ceph osd crush move osd.0 host=node-2 || return 1
    ceph osd crush move osd.1 host=node-3 || return 1
    ceph osd crush move osd.2 host=node-4 || return 1
    ceph osd crush move osd.3 host=node-5 || return 1
    ceph osd crush remove $(hostname -s) || return 1
    # keep restarted OSDs in their zones
    ceph config set osd osd_crush_update_on_start false || return 1

    ceph mon set_location a zone=iris host=node-2 || return 1
    ceph mon set_location b zone=pze host=node-4 || return 1
    ceph mon set_location c zone=arbiter host=node-1 || return 1

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

    ceph osd pool create stretched 8 8 stretch_rule || return 1
    ceph osd pool set stretched size 4 || return 1
    ceph mon enable_stretch_mode c stretch_rule zone || return 1
    wait_for_clean || return 1

    kill_daemons $dir KILL mon.b || return 1
    kill_daemons $dir KILL osd.2 || return 1
    kill_daemons $dir KILL osd.3 || return 1
    ceph osd down osd.2 osd.3
    wait_for_stretch_state 1 0 || return 1

    ceph osd pool create created_degraded 8 8 stretch_rule || return 1
    ceph osd pool ls detail
    timeout 120 rados -p created_degraded put obj $dir/crushmap.bin || return 1

    restart_mon $dir b --public-addr $CEPH_MON_B || return 1
    wait_for_quorum 300 3 || return 1
    activate_osd $dir 2 || return 1
    activate_osd $dir 3 || return 1
    wait_for_stretch_state 0 0 || return 1
    ceph osd pool ls detail

    for field in size min_size peering_crush_bucket_count peering_crush_bucket_target; do
        test "$(pool_field created_degraded $field)" == "$(pool_field stretched $field)" || return 1
    done
    wait_for_clean || return 1
    rados -p created_degraded get obj $dir/obj || return 1
    cmp $dir/crushmap.bin $dir/obj || return 1
}

main mon-stretch-pool-create-degraded "$@"
