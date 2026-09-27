#!/usr/bin/env bash
#
# Crimson Prometheus endpoint, configured before the OSD starts:
#   crimson_prometheus_port_base unset or 0 -> no listener
#   base B, osd N -> listen on B+N
#   crimson_prometheus_prefix names the scrape series
#   B+N above 65535 aborts startup
#
#   cd build && ../qa/run-standalone.sh crimson/prometheus.sh

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

# Distinct from the monitor port below, and from each other.
PROM_BASE=19180

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7196" # git grep '\<7196\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    CEPH_ARGS+="--crimson_cpu_num=1 "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function _cluster_up() {
    local dir=$1
    run_mon $dir a --osd_pool_default_size=1 --mon_allow_pool_size_one=true \
        --osd_pool_default_crimson=true || return 1
    run_mgr $dir x || return 1
}

# run_crimson_osd allocates the next free id. Hold id 0 without starting it
# when the daemon under test must be osd.1.
function _hold_osd_zero() {
    local dir=$1
    local json=$dir/placeholder-osd.json
    echo '{}' > "$json"
    local id
    id=$(ceph osd new "$(uuidgen)" -i "$json") || return 1
    if [ "$id" != "0" ]; then
        echo "expected placeholder osd id 0, got ${id}"
        return 1
    fi
}

function _wait_metric() {
    local port=$1
    local needle=$2
    local out=$3
    local i
    for i in $(seq 1 30); do
        if curl -sf --max-time 5 "http://127.0.0.1:${port}/metrics" >"$out" \
            && grep -q "$needle" "$out"; then
            return 0
        fi
        sleep 1
    done
    echo "missing ${needle} on port ${port}"
    return 1
}

function _refuse_port() {
    local port=$1
    if curl -sf --max-time 5 "http://127.0.0.1:${port}/metrics" >/dev/null; then
        echo "Prometheus endpoint unexpectedly served on port ${port}"
        return 1
    fi
}

function TEST_prometheus_off_when_base_unset() {
    local dir=$1

    _cluster_up $dir || return 1
    run_crimson_osd $dir 0 || return 1

    if grep -q 'starting prometheus server' "$dir/osd.0.log"; then
        echo "prometheus server started with the default base of 0"
        return 1
    fi
    _refuse_port $PROM_BASE || return 1
}

function TEST_prometheus_port_base_plus_osd_id() {
    local dir=$1
    local osd_id=1
    local port=$((PROM_BASE + osd_id))

    _cluster_up $dir || return 1
    ceph config set osd crimson_prometheus_port_base $PROM_BASE || return 1
    ceph config set osd crimson_prometheus_address 127.0.0.1 || return 1
    _hold_osd_zero $dir || return 1
    run_crimson_osd $dir $osd_id || return 1

    local logf=$dir/osd.${osd_id}.log
    grep -q "starting prometheus server on 127.0.0.1:${port} (crimson_prometheus_port_base ${PROM_BASE} + osd id ${osd_id})" "$logf" || {
        echo "prometheus listen line missing from $logf"
        tail -n 80 "$logf" || true
        return 1
    }

    _wait_metric $port osd_reactor_utilization "$dir/metrics.prom" || return 1
    _refuse_port $PROM_BASE || return 1

    local dumped=$dir/dump_metrics.out
    ceph tell osd.$osd_id dump_metrics reactor_utilization >"$dumped" 2>&1 || return 1
    grep -q reactor_utilization "$dumped" || {
        echo "dump_metrics did not report reactor_utilization"
        cat "$dumped"
        return 1
    }
}

function TEST_prometheus_two_osds_use_distinct_ports() {
    local dir=$1

    _cluster_up $dir || return 1
    ceph config set osd crimson_prometheus_port_base $PROM_BASE || return 1
    ceph config set osd crimson_prometheus_address 127.0.0.1 || return 1
    run_crimson_osd $dir 0 || return 1
    run_crimson_osd $dir 1 || return 1

    _wait_metric $PROM_BASE osd_reactor_utilization "$dir/osd0.prom" || return 1
    _wait_metric $((PROM_BASE + 1)) osd_reactor_utilization "$dir/osd1.prom" || return 1
}

function TEST_prometheus_prefix_from_config() {
    local dir=$1

    _cluster_up $dir || return 1
    ceph config set osd crimson_prometheus_port_base $PROM_BASE || return 1
    ceph config set osd crimson_prometheus_address 127.0.0.1 || return 1
    ceph config set osd crimson_prometheus_prefix crim || return 1
    run_crimson_osd $dir 0 || return 1

    _wait_metric $PROM_BASE crim_reactor_utilization "$dir/metrics.prom" || return 1
    if grep -q 'osd_reactor_utilization' "$dir/metrics.prom"; then
        echo "default prefix osd_ was used instead of crim_"
        return 1
    fi
}

function TEST_prometheus_base_plus_id_overflow() {
    local dir=$1
    local saved_timeout=$TIMEOUT

    _cluster_up $dir || return 1
    ceph config set osd crimson_prometheus_port_base 65535 || return 1
    ceph config set osd crimson_prometheus_address 127.0.0.1 || return 1
    _hold_osd_zero $dir || return 1

    TIMEOUT=15
    if run_crimson_osd $dir 1; then
        TIMEOUT=$saved_timeout
        echo "osd.1 started even though 65535 + 1 exceeds 65535"
        return 1
    fi
    TIMEOUT=$saved_timeout

    grep -q 'crimson_prometheus_port_base 65535 + osd id 1 exceeds 65535' \
        "$dir/osd.1.log" || {
        echo "overflow abort missing from $dir/osd.1.log"
        tail -n 40 "$dir/osd.1.log" || true
        return 1
    }
}

main prometheus "$@"
