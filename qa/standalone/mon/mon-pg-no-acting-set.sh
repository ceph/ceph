#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON_A="127.0.0.1:7177" # git grep '\<7177\>' : there must be only one
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

TEST_mon_pg_no_acting_set() {
    local dir=$1
    setup $dir || return 1

    run_mon $dir a --public-addr $CEPH_MON_A || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    ceph osd pool create testpool 64 || return 1
    ceph osd pool set testpool size 2 || return 1
    ceph osd pool set testpool min_size 1 || return 1
    for i in $(seq 1 20); do
        echo "data-$i" | rados -p testpool put obj-$i - || return 1
    done

    sleep 15

    # inject bad crush mapping (OSDs added directly to the root instead of into the appropriate intermediate buckets)
    ceph osd getcrushmap > $dir/crushmap || return 1
    cat > $dir/crushmap_modified.txt << EOF
# begin crush map
tunable choose_local_tries 0
tunable choose_local_fallback_tries 0
tunable choose_total_tries 50
tunable chooseleaf_descend_once 1
tunable chooseleaf_vary_r 1
tunable chooseleaf_stable 1
tunable straw_calc_version 1
tunable allowed_bucket_algs 54

device 0 osd.0 class hdd
device 1 osd.1 class hdd
device 2 osd.2 class hdd

type 0 osd
type 1 host
type 11 root

host ceph-osd0 {
  id -3
  alg straw2
  hash 0
  item osd.0 weight 0.09769
}
host ceph-osd1 {
  id -5
  alg straw2
  hash 0
  item osd.1 weight 0.09769
}
root default {
  id -1
  alg straw2
  hash 0
  item ceph-osd0 weight 0.09769
  item ceph-osd1 weight 0.09769
  item osd.2 weight 0.09769
}

rule replicated_rule {
  id 0
  type replicated
  step take default
  step choose firstn 1 type host
  step chooseleaf firstn 0 type osd
  step emit
}
# end crush map
EOF
    crushtool --compile $dir/crushmap_modified.txt -o $dir/crushmap.bin || return 1
    ceph osd setcrushmap -i $dir/crushmap.bin || return 1

    ceph tell mon.a config set auth_allow_insecure_global_id_reclaim false || return 1
    ceph config set global mon_warn_on_msgr2_not_enabled false || return 1

    sleep 10
    echo "=== Health ==="
    wait_for_health "HEALTH_WARN" || return 1

    ceph -s 2>&1 | grep "stale+active+clean" || return 1

    ceph osd crush set osd.2 0.09769 host=ceph-osd2 || return 1
    ceph osd crush move ceph-osd2 root=default || return 1
    sleep 10

    timeout 5 rados -p testpool get obj-4 /dev/null 2>&1 && echo "obj-4: OK" || return 1

    wait_for_health_ok || return 1

    teardown $dir || return 1
}

main mon-pg-no-acting-set "$@"
