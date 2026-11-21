#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON_A="127.0.0.1:7160" # git grep '\<7160\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7161" # git grep '\<7161\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7162" # git grep '\<7162\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C "

    local funcs=${@:-$(set | ${SED} -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        kill_daemons $dir KILL || return 1
        teardown $dir || return 1
    done
}

function run_mons() {
    local dir=$1
    shift

    run_mon $dir a --public-addr=$CEPH_MON_A "$@" || return 1
    run_mon $dir b --public-addr=$CEPH_MON_B "$@" || return 1
    run_mon $dir c --public-addr=$CEPH_MON_C "$@" || return 1
    wait_for_quorum 300 3 || return 1
}

# start an existing monitor again, without --mkfs
function restart_mon() {
    local dir=$1
    local id=$2
    shift 2

    ceph-mon \
        --id $id \
        --mon-data-avail-crit=1 \
        --mon-data-avail-warn=5 \
        --paxos-propose-interval=0.1 \
        --debug-mon 20 \
        --debug-paxos 20 \
        --chdir= \
        --mon-data=$dir/$id \
        --log-file=$dir/\$name.log \
        --admin-socket=$(get_asok_path) \
        --mon-cluster-log-file=$dir/log \
        --run-dir=$dir \
        --pid-file=$dir/\$name.pid \
        --mon-warn-on-insecure-global-id-reclaim-allowed=false \
        "$@" || return 1
}

function import_bad_caps() {
    local entity=$1

    ceph auth import -i - <<EOF || return 1
[$entity]
  caps mon = this is wrong
  caps osd = does not parse
EOF
}

# wait until the parts of `ceph health detail` matching PATTERN are EXPECTED
function wait_for_health_detail() {
    local pattern=$1
    local expected=$2
    local actual

    for i in $(seq 1 30); do
        actual=$(ceph health detail | grep -oE "$pattern")
        if [ "$actual" == "$expected" ]; then
            return 0
        fi
        sleep 1
    done
    echo "expected:"
    echo "$expected"
    echo "got:"
    echo "$actual"
    ceph health detail
    return 1
}

BAD_CAPS='AUTH_BAD_CAPS: [0-9]+ auth entities|client\.bad[0-9] (mon|osd)'

function TEST_auth_bad_caps() {
    local dir=$1

    run_mons $dir || return 1

    import_bad_caps client.bad1 || return 1
    import_bad_caps client.bad2 || return 1
    wait_for_health_detail "$BAD_CAPS" \
"AUTH_BAD_CAPS: 2 auth entities
client.bad1 mon
client.bad1 osd
client.bad2 mon
client.bad2 osd" || return 1

    ceph auth caps client.bad1 mon 'allow r' || return 1
    wait_for_health_detail "$BAD_CAPS" \
"AUTH_BAD_CAPS: 1 auth entities
client.bad2 mon
client.bad2 osd" || return 1

    ceph auth rm client.bad2 || return 1
    wait_for_health_detail "$BAD_CAPS" '' || return 1
}

# Only mon caps are checked when mon_auth_validate_all_caps is off.
function TEST_auth_bad_caps_validate_all_caps() {
    local dir=$1

    run_mons $dir || return 1
    ceph auth import -i - <<EOF || return 1
[client.bad1]
  caps mon = "allow r"
  caps osd = does not parse
EOF
    wait_for_health_detail "$BAD_CAPS" \
"AUTH_BAD_CAPS: 1 auth entities
client.bad1 osd" || return 1

    ceph config set mon mon_auth_validate_all_caps false || return 1
    wait_for_health_detail "$BAD_CAPS" '' || return 1

    ceph config set mon mon_auth_validate_all_caps true || return 1
    wait_for_health_detail "$BAD_CAPS" \
"AUTH_BAD_CAPS: 1 auth entities
client.bad1 osd" || return 1
}

# A monitor builds the verdicts on its first check as leader, or after a
# restart, then keeps them up to date.
function TEST_auth_bad_caps_across_monitors() {
    local dir=$1

    run_mons $dir || return 1
    import_bad_caps client.bad1 || return 1
    wait_for_health_detail 'AUTH_BAD_CAPS: [0-9]+' 'AUTH_BAD_CAPS: 1' || return 1

    local leader=$(ceph quorum_status --format=json | jq -r .quorum_leader_name)
    kill_daemons $dir TERM mon.$leader || return 1
    wait_for_quorum 300 2 || return 1
    test "$(ceph quorum_status --format=json | jq -r .quorum_leader_name)" != $leader || return 1
    import_bad_caps client.bad2 || return 1
    wait_for_health_detail 'AUTH_BAD_CAPS: [0-9]+' 'AUTH_BAD_CAPS: 2' || return 1

    local addr=CEPH_MON_${leader^^}
    restart_mon $dir $leader --public-addr=${!addr} || return 1
    wait_for_quorum 300 3 || return 1
    test "$(ceph quorum_status --format=json | jq -r .quorum_leader_name)" == $leader || return 1
    import_bad_caps client.bad3 || return 1
    wait_for_health_detail 'AUTH_BAD_CAPS: [0-9]+' 'AUTH_BAD_CAPS: 3' || return 1
    ceph auth rm client.bad1 || return 1
    wait_for_health_detail 'AUTH_BAD_CAPS: [0-9]+' 'AUTH_BAD_CAPS: 2' || return 1
}

function insecure_client_keys() {
    ceph health detail --format=json |
        jq -r '.checks.AUTH_INSECURE_CLIENT_KEY_TYPE.summary.count // 0'
}

function TEST_auth_health_detail_bound() {
    local dir=$1

    run_mons $dir --mon-health-max-detail=3 || return 1
    local before=$(insecure_client_keys)
    for i in 1 2 3 4 5; do
        # without a key: an insecure key type
        printf '[client.nokey%s]\n  caps mon = "allow r"\n' $i | ceph auth import -i - || return 1
    done
    local count=$((before + 5))
    for i in $(seq 1 30); do
        test "$(insecure_client_keys)" == $count && break
        sleep 1
    done
    test "$(insecure_client_keys)" == $count || return 1
    ceph health detail --format=json | jq -r '.checks.AUTH_INSECURE_CLIENT_KEY_TYPE.detail[].message' > $dir/detail
    cat $dir/detail
    test $(grep -c 'using insecure key type' $dir/detail) == 3 || return 1
    grep -qx "$((count - 3)) more entities..." $dir/detail || return 1
    test $(wc -l < $dir/detail) == 4 || return 1
}

main auth-health "$@"

# Local Variables:
# compile-command: "cd ../../../build ; make -j4 && ../qa/run-standalone.sh auth-health.sh"
# End:
