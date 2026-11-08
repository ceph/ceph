#!/usr/bin/env bash
#
# The monitors turn away clients that lack an extended feature they require:
# at authentication, at once and with the reason, over msgr2 and msgr1.
# mon_debug_require_client_ext_features sets the requirement. A client lacks
# the feature with ms_v2_ext_features_hide, or cannot exchange extended
# features at all with ms_v2_disable_ext_features, like older releases.
#

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON_V2="v2:127.0.0.1:7157" # git grep '\<7157\>' : there must be only one
    export CEPH_MON_V1="v1:127.0.0.1:7158" # git grep '\<7158\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none "
    CEPH_ARGS+="--auth_service_required=none --auth_client_required=none "
    CEPH_ARGS+="--mon-host=[$CEPH_MON_V2,$CEPH_MON_V1] "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function require_ext_feature_0() {
    ceph config set mon mon_debug_require_client_ext_features 0 || return 1
    local i
    for i in $(seq 30) ; do
        ceph --ms_v2_ext_features_hide 0 --client_mount_timeout 5 health \
            2> /dev/null || return 0
        sleep 1
    done
    return 1
}

function lift_requirement() {
    ceph config rm mon mon_debug_require_client_ext_features || return 1
    local i
    for i in $(seq 30) ; do
        ceph --ms_v2_ext_features_hide 0 --client_mount_timeout 5 health \
            2> /dev/null && return 0
        sleep 1
    done
    return 1
}

# The client run by the remaining arguments fails within 30 s.
function refused() {
    local err=$1
    shift
    local start=$SECONDS
    ! "$@" --client_mount_timeout 60 health 2> $err || return 1
    cat $err
    (( SECONDS - start < 30 )) || return 1
}

function has_session() {
    ceph tell mon.a sessions | grep -q "\"entity_name\": \"$1\""
}

function TEST_new_clients_turned_away() {
    local dir=$1
    local reason="refused this client: this cluster requires extended features {0} that this client lacks"

    run_mon $dir a || return 1
    require_ext_feature_0 || return 1

    ceph health || return 1
    refused $dir/hide.err ceph --ms_v2_ext_features_hide 0 || return 1
    grep -q "$reason" $dir/hide.err || return 1
    refused $dir/old.err ceph --ms_v2_disable_ext_features=true || return 1
    CEPH_ARGS="$CEPH_ARGS --mon-host=$CEPH_MON_V1" \
        refused $dir/msgr1.err ceph || return 1
    grep -q "$reason.*ms_mode=prefer-crc" $dir/msgr1.err || return 1
    TIMEOUT=60 wait_for_string $dir/log \
        "turned away .* connection attempt(s) from clients lacking required features" || return 1

    lift_requirement || return 1
    ceph --ms_v2_disable_ext_features=true health || return 1
    CEPH_ARGS="$CEPH_ARGS --mon-host=$CEPH_MON_V1" ceph health || return 1
}

function TEST_running_client_keeps_trying() {
    local dir=$1

    run_mon $dir a || return 1
    ceph --name client.watcher --ms_v2_ext_features_hide 0 -w \
        > $dir/watcher.log 2>&1 &
    local watcher=$!
    local i
    for i in $(seq 30) ; do
        has_session client.watcher && break
        sleep 1
    done
    has_session client.watcher || return 1

    require_ext_feature_0 || return 1
    kill_daemons $dir TERM mon.a || return 1
    activate_mon $dir a || return 1
    wait_for_quorum 300 || return 1
    TIMEOUT=60 wait_for_string $dir/watcher.log \
        "every mon refused this running client: .*; will keep trying" || return 1
    ! has_session client.watcher || return 1

    # it retries with backoff, at most 30 s apart
    lift_requirement || return 1
    for i in $(seq 60) ; do
        has_session client.watcher && break
        sleep 1
    done
    has_session client.watcher || return 1
    kill $watcher
    wait $watcher
    return 0
}

function TEST_refused_at_identification() {
    local dir=$1

    run_mon $dir a || return 1
    ceph config set mon mon_reject_clients_at_auth false || return 1
    require_ext_feature_0 || return 1

    # refused in the session handshake instead, it gives up after one
    # client_mount_timeout
    local start=$SECONDS
    ! ceph --ms_v2_ext_features_hide 0 --client_mount_timeout 5 health \
        2> $dir/ident.err || return 1
    cat $dir/ident.err
    (( SECONDS - start >= 5 && SECONDS - start < 30 )) || return 1
    grep -q "refused this client: this cluster requires extended features {0}" \
        $dir/ident.err || return 1
    grep -q "timed out" $dir/ident.err || return 1
}

main mon-client-features "$@"

