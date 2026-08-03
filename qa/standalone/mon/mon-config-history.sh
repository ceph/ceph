#!/usr/bin/env bash
#
# Author: Steven Zhang <yzhan298@gmail.com>
#
# This test verifies that the mon trims config history at changeset granularity.
# Changesets are removed whole -- marker and records together -- so no changeset
# is ever left partially present, and 'ceph config log' and 'ceph config reset'
# both respect the resulting watermark.

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7157"
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function history_keys() {
    ceph config-key ls --format=json | jq -r '.[]' | grep '^config-history/' || true
}

# versions that still have a marker
function marker_versions() {
    history_keys | sed -n -E 's|^config-history/([0-9]+)/$|\1|p' | sort -n
}

# versions that still have at least one +/- record
function record_versions() {
    history_keys | sed -n -E 's|^config-history/([0-9]+)/[+-].*|\1|p' | sort -nu
}

# the watermark is an encoded version_t, so only its presence is checkable here
function has_watermark() {
    ceph config-key exists config-history-first >/dev/null 2>&1
}

function wait_for_history_first() {
    local expected=$1
    local -a delays=($(get_timeout_delays $TIMEOUT .1))
    local -i loop=0
    local n

    while true ; do
        n=$(marker_versions | head -1)
        if [ "$n" = "$expected" ] ; then
            return 0
        fi
        if (( loop >= ${#delays[*]} )) ; then
            echo "timed out: oldest changeset is '$n', expected '$expected'"
            history_keys
            return 1
        fi
        sleep ${delays[$loop]}
        loop+=1
    done
}

##
# Trimming removes whole changesets and leaves the live config alone.
#
function TEST_config_history_trim() {
    local dir=$1

    run_mon $dir a --mon-tick-interval=1 \
        --mon-config-history-max-changesets=3 || return 1

    local i
    for i in $(seq 1 8) ; do
        ceph config set osd osd_memory_target $((4294967296 + i * 1048576)) || return 1
    done

    local last=$((4294967296 + 8 * 1048576))
    test "$(ceph config-key get config/osd/osd_memory_target)" = "$last" || return 1

    # only the newest 3 changesets survive; 'version' is the last one written
    local version=$(marker_versions | tail -1)
    wait_for_history_first $((version - 2)) || return 1

    # trimming must never touch the live value under config/
    test "$(ceph config-key get config/osd/osd_memory_target)" = "$last" || return 1
    test "$(ceph config get osd.0 osd_memory_target)" = "$last" || return 1
}

##
# A changeset is removed in its entirety: no record may outlive its marker, and
# no marker may outlive its records.  This is what the per-key implementation
# got wrong -- trimming one option's record deleted the shared marker and
# orphaned every other option recorded in the same changeset.
#
function TEST_config_history_no_orphans() {
    local dir=$1

    run_mon $dir a --mon-tick-interval=1 \
        --mon-config-history-max-changesets=3 || return 1

    # one changeset carrying several options, the way assimilate-conf and
    # 'config reset' both produce
    cat > $dir/multi.conf <<EOF
[osd]
osd_memory_target = 4294967296
osd_scrub_min_interval = 3600
osd_scrub_load_threshold = 2000
EOF
    ceph config assimilate-conf -i $dir/multi.conf || return 1

    # churn only one of them, so the shared changeset ages out on its account
    local i
    for i in $(seq 1 8) ; do
        ceph config set osd osd_memory_target $((4294967296 + i * 1048576)) || return 1
    done

    local version=$(marker_versions | tail -1)
    wait_for_history_first $((version - 2)) || return 1

    # the two sets must be identical: no orphaned records, no empty markers
    local markers=$(marker_versions | tr '\n' ' ')
    local records=$(record_versions | tr '\n' ' ')
    if [ "$markers" != "$records" ] ; then
        echo "changeset mismatch"
        echo "  markers: $markers"
        echo "  records: $records"
        history_keys
        return 1
    fi

    # every surviving changeset must still report a real timestamp; a missing
    # marker shows up as 0.000000 in 'config log'
    ceph config log 100 | grep -q '0\.000000' && return 1

    return 0
}

##
# 'config reset' must refuse to target a trimmed changeset rather than silently
# replaying only the part of the history that survived.  The boundary is off by
# one from the watermark: reverting to <v> replays changesets (v, version], so
# reverting to first - 1 is still fully satisfiable.
#
function TEST_config_history_reset_refused() {
    local dir=$1

    run_mon $dir a --mon-tick-interval=1 \
        --mon-config-history-max-changesets=3 || return 1

    local i
    for i in $(seq 1 8) ; do
        ceph config set osd osd_memory_target $((4294967296 + i * 1048576)) || return 1
    done

    local version=$(marker_versions | tail -1)
    local first=$((version - 2))
    wait_for_history_first $first || return 1
    has_watermark || return 1

    local last=$((4294967296 + 8 * 1048576))

    # needs changeset first - 1, which is gone: must fail, and must not change
    # anything
    expect_failure $dir "has been trimmed" \
        ceph config reset $((first - 2)) || return 1
    test "$(ceph config-key get config/osd/osd_memory_target)" = "$last" || return 1

    # needs changesets [first, version], all of which survived: still allowed
    ceph config reset $((first - 1)) || return 1
    test "$(ceph config-key get config/osd/osd_memory_target)" != "$last" || return 1
}

##
# Nothing has been trimmed, so nothing may be refused.  'ceph config reset 0'
# reverts the whole history and is the documented way back to the defaults; it
# must keep working on a cluster that has never trimmed, where the oldest
# changeset is version 1.
#
function TEST_config_history_reset_zero() {
    local dir=$1

    run_mon $dir a --mon-tick-interval=1 || return 1

    local i
    for i in $(seq 1 5) ; do
        ceph config set osd osd_memory_target $((4294967296 + i * 1048576)) || return 1
    done
    test "$(ceph config-key get config/osd/osd_memory_target)" \
        = "$((4294967296 + 5 * 1048576))" || return 1

    # no trim has happened, so no watermark has been persisted and no revert
    # target may be refused.  note the oldest changeset is 2, not 1: version 1
    # is ConfigMonitor's create_initial() commit and carries no changeset, so
    # the oldest key present is not a bound on what can be reverted to
    has_watermark && return 1
    test "$(marker_versions | head -1)" = "2" || return 1

    ceph config reset 0 || return 1
    ceph config-key exists config/osd/osd_memory_target && return 1

    return 0
}

##
# The watermark lives in the store, not in ConfigMonitor's memory: trims commit
# through KVMonitor alone and never advance ConfigMonitor's paxos version, so a
# cached copy would not survive a restart or a leader change.
#
function TEST_config_history_watermark_survives_restart() {
    local dir=$1

    run_mon $dir a --mon-tick-interval=1 \
        --mon-config-history-max-changesets=3 || return 1

    local i
    for i in $(seq 1 8) ; do
        ceph config set osd osd_memory_target $((4294967296 + i * 1048576)) || return 1
    done

    local version=$(marker_versions | tail -1)
    local first=$((version - 2))
    wait_for_history_first $first || return 1

    # run_mon's --mkfs exits without touching a non-empty mon-data, so this
    # brings mon.a back up on the store it just wrote
    kill_daemons $dir TERM mon.a || return 1
    run_mon $dir a --mon-tick-interval=1 \
        --mon-config-history-max-changesets=3 || return 1

    # 'config log' still stops at the watermark rather than reporting the
    # trimmed changesets as empty
    test "$(ceph config log 100 | grep -c '^--- ')" = "3" || return 1
    ceph config log 100 | grep -q '0\.000000' && return 1

    expect_failure $dir "has been trimmed" \
        ceph config reset $((first - 2)) || return 1

    return 0
}

##
# A backlog bigger than one tick's worth of work must still converge.  This is
# the shape an upgrade takes: history accumulated under a version that never
# trimmed, with an operator then turning trimming on.  There is no watermark to
# resume from and the backlog is far larger than max_changesets_per_tick, so it
# only finishes if each round persists where it got to.
#
# It also covers the ordering trap: the versions being removed span 2..57, so a
# trim that walked the keys in lexicographic order rather than numerically
# ('config-history/10/' sorts before 'config-history/2/') would strand some of
# them below the watermark.
#
function TEST_config_history_trim_large_backlog() {
    local dir=$1

    # deliberately no --mon-config-history-max-changesets: a command line value
    # outranks 'ceph config set' and the point here is to change it at runtime.
    # the default is high enough that nothing is trimmed while we build up.
    run_mon $dir a --mon-tick-interval=1 || return 1

    local i
    for i in $(seq 1 60) ; do
        ceph config set osd osd_memory_target $((4294967296 + i * 1048576)) || return 1
    done
    test "$(marker_versions | wc -l)" = "60" || return 1
    has_watermark && return 1

    local last=$((4294967296 + 60 * 1048576))

    # turn trimming on: all but 5 changesets must go, many times more than the
    # 16 a single tick removes
    ceph config set mon mon_config_history_max_changesets 5 || return 1

    local version=$(marker_versions | tail -1)
    local first=$((version - 4))
    wait_for_history_first $first || return 1

    # nothing below the watermark may survive, marker or record
    test "$(marker_versions | wc -l)" = "5" || return 1
    test "$(marker_versions | head -1)" = "$first" || return 1
    test "$(record_versions | head -1)" = "$first" || return 1

    # and the survivors are still whole
    test "$(marker_versions | tr '\n' ' ')" = "$(record_versions | tr '\n' ' ')" \
        || return 1
    test "$(ceph config log 100 | grep -c '^--- ')" = "5" || return 1
    ceph config log 100 | grep -q '0\.000000' && return 1

    # run_mon runs with --debug-mon 20, so every trim round is logged.  more
    # than one round is what makes this the resumable path rather than a single
    # pass that happened to fit
    test "$(grep -c '_trim_config_history trimmed changesets' $dir/mon.a.log)" \
        -ge 2 || return 1

    # the live config is untouched throughout
    test "$(ceph config-key get config/osd/osd_memory_target)" = "$last" || return 1

    return 0
}

main mon-config-history "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && \
#     ../qa/standalone/mon/mon-config-history.sh"
# End:
