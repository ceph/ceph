#!/usr/bin/env bash
#
# Standalone tests for RADOS watch/notify on a Crimson OSD, with emphasis on
# the watch connection lifecycle: registration, notify delivery, notify
# timeout, abrupt client disconnect (connection reset) and re-establishing a
# watch afterwards.
#
# The watches are driven through the `rados` CLI:
#   rados -p P watch OBJ        - holds a watch open, prints "NOTIFY ..." events
#   rados -p P notify OBJ MSG   - sends a notify (10s client timeout)
#   rados -p P listwatchers OBJ - prints "watcher=<addr> client.<id> cookie=<c>"
#
# Run with:
#   cd build && ../qa/run-standalone.sh crimson/watch-notify.sh
# or a single case:
#   cd build && ../qa/run-standalone.sh "crimson/watch-notify.sh TEST_notify_delivery"

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

# Watch timeout used by the OSD for client watches (osd_client_watch_timeout).
# Kept comfortably larger than the 10s notify timeout baked into `rados notify`
# so a watch is still registered while a notify to it is in flight, yet small
# enough that reaping a dead watch does not make the tests crawl.
WATCH_TIMEOUT=25

# Per-watcher bookkeeping, keyed by a caller-supplied tag.
declare -A W_PID    # tag -> rados-watch pid
declare -A W_HOLD   # tag -> stdin holder pid (keeps the watch's stdin open)
declare -A W_FIFO   # tag -> fifo feeding the watch's stdin
declare -A W_OUT    # tag -> file capturing the watch's stdout/stderr

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7148" # git grep '\<7148\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    CEPH_ARGS+="--crimson_cpu_num=2 "
    CEPH_ARGS+="--osd_client_watch_timeout=$WATCH_TIMEOUT "

    export OUTDIR=${TMPDIR:-/tmp}/watch-notify-$$
    rm -rf "$OUTDIR"
    mkdir -p "$OUTDIR"
    trap 'rm -rf "$OUTDIR"' EXIT

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        echo "-------------- Prepare Test $func -------------------"
        setup $dir || return 1
        echo "-------------- Run Test $func -----------------------"
        $func $dir || { _cleanup_watchers; teardown $dir; return 1; }
        _cleanup_watchers
        echo "-------------- Teardown Test $func ------------------"
        teardown $dir || return 1
        echo "-------------- Complete Test $func ------------------"
    done
}

#
# Cluster / helper plumbing
#

# Bring up a single-OSD Crimson cluster with a crimson pool named "foo".
function _setup_crimson_cluster() {
    local dir=$1

    run_mon $dir a --osd_pool_default_size=1 --mon_allow_pool_size_one=true \
        --osd_pool_default_crimson=true || return 1
    run_mgr $dir x || return 1
    run_crimson_osd $dir 0 || return 1

    create_pool foo 1 1 || return 1
    # Confirm the pool really is a crimson pool.
    ceph osd pool ls detail --format json |
        jq -e '.[] | select(.pool_name == "foo") | (.flags_names // [] | index("crimson"))' \
        >/dev/null || return 1
    wait_for_clean || return 1
}

# Count the watchers currently registered on an object.
# Prints the count on success (0 is a legitimate answer). On a listwatchers
# failure it warns and prints the sentinel "ERR" - a value no real count can
# take - so callers (and in particular a "wait for 0" poll) never mistake a
# transient error for "no watchers left". "ERR" is deliberately non-numeric so
# it can't accidentally satisfy an arithmetic comparison either.
function _watch_count() {
    local pool=$1 obj=$2 out
    if ! out=$(rados -p "$pool" listwatchers "$obj" 2>/dev/null); then
        echo "WARNING: listwatchers $pool/$obj failed" >&2
        echo ERR
        return 0
    fi
    grep -c '^watcher=' <<<"$out" || true   # 0 matches is fine, not an error
}

# Poll until the watcher count on an object reaches the wanted value.
function _wait_watch_count() {
    local pool=$1 obj=$2 want=$3 timeout=${4:-30}
    local deadline=$(( $(date +%s) + timeout ))
    local saved_flag=${-//[^x]/}
    set +x
    local got
    while [ $(date +%s) -lt $deadline ]; do
        got=$(_watch_count "$pool" "$obj")
        if [ "$got" = "$want" ]; then
            [ -n "$saved_flag" ] && set -x
            return 0
        fi
        sleep 1
    done
    [ -n "$saved_flag" ] && set -x
    echo "Timed out waiting for $pool/$obj watcher count == $want (last: ${got:-none})"
    rados -p "$pool" listwatchers "$obj" || true
    return 1
}

# Poll until a watcher (by tag) has printed a NOTIFY line to its capture file.
# `rados notify` returns once the OSD has acked it, which is not the same moment
# the background `rados watch` process flushes its NOTIFY output - so callers
# must poll rather than grep once immediately after notify returns.
function _wait_notify_seen() {
    local tag=$1 timeout=${2:-15}
    local out="${W_OUT[$tag]}"
    local deadline=$(( $(date +%s) + timeout ))
    while [ $(date +%s) -lt $deadline ]; do
        grep -q '^NOTIFY' "$out" && return 0
        sleep 1
    done
    echo "watcher $tag did not receive NOTIFY; output:"
    cat "$out"
    return 1
}

# Start a background `rados watch` on an object and wait until it registers.
# The watch's stdin is fed from a fifo that is held open (but never written to)
# so `rados watch` blocks in getchar() and keeps the watch alive.
function _start_watcher() {
    local pool=$1 obj=$2 tag=$3
    local fifo="$OUTDIR/w-$tag.fifo"
    local out="$OUTDIR/w-$tag.out"
    rm -f "$fifo" "$out"
    mkfifo "$fifo" || return 1
    # Holder keeps the write end open so the reader never sees EOF.
    sleep 100000 > "$fifo" &
    W_HOLD[$tag]=$!
    rados -p "$pool" watch "$obj" < "$fifo" > "$out" 2>&1 &
    W_PID[$tag]=$!
    W_FIFO[$tag]="$fifo"
    W_OUT[$tag]="$out"
}

# Abruptly drop a watcher (SIGKILL) - no clean unwatch, so the OSD sees a
# connection reset. This is our "client disconnect" trigger.
# NOTE: only ever `wait` on the specific watcher pid - a bare `wait` would also
# block on the long-lived mon/mgr/osd daemons that run() started in the
# background.
function _kill_watcher() {
    local tag=$1
    if [ -n "${W_PID[$tag]:-}" ]; then
        kill -9 "${W_PID[$tag]}" 2>/dev/null || true
        wait "${W_PID[$tag]}" 2>/dev/null || true
        unset 'W_PID[$tag]'
    fi
}

# Kill every outstanding watcher and its stdin holder; called between tests.
function _cleanup_watchers() {
    local tag
    for tag in "${!W_PID[@]}"; do
        kill -9 "${W_PID[$tag]}" 2>/dev/null || true
        wait "${W_PID[$tag]}" 2>/dev/null || true
    done
    for tag in "${!W_HOLD[@]}"; do
        kill -9 "${W_HOLD[$tag]}" 2>/dev/null || true
        wait "${W_HOLD[$tag]}" 2>/dev/null || true
    done
    for tag in "${!W_FIFO[@]}"; do
        rm -f "${W_FIFO[$tag]}" 2>/dev/null || true
    done
    W_PID=(); W_HOLD=(); W_FIFO=(); W_OUT=()
}

#
# Test cases
#

# A single watch registers, and a clean unwatch (client exit) deregisters it.
function TEST_watch_register_unregister() {
    local dir=$1
    _setup_crimson_cluster $dir || return 1

    local obj=obj-register
    echo data | rados -p foo put $obj - || return 1

    local n=$(_watch_count foo $obj)
    [ "$n" = "0" ] || { echo "expected 0 watchers initially, got '$n'"; return 1; }

    _start_watcher foo $obj a || return 1
    _wait_watch_count foo $obj 1 30 || return 1

    # Clean shutdown: send a newline so `rados watch` returns from getchar() and
    # calls unwatch2(); the watcher should then deregister.
    echo > "${W_FIFO[a]}"
    wait "${W_PID[a]}" 2>/dev/null || true
    unset 'W_PID[a]'
    _wait_watch_count foo $obj 0 30 || return 1
}

# A notify is delivered to a live watcher (watcher prints NOTIFY, acks it, and
# the notifier returns success).
function TEST_notify_delivery() {
    local dir=$1
    _setup_crimson_cluster $dir || return 1

    local obj=obj-notify
    echo data | rados -p foo put $obj - || return 1

    _start_watcher foo $obj a || return 1
    _wait_watch_count foo $obj 1 30 || return 1

    timeout 30 rados -p foo notify $obj "hello" || return 1

    # The watcher should have observed the notify.
    _wait_notify_seen a 15 || return 1
}

# Notifying an object with no watchers completes promptly and successfully
# (empty completion, no timeout, no hang).
function TEST_notify_no_watchers() {
    local dir=$1
    _setup_crimson_cluster $dir || return 1

    local obj=obj-nowatch
    echo data | rados -p foo put $obj - || return 1

    local n=$(_watch_count foo $obj)
    [ "$n" = "0" ] || { echo "expected 0 watchers initially, got '$n'"; return 1; }

    local start=$(date +%s)
    timeout 30 rados -p foo notify $obj "hello" || return 1
    local elapsed=$(( $(date +%s) - start ))
    # With no watchers the notify must not wait out the notify timeout.
    [ $elapsed -lt 15 ] || { echo "notify with no watchers took ${elapsed}s"; return 1; }
}

# Several watchers on one object are all registered.
function TEST_multiple_watchers() {
    local dir=$1
    _setup_crimson_cluster $dir || return 1

    local obj=obj-multi
    echo data | rados -p foo put $obj - || return 1

    _start_watcher foo $obj a || return 1
    _start_watcher foo $obj b || return 1
    _start_watcher foo $obj c || return 1

    _wait_watch_count foo $obj 3 40 || return 1

    # A notify should reach all of them. Poll each watcher's output: notify
    # returning only means the OSD acked, not that every background watcher has
    # flushed its NOTIFY line yet.
    timeout 30 rados -p foo notify $obj "ping" || return 1
    local tag
    for tag in a b c; do
        _wait_notify_seen $tag 15 || return 1
    done
}

# An abruptly-disconnected watcher (client killed, connection reset) is
# eventually reaped from the object's watcher list.
function TEST_disconnect_reaped() {
    local dir=$1
    _setup_crimson_cluster $dir || return 1

    local obj=obj-disc
    echo data | rados -p foo put $obj - || return 1

    _start_watcher foo $obj a || return 1
    _wait_watch_count foo $obj 1 30 || return 1

    # Abrupt disconnect: the OSD sees a connection reset (no unwatch).
    _kill_watcher a

    # The watch must be dropped within roughly the watch timeout.
    _wait_watch_count foo $obj 0 $(( WATCH_TIMEOUT + 25 )) || return 1
}

# A notify to an object whose only watcher has just been killed must complete
# (report the watcher as missed / time out) rather than hang. This exercises
# the notify-timeout path and Watch::cancel_notify().
function TEST_notify_dead_watcher_times_out() {
    local dir=$1
    _setup_crimson_cluster $dir || return 1

    local obj=obj-deadnotify
    echo data | rados -p foo put $obj - || return 1

    _start_watcher foo $obj a || return 1
    _wait_watch_count foo $obj 1 30 || return 1

    # Abruptly kill the watcher. The reset only *disconnects* the watch (drops
    # its conn and re-arms the watch timeout); it does not deregister it, so the
    # watch stays registered - and thus a notify target - for up to
    # WATCH_TIMEOUT.
    _kill_watcher a

    # Prove the notify below actually runs against a dead-but-registered
    # watcher. Without this the reset could (in some future change) drop the
    # watch first, and the notify would then trivially succeed against an empty
    # watcher list - passing while testing nothing.
    local n=$(_watch_count foo $obj)
    [ "$n" = "1" ] || { echo "expected the dead watch still registered, got '$n'"; return 1; }

    # The dead watcher can never ack, so `rados notify` (10s notify timeout)
    # must (a) finish rather than hang, and (b) actually report that watcher as
    # timed out. rados prints "timeout client.<gid> cookie <c>" for each watcher
    # that missed the notify (see tools/rados/rados.cc). We require that line:
    #   - a trivial success (no watchers) prints nothing and returns 0;
    #   - an unrelated CLI/connection error prints no "timeout client." line;
    # so matching it is what distinguishes the real notify-timeout path (which
    # exercises Watch::cancel_notify()) from both of those.
    local out rc=0
    out=$(timeout 40 rados -p foo notify $obj "anyone?" 2>&1) || rc=$?
    echo "$out"
    [ $rc -ne 124 ] || { echo "notify to a dead watcher hung"; return 1; }
    echo "$out" | grep -q '^timeout client\.' || {
        echo "notify did not report the dead watcher as timed out (rc=$rc)"; return 1;
    }
}

# After a watcher disconnects, a fresh watch on the same object can be
# established and receives notifies - the object's watch machinery recovers.
function TEST_rewatch_after_disconnect() {
    local dir=$1
    _setup_crimson_cluster $dir || return 1

    local obj=obj-rewatch
    echo data | rados -p foo put $obj - || return 1

    _start_watcher foo $obj a || return 1
    _wait_watch_count foo $obj 1 30 || return 1
    _kill_watcher a
    _wait_watch_count foo $obj 0 $(( WATCH_TIMEOUT + 25 )) || return 1

    # New watch on the same object.
    _start_watcher foo $obj b || return 1
    _wait_watch_count foo $obj 1 30 || return 1

    timeout 30 rados -p foo notify $obj "again" || return 1
    _wait_notify_seen b 15 || return 1
}

main watch-notify "$@"
