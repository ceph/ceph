#!/bin/bash
#
# Exercise "ceph config-key rm-range".
#
set -e

function expect_true()
{
    if "$@"; then return 0; else return 1; fi
}

function expect_false()
{
    if "$@"; then return 1; else return 0; fi
}

function expect_key()
{
    expect_true ceph config-key get "$1" > /dev/null
}

function expect_no_key()
{
    expect_false ceph config-key get "$1" > /dev/null
}

function wait_for_log()
{
    local log="$1"
    local pattern="$2"
    local tries=60
    while [ $tries -gt 0 ]; do
        if grep -aq "$pattern" "$log"; then
            return 0
        fi
        sleep 1
        tries=$((tries - 1))
    done
    echo "timed out waiting for '$pattern' in $log" >&2
    return 1
}

ceph config-key set "test/range/key1" "value1"
ceph config-key set "test/range/key2" "value2"
ceph config-key set "test/range/key3" "value3"
ceph config-key set "test/range/subdir/key4" "value4"
ceph config-key set "test/range/subdir/key5" "value5"
ceph config-key set "test/other/key6" "value6"

# Bounded range: [prefix/start, prefix/end), so key3 survives.
expect_true ceph config-key rm-range "test/range" "key1" "key3"
expect_no_key "test/range/key1"
expect_no_key "test/range/key2"
expect_key "test/range/key3"
expect_key "test/range/subdir/key4"
expect_key "test/range/subdir/key5"
expect_key "test/other/key6"

# No bounds: every key under the prefix, leaving sibling prefixes alone.
expect_true ceph config-key rm-range "test/range"
expect_no_key "test/range/key3"
expect_no_key "test/range/subdir/key4"
expect_no_key "test/range/subdir/key5"
expect_key "test/other/key6"

# Only a start bound: open above.
ceph config-key set "test/sb/ka" "v"
ceph config-key set "test/sb/kb" "v"
ceph config-key set "test/sb/kc" "v"
expect_true ceph config-key rm-range "test/sb" "kb"
expect_key "test/sb/ka"
expect_no_key "test/sb/kb"
expect_no_key "test/sb/kc"

# Only an end bound: open below.
ceph config-key set "test/eb/ka" "v"
ceph config-key set "test/eb/kb" "v"
ceph config-key set "test/eb/kc" "v"
expect_true ceph config-key rm-range "test/eb" "" "kc"
expect_no_key "test/eb/ka"
expect_no_key "test/eb/kb"
expect_key "test/eb/kc"

# Keys sorting above '~' (0x7e) must still be removed by a prefix sweep.
ceph config-key set "test/tilde/aaa" "v"
ceph config-key set "test/tilde/}mid" "v"
ceph config-key set "test/tilde/~high" "v"
expect_true ceph config-key rm-range "test/tilde"
expect_no_key "test/tilde/aaa"
expect_no_key "test/tilde/}mid"
expect_no_key "test/tilde/~high"

# Removing nothing is not an error.
expect_true ceph config-key rm-range "test/nonexistent" "aaa" "zzz"
expect_true ceph config-key rm-range "test/nonexistent"

# Rejected ranges leave the store untouched.
ceph config-key set "test/empty/key1" "value1"
expect_false ceph config-key rm-range "test/empty" "key1" "key1"
expect_false ceph config-key rm-range "test/empty" "zzz" "aaa"
expect_key "test/empty/key1"
ceph config-key rm "test/empty/key1"

# A bulk removal must be committed as a single range operation rather than
# one operation per key: that is the whole point of rm-range, and expanding
# it per key would multiply the size of the paxos transaction that every
# monitor has to replicate and store.
MON_ID=$(ceph mon dump 2>/dev/null | awk '/^0:/ {print $NF}' | sed 's/^mon\.//' | head -1)
if [ -z "$MON_ID" ]; then
    MON_ID=a
fi
MON_LOG="out/mon.$MON_ID.log"
if [ -w "$MON_LOG" ]; then
    ceph tell mon.$MON_ID config set debug_mon 20
    for i in $(seq 1 200); do
        ceph config-key set "test/bulk/k$(printf %04d $i)" "v"
    done
    expect_true ceph config-key rm-range "test/bulk"
    wait_for_log "$MON_LOG" "encode_pending rm_range \[test/bulk,"
    # the individual keys must not appear as per-key removals
    if grep -aq "encode_pending rm test/bulk/k" "$MON_LOG"; then
        echo "rm-range was expanded into per-key removals" >&2
        exit 1
    fi
    expect_no_key "test/bulk/k0001"
    expect_no_key "test/bulk/k0200"
    ceph tell mon.$MON_ID config set debug_mon 0
fi

# Subscribers are told about the range itself, not about each removed key,
# and not by being resynced: keys are ordered, so a subscriber applies the
# interval to its own ordered copy. Watch the active mgr's config store cache.
ACTIVE_MGR_ID=$(ceph mgr dump | jq -r '.active_name')
MGR_LOG="out/mgr.$ACTIVE_MGR_ID.log"
if [ -w "$MGR_LOG" ]; then
    ceph tell mgr.$ACTIVE_MGR_ID config set debug_mgr 20

    ceph config-key set "config/mgr.$ACTIVE_MGR_ID/rmrange_probe_a" "1"
    ceph config-key set "config/mgr.$ACTIVE_MGR_ID/rmrange_probe_b" "2"
    wait_for_log "$MGR_LOG" \
        "set config/mgr.$ACTIVE_MGR_ID/rmrange_probe_b"

    MGR_MARK=$(wc -l < "$MGR_LOG")
    expect_true ceph config-key rm-range \
        "config/mgr.$ACTIVE_MGR_ID/rmrange_probe"

    # the update must carry a range, applied incrementally
    wait_for_log "$MGR_LOG" "update_kv_data  rm range \[config/mgr"
    expect_no_key "config/mgr.$ACTIVE_MGR_ID/rmrange_probe_a"
    expect_no_key "config/mgr.$ACTIVE_MGR_ID/rmrange_probe_b"

    # and it must not have degenerated into a resync of the whole prefix,
    # which would make the update cost scale with the surviving keys
    if tail -n +$MGR_MARK "$MGR_LOG" |
            grep -aq "update_kv_data full update on config/"; then
        echo "range removal resynced the prefix instead of sending a range" >&2
        exit 1
    fi

    # a removal outside the watched prefix must not reach the subscriber
    ceph config-key set "config/mgr.$ACTIVE_MGR_ID/rmrange_keep" "1"
    wait_for_log "$MGR_LOG" "set config/mgr.$ACTIVE_MGR_ID/rmrange_keep"
    ceph config-key set "rmrange_elsewhere/k1" "1"
    expect_true ceph config-key rm-range "rmrange_elsewhere"
    expect_key "config/mgr.$ACTIVE_MGR_ID/rmrange_keep"
    ceph config-key rm "config/mgr.$ACTIVE_MGR_ID/rmrange_keep"

    ceph tell mgr.$ACTIVE_MGR_ID config set debug_mgr 0
fi

ceph config-key rm-range "test"

echo OK
