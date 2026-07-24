#!/bin/bash
#
# Test script for config-key rm-range functionality
# This script tests the new range deletion functionality in KVMonitor
#
set -e

function expect_true()
{
    # expect the command to return true
    if "$@"; then return 0; else return 1; fi
}

function expect_false()
{
    # expect the command to return false
    if "$@"; then return 1; else return 0; fi
}

# Setup test data
ceph config-key set "test/range/key1" "value1"
ceph config-key set "test/range/key2" "value2"
ceph config-key set "test/range/key3" "value3"
ceph config-key set "test/range/subdir/key4" "value4"
ceph config-key set "test/range/subdir/key5" "value5"
ceph config-key set "test/other/key6" "value6"

# Test 1: Range deletion with start and end bounds
expect_true ceph config-key rm-range "test/range" "key1" "key3"

# Check that the keys are deleted
expect_false ceph config-key get "test/range/key1"
expect_false ceph config-key get "test/range/key2"
expect_true ceph config-key get "test/range/key3"
expect_true ceph config-key get "test/range/subdir/key4"
expect_true ceph config-key get "test/range/subdir/key5"
expect_true ceph config-key get "test/other/key6"

# Test 2: Prefix deletion (remove all keys with prefix)
expect_true ceph config-key rm-range "test/range"

# Check that all test/range keys are deleted
expect_false ceph config-key get "test/range/key3"
expect_false ceph config-key get "test/range/subdir/key4"
expect_false ceph config-key get "test/range/subdir/key5"
expect_true ceph config-key get "test/other/key6"

# Test 3: Range deletion on non-existent keys (start > end)
expect_false ceph config-key rm-range "nonexistent/prefix" "start" "end"

# Test 4: Range deletion on non-existent keys (start < end), no error
expect_true ceph config-key rm-range "test/range" "key_nonexistent1" "key_nonexistent2"

# Test 4: Empty range (start == end, should delete nothing)
expect_true ceph config-key set "test/empty/key1" "value1"
expect_false ceph config-key rm-range "test/empty" "key1" "key1"
expect_true ceph config-key get "test/empty/key1"

# Test 5: Range deletion on mgr config cache
ceph config-key set "config/mgr.x/osd_debug_misdirected_ops" "true"
ceph config-key set "config/mgr.x/osd_scrub_load_threshold" "1000"
ceph config-key set "config/mgr.x/osd_scrub_sleep" "1000"

# use log to check if the keys are deleted in mgr config cache
ACTIVE_MGR_ID=$(ceph mgr dump | jq -r '.active_name')
ceph daemon mgr.$ACTIVE_MGR_ID config set debug_mgr 20
expect_true ceph config-key rm-range "config/mgr.x" "osd_debug_misdirected_ops" "osd_scrub_sleep"
expect_true tail -n 10000 out/mgr.$ACTIVE_MGR_ID.log | grep "update_kv_data" |grep "rm osd_scrub_load_threshold"
expect_false tail -n 10000 out/mgr.$ACTIVE_MGR_ID.log | grep "update_kv_data" |grep "rm osd_scrub_sleep"

# Check that the keys are deleted
expect_false ceph config-key get "config/mgr.x/osd_debug_misdirected_ops"
expect_false ceph config-key get "config/mgr.x/osd_scrub_load_threshold"
expect_true ceph config-key get "config/mgr.x/osd_scrub_sleep"

# Cleanup
ceph config-key rm-range "config/mgr" "o" "o~"
ceph config-key rm-range "config/mgr.x" "o" "o~"
ceph config-key rm-range "test" "a" "z~"
