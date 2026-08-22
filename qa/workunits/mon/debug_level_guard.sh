#!/bin/bash -ex
#
# Test the debug level guard: auto-revert of excessively high debug levels.
#

function expect_false()
{
	set -x
	if "$@"; then return 1; else return 0; fi
}

function get_daemon_debug() {
	# $1 = daemon (e.g. osd.0), $2 = subsys (e.g. debug_osd)
	ceph daemon "$1" config get "$2" | jq -r ".[\"$2\"]" | cut -d/ -f1
}

function wait_for_revert() {
	# $1 = daemon, $2 = subsys, $3 = max_wait, $4 = threshold
	local daemon="$1"
	local subsys="$2"
	local max_wait="$3"
	local threshold="$4"
	local waited=0
	while [ $waited -lt "$max_wait" ]; do
		local level
		level=$(get_daemon_debug "$daemon" "$subsys")
		if [ "$level" -lt "$threshold" ]; then
			return 0
		fi
		sleep 1
		waited=$((waited + 1))
	done
	echo "Timed out waiting for $subsys on $daemon to revert below $threshold"
	return 1
}

function wait_for_health_warning() {
	# $1 = health check code, $2 = max_wait
	local code="$1"
	local max_wait="$2"
	local waited=0
	while [ $waited -lt "$max_wait" ]; do
		if ceph health detail | grep -q "$code"; then
			return 0
		fi
		sleep 1
		waited=$((waited + 1))
	done
	echo "Timed out waiting for $code in ceph health detail"
	ceph health detail
	return 1
}

function wait_for_health_clear() {
	# $1 = health check code, $2 = max_wait
	local code="$1"
	local max_wait="$2"
	local waited=0
	while [ $waited -lt "$max_wait" ]; do
		if ! ceph health detail | grep -q "$code"; then
			return 0
		fi
		sleep 1
		waited=$((waited + 1))
	done
	echo "Timed out waiting for $code to clear"
	ceph health detail
	return 1
}

# Enable the guard with a short timeout for testing
ceph config set global high_debug_level_threshold 10
ceph config set global high_debug_level_reset_timeout 5

# Give daemons time to pick up config
sleep 2

# Test 1: Set high debug level on osd.0, verify it auto-reverts
echo "Test 1: auto-revert after timeout"
ceph tell osd.0 config set debug_osd 20
sleep 1
level=$(get_daemon_debug osd.0 debug_osd)
test "$level" -eq 20
wait_for_health_warning HIGH_DEBUG_LEVEL 15
echo "PASS: HIGH_DEBUG_LEVEL warning surfaced"
wait_for_revert osd.0 debug_osd 10 10
echo "PASS: debug_osd reverted"
wait_for_health_clear HIGH_DEBUG_LEVEL 30
echo "PASS: HIGH_DEBUG_LEVEL warning cleared"

# Test 2: Below threshold is not affected
echo "Test 2: below threshold not affected"
ceph tell osd.0 config set debug_osd 5
sleep 8
level=$(get_daemon_debug osd.0 debug_osd)
test "$level" -eq 5
echo "PASS: below-threshold level preserved"

# Test 3: Manual lower before timeout cancels revert
echo "Test 3: manual lower cancels revert"
ceph tell osd.0 config set debug_osd 20
sleep 2
ceph tell osd.0 config set debug_osd 3
sleep 8
level=$(get_daemon_debug osd.0 debug_osd)
test "$level" -eq 3
echo "PASS: manual lower preserved"

# Test 4: Disabled guard (threshold=0) means no revert
echo "Test 4: disabled guard"
ceph config set global high_debug_level_threshold 0
sleep 2
ceph tell osd.0 config set debug_osd 20
sleep 8
level=$(get_daemon_debug osd.0 debug_osd)
test "$level" -eq 20
# Clean up
ceph tell osd.0 config set debug_osd 1
echo "PASS: guard disabled, no revert"

# Cleanup
ceph config rm global high_debug_level_threshold
ceph config rm global high_debug_level_reset_timeout

echo "All debug level guard tests passed"
