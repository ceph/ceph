#!/usr/bin/env bash
set -ex

# ./test.sh                       # Default: parallel mode, 30-min timeout
# ./test.sh --serial              # Serial mode, 30-min timeout
# ./test.sh --crimson             # Crimson mode, 30-min timeout
# ./test.sh --timeout 3600        # Parallel mode, 60-min timeout
# ./test.sh --serial --timeout 60 # Serial mode, 1-min timeout
# ./test.sh --crimson --timeout 0 # Crimson mode, no timeout

# First argument must be either --serial or --crimson or nothing
parallel=1
crimson=0
if [ "$1" = "--serial" ]; then
    parallel=0
    shift # Remove the first argument from the list so timeout can be processed next
elif [ "$1" = "--crimson" ]; then
    parallel=0
    crimson=1
    shift
fi

# After processing the first arg, check for --timeout
timeout=1800  # 30 minutes default value
if [ "$1" = "--timeout" ]; then
    shift
    if [ -n "$1" ] && [[ "$1" =~ ^[0-9]+$ ]]; then
        timeout=$1
        shift # Remove the timeout value from the list so color can be processed next
    else
        echo "Invalid or missing timeout value after --timeout. Must be a number."
        exit 1
    fi
fi

color="" # Default color setting for gtest in terminal (-t)
[ -t 1 ] && color="--gtest_color=yes"

function cleanup() {
    pkill -P $$ || true
}
trap cleanup EXIT ERR HUP INT QUIT

GTEST_OUTPUT_DIR=${TESTDIR:-$(mktemp -d)}/archive/unit_test_xml_report
mkdir -p $GTEST_OUTPUT_DIR

declare -A pids

for f in \
    api_aio api_aio_pp \
    api_io api_io_pp \
    api_asio api_list \
    api_lock api_lock_pp \
    api_misc api_misc_pp \
    api_tier_pp \
    api_pool \
    api_snapshots api_snapshots_pp \
    api_stat api_stat_pp \
    api_watch_notify api_watch_notify_pp \
    api_cmd api_cmd_pp \
    api_service api_service_pp \
    api_c_write_operations \
    api_c_read_operations \
    list_parallel \
    open_pools_parallel \
    delete_pools_parallel
do
    if [ $parallel -eq 1 ]; then
	r=`printf '%25s' $f`
	ff=`echo $f | awk '{print $1}'`
	bash -o pipefail -exc "ceph_test_rados_$f --gtest_output=xml:$GTEST_OUTPUT_DIR/$f.xml $color 2>&1 | tee ceph_test_rados_$ff.log | sed \"s/^/$r: /\"" &
	pid=$!
	echo "test $f on pid $pid"
	pids[$f]=$pid
    else
	ceph_test_rados_$f
    fi
done

for f in \
    cls cmd handler_error io ec_io list ec_list misc pool read_operations snapshots \
    watch_notify write_operations
do
    if [ $parallel -eq 1 ]; then
	r=`printf '%25s' $f`
	ff=`echo $f | awk '{print $1}'`
	bash -o pipefail -exc "ceph_test_neorados_$f --gtest_output=xml:$GTEST_OUTPUT_DIR/neorados_$f.xml $color 2>&1 | tee ceph_test_neorados_$ff.log | sed \"s/^/$r: /\"" &
	pid=$!
	echo "test $f on pid $pid"
	pids[$f]=$pid
    else
	if [ $crimson -eq 1 ]; then
		if [ $f = "ec_io" ] || [ $f = "ec_list" ]; then
			echo "Skipping EC with Crimson"
			continue
		fi
	fi
	ceph_test_neorados_$f
    fi
done

ret=0
if [ $parallel -eq 1 ]; then
for t in "${!pids[@]}"
do
    # Set timeout values
    max_wait=$timeout
    waited=0
    check_interval=10
    pid=${pids[$t]}
    echo "Checking Test $t (PID $pid)..."
    # Check in a loop with timeout
    # kill -0 checks if the process is running
    # and 2 >/dev/null suppresses error messages if the process is not found
    while kill -0 $pid 2>/dev/null; do
        sleep $check_interval
        waited=$((waited + check_interval))
        echo "Waiting for test $t (PID $pid)... waited $waited seconds"
        if [ $waited -ge $max_wait ]; then
        # Process timed out
        echo "ERROR: Test $t ($pid) - TIMED OUT after $max_wait seconds"

        # Create fallback XML file
        xml_path="$GTEST_OUTPUT_DIR/$t.xml"
        if [[ $t == neorados_* ]]; then
            xml_path="$GTEST_OUTPUT_DIR/neorados_$t.xml"
        fi

        echo "Creating fallback XML report at $xml_path"
        cat > "$xml_path" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<testsuites tests="1" failures="1" disabled="0" errors="0" time="${max_wait}.000" timestamp="$(date -Iseconds)" name="AllTests">
  <testsuite name="Timeout" tests="1" failures="1" disabled="0" errors="0" time="${max_wait}.000">
    <testcase name="UnknownTestCase" status="timeout" time="${max_wait}.000" classname="${t}">
      <failure message="Test suite timed out after ${max_wait} seconds" type="timeout">
        The test suite ${t} (PID ${pid}) exceeded the maximum allowed execution time of ${max_wait} seconds.
        Unable to determine which specific test case was running when the timeout occurred.
        Please check the logs for more details, this may indicate a hang, deadlock, or extremely slow performance.
      </failure>
    </testcase>
  </testsuite>
</testsuites>
EOF
        kill -9 $pid 2>/dev/null || true
        ret=1
        break
        fi
    done
    # Only wait after process has ended naturally or been killed
    # We only call wait after determining that the process is no longer running
    # So this won't hang indefinitely like https://tracker.ceph.com/issues/70772
    wait $pid 2>/dev/null || {
        echo "ERROR: Test $t (PID $pid) failed with non-zero exit status"
        echo "Check the logs for failures in $t"
        ret=1
    }
done
fi

exit $ret
