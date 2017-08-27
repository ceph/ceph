#!/usr/bin/env bash
set -x

#
# Test the lost object logic
#

# Includes
source "`dirname $0`/test_common.sh"

TEST_POOL=rbd

# Functions
setup() {
        export CEPH_NUM_OSD=$1
        vstart_config=$2

        # Start ceph
        ./stop.sh

        # set recovery start to a really long time to ensure that we don't start recovery
        ./vstart.sh -d -n -o "$vstart_config" || die "vstart failed"

	# for exiting pools set size not greater than number of OSDs,
	# so recovery from degraded ps is possible
	local changed=0
	for pool in `./ceph osd pool ls`; do
	    local size=`./ceph osd pool get ${pool} size | awk '{print $2}'`
	    if [ "${size}" -gt "${CEPH_NUM_OSD}" ]; then
		./ceph osd pool set ${pool} size ${CEPH_NUM_OSD}
		changed=1
	    fi
	done
	if [ ${changed} -eq 1 ]; then
	    # XXX: When a pool has degraded pgs due to size greater than number
	    # of OSDs, after decreasing the size the recovery still could stuck
	    # and requires an additional kick.
	    ./ceph osd out 0
	    ./ceph osd in 0
	fi

	poll_cmd "./ceph health" HEALTH_OK 1 30
}

recovery1_impl() {
        # Write lots and lots of objects
        write_objects 1 1 200 4000 $TEST_POOL

        # Take down osd1
        stop_osd 1

        # Continue writing a lot of objects
        write_objects 2 2 200 4000 $TEST_POOL

        # Bring up osd1
        restart_osd 1

        # Finish peering.
        sleep 15

        # Stop osd0.
        # At this point we have peered, but *NOT* recovered.
        # Objects should be lost.
        stop_osd 0

	poll_cmd "./ceph pg debug degraded_pgs_exist" TRUE 3 120
        [ $? -eq 1 ] || die "Failed to see degraded PGs."
	poll_cmd "./ceph pg debug unfound_objects_exist" TRUE 3 120
        [ $? -eq 1 ] || die "Failed to see unfound objects."
        echo "Got unfound objects."

        restart_osd 0
	sleep 20
	start_recovery 2

        # Turn on recovery and wait for it to complete.
	poll_cmd "./ceph pg debug unfound_objects_exist" FALSE 3 120
        [ $? -eq 1 ] || die "Failed to recover unfound objects."
	poll_cmd "./ceph pg debug degraded_pgs_exist" FALSE 3 120
        [ $? -eq 1 ] || die "Recovery never finished."
}

recovery1() {
        setup 2 'osd recovery delay start = 10000'
        recovery1_impl
}

lost1_impl() {
	local flags="$@"
	local lost_action=delete
	local pgs_unfound pg

	if is_set revert_lost $flags; then
	    lost_action=revert
	fi

        # Write lots and lots of objects
        write_objects 1 1 20 8000 $TEST_POOL

        # Take down osd1
        stop_osd 1

        # Continue writing a lot of objects
        write_objects 2 2 20 8000 $TEST_POOL

        # Bring up osd1
        restart_osd 1

        # Finish peering.
        sleep 15

        # Stop osd0.
        # At this point we have peered, but *NOT* recovered.
        # Objects should be lost.
        stop_osd 0

	# Since recovery can't proceed, stuff should be unfound.
	poll_cmd "./ceph pg debug unfound_objects_exist" TRUE 3 120
        [ $? -eq 1 ] || die "Failed to see unfound objects."

	pgs_unfound=`./ceph health detail |awk '$1 = "pg" && /[0-9] unfound$/ {print $2}'`

	[ -n "$pgs_unfound" ] || die "no pg with unfound objects"

	for pg in $pgs_unfound; do
	    ./ceph pg $pg mark_unfound_lost revert &&
	    die "mark_unfound_lost unexpectedly succeeded for pg $pg"
	done

	if ! is_set mark_osd_lost $flags && ! is_set rm_osd $flags; then
	    return
	fi

	if is_set try_to_fetch_unfound $flags; then
	  # Ask for an object while it's still unfound, and
	  # verify we get woken to an error when it's declared lost.
	  echo "trying to get one of the unfound objects"
	  (
	  ./rados -c ./ceph.conf -p $TEST_POOL get obj02 $TEMPDIR/obj02 &&\
	    die "expected radostool error"
	  ) &
	fi

	if is_set mark_osd_lost $flags; then
	  ./ceph osd lost 0 --yes-i-really-mean-it
	fi

	if is_set rm_osd $flags; then
	    ./ceph osd rm 0
	fi

	if ! is_set auto_mark_unfound_lost $flags; then
	    for pg in $pgs_unfound; do
		./ceph pg $pg mark_unfound_lost ${lost_action} ||
		  die "mark_unfound_lost failed for pg $pg"
	    done
	fi

	start_recovery 2

	# Unfound objects go away and are turned into lost objects.
	poll_cmd "./ceph pg debug unfound_objects_exist" FALSE 3 120
        [ $? -eq 1 ] || die "Unfound objects didn't go away."

	for pg in `ceph pg ls | awk '/^[0-9]/ {print $1}'`; do
	    ./ceph pg $pg mark_unfound_lost revert 2>&1 |
	      grep 'pg has no unfound objects' ||
	      die "pg $pg has unfound objects"
	done

	# Reading from a lost object gives back an error code.
	# TODO: check error code
	./rados -c ./ceph.conf -p $TEST_POOL get obj01 $TEMPDIR/obj01
	if [ lost_action = delete -a $? -eq 0 ]; then
	  die "expected radostool error"
	elif [ lost_action = revert -a $? -ne 0 ]; then
	  die "unexpected radostool error"
	fi

	if is_set try_to_fetch_unfound $flags; then
	  echo "waiting for the try_to_fetch_unfound \
radostool instance to finish"
	  wait
	fi
}

lost1() {
        setup 2 'osd recovery delay start = 10000'
        lost1_impl mark_osd_lost revert_lost
}

lost2() {
        setup 2 'osd recovery delay start = 10000'
        lost1_impl mark_osd_lost try_to_fetch_unfound
}

lost3() {
        setup 2 'osd recovery delay start = 10000'
        lost1_impl rm_osd
}

lost4() {
        setup 2 'osd recovery delay start = 10000'
        lost1_impl mark_osd_lost rm_osd
}

lost5() {
        setup 2 'osd recovery delay start = 10000'
        lost1_impl mark_osd_lost auto_mark_unfound_lost
}

all_osds_die_impl() {
        poll_cmd "./ceph osd stat" '3 up, 3 in' 20 240
        [ $? -eq 1 ] || die "didn't start 3 osds"

        stop_osd 0
        stop_osd 1
        stop_osd 2

	# wait for the MOSDPGStat timeout
        poll_cmd "./ceph osd stat" '0 up' 20 240
        [ $? -eq 1 ] || die "all osds weren't marked as down"
}

all_osds_die() {
	setup 3 'osd mon report interval max = 60
	osd mon report interval min = 3
	mon osd report timeout = 60'

	all_osds_die_impl
}

run() {
        recovery1 || die "test failed"

        lost1 || die "test failed"

	# XXX: try_to_fetch_unfound test currently hangs on "waiting for the
	# try_to_fetch_unfound radostool instance to finish"
	#lost2 || die "test failed"

	lost3 || die "test failed"

	lost4 || die "test failed"

	# XXX: automatically marking lost is not implemented
	#lost5 || die "test failed"

        all_osds_die || die "test failed"
}

if [ -z "$@" ]; then
	run
	echo OK
	exit 0
fi

$@
