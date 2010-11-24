#!/bin/bash -x

#
# Test the lost object logic
#

# Includes
source "`dirname $0`/test_common.sh"

# Functions
setup() {
        export CEPH_NUM_OSD=$1

        # Start ceph
        ./stop.sh

        # set recovery start to a really long time to ensure that we don't start recovery
        ./vstart.sh -d -n -o 'osd recovery delay start = 10000' || die "vstart failed"
}

recovery1_impl() {
        # Write lots and lots of objects
        write_objects 1 1 200 4000

        # Take down osd1
        stop_osd 1

        # Continue writing a lot of objects
        write_objects 2 2 200 4000

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
        setup 2
        recovery1_impl
}

lost1_impl() {
        # Write lots and lots of objects
        write_objects 1 1 20 8000

        # Take down osd1
        stop_osd 1

        # Continue writing a lot of objects
        write_objects 2 2 20 8000

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

        # Lose all objects.
	./ceph osd lost 0 --yes-i-really-mean-it

	# Unfound objects go away and are turned into lost objects.
	poll_cmd "./ceph pg debug unfound_objects_exist" FALSE 3 120
        [ $? -eq 1 ] || die "Unfound objects didn't go away."

	# Reading from a lost object gives back an error code.
	# TODO: check error code
	./rados -p data get obj01 $TEMPDIR/obj01 &&\
	  die "expected radostool error"
}

# TODO: lost2 test where we ask for an object while it's still unfound, and
# verify we get woken to an error when it's declared lost.

lost1() {
        setup 2
        lost1_impl
}

run() {
        recovery1 || die "test failed"

        lost1 || die "test failed"
}

$@
