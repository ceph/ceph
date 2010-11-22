#!/bin/bash -x

#
# Test the lost object logic
#

# Includes
source "`dirname $0`/test_common.sh"

# Functions
my_write_objects() {
        write_objects $1 $2 10 1000000
}

setup() {
        export CEPH_NUM_OSD=$1

        # Start ceph
        ./stop.sh

        # set recovery start to a really long time to ensure that we don't start recovery
        ./vstart.sh -d -n -o 'osd recovery delay start = 10000' || die "vstart failed"
}

recovery1_impl() {
        # Write lots and lots of objects
        my_write_objects 1 1

        # Take down osd1
        stop_osd 1

        # Continue writing a lot of objects
        my_write_objects 2 2

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

run() {
        recovery1 || die "test failed"
}

$@
