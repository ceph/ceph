#!/usr/bin/env bash
set -x

#
# Creates some unfound objects and then tests finding them.
#

# Includes
source "`dirname $0`/test_common.sh"

TEST_POOL=rbd

# Functions
my_write_objects() {
        write_objects $1 $2 10 1000000 $TEST_POOL
}

setup() {
        export CEPH_NUM_OSD=$1

        # Start ceph
        ./stop.sh

        # set recovery start to a really long time to ensure that we don't start recovery
        ./vstart.sh -d -n -o 'osd recovery delay start = 10000
osd max scrubs = 0' || die "vstart failed"
}

osd_resurrection_1_impl() {
        # Write lots and lots of objects
        my_write_objects 1 2

        # Take down osd1
        stop_osd 1

        # Continue writing a lot of objects
        my_write_objects 3 4

        # Bring up osd1
        restart_osd 1

        # Finish peering.
        sleep 15

        # Stop osd0.
        # At this point we have peered, but *NOT* recovered.
        # Objects should be lost.
        stop_osd 0

	poll_cmd "./ceph pg debug unfound_objects_exist" TRUE 3 120
        [ $? -eq 1 ] || die "Failed to see unfound objects."
        echo "Got unfound objects."

        (
                ./rados -c ./ceph.conf -p $TEST_POOL get obj01 $TEMPDIR/obj01 || die "radostool failed"
        ) &
        sleep 5
        [ -e $TEMPDIR/obj01 ] && die "unexpected error: fetched unfound object?"

        restart_osd 0

	poll_cmd "./ceph pg debug unfound_objects_exist" FALSE 3 120
        [ $? -eq 1 ] || die "Failed to recover unfound objects."

        wait
        [ -e $TEMPDIR/obj01 ] || die "unexpected error: failed to fetched newly-found object"

        # Turn off recovery delay start and verify that every osd gets copies
        # of the correct objects.
        echo "starting recovery..."
        start_recovery 2

        # success
        return 0
}

osd_resurrection_1() {
        setup 2
        osd_resurrection_1_impl
}

stray_test_impl() {
        stop_osd 0
        # 0:stopped 1:active 2:active

        my_write_objects 1 1

        stop_osd 1
        sleep 15
        # 0:stopped 1:stopped(ver1) 2:active(ver1)

        my_write_objects 2 2

        restart_osd 1
        sleep 15
        # 0:stopped 1:active(ver1) 2:active(ver2)

        stop_osd 2
        sleep 15
        # 0:stopped 1:active(ver1) 2:stopped(ver2)

        restart_osd 0
        sleep 15
        # 0:active 1:active(ver1) 2:stopped(ver2)

	poll_cmd "./ceph pg debug unfound_objects_exist" TRUE 5 300
        [ $? -eq 1 ] || die "Failed to see unfound objects."

        #
        # Now, when we bring up osd2, it wil be considered a stray. However, it
        # has the version that we need-- the very latest version of the
        # objects.
        #

        restart_osd 2
        sleep 15

	poll_cmd "./ceph pg debug unfound_objects_exist" FALSE 4 240
        [ $? -eq 1 ] || die "Failed to discover unfound objects."

        echo "starting recovery..."
        start_recovery 3

        # success
        return 0
}

stray_test() {
        setup 3
        stray_test_impl
}

run() {
        osd_resurrection_1 || die "test failed"

        stray_test || die "test failed"
}

$@
