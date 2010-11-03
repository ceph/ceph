#!/bin/bash -x

#
# Creates some unfound objects and then tests finding them.
#

# Includes
source "`dirname $0`/test_common.sh"

# Constants
my_write_objects() {
        write_objects $1 $2 10 1000000
}

setup() {
        # Start ceph
        ./stop.sh

        # set recovery start to a really long time to ensure that we don't start recovery
        CEPH_NUM_OSD=2 ./vstart.sh -d -n -o 'osd recovery delay start = 10000' || die "vstart failed"
}

do_test() {
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

        echo "There should be unfound objects."
        continue_prompt "to test finding the unfound objects."

        restart_osd 0
}

run() {
        setup

        do_test
}

$@
