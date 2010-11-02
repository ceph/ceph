#!/bin/bash -x

#
# Simple test of recovery logic
#

# Includes
dir=`dirname $0`
source $dir/test_common.sh
setup_tempdir
cd $dir/..

# Constants
MAX_OBJS=50
OBJ_SIZE=1000000

write_objects() {
        start_ver=$1
        stop_ver=$2
        for v in `seq $start_ver $stop_ver`; do
                chr=`perl -e "print chr(48+$v)"`
                head -c $OBJ_SIZE /dev/zero  | tr '\0' "$chr" > $TEMPDIR/ver$v
                for i in `seq -w 1 $MAX_OBJS`; do
                        ./rados -p data put obj$i $TEMPDIR/ver$v || die "radostool failed"
                done
        done
}

setup() {
        # Start ceph
        ./stop.sh

        # set recovery start to a really long time to ensure that we don't start recovery
        CEPH_NUM_OSD=2 ./vstart.sh -d -n -o 'osd recovery delay start = 10000' || die "vstart failed"
}

do_test() {
        # Write lots and lots of objects
        write_objects 1 2

        # Take down osd1
        stop_osd 1

        # Continue writing a lot of objects
        write_objects 3 4

        # Bring up osd1
        restart_osd 1

        # Finish peering.
        sleep 15

        # Stop osd0.
        # At this point we have peered, but *NOT* recovered.
        # Objects should be lost.
        stop_osd 0

        # Now we should be in LOST_REVERT
}

run() {
        setup

        do_test
}

$@
