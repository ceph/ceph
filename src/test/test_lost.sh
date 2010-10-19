#!/bin/bash -x

#
# Simple test of recovery logic
#

# Constants
MAX_OBJS=100
OBJ_SIZE=1000000
TEMPDIR=`mktemp -d`
SDIR=`dirname $0`/..

# Initialization
cd $SDIR
[ -e "/dev/urandom" ] || die "need /dev/urandom"
trap cleanup INT TERM EXIT
rm -rf $TEMPDIR
mkdir -p $TEMPDIR || die "failed to make tempdir"

# Functions
cleanup() {
        rm -rf "${TEMPDIR}"
}

die() {
        echo $@
        exit 1
}

stop_osd() {
        osd_index=$1
        pidfile="out/osd.$osd_index.pid"
        if [ -e $pidfile ]; then
                kill `cat $pidfile` && return 0
        else
                echo "cosd process $osd_index is not running" 
        fi
        return 1
}

restart_osd() {
        osd_index=$1
        ./cosd -i $osd_index -c ceph.conf &
}

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

        # TODO: somehow constrain the number of PGs created by vstart.sh so that
        # situations are more reproducible
        CEPH_NUM_OSD=2 ./vstart.sh -d -n || die "vstart failed"
}

do_test() {
        # Write lots and lots of objects
        write_objects 1 3

        # Take down osd1
        stop_osd 1

        # Continue writing a lot of objects
        write_objects 4 6

        # Bring up osd1
        restart_osd 1

        # Give recovery some time to start
        sleep 20

        # Stop osd0 before recovery can complete
        stop_osd 0

        # Now we should be in LOST_REVERT
}

run() {
        setup

        do_test
}

$@
