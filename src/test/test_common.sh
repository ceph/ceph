#!/bin/bash -x

#
# test_common.sh
#
# Common routines for tests
#

# Functions
cleanup() {
        rm -rf "${TEMPDIR}"
}

setup_tempdir() {
        TEMPDIR=`mktemp -d`
        trap cleanup INT TERM EXIT
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
