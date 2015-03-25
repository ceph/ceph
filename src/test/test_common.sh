#!/bin/bash -x

#
# test_common.sh
#
# Common routines for tests
#
#
# Environment variables that affect tests:
# KEEP_TEMPDIR                  If set, the tempdir will not be deleted
#                               when the test is over.
#

# Clean up the temporary directory
cleanup() {
        if [ -n ${TEMPDIR} ]; then
                rm -rf "${TEMPDIR}"
        fi
}

# Create a temporary directory where test files will be stored.
setup_tempdir() {
        TEMPDIR=`mktemp -d`
        if [ -z $KEEP_TEMPDIR ]; then
                trap cleanup INT TERM EXIT
        fi
}

# Standard initialization function for tests
init() {
        setup_tempdir
        cd `dirname $0`/..
}

# Exit with an error message.
die() {
        echo $@
        exit 1
}

# Test that flag is set (the element is found in the list)
is_set()
{
	local flag=$1; shift
	local flags="$@"
	local i

	for i in ${flags}; do
		if [ "${flag}" = "${i}" ]; then
			return 0
		fi
	done
	return 1
}

# Stop an OSD started by vstart
stop_osd() {
        osd_index=$1
        pidfile="out/osd.$osd_index.pid"
        if [ -e $pidfile ]; then
                if kill `cat $pidfile` ; then
                        poll_cmd "eval test -e $pidfile ; echo \$?" "1" 1 30
                        [ $? -eq 1 ] && return 0
                        echo "ceph-osd process did not terminate correctly"
                else
                        echo "kill `cat $pidfile` failed"
                fi
        else
                echo "ceph-osd process $osd_index is not running"
        fi
        return 1
}

# Restart an OSD started by vstart
restart_osd() {
        osd_index=$1
        ./ceph-osd -i $osd_index -c ceph.conf &
}

# Ask the user a yes/no question and get the response
yes_or_no_choice() {
        while true; do
                echo -n "${1} [y/n] "
                read ans
                case "${ans}" in
                        y|Y|yes|YES|Yes) return 0 ;;
                        n|N|no|NO|No) return 1 ;;
                        *) echo "Please type yes or no."
                        echo ;;
                esac
        done
}

# Block until the user says "continue" or "c"
continue_prompt() {
        prompt=${1:-"to go on"}
        while true; do
                echo "Please type 'c' or 'continue' ${prompt}."
                read ans
                case "${ans}" in
                        c|continue) return 0 ;;
                        *) echo ;;
                esac
        done
}

# Write a bunch of objects to rados
write_objects() {
        start_ver=$1
        stop_ver=$2
        num_objs=$3
        obj_size=$4
        pool=$5
        [ -d "${TEMPDIR}" ] || die "must setup_tempdir"
        for v in `seq $start_ver $stop_ver`; do
                chr=`perl -e "print chr(48+$v)"`
                head -c $obj_size /dev/zero  | tr '\0' "$chr" > $TEMPDIR/ver$v
                for i in `seq -w 1 $num_objs`; do
                        ./rados -c ./ceph.conf -p $pool put obj$i $TEMPDIR/ver$v || die "radostool failed"
                done
        done
}

read_objects() {
        ver=$1
        num_objs=$2
        obj_size=$3
        [ -d "${TEMPDIR}" ] || die "must setup_tempdir"
	chr=`perl -e "print chr(48+$ver)"`
	head -c $obj_size /dev/zero  | tr '\0' "$chr" > $TEMPDIR/exemplar
        for i in `seq -w 1 $num_objs`; do
                ./rados -c ./ceph.conf -p $pool get obj$i $TEMPDIR/out$i || die "radostool failed"
		cmp $TEMPDIR/out$i $TEMPDIR/exemplar || die "got back incorrect obj$i"
        done
}

poll_cmd() {
        command=$1
        search_str=$2
        polling_interval=$3
        total_time=$4

        t=0
        while [ $t -lt $total_time ]; do
                $command | grep "$search_str"
                [ $? -eq 0 ] && return 1
                sleep $polling_interval
                t=$(($t+$polling_interval))
        done

        return 0
}

dump_osd_store() {
        set +x
        echo "dumping osd store..."
        find ./dev/osd* -type f | grep obj | grep head$ | sort | while read file; do
                echo $file
                head -c 10 $file
                echo
        done
}

start_recovery() {
        CEPH_NUM_OSD=$1
        osd=0
        while [ $osd -lt $CEPH_NUM_OSD ]; do
                ./ceph -c ./ceph.conf tell osd.$osd debug kick_recovery_wq 0
                osd=$((osd+1))
        done
}

init
