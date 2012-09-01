#!/bin/sh

test -d dev/osd0/. && test -e dev/sudo && SUDO="sudo"

do_killall() {
	pg=`pgrep -f ceph-run.*$1`
	[ -n "$pg" ] && kill $pg
	$SUDO killall $1
}

usage="usage: $0 [all] [mon] [mds] [osd]\n"

stop_all=1
stop_mon=0
stop_mds=0
stop_osd=0
stop_rgw=0

while [ $# -ge 1 ]; do
    case $1 in
	all )
	    stop_all=1
	    ;;
	mon | ceph-mon )
	    stop_mon=1
	    stop_all=0
	    ;;
	mds | ceph-mds )
	    stop_mds=1
	    stop_all=0
	    ;;
	osd | ceph-osd )
	    stop_osd=1
	    stop_all=0
	    ;;
	* )
	    printf "$usage"
	    exit
    esac
    shift
done

if [ $stop_all -eq 1 ]; then
	killall ceph-mon ceph-mds ceph-osd radosgw lt-radosgw apache2
	pkill -f valgrind.bin.\*ceph-mon
	$SUDO pkill -f valgrind.bin.\*ceph-osd
	pkill -f valgrind.bin.\*ceph-mds
else
	[ $stop_mon -eq 1 ] && do_killall ceph-mon
	[ $stop_mds -eq 1 ] && do_killall ceph-mds
	[ $stop_osd -eq 1 ] && do_killall ceph-osd
	[ $stop_rgw -eq 1 ] && do_killall radosgw lt-radosgw apache2
fi
