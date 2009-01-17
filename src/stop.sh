#!/bin/bash

do_killall() {
	pg=`pgrep -f crun.*$1`
	[ "$pg" != "" ] && kill $pg
	killall $1
}

usage="usage: $0 [all] [mon] [mds] [osd]\n"

let stop_all=1
let stop_mon=0
let stop_mds=0
let stop_osd=0

while [ $# -ge 1 ]; do
    case $1 in
	all )
	    stop_all=1
	    ;;
	mon | cmon )
	    stop_mon=1
	    stop_all=0
	    ;;
	mds | cmds )
	    stop_mds=1
	    stop_all=0
	    ;;
	osd | cosd )
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
	killall crun cmon cmds cosd
else
	[ $stop_mon -eq 1 ] && do_killall cmon
	[ $stop_mds -eq 1 ] && do_killall cmds
	[ $stop_osd -eq 1 ] && do_killall cosd
fi
