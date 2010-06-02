#!/bin/sh

load=0
load_all=0
fname=""
libdir=""

BINDIR=`dirname $0`

load_opt="changed"

usage="usage: $0 [option]... -L <libdir> [cls_filename]\n"
usage=$usage"options:\n"
usage=$usage"\t-L, --libdir\n"
usage=$usage"\t-l, --load\n"
usage=$usage"\t-a, --load-all\n"

usage_exit() {
	printf "$usage"
	exit 1
}

err_exit() {
	echo "Error: $*"
	exit 1
}

while [ $# -ge 1 ]; do
case $1 in
    -L | --libdir )
	    shift
            libdir=$1
	    ;;
    -l | --load )
	    load=1
	    ;;
    -a | --load-all )
	    load_all=1
	    ;;
    * )
	    [ "$fname" != "" ] && usage_exit
	    fname=$1
	    ;;

esac
shift
done

[ "$libdir" == "" ] && usage_exit

[ "$fname" == "" ] && [ $load_all -eq 0 ] && usage_exit


load() {
	fn=$1
	$BINDIR/ceph class add -i $fn `$BINDIR/cclsinfo.sh $fn` $load_opt
}

load_all() {
	all=`find $libdir -name 'libcls_*.so'`;
	if [ "$all" != "" ]; then
		for fn in $all; do
			echo Loading class: $fn: `$BINDIR/cclsinfo.sh $fn`
			load $fn
		done
	fi
}

[ $load -eq 1 ] && load $fname
[ $load_all -eq 1 ] && load_all

