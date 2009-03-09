
CCONF="$BINDIR/cconf"

conf=$ETCDIR"/cluster.conf"
runtime_conf=$ETCDIR"/ceph.conf"

hostname=`hostname | cut -d . -f 1`


# make sure cluster.conf exists
if [ ! -e $conf ]; then
    echo "$0: Cluster conf $conf not found"
    usage_exit
fi


check_host() {
    # what host is this daemon assigned to?
    host=`$CCONF -c $conf -s $name -s $type host`
    ssh=""
    dir=$PWD
    if [[ $host != "" ]]; then
	#echo host for $name is $host, i am $hostname
	if [[ $host != $hostname ]]; then
	    # skip, unless we're starting remote daemons too
	    if [[ $allhosts -eq 0 ]]; then
		return 1
	    fi

	    # we'll need to ssh into that host
	    ssh="ssh root@$host"
	    get_conf dir "$dir" "ssh path" $sections
	fi
    else
	host=$hostname
    fi

    echo -n "=== $name === "

    return 0
}

do_cmd() {
    [[ $verbose = 1 ]] && echo "--- $host:$dir# $1"
    if [[ $ssh = "" ]]; then
	ulimit -c unlimited
	bash -c "$1" || (echo failed. ; exit 1)
    else
	$ssh "cd $dir ; ulimit -c unlimited ; $1" || (echo failed. ; exit 1)
    fi
}

get_name_list() {
    orig=$1

    if [[ $orig = "" ]]; then
        # extract list of monitors, mdss, osds defined in startup.conf
	what=`$CCONF -c $conf -l mon | egrep -v '^mon$' ; \
	    $CCONF -c $conf -l mds | egrep -v '^mds$' ; \
	    $CCONF -c $conf -l osd | egrep -v '^osd$'`
	return
    fi

    what=""
    for f in "$orig"; do
	case $f in
	    mon | osd | mds)
		bit=`$CCONF -c $conf -l $f | egrep -v "^$f$"`
		what="$what $bit"
		;;
	    *)
		what="$what $f"
		;;
	esac
    done
}


get_val() {
  [ "$2" != "" ] && export $1=$2 || export $1=`$CCONF -c $conf "$3" "$4" "$5"`
}

get_val_bool() {
  if [ "$2" != "" ]; then
	export $1=$2
  else
	tmp=`$CCONF "$3" "$4" "$5"`
	export $1=$5

	[ "$tmp" == "0" ] && export $1=0
	[ "$tmp" == "false" ] && export $1=0
	[ "$tmp" == "1" ] && export $1=1
	[ "$tmp" == "true" ] && export $1=1
  fi
}

get_conf() {
	var=$1
	def=$2
	key=$3
	shift; shift; shift

	tmp=""
	while [ $# -ge 1 ]; do
		tmp=$tmp" -s $1"
		shift
	done
	eval "$var=\"`$CCONF -c $conf $tmp \"$key\" \"$def\"`\""
}

get_conf_bool() {
	get_conf "$@"

	eval "val=$"$1
	[ "$val" == "0" ] && export $1=0
	[ "$val" == "false" ] && export $1=0
	[ "$val" == "1" ] && export $1=1
	[ "$val" == "true" ] && export $1=1
}

