#!/bin/sh

CCONF="$BINDIR/ceph-conf"

default_conf=$ETCDIR"/ceph.conf"
conf=$default_conf

hostname=`hostname -s`

verify_conf() {
    # fetch conf?
    if [ -x "$ETCDIR/fetch_config" ] && [ "$conf" = "$default_conf" ]; then
	conf="/tmp/fetched.ceph.conf.$$"
	echo "[$ETCDIR/fetch_config $conf]"
	if $ETCDIR/fetch_config $conf && [ -e $conf ]; then true ; else
	    echo "$0: failed to fetch config with '$ETCDIR/fetch_config $conf'"
	    exit 1
	fi
	# yay!
    else
        # make sure ceph.conf exists
	if [ ! -e $conf ]; then
	    if [ "$conf" = "$default_conf" ]; then
		echo "$0: ceph conf $conf not found; system is not configured."
		exit 0
	    fi
	    echo "$0: ceph conf $conf not found!"
	    usage_exit
	fi
    fi
}


check_host() {
    # what host is this daemon assigned to?
    host=`$CCONF -c $conf -n $type.$id host`
    if [ "$host" = "localhost" ]; then
	echo "$0: use a proper short hostname (hostname -s), not 'localhost', in $conf section $type.$id; skipping entry"
	return 1
    fi
    if expr match "$host" '.*\.' > /dev/null 2>&1; then
	echo "$0: $conf section $type.$id"
	echo "contains host=$host, which contains dots; this is probably wrong"
	echo "It must match the result of hostname -s"
    fi
    ssh=""
    rootssh=""
    sshdir=$PWD
    get_conf user "" "user"

    #echo host for $name is $host, i am $hostname

    if [ -e "/var/lib/ceph/$type/ceph-$id/upstart" ]; then
	return 1
    fi

    # sysvinit managed instance in standard location?
    if [ -e "/var/lib/ceph/$type/ceph-$id/sysvinit" ]; then
	host="$hostname"
	echo "=== $type.$id === "
	return 0
    fi

    # ignore all sections without 'host' defined
    if [ -z "$host" ]; then
	return 1
    fi

    if [ "$host" != "$hostname" ]; then
	# skip, unless we're starting remote daemons too
	if [ $allhosts -eq 0 ]; then
	    return 1
	fi

	# we'll need to ssh into that host
	if [ -z "$user" ]; then
	    ssh="ssh $host"
	else
	    ssh="ssh $user@$host"
	fi
	rootssh="ssh root@$host"
	get_conf sshdir "$sshdir" "ssh path"
    fi

    echo "=== $type.$id === "

    return 0
}

do_cmd() {
    if [ -z "$ssh" ]; then
	[ $verbose -eq 1 ] && echo "--- $host# $1"
	ulimit -c unlimited
	whoami=`whoami`
	if [ "$whoami" = "$user" ] || [ -z "$user" ]; then
	    bash -c "$1" || { [ -z "$3" ] && echo "failed: '$1'" && exit 1; }
	else
	    sudo su $user -c "$1" || { [ -z "$3" ] && echo "failed: '$1'" && exit 1; }
	fi
    else
	[ $verbose -eq 1 ] && echo "--- $ssh $2 \"if [ ! -d $sshdir ]; then mkdir -p $sshdir; fi; cd $sshdir ; ulimit -c unlimited ; $1\""
	$ssh $2 "if [ ! -d $sshdir ]; then mkdir -p $sshdir; fi; cd $sshdir ; ulimit -c unlimited ; $1" || { [ -z "$3" ] && echo "failed: '$ssh $1'" && exit 1; }
    fi
}

do_cmd_okfail() {
    ERR=0
    if [ -z "$ssh" ]; then
	[ $verbose -eq 1 ] && echo "--- $host# $1"
	ulimit -c unlimited
	whoami=`whoami`
	if [ "$whoami" = "$user" ] || [ -z "$user" ]; then
	    bash -c "$1" || { [ -z "$3" ] && echo "failed: '$1'" && ERR=1 && return 1; }
	else
	    sudo su $user -c "$1" || { [ -z "$3" ] && echo "failed: '$1'" && ERR=1 && return 1; }
	fi
    else
	[ $verbose -eq 1 ] && echo "--- $ssh $2 \"if [ ! -d $sshdir ]; then mkdir -p $sshdir; fi; cd $sshdir ; ulimit -c unlimited ; $1\""
	$ssh $2 "if [ ! -d $sshdir ]; then mkdir -p $sshdir; fi; cd $sshdir ; ulimit -c unlimited ; $1" || { [ -z "$3" ] && echo "failed: '$ssh $1'" && ERR=1 && return 1; }
    fi
    return 0
}

do_root_cmd() {
    if [ -z "$ssh" ]; then
	[ $verbose -eq 1 ] && echo "--- $host# $1"
	ulimit -c unlimited
	whoami=`whoami`
	if [ "$whoami" = "root" ]; then
	    bash -c "$1" || { echo "failed: '$1'" ; exit 1; }
	else
	    sudo bash -c "$1" || { echo "failed: '$1'" ; exit 1; }
	fi
    else
	[ $verbose -eq 1 ] && echo "--- $rootssh $2 \"if [ ! -d $sshdir ]; then mkdir -p $sshdir; fi ; cd $sshdir ; ulimit -c unlimited ; $1\""
	$rootssh $2 "if [ ! -d $sshdir ]; then mkdir -p $sshdir; fi ; cd $sshdir; ulimit -c unlimited ; $1" || { echo "failed: '$rootssh $1'" ; exit 1; }
    fi
}

get_local_daemon_list() {
    type=$1
    if [ -d "/var/lib/ceph/$type" ]; then
	for i in `find -L /var/lib/ceph/$type -mindepth 1 -maxdepth 1 -type d -printf '%f\n'`; do
	    if [ -e "/var/lib/ceph/$type/$i/sysvinit" ]; then
		id=`echo $i | sed 's/[^-]*-//'`
		local="$local $type.$id"
	    fi
	done
    fi
}

get_local_name_list() {
    # enumerate local directories
    local=""
    get_local_daemon_list "mon"
    get_local_daemon_list "osd"
    get_local_daemon_list "mds"
}

get_name_list() {
    orig="$*"

    # extract list of monitors, mdss, osds defined in startup.conf
    allconf="$local "`$CCONF -c $conf -l mon | egrep -v '^mon$' || true ; \
	$CCONF -c $conf -l mds | egrep -v '^mds$' || true ; \
	$CCONF -c $conf -l osd | egrep -v '^osd$' || true`

    if [ -z "$orig" ]; then
	what="$allconf"
	return
    fi

    what=""
    for f in $orig; do
	type=`echo $f | cut -c 1-3`   # e.g. 'mon', if $item is 'mon1'
	id=`echo $f | cut -c 4- | sed 's/\\.//'`
	case $f in
	    mon | osd | mds)
		for d in $allconf; do
		    if echo $d | grep -q ^$type; then
			what="$what $d"
		    fi
		done
		;;
	    *)
		if ! echo " " $allconf $local " " | egrep -q "( $type$id | $type.$id )"; then
		    echo "$0: $type.$id not found ($conf defines" $allconf", /var/lib/ceph defines" $local")"
		    exit 1
		fi
		what="$what $f"
		;;
	esac
    done
}

get_conf() {
	var=$1
	def=$2
	key=$3
	shift; shift; shift

	if [ -z "$1" ]; then
	    [ "$verbose" -eq 1 ] && echo "$CCONF -c $conf -n $type.$id \"$key\""
	    eval "$var=\"`$CCONF -c $conf -n $type.$id \"$key\" || eval echo -n \"$def\"`\""
	else
	    [ "$verbose" -eq 1 ] && echo "$CCONF -c $conf -s $1 \"$key\""
	    eval "$var=\"`$CCONF -c $conf -s $1 \"$key\" || eval echo -n \"$def\"`\""
	fi
}

get_conf_bool() {
	get_conf "$@"

	eval "val=$"$1
	[ "$val" = "0" ] && export $1=0
	[ "$val" = "false" ] && export $1=0
	[ "$val" = "1" ] && export $1=1
	[ "$val" = "true" ] && export $1=1
}

