#!/bin/bash

die() {
	echo "$*"
	exit 1
}

cleanup() {
    rm -rf $TDIR
    TDIR=""
}

set_variables() {
    # defaults
    [ -z "$bindir" ] && bindir=$PWD       # location of init-ceph
    if [ -z "$conf" ]; then
        conf="$basedir/ceph.conf"
        [ -e $conf ] || conf="/etc/ceph/ceph.conf"
    fi
    [ -e $conf ] || die "conf file not found"

    CCONF="ceph-conf -c $conf"

    [ -z "$mnt" ] && mnt="/c"
    if [ -z "$monhost" ]; then
        $CCONF -t mon -i 0 'mon addr' > $TDIR/cconf_mon
        if [ $? -ne 0 ]; then
            $CCONF -t mon.a -i 0 'mon addr' > $TDIR/cconf_mon
            [ $? -ne 0 ] && die "can't figure out \$monhost"
        fi
        read monhost < $TDIR/cconf_mon
    fi

    [ -z "$imgsize" ] && imgsize=1024
    [ -z "$user" ] && user=admin
    [ -z "$keyring" ] && keyring="`$CCONF keyring`"
    [ -z "$secret" ] && secret="`ceph-authtool $keyring -n client.$user -p`"

    monip="`echo $monhost | sed 's/:/ /g' | awk '{print $1}'`"
    monport="`echo $monhost | sed 's/:/ /g' | awk '{print $2}'`"

    [ -z "$monip" ] && die "bad mon address"

    [ -z "$monport" ] && monport=6789

    set -e

    mydir=`hostname`_`echo $0 | sed 's/\//_/g'`

    img_name=test.`hostname`.$$
}

rbd_load() {
	modprobe rbd
}

rbd_create_image() {
	id=$1
	rbd create $img_name.$id --size=$imgsize
}

rbd_add() {
	id=$1
	echo "$monip:$monport name=$user,secret=$secret rbd $img_name.$id" \
	    > /sys/bus/rbd/add

	pushd /sys/bus/rbd/devices &> /dev/null
	[ $? -eq 0 ] || die "failed to cd"
	devid=""
	rm -f "$TDIR/rbd_devs"
	for f in *; do echo $f >> "$TDIR/rbd_devs"; done
	sort -nr "$TDIR/rbd_devs" > "$TDIR/rev_rbd_devs"
	while read f < "$TDIR/rev_rbd_devs"; do
	  read d_img_name < "$f/name"
	  if [ "x$d_img_name" == "x$img_name.$id" ]; then
	    devid=$f
	    break
	  fi
	done
	popd &> /dev/null

	[ "x$devid" == "x" ] && die "failed to find $img_name.$id"

	export rbd$id=$devid
	while [ ! -e /dev/rbd$devid ]; do sleep 1; done
}

rbd_test_init() {
	rbd_load
}

rbd_remove() {
	echo $1 > /sys/bus/rbd/remove
}

rbd_rm_image() {
	id=$1
	rbd rm $imgname.$id
}

TDIR=`mktemp -d`
trap cleanup INT TERM EXIT
set_variables
