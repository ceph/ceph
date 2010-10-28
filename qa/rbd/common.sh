
error_exit() {
	echo "$*"
	exit 1
}

# defaults
[ -z "$bindir" ] && bindir=$PWD       # location of init-ceph
if [ -z "$conf" ]; then
	conf="$basedir/ceph.conf"
	[ -e $conf ] || conf="/etc/ceph/ceph.conf"
fi
[ -e $conf ] || error_exit "conf file not found"

CCONF="cconf -c $conf"

[ -z "$mnt" ] && mnt="/c"
[ -z "$monhost" ] && monhost="`$CCONF -t mon -i 0 'mon addr'`"
[ -z "$imgsize" ] && imgsize=1024
[ -z "$user" ] && user=admin
[ -z "$keyring" ] && keyring="`$CCONF keyring`"
[ -z "$secret" ] && secret="`cauthtool $keyring -n client.$user -p`"


monip="`echo $monhost | sed 's/:/ /g' | awk '{print $1}'`"
monport="`echo $monhost | sed 's/:/ /g' | awk '{print $2}'`"

[ -z "$monip" ] && error_exit "bad mon address"

[ -z "$monport" ] && monport=6789

set -e

mydir=`hostname`_`echo $0 | sed 's/\//_/g'`

img_name=test.`hostname`.$$


rbd_load() {
	modprobe rbd
}

rbd_create_image() {
	id=$1
	rbd create $img_name.$id --size=$imgsize
}

rbd_add() {
	id=$1
	echo "$monip:$monport name=$user,secret=$secret rbd $img_name.$id" > /sys/class/rbd/add
	devid="`cat /sys/class/rbd/list | grep $img_name.$ext | tail -1 | cut -f1`"
	export rbd$id=$devid
	while [ ! -e /dev/rbd$devid ]; do sleep 0; done
}

rbd_test_init() {
	rbd_load
}


rbd_remove() {
	echo $1 > /sys/class/rbd/remove
}

rbd_rm_image() {
	id=$1
	rbd rm $imgname.$id
}
