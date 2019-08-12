
# defaults
[ -z "$bindir" ] && bindir=$PWD       # location of init-ceph
[ -z "$conf" ] && conf="$basedir/ceph.conf"
[ -z "$mnt" ] && mnt="/c"
[ -z "$monhost" ] && monhost="cosd0"

set -e

mydir=`hostname`_`echo $0 | sed 's/\//_/g'`

client_mount()
{
    /bin/mount -t ceph $monhost:/ $mnt
}

client_umount()
{
    /bin/umount $mnt
    # look for VFS complaints
    if dmesg | tail -n 50 | grep -c "VFS: Busy inodes" ; then
	echo "looks like we left inodes pinned"
	exit 1
    fi
}

ceph_start()
{
    $bindir/init-ceph -c $conf start ${1}
}

ceph_stop()
{
    $bindir/init-ceph -c $conf stop ${1}
}

ceph_restart()
{
    $bindir/init-ceph -c $conf restart ${1}
}

ceph_command()
{
    $bindir/ceph -c $conf $*
}

client_enter_mydir()
{
    pushd .
    test -d $mnt/$mydir && rm -r $mnt/$mydir
    mkdir $mnt/$mydir
    cd $mnt/$mydir
}

client_leave_mydir()
{
    popd
}
