#!/bin/sh
#
# mkcephfs
#
# This tool is designed to be flexible.  There are two ways to go:
#
# The easy way does everything for you using ssh keys.  This does not
# scale well for large clusters.
#
#  master$ mkcephfs -a -c /etc/ceph/ceph.conf
#
# Alternatively, you can use whatever file distribution and/or job
# launching you want.
#
#  master$ mkdir /tmp/foo
#  master$ mkcephfs -d /tmp/foo -c /etc/ceph/ceph.conf --prepare-monmap
#
#     ...copy/share /tmp/foo with all osd and mds nodes at /tmp/bar...
#
#  osd$ mkcephfs -d /tmp/bar --init-local-daemons osd
#  mds$ mkcephfs -d /tmp/bar --init-local-daemons mds
#
#     ...gather contents of /tmp/bar's back into /tmp/foo...
#
#  master$ mkcephfs -d /tmp/foo --prepare-mon
#
#     ...distribute /tmp/foo to each monitor node...
#
#  mon$ mkcephfs -d /tmp/foo --init-local-daemons mon
#  
#  master$ cp /tmp/foo/keyring.admin /etc/ceph/keyring  # don't forget!
#
# In the degenerate case (one node), this is just
#
#  mkdir /tmp/foo
#  mkcephfs -c ceph.conf -d /tmp/foo --prepare-monmap
#  mkcephfs -d /tmp/foo --init-local-daemons mds
#  mkcephfs -d /tmp/foo --init-local-daemons osd
#  mkcephfs -d /tmp/foo --prepare-mon
#  mkcephfs -d /tmp/foo --init-local-daemons mon
#  cp /tmp/foo/keyring.admin /etc/ceph/keyring
#
# or simply
#
#  mkcephfs -a -c ceph.conf
#

set -e

trap 'echo "\nWARNING: mkcephfs is now deprecated in favour of ceph-deploy. Please see: \n http://github.com/ceph/ceph-deploy"' EXIT

# if we start up as ./mkcephfs, assume everything else is in the
# current directory too.
if [ `dirname $0` = "." ] && [ $PWD != "/etc/init.d" ]; then
    BINDIR=.
    LIBDIR=.
    ETCDIR=.
else
    BINDIR=@bindir@
    LIBDIR=@libdir@/ceph
    ETCDIR=@sysconfdir@/ceph
fi

usage_exit() {
    echo "usage: $0 -a -c ceph.conf [-k adminkeyring] [--mkfs]"
    echo "   to generate a new ceph cluster on all nodes; for advanced usage see man page"
    echo "   ** be careful, this WILL clobber old data; check your ceph.conf carefully **"
    exit
}

. $LIBDIR/ceph_common.sh


allhosts=0
mkfs=0
preparemonmap=0
prepareosdfs=""
initdaemon=""
initdaemons=""
preparemon=0

numosd=
useosdmap=
usecrushmapsrc=
usecrushmap=
verbose=0
adminkeyring=""
conf=""
dir=""
moreargs=""
auto_action=0
manual_action=0
nocopyconf=0

while [ $# -ge 1 ]; do
case $1 in
    -v )
	    verbose=1;
	    ;;
    --dir | -d)
	    [ -z "$2" ] && usage_exit
	    shift
	    dir=$1
	    ;;
    --allhosts | -a)
	    allhosts=1
            auto_action=1
	    ;;
    --prepare-monmap)
	    preparemonmap=1
            manual_action=1
	    ;;
    --prepare-osdfs)
	    [ -z "$2" ] && usage_exit
	    shift
	    prepareosdfs=$1
            manual_action=1
	    ;;
    --init-daemon)
	    [ -z "$2" ] && usage_exit
	    shift
	    initdaemon=$1
            manual_action=1
	    ;;
    --init-local-daemons)
	    [ -z "$2" ] && usage_exit
	    shift
	    initlocaldaemons=$1
            manual_action=1
	    ;;
    --prepare-mon)
	    preparemon=1
            manual_action=1
	    ;;
    --mkbtrfs | --mkfs)
	    mkfs=1
	    ;;
    --no-copy-conf)
	    nocopyconf=1
	    ;;
    --conf | -c)
	    [ -z "$2" ] && usage_exit
	    shift
	    conf=$1
	    ;;
    --numosd)
	    [ -z "$2" ] && usage_exit
	    shift
	    numosd=$1
	    moreargs="$moreargs --numosd $1"
	    ;;
    --osdmap)
	    [ -z "$2" ] && usage_exit
	    shift
	    useosdmap=$1
	    moreargs="$moreargs --osdmap $1"
	    ;;
    --crushmapsrc)
	    [ -z "$2" ] && usage_exit
	    shift
	    usecrushmapsrc=$1
	    moreargs="$moreargs --crushmapsrc $1"
	    ;;
    --crushmap)
	    [ -z "$2" ] && usage_exit
	    shift
	    usecrushmap=$1
	    moreargs="$moreargs --crushmap $1"
	    ;;
    -k)
	    [ -z "$2" ] && usage_exit
	    shift
	    adminkeyring=$1
	    ;;
    *)
	    echo unrecognized option \'$1\'
	    usage_exit
	    ;;
esac
shift
done


[ -z "$conf" ] && [ -n "$dir" ] && conf="$dir/conf"

if [ $manual_action -eq 0 ]; then
    if [ $auto_action -eq 0 ]; then
        echo "You must specify an action. See man page."
        usage_exit
    fi
elif [ $auto_action -eq 1 ]; then
    echo "The -a option cannot be combined with other subcommands; see man page."
    usage_exit
fi

### prepare-monmap ###

if [ $preparemonmap -eq 1 ]; then
    echo "preparing monmap in $dir/monmap"

    # first, make a list of monitors
    mons=`$CCONF -c $conf -l mon | egrep -v '^mon$' | sort`
    args=""

    type="mon"
    for name in $mons; do
	id=`echo $name | cut -c 4- | sed 's/^\\.//'`
	get_conf addr "" "mon addr"
	if [ -z "$addr" ]; then
	    echo "$0: monitor $name has no address defined." 1>&2
	    exit 1
	fi
	args=$args" --add $id $addr"
    done

    if [ -z "$args" ]; then
	echo "$0: no monitors found in config, aborting." 1>&2
	exit 1
    fi

    # build monmap
    monmap="$dir/monmap"
    echo $BINDIR/monmaptool --create --clobber $args --print $monmap || exit 1
    $BINDIR/monmaptool --create --clobber $args --print $monmap || exit 1
        
    # copy conf
    cp $conf $dir/conf

    exit 0
fi


### init-daemon ###

if [ -n "$initdaemon" ]; then
    name=$initdaemon
    type=`echo $name | cut -c 1-3`   # e.g. 'mon', if $name is 'mon1'
    id=`echo $name | cut -c 4- | sed 's/^\\.//'`
    name="$type.$id"
    
    # create /var/run/ceph (or wherever pid file and/or admin socket live)
    get_conf pid_file "/var/run/ceph/$name.pid" "pid file"
    rundir=`dirname $pid_file`
    if [ "$rundir" != "." ] && [ ! -d "$rundir" ]; then
	mkdir -p $rundir
    fi
    get_conf asok_file "/var/run/ceph/$name.asok" "admin socket"
    rundir=`dirname $asok_file`
    if [ "$rundir" != "." ] && [ ! -d "$rundir" ]; then
	mkdir -p $rundir
    fi

    if [ $type = "osd" ]; then
	$BINDIR/ceph-osd -c $conf --monmap $dir/monmap -i $id --mkfs --mkkey

	get_conf osd_data "/var/lib/ceph/osd/ceph-$id" "osd data"
	get_conf osd_keyring "$osd_data/keyring" "keyring"
	$BINDIR/ceph-authtool -p -n $name $osd_keyring > $dir/key.$name
    fi
    
    if [ $type = "mds" ]; then
	get_conf mds_data "/var/lib/ceph/mds/ceph-$id" "mds data"
	get_conf mds_keyring "$mds_data/keyring" "keyring"
	test -d $mds_data || mkdir -p $mds_data
	echo "creating private key for $name keyring $mds_keyring"
	$BINDIR/ceph-authtool --create-keyring --gen-key -n $name $mds_keyring
	$BINDIR/ceph-authtool -p -n $name $mds_keyring > $dir/key.$name
    fi

    if [ $type = "mon" ]; then
	get_conf mon_data "/var/lib/ceph/mon/ceph-$id" "mon data"
	mkdir -p "$mon_data"
	if ! find "$mon_data" -maxdepth 0 -empty | read foo; then
	    echo "ERROR: $name mon_data directory $mon_data is not empty."
	    echo "       Please make sure that it is empty before running mkcephfs."
	    exit 1
	fi
	$BINDIR/ceph-mon -c $conf --mkfs -i $id --monmap $dir/monmap --osdmap $dir/osdmap -k $dir/keyring.mon
    fi
    
    exit 0
fi


## init-local-daemons ##

if [ -n "$initlocaldaemons" ]; then
    get_name_list "$initlocaldaemons"
    for name in $what; do
	type=`echo $name | cut -c 1-3`   # e.g. 'mon', if $name is 'mon1'
	id=`echo $name | cut -c 4- | sed 's/^\\.//'`
	num=$id
	name="$type.$id"

	check_host || continue

	$0 -d $dir --init-daemon $name	
    done
    exit 0
fi


### prepare-osdfs ###

if [ -n "$prepareosdfs" ]; then
    name=$prepareosdfs
    type=`echo $name | cut -c 1-3`   # e.g. 'mon', if $name is 'mon1'
    id=`echo $name | cut -c 4- | sed 's/^\\.//'`
    name="$type.$id"

    get_conf osd_data "/var/lib/ceph/osd/ceph-$id" "osd data"
    get_conf osd_journal "$osd_data/journal" "osd journal"
    get_conf fs_path "$osd_data" "fs path"  # mount point defaults so osd data
    get_conf fs_devs "" "devs"
    get_conf fs_type "" "osd mkfs type"

    if [ -z "$fs_devs" ]; then
	# try to fallback to old keys
        get_conf tmp_btrfs_devs "" "btrfs devs"
        if [ -n "$tmp_btrfs_devs" ]; then
            fs_devs="$tmp_btrfs_devs"
        else
            echo "no devs defined for $name"
            exit 1
        fi
    fi
    if [ -z "$fs_type" ]; then
        # try to fallback to to old keys
        get_conf tmp_devs "" "btrfs devs"
        if [ -n "$tmp_devs" ]; then
            fs_type="btrfs"
        else
            echo No filesystem type defined!
            exit 1
        fi
    fi

    first_dev=`echo $fs_devs | cut '-d ' -f 1`
    get_conf fs_opt "" "osd mount options $fs_type"
    if [ -z "$fs_opt" ]; then
        if [ "$fs_type" = "btrfs" ]; then
            #try to fallback to old keys
            get_conf fs_opt "" "btrfs options"
        fi
        if [ -z "$fs_opt" ]; then
	    if [ "$fs_type" = "xfs" ]; then
		fs_opt="rw,noatime,inode64"
	    else
                #fallback to use at least rw,noatime
		fs_opt="rw,noatime"
	    fi
        fi
    fi
    [ -n "$fs_opt" ] && fs_opt="-o $fs_opt"
    get_conf osd_user "root" "user"
    
    if [ -n "$osd_journal" ] && echo "fs_devs" | grep -q -w "$osd_journal" ; then
        echo "ERROR: osd journal device ($osd_journal) also used by devs ($fs_devs)"
	exit 1
    fi
    
    test -d $osd_data || mkdir -p $osd_data

    if [ -n "$osd_journal" ]; then
	test -d $osd_journal || mkdir -p `dirname $osd_journal`
    fi

    umount $fs_path || true
    for f in $fs_devs ; do
	umount $f || true
    done

    get_conf mkfs_opt "" "osd mkfs options $fs_type"
    if [ "$fs_type" = "xfs" ] && [ -z "$mkfs_opt" ]; then
        echo Xfs filesystem found add missing -f mkfs option!
	mkfs_opt="-f"
    fi
    modprobe $fs_type || true
    mkfs.$fs_type $mkfs_opt $fs_devs
    mount -t $fs_type $fs_opt $first_dev $fs_path
    chown $osd_user $fs_path
    chmod +w $fs_path

    exit 0
fi



### prepare-mon ###

if [ $preparemon -eq 1 ]; then

    if [ -n "$useosdmap" ]; then
	echo "Using osdmap $useosdmap"
	cp $useosdmap $dir/osdmap
    else
	echo "Building generic osdmap from $conf"
	$BINDIR/osdmaptool --create-from-conf $dir/osdmap -c $conf
    fi

    # import crush map?
    get_conf crushmapsrc "" "crush map src" mon global
    if [ -n "$crushmapsrc" ]; then
	echo Compiling crush map from $crushmapsrc to $dir/crushmap
	$BINDIR/crushtool -c $crushmapsrc -o $dir/crushmap
    fi
    get_conf crushmap "$usecrushmap" "crush map" mon global
    if [ -n "$crushmap" ]; then
	echo Importing crush map from $crushmap
	$BINDIR/osdmaptool --import-crush $crushmap $dir/osdmap
    fi

    # admin keyring
    echo Generating admin key at $dir/keyring.admin
    $BINDIR/ceph-authtool --create-keyring --gen-key -n client.admin $dir/keyring.admin

    # mon keyring
    echo Building initial monitor keyring
    cp $dir/keyring.admin $dir/keyring.mon
    $BINDIR/ceph-authtool -n client.admin --set-uid=0 \
	--cap mon 'allow *' \
	--cap osd 'allow *' \
	--cap mds 'allow' \
	$dir/keyring.mon

    $BINDIR/ceph-authtool --gen-key -n mon. $dir/keyring.mon --cap mon 'allow *'

    for k in $dir/key.*
    do
	kname=`echo $k | sed 's/.*key\.//'`
	ktype=`echo $kname | cut -c 1-3`
	kid=`echo $kname | cut -c 4- | sed 's/^\\.//'`
	kname="$ktype.$kid"
	secret=`cat $k`
	if [ "$ktype" = "osd" ]; then
	    $BINDIR/ceph-authtool -n $kname --add-key $secret $dir/keyring.mon \
		--cap mon 'allow rwx' \
		--cap osd 'allow *'
	fi
	if [ "$ktype" = "mds" ]; then
	    $BINDIR/ceph-authtool -n $kname --add-key $secret $dir/keyring.mon \
		--cap mon "allow rwx" \
		--cap osd 'allow *' \
		--cap mds 'allow'
	fi
    done

    exit 0
fi





### do everything via ssh ###

if [ $allhosts -eq 1 ]; then

    verify_conf

    # do it all
    if [ -z "$dir" ]; then
	dir=`mktemp -d -t mkcephfs.XXXXXXXXXX` || exit 1
	echo "temp dir is $dir"
	trap "rm -rf $dir ; exit" INT TERM EXIT
    fi

    $0 --prepare-monmap -d $dir -c $conf

    # osd, mds
    get_name_list "osd mds"
    for name in $what; do
	type=`echo $name | cut -c 1-3`   # e.g. 'mon', if $name is 'mon1'
	id=`echo $name | cut -c 4- | sed 's/^\\.//'`
	num=$id
	name="$type.$id"

	check_host || continue

	if [ -n "$ssh" ]; then
	    rdir=`mktemp -u /tmp/mkfs.ceph.XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX` || exit 1
	    echo pushing conf and monmap to $host:$rdir
	    do_cmd "mkdir -p $rdir"
	    scp -q $dir/conf $host:$rdir
	    scp -q $dir/monmap $host:$rdir

	    if [ $nocopyconf -eq 0 ]; then
		# also put conf at /etc/ceph/ceph.conf
		scp -q $dir/conf $host:/etc/ceph/ceph.conf
	    fi
	else
	    rdir=$dir

	    if [ $nocopyconf -eq 0 ]; then
		# also put conf at /etc/ceph/ceph.conf
		cp $dir/conf /etc/ceph/ceph.conf
	    fi
	fi
	
	if [ $mkfs -eq 1 ] && [ "$type" = "osd" ]; then
	    do_root_cmd "$0 -d $rdir --prepare-osdfs $name"
	fi

	do_root_cmd "$0 -d $rdir --init-daemon $name"

	# collect the key
	if [ -n "$ssh" ]; then
	    echo collecting $name key
	    scp -q $host:$rdir/key.$name $dir
	    #cleanup no longer need rdir
	    do_cmd "rm -r $rdir"
	fi
    done

    # prepare monitors
    $0 -d $dir --prepare-mon $moreargs
    
    # mons
    get_name_list "mon"
    for name in $what; do
	type=`echo $name | cut -c 1-3`   # e.g. 'mon', if $name is 'mon1'
	id=`echo $name | cut -c 4- | sed 's/^\\.//'`
	num=$id
	name="$type.$id"

	check_host || continue
	
	if [ -n "$ssh" ]; then
	    rdir=`mktemp -u /tmp/mkfs.ceph.XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX` || exit 1
	    echo pushing everything to $host
	    ssh $host mkdir -p $rdir
	    scp -q $dir/* $host:$rdir

	    if [ $nocopyconf -eq 0 ]; then
		# also put conf at /etc/ceph/ceph.conf
		scp -q $dir/conf $host:/etc/ceph/ceph.conf
	    fi
	else
	    rdir=$dir

	    if [ $nocopyconf -eq 0 ]; then
	        # also put conf at /etc/ceph/ceph.conf
		cp $dir/conf /etc/ceph/ceph.conf
	    fi
	fi
	
	do_root_cmd "$0 -d $rdir --init-daemon $name"

	if [ -n "$ssh" ]; then
	    #cleanup no longer need rdir
	    do_cmd "rm -r $rdir"
	fi
    done

    # admin keyring
    if [ -z "$adminkeyring" ]; then
	get_conf adminkeyring "/etc/ceph/keyring" "keyring" global
    fi
    echo "placing client.admin keyring in $adminkeyring"
    cp $dir/keyring.admin $adminkeyring

    exit 0
fi

