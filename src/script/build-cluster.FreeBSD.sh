#!/bin/sh

# Start a number of jails and create a full set of Ceph servers 
# on them: Mon, Osd, Mgr, ....
# freestyle copy of what src/vstart.sh does for starting clusters for testing
#

BASE_CONF_FILE="/usr/jails/ceph-0/etc/ceph/ceph.conf"

config_setup_defaults() {
    extra_con=""
    new=0
    standby=0
    ipnet=""
    nodaemon=0
    smallmds=0
    short=0
    overwrite_conf=1
    cephx=1 #turn cephx on by default
    memstore=0
    rgw_frontend="civetweb"
    lockdep=${LOCKDEP:-1}
    destroy=0
    verbose=0

    # Packages to be installed in the jails
    PACKAGES="sudo ceph-devel python" 

    # The network prefix of the network to use for the jails/servers
    CEPH_NET="192.168.11" 
    # The interface to assign the network to
    CEPH_IF="em0"

    # list with devices on which a OSD should be put
    CEPH_DISKS="ada0 ada1 ada3 ada4 ada7 ada8 ada9" 
    CEPH_DISK_CNT=`echo $CEPH_DISKS | wc -w`
    CEPH_CACHE_DISK="ada10"

    JAILS_HOME="/usr/jails"
    JAILS_RELEASE="12.0-CURRENT"
    freebsd_current=0
    JAILS_COUNT=5

    [ -z "$MON" ] && MON_COUNT="$MON"
    [ -z "$OSD" ] && OSD_COUNT="$OSD"
    [ -z "$MDS" ] && MDS_COUNT="$MDS"
    [ -z "$MGR" ] && MGR_COUNT="$MGR"
    [ -z "$FS"  ] && FS_COUNT="$FS"
    [ -z "$RGW_COUNT" ] && RGW_COUNT="$RGW"

    [ -z "$MON_COUNT" ] && MON_COUNT=3
    [ -z "$OSD_COUNT" ] && OSD_COUNT=${CEPH_DISK_CNT}
    [ -z "$MDS_COUNT" ] && MDS_COUNT=1
    [ -z "$MGR_COUNT" ] && MGR_COUNT=1
    [ -z "$FS_COUNT"  ] && FS_COUNT=2
    [ -z "$MDS_MAX" ]   && CEPH_MAX_MDS=1
    [ -z "$RGW_COUNT" ] && RGW_COUNT=0

    if [ $OSD_COUNT -gt 3 ]; then
        OSD_POOL_DEFAULT_SIZE=3
    else
        OSD_POOL_DEFAULT_SIZE=$OSD_COUNT
    fi

    CEPH_FSID=`uuidgen` 
}

config_setup() {
    while [ $# -ge 1 ]; do
        case $1 in
            -h | --help )
                    usage
                    ;;
            -v | --verbose )
                    set -x
                    verbose=1
                    ;;
            -s | --standby_mds)
                    standby=1
                    ;;
            -i )
                    [ -z "$2" ] && usage_exit
                    ipnet="$2"
                    shift
                    ;;
#            -e )
#                    ec=1
#                    ;;
#            --new | -n )
#                    new=1
#                    ;;
#            --not-new | -N )
#                new=0
#                ;;
            --smallmds )
                    smallmds=1
                    ;;
            --mon_num )
                    echo "mon_num:$2"
                    CEPH_NUM_MON="$2"
                    shift
                    ;;
            --osd_num )
                    CEPH_NUM_OSD=$2
                    shift
                    ;;
            --mds_num )
                    CEPH_NUM_MDS=$2
                    shift
                    ;;
            --rgw_num )
                    CEPH_NUM_RGW=$2
                    shift
                    ;;
            --mgr_num )
                    CEPH_NUM_MGR=$2
                    shift
                    ;;
#            --rgw_port )
#                    CEPH_RGW_PORT=$2
#                    shift
#                    ;;
#            --rgw_frontend )
#                    rgw_frontend=$2
#                    shift
#                    ;;
#            -m )
#                    [ -z "$2" ] && usage_exit
#                    MON_ADDR=$2
#                    shift
#                    ;;
#            -k )
#                    if [ ! -r $conf_fn ]; then
#                        echo "cannot use old configuration: $conf_fn not readable." >&2
#                        exit
#                    fi
#                    overwrite_conf=0
#                    ;;
#            --memstore )
#                    memstore=1
#                    ;;
            -o )
                    extra_conf="$extra_conf     $2
        "
                    shift
                    ;;
#            --nolockdep )
#                   lockdep=0
#                    ;;
#            --multimds)
#                CEPH_MAX_MDS="$2"
#                shift
#                ;;
            --destroy)
                    destroy=1
                    ;;
            * )
                    usage
        esac
    shift
    done

}

usage() {
    umsg="Usage: $0 [option]... \nex: $0 -v --mon_num 3 --osd_num 3 --mds_num 1 --rgw_num 1\n"
    umsg=$umsg"options:\n"
    umsg=$umsg"\t-h, --help\n"
    umsg=$umsg"\t-v, --verbose\n"
#    umsg=$umsg"\t-s, --standby_mds: Generate standby-replay MDS for each active\n"
    umsg=$umsg"\t-i <ip>: network prefix (X.Y.Z) to be used for the daemons.\n"
    umsg=$umsg"\t         eg. 192.168.11 \n"
    umsg=$umsg"\t         which will use 192.168.11.201,210-219 for the jails\n"
    umsg=$umsg"\t--destroy Destroy the running cluster\n"
#    umsg=$umsg"\t-n, --new\n"
#    umsg=$umsg"\t-N, --not-new: reuse existing cluster config (default)\n"
#   umsg=$umsg"\t--smallmds: limit mds cache size\n"
#    umsg=$umsg"\t-m ip:port\t\tspecify monitor address\n"
#    umsg=$umsg"\t-k keep old configuration files\n"
    umsg=$umsg"\t-o config\t\t add extra config parameters to all sections\n"
    umsg=$umsg"\t--mon_num specify ceph monitor count\n"
    umsg=$umsg"\t--osd_num specify ceph osd count\n"
#    umsg=$umsg"\t--mds_num specify ceph mds count\n"
#    umsg=$umsg"\t--rgw_num specify ceph rgw count\n"
    umsg=$umsg"\t--mgr_num specify ceph mgr count\n"
#    umsg=$umsg"\t--rgw_port specify ceph rgw http listen port\n"
#    umsg=$umsg"\t--rgw_frontend specify the rgw frontend configuration\n"
#    umsg=$umsg"\t--memstore use memstore as the osd objectstore backend\n"
#    umsg=$umsg"\t--nolockdep disable lockdep\n"
#    umsg=$umsg"\t--multimds <count> allow multimds with maximum active count\n"

    printf "$umsg"
    exit
}

copy_config() {
    local tojail=$1
    [ $tojail = ceph-0 ] || \
        cp ${BASE_CONF_FILE} /usr/jails/${tojail}/etc/ceph/ceph.conf
}

network_setup() {
    #default base adress
    ifconfig ${CEPH_IF} alias ${CEPH_NET}.201
    for i in `jot ${JAILS_COUNT} 0 `; do
         ifconfig ${CEPH_IF} alias ${CEPH_NET}.21${i}
    done
}

ezjail_setup() {
    pkg install -y ezjail
    [ ! -d /usr/jails ] && zfs create -o mountpoint=${JAILS_HOME} zroot/jails 
 
    # Build the base jail
    if [ $freebsd_current -eq 1 ]; then
        # Fetch FreeBSD release CURRENT
        mkdir -p /tmp/release
        for f in base lib32 kernel; do
        [ -f /tmp/release/$f.txz ] || 
        fetch -o /tmp/release https://download.freebsd.org/ftp/snapshots/amd64/${JAILS_RELEASE}/$f.txz
        done
        [ ! -d ${JAILS_HOME}/basejail ] && ezjail-admin install -h file:////tmp/release
    else
        # Use the current OS install to build the jails
        [ ! -d ${JAILS_HOME}/basejail ] && ezjail-admin install
    fi
    
    ezjail_create_flavour
    # add config to ezjail
    cat << __EOF >> /usr/local/etc/ezjail.conf
ezjail_use_zfs="YES"
ezjail_use_zfs_for_jails="YES"
ezjail_jailzfs="zroot/jails"
ezjail_zfs_properties="-o compression=lz4 -o atime=off"
ezjail_jailtemplate=${HOME_JAILS}/newjail
__EOF

    # create the jails
    for i in `jot ${JAILS_COUNT} 0 `; do
        ALREADY=""
	[ -d ${JAILS_HOME}/ceph-${i} ] && ALREADY="-x" 
	ezjail-admin create -f ceph -c zfs ${ALREADY} ceph-${i} ${CEPH_NET}.21${i} 
	echo "${CEPH_NET}.21${i} ceph-${i}" >> /etc/hosts
    done
    sort -n /etc/hosts | uniq > /etc/hosts.new && mv /etc/hosts.new /etc/hosts
}

ezjail_create_flavour() {
    [ -d ${JAILS_HOME}/flavours/ceph ] && rm -rf ${JAILS_HOME}/flavours/ceph
    cp -r ${JAILS_HOME}/flavours/example ${JAILS_HOME}/flavours/ceph
    cd ${JAILS_HOME}/flavours/ceph
    cp /etc/resolv.conf ./etc
    echo "ceph_enable=YES" >> ./etc/rc.conf

    mkdir -p ./rc.d
cat > ./rc.d/ezjail.flavour.ceph << '__EOF'
#!/bin/sh
#
# BEFORE: DAEMON
# PROVIDE: ezjail.flavour.ceph
#
# ezjail flavour example

. /etc/rc.subr

name=ezjail.flavour.ceph
start_cmd=flavour_setup

flavour_setup() {

# Remove traces of ourself
# N.B.: Do NOT rm $0, it points to /etc/rc
##########################
  rm -f "/etc/rc.d/ezjail.flavour.ceph"

# Packages
###########
ASSUME_ALWAYS_YES=yes
pkg install pkg
pkg install -y sudo
pkg install -y bash
pkg install -y net/ceph-devel

}

run_rc_command "$1"
__EOF
}

ezjail_pre_conf() {
    for jp in `ls -d ${JAILS_HOME}/ceph-? | sort -n `; do
        name=`basename $jp`
        name_=`echo $name | sed 's/-/_/'`
	echo "linproc $jp/compat/linux/proc linprocfs rw 0 0" >> /etc/fstab.${name_}
        if [ -d /usr/jails/$name ]; then
            # Preloading the PKG cache, so we do not download too much
            [ -d $jp/var/cache ] || mkdir -p $jp/var/cache
            [ -d /tmp/var/cache/pkg ] && cp -r /tmp/var/cache/pkg $jp/var/cache
            cp /var/db/pkg/repo-FreeBSD.sqlite $jp/var/db/pkg/repo-FreeBSD.sqlite
            mkdir $jp/etc/ceph
            cp /etc/localtime $jp/etc
            cp /etc/resolv.conf $jp/etc
	    mkdir -p $jp/compat/linux/proc
        fi
done
}

ezjail_start() {
    # lets start the jails in the correct numeric order
    for jp in `ls -d ${JAILS_HOME}/ceph-? | sort -n `; do
        name=`basename $jp`
        service ezjail onestart $name
    done
}

ezjail_post_conf() {
    for jid in `jls | grep ceph- | awk '{print $1}' | sort -n` ; do
	pkg -j $jid install -y pkg
	for p in ${PACKAGES} ; do
	    pkg -j $jid install -y $p
	done
    done
}

zfs_create_osd_disks() {
    osdcnt=0;
    osdmnt=0;
    cachedisksize=`diskinfo -v /dev/${CEPH_CACHE_DISK} | head -n 3 | tail -n 1 | awk '{print $1}'`
    cachesize=`bc -e "${cachedisksize}/1024/1024/1024/${CEPH_DISK_CNT}-1" -e quit`
    ssdpartitions=$(($CEPH_DISK_CNT*2))
    if gpart list $CEPH_CACHE_DISK > /dev/null ; then
        for c in `gpart list ${CEPH_CACHE_DISK} | grep index: | awk '{ print $2 }'`; do
	    i=$(($c-1))
            gpart delete -i${c} $CEPH_CACHE_DISK
        done
	gpart destroy -F $CEPH_CACHE_DISK
    fi 

    gpart create -s GPT $CEPH_CACHE_DISK
    for c in `jot ${CEPH_DISK_CNT} 0`; do
        gpart add -a 1M -s ${cachesize}G -t freebsd-zfs -l cache.${c} $CEPH_CACHE_DISK
        gpart add -a 1M -s 1G            -t freebsd-zfs -l log.${c}   $CEPH_CACHE_DISK
    done
 
    for d in ${CEPH_DISKS} ; do
        # Clean the disk first
	gpart list $d > /dev/null && \
	    gpart destroy -F $d 
	#create GPT and partition
        gpart create -s GPT $d
        # Begin offset at 1m for alignment
        gpart add -a 1M -t freebsd-zfs -l osd.${osdcnt} ${d}

	# create the ZFS disk
	zpool create -f osd-${osdcnt}       /dev/gpt/osd.${osdcnt}
	zpool add -f    osd-${osdcnt} cache /dev/gpt/cache.${osdcnt}
	zpool add -f    osd-${osdcnt} log   /dev/gpt/log.${osdcnt}
	# Distribute the OSDs over the deamons
	zfs set mountpoint=${JAILS_HOME}/ceph-${osdmnt}/var/lib/ceph/osd/osd.${osdcnt} osd-${osdcnt}
	osdcnt=$(($osdcnt+1))
	osdmnt=$(($osdmnt+1))
	if [ $osdmnt -ge ${JAILS_COUNT} ]; then
	    osdmnt=0
	fi
    done
}

mon_create() {
    local moncnt=0
    for f in a b c d e f g h i j k l m n o p q r s t u v w x y z
    do
        [ $moncnt -eq $MON_COUNT ] && break;
        MON_IP=`jls | grep ceph-${moncnt} | awk '{print $2}'`
        
        if [ -z "$MONS" ];
        then
            MONS="$f"
            MON_IPS="${MON_IP}"
            MONMAP_IPS="--add $f $MON_IP"
            echo "[mon.$f]" > /tmp/mon.conf
        else
            MONS="$MONS $f"
            MONS_IPS="$MON_IPS $MON_IP"
            MONMAP_IPS="$MONMAP_IPS --add $f $MON_IP"
            echo "[mon.$f]" >> /tmp/mon.conf
        fi
        echo "    host = ceph-${moncnt}" >> /tmp/mon.conf
        moncnt=$(($moncnt + 1))
    done


    # create a config file
    cat << __EOF > ${BASE_CONF_FILE}
[global] 
fsid = ${CEPH_FSID}
mon initial members = ${MONS}
mon host = ${MON_IPS}

[mon]
__EOF
    cat /tmp/mon.conf >> ${BASE_CONF_FILE}

    CEPH_ID0=`jls | grep ceph-0 | awk '{print $1}'`

    # Arrange keys.
    jexec ${CEPH_ID0} ceph-authtool  /tmp/ceph.mon.keyring --create-keyring --gen-key --name=mon. --cap mon 'allow *'
    jexec ${CEPH_ID0} ceph-authtool --create-keyring /etc/ceph/ceph.client.admin.keyring --gen-key -n client.admin \
                 --set-uid=0 --cap mon 'allow *' --cap osd 'allow *' --cap mds 'allow *'
    jexec ${CEPH_ID0} ceph-authtool /tmp/ceph.mon.keyring --import-keyring /etc/ceph/ceph.client.admin.keyring

    # Create the monmap
    jexec ${CEPH_ID0} monmaptool --create ${MONMAP_IPS} --fsid ${CEPH_FSID} --print --clobber /tmp/monmap


    moncnt=0
    for f in a b c d e f g h i j k l m n o p q r s t u v w x y z
    do
        [ $moncnt -eq $MON_COUNT ] && break;
        MON_ID=`jls | grep ceph-${moncnt} | awk '{print $1}'`
        if [ $moncnt -ne 0 ] ; then
            cp /usr/jails/ceph-0/tmp/monmap /usr/jails/ceph-${moncnt}/tmp/monmap
            cp /usr/jails/ceph-0//tmp/ceph.mon.keyring /usr/jails/ceph-${moncnt}/tmp/ceph.mon.keyring
        fi
        jexec ${MON_ID} mkdir /var/lib/ceph/mon/mon.$f
        jexec ${MON_ID} ceph-mon --mkfs -i $f --monmap /tmp/monmap --keyring /tmp/ceph.mon.keyring
        jexec ${MON_ID} ceph-mon -i $f
        moncnt=$(($moncnt + 1))
    done
    jexec ${CEPH_ID0} ceph-create-keys -v -i a

}

osd_create() {

    cat << __EOF >> ${BASE_CONF_FILE}

[osd]
    osd check max object name len on startup = false
__EOF

    for jail in `jls | grep ceph- | awk '{print $3}' | sort` ; do
        OSD_ID=`jls | grep "$jail " | awk '{print $1}'`
        OSD_IP=`jls | grep "$jail " | awk '{print $2}'`
        OSD_PATH=`jls | grep "$jail " | awk '{print $4}'`

        # copy some of the files from the MON/ceph-0 
        if [ $jail != ceph-0 ] ; then
	    cp ${BASE_CONF_FILE}                  	 ${OSD_PATH}/etc/ceph/
	    cp /usr/jails/ceph-0/etc/ceph/ceph.client.admin.keyring 	 ${OSD_PATH}/etc/ceph/
	    cp /usr/jails/ceph-0/var/lib/ceph/bootstrap-osd/ceph.keyring ${OSD_PATH}/var/lib/ceph/bootstrap-osd/
	    cp /etc/hosts              		 			 ${OSD_PATH}/etc
	fi 
	ln -s /etc/ceph/ceph.conf 				${OSD_PATH}/usr/local/etc/ceph/ceph.conf
	mkdir -p           ${OSD_PATH}/var/lib/ceph ${OSD_PATH}/var/run/ceph ${OSD_PATH}/var/log/ceph
	chown -R ceph:ceph ${OSD_PATH}/var/lib/ceph ${OSD_PATH}/var/run/ceph ${OSD_PATH}/var/log/ceph
	# Repair bugs:
        cp /usr/local/etc/rc.d/ceph			${OSD_PATH}/usr/local/etc/rc.d/
        cp /usr/local/lib/python2.7/site-packages/ceph_disk/main.py \
			${OSD_PATH}/usr/local/lib/python2.7/site-packages/ceph_disk/main.py
	 cp /usr/local/bin/init-ceph 			${OSD_PATH}/usr/local/bin/init-ceph
	 cp /usr/local/libexec/ceph/ceph_common.sh	${OSD_PATH}/usr/local/libexec/ceph/ceph_common.sh
    done

    # Start the OSDs in sequnce of the numbering on disk
    # so the disknumber matches the osd number
    for OSD_ID in `jot ${CEPH_DISK_CNT} 0` ; do
        OSD=osd.${OSD_ID}
        OSD_PATH=`echo /usr/jails/ceph-?/var/lib/ceph/osd/osd.${OSD_ID}`
        echo Start the OSD: $OSD in $OSD_PATH
        JAIL=`echo $OSD_PATH | awk -F/ '{print $4}'`
        JAIL_ID=`jls | grep "$JAIL " | awk '{print $1}'`
        cat << __EOF >> ${BASE_CONF_FILE}
[${OSD}]
    host = ${JAIL}
__EOF
        copy_config ${JAIL}
        jexec ${JAIL_ID} ceph-disk prepare  /var/lib/ceph/osd/${OSD}
        jexec ${JAIL_ID} ceph-disk activate /var/lib/ceph/osd/${OSD}
        # ceph-osd -i 0 --osd-data $STORE
    done
 
}

mgr_create() {
    local mgr=0

    MGR_ID=`jls | grep ceph-${mgr} | awk '{print $1}'`
    cat << __EOF >> ${BASE_CONF_FILE}
[mgr]
        mgr modules = rest fsstatus dashboard
        mon reweight min pgs per osd = 4
__EOF

    for name in x y z a b c d e f g h i j k l m n o p ; do
        [ $mgr -eq $MGR_COUNT ] && break
        MGR_ID=`jls | grep ceph-${mgr} | awk '{print $1}'`
        MGR_IP=`jls | grep ceph-${mgr} | awk '{print $2}'`
        MGR_DIR=/var/lib/ceph/mgr/mgr.$name
        jexec ${MGR_ID}  mkdir -p $MGR_DIR
        jexec ${MGR_ID}  ln -s $MGR_DIR /var/lib/ceph/mgr/ceph-$name
        key_fn=$MGR_DIR/keyring
        jexec ${MGR_ID} ceph-authtool --create-keyring --gen-key --name=mgr.$name $key_fn
        jexec ${MGR_ID} ceph -i $key_fn auth add mgr.$name mon 'allow profile mgr' mds 'allow *' osd 'allow *'
        cat << __EOF >> ${BASE_CONF_FILE}
[mgr.$name]
        host = ceph-${MGR_ID}
__EOF
	copy_config ceph-${mgr}
        jexec ${MGR_ID} ceph-mgr -i $name 
        mgr=$(($mgr + 1))
    done
}

mds_create() {
    local mds=0

    MDS_ID=`jls | grep ceph-${mds} | awk '{print $1}'`
    cat << __EOF >> ${BASE_CONF_FILE}
[mgr]
        mgr modules = rest fsstatus dashboard
        mon reweight min pgs per osd = 4
__EOF

    for name in a b c d e f g h i j k l m n o p ; do
        [ $mds -eq $MDS_COUNT ] && break
        MDS_ID=`jls | grep ceph-${mds} | awk '{print $1}'`
        MDS_IP=`jls | grep ceph-${mds} | awk '{print $2}'`
        MDS_DIR=/var/lib/ceph/mds/mds.$name
        jexec ${MDS_ID}  mkdir -p $MDS_DIR
        jexec ${MDS_ID}  ln -s $MDS_DIR /var/lib/ceph/mds/ceph-$name
        key_fn=$MDS_DIR/keyring

        if [ "$standby" -eq 1 ]; then
            mkdir -p ${MDS_DIR}s
            cat << __EOF >> ${BASE_CONF_FILE}
       mds standby for rank = $mds
[mds.${name}s]
        mds standby replay = true
        mds standby for name = ${name}
__EOF
            copy_config ceph-${mds}
        fi
        jexec ${MDS_ID} ceph-authtool --create-keyring --gen-key \
                --name="mds.$name" "$key_fn"
        jexec ${MDS_ID} ceph -i "$key_fn" auth add "mds.$name" \
                mon 'allow profile mds' osd 'allow *' mds 'allow' \
                mgr 'allow profile
 mds'
        if [ "$standby" -eq 1 ]; then
            jexec ${MDS_ID} ceph-authtool --create-keyring --gen-key \
                --name="mds.${name}s" "${MDS_DIR}s/keyring"
            jexec ${MDS_ID} ceph -i "${MDS_DIR}s/keyring" \
                auth add "mds.${name}s" mon 'allow profile mds' \
                osd 'allow *' mds 'allow' mgr 'allow profile mds'
        fi

        jexec ${MDS_ID} ceph-mds -i ${name} $ARGS $CMDS_ARGS
        if [ "$standby" -eq 1 ]; then
            jexec ${MDS_ID} ceph-mds -i ${name}s $ARGS $CMDS_ARGS
        fi

        jexec ${MDS_ID} ceph-authtool --create-keyring --gen-key \
            --name=mgr.$name $key_fn
        jexec ${MDS_ID} ceph -i $key_fn auth add mgr.$name \
            mon 'allow profile mgr' mds 'allow *' osd 'allow *'

        mds=$(($mds + 1))
    done
}

cephfs_create() {
    if [ "$FS_COUNT" -gt "0" ] ; then
        local cfs=0
        CFS_ID=`jls | grep ceph-${cfs} | awk '{print $1}'`
        CFS_IP=`jls | grep ceph-${cfs} | awk '{print $2}'`
        if [ "$FS_COUNT" -gt "1" ] ; then
            jexec ${CFS_ID} ceph fs flag set enable_multiple true --yes-i-really-mean-it
        fi

        for name in a b c d e f g h i j k l m n o p
        do
            jexec ${CFS_ID} ceph osd pool create "cephfs_data_${name}" 8
            jexec ${CFS_ID} ceph osd pool create "cephfs_metadata_${name}" 8
            jexec ${CFS_ID} ceph fs new "cephfs_${name}" "cephfs_metadata_${name}" "cephfs_data_${name}"
            fs=$(($fs + 1))
            [ $fs -eq $FS_COUNT ] && break
        done
    fi
}

network_delete() {
    #default base adress
    ifconfig ${CEPH_IF} alias ${CEPH_NET}.201
    for i in `jls | grep osd- | awk '{print $2}'`; do
         ifconfig ${CEPH_IF} -alias ${i}
    done
}

ezjails_delete() {
service ezjail onestop

for j in `zfs list | grep osd- | awk '{print $1}'` ; do
    zpool destroy -f $j
done

for j in /usr/jails/ceph-?; do
    name=`basename $j`
    if [ -d /usr/jails/$name ]; then
        ezjail-admin delete $name
        zfs destroy -rf zroot/jails/$name
        [ -d /usr/jails/$name ] && rm -rf /usr/jails/$name
    fi
done
}


cluster_create() {
    mon_create
    osd_create
    mgr_create
    mds_create
    cephfs_create
}

jails_create() {
    network_setup
    ezjail_setup
    zfs_create_osd_disks
    ezjail_pre_conf
    ezjail_start
    ezjail_post_conf
}

config_setup_defaults
config_setup "$*"

if [ $destroy -eq 0 ]; then
    if [ sysctl net.inet.tcp.msl -ne 3000 ]; then
        # Reduce the lockup time on released ports
        sysctl net.inet.tcp.msl=3000
        echo setting the msl setting: sysctl net.inet.tcp.msl=3000
        echo need to set this in /etc/sysctl.conf
    fi
    jails_create
    cluster_create

    jexec ${CEPH_ID0} ceph -s
    echo Cluster has started.
else
    ezjails_delete
    network_delete
    echo Cluster has terminated.
fi

