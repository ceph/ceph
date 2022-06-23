#!/usr/bin/env bash

# This is one helper for mounting the ceph-fuse/kernel clients by
# unsharing the network namespace, let's call it netns container.
# With the netns container, you can easily suspend or resume the
# virtual network interface to simulate the client node hard
# shutdown for some test cases.
#
#         netnsX                 netnsY                 netnsZ
#     --------------         --------------         --------------
#    | mount client |       | mount client |       | mount client |
#    |    default   |  ...  |    default   |  ...  |    default   |
#    |192.168.0.1/16|       |192.168.0.2/16|       |192.168.0.3/16|
#    |    veth0     |       |     veth0    |       |     veth0    |
#     --------------         --------------         -------------
#           |                       |                      |
#            \                      | brx.Y               /
#             \          ----------------------          /
#              \  brx.X |       ceph-brx       | brx.Z  / 
#               \------>|       default        |<------/
#                   |   |  192.168.255.254/16  |   |
#                   |    ----------------------    |
#            (suspend/resume)       |       (suspend/resume)
#                              -----------
#                             |  Physical |
#                             | A.B.C.D/M |
#                              -----------
#
# Defaultly it will use the 192.168.X.Y/16 private network IPs for
# the ceph-brx and netnses as above. And you can also specify your
# own new ip/mask for the ceph-brx, like:
#
#  $ unshare_ns_mount.sh --fuse /mnt/cephfs --brxip 172.19.100.100/12
#
# Then the each netns will get a new ip from the ranges:
# [172.16.0.1 ~ 172.19.100.99]/12 and [172.19.100.101 ~ 172.31.255.254]/12

usage() {
    echo ""
    echo "This will help to isolate the network namespace from OS for the mount client!"
    echo ""
    echo "usage: unshare_ns_mount.sh [OPTIONS [parameters]] [--brxip <ip_address/mask>]"
    echo "OPTIONS:" 
    echo -e "  --fuse    <ceph-fuse options>"
    echo -e "\tThe ceph-fuse command options"
    echo -e "\t  $ unshare_ns_mount.sh --fuse -m 192.168.0.1:6789 /mnt/cephfs -o nonempty"
    echo ""
    echo -e "  --kernel  <mount options>"
    echo -e "\tThe mount command options"
    echo -e "\t  $ unshare_ns_mount.sh --kernel -t ceph 192.168.0.1:6789:/ /mnt/cephfs -o fs=a"
    echo ""
    echo -e "  --suspend <mountpoint>"
    echo -e "\tDown the veth interface in the network namespace"
    echo -e "\t  $ unshare_ns_mount.sh --suspend /mnt/cephfs"
    echo ""
    echo -e "  --resume  <mountpoint>"
    echo -e "\tUp the veth interface in the network namespace"
    echo -e "\t  $ unshare_ns_mount.sh --resume /mnt/cephfs"
    echo ""
    echo -e "  --umount  <mountpoint>"
    echo -e "\tUmount and delete the network namespace"
    echo -e "\t  $ unshare_ns_mount.sh --umount /mnt/cephfs"
    echo ""
    echo -e "  --brxip   <ip_address/mask>"
    echo -e "\tSpecify ip/mask for ceph-brx and it only makes sense for --fuse/--kernel options"
    echo -e "\t(default: 192.168.255.254/16, netns ip: 192.168.0.1/16 ~ 192.168.255.253/16)"
    echo -e "\t  $ unshare_ns_mount.sh --fuse -m 192.168.0.1:6789 /mnt/cephfs --brxip 172.19.255.254/12"
    echo -e "\t  $ unshare_ns_mount.sh --kernel 192.168.0.1:6789:/ /mnt/cephfs --brxip 172.19.255.254/12"
    echo ""
    echo -e "  -h, --help"
    echo -e "\tPrint help"
    echo ""
}

CEPH_BRX=ceph-brx
CEPH_BRX_IP_DEF=192.168.255.254
NET_MASK_DEF=16
BRD_DEF=192.168.255.255

CEPH_BRX_IP=$CEPH_BRX_IP_DEF
NET_MASK=$NET_MASK_DEF
BRD=$BRD_DEF

mountpoint=""
new_netns=""
fuse_type=false

function get_mountpoint() {
    for param in $@
    do
        if [ -d $param ]; then
            # skipping "--client_mountpoint/-r root_directory"
            # option for ceph-fuse command
            if [ "$last" == "-r" -o "$last" == "--client_mountpoint" ]; then
                last=$param
                continue
	        fi
            if [ "0$mountpoint" != "0" ]; then
                echo "Oops: too many mountpoint options!"
                exit 1
            fi
            mountpoint=$param
        fi
        last=$param
    done

    if [ "0$mountpoint" == "0" ]; then
        echo "Oops: mountpoint path is not a directory or no mountpoint specified!"
        exit 1
    fi
}

function get_new_netns() {
    # prune the repeating slashes:
    # "/mnt///cephfs///" --> "/mnt/cephfs/"
    __mountpoint=`echo "$mountpoint" | sed 's/\/\+/\//g'`

    # prune the leading slashes
    while [ ${__mountpoint:0:1} == "/" ]
    do
        __mountpoint=${__mountpoint:1}
    done

    # prune the last slashes
    while [ ${__mountpoint: -1} == "/" ]
    do
        __mountpoint=${__mountpoint:0:-1}
    done

    # replace '/' with '-'
    __mountpoint=${__mountpoint//\//-}

    # "mnt/cephfs" --> "ceph-fuse-mnt-cephfs"
    if [ "$1" == "--fuse" ]; then
        new_netns=`echo ceph-fuse-$__mountpoint`
        fuse_type=true
        return
    fi

    # "mnt/cephfs" --> "ceph-kernel-mnt-cephfs"
    if [ "$1" == "--kernel" ]; then
        new_netns=`echo ceph-kernel-$__mountpoint`
        return
    fi

    # we are in umount/suspend/resume routines
    for ns in `ip netns list | awk '{print $1}'`
    do
        if [ "$ns" == "ceph-fuse-$__mountpoint" ]; then
            new_netns=$ns
            fuse_type=true
            return
        fi
        if [ "$ns" == "ceph-kernel-$__mountpoint" ]; then
            new_netns=$ns
            return
        fi
    done
    
    if [ "0$new_netns" == "0" ]; then
        echo "Oops, netns 'ceph-{fuse/kernel}-$__mountpoint' does not exists!"
        exit 1
    fi
}

# the peer veth name will be "brx.$nsid" on host node
function get_netns_brx() {
    get_new_netns 

    nsid=`ip netns list-id | grep "$new_netns" | awk '{print $2}'`
    netns_veth=brx.$nsid
    eval $1="$netns_veth"
}

function suspend_netns_veth() {
    get_mountpoint $@

    get_netns_brx brx
    ip link set $brx down
    exit 0
}

function resume_netns_veth() {
    get_mountpoint $@

    get_netns_brx brx
    ip link set $brx up
    exit 0
}

# help and usage
if [ $# == 0 -o "$1" == "-h" -o "$1" == "--help" ]; then
    usage
    exit 0
fi

# suspend the veth from network namespace
if [ $1 == "--suspend" ]; then
    suspend_netns_veth $@
    exit 0
fi

# resume the veth from network namespace
if [ $1 == "--resume" ]; then
    resume_netns_veth $@
    exit 0
fi

function ceph_umount() {
    get_mountpoint $@
    get_new_netns

    if [ $fuse_type == true ]; then
        nsenter --net=/var/run/netns/$new_netns fusermount -u $mountpoint 2>/dev/null
    else
        nsenter --net=/var/run/netns/$new_netns umount $mountpoint 2>/dev/null
    fi

    # let's wait for a while to let the umount operation
    # to finish before deleting the netns
    while [ 1 ]
    do
        for pid in `ip netns pids $new_netns 2>/dev/null`
        do
            name=`cat /proc/$pid/comm 2>/dev/null`
            if [ "$name" == "ceph-fuse" ]; then
                break
            fi
        done

        if [ "$name" == "ceph-fuse" ]; then
            name=""
            usleep 100000
            continue
        fi

        break
    done

    nsid=`ip netns list-id | grep "$new_netns" | awk '{print $2}'`
    netns_brx=brx.$nsid

    # brctl delif $CEPH_BRX $netns_brx 2>/dev/null
    nmcli connection down $netns_brx down 2>/dev/null
    nmcli connection delete $netns_brx 2>/dev/null

    ip netns delete $new_netns 2>/dev/null
    
    # if this is the last netns_brx, will delete
    # the $CEPH_BRX and restore the OS configure
    # rc=`brctl show ceph-brx 2>/dev/null | grep 'brx\.'|wc -l`
    rc=`nmcli connection show 2>/dev/null | grep 'brx\.' | wc -l`
    if [ $rc == 0 ]; then
        ip link set $CEPH_BRX down 2>/dev/null
        # brctl delbr $CEPH_BRX 2>/dev/null
        nmcli connection delete $CEPH_BRX 2>/dev/null

        # restore the ip forward
        tmpfile=`ls /tmp/ | grep "$CEPH_BRX\."`
        tmpfile=/tmp/$tmpfile
        if [ ! -f $tmpfile ]; then
            echo "Oops, the $CEPH_BRX.XXX temp file does not exist!"
        else
            save=`cat $tmpfile`
            echo $save > /proc/sys/net/ipv4/ip_forward
            rm -rf $tmpfile
        fi

        # drop the iptables NAT rules
        host_nic=`route | grep default | awk '{print $8}'`
        iptables -D FORWARD -o $host_nic -i $CEPH_BRX -j ACCEPT
        iptables -D FORWARD -i $host_nic -o $CEPH_BRX -j ACCEPT
        iptables -t nat -D POSTROUTING -s $CEPH_BRX_IP/$NET_MASK -o $host_nic -j MASQUERADE
    fi
}

function get_brd_mask() {
    first=`echo "$CEPH_BRX_IP" | awk -F. '{print $1}'`
    second=`echo "$CEPH_BRX_IP" | awk -F. '{print $2}'`
    third=`echo "$CEPH_BRX_IP" | awk -F. '{print $3}'`
    fourth=`echo "$CEPH_BRX_IP" | awk -F. '{print $4}'`

    if [ "$first" == "172" ]; then
        second_max=31
    else
        second_max=255
    fi
    third_max=255
    fourth_max=255

    if [ $NET_MASK -lt 16 ]; then
        let power=16-$NET_MASK
        m=`awk 'BEGIN{printf 2^"'$power'"-1}'`
        second=$((second&~m))
        let second_max=$second+$m
    elif [ $NET_MASK -lt 24 ]; then
        let power=24-$NET_MASK
        m=`awk 'BEGIN{printf 2^"'$power'"-1}'`
        third=$((third&~m))
        let third_max=$third+$m
        second_max=$second
    elif [ $NET_MASK -lt 32 ]; then
        let power=32-$NET_MASK
        m=`awk 'BEGIN{printf 2^"'$power'"-1}'`
        fourth=$((fourth&~m))
        let fourth_max=$fourth+$m
        second_max=$second
        third_max=$third
    fi

    BRD=$first.$second_max.$third_max.$fourth_max
}

# As default:
# The netns IP will be 192.168.0.1 ~ 192.168.255.253,
# and 192.168.255.254 is saved for $CEPH_BRX
function get_new_ns_ip() {
    first=`echo "$CEPH_BRX_IP" | awk -F. '{print $1}'`
    second=`echo "$CEPH_BRX_IP" | awk -F. '{print $2}'`
    third=`echo "$CEPH_BRX_IP" | awk -F. '{print $3}'`
    fourth=`echo "$CEPH_BRX_IP" | awk -F. '{print $4}'`

    if [ "$first" == ""172 ]; then
        second_max=31
    else
        second_max=255
    fi
    third_max=255
    fourth_max=254

    if [ $NET_MASK -lt 16 ]; then
        let power=16-$NET_MASK
        m=`awk 'BEGIN{printf 2^"'$power'"-1}'`
        second=$((second&~m))
        let second_max=$second+$m
        third=0
        fourth=1
    elif [ $NET_MASK -lt 24 ]; then
        let power=24-$NET_MASK
        m=`awk 'BEGIN{printf 2^"'$power'"-1}'`
        third=$((third&~m))
        let third_max=$third+$m
        second_max=$second
        fourth=1
    elif [ $NET_MASK -lt 32 ]; then
        let power=32-$NET_MASK
        m=`awk 'BEGIN{printf 2^"'$power'"-1}'`
        fourth=$((fourth&~m))
        let fourth+=1
        let fourth_max=$fourth+$m-1
        second_max=$second
        third_max=$third
    fi

    while [ $second -le $second_max -a $third -le $third_max -a $fourth -le $fourth_max ]
    do
        conflict=false

        # check from the existing network namespaces
        for netns in `ip netns list | awk '{print $1}'`
        do
            ip=`ip netns exec $netns ip addr | grep "inet " | grep "veth0"`
            ip=`echo "$ip" | awk '{print $2}' | awk -F/ '{print $1}'`
            if [ "0$ip" == "0" ]; then
                continue
            fi
            if [ "$first.$second.$third.$fourth" == "$ip" ]; then
                conflict=true

                let fourth+=1
                if [ $fourth -le $fourth_max ]; then
                     break
                fi
		
                fourth=0
                let third+=1
                if [ $third -le $third_max ]; then
                     break
                fi

                third=0
                let second+=1
                if [ $second -le $second_max ]; then
                    break
                fi

                echo "Oops: we have ran out of the ip addresses!"
                exit 1
            fi
        done

        # have we found one ?
        if [ $conflict == false ]; then
            break
        fi
    done

    ip=$first.$second.$third.$fourth
    max=$first.$second_max.$third_max.$fourth_max
    if [ "$ip" == "$max" ]; then
        echo "Oops: we have ran out of the ip addresses!"
        exit 1
    fi

    eval $1="$ip"
}

function check_valid_private_ip() {
    first=`echo "$1" | awk -F. '{print $1}'`
    second=`echo "$1" | awk -F. '{print $2}'`

    # private network class A 10.0.0.0 - 10.255.255.255
    if [ "$first" == "10" -a $NET_MASK -ge 8 ]; then
        return
    fi

    # private network class B 172.16.0.0 - 172.31.255.255
    if [ "$first" == "172" -a $second -ge 16 -a $second -le 31 -a $NET_MASK -ge 12 ]; then
        return
    fi

    # private network class C 192.168.0.0 - 192.168.255.255
    if [ "$first" == "192" -a "$second" == "168" -a $NET_MASK -ge 16 ]; then
        return
    fi

    echo "Oops: invalid private ip address '$CEPH_BRX_IP/$NET_MASK'!"
    exit 1
}

function setup_bridge_and_nat() {
    # check and parse the --brxip parameter
    is_brxip=false
    for ip in $@
    do
        if [ "$ip" == "--brxip" ]; then
            is_brxip=true
            continue
        fi
        if [ $is_brxip == true ]; then
            new_brxip=$ip
            break
        fi
    done

    # if the $CEPH_BRX already exists, then check the new
    # brxip, if not match fail it without doing anything.
    rc=`ip addr | grep "inet " | grep " $CEPH_BRX"`
    if [ "0$rc" != "0" ]; then
        existing_brxip=`echo "$rc" | awk '{print $2}'`
        if [ "0$new_brxip" != "0" -a "$existing_brxip" != "$new_brxip" ]; then
            echo "Oops: conflict with the existing $CEPH_BRX ip '$existing_brxip', new '$new_brxip'!"
            exit 1
        fi

        CEPH_BRX_IP=`echo "$existing_brxip" | awk -F/ '{print $1}'`
        NET_MASK=`echo "$existing_brxip" | awk -F/ '{print $2}'`
        get_brd_mask
        return
    fi

    # if it is the first time to run the the script or there
    # is no any network namespace exists, we need to setup
    # the $CEPH_BRX, if no --brxip is specified will use the
    # default $CEPH_BRX_IP/$NET_MASK
    if [ "0$new_brxip" != "0" ]; then
        CEPH_BRX_IP=`echo "$new_brxip" | awk -F/ '{print $1}'`
        NET_MASK=`echo "$new_brxip" | awk -F/ '{print $2}'`
        get_brd_mask
        check_valid_private_ip $CEPH_BRX_IP
    fi

    # brctl addbr $CEPH_BRX
    nmcli connection add type bridge con-name $CEPH_BRX ifname $CEPH_BRX stp no
    # ip link set $CEPH_BRX up
    # ip addr add $CEPH_BRX_IP/$NET_MASK brd $BRD dev $CEPH_BRX
    nmcli connection modify $CEPH_BRX ipv4.addresses $CEPH_BRX_IP/$NET_MASK ipv4.method manual
    nmcli connection up $CEPH_BRX

    # setup the NAT
    rm -rf /tmp/ceph-brx.*
    tmpfile=$(mktemp /tmp/ceph-brx.XXXXXXXX)
    save=`cat /proc/sys/net/ipv4/ip_forward`
    echo $save > $tmpfile
    echo 1 > /proc/sys/net/ipv4/ip_forward

    host_nic=`route | grep default | awk '{print $8}'`
    iptables -A FORWARD -o $host_nic -i $CEPH_BRX -j ACCEPT
    iptables -A FORWARD -i $host_nic -o $CEPH_BRX -j ACCEPT
    iptables -t nat -A POSTROUTING -s $CEPH_BRX_IP/$NET_MASK -o $host_nic -j MASQUERADE
}

function __ceph_mount() {
    # for some options like the '-t' in mount command
    # the nsenter command will take over it, so it is
    # hard to pass it direct to the netns.
    # here we will create one temp file with x mode
    tmpfile=$(mktemp /tmp/ceph-nsenter.XXXXXXXX)
    chmod +x $tmpfile
    if [ "$1" == "--kernel" ]; then
        cmd=`echo "$@" | sed 's/--kernel/mount/'`
    else
        cmd=`echo "$@" | sed 's/--fuse/ceph-fuse/'`
    fi

    # remove the --brxip parameter
    cmd=`echo "$cmd" | sed 's/--brxip.*\/[0-9]* //'`

    # enter $new_netns and run ceph fuse client mount,
    # we couldn't use 'ip netns exec' here because it
    # will unshare the mount namespace.
    echo "$cmd" > $tmpfile
    nsenter --net=/var/run/netns/$new_netns /bin/bash $tmpfile ; echo $? > $tmpfile
    rc=`cat $tmpfile`
    rm -f $tmpfile

    # fall back
    if [ $rc != 0 ]; then
        m=$mountpoint
        mountpoint=""
        ceph_umount $m
    fi
}

function get_new_nsid() {
    # get one uniq netns id
    uniq_id=0
    while [ 1 ]
    do
        rc=`ip netns list-id | grep "nsid $uniq_id "`
        if [ "0$rc" == "0" ]; then
            break
        fi
        let uniq_id+=1
    done

    eval $1="$uniq_id"
}

function ceph_mount() {
    get_mountpoint $@
    setup_bridge_and_nat $@

    get_new_netns $1
    rc=`ip netns list | grep "$new_netns" | awk '{print $1}'`
    if [ "0$rc" != "0" ]; then
        echo "Oops: the netns "$new_netns" already exists!"
        exit 1
    fi

    get_new_nsid new_nsid

    # create a new network namespace
    ip netns add $new_netns
    ip netns set $new_netns $new_nsid

    get_new_ns_ip ns_ip
    if [ 0"$ns_ip" == "0" ]; then
        echo "Oops: there is no ip address could be used any more!"
        exit 1
    fi

    # veth interface in netns
    ns_veth=veth0
    netns_brx=brx.$new_nsid

    # setup veth interfaces
    ip link add $ns_veth netns $new_netns type veth peer name $netns_brx
    ip netns exec $new_netns ip addr add $ns_ip/$NET_MASK brd $BRD dev $ns_veth
    ip netns exec $new_netns ip link set $ns_veth up
    ip netns exec $new_netns ip link set lo up
    ip netns exec $new_netns ip route add default via $CEPH_BRX_IP

    # bring up the bridge interface and join it to $CEPH_BRX
    # brctl addif $CEPH_BRX $netns_brx
    nmcli connection add type bridge-slave con-name $netns_brx ifname $netns_brx master $CEPH_BRX
    nmcli connection up $netns_brx
    # ip link set $netns_brx up

    __ceph_mount $@
}

if [ "$1" == "--umount" ]; then
    ceph_umount $@
    exit 0
fi

# mount in the netns
if [ "$1" != "--kernel" -a "$1" != "--fuse" ]; then
    echo "Oops: invalid mount options '$1'!"
    exit 1
fi

ceph_mount $@
