Testing changes to the Linux Kernel CephFS driver
=================================================

This walkthrough will explain one (opinionated) way to do testing of the Linux
kernel client against a development cluster. We will try to mimimize any
assumptions about pre-existing knowledge of how to do kernel builds or any
related best-practices.

.. note:: There are many completely valid ways to do kernel development for
          Ceph. This guide is a walkthrough of the author's own environment.
          You may decide to do things very differently.

Step One: build the kernel
==========================

Clone the kernel:

.. code-block:: bash

    git init linux && cd linux
    git remote add torvalds git://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git
    git remote add ceph https://github.com/ceph/ceph-client.git
    git fetch && git checkout torvalds/master


Configure the kernel:

.. code-block:: bash

    make defconfig

.. note:: You can alternatively use the `Ceph Kernel QA Config`_ for building the kernel.

We now have a kernel config with reasonable defaults for the architecture you're
building on. The next thing to do is to enable configs which will build Ceph and/or
provide functionality we need to do testing.

.. code-block:: bash

    cat > ~/.ceph.config <<EOF
    CONFIG_CEPH_FS=y
    CONFIG_CEPH_FSCACHE=y
    CONFIG_CEPH_FS_POSIX_ACL=y
    CONFIG_CEPH_FS_SECURITY_LABEL=y
    CONFIG_CEPH_LIB_PRETTYDEBUG=y
    CONFIG_DYNAMIC_DEBUG=y
    CONFIG_DYNAMIC_DEBUG_CORE=y
    CONFIG_FRAME_POINTER=y
    CONFIG_FSCACHE
    CONFIG_FSCACHE_STATS
    CONFIG_FS_ENCRYPTION=y
    CONFIG_FS_ENCRYPTION_ALGS=y
    CONFIG_KGDB=y
    CONFIG_KGDB_SERIAL_CONSOLE=y
    CONFIG_XFS_FS=y
    EOF

Beyond enabling Ceph-related configs, we are also enabling some useful
debug configs and XFS (as an alternative to ext4 if needed for our root file
system).

.. note:: It is a good idea to not build anything as a kernel module. Otherwise, you would need to ``make modules_install`` on the root drive of the VM.

Now, merge the configs.


.. code-block:: bash


    scripts/kconfig/merge_config.sh .config ~/.ceph.config


Finally, build the kernel:

.. code-block:: bash

    make -j


.. note:: This document does not discuss how to get relevant utilities for your
          distribution to actually build the kernel, like gcc. Please use your search
          engine of choice to learn how to do that.


Step Two: create a VM
=====================

A virtual machine is a good choice for testing the kernel client for a few reasons:

* You can more easily monitor and configure networking for the VM.
* You can very rapidly test a change to the kernel (build -> mount in less than 10 seconds).
* A fault in the kernel won't crash your machine.
* You have a suite of tools available for analysis on the running kernel.

The main decision for you to make is what Linux distribution you want to use.
This document uses Arch Linux due to the author's familiarity. We also use LVM
to create a volume. You may use partitions or whatever mechanism you like to
create a block device. In general, this block device will be used repeatedly in
testing. You may want to use snapshots to avoid a VM somehow corrupting your
root disk and forcing you to start over.


.. code-block:: bash

    # create a volume
    VOLUME_GROUP=foo
    sudo lvcreate -L 256G "$VOLUME_GROUP" -n $(whoami)-vm-0
    DEV="/dev/${VOLUME_GROUP}/$(whoami)-vm-0"
    sudo mkfs.xfs "$DEV"
    sudo mount "$DEV" /mnt
    sudo pacstrap /mnt base base-devel vim less jq
    sudo arch-chroot /mnt
    # # delete root's password for ease of login
    # passwd -d root
    # mkdir -p /root/.ssh && echo "$YOUR_SSH_KEY_PUBKEY" >> /root/.ssh/authorized_keys
    # exit
    sudo umount /mnt

Once that's done, we should be able to run a VM:


.. code-block:: bash

    qemu-system-x86_64 -enable-kvm -kernel $(pwd)/arch/x86/boot/bzImage -drive file="$DEV",if=virtio,format=raw -append 'root=/dev/vda rw'

You should see output like:

::

    VNC server running on ::1:5900

You could view that console using:


.. code-block:: bash

    vncviewer 127.0.0.1:5900

Congratulations, you have a VM running the kernel that you just built.


Step Three: Networking the VM
=============================

This is the "hard part" and requires the most customization depending on what
you want to do. For this author, I currently have a development setup like:


::

     sepian netns
    ______________
   |              |
   | kernel VM    |              sepia-bounce VM      vossi04.front.sepia.ceph.com
   |  -------  |  |                  ------                    -------
   |  |     |  |  | 192.168.20.1     |    |                    |     |
   |  |     |--|--|- <- wireguard -> |    |  <-- sepia vpn ->  |     |
   |  |_____|  |  |     192.168.20.2 |____|                    |_____|
   |          br0 |
   |______________|


The sepia-bounce VM is used as a bounce box to the sepia lab. It can proxy ssh
connections, route any sepia-bound traffic, or serve as a DNS proxy. The use of
a sepia-bounce VM is optional but can be useful, especially if you want to
create numerous kernel VMs for testing.

I like to use the vossi04 `developer playground`_ to build Ceph and setup a
vstart cluster.  It has sufficient resources to make building Ceph very fast
(~5 minutes cold build) and local disk resources to run a decent vstart
cluster.

To avoid overcomplicating this document with the details of the sepia-bounce
VM, I will note the following main configurations used for the purpose of
testing the kernel:

- setup a wireguard tunnel between the machine creating kernel VMs and the sepia-bounce VM
- use ``systemd-resolved`` as a DNS resolver and listen on 192.168.20.2 (instead of just localhost)
- connect to the sepia `VPN`_ and use `systemd resolved update script`_ to configure ``systemd-resolved`` to use the DNS servers acquired via DHCP from the sepia VPN
- configure ``firewalld`` to allow wireguard traffic and to masquerade and forward traffic to the sepia vpn

The next task is to connect the kernel VM to the sepia-bounce VM. A network
namespace can be useful for this purpose to isolate traffic / routing rules for
the VMs. For me, I orchestrate this using a custom systemd one-shot unit that
looks like:

::

    # create the net namespace
    ExecStart=/usr/bin/ip netns add sepian
    # bring lo up
    ExecStart=/usr/bin/ip netns exec sepian ip link set dev lo up
    # setup wireguard to sepia-bounce
    ExecStart=/usr/bin/ip link add wg-sepian type wireguard
    ExecStart=/usr/bin/wg setconf wg-sepian /etc/wireguard/wg-sepian.conf
    # move the wireguard interface to the sepian nents
    ExecStart=/usr/bin/ip link set wg-sepian netns sepian
    # configure the static ip and bring it up
    ExecStart=/usr/bin/ip netns exec sepian ip addr add 192.168.20.1/24 dev wg-sepian
    ExecStart=/usr/bin/ip netns exec sepian ip link set wg-sepian up
    # logging info
    ExecStart=/usr/bin/ip netns exec sepian ip addr
    ExecStart=/usr/bin/ip netns exec sepian ip route
    # make wireguard the default route
    ExecStart=/usr/bin/ip netns exec sepian ip route add default via 192.168.20.2 dev wg-sepian
    # more logging
    ExecStart=/usr/bin/ip netns exec sepian ip route
    # add a bridge interface for VMs
    ExecStart=/usr/bin/ip netns exec sepian ip link add name br0 type bridge
    # configure the addresses and bring it up
    ExecStart=/usr/bin/ip netns exec sepian ip addr add 192.168.0.1/24 dev br0
    ExecStart=/usr/bin/ip netns exec sepian ip link set br0 up
    # masquerade/forward traffic to sepia-bounce
    ExecStart=/usr/bin/ip netns exec sepian iptables -t nat -A POSTROUTING -o wg-sepian -j MASQUERADE


When using the network namespace, we will use ``ip netns exec``. There is a
handy feature to automatically bind mount files into the ``/etc`` namespace for
commands run via that command:

::

    # cat /etc/netns/sepian/resolv.conf
    nameserver 192.168.20.2

That file will configure the libc name resolution stack to route DNS requests
for applications to the ``systemd-resolved`` daemon running on sepia-bounce.
Consequently, any application running in that netns will be able to resolve
sepia hostnames:

::

    $ sudo ip netns exec sepian host vossi04.front.sepia.ceph.com
    vossi04.front.sepia.ceph.com has address 172.21.10.4


Okay, great. We have a network namespace that forwards traffic to the sepia
VPN.  The next mental step is to connect virtual machines running a kernel to
the bridge we have configured. The straightforward way to do that is to create
a "tap" device which connects to the bridge:

.. code-block:: bash

     sudo ip netns exec sepian qemu-system-x86_64 \
         -enable-kvm \
         -kernel $(pwd)/arch/x86/boot/bzImage \
         -drive file="$DEV",if=virtio,format=raw \
         -netdev tap,id=net0,ifname=tap0,script="$HOME/bin/qemu-br0",downscript=no \
         -device virtio-net-pci,netdev=net0 \
         -append 'root=/dev/vda rw'

The new relevant bits here are (a) executing the VM in the netns we have
constructed; (b) a ``-netdev``  command to configure a tap device; (c) a
virtual network card for the VM. There is also a script ``$HOME/bin/qemu-br0``
run by qemu to configure the tap device it creates for the VM:

::

    #!/bin/bash
    tap=$1
    ip link set "$tap" master br0
    ip link set dev "$tap" up

That simply plugs the new tap device into the bridge.

This is all well and good but we are now missing one last crucial step. What is
the IP address of the VM?  There are two options:

1. configure a static IP but the VM's root device networking stack
   configuration must be modified
2. use DHCP and configure the root device for VMs to always use dhcp to
   configure their ethernet device addresses

The second option is more complicated to setup, since you must run a DHCP
server now, but provides the greatest flexibility for adding more VMs as needed
when testing.

The modified (or "hacked") standard dhcpd systemd service looks like:

::

    # cat sepian-dhcpd.service
    [Unit]
    Description=IPv4 DHCP server
    After=network.target network-online.target sepian-netns.service
    Wants=network-online.target
    Requires=sepian-netns.service
    
    [Service]
    ExecStartPre=/usr/bin/touch /tmp/dhcpd.leases
    ExecStartPre=/usr/bin/cat /etc/netns/sepian/dhcpd.conf
    ExecStart=/usr/bin/dhcpd -f -4 -q -cf /etc/netns/sepian/dhcpd.conf -lf /tmp/dhcpd.leases
    NetworkNamespacePath=/var/run/netns/sepian
    RuntimeDirectory=dhcpd4
    User=dhcp
    AmbientCapabilities=CAP_NET_BIND_SERVICE CAP_NET_RAW
    ProtectSystem=full
    ProtectHome=on
    KillSignal=SIGINT
    # We pull in network-online.target for a configured network connection.
    # However this is not guaranteed to be the network connection our
    # networks are configured for. So try to restart on failure with a delay
    # of two seconds. Rate limiting kicks in after 12 seconds.
    RestartSec=2s
    Restart=on-failure
    StartLimitInterval=12s
    
    [Install]
    WantedBy=multi-user.target

Similarly, the referenced dhcpd.conf:

::

    # cat /etc/netns/sepian/dhcpd.conf
    option domain-name-servers 192.168.20.2;
    option subnet-mask 255.255.255.0;
    option routers 192.168.0.1;
    subnet 192.168.0.0 netmask 255.255.255.0 {
        range 192.168.0.100 192.168.0.199;
    }

Importantly, this tells the VM to route traffic to 192.168.0.1 (the IP of the
bridge in the netns) and DNS can be provided by 192.168.20.2 (via
``systemd-resolved`` on the sepia-bounce VM).

In the VM, the networking looks like:

::

	[root@archlinux ~]# ip link
	1: lo: <LOOPBACK,UP,LOWER_UP> mtu 65536 qdisc noqueue state UNKNOWN mode DEFAULT group default qlen 1000
    	link/loopback 00:00:00:00:00:00 brd 00:00:00:00:00:00
	2: enp0s3: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc pfifo_fast state UP mode DEFAULT group default qlen 1000
    	link/ether 52:54:00:12:34:56 brd ff:ff:ff:ff:ff:ff
	3: sit0@NONE: <NOARP> mtu 1480 qdisc noop state DOWN mode DEFAULT group default qlen 1000
    	link/sit 0.0.0.0 brd 0.0.0.0
	[root@archlinux ~]# ip addr
	1: lo: <LOOPBACK,UP,LOWER_UP> mtu 65536 qdisc noqueue state UNKNOWN group default qlen 1000
    	link/loopback 00:00:00:00:00:00 brd 00:00:00:00:00:00
    	inet 127.0.0.1/8 scope host lo
       	valid_lft forever preferred_lft forever
    	inet6 ::1/128 scope host noprefixroute 
       	valid_lft forever preferred_lft forever
	2: enp0s3: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc pfifo_fast state UP group default qlen 1000
    	link/ether 52:54:00:12:34:56 brd ff:ff:ff:ff:ff:ff
    	inet 192.168.0.100/24 metric 1024 brd 192.168.0.255 scope global dynamic enp0s3
       	valid_lft 28435sec preferred_lft 28435sec
    	inet6 fe80::5054:ff:fe12:3456/64 scope link proto kernel_ll 
       	valid_lft forever preferred_lft forever
	3: sit0@NONE: <NOARP> mtu 1480 qdisc noop state DOWN group default qlen 1000
    	link/sit 0.0.0.0 brd 0.0.0.0
	[root@archlinux ~]# systemd-resolve --status
	Global
           	Protocols: +LLMNR +mDNS -DNSOverTLS DNSSEC=no/unsupported
    	resolv.conf mode: stub
	Fallback DNS Servers: 1.1.1.1#cloudflare-dns.com 9.9.9.9#dns.quad9.net 8.8.8.8#dns.google 2606:4700:4700::1111#cloudflare-dns.com 2620:fe::9#dns.quad9.net 2001:4860:4860::8888#dns.google
	
	Link 2 (enp0s3)
    	Current Scopes: DNS LLMNR/IPv4 LLMNR/IPv6
         	Protocols: +DefaultRoute +LLMNR -mDNS -DNSOverTLS DNSSEC=no/unsupported
	Current DNS Server: 192.168.20.2
       	DNS Servers: 192.168.20.2
	
	Link 3 (sit0)
    	Current Scopes: none
         	Protocols: -DefaultRoute +LLMNR +mDNS -DNSOverTLS DNSSEC=no/unsupported


Finally, some other networking configurations to consider:

* Run the VM on your machine with full access to the host networking stack. If you have the sepia vpn, this will probably work without too much configuration.
* Run the VM in a netns as above but also setup the sepia vpn in the same netns. This can help to avoid using a sepia-bounce VM. You'll still need to configure routing between the bridge and the sepia VPN.
* Run the VM in a netns as above but only use a local vstart cluster (possibly in another VM) in the same netns.


Step Four: mounting a CephFS file system in your VM
---------------------------------------------------

This guide uses a vstart cluster on a machine in the sepia lab. Because the mon
addresses will change with any new vstart cluster, it will invalidate any
static configuration we may setup for our VM mounting the CephFS via the kernel
driver.  So, we should create a script to fetch the configuration for our
vstart cluster prior to mounting:

.. code-block:: bash

    #!/bin/bash
    # kmount.sh -- mount a vstart Ceph cluster on a remote machine
    
    # the cephx client credential, vstart creates "client.fs" by default
    NAME=fs
    # static fs name, vstart creates an "a" file system by default
    FS=a
    # where to mount on the VM
    MOUNTPOINT=/mnt
    # cephfs mount point (root by default)
    CEPHFS_MOUNTPOINT=/
    
    function run {
        printf '%s\n' "$*" >&2
        "$@"
    }
    
    function mssh {
        run ssh vossi04.front.sepia.ceph.com "cd ceph/build && (source vstart_environment.sh; $1)"
    }
    
    # create the minimum config (including mon addresses) and store it in the VM's ceph.conf. This is not used for mounting; we're storing it for potential use with `ceph` commands.
    mssh "ceph config generate-minimal-conf" > /etc/ceph/ceph.conf
    # get the vstart cluster's fsid
    FSID=$(mssh "ceph fsid")
    # get the auth key associated with client.fs
    KEY=$(mssh "ceph auth get-key client.$NAME")
    # dump the v2 mon addresses and format for the -o mon_addr mount option
    MONS=$(mssh "ceph mon dump --format=json" | jq -r '.mons[] | .public_addrs.addrvec[] | select(.type == "v2") | .addr' | paste -s -d/)
    
    # turn on kernel debugging (and any other debugging you'd like)
    echo "module ceph +p" | tee /sys/kernel/debug/dynamic_debug/control
    # do the mount! we use the new device syntax for this mount
    run mount -t ceph "${NAME}@${FSID}.${FS}=${CEPHFS_MOUNTPOINT}" -o "mon_addr=${MONS},ms_mode=crc,name=${NAME},secret=${KEY},norequire_active_mds,noshare" "$MOUNTPOINT"

That would be run like:

.. code-block:: bash

    $ sudo ip netns exec sepian ssh root@192.168.0.100 ./kmount.sh
    ...
    mount -t ceph fs@c9653bca-110b-4f70-9f84-5a195b205e9a.a=/ -o mon_addr=172.21.10.4:40762/172.21.10.4:40764/172.21.10.4:40766,ms_mode=crc,name=fs,secret=AQD0jgln43pBCxAA7cJlZ4Px7J0UmiK4A4j3rA==,norequire_active_mds,noshare /mnt
    $ sudo ip netns exec sepian ssh root@192.168.0.100 df -h /mnt
    Filesystem                                   Size  Used Avail Use% Mounted on
    fs@c9653bca-110b-4f70-9f84-5a195b205e9a.a=/  169G     0  169G   0% /mnt


If you run into difficulties, it may be:

* The firewall on the node running the vstart cluster is blocking your connections.
* Some misconfiguration in your networking stack.
* An incorrect configuration for the mount.


Step Five: testing kernel changes in teuthology
-----------------------------------------------

There 3 static branches in the `ceph kernel git repository`_ managed by the Ceph team:

* `for-linus <https://github.com/ceph/ceph-client/tree/for-linus>`_: A branch managed by the primary Ceph maintainer to share changes with Linus Torvalds (upstream). Do not push to this branch.
* `master <https://github.com/ceph/ceph-client/tree/master>`_: A staging ground for patches planned to be sent to Linus. Do not push to this branch. 
* `testing <https://github.com/ceph/ceph-client/tree/testing>`_ A staging ground for miscellaneous patches that need wider QA testing (via nightlies or regular Ceph QA testing). Push patches you believe to be nearly ready for upstream acceptance.

You may also push a ``wip-$feature`` branch to the ``ceph-client.git``
repository which will be built by Jenkins. Then view the results of the build
in `Shaman <https://shaman.ceph.com/builds/kernel/>`_.

Once a kernel branch is built, you can test it via the ``fs`` CephFS QA suite:

.. code-block:: bash

    $ teuthology-suite ... --suite fs --kernel wip-$feature --filter k-testing


The ``k-testing`` filter is looking for the fragment which normally sets
``testing`` branch of the kernel for routine QA. That is, the ``fs`` suite
regularly runs tests against whatever is in the ``testing`` branch of the
kernel. We are overriding that choice of kernel branch via the ``--kernel
wip-$featuree`` switch.

.. note:: Without filtering for ``k-testing``, the ``fs`` suite will also run jobs using ceph-fuse or stock kernel, libcephfs tests, and other tests that may not be of interest to you when evaluating changes to the kernel.

The actual override is controlled using Lua merge scripts in the
``k-testing.yaml`` fragment. See that file for more details.


.. _VPN: https://wiki.sepia.ceph.com/doku.php?id=vpnaccess
.. _systemd resolved update script: systemd-resolved: https://wiki.archlinux.org/title/Systemd-resolved
.. _Ceph Kernel QA Config: https://github.com/ceph/ceph-build/tree/899d0848a0f487f7e4cee773556aaf9529b8db26/kernel/build
.. _developer playground: https://wiki.sepia.ceph.com/doku.php?id=devplayground#developer_playgrounds
.. _ceph kernel git repository: https://github.com/ceph/ceph-client
