:orphan:

========================================
 mount.ceph -- mount a Ceph file system
========================================

.. program:: mount.ceph

Synopsis
========

| **mount.ceph** *name*@*fsid*.*fs_name*=/[*subdir*] *dir* [-o *options* ]


Description
===========

**mount.ceph** is a helper for mounting the Ceph file system on a Linux host.
It serves to resolve monitor hostname(s) into IP addresses and read
authentication keys from disk; the Linux kernel client component does most of
the real work. To mount a Ceph file system use::

  mount.ceph name@07fe3187-00d9-42a3-814b-72a4d5e7d5be.fs_name=/ /mnt/mycephfs -o mon_addr=1.2.3.4

where "name" is the RADOS client name (referred to hereafter as "RADOS user",
and meaning any individual or system actor such as an application). 

Mount helper can fill in the cluster FSID by reading the ceph configuration file.
Its recommended to call the mount helper via mount(8) as per::

  mount -t ceph name@.fs_name=/ /mnt/mycephfs -o mon_addr=1.2.3.4

Note that the dot ``.`` still needs to be a part of the device string in this case.

The first argument is the device part of the mount command. It includes the
RADOS user for authentication, the file system name and a path within CephFS
that will be mounted at the mount point.

Monitor addresses can be passed using ``mon_addr`` mount option. Multiple monitor
addresses can be passed by separating addresses with a slash (`/`). Only one
monitor is needed to mount successfully; the client will learn about all monitors
from any responsive monitor. However, it is a good idea to specify more than one
in case the one happens to be down at the time of mount. Monitor addresses takes
the form ip_address[:port]. If the port is not specified, the Ceph default of 6789
is assumed.

If monitor addresses are not specified, then **mount.ceph** will attempt to determine
monitor addresses using local configuration files and/or DNS SRV records. In similar
way, if authentication is enabled on Ceph cluster (which is done using CephX) and
options ``secret`` and ``secretfile`` are not specified in the command, the mount
helper will spawn a child process that will use the standard Ceph library routines
to find a keyring and fetch the secret from it (including the monitor address and
FSID if those not specified).

A sub-directory of the file system can be mounted by specifying the (absolute)
path to the sub-directory right after "=" in the device part of the mount command.

Mount helper application conventions dictate that the first two options are
device to be mounted and the mountpoint for that device. Options must be
passed only after these fixed arguments.


Options
=======

Basic
-----

:command:`conf`
    Path to a ceph.conf file. This is used to initialize the Ceph context
    for autodiscovery of monitor addresses and auth secrets. The default is
    to use the standard search path for ceph.conf files.

:command:`mount_timeout`
    int (seconds), Default: 60

:command:`ms_mode=<legacy|crc|secure|prefer-crc|prefer-secure>`
    Set the connection mode that the client uses for transport. The available
    modes are:

    - ``legacy``: use messenger v1 protocol to talk to the cluster

    - ``crc``: use messenger v2, without on-the-wire encryption

    - ``secure``: use messenger v2, with on-the-wire encryption

    - ``prefer-crc``: crc mode, if denied agree to secure mode

    - ``prefer-secure``: secure mode, if denied agree to crc mode

:command:`mon_addr`
    Monitor address of the cluster in the form of ip_address[:port]

:command:`fsid`
    Cluster FSID. This can be found using `ceph fsid` command.

:command:`secret`
    secret key for use with CephX. This option is insecure because it exposes
    the secret on the command line. To avoid this, use the secretfile option.

:command:`secretfile`
    path to file containing the secret key to use with CephX

:command:`recover_session=<no|clean>`
    Set auto reconnect mode in the case where the client is blocklisted. The
    available modes are ``no`` and ``clean``. The default is ``no``.

    - ``no``: never attempt to reconnect when client detects that it has been
      blocklisted. Blocklisted clients will not attempt to reconnect and
      their operations will fail too.

    - ``clean``: client reconnects to the Ceph cluster automatically when it
      detects that it has been blocklisted. During reconnect, client drops
      dirty data/metadata, invalidates page caches and writable file handles.
      After reconnect, file locks become stale because the MDS loses track of
      them. If an inode contains any stale file locks, read/write on the inode
      is not allowed until applications release all stale file locks.

:command: `fs=<fs-name>`
    Specify the non-default file system to be mounted, when using the old syntax.

:command: `mds_namespace=<fs-name>`
    A synonym of "fs=" (Deprecated).

Advanced
--------
:command:`cap_release_safety`
    int, Default: calculated

:command:`caps_wanted_delay_max`
    int, cap release delay, Default: 60

:command:`caps_wanted_delay_min`
    int, cap release delay, Default: 5

:command:`dirstat`
    funky `cat dirname` for stats, Default: off

:command:`nodirstat`
    no funky `cat dirname` for stats

:command:`ip`
    my ip

:command:`noasyncreaddir`
    no dcache readdir

:command:`nocrc`
    no data crc on writes

:command:`noshare`
    create a new client instance, instead of sharing an existing instance of
    a client mounting the same cluster

:command:`osdkeepalive`
    int, Default: 5

:command:`osd_idle_ttl`
    int (seconds), Default: 60

:command:`rasize`
    int (bytes), max readahead. Default: 8388608 (8192*1024)

:command:`rbytes`
    Report the recursive size of the directory contents for st_size on
    directories.  Default: off

:command:`norbytes`
    Do not report the recursive size of the directory contents for
    st_size on directories.

:command:`readdir_max_bytes`
    int, Default: 524288 (512*1024)

:command:`readdir_max_entries`
    int, Default: 1024

:command:`rsize`
    int (bytes), max read size. Default: 16777216 (16*1024*1024)

:command:`snapdirname`
    string, set the name of the hidden snapdir. Default: .snap

:command:`write_congestion_kb`
    int (kb), max writeback in flight. scale with available
    memory. Default: calculated from available memory

:command:`wsize`
    int (bytes), max write size. Default: 16777216 (16*1024*1024) (writeback
    uses smaller of wsize and stripe unit)

:command:`wsync`
    Execute all namespace operations synchronously. This ensures that the
    namespace operation will only complete after receiving a reply from
    the MDS. This is the default.

:command:`nowsync`
    Allow the client to do namespace operations asynchronously. When this
    option is enabled, a namespace operation may complete before the MDS
    replies, if it has sufficient capabilities to do so.

:command:`crush_location=x`
    Specify the location of the client in terms of CRUSH hierarchy (since 5.8).
    This is a set of key-value pairs separated from each other by '|', with
    keys separated from values by ':'.  Note that '|' may need to be quoted
    or escaped to avoid it being interpreted as a pipe by the shell. The key
    is the bucket type name (e.g. rack, datacenter or region with default
    bucket types) and the value is the bucket name. For example, to indicate
    that the client is local to rack "myrack", data center "mydc" and region
    "myregion"::

      crush_location=rack:myrack|datacenter:mydc|region:myregion

    Each key-value pair stands on its own: "myrack" doesn't need to reside in
    "mydc", which in turn doesn't need to reside in "myregion".  The location
    is not a path to the root of the hierarchy but rather a set of nodes that
    are matched independently.  "Multipath" locations are supported, so it is
    possible to indicate locality for multiple parallel hierarchies::

      crush_location=rack:myrack1|rack:myrack2|datacenter:mydc


:command:`read_from_replica=<no|balance|localize>`
    - ``no``: Disable replica reads, always pick the primary OSD (since 5.8, default).

    - ``balance``: When a replicated pool receives a read request, pick a random
      OSD from the PG's acting set to serve it (since 5.8).

      This mode is safe for general use only since Octopus (i.e. after "ceph osd
      require-osd-release octopus"). Otherwise it should be limited to read-only
      workloads such as snapshots.

    - ``localize``: When a replicated pool receives a read request, pick the most
      local OSD to serve it (since 5.8). The locality metric is calculated against
      the location of the client given with crush_location; a match with the
      lowest-valued bucket type wins.  For example, an OSD in a matching rack
      is closer than an OSD in a matching data center, which in turn is closer
      than an OSD in a matching region.

      This mode is safe for general use only since Octopus (i.e. after "ceph osd
      require-osd-release octopus").  Otherwise it should be limited to read-only
      workloads such as snapshots.



Examples
========

Mount the full file system::

    mount -t ceph fs_user@.mycephfs2=/ /mnt/mycephfs

Mount only part of the namespace/file system::

    mount.ceph fs_user@.mycephfs2=/some/directory/in/cephfs /mnt/mycephfs

Pass the monitor host's IP address, optionally::

    mount.ceph fs_user@.mycephfs2=/ /mnt/mycephfs -o mon_addr=192.168.0.1

Pass the port along with IP address if it's running on a non-standard port::

    mount.ceph fs_user@.mycephfs2=/ /mnt/mycephfs -o mon_addr=192.168.0.1:7000

If there are multiple monitors, pass each address separated by a `/`::

   mount.ceph fs_user@.mycephfs2=/ /mnt/mycephfs -o mon_addr=192.168.0.1/192.168.0.2/192.168.0.3

Pass secret key for CephX user optionally::

    mount.ceph fs_user@.mycephfs2=/ /mnt/mycephfs -o secret=AQATSKdNGBnwLhAAnNDKnH65FmVKpXZJVasUeQ==

Pass file containing secret key to avoid leaving secret key in shell's command
history::

    mount.ceph fs_user@.mycephfs2=/ /mnt/mycephfs -o secretfile=/etc/ceph/fs_username.secret

If authentication is disabled on Ceph cluster, omit the credential related option::

    mount.ceph fs_user@.mycephfs2=/ /mnt/mycephfs

To mount using the old syntax::

    mount -t ceph 192.168.0.1:/ /mnt/mycephfs

Availability
============

**mount.ceph** is part of Ceph, a massively scalable, open-source, distributed
storage system. Please refer to the Ceph documentation at https://docs.ceph.com
for more information.

Feature Availability
====================

The ``recover_session=`` option was added to mainline Linux kernels in v5.4.
``wsync`` and ``nowsync`` were added in v5.7.

See also
========

:doc:`ceph-fuse <ceph-fuse>`\(8),
:doc:`ceph <ceph>`\(8)
