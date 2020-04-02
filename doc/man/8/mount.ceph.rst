:orphan:

========================================
 mount.ceph -- mount a Ceph file system
========================================

.. program:: mount.ceph

Synopsis
========

| **mount.ceph** [*mon1_socket*\ ,\ *mon2_socket*\ ,...]:/[*subdir*] *dir* [
  -o *options* ]


Description
===========

**mount.ceph** is a helper for mounting the Ceph file system on a Linux host.
It serves to resolve monitor hostname(s) into IP addresses and read
authentication keys from disk; the Linux kernel client component does most of
the real work. In fact, it is possible to mount a non-authenticated Ceph file
system without mount.ceph by specifying monitor address(es) by IP::

        mount -t ceph 1.2.3.4:/ /mnt/mycephfs

The first argument is the device part of the mount command. It includes host's
socket and path within CephFS that will be mounted at the mount point. The
socket, obviously, takes the form ip_address[:port]. If the port is not
specified, the Ceph default of 6789 is assumed. Multiple monitor addresses can
be passed by separating them by commas. Only one monitor is needed to mount
successfully; the client will learn about all monitors from any responsive
monitor. However, it is a good idea to specify more than one in case the one
happens to be down at the time of mount.

If the host portion of the device is left blank, then **mount.ceph** will
attempt to determine monitor addresses using local configuration files
and/or DNS SRV records. In similar way, if authentication is enabled on Ceph
cluster (which is done using CephX) and options ``secret`` and ``secretfile``
are not specified in the command, the mount helper will spawn a child process
that will use the standard Ceph library routines to find a keyring and fetch
the secret from it.

A sub-directory of the file system can be mounted by specifying the (absolute)
path to the sub-directory right after ":" after the socket in the device part
of the mount command.

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

:command: `fs=<fs-name>`
    Specify the non-default file system to be mounted. Not passing this
    option mounts the default file system.

:command: `mds_namespace=<fs-name>`
    A synonym of "fs=" and its use is deprecated.

:command:`mount_timeout`
    int (seconds), Default: 60

:command:`name`
    RADOS user to authenticate as when using CephX. Default: guest

:command:`secret`
    secret key for use with CephX. This option is insecure because it exposes
    the secret on the command line. To avoid this, use the secretfile option.

:command:`secretfile`
    path to file containing the secret key to use with CephX

:command:`recover_session=<no|clean>`
    Set auto reconnect mode in the case where the client is blacklisted. The
    available modes are ``no`` and ``clean``. The default is ``no``.

    - ``no``: never attempt to reconnect when client detects that it has been
       blacklisted. Blacklisted clients will not attempt to reconnect and
       their operations will fail too.

    - ``clean``: client reconnects to the Ceph cluster automatically when it
      detects that it has been blacklisted. During reconnect, client drops
      dirty data/metadata, invalidates page caches and writable file handles.
      After reconnect, file locks become stale because the MDS loses track of
      them. If an inode contains any stale file locks, read/write on the inode
      is not allowed until applications release all stale file locks.

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

:command:`osdtimeout`
    int (seconds), Default: 60

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

Examples
========

Mount the full file system::

    mount.ceph :/ /mnt/mycephfs

Assuming mount.ceph is installed properly, it should be automatically invoked
by mount(8)::

    mount -t ceph :/ /mnt/mycephfs

Mount only part of the namespace/file system::

    mount.ceph :/some/directory/in/cephfs /mnt/mycephfs

Mount non-default FS, in case cluster has multiple FSs::
    mount -t ceph :/ /mnt/mycephfs2 -o fs=mycephfs2
    
    or
    
    mount -t ceph :/ /mnt/mycephfs2 -o mds_namespace=mycephfs2 # This option name is deprecated.

Pass the monitor host's IP address, optionally::

    mount.ceph 192.168.0.1:/ /mnt/mycephfs

Pass the port along with IP address if it's running on a non-standard port::

    mount.ceph 192.168.0.1:7000:/ /mnt/mycephfs

If there are multiple monitors, passes addresses separated by a comma::

   mount.ceph 192.168.0.1,192.168.0.2,192.168.0.3:/ /mnt/mycephfs

If authentication is enabled on Ceph cluster::

    mount.ceph :/ /mnt/mycephfs -o name=fs_username

Pass secret key for CephX user optionally::

    mount.ceph :/ /mnt/mycephfs -o name=fs_username,secret=AQATSKdNGBnwLhAAnNDKnH65FmVKpXZJVasUeQ==

Pass file containing secret key to avoid leaving secret key in shell's command
history::

    mount.ceph :/ /mnt/mycephfs -o name=fs_username,secretfile=/etc/ceph/fs_username.secret


Availability
============

**mount.ceph** is part of Ceph, a massively scalable, open-source, distributed
storage system. Please refer to the Ceph documentation at http://ceph.com/docs
for more information.

Feature Availability
====================

The ``recover_session=`` option was added to mainline Linux kernels in v5.4.
``wsync`` and ``nowsync`` were added in v5.7.

See also
========

:doc:`ceph-fuse <ceph-fuse>`\(8),
:doc:`ceph <ceph>`\(8)
