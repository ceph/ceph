:orphan:

========================================
 mount.ceph -- mount a ceph file system
========================================

.. program:: mount.ceph

Synopsis
========

| **mount.ceph** *monaddr1*\ [,\ *monaddr2*\ ,...]:/[*subdir*] *dir* [
  -o *options* ]


Description
===========

**mount.ceph** is a simple helper for mounting the Ceph file system on
a Linux host. It serves to resolve monitor hostname(s) into IP
addresses and read authentication keys from disk; the Linux kernel
client component does most of the real work. In fact, it is possible
to mount a non-authenticated Ceph file system without mount.ceph by
specifying monitor address(es) by IP::

        mount -t ceph 1.2.3.4:/ mountpoint

Each monitor address monaddr takes the form host[:port]. If the port
is not specified, the Ceph default of 6789 is assumed.

Multiple monitor addresses can be separated by commas. Only one
responsible monitor is needed to successfully mount; the client will
learn about all monitors from any responsive monitor. However, it is a
good idea to specify more than one in case one happens to be down at
the time of mount.

A subdirectory subdir may be specified if a subset of the file system
is to be mounted.

Mount helper application conventions dictate that the first two
options are device to be mounted and destination path. Options must be
passed only after these fixed arguments.


Options
=======

:command:`wsize`
  int, max write size. Default: none (writeback uses smaller of wsize
  and stripe unit)

:command:`rsize`
  int (bytes), max readahead, multiple of 1024, Default: 524288
  (512*1024)

:command:`osdtimeout`
  int (seconds), Default: 60

:command:`osdkeepalive`
  int, Default: 5

:command:`mount_timeout`
  int (seconds), Default: 60

:command:`osd_idle_ttl`
  int (seconds), Default: 60

:command:`caps_wanted_delay_min`
  int, cap release delay, Default: 5

:command:`caps_wanted_delay_max`
  int, cap release delay, Default: 60

:command:`cap_release_safety`
  int, Default: calculated

:command:`readdir_max_entries`
  int, Default: 1024

:command:`readdir_max_bytes`
  int, Default: 524288 (512*1024)

:command:`write_congestion_kb`
  int (kb), max writeback in flight. scale with available
  memory. Default: calculated from available memory

:command:`snapdirname`
  string, set the name of the hidden snapdir. Default: .snap

:command:`name`
  RADOS user to authenticate as when using cephx. Default: guest

:command:`secret`
  secret key for use with cephx. This option is insecure because it exposes
  the secret on the command line. To avoid this, use the secretfile option.

:command:`secretfile`
  path to file containing the secret key to use with cephx

:command:`ip`
  my ip

:command:`noshare`
  create a new client instance, instead of sharing an existing
  instance of a client mounting the same cluster

:command:`dirstat`
  funky `cat dirname` for stats, Default: off

:command:`nodirstat`
  no funky `cat dirname` for stats

:command:`rbytes`
  Report the recursive size of the directory contents for st_size on
  directories.  Default: on

:command:`norbytes`
  Do not report the recursive size of the directory contents for
  st_size on directories.

:command:`nocrc`
  no data crc on writes

:command:`noasyncreaddir`
  no dcache readdir


Examples
========

Mount the full file system::

        mount.ceph monhost:/ /mnt/foo

If there are multiple monitors::

        mount.ceph monhost1,monhost2,monhost3:/ /mnt/foo

If :doc:`ceph-mon <ceph-mon>`\(8) is running on a non-standard
port::

        mount.ceph monhost1:7000,monhost2:7000,monhost3:7000:/ /mnt/foo

To mount only part of the namespace::

        mount.ceph monhost1:/some/small/thing /mnt/thing

Assuming mount.ceph(8) is installed properly, it should be
automatically invoked by mount(8) like so::

        mount -t ceph monhost:/ /mnt/foo


Availability
============

**mount.ceph** is part of Ceph, a massively scalable, open-source, distributed storage system. Please
refer to the Ceph documentation at http://ceph.com/docs for more
information.

See also
========

:doc:`ceph-fuse <ceph-fuse>`\(8),
:doc:`ceph <ceph>`\(8)
