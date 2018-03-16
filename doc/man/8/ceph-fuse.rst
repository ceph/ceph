:orphan:

=========================================
 ceph-fuse -- FUSE-based client for ceph
=========================================

.. program:: ceph-fuse

Synopsis
========

| **ceph-fuse** [-n *client.username*] [ -m *monaddr*:*port* ] *mountpoint* [ *fuse options* ]


Description
===========

**ceph-fuse** is a FUSE (File system in USErspace) client for Ceph
distributed file system. It will mount a ceph file system specified
via the -m option or described by ceph.conf (see below) at the
specific mount point. See `Mount Ceph FS using FUSE`_ for detailed
information.

The file system can be unmounted with::

        fusermount -u mountpoint

or by sending ``SIGINT`` to the ``ceph-fuse`` process.


Options
=======

Any options not recognized by ceph-fuse will be passed on to libfuse.

.. option:: -o opt,[opt...]

   Mount options.

.. option:: -d

   Run in foreground, send all log otuput to stderr and enable FUSE debugging (-o debug).

.. option:: -c ceph.conf, --conf=ceph.conf

   Use *ceph.conf* configuration file instead of the default
   ``/etc/ceph/ceph.conf`` to determine monitor addresses during startup.

.. option:: -m monaddress[:port]

   Connect to specified monitor (instead of looking through ceph.conf).

.. option:: --client_mountpoint/-r root_directory

   Use root_directory as the mounted root, rather than the full Ceph tree.

.. option:: -f

   Foreground: do not daemonize after startup (run in foreground). Do not generate a pid file.

.. option:: -s

   Disable multi-threaded operation.

Availability
============

**ceph-fuse** is part of Ceph, a massively scalable, open-source, distributed storage system. Please refer to
the Ceph documentation at http://ceph.com/docs for more information.


See also
========

fusermount(8),
:doc:`ceph <ceph>`\(8)

.. _Mount Ceph FS using FUSE: ../../../cephfs/fuse/
