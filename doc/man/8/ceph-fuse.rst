:orphan:

.. _man-ceph-fuse: 

=========================================
 ceph-fuse -- FUSE-based client for ceph
=========================================

.. program:: ceph-fuse

Synopsis
========

| **ceph-fuse** [-n *client.username*] [ -m *monaddr*:*port* ] *mountpoint* [ *fuse options* ]


Description
===========

**ceph-fuse** is a FUSE ("Filesystem in USErspace") client for Ceph
distributed file system. It will mount a ceph file system specified via the -m
option or described by ceph.conf (see below) at the specific mount point. See
`Mount CephFS using FUSE`_ for detailed information.

The file system can be unmounted with::

        fusermount -u mountpoint

or by sending ``SIGINT`` to the ``ceph-fuse`` process.


Options
=======

Any options not recognized by ceph-fuse will be passed on to libfuse.

.. option:: -o opt,[opt...]

   Mount options.

.. option:: -c ceph.conf, --conf=ceph.conf

   Use *ceph.conf* configuration file instead of the default
   ``/etc/ceph/ceph.conf`` to determine monitor addresses during startup.

.. option:: -m monaddress[:port]

   Connect to specified monitor (instead of looking through ceph.conf).

.. option:: -n client.{cephx-username}

   Pass the name of CephX user whose secret key is be to used for mounting.

.. option:: --id <client-id>

   Pass the name of CephX user whose secret key is be to used for mounting.
   ``--id`` takes just the ID of the client in contrast to ``-n``. For
   example, ``--id 0`` for using ``client.0``.

.. option:: -k <path-to-keyring>

   Provide path to keyring; useful when it's absent in standard locations.

.. option:: --client_mountpoint/-r root_directory

   Use root_directory as the mounted root, rather than the full Ceph tree.

.. option:: -f

   Foreground: do not daemonize after startup (run in foreground). Do not generate a pid file.

.. option:: -d

   Run in foreground, send all log output to stderr and enable FUSE debugging
   (-o debug).

.. option:: -s

   Disable multi-threaded operation.

.. option:: --client_fs

   Pass the name of Ceph FS to be mounted. Not passing this option mounts the
   default Ceph FS on the Ceph cluster.

Availability
============

**ceph-fuse** is part of Ceph, a massively scalable, open-source, distributed storage system. Please refer to
the Ceph documentation at https://docs.ceph.com for more information.


See also
========

fusermount(8),
:doc:`ceph <ceph>`\(8)

.. _Mount CephFS using FUSE: ../../../cephfs/mount-using-fuse/
