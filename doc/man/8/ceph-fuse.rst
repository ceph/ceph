:orphan:

=========================================
 ceph-fuse -- FUSE-based client for ceph
=========================================

.. program:: ceph-fuse

Synopsis
========

| **ceph-fuse** [ -m *monaddr*:*port* ] *mountpoint* [ *fuse options* ]


Description
===========

**ceph-fuse** is a FUSE (File system in USErspace) client for Ceph
distributed file system. It will mount a ceph file system (specified
via the -m option for described by ceph.conf (see below) at the
specific mount point.

The file system can be unmounted with::

        fusermount -u mountpoint

or by sending ``SIGINT`` to the ``ceph-fuse`` process.


Options
=======

Any options not recognized by ceph-fuse will be passed on to libfuse.

.. option:: -d

   Detach from console and daemonize after startup.

.. option:: -c ceph.conf, --conf=ceph.conf

   Use *ceph.conf* configuration file instead of the default
   ``/etc/ceph/ceph.conf`` to determine monitor addresses during startup.

.. option:: -m monaddress[:port]

   Connect to specified monitor (instead of looking through ceph.conf).

.. option:: -r root_directory

   Use root_directory as the mounted root, rather than the full Ceph tree.


Availability
============

**ceph-fuse** is part of Ceph, a massively scalable, open-source, distributed storage system. Please refer to
the Ceph documentation at http://ceph.com/docs for more information.


See also
========

fusermount(8),
:doc:`ceph <ceph>`\(8)
