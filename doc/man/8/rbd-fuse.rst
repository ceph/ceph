:orphan:

=======================================
 rbd-fuse -- expose rbd images as files
=======================================

.. program:: rbd-fuse

Synopsis
========

| **rbd-fuse** [ -p pool ] [-c conffile] *mountpoint* [ *fuse options* ]


Description
===========

**rbd-fuse** is a FUSE (File system in USErspace) client for RADOS
block device (rbd) images.  Given a pool containing rbd images,
it will mount a userspace filesystem allowing access to those images
as regular files at **mountpoint**.

The file system can be unmounted with::

        fusermount -u mountpoint

or by sending ``SIGINT`` to the ``rbd-fuse`` process.


Options
=======

Any options not recognized by rbd-fuse will be passed on to libfuse.

.. option:: -c ceph.conf

   Use *ceph.conf* configuration file instead of the default
   ``/etc/ceph/ceph.conf`` to determine monitor addresses during startup.

.. option:: -p pool

   Use *pool* as the pool to search for rbd images.  Default is ``rbd``.


Availability
============

**rbd-fuse** is part of Ceph, a massively scalable, open-source, distributed storage system. Please refer to
the Ceph documentation at http://ceph.com/docs for more information.


See also
========

fusermount(8),
:doc:`rbd <rbd>`\(8)
