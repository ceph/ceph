:orphan:

=========================================
 rbd-nbd -- map rbd images to nbd device
=========================================

.. program:: rbd-nbd

Synopsis
========

| **rbd-nbd** [-c conf] [--nbds_max *limit*] [--read-only] [--device *nbd device*] map *image-spec* | *snap-spec*
| **rbd-nbd** unmap *nbd device*
| **rbd-nbd** list-mapped

Description
===========

**rbd-nbd** is a client for RADOS block device (rbd) images like rbd kernel module.
It will map a rbd image to a nbd (Network Block Device) device, allowing access it
as regular local block device.

Options
=======

.. option:: -c ceph.conf

   Use *ceph.conf* configuration file instead of the default
   ``/etc/ceph/ceph.conf`` to determine monitor addresses during startup.

.. option:: --nbds_max *limit*

   Override the parameter of NBD kernel module when modprobe, used to
   limit the count of nbd device.

Image and snap specs
====================

| *image-spec* is [*pool-name*]/*image-name*
| *snap-spec*  is [*pool-name*]/*image-name*\ @\ *snap-name*

The default for *pool-name* is "rbd".  If an image name contains a slash
character ('/'), *pool-name* is required.

Availability
============

**rbd-nbd** is part of Ceph, a massively scalable, open-source, distributed storage system. Please refer to
the Ceph documentation at http://ceph.com/docs for more information.


See also
========

:doc:`rbd <rbd>`\(8)
