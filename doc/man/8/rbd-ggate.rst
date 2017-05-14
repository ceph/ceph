:orphan:

==================================================
 rbd-ggate -- map rbd images via FreeBSD GEOM Gate
==================================================

.. program:: rbd-ggate

Synopsis
========

| **rbd-ggate** [--read-only] [--exclusive] [--device *ggate device*] map *image-spec* | *snap-spec*
| **rbd-ggate** unmap *ggate device*
| **rbd-ggate** list

Description
===========

**rbd-ggate** is a client for RADOS block device (rbd) images. It will
map a rbd image to a ggate (FreeBSD GEOM Gate class) device, allowing
access it as regular local block device.

Commands
========

map
---

Spawn a process responsible for the creation of ggate device and
forwarding I/O requests between the GEOM Gate kernel subsystem and
RADOS.

unmap
-----

Destroy ggate device and terminate the process responsible for it.

list
----

List mapped ggate devices.

Options
=======

.. option:: --device *ggate device*

   Specify ggate device path.

.. option:: --read-only

   Map read-only.

.. option:: --exclusive

   Forbid writes by other clients.

Image and snap specs
====================

| *image-spec* is [*pool-name*]/*image-name*
| *snap-spec*  is [*pool-name*]/*image-name*\ @\ *snap-name*

The default for *pool-name* is "rbd".  If an image name contains a slash
character ('/'), *pool-name* is required.

Availability
============

**rbd-ggate** is part of Ceph, a massively scalable, open-source,
distributed storage system. Please refer to the Ceph documentation at
http://ceph.com/docs for more information.


See also
========

:doc:`rbd <rbd>`\(8)
:doc:`ceph <ceph>`\(8)
