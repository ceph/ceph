:orphan:

=========================================
 rbd-nbd -- map rbd images to nbd device
=========================================

.. program:: rbd-nbd

Synopsis
========

| **rbd-nbd** [Map Option] [General Option] map *image-spec* | *snap-spec*
| **rbd-nbd** unmap *nbd device*
| **rbd-nbd** [List Option] list-mapped

Description
===========

**rbd-nbd** is a client for RADOS block device (rbd) images like rbd kernel module.
It will map a rbd image to a nbd (Network Block Device) device, allowing access it
as regular local block device.

Map Options
===========

.. option:: --read-only

   Map read-only.

.. option:: --nbds_max *limit*

   Override the parameter of NBD kernel module when modprobe, used to
   limit the count of nbd device.

.. option:: --max_part *limit*

    Override for module param nbds_max.

.. option:: --exclusive

   Forbid writes by other clients.

.. option:: --timeout *seconds*

   Override device timeout. Linux kernel will default to a 30 second request timeout.
   Allow the user to optionally specify an alternate timeout.

.. option:: --try-netlink

   Try to use the Linux kernel's netlink inteface. This allows devices to be
   created on demand instead of being limited to ``nbds_max`` devices.

List Options
============

.. option:: --format *plain|json|xml*

   Output format (default: plain)

.. option:: --pretty-format

   Pretty formatting (json and xml)

General Options
===============

.. option:: -c ceph.conf

   Use *ceph.conf* configuration file instead of the default
   ``/etc/ceph/ceph.conf`` to determine monitor addresses during startup.

.. option:: --id/-i *ID*

   Set ID portion of my name.

.. option:: --name/-n *TYPE.ID*

   Set name.

.. option:: --cluster *NAME*

   Set cluster name (default: ceph).

.. option:: --setuser *USER*

   Set uid to user or uid (and gid to user's gid).

.. option:: --setgroup *GROUP*

   Set gid to group or gid.

.. option:: --version

   Show version and quit.

.. option:: -d

   Run in foreground, log to stderr.

.. option:: -f

   Run in foreground, log to usual location.

.. option:: --debug_ms *N*

   Set message debug level (e.g. 1).

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
