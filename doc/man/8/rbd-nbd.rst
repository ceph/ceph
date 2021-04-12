:orphan:

=========================================
 rbd-nbd -- map rbd images to nbd device
=========================================

.. program:: rbd-nbd

Synopsis
========

| **rbd-nbd** [-c conf] [--read-only] [--device *nbd device*] [--nbds_max *limit*] [--max_part *limit*] [--exclusive] [--encryption-format *format*] [--encryption-passphrase-file *passphrase-file*] [--io-timeout *seconds*] [--reattach-timeout *seconds*] map *image-spec* | *snap-spec*
| **rbd-nbd** unmap *nbd device* | *image-spec* | *snap-spec*
| **rbd-nbd** list-mapped
| **rbd-nbd** attach --device *nbd device* *image-spec* | *snap-spec*
| **rbd-nbd** detach *nbd device* | *image-spec* | *snap-spec*
| **rbd-nbd** restore [--attach-list *<nbd-device,image-spec,snap-spec,...>* | --saveconfig-file *file*]

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

.. option:: --read-only

   Map read-only.

.. option:: --nbds_max *limit*

   Override the parameter nbds_max of NBD kernel module when modprobe, used to
   limit the count of nbd device.

.. option:: --max_part *limit*

    Override for module param max_part.

.. option:: --exclusive

   Forbid writes by other clients.

.. option:: --encryption-format

   Image encryption format.
   Possible values: *luks1*, *luks2*

.. option:: --encryption-passphrase-file

   Path of file containing a passphrase for unlocking image encryption.

.. option:: --io-timeout *seconds*

   Override device timeout. Linux kernel will default to a 30 second request timeout.
   Allow the user to optionally specify an alternate timeout.

.. option:: --reattach-timeout *seconds*

   Specify timeout for the kernel to wait for a new rbd-nbd process is
   attached after the old process is detached. The default is 30
   second.

.. option:: --attach-list *<nbd-device,image-spec,snap-spec,...>*

   Should be used with the restore command, it specifies the comma-separated
   list of nbd-devices and/or image-specs and/or snap-specs which needs to be
   re-attached. If this option is not specified, the restore command will
   attempt to re-attach all the rbd-nbd devices available in the saveconfig
   file.

.. option:: --saveconfig-file *file*

   Path of the JSON file that will contain data about the rbd-nbd devices.
   The default file path is /etc/ceph/saveconfig.json

Image and snap specs
====================

| *image-spec* is [*pool-name*]/*image-name*
| *snap-spec*  is [*pool-name*]/*image-name*\ @\ *snap-name*

The default for *pool-name* is "rbd".  If an image name contains a slash
character ('/'), *pool-name* is required.

Availability
============

**rbd-nbd** is part of Ceph, a massively scalable, open-source, distributed storage system. Please refer to
the Ceph documentation at https://docs.ceph.com/ for more information.


See also
========

:doc:`rbd <rbd>`\(8)
