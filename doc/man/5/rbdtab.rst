:orphan:

===================================================
 rbdtab -- Configuration for RBD device map
===================================================

Synopsis
========

| **/etc/ceph/rbdtab**


Description
===========

The /etc/ceph/rbdtab file describes RBD devices that are set up during system
boot.

Empty lines and lines starting with the "#" character are ignored. Each of the
remaining lines describes one RBD block device. Fields are delimited by white
space.

Each line is in the form::

    device-type image-or-snap-spec generic-options map-options unmap-options

#. device-type is the device type used, as recognized by ``--device-type``
   argument of ``rbd device map`` command. Currently, only ``krbd`` is
   supported.

#. image-or-snap-spec is the image or snapshot specification, in the form of
   <pool-name>/[<namespace>/]<image-name>[@<snap-name>].

#. generic-options is a comma-delimited list of options. This field is optional.
   The supported options are listed below.

#. map-options is passed unchanged to ``--options`` argument of ``rbd device
   map`` command. This field is optional.

#. unmap-options is passed unchanged to ``--options`` argument of ``rbd device
   unmap`` command. This field is optional.

"none" or "-" may be specified as a placeholder for optional fields.

If successful, the generated unit maps the image to a ``/dev/rbdX`` device, at
which point a udev rule is triggered to create a friendly device name symlink,
``/dev/rbd/<image-or-snap-spec>``, pointing to the mapped device.

When writing ``/etc/fstab`` entries for RBD images, the ``_netdev`` mount option
should be used. Otherwise, a dependency loop might be created where the mount
point will be pulled in by local-fs.target, while the service to configure the
network is usually only started after the local file system has been mounted.

At early boot and when the system manager configuration is reloaded, this file
is translated into native systemd units by :doc:`rbdmap-generator <../8/rbdmap-generator>`\(8).


Generic Options
===============

.. option:: noauto, auto

   With ``noauto``, the device will not be added as a dependency for
   rbdmap.target. This means that it will not be automatically mapped on boot,
   unless something else pulls it in. In particular, if the device is used for a
   mount point, it'll be mapped automatically during boot, unless the mount
   point itself is also disabled with ``noauto``.

.. option:: nofail, fail

   With ``nofail``, this device will not be a hard dependency of rbdmap.target.
   It'll still be pulled in and started, but the system will not wait for the
   device to show up, and boot will not fail if this is unsuccessful. Note that
   other units that depend on the RBD device may still fail. In particular, if
   the device is used for a mount point, the mount point itself also needs to
   have the ``nofail`` option, or the boot will fail if the device is not mapped
   successfully.

.. option:: x-systemd.requires=, x-systemd.requires-mounts-for=, x-systemd.before=, \
   x-systemd.after=, x-systemd.wanted-by=, x-systemd.required-by=

   See :manpage:`systemd.unit(5)` for details.

All other options that not start with "x-" are passed to ``rbd device map``
command directly as command-line arguments.. See :manpage:`rbd(8)` for details


Examples
========

Example ``/etc/ceph/rbdtab`` for three RBD images: "bar1" in pool "rbd"; "bar2"
and "bar3" in pool "foopool", "bar3" also in namespace "ns"::

    krbd rbd/bar1
    krbd foopool/bar2     id=admin,noauto
    krbd foopool/ns/bar3  none            lock_on_read,queue_depth=1024  force

When the devices are mapped, the following ``rbd device map`` commands are
called::

    rbd device map rbd/bar1        --device-type=krbd
    rbd device map foopool/bar2    --device-type=krbd --id=admin
    rbd device map foopool/ns/bar3 --device-type=krbd --options=lock_on_read,queue_depth=1024

When the last one is unmapped, the following command is called::

    rbd device unmap foopool/ns/bar3 --device-type=krbd --options=force

If the images had XFS file systems on them, the corresponding ``/etc/fstab``
entries might look like this::

    /dev/rbd/rbd/bar1        /mnt/bar1 xfs _netdev                     0 0
    /dev/rbd/foopool/bar2    /mnt/bar2 xfs _netdev,x-systemd.automount 0 0
    /dev/rbd/foopool/ns/bar3 /mnt/bar3 xfs _netdev                     0 0

For image "bar2", we create an automount in this example. The first access to
the directory /mnt/bar2 will trigger the mount, which in turn will trigger the
RBD device mapping.


See also
========

:doc:`rbd <../8/rbd>`\(8),
:doc:`rbdmap-generator <../8/rbdmap-generator>`\(8)
