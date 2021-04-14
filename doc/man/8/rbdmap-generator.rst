:orphan:

============================================================================
 rbdmap-generator -- Systemd unit generator for map RBD devices at boot time
============================================================================

.. program:: rbdmap-generator

Synopsis
========

| **/lib/systemd/system-generators/rbdmap-generator**


Description
===========

**rbdmap-generator** is a generator that translates /etc/ceph/rbdmap into native
systemd units early at boot and when configuration of the system manager is
reloaded with ``systemctl daemon-reload``. This will create rbdmap@.service
units as necessary. When these units are activated or deactivated, ``rbd device
map`` or ``rbd device unmap`` command is called as appropriate.

**rbdmap-generator** implements **systemd.generator**\(7)

The /etc/ceph/rbdmap file describes RBD devices that are set up during system
boot.

Empty lines and lines starting with the "#" character are ignored. Each of the
remaining lines describes one RBD block device. Fields are delimited by white
space.

Each line is in the form::

    image-or-snap-spec options

#. image-or-snap-spec is the image or snapshot specification, in the form of
   [<pool-name>/[<namespace>/]]<image-name>[@<snap-name>]. If only <image-name>
   is specified, <pool-name> defaults to "rbd".

#. options is a comma-delimited list of options. This field is optional. The
   supported options are listed below.

If successful, the generated unit maps the image to a ``/dev/rbdX`` device, at
which point a udev rule is triggered to create a friendly device name symlink,
``/dev/rbd/<image-or-snap-spec>``, pointing to the mapped device.

When writing ``/etc/fstab`` entries for RBD images, the ``_netdev`` mount option
should be used. Otherwise, a dependency loop might be created where the mount
point will be pulled in by local-fs.target, while the service to configure the
network is usually only started after the local file system has been mounted.

Supported Options in Config File
--------------------------------

.. option:: conf=, cluster=, id=, name=, mon_host=, keyfile=, keyring=, \
   device-type=, read-only, exclusive, quiesce=, quiesce-hook=

   These options are passed to :program:`rbd`\(8) command directly as
   command-line arguments.

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

   See systemd.unit(5) for details.

All other options that not start with "x-" are passed unchanged to ``rbd device
map`` command as the value of ``--options`` argument. See "Kernel rbd (krbd)
options" in :program:`rbd`\(8) for details


Examples
========

Example ``/etc/ceph/rbdmap`` for three RBD images: "bar1" in pool "rbd"; "bar2"
and "bar3" in pool "foopool"::

    bar1
    foopool/bar2    id=admin,noauto
    foopool/bar3    id=admin,lock_on_read,queue_depth=1024

When the devices are mapped, the following ``rbd device map`` commands are
called::

    rbd device map rbd/bar1
    rbd device map foopool/bar2 --id=admin
    rbd device map foopool/bar3 --id=admin --options=lock_on_read,queue_depth=1024

If the images had XFS file systems on them, the corresponding ``/etc/fstab``
entries might look like this::

    /dev/rbd/rbd/bar1     /mnt/bar1 xfs _netdev                     0 0
    /dev/rbd/foopool/bar2 /mnt/bar2 xfs _netdev,x-systemd.automount 0 0
    /dev/rbd/foopool/bar3 /mnt/bar3 xfs _netdev                     0 0

For image "bar2", we create an automount in this example. The first access to
the directory /mnt/bar2 will trigger the mount, which in turn will trigger the
RBD device mapping.

After creating the images and populating the ``/etc/ceph/rbdmap`` file, making
the images get automatically mapped and mounted at boot is just a matter of
enabling that unit (which may already be done on package installation)::

    systemctl enable rbdmap.target


Options
=======

None


Availability
============

**rbdmap-generator** is part of Ceph, a massively scalable, open-source,
distributed storage system. Please refer to the Ceph documentation at
https://docs.ceph.com for more information.


See also
========

:doc:`rbd <rbd>`\(8),
systemd.generator(7)
