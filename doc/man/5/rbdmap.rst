:orphan:

====================================================
 rbdmap -- Lagency configuration for RBD devices map
====================================================

Synopsis
========

| **/etc/ceph/rbdmap**

Description
===========

The /etc/ceph/rbdmap file describes RBD devices that are set up during system boot.
This file is deprecated. Please use :doc:`rbdtab <rbdtab>`\(5) for new deployment.

The configuration file format is::

    IMAGESPEC RBDOPTS

where ``IMAGESPEC`` should be specified as ``POOLNAME/IMAGENAME`` (the pool
name, a forward slash, and the image name), or merely ``IMAGENAME``, in which
case the ``POOLNAME`` defaults to "rbd". ``RBDOPTS`` is an optional list of
parameters to be passed to the underlying ``rbd map`` command. These parameters
and their values should be specified as a comma-separated string::

    PARAM1=VAL1,PARAM2=VAL2,...,PARAMN=VALN 

This will cause the script to issue an ``rbd map`` command like the following::

    rbd map POOLNAME/IMAGENAME --PARAM1 VAL1 --PARAM2 VAL2 

(See :doc:`rbd <../8/rbd>`\(8) for a full list of possible options.)
For parameters and values which contain commas or equality signs, a simple
apostrophe can be used to prevent replacing them.

If successful, the ``rbd map`` operation maps the image to a ``/dev/rbdX``
device, at which point a udev rule is triggered to create a friendly device
name symlink, ``/dev/rbd/POOLNAME/IMAGENAME``, pointing to the real mapped
device.

In order for mounting/unmounting to succeed, the friendly device name must
have a corresponding entry in ``/etc/fstab``.

When writing ``/etc/fstab`` entries for RBD images, it's a good idea to specify
the "_netdev" mount option. Otherwise, a dependency loop might be created where the mount
point will be pulled in by local-fs.target, while the service to configure the
network is usually only started after the local file system has been mounted.

Historically, this file is read by a shell script ``rbdmap``, which is now removed.
Currently, at early boot and when the system manager configuration is reloaded,
if ``/etc/ceph/rbdtab`` does not exists, this file is translated into native
systemd units by :doc:`rbdmap-generator <../8/rbdmap-generator>`\(8)
for capability reason.

Examples
========

Example ``/etc/ceph/rbdmap`` for three RBD images called "bar1", "bar2" and "bar3", 
which are in pool "foopool"::

    foopool/bar1    id=admin,keyring=/etc/ceph/ceph.client.admin.keyring
    foopool/bar2    id=admin,keyring=/etc/ceph/ceph.client.admin.keyring
    foopool/bar3    id=admin,keyring=/etc/ceph/ceph.client.admin.keyring,options='lock_on_read,queue_depth=1024'

Each line in the file contains two strings: the image spec and the options to
be passed to ``rbd map``. These two lines get transformed into the following
commands::

    rbd map foopool/bar1 --id admin --keyring /etc/ceph/ceph.client.admin.keyring
    rbd map foopool/bar2 --id admin --keyring /etc/ceph/ceph.client.admin.keyring
    rbd map foopool/bar2 --id admin --keyring /etc/ceph/ceph.client.admin.keyring --options lock_on_read,queue_depth=1024

If the images had XFS file systems on them, the corresponding ``/etc/fstab``
entries might look like this::

    /dev/rbd/foopool/bar1 /mnt/bar1 xfs _netdev 0 0
    /dev/rbd/foopool/bar2 /mnt/bar2 xfs _netdev 0 0
    /dev/rbd/foopool/bar3 /mnt/bar3 xfs _netdev 0 0


Options
=======

None


See also
========

:doc:`rbd <../8/rbd>`\(8),
:doc:`rbdmap-generator <../8/rbdmap-generator>`\(8)
