.. _ceph-volume:

ceph-volume
===========
Deploy OSDs with different device technologies like lvm or physical disks using
pluggable tools (:doc:`lvm/index` itself is treated like a plugin) and trying to
follow a predictable, and robust way of preparing, activating, and starting OSDs.

:ref:`Overview <ceph-volume-overview>` |
:ref:`Plugin Guide <ceph-volume-plugins>` |


**Command Line Subcommands**

There is currently support for ``lvm``, and plain disks (with GPT partitions)
that may have been deployed with ``ceph-disk``.

``zfs`` support is available for running a FreeBSD cluster.

* :ref:`ceph-volume-lvm`
* :ref:`ceph-volume-simple`
* :ref:`ceph-volume-zfs`

**Node inventory**

The :ref:`ceph-volume-inventory` subcommand provides information and metadata
about a nodes physical disk inventory.


Migrating
---------
Starting on Ceph version 13.0.0, ``ceph-disk`` is deprecated. Deprecation
warnings will show up that will link to this page. It is strongly suggested
that users start consuming ``ceph-volume``. There are two paths for migrating:

#. Keep OSDs deployed with ``ceph-disk``: The :ref:`ceph-volume-simple` command
   provides a way to take over the management while disabling ``ceph-disk``
   triggers.
#. Redeploy existing OSDs with ``ceph-volume``: This is covered in depth on
   :ref:`rados-replacing-an-osd`

For details on why ``ceph-disk`` was removed please see the :ref:`Why was
ceph-disk replaced? <ceph-disk-replaced>` section.


New deployments
^^^^^^^^^^^^^^^
For new deployments, :ref:`ceph-volume-lvm` is recommended, it can use any
logical volume as input for data OSDs, or it can setup a minimal/naive logical
volume from a device.

Existing OSDs
^^^^^^^^^^^^^
If the cluster has OSDs that were provisioned with ``ceph-disk``, then
``ceph-volume`` can take over the management of these with
:ref:`ceph-volume-simple`. A scan is done on the data device or OSD directory,
and ``ceph-disk`` is fully disabled. Encryption is fully supported.


.. toctree::
   :hidden:
   :maxdepth: 3
   :caption: Contents:

   intro
   systemd
   inventory
   drive-group
   lvm/index
   lvm/activate
   lvm/batch
   lvm/encryption
   lvm/prepare
   lvm/create
   lvm/scan
   lvm/systemd
   lvm/list
   lvm/zap
   simple/index
   simple/activate
   simple/scan
   simple/systemd
   zfs/index
   zfs/inventory
