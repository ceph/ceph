.. _ceph-volume:

ceph-volume
===========
Deploy OSDs with different device technologies like LVM or physical disks using
pluggable tools (:doc:`lvm/index` itself is treated like a plugin) and trying to
follow a predictable and robust way of preparing, activating, and starting OSDs.

:ref:`Overview <ceph-volume-overview>` |
:ref:`Plugin Guide <ceph-volume-plugins>`


**Command Line Subcommands**

There is currently support for ``lvm``, and plain disks (with GPT partitions).

``zfs`` support is available for running a FreeBSD cluster.

* :ref:`ceph-volume-lvm`
* :ref:`ceph-volume-simple`
* :ref:`ceph-volume-zfs`

**Node inventory**

The :ref:`ceph-volume-inventory` subcommand provides information and metadata
about a node's physical disk inventory.


New deployments
---------------
For new deployments, :ref:`ceph-volume-lvm` is recommended. It can use any
logical volume as input for data OSDs, or it can set up a minimal logical
volume from a device.


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
   lvm/migrate
   lvm/newdb
   lvm/newwal
   simple/index
   simple/activate
   simple/scan
   simple/systemd
   zfs/index
   zfs/inventory
