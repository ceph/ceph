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

* :ref:`ceph-volume-lvm`
* :ref:`ceph-volume-simple`


Migrating
---------
Starting on Ceph version 12.2.2, ``ceph-disk`` is deprecated. Deprecation
warnings will show up that will link to this page. It is strongly suggested
that users start consuming ``ceph-volume``.

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
and ``ceph-disk`` is fully disabled.

Encrypted OSDs
^^^^^^^^^^^^^^
If using encryption with OSDs, there is currently no support in ``ceph-volume``
for this scenario (although support for this is coming soon). In this case, it
is OK to continue to use ``ceph-disk`` until ``ceph-volume`` fully supports it.
This page will be updated when that happens.

.. toctree::
   :hidden:
   :maxdepth: 3
   :caption: Contents:

   intro
   systemd
   lvm/index
   lvm/activate
   lvm/prepare
   lvm/scan
   lvm/systemd
   lvm/list
   lvm/zap
   simple/index
   simple/activate
   simple/scan
   simple/systemd
