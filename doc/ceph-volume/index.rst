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
