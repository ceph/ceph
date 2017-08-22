.. _ceph-volume:

ceph-volume
===========
Deploy OSDs with different device technologies like lvm or physical disks using
pluggable tools (:doc:`lvm/index` itself is treated like a plugin). It tries to
follow the workflow of ``ceph-disk`` for deploying OSDs, with a predictable,
and robust way of preparing, activating, and starting OSDs.

:ref:`Overview <ceph-volume-overview>` |
:ref:`Plugin Guide <ceph-volume-plugins>` |


**Command Line Subcommands**
Although currently there is support for ``lvm``, the plan is to support other
technologies, including plain disks.

* :ref:`ceph-volume-lvm`

.. toctree::
   :hidden:
   :maxdepth: 3
   :caption: Contents:

   intro
   lvm/index
   lvm/activate
   lvm/prepare
   lvm/scan
   lvm/systemd
