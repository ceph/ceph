.. _ceph-volume-zfs:

``zfs``
=======

Implements the functionality needed to deploy OSDs on FreeBSD using ZFS
pools and zvols as the underlying block storage:
``ceph-volume zfs``

**Command Line Subcommands**

.. toctree::
   :maxdepth: 1

   inventory
   prepare
   activate
   list
   zap

**Internal functionality**

There are other aspects of the ``zfs`` subcommand that are internal and not
exposed to the user directly. These sections explain how the pieces work
together, clarifying the workflows of the tool.

:ref:`zfs <ceph-volume-zfs-api>`

