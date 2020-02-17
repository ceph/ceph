.. _ceph-volume-zfs:

``zfs``
=======
Implements the functionality needed to deploy OSDs from the ``zfs`` subcommand:
``ceph-volume zfs``

The current implementation only works for ZFS on FreeBSD

**Command Line Subcommands**

* :ref:`ceph-volume-zfs-inventory`

.. not yet implemented
.. * :ref:`ceph-volume-zfs-prepare`

.. * :ref:`ceph-volume-zfs-activate`

.. * :ref:`ceph-volume-zfs-create`

.. * :ref:`ceph-volume-zfs-list`

.. * :ref:`ceph-volume-zfs-scan`

**Internal functionality**

There are other aspects of the ``zfs`` subcommand that are internal and not
exposed to the user, these sections explain how these pieces work together,
clarifying the workflows of the tool.

:ref:`zfs <ceph-volume-zfs-api>`
