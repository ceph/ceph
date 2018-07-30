.. _ceph-volume-lvm:

``lvm``
=======
Implements the functionality needed to deploy OSDs from the ``lvm`` subcommand:
``ceph-volume lvm``

**Command Line Subcommands**

* :ref:`ceph-volume-lvm-prepare`

* :ref:`ceph-volume-lvm-activate`

* :ref:`ceph-volume-lvm-create`

* :ref:`ceph-volume-lvm-list`

.. not yet implemented
.. * :ref:`ceph-volume-lvm-scan`

**Internal functionality**

There are other aspects of the ``lvm`` subcommand that are internal and not
exposed to the user, these sections explain how these pieces work together,
clarifying the workflows of the tool.

:ref:`Systemd Units <ceph-volume-lvm-systemd>` |
:ref:`lvm <ceph-volume-lvm-api>`
