=======================
 RBD Persistent Cache
=======================

.. index:: Ceph Block Device; Persistent Cache

Shared Read-only Parent Image Cache
===================================

Cloned RBD images(`rbd-snapshots`_) from one parent usually only modify a small
portion of the image. E.g., in a VDI workload, the VMs are using the same cloned
images with only hostname and IP address changed most likely. On the booting
stage, all of these VMs would read the parent content from the RADOS cluster.
If we have a local cache of the parent image, this will help to speed up the
read process on one host as well as to save the south-north network traffic.
RBD shared read-only parent image cache requires expeclictly enabling in
ceph.conf. The ``ceph-immmutable-object-cache`` daemon is responsible for
caching the parent content on local disk, and future reads on those contents
will be serviced from the local cache.

.. note:: RBD shared read-only parent image cache requires the Ceph Nautiaulas release or later.

.. ditaa::  +--------------------------------------------------------+
            |                       QEMU                             |
            +--------------------------------------------------------+
            |             librbd(cloned images)                      |
            +-------------------+-+----------------------------------+
            |      librados     | |    ceph-immutable-object-cache   |
            +-------------------+ +----------------------------------+
            |      OSDs/Mons    | |     Local cached parent image    |
            +-------------------+ +----------------------------------+


Enable RBD Shared Read-only Parent Image Cache
----------------------------------------------

The ``ceph.conf`` file `ceph-conf`_ settings for RBD shared read-only parent
image cache should be set in the ``[client]`` section of your configuration
file.
The settings include:

``rbd shared cache enabled``

:Description: Enable caching for shared read-only cache.
:Type: Boolean
:Required: No
:Default: ``false``

Immutable Object Cache Daemon
=============================

The ``ceph-immutable-object-cache`` daemon is responsible for caching parent
image content on local caching directory.

The ``ceph-immutable-object-cache`` daemon is available within the optional
``ceph-immutable-object-cache`` distribution package.

.. important:: ``ceph-immutable-object-cache`` daemon requires the ability to
   connect RADOS clusters.

``ceph-immutable-object-cache`` daemon should use a unique Ceph user ID.
To `create a Ceph user`_, with ``ceph`` specify the ``auth get-or-create``
command, user name, monitor caps, and OSD caps::

  ceph auth get-or-create client.ceph-immutable-object-cache.{unique id} mon 'profile immutable-object-cache' osd 'profile rbd'

The ``ceph-immutable-object-cache`` daemon can be managed by ``systemd`` by specifying the user
ID as the daemon instance::

  systemctl enable ceph-immutable-object-cache@immutable-object-cache.{unique id}

The ``ceph-immutable-object-cache`` can also be run in foreground by ``ceph-immutable-object-cache`` command::

  ceph-immutable-object-cache -f --log-file={log_path}

.. _rbd-snapshots: ../rbd-snapshot
.. _ceph-conf: ../../rados/configuration/ceph-conf/#running-multiple-clusters
.. _create a Ceph user: ../../rados/operations/user-management#add-a-user

