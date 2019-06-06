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

.. note:: RBD shared read-only parent image cache requires the Ceph Nautilus release or later.

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

To enable RBD shared read-only parent image cache, the following Ceph settings
need to added in the ``[client]`` section `ceph-conf`_ of your ``ceph.conf``
file.

``rbd parent cache enabled = true``


Immutable Object Cache Daemon
=============================

The ``ceph-immutable-object-cache`` daemon is responsible for caching parent
image content within its local caching directory. For better performance it's
recommended to use SSDs as the underlying storage.

The key components of the daemon are:
- domain socket based simple IPC
- simple LRU policy based promotion/demotion on cache capacity management
- simple file based caching store for RADOS objects

On the opening of each cloned rbd image, librbd will try to connect to the
cache daemon over domain socket based IPC. If it's successfully connected,
librbd will automatically check with the daemon on the following reads.
If there's read that's not cached, the daemon will promote the RADOS object
to local caching directory. So the next read on that object will be serviced
from local file. The daemon also maintains a simple LRU statics so if there's
not enough capacity it will delete some cold cache files.

Some important cache options correspond to the following settings.
``immutable_object_cache_path``
The immutable object cache data directory.

``immutable_object_cache_max_size``
The max size for immutable cache.

``immutable_object_cache_watermark``
The watermark for the cache. If the capacity reaches to this watermark, the
daemon will delete cache files based the LRU statics.

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
.. _ceph-conf: ../../rados/configuration/ceph-conf/#configuration-sections
.. _create a Ceph user: ../../rados/operations/user-management#add-a-user

