=======================
 RBD Persistent Cache
=======================

.. index:: Ceph Block Device; Persistent Cache

Shared, Read-only Parent Image Cache
====================================

`Cloned RBD images`_ from a parent usually only modify a small portion of
the image. For example, in a VDI workload, the VMs are cloned from the same
base image and initially only differ by hostname and IP address. During the
booting stage, all of these VMs would re-read portions of duplicate parent
image data from the RADOS cluster. If we have a local cache of the parent
image, this will help to speed up the read process on one host, as well as
to save the client to cluster network traffic.
RBD shared read-only parent image cache requires explicitly enabling in
``ceph.conf``. The ``ceph-immutable-object-cache`` daemon is responsible for
caching the parent content on the local disk, and future reads on that data
will be serviced from the local cache.

.. note:: RBD shared read-only parent image cache requires the Ceph Nautilus release or later.

.. ditaa::

            +--------------------------------------------------------+
            |                         QEMU                           |
            +--------------------------------------------------------+
            |                librbd (cloned images)                  |
            +-------------------+-+----------------------------------+
            |      librados     | |  ceph--immutable--object--cache  |
            +-------------------+ +----------------------------------+
            |      OSDs/Mons    | |     local cached parent image    |
            +-------------------+ +----------------------------------+


Enable RBD Shared Read-only Parent Image Cache
----------------------------------------------

To enable RBD shared read-only parent image cache, the following Ceph settings
need to added in the ``[client]`` `section`_ of your ``ceph.conf`` file::

        rbd parent cache enabled = true
        rbd plugins = parent_cache

Immutable Object Cache Daemon
=============================

The ``ceph-immutable-object-cache`` daemon is responsible for caching parent
image content within its local caching directory. For better performance it's
recommended to use SSDs as the underlying storage.

The key components of the daemon are:

#. **Domain socket based IPC:** The daemon will listen on a local domain
   socket on start up and wait for connections from librbd clients.

#. **LRU based promotion/demotion policy:** The daemon will maintain
   in-memory statistics of cache-hits on each cache file. It will demote the
   cold cache if capacity reaches to the configured threshold.

#. **File-based caching store:** The daemon will maintain a simple file
   based cache store. On promotion the RADOS objects will be fetched from
   RADOS cluster and stored in the local caching directory.

On opening each cloned rbd image, ``librbd`` will try to connect to the
cache daemon over its domain socket. If it's successfully connected,
``librbd`` will automatically check with the daemon on the subsequent reads.
If there's a read that's not cached, the daemon will promote the RADOS object
to local caching directory, so the next read on that object will be serviced
from local file. The daemon also maintains simple LRU statistics so if there's
not enough capacity it will delete some cold cache files.

Here are some important cache options correspond to the following settings:

- ``immutable_object_cache_sock`` The path to the domain socket used for
  communication between librbd clients and the ceph-immutable-object-cache
  daemon.

- ``immutable_object_cache_path`` The immutable object cache data directory.

- ``immutable_object_cache_max_size`` The max size for immutable cache.

- ``immutable_object_cache_watermark`` The watermark for the cache. If the
  capacity reaches to this watermark, the daemon will delete cold cache based
  on the LRU statistics.

The ``ceph-immutable-object-cache`` daemon is available within the optional
``ceph-immutable-object-cache`` distribution package.

.. important:: ``ceph-immutable-object-cache`` daemon requires the ability to
   connect RADOS clusters.

``ceph-immutable-object-cache`` daemon should use a unique Ceph user ID.
To `create a Ceph user`_, with ``ceph`` specify the ``auth get-or-create``
command, user name, monitor caps, and OSD caps::

  ceph auth get-or-create client.ceph-immutable-object-cache.{unique id} mon 'allow r' osd 'profile rbd-read-only'

The ``ceph-immutable-object-cache`` daemon can be managed by ``systemd`` by specifying the user
ID as the daemon instance::

  systemctl enable ceph-immutable-object-cache@immutable-object-cache.{unique id}

The ``ceph-immutable-object-cache`` can also be run in foreground by ``ceph-immutable-object-cache`` command::

  ceph-immutable-object-cache -f --log-file={log_path}

.. _Cloned RBD Images: ../rbd-snapshot/#layering
.. _section: ../../rados/configuration/ceph-conf/#configuration-sections
.. _create a Ceph user: ../../rados/operations/user-management#add-a-user

