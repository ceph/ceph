===============================
 RBD Persistent Read-only Cache
===============================

.. index:: Ceph Block Device; Persistent Read-only Cache

Shared, Read-only Parent Image Cache
====================================

`Cloned RBD images`_ usually modify only a small fraction of the parent
image. For example, in a VDI use-case, VMs are cloned from the same
base image and initially differ only by hostname and IP address. During
booting, all of these VMs read portions of the same parent
image data. If we have a local cache of the parent
image, this speeds up reads on the caching host.  We also achieve
reduction of client-to-cluster network traffic.
RBD cache must be explicitly enabled in
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

Introduction and Generic Settings
---------------------------------

The ``ceph-immutable-object-cache`` daemon is responsible for caching parent
image content within its local caching directory. Using SSDs as the underlying
storage is recommended because doing so provides better performance. 

The key components of the daemon are:

#. **Domain socket based IPC:** The daemon listens on a local domain socket at 
   startup and waits for connections from librbd clients.

#. **LRU based promotion/demotion policy:** The daemon maintains in-memory
   statistics of cache hits for each cache file. It demotes the cold cache
   if capacity reaches the configured threshold.

#. **File-based caching store:** The daemon maintains a simple file-based cache
   store. On promotion, the RADOS objects are fetched from RADOS cluster and
   stored in the local caching directory.

When each cloned RBD image is opened, ``librbd`` tries to connect to the cache
daemon through its Unix domain socket. After ``librbd`` is successfully
connected, it coordinates with the daemon upon every subsequent read. In the
case of an uncached read, the daemon promotes the RADOS object to the local
caching directory and the next read of the object is serviced from the cache.
The daemon maintains simple LRU statistics, which are used to evict cold cache
files when required (for example, when the cache is at capacity and under
pressure). 

Here are some important cache configuration settings:

``immutable_object_cache_sock``

:Description: The path to the domain socket used for communication between
              librbd clients and the ceph-immutable-object-cache daemon.
:Type: String
:Required: No
:Default: ``/var/run/ceph/immutable_object_cache_sock``


``immutable_object_cache_path``

:Description: The immutable object cache data directory.
:Type: String
:Required: No
:Default: ``/tmp/ceph_immutable_object_cache``


``immutable_object_cache_max_size``

:Description: The max size for immutable cache.
:Type: Size
:Required: No
:Default: ``1G``


``immutable_object_cache_watermark``

:Description: The high-water mark for the cache. The value is between (0, 1).
              If the cache size reaches this threshold the daemon will start
              to delete cold cache based on LRU statistics.
:Type: Float
:Required: No
:Default: ``0.9``

The ``ceph-immutable-object-cache`` daemon is available within the optional
``ceph-immutable-object-cache`` distribution package.

.. important:: ``ceph-immutable-object-cache`` daemon requires the ability to
   connect RADOS clusters.

Running the Immutable Object Cache Daemon
-----------------------------------------

``ceph-immutable-object-cache`` daemon should use a unique Ceph user ID.
To `create a Ceph user`_, with ``ceph`` specify the ``auth get-or-create``
command, user name, monitor caps, and OSD caps::

  ceph auth get-or-create client.ceph-immutable-object-cache.{unique id} mon 'allow r' osd 'profile rbd-read-only'

The ``ceph-immutable-object-cache`` daemon can be managed by ``systemd`` by specifying the user
ID as the daemon instance::

  systemctl enable ceph-immutable-object-cache@ceph-immutable-object-cache.{unique id}

The ``ceph-immutable-object-cache`` can also be run in foreground by ``ceph-immutable-object-cache`` command::

  ceph-immutable-object-cache -f --log-file={log_path}

QOS Settings
------------

The immutable object cache supports throttling, controlled by the following settings:

``immutable_object_cache_qos_schedule_tick_min``

:Description: Minimum schedule tick for immutable object cache.
:Type: Milliseconds
:Required: No
:Default: ``50``


``immutable_object_cache_qos_iops_limit``

:Description: The desired immutable object cache IO operations limit per second.
:Type: Unsigned Integer
:Required: No
:Default: ``0``


``immutable_object_cache_qos_iops_burst``

:Description: The desired burst limit of immutable object cache IO operations.
:Type: Unsigned Integer
:Required: No
:Default: ``0``


``immutable_object_cache_qos_iops_burst_seconds``

:Description: The desired burst duration in seconds of immutable object cache IO operations.
:Type: Seconds
:Required: No
:Default: ``1``


``immutable_object_cache_qos_bps_limit``

:Description: The desired immutable object cache IO bytes limit per second.
:Type: Unsigned Integer
:Required: No
:Default: ``0``


``immutable_object_cache_qos_bps_burst``

:Description: The desired burst limit of immutable object cache IO bytes.
:Type: Unsigned Integer
:Required: No
:Default: ``0``


``immutable_object_cache_qos_bps_burst_seconds``

:Description: The desired burst duration in seconds of immutable object cache IO bytes.
:Type: Seconds
:Required: No
:Default: ``1``

.. _Cloned RBD Images: ../rbd-snapshot/#layering
.. _section: ../../rados/configuration/ceph-conf/#configuration-sections
.. _create a Ceph user: ../../rados/operations/user-management#add-a-user

