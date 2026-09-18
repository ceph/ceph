.. _arch-cache-tiering:

Cache Tiering (Deprecated)
==========================

.. meta::
   :description: What cache tiering is. Cache tiering is deprecated.
   :ceph-page-type: concept
   :ceph-applies-to: squid, tentacle
   :ceph-reviewed: 2026-09
   :ceph-owner: rados

A cache tier provides Ceph Clients with better I/O performance for a subset of
the data stored in a backing storage tier. Cache tiering involves creating a
pool of relatively fast/expensive storage devices (e.g., solid state drives)
configured to act as a cache tier, and a backing pool of either erasure-coded
or relatively slower/cheaper devices configured to act as an economical storage
tier.

.. note:: Cache tiering is deprecated since Reef and is not recommended for
   new clusters. The Ceph objecter handles where to place the objects and the tiering
agent determines when to flush objects from the cache to the backing storage
tier. So the cache tier and the backing storage tier are completely transparent
to Ceph clients.


.. ditaa::

           +-------------+
           | Ceph Client |
           +------+------+
                  ^
     Tiering is   |
    Transparent   |              Faster I/O
        to Ceph   |           +---------------+
     Client Ops   |           |               |
                  |    +----->+   Cache Tier  |
                  |    |      |               |
                  |    |      +-----+---+-----+
                  |    |            |   ^
                  v    v            |   |   Active Data in Cache Tier
           +------+----+--+         |   |
           |   Objecter   |         |   |
           +-----------+--+         |   |
                       ^            |   |   Inactive Data in Storage Tier
                       |            v   |
                       |      +-----+---+-----+
                       |      |               |
                       +----->|  Storage Tier |
                              |               |
                              +---------------+
                                 Slower I/O

Additional Resources
====================

- :doc:`Cache tiering operations </rados/operations/cache-tiering>`
- :ref:`Pools <rados_pools>`
