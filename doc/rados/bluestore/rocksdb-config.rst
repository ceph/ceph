============================
 RocksDB Config Reference
============================

.. note:: As of the Tentacle release, two Ceph services use RocksDB: Monitors and OSDs.
   This document focuses on OSD's RocksDB.

.. index:: bluestore; rocksdb caching

RocksDB caching
===============

RocksDB caching is based on preserving parts of ``.sst`` files in block cache.
For more details, see the `Block-Cache wiki <https://github.com/facebook/rocksdb/wiki/Block-Cache>`_.
Ceph implements its own flavor of block cache.
See the `source code <https://github.com/ceph/ceph/tree/main/src/kv/rocksdb_cache>`_ for more details.
This custom implementation brings together RocksDB block cache, BlueStore metadata cache,
and BlueStore data cache to compete for available memory.

Cache sharding
--------------

As the default RocksDB block cache, Ceph block cache is sharded.
Sharding is controlled by configuration. The purpose of sharding is to
streamline multi-threaded access.

.. confval:: rocksdb_cache_shard_bits


Perf counters
-------------

Ceph RocksDB cache operations are tracked by performance counters.
In the default configuration BlueStore creates two block caches:
``O`` for onodes, and ``default`` for everything else.
The sections created in performance counters are named ``rocksdb-cache-O`` and
``rocksdb-cache-default``.

.. prompt:: bash #

  ceph tell osd.0 perf dump rocksdb-cache-O

.. code-block:: 

  "rocksdb-cache-O": {
    "capacity": 134217728,
    "usage": 134182832,
    "pinned": 0,
    "elems": 24502,
    "inserts": 25806978,
    "lookups": 150436987,
    "hits": 124629911,
    "misses": 25807076
  }

Values ``capacity``, ``usage``, ``pinned`` and ``elems`` reflect the current state of the cache.
Values ``inserts``, ``lookups``, ``hits`` and ``misses`` are increased on relevant event.

.. index:: rocksdb; perf counters

Admin commands
--------------

Performance counters show a brief summation, but each cache shard has its own stats.
To list RocksDB onode block cache details for each shard, run an admin socket command:

.. prompt:: bash #

  ceph tell osd.0 rocksdb show cache O

.. code-block::

    shard  capacity     usage   pinned   elems  inserts  lookups     hits   misses
        0  13631488  11076400        0    2099   136987   822679   685923   136756
        1  13631488  11549712        0    2043   133359   571500   438383   133117
        2  13631488  11060608        0    2232   135076   908468   773313   135155
        3  13631488  11166896        0    2269   134006   427070   293147   133923
        4  13631488  11117984        0    2297   133367   700242   567318   132924
        5  13631488  11306672        0    2155   137501  1130135   991810   138325
        6  13631488  11506512        0    2353   134515   662792   528514   134278
        7  13631488  11093856        0    2316   135348   718971   583421   135550
        8  13631488  11660624        0    2424   137363  1092043   954248   137795
        9  13631488  10962000        0    2561   131982   431702   300467   131235
       10  13631488  11379392        0    1916   134543   477118   342854   134264
       11  13631488  11294272        0    2555   134508   512393   378337   134056
       12  13631488  11277136        0    2079   137312  1131571   993692   137879
       13  13631488  10887776        0    2543   134001   567073   432903   134170
       14  13631488  10986528        0    2394   133288   584452   451018   133434
       15  13631488  11954464        0    2456   134615   708285   573374   134911

Counters that support clearing can be reset to zero by running a command:

.. prompt:: bash #

   ceph tell osd.0 rocksdb reset cache O

.. index:: bluestore; rocksdb; admin commands

Optimum shard count
-------------------

In most cases 16 shards as defined by ``rocksdb_cache_shard_bits=4`` is a good choice.
Large OSDs can easily accommodate millions of objects and thus having millions of keys
to encode onode metadata. While the number of keys is not directly a problem,
it causes RocksDB to create very large index blocks.
During leveled compaction RocksDB merges two levels. Two index blocks are
needed at the same time.
A problem appears if both index blocks belong to the same shard
and their total size is more than capacity of the shard.
Both index blocks cannot fit in the shard simultaneously and access to index block
causes the other block to be evicted from the cache.
The constant thrashing persists until the current RocksDB compaction step finishes.

Thrashing detection
*******************

.. prompt:: bash #

  ceph tell osd.0 rocksdb show cache O

.. code-block::

    shard  capacity     usage   pinned   elems  inserts  lookups     hits   misses
      ...
        2  13631488  11060608        0    2232   135076   908468   773313   135155
        3  13631488   8166896        0       1   134006   427070   293147   133923
        4  13631488  11117984        0    2297   133367   700242   567318   132924
      ...

It is likely that shard 3 is doing constant eviction. To verify, reset counters to zero
and observe the relation between ``misses`` and ``hits``.
Also, the perf counter for ``bluefs.read_bytes`` will be increasing very fast as RocksDB is
reading the same index blocks over and over again.

Mitigation
**********

More like a workaround. Reduce ``rocksdb_cache_shard_bits``.
This will have a slight negative effect on the baseline RocksDB performance,
as fewer shards means more opportunity for lock contention.
