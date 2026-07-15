ceph_test_rados — Model-Based RADOS Stress Test
================================================

``ceph_test_rados`` is a model-based integration test that verifies the
data correctness of the RADOS layer under stress. It maintains an in-memory
model of expected object data and metadata, and compares it against the
actual object data returned by RADOS after every read, detecting data
corruption, snapshot inconsistencies, and attribute mismatches.

.. note::

   This is **not** a performance benchmark. For throughput and latency
   measurement, use ``rados bench``. ``ceph_test_rados`` is a
   *correctness verifier*.

How It Works
------------

1. **Initialization**: Creates ``--objects`` initial objects via write
   (or append for EC pools).
2. **Stress loop**: Generates a randomized stream of up to ``--max-ops``
   operations, each selected by weighted probability from the
   ``--op`` arguments.
3. **Verification**: Every read dispatches 3 pipelined reads and
   compares data, xattrs, and omap entries against the in-memory model.
4. **Completion**: Prints the error count and per-operation-type
   statistics to stderr.

Architecture
~~~~~~~~~~~~

The tool is built from several components:

- ``TestRados.cc`` — CLI parsing, ``main()``, and the
  ``WeightedTestGenerator`` which selects operations by weight.
- ``RadosModel.h`` — The ``RadosTestContext`` (in-memory model) and all
  26 ``TestOp`` subclasses (``ReadOp``, ``WriteOp``, ``SnapCreateOp``,
  etc.).
- ``Object.h`` / ``Object.cc`` — Content generators
  (``VarLenGenerator``, ``AppendGenerator``) and the ``ObjectDesc``
  model that tracks layered object contents across snapshots.
- ``TestOpStat.h`` — Per-operation-type latency statistics collector.

Synopsis
--------

::

    ceph_test_rados
        --op <read|write|write_excl|writesame|delete|snap_create|snap_remove|
              rollback|setattr|rmattr|watch|copy_from|hit_set_list|is_dirty|
              undirty|cache_flush|cache_try_flush|cache_evict|append|append_excl|
              set_redirect|unset_redirect|chunk_read|tier_promote|tier_flush|
              set_chunk|tier_evict> <weight>
        [--op <operation_type> <weight> ...]
        [--pool <pool_name>]
        [--max-ops <op_count>]
        [--objects <object_count>]
        [--max-in-flight <max_concurrent>]
        [--size <max_size_bytes>]
        [--min-stride-size <bytes>]
        [--max-stride-size <bytes>]
        [--max-seconds <seconds>]
        [--ec-pool]
        [--no-omap]
        [--no-sparse]
        [--pool-snaps]
        [--balance-reads]
        [--localize-reads]
        [--offlen_randomization_ratio <0-100>]
        [--write-fadvise-dontneed]
        [--max-attr-len <bytes>]
        [--set_redirect]
        [--set_chunk]
        [--low_tier_pool <pool_name>]
        [--enable_dedup]
        [--dedup_chunk_algo <fastcdc|fixcdc>]
        [--dedup_chunk_size <bytes>]
        [--timestamps]

At least one ``--op`` with a positive weight is required.

Core Parameters
---------------

``--pool <name>``
    Target RADOS pool (must already exist). Default: ``rbd``.

``--max-ops <n>``
    Maximum number of operations to execute (including initial object
    writes). Default: ``1000``.

``--objects <n>``
    Number of distinct objects to create and test against. Must satisfy
    ``max_in_flight * 2 <= objects``. Default: ``50``.

``--max-in-flight <n>``
    Maximum concurrent asynchronous operations. Default: ``16``.

``--max-seconds <n>``
    Wall-clock time limit in seconds. ``0`` means unlimited (run until
    ``--max-ops`` is exhausted). Default: ``0``.

Object Geometry
---------------

``--size <n>``
    Maximum object size in bytes. Actual sizes are randomized within
    approximately ``[size/2, size]``. Default: ``4000000`` (~3.8 MiB).

``--min-stride-size <n>``
    Minimum write stride in bytes. Must be < ``--max-stride-size``
    and <= ``--size``. Default: ``size / 10``.

``--max-stride-size <n>``
    Maximum write stride in bytes. Must be > ``--min-stride-size``
    and <= ``--size``. Default: ``size / 5``.

Pool Type and Behavior
----------------------

``--ec-pool``
    Indicates that the target is an erasure-coded pool **that does not support overwrites**.
    **Must appear before any** ``--op`` **arguments.**
    
    .. note::

       This is largely a legacy parameter. When Ceph originally introduced
       EC pools, they did not support partial overwrites or sparse reads. Today,
       if an EC pool supports overwrites (e.g., via BlueStore), you should *not*
       use this flag, so that ``ceph_test_rados`` can test partial overwrites.
       In the Teuthology QA suite, setting ``erasure_code_use_overwrites: true``
       prevents the test runner from passing this flag.

    Using this flag has the following effects:
    
    1. Implicitly sets ``--no-sparse``.
    2. Initial object creation writes use ``append`` mode instead of ``write``.
    3. Overwrite operations (``write``, ``write_excl``, ``writesame``) are
       disallowed and will cause startup validation to fail.

``--no-omap``
    Disable omap operations. Automatically set if the pool does not
    support omap (auto-detected at startup).

``--no-sparse``
    Disable sparse reads (use full reads only). Automatically set when
    ``--ec-pool`` is used.

``--pool-snaps``
    Use pool-level snapshots instead of self-managed snapshots.

Read Routing
------------

``--balance-reads``
    Set ``LIBRADOS_OPERATION_BALANCE_READS`` on read operations,
    allowing reads from any replica.

``--localize-reads``
    Set ``LIBRADOS_OPERATION_LOCALIZE_READS`` on read operations,
    preferring the closest replica.

``--offlen_randomization_ratio <n>``
    Percentage chance (0–100) that a read uses a randomized offset
    instead of reading from offset 0. Default: ``50``.

Write Behavior
--------------

``--write-fadvise-dontneed``
    Set the ``write_fadvise_dontneed`` flag on the pool, advising the
    OSD backend not to cache written data.

``--max-attr-len <n>``
    Maximum generated xattr length in bytes. Default: ``20000``.

Manifest and Tiering
--------------------

``--set_redirect``
    Enable redirect manifest testing. Requires ``--low_tier_pool``.

``--set_chunk``
    Enable chunk-based manifest testing. Requires ``--low_tier_pool``.

``--low_tier_pool <name>``
    Low-tier pool for redirect/chunk/dedup operations. Must be a
    different pool from ``--pool`` to avoid a known race condition.
    Required when ``--set_redirect`` or ``--set_chunk`` is set.

Deduplication
-------------

``--enable_dedup``
    Enable deduplication testing. Requires ``--dedup_chunk_algo`` and
    ``--dedup_chunk_size``. Configures the pool with SHA-256
    fingerprinting and the specified chunking algorithm.

``--dedup_chunk_algo <algorithm>``
    Chunking algorithm: ``fastcdc`` or ``fixcdc``.

``--dedup_chunk_size <size>``
    Chunk size for content-defined chunking (e.g., ``131072``).

Output
------

``--timestamps``
    Prefix each output line with a coarse timestamp.

Operation Types
---------------

Operations are specified via ``--op <name> <weight>``. Weights are
relative: an operation with weight 100 is twice as likely as one with
weight 50.

.. list-table::
   :header-rows: 1
   :widths: 20 10 70

   * - Name
     - Valid with --ec-pool
     - Description
   * - ``read``
     - Yes
     - Read and verify object data, xattrs, and omap against the model.
   * - ``write``
     - No
     - Random-offset partial write.
   * - ``write_excl``
     - No
     - Random-offset partial write that asserts the object already exists
       (``assert_exists()``) as part of the transaction.
   * - ``writesame``
     - No
     - Write same data pattern across an extent.
   * - ``delete``
     - Yes
     - Delete an object.
   * - ``snap_create``
     - Yes
     - Create a snapshot (quiesces in-flight ops first).
   * - ``snap_remove``
     - Yes
     - Remove a snapshot.
   * - ``rollback``
     - Yes
     - Roll back an object to a previous snapshot.
   * - ``setattr``
     - Yes
     - Set random xattrs (and omap if supported).
   * - ``rmattr``
     - Yes
     - Remove random xattrs (and omap if supported).
   * - ``watch``
     - Yes
     - Establish a watch, self-notify, wait for callback.
   * - ``copy_from``
     - Yes
     - Server-side copy between objects in the pool.
   * - ``hit_set_list``
     - Yes
     - List HitSet entries.
   * - ``is_dirty``
     - Yes
     - Check object dirty state (cache tier).
   * - ``undirty``
     - Yes
     - Mark object clean (cache tier).
   * - ``cache_flush``
     - Yes
     - Flush object from cache tier (blocking).
   * - ``cache_try_flush``
     - Yes
     - Try to flush object from cache tier (non-blocking).
   * - ``cache_evict``
     - Yes
     - Evict object from cache tier.
   * - ``append``
     - Yes
     - Append data to an object.
   * - ``append_excl``
     - Yes
     - Append data that asserts the object already exists.
   * - ``set_redirect``
     - Yes
     - Set redirect manifest to low-tier pool.
   * - ``unset_redirect``
     - Yes
     - Remove redirect manifest.
   * - ``chunk_read``
     - Yes
     - Read and verify a chunk from a manifest object.
   * - ``tier_promote``
     - Yes
     - Promote object from lower tier.
   * - ``tier_flush``
     - Yes
     - Flush object to backing tier.
   * - ``set_chunk``
     - Yes
     - Set chunk manifest (requires ``--enable_dedup``).
   * - ``tier_evict``
     - Yes
     - Evict object to backing tier.

Environment Variables
---------------------

``CEPH_CLIENT_ID``
    Client ID for the librados connection. If unset, connects as the
    default client.

Standard Ceph environment variables (``CEPH_CONF``, ``CEPH_KEYRING``,
etc.) are respected.

Teuthology Integration
----------------------

The tool is typically invoked via the ``rados`` Teuthology task defined
in ``qa/tasks/rados.py``. The task creates pools, translates YAML
configuration into CLI arguments, and manages the process lifecycle.

Example YAML configuration::

    tasks:
    - rados:
        clients: [client.0]
        ops: 400000
        max_seconds: 600
        objects: 1024
        size: 16384
        op_weights:
          read: 100
          write: 100
          delete: 50
          snap_create: 50
          snap_remove: 50
          rollback: 50

Workload examples are in ``qa/suites/rados/thrash*/workloads/``.

.. note::

   The Teuthology wrapper automatically splits ``write`` and ``append``
   weights into regular and ``_excl`` halves. This does not happen at
   the CLI level: specify both variants explicitly when invoking the
   binary directly.

Examples
--------

Basic replicated pool test::

    ceph_test_rados \
      --pool testpool \
      --max-ops 10000 \
      --objects 500 \
      --max-in-flight 16 \
      --size 4000000 \
      --op read 100 \
      --op write 100 \
      --op delete 10

EC pool (without allow_ec_overwrites) with snapshots::

    ceph_test_rados \
      --ec-pool \
      --pool my-ec-pool \
      --max-ops 4000 \
      --objects 50 \
      --pool-snaps \
      --op read 100 \
      --op append 100 \
      --op delete 50 \
      --op snap_create 50 \
      --op snap_remove 50 \
      --op rollback 50

Deduplication test::

    ceph_test_rados \
      --pool testpool \
      --low_tier_pool low_tier \
      --set_chunk \
      --enable_dedup \
      --dedup_chunk_algo fastcdc \
      --dedup_chunk_size 131072 \
      --max-ops 1500 \
      --objects 50 \
      --op read 100 \
      --op write 50 \
      --op set_chunk 30 \
      --op tier_promote 10

Exit Status
-----------

The tool will immediately panic (via ``ceph_abort()``) and dump core
if any data verification errors (e.g., mismatching object content,
corrupt metadata) are detected during reads.

If no bugs are hit and the execution time/op count is exhausted, the
tool will exit cleanly with status **0**.

Exit status **1** indicates a startup validation failure (such as
incompatible arguments).

Source Files
------------

- ``src/test/osd/TestRados.cc`` — CLI parsing and main loop
- ``src/test/osd/RadosModel.h`` — Test context and operation classes
- ``src/test/osd/Object.h`` — Content generation and verification model
- ``src/test/osd/TestOpStat.h`` — Operation statistics
- ``qa/tasks/rados.py`` — Teuthology task wrapper
