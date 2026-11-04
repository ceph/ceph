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
   (or append for EC pools without overwrites).
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
        --op <read|write|write_excl|writesame|zero|write_zeros|delete|
              snap_create|snap_remove|
              rollback|setattr|rmattr|watch|copy_from|hit_set_list|is_dirty|
              undirty|cache_flush|cache_try_flush|cache_evict|append|append_excl|
              set_redirect|unset_redirect|chunk_read|tier_promote|tier_flush|
              set_chunk|tier_evict|mapext> <weight>
        [--op <operation_type> <weight> ...]
        [--pool <pool_name>]
        [--max-ops <op_count>]
        [--objects <object_count>]
        [--max-in-flight <max_concurrent>]
        [--size <max_size_bytes>]
        [--min-stride-size <bytes>]
        [--max-stride-size <bytes>]
        [--max-seconds <seconds>]
        [--ec-pool]   (deprecated, ignored)
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

Pool Support Detection
~~~~~~~~~~~~~~~~~~~~~~

At startup the tool queries the target pool (``ceph osd pool get``) to
find out whether it is replicated or erasure coded and which features it
supports. Operations and features the pool cannot support are removed
with a ``Warning:`` message and the test continues with the rest; the
tool does not exit because of an unsupported operation. Replicated pools
are never restricted.

For erasure-coded pools:

- ``write``, ``write_excl``, ``writesame``, ``zero`` and ``write_zeros``
  require ``allow_ec_overwrites``.
- ``mapext`` requires ``allow_ec_optimizations`` and
  ``preserve_allocation``.
- Sparse reads (part of ``read``) require ``allow_ec_optimizations`` and
  ``preserve_allocation``. Without them sparse reads are disabled as if
  ``--no-sparse`` had been given, and normal reads continue.
- Omap is disabled when the pool does not report ``supports_omap``.

``preserve_allocation`` can be read with
``ceph osd pool get <pool> preserve_allocation``. It can only be set with
``ceph osd pool set <pool> set_pool_flags <n> --yes-i-really-mean-it``.

If no operations remain after filtering, the tool prints an error and
exits with status 1.

``--ec-pool``
    Deprecated and ignored. The pool type is detected automatically, so
    the flag is no longer needed. Passing it prints a warning.

``--no-omap``
    Disable omap operations. Set automatically if the pool does not
    support omap (auto-detected at startup).

``--no-sparse``
    Disable sparse reads (use full reads only). Set automatically if an
    erasure-coded pool lacks ``allow_ec_optimizations`` or
    ``preserve_allocation``.

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
   :widths: 20 25 55

   * - Name
     - Requirement on EC pools
     - Description
   * - ``read``
     - none
     - Read and verify object data, xattrs, and omap against the model.
   * - ``write``
     - ``allow_ec_overwrites``
     - Random-offset partial write.
   * - ``write_excl``
     - ``allow_ec_overwrites``
     - Random-offset partial write that asserts the object already exists
       (``assert_exists()``) as part of the transaction.
   * - ``writesame``
     - ``allow_ec_overwrites``
     - Write same data pattern across an extent.
   * - ``zero``
     - ``allow_ec_overwrites``
     - Zero a random extent of an object.
   * - ``write_zeros``
     - ``allow_ec_overwrites``
     - Write an extent of explicit zero data.
   * - ``mapext``
     - ``allow_ec_optimizations`` and ``preserve_allocation``
     - Query the allocated extents of an object and verify them against
       the model.
   * - ``delete``
     - none
     - Delete an object.
   * - ``snap_create``
     - none
     - Create a snapshot (quiesces in-flight ops first).
   * - ``snap_remove``
     - none
     - Remove a snapshot.
   * - ``rollback``
     - none
     - Roll back an object to a previous snapshot.
   * - ``setattr``
     - none
     - Set random xattrs (and omap if supported).
   * - ``rmattr``
     - none
     - Remove random xattrs (and omap if supported).
   * - ``watch``
     - none
     - Establish a watch, self-notify, wait for callback.
   * - ``copy_from``
     - none
     - Server-side copy between objects in the pool.
   * - ``hit_set_list``
     - none
     - List HitSet entries.
   * - ``is_dirty``
     - none
     - Check object dirty state (cache tier).
   * - ``undirty``
     - none
     - Mark object clean (cache tier).
   * - ``cache_flush``
     - none
     - Flush object from cache tier (blocking).
   * - ``cache_try_flush``
     - none
     - Try to flush object from cache tier (non-blocking).
   * - ``cache_evict``
     - none
     - Evict object from cache tier.
   * - ``append``
     - none
     - Append data to an object.
   * - ``append_excl``
     - none
     - Append data that asserts the object already exists.
   * - ``set_redirect``
     - none
     - Set redirect manifest to low-tier pool.
   * - ``unset_redirect``
     - none
     - Remove redirect manifest.
   * - ``chunk_read``
     - none
     - Read and verify a chunk from a manifest object.
   * - ``tier_promote``
     - none
     - Promote object from lower tier.
   * - ``tier_flush``
     - none
     - Flush object to backing tier.
   * - ``set_chunk``
     - none
     - Set chunk manifest (requires ``--enable_dedup``).
   * - ``tier_evict``
     - none
     - Evict object to backing tier.

Operations are never restricted on replicated pools.

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

EC pool with snapshots (the pool type is detected automatically; ops the
pool cannot support, such as ``write`` without ``allow_ec_overwrites``,
are skipped with a warning)::

    ceph_test_rados \
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

Operations or features that the pool does not support produce a
``Warning:`` message and are skipped; they do not cause a non-zero exit.

Exit status **1** indicates a startup failure, such as invalid arguments
or no operations remaining to run once unsupported ones have been removed.

Adding a New Operation
----------------------

1. Add an enumerator to ``TestOpType`` in ``RadosModel.h``, before
   ``NUM_TEST_OP_TYPES``.
2. Add a row to ``op_types[]`` in ``TestRados.cc`` with the pool support
   the operation requires on erasure-coded pools (``SUPPORT_NONE`` if
   none). The build fails if a ``TestOpType`` has no row.
3. Add a ``case`` to ``WeightedTestGenerator::gen_op``. There is no
   ``default:`` label, so the compiler warns about a missing case.
4. Update ``usage()`` and the operation table in this document.

To add a new kind of pool support, add a value to ``PoolSupport`` and a
row to ``pool_support_infos[]`` in ``RadosPoolSupport.h``. Features that
are not operations (sparse reads, omap) are listed in ``pool_features[]``
in ``TestRados.cc`` and are handled with the same check.

Source Files
------------

- ``src/test/osd/TestRados.cc`` — CLI parsing and main loop
- ``src/test/osd/RadosPoolSupport.h`` — Pool support detection and
  op/feature gating
- ``src/test/osd/RadosModel.h`` — Test context and operation classes
- ``src/test/osd/Object.h`` — Content generation and verification model
- ``src/test/osd/TestOpStat.h`` — Operation statistics
- ``qa/tasks/rados.py`` — Teuthology task wrapper
