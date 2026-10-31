=====================================================================
EC Direct Reads with Client-Side Decode - Design
=====================================================================

:Author: Alex Ainscow
:Date: 2026-10-16
:Status: Draft
:Builds on: :doc:`direct_reads`, :doc:`client_support`

.. contents::
   :local:
   :depth: 2

1. Approach & Planning
======================

Goal
----

Clients reading from Fast EC pools (pools with ``allow_ec_optimizations``)
fetch object data directly from the OSDs that store the shards, and
reconstruct missing data themselves when a shard is unavailable. The primary
OSD only serves a client read when the read needs the primary to decide
something (ordering, recovery, conflicts, complex operations).

Problem Statement
-----------------

Today an EC client read goes to the primary OSD of the placement group (PG).
The primary reads the shards it needs from the other OSDs in the PG, decodes
them if a shard is missing, and returns the result. Every byte of the read
crosses the cluster network twice and passes through the primary's CPU and
messenger.

The EC direct reads feature (:doc:`direct_reads`), delivered for Umbrella,
lets the client (librados ``Objecter``) split a read into one sub-read per
data shard and send them straight to the shard OSDs. This only works while
every data shard the read touches is healthy. The client falls back to the
primary when:

- any OSD holding a needed data shard is down or missing from the acting set
  (degraded PG);
- a shard OSD returns ``-EAGAIN`` (object missing on the shard, unstable
  write, interval change);
- the read is mixed with operations the split framework does not support;
- the client did not ask for balanced reads (``BALANCE_READS``).

The degraded case causes the most harm. When an OSD fails, every PG that had
a shard on that OSD sends all of its reads back through its primary, and the
primary has to decode them. For the main target workload, RGW serving large
objects from an EC data pool, this moves a large share of the cluster's read
bandwidth onto a subset of OSDs at a time when the cluster is already doing
recovery. Client latency and throughput drop exactly when the cluster is
least able to absorb it.

RGW already uses direct reads when the operator sets
``rados_replica_read_policy=balance`` for the RGW daemons, so RGW needs no
changes for this design.

There is also a gap in the healthy path. In the current implementation, a
non-``EAGAIN`` error from *any* shard (for example ``-EIO`` from a single
medium error) is returned to the application instead of being retried via
the primary. See ``SplitOp::assemble_rc()``.

Why Is This Important?
----------------------

- RGW on Fast EC is the main capacity tier for object storage. Large-object
  GET bandwidth is the key metric, and the primary-mediated path caps it at
  about half the available network bandwidth in the PG.
- Degraded-mode performance is a common point in competitive evaluations. A
  single failed OSD should not cause a cluster-wide GET throughput drop.
- Reads that stay off the primary leave the primary's CPU and network free
  for writes and recovery, which shortens the degraded window.

Scope
-----

In Scope (Requirements)
~~~~~~~~~~~~~~~~~~~~~~~

#. **Fast EC only.** Applies only to pools with ``allow_ec_optimizations``
   and the ``split_reads`` pool flag. Legacy EC pools and replicated pools
   are unchanged.
#. **Client-side decode.** When a needed data shard is unavailable in the
   client's OSDMap, the client reads enough other shards (data and coding)
   and decodes the missing data itself, using the same EC plugin and the
   same decode code as the OSD.
#. **EAGAIN falls back to the primary.** Any condition where a shard OSD
   cannot safely serve the read (object missing on the shard, unstable
   write, interval change during the read, torn-read version mismatch) is
   resolved by re-issuing the whole operation to the primary, without split
   flags, exactly once.
#. **Ordered reads go via the primary.** Operations flagged
   ``RWORDERED`` (``librados::OPERATION_ORDER_READS_WRITES``) are never split.
#. **Complex operations go via the primary.** Only ``READ`` and
   ``SPARSE_READ`` are split. ``STAT``, ``GETXATTR``, ``GETXATTRS``,
   ``CMPXATTR``, ``CHECKSUM`` and read-only ``CALL`` may accompany a split
   read and are executed on the primary in the same operation (as today).
   Any other operation causes the whole request to go to the primary.
#. **Shared code with Fast EC.** Read planning, extent arithmetic and decode
   come from code shared with the OSD's Fast EC read pipeline. The client
   does not have its own EC implementation.
#. **RGW coverage.** With ``rados_replica_read_policy=balance``, all RGW
   data read paths (see `RGW Read Path Coverage`_) use direct reads on Fast
   EC data pools, including in degraded mode, without RGW code changes.
#. **Upgrade.** Client-side decode is only enabled once every OSD runs a
   release that supports it. Older clients keep working (healthy direct reads
   or primary reads). Newer clients against older OSDs behave as today.
#. **Supportability.** Perf counters and logs show how many reads were
   direct, how many were decoded in the client, and why each fallback
   happened. Client decode can be disabled per pool and per client.
#. **Failure scenarios.** OSD down, OSD flapping, OSD unresponsive before
   being marked down, shard object missing (recovery), medium error on a
   shard, EC plugin not installed on the client, interval change during the
   read, and a full pool.

Not in Scope
~~~~~~~~~~~~

- Kernel clients (krbd, kcephfs). They continue to read via the primary.
- Crimson OSD.
- Legacy EC pools and replicated pools.
- Client-side decode for ``SPARSE_READ``. A sparse read that would need
  decode goes to the primary. This avoids clashing with the allocation-
  tracking work for RBD encryption on EC pools.
- Plugins with sub-chunks (CLAY). These pools get healthy direct reads, but
  degraded reads go to the primary.
- Direct reads of omap (EC omap is served by the primary).
- Recovering from shard medium errors (``-EIO``) in the client. These go to
  the primary so the error is recorded and repaired (see
  `Error Handling`_).
- Hedged/redundant reads to hide slow shards. This is a possible follow-on.
- Locality-aware shard selection for stretch clusters. This is a follow-on
  that this design enables (see `Future Work`_).

Dependencies
------------

- EC direct reads / ``SplitOp`` framework (Umbrella) and the recent
  ``Objecter`` fixes for forced-OSD reads on interval change.
- Fast EC read pipeline (``ECCommon::ReadPipeline``, ``ECUtil``).
- Build/packaging: EC plugins must be available to the RGW process.
- QE / Performance: RGW large-object degraded-mode benchmarks.

Customers & Stakeholders
------------------------

- **Driving:** Product management for object storage. Customers running RGW
  on Fast EC with large objects (backup, media, AI data lakes).
- **Impacted squads:** RADOS (Objecter, OSD, Mon), Build (packaging).
- **Contributing teams:**

  - RGW: review `RGW Read Path Coverage`_ and the head-object behaviour.
    No RGW code changes.
  - QE: teuthology suites for RGW on Fast EC with OSD thrashing.
  - Performance: healthy and degraded RGW GET benchmarks before merge to
    main.
  - Build: confirm EC plugin availability in RGW packages and containers.
  - Publications: config options, pool flag, release notes.

Every stakeholder above must review the Approach section.

Customer Facing Story / High-Level User Stories
-----------------------------------------------

**Story 1 (MVP): Direct large-object GETs from RGW.**
As an object-storage operator with a Fast EC data pool, I want RGW GETs to
read object data directly from the shard OSDs, so that GET throughput scales
with the number of OSDs rather than being limited by primary OSDs.

*Acceptance criteria:*

- With ``rados_replica_read_policy=balance`` set for RGW on a healthy
  cluster, at least 99% of data bytes returned by RGW GETs of objects of
  64 MiB or larger are served by direct reads (``objecter`` counters on the
  RGW process).
- Read data is byte-identical to the data written (checksum verified by the
  benchmark tool).
- Ordered and complex operations from RGW keep working unchanged
  (RGW s3-tests pass).

**Story 2 (MVP): Degraded reads without the primary.**
As an operator, I want reads to keep being served directly from the
surviving shards when an OSD is down, with decoding done by the client, so
that a single OSD failure does not cause a GET throughput drop or a load spike
on primary OSDs.

*Acceptance criteria:*

- With one OSD down (marked down, PGs active+degraded), at least 99% of RGW
  GET data bytes for affected PGs are served by direct reads with client
  decode. The fallback counters account for the rest.
- Degraded-mode large-object GET throughput is at least 90% of healthy
  throughput on the same cluster (exact threshold to be confirmed with the
  Performance team).
- Data integrity: ``ceph_test_rados_io_sequence`` and ``ceph_test_rados``
  with thrashing report no miscompares.

**Story 3 (MVP): Safe fallback.**
As an application developer, I want reads that need ordering, recovery or
primary-only decisions to behave exactly as before, so that enabling this
feature cannot change results for my application.

*Acceptance criteria:*

- ``RWORDERED`` reads are never sent as direct reads (test asserts no
  ``EC_DIRECT_READ`` sub-ops).
- Injected ``-EAGAIN``, version mismatch, interval change and ``-EIO`` each
  cause exactly one re-issue to the primary, and return correct data.

**Story 4 (MVP): Observability and control.**
As a support engineer, I want counters and logs that show direct, decoded
and fallback reads (with the reason for each fallback), and switches to turn
client decode off per pool or per client, so that I can diagnose and contain
problems in the field.

*Acceptance criteria:*

- New ``objecter`` perf counters (see `Debug Strategy & Performance
  Counters`_) are visible via ``ceph daemon <rgw> perf dump``.
- ``ceph osd pool set <pool> ec_client_decode false`` stops client decode
  within one OSDMap epoch on all clients. Degraded reads then use the
  primary.

**Story 5 (Stretch): Reference shard off the primary.**
As an operator, I want the small metadata part of a direct read (version
check, size, xattrs) to be served by any primary-capable shard, so that
single-chunk reads never need a message to the primary.

Success Criteria
----------------

- Stories 1–4 acceptance criteria met.
- No regression in RGW s3-tests, ``rados`` and ``rgw`` teuthology suites
  run on Fast EC data pools.
- RGW small-object GET (4 KiB–1 MiB) p99 latency with
  ``rados_replica_read_policy=balance`` is no worse than 10% above the
  primary-mediated baseline. If it is, see `Open Questions`_.
- Primary OSD network egress for read workloads drops to about the primary's
  own shard share (about ``1/k`` of the PG's read bandwidth) in both healthy
  and degraded modes.

Impact on Dependent Products
----------------------------

Downstream products that deploy RGW (for example ODF/Fusion object
services) need to ship the EC plugins in the RGW container image. The
standard Ceph container already includes them. To use direct reads, they set
``rados_replica_read_policy=balance`` in the RGW configuration. Otherwise,
there is no impact.

High-Level Solution
-------------------

The client already knows the PG's acting set and the pool's EC profile from
the OSDMap. That is enough to work out which OSD holds which part of the
object and, if one is missing, which other shards to read to rebuild it.

.. ditaa::

   Healthy (today, unchanged in behaviour)

           +-> OSD shard 0 (primary: data + version/size) --+
           |                                               |
   client -+-> OSD shard 1 (data) -------------------------+--> client
           |                                               |   destripe
           +-> OSD shard 2 (data) -------------------------+

   Degraded: shard 1 is down (new)

           +-> OSD shard 0 (primary: data + version/size) --+
           |                                               |
   client -+-> OSD shard 2 (data) -------------------------+--> client
           |                                               |   decode
           +-> OSD shard 3 (coding) -----------------------+   + destripe

   Fallback (EAGAIN, mismatch, interval change, EIO, unsupported)

   client --> primary --> (primary's own reads/decode) --> client

Key concepts:

- **Direct read:** a client read split into one sub-read per shard OSD.
- **Reference sub-read:** the sub-read sent to the primary. It carries the
  object version map used to detect torn reads, any metadata operations
  (stat, xattrs), and the object size when a decode is needed.
- **Client decode:** reconstructing missing data shards in the client from
  the shards that were read. It uses the pool's EC plugin and the same
  ``ECUtil`` code as the OSD.
- **Fallback:** re-sending the original operation, unchanged, to the
  primary.

What is permanent and what is first-release only:

- Permanent: ordered reads and complex operations go via the primary;
  ``EAGAIN``-class conditions go via the primary; Fast EC only.
- First release: the reference sub-read always goes to the primary (Story 5
  lifts this); ``-EIO`` from a shard goes to the primary; sparse reads and
  sub-chunk plugins do not decode in the client.

Alternatives considered:

#. **Keep degraded reads on the primary (status quo).** Rejected. This
   concentrates load on primaries exactly when the cluster is degraded,
   which is the main problem being solved.
#. **Delegate decode to a surviving shard OSD.** The client sends the
   degraded read to any surviving shard OSD, and that OSD runs the read
   pipeline. Rejected. It still needs two network hops, and a non-primary
   OSD does not have the peering and missing-set knowledge that the read
   pipeline relies on. Making it safe would be a large OSD change.
#. **A separate client-side EC implementation.** Rejected. It would
   duplicate stripe arithmetic and decode logic that already exist and have
   been hardened in Fast EC (requirement 6). It would also risk the client
   and OSD reconstructing different data.
#. **Decode in the client on shard ``-EIO``.** Rejected for the first
   release. It would hide medium errors from the OSD, which would then not
   be repaired. These errors are rare, so the cost of going via the primary
   is small.

User Experience
---------------

No user action is needed in the normal case, beyond the existing opt-in to
balanced reads.

#. **Upgrade.** After all OSDs run the V release and the operator runs
   ``ceph osd require-osd-release v``, the monitor sets the
   ``ec_client_decode`` flag on every Fast EC pool, alongside the existing
   ``split_reads`` flag.
#. **RGW** opts in with the existing client option, for example
   ``ceph config set client.rgw rados_replica_read_policy balance``. RGW
   sets no read flags of its own and sends no ordered reads, so every RGW
   data read then uses direct reads. No RGW code changes are needed.
#. **Other librados clients** opt in as today: per operation with
   ``OPERATION_BALANCE_READS``, or globally with
   ``rados_replica_read_policy=balance``.
#. **Failure.** When an OSD goes down, nothing is visible to the user apart
   from the usual health warnings. Reads continue at near-healthy throughput.
#. **Diagnosis.** ``ceph daemon <client> perf dump objecter`` shows direct,
   decoded and fallback counts. ``ceph osd pool ls detail`` shows the pool
   flags.
#. **Containment.** ``ceph osd pool set <pool> ec_client_decode false``
   (whole cluster) or ``rados_ec_client_decode=false`` (one client) turns
   decode off. Setting ``rados_replica_read_policy`` back to ``default``
   returns a client, including RGW, to primary-only reads.

Terminology: *direct read*, *client decode*, *reference sub-read*,
*fallback* (defined above). There are no GUI changes.

Limitations
-----------

- **What:** Kernel clients do not use direct reads or client decode.
  **Why:** the kernel cannot load userspace EC plugins and the
  ``SplitOp`` logic would need to be reimplemented in the kernel.
  **Permanent or first-release?** First release. There is no follow-on epic
  planned.
- **What:** Ordered (``RWORDERED``) reads always go via the primary.
  **Why:** only the primary can order reads against in-flight writes.
  **Permanent.**
- **What:** Operations other than ``READ``/``SPARSE_READ`` plus the listed
  metadata operations go via the primary. **Why:** they need the primary's
  object context or are not safe on a shard. **Permanent.**
- **What:** ``EAGAIN``-class conditions go via the primary. **Why:** only the
  primary can wait for recovery or resolve unstable writes. **Permanent.**
- **What:** Sparse reads that need decode go via the primary. **Why:**
  decoded data loses allocation information. **First-release.** This could
  be revisited after the allocation-tracking work lands.
- **What:** Shard ``-EIO`` goes via the primary. **Why:** the error must be
  recorded and repaired. **First-release.** This could be revisited if
  shard error reporting from clients is added.
- **What:** The reference sub-read always targets the primary. **Why:**
  simplest correct source of versions. **First-release.** Story 5 lifts
  this.
- **What:** Plugins with sub-chunks (CLAY) do not decode in the client.
  **Why:** extra complexity and test cost for a rarely used plugin.
  **First-release.** No follow-on is planned.
- **What:** Client decode needs the EC plugin libraries on the client host.
  **Why:** decode must use the pool's own plugin. **Permanent.** If the
  plugin is missing, degraded reads go via the primary.

Delivery Target
---------------

- **Target release:** V (the release after Umbrella).
- **Risk level:** High. This touches the client I/O path, OSD read
  handling and data integrity in degraded mode.
- **Delivery mechanism:** Upstream first.

Milestones
----------

#. Design approved.
#. Shared EC read library builds into ``libceph-common``, and the OSD read
   pipeline uses the extracted planner with no behaviour change (merged to
   main).
#. OSD shard-space and coding-shard direct reads merged behind the pool flag.
#. Client decode prototype passes ``ceph_test_rados_io_sequence`` with forced
   decode.
#. Error-handling corrections and counters merged.
#. Performance baseline (healthy and degraded) established.
#. Feature complete. Teuthology suites green.

Standard Stories
----------------

**DST material:** Required. This covers how to read the new counters, how to
tell a client decode from a primary decode in logs, and how to turn the
feature off. A story will be raised.

**Documentation:** Required. This covers the new pool flag, client config
options, guidance to set ``rados_replica_read_policy=balance`` for RGW, and
release notes.

Risks
-----

- **Risk:** Small-object RGW GETs with ``rados_replica_read_policy=balance``
  regress, because the head-object read (prefetch of up to
  ``rgw_max_chunk_size``) is split into ``k`` sub-reads even when the object
  is only a few KiB. This is existing direct-read behaviour that this design
  does not change.
  **Likelihood / Impact:** Medium / Medium.
  **Mitigation:** Benchmark early (milestone 6). If it regresses, see
  `Open Questions`_.
- **Risk:** Extracting the planner from ``ECCommon`` changes OSD behaviour.
  **Likelihood / Impact:** Low / High.
  **Mitigation:** Pure refactor with an equivalence unit test that runs both
  old and new planners over randomised inputs before the old one is
  deleted.
- **Risk:** Client CPU for decode competes with RGW.
  **Likelihood / Impact:** Medium / Low.
  **Mitigation:** Decode only runs in degraded mode. ISA-L is the default
  plugin. Counters expose decode bytes and latency. Per-client off switch.
- **Risk:** Packaging. Debian ``radosgw`` depends on ``ceph-common``, not
  ``ceph-base`` (where the plugins are), so plugins may be missing on
  bare-metal Debian/Ubuntu installs.
  **Likelihood / Impact:** High / Low (falls back to the primary).
  **Mitigation:** Packaging story to move ``libec_*.so`` into a package that
  ``radosgw`` depends on. Log a warning once.
- **Risk:** In-flight Objecter work on forced-OSD reads (this branch
  family) touches the same code.
  **Likelihood / Impact:** High / Medium.
  **Mitigation:** Land those fixes first. This epic rebases on them.
- **Risk:** Hidden assumptions in the existing split-read code about
  ro-offset (logical) sub-reads.
  **Likelihood / Impact:** Medium / Medium.
  **Mitigation:** The shard-space protocol is only used when the new pool
  flag is set. The legacy path is kept for one release.

2. Externally-Visible Changes
=============================

What Can Go Wrong
-----------------

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - Failure
     - How the user finds out
     - What the user can do
   * - EC plugin library cannot be loaded on the client
     - Client log (level 0, once per pool per process):
       ``unable to load erasure code plugin '<plugin>' for pool <pool>:
       <error>; degraded reads will use the primary OSD``. Counter
       ``ec_dr_fallback_no_codec`` increases.
     - Install the package that provides ``libec_<plugin>.so`` (or check
       ``erasure_code_dir``). No data impact.
   * - Client decode returns an error (should not happen)
     - Client log at level 0 with pool, PG, object and plan. Counter
       ``ec_dr_fallback_decode_error``. The read is retried via the primary,
       so the application sees no error.
     - Collect client logs at ``debug_objecter=20`` and raise a defect.
       Optionally set ``ec_client_decode false`` on the pool.
   * - High fallback rate (recovery, write/read conflicts)
     - ``ec_dr_fallback_eagain`` / ``ec_dr_fallback_version`` counters climb.
       OSD ``replica_read_redirect_missing`` / ``_conflict`` climb.
     - Expected during recovery. If it persists on a healthy cluster, check
       for read/write contention on the same objects.
   * - Data miscompare (bug)
     - Application-level checksum failure (for example, RGW ETag mismatch
       on multipart verification). The debug option
       ``objecter_debug_ec_decode_verify`` turns this into a client assert
       with a full dump.
     - Disable with ``ec_client_decode false`` and raise a critical defect.
   * - Shard OSD unresponsive but not yet marked down
     - Read latency rises to the OSD heartbeat grace, as it does today on the
       primary path.
     - None needed. Once the OSD is marked down the read falls back, and new
       reads decode around it.

Command Line Interface (CLI)
----------------------------

One new pool flag is settable through the existing ``osd pool set`` and
``osd pool get`` commands. There are no new commands.

``ceph osd pool set <pool> ec_client_decode true|false``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Purpose:** Allow or forbid clients to reconstruct missing EC shards
themselves when doing direct reads on this pool.

**Syntax:**
``ceph osd pool set <pool-name> ec_client_decode (true | false | 1 | 0)``

**Parameters:**

- ``pool-name``: existing pool name (string).
- value: boolean. There is no default; the value is required.

**Description (for documentation):** When ``ec_client_decode`` is enabled
on a Fast EC pool, clients that perform direct reads can rebuild data from
the remaining shards when an OSD is unavailable, instead of sending the read
to the primary OSD. The monitor sets this flag automatically on Fast EC pools
once ``require_osd_release`` is V or later. Disabling it sends all degraded
reads via the primary OSD. Healthy direct reads are not affected.

**Notes (not for documentation):** Takes effect when clients receive the new
OSDMap. In-flight operations are unaffected. Pools created after
``require_osd_release`` reaches V get the flag at creation. If an operator
clears the flag, the monitor does not set it again; it only sets the flag
automatically when ``require_osd_release`` is raised.

**Output:** None. ``ceph osd pool get <pool> ec_client_decode`` prints
``ec_client_decode: true|false``. ``ceph osd pool ls detail`` lists
``ec_client_decode`` among the pool flags when set.

**CLI error messages:**

.. list-table::
   :header-rows: 1
   :widths: 40 30 30

   * - Message
     - Explanation
     - User action
   * - ``Error EINVAL: pool '<pool>' is not an erasure-coded pool with
       allow_ec_optimizations enabled``
     - The flag only applies to Fast EC pools.
     - None, or enable ``allow_ec_optimizations`` first.
   * - ``Error EPERM: all OSDs must be running the V release or later;
       run 'ceph osd require-osd-release v' first``
     - Older OSDs cannot serve shard-space or coding-shard direct reads.
     - Finish the upgrade, then retry.
   * - ``Error EINVAL: pool '<pool>' is a crimson pool; client erasure-code
       decode is not supported``
     - Crimson does not support direct reads.
     - None.
   * - ``Error EINVAL: expecting value 'true', 'false', '0', or '1'``
     - Bad value.
     - Retry with a valid value.

**Python bindings, Dashboard REST API, Dashboard:** No changes. Pool flags
are already displayed generically.

Cluster Log & Health Checks
---------------------------

- **Cluster log:** No new entries. Client decode happens in client
  processes, which do not write to the cluster log. Shard ``-EIO`` continues
  to be logged by the primary when it handles the fallback read.
- **Health checks:** None. Degraded PGs are already reported by existing
  checks.

Other API Interfaces
--------------------

- **librados / neorados:** No API changes. Behaviour for callers that set
  ``OPERATION_BALANCE_READS`` (or have ``rados_replica_read_policy=balance``)
  on Fast EC pools: degraded reads may now be served by direct reads. The
  semantics are those already documented for direct reads: a non-ordered
  read may be reordered relative to an uncompleted write, and never returns
  a torn result.
- **RADOS wire protocol:** New op flag ``CEPH_OSD_OP_FLAG_EC_SHARD_EXTENT``
  on ``READ``/``SPARSE_READ`` ops inside an ``EC_DIRECT_READ`` ``MOSDOp``
  (see `OSD Changes`_). It is only sent when the pool has
  ``ec_client_decode``, which guarantees the OSDs understand it.
- **S3/Swift:** No API change and no RGW code change.
- **Grafana:** No new dashboards. The new ``objecter`` counters on RGW are
  exported through the existing RGW perf counter path. Adding a
  "direct vs fallback reads" panel to the RGW dashboard is a stretch item.

3. Design
=========

External Changes / Interfaces
-----------------------------

- ``SplitOp`` sub-read errors are reclassified (see `Error Handling`_). A
  shard ``-EIO`` now falls back to the primary instead of being returned to
  the application. This is a bug fix to existing behaviour.

Detailed Design
---------------

Design overview
~~~~~~~~~~~~~~~

.. ditaa::

   +--------------------------- libceph-common ----------------------------+
   |                                                                       |
   |  Objecter ----------------> SplitOp::create()                         |
   |    |  codec cache              |                                      |
   |    v                           v                                      |
   |  ECCodecCache  <------  ECSplitOp  ---- uses ---->  ECUtil (shared)   |
   |  (pool -> ec_impl,             |                    - stripe_info_t   |
   |   stripe_info_t)               |                    - shard_extent_*  |
   |                                |                    - plan_read()     |
   |  ErasureCodePluginRegistry <---+                    - complete_read() |
   +---------------------------------------------------------------------- +
                                    | MOSDOp (EC_DIRECT_READ, FORCE_OSD,
                                    |  FAIL_ON_EAGAIN, op flag
                                    |  EC_SHARD_EXTENT)
                                    v
   +------------------------------- ceph-osd -----------------------------+
   |  PrimaryLogPG::do_op  -> do_read / do_sparse_read                    |
   |      -> ECBackend::objects_read_local (shard extents, data or coding)|
   |  ECCommon::ReadPipeline -> ECUtil::plan_read / complete_read (shared)|
   +-----------------------------------------------------------------------+

Components and responsibilities:

``ECUtil`` shared EC read library (new home, existing code)
    Stripe geometry, shard extent sets and maps, decode, zero padding,
    destriping (``get_ro_buffer``), plus two functions extracted from
    ``ECCommon::ReadPipeline``: ``plan_read()`` and ``complete_read()``.
    Built as an object library that is linked into both ``libceph-common``
    and the OSD.

``ECCodecCache`` (new, in ``Objecter``)
    For each pool id, lazily creates and caches the plugin instance
    (``ErasureCodeInterfaceRef``) and ``stripe_info_t`` from the pool's EC
    profile in the OSDMap. Load failures are cached too (negative cache).

``ECSplitOp`` (extended)
    Decides between a healthy plan, a degraded plan or fallback. Builds
    sub-reads in shard space. On completion, checks versions, assembles,
    and decodes if needed, or falls back.

OSD (small changes)
    Serves direct reads whose extents are in shard space, on any shard
    including coding shards.

Monitor (small change)
    New pool flag ``ec_client_decode``, set automatically and settable by
    the operator.

Why the design meets each requirement
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Requirement
     - How it is met
   * - Client decode (R2, Story 2)
     - ``ECSplitOp`` builds a degraded plan with ``plan_read()`` over the
       shards available in the OSDMap, and decodes with ``complete_read()``.
   * - EAGAIN to the primary (R3, Story 3)
     - Unchanged ``FAIL_ON_EAGAIN`` + ``op_post_split_op_complete()`` path,
       extended to the new classes in `Error Handling`_. At most one
       fallback per operation.
   * - Ordered reads via the primary (R4)
     - ``get_read_flags()`` already strips balance flags from ``RWORDERED``
       ops. ``validate_flags()`` adds an explicit ``RWORDERED`` reject, and
       the OSD already rejects ``EC_DIRECT_READ`` + ``rwordered`` with
       ``-EINVAL``.
   * - Complex operations via the primary (R5)
     - ``validate_operations()`` allow-list is unchanged. Allowed metadata
       ops are placed only on the reference (primary) sub-read.
   * - Common code with Fast EC (R6)
     - All extent arithmetic, planning and decode come from ``ECUtil``,
       which the OSD also uses. The client-only ``ECStripeIterator`` destripe
       is retired on the new path.
   * - Fast EC only (R1)
     - Requires ``split_reads`` (only set for Fast EC) and
       ``ec_client_decode`` (only allowed for Fast EC).
   * - RGW (R7, Story 1)
     - See `RGW Read Path Coverage`_.

Shared EC read library
~~~~~~~~~~~~~~~~~~~~~~

The OSD's read pipeline already splits planning from execution:

- ``get_min_want_to_read_shards()`` turns logical (ro) ranges into wanted
  shard extents.
- ``get_min_avail_to_read_shards()`` takes the set of available shards
  (from peering) and asks the plugin ``minimum_to_decode()`` which shards to
  read. It then computes per-shard read extents and ``zeros_for_decode``
  using the object size.
- ``ClientReadCompleter::finish_single_request()`` pads zeros, calls
  ``shard_extent_map_t::decode()`` and builds the result with
  ``get_ro_buffer()``.

Only the computation of the *available* set depends on OSD state
(``get_all_avail_shards()`` reads the missing sets). Everything else depends
only on ``stripe_info_t``, the plugin and the object size. The design
extracts that part:

.. code-block:: cpp

   namespace ECUtil {
   struct read_plan_t {
     shard_extent_set_t want;             // shard extents the caller wants
     shard_extent_set_t reads;            // shard extents to fetch
     shard_extent_set_t zeros_for_decode; // extents known to be zero
     shard_id_set decode_shards;          // shards to rebuild (empty if healthy)
   };

   // object_size == std::nullopt: size unknown at plan time (client case);
   // reads are not trimmed and zeros are derived at completion.
   int plan_read(const stripe_info_t &sinfo,
                 const ErasureCodeInterfaceRef &ec_impl,
                 const std::list<ec_align_t> &to_read,
                 const shard_id_set &available,
                 std::optional<uint64_t> object_size,
                 read_plan_t *plan);

   // Pads, decodes if needed and returns one bufferlist per to_read entry,
   // truncated to object_size.
   int complete_read(const stripe_info_t &sinfo,
                     const ErasureCodeInterfaceRef &ec_impl,
                     const read_plan_t &plan,
                     uint64_t object_size,
                     shard_extent_map_t &&buffers,
                     const std::list<ec_align_t> &to_read,
                     std::vector<bufferlist> *out);
   }

``ECCommon::ReadPipeline`` keeps its OSD-specific wrapper. It computes the
available set and the ``pg_shard_t`` mapping, calls ``plan_read()``, and
converts the plan into ``read_request_t``. The wrapper still handles
sub-chunks (CLAY) and redundant reads for the OSD. The client rejects
sub-chunk plugins before planning.

Build changes:

- ``ECUtil.cc`` and the new ``ECReadPlan.cc`` move into an object library
  linked into ``ceph-common`` and the OSD. ``ECUtil.cc`` currently includes
  ``global/global_context.h``. That dependency is removed (pass ``cct``
  or ``dpp``) because ``librados`` must not depend on the global context.
- ``ErasureCodePlugin.cc`` (the plugin registry) is already in
  ``ceph-common``.
- A helper ``ECUtil::create_ec_impl(cct, profile, &ec_impl, &ss)`` wraps
  ``ErasureCodePluginRegistry::factory()``. The OSD and monitor call sites
  are switched to it so that all three create plugins the same way.

Client codec cache
~~~~~~~~~~~~~~~~~~

.. code-block:: cpp

   struct ECCodecInfo {
     std::string profile_name;
     ErasureCodeInterfaceRef ec_impl;      // null => negative entry
     std::optional<ECUtil::stripe_info_t> sinfo;
     int load_error = 0;
   };
   // Objecter member, own mutex (not rwlock):
   std::map<int64_t, std::shared_ptr<const ECCodecInfo>> ec_codecs;

- An entry is created the first time a degraded plan is needed for a pool.
  It is not created for healthy reads, so clients that never see a degraded
  PG never load a plugin.
- The profile comes from ``osdmap->get_erasure_code_profile(
  pi->erasure_code_profile)``. A pool's profile cannot change after
  creation. Entries are dropped when the pool is deleted (seen in
  ``handle_osd_map``).
- Plugin loading (``dlopen``) happens while the caller holds the Objecter
  ``rwlock`` shared. This happens once per plugin per process, and the
  plugin registry has its own lock.
- A load failure stores a negative entry and logs a warning once. Degraded
  reads on that pool then fall back to the primary.
- ``ec_impl`` objects are immutable after ``init()`` and thread-safe for
  ``decode``, as they are in the OSD, where many PGs share them.

Client planning (``ECSplitOp``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The existing staged ``create()`` is kept. Changes, in order:

#. ``validate_flags()``: also reject ``RWORDERED`` (defence in depth).
#. Compute ``available``: shard ``s`` is available if ``acting[s]`` is not
   ``CRUSH_ITEM_NONE``, ``osdmap->is_up(acting[s])``, and ``s`` is not in
   the debug exclusion set (``objecter_debug_ec_direct_read_exclude_shards``,
   used for testing).
#. For each ``READ``/``SPARSE_READ`` op, compute wanted shard extents
   (``ro_range_to_shard_extent_set``).
#. **Healthy:** if every wanted shard is available, the plan is just the
   wanted extents. This is the same set of sub-reads as today, but expressed
   in shard space when the pool has ``ec_client_decode``.
#. **Degraded:** otherwise, if all of the following hold, call
   ``plan_read(..., available, std::nullopt)``:

   - the pool has ``ec_client_decode``,
   - the client has ``rados_ec_client_decode=true``,
   - the codec cache has a valid entry,
   - the plugin has no sub-chunks,
   - the op contains no ``SPARSE_READ``.

   ``plan_read`` may return an error (fewer than ``k`` shards available),
   in which case fall back.
#. **Otherwise:** fall back (send the original op to the primary).
#. The reference sub-read always targets the primary, which is always up by
   definition of ``acting_primary``. It carries:

   - ``GET_INTERNAL_VERSIONS``,
   - all allowed metadata ops,
   - the ``objver`` request if the caller asked for it,
   - in a degraded plan, an internal ``STAT`` to learn the object size.

   It also carries the primary's own shard reads, if any.
#. Every other sub-read carries its shard's reads and
   ``GET_INTERNAL_VERSIONS``.
#. Submit as today (``FORCE_OSD``, ``FAIL_ON_EAGAIN``, ``EC_DIRECT_READ``).

Why the object size is learned at completion: the client cannot know the
size before reading, and a second round trip would cost latency. Reads past
the end of a shard return short data. The OSD reads the shard object; it
does not use ``oi.size``. The client then:

- uses ``ro_size_to_zero_mask(size)`` from the ``STAT`` result to fill
  ``zeros_for_decode``;
- zero-pads short shard buffers up to the planned extents;
- decodes;
- truncates the result to ``min(len, size - off)``.

This is the same zero model the OSD uses (``ro_size_to_read_mask`` /
``ro_size_to_zero_mask``). For healthy plans no size is needed. Assembly
stops at the first short data shard, as it does today.

Data structures (degraded op in flight):

.. ditaa::

   Objecter::Op (orig_op, in splitop_session) <-----------------+
        ^                                                       |
        | orig_op                                               |
   ECSplitOp (shared_ptr, kept alive by Finishers) -------------+
        |-- plan: ECUtil::read_plan_t
        |-- codec: shared_ptr<const ECCodecInfo> --> ec_impl, sinfo
        |-- sub_reads: map<shard, SubRead>
        |      SubRead{ ObjectOperation rd, details[op], rc,
        |               internal_version, shard_extents }
        +-- reference_sub_read = primary shard
   Finisher (per sub-op) --> shared_ptr<ECSplitOp>, SubRead&

Completion and assembly
~~~~~~~~~~~~~~~~~~~~~~~

When the last sub-read completes, the ``ECSplitOp`` destructor runs
``complete()``, as today:

#. Classify the return codes (`Error Handling`_). Any fallback class leads
   to ``op_post_split_op_complete(-EAGAIN)``, which resubmits the original
   op to the primary with direct-read flags cleared.
#. Version check (``version_mismatch()``, unchanged logic). For every
   non-reference sub-read ``s``, the reference's map must contain ``s`` with
   the same version that ``s`` returned. Coding shards are included.
#. Put the shard buffers into a ``shard_extent_map_t``.
#. Healthy plan: ``get_ro_buffer(off, len)`` per read op. This replaces
   ``ECStripeIterator`` assembly on the new path. Sparse reads use the
   existing extent-map assembly, rewritten over the same structure.
#. Degraded plan: ``complete_read()`` with the ``STAT`` size. If it returns
   an error, count it and fall back.
#. Reference metadata op results are copied through unchanged
   (``out_ops``). Then ``process_op_reply_handlers()`` runs, as today.
#. Optional verification (``objecter_debug_ec_decode_verify``): re-read the
   same range via the primary and compare. Assert on mismatch. Test builds
   only.

Error Handling
~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 35 20 45

   * - Condition
     - Action
     - Reason
   * - Any sub-read ``-EAGAIN`` (object missing on the shard, unstable write
       ``can_serve_read()``, primary-only op), or interval change
       (``FAIL_ON_EAGAIN`` → ``POOL_DNE``)
     - Fall back
     - Only the primary can wait for recovery or resolve the conflict.
   * - Version mismatch, or a version missing from the reference map
     - Fall back
     - Torn or racing read.
   * - Non-reference sub-read error other than ``-EAGAIN`` (including
       ``-EIO``, ``-ENOENT``, ``-ENODATA``)
     - Fall back
     - The reference decides if the object exists. A shard error is either
       a race or a medium error that the primary must record and repair.
       **Change from today**: ``assemble_rc()`` currently returns these to
       the application.
   * - Reference sub-read returns an error for a user op (for example
       ``-ENOENT``, ``-ECANCELED`` from ``cmpxattr``, ``-ENODATA``)
     - Return to the application
     - Authoritative. The primary would have returned the same result.
   * - Reference sub-read ``-EIO`` on a data read of its own shard
     - Fall back
     - Same as a shard ``-EIO``.
   * - Decode error, or plugin missing at completion
     - Fall back, log
     - Defensive.
   * - Op cancelled or timed out
     - Complete with ``-ECANCELED`` or ``-ETIMEDOUT``
     - Unchanged (``split_op_tids`` cancel path).

Fallback happens at most once. The resubmitted op has
``BALANCE_READS``, ``EC_DIRECT_READ`` and ``FORCE_OSD`` cleared, so
``SplitOp::create()`` rejects it in stage 1, and it follows the normal
primary path, including the normal ``-EAGAIN`` resend on the primary.

Lifecycle state diagram
~~~~~~~~~~~~~~~~~~~~~~~

.. ditaa::

                 +-----------+
     op_submit ->|  VALIDATE |--reject--------------------------+
                 +-----------+                                  |
                       | ok                                     v
                 +-----------+   single shard,         +----------------+
                 |   PLAN    |--- no version -------> | DIRECT (1 op)  |
                 +-----------+                         +----------------+
                  |healthy |degraded  |no plan                |
                  v        v          +-------------+        EAGAIN etc.
                 +-----------+                      |           |
                 | SUBMITTED |                      v           v
                 +-----------+                +-----------------------+
                       | all sub-reads done   |  PRIMARY (normal op,  |
                       v                      |  never split again)   |
                 +-----------+  fallback class+-----------------------+
                 | CLASSIFY  |--------------->        |
                 +-----------+                        |
                   |ok    |authoritative error        |
                   v      v                           v
              +--------+ +----------+           +-----------+
              |ASSEMBLE| | COMPLETE |<----------| COMPLETE  |
              |/DECODE |-|  (rc)    |           |  (rc)     |
              +--------+ +----------+           +-----------+
                 | decode error -> PRIMARY

Every path ends in COMPLETE. The only loop is a single transition into
PRIMARY, which never re-enters PLAN. This bounds the work per operation.

Correctness argument
~~~~~~~~~~~~~~~~~~~~

**C1 – Only committed data is returned.**

- Non-primary shards return ``-EAGAIN`` if the object has an unstable
  (possibly rolled back) write (``recovery_state.can_serve_read()``).
- On the primary, a read waits behind in-flight writes to the same object
  (object context read/write lock). It therefore only sees committed state.
- These are existing mechanisms that this design relies on. Review must
  confirm they also cover coding shards, which this design reads directly
  for the first time.

**C2 – No torn reads.** Each sub-read returns the version of the object as
stored on that shard, read in the same ``MOSDOp`` as the data. The primary
returns, for every shard, the version that shard must have for the current
object state (``oi.version`` or ``oi.shard_versions[s]`` for shards skipped
by partial writes). Assembly only happens when every shard read matches.
Shard contents at matching versions are, by construction of the Fast EC
write path, the encoding of one object state:

- partial writes leave unwritten data shards unchanged and record their
  older version;
- coding shards are always written.

So the decode inputs are consistent, and decoding produces that state's
data.

**C3 – Same result as the primary.** The client uses the same plugin
(profile from the OSDMap), the same chunk mapping (``stripe_info_t`` from
the same ``pg_pool_t``), the same planner and the same decode function as
the primary. Given the same inputs, it produces the same bytes. The debug
verify option tests this end to end.

**C4 – Ordering.** ``RWORDERED`` ops never split, so they are ordered by
the primary as today. Non-ordered reads get the documented direct-read
semantics: a read may return the state before or after an uncompleted
write, never a mix (C2).

**C5 – Liveness.** See the state diagram. Every sub-read either completes,
is failed back on interval change (``FAIL_ON_EAGAIN``), or is cancelled. Then
the op completes or falls back exactly once.

**C6 – Upgrade safety.** Shard-space reads are only sent when the pool has
``ec_client_decode``. The monitor only allows that flag when
``require_osd_release >= V``, so every OSD that can be in an acting set
understands the new op flag.

OSD Changes
~~~~~~~~~~~

- New op flag ``CEPH_OSD_OP_FLAG_EC_SHARD_EXTENT`` (next free bit in
  ``CEPH_OSD_OP_FLAG_*``). It is valid only with an ``EC_DIRECT_READ``
  ``MOSDOp``. It means ``op.extent`` is in this shard's address space. Any
  other use returns ``-EINVAL``.
- ``PrimaryLogPG::do_read`` / ``do_sparse_read``: when the flag is set, skip
  ``extent_to_shard_extent()`` and the ``oi.size`` trimming (``oi.size`` is
  a logical size, and the shard's own object bounds the read). Pass the
  shard extent to ``ECBackend::objects_read_local()`` (a new overload taking
  shard offset and length). The read size is limited to the existing
  ``osd_max_object_size`` checks.
- Coding shards: no gate change is needed. ``EC_DIRECT_READ`` is accepted
  whenever the OSD is in the acting set (``is_primary() ||
  is_nonprimary()``). Without the new flag, a coding shard has no meaningful
  ro-to-shard mapping, so coding-shard direct reads are only valid with
  ``EC_SHARD_EXTENT``. The OSD returns ``-EINVAL`` otherwise, which is
  defensive because clients never send that.
- ``GET_INTERNAL_VERSIONS`` is unchanged in the first release. Story 5
  changes ``get_internal_versions()`` to return the full map on any
  primary-capable shard (``!is_nonprimary_shard(whoami)``), since those
  shards receive the full ``OI`` including ``shard_versions``.
- New OSD counters: ``ec_direct_read_served`` and
  ``ec_direct_read_coding_served``.

RGW Read Path Coverage
~~~~~~~~~~~~~~~~~~~~~~

RGW stores an S3 object as a *head* RADOS object (xattrs, manifest, and up
to ``rgw_max_chunk_size`` (4 MiB) of inline data when written to the
default storage class) plus *tail* objects (4 MiB stripes
(``rgw_obj_stripe_size``) or multipart parts). For non-default storage
classes the head lives in the default storage class's pool with no inline
data, and all data is in tail objects in the EC pool.

The table assumes RGW runs with ``rados_replica_read_policy=balance``. RGW
sets no read flags of its own and sends no ordered reads, so the policy adds
``BALANCE_READS`` to every read below. RGW requests the object version on
every read, so the reference sub-read is always sent.

.. list-table::
   :header-rows: 1
   :widths: 25 35 40

   * - RGW path
     - RADOS operation
     - Behaviour with this design
   * - GET: head state / prefetch (``get_obj_state_impl`` →
       ``raw_obj_stat``)
     - ``GETXATTRS`` + ``STAT`` + ``READ(0, 4 MiB)``, version requested
     - Split. Metadata and version go on the reference (primary) sub-read.
       Data reads go to the data shards; decode if degraded (``STAT`` is
       already present, so the size is known). See the head-prefetch risk
       below.
   * - GET: remaining head data (``get_obj_iterate_cb``, head)
     - ``CMPXATTR(id_tag)`` + ``READ``
     - Split. ``cmpxattr`` is evaluated on the reference. ``-ECANCELED``
       is authoritative.
   * - GET: tail stripes / multipart parts (``get_obj_iterate_cb``)
     - ``READ(ofs, ≤ 4 MiB)``. Up to 16 MiB in flight
       (``rgw_get_obj_window_size``).
     - Split and decode if degraded.
   * - Range GET, ``partNumber`` GET
     - Unaligned ``READ`` on head or tail
     - Split. The planner aligns shard reads (4 KiB) and decode handles
       partial stripes.
   * - SSE (decryption), compression
     - Block-aligned ranged ``READ``\ s, decoded in RGW
     - As above. No dependence on sparse reads.
   * - HEAD object, ``GetObjectAttributes``, versioning (OLH) resolution
     - ``GETXATTRS`` / ``STAT``, no data read
     - Not split (no data read). Goes to the primary. Small, metadata only.
       Story 5 could move these to any primary-capable shard.
   * - CopyObject (RGW-side copy), lifecycle transition, cloud transition,
       multisite fetch (remote GET), S3 Select, Swift DLO/SLO segments
     - Same GET paths as above on the source
     - Covered by the GET rows.
   * - Copy using OSD ``copy_from``
     - OSD-internal read
     - Not a client read. Unchanged.
   * - Bucket index, metadata pools (cls_rgw, omap, ``cls_version``)
     - ``CALL`` / omap ops
     - Primary (complex ops / omap). Unchanged.
   * - D3N / D4N caches
     - Cache misses use the GET paths above
     - Covered.

**Head prefetch on small objects.** ``READ(0, 4 MiB)`` with an unknown size
is split into ``k`` sub-reads even for a 4 KiB object. Most of them return
no data. Via the primary, the read costs one message and one shard read. As
a direct read it costs ``k`` messages. This is existing direct-read
behaviour that this design does not change. Large objects are unaffected:
the prefetch is one 4 MiB read in a multi-GiB GET. After the prefetch, RGW
knows the object size, so later reads are size-bounded and split
efficiently. If benchmarks show a regression (success criterion), see
`Open Questions`_.

Is there a simpler solution?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The simplest version of this feature is "client decode, everything else
unchanged". That is what this design is. It adds one planning branch and one
completion branch to ``ECSplitOp``, and makes two small OSD changes. The
extra work is the extraction of shared code, which requirement 6 asks for and
which removes duplicate destripe logic from the client. Reading coding shards
in ro-space (no new op flag) was considered, but coding shards have no
ro-space mapping. It would need an equally sized OSD change with less clear
semantics.

Security
~~~~~~~~

- No new authentication or authorisation paths. Each sub-read is a normal
  ``MOSDOp`` checked against the client's caps for the pool and namespace,
  exactly as the primary-directed op would be. Coding-shard data is derived
  from data the client is already allowed to read.
- The client loads EC plugins from ``erasure_code_dir``, which is set by the
  client's own configuration. This is no different from other plugin types
  already loaded by clients (compressors, crypto).
- No cryptographic changes. RGW SSE decryption is unchanged and happens
  after the read.

Resource usage
~~~~~~~~~~~~~~

- **Memory, persistent:** one ``ECCodecInfo`` per EC pool used in degraded
  mode (plugin matrices, a few KiB). Plugin shared objects are loaded once.
- **Memory, runtime:** a degraded op holds the buffers for each shard read,
  about ``(k_read / k) × request`` for full-stripe reads, plus a transient
  aligned copy during decode (``pad_and_rebuild_to_ec_align``). In-flight
  bytes are bounded by the existing Objecter throttle
  (``objecter_inflight_op_bytes``). Bytes are charged at the requested size,
  as today (see :doc:`direct_reads`, Throttling Considerations), so peak
  client memory in fully degraded operation can reach about twice the
  throttle.
- **CPU, client:** decode only in degraded mode. ISA-L decodes at several
  GB/s per core. A destripe that uses ``shard_extent_map_t`` instead of
  ``ECStripeIterator`` must stay zero-copy; this is verified in the
  performance plan.
- **CPU, OSD:** primaries do less work. Shard OSDs serve the same bytes they
  would have served to the primary.
- **Scalability:** no per-object client state. The codec cache scales with
  the number of pools.

Upgrade & Backwards Compatibility
---------------------------------

- **Messages:** the only wire change is a new op flag, sent only when
  ``ec_client_decode`` is set on the pool.
- **Daemon dependencies:** ``ec_client_decode`` requires
  ``require_osd_release >= V``. It is set automatically when that is raised
  (same mechanism as ``split_reads`` in ``maybe_enable_pool_split_ops``).
  Manual setting is refused before that.
- **Pool encoding:** the new flag bit is stripped when encoding
  ``pg_pool_t`` for peers without the V feature, using the same pattern as
  ``FLAG_CLIENT_SPLIT_READS``. Older clients that do see it ignore it.
- **Mixed clients:** pre-V clients keep the Umbrella behaviour (ro-space
  healthy direct reads, primary for degraded). The OSD keeps supporting
  ro-space direct reads. V clients use the new path only on flagged pools.
- **On-disk:** none.
- **Downgrade:** not supported after ``require_osd_release`` reaches V (the
  standard rule).

Limits and Config Settings
--------------------------

.. list-table::
   :header-rows: 1
   :widths: 30 12 12 46

   * - Name
     - Type
     - Default
     - Description
   * - ``ec_client_decode`` (pool flag)
     - flag
     - auto on (V)
     - See CLI section.
   * - ``rados_ec_client_decode``
     - bool
     - ``true``
     - Client: allow client-side decode for direct reads. Runtime. When
       false, degraded reads go via the primary.
   * - ``objecter_debug_ec_direct_read_exclude_shards``
     - str
     - empty
     - Dev/test: comma-separated shard ids treated as unavailable, to force
       client decode.
   * - ``objecter_debug_ec_decode_verify``
     - bool
     - ``false``
     - Dev/test: re-read every decoded range via the primary and assert
       equality.

No new hard limits. Decode is limited to plugins without sub-chunks. Shard
counts follow the existing Fast EC limits.

Security & Packaging
--------------------

- **New open-source components:** none.
- **New deliverable/package:** none. Packaging change: EC plugins must be
  installable with ``radosgw`` on Debian/Ubuntu. Options are moving
  ``libec_*.so`` into a package that ``radosgw`` depends on, or adding a
  dependency on ``ceph-base``. RPM ``ceph-radosgw`` already requires
  ``ceph-base``.
- **Cryptographic changes:** none.
- **Security risk from setting/interface changes:** none identified (see
  `Security`_).

Debug Strategy & Performance Counters
-------------------------------------

**Objecter perf counters** (``objecter`` section, on every librados client):

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Counter
     - Meaning
   * - ``ec_dr_ops`` / ``ec_dr_bytes``
     - Ops and bytes completed by EC direct reads (healthy or decoded)
   * - ``ec_dr_decode_ops`` / ``ec_dr_decode_bytes``
     - Of those, ops and bytes that needed client decode
   * - ``ec_dr_decode_lat``
     - Time spent in ``complete_read()`` for decoded ops
   * - ``ec_dr_fallback_eagain``
     - Fallback: a sub-read returned ``-EAGAIN``
   * - ``ec_dr_fallback_interval``
     - Fallback: interval change during the read
   * - ``ec_dr_fallback_version``
     - Fallback: version mismatch
   * - ``ec_dr_fallback_shard_error``
     - Fallback: non-reference error (``-EIO`` and others)
   * - ``ec_dr_fallback_no_codec``
     - Not split or fallback: plugin unavailable, or decode disabled
   * - ``ec_dr_fallback_decode_error``
     - Fallback: decode failed
   * - ``ec_dr_not_split_unsupported``
     - Not split: unsupported op mix, ``RWORDERED``, sparse needing decode

The existing ``split_op_reads`` counter is kept.

**OSD perf counters:** ``ec_direct_read_served``,
``ec_direct_read_coding_served``. The existing
``replica_read_redirect_missing`` and ``replica_read_redirect_conflict``
counters already count the OSD side of ``-EAGAIN``.

**Logging:**

- ``debug_objecter=20`` already logs every split decision. New log lines
  cover the plan type (healthy/degraded), the available set, decode shards,
  the per-shard read extents, and the fallback class.
- The decode error log at level 0 includes enough to reproduce: pool,
  pgid, object, epoch, plan and versions.

**Admin socket:** ``objecter_requests`` already dumps the split-op session.
It is extended to show, for each split op, the plan type and the
outstanding sub-op tids with their target OSD and shard.

**Callhome / telemetry:** add the ``ec_dr_*`` counters for RGW daemons to
the perf channel, to measure adoption and the fallback rate.

**Dashboards:** no changes required. A "direct vs fallback reads" RGW panel
is a stretch item.

4. Test/Validation Strategy
===========================

Unit tests
----------

- **Planner equivalence:** run the old ``get_min_avail_to_read_shards`` and
  the new ``plan_read`` on randomised k/m (2–12 / 1–4), chunk sizes,
  chunk mappings, available sets and ranges. Plans must match. Run before
  the old code is deleted.
- **Unknown-size planning:** ``plan_read(nullopt)`` followed by
  ``complete_read(size)`` must equal the OSD result for the same object
  across sizes around chunk and stripe boundaries, including empty and
  sub-chunk objects.
- **Decode equivalence:** encode random objects with jerasure and ISA-L,
  drop every combination of up to ``m`` shards, and check that the client
  path reconstructs the exact bytes.
- **ECSplitOp classification:** a mock Objecter feeding every return-code
  combination in `Error Handling`_. Assert the action taken and that
  fallback happens at most once.
- **SplitOp destripe:** the existing ``ECStripeIterator`` tests are ported
  to the ``shard_extent_map_t`` path.

CI / Automation
---------------

- ``ceph_test_rados_io_sequence``: add a ``--ec-client-decode`` mode that
  sets ``objecter_debug_ec_direct_read_exclude_shards`` per sequence, so
  every sequence runs healthy and with each shard excluded. Turn on
  ``objecter_debug_ec_decode_verify``.
- Extend the existing EC read injects (types 0 and 1) to target shard OSDs
  on the direct-read path. Add an ``-EAGAIN`` inject to exercise fallback.
- ``qa/standalone``: tests for the pool flag CLI (all error messages),
  auto-enable on ``require-osd-release``, and that an ``RWORDERED`` op never
  produces ``EC_DIRECT_READ`` sub-ops.
- Teuthology:

  - ``rados/thrash-erasure-code`` with Fast EC,
    ``rados_replica_read_policy=balance`` and decode on.
  - ``rgw`` suites (s3-tests, multisite, crypt, compression) with the data
    pool on Fast EC (4+2, 8+3), ``rados_replica_read_policy=balance`` and
    OSD thrashing.

Configurations
--------------

- Minimum: 6 OSDs, Fast EC 4+2, jerasure and ISA-L.
- 8+3 and 2+2 (k = m edge case). Non-default chunk mapping (``mapping=``)
  to exercise ``get_shard()`` remapping.
- Primary on a coding shard (via ``pg_temp``/``primaryfirst``).

Functional Test Coverage
------------------------

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Story
     - Tests
   * - 1
     - RGW large-object GET benchmark with ``ec_dr_*`` counters checked.
       s3-tests.
   * - 2
     - Stop one OSD (and then two on 8+3). Run RGW GETs and
       ``io_sequence``. Check counters and data.
   * - 3
     - Standalone ``RWORDERED`` test. Inject tests for each fallback class.
   * - 4
     - Counter presence. Pool flag on/off behaviour.

Real hardware finds problems that unit tests do not: plugin availability in
real packages, decode CPU cost, and timing windows around OSD mark-down.
Record the hardware configuration, the plugin, k/m and the RGW workload mix
for every test run.

I/O Path Stress & Data Integrity
--------------------------------

- ``ceph_test_rados`` with balanced reads on Fast EC under
  ``thrash-erasure-code``. The tool verifies all reads.
- ``io_sequence`` with concurrent write/read sequences. Assert that no torn
  results occur and that reads see either the old or the new data.
- RGW: upload with content checksums and GET-verify under OSD kill/revive,
  ``noout``/``out`` cycles and backfill. Cover multipart, SSE-S3,
  compression and range reads.
- Workloads: 4 KiB–4 MiB objects, 64 MiB–4 GiB objects, range reads of
  random sizes, sequential and random.

Upgrade Testing
---------------

- ``upgrade/umbrella-x``: with mixed OSDs, clients must not send shard-space
  reads (pool flag absent). After ``require-osd-release v``, the flag
  appears and degraded reads decode. Umbrella clients against V OSDs must
  keep working.
- RGW upgrade with traffic running across the flag change.

Error Recovery / RAS Testing
----------------------------

- Plugin missing on the client (rename ``libec_isa.so``): degraded reads
  fall back, the warning is logged once, and the counter increases.
- Shard ``-EIO`` inject: fallback, and the primary logs and repairs.
- Interval change mid-read (``objecter_debug_inject_relock_delay`` plus OSD
  mark-down): fallback once.
- Decode error inject (dev-only hook): fallback and level-0 log.

Performance
-----------

- RGW GET throughput (warp or hsbench). Measure baseline (primary),
  healthy direct, and degraded direct. Include large objects (≥ 64 MiB) and
  small objects (4 KiB–1 MiB).
- Targets (to be confirmed with Performance): degraded ≥ 90% of healthy
  throughput; small-object p99 latency ≤ +10% of baseline; primary OSD
  egress about ``1/k`` of the PG read bandwidth.
- Client CPU per GB read, healthy and degraded. Destripe must show no
  regression compared with ``ECStripeIterator``.
- Interlock with the QE performance team for HDD and NVMe clusters.

Interoperability
----------------

- RGW in containers (cephadm, Rook) and on bare-metal Debian/Ubuntu and
  RHEL packages: plugin availability.
- Mixed-version clients (Umbrella RGW against V cluster).

UI Testing
----------

No GUI changes, so no GUI test story is needed.

Test Hardware
-------------

Standard lab hardware. At least 12 OSD hosts are needed for the 8+3
host-failure-domain degraded tests.

5. Maintenance (on Epic closure)
================================

To be completed at epic closure:

- Release notes: new pool flag, ``rados_replica_read_policy=balance``
  guidance for RGW, Debian packaging note.
- DST material link.
- Development and support debug guide: counters, log lines, switches.
- Follow-on epics identified (see below).

Future Work
-----------

- Story 5: reference sub-read on any primary-capable shard. Metadata-only
  ops (HEAD) served by those shards.
- Locality-aware shard selection (``LOCALIZE_READS``) for EC stretch
  clusters (:doc:`ec_stretch_cluster`). Choose the ``k`` shards nearest the
  client, which the planner's ``available`` input makes straightforward.
- Hedged reads (read ``k+1`` shards, decode from the first ``k`` to reply)
  to cut tail latency.
- Client decode for sparse reads once allocation tracking lands.
- Re-plan instead of falling back on interval change.

Open Questions
--------------

#. If the small-object benchmark regresses with
   ``rados_replica_read_policy=balance``, is guidance to keep the default
   policy for small-object RGW workloads enough, or is an Objecter change
   needed for the head prefetch?
#. Should ``split_reads`` also become operator-settable, so that support can
   turn off all direct reads on a pool without client config changes?
#. Exact bit for ``CEPH_OSD_OP_FLAG_EC_SHARD_EXTENT`` and whether the
   kernel client headers need it reserved.
