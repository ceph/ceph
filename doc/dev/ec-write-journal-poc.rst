A write journal for optimized EC pools
======================================

:Status: Request for Comments (RFC). The code is a proof of concept (PoC):
         disabled by default, restricted to a single administrator-enrolled
         pool, and benchmarked on a v20.2.4 prototype. This branch ports the
         PoC to ``main`` (compile-checked and unit-tested on ``main``; cluster
         testing on ``main`` is pending).

Summary
-------

In Ceph's optimized ("fast") erasure-coded (EC) pools, small overwrites
are expensive: each overwrite requires a read-modify-write (RMW)
cycle across its entire stripe, serializes through the Placement Group (PG)
in-order pipeline, and writes a PG log entry on every touched shard.

This RFC proposes a lightweight **EC write journal** that buffers small,
aligned overwrites instead of immediately running RMW:

1. **Unlogged Record Appends:** The primary appends the overwrite as an
   unlogged *record* to a PG-local append-only log replicated across m + 1
   holder shards (shard 0 plus the m parity shards).
2. **Immediate Client Ack:** The client is acknowledged as soon as all m + 1
   holders commit the record to disk, surviving up to m failures.
3. **Coalesced Flushes:** The primary tracks live records in an in-memory index
   and flushes accumulated updates later as standard, logged EC writes.
   Stripes that are completely covered flush without reading chunks from disk.
4. **Transparent Reads:** The primary serves client reads by overlaying live
   journaled bytes on top of data read from shards.
5. **No Impact on Unenrolled Pools:** Pools that do not opt in see no
   behavioral change and no measurable performance difference (an unenrolled
   optimized EC pool on this code matched vanilla v20.2.4 within 1% IOPS and
   2% p99).

Key results from prototype testing on an 11-node cluster (8+3 EC, 55 NVMe
OSDs, 11 clients at queue depth 128), with a 256 MiB journal budget per PG;
the 64 MiB default is compared in `Sizing the Journal Budget`_:

* **Higher Throughput:** Achieved **1.9x** write IOPS for hot 4 KiB random
  writes (where stripes coalesce multiple writes before flushing) and
  **1.3x-1.5x** on realistic mixed write sizes, with reduced mean and p99
  latency. Throughput converges toward baseline on cold workloads where
  stripes are rarely rewritten before eviction, and at the 64 MiB default
  dips slightly below it on the coldest sets (0.88x-0.96x at 96 GiB).
* **Lower Write Amplification:** Despite writing payloads twice (once to the
  journal and once during flush), the journaled pool reduced total block
  device writes by **20% to 54%** in every workload measured at that budget
  (0.46x-0.77x device bytes on 4 KiB; 0.80x-0.94x on mixed workloads). At the
  64 MiB default the saving holds on hot sets (0.54x-0.81x) and disappears on
  cold ones (0.93x-1.07x). This saving occurs because unlogged records bypass
  the per-write RocksDB metadata updates (onodes, PG log entries, PG info,
  and the per-write rollback clone with its later removal) that dominate the
  fast-EC path.
* **Durability:** Kill tests under active write load (single host failure, and
  two hosts killed 3 seconds apart) confirmed zero data loss and clean replay.

Problem Statement
-----------------

On an 8+3 EC pool with 4 KiB chunks (32 KiB stripe), overwriting 4 KiB
inside an existing object requires:

* 3 to 4 data chunk reads,
* 1 data chunk write and 3 parity chunk writes,
* A PG log entry on each written shard,
* Serialization through the PG's in-order RMW pipeline, and
* A periodic *roll-forward kick* (a transaction-empty op updating full PG info
  on every shard) whenever the pipeline empties (roughly every third write under load).

On NVMe hardware, a single 4 KiB client overwrite translates into approximately
6.5 physical I/O operations and **86 KiB** of device writes: only 16 KiB is
actual chunk data, while the remaining 70 KiB is RocksDB metadata (29 KiB WAL
plus 41 KiB compaction). High tail latency follows: median latency was 6.7 ms,
while p99 reached 68 ms.

Real-world RBD workloads leave many writes small enough to coalesce. The
write-size histograms of two large production RBD clusters, one IOPS-heavy and
one bandwidth-heavy (lifetime OSD histograms, hundreds of billions of writes),
show:

* **52-62%** of writes are at most 32 KiB (42% and 30% are 4 KiB writes),
* **29-39%** are 128 KiB or larger, and
* at most 0.3% are smaller than 4 KiB; of 22,171 writes sampled in flight,
  all but 31 were 4 KiB aligned.

The journal as built takes **52-63%** of the writes by count: those up to
32 KiB inside one stripe or across one boundary. Larger and full-stripe writes
keep the direct path.

Because these writes exhibit high temporal and spatial locality, buffering
them allows multiple overwrites to merge into a single stripe flush,
eliminating read-modify-write chunk reads and amortizing metadata updates.

Architecture & Core Concepts
----------------------------

Terminology
~~~~~~~~~~~

Stripe
   The standard EC stripe unit (k times the chunk size, e.g., 32 KiB for
   8+3 with 4 KiB chunks). The journal uses stripes as the unit of flush.
   A stripe's records may span multiple slots and segments; the primary's
   in-memory index groups them by stripe. When flushed, all live blocks for the
   stripe are submitted as a single standard EC write, updating the object's
   ``ec_journal`` xattr with the latest materialized journal version.

Slot
   A disk object named ``ecj.<0..15>`` in the PG-local namespace
   (``.internal_pg_local``). Each journaled PG allocates 16 fixed slots on
   each of its m + 1 holders, budgeting ``osd_ec_journal_max_bytes / 16`` per
   slot (4 MiB by default with a 64 MiB PG budget). Slots grow on demand as
   records are appended. Slots provide full-replica redundancy for unflushed
   data: each holder stores identical slot data and OMAP headers, surviving
   up to m failures. Slots persist across PG intervals and restarts, and are
   deleted only during a journal drain.

Segment
   The in-memory tracking unit for an active slot on the primary. Exactly one
   segment is *open* for appends at any time. When its slot fills, the segment
   is *closed*, and the next segment opens in the lowest free slot. A closed
   segment is *released* and its slot recycled once all its records have either
   been overwritten or flushed to disk and all appends are durable. Reclaiming
   space always happens a full slot at a time.

Record
   A single journaled client write targeting a single stripe (writes spanning
   two stripes produce two coordinated records). On disk, each holder stores
   the payload at a 4 KiB-aligned offset within the slot object, accompanied
   by an immutable OMAP header entry keyed by that offset. Records are written
   once, never updated in place, take the next user version, and bypass the PG
   log until their stripe flushes.

OMAP Header
   Per-record metadata stored in the slot's OMAP table. The key is ``r`` followed
   by the 16-hex-digit slot offset (e.g., ``r0000000000009000``), ensuring keys
   sort in append order. The value (~175 bytes) contains a magic number, crc32c
   checksum (covering header and payload), slot offset, target object, journal
   version, object size, write offset, write length, client mtime, request ID,
   and user version. During peering or recovery, a replaying primary inspects
   OMAP headers to determine which records are live and rebuilds the request
   and metadata index, reading only live payloads into memory.

Index
   The primary's in-memory directory of live records, organized by object and
   stripe offset. It tracks live 4 KiB blocks (with the newest record winning
   per block), payload buffer references, journal versions, append sequences,
   and holder durability state. The index exists exclusively in primary memory;
   it is rebuilt during PG activation by reading OMAP headers from the slots.
   Client reads and flush decisions consult this index.

Entity Relationships
~~~~~~~~~~~~~~~~~~~~

The diagram below illustrates how client writes flow into records, slots, and
the primary's index::

   Stripe        Standard EC stripe (32 KiB on 8+3); the unit of flush.
     |           Its journaled writes live across slots; the primary index
     | 1 : N     groups them per stripe.
     v
   Slot          16 fixed objects per PG (ecj.0 .. ecj.15) on m + 1 holders.
     |           Each budgets max_bytes / 16 (4 MiB default).
     | 1 : 1     Holds one active segment at a time.
     v
   Segment       In-memory rotation state. Appended to while open, then closed;
     |           recycled when all its records are dead or flushed.
     | 1 : N
     v
   Record        One client write to one stripe (cross-stripe writes split into 2).
     |           Whole 4 KiB blocks appended at a 4 KiB-aligned offset.
     | 1 : 1     Written once, unlogged, carries user version and request ID.
     v
   OMAP Header   Key "r" + 16-hex offset in slot OMAP. Contains crc32c, object,
                 stripe offset, journal version, length, mtime, reqid, user version.
     |           Replay uses headers to identify live records and rebuild requests.
     | N : 1
     v
   Index         Primary in-memory map per stripe. Tracks live 4 KiB blocks, newest
                 version per block, payload references, and durability.
                 Drives client read overlays and background flushes.

Records & Write Lifecycle
~~~~~~~~~~~~~~~~~~~~~~~~~

Admission is evaluated early in ``PrimaryLogPG::do_op`` via
``PGBackend::ec_journal_admit`` before the operation receives an object version.
An admitted write completely bypasses ``execute_ctx``: ``do_op`` passes it
directly to the backend within the PG-lock critical section with no
``OpContext``, no object version, no PG log entry, and no object lock.

``ECBackend::journal_write`` processes the write as follows:

1. **Assigns a Journal Version:** Generates an ``eversion_t`` consisting of the
   current interval start epoch and a monotonic append sequence.
2. **Issues Unlogged Sub-Writes:** Sends an unlogged ``ECSubWrite`` (encoding
   version 6) to each of the m + 1 holder shards carrying the slot append,
   the OMAP header, and (if rotating segments) the slot reset transaction.
3. **Executes on Holders:** Each holder (``handle_sub_write`` with the
   ``unlogged`` flag) queues the transaction and acknowledges on disk commit.
   It creates no PG log entry, writes no PG info, and updates no object state.
4. **Acknowledges Client:** The primary tracks in-flight writes in a per-tid
   table. Once all m + 1 holders reply, the record is marked durable in the
   in-memory index, and the client is acknowledged in order per object.

Writes across distinct objects complete independently without queuing behind
the RMW pipeline, flush reads, or k + m parity commits.

**Replica Placement:**
Records are replicated to m + 1 holder shards: shard 0 and the m parity
shards. Under ``allow_ec_optimizations`` these are exactly the shards eligible
to act as primary, so the primary is always one of them, and any newly elected
primary has a local copy of every acknowledged record for replay. The records
survive up to m failures, like the erasure-coded data.

**Slot Management:** Slots reside in the PG-local internal namespace
(``.internal_pg_local``) with the PG's own hash prefix. The total PG budget
(``osd_ec_journal_max_bytes``, default 64 MiB, minimum 16 MiB) is divided
evenly across 16 fixed slots, giving 4 MiB per segment by default. Payloads are
whole 4 KiB blocks at 4 KiB-aligned offsets, which keeps BlueStore from
triggering deferred write cycles or read-modify-writes on unaligned appends.
Reusing a slot cleans up old data and OMAP headers atomically in the
transaction that writes the new segment's first record, avoiding separate
background trim transactions.

Why Records Are Not PG-Logged
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

An early prototype recorded every journal write with a standard PG log entry
on every holder. Benchmarks showed two problems:

* **RocksDB Contention:** Each holder transaction touched ~7 RocksDB keys and
  wrote 5.9 KiB to the WAL (1.7 KiB for PG log and PG info), driving the
  ``bstore_kv_sync`` thread to 90-97% CPU utilization.
* **Pipeline Head-of-Line Blocking:** Because sub-writes had to complete in
  strict version order to maintain monotonic ``pg_committed_to``, client
  acknowledgments frequently stalled behind slow RMW reads and k + m shard
  commits, spiking tail latency by hundreds of milliseconds.

A logged EC write also carries its own undo: every written shard clones the
overwritten extents into a per-write rollback object, and the log entry keeps
the recipe to apply it (the previous object info and attributes), so that a
write committed on some shards but not others can be rolled back at peering;
once the write is committed everywhere, the roll-forward deletes the clone
again. That is one object creation and one removal per shard per write in
RocksDB, on top of the log entry, and in our measurements a source of
compaction load on EC pools. A record carries none of this: it is never rolled
back (a record is adopted or superseded, never undone), so the holders write
nothing to undo and delete nothing later. Only the flush, one logged write per
stripe, keeps rollback state.

By switching to unlogged records, holder transactions were reduced to 3 RocksDB
keys (slot onode, extent shard, and OMAP header), cutting WAL traffic from
5.9 KiB to 3.7 KiB per transaction. At the PG level, WAL writes dropped from
29.2 KiB to 15.0 KiB per 4 KiB client write, and OSD CPU time dropped from
3.87 ms to 1.55 ms.

The first prototype, with a PG log entry per record, kept almost none of the
journal's benefit: measured on the same cluster against fast-EC, it gained only
7-9% write IOPS on hot 4 KiB random-write sets, lost 4-30% on colder ones
(0.96x at 24 GiB and 0.70x at 96 GiB per volume) and wrote 1.05x-1.46x the
device bytes, although it already coalesced writes and avoided the per-write
read-modify-write. With the records made unlogged and the design otherwise the
same (coalescing, m + 1 holders, logged flushes), the same workloads measured
1.8x write IOPS hot, 1.0x cold and 0.57x the device bytes. Most of the saving
came from removing the per-write PG log transaction; avoiding the
read-modify-write and coalescing the parity writes account for the rest.

Preserving RADOS Semantics Without Per-Write Logging
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Standard RADOS logged writes provide at-most-once execution (via the PG log's
request history) and immediate metadata visibility (advancing user versions and
timestamps). The journal preserves these semantics without the write-amplification
penalties of per-write PG logging:

* **At-Most-Once Execution (Request IDs & Dups):**
  Each record header stores the client's request ID (``osd_reqid_t``) and the
  user version reported in its reply. While a record remains unmaterialized, the
  primary tracks its request ID in memory:

  - If a duplicate resend arrives for a durable record, the primary answers it
    immediately with the recorded user version and success status, executing nothing twice.
  - If a resend arrives while the write is still in flight, it is held until all
    holders commit and answered immediately after the original write.
  - When the stripe is materialized (via background flush or direct-write absorption),
    the materializing transaction logs the accumulated request IDs as its
    ``extra_reqids`` (analogous to how ``copy-from`` logs sub-requests). From then
    on, the standard PG log dup window answers any further resends.
  - The primary remembers the request IDs of the newest
    ``osd_pg_log_dups_tracked`` + ``osd_min_pg_log_entries`` requests (3,250 by
    default), the PG log's own window; an older resend runs again, as it does
    on a logged pool. A dead segment keeps its slot while any of its records'
    IDs is still in the window, so a replaying primary rebuilds the same
    window from the slots that the old primary held in memory.

* **Immediate Metadata Visibility (User Versions & Timestamps):**
  A journaled write immediately advances the pool's monotonic user version
  sequence and updates ``mtime`` in the primary's cached ``ObjectContext``:

  - Operations querying metadata (``stat``, ``assert-version``, ``get_last_version``)
    observe journaled updates immediately, so guards such as librbd encryption's
    ``assert-version`` behave as they do on a logged pool.
  - Logged operations of the PG allocate user versions strictly above the newest
    journaled version, preserving global monotonicity across the pool.
  - The final stripe materialization writes the accumulated metadata to the
    on-disk ``object_info_t``. Only the internal PG log version (which clients do
    not observe) waits for stripe flush.

* **Crash Recovery via Adoption:**
  If a primary fails, unacknowledged records committed to disk on at least one
  surviving holder are adopted during activation replay rather than rolled back,
  as RADOS allows for an unacknowledged write. An adopted record may be on fewer
  than m + 1 holders, so reads of its stripe wait for the flush that replay
  starts at once, which makes the data m + 1 durable before anyone sees it. A
  client resending an in-flight write after failover meets the adopted record
  and receives a reply immediately without re-executing.

In-Memory Index & Stripe Flushes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The primary maintains an in-memory index tracking live 4 KiB blocks grouped by
stripe. Key design properties:

* **Fine-Grained Coalescing:** The newest append sequence wins per 4 KiB block.
  When a block is superseded by a newer write, its previous in-memory buffer is
  freed immediately. Memory usage is therefore bounded by the active working set
  of dirty blocks rather than the total append volume.
* **Memory Overhead:** Block payloads are zero-copy slices of client write
  buffers (~96% of ``max_bytes`` under 4 KiB records). Index metadata adds ~160
  bytes per live 4 KiB block, ~240 bytes per segment record, and ~560 bytes per
  active stripe, plus about 1 MiB per PG for the request window (3,250 IDs,
  independent of the budget). Total primary process memory is approximately
  1.1x to 1.2x ``max_bytes`` at capacity.
* **Flush Execution:** A flush is executed as an internal logged EC write
  (``PrimaryLogPG::ec_journal_flush``, similar to a cache tier promote). It
  issues standard EC writes with object versions and PG log entries. Complete
  stripes (where all blocks are present up to the object boundary) write out
  directly without chunk reads; partial stripes compute parity deltas via the
  standard planner.
* **Crash Consistency:** Because flushes are fully logged, any crash or torn
  write during a flush is resolved by standard Ceph peering and rollback. Flushes
  bypass ``do_op`` gates but gracefully skip objects undergoing scrub, backfill,
  or recovery.
* **Concurrency:** Up to ``osd_ec_journal_max_flushes`` (default 32) stripe
  flushes may run concurrently per PG; the cap bounds what a flush burst (a
  replayed backlog, a reclaimed segment) pushes into the PG's RMW pipeline
  beside client writes.

**Flush Policy:**
Flushes operate per stripe (not per segment) according to two rules:

1. **Eager Flush for Complete Stripes:** Any stripe whose live blocks fully
   cover the stripe and are durable flushes immediately as soon as the flusher
   has capacity, eliminating chunk read overhead.
2. **Demand-Driven Flush for Partial Stripes:** Incomplete stripes flush
   strictly oldest-first only when triggered by:

   - **Segment Reclaim:** Freeing the oldest segment when all slots fill.
     Reclaim is sequence-bounded (``Journal::reclaim_bound``) to flush only the
     stripes pinning the oldest segment, preventing flush storms.
   - **Conflicting Operations:** A non-journaled mutation (e.g., truncate, delete)
     touching the object.
   - **Administrative Drain:** When ``osd_ec_journal_drain`` is requested.
   - **Replay Backlog:** Adopting recovered records after an interval change.

Client reads never trigger flushes; they are served directly from the index.

Admission Criteria
~~~~~~~~~~~~~~~~~~

To enter the journal, an incoming op must satisfy
``ECWriteJournal::candidate_write``:

* Single plain ``WRITE`` op of whole, 4 KiB-aligned blocks.
* Fits inside one stripe, or crosses exactly one stripe boundary up to
  ``osd_ec_journal_max_write_bytes`` (default 32 KiB).
* Targets an existing head object with no snapshot state or attributes.
* Does not change object size and does not overwrite the entire object (full-object
  overwrites set data digests, which must remain accurate).
* PG acting set is complete (not degraded, undersized, or backfilling) and no
  drain is active.
* Object is not locked by an exclusive asynchronous read/write op.

For an eligible write, ``ECBackend::journal_admit`` decides:

1. **Full-Stripe Writes Go Direct:** An aligned full-stripe write bypasses the
   journal since it already requires no reads. Any existing journal records for
   that stripe are absorbed and dropped on commit.
2. **Active Stripes Enter Journal:** If the target stripe already has live or
   flushing records in the journal, the new write is journaled to preserve order.
   If the journal is temporarily full, it waits for space.
3. **New Stripes Enter Journal if Space Permits:** Otherwise, the write is
   journaled unless the log is full, in which case it falls back to the direct
   optimized path.

Writes Across Stripe Boundaries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

About a quarter of the sampled production writes (24-25% on both clusters)
cross a stripe boundary. The journal handles these by splitting the write into
**two coordinated records** (one per stripe):

* Appended with consecutive journal versions in a single unlogged holder transaction.
* Managed under a single sub-write transaction ID; holders commit both or neither.
* Client receives a single acknowledgment once both records are durable.
* Flushes, index tracking, and replay treat each half independently, allowing each
  stripe to coalesce and flush on its own schedule.

The boundary limit (``osd_ec_journal_max_write_bytes``, default 32 KiB; 0 to
disable) prevents large writes from exhausting slot capacity.

Direct Writes Beside the Journal
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Non-candidate plain overwrites (e.g., unaligned writes or multi-stripe ranges)
execute immediately on the standard optimized EC path while **absorbing any
overlapping journaled stripes** (``Journal::take_range``):

* **Fully Overwritten Stripes:** The stripe's journal records are discarded
  without being flushed once the direct write commits.
* **Partially Overwritten Stripes:** Unwritten live blocks from the journal are
  merged into the direct write's transaction, materializing the entire stripe
  in one shot.

The absorption keeps three invariants:

* **Ordering:** Overlapping stripes are marked flushing. Newer incoming writes
  to those stripes receive higher sequence numbers and queue behind the direct write.
* **Durability:** Direct writes wait in ``do_op`` until any in-flight journal
  records in their range commit, ensuring acknowledged data is never lost.
* **Slot Safety:** Absorbed blocks remain live until the direct write commits
  across all shards, ensuring slots are not reused prematurely.

**Barrier Mutations:**
Operations such as truncates, deletes, object size changes, read-modify ops,
or snapshot mutations wait in ``do_op`` until all journaled stripes of the target
object have flushed completely. Subsequent operations on that object queue behind
the barrier, preserving strict per-object serialization.

Reads & Data Serving
~~~~~~~~~~~~~~~~~~~~

Client reads are served exclusively by the primary using an **in-memory overlay**:

* **Overlay Mechanics:** The primary's index tracks every live block for active
  stripes. In ``ECBackend::objects_read_async``, the primary reads underlying
  chunks from disk and overlays live journaled blocks onto the result buffer.
* **Fast-Path Zero-Disk Reads:** If live blocks in the journal cover the entire
  read request, shard reads are bypassed completely. The read still traverses
  the read pipeline (as an empty op) to preserve PG in-order completion.
* **Read Durability Invariants:** Reads observe only records that are durable on
  all m + 1 holders. If a read targets a stripe with an in-flight record append,
  it parks in ``execute_ctx`` (via ``ec_journal_read_ready``) for one round-trip
  until the append commits, without forcing a flush.
* **Mutual Exclusion with Flushes:** A stripe flush modifies shard data on disk.
  To prevent degraded or parity reads from reconstructing chunks across mixed
  pre- and post-flush states, reads and flushes of the same stripe are strictly
  mutually exclusive. Flushes never starve reads, and reads never starve
  reclaims.
* **Routing of Direct EC Reads:** Because only the primary maintains the journal
  index, individual shards cannot serve reads independently. If a client attempts
  direct shard reads (e.g., using ``split_reads`` with balanced read policy),
  shards return ``-EAGAIN``. The client ``Objecter`` catches this, clears the
  direct-read flag, and reroutes the read to the primary.

**Visibility & Attribute Semantics:**
Both data bytes and object metadata are overlaid in primary memory:

* Object metadata (``user_version`` and ``mtime``) updates in memory immediately
  upon write admission. Read operations querying object attributes (``stat``,
  ``assert-version``, ``get_last_version``) observe journaled updates in real
  time without waiting for a flush.
* Stripe flushes or absorbing direct writes persist the newest metadata to the
  on-disk ``object_info_t``.
* Only the object's internal PG log version (not visible to clients) advances
  at stripe flush.

Materialization Markers (The ``ec_journal`` Xattr)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Slots are append-only, so a flushed record stays in its slot until its segment
is released; without a marker a replaying primary could not tell it from a live
one and would re-adopt data that a later direct write may already have
superseded. To determine which records have been permanently written to
erasure-coded storage, flushes maintain a compact xattr named ``ec_journal``
(``ECWriteJournal::Materialized``) on the target object, in the same
transaction as the data they materialize:

* **Structure:** Consists of a ``base`` journal version plus a per-stripe map
  of the newest journal version materialized: ``stripes[stripe_id] = version``.
* **Materialization Rule:** A record with version V in stripe s is
  materialized if ``V <= max(base, stripes[s])``.
* **Compaction:** When an object has no other live journal records, ``base``
  advances to the current journal version, clearing the map. When live records
  exist, ``base`` advances to the lowest live version, pruning older entries.
* **Efficiency:** Stored on shard 0 and parity shards. To avoid onode bloat,
  entries are encoded as compact varint deltas (4-5 bytes per entry).

Peering & Crash Recovery
~~~~~~~~~~~~~~~~~~~~~~~~

An interval change discards all primary in-memory state. Durability and recovery
proceed entirely from disk:

1. **Activation Replay:** During PG activation (before serving client I/O in
   ``PrimaryLogPG::on_activate_complete``), the primary scans its local slot
   OMAP headers.
2. **Record Classification:** Each record is categorized:

   - **Dead:** The target object was deleted or the ``ec_journal`` xattr confirms
     the record was already flushed.
   - **Deferred:** The object is degraded and awaiting recovery; judgment is
     deferred until recovery brings the object's attribute up to date.
   - **Live:** The record is adopted into the in-memory index in version order.
     Only payloads of live records are read into memory.

3. **Peer Slot Queries:** If the new primary missed recent writes (e.g., it was
   offline when a write committed), it queries peer slot OMAP headers using
   ``handle_sub_read``. Any missing live records are fetched across holders
   before client operations resume.
4. **PG Splitting:** On a PG split, shards copy their slot objects into the child
   collection within the split transaction. Child slots retain identical data and
   offsets, filtered by the child's hash range. PG merge is currently unsupported
   and requires draining the journal first.

Graceful Shutdown
~~~~~~~~~~~~~~~~~

During clean OSD shutdown, each OSD initiates background drains on enrolled PGs
before entering ``STATE_STOPPING``. The shutdown sequence waits outside locks
for up to 30 seconds for flushes to complete. If the timeout expires, the OSD
shuts down anyway; remaining records are replayed cleanly by the successor primary.

Isolation of Unenrolled Pools
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Pools that are not enrolled see no behavioral change and no measurable
performance impact. An A/B on the test cluster, the same optimized EC pool left
unenrolled on this code and on vanilla v20.2.4 (aligned 4 KiB writes on 3 and
96 GiB sets, the size mix with 30% reads), agreed within 1% on write IOPS, 2% on
p99 latency and 0.3% on device bytes per write; the pool enrolled but drained
was no slower either. The isolation rules:

* All journal hooks check ``pgbackend->ec_journal_enrolled()`` before execution.
* Shared listing operations (scrub, backfill, pgls) skip the internal namespace
  only on enrolled pools.
* Sub-write messages use version 6 (with ``unlogged`` flag) only for journal
  records; standard pools continue using sub-write version 5.
* Two generic bugfixes in shared code included in this branch benefit all
  fast-EC pools: taking completed reads off the in-flight list before
  callback execution, and including all shards in log entries when degraded or
  undersized.

Empirical Performance & Evaluation
----------------------------------

All benchmark results were gathered using the v20.2.4 prototype on an 11-node
test cluster:

* **Hardware & Layout:** 55 NVMe OSDs, 8+3 EC profile with 4 KiB chunks (32 KiB
  stripes), 512 PGs.
* **Workload:** 11 clients running ``fio`` random writes at queue depth 128 (one
  RBD image per client).
* **Baselines:** The enrolled pool was compared against the identical pool with the
  journal disabled ("fast-EC"; "Optimized" in the tables) and against a classic EC pool.
* **Configurations:** The baseline tests used a 256 MiB journal budget per PG
  (16 segments of 16 MiB). Results at the 64 MiB default are compared in
  `Sizing the Journal Budget`_.
* **Roll-Forward Kick:** Deferring the fast-EC pipeline's transaction-empty
  roll-forward kick by 200 ms while a PG is idle
  (``osd_ec_rollforward_delay_ms``, applied to the enrolled pool in this
  branch) is a fast-EC improvement in its own right. Measured at saturation on
  aligned 4 KiB (3 and 96 GiB sets), it gives the fast-EC path 1.05x write
  IOPS, 0.93x p99 and 0.95x device bytes.

The *working set* represents the random write boundary per client and dictates
spatial locality:

* **3 GiB per client (Hot content):** High locality. A stripe accumulates many
  overwrites before flushing (~21 writes per flushed stripe).
* **96 GiB per client (~1 TB total, Cold content):** Low locality. Most stripes
  accumulate only 1-2 writes before eviction, approaching the cost of direct RMW.
* **Skewed distribution:** Realistic production traffic exhibits strong skew toward
  hot objects, landing between these extremes. With 90% of the writes aimed at
  10% of a 96 GiB set (4 KiB writes), the journal reached **1.39x** the
  optimized pool's IOPS (185.9k vs. 134.2k, p99 59 ms vs. 76 ms, 0.58x device
  bytes) where the uniform 96 GiB set gave 1.05x.

Throughput and Latency
~~~~~~~~~~~~~~~~~~~~~~

**Aligned 4 KiB Random Writes (IOPS, 256 MiB budget):**

=========== ======== ========= ======= =========
Working Set Journal  Optimized Classic Journal /
                                       Optimized
=========== ======== ========= ======= =========
3 GiB       265.7k   137.3k    69.0k   **1.94x**
6 GiB       231.9k   136.8k    71.4k   **1.70x**
24 GiB      177.0k   132.6k    69.3k   **1.34x**
96 GiB      136.0k   129.6k    62.2k   **1.05x**
=========== ======== ========= ======= =========

**Production Size Mix (with 30% concurrent reads, Write IOPS, 256 MiB budget):**

=========== ======== ========= ======= =========
Working Set Journal  Optimized Classic Journal /
                                       Optimized
=========== ======== ========= ======= =========
3 GiB       105.8k   72.3k     53.2k   **1.46x**
6 GiB       104.1k   74.3k     54.4k   **1.40x**
24 GiB      98.4k    73.6k     51.7k   **1.34x**
96 GiB      79.0k    71.3k     50.4k   **1.11x**
=========== ======== ========= ======= =========

Coalescing raises throughput on hot data, tapering
toward parity as workloads turn cold. In the worst-case cold scenario, a stripe
incurs a record append plus a classic EC write.

**Latency Reductions:**

* **Aligned 4 KiB Random Writes (3 GiB hot set):** Mean latency dropped from
  10.3 ms to **5.3 ms**, while p99 dropped from 67 ms to **39 ms**.
* **Production Write-Size Mix (3 GiB set, write-only):** Uncapped writes ran at
  1.30x IOPS (121.5k vs. 93.5k) with mean latency of 11.6 ms vs. 15.0 ms and p99
  of 66 ms vs. 77 ms. Under *equal load* (journal capped at the optimized pool's
  uncapped rate of 87.2k IOPS), mean latency fell from 16.1 ms to **8.6 ms** and
  p99 dropped from 93 ms to **47 ms**; throttled further to 70% of that rate,
  mean latency was 4.9 ms vs. 9.6 ms and p99 was 27 ms vs. 54 ms, showing lower
  p99 across every write-size class.

Write Amplification & Device Bytes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Although every journaled write is written twice (once to the slot and once during
stripe flush), the journaled pool wrote **fewer bytes to disk**
than the fast-EC baseline:

**Total Device Bytes Written per Client Byte (Data + RocksDB WAL and SST, 256 MiB budget):**

==================== ======= ========= ======= =========
Workload             Journal Optimized Classic Journal /
                                               Optimized
==================== ======= ========= ======= =========
4 KiB, 3 GiB         9.9     21.6      45.3    **0.46x**
4 KiB, 6 GiB         10.7    21.5      44.0    **0.50x**
4 KiB, 24 GiB        15.4    22.0      45.2    **0.70x**
4 KiB, 96 GiB        17.3    22.6      35.8    **0.77x**
Mixed sizes, 3 GiB   3.23    4.02      5.52    **0.80x**
Mixed sizes, 6 GiB   3.26    3.99      5.46    **0.82x**
Mixed sizes, 24 GiB  3.38    4.00      5.60    **0.84x**
Mixed sizes, 96 GiB  3.11    3.31      4.22    **0.94x**
==================== ======= ========= ======= =========

**Why the Journal Saves Device Writes:**
On the fast-EC path, every small client write triggers an onode update,
a PG log entry, and a PG info update on every shard written. RocksDB writes these
updates to its WAL and later compacts them through SST levels.

In contrast, an unlogged journal record touches only its slot file and one
OMAP key per holder. Object metadata is updated once during the coalesced stripe
flush. On 4 KiB writes (3 GiB set), the journal generated 39.4 KiB total device
writes per client write versus 86.4 KiB on fast-EC: slightly more data
(18.1 KiB vs 16.2 KiB) but far less RocksDB traffic (15.0 KiB WAL vs 29.2 KiB;
6.3 KiB compaction vs 41.0 KiB). Furthermore, because complete stripes flush
without reads, device read ops fell from 3.4 to 0.01 per client write.

Durability & Fault Tolerance
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Durability was validated under heavy write saturation:

* **Host Failure Under Load:** Killing all 5 OSDs on a storage host during active
  writes resulted in 36 million records adopted across new primaries (1,670
  evaluated after object recovery, 1 fetched across peers), with 238 in-flight
  writes safely replayed. Post-recovery verification showed zero data corruption.
* **Dual Host Failures:** Survived two hosts killed 3 seconds apart
  under load across multiple test runs without data loss.
* **Filesystem & Image Verification:** Cleanly mounted and verified an ext4
  filesystem on krbd while the journal operated at full capacity, including
  data verification across full image drains.

Costs & Current Limitations
---------------------------

* **Slot Storage Overhead:** Records are duplicated across m + 1 holders and
  stored as whole 4 KiB blocks. A 4 KiB write occupies 16 KiB of slot data on
  8+3 until flushed. However, metadata savings consistently offset this
  overhead in benchmarks.
* **Primary Memory Footprint:** The primary holds live block payloads in memory
  (up to ~96% of ``max_bytes`` per PG) plus index metadata, totaling ~1.1x-1.2x
  ``max_bytes`` in process memory. Payloads are accounted under BlueStore's
  cache mempool. An index-only architecture reading payloads on demand from disk
  is discussed in `Future Work`_.
* **Shared RMW Commit Pipeline:** Stripe flushes share the standard PG RMW queue
  with direct writes, meaning slow flush I/O can delay subsequent direct writes
  on the same PG.
* **Scope Restrictions:** Limited to one administrator-enrolled pool (4+3, 8+3,
  or 12+3 profiles with 4 KiB chunks). Restricted to head objects; does not
  support snapshots, omap, or xattr updates in journaled writes.
* **Format Upgrades:** The record format lacks an in-place upgrade path: the
  journal must be drained via ``osd_ec_journal_drain`` before upgrading builds.
* **Write Shapes:** Only plain writes of whole, 4 KiB-aligned blocks are
  journaled (inside one stripe, or across one stripe boundary up to 32 KiB).
  Smaller or unaligned writes (for example 512-byte guest sectors) bypass the
  journal and take the overlapping journaled stripes with them on the direct
  path.
* **Scrub & Recovery Separation:** Slot contents and unflushed records are not
  scrubbed. The journal pauses admission and drains whenever the acting set is
  degraded, undersized, or recovering.
* **PG Merge:** PG split is fully supported; PG merge is currently unsupported
  and requires draining the pool prior to reducing ``pg_num``.

Configuration & Observability
-----------------------------

**Startup Options (OSD daemon configuration):**

* ``osd_ec_journal_enable`` (default: ``false``): Globally enables the feature.
* ``osd_ec_journal_pool`` (default: ``-1``): Pool ID enrolled in journaling.
* ``osd_ec_journal_max_bytes`` (default: ``64 MiB``, minimum: ``16 MiB``):
  Storage budget per PG on each holder, split evenly across 16 slots (4 MiB
  segments by default).
* ``osd_ec_journal_max_flushes`` (default: ``32``): Maximum stripe flushes in
  flight concurrently per PG.

**Runtime Options:**

* ``osd_ec_journal_max_write_bytes`` (default: ``32 KiB``): Upper size limit for
  candidate writes; set to 0 to force all cross-stripe writes direct.
* ``osd_ec_journal_drain`` (default: ``false``): Initiates an orderly drain: halts
  new admissions, flushes all dirty stripes, and deletes slot objects.
* ``osd_ec_rollforward_delay_ms`` (default: ``200``): How long a PG of the
  enrolled pool stays quiet before its roll-forward kick (0 sends it at once).
  Pools outside the journal keep the immediate kick.

**Observability:**
PG status is exposed via ``ceph tell <pgid> query`` under ``ec_write_journal_poc``,
tracking live bytes, records, segments, admission/bypass counts, flush reasons,
replay progress, the request window (``requests``, ``request_window``,
``pinned_segments``, ``dup_replies``, ``dup_held``) and the objects with a
metadata overlay (``object_metas``, ``newest_user_version``). During a drain,
administrators can observe ``bytes``, ``records``, and ``flushes_in_flight``
decrease to zero.

Sizing the Journal Budget
-------------------------

The journal budget (``osd_ec_journal_max_bytes``) controls how long an incomplete
stripe remains buffered in memory and disk slots before eviction, directly
determining how many client writes coalesce into each flush:

* **Retention Time:** A record is only evicted when its segment becomes the oldest
  in a full 16-segment log. In practice, a stripe accumulates writes across
  about 14/16 of the total budget.
* **Workload Dynamics:** At peak synthetic saturation (>500 writes/sec/PG), a
  256 MiB budget held stripes for ~2 minutes. In a realistic production pool
  (e.g., 30,000 writes/sec across 8,192 PGs, or <4 writes/sec/PG), a 64 MiB
  budget retains stripes for over an hour.

The budget incurs two concrete hardware resource costs:

1. **Slot Disk Space (on all m + 1 holders):** At full capacity, disk usage
   per OSD is ``max_bytes x (m + 1) x PGs / OSDs``.
   On our test cluster (512 PGs, 55 OSDs, 8+3), each OSD acted as a holder for
   ~37 PGs, occupying ~9 GiB of disk space per OSD (238 MiB per PG).
2. **Memory Footprint (on the primary OSD only):** The primary holds live payload
   buffers in memory, totaling ~1.1x to 1.2x ``max_bytes`` in process memory
   including index structures. Payloads are accounted in BlueStore's data cache
   mempool under ``osd_memory_target``, naturally displacing cached chunks rather
   than expanding heap unconstrained.

**Budget Sensitivity Analysis (64 MiB vs. 256 MiB):**

The table below contrasts the 64 MiB default (16 segments of 4 MiB) against the
256 MiB configuration (16 segments of 16 MiB):

================ ============= ============= ==================
Workload         IOPS 64/256   Bytes 64/256  Writes per Flush
================ ============= ============= ==================
4 KiB, 3 GiB     1.68x / 1.94x 0.54x / 0.46x 8.5 / 21.0
4 KiB, 6 GiB     1.42x / 1.70x 0.75x / 0.50x 4.7 / 15.7
4 KiB, 24 GiB    1.04x / 1.34x 1.07x / 0.70x 1.9 / 4.6
4 KiB, 96 GiB    0.88x / 1.05x 0.93x / 0.77x 1.2 / 2.0
Mixed, 3 GiB     1.44x / 1.46x 0.79x / 0.80x 52.8 / 65.5
Mixed, 6 GiB     1.37x / 1.40x 0.81x / 0.82x 16.9 / 65.4
Mixed, 24 GiB    1.13x / 1.34x 0.99x / 0.84x 2.2 / 18.2
Mixed, 96 GiB    0.96x / 1.11x 1.07x / 0.94x 1.1 / 2.3
================ ============= ============= ==================

**Key Takeaways:**

* With a 64 MiB budget, hot workloads and production mixed sizes maintain strong
  gains (1.37x-1.44x IOPS, ~20% device write savings).
* On very cold workloads (96 GiB uniform random), 64 MiB retention is insufficient
  to coalesce writes, causing performance to dip slightly below baseline (0.88x-0.96x).
* Skew restores the gain at 64 MiB: with 90% of the writes aimed at 10% of the
  same 96 GiB set, the journal reached 1.20x IOPS at 0.94x device bytes (1.39x
  and 0.58x at 256 MiB). Coalescing depends on the budget relative to the hot
  set per PG, not on time, so skewed production traffic pays where uniform
  random writes do not.
* **Why 64 MiB is Default:** The budget is per PG and the primary holds it in
  memory, so it multiplies by the primary PGs of every OSD. At production scale
  256 MiB per PG can be too rich for the OSD memory budget; 64 MiB fits. A
  memory-light index that reads payloads back from the slots at flush time
  (`Future Work`_, open question 8) would decouple the budget from primary
  memory and allow larger budgets.

Validation & Unit Testing
-------------------------

The implementation is verified by ``unittest_ec_write_journal`` (272 tests,
executed plain and under ASan/UBSan):

* **Coverage:** Validates record encoding, durability invariants, flush ordering,
  supersede mechanics, cross-segment coalescing, slot recycling, crash recovery,
  and replay edge cases (unreadable slots, corrupted CRC, failed holder fetches).
* **Integration Tests:** Verifies client read overlays, cross-stripe write atomicity,
  direct write absorption, compact xattr marker updates, and PG split slot cloning.
* **Planner Validation:** Tests execute the real EC planner and transaction
  generator, ensuring journaled writes touch only shard 0 and parity shards,
  while standard writes span all shards.
* **Test Runner:** Executable via ``src/script/test-ec-write-journal-poc.sh``
  against existing build trees.

Open Questions for Upstream Reviewers
-------------------------------------

1. **Storage Layer for Records:** Is storing PG-local slot objects in the
   ``.internal_pg_local`` namespace across m + 1 primary-eligible shards the
   right approach, or should records live in the PG log, in BlueStore internal
   structures, or striped across all acting shards?
2. **Unlogged Sub-Writes & Peering Integration:** Records bypass the PG log and
   are recovered during peering from slot OMAP headers, tracked via the object's
   ``ec_journal`` xattr. Does this design align well with Ceph's peering, scrub,
   and backfill invariants?
3. **Preserving RADOS Semantics via ``extra_reqids``:** The journal maintains
   at-most-once execution and immediate metadata visibility (monotonic
   ``user_version`` and ``mtime``) without per-write PG log entries by caching
   request IDs in memory and logging them as ``extra_reqids`` during stripe flush.
   Does this handoff align well with upstream conventions for unlogged fast-paths?
4. **Client Direct Read Routing:** Because only the primary possesses the journal
   index, direct EC reads (``split_reads``) return ``-EAGAIN``, forcing the client
   to retry via the primary. Is there a preferred way to handle this routing?
5. **Enrollment Mechanism:** Should enrollment remain an OSD configuration option,
   or transition to a pool flag (e.g., ``ceph osd pool set <pool> ec_write_journal 1``)
   or EC profile setting?
6. **Fixed-Rate Benchmarking:** Write amplification savings were demonstrated under
   saturation. Which fixed-rate comparisons (e.g., 20k IOPS capped) are desired
   by maintainers?
7. **Resource Accounting (Per-PG vs. Per-OSD):** Currently, ``max_bytes`` and
   ``max_flushes`` are budgeted per PG without an OSD-level ceiling, so an OSD's
   bound is its primary PGs times the cap. Should the journal implement an
   aggregate per-OSD memory, disk and flush-concurrency budget shared
   dynamically across primary PGs?
8. **In-Memory Payloads vs. Disk-Backed Index:** In the current PoC, the primary
   caches live payloads in RAM. An experimental memory-light index reads payloads
   back from disk on demand during flush, reducing RAM usage by ~20x (from 256 MiB
   to 9 MiB per PG) at a 13-26% write throughput penalty. Should we adopt disk-backed
   payloads or retain in-memory buffers?
9. **Unaligned Writes:** The index tracks 4 KiB blocks, so only whole,
   4 KiB-aligned writes are journaled; smaller or unaligned writes take the
   direct path. Of 22,171 production RBD writes sampled on two clusters, all
   but 31 were 4 KiB aligned. Accepting arbitrary alignment requires
   range-based indexing (see `Future Work`_). Should the journal accept
   unaligned writes?

Future Work
-----------

* **Memory-Light Index (Disk-Backed Payloads):** Retain only metadata (object,
  offset, length, version, slot offset) in memory and fetch payloads from local
  slot objects at flush time. This decouples retention time from primary RAM,
  allowing multi-gigabyte journal budgets per PG.
* **Range-Based Indexing:** Track arbitrary byte ranges instead of one node per
  4 KiB block, reducing index overhead from 12-23% of payload to roughly 5-8%
  and enabling support for unaligned guest I/O.
* **Striping Across All Shards:** Distribute slot objects uniformly across all
  k + m shards to balance disk I/O and capacity evenly across the cluster.
* **Monitor-Side PG Merge Guards:** Introduce monitor protections to prevent
  administrative ``pg_num`` reductions on enrolled pools until journals are fully
  drained.
