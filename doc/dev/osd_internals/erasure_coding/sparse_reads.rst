================================================================
EC Support for Sparse Reads and Logical Allocation - Design
================================================================

1. Overview
===========

This document describes the design for supporting reliable sparse reads on
Erasure Coded (EC) pools. The solution enables sparse read operations to
accurately report allocation state and avoid transferring data for unallocated
regions. This also provides guaranteed logical allocation preservation for use
cases that require it (such as encryption).

2. Terminology
==============

**Stripe**

A chunk-aligned cross-section of the object across all shards. A stripe spans
``chunk_size × k`` bytes of object data (where ``k`` is the number of data
shards). It is the unit of EC encoding and decoding.

**Slice**

A cross-section of the object at the same LBA range across all shards, at any
alignment. A stripe is a special case of a slice that is aligned to
``chunk_size``. In this design, slices are always aligned to ``EC_ALIGN_SIZE``
(4K). A single stripe contains ``chunk_size / EC_ALIGN_SIZE`` slices.

3. Current State
================

EC pools currently have the following limitations:

**Sparse Reads**

- EC treats all reads as fully allocated, transferring data for the
  entire requested range regardless of actual allocation state

**MAPEXT Operation**

- MAPEXT is not supported on EC pools (returns ``-EOPNOTSUPP``)

**ZERO Operation**

- ``CEPH_OSD_OP_ZERO`` is intended to *deallocate* storage (punch a hole),
  but the current EC implementation instead writes a buffer of literal zeros
  through the normal encode-and-write path.  The deallocation never reaches
  the object store, so no storage is reclaimed and sparse reads cannot detect
  the hole.

**Zero-Block Handling During Recovery**

During recovery and reconstruction, EC pools cannot distinguish between:

- Zeros that were explicitly written by a client
- Regions that were never allocated

**Note:** This is a fundamental property of erasure coding mathematics, not an
implementation shortcoming. When data is reconstructed from parity chunks, the
erasure coding algorithm can only regenerate the data values: it has no
information about whether a block of zeros was explicitly written or never
allocated. Additional metadata is required to preserve this distinction.

EC recovery currently:

1. Reads available shards
2. Reconstructs missing data
3. Detects blocks of zeros in reconstructed data
4. Marks zero blocks as unallocated to save space

This means:

1. A client that writes zeros may later find those regions reported as
   unallocated during sparse reads
2. Allocation state is not preserved across recovery/reconstruction
3. Clients cannot rely on sparse reads returning accurate allocation
   information

**Parity Deallocation During Recovery**

A further consequence of zero-detect during recovery is that parity shards
may be deallocated when they would not otherwise be. When all data shards in
a slice contain zeros (whether explicitly written or force-allocated), the
reconstructed parity for that slice is also all zeros. The zero-detect rule
then deallocates the parity extents — even when the data content is unchanged.

**Impact**

These limitations prevent use cases that require guaranteed allocation
preservation, such as encryption (where data may encrypt to all zeros) and
other applications that depend on accurate logical allocation state.

4. Problem Statement
====================

The requirement is to provide:

1. **Sparse Read Support**: Enable EC pools to accurately report allocation
   state during sparse reads by querying BlueStore and correctly merging
   results across shards. This is supported unconditionally: zero-detect
   does not need to be enabled.

2. **Logical Allocation Preservation**: Track which 4K-aligned blocks contain
   explicitly written zeros, ensuring they remain allocated through recovery
   and reconstruction. This requires zero-detect to be enabled.

The core technical challenge is that EC pools maintain allocation information
at 4K resolution during normal operations, but this information is lost during
recovery/reconstruction when data is regenerated from coding parities.

Without these mechanisms, clients that rely on accurate logical allocation
information cannot use EC pools reliably.

**Primary Use Case: Encryption**

The primary use case for this feature is encrypted data (RBD with LUKS,
CephFS with fscrypt). For encrypted workloads, the probability of a 4K block
encrypting to all zeros is pathologically low (theoretically 1 in 2^(4096*8)).
This design optimizes for this property, assuming that genuine blocks of
zeros are extremely rare, allowing for efficient metadata storage and fast
zero detection with early exit.

5. Proposed Solution
====================

The solution tracks which 4K-aligned blocks contain all zeros when written,
stores this information in object metadata (``force_allocated_extents``), and
uses it during recovery/reconstruction to preserve the distinction between
allocated-zeros and unallocated regions.

**Key Design Decisions:**

1. **Zero detection in PrimaryLogPG**: Zero detection is performed in
   ``PrimaryLogPG`` before the operation is dispatched to the backend. This
   keeps the logic common to both EC and replicated pools. For fast EC pools,
   the results are stored in ``force_allocated_extents`` and used during
   recovery. For replicated pools, ``force_allocated_extents`` is not used;
   however, zero detection results may be useful to replicated pool
   applications in future (for example, to enable efficient storage by
   avoiding writes of known-zero blocks).

2. **Two mechanisms to enable zero detection**: Zero detection can be enabled
   by either of two independent mechanisms:

   - **Pool property** (``preserve_allocation``): enables zero detection for
     every write to every object in the pool. Simple to deploy — no client
     changes required — but applies unconditionally to all workloads on that
     pool, including unencrypted ones. In a mixed pool where only a subset of
     images are encrypted, all writes pay the zero-detection cost and all
     objects accumulate ``force_allocated_extents`` metadata, regardless of
     whether they need it. In the worst case (large objects with highly
     fragmented zero patterns), the per-object OI overhead could be
     significant.

   - **MOSDOp flag** (``CEPH_OSD_FLAG_PRESERVE_ALLOCATION``): a flag carried on
     the outer ``MOSDOp`` message, set by the client on a per-request basis.
     This allows clients that know their data requires zero tracking (e.g. RBD
     with encryption enabled) to opt in on a per-I/O basis, without affecting
     unencrypted images sharing the same pool. Requires client-side support to
     set the flag; older clients that do not set it receive no tracking.

   Both mechanisms are supported. Zero detection is performed in PrimaryLogPG
   whenever either the pool flag is set or the MOSDOp flag is present on the
   incoming request.

3. **4K block granularity**: Aligned with EC operations, LUKS2, and fscrypt
   block sizes.

4. **``force_allocated_extents`` is fast EC only**: The metadata stored in
   ``force_allocated_extents`` has no utility for replicated pools.
   Replicated pools maintain correct allocation state through BlueStore
   directly and do not lose it during recovery.

6. Metadata Storage
===================

**Object Info Extension**

Force-allocated extents are stored in ``object_info_t`` as a
``force_allocated_extents_t``, a custom type that holds an adaptive
representation (interval set or bitmap — see section 6.1) with deferred
decode semantics (see section 6.2):

.. code-block:: cpp

    struct object_info_t {
      // ... existing fields ...
      force_allocated_extents_t force_allocated_extents;
      // ... existing fields ...
    };

**Note:** ``force_allocated_extents`` is only populated and consulted for fast
EC pools. It is never set or read for replicated pools.

6.1. Adaptive Representation
-----------------------------

The representation used for ``force_allocated_extents`` is chosen dynamically
on each encode: whichever of the two representations is smaller is used. No
fixed threshold or hysteresis is applied — the decision is made purely on
encoded size at the time of writing. Both directions of conversion must be
supported (interval_set → bitmap and bitmap → interval_set) since the optimal
choice can change as extents are added or removed.

**interval_set<uint32_t> (sparse representation)**

Each interval stores a start block index and a length in units of 4K blocks,
using 32-bit values. Encoded size is proportional to the number of intervals.
This is the smaller representation when extents are few and clustered.

**Bitmap (dense representation)**

A bit-count field is stored at the beginning of the bitmap, followed by one
bit per 4K block of the object. Bit ``n`` is set if block ``n`` is
force-allocated. The bitmap size is fixed by the object size rounded up to a
4K boundary, regardless of how many bits are set. This is the smaller
representation when extents are numerous and fragmented.

The leading bit-count field allows the decoder to distinguish a bitmap from an
interval_set encoding and to allocate the correct buffer without needing a
separate tag byte.

**Storage Characteristics**

- For encrypted workloads, the probability of a 4K block encrypting to all
  zeros is pathologically low (theoretically 1 in 2^(4096×8)), so the
  interval_set will almost always be empty and the bitmap will never be chosen.
- For other workloads, force-allocated extents are relatively rare; the
  interval_set will be smaller in almost all realistic cases.
- The bitmap is a safety net for adversarial or highly fragmented patterns.
  The dynamic selection means there is no sharp cliff — as fragmentation
  increases, the representation transitions smoothly to bitmap at exactly the
  point it becomes beneficial.

**OBC Cache Memory Requirement**

``object_info_t`` is cached in the Object Context (OBC) cache in memory for
each open object. The encoded ``force_allocated_extents`` field contributes to
the in-memory size of every cached OBC. The representation must therefore
remain small: an unbounded or linearly-growing structure would cause memory
pressure proportional to the number of concurrently cached objects. The bitmap
representation imposes a hard upper bound of ``object_size / 4K / 8`` bytes
per object (e.g. 512 bytes for a 4 MiB RBD object), which is acceptable. The
adaptive selection between interval_set and bitmap ensures this bound is
respected regardless of access pattern.

6.2. Deferred Decode and Zero-Copy Re-encode
--------------------------------------------

``force_allocated_extents_t`` uses a lazy decode strategy to avoid paying
decode and re-encode costs on operations that never touch the FAE field.

**Storage layout**

The object holds two representations simultaneously:

- ``raw_`` — a ``bufferlist`` containing the encoded bytes exactly as they
  were read from the OI xattr.  This is the authoritative copy while the field
  has not been accessed.
- ``bitmaps_`` — the live in-memory ``map<uint64_t, fae_bitmap_t>`` used for
  all queries and mutations.  This is populated on first access and becomes
  authoritative after any mutation.
- ``dirty_`` — a flag that tracks which representation is authoritative.
  When ``dirty_`` is ``false`` and ``raw_`` is non-empty, the encoded bytes
  in ``raw_`` are authoritative and ``bitmaps_`` has not yet been populated.
  Once ``materialise()`` runs (populating ``bitmaps_`` from ``raw_``), or once
  any mutation is applied, ``dirty_`` is set to ``true`` to indicate that
  ``bitmaps_`` is now authoritative.

**Decode path (materialise)**

On first access to any query or mutation method, ``materialise()`` is called.
If ``dirty_`` is already ``true``, ``bitmaps_`` is already authoritative and
the call returns immediately with no work done.  Otherwise it parses ``raw_``
into ``bitmaps_``, clears ``raw_``, and sets ``dirty_ = true``.

**OI decode fast path**

When ``object_info_t`` is decoded from an xattr, the FAE field bytes are
consumed from the stream and stored in ``raw_`` verbatim.  ``bitmaps_`` is left
empty and ``dirty_`` is set to ``false``.  No bitmap parsing takes place at
this point.  Operations that read or modify other OI fields (e.g. size, mtime)
pay zero cost for the FAE field.

**Encode fast path**

When ``object_info_t`` is serialised back to an xattr:

- If ``dirty_`` is ``false`` and ``raw_`` is non-empty, the original bytes are
  appended directly to the output ``bufferlist`` with no re-encoding.  This
  path is taken whenever the FAE field was decoded from storage but never
  modified.
- If ``dirty_`` is ``true``, the live ``bitmaps_`` are encoded into ``raw_``
  and then appended.  The result is cached in ``raw_`` so that subsequent
  encodes of the same object also take the fast path.

The net effect is that the common case — an OI read followed by a write that
does not touch FAE — touches the FAE bytes only twice: once to copy them into
``raw_`` on decode, and once to copy them back out on encode.

7. Recovery Rules
=================

During EC recovery and backfill, the following rules apply:

**If a block is force-allocated according to OI:**

- If data has been recovered for the shard, it will be written as-is (no
  zero-detect)
- If no data has been recovered, a zero block will be written

**Otherwise (block not in force_allocated_extents):**

- If data has been recovered for the shard:

  - If the recovered block is all zeros, deallocate it
  - Otherwise write it as-is

- If no data has been recovered, deallocate (do not write a zero block)

**Parity shards:**

- Zero-detection is applied to parity shards in the same way as data shards.
  If a parity shard extent is all zeros after encoding or reconstruction, it
  is deallocated (left as a hole).
- Zero-detection on parity is the responsibility of the EC plugin, which may
  optimise the detection (for example by determining during encoding that
  parity is zero without a separate pass). Early implementations may perform
  zero-detection on the encoded parity output after the fact.
- This deallocation of parity means that thick provisioning (``rbd
  --thick-provision``) is not supported on EC pools: a subsequent ZERO op
  against a thick-provisioned image will reclaim the reserved parity storage.
  See section 11.

**Code Locations**

The recovery rules are implemented in:

- ``src/osd/ECCommon.cc`` — recovery push path (``ec_sparse_finish_read``,
  FAE consultation during reconstruction)
- ``src/osd/PrimaryLogPG.cc`` — SPARSE_READ and MAPEXT dispatch

7.1. Whiteout Objects
---------------------

Whiteout objects are cache-tier markers created when an object is deleted in a
cache tier.  They have no real data content.

**Recovery path**

``force_allocated_extents`` shall be ignored for whiteout objects during
recovery.  Consulting FAE on a whiteout would cause spurious zero blocks to be
pushed to shards where no data should exist.

**Delete / evict path**

When an object transitions to a whiteout (on delete or evict), any existing
``force_allocated_extents`` on that object shall be discarded.  This ensures
that if the object is later promoted from the backing tier it starts with a
clean FAE state.

8. Modified RADOS Operations
=============================

8.1. SPARSE_READ
----------------

**CEPH_OSD_OP_SPARSE_READ**

SPARSE_READ is the indirect way to query allocation state - it returns both
data and an extent map showing which regions contained data.

**Current Behavior (EC):**

- Legacy EC reads: treat all reads as fully allocated, transfer entire
  requested range
- EC Direct reads: provide limited sparse read support on good path only
- During recovery/reconstruction: may incorrectly report written-zeros as
  unallocated

**New Behavior:**

SPARSE_READ on EC pools has two code paths depending on the release gate and
pool type:

**Legacy / pre-Vampire fallback:**

When ``require_osd_release`` has not yet reached *Vampire*, or when the pool
does not have EC optimisations enabled (Classic EC), ``objects_sparse_read_async``
returns ``-EOPNOTSUPP`` and SPARSE_READ falls back to issuing a normal full
async read.  The result is wrapped in a synthetic extent map that reports the
entire requested range as a single allocated extent.

This fallback does not violate any allocation-accuracy promise: the Vampire
release gate and the ``preserve_allocation`` / ``CEPH_OSD_FLAG_PRESERVE_ALLOCATION``
tracking mechanisms are both inactive on these paths, so no FAE metadata has
been accumulated and no accuracy guarantee has been made to the client.

**Good Path (No Recovery):**

1. Query BlueStore for physical allocation on each shard
2. Merge allocation results across shards
3. Return allocation state directly from BlueStore
4. No need to consult ``force_allocated_extents`` — BlueStore already
   maintains correct allocation state

**Recovery/Reconstruction Path:**

When one or more shards must be recovered via erasure decoding, the sparse-read
path cannot query BlueStore directly for extent state — the physical allocation
on individual shards reflects shard-level storage, not the logical object.
Instead a two-stage approach is used:

**Stage 1 — Uniform read and decode**

1. Issue a uniform read across all healthy data shards covering the requested
   range.
2. Decode the missing shards using the erasure code.

**Stage 2 — FAE-guided rescan**

For each decoded shard, scan the recovered data block by block:

- A block that contains non-zero data is allocated.
- A block that contains only zeros is allocated only if it is covered by the
  shard-projected ``force_allocated_extents``; otherwise it is an unallocated
  hole.

The shard-projected FAE is derived by mapping the logical-object FAE through
the EC stripe geometry to obtain per-shard block indices.

The resulting per-shard extent maps are merged to produce the logical extent
map returned to the client.

**Note:** The ``force_allocated_extents`` metadata is only required during
recovery and reconstruction operations. During normal operations, BlueStore
maintains allocation state correctly, so sparse reads can query BlueStore
directly without consulting the force-allocated extent map.

**Note on Direct Reads:** Direct EC reads (using
``CEPH_OSD_FLAG_EC_DIRECT_READ``) already correctly handle allocation
information by collecting and merging results from each shard. This existing
behavior does not require modification.

8.2. MAPEXT
-----------

**CEPH_OSD_OP_MAPEXT**

MAPEXT is the direct way to query logical extent state, similar to the POSIX
``fiemap()`` system call. It returns a map showing which byte ranges of an
object are allocated versus unallocated (holes).

**Current Behavior:**

- **Replicated pools**: Calls ``store->fiemap()`` to query BlueStore and
  returns the extent map
- **EC pools**: Returns ``-EOPNOTSUPP`` (Operation Not Supported) - MAPEXT is
  completely non-functional on EC pools

The lack of MAPEXT support on EC pools is due to:

1. Objects are split across multiple shards on different OSDs
2. No mechanism exists to query and merge allocation information across shards
3. During recovery/reconstruction, allocation information is lost

**New Behavior:**

MAPEXT is implemented for fast EC pools (those with EC optimisations enabled)
following the same rules as SPARSE_READ.  Classic EC pools (without EC
optimisations) continue to return ``-EOPNOTSUPP`` permanently, regardless of
the release gate, because they lack the shard-level fiemap infrastructure
required to construct an accurate extent map.

**Good Path:**

1. Query BlueStore allocation state on each shard
2. Merge results across shards
3. Return combined extent map

**Recovery/Reconstruction Path:**

MAPEXT reuses the same two-stage decode+rescan path as SPARSE_READ (see
section 8.1), but discards the data buffer after the rescan. Only the derived
extent map is returned to the client; no object data is transferred.

8.3. WRITE
----------

**CEPH_OSD_OP_WRITE**

**Behavior when tracking is enabled:**

1. Check if op needs zero tracking (pool has ``preserve_allocation`` set, or
   ``CEPH_OSD_FLAG_PRESERVE_ALLOCATION`` is set on the MOSDOp)
2. If zero tracking is needed, perform zero-detection on the write data in
   ``PrimaryLogPG`` before dispatching to the backend
3. Update object data as normal
4. For fast EC pools only: add any detected all-zero 4K blocks to
   ``force_allocated_extents``
5. Non-zero data can overlay existing force-allocated extents without updating
   the extent set (the zeros are now overwritten with non-zero data)
6. Persist updated OI with object

**Rationale for not removing extents on non-zero overwrite:**

Two reasons justify leaving stale ``force_allocated_extents`` entries in place
when non-zero data is written over a previously force-allocated region:

1. **Overhead**: Removing entries on every non-zero overwrite would require
   intersecting the write range against ``force_allocated_extents`` on the hot
   write path. Since the entries are harmless once the region contains non-zero
   data — they will simply cause recovery to write an allocated zero block that
   will then be immediately overwritten by the real data during normal
   operation — the cost of maintaining them precisely is not justified.

2. **Fragmentation of large zero writes**: A thick-provisioned image or a
   large initialising write may write zeros across the entire object, producing
   a single large interval in ``force_allocated_extents``. If subsequent
   partial overwrites with non-zero data were to carve out sub-ranges from that
   interval, the result would be a highly fragmented interval_set — potentially
   pushing the representation toward the bitmap, increasing OI size, and adding
   encode/decode cost on every subsequent write. Leaving the interval intact
   avoids this fragmentation entirely.

8.4. WRITEFULL
--------------

**CEPH_OSD_OP_WRITEFULL**

**Behavior when tracking is enabled:**

1. For fast EC pools only: clear all existing ``force_allocated_extents``
   (since WRITEFULL replaces all object data)
2. Update object data
3. Perform zero-detection on the new data
4. For fast EC pools only: set ``force_allocated_extents`` to cover any
   all-zero 4K blocks in the write
5. Persist updated OI

8.5. WRITESAME
--------------

**CEPH_OSD_OP_WRITESAME**

**Behavior when tracking is enabled:**

1. If writing non-zero data: no action needed on ``force_allocated_extents``
2. If writing zeros and pool is fast EC: add the written extent to
   ``force_allocated_extents``
3. Persist updated OI

8.6. TRUNCATE / TRIMTRUNC
-------------------------

**CEPH_OSD_OP_TRUNCATE, CEPH_OSD_OP_TRIMTRUNC**

**Behavior when tracking is enabled:**

1. For fast EC pools only: remove ``force_allocated_extents`` entries beyond
   the new size
2. Perform truncate operation as normal
3. Persist updated OI

8.7. ZERO
---------

**CEPH_OSD_OP_ZERO**

``CEPH_OSD_OP_ZERO`` is semantically a deallocation operation — it punches a
hole in the object, freeing the underlying storage. It is not a write of zeros.

PrimaryLogPG converts a ZERO op at the end of an object to a truncate. The EC
layer stripes the remaining ZERO range across the data shards and sends ZERO
ops down to the object store.

For parity calculation, zeroed data extents are treated as a write of zeros.
Any resulting parity that is all zeros is deallocated using the same mechanism
as normal writes.

ZERO ops are permitted at any byte alignment. EC deallocates at 4K granularity.
Any sub-4K-aligned portion of the range that cannot be deallocated is written
as literal zeros.

ZERO ops clear ``force_allocated_extents`` entries for any fully covered
4K-aligned extent.

8.8. COPY_FROM
--------------

**CEPH_OSD_OP_COPY_FROM**

COPY_FROM is a server-side copy operation that copies an object from a source
OID (potentially in a different pool) to the destination object.

**Behavior when tracking is enabled:**

Whether FAE is tracked on the destination is determined entirely by the
*destination* pool: tracking is active when the destination is a fast EC pool
(``allows_ecoptimizations()``) and the release gate has been reached.  The
source pool type and whether the source object has FAE are irrelevant.

For each chunk of data received during the copy, two paths exist:

- **Sparse chunk** (source provided an extent map): only the non-hole extents
  are written to the destination; zero-detection runs on those extents only.
  Holes in the source extent map are preserved as holes on the destination.
- **Non-sparse chunk** (Classic EC source or old peer): a single contiguous
  write; zero-detection runs on the full chunk buffer.

In both cases the detected zero-block results are accumulated in a temporary
``force_allocated_extents`` on the in-progress copy state.

When the copy completes (``finish_copyfrom``), the accumulated FAE is installed
on the destination object's OI.  FAE is never inherited from the source object.

Classic EC destination pools do not have FAE set because the
``allows_ecoptimizations()`` check on the destination fails.  Cross-pool copies
(e.g. replicated source → fast EC destination) are handled correctly because
the FAE decision is based solely on the destination pool.

This feature is gated on the *Vampire* release (see section 9).

8.9. Snapshots and Clones
-------------------------

**FAE preservation across snapshot and rollback operations**

When a selfmanaged snapshot is armed and the head object is written, the OSD
creates a clone of the head.  The clone shall inherit the head's
``force_allocated_extents`` at the moment the snapshot is taken, so that sparse
reads on the snapshot OID return the allocation state that was current at
snapshot time.

**Rollback**

Rolling back to a snapshot shall restore the ``force_allocated_extents`` from
that snapshot onto the head object.  After rollback, sparse reads on the head
OID reflect the allocation state captured at snapshot time.

9. Upgrade and Release Compatibility
=====================================

All new OSD-side behaviour introduced by this design — FAE tracking on writes,
MAPEXT support, SPARSE_READ reconstruction, and FAE handling on copy-from — is
gated behind the *Vampire* release flag (``require_osd_release >= Vampire``).

**Behaviour before Vampire is reached**

- OSDs running the new code but operating in a cluster whose
  ``require_osd_release`` is below *Vampire* behave identically to unmodified
  OSDs.  Zero detection is not performed, ``force_allocated_extents`` is never
  written to object info, and MAPEXT continues to return ``-EOPNOTSUPP`` on EC
  pools.
- Pre-Vampire OSDs will therefore never encounter the
  ``force_allocated_extents`` field in an object info record.

**Rolling-upgrade safety**

During a rolling upgrade from a pre-Vampire release, the cluster passes through
a mixed-version state.  Because all new behaviour is disabled until
``require_osd_release`` is advanced by the operator, mixed-version clusters
operate correctly throughout the upgrade:

1. New OSDs do not write ``force_allocated_extents`` until the gate opens.
2. Old OSDs never see an unknown OI field.
3. Recovery code on new OSDs checks the release gate before consulting FAE,
   so no shard will be incorrectly retained or discarded during a mixed-version
   recovery.

The gate is advanced by the operator with::

    ceph osd require-osd-release vampire

after all OSDs have been upgraded to the *Vampire* release or later.

10. Development Notes
=====================

10.1. Enabling Zero Detection
-----------------------------

Zero detection in PrimaryLogPG is triggered by either of the two mechanisms
described in section 5. Both can be used simultaneously.

**Pool property (``preserve_allocation``)**

A pool-level boolean property that enables zero detection for all writes to
all objects in the pool. This is a coarse-grained mechanism: it applies to
every write regardless of which client or image issued it. It is appropriate
when all workloads on the pool require zero tracking, or when deploying
without client changes.

**MOSDOp flag (``CEPH_OSD_FLAG_PRESERVE_ALLOCATION``)**

A flag on the outer ``MOSDOp`` message, set by the client on individual
requests. PrimaryLogPG checks for this flag in ``m->get_flags()`` when
processing a write operation. If present, zero detection is performed for that
request regardless of pool configuration. This is a fine-grained mechanism:
only writes from clients that explicitly set the flag are tracked, avoiding
unnecessary overhead and ``force_allocated_extents`` accumulation for
unencrypted workloads sharing the same pool.

**RBD implementation requirements**

Once sparse read support is implemented, the following RBD client changes are
needed:

1. **EC pool validation**: When enabling encryption (``rbd encryption format``),
   verify that the EC data pool has ``preserve_allocation`` set. Return a clear
   error if not.

2. **CLI guidance**: Update ``rbd encryption format`` to check pool
   configuration and provide helpful error messages guiding users to enable
   the property.

**Documentation**

Full user-facing documentation of how to enable each mechanism (pool property
CLI syntax, librados API for setting the MOSDOp flag, and client-specific
guidance for RBD and CephFS) will be provided in a separate documentation
update once the feature is implemented.

10.2. Block Size and Zero-Detection Algorithm
---------------------------------------------

The implementation uses 4K block granularity, aligned with EC operations,
LUKS2, and fscrypt block sizes.

Zero detection uses the existing ``mem_is_zero()`` implementation. Improving
the zero-detection algorithm (e.g. using platform-optimised SIMD routines) is
a separate concern and out of scope for this design.

10.3. Testing Requirements
--------------------------

The primary unit test suite for this feature is
``ceph_test_rados_api_sparse_reads_pp``, built from
``src/test/librados/sparse_reads_cxx.cc``.  It covers FAE tracking, MAPEXT,
SPARSE_READ, copy-from, snapshots, and the per-request flag across replicated
and fast EC pools.  It is run as part of the RADOS API workunit
(``qa/workunits/rados/test.sh``).

**Most tests can be done without encryption** by using regular writes of zero
data and verifying allocation state is preserved through recovery.

**Test Cases:**

1. **Basic zero-block preservation:**

   - Write zeros to EC pool object with tracking enabled
   - Trigger recovery
   - Verify zeros remain allocated (sparse read reports them as allocated)

2. **Mixed zero and non-zero data:**

   - Write pattern of zeros and non-zero data
   - Trigger recovery
   - Verify allocation state matches original pattern

3. **Overwrite scenarios:**

   - Write zeros (become force-allocated)
   - Overwrite with non-zero data
   - Trigger recovery
   - Verify non-zero data is preserved, force-allocated extent removed

4. **WRITEFULL clears tracking:**

   - Write zeros (become force-allocated)
   - WRITEFULL with non-zero data
   - Verify force-allocated extents cleared

5. **ZERO operation deallocates storage:**

   - Write non-zero data to a slice-aligned region
   - Issue ZERO covering the complete region
   - Verify the region is reported as unallocated (hole) by sparse read
     without triggering recovery — deallocation must happen immediately
   - Write non-zero data spanning a partial slice at each edge
   - Issue ZERO covering a range that leaves partial slices at both ends
   - Verify data shards for the zeroed portion are deallocated and parity
     shards are updated (not deallocated) for the partial-slice edges
   - Write zeros to a region (become force-allocated via tracking)
   - Issue ZERO on the same region
   - Verify ``force_allocated_extents`` entry is removed
   - Trigger recovery and verify region is reported as unallocated

6. **TRUNCATE removes extents:**

   - Write zeros at various offsets
   - Truncate to smaller size
   - Verify force-allocated extents beyond truncate point removed

7. **MAPEXT returns correct allocation:**

   - Write pattern of zeros and non-zero data
   - Query with MAPEXT
   - Verify extent map matches write pattern

8. **SPARSE_READ returns correct allocation:**

   - Write pattern of zeros and non-zero data
   - Read with SPARSE_READ
   - Verify extent map matches write pattern

9. **Recovery path sparse reads:**

   - Write zeros
   - Trigger recovery
   - Perform sparse read during recovery
   - Verify zeros reported as allocated

10. **Direct reads with force-allocated extents:**

    - Write zeros to object spanning multiple shards
    - Trigger recovery on subset of shards
    - Perform direct read
    - Verify allocation state correctly merged across shards

11. **Performance benchmarks:**

    - Measure write path overhead with tracking enabled
    - Compare encrypted vs non-encrypted workloads
    - Verify < 1% impact for encrypted workloads

12. **Two-stage zero detection validation:**

    - Write 4K block with first byte zero, second byte non-zero
    - Verify block NOT marked as force-allocated (quick check fails)
    - Write 4K block with first 8 bytes zero, remaining bytes non-zero
    - Verify block NOT marked as force-allocated (full check fails)
    - Write 4K block with first 8 bytes zero, remaining bytes zero
    - Verify block IS marked as force-allocated (both checks pass)
    - Write 4K block with all bytes zero
    - Verify block IS marked as force-allocated

13. **Stress tests:**

    - Large objects with various zero-block patterns
    - Concurrent recovery and writes
    - Multiple recovery cycles
    - Verify allocation state remains consistent

14. **ZERO op coverage in ceph_test_rados_io_sequence:**

    ``ceph_test_rados_io_sequence`` exercises random sequences of RADOS
    operations against EC and replicated pools and verifies correctness.
    The ZERO operation must be added to the operation set so that the
    following edge cases are exercised automatically:

    - **ZERO combined with writes in the same op**: include ZERO and WRITE
      sub-ops in the same ``MOSDOp`` to verify correct ordering and that
      allocation state after the compound op is consistent.

    - **Misaligned ZERO**: issue ZERO ops whose start and/or end are not
      4K-aligned, verifying that sub-4K head and tail regions are written
      with literal zeros while the 4K-aligned interior is deallocated.

    - **Multiple ZERO ops in one sequence step**: issue several ZERO ops
      covering overlapping or adjacent ranges within a single compound op,
      verifying that the combined deallocation is handled correctly.

    - **ZERO combined with TRUNCATE**: issue a ZERO followed by a TRUNCATE
      (or vice versa) within the same op sequence step, verifying that
      ``force_allocated_extents`` entries and physical allocation are both
      updated correctly when the two operations interact at range boundaries.

11. Features NOT Implemented
=============================

The following features are explicitly out of scope for this design:

**Thick Provisioning Support**

Thick provisioning (``rbd --thick-provision``) requires that all storage is
physically allocated upfront and remains allocated even if zeros are written
back into the image. This design does not support that guarantee: ZERO ops
deallocate storage (via zero detection on encoded shard extents), so any
subsequent ZERO issued against a thick-provisioned image would reclaim the
reserved space. Thick provisioning on EC pools is out of scope for this design.

**Alternative Block Size Support**

Only 4K block size is supported. Legacy 512-byte block sizes (LUKS1) are not
supported. Supporting 512-byte blocks would require:

1. Zero detection at 512-byte granularity
2. Preventing EC read-modify-write from growing 512-byte allocated blocks to
   4K
3. More complex extent tracking

This can be added in a future iteration if there is demand.

12. Optional Performance Improvement: ISA-L Zero Detection
==========================================================

``mem_is_zero()`` currently uses a portable C implementation with compile-time
platform selection. An optional performance improvement is to replace it with
calls into the ISA-L ``isal_zero_detect()`` function, which provides
runtime-dispatched SIMD implementations (AVX-512, AVX2, AVX, SSE on x86-64;
NEON and SVE variants on aarch64).

**Pre-check on the first ``uint64_t``**

Before calling ``isal_zero_detect()``, test the first 8 bytes of the buffer.
If they are non-zero the block can be rejected immediately without invoking
ISA-L at all. This pre-check is essentially free for two reasons:

- The memory access is not wasted: loading the first 8 bytes brings the first
  cache line into L1, which ISA-L will read anyway for the all-zero case. The
  pre-check simply makes use of a cache line that was going to be fetched
  regardless.

- It is a correctness requirement for some ISA-L implementations. Certain
  aarch64 implementations in ISA-L require a minimum buffer size (greater than
  64 bytes) and assume the caller will not pass a trivially non-zero buffer.
  The pre-check on the first ``uint64_t`` acts as a guard that makes it safe
  to call ``isal_zero_detect()`` unconditionally for any buffer that passes
  the initial test.

For encrypted workloads — the primary use case — the probability of the first
8 bytes being zero is negligible, so almost all blocks are rejected at the
pre-check stage with a single comparison.

This improvement will be implemented if performance measurements indicate that
``mem_is_zero()`` is a bottleneck in the zero-detection path. It will be
delivered under an independent PR, separate from the main sparse read
implementation. It applies to all callers of ``mem_is_zero()`` throughout
Ceph, not just the zero-detection path introduced here.

13. Rejected Implementation Approaches
=======================================

13.1. Client-Side Zero Detection
---------------------------------

**Approach:**

Add a new operation ``CEPH_OSD_OP_WRITE_ZERO_DETECTED`` where the client
performs zero-detection and passes an interval_set of zero-block extents along
with the write data.

**Advantages:**

- Data is hot in CPU cache immediately after client processing (e.g., after
  encryption)
- Only checks when client determines it's necessary
- Can be optimized per client type

**Disadvantages:**

- Requires new OSD operation
- Each client type needs separate implementation (RBD, CephFS, etc.)
- Adds complexity to client code
- Does not benefit future RADOS clients automatically

**Rejection Rationale:**

PrimaryLogPG detection provides a centralized implementation that benefits all
current and future RADOS clients via a single code path shared between EC and
replicated pools. This also positions zero detection as a shared building block
for future optimisations that are not specific to EC.

13.2. Full Allocation Tracking
-------------------------------

**Approach:**

Instead of tracking force-allocated extents (regions with explicitly written
zeros), track all allocated regions in an interval_set in the OI.

**Advantages:**

- Simpler conceptual model (just track what's allocated)
- No zero-detection overhead
- Works equally well for all workload types

**Disadvantages:**

- Larger metadata overhead (interval_set typically non-empty)
- Must update interval_set on every write operation
- More complex interval_set management (merging, splitting)
- Larger OI size for most objects

**Rejection Rationale:**

The primary use case is encryption where zeros are pathologically rare. Force-
allocated extent tracking optimizes for this case with minimal metadata
overhead. Zero-detection is highly optimized (ISA-L, early exit) and operates
on data already in CPU cache from EC encoding.
