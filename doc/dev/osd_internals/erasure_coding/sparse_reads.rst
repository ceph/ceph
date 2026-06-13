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

2. Current State
================

EC pools currently have the following limitations:

**Sparse Reads**

- EC reads treat all reads as fully allocated, transferring data for the
  entire requested range regardless of actual allocation state

**MAPEXT Operation**

- MAPEXT is not supported on EC pools (returns ``-EOPNOTSUPP``)

**Zero-Block Handling During Recovery**

During recovery and reconstruction, EC pools cannot distinguish between:

- Zeros that were explicitly written by a client
- Regions that were never allocated

**Note:** This is a fundamental property of erasure coding mathematics, not an
implementation shortcoming. When data is reconstructed from parity chunks, the
erasure coding algorithm can only regenerate the data values - it has no
information about whether a block of zeros was explicitly written or never
allocated. Additional metadata is required to preserve this distinction.

EC recovery currently:

1. Reads available shards
2. Reconstructs missing data using erasure coding
3. Detects blocks of zeros in reconstructed data
4. Marks zero blocks as unallocated to save space

This means:

1. A client that writes zeros may later find those regions reported as
   unallocated during sparse reads
2. Allocation state is not preserved across recovery/reconstruction
3. Clients cannot rely on sparse reads returning accurate allocation
   information

**Impact**

This limitation prevents use cases that require guaranteed allocation
preservation, such as encryption (where data may encrypt to all zeros) and
other applications that depend on accurate logical allocation state.

3. Problem Statement
====================

The requirement is to provide:

1. **Sparse Read Support**: Enable EC pools to accurately report allocation
   state during sparse reads by querying BlueStore and correctly merging
   results across shards

2. **Logical Allocation Preservation**: Track which 4K-aligned blocks contain
   explicitly written zeros, ensuring they remain allocated through recovery
   and reconstruction

The core technical challenge is that EC pools maintain allocation information
at 4K resolution during normal operations, but this information is lost during
recovery/reconstruction when data is regenerated from coding parities.

Without these mechanisms, clients that rely on accurate logical allocation
information cannot use EC pools reliably.

**Primary Use Case: Encryption**

The primary use case for this feature is encrypted data (RBD with LUKS,
CephFS with fscrypt). For encrypted workloads, the probability of a 4K block
encrypting to all zeros is pathologically low (theoretically 1 in 2^(4096*8)).
This design optimizes for this property - assuming that genuine blocks of
zeros are extremely rare, allowing for efficient metadata storage and fast
zero detection with early exit.

4. Proposed Solution
====================

The solution tracks which 4K-aligned blocks contain all zeros when written,
stores this information in object metadata (``force_allocated_extents``), and
uses it during recovery/reconstruction to preserve the distinction between
allocated-zeros and unallocated regions.

**Key Design Decisions:**

1. **OSD-side zero detection**: The OSD performs zero detection on write
   operations when tracking is enabled. This benefits all RADOS clients (RBD,
   CephFS, future clients) with a centralized implementation.

2. **Pool flag**: A pool flag (``FLAG_TRACK_ZERO_BLOCKS``) enables tracking for
   all objects in the pool. Set via ``ceph osd pool set <pool> set_pool_flags
   <flag_value> --yes-i-really-mean-it``.

3. **4K block granularity**: Aligned with EC operations, LUKS2, and fscrypt
   block sizes.

4. **Optimized for encryption**: The design assumes zeros are rare (as in
   encrypted data), using a two-stage detection algorithm with early exit.

5. Metadata Storage
===================

**Object Info Extension**

Force-allocated extents are stored in ``object_info_t`` as an
``interval_set<uint64_t>``:

.. code-block:: cpp

    struct object_info_t {
      // ... existing fields ...
      interval_set<uint64_t> force_allocated_extents;  // 4K-aligned extents
      // ... existing fields ...
    };

**Storage Characteristics**

- Empty interval_set requires minimal storage (just a flag bit in OI)
- For encrypted workloads, probability of producing all-zero 4K blocks is
  pathologically low (theoretically 1 in 2^(4096*8))
- For other workloads, zero-blocks are relatively rare though more common than
  encrypted data
- In practice, the interval_set will be empty most of the time
- interval_set provides efficient storage for the rare cases with zero blocks

**Note:** While object_info_t is generally suitable for this metadata, if
extensive force-allocated extent tracking becomes necessary, alternative
storage mechanisms should be evaluated.

6. Recovery Rules
=================

During EC recovery and backfill, the following rules apply:

**If a block is force-allocated according to OI:**

- If data has been recovered for the shard, it will be written as-is (no
  zero-detect)
- If no data has been recovered, a zero block will be written

**Otherwise (block not in force_allocated_extents):**

- If recovered block is all zeros, deallocate it
- Otherwise write it as-is

**Code Locations**

The following areas need modification:

- ``ECBackend::RecoveryBackend::continue_recovery_op()`` - recovery path
- ``ECCommon::ReadPipeline::objects_read_and_reconstruct()`` - reconstruction
  path
- Shard-level recovery operations in ``ECBackendL.cc`` and ``ECBackend.cc``

**Implementation Note**

Current code in ``src/osd/ECBackend.cc`` around line 670 has a comment about
zero-padding during recovery. This area and similar code paths need careful
review to ensure zero-block metadata is properly consulted.

7. Modified RADOS Operations
=============================

7.1. SPARSE_READ
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

**Good Path (No Recovery):**

1. Query BlueStore for physical allocation on each shard
2. Merge allocation results across shards
3. Return allocation state directly from BlueStore
4. No need to consult ``force_allocated_extents`` - BlueStore already
   maintains correct allocation state

**Recovery/Reconstruction Path:**

1. Query BlueStore for physical allocation
2. Consult ``force_allocated_extents`` from OI
3. Return extent map where:

   - Extents in ``force_allocated_extents`` are marked as allocated
   - Other allocated extents are marked as allocated
   - Unallocated extents are marked as unallocated

**Note:** The ``force_allocated_extents`` metadata is only required during
recovery and reconstruction operations. During normal operations, BlueStore
maintains allocation state correctly, so sparse reads can query BlueStore
directly without consulting the force-allocated extent map.

**Note on Direct Reads:** Direct EC reads (using
``CEPH_OSD_FLAG_EC_DIRECT_READ``) already correctly handle allocation
information by collecting and merging results from each shard. This existing
behavior does not require modification.

7.2. MAPEXT
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

MAPEXT will be implemented for EC pools following the same rules as
SPARSE_READ:

**Good Path:**

1. Query BlueStore allocation state on each shard
2. Merge results across shards
3. Return combined extent map

**Recovery/Reconstruction Path:**

1. Query BlueStore allocation state
2. Consult ``force_allocated_extents`` from OI
3. Merge results ensuring force-allocated extents are marked as allocated

7.3. WRITE
----------

**CEPH_OSD_OP_WRITE**

**Behavior when tracking is enabled:**

1. Check if pool has ``FLAG_TRACK_ZERO_BLOCKS`` flag set
2. If pool flag is set, perform zero-detection on all writes to the pool
3. Update object data as normal
4. Perform zero-detection on write data using two-stage algorithm:

   a. **Quick check**: Test first 8 bytes (uint64_t) of each 4K block
   b. **Full check**: If first 8 bytes are zero, use optimized
      ``mem_is_zero()`` function (leverages ISA-L on x86_64) to verify entire
      block

5. Add any detected all-zero 4K blocks to ``force_allocated_extents``
6. Non-zero data can overlay existing force-allocated extents without updating
   the extent set (the zeros are now overwritten with non-zero data)
7. Persist updated OI with object

**Rationale for two-stage detection:**

For encryption workloads (and other very-low-probability-of-zeros cases), the
quick check of the first 8 bytes will almost always fail (non-zero), providing
immediate early exit. This is faster than calling even highly optimized
functions like ``mem_is_zero()``. Only when the first 8 bytes are zero (rare)
do we need the full ISA-L optimized check.

**Performance Note:**

Since EC encoding already requires the CPU to read all the data to compute
parity chunks, zero detection reads data that is already in CPU cache from the
encoding operation. This is why the performance impact is minimal - we're not
adding a new data read, just an additional check on data that's already been
loaded.

For EC read-modify-write (RMW) operations, zero detection should be performed
immediately after the read phase completes and just before the new data is
merged into the read buffer. This timing ensures the read data is still hot in
CPU cache while allowing detection to occur on the complete buffer that will be
written (after merging new data with existing data).

7.4. WRITEFULL
--------------

**CEPH_OSD_OP_WRITEFULL**

**Behavior when tracking is enabled:**

1. Clear all existing ``force_allocated_extents`` (since WRITEFULL replaces
   all object data)
2. Update object data
3. Perform zero-detection on the new data
4. Set ``force_allocated_extents`` to cover any all-zero 4K blocks in the
   write
5. Persist updated OI

7.5. WRITESAME
--------------

**CEPH_OSD_OP_WRITESAME**

**Behavior when tracking is enabled:**

1. If writing non-zero data: no action needed on ``force_allocated_extents``
2. If writing zeros: add the written extent to ``force_allocated_extents``
3. Persist updated OI

7.6. TRUNCATE / TRIMTRUNC
-------------------------

**CEPH_OSD_OP_TRUNCATE, CEPH_OSD_OP_TRIMTRUNC**

**Behavior when tracking is enabled:**

1. Remove ``force_allocated_extents`` beyond the new size
2. Perform truncate operation as normal
3. Persist updated OI

7.7. ZERO
---------

**CEPH_OSD_OP_ZERO**

**Behavior when tracking is enabled:**

1. Remove ``force_allocated_extents`` for the zeroed regions
2. Perform zero operation (explicitly deallocates data and writes zeros)
3. Persist updated OI

**Rationale:** ZERO operations explicitly deallocate storage, so these regions
should not be preserved as allocated during recovery.

8. Development Notes
====================

8.1. Pool Flag
--------------

Zero-block tracking is enabled per-pool using the ``FLAG_TRACK_ZERO_BLOCKS``
pool flag. This flag can be set using:

.. code-block:: bash

   ceph osd pool set <pool> set_pool_flags <flag_value> --yes-i-really-mean-it

Where ``<flag_value>`` is the numeric value of ``FLAG_TRACK_ZERO_BLOCKS``.

**Note:** This is a development tool and requires the ``--yes-i-really-mean-it``
confirmation flag.

8.2. Block Size
---------------

The implementation uses 4K block granularity, aligned with EC operations,
LUKS2, and fscrypt block sizes. ISA-L zero detection is optimized for 4K
blocks.

8.3. Testing Requirements
-------------------------

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

5. **ZERO operation deallocates:**

   - Write zeros (become force-allocated)
   - ZERO operation on same region
   - Trigger recovery
   - Verify region is deallocated (not force-allocated)

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

9. Client Configuration Requirements
=====================================

To use encryption on EC pools, clients must be configured appropriately:

9.1. RBD Configuration
----------------------

**Current State**

There is no explicit code restriction preventing RBD encryption on EC pools.
However, encryption does not work correctly because:

1. EC sparse reads always report all data as allocated (even unallocated regions)
2. During recovery, EC cannot distinguish explicitly-written zeros from
   unallocated regions
3. This causes data corruption for encrypted images where decrypted zeros get
   deallocated

**Note on Journaling**: RBD encryption currently blocks images with journaling
enabled (see ``librbd/crypto/LoadRequest.cc:57-61``). This is a separate
restriction unrelated to EC pools - it applies to all pool types. Addressing
the journaling restriction is out of scope for this design.

**Required Changes**

Once sparse read support is implemented, the following client-side changes are
needed:

1. **Add EC pool validation**: When enabling encryption, verify that EC data
   pools have the ``FLAG_TRACK_ZERO_BLOCKS`` flag set. Return a clear error if
   not.

2. **CLI support**: Update ``rbd encryption format`` to check pool
   configuration and provide helpful error messages guiding users to enable the
   flag.

**Configuration Steps**

To enable RBD encryption on an EC pool:

1. Enable zero-block tracking on the EC pool::

     ceph osd pool set <ec-pool> set_pool_flags 4194304 --yes-i-really-mean-it

2. Create or format the RBD image with encryption::

     rbd create --size 10G --data-pool <ec-pool> mypool/myimage
     rbd encryption format mypool/myimage luks2 passphrase.txt

3. Load encryption when opening the image::

     rbd encryption load mypool/myimage luks2 passphrase.txt

9.2. CephFS Configuration
--------------------------

**Current State**

CephFS fscrypt operates at the MDS level and does not have explicit EC pool
restrictions. The fscrypt implementation works with file metadata and should
function transparently with EC pools that have sparse read support.

**Verification Required**

While no code changes are expected for CephFS, the following must be verified:

1. fscrypt file encryption/decryption works correctly on EC pools
2. Sparse reads return correct allocation information for encrypted files
3. Performance is acceptable for typical workloads

**Configuration Steps**

To use CephFS fscrypt on an EC pool:

1. Enable zero-block tracking on the EC data pool::

     ceph osd pool set <ec-pool> set_pool_flags 4194304 --yes-i-really-mean-it

2. Configure CephFS to use the EC pool as a data pool

3. Enable fscrypt on directories as normal - no special configuration needed

9.3. Other Clients
------------------

RGW and other RADOS clients are not explicitly supported in this design. While
the OSD-side changes are client-agnostic, client-specific validation and
configuration may be needed for other use cases.

10. Features NOT Implemented
=============================

The following features are explicitly out of scope for this design:

**Thick Provisioning Support**

The ``--thick-provision`` feature for RBD will be supported through the
WRITESAME operation handling. When RBD creates a thick-provisioned image, it
uses WRITESAME to write zeros across the entire image, and these zeros will be
tracked as force-allocated extents, preventing deallocation during recovery.

However, this design does not prevent BlueStore compression or other storage
optimizations that may affect thick provisioning guarantees at the storage
layer. Additionally, extensive testing of thick provisioning scenarios is not
planned as part of this work - the primary focus is on encryption use cases.

**Alternative Block Size Support**

Only 4K block size is supported. Legacy 512-byte block sizes (LUKS1) are not
supported. Supporting 512-byte blocks would require:

1. Zero detection at 512-byte granularity
2. Preventing EC read-modify-write from growing 512-byte allocated blocks to
   4K
3. More complex extent tracking

This can be added in a future iteration if there is demand.

11. Rejected Implementation Approaches
=======================================

11.1. Client-Side Zero Detection
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

OSD-side detection provides a centralized implementation that benefits all
current and future RADOS clients. The performance difference is minimal since
EC encoding already requires the CPU to read the data, so zero detection
operates on data that's already in CPU cache.

11.2. Full Allocation Tracking
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

.. Made with Bob
