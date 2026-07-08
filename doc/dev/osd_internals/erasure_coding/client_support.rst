======================================================
Support for RBD, RGW and CephFS in Erasure Coded Pools
======================================================

Introduction
============

This document covers the design for enabling omap (object map) support
and synchronous read operations in erasure-coded pools.
These enhancements enable EC pools to support Cls methods, as well as
RBD, RGW, and CephFS workloads without the need for a separate replica
pool for metadata.

Current Limitations
-------------------

Erasure-coded pools have previously been limited in their support for
metadata operations. Specifically:

- **Omap operations** (key-value metadata storage on objects) were not
  supported, limiting the use of EC pools for workloads requiring metadata.
- **Cls operations** (server-side object class methods) were not available,
  preventing RBD and other advanced features from working with EC pools.
- **Synchronous read operations** were not implemented in the EC backend,
  which are required for Cls operations to function correctly.

These limitations prevented EC pools from being used for many important
workloads, particularly RBD (RADOS Block Device) which relies heavily on both
omap and Cls operations.

Feature Relationships
---------------------

The two main features in this design are independent but complementary:

- **Omap Support**: Enables key-value metadata storage on EC pool objects
  through replication across primary-capable shards with journal-based updates
  managed by the primary OSD.
- **Synchronous Reads**: Provides synchronous read semantics in the EC backend
  using Boost pull-type coroutines, enabling synchronous operations without
  blocking threads.

Together, these features enable full support for RBD, RGW and CephFS
on erasure-coded pools.


Omap Support for EC Pools
==========================

Current Limitations
-------------------

In the original EC pool implementation, omap operations were not supported due
to the complexity of maintaining consistent key-value metadata across erasure-
coded shards. Unlike replicated pools where each replica maintains a complete
copy of the omap data, EC pools distribute data across multiple shards, making
metadata management more complex.

The primary challenges include:

- Ensuring consistency of metadata across shards
- Handling partial updates and failures
- Maintaining performance for metadata operations
- Supporting recovery and reconstruction scenarios

Design Approach
---------------

The omap implementation for EC pools uses a replication-based approach:

- Omap data is **replicated** across all primary-capable shards in a PG
- A **journal** is used to store omap updates before they are committed
- Updates are applied atomically across all primary-capable shards
- Consistency is maintained through the journal commit protocol

This approach provides:

- Strong consistency guarantees for metadata operations
- Efficient recovery through journal replay
- Compatibility with existing omap APIs
- Minimal impact on data path performance

Omap Architecture
-----------------

Shard Distribution and Primary-Capable Shards
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In an erasure-coded pool with k data shards and m parity shards, the
primary-capable shards are:

- The first data shard
- All m parity shards

This means there are **m + 1 primary-capable shards** in total. For example,
in a k=4, m=2 configuration, shards 0, 4, and 5 are primary-capable.

.. ditaa::

   EC Pool (k=4, m=2)

   +--------+  +--------+  +--------+  +--------+  +--------+  +--------+
   | Shard  |  | Shard  |  | Shard  |  | Shard  |  | Shard  |  | Shard  |
   |   0    |  |   1    |  |   2    |  |   3    |  |   4    |  |   5    |
   | (Data) |  | (Data) |  | (Data) |  | (Data) |  | (Parity|  | (Parity|
   |PRIMARY |  |        |  |        |  |        |  |PRIMARY |  |PRIMARY |
   |CAPABLE |  |        |  |        |  |        |  |CAPABLE |  |CAPABLE |
   +--------+  +--------+  +--------+  +--------+  +--------+  +--------+
       |                                               |           |
       v                                               v           v
   +--------+                                      +--------+  +--------+
   |  Omap  |                                      |  Omap  |  |  Omap  |
   | Replica|                                      | Replica|  | Replica|
   +--------+                                      +--------+  +--------+

   Primary-capable shards (0, 4, 5) maintain omap replicas

Each primary-capable shard maintains a complete copy of the omap data for
objects in the PG. This replication ensures that omap data remains available
even if some shards fail, and allows any primary-capable shard to serve omap
read requests when acting as primary.

Journal Implementation
~~~~~~~~~~~~~~~~~~~~~~

The ECOmapJournal is maintained on the primary OSD. The journal:

- Records all omap updates before they are applied
- Ensures atomic application of updates across all primary-capable replicas
- Enables recovery in case of failures during update operations
- Provides a consistent view of omap state during recovery
- Handles object deletion and recreation

Object State Map
^^^^^^^^^^^^^^^^

The journal maintains an ``object_state_map`` to track objects that are in the
process of being deleted. This map is critical for ensuring that omap updates
are written to the correct object generation when objects are deleted and
recreated.

The object_state_map:

- **Tracks outstanding deletes**: When a delete operation is appended to the
  journal, the object's version number is added to the map along with a boolean
  indicating whether it's a lost delete
- **Manages version lifecycle**: The version number remains in the map until
  the delete is trimmed from the PG log
- **Determines generation numbers**: The version number is used to calculate
  the generation number for any outstanding omap updates, ensuring updates are
  applied to the correct object generation
- **Handles object recreation**: If an object is deleted and then recreated
  before the delete is trimmed, the map ensures omap updates target the
  appropriate generation

When ``get_generation()`` is called for an object, it returns:

- The lowest version number from the object_state_map if any deletes are
  outstanding
- A boolean indicating whether the delete was lost
- ``NO_GEN`` if no deletes are outstanding for the object

This mechanism prevents omap updates from being applied to the wrong generation
of an object, which could occur if an object is deleted and recreated while
omap updates are still in flight.

**Operations Using append_delete/trim_delete:**

The object_state_map's ``append_delete`` and ``trim_delete`` sequence is used
by several operations that involve object deletion and recreation:

- **Explicit deletes**: Direct object deletion operations
- **REPLACE operations**: ``copy_from`` operations that atomically delete and
  recreate objects (see below)
- **Clone operations to non-snapshot objects**: When cloning to a non-snapshot
  object, the target object is effectively deleted and recreated with the
  cloned content, requiring the same generation tracking as other
  delete-and-recreate operations

All of these operations follow the same pattern: a delete is appended to the
object_state_map when the operation is logged, and the version is trimmed when
the PG log entry is eventually removed. This ensures consistent generation
tracking regardless of which operation causes the object lifecycle transition.

**Clone Operations and Outstanding Omap Updates:**

Clone operations require special handling to ensure that outstanding omap
updates are properly applied to the cloned object. When a clone operation is
performed:

#. A visitor pattern is used to traverse the PG log and accumulate all
   outstanding omap updates for the source object
#. These accumulated omap updates are collected from journal entries that have
   not yet been applied to the object store
#. All accumulated updates are then applied to the clone transaction, ensuring
   the cloned object receives the complete, up-to-date omap state
#. This process ensures that the clone includes not just the omap data from the
   object store, but also any in-flight updates that exist only in the journal

This visitor-based approach is necessary because the journal may contain omap
updates that have been logged but not yet applied to the source object's
persistent storage. Without accumulating these updates, the clone would have
stale omap data, missing recent modifications. By applying all outstanding
updates to the clone transaction, the system ensures that clones are created
with a consistent and complete view of the object's omap state, including both
persisted data and in-flight journal entries.

**Trimmer Architecture for Post-Removal Scenarios:**

The EC omap implementation uses a trimmer to determine when to apply journal
updates to the underlying BlueStore. However, a special case arises when an
object has just been deleted: we must avoid applying omap updates to an object
that no longer exists. To handle this, a specialized trimmer architecture has
been implemented:

- **TrimmerPostRemove**: A base class that performs all standard trimming
  actions except EC omap operations. This trimmer handles PG log cleanup
  without attempting to apply omap updates to the object store.

- **Trimmer**: Inherits from TrimmerPostRemove and overrides the ``ec_omap``
  method to add EC omap update application. This is the standard trimmer used
  during normal PG log maintenance.

- **trim_after_remove()**: A function that uses TrimmerPostRemove to trim the
  PG log immediately after an object has been removed. This is called in
  PGLog.h after a remove operation (``rollbacker->trim(i)``).

The key insight is that when trimming after a remove operation, we want to
clean up the PG log entries but we must not apply any omap updates from those
entries to BlueStore, since the object has just been deleted. By using
TrimmerPostRemove (which skips the ``ec_omap`` step), the system ensures that:

#. PG log entries are properly trimmed after object removal
#. Omap updates from those entries are not applied to the now-deleted object
#. The journal's object_state_map is updated appropriately via trim_delete
#. Normal trimming (using the full Trimmer class) continues to apply omap
   updates for objects that still exist

This design prevents attempting to write omap data to deleted objects while
maintaining proper journal cleanup and state tracking.

REPLACE Operation Type
^^^^^^^^^^^^^^^^^^^^^^

To properly support the object_state_map mechanism, a new ``REPLACE`` operation
type has been added to ``pg_log_entry_t``. This operation type is used in place
of the ``MODIFY`` operation type for ``copy_from`` operations where an object
is deleted and recreated.

**Why REPLACE is Necessary:**

The ``copy_from`` operation atomically deletes an existing object and recreates
it with new content. Without the REPLACE operation type, this would be logged
as a simple MODIFY operation, which would not trigger the journal to track the
deletion in the object_state_map. This creates a critical problem:

- If omap updates are in flight when a ``copy_from`` occurs, they could be
  applied to the wrong generation of the object
- The journal would not know that the object was deleted and recreated
- Outstanding omap updates would target the old generation, causing data
  corruption or inconsistency

**How REPLACE Works:**

When a REPLACE operation is logged:

#. The journal appends a delete to the object_state_map, recording the version
   number
#. This ensures that any outstanding omap updates will use the correct
   generation number
#. The object is then recreated with new content
#. When the delete is eventually trimmed from the PG log, the version is
   removed from the object_state_map

This approach ensures that ``copy_from`` operations, which are commonly used
for object cloning and migration, correctly interact with the omap journal's
generation tracking mechanism. Without REPLACE, the object_state_map would not
be aware of the implicit delete, leading to potential data corruption when omap
updates are applied to recreated objects.

Two-Phase Update Design
^^^^^^^^^^^^^^^^^^^^^^^

For EC pools, omap updates are persisted in PG log entries first and are then
applied to the object store once all copies have been updated and the transaction
can no longer be rolled back. This two-phase update approach is more efficient
than reading and saving the old omap data in case the transaction has to be
rolled back.

To avoid omap reads having to search PG log entries for recent updates, the
ECOmapJournal tracks updates that have not yet been applied to the object store
in memory. The ECOmapJournal provides a fast way of locating recent omap updates,
ensuring efficient read operations while updates are in flight.

Journal entries contain:

- List of omap Updates:
    - Operation type (set, remove, clear)
    - Bufferlist containing details about the operation (e.g. key/value pairs)
- An optional omap header
- A 'clear omap' boolean
- The object version

The journals on primary-capable shards that are not the primary shard store
the object deletion information, but not the omap updates. This allows for
updates to be committed to the correct object generation.

Journal Persistence and Peering
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ECOmapJournal does not need to be persistent because the updates are also
stored in the PG log entries. The journal is short-lived and volatile, containing
only entries for in-flight writes that are updating an omap. Whenever a new peering
interval starts, the journal is discarded. After any disruption, the Peering process
will roll forward or backward each outstanding entry in the PG log so the object
store will be up to date, eliminating the need for complicated reconciliation of the
log and journal.

Commit Protocol
~~~~~~~~~~~~~~~

The omap commit protocol ensures that updates are applied consistently across
all primary-capable shards. The protocol is divided into two phases: Apply
Update and Complete Update.

Apply Update Phase
^^^^^^^^^^^^^^^^^^

This is entered when the PG needs to apply omap updates. The primary adds the updates
to its journal and replicates them via the PG log:

.. ditaa::

   Primary                    Primary-Capable           Primary-Capable
                                 Shard 1                   Shard 2
      |                             |                          |
      | Store in PG Log             |                          |
      |------+                      |                          |
      |      |                      |                          |
      |<-----+                      |                          |
      |                             |                          |
      | Add to Journal              |                          |
      |------+                      |                          |
      |      |                      |                          |
      |<-----+                      |                          |
      |                             |                          |
      | Send PG Log Entry           |                          |
      |---------------------------->|                          |
      |                             |                          |
      | Send PG Log Entry           |                          |
      |------------------------------------------------------->|
      |                             |                          |
      |                             | Store in PG Log          |
      |                             |------+                   |
      |                             |      |                   |
      |                             |<-----+                   | Store in PG Log
      |                             |                          |------+
      |                         ACK |                          |      |
      |<----------------------------|                          |<-----+
      |                             |                          |
      |                             |                      ACK |
      |<-------------------------------------------------------|
      |                             |                          |

Complete Update Phase
^^^^^^^^^^^^^^^^^^^^^

Triggered later (during PG log trim). The primary applies updates to
object stores and coordinates completion across all shards:

.. ditaa::

   Primary                    Primary-Capable           Primary-Capable
                                 Shard 1                   Shard 2
      |                             |                          |
      | Apply to Object Store       |                          |
      |------+                      |                          |
      |      |                      |                          |
      |<-----+                      |                          |
      |                             |                          |
      | Remove Journal Entry        |                          |
      |------+                      |                          |
      |      |                      |                          |
      |<-----+                      |                          |
      |                             |                          |
      | Complete Update             |                          |
      |---------------------------->|                          |
      |                             |                          |
      | Complete Update             |                          |
      |------------------------------------------------------->|
      |                             |                          |
      |                             | Apply to Store           |
      |                             |------+                   |
      |                             |      |                   |
      |                             |<-----+                   | Apply to Store
      |                             |                          |------+
      |                         ACK |                          |      |
      |<----------------------------|                          |<-----+
      |                             |                          |
      |                             |                      ACK |
      |<-------------------------------------------------------|
      |                             |                          |

Protocol Steps
^^^^^^^^^^^^^^

**Apply Update Phase:**

#. Primary adds the omap updates to its local ECOmapJournal as an ECOmapJournalEntry
#. Primary encodes the omap updates into the PG log as a PG log entry
#. Primary sends PG log entry to all other primary-capable shards
#. Each shard stores the PG log entry in its local PG log

**Complete Update Phase:**

#. Primary applies the omap updates from a PG log entry to its own object store
#. Primary removes the corresponding journal entry
#. Primary sends "Complete Update" messages to all primary-capable shards
#. Each shard applies the update from its PG log to its object store
#. Each shard sends an ACK back to the primary

This journal approach means that if a write fails before it is completed,
there is nothing to rollback in the object stores. This means that it is
not necessary to read and store old omap data just incase an update needs
to be undone.

Recovery and Consistency
~~~~~~~~~~~~~~~~~~~~~~~~~

Omap recovery is integrated into the existing EC recovery loop. When a
primary-capable shard recovers:

- The recovering shard receives omap data from the current primary or another
  primary-capable shard
- Omap data is transferred as part of the normal EC recovery process

During primary failover:

- The new primary (which must be a primary-capable shard) already has a
  complete copy of the omap data
- A new journal is initialized on the new primary
- Operations can continue without data loss

This integration with existing recovery mechanisms simplifies the
implementation and ensures consistency with EC pool recovery behavior.

Omap Operations
---------------

Supported Operations
~~~~~~~~~~~~~~~~~~~~

The following omap operations are supported in EC pools:

- ``omap_get_keys``: Retrieve all keys in the omap
- ``omap_get_vals``: Retrieve all key-value pairs in the omap
- ``omap_get_vals_by_keys``: Retrieve specific key-value pairs
- ``omap_set``: Set one or more key-value pairs
- ``omap_rm_keys``: Remove one or more keys
- ``omap_clear``: Remove all key-value pairs
- ``omap_get_header``: Retrieve the omap header
- ``omap_set_header``: Set the omap header
- ``omap_cmp``: Compare omap values with other values

These operations provide the same semantics as in replicated pools, ensuring
compatibility with existing applications.

Read Operation Flow
~~~~~~~~~~~~~~~~~~~

Read operations follow a simple flow:

.. ditaa::

   Client                Primary
      |                     |
      | Omap Read           |
      |-------------------->|
      |                     |
      |                     | Read Local Omap
      |                     |------+
      |                     |      |
      |                     |<-----+
      |                     |
      |                     | Apply Journal Updates
      |                     |------+
      |                     |      |
      |                     |<-----+
      |                     |
      | Return Data         |
      |<--------------------|

Read operations are served from the primary OSD by:

#. Reading the stored omap data from the local replica
#. Applying any pending updates from the ECOmapJournal on top of the stored
   omap
#. Returning the combined result to the client

Using a journal means that there is a lag between an omap update and the
update being applied to the object store. Therefore, it is important that
modifications in the journal are considered during client omap reads, to
ensure that the correct data is returned.

The journal updates are applied in-memory during the read
operation, providing low-latency access to the omap data while maintaining
consistency.

Journal Overhead
~~~~~~~~~~~~~~~~

The journal introduces some performance overhead:

- **Journal Updates**: Each omap update requires the journal to be updated
- **Latency**: Omap operations require the primary osd to check the journal for
  updates
- **Storage**: Journal entries consume memory on the primary osd

However, this overhead is acceptable given the consistency guarantees provided.
Performance testing will quantify the impact and guide optimisation efforts.

Replication Impact
~~~~~~~~~~~~~~~~~~

Replicating omap data across primary-capable shards has performance
implications:

- **Network Traffic**: Updates generate network traffic to multiple shards
- **Storage**: Each primary-capable shard stores a complete omap replica
- **CPU**: Applying updates on multiple shards consumes CPU

These costs are offset by the benefits of high availability and fast reads.

Crimson-Specific Considerations
--------------------------------

The Crimson implementation will need to:

- Implement omap support using Crimson's asynchronous architecture
- Integrate with Crimson's seastar-based I/O framework
- Adapt the journal mechanism to Crimson's storage backend
- Ensure compatibility with the classic OSD implementation


Synchronous Reads
=================

Motivation
----------

Synchronous read operations are required to support Cls operations in EC pools.
Cls methods must execute synchronously, meaning they must complete a read
operation and receive the data before proceeding with their logic. The
traditional asynchronous read path in the EC backend does not provide this
capability.

Additionally, synchronous reads are beneficial for:

- Simplifying certain code paths that require sequential operations
- Enabling synchronous semantics without blocking threads
- Supporting future features that require synchronous data access

The key challenge is implementing synchronous semantics without blocking
threads, which would harm performance and scalability.

Implementation Design
---------------------

The synchronous read implementation uses Boost pull-type coroutines to provide
synchronous semantics without blocking threads:

- A Boost coroutine is created for the synchronous read operation
- The coroutine initiates an asynchronous read and yields control
- When the asynchronous read completes, the coroutine is resumed
- The coroutine returns the read data to the caller

This approach provides the synchronous semantics required by Cls operations
while maintaining the performance benefits of asynchronous I/O and avoiding
thread blocking.

Boost Pull-Type Coroutines
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The implementation uses Boost.Coroutine2 pull-type coroutines to bridge
asynchronous and synchronous code. The coroutine:

- Yields control back to the caller when waiting for I/O
- Allows the thread to process other work while I/O is in progress
- Resumes execution when the asynchronous operation completes
- Provides synchronous semantics to the Cls operation

This mechanism allows Cls operations to be written in a straightforward,
synchronous style while the underlying I/O remains asynchronous and
non-blocking.

Integration with EC Backend
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The synchronous read path integrates with the existing EC backend:

- The ECSwitch routes synchronous reads to the EC Backend
- The handler uses the existing asynchronous read infrastructure
- Results are returned synchronously to the caller using the coroutine

This integration minimizes code duplication and leverages existing, well-tested
read logic.

Synchronous Operation Ordering Guarantees
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Synchronous operations provide strong ordering guarantees:

- Synchronous reads block concurrent operations to the same object
- Multiple synchronous reads to the same object are serialized


Performance Impact
------------------

Latency Considerations
~~~~~~~~~~~~~~~~~~~~~~

Synchronous reads introduce some latency overhead compared to pure asynchronous
operations:

- **Coroutine Overhead**: Creating and resuming coroutines has a small CPU cost
- **Context Switching**: Yielding and resuming coroutines involves context
  switching overhead

However, these overheads are minimal compared to the I/O latency itself. In
practice, the latency impact will be negligible for most workloads.

Throughput Implications
~~~~~~~~~~~~~~~~~~~~~~~~

The throughput impact of synchronous reads depends on the workload:

- **Cls-Heavy Workloads**: Workloads with many Cls operations may see some
  impact, but the coroutine approach minimizes this
- **Mixed Workloads**: Workloads with a mix of synchronous and asynchronous
  operations will see minimal impact
- **Pure Data Workloads**: Workloads without Cls operations will see very
  minimal impact

Performance testing will quantify these impacts and guide optimisation efforts.

Comparison with Async Operations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Synchronous reads are not intended to replace asynchronous operations. They
serve a specific purpose for Cls operations and other use cases requiring
synchronous semantics. The EC backend continues to use asynchronous reads for
all other operations, maintaining optimal performance for the common case.


Cls, RBD, RGW and CephFS Support
================================

Cls (Class) Support
-------------------

Background
~~~~~~~~~~

Cls (class) operations are server-side methods that execute on OSDs, enabling
efficient data processing without client round-trips. Examples include:

- **RBD Operations**: Image metadata management, snapshot operations
- **RGW Operations**: Bucket index operations, object tagging
- **Custom Operations**: User-defined server-side processing

Cls operations are inherently synchronous - they must read data, process it,
and potentially write results, all within a single operation context. This
synchronous nature is why they require synchronous read support in the EC
backend.

Current Limitations in EC Pools
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Without synchronous read support, Cls operations could not be implemented in
EC pools. This prevented:

- RBD from using EC pools for image storage
- RGW from using EC pools for certain bucket operations
- Custom Cls methods from working with EC pools

Enabling Cls in EC Pools
~~~~~~~~~~~~~~~~~~~~~~~~~

Technical Requirements
^^^^^^^^^^^^^^^^^^^^^^

Enabling Cls in EC pools requires:

#. **Synchronous Read Support**: Implemented via coroutines as described above
#. **Omap Support**: Many Cls operations require omap for metadata storage

All of these requirements are met by the features described in this document.

Integration with Synchronous Reads
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Cls operations use synchronous reads through a straightforward integration:

.. code-block:: cpp

   // Simplified Cls operation using synchronous reads
   int cls_method(cls_method_context_t hctx)
   {
       bufferlist bl;

       // Synchronous read - blocks until data is available
       int r = cls_cxx_read(hctx, 0, 1024, &bl);
       if (r < 0)
           return r;

       // Process data
       process_data(bl);

       // Write results
       return cls_cxx_write(hctx, 0, bl);
   }

The ``cls_cxx_read`` function internally uses the synchronous read path,
suspending the coroutine until data is available.

Integration with Omap Support
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Many Cls operations require omap access for metadata:

.. code-block:: cpp

   // Cls operation using omap
   int cls_method_with_omap(cls_method_context_t hctx)
   {
       map<string, bufferlist> vals;

       // Read omap values
       int r = cls_cxx_map_get_vals(hctx, "", "", 100, &vals);
       if (r < 0)
           return r;

       // Process metadata
       process_metadata(vals);

       // Update omap
       return cls_cxx_map_set_vals(hctx, &vals);
   }

The omap operations work seamlessly with Cls, providing the metadata storage
required for complex operations.

RBD Support
-----------

RBD (RADOS Block Device) is a primary beneficiary of Cls support in EC pools.
RBD uses Cls operations extensively for:

- Image metadata management
- Snapshot operations
- Clone operations
- Exclusive lock management

With Cls support, RBD can now use EC pools for metadata. This gives the user more flexibility
about how they use RBD, including the option to use a single EC pool for data and metadata.

RGW Support
-----------

RGW (RADOS Gateway) benefits immensely from omap and Cls support in EC pools,
as it heavily relies on these features for S3 and Swift object storage semantics.
Traditionally, RGW required separate replicated pools for metadata and bucket indices.
RGW uses omap and Cls operations extensively for:

- Bucket index management (tracking objects within buckets)
- Multipart upload state tracking and assembly
- User quota and usage tracking
- Object extended attributes and custom metadata

With omap and synchronous read support natively in EC pools, as well as a few tweaks to remove
current restrictions, users will be able to use EC pools as metadata pools in RGW.

CephFS Support
--------------

CephFS (Ceph File System) requires robust omap support and strictly consistent reads
to maintain POSIX-compliant file system semantics.
Historically, the MDS (Metadata Server) required a dedicated replicated pool for its
metadata backing store. CephFS relies on omap and synchronous operations for:

- Directory object management (storing dentries as omap key-value pairs)
- MDS journal and log storage
- File extended attributes (xattrs) and layout metadata
- Inode state management and lock tracking

By bringing omap and synchronous reads to EC pools, as well as a few tweaks to remove 
current retrictions, users will be able to use an EC pool as the metadata pool in CephFS.


Testing
=======

Test Strategy Overview
----------------------

The testing strategy for these features is comprehensive and multi-layered:

- **Omap Journal Unit Tests**: Test the functionality of the journal in isolation
- **Omap Integration Tests**: Test omap operations and recovery in a full rados cluster
- **Cls Integration Tests**: Test cls method calls in a full rados cluster
- **EC Omap in Teuthology Tests**: Allow omap operations in EC pools with ceph_test_rados
- **RBD in Teuthology Tests**: Test RBD in teuthology with an EC metadata pool, and a single EC pool for data and metadata
- **RGW in Teuthology Tests**: Test RGW in teuthology using EC metadata pools
- **CephFS in Teuthology Tests**: Test CephFS in teuthology using an EC metadata pool

A key aspect of the testing strategy is the use of common test fixtures that
enable running existing tests on both replicated and fast EC pools.

Common Test Class Approach
~~~~~~~~~~~~~~~~~~~~~~~~~~~

A common test class has been implemented that:

- Provides a unified interface for test cases
- Supports both replicated and FastEC pools
- Allows existing test suites to run on EC pools with minimal modifications
- Ensures consistent test coverage across pool types
- Reduces code duplication

This approach significantly increases test coverage while minimizing test
development effort.

Existing Test Suite Integration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Integration with existing test suites includes:

- Running existing Cls tests on EC pools
- Enabling omap operations in EC pools for ceph_test_rados
- Changing all uses of RBD, RGW and CephFS to use just an EC pool in Teuthology

This integration ensures comprehensive coverage with minimal new test
development.

Migration and Compatibility
===========================

Release Requirements
--------------------

Umbrella Release Requirement
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ability to enable omap support on EC pools will be tied to the **Umbrella
release**. This introduces important version requirements:

- **All OSDs must be running at least the Umbrella release** before omap
  support can be enabled on any EC pool
- **Any OSDs added to the cluster in the future** must also be running at least
  the Umbrella release
- Attempting to enable omap support on a cluster with pre-Umbrella OSDs will
  fail with an error

This requirement ensures that all OSDs in the cluster have the necessary code
to support omap operations on EC pools, preventing data corruption or
inconsistencies that could arise from version mismatches.

Upgrade Path
------------

Enabling Features on Existing Pools
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Existing EC pools can be upgraded to support the new features:

#. Upgrade all OSDs to the Umbrella release or later
#. Verify that all OSDs in the cluster are at the required version
#. Enable EC overwrites on the pool (if not already enabled)
#. Enable EC optimisations on the pool (if not already enabled)
#. Enable omap support on the pool (if desired)
#. Existing data remains accessible throughout the upgrade

The upgrade process is designed to be non-disruptive, but the version
requirement must be strictly enforced.

Backward Compatibility
~~~~~~~~~~~~~~~~~~~~~~

Backward compatibility is maintained with important caveats:

- Pools without omap support continue to work as before on any version
- Clients that don't use the new features are unaffected
- **Downgrade is not supported** once omap has been enabled on an EC pool, as
  pre-Umbrella OSDs cannot handle omap data on EC pools
- Pools with omap enabled require all OSDs to remain at Umbrella or later

This compatibility ensures that upgrades are safe, but downgrades are
restricted to protect data integrity.

Configuration
-------------

Required Settings
~~~~~~~~~~~~~~~~~

To enable the new features, the following OSDMap pool settings are required:

- ``allows_ec_overwrites = true``
- ``allows_ec_optimizations = true``
- ``supports_omap = true``

These settings can be configured per-pool. The cluster will enforce that all
OSDs are at the Umbrella release before allowing omap support to be enabled.