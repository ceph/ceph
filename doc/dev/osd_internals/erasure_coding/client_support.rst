======================================================
Support for RBD, RGW and CephFS in Erasure Coded Pools
======================================================

Introduction
============

This document covers the design for enabling Omap (object map) support
and synchronous read operations in erasure-coded pools.
These enhancements enable EC pools to support CLS methods, as well as
RBD, RGW and CephFS workloads without the need for a separate replica
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

Journal entries contain:

- List of Omap Updates:
    - Operation type (set, remove, clear)
    - Bufferlist containing details about the operation (e.g. key/value pairs)
- An optional omap header
- A 'clear omap' boolean
- The object version

The journals on primary-capable shards that are not the primary shard store
the object deletion information, but not the omap updates. This allows for
updates to be committed to the correct object generation.

The journal is short lived and volatile. The journal only contains entries
for in-flight writes that are updating an omap. Whenever a new peering
interval starts, the journal is discarded. This is because peering resolves
all in-flight writes either by rolling back the lo entries, or marking them
complete (bringing the object store up to date) before allowing I/O to continue.

Commit Protocol
~~~~~~~~~~~~~~~

The omap commit protocol ensures that updates are applied consistently across
all primary-capable shards. The protocol is divided into two phases: Apply
Update and Complete Update.

Apply Update Phase
^^^^^^^^^^^^^^^^^^

Triggered when the PG needs to apply omap updates. The primary adds the updates
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
~~~~~~~~~~~~~~

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


CLS, RBD, RGW and CephFS Support
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

With Cls support, RBD can now use EC pools for image storage, providing
significant storage efficiency improvements for block storage workloads.

RGW Support
-----------

RGW (RADOS Gateway) benefits immensely from Omap and Cls support in EC pools,
as it heavily relies on these features for S3 and Swift object storage semantics.
Traditionally, RGW required separate replicated pools for metadata and bucket indices.
RGW uses Omap and Cls operations extensively for:

- Bucket index management (tracking objects within buckets)
- Multipart upload state tracking and assembly
- User quota and usage tracking
- Object extended attributes and custom metadata

With Omap and synchronous read support natively in EC pools,
RGW deployments can consolidate their architecture by storing both object data and
highly-mutated bucket indices in a single, space-efficient EC pool, drastically reducing
the storage overhead and operational complexity of large-scale object storage clusters.

CephFS Support
--------------

CephFS (Ceph File System) requires robust Omap support and strictly consistent reads
to maintain POSIX-compliant file system semantics.
Historically, the MDS (Metadata Server) required a dedicated replicated pool for its
metadata backing store. CephFS relies on Omap and synchronous operations for:

- Directory object management (storing dentries as Omap key-value pairs)
- MDS journal and log storage
- File extended attributes (xattrs) and layout metadata
- Inode state management and lock tracking

By bringing Omap and synchronous reads to EC pools, CephFS can store its complex directory
hierarchies and metadata structures directly alongside file data. This eliminates the
necessity for a segregated replicated metadata pool, optimizing cluster footprint and
simplifying capacity planning for file-based workloads.


Testing
=======

Test Strategy Overview
----------------------

The testing strategy for these features is comprehensive and multi-layered:

- **Omap Journal Unit Tests**: Test the functionality of the journal in isolation
- **Omap Integration Tests**: Test omap operations and recovery in a full rados cluster
- **Cls Integration Tests**: Test cls method calls in a full rados cluster
- **EC Omap in Teuthology Tests**: Allow omap operations in EC pools with ceph_test_rados
- **RBD in Teuthology Tests**: Change all uses of RBD in teuthology to use just an EC pool
- **RGW in Teuthology Tests**: Change all uses of RGW in teuthology to use just an EC pool
- **CephFS in Teuthology Tests**: Change all uses of CephFS in teuthology to use just an EC pool

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

To enable the new features, the following settings are required:

- ``allows_ec_overwrites = true`
- ``allows_ec_optimizations = true``
- ``supports_omap = true``

These settings can be configured per-pool. The cluster will enforce that all
OSDs are at the Umbrella release before allowing omap support to be enabled.