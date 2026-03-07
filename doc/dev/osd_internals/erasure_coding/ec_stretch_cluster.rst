=====================================================
Design Document: Ceph Erasure Coded (EC) Stretch Cluster
=====================================================

:Status: Draft
:Date: February 2026
:Feature: Replicated Erasure Coding for Stretch Clusters

.. note::

   **Phased Delivery (R1, R2, …)**

   This document distinguishes between **R1** and **later release** work.
   Sections and sub-sections are annotated accordingly. R1 delivers:

   - **Local direct reads** (good path only; any failure redirects to the
     Primary)
   - **Writes via Primary** (all write IO traverses the inter-site link; no
     Remote Primary encoding)
   - **Recovery from Primary** (Primary reads all data and writes all remote
     shards; no inter-site bandwidth optimisation)

   The labels R1, R2, etc. refer to **incremental PR delivery milestones** —
   they do *not* map one-to-one to Ceph named releases. Multiple delivery
   phases may land within a single Ceph release, or a single phase may span
   releases depending on development progress.

   The implementation order is detailed in Section 14.

.. important::

   **Scope and Applicability**

   This design is an **extension to Fast EC** (Fast Erasure Coding) only. We make
   **no attempt to support legacy EC** implementations with this feature. All
   functionality described in this document requires Fast EC to be enabled.

   Additionally, this design focuses exclusively on **Erasure Coded stretch
   clusters**. **No enhancements to Replica stretch clusters** are being made as
   part of this work, although some of the principles and architectural patterns
   described here could potentially be applied to replica configurations in future
   work. This document covers the pool configuration, CRUSH integration,
   read/write paths, recovery strategy, and peering changes needed for the
   initial implementation.

1. Intent & High-Level Summary
-------------------------------

The primary goal of this feature is to support a Replicated Erasure Coded (EC)
configuration within a multi-site Ceph cluster (Stretch Cluster), building upon
the Fast EC infrastructure.

.. note::

   Throughout this document, a *site* refers to a group of OSDs within a single
   CRUSH failure domain — typically a data center, but it could be any CRUSH
   bucket type (e.g., rack, room). The key characteristic of a site boundary is
   that traffic crossing it incurs a significant performance penalty: higher
   latency and/or lower bandwidth compared to intra-site communication.

   The term *inter-site link* refers to the network connection between these
   failure domains (e.g., the dedicated link between data centers). This is
   typically the most bandwidth-constrained and latency-sensitive path in the
   cluster. There is a strong desire to minimize inter-site link traffic, even
   to the extent of issuing additional local reads to avoid sending data across
   this link.

Currently, Ceph Stretch clusters typically rely on Replica to ensure data
availability across sites (e.g., data centers). While Erasure Coding provides
storage efficiency, applying a standard EC profile naively across a stretch
cluster introduces significant operational problems.

Consider a 2-site stretch cluster using a standard EC profile of ``k=4, m=6``.
While the high parity count provides sufficient fault tolerance to survive a
full site loss, this configuration suffers from three fundamental
inefficiencies:

1. **Write Bandwidth Waste**: Every write must distribute all ``k+m`` chunks
   across both sites. The coding (parity) shards are unnecessarily duplicated
   over the inter-site link, consuming bandwidth that scales linearly with the
   number of shards.
2. **Reads Must Cross Sites**: Serving a read requires retrieving at least ``k``
   chunks, which in general will span both sites. Every read operation incurs
   inter-site link latency.
3. **Recovery is Inter-Site Link Bound**: After a site failure, recovering the
   lost chunks requires reading surviving chunks across the inter-site link,
   placing the entire recovery burden on the most constrained network path.

This feature introduces a hybrid approach to eliminate these problems: creating
Erasure Coded stripes (defined by ``k + m``) and replicating those stripes
``num_sites`` times across a specified topology level (e.g., Data Center). Each site
holds a complete, independent copy of the EC stripe, meaning reads and recovery
can be performed entirely within a single site. This combines the local storage
efficiency of EC with the site-level redundancy of a stretch cluster, while
minimizing inter-site link usage to write replication only.


2. Proposed Configuration Changes
----------------------------------

To support this topology, the EC Pool configuration interface will be expanded.

2.1 Existing Parameters
~~~~~~~~~~~~~~~~~~~~~~~

The following standard EC parameters remain central to the configuration:

- **k**: Number of data chunks.
- **m**: Number of parity chunks.
- **chunk_size**: The size of the data chunks.

2.2 New Parameters
~~~~~~~~~~~~~~~~~~

We propose adding two new configuration options to the pool definition:

**r (Replicas)**
  - *Definition*: The number of full replicas of the EC stripe to be stored.
  - *Behavior*: Instead of distributing ``k+m`` chunks across the entire
    cluster, the system will generate ``num_sites`` distinct copies of the ``(k+m)``
    set.
  - *Pool Size*: The resulting pool ``size`` is ``num_sites × (k + m)``.
  - *Example*: If ``num_sites=2``, the cluster maintains two full copies of the EC set
    (e.g., one full set in Site A, one full set in Site B).

**site_failure_domain**
  - *Definition*: Configures the CRUSH rule level at which the replication
    (``num_sites``) occurs. This maps directly to the CRUSH bucket type used to
    construct the top-level placement rule.
  - *Default Value*: ``datacenter``
  - *Purpose*: This defines the boundary for the replicas. The EC chunks
    (``k+m``) are distributed within this domain, while the ``num_sites`` replicas are
    distributed across these domains.
  - *CRUSH Rule Automation*: When ``site_failure_domain`` is specified, the system
    can automatically generate an appropriate CRUSH rule for common scenarios,
    simplifying pool creation. For advanced topologies, administrators may still
    create a custom CRUSH rule and specify it explicitly at pool creation time.


3. Approaches Considered but Rejected
--------------------------------------

We evaluated and rejected the following two alternative designs:

3.1 Multi-layered Backends
~~~~~~~~~~~~~~~~~~~~~~~~~~

This approach involved re-using the existing ``ReplicationBackend`` to manage
the top-level replication, with EC acting as a secondary layer.

- *Reason for Rejection*: This would require complex, multi-layered peering
  logic where the replication layer interacts with the EC layer. Ensuring the
  correctness of these interactions is difficult. Additionally, the front-end
  and back-end interfaces of the two backends differ significantly (e.g.,
  support for synchronous reads), which would require substantial refactoring.
- *Advantage of Proposed Solution*: Our chosen bespoke approach minimizes
  changes to the peering state machine and offers better potential for recovery
  during complex failure scenarios.

3.2 LRC (Locally Repairable Codes) Plugin
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This approach involved configuring the existing LRC plugin to provide multi-site
EC semantics. While LRC is designed for locality-aware erasure coding, it does
not address the core problems this design aims to solve:

- *Reason for Rejection*:

  1. **Writes still cross sites**: LRC distributes all chunks (data, local
     parity, and global parity) across the full CRUSH topology. Every write
     operation sends chunks over the inter-site link, offering no bandwidth
     savings over standard EC.
  2. **Reads still cross sites**: Reading the original data requires ``k`` data
     chunks, which are spread across all sites. LRC provides no mechanism for a
     single site to independently serve reads.
  3. **Recovery remains primary-centralized**: In the current Ceph EC
     infrastructure, all recovery operations are coordinated by the Primary OSD.
     While LRC defines local repair groups at the coding level, the EC backend
     would still need to be extended to delegate recovery execution to remote
     sites — the same complexity required by this proposal.

- *Advantage of Proposed Solution*: The replicated EC stripe approach places a
  complete ``(k+m)`` set at each site, which inherently enables site-local
  reads, site-local single-OSD recovery (without any special coding scheme), and
  limits inter-site link usage to write replication. It achieves better locality
  than LRC with less infrastructure complexity.


4. Configuration Logic (Preliminary)
--------------------------------------

The interactions between these parameters imply a hierarchy in the CRUSH
rule generation, implemented using CRUSH MSR (Multi-Step Rule):

- **Top Level**: The CRUSH rule selects ``num_sites`` buckets of type
  ``site_failure_domain`` (e.g., select 2 data centers).
- **Lower Level**: Inside each selected ``site_failure_domain``, the CRUSH rule
  selects ``k+m`` OSDs to store the chunks.

This leverages the existing CRUSH MSR infrastructure — no changes to CRUSH
itself are required.


5. Multi-Site & Topology Considerations
-----------------------------------------

While this architecture is capable of supporting N-site configurations,
specific attention is given to the common 2-site High Availability (HA) use
case.

- **Reference Diagrams**: For clarity, architectural diagrams and examples
  within this design documentation will primarily depict a 2-site HA
  configuration (``num_sites=2``, with 2 data centers).
- **Logical Scalability**: The design is logically N-way capable. The parameter
  ``num_sites`` is not limited to 2; the system supports any valid CRUSH topology where
  ``num_sites`` failure domains exist.
- **Testing Strategy**: Testing will follow a phased approach:

  1. **Unit Tests (Initial)**: The unit test framework will include tests for
     both 2-site (``num_sites=2``) and 3-site (``num_sites=3``) configurations, validating the
     core peering and recovery logic for N-way topologies.
  2. **Full 2-Site Testing (Initial Release)**: 2-site configurations will be
     fully tested end-to-end for the initial release, covering all read, write,
     recovery, and failure scenarios.
  3. **Full 3-Site Testing (Later Release)**: Full integration and real-world
     testing of 3-site configurations will be deferred to a subsequent release.


6. Topologies and Terminology
-------------------------------

To illustrate the relationship between the replication layer, the EC layer, and
the physical topology, we define the following terms and visualization.

6.1 Logical Diagram (2-Site HA)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following diagram illustrates a cluster configured with ``r=2``, ``k=2``,
``m=1`` spanning two Data Centers.

.. ditaa::
  +-------------------+             +-------------------+
  |      Client A     |             |      Client B     |
  +---------+---------+             +---------+---------+
            |                                 |          
            v                                 v          
  +---------------------------------------------------+
  |             Logical Replication Layer             |
  |        (num_sites=2, site_failure_domain=DC)      |
  +--------------------------+------------------------+
                             |                           
              +--------------+--------------+            
              |                             |            
              v                             v            
  +-----------+---------+       +-----------+-----------+
  |                     |       |                       |
  |                     |       |                       |
  |    Data Center A    |       |     Data Center B     |
  |  (Replica/Site 1)   |       |   (Replica/Site 2)    |
  |                     |       |                       |
  |                     |       |                       |
  +--+-------+-------+--+       +---+-------+-------+---+
     |       |       |              |       |       |    
     v       v       v              v       v       v
  +-----+ +-----+ +-----+        +-----+ +-----+ +-----+ 
  | OSD | | OSD | | OSD |        | OSD | | OSD | | OSD | 
  |Shard| |Shard| |Shard|        |Shard| |Shard| |Shard| 
  |  0  | |  1  | |  2  |        |  3  | |  4  | |  5  | 
  +-^---+ +-----+ +-----+        +-^---+ +-----+ +-----+ 
    |                              |                                  
 Primary                         Remote                  
                                 Primary                            

6.2 Key Definitions
~~~~~~~~~~~~~~~~~~~~

**Primary**
  The primary OSD in the acting set for a Placement Group (PG). This OSD is
  responsible for coordinating writes and reads for the PG.
  (Standard Ceph terminology.)

**Shard**
  The globally unique position of a chunk within the pool's acting set. Shards
  are numbered ``0`` through ``num_sites × (k + m) - 1``. In the diagram above, Site A
  holds Shards 0, 1, 2 and Site B holds Shards 3, 4, 5.

**Remote**
  Generally refers to any ``site_failure_domain`` (datacenter) which does not
  contain the current Primary.

**Remote Primary**
  An internal EC convention. The Remote Primary is the first primary-capable
  shard in the acting set that resides in a given remote ``site_failure_domain``.
  Primary-capable shards are identified by excluding the non-primary-capable
  shard list stored in the pool configuration. Although the Remote Primary
  role is not explicitly stored in the OSDMap, it can be deterministically
  calculated from the acting set, pool configuration (including the
  non-primary-capable shard list), and CRUSH topology; every node — including
  clients — will arrive at the same answer. Clients must be able to identify
  the Remote Primary in order to submit IO to their local site. Restricting
  the choice to primary-capable shards maintains consistency with FastEC
  semantics.

  - *Cardinality*: There is one Remote Primary per remote ``site_failure_domain``.
    In an ``num_sites``-site configuration, there are ``num_sites-1`` Remote Primaries.
  - *Failover*: If the current Remote Primary for a site becomes unavailable,
    the role passes to the next shard in the acting set within that
    ``site_failure_domain``. If no shards remain in the ``site_failure_domain``, the
    write path falls back to Direct-to-OSD (see Section 8.2.2) for that site.
  - *Role*: The Remote Primary coordinates replication and recovery operations
    within its site on behalf of the Primary.

**Remote Shard**
  A specific shard located within a remote datacenter.


7. Read Path & Recovery Strategies
-------------------------------------

This architecture supports multiple read strategies to optimise for locality
and handle failure scenarios. The strategies are presented in order of
increasing complexity: starting with the baseline read-from-Primary path,
then layering direct-read optimisations on top.

7.1 Read from Primary — *R1*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The standard read path involves the client contacting the Primary OSD.

- **Local Priority**: The Primary will prioritize reading from local OSDs
  (within its own ``site_failure_domain``) to serve the request or recover data
  (reconstruct the stripe). This minimizes inter-site link traffic.

7.1.1 Local Recovery
^^^^^^^^^^^^^^^^^^^^^

When the Primary cannot serve a read from a single local shard (e.g., because
a shard is missing or degraded), it reconstructs the data from the remaining
local shards within its ``site_failure_domain``.

.. mermaid::

     sequenceDiagram
       title Direct Read — Primary Site

       participant C as Client
       participant P as Primary
       participant LS as Local Shard

       C->>LS: Direct Read
       activate LS
       LS->>C: Complete
       deactivate LS

       C->>LS: Direct Read
       activate LS
       LS->>C: -EAGAIN
       deactivate LS
       C->>P: Full Read
       activate P
       P->>LS: Read
       activate LS
       LS->>P: Read Done
       deactivate LS
       P->>C: Done
       deactivate P

7.1.2 Remote Recovery
^^^^^^^^^^^^^^^^^^^^^^

If the Primary cannot reconstruct data using only local shards, it may issue
direct reads to remote OSDs. This cross-site read path serves two purposes:

- **Backfill / Recovery**: When a local OSD is down or being backfilled, the
  Primary can read the corresponding shard from the remote site to recover the
  missing data. It is likely that a site with insufficient EC chunks to
  reconstruct locally would be taken offline by the stretch cluster monitor
  logic, but this fallback ensures availability during transient states.
- **Medium Error Recovery**: If a local OSD returns a medium error (e.g.,
  unreadable sector), the Primary can recover the affected data by reading from
  remote shards and reconstructing locally.

7.2 Direct Reads — Primary Site — *R1*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This feature extends the existing "EC Direct Reads" capability (documented
separately) to the stretch cluster topology. A client co-located with the
Primary can read data directly from a local shard, bypassing the Primary
entirely on the good path. **No new code is required** — this is the standard
EC Direct Read path operating over stretch-cluster shard numbering. Any failure
falls back to the Primary read path (Section 7.1).

.. mermaid::

   sequenceDiagram
       title Direct Read — Primary Site

       participant C as Client
       participant P as Primary
       participant LS as Local Shard

       C->>LS: Direct Read
       activate LS
       LS->>C: Complete
       deactivate LS

       C->>LS: Direct Read
       activate LS
       LS->>C: -EAGAIN
       deactivate LS
       C->>P: Full Read
       activate P
       P->>LS: Read
       activate LS
       LS->>P: Read Done
       deactivate LS
       P->>C: Done
       deactivate P

- **Failure Handling & Redirection (R1)**:

  Any failure encountered during a direct read — whether a missing shard,
  ``-EAGAIN`` rejection, or medium error — results in the client redirecting
  the operation to the **Primary** (regardless of the Primary's location).

7.3 Direct Reads — Remote Site — *R1*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A client in a remote ``site_failure_domain`` can read directly from a local shard,
but unlike Section 7.2 (where the client is co-located with the Primary and
the standard EC Direct Read path applies unmodified), the remote-site case
requires the client to identify which shards are local.

- **Local Shard Identification**: The client determines which OSDs in the
  acting set reside within its own ``site_failure_domain`` by comparing OSD CRUSH
  locations against its own. This identifies the ``k+m`` shards of the local
  replica. The client then selects a suitable data shard from this set for the
  direct read.

- **Unavailable Local OSDs**: If the local shards are not available (e.g., the
  local OSDs are down or the client cannot identify any local replica), the
  client does not attempt a direct read and instead directs the operation to
  the Primary. In later releases this will be improved to redirect to the
  local Remote Primary instead.

- **Direct Read Execution**: If a local shard is available, the client sends
  the read directly to that OSD. As detailed in the ``ec_direct_reads.rst``
  design, it is generally acceptable for reads to overtake in-flight writes.
  However, the OSD is aware if it has an "uncommitted" write (a write that
  could potentially be rolled back) for the requested object. If such a write
  exists, or if the client's operation requires strict ordering (e.g. the
  ``rwordered`` flag is set), the OSD rejects the operation with ``-EAGAIN``.
  Data access errors (e.g., a media error on the underlying storage) also cause
  the OSD to reject the operation.

- **Failure Handling & Redirection (R1)**:

  Any rejection — whether ``-EAGAIN`` (uncommitted write), a data access error,
  or any other failure — causes the client to redirect the operation to the
  **Primary** (regardless of the Primary's location). No attempt is made to
  resolve failures locally in R1; this will be improved in later releases by
  redirecting to the local Remote Primary (Section 7.5).

7.4 Read from Remote Primary — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the R1 release, the primary will handle all failures. In later releases,
an op which cannot be processed as a direct read will be directed at the 
remote primary instead. The remote primary will attempt to reconstruct the
read data from the local shards directly. 

A conflict due to an uncommitted write will be continue to be handled by
the primary, as the remote primary must also rejected an op if an
uncommitted write exists for that object. 

- **Local Recovery**: A read directed to a Remote Primary will attempt to serve
  the request by recovering data using only OSDs within the same data center
  (the remote site).
- **Site Degradation**: If a site has insufficient redundancy to reconstruct
  data locally, that site should be taken offline to clients rather than serving
  reads that would require inter-site link access. (How? - Needs to be covered elsewhere)
- **``-EAGAIN`` Behavior**: The Remote Primary will return ``-EAGAIN`` only in
  short-lived transient conditions:


7.4.1 Safe Shard Identification & Synchronous Recovery — *Later Release*
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

By default, the Remote Primary does not inherently know which shards are
consistent and safe to read. To manage this, the Peering process will be
enhanced.

- **Synchronous Recovery Set**: The existing peering Activate message will be
  extended to include a "Synchronous Recovery Set". This set identifies shards
  that are currently undergoing recovery and may not yet be consistent.

  .. note::

     The peering state machine has a transition from Active+Clean to Recovery,
     which implies that recovery can start without a new peering interval.
     This needs further investigation to ensure the Synchronous Recovery Set
     is correctly maintained across such transitions.

- **Consistency Guarantee**: Within any single epoch, the Synchronous Recovery
  Set can only reduce over time (shards are removed as they complete recovery).
  The set is treated as binary — either the full recovery set is active, or it
  is empty. This ensures the Remote Primary is *conservatively stale*: it may
  reject reads it could safely serve, but will never read from an inconsistent
  shard.

- **Initial Behaviour**:

  - Shards listed in the Synchronous Recovery Set will not be used for reads
    by the Remote Primary.
  - If the remaining available shards are insufficient to reconstruct the data,
    the Remote Primary will return ``-EAGAIN``.
  - Recovery messages will signal the Remote Primary when recovery completes,
    clearing the set.

- **Subsequent Enhancement**:

  - A mechanism will be added to allow the Remote Primary to request permission
    to read from a shard within the Synchronous Recovery Set.
  - This request will trigger the necessary recovery for that specific object
    (if not already complete) before the read is permitted.


8. Write Path & Transaction Handling
--------------------------------------

This section details how write operations are managed, specifically focusing on
Read-Modify-Write (RMW) sequences and the replication of write transactions to
remote sites.

8.1 Direct-to-OSD Writes (Primary Encodes All Shards) — *R1*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In R1, all writes are coordinated exclusively by the Primary.

- **Mechanism**: The Primary generates all ``num_sites × (k + m)`` coded shard writes
  and sends individual write operations directly to every OSD in the acting
  set, including those in remote ``site_failure_domain``\s.
- **RMW Reads**: The "read" portion of any Read-Modify-Write cycle is performed
  locally by the Primary, strictly adhering to the logic defined in Section 7
  (Read Path & Recovery Strategies).
- **Inter-site traffic**: This sends all coded shard data (including parity)
  over the inter-site link. While this is not bandwidth-optimal, it requires
  no Remote Primary involvement in write processing and therefore no new
  write-path code beyond extending the existing shard fan-out to the larger
  acting set.

8.2 Replicate Transaction (Preferred Path) — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   This section is deferred to a later release. R1 sends all coded
   shards directly from the Primary.

.. mermaid::

   sequenceDiagram
       title Write to Primary

       participant C as Client
       participant P as Primary
       participant LS as Local Shard
       participant RP as Remote Primary
       participant RS as Remote Shard

       C->>P: Submit Op
       activate P

       P->>RP: Replicate
       activate RP

       note over P: Replicate message must contain<br/>data and which shards are to be updated.<br/>When OSDs are recovering, the primary<br/>may need to update remote shards directly.

       P->>LS: SubRead (cache)
       activate LS
       LS->>P: SubReadReply
       deactivate LS

       P->>LS: SubWrite
       activate LS
       LS->>P: SubWriteReply
       deactivate LS

       RP->>RS: SubRead (cache)
       activate RS
       RS->>RP: SubReadReply
       deactivate RS

       RP->>RS: SubWrite
       activate RS
       RS->>RP: SubWriteReply
       deactivate RS

       RP->>P: ReplicateDone
       deactivate RP

       P->>C: Complete
       deactivate P

This mechanism is designed to minimize inter-site link bandwidth usage, which
is often the most constrained resource in stretch clusters.

- **Mechanism**: The replicate message is an extension of the existing
  sub-write message. Instead of sending individually coded shard data to every
  OSD in the remote site, the Primary sends a copy of the *PGBackend
  transaction* (i.e., the raw write data and metadata, prior to EC encoding) to
  the Remote Primary.
- **Role of Remote Primary**: Upon receipt, the Remote Primary processes the
  transaction through its local ECTransaction pipeline — largely unmodified
  code — to generate the coded shards for its site. For writes smaller than a
  full stripe, this includes the Remote Primary issuing reads to its local
  shards so that the parity update can be calculated. It then fans out the shard
  writes to the local OSDs within its ``site_failure_domain``. This means coding
  (parity) data is never sent over the inter-site link; it is computed
  independently at each site.
- **Completion**: The Remote Primary responds using the existing sub-write-reply
  message once all local shard writes are durable.
- **Crash Handling**: If the Remote Primary crashes mid-fan-out, any partial
  writes on remote shards are rolled back by the existing peering process. No
  new crash-recovery mechanisms are required.
- **Benefit**: This reduces inter-site link traffic to a single transaction
  message per remote site, carrying only the raw data — not the ``k+m`` coded
  chunks.

8.3 Client Writes Direct to Remote Primary — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   This section is deferred to a later release. It requires Replicate
   Transaction (Section 8.2) as a prerequisite, since the Remote Primary must
   be able to process transactions and fan out shard writes locally.

.. mermaid::

   sequenceDiagram
       title Write to Remote Primary

       participant C as Client
       participant P as Primary
       participant LS as Local Shard
       participant RC as Remote Client
       participant RP as Remote Primary
       participant RS as Remote Shard

       RC->>RP: Write op
       activate RP

       RP->>P: Replicate
       activate P
       P->>RP: Permission to write

       note over RP: There is potential for pre-emptive caching here,<br/>but it adds complexity and mostly does not actually<br/>help performance, as the limiting factor is actually<br/>the remote write.

       RP->>RS: SubRead (cache)
       activate RS
       RS->>RP: SubReadReply
       deactivate RS

       RP->>RS: SubWrite
       activate RS
       RS->>RP: SubWriteReply
       deactivate RS

       P->>LS: SubRead (cache)
       activate LS
       LS->>P: SubReadReply
       deactivate LS

       P->>LS: SubWrite
       activate LS
       LS->>P: SubWriteReply
       deactivate LS

       P->>RP: write done
       RP->>RC: Complete

       note over RP: The write complete message is permitted<br/>as soon as all SubWriteReply messages have<br/>been received by the remote. It is shown<br/>here to demonstrate that it is required to<br/>complete processing on the Primary.

       RP->>P: Remote write complete
       deactivate P
       deactivate RP

       P->>P: PG Idle.

       P->>RP: DummyOp
       activate P
       activate RP

       P->>LS: DummyOp
       activate LS
       LS->>P: Done
       deactivate LS

       RP->>P: Done
       deactivate P

       note over RP: Message ordering means that remote<br/>primary does not need to wait for<br/>dummy ops to complete

       RP->>RS: DummyOp
       activate RS
       RS->>RP: Done
       deactivate RS
       deactivate RP

This mechanism allows a client to write to its local Remote Primary rather than
sending data over the inter-site link to the Primary. The key benefit is that
the write data crosses the inter-site link only once (Remote Primary → Primary)
rather than twice (client → Primary → Remote Primary). Unlike Section 8.2 where
the Primary initiates the transaction and replicates it outward, here the
Remote Primary receives the client write directly and coordinates with the
Primary to maintain global ordering while avoiding redundant data transfer.

- **Mechanism**:

  1. The client sends the write to its local **Remote Primary**. The Remote
     Primary stashes a local copy of the write data but does not yet fan out
     to local shards.
  2. The Remote Primary replicates the *PGBackend transaction* (raw write data,
     prior to EC encoding) to the **Primary**.
  3. The Primary processes the transaction as normal through its local
     ECTransaction pipeline, performing cache reads, encoding, and fanning out
     shard writes to its local OSDs. When the Primary reaches the step where it
     would normally replicate data to the originating Remote Primary (as in
     Section 8.2), it instead sends a **write-permission message** to that
     Remote Primary, since the Remote Primary already holds the data. Writes to
     any *other* Remote Primaries (in an ``num_sites > 2`` configuration) proceed via
     the normal Replicate Transaction path (Section 8.2).
  4. Upon receiving write-permission, the Remote Primary processes the
     transaction through its local ECTransaction pipeline, generating and
     fanning out the coded shard writes to the local OSDs within its
     ``site_failure_domain``.
  5. Once the Primary's own local writes are durable and all other sites have
     confirmed durability, the Primary sends a **completion message** to the
     originating Remote Primary. The Remote Primary then responds to the client
     once the completion message is received and its own local writes are also
     durable.

- **Write Ordering**: Strict write ordering must be maintained across all sites.
  Since the Primary remains the single authority for PG log sequencing:

  - The Primary sequences the write as part of its normal processing in step 3.
    The write-permission message carries the assigned sequence number, ensuring
    that writes arriving at the Primary directly and writes arriving via a
    Remote Primary are globally ordered.
  - Writes from multiple Remote Primaries (in an ``num_sites > 2`` configuration) are
    serialised through the Primary's normal sequencing mechanism — no special
    handling is required beyond the existing PG log ordering.
  - The Remote Primary does not fan out to local shards until it receives the
    write-permission message, guaranteeing that local shard writes occur in
    the correct global order.

- **Benefit**: For workloads where the client is co-located with a remote site,
  write data traverses the inter-site link exactly once (Remote Primary →
  Primary transaction replication). This halves the inter-site bandwidth
  compared to R1 (client → Primary → Remote Primary), and is equivalent to
  Section 8.2 but with the added advantage that the client experiences local
  write latency for the initial acknowledgement. Crucially, the data is never
  sent back to the originating Remote Primary — the Primary only sends
  lightweight permission and completion messages.

- **Failure Handling**: If the Remote Primary is unavailable, the client falls
  back to writing directly to the Primary (Section 8.1 or 8.2, depending on
  what is available). If the Primary is unreachable from the Remote Primary
  mid-transaction, the Remote Primary returns an error to the client and any
  stashed data is discarded (no local shard writes have been issued, since
  write-permission was never received).

- **R3 Enhancement: Forward to Other Remote Primaries (``num_sites > 2``)**:

  .. note::

     This enhancement is a potential R3 feature and may not be implemented.

  In topologies with more than two sites, the originating Remote Primary could
  forward the transaction to the other Remote Primaries in parallel with
  sending it to the Primary. This would improve write latency for those sites
  by allowing them to receive the data directly from a peer Remote Primary
  rather than waiting for the Primary to replicate outward. The Primary would
  still issue write-permission messages to all Remote Primaries to maintain
  global ordering, but the data transfer to additional sites would already be
  complete, reducing the critical path.

8.4 Hybrid Approach — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   Requires Replicate Transaction (Section 8.2). Deferred to a later release.

It is acknowledged that complex failure scenarios may require a combination of
Replicate Transaction, Client-via-Remote-Primary, and Direct-to-OSD approaches.

- **Example**: In a partial failure where one remote site is healthy and another
  is degraded, the Primary may use Replicate Transaction for the healthy site
  while simultaneously using Direct-to-OSD for the degraded site within the
  same transaction lifecycle.


9. Recovery Logic
------------------

All recovery operations are centralized and coordinated by the Primary OSD.

9.1 Primary-Centric Recovery — *R1*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In R1, the Primary performs all recovery without delegating to Remote
Primaries and without attempting to minimize inter-site bandwidth.

- **Local Recovery**: The Primary recovers missing shards in its own
  ``site_failure_domain`` using available local chunks, exactly as standard EC
  recovery.
- **Remote Recovery**: For every remote ``site_failure_domain``, the Primary reads
  all shards required to reconstruct any missing data, encodes the missing
  shards locally, and pushes them directly to the target remote OSDs.
- **No Remote Primary Coordination**: The Remote Primary plays no role in
  recovery in R1. All cross-site data flows from or to the Primary.
- **Trade-off**: This approach may send more data over the inter-site link than
  necessary, but it avoids the complexity of remote-delegated recovery and uses
  existing recovery infrastructure with minimal modification.

9.2 Cost-Based Recovery Planning — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   This section is deferred to a later release. R1 uses Primary-centric
   recovery (Section 9.1) for all scenarios.

When the system identifies an individual object (including its clones) requiring
recovery, the Primary executes a per-object assessment and planning phase. This
leverages the existing, reactive, object-by-object recovery mechanism rather
than introducing a broad distributed orchestration layer. The overriding goal
is to **minimize inter-site link bandwidth** — every cross-site transfer is
expensive, so the Primary must dynamically choose the recovery strategy for
each object that results in the fewest bytes crossing site boundaries.

- **Assess Capabilities**: For each ``site_failure_domain`` (site), the Primary
  determines how many consistent chunks are available locally and how many are
  missing.
- **Cost-Based Plan Selection**: The Primary evaluates the inter-site bandwidth
  cost of each viable recovery strategy and selects the cheapest option. The
  key trade-off is between:

  1. *Remote Primary performs local recovery with remote reads* — the Remote
     Primary reads a small number of missing shards from the Primary's site
     and reconstructs locally.
  2. *Primary reconstructs and pushes* — the Primary reconstructs the full
     object and pushes the missing shards to the remote site.

  The optimal choice depends on how many shards each site is missing.

- **Example**: Consider a ``6+2`` (k=6, m=2) configuration where Site A has 8
  good shards and Site B has only 5 good shards (3 missing). Two options:

  - *Option A (Remote Primary reads remotely)*: The Remote Primary at Site B
    needs only **1 remote read** (it has 5 of the 6 required chunks locally
    and fetches 1 from Site A) to reconstruct the 3 missing shards locally.
    **Cost: 1 shard across the inter-site link.**
  - *Option B (Primary pushes)*: The Primary reconstructs the 3 missing shards
    at Site A and pushes them to Site B. **Cost: 3 shards across the
    inter-site link.**

  Option A is clearly cheaper. A general algorithm should compare the
  cross-site transfer cost for each strategy and choose the minimum.
  Where inter-site bandwidth consumption is equal, the algorithm should tie-break
  by optimising for local read bandwidth or the total number of operations.

  (In the future, the system may adapt these priorities — e.g. explicitly
  optimising for read count rather than inter-site bandwidth on high-bandwidth
  links to slower rotational media — whether through auto-tuning or operator
  configuration. However, exploring alternative optimisation modes is beyond the
  scope of this design.)

  .. note::

     The detailed algorithm for optimal recovery planning requires further
     design work. The general principle is: count the number of cross-site
     shard transfers each strategy would require and choose the minimum.

- **Plan Construction**: For each domain, the Primary constructs a "Recovery
  Plan" message containing specific instructions detailing which OSDs must be
  used to read the available chunks and which OSDs are the targets to write the
  recovered chunks. Where cross-site reads are part of the plan, the message
  specifies which remote shards to fetch.

9.3 Delegated Remote Recovery — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   Requires Cost-Based Recovery Planning (Section 9.2). Deferred to a later
   release.

- **Remote Recovery (Plan-Based)**: The Primary sends a per-object "Recovery Plan"
  to the Remote Primary (or appropriate peers), instructing them to execute the
  recovery for that object (and clones) locally within their domain using the
  provided read/write set. If the plan includes cross-site reads, the Remote Primary
  fetches the specified shards before reconstructing. Because this ties directly
  into the existing object recovery lifecycle, any failure of a remote peer
  during the plan simply drops the recovery op. The Primary detects the failure
  via standard peering or timeout mechanisms and will naturally re-assess and
  generate a new plan for the object.

9.4 Fallback Strategies — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   These strategies are part of the Cost-Based Recovery system (Section 9.2)
   and are deferred to a later release.

9.4.1 Remote Fallback (Push Recovery)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If a remote domain cannot perform local recovery, and the cost-based analysis
determines that Primary-side reconstruction and push is the cheapest option:

1. The Primary reconstructs the full object locally (using whatever valid shards
   are available across the cluster).
2. The Primary pushes the full object data to the Remote Primary.
3. This push is accompanied by a set of instructions specifying which shards in
   the remote site must be written.

9.4.2 Primary Domain Fallback
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If the Primary's own domain lacks sufficient chunks to recover locally:

1. The Primary is permitted to read required shards from remote sites to perform
   the reconstruction.
2. While this crosses site boundaries, the cost-based planning ensures it is
   only chosen when it results in fewer cross-site transfers than any
   alternative.


10. PG Log Handling — *R1*
---------------------------

This section describes how PG log entries are managed across replicated EC
stripes, building on the FastEC design for primary-capable and non-primary
shards.

10.1 Primary-Capable vs. Non-Primary Shards
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The existing FastEC design distinguishes between two classes of shard:

- **Primary-capable shards**: Maintain a full copy of the PG log, enabling
  them to take over as Primary if needed.
- **Non-primary shards**: Maintain a reduced PG log, omitting entries where no
  write was performed to that specific shard.

For replicated EC stretch clusters, the non-primary shard selection algorithm
will be extended. The following shards will be classified as **primary-capable**
(i.e., they maintain a full PG log):

- The first data shard in each replica (per ``site_failure_domain``).
- All coding (parity) shards in each replica.

All remaining data shards will be non-primary shards with reduced logs.

.. note::

   Implementation detail: it needs to be determined whether ``pg_temp`` should
   be used to arrange all primary-capable shards (local, then remote) ahead of
   non-primary-capable shards in the acting set ordering.

10.2 Independent Log Generation at Remote Primary — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   Log generation at the Remote Primary is only needed when Replicate
   Transaction writes (Section 8.2) are implemented. For R1, where the
   Primary sends pre-encoded shards directly, PG log entries are generated
   solely by the Primary and distributed with the sub-write messages as per
   existing behaviour.

For latency optimization, the replicate transaction message (see Section 8.2)
will be sent to the Remote Primary *before* the cache read has completed on the
Primary. This means the Remote Primary cannot receive a pre-built log entry from
the Primary — it must generate its own PG log entry independently from the
transaction data.

Both the Primary and Remote Primary will produce equivalent log entries from the
same transaction, but they are generated independently at each site.

10.3 Log Entry Compatibility & Upgrade Safety — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``osdmap.requires_osd_release`` field dictates the minimum code level that
OSDs can run, and therefore determines the log entry format that must be used.
No new versioning mechanism is required — the existing release gating already
constrains log entry compatibility across mixed-version clusters.

Because the Remote Primary generates its own log entry from the transaction
data, it must populate the Object-Based Context (OBC) to obtain the old
version of attributes (including ``object_info_t`` for old size). This is an
additional reason the Remote Primary must be a **primary-capable shard** — only
primary-capable shards maintain the full PG log and OBC state needed for
correct log entry construction.

**Debug Mode for Log Entry Equivalence**

A debug mode will be implemented that compares the log entries generated by the
Primary and Remote Primary for each transaction. When enabled, the Primary will
include its generated log entry in the replicate transaction message, and the
Remote Primary will assert that its independently generated entry is identical.
This mode will be **enabled by default in teuthology testing** to catch any
divergence early. It will be available as a runtime configuration option for
production debugging but disabled by default in production due to the
additional message overhead.

.. warning::

   If a future release changes how PG log entries are derived from
   transactions, the debug equivalence mode provides an automated safety net.
   Any divergence between Primary and Remote Primary log entries will be caught
   immediately in CI.


11. Peering & Stretch Mode — *R1*
-----------------------------------

This section covers the peering, ``min_size``, and stretch-mode integration
required for replicated EC pools. The design follows the existing replica
stretch cluster pattern as closely as possible, with EC-specific adaptations.

.. important::

   **R1 scope is simple two-site (num_sites=2).** Three-site (``num_sites=3``) details are
   described for design completeness but will be implemented in a later
   release.

11.1 Pool Lifecycle
~~~~~~~~~~~~~~~~~~~~~

A replicated EC pool **requires** stretch mode to be enabled
on the cluster. Creating an ``num_sites > 1`` pool without stretch mode is not
supported.

- **CRUSH rule**: Set to the stretch CRUSH rule at ``enable_stretch_mode``.
- **min_size**: Managed by the monitor. Automatically adjusted during stretch
  mode state transitions (Section 11.3).
- **Peering**: Uses stretch-aware acting set calculation with parameters
  adjusted by the monitor during state transitions.
- **Failure**: Automatic ``min_size`` reduction, degraded/recovery/healthy
  stretch mode transitions — all managed by OSDMonitor.

**Prerequisites for ``num_sites > 1`` Pool Creation**

.. list-table::
   :header-rows: 1

   * - Requirement
     - Reason
   * - All OSDs have the ``num_sites > 1`` feature bit
     - Ensures all OSDs understand extended acting-set semantics (Section 15)
   * - Stretch mode must be enabled on the cluster
     - ``num_sites > 1`` pools require the stretch mode state machine for
       ``min_size`` management and site failover
   * - ``site_failure_domain`` must match the stretch mode failure domain
     - The CRUSH rule must align with the stretch cluster topology

11.2 Minimum PG Size Semantics
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For a K+M pool with num_sites copies, the total acting set size is ``num_sites × (K+M)``.
The permitted ``min_size`` range is::

    min_size ∈ [ num_sites × (K+M) − M,  num_sites × (K+M) ]

In other words, **a maximum of M failures are permitted** across the entire
acting set in healthy mode. This is the natural extension of EC's existing
guarantee: M parity shards can tolerate M failures.

**Examples:**

.. list-table::
   :header-rows: 1

   * - K
     - M
     - R
     - size
     - min_size range
     - Default min_size
   * - 2
     - 1
     - 2
     - 6
     - 5 – 6
     - 5
   * - 4
     - 2
     - 2
     - 12
     - 10 – 12
     - 10
   * - 2
     - 1
     - 3
     - 9
     - 8 – 9
     - 8
   * - 4
     - 2
     - 3
     - 18
     - 16 – 18
     - 16

**Why This Range?**

- **Upper bound** (``num_sites × (K+M)``): All shards present; no failures tolerated
  before IO stops.
- **Lower bound** (``num_sites × (K+M) − M``): Up to M shards can be lost (across
  any sites) while every site that retains at least K shards can still
  reconstruct data. Below this threshold, the wrong combination of failures
  risks data loss.

The monitor manages ``min_size`` automatically during stretch mode state
transitions (Section 11.3). The ``min_size`` is validated to be within the
permitted range at pool creation and during stretch mode operations.

**Per-Site min_size — Later Release**

A more sophisticated implementation would retain the standard ``min_size``
range of ``k`` to ``k + m`` but reinterpret it as the minimum number of
shards that must be present *per site* for that site to be usable. If a site
does not meet its per-site minimum, it is removed from the up set. This is
described further in Section 14.4.

11.3 Stretch Mode State Machine and min_size Transitions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When stretch mode is enabled, ``min_size`` transitions are managed by
OSDMonitor, mirroring the replica stretch mode pattern. The state machine is
**identical** to the replica stretch mode state machine — we reuse the
existing OSDMonitor infrastructure.

.. mermaid::

   stateDiagram-v2
       [*] --> Healthy: enable_stretch_mode
       Healthy --> Degraded: site failure detected
       Degraded --> Recovery: failed site returns
       Recovery --> Healthy: all PGs clean
       Healthy --> [*]: disable_stretch_mode
       Degraded --> Recovery: force_recovery_stretch_mode CLI
       Recovery --> Healthy: force_healthy_stretch_mode CLI

**Two-Site Transitions (num_sites=2) — R1**

.. list-table::
   :header-rows: 1

   * - Stretch State
     - min_size
     - Reasoning
   * - **Healthy**
     - ``num_sites × (K+M) − M``
     - Full redundancy; tolerate up to M failures
   * - **Degraded** (one site down)
     - ``K``
     - One site lost (K+M shards gone). Surviving site has K+M shards;
       can tolerate M more losses. ``K+M − M = K``.
   * - **Recovery** (site returning)
     - ``K`` (same as degraded)
     - Keep reduced min_size until resync is complete
   * - **Healthy** (resync complete)
     - ``num_sites × (K+M) − M``
     - Full min_size restored

**Concrete example — K=2, M=1, num_sites=2 (size=6):**

.. list-table::
   :header-rows: 1

   * - Stretch State
     - min_size
   * - Healthy
     - 5
   * - Degraded
     - 2
   * - Recovery
     - 2
   * - Healthy (restored)
     - 5

**The degraded-mode formula:**

- Healthy min_size: ``num_sites × (K+M) − M``
- Degraded min_size: ``(num_sites−1) × (K+M) − M`` = healthy min_size minus
  ``(K+M)``
- **Rule: if a site fails, reduce min_size by K+M. If a site becomes
  healthy again, increase min_size by K+M.**

**Three-Site Transitions (num_sites=3) — Later Release**

.. list-table::
   :header-rows: 1

   * - Stretch State
     - min_size
     - Example (K=2, M=1)
   * - Healthy (3 sites)
     - ``3(K+M) − M``
     - 8
   * - One site down
     - ``2(K+M) − M``
     - 5
   * - Two sites down
     - ``K``
     - 2

11.4 OSDMonitor Changes
~~~~~~~~~~~~~~~~~~~~~~~~~~

The existing OSDMonitor stretch mode code contains
``if (is_replicated()) { ... } else { /* not supported */ }`` patterns in
several places. These gaps must be filled for EC pools with ``num_sites > 1``.

.. note::

   The ``min_size`` calculations require ``k``, ``m``, and ``num_sites`` from the
   pool's EC profile. These are stored in ``OSDMap::erasure_code_profiles``
   as string maps and are readily available to OSDMonitor.

**11.4.1 Pool Stretch Set / Unset** (``prepare_command_pool_stretch_set``,
``prepare_command_pool_stretch_unset``)

``stretch_set`` currently works for any pool type, setting
``peering_crush_bucket_*``, ``crush_rule``, ``size``, ``min_size``.

For EC pools with ``num_sites > 1``, add validation:

- Validate ``min_size ∈ [num_sites×(K+M)−M, num_sites×(K+M)]``
- If ``size`` is provided, validate it matches ``num_sites × (K+M)``

``stretch_unset`` clears all ``peering_crush_*`` fields. No EC-specific
changes required.

**11.4.2 Enable/Disable Stretch Mode** (``try_enable_stretch_mode_pools``)

*Currently rejects EC pools.* Change to accept EC pools with ``num_sites > 1``:

- Set ``peering_crush_bucket_count``, ``peering_crush_bucket_target``,
  ``peering_crush_bucket_barrier`` (same values as replica)
- Set ``crush_rule`` to the stretch CRUSH rule
- Set ``size = r × (k + m)`` (should already be correct from pool creation)
- Set ``min_size = r × (k + m) − m``

**Pool Creation Gate**: Pool creation with ``num_sites > 1`` must be rejected if
stretch mode is not already enabled on the cluster. This is validated in
``OSDMonitor::prepare_new_pool``.

**11.4.3 Degraded Stretch Mode** (``trigger_degraded_stretch_mode``)

*Currently sets* ``newp.min_size = pgi.second.min_size / 2`` *for replica
pools.* For EC pools, compute::

    newp.min_size = p.min_size - (k + m)

For a K=2, M=1, num_sites=2 pool: ``min_size = 5 − 3 = 2``.

Also set ``peering_crush_bucket_count`` and
``peering_crush_mandatory_member`` as for replicated pools.

**11.4.4 Healthy Stretch Mode** (``trigger_healthy_stretch_mode``)

*Currently reads* ``mon_stretch_pool_min_size`` *config for replica pools.*
For EC pools, compute from the EC profile::

    newp.min_size = r × (k + m) − m

**11.4.5 Recovery Stretch Mode** (``trigger_recovery_stretch_mode``)

No changes required — does not modify ``min_size``.

11.5 Peering State Changes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**11.5.1 Acting Set Calculation**

``calc_replicated_acting_stretch`` is pool-type agnostic at the CRUSH bucket
level, but for EC pools the acting set has **shard semantics** — position
``i`` must serve shard ``i``. A new ``calc_ec_acting_stretch`` function (or
an extension of the existing ``calc_ec_acting``) is required.

Algorithm for each position ``i`` (0 to ``num_sites×(K+M)−1``):

1. Prefer ``up[i]`` if usable
2. Otherwise prefer ``acting[i]`` if usable
3. Otherwise search strays for an OSD with shard ``i``
4. If no usable OSD found, leave as ``CRUSH_ITEM_NONE``

CRUSH ``bucket_max`` constraints apply: no site may contribute more than
``size / peering_crush_bucket_target`` OSDs.

.. note::

   This is essentially ``calc_ec_acting`` with the additional CRUSH bucket
   awareness from the stretch path.

**11.5.2 Async Recovery**

``choose_async_recovery_replicated`` checks ``stretch_set_can_peer()`` before
removing an OSD from async recovery. An EC stretch variant must respect shard
identity: removing an OSD must not cause any site to drop below K shards.

**11.5.3 Stretch Set Validation** (``stretch_set_can_peer``)

For EC pools, additionally verify:

- The acting set includes at least K shards in at least one surviving site
- In healthy mode, all sites have ``K+M`` shards

11.6 Relationship to ``peering_crush_bucket_*`` Fields
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The existing ``pg_pool_t`` fields remain unchanged:

.. list-table::
   :header-rows: 1

   * - Field
     - Purpose
     - EC Stretch Usage
   * - ``peering_crush_bucket_count``
     - Min distinct CRUSH buckets in acting set
     - num_sites (sites) in healthy; reduced during degraded
   * - ``peering_crush_bucket_target``
     - Target CRUSH buckets for ``bucket_max`` calc
     - num_sites (sites) in healthy; reduced during degraded
   * - ``peering_crush_bucket_barrier``
     - CRUSH type level (e.g., datacenter)
     - Same as replica — the failure domain
   * - ``peering_crush_mandatory_member``
     - CRUSH bucket that must be represented
     - Set to surviving site during degraded mode

**Example values for num_sites=2, K=2, M=1 (size=6):**

.. list-table::
   :header-rows: 1

   * - Stretch State
     - bucket_count
     - bucket_target
     - bucket_max
     - mandatory_member
   * - Healthy
     - 2
     - 2
     - 3
     - NONE
   * - Degraded
     - 1
     - 1
     - 6
     - surviving_site
   * - Recovery
     - 1
     - 1
     - 6
     - surviving_site
   * - Healthy (restored)
     - 2
     - 2
     - 3
     - NONE

``bucket_max = ceil(size / bucket_target)`` — in healthy mode, ``6 / 2 = 3 =
K+M``. This naturally prevents more than one copy of each shard per site.

11.7 Network Partition Handling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The existing Ceph stretch cluster mechanism handles network partitions as
follows:

- **MON-Based Site Election**: The MONs are distributed between sites (plus a
  tie-break site). When the inter-site link is severed, the MONs elect which
  site continues to operate.
- **OSD Heartbeat Discovery**: OSDs heartbeat the MONs and will discover if
  they are attached to the losing site. OSDs on the losing site stop
  processing IO.
- **Lease-Based Read Gating**: A lease determines whether OSDs are permitted to
  process read IOs. The surviving site must wait for the lease to expire before
  new writes can be processed, ensuring no stale reads are served by the losing
  site during the transition.

This mechanism does not preclude more complex failure scenarios that may also
need to be treated as site failures:

- **Asymmetric Network Splits**: OSDs at a remote site may be able to
  communicate with a local MON even though the MONs have determined a site
  failover has occurred.
- **Partial Site Failures**: Loss of a rack or subset of OSDs within a site may
  warrant treating the site as failed (e.g., if the remaining shards fall below
  the per-site ``min_size`` threshold defined in Section 11.2).

11.8 Online OSDs in Offline Sites
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When a site is marked as offline but individual OSDs within that site remain
reachable, they must be removed from the up set that is presented to the
Objecter and peering state machine. The mechanism ties to the ``min_size``
logic described in Section 11.2:

- **Removal**: If fewer than ``k`` OSDs from a site are present in the up set,
  the remaining OSDs for that site are also removed. This forces a site
  failover and prevents partial-site IO.
- **Reintegration**: When enough OSDs return to meet the per-site threshold,
  they are added back into the up set. Standard peering will handle resyncing
  data — no new recovery code is required for reintegration.


12. Scrub Behaviour — *R1*
-----------------------------

The only modification required to scrub is how it verifies the CRCs received
from each shard during deep scrub. The deep scrub process must:

1. **Verify the coding CRC**, where the EC plugin supports it. This is current
   behaviour but must be extended to cover the replicated shards (i.e.,
   verifying the coding relationship within each site's stripe independently).
2. **Verify cross-replica CRC equivalence**: corresponding shards in each
   replica must have the same CRC. For example, SiteShard 0 at Site A must
   match SiteShard 0 at Site B.

Scrub never reads data to the Primary — it only collects and compares CRCs
reported by each OSD. This means inter-site bandwidth is not a significant
concern, and a primary-centric scrub process continues to make sense for
replicated EC stretch clusters.


13. Migration Path
-------------------

13.1 Pool Migration — *R1*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Migration from an existing EC pool to a replicated EC stretch cluster pool will
leverage the **Pool Migration** design, which is being implemented for the
Umbrella release. This document does not define a separate migration mechanism.

13.2 In-Place Replica Count Modification — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In a later release, users will be able to modify ``num_sites`` for an existing pool by
swapping in a new EC profile that is otherwise identical (same ``k``, ``m``,
plugin, etc.) but specifies a different ``num_sites`` value. Upon profile change, the
CRUSH rule will be updated to reflect the new replica count, and the standard
recovery process will automatically perform all necessary expansion (or
contraction) to match the new configuration — no manual data migration is
required.


14. Implementation Order
-------------------------

This section defines the phased implementation plan, broken into single-sprint
stories.

14.1 R1 — Read-Optimised Stretch Cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

R1 delivers a functional EC stretch cluster with local read optimisation
(comparable to what Replica stretch clusters provide today). All writes and
recovery traverse the inter-site link via the Primary.

1. **CRUSH MSR Rule Generation for Replicated EC**
   Implement automatic CRUSH MSR rule creation from the ``num_sites`` and
   ``site_failure_domain`` pool parameters (Section 4). Validate that the acting
   set places ``k+m`` shards per ``site_failure_domain``.

2. **EC Profile Extension: ``num_sites`` and ``site_failure_domain`` Parameters**
   Extend the EC profile and pool configuration to accept and validate ``num_sites``
   and ``site_failure_domain``. Wire these into pool creation and ``ceph osd pool``
   commands (Section 2).

3. **Primary-Capable Shard Selection for Replicated EC**
   Extend the non-primary shard selection algorithm to designate the first data
   shard and all parity shards per replica as primary-capable (Section 10.1).

4. **Stretch Mode and Peering for Replicated EC**
   Broken into the following sub-stories (see Sections 11.1–11.6):

   a. **Pool Stretch Set/Unset for EC** (OSDMonitor): Allow ``osd pool
      stretch set/unset`` on EC pools with ``num_sites > 1``. Add EC-specific
      ``min_size`` range validation (Section 11.4.1).
   b. **Enable/Disable Stretch Mode for EC** (OSDMonitor): Allow
      ``mon enable_stretch_mode`` when the cluster has EC pools with
      ``num_sites > 1``. Set ``min_size = r × (k+m) − m`` (Section 11.4.2).
   c. **Stretch Mode Transitions for EC** (OSDMonitor): Implement
      degraded/recovery/healthy transitions. On site failure, reduce
      ``min_size`` by ``k+m``; on recovery, restore it (Sections 11.4.3–5).
   d. **EC Peering with Stretch Constraints** (PeeringState): Create
      ``calc_ec_acting_stretch`` to respect both shard identity and CRUSH
      ``bucket_max`` constraints. Extend async recovery checks
      (Section 11.5).
   e. **is_recoverable / is_readable for Stretch EC**: Verify and extend
      ``ECRecPred`` and ``ECReadPred`` to account for the larger shard set
      and per-site constraints (Section 11.5.3).

5. **Primary Write Fan-Out to All Shards (Direct-to-OSD Writes)**
   Extend the Primary write path to encode and distribute ``num_sites × (k + m)``
   shard writes directly to all OSDs in the acting set (Section 8.1).

6. **Direct-to-OSD Reads with Primary Fallback**
   Extend EC Direct Reads to the stretch topology: clients read locally and
   redirect all failures to the Primary (Sections 7.2, 7.3).

7a. **Single-OSD Recovery (Within-Site)**
    Extend recovery so the Primary can recover a single missing OSD within any
    replica. The Primary reads shards from the affected replica, reconstructs
    the missing shard, and pushes it to the replacement OSD (Section 9.1).

7b. **Full-Site Recovery**
    Extend recovery to handle full-site loss: the Primary reads from its local
    (surviving) replica, encodes the full stripe, and pushes all ``k+m`` shards
    to the recovering site's OSDs. Also covers multi-OSD failures within a
    single site (Section 9.1).

8. **Scrub CRC Verification for Replicated Shards**
   Extend deep scrub to verify cross-replica CRC equivalence for corresponding
   shards (Section 12).

9. **Network Partition and Site Failover Integration**
   Validate the existing MON-based site election and OSD heartbeat mechanisms
   work correctly with the replicated EC acting set (Section 11.7).

10. **End-to-End 2-Site Integration Testing**
    Full integration test suite covering read, write, recovery, scrub, and
    site-failover scenarios for a 2-site (``num_sites=2``) configuration.

11. **Inter-Site Statistics**
    Add perf counters tracking cross-site bytes, operation counts, latency
    histograms, and local vs remote read ratios to give operators visibility
    into inter-site link utilisation (Section 17).

14.2 Later Release — Recovery Bandwidth Optimisation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These stories reduce inter-site link bandwidth consumed during recovery, in
priority order.

1. **Cost-Based Recovery Planning**
   Implement the assessment and plan-selection algorithm that compares
   cross-site transfer costs for each recovery strategy (Section 9.2).

2. **Recovery Plan Message and Remote Primary Execution**
   Define the "Recovery Plan" message format and implement Remote Primary
   execution of delegated recovery within its ``site_failure_domain``
   (Section 9.3).

3. **Push Recovery Fallback**
   Implement the path where the Primary reconstructs the full object and
   pushes to the Remote Primary with shard-write instructions (Section 9.4.1).

4. **Primary Domain Fallback (Cross-Site Read for Local Recovery)**
   Allow the Primary to read shards from remote sites when its own domain
   lacks sufficient chunks, only when cost-based planning selects it
   (Section 9.4.2).

14.3 Later Release — Write Bandwidth Optimisation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These stories reduce inter-site link bandwidth consumed during writes, in
priority order.

1. **Replicate Transaction Message**
   Implement the replicate transaction message that sends raw write data to
   the Remote Primary instead of pre-encoded shards. The Remote Primary
   encodes locally (Section 8.2).

2. **Independent PG Log Generation at Remote Primary**
   Enable the Remote Primary to generate its own PG log entries from the
   replicate transaction, including OBC population and the debug equivalence
   mode (Sections 10.2, 10.3).

3. **Client Writes Direct to Remote Primary**
   Enable clients to write to their local Remote Primary, which stashes the
   data, replicates to the Primary, and fans out locally only after receiving
   write-permission. Includes the write-permission/completion message
   mechanism to maintain global consistency (Section 8.3).

4. **Hybrid Write Path (Replicate + Direct-to-OSD)**
   Support mixed write strategies within a single transaction when sites are
   in different health states (Section 8.4).

14.4 Later Release — Additional Enhancements
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These stories improve resilience and flexibility but are lower priority than
bandwidth optimisations.

1. **Read from Remote Primary**
   Enable clients to send reads to their local Remote Primary for local
   reconstruction, including the Synchronous Recovery Set mechanism
   (Sections 7.4, 7.4.1).

2. **Direct-to-OSD Failure Redirection to Remote Primary**
   Upgrade direct-read failure handling to redirect to the local Remote
   Primary instead of the global Primary (Section 7.5).

3. **Per-Site min_size with Site Failover**
   Implement per-site minimum shard thresholds that automatically trigger
   site failover (Section 11.2, later release).

4. **Online OSDs in Offline Sites Handling**
   Implement removal of residual online OSDs from the up set when their site
   is offline (Section 11.8).

5. **In-Place Replica Count Modification**
   Allow ``num_sites`` to be changed on an existing pool via EC profile swap
   (Section 13.2).

6. **3-Site (``num_sites=3``) Full Integration Testing**
   Full integration and real-world testing of 3-site configurations
   (Section 5).


15. Upgrade & Backward Compatibility
--------------------------------------

Replicated EC pools with ``num_sites=1`` are fully backward compatible. An ``num_sites=1``
profile produces a standard ``(k+m)`` acting set with no additional replication,
no new on-disk format, and no new wire messages. Existing OSDs and clients will
handle these pools without modification, because the resulting behaviour is
identical to a conventional EC pool.

Pools with ``num_sites > 1`` introduce a larger acting set (``num_sites × (k + m)`` shards),
new CRUSH MSR rules, and — in later releases — new inter-OSD messages
(Replicate Transaction, Read Permissions, etc.). To prevent mixed-version
clusters from misinterpreting these pools:

- A new **OSD feature bit** will gate the creation of ``num_sites > 1`` profiles. The
  monitor will reject pool creation or EC profile changes that set ``num_sites > 1``
  unless all OSDs in the cluster advertise this feature bit.
- This ensures that every OSD in the cluster understands the extended acting
  set semantics, shard numbering, and any new message types before an ``num_sites > 1``
  pool can be instantiated.
- No data migration is required when upgrading: the feature bit is purely an
  admission control mechanism. Once all OSDs are upgraded and the bit is
  present, ``num_sites > 1`` pools can be created normally.


16. Kernel Changes
-------------------

The kernel RBD client (``krbd``) implements its own EC direct read path,
independent of the userspace ``librados`` client. For replicated EC pools, the
``krbd`` direct read logic must be updated to understand the ``num_sites × (k + m)``
shard layout so that it can identify and target local shards within the
client's ``site_failure_domain``. Without this change, ``krbd`` may attempt direct
reads to shards in a remote site, negating the locality benefit.

The required modification is small — the shard selection logic needs to account
for the replica stride when choosing which shard to read from — but it is a
kernel-side change and therefore follows the kernel release cycle independently
of the Ceph userspace releases.


17. Inter-Site Statistics — *R1*
----------------------------------

Operators need visibility into inter-site link utilisation to validate that
the stretch EC design is delivering its locality benefits and to plan capacity.
The following statistics will be exposed per-pool and per-OSD:

- **Cross-site bytes sent / received**: Total bytes transferred to/from OSDs in
  remote ``site_failure_domain``\s, broken down by operation type:

  - Write fan-out (shard data sent to remote site)
  - Recovery push (shard data sent during recovery)
  - Remote read (shard data read from remote site for reconstruction or
    medium-error recovery)

- **Cross-site operation counts**: Number of cross-site sub-operations, broken
  down by type (sub-write, sub-read, recovery push).

- **Cross-site latency**: Histogram (p50 / p95 / p99) of round-trip latency for
  cross-site sub-operations, useful for detecting link degradation.

- **Local vs remote read ratio**: For direct-read-enabled pools, the fraction of
  reads served locally versus those that fell back to the Primary across a site
  boundary. A high remote ratio indicates a locality problem.

These counters will be exposed through the existing ``ceph perf`` counter
infrastructure and will be queryable via ``ceph tell osd.N perf dump`` and
the Ceph Manager dashboard. They should also be available at the pool level
via ``ceph osd pool stats``.

.. note::

   The inter-site classification relies on comparing the CRUSH location of the
   source OSD with the destination OSD's ``site_failure_domain``. This comparison is
   already performed during shard fan-out; the statistics layer adds only
   counter increments on the existing code path.
