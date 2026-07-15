========================================================
Design Document: Ceph Erasure Coded (EC) Stretch Cluster
========================================================

.. note::

   **Phased Delivery (R1, R2, …)**

   This document distinguishes between **R1** and **later release** work.
   Sections and sub-sections are annotated accordingly. R1 delivers:

   - **Zone-local direct reads** (good path only; any failure redirects to the
     Primary)
   - **Writes via Primary** (all write IO traverses the inter-zone link; no
     Zone Primary encoding)
   - **Recovery from Primary** (Primary reads all data and writes all remote-zone
     shards; no inter-zone bandwidth optimization)

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


1. Intent & High-Level Summary
-------------------------------

The primary goal of this feature is to support a Replicated Erasure Coded (EC)
configuration within a multi-zone Ceph cluster (Stretch Cluster), building upon
the Fast EC infrastructure.

.. note::

   Throughout this document, a *zone* refers to a group of OSDs within a single
   CRUSH failure domain — typically a data center, but it could be any CRUSH
   bucket type (e.g., rack, room). The key characteristic of a zone boundary is
   that traffic crossing it incurs a significant performance penalty: higher
   latency and/or lower bandwidth compared to intra-zone communication.

   The term *inter-zone link* refers to the network connection between these
   failure domains (e.g., the dedicated link between data centers). This is
   typically the most bandwidth-constrained and latency-sensitive path in the
   cluster. There is a strong desire to minimize inter-zone link traffic, even
   to the extent of issuing additional zone-local reads to avoid sending data across
   this link.

Currently, Ceph Stretch clusters typically rely on Replica to ensure data
availability across zones (e.g., data centers). While Erasure Coding provides
storage efficiency, applying a standard EC profile naively across a stretch
cluster introduces significant operational problems.

Consider a 2-zone stretch cluster using a standard EC profile of ``k=4, m=6``.
While the high parity count provides sufficient fault tolerance to survive a
full zone loss, this configuration suffers from three fundamental
inefficiencies:

1. **Write Bandwidth Waste**: Every write must distribute all ``k+m`` chunks
   across both zones. The coding (parity) shards are unnecessarily duplicated
   over the inter-zone link, consuming bandwidth that scales linearly with the
   number of shards.
2. **Reads Must Cross Zones**: Serving a read requires retrieving at least ``k``
   chunks, which in general will span both zones. Every read operation incurs
   inter-zone link latency.
3. **Recovery is Inter-Zone Link Bound**: After a zone failure, recovering the
   lost chunks requires reading surviving chunks across the inter-zone link,
   placing the entire recovery burden on the most constrained network path.

This feature introduces a hybrid approach to eliminate these problems: creating
Erasure Coded stripes (defined by ``k + m``) and replicating those stripes
``zones`` times across a specified topology level (e.g., Data Center). Each zone
holds a complete, independent copy of the EC stripe, meaning reads and recovery
can be performed entirely within a single zone. This combines the zone-local storage
efficiency of EC with the zone-level redundancy of a stretch cluster, while
minimizing inter-zone link usage to write replication only.

2. Proposed Configuration (CLI) Changes
---------------------------------------

To support this topology, the EC Pool configuration interface will be expanded 
directly within the pool creation command.  We intend to address a number of
issues with the current CLI design:

* Create-then-set flow.  A typical setup procedure of a pool requires multiple CLI
  commands (e.g. create replica, then set num copies, then set stretched, etc... )
* Remove the need for an EC profile.  Indeed, stretch mode EC pools will be mutually
  exclusive with the use of a profile.

The current CLI behaviour of accepting either positional or non-positional arguments
will be maintained, however no positional arguments will be added for the new
functionality.  

Supporting stretch EC pools will require changes to several CLI commands. The design 
document will focus on just the changes that will be made to the pool create CLI 
to give an idea of how the new CLI will work. Similar changes will be made to the 
CLIs that allow modification of a pool. It also expected that changes will be made 
to the CLIs that control stretch mode.



2.1 ceph osd pool create
~~~~~~~~~~~~~~~~~~~~~~~~

The ceph osd pool create command will be extended to become a parameterized command.

For backward compatibility, the positional arguments will be maintained and can be
specified along side the new paramaters. 

2.2.1 Full command syntax
^^^^^^^^^^^^^^^^^^^^^^^^^

The full command syntax is listed here. Refer to the later sections for pool-type specific details.

.. code-block:: text

   ceph osd pool create {pool_name} 
        [{pg_num} | --pg_num <pg_num>]
        [{pgp_num} | --pgp_num <pgp_num>]
        [{replicated | erasure} | --pool_type <replicated|erasure>]
        [{expected_num_objects} | --expected_num_objects <expected_num_objects>]
        
        # CRUSH Placement Options (Mutually Exclusive)
        [ {crush_rule_name} | --crush_rule_name <crush_rule_name> | 
          [--crush_root <crush_root>] [--zone_failure_domain <zone_failure_domain>] [--osd_failure_domain <osd_failure_domain>] [--crush_device_class <class>] ]
        
        # Replica-Specific Options
        [--size <size>]
        
        # Erasure-Specific Options
        [--data_shards <num_data_shards>]
        [--coding_shards <num_coding_shards>]
        [--stripe_unit <stripe_unit>]
        [ {erasure_code_profile} | --profile <profile_name> ]
        
        # Topology and Redundancy
        [--zones <num_zones>]
        [--min_size <min_size>]
        
        # Autoscaling and General Config
        [--autoscale_mode=<on,off,warn>]
        [--pg_num_min <pg_num_min>]
        [--pg_num_max <pg_num_max>]
        [--bulk]
        [--target_size_bytes <target_size_bytes>]
        [--target_size_ratio <target_size_ratio>]
        [--crimson]
        [--yes_i_really_mean_it]
        
        # Legacy Options
        [--stretch_mode]

2.2.2 Legacy positional syntax
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following syntax, taken from the current docs, will be maintained for backward compatibility:

.. code-block:: text

   ceph osd pool create {pool_name} [{pg_num} [{pgp_num}]] [replicated] \
            [crush_rule_name] [expected_num_objects]

.. code-block:: text

   ceph osd pool create {pool_name} [{pg_num} [{pgp_num}]] erasure \
            [erasure_code_profile] [crush_rule_name] [expected_num_objects] [--autoscale_mode=<on,off,warn>]

This syntax can be used with the new parameters which do not directly conflict.  For example --pg_num cannot 
be used with its positional counterpart. 

2.2.1 Basic Parameters
^^^^^^^^^^^^^^^^^^^^^^

These are the primary parameters required for standard deployments.

**--pool_type** (or positional equivalent)
  - *Definition*: The type of pool to create. 
  - *Values*: ``replicated`` or ``erasure``.
  - *Default Value*: Derived from ``osd_pool_default_type`` if omitted, but highly recommended to specify.

**--size**
  - *Definition*: The number of OSDs the data is striped over for each object. (Replica only)
  - *Note*: Parameter will be ignored, rather than rejected for EC pools, for backward compatibility.

**--data_shards** (or **--k**)
  - *Definition*: Within a zone, the number of OSDs the data is striped over for each object. (EC only)

**--coding_shards** (or **--m**)
  - *Definition*: Within a zone, the number of OSDs the coding shards are striped over. (EC only)

**--zones**
  - *Definition*: For a stretched cluster configuration defines the number of zones, each which store a full replica of the pool. (EC or Replica)
  - *Default Value*: ``1``
  - *Behavior*: Setting this to >1 creates a stretched pool.    
     A non-stretched pool achieves redundancy accross OSDs.  A stretched pool creates redundancy
     accross ``zones``. 
  - *Pool Size*: The resulting pool ``size`` is ``zones × (k + m)``.


2.2.2 Advanced
^^^^^^^^^^^^^^

These parameters are intended for advanced users and offer finer control over the cluster layout.

**--stripe_unit**
  - *Definition*: The amount of data in a shard, per stripe. Sometimes referred to as "chunk size".  (EC only)
  - *Default Value*: 16k for Fast EC, 4k for classic EC.
  - *Purpose*: Where beneficial for a well-known access pattern, the stripe unit can
    be tuned to any 4k-divisible value. 

**--zone_failure_domain**
  - *Definition*: The CRUSH bucket type over which zone-redundancy is achieved.
  - *Default Value*: ``datacenter``
  - *Purpose*: This is the CRUSH bucket type that defines a zone.
  - *Note*: Mutually exclusive with ``--crush_rule``
  
**--osd_failure_domain**
  - *Definition*: The CRUSH bucket type over which OSD-redundancy is achieved.
  - *Default Value*: ``host``
  - *Note*: Mutually exclusive with ``--crush_rule``

**--crush_device_class**
  - *Definition*: Restrict placement to devices of a specific class (e.g., ``ssd`` or ``hdd``), using the CRUSH device class names in the CRUSH map.
  - *Purpose*: Only required if a cluster has a mixture of different classes of OSD.
  - *Note*: Mutually exclusive with ``--crush_rule``

**--crush_root**
  - *Definition*: The root of the CRUSH tree to use.  (R2 only)
  - *Default Value*: The cluster root (``default``).
  - *Topology and Validation*: Specifying the CRUSH root to use (defaults to ``default``), the CRUSH level for a zone (defaults to ``datacenter``), and the number of zones (defaults to ``1``) is sufficient to define the pool's placement:

    - Example 1: In a cluster with 2 datacenters, specifying ``--zones 2`` will create a stretch pool across the 2 datacenters.
    - Example 2: In a cluster with 2 datacenters, specifying ``--crush_root DC1`` will create an pool completely contained in DC1.
    - Validation: If there are N datacenters with the same root and you specify a number of zones M != N, the command will fail because the specified number of zones is different from the number of zones in the CRUSH hierarchy.
    - Custom Rules: If users want to use a subset of zones (e.g., a special 3-datacenter configuration), they must specify a custom CRUSH rule. A custom CRUSH rule is mutually exclusive with specifying the CRUSH root and/or CRUSH level.

  - *Operational Restrictions*: When there are stretch pools, adding or moving a CRUSH bucket that impacts the number of zones for the pool will require a ``yes-i-really-mean-it`` flag, as this is liable to break things.
  - *Note*: Mutually exclusive with ``--crush_rule``

**--min_size**
  - *Definition*: The minimum number of shards required to serve I/O within a zone.
  - *Note*: The value should be within allowed specified ranges. For replica pools this range is ``1-<num replicas>``, for EC pools this is ``<num data shards>-<num data shards>+<num coding shards>``.

**--crush_rule**
  - *Definition*: Use this CRUSH rule, instead of an auto-generated rule. 
  - *Purpose*: Create a bespoke CRUSH rule for advanced use cases not covered by the auto rule generation above. 
  - *Note*: Mutually exclusive with ``--crush_root``, ``--osd_failure_domain`` and ``--zone_failure_domain``

**--profile**
  - *Definition*: The legacy EC Profile to use. 
  - *Note*: Mutually exclusive with ``--zones``: Cannot be used with multi-zone configurations. 

.. note:: 

   The following parameters are existing, standard pool configuration options included here for completeness.

**--pg_num**
  - *Definition*: The total number of placement groups for the pool.

**--pgp_num**
  - *Definition*: The total number of placement groups for placement purposes.
  - *Note*: This should never be modified. Purely here for backward compaitbility.

**--expected_num_objects**
  - *Definition*: The expected number of objects for this pool, used to pre-split placement groups at pool creation.

**--autoscale_mode**
  - *Definition*: The auto-scaling mode for placement groups.
  - *Values*: ``on``, ``off``, or ``warn``.

**--pg_num_min**
  - *Definition*: The minimum number of placement groups when auto-scaling is active.

**--pg_num_max**
  - *Definition*: The maximum number of placement groups when auto-scaling is active.

**--bulk**
  - *Definition*: Flags the pool as a "bulk" pool to pre-allocate more placement groups automatically.

**--target_size_bytes**
  - *Definition*: The expected total size of the pool in bytes, used to guide PG autoscaling.

**--target_size_ratio**
  - *Definition*: The expected ratio of the cluster's total capacity this pool will consume, used for PG autoscaling.

**--crimson**
  - *Definition*: Flags the pool to run on Crimson OSD.
  - *Note*: Crimson OSD is experimental.

**--yes_i_really_mean_it**
  - *Definition*: Internal safety override flag. In the context of pool creation, it is specifically used to allow the creation of hidden or system-reserved pools whose names begin with a dot (e.g., ``.rgw.root``).


2.2.3 Deprecated
^^^^^^^^^^^^^^^^

These are used for backward compatibility only.

**--stretch_mode**
  - *Definition*: Deprecated parameter for Replica pools which configures two zones with half the specified number of replica copies in each zone. Not supported for EC pools.
  - *Note*: Mutually exclusive with ``--zones`` and erasure pools.


2.3 Examples
~~~~~~~~~~~~

The following are examples of how the new parameterized ``ceph osd pool create`` command simplifies pool creation across different topologies.

**Example 1: Basic Replicated Pool**
Create a standard 3-way replicated pool containing 128 placement groups:

.. code-block:: bash

   ceph osd pool create my_rep_pool --pool_type replicated --size 3 --pg_num 128

**Example 2: Standard Erasure Coded Pool**
Create an erasure-coded pool using a ``k=4, m=2`` configuration (yielding a size of 6 shards) with 64 placement groups:

.. code-block:: bash

   ceph osd pool create my_ec_pool --pool_type erasure --data_shards 4 --coding_shards 2 --pg_num 64

**Example 3: Stretched Replicated Pool**
Create a replicated pool that spans across two datacenters, achieving a total size of 4 (2 replicas in each datacenter):

.. code-block:: bash

   ceph osd pool create stretch_rep --pool_type replicated --size 4 --zones 2 --zone_failure_domain datacenter

**Example 4: Stretched Erasure Coded Pool**
Create an erasure-coded pool stretched across two racks. Using a ``k=4, m=2`` configuration per zone across 2 zones creates a total pool size of 12 shards (4 data and 2 coding per rack):

.. code-block:: bash

   ceph osd pool create stretch_ec --pool_type erasure --data_shards 4 --coding_shards 2 --zones 2 --zone_failure_domain rack

**Example 5: Single Datacenter Erasure Coded Pool**
Create an EC pool confined entirely to a specific datacenter using the ``--crush_root`` parameter:

.. code-block:: bash

   ceph osd pool create dc1_ec --pool_type erasure --data_shards 4 --coding_shards 2 --crush_root DC1 

**Example 6: Bulk Erasure Coded Pool with Autoscaling Limits**
Create an EC pool where the system automatically scales the PG count but enforces a minimum boundary, marking it as a bulk pool:

.. code-block:: bash

   ceph osd pool create bulk_ec --pool_type erasure --data_shards 6 --coding_shards 3 --autoscale_mode on --pg_num_min 128 --bulk


1. Approaches Considered but Rejected
-------------------------------------

We evaluated and rejected the following two alternative designs:

3.1 Multi-layered Backends
~~~~~~~~~~~~~~~~~~~~~~~~~~

This approach involved re-using the existing ``ReplicationBackend`` to manage
the top-level replication, with EC acting as a secondary layer.

- *Reason for Rejection*: This would require complex, multi-layered peering
  logic where the replication layer interacts with the EC layer. Ensuring the
  correctness of these interactions is difficult. Additionally, the front-end
  and back-end interfaces of the two back ends differ significantly (e.g.,
  support for synchronous reads), which would require substantial refactoring.
- *Advantage of Proposed Solution*: Our chosen bespoke approach minimizes
  changes to the peering state machine and offers better potential for recovery
  during complex failure scenarios.

3.2 LRC (Locally Repairable Codes) Plugin
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This approach involved configuring the existing LRC plugin to provide multi-zone
EC semantics. While LRC is designed for locality-aware erasure coding, it does
not address the core problems this design aims to solve:

- *Reason for Rejection*:

  1. **Writes still cross zones**: LRC distributes all chunks (data, local
     parity, and global parity) across the full CRUSH topology. Every write
     operation sends chunks over the inter-zone link, offering no bandwidth
     savings over standard EC.
  2. **Reads still cross zones**: Reading the original data requires ``k`` data
     chunks, which are spread across all zones. LRC provides no mechanism for a
     single zone to independently serve reads.
  3. **Recovery remains primary-centralized**: In the current Ceph EC
     infrastructure, all recovery operations are coordinated by the Primary OSD.
     While LRC defines local repair groups at the coding level, the EC back end
     would still need to be extended to delegate recovery execution to remote
     zones — the same complexity required by this proposal.

- *Advantage of Proposed Solution*: The replicated EC stripe approach places a
  complete ``(k+m)`` set at each zone, which inherently enables zone-local
  reads, zone-local single-OSD recovery (without any special coding scheme), and
  limits inter-zone link usage to write replication. It achieves better locality
  than LRC with less infrastructure complexity.


4. Configuration Logic (Preliminary)
--------------------------------------

The interactions between these parameters imply a hierarchy in the CRUSH
rule generation:

- **Top Level**: The CRUSH rule selects ``zones`` buckets of type
  ``zone`` (e.g., select 2 data centers).
- **Lower Level**: Inside each selected ``zone``, the CRUSH rule
  selects ``k+m`` OSDs to store the chunks.

This leverages standard CRUSH mechanisms — no changes to CRUSH
itself are required.


5. Multi-Zone & Topology Considerations
-----------------------------------------

While this architecture is capable of supporting N-zone configurations,
specific attention is given to the common 2-zone High Availability (HA) use
case.

- **Reference Diagrams**: For clarity, architectural diagrams and examples
  within this design documentation will primarily depict a 2-zone HA
  configuration (``zones=2``, with 2 data centers).
- **Logical Scalability**: The design is logically N-way capable. The parameter
  ``zones`` is not limited to 2; the system supports any valid CRUSH topology where
  ``zones`` failure domains exist.
- **Testing Strategy**: Testing will follow a phased approach:

  1. **Unit Tests (Initial)**: The unit test framework will include tests for
     both 2-zone (``zones=2``) and 3-zone (``zones=3``) configurations, validating the
     core peering and recovery logic for N-way topologies.
  2. **Full 2-Zone Testing (Initial Release)**: 2-zone configurations will be
     fully tested end-to-end for the initial release, covering all read, write,
     recovery, and failure scenarios.
  3. **Full 3-Zone Testing (Later Release)**: Full integration and real-world
     testing of 3-zone configurations will be deferred to a subsequent release.


5.1 Single-Zone Pools
~~~~~~~~~~~~~~~~~~~~~

The use case for "single-zone" pools is a Ceph cluster which is split across multiple data centers, but the redundancy is provided by the application. Here, the user can specify a pool which is restricted to a single data center. It should be noted that this means that loss of an inter-zone link will lead to an entire zone being lost (something that would not be the case for two independent clusters).


6. Topologies and Terminology
-------------------------------

To illustrate the relationship between the replication layer, the EC layer, and
the physical topology, we define the following terms and visualization.

6.1 Logical Diagram (2-Zone HA)
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
  |           (zones=2, zone_failure_domain=DC)          |
  +--------------------------+------------------------+
                             |                           
              +--------------+--------------+            
              |                             |            
              v                             v            
  +-----------+---------+       +-----------+-----------+
  |                     |       |                       |
  |                     |       |                       |
  |    Data Center A    |       |     Data Center B     |
  |  (Replica/Zone 1)   |       |   (Replica/Zone 2)    |
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
 Primary                         Zone                  
                                 Primary                            

6.2 Key Definitions
~~~~~~~~~~~~~~~~~~~~

**Primary**
  The primary OSD in the acting set for a Placement Group (PG). This OSD is
  responsible for coordinating writes and reads for the PG.
  (Standard Ceph terminology.)

**Shard**
  The globally unique position of a chunk within the pool's acting set. Shards
  are numbered ``0`` through ``zones × (k + m) - 1``. In the diagram above, Zone A
  holds Shards 0, 1, 2 and Zone B holds Shards 3, 4, 5.

**Zone-local**
  An OSD or shard is zone-local if it shares the same ``zone`` as another
  OSD or shard.

**Zone Primary**
  An internal EC convention. The Zone Primary is the first primary-capable
  shard in the acting set that resides in a given zone.

  These zone primaries will be used in (post-R1) stretch clusters to perform
  local-to-zone erasure coding.  The intent is to minimize inter-zone bandwidth
  requirements. 

**Remote-zone Shard**
  A relative term referring to a shard in a different zone.  For example: "When 
  recovering data in Zone A, a remote-zone shard may be used to read data"


7. Read Path & Recovery Strategies
----------------------------------

This architecture supports multiple read strategies to optimize for locality
and handle failure scenarios. The strategies are presented in order of
increasing complexity: starting with the baseline read-from-Primary path,
then layering direct-read optimizations on top.

7.1 Read from Primary — *R1*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The standard read path involves the client contacting the Primary OSD.

- **Local Priority**: The Primary will prioritize reading from local-to-zone OSDs
  (within its own ``zone``) to serve the request or recover data
  (reconstruct the stripe). This minimizes inter-zone link traffic.

7.1.1 Zone-local Recovery
^^^^^^^^^^^^^^^^^^^^^^^^^

When the Primary cannot serve a read from a single zone-local shard (e.g., because
a shard is missing or degraded), it reconstructs the data from the remaining
zone-local shards within its ``zone``.

7.1.2 Zone-Local Recovery
^^^^^^^^^^^^^^^^^^^^^^^^^

If the Primary cannot reconstruct data using only zone-local shards, it may issue
direct reads to remote-zone OSDs. This cross-zone read path serves two purposes:

- **Backfill / Recovery**: When a zone-local OSD is down or being backfilled, the
  Primary can read the corresponding shard from the remote zone to recover the
  missing data. It is likely that a zone with insufficient EC chunks to
  reconstruct locally would be taken offline by the stretch cluster monitor
  logic, but this fallback ensures availability during transient states.
- **Medium Error Recovery**: If a zone-local OSD returns a medium error (e.g.,
  unreadable sector), the Primary can recover the affected data by reading from
  remote-zone shards and reconstructing locally.

7.2 Direct Reads — Primary Zone — *R1*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This feature extends the existing "EC Direct Reads" capability (documented
separately) to the stretch cluster topology. A client co-located with the
Primary can read data directly from a zone-local shard, bypassing the Primary
entirely on the good path. **No new code is required** — this is the standard
EC Direct Read path operating over stretch-cluster shard numbering. Any failure
falls back to the Primary read path (Section 7.1).

.. mermaid::

   sequenceDiagram
       title Direct Read — Primary Zone

       participant C as Client
       participant P as Primary
       participant ZS as Zone-local Shard

       C->>ZS: Direct Read
       activate ZS
       ZS->>C: Complete
       deactivate ZS

       C->>ZS: Direct Read
       activate ZS
       ZS->>C: -EAGAIN
       deactivate ZS
       C->>P: Full Read
       activate P
       P->>ZS: Read
       activate ZS
       ZS->>P: Read Done
       deactivate ZS
       P->>C: Done
       deactivate P

- **Failure Handling & Redirection (R1)**:

  Any failure encountered during a direct read — whether a missing shard,
  ``-EAGAIN`` rejection, or medium error — results in the client redirecting
  the operation to the **Primary** (regardless of the Primary's location).

7.3 Direct Reads — Zone-Local — *R1*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The client will read from a zone-local shard.

- **Zone-Local Shard Identification**: The client determines which OSDs in the
  acting set reside within its own zone by comparing OSD CRUSH
  locations against its own. This identifies the ``k+m`` shards of the local
  zone. The client then selects a suitable set of data shards for the
  direct read.

- **Unavailable Zone-local OSDs**: If the zone-local shards are not available (e.g., the
  zone-local OSDs are down or the client cannot identify any zone-local replica), the
  client does not attempt a direct read and instead directs the operation to
  the Primary. In later releases this will be improved to redirect to the
  local Zone Primary instead.

- **Direct Read Execution**: If a zone-local shard is available, the client sends
  the read directly to that OSD. As detailed in the ``ec_direct_reads.rst``
  design, it is generally acceptable for reads to overtake in-flight writes.
  However, the OSD is aware if it has an "uncommitted" write (a write that
  could potentially be rolled back) for the requested object. If such a write
  exists, or if the client's operation requires strict ordering (e.g. the
  ``rwordered`` flag is set), the OSD rejects the operation with ``-EAGAIN``.
  Data access errors (e.g., a media error on the underlying storage) also cause
  the OSD to reject the operation.

- **Failure Handling & Redirection (R1)**:

  An -EAGAIN will cause the client to retry the op. For R1, the op will be 
  redriven to the primary.  R2 will redrive the op to the zone primary. 

7.4 Read from Zone Primary — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the R1 release, the primary will handle all failures. In later releases,
an op which cannot be processed as a direct read will be directed at the 
zone primary instead. The zone primary will attempt to reconstruct the
read data from the zone-local shards directly. 

A conflict due to an uncommitted write will be continue to be handled by
the primary, as the zone primary must also rejected an op if an
uncommitted write exists for that object. 

- **Zone-local Recovery**: A read directed to a Zone Primary will attempt to serve
  the request by recovering data using only OSDs within the same data center
  (the remote zone).
- **Zone Degradation**: If a zone has insufficient redundancy to reconstruct
  data locally, that zone should be taken offline to clients rather than serving
  reads that would require inter-zone link access. (How? - Needs to be covered elsewhere)
- **``-EAGAIN`` Behavior**: The Zone Primary will return ``-EAGAIN`` only in
  short-lived transient conditions:


7.4.1 Safe Shard Identification & Synchronous Recovery — *Later Release*
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

By default, the Zone Primary does not inherently know which shards are
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
  is empty. This ensures the Zone Primary is *conservatively stale*: it may
  reject reads it could safely serve, but will never read from an inconsistent
  shard.

- **Initial Behavior**:

  - Shards listed in the Synchronous Recovery Set will not be used for reads
    by the Zone Primary.
  - If the remaining available shards are insufficient to reconstruct the data,
    the Zone Primary will return ``-EAGAIN``.
  - Recovery messages will signal the Zone Primary when recovery completes,
    clearing the set.

- **Subsequent Enhancement**:

  - A mechanism will be added to allow the Zone Primary to request permission
    to read from a shard within the Synchronous Recovery Set.
  - This request will trigger the necessary recovery for that specific object
    (if not already complete) before the read is permitted.


8. Write Path & Transaction Handling
------------------------------------

This section details how write operations are managed, specifically focusing on
Read-Modify-Write (RMW) sequences and the replication of write transactions to
remote zones.

8.1 Direct-to-OSD Writes (Primary Encodes All Shards) — *R1*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In R1, all writes are coordinated exclusively by the Primary.

- **Mechanism**: The Primary generates all ``zones × (k + m)`` coded shard writes
  and sends individual write operations directly to every OSD in the acting
  set, including those in remote zones.
- **RMW Reads**: The "read" portion of any Read-Modify-Write cycle is performed
  locally by the Primary, strictly adhering to the logic defined in Section 7
  (Read Path & Recovery Strategies).
- **Inter-zone traffic**: This sends all coded shard data (including parity)
  over the inter-zone link. While this is not bandwidth-optimal, it requires
  no Zone Primary involvement in write processing and therefore no new
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
       participant ZS as Zone-local Shard
       participant ZP as Zone Primary
       participant RS as Remote-zone Shard

       C->>P: Submit Op
       activate P

       P->>RP: Replicate
       activate ZP

       note over P: Replicate message must contain<br/>data and which shards are to be updated.<br/>When OSDs are recovering, the primary<br/>may need to update remote-zone shards directly.

       P->>ZS: SubRead (cache)
       activate ZS
       ZS->>P: SubReadReply
       deactivate ZS

       P->>ZS: SubWrite
       activate ZS
       ZS->>P: SubWriteReply
       deactivate ZS

       ZP->>RS: SubRead (cache)
       activate RS
       RS->>RP: SubReadReply
       deactivate RS

       ZP->>RS: SubWrite
       activate RS
       RS->>RP: SubWriteReply
       deactivate RS

       ZP->>P: ReplicateDone
       deactivate ZP

       P->>C: Complete
       deactivate P

This mechanism is designed to minimize inter-zone link bandwidth usage, which
is often the most constrained resource in stretch clusters.

- **Mechanism**: The replicate message is an extension of the existing
  sub-write message. Instead of sending individually coded shard data to every
  OSD in the remote zone, the Primary sends a copy of the *PGBackend
  transaction* (i.e., the raw write data and metadata, prior to EC encoding) to
  the Zone Primary.
- **Role of Zone Primary**: Upon receipt, the Zone Primary processes the
  transaction through its local ECTransaction pipeline — largely unmodified
  code — to generate the coded shards for its zone. For writes smaller than a
  full stripe, this includes the Zone Primary issuing reads to its zone-local
  shards so that the parity update can be calculated. It then fans out the shard
  writes to the zone-local OSDs within its ``zone``. This means coding
  (parity) data is never sent over the inter-zone link; it is computed
  independently at each zone.
- **Completion**: The Zone Primary responds using the existing sub-write-reply
  message once all zone-local shard writes are durable.
- **Crash Handling**: If the Zone Primary crashes mid-fan-out, any partial
  writes on remote-zone shards are rolled back by the existing peering process. No
  new crash-recovery mechanisms are required.
- **Benefit**: This reduces inter-zone link traffic to a single transaction
  message per remote zone, carrying only the raw data — not the ``k+m`` coded
  chunks.

8.3 Client Writes Direct to Zone Primary — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   This section is deferred to a later release. It requires Replicate
   Transaction (Section 8.2) as a prerequisite, since the Zone Primary must
   be able to process transactions and fan out shard writes locally.

.. mermaid::

   sequenceDiagram
       title Write to Zone Primary

       participant C as Client
       participant P as Primary
       participant ZS as Zone-local Shard
       participant RC as Remote Client
       participant ZP as Zone Primary
       participant RS as Remote-zone Shard

       RC->>RP: Write op
       activate ZP

       ZP->>P: Replicate
       activate P
       P->>RP: Permission to write

       note over ZP: There is potential for pre-emptive caching here,<br/>but it adds complexity and mostly does not actually<br/>help performance, as the limiting factor is actually<br/>the Remote-zone write.

       ZP->>RS: SubRead (cache)
       activate RS
       RS->>RP: SubReadReply
       deactivate RS

       ZP->>RS: SubWrite
       activate RS
       RS->>RP: SubWriteReply
       deactivate RS

       P->>ZS: SubRead (cache)
       activate ZS
       ZS->>P: SubReadReply
       deactivate ZS

       P->>ZS: SubWrite
       activate ZS
       ZS->>P: SubWriteReply
       deactivate ZS

       P->>RP: write done
       ZP->>RC: Complete

       note over ZP: The write complete message is permitted<br/>as soon as all SubWriteReply messages have<br/>been received by the remote. It is shown<br/>here to demonstrate that it is required to<br/>complete processing on the Primary.

       ZP->>P: Remote-zone write complete
       deactivate P
       deactivate ZP

       P->>P: PG Idle.

       P->>RP: DummyOp
       activate P
       activate ZP

       P->>ZS: DummyOp
       activate ZS
       ZS->>P: Done
       deactivate ZS

       ZP->>P: Done
       deactivate P

       note over ZP: Message ordering means that remote<br/>primary does not need to wait for<br/>dummy ops to complete

       ZP->>RS: DummyOp
       activate RS
       RS->>RP: Done
       deactivate RS
       deactivate ZP

This mechanism allows a client to write to its local Zone Primary rather than
sending data over the inter-zone link to the Primary. The key benefit is that
the write data crosses the inter-zone link only once (Zone Primary → Primary)
rather than twice (client → Primary → Zone Primary). Unlike Section 8.2 where
the Primary initiates the transaction and replicates it outward, here the
Zone Primary receives the client write directly and coordinates with the
Primary to maintain global ordering while avoiding redundant data transfer.

- **Mechanism**:

  1. The client sends the write to its local **Zone Primary**. The Zone
     Primary stashes a local copy of the write data but does not yet fan out
     to zone-local shards.
  2. The Zone Primary replicates the *PGBackend transaction* (raw write data,
     prior to EC encoding) to the **Primary**.
  3. The Primary processes the transaction as normal through its local
     ECTransaction pipeline, performing cache reads, encoding, and fanning out
     shard writes to its zone-local OSDs. When the Primary reaches the step where it
     would normally replicate data to the originating Zone Primary (as in
     Section 8.2), it instead sends a **write-permission message** to that
     Zone Primary, since the Zone Primary already holds the data. Writes to
     any *other* Zone Primaries (in an ``zones > 2`` configuration) proceed via
     the normal Replicate Transaction path (Section 8.2).
  4. Upon receiving write-permission, the Zone Primary processes the
     transaction through its local ECTransaction pipeline, generating and
     fanning out the coded shard writes to the zone-local OSDs within its
     ``zone``.
  5. Once the Primary's own local writes are durable and all other zones have
     confirmed durability, the Primary sends a **completion message** to the
     originating Zone Primary. The Zone Primary then responds to the client
     once the completion message is received and its own local writes are also
     durable.

- **Write Ordering**: Strict write ordering must be maintained across all zones.
  Since the Primary remains the single authority for PG log sequencing:

  - The Primary sequences the write as part of its normal processing in step 3.
    The write-permission message carries the assigned sequence number, ensuring
    that writes arriving at the Primary directly and writes arriving via a
    Zone Primary are globally ordered.
  - Writes from multiple Zone Primaries (in an ``zones > 2`` configuration) are
    serialized through the Primary's normal sequencing mechanism — no special
    handling is required beyond the existing PG log ordering.
  - The Zone Primary does not fan out to zone-local shards until it receives the
    write-permission message, guaranteeing that zone-local shard writes occur in
    the correct global order.

- **Benefit**: For workloads where the client is co-located with a remote zone,
  write data traverses the inter-zone link exactly once (Zone Primary →
  Primary transaction replication). This halves the inter-zone bandwidth
  compared to R1 (client → Primary → Zone Primary), and is equivalent to
  Section 8.2 but with the added advantage that the client experiences local
  write latency for the initial acknowledgement. Crucially, the data is never
  sent back to the originating Zone Primary — the Primary only sends
  lightweight permission and completion messages.

- **Failure Handling**: If the Zone Primary is unavailable, the client falls
  back to writing directly to the Primary (Section 8.1 or 8.2, depending on
  what is available). If the Primary is unreachable from the Zone Primary
  mid-transaction, the Zone Primary returns an error to the client and any
  stashed data is discarded (no zone-local shard writes have been issued, since
  write-permission was never received).

- **R3 Enhancement: Forward to Other Zone Primaries (``zones > 2``)**:

  .. note::

     This enhancement is a potential R3 feature and may not be implemented.

  In topologies with more than two zones, the originating Zone Primary could
  forward the transaction to the other Zone Primaries in parallel with
  sending it to the Primary. This would improve write latency for those zones
  by allowing them to receive the data directly from a peer Zone Primary
  rather than waiting for the Primary to replicate outward. The Primary would
  still issue write-permission messages to all Zone Primaries to maintain
  global ordering, but the data transfer to additional zones would already be
  complete, reducing the critical path.

  *Complexity / Review Note*: There is a known race condition here. The
  write-permission message originating from the Primary might overtake the
  data transfer sent by the originating Zone Primary. To handle this, a
  unique transaction ID will be required to definitively tie these two
  independent messages together at the receiving Zone Primary.

8.4 Hybrid Approach — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   Requires Replicate Transaction (Section 8.2). Deferred to a later release.

It is acknowledged that complex failure scenarios may require a combination of
Replicate Transaction, Client-via-Remote-Primary, and Direct-to-OSD approaches.

- **Example**: In a partial failure where one remote zone is healthy and another
  is degraded, the Primary may use Replicate Transaction for the healthy zone
  while simultaneously using Direct-to-OSD for the degraded zone within the
  same transaction lifecycle.


9. Recovery Logic
------------------

All recovery operations are centralized and coordinated by the Primary OSD.

9.1 Primary-Centric Recovery — *R1*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In R1, the Primary performs all recovery without delegating to Zone
Primaries and without attempting to minimize inter-zone bandwidth.

- **Zone-local Recovery**: The Primary recovers missing shards in its own
  ``zone`` using available zone-local chunks, exactly as standard EC
  recovery.
- **Remote-zone Recovery**: For every remote ``zone``, the Primary reads
  all shards required to reconstruct any missing data, encodes the missing
  shards locally, and pushes them directly to the target remote-zone OSDs.
- **No Zone Primary Coordination**: The Zone Primary plays no role in
  recovery in R1. All cross-zone data flows from or to the Primary.
- **Trade-off**: This approach may send more data over the inter-zone link than
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
is to **minimize inter-zone link bandwidth** — every cross-zone transfer is
expensive, so the Primary must dynamically choose the recovery strategy for
each object that results in the fewest bytes crossing zone boundaries.

- **Assess Capabilities**: For each ``zone``, the Primary
  determines how many consistent chunks are available locally and how many are
  missing.
- **Cost-Based Plan Selection**: The Primary evaluates the inter-zone bandwidth
  cost of each viable recovery strategy and selects the cheapest option. The
  key trade-off is between:

  1. *Zone Primary performs zone-local recovery with remote-zone reads* — the Zone
     Primary reads a small number of missing shards from the Primary's zone
     and reconstructs locally.
  2. *Primary reconstructs and pushes* — the Primary reconstructs the full
     object and pushes the missing shards to the remote zone.

  The optimal choice depends on how many shards each zone is missing.

- **Example**: Consider a ``6+2`` (k=6, m=2) configuration where Zone A has 8
  good shards and Zone B has only 5 good shards (3 missing). Two options:

  - *Option A (Zone Primary reads remotely)*: The Zone Primary at Zone B
    needs only **1 remote-zone read** (it has 5 of the 6 required chunks locally
    and fetches 1 from Zone A) to reconstruct the 3 missing shards locally.
    **Cost: 1 shard across the inter-zone link.**
  - *Option B (Primary pushes)*: The Primary reconstructs the 3 missing shards
    at Zone A and pushes them to Zone B. **Cost: 3 shards across the
    inter-zone link.**

  Option A is clearly cheaper. A general algorithm should compare the
  cross-zone transfer cost for each strategy and choose the minimum.
  Where inter-zone bandwidth consumption is equal, the algorithm should tie-break
  by optimizing for zone-local read bandwidth or the total number of operations.

  (In the future, the system may adapt these priorities — e.g. explicitly
  optimizing for read count rather than inter-zone bandwidth on high-bandwidth
  links to slower rotational media — whether through auto-tuning or operator
  configuration. However, exploring alternative optimization modes is beyond the
  scope of this design.)

  .. note::

     The detailed algorithm for optimal recovery planning requires further
     design work. The general principle is: count the number of cross-zone
     shard transfers each strategy would require and choose the minimum.

- **Plan Construction**: For each domain, the Primary constructs a "Recovery
  Plan" message containing specific instructions detailing which OSDs must be
  used to read the available chunks and which OSDs are the targets to write the
  recovered chunks. Where cross-zone reads are part of the plan, the message
  specifies which remote-zone shards to fetch.

9.3 Delegated Remote-zone Recovery — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   Requires Cost-Based Recovery Planning (Section 9.2). Deferred to a later
   release.

- **Remote-zone Recovery (Plan-Based)**: The Primary sends a per-object "Recovery Plan"
  to the Zone Primary (or appropriate peers), instructing them to execute the
  recovery for that object (and clones) locally within their domain using the
  provided read/write set. If the plan includes cross-zone reads, the Zone Primary
  fetches the specified shards before reconstructing. Because this ties directly
  into the existing object recovery lifecycle, any failure of a remote-zone peer
  during the plan simply drops the recovery op. The Primary detects the failure
  via standard peering or timeout mechanisms and will naturally re-assess and
  generate a new plan for the object.

9.4 Fallback Strategies — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   These strategies are part of the Cost-Based Recovery system (Section 9.2)
   and are deferred to a later release.

9.4.1 Remote-zone Fallback (Push Recovery)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If a remote zone cannot perform zone-local recovery, and the cost-based analysis
determines that Primary-side reconstruction and push is the cheapest option:

1. The Primary reconstructs the full object locally (using whatever valid shards
   are available across the cluster).
2. The Primary pushes the full object data to the Zone Primary.
3. This push is accompanied by a set of instructions specifying which shards in
   the remote zone must be written.

9.4.2 Primary Domain Fallback
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If the Primary's own domain lacks sufficient chunks to recover locally:

1. The Primary is permitted to read required shards from remote zones to perform
   the reconstruction.
2. While this crosses zone boundaries, the cost-based planning ensures it is
   only chosen when it results in fewer cross-zone transfers than any
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

- The first data shard in each replica (per ``zone``).
- All coding (parity) shards in each replica.

All remaining data shards will be non-primary shards with reduced logs.

.. note::

   Implementation detail: it needs to be determined whether ``pg_temp`` should
   be used to arrange all primary-capable shards (local, then remote) ahead of
   non-primary-capable shards in the acting set ordering.

10.2 Independent Log Generation at Zone Primary — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   Log generation at the Zone Primary is only needed when Replicate
   Transaction writes (Section 8.2) are implemented. For R1, where the
   Primary sends pre-encoded shards directly, PG log entries are generated
   solely by the Primary and distributed with the sub-write messages as per
   existing behavior.

For latency optimization, the replicate transaction message (see Section 8.2)
will be sent to the Zone Primary *before* the cache read has completed on the
Primary. This means the Zone Primary cannot receive a pre-built log entry from
the Primary — it must generate its own PG log entry independently from the
transaction data.

Both the Primary and Zone Primary will produce equivalent log entries from the
same transaction, but they are generated independently at each zone.

10.3 Log Entry Compatibility & Upgrade Safety — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``osdmap.requires_osd_release`` field dictates the minimum code level that
OSDs can run, and therefore determines the log entry format that must be used.
No new versioning mechanism is required — the existing release gating already
constrains log entry compatibility across mixed-version clusters.

Because the Zone Primary generates its own log entry from the transaction
data, it must populate the Object-Based Context (OBC) to obtain the old
version of attributes (including ``object_info_t`` for old size). This is an
additional reason the Zone Primary must be a **primary-capable shard** — only
primary-capable shards maintain the full PG log and OBC state needed for
correct log entry construction.

**Debug Mode for Log Entry Equivalence**

A debug mode will be implemented that compares the log entries generated by the
Primary and Zone Primary for each transaction. When enabled, the Primary will
include its generated log entry in the replicate transaction message, and the
Zone Primary will assert that its independently generated entry is identical.
This mode will be **enabled by default in teuthology testing** to catch any
divergence early. It will be available as a runtime configuration option for
production debugging but disabled by default in production due to the
additional message overhead.

.. warning::

   If a future release changes how PG log entries are derived from
   transactions, the debug equivalence mode provides an automated safety net.
   Any divergence between Primary and Zone Primary log entries will be caught
   immediately in CI.


11. Peering & Stretch Mode — *R1*
-----------------------------------

This section covers the peering, ``min_size``, and stretch-mode integration
required for replicated EC pools. The design follows the existing replica
stretch cluster pattern as closely as possible, with EC-specific adaptations.

.. important::

   **R1 scope is simple two-zone (zones=2).** Three-zone (``zones=3``) details are
   described for design completeness but will be implemented in a later
   release.

11.1 Pool Lifecycle
~~~~~~~~~~~~~~~~~~~~~

.. note::
   **CLI Under Review**: We are reviewing the CLIs for enabling stretch mode and 
   setting stretch mode on a pool. We aim to either get rid of it completely 
   (i.e., infer stretch mode automatically when you create a pool with ``zones > 1``) 
   or just have an enable/disable stretch mode CLI with no arguments. We also 
   will ensure the new CLI works with both stretch-EC and stretch-replica pools for R1.

A replicated EC pool implicitly operates in stretch mode. Creation and configuration will be
simplified based on the final CLI iteration as noted above.

- **CRUSH rule**: Set to the stretch CRUSH rule when stretch mode is active.
- **min_size**: Managed by the monitor. Automatically adjusted during stretch
  mode state transitions (Section 11.3).
- **Peering**: Uses stretch-aware acting set calculation with parameters
  adjusted by the monitor during state transitions.
- **Failure**: Automatic ``min_size`` reduction, degraded/recovery/healthy
  stretch mode transitions — all managed by OSDMonitor.

**Prerequisites for ``zones > 1`` Pool Creation**

.. list-table::
   :header-rows: 1

   * - Requirement
     - Reason
   * - All OSDs have the ``zones > 1`` feature bit
     - Ensures all OSDs understand extended acting-set semantics (Section 15)
   * - Stretch mode must be enabled on the cluster
     - ``zones > 1`` pools require the stretch mode state machine for
       ``min_size`` management and zone failover
   * - ``zone_failure_domain`` must match the stretch mode failure domain
     - The CRUSH rule must align with the stretch cluster topology

11.2 Minimum PG Size Semantics
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The user defines a ``min_size`` for a single zone, which implicitly specifies
the number of failures the pool will tolerate. For an EC pool, this is in the
range ``K`` to ``K+M``, defining a tolerance of ``0`` to ``M`` failures. For a
replica pool, this is in the range ``1`` to ``size``, defining a tolerance of
``0`` to ``size - min_size`` failures. 

Let the number of tolerated failures derived from this setup be denoted as **F**.

If the number of zones > 1, then this setting is dynamically *interpreted*
according to the cluster's stretch mode (Healthy, Degraded, Recovery). Both
EC and Replica pools with multiple zones will interpret ``min_size`` this way,
rather than actively modifying the ``min_size`` setting whenever a stretch
mode transition occurs.

11.2.1 Per-Pool Min-Size Interpretations — *R1*
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The pool statically tolerates up to ``F`` OSD failures, but that tolerance
applies to different scopes based on the active stretch mode:

- **Healthy Mode**: There may be up to ``F`` OSD failures across the OSDs in
  *all zones combined*.
- **Degraded Mode**: There may be up to ``F`` OSD failures across the OSDs in
  the *surviving zone(s)*. The failed zone is entirely ignored.
- **Recovery Mode**: There may be up to ``F`` OSD failures across the OSDs in
  the *surviving zone(s)*. The recovering zone is entirely ignored.

.. note::
   A partial failure within a zone may drop a pool below its ``min_size``
   requirement and cause I/O to stop. Manually removing the rest of the failed
   zone will cause a transition to **Degraded Stretch Mode**, which might be
   sufficient to bring the pool back online because the ``min_size`` requirement
   is now met by the surviving zone. There will be no automation to promote a
   partial zone failure to a whole zone failure.

11.2.2 Per-Zone Min-Size Interpretations — *Later Release*
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A more sophisticated implementation will reinterpret this tolerance strictly on
a per-zone basis:

- There may be up to ``F`` OSD failures *in each individual zone*.
- If any single zone experiences more than ``F`` OSD failures, it will
  automatically trigger a transition into **Degraded Stretch Mode**, effectively
  treating all OSDs in the zone as failed.

11.3 Stretch Mode State Machine
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When stretch mode is enabled, the state machine behaves identically to the
replica stretch mode state machine — leveraging the existing OSDMonitor
infrastructure. As noted above, the explicit ``min_size`` setting is simply
*interpreted* against this state machine, rather than actively mutated by the
OSDMonitor upon transitions.

.. mermaid::

   stateDiagram-v2
       [*] --> Healthy: enable_stretch_mode
       Healthy --> Degraded: zone failure detected
       Degraded --> Recovery: failed zone returns
       Recovery --> Healthy: all PGs clean
       Healthy --> [*]: disable_stretch_mode
       Degraded --> Recovery: force_recovery_stretch_mode CLI
       Recovery --> Healthy: force_healthy_stretch_mode CLI

**Two-Zone Transitions (zones=2) — R1**

.. list-table::
   :header-rows: 1

   * - Stretch State
     - min_size
     - Reasoning
   * - **Healthy**
     - ``zones × (K+M) − M``
     - Full redundancy; tolerate up to M failures
   * - **Degraded** (one zone down)
     - ``K``
     - One zone lost (K+M shards gone). Surviving zone has K+M shards;
       can tolerate M more losses. ``K+M − M = K``.
   * - **Recovery** (zone returning)
     - ``K`` (same as degraded)
     - Keep reduced min_size until resync is complete
   * - **Healthy** (resync complete)
     - ``zones × (K+M) − M``
     - Full min_size restored

**Concrete example — K=2, M=1, zones=2 (size=6):**

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

- Healthy min_size: ``zones × (K+M) − M``
- Degraded min_size: ``(num_zones−1) × (K+M) − M`` = healthy min_size minus
  ``(K+M)``
- **Rule: if a zone fails, reduce min_size by K+M. If a zone becomes
  healthy again, increase min_size by K+M.**

**Three-Zone Transitions (zones=3) — Later Release**

.. list-table::
   :header-rows: 1

   * - Stretch State
     - min_size
     - Example (K=2, M=1)
   * - Healthy (3 zones)
     - ``3(K+M) − M``
     - 8
   * - One zone down
     - ``2(K+M) − M``
     - 5
   * - Two zones down
     - ``K``
     - 2

11.4 OSDMonitor Changes
~~~~~~~~~~~~~~~~~~~~~~~~~~

The existing OSDMonitor stretch mode code contains
``if (is_replicated()) { ... } else { /* not supported */ }`` patterns in
several places. These gaps must be filled for EC pools with ``zones > 1``.

**11.4.1 Pool Stretch Set / Unset** (``prepare_command_pool_stretch_set``,
``prepare_command_pool_stretch_unset``)

``stretch_set`` currently works for any pool type, setting
``peering_crush_bucket_*``, ``crush_rule``, ``size``, ``min_size``.

For EC pools with ``zones > 1``, add validation:

- Validate ``min_size ∈ [num_zones×(K+M)−M, num_zones×(K+M)]``
- If ``size`` is provided, validate it matches ``zones × (K+M)``

``stretch_unset`` clears all ``peering_crush_*`` fields. No EC-specific
changes required.

**11.4.2 Enable/Disable Stretch Mode** (``try_enable_stretch_mode_pools``)

*Currently rejects EC pools.* Change to accept EC pools with ``zones > 1``:

- Set ``peering_crush_bucket_count``, ``peering_crush_bucket_target``,
  ``peering_crush_bucket_barrier`` (same values as replica)
- Set ``crush_rule`` to the stretch CRUSH rule
- Set ``size = r × (k + m)`` (should already be correct from pool creation)
- Set ``min_size = r × (k + m) − m``

**Pool Creation Gate**: Pool creation with ``zones > 1`` must be rejected if
stretch mode is not already enabled on the cluster. This is validated in
``OSDMonitor::prepare_new_pool``.

**11.4.3 Degraded Stretch Mode** (``trigger_degraded_stretch_mode``)

*Currently sets* ``newp.min_size = pgi.second.min_size / 2`` *for replica
pools.* For EC pools, compute::

    newp.min_size = p.min_size - (k + m)

For a K=2, M=1, zones=2 pool: ``min_size = 5 − 3 = 2``.

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

Algorithm for each position ``i`` (0 to ``num_zones×(K+M)−1``):

1. Prefer ``up[i]`` if usable
2. Otherwise prefer ``acting[i]`` if usable
3. Otherwise search strays for an OSD with shard ``i``
4. If no usable OSD found, leave as ``CRUSH_ITEM_NONE``

CRUSH ``bucket_max`` constraints apply: no zone may contribute more than
``size / peering_crush_bucket_target`` OSDs.

.. note::

   This is essentially ``calc_ec_acting`` with the additional CRUSH bucket
   awareness from the stretch path.

**11.5.2 Async Recovery**

``choose_async_recovery_replicated`` checks ``stretch_set_can_peer()`` before
removing an OSD from async recovery. An EC stretch variant must respect shard
identity: removing an OSD must not cause any zone to drop below K shards.

**11.5.3 Stretch Set Validation** (``stretch_set_can_peer``)

For EC pools, additionally verify:

- The acting set includes at least K shards in at least one surviving zone
- In healthy mode, all zones have ``K+M`` shards

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
     - zones (zones) in healthy; reduced during degraded
   * - ``peering_crush_bucket_target``
     - Target CRUSH buckets for ``bucket_max`` calc
     - zones (zones) in healthy; reduced during degraded
   * - ``peering_crush_bucket_barrier``
     - CRUSH type level (e.g., datacenter)
     - Same as replica — the failure domain
   * - ``peering_crush_mandatory_member``
     - CRUSH bucket that must be represented
     - Set to surviving zone during degraded mode

**Example values for zones=2, K=2, M=1 (size=6):**

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
K+M``. This naturally prevents more than one copy of each shard per zone.

11.7 Network Partition Handling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The existing Ceph stretch cluster mechanism handles network partitions as
follows:

- **MON-Based Zone Election**: The MONs are distributed between zones (plus a
  tie-break zone). When the inter-zone link is severed, the MONs elect which
  zone continues to operate.
- **OSD Heartbeat Discovery**: OSDs heartbeat the MONs and will discover if
  they are attached to the losing zone. OSDs on the losing zone stop
  processing IO.
- **Lease-Based Read Gating**: A lease determines whether OSDs are permitted to
  process read IOs. The surviving zone must wait for the lease to expire before
  new writes can be processed, ensuring no stale reads are served by the losing
  zone during the transition.

This mechanism does not preclude more complex failure scenarios that may also
need to be treated as zone failures:

- **Asymmetric Network Splits**: OSDs at a remote zone may be able to
  communicate with a zone-local MON even though the MONs have determined a zone
  failover has occurred.
- **Partial Zone Failures**: Loss of a rack or subset of OSDs within a zone may
  warrant treating the zone as failed (e.g., if the remaining shards fall below
  the per-zone ``min_size`` threshold defined in Section 11.2).

11.8 Online OSDs in Offline Zones
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When a zone is marked as offline but individual OSDs within that zone remain
reachable, they must be removed from the up set that is presented to the
Objecter and peering state machine. The mechanism ties to the ``min_size``
logic described in Section 11.2:

- **Removal**: If fewer than ``k`` OSDs from a zone are present in the up set,
  the remaining OSDs for that zone are also removed. This forces a zone
  failover and prevents partial-zone IO.
- **Reintegration**: When enough OSDs return to meet the per-zone threshold,
  they are added back into the up set. Standard peering will handle resyncing
  data — no new recovery code is required for reintegration.


12. Scrub Behavior — *R1*
-----------------------------

The only modification required to scrub is how it verifies the CRCs received
from each shard during deep scrub. The deep scrub process must:

1. **Verify the coding CRC**, where the EC plugin supports it. This is current
   behavior but must be extended to cover the replicated shards (i.e.,
   verifying the coding relationship within each zone's stripe independently).
2. **Verify cross-replica CRC equivalence**: corresponding shards in each
   replica must have the same CRC. For example, SiteShard 0 at Zone A must
   match SiteShard 0 at Zone B.

Scrub never reads data to the Primary — it only collects and compares CRCs
reported by each OSD. This means inter-zone bandwidth is not a significant
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

In a later release, users will be able to modify ``zones`` for an existing pool by
swapping in a new EC profile that is otherwise identical (same ``k``, ``m``,
plugin, etc.) but specifies a different ``zones`` value. Upon profile change, the
CRUSH rule will be updated to reflect the new replica count, and the standard
recovery process will automatically perform all necessary expansion (or
contraction) to match the new configuration — no manual data migration is
required.


14. Implementation Order
-------------------------

This section defines the phased implementation plan, broken into single-sprint
stories.

14.1 R1 — Read-Optimized Stretch Cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

R1 delivers a functional EC stretch cluster with zone-local read optimization
(comparable to what Replica stretch clusters provide today). All writes and
recovery traverse the inter-zone link via the Primary.

1. **CRUSH Rule Generation for Replicated EC**
   Implement automatic CRUSH rule creation from the ``zones`` and
   ``zone_failure_domain`` pool parameters (Section 4). Validate that the acting
   set places ``k+m`` shards per ``zone``.

2. **EC Profile Extension: ``zones`` and ``zone_failure_domain`` Parameters**
   Extend the EC profile and pool configuration to accept and validate ``zones``
   and ``zone_failure_domain``. Wire these into pool creation and ``ceph osd pool``
   commands (Section 2).

3. **Primary-Capable Shard Selection for Replicated EC**
   Extend the non-primary shard selection algorithm to designate the first data
   shard and all parity shards per replica as primary-capable (Section 10.1).

4. **Stretch Mode and Peering for Replicated EC**
   Broken into the following sub-stories (see Sections 11.1–11.6):

   a. **Pool Stretch Set/Unset for EC** (OSDMonitor): Allow ``osd pool
      stretch set/unset`` on EC pools with ``zones > 1``. Add EC-specific
      ``min_size`` range validation (Section 11.4.1).
   b. **Enable/Disable Stretch Mode for EC** (OSDMonitor): Allow
      ``mon enable_stretch_mode`` when the cluster has EC pools with
      ``zones > 1``. Set ``min_size = r × (k+m) − m`` (Section 11.4.2).
   c. **Stretch Mode Transitions for EC** (OSDMonitor): Implement
      degraded/recovery/healthy transitions. On zone failure, reduce
      ``min_size`` by ``k+m``; on recovery, restore it (Sections 11.4.3–5).
   d. **EC Peering with Stretch Constraints** (PeeringState): Create
      ``calc_ec_acting_stretch`` to respect both shard identity and CRUSH
      ``bucket_max`` constraints. Extend async recovery checks
      (Section 11.5).
   e. **is_recoverable / is_readable for Stretch EC**: Verify and extend
      ``ECRecPred`` and ``ECReadPred`` to account for the larger shard set
      and per-zone constraints (Section 11.5.3).

5. **Primary Write Fan-Out to All Shards (Direct-to-OSD Writes)**
   Extend the Primary write path to encode and distribute ``zones × (k + m)``
   shard writes directly to all OSDs in the acting set (Section 8.1).

6. **Direct-to-OSD Reads with Primary Fallback**
   Extend EC Direct Reads to the stretch topology: clients read locally and
   redirect all failures to the Primary (Sections 7.2, 7.3).

7a. **Single-OSD Recovery (Within-Zone)**
    Extend recovery so the Primary can recover a single missing OSD within any
    replica. The Primary reads shards from the affected replica, reconstructs
    the missing shard, and pushes it to the replacement OSD (Section 9.1).

7b. **Full-Zone Recovery**
    Extend recovery to handle full-zone loss: the Primary reads from its local
    (surviving) replica, encodes the full stripe, and pushes all ``k+m`` shards
    to the recovering zone's OSDs. Also covers multi-OSD failures within a
    single zone (Section 9.1).

8. **Scrub CRC Verification for Replicated Shards**
   Extend deep scrub to verify cross-replica CRC equivalence for corresponding
   shards (Section 12).

9. **Network Partition and Zone Failover Integration**
   Validate the existing MON-based zone election and OSD heartbeat mechanisms
   work correctly with the replicated EC acting set (Section 11.7).

10. **End-to-End 2-Zone Integration Testing**
    Full integration test suite covering read, write, recovery, scrub, and
    zone-failover scenarios for a 2-zone (``zones=2``) configuration.

11. **Inter-Zone Statistics**
    Add perf counters tracking cross-zone bytes, operation counts, latency
    histograms, and zone-local vs remote-zone-zone read ratios to give operators visibility
    into inter-zone link utilization (Section 17).

14.2 Later Release — Recovery Bandwidth Optimization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These stories reduce inter-zone link bandwidth consumed during recovery, in
priority order.

1. **Cost-Based Recovery Planning**
   Implement the assessment and plan-selection algorithm that compares
   cross-zone transfer costs for each recovery strategy (Section 9.2).

2. **Recovery Plan Message and Zone Primary Execution**
   Define the "Recovery Plan" message format and implement Zone Primary
   execution of delegated recovery within its ``zone``
   (Section 9.3).

3. **Push Recovery Fallback**
   Implement the path where the Primary reconstructs the full object and
   pushes to the Zone Primary with shard-write instructions (Section 9.4.1).

4. **Primary Domain Fallback (Cross-Zone Read for Zone-local Recovery)**
   Allow the Primary to read shards from remote zones when its own domain
   lacks sufficient chunks, only when cost-based planning selects it
   (Section 9.4.2).

14.3 Later Release — Write Bandwidth Optimization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These stories reduce inter-zone link bandwidth consumed during writes, in
priority order.

1. **Replicate Transaction Message**
   Implement the replicate transaction message that sends raw write data to
   the Zone Primary instead of pre-encoded shards. The Zone Primary
   encodes locally (Section 8.2).

2. **Independent PG Log Generation at Zone Primary**
   Enable the Zone Primary to generate its own PG log entries from the
   replicate transaction, including OBC population and the debug equivalence
   mode (Sections 10.2, 10.3).

3. **Client Writes Direct to Zone Primary**
   Enable clients to write to their local Zone Primary, which stashes the
   data, replicates to the Primary, and fans out locally only after receiving
   write-permission. Includes the write-permission/completion message
   mechanism to maintain global consistency (Section 8.3).

4. **Hybrid Write Path (Replicate + Direct-to-OSD)**
   Support mixed write strategies within a single transaction when zones are
   in different health states (Section 8.4).

14.4 Later Release — Additional Enhancements
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These stories improve resilience and flexibility but are lower priority than
bandwidth optimizations.

1. **Read from Zone Primary**
   Enable clients to send reads to their local Zone Primary for local
   reconstruction, including the Synchronous Recovery Set mechanism
   (Sections 7.4, 7.4.1).

2. **Direct-to-OSD Failure Redirection to Zone Primary**
   Upgrade direct-read failure handling to redirect to the local Zone
   Primary instead of the global Primary (Section 7.5).

3. **Per-Zone min_size with Zone Failover**
   Implement per-zone minimum shard thresholds that automatically trigger
   zone failover (Section 11.2, later release).

4. **Online OSDs in Offline Zones Handling**
   Implement removal of residual online OSDs from the up set when their zone
   is offline (Section 11.8).

5. **In-Place Replica Count Modification**
   Allow ``zones`` to be changed on an existing pool via EC profile swap
   (Section 13.2).

6. **3-Zone (``zones=3``) Full Integration Testing**
   Full integration and real-world testing of 3-zone configurations
   (Section 5).


15. Upgrade & Backward Compatibility
--------------------------------------

Replicated EC pools with ``zones=1`` are fully backward compatible. An ``zones=1``
profile produces a standard ``(k+m)`` acting set with no additional replication,
no new on-disk format, and no new wire messages. Existing OSDs and clients will
handle these pools without modification, because the resulting behavior is
identical to a conventional EC pool.

Pools with ``zones > 1`` introduce a larger acting set (``zones × (k + m)`` shards),
new CRUSH rules, and — in later releases — new inter-OSD messages
(Replicate Transaction, Read Permissions, etc.). To prevent mixed-version
clusters from misinterpreting these pools:

- A new **OSD feature bit** will gate the creation of ``zones > 1`` profiles. The
  monitor will reject pool creation or EC profile changes that set ``zones > 1``
  unless all OSDs in the cluster advertise this feature bit.
- This ensures that every OSD in the cluster understands the extended acting
  set semantics, shard numbering, and any new message types before an ``zones > 1``
  pool can be instantiated.
- No data migration is required when upgrading: the feature bit is purely an
  admission control mechanism. Once all OSDs are upgraded and the bit is
  present, ``zones > 1`` pools can be created normally.

.. note::
   **Upgrade Scenarios**: We are still thinking through upgrade scenarios from older 
   releases (e.g. can we infer ``zones = 2`` pools automatically at upgrade time?).
   
   Open design decisions that will be resolved at implementation:

   What to do with stretched replica pools during upgrade. One option is to leave 
   these as is (with num_zones = 1 and the current interpretation of min-size) 
   and continue to support all the arguments on enable/disable stretch mode. An
   alternative option would be to try and automatically convert these pools to 
   num_zones = 2 and the new interpretation of min-size). These pools should 
   currently have a custom CRUSH rule which should be maintained.

16. Kernel Changes
-------------------

The kernel RBD client (``krbd``) implements its own EC direct read path,
independent of the userspace ``librados`` client. For replicated EC pools, the
``krbd`` direct read logic must be updated to understand the ``zones × (k + m)``
shard layout so that it can identify and target zone-local shards within the
client's ``zone``. Without this change, ``krbd`` may attempt direct
reads to shards in a remote zone, negating the locality benefit.

The required modification is small — the shard selection logic needs to account
for the replica stride when choosing which shard to read from — but it is a
kernel-side change and therefore follows the kernel release cycle independently
of the Ceph userspace releases.


17. Inter-Zone Statistics — *R1*
----------------------------------

Operators need visibility into inter-zone link utilization to validate that
the stretch EC design is delivering its locality benefits and to plan capacity.
The following statistics will be exposed per-pool and per-OSD:

- **Cross-zone bytes sent / received**: Total bytes transferred to/from OSDs in
  remote zones, broken down by operation type:

  - Write fan-out (shard data sent to remote zone)
  - Recovery push (shard data sent during recovery)
  - remote-zone read (shard data read from remote zone for reconstruction or
    medium-error recovery)

- **Cross-zone operation counts**: Number of cross-zone sub-operations, broken
  down by type (sub-write, sub-read, recovery push).

- **Cross-zone latency**: Histogram (p50 / p95 / p99) of round-trip latency for
  cross-zone sub-operations, useful for detecting link degradation.

- **local vs remote zone read ratio**: For direct-read-enabled pools, the fraction of
  reads served locally versus those that fell back to the Primary across a zone
  boundary. A high remote-zone ratio indicates a locality problem.

These counters will be exposed through the existing ``ceph perf`` counter
infrastructure and will be queryable via ``ceph tell osd.N perf dump`` and
the Ceph Manager dashboard. They should also be available at the pool level
via ``ceph osd pool stats``.

.. note::

   The inter-zone classification relies on comparing the CRUSH location of the
   source OSD with the destination OSD's ``zone``. This comparison is
   already performed during shard fan-out; the statistics layer adds only
   counter increments on the existing code path.


18. Non-Redundant Pool Zone Affinity
------------------------------------

A stretch cluster topology may additionally host workloads that do not require
multi-zone redundancy, or where redundancy is handled at a higher application
layer. To accommodate this, a non-redundant pool can be configured with zone
affinity. 

This is achieved by setting up the pool to use a specific CRUSH root. When creating the pool, you set the ``zones`` to ``1`` (the default) and pass a ``crush_root`` parameter targeting a specific datacenter bucket or zone bucket within your CRUSH hierarchy.

Implementation Details
~~~~~~~~~~~~~~~~~~~~~~

When configuring a pool with affinity to a specific zone, the system generates a 
CRUSH rule that performs a standard ``take <crush_root>`` operation on the 
designated zone bucket.

For instance, if a cluster has two datacenters defined in CRUSH as ``DC1`` and 
``DC2``, configuring a pool with ``crush_root=DC1`` and ``zones=1`` will prompt the 
system to generate a CRUSH rule that starts with ``take DC1``, ensuring all 
data for that pool resides completely within that datacenter. This allows non-redundant 
applications to leverage zone-local storage without incurring the latency or bandwidth 
costs of crossing the inter-zone link.
