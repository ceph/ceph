Live CephFS Async Replication with Snapshot-Based Recovery
==========================================================

Purpose
-------

This note is a Ceph-facing abstract for a proposed one-way CephFS disaster
recovery mechanism. "One-way" means single-source, passive-replica replication:
a designated primary CephFS cluster replicates to a passive secondary cluster
that accepts no independent client writes. This is not active-active
replication. The goal is deliberately narrow: provide the same consistency
class as ``cephfs-mirror`` recovery points, while improving RPO and RTO through
a live replay stream and finer recovery-point granularity.

Detailed design material is available at the reference
`repository <https://github.com/Markuze/async_rep>`_.

Abstract
--------

``cephfs-mirror`` is the consistency benchmark. It produces remote recovery
points by mirroring CephFS snapshots. This proposal targets the same trusted
recovery-point semantics: the promoted point is as coherent as the
corresponding source snapshot.

The difference is how the replica reaches that point. Instead of waiting for a
snapshot-diff pipeline to discover and transfer each recovery point, a live
asynchronous stream keeps a passive CephFS replica close to the source. When a
snapshot recovery point is published, most of the underlying namespace and data
changes have already been transported and replayed. This allows finer
checkpoint cadence, lower verified RPO, and shorter failover preparation,
because the replica is already warm.

The live stream is built as a "reverse Ganesha" pipeline. This is a conceptual
analogy, not a dependency on NFS-Ganesha software: no Ganesha deployment is
required. NFS-Ganesha accepts file-service protocol operations and issues CephFS
client operations; the analogy reverses that flow. Reverse Ganesha observes
committed CephFS client traffic, reconstructs the logical filesystem operations,
and replays those operations on a passive CephFS replica. MDS traffic describes namespace and metadata effects such as
``create``, ``rename``, ``unlink`` and ``setattr``. OSD traffic describes file
data effects. CephFS layout metadata joins OSD object writes back to logical
file offsets. Source-side tokens such as object ``user_version``, truncate
sequence, and metadata versions provide ordering evidence for the domains where
order affects the result.

The live stream by itself is not the consistency claim. It is a low-lag,
best-effort tier that keeps the target close to the source and reduces the work
remaining at the next trusted recovery point, which remains snapshot-based.

The result is a two-tier DR model:

* a live best-effort replica with low lag and per-inode convergence; and
* periodic verified recovery points with the same consistency class as
  ``cephfs-mirror`` snapshot recovery points.

Recovery Model
--------------

The design separates operational freshness from verified recovery.

Best-effort live replica
~~~~~~~~~~~~~~~~~~~~~~~~

The gateway replays committed source mutations continuously. When the stream is
caught up, the replica can be close to the source in wall-clock time. This tier
is useful for warm standby, inspection, and reducing the amount of work needed
to publish the next verified checkpoint.

This tier preserves source order where the source provides a usable ordering
domain, especially same-file and same-object conflicts. It does not claim that
every instant of the replica corresponds to a coherent source filesystem
instant across independent files, clients, MDS traffic and OSD traffic.

Verified snapshot recovery
~~~~~~~~~~~~~~~~~~~~~~~~~~

Verified recovery points are published from source snapshots. When a source
snapshot is taken, the gateway marks the corresponding barrier in the captured
stream, drains every pre-barrier record to the replica, snapshots the replica,
and holds post-barrier records aside until that replica snapshot is durable. The
replica recovery point therefore reflects exactly the named source snapshot, and
is treated as trusted only after that correspondence is confirmed.

At failover, the operator promotes the last verified recovery point. Promoting a
newer live state is an explicit best-effort choice, not the consistency
guarantee of the system.

Consistency Claim
-----------------

The consistency claim is parity with ``cephfs-mirror``. A published recovery
point should provide the same class of coherent snapshot recovery point that an
operator expects from CephFS snapshot mirroring.

The proposal does not require a global total order of every CephFS event.
Replay needs source order only for domains where order changes the result:
same object, same file, truncate/write races, conflicting metadata changes, and
path-changing namespace operations. Cross-file recovery consistency is supplied
by the published snapshot recovery point, not by treating passive observation
as a cluster-wide linearization mechanism.

Any stronger consistency target, including application-level consistency or
more exact cross-subsystem causal reconstruction, is outside this proposal and
belongs in future work.

Baseline Implementation Path
----------------------------

The baseline path is an external proof of the live translation and replay
pipeline.

Required existing capabilities:

* CephFS snapshots as the recovery-point unit.
* Observable committed MDS and OSD client traffic, or an equivalent trusted
  source-side observation point.
* A passive CephFS replica written only by the replay backend.
* A verifier or publication rule that marks only matching snapshot recovery
  points as trusted.

New non-Ceph components:

* transparent capture layer;
* translation gateway;
* durable operation spool;
* per-domain ordering and deduplication;
* checkpoint bookkeeping;
* remote libceph replay backend;
* optional snapshot verifier.

This path is suitable for a proof of concept because it isolates the hard
semantic question: can committed CephFS MDS and OSD effects be translated into
a correct logical replay stream? Once that stream exists, snapshot-based
recovery points provide the same consistency target as existing snapshot
mirroring, while the live replay stream improves freshness.

Optimized Ceph-Integrated Path
------------------------------

The optimized path moves some capture, ordering, persistence, and bandwidth
work into Ceph. It is not needed to state the DR goal, but it is the likely
production hardening direction.

Useful Ceph-side additions include:

* an MDS-exported metadata mutation stream with explicit commit order;
* an OSD-exported committed-write stream, or a persistent source-side event
  journal, so replay is not dependent on packet inspection;
* a checkpoint publication token that lets the replay service prove which
  records belong to a named source snapshot;
* first-class support for encrypted messenger deployments by exporting events
  after authentication and authorization rather than inspecting plaintext
  packets;
* source-side compaction, write coalescing, deduplication, and bandwidth
  accounting.

These additions optimize scalability, observability, encrypted deployments and
operational robustness.

Comparison with cephfs-mirror
-----------------------------

``cephfs-mirror`` is snapshot driven. It copies snapshot deltas and produces a
remote snapshot recovery point. That is a strong and simple consistency model,
but verified RPO is bounded by snapshot cadence plus mirror completion time,
and failover readiness depends on how current the last mirrored snapshot is.

This proposal keeps the snapshot recovery point as the trusted consistency
unit, but uses live replay to keep the passive replica close to the source
between recovery points. The intended advantages are:

* **Lower RPO:** recovery points can be cut more frequently because the live
  stream has already moved most changes before publication.
* **Shorter RTO:** the passive filesystem is already warm and near-current, so
  promotion has less catch-up work than a target that advances only when
  snapshot deltas are mirrored.
* **Better granularity:** operators can choose finer recovery scopes and
  cadence without changing the consistency claim.

Known Limits
------------

This is one-way asynchronous DR, not synchronous replication and not
active-active CephFS.

The WAN must be provisioned for sustained source mutation bandwidth. If the
network or replay backend falls behind, best-effort lag grows and verified RPO
depends on how long it takes to publish the next trusted recovery point.

The baseline depends on complete observation of in-scope committed traffic.
Missing a client, MDS session, OSD write, or reply invalidates replay
correctness. Encrypted messenger deployments need a trusted observation point
or an official event-export path.

The production system must prove that each published recovery point
corresponds to the source snapshot it names. Until that proof is implemented
and verified, the live stream should be treated as an operational accelerator,
not the trusted consistency mechanism.

FAQ from Review Discussion
--------------------------

Does this require a linear order of every event in the Ceph cluster?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No. A total cluster-wide order is neither feasible nor needed for the stated
goal. The live replay stream needs source order only for non-commuting
operations: same object, same file, truncate/write races, conflicting metadata
changes and path-changing namespace operations. Independent operations can
replay in parallel. Trusted recovery consistency comes from the published
snapshot recovery point.

Can passive capture exactly mimic the source at an arbitrary point in time?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No. The proposal does not claim arbitrary-time exactness. It claims trusted
recovery points with the same consistency class as snapshot mirroring. The live
replica may be newer than the last verified point, but that newer state is a
best-effort operational tier.

What about fsync or writes that cross object boundaries?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For final same-file convergence, object versions, truncate sequencing and file
layout provide the ordering evidence needed to reconstruct committed extents.
But an ``fsync`` does not give a passive external observer a universal ordering
token that also orders unrelated MDS and OSD effects across the filesystem.
For a trusted DR point, the system publishes a snapshot recovery point and
claims parity with ``cephfs-mirror``.

Why use a proxy/gateway instead of having MDS and OSD daemons send update logs?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Daemon-exported logs are a valid optimized path. The external gateway is the
conservative proof path because it tests whether semantic translation is
possible without first changing Ceph daemons. If the proof is useful, an
MDS/OSD event export or source-side persistent journal would be cleaner for
production.

Doesn't replaying MDS operations collide with OSD writes the replica already received?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No, because the replica never replays daemon-to-daemon traffic. Reverse Ganesha
reconstructs logical (VFS) operations from the observed client traffic and
issues them through the replica's own libceph client; the replica MDS then
drives its own OSD writes, exactly as a fresh client operation would. Captured
MDS and OSD effects are fused into one logical operation and replayed once at
the client level, not as two independent streams. The double-apply concern
applies to the proxy-per-daemon model, where a captured MDS operation is
replayed against OSDs that already saw the data; replaying client-level
operations avoids it by construction.

Does the gateway need persistent storage?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. A serious async replication system needs durable buffering for network
delay, replay delay, gateway restart and checkpoint publication. The proposal
does not treat buffering as free memory. The baseline keeps that persistence in
the external service; the optimized path could move it into Ceph-managed
services or an official event journal.

Why not just use existing Ceph services for that persistence?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For a POC, keeping the service external isolates the hard question: can CephFS
wire-visible effects be translated into a correct replay stream? For a product
path, using Ceph services for event export and durable source-side logging is
reasonable and probably preferable.

What is the target use case?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The target is one-way CephFS disaster recovery: a passive remote filesystem
kept close to the source, periodic trusted recovery points, and faster
promotion preparation than a purely snapshot-diff pipeline. It is not designed
for active-active writes or conflict resolution.

How can this scale if clients talk to many MDS and OSD connections?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The capture layer must see all relevant committed traffic, but replay ordering
can be sharded by ordering domain. Data can shard by source inode or object;
namespace work can shard by subtree or metadata authority; only conflicting
operations need to meet in the same ordering queue. Checkpoint publication is a
coordination point, not necessarily a single replay bottleneck for all data.

Does inter-site bandwidth have to match the source write rate?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For continuous low-lag replication, yes: the remote side must sustain the
source mutation rate after compression, coalescing and deduplication. If it
cannot, lag grows. Better recovery-point granularity improves RPO only when the
live stream and replay backend keep up.

Where do the RPO and RTO gains come from?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

RPO improves because trusted recovery points can be published at finer
granularity: the replica is already receiving the live change stream instead
of waiting for each snapshot interval to discover and transfer a large diff.
RTO improves because the target filesystem is already warm and near-current, so
promotion has less remaining work.

Is this a replacement for cephfs-mirror?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Not initially. ``cephfs-mirror`` remains the established snapshot replication
mechanism. This proposal is a complementary DR architecture: prove live replay,
layer trusted snapshot recovery points on top, then evaluate whether the
combined model can provide comparable consistency with better freshness and
failover preparation for selected workloads.

