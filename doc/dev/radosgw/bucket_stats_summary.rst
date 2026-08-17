.. _rgw_bucket_stats_summary:

===========================================
RGW Bucket Statistics Summary (Design Note)
===========================================

This note proposes a per-bucket statistics summary for the RADOS driver.
Its purpose is to reduce the bucket-index reads used to refresh bucket and
owner quota statistics when many RGW instances serve large, sharded buckets.

Motivation
==========

Bucket statistics are stored in bucket-index shard headers.  Reading an
accurate total requires reading every shard in the current bucket-index
layout.  When each RGW instance independently refreshes its quota cache, a
bucket with ``S`` shards served by ``N`` RGWs can generate approximately
``N * S`` bucket-index reads per cache-refresh period.  This can create a
periodic bucket-list storm at the OSDs.

RGW quota checking is already cached per gateway and therefore eventually
consistent across multiple gateways.  The design below keeps that model.  It
trades exact, repeated scans for bounded, explicitly approximate statistics.

Summary Object
==============

RGW stores one summary object for each bucket incarnation in the bucket index
pool.  Its object name is derived from ``bucket_id``.  The encoded payload
also contains the bucket id and creation time, so a stale object cannot be
accepted for a deleted and recreated bucket.

The payload contains:

* the identity fields above;
* the current bucket-index layout generation, updated during resharding;
* aggregate bucket statistics, including the statistics needed for owner
  quota accounting;
* the completion time of the latest whole-scale update; and
* optional diagnostic metadata, such as the start and completion times of the
  latest full scan.

The summary object is an optimization and is not the authoritative source of
bucket contents.  Bucket-index headers remain the source for a full refresh.

Updates
=======

There are two update operations.

Ordinary delta update
---------------------

Each RGW process keeps an in-memory accumulator for every bucket it mutates.
It records the statistics delta for each successful bucket mutation.  At a
configured interval, or when the accumulated delta exceeds a configured
threshold, the process submits the delta to the summary object.

The operation must be implemented as one server-side atomic operation, for
example a CLS method with the following semantics::

   summary.stats += delta
   summary.main_stats += main_delta
   return summary.stats

It must not be a client-side read-modify-write sequence.  Atomic application
prevents concurrent periodic updates from losing each other.  The returned
total refreshes the calling RGW's local quota cache.  Before the next flush,
the gateway also accounts for its own pending local delta when checking a
quota.

Normal librados retries retain the same in-flight operation identity and are
handled by the RADOS request path.  This design does not introduce a separate
application-level delta journal or per-writer sequence table.

Whole-scale update
------------------

The first time an RGW process needs to update a bucket summary in its
lifetime, it reads statistics from every shard of the current bucket-index
layout, aggregates them, and overwrites the summary object with the result.
This is a whole-scale update.

There is deliberately no global bootstrap coordinator: different RGWs may
perform their one-time whole-scale update independently.  A whole-scale
update is also required when the summary is missing or invalid.

The RGW should publish a whole-scale update only after the mutations covered
by its own initial accumulation are reflected in the bucket index, then clear
those covered local deltas.  Subsequent mutations are sent through ordinary
delta updates.

Consistency Model
=================

An ordinary update and a whole-scale update can overlap.  A full shard scan
may include a mutation whose ordinary delta was already applied to the summary
object, or it may miss a mutation whose shard was read before the mutation.
The whole-scale overwrite can therefore lose an already-applied ordinary
delta.

This is an accepted approximation.  For one whole-scale update, the potential
discrepancy is limited by the absolute statistics churn during the full scan
window, from the first shard read to publication of the summary object.  The
same error can recur when another RGW process first touches that bucket or
when a gateway restarts.  Unflushed in-memory deltas are also absent after a
gateway crash.

The resulting statistics are suitable for the existing eventually consistent
quota model, but they do not provide a strict distributed quota limit.  A
bucket can exceed quota by pending deltas across RGWs and by the error of
recent whole-scale updates.

Resharding and Bucket Incarnations
==================================

Resharding changes the bucket-index layout, not the meaning of a bucket
statistics delta.  Ordinary delta updates are therefore accepted across a
layout-generation change.

After the new layout becomes current, resharding performs a metadata-only
update of the summary object's layout generation.  This update retains the
aggregate statistics; it does not require a whole-scale scan.  A later
whole-scale update reads the current shard set and records the same generation.
The metadata update can be retried if the summary object is temporarily
unavailable, and it is not an ordinary-update fence.

The summary object is associated with the bucket id and validates the bucket
creation time.  If a resharding path creates a new bucket id, it naturally
uses a new summary object; the prior object is stale and may be cleaned up.
If the bucket id remains the same, the layout-generation check forces a
whole-scale refresh.

Operational Tradeoffs
=====================

This design removes the recurring ``N * S`` scan pattern.  It does not remove
all scans: a bucket may still receive one full scan per RGW process lifetime.
A fleet restart or rapid autoscaling can therefore cause a temporary scan burst.
This is an explicit tradeoff for avoiding global initialization coordination
and durable mutation materialization.

The implementation should expose metrics for whole-scale scan count and
duration, ordinary update failures, pending local-delta age, and summary age.
Those metrics make the approximation and any startup burst observable.
