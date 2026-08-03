=================
BFQ Op Scheduler
=================

The ``bfq`` op queue (``osd_op_queue = bfq``, experimental) is modeled on
the Linux BFQ (Budget Fair Queueing) I/O scheduler as exposed through the
cgroups v2 io controller.  It is an alternative to ``mclock_scheduler``
and ``wpq`` built around a different first principle: **purely
proportional sharing**.  Where mclock's reservation and limit tags are
denominated in absolute IOPS -- which is why it needs the boot-time OSD
bench and the ``osd_mclock_max_capacity_iops_*`` estimates -- bfq
distributes whatever throughput the OSD actually delivers according to
weights, and needs no estimate of device capacity at all.

Hierarchy
=========

Service (in scaled bytes) is shared by a two-level B-WF2Q+ hierarchy of
weighted groups, in the spirit of ``io.bfq.weight``::

   root
   |-- client group                       (osd_bfq_client_group_weight)
   |   |-- block       (rbd pools)        (osd_bfq_client_block_weight)
   |   |-- object      (rgw data pools)   (osd_bfq_client_object_weight)
   |   |-- object_meta (rgw omap pools)   (osd_bfq_client_object_meta_weight)
   |   |-- file        (cephfs data)      (osd_bfq_client_file_weight)
   |   |-- file_meta   (cephfs metadata)  (osd_bfq_client_file_meta_weight)
   |   `-- other                          (osd_bfq_client_other_weight)
   `-- background group                   (osd_bfq_background_group_weight)
       |-- recovery                       (osd_bfq_background_recovery_weight)
       `-- best_effort                    (osd_bfq_background_best_effort_weight)

Client ops are classified into workload streams by the application
metadata of their pool (``ceph osd pool application enable``): pools
tagged ``rbd`` map to the block stream, ``rgw`` to the object streams,
``cephfs`` to the file streams; untagged or ambiguously tagged pools
fall into the other stream.

``rgw`` and ``cephfs`` pools are further split into *data* and
*metadata* streams, from **explicit application metadata only**:

* cephfs pools already carry the marker: the mon stamps a ``data`` or
  ``metadata`` key under the ``cephfs`` application tag at ``fs new``
  / ``fs add_data_pool`` time, for pre-created pools too.
* rgw pools are marked with a ``traffic-class`` key under the ``rgw``
  tag::

     ceph osd pool application set default.rgw.buckets.index rgw traffic-class metadata

  ``metadata`` selects the metadata stream; any other value, or no
  key, serves data.  An explicit ``traffic-class`` also overrides the
  cephfs marker, and is the intended forward path: *traffic class* is
  a scheduling concept the application (or operator) declares, in the
  spirit of Tectonic's TrafficGroup/TrafficClass model [Tectonic21]_.

Autoscaler hints (the ``bulk`` flag, ``pg_autoscale_bias``) are
deliberately **not** consulted, although rgw happens to set them on
the pools it creates: they are absent on pools pre-created by the
operator (common where the autoscaler is unwelcome), inert or rejected
under ratio-driven autoscaling, and pool sizing hints are not QoS
policy.  Bulk data I/O and tiny latency-critical omap I/O have nothing
in common -- separate leaves give each its own self-tuning budget and
its own weight, so a bucket listing or an MDS journal flush never
queues behind a competing stream's object bodies.

The per-shard pool map refreshes on every OSDMap the shard consumes, so
tag changes take effect without a restart.  Background ops are split by
their existing scheduler class (``background_recovery`` vs
``background_best_effort``).

Weights follow the cgroups v2 convention: 1..1000, default 100, purely
relative, adjustable at runtime.

Algorithm
=========

Each backlogged leaf stream is granted a byte *budget*
(``osd_bfq_max_budget`` caps it) and is served exclusively until the
budget is exhausted, the stream empties, or the round exceeds
``osd_bfq_budget_timeout``.  Streams and groups are scheduled by WF2Q+:
an entity is *eligible* once its virtual start tag is at or behind the
tree's virtual time, and the eligible entity with the smallest virtual
finish tag runs next.  Finish tags are computed over the *assigned*
budget at activation and **back-shifted** to the *consumed* service at
expiration, so unused budget carries no penalty.  The next budget adapts
to observed demand: it doubles (up to the cap) when a round ends by
exhaustion and shrinks toward actual usage when the stream empties.

Deliberate exclusions relative to the kernel implementation:

* no weight raising (the interactive/soft-realtime low-latency
  heuristics), and
* no device idling/anticipation -- an emptied stream expires
  immediately, and ``dequeue()`` never asks the shard worker to wait.

Accounting model
================

Service is charged at *dequeue* (dispatch to a shard worker), not at
device completion; the kernel charges at completion and knows the device
is exclusively occupied, while the OSD does not.  Fairness is therefore
over dispatched scaled bytes -- the same accounting model mclock already
uses -- and budget exclusivity means exclusive *selection*, not
exclusive device occupancy, when ``osd_op_num_threads_per_shard > 1``.

Cost model
----------

The time an I/O actually takes has the form::

   t(IO) = per-op pipeline overhead + positioning time + bytes / bandwidth

By default each item is charged ``max(item cost, osd_bfq_min_cost)``
bytes: pure byte counting, with the floor standing in for both fixed
components at once.  This needs no calibration and is a good fit for
large ops on flash, but it is a single-constant approximation of a
two-constant reality -- above the floor, a stream of small random ops
is undercharged relative to the pipeline time it consumes, and on
HDDs large sequential ops are overcharged relative to their
seek-dominated device time.

Deployments that care can switch to the additive two-constant model by
setting either of:

* ``osd_bfq_cost_per_op`` -- the fixed OSD pipeline overhead of any op
  (network decode, WAL append, RocksDB update, checksums) in
  byte-equivalents.  On NVMe with small-block workloads the OSD's
  ops/s ceiling, not the device, is the bottleneck; set this to the op
  size at which the ops/s and bandwidth ceilings meet (typically
  16-64 KiB).
* ``osd_bfq_cost_per_io`` -- the positioning (seek) cost in
  byte-equivalents.  Zero on flash; on HDDs roughly the seek-bandwidth
  product (512 KiB to 1 MiB for 7200 rpm), which charges a 4 KiB and a
  256 KiB random read nearly the same, as the platter does.

When either constant is non-zero every op is charged ``cost_per_op +
cost_per_io + bytes`` and the ``osd_bfq_min_cost`` floor is ignored.
Both constants default to 0: the defaults stay calibration-free, and
precision is opt-in -- fio measurements, no boot-time benchmark.

As with mclock, ops of the ``immediate`` class (peering and similar) and
ops at or above ``osd_op_queue_cut_off`` bypass the hierarchy through a
strict priority queue, and requeued ops (``enqueue_front``) re-enter via
that queue rather than through the fair hierarchy.

The latency cost of budget exclusivity
======================================

Budget exclusivity is where bfq's throughput isolation comes from, and
it has a price: while one stream is in service every other stream
waits, so a small op arriving on an idle stream can wait for the
in-service stream's remaining budget to drain.  The per-rotation bound
is approximately::

   max wait ~= osd_bfq_max_budget / device_bandwidth   (per competing stream)

with ``osd_bfq_budget_timeout`` (default 125 ms) as a coarse backstop.
An 8 MiB budget is ~2.7 ms on a 3 GB/s NVMe but ~53 ms on a 150 MB/s
HDD.  Because the bound is bandwidth-proportional, the default
``osd_bfq_max_budget = 0`` (auto) selects 1 MiB on non-rotational
devices -- rotation between streams costs nothing on flash, so the
smaller budget buys latency without giving up throughput isolation --
and 8 MiB on rotational devices, where longer exclusive rounds
preserve sequentiality (matching the spirit of the kernel BFQ
default).  The kernel's answer to this trade-off, weight raising for
latency-sensitive queues, is deliberately out of scope (see above).

Two further mitigations are structural.  The strict queue bypasses the
hierarchy entirely, so high-priority ops never pay the rotation
latency.  And because the latency floor is paid per *stream* rotation,
the data/metadata split above shrinks it where it hurts most: a
metadata stream gets its own short exclusive rounds (its budget
self-tunes down to tiny-op demand) instead of queueing inside a shared
stream behind bulk data.

Each OSD shard runs an independent scheduler instance, so weights
describe per-shard shares; fairness across shards is statistical, via
PG-to-shard hashing (again matching mclock).

Observability
=============

``ceph daemon osd.N dump_op_pq_state`` dumps, per shard, the virtual
times and tags of both tree levels, per-stream queue depths and next
budgets, and the strict queue.

Microbenchmarks
===============

``ceph_bench_op_scheduler`` (built from ``src/test/osd``) runs identical
synthetic workloads through wpq, mclock, and bfq in-process, pacing
dequeues at a simulated device rate so saturation dynamics are
meaningful; mclock's capacity model is calibrated to the same rate.  The
scenarios: a backlogged victim vs an aggressor fanning out over 1..16
client sessions (share isolation), a paced victim under a saturating
aggressor (latency isolation), and client-vs-recovery (class weights).
``unittest_scheduler_isolation`` asserts loose bounds on the bfq
isolation properties; because the harness paces in wall time it skips
itself unless ``CEPH_TEST_SCHEDULER_ISOLATION`` is set in the
environment (shared CI executors are too noisy for it).

The headline result: with an rbd-pool victim and an rgw-pool aggressor,
wpq and mclock dilute the victim's share as 1/(sessions+1) -- wpq
round-robins owners, and mclock maps *all* client ops to a single
dmclock client -- while bfq holds the configured weight ratio flat
regardless of session count, and bounds a paced victim's p99 queueing
delay by budget rotation rather than by the aggressor's backlog depth.

Follow-up candidates
====================

* Auto-tuning ``osd_bfq_max_budget`` from the observed per-shard service
  rate (the analogue of the kernel's peak-rate estimator -- measured
  online, never configured); the rotational-media default above is the
  static approximation of this.
* An ``osd_bfq_latency_target_ms`` that derives the budget cap from a
  latency goal instead of exposing bytes directly.
* Evolving the fixed stream enum into declared *traffic groups* and
  *traffic classes* (the Tectonic model [Tectonic21]_): rgw stamping
  ``traffic-class`` on the pools it creates; dynamic leaves registered
  from pool metadata (e.g. ``traffic-group: warehouse``) with weights
  from a map rather than one option per stream, letting rgw steer
  tenants into distinct groups via placement targets even on
  single-protocol clusters; ultimately an op-carried, capability-gated
  (group, class) tag for traffic that shares a pool (the bucket index
  pool serves every principal in a zone, so principal-level isolation
  of index I/O cannot be pool-granular).
* Perf counters mirroring the mclock ones.
* Completion-based charging, which would recover more of BFQ's
  device-time fairness but requires feedback from the op pipeline.

References
==========

.. [Tectonic21] Satadru Pan et al., "Facebook's Tectonic Filesystem:
   Efficiency from Exascale", FAST '21.
   https://www.usenix.org/conference/fast21/presentation/pan
