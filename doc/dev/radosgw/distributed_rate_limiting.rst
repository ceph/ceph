=============================
Distributed RGW Rate Limiting
=============================

.. warning::
	This document is a draft, it might not be accurate

This document proposes an optional, eventually consistent rate-limiting
mode for deployments with more than one RGW instance in a zone. Option
names and implementation details remain subject to review.

Related discussion: `PR #70008
<https://github.com/ceph/ceph/pull/70008>`_ and `Tracker #78006
<http://tracker.ceph.com/issues/78006>`_. User-facing rate-limit behavior
today is described in :ref:`radosgw-rate-limit-management`. Zone-feature
migration follows :ref:`radosgw-zone-features`.

-------
Problem
-------

RGW currently enforces user, bucket, and anonymous rate limits with token
buckets in each ``radosgw`` process. The configuration is shared, but
runtime token state is not. Operators therefore divide a desired
zone-wide limit by the number of active gateways.

This has two undesirable consequences:

* changing the number of gateways changes the effective limit; and
* uneven request distribution can leave capacity unused on one gateway
  while another gateway rejects requests.

Persisting and updating one counter in RADOS for every request would
provide stronger consistency, but it would put distributed I/O on the
request path. An over-limit client would then consume RGW and OSD
resources before RGW could reject the request.

Memory pressure and frontend backpressure are separate problems.
Distributed rate limiting is not intended to control RGW concurrency or
prevent out-of-memory conditions.

-------------------
Goals and Non-Goals
-------------------

Goals:

* keep admission decisions in memory, with no RADOS operation on the
  HTTP request path;
* interpret a configured limit as an approximate per-zone limit without
  requiring operators to divide it by the gateway count;
* redistribute unused capacity when traffic is not balanced across
  gateways;
* retain the current ``RGWRateLimitInfo`` data model, admin commands,
  request classification, and HTTP error behavior;
* tolerate gateway joins, leaves, and individual restarts;
* remain optional, with the existing local behavior as the default; and
* enable the distributed mode only through an explicit zone feature after
  all gateways in the zone support it.

Non-goals:

* linearizable counters or billing-grade accounting;
* a strict upper bound during membership changes or communication
  failures;
* cross-zone or cross-zonegroup rate limits;
* durable runtime state across a simultaneous restart of all gateways;
* an external Redis or Valkey dependency;
* dynamic resharding of demand objects in the first implementation; and
* a solution for request concurrency, OSD backpressure, or RGW memory
  pressure.

--------------------
Current Request Path
--------------------

``ActiveRateLimiter`` owns the in-memory maps of ``RateLimiterEntry``
objects. For each request, ``rate_limit()`` in ``rgw_process.cc``
selects the effective user and bucket configuration and calls the local
limiter. Read and write bytes are charged later by ``rgw_rest.cc``. If
the user check succeeds but the bucket check fails, the user operation
token is returned.

Keys retain their current forms: ``"u" + uid`` for users, including the
anonymous user id, and ``"b" + bucket marker`` for buckets. User and
bucket limits remain independent dimensions of the same request.

The distributed mode preserves these call sites and the existing
token-bucket behavior. It changes the refill rate and burst capacity
supplied to each local entry. The configured limit remains the zone-wide
budget, while a coordinator calculates the local share of that budget.

---------------
Proposed Design
---------------

Each gateway runs two independent paths:

* The request path uses only its local token buckets.
* A background coordinator exchanges membership and demand reports with
  peer gateways through RADOS watch-notify.

The design uses dedicated objects in the zone's control pool. It follows
the watch registration, re-registration, sharding, and asynchronous
callback patterns used by ``RGWSI_Notify``, but does not reuse its
cache-specific payload or retry behavior. The periodic peer-exchange
flow is closer to ``rgw::sync_fairness`` and ``RGWRadosNotifyCR`` than
to cache invalidation.

No notify, read, write, or CLS operation is issued synchronously by a
request.

Coordination objects
~~~~~~~~~~~~~~~~~~~~

The coordinator uses:

* one membership object for gateway heartbeats; and
* a fixed number of demand objects selected by a stable hash of the
  rate-limit key.

Every participating gateway watches the membership object and all demand
objects. Sharding limits payload size and allows report processing to
run in parallel; it does not remove watch-notify fan-out.

The cache notification objects are not reused. Rate-limit messages have
a different lifetime and failure policy, and
``RGWSI_Notify::robust_notify()`` may replace a timed-out cache update
with a cache invalidation. That behavior is not meaningful for demand
reports.

Demand-object shard count is not chosen by trial alone. It is a function
of the maximum useful notify/omap payload size and the number of active
user and bucket keys that must be reported. The first implementation
does not reshard these objects at runtime. The deployment publishes a
hard cap on the number of concurrently tracked keys in distributed mode.
When that cap is exceeded, additional keys (or the whole gateway, if the
cap cannot be applied per key) fall back to local enforcement for those
keys until operators raise capacity or reduce the tracked set.

Membership
~~~~~~~~~~

Watch registrations alone are not used as the membership protocol. A
gateway creates a random instance identifier at startup and periodically
publishes a heartbeat. Peers maintain a soft-state membership table and
expire an instance after a configurable number of missed report
intervals.

Membership leave is soft-state, not a leader-owned delete:

* heartbeats are published to the membership object and delivered through
  watch-notify;
* each peer expires a missing instance in its in-memory table after the
  missed-interval threshold;
* no single RGW is responsible for removing another instance from the
  membership object; and
* optional explicit leave notifies may be added for faster convergence,
  but expiry remains the source of truth.

As a result:

* a restarted process has a new instance identifier;
* a stale pre-restart instance ages out without requiring a durable
  identity;
* adding a gateway reduces the base shares after the heartbeat is
  observed; and
* removing a gateway does not immediately expand other shares.
  Expansion is delayed until the old instance expires.

Membership is local to a zone. Mixed operation, where some gateways use
the distributed mode and others enforce the full local limit, is not
supported. Operators enable the mode only through the zone-feature
procedure described below.

Demand reports
~~~~~~~~~~~~~~

Each local limiter records demand over a short reporting interval.
Operation demand includes net consumed tokens and requests rejected by
that limiter because its local allocation was exhausted. Token giveback
after a bucket rejection is reflected in the net value. Bandwidth demand
is based on bytes observed by the existing read and write accounting
hooks.

Hot-path recording must not introduce a write mutex around admission.
Request threads update demand through atomic counters or a lock-free
handoff into the coordinator. Applying a new local allocation ``L_i``
similarly avoids a global lock on the request path (for example atomic
replacement of per-entry refill and burst parameters).

Only keys with an enabled limit and recent activity are reported.
Reports are batched by demand-object shard and bounded by entry count
and encoded size. Batching uses an in-process coordinator queue or
buffer in each RGW; it is not a CLS queue and is not on the HTTP
admission path. If that in-memory queue or the notify payload limit is
reached, the coordinator drops demand detail and retains the base
allocation described below.

Messages use Ceph's versioned ``encode()`` and ``decode()`` conventions
and include a sender instance identifier, sequence number, reporting
interval identifier, configuration fingerprint, rate-limit key, and
per-dimension demand. Receivers ignore duplicate or older sequence
numbers and do not merge demand from incompatible configuration
fingerprints.

Allocation
~~~~~~~~~~

Allocation is calculated independently for each rate-limit key and each
enabled dimension. Let ``B`` be the configured token budget and ``N``
the number of live gateway instances.

``B`` retains the semantics of :confval:`rgw_ratelimit_interval`. The
shorter gossip interval only controls how frequently demand and
allocations converge; it does not create a second token-bucket window.

Every gateway initially receives a base allocation of ``B / N``. A small
portion of this base is retained as a reserve for a new request on a
previously idle gateway. Base and reserve allocations together never
exceed ``B``.

The allocator then redistributes unused base capacity:

#. Gateways whose reported demand is below their current allocation
   surrender the unused portion above their reserve.
#. The surrendered capacity is divided among gateways whose attempted
   demand exceeds their allocation.
#. These steps repeat until there is no unused capacity or no
   unsatisfied demand.
#. Any remaining capacity is retained as equal burst reserve.

This is a max-min-fair water-filling calculation with a small reserve.
All gateways use the same ordered membership and demand inputs, so they
converge on the same result without a leader.

The resulting allocation controls both refill rate and burst capacity of
the local token bucket. When an allocation shrinks, existing positive
tokens are clamped to the new capacity. Existing byte debt is preserved.

When membership and reports are stable, the sum of local allocations for
a key and dimension is at most the configured budget. During startup, a
network partition, or a membership transition, gateways can temporarily
admit more than the configured zone-wide rate. Watch-notify messages are
not persistent: after one gateway restarts, peers retain soft state and
the restarted gateway converges on later reports. If every gateway
restarts at the same time, all runtime state is lost, as with the
current limiter.

----------------
Failure Behavior
----------------

The request path never waits for coordination. On notification failure,
the coordinator keeps its last allocation while peer state is still
within its expiry period.

After coordination has remained unavailable beyond that period,
deployments must choose between availability and tighter enforcement:

``local``
  Return gradually to the full local configured limit. This preserves
  availability but can multiply the effective limit by the number of
  gateways. This is the proposed default.

``hold``
  Keep the last known allocation and do not redistribute expired
  shares. This reduces expansion when a peer disappears, but does not
  provide a hard global bound. In particular, when a new gateway joins
  before membership and allocations reconverge, the sum of local shares
  can temporarily exceed ``B``. ``hold`` also underutilizes capacity
  after a gateway failure until operators or later intervals correct
  the membership view.

Startup before the first membership view follows the same policy. The
selected policy, current mode, peer count, age of the last successful
report, and notification errors must be visible in logs and metrics.

--------------------------
Rollout and Zone Feature
--------------------------

Distributed rate limiting is opt-in. Until the feature is enabled, every
gateway continues to use the existing local limiter and the existing
``RGWRateLimitInfo`` configuration without change.

The implementation adds a zone feature (see
:ref:`radosgw-zone-features`). The intended rollout is:

#. Upgrade all RGWs (and required OSDs, if any new object-class
   dependency is introduced later) in the zone, then mark the feature
   supported on the zone.
#. Keep ``rgw_ratelimit_backend`` at ``local`` so behavior remains
   unchanged.
#. When every zone that participates in the zonegroup supports the
   feature, enable it on the zonegroup and commit a period update.
#. Only then select the distributed backend on the gateways that should
   participate, and restart those gateways as required.

Until that enablement completes, local rate limits already configured by
operators keep working exactly as today. Mixed fleets, where some
gateways run distributed mode and others still apply the full local
budget, are not supported.

-------------
Configuration
-------------

The exact option names are provisional. The implementation is expected
to need options equivalent to:

* ``rgw_ratelimit_backend`` (default ``local``): select ``local`` or the
  proposed ``distributed`` mode. The first shipping step exposes only
  ``local`` until the zone feature and distributed path are ready;
* ``rgw_ratelimit_gossip_interval``: heartbeat and demand-report
  interval;
* ``rgw_ratelimit_gossip_num_shards``: fixed number of dedicated demand
  objects, sized from payload limits and the tracked-key cap rather than
  from ad-hoc testing alone;
* ``rgw_ratelimit_gossip_max_tracked_keys``: hard cap that triggers
  per-key or gateway fallback to local enforcement; and
* ``rgw_ratelimit_gossip_failure_mode`` (default ``local``): select
  ``local`` or ``hold`` after coordination expires.

The existing :confval:`rgw_ratelimit_interval` and all user, bucket, and
global rate-limit settings remain authoritative. A restart is expected
when the backend is changed.

---------
Multisite
---------

Configuration can continue to replicate through existing RGW metadata
and period mechanisms. Runtime membership, demand, and allocation do not
cross zone boundaries. Each zone therefore enforces an independent
approximation of the configured limit. Zone-feature support and
enablement follow the same per-zone and zonegroup rules as other RGW
features.

-----------------------
Alternatives Considered
-----------------------

* **Rate-limit-specific CLS methods:** stronger consistency, but require
  a distributed write before each admission decision and move
  RGW-specific policy into the OSD. Not proposed for the request path.
* **Generic CLS counters:** keep token-bucket policy in RGW, but still
  require a RADOS operation when used for each request. Not proposed for
  admission. A later experiment may record counters asynchronously after
  the client response is sent, which would raise cluster IOPS without
  adding that latency to the request. That path is for persistence
  research only and does not replace watch-notify coordination.
* **External Redis or Valkey:** shared counters and persistence, but
  normally add a network operation to each request and introduce another
  operational dependency.
* **Startup-only counter restore:** reduces the effect of a restart, but
  does not coordinate gateways while they are running.

-------
Testing
-------

Implementation should include:

* allocator unit tests for balanced and skewed demand, idle peers,
  saturation, rounding, changing membership, and all independently
  limited operation and byte dimensions;
* protocol tests for versioning, malformed payloads, duplicate and
  out-of-order messages, payload bounds, peer expiry, and restart with a
  new instance identifier;
* concurrency tests that show request-path demand accounting and
  allocation updates do not take a write mutex on the admission path;
* multi-RGW tests that compare aggregate accepted traffic with the
  configured budget, verify redistribution from idle gateways, exercise
  join/leave/restart/partition, inject notify timeouts for both failure
  modes, verify overshoot during join under ``hold``, verify that local
  mode is unchanged, and verify that request threads issue no RADOS
  coordination operations;
* capacity tests that relate shard count and the tracked-key hard cap to
  payload size and fallback-to-local behavior; and
* scale tests that report request latency and throughput relative to the
  local limiter, RADOS notification rate and bytes, convergence time,
  overshoot, underutilization, and coordinator CPU and memory.

Default gossip interval values will still be refined from those results
after the capacity model for shards and tracked keys is fixed.
