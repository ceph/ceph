.. _cephfs_mds_phase_times:

================
MDS Phase Times
================

An MDS rank does almost all of its work under a single lock, ``mds_lock``.
Client requests dispatched by the messenger threads, journal completions run by
the finisher, the periodic tick, cache trimming, client recall and log trimming
all take it. That makes ``mds_lock`` -- rather than any one thread -- the
resource a busy rank exhausts first, and it is why a rank rarely uses much more
than one core no matter how many are available.

Phase times answer the question that follows from this: of the wall clock a
rank spends holding ``mds_lock``, how much goes to each kind of work? Knowing
that is usually the difference between a useful scaling decision and a guess,
because the remedies point in opposite directions -- more cache memory on the
rank, more ranks, or a faster metadata pool.

Tracking is off by default. Turn it on with
:confval:`mds_enable_phase_tracker`, which may be changed at runtime:

.. prompt:: bash #

   ceph config set mds mds_enable_phase_tracker true

Enabling it resets the counters, so everything reported afterwards describes
only the period since it was turned on; work that was already in progress when
it was turned on is not charged to any phase, since only part of it happened in
the period being reported. See `Cost`_ for what it costs to leave on, and why
it is not on by default.

Reading the breakdown
=====================

For a one-shot view, ask a rank where its time went:

.. prompt:: bash #

   ceph tell mds.a dump phase times

Abridged -- the real output lists every phase, busiest first:

.. code-block:: json

   {
       "enabled": true,
       "elapsed_sec": 3600.12,
       "mds_lock": {
           "acquisitions": 41203866,
           "wait_sec": 8121.55,
           "held_sec": 3216.44,
           "utilization": 0.893
       },
       "accounted_sec": 3201.87,
       "phases": [
           {
               "phase": "client_request",
               "total_sec": 2402.11,
               "count": 18442013,
               "mean_ms": 0.130,
               "pct_of_elapsed": 66.72,
               "pct_of_accounted": 75.02
           },
           {
               "phase": "client_caps",
               "total_sec": 401.55,
               "count": 9120441,
               "mean_ms": 0.044,
               "pct_of_elapsed": 11.15,
               "pct_of_accounted": 12.54
           },
           {
               "phase": "cache_trim",
               "total_sec": 288.90,
               "count": 3591,
               "mean_ms": 80.45,
               "pct_of_elapsed": 8.02,
               "pct_of_accounted": 9.02
           }
       ]
   }

Everything is reported for the period since tracking was enabled. The numbers
worth reading first:

``mds_lock.utilization``
  The share of the wall clock the rank spent holding ``mds_lock``. A rank
  approaching ``1.0`` is saturated and cannot go faster without being split up,
  whatever else is tuned. A rank well below it that still serves slow requests
  is waiting on something else -- most often the metadata pool.

``mds_lock.wait_sec``
  Time other threads spent blocked on the lock. This is a sum over threads, so
  it can exceed the elapsed time; what matters is its trend against
  ``held_sec``.

``phases[].pct_of_elapsed``
  The share of the wall clock charged to each phase. Time is charged
  *exclusively*: a phase is credited only with the time spent in it and not
  with the time spent in phases it dispatched into, so the values are additive.

``accounted_sec``
  The sum of all phases. It should track ``mds_lock.held_sec`` closely -- a
  little over it, since ``heap_release`` is counted here but runs without the
  lock. A large *shortfall* means significant work is happening in a path that
  is not yet instrumented.

Phases
======

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Phase
     - Work charged to it
   * - ``client_request``
     - Handling ``CEPH_MSG_CLIENT_REQUEST``: path traversal, locking, journal
       submission and replies.
   * - ``client_caps``
     - Capability and lease messages from clients, including cap flushes and
       releases.
   * - ``client_session``
     - Session open/close, reconnect and reclaim.
   * - ``peer_request``
     - Requests from other ranks acting on behalf of a client request.
   * - ``cache_message``
     - Inter-MDS cache traffic: discover, resolve, dentry and inode updates.
   * - ``migrator_message``
     - Subtree import and export. A large share means the balancer is moving
       metadata around, which is work not being spent on clients.
   * - ``locker_message``
     - Distributed lock traffic between ranks.
   * - ``heartbeat_message``
     - MDS load heartbeats consumed by the balancer.
   * - ``table_message``, ``quiesce_message``, ``scrub_message``
     - Anchor/snap table traffic, quiesce database replication, and scrub.
   * - ``other_message``
     - Anything else reaching ``MDSRank::handle_message()``. This should stay
       near zero; a large share means a message class that deserves a phase of
       its own is being dispatched in volume.
   * - ``io_completion``
     - Journal and object IO completions, run under ``mds_lock`` by the
       objecter's finisher. This is the second half of a client request that
       had to wait for the journal, so on a write-heavy rank it is charged
       much of the work ``client_request`` appears to be missing.
   * - ``finished_contexts``
     - Queued contexts: waiters unblocked by an IO completion and drained on
       the next pass through dispatch.
   * - ``tick``
     - The periodic tick, excluding ``locker_tick`` and ``balancer_tick``.
   * - ``locker_tick``
     - Periodic cap revocation and idle session detection.
   * - ``balancer_tick``
     - Periodic load balancing decisions.
   * - ``cache_memory``
     - Sampling the process memory footprint.
   * - ``cache_trim``
     - Trimming inodes and dentries out of the cache.
   * - ``client_leases``
     - Trimming client dentry leases.
   * - ``client_recall``
     - Asking clients to release capabilities so the cache can shrink.
   * - ``heap_release``
     - Returning free memory to the OS. Unlike the phases above this one runs
       *without* ``mds_lock``, so it is excluded from the lock utilization, but
       a slow release still stalls all cache trimming behind it.
   * - ``log_trim``
     - Trimming journal segments.

Graphing it
===========

The same values are exported as perf counters, so they reach
``mgr/prometheus`` and any other collector without further work:

.. prompt:: bash #

   ceph tell mds.a perf dump mds_phase

Each phase is a time average: ``sum`` is the total time charged to the phase in
nanoseconds and ``avgcount`` is the number of times it was entered, so the rate
of ``sum`` is the fraction of a rank spent on that phase and
``sum / avgcount`` is the mean cost of one entry. ``lock_held`` and
``lock_wait`` are plain time counters; the rate of ``lock_held`` is the rank's
utilization.

Deciding what to do about it
============================

.. list-table::
   :widths: 45 55
   :header-rows: 1

   * - What the breakdown shows
     - What it usually means
   * - ``utilization`` near 1, ``client_request``, ``io_completion`` and
       ``client_caps`` dominating
     - The rank is serialization-bound on real client work. Scale out: add
       ranks, and pin subtrees to spread the load deliberately. More memory
       will not help.
   * - ``cache_trim`` plus ``client_recall`` a large share, ``mds_mem.rss``
       near :confval:`mds_cache_memory_limit`, high ``mds.inodes_expired``
     - The rank is thrashing its cache. Scale up:
       raise :confval:`mds_cache_memory_limit` on that rank. See
       :doc:`/cephfs/cache-configuration`.
   * - ``utilization`` low but ``mds_server.req_*_latency`` and
       ``mds_log.jlat`` high
     - The rank is idle waiting for the metadata pool. Neither scaling
       direction helps; the OSDs backing the metadata pool do.
   * - ``migrator_message`` and ``balancer_tick`` a large share
     - The balancer is thrashing subtrees. Pin the hot subtrees, or tune the
       ephemeral pinning and ``mds_bal_*`` settings. See
       :doc:`/cephfs/multimds`.
   * - ``client_recall`` high with ``mds_server.*_recall_throttle`` climbing
     - Clients are not releasing caps fast enough for recall to keep up.
       Investigate the clients before adding MDS capacity.

Cost
====

Accounting is not free, and because it happens under ``mds_lock`` its cost is
serialized along with everything else. That is why it is off by default: a
rank should not pay for instrumentation nobody is reading.

Per instrumented scope the cost is two monotonic clock reads, two loads of a
generation counter, and the three atomic increments any Ceph time average
already costs; per ``mds_lock`` acquisition it is three more clock reads and
three atomic increments. A client
request typically passes through two acquisitions and two scopes -- once on
dispatch, once when its journal completion runs -- so roughly ten clock reads
and a dozen increments, a few hundred nanoseconds of added serialized work per
request.

Against a mean ``client_request`` cost in the hundreds of microseconds that is
a fraction of a percent per request, and at 50,000 requests per second on a
saturated rank it comes to one or two percent of the rank's capacity. It adds
next to no cross-core cache line contention: every counter it touches except
``heap_release`` is written under ``mds_lock`` and is therefore effectively
single-writer.

Those numbers assume ``clock_gettime`` is a vDSO read, which requires the host
to be using the TSC clocksource:

.. prompt:: bash #

   cat /sys/devices/system/clocksource/clocksource0/current_clocksource

On a host that has fallen back to ``hpet``, ``acpi_pm`` or an unaccelerated
hypervisor clock, each read can cost hundreds of nanoseconds to microseconds
instead of tens, and the overhead stops being negligible. Leave the tracker
off on such hosts, or turn it on only for as long as it takes to answer a
question. With it off the counters do not advance and the residual cost is one
relaxed load of a boolean per lock acquisition.

If you need to confirm the overhead on your own hardware rather than take
these figures on trust, the honest measurement is a fixed metadata benchmark
run twice against the same rank with the setting toggled between runs, since
the tracker cannot measure its own cost.

Limitations
===========

Phase times measure wall clock, not CPU time: a phase that blocks while
holding ``mds_lock`` is charged for the time it blocks, which is the point --
that time is denied to every other phase. But it does mean a large
``client_request`` share is not by itself evidence that the rank is CPU-bound;
confirm with ``mds_lock.utilization`` and, if a call-level breakdown is needed,
with a profiler against ``ceph-mds``.

Work that runs off ``mds_lock`` entirely -- the objecter, the purge queue,
messenger threads -- is not covered here. Use ``perf dump`` sections
``objecter``, ``purge_queue`` and ``finisher-*`` for those.
