======================================================================
SeaStore write-path benchmark: replicated pool (size=5) results
======================================================================

:Date: 2026-07 (point-in-time snapshot of the crimson/SeaStore build below;
       absolute numbers track this build and test bed, not a spec)
:Hardware: 8 hosts (AMD EPYC 7643, 48 physical cores / 96 threads,
           5x 1.7 TB NVMe, 251 GB RAM), 100 GbE cluster fabric
:Cluster: 40 crimson OSDs (5/host, 1 OSD per NVMe), 3 mons, pool ``rep5``
          size=5 min_size=3 pg_num=1024 (~128 PGs/OSD)
:Client: single remote host (96 cores, 100 GbE) driving ``fio``
         (libaio) over kernel-mapped RBD images
:Ceph: 20.2.2, RelWithDebInfo + IPO/LTO, same source tree. Crimson OSD +
       SeaStore (``WITH_CRIMSON=ON, SPDK=OFF``) evaluated against classic OSD +
       BlueStore (``WITH_CRIMSON=OFF, SPDK=ON``).
:Related: single-host OSD / core scaling is in
          :doc:`seastore-write-path-single-host-benchmark`

.. contents::
   :local:
   :depth: 1


Bottom line
===========

On identical hardware, **classic BlueStore is ~7x faster than crimson/SeaStore
on the 4 KiB write path and stays stable at any concurrency**. Crimson does not sustain replicated
write load: across many layouts - varying OSD count, reactors per OSD, and the
``--memory`` cap - every run crashed an OSD within a couple of minutes. The
dominant crash was a messenger bug (``FrameAssemblerV2`` late frame-abort), now
**fixed** (tracker https://tracker.ceph.com/issues/78119, upstream PR
https://github.com/ceph/ceph/pull/70166). Even with that fixed a **SeaStore
omap abort still crashes OSDs under load** - the runs kept dying, just via the
omap path. One **clean 25-min soak** (5x4 = 5 OSDs/host x 4 cores each, no ``--memory`` cap) reached
**38k IOPS** (vs classic 183k) with a flat ~30-38k ceiling
and p99 up to ~1.15 s, but that clean window is not reliably reproducible. All
figures here are **4 KiB random write**; **4 MiB (bandwidth) write could not be
characterized** - crimson OSDs aborted under sustained load before a clean 4M
run completed.
**In this build crimson/SeaStore does not yet sustain replicated write load; the
messenger fix here removes one crash, and the remaining SeaStore omap abort is
the main blocker to a stable characterization.**


Cluster and method
==================

* 8 OSD hosts, **40 OSDs** (5/host, each on its own NVMe), pool ``rep5``
  size=5 min_size=3, **pg_num=1024** (~128 PGs/OSD; the smaller-OSD-count
  layouts below scaled pg_num to keep ~128-160 PGs/OSD). Each client write costs
  5 OSD writes.
* Load from **one remote client** via an RBD image kernel-mapped to
  ``/dev/rbdN`` + ``fio libaio`` (fio's ``rados`` ioengine hangs against
  crimson; the RBD path gives real percentiles). One client is sufficient for
  the 4K IOPS test but bottlenecks on 4M bandwidth - more clients would be
  needed to characterize 4M.
* **Precondition:** sequential 1 MiB fill of a ~1 TiB working set (4 images).
  **Soak:** sustained 4 KiB random overwrite (``numjobs=8 iodepth=64``).
* **Concurrency notation:** ``njN x qdM`` = fio ``numjobs=N iodepth=M``, i.e.
  N worker threads each keeping M IOs in flight (~N*M outstanding IOs total),
  spread across the 4 kernel-mapped RBD images. E.g. ``nj8 x qd64`` = 8 jobs x
  64 = up to 512 outstanding 4 KiB writes.
* **Health-gated:** each run holds until 40/40 OSDs up and all PGs
  ``active+clean`` before preconditioning, and re-verifies before the soak.
* Every layout requires a **fresh mon redeploy** (new fsid); in-place OSD
  reprovision reliably wedges the cluster.


Memory behavior (run uncapped until upstream adds backpressure)
===============================================================

The first run (27 GB/OSD) OOM'd under the soak with ``std::bad_alloc``. Root
cause is a **per-shard** cap, not a total-budget cap.

**The Seastar allocator is not a backpressure mechanism — it is a brute-force
abort (ATM).** Verified in ``src/seastar/src/core/memory.cc``: when a shard
cannot satisfy an allocation it runs its registered reclaimers *synchronously*
(best-effort cache eviction — ``find_and_unlink_span_reclaiming`` ->
``run_reclaimers``); if that frees nothing it **throws ``std::bad_alloc``**, or
in the can't-even-throw OOM path calls ``abort()`` directly. Nothing throttles,
pauses, or slows write ingest as a shard nears its ``--memory / cores`` slice —
it is allocate-or-die. SeaStore does not throttle ingest against the budget
either, so the OSD keeps accepting writes until the next allocation fails, then
aborts the whole process (the ``Bad allocation [buffer:1]`` crash). *Real*
backpressure would slow ingest before the wall; there is none today.

Details:

* Seastar splits ``--memory`` **equally per reactor**, so the real limit is
  ``--memory / cores`` *per shard*. A shard that exhausts its slice self-aborts
  the whole OSD while the machine still has hundreds of GB free (Seastar's
  per-shard allocator, **not** the kernel OOM-killer, **not** systemd).
* A shard uses **~5 GB in steady state** — about half of that (~2 GB) is
  SeaStore's metadata LRU cache (the ``seastore_cache_lru_size`` 2 GB/reactor
  default; the rest is transaction/working memory).
* Shards are **not uniform**: the busiest shard runs about **2x** the average.
  So you must size for the *hot* shard, not the average → budget
  **~10-12 GB per reactor** (~2x the ~5 GB footprint).
* Worked example: the first run's ``--memory 27G`` over 9 reactors is only
  **~3 GB/reactor** — below even the *average* footprint, so it OOMs. In
  contrast ``--memory 50G`` over 4 reactors = **12.5 GB/reactor**, which holds.

With adequate per-shard memory the OOM disappears (RSS plateaus at ~5 GB/shard),
and uncapped OSDs self-limit at ~19.4 GB RSS (~4.85 GB/shard, flat across OSDs)
- memory does not run away. Recommendation: **run without a ``--memory`` cap**
and plan ~20-25 GB host RAM per OSD. This settles only the memory dimension; the
OSD crashes discussed under Instability are a separate messenger/SeaStore
problem, not memory exhaustion, and real ingest backpressure remains an upstream
prerequisite.


Instability - the blocker
=========================

**Under replication (size=5), crimson does not sustain write load.** Many
layouts were run - varying OSD count (1-5 per host, 8-40 total), reactors per OSD
(4-10), and the per-shard ``--memory`` cap (30-120 GB, and uncapped) - and under
sustained 4 KiB write load every one crashed an OSD within a couple of minutes,
during either the sequential precondition or the random-write soak.

The dominant crash was a **messenger bug**: ``FrameAssemblerV2`` aborted on a
kernel-client *late frame-abort* (root cause and fix below). That is now
**fixed** (tracker https://tracker.ceph.com/issues/78119, upstream PR
https://github.com/ceph/ceph/pull/70166). With the fix in place the messenger
crash no longer occurs - but a **SeaStore omap abort still crashes OSDs under
load**, so the runs kept dying, just via the omap path. **One clean 25-min soak** (5x4, no ``--memory`` cap -
the 38k result below) completed before an OSD aborted; that clean window is **not reliably reproducible** - the
omap abort is a probabilistic hazard whose rate scales with concurrency and with
SeaStore cleaner/GC churn (it fired at nj64 in one run, ~80 s into the soak in
another).

* Crimson has **no per-shard fault isolation** - one shard's ``abort()`` kills
  the whole OSD process.
* **No cluster-level fault containment either.** A single OSD abort can ignite
  an unassisted cluster-wide cascade: the recovery/peering churn crash-loops
  other OSDs with no client IO at all, and only an operator freeze
  (``nodown,noout,norecover,nobackfill,pause``) stops it.

Root cause: two upstream ``ceph_abort`` paths
---------------------------------------------

1. **Messenger (FIXED).** ``FrameAssemblerV2`` ``ceph_abort_msg("TODO")``: the
   receive path for the kernel RBD client's *late frame-abort* was never
   implemented - a literal ``TODO``. The kernel client legally aborts a
   partially-transmitted frame (``FRAME_LATE_STATUS_ABORTED``) when it revokes
   an in-flight message - e.g. re-sending an osd op after an osdmap change
   re-targets its PG. Classic drops the aborted frame and continues
   (``ProtocolV2::_handle_read_frame_epilogue_main``); crimson aborted the whole
   OSD, and the resulting osdmap churn caused further revokes on other sessions
   (the cascade). Fixed with a classic-parity drop-and-continue path in
   ``read_frame_payload`` - tracker https://tracker.ceph.com/issues/78119,
   upstream PR https://github.com/ceph/ceph/pull/70166 (branch
   ``vgoot/crimson/net/late-frame-abort``; builds clean on ``main``, crimson
   messenger unit tests pass).
2. **SeaStore metadata integrity (still open).** ``omap_load_extent`` /
   ``pin_to_extent`` aborts on a bad OMap B-tree extent read or an *"extent
   checksum inconsistent"* mismatch (``transaction_manager.h``). Confirmed
   present and unfixed in current upstream ``ceph/ceph`` v21.3.0 (git blame:
   upstream authors, no local changes). **This is the crash that remains after
   the messenger fix**; only one clean soak completed, so it is
   probabilistically hard to reproduce.

Follow-on damage
----------------

* An abort under load can leave SeaStore **on-disk inconsistent**, so the OSD
  then **crash-loops on restart** (extent-checksum abort) -> permanent OSD loss.
* A cold-restarted crimson OSD also fails to rejoin an aged cluster (suspected
  cephx rotating-key decode), compounding the loss.
* A residual **reactor-stall abort** was also seen: escalating "``Reactor
  stalled for N ms``" then "``Aborting on shard M``" at low RSS, independent of
  the memory budget.


Crimson throughput (the one clean 5x4 no-cap soak)
==================================================

Measured on the single clean run that completed (5x4, no ``--memory``
cap) - these are *real sustained numbers*, not pre-crash bursts, though the run
is **not reliably reproducible** (the SeaStore omap abort, see Instability):

* **25-min 4K soak (nj8 x qd64): 38.0k IOPS sustained** (149 MiB/s, 218 GiB
  written), p50 4.0 ms, **p99 296 ms**, p99.9 768 ms. 40/40 OSDs up, zero
  crashes. Clean 1 TiB precondition before it (~1.9 GiB/s 1 MiB seq).
* **Concurrency sweep: the curve is FLAT.** ~30-38k IOPS from nj4 all the way
  to nj48 - crimson's knee is effectively at the *first rung*, and added
  concurrency converts entirely into latency (p99 55 ms at nj4 -> 1.15 s at
  nj48) - classic SeaStore-backpressure behavior. At nj64 it crashed
  (stall-spiral -> omap abort). The client stayed ~97% idle: cluster-bound.

.. list-table:: 4 KiB random write sweep (60 s/step, single client) - crimson vs classic
   :header-rows: 1

   * - parallelism
     - crimson IOPS
     - crimson p99
     - classic IOPS
     - classic p99
     - classic BW
   * - nj4 x qd64
     - 30.6k
     - 55 ms
     - 140k
     - 5.7 ms
     - 547 MiB/s
   * - nj8 x qd64
     - 28.5k
     - 558 ms
     - 183k
     - 12.8 ms
     - 717 MiB/s
   * - nj16 x qd64
     - 32.5k
     - 793 ms
     - 231k *(classic knee)*
     - 26.6 ms
     - 901 MiB/s
   * - nj32 x qd64
     - 35.4k
     - 986 ms
     - 256k
     - 63.7 ms
     - 1.0 GiB/s
   * - nj48 x qd64
     - 38.1k
     - 1.15 s
     - 271k
     - 89.7 ms
     - 1.06 GiB/s
   * - nj64 x qd64
     - **CRASH** (omap)
     - \-
     - 274k
     - 179 ms
     - 1.07 GiB/s

Historical (unstable, capped-era) observations: transient pre-crash bursts up
to ~110k client IOPS were seen while OSDs were already dying - not a peak the
cluster can hold. **4 MiB sequential** could not be characterized under crimson
- OSDs aborted under sustained load.


Classic BlueStore baseline (same hardware)
==========================================

Classic ``ceph-osd`` + BlueStore was built from the **same 20.2.2 source**,
installed on the same 8 hosts, same 40 OSDs / size=5 / pg_num=1024. Its 4 KiB
sweep is the ``classic`` columns of the table above (knee at ``nj16`` = 231k).

* The single client is NOT the bottleneck for 4 KiB random write. The client stays **83-92% idle**
  (fio CPU ~5%) all the way to 274k, so the ~**274k plateau is the cluster's 4K
  write ceiling**, not the client. Knee ~``nj16`` (231k at p99 27 ms); pushing
  past it buys IOPS only by exploding p99 (179 ms at nj64).
* **Sustained (25-min soak):** ``nj8 x qd64`` held **183k IOPS for the full
  25 min** (p50 2.1 ms, p99 15.8 ms; only ~4% decay from 191k), 40/40 OSDs up,
  zero crashes, 1.05 TiB written. Sustained ~= burst - no BlueStore compaction
  cliff. This is the trustworthy soak number; an earlier 105k reading was
  taken before the cluster had fully settled (residual recovery) and is discarded.

Versus crimson (stable 5x4 uncapped config, matched concurrency): classic is
**~4.8x at nj8 sustained** (183k vs 38k, with p99 15.8 ms vs 296 ms) and
**~7x at the respective ceilings** (274k scaling vs ~30-38k flat). Classic
also survives nj64 (179 ms p99) where crimson aborts.


Recommendation
==============

* **Not production-ready for replicated write workloads** (matches upstream's
  own "tech preview, not for production").
* If testing continues: run thin OSDs (1 OSD/NVMe) **with NO ``--memory`` cap**.
  Size **reactors per OSD from the host budget, not a fixed number** - keep total
  reactors <= physical cores (never oversubscribe) and leave each reactor/shard
  ~10-12 GB of RAM (steady footprint ~5 GB, the hot shard ~2x), i.e.
  ``reactors_per_OSD ~ min(cores / OSDs, RAM / OSDs / ~10 GB)``. Uncapped OSDs
  self-limit at ~19.4 GB RSS (~5 GB/shard), so memory does not run away.
  Dedicated cores + dedicated NVMe per OSD, drive load from a remote client (add
  clients to characterize 4M bandwidth).
* Blockers requiring **upstream** fixes before any rollout:

  #. SeaStore OMap/extent ``assert_all`` -> make extent-load errors recoverable.
  #. Messenger ``FrameAssemblerV2`` late-abort ``TODO`` -> **FIXED**
     (classic-parity drop-and-continue; see Root cause above). Tracker
     https://tracker.ceph.com/issues/78119; upstream PR
     https://github.com/ceph/ceph/pull/70166
     (``vgoot/crimson/net/late-frame-abort``, builds clean on ``main``, crimson
     messenger unit tests pass).
  #. **Memory backpressure** -> throttle/stall ingest as a shard nears its
     ``--memory / cores`` slice, instead of the current brute-force
     ``std::bad_alloc`` abort. (Not a stability blocker on its own - run
     uncapped - but required before a ``--memory`` cap is usable.)
  #. **Reactor-stall escalation at high concurrency** (nj64: 272 ms -> 29 s
     stalls -> omap abort) and the **unassisted post-abort cascade** (recovery
     churn crash-loops healthy OSDs, incl. a SIGILL path in messenger teardown;
     only an operator freeze stops it) -> need load shedding + cluster-level
     fault containment.
  #. Crimson cold-boot cephx failure on aged clusters (a crashed OSD cannot
     rejoin).
  #. In-place OSD reprovision (purge + re-mkfs) aborted an OSD on boot when a
     peer was purged across an osdmap batch -> **FIXED** (the multi-OSD
     boot-crash fix this harness needed, upstream PR
     https://github.com/ceph/ceph/pull/69972; see
     :doc:`seastore-write-path-single-host-benchmark`), so an OSD can now be
     decommissioned and re-added in place.
  #. Ideally, per-shard fault isolation so one shard's abort does not kill the
     OSD.
