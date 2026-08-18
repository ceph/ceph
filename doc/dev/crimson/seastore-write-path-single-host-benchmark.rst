======================================================================
SeaStore write-path benchmark: single-host OSD / core scaling
======================================================================

:Date: 2026-07 (point-in-time snapshot of the crimson/SeaStore build below;
       absolute numbers track this build and test bed, not a spec)
:Hardware: AMD EPYC 7643, 48 physical cores / 96 threads, single NUMA,
           5x 1.7 TB Micron NVMe, 251 GB RAM
:Ceph: 20.2.2, crimson OSD + SeaStore (``WITH_CRIMSON=ON, SPDK=OFF``),
       RelWithDebInfo + IPO/LTO
:Pools: ``size=1`` (single host)

.. contents::
   :local:
   :depth: 1


Summary
=======

On a single crimson node, 4 KiB random-write IOPS scales along two axes:

* **Number of cores (reactors) per OSD.** Within one OSD, adding reactors helps
  only up to a knee around **16-32 reactors**, then flattens; past the knee more
  reactors do not help, and past the 48 physical cores throughput drops (looks
  like SMT siblings contending).
* **Number of OSDs.** A single OSD tops out around **~9-10k** 4K write IOPS, so
  throughput scales by adding OSDs, not reactors: at a fixed core / RAM budget,
  splitting the same cores into more OSDs multiplies IOPS.

**Recommendation: one OSD per NVMe (dedicated), ~9 reactors each.** On the
5-NVMe nodes: 5 OSDs x ~9 reactors (45 reactors <= 48 physical, ~3 reserved for
mon/system).

Results
=======

Reactor width of a single OSD
-----------------------------
Sweep cpuset width (4 / 8 / 16 / 32 / 64 reactors) on one OSD. IOPS climbs then
flattens by a knee around **16-32 reactors**; 64 reactors oversubscribes the 48
physical cores and throughput drops. Rule: reactors/node <= 48, fill physical
cores before SMT. 

OSD count at a fixed budget
---------------------------
Hold the budget constant (44 reactors, 132 GB RAM) and vary only the OSD split.
4 KiB random write, load from a remote client:

.. list-table::
   :header-rows: 1

   * - OSDs
     - reactors/OSD
     - 4K IOPS
   * - 4
     - 11
     - **~47-53k**
   * - 2
     - 22
     - ~31k
   * - 1
     - 44
     - ~9k

Only the many-thin-OSD layout has concurrency headroom: 4 OSDs keeps scaling
with client concurrency (to ~53k), while a single fat OSD peaks early and then
*degrades* under more concurrency. 


Boot-crash fix (submitted upstream)
===================================

This benchmark required it: an OSD's pinned reactor cores
(``crimson_seastar_cpu_cores``) are fixed at ``mkfs`` and cannot be changed
afterwards, so every layout in the sweep meant **purge + reprovision** of the
OSDs. That purge hit a boot crash that blocked the harness; the fix below
resolved it.

``OSD::committed_osd_maps()`` marked a peer's cluster address down using the
**new** osdmap (``osdmap->get_cluster_addrs(osd_id)``) while ``osd_id`` came
from ``old_map->get_all_osds()``. If that peer was purged in the new map,
``get_cluster_addrs`` hits ``ceph_assert(exists())`` and the OSD aborts on
boot - hit on OSD decommission / reprovision on multi-OSD hosts. Fix: look the
address up in ``old_map`` (guaranteed to contain the id and its last-known
address).

Committed as ``b52f75e720c`` (``src/crimson/osd/osd.cc``); submitted upstream:
https://github.com/ceph/ceph/pull/69972.


Operational notes
=================

* **Run OSDs as root; log to a writable path outside ``/usr``** via
  ``ReadWritePaths=<your OSD log directory>`` on the ``ceph-osd@`` unit -
  ``ProtectSystem=full`` otherwise remounts ``/usr`` read-only and crimson-osd
  aborts on log open.
* **Build crimson debs with ``WITH_CRIMSON=1``** (the env-file default silently
  builds the classic OSD); targets must be Ubuntu 24.04 (noble).
* **Drive load from a remote client** - A single client
  saturates on 4 MiB (bandwidth) load, so characterizing 4M throughput needs
  more client machines; one client is sufficient for the 4K IOPS measurements
  here.

