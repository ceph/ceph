.. _cephfs-mdsbench:

==============
cephfs-mdsbench
==============

``cephfs-mdsbench`` is a client-side MDS **metadata** stress and benchmark tool.
It drives tunable lookup/stat, open/close, create/unlink, setattr, mkdir/rmdir,
readdir, and small-write pressure against CephFS through ``libcephfs``.

It is intended for:

* Reproducing metadata-heavy load that stresses MDS journal, PurgeQueue, capability
  flush, and Objecter throttle paths (regression for journaler / balanced-budget
  stalls and Init ``CLIENT_CAPS`` storms).
* Standalone MDS metadata performance measurement (ops/s and latency percentiles).

Default mix and tree scale target typical **software-build style** metadata load
(stat-heavy Run mix; nested Dir/bucket trees), calibrated against observed client
rates and MDS ``perf_dump`` counters. For data-plane I/O benchmarking, use
:ref:`cephfs-tool <cephfs-tool>` instead.

Key features
============

* Fixed nested tree per client:
  ``workdir/CL{id}_MDSBENCH/Dir*/bucket*/f*``
  (~1300 dirs / ~130k files per client by default).
* Weighted Run-phase op mixes: ``balanced``, ``journal-heavy``, ``lookup-heavy``,
  or a custom ``--mix``.
* Target aggregate rate limiting (``--ops-per-sec``).
* Single-process multi-client threads, or multi-process launches with
  ``--client-id`` and a CephFS ready-file start barrier.
* Init populate path (create / optional write-fill / close) for ``CLIENT_CAPS``
  storm reproduction (``--cap-storm``, ``--prep-only``).
* Progress reporting and optional JSON summary.

Building
========

When building Ceph normally, ``cephfs-mdsbench`` is produced with the other
CephFS tools (``ninja cephfs-mdsbench`` in the build directory).

Standalone sketch (against an installed ``libcephfs``)::

    g++ --std=c++20 -D_FILE_OFFSET_BITS=64 -O3 -o cephfs-mdsbench \
        cephfs-mdsbench.cc -lcephfs -lpthread -lboost_program_options

Usage
=====

::

    cephfs-mdsbench [options]

Each workload client has a numeric id and only performs I/O under::

    <workdir>/CL{id}_MDSBENCH/...

* **Single process:** ``--clients N`` creates ``CL0``…``CL{N-1}`` and one worker
  thread per id (shared precreate, then all start the measured mix together).
* **Multi-process:** each process uses ``--client-id K --clients N`` and only
  seeds/uses ``CL{K}_MDSBENCH``. With ``--client-id``, a start barrier under
  ``workdir/.mdsbench_barrier/ready.{id}`` is enabled by default so peers finish
  Init before any process enters the measured phase (disable with ``--no-barrier``).

Tree geometry
=============

Tree size is **not** derived from ``--clients`` or ``--ops-per-sec``. Defaults
use a fixed nested Init layout:

==============  =======  =====================================
Parameter       Default  Meaning
==============  =======  =====================================
top dirs        50       ``Dir0`` … ``Dir49``
buckets/dir     26       ``bucket0`` … ``bucket25``
files/bucket    100      ``f0`` … ``f99``
==============  =======  =====================================

That yields about **1300 directories** and **130000 files** per client root.
``--ops-per-sec`` only throttles the measured Run mix; ``--clients`` only
controls how many independent ``CL*_MDSBENCH`` trees (and workers) you run.

Hidden overrides (not shown in ``--help``, still accepted)::

    --top-dirs --buckets-per-dir --files-per-bucket

Options
=======

General / cluster
-----------------

.. option:: -h, --help

   Show help

.. option:: -c, --conf <path>

   Ceph config file

.. option:: -i, --id <id>

   CephX client id (default: ``admin``)

.. option:: -k, --keyring <path>

   Keyring path

.. option:: --filesystem, --fs <name>

   CephFS name (``client_mds_namespace``)

.. option:: --root-path <path>

   Mount root inside the filesystem (default: ``/``)

.. option:: --uid <uid>, --gid <gid>

   Mount credentials (default: ``-1`` = unset)

.. option:: --workdir <path>

   Work directory in CephFS (default: ``/mdsbench``)

Workload / Run mix
------------------

.. option:: --mode <balanced|journal-heavy|lookup-heavy>

   Preset op mix (default: ``balanced``). See :ref:`mdsbench-modes`.

.. option:: --mix <stat=W,open_close=W,...>

   Custom relative weights (sets mode to ``custom``)

.. option:: --clients <n>

   Number of client ids / ``CL{id}_MDSBENCH`` roots (default: ``24``)

.. option:: --client-id <k>

   If ``>= 0``, this process only uses ``CL{k}_MDSBENCH`` (multi-process)

.. option:: --ops-per-sec <rate>

   Target aggregate op rate for the measured phase; ``0`` = unlimited
   (default: ``12000``)

.. option:: --duration <sec>

   Measured duration in seconds (default: ``60``)

.. option:: --warmup <sec>

   Warmup before measurement (default: ``5``)

.. option:: --seed <n>

   RNG seed (default: ``1``)

.. option:: --write-bytes <n>

   Size of ``write_small`` Run ops (default: ``4096``)

.. option:: --scratch-slots <n>

   Transient name slots per client for create/unlink/mkdir/rmdir (default: ``64``)

Init / precreate
----------------

Init (tree populate) is separate from the measured Run mix. Large
``CLIENT_CAPS`` floods are typically produced during Init create/write-fill/close,
not during Run STATs.

.. option:: --init-file-bytes <n>

   Bytes to write-fill during Init (default: ``16384``; ``0`` = create-only)

.. option:: --init-fill-percent <0-100>

   Percent of files to write-fill (default: ``46``)

.. option:: --init-threads <n>

   Parallel create/fill workers in this process during Init (default: ``1``)

.. option:: --prep-only

   Only run Init (and barrier ready signal if enabled); do not run the mix

.. option:: --skip-precreate

   Skip Init; use an existing tree

.. option:: --cap-storm

   Preset for Init ``CLIENT_CAPS`` storm reproduction: implies ``--prep-only``,
   keeps the tree, defaults to 16KiB fill / 46% fill / 8 init threads, and
   enables progress

Coordination / output
---------------------

.. option:: --barrier / --no-barrier

   Force or disable the multi-process ready-file barrier. With ``--client-id``,
   barrier is on unless ``--no-barrier`` is set. Each client clears its own
   ``ready.{id}`` at start to avoid stale signals from crashed runs.

.. option:: --barrier-timeout <sec>

   Barrier wait timeout; ``0`` = forever (default)

.. option:: --progress / --progress-interval <pct>

   Live progress (percent, IOPS/creates-per-sec, ETA). Interval is percent
   between updates (default: ``10``)

.. option:: --json <path|->

   Write JSON summary (``-`` = stdout)

.. option:: --no-cleanup / --keep-tree

   Leave the workdir tree in place after the run

.. _mdsbench-modes:

Op mix modes
============

Weights are relative and normalized at runtime.

**balanced** (default)
  Software-build style mix: mostly ``stat``, then ``open_close``,
  ``write_small``, ``access``, ``chmod``, with lighter
  create/unlink/readdir/mkdir.

**journal-heavy**
  Shifts toward mutating ops that show up as MDS ``req_create`` /
  ``req_unlink`` / ``req_setattr`` / mkdir/rmdir and drive journal ``evadd``
  and PurgeQueue activity.

**lookup-heavy**
  Emphasizes ``stat`` / ``access`` / ``readdir`` (MDS ``req_lookup``-heavy
  wire traffic).

**custom**
  Via ``--mix``, e.g. ``stat=70,open_close=10,create=5,unlink=5,...``.

Outputs
=======

Console reports elapsed time, aggregate ops/s, errors, and per-op count with
mean / p50 / p99 latency (µs).

``--json`` includes configuration (clients, mix, init fill settings, tree
counts), summary rates, and per-op percentiles.

Examples
========

Balanced metadata Run (single process)
--------------------------------------

Populate all client trees in-process, then run a rate-limited balanced mix::

    cephfs-mdsbench -c /etc/ceph/ceph.conf \
      --clients 24 --ops-per-sec 12000 --duration 120 \
      --mode balanced --progress --json /tmp/mdsbench-balanced.json

Journal / PurgeQueue pressure
-----------------------------

Favor create/unlink/setattr-style ops after Init::

    cephfs-mdsbench -c /etc/ceph/ceph.conf \
      --clients 24 --mode journal-heavy --ops-per-sec 8000 \
      --duration 180 --progress --json /tmp/mdsbench-journal.json

Useful when validating MDS journaler unlock/defer across Objecter throttle,
or PurgeQueue behavior under stray/unlink load. Correlate with MDS
``perf_dump`` deltas for ``mds_log.evadd``, ``purge_queue``, and
``throttle-objecter_ops``.

Lookup-heavy Run
----------------

::

    cephfs-mdsbench -c /etc/ceph/ceph.conf \
      --clients 24 --mode lookup-heavy --ops-per-sec 12000 \
      --duration 120 --progress

Expect MDS-side traffic dominated by ``req_lookup`` (and related getattr paths).

Multi-process Run with start barrier
------------------------------------

Launch one process per client id so each owns ``CL{i}_MDSBENCH``. Init finishes
on all peers before any measured phase starts::

    # on each launcher, unique i in 0..23
    cephfs-mdsbench -c /etc/ceph/ceph.conf \
      --clients 24 --client-id $i \
      --ops-per-sec 500 --duration 120 --mode balanced --progress

Per-process ``--ops-per-sec`` is that process's share; sum across processes for
cluster-offered rate.

Init ``CLIENT_CAPS`` storm
--------------------------

Large Client-lane / ``CLIENT_CAPS`` floods are typically driven by **Init**
create/write-fill/close (dirty cap flushes), not by the measured Run mix.
Drive that producer path by launching many processes together::

    cephfs-mdsbench -c /etc/ceph/ceph.conf \
      --cap-storm --clients 24 --client-id $i --progress

Equivalent explicit knobs::

    cephfs-mdsbench -c /etc/ceph/ceph.conf \
      --prep-only --clients 24 --client-id $i \
      --init-file-bytes 16384 --init-fill-percent 46 --init-threads 8 \
      --progress --keep-tree

While Init runs, watch MDS for rising ``handle_client_caps`` / reactor Client
lane depth (or ``mds_server`` / session cap counters).

.. note::

   Cap-flush storms seen on **kernel** CephFS clients may differ in rate from
   ``libcephfs``. Prefer many concurrent ``--client-id`` processes (sessions)
   over a single process when aiming for storm scale.

Staged Init then Run
--------------------

Prep everywhere, then a second wave that skips seeding and barriers into the
mix::

    # wave 1 — seed only
    cephfs-mdsbench -c /etc/ceph/ceph.conf \
      --prep-only --clients 24 --client-id $i --keep-tree --progress

    # wave 2 — measured mix on existing trees
    cephfs-mdsbench -c /etc/ceph/ceph.conf \
      --skip-precreate --clients 24 --client-id $i \
      --ops-per-sec 500 --duration 120 --mode journal-heavy --progress

What to measure on the MDS
==========================

Pair client JSON with MDS ``perf_dump`` (or asok) deltas over the same window:

* **Run mix:** ``mds_server.req_*`` (especially ``req_lookup``, ``req_create``,
  ``req_unlink``, ``req_setattr``), ``mds_log.evadd``, ``purge_queue``,
  ``objecter`` write ops, ``throttle-objecter_ops`` waits.
* **Init CAPS storm:** ``handle_client_caps`` / Client-lane queue depth,
  journal events dominated by cap updates (``EUpdate`` "cap update"), not
  Run-phase ``req_lookup`` spikes.

Limitations
===========

* Op selection and path naming are intentionally simplified.
* libcephfs only (no kernel ``--mount`` path yet).
* ``--ops-per-sec`` applies to the measured Run phase, not Init populate.
* Multi-process barrier uses marker files under the CephFS workdir; clear
  ``.mdsbench_barrier`` if a crashed run leaves the directory inconsistent
  (each client also unlinks its own ``ready.{id}`` at start).

See also
========

* :doc:`cephfs-tool` — libcephfs data-plane I/O benchmark
* :doc:`capabilities` — CephFS capability model
* :doc:`mds-journaling` — MDS journaling overview
* :doc:`purge-queue` — stray/purge background work
