.. _dev_vstart_chaos:

=====================================
 Fault injection on a vstart cluster
=====================================

The scripts in ``src/script/vstart_chaos/`` create a :ref:`vstart
<dev_deploying_a_development_cluster>` cluster with one test pool, inject
random faults into it while client workloads and I/O probes run, and then
check that the cluster returns to a clean state. They are a developer tool
for exercising OSD and monitor changes under failures on one machine, not
part of ``make check`` or teuthology. By default the fault phase lasts up to
ten hours. A run destroys the vstart cluster in ``$CEPH_BUILD`` (see
`Environment`_), including its daemon logs in ``$CEPH_BUILD/out`` and the
``core*`` files in ``$CEPH_BUILD``; ``run_chaos.sh`` asks first unless
``-y`` is given.

The scripts
===========

======================  =======================================================
``run_chaos.sh``        Does one complete run. Start here.
``env.sh``              Sets ``CEPH_BUILD`` and the paths that go with it.
``setup_cluster.sh``    Creates the vstart cluster and the test pool.
``chaos.py``            Runs the workloads, injects faults, checks the cluster.
``make_snapshot.sh``    Copies the binaries of a build to a fixed directory.
``upgrade_cluster.sh``  Restarts the monitors and OSDs on such a snapshot.
``io_probe.py``         The I/O probe workload.
``rbd_roundtrip.py``    The rbd workload.
``rados_cleanup.py``    Cleans up after a finished ``ceph_test_rados`` client.
``radosc.py``           A ctypes librados wrapper for balanced/localized reads.
======================  =======================================================

``io_route.py`` and ``failover_probe.py`` are separate checks that
``run_chaos.sh`` does not run; source ``env.sh`` before running them by hand.

Requirements
============

* A build directory directly inside its source tree, such as
  ``<source>/build``, with what the ``vstart`` target builds (including the
  ``rados`` and ``rbd`` Python bindings), ``ceph_test_rados`` and
  ``ceph_test_rados_io_sequence``. A full build has them all; otherwise, in
  the build directory::

     ninja vstart ceph_test_rados ceph_test_rados_io_sequence

* Python 3.9 or later; ``jq``, ``patchelf`` and ``readelf`` for snapshots.
* Disk space: the sparse OSD devices and the logs can grow to about 40G
  over a long run at the default log levels, and more with ``-l high``.
  ``run_chaos.sh`` does not start with less than 20G available and warns
  below 40G; ``chaos.py`` stops a run below 10G free.
* One run per host: ``run_chaos.sh`` exits if a ``chaos.py`` is running, and
  the cluster always uses ``CEPH_PORT=30000``. Stop a cluster that a run left
  in another build directory before you start a run in a new one.

Environment
-----------

``CEPH_BUILD``
   The build directory that the cluster runs from. Default: ``build/`` in the
   main checkout of the scripts' git repository, even from a worktree.
``CHAOS_RUNS``
   Where runs write their output. Default: ``$CEPH_BUILD/chaos-runs``.
``CHAOS_SNAPS``
   Where ``-b`` puts snapshots. Default: ``$CEPH_BUILD/chaos-bins``.

``env.sh`` also sets ``PATH``, ``LD_LIBRARY_PATH``, ``PYTHONPATH``,
``CEPH_CONF`` and ``CEPH_KEYRING``. To use the cluster from another shell::

   export CEPH_BUILD=$PWD/build
   . src/script/vstart_chaos/env.sh
   ceph -s

Quick start
===========

From the top of the source tree, with the build in ``build/``::

   CEPH_BUILD=$PWD/build src/script/vstart_chaos/run_chaos.sh \
       -p erasure -k 2 -m 1 -t 7200

This runs the fault phase for up to two hours on an erasure coded 2+1 pool
with ``allow_ec_optimizations``, in one zone. ``run_chaos.sh``:

#. Checks the options. If there is a cluster in ``$CEPH_BUILD``, prints its
   ``ceph-mon --version`` and commit and asks before destroying it.
#. With ``-b``, makes the binary snapshot. Then checks the free disk space.
#. Creates the run directory, stops the old cluster and starts a new one
   with 3 monitors, 1 manager and 8 OSDs (``setup_cluster.sh``).
#. With ``-s`` or ``-b``, runs ``upgrade_cluster.sh``.
#. Runs ``chaos.py`` under ``timeout``, with its output in ``chaos.out``.
#. If ``chaos.py`` failed, saves evidence and archives the daemon logs.
#. Writes and prints ``SUMMARY``, and leaves the cluster running.

It exits with 0 when the requested cycles completed, 124 when the time limit
was reached without a failure, and with another non-zero status when the run
failed or could not start. Start a long run in ``tmux`` or ``screen``.

run_chaos.sh options
====================

``-h`` prints the usage text.

.. program:: run_chaos.sh

.. option:: -s DIR

   Run on the snapshot in *DIR*. Default: the binaries in ``$CEPH_BUILD``.

.. option:: -b DIR

   Snapshot the build directory *DIR* into a new directory under
   ``$CHAOS_SNAPS`` and run on it. Overrides ``-s``.

.. option:: -S SEED

   Seed for ``chaos.py``. Default: random.

.. option:: -t SECS

   Wall-clock limit for the fault phase. Default: 36000.

.. option:: -c N

   Number of cycles; 0 runs until a failure or the time limit. Default: 0.

.. option:: -n NAME

   Run directory name; must be new. Default: ``chaos-<MMDD-HHMM>-s<seed>``.

.. option:: -p TYPE

   Pool type: ``erasure`` (default) or ``replicated``.

.. option:: -z N

   Number of zones: 1 (default) or 2.

.. option:: -g

   With ``-z 2``, use global stretch mode even where ``--num_zones`` works.

.. option:: -k K, -m M

   EC profile *k* and *m*. Defaults: 2 and 1.

.. option:: -r N

   Replicated pool size per zone. Default: 3 with one zone, 2 with two zones.

.. option:: -L

   Leave ``allow_ec_optimizations`` off on a single-zone erasure coded pool.

.. option:: -l LEVELS

   Daemon log levels: ``low``, ``default`` or ``high``. A level such as
   ``1/20`` is a file level and a memory level: messages up to the first go
   to the log files in ``$CEPH_BUILD/out``, and messages up to the second
   are kept in memory and written to the log only when the daemon crashes.

   ``default``
      ``debug_osd`` and ``debug_mon`` 1/20 and ``debug_ms`` 0/5, with
      BlueStore, BlueFS, bdev and RocksDB at 1/10 or 1/5 on the OSDs. Other
      subsystems keep the levels that ``vstart.sh -d`` sets.
   ``low``
      As ``default``, but the OSDs' ``monc``, ``objecter``, ``mgrc``,
      ``reserver`` and ``objclass`` subsystems and the manager write only
      level 1 to the file.
   ``high``
      As ``default``, but ``debug_osd`` and ``debug_mon`` 20 and
      ``debug_ms`` 1 on the OSDs and monitors, which shows what stuck
      operations and peering did.

   High log levels take a lot of disk space. Creating the cluster logs up
   to about 1G. After that, the daemon logs of a two-zone erasure coded 2+1
   run grew by about 0.3G an hour with ``default``, 0.1G with ``low`` and
   65G with ``high``, almost all of it ``debug_osd`` 20 on the OSDs
   (``-D osd:osd=10`` roughly halves that). ``high`` also slows the OSDs
   down. Keep runs with ``high`` short with ``-c`` or ``-t``: ``chaos.py``
   stops a run below 10G free.

   The levels are set in the configuration database, so that restarted
   daemons keep them, and ``ceph config set osd debug_osd 20`` changes the
   OSDs' level during a run.

.. option:: -D LIST

   More log levels, set after those of ``-l``: comma-separated
   ``[TYPE:]SUBSYS=LEVEL`` entries, where *TYPE* is ``osd``, ``mon`` or
   ``mgr`` (default: all three) and *LEVEL* is a file level or
   ``file/memory``. For example, ``-D osd=10,ms=1`` sets ``debug_osd`` to
   10 and ``debug_ms`` to 1 on every daemon, and ``-D osd:bluestore=1/20``
   sets ``debug_bluestore`` on the OSDs only.

.. option:: -x

   Stop the cluster after the run. Default: leave it running.

.. option:: -y

   Do not ask before destroying the existing cluster.

Options for chaos.py
--------------------

Arguments after ``--`` go to ``chaos.py`` and override those that
``run_chaos.sh`` passes, which include ``--quiesce-every 30``,
``--clean-timeout 2400``, ``--revive-timeout 2400`` and
``--read-policies none,localize,balance``. For example::

   CEPH_BUILD=$PWD/build src/script/vstart_chaos/run_chaos.sh -c 20 -- \
       --read-policies none --workloads rados,ioseq,probe

.. program:: chaos.py

.. option:: --actions LIST

   Comma-separated ``action=weight`` pairs; leave an action out to disable
   it. The actions are ``mark_down``, ``out_in``, ``kill_restart``,
   ``upmap_items``, ``rm_upmap``, ``repeer``, ``deep_scrub``, ``reweight``,
   ``primary_affinity``, ``pg_num`` and, with two zones, ``upmap_zone``,
   ``upmap_flip``, ``zone_partial`` (erasure coded only) and
   ``zone_failover``.

.. option:: --workloads LIST

   Default: ``rados,ioseq,rbd,probe``. ``ioseqrec`` adds a
   ``ceph_test_rados_io_sequence --testrecovery`` client.

.. option:: --read-policies LIST

   From ``none`` (plain reads), ``balance`` and ``localize``.

.. option:: --zf-variants LIST

   From ``standard``, ``osds_first``, ``flap``, ``surviving_loss``,
   ``osds_only`` and ``mon_only``. ``run_chaos.sh`` leaves out ``osds_only``.

.. option:: --ignore-crash TEXT

   Treat crash lines that contain *TEXT* as known. Can be repeated.

With ``env.sh`` sourced, ``python3 src/script/vstart_chaos/chaos.py --help``
lists the rest.

.. warning::

   Do not give ``--pool``, ``--cycles``, ``--seed`` or ``--rundir`` after
   ``--``; the summary and the saved evidence use ``run_chaos.sh``'s values.

Pool layouts
============

``setup_cluster.sh`` creates the test pool ``chaos`` and a replicated pool
``rbd`` for the rbd workload's image metadata, which with two zones also
keeps 2 copies in each zone. With one zone, an erasure coded pool uses
``crush-failure-domain=osd`` and ``allow_ec_overwrites``, and
``allow_ec_optimizations`` unless ``-L`` is given. A single-zone replicated
pool has ``min_size`` one less than its size, but at least 1.

With ``-z 2``, ``osd.0`` to ``osd.3`` are in the CRUSH datacenter ``dc1`` and
``osd.4`` to ``osd.7`` in ``dc2``, with monitor ``a`` in ``dc1``, ``b`` in
``dc2`` and the arbiter ``c``. The pool spans both in stretch mode:

* Where the monitors in ``$CEPH_BUILD`` (whatever ``-s`` or ``-b`` say)
  support ``osd pool create --num_zones``, it is a ``--num_zones`` pool,
  erasure coded or replicated.
* Otherwise, or with ``-g``, it uses global stretch mode, which takes only
  replicated pools with 2 copies per zone.

At the time of writing, main does not have ``osd pool create --num_zones``,
so a two-zone run on main needs ``-p replicated``.

What a run does
===============

``chaos.py`` uses only the workloads and faults that apply to the pool. Its
failure budget is the number of OSDs that may fail in each zone: *m* for an
erasure coded pool, size minus ``min_size`` for a replicated pool in one
zone, and the zone size minus one for a replicated pool over two zones.

Workloads and probes
   ``ceph_test_rados``, ``ceph_test_rados_io_sequence``, an rbd workload and
   ``io_probe.py`` probes, per read policy (and zone) where that applies. Each
   starts again when it finishes and stops the run if it fails. The rbd
   workload and the probes check reads against an in-memory copy.
Faults
   Each cycle runs 1 to ``--max-actions`` weighted random actions. Faults that
   take one OSD down or out keep each zone within its failure budget.
Zone failures
   ``zone_failover`` fails a zone's OSDs, its monitor or both for
   ``--hold-cycles`` cycles while the workloads go on, then revives them.
Checks
   Each cycle stops the run on a crash line in a daemon log, a dead OSD or
   monitor that ``chaos.py`` did not kill, a failed workload, a probe operation
   stuck past ``--stuck-limit``, low disk space or a full OSD.
Quiesce
   Every ``--quiesce-every`` cycles and at the end, ``chaos.py`` starts the
   daemons that it stopped, waits for the PGs of every pool to be
   ``active+clean`` and not remapped, deep scrubs every PG and stops the run on
   an inconsistent PG or a deep scrub that does not finish.

Binary snapshots and upgrades
=============================

By default the daemons and workloads use the binaries in ``$CEPH_BUILD``, so
rebuilding that tree during a run changes the daemons that the run restarts. To
go on building, or to run another build, use ``-b`` or ``-s``: the cluster is
created from ``$CEPH_BUILD``, then ``upgrade_cluster.sh`` restarts the monitors
and then the OSDs one at a time on the snapshot. The manager, the ``ceph``
command and the Python bindings stay on ``$CEPH_BUILD``. Do not change a
snapshot that running processes use; make a new one.

Reading the results
===================

Each run writes to ``$CHAOS_RUNS/<name>``::

   run.log             what run_chaos.sh printed after creating this directory
   setup.log           output of setup_cluster.sh
   upgrade.log         output of upgrade_cluster.sh (with -s or -b)
   chaos.out           output of chaos.py
   SUMMARY             the summary
   chaos/timeline.log  one line per action and zone failure step
   chaos/findings.log  findings, if any
   chaos/diag-c<cycle>-finding/, chaos/diag-c<cycle>-fatal/
                       cluster state at a finding or at the failure
   fail-evidence/      after a failure: dumps and pg query of unclean PGs
   dup-evidence/       when a clean PG has the same OSD twice in its acting set
   daemon-logs.tar.gz  in either case: the OSD, monitor and manager logs

``SUMMARY`` shows the binaries and their commit, the pool, the seed, the
duration, the result, the findings, ``ceph crash ls``, assert and signal lines
from the daemon logs, and ``ceph health``. The result is
``PASS: completed <N> cycles``, ``PASS: time limit reached, no failure`` or
``STOPPED rc=<rc>: <reason>``, where the reason is the first ``FATAL`` line of
``chaos.out``. Its second word names the failed check, such as ``CRASH``,
``DEAD``, ``STUCK_OP`` or ``INCONSISTENT``; ``ENV_LOW_DISK`` and
``ENV_OSD_FULL`` are problems of the test environment. Findings did not stop
the run but may need a look.

To look into a failure, start with ``diag-c<cycle>-fatal`` and the lines of
``timeline.log`` before that cycle. The cluster is still running unless
``-x`` was given, and its daemon logs are in ``$CEPH_BUILD/out``.

``-S <seed>`` with the same options seeds the random choices of ``chaos.py``
the same way. They also depend on the cluster's state and timing, and
``ceph_test_rados`` and the probes get no seed, so a rerun is not exact.

Stopping and cleaning up
========================

To end a run early, send SIGTERM to ``chaos.py``
(``pgrep -f '^python3 chaos.py'`` finds it). It then starts the stopped
daemons, waits for the workloads and quiesces the cluster before it exits.
Ctrl-C in the terminal of ``run_chaos.sh`` does not reach ``chaos.py``.

The next run replaces the cluster that a run leaves running. To stop it, run
``../src/stop.sh`` from ``$CEPH_BUILD``, but note that it stops every Ceph
daemon of your user on the host, not only this cluster's. Remove old run
directories and snapshots from ``$CHAOS_RUNS`` and ``$CHAOS_SNAPS`` by hand.

Known issues on main
====================

At the time of writing, runs on main may stop on the first two of these;
the scripts work around the other two:

* https://tracker.ceph.com/issues/81194: balanced direct EC reads do not
  complete after PG changes. ``-- --read-policies none,localize`` avoids it.
* https://tracker.ceph.com/issues/81367: clients read stale omap from
  replicas that were down during a ``copy_from``.
* https://tracker.ceph.com/issues/81136: ``ceph mon enable_stretch_mode``
  can reply with an error although it worked.
* https://tracker.ceph.com/issues/81389: sequence 16 of
  ``ceph_test_rados_io_sequence`` asserts on replicated pools.
