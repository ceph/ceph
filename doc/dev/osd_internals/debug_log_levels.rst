==========================
OSD Debug Log Level Policy
==========================

Overview
--------

A classic OSD running at ``debug_osd = 20`` (with BlueStore and BlueFS at
20) under a teuthology workload writes 40-100 MB of log per second; one
OSD produced 219 GB in 65 minutes.  Logs of that size are expensive to
store and slow to search, so jobs are run at lower debug levels, which then
lack the information needed to debug a failure.

The policy below makes **debug level 10 a lean level** that is cheap enough
to leave enabled everywhere, while **level 20 stays the full trace**.  The
tools described at the end of this document measure the cost of each level
and catch regressions automatically.

See :doc:`/rados/troubleshooting/log-and-debug` for how debug levels are
configured.

Level meanings
--------------

Each ``dout()``/``ldout()``/``ldpp_dout()``/``psdout()`` statement has a
level.  A daemon writes a statement when the level is less than or equal to
the configured log level of its subsystem (``debug_osd``, ``debug_bluestore``
and so on).

``-1`` (``derr``, ``lderr``)
  Errors.  Always written.

``0``
  Unusual events that an administrator must see, for example an OSD being
  wrongly marked down.  Cluster-visible events belong in the cluster log
  (``clog``) as well.

``1``
  Rare, significant events: daemon start/stop, configuration changes.  The
  messenger logs one line per message sent and received at ``debug_ms = 1``.

``5``
  Infrequent state changes: peering state machine transitions, interval
  changes, start and finish of recovery, backfill and scrub.

``10``
  **The lean level.**  Everything needed to reconstruct what happened when
  something goes wrong, and nothing else:

  * at most one or two lines per IO per layer on the good path (for example
    one line when an op is dequeued, one when an EC write is started and one
    when it commits);
  * every state change: peering state transitions, interval changes,
    recovery/backfill/scrub start and finish, and map changes that actually
    change something;
  * error, unusual and slow paths **in detail**: retries, resends, EIO,
    missing objects, blocked ops, unexpected messages.

``15``
  A step-by-step trace of the good path: the individual steps of an op in
  each layer.

``20``
  The full trace: no-op decisions ("nothing to do", "not scheduling",
  "empty queue"), data structure dumps, per-extent and per-shard detail.

Levels above 20 (``25``, ``30``) exist in some places for exceptionally
voluminous output.  Do not add new ones: everything must be visible at 20.

Debug level 20 output must otherwise be unchanged by a re-levelling pass:
do not move a statement that used to be at 20 or below to above 20, and do
not delete one.  A statement that used to sit above 20 may still be pulled
down into the lean level when it turns out to be a genuine error path and
not exceptionally voluminous; note the change explicitly in the commit
message when you do this, since it does add a line to the level 20 output
that was not there before.  For example, the ``-EIO`` ("stat_error")
branch of ``PGBackend::be_scan_list()`` moved from ``25`` to ``10``: it is
a real, rare error (a failed stat/getattrs on an object during scrub) and
belongs at the lean level per the rule above.  The sibling ``-ENOENT``
branch in the same function is a benign race with a concurrent delete and
was left at ``25`` rather than promoted, to keep level 20 output stable for
a non-error case.

Writing a level 10 line
-----------------------

Engineers read logs with ``grep`` and ``lnav``, one line at a time, often
after filtering by PG, object or request.  Every line at level 10 or below
must therefore be self-contained:

* Put the identifiers on the same line: the pgid (a PG dout prefix already
  provides it), the object (``hobject_t``), the request (``reqid`` and/or
  ``tid``), the version and the shard.
* One entry is one line.  Never use ``std::endl`` or ``'\n'`` inside a log
  statement, and print containers on a single line.
* Prefer enriching an existing level 10 line with the fields you need over
  adding a new one.  If you add a summary line, add one per op per layer
  and move the per-step lines it replaces to 15 or 20.
* Anything computed only for the log line must be computed inside the
  ``dout`` statement (or a ``dout`` block), so that it costs nothing when the
  level is disabled.
* Do not demote an error-path line just because it is at 10 in a hot
  function: errors belong at 10 or below.

Budgets
-------

The budgets are defined relative to what the same workload costs at level
20, which the tools below can compute from any level 20 log:

* **Bytes at level <= 10 are at most 20% of the bytes at level 20** for the
  same workload (an 80% reduction).
* **Bytes at level <= 10 per client op** (summed over all OSDs and divided
  by the number of client ops dequeued by the primaries): at most 20 KiB.
  At level 20 a client write currently costs 65-110 KB of OSD log, EC more
  than replicated.
* **Every kept (level <= 10) entry is a single line** (see above): measured
  as ``kept.multiline_entries``, and off by default as a budget
  (``max_kept_multiline_entries``) until it has been checked against real
  logs.

For reference, rados teuthology logs from mid 2026 spend 39-52% of their
level 20 bytes on level <= 10 lines, 31-53 KB per client op.

The ``qa`` task and the unit test described below start with warnings or
generous limits and are tightened as the code approaches the budgets.

Tools
-----

``src/script/ceph_log_budget.py``
  Analyses one or more daemon logs (plain or ``.gz``): bytes and lines by
  level and component, the bytes that would be written at a lower level
  (``--level``, default 10), the top message templates overall and among
  the kept lines, client ops and kept bytes per client op, and optionally
  the source lines the top templates come from (``--source-root``).
  Budgets can be given on the command line; the exit status is 1 if one is
  exceeded.  For example::

    src/script/ceph_log_budget.py --level 10 --source-root . \
        --max-kept-bytes-per-op 20480 --max-kept-fraction 0.2 \
        remote/*/log/ceph-osd.3.log.gz

  ``--json`` produces a machine readable report.  Large logs are analysed
  quickly by computing templates on a sample of the lines (``--sample
  auto``); totals are always exact.

``qa/tasks/log_budget.py``
  A teuthology task that runs the analysis on every OSD log of the job at
  teardown, before the logs are compressed, writes ``log_budget.json`` to
  the job archive and warns (``mode: warn``, the default) or fails the job
  (``mode: fail``) when a budget is exceeded, naming the top level 10
  templates and the source lines they most likely come from.  List it
  after the ``ceph`` task and before the workload; see
  ``qa/debug/log_budget.yaml`` for an example.

``unittest_log_budget`` (``src/test/osd/TestLogBudget.cc``)
  A ``make check`` test that drives creates, partial overwrites and reads
  through the in-process EC and replicated backend fixtures at
  ``debug_osd = 20`` and at ``debug_osd = 10``, captures the log entries and
  checks the level 10 entries and bytes per op, the level 10 / level 20 byte
  ratio and the level 10 output of an OSD failure and recovery cycle.  It
  prints ``LOG_BUDGET`` lines with the measured values, and is report-only
  (it never fails) until its budgets have been seen against a real run and
  tightened; set ``CEPH_LOG_BUDGET_ENFORCE=1`` to fail on a budget that is
  exceeded.  It does not cover ``OSD::dequeue_op``, ``PrimaryLogPG`` or
  BlueStore.

``src/script/dout_hotpath_lint.py`` (ctest ``dout_hotpath_lint``)
  A source-level ratchet.  ``src/script/dout_hotpath_baseline.json`` lists
  hot-path functions (op dispatch, ``PrimaryLogPG``, the EC and replicated
  backends, pg log, BlueStore transaction and BlueFS flush paths, messenger)
  with the number of debug statements at level 10 or below in each.  The
  test fails if a function gains one.  If the new line is a genuine error,
  unusual or blocked/bounced path (dropped or discarded message, caps
  rejection, an op requeued because the PG is not yet peered/active or a
  map is not yet available, an EAGAIN bounce, ...), mark it with a
  ``// dout-lint: error-path`` comment on the same or the preceding line.
  A ``ceph::dout::need_dynamic(cond ? a : b)`` level is
  classified by ``min(a, b)``; a level that cannot be resolved that way (a
  bare variable, or a call to a helper) is reported but does not fail the
  ratchet.  If a function loses lines, run::

    src/script/dout_hotpath_lint.py --update

  and commit the lowered baseline.  If a function is renamed or moved, it
  is reported as a note (not a failure, so an unrelated refactor does not
  fail make check); update its entry in the baseline file, or pass
  ``--strict`` in a dedicated, non-gating job to make a missing function
  fatal.
