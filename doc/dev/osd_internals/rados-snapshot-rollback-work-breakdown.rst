RADOS Snapshot Rollback -- Work Breakdown
==========================================

Design Document Version 7 - S/M/L sizing - commit-sized sub-tasks ~75 LOC
each

**Legend:** - **S** Small -- 1--3 commits - **M** Medium -- 4--7 commits,
2--3 subsystems - **L** Large -- 8+ commits, cross-cutting

--------------

Core OSD / MON Infrastructure (Sec.3--Sec.9)
---------------------------------------------

WI-1 - Core Data Structures ``[S]``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Add ``rollback_snap_info_t``, extend ``pg_pool_t``, ``pg_info_t``,
``pg_stat_t``, ``PG`` with new fields; versioned encoding (Sec.3).

**Files:** ``src/osd/osd_types.h`` - ``osd_types.cc`` - ``PG.h`` -
``PG.cc``

+--------+-----------------------+--------------------+-----------------+
| #      | Commit                | Files              | ~LOC            |
+========+=======================+====================+=================+
| WI-1-a | Define                | ``osd_types.h``,   | ~55             |
|        | ``rollback_snap_      | ``.cc``            |                 |
|        | info_t``              |                    |                 |
|        | with encode/decode.   |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-1-b | Add                   | ``osd_types.h``,   | ~70             |
|        | ``rollback_snaps`` to | ``.cc``            |                 |
|        | ``pg_pool_t``; bump   |                    |                 |
|        | encode version.       |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-1-c | Add                   | ``osd_types.h``,   | ~75             |
|        | ``completed_          | ``.cc``            |                 |
|        | rollbacks``           |                    |                 |
|        | to ``pg_info_t`` and  |                    |                 |
|        | ``pg_stat_t``.        |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-1-d | Add                   | ``PG.h``,          | ~30             |
|        | ``rollback_trimq`` to | ``PG.cc``          |                 |
|        | ``PG``; initialise.   |                    |                 |
+--------+-----------------------+--------------------+-----------------+

--------------

WI-2 - OSDMap Extensions ``[M]``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Add ``rollback_snaps_queue``, incremental fields,
``apply_incremental()`` merge; OSD compat guard (Sec.3.2).

**Files:** ``src/osd/OSDMap.h`` - ``OSDMap.cc``

+--------+-----------------------+--------------------+-----------------+
| #      | Commit                | Files              | ~LOC            |
+========+=======================+====================+=================+
| WI-2-a | Declare new queue and | ``OSDMap.h``       | ~40             |
|        | incremental fields.   |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-2-b | Encode/decode         | ``OSDMap.cc``      | ~70             |
|        | ``rollback_snaps_     |                    |                 |
|        | queue``               |                    |                 |
|        | under feature bit.    |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-2-c | Encode/decode         | ``OSDMap.cc``      | ~65             |
|        | incremental rollback  |                    |                 |
|        | fields.               |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-2-d | ``apply_              | ``OSDMap.cc``      | ~55             |
|        | incremental()``:      |                    |                 |
|        | insert/erase          |                    |                 |
|        | rollbacks.            |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-2-e | *Not required.*       |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-2-f | Add                   | ``osd_types.h``,   | ~45             |
|        | ``completed_          | ``.cc``,           |                 |
|        | rollbacks_last``      | ``OSD.cc``         |                 |
|        | to ``OSDSuperblock``; |                    |                 |
|        | bump encode version;  |                    |                 |
|        | add                   |                    |                 |
|        | ``reset_completed_    |                    |                 |
|        | rollbacks_last``      |                    |                 |
|        | OSD admin-socket      |                    |                 |
|        | command (mirrors      |                    |                 |
|        | ``reset_purged_       |                    |                 |
|        | snaps_last``).        |                    |                 |
+--------+-----------------------+--------------------+-----------------+

--------------

WI-3 - Pool Op Codes and MON Handling ``[M]``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

New opcodes; ``preprocess_pool_op()`` with Umbrella gate;
``prepare_pool_op()``; ``try_prune_completed_rollbacks()`` with
production guards; ``encode_pending()`` persistence;
``preprocess_get_completed_rollbacks()`` handler (Sec.4).

**Files:** ``src/include/ceph_fs.h`` - ``src/mon/OSDMonitor.h`` -
``OSDMonitor.cc``

+--------+-----------------------+--------------------+-----------------+
| #      | Commit                | Files              | ~LOC            |
+========+=======================+====================+=================+
| WI-3-a | Add opcodes 0x31 and  | ``ceph_fs.h``      | ~15             |
|        | 0x32.                 |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-3-b | ``preprocess``        | ``OSDMonitor.cc``  | ~50             |
|        | ``ROLLBACK_SNAP``:    |                    |                 |
|        | Umbrella guard, mode  |                    |                 |
|        | check, snap-exists,   |                    |                 |
|        | idempotency.          |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-3-c | ``preprocess``        | ``OSDMonitor.cc``  | ~55             |
|        | ``ROLLBACK_UNMANAGED_ |                    |                 |
|        | SNAP``:               |                    |                 |
|        | same + snap-seq bound |                    |                 |
|        | + removed_snaps       |                    |                 |
|        | check.                |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-3-d | ``prepare``: allocate | ``OSDMonitor.cc``  | ~75             |
|        | rollback ID; record   |                    |                 |
|        | in pool and           |                    |                 |
|        | pending_inc; encode   |                    |                 |
|        | reply.                |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-3-e | Implement full        | ``OSDMonitor.h``,  | ~90             |
|        | ``try_prune_          | ``.cc``            |                 |
|        | completed_            |                    |                 |
|        | rollbacks()``         |                    |                 |
|        | with readability      |                    |                 |
|        | guard, idempotency    |                    |                 |
|        | guard (bail if        |                    |                 |
|        | ``pending_inc.        |                    |                 |
|        | new_completed_        |                    |                 |
|        | rollbacks``           |                    |                 |
|        | non-empty), per-epoch |                    |                 |
|        | cap via               |                    |                 |
|        | ``mon_max_snap_       |                    |                 |
|        | prune_per_epoch``,    |                    |                 |
|        | and already-pruned    |                    |                 |
|        | skip; call from       |                    |                 |
|        | ``tick()``.           |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-3-f | Add                   | ``OSDMonitor.cc``  | ~40             |
|        | ``encode_pending()``  |                    |                 |
|        | persistence block:    |                    |                 |
|        | write                 |                    |                 |
|        | ``new_completed_      |                    |                 |
|        | rollbacks``           |                    |                 |
|        | to                    |                    |                 |
|        | ``OSD_SNAP_PREFIX``   |                    |                 |
|        | under                 |                    |                 |
|        | ``completed_rollback_ |                    |                 |
|        | epoch_<hex>``         |                    |                 |
|        | key on each Paxos     |                    |                 |
|        | commit; implement     |                    |                 |
|        | ``make_completed_     |                    |                 |
|        | rollback_epoch_       |                    |                 |
|        | key()``               |                    |                 |
|        | helper.               |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-3-g | Implement             | ``OSDMonitor.h``,  | ~75             |
|        | ``preprocess_get_     | ``.cc``            |                 |
|        | completed_            |                    |                 |
|        | rollbacks()``:        |                    |                 |
|        | range-scan            |                    |                 |
|        | ``OSD_SNAP_PREFIX``   |                    |                 |
|        | for                   |                    |                 |
|        | ``completed_rollback_ |                    |                 |
|        | epoch_*``             |                    |                 |
|        | keys; assemble        |                    |                 |
|        | per-epoch map; reply  |                    |                 |
|        | with                  |                    |                 |
|        | ``MMonGetCompleted    |                    |                 |
|        | RollbacksReply``;     |                    |                 |
|        | register handler in   |                    |                 |
|        | ``preprocess_         |                    |                 |
|        | query()``.            |                    |                 |
+--------+-----------------------+--------------------+-----------------+

--------------

WI-4 - PGMap Rollback Aggregation ``[S]``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Add ``calc_completed_rollbacks()`` for per-pool intersection for MON
completion detection (Sec.7.7).

**Files:** ``src/mon/PGMap.h`` - ``PGMap.cc``

+--------+-----------------------+--------------------+-----------------+
| #      | Commit                | Files              | ~LOC            |
+========+=======================+====================+=================+
| WI-4-a | Add                   | ``PGMap.h``,       | ~50             |
|        | ``completed_          | ``.cc``            |                 |
|        | rollbacks``           |                    |                 |
|        | to digest; populate   |                    |                 |
|        | from ``pg_stat_t``.   |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-4-b | Implement             | ``PGMap.cc``       | ~75             |
|        | ``calc_completed_     |                    |                 |
|        | rollbacks()``:        |                    |                 |
|        | per-pool              |                    |                 |
|        | intersection.         |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-4-c | *Test:* Unit test for | ``test/mon/        | ~55             |
|        | ``calc_completed_     | OSDMonitorTest.cc``|                 |
|        | rollbacks()``:        |                    |                 |
|        | varying per-PG        |                    |                 |
|        | completed sets;       |                    |                 |
|        | verify intersection   |                    |                 |
|        | and MON prune trigger |                    |                 |
|        | round-trip.           |                    |                 |
+--------+-----------------------+--------------------+-----------------+

--------------

WI-5 - PG Work Queue Population ``[S]``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Populate ``rollback_trimq`` on activation/advmap; drain on completion;
extend ``kick_snap_trim()`` (Sec.7.3).

**Files:** ``src/osd/PG.cc`` - ``PeeringState.cc`` - ``PrimaryLogPG.cc``

+--------+-----------------------+--------------------+-----------------+
| #      | Commit                | Files              | ~LOC            |
+========+=======================+====================+=================+
| WI-5-a | ``activate()``:       | ``PeeringState.cc``| ~60             |
|        | populate              |                    |                 |
|        | rollback_trimq;       |                    |                 |
|        | include               |                    |                 |
|        | completed_rollbacks   |                    |                 |
|        | in stats.             |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-5-b | ``on_active_          | ``PG.cc``,         | ~65             |
|        | advmap()``:           | ``PrimaryLogPG.cc``|                 |
|        | add/drain rollbacks;  |                    |                 |
|        | extend                |                    |                 |
|        | ``kick_snap_trim()``. |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-5-c | *Test:* Activate a PG | ``TestPrimary      | ~55             |
|        | with a pending        | LogPG.cc``         |                 |
|        | rollback in the       |                    |                 |
|        | OSDMap; assert        |                    |                 |
|        | ``rollback_trimq`` is |                    |                 |
|        | populated with the    |                    |                 |
|        | correct entry. Re-run |                    |                 |
|        | with the entry        |                    |                 |
|        | already in            |                    |                 |
|        | ``completed_          |                    |                 |
|        | rollbacks``;          |                    |                 |
|        | assert it is not      |                    |                 |
|        | re-added.             |                    |                 |
+--------+-----------------------+--------------------+-----------------+

--------------

WI-6 - JIT Write Path -- Pending-Op Resolution Engine ``[L]``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Extract ``build_pending_ops()``, ``execute_clone_plan()``,
``update_snapset_for_rollback()``; integrate into ``make_writeable()``
as two-transaction split (Sec.5).

**Files:** ``src/osd/PrimaryLogPG.h`` - ``PrimaryLogPG.cc``

+--------+-----------------------+--------------------+-----------------+
| #      | Commit                | Files              | ~LOC            |
+========+=======================+====================+=================+
| WI-6-a | Define                | ``PrimaryLogPG.h``,| ~70             |
|        | ``pending_op_t``;     | ``.cc``            |                 |
|        | implement             |                    |                 |
|        | ``build_pending_      |                    |                 |
|        | ops()``.              |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-6-b | Implement             | ``PrimaryLogPG.cc``| ~75             |
|        | ``execute_clone_      |                    |                 |
|        | plan()``:             |                    |                 |
|        | walk ops, maintain    |                    |                 |
|        | head_source, SNAP     |                    |                 |
|        | optimisation.         |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-6-c | Implement             | ``PrimaryLogPG.cc``| ~75             |
|        | ``update_snapset_     |                    |                 |
|        | for_rollback()``:     |                    |                 |
|        | update SnapSet;       |                    |                 |
|        | advance seq; write    |                    |                 |
|        | SS_ATTR.              |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-6-d | Emit PGLog entries    | ``PrimaryLogPG.cc``| ~65             |
|        | for txn 1: CLONE per  |                    |                 |
|        | clone; MODIFY for     |                    |                 |
|        | updated head.         |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-6-e | Integrate two-txn     | ``PrimaryLogPG.cc``| ~75             |
|        | split into            |                    |                 |
|        | ``make_writeable()``: |                    |                 |
|        | increment             |                    |                 |
|        | ``jit_rollback_       |                    |                 |
|        | inflight[rb_id]``     |                    |                 |
|        | on entry; decrement   |                    |                 |
|        | in txn 1              |                    |                 |
|        | ``on_success``        |                    |                 |
|        | callback (kicking     |                    |                 |
|        | trimmer if waiting).  |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-6-f | Add ORDERSNAP         | ``TestPrimary      | ~45             |
|        | interaction test:     | LogPG.cc``         |                 |
|        | issue write with      |                    |                 |
|        | ``snapc.seq`` <       |                    |                 |
|        | ``rb_id``; assert     |                    |                 |
|        | ORDERSNAP rejection;  |                    |                 |
|        | refresh SnapContext;  |                    |                 |
|        | assert write          |                    |                 |
|        | succeeds.             |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-6-g | Fixes to WI-6 for JIT | ``PrimaryLogPG.cc``| ~162            |
|        | writes to use 2       |                    |                 |
|        | transactions:         |                    |                 |
|        | corrected two-txn     |                    |                 |
|        | split logic in        |                    |                 |
|        | ``execute_clone_      |                    |                 |
|        | plan()`` and          |                    |                 |
|        | ``make_writeable()``. |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-6-h | Fix log version       | ``PrimaryLogPG.cc``| ~2              |
|        | numbering for WI-6:   |                    |                 |
|        | correct               |                    |                 |
|        | ``pg_log_entry_t``    |                    |                 |
|        | version field.        |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-6-i | NOP a rollback of a   | ``PrimaryLogPG.cc``| ~101            |
|        | clone that doesn't    |                    |                 |
|        | exist: guard in       |                    |                 |
|        | ``build_pending_      |                    |                 |
|        | ops()`` to skip ops   |                    |                 |
|        | when the source clone |                    |                 |
|        | is absent.            |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-6-j | Fix snap->clone ID    | ``PrimaryLogPG.cc``| ~46             |
|        | mapping for read and  | ``PrimaryLogPG.h`` |                 |
|        | write paths: correct  |                    |                 |
|        | lookup so snap IDs    |                    |                 |
|        | resolve to the right  |                    |                 |
|        | clone object.         |                    |                 |
+--------+-----------------------+--------------------+-----------------+

--------------

WI-7 - Read Path Redirect ``[S]``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Insert rollback-aware redirect in ``do_op()``; implement
``find_latest_rollback_source()`` (Sec.6).

**Files:** ``src/osd/PrimaryLogPG.cc``

+--------+-----------------------+--------------------+-----------------+
| #      | Commit                | Files              | ~LOC            |
+========+=======================+====================+=================+
| WI-7-a | Implement             | ``PrimaryLogPG.cc``| ~35             |
|        | ``find_latest_        |                    |                 |
|        | rollback_source()``.  |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-7-b | Redirect block in     | ``PrimaryLogPG.cc``| ~55             |
|        | ``do_op()``: swap     |                    |                 |
|        | obc/soid; handle      |                    |                 |
|        | ENOENT.               |                    |                 |
+--------+-----------------------+--------------------+-----------------+

--------------

WI-8 - Background Rollback via Extended SnapTrimmer ``[L]``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Pass-selection fairness; ``rollback_then_trim()`` reusing WI-6 helpers;
completion signalling (Sec.7.2--7.6).

**Files:** ``src/osd/PrimaryLogPG.h`` - ``PrimaryLogPG.cc``

+--------+-----------------------+--------------------+-----------------+
| #      | Commit                | Files              | ~LOC            |
+========+=======================+====================+=================+
| WI-8-a | Add state fields;     | ``PrimaryLogPG.h``,| ~45             |
|        | ``find_rollback_      | ``.cc``            |                 |
|        | for_source()``.       |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-8-b | Pass-selection at     | ``PrimaryLogPG.cc``| ~70             |
|        | ``AwaitAsyncWork``    |                    |                 |
|        | entry.                |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-8-c | Refactor              | ``PrimaryLogPG.cc``| ~75             |
|        | ``trim_object()`` ->  |                    |                 |
|        | ``trim_object_        |                    |                 |
|        | snap(ctx)``.          |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-8-d | Implement             | ``PrimaryLogPG.cc``| ~75             |
|        | ``rollback_then_      |                    |                 |
|        | trim()``.             |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-8-e | Replace per-object    | ``PrimaryLogPG.cc``| ~65             |
|        | loop body; transition |                    |                 |
|        | to ``WaitRepops``.    |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-8-f | Handle nullopt: erase | ``PrimaryLogPG.cc``| ~75             |
|        | trimq; update         |                    |                 |
|        | completed; existing   |                    |                 |
|        | purge-snaps path.     |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-8-g | Update                | ``PrimaryLogPG.cc``| ~30             |
|        | ``NotTrimming::       |                    |                 |
|        | react(KickTrim)``     |                    |                 |
|        | guard.                |                    |                 |
+--------+-----------------------+--------------------+-----------------+

--------------

WI-9 - Core Unit and Integration Tests ``[L]``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Encode/decode, OSDMap, clone-plan, read redirect, trimmer selection, MON
integration.

**Files:** ``src/test/osd/`` - ``src/test/mon/``

+--------+-----------------------+--------------------+-----------------+
| #      | Commit                | Files              | ~LOC            |
+========+=======================+====================+=================+
| WI-9-a | Encode/decode         | ``TestOSDTypes.cc``| ~75             |
|        | round-trips;          |                    |                 |
|        | version-downgrade.    |                    |                 |
|        | *Also:* encode at new |                    |                 |
|        | version, decode with  |                    |                 |
|        | old-version decoder;  |                    |                 |
|        | verify no crash and   |                    |                 |
|        | all previously-known  |                    |                 |
|        | fields intact         |                    |                 |
|        | (backward-compat      |                    |                 |
|        | round-trip).          |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-9-b | OSDMap                | ``TestOSDMap.cc``  | ~75             |
|        | ``apply_              |                    |                 |
|        | incremental()``       |                    |                 |
|        | rollback queue.       |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-9-c | ``build_pending_      | ``TestPrimary      | ~75             |
|        | ops()``:              | LogPG.cc``         |                 |
|        | simple, stacked, NOP, |                    |                 |
|        | multi-snap-one-clone. |                    |                 |
|        | *Also:* object        |                    |                 |
|        | created after         |                    |                 |
|        | snapshot -- JIT write |                    |                 |
|        | must succeed and      |                    |                 |
|        | advance               |                    |                 |
|        | ``SnapSet::seq``      |                    |                 |
|        | without error (no     |                    |                 |
|        | rollback clone        |                    |                 |
|        | emitted).             |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-9-d | ``execute_clone_      | ``TestPrimary      | ~75             |
|        | plan()``:             | LogPG.cc``         |                 |
|        | Sec.8.1 and Sec.8.2   |                    |                 |
|        | transaction           |                    |                 |
|        | sequences. *Also:*    |                    |                 |
|        | (a) assert PGLog      |                    |                 |
|        | entry sequence: CLONE |                    |                 |
|        | entries first, then   |                    |                 |
|        | MODIFY for head (both |                    |                 |
|        | txn 1), then MODIFY   |                    |                 |
|        | for head (txn 2) --   |                    |                 |
|        | out-of-order or       |                    |                 |
|        | missing entries must  |                    |                 |
|        | fail the test; (b)    |                    |                 |
|        | assert that after a   |                    |                 |
|        | JIT rollback          |                    |                 |
|        | ``snap_mapper.        |                    |                 |
|        | get_next_objects_     |                    |                 |
|        | to_trim()``           |                    |                 |
|        | returns the newly     |                    |                 |
|        | created clones        |                    |                 |
|        | (SnapMapper           |                    |                 |
|        | registration).        |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-9-e | Read redirect: clone  | ``TestPrimary      | ~75             |
|        | exists, ENOENT,       | LogPG.cc``         |                 |
|        | pending snap +        |                    |                 |
|        | rollback, stacked     |                    |                 |
|        | (including stacked    |                    |                 |
|        | ENOENT -- most recent |                    |                 |
|        | rollback defines      |                    |                 |
|        | object state).        |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-9-f | Trimmer               | ``TestPrimary      | ~75             |
|        | pass-selection:       | LogPG.cc``         |                 |
|        | lowest-ID; source     |                    |                 |
|        | protected; TRIM       |                    |                 |
|        | precedence. *Also:*   |                    |                 |
|        | (a) combined          |                    |                 |
|        | rollback+trim         |                    |                 |
|        | ordering -- snap in   |                    |                 |
|        | both ``snap_trimq``   |                    |                 |
|        | and as rollback       |                    |                 |
|        | source; verify        |                    |                 |
|        | rollback happens      |                    |                 |
|        | before clone          |                    |                 |
|        | deletion; (b) NOP     |                    |                 |
|        | rollback fast-exit -- |                    |                 |
|        | snap mapper returns   |                    |                 |
|        | ``nullopt`` on first  |                    |                 |
|        | call; verify rollback |                    |                 |
|        | immediately inserted  |                    |                 |
|        | into                  |                    |                 |
|        | ``completed_          |                    |                 |
|        | rollbacks``.          |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| WI-9-g | MON integration:      | ``test/mon/        | ~75             |
|        | preprocess guards;    | OSDMonitorTest.cc``|                 |
|        | idempotency; ID       |                    |                 |
|        | allocation; prune.    |                    |                 |
+--------+-----------------------+--------------------+-----------------+

--------------

Upgrade Gate, Client APIs, Integration (Sec.10--Sec.15, Sec.20)
----------------------------------------------------------------

--------------

WI-10 - Software Upgrade Gate ``[S]``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``require_osd_release >= umbrella`` guard in MON preprocess and prepare;
encoding compat (Sec.10).

**Files:** ``src/mon/OSDMonitor.cc``

+--------+-----------------------+--------------------+-----------------+
| #      | Commit                | Files              | ~LOC            |
+========+=======================+====================+=================+
| W      | *Not required.*       |                    |                 |
| I-10-a |                       |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | Unit test: ``-EPERM`` | ``test/mon/        | ~45             |
| I-10-b | at tentacle; success  | OSDMonitorTest.cc``|                 |
|        | at umbrella.          |                    |                 |
+--------+-----------------------+--------------------+-----------------+

--------------

WI-11 - Objecter Layer ``[S]``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Add ``rollback_pool_snap()`` and ``rollback_selfmanaged_snap()``
dispatching via ``_pool_op()`` (Sec.11.2).

**Files:** ``src/osdc/Objecter.h`` - ``Objecter.cc``

+--------+-----------------------+--------------------+-----------------+
| #      | Commit                | Files              | ~LOC            |
+========+=======================+====================+=================+
| W      | Declare both methods  | ``Objecter.h``     | ~35             |
| I-11-a | with Context          |                    |                 |
|        | overloads.            |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | Implement calling     | ``Objecter.cc``    | ~55             |
| I-11-b | ``_pool_op(ROLLBACK_  |                    |                 |
|        | SNAP/UNMANAGED)``;    |                    |                 |
|        | decode rollback_id.   |                    |                 |
+--------+-----------------------+--------------------+-----------------+

--------------

WI-12 - librados (C + C++) + neorados + Python APIs ``[L]``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Full API stack: ``IoCtxImpl``, C++ ``IoCtx``, C ``rados_ioctx_*_all``,
neorados ``RADOS``, Python ``Ioctx`` (Sec.11.3--11.7).

**Files:** ``src/librados/`` - ``src/include/rados/`` -
``src/include/neorados/`` - ``src/neorados/`` - ``src/pybind/rados/``

+--------+-----------------------+--------------------+-----------------+
| #      | Commit                | Files              | ~LOC            |
+========+=======================+====================+=================+
| W      | Add                   | ``IoCtxImpl.h``,   | ~75             |
| I-12-a | ``snap_rollback()``   | ``.cc``            |                 |
|        | and                   |                    |                 |
|        | ``selfmanaged_snap_   |                    |                 |
|        | rollback()``          |                    |                 |
|        | to ``IoCtxImpl``      |                    |                 |
|        | using mutex/cond      |                    |                 |
|        | pattern.              |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | Add C++               | ``librados.hpp``,  | ~50             |
| I-12-b | ``IoCtx::             | ``librados.cc``    |                 |
|        | snap_rollback()``     |                    |                 |
|        | and                   |                    |                 |
|        | ``selfmanaged_snap_   |                    |                 |
|        | rollback()``;         |                    |                 |
|        | forwarding bodies in  |                    |                 |
|        | ``librados.cc``.      |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | Add C                 | ``librados.h``,    | ~65             |
| I-12-c | ``rados_ioctx_snap_   | ``librados.cc``    |                 |
|        | rollback_all()``      |                    |                 |
|        | and                   |                    |                 |
|        | ``rados_ioctx_        |                    |                 |
|        | selfmanaged_snap_     |                    |                 |
|        | rollback_all()``      |                    |                 |
|        | with doc comments.    |                    |                 |
|        | Names use ``_all``    |                    |                 |
|        | suffix to avoid       |                    |                 |
|        | collision with        |                    |                 |
|        | existing per-object   |                    |                 |
|        | ``rados_ioctx_snap_   |                    |                 |
|        | rollback(io, oid,     |                    |                 |
|        | snapname)``.          |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | Add neorados          | ``RADOS.hpp``,     | ~75             |
| I-12-d | ``RADOS::rollback_    | ``RADOS.cc``       |                 |
|        | pool_snap()``         |                    |                 |
|        | and                   |                    |                 |
|        | ``rollback_           |                    |                 |
|        | selfmanaged_snap()``  |                    |                 |
|        | as Asio templates;    |                    |                 |
|        | dispatch in           |                    |                 |
|        | ``RADOS.cc``.         |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | Add Python            | ``rados.pyx``,     | ~70             |
| I-12-e | ``Ioctx.              | ``c_rados.pxd``    |                 |
|        | rollback_snap()``     |                    |                 |
|        | and                   |                    |                 |
|        | ``rollback_self_      |                    |                 |
|        | managed_snap()``;     |                    |                 |
|        | add                   |                    |                 |
|        | ``rados_ioctx_snap_   |                    |                 |
|        | rollback_all``        |                    |                 |
|        | and                   |                    |                 |
|        | ``rados_ioctx_        |                    |                 |
|        | selfmanaged_snap_     |                    |                 |
|        | rollback_all``        |                    |                 |
|        | declarations to       |                    |                 |
|        | ``c_rados.pxd``.      |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | *Test:* Python        | ``src/test/pybind/ | ~55             |
| I-12-f | binding tests in      | test_rados.py``    |                 |
|        | ``test_rados.py``:    |                    |                 |
|        | ``rollback_snap()``   |                    |                 |
|        | success;              |                    |                 |
|        | ``rollback_snap()``   |                    |                 |
|        | with non-existent     |                    |                 |
|        | snapshot raises       |                    |                 |
|        | ``ObjectNotFound``;   |                    |                 |
|        | ``rollback_self_      |                    |                 |
|        | managed_snap()``      |                    |                 |
|        | success.              |                    |                 |
+--------+-----------------------+--------------------+-----------------+

--------------

WI-13 - RADOS CLI (``rollbacksnap``) ``[S]``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Add ``rollbacksnap`` command; auto-detect pool mode; dispatch to
``snap_rollback()`` or ``selfmanaged_snap_rollback()`` (Sec.12).

**Files:** ``src/tools/rados/rados.cc``

+--------+-----------------------+--------------------+-----------------+
| #      | Commit                | Files              | ~LOC            |
+========+=======================+====================+=================+
| W      | Add handler: detect   | ``rados.cc``       | ~65             |
| I-13-a | mode; dispatch;       |                    |                 |
|        | extend usage string.  |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | Error-message branch  | ``rados.cc``       | ~40             |
| I-13-b | for ``-EPERM``: call  |                    |                 |
|        | ``get_min_compatible_ |                    |                 |
|        | osd()``;              |                    |                 |
|        | format release name.  |                    |                 |
|        | Handle ``-ENOENT``    |                    |                 |
|        | and bad numeric       |                    |                 |
|        | argument.             |                    |                 |
+--------+-----------------------+--------------------+-----------------+

--------------

WI-14 - librbd Pool-Op Fast Path ``[M]``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Replace per-object loop with ``selfmanaged_snap_rollback()`` fast path;
silent fallback (Sec.13).

**Files:** ``src/librbd/operation/SnapshotRollbackRequest.cc``

+--------+-----------------------+--------------------+-----------------+
| #      | Commit                | Files              | ~LOC            |
+========+=======================+====================+=================+
| W      | Fast-path block:      | ``SnapshotRollback | ~55             |
| I-14-a | probe release; call   | Request.cc``       |                 |
|        | ``selfmanaged_snap_   |                    |                 |
|        | rollback(m_snap_id)`` |                    |                 |
|        | short-circuit on      |                    |                 |
|        | success.              |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | On failure: log level | ``SnapshotRollback | ~30             |
| I-14-b | 1; fall through to    | Request.cc``       |                 |
|        | AsyncObjectThrottle.  |                    |                 |
|        | Add perf counter.     |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | Unit tests: mock      | ``test/librbd/     | ~75             |
| I-14-c | umbrella -> fast path | test_internal.cc`` |                 |
|        | used; mock tentacle   |                    |                 |
|        | -> per-object         |                    |                 |
|        | fallback.             |                    |                 |
|        | *Also:* journal       |                    |                 |
|        | replay -- replaying a |                    |                 |
|        | ``SnapRollbackEvent`` |                    |                 |
|        | on umbrella cluster   |                    |                 |
|        | uses fast path;       |                    |                 |
|        | verify correct        |                    |                 |
|        | dispatch to           |                    |                 |
|        | ``selfmanaged_snap_   |                    |                 |
|        | rollback()``          |                    |                 |
|        | (Sec.13.5).           |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | Integration test: rbd | ``test/librbd/     | ~70             |
| I-14-d | snap rollback on      | test_librbd.cc``   |                 |
|        | umbrella cluster;     |                    |                 |
|        | verify data           |                    |                 |
|        | integrity; no         |                    |                 |
|        | per-object ops in OSD |                    |                 |
|        | logs.                 |                    |                 |
+--------+-----------------------+--------------------+-----------------+

--------------

WI-15 - ceph_test_rados Extension (``SnapRollbackOp``) ``[M]``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Add ``SnapRollbackOp`` and ``roll_back_pool()`` for both pool-managed
and selfmanaged modes; mix with ``RollbackOp``; QA yaml + ``rados.py``
(Sec.14).

**Files:** ``src/test/osd/RadosModel.h`` - ``TestRados.cc`` -
``src/qa/tasks/rados.py`` - ``src/qa/suites/**/*.yaml``

+--------+-----------------------+--------------------+-----------------+
| #      | Commit                | Files              | ~LOC            |
+========+=======================+====================+=================+
| W      | Add                   | ``RadosModel.h``   | ~55             |
| I-15-a | TEST_OP_SNAP_ROLLBACK |                    |                 |
|        | to enum; implement    |                    |                 |
|        | ``roll_back_pool()``. |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | Implement             | ``RadosModel.h``   | ~75             |
| I-15-b | ``SnapRollbackOp``:   |                    |                 |
|        | quiesce, model        |                    |                 |
|        | update, dispatch to   |                    |                 |
|        | ``snap_rollback()``   |                    |                 |
|        | or                    |                    |                 |
|        | ``selfmanaged_snap_   |                    |                 |
|        | rollback()``,         |                    |                 |
|        | abort on error.       |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | Register              | ``TestRados.cc``   | ~35             |
| I-15-c | ``SnapRollbackOp`` at |                    |                 |
|        | weight 2 in both snap |                    |                 |
|        | modes.                |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | Add                   | ``qa/tasks/        | ~15             |
| I-15-d | ``"snap_rollback"``   | rados.py``         |                 |
|        | to op-weight field    |                    |                 |
|        | list in ``rados.py``; |                    |                 |
|        | update docstring.     |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | Add                   | ``qa/              | ~25             |
| I-15-e | ``snap_rollback: 10`` | suites/**/*.yaml`` |                 |
|        | alongside             |                    |                 |
|        | ``rollback: 50`` in   |                    |                 |
|        | all applicable        |                    |                 |
|        | non-Crimson QA        |                    |                 |
|        | workload YAML files   |                    |                 |
|        | (~23 files).          |                    |                 |
|        | Upgrade-first-half    |                    |                 |
|        | phases set            |                    |                 |
|        | ``snap_rollback: 0``. |                    |                 |
+--------+-----------------------+--------------------+-----------------+

--------------

WI-16 - librados Test Extensions ``[M]``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

C API GTests in ``snapshots.cc``, C++ GTests in ``snapshots_cxx.cc``,
in-memory stub in ``librados_test_stub`` (Sec.15).

**Files:** ``src/test/librados/snapshots.cc`` - ``snapshots_cxx.cc`` -
``src/test/librados_test_stub/``

+--------+-----------------------+--------------------+-----------------+
| #      | Commit                | Files              | ~LOC            |
+========+=======================+====================+=================+
| W      | C++ tests:            | ``snapshots_cxx.   | ~75             |
| I-16-a | ``PoolSnapRollbackPP``| cc``               |                 |
|        | (success, ENOENT,     |                    |                 |
|        | idempotent),          |                    |                 |
|        | ``PoolSelfmanaged     |                    |                 |
|        | SnapRollbackPP``      |                    |                 |
|        | (success,             |                    |                 |
|        | idempotent). *Must    |                    |                 |
|        | include               |                    |                 |
|        | data-integrity case:* |                    |                 |
|        | write data A ->       |                    |                 |
|        | create snap ->        |                    |                 |
|        | write data B ->       |                    |                 |
|        | rollback -> read back |                    |                 |
|        | and assert content    |                    |                 |
|        | equals A.             |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | C tests:              | ``snapshots.cc``   | ~70             |
| I-16-b | ``PoolSnapRollback``, |                    |                 |
|        | ``PoolSnapRollback    |                    |                 |
|        | Noent``,              |                    |                 |
|        | ``PoolSelfmanaged     |                    |                 |
|        | SnapRollback``.       |                    |                 |
|        | *Must include         |                    |                 |
|        | data-integrity case:* |                    |                 |
|        | write -> snap ->      |                    |                 |
|        | overwrite -> rollback |                    |                 |
|        | -> read and verify    |                    |                 |
|        | object content equals |                    |                 |
|        | snapshot content.     |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | Test stub: add        | ``TestIoCtxImpl.h``| ~75             |
| I-16-c | ``snap_rollback()``   | ``TestMemIoCtx     |                 |
|        | and                   | Impl.h/.cc``       |                 |
|        | ``selfmanaged_snap_   |                    |                 |
|        | rollback()``          |                    |                 |
|        | virtual methods to    |                    |                 |
|        | ``TestIoCtxImpl``;    |                    |                 |
|        | implement in          |                    |                 |
|        | ``TestMemIoCtxImpl``  |                    |                 |
|        | with fake rollback-ID |                    |                 |
|        | allocation. **Known   |                    |                 |
|        | limitation:** the     |                    |                 |
|        | stub's read path does |                    |                 |
|        | not redirect reads    |                    |                 |
|        | through               |                    |                 |
|        | ``pending_rollbacks`` |                    |                 |
|        | to the source snap    |                    |                 |
|        | content;              |                    |                 |
|        | read-correctness      |                    |                 |
|        | regressions in the    |                    |                 |
|        | librbd fast path      |                    |                 |
|        | (WI-14-c) therefore   |                    |                 |
|        | require a real OSD    |                    |                 |
|        | rather than the stub. |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | Mock stub: add        | ``MockTestMem      | ~35             |
| I-16-d | ``MOCK_METHOD``       | IoCtxImpl.h``      |                 |
|        | declarations and      |                    |                 |
|        | ``ON_CALL`` defaults  |                    |                 |
|        | for both new methods. |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | Add neorados          | ``test/neorados/   | ~65             |
| I-16-e | ``PoolSnapRollback``, | snapshots.cc``     |                 |
|        | ``PoolSnapRollback    |                    |                 |
|        | Noent``, and          |                    |                 |
|        | ``PoolSelfmanaged     |                    |                 |
|        | SnapRollback``        |                    |                 |
|        | tests with            |                    |                 |
|        | data-integrity checks |                    |                 |
|        | to ``snapshots.cc``.  |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | Add tests for more    | ``test/librados/   | ~329            |
| I-16-f | complex sequences of  | snapshots_cxx.cc`` |                 |
|        | snapshots +           |                    |                 |
|        | rollbacks: multi-snap |                    |                 |
|        | interleaved write /   |                    |                 |
|        | snap / rollback       |                    |                 |
|        | scenarios in C++      |                    |                 |
|        | GTest suite.          |                    |                 |
+--------+-----------------------+--------------------+-----------------+

--------------

WI-17 - OSD Boot-Time Completed-Rollbacks Catch-Up ``[M]``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

New message types; ``_get_completed_rollbacks()``;
``handle_get_completed_rollbacks_reply()``;
``SnapMapper::record_completed_rollbacks()``; inline recording in
``handle_osd_map()``; boot guard in ``_send_boot()`` (Sec.4.9).

**Files:** ``src/messages/`` - ``src/osd/OSD.h`` - ``OSD.cc`` -
``src/osd/SnapMapper.h`` - ``SnapMapper.cc``

+--------+-----------------------+--------------------+-----------------+
| #      | Commit                | Files              | ~LOC            |
+========+=======================+====================+=================+
| W      | *Not required.*       |                    |                 |
| I-17-a |                       |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | Implement             | ``SnapMapper.h``,  | ~70             |
| I-17-b | ``SnapMapper::        | ``SnapMapper.cc``  |                 |
|        | record_completed_     |                    |                 |
|        | rollbacks()``:        |                    |                 |
|        | iterate per-epoch     |                    |                 |
|        | map, call             |                    |                 |
|        | ``set_completed_      |                    |                 |
|        | rollback(driver,      |                    |                 |
|        | t, pool_id, rb_id)``  |                    |                 |
|        | for each ID;          |                    |                 |
|        | implement             |                    |                 |
|        | ``set_completed_      |                    |                 |
|        | rollback()``          |                    |                 |
|        | /                     |                    |                 |
|        | ``is_completed_       |                    |                 |
|        | rollback()``          |                    |                 |
|        | key helpers.          |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | Populate              | ``OSD.cc``         | ~55             |
| I-17-c | ``completed_          |                    |                 |
|        | rollbacks[e]``        |                    |                 |
|        | local map in the      |                    |                 |
|        | epoch-iteration loop  |                    |                 |
|        | of                    |                    |                 |
|        | ``handle_osd_map()``  |                    |                 |
|        | (mirrors the existing |                    |                 |
|        | ``purged_snaps[e]``   |                    |                 |
|        | line); call           |                    |                 |
|        | ``SnapMapper::        |                    |                 |
|        | record_completed_     |                    |                 |
|        | rollbacks()``         |                    |                 |
|        | in the post-loop      |                    |                 |
|        | block guarded by      |                    |                 |
|        | ``completed_          |                    |                 |
|        | rollbacks_last        |                    |                 |
|        | == start - 1``;       |                    |                 |
|        | update                |                    |                 |
|        | ``superblock.         |                    |                 |
|        | completed_rollbacks_  |                    |                 |
|        | last``.               |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | Implement             | ``OSD.h``,         | ~75             |
| I-17-d | ``_get_completed_     | ``OSD.cc``         |                 |
|        | rollbacks()``         |                    |                 |
|        | and                   |                    |                 |
|        | ``handle_get_         |                    |                 |
|        | completed_rollbacks_  |                    |                 |
|        | reply()``;            |                    |                 |
|        | register reply        |                    |                 |
|        | handler in the OSD    |                    |                 |
|        | message dispatch      |                    |                 |
|        | table; add boot guard |                    |                 |
|        | in ``_send_boot()``   |                    |                 |
|        | analogous to the      |                    |                 |
|        | ``purged_snaps_last`` |                    |                 |
|        | check.                |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | *Test:* (a) Unit test | ``test/osd/        | ~75             |
| I-17-e | for                   | TestSnapMapper.cc``|                 |
|        | ``record_completed_   | ``test/librados/   |                 |
|        | rollbacks()``:        | snapshots_cxx.cc`` |                 |
|        | populate a map with   |                    |                 |
|        | two epochs and two    |                    |                 |
|        | pools; assert         |                    |                 |
|        | ``is_completed_       |                    |                 |
|        | rollback()``          |                    |                 |
|        | returns true for all  |                    |                 |
|        | recorded IDs and      |                    |                 |
|        | false for unrecorded  |                    |                 |
|        | ones. (b) Integration |                    |                 |
|        | test: bring OSD       |                    |                 |
|        | offline; issue a      |                    |                 |
|        | rollback; confirm     |                    |                 |
|        | completion; restart   |                    |                 |
|        | OSD; assert           |                    |                 |
|        | ``completed_          |                    |                 |
|        | rollbacks_last``      |                    |                 |
|        | == ``current_epoch``  |                    |                 |
|        | after boot and that   |                    |                 |
|        | the source clone is   |                    |                 |
|        | no longer protected.  |                    |                 |
+--------+-----------------------+--------------------+-----------------+

--------------

WI-18 - Snapshot Clone Logic: Ignore Rollback Snap IDs ``[S]``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Fix ``make_writeable()`` so that rollback IDs that advance ``snap_seq``
without being inserted into ``pg_pool_t::snaps`` cannot corrupt clone
naming, the clone gate, or the persisted ``SnapSet::seq`` (Sec.16).

**Files:** ``src/osd/osd_types.h`` - ``src/osd/PrimaryLogPG.cc``

+--------+-----------------------+--------------------+-----------------+
| #      | Commit                | Files              | ~LOC            |
+========+=======================+====================+=================+
| W      | Add ``snapid_t        | ``osd_types.h``    | ~10             |
| I-18-a | real_snap_seq`` field |                    |                 |
|        | to ``OpContext``,     |                    |                 |
|        | default ``0``.        |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | In                    | ``PrimaryLogPG.cc``| ~20             |
| I-18-b | ``execute_ctx()``,    |                    |                 |
|        | after populating      |                    |                 |
|        | ``ctx->snapc``,       |                    |                 |
|        | compute               |                    |                 |
|        | ``ctx->real_snap_seq``|                    |                 |
|        | as highest key in     |                    |                 |
|        | ``pool.info.snaps``   |                    |                 |
|        | (or ``0`` if empty).  |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | Replace               | ``PrimaryLogPG.cc``| ~15             |
| I-18-c | ``coid.snap =         |                    |                 |
|        | snapc.seq`` with      |                    |                 |
|        | ``coid.snap =         |                    |                 |
|        | ctx->real_snap_seq``  |                    |                 |
|        | in clone-naming block |                    |                 |
|        | of ``make_writeable``.|                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | Guard clone gate:     | ``PrimaryLogPG.cc``| ~15             |
| I-18-d | compare               |                    |                 |
|        | ``snapc.snaps[0]``    |                    |                 |
|        | against               |                    |                 |
|        | ``new_snapset``       |                    |                 |
|        | highest real-clone    |                    |                 |
|        | snap (via             |                    |                 |
|        | ``real_snap_seq()``   |                    |                 |
|        | helper or             |                    |                 |
|        | ``rollback_snaps``    |                    |                 |
|        | check), not raw       |                    |                 |
|        | ``SnapSet::seq``.     |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | Replace               | ``PrimaryLogPG.cc``| ~10             |
| I-18-e | ``ctx->new_snapset.   |                    |                 |
|        | seq = snapc.seq``     |                    |                 |
|        | with                  |                    |                 |
|        | ``ctx->new_snapset.   |                    |                 |
|        | seq =                 |                    |                 |
|        | ctx->real_snap_seq``  |                    |                 |
|        | so persisted          |                    |                 |
|        | ``SnapSet::seq``      |                    |                 |
|        | never holds a         |                    |                 |
|        | rollback ID.          |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | *Test:* Pool-snaps    | ``TestPrimary      | ~75             |
| I-18-f | write with a pending  | LogPG.cc``         |                 |
|        | rollback ID           |                    |                 |
|        | interleaved: (a)      |                    |                 |
|        | assert no spurious    |                    |                 |
|        | clone when rollback   |                    |                 |
|        | ID is sole seq        |                    |                 |
|        | advance; (b) assert   |                    |                 |
|        | clone is named after  |                    |                 |
|        | real snap ``S_new``,  |                    |                 |
|        | not rollback ID       |                    |                 |
|        | ``R``, when a new     |                    |                 |
|        | snapshot is created   |                    |                 |
|        | after ``R``; (c)      |                    |                 |
|        | assert                |                    |                 |
|        | ``SnapSet::seq``      |                    |                 |
|        | never contains a      |                    |                 |
|        | rollback ID after any |                    |                 |
|        | write.                |                    |                 |
+--------+-----------------------+--------------------+-----------------+

--------------

WI-19 - Unmanaged Snap Rollback: Later-Snapshot Awareness ``[M]``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Fix the snaptrimmer and JIT write path so that a
``POOL_OP_ROLLBACK_UNMANAGED_SNAP`` rollback correctly detects and
preserves later snapshots, by carrying the client ``SnapContext`` in the
rollback request and storing it in ``rollback_snap_info_t`` (Sec.17).

**Files:** ``src/osd/osd_types.h`` - ``src/osd/osd_types.cc`` -
``src/mon/OSDMonitor.cc`` - ``src/osdc/Objecter.h`` -
``src/osdc/Objecter.cc`` - ``src/librados/IoCtxImpl.h`` -
``src/librados/IoCtxImpl.cc`` - ``src/include/rados/librados.h`` -
``src/include/rados/librados.hpp`` - ``src/osd/PrimaryLogPG.cc``

+--------+-----------------------+--------------------+-----------------+
| #      | Commit                | Files              | ~LOC            |
+========+=======================+====================+=================+
| W      | Add ``SnapContext     | ``osd_types.h``,   | ~25             |
| I-19-a | snapc`` field to      | ``osd_types.cc``   |                 |
|        | ``rollback_snap_      |                    |                 |
|        | info_t``; bump        |                    |                 |
|        | ``struct_v`` in       |                    |                 |
|        | encode/decode so      |                    |                 |
|        | older OSDs decode     |                    |                 |
|        | safely with empty     |                    |                 |
|        | ``snapc``.            |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | In                    | ``OSDMonitor.cc``  | ~30             |
| I-19-b | ``preprocess_         |                    |                 |
|        | pool_op()``           |                    |                 |
|        | ``POOL_OP_ROLLBACK_   |                    |                 |
|        | UNMANAGED_SNAP``      |                    |                 |
|        | case: add             |                    |                 |
|        | ``SnapContext``       |                    |                 |
|        | validation            |                    |                 |
|        | (``is_valid()``,      |                    |                 |
|        | ``seq >= snapid``,    |                    |                 |
|        | ``snapid`` present    |                    |                 |
|        | in ``snaps``);        |                    |                 |
|        | return ``-EINVAL``    |                    |                 |
|        | on failure.           |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | In                    | ``OSDMonitor.cc``  | ~10             |
| I-19-c | ``prepare_pool_op()`` |                    |                 |
|        | ``POOL_OP_ROLLBACK_   |                    |                 |
|        | UNMANAGED_SNAP``      |                    |                 |
|        | case: copy            |                    |                 |
|        | ``m->snapc`` into     |                    |                 |
|        | ``rb.snapc`` before   |                    |                 |
|        | storing the           |                    |                 |
|        | ``rollback_snap_      |                    |                 |
|        | info_t``.             |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | Update                | ``Objecter.h``,    | ~20             |
| I-19-d | ``rollback_           | ``Objecter.cc``,   |                 |
|        | selfmanaged_snap()``  | ``IoCtxImpl.h``,   |                 |
|        | in ``Objecter`` to    | ``IoCtxImpl.cc``,  |                 |
|        | accept and forward    | ``librados.h``,    |                 |
|        | a ``SnapContext``     | ``librados.hpp``   |                 |
|        | parameter. Update     |                    |                 |
|        | C and C++ librados    |                    |                 |
|        | wrappers to match.    |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | Add                   | ``PrimaryLogPG.cc``| ~30             |
| I-19-e | ``snap_id_set_from_   |                    |                 |
|        | snapc()`` helper      |                    |                 |
|        | and modify            |                    |                 |
|        | ``build_pending_      |                    |                 |
|        | ops()`` to fall back  |                    |                 |
|        | to ``rb_info->snapc`` |                    |                 |
|        | when                  |                    |                 |
|        | ``pg_pool_t::snaps``  |                    |                 |
|        | is empty (unmanaged   |                    |                 |
|        | pool).                |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | *Test:* Unmanaged-    | ``TestPrimaryLog   | ~80             |
| I-19-f | snap rollback with    | PG.cc`` or         |                 |
|        | a later snap ``T``    | ``snapshots_cxx    |                 |
|        | present: (a) assert   | .cc``              |                 |
|        | ``clone(head -> T)``  |                    |                 |
|        | is emitted before     |                    |                 |
|        | the rollback clone;   |                    |                 |
|        | (b) assert the fix    |                    |                 |
|        | is a no-op when no    |                    |                 |
|        | later snap exists;    |                    |                 |
|        | (c) assert stale      |                    |                 |
|        | ``SnapContext``       |                    |                 |
|        | returns ``-EINVAL``   |                    |                 |
|        | from preprocess.      |                    |                 |
+--------+-----------------------+--------------------+-----------------+

--------------

WI-20 - ``ceph osd pool rollbacksnap`` CLI ``[S]``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Add ``ceph osd pool rollbacksnap <pool> <snap>`` as a ``ceph``
monitor command alongside the existing ``osd pool mksnap`` and
``osd pool rmsnap`` commands. The command targets pool-managed snapshot
mode only; selfmanaged pools continue to use ``rados -p <pool>
rollbacksnap <snap-id>`` (Sec.12.4).

**Files:** ``src/mon/MonCommands.h`` - ``src/mon/OSDMonitor.cc``

+--------+-----------------------+--------------------+-----------------+
| #      | Commit                | Files              | ~LOC            |
+========+=======================+====================+=================+
| W      | Register              | ``MonCommands.h``  | ~5              |
| I-20-a | ``osd pool            |                    |                 |
|        | rollbacksnap``        |                    |                 |
|        | ``COMMAND`` entry     |                    |                 |
|        | with                  |                    |                 |
|        | ``CephPoolname`` +    |                    |                 |
|        | ``CephString``        |                    |                 |
|        | params, ``"osd"``     |                    |                 |
|        | service, ``"rw"``     |                    |                 |
|        | permission.           |                    |                 |
+--------+-----------------------+--------------------+-----------------+
| W      | Add handler in        | ``OSDMonitor.cc``  | ~70             |
| I-20-b | ``prepare_command_    |                    |                 |
|        | impl()`` alongside    |                    |                 |
|        | ``osd pool mksnap``   |                    |                 |
|        | and ``osd pool        |                    |                 |
|        | rmsnap``: validate    |                    |                 |
|        | release gate; reject  |                    |                 |
|        | unmanaged-snap mode;  |                    |                 |
|        | verify snap exists;   |                    |                 |
|        | idempotency check;    |                    |                 |
|        | allocate rollback_id; |                    |                 |
|        | update                |                    |                 |
|        | ``pending_inc``       |                    |                 |
|        | and call              |                    |                 |
|        | ``wait_for_commit``.  |                    |                 |
+--------+-----------------------+--------------------+-----------------+

--------------

Summary
-------

+--------+------------+-------------------+----------------------------+
| WI     | Size       | Commits           | Description                |
+========+============+===================+============================+
| WI-1   | S          | 4                 | Core data structures --    |
|        |            |                   | new types, encode/decode,  |
|        |            |                   | PG field                   |
+--------+------------+-------------------+----------------------------+
| WI-2   | M          | 6                 | OSDMap extensions --       |
|        |            |                   | queue, incremental,        |
|        |            |                   | apply_incremental, compat, |
|        |            |                   | OSDSuperblock              |
|        |            |                   | completed_rollbacks_last   |
+--------+------------+-------------------+----------------------------+
| WI-3   | M          | 7                 | Pool op codes --           |
|        |            |                   | preprocess (Umbrella gate),|
|        |            |                   | prepare, prune (with       |
|        |            |                   | guards), encode_pending    |
|        |            |                   | persistence,               |
|        |            |                   | preprocess_get_completed_  |
|        |            |                   | rollbacks                  |
+--------+------------+-------------------+----------------------------+
| WI-4   | S          | 3                 | PGMap rollback aggregation |
|        |            |                   | -- intersection for MON    |
|        |            |                   | completion + unit test     |
+--------+------------+-------------------+----------------------------+
| WI-5   | S          | 3                 | PG work queue -- activate, |
|        |            |                   | advmap, kick_snap_trim +   |
|        |            |                   | activation test            |
+--------+------------+-------------------+----------------------------+
| WI-6   | L          | 10                | JIT write path --          |
|        |            |                   | pending-op engine, clone   |
|        |            |                   | plan, snapset, log         |
|        |            |                   | entries, JIT inflight      |
|        |            |                   | interlock, ORDERSNAP test; |
|        |            |                   | fixes: 2-txn split,        |
|        |            |                   | log version, NOP missing   |
|        |            |                   | clone, snap->clone ID      |
+--------+------------+-------------------+----------------------------+
| WI-7   | S          | 2                 | Read path redirect --      |
|        |            |                   | find source, redirect or   |
|        |            |                   | ENOENT                     |
+--------+------------+-------------------+----------------------------+
| WI-8   | L          | 7                 | Background rollback via    |
|        |            |                   | extended SnapTrimmer       |
+--------+------------+-------------------+----------------------------+
| WI-9   | L          | 7                 | Core unit + MON            |
|        |            |                   | integration tests          |
+--------+------------+-------------------+----------------------------+
| WI-10  | S          | 2                 | Upgrade gate --            |
|        |            |                   | require_osd_release >=     |
|        |            |                   | umbrella + test            |
+--------+------------+-------------------+----------------------------+
| WI-11  | S          | 2                 | Objecter --                |
|        |            |                   | rollback_pool_snap() +     |
|        |            |                   | rollback_selfmanaged_snap()|
+--------+------------+-------------------+----------------------------+
| WI-12  | L          | 6                 | librados C+C++ + neorados  |
|        |            |                   | + Python --                |
|        |            |                   | snap_rollback_all /        |
|        |            |                   | selfmanaged_snap_          |
|        |            |                   | rollback_all + Python      |
|        |            |                   | tests                      |
+--------+------------+-------------------+----------------------------+
| WI-13  | S          | 2                 | RADOS CLI -- rollbacksnap  |
|        |            |                   | command (pool-managed +    |
|        |            |                   | selfmanaged)               |
+--------+------------+-------------------+----------------------------+
| WI-14  | M          | 4                 | librbd -- pool-op fast     |
|        |            |                   | path via                   |
|        |            |                   | selfmanaged_snap_          |
|        |            |                   | rollback() with fallback   |
+--------+------------+-------------------+----------------------------+
| WI-15  | M          | 5                 | ceph_test_rados --         |
|        |            |                   | SnapRollbackOp both snap   |
|        |            |                   | modes + QA yaml + rados.py |
+--------+------------+-------------------+----------------------------+
| WI-16  | M          | 6                 | librados tests -- C + C++  |
|        |            |                   | GTests (with               |
|        |            |                   | data-integrity read-back)  |
|        |            |                   | + in-memory stub +         |
|        |            |                   | neorados data-integrity    |
|        |            |                   | tests + complex snapshot / |
|        |            |                   | rollback sequence tests    |
+--------+------------+-------------------+----------------------------+
| WI-17  | M          | 5                 | OSD boot-time catch-up --  |
|        |            |                   | messages, SnapMapper::     |
|        |            |                   | record_completed_          |
|        |            |                   | rollbacks(),               |
|        |            |                   | handle_osd_map inline      |
|        |            |                   | recording, boot guard      |
+--------+------------+-------------------+----------------------------+
| WI-18  | S          | 6                 | Snapshot clone logic:      |
|        |            |                   | ignore rollback snap IDs   |
|        |            |                   | -- real_snap_seq field,    |
|        |            |                   | clone naming, gate guard,  |
|        |            |                   | SnapSet::seq update,       |
|        |            |                   | regression tests           |
+--------+------------+-------------------+----------------------------+
| WI-19  | M          | 6                 | Unmanaged snap rollback:   |
|        |            |                   | later-snapshot awareness   |
|        |            |                   | -- snapc field in          |
|        |            |                   | rollback_snap_info_t,      |
|        |            |                   | MON validation + prepare,  |
|        |            |                   | Objecter + librados API    |
|        |            |                   | update,                    |
|        |            |                   | build_pending_ops() fix,   |
|        |            |                   | regression tests           |
+--------+------------+-------------------+----------------------------+
| WI-20  | S          | 2                 | ceph CLI --                |
|        |            |                   | ``ceph osd pool            |
|        |            |                   | rollbacksnap``             |
|        |            |                   | (pool-managed only;        |
|        |            |                   | MonCommands.h +            |
|        |            |                   | OSDMonitor handler)        |
+--------+------------+-------------------+----------------------------+
| **T    |            | **98**            | ~98 commits - ~5,800 net   |
| otal** |            |                   | lines of implementation +  |
|        |            |                   | test code                  |
+--------+------------+-------------------+----------------------------+

--------------

**Critical path:** WI-1 -> WI-2 -> WI-3/WI-10 -> WI-4 -> WI-5 -> WI-6 -> WI-7
-> WI-8 -> WI-9.

**Boot catch-up chain:** WI-2 (superblock field) -> WI-17 (messages +
SnapMapper + OSD handler) -- can proceed in parallel with WI-5 onwards.

**API chain:** WI-11 -> WI-12 -> WI-13 (CLI) - WI-14 (librbd) - WI-15
(test tool) - WI-16 (librados tests). WI-12 is a prerequisite for WI-13,
WI-14, WI-15, and WI-16. WI-16 depends on WI-12 for the stub.

**Snapshot correctness chain:** WI-18 (clone logic) and WI-19
(unmanaged later-snap) are independent of each other but both depend on
WI-6 (JIT write path) being complete.
