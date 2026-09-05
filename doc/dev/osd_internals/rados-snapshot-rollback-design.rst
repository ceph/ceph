RADOS Snapshot Rollback
=======================

Design Document Version 8

Author: bill_scales@uk.ibm.com

Assisted-by: IBM Bob 2.0.3

Warning: Psuedo code is used just to illustrate the design concepts,
there are shortcuts that will need to be addressed when writing
implementaiton code

--------------

Table of Contents
-----------------

1.  `Concept and Goals <#1-concept-and-goals>`__

2.  `Background: How RADOS Snapshots Work
    Today <#2-background-how-rados-snapshots-work-today>`__

3.  `Data Structure Additions <#3-data-structure-additions>`__

4.  `New Pool Operations <#4-new-pool-operations>`__

5.  `Write I/O Path: Just-in-Time
    Rollback <#5-write-io-path-just-in-time-rollback>`__

6.  `Read I/O Path: Redirect to Source
    Clone <#6-read-io-path-redirect-to-source-clone>`__

7.  `Background Rollback via Extended Snap
    Trimmer <#7-background-rollback-via-extended-snap-trimmer>`__

8.  `Worked Scenarios <#8-worked-scenarios>`__

9.  `Edge Cases and Constraints <#9-edge-cases-and-constraints>`__

10. `Software Upgrade and
    Compatibility <#10-software-upgrade-and-compatibility>`__

11. `RADOS API Changes <#11-rados-api-changes>`__

12. `RADOS CLI Extension <#12-rados-cli-extension>`__

13. `librbd Integration <#13-librbd-integration>`__

14. `ceph_test_rados Extension <#14-ceph_test_rados-extension>`__

15. `librados Test Extensions <#15-librados-test-extensions>`__

16. `Snapshot Code Changes to Ignore Rollback Snap IDs <#16-snapshot-code-changes-to-ignore-rollback-snap-ids>`__

17. `Unmanaged Snap Rollback: Later-Snapshot Awareness <#17-unmanaged-snap-rollback-later-snapshot-awareness>`__

18. `Post-Rollback Head-Deletion Sweep for Objects Created After the Snapshot <#18-post-rollback-head-deletion-sweep-for-objects-created-after-the-snapshot>`__

19. .. rubric:: `Summary of Changes <#19-summary-of-changes>`__
       :name: summary-of-changes

1. Concept and Goals
--------------------

Currently RADOS implements a snapshot feature that allows point-in-time
copies of a set of objects to be created. Snapshot creation is an O(1)
operation, defering the actual work of cloning objects to a just-in-time
process when the object is next written. All snapshots of a RADOS object
are stored on the same OSDs as the head object which makes it easy to
implement this process within the OSD daemon.

RADOS also implements a rollback I/O operation which will restore a
RADOS object to a previous point-in-time snapshot. Clients wishing to
rollback a set of RADOS objects must iterrate across these objects
issuing rollback I/O operations and need to deal with how to make this
rollback appear atomic. This implementation of rollback is O(number of
objects), unless the client implements its own just-in-time processes
for deferring when it issues rollback operations.

This design document considers a new RADOS snapshot rollback feature
that provides the ability to revert all objects in a pool to the state
captured in a previously created snapshot with similar performance to
taking a snapshot, without requiring client coordination, and without
blocking client I/O while the rollback occurs.

The key design goals are:

-  **O(1) acknowledgement.** Initiating a rollback completes in constant
   time at the MON, regardless of pool size or object count.
-  **Transparent to clients.** No changes are required to client
   applications. Clients continue to issue ordinary read and write I/Os
   to the head object; the PG handles rollback invisibly. No extra
   metadata needs to be passed with I/O requests.
-  **Deferred, distributed work.** The actual data movement (cloning the
   source snapshot back to the head) is deferred and is performed either
   just-in-time when an object is next written, or by the existing snap
   trimmer process extended to also process rollback work. Work is
   naturally distributed across all OSDs that host PGs for the pool, so
   throughput scales with cluster size.
-  **Correctness under stacked operations.** Multiple snapshots and
   rollbacks may be issued in any order. Each object’s I/O path
   correctly resolves the full pending operation list before servicing
   client I/O.
-  **Works with both pool-managed and unmanaged snapshots.** Two new
   pool operations mirror the existing create/delete pairs for each
   snapshot mode.

The purpose of implementing snapshot rollback at the RADOS layer is to
benefit both librbd and also future use by cephfs. See also
https://ibm-ceph.atlassian.net/browse/IBMCEPH-14567 whose requirements
can be satisifed by this design.

--------------

2. Background: How RADOS Snapshots Work Today
---------------------------------------------

2.1 Snapshot Modes
~~~~~~~~~~~~~~~~~~

RADOS supports two exclusive snapshot modes per pool:

-  **Pool-managed snaps** (``FLAG_POOL_SNAPS``): Snapshots are named and
   created by the MON. The ``pg_pool_t::snaps`` map records
   ``pool_snap_info_t`` entries keyed by ``snapid_t``. The head object’s
   ``SnapSet::seq`` records the highest snap sequence the object has
   been written at.
-  **Unmanaged (self-managed) snaps** (``FLAG_SELFMANAGED_SNAPS``): Snap
   IDs are allocated by the MON but the client manages which snap ID is
   active at write time via the ``SnapContext`` carried in the write
   message. Removed snaps are tracked in ``pg_pool_t::removed_snaps``
   (pre-Octopus) or in the OSDMap’s ``removed_snaps_queue`` (Octopus+).

2.2 SnapContext and SnapSet
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Every write operation carries a ``SnapContext``:

.. code:: cpp

   struct SnapContext {
     snapid_t seq;                     // current pool snap sequence
     std::vector<snapid_t> snaps;      // all existing snaps, descending order
   };

Each head object stores a ``SnapSet`` in the ``SS_ATTR`` xattr:

.. code:: cpp

   struct SnapSet {
     snapid_t seq;                                           // highest snapc.seq seen at write time
     std::vector<snapid_t> clones;                           // ascending clone snap IDs
     std::map<snapid_t, uint64_t> clone_size;
     std::map<snapid_t, interval_set<uint64_t>> clone_overlap;
     std::map<snapid_t, std::vector<snapid_t>> clone_snaps;  // per-clone snap coverage (descending)
   };

The ``clone_snaps`` field is important: multiple snapshots taken without
any intervening write to the object are all recorded against a **single
clone**. For example, if snaps 3, 4, and 5 are created without modifying
the object, only one clone object is created (named ``oid@3``, the
sequence at first write) and ``clone_snaps[3] = {5, 4, 3}`` covers all
three. This has consequences for rollback and is discussed in section
9.7.

2.3 Clone Creation on Write (``make_writeable()``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In ``PrimaryLogPG::make_writeable()``, before applying a write, the PG
checks:

.. code:: cpp

   if (snapc.snaps[0] > ctx->new_snapset.seq) {
       // create a clone: t->clone(coid, head)
   }

The clone object is named ``oid@snapc.seq``. After the clone, the head’s
``SnapSet::seq`` is updated to ``snapc.seq``. This is the sentinel used
by rollback to detect pending work.

2.4 Snaptrim
~~~~~~~~~~~~

2.4.1 Initiating a Snap Deletion
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When a snapshot is deleted the MON handles a ``POOL_OP_DELETE_SNAP`` or
``POOL_OP_DELETE_UNMANAGED_SNAP`` pool operation. In
``prepare_pool_op()`` it records the deleted snap ID in
``pending_inc.new_removed_snaps[pool_id]``, which is a
``snap_interval_set_t`` of snap IDs to be trimmed. This is committed to
the OSDMap by the Paxos proposal round.

``OSDMap::apply_incremental()`` merges ``new_removed_snaps`` into the
persistent ``removed_snaps_queue`` field (a per-pool
``snap_interval_set_t`` of all snap IDs that have been deleted but not
yet fully trimmed across all PGs):

.. code:: cpp

   // OSDMap::apply_incremental()
   for (auto p = new_removed_snaps.begin(); p != new_removed_snaps.end(); ++p) {
       removed_snaps_queue[p->first].union_of(p->second);
   }

Every OSD receives the updated OSDMap and, for each active primary PG,
``PG::on_active_advmap()`` runs. It reads
``osdmap->get_new_removed_snaps()`` and unions the new snap IDs into the
PG’s local ``snap_trimq``. ``kick_snap_trim()`` is then called to wake
the ``SnapTrimmer`` state machine if the PG is active, primary, and
clean.

2.4.2 The ``SnapTrimmer`` State Machine
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``SnapTrimmer`` is a ``boost::statechart`` state machine on
``PrimaryLogPG`` with the following states: ``NotTrimming``,
``Trimming/WaitReservation``, ``Trimming/AwaitAsyncWork``,
``Trimming/WaitRepops``, ``Trimming/WaitTrimTimer``, and
``Trimming/WaitRWLock``.

In ``AwaitAsyncWork`` the machine selects the lowest snap ID
``snap_to_trim`` from ``snap_trimq`` and calls
``snap_mapper.get_next_objects_to_trim(snap_to_trim, max)`` to obtain a
batch of objects that have a clone registered under that snap ID. For
each object it calls ``trim_object()``, which removes the clone from the
object store, updates the head’s ``SnapSet``, and emits PGLog entries.
The batch is submitted as a replicated transaction; the machine
transitions to ``WaitRepops`` until all replicas acknowledge, then back
to ``AwaitAsyncWork`` for the next batch.

When ``get_next_objects_to_trim()`` returns ``nullopt`` (no more objects
for ``snap_to_trim``), the PG has finished trimming that snap:

.. code:: cpp

   // PrimaryLogPG.cc -- AwaitAsyncWork nullopt branch
   pg->snap_trimq.erase(snap_to_trim);
   pg->recovery_state.adjust_purged_snaps(
       [snap_to_trim](auto& purged_snaps) {
           purged_snaps.insert(snap_to_trim);
       });
   pg->write_if_dirty(t);
   pg->recovery_state.share_pg_info();

``adjust_purged_snaps()`` sets ``dirty_big_info = true``, which causes
the next ``share_pg_info()`` call to push the updated ``pg_info_t``
(containing the new ``purged_snaps`` entry) to replicas and -- via
``MPGStats`` -- to the mgr.

2.4.3 MON Aggregation: ``try_prune_purged_snaps()``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The mgr collects ``pg_stat_t`` from every PG’s primary OSD. Each
``pg_stat_t`` carries a ``purged_snaps`` interval set (capped to
``osd_max_snap_prune_intervals_per_epoch`` intervals to bound message
size). The mgr assembles these into a ``PGMap`` and calls
``PGMap::encode_digest()`` → ``calc_purged_snaps()`` before sending its
digest to the MON.

``calc_purged_snaps()`` computes, per pool, the **intersection** of
``pg_stat_t::purged_snaps`` across every PG:

.. code:: cpp

   // PGMap::calc_purged_snaps()
   for (auto& [pgid, stat] : pg_stat) {
       if (stat.state == 0) {
           // PG state unknown -- exclude entire pool
           unknown.insert(pgid.pool());
           purged_snaps.erase(pgid.pool());
           continue;
       } else if (unknown.count(pgid.pool())) {
           continue;
       }
       auto j = purged_snaps.find(pgid.pool());
       if (j == purged_snaps.end()) {
           purged_snaps[pgid.pool()] = stat.purged_snaps;  // seed
       } else {
           j->second.intersection_of(stat.purged_snaps);   // narrow
       }
   }

A snap ID only appears in the result if **every** PG in the pool reports
it as purged, and no PG is in the unknown state. This is the key safety
invariant: the MON will never remove a snap from ``removed_snaps_queue``
until every active PG has confirmed the trim is complete.

``OSDMonitor::try_prune_purged_snaps()`` is called from
``OSDMonitor::tick()`` on every proposal cycle. It reads the digest’s
``purged_snaps``, then for each pool intersects the digest result with
``osdmap.removed_snaps_queue`` (to skip snap IDs that have already been
pruned in a previous epoch) and writes the remaining IDs to
``pending_inc.new_purged_snaps[pool_id]``. It also applies several
production-quality guards:

-  **Readability guard:** bails if the mgr stat digest is not yet
   readable.
-  **Idempotency guard:** bails if ``pending_inc.new_purged_snaps`` is
   already non-empty (already pruned once this epoch).
-  **Per-epoch cap:** limits total snap IDs pruned per epoch to
   ``mon_max_snap_prune_per_epoch`` to prevent oversized Paxos
   proposals.
-  **Already-pruned skip:** iterates ``removed_snaps_queue`` as the
   authoritative source; if a snap ID from the digest is no longer
   present in the queue (it was pruned in a prior epoch), it is silently
   skipped. This handles stale digest entries from PGs that have not yet
   refreshed their ``purged_snaps`` after a prior prune.

2.4.4 OSDMap Broadcast: Removing Snaps from ``removed_snaps_queue``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When the Paxos round commits, ``OSDMonitor::encode_pending()`` writes
two records to the monitor store atomically with the OSDMap increment:

1. The whole ``new_purged_snaps`` map is stored under a per-epoch key
   ``"purged_epoch_<hex>"`` in ``OSD_SNAP_PREFIX``. This enables booting
   OSDs to catch up on epochs they missed (§2.4.6).
2. Each individual snap range is merged into the per-pool purged-snap
   index (via ``insert_purged_snap_update()``), the authoritative record
   used to answer ``MMonGetPurgedSnaps`` queries.

The OSDMap increment carries ``new_purged_snaps``. When every node calls
``OSDMap::apply_incremental()``, the increment is applied:

.. code:: cpp

   // OSDMap::apply_incremental()
   new_purged_snaps = inc.new_purged_snaps;    // store transient per-epoch delta
   for (auto p = new_purged_snaps.begin(); p != new_purged_snaps.end(); ++p) {
       auto q = removed_snaps_queue.find(p->first);
       ceph_assert(q != removed_snaps_queue.end());  // must still be present
       q->second.subtract(p->second);               // REMOVE from the queue
       if (q->second.empty())
           removed_snaps_queue.erase(q);
   }

``new_purged_snaps`` is therefore the **delta for this epoch only** --
not a cumulative set. ``removed_snaps_queue`` shrinks as snap IDs are
confirmed purged across the cluster. A snap ID is only present in
``new_purged_snaps`` in the single epoch it transitions from pending to
complete; in all subsequent epochs the OSDMap carries no record of it
(the snap has been fully removed from OSDMap state and it is the
SnapMapper on each OSD that records completions locally).

2.4.5 PG Response to ``new_purged_snaps``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When each OSD receives the new OSDMap, every active-primary PG’s peering
state machine fires ``PeeringState::Active::react(AdvMap)``, which calls
``PG::on_active_advmap(osdmap)``. That function reads
``osdmap->get_new_purged_snaps()`` and removes those snap IDs from the
PG’s own ``pg_info_t::purged_snaps``:

.. code:: cpp

   // PG::on_active_advmap()
   const auto& new_purged_snaps = osdmap->get_new_purged_snaps();
   auto j = new_purged_snaps.find(get_pgid().pgid.pool());
   if (j != new_purged_snaps.end()) {
       for (auto k : j->second) {
           recovery_state.adjust_purged_snaps(
               [&k](auto& purged_snaps) {
                   purged_snaps.erase(k.first, k.second);
               });
       }
   }

This is the **feedback loop** that prevents the same snap IDs from being
re-reported in future ``pg_stat_t`` messages. After
``adjust_purged_snaps()`` sets ``dirty_big_info = true``, the
``Active::react(AdvMap)`` handler calls ``share_pg_info()`` to push
fresh stats immediately.

The snap IDs being erased from ``pg_info_t::purged_snaps`` on each PG
are precisely those that the MON just removed from
``removed_snaps_queue``, so the OSDMap and per-PG state remain
consistent.

2.4.6 OSD Boot-Time Catch-Up
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

An OSD that was offline when ``new_purged_snaps`` was broadcast will not
have recorded those snap IDs in its local ``SnapMapper``. Without
catch-up it would incorrectly believe those snaps still require trimming
and might attempt to re-trim objects whose clones have already been
deleted.

The catch-up mechanism works as follows. The OSD superblock stores
``purged_snaps_last``: the newest OSDMap epoch whose
``new_purged_snaps`` have been written to the local ``SnapMapper``. In
``OSD::_send_boot()``, before the OSD boots, this field is compared to
``superblock.current_epoch``:

.. code:: cpp

   // OSD::_send_boot()
   if (monmap.min_mon_release >= ceph_release_t::octopus &&
       superblock.purged_snaps_last < superblock.current_epoch) {
       _get_purged_snaps();
       return;  // defer boot until catch-up completes
   }

``_get_purged_snaps()`` sends
``MMonGetPurgedSnaps(purged_snaps_last + 1, current_epoch + 1)`` to the
MON. The MON handles this in ``preprocess_get_purged_snaps()``, scanning
the ``"purged_epoch_*"`` keys written by ``encode_pending()`` (§2.4.4)
and replying with
``MMonGetPurgedSnapsReply(start, last, purged_snaps_map)``. The reply is
capped at ~1 MiB; if more epochs remain, ``last < current_epoch`` and
the OSD fires another request.

On receipt of the reply, ``handle_get_purged_snaps_reply()`` calls
``SnapMapper::record_purged_snaps()`` to write the per-epoch data into
the local ``SnapMapper`` key-space, advances
``superblock.purged_snaps_last = m->last``, and persists the updated
superblock. When ``purged_snaps_last == current_epoch`` the OSD calls
``start_boot()``.

For OSDs that are online and receiving real-time OSDMap increments,
``handle_osd_map()`` records ``new_purged_snaps`` inline in the same
transaction that stores the new OSDMap, keyed by the ``purged_snaps[e]``
local map populated in the epoch-iteration loop:

.. code:: cpp

   // OSD::handle_osd_map() -- inline recording
   if (superblock.purged_snaps_last == start - 1) {
       SnapMapper::record_purged_snaps(cct, osdriver,
           osdriver.get_transaction(&t), purged_snaps);
       superblock.purged_snaps_last = last;
   }

This ensures that ``purged_snaps_last`` always advances with each new
OSDMap batch and the boot-time catch-up path is never needed for an OSD
that has been continuously online.

--------------

3. Data Structure Additions
---------------------------

3.1 ``rollback_snaps`` in ``pg_pool_t`` *(new)*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A rollback is described by a pair: a freshly allocated ``snapid_t`` (the
*rollback ID*, analogous to the snap sequence of a newly created
snapshot) and the ``snapid_t`` of the *source snapshot* whose content
should be restored. The rollback ID is monotonically increasing and is
drawn from the same ``snap_seq`` counter that governs snapshot creation,
ensuring a globally consistent ordering of all snapshot and rollback
events.

.. code:: cpp

   // src/osd/osd_types.h -- additions to pg_pool_t

   struct rollback_snap_info_t {
     snapid_t rollback_id;   // unique ID allocated for this rollback (from snap_seq)
     snapid_t source_snap;   // the snapshot to restore from
     utime_t  stamp;         // wall-clock time the rollback was issued

     void encode(ceph::buffer::list &bl) const;
     void decode(ceph::buffer::list::const_iterator &p);
   };
   WRITE_CLASS_ENCODER(rollback_snap_info_t)

   // Added to pg_pool_t:
   std::map<snapid_t, rollback_snap_info_t> rollback_snaps; // keyed by rollback_id

The choice to key the map by ``rollback_id`` (rather than
``source_snap``) is deliberate: the same source snapshot could legally
be rolled back more than once, and the rollback ID provides a stable,
unique handle for tracking completion and for ordering with respect to
other snapshots.

3.2 OSDMap Fields *(new)*
~~~~~~~~~~~~~~~~~~~~~~~~~

Rollback work is communicated from the MON to PGs via the same OSDMap
increment mechanism used for snaptrim, adding two new fields analogous
to ``new_removed_snaps`` and ``new_purged_snaps``:

.. code:: cpp

   // src/osd/OSDMap.h

   // In OSDMap::Incremental:
   mempool::osdmap::map<int64_t,   // pool id
     std::map<snapid_t, rollback_snap_info_t>> new_rollback_snaps;   // rollbacks added this epoch
   mempool::osdmap::map<int64_t,
     snap_interval_set_t> new_completed_rollbacks;                   // rollbacks finished this epoch

   // In OSDMap (mirrors removed_snaps_queue):
   mempool::osdmap::map<int64_t,
     std::map<snapid_t, rollback_snap_info_t>> rollback_snaps_queue; // all pending rollback work
   mempool::osdmap::map<int64_t,
     snap_interval_set_t> new_completed_rollbacks;                   // transient per epoch

``OSDMap::apply_incremental()`` merges ``new_rollback_snaps`` into
``rollback_snaps_queue`` (additions) and uses
``new_completed_rollbacks`` to remove completed entries, exactly as
``new_removed_snaps`` / ``new_purged_snaps`` are used for snaptrim.

3.3 ``pg_info_t`` Addition *(new)*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: cpp

   // src/osd/osd_types.h -- in pg_info_t
   snap_interval_set_t completed_rollbacks; // rollback IDs fully processed by this PG

This field mirrors ``purged_snaps`` and is reported to the MON via
``pg_stat_t``. The MON intersects it across all PGs in the pool to
determine when a rollback is globally complete and can be removed from
``rollback_snaps_queue``.

3.4 PG-Local Work Queue *(new)*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: cpp

   // src/osd/PG.h -- analogous to snap_trimq
   std::map<snapid_t, rollback_snap_info_t> rollback_trimq;  // pending per-PG rollback work

3.5 ``OSDSuperblock`` Addition *(new)*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: cpp

   // src/osd/osd_types.h -- in OSDSuperblock
   epoch_t completed_rollbacks_last = 0;  // newest epoch whose new_completed_rollbacks
                                          // have been recorded in the local SnapMapper
                                          // (mirrors purged_snaps_last)

This field is updated by ``OSD::handle_osd_map()`` each time
``new_completed_rollbacks`` from a batch of OSDMap epochs are written to
the local ``SnapMapper`` store. On boot, if
``completed_rollbacks_last < current_epoch``, the OSD queries the MON
for the missing epoch range before marking itself ready (§4.9).

--------------

4. New Pool Operations
----------------------

4.1 The Problem
~~~~~~~~~~~~~~~

RADOS pool operations -- snapshot creation and deletion -- are already
sent as ``MPoolOp`` messages to the MON, which validates the request,
updates the ``pg_pool_t`` inside the OSDMap, and replies to the client.
The rollback feature needs to hook into this same mechanism: the client
must be able to initiate a rollback with a single lightweight request,
receive an acknowledgement immediately (before any data has moved), and
have the resulting work description broadcast to all OSDs via the
OSDMap.

Rollbacks must be ordered correctly with respect to existing snapshots,
assigned a unique sequence number, and tracked until every PG in the
pool has completed the work. The design must also be idempotent: a
client that retries a rollback request after a timeout must receive the
same result without triggering duplicate work.

4.2 New Pool Op Codes *(new)*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Two new values are added to the pool operation enum in
``src/include/ceph_fs.h``:

.. code:: cpp

   // src/include/ceph_fs.h
   enum {
     POOL_OP_CREATE                    = 0x01,
     POOL_OP_DELETE                    = 0x02,
     POOL_OP_AUID_CHANGE               = 0x03,
     POOL_OP_CREATE_SNAP               = 0x11,
     POOL_OP_DELETE_SNAP               = 0x12,
     POOL_OP_CREATE_UNMANAGED_SNAP     = 0x21,
     POOL_OP_DELETE_UNMANAGED_SNAP     = 0x22,
     // NEW:
     POOL_OP_ROLLBACK_SNAP             = 0x31,  // pool-managed snapshot rollback
     POOL_OP_ROLLBACK_UNMANAGED_SNAP   = 0x32,  // unmanaged snapshot rollback
   };

The client message ``MPoolOp`` already carries a ``snapid`` field (used
by ``POOL_OP_DELETE_UNMANAGED_SNAP``) and a ``name`` string field (used
by ``POOL_OP_DELETE_SNAP``). No changes to the message format are
needed: the same fields are reused -- ``name`` identifies the target
snapshot for pool-managed rollback, and ``snapid`` identifies it for
unmanaged rollback.

4.3 Idempotency
~~~~~~~~~~~~~~~

Pool operations must be idempotent: if the same rollback request is
issued twice (for example due to a client retry after a network
timeout), the second request must complete successfully but must not
schedule any additional work.

Idempotency is achieved in ``preprocess_pool_op()``. Before the proposal
round, the preprocess handler checks whether a rollback of the requested
snapshot is already recorded in ``pg_pool_t::rollback_snaps``. If a
matching entry exists (same ``source_snap``), the preprocess handler
replies immediately with the existing ``rollback_id`` and returns
``true`` (handled), preventing any re-entry into the prepare phase:

.. code:: cpp

   // src/mon/OSDMonitor.cc -- inside preprocess_pool_op(), POOL_OP_ROLLBACK_SNAP case
   for (auto& [rb_id, rb] : pp.rollback_snaps) {
     if (rb.source_snap == source_snap_for_request) {
       // Already pending: return the existing rollback_id, no new work
       encode(rb_id, reply_data);
       _pool_op_reply(op, 0, osdmap.get_epoch(), &reply_data);
       return true;
     }
   }

This mirrors the existing idempotency behaviour for
``POOL_OP_CREATE_SNAP``, which returns success without modifying state
if the named snapshot already exists.

4.4 ``OSDMonitor::preprocess_pool_op()`` Changes *(modified)*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The preprocess step performs fast, read-only validation before the
proposal round. The following cases are added inside the existing switch
statement (idempotency check shown above is part of each case):

.. code:: cpp

   // src/mon/OSDMonitor.cc -- inside preprocess_pool_op()

   case POOL_OP_ROLLBACK_SNAP: {
     if (pp.is_unmanaged_snaps_mode()) {
       _pool_op_reply(op, -EINVAL, osdmap.get_epoch());
       return true;                        // handled: error
     }
     if (!pp.snap_exists(m->name.c_str())) {
       _pool_op_reply(op, -ENOENT, osdmap.get_epoch());
       return true;                        // snapshot does not exist
     }
     snapid_t source = pp.get_snap(m->name.c_str()).snapid;
     // Idempotency: already have a pending rollback of this snapshot?
     for (auto& [rb_id, rb] : pp.rollback_snaps) {
       if (rb.source_snap == source) {
         encode(rb_id, reply_data);
         _pool_op_reply(op, 0, osdmap.get_epoch(), &reply_data);
         return true;
       }
     }
     break;                                // fall through to prepare
   }

   case POOL_OP_ROLLBACK_UNMANAGED_SNAP: {
     if (pp.is_pool_snaps_mode()) {
       _pool_op_reply(op, -EINVAL, osdmap.get_epoch());
       return true;
     }
     if (m->snapid > pp.get_snap_seq()) {
       _pool_op_reply(op, -ENOENT, osdmap.get_epoch());
       return true;
     }
     if (_is_removed_snap(m->pool, m->snapid)) {
       _pool_op_reply(op, -ENOENT, osdmap.get_epoch()); // snap already deleted
       return true;
     }
     // Idempotency: already have a pending rollback of this snap ID?
     for (auto& [rb_id, rb] : pp.rollback_snaps) {
       if (rb.source_snap == m->snapid) {
         encode(rb_id, reply_data);
         _pool_op_reply(op, 0, osdmap.get_epoch(), &reply_data);
         return true;
       }
     }
     break;
   }

4.5 ``OSDMonitor::prepare_pool_op()`` Changes *(modified)*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The prepare step modifies ``pending_inc``, which is serialised and
applied to the OSDMap on proposal commit. The new cases allocate a
rollback ID from ``snap_seq`` and record the rollback in
``pg_pool_t::rollback_snaps`` and in the OSDMap increment:

.. code:: cpp

   // src/mon/OSDMonitor.cc -- inside prepare_pool_op()

   case POOL_OP_ROLLBACK_SNAP: {
     snapid_t source = pp.get_snap(m->name.c_str()).snapid;
     rollback_snap_info_t rb;
     rb.source_snap  = source;
     rb.rollback_id  = pp.get_snap_seq() + 1;
     rb.stamp        = ceph_clock_now();

     pp.snap_seq = rb.rollback_id;          // advance snap_seq (same as create_snap)
     pp.set_snap_epoch(pending_inc.epoch);
     pp.rollback_snaps[rb.rollback_id] = rb;

     pending_inc.new_pools[m->pool] = pp;
     pending_inc.new_rollback_snaps[m->pool][rb.rollback_id] = rb;

     encode(rb.rollback_id, reply_data);    // return allocated rollback ID to caller
     changed = true;
     break;
   }

   case POOL_OP_ROLLBACK_UNMANAGED_SNAP: {
     rollback_snap_info_t rb;
     rb.source_snap  = m->snapid;
     rb.rollback_id  = pp.get_snap_seq() + 1;
     rb.stamp        = ceph_clock_now();

     pp.snap_seq = rb.rollback_id;
     pp.set_snap_epoch(pending_inc.epoch);
     pp.rollback_snaps[rb.rollback_id] = rb;

     pending_inc.new_pools[m->pool]    = pp;
     pending_inc.new_rollback_snaps[m->pool][rb.rollback_id] = rb;

     encode(rb.rollback_id, reply_data);
     changed = true;
     break;
   }

The client reply is sent via the existing ``C_PoolOp`` callback and
``_pool_op_reply()`` mechanism, unchanged. The reply carries the
allocated rollback ID in ``reply_data``, so callers that want to poll
for completion have a handle to query.

4.6 MON Completion Detection: ``try_prune_completed_rollbacks()`` *(new)*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A new MON tick function, analogous to ``try_prune_purged_snaps()``, is
called from ``OSDMonitor::tick()``. It computes the intersection of
``completed_rollbacks`` reported by all PGs in the pool via the
``PGMapDigest`` and, once every PG has reported a given rollback ID as
complete, removes it from ``rollback_snaps_queue`` by emitting it in
``pending_inc.new_completed_rollbacks``.

The implementation mirrors ``try_prune_purged_snaps()`` in its
production-quality guards:

-  **Idempotency guard:** Returns immediately if
   ``pending_inc.new_completed_rollbacks`` is already non-empty -- the
   monitor already pruned during this epoch proposal round and must not
   double-prune.
-  **Readability guard:** Returns immediately if the mgr stat digest is
   not yet readable.
-  **Per-epoch cap:** Limits the total number of rollback IDs pruned in
   a single epoch to ``mon_max_snap_prune_per_epoch`` (reusing the same
   config key) to prevent runaway proposal sizes.
-  **Already-pruned check:** Before accepting a rollback ID from the
   digest, verifies that the rollback ID is still present in
   ``rollback_snaps_queue``. PGs may still be reporting a completion for
   an ID that was already pruned in a prior epoch; those stale reports
   are silently skipped.
-  **Unknown-state PG exclusion:** ``PGMap::calc_completed_rollbacks()``
   (§WI-4) already excludes any pool that has at least one PG with
   ``state == 0`` (unknown / not yet reported). The MON therefore never
   acts on a pool where any PG has not yet reported, providing the same
   conservative guarantee as ``calc_purged_snaps()``.

.. code:: cpp

   // src/mon/OSDMonitor.cc -- new function

   bool OSDMonitor::try_prune_completed_rollbacks()
   {
     if (!mon.mgrstatmon()->is_readable()) {
       return false;
     }
     if (!pending_inc.new_completed_rollbacks.empty()) {
       return false;  // already pruned for this epoch
     }

     unsigned max_prune = cct->_conf.get_val<uint64_t>(
       "mon_max_snap_prune_per_epoch");
     if (!max_prune) {
       max_prune = 100000;
     }
     dout(10) << __func__ << " max_prune " << max_prune << dendl;

     unsigned actually_pruned = 0;
     auto& completed = mon.mgrstatmon()->get_digest().completed_rollbacks;

     for (auto& [pool_id, pool_completed] : completed) {
       if (actually_pruned >= max_prune) {
         break;
       }
       auto r = osdmap.rollback_snaps_queue.find(pool_id);
       if (r == osdmap.rollback_snaps_queue.end()) {
         continue;  // no pending rollbacks for this pool
       }

       snap_interval_set_t to_prune;
       unsigned maybe_pruned = actually_pruned;

       for (auto& [rb_id, _] : r->second) {
         // pool_completed is the intersection across all PGs -- only IDs that
         // every PG has acknowledged are present here.
         if (!pool_completed.contains(rb_id)) {
           continue;
         }
         // Double-check: still present in rollback_snaps_queue (stale digest
         // entries from PGs that lag behind a prior prune are dropped here).
         // The map lookup above already confirmed the pool entry exists; the
         // rb_id key check inside r->second handles the per-ID staleness.
         to_prune.insert(rb_id);
         ++maybe_pruned;
         if (maybe_pruned >= max_prune) {
           break;
         }
       }

       if (!to_prune.empty()) {
         pending_inc.new_completed_rollbacks[pool_id].swap(to_prune);
         actually_pruned += pending_inc.new_completed_rollbacks[pool_id].size();
         dout(10) << __func__ << " pool " << pool_id
                  << " pruning completed rollbacks "
                  << pending_inc.new_completed_rollbacks[pool_id] << dendl;
       }
     }

     dout(10) << __func__ << " actually pruned " << actually_pruned << dendl;
     return !!actually_pruned;
   }

4.7 ``encode_pending()``: Persisting Completed Rollbacks for Boot Replay *(new)*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Analogous to the ``purged_snaps`` block in
``OSDMonitor::encode_pending()``, each epoch’s
``new_completed_rollbacks`` is written to the monitor store under a
per-epoch key so that booting OSDs can catch up on epochs they missed
while offline (§4.9). This happens in the same Paxos commit as the
OSDMap increment.

.. code:: cpp

   // src/mon/OSDMonitor.cc -- inside encode_pending(), in the purged_snaps block

   // completed_rollbacks -- persist per-epoch for OSD boot replay
   if (tmp.require_osd_release >= ceph_release_t::umbrella &&
       !pending_inc.new_completed_rollbacks.empty()) {
     // Store the full map for this epoch keyed as "completed_rollback_epoch_<hex>"
     string k = make_completed_rollback_epoch_key(pending_inc.epoch);
     bufferlist v;
     encode(pending_inc.new_completed_rollbacks, v);
     t->put(OSD_SNAP_PREFIX, k, v);
   }

The helper ``make_completed_rollback_epoch_key(epoch)`` returns a string
of the form ``"completed_rollback_epoch_%08x"`` -- a format that sorts
lexicographically by epoch, allowing efficient range scans during
catch-up queries:

.. code:: cpp

   static std::string make_completed_rollback_epoch_key(epoch_t e)
   {
     char buf[64];
     snprintf(buf, sizeof(buf), "completed_rollback_epoch_%08x", e);
     return buf;
   }

No per-rollback-ID index (analogous to ``insert_purged_snap_update``) is
required because rollback IDs are looked up exclusively by epoch range
during the boot catch-up path, not by ID.

4.8 ``preprocess_get_completed_rollbacks()``: MON Query Handler *(new)*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The monitor handles a new message
``MMonGetCompletedRollbacks(start, last)`` from booting OSDs, returning
all ``new_completed_rollbacks`` maps for epochs in ``[start, last]``.
This is the direct analogue of ``preprocess_get_purged_snaps()``.

.. code:: cpp

   // src/mon/OSDMonitor.cc -- new function

   bool OSDMonitor::preprocess_get_completed_rollbacks(MonOpRequestRef op)
   {
     op->mark_osdmon_event(__func__);
     auto m = op->get_req<MMonGetCompletedRollbacks>();
     dout(7) << __func__ << " " << *m << dendl;

     // epoch → { pool_id → snap_interval_set_t of completed rollback IDs }
     map<epoch_t,
         map<int64_t, snap_interval_set_t>> r;

     string k = make_completed_rollback_epoch_key(m->start);
     auto it = mon.store->get_iterator(OSD_SNAP_PREFIX);
     it->upper_bound(k);
     unsigned long epoch = m->last;
     int n = 0;

     while (it->valid()) {
       if (it->key().find("completed_rollback_epoch_") != 0) {
         break;
       }
       int parsed = sscanf(it->key().c_str(),
                           "completed_rollback_epoch_%lx", &epoch);
       if (parsed != 1) {
         derr << __func__ << " unable to parse key '" << it->key() << "'" << dendl;
       } else if (epoch > m->last) {
         break;
       } else {
         bufferlist bl = it->value();
         auto p = bl.cbegin();
         try {
           ceph::decode(r[epoch], p);
         } catch (ceph::buffer::error& e) {
           derr << __func__ << " unable to parse value for key '"
                << it->key() << "'" << dendl;
         }
         n += 4 + r[epoch].size() * 16;
       }
       if (n > 1048576) {
         // semi-arbitrary 1 MiB per-reply cap (same as purged_snaps handler)
         break;
       }
       it->next();
     }

     auto reply = make_message<MMonGetCompletedRollbacksReply>(m->start, epoch);
     reply->completed_rollbacks.swap(r);
     mon.send_reply(op, reply.detach());
     return true;
   }

The reply message ``MMonGetCompletedRollbacksReply`` carries: -
``start``: the first epoch requested. - ``last``: the last epoch
included in this reply (may be less than requested if the reply was
capped at 1 MiB). - ``completed_rollbacks``: the per-epoch map.

Two new message types are required:

================================== =========================== =========
Message                            Analogous to                Direction
================================== =========================== =========
``MMonGetCompletedRollbacks``      ``MMonGetPurgedSnaps``      OSD → MON
``MMonGetCompletedRollbacksReply`` ``MMonGetPurgedSnapsReply`` MON → OSD
================================== =========================== =========

4.9 OSD Boot-Time Catch-Up *(new)*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When an OSD starts up, it may have been offline for several OSDMap
epochs and therefore missed some ``new_completed_rollbacks`` increments.
Without replaying these, the OSD’s local ``SnapMapper`` would
incorrectly retain rollback source clone objects (thinking they are
still needed), interfering with the background rollback trimmer’s
correctness checks on restart.

The catch-up mechanism mirrors the ``purged_snaps_last`` /
``_get_purged_snaps()`` pattern exactly:

4.9.1 ``OSD::_send_boot()`` Check
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

During the pre-boot readiness check in ``OSD::_send_boot()``, after the
``purged_snaps_last`` check, an analogous guard is added:

.. code:: cpp

   // src/osd/OSD.cc -- inside _send_boot(), after purged_snaps_last check

   if (monmap.min_mon_release >= ceph_release_t::umbrella &&
       superblock.completed_rollbacks_last < superblock.current_epoch) {
     dout(10) << __func__ << " completed_rollbacks_last "
              << superblock.completed_rollbacks_last
              << " < newest_map " << superblock.current_epoch << dendl;
     _get_completed_rollbacks();
     return;
   }

4.9.2 ``OSD::_get_completed_rollbacks()``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: cpp

   // src/osd/OSD.cc -- new function

   void OSD::_get_completed_rollbacks()
   {
     // Stateless, may send overlapping requests; correctness guaranteed by
     // idempotent apply in handle_get_completed_rollbacks_reply().
     dout(10) << __func__
              << " completed_rollbacks_last " << superblock.completed_rollbacks_last
              << ", newest_map " << superblock.current_epoch << dendl;
     auto *m = new MMonGetCompletedRollbacks(
       superblock.completed_rollbacks_last + 1,
       superblock.current_epoch + 1);
     monc->send_mon_message(m);
   }

4.9.3 ``OSD::handle_get_completed_rollbacks_reply()``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: cpp

   // src/osd/OSD.cc -- new function

   void OSD::handle_get_completed_rollbacks_reply(
     MMonGetCompletedRollbacksReply *m)
   {
     dout(10) << __func__ << " " << *m << dendl;
     ObjectStore::Transaction t;

     if (!is_preboot() ||
         m->last < superblock.completed_rollbacks_last) {
       goto out;
     }

     {
       OSDriver osdriver{store.get(), service.meta_ch, make_purged_snaps_oid()};
       // record_completed_rollbacks() marks each completed rollback_id in the
       // SnapMapper so the background trimmer knows it no longer needs to
       // protect those source clone objects.
       SnapMapper::record_completed_rollbacks(
         cct,
         osdriver,
         osdriver.get_transaction(&t),
         m->completed_rollbacks);
     }

     superblock.completed_rollbacks_last = m->last;
     write_superblock(cct, superblock, t);
     store->queue_transaction(service.meta_ch, std::move(t));
     service.publish_superblock(superblock);

     if (m->last < superblock.current_epoch) {
       _get_completed_rollbacks();   // more epochs to fetch
     } else {
       start_boot();                 // fully caught up
     }

   out:
     m->put();
   }

4.9.4 ``OSD::handle_osd_map()`` -- Inline Recording
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For OSDs that are online and receiving OSDMap increments in real time,
the catch-up request is never needed because each epoch’s
``new_completed_rollbacks`` is recorded inline in ``handle_osd_map()``,
exactly as ``new_purged_snaps`` is today:

.. code:: cpp

   // src/osd/OSD.cc -- inside handle_osd_map(), after purged_snaps block

   // record new completed_rollbacks
   if (superblock.completed_rollbacks_last == start - 1) {
     OSDriver osdriver{store.get(), service.meta_ch, make_purged_snaps_oid()};
     SnapMapper::record_completed_rollbacks(
       cct,
       osdriver,
       osdriver.get_transaction(&t),
       completed_rollbacks);   // map<epoch_t, map<int64_t,snap_interval_set_t>>
                               // populated in the same epoch-iteration loop as
                               // purged_snaps (see handle_osd_map() §8374)
     superblock.completed_rollbacks_last = last;
   } else {
     dout(10) << __func__
              << " superblock completed_rollbacks_last is "
              << superblock.completed_rollbacks_last
              << ", not recording new completed_rollbacks" << dendl;
   }

The ``completed_rollbacks`` local map (parallel to the existing
``purged_snaps`` local map in ``handle_osd_map()``) is populated in the
same epoch-iteration loop:

.. code:: cpp

   completed_rollbacks[e] = o->get_new_completed_rollbacks();

where ``OSDMap::get_new_completed_rollbacks()`` returns
``new_completed_rollbacks`` for that epoch -- analogous to
``get_new_purged_snaps()``.

4.9.5 ``SnapMapper::record_completed_rollbacks()``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: cpp

   // src/osd/SnapMapper.cc -- new function

   void SnapMapper::record_completed_rollbacks(
     CephContext *cct,
     OSDriver& driver,
     OSDriver::OSTransaction& t,
     const map<epoch_t,
               map<int64_t, snap_interval_set_t>>& completed_rollbacks)
   {
     for (auto& [epoch, pool_map] : completed_rollbacks) {
       for (auto& [pool_id, rb_ids] : pool_map) {
         for (auto i = rb_ids.begin(); i != rb_ids.end(); ++i) {
           snapid_t rb_id = i.get_start();
           snapid_t rb_end = i.get_start() + i.get_len();
           while (rb_id < rb_end) {
             // Mark this rollback_id as completed in the local SnapMapper
             // key-space so the trimmer can verify its completion state on
             // restart without querying the MON.
             set_completed_rollback(driver, t, pool_id, rb_id);
             ++rb_id;
           }
         }
       }
     }
   }

The ``set_completed_rollback()`` helper writes a small key-value record
under a key of the form ``"completed_rb_<pool_hex>_<rb_id_hex>"`` into
the same ``OSD_SNAP`` object store namespace used by the existing
purged-snaps record. The background rollback trimmer reads this on
startup to skip rollbacks that are already globally confirmed complete
(without needing to wait for an OSDMap that includes
``new_completed_rollbacks`` for the relevant pool).

4.9.6 Safety Guarantee
^^^^^^^^^^^^^^^^^^^^^^

The same conservative guarantee that governs ``purged_snaps`` applies
here:

   A rollback ID is only removed from ``rollback_snaps_queue`` after the
   ``PGMapDigest`` records it in the intersection across **all** PGs in
   the pool (§4.6 / WI-4). Any PG with ``state == 0`` blocks the entire
   pool. Therefore no OSD can see ``new_completed_rollbacks`` for a
   rollback ID before every PG that is active has confirmed the rollback
   is complete.

An OSD that was offline during the pruning and boots later will catch up
via §4.9.2–§4.9.3, receiving the same ``new_completed_rollbacks`` data
from the monitor’s persistent store before it starts booting PGs. This
ensures the OSD never presents an inconsistent view of which rollbacks
are in progress.

--------------

5. Write I/O Path: Just-in-Time Rollback
----------------------------------------

.. _the-problem-1:

5.1 The Problem
~~~~~~~~~~~~~~~

When a client writes to a RADOS object and there is a pending rollback
for the pool, the head object must first be brought to the state it
should have at the time of the write -- that is, the state resulting from
applying all snapshots and rollbacks that have been issued since the
object was last written. If this work is not done before the write, the
SnapSet on the head will be inconsistent with the pool’s snap/rollback
history: future reads and clone operations will produce incorrect
results, and the background snap trimmer may delete clone objects that
should still be visible.

The just-in-time (JIT) path resolves all pending work for an object as
part of the first write to that object after any rollback, before the
new client data is applied. This keeps the write path as the single
point of truth for per-object snapshot state, and ensures objects that
are actively written never accumulate a backlog of unprocessed rollback
work.

5.2 Where the Check Lives
~~~~~~~~~~~~~~~~~~~~~~~~~

The existing snapshot clone logic lives in
``PrimaryLogPG::make_writeable()`` (``PrimaryLogPG.cc``). This is the
natural place to integrate rollback processing: it already holds the
object context, has access to the ``SnapContext``, and is responsible
for issuing all clone operations into the ``PGTransaction`` before the
client write is applied. Rollback handling is inserted immediately after
the existing clone-creation block.

5.3 Detecting Pending Rollback Work
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The head object’s ``SnapSet::seq`` records the highest pool snap
sequence the object has been written at. The pool’s
``rollback_snaps_queue`` (available via ``get_osdmap()``) contains all
rollback IDs that are greater than the object’s current
``SnapSet::seq``. Any such entry represents work that must be applied
before the incoming write:

.. code:: cpp

   // Pseudocode in make_writeable(), after existing clone creation

   snapid_t obj_seq = ctx->new_snapset.seq;
   auto& rb_queue = get_osdmap()->get_rollback_snaps_queue();
   auto pool_it   = rb_queue.find(info.pgid.pgid.pool());

   if (pool_it != rb_queue.end()) {
     // Collect all rollbacks with rollback_id > obj_seq, sorted ascending
     for (auto& [rb_id, rb_info] : pool_it->second) {
       if (rb_id > obj_seq) {
         pending_rollbacks.push_back(rb_info);  // sorted by rb_id already (std::map)
       }
     }
   }

5.4 Resolving the Full Operation Sequence
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Between ``obj_seq`` (the last processed sequence) and ``snapc.seq`` (the
current pool sequence) there may be a mixture of ordinary snapshot
creations and rollbacks in any order. The PG must resolve them all in
the correct historical order before applying the client write.

The algorithm walks the ``pg_pool_t::snaps`` map and ``rollback_snaps``
map together, processing each event in ascending ``snapid_t`` order:

.. code:: cpp

   struct pending_op_t {
     enum Type { SNAP, ROLLBACK } type;
     snapid_t id;       // snap ID or rollback ID
     snapid_t source;   // for ROLLBACK: the source snapshot
   };

   // Collect all operations with id in (obj_seq, snapc.seq], sorted by id
   std::vector<pending_op_t> ops;
   for (auto& [snap_id, _] : pp.snaps) {
     if (snap_id > obj_seq && snap_id <= snapc.seq)
       ops.push_back({SNAP, snap_id, CEPH_NOSNAP});
   }
   for (auto& [rb_id, rb] : pp.rollback_snaps) {
     if (rb_id > obj_seq && rb_id <= snapc.seq)
       ops.push_back({ROLLBACK, rb_id, rb.source_snap});
   }
   std::sort(ops.begin(), ops.end(), [](auto& a, auto& b){ return a.id < b.id; });

5.5 Generating Clone Transactions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The resolved operation list is then collapsed into the minimum set of
clone operations. The key insight is that consecutive operations on the
head can be optimised: only the *last* write to a destination matters,
so intermediate writes to the head are elided when a subsequent rollback
overwrites them.

   **Transaction constraint:** ``ObjectStore`` and ``EC`` do not permit
   a ``clone(dst, src)`` and a ``write(dst)`` in the same transaction.
   Therefore clone operations are always emitted in a first transaction,
   and the client write in a second transaction. This is different from
   snapshots where it is possible to combine a clone with the snapshot
   as a destination and a write to the head within the transaction. As
   long as the update to the SnapSet stored in the SS_ATTR is part of
   the 1st transaction there are no problems with breaking this update
   into two separate transactions (with the possibility that an
   interruption means only the 1st transaction is committed).

For each ``pending_op_t`` in order, compute the current logical head
state and emit clone operations:

.. code:: cpp

   // Transaction 1: all clones
   // "head_source" tracks which object currently holds the logical head content
   hobject_t head_source = soid;              // initially the actual head object

   for (int i = 0; i < ops.size(); ++i) {
     const auto& op = ops[i];

     if (op.type == SNAP) {
       // Clone head_source -> soid@op.id to preserve the logical head content
       hobject_t dst = soid;  dst.snap = op.id;
       t1->clone(dst, head_source);
       // head_source unchanged: we cloned FROM it, not to it

     } else { // ROLLBACK
       hobject_t src_clone = soid;  src_clone.snap = op.source;

       // Clone source_snap -> head
       t1->clone(soid, src_clone);            // head now holds source content
       head_source = src_clone;              // future SNAPs clone from here

       // Consume any immediately following SNAPs, cloning directly from the
       // source rather than via the head (optimisation)
       for (int j = i+1; j < ops.size() && ops[j].type == SNAP; ++j) {
         hobject_t dst = soid;  dst.snap = ops[j].id;
         t1->clone(dst, src_clone);
         ++i;  // consumed
       }
     }
   }

   // Transaction 2: client write to head
   t2->write(soid, ...);

As part of the 1st transaction the head’s ``SnapSet::seq`` is updated to
``snapc.seq``, exactly as in the normal write path.

5.6 PGLog Entries
~~~~~~~~~~~~~~~~~

The rollback processing in transaction 1 produces two kinds of log
entry, written as part of the same transaction so the log is always
consistent with the object store state.

**CLONE entries -- one per new clone created.** Each clone operation
(``t1->clone(dst, src)``) is recorded as a ``pg_log_entry_t::CLONE``
entry for the destination object ``dst``. The ``snaps`` buffer in the
entry is encoded with the vector of snap IDs that the new clone covers
(taken from ``clone_snaps[dst.snap]`` after the SnapSet is updated).
This matches exactly the CLONE entries produced by the normal
``make_writeable()`` path for snapshot-triggered clones, so
``PG::update_snap_map()`` can process them without modification, calling
``snap_mapper.add_oid(dst, snaps)`` to register each new clone.

**MODIFY entry -- one for the head object.** After all clone operations
in transaction 1 (which may include copying a source snapshot into the
head), the updated head ``SnapSet`` (with the new ``seq``, ``clones``,
``clone_snaps``, etc.) is written to the ``SS_ATTR`` xattr. This change
to the head object is recorded as a ``pg_log_entry_t::MODIFY`` entry for
the head ``soid``. The MODIFY entry carries the updated
``object_info_t`` in ``OI_ATTR`` and serves as the record that the head
has been brought up to date with the pending rollback(s).

Transaction 2 (the actual client write) then produces its own normal
MODIFY entry for the head, as it would for any write.

A summary of the log entry sequence for a JIT rollback write:

::

   Transaction 1:
     pg_log_entry_t::CLONE  for each newly created clone (one per clone op)
     pg_log_entry_t::MODIFY for the head  (SnapSet + OI updated to snapc.seq)

   Transaction 2:
     pg_log_entry_t::MODIFY for the head  (client data + version bump)

5.7 SnapSet and SnapMapper Updates
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``SnapSet::clones`` vector and the associated metadata maps
(``clone_size``, ``clone_overlap``, ``clone_snaps``) are updated for
each new clone created in transaction 1, using the same bookkeeping
already present in ``make_writeable()`` for snapshot-triggered clones.
The ``SnapSet::seq`` is advanced to ``snapc.seq`` as part of transaction
1. The snap mapper is updated via the existing ``PG::update_snap_map()``
path processing the CLONE log entries described above. No changes to the
snap mapper itself are required.

--------------

6. Read I/O Path: Redirect to Source Clone
------------------------------------------

.. _the-problem-2:

6.1 The Problem
~~~~~~~~~~~~~~~

When a rollback is pending but the head object has not yet been written
(so the just-in-time path has not run), a read of the head object would
return stale pre-rollback content. The client expects to read the state
of the object as it existed at the rollback snapshot.

The read path handles this purely by **redirecting the read to the
appropriate source clone** without performing any rollback work. No
clones are created and no SnapSets are modified by a read. All deferred
work remains deferred; the redirect is a lightweight, read-only
operation that simply routes the I/O to the correct existing object.

.. _where-the-check-lives-1:

6.2 Where the Check Lives
~~~~~~~~~~~~~~~~~~~~~~~~~

Read operations enter ``PrimaryLogPG::do_op()``, which looks up the
object context via ``get_object_context()`` and then calls
``do_read()``. The redirect logic is inserted at the start of the
read-path handling, after the object context is obtained but before
``do_read()`` is called.

6.3 Redirect Logic
~~~~~~~~~~~~~~~~~~

.. code:: cpp

   // Pseudocode inside PrimaryLogPG::do_op(), read path

   if (op_is_read(m->get_op())) {
     snapid_t obj_seq   = obc->ssc->snapset.seq;
     snapid_t rb_source = find_latest_rollback_source(
                            get_osdmap(), info.pgid.pgid.pool(), obj_seq);

     if (rb_source != CEPH_NOSNAP) {
       hobject_t redirect_oid = soid;
       redirect_oid.snap = rb_source;

       ObjectContextRef redirect_obc = get_object_context(redirect_oid, false);
       if (!redirect_obc || !redirect_obc->obs.exists) {
         // Object did not exist at the snapshot; treat as non-existent
         osd->reply_op_error(op, -ENOENT);
         return;
       }
       obc  = redirect_obc;
       soid = redirect_oid;
       // Proceed with normal do_read() against the clone
     }
   }

The helper ``find_latest_rollback_source()`` scans
``rollback_snaps_queue`` for the pool and returns the ``source_snap`` of
the rollback with the highest ``rollback_id`` greater than ``obj_seq``.
When multiple stacked rollbacks are pending only the most recent one
matters for reads -- each subsequent rollback supersedes the previous.

   **Design intent:** the most recent pending rollback defines the
   authoritative state of every object. If a stacked earlier rollback
   would have had a valid clone source but the most recent rollback’s
   source clone does not exist on this object (§6.4), the correct result
   is still ``ENOENT`` -- the pool was most recently rolled back to a
   point in time where the object did not exist, so from the client’s
   perspective the object does not exist. Test coverage: the
   stacked-rollback ENOENT case is an explicit scenario in WI-9-e.

.. code:: cpp

   snapid_t find_latest_rollback_source(const OSDMapRef& osdmap,
                                        int64_t pool_id,
                                        snapid_t obj_seq)
   {
     auto it = osdmap->rollback_snaps_queue.find(pool_id);
     if (it == osdmap->rollback_snaps_queue.end()) return CEPH_NOSNAP;

     snapid_t latest_source = CEPH_NOSNAP;
     for (auto& [rb_id, rb_info] : it->second) {
       if (rb_id > obj_seq) {
         latest_source = rb_info.source_snap;  // last entry (map is ascending) wins
       }
     }
     return latest_source;
   }

6.4 Object Not in Snapshot
~~~~~~~~~~~~~~~~~~~~~~~~~~

If the source clone does not exist (i.e. the object was created after
the snapshot was taken but before the rollback), reading the head while
a rollback is pending should semantically return the state at the
rollback snapshot -- which means the object does not exist. The correct
response is ``-ENOENT``.

6.5 Pending Snapshot Followed Immediately by a Rollback of That Snapshot
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Consider the case where a snapshot is created and then immediately
rolled back before any object is written:

::

   Create snap S    (rollback_snaps_queue contains nothing new yet; snaps[S] added)
   Rollback snap S  (rollback_id=R, source=S added to rollback_snaps_queue)

For any object whose ``SnapSet::seq < S``, both events are pending. When
the read redirect logic runs it finds rollback R with
``source_snap = S``. It then looks up the clone for snap S -- but no
clone for snap S exists on this object (the object has not been written
since before snap S was created, so snap S would share an existing clone
from before S, or the head itself represents S).

More precisely: if the object has ``SnapSet::seq < S`` then it has not
been written since before S, meaning reading snap S is equivalent to
reading the current head. The rollback to S is therefore a no-op for
this object -- the head already contains the correct data.

The redirect logic handles this correctly without special-casing:
``rb_source = S`` is the snap ID to redirect to. The existing clone
lookup for snap S falls back to the head (since ``SnapSet::seq < S``
means the head already is the snap-S state), so ``do_read()`` proceeds
against the head object as normal.

At write time, the JIT rollback path will similarly find no clone to
copy from for snap S and will skip the clone operation for this
rollback, only updating ``SnapSet::seq`` to reflect that rollback R has
been processed.

6.6 ORDERSNAP Interaction
~~~~~~~~~~~~~~~~~~~~~~~~~

The existing ``CEPH_OSD_OP_FLAG_ORDERSNAP`` check rejects writes whose
``snapc.seq`` is less than ``SnapSet::seq``. Because rollback IDs are
allocated from the same ``snap_seq`` counter, a client that obtained its
``SnapContext`` before the rollback was issued will have a ``snapc.seq``
less than the rollback ID. This is detected by the existing ORDERSNAP
guard and the client must refresh its ``SnapContext`` and retry, exactly
as it would if a new snapshot had been created concurrently.

--------------

7. Background Rollback via Extended Snap Trimmer
------------------------------------------------

.. _the-problem-3:

7.1 The Problem
~~~~~~~~~~~~~~~

For objects that are never written after a rollback is issued, the
just-in-time path (section 5) never fires. Reads to these objects will
continue to be redirected to read the snapshot that is being rolled
back, but the the stale pre-rollback content is left in the head object
consuming unnecessary storage. A background sweep is therefore required
to bring every object in the pool to the correct post-rollback state.

A naive implementation would introduce a separate background trimmer
exclusively for rollback, but this creates a problem: the existing snap
trimmer must not delete a clone that is still needed as a rollback
source, and a separate process would require coordination between the
two. It would also double the scanning and scheduling infrastructure for
what is fundamentally a similar per-object operation.

Instead, the existing ``SnapTrimmer`` state machine is extended to also
process rollback work. This is the natural fit because:

-  The snap trimmer already has all required infrastructure: per-object
   iteration via ``snap_mapper.get_next_objects_to_trim()``, OSD-level
   reservation and throttling, the ``WaitRepops``/``WaitTrimTimer``
   replication cycle, and the ``PGSnapTrim`` scheduler work item.
-  Rollback and snaptrim work are mutually constrained: a source clone
   must not be deleted until the rollback that reads from it is
   complete. Running both operations in the same state machine, in the
   same per-object pass, enforces this ordering without any additional
   coordination.
-  The MON communication model -- OSDMap increments to broadcast work,
   ``pg_info_t`` reporting from PGs, MON intersection to detect
   completion -- is identical for both operations, allowing the same code
   paths to be reused.

7.2 Interaction Between Snaptrim and Rollback
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each work cycle of the extended snap trimmer operates on a single snap
ID ``X``, scanning all objects in the PG that have a clone registered
under ``X``. Within that scan, rollback work and trim work are
**interleaved per object**: for each object encountered, any pending
rollback is applied first, then the trim deletion is performed (if this
is a trim pass). This maximises I/O efficiency because both operations
touch the same object and its SnapSet in the same transaction, and
avoids the need for a separate second scan over the same set of objects.

7.2.1 Pass Mode: Trim or Rollback-only
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Each cycle is classified as one of two *pass modes* for snap ID ``X``:

+-----------------------+-----------------------+-----------------------+
| Mode                  | Condition             | Object work           |
+=======================+=======================+=======================+
| **Trim pass**         | ``X`` is in           | Apply rollback (if    |
|                       | ``snap_trimq`` (snap  | pending for this      |
|                       | deletion pending)     | object) **then**      |
|                       |                       | delete clone ``X``    |
+-----------------------+-----------------------+-----------------------+
| **Rollback-only       | ``X`` is **not** in   | Apply rollback only;  |
| pass**                | ``snap_trimq`` but is | do not delete clone   |
|                       | the ``source_snap``   | ``X``                 |
|                       | of a pending rollback |                       |
+-----------------------+-----------------------+-----------------------+

When ``X`` is simultaneously in ``snap_trimq`` and is a pending rollback
source, the trim pass handles both in a single scan. Rollback work is
always done first for each object so that the clone being deleted is no
longer needed as a rollback source at the point of deletion.

7.2.2 Choosing Snap ID X and Pass Mode: Fairness
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

At the start of each ``AwaitAsyncWork`` cycle, the trimmer selects which
snap ID to work on next. Both ``snap_trimq`` and ``rollback_trimq``
contribute candidates. The selection uses a simple **lowest-ID-first**
fairness rule:

::

   candidates = {}

   if snap_trimq is non-empty:
       candidates.add(snap_trimq.range_start(), mode=TRIM)

   for each rb in rollback_trimq (ascending rb_id):
       candidates.add(rb.source_snap, mode=ROLLBACK_ONLY)
       // Note: if rb.source_snap is already in candidates as TRIM, upgrade to TRIM
       // (trim pass handles rollback too)

   X, mode = candidates.lowest_snap_id()

Because rollback IDs are allocated from ``snap_seq`` (always greater
than any pre-existing snap ID), ``snap_trimq`` candidates naturally tend
to be lower IDs than ``rollback_trimq`` candidates. However, the unified
lowest-ID selection ensures that if a rollback’s ``source_snap`` is a
lower ID than the next snap to trim, the rollback is processed first --
preventing the trimmer from ever deleting a source clone before its
rollback data has been propagated to the head.

7.2.3 Combined Per-Object Operation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Within the scan for snap ID ``X``, each object is processed as follows:

::

   for each object OBJ with a clone registered under snap X:

       pending_rollback = find rollback in rollback_trimq where source_snap == X
                          AND OBJ has SnapSet::seq < rb_id

       if pending_rollback:
           apply_rollback_to_object(OBJ, rb_id)     // section 7.5

       if mode == TRIM:
           trim_object(OBJ, X)                       // existing trim_object() logic

Both operations write to the same object in the same
``OpContextUPtr``/transaction, submitted together via
``simple_opc_submit()``. This means a single replication round-trip
covers both the rollback clone operations and the snap deletion, and the
object is touched only once per pass.

7.2.4 Completion
^^^^^^^^^^^^^^^^

**Rollback completion** for snap X: after the snap mapper returns
``nullopt`` for snap X (no more objects to process), all background
rollback work for X has been applied via ``rollback_then_trim()``.
However, some objects may have had their rollback work initiated earlier
by the JIT write path (§5). To prevent a race where the background
trimmer declares completion before in-flight JIT transactions commit,
the trimmer must wait for all such transactions to complete before
marking the rollback done.

The interlock works as follows: before adding ``rb_id`` to
``pg_info_t::completed_rollbacks``, the trimmer checks whether there are
any in-flight ``OpContext`` objects that were started by the JIT path
with a pending rollback for ``rb_id``. These are tracked via a per-PG
counter ``jit_rollback_inflight[rb_id]`` that is incremented when the
JIT path begins a rollback transaction and decremented in its completion
callback. The trimmer’s ``nullopt`` handler:

.. code:: cpp

   // In AwaitAsyncWork::react(DoSnapWork), nullopt branch
   if (rb_info) {
     if (pg->jit_rollback_inflight.count(rb_id) &&
         pg->jit_rollback_inflight[rb_id] > 0) {
       // JIT work still in flight -- defer completion; requeue a check
       pg->rollback_trimq_repeat.insert(rb_id);
       // transition back to WaitTrimTimer instead of marking complete
     } else {
       pg->rollback_trimq.erase(rb_id);
       pg->jit_rollback_inflight.erase(rb_id);
       pg->recovery_state.adjust_completed_rollbacks(
         [rb_id](auto& cr) { cr.insert(rb_id); });
       pg->write_if_dirty(t);
       pg->recovery_state.share_pg_info();
     }
   }

The JIT path decrements ``jit_rollback_inflight[rb_id]`` (and kicks the
trimmer if it was waiting) in the ``on_success`` callback of transaction
1, guaranteeing that the count reaches zero only after all replica
replications of the rollback have completed.

**Trim completion** for snap X: after the scan returns ``nullopt``,
``trim_object()`` has deleted all clones for X. The snap mapper performs
its normal verification scan to confirm all objects for X are gone, then
X is removed from ``snap_trimq`` and added to
``pg_info_t::purged_snaps``, exactly as in the existing trim path.

``snap_trimq_repeat`` is retained unchanged for its existing purposes
(lock contention, scrub interference). The new ``rollback_trimq_repeat``
serves the analogous purpose for the JIT-inflight check described above.

7.3 PG Work Queue Population
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When a PG activates or receives an updated OSDMap, it reads the pool’s
``rollback_snaps_queue`` from the OSDMap and populates its local
``rollback_trimq``, subtracting any rollbacks already recorded in
``pg_info_t::completed_rollbacks``:

.. code:: cpp

   // src/osd/PeeringState.cc -- in activate() and on_active_advmap()
   auto& rb_queue = osdmap->get_rollback_snaps_queue();
   auto pool_it   = rb_queue.find(info.pgid.pgid.pool());
   if (pool_it != rb_queue.end()) {
     for (auto& [rb_id, rb_info] : pool_it->second) {
       if (!info.completed_rollbacks.contains(rb_id)) {
         pg->rollback_trimq[rb_id] = rb_info;
       }
     }
   }

When ``new_rollback_snaps`` arrives in a new OSDMap increment, the PG
adds the new entries to ``rollback_trimq``. When
``new_completed_rollbacks`` arrives it removes the corresponding entries
(work confirmed complete cluster-wide).

The existing ``kick_snap_trim()`` is extended to also check
``rollback_trimq``:

.. code:: cpp

   void PrimaryLogPG::kick_snap_trim() {
     if (is_active() && is_primary() && is_clean() &&
         !state_test(PREMERGE) &&
         (!snap_trimq.empty() || !rollback_trimq.empty())) {  // extended
       if (!osdmap->test_flag(NOSNAPTRIM)) {
         snap_trimmer_machine.process_event(KickTrim());
       }
     }
   }

7.4 Extended ``SnapTrimmer`` State Machine
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The existing state machine states (``NotTrimming``,
``Trimming/WaitReservation``, ``Trimming/AwaitAsyncWork``,
``Trimming/WaitRepops``, ``Trimming/WaitTrimTimer``,
``Trimming/WaitRWLock``) are all unchanged. The extension adds two new
data members to the ``Trimming`` state to record the current pass
selection:

.. code:: cpp

   // src/osd/PrimaryLogPG.h -- in struct Trimming
   snapid_t snap_being_processed;  // the snap ID X selected for this pass
   bool     is_trim_pass;          // true = TRIM mode; false = ROLLBACK_ONLY mode

The ``AwaitAsyncWork::react(DoSnapWork)`` handler is modified at its
entry to run the fairness selection (section 7.2.2) and record the
result in these fields, then proceeds with the interleaved per-object
loop (section 7.5). All other state transitions
(``WaitRepops → WaitTrimTimer → AwaitAsyncWork``) are unchanged: each
``AwaitAsyncWork`` cycle processes one batch of objects for the
currently selected ``(snap_being_processed, is_trim_pass)`` pair, and
the cycle repeats until the snap mapper returns ``nullopt``.

7.5 Per-Object Work: Interleaved Rollback and Trim
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each ``AwaitAsyncWork`` batch uses
``snap_mapper.get_next_objects_to_trim(X, max)`` to obtain up to ``max``
objects that have a clone under snap ID ``X``. The same call is used
regardless of pass mode; the difference lies in what is done with each
object.

.. code:: cpp

   // In AwaitAsyncWork::react(DoSnapWork) -- the interleaved per-object loop

   snapid_t X        = snap_being_processed;
   bool     do_trim  = is_trim_pass;

   // Find the pending rollback whose source_snap == X, if any
   rollback_snap_info_t* rb_info = find_rollback_for_source(pg->rollback_trimq, X);

   auto to_process = pg->snap_mapper.get_next_objects_to_trim(X, max);

   if (!to_process.has_value()) {
     // Scan complete for snap X

     if (rb_info) {
       // Rollback work is done for X: signal completion (section 7.5)
       pg->rollback_trimq.erase(rb_info->rollback_id);
       pg->recovery_state.adjust_completed_rollbacks(
         [rb_id](auto& cr) { cr.insert(rb_id); });
       pg->write_if_dirty(t);
       pg->recovery_state.share_pg_info();
     }

     if (do_trim) {
       // snap_mapper performs its standard verification scan to confirm all
       // clones for X have been deleted (existing behaviour, unchanged).
       // Then X is removed from snap_trimq and added to purged_snaps.
       pg->snap_trimq.erase(X);
       pg->recovery_state.adjust_purged_snaps(
         [X](auto& ps) { ps.insert(X); });
       pg->write_if_dirty(t);
       pg->recovery_state.share_pg_info();
     }

     // Post KickTrim to start next pass
   } else {
     for (auto& obj : *to_process) {
       OpContextUPtr ctx = rollback_then_trim(obj, X, rb_info, do_trim);
       in_flight.insert(obj);
       ctx->register_on_success(/* decrement in_flight, post RepopsComplete */);
       simple_opc_submit(std::move(ctx));
     }
     // transition to WaitRepops
   }

The per-object function ``rollback_then_trim()`` combines rollback and
trim work in a single ``OpContextUPtr``/transaction:

.. code:: cpp

   OpContextUPtr PrimaryLogPG::rollback_then_trim(
     const hobject_t& soid,
     snapid_t X,
     const rollback_snap_info_t* rb_info,  // null if no rollback pending for X
     bool do_trim)
   {
     ObjectContextRef obc = get_object_context(soid, false);
     OpContextUPtr ctx    = simple_opc_create(obc);
     PGTransaction* t     = ctx->op_t.get();

     // Step 1: Apply rollback work for this object (if pending)
     if (rb_info && obc->ssc->snapset.seq < rb_info->rollback_id) {
       // Build pending_op_t list for (obj_seq, rb_id] -- identical to section 5.4
       auto ops = build_pending_ops(obc->ssc->snapset.seq, rb_info->rollback_id);
       if (!ops.empty()) {
         execute_clone_plan(t, soid, ops);       // section 5.5 -- shared with JIT path
         update_snapset_for_rollback(ctx.get(), ops);
         // Emits CLONE entries for new clones + MODIFY for head (section 5.6)
       }
     }

     // Step 2: Trim clone X from this object (if trim pass)
     if (do_trim) {
       trim_object_snap(ctx.get(), soid, X);
       // trim_object_snap() is the per-object portion of the existing trim_object()
       // logic: removes X from SnapSet::clone_snaps, deletes clone if no snaps
       // remain, updates clone_overlap, emits DELETE or MODIFY log entry.
     }

     finish_ctx(ctx.get(), pg_log_entry_t::MODIFY);
     return ctx;
   }

There is scope for reusing a lot of the rollback code on the JIT write
path (section 5) to implement the background rollback process.

7.6 PG Completion Signalling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When the rollback trimmer finishes processing all objects in the PG for
a given rollback ID, the PG:

1. Erases ``rb_id`` from ``rollback_trimq``.
2. Adds ``rb_id`` to ``pg_info_t::completed_rollbacks``.
3. Calls ``recovery_state.share_pg_info()`` to propagate the update to
   replicas and to the MON.
4. Writes the updated ``pg_info_t`` to the object store.

7.7 MON Aggregation and OSDMap Update
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The MON receives ``completed_rollbacks`` via ``pg_stat_t`` in
``MPGStats`` messages. ``PGMap::calc_completed_rollbacks()`` (new,
analogous to ``calc_purged_snaps()``) computes the intersection across
all PGs in the pool. When every PG in the pool has reported a given
``rb_id`` as complete, ``OSDMonitor::try_prune_completed_rollbacks()``
emits ``pending_inc.new_completed_rollbacks[pool_id]``.

When this OSDMap increment is applied:

-  ``OSDMap::apply_incremental()`` removes the completed rollback ID
   from ``rollback_snaps_queue``.
-  Each PG receives the update in its next OSDMap processing pass and
   removes the completed entry from ``rollback_trimq``.
-  The pool’s ``pg_pool_t::rollback_snaps`` map is updated (the entry is
   removed in ``pending_inc.new_pools``).

**End-to-end data flow:**

::

   MON issues ROLLBACK_SNAP pool op
            |
            v
    pending_inc.new_rollback_snaps[pool][rb_id] = rb_info
    pg_pool_t::rollback_snaps[rb_id] = rb_info
    snap_seq += 1  (rb_id allocated from snap_seq counter)
            |
            v  (OSDMap broadcast)
    OSDMap::rollback_snaps_queue[pool][rb_id] = rb_info
            |
       +----|-----------------------------------+
       |    v  (PG activation / OSDMap update)  |
       | PG::rollback_trimq[rb_id] = rb_info    |  (repeated for all PGs)
       |    |                                   |
       |    +---> Just-in-time (on write):      |
       |    |       make_writeable() resolves   |
       |    |       pending ops then writes     |
       |    |                                   |
       |    +---> Extended SnapTrimmer          |
       |    |     (background, interleaved):    |
       |    |       rollback_then_trim() per    |
       |    |       object in one pass          |
       |    v                                   |
       | pg_info_t::completed_rollbacks += rb_id|
       | share_pg_info() --> MON via pg_stat_t  |
       +----------------------------------------+
            |
            v  (MON tick: try_prune_completed_rollbacks)
    intersection across all PGs == rb_id completed
            |
            v
    pending_inc.new_completed_rollbacks[pool] += rb_id
            |
            v  (OSDMap broadcast)
    rollback_snaps_queue[pool].erase(rb_id)
    pg_pool_t::rollback_snaps.erase(rb_id)

7.8 Throttling
~~~~~~~~~~~~~~

Both rollback and trim work run inside the existing ``SnapTrimmer``
reservation and scheduling framework via the same
``OSDService::queue_for_snap_trim()`` enqueue point. No additional
throttling mechanism is required. Because rollback and trim work are now
combined per object in ``rollback_then_trim()``, each scheduler work
item covers both operations at once, keeping cost accounting accurate.

--------------

8. Worked Scenarios
-------------------

8.1 Simple Rollback
~~~~~~~~~~~~~~~~~~~

+-----------------------------------+-----------------------------------+
| Step                              | State                             |
+===================================+===================================+
| Initial state                     | Clone@1 (snap 1, contents A);     |
|                                   | Head (contents B,                 |
|                                   | ``SnapSet::seq``\ =1)             |
+-----------------------------------+-----------------------------------+
| ``ROLLBACK_SNAP snap=1``          | rollback_id=2 allocated.          |
|                                   | ``rollback_                       |
|                                   | snaps_queue[pool][2]={source=1}`` |
+-----------------------------------+-----------------------------------+
| **Read head**                     | ``obj_seq``\ =1, rollback pending |
|                                   | (``rb_id``\ =2 >                  |
|                                   | ``obj_seq``\ =1). Read is         |
|                                   | redirected to clone@1. Returns A. |
|                                   | No rollback work is performed.    |
+-----------------------------------+-----------------------------------+
| **Write D to head**               | JIT rollback fires. Pending ops:  |
|                                   | [ROLLBACK id=2, source=1]. Clone  |
|                                   | clone@1(A)→head.                  |
|                                   | ``SnapSet::seq``\ =2. Write D.    |
|                                   | Head now contains D.              |
+-----------------------------------+-----------------------------------+
| **Another write E**               | ``obj_seq``\ =2 == ``snapc.seq``. |
|                                   | No pending rollback. Normal       |
|                                   | write. Head contains E.           |
+-----------------------------------+-----------------------------------+

..

   After rollback ID 2 is allocated, ``snap_seq`` advances to 2. Snap 1
   continues to exist and its clone (clone@1, contents A) is preserved.
   The rollback copies clone@1 back to the head in-place at write time.
   The snap 1 clone remains available for future reads. The snap trimmer
   does not delete clone@1 until rollback 2 is marked complete.

8.2 Stacked Snapshots and Rollbacks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Setup:**

+-----------------------+--------------------+-----------------------+
| Event                 | ``snap_seq`` after | Description           |
+=======================+====================+=======================+
| Initial state         | 2                  | Clone@1 (A), Clone@2  |
|                       |                    | (B), Head (C, seq=2)  |
+-----------------------+--------------------+-----------------------+
| Create snap 3         | 3                  | ``snaps[3]`` added    |
+-----------------------+--------------------+-----------------------+
| Rollback snap 1       | 4                  | ``rollback_           |
| (id=4)                |                    | snaps[4]={source=1}`` |
+-----------------------+--------------------+-----------------------+
| Create snap 5         | 5                  | ``snaps[5]`` added    |
+-----------------------+--------------------+-----------------------+
| Rollback snap 2       | 6                  | ``rollback_           |
| (id=6)                |                    | snaps[6]={source=2}`` |
+-----------------------+--------------------+-----------------------+
| Create snap 7         | 7                  | ``snaps[7]`` added    |
+-----------------------+--------------------+-----------------------+

**Write D arrives at head** (``obj_seq``\ =2, ``snapc.seq``\ =7).

Pending ops in (2,7] sorted by ID:

1. SNAP id=3
2. ROLLBACK id=4, source=1 (content A)
3. SNAP id=5
4. ROLLBACK id=6, source=2 (content B)
5. SNAP id=7

Resolved clone plan:

+-----------------------+-----------------------+-----------------------+
| Op                    | Action                | ``head_source`` after |
+=======================+=======================+=======================+
| SNAP 3                | clone head(C) →       | head (C)              |
|                       | clone@3               |                       |
+-----------------------+-----------------------+-----------------------+
| ROLLBACK 4 (src=1)    | clone clone@1(A) →    | clone@1 (A)           |
|                       | head;                 |                       |
|                       | ``he                  |                       |
|                       | ad_source``\ =clone@1 |                       |
+-----------------------+-----------------------+-----------------------+
| SNAP 5                | clone clone@1(A) →    | clone@1 (A)           |
|                       | clone@5 (direct,      |                       |
|                       | skipping head)        |                       |
+-----------------------+-----------------------+-----------------------+
| ROLLBACK 6 (src=2)    | clone clone@2(B) →    | clone@2 (B)           |
|                       | head;                 |                       |
|                       | ``he                  |                       |
|                       | ad_source``\ =clone@2 |                       |
+-----------------------+-----------------------+-----------------------+
| SNAP 7                | clone clone@2(B) →    | clone@2 (B)           |
|                       | clone@7 (direct)      |                       |
+-----------------------+-----------------------+-----------------------+

**Transaction 1** (all clones, single ``ObjectStore`` transaction):

.. code:: cpp

   // ObjectStore::clone(dst, src) convention: first arg is destination
   t.clone(clone@3, head)     // dst=clone@3, src=head  → preserves C
   t.clone(head,   clone@1)   // dst=head, src=clone@1  → restores A
   t.clone(clone@5, clone@1)  // dst=clone@5, src=clone@1  → A (skip head)
   t.clone(head,   clone@2)   // dst=head, src=clone@2  → restores B
   t.clone(clone@7, clone@2)  // dst=clone@7, src=clone@2  → B (skip head)

**Transaction 2:**

.. code:: cpp

   t.write(head, D)           // client data
   // head SnapSet::seq = 7

8.3 Multiple Snapshots Sharing One Clone
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

RADOS only creates a new clone when the head object is actually written.
If multiple snapshots are taken without any intervening write, all those
snap IDs are recorded in a single clone’s ``clone_snaps`` entry rather
than creating separate clone objects per snapshot.

**Scenario:**

::

   Initial state: Head (contents A, SnapSet::seq=0, no clones)

   Create snap 1   (snap_seq=1, no clone yet -- object not written)
   Create snap 2   (snap_seq=2)
   Create snap 3   (snap_seq=3)

   Write B to head

At the write, ``snapc.snaps[0]=3 > SnapSet::seq=0``, so one clone is
created:

::

   Clone@3  (contents A, clone_snaps[3]={3, 2, 1})
   Head     (contents B, SnapSet::seq=3)

All three snap IDs 1, 2, and 3 map to the single clone object ``oid@3``.

**Consequence for rollback -- read path:**

::

   Rollback snap 2  (rollback_id=4, source_snap=2)
   Read head

The read redirect resolves ``rb_source = 2`` and looks up the clone for
snap 2 in the SnapSet. The existing clone lookup (``SnapSet::clones``
binary search) finds that snap 2 falls within
``clone_snaps[3]={3,2,1}``, so the read is redirected to ``oid@3``,
which holds content A. This is correct: snap 2 was a snapshot of content
A.

**Consequence for rollback -- write path:**

::

   Write C to head

The JIT path resolves ops in (3, 4]: ``[ROLLBACK id=4, source=2]``. It
needs to locate the clone for snap 2, which is ``oid@3``. It clones
``oid@3 → head`` (restoring A) and writes C. The source clone ``oid@3``
is not deleted -- it is still needed for snaps 1, 2, and 3.

**Consequence for rollback of a snap with no dedicated clone:**

Rollback always copies from the ``source_snap``\ ’s *logical content*,
not from a named clone. The clone lookup for the source snap
(``find_latest_rollback_source`` returns the snap ID, and the code then
looks up the clone object that covers that snap ID via
``SnapSet::clone_snaps``) handles multi-snap clones transparently. No
special-casing is required.

**Rollback of snap 1 vs snap 2 vs snap 3 in this scenario:**

All three rollbacks redirect to the same clone (``oid@3``) and therefore
restore the same content A. This is semantically correct: snaps 1, 2,
and 3 all captured content A (the object was not modified between them).

--------------

9. Edge Cases and Constraints
-----------------------------

9.1 Rolling Back to a Deleted Snapshot
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The MON validates that the source snapshot exists at the time the
rollback is issued. If the snapshot is subsequently deleted before all
rollback work is complete, both the rollback and the snaptrim for that
snap will be present in their respective queues (``rollback_trimq`` and
``snap_trimq``). The interleaved pass design from section 7.2 handles
this directly: the fairness selection (section 7.2.2) will choose snap
ID S with mode TRIM (since S is in both ``snap_trimq`` and is a rollback
source, the trim pass mode takes precedence). Within the scan,
``rollback_then_trim()`` performs the rollback work first for each
object, then deletes the clone -- so by the time a clone is deleted, the
rollback data has already been copied to the head. No deferral mechanism
is required.

9.2 Object Created After Snapshot
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   **This section was found to be incomplete.** The claim that "the
   background trimmer skips such objects" is correct for the
   snap-mapper-driven phase, but the original design omitted a
   necessary second phase. Without a post-scan head-deletion sweep,
   objects created after the snapshot and never written during the
   rollback window reappear after rollback completion. Section 18
   describes the corrected design and implementation.

If an object does not exist in the source snapshot (no clone exists for
that snap ID), a read redirect returns ``ENOENT``. The snap-mapper-driven
phase of the background trimmer skips such objects (the snap mapper
returns no entries for them). The JIT path on write creates no rollback
clone but still updates ``SnapSet::seq`` to record that rollback has been
processed for this object.

However, if no write is ever issued to such an object during the rollback
window, the head remains live. When the rollback is declared complete
and removed from ``rollback_snaps_queue``, the read redirect ceases and
the object becomes visible again -- incorrectly. The fix (section 18) is
a post-scan head-deletion sweep that runs after the snap-mapper scan
exhausts all objects with clones for the source snap, and deletes any
head whose ``SnapSet::seq < rb_id`` and which has no clone for the
source snap.

9.3 Rollback of Unmanaged Snaps
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For unmanaged snapshots the snap ID is supplied by the client in the
``MPoolOp`` message (``snapid`` field). The same
``rollback_snap_info_t`` structure and processing apply. The MON
validates against ``removed_snaps_queue`` (to ensure the snap has not
already been trimmed) rather than the ``snaps`` name map.

9.4 Recovery and Backfill Interaction
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

During recovery, the replica applies the same ``PGTransaction`` as the
primary. Clone operations generated by the rollback path are ordinary
``ObjectStore`` clone operations and are replicated without special
handling. The snap mapper on replicas is updated via the existing
``update_snap_map()`` path from log entries.

9.5 Concurrent Rollbacks
~~~~~~~~~~~~~~~~~~~~~~~~

Because each rollback is assigned a unique, monotonically increasing ID
from ``snap_seq``, multiple concurrent rollbacks on the same pool are
naturally serialised by the ordering of IDs. The per-object resolution
algorithm (section 5.4) processes all pending operations in ID order and
produces a deterministic result regardless of when each rollback was
issued.

9.6 Encoding and Compatibility
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``pg_pool_t`` uses versioned encoding. The ``rollback_snaps`` field is
added under a new encoding version so that older OSDs that do not
understand the field can still decode pools that do not use rollbacks.
An OSD that does not support rollback must refuse to become primary for
a pool that has a non-empty ``rollback_snaps_queue``; this is enforced
via the standard ``require_osd_release`` / ``min_compat_client``
mechanism.

9.7 Snap Immediately Created and Then Rolled Back (NOP Rollback)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

As described in section 6.5, if a snapshot S is created and then rolled
back to via rollback ID R before any object in the pool is written, the
rollback is effectively a NOP for every such object. For each object,
``SnapSet::seq < S``, which means the object already represents the
state at snap S. The rollback work reduces to: update ``SnapSet::seq``
to R and record ``completed_rollbacks += R``. No clone operations are
emitted. The background trimmer recognises this via the snap mapper
returning no objects for ``source_snap = S`` (there are no clones for S
because no objects were written after S was taken), and immediately
marks the rollback complete for the PG.

--------------

10. Software Upgrade and Compatibility
--------------------------------------

10.1 Feature Release
~~~~~~~~~~~~~~~~~~~~

Pool-level snapshot rollback is introduced in the **Umbrella** release.
The feature depends on new ``pg_pool_t`` fields, new ``OSDMap``
incremental types, and new PG state-machine behaviour that are not
present in earlier releases. A mixed cluster where some MONs or OSDs are
running pre-Umbrella code would not be able to correctly process or
replicate the new pool operations.

10.2 Gating on ``require_osd_release``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``OSDMap`` field ``require_osd_release`` (type ``ceph_release_t``,
declared in ``src/osd/OSDMap.h``) records the minimum OSD release that
the cluster administrator has asserted is running everywhere. The MON
advances this field only after confirming that all OSDs have booted on
the new release; it is therefore a reliable cluster-wide gate.

The new pool-op cases in ``preprocess_pool_op()`` check this field as
the first validation step, before any other logic:

.. code:: cpp

   // src/mon/OSDMonitor.cc -- inside preprocess_pool_op(), new cases

   case POOL_OP_ROLLBACK_SNAP:
   case POOL_OP_ROLLBACK_UNMANAGED_SNAP: {
     if (osdmap.require_osd_release < ceph_release_t::umbrella) {
       _pool_op_reply(op, -EPERM, osdmap.get_epoch());
       return true;   // handled: cluster is not yet fully upgraded
     }
     // … remainder of validation (mode check, snap exists, idempotency) …
   }

Returning ``-EPERM`` mirrors the convention used for other release-gated
operations (e.g. the tentacle guard on ``allow_ec_optimizations`` in
``OSDMonitor::prepare_set_flag()``). The client receives this error code
and can translate it into a meaningful diagnostic message (see section
12.3).

The ``prepare_pool_op()`` handler for the two new cases also carries the
same guard as a defence-in-depth check, since ``prepare_pool_op()`` is
only reached after ``preprocess_pool_op()`` returns ``false`` (i.e. did
not handle the request). Under normal operation the preprocess guard
fires first; the prepare guard is a safety net for any future code path
that could bypass preprocess.

10.3 Encoding Compatibility
~~~~~~~~~~~~~~~~~~~~~~~~~~~

``pg_pool_t``, ``pg_info_t``, and ``pg_stat_t`` use versioned encoding
(see section 3). The new fields are written under a new encoding version
and are skipped by older decoders that do not recognise the version. An
older OSD decoding a ``pg_pool_t`` with ``rollback_snaps`` present will
simply ignore the field (the struct version check will treat it as
trailing unknown data). This is safe because the OSD will also have
refused the primary role (section 11.3), so it will never act on the
data it cannot decode.

``OSDMap::Incremental`` carries the new ``new_rollback_snaps`` and
``new_completed_rollbacks`` fields under a bumped incremental encoding
version. An older OSD applying the incremental will stop decoding at the
previous version boundary and will not see the new fields, which is
consistent with its inability to process rollback work.

--------------

11. RADOS API Changes
---------------------

11.1 Overview
~~~~~~~~~~~~~

The new pool-level snapshot rollback operation must be exposed through
all layers of the RADOS client API stack: the ``Objecter`` layer in
``src/osdc/`` dispatches the pool op to the MON; the internal
``IoCtxImpl`` layer wraps it; the librados C and C++ public APIs expose
it to callers; and the neorados C++ API adds it alongside the equivalent
snap create/delete operations. The naming convention follows the
existing snap remove APIs exactly:

+-----------------------------------+-----------------------------------+
| Existing API                      | New counterpart                   |
+===================================+===================================+
| ``IoCtx::snap_remove(name)``      | ``IoCtx::sn                       |
|                                   | ap_rollback(name, &rollback_id)`` |
+-----------------------------------+-----------------------------------+
| ``IoC                             | ``IoCtx::selfmanaged_             |
| tx::selfmanaged_snap_remove(id)`` | snap_rollback(id, &rollback_id)`` |
+-----------------------------------+-----------------------------------+
| ``ra                              | ``rados_ioctx_snap_r              |
| dos_ioctx_snap_remove(io, name)`` | ollback(io, name, &rollback_id)`` |
+-----------------------------------+-----------------------------------+
| ``rados_ioctx_                    | ``rados_ioctx_selfmanaged_snap    |
| selfmanaged_snap_remove(io, id)`` | _rollback(io, id, &rollback_id)`` |
+-----------------------------------+-----------------------------------+
| ``RADOS::dele                     | ``RADOS::rollback_pool_snap(p     |
| te_pool_snap(pool, name, token)`` | ool, name, &rollback_id, token)`` |
+-----------------------------------+-----------------------------------+
| ``RADOS::delete_se                | `                                 |
| lfmanaged_snap(pool, id, token)`` | `RADOS::rollback_selfmanaged_snap |
|                                   | (pool, id, &rollback_id, token)`` |
+-----------------------------------+-----------------------------------+

All new APIs return ``rollback_id`` (an allocated ``snapid_t`` /
``uint64_t``) which can be used by callers that wish to poll for
completion.

11.2 Objecter Layer (``src/osdc/Objecter``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Two new methods are added to ``Objecter``, directly paralleling
``create_pool_snap()`` / ``delete_pool_snap()`` and
``allocate_selfmanaged_snap()`` / ``delete_selfmanaged_snap()``:

.. code:: cpp

   // src/osdc/Objecter.h -- new declarations alongside existing pool-snap methods

   void rollback_pool_snap(int64_t pool, std::string_view snapName,
                           decltype(PoolOp::onfinish)&& onfinish);
   void rollback_pool_snap(int64_t pool, std::string_view snapName,
                           Context *onfinish) {
     rollback_pool_snap(pool, snapName,
                        OpContextVert<ceph::buffer::list>(onfinish, nullptr));
   }

   void rollback_selfmanaged_snap(int64_t pool, snapid_t snap,
                                  decltype(PoolOp::onfinish)&& onfinish);
   void rollback_selfmanaged_snap(int64_t pool, snapid_t snap,
                                  Context *onfinish) {
     rollback_selfmanaged_snap(pool, snap,
                               OpContextVert<ceph::buffer::list>(onfinish, nullptr));
   }

The implementation in ``src/osdc/Objecter.cc`` calls the private
``_pool_op()`` helper with the appropriate opcode:

.. code:: cpp

   // src/osdc/Objecter.cc

   void Objecter::rollback_pool_snap(int64_t pool, std::string_view snapName,
                                     decltype(PoolOp::onfinish)&& onfinish)
   {
     _pool_op(POOL_OP_ROLLBACK_SNAP, pool, std::string(snapName), 0,
              std::move(onfinish));
   }

   void Objecter::rollback_selfmanaged_snap(int64_t pool, snapid_t snap,
                                            decltype(PoolOp::onfinish)&& onfinish)
   {
     _pool_op(POOL_OP_ROLLBACK_UNMANAGED_SNAP, pool, {}, snap,
              std::move(onfinish));
   }

The ``onfinish`` callback receives a ``ceph::buffer::list`` containing
the encoded ``rollback_id`` in its reply data, which upper layers decode
and return to callers.

11.3 Internal Layer (``src/librados/IoCtxImpl``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Two new methods are added to ``IoCtxImpl``, each using the same
synchronous mutex-and-condition-variable pattern as ``snap_remove()``
and ``selfmanaged_snap_remove()``:

.. code:: cpp

   // src/librados/IoCtxImpl.h -- new declarations

   int snap_rollback(const char* snapName, uint64_t *rollback_id);
   int selfmanaged_snap_rollback(uint64_t snap_id, uint64_t *rollback_id);

.. code:: cpp

   // src/librados/IoCtxImpl.cc -- implementations

   int librados::IoCtxImpl::snap_rollback(const char *snapName,
                                          uint64_t *rollback_id)
   {
     ceph::mutex mylock = ceph::make_mutex("IoCtxImpl::snap_rollback::mylock");
     ceph::condition_variable cond;
     bool done = false;
     int reply = 0;
     ceph::buffer::list reply_bl;

     objecter->rollback_pool_snap(poolid, snapName,
       new C_SafeCond(mylock, cond, &done, &reply, &reply_bl));

     std::unique_lock l{mylock};
     cond.wait(l, [&done] { return done; });

     if (reply == 0 && rollback_id) {
       auto iter = reply_bl.cbegin();
       decode(*rollback_id, iter);
     }
     return reply;
   }

   int librados::IoCtxImpl::selfmanaged_snap_rollback(uint64_t snap_id,
                                                      uint64_t *rollback_id)
   {
     ceph::mutex mylock =
       ceph::make_mutex("IoCtxImpl::selfmanaged_snap_rollback::mylock");
     ceph::condition_variable cond;
     bool done = false;
     int reply = 0;
     ceph::buffer::list reply_bl;

     objecter->rollback_selfmanaged_snap(poolid, snap_id,
       new C_SafeCond(mylock, cond, &done, &reply, &reply_bl));

     std::unique_lock l{mylock};
     cond.wait(l, [&done] { return done; });

     if (reply == 0 && rollback_id) {
       auto iter = reply_bl.cbegin();
       decode(*rollback_id, iter);
     }
     return reply;
   }

11.4 librados C++ API (``src/include/rados/librados.hpp``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Two new methods are added to ``librados::IoCtx``, following the same
pattern as ``snap_remove()`` and ``selfmanaged_snap_remove()``:

.. code:: cpp

   // src/include/rados/librados.hpp -- in class IoCtx

   /// Initiate a pool-level snapshot rollback (pool-managed snaps).
   /// Completes in O(1); background work is performed by OSDs.
   /// @param snapname  name of the existing pool snapshot to restore
   /// @param rollback_id  [out] allocated rollback ID (for completion polling)
   /// @returns 0 on success, -EPERM if cluster is not fully upgraded to Umbrella,
   ///          -ENOENT if snapname does not exist, -EINVAL if pool is in
   ///          selfmanaged-snap mode
   int snap_rollback(const std::string& snapname, uint64_t *rollback_id);

   /// Initiate a pool-level snapshot rollback (selfmanaged snaps).
   /// @param snap_id   the selfmanaged snap ID to restore from
   /// @param rollback_id  [out] allocated rollback ID (for completion polling)
   /// @returns 0 on success, -EPERM if cluster not upgraded, -ENOENT if snap
   ///          already deleted, -EINVAL if pool is in pool-managed-snap mode
   int selfmanaged_snap_rollback(uint64_t snap_id, uint64_t *rollback_id);

The ``librados.cc`` forwarding bodies call
``IoCtxImpl::snap_rollback()`` and
``IoCtxImpl::selfmanaged_snap_rollback()`` respectively, exactly as
``snap_remove()`` forwards to ``IoCtxImpl::snap_remove()``.

11.5 librados C API (``src/include/rados/librados.h``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Two new C functions are added, following the naming convention of the
existing ``rados_ioctx_snap_remove()`` and
``rados_ioctx_selfmanaged_snap_remove()``:

.. code:: c

   /* src/include/rados/librados.h */

   /**
    * Initiate a pool-level snapshot rollback (pool-managed snaps).
    *
    * Completes in O(1) time; background work is performed by OSDs.
    * The allocated rollback ID is returned in *rollback_id.
    *
    * @param io        the pool I/O context
    * @param snapname  name of the snapshot to restore
    * @param rollback_id  [out] allocated rollback ID
    * @returns 0 on success, negative error code on failure
    *   -EPERM   cluster require_osd_release < umbrella
    *   -ENOENT  snapshot does not exist
    *   -EINVAL  pool is in selfmanaged-snap mode
    */
   CEPH_RADOS_API int rados_ioctx_snap_rollback_all(rados_ioctx_t io,
                                                     const char *snapname,
                                                     uint64_t *rollback_id);

   /**
    * Initiate a pool-level snapshot rollback (selfmanaged snaps).
    *
    * @param io        the pool I/O context
    * @param snap_id   selfmanaged snap ID to restore from
    * @param rollback_id  [out] allocated rollback ID
    * @returns 0 on success, negative error code on failure
    *   -EPERM   cluster require_osd_release < umbrella
    *   -ENOENT  snap ID has been deleted
    *   -EINVAL  pool is in pool-managed-snap mode
    */
   CEPH_RADOS_API int rados_ioctx_selfmanaged_snap_rollback_all(rados_ioctx_t io,
                                                                 uint64_t snap_id,
                                                                 uint64_t *rollback_id);

..

   **Note:** The existing
   ``rados_ioctx_snap_rollback(io, oid, snapname)`` (which rolls back a
   single *object*) retains its current name and signature unchanged.
   The new pool-level variants use the ``_all`` suffix to avoid any name
   collision: ``rados_ioctx_snap_rollback_all`` (pool-managed) and
   ``rados_ioctx_selfmanaged_snap_rollback_all`` (selfmanaged). The
   ``oid`` parameter is absent from the pool-level functions, making the
   distinction clear in both name and signature.

The C implementations in ``src/librados/librados.cc`` forward to the
corresponding ``IoCtxImpl`` methods.

11.6 neorados C++ API (``src/include/neorados/RADOS.hpp``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The neorados ``RADOS`` class already exposes ``create_pool_snap()``,
``delete_pool_snap()``, ``allocate_selfmanaged_snap()``, and
``delete_selfmanaged_snap()`` as Asio completion-token templates. Two
new methods are added in the same style:

.. code:: cpp

   // src/include/neorados/RADOS.hpp -- in class RADOS

   using RollbackSnapSig = void(boost::system::error_code, std::uint64_t);
   using RollbackSnapComp =
     boost::asio::any_completion_handler<RollbackSnapSig>;

   /// Initiate a pool-level snapshot rollback (pool-managed snaps).
   template<boost::asio::completion_token_for<RollbackSnapSig> CompletionToken>
   auto rollback_pool_snap(int64_t pool, std::string snap_name,
                           CompletionToken&& token) {
     return boost::asio::async_initiate<decltype(consigned), RollbackSnapSig>(
       [pool, this](auto&& handler, std::string snap_name) {
         rollback_pool_snap_(pool, std::move(snap_name),
                             std::move(handler));
       }, consigned, std::move(snap_name));
   }

   /// Initiate a pool-level snapshot rollback (selfmanaged snaps).
   template<boost::asio::completion_token_for<RollbackSnapSig> CompletionToken>
   auto rollback_selfmanaged_snap(int64_t pool, std::uint64_t snap,
                                  CompletionToken&& token) {
     return boost::asio::async_initiate<decltype(consigned), RollbackSnapSig>(
       [pool, snap, this](auto&& handler) {
         rollback_selfmanaged_snap_(pool, snap, std::move(handler));
       }, consigned);
   }

The private dispatch functions ``rollback_pool_snap_()`` and
``rollback_selfmanaged_snap_()`` in ``src/neorados/RADOS.cc`` call
``impl->objecter->rollback_pool_snap()`` and
``impl->objecter->rollback_selfmanaged_snap()`` respectively, decoding
the ``rollback_id`` from the reply buffer and forwarding it to the
completion handler, mirroring the ``create_pool_snap_()`` /
``delete_pool_snap_()`` pattern. ### 11.7 Python RADOS Bindings
(``src/pybind/rados``)

The Python RADOS bindings in ``src/pybind/rados/rados.pyx`` expose the
librados C API to Python callers. The bindings already wrap pool-managed
and selfmanaged snap operations using the Cython ``rados_ioctx_*``
function declarations in ``src/pybind/rados/c_rados.pxd``.

Two new methods are added to the ``Ioctx`` class, following exactly the
same pattern as the existing ``create_snap()`` / ``remove_snap()`` and
``create_self_managed_snap()`` / ``remove_self_managed_snap()`` methods:

.. code:: python

   # src/pybind/rados/rados.pyx -- new methods on class Ioctx

   def rollback_snap(self, snap_name: str) -> int:
       """
       Initiate a pool-level snapshot rollback (pool-managed snaps).

       Completes in O(1) time; background work is performed by OSDs.

       :param snap_name: name of the snapshot to restore
       :returns: rollback ID (int) allocated for this rollback
       :raises: :class:`Error` on failure, including:
           - :class:`PermissionError` if require_osd_release < umbrella
           - :class:`ObjectNotFound` if snapshot does not exist
       """
       self.require_ioctx_open()
       snap_name_raw = cstr(snap_name, 'snap_name')
       cdef:
           char *_snap_name = snap_name_raw
           uint64_t _rollback_id = 0
       with nogil:
           ret = rados_ioctx_snap_rollback_all(self.io, _snap_name, &_rollback_id)
       if ret != 0:
           raise make_ex(ret, "Failed to roll back pool to snap %s" % snap_name)
       return int(_rollback_id)


   def rollback_self_managed_snap(self, snap_id: int) -> int:
       """
       Initiate a pool-level snapshot rollback (selfmanaged snaps).

       Completes in O(1) time; background work is performed by OSDs.

       :param snap_id: the selfmanaged snap ID to restore from
       :returns: rollback ID (int) allocated for this rollback
       :raises: :class:`Error` on failure, including:
           - :class:`PermissionError` if require_osd_release < umbrella
           - :class:`ObjectNotFound` if snap ID has been deleted
       """
       self.require_ioctx_open()
       cdef:
           rados_snap_t _snap_id = snap_id
           uint64_t _rollback_id = 0
       with nogil:
           ret = rados_ioctx_selfmanaged_snap_rollback_all(self.io, _snap_id,
                                                            &_rollback_id)
       if ret != 0:
           raise make_ex(ret,
               "Failed to roll back pool to selfmanaged snap %d" % snap_id)
       return int(_rollback_id)

The two new C function declarations are added to ``c_rados.pxd``:

.. code:: python

   # src/pybind/rados/c_rados.pxd -- new declarations alongside existing snap ops
   # The C functions use the _all suffix; Cython aliases them with the same name.

   int rados_ioctx_snap_rollback_all(rados_ioctx_t io, const char *snapname,
                                      uint64_t *rollback_id)
   int rados_ioctx_selfmanaged_snap_rollback_all(rados_ioctx_t io,
                                                  uint64_t snap_id,
                                                  uint64_t *rollback_id)

The Python methods call these ``_all``-suffixed C functions. The
existing ``rados_ioctx_snap_rollback(io, oid, snapname)`` and
``rados_ioctx_selfmanaged_snap_rollback(io, oid, snap_id)`` object-level
declarations in ``c_rados.pxd`` are untouched; there is no name
collision because the new C symbols have distinct names.

--------------

12. RADOS CLI Extension
-----------------------

12.1 Existing Pool Snapshot Commands
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``rados`` command-line tool (``src/tools/rados/rados.cc``) already
supports pool-managed snapshot operations via:

::

   rados -p <pool> mksnap <snap-name>     # calls io_ctx.snap_create()
   rados -p <pool> rmsnap <snap-name>     # calls io_ctx.snap_remove()

These commands operate on a named snapshot within a pool and are guarded
by a ``pool_is_in_selfmanaged_snaps_mode()`` check that rejects the
operation when the pool is in unmanaged-snap mode. The existing
per-object rollback command:

::

   rados -p <pool> rollback <obj-name> <snap-name>   # calls io_ctx.snap_rollback()

rolls back a single named object to a snapshot and is unrelated to the
new pool-level operation.

The ``ceph`` CLI (``src/mon/OSDMonitor.cc`` /
``src/mon/MonCommands.h``) already exposes pool-managed snapshot
lifecycle operations symmetrically:

::

   ceph osd pool mksnap  <pool> <snap-name>   # adds a pool-managed snap
   ceph osd pool rmsnap  <pool> <snap-name>   # removes a pool-managed snap

12.2 New ``rollbacksnap`` Command (``rados`` CLI)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A new command ``rollbacksnap`` is added to the ``rados`` tool alongside
``mksnap`` and ``rmsnap``. It issues ``POOL_OP_ROLLBACK_SNAP``
(pool-managed mode) or ``POOL_OP_ROLLBACK_UNMANAGED_SNAP`` (selfmanaged
mode) and waits for the O(1) MON acknowledgement before returning. The
correct operation is selected by querying the pool's snap mode, exactly
as ``mksnap``/``rmsnap`` do:

::

   rados -p <pool> rollbacksnap <snap-name>           # pool-managed snaps
   rados -p <pool> rollbacksnap <snap-id>             # selfmanaged snaps (numeric ID)

The implementation detects the pool mode via
``rados.pool_is_in_selfmanaged_snaps_mode()`` and dispatches to the
appropriate API call:

.. code:: cpp

   // src/tools/rados/rados.cc -- alongside the mksnap and rmsnap handlers

   else if (strcmp(nargs[0], "rollbacksnap") == 0) {
     if (!pool_name || nargs.size() < 2) {
       usage(cerr);
       return 1;
     }

     uint64_t rollback_id = 0;
     ret = rados.pool_is_in_selfmanaged_snaps_mode(pool_name);
     if (ret < 0) {
       cerr << "failed to query pool " << pool_name
            << " for selfmanaged snaps: " << cpp_strerror(ret) << std::endl;
       return 1;
     } else if (ret > 0) {
       // selfmanaged snaps: argument is a numeric snap ID
       char *endptr = nullptr;
       uint64_t snap_id = strtoull(nargs[1], &endptr, 10);
       if (*endptr || snap_id == 0) {
         cerr << "error: selfmanaged snap ID must be a positive integer" << std::endl;
         return 1;
       }
       ret = io_ctx.selfmanaged_snap_rollback(snap_id, &rollback_id);
     } else {
       // pool-managed snaps: argument is a snap name
       ret = io_ctx.snap_rollback(nargs[1], &rollback_id);
     }

     if (ret < 0) {
       if (ret == -EPERM) {
         int8_t release_raw = 0;
         rados.get_min_compatible_osd(&release_raw);
         auto release = static_cast<ceph_release_t>(release_raw);
         cerr << "error: pool-level snapshot rollback requires all OSDs to be "
                 "running Umbrella or later (require_osd_release is currently "
              << to_string(release) << ")" << std::endl;
       } else if (ret == -ENOENT) {
         cerr << "error: snapshot '" << nargs[1]
              << "' does not exist in pool '" << pool_name << "'" << std::endl;
       } else {
         cerr << "error rolling back pool " << pool_name
              << " to snapshot '" << nargs[1] << "': "
              << cpp_strerror(ret) << std::endl;
       }
       return 1;
     }
     cout << "initiated rollback of pool " << pool_name
          << " to snapshot '" << nargs[1] << "'"
          << " (rollback id " << rollback_id << ")" << std::endl;
   }

The usage string is extended:

::

      rollbacksnap <snap-name|snap-id>  roll back entire pool to snap
                                        (name for pool-managed, ID for selfmanaged)

12.3 Error Messages
~~~~~~~~~~~~~~~~~~~

The following error conditions are detected and reported with
descriptive messages:

+-----------------------+-----------------------+-----------------------+
| Error                 | Condition             | Message               |
+=======================+=======================+=======================+
| ``-EPERM``            | Cluster               | ``"error:             |
|                       | ``                    | pool-level snapshot r |
|                       | require_osd_release`` | ollback requires all  |
|                       | < Umbrella            | OSDs to be running Um |
|                       |                       | brella or later (requ |
|                       |                       | ire_osd_release is cu |
|                       |                       | rrently <release>)"`` |
+-----------------------+-----------------------+-----------------------+
| ``-ENOENT``           | Named snapshot does   | ``"error: snapshot '  |
|                       | not exist             | <name>' does not exis |
|                       |                       | t in pool '<pool>'"`` |
+-----------------------+-----------------------+-----------------------+
| ``-ENOENT``           | Selfmanaged snap ID   | ``"err                |
|                       | already deleted       | or: snap ID <id> has  |
|                       |                       | already been deleted  |
|                       |                       | from pool '<pool>'"`` |
+-----------------------+-----------------------+-----------------------+
| Bad argument          | Selfmanaged mode but  | ``"error: selfman     |
|                       | non-numeric argument  | aged snap ID must be  |
|                       |                       | a positive integer"`` |
+-----------------------+-----------------------+-----------------------+

The ``-EPERM`` message is enriched with the current
``require_osd_release`` value by calling
``Rados::get_min_compatible_osd()``, casting to ``ceph_release_t``, and
printing using ``to_string()``.

12.4 New ``ceph osd pool rollbacksnap`` Command
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A parallel ``ceph`` CLI command is registered in
``src/mon/MonCommands.h`` and handled in
``OSDMonitor::prepare_command_impl()`` alongside the existing
``osd pool mksnap`` and ``osd pool rmsnap`` commands:

::

   ceph osd pool rollbacksnap <pool> <snap-name>    # pool-managed snaps only

This command is restricted to pools in pool-managed snapshot mode
(``-EINVAL`` is returned for unmanaged-snap pools; use the ``rados``
CLI for selfmanaged pools). The handler:

1. Validates ``require_osd_release >= umbrella`` (``-EPERM`` if not
   met).
2. Rejects unmanaged-snap-mode pools (``-EINVAL``).
3. Verifies the named snapshot exists in the pool (``-ENOENT`` if not).
4. Checks idempotency: if a rollback of the same snapshot is already
   recorded in ``pg_pool_t::rollback_snaps``, returns immediately with
   the existing rollback id and a success status.
5. Allocates ``rollback_id = snap_seq + 1``, stamps
   ``rollback_snap_info_t``, advances ``pp->snap_seq``,
   inserts into ``pp->rollback_snaps`` and
   ``pending_inc.new_rollback_snaps``, then calls ``wait_for_commit``.

On success the command prints::

   initiated rollback of pool <pool> to snapshot '<snap>' (rollback id <id>)

+-----------------------+-----------------------+-----------------------+
| Error                 | Condition             | Message               |
+=======================+=======================+=======================+
| ``-EPERM``            | ``require_osd_releas  | ``"pool-level         |
|                       | e`` < Umbrella        | snapshot rollback     |
|                       |                       | requires all OSDs to  |
|                       |                       | be running Umbrella   |
|                       |                       | or later"``           |
+-----------------------+-----------------------+-----------------------+
| ``-EINVAL``           | Pool is in unmanaged  | ``"pool <pool> is in  |
|                       | snaps mode            | unmanaged snaps       |
|                       |                       | mode"``               |
+-----------------------+-----------------------+-----------------------+
| ``-ENOENT``           | Pool not found        | ``"unrecognized pool  |
|                       |                       | '<pool>'"``           |
+-----------------------+-----------------------+-----------------------+
| ``-ENOENT``           | Named snapshot does   | ``"pool <pool> snap   |
|                       | not exist             | <snap> does not       |
|                       |                       | exist"``              |
+-----------------------+-----------------------+-----------------------+

--------------

13. librbd Integration
----------------------

13.1 Current Per-Object Rollback
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``librbd`` snapshot rollback is implemented in
``src/librbd/operation/SnapshotRollbackRequest.cc``. The key step is the
``send_rollback_objects()`` function, which uses ``AsyncObjectThrottle``
to iterate across all objects in the image and issue
``op.selfmanaged_snap_rollback(m_snap_id)`` on each one via
``image_ctx.data_ctx.aio_operate()``. This is an O(number of objects)
operation and is the bottleneck for large images.

13.2 New Pool-Op Fast Path
~~~~~~~~~~~~~~~~~~~~~~~~~~

When the cluster is running Umbrella or later,
``send_rollback_objects()`` is replaced by a single pool-op call via the
new ``librados::IoCtx::selfmanaged_snap_rollback()`` API (§13.4). The
check is made at the point where ``send_rollback_objects()`` would be
called, so all preceding state-machine steps (blocking writes, resizing
the image to the snapshot size, rolling back the object map) remain
unchanged.

The cluster release is determined by reading ``require_osd_release``
from the OSDMap via ``image_ctx.data_ctx.get_min_compatible_osd()``:

.. code:: cpp

   // src/librbd/operation/SnapshotRollbackRequest.cc
   // Inside send_rollback_objects() -- new fast-path check at entry

   void SnapshotRollbackRequest<I>::send_rollback_objects() {
     I &image_ctx = this->m_image_ctx;
     CephContext *cct = image_ctx.cct;

     // Attempt the fast pool-op rollback path (available from Umbrella onwards)
     int8_t require_osd_release_raw = 0;
     int r = image_ctx.data_ctx.get_min_compatible_osd(&require_osd_release_raw);
     if (r == 0) {
       auto require_osd_release =
         static_cast<ceph_release_t>(require_osd_release_raw);

       if (require_osd_release >= ceph_release_t::umbrella) {
         ldout(cct, 5) << this << " " << __func__
                       << ": using pool-op rollback fast path" << dendl;
         uint64_t rollback_id = 0;
         // RBD always uses selfmanaged snaps on its data pool
         r = image_ctx.data_ctx.selfmanaged_snap_rollback(m_snap_id, &rollback_id);
         if (r == 0) {
           // Pool-op issued; proceed to handle_rollback_objects()
           // (which transitions to send_refresh_object_map)
           Context *ctx = create_context_callback<
             SnapshotRollbackRequest<I>,
             &SnapshotRollbackRequest<I>::handle_rollback_objects>(this);
           ctx->complete(0);
           return;
         }
         // Non-fatal: fall through to per-object path on any error
         ldout(cct, 1) << this << " " << __func__
                       << ": pool-op rollback failed (" << cpp_strerror(r)
                       << "), falling back to per-object rollback" << dendl;
       }
     }

     // --- existing per-object AsyncObjectThrottle path below ---
     uint64_t num_objects;
     // … (unchanged) …
   }

13.3 Fallback Behaviour
~~~~~~~~~~~~~~~~~~~~~~~

The fast path is attempted first. Any failure (including ``-EPERM`` from
a cluster that is only partially upgraded, or any transient error)
causes a silent fallback to the existing per-object
``selfmanaged_snap_rollback`` path. This ensures that librbd continues
to work correctly against clusters running any release, without
requiring a separate feature-detection probe or configuration option.

The fallback logs at level 1 (warnings) to avoid alarming operators
during normal rolling upgrades. Once the cluster is fully on Umbrella
the pool-op path will succeed on the next invocation.

13.4 Interaction with Object Map and Cache Invalidation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The pool-op fast path bypasses ``send_rollback_objects()`` but leaves
all other state-machine states intact:

-  ``send_block_writes()`` -- unchanged; writes are blocked before the
   rollback.
-  ``send_resize_image()`` -- unchanged; the image is resized to the
   snapshot size before the rollback is issued.
-  ``send_get_snap_object_map()`` / ``send_rollback_object_map()`` --
   unchanged; the in-memory and on-disk object maps are updated to
   reflect the snapshot state.
-  ``send_refresh_object_map()`` -- unchanged.
-  ``send_invalidate_cache()`` -- unchanged; the cache is invalidated
   after the pool-op completes so that subsequent reads pick up the
   rolled-back data.

Because the pool-op rollback is transparent at the RADOS level (reads
are redirected and writes trigger JIT rollback), the object map update
performed by the existing steps is still required: it ensures that
librbd’s own internal metadata is consistent with what the RADOS layer
will present after the rollback.

13.5 Journal Replay
~~~~~~~~~~~~~~~~~~~

The ``SnapRollbackEvent`` recorded in the librbd journal carries
``snap_namespace`` and ``snap_name`` (see
``src/librbd/journal/Types.h``). During replay, ``journal::Replay``
calls ``execute_snap_rollback()``, which goes through the full
``SnapshotRollbackRequest`` state machine. The pool-op fast path is
available during replay on an Umbrella cluster and will be used
automatically, since replay calls the same ``send_rollback_objects()``
entry point.

--------------

14. ceph_test_rados Extension
-----------------------------

14.1 Overview of ceph_test_rados
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``ceph_test_rados`` is a randomised I/O stress tool implemented in
``src/test/osd/TestRados.cc`` and ``src/test/osd/RadosModel.h``. It
issues a configurable mixture of operations against a RADOS pool --
reads, writes, deletes, snap creates, snap removes, and per-object
rollbacks -- driven by a weighted random scheduler in ``TestRados.cc``.

State is tracked in ``RadosTestContext`` (declared in ``RadosModel.h``),
which maintains:

-  ``pool_obj_cont``: a
   ``std::map<int, std::map<std::string, ObjectDesc>>`` keyed by the
   model’s internal snap sequence number. Each entry records the
   expected content descriptor (``ObjectDesc``) for every object at that
   snap level.
-  ``snaps``: a ``std::map<int, uint64_t>`` mapping model snap sequence
   numbers to the RADOS snap IDs returned by ``snap_create()`` or
   ``selfmanaged_snap_create()``.
-  ``current_snap``: the model’s current sequence counter, incremented
   by ``add_snap()`` after each snap creation.
-  ``roll_back(oid, snap)``: copies the ``ObjectDesc`` for ``oid`` at
   model snap ``snap`` into the current (``pool_obj_cont.rbegin()``)
   level, marking it dirty.

The existing ``RollbackOp`` class issues ``op.snap_rollback(snap)``
(pool-managed mode) or ``op.selfmanaged_snap_rollback(snap)`` (unmanaged
mode) for a **single object** chosen randomly from ``oid_not_in_use``.

14.2 New ``SnapRollbackOp`` Class
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A new ``SnapRollbackOp`` class is added to ``RadosModel.h``, parallel to
the existing ``SnapCreateOp``, ``SnapRemoveOp``, and ``RollbackOp``
classes. It issues the new pool-level rollback operation and updates the
model to reflect that **every object** in the pool has been rolled back
to the chosen snapshot.

The class handles both pool-managed and selfmanaged snap modes: in
pool-managed mode it calls
``io_ctx.snap_rollback(snapname, &rollback_id)`` after resolving the
snap name via ``io_ctx.snap_get_name()``; in selfmanaged mode it calls
``io_ctx.selfmanaged_snap_rollback(snap_id, &rollback_id)`` directly
using the RADOS snap ID stored in ``context->snaps``.

.. code:: cpp

   // src/test/osd/RadosModel.h -- new class

   class SnapRollbackOp : public TestOp {
   public:
     int snap_to_roll_back_to;           // model snap sequence number
     std::shared_ptr<int> in_use;

     SnapRollbackOp(int n, RadosTestContext *context, TestOpStat *stat = 0)
       : TestOp(n, context, stat), snap_to_roll_back_to(-1)
     {}

     void _begin() override
     {
       std::lock_guard l{context->state_lock};

       if (context->snaps.empty()) {
         context->kick();
         done = true;
         return;
       }

       // Must quiesce: model update applies to all objects simultaneously.
       if (!context->oid_in_use.empty()) {
         context->kick();
         done = true;
         return;
       }

       snap_to_roll_back_to = rand_choose(context->snaps)->first;
       in_use = context->snaps_in_use.lookup_or_create(
         snap_to_roll_back_to, snap_to_roll_back_to);

       context->cout_prefix() << "pool-level snap rollback to snap "
                              << snap_to_roll_back_to << std::endl;

       // Update model: roll back every known object to this snap
       context->roll_back_pool(snap_to_roll_back_to);

       uint64_t rollback_id = 0;
       uint64_t rados_snap  = context->snaps[snap_to_roll_back_to];
       int r;

       if (context->pool_snaps) {
         std::string snapname;
         r = context->io_ctx.snap_get_name(rados_snap, &snapname);
         if (r < 0) {
           std::cerr << "SnapRollbackOp: snap_get_name failed: "
                     << cpp_strerror(r) << std::endl;
           ceph_abort();
         }
         r = context->io_ctx.snap_rollback(snapname, &rollback_id);
       } else {
         r = context->io_ctx.selfmanaged_snap_rollback(rados_snap, &rollback_id);
       }

       if (r < 0) {
         std::cerr << "SnapRollbackOp failed: " << cpp_strerror(r) << std::endl;
         ceph_abort();
       }

       in_use.reset();
       done = true;
       context->kick();
     }

     bool finished() override { return done; }
     bool must_quiesce_other_ops() override { return true; }

     std::string getType() override { return "SnapRollbackOp"; }
   };

14.3 Model Update: ``roll_back_pool()``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A new method ``roll_back_pool(int snap)`` is added to
``RadosTestContext``. It applies the rollback to **every object**
tracked in the model, mirroring what the pool-op does at the RADOS
level:

.. code:: cpp

   // src/test/osd/RadosModel.h -- new method in RadosTestContext

   void roll_back_pool(int snap)
   {
     // Collect the set of all object names across all snap levels
     std::set<std::string> all_oids;
     for (auto& [level, objs] : pool_obj_cont) {
       for (auto& [oid, _] : objs) {
         all_oids.insert(oid);
       }
     }

     // For each object, apply the per-object roll_back() logic at the
     // current snap level.  Objects that did not exist at 'snap' are
     // marked as deleted (roll_back() already handles this via find_object).
     for (auto& oid : all_oids) {
       roll_back(oid, snap);
     }

     // Advance current_snap to record that this pool event has occurred,
     // using add_snap() with the RADOS rollback ID.  The rollback ID is
     // stored alongside existing snap IDs so that reads performed after the
     // rollback but before a new snapshot is taken use the correct model state.
   }

Unlike ``roll_back()``, which is called per-object inside
``RollbackOp``, ``roll_back_pool()`` holds ``state_lock`` for its entire
duration and requires ``oid_in_use`` to be empty (enforced in
``_begin()`` via ``must_quiesce_other_ops()``).

14.4 Mixed Operation Mode
~~~~~~~~~~~~~~~~~~~~~~~~~

The goal is to exercise both per-object rollback (``RollbackOp``) and
pool-level rollback (``SnapRollbackOp``) in the same test run, so that
the interaction between the two mechanisms is tested: some objects may
have their rollback resolved by a JIT write (triggered by the next
``WriteOp``) while others are resolved by the background trimmer, and
all reads must return correct data throughout.

A new entry is added to the ``TestOpType`` enum:

.. code:: cpp

   // src/test/osd/RadosModel.h -- in enum TestOpType

   TEST_OP_SNAP_ROLLBACK,   // new: pool-level snap rollback (both snap modes)

In ``TestRados.cc``, the op-weight table is extended so that
``SnapRollbackOp`` is scheduled alongside ``RollbackOp`` in both
pool-snaps and selfmanaged-snaps modes. The relative weight is chosen so
that roughly one pool rollback occurs for every five per-object
rollbacks, ensuring coverage without dominating the run (pool rollbacks
require quiescing all other ops):

.. code:: cpp

   // src/test/osd/TestRados.cc -- in the op-weight initialisation

   // Active in both pool_snaps and selfmanaged-snaps modes:
   op_weights.push_back({TEST_OP_SNAP_ROLLBACK, 2});

14.5 Read Validation After Pool Rollback
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

After ``SnapRollbackOp`` completes and ``roll_back_pool()`` has updated
the model, subsequent ``ReadOp`` operations validate against the
rolled-back ``ObjectDesc`` exactly as they would after any other
model-state change. No special handling in ``ReadOp`` is required:
``find_object()`` always looks up the most recent entry in
``pool_obj_cont`` (the ``rbegin()`` level), which is the post-rollback
content installed by ``roll_back_pool()``.

For reads that target a specific snap (i.e. ``snap >= 0``), the model
lookup also continues to work correctly: the snap levels below
``current_snap`` are unchanged by the pool rollback, so historical-snap
reads return the same data before and after the pool rollback, which
matches the RADOS behaviour (pool rollback only affects the head;
existing clones are preserved).

14.6 Quiesce and Ordering
~~~~~~~~~~~~~~~~~~~~~~~~~

``SnapRollbackOp`` returns ``true`` from ``must_quiesce_other_ops()``,
which causes the ``TestRados.cc`` scheduler to wait for all in-flight
ops to drain before issuing the pool rollback. This is necessary
because:

1. The pool-op is acknowledged by the MON immediately, but individual
   objects may not yet reflect the rolled-back state. A concurrent write
   could trigger the JIT path on one object but not another, leaving the
   model transiently inconsistent with outstanding I/O.

2. The model update (``roll_back_pool()``) must be atomic with respect
   to the snap IDs recorded in ``snaps_in_use``. Any ``RollbackOp`` in
   flight must complete before the pool rollback resets the model state
   for all objects.

The quiesce mechanism is identical to the one already present for
``SnapCreateOp`` in pool-snaps mode.

14.7 Snap Name Lookup for Pool-Managed Mode
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``io_ctx.snap_rollback()`` takes a snap name (string).
``SnapRollbackOp`` uses
``io_ctx.snap_get_name(rados_snap_id, &snapname)`` to resolve the RADOS
snap ID stored in ``context->snaps[snap_to_roll_back_to]`` back to its
name before calling the pool-op. In selfmanaged mode the RADOS snap ID
is passed directly to ``selfmanaged_snap_rollback()``, no name lookup
required. ### 14.8 QA Suite Updates (``src/qa``)

14.8.1 YAML Workload Files
^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``ceph_test_rados`` op-weight framework maps YAML keys in the
``op_weights`` dictionary to ``--op <name> <weight>`` command-line
arguments. All existing YAML files that set ``rollback: 50`` (or any
non-zero value) must also set ``snap_rollback: 10`` to exercise the new
pool-level rollback API.

The affected files are all those under ``src/qa/suites/`` that currently
contain a ``rollback:`` line with a value greater than zero:

::

   src/qa/suites/powercycle/osd/tasks/snaps-few-objects.yaml
   src/qa/suites/powercycle/osd/tasks/snaps-many-objects.yaml
   src/qa/suites/big/rados-thrash/workloads/snaps-few-objects.yaml
   src/qa/suites/rados/monthrash/workloads/snaps-few-objects.yaml
   src/qa/suites/rados/thrash-old-clients/workloads/snaps-few-objects.yaml
   src/qa/suites/rados/thrash/workloads/pool-snaps-few-objects.yaml
   src/qa/suites/rados/thrash/workloads/snaps-few-objects.yaml
   src/qa/suites/rados/thrash/workloads/snaps-few-objects-balanced.yaml
   src/qa/suites/rados/thrash/workloads/snaps-few-objects-localized.yaml
   src/qa/suites/rados/thrash/workloads/small-objects.yaml
   src/qa/suites/rados/thrash/workloads/small-objects-balanced.yaml
   src/qa/suites/rados/thrash/workloads/small-objects-localized.yaml
   src/qa/suites/rados/thrash/workloads/dedup-io-snaps.yaml
   src/qa/suites/rados/thrash-erasure-code/workloads/ec-small-objects.yaml
   src/qa/suites/rados/thrash-erasure-code/workloads/ec-small-objects-fast-read.yaml
   src/qa/suites/rados/thrash-erasure-code-overwrites/workloads/ec-snaps-few-objects-overwrites.yaml
   src/qa/suites/smoke/basic/tasks/test/rados_ec_snaps.yaml
   src/qa/suites/upgrade/tentacle-x/stress-split/2-first-half-tasks/snaps-few-objects.yaml
   src/qa/suites/upgrade/tentacle-x/stress-split/3-stress-tasks/snaps-few-objects.yaml
   src/qa/suites/upgrade/tentacle-x/parallel/workload/ec-rados-default.yaml
   src/qa/suites/upgrade/squid-x/stress-split/2-first-half-tasks/snaps-few-objects.yaml
   src/qa/suites/upgrade/squid-x/stress-split/3-stress-tasks/snaps-few-objects.yaml
   src/qa/suites/upgrade/squid-x/parallel/workload/ec-rados-default.yaml

In each file, the line ``rollback: 50`` (or the existing rollback
weight) is accompanied by a new ``snap_rollback: 10`` line immediately
below it. Example diff for a typical workload file:

.. code:: yaml

          rollback: 50
   +      snap_rollback: 10

The weight of 10 (one fifth of the existing rollback weight of 50) is
chosen to give reasonable coverage without dominating the run, since
pool-level rollbacks require quiescing all in-flight ops (see §14.6).

Files under ``src/qa/suites/crimson-rados/`` that already set
``rollback: 0`` are **not** updated because Crimson does not yet support
pool-level snapshot rollback.

Files under ``src/qa/suites/upgrade/`` at the squid-x and tentacle-x
paths are updated with ``snap_rollback: 10``, but only in the upgrade
test phases that run against a fully upgraded cluster (i.e. not the
``2-first-half-tasks`` phase where some OSDs may still be running the
older release). In practice this means ``snap_rollback: 10`` is safe to
add to the ``3-stress-tasks`` and ``parallel`` phases, but the
``2-first-half-tasks`` phase should use ``snap_rollback: 0`` so that the
new API is not exercised against a partially-upgraded cluster.

14.8.2 ``src/qa/tasks/rados.py``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``rados.py`` task maps the ``op_weights`` YAML keys to
``--op <name> <weight>`` arguments passed to ``ceph_test_rados``. The
op-weight handling loop already iterates over a list of supported
operation names and appends each to ``args`` when it appears in the YAML
config. The single change required is to add ``"snap_rollback"`` to the
list:

.. code:: python

   # src/qa/tasks/rados.py -- inside task(), the op-weights loop

       # Parallel of the op_types in test/osd/TestRados.cc
       for field in [
           # read handled above
           # write handled above
           # delete handled above
           "snap_create",
           "snap_remove",
           "rollback",
           "snap_rollback",    # NEW: pool-level snapshot rollback
           "setattr",
           ...
       ]:
           if field in op_weights:
               weights[field] = op_weights[field]

The docstring for the ``task()`` function is also updated to document
the new ``snap_rollback`` key:

.. code:: python

       op_weights: <dictionary mapping operation type to integer weight>
         snap_rollback: weight for pool-level snapshot rollback operations
                        (pool-managed mode uses snap name; selfmanaged uses snap ID)
                        Default: 0 (disabled). Recommended: 10 when rollback > 0.

No other changes to ``rados.py`` are required: ``snap_rollback`` uses
the same command-line argument mechanism as all other op weights, and
``SnapRollbackOp`` in ``TestRados.cc`` already handles the pool-mode
detection internally (§14.2).

--------------

15. librados Test Extensions
----------------------------

.. _overview-1:

15.1 Overview
~~~~~~~~~~~~~

The new ``snap_rollback()`` and ``selfmanaged_snap_rollback()`` APIs
must be covered by tests in the existing librados test infrastructure.
Two source files are relevant:

-  ``src/test/librados/snapshots.cc`` -- C API (``rados_ioctx_*``) GTest
   suite
-  ``src/test/librados/snapshots_cxx.cc`` -- C++ API
   (``librados::IoCtx``) GTest suite
-  ``src/test/librados_test_stub/`` -- in-memory stub used by librbd unit
   tests; the new pool-op functions must be stubbed here so that
   librbd’s unit tests can exercise the fast path (§13.3)

15.2 C++ API Tests (``snapshots_cxx.cc``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

New test cases are added to ``LibRadosSnapshotsPP`` (pool-managed snaps)
and ``LibRadosSnapshotsSelfManagedPP`` (selfmanaged snaps), following
the style of the existing ``RollbackPP`` and ``SelfManagedRollbackPP``
tests.

.. code:: cpp

   // src/test/librados/snapshots_cxx.cc -- new tests

   // Pool-managed snap rollback: basic success path
   TEST_F(LibRadosSnapshotsPP, PoolSnapRollbackPP) {
     char buf[bufsize];
     char buf2[bufsize];
     memset(buf,  0xcc, sizeof(buf));
     memset(buf2, 0xdd, sizeof(buf2));
     bufferlist bl1, bl2;
     bl1.append(buf,  sizeof(buf));
     bl2.append(buf2, sizeof(buf2));
     // Write initial content and create snapshot
     ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));
     ASSERT_EQ(0, ioctx.snap_create("rollback_snap"));
     rados_snap_t rid;
     ASSERT_EQ(0, ioctx.snap_lookup("rollback_snap", &rid));
     // Overwrite with different content
     ASSERT_EQ(0, ioctx.write("foo", bl2, sizeof(buf2), 0));
     // Issue pool-level rollback
     uint64_t rollback_id = 0;
     ASSERT_EQ(0, ioctx.snap_rollback("rollback_snap", &rollback_id));
     EXPECT_GT(rollback_id, (uint64_t)0);
     // Verify rollback_id is greater than the snap sequence of the snapshot
     EXPECT_GT(rollback_id, (uint64_t)rid);
     ASSERT_EQ(0, ioctx.snap_remove("rollback_snap"));
   }

   // Pool-managed snap rollback: snap does not exist → -ENOENT
   TEST_F(LibRadosSnapshotsPP, PoolSnapRollbackNoentPP) {
     uint64_t rollback_id = 0;
     ASSERT_EQ(-ENOENT, ioctx.snap_rollback("nonexistent_snap", &rollback_id));
   }

   // Pool-managed snap rollback: idempotency
   // Calling with the same snap name twice returns the same rollback_id
   TEST_F(LibRadosSnapshotsPP, PoolSnapRollbackIdempotentPP) {
     char buf[bufsize];
     memset(buf, 0xcc, sizeof(buf));
     bufferlist bl;
     bl.append(buf, sizeof(buf));
     ASSERT_EQ(0, ioctx.write("foo", bl, sizeof(buf), 0));
     ASSERT_EQ(0, ioctx.snap_create("idem_snap"));
     uint64_t id1 = 0, id2 = 0;
     ASSERT_EQ(0, ioctx.snap_rollback("idem_snap", &id1));
     ASSERT_EQ(0, ioctx.snap_rollback("idem_snap", &id2));
     EXPECT_EQ(id1, id2);
     ASSERT_EQ(0, ioctx.snap_remove("idem_snap"));
   }

   // Selfmanaged snap rollback: basic success path
   TEST_F(LibRadosSnapshotsSelfManagedPP, PoolSelfmanagedSnapRollbackPP) {
     std::vector<uint64_t> my_snaps;
     my_snaps.push_back(-2);
     ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.back()));
     ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
     char buf[bufsize];
     memset(buf, 0xcc, sizeof(buf));
     bufferlist bl1;
     bl1.append(buf, sizeof(buf));
     ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));
     // Issue pool-level selfmanaged snap rollback
     uint64_t rollback_id = 0;
     ASSERT_EQ(0, ioctx.selfmanaged_snap_rollback(my_snaps[0], &rollback_id));
     EXPECT_GT(rollback_id, my_snaps[0]);
     ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
   }

   // Selfmanaged snap rollback: idempotency
   TEST_F(LibRadosSnapshotsSelfManagedPP, PoolSelfmanagedSnapRollbackIdempotentPP) {
     std::vector<uint64_t> my_snaps;
     my_snaps.push_back(-2);
     ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.back()));
     ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
     char buf[bufsize];
     memset(buf, 0xcc, sizeof(buf));
     bufferlist bl;
     bl.append(buf, sizeof(buf));
     ASSERT_EQ(0, ioctx.write("foo", bl, sizeof(buf), 0));
     uint64_t id1 = 0, id2 = 0;
     ASSERT_EQ(0, ioctx.selfmanaged_snap_rollback(my_snaps[0], &id1));
     ASSERT_EQ(0, ioctx.selfmanaged_snap_rollback(my_snaps[0], &id2));
     EXPECT_EQ(id1, id2);
     ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
   }

15.3 C API Tests (``snapshots.cc``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Parallel tests are added to ``LibRadosSnapshots`` and
``LibRadosSnapshotsSelfManaged`` using the C
``rados_ioctx_snap_rollback()`` and
``rados_ioctx_selfmanaged_snap_rollback()`` functions.

.. code:: cpp

   // src/test/librados/snapshots.cc -- new tests

   // Pool-managed snap rollback: basic success path (C API)
   TEST_F(LibRadosSnapshots, PoolSnapRollback) {
     char buf[bufsize];
     char buf2[bufsize];
     memset(buf,  0xcc, sizeof(buf));
     memset(buf2, 0xdd, sizeof(buf2));
     ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
     ASSERT_EQ(0, rados_ioctx_snap_create(ioctx, "rollback_snap"));
     rados_snap_t rid;
     ASSERT_EQ(0, rados_ioctx_snap_lookup(ioctx, "rollback_snap", &rid));
     ASSERT_EQ(0, rados_write(ioctx, "foo", buf2, sizeof(buf2), 0));
     uint64_t rollback_id = 0;
     ASSERT_EQ(0, rados_ioctx_snap_rollback(ioctx, "rollback_snap", &rollback_id));
     EXPECT_GT(rollback_id, (uint64_t)0);
     EXPECT_GT(rollback_id, (uint64_t)rid);
     EXPECT_EQ(0, rados_ioctx_snap_remove(ioctx, "rollback_snap"));
   }

   // Pool-managed snap rollback: snap does not exist → -ENOENT
   TEST_F(LibRadosSnapshots, PoolSnapRollbackNoent) {
     uint64_t rollback_id = 0;
     ASSERT_EQ(-ENOENT,
       rados_ioctx_snap_rollback(ioctx, "nonexistent_snap", &rollback_id));
   }

   // Selfmanaged snap rollback: basic success path (C API)
   TEST_F(LibRadosSnapshotsSelfManaged, PoolSelfmanagedSnapRollback) {
     std::vector<uint64_t> my_snaps;
     my_snaps.push_back(0);
     ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_create(ioctx, &my_snaps.back()));
     uint64_t snap_id = my_snaps[0];
     ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_set_write_ctx(
         ioctx, snap_id,
         reinterpret_cast<uint64_t*>(my_snaps.data()),
         static_cast<int>(my_snaps.size())));
     char buf[bufsize];
     memset(buf, 0xcc, sizeof(buf));
     ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
     uint64_t rollback_id = 0;
     ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_rollback(ioctx, snap_id,
                                                         &rollback_id));
     EXPECT_GT(rollback_id, snap_id);
     ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_remove(ioctx, snap_id));
   }

15.4 Test Stub (``src/test/librados_test_stub``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The in-memory test stub used by librbd unit tests must also implement
the new pool-level rollback operations so that the librbd fast-path test
(§13.3) can exercise ``selfmanaged_snap_rollback()`` without a real
cluster.

Two new virtual methods are added to ``TestIoCtxImpl`` and implemented
in ``TestMemIoCtxImpl``:

.. code:: cpp

   // src/test/librados_test_stub/TestIoCtxImpl.h -- new virtual methods

   virtual int snap_rollback(const std::string& snap_name,
                             uint64_t *rollback_id) = 0;
   virtual int selfmanaged_snap_rollback(uint64_t snap_id,
                                         uint64_t *rollback_id) = 0;

.. code:: cpp

   // src/test/librados_test_stub/TestMemIoCtxImpl.cc -- implementations
   // These simulate the O(1) MON acknowledgement by simply allocating a
   // monotonically increasing fake rollback_id and recording the pending rollback
   // in the in-memory cluster state.

   int TestMemIoCtxImpl::snap_rollback(const std::string& snap_name,
                                        uint64_t *rollback_id)
   {
     // Allocate a fake rollback ID from the pool snap sequence counter
     // and record the rollback in TestMemCluster (for read redirect simulation)
     auto cluster = get_cluster();
     auto pool    = cluster->get_pool(m_pool_name);
     if (!pool) return -ENOENT;
     auto it = pool->snap_names.find(snap_name);
     if (it == pool->snap_names.end()) return -ENOENT;
     *rollback_id = cluster->allocate_snap_id(m_pool_name);
     pool->pending_rollbacks[*rollback_id] = it->second;  // rb_id -> source_snap
     return 0;
   }

   int TestMemIoCtxImpl::selfmanaged_snap_rollback(uint64_t snap_id,
                                                    uint64_t *rollback_id)
   {
     auto cluster = get_cluster();
     auto pool    = cluster->get_pool(m_pool_name);
     if (!pool) return -ENOENT;
     *rollback_id = cluster->allocate_snap_id(m_pool_name);
     pool->pending_rollbacks[*rollback_id] = snap_id;
     return 0;
   }

The ``MockTestMemIoCtxImpl`` in ``MockTestMemIoCtxImpl.h`` gains the
corresponding ``MOCK_METHOD`` declarations and ``ON_CALL`` default
delegations, following the existing pattern for
``selfmanaged_snap_create`` etc.:

.. code:: cpp

   // src/test/librados_test_stub/MockTestMemIoCtxImpl.h

   MOCK_METHOD2(snap_rollback,
                int(const std::string& snap_name, uint64_t *rollback_id));
   int do_snap_rollback(const std::string& snap_name, uint64_t *rollback_id) {
     return TestMemIoCtxImpl::snap_rollback(snap_name, rollback_id);
   }

   MOCK_METHOD2(selfmanaged_snap_rollback,
                int(uint64_t snap_id, uint64_t *rollback_id));
   int do_selfmanaged_snap_rollback(uint64_t snap_id, uint64_t *rollback_id) {
     return TestMemIoCtxImpl::selfmanaged_snap_rollback(snap_id, rollback_id);
   }

--------------

16. Snapshot Code Changes to Ignore Rollback Snap IDs
------------------------------------------------------

16.1 The Problem
~~~~~~~~~~~~~~~~

Rollback IDs are allocated via ``prepare_pool_op()`` using the same
counter as ordinary pool snapshot creation: ``snap_seq`` is incremented
and the new value is stored as the ``rollback_id``.  The crucial
difference is that a rollback ID is **only** inserted into
``pg_pool_t::rollback_snaps``; it is deliberately **not** inserted into
``pg_pool_t::snaps`` (the map of named pool snapshots).

However, ``pg_pool_t::get_snap_context()`` builds the ``SnapContext``
that drives the entire write I/O path:

.. code:: cpp

   // src/osd/osd_types.cc
   SnapContext pg_pool_t::get_snap_context() const
   {
     vector<snapid_t> s(snaps.size());
     unsigned i = 0;
     for (auto p = snaps.crbegin(); p != snaps.crend(); ++p)
       s[i++] = p->first;
     return SnapContext(get_snap_seq(), s);   // seq == snap_seq, s == real snaps only
   }

The resulting ``SnapContext`` therefore has:

-  ``seq`` equal to ``snap_seq``, which may be a rollback ID;
-  ``snaps`` containing only the IDs of real, named snapshots (since
   rollback IDs are never inserted into ``pg_pool_t::snaps``).

This ``SnapContext`` is loaded into ``pool.snapc`` by
``PGPool::PGPool()`` and ``PGPool::update()`` (``src/osd/PeeringState.h``
and ``src/osd/PeeringState.cc``), and then copied into ``ctx->snapc``
inside ``execute_ctx()`` for every pool-snaps-mode write:

.. code:: cpp

   // src/osd/PrimaryLogPG.cc -- execute_ctx()
   if (!(m->has_flag(CEPH_OSD_FLAG_ENFORCE_SNAPC)) &&
       pool.info.is_pool_snaps_mode()) {
     ctx->snapc = pool.snapc;        // seq may be a rollback ID
   }

The resulting ``ctx->snapc`` is then consumed by
``make_writeable()`` to decide whether a snapshot clone is needed and
how to name it.  Because ``snapc.seq`` can be a rollback ID, two
distinct failure modes arise in the existing clone logic.

16.2 Failure Mode 1: Spurious Clone Triggered by a Stale ``SnapSet::seq``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The clone gate condition in ``make_writeable()`` (line 9084 in
``PrimaryLogPG.cc``) is:

.. code:: cpp

   if ((ctx->obs->exists && !ctx->obs->oi.is_whiteout()) && // head exists
       snapc.snaps.size() &&                                 // real snaps exist
       !ctx->cache_operation &&
       snapc.snaps[0] > ctx->new_snapset.seq) {              // object is old
     // ... create clone ...
   }

The condition ``snapc.snaps[0] > ctx->new_snapset.seq`` compares the
highest **real** snap ID against the ``SnapSet::seq`` stored on the
object.

After a previous write that happened when a rollback was pending,
``SnapSet::seq`` will have been advanced to ``snapc.seq`` (which was a
rollback ID) at line 9245:

.. code:: cpp

   if (snapc.seq > ctx->new_snapset.seq) {
     ctx->new_snapset.seq = snapc.seq;   // seq is a rollback ID
   }

So the object's stored ``SnapSet::seq`` is already at the rollback ID,
which is numerically higher than all real snap IDs.  On the *next*
write:

- ``snapc.seq`` is unchanged (still the rollback ID, assuming no new
  snapshot has been created).
- ``snapc.snaps[0]`` is the highest real snap -- which is *less than*
  the stored ``SnapSet::seq``.
- The gate fires as ``false`` (no clone created) -- which is correct
  here.

However, if a *new* real snapshot ``S_new`` is created after the
rollback ID was assigned but before the object is written again, then:

- ``snapc.snaps[0]`` = ``S_new`` > rollback ID = ``SnapSet::seq``.
- The gate correctly fires ``true``.
- The clone is named ``coid.snap = snapc.seq`` (line 9087) -- which is
  again the rollback ID, not ``S_new``.

This produces a clone object at snap ID ``rollback_id`` instead of the
expected snap ID of the most recent real snapshot, corrupting the
``SnapSet``.

16.3 Failure Mode 2: Clone Named After a Rollback ID
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Even when the clone gate fires correctly (a real snapshot does require a
clone), the clone object is named using ``snapc.seq``:

.. code:: cpp

   hobject_t coid = soid;
   coid.snap = snapc.seq;   // WRONG: snapc.seq may be a rollback ID

If ``snapc.seq`` is a rollback ID ``R`` rather than the ID of the most
recent real snapshot ``S``, the clone is named ``soid@R`` instead of
``soid@S``.  The ``clone_snaps`` entry and the snap-mapper registration
are made for the clone at ``R``, so:

- The clone is never found during snaptrim of ``S`` (the snap mapper
  returns no entry for ``S``).
- The clone **is** found (incorrectly) during the background rollback
  trimmer's processing of rollback ``R``, which treats it as a data
  clone rather than a rollback artifact.
- ``SnapSet::clones`` records snap ID ``R``, so any read at snap ``S``
  will find no clone to redirect to and incorrectly report that the
  object did not exist at snap ``S``.

16.4 Root Cause Summary
~~~~~~~~~~~~~~~~~~~~~~~

Both failures share the same root cause: the existing snapshot clone
logic in ``make_writeable()`` was written under the assumption that
``snapc.seq`` always identifies a real named snapshot whose ID also
appears in ``snapc.snaps``.  Rollback IDs break that invariant:
``snapc.seq`` can be a rollback ID that is absent from ``snapc.snaps``.

The three specific assumptions that are violated are:

1. **Clone naming** (line 9087): ``coid.snap = snapc.seq`` assumes
   ``snapc.seq`` is a real snap ID.
2. **Gate comparison** (line 9084): ``snapc.snaps[0] > ctx->new_snapset.seq``
   relies on a ``SnapSet::seq`` that was set correctly to the most
   recent real snap; if it was instead set to a rollback ID, the
   comparison is wrong.
3. **``SnapSet::seq`` update** (line 9245): advancing the stored seq to
   ``snapc.seq`` is harmless for real snaps but contaminates the object
   with a rollback ID as its reference sequence number.

16.5 Required Changes to ``make_writeable()``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The fix is to compute a *real-snap-only* sequence value alongside
``ctx->snapc`` and use it everywhere the existing code uses
``snapc.seq`` for snapshot (not rollback) purposes.

**Step 1 -- derive ``real_snapc.seq`` in ``execute_ctx()``.**

After the existing block that populates ``ctx->snapc``, compute the
highest snap ID that is actually a named pool snapshot:

.. code:: cpp

   // src/osd/PrimaryLogPG.cc -- execute_ctx(), after ctx->snapc is set
   if (pool.info.is_pool_snaps_mode()) {
     // Compute the highest real snapshot ID that is <= snapc.seq.
     // rollback_snaps entries advance snap_seq without inserting into
     // pg_pool_t::snaps, so snapc.seq may be a rollback ID.  Use the
     // highest key in pool.info.snaps (or 0 if none) as the effective
     // sequence for all snapshot clone decisions.
     ctx->real_snap_seq = pool.info.snaps.empty()
                            ? snapid_t(0)
                            : pool.info.snaps.rbegin()->first;
   }

``OpContext`` grows a new field ``snapid_t real_snap_seq`` (defaulting
to ``CEPH_NOSNAP``).

**Step 2 -- use ``real_snap_seq`` for clone naming.**

Inside the clone block in ``make_writeable()`` (line 9087), replace the
use of ``snapc.seq`` with ``ctx->real_snap_seq``:

.. code:: cpp

   // Before (line 9087):
   coid.snap = snapc.seq;

   // After:
   coid.snap = ctx->real_snap_seq;

This ensures the clone object is named after the most recent real
snapshot, not after an intervening rollback ID.

**Step 3 -- guard the clone gate against a rollback-contaminated
``SnapSet::seq``.**

The gate condition (line 9084) compares ``snapc.snaps[0]`` against
``ctx->new_snapset.seq``.  If the object's stored ``SnapSet::seq`` was
previously set to a rollback ID, this comparison will use the wrong
baseline.  The fix is to also replace the reference sequence in the
gate:

.. code:: cpp

   // Before (line 9084):
   snapc.snaps[0] > ctx->new_snapset.seq

   // After:
   snapc.snaps[0] > ctx->new_snapset.real_snap_seq()

where ``SnapSet::real_snap_seq()`` is a new helper that returns the
highest clone snap ID currently recorded in ``SnapSet::clones`` (i.e.,
the highest snap at which a real clone was last created for this
object), falling back to ``0`` if ``clones`` is empty.  This is the
correct baseline: the head must be cloned when a new real snapshot
exists that is more recent than the most recent real clone.

Equivalently, rather than adding a method to ``SnapSet``, the gate can
be rewritten to compare directly against ``ctx->real_snap_seq``:

.. code:: cpp

   snapc.snaps[0] > ctx->new_snapset.seq &&
   !pg_pool_t_is_rollback_snap(pool.info, ctx->new_snapset.seq)

where ``pg_pool_t_is_rollback_snap()`` returns ``true`` if the given
snap ID appears in ``pool.info.rollback_snaps``.  If the stored seq is a
rollback ID, the gate is bypassed for that comparison and the clone is
only created when a real newer snap truly demands it.

**Step 4 -- suppress advancing ``SnapSet::seq`` to a rollback ID.**

The update at line 9243--9245:

.. code:: cpp

   if (snapc.seq > ctx->new_snapset.seq) {
     ctx->new_snapset.seq = snapc.seq;
   }

must not store a rollback ID into the persistent ``SnapSet::seq``.
Replace with:

.. code:: cpp

   if (ctx->real_snap_seq > ctx->new_snapset.seq) {
     ctx->new_snapset.seq = ctx->real_snap_seq;
   }

This keeps ``SnapSet::seq`` equal to the highest *real* snapshot ID the
object has been brought up to date with, leaving rollback IDs out of the
object's own metadata entirely.

16.6 ``filter_snapc()`` and the Client-Supplied ``SnapContext``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When the client is **not** in pool-snaps mode (i.e., the
``CEPH_OSD_FLAG_ENFORCE_SNAPC`` flag is set), ``ctx->snapc`` is
populated from the client message rather than from ``pool.snapc``.  In
that case the ``snaps`` vector comes entirely from the client and does
not go through ``get_snap_context()``, so rollback IDs are never present
in client-supplied snap contexts.  The ``filter_snapc()`` function
(``src/osd/PG.cc``) removes IDs that are already in the trim queue or
purged-snaps set; it does not need to be modified.

The unmanaged-snap rollback path (``POOL_OP_ROLLBACK_UNMANAGED_SNAP``)
similarly assigns a rollback ID by incrementing ``snap_seq`` without
inserting into the snap list, so the same changes to ``make_writeable()``
apply equally to unmanaged-snap pools.

16.7 ``clone_snaps`` Vector Correctness
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``snaps`` vector computed inside the clone block (lines 9089--9094)
iterates over ``snapc.snaps`` and collects all snap IDs greater than
``ctx->new_snapset.seq``:

.. code:: cpp

   const auto snaps = [&] {
     auto last = find_if_not(
       begin(snapc.snaps), end(snapc.snaps),
       [&](snapid_t snap_id) { return snap_id > ctx->new_snapset.seq; });
     return vector<snapid_t>{begin(snapc.snaps), last};
   }();

Because ``snapc.snaps`` contains **only** real snap IDs (rollback IDs
are never inserted into ``pg_pool_t::snaps`` and therefore never appear
in the vector returned by ``get_snap_context()``), this lambda already
produces the correct set of snap IDs that the new clone covers.  No
change is needed here; the fix is entirely in the clone naming (step 2)
and the gate / seq update (steps 3 and 4) described above.

16.8 Interaction with the JIT Rollback Block
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The JIT rollback block added to ``make_writeable()`` by section 5 runs
**after** the regular clone block and uses ``snapc.seq`` only to
bound the range of pending operations in ``build_pending_ops()``:

.. code:: cpp

   auto ops = build_pending_ops(pool.info, obj_seq, snapc.seq);

Here ``snapc.seq`` is correct: it must include rollback IDs in the
upper bound because rollback IDs are precisely what ``build_pending_ops``
is looking for.  This use of ``snapc.seq`` must **not** be changed to
``real_snap_seq``.

In summary, the two distinct roles of ``snapc.seq`` must be separated:

- Use ``ctx->real_snap_seq`` wherever the seq identifies a snapshot
  clone target (clone naming, gate condition, ``SnapSet::seq`` update).
- Keep ``snapc.seq`` wherever the seq is an upper bound on the pool's
  event history, which includes rollback IDs (``build_pending_ops``,
  ``ORDERSNAP`` validation).

--------------

.. _unmanaged-snap-rollback-later-snapshot:

17. Unmanaged Snap Rollback: Later-Snapshot Awareness
------------------------------------------------------

This is a late design change to address a problem found with the
trimmer performing rollback work for self managed snapshots. In
basic scenarios (e.g. write, create snapshot 1, write, rollback
snapshot 1) the rollback can be performed without knowing the
SnapContext, however there are more complex scenarios (e.g.
write, create snapshot 1, write, create snapshot 2, rollback
snapshot 1) where the rollback cannot be performed without
the SnapContext because the head needs to be cloned before
the rollback can occur.

To fix this POOL_OP_ROLLBACK_UNMANAGED_SNAP needs to provide
the SnapContext and this needs to be stored in the OSDMap
until the trim completes.

17.1 The Problem
~~~~~~~~~~~~~~~~

For *pool-managed* snapshots the full ordered list of snapshots is stored
in ``pg_pool_t::snaps``. When the snaptrimmer (or the JIT write path)
resolves the operation sequence for a rollback of snapshot ``S`` to the
head it can consult ``pg_pool_t::snaps`` and
``pg_pool_t::rollback_snaps`` together to detect whether any snapshot
``T > S`` (a *later snapshot*) was created after ``S``. If ``T``
exists, the algorithm must clone the current head content to ``T``
before replacing the head with the content from ``S``, so that the
object's version at ``T`` is correctly preserved.

For *unmanaged* (self-managed) snapshots there is no equivalent of
``pg_pool_t::snaps``. The client maintains its own per-image snapshot
list (e.g. in librbd's header object) and never registers individual
snap IDs with the MON. Consequently:

-  ``pg_pool_t::snaps`` is always empty for unmanaged-snap pools.
-  ``rollback_snap_info_t`` as currently defined contains only
   ``rollback_id``, ``source_snap``, and ``stamp``.
-  When the snaptrimmer processes a ``POOL_OP_ROLLBACK_UNMANAGED_SNAP``
   rollback it has no information about snapshots that were created after
   ``source_snap``.

If a later snapshot ``T`` exists and the snaptrimmer silently omits the
``clone(head → T)`` step, the clone for ``T`` is never created.
Subsequent reads at snap ``T`` will be misdirected and data integrity is
violated.

17.2 The Fix: Carry the SnapContext in the Rollback Request
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``CEPH_OSD_OP_WRITE`` OSD op already solves an analogous problem:
every write carries the client's full ``SnapContext`` (the ordered list
of all currently live snap IDs together with the current sequence
number). The OSD uses the client-supplied ``SnapContext`` to determine
which clones to create before applying the write.

The same approach is applied to unmanaged-snap rollbacks. The client
issuing ``POOL_OP_ROLLBACK_UNMANAGED_SNAP`` must supply its current
``SnapContext`` in the ``MPoolOp`` message. The MON stores it in the
``rollback_snap_info_t`` record. The snaptrimmer and JIT path can then
consult ``rb_info->snapc`` when resolving the operation sequence,
exactly as they consult ``pg_pool_t::snaps`` for pool-managed rollbacks.

17.3 Changes to ``rollback_snap_info_t``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A new optional field is added to ``rollback_snap_info_t``:

.. code:: cpp

   // src/osd/osd_types.h
   struct rollback_snap_info_t {
     snapid_t       rollback_id;   // unique ID allocated for this rollback
     snapid_t       source_snap;   // the snapshot to restore from
     utime_t        stamp;         // wall-clock time the rollback was issued
     SnapContext    snapc;         // NEW (unmanaged only): client-supplied SnapContext
                                   //   snapc.seq  == highest live snap ID at time of request
                                   //   snapc.snaps == ordered list of all live snap IDs
                                   // Always empty for pool-managed rollbacks.

     void encode(ceph::buffer::list &bl) const;
     void decode(ceph::buffer::list::const_iterator &p);
   };

The field is encoded under a new ``struct_v`` bump so that older OSDs
receiving an incremental OSDMap that does not include this field can
still decode the record (they see an empty ``SnapContext`` and apply the
conservative behaviour of not performing any ``clone(head → T)`` steps,
which is safe because it degrades to the pre-fix behaviour).

17.4 Changes to ``MPoolOp``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No wire-format change to ``MPoolOp`` is required. The message already
carries a ``snapc`` field (of type ``SnapContext``) used by
``POOL_OP_CREATE_UNMANAGED_SNAP``. For
``POOL_OP_ROLLBACK_UNMANAGED_SNAP`` the client simply populates this
field with its current snapshot list before sending the message, exactly
as it does for a write:

.. code:: cpp

   // Caller (e.g. IoCtxImpl::selfmanaged_snap_rollback())
   MPoolOp *m = new MPoolOp(..., POOL_OP_ROLLBACK_UNMANAGED_SNAP);
   m->snapid = snap_id;           // the snap to roll back to (existing field)
   m->snapc  = my_current_snapc;  // NEW: pass current SnapContext (existing field, new use)

17.5 Changes to ``OSDMonitor::prepare_pool_op()``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the ``POOL_OP_ROLLBACK_UNMANAGED_SNAP`` case, the prepare handler
copies the client-supplied ``SnapContext`` into the
``rollback_snap_info_t`` before storing it:

.. code:: cpp

   // src/mon/OSDMonitor.cc -- inside prepare_pool_op()
   case POOL_OP_ROLLBACK_UNMANAGED_SNAP: {
     rollback_snap_info_t rb;
     rb.source_snap  = m->snapid;
     rb.rollback_id  = pp.get_snap_seq() + 1;
     rb.stamp        = ceph_clock_now();
     rb.snapc        = m->snapc;            // NEW: store client SnapContext

     pp.snap_seq = rb.rollback_id;
     pp.set_snap_epoch(pending_inc.epoch);
     pp.rollback_snaps[rb.rollback_id] = rb;

     pending_inc.new_pools[m->pool]    = pp;
     pending_inc.new_rollback_snaps[m->pool][rb.rollback_id] = rb;

     encode(rb.rollback_id, reply_data);
     changed = true;
     break;
   }

The pool-managed case (``POOL_OP_ROLLBACK_SNAP``) is unchanged; its
``rb.snapc`` field is left as a default-constructed empty
``SnapContext``.

17.6 Changes to ``build_pending_ops()``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``build_pending_ops()`` currently collects operations from
``pg_pool_t::snaps`` and ``pg_pool_t::rollback_snaps`` in the range
``(obj_seq, snapc.seq]``. For an unmanaged-snap rollback the
``pg_pool_t::snaps`` map is empty, so no ``SNAP`` entries are emitted
for snap IDs that follow ``source_snap``. This is the root cause of the
missing ``clone(head → T)`` step.

The fix is to fall back to the ``SnapContext`` stored in the matching
``rollback_snap_info_t`` when ``pg_pool_t::snaps`` is empty (i.e. the
pool is in unmanaged mode):

.. code:: cpp

   // src/osd/PrimaryLogPG.cc -- inside build_pending_ops()

   // If pool uses unmanaged snaps and we have a rb_info with a stored
   // snapc, use that as the source of SNAP entries instead of
   // pg_pool_t::snaps (which is empty for unmanaged pools).
   const auto& snap_source =
     (!pool.info.snaps.empty())
       ? pool.info.snaps       // pool-managed: use pg_pool_t::snaps directly
       : snap_id_set_from_snapc(rb_info ? rb_info->snapc : SnapContext{});

   for (snapid_t sid : snap_source) {
     if (sid > obj_seq && sid <= upper_bound)
       ops.push_back({pending_op_t::SNAP, sid, CEPH_NOSNAP});
   }
   for (auto& [rb_id, rb] : pool.info.rollback_snaps) {
     if (rb_id > obj_seq && rb_id <= upper_bound)
       ops.push_back({pending_op_t::ROLLBACK, rb_id, rb.source_snap});
   }
   std::sort(ops.begin(), ops.end(), [](auto& a, auto& b){ return a.id < b.id; });

where ``snap_id_set_from_snapc()`` is a trivial helper that returns a
``std::set<snapid_t>`` from ``SnapContext::snaps``:

.. code:: cpp

   static std::set<snapid_t>
   snap_id_set_from_snapc(const SnapContext& sc) {
     return std::set<snapid_t>(sc.snaps.begin(), sc.snaps.end());
   }

The same change applies to both the JIT path (section 5.4) and the
background rollback path (section 7.5), since both call
``build_pending_ops()``.

17.7 ``preprocess_pool_op()`` Validation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The preprocess handler for ``POOL_OP_ROLLBACK_UNMANAGED_SNAP`` should
validate the client-supplied ``SnapContext`` before accepting the
request. Specifically:

1. ``m->snapc.seq`` must be ``≥ m->snapid`` (the snap being rolled back
   must appear in or before the current sequence).
2. ``m->snapid`` must appear in ``m->snapc.snaps`` (the snap exists in
   the client's list).
3. ``m->snapc`` must be well-formed (strictly decreasing ``snaps``
   vector, all entries ``≤ snapc.seq``).

If any check fails the preprocess handler returns ``-EINVAL``. This
prevents a corrupt or stale ``SnapContext`` from being persisted into
the OSDMap.

.. code:: cpp

   // src/mon/OSDMonitor.cc -- inside preprocess_pool_op()
   case POOL_OP_ROLLBACK_UNMANAGED_SNAP: {
     // ... existing checks (pool mode, snap_seq bounds, removed_snaps_queue) ...

     // NEW: validate client-supplied SnapContext
     if (!m->snapc.is_valid()) {
       _pool_op_reply(op, -EINVAL, osdmap.get_epoch());
       return true;
     }
     if (m->snapc.seq < m->snapid ||
         std::find(m->snapc.snaps.begin(), m->snapc.snaps.end(),
                   m->snapid) == m->snapc.snaps.end()) {
       _pool_op_reply(op, -EINVAL, osdmap.get_epoch());
       return true;
     }
     // ... idempotency check, fall through to prepare ...
   }

17.8 Client-Side Changes (Objecter and librados)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``Objecter::rollback_selfmanaged_snap()`` method (added by section
11.2) must be updated to accept a ``SnapContext`` parameter and forward
it in the ``MPoolOp`` message:

.. code:: cpp

   // src/osdc/Objecter.h  (updated signature)
   void rollback_selfmanaged_snap(int64_t pool,
                                  snapid_t snap_id,
                                  const SnapContext& snapc,  // NEW
                                  Context *onfinish);

   // src/osdc/Objecter.cc
   void Objecter::rollback_selfmanaged_snap(int64_t pool,
                                            snapid_t snap_id,
                                            const SnapContext& snapc,
                                            Context *onfinish) {
     MPoolOp *m = new MPoolOp(monc->get_fsid(), 0, pool,
                               POOL_OP_ROLLBACK_UNMANAGED_SNAP, 0, snap_id);
     m->snapc = snapc;                          // NEW
     pool_op_submit(m, onfinish);
   }

The librados C and C++ wrappers (``IoCtxImpl::selfmanaged_snap_rollback()``
and ``rados_ioctx_selfmanaged_snap_rollback_all()``) are updated to
accept and forward a ``SnapContext`` argument. The caller (librbd or
application code) supplies its current snapshot list at the time of the
rollback request, which is the same list it would supply when performing
a ``CEPH_OSD_OP_WRITE`` at that instant.

17.9 Safety Properties
~~~~~~~~~~~~~~~~~~~~~~~

**Staleness.** If the client's ``SnapContext`` is stale (e.g. a snap
``T`` was created but not yet reflected in the client's list) then
``clone(head → T)`` is not emitted for ``T``. This is the same
as the pre-fix behaviour and is safe by the same argument that applies
to ``CEPH_OSD_OP_WRITE``: a write with a stale ``SnapContext`` similarly
omits the clone for ``T``. The client is responsible for refreshing its
``SnapContext`` before issuing destructive operations.

**Extra snaps.** If the client's ``SnapContext`` contains snap IDs that
were deleted after the rollback request is issued but before the
snaptrimmer processes the object, ``build_pending_ops()`` may emit a
``SNAP`` entry for a snap ID that no longer exists. This is harmless:
the generated clone will be trimmed immediately by the existing
snaptrimmer pass for that snap ID.

**Pool-managed rollbacks.** The ``snapc`` field in
``rollback_snap_info_t`` is always empty for pool-managed rollbacks.
``build_pending_ops()`` uses ``pg_pool_t::snaps`` (non-empty for pool-
managed pools) and never falls back to the stored ``snapc``. Behaviour
for pool-managed rollbacks is therefore unchanged.

**Encoding compatibility.** The ``struct_v`` bump in
``rollback_snap_info_t::encode()`` ensures that an older OSD decoding
the record sees an empty ``snapc``. Because an empty ``snapc`` causes
``snap_id_set_from_snapc()`` to return an empty set, no spurious
``SNAP`` entries are added. The omission of the ``clone(head → T)``
step is acceptable during a mixed-version rolling upgrade; once all OSDs
are upgraded, the correct behaviour is restored.

--------------

18. Post-Rollback Head-Deletion Sweep for Objects Created After the Snapshot
----------------------------------------------------------------------------

18.1 The Gap in Section 9.2 and the Original Section 7 Design
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The original section 9.2 stated:

   "The background trimmer skips such objects (the snap mapper returns no
   entries for them). The JIT path on write creates no rollback clone but
   still updates ``SnapSet::seq`` to record that rollback has been
   processed for this object."

This is **correct for the JIT write path** -- if a write is issued to
object ``foo`` (created after the snapshot) while the rollback is
pending, the JIT path advances ``SnapSet::seq`` to the current
``snapc.seq`` (which is ≥ ``rb_id``), causing
``find_latest_rollback_source()`` to return ``CEPH_NOSNAP`` and the read
redirect to become a no-op. At that point the object correctly does not
exist from the client's perspective (since the JIT path produces no
clone and no data -- the object is deleted before the client write is
applied).

However, the original section 7 design had a **correctness bug** for the
case where **no write ever reaches** object ``foo`` during the lifetime
of the rollback. In that case:

1. The read redirect (section 6) correctly returns ``ENOENT`` for any
   read to ``foo`` while ``rb_id`` is still in ``rollback_snaps_queue``
   (because the redirect to ``source_snap`` finds no clone and returns
   ``-ENOENT``).
2. The snap mapper has no clone registered under ``source_snap`` for
   ``foo`` (since ``foo`` was created after the snapshot), so
   ``get_next_objects_to_trim(source_snap, max)`` never returns ``foo``.
3. ``rollback_then_trim()`` is therefore never called for ``foo``.
4. ``foo``'s head object remains live and unmodified in the object store.
5. When the rollback completes and ``rb_id`` is removed from
   ``rollback_snaps_queue``, ``find_latest_rollback_source()`` finds no
   pending rollback with ``rb_id > foo.SnapSet::seq`` and returns
   ``CEPH_NOSNAP``. The read redirect ceases.
6. Reads to ``foo`` now reach the head directly. **Object ``foo``
   reappears**, which is incorrect: the rollback should have removed it.

This section describes the additional sweep required to close this gap.

18.2 The Fix: Head-Deletion Sweep at Rollback Completion
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The snap mapper only indexes objects by their clone snap IDs. It has no
way to enumerate head objects that have no clones for a given snap ID.
Therefore, after the snap-mapper scan for ``source_snap X`` returns
``nullopt`` (confirming that all objects *with clones* for ``X`` have
been processed), a separate full-PG enumeration of head objects is
required to catch any head objects that were created after snap ``X``
and were never touched by the JIT path.

The criterion for a head object requiring deletion is:

::

    head object OBJ satisfies ALL of:
      1. OBJ exists (is not already deleted)
      2. OBJ.SnapSet::seq < rb_id         (rollback not yet applied by JIT)
      3. no clone of OBJ exists for source_snap X  (object post-dates the snapshot)

The action for each such object is a plain **object deletion**: the head
is removed from the object store, the ``SnapSet`` (which has no clones
to update) is discarded, and a ``DELETE`` ``pg_log_entry_t`` is emitted
for the object.

Note that condition 3 is redundant with the snap-mapper scan completing
without returning ``OBJ`` -- if a clone under ``X`` existed the scan
would already have processed ``OBJ`` via ``rollback_then_trim()``.
Condition 2 prevents double-processing objects that were already handled
by the JIT path (their ``SnapSet::seq >= rb_id``).

18.3 Where the Sweep Runs
~~~~~~~~~~~~~~~~~~~~~~~~~

The sweep runs as an additional phase inside
``AwaitAsyncWork::react(DoSnapWork)``, triggered only in rollback-only
pass mode (``is_trim_pass == false``) and only at the point the snap
mapper returns ``nullopt`` -- i.e., after all clone-bearing objects for
``source_snap X`` have been processed. A trim pass does not need this
sweep: if ``source_snap X`` is being trimmed then any object ``foo``
created after ``X`` has its ``SnapSet::seq > X`` (it was written after
``X`` was taken), and the snap being trimmed is ``X`` itself, not a
rollback source -- the trim pass does not change the head-exists/not
question for objects post-dating ``X``.

**Batched enumeration.** The sweep iterates all head objects in the PG
using ``pgbackend->objects_list_partial()`` (the same function used by
``scan_range_primary()`` in the backfill path). To avoid holding the PG
lock across a full object store scan, the iteration follows the same
batched pattern used by backfill:

- Each call to ``objects_list_partial()`` requests between
  ``osd_backfill_scan_min`` (default **64**) and
  ``osd_backfill_scan_max`` (default **512**) objects, returning a batch
  of up to 512 candidate head objects and an updated cursor (``end``
  position) for the next call. These values are the same defaults used
  by ``PrimaryLogPG::scan_range_primary()``.
- The batch is stored in a ``vector<hobject_t>
  head_deletion_sweep_pending`` field inside the ``Trimming`` state,
  alongside the sweep cursor ``head_deletion_sweep_cursor`` and a
  completion flag ``head_deletion_sweep_done``.
- Each ``AwaitAsyncWork`` cycle **deletes exactly one qualifying object**
  from the front of ``head_deletion_sweep_pending`` (after applying the
  filter from section 18.2), then transitions through ``WaitRepops`` /
  ``WaitTrimTimer`` before returning for the next scheduled invocation of
  the trimmer. This matches the snap trimmer's existing one-object-per-
  cycle discipline and keeps individual transactions small.
- When ``head_deletion_sweep_pending`` is exhausted and the cursor has
  not yet reached ``hobject_t::get_max()``, the next
  ``AwaitAsyncWork`` cycle calls ``objects_list_partial()`` again from
  the stored cursor to replenish the list before processing the next
  object.
- When both the list is empty **and** the cursor has reached
  ``hobject_t::get_max()``, ``head_deletion_sweep_done`` is set to
  ``true`` and the sweep is complete.

The rationale for fetching N objects at a time but deleting only one per
cycle mirrors the backfill design: listing objects in bulk amortises the
cost of ``objects_list_partial()`` (which may involve a RocksDB or
FileStore scan), while deleting one object per cycle keeps the
transaction log manageable and preserves the snap trimmer's throttling
behaviour (``WaitTrimTimer`` sleep between objects).

The deletion of each qualifying head object is submitted as a
replicated transaction via ``simple_opc_submit()``, with the same
``WaitRepops`` acknowledgement cycle used for clone operations.

18.4 Ordering Relative to the Snap-Mapper Scan
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The head-deletion sweep runs **after** the snap-mapper scan returns
``nullopt``, not interleaved with it. This ordering is safe because:

- Objects found by the snap-mapper scan (those with a clone under ``X``)
  have already had their rollback applied by ``rollback_then_trim()``.
  After that call their ``SnapSet::seq`` is advanced to ``>= rb_id``,
  so they will not satisfy condition 2 of the sweep filter and will not
  be deleted.
- Objects found by the head-deletion sweep (those with no clone under
  ``X``) are disjoint from the snap-mapper results for ``X``, so there
  is no conflict or double-processing.

18.5 JIT Interaction
~~~~~~~~~~~~~~~~~~~~

If a write reaches object ``foo`` while the head-deletion sweep is in
progress (i.e. between the snap-mapper scan completing and the sweep
completing), the JIT path runs in ``make_writeable()`` and advances
``foo.SnapSet::seq`` to ``>= rb_id``. This means:

- If the sweep cursor has not yet reached ``foo``, the sweep will
  encounter ``foo`` with ``SnapSet::seq >= rb_id``, fail condition 2,
  and correctly skip it (the JIT path already handled it by deleting the
  head before the write was applied).
- If the sweep has already deleted ``foo`` before the write arrives, the
  write creates a new head object, which is correct (the rollback has
  been applied; a new write after the rollback is valid).

The ``jit_rollback_inflight[rb_id]`` interlock (section 7.2.4) already
ensures the rollback is not declared complete until all in-flight JIT
transactions have committed, preventing a race where the sweep completes
and the rollback is marked done before a concurrent JIT deletion has
been replicated.

18.6 Completion Ordering
~~~~~~~~~~~~~~~~~~~~~~~~

The rollback for ``rb_id`` is marked complete (added to
``pg_info_t::completed_rollbacks``, ``share_pg_info()`` called) only
after **both** the snap-mapper scan and the head-deletion sweep have
reached their respective termination points. The existing nullopt handler
in ``AwaitAsyncWork`` is extended to gate completion on the sweep also
being finished:

.. code:: cpp

   // In AwaitAsyncWork::react(DoSnapWork), nullopt branch (rollback-only pass)
   if (rb_info) {
     if (!head_deletion_sweep_complete) {
       // Start or continue the head-deletion sweep
       start_head_deletion_sweep(rb_info);
       // transitions to WaitRepops for the next batch of deletions
     } else if (pg->jit_rollback_inflight.count(rb_id) &&
                pg->jit_rollback_inflight[rb_id] > 0) {
       pg->rollback_trimq_repeat.insert(rb_id);
     } else {
       pg->rollback_trimq.erase(rb_info->rollback_id);
       pg->recovery_state.adjust_completed_rollbacks(
         [rb_id](auto& cr) { cr.insert(rb_id); });
       pg->write_if_dirty(t);
       pg->recovery_state.share_pg_info();
     }
   }

18.7 Impact on Section 9.2
~~~~~~~~~~~~~~~~~~~~~~~~~~

Section 9.2 should be read with the following correction: the statement
"The background trimmer skips such objects" is correct only in the sense
that the snap-mapper-driven phase of the trimmer skips them. The
**head-deletion sweep** described in this section (section 18) is the
mechanism that correctly handles such objects by deleting their head.
After the head-deletion sweep completes, object ``foo`` no longer exists
and will not reappear when the rollback is declared done and removed from
``rollback_snaps_queue``.

--------------

.. _summary-of-changes-1:

19. Summary of Changes
----------------------

+-----------------------------------+-----------------------------------+
| File                              | Change                            |
+===================================+===================================+
| ``src/include/ceph_fs.h``         | **new** Add                       |
|                                   | ``POOL_OP_ROLLBACK_SNAP = 0x31``  |
|                                   | and                               |
|                                   | ``POOL_OP                         |
|                                   | _ROLLBACK_UNMANAGED_SNAP = 0x32`` |
|                                   | to pool op enum.                  |
+-----------------------------------+-----------------------------------+
| ``src/osd/osd_types.h``           | **new** Add                       |
|                                   | ``rollback_snap_info_t`` struct.  |
|                                   | Add ``rollback_snaps`` map to     |
|                                   | ``pg_pool_t``. Add                |
|                                   | ``completed_rollbacks`` to        |
|                                   | ``pg_info_t`` and ``pg_stat_t``.  |
+-----------------------------------+-----------------------------------+
| ``src/osd/osd_types.cc``          | **modified** Encode/decode new    |
|                                   | fields in ``pg_pool_t``,          |
|                                   | ``pg_info_t``, ``pg_stat_t``      |
|                                   | under new encoding versions.      |
+-----------------------------------+-----------------------------------+
| ``src/osd/OSDMap.h``              | **new** Add                       |
|                                   | ``rollback_snaps_queue``,         |
|                                   | ``new_rollback_snaps``,           |
|                                   | ``new_completed_rollbacks`` to    |
|                                   | ``OSDMap`` and                    |
|                                   | ``OSDMap::Incremental``.          |
+-----------------------------------+-----------------------------------+
| ``src/osd/OSDMap.cc``             | **modified** Encode/decode new    |
|                                   | incremental fields. Update        |
|                                   | ``apply_incremental()`` to merge  |
|                                   | rollback queue additions and      |
|                                   | removals.                         |
+-----------------------------------+-----------------------------------+
| ``src/mon/OSDMonitor.cc``         | **modified** Add rollback cases   |
|                                   | (with idempotency checks) to      |
|                                   | ``preprocess_pool_op()`` and      |
|                                   | ``prepare_pool_op()``. Add        |
|                                   | ``                                |
|                                   | try_prune_completed_rollbacks()`` |
|                                   | called from ``tick()``. Add       |
|                                   | ``                                |
|                                   | require_osd_release >= umbrella`` |
|                                   | guard to both pool-op cases. Add  |
|                                   | ``osd pool rollbacksnap``         |
|                                   | handler in                        |
|                                   | ``prepare_command_impl()``        |
|                                   | alongside ``osd pool mksnap``     |
|                                   | and ``osd pool rmsnap``.          |
+-----------------------------------+-----------------------------------+
| ``src/mon/MonCommands.h``         | **modified** Register             |
|                                   | ``osd pool rollbacksnap``         |
|                                   | command alongside                 |
|                                   | ``osd pool mksnap`` and           |
|                                   | ``osd pool rmsnap``.              |
+-----------------------------------+-----------------------------------+
| ``src/mon/OSDMonitor.h``          | **modified** Declare              |
|                                   | ``t                               |
|                                   | ry_prune_completed_rollbacks()``. |
+-----------------------------------+-----------------------------------+
| ``src/mon/PGMap.cc``              | **new** Add                       |
|                                   | ``calc_completed_rollbacks()``    |
|                                   | analogous to                      |
|                                   | ``calc_purged_snaps()``.          |
+-----------------------------------+-----------------------------------+
| ``src/osd/PrimaryLogPG.cc``       | **modified** Extract              |
|                                   | ``build_pending_ops()``,          |
|                                   | ``execute_clone_plan()``,         |
|                                   | ``update_snapset_for_rollback()`` |
|                                   | from ``make_writeable()``. Extend |
|                                   | ``make_writeable()`` for JIT      |
|                                   | rollback (incrementing            |
|                                   | ``jit_rollback_inflight[rb_id]``  |
|                                   | on entry and decrementing in the  |
|                                   | txn 1 ``on_success`` callback).   |
|                                   | Add read redirect in ``do_op()``. |
|                                   | Add ``rollback_then_trim()``      |
|                                   | per-object function. Refactor     |
|                                   | ``trim_object()`` into            |
|                                   | ``trim_object_snap()`` accepting  |
|                                   | an existing ``OpContextUPtr``.    |
|                                   | Extend ``kick_snap_trim()`` to    |
|                                   | check ``rollback_trimq``. Extend  |
|                                   | ``SnapTrimmer`` state reaction    |
|                                   | handlers:                         |
|                                   | ``NotTrimming::react(KickTrim)``  |
|                                   | to consider ``rollback_trimq``;   |
|                                   | ``Aw                              |
|                                   | aitAsyncWork::react(DoSnapWork)`` |
|                                   | to run the interleaved            |
|                                   | pass-selection (section 7.2.2)    |
|                                   | and call ``rollback_then_trim()`` |
|                                   | per object; completion handling   |
|                                   | for both rollback and trim when   |
|                                   | snap mapper returns ``nullopt``,  |
|                                   | with JIT-inflight interlock check |
|                                   | before marking complete.          |
+-----------------------------------+-----------------------------------+
| ``src/osd/PrimaryLogPG.h``        | **modified** Add                  |
|                                   | ``snap_being_processed`` and      |
|                                   | ``is_trim_pass`` fields to the    |
|                                   | ``Trimming`` state struct. All    |
|                                   | other state structs               |
|                                   | (``NotTrimming``,                 |
|                                   | ``WaitReservation``,              |
|                                   | ``AwaitAsyncWork``,               |
|                                   | ``WaitRepops``,                   |
|                                   | ``WaitTrimTimer``,                |
|                                   | ``WaitRWLock``) and the           |
|                                   | ``SnapTrimmer`` machine class     |
|                                   | declaration are unchanged.        |
+-----------------------------------+-----------------------------------+
| ``src/osd/PG.h``                  | **modified** Add                  |
|                                   | ``rollback_trimq``,               |
|                                   | ``rollback_trimq_repeat``, and    |
|                                   | ``jit_rollback_inflight`` fields  |
|                                   | alongside existing ``snap_trimq`` |
|                                   | and ``snap_trimq_repeat``.        |
+-----------------------------------+-----------------------------------+
| ``src/osd/PG.cc``                 | **modified** Extend               |
|                                   | ``on_active_advmap()`` to         |
|                                   | populate ``rollback_trimq`` from  |
|                                   | ``new_rollback_snaps`` and drain  |
|                                   | entries confirmed by              |
|                                   | ``new_completed_rollbacks``,      |
|                                   | alongside the existing            |
|                                   | ``snap_trimq`` population from    |
|                                   | ``new_removed_snaps``.            |
+-----------------------------------+-----------------------------------+
| ``src/osd/PeeringState.cc``       | **modified** Initialise           |
|                                   | ``rollback_trimq`` on PG          |
|                                   | activation (alongside             |
|                                   | ``snap_trimq``). Include          |
|                                   | ``completed_rollbacks`` in        |
|                                   | ``prepare_stats_for_publish()``   |
|                                   | output.                           |
+-----------------------------------+-----------------------------------+
| ``src/osdc/Objecter.h``           | **new** Add                       |
|                                   | ``rollback_pool_snap()`` and      |
|                                   | ``rollback_selfmanaged_snap()``   |
|                                   | pool-op methods alongside         |
|                                   | existing ``create_pool_snap()``,  |
|                                   | ``delete_pool_snap()``,           |
|                                   | ``allocate_selfmanaged_snap()``,  |
|                                   | ``delete_selfmanaged_snap()``.    |
+-----------------------------------+-----------------------------------+
| ``src/osdc/Objecter.cc``          | **new** Implement                 |
|                                   | ``rollback_pool_snap()`` and      |
|                                   | ``rollback_selfmanaged_snap()``   |
|                                   | dispatching                       |
|                                   | ``POOL_OP_ROLLBACK_SNAP`` and     |
|                                   | ``                                |
|                                   | POOL_OP_ROLLBACK_UNMANAGED_SNAP`` |
|                                   | respectively via the existing     |
|                                   | ``_pool_op()`` helper.            |
+-----------------------------------+-----------------------------------+
| ``src/include/rados/librados.h``  | **new** Add C API:                |
|                                   | ``rados_ioctx_snap_rollback_      |
|                                   | all(io, snap_name, rollback_id)`` |
|                                   | and                               |
|                                   | ``rado                            |
|                                   | s_ioctx_selfmanaged_snap_rollback |
|                                   | _all(io, snap_id, rollback_id)``. |
+-----------------------------------+-----------------------------------+
| `                                 | **new** Add C++ API:              |
| `src/include/rados/librados.hpp`` | ``IoCtx::snap_r                   |
|                                   | ollback(snap_name, rollback_id)`` |
|                                   | and                               |
|                                   | ``IoCtx::selfmanaged_snap_        |
|                                   | rollback(snap_id, rollback_id)``. |
+-----------------------------------+-----------------------------------+
| ``src/librados/IoCtxImpl.h``      | **new** Declare                   |
|                                   | ``snap_rollback()`` and           |
|                                   | ``selfmanaged_snap_rollback()``   |
|                                   | on ``IoCtxImpl``.                 |
+-----------------------------------+-----------------------------------+
| ``src/librados/IoCtxImpl.cc``     | **new** Implement                 |
|                                   | ``snap_rollback()`` and           |
|                                   | ``selfmanaged_snap_rollback()``   |
|                                   | via                               |
|                                   | `                                 |
|                                   | `Objecter::rollback_pool_snap()`` |
|                                   | /                                 |
|                                   | ``rollback_selfmanaged_snap()``.  |
+-----------------------------------+-----------------------------------+
| `                                 | **new** Add                       |
| `src/include/neorados/RADOS.hpp`` | ``RADOS::rollback_pool_snap()``   |
|                                   | and                               |
|                                   | ``RAD                             |
|                                   | OS::rollback_selfmanaged_snap()`` |
|                                   | alongside existing                |
|                                   | ``create_pool_snap()``,           |
|                                   | ``delete_pool_snap()``.           |
+-----------------------------------+-----------------------------------+
| ``src/neorados/RADOS.cc``         | **new** Implement                 |
|                                   | ``rollback_pool_snap_()`` and     |
|                                   | ``rollback_selfmanaged_snap_()``  |
|                                   | dispatch functions.               |
+-----------------------------------+-----------------------------------+
| ``src/tools/rados/rados.cc``      | **modified** Add                  |
|                                   | ``rollbacksnap <snap-name>``      |
|                                   | command alongside                 |
|                                   | ``mksnap``/``rmsnap``. Supports   |
|                                   | both pool-managed                 |
|                                   | (``snap_rollback()``) and         |
|                                   | selfmanaged                       |
|                                   | (``selfmanaged_snap_rollback()``) |
|                                   | modes. Add usage text and error   |
|                                   | handling.                         |
+-----------------------------------+-----------------------------------+
| ``src/librbd/opera                | **modified** Add pool-op fast     |
| tion/SnapshotRollbackRequest.cc`` | path in                           |
|                                   | ``send_rollback_objects()`` via   |
|                                   | ``selfmanaged_snap_rollback()``;  |
|                                   | fall back to per-object           |
|                                   | ``selfmanaged_snap_rollback``     |
|                                   | when the pool-op is unavailable.  |
+-----------------------------------+-----------------------------------+
| ``src/test/osd/RadosModel.h``     | **modified** Add                  |
|                                   | ``SnapRollbackOp`` class. Add     |
|                                   | ``TEST_OP_SNAP_ROLLBACK`` to      |
|                                   | ``TestOpType`` enum. Extend       |
|                                   | ``Rad                             |
|                                   | osTestContext::roll_back_pool()`` |
|                                   | to update the full-pool model.    |
|                                   | Supports both pool-managed        |
|                                   | (``snap_rollback()``) and         |
|                                   | selfmanaged                       |
|                                   | (``selfmanaged_snap_rollback()``) |
|                                   | modes.                            |
+-----------------------------------+-----------------------------------+
| ``src/test/osd/TestRados.cc``     | **modified** Register             |
|                                   | ``SnapRollbackOp`` in the         |
|                                   | op-weight table. Mix with         |
|                                   | existing ``RollbackOp`` weight    |
|                                   | for both pool-snaps and           |
|                                   | selfmanaged-snaps modes.          |
+-----------------------------------+-----------------------------------+
| ``src/pybind/rados/rados.pyx``    | **new** Add ``rollback_snap()``   |
|                                   | and                               |
|                                   | ``rollback_self_managed_snap()``  |
|                                   | methods to ``Ioctx``; new C       |
|                                   | function declarations in          |
|                                   | ``c_rados.pxd``.                  |
+-----------------------------------+-----------------------------------+
| ``src/pybind/rados/c_rados.pxd``  | **new** Add                       |
|                                   | ``                                |
|                                   | rados_ioctx_snap_rollback_all()`` |
|                                   | and                               |
|                                   | ``rados_ioctx_                    |
|                                   | selfmanaged_snap_rollback_all()`` |
|                                   | Cython declarations.              |
+-----------------------------------+-----------------------------------+
| `                                 | **new** Add ``PoolSnapRollback``, |
| `src/test/librados/snapshots.cc`` | ``PoolSnapRollbackNoent``,        |
|                                   | ``PoolSelfmanagedSnapRollback`` C |
|                                   | API test cases.                   |
+-----------------------------------+-----------------------------------+
| ``src                             | **new** Add                       |
| /test/librados/snapshots_cxx.cc`` | ``PoolSnapRollbackPP``,           |
|                                   | ``PoolSnapRollbackNoentPP``,      |
|                                   | ``PoolSnapRollbackIdempotentPP``, |
|                                   | `                                 |
|                                   | `PoolSelfmanagedSnapRollbackPP``, |
|                                   | ``PoolSelf                        |
|                                   | managedSnapRollbackIdempotentPP`` |
|                                   | C++ API test cases.               |
+-----------------------------------+-----------------------------------+
| ``src/test/lib                    | **new** Add ``snap_rollback()``   |
| rados_test_stub/TestIoCtxImpl.h`` | and                               |
|                                   | ``selfmanaged_snap_rollback()``   |
|                                   | virtual methods.                  |
+-----------------------------------+-----------------------------------+
| ``src/test/librad                 | **new** Declare overrides.        |
| os_test_stub/TestMemIoCtxImpl.h`` |                                   |
+-----------------------------------+-----------------------------------+
| ``src/test/librado                | **new** Implement in-memory stub  |
| s_test_stub/TestMemIoCtxImpl.cc`` | for pool-level rollback: allocate |
|                                   | fake rollback ID, record pending  |
|                                   | rollback in cluster state.        |
+-----------------------------------+-----------------------------------+
| ``src/test/librados_t             | **new** Add ``MOCK_METHOD``       |
| est_stub/MockTestMemIoCtxImpl.h`` | declarations and ``ON_CALL``      |
|                                   | defaults for ``snap_rollback()``  |
|                                   | and                               |
|                                   | ``selfmanaged_snap_rollback()``.  |
+-----------------------------------+-----------------------------------+
| ``src/qa/tasks/rados.py``         | **modified** Add                  |
|                                   | ``"snap_rollback"`` to the        |
|                                   | op-weight field list; update      |
|                                   | docstring.                        |
+-----------------------------------+-----------------------------------+
| ``src/qa/suites/*/snaps*.yaml``   | **modified** Add                  |
| (and related)                     | ``snap_rollback: 10`` alongside   |
|                                   | existing ``rollback: 50`` entries |
|                                   | in all non-Crimson,               |
|                                   | non-upgrade-first-half workload   |
|                                   | YAML files.                       |
+-----------------------------------+-----------------------------------+
| ``src/osd/PrimaryLogPG.h``        | **modified** Add                  |
|                                   | ``head_deletion_sweep_cursor``,   |
|                                   | ``head_deletion_sweep_done``, and |
|                                   | ``head_deletion_sweep_pending``   |
|                                   | fields to the ``Trimming`` state  |
|                                   | struct alongside                  |
|                                   | ``snap_being_processed`` and      |
|                                   | ``is_trim_pass``.                 |
+-----------------------------------+-----------------------------------+
| ``src/osd/PrimaryLogPG.cc``       | **modified** Add                  |
|                                   | ``start_head_deletion_sweep()``   |
|                                   | and                               |
|                                   | ``delete_post_snapshot_heads()``  |
|                                   | helpers. Extend                   |
|                                   | ``AwaitAsyncWork::react           |
|                                   | (DoSnapWork)`` nullopt branch to  |
|                                   | run the sweep (fetching up to     |
|                                   | ``osd_backfill_scan_max`` objects |
|                                   | per batch, deleting one per       |
|                                   | cycle) and gate completion on its |
|                                   | finishing. Extend the per-object  |
|                                   | loop in ``rollback_then_trim()``  |
|                                   | pass initialisation to reset the  |
|                                   | sweep cursor and pending list at  |
|                                   | the start of each rollback-only   |
|                                   | pass.                             |
+-----------------------------------+-----------------------------------+
| ``src/test/librados/              | **new** Add three C++ GTest       |
| snapshots_cxx.cc``                | cases against a real cluster      |
|                                   | (style matching WI-16-f):         |
|                                   | ``PoolSnapRollbackPostSnap        |
|                                   | Object`` -- creates snap1, writes |
|                                   | ``foo``, verifies read succeeds   |
|                                   | before rollback, issues rollback, |
|                                   | verifies ``-ENOENT`` with         |
|                                   | trimmer frozen, enables trimmer,  |
|                                   | waits, verifies ``-ENOENT``       |
|                                   | after sweep completes.            |
|                                   | ``PoolSnapRollbackPostSnap        |
|                                   | ObjectSnap2`` -- creates snap1,   |
|                                   | writes ``foo``, creates snap2,    |
|                                   | rolls back to snap1, verifies     |
|                                   | head ``-ENOENT`` and snap2        |
|                                   | readable both before and after    |
|                                   | trim. ``PoolSnapRollbackPostSnap  |
|                                   | ObjectWriteAfterRollback`` --     |
|                                   | creates snap1, writes ``foo``,    |
|                                   | rolls back to snap1, re-writes    |
|                                   | ``foo`` (triggering JIT path),    |
|                                   | enables trimmer, waits, verifies  |
|                                   | head survives with                |
|                                   | post-rollback content.            |
+-----------------------------------+-----------------------------------+

