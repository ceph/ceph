===========================================
PG Merge Synchronization in Crimson
===========================================

This document describes the infrastructure used to coordinate the merging
of Placement Groups (PGs) in Crimson.

Background
----------
Crimson utilizes a shared-nothing memory model where each PG is owned by
a specific CPU core. Merging requires cross-core coordination while
maintaining memory safety and epoch consistency.

Core Concepts
-------------

.. _migration_safety:

Migration Safety (Memory Ownership)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
PGs are managed by their ``birth_shard``. To prevent cross-shard
deallocation crashes:

1. **Extraction**: ``ShardServices::extract_pg()`` removes the source PG
   from its shard map so it stops receiving messages.
2. **Transportation**: The PG crosses cores in a ``seastar::foreign_ptr``.
3. **Storage**: The target shard stores each source in
   ``crimson::local_shared_foreign_ptr<Ref<PG>>`` inside the target PG's
   rendezvous map. When the target drops the last reference after
   ``PG::merge_from()``, destruction is routed back to the source
   ``birth_shard``.

.. _synchronization:

Target and Source PG Synchronization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Merging involves a race between source PGs checking in and the target PG
reaching the merge point in ``PGAdvanceMap``.

Synchronization state lives on the **target PG**, not in a per-shard
registry:

* **``merge_rendezvous_t``**: A map of arrived sources plus a
  ``seastar::semaphore`` counting first-time registrations.
* **``PG::add_merge_source()``**: Called on the target's shard when
  ``ShardServices::register_merge_source()`` delivers a source PG.
  Duplicate source pgids are ignored (idempotent under replay).
* **``PG::collect_merge_sources(n)``**: Waits on the rendezvous semaphore
  for ``n`` arrivals (one signal per first insert), asserts the map has
  ``n`` entries, then returns sources and clears the rendezvous.  If
  ``reset_merge_rendezvous()`` breaks the wait, returns an empty map.
* **``PG::reset_merge_rendezvous()``**: Clears in-flight handoffs and
  resets the semaphore. Called on ``PG::stop()`` or after a Seastore
  cross-shard abort so a failed try cannot leave stale sources for a
  later epoch.

**Cross-shard handoff**: ``register_merge_source()`` resolves the target
shard, extracts the source locally, and uses ``invoke_on`` to call
``add_merge_source()`` on the target PG.

.. _map_consistency:

Map Advancement and Pipeline Stalling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
To ensure the merge occurs between PGs at the same logical time:

* ``PGAdvanceMap::check_for_merges()`` detects ``pg_num`` shrink between
  map epochs and returns a ``merge_result_t`` (source, target, or none).
* ``merge_pg()`` performs merge-specific work only (Seastore eligibility,
  rendezvous collection, ``PG::merge_from()`` on the target).
* A single ``PGAdvanceMap`` operation may batch multiple map epochs
  (``from`` through ``to``). Only a **merge source** stops the epoch
  loop early: ``finish_merge_source()`` commits ``rctx``, stops the PG,
  and calls ``register_merge_source()``. A **merge target** runs
  ``merge_from()`` at the merge epoch, then continues advancing through
  the remaining epochs up to ``to`` (matching classic OSD behavior).
  Normal completion at the end of ``start()`` activates the final map
  and commits ``rctx`` (also covering a completed merge target).
* The target does not call ``merge_from()`` until sources frozen at the
  merge epoch have arrived on its rendezvous.

.. _seastore_cross_shard:

Seastore Cross-Shard Merges
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Seastore does not support merging collections across reactor shards.
Before a source hands off or a target collects sources, ``merge_pg()``
calls ``ShardServices::seastore_merge_shards_ok()`` with the full sibling
set. If the target and any source map to different shards, the OSD aborts
the attempt: clears ready-to-merge state, sends
``MOSDPGReadyToMerge{ ready=false }`` for the source so the monitor does
not commit the unsafe ``pg_num`` decrement, resets the target rendezvous,
and sends ``MOSDPGStopMerge`` to the monitor (once per pool per OSD).
The monitor clamps ``pg_num_target`` to ``pg_num`` and clears
``FLAG_CRIMSON_ALLOW_PG_MERGE`` for that pool until operators re-enable
merging (``crimson_allow_pg_merge``).

Cleanup
-------
After ``complete_rctx()``, the target releases its ``local_shared_foreign_ptr``
references; source PG objects are destroyed on their ``birth_shard``.

When the OSD finishes consuming a new OSDMap in ``OSD::committed_osd_maps()``,
``OSDSingletonState::prune_sent_ready_to_merge()`` drops ``sent_ready_to_merge_source``
entries (and stale ``pools_merge_stopped_reported`` pool ids) for PGs that no
longer exist in the committed map, mirroring classic ``OSD::consume_map()``.
