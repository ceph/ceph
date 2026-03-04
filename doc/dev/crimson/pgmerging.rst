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

1. **Extraction**: Source PGs are removed from their local shard maps.
2. **Transportation**: PGs travel via ``seastar::foreign_ptr``.
3. **Storage**: Target shards hold PGs in ``crimson::local_shared_foreign_ptr``.
   This ensures that when the reference is dropped, the destruction 
   message is automatically routed back to the ``birth_shard``.

.. _synchronization:

Target and Source PG Synchronization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Merging involves a race between Source PG "check-in" and Target PG 
readiness.

* **Merge Waiter**: A per-shard registry (``local_merge_waiter``) that 
  holds arriving Source PGs till the target PG arrives.
* **Shared Promises**: If the Target PG arrives at the merge point 
  before its sources, it suspends on a ``seastar::shared_promise``.
* **Resumption**: The promise is fulfilled only when the ``sources_needed`` 
  count is met in the waiter.

.. _map_consistency:

Map Advancement and Pipeline Stalling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
To ensure the merge occurs between PGs at the same logical time:

* The Target PG's peering pipeline is stalled at the ``PGAdvanceMap`` 
  stage.
* This ensures that the Target PG does not move to a newer OSD Map 
  epoch until the Source PGs (frozen at the merge epoch) are 
  successfully integrated.
* We then resume map advancement for the target PG.

Cleanup Phase
-------------
After the merge, ``perform_source_cleanup()`` drops the local references. 
The ``local_shared_foreign_ptr`` then handles the "return-to-home" shard
logic for the Source PGs ensuring that they are destroyed on their ``birth_shard``.