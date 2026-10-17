# src/osd/scrubber/ — Scrubbing Subsystem

Scrubbing verifies data integrity by comparing object replicas (or EC shards) across OSDs. Two modes: **shallow** (metadata only) and **deep** (reads and checksums all data). Lifecycle managed by a **boost::statechart state machine** (`scrub_machine.h/cc`).

Before scrubbing, the primary must reserve slots on all replica OSDs (`scrub_reservations.h/cc`) — prevents scrub storms but can delay necessary scrubs.

The state machine is complex with many states and transitions. Changes to scrub timing interact with recovery, backfill, and client I/O — easy to affect cluster performance inadvertently.
