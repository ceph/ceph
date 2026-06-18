# src/osd/scrubber/ — Scrubbing Subsystem

## Purpose

The scrubbing subsystem verifies data integrity by comparing object replicas (or EC shards) across OSDs. It detects and can repair data corruption, bitrot, and metadata inconsistencies. Scrubbing operates at the PG level — each PG is independently scheduled and scrubbed.

There are two scrub modes:
- **Shallow scrub**: compares metadata (size, attributes, omap) across replicas
- **Deep scrub**: additionally reads and checksums all object data

The scrub lifecycle is managed by a **boost::statechart state machine** that coordinates chunk-by-chunk scanning across the primary and replica OSDs.

## Key Files

| File | Role |
|------|------|
| `pg_scrubber.h/cc` | Per-PG scrubber implementation — owns the scrub state machine and coordinates the scrub process. |
| `scrub_machine.h/cc` | Scrub state machine (boost::statechart). States: NotActive, NewChunk, WaitPushes, WaitLastUpdate, BuildMap, WaitReplicas, WaitDigestUpdate, etc. |
| `scrub_backend.h/cc` | Comparison logic — compares replica maps to detect inconsistencies and determine repairs. |
| `osd_scrub.h/cc` | OSD-level scrub coordination — manages the scrub queue across all PGs on this OSD. |
| `osd_scrub_sched.h/cc` | Scrub scheduling queue — determines which PGs to scrub and when. |
| `scrub_reservations.h/cc` | Replica reservation management — reserves scrub slots on replica OSDs before starting. |
| `scrub_resources.h/cc` | Resource limiting — controls how many PGs can scrub concurrently. |
| `scrub_job.h/cc` | Scrub job definition — encapsulates a single scrub task's parameters and state. |
| `ScrubStore.h/cc` | Persistent storage of scrub errors (stored as objects in the PG). |
| `PrimaryLogScrub.h/cc` | Integration with PrimaryLogPG — bridges the PG and scrubber. |

## Patterns and Idioms

### Scrub State Machine Flow
1. **NotActive** → scrub scheduled, PG enters scrub mode
2. **NewChunk** → select next chunk of objects to scrub
3. **WaitPushes** → wait for any pending recovery pushes
4. **WaitLastUpdate** → wait for outstanding writes to complete
5. **BuildMap** → primary builds scrub map for its chunk
6. **WaitReplicas** → request and wait for replica scrub maps
7. **WaitDigestUpdate** → compare maps, repair if needed
8. Loop back to NewChunk until all objects are scrubbed

### Reservation Protocol
Before scrubbing, the primary must reserve scrub slots on all replica OSDs. This prevents too many concurrent scrubs from overloading the cluster. Reservations are managed by `scrub_reservations.h/cc`.

### Chunk-Based Processing
Objects are scrubbed in chunks (configurable size) to avoid holding PG locks for too long and to allow interleaving with regular I/O.

## Dependencies

Uses `osd/PG.h`, `osd/PrimaryLogPG.h`, `os/ObjectStore.h`, `common/`. Key messages: `MOSDRepScrub`, `MOSDRepScrubMap`.

## Navigation Hints

- To understand the scrub lifecycle: read the state machine in `scrub_machine.h`
- To understand how replicas are compared: see `scrub_backend.cc`
- To understand scheduling policy: see `osd_scrub_sched.cc`
- To modify scrub behavior: likely need to change both the state machine and the backend

## Gotchas

- The state machine is complex with many states and transitions. Drawing the state diagram helps when making changes.
- Scrub interacts with recovery, backfill, and client I/O. Changes to scrub timing can affect cluster performance.
- Deep scrub reads all data — it can generate significant I/O load. The scheduling system throttles this.
- Scrub errors are stored persistently. The error reporting and repair path is separate from the detection path.
- The reservation system prevents scrub storms but can also delay necessary scrubs if many PGs need scrubbing simultaneously.
