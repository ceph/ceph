# src/osd/ — Object Storage Daemon

## Purpose

The OSD (Object Storage Daemon) is the core data storage daemon in Ceph. Each OSD process manages one or more storage devices and is responsible for storing objects, handling replication and erasure coding, recovery, scrubbing, and serving client I/O.

The OSD manages **Placement Groups (PGs)** — the fundamental unit of data distribution. Each PG is a logical group of objects that maps to a set of OSDs via CRUSH. The OSD implements a layered architecture: the OSD daemon handles map processing, messaging, and heartbeats; PG handles peering, state management, and recovery coordination; PGBackend (ReplicatedBackend or ECBackend) handles the specifics of replication vs. erasure coding; and the ObjectStore handles on-disk persistence.

The peering state machine (boost::statechart-based) is one of the most complex parts of the codebase. It determines which OSDs have authoritative copies of a PG and brings them into sync.

## Key Files

| File | Role |
|------|------|
| `OSD.h/cc` | Main daemon class. Message dispatch, OSDMap processing, heartbeats, PG lifecycle. |
| `PG.h/cc` | Base Placement Group class. Peering state management, recovery coordination. |
| `PrimaryLogPG.h/cc` | Primary PG implementation. Handles client ops via `do_op()`/`do_osd_ops()`, executes RADOS operations. The main workhorse. |
| `PeeringState.h/cc` | Peering state machine (boost::statechart). States: Reset, Started, Primary, Stray, Peering, Active, etc. |
| `PGBackend.h/cc` | Abstract backend interface for PG I/O operations. |
| `ReplicatedBackend.h/cc` | Replicated pool I/O — sends writes to all replicas. |
| `ECBackend.h/cc` | Erasure code pool I/O — encodes data into chunks, distributes to shards. |
| `ECCommon.h/cc` | Shared EC utilities. |
| `ECUtil.h/cc` | EC chunk/stripe calculations. |
| `OSDMap.h/cc` | Cluster topology map. Pool properties, PG-to-OSD mapping, OSD states. Used by many subsystems, not just OSD. |
| `osd_types.h/cc` | Core type definitions: `pg_t`, `spg_t`, `hobject_t`, `ghobject_t`, `eversion_t`, `pg_info_t`, etc. |
| `PGLog.h/cc` | PG operation log — tracks recent operations for recovery. |
| `SnapMapper.h/cc` | Mapping between snapshots and clone objects. |
| `Session.h/cc` | Client session management. |
| `Watch.h/cc` | Object watch/notify mechanism (used by RBD). |
| `OpRequest.h/cc` | Operation request wrapper with tracking. |
| `ClassHandler.h/cc` | Loads and executes RADOS class (cls) methods. |
| `osd_perf_counters.h` | Performance counter definitions. |

## Directory Structure

| Subdirectory | Contents |
|---|---|
| `scrubber/` | Scrubbing subsystem — data integrity verification. See [scrubber/AGENTS.md](scrubber/AGENTS.md) |
| `scheduler/` | Op scheduling with mClock QoS (`mClockScheduler.h/cc`) |

## Patterns and Idioms

### PG Lock Ordering
The OSD has strict lock ordering to avoid deadlocks. The general rule is: PG lock → shard lock → OSD-level locks. See comments in `OSD.h` for the full ordering.

### Operation Dispatch Flow
1. Client sends `MOSDOp` message
2. `OSD::ms_dispatch()` → `OSD::dispatch_op_fast()` → enqueue on PG's op queue
3. PG dequeues → `PrimaryLogPG::do_op()` → `PrimaryLogPG::do_osd_ops()` executes the RADOS ops
4. For writes: create `ObjectStore::Transaction`, submit to ObjectStore, replicate to secondaries

### Peering State Machine
PG peering uses boost::statechart with states defined in `PeeringState.h`. Key states:
- `Reset` → initial or recovery from failure
- `Peering` → discovering which OSDs have data, choosing authoritative log
- `Active` → PG is serving I/O
- `Recovery` → missing objects being recovered from peers

### EC Backend Architecture
There are two EC implementations, switched at runtime by `ECSwitch.h`:

- **FastEC** (optimized): `ECBackend.cc`, `ECCommon.cc`, `ECExtentCache.cc`, `ECTransaction.cc`, `ECUtil.cc` — the current active development target. All new EC work should go here unless explicitly told otherwise.
- **Classic/Legacy EC**: `ECBackendL.cc`, `ECCommonL.cc`, `ECExtentCacheL.cc`, `ECTransactionL.cc`, `ECUtilL.cc` — the original implementation, kept for stability while FastEC matures. Likely to be deleted in the future. Do not add features to these files.

`ECSwitch` delegates to one or the other based on pool configuration (`allows_ecoptimizations()`). Related shared files: `ECMsgTypes.h/cc` (message types), `ECTypes.h` (shared types), `ECListener.h` (PG callback interface), `ECInject.h/cc` (error injection for testing), `ECOmapJournal.h/cc` (omap journaling).

### Object Identifiers
- `hobject_t` — hash-ordered object identifier (pool, hash, key, oid, snap, namespace)
- `ghobject_t` — extends `hobject_t` with `generation` and `shard_id` for EC objects
- `pg_t` — placement group identifier (pool, seed)
- `spg_t` — sharded PG identifier (pg_t + shard_id)

## Dependencies

Uses `os/` (ObjectStore), `msg/` (Messenger), `crush/`, `erasure-code/`, `cls/`, `common/`, `include/`. Key messages: `MOSDOp`, `MOSDRepOp`, `MOSDPGInfo`, `MOSDPGLog`.

## Navigation Hints

- To understand how a client read/write is processed, start at `PrimaryLogPG::do_op()` → `do_osd_ops()`
- To understand peering, start at `PeeringState.h` and follow the state transitions
- To understand recovery, look at `PrimaryLogPG::start_recovery_ops()` and the RecoveryBackend classes
- To understand EC encode/decode, start at `ECBackend::handle_sub_write()` and `ECUtil.h`
- To add a new RADOS operation, add the op constant to `include/rados.h`, then implement in `PrimaryLogPG::do_osd_ops()`

## Gotchas

- `OSDMap` is NOT only used by the OSD. Monitors, MDS, clients, and librbd all use it. Changes to OSDMap affect the entire cluster.
- The peering state machine is extremely complex (~40 states). Modifying it without understanding the full state graph will introduce subtle bugs.
- `hobject_t` vs. `ghobject_t`: ghobject_t adds generation/shard_id for EC objects. Most internal OSD code uses `ghobject_t`.
- `PrimaryLogPG::do_osd_ops()` is very large. Individual op implementations are in the giant switch statement — search by `CEPH_OSD_OP_*` constant.
