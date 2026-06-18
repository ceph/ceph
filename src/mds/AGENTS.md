# src/mds/ — Metadata Server

## Purpose

The MDS (Metadata Server) manages the CephFS filesystem namespace. It maintains an in-memory metadata cache of inodes, dentries, and directory fragments, provides distributed locking for concurrent access, and journals all metadata mutations for crash recovery.

The MDS uses a **hierarchical metadata cache** built from `CInode` (inode), `CDentry` (directory entry), and `CDir` (directory fragment) objects. For multi-MDS scaling, the namespace is partitioned into **subtrees**, each owned by one MDS rank. Subtrees can be migrated between MDS ranks for load balancing.

Client access is controlled through a **capability (cap)** system — clients receive read/write/exclusive capabilities on inodes, which the MDS grants and revokes to maintain consistency.

## Key Files

| File | Role |
|------|------|
| `MDSDaemon.h/cc` | Daemon process lifecycle — startup, shutdown, signal handling. |
| `MDSRank.h/cc` | Per-rank MDS logic. Central coordinator for all subsystems. A daemon process runs one rank. |
| `MDCache.h/cc` | The metadata cache — manages CInode/CDir/CDentry tree, discovery, replication, migration. Largest file (~15K lines). |
| `Server.h/cc` | Handles client requests — lookups, creates, unlinks, renames, setattr, etc. |
| `Locker.h/cc` | Distributed lock manager — cap grants/revocations, file locks, scatter locks (~6K lines). |
| `CInode.h/cc` | Inode cache entry. Holds inode metadata, lock state, capabilities. |
| `CDir.h/cc` | Directory fragment cache entry. Directories can be split into fragments for parallelism. |
| `CDentry.h/cc` | Dentry (name→inode link) cache entry. |
| `MDLog.h/cc` | Metadata journal management — writing and trimming log segments. |
| `MDBalancer.h/cc` | Load balancing across MDS ranks — decides when/what to migrate. |
| `Migrator.h/cc` | Subtree/metadata migration protocol between MDS ranks. |
| `SessionMap.h/cc` | Client session tracking and persistence. |
| `Beacon.h/cc` | MDS→Monitor heartbeat reporting. |
| `SnapRealm.h/cc` | Snapshot realm management (snapshot hierarchy). |
| `SnapServer.h/cc` / `SnapClient.h/cc` | Snapshot ID allocation and distribution. |
| `StrayManager.h/cc` | Orphaned/stray inode cleanup (purging). |
| `Capability.h/cc` | Client capability grant/revoke structures. |
| `Mutation.h/cc` | Metadata mutation tracking. |
| `MDSAuthCaps.h/cc` | MDS authorization/capability checking. |
| `SimpleLock.h` / `ScatterLock.h` / `LocalLock.h` | Lock type implementations. |
| `locks.h` | Lock state machine definitions — all lock type state transitions. |

## Directory Structure

| Subdirectory | Contents |
|---|---|
| `events/` | Journal event types: `EUpdate`, `ESession`, `EMetaBlob`, `EExport`, `EFragment`, etc. |
| `balancers/` | Load balancing policy implementations (Lua-based, e.g., `greedyspill.lua`) |

## Patterns and Idioms

### Metadata Mutation Flow
1. Client request arrives at `Server::handle_client_request()`
2. Path traversal via `MDCache::path_traverse()` discovers/loads inodes
3. Locks acquired via `Locker::acquire_locks()`
4. Mutation journaled: create `EUpdate` with `EMetaBlob` → `MDLog::submit_entry()`
5. On journal flush: apply changes to in-memory cache
6. Reply to client

### Lock Types
- `SimpleLock` — basic read/write/exclusive lock (most metadata)
- `ScatterLock` — aggregation lock for distributed counters (e.g., directory stats that are updated by multiple clients)
- `LocalLock` — lock with no remote replication (local-only state)

Each lock type has a state machine defined in `locks.h`.

### Capability (Cap) System
Clients receive capabilities granting permission to cache metadata locally (read, write, buffer, exclusive). The Locker manages cap grants and revocations. See `Capability.h` for the cap types.

### Directory Fragmentation
Directories can be split into fragments (`frag_t`) for parallel access across MDS ranks. `CDir` represents one fragment. See `frag_t` in `include/ceph_frag.h`.

## Dependencies

Uses `common/`, `msg/`, `osdc/` (Objecter for RADOS I/O), `include/`. Key messages: `MClientRequest`, `MClientCaps`, `MDirUpdate`.

## Navigation Hints

- To understand how a specific client operation (e.g., `mkdir`) is handled: search for it in `Server.cc` (e.g., `Server::handle_client_mkdir()`)
- To understand the lock state transitions: read `locks.h` for the state tables
- To understand cap revocation: start at `Locker::issue_caps()` and `Locker::revoke_stale_caps()`
- To understand subtree migration: start at `Migrator::export_dir()` and `Migrator::import_dir()`
- To add a new journal event type: create a new class in `events/`, register in `LogEvent.h`

## Gotchas

- `MDCache.cc` is massive (~15K lines). When working on specific features, focus on the relevant methods rather than reading the whole file.
- `Locker.cc` is ~6K lines of cap/lock state transitions. The locking model is intricate and easy to break.
- `frag_t` (directory fragmentation) uses a bitwise prefix scheme. Understanding it is necessary for any work on directory operations.
- CInode/CDentry/CDir use intrusive reference counting. Be careful with ownership — don't hold raw pointers across async operations.
- Multi-MDS setups add significant complexity around subtree management and migration.
