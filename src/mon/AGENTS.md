# src/mon/ — Monitor Daemon

## Purpose

The Monitor daemon maintains cluster consensus and manages all cluster state maps. A Ceph cluster typically runs 3-5 monitors that use the **Paxos** consensus algorithm to agree on cluster state. Each type of cluster state (OSDMap, MonMap, MDSMap, AuthMap, etc.) is managed by a dedicated **PaxosService** subclass.

The monitor handles leader election, processes state change proposals (e.g., OSD up/down, pool creation, auth key management), and distributes updated maps to all cluster participants.

## Key Files

| File | Role |
|------|------|
| `Monitor.h/cc` | Main daemon class. Holds PaxosService instances, handles election, message dispatch, admin commands. |
| `Paxos.h/cc` | Core Paxos consensus algorithm implementation. |
| `PaxosService.h/cc` | Base class for all monitor services. Defines the `preprocess_query()` / `prepare_update()` lifecycle. |
| `Elector.h/cc` | Monitor leader election protocol. |
| `ElectionLogic.h/cc` | Election algorithm logic (separated from networking). |
| `OSDMonitor.h/cc` | Manages OSDMap — processes OSD state changes, pool creation/deletion, CRUSH rule changes. Largest PaxosService. |
| `MDSMonitor.h/cc` | Manages MDSMap/FSMap — MDS rank assignment, filesystem status. |
| `AuthMonitor.h/cc` | Manages authentication keys and capabilities (CephX). |
| `MgrMonitor.h/cc` | Manages MgrMap — active/standby manager status. |
| `ConfigMonitor.h/cc` | Centralized configuration store — distributes config to daemons. |
| `HealthMonitor.h/cc` | Aggregates health checks from all services. |
| `LogMonitor.h/cc` | Cluster log management. |
| `KVMonitor.h/cc` | Generic KV metadata store. |
| `MonMap.h/cc` | Monitor cluster membership map. |
| `PGMap.h/cc` | Aggregated PG statistics (used for `ceph status` output). |
| `MonCommands.h` | CLI command definitions — macro-based DSL defining all `ceph` CLI commands handled by the monitor. |
| `MonClient.h/cc` | Client-side monitor connection (used by all daemons to subscribe to maps). |
| `Session.h/cc` | Monitor session tracking. |
| `MonOpRequest.h/cc` | Monitor operation request wrapper. |

## Patterns and Idioms

### PaxosService Lifecycle
All PaxosService subclasses follow the same pattern:

1. **`preprocess_query()`** — read-only check. Can answer immediately without Paxos (e.g., returning current state). Return `true` if handled, `false` if it needs Paxos.
2. **`prepare_update()`** — propose a state change through Paxos. Stages the change in a pending buffer.
3. Paxos commits → `update_from_paxos()` is called on all monitors to apply the committed state.

When adding a new monitor command or state change, follow this pattern exactly.

### MonCommands DSL
CLI commands are defined in `MonCommands.h` using a macro-based DSL:
```cpp
COMMAND("osd pool create "
        "name=pool,type=CephString "
        "name=pg_num,type=CephInt,range=0",
        "create pool",
        "osd", "rw")
```
Each entry defines: command syntax, help text, module, and permissions.

### Map Versioning
Every cluster map has a monotonically increasing `epoch_t` version. The monitor stores all versions (for clients that need to catch up). Map trimming removes old versions based on configurable retention.

## Dependencies

Uses `common/`, `msg/`, `auth/`, `kv/` (RocksDB), `include/`. References map types from `osd/OSDMap.h` and `mds/FSMap.h`.

## Navigation Hints

- To add a new `ceph` CLI command: add the command definition to `MonCommands.h`, then implement handling in the appropriate PaxosService's `preprocess_query()` or `prepare_update()`
- To understand how OSD state changes propagate: follow `OSDMonitor::prepare_update()` → `OSDMonitor::encode_pending()` → Paxos commit → `OSDMonitor::update_from_paxos()`
- To understand election: `Elector.cc` handles the messaging, `ElectionLogic.cc` has the algorithm
- To understand how daemons subscribe to maps: see `MonClient.h/cc`

## Gotchas

- `MonCommands.h` uses a macro-based DSL that can be confusing. The command string format is positional — parameter names and types must match exactly.
- The monitor stores ALL versions of cluster maps. This means unbounded disk growth if trimming is misconfigured.
- `OSDMonitor` is by far the largest PaxosService. It handles pools, CRUSH rules, OSD state, and device classes — all in one service.
- The monitor's `MonitorDBStore` is a different KV store from the OSD's `KeyValueDB`. They both wrap RocksDB but have different schemas.
- `PGMap` is populated from OSD reports and aggregated by the monitor. It is NOT a Paxos-managed map — it's updated out-of-band.
