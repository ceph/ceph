# src/mon/ — Monitor Daemon

Monitors use **Paxos** consensus. Each cluster state type (OSDMap, MDSMap, AuthMap, etc.) is managed by a dedicated **PaxosService** subclass.

## PaxosService lifecycle — follow this for all new commands/state changes

1. **`preprocess_query()`** — read-only check, can answer without Paxos. Return `true` if handled.
2. **`prepare_update()`** — propose a change through Paxos, stages into a pending buffer.
3. Paxos commits → `update_from_paxos()` applies committed state on all monitors.

## MonCommands DSL

CLI commands are defined in `MonCommands.h` via a macro-based DSL. The command string format is positional — parameter names and types must match exactly.

## Gotchas

- `PGMap` is populated from OSD reports and aggregated by the monitor. It is **NOT** a Paxos-managed map — updated out-of-band.
- `OSDMonitor` is by far the largest PaxosService — handles pools, CRUSH rules, OSD state, and device classes all in one.
- The monitor stores ALL versions of cluster maps. Unbounded disk growth if trimming is misconfigured.
