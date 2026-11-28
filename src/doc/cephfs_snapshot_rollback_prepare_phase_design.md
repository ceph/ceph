# CephFS subvolume snapshot rollback: prepare phase (design)

Design for the **prepare** step of in-place subvolume snapshot rollback ([Feature #77215](https://tracker.ceph.com/issues/77215)). Rollback **execution** (copy-on-access, dentry splice, `CEPH_OSD_OP_ROLLBACK`, readdir under COA) is in [`cephfs_snapshot_rollback_design.md`](cephfs_snapshot_rollback_design.md). Orchestrator drain contract: [Tasks #82124](https://tracker.ceph.com/issues/82124).

**Status:** Draft for review.

---

## 0. Lifecycle placement

Prepare sits between orchestrator drain and rollback **start**:

| Phase | Owner | Outcome |
|-------|--------|---------|
| Drain | Orchestrator / operator | Workloads stopped; CSI unpublish/unstage; NFS exports removed where they overlap the subvolume |
| **Prepare (this doc)** | `mgr/volumes` + MDS | `ready: true`, `freeze_epoch`, `ttl`; journaled rollback-freeze on subvolume root |
| Start | `mgr/volumes` + MDS | COA marker (`rollback_to`); see main design §3.2 |
| Rollback (lazy) | MDS + RADOS | Metadata splice on access; `CEPH_OSD_OP_ROLLBACK` for file data |

Shared **scope and constraints** from the umbrella rollback design (abridged):

- **Subvolume rollback** to a named snapshot only (not arbitrary paths without subvolume semantics).
- **IO blocking:** From successful prepare until start consumes `freeze_epoch`, non-privileged clients must not acquire caps under the subtree. After start, COA rules in the main design govern client visibility until rollback completes.
- **Atomicity / failover:** Prepare state (`preparing`, `frozen`, epoch, ttl) is journaled on the subvolume root and must survive MDS failover. Partial prepare must not leave a false `frozen`.
- **Hardlinks:** Same constraint as the main design—hardlinks only within the subvolume subtree; prepare does not change that model.

---

## 1. Goals and non-goals

### Goals

- After external drain (workload scale-down, CSI unpublish/unstage, optional NetworkFence), put **one subvolume** into a state where **rollback may start safely**.
- Return a **binary** result: ready or not ready; if not ready, explain why (holders, blocking conditions).
- Work on **multi-rank** clusters using the same cross-rank cap-recall pattern as **subvolume quarantine**.
- Support **indirect access**: clients mounted at `/` or an ancestor path (mgr/volumes, NFS-Ganesha default `cmount_path`) that hold caps under the subvolume.
- Use **MDS auth caps** (rollback privilege) so the rollback operator is not evicted or blocklisted during prepare.

### Non-goals (this document)

- Orchestrator implementation (Rook, ceph-csi, NFS export lifecycle automation).
- Rollback execution (COA marker, lazy data rollback, start/status/cancel).
- Replacing long-lived `ceph fs quiesce` sets for this operation.

### Assumption (outside Ceph)

The operator has already stopped consumers of the subvolume (Kubernetes scale-down, NFS export removal where applicable, manual umount on bare metal). Prepare clears **stragglers** and **freezes** the subtree; it does not watch pods or kubelet.

---

## 2. Terminology

| Term | Meaning |
|------|---------|
| **Subvolume root** | Directory inode where `ceph.dir.subvolume` is set; mgr path `/volumes/<group>/<name>`. |
| **Subtree** | All dentries and inodes under the subvolume root. |
| **Freeze** | Journaled policy on the subvolume root: no new caps and no client data/metadata access under the subtree until released or consumed by start. |
| **`freeze_epoch`** | Per-subvolume monotonic id identifying one freeze generation. Not an OSD or MDS map epoch. |
| **`ttl`** | Seconds remaining until an **unused** freeze auto-expires (lease before rollback starts). |
| **Rollback privilege** | MDS auth cap **`b`** (path) or **`B`** (filesystem-wide), modeled on quarantine **`q`** / **`Q`**. |
| **Tell body** | JSON-like dict `mgr` passes to an MDS rank via `tell_command("mds", gid, cmd_dict)`. |

---

## 3. Readiness predicate

`ready: true` only if **all** of the following hold:

| ID | Condition |
|----|-----------|
| R1 | No **non-privileged** session has **non-PIN caps** on any inode in the subtree. |
| R2 | No **non-privileged** in-flight client request targets a path under the subvolume root. |
| R3 | **Freeze** is active on the subvolume root: journaled `freeze_epoch`, `frozen_at`, lease not expired. |
| R4 | **All MDS ranks** that hold authoritative dirfrags in the subtree have completed the cap-recall walk for this prepare operation. |
| R5 | No **blocking** subvolume state (e.g. active quarantine without a defined ordering, incompatible async clone); see §10. |

**Privileged session:** Cephx identity whose MDS auth caps include **`b`** or **`B`** covering this subvolume path (§6). Such sessions are **not** blocklisted and **not** evicted as a session; their **ordinary** caps under the subtree are still revoked during prepare. Privilege means the session may remain connected and may invoke rollback admin operations—not continued read/write IO to user data.

**Indirect access:** `client_metadata["root"]` may be `/` or `/volumes` while caps exist under `/volumes/.../subvol`. Readiness is **cap-based**, not “mount root under subvolume”.

---

## 4. Session handling

### 4.1 Why mount root is insufficient

| Client | Typical `root` metadata | Access pattern |
|--------|-------------------------|----------------|
| ceph-csi / kernel on PVC | Subvolume or below | Direct mount |
| mgr/volumes libcephfs pool | `/` | Path under `/volumes/...` |
| NFS-Ganesha (default) | `/` (`cmount_path`) | Export `path` under volumes |
| NFS-Ganesha (optional) | `/volumes` | Export under `path` |

Default Ganesha config (`cmount_path = "/"`, export `path` = subvolume path) is implemented in `mgr/nfs` (`ganesha_conf.py`, `export.py`). Those sessions are **ancestor mounts** but are **not** safe to ignore.

### 4.2 Actions by session type

| Type | Detection | Prepare action |
|------|-----------|----------------|
| **Workload** | Mount root at subvolume or below; no `b`/`B` | Revoke caps; wait; blocklist on `--force` if caps persist |
| **Ancestor, no privilege** | `root` above subvolume; no `b`/`B` (typical `nfs.*`, `client.admin` without `b`) | Revoke all caps under subtree; no blocklist unless narrow dedicated mount (see §4.3) |
| **Rollback driver** | `b`/`B` on subvolume path | Do **not** blocklist or evict session; revoke normal caps under subtree; allow rollback admin path |
| **Privileged but wrong path** | `B` or `b` on other path only | Treated as non-privileged for this subvolume |

### 4.3 Blocklist policy

Blocklisting a session with `cmount_path = /` (Ganesha client pool) can break **unrelated exports** sharing that CephFS client.

**Default:** blocklist only when:

- Mount root is the subvolume or below, **and**
- Caps remain after recall timeout, **and**
- `--force` is set (bare metal), **or**
- Orchestrator policy explicitly requests forced eviction (out of band).

**Never blocklist** sessions with rollback **`b`/`B`** on this path.

---

## 5. Rollback auth caps

Model on quarantine (`MDSAuthCaps`: **`q`**, **`Q`** in `src/mds/MDSAuthCaps.h`):

### 5.1 New capability bits

| Bit | Letter | Mon/`fs authorize` | Meaning |
|-----|--------|---------------------|---------|
| Per-path rollback | **`b`** | `b` in path grant | May run prepare/start/release for matching paths; not blocklisted during prepare |
| Global rollback | **`B`** | `B` | Same for any path in the filesystem grant |

**Invariants (mirror quarantine):**

- **`b`/`B` are not implied** by `allow … rw` or `all`.
- Parsing and checks live in `MDSAuthCaps` and `Session` request authorization.

**Example:**

```text
ceph fs authorize cephfs client.rollback-operator /volumes rwb
ceph auth caps client.rollback-operator mds 'allow rwb path=/volumes' mon 'allow r' osd 'allow rwx pool=cephfs-metadata, allow rwx pool=cephfs-data'
```

The active mgr should use an identity granted **`b`** (or `B`) for volumes paths used for rollback, **not** rely on unscoped `client.admin` without `b`.

### 5.2 Client feature (optional)

Quarantine uses `CEPHFS_FEATURE_QUARANTINE` for client-aware behavior. Rollback may define **`CEPHFS_FEATURE_ROLLBACK`** so clients understand freeze/rollback notifications; quarantine-style matrix applies for old clients (block vs `-EACCES` vs pin-only). Minimum for v1: server-side freeze mask without client feature; document behavior for kernel/fuse without feature.

### 5.3 Who gets `b`

| Entity | `b` |
|--------|-----|
| Dedicated mgr rollback cephx user | Yes |
| `nfs.*` Ganesha users | No (default) |
| Application / CSI cephx users | No |
| Human `client.admin` without `b` | No |

---

## 6. MDS implementation

### 6.1 Pattern: quarantine + internal `quiesce_path`

Prepare does **not** call `ceph fs quiesce` or create a QuiesceDb set. It uses the same **internal** entry point as subvolume quarantine: `MDCache::quiesce_path()` with a rollback-specific finisher flag (parallel to `quarantine=true` on `C_MDS_QuiescePath`).

Reference flow today (`start_quarantine_cap_revocation` → `quiesce_path`):

1. Set operational state on the subvolume root (quarantine journals its flag first; see phase order below for rollback).
2. `MDCache::quiesce_path(filepath(subvolume_root), finisher)`.
3. Auth rank starts `CEPH_MDS_OP_QUIESCE_PATH` → per-inode `CEPH_MDS_OP_QUIESCE_INODE` via `dispatch_quiesce_path` / `dispatch_quiesce_inode` on each rank (`MDCache.cc`).
4. In rollback mode, quarantine branch of `dispatch_quiesce_inode` is reused or mirrored: `eval` / `issue_caps` with **rollback-freeze** mask, subtree walk for dentries, `QuarantineTracker`-style completion.

After the walk succeeds, **release** the short-lived quiesce ops (same as quarantine: the walk ends; locks are not held for the whole `ttl`). Long-lived exclusion is **only** the journaled rollback-freeze on the subvolume root (§6.2).

#### 6.1.1 Prepare: two explicit phases

| Phase | What runs | Durable state after phase |
|-------|-----------|---------------------------|
| **1 — Cap recall** | `ROLLBACK_PREPARE_AUTH` on auth rank calls internal **`quiesce_path`** (rollback flag); all ranks run the quiesce subtree walk; optional blocklist for stubborn workload sessions (`--force`). | `rollback_freeze_state = preparing` may be journaled when prepare starts (so failover can resume or abort). **No `freeze_epoch` exposed yet.** |
| **2 — Freeze commit** | Only if phase 1 meets R1–R4 (§3): auth rank **journals** `rollback_freeze_epoch++`, `frozen_at`, `ttl`, `state = frozen`, enables rollback-freeze cap mask on root. | `ready: true`, `freeze_epoch`, `ttl` returned to mgr. |

**Ordering rule:** Phase 2 runs **only after** phase 1 completes successfully on all ranks. If the walk fails or caps remain, return `ready: false` and **do not** advance epoch or set `frozen` (or revert a tentative `preparing` journal).

**Why not QuiesceDb for phase 1?** A single-root internal `quiesce_path` already coordinates multi-rank cap recall (same as quarantine). QuiesceDb adds leader routing, set IDs, and holding `QUIESCED`—none of which are needed for a one-shot recall before a journaled freeze.

### 6.2 New rollback-freeze metadata (subvolume root)

Journaled fields (exact encoding TBD):

| Field | Purpose |
|-------|---------|
| `rollback_freeze_epoch` | `u64`, incremented on each new freeze |
| `rollback_freeze_state` | `none` \| `preparing` \| `frozen` |
| `rollback_freeze_at` | Time freeze became `frozen` (for `ttl`) |
| `rollback_freeze_ttl_sec` | Lease duration configured at prepare |

Replay after MDS failover restores state; expired lease cleared before serving subtree.

### 6.3 Internal operations

| Op | Rank | Role |
|----|------|------|
| `ROLLBACK_PREPARE_AUTH` | Auth of subvolume root | Journal state, orchestrate walk, aggregate acks, reply |
| `ROLLBACK_PREPARE_WORK` / quiesce variant | Each rank | Cap recall under subtree, apply rollback-freeze cap mask |
| `ROLLBACK_PREPARE_RELEASE` | Auth | Clear freeze if unused |

**Rollback-freeze cap mask:** Like quarantine mask on `CInode::get_caps_*`—deny issue of read/write/file caps under frozen root; privileged sessions may still be authorized for admin rollback operations on path.

### 6.4 Multi-rank completion

Use a tracker analogous to `QuarantineTracker` (`src/mds/QuarantineManager.h`):

- Auth rank seals work, waits for per-rank acks.
- Failure on any rank → `ready: false`, no `frozen` (or rollback to previous state).
- Mgr receives single JSON reply from auth rank via tell completion.

### 6.5 Admin command (asok / mon tell)

Registered on MDS (names illustrative):

```text
rollback prepare path=<path> ttl=<sec> force=<bool> snap=<name> ...
rollback prepare release path=<path> freeze_epoch=<n>
```

Handler returns JSON in reply buffer (same pattern as `command_quarantine_dir` in `MDSRankQuarantine.cc`).

---

## 7. mgr/volumes interface

### 7.1 CLI

```text
ceph fs subvolume snapshot rollback prepare <vol_name> <subvol_name>
    [--group_name <group>]
    [--ttl <seconds>]
    [--force]
    [--release]
    [--snap <snap_name>]
```

| Argument | Default | Meaning |
|----------|---------|---------|
| `vol_name` | — | Filesystem / volume name |
| `subvol_name` | — | Target subvolume |
| `--group_name` | `_nogroup` | Subvolume group |
| `--ttl` | configurable | Seconds for unused freeze lease |
| `--force` | off | Allow blocklist of stubborn workload sessions |
| `--release` | — | Drop freeze for `freeze_epoch` (see reply); no prepare |
| `--snap` | — | Optional: verify snapshot exists before freeze |

### 7.2 No libcephfs walk of subvolume

Follow `quarantine_subvolume` (`src/pybind/mgr/volumes/fs/volume.py`):

- Build path from `volspec` only.
- `tell_command("mds", gid, cmd_dict, one_shot=True)` to active rank.
- Do **not** `open_subvol()` for prepare (avoids self-blocking when subtree is frozen).

### 7.3 Tell body

`cmd_dict` passed to `tell_command` is the **tell body**. Example:

```json
{
  "prefix": "rollback prepare",
  "path": "/volumes/csi/pvc-4f2a",
  "ttl": 300,
  "force": false,
  "snap_name": "snap1",
  "initiator_entity": "client.rollback-operator",
  "initiator_addrs": ["v2:10.0.0.5:6800/12345"]
}
```

| Field | Source | Purpose |
|-------|--------|---------|
| `path` | volumes layout | Subvolume root |
| `ttl`, `force`, `snap_name` | CLI | See §7.1 |
| `initiator_*` | mgr (optional) | Never blocklist the entity driving this prepare |

Optional future fields from `mgr/nfs`: `overlapping_exports[]` for clearer `blocking` reasons when Ganesha caps remain.

### 7.4 Optional client metadata

Recommended on libcephfs connect (`mgr_util.py`):

```text
client_metadata=ceph_daemon=mgr/volumes
```

For **identification and logging** only; **authorization** remains **`b`/`B`** caps.

Ganesha FSAL (out-of-tree): `ceph_daemon=nfs-ganesha` when upstream accepts a patch—optional, not required for prepare logic.

---

## 8. Responses

### 8.1 Ready

```json
{
  "ready": true,
  "freeze_epoch": 7,
  "ttl": 300
}
```

- **`ttl`:** seconds **remaining** until auto-release if rollback does not start (refreshed on idempotent prepare while still frozen and R1–R4 hold).
- **`freeze_epoch`:** must be passed to **start** (separate subtask).

### 8.2 Not ready

```json
{
  "ready": false,
  "holders": [
    {
      "entity": "client.4567",
      "addr": "10.0.0.12:0/1846231501",
      "hostname": "node-a",
      "mount_root": "/volumes/csi/pvc-4f2a",
      "privileged": false,
      "blocklist_eligible": true,
      "reason": "caps not released"
    }
  ],
  "blocking": []
}
```

`blocking` examples: `quarantine`, `clone_in_progress`, `rank_unavailable`, `nfs_export_active` (if overlap hint supplied).

No `freeze_epoch` or `ttl` on failure—the subvolume is not left frozen.

### 8.3 Idempotency

| Situation | Behavior |
|-----------|----------|
| Already `frozen`, still clear | `ready: true`, same `freeze_epoch`, refreshed `ttl` |
| Prepare while `preparing` | Wait or `-EINPROGRESS` |
| Rollback running | Reject prepare (not `ready: false`) |
| Expired lease | Auto-unfreeze; next prepare may allocate new epoch |

---

## 9. State machine (subvolume)

```text
UNFROZEN
  │ prepare (success)
  ▼
FROZEN (ttl running)
  │ ttl expires          ──► UNFROZEN
  │ --release            ──► UNFROZEN
  │ start(freeze_epoch)  ──► ROLLBACK_RUNNING (ttl pinned / ignored)
  │ prepare (refresh)    ──► FROZEN (same epoch, new ttl)

ROLLBACK_RUNNING
  │ complete             ──► UNFROZEN (or INCOMPLETE — start subtask)
  │ fail before marker     ──► UNFROZEN
```

**Start** (not specified here) must reject stale `freeze_epoch` after expiry or release.

---

## 10. Conflicts and ordering

| Condition | Prepare behavior |
|-----------|------------------|
| Subvolume quarantined | `ready: false`, `blocking: ["quarantine"]` unless product defines quarantine-then-rollback order |
| Clone in progress to/from subvolume | `blocking: ["clone_in_progress"]` |
| Another rollback / prepare on same subvolume | Serialize or `EINPROGRESS` |

**NFS:** Operator should remove or disable exports whose `path` is under the subvolume before prepare. Until caps are gone, Ganesha sessions appear in `holders` with `mount_root: "/"`, `blocklist_eligible: false`.

---

## 11. Orchestrator reference workflow (contract only)

1. Scale down workload using the volume.
2. Wait for `NodeUnpublishVolume` and `NodeUnstageVolume` (and disable NFS exports overlapping the subvolume).
3. Loop: `ceph fs subvolume snapshot rollback prepare …` until `ready: true`.
4. On `holders`, fence stuck nodes (NetworkFence) or fix manual mounts; retry prepare—not start.
5. Call **start** with `freeze_epoch` before `ttl` expires.

Bare metal: same prepare; use `--force` instead of NetworkFence when appropriate.

---

## 12. Handoff to rollback start

Prepare guarantees:

- Subtree frozen at `freeze_epoch`.
- No non-privileged caps under subtree.
- Privileged rollback driver may still run tell/start.

**Start** ([`cephfs_snapshot_rollback_design.md`](cephfs_snapshot_rollback_design.md) §3.2) must:

- Verify `freeze_epoch` matches journaled state and lease is valid.
- Apply COA marker and rollback machinery in one atomic step with freeze pin.
- Not depend on orchestrator drain alone—prepare already enforced at cap level.

After start, user-facing IO may resume under COA; cap recall during prepare is not the same as holding the tree fully quiesced for the entire rollback duration.

---

## 13. Testing (outline)

| Case | Expectation |
|------|-------------|
| Kernel fuse on subvolume path | `ready: false` until umount; optional `--force` blocklist |
| mgr tell prepare without opening subvolume | Success |
| libcephfs on `/` with caps under subvolume, no `b` | Caps revoked; `ready: true` when clear |
| Session with `b` on path | Not blocklisted; caps under subtree still cleared |
| Multi-rank subvolume | All ranks ack before `ready: true` |
| `ttl` expiry | Unfrozen; stale start rejected |
| MDS failover during `preparing` | Resume or fail safe per journaled state |
| Quarantine enabled | Documented `blocking` |
| Simulated Ganesha: `nfs.*`, `root=/` | Holder until caps gone; no blocklist |

---

## 14. Implementation checklist

### MDS / client

- [ ] `MDSAuthCaps`: parse and check `b`/`B`
- [ ] Subvolume root journal: freeze epoch and state
- [ ] Rollback-freeze cap mask on inodes
- [ ] Prepare auth/work ops: internal `MDCache::quiesce_path()` (extend quarantine / `C_MDS_QuiescePath` rollback flag)
- [ ] Multi-rank ack + tracker
- [ ] `rollback prepare` / `release` tell handlers
- [ ] Optional `CEPHFS_FEATURE_ROLLBACK`

### mgr/volumes

- [ ] CLI `fs subvolume snapshot rollback prepare`
- [ ] Tell to MDS; no `open_subvol` for prepare
- [ ] Cephfs entity with `b` for volumes paths
- [ ] Optional `client_metadata=ceph_daemon=mgr/volumes`

### docs / ops

- [ ] `fs authorize` examples for rollback operator
- [ ] Update [Tasks #82124](https://tracker.ceph.com/issues/82124) prepare subtask
- [ ] Cross-link [`cephfs_snapshot_rollback_design.md`](cephfs_snapshot_rollback_design.md) (same tree; PR #69327 upstream)

### Future

- [ ] `mgr/nfs` overlap hints in tell body
- [ ] CSI-Addons mapping for prepare/start/status

---

## 15. References

- [Feature #77215](https://tracker.ceph.com/issues/77215) — subvolume snapshot rollback
- [Tasks #82124](https://tracker.ceph.com/issues/82124) — orchestrator co-design
- [Tasks #77216](https://tracker.ceph.com/issues/77216) / [PR #69327](https://github.com/ceph/ceph/pull/69327) — umbrella rollback design
- [`cephfs_snapshot_rollback_design.md`](cephfs_snapshot_rollback_design.md) — COA, dentry cases, readdir options
- `doc/dev/mds_internals/quiesce.rst` — quiesce mechanism
- `doc/cephfs/fs-volumes.rst` — subvolume quarantine and `rwq` caps
- `src/pybind/mgr/nfs/ganesha_conf.py` — default `cmount_path=/`
- `src/mds/MDSRankQuarantine.cc`, `src/mds/QuarantineManager.h` — quarantine tell + tracker pattern