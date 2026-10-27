# Design Document: CephFS Snapshot Rollback Feature

## 1. Introduction

This document describes the design for an in-place, subvolume snapshot rollback feature for CephFS. Snapshot rollback enables a cluster operator to revert a CephFS subvolume to the exact state captured at a previously taken snapshot — including both file data and filesystem metadata.

Rollback is an operator-driven operation. During rollback, all client IO is suspended on the target directory subtree (subvolume) for the duration of the operation. When rollback completes, IO resumes with the filesystem reflecting the snapshot state.

---

## 2. Scope & Constraints

### Scope
* **Subvolume rollback** to a named snapshot.
* **Rollback of file data** in the CephFS data pool (RADOS objects).
* **Rollback of filesystem metadata** in the CephFS metadata pool (inodes, dentries).
* **Handling of edge cases** including files modified, created, deleted, or renamed after the snapshot was captured.
* **Concurrent rollback execution** across multiple independent CephFS subvolumes.

### Constraints
* **IO Blocking:** CephFS client capabilities (caps) are revoked on the target subtree before rollback begins. No client data or metadata IO is permitted until the rollback operation fully completes.
* **Atomicity:** The rollback must leave the filesystem in either the fully rolled-back state or a clean, recoverable intermediate state. Partial states must be systematically detectable and resumable following an MDS failover.
* **Hardlinks:** The MDS enforces that hardlinks may only exist within the same directory subtree (under a subvolumes).

---

## 3. Design & Operator Workflow

The rollback lifecycle is initiated by an explicit operator workflow. An operator determines that a subvolume needs to be reverted to a specific snapshot and triggers the operation via the CLI. This command request is routed directly to the Ceph Metadata Server (MDS).

```
[ Operator CLI Request ] ---> [ Ceph MDS ]
                                   |
                         (Revoke Client Caps)
                                   |
               +-------------------+-------------------+
               |                                       |
     [ Responsive Clients ]                 [ Unresponsive Clients ]
     (Flush writes & invalidate)            (Timeout -> Forcefully Evicted)
               |                                       |
               +-------------------+-------------------+
                                   |
                      (All Caps Revoked/Cleared)
                                   |
                    [ Apply COA & Rollback Marker ]
```

### Step 1: Cap Revocation and Distributed Synchronization
The primary objective of the MDS is to guarantee that no conflicting IO operations from any active clients interleave with the rollback process. This isolation is achieved via strict capability management:
1. **Revoking Caps:** The MDS revokes all outstanding caps from all clients with active sessions on the target subtree. This forces clients to flush buffered writes, clear cached reads, and write back pending metadata updates (e.g., file size, `mtime`, `ctime`).
2. **Cap Locking:** The MDS refrains from granting any new caps to clients on this subtree until it is structurally safe to do so, freezing client-side operations.
3. **Fencing Unresponsive Clients:** Clients that fail to respond to cap revocation requests within the configured timeout window are forcefully evicted from the cluster and appended to the OSD blocklist. This ensures that a buggy or unresponsive client cannot indefinitely stall cluster administration and/or initiate IO in midst of a rollback operation.

Once acknowledgments are received from all remaining viable clients, the MDS proceeds with the structural namespace transformation.

### Step 2: Applying Copy-on-Access (COA) Semantics
The MDS marks the root inode of the target subvolume with an internal operational marker. This marker indicates that the subtree has transitioned to **Copy-on-Access (COA)** semantics relative to the designated target `snap-id`.

By establishing this baseline marker at the root, the MDS possesses the minimal necessary metadata context to evaluate, slice, and roll back files or nested subdirectories lazily on-demand upon their very first access, optimizing up-front execution times.

---

## 4. Conditions to Initiate Rollback Evaluation

The core criteria to evaluate a snapshot rollback for a given dentry occurs when its `first` field mismatches the `rollback_to` marker during a namespace traversal. This logic must robustly handle scenarios where a directory is rolled back multiple times and some intermediate dentries or inodes have already been processed in prior runs (e.g., a file rolled back to `snap-id 4` previously, now being evaluated for a directory rollback to `snap-id 5`).

### The Problem with Naive Implementations

1. **The `first != rollback_to` Approach:**
   The fundamental issue with a naive inequality check is that once a dentry or inode is rolled back, its `first` field is updated to the next snapshot sequence number. Consequently, on any subsequent validation loop or evaluation, this condition will always evaluate to `true`, causing redundant or broken execution paths.

2. **The `first < rollback_to` Approach:**
   While modifying the condition to a strict less-than check (`first < rollback_to`) avoids the immediate pitfall mentioned above, it introduces a regression for subsequent rollbacks. Because a previous rollback pushes the `first` field forward to the next snapshot sequence number, the condition `first < rollback_to` will prematurely evaluate to `false` when trying to roll back to a higher intermediate snapshot.

### Solution
To resolve these tracking anomalies, the dentry tracks its historical rollback origin metadata internally. A rollback operation is triggered when the dentry's `first` field is strictly less than the `rollback_to` target marker, leveraging explicit generation splices where appropriate.

---

## 5. Core Rollback Mechanism

Rolling back a dentry or an inode requires a dual-layered synchronization approach:
1. Re-aligning and fixing the dentry's snapshot metadata boundary fields: `[first, last]`.
2. Executing concurrent, asynchronous rollback operations on the underlying distributed data blocks by issuing `CEPH_OSD_OP_ROLLBACK` operations directly to RADOS.

Let's understand the mechanism using various cases.

### CASE I: File exists in head and is unchanged acrosssSnapshots

Consider a file that exists in the current `head` revision and is a part of three historic snapshots with snapshot IDs `[3, 4, 5]`. This file has not undergone any mutations or modifications in any of these snapshots. In standard CephFS architecture, this implies a single inode and a single dentry are shared globally across `head` and all three snapshots.

#### Initial Architecture State (Pre-Rollback)
* **OMAP Key:** `file_head`
* **Dentry Range Tracking:** `[2, head]` $\rightarrow$ mapped to `dentry(first, last)`
* **Inode Range Tracking:** `inode [2, head]` $\rightarrow$ mapped to `inode(first, last)`
* **Metadata Context Flags:** `{ snaps: [3, 4, 5] }`, `{ rollback_to: 4 }`

When the directory is marked to be rolled back to `snap-id = 4`, the Metadata Server (MDS) cannot simply overwrite existing fields because it needs to perform a structural **splice** in the dentry/inode progression timeline.

A new snapshot ID is allocated (e.g., `snap-id = 6`), which serves as the new `first` boundary field for the head dentry. The dentry's `first` field cannot naively be set to the targeted snapshot ID (`4`), as doing so would violate fundamental invariant assumptions within the MDS.

> **Architectural Invariant Warning:**
> If the head dentry's `first` field were naively set to `4`, the resulting structural state would look like:
> * `file_head : [4, head]`
> * `file_4 : [3, 4]`
>
> This is invalid because it implies that two distinct, divergent versions of the same dentry exist concurrently within the same snapshot boundary.

#### Post-Rollback State

To maintain consistency, the next sequence number (`6`) becomes the `first` field for the head dentry. The updated OMAP key-value states are laid out as follows:

| OMAP Key | Dentry Boundary (`[first, last]`) | Inode Boundary (`[first, last]`) | Metadata Mutation Notes |
| :--- | :--- | :--- | :--- |
| **`file_head`** | `[6, head]` | `inode [6, head]` | Bitwise copy from target snapshot metadata. |
| **`file_5`** | `[3, 5]` | `inode [3, 5]` | Forked via `copy-from: 3` or `[2, head]`. |

**Data Pool Resolution:** The underlying data objects are reverted by transmitting `CEPH_OSD_OP_ROLLBACK` to the RADOS data pool for the file data objects, effectively forcing the block state at `snap-id = 4` to become the active head object.

---

### CASE II: File modified across multiple distinct snapshots

In this scenario, a file has undergone multiple updates, resulting in physical structural fragmentation across snapshot epochs.

#### Pre-Rollback State
* **OMAP Keys & Layout:**
  * `file_head` : `[b, head]` $\rightarrow$ `inode [b, head]`
  * `file_a` : `[8, a]` $\rightarrow$ `inode [8, a]`
  * `file_7` : `[7, 7]` $\rightarrow$ `inode [7, 7]`
* **Active Snapshot Vector:** `[7, 8, 9, a, b]`
* **Rollback Target Context:** `rollback_to : 9`

#### Mechanism
1. The next available global snapshot sequence identifier `c` is claimed to act as the head dentry's `first` boundary field.
2. The head inode structure is deep-copied from the historical state frozen at snapshot `9`.
3. Because the pre-rollback head layout (`file_head`) was shared with snapshot `b`, the inode/dentry undergoes a strict structural **split** on its `first` field. This forks a dedicated, standalone dentry/inode pair to preserve snapshot `b`.
4. The remaining historical pairs (`file_a`, `file_7`) remain completely untouched.

#### Final Layout (Post-Rollback)
* `file_head` : `[c, head]` $\rightarrow$ `inode [c, head]` *(Deep copied from snapshot 9)*
* `file_b` : `[b, b]` $\rightarrow$ `inode [b, b]` *(Preserved via structural split from old head block)*
* `file_a` : `[8, a]` $\rightarrow$ *Unchanged*
* `file_7` : `[7, 7]` $\rightarrow$ *Unchanged*

**Data Action:** An explicit `CEPH_OSD_OP_ROLLBACK` chain is dispatched to RADOS for object mappings matching `9 -> c`.

---

### CASE III: File deleted and re-created in head

This edge case addresses the scenario where a file is completely unlinked/deleted and a new file with the exact same dentry name is re-created in `head`, making the head inode entirely distinct from its snapshot ancestors.

#### Pre-Rollback State
* `file_head` : `[9, head]` $\rightarrow$ `inode [9, head]` *(inode# 0x3)*
* `file_8` : `[7, 8]` $\rightarrow$ `inode [7, 8]` *(inode# 0x2)*
* **Active Snapshot Vector:** `[7, 8, 9]`
* **Rollback Target Context:** `rollback_to : 8`

#### Mechanism
Upon initiating the rollback, the newly created instance in head (`ino: 0x3`) must be unlinked from the active namespace, surviving only inside the historical snapshot `9` context. The active `head` must safely roll back to tracking `inode# 0x2`.

#### Final Layout (Post-Rollback)
* `file_head` : `[a, head]` $\rightarrow$ `inode [a, head]` *(tracks inode# 0x2, copied from snapshot 8)*
* `file_8` : `[7, 8]` $\rightarrow$ *Unchanged* *(tracks inode# 0x2)*
* `file_9` : `[9, 9]` $\rightarrow$ `inode [9, 9]` *(tracks inode# 0x3, preserved via split from the old head)*

**Data Action:** The existing head data objects for `inode# 0x3` are safely routed to the purge queue. Concurrently, an `OP_ROLLBACK` is issued to the data pool for `inode# 0x2` to transition state boundaries from `8 -> a`.

---

## 6. Inode purging and hardlink handling

### Purging the head inode
When a head inode must be evicted or purged during a snapshot rollback operation, the subsystem utilizes the CephFS **stray manager** machinery. Because this engine operates with native snapshot awareness at its core, the Copy-on-Write (COW) underlying block mechanics are enforced transparently at the RADOS layer. This guarantees that while the active head references are dismantled, all historical data blocks tied to active snapshots remain safe from corruption or accidental deletion.

### CASE IV: Hardlinked inodes
This scenario details rolling back a file from a snapshot where the active head inode contains multiple links (hardlinks).

CephFS strictly segregates link semantics into two paradigms:
1. **Primary Link:** Point-to-point map directly linking the dentry to the full, concrete inode metadata structure.
2. **Secondary Links (Remote Links):** Lightweight dentries pointing back to the primary Inode ID number.

The Metadata Server resolves the absolute canonical path for a primary link by executing a lookup against the backtrace metadata embedded within the zero'th (`0th`) data object in the RADOS data pool. Secondary links independently mirror their own lifecycle boundaries using standard `first` and `last` tracking fields.

Rolling back either a primary or a secondary link independently follows identical structural splitting logic to the single-link cases outlined previously. However, structural constraints require that the dentry must reside inside the system `stray` directory until the absolute last link referencing the underlying inode (whether primary or secondary) drops to a link count of `0` due to successive rollbacks, at which point it is safely reaped by the MDS purge queue.

### Link Rollback Decision Matrix

The following state space defines the coordination required between primary and secondary link states during rollback scenario for primary or secondary links:

| Primary State \ Secondary State | Not Rolled Back | Rolled Back |
| :--- | :--- | :--- |
| **Not Rolled Back** | **(a)** | **(b)** |
| **Rolled Back** | **(c)** | **(d)** |

(a) When neither of the links are rolled back, accessing the primary link triggers copy-on-access as described in the previous sections. The inode is relinked to the stray directory with reduced link count. Normally, in CephFS, a stray dentry can be reintegrated to a secondary link, thereby converting the secondary link to primary. This step will be a deversion in the rollback case -- the "rolled back" stray dentry will remain in stray until the inode's link count drops to 0, implying that all other links would need to undergo a rollback.

(b) The primary dentry hasn't yet undergone a rollback. Rolling back the secondary dentry triggers a rollback of the primary dentry and relinking the inode in stray with reduced link count.

(c) When the primary is already rolled back, the stray dentry already exists (if it still had links). Apart from rolling back the secondary link, the stray inode link count is reduced.

(d) This case is similar to (c) but there are more than one secondary links, and some of the links have already rolled back.

## 7. Rolling back directories

A directory can be in many different states relative to the snapshot being rolled back to. This section classifies these states by comparing, for a given dentry name `D` in a parent directory, what `D` refers to in the snapshot `S` (the `rollback_to` snapshot) versus what it refers to in `head`.

### Directory snapshot representation

Directory inodes differ from regular files in how their snapshotted versions are stored, which affects every case below:

* **Directory inodes are multiversion.** When a directory's metadata is modified across a snapshot boundary, the MDS does not fork a new dentry/inode pair as it does for regular files (Cases I-III). Instead, the older version of the inode is preserved in the inode's own `old_inodes` map, and the inode's `first` field is advanced. The primary dentry continues to span `[first, head]`.
* **Unlinked or renamed directories leave a remote dentry behind.** When a directory's primary dentry is moved away (unlinked to stray, or renamed) and the old location is covered by a snapshot, the old location keeps a *remote* dentry `[first, last]` that refers to the directory inode by inode number. The inode itself stays linked at its new primary location (the stray directory, or the rename destination), and still carries its `old_inodes` versions.

Throughout this section, the active snapshot vector is `[3, 4, 5]`, the rollback target is `rollback_to : 4`, and `6` is the next snapshot sequence number allocated for the rollback (as in Case I).

### 7.1 Existence and identity: which inode the name `D` refers to

#### CASE A1: Directory exists in snapshot and head, same inode

This is the common case: the directory was neither deleted nor replaced after `S`, although its metadata and/or contents may have changed.

* **Pre-Rollback:** `D_head : [2, head]` $\rightarrow$ `inode# X [2, head]`, with `X.old_inodes` holding any versions that were modified across snapshot boundaries.
* **Mechanism:**
  1. The current head version of `X` is copied into `X.old_inodes` so that snapshot `5` keeps seeing it.
  2. The head metadata of `X` is replaced with the version of `X` visible in snapshot `4` (from `X.old_inodes`, or from head if `X` was not modified since `4`).
  3. `X.first` is set to `6`. The dentry `D_head` remains `[2, head]` since the inode is multiversion and no dentry fork is required.
  4. The rollback recurses into the children of `X`, each of which is evaluated independently (as a file case, or as one of the directory cases in this section).
* **Post-Rollback:** `D_head : [2, head]` $\rightarrow$ `inode# X [6, head]` *(metadata as of snapshot 4)*.

#### CASE A2: Directory does not exist in snapshot, exists in head

The directory was created after `S`. Rolling back requires removing it, along with everything under it, from `head`.

* **Pre-Rollback:** `D_head : [5, head]` $\rightarrow$ `inode# Y [5, head]` *(created after snapshot 4, before snapshot 5)*.
* **Mechanism:**
  1. The contents of `Y` are removed from head recursively. Each child is unlinked from head (a child directory is itself in state A2 relative to `S`).
  2. `Y` is unlinked from head. Since snapshot `5` covers `Y`, `Y` is relinked into the stray directory and a remote dentry `D : [5, 5]` is left in the parent so that snapshot `5` still sees it.
  3. If no snapshot covers `Y` (i.e., `Y` was created after the most recent snapshot), there is nothing to preserve and the entire subtree is purged.
* **Post-Rollback:**
  * `D : [5, 5]` $\rightarrow$ remote link to `inode# Y` *(visible only in snapshot 5)*
  * `~mdsN/strayK/Y` $\rightarrow$ `inode# Y` *(in stray until snapshot 5 is removed)*
  * No head dentry for `D`.

The MDS will need to add support to be able to recursively remove a directory tree by itself. There is redmine tracker (https://tracker.ceph.com/issues/44455) already for this targetting an improvement in mgr/volumes plugin. This feature becomes a prerequisite for snapshot rollback. The tracker has undergone discussion which are a bit dated and would need to be revisited.

#### CASE A3: Directory exists in snapshot, deleted in head

The directory was removed (`rmdir`) after `S` and not re-created. Rolling back requires reinstating it, along with its entire subtree, into `head`.

* **Pre-Rollback:**
  * `D : [2, 5]` $\rightarrow$ remote link to `inode# X` *(left behind by `rmdir` after snapshot 5)*
  * `~mdsN/strayK/X` $\rightarrow$ `inode# X`, with `X.old_inodes` holding its snapshotted versions
* **Mechanism:**
  1. `X` is relinked from the stray directory back into the parent as a primary dentry `D_head : [6, head]`. The stray dentry is removed.
  2. The head metadata of `X` is replaced with the version visible in snapshot `4`, and `X.first` is set to `6`.
  3. Since `rmdir` only succeeds on an empty directory, every child of `X` that existed in snapshot `4` is now a snapshot-only dentry. Each of these is reinstated into head recursively (a child directory is itself in state A3, and a child file is reinstated from its snapshot dentry).
* **Post-Rollback:**
  * `D_head : [6, head]` $\rightarrow$ `inode# X [6, head]` *(metadata as of snapshot 4)*
  * `D : [2, 5]` $\rightarrow$ remote link to `inode# X` *(unchanged, serves snapshots 3-5)*

While `X` is in the process of being reinstated, the stray manager must not evaluate it for purging or reintegration. Snapshot `S` must also be protected from removal for the duration of the rollback, since it is the only thing keeping `X` alive in stray.

#### CASE A4: Directory exists in snapshot and head, different inodes

The directory was deleted after `S` and a new directory with the same name was created in head. This is the directory equivalent of CASE III.

* **Pre-Rollback:**
  * `D_head : [5, head]` $\rightarrow$ `inode# Y [5, head]` *(new directory)*
  * `D : [2, 4]` $\rightarrow$ remote link to `inode# X` *(original directory, deleted after snapshot 4)*
  * `~mdsN/strayK/X` $\rightarrow$ `inode# X`
* **Mechanism:** This is CASE A2 applied to `Y` followed by CASE A3 applied to `X`. The order matters: `Y` must be removed from head first to free the name before `X` can be reinstated under it. If the MDS fails over between the two steps, the dentry is left with no head linkage, which is exactly the pre-rollback state of CASE A3 and is resumed as such.
* **Post-Rollback:**
  * `D_head : [6, head]` $\rightarrow$ `inode# X [6, head]` *(metadata as of snapshot 4)*
  * `D : [5, 5]` $\rightarrow$ remote link to `inode# Y`, with `Y` in stray
  * `D : [2, 4]` $\rightarrow$ remote link to `inode# X` *(unchanged)*

#### CASE A5: Name changed type between snapshot and head

The name `D` refers to a regular file (or symlink) in the snapshot and a directory in head, or the other way round. The directory cases above therefore cannot assume that the snapshot and head versions of `D` are of the same type.

* **File in snapshot, directory in head:** CASE A2 is applied to the head directory, followed by reinstating the file from its snapshot dentry.
* **Directory in snapshot, file in head:** The head file is unlinked (as in the head half of CASE III, with its data routed to the purge queue if no snapshot covers it), followed by CASE A3 for the directory.

As with CASE A4, the head entry is always removed before the snapshot entry is reinstated, so an interrupted rollback always resumes from a well-defined state.

### Summary

| Case | In snapshot `S` | In head | Rollback action | Recursion into children |
| :--- | :--- | :--- | :--- | :--- |
| **A1** | directory, `inode# X` | directory, `inode# X` | Restore `X` metadata from snapshot `S`; preserve the current head version in `X.old_inodes`; bump `X.first`. | Every child is evaluated independently. |
| **A2** | absent | directory, `inode# Y` | Remove `Y` from head. Relink `Y` to stray if a newer snapshot covers it; otherwise purge. | Every head child is removed (recursive removal, tracker 44455). |
| **A3** | directory, `inode# X` | absent | Relink `X` from stray into head; restore metadata from snapshot `S`. | Every snapshot child is reinstated. |
| **A4** | directory, `inode# X` | directory, `inode# Y` (`X != Y`) | A2 on `Y`, then A3 on `X`. | Head children of `Y` removed; snapshot children of `X` reinstated. |
| **A5** | file/symlink (or directory) | directory (or file/symlink) | Remove the head entry, then reinstate the snapshot entry. | Only for the directory side. |

## 8. Readdir and capability handling under Copy-on-Access

With Copy-on-Access (COA), client IO resumes once the `rollback_to` marker is set on the subvolume root, and dentries are rolled back lazily on first access. `readdir` needs special attention under this model because it touches every dentry in a directory fragment at once.

Normally, the MDS responds to a `readdir` request with the inode metadata of each dentry and issues caps on those inodes (along with dentry leases) to the client. Under COA, issuing caps for an inode that has not yet been rolled back would expose head state that is no longer valid, so the MDS must decide, for each dentry in the reply, whether to roll it back first or to withhold caps.

The set of names returned by `readdir` must itself reflect the snapshot being rolled back to: names that exist only in the snapshot (CASE A3) must appear, and names that exist only in head (CASE A2) must not. Therefore, the MDS must determine the snapshot view of every dentry in the dirfrag to build a correct `readdir` reply, regardless of how caps are handled.

### Option 1: Roll back the entire dirfrag before replying

On the first `readdir` (or lookup) of a dirfrag that has not yet been rolled back, the MDS rolls back the metadata of every dentry in that dirfrag, journals it, and then serves the `readdir` normally, including caps.

* Since determining the snapshot view of each dentry is required anyway, performing the metadata splice at the same time adds limited extra cost.
* The expensive part, rolling back file data in RADOS via `CEPH_OSD_OP_ROLLBACK`, is decoupled from the metadata splice. The MDS issues metadata caps (`p`, `As`, `Ls`, `Xs`) immediately but withholds file caps (`Fr`, `Fc`, `Fw`, `Fb`) on an inode until its data rollback has completed. Clients that attempt to read or write such a file block on cap acquisition until then.
* The dirfrag becomes the unit of COA, so the rollback state can be tracked per dirfrag rather than per dentry.

### Option 2: Reply to readdir without issuing caps

The MDS builds the `readdir` reply from the snapshot view of each dentry without modifying head, and does not issue caps or dentry leases on the returned inodes. A subsequent `getattr`/`lookup`/`open` on an individual entry triggers the rollback of that dentry.

* Avoids rolling back every dentry in a dirfrag for a single client request.
* Clients must issue a `getattr` on each entry to obtain caps, so `ls -l` style workloads turn into one MDS round trip per entry.
* Without `Fs` on the directory, clients will not consider the directory complete and will not serve subsequent lookups from their cache.
* Requires verification that existing (legacy) clients tolerate `readdir` replies without caps on head inodes. There is precedent: `readdir` of a `.snap` directory already returns inodes without caps. Both the kernel client and ceph-fuse need to be tested.

### Option 3: Block readdir until the dirfrag has been rolled back in the background

The `readdir` request waits until the background rollback reaches that dirfrag.

* Simplest to implement.
* The latency of the first `readdir` is unbounded and depends on the progress of the background rollback.

### Recommendation

Option 1, with file caps withheld per inode until its data rollback completes. Since a correct `readdir` reply requires the snapshot view of every dentry in the dirfrag anyway, rolling back the metadata at that point is the natural place to do it, and it keeps client-visible behaviour (caps, leases, directory completeness) unchanged for both legacy and newer clients.
