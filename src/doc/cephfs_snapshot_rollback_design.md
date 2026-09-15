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

## 7. Rolling back nested directories in head

The MDS will need to add support to be able to recursively remove a directory tree by itself. There is redmine trakcer (https://tracker.ceph.com/issues/44455) already for this targetting an improvement in mgr/volumes plugin. This feature becomes a prerequisite for snapshot rollback. The tracker has undergone discussion which are a bit dated and would need to be revisited.