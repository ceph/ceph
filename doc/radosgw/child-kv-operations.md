# Child KV Operations — Annotations, Tags, Extended Value

## Semantics

Child KV entries (`:C:` namespace) store metadata that belongs to a parent object (`:O:`) but doesn't fit inline in the parent's value, or represents independently-addressable sub-resources (like S3 annotations).

Child entries are:
- **Owned by a specific version** of the parent object, identified by `parent_ref_tag`.
- **Co-located** on the same shard as the parent (same `bucket_id + object_name` → same `shard_id`).
- **Deleted when the parent is deleted or overwritten** — GC workers clean children as part of parent cleanup.

Three child types exist (in the `C:` category within the `S` namespace):

| Type byte | Name | Content | Storage-tier data? |
|---|---|---|---|
| `A` | Annotation | Application-defined key-value sub-resource | Optional (blob via `anno_ref_tag`) |
| `T` | Tags | S3 object tags (array of up to 10 key-value pairs) | Never |
| `E` | Extended Value | Metadata overflow (manifest, compression info) | Never |


---

## Key Structure

```
C:<parent_ref_tag><type><id>
```

Full key layout (from Listing-and-Key-Scheme.md):
```
[header 14B] [ref_tag 12B] [child_type 1B] [child_id 0–512B]
```

| Child type | child_id | Example |
|---|---|---|
| `A` (annotation) | annotation key name (1–512B) | `C:<ref_tag>A<"user-metadata-1">` |
| `T` (tags) | empty (single entry per object) | `C:<ref_tag>T` |
| `E` (extended value) | empty (single entry per object) | `C:<ref_tag>E` |

---

## Parent Dependency

A child entry cannot exist without its parent. Every operation that creates or modifies a child entry must verify the parent exists and hasn't been replaced.

**For current version (targeting O:):**

1. **`get(O:)` → verify non-null.**
    - If null, the object was deleted — abort.
2. **Verify `O:.ref_tag == expected_parent_ref_tag`.**
    - If mismatch, the object was overwritten with a new version — the old children belong to the old version (now in `:V:` or `G:`).
    - Abort.

**For old version (targeting V:<vid> — S3 `versionId` parameter):**

1. **`get(V:<vid>)` → check result:**
   - If exists → use V:<vid> as parent. Read `ref_tag` from value.
   - If null → version may have been promoted to current (DELETE Case 2). Fallback:
2. **Fallback: `get(O:)` → check if `O:.version_id == vid`:**
   - If match → version was promoted to O:. Use O: as parent.
   - If no match → version truly deleted. Return `NoSuchVersion`.

This fallback handles the case where DELETE Case 2 promoted V:<vid> to O:. The version still exists — just in a different location. Clients using versionId APIs don't need to know whether their target is current or non-current.

```
// Version resolution (applies to GET, PUT child ops, DELETE child ops)
get(V:<vid>)
if exists:
  parent = V:<vid>
else:
  get(O:) → if O:.version_id == vid:
    parent = O:
  else:
    → abort (NoSuchVersion)
```

In both cases, the parent entry (O: or V:<vid>) is read AND written (count fields updated) in the same transaction — providing write-write conflict detection on TiKV with concurrent DELETE operations.

**Why `get(O:)` / `get(V:<vid>)` implies bucket exists:**

- DeleteBucket verifies both `:O:` and `:V:` are empty before deleting `B`.
- If any `:O:` or `:V:` entry exists → DeleteBucket returns `BucketNotEmpty`.
- Therefore, a successful `get(O:)` or `get(V:<vid>)` guarantees `B` exists.
- No separate `get(B)` is needed for child operations.

**FDB (strong, transactional guarantee):**

- DeleteBucket's RangeScans create read conflict ranges.
- If O: or V: exists in your transaction, DeleteBucket cannot commit concurrently.
- Absolute guarantee.

**TiKV (soft, probabilistic guarantee):**

- DeleteBucket's RangeScans do NOT create conflict ranges.
- In the extremely unlikely pend-delete residual risk scenario, O:/V: can exist for a bucket that was already deleted.
- If violated: child operations produce orphaned entries — cleaned by periodic scrubber.
- See [bucket_delete.md — Residual risk](bucket_delete.md#residual-risk).


---

## When P: Coordination is Needed

**Rule:** A `P:` coordination entry is needed only when an operation writes to the storage tier between transaction phases (the two-domain problem).

| Operation | Storage-tier write? | P: needed? |
|---|---|---|
| PutObjectAnnotation (with blob data) | Yes (annotation blob) | Yes (`P:A`) |
| PutObjectAnnotation (inline value only) | No | No — single txn |
| DeleteObjectAnnotation | No (GC frees data asynchronously) | No — single txn |
| PutObjectTagging | No (pure KV) | No — single txn |
| DeleteObjectTagging | No (pure KV) | No — single txn |
| Extended Value write | No (pure KV overflow) | No — written in PUT Phase 3 txn |

When extended value data lives on the storage tier, it is referenced directly from the `:O:` value (blob pointer in the manifest). No child KV entry is created — it is covered by the parent's `P:O` coordination entry during PUT.

---

## Operations

### PutObjectAnnotation — With Storage-Tier Data (overwrite)

Annotations with blob data require the three-phase protocol:

**Phase 1 — Record intent:**
```
txn {
  1. Generate anno_ref_tag
  2. get(O:) → if null → abort
     Read annotation_count → if annotation_count >= max → abort (limit exceeded)
     Record parent_ref_tag = O:.ref_tag
  3. put(P:A, {anno_ref_tag, parent_ref_tag, object_name, annotation_key, timestamp})
  commit
}
```

**Phase 2 — Write annotation data (storage tier, no KV):**

Write annotation blob addressed by `anno_ref_tag`.

**Phase 3 — Commit annotation:**

**Normal path (commit):**
```
txn {
  1. get(P:A) → if null → abort (sweeper already took it — nothing to clean)
  2. get(O:) → if null or ref_tag mismatch → self-cleanup abort (see below)
  3. get(C:<parent_ref_tag>A<annotation_key>) → check if overwrite
     If overwrite with storage data → put(G:A, {old_anno_ref_tag, old_chunk_pointers})
  4. put(C:<parent_ref_tag>A<annotation_key>, {anno_ref_tag, size, content_type, ...})
  5. delete(P:A)
  6. put(O:, annotation_count + 1)  // new annotation
     — or put(O:, same_count)        // overwrite — count unchanged
  commit
}
```

**Self-cleanup abort** (parent deleted or overwritten):
```
txn {
  1. get(P:A) → exists
  2. get(O:) → null or ref_tag mismatch → invalid state
  3. put(G:A, {anno_ref_tag})  ← queue annotation blob for GC
  4. delete(P:A)
  commit  ← cleanup-abort, return error to client
}
```

Live processes self-clean on abort: move P:A → G:A immediately. Sweeper only handles entries from crashed processes.

Step 6 always writes O: regardless of new or overwrite — creates **write-write** conflict with concurrent parent DELETE/PUT on both FDB and TiKV.

**Crash recovery:**
- Crash before Phase 1 commit: nothing written. Clean.
- Crash after Phase 1, before Phase 3: `P:A` exists. Sweeper moves to `G:A`, GC frees annotation blob.
- Crash after Phase 3 commit: annotation is live. `P:A` gone. Clean.

### PutObjectAnnotation — Inline Value (no storage-tier data)

Small annotations without blob data are a single transaction:

```
txn {
  1. get(O:) → if null → abort
     Read annotation_count → if annotation_count >= max → abort (limit exceeded)
     Record parent_ref_tag = O:.ref_tag
  2. get(C:<parent_ref_tag>A<annotation_key>) → check if overwrite
     If overwrite with storage data → put(G:A, {old_anno_ref_tag, old_chunk_pointers})
  3. put(C:<parent_ref_tag>A<annotation_key>, {inline_value, size, content_type, ...})
  4. put(O:, annotation_count + 1)  // new annotation
     — or put(O:, same_count)        // overwrite — count unchanged
  commit
}
```

No P:A needed — no storage-tier write, no two-domain problem. Always writes O: for conflict detection and count maintenance.

### DeleteObjectAnnotation

Always a single transaction (deletion is a KV operation; old annotation data is GC'd asynchronously):

```
txn {
  1. get(O:) → if null → abort
     Record parent_ref_tag = O:.ref_tag
  2. get(C:<parent_ref_tag>A<annotation_key>) → if null → success (idempotent)
  3. If old annotation has storage data:
     put(G:A, {old_anno_ref_tag, old_chunk_pointers})
  4. delete(C:<parent_ref_tag>A<annotation_key>)
  5. put(O:, annotation_count - 1)
  commit
}
```

Always writes the parent entry (O: or V:<vid>) — decrements count, creates write-write conflict on both systems.

### PutObjectAnnotation — Versioned (with versionId, storage-tier data)

Full flow for annotating a specific version. Both Phase 1 and Phase 3 use version resolution (fallback to O: if V:<vid> was promoted).

**Phase 1 — Record intent:**
```
txn {
  1. Generate anno_ref_tag
  2. Resolve parent:
     get(V:<vid>) → if exists: parent = V:<vid>
     if null: get(O:) → if O:.version_id == vid: parent = O:
     else → abort (NoSuchVersion)
  3. Read annotation_count from parent → if >= max → abort (limit exceeded)
     Record parent_ref_tag = parent.ref_tag
  4. put(P:A, {anno_ref_tag, parent_ref_tag, object_name, annotation_key, vid, timestamp})
  commit
}
```

**Phase 2 — Write annotation data (storage tier, no KV):**

Write annotation blob addressed by `anno_ref_tag`.

**Phase 3 — Commit annotation:**

**Normal path (commit):**
```
txn {
  1. get(P:A) → if null → abort (sweeper already took it)
  2. Resolve parent (same logic as Phase 1):
     get(V:<vid>) → if exists: parent = V:<vid>
     if null: get(O:) → if O:.version_id == vid: parent = O:
     else → self-cleanup abort (version deleted between phases)
  3. Verify parent.ref_tag == P:A.parent_ref_tag → if mismatch → self-cleanup abort
  4. get(C:<parent_ref_tag>A<annotation_key>) → if overwrite with storage data:
     put(G:A, {old_anno_ref_tag, old_chunk_pointers})
  5. put(C:<parent_ref_tag>A<annotation_key>, {anno_ref_tag, size, content_type, ...})
  6. put(parent, annotation_count + 1)  // or same count if overwrite
  7. delete(P:A)
  commit
}
```

**Why both phases need version resolution:**
- **Phase 1:** If promotion happened before the client's request — find the version in O: rather than failing.
- **Phase 3:** If promotion happened between Phase 1 and Phase 3 — the version moved from V: to O: during the data upload. ref_tag is preserved during promotion, so the match in step 3 succeeds regardless of location.

**Self-cleanup abort** (version deleted or ref_tag mismatch):
```
txn {
  1. get(P:A) → exists
  2. Resolve parent → not found or ref_tag mismatch
  3. put(G:A, {anno_ref_tag})  ← queue annotation blob for GC
  4. delete(P:A)
  commit  ← cleanup-abort, return error to client
}
```

Live processes self-clean on abort. Sweeper only handles entries from crashed processes.

**Conflict safety:** `put(parent, ...)` writes to the resolved entry (O: or V:<vid>). This provides write-write conflict with:
- DELETE Case 1 targeting V:<vid> (if parent is V:)
- DELETE Case 2 promoting V:<vid> (if parent is V: — concurrent promotion conflicts)
- Any concurrent PUT overwriting O: (if parent is O:)

**Cost:** Happy path (V:<vid> exists): same as before — no extra reads. Promotion path: one extra `get(O:)` per phase (rare).

---

### PutObjectTagging (full replacement — AWS API)

Tags are stored as an array of up to 10 key-value pairs. S3 `PutObjectTagging` replaces the entire tag set. The storage model (inline in O: value vs external C:T entry) is determined by whether the new tag set exceeds the inline size budget.

```
txn {
  1. get(O:) → if null → abort; read ref_tag, current tag state (inline/external/none)
     Compute new_tag_count = len(new_tags)  // uint8, max 10

  Case A — Current: inline or none, New: fits inline
    2. put(O:, value_with_new_inline_tags + tag_count = new_tag_count)

  Case B — Current: inline or none, New: exceeds threshold
    2. put(O:, clear_inline_tags + set_external_flag + tag_count = new_tag_count)
    3. put(C:<ref_tag>T, new_tag_array)

  Case C — Current: external, New: exceeds threshold
    2. put(C:<ref_tag>T, new_tag_array)
    3. put(O:, tag_count = new_tag_count)

  Case D — Current: external, New: fits inline
    2. delete(C:<ref_tag>T)
    3. put(O:, clear_external_flag + set_inline_tags + tag_count = new_tag_count)

  commit
}
```

`tag_count` is a single byte (uint8, max value 10 per S3 limit). All cases write the parent entry (O: for current version, V:<vid> for old versions) with the updated count — provides write-write conflict on both FDB and TiKV.

**Old-version variant:** When targeting a non-current version (S3 `versionId` parameter), use the version resolution logic from [Parent Dependency](#parent-dependency) to locate the parent (V:<vid> or O: if promoted). Replace `get(O:)` with the resolved parent and `put(O:, ...)` with `put(parent, ...)` in all cases above.

### DeleteObjectTagging (remove all — AWS API)

S3 `DeleteObjectTagging` removes all tags regardless of storage model:

```
txn {
  1. get(O:) → if null → abort; read ref_tag, current tag state
  2. If external: delete(C:<ref_tag>T)
  3. put(O:, clear_all_tag_fields + tag_count = 0)
  commit
}
```

Always writes O: (clears tag fields) → write-write conflict on both FDB and TiKV.

### Extended Value Operations

Extended value holds metadata overflow — internal data that logically belongs to the object but exceeds the O: value size (e.g., large manifests, compression dictionaries, encryption metadata for multipart objects). Extended value is NOT user-addressable — clients never read/write it directly. It is accessed internally as part of GET/PUT operations.

**Three-level storage (similar to annotations, but no standalone KV when on data tier):**

| Metadata size | Storage | Pointer in O: | C:E entry? |
|---|---|---|---|
| Fits in O: value | Inline in O: | N/A (data is there) | No |
| Overflows O: but < KV value limit | C:<ref_tag>E (single entry) | `{ev_location: CHILD_E}` | Yes |
| Exceeds KV value limit (extreme multipart) | Data tier blob | `{ev_location: STORAGE, storage_id, blob_id, offset, length}` | No |

Unlike annotations, extended value on the storage tier does NOT need a C:E entry — O: holds the storage pointer directly. Extended value is not user-addressable, so there's nothing for a client to look up by name.

**Write (during PUT Phase 3):**

Extended values are written atomically in the same transaction as the parent O: entry:

```
txn {
  ... (PUT Phase 3 steps) ...
  get(P:O) → if null → abort
  put(O:, value_with_ev_location_flag)
  put(C:<ref_tag>E, overflow_metadata)  // only if ev_location = CHILD_E
  delete(P:O)
  commit
}
```

For storage-tier extended value: written in PUT Phase 2 alongside object data (same blob or adjacent blob, same P:O coordination). O: stores the pointer directly — no C:E entry created.

No separate P: coordination needed for C:E — piggybacks on the parent's P:O.

**Read (on first access):**

When GET encounters extended value:
- `CHILD_E`: follow-up read `get(C:<ref_tag>E)`. Single KV read, typically cached after first access.
- `STORAGE`: fetch from data tier (same as reading object data — one storage-tier read).

**Lifecycle:** Owned entirely by the parent. When the parent moves to G: (deletion/overwrite):
- `CHILD_E`: GC deletes C:E via `RangeScan(C:<ref_tag>*)`.
- `STORAGE`: GC frees the storage blob (same mechanism as freeing object data — may be the same blob).

---

## Data Storage

Object data is stored at one of three tiers (inline in O:, D: namespace entry, or storage tier) based on size.\
The data tiering model, D: namespace key schema, PUT/GET transactions per tier, GC cleanup, background migration, and capacity considerations are documented in [data-tiering.md](data-tiering.md).

Child KV entries (C:A, C:T, C:E) are independent of the data tier — they coexist with any tier and are cleaned uniformly by GC via `RangeScan(C:<ref_tag>*)`.

---

## TiKV: Conflict Detection via Functional O: Writes

### Why child operations are safe on TiKV

TiKV optimistic transactions detect only write-write conflicts. A child operation that reads O: but doesn't write it cannot detect a concurrent DELETE/PUT of the parent. However, all child operations in this document write O: for **functional** reasons:

- **Annotations:** write `annotation_count` to O: (increment on add, decrement on delete, same value on overwrite)
- **Tags:** write `tag_count` to O: (updated on every PutObjectTagging/DeleteObjectTagging)
- **Extended Value:** written in PUT Phase 3 alongside O: (no separate operation)

These functional writes create write-write conflicts with concurrent parent DELETE/PUT on TiKV — no additional conflict-detection field is needed.

### Operations and their O: write reason

| Operation | O: field written | TiKV conflict via |
|---|---|---|
| PutObjectAnnotation (new) | `annotation_count + 1` | Write-write on O: |
| PutObjectAnnotation (overwrite) | `annotation_count` (same) | Write-write on O: |
| DeleteObjectAnnotation | `annotation_count - 1` | Write-write on O: |
| PutObjectTagging (all cases) | `tag_count` | Write-write on O: |
| DeleteObjectTagging | `tag_count = 0` | Write-write on O: |
| Extended Value write | Part of PUT Phase 3 | Write-write on O: (PUT writes O:) |

All child operations use identical transactions on FDB and TiKV. No backend-specific code paths.

### On FDB

FDB does not need the O: write for conflict detection — the `get(O:)` read creates a conflict range that detects concurrent writes. However, the functional writes (counts) are still performed on FDB because they serve a data purpose (limit enforcement, HeadObject metadata). Same code, both systems.

### Where conflict detection comes from

All child operations write their parent entry (O: for current versions, V:<vid> for old versions) for functional AWS reasons — `tag_count`, `annotation_count`, or inline tag data. These writes provide write-write conflict detection on TiKV as a free side effect. No additional conflict-detection field is needed for any child operation.

For DELETE with version-id Case 1: Case 1 writes (deletes) V:<X>. Any child operation targeting V:<X> also writes V:<X> (count fields). Write-write conflict on V:<X> provides mutual exclusion. See [transaction-safety.md](transaction-safety.md).

---

## Child Cleanup on Parent Deletion

When a parent object is deleted or overwritten, the parent entry moves to `G:` (GC namespace). The G: entry retains the `ref_tag`. Background GC workers are responsible for cleaning all children:

**GC worker processing G:O (or G:V) entry:**
```
1. RangeScan(C:<ref_tag>*) → find all child entries
2. For each C:A with storage-tier data:
   - put(G:A, {anno_ref_tag, chunk_pointers}) — queue annotation blob for deletion
   - delete(C:A entry)
3. For C:T, C:E entries (pure KV):
   - delete directly (no storage-tier cleanup needed)
4. Free parent object's storage-tier data (by ref_tag)
5. delete(G:O entry)
```

**Properties:**
- Children are cleaned AFTER the parent is moved to G: — no race with active readers (parent is already invisible to S3 API once O: is gone).
- Annotation blobs are cleaned via G:A entries (idempotent, safe to retry).
- Tags and extended values are pure KV deletes — no external cleanup.
- If GC crashes mid-cleanup: G:O still exists (step 5 not reached), GC retries. Idempotent deletes ensure no double-free.
- **Pagination:** An object can have up to 1000 annotations. The `RangeScan(C:<ref_tag>*)` may return a large result set. GC must handle paginated scans — processing and deleting entries in batches while keeping G:O alive until all children are cleaned. Design TBD.

---

## Read-Path Operations

Read operations on child KVs are pure snapshot reads — no writes, no conflict detection needed. Snapshot isolation provides a consistent point-in-time view on both FDB and TiKV.

### GetObjectTagging

Single transaction — both reads in the same snapshot:

```
txn {
  get(O:) → if null → 404; read ref_tag
  get(C:<ref_tag>T) → read tag array (null if inline or no tags)
  Return tags (from C:T if external, or from O: value if inline)
}
```

**Analysis:** Safe on both FDB and TiKV. If a concurrent DELETE removes O: after our snapshot, we still return the tags as of our snapshot — valid under S3 read consistency. If DELETE + GC committed before our snapshot, `get(O:)` → null → 404.

### GetObjectAnnotation (inline value — no storage-tier data)

Single transaction:

```
txn {
  get(O:) → if null → 404; read parent_ref_tag
  get(C:<parent_ref_tag>A<annotation_key>) → if null → 404
  Return annotation value
}
```

**Analysis:** Same as GetObjectTagging — single snapshot, safe on both systems.

### GetObjectAnnotation (with storage-tier data)

Multi-phase read — KV metadata and storage-tier data cannot be read atomically:

```
Phase 1 (KV snapshot):
txn {
  get(O:) → if null → 404; read parent_ref_tag
  get(C:<parent_ref_tag>A<annotation_key>) → if null → 404
  Record anno_ref_tag and data-tier pointer from C:A value
}

Phase 2 (storage tier):
  Read annotation blob using anno_ref_tag
  → if blob not found (GC freed it) → retry from Phase 1

Phase 3 (verification):
txn {
  get(O:) → verify parent_ref_tag matches Phase 1
  get(C:<parent_ref_tag>A<annotation_key>) → verify anno_ref_tag matches Phase 1
  → if match → return data to client
  → if mismatch → retry from Phase 1 (annotation was overwritten)
}
```

**Analysis:**

- **Phase 1:** consistent KV snapshot. Safe.
- **Phase 2:** reads blob outside KV transaction. Can fail if GC freed the blob (annotation was overwritten, G:A processed). On failure → retry from Phase 1.
- **Phase 3:** re-verifies annotation is still current. If ref_tags match → data is valid, return. If mismatch → annotation was overwritten between phases → retry.
- **All phases are read-only** — no writes, no conflict detection needed.
- **Safe on both FDB and TiKV.** Purely snapshot reads + application-level verification.
- **No TiKV special handling needed for read paths.**

### GetObjectExtendedValue

Single transaction (extended value is always in KV, never on storage tier):

```
txn {
  get(O:) → if null → 404; read ref_tag, verify has_extended_value flag
  get(C:<ref_tag>E) → read overflow metadata
  Return extended value
}
```

**Analysis:** Same pattern as GetObjectTagging — single snapshot, safe on both systems. Typically cached after first access (extended value doesn't change without a full PUT overwrite).

---

## Sweeper and GC Interaction with Child KV — Safety Overview

### What triggers background cleanup?

| Entry | Created by | Cleaned by | Interacts with children? |
|---|---|---|---|
| `P:O` | PUT Phase 1 | Sweeper → G:O → GC | No (Phase 3 never committed → no children exist) |
| `P:A` | PutObjectAnnotation Phase 1 | Sweeper → G:A → GC | No (Phase 3 never committed → no C:A created) |
| `P:M` | CompleteMultipartUpload Phase 1 | Sweeper (replay completion or rollback) | No (separate domain — :M: parts, not :C: children). See [S3-Operations — Upload protection model](S3-Operations-Over-KV.md#upload-protection-model). |
| `G:O` | DELETE / PUT overwrite | GC worker | Yes — scans and deletes all C:<ref_tag>* entries |
| `G:A` | Annotation overwrite/delete | GC worker | Yes — frees annotation blob on storage tier |

### Sweeper interaction — minimal

The sweeper prevents children from being created — it does NOT clean existing ones:
- Moves P:O → G:O (or P:A → G:A) atomically in a single transaction
- After commit: Phase 3's `get(P:X)` → null → abort → no C: entry written
- The write-write conflict on `delete(P:X)` ensures mutual exclusion between sweeper and Phase 3

### GC interaction — safe by design

GC runs only after O: is removed (G:O committed). Child operations cannot race with GC because:

1. **Child writers:** Every child write does `get(O:)` → null → abort. On TiKV, the functional write to O: (`annotation_count`, `tag_count`, or same value on overwrite) creates write-write conflict with the DELETE that removed O: → transaction fails → retry → O: null → abort. No new children can be created after O: is removed.

2. **Child readers:** Use transactional reads (single snapshot). If O: exists in the snapshot, C: entries are intact at that snapshot (GC hasn't committed yet at that point in time). Consistent view guaranteed.

3. **Atomicity of sweeper move:** The sweeper's `{get(P:X), put(G:X), delete(P:X)}` is a single atomic transaction. This prevents the race where GC frees data (via G:X) while Phase 3 is still able to commit (P:X still exists). After commit: P:X is gone AND G:X exists simultaneously — Phase 3 sees null, GC can safely proceed.

### FDB vs TiKV — no difference for background cleanup

Both systems handle sweeper/GC safely:
- Sweeper's write-write conflict on `delete(P:X)` works on both (both detect concurrent Phase 3 deleting the same key)
- GC processes G: entries after O: is removed — child operations detect O: removal via `get(O:)` (FDB: read conflict range, TiKV: functional O: write creates write-write conflict)
- Read-path operations are pure snapshot reads — no difference between FDB and TiKV
