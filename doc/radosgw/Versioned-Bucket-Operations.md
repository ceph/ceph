# Versioned Bucket Operations

## Introduction

This document describes how S3 versioning operations map to the KV schema defined in [KV-Based-Design-For-RGW.md](KV-Based-Design-For-RGW.md) and [Listing-and-Key-Scheme.md](Listing-and-Key-Scheme.md).

---
## Version ID Scheme

- First version: `version_id = max_uint32` (0xFFFFFFFF). `next_vid = max_uint32 - 1`.
- Each subsequent version (PUT or DELETE): `version_id = O:.next_vid`. Then `O:.next_vid = vid - 1`.
- The `:O:` value stores both fields:
  - `version_id` — this version's ID. What GET returns, what goes into the V: key when moved to history.
  - `next_vid` — the next available ID. Monotonically decreasing counter, consumed on each new version. Never resets upward.
- On promotion (DELETE Case 2): `next_vid` is inherited from the OUTGOING O: entry, not from the promoted V: entry. This prevents version ID reuse after deletion — deleted IDs are permanently consumed.
- In `:V:`, entries include the version_id in the **Key**\
keys sort by `<object_name><version_id>`.\
Since version_id decreases over time and is stored big-endian, later versions have smaller byte values and sort first — latest version first.

## KV Schema

&lt;namespace&gt; &lt;shard_count&gt; &lt;shard_id&gt; &lt;bucket_id&gt;**&lt;category&gt;**&lt;object_name&gt;

Four **categories** separate current objects:
- Current S3 **O**bjects
- S3 old **V**ersions
- RGW internal **C**hild entries
- **M**ultipart uploads.

Category tags are 1-byte ASCII values (`O`, `V`, `C`, `M`).\
For visual clarity they are written as `:O:`, `:V:`, `:C:`, `:M:`

**Current object (`:O:`):**

```
<namespace> <shard_count> <shard_id> <bucket_id> O <object_name>
```

**Old versions and delete markers (`:V:`):**

```
<namespace> <shard_count> <shard_id> <bucket_id> V <object_name> <version_id>
```

**Child entries (`:C:`):**

```
<namespace> <shard_count> <shard_id> <bucket_id> C <ref_tag> <child_type> <child_id>
```

Child type values: `A` (annotation), `T` (tags), `E` (extended value).

**Multipart uploads (`:M:`):**

```
<namespace> <shard_count> <shard_id> <bucket_id> M <object_name> <ref_tag> <part_number>
```

---

### `:O:` — Current Version

```
<namespace> <shard_count> <shard_id> <bucket_id> O <object_name>
```


One entry per object key.\
The **value** contains the current version's metadata:
- version_id — the current version's ID (stored in the value, not the key).
- ref_tag — unique write instance identifier (12 B).
- All S3 attributes (etag, last_modified, size, etc.).
- Chunk pointers for data access.
- Fenced flag — set when the entry is a delete marker; GetObject returns 404.

In a versioned bucket, `:O:` always has exactly one entry per object_name — either a live value or a fenced delete marker.\
ListObjectsV2 scans `:O:` and skips fenced entries.

---

### `:V:` — Old Versions and Delete Markers

```
<namespace> <shard_count> <shard_id> <bucket_id> V <object_name> <version_id 4B>
```

### The **value** contains:

- All S3 attributes for that version.
- ref_tag, chunk pointers (for live versions).
- Delete marker flag (for delete markers — no data, no ref_tag).

All versions of the same object share the same shard_id (version_id is excluded from the hash). All operations are shard-local transactions.

## Shard Locality

`shard_id = hash(bucket_id + object_name) % shard_count`.

version_id is excluded from the hash.\
All entries for the same object_name — `:O:` and `:V:` entries — land on the same shard.\
Moving a version from `:O:` to `:V:` (or back) is always a local transaction.

---

## Listing

### ListObjectsV2

Scans `:O:` only. Returns current objects in lexicographic order by object_name.

```
Range scan: <namespace><shard_count><shard_id><bucket_id>O ...
```

For each entry:
- If live → include in results (key, last_modified, etag, size, storage_class, owner, checksum_algorithm).
- If fenced (delete marker) → skip. The object appears deleted.

No interaction with `:V:`. No merge. No version awareness. One range scan per shard (or a single scan without sharding).

Prefix/delimiter filtering applies to object_name as usual.

### ListObjectVersions

Returns all versions of all objects — live values, delete markers, and fenced delete markers — sorted by object_name, then latest version first.

Scans both `:O:` and `:V:`, merges by object_name.

```
Range scan on :O: → <namespace><shard_count><shard_id><bucket_id>O ...
Range scan on :V: → <namespace><shard_count><shard_id><bucket_id>V ...
```

**Merge protocol:**

For each object_name encountered in either scan:

1. The `:O:` entry (if present) is the **current version** — it sorts first for that object_name.
2. `:V:` entries follow in version_id order (latest first by key construction).

The merge advances through both scans in parallel by object_name. When the same object_name appears in both `:O:` and `:V:`, the `:O:` entry is emitted first, then all `:V:` entries for that object_name, then the merge moves to the next object_name.

Each entry is returned with:
- key, version_id, is_latest (true only for the `:O:` entry).
- last_modified, etag, size, storage_class, owner.
- Type indicator: `Version` or `DeleteMarker`.

Prefix/delimiter filtering applies to object_name.

With sharding: fan out to all shards, merge-sort across shards by object_name.

---

## The Uniform Rule

All operations that write a new value to `:O:` on a versioned bucket — including PUT, DELETE-without-version-id (creates delete marker), and CompleteMultipartUpload (writes assembled manifest):

1. Move the current `:O:` entry to `:V:` (keyed by its version_id).
2. Write a new entry to `:O:` with `version_id = O:.next_vid`. Set `O:.next_vid = vid - 1`.
3. All in a single transaction.

PUT writes a live value. DELETE writes a **fenced delete marker**.

A delete marker in `:O:` is a lightweight value — a fenced flag, the delete marker's version_id, a timestamp, and the owner. GetObject reads `:O:`, sees the fenced flag, returns 404 with `x-amz-delete-marker: true` and the delete marker's version_id.

A delete marker is just a value. When displaced by the next PUT or DELETE, it moves from `:O:` to `:V:` like any other value.

---

## Operations

### PUT (versioned bucket)

PUT follows the standard 3-phase protocol. See [S3 Operations — PUT](S3-Operations-Over-KV.md#put) for the full flow and RADOS comparison.

1. Generate ref_tag.
2. Write `P:O` coordination entry with object_name, ref_tag, and estimated size.
3. Write data to storage tier.
4. In a single transaction:
   - Read `:O:` to get the current version_id.
   - If a KV exists in `:O:`
       - Move the current `:O:` entry to `:V:`\
       (key = `...V<object_name><current_version_id>`).
       - Write new live value to `:O:` with `version_id = O:.next_vid`, `next_vid = vid - 1`.
   - If no KV exists in `:O:`
       - Write to `:O:` with `version_id = max_uint32`, `next_vid = max_uint32 - 1`.
   - Delete the `P:O` coordination entry.

No GC entry — the old version is preserved in `:V:`, not orphaned.

**Crash recovery:** If the process crashes before the commit transaction, the `P:O` entry remains. The background sweeper finds the stale entry, frees orphaned storage-tier data by ref_tag, then deletes `P:O`. See [Sweeper Protocol](S3-Operations-Over-KV.md#sweeper-protocol).

### DELETE without version-id (create delete marker)

No `P:O` coordination entry is needed — since no storage-tier data is written.

1. In a single transaction:
   - Read `:O:` to get the current version_id.
   - If a KV exists in `:O:`
       - Move the current `:O:` entry to `:V:`\
       (key = `...V<object_name><current_version_id>`).
       - Write fenced delete marker to `:O:` with `version_id = O:.next_vid`, `next_vid = vid - 1`.
   - If no KV exists in `:O:`
       - Write fenced delete marker to `:O:` with `version_id = max_uint32`, `next_vid = max_uint32 - 1`.
       - Nothing to move to `:V:`.
2. Return the delete marker's version_id to the client.

No data is removed. No GC entry. All previous versions remain accessible by version-id.


### DELETE with version-id (remove specific version)

Permanently removes a specific version.\
Two cases depending on where the target version lives:

Case 1 — Target is in `:V:`:
- The `:V:` entry is deleted (or moved to `G` if it has data).
- No promotion. `:O:` is unaffected.

Case 2 — Target is the current version in `:O:`:
- Removing the current version requires promotion —
    - The latest version from `:V:` must become the new current version.
    - If no `:V:` entry exists, `:O:` is removed entirely.
  
In a single transaction:
- Read `:O:` to get the current version_id.
- If target version_id does not match (Case 1):
    - Read the `:V:` entry at `...V<object_name><target_version_id>`.
        - If not found → 404 (NoSuchKey).
    - If it had data (not a delete marker):
        - Move the `:V:` entry to the `G` namespace with a stripped value.
    - If it is a delete marker:
        - Simply delete the `:V:` entry.
- If target version_id matches (Case 2):
    - Save `old_next_vid = O:.next_vid` (preserve the version counter).
    - Find the latest entry in `:V:` for this object_name
        - range scan on `...V<object_name>` with limit=1
    - If a `:V:` entry exists:
        - Move it from `:V:` to `:O:` (promoting it to current).
        - Set `O:.next_vid = old_next_vid` (inherit counter from outgoing O:, NOT from promoted V:).
    - If no `:V:` entry exists:
        - Remove `:O:` entirely — the object ceases to exist.
    - If the removed `:O:` entry had data (not a delete marker):
        - Move to the `G` namespace with a stripped value.
    - If the removed `:O:` entry is a delete marker:
        - Simply delete it.

The `:O:` read is always inside the transaction —
- For operations that write O: (PUT, DELETE-without-vid, Case 2): the `:O:` write serves as the serialization point. Write-write conflict on `:O:` protects against concurrent modifications.
- For Case 1 (delete non-current version): `:O:` is only read (to confirm target ≠ current). Serialization is via write-write conflict on the V:<target> key. On FDB, the O: read also creates a conflict range. On TiKV, this is safe because V: entries are immutable — no concurrent operation can invalidate a non-current version deletion. See [transaction-safety.md](transaction-safety.md).
- See [The Uniform Rule](#the-uniform-rule) for the shared `:O:` write invariant (applies to all operations except Case 1).

This is the only operation that moves a KV from `:V:` to `:O:`.\
All other versioned operations only move from `:O:` to `:V:`.

**Child operations on old versions (tags, annotations):** S3 allows PutObjectTagging and PutObjectAnnotation with a `versionId` parameter targeting a non-current version. These operations read and write V:<vid> (updating `tag_count` or `annotation_count` in the version entry) instead of O:. The write to V:<vid> provides write-write conflict with DELETE Case 1 targeting the same version. See [child-kv-operations.md](child-kv-operations.md).

**DeleteBucket and version entries:** DeleteBucket scans both `:O:` and `:V:` for committed data. If any `:V:` entries exist, DeleteBucket returns `BucketNotEmpty` — the client must delete all versions before the bucket can be removed. This covers the case where a bucket was versioned, then suspended, and objects deleted non-versioned (leaving orphaned V: entries without corresponding O: entries). V: entries are treated identically to O: entries: committed client data that cannot be force-aborted. See [bucket_delete.md](bucket_delete.md).

**Undelete:**
- Removing a delete marker (DELETE with version-id targeting a delete marker in `:O:`) triggers Case 2.
- The latest entry from `:V:` is promoted to `:O:`.
- If the promoted entry is a live version, the object is restored.
- If it is another delete marker, the object remains deleted.

---

## Open Issue: Versioning State Change During In-Flight PUT

### The Problem

PUT Phase 3 must decide whether old O: goes to V: (versioned) or G:O (non-versioned). This decision uses the bucket's versioning state. If versioning is enabled or suspended between Phase 1 and Phase 3:

- Phase 3 uses cached bucket metadata from request start (stale).
- Wrong decision: old O: moved to G:O instead of V: → **previous version lost** (versioned was just enabled).
- Or: old O: moved to V: instead of G:O → unnecessary version preserved (versioned was just suspended — storage leak, benign).

The window is narrow (only during the exact transition moment while a PUT Phase 2 is in-flight). But the first case is data loss.

### Option A — Conditional B read on long Phase 2

If elapsed time between Phase 1 and Phase 3 exceeds a threshold (e.g., 1 second):

- Phase 3 re-reads B to get fresh versioning state before deciding V: vs G:O.
- Only impacts large Tier 3 PUTs with slow uploads (> 1 second Phase 2).
- Tier 1/2 (single-transaction): no issue (no Phase 1/3 split).
- Fast Tier 3 (< 1 second): no extra read.
- Very small subset of PUTs impacted.

### Option B — Cluster state transfer (broadcast invalidation)

Versioning enable/disable triggers a cluster-wide cache invalidation:

- All RGW processes refresh their bucket metadata cache for the affected bucket.
- Next operation on that bucket uses fresh versioning state.
- More robust — covers all in-flight operations, not just slow ones.
- Higher complexity (needs coordination channel between RGW instances).
- Same infrastructure could serve other bucket-level state changes (policies, quotas, lifecycle).

### Note

The same issue exists in the current RADOS model — `RGWSetBucketVersioning::execute` writes `bucket_info` with no broadcast, no cache invalidation, no wait for in-flight operations. Other RGW processes pick up the change on TTL-based cache refresh. In-flight operations use whatever state they cached at request start. No mitigation exists today. **Need to verify with Casey.**

---

## Worked Examples

### Example 1 — PUT, PUT, DELETE, PUT, DELETE

```
PUT(Key1, V1)
PUT(Key1, V2)
DELETE(Key1)
PUT(Key1, V3)
DELETE(Key1)
```

| Step | Operation | `:O:` | `:V:` |
|---|---|---|---|
| 1 | PUT V1 | V1 (ver=FFFF) | — |
| 2 | PUT V2 | V2 (ver=FFFE) | V1 (ver=FFFF) |
| 3 | DELETE | dm1 fenced (ver=FFFD) | V2 (ver=FFFE), V1 (ver=FFFF) |
| 4 | PUT V3 | V3 (ver=FFFC) | dm1 (ver=FFFD), V2 (ver=FFFE), V1 (ver=FFFF) |
| 5 | DELETE | dm2 fenced (ver=FFFB) | V3 (ver=FFFC), dm1 (ver=FFFD), V2 (ver=FFFE), V1 (ver=FFFF) |

(version_id values shown as last 4 hex digits for brevity; actual values are full uint32)

ListObjectsV2: skips fenced `:O:` → returns nothing.

ListObjectVersions: dm2, V3, dm1, V2, V1 — latest first.

### Example 2 — Double delete

```
PUT(Key1, V1)
DELETE(Key1)
DELETE(Key1)
```

| Step | Operation | `:O:` | `:V:` |
|---|---|---|---|
| 1 | PUT V1 | V1 (ver=FFFF) | — |
| 2 | DELETE | dm1 fenced (ver=FFFE) | V1 (ver=FFFF) |
| 3 | DELETE | dm2 fenced (ver=FFFD) | dm1 (ver=FFFE), V1 (ver=FFFF) |

Each DELETE creates a new delete marker.

### Example 3 — Undelete

```
ver_1=PUT(Key1, V1)
ver_2=DELETE(Key1)
DeleteObject(Key1, versionId=ver_2)
```

| Step | Operation | `:O:` | `:V:` |
|---|---|---|---|
| 1 | PUT V1 | V1 (ver=FFFF) | — |
| 2 | DELETE | dm1 fenced (ver=FFFE) | V1 (ver=FFFF) |
| 3 | Delete FFFE | V1 (ver=FFFF) | — |

Step 3:
- dm1's version_id matches `:O:`.
- Promotion triggers — V1 is moved from `:V:` to `:O:`.
- The object is restored.

GetObject(Key1) → returns V1. ListObjectsV2 → returns Key1.

### Example 4 — Delete specific old version

```
ver_1=PUT(Key1, V1)
ver_2=PUT(Key1, V2)
ver_3=PUT(Key1, V3)
DeleteObject(Key1, versionId=ver_1)
```

| Step | Operation | `:O:` | `:V:` |
|---|---|---|---|
| 1 | PUT V1 | V1 (ver=FFFF) | — |
| 2 | PUT V2 | V2 (ver=FFFE) | V1 (ver=FFFF) |
| 3 | PUT V3 | V3 (ver=FFFD) | V2 (ver=FFFE), V1 (ver=FFFF) |
| 4 | Delete FFFF | V3 (ver=FFFD) | V2 (ver=FFFE) |

Step 4:
- target version_id (FFFF) is not in `:O:` (FFFD).
- Simple `:V:` delete — no promotion.
- V1's entry is moved to the `G` namespace for async data cleanup.

---

## Listing Behavior

### ListObjectsV2

- Scans `:O:` only
- Filters out fenced delete markers — returns nothing for that object_name
- Non-versioned deleted objects are simply absent (their entries have been moved to the `G` namespace)
- No interaction with `:V:`.

### ListObjectVersions

- Merges `:O:` and `:V:` for each object_name.
- The `:O:` entry (live or fenced delete marker) represents the current version.
- For each object_name, it sorts first (latest version). `:V:` entries follow in version_id order (latest first by construction).
- Delete markers in both `:O:` (fenced) and `:V:` are returned with a `DeleteMarker` type indicator.

---

## GetObject Behavior

### Without version-id

1. Read `:O:` for the object_name.
2. If not found → 404 (NoSuchKey).
3. If fenced (delete marker) → 404 with `x-amz-delete-marker: true` and the delete marker's version_id.
4. If live → return the object.

Single read. No need to check `:V:`.

### With version-id

1. Read `:O:` for the object_name. Compare version_id in the value.
2. If it matches → return from `:O:` (or 405 Method Not Allowed error if fenced delete marker).
3. If it doesn't match → read `...V<object_name><version_id>`.
4. If found → return the version (or 405 Method Not Allowed error if a delete marker).
5. If not found → 404.

---

## Non-Versioned Buckets

Non-versioned buckets do not use `:V:` at all.
- No version_id tracking.
- No delete markers.
- No promotion.

Operation on Non-versioned buckets:
- PUT follows the standard 3-phase protocol:
    - (`P:O` → data write → commit transaction).
    - If the object key already exists, the old `:O:` entry is moved to the `G` (GC) namespace with a stripped value within the commit transaction.
        - See [S3 Operations — PUT](S3-Operations-Over-KV.md#put).
- DELETE moves the `:O:` entry to the `G` namespace in a single transaction.
    - The key is removed from `:O:` — subsequent reads find no entry and return 404 naturally.
        - See [S3 Operations — DELETE](S3-Operations-Over-KV.md#delete-non-versioned).


---

## Delete All Versions (Optional Vendor Extension)

> **Note:** AWS S3 does not provide a single API call to delete an object along with all its versions. This is an optional vendor-specific extension that may never be implemented.

- A single call deletes the `:O:` entry and writes one `G:F` (**F**ull Object GC) directive to the `G` namespace.
- The `G:F` key contains the `object_name` and the `ref_tag` from the `:O:` entry.
- The object becomes immediately invisible.
- Background workers asynchronously rangeScan `:V:` for all older versions of the object
    - free data
    - clean up children
    - and remove all entries.

See [S3 Operations — Delete All Versions](S3-Operations-Over-KV.md#delete-all-versions-optional-vendor-extension) for the detailed design.
