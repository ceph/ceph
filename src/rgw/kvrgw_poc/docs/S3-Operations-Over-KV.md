# S3 Operations over KV-DB

## Introduction

This document describes how S3 operations are implemented on top of the KV-based metadata layer defined in the KV-Based Design for RGW.

The design is DB-agnostic.\
All operations are expressed in terms of the KV Access API (Get, Put, Delete, RangeScan, RangeDelete, transactions).\
Each supported DB implements this API using its native client library.

### Two-Tier Metadata Model

Object metadata is stored in two tiers:

- **Tier 1 — KV value.** \
A single KV entry per object holds all user-visible S3 attributes, RGW internal fields, and read-path metadata (manifest, compression, encryption) when they fit.\
This is the primary metadata store.\
HEAD requests and listing are served entirely from this tier.

- **Tier 2 — Extended value.**\
When read-path metadata overflows the KV entry (large compressed multipart objects), it is stored in an extended value.\
The KV entry contains routing info pointing to the extended value's location.

The extended value is a logical concept — its physical storage is pluggable:
- Data header prepended to the first data chunk.
- Standalone metadata file alongside the data.
- Object annotation (sidecar metadata).
- Additional KV entries under a related key.

The choice depends on the storage backend. RGW must not assume a specific storage mechanism.

For the vast majority of objects (single-part, small multipart, uncompressed multipart), the KV entry is the sole metadata store. Only very large compressed multipart objects require the extended value for byte-range resolution.

---

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


**Field definitions:**

- **namespace (1 B, ASCII)** — KV namespace:
- `S` - S3 object entries
- `B` - bucket metadata (keyed by tenant_id + bucket_name)
- `T` - tenant metadata
- `Z` - zone/realm metadata
- `G` - GC entries for background data cleanup
- `P` - pending multi-phase operations — crash recovery coordination

- **shard_count (2 B, uint16 big-endian)** — number of shards for this bucket.\
`shard_id = hash(bucket_id + object_name) % shard_count`.
- **shard_id (2 B, uint16 big-endian)** — zero-based shard index.\
Invariant: `shard_id < shard_count`. When `shard_count = 1`, always `0`.
- **bucket_id (8 B)** — binary bucket identifier.
- **cat (1 B, ASCII)** — category tag: `O`, `V`, `M`, or `C`.
- **object_name (1–1024 B)** — the S3 object key, stored as raw bytes. Preserves natural lexicographic ordering.
- **version_id (4 B, uint32 big-endian)** — descending; first value is `max_uint32`, then decreasing. Latest version sorts first.
- **ref_tag (12 B)** — compact unique identifier from the parent `:O:` entry (see [ref_tag](#ref_tag)).
- **child_type (1 B, ASCII)** — `A`, `T`, or `E`.
- **part_number (2 B, uint16 big-endian)** —
    - 0 for upload metadata
    - 1–10000 for individual parts
- **child_id (0–512 B)** — annotation name (type `A`); empty for `T`, `E`.

---

## Bucket Operations

### CreateBucket (MakeBucket)

1. In a single transaction:
   - Read `B <tenant_id> <bucket_name>` — if exists → return `BucketAlreadyExists`.
   - `ATOMIC_ADD` on the monotonic bucket counter KV → allocate new `bucket_id`.
   - Write `B <tenant_id> <bucket_name>` → `bucket_id` (8 B) + bucket metadata (ACL, policies, quota, versioning state, lifecycle rules, shard_count).
   - Commit.

The transaction ensures no duplicate bucket names per tenant and no wasted bucket_ids on conflict — the atomic-add rolls back with the transaction.

**Bucket_id allocation:** A single KV entry serves as a monotonic counter. Key = well-known constant, value = uint64. Each CreateBucket increments it atomically within the transaction. The resulting value becomes the new bucket's 8-byte `bucket_id`. See [Listing-and-Key-Scheme.md](Listing-and-Key-Scheme.md) for the `B` namespace key layout.

**DB portability note:** `ATOMIC_ADD` is an FDB conflict-free mutation — concurrent CreateBucket transactions don't conflict on the counter key. On TiKV (or other KV stores without atomic mutations), replace with `counter = Read(K), Write(K, counter+1)`. This causes concurrent CreateBucket transactions to conflict on the counter key, but bucket creation is infrequent so the retry cost is negligible.

### HeadBucket

1. Read `B <tenant_id> <bucket_name>`.
2. If not found → return 404 (NoSuchBucket).
3. Return bucket metadata from the value.

### DeleteBucket

DeleteBucket uses a minimal transaction — it only verifies committed data is empty and deletes B. In-flight operations (P: and M: entries) are NOT processed inside the transaction — they self-destruct after the bucket is removed. See [Bucket-State-Management.md — Bucket Deletion Rules](Bucket-State-Management.md#6-bucket-deletion-rules) for the full protocol and safety analysis.

**DeleteBucket transaction:**

```
txn {
  RangeScan :O: (limit 1) → if hit → return BucketNotEmpty
  RangeScan :V: (limit 1) → if hit → return BucketNotEmpty

  if bucket.block_delete_on_multipart:
    RangeScan :M: (limit 1) → if hit → return BucketNotEmpty

  delete(B)
  commit
}
```

After commit:
- Invalidate local bucket cache; optionally broadcast cluster-wide cache revoke.
- P: and M: entries self-destruct through existing mechanisms:
  - Live PUT Phase 3: `get(B)` → null → self-abort → create G:S, delete P:O.
  - Live CompleteMultipartUpload Phase 3: `get(B)` → null → self-abort.
  - Crashed processes: sweeper finds stale P:O and P:M on next pass → G:S, delete P:O or P:M.
- An optional post-commit cleanup loop speeds up storage-tier reclamation by scanning P: and M: entries for the deleted bucket in batches. See [Bucket-State-Management.md — Post-Commit Cleanup](Bucket-State-Management.md#post-commit-cleanup-best-effort-optimization).

**FDB safety:**

Two independent conflict points protect against concurrent PUT:

1. **Conflict on B:** PUT Phase 3 reads `get(B)` (read conflict range). DeleteBucket writes `delete(B)`. If DeleteBucket commits first → PUT Phase 3 conflicts on B → retries → sees B null → self-aborts.
2. **Conflict on O:** PUT Phase 3 writes `put(O:)`. DeleteBucket reads `RangeScan :O:` (read conflict range). If PUT Phase 3 commits first → DeleteBucket conflicts on O: → retries → sees O: → BucketNotEmpty.

Either conflict point catches the race. See [Bucket-State-Management.md — FDB Safety](Bucket-State-Management.md#fdb-safety).

**TiKV safety:**

On TiKV, `get(B)` in PUT Phase 3 is a snapshot read — no conflict range. A PUT Phase 3 that started before DeleteBucket can commit O: after B is deleted. Best-effort safety with negligible residual risk (single-digit ms race window). No worse than the current RADOS model. See [Bucket-State-Management.md — TiKV Safety](Bucket-State-Management.md#tikv-safety).

### Multipart Upload Handling on DeleteBucket

A per-bucket flag `block-delete-on-multipart-upload` controls whether DeleteBucket checks for active multipart uploads:

- **Flag set (AWS requirement for directory buckets):** DeleteBucket scans `:M:` inside the transaction. If any `:M:` head entries exist → return `BucketNotEmpty`. The client must abort all multipart uploads before deleting the bucket.

- **Flag clear (default behavior):** DeleteBucket does not check `:M:` entries. After B is deleted, active multipart uploads self-destruct: CompleteMultipartUpload Phase 3 does `get(B) → null → abort`. Orphaned M: heads are cleaned by the lifecycle engine (`AbortIncompleteMultipartUpload` policy) or the post-commit cleanup loop.

### DeleteBucket Race Condition in Current RADOS Model

The current RGW `delete_bucket` flow has a race condition between `check_bucket_empty` and concurrent PUT operations:

1. RGW-A resolves bucket name → bucket_id (cached). Starts PUT.
2. RGW-B starts DeleteBucket. Calls `check_bucket_empty()` — scans all BI shards. Finds no entries (RGW-A hasn't called `prepare_op` yet). Returns empty.
3. RGW-A calls `prepare_op` — writes pending BI entry to shard OMAP.
4. RGW-A writes data to storage tier (Phase 2).
5. RGW-A calls `complete_op` — succeeds. Object is committed. PUT returns success to client.
6. RGW-B continues DeleteBucket — removes bucket entrypoint, removes bucket instance info, calls `clean_index()` which removes shard objects (including the committed BI entry).

Result: the client received success on PUT, but the object is inaccessible — no bucket metadata exists to resolve the bucket name. The BI entry and data are silently destroyed by `clean_index()`. The client cannot read, delete, or even discover the object.

No fencing step exists in the current flow. The `BUCKET_DELETED` flag is only set in the multisite syncing path and is never checked on the write path.

**KV model solution (FDB):**
- The KV model eliminates this race on FDB. 
- DeleteBucket's RangeScans create read conflict ranges — \
any concurrent PUT Phase 3 (writing `O:`) triggers a conflict. 
- PUT Phase 3 creates read conflict ranges on the existence of the bucket doing `get(B)` and `put(O:)` in a single transaction.
- On TiKV, the guarantee is weaker — snapshot reads on `get(B)` cannot detect concurrent `delete(B)`. Best-effort safety with negligible residual risk (single-digit ms race window). See [Bucket-State-Management.md — TiKV Safety](Bucket-State-Management.md#tikv-safety).

### ListBuckets

1. Prefix scan on `B <tenant_id>` — returns all bucket names for the tenant in lexicographic order.
2. Return bucket names and creation timestamps.

### Bucket Resolution (per-request)

Every S3 object operation begins by resolving `(tenant_id, bucket_name) → bucket_id`:

All paths call `get_cached_bucket_entry()` at request entry — resolves `bucket_id`, enforces policy, and checks quota. The cache uses a 3-second TTL with refresh-before-reject (if cached state would deny, refresh before rejecting). See [bucket_cache.md](bucket_cache.md) for the full cache model.

**Read paths (GET, HEAD, LIST):**

1. `get_cached_bucket_entry()` — resolves bucket_id, enforces policy (s3:GetObject / s3:ListBucket).
2. Proceed with KV read or range scan.
3. LIST additionally reads `B` fresh inside the operation (negligible cost vs range scan). See [bucket_cache.md — Listing](bucket_cache.md#5-getb-for-listing).

**Write paths (PUT, DELETE without version-id, CompleteMultipartUpload):**

Write operations use cached bucket metadata at request entry (soft check) and read `B` fresh in the commit transaction (hard enforcement). Phase 3 also reads `get(P:O)` to verify the coordination entry was not removed by the sweeper or bucket-delete. See [PUT — bucket verification](#put) and [Bucket-State-Management.md](Bucket-State-Management.md).

1. `get_cached_bucket_entry()` — soft-check policies/quotas, resolve `bucket_id` for key construction.
2. Phase 1: single KV write — `P:O` coordination entry. No transaction, no reads.
3. Phase 3 transaction: `get(P:O)` + `get(B)` + `get(O:)` + enforcement checks + write `:O:` + delete `P:O`. The `get(B)` read enforces current policies, quotas, versioning state, and bucket existence. The `get(P:O)` read provides existence verification and conflict detection against the sweeper and bucket-delete.

**Cached-only paths (DELETE with version-id, tagging, ACL):**

These operations rely entirely on cached bucket metadata — no `get(B)` in their transaction. See [bucket_cache.md — Section 8](bucket_cache.md#8-delete-and-getb) and [Section 10](bucket_cache.md#10-which-operations-use-cached-vs-fresh-bucket-state).

**Cache invalidation:** on DeleteBucket and on any write-path `Get(B)` miss. The cache is stable under normal operation — bucket_id never changes after creation. Invalidation events are rare (bucket deletion). The same cache holds bucket metadata (versioning state, ACLs, policies) to avoid repeated reads.

---
 
## HEAD

1. `get_cached_bucket_entry(bucket_name, client, s3:GetObject, 0)` — resolve bucket_id, enforce policy. If denied → 403. If bucket not found → 404.
2. Read the KV entry.
3. If not found → return 404.\
   If fenced delete marker → return 404 with `x-amz-delete-marker: true` and the version_id.
4. Return all S3-visible attributes from the value.

Single KV read. No data-tier access. No extended value access.

All user-visible attributes are in the KV value: etag, last_modified, size, storage_class, owner, content_type, checksum_algorithm, content_disposition, cache_control, user metadata (x-amz-meta-*), checksum values, object lock settings.

**Cost compared to the current model.** Cheaper.

A single KV read replaces a RADOS head-object read.

<a id="rados-cache-model"></a>
RADOS was not optimized for small KV access —
- Its caching model holds large data objects in cache, reducing the number of metadata entries that fit in cache.
- Bucket listing in the current model further degrades HEAD cache hit rates — the sequential scan evicts existing metadata cache entries, polluting the cache with listing data that won't be reused.
- RADOS also has a much heavier code stack.

The KV store is purpose-built for small KV access, operates on SSD/NVMe, and its distributed cache is optimized for high entry counts.

---

## GET (Full Object)

1. `get_cached_bucket_entry(bucket_name, client, s3:GetObject, 0)` — resolve bucket_id, enforce policy. If denied → 403. If bucket not found → 404.
2. Read the KV entry.
3. If not found → return 404. If fenced delete marker → return 404.

4. **KV value contains read-path metadata (common case):**
   - Extract chunk pointers directly from the KV value.
   - Read data chunks from the storage tier.
   - No extended value access needed.

5. **KV value does not contain read-path metadata (large compressed multipart):**
   - Read the extended value from its storage location.
     For full-object reads from offset 0, the extended value may be included
     in the first data chunk at no extra cost (when stored as a data header).
   - Verify ref_tag from the extended value against the KV value.
   - Use the full manifest to read and reassemble data chunks.

6. Decompress/decrypt and stream to client.

**Cost compared to the current model.** The KV read is a new cost. In the current model, metadata is piggybacked on the RADOS head-object data read — no separate metadata round-trip.

This is mitigated by the KV store's distributed cache serving hot entries from memory. The same [RADOS caching limitations](#rados-cache-model) apply here — making the extra round-trip cheaper than it appears.

---

## GET (Byte-Range)

Byte-range reads cannot assume reading from the start of the object. The read-path metadata (manifest, compression block table, encryption parameters) must be resolved before the target chunk can be located.

1. `get_cached_bucket_entry(bucket_name, client, s3:GetObject, 0)` — resolve bucket_id, enforce policy.
2. Read the KV entry.
3. If not found → return 404. If fenced delete marker → return 404.

3. **KV value contains read-path metadata (common case):**
   - Use it directly to locate the target chunk and determine compression block boundaries.
   - Issue a read to the data store for the target byte range.
   - No extended value access needed.

4. **KV value does not contain read-path metadata (large compressed multipart):**
   - Check the local extended-value cache (see [below](#extended-value-cache)). On cache hit with matching ref_tag → use the cached metadata.
   - On cache miss → read the extended value from its storage location. Cache it locally.
   - Locate the target chunk and read the byte range.

5. Decompress/decrypt and stream to client.

### Extended Value Cache

For large objects whose read-path metadata overflows the KV entry, byte-range reads would require reading the extended value on every request. To avoid this, each RGW server maintains a local in-memory cache of extended values.

**Cache key:** `(bucket_id, object_name, version_id, ref_tag)`.

**Flow:**
- First byte-range read: read the extended value, extract manifest/compression/encryption info, cache locally.
- Subsequent byte-range reads: read the KV entry (always — to confirm existence and get the current ref_tag), compare ref_tag against cached entry, use cached metadata on match.

**Invalidation:** If the KV read returns not-found (object was deleted) or a different ref_tag (object was overwritten), the cached entry is purged. No separate invalidation protocol — the ref_tag comparison is the integrity check.

S3 objects are immutable. The ref_tag only changes on overwrite or delete — both rare for large objects.

**Cost compared to the current model.** Similar for the common case.

One KV read plus one data read, versus one RADOS head-object read plus a seek to the target range.

For large compressed multipart objects whose read-path metadata overflows the KV value, the first byte-range read adds an extended value access. Subsequent reads hit the local cache.

Overall comparable. The same [RADOS caching limitations](#rados-cache-model) apply here — making the KV read cheaper than the equivalent RADOS metadata access.

---

## PUT

<a id="put"></a>
PUT uses a three-tier data storage model. The tier is selected by object size at PUT time:

- **Tier 1 (< 256B):** Data stored inline in the O: value. Single transaction. No P:O, no storage-tier write.
- **Tier 2 (256B–8KB):** Data stored in child KV entry C:<ref_tag>D. Single transaction. No P:O, no storage-tier write.
- **Tier 3 (> 8KB):** Data stored on the storage tier. Three-phase protocol with P:O coordination.

Multipart uploads always use Tier 3. See [data-tiering.md](data-tiering.md) for Tier 1/2 transactions, D: namespace key schema, migration protocol, and capacity considerations.

The chunk descriptor in the O: value encapsulates the data location: `{type: INLINE, data}`, `{type: CHILD_D}`, or `{type: STORAGE, storage_id, blob_id, offset, length}`. `storage_id` identifies the backend (rados, xfs, etc.).

**Tier 3 protocol (> 8KB) — multi-phase with P:O coordination:**

PUT (Tier 3) is a multi-phase operation coordinated through the `P` namespace (see [Pending Operations Namespace](#pending-operations-namespace)) to prevent orphaned storage-tier data on crash.

**Phase 1 — record intent (single KV write):**

1. Generate a `ref_tag` for the new object instance.
2. Write a `P:O` coordination entry with the object_name, ref_tag, and estimated size.
   - Single KV write — no transaction, no reads.
   - Uses cached `bucket_id` to construct the key. Tenant, policies, and quotas are soft-checked from cache; hard enforcement happens in Phase 3. See [Bucket-State-Management.md — Caching Strategy](Bucket-State-Management.md#1-bucket-metadata-caching-strategy).

**Phase 2 — write data (no KV involvement):**

3. Write data to the storage tier. Chunks embed the `ref_tag`. Include the extended value (manifest, compression, encryption metadata) in the storage-tier-appropriate location.

**Phase 3 — commit metadata (single-shard transaction, single enforcement point):**

Phase 3 reads B fresh and applies the **adapt-or-abort** rule: versioning state changed → adapt; bucket deleted, bucket_id mismatch, policy denied, or quota exceeded → abort and self-clean. See [Bucket-State-Management.md — PUT Phase 3](Bucket-State-Management.md#10-put-3-phase-protocol--full-detail) for the full protocol.

4. In a single transaction:
   - `get(P:O)` — if null → abort (sweeper or bucket-delete removed it).
   - `get(B)` — fresh bucket metadata (bucket_id, versioning state, quotas, policies).
   - `get(O:)` — read existing entry (may be null for new objects).
   - If B not found → `put(G:S, {ref_tag}); delete(P:O); commit` → return HTTP 404 (NoSuchBucket).
   - If B.bucket_id != cached_bucket_id → `put(G:S, {ref_tag}); delete(P:O); commit` → return HTTP 500 (InternalError).
   - If policy_denies(client, operation, B.policy) → `put(G:S, {ref_tag}); delete(P:O); commit` → return HTTP 403 (AccessDenied).
   - If exceeds(B.quota, this_object_size) → `put(G:S, {ref_tag}); delete(P:O); commit` → return HTTP 429 (QuotaExceeded).
   - Apply current versioning state from B — displace old O: and write new O::
     - **If unversioned:**
       - If O: exists: if O:.vid != NO_VERSION (0) → abort (invariant violation); `put(G:O, old)`
       - `put(O:, new_value with vid=NO_VERSION, next_vid=max_uint32)`
     - **If versioned:**
       - If O: exists with real vid (≥ 2): `put(V:<old_vid>, old)` — preserve in history
       - If O: exists with NULL_ID (1) or NO_VERSION (0): `put(G:O, old)` — null-version entries [never go to V:](Bucket-State-Management.md#9-null-version-and-no_version-rules)
       - If O: exists: `put(O:, new_value with vid=O:.next_vid, next_vid=vid-1)`
       - If no O: exists: `put(O:, new_value with vid=max_uint32, next_vid=max_uint32-1)`
     - **If suspended:**
       - If O: exists with real vid (≥ 2): `put(V:<old_vid>, old)` — preserve in history
       - If O: exists with NULL_ID (1) or NO_VERSION (0): `put(G:O, old)` — overwrite null/pre-versioning slot
       - If O: exists: `put(O:, new_value with vid=NULL_ID, next_vid=O:.next_vid)`
       - If no O: exists: `put(O:, new_value with vid=NULL_ID, next_vid=max_uint32)`
   - Delete the `P:O` coordination entry.
   - Commit.

The `get(P:O)` read is essential for correctness: it verifies the coordination entry was not cleaned by the sweeper (data freed) or removed by bucket-delete. The read also provides conflict detection — if the sweeper or bucket-delete removes `P:O` concurrently, the transaction conflicts and retries, seeing null on retry and aborting. See [Bucket-State-Management.md](Bucket-State-Management.md) for the full race-condition analysis.

The `get(O:)` read detects overwrites and ensures old data is moved to `G` for async cleanup — a single atomic commit replaces the old entry with the new one. In the current RADOS model, overwriting requires updating the bucket-index entry and deleting the old head object as separate non-atomic operations. See [GC Namespace](#gc-namespace-and-background-cleanup).

All entries for the same object_name share the same shard (version_id is excluded from the hash), so the entire version transition — displacement + new entry — is a single atomic transaction. No OLH coordination object needed. See [Versioned-Bucket-Operations.md](Versioned-Bucket-Operations.md) for worked examples and [Bucket-State-Management.md — Null-Version Rules](Bucket-State-Management.md#9-null-version-and-no_version-rules) for the displacement rationale.

**Crash recovery:** If the process crashes between Phase 1 and Phase 3, the `P:O` entry remains. The background sweeper moves the stale entry to `G:S` (atomically deleting `P:O`). A separate GC worker then frees orphaned storage-tier data using the `ref_tag` from the `G:S` entry. See [Sweeper Protocol](#sweeper-protocol) and [Bucket-State-Management.md](Bucket-State-Management.md).

**Comparison to current model.**

The three phases map directly to the current RGW write path:

| KV model | Current RGW |
|---|---|
| Phase 1 — write `P:O` entry (single KV write, no transaction) | `rgw_bucket_prepare_op` (OMAP pending log on bucket-index shard) |
| Phase 2 — Storage-tier write | RADOS transaction (write + setxattrs on head object) |
| Phase 3 — `get(P:O)` + `get(B)` + `get(O:)` + write `:O:` + delete `P:O` (transaction) | `rgw_bucket_complete_op` (OMAP completion on bucket-index shard) |

Both models use the same prepare/complete pattern around the data write — necessary because the data-tier and metadata-tier cannot share a transaction. The `P:O` entry serves the same role as `rgw_bucket_prepare_op`: crash recovery coordination.

**Cost per PUT:** Phase 1 = 1 write (`P:O`), no transaction. Phase 3 = 3 reads (`P:O` + `B` + `O:`) + 1–2 writes (`:O:` + optionally `G:O` or `V:` on overwrite) + 1 delete (`P:O`), single transaction. Total KV ops: 3 reads + 2–3 writes + 1 delete (plus storage-tier write in Phase 2).

<a id="rados-pg-serialization"></a>
**Key differences:**

- **No PG-level serialization.** 
    - In the current model, all three phases are subject to PG serialization — OMAP mutations on the bucket-index shard and head object writes both serialize through the PG primary.
    - The KV store has no such constraint — unrelated S3 objects never contend.
- **No index drift.** 
    - The current model maintains two separate structures (head object + bucket-index) that can drift apart on crash.
    - The KV model has a single metadata entry (`:O:`) — it IS the index. No dual update, no drift.

---

## DELETE (without version-id)

DELETE without version-id is a single-transaction operation. No `P:O` coordination entry is needed — no storage-tier data is written. Like PUT Phase 3, it reads B fresh and adapts to the current versioning state. See [Bucket-State-Management.md — Refresh Strategy](Bucket-State-Management.md#2-refresh-strategy-by-operation-type) and [Versioned-Bucket-Operations.md — DELETE without version-id](Versioned-Bucket-Operations.md#delete-without-version-id-create-delete-marker).

1. In a single transaction:
   - Read `B` to get fresh bucket state (versioning, policies).
   - Read `:O:` to get the current entry.
   - **If O: exists** — displace old O: (same displacement rules as [PUT Phase 3](#put)):
     - **If unversioned:**
       - If O:.vid != NO_VERSION (0) → abort (invariant violation)
       - `put(G:O, old)` — schedule data + child cleanup
       - Delete O: entirely — no delete marker
     - **If versioned:**
       - If O: has real vid (≥ 2): `put(V:<old_vid>, old)` — preserve in history
       - If O: has NULL_ID (1) or NO_VERSION (0): `put(G:O, old)` — [null-version entries never go to V:](Bucket-State-Management.md#9-null-version-and-no_version-rules)
       - Write fenced delete marker to O: with `vid = O:.next_vid`, `next_vid = vid - 1`
     - **If suspended:**
       - If O: has real vid (≥ 2): `put(V:<old_vid>, old)` — preserve in history
       - If O: has NULL_ID (1) or NO_VERSION (0): `put(G:O, old)` — overwrite null/pre-versioning slot
       - Write fenced delete marker to O: with `vid = NULL_ID`, `next_vid = O:.next_vid`
   - **If no O: exists:**
     - If unversioned: no-op — object doesn't exist, return success (idempotent)
     - If versioned: write fenced DM with `vid = max_uint32`, `next_vid = max_uint32 - 1`
     - If suspended: write fenced DM with `vid = NULL_ID`, `next_vid = max_uint32`
2. Return the delete marker's version_id to the client (versioned/suspended only).

In versioned/suspended modes: no data is removed. No GC entry. Old versions remain accessible by version-id.\
In unversioned mode: object is removed and data is scheduled for GC via the G:O entry.

GetObject reads `:O:`, sees the fenced flag, returns 404 with `x-amz-delete-marker: true`.

All entries for the same object_name share the same shard (version_id is excluded from the hash), so the operation is a single-shard atomic transaction.

Data and child cleanup (unversioned case) happen asynchronously — background workers scan `G`, free storage-tier data, remove child KV entries (`RangeDelete(...C<ref_tag>)`), then remove the `G` entry.

See [Versioned-Bucket-Operations.md](Versioned-Bucket-Operations.md) for the uniform rule, worked examples, and promotion logic.

**Comparison to current model (non-versioned DELETE).**

The current RADOS DELETE requires four non-atomic operations on at least three different RADOS objects:

| Step | RADOS model | KV model |
|---|---|---|
| 1. Record intent | `rgw_bucket_prepare_op` (OMAP pending log on bucket-index shard) | — |
| 2. Fence the object | RADOS delete of head object across all K+M EC members (expensive, potentially on slow HDD) | Move `:O:` to `G` (single KV transaction, SSD/NVMe) |
| 3. Finalize index | `rgw_bucket_complete_op` (OMAP completion on bucket-index shard) | —  |
| 4. Schedule GC | Write to GC log (separate RADOS object — schedules tail object cleanup) | — (The transaction above already created `G` entry in the GC domain) |

The KV model collapses all four steps into a single atomic transaction. No separate prepare/complete cycle, no separate GC log write.

**Key differences (non-versioned):**

**1. Atomicity — single domain vs. three independent domains.** The KV model performs DELETE as a single transaction: read `:O:`, write to `G`, delete from `:O:` — committed atomically. The RADOS model operates across three independent domains (bucket-index, head objects, GC log) with no cross-domain atomicity.

**2. Performance — fewer operations, always fast.**

| KV model (single transaction) | RADOS model (multiple domains) |
|---|---|
| Read `B` + Read `:O:` KV | BI `rgw_bucket_prepare_op` (OMAP write) |
| Write stripped KV to `G` | Head object read |
| Delete KV from `:O:` | Head object delete across K+M EC members |
| | GC log write |
| | BI `rgw_bucket_complete_op` (OMAP write) |

**Comparison to current model (versioned DELETE — create delete marker).**

| | KV model | RADOS model |
|---|---|---|
| **Reads** | 2 (read `B` + read `:O:`) | 1 (read BI OMAP to resolve OLH state) |
| **Metadata writes** | 1-2 writes in a single transaction: write DM to `:O:`, conditionally write old `:O:` to `:V:` | 4-5 writes across 3 domains |
| **Storage-tier writes** | 0 | 1 — create zero-byte DM head object across K+M EC members |
| **Domains touched** | 1 (KV) | 3 (`rgw.buckets.index`, `rgw.buckets.data`, OLH object) |
| **Transactions** | 1 atomic | 0 — all writes are independent, non-atomic |
| **Total I/O ops** | 3-4 (2 reads + 1-2 writes, single domain, SSD/NVMe) | 6-7 metadata ops + K+M storage-tier writes, across 3 domains, potentially on HDD |

**Key differences (versioned):**

**1. No standalone RADOS object for the delete marker.** In the RADOS model, a delete marker is a full RADOS head object — zero bytes of user data, but carrying full RADOS overhead (PG membership, OSD tracking, recovery bookkeeping). On EC pools, replicated across all K+M members. In the KV model, a delete marker is a fenced flag in the `:O:` entry — a few bytes in an existing KV value. No separate object, no EC fan-out.

**2. No OLH.** The RADOS model requires an Object Logical Head — a separate RADOS object that acts as an indirection pointer to the current version. Creating a delete marker must update both the OLH object and the bucket-index, with a log replay mechanism (`apply_olh_log`) to keep them consistent. Racing deletes can leak OLH entries. The KV model has no OLH — the `:O:` entry IS the current version. No indirection, no log replay, no leak.

**3. Atomicity.** The KV model performs all operations in a single transaction: displace old version, write DM to `:O:`. The RADOS model operates across three independent domains with no cross-domain atomicity.

---

## DELETE (with version-id — remove specific version)

Permanently removes a specific version. This operation does not include `get(B)` in its transaction — it is mechanical (targets a specific version_id regardless of versioning state), bucket existence is self-resolving (no B → no O:/V: → 404), and policy enforcement uses the cached soft-check with refresh-before-reject. See [bucket_cache.md — DELETE and get(B)](bucket_cache.md#8-delete-and-getb).

Two cases depending on where the target version lives:

**Case 1 — Target is not the current version (i.e. KV lives in `:V:`:)**
- The `:V:` entry is deleted (or moved to `G` if it has data).
- No promotion. `:O:` is unaffected.

**Case 2 — Target is the current version in `:O:`:**
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
            - Set `O.version_id = version_id from Key` (the promoted V: ID).
            - Set `O:.next_vid = old_next_vid` (inherit counter from outgoing O:, NOT from promoted V:).
            - This prevents version ID reuse after deletion — deleted IDs are permanently consumed.
    - If no `:V:` entry exists:
        - Remove `:O:` entirely — the object ceases to exist.
    - If the removed `:O:` entry had data (not a delete marker):
        - Move to the `G` namespace with a stripped value.
    - If the removed `:O:` entry is a delete marker:
        - Simply delete it.

The `:O:` read is always inside the transaction —
- It serves as the serialization point.
- Every operation that mutates the version chain writes to `:O:`
- Write-Write conflict on `:O:` protects against concurrent modifications.
- See [Versioned-Bucket-Operations — The Uniform Rule](Versioned-Bucket-Operations.md#the-uniform-rule) for the shared `:O:` write invariant.

**Undelete:**
- Removing a delete marker (DELETE with version-id targeting a delete marker in `:O:`) triggers Case 2.
- The latest entry from `:V:` is promoted to `:O:`.
- If the promoted entry is a live version, the object is restored.
- If it is another delete marker, the object remains deleted.

**Comparison to current model (Case 1).**

In the RADOS model, each version (including delete markers) is a standalone RADOS head object in `rgw.buckets.data`.\
Deleting a non-current version requires reading the OLH to confirm the target is not current, then following the standard bucket-index transaction:

| Step | RADOS model | KV model |
|---|---|---|
| 0. Resolve current version | Read OLH (`rgw.buckets.data`) to confirm target is not current | **Transaction: Step-1:**  Read `:O:` — version_id doesn't match → Case 1 |
| 1. Read target version | — (head object read is part of delete) | **Transaction: Step-2:** Read `:V:` entry |
| 2. Record intent | `rgw_bucket_prepare_op` — OMAP write (`rgw.buckets.index`) | — |
| 3. Delete head object | RADOS delete across K+M EC members (`rgw.buckets.data`) | — (no head object exists) |
| 4. GC log write | Schedule tail object cleanup (`rgw.log`) — conditional, only if data beyond head | — (part of step 6) |
| 5. Finalize index | `rgw_bucket_complete_op` + OLH log entry — OMAP write (`rgw.buckets.index`) | — |
| 6. Sync OLH object | `apply_olh_log()` — update OLH RADOS object (`rgw.buckets.data`) | **Transaction: Step-3:** move `:V:` to `G` (or delete if DM) |
| 7. Trim OLH log | Remove applied log entries — OMAP write (`rgw.buckets.index`) | — |

RADOS:
- 1 read + 5-6 non-atomic writes across 3 pools\
(`rgw.buckets.index`, `rgw.buckets.data`, `rgw.log`).
- Bucket index shard PG is hit 3 times.
- The OLH log records the deletion and `apply_olh_log()` must sync the OLH RADOS object.
- **For non-current delete markers:** the same full flow applies — EC fan-out to delete a zero-byte RADOS head object.

KV:
- Single transaction with 2 reads and 1-2 writes
- Single domain
- No OLH
- No head object
- No EC fan-out.

**Comparison to current model (Case 2).**

Deleting the current version is the most complex versioned delete in the RADOS model.\
It requires promotion — the OLH must be re-pointed to the next version.\
The OLH log records two operations per epoch for this case: `REMOVE_INSTANCE` + `LINK_OLH` (or `UNLINK_OLH` if the last version is removed).

Step-by-step comparison (normal path, no crash recovery):

| Step | RADOS model | KV model |
|---|---|---|
| 0. Resolve current version | Read OLH (`rgw.buckets.data`) to confirm target is the current version | **Transaction: Step-1:** Read `:O:` — version_id matches → Case 2 |
| 1. Find version to promote | Scan BI instance entries for next version (`rgw.buckets.index`) | **Transaction: Step-2:** Range scan on `:V:<object_name>` (first key) |
| 2. BI cls transaction | prepare + OLH log (2 entries) + instance removal + complete (`rgw.buckets.index`) | — |
| 3. Sync OLH object | `apply_olh_log()` — update OLH RADOS object to point to promoted version, or delete if last (`rgw.buckets.data`) | — |
| 4. Trim OLH log | Remove applied log entries — OMAP write (`rgw.buckets.index`) | — |
| 5. Delete head object | RADOS delete across K+M EC members, zero-byte fan-out if target is a DM (`rgw.buckets.data`) | — (no head object exists) |
| 6. GC log write | Schedule tail object cleanup (`rgw.log`) — conditional, only if data beyond head | — (part of step 7) |
| 7. Commit | — (all steps above are independent, non-atomic) | **Transaction: Step-3:** overwrite `:O:` with promoted `:V:` value, delete from `:V:`, conditionally write old `:O:` to `G` |

RADOS:
- 1 read + 1 scan + 5-6 non-atomic writes across 3 pools.
- Bucket Index shard PG hit 3 times (BI transaction, OLH log read, log trim).
- OLH and head object may be on different PGs in `rgw.buckets.data`.

KV:
- Single transaction with 1 read, 1 scan and 1-3 writes (depending on `:O:` and `:V:` state)
- All atomic on a single shard.

The version scan (step 1) has the same cost in both models — a single seek to find the next version, O(1) regardless of how many versions exist.

The KV model has no OLH —
- `:O:` IS the current version pointer.
- Promotion is a direct value overwrite within a single transaction, not a log-replay sync protocol.

---

## LIST (ListObjectsV2)

### Without sharding

1. Resolve bucket: (tenant_id, bucket_name) → bucket_id (local cache).
2. Read `B` fresh — always, regardless of cache age. One extra read is negligible against the range scan cost. Refreshes the cache for subsequent GET/HEAD requests. If B not found → return 404 (NoSuchBucket). If policy denies → return 403. See [Bucket-State-Management.md — Refresh Strategy](Bucket-State-Management.md#2-refresh-strategy-by-operation-type).
3. Range scan on `:O:` entries for this bucket — `<namespace><shard_count><shard_id><bucket_id>O` — with the appropriate start marker and limit.
4. Filter out fenced entries (delete markers in versioned buckets). Non-versioned deleted objects have no `:O:` entry — they are simply absent.
5. Return up to 1000 keys with listing attributes from the KV values.

Pagination: ListObjectsV2 uses an opaque continuation token that embeds `bucket_id`, last key, and shard markers. On each page, `get(B).bucket_id` is compared to the token's bucket_id — a mismatch (bucket deleted and recreated between pages) returns an error and the client retries from scratch. See [bucket_cache.md — Listing and Bucket Identity](bucket_cache.md#9-listing-and-bucket-identity).

ListObjectsV1 uses a client-supplied marker (last key) — no room for server-side state. Bucket identity protection is not possible for V1.

All listing attributes come from the KV value — no data-tier access, no extended value access.

### With sharding

1. Resolve bucket: (tenant_id, bucket_name) → bucket_id (local cache).
2. Read `B` fresh (same as above — always fresh for LIST).
3. For each shard, issue a range scan on `<namespace><shard_count><shard_id><bucket_id>O` with the appropriate start marker.
4. Filter out fenced entries (delete markers in versioned buckets) in each shard's results.
5. Merge-sort across all shards to produce global lexicographic order.
6. Return up to 1000 keys.

Pagination stores per-shard markers in the continuation token.

During resharding, listing must merge streams from both old and new shard ranges. See [Listing During Resharding](Listing-and-Key-Scheme.md#listing-during-resharding) for the full protocol.

**Comparison to current model.**

In the RADOS model, the bucket-index is a single OMAP per shard that mixes all entry types in the default namespace:
- current version instances
- old version instances
- OLH entries
- delete marker instances
- pending entries.

Multipart upload entries are in a separate RADOS namespace (`RGW_OBJ_NS_MULTIPART`) and filtered at the OSD level — they do not add noise to object listing.

ListObjectsV2 must
- scan through all of these
- transfer them over the network from OSD to RGW
- and filter at the RGW to find only current live versions.

In the KV model:
- The category byte in the key (`O`, `V`, `M`, `C`) separates these into non-overlapping key ranges.
- ListObjectsV2 scans the `:O:` prefix — it physically never encounters `:V:` (old versions), `:M:` (multipart uploads), or `:C:` (children).
- The separation happens at the storage level, not by reading and discarding.

| Aspect | RADOS model | KV model |
|---|---|---|
| Entries scanned per object | OLH + all version instances + DMs + pending | 1 entry (`:O:` — current version only) |
| Noise in scan (versioned, 10 versions/obj) | ~90%+ entries are non-visible (OLH, old versions, pending) — all transferred over the network and discarded at RGW | Near zero — only fenced DMs to skip (single flag check) |
| Shard fan-out | Every listing page queries all shards (up to 1999), merge-sorted at RGW | `shard_count=1`: single sequential scan, no merge. Fleet: up to 128 shards |
| Requests per page | RGW must loop over multiple get-range requests — cannot predict how many useful entries a batch will contain due to non-visible noise. Each retry re-fetches from all shards. | Single range scan request — all `:O:` entries are valid. For versioned buckets, request ~10% extra to account for fenced DMs. |
| Network cost | All entries (including noise) sent from OSD to RGW over the network — filtering happens at the RGW, not at the OSD | Scan returns `:O:` entries directly — network transfer proportional to actual results |
| Cache pollution | All entries — current, old versions, OLH, pending — are pulled into RocksDB block cache during listing, evicting useful data and degrading performance for other operations on the same OSD | Range scans use a `NO-CACHE` flag — listing bypasses the cache entirely, leaving it undisturbed for point operations |

---

### ListObjectVersions

Lists all versions (including delete markers) for objects in a bucket.

1. Read `B` fresh (same as ListObjectsV2 — always fresh for LIST operations).
2. Range scan on `:O:` entries: `<namespace><shard_count><shard_id><bucket_id>O`.
3. Range scan on `:V:` entries: `<namespace><shard_count><shard_id><bucket_id>V`.
3. Merge by object_name — the `:O:` entry (current version) sorts first for each object_name, `:V:` entries follow in version_id order (latest first).
4. Return all versions and delete markers with their version_id.
5. Each entry includes its type (version or delete marker).

With sharding: fan out to all shards, merge across shards by object_name.

See [Versioned-Bucket-Operations.md](Versioned-Bucket-Operations.md) for the full merge protocol and worked examples.

**Comparison to current model.**

The same structural differences described in the [ListObjectsV2 comparison](#list-listobjectsv2) apply here:
noise from OLH/pending entries, shard fan-out, unpredictable batch sizes, network amplification, and cache pollution.

**Key distinction:** old versions and delete markers are **signal, not noise** — ListObjectVersions needs them. The noise ratio is therefore lower than ListObjectsV2, but OLH and pending entries remain unwanted overhead.

---

## Multipart Upload

Multipart upload state is co-located on the same shard as the target object. The shard is determined by `hash(bucket_id + object_name)` — the same hash used for the final object. This ensures all multipart operations are local transactions.

### InitiateMultipartUpload

AWS allows multiple concurrent multipart uploads for the same object key, even in non-versioned buckets. Each upload gets its own ref_tag (upload_id). Multiple `:M:` head entries (part_number=0) with different ref_tags can coexist for the same object_name. No check for existing `:O:` or other active uploads is needed — the final object state is determined by whichever `CompleteMultipartUpload` is processed last.

1. Generate a ref_tag (this serves as the upload_id).
2. Write head entry:
   - Key: `<namespace><shard_count><shard_id><bucket_id>M<object_name><ref_tag><part_number=0>`
   - Value: upload metadata (initiated timestamp, owner, requested attributes).
   - Single KV write — no transaction, no reads. Same pattern as PUT Phase 1.
   - Uses cached `bucket_id` to construct the key. Hard enforcement happens at CompleteMultipartUpload Phase 3 (`get(B)`).
3. Return ref_tag (encoded as opaque string) to client as upload_id.

The head entry's existence means the upload is active — no state field is needed.

### UploadPart

Bucket existence is soft-checked from cache at InitiateMultipartUpload. Hard enforcement happens at CompleteMultipartUpload Phase 3 (`get(B) → null → abort`). If the bucket is deleted mid-upload, parts are wasted work — cleaned up when CMU Phase 3 self-aborts or by the sweeper.

UploadPart uses a multi-phase flow. Phase 1 creates a pending `:M:` entry (the part's own KV) to register intent before writing data. Phase 3 updates the same `:M:` entry to committed state with the final metadata. The `:M:` entry serves as both coordination mechanism and final metadata store — no separate per-part `P` entries are needed.

**`:M:` part entry states:**

| State | Meaning | Sweeper action |
|---|---|---|
| `pending` | Phase 1 committed, data write in-flight. No prior data. | Blind-delete `ref_tag-part_number` from storage tier. Delete `:M:`. |
| `cleanup` | Re-upload detected, old data being freed. Old manifest in entry. | Free data via manifest. Blind-delete `ref_tag-part_number` (new upload may have started). Delete `:M:`. |
| `committed` | Data written, part complete. Manifest is valid. | Only cleaned up as part of abort/complete `G:M` processing. |

**Phase 1 — verify upload and register intent (single-shard transaction):**

1. In a single transaction:
   - Read the head entry (`part_number=0`) — verify the key exists (existence = upload active).
   - If head is missing → return `NoSuchUpload`. No data is written.
   - Read the `:M:` entry for this `part_number`:
     - **Does not exist** → write `:M:` entry with `state=pending` + timestamp. Commit. Proceed to Phase 2.
     - **Exists, `state=committed`** → re-upload. Set `state=cleanup` + new timestamp (old chunk pointer/manifest remains in the entry). Commit. Proceed to Phase 1a.
     - **Exists, `state=pending` or `state=cleanup`** → another operation on this part is in-flight. Return retryable error. **TBD:** failure handling — if a part remains in `pending` or `cleanup` state indefinitely (crashed uploader, sweeper delay), the system cannot reject new uploads forever. Need to define a timeout-based takeover or forced-cleanup mechanism so that a subsequent upload can eventually proceed.
   - Commit.

Concurrent uploads for **different** part numbers read and write different `:M:` keys — they do not contend. Concurrent uploads for the **same** part number read and write the same `:M:` key — FDB detects the conflict and one transaction retries, seeing the pending/cleanup state, and returns a retryable error.

**Phase 1a — free old data and transition to pending (re-upload only):**

This phase runs only when re-uploading a previously committed part. The uploader performs the cleanup itself; the sweeper only intervenes if the uploader crashes during this phase.

1. Free old storage-tier data (`ref_tag-part_number`) using the old chunk pointer/manifest read in Phase 1 (held in memory).
2. Write `:M:` entry with `state=pending` + cleared chunk pointer/manifest. Old data is gone, no stale references remain.
3. Proceed to Phase 2.

**Phase 2 — write data (no KV involvement):**

2. Write part data to the storage tier. Data is named deterministically: `ref_tag-part_number`.

**Phase 3 — commit part metadata (blind write):**

3. Overwrite the `:M:` entry:
   - Key: `<namespace><shard_count><shard_id><bucket_id>M<object_name><ref_tag><part_number>`
   - Value: `state=committed`, chunk pointer, etag, size.

Single KV put — overwrites the pending entry with committed state and final metadata. No read, no transaction.

**Crash recovery:**

- `state=pending`: process crashed between Phase 1 and Phase 3 (or between Phase 1a and Phase 3 for re-uploads). Sweeper blind-deletes `ref_tag-part_number` from storage tier, deletes the `:M:` entry.
- `state=cleanup`: process crashed during Phase 1a (re-upload). Sweeper frees storage-tier data using the manifest in the entry, blind-deletes `ref_tag-part_number` (in case new data was partially written), deletes the `:M:` entry.
- If the process crashes before Phase 1 commits, nothing was created — no cleanup needed.

Each part upload is independent. Parts can be uploaded in parallel, out of order. `part_number` ranges from 1 to 10,000.

**Cost per part (normal upload):** Phase 1 = 2 reads (head + `:M:` key) + 1 write, single transaction. Phase 3 = 1 blind write. Total: 2 reads, 2 writes.

**Cost per part (re-upload):** Phase 1 = 2 reads + 1 write (transaction). Phase 1a = 1 storage-tier delete + 1 KV write. Phase 3 = 1 blind write. Total: 2 reads, 3 writes + 1 storage-tier delete.

### CompleteMultipartUpload

Completion is a multi-phase operation. Reading up to 10,000 part KVs to build the manifest cannot happen inside a single transaction (too large a conflict set). Instead, the upload is fenced by absorbing the head entry into `P:M`, parts are read lock-free, and the final object is committed in a small transaction.

**Phase 1 — fence the upload (single-shard transaction):**

1. In a single transaction:
   - Read the head entry (`part_number=0`). If missing → abort.
   - Write `P:M` coordination entry with head metadata + client's part list (part numbers + etags) + timestamp + completion marker.
   - Delete the head `:M:` entry (fence).
   - Commit.

The head deletion fences the upload — subsequent UploadPart Phase 1 transactions fail immediately (head missing). The `P:M` entry is created here (not at Initiate) as the crash-recovery anchor for the completion operation. No `G:M` entry is created here — GC must not trigger before Phase 3 commits the final object.

<a id="upload-protection-model"></a>
**Upload protection model:** Exactly one of M: head OR P:M exists at any time (atomically swapped in this transaction). After bucket deletion, whichever exists self-destructs: CMU Phase 3 does `get(B) → null → abort`; the sweeper cleans stale P:M entries. See [Bucket-State-Management.md — Bucket Deletion Rules](Bucket-State-Management.md#6-bucket-deletion-rules).

**Phase 2 — build manifest (lock-free):**

2. `RangeScan(M:<object_name><ref_tag>)` — read all part entries. Safe because the fence prevents new Phase 1 transactions. In-flight UploadPart Phase 3 blind writes may still create or update `:M:` entries — but only for parts whose Phase 1 committed before the fence. The scan only considers entries with `state=committed`.
3. Validate against the client-supplied part list: all specified parts exist, etags match.
4. Assemble the final manifest from chunk pointers of all parts. Compute the composite etag, total size, and all requested attributes.

**Phase 3 — commit final object (single-shard transaction):**

5. In a single transaction:
   - `get(P:M)` — if null → abort (DeleteBucket or sweeper force-aborted the upload).
   - `get(B)` — if null → abort (bucket deleted). On FDB, `get(B)` creates a read conflict range that detects concurrent DeleteBucket. On TiKV, same best-effort guarantee as PUT Phase 3.
   - If the object key already exists as a live object → move old `:O:` entry to the `G` (GC) namespace with a stripped value. See [GC Namespace](#gc-namespace-and-background-cleanup).
   - Write the final object KV entry (`:O:`) with the assembled manifest.
   - Write a `G:M` directive with the client's part list and `ref_tag`. The background worker uses these to:
     - Parts in the client list → delete `:M:` KV only (data owned by the final object).
     - Parts NOT in the client list (unused, pending, or orphaned) → delete `:M:` KV + free storage-tier data.
   - Delete the `P:M` coordination entry.
   - Commit.
6. Return success with the final etag.

**Bucket-delete interaction:** On FDB, `get(B)` in Phase 3 creates a read conflict range — if DeleteBucket commits `delete(B)` concurrently, Phase 3 conflicts, retries, sees B null → abort. On TiKV, `get(B)` is a snapshot read with no conflict range — same residual risk as PUT Phase 3 (single-digit ms race window). See [Bucket-State-Management.md — TiKV Safety](Bucket-State-Management.md#tikv-safety).

**Crash recovery:** If the process crashes between Phase 1 and Phase 3, `P:M` remains with completion state. A background sweeper finds stale `P:M` entries with a completion marker, re-runs Phases 2 and 3 using the stored part list. If parts are invalid or missing, the sweeper rolls back: re-creates the head `:M:` entry from `P:M` data, then deletes `P:M`.

**Abandoned uploads (no Complete or Abort):** If InitiateMultipartUpload is never followed by Complete or Abort, the M: head persists indefinitely. The lifecycle engine (per-bucket `AbortIncompleteMultipartUpload` policy) scans M: heads for old timestamps and force-aborts them. After DeleteBucket, orphaned M: heads self-destruct when CMU Phase 3 does `get(B) → null → abort`, or are cleaned by the post-commit cleanup loop or lifecycle engine. See [Bucket-State-Management.md](Bucket-State-Management.md).

**Versioned buckets.** Phase 3 applies the same versioning displacement as PUT Phase 3: if the old O: has a real vid (≥ 2) → move to V: (preserved). If the old O: has NULL_ID (1) or NO_VERSION (0) → move to G:O (null-version entries never go to V:). The new O: entry gets versioning-appropriate vid and next_vid. See [Versioned-Bucket-Operations.md](Versioned-Bucket-Operations.md) and [Bucket-State-Management.md — Null-Version Rules](Bucket-State-Management.md#9-null-version-and-no_version-rules).

### AbortMultipartUpload

1. In a single transaction:
   - Read the head entry (`part_number=0`) — verify exists. If missing → abort (already aborted or completed).
   - Move the head entry to a `G:M` directive: delete from `:M:` key, write to `G` key with **DEEP** flag, head metadata, `ref_tag`, `object_name`.
   - Commit.
2. Return success.

The upload is immediately invisible — the head is gone, so `ListMultipartUploads`, `ListParts`, and all S3 multipart APIs cannot discover the parts. Subsequent UploadPart Phase 1 transactions fail (head missing). In-flight UploadPart Phase 3 blind writes may still create or update orphaned `:M:` entries — these are cleaned up by the G:M cleanup loop.

Note: no `P:M` exists at abort time — P:M is only created by CompleteMultipartUpload Phase 1. If a client calls Abort, the upload is in the active state (M: head exists, no P:M).

One `G:M` directive replaces up to 10,000 individual cleanup operations. The background worker processes the entry using the sweeper cleanup loop (see [Multipart Sweeper Cleanup Loop](#multipart-sweeper-cleanup-loop)).

### Multipart Sweeper Cleanup Loop

The sweeper processes `G:M` entries created by AbortMultipartUpload (DEEP — free all data) and by CompleteMultipartUpload (mixed — free unused parts only, delete KVs for included parts). The `G:M` entry cannot be deleted until all `:M:` entries for the upload are accounted for.

All parts are discoverable via range scan of `:M:` entries for the upload's `ref_tag`. No bitmap is needed — the `:M:` entry itself (with `state=pending`, `state=cleanup`, or `state=committed`) is the coordination record.

**DEEP cleanup (abort or lifecycle expiration):**

```
iteration = 0

loop:
    found = range_scan(:M: entries for this ref_tag)
    for each :M: found:
        if state == cleanup:
            free storage-tier data using manifest in entry
        free storage-tier data (ref_tag + part_number)
        delete :M: KV

    if found == 0:
        break                             # no more parts, done

    iteration += 1

    if iteration > max_retries:           # e.g., 3
        break                             # remaining in-flight uploads will
                                          # create :M: entries that are stale;
                                          # a periodic stale-:M: scan handles them

    wait(T)                               # e.g., 60 seconds — allow in-flight
                                          # Phase 3 blind writes to land

delete G:M entry
```

**Mixed cleanup (after CompleteMultipartUpload):**

Parts in the client's part list have their data owned by the final object — only the `:M:` KV is deleted. Parts NOT in the client list are unused or orphaned — both the `:M:` KV and storage-tier data are deleted.

```
client_parts = G:M.client_part_list
iteration = 0

loop:
    found = range_scan(:M: entries for this ref_tag)
    for each :M: found:
        if state == cleanup:
            free storage-tier data using manifest in entry
        if part_number in client_parts:
            delete :M: KV only            # data owned by final object
        else:
            free storage-tier data (ref_tag + part_number)
            delete :M: KV

    if found == 0:
        break

    iteration += 1

    if iteration > max_retries:
        break

    wait(T)

delete G:M entry
```

**Properties:**

- All parts are discovered via range scan — no bitmap required. Each `:M:` entry is its own coordination record.
- The loop converges because no new `:M:` entries can be created after the head is removed (UploadPart Phase 1 transactions fail on the missing head). In-flight Phase 3 blind writes (from uploads whose Phase 1 committed before the fence) may land during early iterations — subsequent scans catch them.
- Entries in `state=cleanup` require an extra step: free old storage-tier data using the manifest in the entry before the standard `ref_tag-part_number` cleanup. This handles re-uploads that crashed mid-cleanup.
- Pending `:M:` entries (from uploads that committed Phase 1 but crashed before Phase 3) are treated the same as committed entries during cleanup — their storage-tier data (`ref_tag-part_number`) is deleted unconditionally.
- The `G:M` entry is only deleted at the end, serving as a crash-recovery anchor throughout. If the sweeper crashes mid-loop, it restarts and re-scans. All deletes are idempotent.

### ListParts

1. Range scan on `:M:` entries for this object_name and ref_tag: `...M<object_name><ref_tag>`, skipping part_number=0 (upload metadata).
2. Return part metadata (part number, etag, size, last modified).

### ListMultipartUploads

1. Range scan on `:M:` entries across the bucket.
2. With sharding: fan out to all shards, merge-sort results.
3. Return active upload metadata (upload_id=ref_tag encoded as string, key, initiated timestamp, owner).

### Multipart — RADOS vs KV

**UploadPart comparison.**

| Step | RADOS model | KV model |
|---|---|---|
| 1. Verify upload | Read meta object (RADOS read, extra-data pool) | Phase 1 transaction: read head entry (`part_number=0`), verify key exists |
| 2. Record intent | `rgw_bucket_prepare_op` — log pending BI entry for this part (bucket-index OMAP) | Phase 1 transaction: read `:M:` entry for this part, create/overwrite with `state=pending` (same transaction as verify) |
| 3. Write data | Write part as one or more RADOS objects (`__multipart_<obj>.<upload_id>.<part_num>`) in the data pool. Parts larger than `rgw_obj_stripe_size` (4 MiB) generate additional `__shadow_` tail objects. On EC pools each object is written across K+M members. Each write subject to PG-level serialization. | Write data to storage tier. Data named deterministically: `ref_tag-part_number`. No PG serialization. |
| 4. Commit part metadata | Update meta object OMAP: key `part.<part_number>`, value = manifest + etag + size + mtime. Uses `assert_exists()` to detect concurrent abort, `cls_version_inc()` to detect race with CompleteMultipartUpload. | Blind KV put: overwrite `:M:` entry with `state=committed` + chunk pointer, etag, size. |
| 5. Finalize index | `rgw_bucket_complete_op` — finalize BI entry for this part | — (no bucket-index) |
| **Total** | 5 operations across 3 domains (data pool, meta object, bucket index) | 1 transaction (2 reads + 1 write) + 1 data write + 1 blind KV put = 3 operations across 2 domains |

**Key differences — UploadPart:**

- **Crash recovery.** RADOS has no coordination entry for in-flight part uploads. Orphaned data objects from crashes are discoverable only by `rgw-orphan-list` (full pool scan). The KV model's pending `:M:` entry is the coordination record — the sweeper finds stale pending entries and cleans up deterministically.
- **Graceful failure cleanup.** When the RGW process is alive and the meta OMAP update fails (e.g., `-ENOENT` from concurrent abort), the `RadosWriter` destructor deletes written data objects — no orphan leak. However, this destructor-based cleanup has been a source of race-condition bugs: incorrect head-object tracking (#63642), deletion ordering (#11749), data corruption on ETIMEDOUT (#47667). The KV model uses optimistic blind writes — if the upload was aborted, the `:M:` entry is orphaned and cleaned up by the sweeper loop.
- **Race with Complete.** RADOS uses `cls_version_inc()` on each part upload and `cls_version_check()` during Complete's meta-object deletion. If a late UploadPart races in, Complete's delete fails with `ECANCELED`, triggering a retry loop (up to 15 retries). This mechanism was added as a bug fix (PR #58082). The KV model: Complete deletes the head `:M:` entry (fence) and absorbs its metadata into `P:M`. Late Phase 3 blind writes are cleaned up by the sweeper — no retry loop.
- **Part re-upload.** When the same part number is uploaded again, RADOS does not clean up the old part's data objects — the meta OMAP is overwritten, but old data objects are orphaned (bugs #16767, #44660, #57942). The KV model detects re-upload in Phase 1 (`:M:` exists with `state=committed`), transitions to `state=cleanup` (old manifest stays in the entry), frees old storage-tier data, then transitions to `state=pending`. If the uploader crashes during cleanup, the sweeper finishes using the manifest in the `cleanup` entry.
- **Same-part serialization.** RADOS has no mechanism to prevent concurrent uploads of the same part number — both write to overlapping RADOS objects, risking data corruption. The KV model serializes same-part uploads via FDB transaction conflicts: two Phase 1 transactions reading and writing the same `:M:` key conflict, and one retries, seeing `state=pending` or `state=cleanup`, returning a retryable error.
- **Dual bookkeeping.** RADOS maintains part metadata in both a standalone meta object (OMAP) and the bucket index (per-part BI entries). The KV model has a single `:M:` entry per part.

**Cleanup comparison.**

| Scenario | RADOS model | KV model |
|---|---|---|
| **CompleteMultipartUpload** | Parts are tail objects of the head. Completion rewrites the head and chains part manifests. Old head (if overwrite) goes to the GC log. Parts are chained in the manifest — removal depends on GC scanning the manifest chain. | Three-phase flow: fence upload (delete head `:M:`, absorb into `P:M`), lock-free scan of committed parts to build manifest, small transaction to write final `:O:` and create `G:M` MIXED directive with client part list, delete `P:M`. Sweeper cleanup loop deletes part KVs; unused parts' storage-tier data is freed. |
| **AbortMultipartUpload** | Abort reads meta OMAP to find known parts, chains them for GC, then deletes the meta object. Parts not recorded in the OMAP (in-flight or crashed) are invisible to abort and become orphans. `RadosWriter` destructor cleans up in-flight parts best-effort. | Abort deletes head `:M:`, creates `G:M` DEEP directive, deletes `P:M` — all in one transaction. Upload is immediately invisible. Sweeper cleanup loop range-scans all `:M:` entries (pending, cleanup, and committed), frees data, deletes KVs. Multi-pass loop catches late blind writes. |
| **Lifecycle (incomplete upload expiration)** | LC scans the multipart index (`bucket.meta` OMAP) for uploads older than the configured threshold. For each expired upload, issues the same abort flow — delete the upload entry, then individually delete each part object. Crashes during cleanup leave orphaned parts. | LC scans `part_number=0` entries for uploads older than the threshold. For each expired upload, deletes head `:M:`, creates `G:M` DEEP directive, deletes `P:M` (same as abort). Sweeper cleanup loop handles the rest. |
| **Unused-part cleanup** | On completion, parts not in the final manifest may leak — GC only processes parts chained in the head's manifest. | `G:M` MIXED directive carries client part list. Sweeper distinguishes included parts (delete KV only) from unused parts (delete KV + free data). No leak. |

**Key differences — cleanup:**

- **Orphan coverage.** RADOS abort only discovers parts recorded in the meta OMAP. The KV model discovers all parts via range scan of `:M:` entries — both pending and committed — including parts from in-flight uploads that land after abort.
- **No separate multipart index.** RADOS maintains a per-bucket multipart index (OMAP on the bucket meta object). The KV model uses the same key space (`:M:` entries sorted alongside `:O:` and `:V:`), eliminating a separate metadata structure.
- **Directive efficiency.** One `G:M` entry handles an entire upload's cleanup via range scan. RADOS needs one GC log entry per part object, or relies on `bucket check` / `rgw-orphan-list` to discover orphans.
- **LC simplicity.** LC scans `part_number=0` entries directly — no separate multipart index to query. The timestamp is always in the head entry value.

---

## Object Tagging

Object tags are stored inline in the `:O:` value when they are short and space permits. When they exceed the inline budget, they spill to a standalone child KV entry co-located on the same shard as the parent object:

- Key: `<namespace>...<bucket_id>C<ref_tag>T`
- Value: all tags encoded together in a single entry (array of up to 10 key-value pairs).

The `:O:` value stores a `tag_count` field (uint8, max 10) and an `external` flag indicating whether tags are inline or in the child KV.

AWS limits: maximum 10 tags per object, aggregated size up to 5,120 bytes.

### PutObjectTagging

S3 `PutObjectTagging` replaces the entire tag set. The storage model (inline vs external) is determined by whether the new tag set exceeds the inline size budget. All cases write O: (with updated `tag_count`) — provides write-write conflict detection on both FDB and TiKV.

```
txn {
  1. get(O:) → if null or fenced DM → 404; read ref_tag, current tag state

  Case A — Current: inline or none, New: fits inline
    2. put(O:, value_with_new_inline_tags + tag_count = new_count)

  Case B — Current: inline or none, New: exceeds threshold
    2. put(O:, clear_inline_tags + set_external_flag + tag_count = new_count)
    3. put(C:<ref_tag>T, new_tag_array)

  Case C — Current: external, New: exceeds threshold
    2. put(C:<ref_tag>T, new_tag_array)
    3. put(O:, tag_count = new_count)

  Case D — Current: external, New: fits inline
    2. delete(C:<ref_tag>T)
    3. put(O:, clear_external_flag + set_inline_tags + tag_count = new_count)

  commit
}
```

Single transaction. No P: coordination needed (no storage-tier write). See [child-kv-operations.md](child-kv-operations.md) for full analysis.

### GetObjectTagging

```
txn {
  1. get(O:) → if null or fenced DM → 404; read ref_tag, tag state
  2. If inline → return tags from O: value
  3. If external → get(C:<ref_tag>T) → return tag array
}
```

Read-only transaction — single snapshot, safe on both FDB and TiKV.

### DeleteObjectTagging

S3 `DeleteObjectTagging` removes all tags regardless of storage model:

```
txn {
  1. get(O:) → if null or fenced DM → 404; read ref_tag, tag state
  2. If external: delete(C:<ref_tag>T)
  3. put(O:, clear_all_tag_fields + tag_count = 0)
  commit
}
```

Always writes O: — provides write-write conflict on both FDB and TiKV.

---

## Object ACL

Object ACLs control per-object access overrides. S3 supports up to 100 grants per object.

ACLs are stored inline in the parent KV value when they fit. If the ACL is large enough to push the value beyond the size budget, it spills to the extended value.

ACLs are overwritten as a whole (PutObjectAcl replaces the entire ACL), so there is no benefit to storing them in a separate child KV.

### PutObjectAcl

1. Read the parent KV entry.
2. If not found or fenced delete marker → return 404.
3. Update the ACL field in the value.
4. Write the updated KV entry.

Single read-modify-write transaction.

### GetObjectAcl

1. Read the parent KV entry.
2. If not found or fenced delete marker → return 404.
3. Extract and return the ACL from the value.

Single KV read. If the ACL spilled to the extended value, read it from there.

---

## Bucket Delete (Optional Vendor Extension)

> **Note:** AWS S3 does not provide a bulk bucket-delete API. The standard S3 flow requires deleting all objects individually (via DeleteObject or lifecycle expiration) before calling DeleteBucket on an empty bucket. Each individual object delete follows the normal DELETE path and moves entries to `G` with child-presence flags intact.
>
> The batch flow below is an **optional vendor-specific optimized API** that may never be implemented. It is documented here for completeness.

Bulk bucket delete removes all objects and metadata for a bucket in batches — KV stores limit transaction size and duration.

1. Mark the bucket as deleted in bucket metadata (local cache returns 404 for all subsequent requests).
2. Process objects in batches:
   a. Range-read the next N entries (~1000) across all categories for this bucket (`...O`, `...V`, `...M`, `...C`), plus any existing `G` entries for this bucket.
   b. For each entry with data (live `:O:`, `:V:` versions, `:M:` parts): move to the `G` namespace with stripped values for async data cleanup. The stripped value is stripped of child-presence pointers (annotations, tags, extended value) — children are handled separately in steps c–d. The background worker processing this `G` entry only frees the parent's own storage-tier data.
   c. For `:C:` annotation entries with storage-tier data: move to `G:A` with a secondary ref_tag for async data cleanup. The `G:A` key uses a fixed 39-byte format (see [GC Namespace](#gc-namespace-and-background-cleanup)).
   d. Other `:C:` entries (tags, extended value KV) — delete directly (no storage-tier data to free).
   e. Delete all remaining entries from the `S` namespace.
3. Repeat until all category ranges for this bucket are empty in both `S` and `G` namespaces.
4. Remove bucket metadata.

Background workers drain `G` entries and free storage-tier data. See [GC Namespace](#gc-namespace-and-background-cleanup).

Without sharding, the bucket's objects form a contiguous range — each batch is a simple range-read followed by a range-delete.

With sharding, each shard must be processed independently.

---

<a id="delete-all-versions-optional-vendor-extension"></a>
## Delete All Versions (Optional Vendor Extension)

> **EXPERIMENTAL — NOT SAFE FOR IMPLEMENTATION.**
>
> This section describes a vendor-unique extension that is currently broken. The G:F protocol violates key design invariants:
>
> - **Breaks DeleteBucket invariant:** G:F deletes O: but leaves V: entries behind for async cleanup. DeleteBucket requires both :O: and :V: empty — a bucket with pending G:F cleanup cannot be deleted until the background worker completes.
> - **Data loss risk:** The G:F background worker scans ALL V: entries for the object_name and destroys them — including versions written by new PUTs that occurred AFTER the G:F directive was created. No temporal boundary distinguishes old from new V: entries.
>
> **Possible fix (future):** Before processing data, the G:F worker could first move ALL existing V: entries to G: in a single transaction (creating a point-in-time boundary). Only entries moved to G: are processed. New V: entries written after this move are safe — they won't be found in G:. This adds a "fence V: → G:" step before the async cleanup, similar to how CompleteMultipartUpload fences with head deletion.
>
> Until this fix is designed and verified, G:F should not be implemented.

---

> **Note:** AWS S3 does not provide a single API call to delete an object along with all its versions. The standard S3 flow requires calling DeleteObject with each version-id individually. This vendor-unique extension is optional and may never be implemented.

Deletes a specific object and all its versions, parts, and children in a single logical operation.

1. Read the `:O:` entry for the target object to obtain its `ref_tag`.
2. Write a single `G:F` directive to the `G` namespace. The `G:F` key contains the `object_name` and the `ref_tag` from the `:O:` entry.
3. Delete the `:O:` entry.
4. Return success — the object is immediately invisible. All version data is cleaned up asynchronously by background workers.

The `G:F` directive's `size_tier` can be set to 0 (unknown) or estimated from the `:O:` entry metadata.

Background cleanup follows the `G:F` protocol described in [Background Cleanup Protocol](#background-cleanup-protocol): the worker scans all `:O:` and `:V:` entries for the object_name, processes each version (freeing data, cleaning up annotations and children), and removes all entries.

---

## Lifecycle Expiration

Lifecycle expiration follows the same batched pattern as bucket delete, but filters objects by expiration criteria.

The lifecycle engine reads bucket state (`get(B)`) at the start of each bucket's processing pass to determine the current versioning state. Versioning state determines whether expiration creates delete markers (versioned/suspended) or moves entries directly to `G` (unversioned). See [bucket_cache.md — Section 10](bucket_cache.md#10-which-operations-use-cached-vs-fresh-bucket-state).

### Non-versioned buckets

1. Range scan `:O:` entries for this bucket (`<namespace><shard_count><shard_id><bucket_id>O`).
2. For each entry, evaluate lifecycle rules against the KV value (last_modified, tags, storage_class, size).
3. For qualifying entries:
   - Move the `:O:` entry to `G` with a stripped value. See [GC Namespace](#gc-namespace-and-background-cleanup).

### Versioned buckets

1. Range scan `:O:` and `:V:` entries for this bucket, grouping entries by object name.
2. For each object, evaluate version-aware rules:
   - **Expiration** — applies to the current version. Creates a delete marker (same as a versioned DELETE without version-id).
   - **NoncurrentVersionExpiration** — applies to non-current versions older than N days. Moves specific `:V:` entries to `G` with stripped values. See [GC Namespace](#gc-namespace-and-background-cleanup).
   - **ExpiredObjectDeleteMarker** — removes delete markers that are the only remaining version. Simply deletes the entry (no data to clean up).
3. Process in batches within transaction limits.

---

<a id="gc-namespace-and-background-cleanup"></a>
## GC Namespace and Background Cleanup

When an object is deleted or overwritten, its KV entry is moved from the `S` namespace to the `G` (GC) namespace. The `G` entry IS the cleanup record — no separate delete-log is needed.

### G Namespace Key Format

The `G` namespace uses a 15-byte header — one byte longer than `S`/`B`/`Z` keys due to the `size_tier` field:

```
G <size_tier 1B> <shard_count 2B> <shard_id 2B> <bucket_id 8B> <cat 1B> <body>
```

| Field | Size | Encoding | Description |
|---|---|---|---|
| namespace | 1 B | ASCII `G` | GC namespace |
| size_tier | 1 B | uint8 | Logarithmic object size tier (0–34) |
| shard_count | 2 B | uint16 BE | Same shard_count as the original entry |
| shard_id | 2 B | uint16 BE | Same shard_id as the original entry |
| bucket_id | 8 B | binary | Same bucket_id as the original entry |
| cat | 1 B | ASCII | Category: `O` (object), `V` (version), `U` (multipart part), `A` (annotation), `S` (aborted before KV commit), `M` (multipart directive), `F` (full delete directive) |
| body | variable | — | Category-specific fields (see tables below) |

**size_tier formula:** `size_tier = clamp(floor(log2(object_size_bytes)) - 10, 0, 34)`

| size_tier | Tier threshold | Label |
|---|---|---|
| 0 | 2^10 = 1 KB | tiny |
| 6 | 2^16 = 64 KB | small |
| 12 | 2^22 = 4 MB | medium |
| 16 | 2^26 = 64 MB | large |
| 20 | 2^30 = 1 GB | very large |
| 34 | 2^44 = 16 TB | max object |

Objects smaller than 1 KB use `size_tier = 0`.

**"Move to G"** throughout this document means: within a single transaction, write a stripped copy of the entry to the `G` namespace and delete the original from `S`. The `G` key contains the entry's `ref_tag` (unique instance identifier) instead of the `object_name` — the object name is not needed for cleanup and is not preserved in `G`. The value is stripped to cleanup-only fields (ref_tag is omitted from the stripped value since it is already in the key).

The `size_tier` field sorts GC entries by object size — small objects first, large objects last. This enables different cleanup strategies per tier (see [Background Cleanup](#background-cleanup-protocol) below).

**G key bodies — per-instance (fixed-length):**

| Category | Body | Total key size |
|---|---|---|
| `G:O` | `<ref_tag 12B>` | 27 B |
| `G:V` | `<ref_tag 12B><version_id 4B>` | 31 B |
| `G:U` | `<ref_tag 12B><part_number 2B>` | 29 B |
| `G:A` | `<ref_tag 12B><secondary_ref_tag 12B>` | 39 B |
| `G:S` | `<ref_tag 12B>` | 27 B |

Each version of an object has its own unique `ref_tag`, so `G:O` and `G:V` keys are naturally unique. `G:U` keys are unique by `ref_tag + part_number` — used for individual multipart part cleanup during re-uploads. `G:A` keys use a `secondary_ref_tag` (generated by RGW using the same `rgw_id + seq_id` rules) because annotations don't have their own ref_tag.

**G key bodies — directives (variable-length):**

| Category | Body | Total key size |
|---|---|---|
| `G:M` | `<object_name 1–1024B><ref_tag 12B>` | 28–1051 B |
| `G:F` | `<object_name 1–1024B><ref_tag 12B>` | 28–1051 B |

Category `M` (multipart) is a cleanup directive for multipart uploads — instead of representing a single deleted entry, it tells the background worker to scan a range in the `S` namespace and clean up all matching entries. The `G:M` key contains the `object_name` (needed to construct the scan prefix) and the `ref_tag` (upload_id). One directive replaces up to 10,000 individual `G:U` entries. The `size_tier` reflects the total data size of the upload (estimated from the head entry metadata, or 0 if unknown). The `G:M` value contains a **mode flag**:

- **MIXED** (after `CompleteMultipartUpload`): parts in the client list → delete `:M:` KV only (data owned by the final object). Parts NOT in the client list → delete `:M:` KV + free storage-tier data. The `G:M` value carries the client's part list for this distinction.
- **DEEP** (after `AbortMultipartUpload`): free storage-tier data for each part, then delete part KVs. Worker processes using the sweeper cleanup loop (see [Multipart Sweeper Cleanup Loop](#multipart-sweeper-cleanup-loop)).

Category `F` (full) is a cleanup directive for the vendor-unique delete-all-versions operation (see [Delete All Versions](#delete-all-versions-optional-vendor-extension)). The `G:F` key contains the `object_name` and the `ref_tag` (from the `:O:` entry at deletion time). The worker scans `S:O:<object_name>` and `S:V:<object_name>*` to find all versions, reads each version's ref_tag, frees data, cleans up children, and deletes all entries.

For `G:A`, the `size_tier` reflects the annotation data size (not the parent object size). The `ref_tag` is the parent object's ref_tag (retained for tracing). The value is stripped to just the annotation's chunk pointers.

**No object_name in per-instance G keys.** For `G:O`, `G:V`, `G:U`, `G:A` — background cleanup uses chunk pointers (from the stripped value) and ref_tag (from the key) to free data and remove children. The object_name is not needed. `G:M` and `G:F` (directives) are the exceptions — they carry the object_name because the worker needs it to construct scan prefixes in the `S` namespace.

**Header parsing note.** The parser must branch on the namespace byte before parsing the rest of the key. `G` keys have a 15-byte header (extra `size_tier` at offset 1); `S`/`B`/`Z` keys have a 14-byte header.

### GC Value (Stripped)

When moving an entry to `G`, the value is stripped to contain only information needed for cleanup:

**`G:O`, `G:V`, `G:U` entries (parent objects and multipart parts):**

- **Chunk pointers** — `(blob_id, offset, length)` tuples identifying storage-tier data to free.
- **Manifest** — if present (large multipart), needed to locate all data chunks.
- **Child-presence flags** — bitmask indicating which child types exist:
  - External tags (`T`)
  - Extended value (`E`)
  - Annotations (`A`) — including count and whether annotations have their own data chunks.

The ref_tag is not stored in the value — it is already in the `G` key and can be read directly from there for child cleanup.

**`G:A` entries (individual annotations):**

- **Chunk pointers** — `(blob_id, offset, length)` tuples for the annotation's storage-tier data.

No ref_tag (in the key), no manifest (annotations are single-chunk), no child-presence flags (annotations have no children).

**`G:S` entries (aborted before KV commit):**

- No value needed — the ref_tag in the key is sufficient to locate and free storage-tier data. No KV cleanup required (no O:, V:, or C: entries were ever created).

All user-visible attributes are discarded: ACLs, inline tags, user metadata (x-amz-meta-*), listing attributes, object lock settings.

### What Moves to G

Every operation that orphans data moves the old entry to `G`:

- **DELETE (non-versioned)** — move `:O:` to `G`.
- **PUT overwrite (non-versioned)** — move old `:O:` to `G`, write new `:O:`.
- **CompleteMultipartUpload overwrite** — move old `:O:` to `G`. Write `G:M` MIXED directive (background worker deletes part KVs; unused parts' data is freed).
- **DELETE with version-id** — move target `:V:` (or `:O:` with promotion) to `G`.
- **AbortMultipartUpload** — delete the head entry (`part_number=0`), delete `P:M`, write a `G:M` DEEP directive. Background worker frees storage-tier data and deletes part KVs.
- **UploadPart re-upload** — old part data is freed by the uploader; if the uploader crashes mid-cleanup, a `G:U` entry is used by the sweeper to finish freeing old part data.
- **Bucket delete** — move all data-bearing entries to `G` in batches.
- **Lifecycle expiration** — move qualifying entries to `G`.
- **DeleteObjectAnnotation** — if the annotation has storage-tier data chunks, move to `G:A` with a secondary ref_tag (fixed 39-byte key, value stripped to chunk pointers). The original `:C:` annotation entry is deleted atomically in the same transaction.
- **PutObjectAnnotation overwrite** — if the old annotation has storage-tier data chunks, move to `G:A` atomically in the same transaction as writing the new annotation entry. The write itself is coordinated through a `P:A` entry (see [Pending Operations Namespace](#pending-operations-namespace)).

### What Creates G:S (abort / crash cleanup)

G:S entries are not created by "moving" an existing entry — they are written fresh when an operation aborts after storage-tier data was written but before any KV entry (O:, V:, C:) was committed:

- **PUT Phase 3 abort** (bucket deleted, bucket_id mismatch, policy denied, quota exceeded) — `put(G:S, {ref_tag}); delete(P:O)` in the same transaction.
- **Sweeper cleanup of stale P:O** — process crashed between Phase 1 and Phase 3. Sweeper writes `put(G:S, {ref_tag}); delete(P:O)`.
- **Post-commit DeleteBucket cleanup** — best-effort loop scans P: entries for the deleted bucket and moves each to G:S.

The GC worker processing G:S entries only frees storage-tier data by ref_tag — no child scan, no manifest walk, no KV cleanup. See [Bucket-State-Management.md — Abort and Cleanup](Bucket-State-Management.md#11-abort-and-cleanup).

### What Does NOT Move to G

- **DELETE without version-id (versioned bucket)** — creates a delete marker. No data is orphaned. The old `:O:` entry moves to `:V:`, not to `G` — unless the old O: has NULL_ID or NO_VERSION, in which case it moves to `G:O` per the [null-version rule](Bucket-State-Management.md#9-null-version-and-no_version-rules).

### Atomicity

The move to `G` is in the same transaction as the KV mutation — the `:O:` (or `:V:`) delete and the `G` write are atomic. No orphan window.

The `G` entry retains the same `shard_count` and `shard_id` as the original — it is co-located on the same shard and can share a transaction.

<a id="background-cleanup-protocol"></a>
### Background Cleanup Protocol

Background workers scan the `G` namespace and free storage-tier data:

**For `G:O`, `G:V`, `G:M` entries (parent objects):**

1. **Scan `G` entries.** Workers can scan globally, per-tier (specific `size_tier` range), or per-tier-per-shard.
2. **Free storage-tier data.** Read chunk pointers from the stripped value. Delete blobs or punch holes.
3. **Handle annotation data.** If annotations exist (child-presence flag set): extract ref_tag from the `G` key, then `RangeScan(...C<ref_tag>A)` in the `S` namespace to read annotation chunk pointers and free annotation data.
4. **Remove child KV entries.** Extract ref_tag from the `G` key, then `RangeDelete(...C<ref_tag>)` in the `S` namespace — removes all children (tags, annotations, extended value) in a single operation.
5. **Remove the `G` entry.**

**For `G:A` entries (individual annotations):**

1. **Scan `G:A` entries.** Same scan mechanisms as above — `G:A` entries are interleaved with other `G` entries, sorted by `size_tier`.
2. **Free annotation data.** Read chunk pointers from the stripped value. Delete blobs.
3. **Remove the `G:A` entry.**

`G:A` cleanup is simpler — no child scan, no manifest walk. The annotation's data chunks are identified directly from the stripped value.

**For `G:S` entries (aborted before KV commit):**

1. **Scan `G:S` entries.** Same scan mechanisms as above.
2. **Free storage-tier data.** Use ref_tag from the key to locate and delete blobs (deterministic naming). No chunk pointers needed.
3. **Remove the `G:S` entry.**

`G:S` cleanup is the simplest — no child scan, no manifest walk, no KV cleanup. Only storage-tier data is freed.

**For `G:M` entries (multipart directives):**

The `G:M` value contains a **mode flag** (MIXED or DEEP) and a **cursor** (`last_completed_position`) for crash recovery.

**MIXED mode** (after `CompleteMultipartUpload` — included parts' data is owned by the final object):

The `G:M` value carries the client's part list. The sweeper uses the multipart sweeper cleanup loop (see [Multipart Sweeper Cleanup Loop](#multipart-sweeper-cleanup-loop)) with MIXED semantics: parts in the client list have their `:M:` KV deleted (data owned by final object); parts NOT in the list have both `:M:` KV deleted and storage-tier data freed.

**DEEP mode** (after `AbortMultipartUpload` — all data is orphaned):

The sweeper uses the multipart sweeper cleanup loop (see [Multipart Sweeper Cleanup Loop](#multipart-sweeper-cleanup-loop)) with DEEP semantics: all `:M:` entries are deleted and all storage-tier data is freed.

On crash, the worker reads the `G:M` entry and re-runs the cleanup loop from the beginning. All deletes are idempotent — `ref_tag` verification on the storage tier prevents double-free (see [Storage-Tier-Requirements.md](Storage-Tier-Requirements.md)).

**For `G:F` entries (full object delete directives):**

1. **Scan `G:F` entries.** Same scan mechanisms as above.
2. **Extract object_name and ref_tag from the key.** `ref_tag = body[-12:]`, `object_name = body[:-12]`.
3. **Scan all versions.** `RangeScan(S:O:<object_name>)` and `RangeScan(S:V:<object_name>)` — reads each version's ref_tag and chunk pointers.
4. **For each version found:**
   a. Free storage-tier data (blobs, manifests).
   b. Scan and free annotation data: `RangeScan(...C<version_ref_tag>A)`.
   c. Remove all children: `RangeDelete(...C<version_ref_tag>)`.
5. **Remove all `:O:` and `:V:` entries** for this object_name.
6. **Remove the `G:F` entry.**

### Cleanup Strategies by Tier

The `size_tier` field enables differentiated cleanup strategies:

- **Forward scan** (ascending `size_tier`): batch-process many small objects efficiently. Small objects are most numerous and can be deleted in bulk with minimal I/O per entry.
- **Reverse scan** (descending `size_tier`): prioritize freeing large objects first. Each large object reclaims significant storage capacity.
- **Per-tier workers**: dedicated threads or processes for different tier ranges, each with tier-appropriate batching, throttling, and I/O policies.

### Design Properties

- **No separate log structure.** The moved KV entry IS the cleanup record. No log serialization, no log compaction, no log overflow.
- **Shard locality by construction.** `G` entries land on the correct shard automatically — no additional sharding logic.
- **No contention.** Entries are distributed across shards naturally. No hot log key.
- **No bounded queue / ENOSPC.** The `G` namespace grows and shrinks with the KV store — no fixed-size log to overflow.
- **Per-shard independence.** Background workers can drain shards independently, enabling parallel cleanup across the fleet.
- **Resharding.** During resharding (shard_count X → Y), `G` entries may exist with both old and new shard_counts. Background workers must scan both old and new shard_count ranges for `G` entries, similar to how listing merges old+new shard ranges for `S` entries. See [Listing During Resharding](Listing-and-Key-Scheme.md#listing-during-resharding).

### Transaction Patterns

KV and storage-tier operations cannot be made atomic together. The following patterns keep transactions small while ensuring correctness.

**Optimistic delete (single-phase operations).** For DELETE, PUT overwrite, and other operations that move a single entry to `G`:

1. Read K outside the transaction (lock-free). Parse the value, extract ref_tag, chunk pointers, child-presence flags.
2. Build the `G` key (K2) from ref_tag and size_tier. Build the stripped value (V2).
3. Start a small transaction:
   - Read K and verify `ref_tag` matches the value read in step 1. If K is gone or ref_tag changed → abort (another process already handled it).
   - `Put(K2, V2)`.
   - `Delete(K)`.
   - Commit.

The ref_tag acts as a version stamp — if K was modified between the lock-free read and the transaction, the ref_tag won't match and the transaction aborts. All expensive processing (parsing, building V2) happens outside the transaction.

**Storage-tier idempotency.** Every data chunk on the storage tier embeds the `ref_tag` (and `part_number` for multipart parts) as metadata. Before freeing a chunk, the GC worker verifies that the stored ref_tag matches the expected value. If the storage tier supports conditional delete ("delete only if ref_tag matches"), it should be used. This makes all GC deletions idempotent — safe to retry after a crash at any point. See [Storage-Tier-Requirements.md](Storage-Tier-Requirements.md).

**Packed small objects.** For objects packed into shared Packed Objects on the EC tier, "freeing data" means sending a Logical-Punch-Hole (extent map update) rather than a physical delete. The ref_tag is included in the punch-hole request for ownership verification. See [Storage-Tier-Requirements.md](Storage-Tier-Requirements.md) and [ec-obj-packing-design.md](ec-obj-packing-design.md).

---

<a id="pending-operations-namespace"></a>
## Pending Operations Namespace (`P`)

Every operation that writes storage-tier data uses the `P` namespace for crash recovery. A coordination entry records the operation's intent before data is written. If the process crashes mid-operation, a background sweeper finds stale `P` entries and cleans up orphaned data.

### P Namespace Key Format

The `P` namespace uses the same 14-byte header as `S`/`B`/`Z`:

```
P <shard_count 2B> <shard_id 2B> <bucket_id 8B> <op_type 1B> <body>
```

| Field | Size | Encoding | Description |
|---|---|---|---|
| namespace | 1 B | ASCII `P` | Pending operations namespace |
| shard_count | 2 B | uint16 BE | Same as the target object's shard_count |
| shard_id | 2 B | uint16 BE | Same as the target object's shard_id |
| bucket_id | 8 B | binary | Same as the target object's bucket_id |
| op_type | 1 B | ASCII | Operation type: `O`, `A`, or `M` |
| body | variable | — | Op-type-specific fields (see below) |

**Key bodies by op_type:**

| op_type | Body | Total key size |
|---|---|---|
| `O` (PutObject) | `<object_name 1–1024B><ref_tag 12B>` | 27–1049 B |
| `A` (PutObjectAnnotation) | `<object_name 1–1024B><ref_tag 12B>` | 27–1049 B |
| `M` (MultipartUpload) | `<object_name 1–1024B><ref_tag 12B>` | 27–1049 B |

**Parsing:** For `P:O`, `P:A`, `P:M`: `ref_tag = body[-12:]`, `object_name = body[:-12]`.

The `P` entry inherits `shard_count` and `shard_id` from the target object — both keys hash to the same shard, so writing the `P` entry alongside the Phase 1 mutation is a single-shard transaction. The `shard_count` and `shard_id` fields at the start of the key enable per-shard prefix scans (`RangeScan(P + shard_count + shard_id)`), used by the resharding drain protocol to verify all coordination entries have completed before starting BG migration. See [Listing-During-Online-Migration.md](Listing-During-Online-Migration.md#resharding-impact-on-write-operations).

### Operation Types

| op_type | Operation | Value payload |
|---|---|---|
| `O` | PutObject | Estimated size, content-type hint |
| `A` | PutObjectAnnotation | Parent ref_tag, annotation name |
| `M` | MultipartUpload | Upload metadata (timestamp, owner, requested attributes); updated with completion state and client's part list during CompleteMultipartUpload Phase 1 |

`P:M` is the crash-recovery anchor for multipart completion: created in CompleteMultipartUpload Phase 1 (absorbs head `:M:` metadata + client's part list + completion marker), deleted in Phase 3. During active upload, only the M: head exists — no P:M. The [upload protection model](#upload-protection-model) ensures exactly one of M: head or P:M exists at any time. After bucket deletion, whichever exists self-destructs: CMU Phase 3 does `get(B) → null → abort`; the sweeper cleans stale P:M entries. Individual part uploads do not create `P` entries — each part's `:M:` entry (with `state=pending`/`cleanup`/`committed`) serves as its own coordination record.

Future multi-phase operations (server-side copy, dedup) will add their own op_types.

<a id="sweeper-protocol"></a>
### Sweeper Protocol

A background sweeper periodically scans the `P` namespace for entries older than a configurable threshold (e.g., 5 minutes):

**For `P:O`, `P:A` entries (cleanup-only):**

The sweeper moves stale entries to the `G` (GC) namespace atomically, then a separate GC worker frees storage-tier data:

```
txn {
  1. get(P:O) → if null → skip (already handled)
  2. put(G:S, {ref_tag})
  3. delete(P:O)
  commit
}
```

The atomic move ensures PUT Phase 3 will see `P:O` as null and abort — preventing writes of `:O:` pointing to data that will be freed. The GC worker later reads `G:S` entries and frees storage-tier data by ref_tag (idempotent delete). See [Bucket-State-Management.md](Bucket-State-Management.md) for the race-condition analysis between the sweeper and PUT Phase 3.

**Storage-tier blob naming and sweeper cleanup.**

Blob naming is deterministic from the ref_tag:

- **Single blob (common case):** blob name = `ref_tag`. One object, one blob.
- **Striped large object:** blob names = `ref_tag + stripe_index`. The sweeper iterates stripe indices (0, 1, 2, ...) issuing deletes until the storage tier returns "not found."

Because blob names are derived from the ref_tag, the sweeper can always locate and delete orphaned data — no chunk pointers needed. Each blob on the storage tier embeds the ref_tag in its metadata, enabling conditional delete (verify ref_tag matches before freeing — idempotent, safe to retry).

**Small-object packing (deferred).** When small objects are packed into shared blobs, the blob_id is not derivable from ref_tag. This requires a separate middleware packing layer — small objects are staged (using ref_tag as identifier) in NVRAM or a logging-FS, then committed as a batch to the storage tier. After the batch commit, the KV is updated with the final `(blob_id, offset, length)`. The packing layer has its own crash recovery semantics and is designed separately.

**For `P:M` entries (multipart upload lifecycle):**

`P:M` entries have two states: **active** (no completion marker — upload is in progress) and **completing** (completion marker present — CompleteMultipartUpload Phase 1 committed but Phase 3 did not).

1. **Active `P:M`** (no completion marker, stale beyond threshold):
   - The upload was initiated but never completed or aborted. The head `:M:` entry may still exist. Check if head `:M:` entry exists:
     - If head exists: the initiator crashed after creating `P:M` but before any parts were uploaded, or parts are still in-flight. Wait for a longer threshold before cleanup. If still stale: delete head `:M:`, write `G:M` DEEP directive, delete `P:M`.
     - If head is missing: another process already aborted or completed the upload. Delete `P:M`.
2. **Completing `P:M`** (completion marker present):
   - CompleteMultipartUpload Phase 1 committed (head `:M:` was deleted, `P:M` was updated) but Phase 3 did not commit. Re-run Phases 2 and 3 using the stored part list. If parts are invalid or missing, roll back: re-create the head `:M:` entry from `P:M` data, clear the completion marker.

The sweeper is idempotent — multiple sweepers can process the same entry safely because each phase uses conditional checks (ref_tag verification, state flags) that prevent double-execution.

---

## Extended Value

### When the extended value is needed

The KV value has a size budget (~1–8KB depending on configuration). When read-path metadata exceeds this budget, it overflows to the extended value.

This happens primarily with large compressed multipart objects, where:

- The compression block table maps logical offsets to physical offsets. Each block adds an entry. For large objects with many parts, this table can grow to tens of KB.
- The manifest maps logical byte ranges to physical chunk locations. For objects with hundreds or thousands of parts, the manifest can grow to tens of KB or more.
- Encryption metadata (per-chunk IVs, key references) grows proportionally with part count.

For single-part objects and small multipart objects, read-path metadata fits comfortably in the KV value. The extended value is not needed.

### How the extended value is accessed

The KV entry always contains routing info pointing to the extended value's location. The retrieval method depends on the storage backend:

- **Data header** — prepended to the first data chunk. For full-object GETs from offset 0, it is read as part of the first chunk at no extra cost. For byte-range reads, it requires a separate read of the first chunk's header.

- **Standalone file** — a separate metadata file alongside the data. Always requires a separate read. Natural fit for POSIX backends that cannot prepend headers to existing files.

- **Object annotation** — sidecar metadata attached to the data object by the storage tier. Retrieval depends on the storage API.

- **Additional KV entries** — overflow stored in the same KV store under a related key (child KV with a distinguishing suffix). Retrieved via a standard KV read, co-located on the same shard.

### Local caching

For objects whose extended value is frequently accessed (large objects receiving repeated byte-range reads), RGW caches the extended value locally after first access.

Cache key: `(bucket_id, object_name, version_id, ref_tag)`.

On every use, the cached entry is validated against the ref_tag from the KV value. A mismatch (object was overwritten or deleted) invalidates the cache entry. No separate invalidation protocol is needed.
