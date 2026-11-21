# Bucket State Management

This document is the source of truth for bucket state semantics, caching strategy, and enforcement rules in the KV-based RGW design.

---

Every S3 operation requires bucket-level metadata to execute:
- bucket existence (a PUT to a deleted bucket must fail)
- the bucket_id (to construct KV keys)
- versioning state (to decide how writes behave)
- quotas (to enforce limits)
- policies (to authorize the client)

This metadata lives in a single KV entry (B) keyed by bucket name.
- Reading B on every request would add a KV round-trip to every operation, doubling the cost of cheap reads like GET/HEAD.
- Instead, we cache bucket metadata locally on each RGW process.
- The challenge is keeping the cache fresh enough to enforce security (policy changes), correctness (versioning transitions, bucket deletion), and limits (quotas) — without paying the read cost on every request.

---

## 1. Bucket Metadata Caching Strategy

Every S3 request resolves `bucket_name → bucket metadata` before any operation logic runs.\
This resolution uses a local in-memory cache to avoid reading B (the bucket metadata KV entry) on every request.

### Refresh Model

Lazy on-demand refresh
- Cache is only refreshed when an incoming request needs bucket metadata AND the cached entry has expired.
- If no requests arrive for a bucket, no reads happen for that bucket. Zero wasted I/O.
- When a refresh happens, it updates the cache for all subsequent requests within the TTL window.

### TTL

**3-second TTL** for cached metadata (versioning, quotas, policies..).

Rationale:
- AWS IAM authorization enforcement has ~4 seconds propagation delay.
- AWS S3 bucket policy changes take "seconds to a few minutes" to propagate.
- AWS documents this as eventual consistency with no guaranteed upper bound.
- A 3-second TTL is at least as strict as AWS itself.
- Current RADOS RGW uses TTL-based cache refresh (typically 60-120 seconds). A 3-second TTL is significantly tighter.

**Reference:** [AWS IAM Troubleshooting — Changes not immediately visible](https://docs.aws.amazon.com/IAM/latest/UserGuide/troubleshoot_general.html)

---

## 2. Refresh Strategy by Operation Type

The core principle:
- If the operation is already expensive, an extra Get(B) will add little to the overall cost.
- If the operation is cheap, use TTL to avoid doubling the cost.

### GET / HEAD (cheap operations)

```
GET(Object) arrives:
  bucket_meta = cache.get(bucket_name)

  if bucket_meta.age > 3 seconds:
    fresh_B = get(B)            // single KV read, no txn
    cache.update(bucket_name, fresh_B)
    bucket_meta = fresh_B

  proceed with GET(Object)
```

- Cost: 0 extra reads if cache is fresh. 1 read (amortized) if stale.
- If 1000 GETs arrive in the same second, only the first triggers a refresh.

### LIST (expensive operations — always read B fresh)

```
LIST page request arrives:
  fresh_B = get(B)              // always — negligible vs. the range scan
  cache.update(bucket_name, fresh_B)

  if policy_denies(client, operation) → reject (403)
  proceed with range scan
```

- LIST does a range scan over potentially thousands of keys per page.
- One extra Get(B) adds negligible cost.
- The fresh read also refreshes the cache — subsequent GETs benefit.

### Bucket-metadata reads (always read B fresh)

Operations whose primary purpose is to read and return fields from `B` always bypass the TTL cache:

```
HeadBucket / GetBucketAcl / GetBucketVersioning / GetBucketLocation / ... arrives:
  fresh_B = get(B)              // always — no TTL check
  if fresh_B is None → return 404 (NoSuchBucket)
  cache.update(bucket_name, fresh_B)
  if policy_denies(client, operation) → reject (403)
  return fields from fresh_B
```

Applies to HeadBucket and all GetBucket* / ListBucket*Configuration APIs. These are infrequent management calls. The response must reflect current bucket state — a stale cache could return 200 after DeleteBucket or outdated ACL, versioning, or location settings immediately after an admin change.

See [bucket_cache.md — Bucket-Metadata Reads](bucket_cache.md#55-bucket-metadata-reads--always-fresh) for the full operation list.

### PUT / DELETE (write operations)

PUT uses cached bucket state before starting the operation.

The transaction that commits the new object to KV includes Get(B) — always reading fresh bucket metadata at commit time.

We may trigger an early cache refresh before rejecting a request based on stale cached state (e.g., quota exceeded) — ensuring admin changes take effect promptly even for rejected operations.

---

## 3. The Two-Domain Problem

- Object data lives on the **storage tier** (blobs addressed by `ref_tag`).
- Object metadata lives in the **KV store**\
(`:O:` entries keyed by bucket_id + object_name).
- These two domains cannot share a transaction — a single atomic operation cannot span both.

This creates a crash-safety problem: if RGW writes data to the storage tier and then crashes before writing the `:O:` metadata entry, the storage-tier data is orphaned — no metadata references it, no client can access or delete it, and no background process knows it exists.

The solution is the **`:P:` (pending) namespace** 
- A coordination entry written to the KV store *before* data is written to the storage tier.
- The `P:O` entry records the intent to create an object (including its `ref_tag`).
- If the process crashes after writing data but before committing metadata, the `P:O` entry remains.
- A background **sweeper** finds stale `P:O` entries, uses the `ref_tag` to locate and free the orphaned storage-tier data, and removes the `P:O` entry.

---

## 4. PutObject — Three-Phase Protocol (Overview)

PUTs use a 3-phase protocol to bridge the two-domain gap safely:

**Phase 1 — Record intent.** 
- Write a `P:O` coordination entry to the KV store.
- Single KV write, no transaction, no reads.
- Uses cached `bucket_id` to construct the key.
- This is the crash-recovery anchor — if the process dies after this point, the sweeper can find and clean up any orphaned data.

**Phase 2 — Write data.**
- Upload blob to the storage tier addressed by `ref_tag`.
- No KV operations.
- Can be slow for large objects.

**Phase 3 — Commit metadata.**
- A single KV transaction (see simplified version below) 
- This is where all enforcement happens — versioning rules, quota checks, policy verification, bucket existence validation.
- If anything is wrong, Phase 3 self-cleans (writes a GC entry for the storage-tier data and removes P:O) without leaving orphans.

```
txn {
  get(P:O) → if null → abort (crashed-and-swept or duplicate)
  get(B)   → fresh bucket metadata (bucket_id, versioning state, quotas)
  get(O:)  → existing entry
  put(O:)  → create new entry
  delete(P:O)
  commit
}
```

Full protocol details with versioning branches and abort paths in [Section 10](#10-put-3-phase-protocol--full-detail).

---

## 5. Sweeper

The sweeper is a background process that scans the `:P:` namespace for stale entries — `P:O` entries whose owning RGW process has crashed without completing Phase 3.
The sweeper scans the entire P: namespace globally — not per-bucket.

### Sweeper action (for stale P:O)

```
txn {
  get(P:O) → if null → skip (already handled)
  put(G:S, {ref_tag})
  delete(P:O)
  commit
}
```

The sweeper moves the `P:O` entry to the `:G:` (GC) namespace.\
A separate GC worker later reads `G:S` entries and frees storage-tier data by `ref_tag` (idempotent delete).


### Sweeper vs PUT Phase 3 — Race Prevention

- The sweeper and PUT Phase 3 can race on the same `P:O` entry.
- Both transactions read and write (delete) the same key.
- This shared read+write pattern provides complete mutual exclusion on both FDB and TiKV.

---

## 6. Bucket Deletion Rules

### AWS Requirement

From the [DeleteBucket API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucket.html):

> "All objects (including all object versions and delete markers) in the bucket must be deleted before the bucket itself can be deleted."

This is unconditional — no qualifiers about bucket state (Enabled vs. Suspended).

### What Blocks DeleteBucket

| Entry type | Blocks deletion? | Rationale |
|---|---|---|
| `:O:` entries (current objects) | Yes | Committed client data |
| `:V:` entries (old versions, delete markers) | Yes | Committed client data |
| `:P:` entries (in-flight operations) | No | Not committed — sweeper handles cleanup |
| `:M:` entries (multipart upload parts) | No | Not committed — can be aborted |

### Suspended State Does Not Change Enforcement

Suspending versioning means "stop creating new versions for future writes."\
It does NOT:
- Make existing versions invisible
- Allow bulk deletion of versions
- Change the DeleteBucket precondition

The client must individually delete every version and delete marker (via DELETE with version-id) before DeleteBucket will succeed.


### DeleteBucket Protocol

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

After commit, P: and M: entries self-destruct through existing mechanisms:
- Live Put Phase 3: `get(B)` → null → self-abort → create G:S, delete P:O.
- Live multipart uploads: next operation reads B → null → self-abort → create G:S, delete P:M.
- Crashed processes: sweeper finds stale P:O and P:M on next pass → G:S, delete P:O or P:M.

### Post-Commit Cleanup (best-effort optimization)

After DeleteBucket commits, an optional cleanup loop speeds up storage-tier reclamation using **RangeScan** and **RangeDelete** instead of processing one KV after another.\
If the process crashes mid-loop, the sweeper handles remaining entries on its next pass.

```
// Clean P: entries for deleted bucket
cursor = start of :P:<shard><bucket_id>
while true:
  txn {
    entries = RangeScan from cursor (limit 1000)
    if empty → break
    for each entry: put(G:S, {ref_tag, source=BUCKET_DELETE})
    RangeDelete cursor ... last_key + 1
    cursor = last_key + 1
    commit
  }

// Clean M: entries for deleted bucket
cursor = start of :M:<shard><bucket_id>
while true:
  txn {
    entries = RangeScan from cursor (limit 1000)
    if empty → break
    for each entry: put(G:S, {ref_tag, source=BUCKET_DELETE})
    RangeDelete cursor ... last_key + 1
    cursor = last_key + 1
    commit
  }
```

Each batch: 1000 entries, ~1.4MB — well within FDB limits. Contiguous range scan + one range delete per batch.

### FDB Safety

The simplified DeleteBucket is fully safe on FDB. Two independent conflict points:

**Conflict 1 — B:**
- Phase 3 (PUT or CompleteMultipartUpload): `get(B)` → read conflict range on B
- DeleteBucket: `delete(B)` → write to B
- If DeleteBucket commits first → Phase 3 conflicts on B → retries → sees B null → self-aborts. ✓
- If Phase 3 commits first → (see conflict 2)

**Conflict 2 — O:**
- Phase 3: `put(O:)` → write to O:
- DeleteBucket: `RangeScan :O: (limit 1)` → read conflict range on O:
- If Phase 3 commits first → DeleteBucket conflicts on O: → retries → sees O: → BucketNotEmpty. ✓

Either conflict point catches the race. Any operation that commits data to a bucket reads B; DeleteBucket deletes B; FDB catches the conflict.

### TiKV Safety

On TiKV, `get(B)` in Phase 3 is a snapshot read — no conflict range. A Phase 3 that started before DeleteBucket can commit O: successfully even after B is deleted. The race window is Phase 3's transaction duration — single-digit milliseconds.

TiKV provides best-effort safety with negligible residual risk. No worse than the current RADOS model (which has no protection at all for concurrent DeleteBucket vs PUT).

### "O:/V: implies bucket exists" Invariant

Operations that read O: or V: rely on the invariant: if O: or V: exists, the bucket must exist.

- **FDB:** Transactional guarantee. DeleteBucket's RangeScans on O:/V: create read conflict ranges. A concurrent operation reading O:/V: in the same serialization window would conflict with DeleteBucket.
- **TiKV:** Soft guarantee. Same residual risk as above — extremely unlikely but not transactionally excluded. If violated, produces orphaned C: entries (invisible garbage). Cleaned by a background scrubber (TBD).

---

## 7. Bucket Recreation and Identity

### bucket_id Semantics

- `bucket_id` is assigned at bucket creation time (a unique identifier, e.g., UUID).
- It is immutable for the lifetime of the bucket.
- It is NOT the bucket name — the name is a human-readable alias that maps to the bucket_id.
- The same bucket name can be reused after deletion. The new bucket gets a new bucket_id.

### Detection in Phase 3

```
Phase 1: cached bucket_id = X (from routing cache)
Phase 2: upload data (ref_tag → storage tier)
Phase 3: get(B) → B.bucket_id = Y

If X != Y → bucket was deleted and recreated under the same name.
  → Abort: write G:S, delete P:O, return HTTP 500.
  → Client SDK retries automatically.
  → Routing cache refreshes with bucket_id = Y.
  → Second attempt succeeds on the correct bucket.
```

### Why Abort (Not Adapt)

- **Security:** Client was authorized against old bucket (X). New bucket (Y) might have different owner or policies. Writing to it without re-authorization is a potential security hole.
- **Simplicity:** Abort + retry is dead simple. The retry path handles everything correctly (fresh routing, fresh authorization).
- **Rarity:** Bucket delete + recreate while a PUT is in-flight is an extremely rare administrative event.

### HTTP 500 on Bucket-ID Mismatch

Returning HTTP 500 (InternalError) on bucket_id mismatch is:
- **Legal:** Part of the S3 API contract. AWS returns 500s for transient internal conditions.
- **Expected:** All AWS SDKs implement automatic retry with exponential backoff (50ms base delay for InternalError).
- **Invisible to the user:** SDK retries automatically. Second attempt refreshes routing cache, succeeds on correct bucket.
- **Rare:** Requires bucket delete + recreate while a PUT is in-flight.

**Reference:** [AWS SDK Retry Behavior](https://docs.aws.amazon.com/sdkref/latest/guide/feature-retry-behavior.html)

---

## 8. Versioning State Machine

```
                 ┌────────────────────────────┐
                 │                            ▼
Disabled ──────→ Enabled ←────────────→ Suspended
   │              (15-min)   (immediate←)      ▲
   └───────────────────────────────────────────┘
                     (15-min propagation)
```

| Transition | Propagation | Reversible? |
|---|---|---|
| Disabled → Enabled | Up to 15 minutes | No — cannot return to Disabled |
| Disabled → Suspended | Up to 15 minutes | No — cannot return to Disabled |
| Enabled → Suspended | Immediate | Yes |
| Suspended → Enabled | Immediate | Yes |

Once a bucket leaves the Disabled state — whether to Enabled or Suspended — there is no going back. AWS forbids returning to Disabled (unversioned). The bucket can only toggle between Enabled and Suspended from that point on.

The 15-minute propagation delay is an artifact of first-time internal infrastructure setup within S3's distributed system. Once a bucket has gone through its first versioning-aware transition (whether to Enabled or Suspended), subsequent Suspended ↔ Enabled transitions are immediate — the versioning infrastructure is already in place and no further propagation is required.

**References:**
- [PutBucketVersioning API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketVersioning.html)
- [AWS CloudFormation VersioningConfiguration](https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-properties-s3-bucket-versioningconfiguration.html)
- [AWS Storage Blog — Zero-downtime S3 Versioning](https://aws.amazon.com/blogs/storage/zero-downtime-amazon-s3-versioning-architectural-patterns-for-mission-critical-workloads/)

---

## 9. Null-Version and NO_VERSION Rules

### Version ID Values

| Value | Name | Meaning |
|---|---|---|
| 0 | `NO_VERSION` | Written in unversioned mode. No versioning applies. |
| 1 | `NULL_ID` | Written in suspended mode. The null-version slot. |
| 2 – max_uint32 | Real version IDs | Versioned entries (start at max_uint32, decrement via next_vid) |

### Displacement Rules

When a new write displaces the current `:O:` entry, the destination depends on the old entry's version_id:

| Old O: version_id | Displaced to | Rationale |
|---|---|---|
| Real vid (>= 2) | `V:<old_vid>` (preserved) | Versioned entry — must be kept in history |
| NULL_ID (1) | `G:O` (overwritten) | Null-version slot — always overwritten, never preserved in V: |
| NO_VERSION (0) | `G:O` (overwritten) | Pre-versioning entry — never preserved in V: |

### The Rule

**Null-version entries (version_id == NULL_ID or NO_VERSION) never move to V: — always overwrite (→ G:O).**

This applies regardless of current bucket state. A null-vid or no-version O: entry represents the "suspended-mode slot" or "pre-versioning entry." It is always overwritten, never preserved as a version in V:. This prevents accumulation of null entries in the version history.

### Rationale

- AWS S3 only preserves one "null version" per object. Successive suspended-mode writes overwrite the null slot.
- Pre-versioning objects (NO_VERSION) were written before versioning existed for this bucket. They have no meaningful version identity to preserve.
- Moving these to V: would create entries with non-unique or semantically meaningless version IDs in the history — confusing for ListObjectVersions and inconsistent with AWS behavior.

---

## 10. PUT 3-Phase Protocol — Full Detail

### Phase 1 — Coordination Entry

```
// All metadata from routing cache (bucket_id, tenant, policies)
// Single KV write — no transaction needed
put(P:O, {ref_tag, estimated_size})
```

Phase 1 is a single KV PUT. No transaction, no reads. The cached bucket_id is sufficient for constructing the P:O key. Tenant, policies, and quotas are soft-checked from cache; hard enforcement happens in Phase 3.

### Phase 2 — Data Upload

Upload data to storage tier. Addressed by ref_tag (independent of bucket_id or versioning state). Can be slow for large objects.

### Phase 3 — Commit Transaction (Single Enforcement Point)

Phase 3 reads B fresh and applies the **adapt-or-abort** rule:

- **Adapt:** versioning state changed → apply current rules, proceed.
- **Abort:** bucket deleted, bucket_id mismatch, policy denies this operation, or quota exceeded by this operation → return appropriate error, self-clean.

```
txn {
  get(P:O) → if null → abort (crashed-and-swept or duplicate)

  get(B)   → fresh bucket metadata (bucket_id, versioning state, quotas)
  get(O:)  → existing entry

  // --- Abort: bucket deleted ---
  If B not found:
    put(G:S, {ref_tag}); delete(P:O); commit
    → return HTTP 404 (NoSuchBucket)

  // --- Abort: bucket recreated ---
  If B.bucket_id != cached_bucket_id:
    put(G:S, {ref_tag}); delete(P:O); commit
    → return HTTP 500 (InternalError)

  // --- Abort: access denied ---
  If policy_denies(client, operation, B.policy):
    put(G:S, {ref_tag}); delete(P:O); commit
    → return HTTP 403 (AccessDenied)

  // --- Abort: quota exceeded ---
  If exceeds(B.quota, this_object_size):
    put(G:S, {ref_tag}); delete(P:O); commit
    → return HTTP 429 (QuotaExceeded)

  // --- Adapt path: apply current versioning state ---
  if unversioned:
    If O: exists: if O:.vid != NO_VERSION (0) → abort (invariant violation)
    If O: exists: put(G:O, old)
    put(O:, new_value with vid=NO_VERSION, next_vid=max_uint32)

  if suspended:
    If O: has real vid (>= 2): put(V:<old_vid>, old)  — preserve versioned entry
    If O: has NULL_ID (1): put(G:O, old)               — overwrite null slot
    If O: has NO_VERSION (0): put(G:O, old)            — overwrite pre-versioning entry
    put(O:, new_value with vid=NULL_ID, next_vid=O:.next_vid)

  if versioned:
    If O: has real vid (>= 2): put(V:<old_vid>, old)  — preserve
    If O: has NULL_ID (1): put(G:O, old)               — null entries NEVER go to V:
    If O: has NO_VERSION (0): put(G:O, old)            — pre-versioning entries NEVER go to V:
    put(O:, new_value with vid=next_vid, next_vid=vid-1)

  delete(P:O)
  commit
}
```

### Why This Works

- **Versioning transitions:** Phase 3 always reads the current state. No triggers, no heuristics, no conditional re-reads. Correct by construction.
- **Quota changes:** Phase 3 enforces the current quota. Admin relaxes quota → next PUT sees it immediately.
- **Policy changes:** Phase 3 checks current policies. Revoked access → next PUT rejected.
- **FDB conflict detection:** Get(B) inside the transaction creates a read conflict range. If B changes after our read but before commit → transaction conflicts → automatic retry with fresh state.
- **TiKV snapshot:** B is part of the transaction snapshot — consistent with the O: read.

### Why get(P:O) is Essential

The `get(P:O)` at the start of Phase 3 serves two purposes:

- **Existence verification:** If P:O is null, the sweeper (or bucket-delete) already claimed it — data cleanup is in progress. Phase 3 must abort without writing O:. Nothing to clean.
- **Conflict detection:** The read of P:O, combined with the `delete(P:O)` at the end, creates mutual exclusion with any concurrent sweeper or bucket-delete transaction targeting the same entry. On FDB, the read creates a conflict range. On TiKV, the write-write on `delete(P:O)` catches concurrency. See [Section 5 — Sweeper vs Phase 3 Race Prevention](#sweeper-vs-put-phase-3--race-prevention).

---

## 11. Abort and Cleanup

### GC Categories

| Category | Meaning | GC worker action |
|---|---|---|
| `G:O` | Overwritten object — KV existed | Free storage-tier data + delete child KVs (C: entries) |
| `G:F` | Full object delete (vendor extension) | Free storage-tier data + scan/delete all V: entries |
| `G:S` | Aborted before KV commit | Free storage-tier data **only** — no KV ever existed |

`G:S` is used when Phase 3 aborts after storage-tier write (Phase 2 completed) but before any O: or V: entry was written. The GC worker only needs the `ref_tag` to locate and free the storage-tier data. No KV cleanup is required.

### Cleanup Responsibility

| Situation | Who cleans up |
|---|---|
| Phase 3 detects bucket_id mismatch or bucket deleted | **Phase 3 itself** — writes G:S + deletes P:O in same transaction |
| Phase 3 detects policy/quota violation (after re-read) | **Phase 3 itself** — writes G:S + deletes P:O in same transaction |
| Process crashes between Phase 1 and Phase 3 | **Sweeper** — finds stale P:O, writes G:S, deletes P:O |

The sweeper only handles crash recovery. Operational aborts are self-cleaning — no orphans are left behind.

### Abort Mechanisms

| | Client-abort | Internal-abort |
|---|---|---|
| Non-multipart PUT (`P:O`) | None (no S3 API) | Sweeper, Bucket-delete |
| Multipart upload (`:M:`) | `AbortMultipartUpload` API | Bucket-delete, Lifecycle engine |

Both internal-abort mechanisms use the same pattern: remove the coordination entry, let the in-flight operation detect it's gone and abort. For `P:O`: move to `G:S`. For `:M:` head: delete head entry, create `G:M` DEEP directive for background cleanup.

**Ownership:**
- **Sweeper**: owns `:P:` cleanup (stale entries from crashed processes).
- **Lifecycle engine**: owns `:M:` cleanup (abandoned multipart uploads, per-bucket `AbortIncompleteMultipartUpload` policy).
- **Bucket-delete**: can abort both `:P:` and `:M:` entries (force-abort during bucket deletion).
