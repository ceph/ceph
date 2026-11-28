# Bucket Cache Model

This document describes the bucket metadata caching strategy used by RGW processes to avoid reading the bucket metadata KV entry (B) on every request.

---

## 1. What Is Cached

Every S3 operation requires bucket-level metadata:
- bucket existence (a PUT to a deleted bucket must fail)
- bucket_id (to construct KV keys)
- versioning state (to decide how writes behave)
- quotas (to enforce limits)
- policies (to authorize the client)

This metadata lives in a single KV entry (B) keyed by bucket name.

---

## 2. Why Cache

Reading B on every request would add a KV round-trip to every operation, doubling the cost of cheap reads like GET/HEAD.

Instead, each RGW process maintains a local in-memory cache of bucket metadata. The challenge is keeping the cache fresh enough to enforce security (policy changes), correctness (versioning transitions, bucket deletion), and limits (quotas) — without paying the read cost on every request.

---

## 3. TTL Refresh

**3-second TTL** for cached metadata.

Rationale:
- AWS IAM authorization enforcement has ~4 seconds propagation delay.
- AWS S3 bucket policy changes take "seconds to a few minutes" to propagate.
- A 3-second TTL is at least as strict as AWS itself.
- Current RADOS RGW uses 60–120 second TTL. A 3-second TTL is significantly tighter.

### Lazy On-Demand Refresh

- Cache is only refreshed when an incoming request needs bucket metadata AND the cached entry has expired.
- If no requests arrive for a bucket, no reads happen for that bucket. Zero wasted I/O.
- When a refresh happens, it updates the cache for all subsequent requests within the TTL window.
- If 1000 GETs arrive in the same second, only the first triggers a refresh.

---

## 3.5. Refresh-Before-Reject

If the cached state would deny the request (policy denies, quota exceeded), the cache is refreshed before rejecting. This ensures admin changes (relaxed quota, updated policy) take effect immediately for requests that would otherwise be rejected on stale data.

The TTL check and the permission check are combined into a single cache-lookup function used by all flows:

```
def get_cached_bucket_entry(bucket_name, client, operation, object_size):
    entry = cache.entries[bucket_name]

    if entry is None \
       or entry.age > TTL \
       or policy_denies(client, operation, entry.meta.policy) \
       or exceeds(entry.meta.quota, object_size):

        fresh = kv.get(B, bucket_name)
        if fresh is None:
            cache.entries.remove(bucket_name)
            return None
        cache.entries[bucket_name] = CacheEntry(fresh, now())
        return fresh

    return entry.meta
```

Three triggers for refresh — any one is sufficient:
- No cache entry (first access or evicted)
- Cache age exceeded TTL (stale)
- Cached state would deny the request (refresh before reject)

If the fresh read still denies → reject. If the fresh read allows → admin changed the rules, proceed.

---

## 4. `get(B)` in Commit Transactions — Hard Enforcement

The cache provides soft checks. Hard enforcement happens inside the commit transaction, where `get(B)` reads bucket metadata fresh from the KV store.

Every transaction that creates or displaces `:O:` includes `get(B)`:

```
txn {
  get(B)   → fresh bucket metadata
  get(O:)  → existing entry

  if B not found → abort (NoSuchBucket)
  if B.bucket_id != cached_bucket_id → abort (InternalError)
  if policy_denies(...) → abort (AccessDenied)
  if exceeds(B.quota, ...) → abort (QuotaExceeded)

  // apply current versioning state from B
  ...
  put(O:, ...)
  commit
}
```

This is the single enforcement point. No matter what the cache said at request entry, the transaction enforces the truth at commit time:
- Versioning state changed between cache read and commit → adapt (apply current rules)
- Bucket deleted → abort
- Policy revoked → abort
- Quota exceeded → abort

On FDB, `get(B)` inside the transaction creates a read conflict range. If B changes after the read but before commit → transaction conflicts → automatic retry with fresh state.

---

## 5. `get(B)` for Listing

LIST operations always read B fresh, regardless of cache age:

```
LIST page request arrives:
  fresh_B = get(B)
  cache.update(bucket_name, fresh_B)

  if policy_denies(client, operation) → reject (403)
  proceed with range scan
```

One extra `get(B)` is negligible against the cost of the range scan (potentially thousands of keys per page). The fresh read also refreshes the cache — subsequent GETs benefit.

---

## 5.5. Bucket-Metadata Reads — Always Fresh

Operations whose primary purpose is to read and return fields from `B` always bypass the TTL cache and read `B` fresh from KV. The response must reflect current bucket state — especially after admin configuration changes or DeleteBucket.

```
def get_fresh_bucket_entry(bucket_name, client, operation):
    fresh = kv.get(B, bucket_name)
    if fresh is None:
        cache.entries.remove(bucket_name)
        return None
    cache.update(bucket_name, fresh)   // side effect: warm cache for object ops
    if policy_denies(client, operation, fresh.policy):
        return DENIED
    return fresh
```

No TTL check. No `get_cached_bucket_entry()`. One `get(B)` per request.

**Operations in this category:**

- HeadBucket
- GetBucketAcl
- GetBucketVersioning
- GetBucketLocation
- GetBucketPolicy
- GetBucketCors
- GetBucketLifecycleConfiguration
- GetBucketTagging
- GetBucketEncryption
- GetBucketOwnershipControls
- GetBucketObjectLockConfiguration
- GetBucketRequestPayment
- GetBucketWebsite
- GetBucketLogging
- GetBucketNotification
- GetBucketReplication
- GetPublicAccessBlock (bucket scope)
- GetBucketAccelerateConfiguration
- ListBucketAnalyticsConfigurations
- ListBucketInventoryConfigurations
- ListBucketMetricsConfigurations
- ListBucketIntelligentTieringConfigurations

These are infrequent management calls — one extra `get(B)` is negligible. A stale cache could return 200 for HeadBucket up to 3 seconds after DeleteBucket, or return outdated ACL/versioning/location settings immediately after an admin change.

`PutBucket*` configuration writes use the normal write path: soft-check via cache at entry, `get(B)` fresh inside the commit transaction.

---

## 6. Phase 1 vs Phase 3 — Cached vs Fresh

For multi-phase operations (Tier 3 PUT — data on external storage tier):

### Phase 1 — Uses Cached Bucket State

```
put(P:O, {ref_tag, estimated_size})
```

- Single KV write. No transaction, no reads.
- Uses cached `bucket_id` to construct the P:O key.
- Policies and quotas are soft-checked from cache (via `get_cached_bucket_entry`).
- If cache says deny → refresh-before-reject fires → reject early or proceed.
- If cache says allow → proceed. Hard enforcement deferred to Phase 3.

Phase 1 is cheap. No correctness risk from stale cache — the worst case is writing a P:O entry that Phase 3 will later abort. The sweeper cleans orphaned P:O entries.

### Phase 3 — Reads B Fresh (Hard Enforcement)

```
txn {
  get(P:O) → existence check
  get(B)   → fresh bucket metadata
  get(O:)  → current entry

  // enforce current state, abort or adapt
  ...
  put(O:, ...)
  delete(P:O)
  commit
}
```

Phase 3 never trusts the cache. It reads B fresh and enforces the current truth. If the versioning state changed between Phase 1 and Phase 3 (admin enabled/suspended versioning during data upload), Phase 3 adapts — it applies the current versioning rules to displace the old O: entry correctly.

---

## 7. Single-Transaction PUT (No External Storage Tier)

For small objects where data lives in the KV layer itself, no P:O coordination is needed — there is no two-domain problem.

### Tier 1 — Inline (data stored in O: value)

Objects < 256B. Data is embedded directly in the O: KV value.

```
meta = get_cached_bucket_entry(bucket_name, client, op, size)
if meta is None → 404

txn {
  get(B)  → hard enforcement (existence, policy, quota, versioning)
  get(O:) → current entry

  // abort checks (same as Phase 3)
  // apply versioning displacement
  put(O:, new_value_with_inline_data)
  commit
}
```

Single transaction. No Phase 1 (no P:O), no Phase 2 (no storage-tier write). But `get(B)` inside the transaction is still required — same hard enforcement as Tier 3 Phase 3.

### Tier 2 — Child D: KV (data stored in derived KV entry)

Objects 256B–8KB. Data is stored in a separate child KV entry `C:<ref_tag>D`, co-located on the same shard.

```
meta = get_cached_bucket_entry(bucket_name, client, op, size)
if meta is None → 404

txn {
  get(B)  → hard enforcement
  get(O:) → current entry

  // abort checks
  // apply versioning displacement
  put(C:<ref_tag>D, object_data)
  put(O:, new_value with chunk_descriptor={type: CHILD_D})
  commit
}
```

Single transaction. The child D: entry is on the same shard (ref_tag-based key, same hash), so both writes are shard-local.

### Why No P:O for Tier 1/2

The P:O coordination entry exists to solve the two-domain problem: if data is written to an external storage tier and the process crashes before committing O:, the storage-tier data is orphaned.

For Tier 1/2, data and metadata are in the same KV transaction. If the transaction commits, both exist. If it doesn't commit (crash, conflict, abort), neither exists. No orphan possible. No sweeper needed.

### Cache Role Is Identical

Regardless of tier, the bucket cache role is the same:
- `get_cached_bucket_entry()` at request entry — soft check, refresh-before-reject
- `get(B)` inside commit transaction — hard enforcement

The only difference is structural: Tier 3 separates these into Phase 1 and Phase 3 (with Phase 2 data write in between). Tier 1/2 collapses them into a single transaction.

---

## 8. DELETE and `get(B)`

### DELETE without version-id — requires `get(B)` in transaction

- Must know current versioning state to decide behavior:
  - Unversioned: remove O:, create G:O
  - Versioned: displace O: to V:, write fenced delete marker
  - Suspended: overwrite null slot, write fenced DM with NULL_ID
- Getting versioning state wrong → data loss or incorrect semantics
- Cached state insufficient — admin can change versioning between request entry and commit
- On FDB: `get(B)` creates read conflict range, catches race with DeleteBucket

### DELETE with version-id — does NOT require `get(B)` in transaction

- Operation is mechanical — targets a specific version_id regardless of versioning state
- Bucket existence is self-resolving: no B → no O:/V: entries → 404 naturally
- On TiKV: `get(B)` is a snapshot read with no write-write conflict — provides no protection
- On FDB: DeleteBucket's RangeScan on O:/V: already conflicts with this operation's writes to O:/V:
- Policy enforcement: AWS documents eventual consistency for policy propagation (seconds to minutes). The 3s TTL cached check with refresh-before-reject is already stricter than AWS requirements

---

## 9. Listing and Bucket Identity

### The Problem

S3 listing is stateless — no server-side cursor between pages:
- Each page is an independent request.
- If a bucket is deleted and recreated (same name, new bucket_id) between pages, the server has no memory of which bucket_id the previous page used.
- The client silently receives a mixed listing:
    - early pages from the old bucket
    - later pages from the new bucket

### Pagination Rules by API

- **ListObjectsV2** — continuation token is opaque. AWS docs: "ContinuationToken is obfuscated." Client passes it back unchanged. Server controls the format.
- **ListObjectsV1** — marker is the last object key, client-supplied. Transparent, defined format.
- **ListObjectVersions** — uses `KeyMarker` + `VersionIdMarker`, both explicit client-supplied values.

### Solution

**ListObjectsV2:** embed bucket_id in the opaque continuation token.

```
first page (no token):
  fresh_B = get(B)
  scan :O: under fresh_B.bucket_id
  return results + token{bucket_id, last_key, shard_markers}

subsequent pages (with token):
  fresh_B = get(B)
  if fresh_B.bucket_id != token.bucket_id:
    return Error(InvalidContinuationToken)
  scan :O: from token position
  return results + token{bucket_id, next_key, shard_markers}
```

- Zero extra reads — `get(B)` is already called on every LIST page
- One 8-byte comparison per page
- Client retries listing from scratch on error

**ListObjectsV1 and ListObjectVersions:** no protection possible
- Markers are client-controlled — no room for server-side state.
- Bucket delete+recreate during active pagination is an extreme edge case (bucket must be emptied first, which itself disrupts listing).
- Acceptable to leave unprotected.

---

## 10. Which Operations Use Cached vs Fresh Bucket State

Most S3 operations call `get_cached_bucket_entry()` at request entry — resolves `bucket_id`, enforces policy, and checks quota. Bucket-metadata reads and LIST bypass the TTL cache entirely (see [§5.5](#55-bucket-metadata-reads--always-fresh) and [§5](#5-getb-for-listing)). Write commits additionally read B fresh inside their transaction for hard enforcement.

### Always fresh — bypasses TTL cache

These operations call `get_fresh_bucket_entry()` — always `get(B)` from KV, no TTL check. The fresh read updates the cache as a side effect.

- HeadBucket
- GetBucketAcl, GetBucketVersioning, GetBucketLocation, GetBucketPolicy, GetBucketCors, GetBucketLifecycleConfiguration, GetBucketTagging, GetBucketEncryption, GetBucketOwnershipControls, GetBucketObjectLockConfiguration, GetBucketRequestPayment, GetBucketWebsite, GetBucketLogging, GetBucketNotification, GetBucketReplication, GetPublicAccessBlock (bucket), GetBucketAccelerateConfiguration, ListBucket*Configurations
- LIST (all variants) — always fresh, negligible cost vs range scan

### Cached state only — `get_cached_bucket_entry()`

These operations rely entirely on cached bucket metadata. No `get(B)` in any transaction.

- GET / HEAD — policy check (s3:GetObject)
- DELETE with version-id — policy check (s3:DeleteObjectVersion)
- UploadPart — policy check (s3:PutObject)
- PutObjectTagging — policy check (s3:PutObjectTagging)
- DeleteObjectTagging — policy check (s3:DeleteObjectTagging)
- GetObjectTagging — policy check (s3:GetObjectTagging)
- AbortMultipartUpload — policy check (s3:AbortMultipartUpload)
- ListParts — policy check (s3:ListMultipartUploadParts)
- ListMultipartUploads — policy check (s3:ListBucketMultipartUploads)
- PUT Phase 1 / InitiateMultipartUpload — policy + quota (soft check, hard enforcement deferred to Phase 3)

Refresh triggers for `get_cached_bucket_entry()`:
- TTL expired — applies to all
- Policy denies — applies to all
- Quota exceeded — applies only to writes with object_size (PUT Phase 1, UploadPart, InitiateMultipartUpload)

### Cached state + fresh `get(B)` in transaction

These operations call `get_cached_bucket_entry()` at request entry AND read B fresh inside their commit transaction. The fresh read provides hard enforcement of versioning state, quotas, and policies at commit time.

- PUT commit (all tiers) — needs current versioning state for displacement rules
- DELETE without version-id — needs current versioning state to decide behavior (remove vs create delete marker)
- CompleteMultipartUpload commit — needs current versioning state for displacement rules
- PutBucketAcl and other PutBucket* configuration writes — read-modify-write on B
- PutObjectAcl — needs `BucketOwnerEnforced` check from fresh B. See [acl.md](acl.md)
- GetObjectAcl — needs `B.owner` to expand canned bucket-owner ACLs. See [acl.md](acl.md)

The dividing line:

| Category | Cache behavior |
|---|---|
| Bucket-metadata **reads** | Always `get(B)` fresh — no TTL |
| Object / data-path ops | TTL cache + refresh-before-reject |
| Write commits | Fresh `get(B)` inside transaction |
| LIST | Always fresh (already) |
