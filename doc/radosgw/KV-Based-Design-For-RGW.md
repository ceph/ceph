# KV-Based Design for RGW

## Introduction

An S3-compatible object store must maintain metadata for every object:

- Its identity — bucket, name, version.
- User-visible attributes — content-type, etag, tags, ACLs, checksums.
- Internal data attributes — manifest, compression, encryption.

This metadata must be accessible independently of the data itself.
HEAD requests return it without reading any data.
Bucket listing must enumerate it efficiently across potentially billions of objects.

In RGW, RADOS head objects serve this purpose. Every S3 object has a corresponding head object that holds its identity, attributes, and a manifest pointing to data chunks.

For small objects, the data itself is inlined into the head object.\
For multipart uploads, storage-class placement, versioning (OLH), and cloud tiering — the head object acts as a pure metadata pointer, a redirection layer to data stored elsewhere.

In effect, RGW is already using head objects as a de facto KV system, but implemented with full RADOS objects carrying all their associated overhead.

This document proposes replacing head objects with entries in an external KV store.

The design is DB-agnostic — it defines the KV contract RGW requires, using FoundationDB and TiKV as reference examples. Any KV system meeting the contract can serve as the metadata layer, including systems that do not yet exist.

---

## Problems with the Current Model

The one-to-one mapping between S3 objects and RADOS objects creates a set of compounding problems:

**1. Small-object packing.**
Each S3 object, no matter how small, requires its own RADOS object.
- Prevents aggregating small objects into larger blobs.
- Every small object carries full RADOS overhead: OSD tracking, PG membership, recovery bookkeeping.
- Recovery cost scales with object count rather than data volume.
- The storage tier cannot optimize for large sequential I/O.

**2. Versioning (OLH).**
Object Lifecycle Head objects add complexity beyond the extra RADOS object itself:
- Managing the current-version pointer.
- Copying data into new objects on version changes.
- Lack of atomicity across OLH, head object, and bucket-index updates.

**3. Server-Side Copy and dedup.**
Multiple S3 objects cannot share a head object.
A server-side copy or deduplication must create a full independent head object even when the data is identical.
- Data inlined in the head-object (up to 4MB) is duplicated and copied by server-side copy.
- Dedup moves the inline data from the head object into a new tail-object to allow full data deduplication.

**4. Key rename.**
Renaming an S3 object requires copying the entire object and deleting the original — there is no in-place rename.\
Especially costly when applications like Apache Spark/Hadoop rename entire directory trees.

**5. Backend lock-in.**
The storage tier must implement the full RADOS object model (xattrs, omap).
RGW cannot use a simpler blob store without reimplementing that model.

**6. EC metadata amplification.**
Every RADOS head object carries metadata (xattrs, omap) replicated across all K+M EC members.
- 6 copies in a 4+2 scheme.
- 11 copies in 8+3.

This far exceeds what metadata protection requires.\
Furthermore, Delete operations must remove rados objects on **all** K+M EC members.

**7. Bucket listing.**
Head objects scattered across RADOS cannot be listed efficiently.\
This is why the bucket-index exists as a separate structure — duplicating metadata to enable listing, at the cost of dual updates and consistency issues.

**8. Storage-class placement.**
When data is placed in a non-default storage class pool, the head object must still reside in the default pool — acting purely as a KV-style pointer at full RADOS object cost.

**9. Delete overhead.**
Deleting an S3 object requires two non-atomic operations on separate systems:
- Removing the bucket-index entry.
- Deleting the head object:
    - Head object might be on a slow storage tier (HDD).
    - On EC, head-object delete fans out across all K+M members.

See [Delete](#delete) for detailed analysis.

---

## Requirements

**R1 — Decouple S3 Object Identity from Storage-Tier Naming.**
S3 identity (bucket_id, object_name, version_id) lives exclusively in the KV key.

The data store has no concept of S3 names — it stores opaque blobs addressed by `(blob_id, offset, length)`.
Chunk pointers in the KV value are the sole link between S3 identity and physical storage.

**R2 — Decouple RGW from the Storage Tier.**
The storage tier becomes a dumb blob store:
- Write bytes.
- Read byte ranges.
- Punch holes.

Zero knowledge of S3 and/or RGW. RGW manages its own metadata in the KV store.

**R3 — KV Semantics.**
- Each S3 object (bucket + name + version) maps to exactly one KV entry in the S3 KV namespace.
- A key with a live value means the object exists.
- Delete removes or invalidates the entry, making it immediately inaccessible.

**R4 — Bucket Listing.**
- KV range scans replace the bucket-index.
- Each entry returns listing attributes directly from the KV value — no data-tier access.

**R5 — KV Value: Core Requirements.**
The KV value must always contain:
- All listing attributes (key, last_modified, etag, size, storage_class, owner, checksum_algorithm).
- Routing info for the first data chunk.
- Routing info for spillover metadata (user attributes, manifest, etc.) when present.

> *Stretch goals — desirable but may be overridden by KV system constraints or value size limits:*
>
> **R5.1 — KV Value Contains the Full S3 API Surface.**
> Aim to store all user-visible S3 attributes (user metadata, tags, ACLs, object lock) in the KV value.
> Objects with very large user attributes may spill to an external location.
> Whether to always keep all user attributes in a single KV entry or allow spillover is an open design question.
>
> **R5.2 — KV Value Contains Full Routing Info.**
> Aim to store the complete manifest, compression, and encryption metadata in the KV value, so that data access requires no secondary metadata lookup.
> When these do not fit (e.g., large compressed multipart objects), RGW falls back to reading them from the data header or spillover storage.

**R6 — Caching Delegated to KV Store.**
No local RGW cache for object KV entries.
The KV store's own distributed cache serves hot entries from memory.

Local RGW caching is limited to:
- Metadata for Bucket/Realms/Zones/etc
- Mapping tables.
- Data headers of large objects.

---

## Delete

Deleting an S3 object involves two distinct concerns:
- **Fencing** — making the object inaccessible.
- **Data cleanup** — reclaiming storage space.

In the current model, both are entangled with RADOS object operations.
A KV-based design separates them cleanly.

### Current Model

Deleting one S3 object requires:
- Updating the bucket-index entry.
- Deleting **all** head objects.

Two non-atomic operations on separate systems.\
A crash between the bucket-index update and the head-object delete can leave the system in inconsistent state.

On EC pools, each RADOS object deletion fans out across all K+M members (6 OSDs for 4+2, 11 for 8+3).

When RADOS objects are stored on HDD, the delete must wait for all members to complete on slow HDD.

### KV Delete Benefits

**Single atomic operation.**
The bucket-index is eliminated.
A single KV mutation (delete or invalidate) makes the object inaccessible.
No second operation on a separate system.

**No EC fan-out for metadata.**
A KV delete does not fan out across K+M members.
The EC cost is deferred entirely to async data cleanup.

**Always on fast storage.**
The KV store resides on SSD/NVMe.
Delete never touches slow HDD — important when data is stored on HDD.

**Faster than RADOS object delete.**
Even on the same hardware, a KV delete is a lightweight metadata operation compared to the full transaction overhead of deleting a RADOS object.

PR #48711 added a new concept (`inline-data=false`) to avoid the delete penalty inline — empty head objects on a fast tier with replica×3.
But even with this change, GC still needs to delete K+M members on the storage tier, which can degrade system performance under heavy delete load.

### Background Data Cleanup

Background processing to reclaim storage-tier space is needed regardless of the metadata model.

The KV design uses a persistent delete-log:
- Every operation that orphans data (DELETE, PUT-overwrite) appends an entry recording the old data references.
- A background process drains the log and frees storage.

Because the delete-log can be written in the same transaction as the KV mutation, there is no orphan window — every orphaned data reference is tracked.

---

## Bucket Listing

The bucket-index exists in the current model because head objects scattered across RADOS (based on CRUSH hashing) cannot be enumerated efficiently.

It duplicates metadata to enable listing, at the cost of:
- Dual updates on every PUT/DELETE (bucket-index + head object).
- Consistency drift between the two.
- A separate resharding mechanism as buckets grow.

With a KV-based metadata layer, the bucket-index is eliminated.
Listing is a direct range scan on the KV store — each entry already contains all listing attributes.
No secondary structure, no dual updates, no resharding.

Listing performance depends on key ordering:
- **Without sharding** — a bucket's objects form a contiguous key range. Listing is a simple range scan.
- **With sharding** — listing requires parallel scans across shards followed by a merge-sort. Still efficient, but more complex.

The full analysis of sharding trade-offs is covered in a separate document.

---

## Architecture

Two components: a KV store for metadata and a data store for opaque blobs.

- **KV store** — owns object existence, all S3 attributes, data pointers, and bucket metadata.
- **Data store** — owns raw bytes. No knowledge of S3, buckets, or object names.

RGW owns all intelligence: access control, manifest interpretation, compression, encryption, packing logic.

The bucket-index is eliminated.
Its functions — listing, metadata lookup, bucket stats — are absorbed by the KV store.

---

## KV Access API

RGW accesses the KV store through a DB-agnostic API.\
The API defines the operations RGW requires — Each DB backend maps these operations to its native primitives.\
Differences in capability are absorbed by the implementation.

This is an early-stage API definition. It will be refined as the design matures and implementation begins.

**Basic operations:**

- `Get(key)` → value, or not-found.
- `Put(key, value)` — write or overwrite.
- `Delete(key)` — remove the entry.

**Range operations:**

- `RangeScan(start, end, limit)` → ordered list of KV pairs. Used for bucket listing.
- `RangeDelete(start, end)` — delete all entries in the range. Used for bucket deletion, prefix cleanup.

**Conditional operations:**

- `PutIfNotExists(key, value)` — write only if the key does not exist. Used in migration, multipart completion.
- `CompareAndSwap(key, expected_value, new_value)` — write only if the current value matches. Used for stats counters on DBs without blind atomic adds.

**Transactions:**

- `BeginTransaction()` → transaction handle.
- Within a transaction: any combination of Get, Put, Delete, RangeScan, conditional operations.
- `Commit()` — atomically apply all mutations, or fail if conflicts are detected.
- `Abort()` — discard all mutations.

Transactions enable atomic multi-key operations: object write + delete-log entry + stats update in a single commit.

Transaction scope is always within a single cluster — cross-cluster atomicity is handled by RGW-level coordination (see Key Sharding).

---

## KV Schema

### Key Format

```
<prefix> <bucket_id> <object_name> <version_id>
```

- **prefix** — a short namespace identifier distinguishing object entries from bucket metadata, stats, and other KV schemas sharing the same store.
- **bucket_id** — binary bucket identifier. All objects in a bucket share the same prefix.
- **object_name** — the S3 object key, stored as raw bytes. Preserves natural lexicographic ordering.
- **version_id** — binary value using a descending scheme. The latest version sorts first, so a non-versioned GET reads the first key without scanning.

Key size: average ~256 bytes. Absolute max ~1040 bytes (1024-byte S3 name limit + 16 bytes for bucket_id and version_id, plus prefix).

Bucket/Realm/Zone metadata (name-to-id mapping, ACLs, policies, quota, versioning, lifecycle) is stored under separate prefixes in the same KV store.

### Value Structure

The KV value is a binary-encoded entry containing all metadata for the object.

Target size is ~1KB. Smaller values are accepted as-is. Larger values are split into a core value and an extended value.

**Core value** (always in the KV entry):

- Listing attributes — etag, last_modified, size, storage_class, owner, checksum_algorithm, content_type, content_encoding, content_language.
- RGW internal fields — state/flags, ref_tag, size_physical, chunk pointers. Each chunk pointer is a `(blob_id, offset, length)` tuple — no S3 names on the storage tier. Multiple S3 objects may share a blob_id (packing).
- Routing info for spillover metadata, when present.

**User-visible attributes** (in the KV entry when they fit):

- content_disposition, cache_control.
- User metadata (x-amz-meta-*) — up to 2KB per S3 limits.
- Object ACL overrides, checksum values, object lock.

User attributes are prioritized over read-path metadata for space in the value.
When user attributes push the value beyond ~2KB, they spill to the extended value.

**Read-path metadata** (in the KV entry when it fits):

- Manifest, compression info, encryption details.
- For most objects (single-part, small multipart), these fit comfortably.
- For large compressed multipart objects, they spill to the data header or extended storage.

To keep values compact, attributes are compressed using various techniques (e.g., binary mapping of repeated strings to short IDs, enum encoding of fixed-vocabulary fields).

### AWS S3 Object Tags

- Object-Tags are stored in a separate child KV (under the same shard) referenced from the S3-Object parent KV
- All Tags are stored together in a single KV
- AWS supports a maximum of 10 tags per object each with an aggregated size of 5,120 bytes

### S3 Delete Reference

On delete, we keep the original Key while replacing the live Value with a minimal delete reference containing:
- A delete flag.
- A timestamp.
- The ref_tag of the deleted object.

The ref_tag is retained so background cleanup can verify data ownership before freeing storage.
All other attributes are discarded.

RGW reads the entry, sees the delete reference, and returns 404.

### Extended Value

When metadata overflows the core KV entry, it is stored in an extended value.
The KV entry always contains routing info pointing to the extended value's location.

The extended value is a logical concept — its physical storage is pluggable. Possible implementations include:

- **Data header** — prepended to the first data chunk on the storage tier. Natural fit for RGW-controlled backends. For full-object GETs from offset 0, it is read as part of the first chunk at no extra cost. For byte-range reads, it requires reading chunk 0 separately (can be cached locally after first access).
- **Object annotations** — sidecar metadata attached to the data object by the storage tier.
- **Standalone files** — separate metadata files alongside the data.
- **Additional KV entries** — overflow stored in the same KV store under a related key.

The choice depends on the storage backend's capabilities.
POSIX systems, for example, cannot prepend headers to existing files and would use an alternative (discussed in the POSIX section).

---

## Operations

### HEAD

1. Translate bucket name to bucket_id (local cache).
2. Read the KV entry.
3. Return all S3-visible attributes from the value.

Single KV read. No data-tier access. Should be faster than the current model.\
The assumption here is that KV-Get is faster than Rados Read with its heavy stack.

### GET (Full Object)

1. Translate bucket name to bucket_id (local cache).
2. Read the KV entry.
3. Read data chunks from the storage tier using the chunk pointers.
5. Read extended-attributes if needed.
6. Extract ref_tag (verify against KV value), manifest, compression info.
7. Decompress/decrypt and stream to client.

The KV read is a new cost compared to the current model, where metadata is piggybacked on the data read.\
This is mitigated by the KV store's distributed cache serving hot entries from memory.

### GET (Byte-Range)

1. Read the KV entry.
2. If read-path metadata is in the KV value (common case): use it directly to locate the target chunk.
3. If not (large compressed multipart): read the extended attributes, or use a locally cached copy.
4. Read and decompress/decrypt the target byte range.

### PUT

1. Write data to the storage tier. The first chunk includes the data header.
2. Write the KV entry:
   - New object or overwriting a delete marker: write the new value.
   - Overwriting a live object: record old data references in the delete-log, then write the new value.

This is cheaper than the current model since no bucket-index update is needed.

### DELETE

1. Read the KV entry to get chunk pointers and ref_tag.
2. Record old data references in the delete-log.
3. Replace the value with a delete reference, or remove the entry.
4. Return success.

Data cleanup happens asynchronously via the delete-log.
This is cheaper than the current model since no bucket-index update is needed.

### LIST (ListObjectsV2)

1. Translate bucket name to bucket_id (local cache).
2. Range scan on the bucket_id prefix.
3. Filter out delete references.
4. Return up to 1000 keys with listing attributes from the KV values.

Pagination uses the last returned key as the continuation marker.

Should be cheaper than the current model with its excessive sharding and OMAP overhead.

---

## Caching

### What RGW Caches Locally
Locally cached metadata — created and removed, but never modified in place:
- **Bucket name → bucket_id mapping** — small, stable, fully cached.
- **Bucket metadata** — ACLs, policies, quota, versioning, lifecycle rules.
- **Realm/Zone metadata**
- **Attribute mapping tables** — binary mappings for repeated strings. Small, append-only, fully cached.
- **Read-path metadata** — for objects whose read-path metadata overflows the KV value. Cached after first access, validated against ref_tag on every use.

### What the KV Store Caches

Object KV entries are not cached locally on RGW servers.\
The KV store's own distributed cache serves frequently accessed entries from memory.

Keeping values small maximizes cache efficiency — more entries fit in the same amount of memory.


---

## Value Sizing

### Why 1KB is the Target

For a typical single-part object with no user metadata, the KV value contains:

- Listing attributes — etag, last_modified, size, storage_class, owner, content_type, checksum_algorithm.
- RGW internal fields — state/flags, ref_tag, chunk pointer(s).
- Read-path metadata — minimal for single-part (compression type, a few encryption fields).

With compact binary encoding, this fits comfortably under 1KB — including common user attributes.
The vast majority of objects in a large-scale system are single-part without unusually large user metadata — the 1KB target covers the common case.

Objects with very large user metadata (approaching the 2KB S3 limit) can push the value beyond 1KB, up to ~4KB.
This is acceptable — user attributes are prioritized for space in the value.

### Why Read-Path Metadata Can Grow Large

**Compression and multipart.**
Compressed multipart objects require a compression block table — a mapping from logical byte offsets to physical byte offsets.
Each block adds an entry.
For a large multipart object with many parts and many compression blocks, this table can grow to tens of KB.

**Encryption.**
Server-side encryption adds per-chunk metadata: key references, initialization vectors, cipher parameters.
For multipart objects with many parts, encryption metadata grows proportionally.

**Manifest.**
The manifest maps logical byte ranges to physical chunk locations.\
When chunks have equal size (except the last one) they can be summarized with a simple formula, but compression means that each and every chunk has different size so we need to list them all.\
For very large multipart objects with hundreds or thousands of parts, the manifest alone can grow to tens of KB — potentially up to multiple MB in extreme cases.

### The Case for Extended Values

This is why the extended value mechanism exists.

For the common case (single-part, small multipart, multipart with fixed chunk size), everything fits in the KV entry.\
For large compressed/encrypted multipart objects, the manifest and block table spill to the extended value — keeping the core KV entry small and the KV store's cache efficient.

The 8KB hard upper limit for the KV value is worth testing to determine the practical impact on cache efficiency and listing bandwidth.

---

## Stats and Quota

### Current Model

Bucket statistics (object count, total bytes) are maintained per-shard in the bucket-index.
Overall bucket stats are the sum across all shards.

This approach suffers from:
- **Drift.** Bucket-index update and head-object write are separate operations. A crash between them leaves stats inconsistent.
- **Expensive repair.** `radosgw-admin bucket check` must iterate all objects to recompute — impractical for large buckets.
- **Approximate quota.** Quotas based on potentially drifted stats may over-allow or wrongly reject requests.

### KV Model

Stats counters can be updated in the same transaction as the object metadata mutation.
No drift, no repair needed, accurate quota enforcement.

However, this is not a simple win.
Concurrent writes to a single bucket all update the same stats keys, creating contention:

- **FDB** — provides blind atomic add operations that merge without conflicting. Well suited for counters. But each counter must be a separate key, so every object write touches multiple unrelated keys (object entry + stats keys). Stats keys are hot and sticky in cache, so the extra writes are cheap in practice.

- **TiKV** — has no blind atomic add, only compare-and-swap. Under high concurrency to a single bucket, this leads to retry storms on stats keys.

Per-shard stats keys (one set per shard) reduce contention proportionally to shard count.
This is a concrete benefit of application-level sharding — discussed further in the sharding document.

---

## Key Sharding

The base key format places all objects in a bucket in a contiguous range:

```
<prefix> <bucket_id> <object_name> <version_id>
```

An alternative is to prepend a shard identifier — a cryptographic hash of `(bucket_id + object_name)`:

```
<prefix> <shard_id> <bucket_id> <object_name> <version_id>
```

The hash distributes objects uniformly across a fixed number of shards, breaking the contiguous bucket range into N disjoint ranges scattered across the key space.

This is a locality vs. distribution tradeoff.

Hashing exists to break locality — to take concentrated load on a local range and spread it uniformly across the system.\
But locality is exactly what makes range operations efficient.

Every benefit of hashing is a consequence of breaking locality.\
Every cost is also a consequence of breaking locality.

### Pros

**Fleet-level balancing without a fleet balancer.**

This is the primary reason to consider hashing.

When RGW scales beyond a single KV cluster to a fleet of independent KV clusters, there is no KV-store mechanism to balance data across clusters.\
Without hashing, bucket data is contiguous — buckets land on specific clusters and grow unevenly, creating imbalance over time.

The only fix would be RGW acting as a cross-cluster data balancer: detecting skew, migrating data between clusters.\
That is a massive engineering and operational burden.

With hashing, shards distribute objects uniformly.\
Assigning equal numbers of shards to each cluster produces a balanced fleet by construction.\
Adding a new cluster means reassigning some shards — no data-level rebalancing within existing clusters.

**Secondary benefits within a single cluster.**

Hashing provides additional benefits that partially offset its costs:

- Evenly balanced write activity — writes to a hot bucket don't concentrate on a few partitions.
- Less internal rebalancing — data is pre-distributed, so the DB's own range-splitting triggers less frequently.
- More predictable performance — fewer rebalance sessions competing with foreground I/O.

These are real but not sufficient on their own to justify hashing. The cons outweigh them at the single-cluster level — which is the reason KV stores don't use hashing internally. They use ordered range-partitioning precisely to preserve locality.

### Cons

**Listing becomes fundamentally harder.**

Without hashing, `ListObjectsV2` is a single ordered range scan. The KV store returns keys in the lexicographic order S3 requires. Pagination is a single cursor. Memory usage is minimal.

With hashing, listing requires:

- N concurrent range scans, one per shard.
- Merge-sort across all N streams to reconstruct lexicographic order.

Strictly harder — more code, more network round-trips, more memory, more failure modes.

**Memory cost of sharded listing.**

Without hashing, listing 1000 objects is a single range-read of ~1.5MB of sequential data, loaded directly into the caller. These entries are unlikely to be accessed again soon, so the read does not pollute the DB cache.

With hashing (e.g., 128 shards), each shard contributes on average ~8 entries — a mere ~12KB read per shard. These are tiny, inefficient requests.

The alternative is to over-fetch from each shard and buffer the excess in RGW memory, consuming N times more memory than the unsharded case.

**Range operations are degraded.**

Any operation that benefits from key contiguity within a bucket is broken by hashing:

- **Range-delete** — deleting all objects in a bucket (or under a prefix) becomes N separate range-deletes across scattered ranges, losing atomicity.

- **Range-rename** — applications like Apache Spark and Hive rename entire directory trees. With hashing, each object maps to a different shard — the rename is N scattered point operations with no locality benefit.

**Mapping table must be fully pinned.**

With locality, accessing a bucket touches a small number of contiguous ranges mapped to a small number of servers. The range-to-server mapping table benefits from caching — hot buckets mean hot ranges.

With hashing, any bucket operation potentially touches every server. The mapping table cannot benefit from caching because access patterns have no locality. The full table must be pinned in memory on every RGW instance, and it scales with fleet size.

**Region hibernation is disabled (TiKV-specific).**

TiKV supports region hibernation — inactive regions stop sending Raft heartbeats, reducing CPU on both TiKV nodes and PD. With locality, cold buckets mean cold regions that go dormant.

With hashing, every region contains fragments of many buckets. Even one active bucket touches all regions. All regions stay awake, broadcasting constant Raft heartbeats. Baseline CPU consumption scales with total region count regardless of actual workload.

**Page-cache waste (RocksDB / TiKV-specific).**

RocksDB caches data in 4KB pages of sequential KV entries. With locality, a page fetch brings in neighbors likely needed next — e.g., adjacent listing entries.

With hashing, adjacent keys on a page belong to unrelated buckets. A typical KV entry is ~1.5KB, so roughly 60% of every cached page is wasted.

FDB caches per KV entry, so hashing does not cause cache waste there.

### Assessment

Hashing is a fleet-level concern, not a single-cluster optimization.

Within a single cluster, the KV store's own auto-sharding handles distribution.\
The costs of hashing — degraded listings, lost range operations, cache waste, disabled hibernation, pinned mapping tables — outweigh the secondary benefits.

For deployments that require a fleet of independent KV clusters, hashing becomes necessary because no KV-store mechanism exists to balance across clusters.\
The secondary benefits — balanced activity, less rebalancing, predictable performance — partially offset the costs, but do not eliminate them.

### Fleet Scaling

Every KV store has a single-cluster capacity ceiling.\
FDB clusters hold approximately 64 billion KV entries (potentially 128 billion).\
TiKV clusters can scale to approximately 1 trillion KV entries (potentially more).

These are large numbers, but **some** production systems may eventually outgrow them.
- The KV store design allocates one extra KV per object to store S3 Object Tags (when present) which can double the KV count
- Object Annotations allow up to 1000 Annotations per Object, and since we use a standalone KV for each Annotation we can have an effective 1 Trillion KV on a system with a mere 1 Billion S3-Objects.

When a single cluster is no longer sufficient, the system must scale out to a fleet of independent KV clusters.\
There is no cross-cluster balancing built into any KV store — the fleet management is RGW's responsibility.

Hashing is the simplest method to support a fleet. The shard prefix distributes data uniformly across clusters, and shard migration provides the growth path.

However, hashing is not the only option. Three strategies exist, each with different trade-offs.

**Strategy 1 — Object-level hashing.**

`shard_id = hash(bucket_id + object_name)`.

Objects within a bucket are scattered across all shards and all clusters.\
Works for any workload — fleet balance is guaranteed by the hash.\
Pays the full locality cost:
- merge-sort listings
- degraded range operations
- pinned mapping tables
- cross-cluster coordination for multi-key operations

The simplest strategy. The most expensive in day-to-day operation.

**Strategy 2 — RGW as fleet rebalancer (no hashing).**

- Keys remain ordered
- No shard prefix
- RGW detects imbalance across clusters and migrates data ranges to restore balance

This requires RGW to build and operate a cross-cluster data balancer — detecting skew, selecting ranges to move, coordinating live migration, handling failures.\
A massive engineering and operational burden. Unattractive.

**Strategy 3 — Bucket-level hashing with fleet-friendly buckets.**

`shard_id = hash(bucket_id)`.

All objects in a bucket share the same shard and land on the same cluster. Intra-bucket locality is fully preserved — listing is a simple range scan, range-delete and range-rename work naturally, no merge-sort.

Fleet balance depends on customers creating many smaller, similarly-sized buckets. The hash distributes buckets evenly across clusters. If one bucket grows disproportionately, its cluster becomes hot — there is no automatic fix without falling back to object-level hashing.

Viable for controlled environments where customers can design their bucket layout.

### Cross-Cluster Coordination Cost

On a single cluster, any set of KV mutations can be committed in one transaction.\
On a fleet, mutations landing on different clusters cannot share a transaction — no KV store supports cross-cluster transactions.

Operations where the source and destination keys hash to different shards require RGW-level coordination:
- two separate transactions
- crash recovery logic
- and potential inconsistency windows.

This is another reason to minimize the number of shards to the absolute minimum needed for fleet balance. Fewer shards means fewer cross-cluster operations. Growing from 2 shards to 4 is far less disruptive than jumping to 128.

With bucket-level hashing, most operations stay within a single bucket and therefore a single cluster — cross-cluster coordination is only needed for cross-bucket operations, which are rare in S3.

### Key Naming and Transaction Locality

Hashing-aware key schemes minimize cross-cluster operations by ensuring that logically related KV entries hash to the same shard.\
This maximizes single-cluster transactions.\
Only operations involving two distinct object identities require cross-cluster coordination.

**Design principle:**\
A KV entry logically tied to a specific object should derive its shard from the parent KV.\
The child KV should **logically** be\
`<prefix> <shard_id> <bucket_id> <object_name>::<child-suffix>`\
This way it will always follow the same shard as the parent KV

We can replace the `<object_name>` (up to 1024 Bytes long) on child KV with an 8-byte `<parent-object-id>` which must be stored in the parent KV and used to construct the child KV.\
Alternatively, we can use a 32-64 byte cryptographic hash as an `<parent-object-id>` saving the need to query the parent KV before accessing the child KV

### Entries Sharing the Parent Shard:

**All versions of an object.**

Every version — current, prior, and delete markers — shares the same `(bucket_id, object_name)` and therefore the same shard. Creating a new version and invalidating the old one is a single local transaction. This eliminates the need for OLH (Object Lifecycle Head) as a separate coordination object.

**Delete markers.**

In S3 versioned buckets, deleting an object creates a delete marker — a lightweight version entry that makes the object appear deleted without removing its data. A delete marker is just another version of the same object. It hashes to the same shard. Creating the delete marker and updating version state is atomic within one transaction.

**Multipart upload state.**

Upload metadata (upload_id, part list) and the final committed object all relate to the same target `(bucket_id, object_name)`. If multipart state keys derive their shard from the target object's identity, all multipart operations — initiate, upload parts, complete, abort — are local. CompleteMultipartUpload (commit parts + write final object + clean up part entries) is a single transaction.

**Extended value / spillover entries.**

When object metadata overflows the core KV entry, spillover entries must use the same key prefix with a distinguishing suffix. This ensures they hash to the same shard. Writing the core entry and its spillover is a single transaction.

**Delete-log entries.**

When an object is overwritten or deleted, the old chunk pointers must be recorded for async data cleanup. The delete-log entry captures which storage-tier blobs to free later. If the delete-log key derives its shard from the same `(bucket_id + object_name)`, the object mutation and the delete-log write are a single transaction — guaranteeing no orphan window. Every overwrite or delete atomically records what to clean up.

**Object tags, ACLs, and lock metadata.**

S3 supports per-object tags (up to 10 key-value pairs) and per-object ACL overrides (up to 100 grants)\
These are typically stored inline in the KV value.\
If they ever spill to separate KV entries — due to size or access pattern optimization — deriving the shard from the same `(bucket_id + object_name)` keeps them co-located.\
PutObjectTagging or PutObjectAcl are transactional with the object's metadata.

- Object-Tags are probably best kept in a standalone child KV since they are individually mutable
- ACL can be stored inside the parent KV or spillover to extended-metadata since they are overwritten together

**Per-shard stats counters.**

If each shard maintains its own stats keys on the same cluster, the object write and the stats update share a transaction. No drift, no separate counter reconciliation.

**Inherently cross-cluster — cannot be solved by naming:**

- **Server-side copy** — source and destination are different object names, different hashes, potentially different clusters. Always requires RGW coordination.

- **Rename** — different name means different hash, potentially different cluster.

- **Cross-bucket operations** — any operation touching objects in different buckets may land on different clusters.


### Logical Hashing Epoch

A deployment does not need to choose its final shard count upfront.

The system supports a **logical hashing epoch** — a global counter that defines the current shard count. Each epoch transition increases the shard count, and the same migration protocol runs each time.

**Epoch 0** — `SHARD_COUNT = 1`. All keys get `shard_id = 0`. Effectively no hashing. The entire keyspace is one contiguous range on one cluster. Simple listing, simple everything. The key still carries the `shard_id` prefix, but it is always 0.

**Epoch 1** — when the cluster reaches a capacity threshold (e.g., 80% full), the admin triggers a reshard. `SHARD_COUNT` increases — for example, to 2. Existing keys are re-keyed with their new `shard_id`. From this point, the fleet growth path is available — shards can be distributed across clusters.

**Epoch N** — further reshards as the system grows. Each transition increases the shard count: 1 → 2 → 4 → 8 → 16 → ... The customer controls when to reshard and by how much.

This means the listing complexity grows gradually, not all at once. With 2 shards, listing is a 2-way merge — barely noticeable. With 4, still manageable. The customer pays only the complexity their current scale demands.

**Resharding mechanism.**

Resharding uses the same watermark-based background migration protocol as directory rename:

- A **background worker** scans range-by-range, re-keying each entry from the old `shard_id` to the new `shard_id`. The watermark advances through the keyspace as processing progresses.

- **On-access reshard** — when a client writes to a key above the watermark (not yet resharded by the background worker), RGW reshards that entry on the fly, writing it with the new `shard_id`.

- **GETs** check the watermark. Below the watermark — look in the new shard. Above the watermark — look in the old shard. A false negative on a race condition (watermark just advanced but the gateway's cached copy is stale) is fixed with a watermark reload and a retry using the new shard.

- **Per-bucket progression** is natural. In epoch 0, each bucket is a contiguous range. The migration can be ordered by bucket, allowing admins to prioritize — e.g., reshard daytime-active buckets during the night.

**Fleet-wide coordination.**

Resharding is a fleet-wide operation — the shard count is a global constant, so changing it affects every cluster. RGW coordinates the transition:

- **Sequential** — reshard one cluster at a time. Lower system load, safer, but slower. Suitable during heavy production traffic.

- **Parallel** — reshard multiple clusters concurrently. Faster overall, but higher I/O pressure across the fleet. Suitable during maintenance windows or low-traffic periods.

Each cluster's migration is independent, with its own watermark progress. RGW ensures all gateways are aware of the new epoch before any migration begins.

---

## Deployment

The KV store is a separate service from the data store.
RGW connects to it via a client library or network protocol — the exact mechanism depends on the KV system chosen.

### Ceph-Integrated

For RGW deployments backed by Ceph RADOS:
- KV store runs on dedicated NVMe partitions on existing OSD nodes, separate from BlueStore.
- Reuses existing infrastructure — no new hardware required.
- Failure domains can be aligned with Ceph's (rack/host).

### Standalone

For RGW deployments without Ceph, or where the KV store is managed independently:
- KV store runs on RGW nodes with local NVMe, or on dedicated metadata nodes.
- The data store can be any backend that supports write, read-range, and punch-hole — RADOS, a distributed filesystem, cloud storage, or other.

