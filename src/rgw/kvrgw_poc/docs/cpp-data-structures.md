# C++ Data Structures

All data structures used by the KVRGW C++ backend. Packed structs use `#pragma pack(1)` — no padding. Multi-byte fields stored big-endian in KV (converted via `htobe64`/`htonl`/`htons` in constructors or `hdr_to_be`/`hdr_from_be`).

## RefTag Type

Defined in `backend/src/ref_tag.hpp`:

```cpp
using RefTag = std::array<uint8_t, 12>;

std::string_view ref_tag_view(const RefTag& tag);  // reinterpret_cast, no copy
```

Replaces `std::string ref_tag` in parse structs (`PoKeyParts`, `GoKeyParts`, `DKeyParts`), `BatchCommitEntry`, `PutObjectRequest`, and `PutInTxnParams`.

---

## Constants

Defined in `backend/src/constants.hpp`:

| Name | Value | Description |
|------|-------|-------------|
| `kRefTagSize` | 12 | ref_tag binary size (bytes) |
| `kEtagSize` | 16 | MD5 digest size (bytes) |
| `kMaxContentTypeLen` | 255 | Max content-type string length |
| `kShardCount` | 1 | Fixed shard count (POC) |
| `kShardId` | 0 | Fixed shard id (POC) |
| `kNamespaceObject` | 'S' | S3 objects namespace |
| `kNamespacePending` | 'P' | Pending operations namespace |
| `kNamespaceGc` | 'G' | GC namespace |
| `kNamespaceData` | 'D' | Tier 2 KV data namespace |
| `kNamespaceBucket` | 'B' | Bucket metadata namespace |
| `kNamespaceTenant` | 'T' | Tenant metadata namespace |
| `kNamespaceLocal` | 'L' | Counters / system maps namespace |
| `kDefaultMaxInline` | 0 | Default max inline size (binary default) |
| `kDefaultMaxKvStore` | 0 | Default max KV store size (binary default) |
| `kDefaultKvStoreCoalescing` | false | Default coalescing flag |
| `kBucketCacheTtl` | 3s | Bucket cache entry TTL |
| `kDenyRead` | 0x01 | access_flags bit: deny GET/HEAD |
| `kDenyWrite` | 0x02 | access_flags bit: deny PUT/DELETE |
| `kDenyList` | 0x04 | access_flags bit: deny ListObjects |
| `kDenyDeleteBucket` | 0x08 | access_flags bit: deny DeleteBucket |
| `kNamespaceRef` | 'R' | Ref-count namespace (CopyObject data sharing) |
| `kNullVersion` | 0xFFFFFFFF | Null version ID (unversioned or suspended) |
| `kFirstVersionId` | 0xFFFFFFFE | First real version ID (decrementing per key) |
| `kFlagSharedData` | 0x04 | ObjectValue flags bit: data shared via CopyObject |
| `kFlagExternalTags` | 0x08 | ObjectValue flags bit: tags stored in C:T child KV |
| `kFlagExternalAnnotations` | 0x10 | ObjectValue flags bit: annotations stored in child KV |
| `kCategoryChild` | 'C' | Child KV category byte |
| `kChildTypeTags` | 'T' | Child type suffix for tags |
| `kChildTypeAnnotation` | 'A' | Child type suffix for annotations (future) |
| `kChildTypeExtended` | 'E' | Child type suffix for extended values (future) |
| `kMaxTags` | 10 | Max S3 tags per object |
| `kMaxTagKeyLen` | 128 | Max tag key length (bytes) |
| `kMaxTagValueLen` | 256 | Max tag value length (bytes) |
| `kMaxTagPayload` | 5120 | Max aggregate tag payload (bytes) |
| `kMaxInlineTagPayload` | 256 | Max tag payload for inline storage |
| `kMaxBatchSize` | 16 | Max entries per batch commit (moved from KvRgwServiceImpl class) |

### KvrgwErrorCode enum

Defined in `proto/kvrgw.proto`, imported via `error_codes.hpp`. All values prefixed `KVRGW_ERR_*`.

| Range | Category | Values |
|-------|----------|--------|
| 0 | Success | `KVRGW_ERR_OK` |
| 100–110 | S3 errors | `NO_SUCH_KEY`, `NO_SUCH_BUCKET`, `BUCKET_ALREADY_EXISTS`, `BUCKET_NOT_EMPTY`, `PRECONDITION_FAILED`, `ACCESS_DENIED`, `INVALID_ARGUMENT`, `INVALID_BUCKET_NAME`, `NO_SUCH_VERSION`, `NO_SUCH_TENANT` |
| 200–206 | FDB retriable | `FDB_CONFLICT`, `FDB_PROCESS_BEHIND`, `FDB_FUTURE_VERSION`, `FDB_TRANSACTION_TOO_OLD`, `FDB_NOT_COMMITTED`, `FDB_COMMIT_UNKNOWN`, `FDB_CLUSTER_VERSION_CHANGED` |
| 300–302 | FDB permanent | `FDB_KEY_TOO_LARGE`, `FDB_VALUE_TOO_LARGE`, `FDB_TRANSACTION_TOO_LARGE` |
| 400–405 | Internal | `INTERNAL`, `CORRUPT_VALUE`, `BUCKET_ID_MISMATCH`, `VALUE_TOO_LARGE`, `TRANSACTION_CONFLICT`, `MAX_RETRIES_EXCEEDED` |

### FDB Named Constants

Defined in `kvrgw::fdb::` namespace in `error_codes.hpp`. Replaces magic numbers throughout the codebase.

```cpp
namespace kvrgw::fdb {
  constexpr fdb_error_t kConflict              = 1020;
  constexpr fdb_error_t kProcessBehind         = 1037;
  constexpr fdb_error_t kFutureVersion         = 1009;
  constexpr fdb_error_t kTransactionTooOld     = 1007;
  constexpr fdb_error_t kCommitUnknownResult   = 1021;
  constexpr fdb_error_t kClusterVersionChanged = 1039;
  constexpr fdb_error_t kKeyTooLarge           = 2102;
  constexpr fdb_error_t kValueTooLarge         = 2103;
  constexpr fdb_error_t kTransactionTooLarge   = 2101;
}
```

### ErrorStats

Defined in `admin_server.hpp`. Atomic counters per error code, O(1) indexed by enum value.

```cpp
struct ErrorStats {
  std::atomic<int64_t> counts[KvrgwErrorCode_ARRAYSIZE]{};

  void record(KvrgwErrorCode code);
  int64_t get(KvrgwErrorCode code) const;
  void reset();
};
```

Admin socket commands: `get-error-stats` (sparse dump of non-zero counts), `reset-error-stats` (dump + zero all).

### VersioningState enum

```cpp
enum VersioningState : uint8_t {
  VERSIONING_DISABLED  = 0,
  VERSIONING_ENABLED   = 1,
  VERSIONING_SUSPENDED = 2,
};
```

Stored in bucket value byte 17. Controlled via PutBucketVersioning.

### Versioning helper structs

```cpp
struct NewVersionIds {
  uint32_t version_id;  // assigned to this write
  uint32_t next_vid;    // stored in header for next write's sequence
};

struct BktVerifyResult {
  std::string bucket_id;
  uint8_t versioning_state{};
};
```

Returned by `verify_bucket_in_txn()` and `resolve_bucket_verify()`.

---

## KV Value Structs

### ObjectValueHeader (62 bytes)

Stored as the O: value in FDB (followed by variable-length content_type + inline_data + tags + metadata).

```cpp
struct ObjectValueHeader {
  uint8_t ref_tag[kRefTagSize];   // offset 0:  unique write instance id
  uint16_t etag_part_count;       // offset 12: 0=single-part, >0=multipart
  uint16_t annotations_count;     // offset 14: annotation count

  uint8_t etag[kEtagSize];        // offset 16: MD5 digest (binary)

  uint64_t size;                  // offset 32: object byte count
  uint32_t last_modified_sec;     // offset 40: creation time (seconds)
  uint32_t last_modified_nsec;    // offset 44: creation time (nanoseconds)

  ChunkDescriptor chunk;          // offset 48: data tier type
  uint8_t flags;                  // offset 49: bit flags (see below)
  uint8_t tags_count;             // offset 50: S3 tag count (max 10)
  uint8_t content_type_len;       // offset 51: length of content_type string

  uint32_t version_id;            // offset 52: version ID (kNullVersion or real vid)
  uint32_t next_vid;              // offset 56: next version_id to assign

  uint16_t metadata_count;        // offset 60: user metadata entry count (x-amz-meta-*)
};
// sizeof == 62
```

Full O: value = `ObjectValueHeader` + `content_type[0..content_type_len]` + `inline_data[0..size]` (INLINE only) + `tag_payload` (if inline tags) + `metadata_payload` (if metadata_count > 0).

Flags byte: `bit 0 (0x01)` = `kFlagExtendedAttrs`, `bit 1 (0x02)` = `kFlagFenced` (delete marker), `bit 2 (0x04)` = `kFlagSharedData` (data shared via CopyObject), `bit 3 (0x08)` = `kFlagExternalTags` (tags in C:T child KV), `bit 4 (0x10)` = `kFlagExternalAnnotations` (annotations in child KV).

### GcValueHeader (14 bytes)

Stored as the G:O value in FDB. Copied from O: at delete/overwrite time.

```cpp
struct GcValueHeader {
  ChunkDescriptor chunk;          // offset 0: tier type (copied from O:)
  uint8_t flags;                  // offset 1: flags (kFlagSharedData for ref-counted entries)
  uint64_t object_size;           // offset 2: object byte count
  uint32_t mtime;                 // offset 10: last_modified_sec (for D: key reconstruction)
};
// sizeof == 14
```

### PoValueHeader (12 bytes)

Stored as the P:O value in FDB. Written at Phase 1 of STORAGE tier PUT.

```cpp
struct PoValueHeader {
  uint64_t estimated_size;        // offset 0: content-length from client
  uint32_t created_at_unix;       // offset 8: Phase 1 commit time (seconds)
};
// sizeof == 12
```

### BucketValueHeader (18 bytes)

Stored as the B: value in FDB (followed by variable-length policy JSON).

```cpp
struct BucketValueHeader {
  uint8_t bucket_id[8];           // offset 0: binary bucket identifier
  int64_t created_at_unix;        // offset 8: bucket creation time
  uint8_t access_flags;           // offset 16: bitmask (kDenyRead|kDenyWrite|kDenyList|kDenyDeleteBucket)
  uint8_t versioning_state;       // offset 17: VersioningState enum (0=disabled, 1=enabled, 2=suspended)
};
// sizeof == 18
```

Full B: value = `BucketValueHeader` (17B) + optional policy JSON (variable length).

### ChunkDescriptor (1 byte)

Identifies where object data is stored. Copied as-is into G:O value.

```cpp
enum ChunkType : uint8_t {
  CHUNK_INLINE       = 'I',  // data in O: value
  CHUNK_CHILD_D      = 'D',  // data in D: namespace (original owner)
  CHUNK_CHILD_D_REF  = 'd',  // data in D: namespace (shared via CopyObject — points to another bucket's D:)
  CHUNK_STORAGE      = 'S',  // data in local filesystem blob (original owner)
  CHUNK_STORAGE_REF  = 's',  // data in local filesystem blob (shared via CopyObject — R: ref-counted)
};

struct ChunkDescriptor {
  ChunkType type;
};
// sizeof == 1
```

---

## Key Structs

### KeyBuf (stack buffer, 1100 bytes max)

Zero-allocation key construction buffer.

```cpp
struct KeyBuf {
  static constexpr size_t kMaxSize = 1100;
  uint8_t data[kMaxSize];
  size_t len;

  void set_header(const H& hdr);    // memcpy packed header
  bool append(const void* src, n);   // append variable tail, returns false on overflow
  std::string_view view() const;     // for KV store API
};
```

### KeyHeaderS (14 bytes) — S: and P: namespaces

```cpp
struct KeyHeaderS {
  char ns;                // 'S' or 'P'
  uint16_t shard_count;   // BE
  uint16_t shard_id;      // BE
  uint8_t bucket_id[8];   // raw binary
  char cat;               // 'O' (object) or op_type for P:
};
```

Variable tail: `object_name` (S:O, P:O) + `ref_tag` (P:O only).

### KeyHeaderG (15 bytes) — G: namespace

```cpp
struct KeyHeaderG {
  char ns;                // 'G'
  uint8_t size_tier;      // log2-based tier for GC ordering
  uint16_t shard_count;   // BE
  uint16_t shard_id;      // BE
  uint8_t bucket_id[8];   // raw binary
  char cat;               // 'O'
};
```

Variable tail: `ref_tag` (12B, fixed).

### KeyHeaderD (31 bytes) — D: namespace (fixed, no variable tail)

```cpp
struct KeyHeaderD {
  char ns;                // 'D'
  uint16_t shard_count;   // BE
  uint16_t shard_id;      // BE
  uint8_t bucket_id[8];   // raw binary
  uint8_t size_tier;      // floor(log2(size))
  uint8_t hash_prefix;    // FNV-1a(ref_tag) % 32
  uint32_t mtime;         // BE, creation time (seconds)
  uint8_t ref_tag[12];    // raw binary
};
```

### KeyHeaderB (5 bytes) — B: namespace

```cpp
struct KeyHeaderB {
  char ns;                // 'B'
  uint32_t tenant_id;     // BE
};
```

Variable tail: `bucket_name`.

### KeyHeaderL (2 bytes) — L: namespace

```cpp
struct KeyHeaderL {
  char ns;                // 'L'
  char type;              // 'N' (counter) or 'I' (id map)
};
```

Variable tail: `name` (1-64 bytes).

---

## OValueBuf (stack buffer, 827 bytes max)

Zero-allocation O: value construction buffer.

```cpp
struct OValueBuf {
  static constexpr size_t kMaxSize = sizeof(ObjectValueHeader) + kMaxContentTypeLen + 256 + kMaxInlineTagPayload;
  uint8_t data[kMaxSize];
  size_t len;

  bool set_header(const ObjectValueHeader& hdr);
  bool append(const void* src, size_t n);
  std::string_view view() const;
};
```

Usage: `set_header(wire)` + `append(content_type)` + `append(inline_data)` + `append(tag_payload)` (if inline tags).

---

## Runtime Config Structs

### TierConfig

```cpp
struct TierConfig {
  uint32_t max_inline;
  uint32_t max_kv_store;
  bool kv_store_coalescing;
  int batch_size{1};         // PUT batch coalescing: objects per FDB txn (1 = disabled)
  int batch_timeout_us{1000}; // max wait before flushing partial batch (microseconds)
  int batch_threads{8};       // committer threads in BatchCommitQueue pool
};
```

### GcPolicy

```cpp
struct GcPolicy {
  bool suspended;
  int interval_sec;
  int max_objects_per_sec;
  int max_mb_per_sec;
};
```

Both use double-buffer state classes (`TierConfigState`, `GcConfigState`) for lock-free online updates via admin socket.

---

## PUT Pipeline Structs

### PutObjectRequest

Input to `put_object_route()` — shared entry point for gRPC PutObject and perf driver.

```cpp
struct PutObjectRequest {
  uint32_t tenant_id;
  std::string bucket_name;
  std::string object_name;
  RefTag ref_tag;
  ObjectValue value;
  uint64_t estimated_size;
  PutCondition* cond;
  PreparedTags* tags;
};
```

### PutObjectResult

Return type of `put_object_route()`.

```cpp
struct PutObjectResult {
  KvrgwErrorCode error_code{KVRGW_ERR_OK};
  std::string etag;
  uint32_t version_id{0};
};
```

### PutInTxnParams

Parameters for `put_object_in_txn()` — the shared transaction body used by `put_object_single_txn`, `put_object_phase3`, and `commit_batch`.

```cpp
struct PutContext {
  FdbFuture f_bkt;               // bucket read future (if has_bucket_future)
  FdbFuture f_obj;               // S:O read future
  FdbFuture f_po;                // P:O read future (single-mode storage tier only)
  bool has_bucket_future{false};
  bool is_storage_tier{false};
  KeyBuf object_key;             // 1100B stack buffer (no heap)
};

struct VerifiedBucket {
  uint32_t tenant_id{};
  const std::string* bucket_name{nullptr};
  BucketState state;
};

struct PutInTxnParams {
  uint32_t tenant_id;
  const std::string& bucket_name;
  const std::string& bucket_id;
  const std::string& object_name;
  const RefTag& ref_tag;
  ObjectValue& value;
  const std::string* data;
  const PutCondition* cond;
  const PreparedTags* tags;
  bool skip_bucket_verify;   // true when bucket already verified in this batch
  bool is_storage_tier;      // true for STORAGE (P:O already written)
};
```

`PutContext` and `VerifiedBucket` are stack-allocated arrays in `commit_batch` (up to `kMaxBatchSize=16`, defined in `constants.hpp`). `put_finalize` does linear scan of the `VerifiedBucket` array instead of `unordered_map` lookup.

### BatchCommitEntry

One PUT operation queued for batch commit. Carries all data needed to call `put_object_in_txn()`.

```cpp
struct BatchCommitEntry {
  uint32_t tenant_id;
  std::string bucket_name;
  std::string object_name;
  RefTag ref_tag;
  ObjectValue value;
  std::string data;
  bool is_storage_tier{false};
  std::string if_match;
  std::string if_none_match;
  std::chrono::steady_clock::time_point enqueued_at;
  std::promise<KvrgwErrorCode> promise;
};
```

### BatchStats

Atomic counters for batch queue observability. Reset on each `batch-stats` query.

```cpp
struct BatchStats {
  std::atomic<int64_t> batch_commits{0};
  std::atomic<int64_t> total_entries_batched{0};
  std::atomic<int64_t> conflict_pushbacks{0};
  std::atomic<int64_t> min_batch_size{0};
  std::atomic<int64_t> max_batch_size{0};
  std::atomic<int64_t> total_wait_us{0};
  std::atomic<int64_t> min_wait_us{0};
  std::atomic<int64_t> max_wait_us{0};
  std::atomic<int64_t> total_queue_size_at_extract{0};
};
```

`total_queue_size_at_extract` accumulates `pending_.size()` each time a worker extracts entries. `avg_queue_size = total_queue_size_at_extract / batch_commits`.

---

## Bucket Policy Structs

### BucketValue (parsed)

In-memory representation after reading B: from FDB.

```cpp
struct BucketValue {
  uint64_t bucket_id;
  int64_t created_at_unix;
  uint8_t access_flags;
  std::string policy_json;        // empty if no policy set
};
```

### BucketCacheEntry

Cached per-bucket state (TTL = `kBucketCacheTtl`).

```cpp
struct BucketCacheEntry {
  uint64_t bucket_id;
  uint8_t access_flags;
  std::chrono::steady_clock::time_point cached_at;
};
```

### BucketState

Returned by `read_bucket_state()` — full bucket metadata from a fresh FDB read.

```cpp
struct BucketState {
  uint64_t bucket_id;
  int64_t created_at_unix;
  uint8_t access_flags;
  std::string policy_json;
};
```

### TxnStats

```cpp
struct TxnStats {
  int attempts;
  int conflicts;
};
```

### TxnRetryPolicy

```cpp
struct TxnRetryPolicy {
  int max_retries = 10;
};
```

---

## Bucket Policy Functions

| Function | Signature | Purpose |
|----------|-----------|---------|
| `read_bucket_state` | `(tenant_id, bucket_name) → expected<optional<BucketState>, fdb_error_t>` | Fresh FDB Get(B); replaces `resolve_bucket_id` |
| `check_access` | `(flag) → KvrgwErrorCode` | Cache-first check; refresh-before-reject on deny |
| `verify_bucket_in_txn` | `(tr, tenant, bucket, id, flag) → expected<void, fdb_error_t>` | In-txn bucket existence + access check |
| `parse_policy_flags` | `(policy_json) → uint8_t` | Extract access_flags bitmask from policy JSON |
| `extract_access_flags` | `(B value bytes) → uint8_t` | Read access_flags from raw B: value at offset 16 |

**Removed:** `resolve_bucket_id` — replaced by `read_bucket_state`.

---

## KvStore::range_scan — disable_ryw flag

```cpp
std::expected<std::vector<RangeScanResult>, fdb_error_t> range_scan(
    std::string_view start, std::string_view end, int limit,
    bool disable_ryw = false);
```

**What RYW cache is:** FDB's client-side Read-Your-Writes cache stores all read results within a transaction in an in-process cache. Subsequent reads of the same key within that transaction return the cached copy, including any uncommitted writes. The cache is built automatically on every read and freed when the transaction is destroyed.

**Why listing doesn't need it:** Listing operations (ListObjects, ListBuckets, ListObjectVersions) are read-only, forward-only sequential scans that never revisit a key. Each `range_scan` call creates a transaction, reads one page, and destroys the transaction. Every entry cached by the RYW layer is dead weight — allocated, populated, and discarded without ever being consulted.

**What the flag does:** When `disable_ryw=true`, the implementation sets `FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE` on the FDB transaction before the `GetRange` call. This tells the FDB client library to skip building the RYW cache entirely — no allocation, no population, no deallocation.

**When to use:** Read-only scans that never read their own writes. All four listing call sites pass `true`. Must NOT be used on transactions that mix reads and writes (PUT, DELETE, etc.) where reading back uncommitted writes is required for correctness.

**Performance impact:** Eliminates per-page RYW cache allocation/population/deallocation overhead on listing scans over large datasets.

---

## In-Memory Working Structs

### ObjectValue

Used during request processing. Not directly written to KV — converted to wire format via `OValueBuf`.

```cpp
using MetadataMap = std::vector<std::pair<std::string, std::string>>;

struct ObjectValue {
  ObjectValueHeader hdr;
  std::string content_type;
  std::vector<uint8_t> inline_data;

  static constexpr uint8_t kFlagExtendedAttrs        = 0x01;
  static constexpr uint8_t kFlagFenced               = 0x02;
  static constexpr uint8_t kFlagSharedData           = 0x04;
  static constexpr uint8_t kFlagExternalTags         = 0x08;
  static constexpr uint8_t kFlagExternalAnnotations  = 0x10;

  bool has_extended_attrs() const;
  bool has_annotations() const;
  bool has_external_annotations() const;  // (flags & kFlagExternalAnnotations) && annotations_count > 0
  bool is_delete_marker() const;
  bool has_shared_data() const;
  bool has_external_tags() const;
  bool has_data() const;
  bool has_metadata() const;              // hdr.metadata_count > 0

  std::string chunk_data_bucket_id;        // for CHILD_D_REF: owner bucket_id
  uint8_t chunk_data_ref_tag[kRefTagSize]; // for CHILD_D_REF: owner ref_tag

  TagSet tags;             // inline tags (when tags_count > 0 && !kFlagExternalTags)
  bool tags_external;      // true when tags stored in C:T child KV
  MetadataMap metadata;    // user metadata (x-amz-meta-*), sorted lexicographically
};
```

### TagSet and Tag Encoding

Defined in `backend/src/tag_value.hpp`. Tags are stored as binary-packed key/value pairs.

```cpp
using TagSet = std::vector<std::pair<std::string, std::string>>;
```

Binary format (same for inline O: tail and external C:T value):
```
For each tag (tags_count entries):
  [key_len   2B BE]   (1-128)
  [key_bytes N B]
  [val_len   2B BE]   (0-256)
  [val_bytes N B]
```

Helper functions:

| Function | Purpose |
|----------|---------|
| `encode_tag_payload(tags)` | TagSet -> binary payload |
| `decode_tag_payload(raw, count)` | binary payload -> TagSet |
| `compute_tag_payload_size(tags)` | Compute encoded size without allocating |
| `encode_metadata(meta)` | MetadataMap -> binary payload (same format as tags) |
| `decode_metadata(raw, count)` | binary payload -> MetadataMap |

Storage model: inline when encoded payload <= 256B (`kMaxInlineTagPayload`), external C:T when larger.

### PreparedTags

Result of validating and encoding proto tags. Used by `PutObject` (inline tags via `x-amz-tagging`) and `PutObjectTagging`.

```cpp
struct PreparedTags {
  TagSet tags;              // validated, sorted lexicographically by key
  std::string encoded;      // binary-packed payload
  uint8_t count;            // number of tags (1–10)
  bool fits_inline;         // true when encoded.size() <= kMaxInlineTagPayload
};
```

Produced by `prepare_tags(proto_tags)`. Consumed by `apply_tags_to_value(ObjectValue&, PreparedTags&, tr, bucket_id, ref_tag)`.

### PutCondition

Conditional write precondition for PutObject (checked inside FDB transaction).

```cpp
struct PutCondition {
  std::string if_match;       // ETag must match (empty = not set)
  std::string if_none_match;  // ETag must NOT match, or "*" = object must not exist
};
```

---

## Ref-Count Structs

Defined in `backend/src/ref_count.hpp`. Used by CopyObject (data sharing) and GcWorker (deferred free).

### RValue (R: namespace — storage-tier ref-count)

```cpp
struct RValue {
  uint64_t ref_count;           // number of O: entries sharing this blob
  std::string chunk_descriptor; // original chunk info (for GC)
};
```

R: key = `['R' 1B][ref_tag 12B]` = 13 bytes. Value = `[ref_count uint64 BE][chunk_descriptor]`.

### DRefInfo (D: namespace — KV-tier ref-count)

D: entries use a flags-past-data model: non-shared entries are just raw data bytes. Shared entries append a flags byte + ref_count after the data.

```cpp
static constexpr uint8_t kDFlagShared = 0x01;  // entry is shared
static constexpr uint8_t kDFlagLarge  = 0x02;  // ref_count is uint64 (not uint16)

struct DRefInfo {
  bool shared;
  uint64_t ref_count;
};
```

Layout: `[data bytes][flags 1B][ref_count 2B or 8B]`. When `len == data_size`, the entry is non-shared. When `len > data_size`, the suffix contains shared ref-count metadata.

Helper functions: `read_d_ref_count()`, `write_d_with_ref()`, `d_data_portion()`.

---

### Parse Result Structs (keys)

Returned by `parse_*_key()` functions. Host byte order, heap-allocated strings for variable fields. `ref_tag` fields use `RefTag` (std::array<uint8_t,12>).

```cpp
struct ObjectKeyParts  { shard_count, shard_id, bucket_id, object_name };
struct VersionKeyParts { shard_count, shard_id, bucket_id, object_name, version_id };
struct PoKeyParts      { shard_count, shard_id, bucket_id, object_name, RefTag ref_tag };
struct GoKeyParts      { size_tier, shard_count, shard_id, bucket_id, RefTag ref_tag };
struct DKeyParts       { shard_count, shard_id, bucket_id, size_tier, hash_prefix, mtime, RefTag ref_tag };
struct BucketKeyParts  { tenant_id, bucket_name };
struct LKeyParts       { type, name };
struct GroupPoKeyParts { shard_count, shard_id, bucket_id, RefTag group_ref_tag };
struct GroupGoKeyParts { size_tier, shard, bucket_id, RefTag group_ref_tag };
```

### Key Format Notes

- **O: keys:** `S<KeyHeaderS><object_name>` — no separator (prefix scan used for listing)
- **V: keys:** `S<KeyHeaderS><object_name>\x00<version_id_be32>` — `\x00` separator prevents prefix collision during promotion scan
- **P: keys:** `P<KeyHeaderS><object_name><ref_tag_12B>` — fixed-size suffix, no separator needed

---

## Group Batch Structs

Defined in `backend/src/gc_value.hpp`. Used by batch commit (group P:O) and group GC (group G:O).

### GroupPoEntry / GroupPoValue

```cpp
struct GroupPoEntry {
  RefTag ref_tag;
  uint64_t estimated_size;
};

struct GroupPoValue {
  std::array<GroupPoEntry, 16> entries;
  uint8_t count;
  uint32_t created_at;
};
```

Value format: `[count 2B BE][ref_tag 12B + size 8B BE]×N[created_at 4B BE]` (per-entry = 20 bytes).

Functions: `make_group_po_value(const GroupPoEntry*, size_t, uint32_t)`, `parse_group_po_value(string_view) → optional<GroupPoValue>`.

### GroupGcEntry / GroupGcValue

```cpp
struct GroupGcEntry {
  RefTag ref_tag;
  ChunkType chunk;
  uint8_t flags;
  uint64_t estimated_size;
};

struct GroupGcValue {
  std::array<GroupGcEntry, 16> entries;
  uint8_t count;
};
```

Value format: `[count 2B BE][ref_tag 12B + chunk 1B + flags 1B + size 8B BE]×N` (per-entry = 22 bytes).

Functions: `make_group_gc_value(const GroupGcEntry*, size_t)`, `parse_group_gc_value(string_view) → optional<GroupGcValue>`.

Group G:O key format: `G:<size_tier 1B><shard 4B><bucket_id 8B><G 1B><group_ref_tag 12B>` (category `'G'` distinguishes from regular `'O'`).

Functions: `make_group_go_key()`, `parse_group_go_key()`.

---

## ErrInsertion (Fault Injection)

Defined in `backend/src/err_insertion.hpp/cpp`. Data member of `KvRgwServiceImpl`.

### FaultType enum

```cpp
enum class FaultType : uint8_t {
  kAbortAfterBatchPhase2,
  kAbortAfterSinglePhase2,
  kAbortSweeperAfterPutGo,
  kAbortGcWorkerMidGroup,
  kFaultTypeCount
};
```

### FaultPayload struct

```cpp
struct FaultPayload {
  uint8_t mode;           // 0x01=fixed, 0x02=counter/periodic, 0x03=time/periodic
  uint32_t period;        // counter period or time interval (us)
  uint32_t burst;         // consecutive activations per trigger
  uint64_t counter;       // internal state
  uint64_t last_time_us;  // internal state (time mode)
};
```

### ErrInsertion class layout

Two-level cache-friendly structure:
- `flags_[kFaultTypeCount]` — hot path; one byte per fault (0 = inactive). `is_error_active(FaultType)` is inline with `[[likely]]` branch on zero.
- `FaultPayload payload_[kFaultTypeCount]` — cold; consulted only when `flags_[i] != 0`.

Admin commands: `set-error <name> [period=N] [burst=N] [interval_us=N]`, `clear-error <name>`.

---

## Source Files

| File | Contains |
|------|----------|
| `error_codes.hpp` | `KvrgwErrorCode` using-declarations, `kvrgw::fdb::` constants, `fdb_to_error()`, `is_retriable*()`, `kvrgw_strerror()` |
| `error_codes.cpp` | `kvrgw_strerror()` implementation |
| `constants.hpp` | All constants (sizes, namespaces, defaults) |
| `key_buf.hpp` | KeyBuf + all KeyHeader packed structs |
| `object_value.hpp` | ObjectValueHeader, GcValueHeader, PoValueHeader, BucketValueHeader, ChunkDescriptor, OValueBuf, ObjectValue |
| `object_value.cpp` | hdr_to_be/hdr_from_be, parse_object_value, etag_display, bucket value, encode/decode_metadata |
| `gc_value.hpp` | GcValue, PoValue wrappers |
| `gc_value.cpp` | make_gc_value, parse_gc_value, make_po_value, parse_po_value, move_po_to_go |
| `ref_count.hpp` | RValue, DRefInfo, read/write helpers for R: and D: ref-counts |
| `keys.hpp` | Key build/parse function declarations + parse result structs |
| `keys.cpp` | Key build/parse implementations, d_size_tier_from_size, d_hash_prefix, `make_ct_key`, `make_c_prefix` (C: child keys) |
| `tag_value.hpp/cpp` | TagSet type, encode/decode/compute tag payload, binary format helpers |
| `bucket_policy.hpp` | BucketState, BucketCacheEntry, BucketValue; `read_bucket_state`, `check_access`, `verify_bucket_in_txn`, `parse_policy_flags` |
| `bucket_policy.cpp` | Implementation of bucket policy functions |
| `kv_store.hpp` | KvStore (factory: `create()`), KvTransaction (`kv_get`, `kv_put`, `kv_del`, `kv_range_scan`, `kv_range_clear`, `kv_async_get`/`kv_wait_get`), `FdbFuture`, `run_transaction` template — all FDB ops return `std::expected<T, fdb_error_t>` (C++23). FDB C API calls inlined here. `KvStore::range_scan()` accepts `disable_ryw` flag — see below. |
| `data_store.hpp` | DataStore (abstract interface), FileDataStore (local FS), PerfDataStore (no-op + configurable sleep) — all I/O ops return `std::error_code` |
| `gc_policy.hpp` | GcPolicy, GcConfigSnapshot |
| `gc_config_state.hpp` | GcConfigState (double-buffer) |
| `tier_config_state.hpp` | TierConfigState (double-buffer) + YAML loader |
| `service_impl.hpp` | TierConfig, KvRgwServiceImpl |
