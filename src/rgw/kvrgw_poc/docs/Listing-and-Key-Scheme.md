# Listing and Key Scheme

## Introduction

This document describes the KV key scheme and its impact on bucket listing operations.

The key scheme is designed to serve listing — each listing API maps to a clean range scan on a dedicated key namespace.

---

## Listing APIs

### ListObjectsV2

Returns up to 1,000 current object keys per page, in lexicographic order.

Each returned entry includes: key, last_modified, etag, size, storage_class, owner, checksum_algorithm.

No old versions. No delete markers. No child entries.

**Example:** A bucket contains objects `a`, `b`, `c`, `d`, `e`. A ListObjectsV2 call with max-keys=2 returns `a`, `b` with `IsTruncated=true` and a continuation token. The next call returns `c`, `d`. The last call returns `e` with `IsTruncated=false`.

**Prefix and delimiter filtering.** ListObjectsV2 supports `prefix` and `delimiter` parameters to emulate directory-like navigation. With `prefix=photos/` and `delimiter=/`, the response returns objects directly under `photos/` and rolls up deeper keys into `CommonPrefixes` entries (e.g., `photos/2024/`).

### ListObjectVersions

Returns all versions of all objects in lexicographic order by object key, with versions of the same object grouped together, latest version first.

Each returned entry includes: key, version_id, is_latest, last_modified, etag, size, storage_class, owner. Delete markers are included with a `DeleteMarker` type indicator.

**Example:** Object `a` was written three times (v1, v2, v3) then deleted (creating dm1), then written again (v4 — current). ListObjectVersions returns them in order: `a/v4`, `a/dm1`, `a/v3`, `a/v2`, `a/v1` — current version from `:O:` first, then `:V:` entries in descending version_id order.

Supports prefix/delimiter filtering, same as ListObjectsV2.

### ListMultipartUploads

Returns all active (in-progress) multipart uploads in the bucket, sorted by object key, then by upload_id (ref_tag).

Each returned entry includes: key, upload_id, initiator, owner, initiated (timestamp), storage_class.

**Example:** Three uploads are in progress — one for object `backup.tar` (ref_tag=R1) and two for object `data.csv` (ref_tag=R2, ref_tag=R3). ListMultipartUploads returns: `backup.tar/R1`, `data.csv/R2`, `data.csv/R3`.

Supports prefix/delimiter filtering on the object key.

### ListObjectAnnotations

Returns all annotations for a specific object version.

Each returned entry includes: annotation name, size, etag, last_modified.

This is a per-object API, not a bucket-wide listing. Up to 1,000 annotations per object version.

**Example:** Object `report.pdf` (version v5) has annotations `summary`, `review-notes`, `approval`. ListObjectAnnotations returns all three with their metadata.

---

## Consistency Requirements

**S3 strong read-after-write consistency** applies per API call.

Each listing call reflects the current state of the bucket at the moment it is processed. After a PUT or DELETE completes, the next list call is guaranteed to reflect it.

S3 does **not** guarantee a point-in-time snapshot across pages. Since each page is a separate API call, the bucket can change between pages.

**Example:** A client PUTs object `foo`, receives success, then immediately calls ListObjectsV2. The response is guaranteed to include `foo`. However, if another client deletes `bar` while the first client is paginating through results, `bar` may appear on an earlier page but be absent from later pages (or vice versa).

---

## KV Schema

Four categories separate current objects, old versions, child entries, and multipart uploads. Category tags are 1-byte ASCII values (`O`, `V`, `M`, `C`). In prose, these are written as `:O:`, `:V:`, `:M:`, `:C:` for visual clarity.

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

The ref_tag serves as the upload_id — generated at `InitiateMultipartUpload`, it uniquely identifies the upload and becomes the object's ref_tag in `:O:` on `CompleteMultipartUpload`. `part_number=0` stores upload metadata; `part_number=1–10000` store individual parts.

### Sharding Requirement

The shard_id is derived from `hash(bucket_id + object_name) % shard_count`.

The shard_count field (starts at 1) records how many shards exist for this bucket. The shard_id is always zero-based: `shard_id = hash(bucket_id + object_name) % shard_count`. Invariant: `shard_id < shard_count`. When `shard_count = 1`, all objects land on `shard_id = 0` (single shard, no effective hashing).

All entries logically tied to the same object must share the same shard_id — this ensures they can be read and written in a single transaction on the same cluster.

Entries sharing the parent shard:
- `:O:` and `:V:` entries for the same object_name hash identically — versioned PUT (move from `:O:` to `:V:`) is a local transaction.
- `:M:` entries are keyed by object_name — multipart operations (initiate, upload part, complete, abort) are local to the target object's shard. The ref_tag in the `:M:` key serves as the upload_id and becomes the object's ref_tag on completion.
- `:C:` entries use the parent's ref_tag but must reside on the same shard as the parent `:O:` entry. The shard_id in the `:C:` key is inherited from the parent, not recomputed from the ref_tag.

---

## Key Namespace Design

The key namespaces were selected to match listing requirements.

Each listing API maps to a range scan on one or two namespaces — no filtering across unrelated entry types. No listing operation ever needs to skip over entries from an unrelated namespace.

### Why four categories

A single flat category would force every listing scan to filter out unrelated entry types — versions, children, multipart state — wasting I/O and complicating pagination. Dedicated categories eliminate this entirely.

**`:O:` — current objects.**

One entry per object key. This is the only namespace ListObjectsV2 touches.

A clean range scan on `:O:` returns exactly what S3 requires — current object keys in lexicographic order. No children, no old versions, no multipart state interleaved.

Fenced entries (delete markers in versioned buckets) are filtered out during the scan. Non-versioned deleted objects have no `:O:` entry — they are simply absent (their entries are moved to the `G` namespace).

**`:V:` — old versions and delete markers.**

Keyed by `<object_name><version_id>` with a descending version_id scheme — latest version sorts first within each object.

ListObjectVersions scans both `:O:` and `:V:`, merge-sorts by object name. All versions of the same object appear together, latest first, as S3 requires. Delete markers are included with their type indicator.

Separating `:V:` from `:O:` means ListObjectsV2 never encounters old versions or delete markers — no filtering needed.

**`:M:` — multipart uploads.**

Keyed by `<object_name><ref_tag><part_number>`. The ref_tag serves as the upload_id. Entries are transient — they exist only between InitiateMultipartUpload and CompleteMultipartUpload / AbortMultipartUpload.

`part_number=0` stores upload metadata. `part_number=1–10000` store individual parts. ListParts is a range scan on a specific `<object_name><ref_tag>` prefix. ListMultipartUploads scans `:M:` and returns only `part_number=0` entries, sorted by object key. Supports prefix/delimiter filtering directly on the object key.

Multipart uploads use a dedicated namespace (not `:C:`) because ListMultipartUploads is a bucket-wide API requiring object-key sort order. Child entries under `:C:` are keyed by ref_tag, not object_name — they cannot satisfy the object-key ordering that ListMultipartUploads requires.

**`:C:` — child entries.**

Keyed by `<ref_tag><child_type><child_id>`. Used for annotations, tags, and extended value.

Child entries are never encountered during bucket-wide listing scans. The `:C:` namespace is only accessed by per-object operations:

- `ListObjectAnnotations` — range scan on `:C:<ref_tag>A` for a specific object.
- `GetObjectTagging` — point read on `:C:<ref_tag>T` (when tags are external; inline tags are served from the `:O:` value).
- Batch cleanup — range delete on `:C:<ref_tag>` removes all children of a deleted object.

### Child type prefixes

Within `:C:`, each child type has a single-character ASCII prefix after the ref_tag:

- `A` — annotation
- `T` — tags
- `E` — extended value (when stored as KV)

This enables type-scoped range operations:

- List all annotations: `RangeScan(...C<ref_tag>A)`
- Delete all annotations: `RangeDelete(...C<ref_tag>A)`
- Delete all children: `RangeDelete(...C<ref_tag>)`

### Listing API to namespace mapping

| Listing API | Namespace(s) | Scan type |
|---|---|---|
| ListBuckets | `B` | Per-tenant prefix scan |
| ListObjectsV2 | `:O:` | Bucket-wide range scan |
| ListObjectVersions | `:O:` + `:V:` | Bucket-wide, merge-sort |
| ListMultipartUploads | `:M:` | Bucket-wide range scan |
| ListObjectAnnotations | `:C:` | Per-object range scan |

No listing operation crosses namespace boundaries unnecessarily. Bucket-wide APIs scan namespaces keyed by object_name. Per-object APIs scan `:C:` keyed by ref_tag.

### Why `:M:` is not under `:C:`

Multipart upload entries could in principle live under `:C:` as children of the target object. They are not placed there because:

1. **ListMultipartUploads requires object-key order.** `:C:` entries are keyed by ref_tag, not object_name. A bucket-wide listing sorted by object key cannot be served from ref_tag-keyed entries without an additional index or full scan.

2. **Multipart entries are keyed by object_name.** The upload targets a specific object key. Keying by `<object_name><ref_tag><part_number>` directly satisfies the listing sort order.

3. **Multipart entries are transient.** They exist only during the upload lifecycle and are deleted atomically on complete or abort. They do not persist as children of the committed object.

### Why `:V:` is not under `:C:`

Old versions could in principle be stored as children of the current version. They are not because:

1. **ListObjectVersions is a bucket-wide API.** It must return all versions of all objects sorted by object key. `:C:` entries keyed by ref_tag cannot serve this ordering.

2. **Versioned PUT is a local transaction.** Creating a new version writes to `:O:` and moves the old entry to `:V:`. Both share the same `(bucket_id, object_name)` and therefore the same shard — no cross-shard coordination, no OLH. Placing versions under `:C:` with a ref_tag key would break this co-location.

3. **version_id is excluded from the shard hash.** All versions of the same object share the same shard_id by construction. This eliminates the need for OLH (Object Lifecycle Head) as a separate coordination object.

---

## Key Schema — Binary Layout

All keys are binary byte strings with no delimiters between fields. Fixed-length fields are adjacent and parsed by position. Variable-length fields use implicit length — derived from total key length minus known fixed parts.

### Canonical Field Names

| Field | Size | Encoding | Description |
|---|---|---|---|
| namespace | 1 B | ASCII | KV namespace: `S` (S3 objects), `B` (bucket metadata), `T` (tenant metadata), `L` (ID allocation and lookup), `D` (Tier 2 KV data), `Z` (zone/realm metadata), `G` (GC — see below), `P` (pending operations — see below) |
| tenant_id | 4 B | uint32 big-endian | Tenant identifier. **`B` namespace only (in key).** In `T` namespace, tenant_id is in the value. |
| size_tier | 1 B | uint8 | Logarithmic object size tier (0–34). **`G` namespace only** — `clamp(floor(log2(size)) - 10, 0, 34)` |
| shard_count | 2 B | uint16 big-endian | Number of shards for this bucket (starts at 1, max 65535) |
| shard_id | 2 B | uint16 big-endian | `hash(bucket_id + object_name) % shard_count`; zero-based, invariant: `shard_id < shard_count` |
| bucket_id | 8 B | binary | Binary bucket identifier |
| cat | 1 B | ASCII | Category tag: `O`, `V`, `M`, `C` (S/Z namespaces); `O`, `V`, `M`, `A`, `P`, `F` (G namespace); operation-specific (P namespace) |
| op_type | 1 B | ASCII | **`P` namespace only** — operation type: `O` (PutObject), `A` (PutObjectAnnotation), `P` (CompleteMultipartUpload), `U` (UploadPart) |
| object_name | 1–1024 B | raw bytes | S3 object key; preserves lexicographic ordering |
| version_id | 4 B | uint32 big-endian | Descending; first value is `max_uint32`, then decreasing |
| ref_tag | 12 B | binary | `rgw_id (4B, uint32 BE) + seq_id (8B, uint64 BE)` — unique write instance identifier |
| child_type | 1 B | ASCII | `A` (annotation), `T` (tags), `E` (extended value) |
| part_number | 2 B | uint16 big-endian | Multipart part number; 0 = upload metadata, 1–10000 = parts |
| child_id | 0–512 B | raw bytes (UTF-8) | Annotation name for type `A`; empty for `T`, `E` |

### Fixed Header

Keys in the `S`, `Z` namespaces share a 14-byte fixed header:

```
offset  0   namespace    1 B
offset  1   shard_count  2 B
offset  3   shard_id     2 B
offset  5   bucket_id    8 B
offset 13   cat          1 B
            ─────────────────
            total      14 B
```

Keys in the `B` (bucket metadata) namespace have a 5-byte header followed by a variable-length bucket_name:

```
offset  0   namespace    1 B   (= 'B')
offset  1   tenant_id    4 B
offset  5   bucket_name  1–63 B
            ─────────────────
            total      6–68 B
```

Keys in the `T` (tenant metadata) namespace have a variable-length key:

```
offset  0   namespace    1 B   (= 'T')
offset  1   tenant_name  1–64 B
            ─────────────────
            total      2–65 B
```

Keys in the `L` (ID allocation and lookup) namespace have a variable-length key:

```
offset  0   namespace    1 B   (= 'L')
offset  1   type         1 B   ('N' = counter, 'I' = instance lookup)
offset  2   name         1–64 B
            ─────────────────
            total      3–66 B
```

Keys in the `G` (GC) namespace have a 15-byte header — an extra `size_tier` byte at offset 1:

```
offset  0   namespace    1 B   (= 'G')
offset  1   size_tier    1 B
offset  2   shard_count  2 B
offset  4   shard_id     2 B
offset  6   bucket_id    8 B
offset 14   cat          1 B
            ─────────────────
            total      15 B
```

Keys in the `P` (pending operations) namespace use the same 14-byte header as `S`/`Z`, but the `cat` byte is replaced by `op_type`:

```
offset  0   namespace    1 B   (= 'P')
offset  1   shard_count  2 B
offset  3   shard_id     2 B
offset  5   bucket_id    8 B
offset 13   op_type      1 B
            ─────────────────
            total      14 B
```

The parser must branch on `namespace` before parsing the rest of the key.

### Key Layouts by Namespace

The common header for `S`/`Z` namespaces (namespace + shard_count + shard_id + bucket_id) is 13 bytes. The cat byte selects the category. Type-specific fields follow.

**`B` — bucket metadata (variable-length key):**

```
[B 1B] [tenant_id 4B] [bucket_name 1–63B]
```

Min: 6 bytes. Max: 68 bytes.\
`tenant_id = key[1:5]`, `bucket_name = key[5:]`\
Value: bucket_id (8B) + bucket metadata (ACL, policies, quota, versioning, lifecycle, shard_count, etc.)

ListBuckets per tenant: prefix scan on `B <tenant_id>`.

**`T` — tenant metadata (variable-length key):**

```
[T 1B] [tenant_name 1–64B]
```

Min: 2 bytes. Max: 65 bytes.\
`tenant_name = key[1:]`\
Value: tenant_id (4B) + tenant-level configuration (quota, rate limits, tenant-wide policies).

**`L` — ID allocation and lookup (variable-length key):**

```
[L 1B] [type 1B] [name 1–64B]
```

Min: 3 bytes. Max: 66 bytes.\
`type = key[1]`, `name = key[2:]`

Type `N` — monotonic counters for ID allocation:

| Key | Value | Usage |
|---|---|---|
| `L N tenant_id` | uint64 | Allocate new tenant_id (atomic-increment) |
| `L N bucket_id` | uint64 | Allocate new bucket_id (atomic-increment) |
| `L N rgw_id` | uint64 | Allocate new RGW instance ID (atomic-increment) |

Type `I` — instance lookup (text name → assigned binary ID):

| Key | Value | Usage |
|---|---|---|
| `L I <rgw_instance_name>` | uint32 RGW-ID | Map RGW instance name to its assigned ID at boot |

**`O` — current objects:**

```
[common 13B] [O] [object_name 1–1024B]
```

Max: 1038 bytes.\
`object_name = key[14:]`

**`V` — versions and delete markers:**

```
[common 13B] [V] [object_name 1–1024B] [version_id 4B]
```

Max: 1042 bytes.\
`object_name = key[14:-4]`, `version_id = key[-4:]`

**`M` — multipart uploads:**

```
[common 13B] [M] [object_name 1–1024B] [ref_tag 12B] [part_number 2B]
```

Max: 1052 bytes.\
`object_name = key[14:-14]`, `ref_tag = key[-14:-2]`, `part_number = key[-2:]`

**`C` — child entries:**

```
[common 13B] [C] [ref_tag 12B] [child_type 1B] [child_id 0–512B]
```

Max: 539 bytes.\
`ref_tag = key[14:26]`, `child_type = key[26]`, `child_id = key[27:]`

**`G` — GC entries (15-byte header, fixed-length keys):**

```
[G 1B] [size_tier 1B] [shard_count 2B] [shard_id 2B] [bucket_id 8B] [cat 1B] [body]
```

G keys use the entry's `ref_tag` instead of `object_name` — the object name is not preserved in `G` (not needed for cleanup). All G keys are fixed-length:

```
G:O  [header 15B] [ref_tag 12B]                          = 27 B
G:V  [header 15B] [ref_tag 12B] [version_id 4B]          = 31 B
G:U  [header 15B] [ref_tag 12B] [part_number 2B]         = 29 B  (multipart part)
G:A  [header 15B] [ref_tag 12B] [secondary_ref_tag 12B]  = 39 B
G:M  [header 15B] [object_name 1–1024B] [ref_tag 12B]    = 28–1051 B  (multipart directive)
G:F  [header 15B] [object_name 1–1024B] [ref_tag 12B]    = 28–1051 B  (full delete directive)
```

Each version has its own unique `ref_tag`, so `G:O` and `G:V` keys are naturally unique. `G:U` uses `ref_tag + part_number` — used for individual part cleanup during re-uploads. `G:A` uses a `secondary_ref_tag` (generated by RGW) because annotations don't have their own ref_tag. `G:M` and `G:F` (directives) are variable-length — they carry the `object_name` because the background worker needs it to construct scan prefixes in the `S` namespace.

`size_tier = clamp(floor(log2(object_size)) - 10, 0, 34)` — entries sort by size within `G`, enabling prioritized background cleanup. For `G:A` entries, `size_tier` reflects the annotation data size (not the parent object size). See [S3 Operations — GC Namespace](S3-Operations-Over-KV.md#gc-namespace-and-background-cleanup).

**`P` — pending operations (14-byte header):**

```
P:O  [header 14B] [object_name 1–1024B] [ref_tag 12B]             = 27–1049 B
P:A  [header 14B] [object_name 1–1024B] [ref_tag 12B]             = 27–1049 B
P:M  [header 14B] [object_name 1–1024B] [ref_tag 12B]             = 27–1049 B  (multipart upload)
```

**Parsing:** For `P:O`, `P:A`, `P:M`: `op_type = key[13]`, `object_name = key[14:-12]`, `ref_tag = key[-12:]`.

The `P` entry inherits `shard_count` and `shard_id` from the target object — writing the `P` entry alongside the Phase 1 mutation is a single-shard transaction. The `ref_tag` ensures uniqueness (e.g., two concurrent multipart uploads for the same object have different ref_tags). The `shard_count` and `shard_id` fields at the start of the key enable per-shard prefix scans (`RangeScan(P + shard_count + shard_id)`), used by the resharding drain protocol. See [S3 Operations — Pending Operations Namespace](S3-Operations-Over-KV.md#pending-operations-namespace) and [Listing-During-Online-Migration.md — Resharding Impact](Listing-During-Online-Migration.md#resharding-impact-on-write-operations).

**`D` — Tier 2 KV data (fixed 31-byte key):**

```
offset  0   namespace    1 B   (= 'D')
offset  1   shard_count  2 B
offset  3   shard_id     2 B
offset  5   bucket_id    8 B
offset 13   size_tier    1 B   floor(log2(size))
offset 14   hash_prefix  1 B   FNV-1a(ref_tag) % 32
offset 15   mtime        4 B   uint32 big-endian (creation time, seconds)
offset 19   ref_tag     12 B
            ─────────────────
            total       31 B
```

Value: raw object data (up to 8KB).

- `size_tier` — enables per-tier prefix scans for LC/migration.
- `hash_prefix` — scatters writes across 32 sub-ranges to prevent hotspots (ref_tag is sequential, not random).
- `mtime` — LRU ordering within each sub-range; 32-way parallel scan + k-way merge for global LRU.

Co-located on the same shard as the parent O: — single-shard transaction for PUT. See [data-tiering.md](../data-tiering.md).

### Variable-Length Encoding

Variable-length fields (`object_name`, `child_id`) use implicit length — the parser reads fixed-length fields by position (from the start and end of the key) and derives the variable field from what remains.

Length prefixes were considered and rejected — inserting a 2-byte length before a variable field changes the sort key from the field's content to `(length, content)`, breaking the lexicographic ordering that all listing APIs require.

S3 object keys can contain any byte value including `0x00`, so null-termination and delimiter-based schemes do not work.

### S3 API String Mapping

The internal binary fields are mapped to opaque strings for the S3 API:

- **version_id** — the internal uint32 is encoded as a string by RGW and returned to the client in PUT/DELETE responses. The client passes this string back in subsequent requests (GetObject, DeleteObject with versionId); RGW decodes it back to uint32.
- **upload_id** — the 12-byte ref_tag is encoded as a string (e.g., hex or base64) and returned to the client by InitiateMultipartUpload. The client uses it in all subsequent multipart calls; RGW decodes it back to the binary ref_tag.

The encoding format is an implementation detail — the S3 API treats both as opaque tokens.

### Construction

1. **Derive shard_id.** `shard_id = hash(bucket_id + object_name) % shard_count`. When `shard_count = 1`, this always yields `0`. For `:C:` keys, shard_id is inherited from the parent `:O:` entry.
2. **Build header.** For `S`/`Z`/`P`: concatenate `namespace(1B) + shard_count(2B, big-endian) + shard_id(2B, big-endian) + bucket_id(8B) + cat(1B)` = 14 bytes (for `P`, the `cat` byte is `op_type`). For `G`: concatenate `namespace(1B) + size_tier(1B) + shard_count(2B, big-endian) + shard_id(2B, big-endian) + bucket_id(8B) + cat(1B)` = 15 bytes. For `B`: concatenate `namespace(1B) + tenant_id(4B, big-endian) + bucket_name`. For `T`: concatenate `namespace(1B) + tenant_name`. For `L`: concatenate `namespace(1B) + type(1B) + name`.
3. **Append type-specific fields.**

   For `S`/`Z` namespaces:
   - `O`: `+ object_name`
   - `V`: `+ object_name + version_id(4B, big-endian)`
   - `M`: `+ object_name + ref_tag(12B) + part_number(2B, big-endian)`
   - `C`: `+ ref_tag(12B) + child_type(1B) + child_id`

   For `G` namespace (per-instance, fixed-length):
   - `O`: `+ ref_tag(12B)` — 27 B
   - `V`: `+ ref_tag(12B) + version_id(4B, big-endian)` — 31 B
   - `M`: `+ ref_tag(12B) + part_number(2B, big-endian)` — 29 B
   - `A`: `+ ref_tag(12B) + secondary_ref_tag(12B)` — 39 B

   For `G` namespace (directives, variable-length):
   - `P`: `+ object_name + ref_tag(12B)` — parts cleanup directive
   - `F`: `+ object_name + ref_tag(12B)` — full object delete directive

   For `P` namespace:
   - `O`, `A`, `P`: `+ object_name + ref_tag(12B)`
   - `U`: `+ object_name + ref_tag(12B) + part_number(2B, big-endian)`

### Deconstruction

Position-based parsing. Branch on the namespace byte first, then read the appropriate header size, validate the category tag, and parse the body.

1. **Parse header.** Read `namespace = key[0]`.
   - If `namespace` is `S`/`Z` (14-byte header):
     - `shard_count = key[1:3]`, `shard_id = key[3:5]`, `bucket_id = key[5:13]`, `cat = key[13]`.
     - Body starts at offset 14.
   - If `namespace` is `P` (14-byte header):
     - `shard_count = key[1:3]`, `shard_id = key[3:5]`, `bucket_id = key[5:13]`, `op_type = key[13]`.
     - Body starts at offset 14.
   - If `namespace` is `G` (15-byte header):
     - `size_tier = key[1]`, `shard_count = key[2:4]`, `shard_id = key[4:6]`, `bucket_id = key[6:14]`, `cat = key[14]`.
     - Body starts at offset 15.
   - If `namespace` is `B` (variable-length):
     - `tenant_id = key[1:5]`, `bucket_name = key[5:]`.
   - If `namespace` is `T` (variable-length):
     - `tenant_name = key[1:]`.
   - If `namespace` is `L` (variable-length):
     - `type = key[1]`, `name = key[2:]`.
   - **Validate invariant (S/Z/G/P only):** `shard_id < shard_count`.

2. **Validate cat / op_type.** For `S`/`Z`: must be one of `{O, V, M, C}`. For `G`: must be one of `{O, V, M, A, P, F}`. For `P` namespace: op_type must be one of `{O, A, P, U}`. For `L` namespace: type must be one of `{N, I}`. `B` and `T` namespaces have no cat/op_type/type field. Reject with error otherwise.

3. **Parse body by cat** (offsets relative to body start).

   For `S`/`Z` namespaces:
   - `O` → `object_name = body[:]`
   - `V` → `object_name = body[:-4]`, `version_id = body[-4:]`
   - `M` → `object_name = body[:-14]`, `ref_tag = body[-14:-2]`, `part_number = body[-2:]`
   - `C` → `ref_tag = body[:12]`, `child_type = body[12]`, `child_id = body[13:]`
     - **Validate child_type.** Must be one of `{A, T, E}`. Reject with error otherwise.

   For `G` namespace (per-instance, fixed-length):
   - `O` → `ref_tag = body[:12]` (body is exactly 12 B)
   - `V` → `ref_tag = body[:12]`, `version_id = body[12:16]` (body is exactly 16 B)
   - `M` → `ref_tag = body[:12]`, `part_number = body[12:14]` (body is exactly 14 B)
   - `A` → `ref_tag = body[:12]`, `secondary_ref_tag = body[12:24]` (body is exactly 24 B)

   For `G` namespace (directives, variable-length):
   - `P` → `object_name = body[:-12]`, `ref_tag = body[-12:]`
   - `F` → `object_name = body[:-12]`, `ref_tag = body[-12:]`

   For `P` namespace (branch on op_type):
   - `O`, `A`, `P` → `object_name = body[:-12]`, `ref_tag = body[-12:]`
   - `U` → `object_name = body[:-14]`, `ref_tag = body[-14:-2]`, `part_number = body[-2:]`

### Key Size Summary

| Namespace | Min | Max | Variable field |
|---|---|---|---|
| `O` | 15 B | 1038 B | object_name (1–1024 B) |
| `V` | 19 B | 1042 B | object_name (1–1024 B) |
| `M` | 29 B | 1052 B | object_name (1–1024 B) |
| `C` | 27 B | 539 B | child_id (0–512 B) |
| `G:O` | 27 B | 27 B | fixed (ref_tag) |
| `G:V` | 31 B | 31 B | fixed (ref_tag + version_id) |
| `G:U` | 29 B | 29 B | fixed (ref_tag + part_number) |
| `G:A` | 39 B | 39 B | fixed (ref_tag + secondary_ref_tag) |
| `G:M` | 28 B | 1051 B | object_name (1–1024 B) + ref_tag |
| `G:F` | 28 B | 1051 B | object_name (1–1024 B) + ref_tag |
| `P:O/A/M` | 27 B | 1049 B | object_name (1–1024 B) + ref_tag |

---

## Listing During Resharding

When the shard count changes (e.g., 2 → 4), a background worker re-keys entries from the old shard_count to the new. During this transition, two sets of keys coexist.

### Invariant

At most two shard_counts coexist at any time. A reshard from shard_count X to shard_count Y must complete before starting another reshard.

### Key Range Separation

Old keys (`shard_count=X`) and new keys (`shard_count=Y`) occupy disjoint byte ranges. Since `shard_count` is at offset 1-2 (big-endian) and X < Y, all old keys sort before all new keys. There is no overlap.

### Watermark

A background worker scans range-by-range, re-keying each entry from the old shard_count/shard_id to the new. The watermark tracks progress through the object_name space.

- Below the watermark: entries have been re-keyed. They exist only in new shard ranges.
- Above the watermark: entries have not been re-keyed. They exist only in old shard ranges.
- No duplicates — each entry exists in exactly one location.

On-access writes above the watermark re-key the entry on the fly (delete old key, write new key in the same transaction).

### Listing Algorithm

During resharding, listing scans both old and new shard ranges and merges all streams:

1. Scan all **new** shards: `shard_count=Y`, `shard_id=0..Y-1`.
2. Scan all **old** shards: `shard_count=X`, `shard_id=0..X-1` — only entries above the watermark remain.
3. Merge-sort all X + Y streams by object_name.

For a typical 2 → 4 reshard, this is 6 concurrent streams. Each stream returns entries in object_name order within its shard. The merge produces global lexicographic order.

Once resharding completes (watermark reaches the end of the keyspace), old shard ranges are empty. Listing reverts to the normal Y-way merge.

---

## Operations by Type — Key Construction from S3 Request

Every S3 request arrives with a bucket name and (for object-level operations) an object key. The KV key must be constructed from these request parameters.

For `:O:`, `:V:`, and `:M:` namespaces, the key is constructed directly — the S3 request provides all fields.

For `:C:` entries, the ref_tag is not present in the S3 request. A parent read is always required to obtain the ref_tag before accessing any child entry.

### Current objects (`:O:`)

**Key:** `<namespace><shard_count><shard_id><bucket_id>O<object_name>`

**From the request:** (tenant_id, bucket_name) → bucket_id (local cache), `shard_id = hash(bucket_id + object_name) % shard_count`. Key is fully constructible.

| Operation | S3 API | Key construction |
|---|---|---|
| Read object metadata | HeadObject | Direct — bucket_id + object_name |
| Read object | GetObject | Direct |
| Write object | PutObject | Direct |
| Delete object | DeleteObject (non-versioned) | Direct |
| List current objects | ListObjectsV2 | Range scan on `...O` prefix |

### Versions (`:V:`)

**Key:** `<namespace><shard_count><shard_id><bucket_id>V<object_name><version_id>`

**From the request:** bucket_name, object_name, version_id — all provided by the client. Key is fully constructible.

| Operation | S3 API | Key construction |
|---|---|---|
| Get specific version | GetObject (with versionId) | Direct — bucket_id + object_name + version_id |
| Delete specific version | DeleteObject (with versionId) | Direct |
| Create delete marker | DeleteObject (versioned, no versionId) | Direct — new version_id generated |
| Remove delete marker | DeleteObject (with versionId targeting marker) | Direct |
| List all versions | ListObjectVersions | Range scan on `:O:` + `:V:`, merge-sort |

No single API to delete all versions — client must list and delete individually.

### Multipart uploads (`:M:`)

**Key:** `<namespace><shard_count><shard_id><bucket_id>M<object_name><ref_tag><part_number>`

**From the request:** bucket_name, object_name, ref_tag (returned as upload_id by InitiateMultipartUpload), part_number (client-supplied). Key is fully constructible.

The ref_tag serves as the upload_id. It is generated at `InitiateMultipartUpload` and returned to the client. On `CompleteMultipartUpload`, it becomes the object's ref_tag in `:O:`.

`part_number=0` stores upload metadata (initiated timestamp, owner, requested attributes). `part_number=1–10000` store individual parts.

| Operation | S3 API | Key construction |
|---|---|---|
| Create upload | InitiateMultipartUpload | Direct — ref_tag generated, write part_number=0 |
| Upload part | UploadPart | Direct — object_name + ref_tag + part_number |
| List parts | ListParts | Range scan on `...M<object_name><ref_tag>` |
| Complete upload | CompleteMultipartUpload | Read all parts, write `:O:` (ref_tag carries over), delete all `:M:` entries for this ref_tag |
| Abort upload | AbortMultipartUpload | Delete all `:M:` entries for this ref_tag |
| List active uploads | ListMultipartUploads | Range scan on `...M` prefix, return part_number=0 entries only |

### Tags (`:O:` inline or `:C:` external)

Tags are stored inline in the `:O:` value when short and space permits. When they exceed the inline budget, they spill to a child KV.

**Key (external):** `<namespace><shard_count><shard_id><bucket_id>C<ref_tag>T`

**From the request:** bucket_name, object_name — parent read is always required to determine inline vs external and to get ref_tag.

| Operation | S3 API | Key construction |
|---|---|---|
| Get all tags | GetObjectTagging | Parent read → if inline, return from `:O:` value; if external, point read `...C<ref_tag>T` |
| Set/replace all tags | PutObjectTagging | Parent read → if inline, update `:O:` value; if external, write `...C<ref_tag>T` |
| Delete all tags | DeleteObjectTagging | Parent read → if inline, update `:O:` value; if external, delete `...C<ref_tag>T` |

All tags stored together. Max 10 tags per object.

### Annotations (`:C:`)

**Key:** `<namespace><shard_count><shard_id><bucket_id>C<ref_tag>A<annotation_name>`

**From the request:** bucket_name, object_name, annotation_name — but ref_tag is not in the request. Must read the parent `:O:` (or `:V:`) entry first.

| Operation | S3 API | Key construction |
|---|---|---|
| Create/update annotation | PutObjectAnnotation | Parent read → ref_tag → write `...C<ref_tag>A<name>` |
| Get annotation | GetObjectAnnotation | Parent read → ref_tag → point read on `...C<ref_tag>A<name>` |
| Delete annotation | DeleteObjectAnnotation | Parent read → ref_tag → delete `...C<ref_tag>A<name>` |
| List all annotations | ListObjectAnnotations | Parent read → ref_tag → range scan on `...C<ref_tag>A` prefix |

Max 1,000 per object version. Each annotation is its own child KV entry. Annotation name: max 512 bytes (UTF-8). Annotation value: up to 1 MiB.

Tied to a specific object version. Independent across versions. Deletion is permanent — no versioning on annotations themselves.

### Extended value (`:C:`)

**Key:** `<namespace><shard_count><shard_id><bucket_id>C<ref_tag>E`

| Operation | Trigger | Key construction |
|---|---|---|
| Write extended value | PUT of large object | ref_tag known at write time — direct |
| Read extended value | GET / byte-range of large object | Parent read → ref_tag → point read |
| Delete extended value | Object DELETE or overwrite | Parent read → ref_tag → delete |

### Child cleanup on parent deletion

When an object is deleted or overwritten, its `:O:` (or `:V:`) entry is moved to the `G` (GC) namespace. The `G` key contains the entry's ref_tag (unique instance identifier) — no object_name. The stripped value contains only cleanup information (chunk pointers, child-presence flags). The `size_tier` in the `G` key is derived from the object size.

Background cleanup workers process `G` entries and handle child KV removal:

- `RangeDelete(...C<ref_tag>)` — removes all children (tags, annotations, extended value) in a single operation.
- For annotations with their own data chunks: `RangeScan(...C<ref_tag>A)` first reads annotation chunk pointers to free annotation data, then `RangeDelete` removes the entries.

See [S3 Operations — GC Namespace](S3-Operations-Over-KV.md#gc-namespace-and-background-cleanup) for the full protocol.

### Individual annotation cleanup

When a single annotation is deleted or overwritten (while the parent object is still live), the old annotation's `:C:` entry is moved to `G:A` with a secondary ref_tag. The `G:A` key is fixed 39 bytes — the annotation name is not preserved. Data store deletion is done by background workers from `G:A`. See [S3 Operations — GC Namespace](S3-Operations-Over-KV.md#gc-namespace-and-background-cleanup).

When a delete marker is created (versioned DELETE without version-id):

- Nothing is moved to `G`. The old `:O:` entry moves to `:V:` — all children are preserved on the underlying version.

### The ref_tag dependency

`:O:`, `:V:`, and `:M:` keys are fully constructible from the S3 request — bucket_name, object_name, version_id, and upload_id are all client-provided.

`:C:` keys require the ref_tag, which is not part of the S3 request. The ref_tag is generated by RGW at write time and stored in the parent `:O:` (or `:V:`) entry. Every child operation must read the parent first to obtain it.

This is an inherent cost of using ref_tag for child keys — one extra read per child access. The tradeoff buys compact child keys (ref_tag is 12 bytes vs. object_name up to 1024 bytes) and clean separation of children from bucket-wide listing scans.

---

## Appendix A — Pseudo-Python Key Construction and Deconstruction

See [kv_key_schema.py](kv_key_schema.py) for the canonical reference implementation with round-trip tests.

Run directly to execute all tests:

```bash
python kv_key_schema.py
```
