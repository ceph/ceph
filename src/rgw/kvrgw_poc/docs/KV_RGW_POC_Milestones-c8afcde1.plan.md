<!-- c8afcde1-af2a-46fa-8765-8a889bf37f62 -->
---
todos:
  - id: "m1-reftag"
    content: "M1: Implement ref_tag generation (rgw_id + seq_id, 12 bytes)"
    status: pending
  - id: "m1-fdb"
    content: "M1: FDB single-node bringup + KV Access API shim (Get, Put, Delete, Transaction)"
    status: pending
  - id: "m1-keys"
    content: "M1: Key construction for S:O: and B namespace"
    status: pending
  - id: "m1-server"
    content: "M1: S3 server hook (RGW process_request or standalone)"
    status: pending
  - id: "m1-put-get-head"
    content: "M1: PUT, GET, HEAD over KV + local FS data tier"
    status: pending
  - id: "m2-list"
    content: "M2: RangeScan + ListObjectsV2 with pagination"
    status: pending
  - id: "m2-po-gc"
    content: "M2: P:O coordination, sweeper, G namespace, GC worker"
    status: pending
  - id: "m2-overwrite"
    content: "M2: PUT overwrite with atomic GC in Phase 3"
    status: pending
  - id: "m2-test"
    content: "M2-test: S3 conformance test suite (basic mode — non-versioned CRUD, listing, overwrite, delete)"
    status: pending
  - id: "m2.5-encoding"
    content: "M2.5: Binary encoding for KV values — standalone milestone"
    status: pending
  - id: "m2.5-test"
    content: "M2.5-test: Re-run S3 conformance suite — verify no regressions after encoding change"
    status: pending
  - id: "m3-versioning"
    content: "M3: Version ID scheme, :V: keys, uniform rule, promotion, ListObjectVersions"
    status: pending
  - id: "m3-test"
    content: "M3-test: S3 conformance suite — versioned bucket tests"
    status: pending
  - id: "m4-multipart"
    content: "M4: Full multipart lifecycle (Initiate, UploadPart, Complete, Abort, sweeper)"
    status: pending
  - id: "m4-test"
    content: "M4-test: S3 conformance suite — multipart upload tests"
    status: pending
  - id: "m5-rados"
    content: "M5: RADOS data tier with storage-tier abstraction, striping, EC support"
    status: pending
  - id: "m5-test"
    content: "M5-test: Full S3 conformance suite + performance baseline over RADOS data tier"
    status: pending
  - id: "m6-packing"
    content: "M6: Small-object packing middleware"
    status: pending
isProject: false
---
# KV-RGW POC Milestones

## Architecture

Two-process architecture:

```
AWS CLI → gofakes3 (Go, S3 protocol) → gRPC (Unix socket) → C++ backend (KV logic, FDB, FS) → FDB + local FS
```

- **S3 frontend:** [gofakes3](https://github.com/johannesboyne/gofakes3) (Go). Handles S3 HTTP protocol, SigV4 auth (accepts any credentials), XML request/response formatting, error codes, path-style addressing, range requests. The Go `Backend` interface methods are thin gRPC client stubs.
- **Storage backend:** C++ gRPC server. Contains all KV logic: key construction, value serialization, FDB transactions, ref_tag generation, local FS data I/O. This is the real system — designed for reuse when migrating to RGW SAL (`FDBStore`) later.
- **Inter-process communication:** gRPC over Unix domain socket. Standard streaming for PUT/GET data (no zero-copy optimization in POC — Unix socket throughput ~6 GB/s, not the bottleneck). Protobuf defines the contract between frontend and backend.
- **Data tier:** local filesystem for M1–M4. RADOS in M5.
- **Metadata tier:** FoundationDB (single-node for POC).

The gRPC interface mirrors the gofakes3 Backend interface: `CreateBucket`, `PutObject`, `GetObject`, `HeadObject`, `DeleteObject`, `ListBucket`, etc. For versioning (M3), the interface extends to match gofakes3's `VersionedBackend`. For multipart (M4), extends to match `MultipartBackend`.

---

## Milestone 1 — Core CRUD: ref_tag, MakeBucket, PUT, GET, HEAD

Minimal end-to-end S3 path: create a bucket, write an object, read it back, inspect its metadata.

**Scope:**

- **gofakes3 frontend** — deploy gofakes3 with a custom Go `Backend` implementation. Each method is a gRPC call to the C++ backend. Define the protobuf service with M1 RPCs: `CreateBucket`, `BucketExists`, `PutObject` (client-streaming for data), `GetObject` (server-streaming for data), `HeadObject`.
- **C++ gRPC backend** — implement the gRPC service. This process owns all storage logic.
- **ref_tag generation** — implement the `rgw_id + seq_id` scheme (12 bytes) in C++. This is foundational — every subsequent milestone depends on it.
- **KV-DB bringup (FDB)** — deploy a single-node FDB cluster. Implement the KV Access API in C++ using FDB's C client: `Get`, `Put`, `Delete`, `Transaction`. No `RangeScan` yet (not needed until M2).
- **Key construction** — implement the `S` namespace key builder for `:O:` entries: `S <shard_count> <shard_id> <bucket_id> O <object_name>`. Use `shard_count=1`, `shard_id=0` (no sharding in M1).
- **`B` namespace** — implement `CreateBucket` in C++: transaction with existence check, `ATOMIC_ADD` on monotonic counter, write `B <tenant_id> <bucket_name> → bucket_id + metadata`. Use a hardcoded `tenant_id` for M1.
- **KV value** — store S3 attributes as a simple serialization (JSON or flat struct). Binary encoding deferred to M2.5.
- **Data tier** — local filesystem. Object data stored as `<data_root>/<ref_tag>` (single blob per object). No replication, no striping, no packing.
- **PUT** — C++ backend: generate ref_tag, receive streamed data from gRPC, write to local FS, write `:O:` KV. No `P:O` coordination in M1 (no crash recovery — deferred to M2).
- **GET** — C++ backend: read `:O:` KV, extract ref_tag, stream file data back over gRPC.
- **HEAD** — C++ backend: read `:O:` KV, return S3 attributes (etag, size, content_type, last_modified). No data-tier access.
- **ETag** — compute MD5 hex digest of uploaded content during PUT. Store in KV value.

**Not in scope:** `P:O` coordination, GC, listing, versioning, multipart, overwrite handling (overwrite simply replaces KV, old data leaked on FS — acceptable for POC M1).

**Exit criteria:** `aws s3 mb`, `aws s3 cp` (upload), `aws s3 cp` (download), `aws s3api head-object` work against the POC (gofakes3 frontend + C++ backend + FDB + local FS).

---

## Milestone 2 — ListObjectsV2, GC, P Domain, Bucket Ops

Production-grade single-object lifecycle: crash recovery, cleanup, efficient listing.

**Scope:**

- **RangeScan** — implement in the KV Access API shim. Required for listing.
- **ListObjectsV2** — range scan on `:O:` prefix, pagination via continuation token, prefix/delimiter filtering.
- **`P` namespace** — implement `P:O` coordination entries. Update PUT to 3-phase with `P:O` create (Phase 1) and delete (Phase 3 transaction).
- **Sweeper** — background thread scanning `P` namespace for stale entries. Frees orphaned FS data by ref_tag, deletes `P:O` entry.
- **`G` namespace** — implement GC entries. Update DELETE (non-versioned) to move `:O:` to `G` in a single transaction.
- **GC worker** — background thread scanning `G` namespace, freeing FS data, deleting `G` entries.
- **PUT overwrite** — Phase 3 reads existing `:O:`, moves old to `G`, writes new — all in one transaction. Old data cleaned by GC worker.
- **Full bucket support** — `ListBuckets`, `DeleteBucket` (with empty check via range scan on `:O:`), `HeadBucket`.
- **DELETE (non-versioned)** — read `:O:` inside transaction, move to `G`.

**Exit criteria:** `aws s3 ls`, `aws s3 rm`, PUT overwrite works correctly (old data cleaned up), crash during PUT leaves no orphaned data (sweeper cleans up), `aws s3 rb` works.

**Test gate:** Run S3 conformance test suite (e.g., AWS S3 test suite, elbencho) in basic non-versioned mode: CRUD, listing, overwrite, delete, bucket operations. All tests must pass before proceeding.

---

## Milestone 2.5 — Binary Encoding

Replace simple serialization with efficient binary encoding for KV values.

**Scope:**

- **Value layout design** — fixed-size fields at known offsets (ref_tag, version_id, size, flags), variable-length fields with length prefixes (etag, content-type, user metadata).
- **Encode/decode library** — zero-copy reads where possible. Versioned format (format byte at offset 0) for future extensibility.
- **Migrate `:O:` values** — replace JSON/flat-struct serialization from M1/M2 with binary encoding.
- **Migrate `G`, `P`, `B` values** — apply binary encoding to all namespace values.

**Exit criteria:** All KV values use binary encoding. No functional change — same S3 behavior.

**Test gate:** Re-run the full M2 S3 conformance suite. All tests must pass — verify no regressions from the encoding change.

---

## Milestone 3 — Versioning

S3 versioning: version chains, delete markers, promotion.

**Scope:**

- **`:V:` key construction** — `S <shard_count> <shard_id> <bucket_id> V <object_name> <version_id>`.
- **Version ID scheme** — `max_uint32` descending, stored in `:O:` value, appended to `:V:` key.
- **PUT (versioned)** — Phase 3 follows the uniform rule: read `:O:`, move to `:V:`, write new `:O:` with decremented version_id. All in one transaction.
- **DELETE without version-id** — create fenced delete marker in `:O:`, move old `:O:` to `:V:`.
- **DELETE with version-id** — unified transaction: read `:O:`, branch on match. Case 1: delete/move `:V:` entry. Case 2: promotion (range scan `:V:` with limit=1, move to `:O:`).
- **GET with version-id** — read `:O:`, compare version_id, fall through to `:V:` read.
- **ListObjectVersions** — dual range scan on `:O:` and `:V:`, merge by object_name.
- **ListObjectsV2 update** — skip fenced entries (delete markers).
- **Bucket versioning configuration** — `PutBucketVersioning`, `GetBucketVersioning`. Store versioning state in `B` namespace value.

**Exit criteria:** `aws s3api put-bucket-versioning`, upload multiple versions, `aws s3api list-object-versions`, delete (creates marker), delete with version-id (promotion), undelete by removing delete marker.

**Test gate:** Run S3 conformance suite with versioned bucket tests: version creation, delete markers, version-id deletion, promotion, undelete, ListObjectVersions. Re-run basic (M2) tests to verify no regressions.

---

## Milestone 4 — Multipart Upload

Full multipart upload lifecycle: initiate, upload parts, complete, abort, cleanup.

**Scope:**

- **`:M:` key construction** — `S <shard_count> <shard_id> <bucket_id> M <object_name> <ref_tag> <part_number>`.
- **InitiateMultipartUpload** — generate ref_tag, write head `:M:` (part_number=0) + `P:M` coordination entry in one transaction.
- **UploadPart** — 3-phase with `:M:` entry states (`pending`, `cleanup`, `committed`). Phase 1: verify head exists, create `:M:` with `state=pending`. Phase 2: write part data to FS as `ref_tag-part_number`. Phase 3: blind write `:M:` with `state=committed` + metadata.
- **Part re-upload** — detect in Phase 1, transition to `state=cleanup`, free old data, then `state=pending`.
- **CompleteMultipartUpload** — 3-phase: fence (absorb head into `P:M`, delete head `:M:`), lock-free scan of committed parts, commit final `:O:` + `G:M` MIXED directive + delete `P:M`.
- **AbortMultipartUpload** — delete head `:M:`, create `G:M` DEEP directive, delete `P:M` in one transaction.
- **Multipart sweeper** — process `G:M` entries (DEEP and MIXED modes). Multi-pass loop with range scan on `:M:` entries.
- **`P:M` sweeper** — handle stale active and completing `P:M` entries.
- **ListMultipartUploads** — scan part_number=0 entries.
- **ListParts** — scan `:M:` entries for a specific ref_tag.
- **`G:U`** — individual part GC for re-upload cleanup.

**Exit criteria:** `aws s3api create-multipart-upload`, `upload-part` (including re-upload), `complete-multipart-upload`, `abort-multipart-upload`, `list-multipart-uploads`, `list-parts`. Crash during any phase leaves no orphaned data.

**Test gate:** Run S3 conformance suite with multipart upload tests: initiate, upload parts (sequential and parallel), complete, abort, re-upload same part, list uploads, list parts. Re-run basic (M2) and versioned (M3) tests to verify no regressions.

---

## Milestone 5 — RADOS Data Tier

Replace local FS data tier with RADOS, enabling distributed storage.

**Scope:**

- **Storage-tier abstraction** — define interface: `Write(ref_tag, data)`, `Read(ref_tag) → data`, `Delete(ref_tag)`. Local FS and RADOS implement the same interface.
- **RADOS writer** — write object data as RADOS objects in a configurable data pool. Object naming: `ref_tag` as RADOS object name (no head object — just data).
- **RADOS reader** — read data by ref_tag from RADOS pool.
- **Striping** — for large objects, stripe across multiple RADOS objects: `ref_tag-0`, `ref_tag-1`, etc. Stripe size configurable.
- **Multipart data** — parts stored as `ref_tag-part_number` in RADOS. CompleteMultipartUpload creates the final manifest referencing part RADOS objects.
- **GC worker update** — delete RADOS objects instead of FS files. Stripe iteration for large objects.
- **EC pool support** — data pool can be erasure-coded. No special handling needed — RADOS handles EC transparently.
- **Configuration** — pool name, stripe size, EC profile selection via bucket or global config.

**Not in scope:** Small-object packing (M6), multi-site replication, lifecycle policies.

**Exit criteria:** Full S3 CRUD + versioning + multipart with data stored in RADOS EC pool, metadata in FDB. GC correctly cleans RADOS objects. Performance baseline established for comparison with current RGW model.

**Test gate:** Run full S3 conformance suite (basic + versioned + multipart) over RADOS data tier. Establish performance baseline (throughput, latency, IOPS) for comparison with current RGW model.

---

## Milestone 6 — Small-Object Packing

Aggregate small objects into shared blobs to reduce per-object storage overhead.

**Scope:**

- **Packing middleware** — sits between KV commit and storage tier. Small objects (below a configurable threshold, e.g., 64 KB) are staged locally (NVRAM or logging-FS) and batched into shared blobs.
- **Blob naming** — shared blobs use a packing-layer-assigned `blob_id`, not derived from `ref_tag`. The KV value stores `(blob_id, offset, length)` after the batch commit.
- **Two-phase commit** — stage small objects using `ref_tag` as identifier → batch commit to storage tier → update KV values with final `(blob_id, offset, length)`.
- **Packing-layer crash recovery** — staged but uncommitted objects are recoverable from the staging area. The `P:O` entry remains until the KV value is updated with final blob coordinates.
- **Logical-Punch-Hole** — freeing a small object's data means marking its region in the shared blob as free (logical punch), not deleting the blob. Blob is reclaimed when fully punched.
- **GC integration** — GC worker issues Logical-Punch-Holes instead of physical deletes for packed objects. Sweeper cleanup uses `ref_tag` to locate staged data in the packing layer.
- **Read path** — GET reads `(blob_id, offset, length)` from KV value, issues a byte-range read to the storage tier.

**Not in scope:** Multi-site replication, lifecycle policies.

**Exit criteria:** Small objects (< 64 KB) are packed into shared blobs. Read/write/delete work correctly. GC correctly punches holes. No orphaned data after crashes. Performance improvement measurable for small-object-heavy workloads.

**Test gate:** Run full S3 conformance suite. Targeted small-object benchmark (many 1 KB–64 KB objects): compare throughput and storage efficiency vs M5 (unpacked).
