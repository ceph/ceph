# Object Tagging — Implementation Plan

Implementation plan for S3 Object Tagging (PutObjectTagging, GetObjectTagging, DeleteObjectTagging) based on:
- [child-kv-operations.md](child-kv-operations.md) — PutObjectTagging / DeleteObjectTagging transaction protocols
- [S3-Operations-Over-KV.md](S3-Operations-Over-KV.md) — Object Tagging section (inline vs external model)
- [KV-Based-Design-For-RGW.md](KV-Based-Design-For-RGW.md) — C: namespace, tag_count field, child KV semantics

---

## Codebase Context (for implementation)

### Project structure

- `backend/src/service_impl.cpp` — all S3 RPC handlers (PutObject, GetObject, DeleteObject, CopyObject, etc.)
- `backend/src/service_impl.hpp` — handler declarations, helper structs (`PutCondition`, `NewVersionIds`, etc.)
- `backend/src/keys.hpp` / `keys.cpp` — key construction (`make_object_key`, `make_go_key`, `make_d_key`, `make_r_key`, etc.) and parsing
- `backend/src/key_buf.hpp` — `KeyBuf` (1100B stack buffer) and all `KeyHeader*` packed structs
- `backend/src/object_value.hpp` / `.cpp` — `ObjectValueHeader` (60B), `OValueBuf`, `ObjectValue`, parse/write helpers
- `backend/src/constants.hpp` — all constants, namespace chars, flags
- `backend/src/gc_worker.cpp` — GC background thread (processes G:O entries)
- `backend/src/gc_value.cpp` — `make_gc_value`, `move_po_to_go`, G:O value construction
- `backend/src/kv_store.hpp` — FDB wrapper (`get`, `set`, `del`, `range_scan`, `range_clear`, `begin_transaction`)
- `backend/src/bucket_policy.hpp` / `.cpp` — `check_access`, `verify_bucket_in_txn`, `read_bucket_state`
- `proto/kvrgw.proto` — gRPC service definition (22 RPCs currently)
- `frontend/backend_kvrgw.go` — Go frontend implementing versitygw `Backend` interface

### Key patterns to follow

- **New RPC:** Add message + RPC to proto → regenerate (`make proto`) → implement in `service_impl.cpp` → wire in `backend_kvrgw.go`
- **Key construction:** Use `KeyBuf` with packed `KeyHeaderS` (14B) + variable tail. See `make_object_key()` as the template.
- **Transactions:** Use `run_transaction` template (retry 10x on FDB conflict 1020) or manual `begin_transaction` + commit loop.
- **Access control:** `check_access(flag)` before operation (soft, cached); `verify_bucket_in_txn(tr, tenant, bucket, id, flag)` inside transaction (hard).
- **Error handling:** All returns via `std::expected<T, fdb_error_t>` or `grpc::Status`. No exceptions.
- **Tenant resolution:** Every RPC starts with `tenant_id_for_name(request.tenant_name())` → maps name to numeric id.
- **O: value rebuild:** Read existing O: → modify in-memory `ObjectValue` → serialize via `OValueBuf` → `put(S:O, buf.view())`.

### Existing flags byte usage (ObjectValueHeader offset 49)

```
bit 0 (0x01) = kFlagExtendedAttrs
bit 1 (0x02) = kFlagFenced (delete marker)
bit 2 (0x04) = kFlagSharedData (CopyObject ref-sharing)
bit 3 (0x08) = kFlagExternalTags  ← NEW (this plan)
```

### Build & test

```bash
./scripts/reload.sh --clean      # rebuild + restart (wipes FDB)
./scripts/reload.sh              # rebuild + restart (keeps data)
./scripts/run_test_plan.sh       # full 10-phase test suite
./scripts/run_ceph_rgw_tests.sh  # ceph s3 compatibility tests
make proto                       # regenerate gRPC stubs after proto changes
```

### Reference docs (in repo root)

- `poc_as_built.md` — full as-built reference (operations, pseudocode, background workers)
- `architecture.md` — component diagram and data flows
- `cpp-data-structures.md` — all packed structs and their layouts
- `s3-operations-code.md` — per-operation code walkthrough
- `child-kv-operations.md` — design doc for child KV operations (the source of truth for tag transaction protocols)

---

## Scope

Three S3 APIs:
- `PUT /bucket/key?tagging` — replace entire tag set (PutObjectTagging)
- `GET /bucket/key?tagging` — read tag set (GetObjectTagging)
- `DELETE /bucket/key?tagging` — remove all tags (DeleteObjectTagging)

AWS limits: max 10 tags per object, each tag key ≤ 128 chars, each tag value ≤ 256 chars, aggregate ≤ 5120 bytes.

---

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Storage model | **Dual-mode: inline in O: when small, external C:T when large** | Follows the base design exactly. Small tag sets (common case) stay inline for single-read HEAD/GET. Large tag sets spill to a child KV entry to keep O: compact. |
| Inline threshold | **`kMaxInlineTagPayload = 256`** (configurable) | Tags ≤ 256B encoded → inline. Tags > 256B encoded → external C:T. Matches the design's "inline when short and space permits" rule. Most real-world tag sets (2–5 short tags) fit easily. |
| Tag encoding | **Binary packed: `[key_len 2B BE][key][val_len 2B BE][val] × N`** | Compact, zero-copy parse, used for both inline payload and C:T value. |
| Tag presence signal | **`tags_count` field (uint8, offset 50) + `kFlagExternalTags` in flags byte** | `tags_count=0` → no tags. `tags_count>0 && !external_flag` → inline. `tags_count>0 && external_flag` → tags in C:T entry. |
| Child KV key | **`S<shard 4B><bucket_id 8B>C<ref_tag 12B>T`** = 26B fixed | Same shard as parent O: (by design). Single entry per object (all tags together). |
| Versioned tagging | **Current version only** (no `versionId` parameter) | Version resolution for child ops deferred to M4+. |
| CopyObject interaction | **Tags are copied with the object** | Inline tags: byte-copy. External tags: copy C:T entry to new ref_tag. |
| Conflict detection | **Always write O: (tags_count field)** | Per design: PutObjectTagging/DeleteObjectTagging always write O: for write-write conflict on both FDB and TiKV. |
| GC cleanup | **GcWorker deletes C:T when processing G:O with external tags** | Uses `RangeDelete(C:<ref_tag>*)` — same pattern annotations will use. |

---

## Storage Model

### Inline mode (tags in O: value)

When encoded tag payload ≤ `kMaxInlineTagPayload` (256B):

```
[ObjectValueHeader 60B]
[content_type 0–255B]
[inline_data 0–N B]        ← only for CHUNK_INLINE tier
[tag_payload 0–256B]       ← present when tags_count > 0 && !kFlagExternalTags
```

### External mode (tags in C:T child KV)

When encoded tag payload > `kMaxInlineTagPayload`:

```
O: value — same as above but NO tag_payload appended; kFlagExternalTags set in flags byte.

C:T key:  S <shard_count 2B> <shard_id 2B> <bucket_id 8B> C <ref_tag 12B> T
C:T value: [tag_payload bytes]  (up to ~5120B)
```

The C:T entry is co-located on the same shard as O: (same bucket_id + same shard prefix). Read and write happen in the same FDB transaction.

### Tag payload binary format (same encoding for both modes)

```
For each tag (tags_count entries):
  [key_len   2B BE]   (1–128)
  [key_bytes N B]
  [val_len   2B BE]   (0–256)
  [val_bytes N B]
```

Total overhead per tag: 4 bytes (lengths) + key + value. Worst case 10 tags: `10 × (4 + 128 + 256) = 3880B`.

### Transition rules (from design doc)

| Current state | New tag set | Action |
|---------------|-------------|--------|
| None/inline | Fits inline | Write inline in O: (Case A) |
| None/inline | Exceeds threshold | Clear inline, set external flag, write C:T (Case B) |
| External | Exceeds threshold | Overwrite C:T, update tags_count in O: (Case C) |
| External | Fits inline | Delete C:T, clear external flag, write inline in O: (Case D) |

---

## Implementation Phases

### Phase 1: Child KV Infrastructure (C: namespace)

This phase builds the reusable C: namespace infrastructure that tags (and later annotations, extended values) will use.

**Key construction** (`keys.hpp` / `keys.cpp`):

```cpp
// C: key = same header as S:O but with category 'C' + ref_tag + child_type + child_id
// For tags: child_type = 'T', child_id = empty (single entry per object)

constexpr char kCategoryChild = 'C';
constexpr char kChildTypeTags = 'T';
constexpr char kChildTypeAnnotation = 'A';
constexpr char kChildTypeExtended = 'E';

KeyBuf make_ct_key(std::string_view bucket_id, std::string_view ref_tag);
// produces: S <shard_count 2B> <shard_id 2B> <bucket_id 8B> C <ref_tag 12B> T
// total: 1 + 2 + 2 + 8 + 1 + 12 + 1 = 27B

KeyBuf make_c_prefix(std::string_view bucket_id, std::string_view ref_tag);
// produces: S <shard_count 2B> <shard_id 2B> <bucket_id 8B> C <ref_tag 12B>
// total: 26B — used for RangeDelete of all children
```

**Constants** (`constants.hpp`):

```cpp
inline constexpr size_t kMaxTags = 10;
inline constexpr size_t kMaxTagKeyLen = 128;
inline constexpr size_t kMaxTagValueLen = 256;
inline constexpr size_t kMaxTagPayload = 5120;
inline constexpr size_t kMaxInlineTagPayload = 256;
inline constexpr uint8_t kFlagExternalTags = 0x08;  // bit 3 in flags byte
```

**Tag encoding helpers** (`tag_value.hpp` / `tag_value.cpp`):

```cpp
using TagSet = std::vector<std::pair<std::string, std::string>>;

std::string encode_tag_payload(const TagSet& tags);
TagSet decode_tag_payload(std::string_view raw, uint8_t tags_count);
size_t compute_tag_payload_size(const TagSet& tags);
```

**GC child cleanup** (update `gc_worker.cpp`):

When processing G:O entries, after freeing data, add a child cleanup step. The G:O `flags` byte (already stored — see `GcValueHeader`) tells us whether external children exist:

```cpp
// After freeing blob/D: data for this G:O entry:
if (gc_val.hdr.flags & kFlagExternalTags) {
  // Or unconditionally — range_clear is a no-op on empty range
  auto c_prefix = make_c_prefix(parts->bucket_id, parts->ref_tag);
  auto c_end = c_prefix;  // append 0xFF to get exclusive end
  c_end.append("\xFF", 1);
  tr->range_clear(c_prefix.view(), c_end.view());
}
```

This is a single FDB `range_clear` covering all child types for the ref_tag. Safe to call even if no children exist (no-op). The end key is `C:<ref_tag>\xFF` which is past all single-byte child type suffixes (`A`, `E`, `T`).

**Note:** `range_clear` maps to FDB's `fdb_transaction_clear_range()`. The KvStore wrapper does NOT have this yet — only `del(key)` exists. Add to `kv_store.hpp` / `kv_store.cpp`:
```cpp
// kv_store.hpp — in KvTransaction class:
void range_clear(std::string_view begin, std::string_view end);

// kv_store.cpp:
void KvTransaction::range_clear(std::string_view begin, std::string_view end) {
  fdb_transaction_clear_range(
      tr_, reinterpret_cast<const uint8_t*>(begin.data()), begin.size(),
      reinterpret_cast<const uint8_t*>(end.data()), end.size());
}
```

### Phase 2: Proto + O: Value Changes

**Proto changes** (`proto/kvrgw.proto`):

```protobuf
message Tag {
  string key = 1;
  string value = 2;
}

message PutObjectTaggingRequest {
  string tenant_name = 1;
  string bucket_name = 2;
  string key = 3;
  repeated Tag tags = 4;
}
message PutObjectTaggingResponse {}

message GetObjectTaggingRequest {
  string tenant_name = 1;
  string bucket_name = 2;
  string key = 3;
}
message GetObjectTaggingResponse {
  repeated Tag tags = 1;
}

message DeleteObjectTaggingRequest {
  string tenant_name = 1;
  string bucket_name = 2;
  string key = 3;
}
message DeleteObjectTaggingResponse {}
```

Add RPCs to service:
```protobuf
rpc PutObjectTagging(PutObjectTaggingRequest) returns (PutObjectTaggingResponse);
rpc GetObjectTagging(GetObjectTaggingRequest) returns (GetObjectTaggingResponse);
rpc DeleteObjectTagging(DeleteObjectTaggingRequest) returns (DeleteObjectTaggingResponse);
```

**O: value changes** (`object_value.hpp` / `object_value.cpp`):

- Add `kFlagExternalTags = 0x08` to flags byte constants.
- Add `TagSet tags` and `bool tags_external` to `ObjectValue`.
- Extend `OValueBuf::kMaxSize` to `sizeof(ObjectValueHeader) + kMaxContentTypeLen + 256 + kMaxInlineTagPayload` (was just + 256 for inline data).
- `parse_object_value()` — if `tags_count > 0 && !kFlagExternalTags`: parse inline tag payload from tail.
- `write_object_value()` — if tags non-empty and inline: append tag payload after inline_data; set `hdr.tags_count`.

### Phase 3: Backend RPCs (dual-mode)

**PutObjectTagging** (`service_impl.cpp`):

Follows the four-case model from the design doc:

```
1. Resolve tenant_id, bucket_id (cached)
2. check_access(kDenyWrite)
3. Validate: tags.size() ≤ 10, each key 1–128, each value 0–256, aggregate ≤ 5120B
4. Encode new tag payload; compute payload_size
5. txn (retry 10x):
     verify_bucket_in_txn(B, kDenyWrite)
     get(S:O) → if null → NOT_FOUND; if delete_marker → NOT_FOUND
     Parse existing ObjectValue (tags_count, external flag, ref_tag)

     Case A — (current: inline/none) AND (new: fits inline)
       Rebuild O: with inline tags + tags_count = new_count
       put(S:O)

     Case B — (current: inline/none) AND (new: exceeds threshold)
       Rebuild O: with NO inline tags, set kFlagExternalTags, tags_count = new_count
       put(S:O)
       put(C:<ref_tag>T, encoded_payload)

     Case C — (current: external) AND (new: exceeds threshold)
       put(C:<ref_tag>T, encoded_payload)    // overwrite
       Rebuild O: with updated tags_count
       put(S:O)

     Case D — (current: external) AND (new: fits inline)
       del(C:<ref_tag>T)
       Rebuild O: with inline tags, clear kFlagExternalTags, tags_count = new_count
       put(S:O)

     commit
```

All cases write O: (with updated tags_count) → write-write conflict on FDB.

**GetObjectTagging** (`service_impl.cpp`):

```
1. Resolve tenant_id, bucket_id (cached)
2. check_access(kDenyRead)
3. Snapshot txn:
     get(S:O) → if null → NOT_FOUND; if delete_marker → NOT_FOUND
     if tags_count == 0: return empty
     if inline (no external flag): return decoded inline tags
     if external: get(C:<ref_tag>T) → return decoded payload
```

Single snapshot transaction (both reads in same snapshot for consistency).

**DeleteObjectTagging** (`service_impl.cpp`):

```
1. Resolve tenant_id, bucket_id (cached)
2. check_access(kDenyWrite)
3. txn (retry 10x):
     verify_bucket_in_txn(B, kDenyWrite)
     get(S:O) → if null → NOT_FOUND; if delete_marker → NOT_FOUND
     if external flag set: del(C:<ref_tag>T)
     Rebuild O: with no tags, clear kFlagExternalTags, tags_count = 0
     put(S:O)
     commit
```

### Phase 4: Frontend Wiring

**Go frontend** (`frontend/backend_kvrgw.go`):

- Implement `PutObjectTagging(ctx, input) → (PutObjectTaggingOutput, error)` — map `input.Tags` to proto `repeated Tag`, call gRPC.
- Implement `GetObjectTagging(ctx, input) → (GetObjectTaggingOutput, error)` — call gRPC, map proto tags to output.
- Implement `DeleteObjectTagging(ctx, input) → (DeleteObjectTaggingOutput, error)` — call gRPC.

versitygw handles S3 XML parsing/serialization for tagging APIs natively.

### Phase 5: CopyObject Tag Handling

Update CopyObject:
- When `replace_metadata = false`:
  - If source has inline tags: byte-copy into destination O: value.
  - If source has external tags: read C:T(src_ref_tag), write C:T(dst_ref_tag) with same payload.
- When `replace_metadata = true`: destination gets no tags (AWS behavior: REPLACE strips metadata). Proto `CopyObjectRequest` may gain `repeated Tag tags` for explicit tag pass-through later.
- Tags are independent copies (no ref-counting on C:T — tags are small, always fully copied).

### Phase 5.5: move_object_to_g Update

The existing `move_object_to_g()` (line 441 in `service_impl.cpp`) decides whether to clean inline or defer to GC. Currently:
```cpp
const bool can_clean_inline =
    (chunk.type == CHUNK_INLINE || (chunk.type == CHUNK_CHILD_D && !coalescing))
    && !value.has_annotations();
```

With external tags, this needs to also check for external children:
```cpp
const bool can_clean_inline =
    (chunk.type == CHUNK_INLINE || (chunk.type == CHUNK_CHILD_D && !coalescing))
    && !value.has_annotations()
    && !(value.hdr.flags & kFlagExternalTags);  // ← NEW: external tags need GC cleanup
```

If the object has `kFlagExternalTags` set, it MUST go through G:O → GcWorker path so the worker can `range_clear` the C:T entry. The flags byte is already copied to `gc_hdr.flags` (line 471), so the GcWorker will see it.

### Phase 6: Tests

**Ceph s3-tests** (add to `scripts/run_ceph_rgw_tests.sh`):
- `test_put_obj_with_tags`
- `test_get_obj_tagging`
- `test_put_modify_tags`
- `test_delete_tags`
- `test_put_max_tags` (10 tags)
- `test_put_excess_tags` (>10 → error)
- `test_put_max_kvsize_tags` (aggregate near 5120B — exercises external mode)
- `test_copy_object_preserves_tags`

**Integration test** (standalone script `scripts/test_tagging.sh`):
- Upload object with aws cli `--tagging "Key1=Val1&Key2=Val2"` (inline)
- Verify via `get-object-tagging`
- Put 10 tags with long values (force external mode)
- Verify via `get-object-tagging`
- Replace with small tag set (external → inline transition, Case D)
- Delete tags
- Verify GC cleans up C:T entries for deleted objects

---

## Files to Modify

| File | Changes |
|------|---------|
| `backend/src/constants.hpp` | Add tag constants, `kFlagExternalTags`, child type chars |
| `backend/src/keys.hpp` / `keys.cpp` | Add `make_ct_key()`, `make_c_prefix()`, C: key parsing |
| `backend/src/tag_value.hpp` / `tag_value.cpp` | **NEW** — encode/decode tag payload, validation |
| `backend/src/object_value.hpp` | Extend OValueBuf kMaxSize; add `tags`, `tags_external` to ObjectValue; `kFlagExternalTags` |
| `backend/src/object_value.cpp` | Parse/write inline tags in O: value tail |
| `backend/src/gc_worker.cpp` | Add `RangeDelete(C:<ref_tag>)` to G:O processing |
| `backend/src/service_impl.hpp` | Declare 3 new RPC handlers |
| `backend/src/service_impl.cpp` | Implement PutObjectTagging (4-case), GetObjectTagging, DeleteObjectTagging |
| `proto/kvrgw.proto` | Add Tag message, 3 request/response messages, 3 RPCs |
| `frontend/backend_kvrgw.go` | Implement 3 tagging methods on KvRgwBackend |
| `scripts/run_ceph_rgw_tests.sh` | Add tagging test names to the enabled list |

---

## What This Does NOT Include (deferred)

- **Versioned tagging** (`versionId` parameter) — requires version resolution logic from child-kv-operations.md. Defer to M4+.
- **Tag-based lifecycle filtering** — lifecycle not implemented in POC.
- **Tag-based access policies** — `s3:ResourceTag` condition keys not implemented.
- **PutObject with inline tags** (`x-amz-tagging` header on PUT) — could be Phase 5.5; low priority vs the three tagging APIs.
- **C:A (annotations), C:E (extended value)** — C: infrastructure is built generically but only `T` child type is used in this plan.

---

## What the C: Infrastructure Enables (future)

The child KV infrastructure built in Phase 1 is generic — it supports any child type keyed by `C:<ref_tag><type><id>`. Future uses:

| Child type | Key suffix | Use case |
|------------|-----------|----------|
| `T` (tags) | `C:<ref_tag>T` | This plan |
| `A` (annotation) | `C:<ref_tag>A<name>` | Object annotations (M4+) |
| `E` (extended) | `C:<ref_tag>E` | Metadata overflow (M4+) |

The `RangeDelete(C:<ref_tag>)` in GcWorker cleans all child types atomically — no per-type cleanup logic needed.

---

## Estimated Effort

| Phase | Effort |
|-------|--------|
| Phase 1: C: infrastructure (keys, GC cleanup) | ~3h |
| Phase 2: Proto + O: value changes | ~2h |
| Phase 3: Backend RPCs (4-case PutObjectTagging) | ~4h |
| Phase 4: Frontend wiring | ~1h |
| Phase 5: CopyObject tag handling | ~2h |
| Phase 6: Tests | ~2h |
| **Total** | **~14h** |
