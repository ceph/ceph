# Multipart Upload Support — Implementation Plan

## Execution instructions

Start with **Milestone 1 only** (Phases 1–4 + Phase 7, in the order listed under "Suggested implementation order" at the end of this document):

- key schema
- `Initiate`/`UploadPart`/`ListParts`/`ListMultipartUploads`
- `Complete`/`Abort`
- `Sweeper`/`GcWorker` extensions
- the `perf_driver` corner-case test suite

Read "Context for implementers" below first — it lists required background docs and key existing source files to read before writing any code.

Everything marked **LOCKED** in this plan is a finalized decision — implement it exactly as written; do not re-derive, simplify, or second-guess it. If something written as **LOCKED** appears to actually be broken or contradictory once you dig into the code, stop and flag it rather than silently deviating.

Do **not** start Milestone 2 (Phase 5: C ABI, Phase 6: Go frontend) until Milestone 1's Phase 7 test suite passes.

Phase 8 (reading back / deleting a completed multipart object) is explicitly deferred and out of scope for now.

Two items are explicitly unresolved, listed under "Open discussion items" near the end of this document — stop and ask before deciding on those rather than picking an approach yourself.

## Context for implementers

This plan adds S3 Multipart Upload support to the KV-RGW POC at `src/rgw/kvrgw_poc/` (FoundationDB-backed metadata + local-filesystem `DataStore` data tier + C++ backend `KvRgwServiceImpl` + C ABI `kvrgw_c_api` + Go frontend implementing versitygw's `backend.Backend` via cgo).

**Required reading before implementing**, roughly in this order:

- `docs/POC-Knowledge.md` — overall POC architecture and conventions.
- `docs/S3-Operations-Over-KV.md` §"Multipart Upload", §"GC Namespace", §"Pending Operations Namespace" — background only. This plan's key schema (Phase 1) **deliberately departs** from that doc's `:M:`-keyed-by-object_name scheme — see "Scope and key decisions" below for why, and treat *this* document as the sole source of truth for the key schema.
- `docs/Listing-and-Key-Scheme.md` — KV namespace/category/key conventions (general background only — same caveat as above regarding the multipart-specific key layout).
- `docs/KV-Based-Design-For-RGW.md` — broader KV design rationale.
- `docs/transaction-safety.md` — needed to follow this plan's race-safety arguments (especially the FDB-vs-TiKV conflict-detection difference: TiKV only detects write-write conflicts, not range-read conflicts).
- `docs/data-tiering.md` — source of the "multipart parts always use Tier 3" rule.

**Key existing source files this plan builds on** (read before writing new code; paths relative to `src/rgw/kvrgw_poc/`):

- `backend/src/constants.hpp`, `backend/src/key_buf.hpp`, `backend/src/keys.hpp`/`.cpp` — namespace/category constants, packed key-header structs (`KeyHeaderS`, `KeyHeaderG`, etc.), key builder/parser functions.
- `backend/src/object_value.hpp`/`.cpp` — `ObjectValue`/`ObjectValueHeader`, `ChunkDescriptor`/`ChunkType`.
- `backend/src/gc_value.hpp`/`.cpp` — `GcDescriptor`, `GroupGcValueBuf`, the existing group-batching pattern this plan's `G:U` group mirrors.
- `backend/src/etag.hpp`/`.cpp` — the `ETag` wire-format class this plan extends with `CompositeEtagBuilder`.
- `backend/src/sweeper.cpp`, `backend/src/gc_worker.cpp` — existing background-cleanup workers this plan extends.
- `backend/src/service_impl.hpp`/`.cpp` — the main backend implementation (`KvRgwServiceImpl`). `put_object`/`put_finalize`/`copy_object`/`delete_multi_*`/`list_buckets` are the existing functions referenced throughout this plan as patterns to follow or reuse.
- `backend/include/kvrgw_c_api.h`, `backend/src/kvrgw_c_api.cpp` — C ABI (Milestone 2 only).
- `frontend/kvrgwc/kvrgwc.go`, `frontend/backend_kvrgw.go` — Go frontend / versitygw `backend.Backend` implementation (Milestone 2 only).

### KV key-scheme primer

Minimum needed to read this plan — see `docs/Listing-and-Key-Scheme.md` for the general scheme (background only, not the source of truth for multipart keys):

- Keys are `[namespace char][...]`. This plan uses namespace `S` (current objects, versions, and — new here — multipart head/part rows) and `G` (garbage-collection directives).
- Within `S`: `O` (current object), `V` (version), and new in this plan:
  - `M` — multipart **head** row only (one per upload, written at `Initiate`, deleted by `Complete`/`Abort`).
  - `U` — multipart **part** rows (one per `(part_number, generation)`, written by `UploadPart`).
- Within `G`: `O` (single GC instance), `G` (grouped GC instances, up to 20/batch), and new in this plan:
  - `M` — whole-upload cleanup directive (one per `Complete`/`Abort`).
  - `U` — per-part-generation cleanup, **always a group of 1–20 entries** (covers both a single re-upload obsoletion and `Complete`'s batched excluded-parts cleanup — see Phase 1).
- `ref_tag` — a 12-byte globally-unique identifier (4B `rgw_id` + 8B `seq_id`), generated via `RefTagGenerator`/`ref_tags_.next()`. One multipart upload has exactly one `ref_tag`, shared by its head and every part/generation.
- `bucket_id` — resolved from `(tenant_id, bucket_name)` via `get_bucket_id_cached`/`resolve_bucket_verify`; present in every key header used here (`KeyHeaderS`, `KeyHeaderG`), even though `S:U`/`G:M`/`G:U` no longer carry `object_name`.
- Every KV range-scan introduced by this plan must use the zero-copy `FdbRangeHolder` pattern (future resolved once, `string_view`s into the holder's own buffer, valid for the holder's lifetime) — see `KvRgwServiceImpl::list_buckets()` in `service_impl.cpp` for the reference example.

### Conventions used in this document

- **LOCKED** marks a design decision that has already been fully settled through review — implement exactly as written; do not re-derive or second-guess it.
- "Milestone 1" / "Milestone 2" — see "Milestone boundary" below for the staged-delivery split. Do not start Milestone 2 work until Milestone 1's Phase 7 test suite passes.
- Phase numbers (1–8) indicate a suggested build order (see "Suggested implementation order" at the end), not strict dependencies beyond what each phase's text says it needs from earlier phases.

## Scope and key decisions

- **Staged delivery:** Milestone 1 builds and tests the core upload protocol entirely at the backend (`KvRgwServiceImpl`) layer, exercised only through `perf_driver` — no C ABI, no Go frontend. Milestone 2 (only started after Milestone 1's test suite passes) wires it through the C ABI (`kvrgw_c_api`) → Go cgo shim (`kvrgwc`) → versitygw `backend.Backend` (`backend_kvrgw.go`). All six multipart methods required by versitygw's interface get real implementations except `UploadPartCopy`, which stays on the `backend.BackendUnsupported` stub (deferred — not discussed in the design docs either). See "Milestone boundary" below.

- **Fidelity:** custom protocol, simplified from the design docs and locked through discussion (see Phase 2/3 "locked design" sections below) — no `P:M` at all; `:M:` head fencing is a direct delete (Complete/Abort) rather than an absorb-into-`P:M` step; re-upload uses a `generation`-in-key + `Obsolete`-flag scheme instead of the docs' `pending/cleanup/committed` state machine; `G:M` has two modes (`DEEP`, `MIXED` — no separate `SHALLOW`) processed by a `GcWorker` cleanup loop; a single, unified `G:U` directive (always a group of 1–20 entries) handles both re-upload cleanup and excluded-part cleanup from a successful `Complete`.

- **Head/part key split — LOCKED, the key design decision of this plan:** the design docs key *every* `:M:` row — head and parts alike — by `object_name + ref_tag + part_number`. This plan deliberately splits that in two:

  - There is **no such thing as "part 0."** The head row, written once by `InitiateMultipartUpload`, is a distinct entity — upload metadata (ref_tag, object_name, requested attributes) — not a part. It lives in category `S:M`, keyed by `object_name + ref_tag`, with **no `part_number` field at all**.
  - Part rows (`part_number` 1–10000, written by `UploadPart`) live in a **separate category, `S:U`**, keyed by `ref_tag + part_number + generation` — fixed-size, **no `object_name`**.

  Why: `ref_tag` already globally and uniquely identifies the upload, so parts never need `object_name` to be found or scanned. Carrying a 1–1024B `object_name` on every one of up to 10,000 part rows would make `CompleteMultipartUpload`'s whole-upload range-scan (and every other per-part scan: `ListParts`, `GcWorker`'s `G:M` pass) far too expensive to fit the "keys and data are short, under ~128B combined per entry, ~1MB total for 10,000 parts" budget this plan's range-scans are sized against. Splitting the category also means `ListMultipartUploads`'s bucket-wide scan of `S:M` now *only ever* encounters head rows — no skip-logic needed at all, since parts physically aren't in that category anymore.

- **Versioning:** **LOCKED** — full support, confined entirely to `CompleteMultipartUpload`'s final write step (after all multipart-specific checks pass, immediately before generating the `:O:` KV — see "Complete / Abort — locked design" step 6 below). No other operation (`Initiate`, `UploadPart`, `Abort`, `ListParts`, `ListMultipartUploads`, `GcWorker`) needs any versioning awareness — a multipart upload's `S:M`/`S:U`/`G:M`/`G:U` bookkeeping is entirely version-agnostic; only the moment the assembled manifest finally becomes a real `:O:` value does versioning matter, and it does so via a new shared helper (`displace_and_version`) extracted from logic currently duplicated inline across `put_finalize`'s two branches and `copy_object`'s destination-write path — code reuse, not a parallel reimplementation. See "Complete / Abort — locked design" step 6 for the exact helper. `PutObjectResult::version_id` (already part of `complete_multipart_upload`'s declared return type) gets populated the same way `put_finalize` populates it.

- **Storage tier:** every part always uses Tier 3 (`DataStore`, `CHUNK_STORAGE`) regardless of size — matches `docs/data-tiering.md` ("Multipart uploads always use Tier 3"). No `D:`/inline path for parts.

- **Sharding:** keys keep the POC's existing fixed `kShardCount=1`/`kShardId=0` — consistent with every other namespace in this POC (no resharding anywhere). `S:U`/`G:M`/`G:U` still carry `bucket_id` via `KeyHeaderS`/`KeyHeaderG` even though they've dropped `object_name` — scoping stays per-bucket.

- **Range scans:** every `RangeScan` introduced by this plan — `UploadPart`'s per-part generation scan, `ListParts`, `ListMultipartUploads`, `Complete`'s full-upload scan, `GcWorker`'s `G:M` processing — must use the zero-copy `FdbRangeHolder` pattern. Per the head/part split above, every one of these scans except `ListMultipartUploads` scans `S:U` by `ref_tag` alone (never `object_name`); `ListMultipartUploads` scans `S:M` bucket-wide (heads only).

- **Deliberate deviations from the design docs**, kept consistent with this POC's existing conventions:

  - No `P:M` at all — `CompleteMultipartUpload` and `AbortMultipartUpload` both fence by directly deleting the `S:M` head in the same txn that writes the `G:M` directive (and, for Complete, the final `:O:`). A crash before that txn commits leaves the upload exactly as it was (nothing partial to recover); a crash after is just the normal `G:M` background cleanup.
  - `CompleteMultipartUpload` requires every part's highest `generation` to be `committed` (no `pending` stragglers) at scan time; if any part is still `pending`, the whole upload is aborted (same code path as `AbortMultipartUpload`, `G:M{DEEP}`) and the client's Complete call fails with `InvalidPart` (400/409) — not just "retry later".
  - `G:M` has two modes only: `DEEP` (abort, or Complete-triggered-abort — free every part's data, since nothing was pre-classified) and `MIXED` (successful Complete — excluded parts' data cleanup was already delegated to `G:U` groups written alongside it, so `MIXED` only ever has to clean up `committed`/included rows, now superseded by the final `:O:`). No separate `SHALLOW` mode. Both modes operate entirely on `S:U` rows — the `S:M` head is already gone by the time `G:M` exists.
  - Re-upload handling uses a `generation` field in the `S:U` part **key** (not value) plus an `Obsolete` flag in the value, instead of the docs' `pending/cleanup/committed` state machine — see "UploadPart — locked design" below. A single, unified `G:U` directive (`ref_tag + part_number + generation` of the group's first entry, value = 1–20 entries) drives per-part-generation data-tier cleanup, independent of the whole-upload `G:M` directive.
  - No lifecycle engine in this POC — `AbortIncompleteMultipartUpload`-style expiry of abandoned uploads is out of scope; only client-driven `Abort`/`Complete` are handled.

- **AWS input validation** — **LOCKED**, see "UploadPart — locked design" (Phase 2) and "Complete / Abort — locked design" (Phase 3) for exact placement:

  - `UploadPart`:
    - `part_number < 1 || part_number > 10000` → reject that single request only, `400 Bad Request`/`InvalidArgument` ("Part number must be an integer between 1 and 10000, inclusive.").
    - `part_size == 0` → reject, `400`/`InvalidArgument`.
    - `part_size > 5GB` → reject, `400`/`EntityTooLarge` ("Your proposed upload exceeds the maximum allowed size.").
    - All three are a pre-check before Phase 1 touches any KV — rejection writes nothing, the upload session is unaffected, client can resubmit a legal part for that `part_number` afterward.
  - `CompleteMultipartUpload`:
    - `client_parts.size() < 1 || client_parts.size() > 10000` → `400`/`InvalidArgument`.
    - Order/duplicate check (`part[i+1].part_number > part[i].part_number` for every adjacent pair) → `InvalidPartOrder`.
    - **Both are a Phase 0 pre-check before any transaction opens** — pure client-payload validation with no KV dependency, so they reject immediately with no teardown, regardless of whether the referenced upload exists (see "Complete / Abort — locked design").
  - `CompleteMultipartUpload`: every folded live (`committed`) part must be `≥5MB`, **except** the row that is the globally-last key in the scanned `ref_tag` range (exempt, any size `1B–5GB`) — enforced inside `scan_and_resolve_parts`. A violation on a non-last part → `abort=true` (same full-teardown path), `EntityTooSmall`.

## Phase 1 — Key schema & constants

Files: `backend/src/constants.hpp`, `backend/src/key_buf.hpp`, `backend/src/keys.hpp`, `backend/src/keys.cpp`.

### Constants

- `kCategoryMultipartHead = 'M'` — `S:` namespace, upload-head rows only.
- `kCategoryMultipartPart = 'U'` — `S:` namespace, part rows only.
- `kOpTypeMultipart = 'M'` — `G:` namespace, whole-upload directive.
- `kOpTypeMultipartPart = 'U'` — `G:` namespace, per-part-generation cleanup group (unified single/batched).

(Both constants.hpp alongside existing `kCategoryObject`/`kCategoryVersion`/`kOpTypeObject`/`kOpTypeGroup`.)

### `S:M` — multipart head row

One per upload, written once by `InitiateMultipartUpload`, deleted by `Complete`/`Abort`. **Not a part** — no `part_number` field.

```
KeyHeaderS(ns='S', cat='M') + object_name + ref_tag(12B)
```

- `make_m_head_key(bucket_id, object_name, ref_tag, KeyBuf&)`.
- `make_m_bucket_prefix(bucket_id)` — bucket-wide prefix, header only. Used by `ListMultipartUploads`. Because this category contains *only* head rows, the scan needs no skip-logic for part rows at all.
- `parse_m_head_key()` — mirrors other `object_name`-tail parsers: `object_name = key[hdr_len:-12]`, `ref_tag = key[-12:]`.

### `S:U` — multipart part row

One per `(ref_tag, part_number, generation)`, written/updated by `UploadPart`. Fixed-size, no `object_name`.

```
KeyHeaderS(ns='S', cat='U') + ref_tag(12B) + part_number(2B BE) + generation(2B BE)
```

- `make_u_part_key(bucket_id, ref_tag, part_number, generation, KeyBuf&)`.
- `make_u_prefix(bucket_id, ref_tag)` — whole-upload **parts** prefix (all part_numbers, all generations). Used by `ListParts`, `scan_and_resolve_parts`, `GcWorker`'s `G:M` pass. Does **not** include the head — head existence/deletion is always a separate `Get`/`kv_del` on the `S:M` key.
- `make_u_part_prefix(bucket_id, ref_tag, part_number)` — single-part, all-generations prefix. Used by `UploadPart` Phase 1 to find the current highest generation for one part.
- `parse_u_key()` — fixed-offset, no backward-parse needed (no variable-length field at all): `ref_tag = key[hdr_len:hdr_len+12]`, `part_number = key[hdr_len+12:hdr_len+14]`, `generation = key[hdr_len+14:hdr_len+16]`.

### `G:M` — whole-upload cleanup directive

Written by `Complete`/`Abort` in the same txn that deletes the `S:M` head. Fixed-size (unlike the design docs' variable-length `object_name`-carrying version) — it only ever needs to scan `S:U` by `ref_tag`, never the head.

```
KeyHeaderG(ns='G', cat='M') + ref_tag(12B)
```

- `make_gm_key(bucket_id, ref_tag, KeyBuf&)`, `parse_gm_key()` — trivial fixed-offset extraction.
- Value: `GmValue` — mode byte `{DEEP, MIXED}` only. See "Value structs" below.

### `G:U` — per-part-generation cleanup, unified group (1–20 entries)

**LOCKED, consolidated:** a single category handles both cases the design docs (and an earlier draft of this plan) kept separate — a lone re-upload obsoletion (`UploadPart` superseding its own prior generation) and `Complete`'s batched excluded-parts cleanup. Both are simply "a group of `G:U` entries," sized 1 for the former, up to 20 for the latter — no separate single-entry vs. group-entry category.

```
KeyHeaderG(ns='G', cat='U') + ref_tag(12B) + part_number(2B)/generation(2B)   [of the group's first entry]
```

- `make_gu_key(bucket_id, ref_tag, first_part_number, first_generation, KeyBuf&)` — the first entry's `(part_number, generation)` is sufficient as a unique discriminator (every `(ref_tag, part_number, generation)` triple is globally unique).
- Every entry in one group shares the group key's `ref_tag` — carried once, not per-entry.

Value — `GuValueBuf`:

```cpp
struct GuEntry {
  uint16_t part_number{};
  uint16_t generation{};
};  // 4B
static_assert(sizeof(GuEntry) == 4);

struct GuValueBuf {
  // count(1B) + sum_size(8B) + up to kMaxBatchSize entries(4B each)
  static constexpr size_t kMaxSize =
      1 + sizeof(uint64_t) + kMaxBatchSize * sizeof(GuEntry);  // 1 + 8 + 20*4 = 89B
  uint8_t data[kMaxSize];
  size_t len{};
};
```

- `count` (1B): 1–20 (`kMaxBatchSize`) entries in this group.
- `sum_size` (8B): aggregate data-tier bytes across all entries in the group — used by `GcWorker` for rate-limiting accounting (mirroring `G:O`'s non-shared `CHUNK_STORAGE` path), without needing a per-entry size field.
- `entries[count]`: `{part_number, generation}` pairs. `ref_tag` (from the key) + `part_number` + `generation` are sufficient for `GcWorker` to construct the data-tier filename `ref_tag-part_number-generation` for each entry — `chunk_type` is implicitly always `CHUNK_STORAGE` for multipart parts, so it's never stored per entry.
- `make_gu_value(GuValueBuf&, const GuEntry* entries, uint8_t count, uint64_t sum_size)`, `parse_gu_value(std::string_view) → optional<GuValue>`.

`GcWorker`'s `G:U` branch (see Phase 4) always iterates `count` entries (1 to 20) — there is no longer a separate single-entry-vs-group code path.

### Value structs

(`object_value.hpp`/new section)

- `MHeadValue` — initiated timestamp, requested content_type/metadata/tags-count. Populated at `Initiate`, consumed by `Complete` to fill in the final `:O:`'s header fields. **Tags are NOT carried here** — they go straight to the `S:C:T` child entry keyed by the upload's own `ref_tag` (`make_ct_key`), and `Complete` reuses that *same* `ref_tag` for the final object, so the tag entry needs no extra copy/write at Complete time.
- `MPartValue` — **LOCKED**: `state` byte `{pending, committed}` (no `cleanup` state — superseded by the generation+`Obsolete` design), `Obsolete` bool, `ChunkDescriptor`, `ETag`, size, mtime.
- `GmValue` — **LOCKED**: mode byte `{DEEP, MIXED}` only — no inline excluded-parts list (superseded by the `G:U` groups `scan_and_resolve_parts` writes directly for excluded parts; see "Complete / Abort — locked design"). `MIXED` just tells `GcWorker` it only needs to deal with `committed`/included rows (the ones that ended up in the final `:O:`'s manifest).
- `GuValueBuf`/`GuEntry` — see above.

### Data-tier filename

**LOCKED**: `ref_tag-part_number-generation` (not just `ref_tag-part_number`), so a fresh re-upload attempt never shares a filename with a stale attempt still being freed by `GcWorker`.

## Phase 2 — Backend: Initiate / UploadPart / ListParts / ListMultipartUploads

File: `backend/src/service_impl.{hpp,cpp}`.

- **`initiate_multipart_upload(tenant_id, bucket_name, object_name, content_type, tags, metadata) → upload_id`**

  Single blind `kv_put` of the `S:M` head, no transaction (nothing else to coordinate — the head is its own durable record, no data-tier write happens here). **Reuses three small existing utilities directly, nothing larger**:

  - `ref_tags_.next()` (same generator instance used by `put_object`) for the upload's `ref_tag`.
  - `get_bucket_id_cached(tenant_id, bucket_name)` for the soft bucket check (hard check deferred to Complete, matching the docs).
  - `now_unix()` (`backend/src/fdb_latency.hpp`) for the head's "initiated timestamp" — the same timestamp primitive already used for `tenant`/`bucket` creation, `PoEntry.gc.mtime`, and `copy_object`'s `last_modified_sec`, not `put_object`'s own separate inline `std::chrono` nsec-precision snippet.

  **Does not reuse** `put_object_route`/`put_object_in_txn` or the `P:O`/`InFlightBatch` pipelined-write machinery — that exists to coordinate a crash-recoverable data-tier write for one object, which doesn't apply here (no data is written at `Initiate` time).

- **LOCKED**: a malformed upload ID (client-supplied string that doesn't decode to a valid 12-byte `ref_tag`) is handled uniformly as `NoSuchUpload` — it simply fails to find an `S:M` head, same as a well-formed-but-nonexistent/already-completed/aborted upload ID. No separate error path.

- **`upload_part(tenant_id, bucket_name, object_name, ref_tag, part_number, data)`**

  **LOCKED** — see "UploadPart — locked design" below, including the Phase 0 AWS size/part-number validation pre-check. Deviates from the design doc's `pending/cleanup/committed` model; no separate `P:` entry for parts (the `S:U` row itself is the coordination record, same role as `P:O`). `object_name` is needed only to build/read the `S:M` head key in Phase 1 — every part-row operation after that uses `ref_tag` alone. **Intentionally does no separate bucket-existence check** — its own `Get(S:M head)` in Phase 1 (`NoSuchUpload` if missing) is the only existence gate.

- **`list_parts(..., ref_tag, continuation) → ListPartsResult`**

  Range scan `make_u_prefix(bucket_id, ref_tag)` (parts only — no need to skip anything, the head physically isn't in this category), grouped by `part_number` and reduced to `max(generation)` per part (guaranteed to be the live row — see locked design); **a part whose live row is still `state=pending` is skipped entirely** (not yet "really" uploaded from the client's perspective — matches S3 semantics, since `UploadPart` is synchronous). Same pagination style as `list_objects`.

  **LOCKED**: fire `async_get(bucket)`, `async_get(S:M head)`, and `async_range_scan(S:U prefix)` all concurrently; `wait()`/`process()` the range-scan results first, then check the head future — missing → `NoSuchUpload` — then the bucket future — missing → `NoSuchBucket` (so the two existence checks run concurrently with the range-scan instead of serially, but are validated in this fixed order: head before bucket).

- **`list_multipart_uploads(..., prefix, continuation) → ListMultipartUploadsResult`**

  Range scan `make_m_bucket_prefix(bucket_id)` over `S:M`. Because this category contains *only* head rows post-split, every row the scan encounters is by construction a head — no filtering needed. Same style as `list_objects`.

  **LOCKED**: same overlapped `async_get(bucket)` + `async_range_scan` pattern as `list_parts` (no per-upload head check needed here — the scan itself only ever surfaces heads).

### UploadPart — locked design

Every generation-superseding step and every data write is its own request's job; the only thing shared across requests for the same `part_number` is the `S:U` row set for that part_number. Invariant: at most one row per `part_number` is ever in a "live" state (`pending` with `Obsolete=false`, or `committed`) at a time — always the highest `generation` present; any lower-generation rows present are necessarily `Obsolete=true` and awaiting self-cleanup by their own writer.

**Phase 0 (pre-check, no KV touched)** — **LOCKED**:

- `part_number < 1 || part_number > 10000` → reject this request only, `400`/`InvalidArgument`.
- `part_size == 0` → reject, `400`/`InvalidArgument`.
- `part_size > 5GB` → reject, `400`/`EntityTooLarge`.

All three happen before Phase 1 begins — nothing is written, the upload session is unaffected, and the client can resubmit a legal part for the same `part_number` with no side effects.

**Phase 1 (txn)** — `Get(S:M head, object_name+ref_tag)`; if missing → return `NoSuchUpload`, abort (no further writes). Otherwise range-scan `make_u_part_prefix(bucket_id, ref_tag, part_number)` for the current highest-generation row, then:

- **No row exists** → create `gen=0, state=pending, Obsolete=false`. This request's generation = 0.
- **Highest row is `committed` (gen=N)** → write `G:U` for gen=N (group of 1; data confirmed to exist — safe to free immediately) + delete the gen=N row + create `gen=N+1, state=pending, Obsolete=false`. This request's generation = N+1.
- **Highest row is `pending` (gen=N)** → set `Obsolete=true` on the gen=N row (left in place, not deleted — file may not exist yet, can't GC it safely) + create `gen=N+1, state=pending, Obsolete=false`. This request's generation = N+1. The gen=N row's own writer will notice `Obsolete=true` in its own Phase 3 and self-convert to a `G:U` group of 1.
- Commit.

**Phase 2** — `data_store_.write("ref_tag-part_number-generation", data)` — no KV, this request's own generation's filename.

**Phase 3 (txn)** — read own row (`ref_tag, part_number, gen` = this request's own generation), then:

- `state=pending` (still `Obsolete=false`) → update in place: `state=committed` + final `ChunkDescriptor`/`ETag`/size + refreshed `mtime` (via `now_unix()`, same shared utility as `Initiate` — see Phase 2). `mtime` is reported as `LastModified` in `ListParts` output; it has no role in `GcWorker`'s cleanup decisions (no aging/timestamp check is used anywhere in this design — see Phase 4's "messy case").
- `Obsolete=true` (superseded by another `UploadPart`, **or by `Abort`/Complete-triggered-abort — see below**) → write `G:U` for own generation (group of 1; data now confirmed to exist) + delete own row, **and return `NoSuchUpload`/404 to the caller instead of success** — the upload may no longer exist by the time this part's write landed.
- Commit.

**Race safety (two Phase-1 transactions racing on the same part_number, same observed highest generation N):** both must write to generation N's exact row (`Obsolete=true`, or delete-after-`G:U`) and both independently compute the identical new-row key `gen=N+1` to insert. Either collision alone is a **write-write conflict on a shared key** — detected on FDB *and* TiKV (TiKV only detects write-write conflicts, not range-read conflicts, per `transaction-safety.md`). The loser retries, re-scans, and correctly lands on `gen=N+2`. No dependence on FDB-specific range-conflict tracking.

## Phase 3 — Backend: Complete / Abort

File: `backend/src/service_impl.{hpp,cpp}`.

- **`complete_multipart_upload(..., ref_tag, client_parts) → PutObjectResult`**: **LOCKED** — see "Complete / Abort — locked design" below. Single transaction, no `P:M`, no lock-free scan phase — the whole range-scan (worst case ~10,000 parts, each row now a fixed ~24B key + small value with no `object_name` tax) fits comfortably inside one FDB txn's size/time budget.

- **`abort_multipart_upload(..., ref_tag)`**: **LOCKED** — single txn: `Get(S:M head)` → missing → return success (idempotent); else `scan_and_resolve_parts(ref_tag, abort_in=true, client_parts={})` (marks any live `pending` row `Obsolete=true`), `kv_del(S:M head)` + `make_gm_key` + `GmValue{DEEP}`, commit, return success — see "Complete / Abort — locked design" below.

### Complete / Abort — locked design

**`CompleteMultipartUpload` Phase 0 (pre-check, no KV touched)** — **LOCKED**: pure client-payload validation, before any transaction opens:

- `client_parts.size() < 1 || client_parts.size() > 10000` → `400`/`InvalidArgument` ("The number of parts specified in the CompleteMultipartUpload request must be between 1 and 10000, inclusive.").
- **Order/duplicate check** — single pass verifying `part[i+1].part_number > part[i].part_number` for every adjacent pair (strictly ascending; already enforced upstream by versitygw, but cheap enough to re-check here). Violation → `InvalidPartOrder`. This also rejects duplicate `part_number` entries (two equal values fail the strict-`>` check).

Both checks are pure request-shape validation with zero KV dependency, so they run **before** `Get(S:M head)` — a malformed request is rejected immediately regardless of whether the referenced upload exists, with no risk of triggering a KV-side teardown for an unrelated reason.

**Shared helper**

```
scan_and_resolve_parts(tr, ref_tag, client_parts, abort_in)
  → {abort_out, manifest?, etag?, max_generation?}
```

Single range-scan over `make_u_prefix(bucket_id, ref_tag)` — **note: no `object_name` parameter at all**, since `S:U` rows are keyed purely by `ref_tag`. Used by both `AbortMultipartUpload` (`abort_in=true`, `client_parts` empty) and `CompleteMultipartUpload` (`abort_in=false`, Phase 0 already validated and sorted `client_parts`).

Scans the whole `ref_tag` prefix in key order (generations for the same part_number are contiguous — take the highest as that part's live row); `abort` starts at `abort_in` and is carried through the scan.

**LOCKED — two sub-passes, no KV writes until outcome is final**: the scan loop only accumulates *in-memory* state (never calls `kv_put`); actual `G:U` group writes are only issued afterward, once `abort_out` is conclusively known to be `false` — this avoids ever leaving speculative writes buffered in the transaction that would need to be "un-written" if a late-discovered failure flips the outcome to abort.

1. **Per-row pass** (in-memory only):

   - Live row `state=pending` → unconditionally set `abort=true` and mark that row `Obsolete=true` (no branch on whether `abort` was already true — same action every time), continue scanning. **Required for correctness** — without this, an `UploadPart` already past its own Phase 1 (mid Phase 2/3) could still commit its row and report success to its caller *after* this Abort/Complete has already returned — an illegal ordering. See "`UploadPart` — locked design" Phase 3 for the matching `Obsolete` check.

   - Live row `state=committed` and `abort==false` → look up this row's `part_number` in `client_parts` (binary search — already sorted by Phase 0's order check), and if found, increment `matched_count`:

     - **Not found (excluded)** → append `{part_number, generation}` to an in-memory excluded list. **No size/etag check** — it's not going into the final object, so its size/etag is irrelevant. **No data freed here** — deferred to `GcWorker` via a `G:U` group (see below/Phase 4).
     - **Found, etag mismatch** → **any failure switches to abort mode**: unconditionally set `abort=true` (same handling as a `pending` row), `InvalidPart` ("specified ETag does not match"), continue scanning.
     - **Found, etag matches (included)** → **size validation — LOCKED**: must be `≥5MB` **except** when `part_number` equals `client_parts.back().part_number` (the true last part of the *client's* list — not merely the last row physically present in the scan, since trailing rows can be excluded). A smaller size on a non-last included part → `abort=true` (same handling as above), `EntityTooSmall`. If the size check passes: append `(part_number, row.etag)` to an in-memory included list (fed to `CompositeEtagBuilder`/manifest only after this sub-pass fully succeeds — not folded inline, so a later-discovered failure never needs to be "un-folded") and track `max(generation)` seen so far among included parts (selects the final generation-table bit-width — see "Manifest encoding", deferred to Phase 8).

   - Live row `state=committed` and `abort==true` → no action; its data gets freed later by `G:M{DEEP}` regardless.

2. **Coverage check** (after the per-row pass, before any KV write): if `abort` is still `false` but `matched_count < client_parts.size()`, some client-specified `part_number` was never found among committed rows → set `abort=true`, `InvalidPart` ("specified part could not be found").

3. **Finalize** — only reached if `abort==false` after step 2: flush the in-memory excluded list as `G:U` groups (`make_gu_key`/`make_gu_value`, up to 20 entries per group, `sum_size` = the group's total bytes) and `kv_put` them into `tr`; fold the in-memory included list (already in ascending `part_number` order from the scan) into a fresh `CompositeEtagBuilder` and `finalize()` it; assemble the manifest from the same included list.

Returns the final `abort` flag; if `false`, also returns the manifest/`ChunkDescriptor` list, the composite `ETag`, and `max_generation`.

**`AbortMultipartUpload`** — single txn: `Get(S:M head)` → missing → return success (idempotent); else `scan_and_resolve_parts(ref_tag, abort_in=true, client_parts={})` (always returns `abort_out=true`; the empty `client_parts` means every committed row is `abort==true` already by the time it's visited, so no excluded/included bookkeeping or `G:U` writes ever happen — consistent, nothing to flush, step 3 never reached), `kv_del(S:M head)` + `kv_put(G:M key, GmValue{DEEP})`, commit, **return success**.

**`CompleteMultipartUpload`** — single txn (Phase 0 above already ran):

1. `Get(S:M head)` → missing → `NoSuchUpload`, abort.
2. `scan_and_resolve_parts(tr, ref_tag, client_parts, abort_in=false)`.
3. **If `abort_out=true`** (some part was `pending`, had an etag mismatch, failed the `≥5MB` check, or a `client_parts` entry was never matched) → `kv_del(S:M head)` + `kv_put(G:M key, GmValue{DEEP})`, commit, **return `InvalidPart`/`EntityTooSmall`** (400/409) to the Complete caller. `scan_and_resolve_parts` never staged any `G:U`/etag writes in this case (per its "no KV writes until outcome is final" rule), so there's nothing to worry about leaking into this txn — `DEEP` mode frees everything directly, no `G:U` grouping needed for the abort path.
4. Otherwise (`abort_out=false`) → finalize the manifest from the `scan_and_resolve_parts` output (see "Manifest encoding" below — deferred, Phase 8); the composite `ETag` is already finalized.
5. Hard bucket check (`verify_bucket_in_txn`/`resolve_bucket_verify`, `get(B)`) → yields `bucket_state.versioning_state` (`vs`).
6. **LOCKED — versioning, via a new shared helper (code reuse, not a parallel reimplementation)**: read the existing `:O:` (if any) → `old_raw`/`old_hdr`. Extract a small private helper from the sequence currently duplicated inline in `put_finalize`'s two branches (`service_impl.cpp` ~1986–1992, ~2085–2087) and `copy_object`'s destination-write path (~3802–3804):

   ```cpp
   // New shared helper — put_finalize's two branches and copy_object's
   // destination-write path get refactored to call this instead of
   // repeating the inline sequence.
   NewVersionIds KvRgwServiceImpl::displace_and_version(
       KvTransaction &tr, VersioningState vs, std::string_view object_key,
       const ObjectValueHeader *old_hdr, std::string_view old_raw)
   {
     if (old_hdr) {
       displace_old_object(tr, vs, object_key, old_raw);
     }
     return compute_new_version(vs, old_hdr);
   }
   ```

   `CompleteMultipartUpload` step 6 calls this exact same helper — `displace_and_version(tr, vs, object_key, old_hdr, old_raw)` → `{version_id, next_vid}` — assigned to the new object's header before `write_object_value`. This is the *only* point anywhere in the multipart flow that's versioning-aware, and it shares code with `put_finalize`/`copy_object` rather than duplicating their logic a third time.

7. `write_object_value` the final `:O:` with the assembled manifest and the version fields from step 6; populate `PutObjectResult::version_id` from `ids.version_id`.
8. `kv_del(S:M head)` + `kv_put(G:M key, GmValue{MIXED})` — **LOCKED**: `GmValue{MIXED}` no longer carries an inline excluded-parts list (superseded by the `G:U` groups `scan_and_resolve_parts` already staged into this txn); `MIXED` mode for `GcWorker` now just means "excluded-part cleanup was already delegated to `G:U` groups — `GcWorker`'s own job for `MIXED` is just to delete the remaining `S:U` rows (the included ones, now superseded by the final `:O:`) once resolved" (see Phase 4).
9. Commit, return success with the final etag and `version_id`.

**Race safety vs. a concurrent `UploadPart`:** steps 1–8 all happen in one txn. Any `UploadPart` Phase 1 that reads/writes a part row inside the scanned `ref_tag` range before this txn commits creates a write-write (or, on FDB, read-write) conflict on that shared key — one side retries. Because step 3 aborts the whole upload before any data is written if a `pending` part is found, there's no scenario where Complete commits while a part is legitimately still being uploaded.

### Composite ETag — locked design

File: `backend/src/etag.{hpp,cpp}`.

New class `CompositeEtagBuilder`, declared as a `friend` of `ETag` (direct access to its private `bytes_[18]`, no new public accessor needed on `ETag`) — streams MD5 over each included part's raw digest, mirroring the existing `EVP_MD_CTX`/`EVP_md5()` `Init`/`Update`/`Final` pattern already used for single-part ETags in `service_impl.cpp` (~line 2289), just called repeatedly instead of once:

```cpp
class CompositeEtagBuilder {
 public:
  CompositeEtagBuilder();
  ~CompositeEtagBuilder();
  // Fold in one part's ETag, in ascending part_number order.
  void fold(const ETag& part_etag);   // EVP_DigestUpdate(ctx_, part_etag.bytes_, kMd5Bytes); ++part_count_
  // MD5 of the concatenated per-part digests + the number of folds so far.
  ETag finalize() const;              // EVP_DigestFinal_ex(ctx_, digest, ...); return ETag::load_raw(digest, part_count_)
 private:
  EVP_MD_CTX* ctx_;
  uint16_t part_count_ = 0;
};
```

Used inside `scan_and_resolve_parts`'s "Finalize" sub-pass (step 3 — only reached once `abort==false` is conclusively known): fold the in-memory included-parts list (already in ascending `part_number` order from the scan) via `builder.fold(part.etag)` for each, then `builder.finalize()` to produce the composite `ETag` returned in the helper's output (used by `CompleteMultipartUpload` step 4).

### Manifest encoding (for the final `:O:` value)

- **Fast path** (all included parts the same size, all at `generation=0`, i.e. no retries at all): compact formula-based manifest — `total_size, num_parts, size_per_part` (+ last-part remainder) + `ref_tag`. Per-part data location is derived arithmetically; no per-part table needed.
- **Sparse retries** (the common case — a handful of parts have `generation > 0`): an exception/interval list of `(part_number, generation)` pairs for just the non-zero-generation parts.
- **Dense retries** (many parts have `generation > 0`): a packed per-part generation table for every part. A header field records the table's bit-width, chosen from `max(generation)` across all parts (nibble/byte/`uint16`) to keep the table compact.

### Background cleanup for `G:M` (both modes)

See Phase 4 (`GcWorker`) for the processing loop. Summary:

- `DEEP` frees every `committed` part's data before deleting its row, but **never** touches an `Obsolete=true, pending` row (unconditionally skipped, not just deferred — only the row's own owning `UploadPart` or a future out-of-scope `Sweeper` pass may resolve it).
- `MIXED` only ever sees `committed`/included rows (excluded parts' data cleanup was already delegated to `G:U` groups by `scan_and_resolve_parts`) and frees them directly.
- Both operate entirely on `S:U` rows scanned by `make_u_prefix(bucket_id, ref_tag)` and keep the `G:M` entry alive until that whole range is empty — which may never happen if an `Obsolete=true, pending` row's owner crashed and never returns (known/accepted gap, see Phase 4's "Sweeper" note).

## Phase 4 — Background workers

Files: `backend/src/sweeper.{hpp,cpp}`, `backend/src/gc_worker.{hpp,cpp}`.

- **Sweeper**: no changes needed for multipart — there's no `P:M` in this design, and stuck/abandoned-upload-head expiry is explicitly out of scope (no lifecycle engine, per "Scope and key decisions"). **Known/accepted gap**: if an `UploadPart`'s owning request crashes after being marked `Obsolete=true` (by a re-upload race or by `Abort`/`Complete`) but before it reaches its own Phase 3, its `S:U` row (and any `G:M` entry waiting on it) stays stuck indefinitely — nothing in this plan resolves it. A future `Sweeper` extension that can identify and resolve such abandoned rows is the natural fix, but is explicitly out of scope for this plan (no lifecycle engine).

- **`GcWorker` (`gc_once`)**: extend the key-type branch (currently `parse_go_key` → per-instance, else `parse_group_go_key` → group) with two new branches:

  - **`parse_gu_key` → `G:U`** (unified group, 1–20 entries): for each entry in the value (`count` of them), free the data-tier blob at `ref_tag-part_number-generation` (`ref_tag` from the key, `part_number`/`generation` from the entry) + once all entries are freed, `kv_del` the `G:U` key. There is a single code path here regardless of `count` — no separate "simple single-entry" vs. "batched" branch.
  - **`parse_gm_key` → `G:M`** (whole-upload cleanup, `DEEP`/`MIXED`) — **LOCKED** algorithm. **Note**: since `scan_and_resolve_parts` now delegates excluded-part data cleanup entirely to `G:U` groups (written in the same txn as the `G:M{MIXED}` directive), every row `G:M` itself ever has to deal with is either a `committed` *included* row (now superseded by the final `:O:`, safe to free/delete directly) or — for `DEEP` (abort) only — any row at all, since nothing was pre-classified:

    1. `RangeScan` `make_u_prefix(bucket_id, ref_tag)` — the whole `S:U` range for this upload.
    2. **Clean case** — every `part_number` present has exactly one row, and it's `committed` (not `Obsolete=true, pending`): for `DEEP`, free every part's data; for `MIXED`, free every remaining row's data (all included, by construction). Then a single `clearRange` over the whole `S:U` prefix for this `ref_tag` (covers all resolved rows in one mutation — this is the former "`SHALLOW`" fast path, now just the zero-work-remaining branch of this same case).
    3. **Messy case** — any `part_number` has multiple rows, **or has a lone row that is `Obsolete=true, pending`** (this can only arise under `DEEP`, since `MIXED` is only reached when the scan found zero `pending` rows): handle `committed` rows individually (free data, delete that row or a tight sub-range). **LOCKED**: any `Obsolete=true, pending` row is *always* skipped by `GcWorker` — unconditionally, regardless of age. `GcWorker` must never move such a row into the GC domain (`G:U`) or free its data itself, under any circumstance — only its own owning `UploadPart` (finishing Phase 2, then noticing `Obsolete=true` in its own Phase 3 and self-converting to a `G:U` group of 1) or a future out-of-scope `Sweeper` pass may ever resolve it. If the owning writer crashed and never returns, that row — and the `G:M` entry waiting on it — stays stuck indefinitely; this is the same known/accepted gap already noted under "Sweeper" above, not a new one. No aging/timestamp check is needed in `GcWorker` for this case: the rule is unconditional, not time-based.
    4. Don't delete the `G:M` entry until the entire `S:U` range for this `ref_tag` is empty — multi-pass loop (bounded iterations + wait, same shape as the existing `gc_once`/rate-limited loop), re-scanning each pass.

  - Reuses `data_store_.remove(filename)` and the existing rate-limiting (`rate_allow`) for all data-tier frees in both branches.

- **Test-only on-demand activation**: extend the existing `Sweeper::process_bucket(bucket_id, force)` force-bypass pattern to `GcWorker` so tests can force-run a `G:U`/`G:M` cleanup pass immediately instead of waiting for the normal interval delay. **Note**: this only bypasses `GcWorker`'s own polling interval — it does **not** (and cannot) force-resolve an `Obsolete=true, pending` row, since `GcWorker` never touches those regardless of age (see "messy case" above); tests covering that scenario must instead drive the owning `UploadPart`'s Phase 2/3 to completion to resolve it.

## Milestone boundary

Phases 1–4 plus the perf_driver/test work in Phase 7 constitute **Milestone 1 — core upload protocol**: `InitiateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload`, `ListParts`, `ListMultipartUploads`, exercised only through `perf_driver` (no C ABI, no Go/versitygw frontend yet).

Scope is strictly "the upload protocol maintains consistent KV/data-tier state" — reading or deleting a *completed* multipart object is Phase 8 (deferred), and S3-HTTP-facing wiring is Milestone 2 (Phases 5–6 below).

Milestone 2 does not start until Milestone 1's corner-case test suite passes.

## Phase 5 — C ABI (Milestone 2)

Files: `backend/include/kvrgw_c_api.h`, `backend/src/kvrgw_c_api.cpp`.

Add, following the existing `kvrgw_put_object`/`kvrgw_list_objects` patterns (buffer-in/arena-out, `abi_guard` wrapper):

`kvrgw_initiate_multipart_upload`, `kvrgw_upload_part`, `kvrgw_complete_multipart_upload` (array of `{part_number, etag}` in, final etag/version out), `kvrgw_abort_multipart_upload`, `kvrgw_list_parts`, `kvrgw_list_multipart_uploads`.

Upload-id/ref_tag encoded as opaque strings on the wire, same as existing `version_id_out` string encoding.

## Phase 6 — Go frontend (Milestone 2)

Files: `frontend/kvrgwc/kvrgwc.go`, `frontend/backend_kvrgw.go`.

- `kvrgwc.go`: one `Handle` method per new ABI call, following the `PutObject`/`ListObjects` cgo-pin pattern (`callPin`, fixed output buffers, arena for variable-length lists).
- `backend_kvrgw.go`: implement `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload`, `ListMultipartUploads`, `ListParts` on `KvRgwBackend`, translating to/from `aws-sdk-go-v2`/`s3response` types via `mapErrorCode`, following the existing `PutObject`/`HeadBucket` translation style. `UploadPartCopy` stays on the embedded `backend.BackendUnsupported` (not implemented).

## Phase 7 — perf_driver & validation (Milestone 1 gate)

Files: `backend/src/perf_driver.{hpp,cpp}`.

- Add `initiate-mpu`/`upload-part`/`complete-mpu`/`abort-mpu`/`list-parts`/`list-mpu` commands to `perf_driver.cpp`'s REPL (same `parse_params`/worker-thread style as `put`/`copy`) — in-process only, no S3/HTTP/Go frontend involved.

- Design an elaborate corner-case test suite, driven entirely through `perf_driver`, covering (at minimum):

  - Happy-path single/multi-part upload.
  - `ListParts`/`ListMultipartUploads` correctness mid-upload and post-complete.
  - Same-part re-upload (generation bump, old data freed via `G:U`).
  - Concurrent re-upload race on the same part (the write-write-conflict/retry scenario).
  - `Complete` with all parts committed (happy path, `MIXED` with/without exclusions).
  - `Complete` finding a `pending` part (whole-upload abort + `InvalidPart`).
  - `Complete`/`Abort` racing a concurrent in-flight `UploadPart` (the `Obsolete`→`NoSuchUpload` ordering fix).
  - Explicit `Abort`; double-`Abort`/`Abort`-after-`Complete` (idempotency).
  - `GcWorker` actually freeing data for `G:M{DEEP}`/`G:M{MIXED}`/`G:U` (verified via the on-demand force-activation hooks, not real-time waiting).
  - The "messy case" multi-generation `G:M` cleanup loop (`Obsolete`-too-young row deferral across passes).
  - Malformed upload-id (`NoSuchUpload`).

- **AWS validation corner cases**:

  - `UploadPart` Phase 0 rejections (`part_number` 0/negative/>10000, `part_size` 0, `part_size` >5GB — each leaves zero KV trace, verify session is otherwise unaffected and the same `part_number` can be legally resubmitted after).
  - `Complete` Phase 0 rejections (`client_parts.size()` 0 and >10000; out-of-order/duplicate `part_number` entries) — verify these reject with **zero KV access at all**, even against a nonexistent upload ID.
  - `Complete` with a non-last *included* committed part <5MB (full-abort path, `EntityTooSmall`).
  - `Complete` with the true last part (by the client's list, not the scan's) <5MB (must succeed — exemption applies, including when trailing excluded parts exist at higher `part_number`s).
  - `Complete` with a client tuple referencing a `part_number` never found among committed rows, and with a mismatched etag (`InvalidPart` in both cases, full-abort path).
  - `Complete` excluding some committed parts from the client's list (verify the excluded parts' `S:U` rows + data are freed via `G:U` groups — including the >20-excluded-parts case, spanning multiple `G:U` groups — and that they're absent from the final object/manifest).
  - `Complete`-triggered abort (pending/mismatch/undersized/missing-part) leaving **no** stray `G:U` writes behind (nothing was ever committed from the failed attempt).
  - `ListParts`/`ListMultipartUploads` against a deleted bucket (`NoSuchBucket`) and a nonexistent/already-torn-down upload (`NoSuchUpload` for `ListParts`).

- **Versioning**: `Complete` against a `VERSIONING_ENABLED` bucket with a pre-existing `:O:` (old object displaced to `:V:` at its own version id, new object gets the next version id, `PutObjectResult::version_id` populated); against `VERSIONING_SUSPENDED` (old object displaced per the real-vs-null-version-id branch, including null-version-slot cleanup); against `VERSIONING_DISABLED`/no prior object (unchanged from today's non-versioned behavior).

- Only once this suite passes does Milestone 2 (Phases 5–6) begin.

## Phase 8 — Deferred: reading back and deleting a completed multipart object

Not addressed by this plan — tracked as explicit follow-up work, to be designed once Phases 1–7 land:

- **GET/HEAD read-back**: the final `:O:` manifest (fast/sparse/dense encodings from Phase 3) needs a new chunk type (existing `ChunkDescriptor`/`ChunkType` in `object_value.hpp` only models one contiguous blob/ref_tag) and `GetObject`'s byte-range logic needs to map a global offset into `(part_number, local offset)`, open the right `ref_tag-part_number-generation` file, and handle reads spanning multiple parts.
- **Delete/overwrite cleanup**: when a completed multipart `:O:` is later deleted or overwritten, `move_object_to_g`/`GcWorker` need to free potentially thousands of part files, not the single blob the current `GcDescriptor` model assumes.

Until this phase lands, a completed multipart object's `:O:` entry exists and is listable, but cannot be read back via `GetObject`/`HeadObject`, and deleting/overwriting it will not correctly free its part data.

## Open discussion items

Not yet resolved — tracked here so they aren't lost while other decisions get locked:

1. **Exact manifest byte-layout** — "Manifest encoding" above is conceptually locked (fast/formula path, sparse exception-list, dense per-part table with nibble/byte/`uint16` generation width) but no concrete struct/byte layout has been specified for any of the three cases.

## Suggested implementation order

**Milestone 1 — core upload protocol (perf_driver only, no frontend):**

1. Phase 1 (schema) — no behavior change, easy to review in isolation.
2. Phase 2 (Initiate/UploadPart/List*).
3. Phase 3 (Complete/Abort).
4. Phase 4 (Sweeper/GcWorker — `G:M`/`G:U` cleanup, on-demand test activation).
5. Phase 7 (perf_driver commands + corner-case test suite) — gate; must pass before Milestone 2.

**Milestone 2 — full-stack wiring (starts only after Milestone 1's tests pass):**

6. Phase 5 (C ABI).
7. Phase 6 (Go frontend).
8. Phase 8 (deferred — GET/HEAD/delete of a completed multipart object) — timing TBD, at least after Milestone 1.
