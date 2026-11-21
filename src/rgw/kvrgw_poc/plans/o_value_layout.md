# `:O:` Value Layout Design

Decisions locked in sessions 2026-11-01 and 2026-11-02. Supersedes the sizing notes in
[`multipart_value.md`](multipart_value.md) and the open decisions in
[`value_size_kb.md`](value_size_kb.md) sections 5.1 and 5.4.

---

## Background for implementers

POC: no backward compatibility is needed; wire formats can change freely.
Where this document conflicts with `multipart_value.md` or `value_size_kb.md`,
this document wins.

- **`:O:`** is the per-object FDB value (`backend/src/object_value.hpp`:
  `ObjectValueHeader`, `ChunkDescriptor`, `write_object_value`). Keys are built
  in `backend/src/keys.cpp`.
- **`C:` children** are extra KVs keyed by the header + `ref_tag` + a child-type
  byte (`make_child_key`; type bytes in `backend/src/constants.hpp`:
  `kChildTypeTags` `T`, `Annotation` `A`, `Extended` `E`, `Manifest` `M`,
  `Checksum` `S`). `C:D` corresponds to `CHUNK_CHILD_D` single-part data.
  `R:<ref_tag>` (`make_r_key`) is the ref-count key and holds only a count.
- **`CHUNK_*` types** (`ChunkType` in `object_value.hpp`): `INLINE`, `CHILD_D`,
  `STORAGE`, `MULTIPART`, and the shared forms `CHILD_D_REF`, `STORAGE_REF`,
  `MULTIPART_REF`. A `_REF` object points at another object's data.
- **`hdr.ref_tag` vs `chunk.ref_tag`:** `hdr.ref_tag` is generated fresh on
  every Put/Copy and identifies the object's own children. `chunk.ref_tag` is
  the ref_tag of the original owner of shared data. `ChunkDescriptor.bucket_id`
  is the bucket of that owner.
- **SRC / TGT** are the source and target objects of a CopyObject.
- **Part files and storage-tier data** live outside FDB. "Storage-tier file"
  means a file such as `<ref_tag>_checksum`.
- **Build and test:** `bash scripts/build_from_tar.sh` builds backend and
  frontend; unit tests are in `backend/tests`; `bash scripts/run_test_plan.sh
  --quick` runs the quick suite (see `README.md`).

---

## Rules

1. **Everything needed for listing must reside in `:O:`.**
2. **Get/Head info should reside in `:O:`.**
3. **Get-object body data has lower priority.**
4. **Tags and the per-part checksum blob are only needed for explicit
   standalone requests — tags go to `C:T`; the blob goes inline when it fits,
   else `C:S` (≤ 75 KB), else the storage-tier file.**

---

## Hard ceiling

**`:O:` is hard-capped at `kMaxOValueBytes` = 1,024 B.**

`OValueBufMultipart` is dissolved. A single `OValueBuf` (1,024 B) serves
all object types. `kMaxMultipartOValueBytes` is removed.

---

## Fixed fields — always in `:O:`

These never spill, regardless of object type.

| Field | Size | Why |
|---|---|---|
| `ObjectValueHeader` | 64 B | All listing fields: flags (uint16), chunk_type, content_type_len, tags_count, metadata_count, `cksum_alg:4 \| reserved:4` (one byte, all object types), size, last_modified_sec, last_modified_nsec, version_id, next_vid, ref_tag, etag (with embedded part_count), annotations_count. `hdr.part_count` removed — redundant with `etag.part_count()`. |
| `content_type` | ≤ 255 B | HeadObject — rule 2 |
| `ChunkDescriptor` | 24 B | **All object types.** Always carries source `bucket_id` + `ref_tag` + `flags` + `type`. Lets GetObject fire data fetch in parallel with any child read |
| `[cksum_loc:4|manifest_loc:4] + mflags:8` | 2 B | **Multipart only.** Single-part needs nothing here: the algorithm is in `hdr.cksum_alg`, `cksum_combined` is always inline, and the checksum is always `FULL_OBJECT`. See encodings below |
| `cksum_combined` | `digest_len(alg)`, ≤ 64 B | Only when `hdr.cksum_alg != NONE`. Exact algorithm size, see table below. HeadObject returns `x-amz-checksum-*` — rule 2 |
| **Fixed floor** | **≤ 409 B** | Worst case: 64 + 255 + 24 + 2 + 64 (SHA512) |
| **Budget remaining** | **≥ 615 B** | Available for metadata, manifest, `first_part_info`, per-part checksum blob, inline data |

`cksum_combined` is **guaranteed budget** (counted in the floor, forced
inline). Everything else — metadata, manifest, `first_part_info`, per-part
checksum blob — competes for the remaining budget in the priority order of
the placement algorithm below.

`cksum_combined` size is the actual digest length of `hdr.cksum_alg`:

| Algorithm | Bytes |
|---|---|
| CRC32, CRC32C | 4 |
| CRC64NVME, XXHash3, XXHash64 | 8 |
| MD5, XXHash128 | 16 |
| SHA1 | 20 |
| SHA256 | 32 |
| SHA512 | 64 |

Note: `hdr.part_count` is removed — it is always identical to `etag.part_count()`
(the ETag `-N` suffix). All callers use `etag.part_count()` directly.

---

## Wire order of `:O:`

1. `ObjectValueHeader` (64 B)
2. `content_type`
3. `ChunkDescriptor` (24 B)
4. `cksum_combined` (if `hdr.cksum_alg != NONE`)
5. metadata_frame (if metadata is inline)
   - single-part: inline data follows the metadata (if stored in `:O:`)
6. Multipart only:
   1. 2-byte header `[cksum_location:4 | manifest_location:4] + mflags`
   2. `first_part_info` (9 B) if no local manifest is stored, otherwise the full manifest (`kManifestInline` or `kMFlagManifestCached`)
   3. per-part checksum blob (if a local copy is stored: `kCksumInline` or `kMFlagCksumCached`)

---

## Placement algorithm — priority order for the remaining budget

Evaluated once at write time in two phases. Priority: `cksum_combined`
(forced) > metadata > manifest > `first_part_info` > per-part checksum blob.

**Phase 1 (below)** only decides *inline vs external* for each item.
**Phase 2 ("External location selection")** then picks the concrete
external location for every item Phase 1 sent out of `:O:`.

```
// step 0: forced fields
fixed_len = sizeof(ObjectValueHeader) + content_type_len        // 0..255
          + sizeof(ChunkDescriptor)
          + (multipart ? 2 : 0)                                 // 2 = [cksum_loc|manifest_loc] + mflags
if (has checksum) {
  store cksum_combined                                          // 4..64 B, exact per alg
  fixed_len += digest_len(alg)
}

// single-part: only metadata competes
if (!multipart) {
  if (fixed_len + metadata_len <= 1024) { store metadata inline; return }
  // else: metadata spills (see "single-part" below)
}

// multipart common case: everything fits
if (fixed_len + metadata_len + manifest_len <= 1024) {
  store metadata + manifest inline            // no first_part_info
  fixed_len += metadata_len + manifest_len
  if (fixed_len + cksum_blob_len <= 1024) store cksum_blob inline
  return
}

// can't store both: metadata has priority over manifest
if (fixed_len + metadata_len + first_part_info_len <= 1024) {
  store metadata inline + first_part_info     // manifest external
  fixed_len += metadata_len + first_part_info_len
}
else if (fixed_len + manifest_len <= 1024) {
  store manifest inline only                  // metadata external, no first_part_info
  fixed_len += manifest_len
}
else {
  store first_part_info only                  // metadata and manifest both external
  fixed_len += first_part_info_len
}

// lowest priority: per-part checksum blob
if (fixed_len + cksum_blob_len <= 1024) store cksum_blob inline
```

### Phase 1 outcomes (multipart): inline vs external

| Case | Metadata | Manifest | `first_part_info` |
|---|---|---|---|
| Everything fits | inline | inline | no |
| Metadata + first_part_info fit | inline | external | yes |
| Only manifest fits | external | inline | no |
| Neither fits | external | external | yes |

`kMFlagFirstPartInfo` is set exactly when `first_part_info` is stored.
`kFlagExternalMetadata` is set exactly when metadata is external.

### External location selection (Phase 2)

Applied only to items Phase 1 marked external.

**Metadata and manifest:**

| Metadata | Manifest | Result |
|---|---|---|
| inline | external | manifest → `C:M` (`kManifestChildM`); no `C:E` exists |
| external | inline | metadata → `C:E`; manifest stays inline |
| external | external | not shared: always one combined `C:E` (`kManifestChildE`); metadata + manifest is ≤ ~75 KB worst case, so it always fits. With `kFlagSharedData`: metadata → `C:E`, manifest → `C:M` (`C:E` is not shareable) |

With `kFlagSharedData` the authoritative manifest is always in `C:M`; the
placement algorithm only decides whether a local copy is kept
(`kMFlagManifestCached`).

The manifest is capped at 75 KB, so it always fits in a KV value. There is no
on-disk manifest file and no storage-tier manifest code.

**Per-part checksum blob** (when not inline): `C:S` (`kCksumChildS`) if the
blob is ≤ 75 KB, else the storage-tier file (`kCksumFile`). The blob can reach
640 KB (SHA512 × 10,000 parts), which exceeds an FDB value. It never displaces
any higher-priority field.

### Single-part (metadata spill and data)

- Metadata inline if `fixed_len + metadata_len ≤ 1,024 B`, else spills to `C:E` (`kFlagExternalMetadata`).

- **Inline in `:O:`**: only if metadata fit in `:O:` AND
  `data_len ≤ min(remaining :O: budget, max_inline)`, where `max_inline` is the
  tier-config threshold (`kDefaultMaxInline` = 256 B by default).
- **`C:E` combined**: if metadata spilled to `C:E` and
  `data_len ≤ max_kv_store`, the data is stored in the same `C:E` value (not in
  `C:D`) and `kFlagDataInChildE` is set. Combined `C:E` is not shareable: the
  data cannot be ref-counted, so CopyObject copies it into the target instead
  of sharing it, and `kFlagSharedData` is never set on such an object.
- **`C:D` child**: metadata fit in `:O:`, and the data does not go inline
  (`data_len > max_inline`, or it exceeds the remaining `:O:` budget), and
  `data_len ≤ max_kv_store` (tier-config limit, default `kDefaultMaxKvStore` =
  4,096 B). `C:D` is shareable (ref-counted), letting CopyObject avoid
  duplicating data.
- **Storage tier**: data larger than `max_kv_store` (`CHUNK_STORAGE`), regardless
  of where metadata lives.

---

## Child KV map

| Child | Type byte | Content | Lifetime | Shareable |
|---|---|---|---|---|
| `C:E` | `'E'` | metadata spillover; optionally + manifest (multipart) or + data (single-part, `kFlagDataInChildE`) | deleted with `:O:`, same transaction | No |
| `C:M` | `'M'` | multipart manifest (standalone, when not in `:O:` or `C:E`) | ref-counted; see "Manifest placement" below | Yes |
| `C:D` | `'D'` | single-part data above `max_inline` and ≤ `max_kv_store`, metadata in `:O:` | ref-counted | Yes |
| `C:T` | `'T'` | object tags | standalone; shared flag in `:O:` controls delete | Conditionally |
| `C:A` | `'A'` | annotations (`kFlagExternalAnnotations`); always owned, keyed by `hdr.ref_tag` | deleted with `:O:`, same transaction | No |
| `C:S` | `'S'` | per-part checksum blob (≤ 75 KB; larger goes to the `kCksumFile` storage-tier file) | standalone; shared flag in `:O:` controls delete | Conditionally |

`C:T` and `C:S` are always accessed via standalone requests (`GetObjectTagging`,
`GetObjectAttributes`). A single GET never needs both.

**Child key `ref_tag`:** owned children (`C:E`, `C:A`, and unshared `C:T` /
`C:S`) are keyed by `hdr.ref_tag`. Shared children (`C:M`, `C:D`, and shared
`C:T` / `C:S`) are keyed by `chunk.ref_tag` — the original owner's ref_tag — and
their ref-count is the `R:<ref_tag>` counter for that same ref_tag. `C:S` has no counter of its own: it follows the `C:M` counter.

---

## Manifest placement (multipart)

Where the manifest goes is decided by the placement algorithm above; the
outcome is recorded in `manifest_location`. Only `C:M` is shareable and
ref-counted; an inline or `C:E` manifest is the object's own copy.

**Read rule (all multipart objects):** the server always checks `:O:` first
for a local manifest (`manifest_location == kManifestInline` or
`kMFlagManifestCached` set) and uses it to read data, including in the shared
case where it is only a cache. When `kMFlagManifestCached` is set the cached
manifest is always used and `C:M` is never read. Likewise, when
`kMFlagCksumCached` is set the cached checksum blob is always used and `C:S`
(or the `kCksumFile` file) is never read. `C:E` / `C:M` / `C:S` is read only
when no local copy is stored.

**Shared objects (`kFlagSharedData`, both SRC and TGT of a copy):** the
authoritative ref-counted manifest is always in `C:M`, never in `C:E`, and
`manifest_location == kManifestChildM` on every sharing object. Each sharing
object is encouraged to keep a local copy in `:O:` whenever it fits and records
it with `kMFlagManifestCached` (same for the checksum blob with
`kMFlagCksumCached`). The state is unambiguous: `kManifestChildM` means "a
`C:M` exists"; the `Cached` bit means "a local copy also exists". When no local
copy fits, `kMFlagFirstPartInfo` is set as normal.

### Multipart header encoding — bytes 0 and 1 (multipart only)

```
byte 0:  [cksum_location:4 | manifest_location:4]
byte 1:  mflags (8 bits)
```

Nibble values are mutually exclusive enumerations — no OR-ing. The checksum
algorithm is **not** stored here — it lives in `hdr.cksum_alg` (see below).

**Byte 0 high nibble — checksum blob location (`cksum_location`):**

| Value | Constant | Meaning |
|---|---|---|
| 0 | `kCksumInline` | Checksum blob inline in `:O:`; the only copy (or absent when `alg == NONE`) |
| 1 | `kCksumChildS` | Authoritative checksum blob in `C:S` standalone child; a local copy may also exist, see `kMFlagCksumCached` |
| 2 | `kCksumFile` | Authoritative checksum blob in `<ref_tag>_checksum` on-disk file; a local copy may also exist, see `kMFlagCksumCached` |
| 3–15 | reserved | — |

**Byte 0 low nibble — manifest location (`manifest_location`):**

| Value | Constant | Meaning |
|---|---|---|
| 0 | `kManifestInline` | Manifest inline in `:O:`; the only copy (unshared object) |
| 1 | `kManifestChildE` | Manifest piggybacked in `C:E` (unshared only) |
| 2 | `kManifestChildM` | Authoritative manifest in `C:M` standalone child (shared); a local copy may also exist, see `kMFlagManifestCached` |
| 3–15 | reserved | — |

**Byte 1 — `mflags`:**

| Bit | Constant | Meaning |
|---|---|---|
| 0x01 | reserved | (was `kMFlagChecksum` — removed; checksum present ⇔ `hdr.cksum_alg != NONE`) |
| 0x02 | `kMFlagComposite` | checksum type `COMPOSITE`; only valid when `hdr.cksum_alg != NONE`. Clear = `FULL_OBJECT` |
| 0x04 | `kMFlagFirstPartInfo` | `first_part_info` (9 B) present; set exactly when no local manifest is stored (`manifest_location != kManifestInline` and `kMFlagManifestCached` clear) |
| 0x08 | `kMFlagManifestCached` | a local manifest copy is stored inline in `:O:` in addition to the external authoritative copy named by `manifest_location`. Valid only with `kFlagSharedData` |
| 0x10 | `kMFlagCksumCached` | a local per-part checksum blob copy is stored inline in `:O:` in addition to the external authoritative copy named by `cksum_location`. Valid only with `kFlagSharedData` |
| 0x20–0x80 | reserved | TBD |

**Local vs external copies.** The location nibbles always name the
authoritative copy. A local copy exists when the location is `Inline`, or when
the matching `Cached` bit is set. Unshared objects never set a `Cached` bit.

### Checksum algorithm — `hdr.cksum_alg` (all object types)

High nibble of header byte 7 (the former pad byte); low nibble reserved.
Shared by single-part and multipart objects.

| Value | Algorithm |
|---|---|
| 0 | `NONE` |
| 1 | `CRC32` |
| 2 | `CRC32C` |
| 3 | `SHA1` |
| 4 | `SHA256` |
| 5 | `CRC64NVME` |
| 6 | `SHA512` |
| 7 | `MD5` |
| 8 | `XXHash3` |
| 9 | `XXHash64` |
| 10 | `XXHash128` |
| 11–15 | reserved |

Replaces the existing bitmap constants `kPlacementManifestChild`, `kPlacementChecksumChild`,
`kPlacementChecksumFile` and the multipart tail's separate `alg` and `type` bytes
(the algorithm now lives in `hdr.cksum_alg`; COMPOSITE vs FULL_OBJECT in `kMFlagComposite`).
`kFlagMultipartChildren` and `kFlagChecksumFile` are removed from
`ObjectValueHeader.flags` — fully redundant.

**Lazy promotion (CopyObject)**: runs in a single transaction covering both
SRC and TGT (CopyObject provides both). Each KV is read at most once and
written at most once; the worst case touches 5 KVs (SRC `:O:`, `C:E`, `C:M`,
`C:S`, TGT `:O:`), far below FDB transaction limits.

1. Read SRC `:O:`.
2. Manifest, by SRC `manifest_location`:
   - `kManifestInline`: create `C:M` from the inline data, ref-count = 2
     (SRC + TGT).
   - `kManifestChildE`: read `C:E`; write `C:M` (manifest, ref-count = 2) and
     rewrite `C:E` with metadata only (metadata stays external).
   - `kManifestChildM` (already shared): increment the ref-count only.
3. Checksum blob, by SRC `cksum_location` (no ref-count of its own; it follows
   the `C:M` ref-count):
   - `kCksumInline`: create `C:S` from the inline data.
   - already external (`kCksumChildS` / `kCksumFile`): nothing to do.
4. Update SRC in a single PUT: `kFlagSharedData`; `manifest_location =
   kManifestChildM`; `cksum_location = kCksumChildS` (unchanged if already
   external). If SRC held the manifest (or checksum) inline, it keeps that
   data as a local copy and sets `kMFlagManifestCached` (or
   `kMFlagCksumCached`). A manifest moved out of `C:E` is cached too if it
   fits. `kFlagExternalMetadata` is unchanged.
5. Write TGT `:O:` as `CHUNK_MULTIPART_REF` with `kFlagSharedData`, the same
   locations, and the cached bits set for whatever it caches: manifest first,
   then, if space still remains, the checksum blob.

SRC and TGT both record the external `C:M` (and `C:S`) in the location
nibbles and both are encouraged to cache locally; a `CHUNK_MULTIPART_REF`
source skips the data movement in steps 2–3 and only increments the `C:M`
ref-count.

**Ref-count cleanup**: a multipart object has one shared ref-count, the `C:M`
count. `C:S` (or the `kCksumFile` file) has no count of its own and always
follows `C:M`: they are created together and removed together. Shared `C:D`
data and shared `C:T` keep their own counts. A shared KV is removed only when
its ref-count reaches zero. Deleting an object with `kFlagSharedData`
decrements the count; the object that takes it to zero deletes the shared KVs.
There is no 2 → 1 promotion: the `R:` key holds only a count, so the
deleter does not know who the other owner is or whether it holds a local
cache, and it does not modify any other object's `:O:`.

---

## GET access pattern

| Object state | Reads | Notes |
|---|---|---|
| Small object, everything inline | `:O:` only | Common case |
| Metadata spilled | `:O:` + `C:E` | `C:E` also carries manifest (multipart) or data (single-part) |
| Metadata inline, manifest in `C:M` | `:O:` + `C:M` | Metadata fit but manifest didn't |
| Shared multipart, manifest cached locally | `:O:` only | `kFlagSharedData` + `kMFlagManifestCached`; `C:M` is authoritative but not read |
| Tags requested | `:O:` + `C:T` | Standalone request |
| Checksum requested | `:O:` + `C:S` | Standalone request |

For storage-tier objects, `ChunkDescriptor` in `:O:` lets the data fetch
fire in parallel with the `C:E` / `C:M` read — no serialisation penalty.

---

## Flags in `ObjectValueHeader.flags`

`ObjectValueHeader` is restructured (POC — no backward compatibility required):
- `flags` widened from `uint8_t` to `uint16_t`.
- Fields reordered for natural alignment: `flags`, `chunk_type`, `content_type_len`,
  `tags_count`, `metadata_count`, `cksum_alg:4|reserved:4`, `size`, `last_modified_sec`,
  `last_modified_nsec`, `version_id`, `next_vid`, `ref_tag`, `etag`, `annotations_count`.
- `hdr.part_count` (`uint16_t`) removed — redundant with `etag.part_count()`.
- The former pad byte (offset 7) now holds `cksum_alg` (high nibble; low nibble reserved) and keeps `size` at offset 8 (8-byte aligned).
- `sizeof(ObjectValueHeader)` remains 64 B; `static_assert` unchanged.

| Bit | Constant | Meaning |
|---|---|---|
| 0x0001 | reserved | (was `kFlagExtendedAttrs` — removed; metadata spill is `kFlagExternalMetadata`) |
| 0x0002 | `kFlagFenced` | delete marker |
| 0x0004 | `kFlagSharedData` | shared ref-counted data; don't delete on object delete. For single-part: `C:D` is shared. For multipart: manifest (`C:M`), checksum blob, and part files are all shared |
| 0x0008 | `kFlagExternalTags` | tags are in `C:T` child |
| 0x0010 | `kFlagExternalAnnotations` | annotations are in `C:A` child |
| 0x0020 | `kFlagDataInChildE` | single-part data is stored in `C:E` together with spilled metadata; requires `kFlagExternalMetadata`; never combined with `kFlagSharedData` (was `kFlagMultipartChildren`, redundant with `chunk_type` + multipart header nibbles) |
| 0x0040 | `kFlagSharedTags` | `C:T` is shared (ref-counted); don't delete on object delete |
| 0x0080 | `kFlagExternalMetadata` | metadata_frame spilled to `C:E` child |

New flags needed:
- `kFlagExternalMetadata = 0x0080` — metadata is in `C:E`, not inline.
- `kFlagSharedTags = 0x0040` — repurposed from `kFlagChecksumFile` (now redundant).
- `kFlagDataInChildE = 0x0020` — single-part data lives in `C:E` next to the metadata.
- `kFlagSharedChecksum` removed — covered by `kFlagSharedData` on multipart objects.

`kFlagMultipartChildren` and `kFlagChecksumFile` are removed — both are fully
encoded in the multipart header nibbles (`cksum_location`, `manifest_location`)
and `chunk_type`.

### `C:` range-clear on delete

The delete path ([`service_impl.cpp:1580`](../backend/src/service_impl.cpp)) issues a
`C:<ref_tag>` range-clear only when the object can own child KVs. The trigger
is built from the remaining signals:

- all object types: `kFlagExternalTags`, `kFlagExternalAnnotations`, `kFlagExternalMetadata`
- unshared multipart only (`CHUNK_MULTIPART`, no `kFlagSharedData`):
  `manifest_location` is `kManifestChildE` or `kManifestChildM`, or
  `cksum_location` is `kCksumChildS`

The range-clear uses `hdr.ref_tag`, so it removes only owned children. If none
apply, no range-clear is issued.

**Multipart object with `kFlagSharedData`:** its `C:M` / `C:S` are keyed by
`chunk.ref_tag` and are not touched by the range-clear. Delete instead:
1. decrement the `C:M` ref-count (using `chunk.ref_tag`);
2. when the count reaches zero, delete `C:M` and, if `hdr.cksum_alg != NONE`,
   the checksum blob (`C:S`, or the `kCksumFile` file). `C:S` has no separate
   decrement.

Shared `kFlagSharedTags` `C:T` follows its own decrement-at-zero rule. A
checksum blob in the `kCksumFile` storage-tier file is not a KV, so a
range-clear never removes it: the delete path removes the file when the object
is unshared, or when the `C:M` count reaches zero for a shared object.

---

## Multipart header semantics

Single-part objects have no such header: their checksum is always
`FULL_OBJECT`, `cksum_combined` is always inline, and `kMFlagComposite`,
`cksum_location`, `manifest_location` and `kMFlagFirstPartInfo` do not apply.

When `hdr.cksum_alg == NONE`: `cksum_combined` is absent (0 B), `cksum_location` nibble and `kMFlagComposite` are ignored.

Checksum type validity per algorithm (enforced by `cksum_type_valid_for_alg`):

| Algorithm | `COMPOSITE` | `FULL_OBJECT` |
|---|---|---|
| CRC-32, CRC-32C | yes | yes |
| CRC-64/NVME | no | yes (the only type) |
| every other algorithm | yes (the only type) | no |

All CRC algorithms (CRC-64/NVME, CRC-32, CRC-32C) support `FULL_OBJECT`, because CRCs can be combined across parts. Narrowing `FULL_OBJECT` to CRC64NVME broke `test_multipart_use_cksum_helper_crc32` / `_crc32c`.

When `kMFlagComposite` is set: checksum type is `COMPOSITE` (`-N` suffix in header). Valid for every algorithm except CRC64NVME.
When `kMFlagComposite` is clear and `hdr.cksum_alg != NONE`: checksum type is `FULL_OBJECT`. Valid for CRC-64/NVME, CRC-32 and CRC-32C only.
When `kMFlagFirstPartInfo` is set: the 9 bytes immediately after the 2-byte header are `{ part_num: 2B BE, gen_id: 2B BE, part_size: 5B BE }` for the lowest-numbered included part. The per-part checksum blob (if inline) follows.

---

## Buffer sizing consequences

- `OValueBuf` = 1,024 B. Used for all object types.
- `OValueBufMultipart` is removed.
- `kMaxMultipartOValueBytes` is removed.
- `place_multipart_children` is replaced by the two-phase placement algorithm
  above. Inline thresholds are computed from the remaining `:O:` budget
  (1,024 B minus the fixed fields and whatever higher-priority items already
  took), not from a fixed constant.
- `kCksumMaxDigestBytes` grows from 32 to 64 (SHA512).
- `C:E` value: `[ChildValueHeader 8 B][metadata_frame][optional: manifest (unshared multipart) or data (single-part)]`.
  Worst case: 8 + `MAX_META_FRAME_BYTES` (2,048 B) + 75 KB manifest ≈ 77 KB,
  within FDB's 100,000 B limit. Single-part data in `C:E` is bounded by
  `max_kv_store` (default 4,096 B).
- `C:M` value: manifest only, ≤ 75 KB. It never goes to the storage-tier file.
- `C:S` value: per-part checksum blob, ≤ 75 KB. Only a checksum blob larger
  than that goes to the storage-tier file (`kCksumFile`).

---

## What this supersedes

- [`multipart_value.md`](multipart_value.md) open decision 1: resolved as
  "manifest always to `C:M` when it doesn't fit in `:O:` or `C:E`; no
  re-run of `place_multipart_children` in self-copy path".
- [`value_size_kb.md`](value_size_kb.md) section 5.1 (`OValueBuf` too
  small): resolved by metadata spill to `C:E`.
- [`value_size_kb.md`](value_size_kb.md) section 5.4 (`OValueBufMultipart`
  oversized): resolved by dissolving it into `OValueBuf`.

## Code impact

| File | Change |
|---|---|
| [`backend/src/object_value.hpp`](../backend/src/object_value.hpp) | Reorder `ObjectValueHeader` (`flags` → `uint16_t`, add `cksum_alg` nibble, drop `part_count`); new flag constants (`kFlagExternalMetadata`, `kFlagSharedTags`, `kFlagDataInChildE`; remove `kFlagExtendedAttrs`, `kFlagMultipartChildren`, `kFlagChecksumFile`); `mflags` constants `kMFlagComposite`, `kMFlagFirstPartInfo`, `kMFlagManifestCached`, `kMFlagCksumCached`; replace `kPlacement*` bitmap with `cksum_location` / `manifest_location` nibbles + `mflags`; `ChunkDescriptor` on the wire for all types (24 B, with `bucket_id`); `write_object_value` / parser follow the wire order; remove `OValueBufMultipart` references |
| [`backend/src/object_value.cpp`](../backend/src/object_value.cpp) | Header byte-order conversion for the new layout; drop `part_count` |
| [`backend/src/multipart_value.hpp`](../backend/src/multipart_value.hpp), [`.cpp`](../backend/src/multipart_value.cpp) | Remove `kMaxMultipartOValueBytes` / `OValueBufMultipart`; replace `place_multipart_children` with the two-phase placement; add `first_part_info` |
| [`backend/src/checksum.hpp`](../backend/src/checksum.hpp), [`.cpp`](../backend/src/checksum.cpp) | Add SHA512, MD5, XXHash3, XXHash64, XXHash128 to `CksumAlg`; `kCksumMaxDigestBytes` 32 → 64; per-algorithm digest length; multipart validity rule (`FULL_OBJECT` for every CRC: CRC-64/NVME, CRC-32, CRC-32C; single-part is always `FULL_OBJECT`) |
| [`backend/src/constants.hpp`](../backend/src/constants.hpp) | Add the 75 KB cap constant for manifest and `C:S` blob |
| [`backend/src/service_impl.cpp`](../backend/src/service_impl.cpp) | PutObject / CopyObject placement; single-part data placement (`:O:` / `C:E` with `kFlagDataInChildE` / `C:D` / storage tier by `max_inline` and `max_kv_store`); CopyObject copies (does not share) `kFlagDataInChildE` data; single-transaction lazy promotion in CopyObject; shared-manifest handling (local cache via `kMFlag*Cached`, `C:M` / `C:S` authoritative); GET manifest read rule; shared ref-count decrement (`C:M` count; `C:S` / `kCksumFile` follow it), delete at zero; `C:` range-clear trigger at ~1580 and `kCksumFile` removal; replace `hdr.part_count` and `kPlacement*` uses |
| [`backend/src/service_impl_multipart.cpp`](../backend/src/service_impl_multipart.cpp) | CompleteMultipartUpload placement; set `hdr.cksum_alg` and `kMFlagComposite`; replace `hdr.part_count` and `kPlacement*` uses |
| [`backend/src/service_impl.hpp`](../backend/src/service_impl.hpp) | `kCksumMaxDigestBytes` sized buffers; `OValueBufMultipart` comment |
| [`backend/src/kvrgw_c_api.cpp`](../backend/src/kvrgw_c_api.cpp) | `hdr.part_count` → `etag.part_count()` |
| Tests | New [`o_value_layout_test.cpp`](../backend/tests/o_value_layout_test.cpp) + `backend/CMakeLists.txt`; updated [`object_value_test.cpp`](../backend/tests/object_value_test.cpp), [`checksum_test.cpp`](../backend/tests/checksum_test.cpp), [`multipart_key_test.cpp`](../backend/tests/multipart_key_test.cpp), [`multipart_children_test.cpp`](../backend/tests/multipart_children_test.cpp), [`multipart_ref_copy_test.cpp`](../backend/tests/multipart_ref_copy_test.cpp), [`multipart_cksum_test.cpp`](../backend/tests/multipart_cksum_test.cpp), [`multipart_phase2_test.cpp`](../backend/tests/multipart_phase2_test.cpp) |

## Implementation sub-tasks

1. **Checksum algorithms.** Extend `CksumAlg`, digest lengths, `kCksumMaxDigestBytes`, and the `COMPOSITE` / `FULL_OBJECT` validity rule. Update `checksum_test.cpp`.
2. **Header restructure.** New `ObjectValueHeader` layout (64 B), `flags` as `uint16_t`, `cksum_alg` nibble, remove `part_count`; switch callers to `etag.part_count()`. Update `object_value_test.cpp`.
3. **Flags and multipart header.** New flag constants (including `kFlagDataInChildE`); `cksum_location` / `manifest_location` nibbles and `mflags` (including `kMFlagManifestCached` / `kMFlagCksumCached`); remove the `kPlacement*` bitmap.
4. **Wire format.** `ChunkDescriptor` for all types; new `write_object_value` and parser in the wire order; `first_part_info`; `cksum_combined` sized per algorithm.
5. **Placement.** Two-phase placement (inline vs external, then `C:E` / `C:M` / `C:S` / file) replacing `place_multipart_children`; remove `OValueBufMultipart`; 75 KB caps.
6. **Write paths.** PutObject (single-part data placement by `max_inline` / `max_kv_store`), CompleteMultipartUpload, CopyObject (single-transaction lazy promotion, local cache bits, `C:M` / `C:S` authoritative; `kFlagDataInChildE` data is copied, not shared).
7. **Read paths.** GET / HEAD manifest read rule (inline first); `C:E` / `C:M` / `C:S` / file reads by location.
8. **Delete and ref-count.** `C:` range-clear trigger; `kCksumFile` removal; shared `C:M` ref-count decrement (`C:S` / `kCksumFile` follow it), delete at zero; shared `C:D` / `C:T` counts; `kFlagSharedTags` handling.
9. **Tests.** Update the multipart test files above and add `o_value_layout_test.cpp` covering the "New tests — location permutations" section.

Tasks 1 and 2 are independent; 3 follows 2; 4 follows 3; 5 follows 4; 6–8 follow 5; 9 runs alongside each.

## New tests — location permutations

New test file `backend/tests/o_value_layout_test.cpp`, registered in
`backend/CMakeLists.txt` the same way as `multipart_phase2_test` (links
`kvrgw` and FDB). The test is table-driven: it enumerates every legal state
below, forces that placement by sizing the inputs, and runs the same
operation set on each state. Illegal combinations are rejected by the parser.

**Small parts.** Multipart states are built with the min-part-size override
(`testutil::MinPartSizeGuard guard(svc, 1)` from
`backend/tests/min_part_size_guard.hpp`, as in `multipart_children_test.cpp`
and `multipart_cksum_test.cpp`) so parts are small, not 5 MiB. Every part is
4 KB–8 KB (the allocation size is already 8 KB). Vary the sizes within that
range so the manifest is not collapsed to a uniform-size encoding. The
checksum blob is `part_count × digest_len`, independent of part size, so the
75 KB `C:S` boundary needs only ≈ 1,200 SHA512 parts (≈ 5–10 MB of data).
With parts below 16 MB (24-bit sizes) a 10,000-part manifest is about
3 B × 10,000 + a nibble × 10,000 + 1.25 KB ≈ 37 KB: always external, but below
the 75 KB cap. The guard restores `kMinPartSizeBytes` on scope exit.

**Multipart, unshared (`CHUNK_MULTIPART`)** — metadata/manifest pairs that
Phase 1 + Phase 2 can produce, × checksum location:

| Metadata | Manifest | Notes |
|---|---|---|
| inline | `kManifestInline` | everything fits |
| inline | `kManifestChildM` | `first_part_info` set |
| external (`C:E`) | `kManifestInline` | |
| external (`C:E`) | `kManifestChildE` | combined `C:E`, `first_part_info` set |

Checksum: `hdr.cksum_alg == NONE`, `kCksumInline`, `kCksumChildS`,
`kCksumFile`. 4 × 4 = 16 states.

**Multipart, shared (`CHUNK_MULTIPART_REF` + `kFlagSharedData`)** — manifest is
always `kManifestChildM`:

| Dimension | Values |
|---|---|
| Metadata | inline, external (`C:E`) |
| `kMFlagManifestCached` | clear (`first_part_info` set), set |
| Checksum | `NONE`; `kCksumChildS` / `kCksumFile`, each with `kMFlagCksumCached` clear or set |

2 × 2 × 5 = 20 states.

**Single-part:**

| Metadata | Data | State |
|---|---|---|
| inline | inline | `CHUNK_INLINE` |
| inline | `C:D` | `CHUNK_CHILD_D`; shared variant `CHUNK_CHILD_D_REF` |
| inline | storage tier | `CHUNK_STORAGE`; shared variant `CHUNK_STORAGE_REF` |
| external | `C:E` | `kFlagDataInChildE`, never shared |
| external | storage tier | `CHUNK_STORAGE`; shared variant `CHUNK_STORAGE_REF` |
| either | empty object | no data |

**Checksum algorithm axis** (applied to every state above that carries a
checksum): CRC32 (4 B), SHA512 (64 B, largest), CRC64NVME (`FULL_OBJECT`
multipart), CRC32 (`FULL_OBJECT` and `COMPOSITE`), and one `COMPOSITE` algorithm.

**Operations run on every state:**
1. Put / CompleteMultipartUpload: assert the exact `:O:` fields
   (`manifest_location`, `cksum_location`, `kMFlag*`, `kFlag*`, wire length ≤
   1,024 B) and the exact set of `C:` KVs and files that exist.
2. HEAD / GET / ranged GET: result is correct, and the KVs read match the
   read rule (no `C:M` read when `kMFlagManifestCached`, no `C:S` / file read
   when `kMFlagCksumCached`).
3. ListObjects: every listed field is served from `:O:` alone.
4. GetObjectAttributes (per-part checksums): correct from inline, `C:S`, file
   and cached copy.
5. CopyObject with each state as SRC: single transaction; SRC and TGT end in
   the expected shared state; `C:M` ref-count == 2; the `C:E` → `C:M` move
   leaves metadata in `C:E`; `kFlagDataInChildE` data is copied, not shared.
6. CopyObject with a shared state as SRC: ref-count increments only.
7. Delete: an unshared object leaves no `C:` KV and no file; a shared object
   decrements the `C:M` count, deletes `C:M` / `C:S` / file only at zero, and
   never touches another object's `:O:`; deleting every owner in every order
   leaves no orphan KV, file or `R:` key.

**Boundary cases:**
- Metadata exactly at, and one byte over, the inline budget.
- Manifest exactly at, and one byte over, the remaining `:O:` budget.
- `first_part_info` fits but manifest does not; neither fits.
- Per-part checksum blob exactly at, and one byte over, the remaining budget.
- `C:S` blob at the 75 KB cap: SHA512 × 1,200 parts = 76,800 B (75 × 1,024) →
  `C:S`; 1,201 parts → file. No 10,000-part case is needed.
- Manifest: 10,000 parts of 4–8 KB (≈ 37 KB) → external (`C:M` or `C:E`). A
  manifest near the 75 KB cap is not reachable with small parts.
- `content_type_len` = 255 with SHA512, the fixed-floor worst case (409 B).
- Single-part data at `max_inline`, `max_inline + 1`, `max_kv_store`,
  `max_kv_store + 1`, with metadata inline and external.

**Parser rejects (illegal combinations):** `kManifestChildE` without
`kFlagExternalMetadata`; a `kMFlag*Cached` bit without `kFlagSharedData`;
`kFlagDataInChildE` with `kFlagSharedData` or without `kFlagExternalMetadata`;
`kMFlagFirstPartInfo` inconsistent with the cached/inline manifest state;
reserved nibble values and reserved `mflags` bits.

## Done when

The backend and frontend build cleanly with no new warnings; every existing
unit test is updated for the new layout and passes; new tests cover each
placement outcome, the local-cache bits, delete-at-zero ref-counting, and the
`kFlagDataInChildE` copy path; and `bash scripts/run_test_plan.sh --quick`
passes.

## Status

`[x] implemented` — 2026-11-03. Backend, frontend and all test targets build with
no new warnings; every unit test and `bash scripts/run_test_plan.sh --quick`
pass (against a live FDB; the plan's own frontends stay up).

New test: `backend/tests/o_value_layout_test.cpp` (single-part size × metadata
matrix, 17 multipart shapes × {unshared delete, owner-first, copy-first},
CopyObject of unshared and shared sources, ref-counts, no orphan `C:` / file /
`R:`). Combinations that real uploads cannot produce (shared Cached-bit
permutations, inline checksum next to `C:M`) are covered by the
`place_multipart` unit test in `multipart_key_test.cpp`; parser rejects are in
`object_value_test.cpp`.

Found and fixed while testing: a `CHUNK_CHILD_D_REF` copy must carry the owner's
`last_modified_sec` (the `D:` key embeds it). Scripts that decode `:O:` bytes
(`test_corrupted_etag.sh`, `test_tier_threshold.sh`, `fdb_list.sh`) were moved to
the 64 B header offsets; `run_multipart_tests.sh` pauses live frontends'
GC workers while it runs.

Checksum validity: `cksum_type_valid_for_alg` allows `FULL_OBJECT` for CRC-32 and
CRC-32C as well as CRC-64/NVME (the s3-tests multipart CRC helpers require it).

Deferred:
- `kFlagSharedTags`: `copy_object` copies tags, it does not share `C:T`.
- `DecodedManifestBody` still uses `std::vector`s on the read paths and in GC.
- Unshared states "inline checksum + `C:M` / `C:E` manifest" are unreachable by
  sizing (a manifest too big for `:O:` implies a checksum blob too big as well)
  and are checked only at the placement unit-test level.
- `multipart_gc_test` waits were raised from 15 s to 60 s (GC cadence is 1 s but
  claims are racy when other GC workers are live).
