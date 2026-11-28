# CopyObject

This document describes the CopyObject design for the KV-based RGW, including data sharing via reference counting.

---

## AWS Spec Summary

- PUT to `/{Key}` with `x-amz-copy-source` header
- Maximum 5 GB for single CopyObject (larger → multipart UploadPartCopy)
- Self-copy only allowed if at least one of metadata, storage class, or encryption changes

### Directives

- **MetadataDirective** (`x-amz-metadata-directive`): `COPY` (default) | `REPLACE`
- **TaggingDirective** (`x-amz-tagging-directive`): `COPY` (default) | `REPLACE` — independent from MetadataDirective
- **AnnotationDirective** (`x-amz-annotation-directive`): `COPY` (default) | `EXCLUDE`

### Source Specification

- `x-amz-copy-source: bucket/key` — copies current version
- `x-amz-copy-source: bucket/key?versionId=XXX` — copies specific version
- If current version is a delete marker → 404 (treated as deleted)

### Destination Versioning

- Versioned → new version ID generated (different from source)
- Suspended → writes NULL_VERSION (overwrites null slot)
- Non-versioned → simple overwrite

### What Is Copied vs Reset

- **Copied** (MetadataDirective=COPY): user metadata, content-type, content-encoding, content-language, content-disposition, cache-control, expires, checksum
- **Copied** (TaggingDirective=COPY): tags
- **Always reset**: ACL (private unless specified), version ID (new), last-modified, Object Lock settings
- **Not copied**: SSE encryption keys (independently specified)

### Conditional Headers

**On the source:**
- `x-amz-copy-source-if-match` / `x-amz-copy-source-if-none-match`
- `x-amz-copy-source-if-modified-since` / `x-amz-copy-source-if-unmodified-since`

**On the destination:**
- `If-Match` / `If-None-Match`

See [conditional-commands.md](conditional-commands.md#copyobject-conditionals).

---

## Scope

- **Same-shard copy:** source and destination are on the same shard → single transaction, no P:O, data shared via ref_count. Works cross-bucket — data sharing is not bucket-scoped.
- **Cross-shard copy:** full data copy (TBD)

Note: shard_id = hash(bucket_id + object_name) % shard_count. Same-shard for different keys in the same bucket is uncommon (hash collision). In practice, same-shard copy is mostly self-copy (metadata/storage-class change). Cross-bucket same-shard is also rare but supported.

---

## Full Transaction (Same-Shard)

```
txn {
  // 1. Resolve source
  get(src O:) or get(src V:<vid>)
  if src not found or src is DM → 404
  check source conditionals → fail? abort

  // 2. Resolve destination
  get(B) → bucket state, policies, quotas
  get(dst O:) → current entry
  check destination conditionals → fail? abort
  check policy, quota → fail? abort

  // 3. Build destination value
  generate new ref_tag for dst (own identity for children)
  apply MetadataDirective (COPY: duplicate src attrs | REPLACE: use request attrs)
  apply TaggingDirective (COPY: duplicate src tags into C:<dst.ref_tag>T | REPLACE: write request tags)
  apply AnnotationDirective (COPY: duplicate src annotations into C:<dst.ref_tag>A | EXCLUDE: none)

  // 4. Data sharing (by tier)
  Case 1 (inline): copy bytes into new O: value
  Case 2 (D:): get(C:<src.chunk_ref_tag>D), inc ref_count, put updated
  Case 3 (storage): create/inc R:<src.chunk_ref_tag>
  Case 4 (E:): inc ref_count on C:<src.chunk_ref_tag>E

  // 5. Set shared-data bit on src and dst (cases 2-4)
  //    (src update is internal, not a new version)

  // 6. Displace dst O: per versioning state
  if versioned:
      If dst O: has real vid: put(V:<old_vid>, old)
      If dst O: has NULL_VERSION: put(V:<NULL_VERSION>, old)
      Write new dst O: with vid=dst_O:.next_vid, next_vid=vid-1
  if suspended:
      If dst O: has real vid: put(V:<old_vid>, old)
      If dst O: has NULL_VERSION: put(G:O, old)
      If V:<NULL_VERSION> exists: put(G:V, existing) — enforce one-null-version rule
      Write new dst O: with vid=NULL_VERSION
  if unversioned:
      If dst O: exists: put(G:O, old)
      Write new dst O: with vid=NULL_VERSION

  // 7. Commit
  commit
}
```

No P:O needed — no storage-tier write, no two-domain problem. Single atomic transaction.

**On transaction conflict and retry:** if the transaction conflicts (e.g., concurrent delete of source), retry re-reads src O: (or V:<vid>). If source no longer exists → abort with 404. The copy never proceeds with a deleted source.

---

## ref_tag: Two Identities

Each O: (or V:) entry has two ref_tags:

- **O:.ref_tag** — this entry's private identity. Generated fresh on every write (including copies). Used for this entry's own C: children (tags, annotations, extended value). `RangeDelete(...C<O:.ref_tag>)` cleans only this entry's children.
- **chunk_descriptor.ref_tag** — where the data lives. Can be shared across copies. R: is keyed by this. GC uses this to locate and free storage-tier data.

For non-copied objects, these are the same value. For copies, they differ — the copy has its own O:.ref_tag but shares the source's chunk_descriptor.ref_tag.

Children (tags, annotations) are NEVER shared. Each copy has independent children under its own O:.ref_tag. TaggingDirective=COPY means "duplicate tag data into the copy's own C: key space" — not "share the same C: entry."

---

## Chunk Descriptor Types

Two variants per data tier — a compact form for non-copied objects and a _REF form for copies:

```
chunk_descriptor:
  {type: INLINE, data}
  {type: CHILD_D}                                         // derive location from O:'s bucket_id + O:.ref_tag
  {type: CHILD_D_REF, bucket_id, ref_tag}                 // explicit location (+20B)
  {type: STORAGE, storage_id, blob_id, offset, length}    // derive ref_tag from O:.ref_tag
  {type: STORAGE_REF, storage_id, blob_id, offset, length, ref_tag}  // explicit ref_tag (+12B)
```

**Two independent concerns:**

1. **Chunk descriptor type** → how to locate the data (addressing):
   - `CHILD_D`: derive from O:'s own bucket_id + O:.ref_tag
   - `CHILD_D_REF`: use stored bucket_id + ref_tag
   - `STORAGE`: derive from O:.ref_tag
   - `STORAGE_REF`: use stored ref_tag

2. **O: shared-data bit** → what to do at cleanup time (lifecycle):
   - Clear: free/delete unconditionally (no ref_count exists)
   - Set: read ref_count first, decrement, only free if zero

These are orthogonal. Descriptor type = where to find the data. Shared bit = whether data is shared and ref_counted.

---

## Data Sharing by Tier

### Case 1 — Inline (< 256B)

Data is embedded in the O: value. Simple byte copy into the new O: entry. No sharing — data is too small to benefit from indirection.

No shared-data bit. No ref_count.

### Case 2 — D: KV (256B–8KB)

New O: references the same `C:<chunk_descriptor.bucket_id>...<chunk_descriptor.ref_tag>D` entry. Increment ref_count inside the D: KV. The D: entry stays in the source bucket's key space.

### Case 3 — Storage-Tier (> 8KB)

Create `R:<chunk_descriptor.ref_tag>` (ref-count KV) with count=2 and a copy of the chunk descriptor. Both source and destination O: keep the chunk descriptor directly (cached copy for read-path optimization). R: is only consulted during GC.

**Read path:** O: holds chunk descriptor directly. GET reads O: → chunk descriptor → storage tier. No extra hop to R:. R: is invisible to reads.

### Case 4 — E: Overflow (TBD)

When chunk descriptor overflows to a child `C:<ref_tag>E` KV: increment ref_count on E: entry, create copy O: pointing to same E:. Set shared-data bit on both.

---

## Shared-Data Bit

A single bit in the O: (or V:) value flags field.

- **Set on:** both source and copy during CopyObject transaction (cases 2–4)
- **Not set on:** Case 1 (inline copy), normal PUT objects
- **Checked by:** delete/displacement path and GC worker

Purpose: tells the delete/GC path "check ref_count before freeing" vs "free unconditionally." Avoids extra reads on the vast majority of objects (non-copied).

---

## R: Key Format

```
R <ref_tag 12B>
```

- Global namespace, not bucket-scoped. Enables cross-bucket data sharing when objects are on the same shard.
- Keyed by the data's ref_tag (from chunk_descriptor.ref_tag).
- Value: ref_count (uint64, big-endian) + chunk descriptor (cached for GC worker)
- Created on first copy (count=2). Incremented on subsequent copies of the same data.

**Ref_count storage:**

- **R:** always uint64 (R: value is small, no reason to optimize). No practical limit.
- **D: and E:** dynamic sizing. Detection: if `len(D: value) > object.data_size`, the bytes past data are flags + ref_count. Normal (non-shared) D:/E: entries contain only raw data — zero overhead.

  D:/E: value layout:
  ```
  [data bytes (object.data_size)] [flags (1 byte)] [ref_count (variable)]
  ```

  Flags byte (only present when value extends past data):

  | shared bit | large bit | meaning | ref_count size |
  |---|---|---|---|
  | 1 | 0 | shared, count fits in 16 bits | 2 bytes (uint16) |
  | 1 | 1 | shared, count exceeded uint16 | 8 bytes (uint64) |

- Non-copied D:/E: entries: no flags byte, no ref_count. `len(value) == data_size`.
- First copy: append flags (shared=1, large=0) + ref_count=2 (2 bytes).
- 65536th copy: set large bit, widen ref_count to uint64 (8 bytes). Same transaction that would have overflowed.

No fallback paths — dynamic growth eliminates overflow entirely.

---

## Delete / GC Interaction

### Case 2 (D: KV) — handled in delete transaction

```
txn {
  // normal displacement of O: or V:
  ...
  if shared-data bit set and chunk_descriptor.type == CHILD_D:
    get(C:<ref_tag>D) → decrement ref_count
    if ref_count == 0: delete(C:<ref_tag>D)
    else: put(C:<ref_tag>D, updated ref_count)
  else:
    delete(C:<ref_tag>D)  // fast path, no extra read
  ...
  commit
}
```

### Case 3 (Storage-tier) — handled by GC worker

Delete path is unchanged — always creates G: entry regardless of shared-data bit.

GC worker processing G: entry:
- If shared-data bit is set in stripped value:
  - Read `R:<ref_tag>` → decrement count
  - If count == 0: delete R:, free storage-tier data
  - If count > 0: update R:, skip data free
- If shared-data bit is clear:
  - Free storage-tier data unconditionally (current behavior)

### Case 4 (E: overflow) — TBD

Same pattern as Case 2: decrement ref_count on E: in the delete transaction.

---

## Metadata-Only Copy (x-amz-metadata-directive: REPLACE)

Allows copy-to-self (same key, same bucket, no source versionId) to change metadata without duplicating data.

### Non-versioned bucket

In-place metadata update. No displacement, no G:O, no ref_count:

```
txn {
  get(O:) → current entry
  put(O:, same_chunk_descriptor + new_metadata)
  commit
}
```

### Versioned bucket

Creates a new version. Old O: displaces to V: (preserved). New O: has new metadata + same data + shared-data bit. Ref_count incremented.

### Suspended bucket

- If O: has NULL_VERSION → in-place metadata update (same as non-versioned). No displacement, no ref_count.
- If O: has real vid → new object: displace real vid to V:, new O: written with NULL_VERSION + new metadata + shared data. Ref_count needed.

### With source versionId

If `x-amz-copy-source` specifies a versionId (even for same key), this is NOT a metadata-only self-copy — it's a genuine copy from a specific version. Source data may differ from current O:. Full copy-with-sharing path applies.

---

## Copy from Specific Version

`x-amz-copy-source-version-id` allows copying from a non-current version (V:<vid>). Same sharing logic applies — the source is a V: entry instead of O:. Shared-data bit set on the V: entry and the new O:.

---

## Multiple Copies / Copy of a Copy

If object A is copied to B, then B is copied to C:
- All three share the same underlying data
- R:.count = 3
- When source already has shared-data bit set → R: already exists → just increment count

---

## Versioning Displacement and Source Update

Setting the shared-data bit on source O: (or V:) is an internal metadata update — not a new version, not visible to the client, does not trigger versioning displacement. It's an in-place field update within the copy transaction.

---

## Cross-Shard Copy (TBD)

When source and destination hash to different shards, data sharing via ref_count is not possible (would require cross-shard transactions). Falls back to full data copy — same as a PUT with data read from the source. Requires P:O coordination (storage-tier write to new location).

---

## Current RGW Gaps (for reference)

The current RGW CopyObject implementation is missing:
- **TaggingDirective** — tags follow MetadataDirective instead of being independent
- **x-amz-tagging on copy** — cannot specify replacement tags during copy
- **Destination If-Match / If-None-Match** — only source conditionals are supported
- **AnnotationDirective** — not yet implemented in upstream
