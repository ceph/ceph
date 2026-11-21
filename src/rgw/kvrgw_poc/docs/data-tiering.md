# Data Tiering — Inline, KV, and Storage Tier

## Three-Tier Data Storage Model

Object data is stored at one of three tiers, selected by size at PUT time:

| Tier | Size threshold | Location | Chunk descriptor |
|---|---|---|---|
| 1 — Inline | < 256B (tunable) | Embedded in O: value | `{type: INLINE, data: <bytes>}` |
| 2 — D: namespace | 256B – 8KB | Separate D: entry (same shard) | `{type: CHILD_D, size_tier, mtime}` |
| 3 — Storage tier | > 8KB | External blob | `{type: STORAGE, storage_id, blob_id, offset, length}` |

The chunk descriptor in the O: value encapsulates the data location. `storage_id` identifies the backend (rados, xfs, cephfs, remote S3, etc.).

**Rules:**
- Multipart uploads → always Tier 3 (storage tier) regardless of part size
- Size thresholds are tuning parameters — can be adjusted without protocol changes

**Storage-engine separation (TBD):**
- TiKV: Titan value separation (threshold > 2KB) or separate CF for D: namespace. TBD — both options keep D: values out of the LSM tree.
- FDB: default mode — 8KB values are within recommended range (< 10KB)

---

## D: Namespace — Key Schema

Tier 2 data lives in the `D:` namespace. The key is designed for both co-location with the parent O: (same shard for atomic PUT) and searchability (size buckets + LRU ordering + write scatter).

```
Key: [D 1B][shard_count 2B][shard_id 2B][bucket_id 8B][size_tier 1B][hash_prefix 1B][mtime 4B][ref_tag 12B]
      = 31B fixed

Value: raw object data (up to 8KB)
```

| Field | Purpose |
|---|---|
| shard_count + shard_id | Same as parent O: (co-located, single-shard transaction) |
| bucket_id | Per-bucket scanning |
| size_tier | Size bucket (0=<256B, 1=256-512B, 2=512-1KB, 3=1-2KB, 4=2-4KB, 5=4-8KB) |
| hash_prefix | `Hash(ref_tag) % 32` — scatters writes across 32 sub-ranges to prevent hotspots |
| mtime | Creation time (4B) — LRU ordering within each sub-range. **Note:** must use a wrap-safe encoding (e.g., uint32 seconds since a custom epoch like 2024-01-01, covering ~136 years). Raw 32-bit Unix time wraps at 2038 (signed) or 2106 (unsigned) — not acceptable for a long-lived key schema. Final encoding TBD. |
| ref_tag | Unique object instance identifier |

**Scan hierarchy (prefix scans):**

| Prefix | Scan purpose |
|---|---|
| `D:<shard_count><shard_id>` | All D: entries on one shard |
| `D:<shard><bucket_id>` | All D: for one bucket (all sizes) |
| `D:<shard><bucket_id><size_tier>` | One size tier in one bucket |
| `D:<shard><bucket_id><size_tier><hash_prefix>` | One sub-range, mtime-sorted (LRU) |

**LRU scan (32-way parallel):**

LC/migration scans all 32 hash_prefix values in parallel for a given (shard, bucket, size_tier), then merges results by mtime. Standard k-way merge gives global LRU order.

**Write hotspot mitigation:**

Without hash_prefix: burst writes create sequential mtime keys → hotspot on one TiKV region. With hash_prefix = Hash(ref_tag) % 32: writes scatter across 32 sub-ranges. Even sequential ref_tags from one RGW distribute evenly across prefixes.

---

## Tier 1 — Inline in O: (< 256B)

Very small objects store data directly in the O: value alongside attributes. Single key, minimum cost.

**PUT (Tier 1):**
```
txn {
  get(B) → if null or delete-pending → NoSuchBucket
  get(O:) → check for overwrite
  If overwrite: put(G:O, {old_ref_tag, chunk_descriptor, child_flags, ...})
  put(O:, {ref_tag, size, etag, content_type, chunk: {type: INLINE, data: <bytes>}, ...})
  commit
}
```

Single transaction, single write target. No P:O, no D: entry, no storage-tier write.

**GET (Tier 1):**
```
get(O:) → return inline data from chunk descriptor
```

Single read. Data is right in the O: value.

**HEAD:** `get(O:)` → return metadata headers only. Inline data is in the value but stripped server-side before responding.

**GC (Tier 1):** When old O: moves to G:O, the inline data is inside the G:O value. GC just deletes the G:O entry — no external cleanup needed (unless the object also has annotations/tags as C: children).

---

## Tier 2 — D: Namespace (256B – 8KB)

Small objects store data in a D: entry on the same shard. Keeps O: small for listing scans while avoiding storage-tier overhead.

**PUT (Tier 2):**
```
txn {
  get(B) → if null or delete-pending → NoSuchBucket
  get(O:) → check for overwrite
  If overwrite: put(G:O, {old_ref_tag, old_chunk_descriptor, child_flags, ...})
  put(O:, {ref_tag, size, etag, content_type, chunk: {type: CHILD_D, size_tier, mtime}, ...})
  put(D:<shard><bucket_id><size_tier><hash_prefix><mtime><ref_tag>, blob_data)
  commit
}
```

Single transaction. No P:O, no storage-tier write. Atomic — no crash-recovery gap.

**GET (Tier 2):**
```
txn {
  get(O:) → read chunk descriptor {type: CHILD_D, size_tier, mtime}
  Construct D: key from (shard + bucket_id + size_tier + Hash(ref_tag)%32 + mtime + ref_tag)
  get(D:<key>) → return blob data
}
```

**HEAD:** `get(O:)` only — returns metadata. No D: read needed.

**GC (Tier 2):** G:O entry contains the chunk descriptor `{type: CHILD_D, size_tier, mtime}`. GC reconstructs the full D: key (knows shard from G:O header, bucket_id from G:O header, size_tier + mtime from chunk descriptor, ref_tag from G:O body) → deletes the D: entry. Also scans `C:<ref_tag>*` for any annotation/tag children.

---

## Tier 3 — Storage Tier (> 8KB)

Large objects use the full three-phase protocol with P:O coordination.

**PUT (Tier 3) — Phase 1:**
```
txn {
  get(B) → if null or delete-pending → NoSuchBucket
  put(P:O, {ref_tag, object_name, estimated_size, created_at})
  commit
}
```

**PUT (Tier 3) — Phase 2:** Write blob to storage tier. Chunk descriptor: `{type: STORAGE, storage_id, blob_id, offset, length}`.

**PUT (Tier 3) — Phase 3:**
```
txn {
  get(P:O) → if null → abort
  get(O:) → overwrite check
  If overwrite: put(G:O, {old_ref_tag, old_chunk_descriptor, child_flags, ...})
  put(O:, {ref_tag, size, etag, chunk: {type: STORAGE, storage_id, blob_id, offset, length}, ...})
  delete(P:O)
  commit
}
```

**GET (Tier 3):** `get(O:)` → read chunk descriptor → fetch data from storage tier via `storage_id + blob_id + offset + length`.

**GC (Tier 3):** G:O entry triggers: free storage-tier blob (via `storage_id + blob_id`) + `RangeScan(C:<ref_tag>*)` to clean children.

---

## GC Cleanup by Chunk Type

GC processes G:O (or G:V) entries. The chunk descriptor in the stripped value tells GC what to do:

| Chunk type | GC action |
|---|---|
| `{type: INLINE}` | NOP for data — just delete G:O. Scan C:<ref_tag>* for annotation/tag children. |
| `{type: CHILD_D, size_tier, mtime}` | Reconstruct D: key → delete D: entry. Scan C:<ref_tag>* for children. |
| `{type: STORAGE, storage_id, blob_id, ...}` | Free storage blob via storage_id. Scan C:<ref_tag>* for children. |

---

## Overwrite Transitions

Overwrite is always: `put(G:O, old_stripped) + put(O:, new_value)` in one transaction. GC handles all cleanup of old data regardless of tier transition:

| Old tier → New tier | PUT protocol | GC cleans old data |
|---|---|---|
| Inline → Inline | Single txn (Tier 1) | G:O has inline data → NOP + scan children |
| Inline → D: | Single txn (Tier 2) | G:O has inline data → NOP + scan children |
| Inline → Storage | 3-phase (Tier 3) | G:O has inline data → NOP + scan children |
| D: → Inline | Single txn (Tier 1) | G:O chunk=CHILD_D → delete old D: entry + scan children |
| D: → D: | Single txn (Tier 2) | G:O chunk=CHILD_D → delete old D: entry + scan children |
| D: → Storage | 3-phase (Tier 3) | G:O chunk=CHILD_D → delete old D: entry + scan children |
| Storage → Inline | Single txn (Tier 1) | G:O chunk=STORAGE → free blob + scan children |
| Storage → D: | Single txn (Tier 2) | G:O chunk=STORAGE → free blob + scan children |
| Storage → Storage | 3-phase (Tier 3) | G:O chunk=STORAGE → free blob + scan children |

Old derived data is NEVER reused — each version has its own ref_tag and its own children/D: entry.

---

## Why P:O is Not Needed for Tiers 1 and 2

The `P:O` coordination entry exists to solve the two-domain problem: storage-tier data can be written but KV commit might fail (crash between Phase 2 and Phase 3). For Tiers 1 and 2:
- Both data and metadata are in the same KV transaction
- Either both commit or neither does — atomic
- No orphaned storage-tier data is possible
- Sweeper has no role for Tier 1/2 PUTs

---

## Versioning

Versioned PUT works identically across all tiers — move old O: to V:<old_vid>, write new O:. The chunk descriptor in V:<vid> retains the tier information. GET with version-id reads V:<vid> → chunk descriptor → fetches data from the appropriate tier (inline in V: value, D: entry by ref_tag, or storage tier).

---

## Cost Comparison

| | Tier 1 (Inline) | Tier 2 (D:) | Tier 3 (Storage) |
|---|---|---|---|
| Transactions | 1 | 1 | 2 |
| KV reads | 2 (B + O:) | 2 (B + O:) | 3 (B + P:O + O:) |
| KV writes | 1–2 (O:, G:O) | 2–3 (O:, D:, G:O) | 2–3 (P:O/O:, G:O) |
| Storage-tier writes | 0 | 0 | 1 |
| GET reads | 1 (O:) | 2 (O: + D:) | 1 (O:) + storage read |
| P:O needed | No | No | Yes |
| Crash recovery | Atomic (N/A) | Atomic (N/A) | Sweeper via P:O |

---

## Background Tier Migration (Tier 2 → Tier 3)

### Purpose

Tier 2 objects (D: entries, 256B–8KB) live in the KV store. When KV capacity is under pressure or objects become cold, they can be migrated to the storage tier. Migration is transparent to clients — the chunk descriptor in O: is updated, GET follows it regardless of tier.

### Migration Protocol (P:D Coordination)

Migration modifies two domains (KV + storage tier) — requires a coordination entry.

**Phase 1 — Record intent:**
```
txn {
  get(O:) → verify exists, read ref_tag, verify chunk type = CHILD_D
  Read D: entry (reconstruct key from chunk descriptor) → get blob data
  put(P:D, {ref_tag, object_name, size})
  commit
}
```

**Phase 2 — Write to storage tier (no KV):**

Write blob to storage tier. Two variants:
- **Simple:** one blob per object (blob_id = ref_tag)
- **Packing:** aggregate multiple objects into a shared blob (uses the same packing mechanism as Tier 3 small-object packing — see [Small-Object-Packing.md](Small-Object-Packing.md))

**Phase 3 — Commit migration:**
```
txn {
  get(P:D) → if null → abort (sweeper cleaned it)
  get(O:) → verify exists, verify ref_tag match (not overwritten during migration)
  put(O:, {chunk: {type: STORAGE, storage_id, blob_id, offset, length}})
  delete(D:<entry>)
  delete(P:D)
  commit
}
```

**Crash recovery:**
- Before Phase 1: nothing happened
- After Phase 1, before Phase 3: P:D remains. Sweeper moves to G:D → GC frees orphaned storage blob.
- After Phase 3: migration complete. D: entry gone, O: points to storage tier.

### Migration Triggers

| Trigger | Description |
|---|---|
| Lifecycle policy (LC) | Per-bucket rule: "migrate D: to storage tier after N days." Cold objects migrated first. |
| Capacity pressure | KV utilization > threshold → migrate largest/oldest D: entries |
| Pre-reshard preparation | Pack all D: entries before resharding (see below) |
| Admin/operator | Manual trigger for capacity planning |

### Packing Variant

The advanced migration aggregates multiple small objects into shared blobs using the same packing mechanism described in [Small-Object-Packing.md](Small-Object-Packing.md). The only difference is timing:
- **Hot path (Tier 3 PUT):** objects are packed within seconds of creation
- **Cold path (Tier 2 migration):** objects lived in D: for days/weeks/months before packing

Both feed into the same packing pipeline. After packing, deletion becomes a logical-punch-hole (mark offset range as free in the shared blob) rather than blob deletion.

### Resharding Interaction

When resharding a bucket, D: entries (up to 8KB each) would need to move between shards — 10-15x more data than metadata-only migration. Solution: **pack before reshard.**

```
Pre-reshard protocol:
1. Scan D: entries for the bucket (by bucket_id prefix)
2. Migrate all D: → storage tier (via packing P:D protocol)
3. All D: entries consumed (deleted in Phase 3)
4. Bucket now has only Tier 1 (inline, travels with O:) and Tier 3 (storage, stays in place)
5. Proceed with lightweight KV-only reshard
```

After reshard: new small objects on new shards create D: entries as normal. The D: namespace resharding concern is eliminated — D: entries are always drained before reshard starts.

---

## Capacity Considerations

| Backend | Max cluster | Tier 2 capacity (avg 4KB objects, ~200B key overhead) |
|---|---|---|
| FDB | 100TB | ~21 billion objects |
| TiKV | 1000TB (1PB) | ~210 billion objects |

Tier 2 reduces per-cluster object capacity by ~5x compared to metadata-only. This is acceptable because:
- The performance win is significant (1 transaction vs 3 round-trips for PUT)
- Background migration (Tier 2 → Tier 3) reclaims capacity when needed
- FDB at 100TB with 21B objects is still a very large cluster
- TiKV at 1PB handles 210B objects before any fleet architecture is needed
