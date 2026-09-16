# Versioning Changes Log

Deviations from [IMPLEMENTATION_PLAN_VERSIONING.md](IMPLEMENTATION_PLAN_VERSIONING.md) are recorded here for review.

---

## Step 1 — V: key sort order clarification

Plan stated "higher vid sorts first (BE descending)" in the test plan. Actual: **lower vid value sorts first** in forward scan (lower BE bytes). This still achieves the design goal because newer displaced versions have lower vid values (allocated by decrementing from 0xFFFFFFFF). A forward range_scan on V: prefix returns the most recently displaced version first.

Implication for Step 8: `DELETE with vid` Case 2 promote-latest should use **forward** `range_scan(V:prefix, limit=1)` — not reverse as the pseudocode says.

## Step 8 — Forward scan for promote-latest

As noted above, `DeleteObjectVersion` Case 2 uses forward `range_scan(V:prefix, limit=1)` to find the most recently displaced version. The plan's pseudocode said `reverse` — corrected in implementation.

## Step 10 — No code changes needed

The existing GcWorker already handles all G:O entries correctly regardless of origin. Versioned displacement writes proper `GcValueHeader` with the correct chunk type. Delete markers displaced to G:O have `CHUNK_INLINE` with size=0 — the GC's inline path simply removes the G:O entry with no blob work. No changes required.

There is no `G:V` key type. `DeleteObjectVersion` moves blob references to G:O (not a hypothetical G:V) because G:O is a generic "blob cleanup queue" — the GcWorker only needs chunk type, size, mtime, and ref_tag to clean a blob, regardless of whether it came from an overwrite, unversioned delete, or explicit version removal.

**Gap identified:** `DeleteBucket` does not scan V: entries. A bucket with orphaned versions (no current O: entries) can be deleted, leaving V: entries permanently unreachable by GC. Fix: `DeleteBucket` should reject with `BucketNotEmpty` if V: prefix is non-empty.
