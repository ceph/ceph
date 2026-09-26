# Versioning Test Plan

Tests aligned to the 10-step implementation order.

## Phase 1 — Data structures

- [ ] Round-trip serialize/deserialize `BucketValueHeader` (18B) with all three versioning states
- [ ] Round-trip `ObjectValueHeader` (60B) with vid=0, vid=1, vid=0xFFFFFFFF
- [ ] `make_v_key` produces correct byte layout; lexicographic ordering: same object, higher vid sorts first (BE descending)
- [ ] `is_delete_marker()` flag check

## Phase 2 — Bucket state + PutBucketVersioning

- [ ] `PutBucketVersioning` Enabled on new bucket → state persists across read
- [ ] `PutBucketVersioning` Suspended → state persists
- [ ] Reject Disabled transition → error
- [ ] `verify_bucket_in_txn` returns correct `VersioningState` after toggle
- [ ] Cache invalidation: state change visible immediately after RPC returns

## Phase 3 — Displacement helpers (unit-level)

- [ ] `displace_old_object` with Disabled → old lands in G:O
- [ ] `displace_old_object` with Enabled + real vid → old lands in V:
- [ ] `displace_old_object` with Enabled + NULL_ID → old lands in G:O (not V:)
- [ ] `compute_new_version` for each (state × old_present) combination (6 cases)

## Phase 4 — PUT with versioning

- [ ] PUT to unversioned bucket → vid=0, no V: entries created
- [ ] Enable versioning → PUT x3 → O: has latest vid, V: has two prior entries with descending vids
- [ ] Suspended → PUT → O:.vid == NULL_ID, old real-vid displaced to V:
- [ ] PUT over a delete marker → DM displaced correctly, new O: is live

## Phase 5 — DELETE without vid

- [ ] Unversioned: DELETE existing object → O: gone, G:O present
- [ ] Unversioned: DELETE non-existent → OK (idempotent)
- [ ] Versioned: DELETE → O: is now a fenced DM with new vid, old version in V:
- [ ] Versioned: DELETE non-existent key → creates DM in O: (vid allocated)
- [ ] Suspended: DELETE → DM with vid=NULL_ID, old displaced

## Phase 6 — GET / HEAD

- [ ] GET on fenced O: → 404 + `x-amz-delete-marker: true` header
- [ ] GET on live O: → 200 + `x-amz-version-id` header (omit if vid=0)
- [ ] GET with version-id matching O: → returns current
- [ ] GET with version-id matching V: entry → returns that version
- [ ] GET with version-id on a DM in V: → 405
- [ ] GET with non-existent vid → 404

## Phase 7 — ListObjectsV2

- [ ] List skips fenced entries; only live objects appear
- [ ] Mixed bucket: 3 live + 2 DMs → list returns exactly 3

## Phase 8 — DELETE with vid

- [ ] Case 1: delete non-current V: entry → removed, blob queued to G:O
- [ ] Case 1: delete non-current DM → removed, no blob work
- [ ] Case 2: delete current O: → latest V: promoted to O:, `next_vid` inherited
- [ ] Case 2: delete current O: with no V: entries → O: deleted entirely
- [ ] Idempotent: delete already-removed vid → OK

## Phase 9 — ListObjectVersions

- [ ] Returns all versions (O: + V:) in `(key ASC, vid DESC)` order
- [ ] Delete markers labeled correctly
- [ ] Pagination (continuation token) works across O:/V: boundary

## Phase 10 — GcWorker

- [ ] G:O entry from versioned displacement (has ref_tag) → blob freed
- [ ] G:O entry from DM displacement (no ref_tag) → entry removed, no blob work
- [ ] V: key not in G:O → untouched by GC (only G:O drives cleanup)

## Integration / end-to-end

- [ ] Full lifecycle: create bucket → enable versioning → PUT v1 → PUT v2 → DELETE → GET (404 + DM) → GET ?versionId=v1 (200) → ListVersions (DM + v2 + v1) → DELETE ?versionId=DM → GET (200, v2 promoted)
- [ ] Transition: start unversioned → PUT → enable → PUT → verify old unversioned entry coexists with new versioned entry
- [ ] Concurrency: two PUTs racing on same key with versioning enabled → both succeed, one displaces the other, no lost versions
- [ ] DeleteBucket reject: enable versioning → PUT v1 → PUT v2 (v1 in V:) → DeleteObjectVersion(v2) removes current → DeleteObjectVersion(v1 promoted) removes O: → V: still has original v1 → DeleteBucket fails BucketNotEmpty
- [ ] DeleteBucket allow: same setup → also DeleteObjectVersion the remaining V: entry → DeleteBucket succeeds
