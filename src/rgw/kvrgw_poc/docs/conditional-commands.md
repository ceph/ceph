# Conditional Commands

Conditional headers allow S3 clients to guard operations with preconditions on the target object's state.

---

## Supported Headers

- `If-Match: <etag>` — proceed only if current etag matches
- `If-Match: *` — proceed only if object exists
- `If-None-Match: *` — proceed only if object does NOT exist (writes: PUT/CompleteMultipartUpload only)
- `If-None-Match: <etag>` — proceed only if etag is different (reads: GET/HEAD only, returns 304 if matches)
- `If-Modified-Since: <date>` — proceed only if modified after date (GET/HEAD only)
- `If-Unmodified-Since: <date>` — proceed only if NOT modified after date (GET/HEAD only)

**Validation (at request parsing, before calling check_preconditions):**
- On write operations (PUT/CompleteMultipartUpload), `If-None-Match` only accepts `*`. If an etag value is provided, reject with 400 Bad Request.
- `If-Match` and `If-None-Match` cannot coexist in the same request → 400 Bad Request.
- If `If-None-Match` is present → discard `If-Modified-Since` (etag takes precedence per RFC 7232).
- If `If-Match` is present → discard `If-Unmodified-Since` (etag takes precedence per RFC 7232).

---

## Supported Operations

- **GET / HEAD** — If-Match, If-None-Match, If-Modified-Since, If-Unmodified-Since
- **PUT / CompleteMultipartUpload** — If-Match, If-None-Match
- **DELETE / DeleteObjects** — If-Match only (no If-None-Match)

---

## Error Codes

- 304 Not Modified — If-None-Match matches, or If-Modified-Since not satisfied (GET/HEAD only)
- 404 Not Found — object does not exist (no entry at all)
- 409 ConditionalRequestConflict — a conflicting concurrent operation modified the object while the conditional request was in progress
- 412 Precondition Failed — object exists but condition not met (including delete marker as current)

Note: non-conditional DELETE of a missing object returns 204 (idempotent success). Only conditional DELETE with If-Match returns 404/412 when the target is missing or a DM.

---

## Base Behavior

All conditional operations follow the same pattern:

1. Resolve the target entry
2. Run check_preconditions() against it
3. Pass → proceed with normal operation logic
4. Fail → abort with error, no state change

The conditional check is independent of versioning state. After it passes, the operation proceeds with its normal versioning-aware flow (displacement, DM creation, promotion, etc.) with no conditional-specific logic.

---

## Target Resolution

- **PUT / DELETE without version-id**: target is O: (current version)
- **DELETE with version-id**: target is the specific version — O: if vid matches current, else V:<vid>
- **GET / HEAD without version-id**: target is O:
- **GET / HEAD with version-id**: target is O: if vid matches, else V:<vid>

Once the target is resolved, the same check_preconditions() runs. Everything else is identical regardless of whether version-id was specified.

---

## Delete Markers

A delete marker has no data and no etag. When the resolved target is a delete marker:

- If-Match: <etag> → 412 (DM exists but has no etag — precondition fails)
- If-Match: * → 412 (DM exists but is logically deleted — precondition "exists" fails)
- If-None-Match: * → OK (logically doesn't exist, proceed) — PUT only

This applies whether the DM is in O: (fenced) or in V: (targeted by version-id).

Distinction from missing object: DM returns 412 (entry found, condition failed). Missing entry returns 404 (entry not found).

**404 vs 405 for GET/HEAD on a DM:** The caller handles this outside check_preconditions(). GET/HEAD without versionId targeting a DM → 404. GET/HEAD with versionId targeting a DM → 405 Method Not Allowed. Both include `x-amz-delete-marker: true` header. The conditional check itself just reports "entry is DM" — the caller maps to the appropriate error code.

---

## check_preconditions()

```
check_preconditions(entry, if_match, if_none_match, if_modified_since, if_unmodified_since):
    if entry is None:
        if if_match → return 404
        if if_none_match == "*" → return OK
        return 404

    if entry is delete marker:
        if if_match → return 412 (DM found, precondition fails)
        if if_none_match == "*" → return OK (logically doesn't exist)
        return 404 (normal DM behavior for GET/HEAD without conditionals)

    if if_match == "*":
        return OK (object exists)
    if if_match and entry.etag != if_match:
        return 412
    if if_none_match == "*" and entry exists:
        return 412
    if if_none_match and if_none_match != "*" and entry.etag == if_none_match:
        return 304
    if if_modified_since and entry.mtime <= if_modified_since:
        return 304
    if if_unmodified_since and entry.mtime > if_unmodified_since:
        return 412

    return OK
```

---

## Placement

- **GET / HEAD**: called after reading the entry. No transaction needed (read-only). A transiently stale 304/412 is harmless — client retries and gets fresh state.
- **PUT Phase 3 / DELETE**: called inside the commit transaction, after get(O:) or get(V:<vid>), before displacement logic.

Two properties:

1. The get(O:) read for the conditional check does not need conflict range protection — it doesn't change anything, and a stale 412 is harmless (client retries).
2. The write-write conflict on `put(O:, ...)` serializes this operation against any concurrent PUT or DELETE on the same key — conditional or not. The other operation doesn't need to be conditional; any write to O: conflicts.

---

## Batch Delete (DeleteObjects)

DeleteObjects supports `If-Match` per-object in the request body. Each object in the batch is evaluated independently — same conditional check, same error codes. Objects that fail the precondition return 412 in the per-object response; objects that pass proceed with normal delete logic. No interaction between objects in the batch.

---

## No Interaction with Versioning Logic

Conditional checks do not alter displacement rules. They are a pure pass/fail gate:
- Pass → operation proceeds with its normal versioning-aware flow
- Fail → abort, no mutation, return error

---

## CopyObject Conditionals

See [copy-object.md — Conditional Headers](copy-object.md#conditional-headers-on-copyobject) for the full CopyObject design including data sharing, ref_count, and metadata-only copy.

CopyObject supports:
- `x-amz-copy-source-if-match` / `x-amz-copy-source-if-none-match` — preconditions on the **source** object
- `If-Match` / `If-None-Match` — preconditions on the **destination** object

Same check_preconditions() logic. Source condition evaluated first; if it fails, operation aborts before touching destination.
