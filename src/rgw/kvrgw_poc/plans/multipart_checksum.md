# Multipart checksums: `x-amz-checksum-*` and `x-amz-sdk-checksum-algorithm`

## Overview

**Goal:** make 7 of the 8 checksum tests in the `multipart-other` category pass:

- `test_multipart_checksum_sha256`
- `test_multipart_use_cksum_helper_{sha256,sha1,crc32,crc32c,crc64nvme}`
- `test_get_multipart_checksum_object_attributes`

`test_multipart_reupload_checksum_and_etag` stays failing by design (see Decisions, item 4).

Source: `../../test/rgw/s3-tests/s3tests/functional/test_s3.py` (lines 15821-16111, 19861).

**Current state (verified):**
- The vendored versitygw controller already parses and validates the checksum request headers and writes the checksum response headers.
  - `UploadPart`: `frontend/third_party/versitygw/s3api/controllers/object-put.go:277`.
  - `CompleteMultipartUpload`: `object-post.go:269`.
  - `CreateMultipartUpload`: `object-post.go:147`.
- `KvRgwBackend` ignores all checksum fields (`frontend/backend_kvrgw.go:930, 956, 979`). `backend/src` has no checksum storage or computation. Only the MD5 ETag exists.
- The C API and the `kvrgwc` Go binding carry no checksum data.

**Test matrix (from the tests):**

| Algorithm | ChecksumType | Test |
|---|---|---|
| SHA256 | COMPOSITE (default) | `test_multipart_checksum_sha256`, `_helper_sha256`, `test_get_multipart_checksum_object_attributes` (and `_reupload_checksum_and_etag`, out of scope) |
| SHA1 | COMPOSITE | `_helper_sha1` |
| CRC32 | FULL_OBJECT | `_helper_crc32` |
| CRC32C | FULL_OBJECT | `_helper_crc32c` |
| CRC64NVME | FULL_OBJECT | `_helper_crc64nvme` |

- COMPOSITE is `base64(hash(concat(raw part digests)))` followed by `-N`.
- FULL_OBJECT is the CRC of the whole object, derived from the part CRCs and part sizes by a CRC combine step. The parts are not re-read.

**Required behaviors:**
- `CreateMultipart` stores the algorithm and the type, and echoes the algorithm.
- `UploadPart` computes the part digest, verifies it against the client's header, stores it, and returns it.
- `Complete`:
  - verifies each part's checksum, which can be missing or mismatched;
  - verifies the client's whole-object checksum;
  - computes and stores the final checksum;
  - returns the checksum and the type.
- Failed verification returns `BadDigest` (400). The upload must stay retryable after `BadDigest`.
- `HeadObject` and `GetObject` return the checksum and type only with `ChecksumMode=ENABLED`.
- `GetObject` with `PartNumber=N` returns the part's checksum.
- `GetObjectAttributes` returns `Checksum` and per-part checksums.

**Non-goals:**
- SHA512, MD5 and XXHASH variants. They are accepted by the controller but not required by any test.
- Non-multipart `PutObject` checksums.
- `UploadPartCopy`.
- COMPOSITE for the CRC algorithms.

## Decisions

1. **Digests are computed in the C++ backend** (in `upload_part`, where the part MD5 is already computed). The Go frontend only passes the algorithm, the type and the client-supplied checksum strings through the C API.
2. **Complete verification and the final checksum computation run in the backend.** Go does not see stored part checksums.
3. **No backward compatibility.** This is a POC. `MPartValue`, the `:O:` header, the head record and the manifest formats change in place, with no versioning or migration. Existing data in the KV store is discarded.
4. **No re-Complete of an already completed upload; AWS rules are kept.** A second Complete on a finished upload keeps returning `NoSuchUpload`. `test_multipart_reupload_checksum_and_etag` completes the same upload twice, so it is out of scope and stays failing. This matches the existing treatment of `test_multipart_upload` in `scripts/run_ceph_rgw_tests.sh:95`.
5. **Per-part checksums of a completed object are stored in three tiers.** The per-part checksum list is one blob: the raw digests of the included parts, concatenated in part-number order (entry i belongs to the i-th included part of the manifest). It is not part of the manifest. The tier is picked once, at Complete, and recorded in the object value, so reads never depend on the current tier configuration.

   | Tier | Condition | Where the blob lives |
   |---|---|---|
   | 1. Embedded | the whole object value, including the blob, stays within `kMaxOValueBytes` (1024) | inside the `:O:` value, read with it |
   | 2. Child-D KV | otherwise, blob size is below the Child-D limit (`max_kv_store`, default `kDefaultMaxKvStore` = 4096, from `tier_config_state_`) | an `S` child KV of the object (item 7), written in the same transaction as the `:O:` value |
   | 3. Data-tier file | otherwise | one file named `<ref_tag>_checksum` in the data tier, referenced from the object value through its ref_tag |

   - 4096 bytes hold 128 SHA256 digests (32 bytes each), 256 SHA1 or 512 CRC64NVME (8 bytes each) digests, or 1024 CRC32 or CRC32C (4 bytes each) digests.
   - The combined (final) checksum of the whole object, the algorithm, the type and the tier marker are small and always embedded in the `:O:` value, whatever tier the blob uses. GetObjectAttributes with only the `Checksum` attribute, and HEAD/GET with `ChecksumMode=ENABLED`, are served from the `:O:` value alone.
   - GetObjectAttributes with `ObjectParts` reads per-part checksums with pagination (`MaxParts`, `PartNumberMarker`). Entry i is at byte offset `i * digest_len` of the blob, so a page needs only the slice `[first_rank * digest_len, (first_rank + n) * digest_len)`. `first_rank` comes from the manifest's included parts (`std::upper_bound` on the marker, the same pagination as `get_object_attributes` in `plans/multipart_test_fixes.md`).
     - Tier 1: slice the embedded blob.
     - Tier 2: one KV get of the record (at most 4096 bytes), then slice in memory.
     - Tier 3: one ranged `data_store_.read(filename, offset, length, out)` for the page only, never the whole file.
   - The blob size is derived (included part count times digest length). The child key is rebuilt on read from the `:O:` value (item 7).
   - Plain GET, and HEAD without `ChecksumMode`, never touch tier 2 or 3. HEAD with `ChecksumMode=ENABLED` only needs the embedded final checksum.
6. **The manifest moves to a child-D KV when it makes the `:O:` value too big.** Decided once at Complete, before the checksum blob is placed:
   - Let `V` be the size of the `:O:` value with the manifest embedded (checksum blob not counted).
   - If `V` > `kMaxOValueBytes` (1024), the manifest is stored in a child-D KV and removed from the `:O:` value. Otherwise it stays embedded, as today.
   - **No size limit on the manifest child for now.** Whatever the manifest size, it fits in a child-D. The Child-D limit (`max_kv_store`, 4096) applies only to the checksum blob (item 5), not to the manifest. The largest manifest is about 75 KB by the formula in `multipart_manifest.hpp:115-121` (my arithmetic, not a compiled value; confirm with a `static_assert` against `kManifestBodyWorstCase`). That is under FDB's 100 KB value limit. Confirm that the child write path applies no 4096-byte check to this record.
   - The checksum blob is then placed by the tier rules in item 5, using the `:O:` size after the manifest decision. A moved manifest makes tier 1 more likely for the blob.
   - A manifest that pushes the `:O:` value over 1 KB only occurs for irregular uploads. The three-knob encoding stores a regular upload (consecutive part numbers, one size, last part shorter) in a few dozen bytes. The manifest grows by roughly 5 bytes for each part with its own size (a 2-byte part id and a 3-byte size), so about 200 such parts reach 1 KB. These figures are estimates and must be measured.
7. **One `:O:` KV owns several child KVs ("child-D").** A multipart object can have up to two new children: the manifest and the checksum blob (tier 2). The design must let the `:O:` value find each child and delete all of them.
   - **Key format (decided).** `[header 14B][ref_tag 12B][child_type 1B]`, 27 bytes. The header is `KeyHeaderS` (`ns='S'`, shard_count, shard_id, bucket_id, `cat='C'`), the same prefix the existing children use (`make_ct_key`, `keys.cpp:517`). The ref_tag is the object's own `hdr.ref_tag`. Two new child types are added next to `kChildTypeTags` `T`, `kChildTypeAnnotation` `A` and `kChildTypeExtended` `E` in `constants.hpp:70-72`: `kChildTypeManifest` = `M` and `kChildTypeChecksum` = `S`.
   - **No size, tier or mtime in the key.** The key is rebuilt from (bucket_id, ref_tag, child_type) alone, so the `:O:` value needs no `child_mtime`, and nothing derived from the object size. This removes the D-key inputs (`size_tier`, `hash_prefix`, `mtime`) that made several children per object impossible.
   - **Parser.** There is no parser for `S:C` keys today (`parse_d_key` handles only the 31-byte `KeyHeaderD`, `keys.cpp:419`). Add one `parse_child_key(key)` returning bucket_id, ref_tag and child_type. It accepts exactly 27 bytes with `ns='S'` and `cat='C'`, and a child_type from the full set `T A E M S`, so it covers the existing children and both new ones. An unknown child_type returns `nullopt`. `parse_d_key` is not changed and keeps parsing data D keys only. The two parsers cannot be confused: namespace `S` versus `D`, and 27 versus 31 bytes.
   - **Value format.** The payload is stored as is: the encoded manifest body for `M`, the concatenated digests for `S`. No `ChildValueHeader` and no ref count (see "Sharing with copies").
   - **Size.** The checksum child (`S`) is written only when the blob is at or under the Child-D limit (`max_kv_store`, 4096). The manifest child (`M`) has no limit for now (item 6). Confirm that the `S:C` write path applies no value-size check other than the store's own.
   - **Locating: one `placement` byte, no redundant state.** The multipart tail segment of the `:O:` value holds one byte: bit 0 = manifest is in an `M` child, bit 1 = checksum blob is in an `S` child (tier 2), bit 2 = checksum blob is a data-tier file (tier 3). Tier 1 is "checksum present, bits 1 and 2 clear". The tier is therefore stored once and cannot disagree with the child list. Readers use this byte, never the current configuration, and skip the `kv_get` for an absent child.
   - **Flags.** Two new object flag bits (the next free bits after 0x10; verify in `object_value.hpp:282-286`): `kFlagMultipartChildren` is set when bit 0 or bit 1 of `placement` is set, and `kFlagChecksumFile` when bit 2 is set. Both are copied into the G:O header flags (`gc_hdr.flags = h->flags`, `service_impl.cpp:1360`), so GC can see them without the `:O:` value. They cannot reuse `kFlagExternalTags` or `kFlagExtendedAttrs`, which also guard the `T`, `A` and `E` children.
   - **Delete all.** All children of an object share the prefix `[header][ref_tag]`, so one range clear removes every child, whatever the count (the pattern at `service_impl.cpp:1426-1430`: `make_c_prefix`, then `kv_range_clear(prefix, prefix + 0xFF)`).
     - **The clear is done at delete time, not in GC.** `move_object_to_g` knows the object's own `hdr.ref_tag`. GC does not: for `CHUNK_MULTIPART_REF` the G:O entry carries the original's shared ref_tag as its chunk identity (`service_impl.cpp:1362-1375`), so `clean_children` in `gc_worker.cpp:347` would clear the original's prefix instead of the copy's. The GC worker therefore needs no child handling for `M` and `S`.
     - `move_object_to_g` clears the object's own prefix in both of its branches (the deferred-to-GC branch at `service_impl.cpp:1357-1387` and the direct branch at 1388-1434) whenever `kFlagMultipartChildren` is set, in the same transaction that deletes the `:O:` KV. In the direct branch, the `M` child is read first, because the `G:M{FREE_PARTS}` entry is built from the manifest (`service_impl.cpp:1411`).
     - **Rule: a `CHUNK_MULTIPART` or `CHUNK_MULTIPART_REF` object is never deleted inline, on any delete path.** Its data always goes to the GC domain: `G:M{FREE_PARTS}` (or `GM_MODE_FREE_PARTS_CKSUM` for a tier 3 object) when unshared, a `G:O` entry with the object's flags when shared. The same transaction clears the object's own `S:C` prefix. `move_object_to_g` already behaves this way (`service_impl.cpp:1404-1420`).
     - `move_object_to_g` is not the only delete path, and the other one breaks the rule. `delete_object_version` removes the current and non-current versions with its own code (`service_impl.cpp:3095-3126` and `3161-3179`). It handles only `CHUNK_CHILD_D` and `CHUNK_STORAGE`, so a multipart object deleted there leaks its part files, its children and its tier 3 file. For the non-current version it also writes a `G:O` entry with `gc_hdr.flags` left at 0, so the shared state is lost.
     - **Fix.** Split `move_object_to_g` into two steps: (a) hand the object's data to GC (the `G:M` or `G:O` entry, plus the clear of the object's own `S:C` prefix) and (b) delete the key. `move_object_to_g` keeps its behavior by calling (a) then (b). In `delete_object_version`, an entry whose `chunk_type` is `CHUNK_MULTIPART` or `CHUNK_MULTIPART_REF` uses step (a) for both the current-version and the non-current-version branch, with its raw value. This is the only multipart delete code.
       - Current version, a `:V:` entry exists to promote: step (a) only. The `kv_put(object_key, promoted)` (line 3089) overwrites the `:O:` key, so no delete is needed, and nothing depends on write order. This matches `displace_old_object` with versioning enabled, which puts the old value and lets the new `:O:` put overwrite the key (`service_impl.cpp:1454-1462`).
       - Current version, nothing to promote: step (a) then the existing `kv_del(object_key)` (line 3095).
       - Non-current version: step (a) with the `:V:` key as the key argument, then the existing `kv_del(v_key)` (line 3179). The `:V:` key is accepted by the parser as at `service_impl.cpp:1482` (to confirm).
     - **Audit.** Every code path that removes or overwrites an `:O:` or `:V:` key is checked for the same rule (grep for `kv_del` on object and version keys, and `displace_old_object` callers at `service_impl.cpp:1498, 1702, 1708`). Any path that does not go through `move_object_to_g` for a multipart value is routed through it.
   - **Tier 3 file cleanup (new).** Today a deleted multipart object only produces `G:M{FREE_PARTS}` from the manifest (`service_impl.cpp:1404-1420`), and the shared last-reference path does the same from the `R:` value (`gc_worker.cpp:381-399`). Neither knows about `<ref_tag>_checksum`. Add `GM_MODE_FREE_PARTS_CKSUM` = 3 (same layout as `FREE_PARTS`; `multipart_value.cpp:196` must accept it). `process_gm_entry` (`gc_worker.cpp:98`) also removes `<ref_tag>_checksum` in that mode. `move_object_to_g` picks mode 3 when `kFlagChecksumFile` is set. The shared last-reference path in the GC worker picks it from `gc_val.hdr.flags`. A copy keeps the flag (it keeps the tier), so whichever of original and copy is dropped last frees the file.
   - **Sharing with copies.** A copy owns its children. `copy_object` writes the copy's children under the copy's own `hdr.ref_tag`, as it already does for the tags child (`service_impl.cpp:4025-4032`), so deleting either object clears only its own prefix, and no ref count is needed.
     - The copy does not blindly copy `placement`. Its `:O:` value can differ from the source's (a replaced metadata directive changes the size), so it loads the source's manifest and checksum blob through the loaders and runs the placement function again for the new value.
     - The `R:` record created for the first copy stores the source's manifest as its chunk descriptor (`service_impl.cpp:3996`), so the source manifest must come from `load_manifest`, not from `src.manifest_body`, which is empty when it is a child.
     - The part files and the tier 3 file stay shared through the data ref_tag.
   - **Failure paths.** The `M` and `S` children are written in the Complete transaction together with the `:O:` put, so a failed or reverted Complete leaves none. The tier 3 file is the only write outside that transaction. It is written after all verification has passed, under a name derived from the upload's ref_tag, so a retried Complete overwrites it. A failed commit removes it inline, best effort. A crash between the file write and the commit leaves one orphan file; the `P:O` record that `select_storage_tier` uses removes `ref_tag.filename()`, not `<ref_tag>_checksum` (`service_impl.cpp:2438-2451`), so it cannot be reused as is. Accepting this window for the POC is the recommendation; extending the orphan record to carry a filename suffix is the alternative if it is not acceptable.
   - **Access.** One loader per child kind, `load_manifest()` and `load_checksum_blob(first_rank, count)`. Each checks the `placement` byte, builds the key, and does one `kv_get` (tier 3 reads go to the data store instead). No caller reads a child key directly.

## Extra storage access

Checksums add no access for objects without a checksum algorithm. Manifest placement (item 6) applies to every multipart object whose `:O:` value with the embedded manifest is over 1 KB, with or without checksums.

| Operation | Tier 1 | Tier 2 | Tier 3 |
|---|---|---|---|
| UploadPart | digest computed in memory and stored in the S:U part row already written | same | same |
| Complete | no extra access | one extra KV put in the `:O:` transaction | one data-tier write after verification, removed again if the commit fails (no `P:O` record; Decisions item 7, failure paths) |
| GetObjectAttributes with `Checksum` only, HEAD/GET with `ChecksumMode=ENABLED` | no extra access | no extra access | no extra access |
| GET/HEAD with `PartNumber=N` | no extra access | one extra KV get in the read transaction | one ranged data-tier read of one digest |
| GetObjectAttributes with `ObjectParts` (one page) | no extra access | one extra KV get in the read transaction | one ranged data-tier read of the page |
| Delete / GC | no extra access | the `S` child is cleared with the `:O:` delete (Decisions item 7) | the file is freed through GC (`GM_MODE_FREE_PARTS_CKSUM`) |

Manifest in a child-D KV (items 6 and 7), independent of the checksum tier:

| Operation | Extra access |
|---|---|
| Complete | one extra KV put in the `:O:` transaction (two when the checksum blob is also a child) |
| Any read that needs the manifest: GET of the body (full, range or `PartNumber`), HEAD with `PartNumber`, GetObjectAttributes with `ObjectParts` | one extra KV get in the read transaction |
| Plain HEAD, GetObjectAttributes without `ObjectParts` | none (`part_count`, size and the combined checksum are in the `:O:` header) |
| CopyObject of a multipart object | one extra KV get of the source manifest, plus one get and one put per child (the copy writes its own children) |
| Delete | one extra KV get of the manifest child (for the `G:M{FREE_PARTS}` entry), plus one range clear of the object's child prefix, all in the delete transaction |
| GetObjectAttributes with `ObjectParts` | the manifest child get and the checksum child get are independent, so issue both asynchronously in the same read transaction (the pattern of `kv_async_get`) |

---

## Sub-task 1: Design spike and algorithm core

**Intent:** build the C++ digest primitives that every other sub-task uses.

**Expected Outcomes:**
- A short decision note appended to this file.
- One C++ checksum module in `backend/src` covering SHA1, SHA256, CRC32, CRC32C and CRC64NVME.
  - Each algorithm can digest a buffer.
  - CRC algorithms support a combine function with the signature (crc1, crc2, len2).
  - Composite builder: hash over raw part digests, formatted as `base64-N`.
- Unit tests using the known values from the s3-tests:
  - parts A/B/C at 5 MiB each;
  - part and composite values for all five algorithms, taken from `test_multipart_use_cksum_helper_*`.

**Todo List:**
- [x] Check if `rgw_cksum.{h,cc}` can be reused, and check how the backend CMake links against it. If not, implement the algorithms directly (OpenSSL for SHA1 and SHA256; table or hardware CRC for the CRC algorithms).
- [x] Implement the module and its unit tests.

**Decision note:** `rgw_cksum.h` includes boost, `fmt`, `common/armor.h`, `include/buffer.h` and `include/encoding.h`, so the backend cannot link it. Implemented directly in `backend/src/checksum.{hpp,cpp}` (OpenSSL EVP for SHA1/SHA256; slice-by-8 table CRC32/CRC32C/CRC64NVME; GF(2) CRC combine; `CksumHasher`, `CombinedCksumBuilder`, base64 helpers, name/type validation). Tests: `backend/tests/checksum_test.cpp` (`checksum_test`), passing.

**Relevant Context:**
- `backend/src/etag.{hpp,cpp}`: existing MD5 and composite ETag pattern (`CompositeEtagBuilder`).
- `backend/src/service_impl_multipart.cpp:624`: existing OpenSSL use.
- `src/rgw/rgw_cksum.{h,cc}`, `rgw_cksum_pipe.*`, `rgw_cksum_digest.h`.

**Status:** `[x] done`

---

## Sub-task 1b: Online minimum-part-size override (admin socket)

**Intent:** let backend tests build multipart objects with many parts cheaply. At the real 5 MiB minimum, 10,000 parts need about 50 GB, which is too much storage and too slow. Checksum and listing logic do not depend on part size, so these tests use the override. It is changed at runtime through the admin socket and is applied only for the duration of each such test.

**Expected Outcomes:**
- The effective minimum non-last part size is a runtime value in the service. It defaults to `kMinPartSizeBytes` (5 MiB), which keeps its value.
- Two admin commands, in the style of `get-tier-config` / `set-tier-config`:
  - `get-min-part-size` returns the effective value.
  - `set-min-part-size <bytes>` sets it. `set-min-part-size 5120` selects 5 KiB. `set-min-part-size default` restores 5 MiB. A value of 0, or above 5 MiB, returns `ERROR`.
- The only enforcement point is the check at `service_impl_multipart.cpp:1161`. It reads the runtime value instead of `kMinPartSizeBytes`.
- The value is process-wide state, so it is read with relaxed atomic loads; no lock on the UploadPart or Complete path.
- Nothing sets it at startup: no env var, no CMake option. A server restart returns it to 5 MiB.
- At 5 KiB, 10,000 parts need about 50 MB, and 256 parts need about 1.3 MB.
- The same mechanism serves every backend multipart test that needs many parts, not only the checksum tests. Candidates are `list_parts` pagination (the current tests use `max_parts` 1 and 1000 with 2 to 4 parts, in `multipart_phase2_test.cpp` and `multipart_gc_test.cpp`) and the over-10,000 check in `multipart_phase2_test.cpp:278`.

**Todo List:**
- [x] Add an atomic minimum-part-size member and accessor/setter to `KvRgwServiceImpl`, next to `err_insertion()` in `service_impl.hpp`.
- [x] Replace the use of `kMinPartSizeBytes` at `service_impl_multipart.cpp:1161`.
- [x] Add the two commands in `AdminServer::handle_line` (`admin_server.cpp`), and pass the service reference through the constructor, as is done for `err_insertion` and `ops_stats` (wired in `kvrgw_runtime.cpp:222`).
- [x] Unit test through `AdminServer::handle_line` or the setter: after `set-min-part-size 5120`, a 5 KiB non-last part is accepted and a 4 KiB one returns `KVRGW_ERR_ENTITY_TOO_SMALL`; after `default`, a 5 KiB non-last part returns `KVRGW_ERR_ENTITY_TOO_SMALL`; invalid values return `ERROR`.
- [x] A shared test helper (scope guard) that sets the value on construction and restores `default` on every exit path, so a failing test cannot leave the server with the small minimum. All many-part tests use it, and the checksum tests in sub-tasks 2 and 5 do too.
- [x] Add a `list_parts` pagination test over many parts using the helper: for example 256 parts of 5 KiB, paged with several `max_parts` values and part-number markers, checking `is_truncated` and the next marker on every page. This extends the existing 2-part test at `multipart_phase2_test.cpp:169`.
- [x] Document the commands next to the other admin commands.

**Relevant Context:**
- `backend/src/constants.hpp:107` (`kMinPartSizeBytes`).
- `backend/src/service_impl_multipart.cpp:1160-1163`.
- `backend/src/admin_server.cpp:168-229` (`get-tier-config` / `set-tier-config` pattern) and `:463` (`set-error`).
- `backend/src/kvrgw_runtime.cpp:222` (admin server wiring).
- `backend/tests/batch_mixed_test.cpp:917` (a test that starts its own admin socket).

**Constraint:** the override is for backend tests only. The s3-tests never set it. `test_multipart_upload_size_too_small` uploads 10 KiB parts and expects `EntityTooSmall`, so it would fail with the small minimum. The checksum s3-tests use 5 MiB parts and 1 KiB parts and need no override.

**Constraint:** tests that run against the same server share the process-wide value, so tests that set the override must not run in parallel with tests that rely on the real minimum (for example the `EntityTooSmall` case at `multipart_phase2_test.cpp:367`). Either run them in one sequential process, or give each test binary its own server instance, as `batch_mixed_test.cpp:917` does with its own admin socket.

**Status:** `[x] done`

**Done:** `min_part_size_` atomic in `KvRgwServiceImpl` (`min_part_size()`), passed to `process_manifest_row` from `scan_upload_rows`; admin commands in `admin_server.cpp`; helper `backend/tests/min_part_size_guard.hpp` (`MinPartSizeGuard`, `admin_cmd`); test `backend/tests/multipart_minpart_test.cpp` (live FDB: needs `FDB_CLUSTER_FILE=<root>/.fdb/fdb.cluster`); docs in `docs/background-and-admin.md`.

---

## Sub-task 2: Persist checksum metadata (backend storage)

**Intent:** store the upload-level algorithm and type, each part's checksum, and the completed object's checksums in the three tiers, so Complete and the read paths can use them.

**Expected Outcomes:**
- The upload head record stores the algorithm and type.
- `MPartValue` stores the part checksum (raw bytes, up to 32 for SHA256; the algorithm comes from the upload).
- The completed object value always embeds the algorithm, the type, the final checksum and the tier marker.
- The per-part checksum blob is stored in tier 1, 2 or 3 (Decisions, item 5). The manifest encoding is not changed, but its placement is (Decisions, item 6).
- The manifest is stored in a child-D KV whenever the `:O:` value with the manifest embedded is over 1024 bytes, with no size limit on the manifest (Decisions item 6).
- The `:O:` value holds one `placement` byte (manifest child, checksum child, checksum file), and two flag bits mark objects that have children or a checksum file (Decisions item 7). Every delete path range-clears the object's own children in the delete transaction. GC does not clear `M` or `S` children.
- A deleted object, or the last reference of a shared one, also frees the `<ref_tag>_checksum` file through a new `G:M` mode.
- Every reader of the manifest gets it through one loader that follows `placement`, so no caller reads `manifest_body` from the value directly.
- Placement is two decisions made by one function: manifest first (input: `:O:` size with the manifest embedded), then the checksum tier (inputs: digest length, included part count, the `:O:` size after the manifest decision plus the fixed checksum segment, Child-D limit).
- The tier 3 filename helper sits next to `part_filename` in `multipart_value.hpp`, so the format is defined in one place.
- The `static_assert` size checks are updated. `kMaxMultipartOValueBytes` grows only by the fixed-size fields, because the blob is embedded only when the value stays within `kMaxOValueBytes`.
- Existing multipart unit tests pass.

**Todo List:**
- [x] Add the algorithm and type fields to `MHeadValue` (S:M head).
- [x] Add a checksum field to `MPartValue`, with `make_m_part_value` and `parse_m_part_value` updated.
- [x] Add the final checksum, algorithm, type and tier marker to the completed `:O:` value as a tail segment, following the `write_object_value` / `parse_object_value` segment pattern. Tier 1 appends the blob as part of that segment.
- [x] Implement the placement function (manifest, then checksum tier).
- [x] Child keys (Decisions, item 7): add `kChildTypeManifest` = `M` and `kChildTypeChecksum` = `S` in `constants.hpp:70-72`. Add `make_child_key(bucket_id, ref_tag, child_type, out)` next to `make_ct_key` (`keys.cpp:517`), and make `make_ct_key` use it.
- [x] Add `parse_child_key` (27 bytes, `ns='S'`, `cat='C'`, child_type in `T A E M S`) and its unit tests: every type round-trips; wrong length, wrong namespace, wrong category and unknown child_type are rejected; a data D key and an `S:O` key are rejected.
- [x] Check that no scan over the `S` namespace (listing, GC, stats) misreads an `S:C` key with the new types.
- [x] Add the `placement` byte (bit 0 `M` child, bit 1 `S` child, bit 2 tier 3 file) to the multipart tail segment of the `:O:` value.
- [x] Add the flag bits `kFlagMultipartChildren` and `kFlagChecksumFile` (`object_value.hpp:282-286` uses 0x01 to 0x10).
- [x] Delete-all in `move_object_to_g` (`service_impl.cpp:1331-1436`), both branches: when `kFlagMultipartChildren` is set, read the `M` child first in the direct branch (for the `G:M{FREE_PARTS}` entry), then range-clear the object's own `S:C` prefix, in the same transaction as the `:O:` delete. `gc_worker.cpp` is not changed for `M` and `S`.
- [x] Split `move_object_to_g` (`service_impl.cpp:1331-1436`) into a GC hand-off step (G entry plus `S:C` prefix clear) and the key delete. `move_object_to_g` calls both, so its callers do not change.
- [x] Multipart is always deleted through the GC domain (Decisions item 7, rule): in `delete_object_version` (`service_impl.cpp:3095-3126`, `3161-3179`), call the hand-off step for `CHUNK_MULTIPART` and `CHUNK_MULTIPART_REF` entries with the raw value, in the current-version branch (promote: no key delete; no promote: existing `kv_del`) and in the non-current branch (existing `kv_del(v_key)`). This also fixes the existing part-file leak there. Audit the other `:O:` / `:V:` removal paths and `displace_old_object` callers.
- [x] Add `GM_MODE_FREE_PARTS_CKSUM` = 3 (`multipart_value.hpp:253`, accepted by `multipart_value.cpp:196`). `process_gm_entry` (`gc_worker.cpp:98`) also removes `<ref_tag>_checksum` in this mode. `move_object_to_g` and the shared last-reference path (`gc_worker.cpp:381-399`) select it when `kFlagChecksumFile` is set.
- [x] Tier 2 of the checksum blob: write it as an `S` child, and read it through `load_checksum_blob`.
- [x] Manifest in a child: write it as an `M` child in the Complete transaction (`service_impl_multipart.cpp:1516-1519`), and add `load_manifest`. Route the existing readers through it: `service_impl.cpp` `decode_manifest_body` call sites (4326, 4484, 4517), `move_object_to_g` (1411), and `copy_object` (3996, 4007).
- [x] CopyObject and `CHUNK_MULTIPART_REF` (`service_impl.cpp:3869-4010`): take the source manifest and checksum blob through the loaders (not `src.manifest_body`, which is empty for an `M` child, including in the `R:` record at 3996), run the placement function for the new value, and write the copy's own `M` and `S` children under its own `hdr.ref_tag`. The tier 3 file stays shared through the data ref_tag.
- [x] Tier 3: write and read `<ref_tag>_checksum` through `data_store_`. Write it only after verification has passed; remove it on a failed commit. The orphan window after a crash is accepted for now (Decisions item 7, failure paths).
- [x] Change the formats in place; no versioning or migration.
- [x] Round-trip unit tests for each changed format and for each tier, including the boundaries: exactly at 1024 bytes and one byte over for the manifest; exactly at the Child-D limit and one byte over for the checksum blob.
- [x] Multi-child unit tests (direct KV level): an object with placement `M`, with `S`, and with both, and with the existing `T` child as well; each child is found by its key; delete removes every child and nothing under another ref_tag; a lookup of a child absent from `placement` does no `kv_get`.
- [x] Delete-path coverage, one test per path, each with an `M` and an `S` child: overwrite of the same key (unversioned), overwrite in a versioned bucket (the old value moves to `:V:` and its children must survive), `DeleteObject` of the current version, `DeleteObject` of a non-current version, and `DeleteObject` without a version id. Include the case of deleting a current multipart version while a non-current version exists, so the promoted value is still present afterwards. After each, the surviving or removed children match the expectation, no inline data deletion happened (the part files are removed only after GC runs), and a tier 3 object leaves no `<ref_tag>_checksum` file once GC has run.
- [x] Manifest placement tests through the service API, using the override from sub-task 1b. Regular uploads give a tiny manifest, so these tests use irregular uploads (each part with its own size, or gaps in the part numbers) to grow it. The part counts are estimates and must be tuned to the measured manifest size:

  | Upload | Manifest | Expected placement |
  |---|---|---|
  | 8 equal parts | under 1 KB | embedded |
  | about 300 parts, each with its own size | over 1 KB | child-D |
  | about 1000 parts, each with its own size | over 4 KB | child-D (no size limit) |
  | 10,000 parts, each with its own size | the largest manifest, about 75 KB | child-D; checks that the record fits |

  Each case reads the object back (full GET, range GET, `PartNumber` GET), deletes it, and checks that the child KVs are gone and the parts are freed. A case with both a child manifest and a child checksum blob checks that two separate child KVs exist, and that both are removed on delete.
- [x] Original-deleted-first and copy-deleted-first cases for a copied object with an `M` child and a tier 2 or 3 checksum blob: the survivor still reads its manifest and checksums (tier 3 file and part files stay until the last reference drops).
- [x] End-to-end tier tests through the service API with SHA256, so the digest length is 32 bytes. Every case below uses the override from sub-task 1b (5 KiB parts) through the shared helper, so no case needs 5 MiB parts:

  | Parts | Blob size | Expected tier |
  |---|---|---|
  | 8 | 256 bytes | 1, embedded |
  | 100 | 3200 bytes | 2, Child-D |
  | 256 | 8192 bytes | 3, data-tier file |

  Each case checks the tier marker, the combined checksum, and every part's checksum read back. These uploads use equal-sized parts, so the manifest is tiny and stays embedded; the 8-part case should still be confirmed against the actual `:O:` value size.
- [x] 10,000-part test (the maximum): 320,000 bytes of SHA256 digests in tier 3. It checks the combined checksum, paginated reads of the whole list, and a ranged read in the middle of the file.

**Relevant Context:**
- `backend/src/multipart_value.hpp`: `MHeadValue` (108), `MPartValue` (195), `kMaxMultipartOValueBytes` (47), `part_filename`.
- `backend/src/object_value.hpp`: `ObjectValueHeader` (178), `ObjectValue` flags (282), `write_object_value` / `parse_object_value`.
- `backend/src/constants.hpp`: `kMaxOValueBytes` (79), `kDefaultMaxKvStore` (54), child types (70-72).
- `backend/src/keys.hpp` / `keys.cpp:517-531`: `make_ct_key`, `make_c_prefix` (the `S:C` child keys). `parse_d_key` (`keys.cpp:419`) parses 31-byte data D keys only and is not changed.
- `backend/src/gc_worker.cpp:98` (`process_gm_entry`, FREE_PARTS), `:308-436` (G:O entries; the `CHUNK_MULTIPART_REF` ref_tag is the shared one).
- `backend/src/service_impl_multipart.cpp:1486-1519`: where the manifest is encoded and the `:O:` value written at Complete.
- `backend/src/object_value.hpp:279-280, 414, 544`, `object_value.cpp:196`: the manifest as the last tail segment and its readers.
- `backend/src/service_impl.cpp`: `select_storage_tier` (2406) for the data-tier write pattern (its orphan record removes `ref_tag.filename()`, so it cannot track `<ref_tag>_checksum` as is); `move_object_to_g` (1331); `delete_object_version` (about 3024-3185); `copy_object` (about 3869-4010).
- `plans/multipart_test_fixes.md`: `CHUNK_MULTIPART_REF` copies share the part files. In this plan a copy owns its `M` and `S` children and shares only the tier 3 file, which is freed by the last-reference drop through `GM_MODE_FREE_PARTS_CKSUM`.

**Status:** `[x] done`

---

## Sub-task 3: CreateMultipart and UploadPart

**Intent:** accept the algorithm, compute and verify part checksums, and return them.

**Expected Outcomes:**
- `CreateMultipartUpload` accepts `ChecksumAlgorithm` and `ChecksumType` and echoes `ChecksumAlgorithm`.
- The default type is COMPOSITE when none is given. FULL_OBJECT is accepted only for the CRC algorithms. Other combinations return an error.
- `UploadPart` computes the digest, rejects a mismatch with the client header (`BadDigest`), stores the digest, and returns `x-amz-checksum-<algo>`. The check runs before the part file is written (`data_store_.write`, `service_impl_multipart.cpp:644`), so a rejected part leaves no file and no row.
- A part resend replaces the stored checksum together with the ETag generation, so a stale checksum is never used.

**Todo List:**
- [x] Extend `kvrgw_initiate_multipart_upload` and `kvrgw_upload_part` in `backend/include/kvrgw_c_api.h` (359, 367) and in `backend/src/kvrgw_c_api.cpp`.
- [x] Extend the `kvrgwc` Go binding.
- [x] Update `KvRgwBackend.CreateMultipartUpload` and `UploadPart` (`frontend/backend_kvrgw.go`), including the response structs.
- [x] Check that the controller response path for `CreateMultipartUpload` (`object-post.go:251-257`) and `UploadPart` (`object-put.go:319-328`) exposes `ChecksumAlgorithm`, and patch the vendored copy if it does not. Record any patch next to the existing local patches. (No patch needed: the controller echoes `x-amz-checksum-algorithm` from the request headers and writes the `UploadPart` checksum headers from the returned `s3.UploadPartOutput`.)
- [x] Add the new error code for `BadDigest`, mapped in `mapErrorCode`.
- [x] Backend unit tests, then run SHA256 part-level assertions from `test_multipart_checksum_sha256`.

**Relevant Context:**
- `backend/src/service_impl_multipart.cpp`: `upload_part` (569), where the part MD5 is computed (624).
- `frontend/backend_kvrgw.go:930-977`.
- `frontend/kvrgwc/kvrgwc.go`: existing error-code table (36).

**Status:** `[x] done`

---

## Sub-task 4: CompleteMultipartUpload

**Intent:** verify, compute and persist the final checksum, and return it.

**Expected Outcomes:**
- Reject with `BadDigest` when:
  - the client's whole-object checksum does not match the computed value (`ChecksumSHA256='bad'`);
  - a part's checksum is missing from the Complete body when the upload has an algorithm;
  - a part's checksum differs from the stored one.
- After a rejected Complete, the upload stays active and can be completed again.
- COMPOSITE: `base64(hash(concat(raw part digests)))-N`.
- FULL_OBJECT: CRC combine over the parts in order, using the part sizes.
- The result returns `ChecksumXXX` and `ChecksumType`.
- The final checksum is persisted in the object value, and the per-part blob in its tier (sub-task 2). The `S` child is written in the same transaction as the `:O:`. The tier 3 file is written after verification passes and just before the commit, and removed if the commit fails (Decisions item 7, failure paths).
- All verification (per-part and whole-object) runs before anything is written, and every rejection takes the existing `IN_TRANSIENT` to `ACTIVE` revert (`service_impl_multipart.cpp:1462`).
- At 10,000 parts, the blob is 320,000 bytes for SHA256. Complete is a multi-transaction operation at that size, so the digests are collected while the part rows are scanned and kept in memory until the final transaction.
- The version-id handling stays as in `plans/multipart_test_fixes.md`.

**Todo List:**
- [x] Extend `CompletedPart` (C API and Go) with the per-part checksum.
- [x] Extend `kvrgw_complete_multipart_upload` with the client's whole-object checksum and type.
- [x] In `complete_multipart_upload_step3`, add verification and computation before the commit.
- [x] Make sure the failed-verification path leaves the head record in ACTIVE (the same revert as the missing-part / wrong-ETag path), including a `BadDigest` found after the scan.
- [x] Complete retried with `ChecksumType` or a whole-object checksum that differs from the upload's algorithm returns `BadDigest` or `InvalidRequest` as the s3-tests expect (check the expected code in `test_s3.py` near 15685-15863 before choosing).
- [x] Update `KvRgwBackend.CompleteMultipartUpload` (`frontend/backend_kvrgw.go:979`) and the `CompleteMultipartUploadResult` fields.
- [x] Unit tests: bad whole-object checksum, missing part checksum, retry after rejection, all five algorithms.

**Relevant Context:**
- `backend/src/service_impl_multipart.cpp`: Complete scan around 1137-1217, `complete_multipart_upload_step3` (1394), the revert at 1462, `complete_multipart_upload` (1546).
- `frontend/third_party/versitygw/s3api/controllers/object-post.go:269-414`.
- `frontend/third_party/versitygw/s3response/s3response.go:455` (`CompleteMultipartUploadResult`).

**Status:** `[x] done`

---

## Sub-task 5: Read paths (Head, Get, GetObjectAttributes)

**Intent:** return the stored checksums where S3 does.

**Expected Outcomes:**
- `HeadObject` and `GetObject` return `ChecksumXXX` and `ChecksumType` only when `x-amz-checksum-mode: ENABLED` is set. Without it, no checksum header is returned.
- `GetObject` with `PartNumber=N` returns that part's checksum and the type.
- `GetObjectAttributes` returns `Checksum{ChecksumType, ChecksumXXX}` from the `:O:` value alone (the combined checksum), without touching the blob.
- With `ObjectParts`, each returned `Parts[i]` carries its checksum, and `MaxParts` / `PartNumberMarker` / `IsTruncated` / `NextPartNumberMarker` paginate over the manifest's included parts. Only the checksum slice for the requested page is read.
- Objects without a checksum, and non-multipart objects, return no checksum.
- The blob is loaded only by the operations that need per-part checksums (`PartNumber` requests and GetObjectAttributes with `ObjectParts`), only from its own tier, and only the needed slice. See "Extra storage access".

**Todo List:**
- [x] Add the checksum to the object metadata struct filled by `fill_object_meta` (`backend/src/kvrgw_c_api.cpp`).
- [x] Add a blob loader that takes (first_rank, count) and follows the tier marker: slice of the embedded blob, a KV get of the `K` child-D then slice, or a ranged `data_store_.read`. It resolves the ref_tag through the REF indirection for copied objects.
- [x] In `get_object_attributes`, fill `Checksum` from the `:O:` value, and fill each page entry's checksum through the loader, using the same rank computation as the page's part list.
- [x] Return the part's checksum from the blob in the `PartNumber` paths of `get_object` and `head_object` (`backend/src/service_impl.cpp`); the entry index is the rank of the part in the manifest's included parts.
- [x] Update `KvRgwBackend.GetObject` (298) and `HeadObject` (372), gated on `ChecksumMode`.
- [x] Extend `get_object_attributes`, which is planned in `plans/multipart_test_fixes.md` (Bug/Gap 3), to carry the checksum. If that plan is not yet implemented, implement the checksum part together with it, and note it in both plans.
- [x] Patch the vendored `s3response.ObjectPart` if it lacks a per-part checksum field.
- [x] Unit tests for each path, including pagination in each tier (8, 100, 256 and 10,000 parts, all using the override helper from sub-task 1b): no marker, mid-list marker, past-end marker, `MaxParts` smaller and larger than the part count, and a last page that is shorter than `MaxParts`.

**Relevant Context:**
- `plans/multipart_test_fixes.md`, section "Bug/Gap 3".
- `frontend/third_party/versitygw/s3api/controllers/object-get.go:397-581`, `object-head.go:123-202`.
- `frontend/third_party/versitygw/s3response/s3response.go:124-137`.

**Status:** `[x] done`

---

## Sub-task 6: Verification and docs

**Intent:** confirm the 7 in-scope tests pass and nothing regressed.

**Expected Outcomes:**
- `./scripts/run_ceph_rgw_tests.sh -c multipart-other` shows the 7 in-scope checksum tests passing. `test_multipart_reupload_checksum_and_etag` still fails (Decisions, item 4).
- The default run (`./scripts/run_ceph_rgw_tests.sh`) has no regressions.
- The passing checksum tests are moved out of `multipart-other` into `multipart` in `scripts/run_ceph_rgw_tests.sh`.
- `docs/ceph_rgw_tests.md` is updated, including the counts and reasons for the checksum rows.

**Todo List:**
- [x] Rebuild the backend and frontend and restart the frontend.
- [x] Run the 7 in-scope tests, then `-c multipart-other`, then the default suite.
- [x] The s3-tests only exercise tier 1 (1, 3 and 13 parts) and never set the override. Tiers 2 and 3 are covered by backend unit tests that use the override: 8 parts (tier 1), 100 parts (tier 2), 256 parts (tier 3) and 10,000 parts, plus the delete, GC and copy cases for each tier.
- [x] Update the script's category lists and the comments above them.
- [x] Update `docs/ceph_rgw_tests.md` (the "Multipart-other" section).

**Relevant Context:**
- `scripts/run_ceph_rgw_tests.sh:112, 126`.
- `docs/ceph_rgw_tests.md:252-274`.
- `.logs/s3tests_mpo2.log`: the failure baseline (27 failed).

**Status:** `[x] done`

---

## Red-team findings (resolved in this plan)

Review of the plan against the code. Each item names the flaw, the evidence and where the plan now handles it.

1. **GC cannot clear a copy's children.** A `CHUNK_MULTIPART_REF` G:O entry carries the original's shared ref_tag (`service_impl.cpp:1362-1375`), so the earlier "GC clears the `S:C` prefix" design would clear the wrong prefix. Children are now cleared at delete time with the object's own ref_tag; GC is unchanged for `M` and `S` (Decisions item 7, Delete all).
2. **`delete_object_version` is a second delete path** that bypasses `move_object_to_g`, `S:C` cleanup and the GC hand-off (`service_impl.cpp:3095-3126`, `3161-3179`). Rule from review: multipart is never deleted inline and always goes to the GC domain. The function now hands multipart entries to the GC hand-off step of a split `move_object_to_g`, which also closes the existing part-file leak without a key-delete ordering dependency when a version is promoted. Todo and tests are in sub-task 2.
3. **The tier 3 file had no cleanup path.** Neither `G:M{FREE_PARTS}` nor the R: last-reference path knows `<ref_tag>_checksum`. Added `GM_MODE_FREE_PARTS_CKSUM` and the `kFlagChecksumFile` flag.
4. **The tier 3 orphan record does not fit.** The `P:O` record used by `select_storage_tier` removes `ref_tag.filename()` (`service_impl.cpp:2438-2451`). The plan now writes the file after verification and removes it on a failed commit. The crash window is an accepted POC risk, stated in Decisions item 7.
5. **Redundant state (`children_mask` plus tier marker) could disagree.** Replaced by one `placement` byte.
6. **`copy_object` read `src.manifest_body`, which is empty for an `M` child**, including for the `R:` record (`service_impl.cpp:3996`). Copy now goes through the loaders and re-runs placement for the new value.
7. **Child value limit.** The earlier wording said 4096 for the blob "below" the limit; the tier boundary is now "at or under", matching the boundary tests.
8. **Verification before write.** A part with a bad client digest must not leave a part file; Complete must verify everything before the tier 3 write (sub-tasks 3 and 4).

## Open risks (not resolved by this plan)

- RESOLVED: the largest manifest measured is 30,017 bytes (10,000 parts, all sizes distinct), accepted as an `M` child (`multipart_children_test`).
- `rgw_cksum` reuse is unchecked (sub-task 1). If it cannot be linked, CRC64NVME and the CRC combine are new code and need the s3-test vectors as their only oracle.
- Splitting `move_object_to_g` changes a shared function used by overwrite and delete. The existing multipart and GC tests (`multipart_gc_test.cpp`, `multipart_ref_copy_test.cpp`) must pass unchanged, and the test for deleting a current multipart version while a non-current version exists must show the promoted value intact.
- Tier 3 crash window leaves one orphan `<ref_tag>_checksum` file.

## Implementation notes (outcome)

- Verified: all 7 in-scope s3-tests pass; `test_multipart_reupload_checksum_and_etag` fails by design; default suite 295 passed.
- Behavior matched to RGW/s3-tests: a part listed in Complete without a checksum is accepted (only a listed, mismatching one gives `BadDigest`); a malformed whole-object checksum header gives `BadDigest` (vendored `ParseCompleteMpChecksumHeaders`, marked "kvrgw local patch"); `GetObject` with `PartNumber` returns that part's checksum and `ChecksumType` without `ChecksumMode`.
- Manifest cost is about 3 bytes per irregular part, so about 500 irregular parts are needed to exceed the 1 KB `:O:` limit; tests use 500.
- Tests: `checksum_test`, `multipart_cksum_test` (algorithms, Complete vectors, read paths and `GetObjectAttributes` pagination in every tier up to 10,000 parts), `multipart_children_test` (placement matrix, every delete path, copy in both deletion orders, 10,000 parts and GC). The latter was reworked per `plans/refactor_multipart_tests.md`: position-tagged data, random upload order, and full verification of every read; `KVRGW_TEST_SEED` replays a run.
