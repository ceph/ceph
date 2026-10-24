# CopyObject Implementation — Remaining Work

## Status

- Phase A (chunk type refactor): DONE — commit `c3d53a8`
- Phase B (ref-counting infra): DONE — commit `60f55d8`
- Phase C proto: DONE — commit `6056c27`
- Phase C handler + frontend: **TODO** (this document)

## What's Already in Place

- `CHUNK_CHILD_D_REF` and `CHUNK_STORAGE_REF` types defined, parse/write logic ready
- `kFlagSharedData` (0x04) bit and `has_shared_data()` helper
- `make_r_key(ref_tag)` — global R: namespace
- GC worker: ref-count aware deletion for both D: and Storage tiers (needs update for new encoding)
- Proto `CopyObject` RPC defined with all fields
- 160 ceph-rgw tests passing

## Ref-Count Encoding Changes (align with source of truth)

### R: Value Layout

```
[ref_count: uint64_be (8 bytes)] [chunk_descriptor: variable]
```

- Always uint64. No practical limit.
- Chunk descriptor cached for GC worker (so GC can free storage-tier data without reading O:).
- Created on first copy with count=2.

**Impact:** GC worker (`gc_worker.cpp` lines 109–131) currently reads uint16. Must update to read uint64 and preserve trailing chunk descriptor.

### D: Value Layout (flags-past-data model)

```
Non-shared:  [data bytes]                              len == object.data_size
Shared:      [data bytes] [flags: 1 byte] [ref_count]  len > object.data_size
```

Flags byte (only present when `len(D: value) > object.data_size`):

| shared bit (0x01) | large bit (0x02) | ref_count size |
|---|---|---|
| 1 | 0 | 2 bytes (uint16_be) |
| 1 | 1 | 8 bytes (uint64_be) |

- Non-shared D: entries have zero ref_count overhead.
- First copy: append flags=0x01 + uint16_be(2) → +3 bytes.
- Overflow past 65535: rewrite with flags=0x03 + uint64_be → +9 bytes.

**Impact:** GC worker (`gc_worker.cpp` lines 148–171) currently reads uint16 prefix. Must update to detect shared via flags-past-data. Phase B's "reads skip 2 bytes" approach is replaced — reads just read `data_size` bytes from offset 0.

### Migration from Phase B Encoding

Phase B wrote D: values with `[uint16_be ref_count] [data]` (ref_count=1 for all entries). This must be removed:
- Normal PUT should write D: as `[data]` only (no prefix).
- Existing entries from Phase B with ref_count=1 prefix need handling — GC/reads already know `data_size`, so they can detect old-format entries if `len == data_size + 2` and first 2 bytes == `htons(1)`. However, for POC scope, a clean restart with `--clean` is acceptable.

## Remaining Steps

### 1. Update ref-count helpers

Add to `object_value.hpp` or new `ref_count.hpp`:

```cpp
// R: value read/write
struct RValue {
  uint64_t ref_count;
  std::string chunk_descriptor;
};
RValue parse_r_value(std::string_view raw);
std::string write_r_value(uint64_t count, std::string_view chunk_descriptor);

// D: ref_count read/write (flags-past-data model)
struct DRefInfo {
  bool shared;
  uint64_t ref_count;  // 0 if not shared
};
DRefInfo read_d_ref_count(std::string_view d_value, uint64_t data_size);
std::string write_d_with_ref(std::string_view data, uint64_t new_count);
```

### 2. Update GC worker for new encoding

- R: path: read uint64_be (8 bytes), preserve chunk_descriptor suffix.
- D: path: use `read_d_ref_count(d_value, object_size)` instead of uint16 prefix.
- Remove old 2-byte prefix logic.

### 3. Update PutObject D: write path

Remove the uint16_be ref_count=1 prefix. D: value = raw data only.

### 4. Add CopyObject declaration to service_impl.hpp

```cpp
grpc::Status CopyObject(
    grpc::ServerContext* context,
    const kvrgw::v1::CopyObjectRequest* request,
    kvrgw::v1::CopyObjectResponse* response) override;
```

### 5. Implement C++ CopyObject handler in service_impl.cpp

Single transaction:

```
CopyObject(request):
  resolve tenant_id
  get src bucket_id (get_bucket_id_cached)
  get dst bucket_id (get_bucket_id_cached or same bucket)

  txn {
    verify_bucket_in_txn(dst) → get versioning_state

    // Read source
    if src_version_id specified:
      try src O: first (if vid matches), else read V:<src_vid>
    else:
      read src O:
    if not found or DM → 404

    // Source preconditions (if_match / if_none_match)
    check_preconditions(src, request->if_match(), request->if_none_match())

    // Read destination O: (for conditionals + displacement)
    read dst O:
    // Destination preconditions (if set)
    if dst_if_match or dst_if_none_match:
      check_preconditions(dst, request->dst_if_match(), request->dst_if_none_match())

    // Metadata-only self-copy optimization
    if same_bucket && same_key && !src_version_id && replace_metadata:
      if unversioned or (suspended && dst has NULL_VERSION):
        // In-place metadata update — no displacement, no ref_count
        put(dst O:, same_chunk_descriptor + new_metadata)
        commit; return
      // else: versioned or suspended-with-real-vid → fall through to full copy path

    // Build new value
    new_value.ref_tag = fresh ref_tag (ref_tags_.next())  // own identity for children
    new_value.etag = src.etag (copy preserves etag)
    new_value.size = src.size
    new_value.last_modified = now
    new_value.content_type = replace_metadata ? request->content_type() : src.content_type

    // Data sharing by tier
    switch (src.chunk.type):
      CHUNK_INLINE:
        new_value.chunk.type = CHUNK_INLINE
        new_value.inline_data = src.inline_data

      CHUNK_CHILD_D or CHUNK_CHILD_D_REF:
        // Resolve D: location
        src_d_bucket = (CHILD_D ? src_bucket_id : src.chunk_data_bucket_id)
        src_d_ref = (CHILD_D ? src.ref_tag : src.chunk_data_ref_tag)
        d_key = make_d_key(src_d_bucket, d_size_tier(src.size), src_d_ref, src.last_modified_sec)
        // Increment ref_count (flags-past-data model)
        d_raw = get(d_key)
        d_ref = read_d_ref_count(d_raw, src.size)
        if !d_ref.shared:
          // First copy — append flags + ref_count=2
          put(d_key, write_d_with_ref(d_raw, 2))
        else:
          // Subsequent copy — increment
          put(d_key, write_d_with_ref(d_raw.substr(0, src.size), d_ref.ref_count + 1))
        // Set shared-data on source (rewrite src O:/V: with updated flags)
        if !(src.flags & kFlagSharedData):
          src.flags |= kFlagSharedData
          rewrite src entry
        // Target is CHILD_D_REF
        new_value.chunk.type = CHUNK_CHILD_D_REF
        new_value.chunk_data_bucket_id = src_d_bucket
        new_value.chunk_data_ref_tag = src_d_ref
        new_value.flags |= kFlagSharedData

      CHUNK_STORAGE or CHUNK_STORAGE_REF:
        src_data_ref = (STORAGE ? src.ref_tag : src.chunk_data_ref_tag)
        if src.has_shared_data() (STORAGE_REF or shared STORAGE):
          // R: already exists — increment
          r_key = make_r_key(src_data_ref)
          r_raw = get(r_key)
          r_val = parse_r_value(r_raw)
          put(r_key, write_r_value(r_val.ref_count + 1, r_val.chunk_descriptor))
        else:
          // First copy — create R: with count=2 + chunk_descriptor
          r_key = make_r_key(src_data_ref)
          chunk_desc = serialize_chunk_descriptor(src)
          put(r_key, write_r_value(2, chunk_desc))
        // Set shared-data on source
        if !(src.flags & kFlagSharedData):
          src.flags |= kFlagSharedData
          rewrite src entry
        // Target is STORAGE_REF
        new_value.chunk.type = CHUNK_STORAGE_REF
        memcpy(new_value.chunk_data_ref_tag, src_data_ref, 12)
        new_value.flags |= kFlagSharedData

    // Versioning (destination)
    if dst O: exists:
      displace_old_object(tr, dst_versioning_state, dst_object_key, *dst_parsed)
    ids = compute_new_version(dst_versioning_state, dst_parsed ? &*dst_parsed : nullptr)
    new_value.version_id = ids.version_id
    new_value.next_vid = ids.next_vid

    // Write destination O:
    write_object_value(buf, new_value)
    put(dst_object_key, buf)

    commit
  }

  // Response
  response->set_etag(new_value.etag_display())
  response->set_last_modified_unix(new_value.last_modified_sec)
  if dst_versioning == ENABLED:
    response->set_version_id(to_string(new_value.version_id))
  response->set_copy_source_version_id(to_string(src.version_id))
```

### 6. Implement Go CopyObject in backend_kvrgw.go

```go
func (b *KvRgwBackend) CopyObject(ctx context.Context, input s3response.CopyObjectInput) (s3response.CopyObjectOutput, error) {
    srcBucket, srcKey, srcVersionId := parseCopySource(deref(input.CopySource))

    req := &pb.CopyObjectRequest{
        TenantName:    b.tenantName(),
        SrcBucketName: srcBucket,
        SrcKey:        srcKey,
        DstBucketName: deref(input.Bucket),
        DstKey:        deref(input.Key),
    }
    if srcVersionId != "" {
        req.SrcVersionId = versionIDToInternal(srcVersionId)
    }
    if input.CopySourceIfMatch != nil {
        req.IfMatch = strings.Trim(*input.CopySourceIfMatch, `"`)
    }
    if input.CopySourceIfNoneMatch != nil {
        req.IfNoneMatch = strings.Trim(*input.CopySourceIfNoneMatch, `"`)
    }
    if input.MetadataDirective == types.MetadataDirectiveReplace {
        req.ReplaceMetadata = true
        if input.ContentType != nil {
            req.ContentType = *input.ContentType
        }
    }

    resp, err := b.client.CopyObject(ctx, req)
    if err != nil {
        return s3response.CopyObjectOutput{}, mapGrpcErr(err)
    }

    lastMod := time.Unix(resp.GetLastModifiedUnix(), 0).UTC()
    etag := fmt.Sprintf(`"%s"`, resp.GetEtag())
    result := s3response.CopyObjectOutput{
        CopyObjectResult: &s3response.CopyObjectResult{
            ETag:         &etag,
            LastModified: &lastMod,
        },
    }
    if vid := resp.GetVersionId(); vid != "" {
        extVid := versionIDFromProto(vid)
        result.VersionId = &extVid
    }
    if svid := resp.GetCopySourceVersionId(); svid != "" {
        extSvid := versionIDFromProto(svid)
        result.CopySourceVersionId = &extSvid
    }
    return result, nil
}
```

### 7. Build and Test

```bash
cd /home/gbenhano/kv_poc
cd frontend && PATH=$PATH:~/go/bin protoc --go_out=pb --go_opt=paths=source_relative \
  --go-grpc_out=pb --go-grpc_opt=paths=source_relative -I ../proto ../proto/kvrgw.proto
go build -o ../build/kv-rgw-frontend .
cd ..
cmake --build build -j$(nproc)
pkill -f kv-rgw-frontend; pkill -f kv-rgw-backend; pkill nginx; sleep 5
bash scripts/reload.sh --clean 1

# Test copy operations
cd ~/clean/ceph/src/test/rgw/s3-tests
S3TEST_CONF=/home/gbenhano/kv_poc/s3tests.conf python3.11 -m pytest \
  s3tests/functional/test_s3.py -k "test_object_copy" --tb=short -q

# Verify no regressions
cd /home/gbenhano/kv_poc && bash scripts/run_ceph_rgw_tests.sh
```

## Key Design Decisions (reference)

- **R: ref_count** — uint64_be (8 bytes). No overflow possible.
- **R: value** — uint64_be count + chunk_descriptor (cached for GC to free data without reading O:).
- **D: ref_count** — flags-past-data: if `len > data_size`, trailing bytes = flags(1B) + ref_count(2B or 8B). Zero overhead for non-shared entries.
- **D: flags byte** — bit 0 (0x01) = shared, bit 1 (0x02) = large (uint64 ref_count).
- Source stays CHUNK_CHILD_D/STORAGE (type unchanged), only shared-data BIT is set on source.
- Target always gets _REF type (CHILD_D_REF or STORAGE_REF).
- R: is global (not bucket-scoped): `R <ref_tag 12B>`.
- Everything in a single transaction (no P: for copy).
- Cross-shard copy: not implemented (always same shard in POC).
- Metadata-only self-copy (non-versioned / suspended-NULL): in-place update, no ref_count.

## Design Docs

- [copy-object.md](../copy-object.md) — full design (source of truth)
- [conditional-commands.md](../conditional-commands.md) — precondition logic
