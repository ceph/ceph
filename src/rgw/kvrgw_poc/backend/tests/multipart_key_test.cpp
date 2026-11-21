// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@ibm.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "constants.hpp"
#include "etag.hpp"
#include "keys.hpp"
#include "multipart_value.hpp"

#include <cassert>
#include <cstring>
#include <iostream>
#include <openssl/evp.h>

using namespace kvrgw;

namespace {

bucket_id_t test_bucket_id(uint64_t v)
{
  uint8_t be[sizeof(uint64_t)];
  uint64_t net = htobe64(v);
  std::memcpy(be, &net, sizeof(net));
  bucket_id_t b;
  b.load(be);
  return b;
}

RefTag make_ref_tag(uint8_t fill)
{
  uint8_t raw[kRefTagSize];
  std::memset(raw, fill, sizeof(raw));
  RefTag rt;
  rt.load(raw);
  return rt;
}

// S:M head key — object_name + ref_tag only, no part_number at all (there is
// no such thing as "part 0" — see keys.hpp).
void test_m_head_key()
{
  const bucket_id_t bucket_id = test_bucket_id(42);
  const std::string object_name = "a/very/long/object-name.bin";
  const RefTag ref_tag = make_ref_tag(0xAB);
  const std::string_view rt_view = ref_tag.view();

  KeyBuf head_key;
  make_m_head_key(bucket_id, object_name, rt_view, head_key);

  // Lives under the bucket-wide head prefix.
  KeyBuf bprefix;
  make_m_bucket_prefix(bucket_id, bprefix);
  assert(head_key.view().substr(0, bprefix.len) == bprefix.view());

  auto parsed = parse_m_head_key_view(head_key.view());
  assert(parsed);
  assert(parsed->bucket_id == bucket_id);
  assert(parsed->object_name == object_name);
  assert(parsed->ref_tag == ref_tag);

  // All-zero ref_tag (RefTag is a sequential rgw_id+seq_id counter, so its
  // leading bytes are frequently 0x00 in practice) must still round-trip --
  // the NUL delimiter, not tail-length guessing, locates the boundary.
  const RefTag zero_ref_tag = make_ref_tag(0x00);
  KeyBuf head_key2;
  make_m_head_key(bucket_id, object_name, zero_ref_tag.view(), head_key2);
  auto parsed2 = parse_m_head_key_view(head_key2.view());
  assert(parsed2);
  assert(parsed2->object_name == object_name);
  assert(parsed2->ref_tag == zero_ref_tag);

  std::cout << "test_m_head_key OK\n";
}

// S:U part key — ref_tag + part_number + generation only, no object_name at
// all (ref_tag already globally/uniquely identifies the upload). Fixed-size,
// lives in its own category, entirely separate from S:M.
void test_u_part_key()
{
  const bucket_id_t bucket_id = test_bucket_id(42);
  const RefTag ref_tag = make_ref_tag(0xAB);
  const std::string_view rt_view = ref_tag.view();

  KeyBuf part_key0;
  make_u_part_key(bucket_id, rt_view, /*part_number=*/5, /*generation=*/0, part_key0);
  auto parsed0 = parse_u_key(part_key0.view());
  assert(parsed0);
  assert(parsed0->bucket_id == bucket_id);
  assert(parsed0->ref_tag == ref_tag);
  assert(parsed0->part_number == 5);
  assert(parsed0->generation == 0);

  KeyBuf part_key1;
  make_u_part_key(bucket_id, rt_view, /*part_number=*/5, /*generation=*/3, part_key1);
  auto parsed1 = parse_u_key(part_key1.view());
  assert(parsed1);
  assert(parsed1->part_number == 5);
  assert(parsed1->generation == 3);

  // Single-part-all-generations prefix scopes exactly the two generations
  // of part 5, not other parts.
  KeyBuf part_prefix;
  make_u_part_prefix(bucket_id, rt_view, 5, part_prefix);
  assert(part_key0.view().substr(0, part_prefix.len) == part_prefix.view());
  assert(part_key1.view().substr(0, part_prefix.len) == part_prefix.view());

  KeyBuf other_part_key;
  make_u_part_key(bucket_id, rt_view, /*part_number=*/6, /*generation=*/0, other_part_key);
  assert(other_part_key.view().substr(0, part_prefix.len) != part_prefix.view());

  // Whole-upload parts prefix covers every part_number/generation for this
  // ref_tag -- does NOT include the head (different category entirely).
  KeyBuf u_prefix;
  make_u_prefix(bucket_id, rt_view, u_prefix);
  assert(part_key0.view().substr(0, u_prefix.len) == u_prefix.view());
  assert(other_part_key.view().substr(0, u_prefix.len) == u_prefix.view());
  KeyBuf head_key;
  make_m_head_key(bucket_id, "some/object", rt_view, head_key);
  assert(head_key.view().substr(0, u_prefix.len) != u_prefix.view());

  // S:M (head) and S:U (parts) never collide even with an all-zero ref_tag
  // -- they're simply different category bytes, no shared-prefix ambiguity
  // possible (unlike the old single-category design).
  const RefTag zero_ref_tag = make_ref_tag(0x00);
  KeyBuf zero_part_key;
  make_u_part_key(bucket_id, zero_ref_tag.view(), /*part_number=*/2,
                  /*generation=*/0, zero_part_key);
  assert(!parse_m_head_key_view(zero_part_key.view()));

  std::cout << "test_u_part_key OK\n";
}

void test_gm_key()
{
  const bucket_id_t bucket_id = test_bucket_id(7);
  const RefTag ref_tag = make_ref_tag(0x11);

  KeyBuf gm_key;
  make_gm_key(bucket_id, ref_tag.view(), gm_key);
  auto parsed = parse_gm_key(gm_key.view());
  assert(parsed);
  assert(parsed->bucket_id == bucket_id);
  assert(parsed->ref_tag == ref_tag);

  std::cout << "test_gm_key OK\n";
}

// G:U key — unaffected by the head/part split (already ref_tag-only).
void test_gu_key()
{
  const bucket_id_t bucket_id = test_bucket_id(9);
  const RefTag ref_tag = make_ref_tag(0x22);

  KeyBuf gu_key;
  make_gu_key(bucket_id, ref_tag.view(), /*part_number=*/17, /*generation=*/2, gu_key);
  auto parsed = parse_gu_key(gu_key.view());
  assert(parsed);
  assert(parsed->bucket_id == bucket_id);
  assert(parsed->ref_tag == ref_tag);
  assert(parsed->part_number == 17);
  assert(parsed->generation == 2);

  std::cout << "test_gu_key OK\n";
}

// G:U value — unified, always a group of 1-20 entries. A single re-upload
// obsoletion writes count=1; CompleteMultipartUpload's batched
// excluded-parts cleanup writes count up to kMaxBatchSize. Both share the
// same wire format now (no more separate single-entry vs. "Group G:U").
void test_gu_value_group()
{
  // count == 1 (re-upload obsoletion case).
  {
    const GuEntry entry{/*part_number=*/5, /*generation=*/2};
    GuValueBuf buf;
    make_gu_value(buf, &entry, 1, /*sum_size=*/123456);
    auto val = parse_gu_value(buf.view());
    assert(val);
    assert(val->count == 1);
    assert(val->sum_size == 123456);
    assert(val->entries[0].part_number == 5 && val->entries[0].generation == 2);
  }

  // count > 1 (CompleteMultipartUpload excluded-parts batch case).
  {
    const GuEntry entries[3] = {{1, 0}, {2, 1}, {3, 0}};
    GuValueBuf buf;
    make_gu_value(buf, entries, 3, /*sum_size=*/999);
    auto val = parse_gu_value(buf.view());
    assert(val);
    assert(val->count == 3);
    assert(val->sum_size == 999);
    assert(val->entries[0].part_number == 1 && val->entries[0].generation == 0);
    assert(val->entries[1].part_number == 2 && val->entries[1].generation == 1);
    assert(val->entries[2].part_number == 3 && val->entries[2].generation == 0);
  }

  std::cout << "test_gu_value_group OK\n";
}

void test_m_head_value()
{
  std::string_view color_key("color");
  std::string_view animal_key("animal");
  const kvrgw::MetaPair meta[] = {{color_key, "blue"}, {animal_key, "dog"}};
  MHeadValueBuf buf;
  const bool ok = make_m_head_value(buf, 1234567, "text/plain", /*tags_count=*/2,
                                    std::span<const kvrgw::MetaPair>(meta, std::size(meta)));
  assert(ok);
  auto parsed = parse_m_head_value(buf.view());
  assert(parsed);
  assert(parsed->hdr.initiated_unix_ms == 1234567);
  assert(parsed->content_type == "text/plain");
  assert(parsed->hdr.tags_count == 2);
  assert(parsed->hdr.metadata_count == static_cast<uint16_t>(std::size(meta)));
  MHeadValueBuf buf2;
  const bool ok2 = make_m_head_value(buf2, 1234567000, "text/plain", 0, {});
  assert(ok2);
  auto parsed2 = parse_m_head_value(buf2.view());
  assert(parsed2);
  assert(parsed2->hdr.initiated_unix_ms == 1234567000);
  assert(parsed2->content_type == "text/plain");
  assert(parsed2->hdr.tags_count == 0);
  assert(parsed2->hdr.metadata_count == 0);
  std::cout << "test_m_head_value OK\n";
}

void test_m_part_value()
{
  const bucket_id_t bucket_id = test_bucket_id(5);
  const RefTag ref_tag = make_ref_tag(0x44);
  ChunkDescriptor chunk(CHUNK_STORAGE, ref_tag.view().data(), bucket_id);

  MPartValue v;
  v.state = M_PART_COMMITTED;
  v.chunk = chunk;
  ETag etag;
  uint8_t digest[16];
  std::memset(digest, 0x55, sizeof(digest));
  etag.load_raw(digest, 0);
  v.etag = etag;
  v.size = 123456;
  v.mtime = 987654;

  uint8_t raw[sizeof(MPartValue)];
  make_m_part_value(v, raw);
  auto parsed = parse_m_part_value(
      std::string_view(reinterpret_cast<const char*>(raw), sizeof(raw)));
  assert(parsed);
  assert(parsed->state == M_PART_COMMITTED);
  assert(parsed->chunk.get_ref_tag() == ref_tag);
  assert(parsed->etag == etag);
  assert(parsed->size == 123456);
  assert(parsed->mtime == 987654);

  std::cout << "test_m_part_value OK\n";
}

// Builds a [header][body] value the way Complete does.
std::string make_gm_bytes(GmMode mode, uint32_t mtime, std::string_view body)
{
  std::string v(kGmHeaderSize, '\0');
  write_gm_header(reinterpret_cast<uint8_t*>(v.data()), mode, mtime);
  v.append(body);
  return v;
}

// A FREE_PARTS blob of exactly `total` bytes with a filler body (placement tests).
std::string mk_blob(size_t total, char fill = 'm')
{
  assert(total > kGmHeaderSize);
  return make_gm_bytes(GM_MODE_FREE_PARTS, 0, std::string(total - kGmHeaderSize, fill));
}

std::string encode_manifest(std::span<const uint16_t> included,
                            std::span<const uint16_t> generations,
                            std::span<const uint64_t> sizes, size_t* keep_len = nullptr)
{
  std::string out(manifest_body_bound(included.size()), '\0');
  const size_t len = encode_manifest_body(reinterpret_cast<uint8_t*>(out.data()), out.size(),
                                          ManifestInput{included, generations, sizes}, keep_len);
  assert(len > 0 && len <= out.size());
  out.resize(len);
  return out;
}

void test_gm_value()
{
  // Header layout: mode, 3 pad bytes, mtime big-endian.
  {
    uint8_t h[kGmHeaderSize];
    write_gm_header(h, GM_MODE_MIXED, 0x01020304);
    assert(h[0] == GM_MODE_MIXED && h[1] == 0 && h[2] == 0 && h[3] == 0);
    assert(h[4] == 1 && h[5] == 2 && h[6] == 3 && h[7] == 4);
  }

  // DEEP -- header only, no keep-list body.
  {
    const std::string v = make_gm_bytes(GM_MODE_DEEP, 111222, {});
    auto parsed = parse_gm_value(v);
    assert(parsed);
    assert(parsed->mode == GM_MODE_DEEP);
    assert(parsed->mtime == 111222);
    assert(parsed->body.empty());
  }

  // Malformed: too short, unknown mode (incl. the removed mode 3), empty body
  // for a mode that needs one.
  {
    assert(!parse_gm_value(std::string_view{}));
    assert(!parse_gm_value(std::string(kGmHeaderSize - 1, '\0')));
    assert(!parse_gm_value(make_gm_bytes(static_cast<GmMode>(3), 0, "x")));
    assert(!parse_gm_value(make_gm_bytes(static_cast<GmMode>(4), 0, "x")));
    assert(!parse_gm_value(make_gm_bytes(GM_MODE_MIXED, 0, {})));
    assert(!parse_gm_value(make_gm_bytes(GM_MODE_FREE_PARTS, 0, {})));
  }

  // MIXED -- carries the manifest's keep-list, in the compact encoding.
  {
    const uint16_t included[2] = {1, 3};
    const uint16_t generations[2] = {0, 2};
    const uint64_t sizes[2] = {100, 100};
    const std::string v =
        make_gm_bytes(GM_MODE_MIXED, 7, encode_manifest(included, generations, sizes));
    auto parsed = parse_gm_value(v);
    assert(parsed);
    assert(parsed->mode == GM_MODE_MIXED);
    assert(parsed->mtime == 7);
    assert(!parsed->body.empty());

    auto decoded = decode_manifest_body(parsed->body);
    assert(decoded);
    assert(decoded->included_parts == std::vector<uint16_t>({1, 3}));
    assert(decoded->generations == std::vector<uint16_t>({0, 2}));

    ManifestKeepCursor cursor(parsed->body);
    assert(cursor.valid());
    assert(cursor.is_kept(1, 0));
    assert(!cursor.is_kept(1, 1));
    assert(!cursor.is_kept(2, 0));  // not included
    assert(cursor.is_kept(3, 2));
    assert(!cursor.is_kept(3, 0));
  }

  std::cout << "test_gm_value OK\n";
}

// FREE_PARTS -- move_object_to_g's verbatim-copy target when a completed
// CHUNK_MULTIPART :O: is deleted/overwritten (no S:U rows left by then --
// the manifest itself is the only remaining record of which parts exist).
void test_gm_value_free_parts()
{
  const uint16_t included[2] = {1, 2};
  const uint16_t generations[2] = {0, 3};
  const uint64_t sizes[2] = {5 * 1024 * 1024, 1024};

  // The stored manifest blob is a ready-made FREE_PARTS value: it is what
  // :O: / S:C:M / R: carry and what GC reads under the G:M key unchanged.
  const std::string blob =
      make_gm_bytes(GM_MODE_FREE_PARTS, 0, encode_manifest(included, generations, sizes));

  auto parsed = parse_gm_value(blob);
  assert(parsed);
  assert(parsed->mode == GM_MODE_FREE_PARTS);
  assert(!parsed->body.empty());
  assert(gm_blob_body(blob) == parsed->body);
  assert(gm_blob_body(make_gm_bytes(GM_MODE_MIXED, 0, "x")).empty());
  assert(gm_blob_body("short").empty());

  auto decoded = decode_manifest_body(parsed->body);
  assert(decoded);
  assert(decoded->included_parts == std::vector<uint16_t>({1, 2}));
  assert(decoded->generations == std::vector<uint16_t>({0, 3}));
  assert(decoded->sizes == std::vector<uint64_t>({5 * 1024 * 1024, 1024}));

  std::cout << "test_gm_value_free_parts OK\n";
}

// part_filename() -- shared data-tier filename builder used by GcWorker
// (G:U, G:M{DEEP,MIXED,FREE_PARTS}) and GetObject's multipart read path.
void test_part_filename()
{
  const RefTag ref_tag = make_ref_tag(0xCD);
  const std::string fname = part_filename(ref_tag, 7, 2);
  assert(!fname.empty());
  assert(fname == ref_tag.filename() + "-7-2");

  std::cout << "test_part_filename OK\n";
}

// CHUNK_MULTIPART :O: value round-trip -- write_object_value/
// parse_object_value's manifest_blob tail segment (always last, no length
// prefix).
void test_chunk_multipart_object_value_roundtrip()
{
  const uint16_t included[3] = {1, 2, 5};
  const uint16_t generations[3] = {0, 1, 0};
  const uint64_t sizes[3] = {5 * 1024 * 1024, 6 * 1024 * 1024, 1024};
  const std::string blob =
      make_gm_bytes(GM_MODE_FREE_PARTS, 0, encode_manifest(included, generations, sizes));

  ObjectValue value;
  value.hdr.ref_tag = make_ref_tag(0xEF);
  value.hdr.size = sizes[0] + sizes[1] + sizes[2];
  value.hdr.chunk_type = CHUNK_MULTIPART;
  value.content_type = "application/octet-stream";
  value.set_self_chunk(kNullBucket);
  uint8_t md5[kMd5Bytes] = {9};
  value.set_etag_raw(md5, 3);
  value.manifest_loc = kManifestInline;
  value.manifest_blob = blob;

  OValueBuf buf;
  assert(write_object_value(buf, value));

  auto parsed = parse_object_value(buf.view());
  assert(parsed);
  assert(parsed->hdr.chunk_type == CHUNK_MULTIPART);
  assert(parsed->hdr.ref_tag == value.hdr.ref_tag);
  assert(parsed->hdr.size == value.hdr.size);
  assert(parsed->content_type == value.content_type);
  assert(parsed->manifest_blob == value.manifest_blob);
  assert(parsed->part_count() == 3);

  auto decoded = decode_manifest_body(gm_blob_body(parsed->manifest_blob));
  assert(decoded);
  assert(decoded->included_parts == std::vector<uint16_t>({1, 2, 5}));
  assert(decoded->generations == std::vector<uint16_t>({0, 1, 0}));
  assert(decoded->sizes == std::vector<uint64_t>({5 * 1024 * 1024, 6 * 1024 * 1024, 1024}));

  // ovh_multipart_manifest_blob -- zero-copy accessor move_object_to_g
  // uses to hand the stored blob to GC as a G:M{FREE_PARTS} value.
  const ObjectValueHeader* h = ovh_ptr(buf.view());
  assert(h);
  assert(h->chunk_type == CHUNK_MULTIPART);
  const std::string_view mv = ovh_multipart_manifest_blob(buf.view(), h);
  assert(mv == value.manifest_blob);
  assert(parse_gm_value(mv) && parse_gm_value(mv)->mode == GM_MODE_FREE_PARTS);

  // A local manifest that is not a FREE_PARTS blob is corrupt.
  {
    ObjectValue bad = value;
    const std::string mixed = make_gm_bytes(GM_MODE_MIXED, 0, "x");
    bad.manifest_blob = mixed;
    OValueBuf bbuf;
    assert(!write_object_value(bbuf, bad));
  }

  std::cout << "test_chunk_multipart_object_value_roundtrip OK\n";
}


// ---------------------------------------------------------------------------
// Child keys (S:C): make_child_key / parse_child_key.
// ---------------------------------------------------------------------------
void test_child_keys()
{
  const bucket_id_t bucket_id = test_bucket_id(21);
  const RefTag ref_tag = make_ref_tag(0x77);
  const char types[] = {kChildTypeTags, kChildTypeAnnotation, kChildTypeExtended,
                        kChildTypeManifest, kChildTypeChecksum};
  for (const char t : types) {
    KeyBuf k;
    make_child_key(bucket_id, ref_tag.view(), t, k);
    assert(k.view().size() == 27);
    auto p = parse_child_key(k.view());
    assert(p);
    assert(p->bucket_id == bucket_id);
    assert(p->ref_tag == ref_tag);
    assert(p->child_type == t);
    // every child sits under the object's own [header][ref_tag] prefix
    KeyBuf prefix;
    make_c_prefix(bucket_id, ref_tag.view(), prefix);
    assert(k.view().substr(0, prefix.view().size()) == prefix.view());
  }
  // make_ct_key is the 'T' child
  KeyBuf ct, ck;
  make_ct_key(bucket_id, ref_tag.view(), ct);
  make_child_key(bucket_id, ref_tag.view(), kChildTypeTags, ck);
  assert(ct.view() == ck.view());

  // rejections
  KeyBuf k;
  make_child_key(bucket_id, ref_tag.view(), kChildTypeManifest, k);
  const std::string good(k.view());
  assert(!parse_child_key(good.substr(0, good.size() - 1)));        // too short
  assert(!parse_child_key(good + "x"));                              // too long
  std::string bad = good;
  bad.back() = 'Z';                                                  // unknown type
  assert(!parse_child_key(bad));
  bad = good;
  bad[0] = 'D';                                                      // wrong namespace
  assert(!parse_child_key(bad));
  bad = good;
  bad[offsetof(KeyHeaderS, cat)] = kCategoryObject;                  // wrong category
  assert(!parse_child_key(bad));
  KeyBuf d;
  make_d_key(bucket_id, 1, ref_tag.view(), 5, d);
  assert(!parse_child_key(d.view()));                                // data D key
  KeyBuf o;
  make_object_key(bucket_id, "some/object-name-xx", o);
  assert(!parse_child_key(o.view()));                                // S:O key
  std::cout << "test_child_keys OK\n";
}

void test_m_head_and_part_checksum_fields()
{
  MHeadValueBuf buf;
  assert(make_m_head_value(buf, 1, "text/plain", 0, {}, CksumAlg::SHA256,
                           CksumType::COMPOSITE));
  auto parsed = parse_m_head_value(buf.view());
  assert(parsed);
  assert(parsed->hdr.cksum_alg == static_cast<uint8_t>(CksumAlg::SHA256));
  assert(parsed->hdr.cksum_type == static_cast<uint8_t>(CksumType::COMPOSITE));
  auto view = view_m_head_value(buf.view());
  assert(view && view->cksum_alg == CksumAlg::SHA256 &&
         view->cksum_type == CksumType::COMPOSITE);

  MPartValue v;
  v.state = M_PART_COMMITTED;
  for (size_t i = 0; i < sizeof(v.cksum); ++i) {
    v.cksum[i] = static_cast<uint8_t>(i + 1);
  }
  uint8_t raw[sizeof(MPartValue)];
  make_m_part_value(v, raw);
  auto pp = parse_m_part_value(
      std::string_view(reinterpret_cast<const char*>(raw), sizeof(raw)));
  assert(pp);
  assert(std::memcmp(pp->cksum, v.cksum, sizeof(v.cksum)) == 0);
  std::cout << "test_m_head_and_part_checksum_fields OK\n";
}

// A multipart :O: value skeleton (everything place_multipart expects of its caller).
ObjectValue mp_base(CksumAlg alg, uint16_t parts, bool shared = false, bool ref = false)
{
  ObjectValue v;
  v.hdr.ref_tag = make_ref_tag(0xA1);
  v.hdr.chunk_type = ref ? CHUNK_MULTIPART_REF : CHUNK_MULTIPART;
  v.hdr.set_cksum_alg(alg);
  if (shared) {
    v.hdr.flags |= kFlagSharedData;
  }
  v.content_type = "application/octet-stream";
  uint8_t md5[kMd5Bytes] = {1, 2, 3};
  v.set_etag_raw(md5, parts);
  if (ref) {
    v.chunk = ChunkDescriptor(CHUNK_MULTIPART_REF, make_ref_tag(0xB2).view().data(),
                              kNullBucket);
  } else {
    v.set_self_chunk(kNullBucket);
  }
  std::array<uint8_t, kCksumMaxDigestBytes> digest{};
  std::fill_n(digest.begin(), cksum_digest_len(alg), uint8_t{0x5A});
  v.set_cksum_combined(digest);
  return v;
}

MultipartPlacementIn mp_in(std::string_view manifest, std::string_view blob, bool composite)
{
  MultipartPlacementIn in;
  in.manifest = manifest;
  in.cksum_blob = blob;
  in.composite = composite;
  in.first_part = FirstPartInfo{1, 0, 5 * 1024 * 1024};
  return in;
}

struct Placed {
  uint8_t cksum_loc{};
  uint8_t manifest_loc{};
  uint8_t mflags{};
  bool ext_meta{};
  size_t wire_len{};
  MultipartPlacementOut out;
};

// place_multipart -> write_object_value -> parse_object_value; checks the wire
// round trip and returns the placement decision.
Placed place_roundtrip(ObjectValue v, const MultipartPlacementIn& in)
{
  Placed r;
  const bool placed = place_multipart(v, in, r.out);
  assert(placed);
  OValueBuf buf;
  const bool written = write_object_value(buf, v);
  assert(written);
  (void)placed;
  (void)written;
  assert(buf.view().size() <= kMaxOValueBytes);
  auto parsed = parse_object_value(buf.view());
  assert(parsed);
  assert(parsed->hdr.flags == v.hdr.flags);
  assert(parsed->cksum_loc == v.cksum_loc);
  assert(parsed->manifest_loc == v.manifest_loc);
  assert(parsed->mflags == v.mflags);
  assert(parsed->cksum_alg() == v.cksum_alg());
  assert(parsed->cksum_type() == v.cksum_type());
  assert(parsed->part_count() == v.part_count());
  assert(parsed->cksum_combined == v.cksum_combined);
  assert(parsed->cksum_blob == v.cksum_blob);
  assert(parsed->manifest_blob == v.manifest_blob);
  assert(std::ranges::equal(parsed->metadata_frame, v.metadata_frame));
  assert(parsed->chunk.get_ref_tag() == v.chunk.get_ref_tag());
  assert(parsed->chunk.get_type() == v.hdr.chunk_type);
  if (v.mflags & kMFlagFirstPartInfo) {
    assert(parsed->first_part.part_number == in.first_part.part_number);
    assert(parsed->first_part.generation == in.first_part.generation);
    assert(parsed->first_part.size == in.first_part.size);
  }
  r.cksum_loc = v.cksum_loc;
  r.manifest_loc = v.manifest_loc;
  r.mflags = v.mflags;
  r.ext_meta = v.has_external_metadata();
  r.wire_len = buf.view().size();
  return r;
}

void test_multipart_object_value_placement()
{
  constexpr size_t dlen = 32;  // SHA256
  const size_t fixed = sizeof(ObjectValueHeader) + std::string_view("application/octet-stream").size() +
                       sizeof(ChunkDescriptor) + kMultipartHdrBytes + dlen;
  constexpr uint8_t kComp = kMFlagComposite;

  // Everything inline, value exactly at the budget.
  {
    const std::string blob(3 * dlen, '\x11');
    const std::string manifest = mk_blob(kMaxOValueBytes - fixed - blob.size());
    const Placed p = place_roundtrip(mp_base(CksumAlg::SHA256, 3), mp_in(manifest, blob, true));
    assert(p.cksum_loc == kCksumInline && p.manifest_loc == kManifestInline);
    assert(p.mflags == kComp && !p.ext_meta && p.wire_len == kMaxOValueBytes);
    assert(!p.out.has_child_e && p.out.manifest_child.empty() && p.out.checksum_child.empty());
    // one byte more manifest: the blob no longer fits -> C:S
    const std::string manifest_p1 = manifest + "m";
    const Placed q = place_roundtrip(mp_base(CksumAlg::SHA256, 3), mp_in(manifest_p1, blob, true));
    assert(q.manifest_loc == kManifestInline && q.cksum_loc == kCksumChildS);
    assert(q.out.checksum_child == blob);
  }
  // Manifest boundary without a checksum: exactly 1,024 stays inline, one byte over -> C:M.
  {
    const size_t f = fixed - dlen;
    const std::string at = mk_blob(kMaxOValueBytes - f, 'a');
    const Placed p = place_roundtrip(mp_base(CksumAlg::NONE, 2), mp_in(at, "", false));
    assert(p.manifest_loc == kManifestInline && p.mflags == 0 && p.wire_len == kMaxOValueBytes);
    const std::string over = at + "a";
    const Placed q = place_roundtrip(mp_base(CksumAlg::NONE, 2), mp_in(over, "", false));
    assert(q.manifest_loc == kManifestChildM && q.mflags == kMFlagFirstPartInfo);
    assert(q.out.manifest_child == over && !q.out.has_child_e && !q.ext_meta);
    assert(q.cksum_loc == kCksumInline);
  }
  // Checksum blob: inline -> C:S up to 75 KB -> file above it.
  {
    const size_t max_parts = kMaxChildPayloadBytes / dlen;  // 2400
    const std::string small(10 * dlen, '\x22');
    const Placed p = place_roundtrip(mp_base(CksumAlg::SHA256, 10), mp_in(mk_blob(10), small, true));
    assert(p.cksum_loc == kCksumInline);
    const std::string at(max_parts * dlen, '\x33');
    const Placed q = place_roundtrip(mp_base(CksumAlg::SHA256, static_cast<uint16_t>(max_parts)),
                                     mp_in(mk_blob(10), at, true));
    assert(q.cksum_loc == kCksumChildS && q.out.checksum_child.size() == kMaxChildPayloadBytes);
    assert(q.out.checksum_file.empty());
    const std::string over((max_parts + 1) * dlen, '\x44');
    const Placed r = place_roundtrip(mp_base(CksumAlg::SHA256, static_cast<uint16_t>(max_parts + 1)),
                                     mp_in(mk_blob(10), over, true));
    assert(r.cksum_loc == kCksumFile && r.out.checksum_file == over);
    assert(r.out.checksum_child.empty());
  }
  // Manifest in C:M together with the checksum blob in C:S / file.
  {
    const std::string manifest = mk_blob(2000, 'z');
    const std::string blob(100 * dlen, '\x55');
    const Placed p = place_roundtrip(mp_base(CksumAlg::SHA256, 100), mp_in(manifest, blob, true));
    assert(p.manifest_loc == kManifestChildM && p.cksum_loc == kCksumChildS);
    assert(p.out.manifest_child == manifest && p.out.checksum_child == blob);
    assert(p.mflags == (kComp | kMFlagFirstPartInfo));
  }
  // Algorithm axis: FULL_OBJECT (CRC64NVME) is not composite; digest widths differ.
  {
    const std::string blob(4 * 8, '\x66');
    const Placed p = place_roundtrip(mp_base(CksumAlg::CRC64NVME, 4), mp_in(mk_blob(20), blob, false));
    assert(p.mflags == 0 && p.cksum_loc == kCksumInline);
    const std::string blob512(4 * 64, '\x77');
    const Placed q = place_roundtrip(mp_base(CksumAlg::SHA512, 4), mp_in(mk_blob(20), blob512, true));
    assert(q.mflags == kComp && q.cksum_loc == kCksumInline);
    const std::string blob32(3 * 4, '\x78');
    const Placed r = place_roundtrip(mp_base(CksumAlg::CRC32, 3), mp_in(mk_blob(20), blob32, true));
    assert(r.mflags == kComp && r.cksum_loc == kCksumInline);
  }
  // Metadata: inline, spilled next to an inline manifest, spilled with the manifest.
  {
    std::array<uint8_t, MAX_META_FRAME_BYTES> frame{};
    size_t frame_len = 0;
    const std::string big_val(1500, 'v');
    const MetaPair pair{"k", big_val};
    assert(encode_metadata(std::span<const MetaPair>(&pair, 1), frame, frame_len));
    const std::span<const uint8_t> mf(frame.data(), frame_len);
    const MetaPair small_pair{"k", "v"};
    std::array<uint8_t, MAX_META_FRAME_BYTES> sframe{};
    size_t sframe_len = 0;
    assert(encode_metadata(std::span<const MetaPair>(&small_pair, 1), sframe, sframe_len));
    const std::span<const uint8_t> smf(sframe.data(), sframe_len);

    auto with_meta = [](ObjectValue v) {
      v.hdr.metadata_count = 1;
      return v;
    };
    // small metadata + small manifest: all inline
    const std::string small_manifest = mk_blob(100);
    MultipartPlacementIn in = mp_in(small_manifest, "", false);
    in.metadata_frame = smf;
    Placed p = place_roundtrip(with_meta(mp_base(CksumAlg::NONE, 2)), in);
    assert(!p.ext_meta && p.manifest_loc == kManifestInline);
    // big metadata + small manifest: metadata -> C:E, manifest stays inline
    in = mp_in(small_manifest, "", false);
    in.metadata_frame = mf;
    p = place_roundtrip(with_meta(mp_base(CksumAlg::NONE, 2)), in);
    assert(p.ext_meta && p.manifest_loc == kManifestInline && p.out.has_child_e);
    assert(std::ranges::equal(p.out.child_e.metadata_frame, mf) && p.out.child_e.tail.empty());
    // big metadata + big manifest: one combined C:E (unshared)
    const std::string big_manifest = mk_blob(3000, 'q');
    in = mp_in(big_manifest, "", false);
    in.metadata_frame = mf;
    p = place_roundtrip(with_meta(mp_base(CksumAlg::NONE, 2)), in);
    assert(p.ext_meta && p.manifest_loc == kManifestChildE && p.out.has_child_e);
    assert(p.out.child_e.tail == big_manifest && p.out.manifest_child.empty());
    assert(p.mflags == kMFlagFirstPartInfo);
    // same, shared: metadata -> C:E, manifest -> C:M
    p = place_roundtrip(with_meta(mp_base(CksumAlg::NONE, 2, /*shared=*/true)), in);
    assert(p.ext_meta && p.manifest_loc == kManifestChildM && p.out.has_child_e);
    assert(p.out.child_e.tail.empty() && p.out.manifest_child == big_manifest);
    // small metadata + big manifest: metadata stays inline, manifest -> C:M
    in = mp_in(big_manifest, "", false);
    in.metadata_frame = smf;
    p = place_roundtrip(with_meta(mp_base(CksumAlg::NONE, 2)), in);
    assert(!p.ext_meta && p.manifest_loc == kManifestChildM && !p.out.has_child_e);
    // metadata_count and the frame must agree
    ObjectValue bad = mp_base(CksumAlg::NONE, 2);
    MultipartPlacementOut po;
    assert(place_multipart(bad, mp_in(mk_blob(100), "", false), po));
    bad.hdr.metadata_count = 1;
    assert(!place_multipart(bad, mp_in(mk_blob(100), "", false), po));
  }
  // Shared (SRC or TGT of a copy): C:M is authoritative, local copies are cached.
  {
    const std::string blob(3 * dlen, '\x11');
    const std::string manifest = mk_blob(300);
    for (bool ref : {false, true}) {
      const Placed p = place_roundtrip(mp_base(CksumAlg::SHA256, 3, true, ref),
                                       mp_in(manifest, blob, true));
      assert(p.manifest_loc == kManifestChildM && p.cksum_loc == kCksumChildS);
      assert(p.mflags == (kComp | kMFlagManifestCached | kMFlagCksumCached));
      assert(p.out.manifest_child == manifest && p.out.checksum_child == blob);
    }
    // the manifest does not fit locally: first_part_info only
    const std::string big = mk_blob(2000);
    const Placed q = place_roundtrip(mp_base(CksumAlg::SHA256, 3, true), mp_in(big, blob, true));
    assert(q.manifest_loc == kManifestChildM && q.out.manifest_child == big);
    assert(q.mflags == (kComp | kMFlagFirstPartInfo | kMFlagCksumCached));
    // algorithm NONE, shared
    const Placed r = place_roundtrip(mp_base(CksumAlg::NONE, 3, true), mp_in(manifest, "", false));
    assert(r.mflags == kMFlagManifestCached && r.cksum_loc == kCksumInline);
    // a blob that never fits locally goes to the file and is not cached
    const std::string huge(2401 * dlen, '\x44');
    const Placed s = place_roundtrip(mp_base(CksumAlg::SHA256, 2401, true), mp_in(manifest, huge, true));
    assert(s.cksum_loc == kCksumFile && !(s.mflags & kMFlagCksumCached));
    assert(s.out.checksum_file == huge);
  }
  // CopyObject of a source whose blob is already external: keep that location.
  {
    const std::string blob(3 * dlen, '\x11');
    const std::string manifest300 = mk_blob(300);
    MultipartPlacementIn in = mp_in(manifest300, blob, true);
    in.existing_cksum_loc = kCksumFile;
    const Placed p = place_roundtrip(mp_base(CksumAlg::SHA256, 3, true, true), in);
    assert(p.cksum_loc == kCksumFile && p.out.checksum_file.empty() &&
           p.out.checksum_child.empty());
    assert(p.mflags & kMFlagCksumCached);
    in = mp_in(manifest300, "", true);
    in.existing_cksum_loc = kCksumChildS;
    const Placed q = place_roundtrip(mp_base(CksumAlg::SHA256, 3, true, true), in);
    assert(q.cksum_loc == kCksumChildS && !(q.mflags & kMFlagCksumCached));
    assert(q.out.checksum_child.empty());
  }
  // Rejections: manifest that is not FREE_PARTS, oversized manifest, wrong blob length.
  {
    MultipartPlacementOut po;
    ObjectValue v = mp_base(CksumAlg::NONE, 2);
    assert(!place_multipart(v, mp_in(make_gm_bytes(GM_MODE_MIXED, 0, "x"), "", false), po));
    const std::string too_big = mk_blob(kMaxChildPayloadBytes + 1);
    v = mp_base(CksumAlg::NONE, 2);
    assert(!place_multipart(v, mp_in(too_big, "", false), po));
    v = mp_base(CksumAlg::SHA256, 3);
    assert(!place_multipart(v, mp_in(mk_blob(20), std::string(2 * dlen, 'x'), true), po));
    // single-part values are not placed here
    v = mp_base(CksumAlg::NONE, 1);
    v.hdr.chunk_type = CHUNK_STORAGE;
    assert(!place_multipart(v, mp_in(mk_blob(20), "", false), po));
  }
  std::cout << "test_multipart_object_value_placement OK\n";
}

// P:M key — Sweeper-driven multipart cleanup entry.
void test_pm_key()
{
  const bucket_id_t bucket_id = test_bucket_id(11);
  const RefTag ref_tag = make_ref_tag(0x66);

  KeyBuf pm_key;
  make_pm_key(bucket_id, ref_tag.view(), pm_key);
  auto parsed = parse_pm_key(pm_key.view());
  assert(parsed);
  assert(parsed->bucket_id == bucket_id);
  assert(parsed->ref_tag == ref_tag);

  std::cout << "test_pm_key OK\n";
}

void test_pm_value()
{
  // P:M uses G:M's value layout: ABORT is DEEP, COMMIT is MIXED.
  {
    const std::string v = make_gm_bytes(GM_MODE_DEEP, 111222, {});
    auto parsed = parse_gm_value(v);
    assert(parsed);
    assert(parsed->mode == GM_MODE_DEEP);
    assert(parsed->mtime == 111222);
    assert(parsed->body.empty());
  }

  // COMMIT -- the Sweeper writes the same bytes under the G:M key, so a
  // P:M{MIXED} value must be a valid G:M{MIXED} value byte for byte.
  {
    const uint16_t included[2] = {2, 4};
    const uint16_t generations[2] = {1, 0};
    const uint64_t sizes[2] = {100, 100};
    const std::string pm =
        make_gm_bytes(GM_MODE_MIXED, 333444, encode_manifest(included, generations, sizes));
    const std::string gm = pm;  // the hand-off copies the value unchanged
    auto parsed = parse_gm_value(gm);
    assert(parsed);
    assert(parsed->mode == GM_MODE_MIXED);
    assert(parsed->mtime == 333444);
    assert(!parsed->body.empty());

    auto decoded = decode_manifest_body(parsed->body);
    assert(decoded);
    assert(decoded->included_parts == std::vector<uint16_t>({2, 4}));
    assert(decoded->generations == std::vector<uint16_t>({1, 0}));
  }

  // Complete's single buffer: [hdr][manifest]. Stored as a FREE_PARTS blob,
  // then the header is patched to MIXED and only the Knob 1+2 prefix is
  // written as the G:M / P:M value. The prefix must work as a keep-list.
  {
    const uint16_t included[4] = {1, 2, 3, 5};
    const uint16_t generations[4] = {0, 1, 0, 2};
    const uint64_t sizes[4] = {5000, 5000, 5000, 77};
    size_t keep_len = 0;
    std::string buf = make_gm_bytes(GM_MODE_FREE_PARTS, 0,
                                    encode_manifest(included, generations, sizes, &keep_len));
    const size_t full_len = buf.size();
    assert(keep_len > 0 && kGmHeaderSize + keep_len < full_len);

    // Step 1: the whole buffer is the stored blob.
    assert(!gm_blob_body(buf).empty());
    auto dec = decode_manifest_body(gm_blob_body(buf));
    assert(dec && dec->sizes == std::vector<uint64_t>({5000, 5000, 5000, 77}));

    // Step 2: patch the header, take the prefix.
    write_gm_header(reinterpret_cast<uint8_t*>(buf.data()), GM_MODE_MIXED, 42);
    const std::string_view prefix(buf.data(), kGmHeaderSize + keep_len);
    auto parsed = parse_gm_value(prefix);
    assert(parsed && parsed->mode == GM_MODE_MIXED && parsed->mtime == 42);
    ManifestKeepCursor cursor(parsed->body);
    assert(cursor.valid());
    assert(cursor.is_kept(1, 0));
    assert(!cursor.is_kept(2, 0));
    assert(cursor.is_kept(2, 1));
    assert(cursor.is_kept(3, 0));
    assert(!cursor.is_kept(4, 0));
    assert(cursor.is_kept(5, 2));
  }

  std::cout << "test_pm_value OK\n";
}

void test_composite_etag_builder()
{
  ETag part1, part2, part3;
  uint8_t d1[16], d2[16], d3[16];
  std::memset(d1, 0x11, sizeof(d1));
  std::memset(d2, 0x22, sizeof(d2));
  std::memset(d3, 0x33, sizeof(d3));
  part1.load_raw(d1, 0);
  part2.load_raw(d2, 0);
  part3.load_raw(d3, 0);

  CompositeEtagBuilder builder;
  builder.fold(part1);
  builder.fold(part2);
  builder.fold(part3);
  const ETag composite = builder.finalize();

  assert(composite.is_multipart());
  assert(composite.part_count() == 3);

  // Cross-check against a manual MD5-of-concatenated-digests computation.
  unsigned char expected[16];
  unsigned int expected_len = 0;
  EVP_MD_CTX *ctx = EVP_MD_CTX_new();
  EVP_DigestInit_ex(ctx, EVP_md5(), nullptr);
  EVP_DigestUpdate(ctx, d1, sizeof(d1));
  EVP_DigestUpdate(ctx, d2, sizeof(d2));
  EVP_DigestUpdate(ctx, d3, sizeof(d3));
  EVP_DigestFinal_ex(ctx, expected, &expected_len);
  EVP_MD_CTX_free(ctx);

  ETag expected_etag;
  expected_etag.load_raw(expected, 3);
  assert(composite == expected_etag);
  assert(composite.to_hex() == expected_etag.to_hex());

  std::cout << "test_composite_etag_builder OK (" << composite.to_hex() << ")\n";
}

} // namespace

int main()
{
  test_m_head_key();
  test_u_part_key();
  test_gm_key();
  test_gu_key();
  test_gu_value_group();
  test_m_head_value();
  test_m_part_value();
  test_gm_value();
  test_gm_value_free_parts();
  test_part_filename();
  test_chunk_multipart_object_value_roundtrip();
  test_child_keys();
  test_m_head_and_part_checksum_fields();
  test_multipart_object_value_placement();
  test_pm_key();
  test_pm_value();
  test_composite_etag_builder();
  std::cout << "ALL MULTIPART KEY/VALUE TESTS PASSED\n";
  return 0;
}
