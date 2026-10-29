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

void test_gm_value()
{
  // DEEP — no keep-list body at all.
  {
    GmValueBuf buf;
    make_gm_value(buf, GM_MODE_DEEP, ManifestInput{});
    auto parsed = parse_gm_value(buf.view());
    assert(parsed);
    assert(parsed->mode == GM_MODE_DEEP);
    assert(parsed->body.empty());
  }

  // MIXED — carries the manifest's keep-list, in the compact encoding.
  {
    const uint16_t included[2] = {1, 3};
    const uint32_t generations[2] = {0, 2};
    const uint64_t sizes[2] = {100, 100};
    GmValueBuf buf;
    make_gm_value(buf, GM_MODE_MIXED, ManifestInput{included, generations, sizes});
    auto parsed = parse_gm_value(buf.view());
    assert(parsed);
    assert(parsed->mode == GM_MODE_MIXED);
    assert(!parsed->body.empty());

    auto decoded = decode_manifest_body(parsed->body);
    assert(decoded);
    assert(decoded->included_parts == std::vector<uint16_t>({1, 3}));
    assert(decoded->generations == std::vector<uint32_t>({0, 2}));

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
  const uint32_t generations[2] = {0, 3};
  const uint64_t sizes[2] = {5 * 1024 * 1024, 1024};

  // Encode once (as the real manifest body -- what complete_multipart_
  // upload_step3 would have put on the :O: value), then verbatim-copy it
  // into a GmValueBuf under FREE_PARTS, mirroring move_object_to_g's own
  // memcpy (no decode/re-encode).
  uint8_t manifest_buf[kManifestBodyWorstCase];
  const size_t manifest_len = encode_manifest_body(
      manifest_buf, sizeof(manifest_buf), ManifestInput{included, generations, sizes});
  assert(manifest_len > 0);

  GmValueBuf buf;
  buf.data[0] = static_cast<uint8_t>(GM_MODE_FREE_PARTS);
  std::memcpy(buf.data + 1, manifest_buf, manifest_len);
  buf.len = 1 + manifest_len;

  auto parsed = parse_gm_value(buf.view());
  assert(parsed);
  assert(parsed->mode == GM_MODE_FREE_PARTS);
  assert(!parsed->body.empty());

  auto decoded = decode_manifest_body(parsed->body);
  assert(decoded);
  assert(decoded->included_parts == std::vector<uint16_t>({1, 2}));
  assert(decoded->generations == std::vector<uint32_t>({0, 3}));
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
// parse_object_value's manifest_body tail segment (always last, no length
// prefix).
void test_chunk_multipart_object_value_roundtrip()
{
  const uint16_t included[3] = {1, 2, 5};
  const uint32_t generations[3] = {0, 1, 0};
  const uint64_t sizes[3] = {5 * 1024 * 1024, 6 * 1024 * 1024, 1024};
  uint8_t manifest_buf[kManifestBodyWorstCase];
  const size_t manifest_len = encode_manifest_body(
      manifest_buf, sizeof(manifest_buf), ManifestInput{included, generations, sizes});
  assert(manifest_len > 0);

  ObjectValue value;
  value.hdr.ref_tag = make_ref_tag(0xEF);
  value.hdr.size = sizes[0] + sizes[1] + sizes[2];
  value.hdr.chunk_type = CHUNK_MULTIPART;
  value.content_type = "application/octet-stream";
  value.manifest_body.assign(reinterpret_cast<const char*>(manifest_buf), manifest_len);

  OValueBufMultipart buf;
  assert(write_object_value(buf, value));

  auto parsed = parse_object_value(buf.view());
  assert(parsed);
  assert(parsed->hdr.chunk_type == CHUNK_MULTIPART);
  assert(parsed->hdr.ref_tag == value.hdr.ref_tag);
  assert(parsed->hdr.size == value.hdr.size);
  assert(parsed->content_type == value.content_type);
  assert(parsed->manifest_body == value.manifest_body);

  auto decoded = decode_manifest_body(parsed->manifest_body);
  assert(decoded);
  assert(decoded->included_parts == std::vector<uint16_t>({1, 2, 5}));
  assert(decoded->generations == std::vector<uint32_t>({0, 1, 0}));
  assert(decoded->sizes == std::vector<uint64_t>({5 * 1024 * 1024, 6 * 1024 * 1024, 1024}));

  // ovh_multipart_manifest_body -- zero-copy accessor move_object_to_g
  // uses to copy the manifest verbatim into G:M{FREE_PARTS}.
  const ObjectValueHeader* h = ovh_ptr(buf.view());
  assert(h);
  assert(h->chunk_type == CHUNK_MULTIPART);
  const std::string_view mv = ovh_multipart_manifest_body(buf.view(), h);
  assert(mv == value.manifest_body);

  std::cout << "test_chunk_multipart_object_value_roundtrip OK\n";
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
  // ABORT — no keep-list body at all.
  {
    PmValueBuf buf;
    make_pm_value(buf, PM_STATE_ABORT, /*mtime=*/111222, ManifestInput{});
    auto parsed = parse_pm_value(buf.view());
    assert(parsed);
    assert(parsed->state == PM_STATE_ABORT);
    assert(parsed->mtime == 111222);
    assert(parsed->body.empty());
  }

  // COMMIT — carries the manifest's keep-list, same shape as G:M{MIXED}'s.
  {
    const uint16_t included[2] = {2, 4};
    const uint32_t generations[2] = {1, 0};
    const uint64_t sizes[2] = {100, 100};
    PmValueBuf buf;
    make_pm_value(buf, PM_STATE_COMMIT, /*mtime=*/333444, ManifestInput{included, generations, sizes});
    auto parsed = parse_pm_value(buf.view());
    assert(parsed);
    assert(parsed->state == PM_STATE_COMMIT);
    assert(parsed->mtime == 333444);
    assert(!parsed->body.empty());

    auto decoded = decode_manifest_body(parsed->body);
    assert(decoded);
    assert(decoded->included_parts == std::vector<uint16_t>({2, 4}));
    assert(decoded->generations == std::vector<uint32_t>({1, 0}));
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
  test_pm_key();
  test_pm_value();
  test_composite_etag_builder();
  std::cout << "ALL MULTIPART KEY/VALUE TESTS PASSED\n";
  return 0;
}
