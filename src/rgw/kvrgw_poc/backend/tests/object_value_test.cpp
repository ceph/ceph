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

#include "id_meta.hpp"
#include "id_tag.hpp"
#include "object_value.hpp"
#include "ref_count.hpp"
#include "tenant_value.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <cstring>
#include <iostream>
#include <span>
#include <string>
#include <vector>

namespace {

void test_bucket_value_endianness()
{
  kvrgw::bucket_id_t bucket_id = 1;

  const int64_t timestamps[] = {0, 1, 1785926327, 2147483647};
  for (const int64_t ts : timestamps) {
    const std::string encoded = kvrgw::make_bucket_value(bucket_id, ts);
    assert(encoded.size() == 18);
    const auto parsed = kvrgw::parse_bucket_value(encoded);
    assert(parsed);
    assert(parsed->bucket_id == bucket_id);
    assert(parsed->created_at_unix == ts);
    assert(parsed->versioning_state == 0);
  }
}

void test_ref_tag_zero_base64_roundtrip()
{
  kvrgw::ObjectValue value;
  std::memset(value.hdr.ref_tag, 0, 12);
  std::memset(value.hdr.etag, 0xDE, 16);
  value.hdr.size = 12;
  value.hdr.last_modified_sec = 1785926327;
  value.hdr.last_modified_nsec = 123456789;
  value.content_type = "text/plain";
  value.hdr.chunk.type = kvrgw::CHUNK_STORAGE;

  kvrgw::OValueBuf buf;
  kvrgw::ObjectValueHeader wire = value.hdr;
  wire.content_type_len = static_cast<uint8_t>(value.content_type.size());
  kvrgw::hdr_to_be(wire);
  assert(buf.set_header(wire));
  assert(buf.append(value.content_type.data(), value.content_type.size()));
  const auto parsed = kvrgw::parse_object_value(buf.view());
  assert(parsed);
  assert(std::memcmp(parsed->hdr.ref_tag, value.hdr.ref_tag, 12) == 0);
  assert(std::memcmp(parsed->hdr.etag, value.hdr.etag, 16) == 0);
  assert(parsed->hdr.size == value.hdr.size);
  assert(parsed->hdr.last_modified_sec == value.hdr.last_modified_sec);
  assert(parsed->hdr.last_modified_nsec == value.hdr.last_modified_nsec);
  assert(parsed->content_type == value.content_type);
  assert(parsed->hdr.chunk.type == kvrgw::CHUNK_STORAGE);
}

void test_legacy_bucket_value_ten_bytes()
{
  std::string legacy(10, '\0');
  legacy[7] = static_cast<char>(0x01);
  legacy[8] = static_cast<char>(0x00);
  legacy[9] = static_cast<char>(0x01);

  const auto parsed = kvrgw::parse_bucket_value(legacy);
  assert(parsed);
  assert(parsed->bucket_id == 1);
  assert(parsed->created_at_unix == 0);
}

void test_tenant_value_roundtrip()
{
  const uint32_t tenant_ids[] = {1, 42, 99999};
  for (const uint32_t id : tenant_ids) {
    const int64_t ts = 1785926327;
    const std::string encoded = kvrgw::make_tenant_value(id, ts);
    assert(encoded.size() == 12);
    const auto parsed = kvrgw::parse_tenant_value(encoded);
    assert(parsed);
    assert(parsed->tenant_id == id);
    assert(parsed->created_at_unix == ts);
  }
}

void test_bucket_value_versioning_state()
{
  kvrgw::bucket_id_t bucket_id = 0x4200000000000000ULL;

  for (uint8_t vs = 0; vs <= 2; ++vs) {
    const std::string encoded = kvrgw::make_bucket_value(
        bucket_id, 1700000000, 0, static_cast<kvrgw::VersioningState>(vs));
    assert(encoded.size() == 18);
    const auto parsed = kvrgw::parse_bucket_value(encoded);
    assert(parsed);
    assert(parsed->bucket_id == bucket_id);
    assert(parsed->versioning_state == vs);
  }
}

void test_object_value_header_version_fields()
{
  kvrgw::ObjectValue value;
  std::memset(value.hdr.ref_tag, 0xAA, 12);
  std::memset(value.hdr.etag, 0xBB, 16);
  value.hdr.size = 100;
  value.hdr.last_modified_sec = 1700000000;
  value.hdr.last_modified_nsec = 500000000;
  value.hdr.chunk.type = kvrgw::CHUNK_STORAGE;
  value.content_type = "text/html";

  const kvrgw::version_id_t vids[] = {
      kvrgw::version_id_t{0}, kvrgw::version_id_t{1},
      kvrgw::version_id_t{0xFFFFFFFF}, kvrgw::version_id_t{0xFFFFFFFE},
      kvrgw::version_id_t{42}};
  for (kvrgw::version_id_t vid : vids) {
    value.hdr.version_id = vid;
    value.hdr.next_vid =
        kvrgw::version_id_t{vid.raw() > 0 ? vid.raw() - 1 : 0xFFFFFFFF};

    kvrgw::OValueBuf buf;
    kvrgw::ObjectValueHeader wire = value.hdr;
    wire.content_type_len = static_cast<uint8_t>(value.content_type.size());
    kvrgw::hdr_to_be(wire);
    assert(buf.set_header(wire));
    assert(buf.append(value.content_type.data(), value.content_type.size()));

    const auto parsed = kvrgw::parse_object_value(buf.view());
    assert(parsed);
    assert(parsed->hdr.version_id == vid);
    assert(parsed->hdr.next_vid == value.hdr.next_vid);
    assert(parsed->hdr.size == 100);
    assert(parsed->content_type == "text/html");
  }
}

void test_is_delete_marker_flag()
{
  kvrgw::ObjectValue value;
  value.hdr.flags = 0;
  assert(!value.is_delete_marker());

  value.hdr.flags = kvrgw::ObjectValue::kFlagFenced;
  assert(value.is_delete_marker());

  value.hdr.flags =
      kvrgw::ObjectValue::kFlagExtendedAttrs | kvrgw::ObjectValue::kFlagFenced;
  assert(value.is_delete_marker());
  assert(value.has_extended_attrs());
}

void test_child_value_header_layout()
{
  kvrgw::ChildValueHeader hdr{};
  assert(sizeof(hdr) == 8);
  assert(static_cast<uint8_t>(kvrgw::CHILD_FLAG_SHARED) == 0x01);
  hdr.flags = kvrgw::CHILD_FLAG_SHARED;
  hdr.ref_count = 7;
  const std::string payload("hello");
  const std::string raw = kvrgw::make_child_value(hdr, payload);
  assert(raw.size() == 8 + payload.size());
  kvrgw::ChildValueHeader parsed{};
  assert(kvrgw::parse_child_value_header(raw, parsed));
  assert(kvrgw::child_flag_shared(parsed.flags));
  assert(parsed.ref_count == 7);
  const auto rest = kvrgw::child_value_payload(raw);
  assert(rest == payload);
  const auto sized = kvrgw::child_value_payload(raw, payload.size());
  assert(sized == payload);
}

void test_tag_encode_exact_size()
{
  const kvrgw::TagPair p{"env", "prod"};
  std::vector<uint8_t> encoded;
  assert(kvrgw::encode(std::span<const kvrgw::TagPair>(&p, 1), encoded));
  assert(!encoded.empty());
  size_t frame_size = 0;
  assert(kvrgw::encoded_tag_frame_size(encoded, frame_size));
  assert(frame_size == encoded.size());
  std::array<kvrgw::TagPair, kvrgw::MAX_TAG_COUNT> tags{};
  assert(kvrgw::decode(encoded, tags));
  assert(tags[0].first == "env");
  assert(tags[0].second == "prod");

  kvrgw::ChildValueHeader ch{};
  const auto ct = kvrgw::make_child_value(
      ch, std::string_view(reinterpret_cast<const char *>(encoded.data()),
                           encoded.size()));
  assert(ct.size() == sizeof(kvrgw::ChildValueHeader) + encoded.size());
  kvrgw::ChildValueHeader ch2{};
  assert(kvrgw::parse_child_value_header(ct, ch2));
  assert(!kvrgw::child_flag_shared(ch2.flags));
  assert(ch2.ref_count == 0);
  const auto packed = kvrgw::child_value_payload(ct);
  assert(packed.size() == encoded.size());
}

void test_inline_metadata_frame_roundtrip()
{
  std::array<uint8_t, kvrgw::MAX_META_FRAME_BYTES> encoded{};
  size_t frame_size = 0;
  const kvrgw::MetaPair p{"color", "blue"};
  assert(kvrgw::encode_metadata(std::span<const kvrgw::MetaPair>(&p, 1),
                                encoded, frame_size));

  kvrgw::ObjectValue value;
  std::memset(value.hdr.ref_tag, 0, 12);
  value.hdr.metadata_count = 1;
  value.content_type = "text/plain";
  value.hdr.chunk.type = kvrgw::CHUNK_STORAGE;
  value.metadata_frame.assign(encoded.data(), encoded.data() + frame_size);

  kvrgw::OValueBuf buf;
  kvrgw::ObjectValueHeader wire = value.hdr;
  wire.content_type_len = static_cast<uint8_t>(value.content_type.size());
  kvrgw::hdr_to_be(wire);
  assert(buf.set_header(wire));
  assert(buf.append(value.content_type.data(), value.content_type.size()));
  assert(buf.append(value.metadata_frame.data(), value.metadata_frame.size()));

  const auto parsed = kvrgw::parse_object_value(buf.view());
  assert(parsed);
  assert(parsed->hdr.metadata_count == 1);
  assert(parsed->metadata_frame.size() == frame_size);
  assert(std::memcmp(parsed->metadata_frame.data(), encoded.data(),
                     frame_size) == 0);
  const auto frame = kvrgw::object_inline_metadata_bytes(buf.view());
  assert(frame.size() == frame_size);
  assert(frame.data() + frame.size() ==
         reinterpret_cast<const uint8_t *>(buf.view().data()) +
             buf.view().size());

  std::array<kvrgw::MetaPair, kvrgw::MAX_META_COUNT> meta{};
  assert(kvrgw::decode_metadata(parsed->metadata_frame, meta));
  assert(meta[0].first == "color");
  assert(meta[0].second == "blue");
}

void test_child_d_header_then_data()
{
  const std::string data(32, 'x');
  kvrgw::ChildValueHeader hdr{};
  const auto raw = kvrgw::make_child_value(hdr, data);
  assert(raw.size() == 8 + data.size());
  const auto payload = kvrgw::child_value_payload(raw, data.size());
  assert(payload.size() == data.size());
  assert(payload == data);
  const auto dref = kvrgw::read_d_ref_count(raw);
  assert(!dref.shared);
  assert(dref.ref_count == 0);
  const auto shared = kvrgw::write_d_with_ref(data, 3);
  const auto dref2 = kvrgw::read_d_ref_count(shared);
  assert(dref2.shared);
  assert(dref2.ref_count == 3);
  assert(kvrgw::d_data_portion(shared, data.size()) == data);
}

} // namespace

int main()
{
  test_bucket_value_endianness();
  test_ref_tag_zero_base64_roundtrip();
  test_legacy_bucket_value_ten_bytes();
  test_tenant_value_roundtrip();
  test_bucket_value_versioning_state();
  test_object_value_header_version_fields();
  test_is_delete_marker_flag();
  test_child_value_header_layout();
  test_tag_encode_exact_size();
  test_inline_metadata_frame_roundtrip();
  test_child_d_header_then_data();
  std::cout << "object_value_test passed\n";
  return 0;
}
