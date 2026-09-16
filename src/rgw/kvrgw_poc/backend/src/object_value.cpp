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

#include "object_value.hpp"

#include "id_meta.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <cassert>
#include <cstring>
#include <endian.h>
#include <span>
#include <string>

namespace kvrgw {

void hdr_to_be(ObjectValueHeader &hdr)
{
  hdr.etag_part_count = htons(hdr.etag_part_count);
  hdr.annotations_count = htons(hdr.annotations_count);
  hdr.size = htobe64(hdr.size);
  hdr.last_modified_sec = htonl(hdr.last_modified_sec);
  hdr.last_modified_nsec = htonl(hdr.last_modified_nsec);
  hdr.version_id = version_id_t{htonl(hdr.version_id.raw())};
  hdr.next_vid = version_id_t{htonl(hdr.next_vid.raw())};
  hdr.metadata_count = htons(hdr.metadata_count);
}

void hdr_from_be(ObjectValueHeader &hdr)
{
  hdr.etag_part_count = ntohs(hdr.etag_part_count);
  hdr.annotations_count = ntohs(hdr.annotations_count);
  hdr.size = be64toh(hdr.size);
  hdr.last_modified_sec = ntohl(hdr.last_modified_sec);
  hdr.last_modified_nsec = ntohl(hdr.last_modified_nsec);
  hdr.version_id = version_id_t{ntohl(hdr.version_id.raw())};
  hdr.next_vid = version_id_t{ntohl(hdr.next_vid.raw())};
  hdr.metadata_count = ntohs(hdr.metadata_count);
}

namespace {

std::string etag_to_hex(const uint8_t *etag, size_t len)
{
  static const char kHex[] = "0123456789abcdef";
  std::string out;
  out.reserve(len * 2);
  for (size_t i = 0; i < len; ++i) {
    out.push_back(kHex[etag[i] >> 4]);
    out.push_back(kHex[etag[i] & 0x0F]);
  }
  return out;
}

} // namespace

std::string ObjectValue::etag_display() const { return get_etag().to_hex(); }

namespace {

size_t object_payload_offset(const ObjectValueHeader &hdr,
                             std::string_view data)
{
  size_t tail_offset = sizeof(ObjectValueHeader) + hdr.content_type_len;
  if (hdr.chunk.type == CHUNK_INLINE && hdr.size > 0) {
    const size_t available =
        data.size() > tail_offset ? data.size() - tail_offset : 0;
    const size_t inline_len =
        std::min(static_cast<size_t>(hdr.size), available);
    tail_offset += inline_len;
  }
  if (hdr.chunk.type == CHUNK_CHILD_D_REF) {
    tail_offset += 8 + kRefTagSize;
  }
  else if (hdr.chunk.type == CHUNK_STORAGE_REF) {
    tail_offset += kRefTagSize;
  }
  return tail_offset;
}

} // namespace

std::span<const uint8_t> object_inline_metadata_bytes(std::string_view data)
{
  if (data.size() < sizeof(ObjectValueHeader)) {
    return {};
  }
  ObjectValueHeader hdr{};
  std::memcpy(&hdr, data.data(), sizeof(ObjectValueHeader));
  hdr_from_be(hdr);
  if (hdr.metadata_count == 0) {
    return {};
  }
  const size_t off = object_payload_offset(hdr, data);
  if (off > data.size()) {
    return {};
  }
  const std::span<const uint8_t> rest(
      reinterpret_cast<const uint8_t *>(data.data() + off), data.size() - off);
  size_t frame_size = 0;
  if (!encoded_metadata_frame_size(rest, frame_size)) {
    return {};
  }
  if (read_be_field<tag_count_t>(rest.data()) != hdr.metadata_count) {
    return {};
  }
  return {rest.data(), frame_size};
}

std::optional<ObjectValue> parse_object_value(std::string_view data)
{
  if (data.size() < sizeof(ObjectValueHeader)) {
    return std::nullopt;
  }

  ObjectValue value;
  std::memcpy(&value.hdr, data.data(), sizeof(ObjectValueHeader));
  hdr_from_be(value.hdr);

  const char ct_byte = static_cast<char>(value.hdr.chunk.type);
  if (ct_byte != CHUNK_INLINE && ct_byte != CHUNK_CHILD_D &&
      ct_byte != CHUNK_CHILD_D_REF && ct_byte != CHUNK_STORAGE &&
      ct_byte != CHUNK_STORAGE_REF) {
    return std::nullopt;
  }

  const uint8_t ct_len = value.hdr.content_type_len;
  if (data.size() < sizeof(ObjectValueHeader) + ct_len) {
    return std::nullopt;
  }

  value.content_type.assign(data.data() + sizeof(ObjectValueHeader), ct_len);

  size_t tail_offset = sizeof(ObjectValueHeader) + ct_len;

  if (value.hdr.chunk.type == CHUNK_INLINE && value.hdr.size > 0) {
    const size_t available = data.size() - tail_offset;
    const size_t inline_len =
        std::min(static_cast<size_t>(value.hdr.size), available);
    const auto *p =
        reinterpret_cast<const uint8_t *>(data.data() + tail_offset);
    value.inline_data.assign(p, p + inline_len);
    tail_offset += inline_len;
  }

  if (value.hdr.chunk.type == CHUNK_CHILD_D_REF) {
    if (data.size() < tail_offset + 8 + kRefTagSize) {
      return std::nullopt;
    }
    uint64_t bid_be;
    std::memcpy(&bid_be, data.data() + tail_offset, 8);
    value.chunk_data_bucket_id = be64toh(bid_be);
    tail_offset += 8;
    std::memcpy(value.chunk_data_ref_tag, data.data() + tail_offset,
                kRefTagSize);
    tail_offset += kRefTagSize;
  }
  else if (value.hdr.chunk.type == CHUNK_STORAGE_REF) {
    if (data.size() < tail_offset + kRefTagSize) {
      return std::nullopt;
    }
    std::memcpy(value.chunk_data_ref_tag, data.data() + tail_offset,
                kRefTagSize);
    tail_offset += kRefTagSize;
  }

  if (value.hdr.metadata_count > 0) {
    const size_t remaining = data.size() - tail_offset;
    const std::span<const uint8_t> rest(
        reinterpret_cast<const uint8_t *>(data.data() + tail_offset),
        remaining);
    size_t frame_size = 0;
    if (!encoded_metadata_frame_size(rest, frame_size)) {
      return std::nullopt;
    }
    if (read_be_field<tag_count_t>(rest.data()) != value.hdr.metadata_count) {
      return std::nullopt;
    }
    value.metadata_frame.assign(rest.data(), rest.data() + frame_size);
  }

  return value;
}

void child_hdr_to_be(ChildValueHeader &hdr)
{
  hdr.ref_count = htonl(hdr.ref_count);
}

void child_hdr_from_be(ChildValueHeader &hdr)
{
  hdr.ref_count = ntohl(hdr.ref_count);
}

std::string make_child_value(ChildValueHeader hdr, std::string_view payload)
{
  child_hdr_to_be(hdr);
  std::string out;
  out.reserve(sizeof(hdr) + payload.size());
  out.append(reinterpret_cast<const char *>(&hdr), sizeof(hdr));
  out.append(payload);
  return out;
}

bool parse_child_value_header(std::string_view data, ChildValueHeader &out)
{
  if (data.size() < sizeof(ChildValueHeader)) {
    return false;
  }
  std::memcpy(&out, data.data(), sizeof(ChildValueHeader));
  child_hdr_from_be(out);
  return true;
}

std::string_view child_value_payload(std::string_view data)
{
  if (data.size() < sizeof(ChildValueHeader)) {
    return {};
  }
  return data.substr(sizeof(ChildValueHeader));
}

std::string_view child_value_payload(std::string_view data,
                                     uint64_t payload_size)
{
  if (data.size() < sizeof(ChildValueHeader) + payload_size) {
    return {};
  }
  return data.substr(sizeof(ChildValueHeader), payload_size);
}

std::string make_bucket_value(bucket_id_t bucket_id, int64_t created_at_unix,
                              uint8_t access_flags,
                              VersioningState versioning_state,
                              std::string_view policy_json)
{
  BucketValueHeader hdr{};
  uint64_t bid_be = htobe64(bucket_id);
  std::memcpy(hdr.bucket_id, &bid_be, sizeof(hdr.bucket_id));
  hdr.created_at_unix =
      static_cast<int64_t>(htobe64(static_cast<uint64_t>(created_at_unix)));
  hdr.access_flags = access_flags;
  hdr.versioning_state = versioning_state;
  std::string out;
  out.append(reinterpret_cast<const char *>(&hdr), sizeof(hdr));
  out.append(policy_json);
  return out;
}

std::optional<BucketValue> parse_bucket_value(std::string_view data)
{
  if (data.size() < 8) {
    return std::nullopt;
  }
  BucketValue value;
  uint64_t be;
  std::memcpy(&be, data.data(), 8);
  value.bucket_id = be64toh(be);
  if (data.size() >= 16) {
    BucketValueHeader hdr{};
    std::memcpy(&hdr, data.data(), std::min(data.size(), sizeof(hdr)));
    value.created_at_unix = static_cast<int64_t>(
        be64toh(static_cast<uint64_t>(hdr.created_at_unix)));
    if (data.size() >= 17) {
      value.access_flags = hdr.access_flags;
    }
    if (data.size() >= sizeof(BucketValueHeader)) {
      value.versioning_state = hdr.versioning_state;
      if (data.size() > sizeof(BucketValueHeader)) {
        value.policy_json.assign(data.data() + sizeof(BucketValueHeader),
                                 data.size() - sizeof(BucketValueHeader));
      }
    }
  }
  else {
    value.created_at_unix = 0;
  }
  return value;
}

} // namespace kvrgw
