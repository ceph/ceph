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

#pragma once

#include "constants.hpp"
#include "typed_ids.hpp"

#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace kvrgw {

enum ChunkType : uint8_t {
  CHUNK_INLINE       = 'I',
  CHUNK_CHILD_D      = 'D',
  CHUNK_CHILD_D_REF  = 'd',
  CHUNK_STORAGE      = 'S',
  CHUNK_STORAGE_REF  = 's',
};

enum ChildValueFlags : uint8_t {
  CHILD_FLAG_SHARED = 1 << 0,  // bit 0
  // bits 1-7 TBD
};

constexpr ChildValueFlags operator|(ChildValueFlags a, ChildValueFlags b) {
  return static_cast<ChildValueFlags>(static_cast<uint8_t>(a) | static_cast<uint8_t>(b));
}
constexpr ChildValueFlags operator&(ChildValueFlags a, ChildValueFlags b) {
  return static_cast<ChildValueFlags>(static_cast<uint8_t>(a) & static_cast<uint8_t>(b));
}
constexpr ChildValueFlags& operator|=(ChildValueFlags& a, ChildValueFlags b) {
  a = a | b;
  return a;
}
constexpr bool child_flag_shared(ChildValueFlags flags) {
  return (static_cast<uint8_t>(flags) & static_cast<uint8_t>(CHILD_FLAG_SHARED)) != 0;
}

#pragma pack(push, 1)

struct ChunkDescriptor {
  ChunkType type{CHUNK_STORAGE};
};

struct ObjectValueHeader {
  uint8_t ref_tag[kRefTagSize]{};
  uint16_t etag_part_count{};
  uint16_t annotations_count{};

  uint8_t etag[kEtagSize]{};

  uint64_t size{};
  uint32_t last_modified_sec{};
  uint32_t last_modified_nsec{};

  ChunkDescriptor chunk;
  uint8_t flags{};
  uint8_t tags_count{};
  uint8_t content_type_len{};

  version_id_t version_id{};
  version_id_t next_vid{};

  uint16_t metadata_count{};
};

struct GcValueHeader {
  ChunkDescriptor chunk;
  uint8_t flags{};
  uint64_t object_size{};
  uint32_t mtime{};
};

struct PoValueHeader {
  uint64_t estimated_size{};
  uint32_t created_at_unix{};
};

struct BucketValueHeader {
  uint8_t bucket_id[8]{};
  int64_t created_at_unix{};
  uint8_t access_flags{};
  VersioningState versioning_state{};
};

struct ChildValueHeader {
  ChildValueFlags flags{};
  uint8_t pad[3]{};
  uint32_t ref_count{};
};

#pragma pack(pop)

static_assert(sizeof(ObjectValueHeader) == 62);
static_assert(sizeof(GcValueHeader) == 14);
static_assert(sizeof(PoValueHeader) == 12);
static_assert(sizeof(BucketValueHeader) == 18);
static_assert(sizeof(ChunkDescriptor) == 1);
static_assert(sizeof(ChildValueHeader) == 8);
static_assert(kMaxOValueBytes > sizeof(ObjectValueHeader));

struct OValueBuf {
  static constexpr size_t kMaxSize = kMaxOValueBytes;
  uint8_t data[kMaxSize];
  size_t len{};

  bool set_header(const ObjectValueHeader& hdr) {
    if (sizeof(hdr) > kMaxSize) {
      return false;
    }
    std::memcpy(data, &hdr, sizeof(hdr));
    len = sizeof(hdr);
    return true;
  }

  bool append(const void* src, size_t n) {
    if (len + n > kMaxSize) {
      return false;
    }
    std::memcpy(data + len, src, n);
    len += n;
    return true;
  }

  std::string_view view() const {
    return {reinterpret_cast<const char*>(data), len};
  }
};

struct ObjectValue {
  ObjectValueHeader hdr{};
  std::string content_type;
  std::vector<uint8_t> inline_data;
  std::vector<uint8_t> metadata_frame;

  static constexpr uint8_t kFlagExtendedAttrs        = 0x01;
  static constexpr uint8_t kFlagFenced               = 0x02;
  static constexpr uint8_t kFlagSharedData           = 0x04;
  static constexpr uint8_t kFlagExternalTags         = 0x08;
  static constexpr uint8_t kFlagExternalAnnotations  = 0x10;

  bool has_extended_attrs() const { return (hdr.flags & kFlagExtendedAttrs) != 0; }
  bool has_annotations() const { return hdr.annotations_count > 0; }
  bool has_external_annotations() const { return (hdr.flags & kFlagExternalAnnotations) && hdr.annotations_count > 0; }
  bool has_metadata() const { return hdr.metadata_count > 0; }
  bool is_delete_marker() const { return (hdr.flags & kFlagFenced) != 0; }
  bool has_shared_data() const { return (hdr.flags & kFlagSharedData) != 0; }
  bool has_external_tags() const { return (hdr.flags & kFlagExternalTags) != 0; }
  bool has_data() const { return hdr.size > 0 || hdr.chunk.type != CHUNK_INLINE; }

  etag_t get_etag() const {
    etag_t e;
    e.deserialize(hdr.etag);
    e.set_part_count(hdr.etag_part_count);
    return e;
  }
  void set_etag(const etag_t& e) {
    e.serialize(hdr.etag);
    hdr.etag_part_count = e.part_count();
  }
  void set_etag_raw(const uint8_t* digest, uint16_t part_count = 0) {
    std::memcpy(hdr.etag, digest, kEtagSize);
    hdr.etag_part_count = part_count;
  }

  std::string etag_display() const;

  bucket_id_t chunk_data_bucket_id{};
  uint8_t chunk_data_ref_tag[kRefTagSize]{};
};

struct BucketValue {
  bucket_id_t bucket_id{};
  int64_t created_at_unix{};
  uint8_t access_flags{};
  VersioningState versioning_state{};
  std::string policy_json;
};

std::optional<ObjectValue> parse_object_value(std::string_view data);
std::span<const uint8_t> object_inline_metadata_bytes(std::string_view data);

void child_hdr_to_be(ChildValueHeader& hdr);
void child_hdr_from_be(ChildValueHeader& hdr);
std::string make_child_value(ChildValueHeader hdr, std::string_view payload);
bool parse_child_value_header(std::string_view data, ChildValueHeader& out);
std::string_view child_value_payload(std::string_view data);
std::string_view child_value_payload(std::string_view data, uint64_t payload_size);

class KvTransaction;
void decrement_or_del_child_d(KvTransaction& tr, std::string_view d_key,
                              uint64_t object_size, bool shared);

std::string make_bucket_value(bucket_id_t bucket_id, int64_t created_at_unix,
                              uint8_t access_flags = 0, VersioningState versioning_state = VERSIONING_DISABLED,
                              std::string_view policy_json = "");
std::optional<BucketValue> parse_bucket_value(std::string_view data);

void hdr_to_be(ObjectValueHeader& hdr);
void hdr_from_be(ObjectValueHeader& hdr);

}  // namespace kvrgw
