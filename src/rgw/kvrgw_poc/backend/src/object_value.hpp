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
#include "etag.hpp"
#include "ref_tag.hpp"
#include "typed_ids.hpp"

#include <arpa/inet.h>
#include <cstdint>
#include <cstring>
#include <endian.h>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace kvrgw {

enum ChunkType : uint8_t {
  CHUNK_EMPTY        = 'E',     // used by delete_markers
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

// Flags for ChunkDescriptor. Each bit is a named constant below.
// No bits defined yet — placeholder for future flags (e.g. encryption, compression).
class __attribute__((packed)) ChunkDescriptorFlags {
 public:
  ChunkDescriptorFlags() = default;
  explicit constexpr ChunkDescriptorFlags(uint8_t v) : val_(v) {}

  constexpr ChunkDescriptorFlags operator|(ChunkDescriptorFlags b) const {
    return ChunkDescriptorFlags(val_ | b.val_);
  }
  constexpr ChunkDescriptorFlags operator&(ChunkDescriptorFlags b) const {
    return ChunkDescriptorFlags(val_ & b.val_);
  }
  constexpr ChunkDescriptorFlags& operator|=(ChunkDescriptorFlags b) {
    val_ |= b.val_;
    return *this;
  }
  constexpr bool any() const { return val_ != 0; }
  constexpr uint8_t raw() const { return val_; }

 private:
  uint8_t val_{0};
};
static_assert(sizeof(ChunkDescriptorFlags) == 1);
// Future: constexpr ChunkDescriptorFlags kChunkFlagEncrypted{0x01};
//         constexpr ChunkDescriptorFlags kChunkFlagCompressed{0x02};

#pragma pack(push, 1)

// Self-sufficient data-location descriptor for one object's data.
// Carries everything needed to locate, reference-count, and clean up the data,
// regardless of which tier it lives on. Future fields (encryption_key,
// compression_alg) will be added in an extended subtype.
//
// part_count is stored internally as BE to avoid any conversion on serialize.
class ChunkDescriptor {
 public:
  ChunkDescriptor() = default;

  // Primary constructor: all identity fields supplied by the caller.
  ChunkDescriptor(ChunkType t, const RefTag& rt, bucket_id_t bid,
                  uint16_t pc = 0,
                  ChunkDescriptorFlags f = ChunkDescriptorFlags{})
      : flags_(f), type_(t), part_count_be_(htons(pc)),
        bucket_id_(bid), ref_tag_(rt) {}

  // Construct from a raw ref_tag byte pointer (string_view of kRefTagSize).
  ChunkDescriptor(ChunkType t, const char* rt_data, bucket_id_t bid,
                  uint16_t pc = 0,
                  ChunkDescriptorFlags f = ChunkDescriptorFlags{})
      : flags_(f), type_(t), part_count_be_(htons(pc)), bucket_id_(bid) {
    ref_tag_.load(rt_data);
  }

  ChunkType         get_type()      const { return type_; }
  const RefTag&     get_ref_tag()   const { return ref_tag_; }
  const bucket_id_t& get_bucket_id() const { return bucket_id_; }
  std::string_view  ref_tag_view()   const { return ref_tag_.view(); }
  std::string       ref_tag_filename() const { return ref_tag_.filename(); }
  std::string_view  bucket_id_view() const { return bucket_id_.view(); }

  // Serialize to dst (must have sizeof(ChunkDescriptor) bytes available).
  // Wire order: flags, type, part_count BE, bucket_id, ref_tag.
  void serialize(uint8_t* dst) const {
    dst[offsetof(ChunkDescriptor, flags_)] = flags_.raw();
    dst[offsetof(ChunkDescriptor, type_)]  = static_cast<uint8_t>(type_);
    std::memcpy(dst + offsetof(ChunkDescriptor, part_count_be_), &part_count_be_, sizeof(part_count_be_));
    std::memcpy(dst + offsetof(ChunkDescriptor, bucket_id_), bucket_id_.view().data(), kBucketIdSize);
    std::memcpy(dst + offsetof(ChunkDescriptor, ref_tag_), ref_tag_.view().data(), kRefTagSize);
  }

  // Load from src (must have sizeof(ChunkDescriptor) bytes available).
  void load(const uint8_t* src) {
    flags_ = ChunkDescriptorFlags(src[offsetof(ChunkDescriptor, flags_)]);
    type_  = static_cast<ChunkType>(src[offsetof(ChunkDescriptor, type_)]);
    std::memcpy(&part_count_be_, src + offsetof(ChunkDescriptor, part_count_be_), sizeof(part_count_be_));
    bucket_id_.load(src + offsetof(ChunkDescriptor, bucket_id_));
    ref_tag_.load(reinterpret_cast<const char*>(src + offsetof(ChunkDescriptor, ref_tag_)));
  }

 private:
  ChunkDescriptorFlags flags_{};
  ChunkType            type_{CHUNK_STORAGE};
  uint16_t             part_count_be_{};   // stored as big-endian
  bucket_id_t          bucket_id_{};
  RefTag               ref_tag_{};
};
static_assert(sizeof(ChunkDescriptor) == 24);

#pragma pack(pop)

#pragma pack(push, 1)

struct ObjectValue;
struct ObjectValueHeader {
  RefTag ref_tag{};
  ETag etag{};
  uint16_t annotations_count{};

  uint64_t size{};
  uint32_t last_modified_sec{};
  uint32_t last_modified_nsec{};

  ChunkType chunk_type{CHUNK_STORAGE};
  uint8_t flags{};
  uint8_t tags_count{};
  uint8_t content_type_len{};

  version_id_t version_id{};
  version_id_t next_vid{};

  uint16_t metadata_count{};
};

struct GcValueHeader {
  ChunkType chunk_type{CHUNK_STORAGE};
  uint8_t   flags{};
  uint8_t   pad[2]{};
};

struct BucketValueHeader {
  bucket_id_t bucket_id{};
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
static_assert(sizeof(GcValueHeader) == 4);
static_assert(sizeof(BucketValueHeader) == 18);
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

  bool append(std::string_view v) {
    return append(v.data(), v.size());
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
  bool has_data() const { return hdr.size > 0 || hdr.chunk_type != CHUNK_INLINE; }

  // Returns the ref_tag view for data access:
  // chunk.ref_tag for REF chunk types, hdr.ref_tag otherwise.
  std::string_view data_ref_view() const {
    if (hdr.chunk_type == CHUNK_CHILD_D_REF ||
        hdr.chunk_type == CHUNK_STORAGE_REF) {
      return chunk.ref_tag_view();
    }
    return hdr.ref_tag.view();
  }

  const ETag& get_etag() const { return hdr.etag; }
  void set_etag(const ETag& e) { hdr.etag = e; }
  void set_etag_raw(const uint8_t* digest, uint16_t part_count = 0) {
    hdr.etag.load_raw(digest, part_count);
  }

  std::string etag_display() const;

  // Full data-location descriptor (in-memory only — not in the packed header).
  // chunk.type mirrors hdr.chunk_type; chunk.ref_tag and chunk.bucket_id are
  // populated from the variable-length wire tail for REF chunk types.
  ChunkDescriptor chunk{};
};

struct BucketValue {
  bucket_id_t bucket_id{};
  int64_t created_at_unix{};
  uint8_t access_flags{};
  VersioningState versioning_state{};
  std::string policy_json;
};

std::optional<ObjectValue> parse_object_value(std::string_view data);

// Zero-copy: validate and return a pointer directly into the FDB buffer.
// Returns nullptr if data is too short or chunk type is invalid.
// The pointer is valid for the lifetime of `data`.
// All multi-byte fields are still big-endian — use the ovh_* accessors below.
inline const ObjectValueHeader* ovh_ptr(std::string_view data)
{
  if (data.size() < sizeof(ObjectValueHeader)) {
    return nullptr;
  }
  const auto* hdr = reinterpret_cast<const ObjectValueHeader*>(data.data());
  const auto ct = hdr->chunk_type;
  if (ct != CHUNK_INLINE && ct != CHUNK_CHILD_D && ct != CHUNK_CHILD_D_REF &&
      ct != CHUNK_STORAGE && ct != CHUNK_STORAGE_REF) {
    return nullptr;
  }
  return hdr;
}

// Accessors that apply byte-swap on the raw big-endian header pointer.
inline bool ovh_is_delete_marker(const ObjectValueHeader* h)
{
  return (h->flags & ObjectValue::kFlagFenced) != 0;
}
inline uint64_t ovh_size(const ObjectValueHeader* h)
{
  return be64toh(h->size);
}
inline int64_t ovh_last_modified_sec(const ObjectValueHeader* h)
{
  return static_cast<int64_t>(be32toh(h->last_modified_sec));
}
inline version_id_t ovh_version_id(const ObjectValueHeader* h)
{
  return h->version_id.from_be();
}
inline version_id_t ovh_next_vid(const ObjectValueHeader* h)
{
  return h->next_vid.from_be();
}
// For CHUNK_CHILD_D_REF / CHUNK_STORAGE_REF: extract the extra ref fields
// that follow the header + content_type in the wire format.
// Returns false if the buffer is too short.
struct OvhChunkRef {
  bucket_id_t bucket_id{};           // only valid for CHUNK_CHILD_D_REF
  std::string_view ref_tag{};        // points into the raw buffer
};
inline bool ovh_chunk_ref(std::string_view raw, const ObjectValueHeader* h,
                           OvhChunkRef& out)
{
  const size_t base = sizeof(ObjectValueHeader) + h->content_type_len;
  if (h->chunk_type == CHUNK_CHILD_D_REF) {
    const size_t need = base + kBucketIdSize + kRefTagSize;
    if (raw.size() < need) {
      return false;
    }
    out.bucket_id.load(raw.data() + base);
    out.ref_tag = raw.substr(base + kBucketIdSize, kRefTagSize);
    return true;
  }
  if (h->chunk_type == CHUNK_STORAGE_REF) {
    const size_t need = base + kRefTagSize;
    if (raw.size() < need) {
      return false;
    }
    out.ref_tag = raw.substr(base, kRefTagSize);
    return true;
  }
  return false;
}

std::string ovh_etag_display(const ObjectValueHeader* h);
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

// Zero-copy: validate and return a pointer directly into the FDB buffer.
// Returns nullptr if data is too short.
inline const BucketValueHeader* bvh_ptr(std::string_view data)
{
  if (data.size() < sizeof(BucketValueHeader)) [[unlikely]] {
    return nullptr;
  }
  return reinterpret_cast<const BucketValueHeader*>(data.data());
}

inline int64_t bvh_created_at_unix(const BucketValueHeader* h)
{
  return static_cast<int64_t>(be64toh(static_cast<uint64_t>(h->created_at_unix)));
}
inline bucket_id_t bvh_bucket_id(const BucketValueHeader* h)
{
  // bucket_id is stored BE — return directly
  return h->bucket_id;
}
inline uint8_t bvh_access_flags(const BucketValueHeader* h)
{
  return h->access_flags;
}
inline VersioningState bvh_versioning_state(const BucketValueHeader* h)
{
  return h->versioning_state;
}

void hdr_to_be(ObjectValueHeader& hdr);
void hdr_from_be(ObjectValueHeader& hdr);

}  // namespace kvrgw
