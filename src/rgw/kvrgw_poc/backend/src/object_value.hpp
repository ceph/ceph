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

#include "checksum.hpp"
#include "constants.hpp"
#include "etag.hpp"
#include "gm_value.hpp"
#include "id_meta.hpp"
#include "ref_tag.hpp"
#include "typed_ids.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <endian.h>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>

namespace kvrgw {

// See plans/o_value_layout.md for the full :O: layout.

enum ChunkType : uint8_t {
  CHUNK_EMPTY        = 'E',     // used by delete_markers
  CHUNK_INLINE       = 'I',
  CHUNK_CHILD_D      = 'D',
  CHUNK_CHILD_D_REF  = 'd',
  CHUNK_STORAGE      = 'S',
  CHUNK_STORAGE_REF  = 's',
  // A completed multipart object -- data lives in N separate part files
  // (Tier 3, "ref_tag-part_number-generation" each), not one contiguous
  // blob. The manifest (a ready-made G:M{FREE_PARTS} value, see gm_value.hpp)
  // describing which parts/generations/sizes make up the object lives where
  // ObjectValue::manifest_loc says (inline, C:E or C:M).
  CHUNK_MULTIPART    = 'P',
  // Zero-copy-shared copy of a CHUNK_MULTIPART object (server-side copy
  // of a completed multipart upload). hdr.ref_tag is this object's own
  // identity (tags/metadata namespace); the *shared* identity lives in
  // chunk.ref_tag.
  CHUNK_MULTIPART_REF = 'p',
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

#pragma pack(push, 1)

// Self-sufficient data-location descriptor for one object's data.
// Carries everything needed to locate, reference-count, and clean up the data,
// regardless of which tier it lives on. Present on the wire for every object
// type (plans/o_value_layout.md).
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

// ObjectValueHeader.flags (uint16_t).  0x0001 is reserved.
inline constexpr uint16_t kFlagFenced              = 0x0002;  // delete marker
inline constexpr uint16_t kFlagSharedData          = 0x0004;  // shared ref-counted data
inline constexpr uint16_t kFlagExternalTags        = 0x0008;  // tags in C:T
inline constexpr uint16_t kFlagExternalAnnotations = 0x0010;  // annotations in C:A
inline constexpr uint16_t kFlagDataInChildE        = 0x0020;  // single-part data in C:E
inline constexpr uint16_t kFlagSharedTags          = 0x0040;  // C:T is shared
inline constexpr uint16_t kFlagExternalMetadata    = 0x0080;  // metadata_frame in C:E
inline constexpr uint16_t kFlagReservedMask        =
    static_cast<uint16_t>(~(kFlagFenced | kFlagSharedData | kFlagExternalTags |
                            kFlagExternalAnnotations | kFlagDataInChildE |
                            kFlagSharedTags | kFlagExternalMetadata));

// Multipart header byte 0 high nibble: where the per-part checksum blob lives.
enum CksumLocation : uint8_t {
  kCksumInline = 0,  // inline in :O: (only copy), or absent when alg == NONE
  kCksumChildS = 1,  // authoritative copy in C:S
  kCksumFile   = 2,  // authoritative copy in <ref_tag>_checksum file
};
// Multipart header byte 0 low nibble: where the manifest lives.
enum ManifestLocation : uint8_t {
  kManifestInline = 0,  // inline in :O: (only copy)
  kManifestChildE = 1,  // piggybacked in C:E (unshared only)
  kManifestChildM = 2,  // authoritative copy in C:M
};

// Multipart header byte 1: mflags.
inline constexpr uint8_t kMFlagComposite     = 0x02;
inline constexpr uint8_t kMFlagFirstPartInfo = 0x04;
inline constexpr uint8_t kMFlagManifestCached = 0x08;
inline constexpr uint8_t kMFlagCksumCached   = 0x10;
inline constexpr uint8_t kMFlagReservedMask  =
    static_cast<uint8_t>(~(kMFlagComposite | kMFlagFirstPartInfo |
                           kMFlagManifestCached | kMFlagCksumCached));

// Multipart-only header: [cksum_location:4 | manifest_location:4] + mflags.
inline constexpr size_t kMultipartHdrBytes = 2;

// Lowest-numbered included part, stored when no local manifest is kept.
// Wire: part_num 2B BE, gen_id 2B BE, part_size 5B BE.
inline constexpr size_t kFirstPartInfoBytes = 2 + 2 + 5;
struct FirstPartInfo {
  uint16_t part_number{};
  uint16_t generation{};
  uint64_t size{};  // < 2^40

  void encode(uint8_t* dst) const {
    dst[0] = static_cast<uint8_t>(part_number >> 8);
    dst[1] = static_cast<uint8_t>(part_number);
    dst[2] = static_cast<uint8_t>(generation >> 8);
    dst[3] = static_cast<uint8_t>(generation);
    for (int i = 0; i < 5; ++i) {
      dst[4 + i] = static_cast<uint8_t>(size >> (8 * (4 - i)));
    }
  }
  static FirstPartInfo decode(const uint8_t* src) {
    FirstPartInfo f;
    f.part_number = static_cast<uint16_t>((src[0] << 8) | src[1]);
    f.generation = static_cast<uint16_t>((src[2] << 8) | src[3]);
    for (int i = 0; i < 5; ++i) {
      f.size = (f.size << 8) | src[4 + i];
    }
    return f;
  }
};

#pragma pack(push, 1)

// 64 B. Field order keeps `size` 8-byte aligned (offset 8).
struct ObjectValueHeader {
  uint16_t flags{};
  ChunkType chunk_type{CHUNK_STORAGE};
  uint8_t content_type_len{};
  uint8_t tags_count{};
  uint16_t metadata_count{};
  uint8_t cksum_alg_byte{};   // cksum_alg:4 (high) | reserved:4 (low)

  uint64_t size{};
  uint32_t last_modified_sec{};
  uint32_t last_modified_nsec{};

  version_id_t version_id{};
  version_id_t next_vid{};

  RefTag ref_tag{};
  ETag etag{};
  uint16_t annotations_count{};

  CksumAlg get_cksum_alg() const {
    return static_cast<CksumAlg>(cksum_alg_byte >> 4);
  }
  void set_cksum_alg(CksumAlg a) {
    cksum_alg_byte = static_cast<uint8_t>(static_cast<uint8_t>(a) << 4);
  }
};

struct GcValueHeader {
  ChunkType chunk_type{CHUNK_STORAGE};
  uint8_t   flags{};   // kGcFlag* below (narrowed from ObjectValueHeader.flags)
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

static_assert(sizeof(ObjectValueHeader) == 64);
static_assert(offsetof(ObjectValueHeader, cksum_alg_byte) == 7);
static_assert(offsetof(ObjectValueHeader, size) == 8);
static_assert(sizeof(GcValueHeader) == 4);
static_assert(sizeof(BucketValueHeader) == 18);
static_assert(sizeof(ChildValueHeader) == 8);
static_assert(kMaxOValueBytes > sizeof(ObjectValueHeader));
static_assert(kCksumAlgMax <= 15);

// GcValueHeader.flags
inline constexpr uint8_t kGcFlagShared           = 0x01;  // kFlagSharedData
inline constexpr uint8_t kGcFlagExternalChildren = 0x02;  // object owns C: children

// The single :O: buffer for every object type (hard cap kMaxOValueBytes).
struct OValueBuf {
  static constexpr size_t kMaxSize = kMaxOValueBytes;
  uint8_t data[kMaxSize];
  size_t len{};

  bool set_header(const ObjectValueHeader& hdr) {
    std::memcpy(data, &hdr, sizeof(hdr));
    len = sizeof(hdr);
    return true;
  }

  bool append(const void* src, size_t n) {
    if (len + n > kMaxSize) {
      return false;
    }
    if (n) {
      std::memcpy(data + len, src, n);
    }
    len += n;
    return true;
  }

  bool append(std::string_view v) { return append(v.data(), v.size()); }
  bool append(std::span<const uint8_t> v) { return append(v.data(), v.size()); }

  std::string_view view() const {
    return {reinterpret_cast<const char*>(data), len};
  }
};

// Non-owning in-memory form of one :O: value. Every variable-length member is
// a view; whoever builds it (parser: the raw FDB value; writers: their own
// stack buffers) keeps the bytes alive. hdr is in host byte order.
struct ObjectValue {
  ObjectValueHeader hdr{};
  ChunkDescriptor chunk{};          // on the wire for every type
  std::string_view content_type;
  // Inline metadata frame; empty when kFlagExternalMetadata (it is in C:E).
  std::span<const uint8_t> metadata_frame;
  // Single-part only: inline data (CHUNK_INLINE, hdr.size bytes).
  std::span<const uint8_t> inline_data;

  // Multipart only.
  uint8_t cksum_loc{kCksumInline};
  uint8_t manifest_loc{kManifestInline};
  uint8_t mflags{};
  FirstPartInfo first_part{};                // valid iff kMFlagFirstPartInfo
  // Local manifest copy (G:M{FREE_PARTS} value): present when manifest_loc ==
  // kManifestInline or kMFlagManifestCached; empty otherwise.
  std::string_view manifest_blob;
  // Local per-part checksum blob copy: present when cksum_loc == kCksumInline
  // (and alg != NONE) or kMFlagCksumCached; empty otherwise.
  std::string_view cksum_blob;

  // Combined digest, first cksum_digest_len(alg) bytes (all object types).
  std::array<uint8_t, kCksumMaxDigestBytes> cksum_combined{};

  static constexpr uint16_t kFlagFenced              = kvrgw::kFlagFenced;
  static constexpr uint16_t kFlagSharedData          = kvrgw::kFlagSharedData;
  static constexpr uint16_t kFlagExternalTags        = kvrgw::kFlagExternalTags;
  static constexpr uint16_t kFlagExternalAnnotations = kvrgw::kFlagExternalAnnotations;
  static constexpr uint16_t kFlagDataInChildE        = kvrgw::kFlagDataInChildE;
  static constexpr uint16_t kFlagSharedTags          = kvrgw::kFlagSharedTags;
  static constexpr uint16_t kFlagExternalMetadata    = kvrgw::kFlagExternalMetadata;

  bool has_annotations() const { return hdr.annotations_count > 0; }
  bool has_external_annotations() const { return (hdr.flags & kFlagExternalAnnotations) && hdr.annotations_count > 0; }
  bool has_metadata() const { return hdr.metadata_count > 0; }
  bool has_external_metadata() const { return (hdr.flags & kFlagExternalMetadata) != 0; }
  bool is_delete_marker() const { return (hdr.flags & kFlagFenced) != 0; }
  bool has_shared_data() const { return (hdr.flags & kFlagSharedData) != 0; }
  bool has_external_tags() const { return (hdr.flags & kFlagExternalTags) != 0; }
  bool has_data() const { return hdr.size > 0 || hdr.chunk_type != CHUNK_INLINE; }

  bool is_multipart() const {
    return hdr.chunk_type == CHUNK_MULTIPART || hdr.chunk_type == CHUNK_MULTIPART_REF;
  }
  CksumAlg cksum_alg() const { return hdr.get_cksum_alg(); }
  // Single-part is always FULL_OBJECT; multipart carries kMFlagComposite.
  CksumType cksum_type() const {
    if (cksum_alg() == CksumAlg::NONE) {
      return CksumType::NONE;
    }
    return (is_multipart() && (mflags & kMFlagComposite)) ? CksumType::COMPOSITE
                                                          : CksumType::FULL_OBJECT;
  }
  size_t cksum_combined_len() const { return cksum_digest_len(cksum_alg()); }
  std::span<const uint8_t> cksum_digest() const {
    return {cksum_combined.data(), cksum_combined_len()};
  }
  uint16_t part_count() const { return hdr.etag.part_count(); }
  // Valid ?partNumber= range is [1, max(1, part_count())].
  uint16_t max_part_number() const {
    const uint16_t pc = part_count();
    return pc ? pc : 1;
  }
  bool has_local_manifest() const {
    return manifest_loc == kManifestInline || (mflags & kMFlagManifestCached);
  }
  bool has_local_cksum_blob() const {
    return cksum_alg() != CksumAlg::NONE &&
           (cksum_loc == kCksumInline || (mflags & kMFlagCksumCached));
  }
  size_t cksum_blob_len() const {
    return static_cast<size_t>(part_count()) * cksum_combined_len();
  }

  bool is_ref() const {
    return hdr.chunk_type == CHUNK_CHILD_D_REF ||
           hdr.chunk_type == CHUNK_STORAGE_REF ||
           hdr.chunk_type == CHUNK_MULTIPART_REF;
  }

  // Returns the ref_tag view for data access:
  // chunk.ref_tag for REF chunk types, hdr.ref_tag otherwise.
  std::string_view data_ref_view() const {
    return is_ref() ? chunk.ref_tag_view() : hdr.ref_tag.view();
  }
  const RefTag& data_ref_tag() const {
    return is_ref() ? chunk.get_ref_tag() : hdr.ref_tag;
  }
  // Bucket that owns the shared data / C:M / C:S: the chunk's for REF
  // objects, the object's own bucket otherwise.
  const bucket_id_t& data_bucket(const bucket_id_t& own) const {
    return is_ref() ? chunk.get_bucket_id() : own;
  }
  // Non-REF objects own their data: the descriptor points at themselves.
  void set_self_chunk(const bucket_id_t& own) {
    chunk = ChunkDescriptor(hdr.chunk_type, hdr.ref_tag, own);
  }

  const ETag& get_etag() const { return hdr.etag; }
  void set_etag(const ETag& e) { hdr.etag = e; }
  void set_etag_raw(const uint8_t* digest, uint16_t part_count = 0) {
    hdr.etag.load_raw(digest, part_count);
  }

  void set_cksum_combined(std::span<const uint8_t> digest) {
    cksum_combined = {};
    std::memcpy(cksum_combined.data(), digest.data(),
                std::min(digest.size(), cksum_combined.size()));
  }

  std::string etag_display() const;
};

// An ObjectValue plus the raw FDB value its views point into. Movable (the
// raw value is always larger than the SSO capacity, so a move keeps the heap
// buffer and the views stay valid); not copyable. Copying the ObjectValue
// base (slicing) gives a non-owning view.
struct OwnedObjectValue : ObjectValue {
  std::string raw;
  // Child values read for this object. The ext_* views point into them and
  // the base ObjectValue stays exactly the wire form (so it can be rewritten).
  // (Both are far larger than the SSO capacity whenever they are non-empty.)
  std::string child_e_raw;   // C:E value
  std::string child_m_raw;   // C:M value (manifest only)
  std::span<const uint8_t> ext_metadata;  // metadata frame from C:E
  std::string_view ext_tail;              // C:E tail: manifest or single-part data
  std::string_view ext_manifest;          // manifest from C:E tail or C:M

  // Effective metadata frame / manifest blob, wherever they were found.
  std::span<const uint8_t> full_metadata() const {
    return has_external_metadata() ? ext_metadata : metadata_frame;
  }
  std::string_view full_manifest() const {
    return manifest_blob.empty() ? ext_manifest : manifest_blob;
  }
  // Single-part body when it is held in :O: or C:E (empty otherwise).
  std::span<const uint8_t> full_inline_data() const {
    if (hdr.flags & kFlagDataInChildE) {
      return {reinterpret_cast<const uint8_t*>(ext_tail.data()), ext_tail.size()};
    }
    return inline_data;
  }

  OwnedObjectValue() = default;
  OwnedObjectValue(const OwnedObjectValue&) = delete;
  OwnedObjectValue& operator=(const OwnedObjectValue&) = delete;
  OwnedObjectValue(OwnedObjectValue&&) noexcept = default;
  OwnedObjectValue& operator=(OwnedObjectValue&&) noexcept = default;
};
static_assert(sizeof(ObjectValueHeader) + sizeof(ChunkDescriptor) > 15,
              "raw :O: values must exceed the std::string SSO capacity");

struct BucketValue {
  bucket_id_t bucket_id{};
  int64_t created_at_unix{};
  uint8_t access_flags{};
  VersioningState versioning_state{};
  std::string policy_json;
};

// Zero-copy parse: the returned ObjectValue's views point into `data`, which
// must outlive it. Rejects every illegal layout combination
// (see validate_object_layout).
std::optional<ObjectValue> parse_object_value(std::string_view data);

// Takes ownership of `raw` and parses it in place.
std::optional<OwnedObjectValue> parse_owned_object_value(std::string raw);

// Checks the cross-field rules shared by the writer and the parser (reserved
// bits, location nibbles, Cached bits, first_part_info consistency, ...).
bool validate_object_layout(const ObjectValue& v);

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
      ct != CHUNK_STORAGE && ct != CHUNK_STORAGE_REF && ct != CHUNK_MULTIPART &&
      ct != CHUNK_MULTIPART_REF) {
    return nullptr;
  }
  return hdr;
}

// Accessors that apply byte-swap on the raw big-endian header pointer.
inline uint16_t ovh_flags(const ObjectValueHeader* h)
{
  return be16toh(h->flags);
}
inline bool ovh_is_delete_marker(const ObjectValueHeader* h)
{
  return (ovh_flags(h) & kFlagFenced) != 0;
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
inline uint16_t ovh_part_count(const ObjectValueHeader* h)
{
  return h->etag.part_count();
}
inline CksumAlg ovh_cksum_alg(const ObjectValueHeader* h)
{
  return h->get_cksum_alg();
}
inline uint16_t ovh_metadata_count(const ObjectValueHeader* h)
{
  return be16toh(h->metadata_count);
}

// The ChunkDescriptor that follows header + content_type (every chunk type).
// Returns false if the buffer is too short.
inline bool ovh_chunk(std::string_view raw, const ObjectValueHeader* h,
                      ChunkDescriptor& out)
{
  const size_t off = sizeof(ObjectValueHeader) + h->content_type_len;
  if (raw.size() < off + sizeof(ChunkDescriptor)) {
    return false;
  }
  out.load(reinterpret_cast<const uint8_t*>(raw.data()) + off);
  return true;
}

// Local manifest blob embedded in the :O: value (inline or cached copy);
// empty if there is none or the value is corrupt. Zero-copy.
std::string_view ovh_multipart_manifest_blob(std::string_view raw,
                                             const ObjectValueHeader* h);

std::string ovh_etag_display(const ObjectValueHeader* h);
// Encoded metadata frame inline in :O:; empty when absent or external.
std::span<const uint8_t> object_inline_metadata_bytes(std::string_view data);

// Narrows ObjectValueHeader.flags to the 8-bit GcValueHeader.flags.
inline uint8_t gc_flags_from_o_flags(uint16_t o_flags)
{
  uint8_t g = 0;
  if (o_flags & kFlagSharedData) {
    g |= kGcFlagShared;
  }
  if (o_flags & (kFlagExternalTags | kFlagExternalAnnotations | kFlagExternalMetadata)) {
    g |= kGcFlagExternalChildren;
  }
  return g;
}

void child_hdr_to_be(ChildValueHeader& hdr);
void child_hdr_from_be(ChildValueHeader& hdr);
std::string make_child_value(ChildValueHeader hdr, std::string_view payload);
bool parse_child_value_header(std::string_view data, ChildValueHeader& out);
std::string_view child_value_payload(std::string_view data);
std::string_view child_value_payload(std::string_view data, uint64_t payload_size);

// C:E value: [ChildValueHeader 8 B][metadata_frame][optional manifest or
// single-part data]. Every part is a view; ChildValueBuf assembles them.
struct ChildEValue {
  ChildValueHeader hdr{};
  std::span<const uint8_t> metadata_frame;
  std::string_view tail;   // manifest blob or data; empty when none

  size_t size() const { return sizeof(hdr) + metadata_frame.size() + tail.size(); }
  // Serializes into dst (>= size() bytes).
  void serialize(uint8_t* dst) const;
};

// One child value, assembled in a stack buffer big enough for the usual cases
// (header + metadata frame + a default-sized max_kv_store body); only a larger
// value (bigger max_kv_store, a manifest or checksum blob up to 75 KB) falls
// back to a heap allocation. The payload is copied once, straight to its final
// place. payload() never includes the header; value() is the whole buffer,
// ready for kv_put. Not copyable or movable (value() may point into *this).
class ChildValueBuf {
 public:
  static constexpr size_t kStackBytes =
      sizeof(ChildValueHeader) + MAX_META_FRAME_BYTES + kDefaultMaxKvStore;

  // `[ChildValueHeader][payload]`
  ChildValueBuf(ChildValueHeader hdr, std::string_view payload);
  // `[payload]` only (no header; e.g. a storage-tier body).
  explicit ChildValueBuf(std::string_view payload);
  // `[head][tail]` (e.g. an R: value: 8 B count + descriptor). payload() is the tail.
  ChildValueBuf(std::string_view head, std::string_view tail);
  // `[ChildValueHeader][metadata_frame][tail]`
  explicit ChildValueBuf(const ChildEValue& e);

  ChildValueBuf(const ChildValueBuf&) = delete;
  ChildValueBuf& operator=(const ChildValueBuf&) = delete;

  std::string_view value() const { return {reinterpret_cast<const char*>(p_), len_}; }
  std::string_view payload() const { return value().substr(hdr_len_); }

 private:
  void alloc(size_t n) {
    len_ = n;
    if (n > kStackBytes) {
      heap_ = std::make_unique_for_overwrite<uint8_t[]>(n);
      p_ = heap_.get();
    } else {
      p_ = stack_;
    }
  }

  alignas(8) uint8_t stack_[kStackBytes];
  std::unique_ptr<uint8_t[]> heap_;
  uint8_t* p_{stack_};
  size_t len_{0};
  size_t hdr_len_{0};
};
// Parsed C:E value (views into the raw child value).
struct ChildEParsed {
  ChildValueHeader hdr{};
  std::span<const uint8_t> metadata_frame;
  std::string_view tail;   // everything after the metadata frame
};
// `has_metadata` is ObjectValueHeader.metadata_count > 0.
std::optional<ChildEParsed> parse_child_e_value(std::string_view data, bool has_metadata);

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

// Stamps hdr.last_modified_sec / last_modified_nsec from the current wall clock.
inline void stamp_mtime(ObjectValueHeader& hdr) {
  using namespace std::chrono;
  const auto tp = system_clock::now();
  const auto s  = duration_cast<seconds>(tp.time_since_epoch());
  const auto ns = duration_cast<nanoseconds>(tp.time_since_epoch())
                - duration_cast<nanoseconds>(s);
  hdr.last_modified_sec  = static_cast<uint32_t>(s.count());
  hdr.last_modified_nsec = static_cast<uint32_t>(ns.count());
}

// Serializes `value` into `buf` in the wire order:
//   header | content_type | ChunkDescriptor | cksum_combined | metadata_frame
//   | single-part: inline data
//   | multipart: [loc nibbles][mflags] [first_part_info | manifest] [cksum blob]
// Returns false when the layout is illegal or does not fit kMaxOValueBytes.
// For non-REF chunk types the chunk's type/ref_tag are set from the header.
bool write_object_value(OValueBuf& buf, const ObjectValue& value);

}  // namespace kvrgw
