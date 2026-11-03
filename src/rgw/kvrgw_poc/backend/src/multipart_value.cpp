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

#include "multipart_value.hpp"

#include "id_meta.hpp"

#include <arpa/inet.h>
#include <cstdio>
#include <cstring>
#include <endian.h>

namespace kvrgw {

//--------------------------------------------------------------------------------
std::string part_filename(const RefTag& ref_tag, uint16_t part_number,
                          uint32_t generation)
{
  char buf[64];
  const int n = std::snprintf(buf, sizeof(buf), "%s-%u-%u",
                              ref_tag.filename().c_str(), part_number,
                              generation);
  if (n <= 0 || static_cast<size_t>(n) >= sizeof(buf)) {
    return {};
  }
  return std::string(buf, static_cast<size_t>(n));
}

//--------------------------------------------------------------------------------
std::string checksum_filename(const RefTag& ref_tag)
{
  return ref_tag.filename() + "_checksum";
}

//--------------------------------------------------------------------------------
std::string_view checksum_filename_buf(const RefTag& ref_tag, std::span<char> out)
{
  constexpr std::string_view kSuffix = "_checksum";
  const size_t hex_len = kRefTagSize * 2;
  if (out.size() < hex_len + kSuffix.size()) {
    return {};
  }
  static constexpr char kHex[] = "0123456789abcdef";
  const std::string_view raw = ref_tag.view();
  for (size_t i = 0; i < kRefTagSize; ++i) {
    const auto b = static_cast<uint8_t>(raw[i]);
    out[2 * i] = kHex[b >> 4];
    out[2 * i + 1] = kHex[b & 0xF];
  }
  std::memcpy(out.data() + hex_len, kSuffix.data(), kSuffix.size());
  return {out.data(), hex_len + kSuffix.size()};
}

//--------------------------------------------------------------------------------
bool place_multipart(ObjectValue& v, const MultipartPlacementIn& in,
                     MultipartPlacementOut& out)
{
  out = {};
  if (!v.is_multipart() || in.manifest.size() > kMaxChildPayloadBytes ||
      gm_blob_body(in.manifest).empty() ||
      (v.hdr.metadata_count > 0) != !in.metadata_frame.empty()) {
    return false;
  }
  const CksumAlg alg = v.cksum_alg();
  const bool has_cksum = alg != CksumAlg::NONE;
  if (has_cksum && in.existing_cksum_loc == kCksumInline &&
      in.cksum_blob.size() != v.cksum_blob_len()) {
    return false;
  }
  const bool shared = (v.hdr.flags & kFlagSharedData) != 0;

  // ---- step 0: forced fields
  const size_t fixed = sizeof(ObjectValueHeader) + v.content_type.size()
      + sizeof(ChunkDescriptor) + kMultipartHdrBytes + cksum_digest_len(alg);
  const size_t meta_len = in.metadata_frame.size();
  const size_t manifest_len = in.manifest.size();

  // ---- phase 1: inline vs external
  bool meta_inline;
  bool manifest_local;     // a local manifest copy is stored in :O:
  size_t used = fixed;
  if (fixed + meta_len + manifest_len <= kMaxOValueBytes) {
    meta_inline = true;
    manifest_local = true;
    used += meta_len + manifest_len;
  } else if (fixed + meta_len + kFirstPartInfoBytes <= kMaxOValueBytes) {
    meta_inline = true;
    manifest_local = false;
    used += meta_len + kFirstPartInfoBytes;
  } else if (fixed + manifest_len <= kMaxOValueBytes) {
    meta_inline = false;
    manifest_local = true;
    used += manifest_len;
  } else {
    meta_inline = false;
    manifest_local = false;
    used += kFirstPartInfoBytes;
  }

  // ---- phase 2: concrete locations
  v.hdr.flags = static_cast<uint16_t>(v.hdr.flags & ~kFlagExternalMetadata);
  v.metadata_frame = {};
  v.manifest_blob = {};
  v.cksum_blob = {};
  v.mflags = in.composite && has_cksum ? kMFlagComposite : 0;
  v.first_part = in.first_part;

  if (meta_inline) {
    v.metadata_frame = in.metadata_frame;
  } else {
    v.hdr.flags |= kFlagExternalMetadata;
    out.has_child_e = true;
    out.child_e.metadata_frame = in.metadata_frame;
  }

  if (shared) {
    // The authoritative manifest is always C:M; the local copy is a cache.
    v.manifest_loc = kManifestChildM;
    out.manifest_child = in.manifest;
    if (manifest_local) {
      v.mflags |= kMFlagManifestCached;
      v.manifest_blob = in.manifest;
    }
  } else if (manifest_local) {
    v.manifest_loc = kManifestInline;
    v.manifest_blob = in.manifest;
  } else if (meta_inline) {
    v.manifest_loc = kManifestChildM;
    out.manifest_child = in.manifest;
  } else {
    v.manifest_loc = kManifestChildE;
    out.child_e.tail = in.manifest;
  }
  if (!manifest_local) {
    v.mflags |= kMFlagFirstPartInfo;
  }

  // ---- per-part checksum blob (lowest priority)
  v.cksum_loc = kCksumInline;
  if (has_cksum) {
    const size_t blen = v.cksum_blob_len();
    const bool fits = used + blen <= kMaxOValueBytes;
    if (in.existing_cksum_loc != kCksumInline) {
      // CopyObject of a source whose blob is already external: keep it there.
      v.cksum_loc = in.existing_cksum_loc;
      if (fits && in.cksum_blob.size() == blen) {
        v.mflags |= kMFlagCksumCached;
        v.cksum_blob = in.cksum_blob;
      }
    } else if (!shared && fits) {
      v.cksum_blob = in.cksum_blob;
    } else if (blen <= kMaxChildPayloadBytes) {
      v.cksum_loc = kCksumChildS;
      out.checksum_child = in.cksum_blob;
    } else {
      v.cksum_loc = kCksumFile;
      out.checksum_file = in.cksum_blob;
    }
    if (in.existing_cksum_loc == kCksumInline && shared && fits) {
      v.mflags |= kMFlagCksumCached;
      v.cksum_blob = in.cksum_blob;
    }
  }
  return validate_object_layout(v);
}

//--------------------------------------------------------------------------------
bool place_single_part(ObjectValue& v, std::span<const uint8_t> metadata_frame,
                       std::span<const uint8_t> data, uint64_t data_len,
                       uint32_t max_inline, uint32_t max_kv_store,
                       SinglePlacementOut& out)
{
  out = {};
  if (v.is_multipart() || (v.hdr.metadata_count > 0) != !metadata_frame.empty() ||
      (!data.empty() && data.size() != data_len)) {
    return false;
  }
  const size_t fixed = sizeof(ObjectValueHeader) + v.content_type.size()
      + sizeof(ChunkDescriptor) + v.cksum_combined_len();
  v.hdr.flags = static_cast<uint16_t>(
      v.hdr.flags & ~(kFlagExternalMetadata | kFlagDataInChildE));
  v.hdr.size = data_len;
  v.metadata_frame = {};
  v.inline_data = {};

  if (fixed + metadata_frame.size() <= kMaxOValueBytes) {
    v.metadata_frame = metadata_frame;
    const size_t remaining = kMaxOValueBytes - fixed - metadata_frame.size();
    if (data_len <= std::min<uint64_t>(remaining, max_inline)) {
      out.data = DataPlace::kInline;
      v.hdr.chunk_type = CHUNK_INLINE;
      v.inline_data = data;
    } else if (data_len <= max_kv_store) {
      out.data = DataPlace::kChildD;
      v.hdr.chunk_type = CHUNK_CHILD_D;
    } else {
      out.data = DataPlace::kStorage;
      v.hdr.chunk_type = CHUNK_STORAGE;
    }
  } else {
    v.hdr.flags |= kFlagExternalMetadata;
    out.has_child_e = true;
    out.child_e.metadata_frame = metadata_frame;
    if (data_len <= max_kv_store) {
      out.data = DataPlace::kChildE;
      v.hdr.flags |= kFlagDataInChildE;
      v.hdr.chunk_type = CHUNK_INLINE;
      out.child_e.tail = {reinterpret_cast<const char*>(data.data()), data.size()};
    } else {
      out.data = DataPlace::kStorage;
      v.hdr.chunk_type = CHUNK_STORAGE;
    }
  }
  return validate_object_layout(v);
}

//--------------------------------------------------------------------------------
bool make_m_head_value(MHeadValueBuf& buf,
                       int64_t initiated_unix_ms,
                       std::string_view content_type,
                       uint8_t tags_count,
                       std::span<const MetaPair> metadata,
                       CksumAlg cksum_alg,
                       CksumType cksum_type)
{
  if (content_type.size() > kMaxContentTypeLen) {
    return false;
  }

  MHeadValueHeader hdr{};
  hdr.initiated_unix_ms = htobe64(initiated_unix_ms);
  hdr.metadata_count = htons(static_cast<uint16_t>(metadata.size()));
  hdr.tags_count = tags_count;
  hdr.content_type_len = static_cast<uint8_t>(content_type.size());
  hdr.cksum_alg = static_cast<uint8_t>(cksum_alg);
  hdr.cksum_type = static_cast<uint8_t>(cksum_type);

  size_t off = 0;
  std::memcpy(buf.data, &hdr, sizeof(hdr));
  off += sizeof(hdr);
  std::memcpy(buf.data + off, content_type.data(), content_type.size());
  off += content_type.size();
  if (!metadata.empty()) {
    // Encode directly into buf — no intermediate copy.
    size_t meta_len = 0;
    const size_t remaining = MHeadValueBuf::kMaxSize - off;
    if (!encode_metadata(metadata,
                         std::span<uint8_t>(buf.data + off, remaining),
                         meta_len)) {
      return false;
    }
    off += meta_len;
  }
  buf.len = off;
  return true;
}

//--------------------------------------------------------------------------------
std::optional<MHeadValue> parse_m_head_value(std::string_view data)
{
  if (data.size() < sizeof(MHeadValueHeader)) {
    return std::nullopt;
  }
  MHeadValue out;
  std::memcpy(&out.hdr, data.data(), sizeof(MHeadValueHeader));
  out.hdr.initiated_unix_ms = be64toh(out.hdr.initiated_unix_ms);
  out.hdr.metadata_count = ntohs(out.hdr.metadata_count);

  size_t off = sizeof(MHeadValueHeader);
  if (data.size() < off + out.hdr.content_type_len) {
    return std::nullopt;
  }
  out.content_type.assign(data.substr(off, out.hdr.content_type_len));
  off += out.hdr.content_type_len;

  if (out.hdr.metadata_count > 0) {
    size_t frame_size = 0;
    const auto rest = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(data.data()) + off, data.size() - off);
    if (!encoded_metadata_frame_size(rest, frame_size) || data.size() < off + frame_size) {
      return std::nullopt;
    }
    out.metadata_frame.assign(data.data() + off, data.data() + off + frame_size);
  }
  return out;
}

//--------------------------------------------------------------------------------
std::optional<MHeadValueView> view_m_head_value(std::string_view data)
{
  if (data.size() < sizeof(MHeadValueHeader)) {
    return std::nullopt;
  }
  MHeadValueHeader hdr{};
  std::memcpy(&hdr, data.data(), sizeof(hdr));

  size_t off = sizeof(MHeadValueHeader);
  if (data.size() < off + hdr.content_type_len) {
    return std::nullopt;
  }
  MHeadValueView out;
  out.tags_count = hdr.tags_count;
  out.cksum_alg = static_cast<CksumAlg>(hdr.cksum_alg);
  out.cksum_type = static_cast<CksumType>(hdr.cksum_type);
  out.content_type = data.substr(off, hdr.content_type_len);
  off += hdr.content_type_len;

  out.metadata_count = ntohs(hdr.metadata_count);
  if (out.metadata_count > 0) {
    size_t frame_size = 0;
    const auto rest = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(data.data()) + off, data.size() - off);
    if (!encoded_metadata_frame_size(rest, frame_size) || data.size() < off + frame_size) {
      return std::nullopt;
    }
    out.metadata_frame = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(data.data()) + off, frame_size);
  }
  return out;
}

//--------------------------------------------------------------------------------
void make_m_part_value(const MPartValue& v, uint8_t* dst)
{
  // Write field-by-field at fixed offsets (not a raw struct memcpy) so this
  // stays correct regardless of compiler padding.
  dst[offsetof(MPartValue, state)] = v.state;
  std::memset(dst + offsetof(MPartValue, pad), 0, sizeof(MPartValue::pad));
  v.chunk.serialize(dst + offsetof(MPartValue, chunk));
  v.etag.store(dst + offsetof(MPartValue, etag));
  const uint64_t size_be = htobe64(v.size);
  std::memcpy(dst + offsetof(MPartValue, size), &size_be, sizeof(size_be));
  const uint32_t mtime_be = htonl(v.mtime);
  std::memcpy(dst + offsetof(MPartValue, mtime), &mtime_be, sizeof(mtime_be));
  std::memcpy(dst + offsetof(MPartValue, cksum), v.cksum, sizeof(v.cksum));
}

//--------------------------------------------------------------------------------
std::optional<MPartValue> parse_m_part_value(std::string_view data)
{
  if (data.size() != sizeof(MPartValue)) {
    return std::nullopt;
  }
  const auto* src = reinterpret_cast<const uint8_t*>(data.data());
  MPartValue v;
  v.state = src[offsetof(MPartValue, state)];
  v.chunk.load(src + offsetof(MPartValue, chunk));
  v.etag.load(src + offsetof(MPartValue, etag));
  uint64_t size_be{};
  std::memcpy(&size_be, src + offsetof(MPartValue, size), sizeof(size_be));
  v.size = be64toh(size_be);
  uint32_t mtime_be{};
  std::memcpy(&mtime_be, src + offsetof(MPartValue, mtime), sizeof(mtime_be));
  v.mtime = ntohl(mtime_be);
  std::memcpy(v.cksum, src + offsetof(MPartValue, cksum), sizeof(v.cksum));
  if (v.state != M_PART_PENDING && v.state != M_PART_COMMITTED) {
    return std::nullopt;
  }
  return v;
}


//--------------------------------------------------------------------------------
void make_gu_value(GuValueBuf& buf, const GuEntry* entries, uint8_t count,
                   uint64_t sum_size)
{
  buf.data[0] = count;
  const uint64_t sum_size_be = htobe64(sum_size);
  std::memcpy(buf.data + 1, &sum_size_be, sizeof(sum_size_be));
  buf.len = 1 + sizeof(sum_size_be);
  for (uint8_t i = 0; i < count; ++i) {
    const uint16_t pn_be = htons(entries[i].part_number);
    const uint32_t gen_be = htonl(entries[i].generation);
    std::memcpy(buf.data + buf.len, &pn_be, sizeof(pn_be));
    buf.len += sizeof(pn_be);
    std::memcpy(buf.data + buf.len, &gen_be, sizeof(gen_be));
    buf.len += sizeof(gen_be);
  }
}

//--------------------------------------------------------------------------------
std::optional<GuValue> parse_gu_value(std::string_view data)
{
  constexpr size_t kFixed = 1 + sizeof(uint64_t);
  if (data.size() < kFixed) {
    return std::nullopt;
  }
  const uint8_t count = static_cast<uint8_t>(data[0]);
  if (count == 0 || count > kMaxBatchSize) {
    return std::nullopt;
  }
  const size_t expected = kFixed + static_cast<size_t>(count) * sizeof(GuEntry);
  if (data.size() != expected) {
    return std::nullopt;
  }
  uint64_t sum_size_be{};
  std::memcpy(&sum_size_be, data.data() + 1, sizeof(sum_size_be));

  GuValue val;
  val.count = count;
  val.sum_size = be64toh(sum_size_be);
  for (size_t i = 0; i < count; ++i) {
    uint16_t pn_be{};
    uint32_t gen_be{};
    const size_t off = kFixed + i * sizeof(GuEntry);
    std::memcpy(&pn_be, data.data() + off, sizeof(pn_be));
    std::memcpy(&gen_be, data.data() + off + sizeof(pn_be), sizeof(gen_be));
    val.entries[i].part_number = be16toh(pn_be);
    val.entries[i].generation = be32toh(gen_be);
  }
  return val;
}

} // namespace kvrgw
