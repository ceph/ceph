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
bool make_m_head_value(MHeadValueBuf& buf,
                       int64_t initiated_unix_ms,
                       std::string_view content_type,
                       uint8_t tags_count,
                       std::span<const MetaPair> metadata)
{
  if (content_type.size() > kMaxContentTypeLen) {
    return false;
  }

  MHeadValueHeader hdr{};
  hdr.initiated_unix_ms = htobe64(initiated_unix_ms);
  hdr.metadata_count = htons(static_cast<uint16_t>(metadata.size()));
  hdr.tags_count = tags_count;
  hdr.content_type_len = static_cast<uint8_t>(content_type.size());

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
  if (v.state != M_PART_PENDING && v.state != M_PART_COMMITTED) {
    return std::nullopt;
  }
  return v;
}


//--------------------------------------------------------------------------------
void make_gm_value(GmValueBuf& buf, GmMode mode, const ManifestInput& in)
{
  buf.data[0] = static_cast<uint8_t>(mode);
  size_t len = 1;
  if (mode == GM_MODE_MIXED) {
    len += encode_manifest_body(buf.data + 1, sizeof(buf.data) - 1, in);
  }
  buf.len = len;
}

//--------------------------------------------------------------------------------
std::optional<GmValue> parse_gm_value(std::string_view data)
{
  if (data.empty()) {
    return std::nullopt;
  }
  const uint8_t mode = static_cast<uint8_t>(data[0]);
  if (mode != GM_MODE_DEEP && mode != GM_MODE_MIXED && mode != GM_MODE_FREE_PARTS) {
    return std::nullopt;
  }

  GmValue v;
  v.mode = mode;
  if (mode == GM_MODE_MIXED || mode == GM_MODE_FREE_PARTS) {
    v.body = data.substr(1);
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

//--------------------------------------------------------------------------------
void make_pm_value(PmValueBuf& buf, PmState state, uint32_t mtime, const ManifestInput& in)
{
  buf.data[0] = static_cast<uint8_t>(state);
  std::memset(buf.data + 1, 0, 3);
  const uint32_t mtime_be = htonl(mtime);
  std::memcpy(buf.data + 4, &mtime_be, sizeof(mtime_be));
  size_t len = 8;
  if (state == PM_STATE_COMMIT) {
    len += encode_manifest_body(buf.data + 8, sizeof(buf.data) - 8, in);
  }
  buf.len = len;
}

//--------------------------------------------------------------------------------
std::optional<PmValue> parse_pm_value(std::string_view data)
{
  constexpr size_t kFixed = 1 + 3 + sizeof(uint32_t);
  if (data.size() < kFixed) {
    return std::nullopt;
  }
  const uint8_t state = static_cast<uint8_t>(data[0]);
  if (state != PM_STATE_COMMIT && state != PM_STATE_ABORT) {
    return std::nullopt;
  }
  uint32_t mtime_be{};
  std::memcpy(&mtime_be, data.data() + 4, sizeof(mtime_be));

  PmValue v;
  v.state = state;
  v.mtime = ntohl(mtime_be);
  if (state == PM_STATE_COMMIT) {
    v.body = data.substr(kFixed);
  }
  return v;
}

} // namespace kvrgw
