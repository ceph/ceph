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
  hdr.flags = htons(hdr.flags);
  hdr.metadata_count = htons(hdr.metadata_count);
  hdr.size = htobe64(hdr.size);
  hdr.last_modified_sec = htonl(hdr.last_modified_sec);
  hdr.last_modified_nsec = htonl(hdr.last_modified_nsec);
  hdr.version_id = hdr.version_id.to_be();
  hdr.next_vid = hdr.next_vid.to_be();
  hdr.annotations_count = htons(hdr.annotations_count);
}

void hdr_from_be(ObjectValueHeader &hdr)
{
  hdr.flags = be16toh(hdr.flags);
  hdr.metadata_count = be16toh(hdr.metadata_count);
  hdr.size = be64toh(hdr.size);
  hdr.last_modified_sec = be32toh(hdr.last_modified_sec);
  hdr.last_modified_nsec = be32toh(hdr.last_modified_nsec);
  hdr.version_id = hdr.version_id.from_be();
  hdr.next_vid = hdr.next_vid.from_be();
  hdr.annotations_count = be16toh(hdr.annotations_count);
}

std::string ObjectValue::etag_display() const { return get_etag().to_hex(); }

std::string ovh_etag_display(const ObjectValueHeader* h)
{
  return h->etag.to_hex();
}

//--------------------------------------------------------------------------------
namespace {

bool valid_chunk_type(uint8_t ct)
{
  return ct == CHUNK_INLINE || ct == CHUNK_CHILD_D || ct == CHUNK_CHILD_D_REF ||
         ct == CHUNK_STORAGE || ct == CHUNK_STORAGE_REF ||
         ct == CHUNK_MULTIPART || ct == CHUNK_MULTIPART_REF;
}

} // namespace

//--------------------------------------------------------------------------------
bool validate_object_layout(const ObjectValue &v)
{
  const ObjectValueHeader &h = v.hdr;
  if (!valid_chunk_type(h.chunk_type) ||
      v.content_type.size() > kMaxContentTypeLen) {
    return false;
  }
  if ((h.flags & kFlagReservedMask) != 0) {
    return false;
  }
  // low nibble of the cksum_alg byte is reserved
  if ((h.cksum_alg_byte & 0x0F) != 0 ||
      static_cast<uint8_t>(h.get_cksum_alg()) > kCksumAlgMax) {
    return false;
  }
  const CksumAlg alg = h.get_cksum_alg();
  const bool ext_meta = (h.flags & kFlagExternalMetadata) != 0;
  const bool shared = (h.flags & kFlagSharedData) != 0;

  // metadata: inline frame iff count > 0 and not external
  if (ext_meta && h.metadata_count == 0) {
    return false;
  }
  if (h.metadata_count > 0 && !ext_meta) {
    if (v.metadata_frame.empty()) {
      return false;
    }
  } else if (!v.metadata_frame.empty()) {
    return false;
  }

  if (!v.is_multipart()) {
    if (v.cksum_loc != kCksumInline || v.manifest_loc != kManifestInline ||
        v.mflags != 0 || !v.manifest_blob.empty() || !v.cksum_blob.empty()) {
      return false;
    }
    if (h.flags & kFlagDataInChildE) {
      if (h.chunk_type != CHUNK_INLINE || !ext_meta || shared ||
          !v.inline_data.empty()) {
        return false;
      }
    } else if (h.chunk_type == CHUNK_INLINE) {
      if (v.inline_data.size() != h.size) {
        return false;
      }
    } else if (!v.inline_data.empty()) {
      return false;
    }
    return true;
  }

  // ---- multipart ----
  if ((h.flags & kFlagDataInChildE) || !v.inline_data.empty()) {
    return false;
  }
  if (v.cksum_loc > kCksumFile || v.manifest_loc > kManifestChildM ||
      (v.mflags & kMFlagReservedMask) != 0) {
    return false;
  }
  if (alg == CksumAlg::NONE) {
    if (v.cksum_loc != kCksumInline ||
        (v.mflags & (kMFlagComposite | kMFlagCksumCached)) != 0 ||
        !v.cksum_blob.empty()) {
      return false;
    }
  } else if (!cksum_type_valid_for_alg(alg, v.cksum_type())) {
    return false;
  }
  if (v.manifest_loc == kManifestChildE && (!ext_meta || shared)) {
    return false;
  }
  if (shared && v.manifest_loc != kManifestChildM) {
    return false;
  }
  if ((v.mflags & (kMFlagManifestCached | kMFlagCksumCached)) && !shared) {
    return false;
  }
  if ((v.mflags & kMFlagManifestCached) && v.manifest_loc == kManifestInline) {
    return false;
  }
  if ((v.mflags & kMFlagCksumCached) && v.cksum_loc == kCksumInline) {
    return false;
  }

  // first_part_info <=> no local manifest
  const bool first_part_info = (v.mflags & kMFlagFirstPartInfo) != 0;
  if (first_part_info == v.has_local_manifest()) {
    return false;
  }
  if (first_part_info) {
    if (!v.manifest_blob.empty() || v.first_part.size >= (1ULL << 40) ||
        v.first_part.part_number == 0) {
      return false;
    }
  } else if (gm_blob_body(v.manifest_blob).empty()) {
    return false;
  }

  if (v.has_local_cksum_blob()) {
    if (v.cksum_blob.size() != v.cksum_blob_len()) {
      return false;
    }
  } else if (!v.cksum_blob.empty()) {
    return false;
  }
  return true;
}

//--------------------------------------------------------------------------------
std::optional<ObjectValue> parse_object_value(std::string_view data)
{
  if (data.size() < sizeof(ObjectValueHeader)) {
    return std::nullopt;
  }

  ObjectValue v;
  std::memcpy(&v.hdr, data.data(), sizeof(ObjectValueHeader));
  hdr_from_be(v.hdr);
  if (!valid_chunk_type(v.hdr.chunk_type)) {
    return std::nullopt;
  }
  const CksumAlg alg = v.hdr.get_cksum_alg();
  if (static_cast<uint8_t>(alg) > kCksumAlgMax) {
    return std::nullopt;
  }

  size_t off = sizeof(ObjectValueHeader);
  const size_t ct_len = v.hdr.content_type_len;
  if (data.size() < off + ct_len + sizeof(ChunkDescriptor)) {
    return std::nullopt;
  }
  v.content_type = data.substr(off, ct_len);
  off += ct_len;

  v.chunk.load(reinterpret_cast<const uint8_t *>(data.data()) + off);
  off += sizeof(ChunkDescriptor);
  if (v.chunk.get_type() != v.hdr.chunk_type) {
    return std::nullopt;
  }

  const size_t dlen = cksum_digest_len(alg);
  if (data.size() < off + dlen) {
    return std::nullopt;
  }
  std::memcpy(v.cksum_combined.data(), data.data() + off, dlen);
  off += dlen;

  const bool ext_meta = (v.hdr.flags & kFlagExternalMetadata) != 0;
  if (v.hdr.metadata_count > 0 && !ext_meta) {
    const std::span<const uint8_t> rest(
        reinterpret_cast<const uint8_t *>(data.data() + off), data.size() - off);
    size_t frame_size = 0;
    if (!encoded_metadata_frame_size(rest, frame_size) ||
        read_be_field<tag_count_t>(rest.data()) != v.hdr.metadata_count) {
      return std::nullopt;
    }
    v.metadata_frame = rest.first(frame_size);
    off += frame_size;
  }

  if (!v.is_multipart()) {
    if (v.hdr.chunk_type == CHUNK_INLINE &&
        !(v.hdr.flags & kFlagDataInChildE)) {
      if (v.hdr.size > data.size() - off) {
        return std::nullopt;
      }
      v.inline_data = {reinterpret_cast<const uint8_t *>(data.data()) + off,
                       static_cast<size_t>(v.hdr.size)};
      off += static_cast<size_t>(v.hdr.size);
    }
    if (off != data.size() || !validate_object_layout(v)) {
      return std::nullopt;
    }
    return v;
  }

  // ---- multipart: [loc nibbles][mflags] then first_part_info | manifest,
  // then the local per-part checksum blob (fixed length) at the very end.
  if (data.size() < off + kMultipartHdrBytes) {
    return std::nullopt;
  }
  v.cksum_loc = static_cast<uint8_t>(data[off]) >> 4;
  v.manifest_loc = static_cast<uint8_t>(data[off]) & 0x0F;
  v.mflags = static_cast<uint8_t>(data[off + 1]);
  off += kMultipartHdrBytes;

  std::string_view rest = data.substr(off);
  if (v.has_local_cksum_blob()) {
    const size_t blen = v.cksum_blob_len();
    if (rest.size() < blen) {
      return std::nullopt;
    }
    v.cksum_blob = rest.substr(rest.size() - blen);
    rest.remove_suffix(blen);
  }
  if (v.mflags & kMFlagFirstPartInfo) {
    if (rest.size() != kFirstPartInfoBytes) {
      return std::nullopt;
    }
    v.first_part = FirstPartInfo::decode(
        reinterpret_cast<const uint8_t *>(rest.data()));
  } else {
    v.manifest_blob = rest;
  }
  if (!validate_object_layout(v)) {
    return std::nullopt;
  }
  return v;
}

//--------------------------------------------------------------------------------
std::optional<OwnedObjectValue> parse_owned_object_value(std::string raw)
{
  OwnedObjectValue out;
  out.raw = std::move(raw);
  const auto parsed = parse_object_value(out.raw);
  if (!parsed) {
    return std::nullopt;
  }
  static_cast<ObjectValue &>(out) = *parsed;
  return out;
}

//--------------------------------------------------------------------------------
std::span<const uint8_t> object_inline_metadata_bytes(std::string_view data)
{
  const auto v = parse_object_value(data);
  return v ? v->metadata_frame : std::span<const uint8_t>{};
}

//--------------------------------------------------------------------------------
std::string_view ovh_multipart_manifest_blob(std::string_view raw,
                                             const ObjectValueHeader *h)
{
  if (!h || (h->chunk_type != CHUNK_MULTIPART &&
             h->chunk_type != CHUNK_MULTIPART_REF)) {
    return {};
  }
  const auto v = parse_object_value(raw);
  return v ? v->manifest_blob : std::string_view{};
}

//--------------------------------------------------------------------------------
bool write_object_value(OValueBuf &buf, const ObjectValue &value)
{
  if (!validate_object_layout(value)) {
    return false;
  }
  const ObjectValueHeader &h = value.hdr;
  ObjectValueHeader wire = h;
  wire.content_type_len = static_cast<uint8_t>(value.content_type.size());
  hdr_to_be(wire);
  buf.set_header(wire);
  if (!buf.append(value.content_type)) {
    return false;
  }

  // Non-REF objects own their data: the descriptor points at themselves.
  const bool is_ref = h.chunk_type == CHUNK_CHILD_D_REF ||
                      h.chunk_type == CHUNK_STORAGE_REF ||
                      h.chunk_type == CHUNK_MULTIPART_REF;
  const ChunkDescriptor cd =
      is_ref ? ChunkDescriptor(h.chunk_type, value.chunk.get_ref_tag(),
                               value.chunk.get_bucket_id())
             : ChunkDescriptor(h.chunk_type, h.ref_tag,
                               value.chunk.get_bucket_id());
  std::array<uint8_t, sizeof(ChunkDescriptor)> cdb;
  cd.serialize(cdb.data());
  if (!buf.append(cdb.data(), cdb.size())) {
    return false;
  }
  if (!buf.append(value.cksum_combined.data(), value.cksum_combined_len()) ||
      !buf.append(value.metadata_frame)) {
    return false;
  }

  if (!value.is_multipart()) {
    return buf.append(value.inline_data);
  }

  const std::array<uint8_t, kMultipartHdrBytes> mh = {
      static_cast<uint8_t>((value.cksum_loc << 4) | (value.manifest_loc & 0x0F)),
      value.mflags};
  if (!buf.append(mh.data(), mh.size())) {
    return false;
  }
  if (value.mflags & kMFlagFirstPartInfo) {
    std::array<uint8_t, kFirstPartInfoBytes> fp;
    value.first_part.encode(fp.data());
    if (!buf.append(fp.data(), fp.size())) {
      return false;
    }
  } else if (!buf.append(value.manifest_blob)) {
    return false;
  }
  return buf.append(value.cksum_blob);
}

//--------------------------------------------------------------------------------
void ChildEValue::serialize(uint8_t *dst) const
{
  ChildValueHeader be = hdr;
  child_hdr_to_be(be);
  std::memcpy(dst, &be, sizeof(be));
  dst += sizeof(be);
  if (!metadata_frame.empty()) {
    std::memcpy(dst, metadata_frame.data(), metadata_frame.size());
    dst += metadata_frame.size();
  }
  if (!tail.empty()) {
    std::memcpy(dst, tail.data(), tail.size());
  }
}

std::optional<ChildEParsed> parse_child_e_value(std::string_view data,
                                                bool has_metadata)
{
  ChildEParsed out;
  if (!parse_child_value_header(data, out.hdr)) {
    return std::nullopt;
  }
  std::string_view rest = data.substr(sizeof(ChildValueHeader));
  if (has_metadata) {
    const std::span<const uint8_t> sp(
        reinterpret_cast<const uint8_t *>(rest.data()), rest.size());
    size_t frame_size = 0;
    if (!encoded_metadata_frame_size(sp, frame_size)) {
      return std::nullopt;
    }
    out.metadata_frame = sp.first(frame_size);
    rest.remove_prefix(frame_size);
  }
  out.tail = rest;
  return out;
}

void child_hdr_to_be(ChildValueHeader &hdr)
{
  hdr.ref_count = htonl(hdr.ref_count);
}

void child_hdr_from_be(ChildValueHeader &hdr)
{
  hdr.ref_count = be32toh(hdr.ref_count);
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

ChildValueBuf::ChildValueBuf(ChildValueHeader hdr, std::string_view payload)
{
  hdr_len_ = sizeof(hdr);
  alloc(sizeof(hdr) + payload.size());
  child_hdr_to_be(hdr);
  std::memcpy(p_, &hdr, sizeof(hdr));
  if (!payload.empty()) {
    std::memcpy(p_ + sizeof(hdr), payload.data(), payload.size());
  }
}

ChildValueBuf::ChildValueBuf(std::string_view payload)
{
  alloc(payload.size());
  if (!payload.empty()) {
    std::memcpy(p_, payload.data(), payload.size());
  }
}

ChildValueBuf::ChildValueBuf(std::string_view head, std::string_view tail)
{
  hdr_len_ = head.size();
  alloc(head.size() + tail.size());
  if (!head.empty()) {
    std::memcpy(p_, head.data(), head.size());
  }
  if (!tail.empty()) {
    std::memcpy(p_ + head.size(), tail.data(), tail.size());
  }
}

ChildValueBuf::ChildValueBuf(const ChildEValue &e)
{
  hdr_len_ = sizeof(ChildValueHeader);
  alloc(e.size());
  e.serialize(p_);
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
  // bucket_id is already BE — store directly
  hdr.bucket_id = bucket_id;
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
  if (data.size() < sizeof(BucketValueHeader)) {
    return std::nullopt;
  }
  BucketValueHeader hdr{};
  std::memcpy(&hdr, data.data(), sizeof(hdr));
  BucketValue value;
  // bucket_id bytes are already BE — load directly
  value.bucket_id = hdr.bucket_id;
  value.created_at_unix = static_cast<int64_t>(
      be64toh(static_cast<uint64_t>(hdr.created_at_unix)));
  value.access_flags = hdr.access_flags;
  value.versioning_state = hdr.versioning_state;
  if (data.size() > sizeof(BucketValueHeader)) {
    value.policy_json.assign(data.data() + sizeof(BucketValueHeader),
                             data.size() - sizeof(BucketValueHeader));
  }
  return value;
}

} // namespace kvrgw
