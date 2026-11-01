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

#include "keys.hpp"

#include "constants.hpp"

#include <arpa/inet.h>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstring>
#include <endian.h>

namespace kvrgw {
// Shared with service_impl_multipart.cpp (declared in service_impl.hpp) —
//--------------------------------------------------------------------------------
std::string prefix_range_end(std::string_view prefix, std::string_view *suffix)
{
  std::string end;
  if (!suffix) {
    end.reserve(prefix.size());
    end.append(prefix);
  }
  else {
    end.reserve(prefix.size() + suffix->size());
    end.append(prefix);
    end.append(*suffix);
  }

  while (!end.empty()) {
    const unsigned char last = static_cast<unsigned char>(end.back());
    if (last < 0xFF) {
      end.back() = static_cast<char>(last + 1);
      return end;
    }
    end.pop_back();
  }
  return std::string("\xFF", 1);
}

//--------------------------------------------------------------------------------
bool starts_with(std::string_view value, std::string_view prefix)
{
  return value.size() >= prefix.size() &&
         value.substr(0, prefix.size()) == prefix;
}

uint8_t size_tier_from_size(uint64_t object_size_bytes)
{
  if (object_size_bytes == 0) {
    return 0;
  }
  const int log2_val = static_cast<int>(
      std::floor(std::log2(static_cast<double>(object_size_bytes))));
  const int tier = log2_val - 10;
  if (tier < 0) {
    return 0;
  }
  if (tier > 34) {
    return 34;
  }
  return static_cast<uint8_t>(tier);
}

uint64_t size_tier_min_bytes(uint8_t tier)
{
  if (tier == 0) {
    return 0;
  }
  return 1ULL << (static_cast<unsigned>(tier) + 10);
}

uint64_t size_tier_max_bytes(uint8_t tier)
{
  if (tier >= 34) {
    return UINT64_MAX;
  }
  return (1ULL << (static_cast<unsigned>(tier) + 11)) - 1;
}

uint8_t d_size_tier_from_size(uint64_t object_size_bytes)
{
  if (object_size_bytes <= 1) {
    return 0;
  }
  int log2_val = 0;
  uint64_t v = object_size_bytes - 1;
  while (v >>= 1) {
    ++log2_val;
  }
  return static_cast<uint8_t>(log2_val);
}

uint8_t d_hash_prefix(std::string_view ref_tag)
{
  if (ref_tag.size() < 4) {
    return 0;
  }
  uint32_t hash = 2166136261u;
  for (size_t i = 0; i < ref_tag.size(); ++i) {
    hash ^= static_cast<uint8_t>(ref_tag[i]);
    hash *= 16777619u;
  }
  return static_cast<uint8_t>(hash % 32);
}

bool is_valid_l_type(char type)
{
  return type == kLocalTypeNumeric || type == kLocalTypeIdMap;
}

KeyBuf make_l_key(char type, std::string_view name)
{
  assert(is_valid_l_type(type));
  assert(!name.empty() && name.size() <= 64);
  KeyBuf key;
  KeyHeaderL hdr(type);
  key.set_header(hdr);
  key.append(name.data(), name.size());
  return key;
}

std::optional<LKeyParts> parse_l_key(std::string_view key)
{
  if (key.size() < 3 || key[0] != kNamespaceLocal) {
    return std::nullopt;
  }
  if (!is_valid_l_type(key[1])) {
    return std::nullopt;
  }
  const std::string_view name = key.substr(2);
  if (name.empty() || name.size() > 64) {
    return std::nullopt;
  }
  LKeyParts parts;
  parts.type = key[1];
  parts.name.assign(name);
  return parts;
}

void make_bucket_key(tenant_id_t tenant_id, std::string_view bucket_name, KeyBuf& out)
{
  KeyHeaderB hdr(tenant_id);
  out.set_header(hdr);
  out.append(bucket_name.data(), bucket_name.size());
}

KeyBuf make_bucket_prefix(tenant_id_t tenant_id)
{
  KeyBuf key;
  KeyHeaderB hdr(tenant_id);
  key.set_header(hdr);
  return key;
}

KeyBuf make_tenant_key(std::string_view tenant_name)
{
  KeyBuf key;
  key.data[0] = static_cast<uint8_t>(kNamespaceTenant);
  key.len = 1;
  key.append(tenant_name.data(), tenant_name.size());
  return key;
}

std::optional<BucketKeyParts> parse_bucket_key(std::string_view key)
{
  constexpr size_t kMinSize = sizeof(KeyHeaderB) + AWS_MinBucketNameLen;
  if (key.size() < kMinSize || key[0] != kNamespaceBucket) [[unlikely]] {
    return std::nullopt;
  }
  BucketKeyParts parts;
  uint32_t net{};
  std::memcpy(&net, key.data() + offsetof(KeyHeaderB, tenant_id), sizeof(net));
  parts.tenant_id = be32toh(net);
  parts.bucket_name.assign(key.substr(sizeof(KeyHeaderB)));
  if (parts.bucket_name.size() > AWS_MaxBucketNameLen) [[unlikely]] {
    return std::nullopt;
  }
  return parts;
}

std::optional<std::string_view> parse_bucket_key_view(std::string_view key)
{
  constexpr size_t kMinSize = sizeof(KeyHeaderB) + AWS_MinBucketNameLen;
  if (key.size() < kMinSize || key[0] != kNamespaceBucket) [[unlikely]] {
    return std::nullopt;
  }
  const std::string_view name = key.substr(sizeof(KeyHeaderB));
  if (name.size() > AWS_MaxBucketNameLen) [[unlikely]] {
    return std::nullopt;
  }
  return name;
}

void make_object_key(bucket_id_t bucket_id, std::string_view object_name, KeyBuf& out)
{
  KeyHeaderS hdr('S', kShardCount, kShardId, bucket_id, kCategoryObject);
  out.set_header(hdr);
  out.append(object_name.data(), object_name.size());
}

KeyBuf make_object_key(bucket_id_t bucket_id, std::string_view object_name)
{
  KeyBuf key;
  make_object_key(bucket_id, object_name, key);
  return key;
}

KeyBuf make_object_prefix(bucket_id_t bucket_id)
{
  KeyBuf key;
  KeyHeaderS hdr('S', kShardCount, kShardId, bucket_id, kCategoryObject);
  key.set_header(hdr);
  return key;
}

KeyBuf make_version_prefix(bucket_id_t bucket_id)
{
  KeyBuf key;
  KeyHeaderS hdr('S', kShardCount, kShardId, bucket_id, kCategoryVersion);
  key.set_header(hdr);
  return key;
}

std::optional<ObjectKeyParts> parse_object_key(std::string_view key)
{
  constexpr size_t kHdrSize = sizeof(KeyHeaderS);
  if (key.size() < kHdrSize + 1 || key[0] != kNamespaceObject ||
      key[offsetof(KeyHeaderS, cat)] != kCategoryObject) [[unlikely]] {
    return std::nullopt;
  }
  ObjectKeyParts parts;
  uint16_t sc_net{}, si_net{};
  std::memcpy(&sc_net, key.data() + offsetof(KeyHeaderS, shard_count), sizeof(sc_net));
  std::memcpy(&si_net, key.data() + offsetof(KeyHeaderS, shard_id), sizeof(si_net));
  parts.shard_count = be16toh(sc_net);
  parts.shard_id = be16toh(si_net);
  parts.bucket_id.load(key.data() + offsetof(KeyHeaderS, bucket_id));
  parts.object_name.assign(key.substr(kHdrSize));
  if (parts.object_name.empty() || parts.object_name.size() > AWS_MaxObjectNameLen) {
    return std::nullopt;
  }
  return parts;
}

std::optional<std::string_view> parse_object_key_view(std::string_view key)
{
  constexpr size_t kHdrSize = sizeof(KeyHeaderS);
  if (key.size() < kHdrSize + 1 || key[0] != kNamespaceObject ||
      key[offsetof(KeyHeaderS, cat)] != kCategoryObject) [[unlikely]] {
    return std::nullopt;
  }
  const std::string_view name = key.substr(kHdrSize);
  if (name.size() > AWS_MaxObjectNameLen) {
    return std::nullopt;
  }
  return name;
}

std::optional<ObjectKeyPartsView> parse_object_key_parts_view(std::string_view key)
{
  constexpr size_t kHdrSize = sizeof(KeyHeaderS);
  if (key.size() < kHdrSize + 1 || key[0] != kNamespaceObject ||
      key[offsetof(KeyHeaderS, cat)] != kCategoryObject) [[unlikely]] {
    return std::nullopt;
  }
  ObjectKeyPartsView parts;
  uint16_t sc_net{}, si_net{};
  std::memcpy(&sc_net, key.data() + offsetof(KeyHeaderS, shard_count), sizeof(sc_net));
  std::memcpy(&si_net, key.data() + offsetof(KeyHeaderS, shard_id), sizeof(si_net));
  parts.shard_count = be16toh(sc_net);
  parts.shard_id    = be16toh(si_net);
  parts.bucket_id.load(key.data() + offsetof(KeyHeaderS, bucket_id));
  parts.object_name = key.substr(kHdrSize);
  if (parts.object_name.empty() || parts.object_name.size() > AWS_MaxObjectNameLen) {
    return std::nullopt;
  }
  return parts;
}

void make_po_key(bucket_id_t bucket_id, std::string_view ref_tag_bytes, KeyBuf& out)
{
  KeyHeaderPO hdr(bucket_id);
  out.set_header(hdr);
  out.append(ref_tag_bytes.data(), ref_tag_bytes.size());
}

void make_group_po_key(std::string_view group_ref_tag, KeyBuf& out)
{
  KeyHeaderPOGroup hdr{};
  out.set_header(hdr);
  out.append(group_ref_tag.data(), group_ref_tag.size());
}

KeyBuf make_p_prefix()
{
  KeyBuf key;
  key.data[0] = static_cast<uint8_t>(kNamespacePending);
  key.len = 1;
  return key;
}

KeyBuf make_p_bucket_prefix(bucket_id_t bucket_id)
{
  KeyBuf key;
  KeyHeaderPO hdr(bucket_id);
  key.set_header(hdr);
  return key;
}

// Single P:O key: [ns='P'][bucket_id 8B][cat='O'][ref_tag 12B] = 22B
std::optional<PoKeyParts> parse_po_key(std::string_view key)
{
  constexpr size_t kPoKeySize = sizeof(KeyHeaderPO) + kRefTagSize;
  if (key.size() != kPoKeySize || key[0] != kNamespacePending ||
      key[offsetof(KeyHeaderPO, cat)] != kCategoryObject) [[unlikely]] {
    return std::nullopt;
  }
  PoKeyParts parts;
  parts.bucket_id.load(key.data() + offsetof(KeyHeaderPO, bucket_id));
  parts.ref_tag.load(key.data() + sizeof(KeyHeaderPO));
  return parts;
}

// Group P:O key: [ns='P'][cat='G'][ref_tag_0 12B] = 14B
bool parse_group_po_key(std::string_view key)
{
  constexpr size_t kGroupPoKeySize = sizeof(KeyHeaderPOGroup) + kRefTagSize;
  return key.size() == kGroupPoKeySize &&
         key[0] == kNamespacePending &&
         key[offsetof(KeyHeaderPOGroup, cat)] == kOpTypeGroup;
}

void make_pm_key(bucket_id_t bucket_id, std::string_view ref_tag, KeyBuf& out)
{
  KeyHeaderPM hdr(bucket_id);
  out.set_header(hdr);
  out.append(ref_tag.data(), ref_tag.size());
}

KeyBuf make_pm_bucket_prefix(bucket_id_t bucket_id)
{
  KeyBuf key;
  KeyHeaderPM hdr(bucket_id);
  key.set_header(hdr);
  return key;
}

// Single P:M key: [ns='P'][cat='M'][bucket_id 8B][ref_tag 12B] = 22B
std::optional<PmKeyParts> parse_pm_key(std::string_view key)
{
  constexpr size_t kPmKeySize = sizeof(KeyHeaderPM) + kRefTagSize;
  if (key.size() != kPmKeySize || key[0] != kNamespacePending ||
      key[offsetof(KeyHeaderPM, cat)] != kCategoryMultipart) [[unlikely]] {
    return std::nullopt;
  }
  PmKeyParts parts;
  parts.bucket_id.load(key.data() + offsetof(KeyHeaderPM, bucket_id));
  parts.ref_tag.load(key.data() + sizeof(KeyHeaderPM));
  return parts;
}

void make_go_key(uint16_t shard_count, uint16_t shard_id, bucket_id_t bucket_id,
                 std::string_view ref_tag_bytes, uint64_t object_size, KeyBuf& out)
{
  KeyHeaderG hdr(size_tier_from_size(object_size), shard_count, shard_id,
                 bucket_id, kCategoryObject);
  out.set_header(hdr);
  out.append(ref_tag_bytes.data(), ref_tag_bytes.size());
}

KeyBuf make_g_prefix()
{
  KeyBuf key;
  key.data[0] = static_cast<uint8_t>(kNamespaceGc);
  key.len = 1;
  return key;
}

std::optional<GoKeyParts> parse_go_key(std::string_view key)
{
  constexpr size_t kGoFixedKeySize = sizeof(KeyHeaderG) + kRefTagSize;
  if (key.size() != kGoFixedKeySize || key[0] != kNamespaceGc ||
      key[offsetof(KeyHeaderG, cat)] != kCategoryObject) [[unlikely]] {
    return std::nullopt;
  }
  GoKeyParts parts;
  parts.size_tier = static_cast<uint8_t>(key[offsetof(KeyHeaderG, size_tier)]);
  uint16_t sc_net{}, si_net{};
  std::memcpy(&sc_net, key.data() + offsetof(KeyHeaderG, shard_count), sizeof(sc_net));
  std::memcpy(&si_net, key.data() + offsetof(KeyHeaderG, shard_id), sizeof(si_net));
  parts.shard_count = be16toh(sc_net);
  parts.shard_id = be16toh(si_net);
  parts.bucket_id.load(key.data() + offsetof(KeyHeaderG, bucket_id));
  parts.ref_tag.load(key.data() + sizeof(KeyHeaderG));
  return parts;
}

void make_d_key(bucket_id_t bucket_id, uint8_t size_tier,
                std::string_view ref_tag, uint32_t mtime, KeyBuf& out)
{
  KeyHeaderD hdr(kShardCount, kShardId, bucket_id, size_tier,
                 d_hash_prefix(ref_tag), mtime, ref_tag.data());
  out.set_header(hdr);
}

std::optional<DKeyParts> parse_d_key(std::string_view key)
{
  constexpr size_t kDKeySize = sizeof(KeyHeaderD);
  if (key.size() != kDKeySize || key[0] != kNamespaceData) [[unlikely]] {
    return std::nullopt;
  }
  DKeyParts parts;
  uint16_t sc_net{}, si_net{};
  std::memcpy(&sc_net, key.data() + offsetof(KeyHeaderD, shard_count), sizeof(sc_net));
  std::memcpy(&si_net, key.data() + offsetof(KeyHeaderD, shard_id), sizeof(si_net));
  parts.shard_count = be16toh(sc_net);
  parts.shard_id = be16toh(si_net);
  parts.bucket_id.load(key.data() + offsetof(KeyHeaderD, bucket_id));
  parts.size_tier = static_cast<uint8_t>(key[offsetof(KeyHeaderD, size_tier)]);
  parts.hash_prefix = static_cast<uint8_t>(key[offsetof(KeyHeaderD, hash_prefix)]);
  uint32_t mt_net{};
  std::memcpy(&mt_net, key.data() + offsetof(KeyHeaderD, mtime), sizeof(mt_net));
  parts.mtime = be32toh(mt_net);
  parts.ref_tag.load(key.data() + offsetof(KeyHeaderD, ref_tag));
  return parts;
}

void make_v_key(bucket_id_t bucket_id, std::string_view object_name,
                version_id_t version_id, KeyBuf& out)
{
  KeyHeaderS hdr('S', kShardCount, kShardId, bucket_id, kCategoryVersion);
  out.set_header(hdr);
  out.append(object_name.data(), object_name.size());
  out.append_byte(kDelimChar);
  char buff[sizeof(version_id_t)];
  version_id.serialize(buff);
  out.append(buff, sizeof(buff));
}

KeyBuf make_v_prefix(bucket_id_t bucket_id, std::string_view object_name)
{
  KeyBuf key;
  KeyHeaderS hdr('S', kShardCount, kShardId, bucket_id, kCategoryVersion);
  key.set_header(hdr);
  key.append(object_name.data(), object_name.size());
  key.append_byte(kDelimChar);
  return key;
}

std::optional<VersionKeyParts> parse_v_key(std::string_view key)
{
  constexpr size_t kNul = sizeof(kDelimChar);
  if (key.size() < sizeof(KeyHeaderS) + kNul + sizeof(version_id_t) ||
      key[0] != kNamespaceObject ||
      key[offsetof(KeyHeaderS, cat)] != kCategoryVersion) [[unlikely]] {
    return std::nullopt;
  }
  const size_t vid_off = key.size() - sizeof(version_id_t);
  if (key[vid_off - 1] != kDelimChar) [[unlikely]] {
    return std::nullopt;
  }
  VersionKeyParts parts;
  uint16_t sc_net{}, si_net{};
  std::memcpy(&sc_net, key.data() + offsetof(KeyHeaderS, shard_count), sizeof(sc_net));
  std::memcpy(&si_net, key.data() + offsetof(KeyHeaderS, shard_id), sizeof(si_net));
  parts.shard_count = be16toh(sc_net);
  parts.shard_id = be16toh(si_net);
  parts.bucket_id.load(key.data() + offsetof(KeyHeaderS, bucket_id));
  parts.object_name.assign(key.data() + sizeof(KeyHeaderS),
                           vid_off - sizeof(KeyHeaderS) - kNul);
  parts.version_id = version_id_t::deserialize(key.data() + vid_off);
  return parts;
}

std::optional<std::string_view> parse_v_key_view(std::string_view key)
{
  constexpr size_t kNul = sizeof(kDelimChar);
  constexpr size_t kMinSize = sizeof(KeyHeaderS) + kNul + sizeof(version_id_t);
  if (key.size() < kMinSize || key[0] != kNamespaceObject ||
      key[offsetof(KeyHeaderS, cat)] != kCategoryVersion) [[unlikely]] {
    return std::nullopt;
  }
  const size_t vid_off  = key.size() - sizeof(version_id_t);
  if (key[vid_off - 1] != kDelimChar) [[unlikely]] {
    return std::nullopt;
  }
  const size_t name_off = sizeof(KeyHeaderS);
  const size_t name_len = vid_off - name_off - kNul;
  if (name_len == 0 || name_len > AWS_MaxObjectNameLen) [[unlikely]] {
    return std::nullopt;
  }
  return key.substr(name_off, name_len);
}

KeyBuf make_r_key(std::string_view ref_tag)
{
  KeyBuf key;
  const char ns = 'R';
  key.append(&ns, 1);
  key.append(ref_tag.data(), ref_tag.size());
  return key;
}

void make_ct_key(bucket_id_t bucket_id, std::string_view ref_tag, KeyBuf& out)
{
  KeyHeaderS hdr('S', kShardCount, kShardId, bucket_id, kCategoryChild);
  out.set_header(hdr);
  out.append(ref_tag.data(), ref_tag.size());
  const char child_type = kChildTypeTags;
  out.append(&child_type, 1);
}

void make_c_prefix(bucket_id_t bucket_id, std::string_view ref_tag, KeyBuf& out)
{
  KeyHeaderS hdr('S', kShardCount, kShardId, bucket_id, kCategoryChild);
  out.set_header(hdr);
  out.append(ref_tag.data(), ref_tag.size());
}

KeyBuf make_group_go_key(std::string_view ref_tag)
{
  KeyBuf key;
  KeyHeaderGGroup hdr{};
  key.set_header(hdr);
  key.append(ref_tag.data(), ref_tag.size());
  return key;
}

std::optional<GroupGoKeyParts> parse_group_go_key(std::string_view key)
{
  constexpr size_t kGroupGoKeySize = sizeof(KeyHeaderGGroup) + kRefTagSize;
  if (key.size() != kGroupGoKeySize || key[0] != kNamespaceGc ||
      key[offsetof(KeyHeaderGGroup, cat)] != kOpTypeGroup) [[unlikely]] {
    return std::nullopt;
  }
  GroupGoKeyParts parts;
  parts.group_ref_tag.load(key.data() + sizeof(KeyHeaderGGroup));
  return parts;
}

 //---------------------------------------------------------------------------
// Multipart upload — S:M
//---------------------------------------------------------------------------

//--------------------------------------------------------------------------------
void make_m_bucket_prefix(bucket_id_t bucket_id, KeyBuf& out)
{
  KeyHeaderS hdr('S', kShardCount, kShardId, bucket_id, kCategoryMultipart);
  out.set_header(hdr);
}

//--------------------------------------------------------------------------------
void make_m_head_key(bucket_id_t bucket_id, std::string_view object_name,
                     std::string_view ref_tag, KeyBuf& out)
{
  make_m_bucket_prefix(bucket_id, out);
  out.append(object_name.data(), object_name.size());
  out.append_byte(kDelimChar);
  out.append(ref_tag.data(), ref_tag.size());
}

//--------------------------------------------------------------------------------
void make_u_prefix(bucket_id_t bucket_id, std::string_view ref_tag, KeyBuf& out)
{
  KeyHeaderS hdr('S', kShardCount, kShardId, bucket_id, kCategoryMultipartPart);
  out.set_header(hdr);
  out.append(ref_tag.data(), ref_tag.size());
}

//--------------------------------------------------------------------------------
void make_u_part_prefix(bucket_id_t bucket_id, std::string_view ref_tag,
                        uint16_t part_number, KeyBuf& out)
{
  make_u_prefix(bucket_id, ref_tag, out);
  const uint16_t part_number_be = htons(part_number);
  out.append(&part_number_be, sizeof(part_number_be));
}

//--------------------------------------------------------------------------------
void make_u_part_key(bucket_id_t bucket_id, std::string_view ref_tag,
                     uint16_t part_number, uint32_t generation, KeyBuf& out)
{
  make_u_part_prefix(bucket_id, ref_tag, part_number, out);
  const uint32_t generation_be = htonl(generation);
  out.append(&generation_be, sizeof(generation_be));
}

//--------------------------------------------------------------------------------
std::optional<UKeyParts> parse_u_key(std::string_view key)
{
  constexpr size_t kHdrSize = sizeof(KeyHeaderS);
  constexpr size_t kExpected = kHdrSize + sizeof(RefTag) +
      sizeof(UKeyParts::part_number) + sizeof(UKeyParts::generation);
  if (key.size() != kExpected) [[unlikely]] {
    return std::nullopt;
  }
  UKeyParts out;
  out.bucket_id.load(key.data() + offsetof(KeyHeaderS, bucket_id));
  out.ref_tag.load(reinterpret_cast<const uint8_t*>(key.data() + kHdrSize));
  uint16_t pn_be{};
  uint32_t gen_be{};
  std::memcpy(&pn_be, key.data() + kHdrSize + kRefTagSize, sizeof(pn_be));
  std::memcpy(&gen_be, key.data() + kHdrSize + kRefTagSize + sizeof(pn_be), sizeof(gen_be));
  out.part_number = be16toh(pn_be);
  out.generation = be32toh(gen_be);
  return out;
}

//--------------------------------------------------------------------------------
std::optional<MHeadKeyParts> parse_m_head_key_view(std::string_view key)
{
  constexpr size_t kHdrSize = sizeof(KeyHeaderS);
  constexpr size_t kDelimSize = sizeof(kDelimChar);
  // Category 'M' contains only head rows (see keys.hpp) -- no disambiguation
  // against part rows needed. Tail is '\0'(1) + ref_tag(12) = 13 bytes;
  // object_name is everything between the header and that fixed tail.
  constexpr size_t kTail = kDelimSize + kRefTagSize;
  if (key.size() < kHdrSize + kTail || key[0] != kNamespaceObject ||
      key[offsetof(KeyHeaderS, cat)] != kCategoryMultipart) [[unlikely]] {
    return std::nullopt;
  }
  const size_t delim_pos = key.size() - kTail;
  if (key[delim_pos] != kDelimChar) [[unlikely]] {
    return std::nullopt;
  }
  MHeadKeyParts parts;
  parts.bucket_id.load(key.data() + offsetof(KeyHeaderS, bucket_id));
  parts.object_name = key.substr(kHdrSize, delim_pos - kHdrSize);
  parts.ref_tag.load(key.data() + delim_pos + kDelimSize);
  return parts;
}

//---------------------------------------------------------------------------
// G:M — whole-upload cleanup directive
//---------------------------------------------------------------------------

//--------------------------------------------------------------------------------
void make_gm_key(bucket_id_t bucket_id, std::string_view ref_tag, KeyBuf& out)
{
  KeyHeaderG hdr(0, kShardCount, kShardId, bucket_id, kOpTypeMultipart);
  out.set_header(hdr);
  out.append(ref_tag.data(), ref_tag.size());
}

//--------------------------------------------------------------------------------
std::optional<GmKeyParts> parse_gm_key(std::string_view key)
{
  constexpr size_t kGmKeySize = sizeof(KeyHeaderG) + kRefTagSize;
  if (key.size() != kGmKeySize || key[0] != kNamespaceGc ||
      key[offsetof(KeyHeaderG, cat)] != kOpTypeMultipart) [[unlikely]] {
    return std::nullopt;
  }
  GmKeyParts parts;
  parts.bucket_id.load(key.data() + offsetof(KeyHeaderG, bucket_id));
  parts.ref_tag.load(key.data() + sizeof(KeyHeaderG));
  return parts;
}

//---------------------------------------------------------------------------
// G:U — per-part-generation re-upload cleanup directive
//---------------------------------------------------------------------------

//--------------------------------------------------------------------------------
void make_gu_key(bucket_id_t bucket_id, std::string_view ref_tag,
                 uint16_t part_number, uint32_t generation, KeyBuf& out)
{
  KeyHeaderG hdr(0, kShardCount, kShardId, bucket_id, kOpTypeGcU);
  out.set_header(hdr);
  out.append(ref_tag.data(), ref_tag.size());
  const uint16_t pn_be = htons(part_number);
  out.append(&pn_be, sizeof(pn_be));
  const uint32_t gen_be = htonl(generation);
  out.append(&gen_be, sizeof(gen_be));
}

//--------------------------------------------------------------------------------
std::optional<GuKeyParts> parse_gu_key(std::string_view key)
{
  constexpr size_t kGuKeySize = sizeof(KeyHeaderG) + sizeof(RefTag) +
      sizeof(GuKeyParts::part_number) + sizeof(GuKeyParts::generation);
  if (key.size() != kGuKeySize || key[0] != kNamespaceGc ||
      key[offsetof(KeyHeaderG, cat)] != kOpTypeGcU) [[unlikely]] {
    return std::nullopt;
  }
  GuKeyParts parts;
  parts.bucket_id.load(key.data() + offsetof(KeyHeaderG, bucket_id));
  parts.ref_tag.load(key.data() + sizeof(KeyHeaderG));
  uint16_t pn_be{};
  uint32_t gen_be{};
  std::memcpy(&pn_be, key.data() + sizeof(KeyHeaderG) + kRefTagSize, sizeof(pn_be));
  std::memcpy(&gen_be, key.data() + sizeof(KeyHeaderG) + kRefTagSize + sizeof(uint16_t),
              sizeof(gen_be));
  parts.part_number = be16toh(pn_be);
  parts.generation = be32toh(gen_be);
  return parts;
}

} // namespace kvrgw
