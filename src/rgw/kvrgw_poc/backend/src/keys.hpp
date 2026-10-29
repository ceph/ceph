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

#include "key_buf.hpp"
#include "ref_tag.hpp"
#include "typed_ids.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace kvrgw {

// Shared string helpers
std::string prefix_range_end(std::string_view prefix, std::string_view *suffix = nullptr);
bool starts_with(std::string_view value, std::string_view prefix);

struct BucketKeyParts {
  tenant_id_t tenant_id{};
  std::string bucket_name;
};

struct ObjectKeyParts {
  uint16_t shard_count{};
  uint16_t shard_id{};
  bucket_id_t bucket_id{};
  std::string object_name;
};

// Zero-copy variant — object_name is a view into the original key buffer.
struct ObjectKeyPartsView {
  uint16_t shard_count{};
  uint16_t shard_id{};
  bucket_id_t bucket_id{};
  std::string_view object_name;
};

struct PoKeyParts {
  bucket_id_t bucket_id{};
  RefTag ref_tag{};
};

struct GoKeyParts {
  uint8_t size_tier{};
  uint16_t shard_count{};
  uint16_t shard_id{};
  bucket_id_t bucket_id{};
  RefTag ref_tag{};
};

struct DKeyParts {
  uint16_t shard_count{};
  uint16_t shard_id{};
  bucket_id_t bucket_id{};
  uint8_t size_tier{};
  uint8_t hash_prefix{};
  uint32_t mtime{};
  RefTag ref_tag{};
};

struct LKeyParts {
  char type{};
  std::string name;
};

KeyBuf make_l_key(char type, std::string_view name);
std::optional<LKeyParts> parse_l_key(std::string_view key);
bool is_valid_l_type(char type);

void   make_bucket_key(tenant_id_t tenant_id, std::string_view bucket_name, KeyBuf& out);
KeyBuf make_bucket_prefix(tenant_id_t tenant_id);
KeyBuf make_tenant_key(std::string_view tenant_name);
std::optional<BucketKeyParts> parse_bucket_key(std::string_view key);
std::optional<std::string_view> parse_bucket_key_view(std::string_view key);

// Prefer the out-param overload below to avoid a KeyBuf copy.
// Value-returning form retained only for listing scan-boundary call sites
// that must convert to std::string (e.g. prefix_range_end inputs).
KeyBuf make_object_key(bucket_id_t bucket_id, std::string_view object_name);
void   make_object_key(bucket_id_t bucket_id, std::string_view object_name, KeyBuf& out);
KeyBuf make_object_prefix(bucket_id_t bucket_id);
KeyBuf make_version_prefix(bucket_id_t bucket_id);
std::optional<ObjectKeyParts> parse_object_key(std::string_view key);
std::optional<std::string_view> parse_object_key_view(std::string_view key);
std::optional<ObjectKeyPartsView> parse_object_key_parts_view(std::string_view key);

void   make_po_key(
    bucket_id_t bucket_id,
    std::string_view ref_tag_bytes,
    KeyBuf& out);
void   make_group_po_key(
    std::string_view group_ref_tag,
    KeyBuf& out);
KeyBuf make_p_prefix();
KeyBuf make_p_bucket_prefix(bucket_id_t bucket_id);
std::optional<PoKeyParts> parse_po_key(std::string_view key);
bool   parse_group_po_key(std::string_view key);

// P:M — Sweeper-driven multipart cleanup entry (see
// plans/fixing_agent_mess.md Section 4). Key: KeyHeaderPM + ref_tag(12B).
void   make_pm_key(bucket_id_t bucket_id, std::string_view ref_tag, KeyBuf& out);
KeyBuf make_pm_bucket_prefix(bucket_id_t bucket_id);

struct PmKeyParts {
  bucket_id_t bucket_id{};
  RefTag ref_tag{};
};
std::optional<PmKeyParts> parse_pm_key(std::string_view key);

void make_go_key(
    uint16_t shard_count,
    uint16_t shard_id,
    bucket_id_t bucket_id,
    std::string_view ref_tag_bytes,
    uint64_t object_size,
    KeyBuf& out);
KeyBuf make_g_prefix();
std::optional<GoKeyParts> parse_go_key(std::string_view key);

uint8_t size_tier_from_size(uint64_t object_size_bytes);
uint64_t size_tier_min_bytes(uint8_t tier);
uint64_t size_tier_max_bytes(uint8_t tier);

void make_d_key(
    bucket_id_t bucket_id,
    uint8_t size_tier,
    std::string_view ref_tag,
    uint32_t mtime,
    KeyBuf& out);
std::optional<DKeyParts> parse_d_key(std::string_view key);
uint8_t d_size_tier_from_size(uint64_t object_size_bytes);
uint8_t d_hash_prefix(std::string_view ref_tag);

struct VersionKeyParts {
  uint16_t shard_count{};
  uint16_t shard_id{};
  bucket_id_t bucket_id{};
  std::string object_name;
  version_id_t version_id{};
};

void   make_v_key(bucket_id_t bucket_id, std::string_view object_name, version_id_t version_id, KeyBuf& out);
KeyBuf make_v_prefix(bucket_id_t bucket_id, std::string_view object_name);
std::optional<VersionKeyParts> parse_v_key(std::string_view key);
std::optional<std::string_view> parse_v_key_view(std::string_view key);

KeyBuf make_r_key(std::string_view ref_tag);

void   make_ct_key(bucket_id_t bucket_id, std::string_view ref_tag, KeyBuf& out);
void   make_c_prefix(bucket_id_t bucket_id, std::string_view ref_tag, KeyBuf& out);

struct GroupGoKeyParts {
  RefTag group_ref_tag{};
};

KeyBuf make_group_go_key(std::string_view ref_tag);
std::optional<GroupGoKeyParts> parse_group_go_key(std::string_view key);

// ---------------------------------------------------------------------------
// Multipart upload — S:M (head row only; see S:U below for parts).
// Created by InitiateMultipartUpload, it is not a part.
// It carries no part_number field at all, and lives alone in category 'M'
//
// Wire layout (NUL delimiter mirrors S:V's object_name/version_id trick —
// object_name is raw, user-controlled bytes, so a fixed-size binary
// suffix immediately after it needs a delimiter to prevent two different
// object_names from producing colliding raw key bytes):
//   KeyHeaderS(cat='M') + object_name + '\0' + ref_tag(12B)
// ---------------------------------------------------------------------------

void make_m_head_key(bucket_id_t bucket_id, std::string_view object_name,
                     std::string_view ref_tag, KeyBuf& out);
// Bucket-wide prefix (header only) — used by ListMultipartUploads. Every row
// this prefix matches is a head row.
void make_m_bucket_prefix(bucket_id_t bucket_id, KeyBuf& out);

struct MHeadKeyParts {
  bucket_id_t bucket_id{};
  std::string_view object_name;  // view into `key`
  RefTag ref_tag{};
};

// object_name = key[hdr_len:-12] up to the NUL delimiter, ref_tag = key[-12:].
std::optional<MHeadKeyParts> parse_m_head_key_view(std::string_view key);

// ---------------------------------------------------------------------------
// S:U — multipart Uploaded_part row (part_number 1-10000), written/updated by
// UploadPart. Separate category from the head — ref_tag already globally
// and uniquely identifies the upload, so part rows never need object_name.
// Fixed-size, no delimiter needed (no variable-length field at all):
//   KeyHeaderS(cat='U') + ref_tag(12B) + part_number(2B BE) + generation(4B BE)
// `generation` is 32-bit (widened from 16-bit) so it can grow monotonically
// forever across a part's lifetime, independent of the small bound on
// concurrently-outstanding generations -- see
// plans/multipart_upload_part_refactor.md Sub-Task 3.
// ---------------------------------------------------------------------------

void make_u_part_key(bucket_id_t bucket_id, std::string_view ref_tag,
                     uint16_t part_number, uint32_t generation, KeyBuf& out);
// Whole-upload parts prefix (header + ref_tag) — every part_number, every
// generation, for this upload. Does NOT include the head (different
// category). Used by ListParts, CompleteMultipartUpload's
// scan_and_resolve_parts, and GcWorker's G:M processing.
void make_u_prefix(bucket_id_t bucket_id, std::string_view ref_tag, KeyBuf& out);
// Single-part, all-generations prefix. Used by UploadPart Phase 1/3 to scan
// the whole row for one part_number.
void make_u_part_prefix(bucket_id_t bucket_id, std::string_view ref_tag,
                        uint16_t part_number, KeyBuf& out);

struct UKeyParts {
  bucket_id_t bucket_id{};
  RefTag ref_tag{};
  uint16_t part_number{};
  uint32_t generation{};
};
// Fixed-offset parse — no prefix length or delimiter needed, every field is
// a known fixed size.
std::optional<UKeyParts> parse_u_key(std::string_view key);

// ---------------------------------------------------------------------------
// G:M — whole-upload cleanup directive (DEEP/MIXED), written by
// Complete/Abort in the same txn that deletes the S:M head.
// Fixed-size, no object_name — GcWorker's G:M scans S:U by ref_tag, never
// touches the head (already deleted synchronously):
//   KeyHeaderG(cat='M') + ref_tag(12B)
// ---------------------------------------------------------------------------

void make_gm_key(bucket_id_t bucket_id, std::string_view ref_tag, KeyBuf& out);

struct GmKeyParts {
  bucket_id_t bucket_id{};
  RefTag ref_tag{};
};
std::optional<GmKeyParts> parse_gm_key(std::string_view key);

// ---------------------------------------------------------------------------
// G:U — per-part-generation cleanup, always a group of 1-20 entries.
// Unified: covers both a single re-upload obsoletion (count=1) and
// CompleteMultipartUpload's batched excluded-parts cleanup (count up to
// kMaxBatchSize) — no separate single-entry category. Every entry in one
// group shares the key's ref_tag; (part_number, generation) of the group's
// first entry is the unique discriminator. Fixed-size key, no object_name:
//   KeyHeaderG(cat='U') + ref_tag(12B) + part_number(2B BE) + generation(4B BE)
// generation is 32-bit, same widening as S:U — see keys.hpp S:U comment.
// Value (count/sum_size/entries) — see GuValueBuf, multipart_value.hpp.
// ---------------------------------------------------------------------------

void make_gu_key(bucket_id_t bucket_id, std::string_view ref_tag,
                 uint16_t part_number, uint32_t generation, KeyBuf& out);

struct GuKeyParts {
  bucket_id_t bucket_id{};
  RefTag ref_tag{};
  uint16_t part_number{};
  uint32_t generation{};
};
std::optional<GuKeyParts> parse_gu_key(std::string_view key);

}  // namespace kvrgw
