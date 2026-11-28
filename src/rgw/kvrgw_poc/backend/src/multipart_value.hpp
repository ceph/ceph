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
#include "gm_value.hpp"
#include "id_meta.hpp"
#include "multipart_manifest.hpp"
#include "object_value.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace kvrgw {

// ---------------------------------------------------------------------------
// Shared data-tier filename builder for one multipart part's generation --
// "ref_tag-part_number-generation" (see "Data-tier filename" in
// plans/multipart.md). Single source of truth, used by GcWorker (G:U,
// G:M{DEEP,MIXED,FREE_PARTS}) and by GetObject's multipart read path
// (service_impl.cpp) -- never duplicated/reformatted independently.
// Returns empty on snprintf overflow (defensive; should never happen at
// these field widths).
// ---------------------------------------------------------------------------
std::string part_filename(const RefTag& ref_tag, uint16_t part_number,
                          uint32_t generation);

// Tier 3 per-part checksum blob file: "<ref_tag>_checksum".
std::string checksum_filename(const RefTag& ref_tag);
// Same name rendered into a caller-provided buffer (no allocation); `out`
// must hold at least kRefTagSize*2 + 10 bytes. Returns empty if too small.
std::string_view checksum_filename_buf(const RefTag& ref_tag, std::span<char> out);

// ---------------------------------------------------------------------------
// Two-phase placement of a :O: value's variable parts (plans/o_value_layout.md).
//
// Phase 1 decides inline vs external for each item against the 1,024 B
// budget; priority: cksum_combined (forced) > metadata > manifest >
// first_part_info > per-part checksum blob.
// Phase 2 picks the concrete external location (C:E / C:M / C:S / file).
//
// Everything is zero-copy: `v` and `out` only hold views of the caller's
// buffers, which must outlive the write.
// ---------------------------------------------------------------------------
struct MultipartPlacementIn {
  std::span<const uint8_t> metadata_frame;  // full encoded frame (may be empty)
  std::string_view manifest;                // G:M{FREE_PARTS} value
  std::string_view cksum_blob;              // part_count * digest_len; empty when alg == NONE
  FirstPartInfo first_part;                 // lowest-numbered included part
  bool composite{false};                    // kMFlagComposite
  // CopyObject of a source whose authoritative checksum blob is already
  // external (C:S / file): keep that location; nothing is written.
  uint8_t existing_cksum_loc{kCksumInline};
};

struct MultipartPlacementOut {
  std::string_view manifest_child;    // C:M payload, valid iff v.manifest_loc == kManifestChildM
  std::string_view checksum_child;    // C:S payload, valid iff v.cksum_loc == kCksumChildS
  std::string_view checksum_file;     // file payload, valid iff v.cksum_loc == kCksumFile
  bool has_child_e{false};            // a C:E value must be written
  ChildEValue child_e;                // metadata [+ manifest when kManifestChildE]
};

// `v` must carry hdr (incl. chunk_type, flags SharedData if shared, cksum alg,
// etag), content_type and chunk. On return it carries kFlagExternalMetadata,
// metadata_frame (inline copy), the location nibbles, mflags, first_part,
// manifest_blob (local copy or empty) and cksum_blob (local copy or empty).
// Returns false if the manifest exceeds kMaxChildPayloadBytes or a field is
// illegal; `v` is then unspecified.
bool place_multipart(ObjectValue& v, const MultipartPlacementIn& in,
                     MultipartPlacementOut& out);

// Single-part data placement.
enum class DataPlace : uint8_t { kInline, kChildE, kChildD, kStorage };
struct SinglePlacementOut {
  DataPlace data{DataPlace::kInline};
  bool has_child_e{false};   // metadata spilled; C:E carries metadata [+ data]
  ChildEValue child_e;
};
// `v` must carry hdr (flags, size, cksum alg), content_type, chunk. Sets
// kFlagExternalMetadata / kFlagDataInChildE, metadata_frame, inline_data and
// hdr.chunk_type (CHUNK_INLINE for inline and C:E data, CHUNK_CHILD_D,
// CHUNK_STORAGE). `data` is the body when it is in memory (may be empty for
// storage-tier data). Not for REF objects.
bool place_single_part(ObjectValue& v, std::span<const uint8_t> metadata_frame,
                       std::span<const uint8_t> data, uint64_t data_len,
                       uint32_t max_inline, uint32_t max_kv_store,
                       SinglePlacementOut& out);

// ---------------------------------------------------------------------------
// S:M head row value (part_number=0) — upload-level metadata captured at
// InitiateMultipartUpload, consumed by CompleteMultipartUpload to populate
// the final :O: object's content_type/metadata.
//
// Tags are NOT carried here — they go straight to the S:C:T child entry
// keyed by the upload's own ref_tag (make_ct_key), and CompleteMultipartUpload
// reuses that *same* ref_tag for the final object, so the tag entry needs no
// extra copy/write at Complete time. tags_count here is informational only
// (mirrors ObjectValueHeader.tags_count) so Complete can set the final
// object's header field without a second read of the child entry.
// ---------------------------------------------------------------------------
// Upload state machine — see plans/fixing_agent_mess.md Section 5:
//   ACTIVE       — set at Initiate. Normal state; both Phase 1 and Phase 3
//                  proceed.
//   ABORTED      — set by Abort. Terminal; both Phase 1 and Phase 3 refuse
//                  with NoSuchUpload. No transition out.
//   IN_TRANSIENT — set by Complete while it consolidates parts into the
//                  final manifest (a multi-txn operation that can't fit in
//                  one transaction at 10,000 parts). Blocks new Phase 1
//                  arrivals (they poll/wait, Section 5); Phase 3 still
//                  commits unconditionally while IN_TRANSIENT (Section 7).
//                  Transitions back to ACTIVE if Complete's part-validation
//                  fails (InvalidPart) — client can retry. A second
//                  Complete/Abort arriving while IN_TRANSIENT is rejected
//                  (InternalError) — never transitions straight to ABORTED.
enum MUploadState : uint8_t {
  M_UPLOAD_ACTIVE       = 0,
  M_UPLOAD_ABORTED      = 1,
  M_UPLOAD_IN_TRANSIENT = 2,
};

#pragma pack(push, 1)
struct MHeadValueHeader {
  int64_t  initiated_unix_ms{};  // ms since Unix epoch (system_clock)
  uint16_t metadata_count{};
  uint8_t  tags_count{};
  uint8_t  content_type_len{};
  uint8_t  state{M_UPLOAD_ACTIVE};
  uint8_t  cksum_alg{0};         // CksumAlg
  uint8_t  cksum_type{0};        // CksumType
  uint8_t  pad{0};
};
#pragma pack(pop)
static_assert(sizeof(MHeadValueHeader) == 16);

struct MHeadValue {
  MHeadValueHeader hdr{};
  std::string content_type;
  std::vector<uint8_t> metadata_frame;
};

struct MHeadValueBuf {
  static constexpr size_t kMaxSize = (sizeof(MHeadValueHeader) + kMaxContentTypeLen
                                      + MAX_META_FRAME_BYTES);
  uint8_t data[kMaxSize];
  size_t len{};

  std::string_view view() const {
    return {reinterpret_cast<const char*>(data), len};
  }
};

bool make_m_head_value(MHeadValueBuf& buf, int64_t initiated_unix_ms,
                       std::string_view content_type, uint8_t tags_count,
                       std::span<const MetaPair> metadata,
                       CksumAlg cksum_alg = CksumAlg::NONE,
                       CksumType cksum_type = CksumType::NONE);
std::optional<MHeadValue> parse_m_head_value(std::string_view data);

// Zero-copy sibling of parse_m_head_value() — same bounds-checking, but
// returns views into `data` instead of allocating owned content_type/
// metadata_frame containers. Use when the caller only needs to slice and
// assign once into its own destination (e.g. Complete building the final
// :O:) rather than hold an independently-owned MHeadValue.
struct MHeadValueView {
  uint8_t tags_count{};
  CksumAlg cksum_alg{CksumAlg::NONE};
  CksumType cksum_type{CksumType::NONE};
  uint16_t metadata_count{};
  std::string_view content_type;            // view into `data`
  std::span<const uint8_t> metadata_frame;  // view into `data`
};
std::optional<MHeadValueView> view_m_head_value(std::string_view data);

// Fixed-offset access to just the head's `state` byte — avoids a full
// parse_m_head_value()/re-serialize round trip when only checking/flipping
// state (Phase 1/3's head check, Complete/Abort's TXN1). Treats a
// too-short/corrupt value as terminal (ABORTED) defensively.
inline uint8_t m_head_state(std::string_view raw) {
  return raw.size() > offsetof(MHeadValueHeader, state)
             ? static_cast<uint8_t>(raw[offsetof(MHeadValueHeader, state)])
             : static_cast<uint8_t>(M_UPLOAD_ABORTED);
}

// Copies `raw` into `buf` verbatim (bounded by MHeadValueBuf::kMaxSize,
// zero heap allocation — same fixed-size stack-buffer idiom as KeyBuf
// elsewhere) and overwrites just the state byte, leaving
// content_type/metadata_frame bytes untouched/unparsed. Callers already
// need a buffer to kv_put from, so mutate it in place instead of
// allocating a fresh std::string per state transition.
inline void write_m_head_state(std::string_view raw, uint8_t new_state, MHeadValueBuf& buf) {
  const size_t n = std::min(raw.size(), sizeof(buf.data));
  std::memcpy(buf.data, raw.data(), n);
  buf.len = n;
  if (n > offsetof(MHeadValueHeader, state)) {
    buf.data[offsetof(MHeadValueHeader, state)] = new_state;
  }
}

// ---------------------------------------------------------------------------
// S:U part row value (part_number>=1) — fixed-size, no variable tail.
// `chunk`/`etag`/`size` are only meaningful once state==M_PART_COMMITTED.
//
// Two-state model — see plans/fixing_agent_mess.md Sections 3/7:
//   PENDING    — data write in flight, not yet confirmed. Phase 1 bounds a
//                row (one part_number) to at most N total entries
//                (kMaxGenerationsPerPart), evicting the oldest COMMITTED
//                entry (never a PENDING one) to make room; if all N are
//                PENDING, the arrival is rejected outright
//                (KVRGW_ERR_UPLOAD_PART_BUSY) -- no in-place supersession.
//   COMMITTED  — normal finish; data write confirmed. Phase 3 commits
//                unconditionally whenever the head is ACTIVE or
//                IN_TRANSIENT (Section 7), so a row can hold more than one
//                COMMITTED entry at once -- Complete picks the generation
//                whose ETag the client sent (Section 8).
// A row's superseded/abandoned entries never sit in an intermediate
// "marked for teardown" state -- they're either evicted/self-GC'd outright
// (delete S:U + write a single-entry G:U), or simply excluded from the
// keep-list Complete/Abort hands to GC.
// ---------------------------------------------------------------------------
enum MPartState : uint8_t {
  M_PART_PENDING    = 0,
  M_PART_COMMITTED  = 1,
};

#pragma pack(push, 1)
struct MPartValue {
  uint8_t state{M_PART_PENDING};
  uint8_t pad[3]{};
  ChunkDescriptor chunk{};
  ETag etag{};
  uint64_t size{};
  uint32_t mtime{};
  // Raw part digest; the first cksum_digest_len(alg) bytes are meaningful,
  // the algorithm lives in the S:M head. Zero when the upload has no algorithm.
  uint8_t cksum[kCksumMaxDigestBytes]{};
};
#pragma pack(pop)
static_assert(sizeof(MPartValue) == 1 + 3 + sizeof(ChunkDescriptor)
              + sizeof(ETag) + sizeof(uint64_t) + sizeof(uint32_t)
              + kCksumMaxDigestBytes);

void make_m_part_value(const MPartValue& v, uint8_t* dst);
std::optional<MPartValue> parse_m_part_value(std::string_view data);

// ---------------------------------------------------------------------------
// Shared (part_number, generation) entry — used both by G:U groups (below)
// and by the keep-lists carried on G:M{MIXED}/P:M (see plans/fixing_agent_mess.md
// Sections 4/6/10). Widened to 32-bit generation -- see keys.hpp S:U key
// comment.
// ---------------------------------------------------------------------------
#pragma pack(push, 1)
struct GuEntry {
  uint16_t part_number{};
  uint32_t generation{};
};
#pragma pack(pop)
static_assert(sizeof(GuEntry) == 6);

// ---------------------------------------------------------------------------
// G:U value — per-part-generation cleanup, always a group of 1-20 entries.
// Unified: a single re-upload obsoletion/eviction writes a group of 1 (see
// plans/fixing_agent_mess.md Section 3's N-bound eviction rule, and Section
// 7's Phase 3 self-migration-to-GC); a successful CompleteMultipartUpload's
// excluded-parts cleanup writes groups of up to kMaxBatchSize. All entries
// in one group share a single ref_tag (carried once, in the key —
// make_gu_key) — unlike GroupGcValue, no per-entry ChunkDescriptor is
// needed; chunk_type is implicitly always CHUNK_STORAGE (multipart parts
// always use Tier 3 — see "Storage tier" in plans/multipart.md). GcWorker's
// G:U branch builds each entry's data-tier filename as
// "ref_tag-part_number-generation" from the key's ref_tag plus the entry's
// (part_number, generation) — never from a stored ChunkDescriptor.
// ---------------------------------------------------------------------------
struct GuValueBuf {
  // count(1B) + sum_size(8B) + up to kMaxBatchSize entries(6B each).
  static constexpr size_t kMaxSize =
      1 + sizeof(uint64_t) + kMaxBatchSize * sizeof(GuEntry);
  uint8_t data[kMaxSize];
  size_t len{};

  std::string_view view() const {
    return {reinterpret_cast<const char*>(data), len};
  }
};

struct GuValue {
  std::array<GuEntry, kMaxBatchSize> entries{};
  uint8_t count{};
  // Aggregate data-tier bytes across all entries in this group — used by
  // GcWorker for rate-limiting accounting (mirrors G:O's non-shared
  // CHUNK_STORAGE path) without needing a per-entry size field.
  uint64_t sum_size{};
};

void make_gu_value(GuValueBuf& buf, const GuEntry* entries, uint8_t count,
                   uint64_t sum_size);
std::optional<GuValue> parse_gu_value(std::string_view data);

}  // namespace kvrgw
