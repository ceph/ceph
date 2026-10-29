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
// Completed multipart object's :O: value -- CHUNK_MULTIPART needs room for
// its manifest tail (multipart_manifest.hpp's worst-case body) on top of
// the usual header/content_type/metadata_frame, far beyond kMaxOValueBytes
// (1024, sized for every other object). A separate, larger OValueBufT<N>
// instantiation keeps every other object write's stack footprint
// unchanged -- see object_value.hpp's OValueBufT.
// ---------------------------------------------------------------------------
// + kRefTagSize covers CHUNK_MULTIPART_REF's extra ref_tag tail segment
// (write_object_value's CHUNK_STORAGE_REF-style REF-fields block, reused
// as-is for CHUNK_MULTIPART_REF) -- plain CHUNK_MULTIPART never uses it.
inline constexpr size_t kMaxMultipartOValueBytes =
    sizeof(ObjectValueHeader) + kMaxContentTypeLen + kRefTagSize
    + MAX_META_FRAME_BYTES + kManifestBodyWorstCase;
using OValueBufMultipart = OValueBufT<kMaxMultipartOValueBytes>;

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
  uint8_t  pad[3]{};
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
                       std::span<const MetaPair> metadata);
std::optional<MHeadValue> parse_m_head_value(std::string_view data);

// Zero-copy sibling of parse_m_head_value() — same bounds-checking, but
// returns views into `data` instead of allocating owned content_type/
// metadata_frame containers. Use when the caller only needs to slice and
// assign once into its own destination (e.g. Complete building the final
// :O:) rather than hold an independently-owned MHeadValue.
struct MHeadValueView {
  uint8_t tags_count{};
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
// zero heap allocation — same fixed-size stack-buffer idiom as KeyBuf/
// GmValueBuf elsewhere) and overwrites just the state byte, leaving
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
};
#pragma pack(pop)
static_assert(sizeof(MPartValue) == 1 + 3 + sizeof(ChunkDescriptor)
              + sizeof(ETag) + sizeof(uint64_t) + sizeof(uint32_t));

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
// G:M value — whole-upload cleanup directive mode, written by Complete/Abort
// in the same txn that deletes the S:M head (clean case), or copied onto a
// P:M entry's keep-list and written later by the Sweeper (messy case).
// See plans/fixing_agent_mess.md Section 10:
//   DEEP  — Abort's clean case. No manifest -- GcWorker frees every S:U
//           row's data individually, then a single clearRange. No
//           keep-list body at all.
//   MIXED — Complete's clean case. Carries the manifest's keep-list, in
//           the compact three-knob encoding (plans/multipart_manifest.md)
//           -- which part-numbers are included, and which generation each
//           one keeps, since a part can have more than one COMMITTED
//           generation and only one is chosen. GcWorker RangeScans S:U
//           (safe -- head already deleted, no new commits possible),
//           frees any row NOT in the keep-list individually, then a
//           single clearRange over the whole range.
//   FREE_PARTS — the completed CHUNK_MULTIPART :O: itself was later
//           deleted/overwritten (move_object_to_g). By then every S:U row
//           for this ref_tag is long gone (MIXED's own GcWorker pass
//           already cleared them) -- the manifest carried on the old :O:
//           value is the *only* remaining record of which part files are
//           still on disk. move_object_to_g copies that manifest's bytes
//           verbatim (no decode/re-encode -- same trick as P:M -> G:M)
//           into a fresh G:M{FREE_PARTS} entry; GcWorker decodes it and
//           frees every (part_number, generation) file directly -- no
//           S:U scan at all, nothing to clearRange.
// Wire: mode(1B) + body (present for MIXED and FREE_PARTS -- see
//       multipart_manifest.hpp's encode_manifest_body).
// ---------------------------------------------------------------------------
enum GmMode : uint8_t {
  GM_MODE_DEEP        = 0,
  GM_MODE_MIXED       = 1,
  GM_MODE_FREE_PARTS  = 2,
};

struct GmValueBuf {
  // mode(1) + worst-case manifest body.
  static constexpr size_t kMaxSize = 1 + kManifestBodyWorstCase;
  uint8_t data[kMaxSize];
  size_t len{};

  std::string_view view() const {
    return {reinterpret_cast<const char*>(data), len};
  }
};

struct GmValue {
  uint8_t mode{GM_MODE_DEEP};
  std::string_view body;  // keep-list body, view into the raw KV value; empty for DEEP
};

// `in` is ignored (may be an empty/default ManifestInput) when mode ==
// GM_MODE_DEEP.
void make_gm_value(GmValueBuf& buf, GmMode mode, const ManifestInput& in);
std::optional<GmValue> parse_gm_value(std::string_view data);

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

// ---------------------------------------------------------------------------
// P:M value — Sweeper-driven cleanup entry, written by Complete/Abort only
// when the TXN2 scan found any PENDING row (the "messy" case — see
// plans/fixing_agent_mess.md Section 4). Mirrors P:O's existing
// GcDescriptor.mtime age-check pattern (sweep_range()'s `now - mtime <
// min_age_sec_` skip).
//   PM_STATE_COMMIT — Complete's messy case. Carries the manifest's
//     keep-list, in the same compact three-knob encoding as G:M{MIXED}'s
//     body (plans/multipart_manifest.md) -- same shape/meaning, so the
//     Sweeper copies the body bytes verbatim onto the G:M{MIXED} it
//     eventually writes once every PENDING row in the row-scan has
//     resolved (Section 4/10), no decode/re-encode needed.
//   PM_STATE_ABORT — Abort's messy case. No keep-list body — the Sweeper
//     eventually writes G:M{DEEP}.
// Wire: state(1B) + pad(3B) + mtime(4B BE) + body (present only for
//       COMMIT -- see multipart_manifest.hpp's encode_manifest_body).
// ---------------------------------------------------------------------------
enum PmState : uint8_t {
  PM_STATE_COMMIT = 0,
  PM_STATE_ABORT  = 1,
};

struct PmValueBuf {
  // state(1) + pad(3) + mtime(4) + worst-case manifest body.
  static constexpr size_t kMaxSize = 1 + 3 + sizeof(uint32_t) + kManifestBodyWorstCase;
  uint8_t data[kMaxSize];
  size_t len{};

  std::string_view view() const {
    return {reinterpret_cast<const char*>(data), len};
  }
};

struct PmValue {
  uint8_t state{PM_STATE_ABORT};
  uint32_t mtime{};
  std::string_view body;  // keep-list body, view into the raw KV value; meaningful only for COMMIT
};

// `in` is ignored (may be an empty/default ManifestInput) when state ==
// PM_STATE_ABORT.
void make_pm_value(PmValueBuf& buf, PmState state, uint32_t mtime, const ManifestInput& in);
std::optional<PmValue> parse_pm_value(std::string_view data);

}  // namespace kvrgw
