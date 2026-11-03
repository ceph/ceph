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

#include <arpa/inet.h>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string_view>

namespace kvrgw {

// ---------------------------------------------------------------------------
// Whole-upload cleanup value, used verbatim by both the G:M key (GcWorker's
// directive) and the P:M key (Sweeper's pending entry) -- they carry the
// same information, so they share one wire layout and the Sweeper's
// P:M -> G:M hand-off writes the same bytes under the other key.
//
//   [mode 1B][pad 3B][mtime 4B BE][body]
//
//   DEEP       — Abort. No body. GcWorker frees every S:U row's data
//                individually, then a single clearRange.
//   MIXED      — Complete. Body is the manifest (multipart_manifest.hpp);
//                only its Knob 1 + Knob 2 (the keep-list) are read.
//                GcWorker RangeScans S:U, frees any row NOT in the keep-list,
//                then a single clearRange.
//   FREE_PARTS — a completed CHUNK_MULTIPART object was deleted/overwritten
//                (every S:U row is long gone); body is the full manifest and
//                GcWorker frees every (part_number, generation) file and
//                the tier 3 "<ref_tag>_checksum" file (a missing file is
//                not an error).
//
// `mtime` is meaningful on P:M only (Sweeper age gate); it is 0 in a stored
// manifest blob (see below).
//
// Every stored copy of a full multipart manifest -- the tail of a
// CHUNK_MULTIPART[_REF] :O: value, its S:C:M child value, and the R:
// descriptor of a shared multipart object -- is stored as a ready-made
// FREE_PARTS value ("manifest blob"), so handing it to GC is a plain
// kv_put of the stored bytes under the G:M key: no allocation, no copy.
// ---------------------------------------------------------------------------
enum GmMode : uint8_t {
  GM_MODE_DEEP       = 0,
  GM_MODE_MIXED      = 1,
  GM_MODE_FREE_PARTS = 2,
};

inline constexpr size_t kGmHeaderSize = 8;

// Writes the 8-byte header into dst[0..8).
inline void write_gm_header(uint8_t* dst, GmMode mode, uint32_t mtime)
{
  dst[0] = static_cast<uint8_t>(mode);
  dst[1] = dst[2] = dst[3] = 0;
  const uint32_t mtime_be = htonl(mtime);
  std::memcpy(dst + 4, &mtime_be, sizeof(mtime_be));
}

struct GmValue {
  uint8_t mode{GM_MODE_DEEP};
  uint32_t mtime{};
  std::string_view body;  // view into the raw value; empty for DEEP
};

inline std::optional<GmValue> parse_gm_value(std::string_view data)
{
  if (data.size() < kGmHeaderSize) {
    return std::nullopt;
  }
  GmValue v;
  v.mode = static_cast<uint8_t>(data[0]);
  uint32_t mtime_be{};
  std::memcpy(&mtime_be, data.data() + 4, sizeof(mtime_be));
  v.mtime = ntohl(mtime_be);
  switch (v.mode) {
    case GM_MODE_DEEP:
      break;
    case GM_MODE_MIXED:
    case GM_MODE_FREE_PARTS:
      v.body = data.substr(kGmHeaderSize);
      if (v.body.empty()) {
        return std::nullopt;
      }
      break;
    default:
      return std::nullopt;
  }
  return v;
}

// The manifest body inside a stored manifest blob; empty if `blob` is not a
// valid FREE_PARTS value.
inline std::string_view gm_blob_body(std::string_view blob)
{
  const auto gm = parse_gm_value(blob);
  return (gm && gm->mode == GM_MODE_FREE_PARTS) ? gm->body : std::string_view{};
}

}  // namespace kvrgw
