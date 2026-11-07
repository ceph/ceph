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

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

namespace kvrgw {

// ---------------------------------------------------------------------------
// Compact three-knob encoding for a multipart upload's keep-list -- see
// plans/multipart_manifest.md. Replaces the flat GuEntry array that used to
// live inside the G:M / P:M values.
//
// Knob 1 -- which part-numbers are included in the final object.
// Knob 2 -- which included parts carry a generation other than 0.
// Knob 3 -- which included parts carry a size other than the discovered
//           default (one Boyer-Moore majority-vote pass).
//
// Knobs 1-3 share a subset-descriptor shape. Each knob uses the cheapest mode:
//   0  continuous            first, last                   (Knob 1: [first,last] range;
//                                                           Knob 2/3: nobody deviates)
//   1  continuous + exclude  first, last, count, excluded[count]
//   2  interval-set          count, { first, last }[count]
//   3  inclusion-list        count, included[count]
//   4  flat array            Knob 1: bitmap, ceil(last/8) bytes, LSB-first, bit i=part(i+1);
//                            Knob 2: bit-packed slots, (kMaxPartNumber*W)/8 bytes;
//                            Knob 3: byte-packed slots, kMaxPartNumber*W bytes.
//                            Mode 4 is the hard ceiling -- chosen when no mode 0-3 is cheaper.
//
// For Knob 2/3, modes 1/2 store *positions* within included_parts (not raw
// part-numbers) so decode's count always matches what was written even when
// Knob 1 has excluded holes in between. Mode 3 and mode 4 use raw values.
//
// Generation is capped at uint16 (max 65,535 retries per part). No kU32 tier.
// Knob 2 width tiers:
//   0 = 2-bit  (max gen <= 3)     packed 4 slots/byte, LSB-first
//   1 = nibble (max gen <= 15)    packed 2 slots/byte, low nibble first
//   2 = byte   (max gen <= 255)
//   3 = uint16 (max gen <= 65535)
// Mode 4 flat array sizes: (kMaxPartNumber * W) / 8 bytes.
// Zero slot = not included (generation 0 is indistinguishable from default,
// which is also 0, so zero means "use default / not present").
//
// Knob 3 width tiers (byte-granular; min part size is 5 MB):
//   0 = 24-bit (max size < 16 MB)
//   1 = 32-bit (max size < 4 GB)
//   2 = 40-bit (max size <= 5 GB, S3 per-part ceiling)
// Mode 4 flat array: kMaxPartNumber * W bytes, slot = (part_num-1)*W,
// zero = not included.
//
// Wire layout (4-byte header + 7-step body):
//   byte 0   high nibble: Knob 2 width tier (0=2bit,1=nibble,2=byte,3=uint16)
//            low nibble:  Knob 3 width tier (0=24bit,1=32bit,2=40bit)
//   byte 1   Knob 1 mode (0-4)
//   byte 2   Knob 2 mode (0-4)
//   byte 3   Knob 3 mode (0-4)
//   1. Knob 1 region          which part-ids are included
//   2. default generation     one value at Knob 2's width (absent when mode 4)
//   3. Knob 2 region          which included part-ids deviate (absent when mode 4)
//   4. deviating generations  one per part-id named in (3) (absent when mode 4)
//      OR Knob 2 flat array   (kMaxPartNumber*W)/8 bytes  (mode 4 only)
//   5. default size           one value at Knob 3's width (absent when mode 4)
//   6. Knob 3 region          which included part-ids deviate (absent when mode 4)
//   7. deviating sizes        one per part-id named in (6) (absent when mode 4)
//      OR Knob 3 flat array   kMaxPartNumber*W bytes      (mode 4 only)
// ---------------------------------------------------------------------------

enum class SubsetMode : uint8_t {
  kContinuous       = 0,
  kContinuousExclude = 1,
  kIntervalSet      = 2,
  kInclusionList    = 3,
  kFlatArray        = 4,
};

// Generation is capped at uint16; kU32 is removed.
enum class GenWidth : uint8_t { k2Bit = 0, kNibble = 1, kByte = 2, kU16 = 3 };
enum class SizeWidth : uint8_t { k24 = 0, k32 = 1, k40 = 2 };

struct ManifestInput {
  std::span<const uint16_t> included_parts;  // sorted ascending, >=1 entry
  std::span<const uint16_t> generations;     // generations[i] <-> included_parts[i]; max 65535
  std::span<const uint64_t> sizes;           // sizes[i] <-> included_parts[i]
};

// Upper bound on encode_manifest_body's output for `n` included parts.
// Knob 1: min(mode3 cost, mode4 ceiling) = min(2+2n, ceil(kMaxPartNumber/8)) = min(2+2n, 1250).
// Knob 2: min(descriptor+values, mode4 flat) = min(6+2n+2n, (kMaxPartNumber*2)/8) = min(6+4n,2500).
//   (Worst tier uint16: mode4=20000; descriptor+values=6+2n+2n with n<=10000.)
//   Conservative: 6 + 2*n + 2*n = 6 + 4*n  for descriptor+values at uint16 tier,
//   capped at (kMaxPartNumber*16)/8 = 20000.
// Knob 3: min(descriptor+values, mode4 flat) at 40-bit tier = min(6+2n+5n, kMaxPartNumber*5)
//   = min(6+7n, 50000).
// Add header(4) + default_gen(2) + default_size(5).
inline constexpr size_t manifest_body_bound(size_t n)
{
  // Knob 1 mode 4: 2-byte count prefix + ceil(kMaxPartNumber/8) bitmap = 1252 bytes
  constexpr size_t k1_flat = 2 + (static_cast<size_t>(kMaxPartNumber) + 7) / 8;  // 1252
  const size_t k1_desc = 2 + 2 * n;
  const size_t k1 = k1_desc < k1_flat ? k1_desc : k1_flat;

  constexpr size_t k2_flat = (static_cast<size_t>(kMaxPartNumber) * 16) / 8;  // 20000
  const size_t k2_desc = 6 + 4 * n;  // descriptor(6+2n) + values(2n) at uint16
  const size_t k2 = k2_desc < k2_flat ? k2_desc : k2_flat;

  constexpr size_t k3_flat = static_cast<size_t>(kMaxPartNumber) * 5;  // 50000
  const size_t k3_desc = 6 + 7 * n;  // descriptor(6+2n) + values(5n) at 40-bit
  const size_t k3 = k3_desc < k3_flat ? k3_desc : k3_flat;

  return 4          // header
       + k1         // Knob 1 region
       + 2          // default generation (uint16 worst case)
       + k2         // Knob 2 region + values (or flat array)
       + 5          // default size (40-bit worst case)
       + k3;        // Knob 3 region + values (or flat array)
}

// Compile-time guarantee: the largest possible G:M / P:M value (Knob 1+2
// only, no Knob 3) plus the G:M header stays within FDB's 100,000 B limit.
// Full manifest (all three knobs) is also within the limit.
static_assert(manifest_body_bound(kMaxPartNumber) < 100000,
              "manifest_body_bound(kMaxPartNumber) must stay under FDB's 100,000 B value limit");

// Writes the 4-byte header + 7-step body into dst (cap bytes). `in`'s three
// spans must be the same non-zero size, included_parts strictly ascending,
// all generations <= 65535.
// Returns bytes written, or 0 on malformed input / overflow (overflow is
// defensive-only -- bounded by manifest_body_bound(n)).
// If `keep_len` is given it receives the length of the prefix that ends
// after step 4 (header, Knob 1, Knob 2): everything ManifestKeepCursor reads.
// That prefix is a valid keep-list body on its own (the G:M{MIXED} / P:M body).
size_t encode_manifest_body(uint8_t* dst, size_t cap, const ManifestInput& in,
                            size_t* keep_len = nullptr);

// Full materialization -- round-trip tests and later Phase 8's manifest
// reader. Not used by GcWorker/Sweeper; see ManifestKeepCursor below.
struct DecodedManifestBody {
  std::vector<uint16_t> included_parts;
  std::vector<uint16_t> generations;
  std::vector<uint64_t> sizes;
};
std::optional<DecodedManifestBody> decode_manifest_body(std::string_view body);

// Incremental decode for GcWorker/Sweeper's S:U scan, which already walks
// every row in ascending part_number order. Answers "is (part_number,
// generation) the kept generation for an included part?" one query at a
// time, without ever materializing a full list. Only Knob 1 + Knob 2 are
// parsed up front; Knob 3's bytes are present in `body` but this cursor
// never decodes them (GC/Sweeper don't need size at all).
class ManifestKeepCursor {
 public:
  explicit ManifestKeepCursor(std::string_view body);

  bool valid() const { return valid_; }

  // part_number must be non-decreasing across calls (matches the S:U
  // range-scan's key order). Safe to call more than once for the same
  // part_number (one row per generation, up to kMaxGenerationsPerPart) --
  // only advances internal position when part_number increases from the
  // previous call.
  bool is_kept(uint16_t part_number, uint16_t generation);

 private:
  // Forward-only enumerator over one subset-descriptor region (modes 0-3).
  // Mode 4 (flat array) is handled directly in the cursor, not here.
  struct SubsetScanner {
    SubsetMode mode{SubsetMode::kContinuous};
    bool mode0_is_range{false};  // true: mode0 means [first,last]; false: mode0 means empty
    uint16_t first{0}, last{0};  // modes 0 (if mode0_is_range) and 1
    uint16_t excl_count{0};      // mode 1
    uint16_t run_count{0};       // mode 2
    uint16_t list_count{0};      // mode 3
    std::string_view array_bytes;  // mode 1 excluded[], mode 2 intervals, mode 3 list

    uint32_t cur{0};        // current candidate value (modes 0,1,2)
    uint16_t fwd_idx{0};    // array_bytes index (modes 1 exclusion scan, 2 run, 3 list)

    // Parses one region starting at body[*offset], advancing *offset
    // past it. Returns false on malformed/truncated input.
    bool parse(SubsetMode m, bool mode0_range, std::string_view body, size_t* offset);

    // Next value in S (ascending), or nullopt once S is exhausted.
    std::optional<uint16_t> peek();
    void advance();
  };

  bool valid_{false};

  // Knob 1 -- either SubsetScanner (modes 0-3) or flat bitmap (mode 4).
  SubsetMode k1_mode_{SubsetMode::kContinuous};
  SubsetScanner knob1_;             // used when k1_mode_ != kFlatArray
  std::string_view k1_bitmap_;     // used when k1_mode_ == kFlatArray
  uint16_t knob1_position_{0};     // 0-based position of the current included part

  uint16_t default_generation_{0};
  GenWidth gen_width_{GenWidth::k2Bit};

  // Knob 2 -- either SubsetScanner (modes 0-3) or flat array (mode 4).
  SubsetMode k2_mode_{SubsetMode::kContinuous};
  SubsetScanner knob2_;             // used when k2_mode_ != kFlatArray
  std::string_view knob2_values_;   // deviating generation values (modes 0-3)
  uint16_t knob2_value_idx_{0};     // how many deviating values consumed so far
  std::string_view k2_flat_;       // used when k2_mode_ == kFlatArray

  // Last query's result, memoized so repeat calls for the same
  // part_number (multiple generation rows) don't re-walk anything.
  uint16_t last_part_number_{0};
  bool last_part_has_result_{false};
  bool last_part_included_{false};
  uint16_t last_part_kept_generation_{0};

  bool knob1_contains(uint16_t part_number);
  bool knob2_deviates(uint16_t part_number, uint16_t position, uint16_t* out_value);
};

}  // namespace kvrgw
