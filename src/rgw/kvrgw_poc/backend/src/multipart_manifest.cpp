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

#include "multipart_manifest.hpp"

#include "id_tag.hpp"  // read_be_field / write_be_field

#include <algorithm>
#include <cstring>

namespace kvrgw {

namespace {

// ---------------------------------------------------------------------------
// Mode-cost helpers -- multipart_manifest.md "Choosing a mode".
// ---------------------------------------------------------------------------
inline size_t mode1_cost(size_t gap_count) { return 6 + 2 * gap_count; }
inline size_t mode2_cost(size_t run_count) { return 2 + 4 * run_count; }
inline size_t mode3_cost(size_t count)     { return 2 + 2 * count; }

// ---------------------------------------------------------------------------
// Non-standard-width byte helpers -- big-endian.
// ---------------------------------------------------------------------------
void write_u24(uint8_t* dst, uint64_t v) {
  dst[0] = static_cast<uint8_t>((v >> 16) & 0xFF);
  dst[1] = static_cast<uint8_t>((v >> 8) & 0xFF);
  dst[2] = static_cast<uint8_t>(v & 0xFF);
}
uint64_t read_u24(const uint8_t* src) {
  return (static_cast<uint64_t>(src[0]) << 16) |
         (static_cast<uint64_t>(src[1]) << 8) |
         static_cast<uint64_t>(src[2]);
}
void write_u40(uint8_t* dst, uint64_t v) {
  dst[0] = static_cast<uint8_t>((v >> 32) & 0xFF);
  dst[1] = static_cast<uint8_t>((v >> 24) & 0xFF);
  dst[2] = static_cast<uint8_t>((v >> 16) & 0xFF);
  dst[3] = static_cast<uint8_t>((v >> 8) & 0xFF);
  dst[4] = static_cast<uint8_t>(v & 0xFF);
}
uint64_t read_u40(const uint8_t* src) {
  return (static_cast<uint64_t>(src[0]) << 32) |
         (static_cast<uint64_t>(src[1]) << 24) |
         (static_cast<uint64_t>(src[2]) << 16) |
         (static_cast<uint64_t>(src[3]) << 8) |
         static_cast<uint64_t>(src[4]);
}

// ---------------------------------------------------------------------------
// Width-tier plumbing.
// ---------------------------------------------------------------------------

// Returns the number of bytes a *single* generation value occupies in
// the default/deviating scalar storage (not the flat array packing).
// 2-bit and nibble tiers: scalar default costs 1 byte (no partner to pack with).
size_t gen_scalar_bytes(GenWidth w) {
  switch (w) {
    case GenWidth::k2Bit:  return 1;
    case GenWidth::kNibble: return 1;
    case GenWidth::kByte:   return 1;
    case GenWidth::kU16:    return 2;
  }
  return 2;
}
size_t size_width_bytes(SizeWidth w) {
  switch (w) {
    case SizeWidth::k24: return 3;
    case SizeWidth::k32: return 4;
    case SizeWidth::k40: return 5;
  }
  return 5;
}
GenWidth gen_width_for_max(uint16_t max_gen) {
  if (max_gen <= 3u)      return GenWidth::k2Bit;
  if (max_gen <= 0xFu)    return GenWidth::kNibble;
  if (max_gen <= 0xFFu)   return GenWidth::kByte;
  return GenWidth::kU16;
}
SizeWidth size_width_for_max(uint64_t max_size) {
  if (max_size <= 0xFFFFFFULL)   return SizeWidth::k24;
  if (max_size <= 0xFFFFFFFFULL) return SizeWidth::k32;
  return SizeWidth::k40;
}

// Write/read a single generation scalar (default or one deviating value).
// For 2-bit and nibble, a lone scalar always occupies one full byte.
void write_gen_scalar(uint8_t* dst, uint16_t v, GenWidth w) {
  switch (w) {
    case GenWidth::k2Bit:
    case GenWidth::kNibble:
    case GenWidth::kByte:
      dst[0] = static_cast<uint8_t>(v & 0xFFu);
      return;
    case GenWidth::kU16:
      write_be_field<uint16_t>(dst, v);
      return;
  }
}
uint16_t read_gen_scalar(const uint8_t* src, GenWidth w) {
  switch (w) {
    case GenWidth::k2Bit:
    case GenWidth::kNibble:
    case GenWidth::kByte:
      return src[0];
    case GenWidth::kU16:
      return read_be_field<uint16_t>(src);
  }
  return 0;
}
void write_size_value(uint8_t* dst, uint64_t v, SizeWidth w) {
  switch (w) {
    case SizeWidth::k24: write_u24(dst, v); return;
    case SizeWidth::k32: write_be_field<uint32_t>(dst, static_cast<uint32_t>(v)); return;
    case SizeWidth::k40: write_u40(dst, v); return;
  }
}
uint64_t read_size_value(const uint8_t* src, SizeWidth w) {
  switch (w) {
    case SizeWidth::k24: return read_u24(src);
    case SizeWidth::k32: return read_be_field<uint32_t>(src);
    case SizeWidth::k40: return read_u40(src);
  }
  return 0;
}

// ---------------------------------------------------------------------------
// Bit-packed array helpers for Knob 2 deviating values (modes 0-3).
// 2-bit: 4 values/byte, LSB-first (bits 0-1 = slot 0, bits 2-3 = slot 1, ...).
// nibble: 2 values/byte, low nibble first.
// byte / uint16: plain array.
// ---------------------------------------------------------------------------
size_t gen_array_bytes(size_t count, GenWidth w) {
  switch (w) {
    case GenWidth::k2Bit:   return (count * 2 + 7) / 8;
    case GenWidth::kNibble: return (count + 1) / 2;
    case GenWidth::kByte:   return count;
    case GenWidth::kU16:    return count * 2;
  }
  return count * 2;
}
void write_gen_array(uint8_t* dst, const uint16_t* values, size_t count, GenWidth w) {
  switch (w) {
    case GenWidth::k2Bit:
      std::memset(dst, 0, (count * 2 + 7) / 8);
      for (size_t i = 0; i < count; ++i) {
        const size_t byte_idx = (i * 2) / 8;
        const size_t bit_off  = (i * 2) % 8;
        dst[byte_idx] |= static_cast<uint8_t>((values[i] & 0x3u) << bit_off);
      }
      return;
    case GenWidth::kNibble:
      for (size_t i = 0; i < count; i += 2) {
        const uint8_t lo = static_cast<uint8_t>(values[i] & 0xFu);
        const uint8_t hi = (i + 1 < count) ? static_cast<uint8_t>(values[i + 1] & 0xFu) : 0;
        dst[i / 2] = static_cast<uint8_t>(lo | (hi << 4));
      }
      return;
    case GenWidth::kByte:
      for (size_t i = 0; i < count; ++i) dst[i] = static_cast<uint8_t>(values[i] & 0xFFu);
      return;
    case GenWidth::kU16:
      for (size_t i = 0; i < count; ++i) write_be_field<uint16_t>(dst + i * 2, values[i]);
      return;
  }
}
uint16_t read_gen_array_at(const uint8_t* src, size_t idx, GenWidth w) {
  switch (w) {
    case GenWidth::k2Bit: {
      const size_t byte_idx = (idx * 2) / 8;
      const size_t bit_off  = (idx * 2) % 8;
      return static_cast<uint16_t>((src[byte_idx] >> bit_off) & 0x3u);
    }
    case GenWidth::kNibble: {
      const uint8_t b = src[idx / 2];
      return (idx % 2 == 0) ? (b & 0xFu) : ((b >> 4) & 0xFu);
    }
    case GenWidth::kByte:
      return src[idx];
    case GenWidth::kU16:
      return read_be_field<uint16_t>(src + idx * 2);
  }
  return 0;
}

// ---------------------------------------------------------------------------
// Knob 2 flat array (mode 4): (kMaxPartNumber * W) / 8 bytes,
// slot = position of part in 0-based bit/nibble/byte/uint16 array,
// zero = not included (or gen 0 -- same value, zero means "default/absent").
// ---------------------------------------------------------------------------
size_t k2_flat_bytes(GenWidth w) {
  switch (w) {
    case GenWidth::k2Bit:   return (static_cast<size_t>(kMaxPartNumber) * 2) / 8;  // 2500
    case GenWidth::kNibble: return (static_cast<size_t>(kMaxPartNumber) * 4) / 8;  // 5000
    case GenWidth::kByte:   return  static_cast<size_t>(kMaxPartNumber);            // 10000
    case GenWidth::kU16:    return  static_cast<size_t>(kMaxPartNumber) * 2;        // 20000
  }
  return static_cast<size_t>(kMaxPartNumber) * 2;
}

// ---------------------------------------------------------------------------
// Knob 1 flat array (mode 4): bitmap, ceil(last_part_id / 8) bytes,
// LSB-first within each byte, bit i set iff part (i+1) is included.
// ---------------------------------------------------------------------------
size_t k1_bitmap_bytes(uint16_t last_part_id) {
  return (static_cast<size_t>(last_part_id) + 7) / 8;
}
static constexpr size_t kK1BitmapMax = (static_cast<size_t>(kMaxPartNumber) + 7) / 8;  // 1250

// ---------------------------------------------------------------------------
// Knob 3 flat array (mode 4): kMaxPartNumber * W bytes,
// slot = (part_num - 1) * W, value = size at width W (little-endian convention
// follows the same byte order as the rest of the file: big-endian).
// Zero = not included.
// ---------------------------------------------------------------------------
size_t k3_flat_bytes(SizeWidth w) {
  return static_cast<size_t>(kMaxPartNumber) * size_width_bytes(w);
}

// ---------------------------------------------------------------------------
// Encoding -- Knob 1 region (membership).
// ---------------------------------------------------------------------------
struct RegionResult { SubsetMode mode; size_t len; bool ok; };

RegionResult encode_knob1_region(uint8_t* dst, size_t cap, std::span<const uint16_t> included) {
  const size_t n = included.size();
  const uint16_t first = included.front();
  const uint16_t last = included.back();

  // mode 4 ceiling
  const size_t flat_cost = k1_bitmap_bytes(last);

  if (static_cast<size_t>(last - first + 1) == n) {
    // mode 0: continuous -- 4 bytes
    if (4 <= flat_cost) {
      if (cap < 4) return {SubsetMode::kContinuous, 0, false};
      write_be_field<uint16_t>(dst, first);
      write_be_field<uint16_t>(dst + 2, last);
      return {SubsetMode::kContinuous, 4, true};
    }
    // flat is cheaper (degenerate tiny case)
    goto write_flat;
  }

  {
    const size_t gap_count = static_cast<size_t>(last - first + 1) - n;
    size_t run_count = 1;
    for (size_t i = 1; i < n; ++i) {
      if (included[i] != static_cast<uint16_t>(included[i - 1] + 1)) ++run_count;
    }
    const size_t c1 = mode1_cost(gap_count);
    const size_t c2 = mode2_cost(run_count);
    const size_t c3 = mode3_cost(n);
    const size_t best_desc = std::min({c1, c2, c3});

    if (best_desc < flat_cost) {
      // pick cheapest descriptor mode
      const SubsetMode mode = (c1 <= c2 && c1 <= c3) ? SubsetMode::kContinuousExclude
                             : (c2 <= c3)             ? SubsetMode::kIntervalSet
                                                       : SubsetMode::kInclusionList;
      if (mode == SubsetMode::kContinuousExclude) {
        if (cap < c1) return {mode, 0, false};
        write_be_field<uint16_t>(dst, first);
        write_be_field<uint16_t>(dst + 2, last);
        write_be_field<uint16_t>(dst + 4, static_cast<uint16_t>(gap_count));
        size_t off = 6;
        size_t idx = 0;
        for (uint32_t v = first; v <= last; ++v) {
          if (idx < n && included[idx] == static_cast<uint16_t>(v)) { ++idx; continue; }
          write_be_field<uint16_t>(dst + off, static_cast<uint16_t>(v));
          off += 2;
        }
        return {mode, off, true};
      }
      if (mode == SubsetMode::kIntervalSet) {
        if (cap < c2) return {mode, 0, false};
        write_be_field<uint16_t>(dst, static_cast<uint16_t>(run_count));
        size_t off = 2, i = 0;
        while (i < n) {
          size_t j = i;
          while (j + 1 < n && included[j + 1] == static_cast<uint16_t>(included[j] + 1)) ++j;
          write_be_field<uint16_t>(dst + off, included[i]);
          write_be_field<uint16_t>(dst + off + 2, included[j]);
          off += 4;
          i = j + 1;
        }
        return {mode, off, true};
      }
      // kInclusionList
      if (cap < c3) return {mode, 0, false};
      write_be_field<uint16_t>(dst, static_cast<uint16_t>(n));
      size_t off = 2;
      for (size_t i = 0; i < n; ++i) {
        write_be_field<uint16_t>(dst + off, included[i]);
        off += 2;
      }
      return {mode, off, true};
    }
  }

write_flat:
  {
    // mode 4: 2-byte count prefix + bitmap bytes
    const size_t total = 2 + flat_cost;
    if (cap < total) return {SubsetMode::kFlatArray, 0, false};
    write_be_field<uint16_t>(dst, static_cast<uint16_t>(flat_cost));
    std::memset(dst + 2, 0, flat_cost);
    for (size_t i = 0; i < n; ++i) {
      const uint16_t pn = included[i];  // 1-based
      const size_t bit = static_cast<size_t>(pn) - 1;
      dst[2 + bit / 8] |= static_cast<uint8_t>(1u << (bit % 8));
    }
    return {SubsetMode::kFlatArray, total, true};
  }
}

// ---------------------------------------------------------------------------
// Encoding -- Knob 2/3's deviation region (shared, modes 0-3).
// dev_values: raw part-numbers of deviating parts (ascending).
// dev_positions: 0-based index within included_parts (ascending, parallel).
// ---------------------------------------------------------------------------
RegionResult encode_deviation_region(uint8_t* dst, size_t cap,
                                     std::span<const uint16_t> dev_values,
                                     std::span<const uint16_t> dev_positions) {
  const size_t n = dev_values.size();
  if (n == 0) {
    return {SubsetMode::kContinuous, 0, true};  // mode 0 -- zero deviation
  }

  const uint16_t pos_first = dev_positions.front();
  const uint16_t pos_last = dev_positions.back();
  const size_t gap_count = static_cast<size_t>(pos_last - pos_first + 1) - n;
  size_t run_count = 1;
  for (size_t i = 1; i < n; ++i) {
    if (dev_positions[i] != static_cast<uint16_t>(dev_positions[i - 1] + 1)) ++run_count;
  }
  const size_t c1 = mode1_cost(gap_count);
  const size_t c2 = mode2_cost(run_count);
  const size_t c3 = mode3_cost(n);
  const SubsetMode mode = (c1 <= c2 && c1 <= c3) ? SubsetMode::kContinuousExclude
                         : (c2 <= c3)             ? SubsetMode::kIntervalSet
                                                   : SubsetMode::kInclusionList;

  if (mode == SubsetMode::kContinuousExclude) {
    if (cap < c1) return {mode, 0, false};
    write_be_field<uint16_t>(dst, pos_first);
    write_be_field<uint16_t>(dst + 2, pos_last);
    write_be_field<uint16_t>(dst + 4, static_cast<uint16_t>(gap_count));
    size_t off = 6;
    size_t dev_idx = 0;
    for (uint32_t p = pos_first; p <= pos_last; ++p) {
      if (dev_idx < n && dev_positions[dev_idx] == static_cast<uint16_t>(p)) { ++dev_idx; continue; }
      write_be_field<uint16_t>(dst + off, static_cast<uint16_t>(p));
      off += 2;
    }
    return {mode, off, true};
  }
  if (mode == SubsetMode::kIntervalSet) {
    if (cap < c2) return {mode, 0, false};
    write_be_field<uint16_t>(dst, static_cast<uint16_t>(run_count));
    size_t off = 2, i = 0;
    while (i < n) {
      size_t j = i;
      while (j + 1 < n && dev_positions[j + 1] == static_cast<uint16_t>(dev_positions[j] + 1)) ++j;
      write_be_field<uint16_t>(dst + off, dev_positions[i]);
      write_be_field<uint16_t>(dst + off + 2, dev_positions[j]);
      off += 4;
      i = j + 1;
    }
    return {mode, off, true};
  }
  // kInclusionList -- raw values.
  if (cap < c3) return {mode, 0, false};
  write_be_field<uint16_t>(dst, static_cast<uint16_t>(n));
  size_t off = 2;
  for (size_t i = 0; i < n; ++i) {
    write_be_field<uint16_t>(dst + off, dev_values[i]);
    off += 2;
  }
  return {mode, off, true};
}

// ---------------------------------------------------------------------------
// Decoding -- Knob 1 region.
// ---------------------------------------------------------------------------
bool decode_knob1_region(std::string_view body, size_t* offset, SubsetMode mode,
                         std::vector<uint16_t>* out) {
  out->clear();
  const auto* p = reinterpret_cast<const uint8_t*>(body.data());
  size_t off = *offset;
  auto fits = [&](size_t extra) { return off + extra <= body.size(); };

  switch (mode) {
    case SubsetMode::kContinuous: {
      if (!fits(4)) return false;
      const uint16_t first = read_be_field<uint16_t>(p + off);
      const uint16_t last = read_be_field<uint16_t>(p + off + 2);
      off += 4;
      if (last < first) return false;
      out->reserve(static_cast<size_t>(last) - first + 1);
      for (uint32_t v = first; v <= last; ++v) out->push_back(static_cast<uint16_t>(v));
      *offset = off;
      return true;
    }
    case SubsetMode::kContinuousExclude: {
      if (!fits(6)) return false;
      const uint16_t first = read_be_field<uint16_t>(p + off);
      const uint16_t last = read_be_field<uint16_t>(p + off + 2);
      const uint16_t count = read_be_field<uint16_t>(p + off + 4);
      off += 6;
      if (!fits(static_cast<size_t>(count) * 2) || last < first) return false;
      size_t excl_idx = 0;
      auto next_excl = [&]() -> uint32_t {
        return excl_idx < count ? read_be_field<uint16_t>(p + off + excl_idx * 2) : 0xFFFFFFFFu;
      };
      uint32_t ne = next_excl();
      for (uint32_t v = first; v <= last; ++v) {
        if (v == ne) { ++excl_idx; ne = next_excl(); continue; }
        out->push_back(static_cast<uint16_t>(v));
      }
      off += static_cast<size_t>(count) * 2;
      *offset = off;
      return true;
    }
    case SubsetMode::kIntervalSet: {
      if (!fits(2)) return false;
      const uint16_t run_count = read_be_field<uint16_t>(p + off);
      off += 2;
      if (!fits(static_cast<size_t>(run_count) * 4)) return false;
      for (uint16_t i = 0; i < run_count; ++i) {
        const uint16_t rf = read_be_field<uint16_t>(p + off);
        const uint16_t rl = read_be_field<uint16_t>(p + off + 2);
        off += 4;
        if (rl < rf) return false;
        for (uint32_t v = rf; v <= rl; ++v) out->push_back(static_cast<uint16_t>(v));
      }
      *offset = off;
      return true;
    }
    case SubsetMode::kInclusionList: {
      if (!fits(2)) return false;
      const uint16_t count = read_be_field<uint16_t>(p + off);
      off += 2;
      if (!fits(static_cast<size_t>(count) * 2)) return false;
      out->reserve(count);
      for (uint16_t i = 0; i < count; ++i) {
        out->push_back(read_be_field<uint16_t>(p + off));
        off += 2;
      }
      *offset = off;
      return true;
    }
    case SubsetMode::kFlatArray: {
      // Variable-length bitmap: read until end of body isn't right --
      // we need the encoder-recorded length. We stored ceil(last/8) bytes.
      // The decoder scans all bytes present and collects set bits.
      // The bitmap ends at offset+ceil(max_part_id/8); since we don't
      // store last separately in mode 4, we scan to the end of what was
      // written -- but we need to know its size. We encode it by storing
      // the byte count as a uint16_t prefix (2 bytes) in mode 4 only.
      if (!fits(2)) return false;
      const uint16_t bmap_bytes = read_be_field<uint16_t>(p + off);
      off += 2;
      if (!fits(bmap_bytes) || bmap_bytes > kK1BitmapMax) return false;
      for (uint16_t b = 0; b < bmap_bytes; ++b) {
        const uint8_t byte = p[off + b];
        for (int bit = 0; bit < 8; ++bit) {
          if (byte & (1u << bit)) {
            const uint32_t pn = static_cast<uint32_t>(b) * 8 + bit + 1;
            if (pn > kMaxPartNumber) return false;
            out->push_back(static_cast<uint16_t>(pn));
          }
        }
      }
      off += bmap_bytes;
      *offset = off;
      return true;
    }
  }
  return false;
}

// ---------------------------------------------------------------------------
// Decoding -- Knob 2/3's deviation region (modes 0-3), into positions.
// ---------------------------------------------------------------------------
bool decode_deviation_positions(std::string_view body, size_t* offset, SubsetMode mode,
                                size_t included_count,
                                std::span<const uint16_t> included_parts,
                                std::vector<uint16_t>* out_positions) {
  out_positions->clear();
  const auto* p = reinterpret_cast<const uint8_t*>(body.data());
  size_t off = *offset;
  auto fits = [&](size_t extra) { return off + extra <= body.size(); };

  switch (mode) {
    case SubsetMode::kContinuous:
      *offset = off;
      return true;  // zero deviation
    case SubsetMode::kContinuousExclude: {
      if (!fits(6)) return false;
      const uint16_t pf = read_be_field<uint16_t>(p + off);
      const uint16_t pl = read_be_field<uint16_t>(p + off + 2);
      const uint16_t count = read_be_field<uint16_t>(p + off + 4);
      off += 6;
      if (!fits(static_cast<size_t>(count) * 2) || pl < pf || pl >= included_count) return false;
      size_t excl_idx = 0;
      auto next_excl = [&]() -> uint32_t {
        return excl_idx < count ? read_be_field<uint16_t>(p + off + excl_idx * 2) : 0xFFFFFFFFu;
      };
      uint32_t ne = next_excl();
      for (uint32_t pos = pf; pos <= pl; ++pos) {
        if (pos == ne) { ++excl_idx; ne = next_excl(); continue; }
        out_positions->push_back(static_cast<uint16_t>(pos));
      }
      off += static_cast<size_t>(count) * 2;
      *offset = off;
      return true;
    }
    case SubsetMode::kIntervalSet: {
      if (!fits(2)) return false;
      const uint16_t run_count = read_be_field<uint16_t>(p + off);
      off += 2;
      if (!fits(static_cast<size_t>(run_count) * 4)) return false;
      for (uint16_t i = 0; i < run_count; ++i) {
        const uint16_t rf = read_be_field<uint16_t>(p + off);
        const uint16_t rl = read_be_field<uint16_t>(p + off + 2);
        off += 4;
        if (rl < rf || rl >= included_count) return false;
        for (uint32_t pos = rf; pos <= rl; ++pos) out_positions->push_back(static_cast<uint16_t>(pos));
      }
      *offset = off;
      return true;
    }
    case SubsetMode::kInclusionList: {
      if (!fits(2)) return false;
      const uint16_t count = read_be_field<uint16_t>(p + off);
      off += 2;
      if (!fits(static_cast<size_t>(count) * 2)) return false;
      out_positions->reserve(count);
      for (uint16_t i = 0; i < count; ++i) {
        const uint16_t raw = read_be_field<uint16_t>(p + off);
        off += 2;
        const auto it = std::lower_bound(included_parts.begin(), included_parts.end(), raw);
        if (it == included_parts.end() || *it != raw) return false;
        out_positions->push_back(static_cast<uint16_t>(it - included_parts.begin()));
      }
      *offset = off;
      return true;
    }
    case SubsetMode::kFlatArray:
      // Handled by caller directly; should not reach here.
      return false;
  }
  return false;
}

}  // namespace

//--------------------------------------------------------------------------------
size_t encode_manifest_body(uint8_t* dst, size_t cap, const ManifestInput& in,
                            size_t* keep_len)
{
  const size_t n = in.included_parts.size();
  if (n == 0 || n > kMaxPartNumber ||
      in.generations.size() != n || in.sizes.size() != n) [[unlikely]] {
    return 0;
  }
  if (cap < 4) return 0;

  size_t off = 4;  // header filled in last

  // ---- Step 1: Knob 1 region ----
  const RegionResult k1 = encode_knob1_region(dst + off, cap - off, in.included_parts);
  if (!k1.ok) return 0;
  off += k1.len;

  // ---- Knob 2: deviating-generation set (gen != 0), running max ----
  std::vector<uint16_t> dev2_values;
  std::vector<uint16_t> dev2_positions;
  uint16_t max_generation = 0;
  for (size_t i = 0; i < n; ++i) {
    if (in.generations[i] > max_generation) max_generation = in.generations[i];
    if (in.generations[i] != 0) {
      dev2_values.push_back(in.included_parts[i]);
      dev2_positions.push_back(static_cast<uint16_t>(i));
    }
  }
  const GenWidth gen_width = gen_width_for_max(max_generation);

  // ---- Steps 2-4: Knob 2 (descriptor or flat) ----
  SubsetMode k2_mode;
  {
    // Try descriptor encoding into a scratch region to measure cost.
    // We always have cap >= manifest_body_bound(n), so a temp measure via
    // encode_deviation_region is safe; just measure, then write the winner.
    const size_t dev_cnt = dev2_values.size();
    const size_t flat_cost = k2_flat_bytes(gen_width);
    const size_t arr_cost  = gen_array_bytes(dev_cnt, gen_width);

    // Estimate descriptor cost without actually encoding it:
    size_t desc_cost = 0;
    if (dev_cnt == 0) {
      desc_cost = 0;  // mode 0
    } else {
      const uint16_t pos_first = dev2_positions.front();
      const uint16_t pos_last  = dev2_positions.back();
      const size_t gap_count = static_cast<size_t>(pos_last - pos_first + 1) - dev_cnt;
      size_t run_count = 1;
      for (size_t i = 1; i < dev_cnt; ++i) {
        if (dev2_positions[i] != static_cast<uint16_t>(dev2_positions[i-1] + 1)) ++run_count;
      }
      desc_cost = std::min({mode1_cost(gap_count), mode2_cost(run_count), mode3_cost(dev_cnt)});
    }

    // total cost: default scalar + descriptor + values
    const size_t total_desc = gen_scalar_bytes(gen_width) + desc_cost + arr_cost;
    const size_t total_flat = flat_cost;  // no default, no descriptor, no value array

    if (total_flat < total_desc) {
      // Mode 4: write flat array
      k2_mode = SubsetMode::kFlatArray;
      if (off + total_flat > cap) return 0;
      std::memset(dst + off, 0, total_flat);
      for (size_t i = 0; i < n; ++i) {
        // slot = (part_num - 1), but we use position i directly since
        // the flat array indexes ALL kMaxPartNumber slots by (part_num-1).
        const uint16_t pn = in.included_parts[i];
        const size_t slot = static_cast<size_t>(pn) - 1;
        const uint16_t gen = in.generations[i];
        // Write into the flat array using gen_array_bytes layout.
        switch (gen_width) {
          case GenWidth::k2Bit: {
            const size_t byte_idx = (slot * 2) / 8;
            const size_t bit_off  = (slot * 2) % 8;
            dst[off + byte_idx] |= static_cast<uint8_t>((gen & 0x3u) << bit_off);
            break;
          }
          case GenWidth::kNibble: {
            const size_t byte_idx = slot / 2;
            if (slot % 2 == 0) {
              dst[off + byte_idx] = static_cast<uint8_t>((dst[off + byte_idx] & 0xF0u) | (gen & 0xFu));
            } else {
              dst[off + byte_idx] = static_cast<uint8_t>((dst[off + byte_idx] & 0x0Fu) | ((gen & 0xFu) << 4));
            }
            break;
          }
          case GenWidth::kByte:
            dst[off + slot] = static_cast<uint8_t>(gen & 0xFFu);
            break;
          case GenWidth::kU16:
            write_be_field<uint16_t>(dst + off + slot * 2, gen);
            break;
        }
      }
      off += total_flat;
    } else {
      // Modes 0-3
      // Step 2: default generation (fixed 0)
      if (off + gen_scalar_bytes(gen_width) > cap) return 0;
      write_gen_scalar(dst + off, 0, gen_width);
      off += gen_scalar_bytes(gen_width);

      // Step 3: Knob 2 region
      const RegionResult k2r = encode_deviation_region(dst + off, cap - off,
                                                        dev2_values, dev2_positions);
      if (!k2r.ok) return 0;
      k2_mode = k2r.mode;
      off += k2r.len;

      // Step 4: deviating generations
      {
        const size_t cnt = dev2_values.size();
        const size_t nb = gen_array_bytes(cnt, gen_width);
        if (off + nb > cap) return 0;
        std::vector<uint16_t> dev2_gens(cnt);
        for (size_t i = 0; i < cnt; ++i) dev2_gens[i] = in.generations[dev2_positions[i]];
        write_gen_array(dst + off, dev2_gens.data(), cnt, gen_width);
        off += nb;
      }
    }
  }

  if (keep_len) *keep_len = off;

  // ---- Knob 3: Boyer-Moore default discovery + running max ----
  uint64_t candidate = 0;
  size_t votes = 0;
  uint64_t max_size = 0;
  for (size_t i = 0; i < n; ++i) {
    const uint64_t s = in.sizes[i];
    if (s > max_size) max_size = s;
    if (votes == 0) { candidate = s; votes = 1; }
    else if (s == candidate) { ++votes; }
    else { --votes; }
  }
  const uint64_t default_size = candidate;
  const SizeWidth size_width = size_width_for_max(max_size);

  // ---- Steps 5-7: Knob 3 (descriptor or flat) ----
  SubsetMode k3_mode;
  {
    std::vector<uint16_t> dev3_values;
    std::vector<uint16_t> dev3_positions;
    for (size_t i = 0; i < n; ++i) {
      if (in.sizes[i] != default_size) {
        dev3_values.push_back(in.included_parts[i]);
        dev3_positions.push_back(static_cast<uint16_t>(i));
      }
    }

    const size_t dev_cnt  = dev3_values.size();
    const size_t vw       = size_width_bytes(size_width);
    const size_t flat_cost = k3_flat_bytes(size_width);

    size_t desc_cost = 0;
    if (dev_cnt == 0) {
      desc_cost = 0;
    } else {
      const uint16_t pos_first = dev3_positions.front();
      const uint16_t pos_last  = dev3_positions.back();
      const size_t gap_count = static_cast<size_t>(pos_last - pos_first + 1) - dev_cnt;
      size_t run_count = 1;
      for (size_t i = 1; i < dev_cnt; ++i) {
        if (dev3_positions[i] != static_cast<uint16_t>(dev3_positions[i-1] + 1)) ++run_count;
      }
      desc_cost = std::min({mode1_cost(gap_count), mode2_cost(run_count), mode3_cost(dev_cnt)});
    }
    const size_t total_desc = vw + desc_cost + dev_cnt * vw;
    const size_t total_flat = flat_cost;

    if (total_flat < total_desc) {
      k3_mode = SubsetMode::kFlatArray;
      if (off + total_flat > cap) return 0;
      std::memset(dst + off, 0, total_flat);
      for (size_t i = 0; i < n; ++i) {
        const size_t slot = static_cast<size_t>(in.included_parts[i]) - 1;
        write_size_value(dst + off + slot * vw, in.sizes[i], size_width);
      }
      off += total_flat;
    } else {
      // Step 5: default size
      if (off + vw > cap) return 0;
      write_size_value(dst + off, default_size, size_width);
      off += vw;

      // Step 6: Knob 3 region
      const RegionResult k3r = encode_deviation_region(dst + off, cap - off,
                                                        dev3_values, dev3_positions);
      if (!k3r.ok) return 0;
      k3_mode = k3r.mode;
      off += k3r.len;

      // Step 7: deviating sizes
      if (off + vw * dev_cnt > cap) return 0;
      for (size_t i = 0; i < dev_cnt; ++i) {
        write_size_value(dst + off, in.sizes[dev3_positions[i]], size_width);
        off += vw;
      }
    }
  }

  // ---- Header ----
  dst[0] = static_cast<uint8_t>((static_cast<uint8_t>(gen_width) << 4) |
                                static_cast<uint8_t>(size_width));
  dst[1] = static_cast<uint8_t>(k1.mode);
  dst[2] = static_cast<uint8_t>(k2_mode);
  dst[3] = static_cast<uint8_t>(k3_mode);

  return off;
}

//--------------------------------------------------------------------------------
std::optional<DecodedManifestBody> decode_manifest_body(std::string_view body)
{
  if (body.size() < 4) return std::nullopt;
  const auto* p = reinterpret_cast<const uint8_t*>(body.data());
  const uint8_t gw_raw = (p[0] >> 4) & 0xFu;
  const uint8_t sw_raw = p[0] & 0xFu;
  if (gw_raw > 3 || sw_raw > 2 || p[1] > 4 || p[2] > 4 || p[3] > 4) return std::nullopt;
  const GenWidth gen_width = static_cast<GenWidth>(gw_raw);
  const SizeWidth size_width = static_cast<SizeWidth>(sw_raw);
  const SubsetMode k1_mode = static_cast<SubsetMode>(p[1]);
  const SubsetMode k2_mode = static_cast<SubsetMode>(p[2]);
  const SubsetMode k3_mode = static_cast<SubsetMode>(p[3]);

  size_t off = 4;
  DecodedManifestBody out;

  // Step 1: Knob 1.
  if (!decode_knob1_region(body, &off, k1_mode, &out.included_parts)) return std::nullopt;
  const size_t n = out.included_parts.size();
  if (n == 0 || n > kMaxPartNumber) return std::nullopt;

  out.generations.assign(n, 0);

  // Steps 2-4: Knob 2.
  if (k2_mode == SubsetMode::kFlatArray) {
    const size_t flat_sz = k2_flat_bytes(gen_width);
    if (off + flat_sz > body.size()) return std::nullopt;
    for (size_t i = 0; i < n; ++i) {
      const size_t slot = static_cast<size_t>(out.included_parts[i]) - 1;
      out.generations[i] = read_gen_array_at(
          reinterpret_cast<const uint8_t*>(body.data()) + off, slot, gen_width);
    }
    off += flat_sz;
  } else {
    // Step 2: default generation.
    const size_t gsz = gen_scalar_bytes(gen_width);
    if (off + gsz > body.size()) return std::nullopt;
    const uint16_t default_gen = read_gen_scalar(p + off, gen_width);
    off += gsz;

    // Step 3: Knob 2 region.
    std::vector<uint16_t> dev2_positions;
    if (!decode_deviation_positions(body, &off, k2_mode, n, out.included_parts, &dev2_positions)) {
      return std::nullopt;
    }

    // Step 4: deviating generation values.
    const size_t dev2_count = dev2_positions.size();
    const size_t nb = gen_array_bytes(dev2_count, gen_width);
    if (off + nb > body.size()) return std::nullopt;
    const auto* arr = reinterpret_cast<const uint8_t*>(body.data()) + off;
    out.generations.assign(n, default_gen);
    for (size_t i = 0; i < dev2_count; ++i) {
      out.generations[dev2_positions[i]] = read_gen_array_at(arr, i, gen_width);
    }
    off += nb;
  }

  out.sizes.assign(n, 0);

  // Steps 5-7: Knob 3.
  const size_t vw = size_width_bytes(size_width);
  if (k3_mode == SubsetMode::kFlatArray) {
    const size_t flat_sz = k3_flat_bytes(size_width);
    if (off + flat_sz > body.size()) return std::nullopt;
    for (size_t i = 0; i < n; ++i) {
      const size_t slot = static_cast<size_t>(out.included_parts[i]) - 1;
      out.sizes[i] = read_size_value(p + off + slot * vw, size_width);
    }
    off += flat_sz;
  } else {
    // Step 5: default size.
    if (off + vw > body.size()) return std::nullopt;
    const uint64_t default_size = read_size_value(p + off, size_width);
    off += vw;

    // Step 6: Knob 3 region.
    std::vector<uint16_t> dev3_positions;
    if (!decode_deviation_positions(body, &off, k3_mode, n, out.included_parts, &dev3_positions)) {
      return std::nullopt;
    }

    // Step 7: deviating size values.
    const size_t dev3_count = dev3_positions.size();
    if (off + vw * dev3_count > body.size()) return std::nullopt;
    out.sizes.assign(n, default_size);
    for (size_t i = 0; i < dev3_count; ++i) {
      out.sizes[dev3_positions[i]] = read_size_value(p + off, size_width);
      off += vw;
    }
  }

  return out;
}

//--------------------------------------------------------------------------------
bool ManifestKeepCursor::SubsetScanner::parse(SubsetMode m, bool mode0_range,
                                              std::string_view body, size_t* offset)
{
  mode = m;
  mode0_is_range = mode0_range;
  const auto* p = reinterpret_cast<const uint8_t*>(body.data());
  size_t off = *offset;
  auto fits = [&](size_t extra) { return off + extra <= body.size(); };

  switch (mode) {
    case SubsetMode::kContinuous: {
      if (!mode0_is_range) break;  // empty (Knob 2's zero-deviation sentinel)
      if (!fits(4)) return false;
      first = read_be_field<uint16_t>(p + off);
      last = read_be_field<uint16_t>(p + off + 2);
      off += 4;
      cur = first;
      break;
    }
    case SubsetMode::kContinuousExclude: {
      if (!fits(6)) return false;
      first = read_be_field<uint16_t>(p + off);
      last = read_be_field<uint16_t>(p + off + 2);
      excl_count = read_be_field<uint16_t>(p + off + 4);
      off += 6;
      if (!fits(static_cast<size_t>(excl_count) * 2)) return false;
      array_bytes = body.substr(off, static_cast<size_t>(excl_count) * 2);
      off += static_cast<size_t>(excl_count) * 2;
      cur = first;
      fwd_idx = 0;
      break;
    }
    case SubsetMode::kIntervalSet: {
      if (!fits(2)) return false;
      run_count = read_be_field<uint16_t>(p + off);
      off += 2;
      if (!fits(static_cast<size_t>(run_count) * 4)) return false;
      array_bytes = body.substr(off, static_cast<size_t>(run_count) * 4);
      off += static_cast<size_t>(run_count) * 4;
      fwd_idx = 0;
      if (run_count > 0) {
        cur = read_be_field<uint16_t>(reinterpret_cast<const uint8_t*>(array_bytes.data()));
      }
      break;
    }
    case SubsetMode::kInclusionList: {
      if (!fits(2)) return false;
      list_count = read_be_field<uint16_t>(p + off);
      off += 2;
      if (!fits(static_cast<size_t>(list_count) * 2)) return false;
      array_bytes = body.substr(off, static_cast<size_t>(list_count) * 2);
      off += static_cast<size_t>(list_count) * 2;
      fwd_idx = 0;
      break;
    }
    case SubsetMode::kFlatArray:
      // Not used via SubsetScanner -- handled in cursor directly.
      return false;
  }
  *offset = off;
  return true;
}

std::optional<uint16_t> ManifestKeepCursor::SubsetScanner::peek()
{
  switch (mode) {
    case SubsetMode::kContinuous: {
      if (!mode0_is_range) return std::nullopt;
      if (cur > last) return std::nullopt;
      return static_cast<uint16_t>(cur);
    }
    case SubsetMode::kContinuousExclude: {
      const auto* ap = reinterpret_cast<const uint8_t*>(array_bytes.data());
      while (cur <= last) {
        if (fwd_idx < excl_count) {
          const uint16_t ev = read_be_field<uint16_t>(ap + static_cast<size_t>(fwd_idx) * 2);
          if (static_cast<uint16_t>(cur) == ev) { ++cur; ++fwd_idx; continue; }
        }
        return static_cast<uint16_t>(cur);
      }
      return std::nullopt;
    }
    case SubsetMode::kIntervalSet: {
      if (fwd_idx >= run_count) return std::nullopt;
      const auto* ap = reinterpret_cast<const uint8_t*>(array_bytes.data());
      uint16_t run_last = read_be_field<uint16_t>(ap + static_cast<size_t>(fwd_idx) * 4 + 2);
      while (cur > run_last) {
        ++fwd_idx;
        if (fwd_idx >= run_count) return std::nullopt;
        cur = read_be_field<uint16_t>(ap + static_cast<size_t>(fwd_idx) * 4);
        run_last = read_be_field<uint16_t>(ap + static_cast<size_t>(fwd_idx) * 4 + 2);
      }
      return static_cast<uint16_t>(cur);
    }
    case SubsetMode::kInclusionList: {
      if (fwd_idx >= list_count) return std::nullopt;
      const auto* ap = reinterpret_cast<const uint8_t*>(array_bytes.data());
      return read_be_field<uint16_t>(ap + static_cast<size_t>(fwd_idx) * 2);
    }
    case SubsetMode::kFlatArray:
      return std::nullopt;
  }
  return std::nullopt;
}

void ManifestKeepCursor::SubsetScanner::advance()
{
  switch (mode) {
    case SubsetMode::kContinuous:
      if (mode0_is_range) ++cur;
      return;
    case SubsetMode::kContinuousExclude:
    case SubsetMode::kIntervalSet:
      ++cur;
      return;
    case SubsetMode::kInclusionList:
      ++fwd_idx;
      return;
    case SubsetMode::kFlatArray:
      return;
  }
}

//--------------------------------------------------------------------------------
ManifestKeepCursor::ManifestKeepCursor(std::string_view body)
{
  if (body.size() < 4) return;
  const auto* p = reinterpret_cast<const uint8_t*>(body.data());
  const uint8_t gw_raw = (p[0] >> 4) & 0xFu;
  if (gw_raw > 3 || p[1] > 4 || p[2] > 4) return;
  gen_width_ = static_cast<GenWidth>(gw_raw);
  k1_mode_ = static_cast<SubsetMode>(p[1]);
  k2_mode_ = static_cast<SubsetMode>(p[2]);

  size_t off = 4;

  // Parse Knob 1.
  if (k1_mode_ == SubsetMode::kFlatArray) {
    // bitmap: prefixed with uint16 byte-count
    if (off + 2 > body.size()) return;
    const uint16_t bmap_bytes = read_be_field<uint16_t>(p + off);
    off += 2;
    if (off + bmap_bytes > body.size() || bmap_bytes > kK1BitmapMax) return;
    k1_bitmap_ = body.substr(off, bmap_bytes);
    off += bmap_bytes;
  } else {
    if (!knob1_.parse(k1_mode_, /*mode0_range=*/true, body, &off)) return;
  }

  // Parse Knob 2.
  if (k2_mode_ == SubsetMode::kFlatArray) {
    const size_t flat_sz = k2_flat_bytes(gen_width_);
    if (off + flat_sz > body.size()) return;
    k2_flat_ = body.substr(off, flat_sz);
    off += flat_sz;
  } else {
    // Step 2: default generation scalar.
    const size_t gsz = gen_scalar_bytes(gen_width_);
    if (off + gsz > body.size()) return;
    default_generation_ = read_gen_scalar(p + off, gen_width_);
    off += gsz;

    // Step 3: Knob 2 region.
    if (!knob2_.parse(k2_mode_, /*mode0_range=*/false, body, &off)) return;

    // Count deviating values to know how many bytes step 4 occupies.
    size_t dev_count = 0;
    switch (k2_mode_) {
      case SubsetMode::kContinuous:
        dev_count = 0;
        break;
      case SubsetMode::kContinuousExclude:
        dev_count = static_cast<size_t>(knob2_.last - knob2_.first + 1) - knob2_.excl_count;
        break;
      case SubsetMode::kIntervalSet: {
        const auto* ap = reinterpret_cast<const uint8_t*>(knob2_.array_bytes.data());
        for (uint16_t i = 0; i < knob2_.run_count; ++i) {
          const uint16_t rf = read_be_field<uint16_t>(ap + static_cast<size_t>(i) * 4);
          const uint16_t rl = read_be_field<uint16_t>(ap + static_cast<size_t>(i) * 4 + 2);
          if (rl < rf) return;
          dev_count += static_cast<size_t>(rl - rf + 1);
        }
        break;
      }
      case SubsetMode::kInclusionList:
        dev_count = knob2_.list_count;
        break;
      case SubsetMode::kFlatArray:
        return;  // should not happen (handled above)
    }

    const size_t values_bytes = gen_array_bytes(dev_count, gen_width_);
    if (off + values_bytes > body.size()) return;
    knob2_values_ = body.substr(off, values_bytes);
    // We don't advance off further -- cursor only reads Knob 1+2.
  }

  valid_ = true;
}

bool ManifestKeepCursor::knob1_contains(uint16_t part_number)
{
  if (k1_mode_ == SubsetMode::kFlatArray) {
    const size_t bit = static_cast<size_t>(part_number) - 1;
    const size_t byte_idx = bit / 8;
    if (byte_idx >= k1_bitmap_.size()) return false;
    const bool contained = (static_cast<uint8_t>(k1_bitmap_[byte_idx]) >> (bit % 8)) & 1u;
    if (contained) {
      // Count bits set before this part_number to get position.
      // We lazily maintain knob1_position_ -- it must track the position of
      // the last found part so knob2_deviates gets the correct position arg.
      // Since is_kept calls knob1_contains in ascending order, we can
      // count bits from the previous position.
      // Simple: recount from start each time (correct, not hot path for flat).
      uint16_t pos = 0;
      for (size_t b = 0; b < bit; ++b) {
        const size_t bi = b / 8;
        if (bi >= k1_bitmap_.size()) break;
        if ((static_cast<uint8_t>(k1_bitmap_[bi]) >> (b % 8)) & 1u) ++pos;
      }
      knob1_position_ = pos;
    }
    return contained;
  }
  for (;;) {
    const auto v = knob1_.peek();
    if (!v) return false;
    if (*v < part_number) { knob1_.advance(); ++knob1_position_; continue; }
    return *v == part_number;
  }
}

bool ManifestKeepCursor::knob2_deviates(uint16_t part_number, uint16_t position,
                                        uint16_t* out_value)
{
  if (k2_mode_ == SubsetMode::kFlatArray) {
    const size_t slot = static_cast<size_t>(part_number) - 1;
    const auto* arr = reinterpret_cast<const uint8_t*>(k2_flat_.data());
    const uint16_t v = read_gen_array_at(arr, slot, gen_width_);
    if (v != 0) { *out_value = v; return true; }
    return false;
  }
  const bool position_space = (knob2_.mode == SubsetMode::kContinuousExclude ||
                               knob2_.mode == SubsetMode::kIntervalSet);
  const uint16_t query = position_space ? position : part_number;
  for (;;) {
    const auto v = knob2_.peek();
    if (!v) return false;
    if (*v < query) { knob2_.advance(); ++knob2_value_idx_; continue; }
    if (*v == query) {
      const auto* arr = reinterpret_cast<const uint8_t*>(knob2_values_.data());
      *out_value = read_gen_array_at(arr, knob2_value_idx_, gen_width_);
      return true;
    }
    return false;
  }
}

bool ManifestKeepCursor::is_kept(uint16_t part_number, uint16_t generation)
{
  if (!valid_) return false;
  if (!last_part_has_result_ || part_number != last_part_number_) {
    last_part_number_ = part_number;
    last_part_included_ = knob1_contains(part_number);
    if (last_part_included_) {
      uint16_t dev_value = 0;
      last_part_kept_generation_ = knob2_deviates(part_number, knob1_position_, &dev_value)
          ? dev_value
          : default_generation_;
    }
    last_part_has_result_ = true;
  }
  return last_part_included_ && generation == last_part_kept_generation_;
}

}  // namespace kvrgw
