// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <vector>
#include <sys/types.h>

#include "rgw_compression_types.h"
#include "rgw_crypt.h"

// Stateless range projection: plaintext byte range -> on-disk byte range
struct DiskRange {
  off_t ofs = 0;        // disk start offset
  off_t end = -1;       // disk end offset (inclusive); end < ofs means empty
  off_t skip = 0;       // bytes to skip in first decompressed/decrypted block
  uint64_t length = 0;  // original plaintext request length
  size_t enc_begin_skip = 0; // plaintext offset within the starting encryption block

  bool empty() const { return end < ofs; }
  uint64_t disk_bytes() const { return empty() ? 0 : end - ofs + 1; }
};

// DiskRange plus block indices for decompression filter init
struct DecompressRange : DiskRange {
  size_t first_block_idx = 0;
  size_t last_block_idx = 0;
  off_t q_ofs = 0;    // offset within first decompressed block
  uint64_t q_len = 0; // plaintext bytes requested
};

// Map plaintext range to compressed on-disk blocks.
inline DecompressRange project_compress_range(
    off_t ofs, off_t end,
    const RGWCompressionInfo& cs_info,
    bool partial_content)
{
  DecompressRange r;
  r.length = end + 1 - ofs;

  size_t first_idx = 0;
  size_t last_idx = 0;

  if (partial_content) {
    // user set a range — find the blocks that bracket it
    first_idx = 0;
    last_idx = 0;
    if (cs_info.blocks.size() > 1) {
      auto cmp_u = [](off_t o, const compression_block& e) {
        return (uint64_t)o < e.old_ofs;
      };
      auto cmp_l = [](const compression_block& e, off_t o) {
        return e.old_ofs <= (uint64_t)o;
      };
      auto fb = std::upper_bound(cs_info.blocks.begin() + 1,
                                 cs_info.blocks.end(),
                                 ofs, cmp_u);
      first_idx = (fb - 1) - cs_info.blocks.begin();

      auto lb = std::lower_bound(fb,
                                 cs_info.blocks.end(),
                                 end, cmp_l);
      last_idx = (lb - 1) - cs_info.blocks.begin();
    }
  } else {
    // full object — use first and last blocks
    first_idx = 0;
    last_idx = cs_info.blocks.size() - 1;
  }

  const auto& first_block = cs_info.blocks[first_idx];
  const auto& last_block = cs_info.blocks[last_idx];

  r.first_block_idx = first_idx;
  r.last_block_idx = last_idx;
  r.q_ofs = ofs - first_block.old_ofs;
  r.q_len = end + 1 - ofs;
  r.skip = r.q_ofs;

  r.ofs = first_block.new_ofs;
  r.end = last_block.new_ofs + last_block.len - 1;

  return r;
}

// Find which multipart part contains a given plaintext offset.
inline void find_part_for_offset(
    off_t plaintext_ofs,
    const std::vector<size_t>& parts_len,
    size_t block_size,
    size_t enc_block_size,
    bool clamp_to_last,
    size_t& part_idx,
    off_t& offset_in_part,
    off_t& cumulative_encrypted)
{
  part_idx = 0;
  offset_in_part = plaintext_ofs;
  cumulative_encrypted = 0;

  size_t limit = (clamp_to_last && parts_len.size() > 0)
                 ? (parts_len.size() - 1)
                 : parts_len.size();

  while (part_idx < limit) {
    size_t part_plain = crypt_enc_to_plaintext_size(
        parts_len[part_idx], block_size, enc_block_size);
    if (offset_in_part < (off_t)part_plain) {
      break;
    }
    offset_in_part -= part_plain;
    cumulative_encrypted += parts_len[part_idx];
    part_idx++;
  }
}

// Map plaintext range to encrypted on-disk bytes.
inline DiskRange project_encrypt_range(
    off_t ofs, off_t end,
    size_t block_size,
    size_t enc_block_size,
    uint64_t encrypted_total_size,
    const std::vector<size_t>& parts_len)
{
  DiskRange r;
  r.length = end + 1 - ofs;

  if (parts_len.size() > 0) {
    // multipart object
    size_t start_part_idx;
    off_t start_offset_in_part;
    off_t start_cumulative;
    find_part_for_offset(ofs, parts_len, block_size, enc_block_size,
                         false, start_part_idx, start_offset_in_part,
                         start_cumulative);

    size_t end_part_idx;
    off_t end_offset_in_part;
    off_t end_cumulative;
    find_part_for_offset(end, parts_len, block_size, enc_block_size,
                         true, end_part_idx, end_offset_in_part,
                         end_cumulative);

    // block-align end within its part (in plaintext space)
    size_t part_plaintext_end = crypt_enc_to_plaintext_size(
        parts_len[end_part_idx], block_size, enc_block_size);
    off_t rounded_end = std::min(
        (off_t)((end_offset_in_part & ~(block_size - 1)) + (block_size - 1)),
        (off_t)(part_plaintext_end - 1));

    // enc_begin_skip is offset within the starting block
    r.enc_begin_skip = start_offset_in_part & (block_size - 1);
    r.skip = r.enc_begin_skip;

    // convert end offset: plaintext -> encrypted, align to encrypted block
    off_t enc_end = crypt_align_enc_block_end(
        crypt_logical_to_enc_offset(rounded_end, block_size, enc_block_size),
        block_size, enc_block_size);
    enc_end = std::min(enc_end, (off_t)(parts_len[end_part_idx] - 1));
    r.end = end_cumulative + enc_end;

    // convert start offset: align in plaintext, then to encrypted
    off_t aligned_start = std::max((off_t)0,
        start_offset_in_part - (off_t)r.enc_begin_skip);
    r.ofs = start_cumulative +
        crypt_logical_to_enc_offset(aligned_start, block_size, enc_block_size);

    // clamp start to end (handles invalid ranges)
    r.ofs = std::min(r.ofs, r.end);
  } else {
    // simple (non-multipart) object
    r.enc_begin_skip = ofs & (block_size - 1);
    r.skip = r.enc_begin_skip;

    // block-align in plaintext space
    off_t aligned_start = ofs & ~(block_size - 1);
    off_t aligned_end = (end & ~(block_size - 1)) + (block_size - 1);

    // convert to encrypted offsets
    r.ofs = crypt_logical_to_enc_offset(aligned_start, block_size, enc_block_size);
    r.end = crypt_align_enc_block_end(
        crypt_logical_to_enc_offset(aligned_end, block_size, enc_block_size),
        block_size, enc_block_size);

    // clamp to actual encrypted object size
    if (encrypted_total_size > 0 && r.end >= (off_t)encrypted_total_size) {
      r.end = encrypted_total_size - 1;
    }
  }

  return r;
}

