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

#include <gtest/gtest.h>
#include "rgw_range_projection.h"

using namespace std;

// helpers to build synthetic compression block maps

static RGWCompressionInfo make_cs_info(
    const vector<pair<uint64_t, uint64_t>>& blocks,
    uint64_t orig_size,
    const string& type = "zlib")
{
  /*
   * Each pair is (uncompressed_size, compressed_size).
   * We compute old_ofs / new_ofs cumulatively.
   */
  RGWCompressionInfo cs;
  cs.compression_type = type;
  cs.orig_size = orig_size;

  uint64_t old_ofs = 0;
  uint64_t new_ofs = 0;
  for (auto& [plain_len, comp_len] : blocks) {
    compression_block b;
    b.old_ofs = old_ofs;
    b.new_ofs = new_ofs;
    b.len = comp_len;
    cs.blocks.push_back(b);
    old_ofs += plain_len;
    new_ofs += comp_len;
  }
  return cs;
}

TEST(DiskRange, EmptyWhenEndLessThanOfs)
{
  DiskRange r;
  r.ofs = 10;
  r.end = 5;
  EXPECT_TRUE(r.empty());
  EXPECT_EQ(0u, r.disk_bytes());
}

TEST(DiskRange, NotEmptyWhenEndEqualsOfs)
{
  DiskRange r;
  r.ofs = 10;
  r.end = 10;
  EXPECT_FALSE(r.empty());
  EXPECT_EQ(1u, r.disk_bytes());
}

TEST(DiskRange, DiskBytesCorrect)
{
  DiskRange r;
  r.ofs = 100;
  r.end = 199;
  EXPECT_EQ(100u, r.disk_bytes());
}

TEST(DiskRange, DefaultIsEmpty)
{
  DiskRange r;
  // default: ofs=0, end=-1
  EXPECT_TRUE(r.empty());
  EXPECT_EQ(0u, r.disk_bytes());
}

TEST(ProjectDecompress, FullObjectReturnsEntireCompressedRange)
{
  // 3 blocks: 4096 plain -> 2048 compressed each
  auto cs = make_cs_info(
      {{4096, 2048}, {4096, 2048}, {4096, 2048}},
      12288);

  auto dr = project_compress_range(0, 12287, cs, false);

  // should span all blocks: disk [0, 6143]
  EXPECT_EQ(0, dr.ofs);
  EXPECT_EQ(6143, dr.end);
  EXPECT_EQ(0u, dr.first_block_idx);
  EXPECT_EQ(2u, dr.last_block_idx);
  EXPECT_EQ(0, dr.q_ofs);
  EXPECT_EQ(12288u, dr.q_len);
  EXPECT_EQ(6144u, dr.disk_bytes());
}

TEST(ProjectDecompress, PartialRangeMapsToCorrectBlocks)
{
  // 4 blocks, each 4096 plain -> 2000 compressed
  auto cs = make_cs_info(
      {{4096, 2000}, {4096, 2000}, {4096, 2000}, {4096, 2000}},
      16384);

  // request plaintext [4200, 8191] — entirely within block 1
  auto dr = project_compress_range(4200, 8191, cs, true);

  EXPECT_EQ(1u, dr.first_block_idx);
  EXPECT_EQ(1u, dr.last_block_idx);
  EXPECT_EQ(104, dr.q_ofs); // 4200 - 4096
  EXPECT_EQ(3992u, dr.q_len); // 8191 - 4200 + 1
  EXPECT_EQ(2000, dr.ofs);
  EXPECT_EQ(3999, dr.end);
}

TEST(ProjectDecompress, PartialRangeSpansMultipleBlocks)
{
  // 4 blocks, each 4096 plain -> 2000 compressed
  auto cs = make_cs_info(
      {{4096, 2000}, {4096, 2000}, {4096, 2000}, {4096, 2000}},
      16384);

  // request plaintext [100, 8999] — spans blocks 0, 1, and 2
  auto dr = project_compress_range(100, 8999, cs, true);

  EXPECT_EQ(0u, dr.first_block_idx);
  EXPECT_EQ(2u, dr.last_block_idx);
  EXPECT_EQ(100, dr.q_ofs);
  EXPECT_EQ(8900u, dr.q_len);
  EXPECT_EQ(0, dr.ofs);
  EXPECT_EQ(5999, dr.end);
}

TEST(ProjectDecompress, SingleBlock)
{
  auto cs = make_cs_info({{8192, 4000}}, 8192);

  auto dr = project_compress_range(100, 999, cs, true);

  EXPECT_EQ(0u, dr.first_block_idx);
  EXPECT_EQ(0u, dr.last_block_idx);
  EXPECT_EQ(100, dr.q_ofs);
  EXPECT_EQ(900u, dr.q_len);
  EXPECT_EQ(0, dr.ofs);
  EXPECT_EQ(3999, dr.end);
  EXPECT_EQ(4000u, dr.disk_bytes());
}

TEST(ProjectDecryptCBC, AlignedToBlockBoundary)
{
  // [100, 999] with block_size=4096 -> aligned to [0, 4095]
  auto dr = project_encrypt_range(100, 999, 4096, 4096, 8192, {});

  EXPECT_EQ(0, dr.ofs);
  EXPECT_EQ(4095, dr.end);
  EXPECT_EQ(100u, dr.enc_begin_skip);
  EXPECT_EQ(100u, dr.skip);
  EXPECT_EQ(4096u, dr.disk_bytes());
}

TEST(ProjectDecryptCBC, FullObject)
{
  // full object [0, 8191]
  auto dr = project_encrypt_range(0, 8191, 4096, 4096, 8192, {});

  EXPECT_EQ(0, dr.ofs);
  EXPECT_EQ(8191, dr.end);
  EXPECT_EQ(0u, dr.enc_begin_skip);
  EXPECT_EQ(0u, dr.skip);
  EXPECT_EQ(8192u, dr.disk_bytes());
}

TEST(ProjectDecryptCBC, NoExpansion)
{
  // CBC: enc_block_size == block_size, so no expansion
  // [4096, 8191] -> [4096, 8191]
  auto dr = project_encrypt_range(4096, 8191, 4096, 4096, 16384, {});

  EXPECT_EQ(4096, dr.ofs);
  EXPECT_EQ(8191, dr.end);
  EXPECT_EQ(0u, dr.enc_begin_skip);
  EXPECT_EQ(4096u, dr.disk_bytes());
}

TEST(ProjectDecryptCBC, MultipartCrossesPartBoundary)
{
  // 2 parts, each 8192 bytes encrypted (=plaintext for CBC)
  // request [4000, 12000] crosses parts
  vector<size_t> parts = {8192, 8192};
  auto dr = project_encrypt_range(4000, 12000, 4096, 4096, 16384, parts);

  // 4000 < 4096, so start is within first block of part 0
  EXPECT_EQ(4000u, dr.enc_begin_skip);
  EXPECT_EQ(0, dr.ofs);
  // end lands in part 1 at offset 3808, block-aligned to 4095, plus part 0 size
  EXPECT_EQ(12287, dr.end);
}

static const size_t GCM_BLOCK = 4096;
static const size_t GCM_ENC_BLOCK = 4112; // 4096 + 16 byte tag

TEST(ProjectDecryptGCM, SizeExpansion)
{
  // plaintext [0, 4095] -> encrypted [0, 4111]
  // 1 chunk of 4096 plain -> 4112 encrypted
  auto dr = project_encrypt_range(0, 4095, GCM_BLOCK, GCM_ENC_BLOCK, 4112, {});

  EXPECT_EQ(0, dr.ofs);
  EXPECT_EQ(4111, dr.end);
  EXPECT_EQ(0u, dr.enc_begin_skip);
  EXPECT_EQ(4112u, dr.disk_bytes());
}

TEST(ProjectDecryptGCM, TailClampedToEncryptedTotalSize)
{
  // object with 2 chunks: total encrypted = 2 * 4112 = 8224
  // request plaintext [0, 8191] -> should clamp to [0, 8223]
  auto dr = project_encrypt_range(0, 8191, GCM_BLOCK, GCM_ENC_BLOCK, 8224, {});

  EXPECT_EQ(0, dr.ofs);
  EXPECT_EQ(8223, dr.end);
  EXPECT_EQ(8224u, dr.disk_bytes());
}

TEST(ProjectDecryptGCM, MidRangeAlignment)
{
  // plaintext [100, 4095] with 2 chunks (total 8224)
  auto dr = project_encrypt_range(100, 4095, GCM_BLOCK, GCM_ENC_BLOCK, 8224, {});

  EXPECT_EQ(0, dr.ofs);
  EXPECT_EQ(4111, dr.end);
  EXPECT_EQ(100u, dr.enc_begin_skip);
  EXPECT_EQ(4112u, dr.disk_bytes());
}

TEST(ProjectDecryptGCM, MultipartCumulativeOffsets)
{
  // 2 parts: part0 = 4112 bytes (1 chunk), part1 = 8224 bytes (2 chunks)
  // total encrypted = 12336
  // request plaintext [0, 4095]: all in part0
  vector<size_t> parts = {4112, 8224};
  auto dr = project_encrypt_range(0, 4095, GCM_BLOCK, GCM_ENC_BLOCK, 12336, parts);

  // entirely within part 0
  EXPECT_EQ(0, dr.ofs);
  EXPECT_EQ(4111, dr.end);
  EXPECT_EQ(0u, dr.enc_begin_skip);
}

TEST(ProjectDecryptGCM, MultipartSecondPart)
{
  // 2 parts: part0 = 4112 (1 plain chunk), part1 = 8224 (2 plain chunks)
  // plaintext sizes: part0=4096, part1=8192
  // request plaintext [4096, 8191]: starts at part1 offset 0
  vector<size_t> parts = {4112, 8224};
  auto dr = project_encrypt_range(4096, 8191, GCM_BLOCK, GCM_ENC_BLOCK, 12336, parts);

  // starts at beginning of part 1 (cumulative enc offset 4112)
  EXPECT_EQ(4112, dr.ofs);
  EXPECT_EQ(8223, dr.end); // 4112 + 4111
  EXPECT_EQ(0u, dr.enc_begin_skip);
}

TEST(ProjectDecryptGCM, EndBeyondLastPartClamped)
{
  // 1 part of 4112 bytes (1 chunk, 4096 plain)
  // request beyond: [0, 99999]
  vector<size_t> parts = {4112};
  auto dr = project_encrypt_range(0, 99999, GCM_BLOCK, GCM_ENC_BLOCK, 4112, parts);

  // end is clamped to the single part's encrypted size
  EXPECT_EQ(0, dr.ofs);
  EXPECT_EQ(4111, dr.end);
}

TEST(ProjectDecryptGCM, InvalidRangeStartClampedToEnd)
{
  // start > total plaintext but within part index bounds
  // For a simple (non-multipart) object: [50000, 50100] with total=4112
  auto dr = project_encrypt_range(50000, 50100, GCM_BLOCK, GCM_ENC_BLOCK, 4112, {});

  // ofs expands well past encrypted_total_size, so ofs > end -> empty range
  EXPECT_TRUE(dr.empty());
  EXPECT_EQ(0u, dr.disk_bytes());
}

TEST(CryptHelpers, LogicalToEncOffsetCBC)
{
  // CBC: identity
  EXPECT_EQ(1000, crypt_logical_to_enc_offset(1000, 4096, 4096));
  EXPECT_EQ(0, crypt_logical_to_enc_offset(0, 4096, 4096));
  EXPECT_EQ(8192, crypt_logical_to_enc_offset(8192, 4096, 4096));
}

TEST(CryptHelpers, LogicalToEncOffsetGCM)
{
  // GCM: chunk 0 stays in [0..4095]
  EXPECT_EQ(0, crypt_logical_to_enc_offset(0, 4096, 4112));
  EXPECT_EQ(4095, crypt_logical_to_enc_offset(4095, 4096, 4112));
  // chunk 1 starts at 4112
  EXPECT_EQ(4112, crypt_logical_to_enc_offset(4096, 4096, 4112));
  // chunk 2 starts at 8224
  EXPECT_EQ(8224, crypt_logical_to_enc_offset(8192, 4096, 4112));
}

TEST(CryptHelpers, AlignEncBlockEndCBC)
{
  // CBC: identity
  EXPECT_EQ(4095, crypt_align_enc_block_end(4095, 4096, 4096));
  EXPECT_EQ(100, crypt_align_enc_block_end(100, 4096, 4096));
}

TEST(CryptHelpers, AlignEncBlockEndGCM)
{
  // GCM: rounds up to end of encrypted block
  EXPECT_EQ(4111, crypt_align_enc_block_end(0, 4096, 4112));
  EXPECT_EQ(4111, crypt_align_enc_block_end(4095, 4096, 4112));
  EXPECT_EQ(8223, crypt_align_enc_block_end(4112, 4096, 4112));
}

TEST(CryptHelpers, EncToPlaintextSize)
{
  // CBC: identity
  EXPECT_EQ(8192u, crypt_enc_to_plaintext_size(8192, 4096, 4096));
  // GCM: 1 chunk
  EXPECT_EQ(4096u, crypt_enc_to_plaintext_size(4112, 4096, 4112));
  // GCM: 2 chunks
  EXPECT_EQ(8192u, crypt_enc_to_plaintext_size(8224, 4096, 4112));
  // GCM: partial chunk (4100 - 16 tag = 4084 plaintext)
  EXPECT_EQ(4084u, crypt_enc_to_plaintext_size(4100, 4096, 4112));
  // GCM: less than tag size -> 0
  EXPECT_EQ(0u, crypt_enc_to_plaintext_size(10, 4096, 4112));
}

