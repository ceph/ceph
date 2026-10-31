// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

/**
 * TestSparseReadReply — unit tests for the split sparse-read reply format.
 *
 * These tests exercise CB_ObjectOperation_sparse_read and the flag logic
 * inside ObjectOperation::sparse_read without any cluster I/O.
 *
 * Coverage:
 *  1. The handler-registering sparse_read overloads set SPLIT_REPLY flag.
 *  2. The no-handler sparse_read(off,len) overload does NOT set the flag.
 *  3. set_last_op_flags() preserves the SPLIT_REPLY flag.
 *  4. Split reply (non-empty metadata) — extents and data decoded correctly.
 *  5. Legacy reply (empty metadata, combined outdata) — same result as (4).
 *  6. Empty-extent split reply — zero extents, zero data, rval untouched.
 *  7. Truncated/corrupt metadata — callback sets *prval = -EIO.
 *  8. Negative r: handler must not touch extents/data at all.
 *  9. Alignment: split reply preserves page alignment; legacy reply does not.
 */

#include <gtest/gtest.h>

#include <map>
#include <vector>
#include <string>

#include "include/rados.h"
#include "include/encoding.h"
#include "include/buffer.h"
#include "osd/osd_types.h"
#include "osdc/Objecter.h"

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

namespace {

/**
 * Build a bufferlist holding the legacy combined sparse-read outdata:
 *   encode(extents_map) ++ encode(data_bl)
 */
bufferlist make_legacy_outdata(const std::map<uint64_t, uint64_t>& extents,
                               const bufferlist& data)
{
  bufferlist bl;
  encode(extents, bl);
  encode(data, bl);
  return bl;
}

/**
 * Build the split-reply metadata bufferlist:
 *   encode(extents_map)
 */
bufferlist make_split_metadata(const std::map<uint64_t, uint64_t>& extents)
{
  bufferlist bl;
  encode(extents, bl);
  return bl;
}

/**
 * Drive the handler stored in an ObjectOperation for op index @idx with the
 * given metadata and data buflists, simulating what
 * Objecter::process_op_reply_handlers does.
 */
void invoke_handler(ObjectOperation& oo, size_t idx,
                    boost::system::error_code ec, int r,
                    const bufferlist& metadata, const bufferlist& data)
{
  ASSERT_LT(idx, oo.out_handler.size());
  ASSERT_TRUE(static_cast<bool>(oo.out_handler[idx]));
  std::move(oo.out_handler[idx])(ec, r, osd_reply_bl{metadata, data});
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// 1. Flag-setting: handler-registering overloads set SPLIT_REPLY
// ---------------------------------------------------------------------------

TEST(SparseReadFlag, Map_overload_sets_flag)
{
  ObjectOperation oo;
  std::map<uint64_t, uint64_t> extents;
  bufferlist data_bl;
  int rval = 0;
  oo.sparse_read(0, 4096, &extents, &data_bl, &rval);

  ASSERT_FALSE(oo.ops.empty());
  EXPECT_NE(0u,
    oo.ops[0].op.flags & CEPH_OSD_OP_FLAG_SUPPORT_OUTMETADATA)
    << "sparse_read(map overload) must set CEPH_OSD_OP_FLAG_SUPPORT_OUTMETADATA";
}

TEST(SparseReadFlag, Vector_overload_sets_flag)
{
  ObjectOperation oo;
  boost::system::error_code ec;
  std::vector<std::pair<uint64_t, uint64_t>> extents;
  bufferlist data_bl;
  oo.sparse_read(0, 4096, &ec, &extents, &data_bl);

  ASSERT_FALSE(oo.ops.empty());
  EXPECT_NE(0u,
    oo.ops[0].op.flags & CEPH_OSD_OP_FLAG_SUPPORT_OUTMETADATA)
    << "sparse_read(vector overload) must set CEPH_OSD_OP_FLAG_SUPPORT_OUTMETADATA";
}

// ---------------------------------------------------------------------------
// 2. Flag-setting: no-handler overload does NOT set SPLIT_REPLY
//
// sparse_read(off, len) has no output pointers and registers no decode
// handler, so there is nothing to decode a split reply into.  The flag is
// therefore absent.  Note: this overload has no known production callers;
// this test just documents the property.
// ---------------------------------------------------------------------------

TEST(SparseReadFlag, No_handler_overload_does_not_set_flag)
{
  ObjectOperation oo;
  oo.sparse_read(0, 4096);  // the (off, len) overload: no output pointers, no handler

  ASSERT_FALSE(oo.ops.empty());
  EXPECT_EQ(0u,
    oo.ops[0].op.flags & CEPH_OSD_OP_FLAG_SUPPORT_OUTMETADATA)
    << "sparse_read(off,len) must NOT set CEPH_OSD_OP_FLAG_SUPPORT_OUTMETADATA";
}

// ---------------------------------------------------------------------------
// 3. set_last_op_flags() preserves the SPLIT_REPLY flag
// ---------------------------------------------------------------------------

TEST(SparseReadFlag, SetLastOpFlags_preserves_split_reply_flag)
{
  ObjectOperation oo;
  std::map<uint64_t, uint64_t> extents;
  bufferlist data_bl;
  int rval = 0;
  oo.sparse_read(0, 4096, &extents, &data_bl, &rval);

  ASSERT_FALSE(oo.ops.empty());
  ASSERT_NE(0u, oo.ops[0].op.flags & CEPH_OSD_OP_FLAG_SUPPORT_OUTMETADATA);

  // Simulate librados set_op_flags2 calling set_last_op_flags with a value
  // that does NOT include SPLIT_REPLY (as the public API knows nothing about
  // this internal flag).
  oo.set_last_op_flags(CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);

  EXPECT_NE(0u, oo.ops[0].op.flags & CEPH_OSD_OP_FLAG_SUPPORT_OUTMETADATA)
    << "set_last_op_flags must keep CEPH_OSD_OP_FLAG_SUPPORT_OUTMETADATA";
  // The caller-supplied flag must also be set.
  EXPECT_NE(0u, oo.ops[0].op.flags & CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
}

// ---------------------------------------------------------------------------
// 4 & 5. Split reply vs legacy reply — both must yield identical results
// ---------------------------------------------------------------------------

namespace {

struct SparseReadDecodeFixture {
  // Object content: two extents [0,4096) and [8192,4096), with matching data.
  static constexpr uint64_t OFFSET_A = 0,     LEN_A = 4096;
  static constexpr uint64_t OFFSET_B = 8192,  LEN_B = 4096;

  const std::map<uint64_t, uint64_t> extents = {
    {OFFSET_A, LEN_A}, {OFFSET_B, LEN_B}
  };
  const std::string content_a = std::string(LEN_A, 'A');
  const std::string content_b = std::string(LEN_B, 'B');

  bufferlist raw_data() const {
    bufferlist bl;
    bl.append(content_a);
    bl.append(content_b);
    return bl;
  }
};

} // anonymous namespace

TEST(SparseReadDecode, SplitReply_decodes_extents_and_data)
{
  SparseReadDecodeFixture F;
  bufferlist metadata = make_split_metadata(F.extents);
  bufferlist data     = F.raw_data();

  ObjectOperation oo;
  std::map<uint64_t, uint64_t> got_extents;
  bufferlist got_data;
  int rval = 42;  // sentinel — should remain untouched on success
  oo.sparse_read(0, 12288, &got_extents, &got_data, &rval);

  ASSERT_NO_FATAL_FAILURE(
    invoke_handler(oo, 0, {}, 0, metadata, data));

  EXPECT_EQ(F.extents, got_extents);
  EXPECT_TRUE(got_data.contents_equal(data))
    << "split reply: data_bl must equal raw data";
  EXPECT_EQ(42, rval) << "rval must be unchanged when r == 0 and decode succeeds";
}

TEST(SparseReadDecode, LegacyReply_decodes_extents_and_data)
{
  SparseReadDecodeFixture F;
  bufferlist legacy = make_legacy_outdata(F.extents, F.raw_data());
  bufferlist empty_meta;

  ObjectOperation oo;
  std::map<uint64_t, uint64_t> got_extents;
  bufferlist got_data;
  int rval = 42;
  oo.sparse_read(0, 12288, &got_extents, &got_data, &rval);

  ASSERT_NO_FATAL_FAILURE(
    invoke_handler(oo, 0, {}, 0, empty_meta, legacy));

  EXPECT_EQ(F.extents, got_extents);
  EXPECT_EQ(F.raw_data().length(), got_data.length());
  EXPECT_TRUE(got_data.contents_equal(F.raw_data()))
    << "legacy reply: data_bl must equal raw data";
  EXPECT_EQ(42, rval) << "rval must be unchanged when r == 0 and decode succeeds";
}

TEST(SparseReadDecode, SplitAndLegacy_produce_identical_results)
{
  SparseReadDecodeFixture F;

  // Split decode
  std::map<uint64_t, uint64_t> split_extents;
  bufferlist split_data;
  int split_rval = 0;
  {
    ObjectOperation oo;
    oo.sparse_read(0, 12288, &split_extents, &split_data, &split_rval);
    bufferlist meta = make_split_metadata(F.extents);
    bufferlist data = F.raw_data();
    ASSERT_NO_FATAL_FAILURE(invoke_handler(oo, 0, {}, 0, meta, data));
  }

  // Legacy decode
  std::map<uint64_t, uint64_t> legacy_extents;
  bufferlist legacy_data;
  int legacy_rval = 0;
  {
    ObjectOperation oo;
    oo.sparse_read(0, 12288, &legacy_extents, &legacy_data, &legacy_rval);
    bufferlist legacy = make_legacy_outdata(F.extents, F.raw_data());
    bufferlist empty_meta;
    ASSERT_NO_FATAL_FAILURE(invoke_handler(oo, 0, {}, 0, empty_meta, legacy));
  }

  EXPECT_EQ(split_extents, legacy_extents);
  EXPECT_TRUE(split_data.contents_equal(legacy_data))
    << "split and legacy replies must produce identical data";
}

// ---------------------------------------------------------------------------
// 6. Empty-extent split reply
// ---------------------------------------------------------------------------

TEST(SparseReadDecode, EmptyExtent_SplitReply)
{
  // An empty map still encodes to 4 bytes; the OSD uses this for a
  // zero-length sparse_read, or when the requested range falls entirely in a
  // hole.
  std::map<uint64_t, uint64_t> empty_extents;
  bufferlist metadata = make_split_metadata(empty_extents);
  bufferlist empty_data;

  ObjectOperation oo;
  std::map<uint64_t, uint64_t> got_extents = {{99, 1}};  // pre-populated sentinel
  bufferlist got_data;
  got_data.append("X");  // sentinel
  int rval = 0;
  oo.sparse_read(0, 4096, &got_extents, &got_data, &rval);

  ASSERT_NO_FATAL_FAILURE(
    invoke_handler(oo, 0, {}, 0, metadata, empty_data));

  EXPECT_TRUE(got_extents.empty())
    << "empty-extent split reply must yield an empty extent map";
  // The data bufferlist must be empty (the handler shares raw outdata).
  EXPECT_EQ(0u, got_data.length())
    << "empty-extent split reply must yield an empty data bufferlist";
}

// When both metadata and data are empty and r >= 0 the handler must set -EIO
// (the sub-op was not executed or returned nothing sensible).
TEST(SparseReadDecode, BothEmpty_positive_r_gives_EIO)
{
  bufferlist empty_meta, empty_data;

  ObjectOperation oo;
  std::map<uint64_t, uint64_t> got_extents;
  bufferlist got_data;
  int rval = 0;
  oo.sparse_read(0, 4096, &got_extents, &got_data, &rval);

  ASSERT_NO_FATAL_FAILURE(
    invoke_handler(oo, 0, {}, 0, empty_meta, empty_data));

  EXPECT_EQ(-EIO, rval)
    << "both-empty positive-r reply must set *prval = -EIO";
}

// ---------------------------------------------------------------------------
// 7. Corrupt / truncated metadata → -EIO
// ---------------------------------------------------------------------------

TEST(SparseReadDecode, CorruptMetadata_gives_EIO)
{
  // A bufferlist with one byte is not a valid encoded map.
  bufferlist bad_meta;
  bad_meta.append("X");
  bufferlist empty_data;

  ObjectOperation oo;
  std::map<uint64_t, uint64_t> got_extents;
  bufferlist got_data;
  int rval = 0;
  oo.sparse_read(0, 4096, &got_extents, &got_data, &rval);

  ASSERT_NO_FATAL_FAILURE(
    invoke_handler(oo, 0, {}, 0, bad_meta, empty_data));

  EXPECT_EQ(-EIO, rval)
    << "corrupt metadata must set *prval = -EIO";
}

TEST(SparseReadDecode, TruncatedMetadata_gives_EIO)
{
  // Build a valid metadata bl then lop off the last byte.
  std::map<uint64_t, uint64_t> extents = {{0, 4096}};
  bufferlist metadata = make_split_metadata(extents);
  // Remove the last byte to corrupt it.
  bufferlist truncated;
  truncated.substr_of(metadata, 0, metadata.length() - 1);

  bufferlist empty_data;

  ObjectOperation oo;
  std::map<uint64_t, uint64_t> got_extents;
  bufferlist got_data;
  int rval = 0;
  oo.sparse_read(0, 4096, &got_extents, &got_data, &rval);

  ASSERT_NO_FATAL_FAILURE(
    invoke_handler(oo, 0, {}, 0, truncated, empty_data));

  EXPECT_EQ(-EIO, rval)
    << "truncated metadata must set *prval = -EIO";
}

TEST(SparseReadDecode, CorruptLegacyData_gives_EIO)
{
  // A bufferlist with a few bytes but not a valid encoded map.
  bufferlist bad_data;
  bad_data.append("bad");
  bufferlist empty_meta;

  ObjectOperation oo;
  std::map<uint64_t, uint64_t> got_extents;
  bufferlist got_data;
  int rval = 0;
  oo.sparse_read(0, 4096, &got_extents, &got_data, &rval);

  ASSERT_NO_FATAL_FAILURE(
    invoke_handler(oo, 0, {}, 0, empty_meta, bad_data));

  EXPECT_EQ(-EIO, rval)
    << "corrupt legacy outdata must set *prval = -EIO";
}

// ---------------------------------------------------------------------------
// 8. Negative r: handler must not touch extents/data at all
// ---------------------------------------------------------------------------

TEST(SparseReadDecode, Negative_r_leaves_outputs_untouched)
{
  SparseReadDecodeFixture F;
  bufferlist metadata = make_split_metadata(F.extents);
  bufferlist data     = F.raw_data();

  ObjectOperation oo;
  std::map<uint64_t, uint64_t> got_extents = {{99, 1}};  // sentinel
  bufferlist got_data;
  got_data.append("sentinel");
  int rval = 0;
  oo.sparse_read(0, 12288, &got_extents, &got_data, &rval);

  // Drive with r = -ENOENT.
  ASSERT_NO_FATAL_FAILURE(
    invoke_handler(oo, 0, {}, -ENOENT, metadata, data));

  // CB_ObjectOperation_sparse_read skips decode entirely when r < 0.
  EXPECT_EQ(1u, got_extents.size()) << "negative r must leave extents unchanged";
  EXPECT_FALSE(got_data.contents_equal(data))
    << "negative r must leave data_bl unchanged";
}

// ---------------------------------------------------------------------------
// 9. Alignment: split reply preserves page alignment; legacy reply does not
// ---------------------------------------------------------------------------

TEST(SparseReadDecode, SplitReply_preserves_page_alignment)
{
  SparseReadDecodeFixture F;
  bufferlist metadata = make_split_metadata(F.extents);

  // Construct a page-aligned data bufferlist
  bufferlist aligned_data;
  aligned_data.append(buffer::create_page_aligned(8192));
  ASSERT_TRUE(aligned_data.is_page_aligned());

  ObjectOperation oo;
  std::map<uint64_t, uint64_t> got_extents;
  bufferlist got_data;
  int rval = 0;
  oo.sparse_read(0, 12288, &got_extents, &got_data, &rval);

  ASSERT_NO_FATAL_FAILURE(
    invoke_handler(oo, 0, {}, 0, metadata, aligned_data));

  EXPECT_EQ(F.extents, got_extents);
  EXPECT_TRUE(got_data.contents_equal(aligned_data));
  // In split reply, got_data shares aligned_data directly without slicing
  EXPECT_TRUE(got_data.is_page_aligned())
    << "split reply must preserve page alignment of the data bufferlist";
}

TEST(SparseReadDecode, LegacyReply_is_not_page_aligned)
{
  SparseReadDecodeFixture F;

  bufferlist aligned_data;
  aligned_data.append(buffer::create_page_aligned(8192));
  ASSERT_TRUE(aligned_data.is_page_aligned());

  bufferlist legacy = make_legacy_outdata(F.extents, aligned_data);
  // Rebuild into a contiguous page-aligned buffer to simulate how the message
  // data section arrives over the wire from the Messenger.
  legacy.rebuild_page_aligned();
  ASSERT_TRUE(legacy.is_page_aligned());

  bufferlist empty_meta;

  ObjectOperation oo;
  std::map<uint64_t, uint64_t> got_extents;
  bufferlist got_data;
  int rval = 0;
  oo.sparse_read(0, 12288, &got_extents, &got_data, &rval);

  ASSERT_NO_FATAL_FAILURE(
    invoke_handler(oo, 0, {}, 0, empty_meta, legacy));

  EXPECT_EQ(F.extents, got_extents);
  EXPECT_TRUE(got_data.contents_equal(aligned_data));
  // In legacy reply, decode(*data_bl, iter) creates a sub-buffer slice starting
  // after encode(extents) and the 4-byte length prefix, which is not page-aligned
  EXPECT_FALSE(got_data.is_page_aligned())
    << "legacy reply data bufferlist is offset and should not be page-aligned";
}
