// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw/rgw_putobj.h"
#include <gtest/gtest.h>

inline bufferlist string_buf(const char* buf) {
  bufferlist bl;
  bl.append(buffer::create_static(strlen(buf), (char*)buf));
  return bl;
}

struct Op {
  std::string data;
  uint64_t offset;
};
inline bool operator==(const Op& lhs, const Op& rhs) {
  return lhs.data == rhs.data && lhs.offset == rhs.offset;
}
inline std::ostream& operator<<(std::ostream& out, const Op& op) {
  return out << "{off=" << op.offset << " data='" << op.data << "'}";
}

struct MockProcessor : rgw::putobj::DataProcessor {
  std::vector<Op> ops;

  int process(bufferlist&& data, uint64_t offset) override {
    ops.push_back({data.to_str(), offset});
    return {};
  }
};

TEST(PutObj_Chunk, FlushHalf)
{
  MockProcessor mock;
  rgw::putobj::ChunkProcessor chunk(&mock, 4);

  ASSERT_EQ(0, chunk.process(string_buf("22"), 0));
  ASSERT_TRUE(mock.ops.empty()); // no writes

  ASSERT_EQ(0, chunk.process({}, 2)); // flush
  ASSERT_EQ(2u, mock.ops.size());
  EXPECT_EQ(Op({"22", 0}), mock.ops[0]);
  EXPECT_EQ(Op({"", 2}), mock.ops[1]);
}

TEST(PutObj_Chunk, One)
{
  MockProcessor mock;
  rgw::putobj::ChunkProcessor chunk(&mock, 4);

  ASSERT_EQ(0, chunk.process(string_buf("4444"), 0));
  ASSERT_EQ(1u, mock.ops.size());
  EXPECT_EQ(Op({"4444", 0}), mock.ops[0]);

  ASSERT_EQ(0, chunk.process({}, 4)); // flush
  ASSERT_EQ(2u, mock.ops.size());
  EXPECT_EQ(Op({"", 4}), mock.ops[1]);
}

TEST(PutObj_Chunk, OneAndFlushHalf)
{
  MockProcessor mock;
  rgw::putobj::ChunkProcessor chunk(&mock, 4);

  ASSERT_EQ(0, chunk.process(string_buf("22"), 0));
  ASSERT_TRUE(mock.ops.empty());

  ASSERT_EQ(0, chunk.process(string_buf("4444"), 2));
  ASSERT_EQ(1u, mock.ops.size());
  EXPECT_EQ(Op({"2244", 0}), mock.ops[0]);

  ASSERT_EQ(0, chunk.process({}, 6)); // flush
  ASSERT_EQ(3u, mock.ops.size());
  EXPECT_EQ(Op({"44", 4}), mock.ops[1]);
  EXPECT_EQ(Op({"", 6}), mock.ops[2]);
}

TEST(PutObj_Chunk, Two)
{
  MockProcessor mock;
  rgw::putobj::ChunkProcessor chunk(&mock, 4);

  ASSERT_EQ(0, chunk.process(string_buf("88888888"), 0));
  ASSERT_EQ(2u, mock.ops.size());
  EXPECT_EQ(Op({"8888", 0}), mock.ops[0]);
  EXPECT_EQ(Op({"8888", 4}), mock.ops[1]);

  ASSERT_EQ(0, chunk.process({}, 8)); // flush
  ASSERT_EQ(3u, mock.ops.size());
  EXPECT_EQ(Op({"", 8}), mock.ops[2]);
}

TEST(PutObj_Chunk, TwoAndFlushHalf)
{
  MockProcessor mock;
  rgw::putobj::ChunkProcessor chunk(&mock, 4);

  ASSERT_EQ(0, chunk.process(string_buf("22"), 0));
  ASSERT_TRUE(mock.ops.empty());

  ASSERT_EQ(0, chunk.process(string_buf("88888888"), 2));
  ASSERT_EQ(2u, mock.ops.size());
  EXPECT_EQ(Op({"2288", 0}), mock.ops[0]);
  EXPECT_EQ(Op({"8888", 4}), mock.ops[1]);

  ASSERT_EQ(0, chunk.process({}, 10)); // flush
  ASSERT_EQ(4u, mock.ops.size());
  EXPECT_EQ(Op({"88", 8}), mock.ops[2]);
  EXPECT_EQ(Op({"", 10}), mock.ops[3]);
}
