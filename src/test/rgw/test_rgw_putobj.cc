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


using StripeMap = std::map<uint64_t, uint64_t>; // offset, stripe_size

class StripeMapGen : public rgw::putobj::StripeGenerator {
  const StripeMap& stripes;
 public:
  StripeMapGen(const StripeMap& stripes) : stripes(stripes) {}

  int next(uint64_t offset, uint64_t *stripe_size) override {
    auto i = stripes.find(offset);
    if (i == stripes.end()) {
      return -ENOENT;
    }
    *stripe_size = i->second;
    return 0;
  }
};

TEST(PutObj_Stripe, DifferentStripeSize)
{
  MockProcessor mock;
  StripeMap stripes{
    { 0, 4},
    { 4, 6},
    {10, 2}
  };
  StripeMapGen gen(stripes);
  rgw::putobj::StripeProcessor processor(&mock, &gen, stripes.begin()->second);

  ASSERT_EQ(0, processor.process(string_buf("22"), 0));
  ASSERT_EQ(1u, mock.ops.size());
  EXPECT_EQ(Op({"22", 0}), mock.ops[0]);

  ASSERT_EQ(0, processor.process(string_buf("4444"), 2));
  ASSERT_EQ(4u, mock.ops.size());
  EXPECT_EQ(Op({"44", 2}), mock.ops[1]);
  EXPECT_EQ(Op({"", 4}), mock.ops[2]); // flush
  EXPECT_EQ(Op({"44", 0}), mock.ops[3]);

  ASSERT_EQ(0, processor.process(string_buf("666666"), 6));
  ASSERT_EQ(7u, mock.ops.size());
  EXPECT_EQ(Op({"6666", 2}), mock.ops[4]);
  EXPECT_EQ(Op({"", 6}), mock.ops[5]); // flush
  EXPECT_EQ(Op({"66", 0}), mock.ops[6]);

  ASSERT_EQ(0, processor.process({}, 12));
  ASSERT_EQ(8u, mock.ops.size());
  EXPECT_EQ(Op({"", 2}), mock.ops[7]); // flush

  // gen returns an error past this
  ASSERT_EQ(-ENOENT, processor.process(string_buf("1"), 12));
}

TEST(PutObj_Stripe, SkipFirstChunk)
{
  MockProcessor mock;
  StripeMap stripes{
    {0, 4},
    {4, 4},
  };
  StripeMapGen gen(stripes);
  rgw::putobj::StripeProcessor processor(&mock, &gen, stripes.begin()->second);

  ASSERT_EQ(0, processor.process(string_buf("666666"), 2));
  ASSERT_EQ(3u, mock.ops.size());
  EXPECT_EQ(Op({"66", 2}), mock.ops[0]);
  EXPECT_EQ(Op({"", 4}), mock.ops[1]); // flush
  EXPECT_EQ(Op({"6666", 0}), mock.ops[2]);

  ASSERT_EQ(0, processor.process({}, 8));
  ASSERT_EQ(4u, mock.ops.size());
  EXPECT_EQ(Op({"", 4}), mock.ops[3]); // flush
}
