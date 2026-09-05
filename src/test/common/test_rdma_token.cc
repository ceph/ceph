// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "common/rdma_token.h"

#include <string>

#include "gtest/gtest.h"

using ceph::rdma::parse_rdma_token;
using ceph::rdma::RDMA_TOKEN_MAX_LEN;

// the shape emitted by cuObject clients:
// raddr:rsize:rkey:lid:qp:has_gid:gid
static const std::string valid_token =
  "0102030405060708:01020304:0102aabb:0102:010203:1:"
  "0102030405060708090a0b0c0d0e0f10";

TEST(RdmaToken, ParseValid)
{
  auto w = parse_rdma_token(valid_token);
  ASSERT_TRUE(w);
  EXPECT_EQ(0x0102030405060708ull, w->addr);
  EXPECT_EQ(0x01020304ull, w->size);
}

TEST(RdmaToken, ParseMinimalFields)
{
  // only the leading addr:size fields are interpreted
  auto w = parse_rdma_token("ff:10:rest-is-opaque");
  ASSERT_TRUE(w);
  EXPECT_EQ(0xffull, w->addr);
  EXPECT_EQ(0x10ull, w->size);
}

TEST(RdmaToken, RejectMalformed)
{
  EXPECT_FALSE(parse_rdma_token(""));
  EXPECT_FALSE(parse_rdma_token("deadbeef"));           // no colon
  EXPECT_FALSE(parse_rdma_token("deadbeef:"));          // no second colon
  EXPECT_FALSE(parse_rdma_token(":1234:rkey"));         // empty addr
  EXPECT_FALSE(parse_rdma_token("1234::rkey"));         // empty size
  EXPECT_FALSE(parse_rdma_token("xyz:1234:rkey"));      // non-hex addr
  EXPECT_FALSE(parse_rdma_token("1234:no pe:rkey"));    // non-hex size
  EXPECT_FALSE(parse_rdma_token("11112222333344445:1:x")); // >16 hex digits
  EXPECT_FALSE(parse_rdma_token(std::string(RDMA_TOKEN_MAX_LEN + 1, '1')));
}

TEST(RdmaToken, MaxValues)
{
  auto w = parse_rdma_token("ffffffffffffffff:ffffffff:x");
  ASSERT_TRUE(w);
  EXPECT_EQ(~0ull, w->addr);
  EXPECT_EQ(0xffffffffull, w->size);
}
