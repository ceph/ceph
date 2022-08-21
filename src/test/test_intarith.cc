// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <climits>
#include <gtest/gtest.h>
#include "include/intarith.h"

TEST(intarith, cbits) {
  ASSERT_EQ(0u, cbits(0));
  ASSERT_EQ(1u, cbits(1));
  ASSERT_EQ(2u, cbits(2));
  ASSERT_EQ(2u, cbits(3));
  ASSERT_EQ(3u, cbits(4));
  ASSERT_EQ(0u, cbits(0));
  ASSERT_EQ(1u, cbits(1));
  ASSERT_EQ(2u, cbits(2));
  ASSERT_EQ(2u, cbits(3));
  ASSERT_EQ(3u, cbits(4));
  ASSERT_EQ(9u, cbits(0x100));
  ASSERT_EQ(32u, cbits(0xffffffff));
  ASSERT_EQ(32u, cbits(0xffffffff));
  ASSERT_EQ(32u, cbits(0xffffffff));
  ASSERT_EQ(64u, cbits(0xffffffffffffffff));
}

TEST(intarith, ctz) {
  ASSERT_EQ(32u, ctz(0));
  ASSERT_EQ(0u, ctz(1));
  ASSERT_EQ(1u, ctz(2));
  ASSERT_EQ(0u, ctz(3));
  ASSERT_EQ(2u, ctz(4));
  ASSERT_EQ(64u, ctz(UINT64_C(0)));
  ASSERT_EQ(0u, ctz(UINT64_C(1)));
  ASSERT_EQ(1u, ctz(UINT64_C(2)));
  ASSERT_EQ(0u, ctz(UINT64_C(3)));
  ASSERT_EQ(2u, ctz(UINT64_C(4)));
  ASSERT_EQ(8u, ctz(0x100));
  ASSERT_EQ(8u, ctz(UINT64_C(0x100)));
  ASSERT_EQ(0u, ctz(0xffffffff));
  ASSERT_EQ(0u, ctz(UINT32_C(0xffffffff)));
  ASSERT_EQ(0u, ctz(UINT64_C(0xffffffff)));
  ASSERT_EQ(20u, ctz(UINT64_C(0xffffffff00000)));
  ASSERT_EQ(48u, ctz(UINT64_C(0xff000000000000)));
}

TEST(intarith, p2family) {
  ASSERT_EQ(1024, p2align(1200, 1024));
  ASSERT_EQ(1024, p2align(1024, 1024));
  ASSERT_EQ(0x1200, p2align(0x1234, 0x100));
  ASSERT_EQ(0x5600, p2align(0x5600, 0x100));

  ASSERT_EQ(0x34, p2phase(0x1234, 0x100));
  ASSERT_EQ(0x00, p2phase(0x5600, 0x100));

  ASSERT_EQ(0xcc, p2nphase(0x1234, 0x100));
  ASSERT_EQ(0x00, p2nphase(0x5600, 0x100));

  ASSERT_EQ(0x1300, p2roundup(0x1234, 0x100));
  ASSERT_EQ(0x5600, p2roundup(0x5600, 0x100));
}
