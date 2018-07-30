// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>
#include <gtest/gtest.h>

#include "include/intarith.h"

TEST(intarith, cbits) {
  ASSERT_EQ(0u, cbits(0));
  ASSERT_EQ(1u, cbits(1));
  ASSERT_EQ(2u, cbits(2));
  ASSERT_EQ(2u, cbits(3));
  ASSERT_EQ(3u, cbits(4));
  ASSERT_EQ(0u, cbitsl(0));
  ASSERT_EQ(1u, cbitsl(1));
  ASSERT_EQ(2u, cbitsl(2));
  ASSERT_EQ(2u, cbitsl(3));
  ASSERT_EQ(3u, cbitsl(4));
  ASSERT_EQ(9u, cbits(0x100));
  ASSERT_EQ(32u, cbits(0xffffffff));
  ASSERT_EQ(32u, cbitsl(0xffffffff));
  ASSERT_EQ(32u, cbitsll(0xffffffff));
  ASSERT_EQ(64u, cbitsll(0xffffffffffffffff));
}

TEST(intarith, clz) {
  ASSERT_EQ(32u, clz(0));
  ASSERT_EQ(31u, clz(1));
  ASSERT_EQ(30u, clz(2));
  ASSERT_EQ(30u, clz(3));
  ASSERT_EQ(29u, clz(4));
  ASSERT_EQ(64u, clzll(0));
  ASSERT_EQ(63u, clzll(1));
  ASSERT_EQ(62u, clzll(2));
  ASSERT_EQ(62u, clzll(3));
  ASSERT_EQ(61u, clzll(4));
  ASSERT_EQ(23u, clz(0x100));
  ASSERT_EQ(55u, clzll(0x100));
  ASSERT_EQ(0u, clz(0xffffffff));
  ASSERT_EQ(32u, clzll(0xffffffff));
  ASSERT_EQ(0u, clzll(0xffffffffffffffff));
}

TEST(intarith, ctz) {
  ASSERT_EQ(32u, ctz(0));
  ASSERT_EQ(0u, ctz(1));
  ASSERT_EQ(1u, ctz(2));
  ASSERT_EQ(0u, ctz(3));
  ASSERT_EQ(2u, ctz(4));
  ASSERT_EQ(64u, ctzll(0));
  ASSERT_EQ(0u, ctzll(1));
  ASSERT_EQ(1u, ctzll(2));
  ASSERT_EQ(0u, ctzll(3));
  ASSERT_EQ(2u, ctzll(4));
  ASSERT_EQ(8u, ctz(0x100));
  ASSERT_EQ(8u, ctzll(0x100));
  ASSERT_EQ(0u, ctz(0xffffffff));
  ASSERT_EQ(0u, ctzl(0xffffffff));
  ASSERT_EQ(0u, ctzll(0xffffffff));
  ASSERT_EQ(20u, ctzll(0xffffffff00000));
  ASSERT_EQ(48u, ctzll(0xff000000000000ull));
}

TEST(intarith, p2family) {
  ASSERT_TRUE(isp2(0x100));
  ASSERT_FALSE(isp2(0x1234));

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
