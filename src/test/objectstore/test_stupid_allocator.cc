// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <stdio.h>
#include <string.h>
#include <iostream>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <thread>
#include <iostream>
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "include/stringify.h"
#include "include/scope_guard.h"
#include "common/errno.h"
#include <gtest/gtest.h>

#include "os/bluestore/BlueFS.h"
#include "os/bluestore/StupidAllocator.h"
#include "os/bluestore/StupidAllocator2.h"


#define dout_context cct
#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << "stupidalloc 0x" << this << " "


template<typename T>  // tuple<type to test on, test array size>
class AllocatorTest : public ::testing::Test {

 public:
  typedef T Allocator;
};

typedef ::testing::Types<
  StupidAllocator2,
  StupidAllocator
  > IntervalSetTypes;

TYPED_TEST_CASE(AllocatorTest, IntervalSetTypes);

TYPED_TEST(AllocatorTest, test_add_free_rm_free) {
  typename TestFixture::Allocator sa(g_ceph_context);

  constexpr size_t K = 1<<15;
  constexpr size_t L = 1<<12;

  sa.init_add_free(0, K*3);
  sa.init_add_free(K*3, L);
  sa.init_add_free(K*3+L, K*3);
  sa.init_rm_free(K*3-L, K*3+L*2);
}

TYPED_TEST(AllocatorTest, test_add_free_rm_free_problem_in_recursion) {
  typename TestFixture::Allocator sa(g_ceph_context);

  sa.init_add_free(0x0,0x20000);

  sa.init_add_free(0x40000,0x40000);
  sa.init_add_free(0x200000,0x40000);

  sa.init_add_free(0x80000, 0xc0000);
  sa.init_add_free(0x180000, 0x80000);
  sa.init_add_free(0x480000, 0x80000);

  sa.init_add_free(0x280000, 0x1c0000);
  sa.init_add_free(0x500000, 0x340000);
  sa.init_add_free(0x880000, 0x780000);

  sa.init_rm_free (0xc0000, 0x40000);
}

TYPED_TEST(AllocatorTest, test_automatic_hint) {
  typename TestFixture::Allocator sa(g_ceph_context);
  sa.init_add_free(0x1000,0xff000);
  sa.init_add_free(0x200000,0x300000);
  sa.init_add_free(0xa00000,0x300000);
  sa.init_add_free(0x1400000,0x300000);
  sa.init_add_free(0x1d00000,0x300000);
  sa.init_add_free(0x2700000,0x300000);
  sa.init_add_free(0x3100000,0x300000);
  sa.init_add_free(0x3c00000,0x300000);
  sa.init_add_free(0x5200000,0x300000);
  sa.init_add_free(0x6b00000,0x300000);
  sa.init_add_free(0x8300000,0x300000);
  sa.init_add_free(0x9a00000,0x300000);
  sa.init_add_free(0xb100000,0x300000);
  sa.init_add_free(0xcc00000,0x300000);
  sa.init_add_free(0xe300000,0x300000);
  sa.init_add_free(0xfe00000,0x300000);
  sa.init_add_free(0x11800000,0x300000);
  sa.init_add_free(0x13100000,0x300000);
  sa.init_add_free(0x14a00000,0x300000);
  sa.init_add_free(0x16400000,0x300000);
  sa.init_add_free(0x17c00000,0x300000);
  sa.init_add_free(0x18e00000,0x300000);
  sa.init_add_free(0x19b00000,0x300000);
  sa.init_add_free(0x1aa00000,0x300000);
  sa.init_add_free(0x1b500000,0x300000);
  sa.init_add_free(0x1c700000,0x300000);
  sa.init_add_free(0x1df00000,0x300000);
  sa.init_add_free(0x1fa00000,0x300000);
  sa.init_add_free(0x21200000,0x300000);
  sa.init_add_free(0x22b00000,0x300000);
  sa.init_add_free(0x24500000,0x300000);
  sa.init_add_free(0x25f00000,0x300000);
  sa.init_add_free(0x27a00000,0x300000);
  sa.init_add_free(0x29300000,0x300000);
  sa.init_add_free(0x2ad00000,0x300000);
  sa.init_add_free(0x2c800000,0x300000);
  sa.init_add_free(0x2e000000,0x300000);
  sa.init_add_free(0x2f800000,0x300000);
  sa.init_add_free(0x31400000,0x300000);
  sa.init_add_free(0x32c00000,0x300000);
  sa.init_add_free(0x33c00000,0x300000);
  sa.init_add_free(0x34800000,0x300000);
  sa.init_add_free(0x35d00000,0x300000);
  sa.init_add_free(0x37a00000,0x300000);
  sa.init_add_free(0x39400000,0x300000);
  sa.init_add_free(0x3b100000,0x300000);
  sa.init_add_free(0x3cf00000,0x300000);
  sa.init_add_free(0x3ea00000,0x300000);
  sa.init_add_free(0x40700000,0x300000);
  sa.init_add_free(0x42400000,0x300000);
  sa.init_add_free(0x44500000,0x300000);
  sa.init_add_free(0x46200000,0x300000);
  sa.init_add_free(0x48100000,0x300000);
  sa.init_add_free(0x4a000000,0x300000);
  sa.init_add_free(0x4b800000,0x300000);
  sa.init_add_free(0x4ca00000,0x300000);
  sa.init_add_free(0x4d600000,0x300000);
  sa.init_add_free(0x4ed00000,0x300000);
  sa.init_add_free(0x50e00000,0x300000);
  sa.init_add_free(0x52c00000,0x300000);
  sa.init_add_free(0x54800000,0x300000);
  sa.init_add_free(0x56500000,0x300000);
  sa.init_add_free(0x58500000,0x300000);
  sa.init_add_free(0x5a400000,0x300000);
  sa.init_add_free(0x5c700000,0x300000);
  sa.init_add_free(0x5e900000,0x300000);
  sa.init_add_free(0x5ff00000,0x300000);
  sa.init_add_free(0x61b00000,0x300000);
  sa.init_add_free(0x63d00000,0x300000);
  sa.init_add_free(0x65b00000,0x300000);
  sa.init_add_free(0x67c00000,0x300000);
  sa.init_add_free(0x6a200000,0x300000);
  sa.init_add_free(0x6c100000,0x300000);
  sa.init_add_free(0x6e000000,0x300000);
  sa.init_add_free(0x6ff00000,0x300000);
  sa.init_add_free(0x71e00000,0x300000);
  sa.init_add_free(0x73a00000,0x300000);
  sa.init_add_free(0x75700000,0x300000);
  sa.init_add_free(0x77500000,0x300000);
  sa.init_add_free(0x79300000,0x300000);
  sa.init_add_free(0x7b800000,0x300000);
  sa.init_add_free(0x7cd00000,0x300000);
  sa.init_add_free(0x7df00000,0x300000);
  sa.init_add_free(0x7e600000,0x300000);
  sa.init_add_free(0x7fe00000,0x300000);
  sa.init_add_free(0x81800000,0x300000);
  sa.init_add_free(0x83b00000,0x300000);
  sa.init_add_free(0x85c00000,0x300000);
  sa.init_add_free(0x87d00000,0x300000);
  sa.init_add_free(0x89e00000,0x300000);
  sa.init_add_free(0x8bd00000,0x300000);
  sa.init_add_free(0x8e200000,0x300000);
  sa.init_add_free(0x8fe00000,0x300000);
  sa.init_add_free(0x91d00000,0x300000);
  sa.init_add_free(0x93e00000,0x300000);
  sa.init_add_free(0x95900000,0x300000);
  sa.init_add_free(0x96c00000,0x300000);
  sa.init_add_free(0x97100000,0x300000);
  sa.init_add_free(0x98700000,0x300000);
  sa.init_add_free(0x9a900000,0x300000);
  sa.init_add_free(0x9c400000,0x300000);
  sa.init_add_free(0x9eb00000,0x300000);
  sa.init_add_free(0xa0800000,0x300000);
  sa.init_add_free(0xa2900000,0x300000);
  sa.init_add_free(0xa5100000,0x300000);
  sa.init_add_free(0xa7600000,0x300000);
  sa.init_add_free(0xa9900000,0x300000);
  sa.init_add_free(0xab900000,0x300000);
  sa.init_add_free(0xade00000,0x300000);
  sa.init_add_free(0xaf200000,0x300000);
  sa.init_add_free(0xb0c00000,0x300000);
  sa.init_add_free(0xb2c00000,0x300000);
  sa.init_add_free(0xb4b00000,0x300000);
  sa.init_add_free(0xb6d00000,0x300000);
  sa.init_add_free(0xb8f00000,0x300000);
  sa.init_add_free(0xbaf00000,0x300000);
  sa.init_add_free(0xbcf00000,0x300000);
  sa.init_add_free(0xbeb00000,0x300000);
  sa.init_add_free(0xc0300000,0x300000);
  sa.init_add_free(0xc2100000,0x300000);
  sa.init_add_free(0xc4200000,0x300000);
  sa.init_add_free(0xc5e00000,0x300000);
  sa.init_add_free(0xc6500000,0x300000);
  sa.init_add_free(0xc7e00000,0x300000);
  sa.init_add_free(0xc9800000,0x300000);
  sa.init_add_free(0xcb800000,0x300000);
  sa.init_add_free(0xcd900000,0x300000);
  sa.init_add_free(0xcf400000,0x300000);
  sa.init_add_free(0xd0f00000,0x300000);
  sa.init_add_free(0xd2c00000,0x300000);
  sa.init_add_free(0xd4b00000,0x300000);
  sa.init_add_free(0xd6700000,0x300000);
  sa.init_add_free(0xd7e00000,0x300000);
  sa.init_add_free(0xda800000,0x300000);
  sa.init_add_free(0xdc500000,0x300000);
  sa.init_add_free(0xde200000,0x300000);
  sa.init_add_free(0xdf300000,0x300000);
  sa.init_add_free(0xe0300000,0x300000);
  sa.init_add_free(0xe0c00000,0x300000);
  sa.init_add_free(0xe2500000,0x300000);
  sa.init_add_free(0xe4100000,0x300000);
  sa.init_add_free(0xe6400000,0x300000);
  sa.init_add_free(0xe8200000,0x300000);
  sa.init_add_free(0xeae00000,0x300000);
  sa.init_add_free(0xed800000,0x300000);
  sa.init_add_free(0xefa00000,0x300000);
  sa.init_add_free(0xf1600000,0x300000);
  sa.init_add_free(0xf3200000,0x300000);
  sa.init_add_free(0xf4e00000,0x300000);
  sa.init_add_free(0xf6c00000,0x300000);
  sa.init_add_free(0xf7e00000,0x300000);
  sa.init_add_free(0xf9100000,0x300000);
  sa.init_add_free(0xfae00000,0x300000);
  sa.init_add_free(0xfca00000,0x300000);
  sa.init_add_free(0xfeb00000,0x300000);
  sa.init_add_free(0x100d00000,0x300000);
  sa.init_add_free(0x103000000,0x300000);
  sa.init_add_free(0x105000000,0x300000);
  sa.init_add_free(0x106b00000,0x300000);
  sa.init_add_free(0x108b00000,0x300000);
  sa.init_add_free(0x10a800000,0x300000);
  sa.init_add_free(0x10c800000,0x300000);
  sa.init_add_free(0x10e700000,0x300000);
  sa.init_add_free(0x10ff00000,0x300000);
  sa.init_add_free(0x111300000,0x300000);
  sa.init_add_free(0x111b00000,0x300000);
  sa.init_add_free(0x113500000,0x300000);
  sa.init_add_free(0x115200000,0x300000);
  sa.init_add_free(0x117900000,0x300000);
  sa.init_add_free(0x119e00000,0x300000);
  sa.init_add_free(0x11c000000,0x300000);
  sa.init_add_free(0x11db00000,0x300000);
  sa.init_add_free(0x11e600000,0x300000);
  sa.init_add_free(0x11fd00000,0x300000);
  sa.init_add_free(0x121f00000,0x300000);
  sa.init_add_free(0x123d00000,0x300000);
  sa.init_add_free(0x125e00000,0x300000);
  sa.init_add_free(0x127f00000,0x300000);
  sa.init_add_free(0x12a200000,0x300000);
  sa.init_add_free(0x12ba00000,0x300000);
  sa.init_add_free(0x12d700000,0x300000);
  sa.init_add_free(0x12f400000,0x300000);
  sa.init_add_free(0x130f00000,0x300000);
  sa.init_add_free(0x132d00000,0x300000);
  sa.init_add_free(0x135200000,0x300000);
  sa.init_add_free(0x137200000,0x300000);
  sa.init_add_free(0x139300000,0x300000);
  sa.init_add_free(0x13b300000,0x300000);
  sa.init_add_free(0x13d300000,0x300000);
  sa.init_add_free(0x13f400000,0x300000);
  sa.init_add_free(0x140f00000,0x300000);
  sa.init_add_free(0x142100000,0x300000);
  sa.init_add_free(0x142c00000,0x300000);
  sa.init_add_free(0x144b00000,0x300000);
  sa.init_add_free(0x146c00000,0x300000);
  sa.init_add_free(0x148e00000,0x300000);
  sa.init_add_free(0x14b400000,0x300000);
  sa.init_add_free(0x14d700000,0x300000);
  sa.init_add_free(0x14f800000,0x300000);
  sa.init_add_free(0x151800000,0x300000);
  sa.init_add_free(0x153700000,0x300000);
  sa.init_add_free(0x155b00000,0x300000);
  sa.init_add_free(0x157e00000,0x300000);
  sa.init_add_free(0x159700000,0x300000);
  sa.init_add_free(0x15a800000,0x300000);
  sa.init_add_free(0x15b200000,0x300000);
  sa.init_add_free(0x15ca00000,0x300000);
  sa.init_add_free(0x15eb00000,0x300000);
  sa.init_add_free(0x160e00000,0x300000);
  sa.init_add_free(0x163000000,0x300000);
  sa.init_add_free(0x165500000,0x300000);
  sa.init_add_free(0x167300000,0x300000);
  sa.init_add_free(0x168f00000,0x300000);
  sa.init_add_free(0x16ac00000,0x300000);
  sa.init_add_free(0x16ce00000,0x300000);
  sa.init_add_free(0x16ee00000,0x300000);
  sa.init_add_free(0x171000000,0x300000);
  sa.init_add_free(0x172300000,0x300000);
  sa.init_add_free(0x173400000,0x300000);
  sa.init_add_free(0x173c00000,0x300000);
  sa.init_add_free(0x175400000,0x300000);
  sa.init_add_free(0x177400000,0x300000);
  sa.init_add_free(0x179600000,0x300000);
  sa.init_add_free(0x17b400000,0x300000);
  sa.init_add_free(0x17cb00000,0x300000);
  sa.init_add_free(0x17e300000,0x300000);
  sa.init_add_free(0x180200000,0x300000);
  sa.init_add_free(0x181e00000,0x300000);
  sa.init_add_free(0x183a00000,0x300000);
  sa.init_add_free(0x185800000,0x300000);
  sa.init_add_free(0x187900000,0x300000);
  sa.init_add_free(0x189300000,0x300000);
  sa.init_add_free(0x18b200000,0x300000);
  sa.init_add_free(0x18c800000,0x300000);
  sa.init_add_free(0x18dc00000,0x300000);
  sa.init_add_free(0x18fd00000,0x300000);
  sa.init_add_free(0x191700000,0x300000);
  sa.init_add_free(0x193000000,0x300000);
  sa.init_add_free(0x194f00000,0x300000);
  sa.init_add_free(0x196f00000,0x300000);
  sa.init_add_free(0x199300000,0x300000);
  sa.init_add_free(0x19b100000,0x300000);
  sa.init_add_free(0x19cf00000,0x300000);
  sa.init_add_free(0x19ee00000,0x300000);
  sa.init_add_free(0x1a0d00000,0x300000);
  sa.init_add_free(0x1a2a00000,0x300000);
  sa.init_add_free(0x1a3900000,0x300000);
  sa.init_add_free(0x1a4a00000,0x300000);
  sa.init_add_free(0x1a5600000,0x300000);
  sa.init_add_free(0x1a7100000,0x300000);
  sa.init_add_free(0x1a9200000,0x300000);
  sa.init_add_free(0x1aad00000,0x300000);
  sa.init_add_free(0x1acb00000,0x300000);
  sa.init_add_free(0x1ae600000,0x300000);
  sa.init_add_free(0x1b0200000,0x300000);
  sa.init_add_free(0x1b3100000,0x300000);
  sa.init_add_free(0x1b5300000,0x300000);
  sa.init_add_free(0x1b7000000,0x300000);
  sa.init_add_free(0x1b8f00000,0x300000);
  sa.init_add_free(0x1bb000000,0x300000);
  sa.init_add_free(0x1bc400000,0x300000);
  sa.init_add_free(0x1bd800000,0x300000);
  sa.init_add_free(0x1bdf00000,0x300000);
  sa.init_add_free(0x1bf600000,0x300000);
  sa.init_add_free(0x1c1400000,0x300000);
  sa.init_add_free(0x1c2f00000,0x300000);
  sa.init_add_free(0x1c5900000,0x300000);
  sa.init_add_free(0x1c8100000,0x300000);
  sa.init_add_free(0x1ca400000,0x300000);
  sa.init_add_free(0x1cc300000,0x300000);
  sa.init_add_free(0x1cf200000,0x300000);
  sa.init_add_free(0x1d1500000,0x300000);
  sa.init_add_free(0x1d2f00000,0x300000);
  sa.init_add_free(0x1d4900000,0x300000);
  sa.init_add_free(0x1d5d00000,0x300000);
  sa.init_add_free(0x1d6500000,0x300000);
  sa.init_add_free(0x1d7e00000,0x300000);
  sa.init_add_free(0x1d9800000,0x300000);
  sa.init_add_free(0x1dbc00000,0x300000);
  sa.init_add_free(0x1ddb00000,0x300000);
  sa.init_add_free(0x1de300000,0x300000);
  sa.init_add_free(0x1dfa00000,0x300000);
  sa.init_add_free(0x1e3500000,0x300000);
  sa.init_add_free(0x1e5500000,0x300000);
  sa.init_add_free(0x1e7900000,0x300000);
  sa.init_add_free(0x1e9600000,0x300000);
  sa.init_add_free(0x1ebb00000,0x300000);
  sa.init_add_free(0x1edb00000,0x300000);
  sa.init_add_free(0x1ef000000,0x300000);
  sa.init_add_free(0x1f0d00000,0x300000);
  sa.init_add_free(0x1f3000000,0x300000);
  sa.init_add_free(0x1f5e00000,0x300000);
  sa.init_add_free(0x1f8900000,0x300000);
  sa.init_add_free(0x1fab00000,0x300000);
  sa.init_add_free(0x1fcf00000,0x300000);
  sa.init_add_free(0x214500000,0x300000);
  sa.init_add_free(0x216200000,0x300000);
  sa.init_add_free(0x218200000,0x300000);
  sa.init_add_free(0x21ac00000,0x300000);
  sa.init_add_free(0x21d000000,0x300000);
  sa.init_add_free(0x21e900000,0x300000);
  sa.init_add_free(0x21f800000,0x300000);
  sa.init_add_free(0x221000000,0x300000);
  sa.init_add_free(0x223200000,0x300000);
  sa.init_add_free(0x225100000,0x300000);
  sa.init_add_free(0x227800000,0x300000);
  sa.init_add_free(0x22a900000,0x300000);
  sa.init_add_free(0x22ca00000,0x300000);
  sa.init_add_free(0x22f000000,0x300000);
  sa.init_add_free(0x231000000,0x300000);
  sa.init_add_free(0x233a00000,0x300000);
  sa.init_add_free(0x235000000,0x300000);
  sa.init_add_free(0x236500000,0x300000);
  sa.init_add_free(0x238200000,0x300000);
  sa.init_add_free(0x239d00000,0x300000);
  sa.init_add_free(0x23bd00000,0x300000);
  sa.init_add_free(0x23df00000,0x300000);
  sa.init_add_free(0x23f800000,0x300000);
  sa.init_add_free(0x241500000,0x300000);
  sa.init_add_free(0x242f00000,0x300000);
  sa.init_add_free(0x244b00000,0x300000);
  sa.init_add_free(0x246800000,0x300000);
  sa.init_add_free(0x248400000,0x300000);
  sa.init_add_free(0x24a100000,0x300000);
  sa.init_add_free(0x24bd00000,0x300000);
  sa.init_add_free(0x24da00000,0x300000);
  sa.init_add_free(0x24f600000,0x300000);
  sa.init_add_free(0x250a00000,0x300000);

  sa.init_add_free(0x210900000,0x100000);
  sa.init_add_free(0x212900000,0x100000);
  sa.init_add_free(0x214400000,0x100000);
  sa.init_add_free(0x216100000,0x100000);
  sa.init_add_free(0x218100000,0x100000);
  sa.init_add_free(0x21ab00000,0x100000);
  sa.init_add_free(0x21cf00000,0x100000);
  sa.init_add_free(0x21e800000,0x100000);
  sa.init_add_free(0x21f700000,0x100000);
  sa.init_add_free(0x220f00000,0x100000);
  sa.init_add_free(0x11700000,0x100000);
  sa.init_add_free(0x223100000,0x100000);
  sa.init_add_free(0x225000000,0x100000);
  sa.init_add_free(0x227700000,0x100000);
  sa.init_add_free(0x22a800000,0x100000);
  sa.init_add_free(0x22c900000,0x100000);
  sa.init_add_free(0x22ef00000,0x100000);
  sa.init_add_free(0x230f00000,0x100000);
  sa.init_add_free(0x233900000,0x100000);
  sa.init_add_free(0x234f00000,0x100000);
  sa.init_add_free(0x236400000,0x100000);
  sa.init_add_free(0x238100000,0x100000);
  sa.init_add_free(0x239c00000,0x100000);
  sa.init_add_free(0x23bc00000,0x100000);
  sa.init_add_free(0x23c700000,0x400000);
  sa.init_add_free(0x23de00000,0x100000);
  sa.init_add_free(0x23f700000,0x100000);
  sa.init_add_free(0x241400000,0x100000);
  sa.init_add_free(0x242e00000,0x100000);
  sa.init_add_free(0x244a00000,0x100000);
  sa.init_add_free(0x246700000,0x100000);
  sa.init_add_free(0x248300000,0x100000);
  sa.init_add_free(0x24a000000,0x100000);
  sa.init_add_free(0x24bc00000,0x100000);
  sa.init_add_free(0x24d900000,0x100000);
  sa.init_add_free(0x24f500000,0x100000);
  sa.init_add_free(0x250900000,0x100000);
  sa.init_add_free(0x252300000,0x100000);
  sa.init_add_free(0x252400000,0x100000);
  sa.init_add_free(0x252500000,0x100000);
  sa.init_add_free(0x252600000,0x100000);
  sa.init_add_free(0x100000,0x100000);
  sa.init_add_free(0x1de00000,0x100000);
  sa.init_add_free(0x46100000,0x100000);
  sa.init_add_free(0x73900000,0x100000);
  sa.init_add_free(0xa2800000,0x100000);
  sa.init_add_free(0xdf200000,0x100000);
  sa.init_add_free(0x113400000,0x100000);
  sa.init_add_free(0x144a00000,0x100000);
  sa.init_add_free(0x183900000,0x100000);
  sa.init_add_free(0x1b5200000,0x100000);
  sa.init_add_free(0x1e1600000,0x400000);
  sa.init_add_free(0x1fef00000,0x100000);
  sa.init_add_free(0x1ff000000,0x100000);
  sa.init_add_free(0x1ff100000,0x100000);
  sa.init_add_free(0x1ff200000,0x100000);
  sa.init_add_free(0x201700000,0x100000);
  sa.init_add_free(0x201800000,0x100000);
  sa.init_add_free(0x201900000,0x100000);
  sa.init_add_free(0x203600000,0x100000);
  sa.init_add_free(0x203700000,0x100000);
  sa.init_add_free(0x203800000,0x100000);
  sa.init_add_free(0x205000000,0x100000);
  sa.init_add_free(0x205100000,0x100000);
  sa.init_add_free(0x205200000,0x100000);
  sa.init_add_free(0x206500000,0x100000);
  sa.init_add_free(0x206600000,0x100000);
  sa.init_add_free(0x206700000,0x100000);
  sa.init_add_free(0x207200000,0x100000);
  sa.init_add_free(0x207300000,0x100000);
  sa.init_add_free(0x207400000,0x100000);
  sa.init_add_free(0x208900000,0x100000);
  sa.init_add_free(0x208a00000,0x100000);
  sa.init_add_free(0x208b00000,0x100000);
  sa.init_add_free(0x20b100000,0x100000);
  sa.init_add_free(0x20b200000,0x100000);
  sa.init_add_free(0x20b300000,0x100000);
  sa.init_add_free(0x20cc00000,0x100000);
  sa.init_add_free(0x20cd00000,0x100000);
  sa.init_add_free(0x20ce00000,0x100000);
  sa.init_add_free(0x210a00000,0x100000);
  sa.init_add_free(0x210b00000,0x100000);
  sa.init_add_free(0x210c00000,0x100000);
  sa.init_add_free(0x212a00000,0x100000);
  sa.init_add_free(0x212b00000,0x100000);
  sa.init_add_free(0x212c00000,0x100000);

  uint64_t offset;
  uint32_t allocated;
  sa.reserve(0x100000);
  ASSERT_EQ(sa.allocate_int(0x100000, 4096, 0x1de00000, &offset, &allocated), 0);
  ASSERT_EQ(allocated, 0x100000);
  sa.reserve(0x100000);
  ASSERT_EQ(sa.allocate_int(0x100000, 4096, offset+allocated, &offset, &allocated), 0);
  ASSERT_EQ(allocated, 0x100000);
  ASSERT_EQ(offset, 0x1df00000);
  sa.reserve(0x400000);
  ASSERT_EQ(sa.allocate_int(0x400000, 4096, 0, &offset, &allocated), 0);
  ASSERT_EQ(allocated, 0x400000);

}

TYPED_TEST(AllocatorTest, test_add_free_rm_free_Fibonnaci_CantorSet) {
  //uses CantorSet idea, but:
  //1) in step lower half of regions is retained, higher is deleted
  //2) in step I, after deletion, inserts back intervals that were deleted in iteration I-1
  typedef std::vector<uint64_t> interval_left;
  typedef std::map<uint64_t, interval_left> aset_t;

  auto nextI = [](const aset_t& I, const aset_t& I1deleted, aset_t& Inext, aset_t& deleted){
    Inext.clear();
    deleted.clear();

    //1. move upper halves to deleted regions
    for (auto &ww: I) {
      uint64_t width = ww.first;
      auto elems = ww.second;
      size_t count = elems.size();
      auto &d = deleted[width/2];
      d.resize(count);
      for (size_t i=0; i<count; i++) {
        d[i] = elems[i] + width/2;
      }
    }
    //2. move lower half of regions to Inext
    for (auto &ww: I) {
      uint64_t width = ww.first;
      Inext[width/2] = ww.second;
    }
    //3. append previously deleted to Inext
    for (auto &dd: I1deleted) {
      uint64_t width = dd.first;
      Inext[width].insert(Inext[width].end(), dd.second.begin(), dd.second.end());
    }
  };
/*
  auto print = [](const std::string& name, const aset_t& S) {
    for (auto &s: S) {
      std::cout << name << s.first << "["; //std::endl;
      for (auto &i: s.second) {
        std::cout << i << ",";
      }
      std::cout << "]" << std::endl;
    }
  };
*/
  auto size = [](const aset_t& S) -> uint64_t {
    size_t sum=0;
    for (auto &s: S) {
      sum+=s.first * s.second.size();
    }
    return sum;
  };

  typename TestFixture::Allocator sa(g_ceph_context);
  constexpr size_t M = 1<<24;
  aset_t set;

  set[M] = {0};
  aset_t deleted;
  aset_t new_set,new_deleted;
  sa.init_add_free(0, M);
  ASSERT_EQ(sa.get_free(), size(set));

  for (int i=0;i<10;i++) {
    nextI(set,deleted,new_set,new_deleted);
    for(auto &d: deleted) {
      //these are previosly deleted - are now added
      for(auto &dr: d.second) {
        sa.init_add_free(dr, d.first);
      }
    }
    for(auto &d: new_deleted) {
      //really deleted
      for(auto &dr: d.second) {
        sa.init_rm_free(dr, d.first);
      }
    }

    set.swap(new_set);
    deleted.swap(new_deleted);
    ASSERT_EQ(sa.get_free(), size(set));
  }
}


TYPED_TEST(AllocatorTest, test_fragmentation) {
  typename TestFixture::Allocator sa(g_ceph_context);
  constexpr size_t M = 1<<20;
  constexpr size_t P = 4096;
  sa.init_add_free(0, M);

  for (int i=0; i<100;i++) {
    uint64_t offset;
    uint32_t v = rand() % (M / P);
    uint32_t scale = cbits(M / P) - cbits(v);
    uint64_t size = ( (rand() % (1 << scale)) + 1 ) * P;
    if (0 == sa.reserve(size)) {
      uint32_t allocated = 0;
      if (0 == sa.allocate_int(size, P, 0, &offset, &allocated)) {
        interval_set<uint64_t> tr;
        tr.insert(offset, allocated);
        sa.release(tr);
      }
      sa.unreserve(size - allocated);
    }
    ASSERT_EQ(sa.get_free(), M);
  }
  sa.reserve(M);
  if (std::is_same<typename TestFixture::Allocator, StupidAllocator>::value) {
    std::cerr << "[ SKIPPING REST ] --would fail--" << std::endl;
    return;
  }
  uint64_t offset;
  uint32_t allocated;
  ASSERT_EQ(0, sa.allocate_int(M, P, 0, &offset, &allocated));
  ASSERT_EQ(allocated, M);
}



TYPED_TEST(AllocatorTest, test_fragmentation_dragged) {
  typename TestFixture::Allocator sa(g_ceph_context);
  constexpr size_t M = 1<<26;
  constexpr size_t P = 4096;
  sa.init_add_free(0, M);
  typedef std::pair<uint64_t, uint32_t> allocation_t;
  typedef std::list<allocation_t> allocation_list_t;
  allocation_list_t allocation_list;
  std::vector<allocation_list_t::iterator> allocation_vector;

  for (int i=0; i<10000;i++) {
    uint64_t offset;
    uint32_t v = rand() % (M / P);
    uint32_t scale = cbits(M / P) - cbits(v);
    uint64_t size = ( (rand() % (1 << scale)) + 1 ) * P;
    if (0 == sa.reserve(size)) {
      uint32_t allocated = 0;
      if (0 == sa.allocate_int(size, P, 1, &offset, &allocated)) {
        auto n = allocation_list.emplace(allocation_list.end(), allocation_t{offset,allocated});
        allocation_vector.push_back(n);
      }
      sa.unreserve(size - allocated);
      if (allocation_vector.size() > 100) {
        size_t r = rand()%allocation_vector.size();
        auto it = allocation_vector[r];
        interval_set<uint64_t> tr;
        tr.insert(it->first, it->second);
        sa.release(tr);
        allocation_vector[r] = allocation_vector.back();
        allocation_vector.resize(allocation_vector.size() - 1);
      }
    }
  }
  interval_set<uint64_t> tr;
  for (size_t i = 0; i < allocation_vector.size(); i++) {
    auto it = allocation_vector[i];
    tr.insert(it->first, it->second);
  }
  sa.release(tr);

  ASSERT_EQ(sa.get_free(), M);

  sa.reserve(M);
  if (std::is_same<typename TestFixture::Allocator, StupidAllocator>::value) {
      std::cerr << "[ SKIPPING REST ] --would fail--" << std::endl;
      return;
  }
  uint64_t offset;
  uint32_t allocated;
  ASSERT_EQ(0, sa.allocate_int(M, P, 0, &offset, &allocated));
  ASSERT_EQ(allocated, M);
}

TYPED_TEST(AllocatorTest, performance_test) {
  typename TestFixture::Allocator sa(g_ceph_context);
  srand(1);
  constexpr size_t CAPACITY = 1<<30;
  constexpr size_t MAXALLOC = 1<<26;
  constexpr size_t PAGE = 4096;
  sa.init_add_free(0, CAPACITY);
  typedef std::pair<uint64_t, uint32_t> allocation_t;
  typedef std::list<allocation_t> allocation_list_t;
  allocation_list_t allocation_list;
  std::vector<allocation_list_t::iterator> allocation_vector;

  auto want_size = [CAPACITY](int i) -> uint64_t {
    //graph sin (t+12) + sin (1.1(t+13)) + sin(1.2(t+12)) + sin(1.3(t+13)) + t/8, t=0 to 80
    //graph sin(t/80*pi)*(sin (t)*2 + sin(t*3)*2) + sin(t/80*pi)*10, t=0 to 80
    double t = i / 1000. * 80;
    double r =
        sin (t/80 * M_PI) *
        (sin (t) * 2 + sin (t*3) * 2 + 10);
    return r / 14 * CAPACITY;
  };
  uint64_t allocation_count = 0;

  for (int k=0; k<10; k++)
  for (int i=0; i<1000;i++) {
    uint64_t wsize = want_size(i);
    uint64_t offset;
    uint32_t v = rand() % (MAXALLOC / PAGE);
    uint32_t scale = cbits(MAXALLOC / PAGE) - cbits(v);
    uint64_t size = ( (rand() % (1 << scale)) + 1 ) * PAGE;

    bool more = true;
    while ((CAPACITY - sa.get_free()) < wsize && more) {
      if (0 == sa.reserve(size)) {
        uint32_t allocated = 0;
        if (0 == sa.allocate_int(size, PAGE, 1, &offset, &allocated)) {
          auto n = allocation_list.emplace(allocation_list.end(), allocation_t{offset,allocated});
          allocation_vector.push_back(n);
          allocation_count++;
        } else {
          more = false;
        }
        sa.unreserve(size - allocated);
      }
    }
    while ((CAPACITY - sa.get_free()) > wsize) {
      size_t r = rand()%allocation_vector.size();
      auto it = allocation_vector[r];
      interval_set<uint64_t> tr;
      tr.insert(it->first, it->second);
      sa.release(tr);
      allocation_vector[r] = allocation_vector.back();
      allocation_vector.resize(allocation_vector.size() - 1);
    }

  }
  interval_set<uint64_t> tr;
  for (size_t i = 0; i < allocation_vector.size(); i++) {
    auto it = allocation_vector[i];
    tr.insert(it->first, it->second);
  }
  sa.release(tr);
  ASSERT_EQ(sa.get_free(), CAPACITY);

  if (std::is_same<typename TestFixture::Allocator, StupidAllocator>::value) {
      std::cerr << "[ SKIPPING REST ] --would fail--" << std::endl;
      return;
  }
  uint64_t offset;
  uint32_t allocated;
  sa.reserve(CAPACITY);
  ASSERT_EQ(0, sa.allocate_int(CAPACITY, PAGE, 0, &offset, &allocated));
  ASSERT_EQ(allocated, CAPACITY);
}

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  map<string,string> defaults = {
    { "debug_bluefs", "1/20" },
    //{ "debug_bluestore", "30/30" },
    { "debug_bdev", "1/20" }
  };

  auto cct = global_init(&defaults, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->set_val(
    "enable_experimental_unrecoverable_data_corrupting_features",
    "*");
  g_ceph_context->_conf->apply_changes(NULL);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
