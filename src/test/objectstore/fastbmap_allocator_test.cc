// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>
#include <gtest/gtest.h>

#include "os/bluestore/fastbmap_allocator_impl.h"

class TestAllocatorLevel01 : public AllocatorLevel01Loose
{
public:
  void init(uint64_t capacity, uint64_t alloc_unit)
  {
    _init(capacity, alloc_unit);
  }
  interval_t allocate_l1_cont(uint64_t length, uint64_t min_length,
    uint64_t pos_start, uint64_t pos_end)
  {
    return _allocate_l1_contiguous(length, min_length, 0, pos_start, pos_end);
  }
  void free_l1(const interval_t& r)
  {
    _free_l1(r.offset, r.length);
  }
};

class TestAllocatorLevel02 : public AllocatorLevel02<AllocatorLevel01Loose>
{
public:
  void init(uint64_t capacity, uint64_t alloc_unit)
  {
    _init(capacity, alloc_unit);
  }
  void allocate_l2(uint64_t length, uint64_t min_length,
    uint64_t* allocated0,
    interval_vector_t* res)
  {
    uint64_t allocated = 0;
    uint64_t hint = 0; // trigger internal l2 hint support
    _allocate_l2(length, min_length, 0, hint, &allocated, res);
    *allocated0 += allocated;
  }
  void free_l2(const interval_vector_t& r)
  {
    _free_l2(r);
  }
};

const uint64_t _1m = 1024 * 1024;
const uint64_t _2m = 2 * 1024 * 1024;

TEST(TestAllocatorLevel01, test_l1)
{
  TestAllocatorLevel01 al1;
  uint64_t num_l1_entries = 3 * 256;
  uint64_t capacity = num_l1_entries * 512 * 4096;
  al1.init(capacity, 0x1000);
  ASSERT_EQ(capacity, al1.debug_get_free());

  auto i1 = al1.allocate_l1_cont(0x1000, 0x1000, 0, num_l1_entries);
  ASSERT_EQ(i1.offset, 0u);
  ASSERT_EQ(i1.length, 0x1000u);
  ASSERT_EQ(capacity - 0x1000, al1.debug_get_free());

  auto i2 = al1.allocate_l1_cont(0x1000, 0x1000, 0, num_l1_entries);
  ASSERT_EQ(i2.offset, 0x1000u);
  ASSERT_EQ(i2.length, 0x1000u);
  al1.free_l1(i2);
  al1.free_l1(i1);
  i1 = al1.allocate_l1_cont(0x1000, 0x1000, 0, num_l1_entries);
  ASSERT_EQ(i1.offset, 0u);
  ASSERT_EQ(i1.length, 0x1000u);
  i2 = al1.allocate_l1_cont(0x1000, 0x1000, 0, num_l1_entries);
  ASSERT_EQ(i2.offset, 0x1000u);
  ASSERT_EQ(i2.length, 0x1000u);
  al1.free_l1(i1);
  al1.free_l1(i2);

  i1 = al1.allocate_l1_cont(0x2000, 0x1000, 0, num_l1_entries);
  ASSERT_EQ(i1.offset, 0u);
  ASSERT_EQ(i1.length, 0x2000u);

  i2 = al1.allocate_l1_cont(0x3000, 0x1000, 0, num_l1_entries);
  ASSERT_EQ(i2.offset, 0x2000u);
  ASSERT_EQ(i2.length, 0x3000u);

  al1.free_l1(i1);
  al1.free_l1(i2);

  i1 = al1.allocate_l1_cont(0x2000, 0x1000, 0, num_l1_entries);
  ASSERT_EQ(i1.offset, 0u);
  ASSERT_EQ(i1.length, 0x2000u);

  i2 = al1.allocate_l1_cont(2 * 1024 * 1024, 0x1000, 0, num_l1_entries);
  ASSERT_EQ(i2.offset, 2u * 1024u * 1024u);
  ASSERT_EQ(i2.length, 2u * 1024u * 1024u);

  al1.free_l1(i1);
  i1 = al1.allocate_l1_cont(1024 * 1024, 0x1000, 0, num_l1_entries);
  ASSERT_EQ(i1.offset, 0u);
  ASSERT_EQ(i1.length, 1024u * 1024u);

  auto i3 = al1.allocate_l1_cont(1024 * 1024 + 0x1000, 0x1000, 0, num_l1_entries);
  ASSERT_EQ(i3.offset, 2u * 2u * 1024u * 1024u);
  ASSERT_EQ(i3.length, 1024u * 1024u + 0x1000u);

  // here we have the following layout:
  // Alloc: 0~1M, 2M~2M, 4M~1M+4K
  // Free: 1M~1M, 4M+4K ~ 2M-4K, 6M ~...
  //
  auto i4 = al1.allocate_l1_cont(1024 * 1024, 0x1000, 0, num_l1_entries);
  ASSERT_EQ(1 * 1024 * 1024u, i4.offset);
  ASSERT_EQ(1024 * 1024u, i4.length);
  al1.free_l1(i4);

  i4 = al1.allocate_l1_cont(1024 * 1024 - 0x1000, 0x1000, 0, num_l1_entries);
  ASSERT_EQ(i4.offset, 5u * 1024u * 1024u + 0x1000u);
  ASSERT_EQ(i4.length, 1024u * 1024u - 0x1000u);
  al1.free_l1(i4);

  i4 = al1.allocate_l1_cont(1024 * 1024 + 0x1000, 0x1000, 0, num_l1_entries);
  ASSERT_EQ(i4.offset, 6u * 1024u * 1024u);
  //ASSERT_EQ(i4.offset, 5 * 1024 * 1024 + 0x1000);
  ASSERT_EQ(i4.length, 1024u * 1024u + 0x1000u);

  al1.free_l1(i1);
  al1.free_l1(i2);
  al1.free_l1(i3);
  al1.free_l1(i4);

  i1 = al1.allocate_l1_cont(1024 * 1024, 0x1000, 0, num_l1_entries);
  ASSERT_EQ(i1.offset, 0u);
  ASSERT_EQ(i1.length, 1024u * 1024u);

  i2 = al1.allocate_l1_cont(1024 * 1024, 0x1000, 0, num_l1_entries);
  ASSERT_EQ(i2.offset, 1u * 1024u * 1024u);
  ASSERT_EQ(i2.length, 1024u * 1024u);

  i3 = al1.allocate_l1_cont(512 * 1024, 0x1000, 0, num_l1_entries);
  ASSERT_EQ(i3.offset, 2u * 1024u * 1024u);
  ASSERT_EQ(i3.length, 512u * 1024u);

  i4 = al1.allocate_l1_cont(1536 * 1024, 0x1000, 0, num_l1_entries);
  ASSERT_EQ(i4.offset, (2u * 1024u + 512u) * 1024u);
  ASSERT_EQ(i4.length, 1536u * 1024u);
  // making a hole 1.5 Mb length
  al1.free_l1(i2);
  al1.free_l1(i3);
  // and trying to fill it
  i2 = al1.allocate_l1_cont(1536 * 1024, 0x1000, 0, num_l1_entries);
  ASSERT_EQ(i2.offset, 1024u * 1024u);
  ASSERT_EQ(i2.length, 1536u * 1024u);

  al1.free_l1(i2);
  // and trying to fill it partially
  i2 = al1.allocate_l1_cont(1528 * 1024, 0x1000, 0, num_l1_entries);
  ASSERT_EQ(i2.offset, 1024u * 1024u);
  ASSERT_EQ(i2.length, 1528u * 1024u);

  i3 = al1.allocate_l1_cont(8 * 1024, 0x1000, 0, num_l1_entries);
  ASSERT_EQ(i3.offset, 2552u * 1024u);
  ASSERT_EQ(i3.length, 8u * 1024u);

  al1.free_l1(i2);
  // here we have the following layout:
  // Alloc: 0~1M, 2552K~8K, num_l1_entries0K~1.5M
  // Free: 1M~1528K, 4M ~...
  //
  i2 = al1.allocate_l1_cont(1536 * 1024, 0x1000, 0, num_l1_entries);
  ASSERT_EQ(i2.offset, 4u * 1024u * 1024u);
  ASSERT_EQ(i2.length, 1536u * 1024u);

  al1.free_l1(i1);
  al1.free_l1(i2);
  al1.free_l1(i3);
  al1.free_l1(i4);
  ASSERT_EQ(capacity, al1.debug_get_free());

  for (uint64_t i = 0; i < capacity; i += _2m) {
    i1 = al1.allocate_l1_cont(_2m, _2m, 0, num_l1_entries);
    ASSERT_EQ(i1.offset, i);
    ASSERT_EQ(i1.length, _2m);
  }
  ASSERT_EQ(0u, al1.debug_get_free());
  i2 = al1.allocate_l1_cont(_2m, _2m, 0, num_l1_entries);
  ASSERT_EQ(i2.length, 0u);
  ASSERT_EQ(0u, al1.debug_get_free());

  al1.free_l1(i1);
  i2 = al1.allocate_l1_cont(_2m, _2m, 0, num_l1_entries);
  ASSERT_EQ(i2, i1);
  al1.free_l1(i2);
  i2 = al1.allocate_l1_cont(_1m, _1m, 0, num_l1_entries);
  ASSERT_EQ(i2.offset, i1.offset);
  ASSERT_EQ(i2.length, _1m);

  i3 = al1.allocate_l1_cont(_2m, _2m, 0, num_l1_entries);
  ASSERT_EQ(i3.length, 0u);

  i3 = al1.allocate_l1_cont(_2m, _1m, 0, num_l1_entries);
  ASSERT_EQ(i3.length, _1m);

  i4 = al1.allocate_l1_cont(_2m, _1m, 0, num_l1_entries);
  ASSERT_EQ(i4.length, 0u);

  al1.free_l1(i2);
  i2 = al1.allocate_l1_cont(_2m, _2m, 0, num_l1_entries);
  ASSERT_EQ(i2.length, 0u);

  i2 = al1.allocate_l1_cont(_2m, 0x1000, 0, num_l1_entries);
  ASSERT_EQ(i2.length, _1m);

  al1.free_l1(i2);
  al1.free_l1(i3);
  ASSERT_EQ(_2m, al1.debug_get_free());

  i1 = al1.allocate_l1_cont(_2m - 3 * 0x1000, 0x1000, 0, num_l1_entries);
  i2 = al1.allocate_l1_cont(0x1000, 0x1000, 0, num_l1_entries);
  i3 = al1.allocate_l1_cont(0x1000, 0x1000, 0, num_l1_entries);
  i4 = al1.allocate_l1_cont(0x1000, 0x1000, 0, num_l1_entries);
  ASSERT_EQ(0u, al1.debug_get_free());

  al1.free_l1(i2);
  al1.free_l1(i4);

  i2 = al1.allocate_l1_cont(0x4000, 0x2000, 0, num_l1_entries);
  ASSERT_EQ(i2.length, 0u);
  i2 = al1.allocate_l1_cont(0x4000, 0x1000, 0, num_l1_entries);
  ASSERT_EQ(i2.length, 0x1000u);

  al1.free_l1(i3);
  i3 = al1.allocate_l1_cont(0x6000, 0x3000, 0, num_l1_entries);
  ASSERT_EQ(i3.length, 0u);
  i3 = al1.allocate_l1_cont(0x6000, 0x1000, 0, num_l1_entries);
  ASSERT_EQ(i3.length, 0x2000u);
  ASSERT_EQ(0u, al1.debug_get_free());

  std::cout << "Done L1" << std::endl;
}

TEST(TestAllocatorLevel01, test_l2)
{
  TestAllocatorLevel02 al2;
  uint64_t num_l2_entries = 64;// *512;
  uint64_t capacity = num_l2_entries * 256 * 512 * 4096;
  al2.init(capacity, 0x1000);
  std::cout << "Init L2" << std::endl;

  uint64_t allocated1 = 0;
  interval_vector_t a1;
  al2.allocate_l2(0x2000, 0x2000, &allocated1, &a1);
  ASSERT_EQ(allocated1, 0x2000u);
  ASSERT_EQ(a1[0].offset, 0u);
  ASSERT_EQ(a1[0].length, 0x2000u);

  // limit query range in debug_get_free for the sake of performance
  ASSERT_EQ(0x2000u, al2.debug_get_allocated(0, 1));
  ASSERT_EQ(0u, al2.debug_get_allocated(1, 2));

  uint64_t allocated2 = 0;
  interval_vector_t a2;
  al2.allocate_l2(0x2000, 0x2000, &allocated2, &a2);
  ASSERT_EQ(allocated2, 0x2000u);
  ASSERT_EQ(a2[0].offset, 0x2000u);
  ASSERT_EQ(a2[0].length, 0x2000u);
  // limit query range in debug_get_free for the sake of performance
  ASSERT_EQ(0x4000u, al2.debug_get_allocated(0, 1));
  ASSERT_EQ(0u, al2.debug_get_allocated(1, 2));

  al2.free_l2(a1);

  allocated2 = 0;
  a2.clear();
  al2.allocate_l2(0x1000, 0x1000, &allocated2, &a2);
  ASSERT_EQ(allocated2, 0x1000u);
  ASSERT_EQ(a2[0].offset, 0x0000u);
  ASSERT_EQ(a2[0].length, 0x1000u);
  // limit query range in debug_get_free for the sake of performance
  ASSERT_EQ(0x3000u, al2.debug_get_allocated(0, 1));
  ASSERT_EQ(0u, al2.debug_get_allocated(1, 2));

  uint64_t allocated3 = 0;
  interval_vector_t a3;
  al2.allocate_l2(0x2000, 0x1000, &allocated3, &a3);
  ASSERT_EQ(allocated3, 0x2000u);
  ASSERT_EQ(a3.size(), 2u);
  ASSERT_EQ(a3[0].offset, 0x1000u);
  ASSERT_EQ(a3[0].length, 0x1000u);
  ASSERT_EQ(a3[1].offset, 0x4000u);
  ASSERT_EQ(a3[1].length, 0x1000u);
  // limit query range in debug_get_free for the sake of performance
  ASSERT_EQ(0x5000u, al2.debug_get_allocated(0, 1));
  ASSERT_EQ(0u, al2.debug_get_allocated(1, 2));
  {
    interval_vector_t r;
    r.emplace_back(0x0, 0x5000);
    al2.free_l2(r);
  }

  a3.clear();
  allocated3 = 0;
  al2.allocate_l2(_1m, _1m, &allocated3, &a3);
  ASSERT_EQ(a3.size(), 1u);
  ASSERT_EQ(a3[0].offset, 0u);
  ASSERT_EQ(a3[0].length, _1m);

  al2.free_l2(a3);

  a3.clear();
  allocated3 = 0;
  al2.allocate_l2(4 * _1m, _1m, &allocated3, &a3);
  ASSERT_EQ(a3.size(), 1u);
  ASSERT_EQ(a3[0].offset, 0u);
  ASSERT_EQ(a3[0].length, 4 * _1m);

  al2.free_l2(a3);

#ifndef _DEBUG
  for (uint64_t i = 0; i < capacity; i += 0x1000) {
    uint64_t allocated4 = 0;
    interval_vector_t a4;
    al2.allocate_l2(0x1000, 0x1000, &allocated4, &a4);
    ASSERT_EQ(a4.size(), 1u);
    ASSERT_EQ(a4[0].offset, i);
    ASSERT_EQ(a4[0].length, 0x1000u);
    if (0 == (i % (1 * 1024 * _1m))) {
      std::cout << "alloc1 " << i / 1024 / 1024 << " mb of "
        << capacity / 1024 / 1024 << std::endl;
    }
  }
#else
  for (uint64_t i = 0; i < capacity; i += _2m) {
    uint64_t allocated4 = 0;
    interval_vector_t a4;
    al2.allocate_l2(_2m, _2m, &allocated4, &a4);
    ASSERT_EQ(a4.size(), 1);
    ASSERT_EQ(a4[0].offset, i);
    ASSERT_EQ(a4[0].length, _2m);
    if (0 == (i % (1 * 1024 * _1m))) {
      std::cout << "alloc1 " << i / 1024 / 1024 << " mb of "
                << capacity / 1024 / 1024 << std::endl;
    }
  }
#endif

  ASSERT_EQ(0u, al2.debug_get_free());
  for (uint64_t i = 0; i < capacity; i += _1m) {
    interval_vector_t r;
    r.emplace_back(i, _1m);
    al2.free_l2(r);
    if (0 == (i % (1 * 1024 * _1m))) {
      std::cout << "free1 " << i / 1024 / 1024 << " mb of "
        << capacity / 1024 / 1024 << std::endl;
    }
  }
  ASSERT_EQ(capacity, al2.debug_get_free());

  for (uint64_t i = 0; i < capacity; i += _1m) {
    uint64_t allocated4 = 0;
    interval_vector_t a4;
    al2.allocate_l2(_1m, _1m, &allocated4, &a4);
    ASSERT_EQ(a4.size(), 1u);
    ASSERT_EQ(allocated4, _1m);
    ASSERT_EQ(a4[0].offset, i);
    ASSERT_EQ(a4[0].length, _1m);
    if (0 == (i % (1 * 1024 * _1m))) {
      std::cout << "alloc2 " << i / 1024 / 1024 << " mb of "
        << capacity / 1024 / 1024 << std::endl;
    }
  }
  ASSERT_EQ(0u, al2.debug_get_free());
  uint64_t allocated4 = 0;
  interval_vector_t a4;
  al2.allocate_l2(_1m, _1m, &allocated4, &a4);
  ASSERT_EQ(a4.size(), 0u);
  al2.allocate_l2(0x1000, 0x1000, &allocated4, &a4);
  ASSERT_EQ(a4.size(), 0u);

  for (uint64_t i = 0; i < capacity; i += 0x2000) {
    interval_vector_t r;
    r.emplace_back(i, 0x1000);
    al2.free_l2(r);
    if (0 == (i % (1 * 1024 * _1m))) {
      std::cout << "free2 " << i / 1024 / 1024 << " mb of "
        << capacity / 1024 / 1024 << std::endl;
    }
  }
  ASSERT_EQ(capacity / 2, al2.debug_get_free());

  // unable to allocate due to fragmentation
  al2.allocate_l2(_1m, _1m, &allocated4, &a4);
  ASSERT_EQ(a4.size(), 0u);

  for (uint64_t i = 0; i < capacity; i += 2 * _1m) {
    a4.clear();
    allocated4 = 0;
    al2.allocate_l2(_1m, 0x1000, &allocated4, &a4);
    ASSERT_EQ(a4.size(), _1m / 0x1000);
    ASSERT_EQ(allocated4, _1m);
    ASSERT_EQ(a4[0].offset, i);
    ASSERT_EQ(a4[0].length, 0x1000u);
    if (0 == (i % (1 * 1024 * _1m))) {
      std::cout << "alloc3 " << i / 1024 / 1024 << " mb of "
        << capacity / 1024 / 1024 << std::endl;
    }
  }
  ASSERT_EQ(0u, al2.debug_get_free());

  std::cout << "Done L2" << std::endl;
}

TEST(TestAllocatorLevel01, test_l2_huge)
{
  TestAllocatorLevel02 al2;
  uint64_t num_l2_entries = 4 * 512;
  uint64_t capacity = num_l2_entries * 256 * 512 * 4096; // 1 TB
  al2.init(capacity, 0x1000);
  std::cout << "Init L2 Huge" << std::endl;

  for (uint64_t i = 0; i < capacity; i += _1m) {
    uint64_t allocated4 = 0;
    interval_vector_t a4;
    al2.allocate_l2(0x1000, 0x1000, &allocated4, &a4);
    ASSERT_EQ(a4.size(), 1u);
    ASSERT_EQ(allocated4, 0x1000u);
    ASSERT_EQ(a4[0].offset, i);
    ASSERT_EQ(a4[0].length, 0x1000u);

    allocated4 = 0;
    a4.clear();
    al2.allocate_l2(_1m - 0x1000, 0x1000, &allocated4, &a4);
    ASSERT_EQ(a4.size(), 1u);
    ASSERT_EQ(allocated4, _1m - 0x1000);
    ASSERT_EQ(a4[0].offset, i + 0x1000);
    ASSERT_EQ(a4[0].length, _1m - 0x1000);
    if (0 == (i % (1 * 1024 * _1m))) {
      std::cout << "allocH " << i / 1024 / 1024 << " mb of "
        << capacity / 1024 / 1024 << std::endl;
    }
  }
  for (uint64_t i = 0; i < capacity; i += _1m) {
    interval_vector_t a4;
    a4.emplace_back(i, 0x1000);
    al2.free_l2(a4);
    if (0 == (i % (1 * 1024 * _1m))) {
      std::cout << "freeH1 " << i / 1024 / 1024 << " mb of "
        << capacity / 1024 / 1024 << std::endl;
    }
  }
  {
    std::cout << "Try" << std::endl;
    time_t t = time(NULL);
    for (int i = 0; i < 10; ++i) {
      uint64_t allocated = 0;
      interval_vector_t a;
      al2.allocate_l2(0x2000, 0x2000, &allocated, &a);
      ASSERT_EQ(a.size(), 0u);
    }
    std::cout << "End try in " << time(NULL) - t << " seconds" << std::endl;
  }
  {
    std::cout << "Try" << std::endl;
    time_t t = time(NULL);
    for (int i = 0; i < 10; ++i) {
      uint64_t allocated = 0;
      interval_vector_t a;
      al2.allocate_l2(_2m, _2m, &allocated, &a);
      ASSERT_EQ(a.size(), 0u);
    }
    std::cout << "End try in " << time(NULL) - t << " seconds" << std::endl;
  }

  ASSERT_EQ((capacity / _1m) * 0x1000, al2.debug_get_free());

  std::cout << "Done L2 Huge" << std::endl;
}

TEST(TestAllocatorLevel01, test_l2_unaligned)
{
  {
    TestAllocatorLevel02 al2;
    uint64_t num_l2_entries = 3;
    uint64_t capacity = num_l2_entries * 256 * 512 * 4096; // 3x512 MB
    al2.init(capacity, 0x1000);
    std::cout << "Init L2 Unaligned" << std::endl;

    for (uint64_t i = 0; i < capacity; i += _1m / 2) {
      uint64_t allocated4 = 0;
      interval_vector_t a4;
      al2.allocate_l2(_1m / 2, _1m / 2, &allocated4, &a4);
      ASSERT_EQ(a4.size(), 1u);
      ASSERT_EQ(allocated4, _1m / 2);
      ASSERT_EQ(a4[0].offset, i);
      ASSERT_EQ(a4[0].length, _1m / 2);
      if (0 == (i % (1 * 1024 * _1m))) {
        std::cout << "allocU " << i / 1024 / 1024 << " mb of "
          << capacity / 1024 / 1024 << std::endl;
      }
    }
    ASSERT_EQ(0u, al2.debug_get_free());
    {
      // no space to allocate
      uint64_t allocated4 = 0;
      interval_vector_t a4;
      al2.allocate_l2(0x1000, 0x1000, &allocated4, &a4);
      ASSERT_EQ(a4.size(), 0u);
    }
  }
  {
    TestAllocatorLevel02 al2;
    uint64_t capacity = 500 * 512 * 4096; // 500x2 MB
    al2.init(capacity, 0x1000);
    std::cout << ("Init L2 Unaligned2\n");
    for (uint64_t i = 0; i < capacity; i += _1m / 2) {
      uint64_t allocated4 = 0;
      interval_vector_t a4;
      al2.allocate_l2(_1m / 2, _1m / 2, &allocated4, &a4);
      ASSERT_EQ(a4.size(), 1u);
      ASSERT_EQ(allocated4, _1m / 2);
      ASSERT_EQ(a4[0].offset, i);
      ASSERT_EQ(a4[0].length, _1m / 2);
      if (0 == (i % (1 * 1024 * _1m))) {
        std::cout << "allocU2 " << i / 1024 / 1024 << " mb of "
          << capacity / 1024 / 1024 << std::endl;
      }
    }
    ASSERT_EQ(0u, al2.debug_get_free());
    {
      // no space to allocate
      uint64_t allocated4 = 0;
      interval_vector_t a4;
      al2.allocate_l2(0x1000, 0x1000, &allocated4, &a4);
      ASSERT_EQ(a4.size(), 0u);
    }
  }

  {
    TestAllocatorLevel02 al2;
    uint64_t capacity = 100 * 512 * 4096 + 127 * 4096;
    al2.init(capacity, 0x1000);
    std::cout << "Init L2 Unaligned2" << std::endl;
    for (uint64_t i = 0; i < capacity; i += 0x1000) {
      uint64_t allocated4 = 0;
      interval_vector_t a4;
      al2.allocate_l2(0x1000, 0x1000, &allocated4, &a4);
      ASSERT_EQ(a4.size(), 1u);
      ASSERT_EQ(allocated4, 0x1000u);
      ASSERT_EQ(a4[0].offset, i);
      ASSERT_EQ(a4[0].length, 0x1000u);
    }
    ASSERT_EQ(0u, al2.debug_get_free());
    {
      // no space to allocate
      uint64_t allocated4 = 0;
      interval_vector_t a4;
      al2.allocate_l2(0x1000, 0x1000, &allocated4, &a4);
      ASSERT_EQ(a4.size(), 0u);
    }
  }
  {
    TestAllocatorLevel02 al2;
    uint64_t capacity = 3 * 4096;
    al2.init(capacity, 0x1000);
    std::cout << "Init L2 Unaligned2" << std::endl;
    for (uint64_t i = 0; i < capacity; i += 0x1000) {
      uint64_t allocated4 = 0;
      interval_vector_t a4;
      al2.allocate_l2(0x1000, 0x1000, &allocated4, &a4);
      ASSERT_EQ(a4.size(), 1u);
      ASSERT_EQ(allocated4, 0x1000u);
      ASSERT_EQ(a4[0].offset, i);
      ASSERT_EQ(a4[0].length, 0x1000u);
    }
    ASSERT_EQ(0u, al2.debug_get_free());
    {
      // no space to allocate
      uint64_t allocated4 = 0;
      interval_vector_t a4;
      al2.allocate_l2(0x1000, 0x1000, &allocated4, &a4);
      ASSERT_EQ(a4.size(), 0u);
    }
  }

  std::cout << "Done L2 Unaligned" << std::endl;
}

TEST(TestAllocatorLevel01, test_l2_contiguous_alignment)
{
  {
    TestAllocatorLevel02 al2;
    uint64_t num_l2_entries = 3;
    uint64_t capacity = num_l2_entries * 256 * 512 * 4096; // 3x512 MB
    uint64_t num_chunks = capacity / 4096;
    al2.init(capacity, 4096);
    std::cout << "Init L2 cont aligned" << std::endl;

    std::map<size_t, size_t> bins_overall;
    al2.collect_stats(bins_overall);
    ASSERT_EQ(bins_overall.size(), 1u);
//    std::cout<<bins_overall.begin()->first << std::endl;
    ASSERT_EQ(bins_overall[cbits(num_chunks) - 1], 1u);

    for (uint64_t i = 0; i < capacity / 2; i += _1m) {
      uint64_t allocated4 = 0;
      interval_vector_t a4;
      al2.allocate_l2(_1m, _1m, &allocated4, &a4);
      ASSERT_EQ(a4.size(), 1u);
      ASSERT_EQ(allocated4, _1m);
      ASSERT_EQ(a4[0].offset, i);
      ASSERT_EQ(a4[0].length, _1m);
    }
    ASSERT_EQ(capacity / 2, al2.debug_get_free());

    bins_overall.clear();
    al2.collect_stats(bins_overall);
    ASSERT_EQ(bins_overall.size(), 1u);
    ASSERT_EQ(bins_overall[cbits(num_chunks / 2) - 1], 1u);

    {
      size_t to_release = 2 * _1m + 0x1000;
      // release 2M + 4K at the beginning
      interval_vector_t r;
      r.emplace_back(0, to_release);
      al2.free_l2(r);
      bins_overall.clear();
      al2.collect_stats(bins_overall);
      ASSERT_EQ(bins_overall.size(), 2u);
      ASSERT_EQ(bins_overall[cbits(to_release / 0x1000) - 1], 1u);
      ASSERT_EQ(bins_overall[cbits(num_chunks / 2) - 1], 1u);
    }
    {
      // allocate 4K within the deallocated range
      uint64_t allocated4 = 0;
      interval_vector_t a4;
      al2.allocate_l2(0x1000, 0x1000, &allocated4, &a4);
      ASSERT_EQ(a4.size(), 1u);
      ASSERT_EQ(allocated4, 0x1000u);
      ASSERT_EQ(a4[0].offset, 0u);
      ASSERT_EQ(a4[0].length, 0x1000u);
      bins_overall.clear();
      al2.collect_stats(bins_overall);
      ASSERT_EQ(bins_overall.size(), 2u);
      ASSERT_EQ(bins_overall[cbits(2 * _1m / 0x1000) - 1], 1u);
      ASSERT_EQ(bins_overall[cbits(num_chunks / 2) - 1], 1u);
    }
    {
      // allocate 1M - should go to the second 1M chunk
      uint64_t allocated4 = 0;
      interval_vector_t a4;
      al2.allocate_l2(_1m, _1m, &allocated4, &a4);
      ASSERT_EQ(a4.size(), 1u);
      ASSERT_EQ(allocated4, _1m);
      ASSERT_EQ(a4[0].offset, _1m);
      ASSERT_EQ(a4[0].length, _1m);
      bins_overall.clear();
      al2.collect_stats(bins_overall);
      ASSERT_EQ(bins_overall.size(), 3u);
      ASSERT_EQ(bins_overall[0], 1u);
      ASSERT_EQ(bins_overall[cbits((_1m - 0x1000) / 0x1000) - 1], 1u);
      ASSERT_EQ(bins_overall[cbits(num_chunks / 2) - 1], 1u);
    }
    {
      // and allocate yet another 8K within the deallocated range
      uint64_t allocated4 = 0;
      interval_vector_t a4;
      al2.allocate_l2(0x2000, 0x1000, &allocated4, &a4);
      ASSERT_EQ(a4.size(), 1u);
      ASSERT_EQ(allocated4, 0x2000u);
      ASSERT_EQ(a4[0].offset, 0x1000u);
      ASSERT_EQ(a4[0].length, 0x2000u);
      bins_overall.clear();
      al2.collect_stats(bins_overall);
      ASSERT_EQ(bins_overall[0], 1u);
      ASSERT_EQ(bins_overall[cbits((_1m - 0x3000) / 0x1000) - 1], 1u);
      ASSERT_EQ(bins_overall[cbits(num_chunks / 2) - 1], 1u);
    }
    {
      // release just allocated 1M
      interval_vector_t r;
      r.emplace_back(_1m, _1m);
      al2.free_l2(r);
      bins_overall.clear();
      al2.collect_stats(bins_overall);
      ASSERT_EQ(bins_overall.size(), 2u);
      ASSERT_EQ(bins_overall[cbits((2 * _1m - 0x3000) / 0x1000) - 1], 1u);
      ASSERT_EQ(bins_overall[cbits(num_chunks / 2) - 1], 1u);
    }
    {
      // allocate 3M - should go to the second 1M chunk and @capacity/2
      uint64_t allocated4 = 0;
      interval_vector_t a4;
      al2.allocate_l2(3 * _1m, _1m, &allocated4, &a4);
      ASSERT_EQ(a4.size(), 2u);
      ASSERT_EQ(allocated4, 3 * _1m);
      ASSERT_EQ(a4[0].offset, _1m);
      ASSERT_EQ(a4[0].length, _1m);
      ASSERT_EQ(a4[1].offset, capacity / 2);
      ASSERT_EQ(a4[1].length, 2 * _1m);
      bins_overall.clear();
      al2.collect_stats(bins_overall);
      ASSERT_EQ(bins_overall.size(), 3u);
      ASSERT_EQ(bins_overall[0], 1u);
      ASSERT_EQ(bins_overall[cbits((_1m - 0x3000) / 0x1000) - 1], 1u);
      ASSERT_EQ(bins_overall[cbits((num_chunks - 512) / 2) - 1], 1u);
    }
    {
      // release allocated 1M in the second meg chunk except
      // the first 4K chunk
      interval_vector_t r;
      r.emplace_back(_1m + 0x1000, _1m);
      al2.free_l2(r);
      bins_overall.clear();
      al2.collect_stats(bins_overall);
      ASSERT_EQ(bins_overall.size(), 3u);
      ASSERT_EQ(bins_overall[cbits(_1m / 0x1000) - 1], 1u);
      ASSERT_EQ(bins_overall[cbits((_1m - 0x3000) / 0x1000) - 1], 1u);
      ASSERT_EQ(bins_overall[cbits((num_chunks - 512) / 2) - 1], 1u);
    }
    {
      // release 2M @(capacity / 2)
      interval_vector_t r;
      r.emplace_back(capacity / 2, 2 * _1m);
      al2.free_l2(r);
      bins_overall.clear();
      al2.collect_stats(bins_overall);
      ASSERT_EQ(bins_overall.size(), 3u);
      ASSERT_EQ(bins_overall[cbits(_1m / 0x1000) - 1], 1u);
      ASSERT_EQ(bins_overall[cbits((_1m - 0x3000) / 0x1000) - 1], 1u);
      ASSERT_EQ(bins_overall[cbits((num_chunks) / 2) - 1], 1u);
    }
    {
      // allocate 4x512K - should go to the second halves of
      // the first and second 1M chunks and @(capacity / 2)
      uint64_t allocated4 = 0;
      interval_vector_t a4;
      al2.allocate_l2(2 * _1m, _1m / 2, &allocated4, &a4);
      ASSERT_EQ(a4.size(), 3u);
      ASSERT_EQ(allocated4, 2 * _1m);
      ASSERT_EQ(a4[0].offset, _1m / 2);
      ASSERT_EQ(a4[0].length, _1m / 2);
      ASSERT_EQ(a4[1].offset, _1m + _1m / 2);
      ASSERT_EQ(a4[1].length, _1m / 2);
      ASSERT_EQ(a4[2].offset, capacity / 2);
      ASSERT_EQ(a4[2].length, _1m);

      bins_overall.clear();
      al2.collect_stats(bins_overall);
      ASSERT_EQ(bins_overall.size(), 3u);
      ASSERT_EQ(bins_overall[0], 1u);
      // below we have 512K - 4K & 512K - 12K chunks which both fit into
      // the same bin = 6
      ASSERT_EQ(bins_overall[6], 2u);
      ASSERT_EQ(bins_overall[cbits((num_chunks - 256) / 2) - 1], 1u);

    }
    {
      // cleanup first 2M except except the last 4K chunk
      interval_vector_t r;
      r.emplace_back(0, 2 * _1m - 0x1000);
      al2.free_l2(r);
      bins_overall.clear();
      al2.collect_stats(bins_overall);

      ASSERT_EQ(bins_overall.size(), 3u);
      ASSERT_EQ(bins_overall[0], 1u);
      ASSERT_EQ(bins_overall[cbits((_2m - 0x1000) / 0x1000) - 1], 1u);
      ASSERT_EQ(bins_overall[cbits((num_chunks - 256) / 2) - 1], 1u);
    }
    {
      // release 2M @(capacity / 2)
      interval_vector_t r;
      r.emplace_back(capacity / 2, 2 * _1m);
      al2.free_l2(r);
      bins_overall.clear();
      al2.collect_stats(bins_overall);

      ASSERT_EQ(bins_overall.size(), 3u);
      ASSERT_EQ(bins_overall[0], 1u);
      ASSERT_EQ(bins_overall[cbits((_2m - 0x1000) / 0x1000) - 1], 1u);
      ASSERT_EQ(bins_overall[cbits(num_chunks / 2) - 1], 1u);
    }
    {
      // allocate 132M using 4M granularity should go to (capacity / 2)
      uint64_t allocated4 = 0;
      interval_vector_t a4;
      al2.allocate_l2(132 * _1m, 4 * _1m , &allocated4, &a4);
      ASSERT_EQ(a4.size(), 1u);
      ASSERT_EQ(a4[0].offset, capacity / 2);
      ASSERT_EQ(a4[0].length, 132 * _1m);

      bins_overall.clear();
      al2.collect_stats(bins_overall);
      ASSERT_EQ(bins_overall.size(), 3u);
    }
    {
      // cleanup left 4K chunk in the first 2M
      interval_vector_t r;
      r.emplace_back(2 * _1m - 0x1000, 0x1000);
      al2.free_l2(r);
      bins_overall.clear();
      al2.collect_stats(bins_overall);

      ASSERT_EQ(bins_overall.size(), 2u);
    }
    {
      // release 132M @(capacity / 2)
      interval_vector_t r;
      r.emplace_back(capacity / 2, 132 * _1m);
      al2.free_l2(r);
    }
    {
      // allocate 132M using 2M granularity should go to the first chunk and to
      // (capacity / 2)
      uint64_t allocated4 = 0;
      interval_vector_t a4;
      al2.allocate_l2(132 * _1m, 2 * _1m , &allocated4, &a4);
      ASSERT_EQ(a4.size(), 2u);
      ASSERT_EQ(a4[0].offset, 0u);
      ASSERT_EQ(a4[0].length, 2 * _1m);
      ASSERT_EQ(a4[1].offset, capacity / 2);
      ASSERT_EQ(a4[1].length, 130 * _1m);
    }
    {
      // release 130M @(capacity / 2)
      interval_vector_t r;
      r.emplace_back(capacity / 2, 132 * _1m);
      al2.free_l2(r);
    }
    {
      // release 4K~16K
      // release 28K~32K
      // release 68K~24K
      interval_vector_t r;
      r.emplace_back(0x1000, 0x4000);
      r.emplace_back(0x7000, 0x8000);
      r.emplace_back(0x11000, 0x6000);
      al2.free_l2(r);
    }
    {
      // allocate 32K using 16K granularity - should bypass the first
      // unaligned extent, use the second free extent partially given
      // the 16K alignment and then fallback to capacity / 2
      uint64_t allocated4 = 0;
      interval_vector_t a4;
      al2.allocate_l2(0x8000, 0x4000, &allocated4, &a4);
      ASSERT_EQ(a4.size(), 2u);
      ASSERT_EQ(a4[0].offset, 0x8000u);
      ASSERT_EQ(a4[0].length, 0x4000u);
      ASSERT_EQ(a4[1].offset, capacity / 2);
      ASSERT_EQ(a4[1].length, 0x4000u);
    }

  }
  std::cout << "Done L2 cont aligned" << std::endl;
}

TEST(TestAllocatorLevel01, test_4G_alloc_bug)
{
  {
    TestAllocatorLevel02 al2;
    uint64_t capacity = 0x8000 * _1m; // = 32GB
    al2.init(capacity, 0x10000);
    std::cout << "Init L2 cont aligned" << std::endl;

      uint64_t allocated4 = 0;
      interval_vector_t a4;
      al2.allocate_l2(_1m, _1m, &allocated4, &a4);
      ASSERT_EQ(a4.size(), 1u); // the bug caused no allocations here
      ASSERT_EQ(allocated4, _1m);
      ASSERT_EQ(a4[0].offset, 0u);
      ASSERT_EQ(a4[0].length, _1m);
  }
}

TEST(TestAllocatorLevel01, test_4G_alloc_bug2)
{
  {
    TestAllocatorLevel02 al2;
    uint64_t capacity = 0x8000 * _1m; // = 32GB
    al2.init(capacity, 0x10000);

    for (uint64_t i = 0; i < capacity; i += _1m) {
      uint64_t allocated4 = 0;
      interval_vector_t a4;
      al2.allocate_l2(_1m, _1m, &allocated4, &a4);
      ASSERT_EQ(a4.size(), 1u);
      ASSERT_EQ(allocated4, _1m);
      ASSERT_EQ(a4[0].offset, i);
      ASSERT_EQ(a4[0].length, _1m);
    }
    ASSERT_EQ(0u , al2.debug_get_free());

    interval_vector_t r;
    r.emplace_back(0x5fec30000, 0x13d0000);
    r.emplace_back(0x628000000, 0x80000000);
    r.emplace_back(0x6a8000000, 0x80000000);
    r.emplace_back(0x728100000, 0x70000);
    al2.free_l2(r);

    std::map<size_t, size_t> bins_overall;
    al2.collect_stats(bins_overall);

    uint64_t allocated4 = 0;
    interval_vector_t a4;
    al2.allocate_l2(0x3e000000, _1m, &allocated4, &a4);
    ASSERT_EQ(a4.size(), 2u);
    ASSERT_EQ(allocated4, 0x3e000000u);
    ASSERT_EQ(a4[0].offset, 0x5fed00000u);
    ASSERT_EQ(a4[0].length, 0x1300000u);
    ASSERT_EQ(a4[1].offset, 0x628000000u);
    ASSERT_EQ(a4[1].length, 0x3cd00000u);
  }
}

TEST(TestAllocatorLevel01, test_4G_alloc_bug3)
{
  {
    TestAllocatorLevel02 al2;
    uint64_t capacity = 0x8000 * _1m; // = 32GB
    al2.init(capacity, 0x10000);
    std::cout << "Init L2 cont aligned" << std::endl;

      uint64_t allocated4 = 0;
      interval_vector_t a4;
      al2.allocate_l2(4096ull * _1m, _1m, &allocated4, &a4);
      ASSERT_EQ(a4.size(), 2u); // allocator has to split into 2 allocations
      ASSERT_EQ(allocated4, 4096ull * _1m);
      ASSERT_EQ(a4[0].offset, 0u);
      ASSERT_EQ(a4[0].length, 2048ull * _1m);
      ASSERT_EQ(a4[1].offset, 2048ull * _1m);
      ASSERT_EQ(a4[1].length, 2048ull * _1m);
  }
}
