// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * In memory space allocator test cases.
 * Author: Ramesh Chander, Ramesh.Chander@sandisk.com
 */
#include <iostream>
#include <boost/scoped_ptr.hpp>
#include <gtest/gtest.h>

#include "common/Cond.h"
#include "common/errno.h"
#include "include/stringify.h"
#include "include/Context.h"
#include "os/bluestore/Allocator.h"

using namespace std;

typedef boost::mt11213b gen_type;

class AllocTest : public ::testing::TestWithParam<const char*> {

public:
  boost::scoped_ptr<Allocator> alloc;
  AllocTest(): alloc(0) { }
  void init_alloc(int64_t size, uint64_t min_alloc_size) {
    std::cout << "Creating alloc type " << string(GetParam()) << " \n";
    alloc.reset(Allocator::create(g_ceph_context, GetParam(), size,
				  min_alloc_size,
				  256*1048576, 100*256*1048576ull));
  }

  void init_close() {
    alloc.reset(0);
  }
};

TEST_P(AllocTest, test_alloc_init)
{
  int64_t blocks = 64;
  init_alloc(blocks, 1);
  ASSERT_EQ(0U, alloc->get_free());
  alloc->shutdown(); 
  blocks = 1024 * 2 + 16;
  init_alloc(blocks, 1);
  ASSERT_EQ(0U, alloc->get_free());
  alloc->shutdown(); 
  blocks = 1024 * 2;
  init_alloc(blocks, 1);
  ASSERT_EQ(alloc->get_free(), (uint64_t) 0);
}

TEST_P(AllocTest, test_init_add_free)
{
  int64_t block_size = 1024;
  int64_t capacity = 4 * 1024 * block_size;

  {
    init_alloc(capacity, block_size);

    auto free = alloc->get_free();
    alloc->init_add_free(block_size, 0);
    ASSERT_EQ(free, alloc->get_free());

    alloc->init_rm_free(block_size, 0);
    ASSERT_EQ(free, alloc->get_free());
  }
}

TEST_P(AllocTest, test_alloc_min_alloc)
{
  int64_t block_size = 1024;
  int64_t capacity = 4 * 1024 * block_size;

  {
    init_alloc(capacity, block_size);

    alloc->init_add_free(block_size, block_size);
    PExtentVector extents;
    EXPECT_EQ(block_size, alloc->allocate(block_size, block_size,
					  0, (int64_t) 0, &extents));
  }

  /*
   * Allocate extent and make sure all comes in single extent.
   */   
  {
    init_alloc(capacity, block_size);
    alloc->init_add_free(0, block_size * 4);
    PExtentVector extents;
    EXPECT_EQ(4*block_size,
	      alloc->allocate(4 * (uint64_t)block_size, (uint64_t) block_size,
			      0, (int64_t) 0, &extents));
    EXPECT_EQ(1u, extents.size());
    EXPECT_EQ(extents[0].length, 4 * block_size);
  }

  /*
   * Allocate extent and make sure we get two different extents.
   */
  {
    init_alloc(capacity, block_size);
    alloc->init_add_free(0, block_size * 2);
    alloc->init_add_free(3 * block_size, block_size * 2);
    PExtentVector extents;
  
    EXPECT_EQ(4*block_size,
	      alloc->allocate(4 * (uint64_t)block_size, (uint64_t) block_size,
			      0, (int64_t) 0, &extents));
    EXPECT_EQ(2u, extents.size());
    EXPECT_EQ(extents[0].length, 2 * block_size);
    EXPECT_EQ(extents[1].length, 2 * block_size);
  }
  alloc->shutdown();
}

TEST_P(AllocTest, test_alloc_min_max_alloc)
{
  int64_t block_size = 1024;

  int64_t capacity = 4 * 1024 * block_size;
  init_alloc(capacity, block_size);

  /*
   * Make sure we get all extents different when
   * min_alloc_size == max_alloc_size
   */
  {
    init_alloc(capacity, block_size);
    alloc->init_add_free(0, block_size * 4);
    PExtentVector extents;
    EXPECT_EQ(4*block_size,
	      alloc->allocate(4 * (uint64_t)block_size, (uint64_t) block_size,
			      block_size, (int64_t) 0, &extents));
    for (auto e : extents) {
      EXPECT_EQ(e.length, block_size);
    }
    EXPECT_EQ(4u, extents.size());
  }


  /*
   * Make sure we get extents of length max_alloc size
   * when max alloc size > min_alloc size
   */
  {
    init_alloc(capacity, block_size);
    alloc->init_add_free(0, block_size * 4);
    PExtentVector extents;
    EXPECT_EQ(4*block_size,
	      alloc->allocate(4 * (uint64_t)block_size, (uint64_t) block_size,
			      2 * block_size, (int64_t) 0, &extents));
    EXPECT_EQ(2u, extents.size());
    for (auto& e : extents) {
      EXPECT_EQ(e.length, block_size * 2);
    }
  }

  /*
   * Make sure allocations are of min_alloc_size when min_alloc_size > block_size.
   */
  {
    init_alloc(capacity, block_size);
    alloc->init_add_free(0, block_size * 1024);
    PExtentVector extents;
    EXPECT_EQ(1024 * block_size,
	      alloc->allocate(1024 * (uint64_t)block_size,
			      (uint64_t) block_size * 4,
			      block_size * 4, (int64_t) 0, &extents));
    for (auto& e : extents) {
      EXPECT_EQ(e.length, block_size * 4);
    }
    EXPECT_EQ(1024u/4, extents.size());
  }

  /*
   * Allocate and free.
   */
  {
    init_alloc(capacity, block_size);
    alloc->init_add_free(0, block_size * 16);
    PExtentVector extents;
    EXPECT_EQ(16 * block_size,
	      alloc->allocate(16 * (uint64_t)block_size, (uint64_t) block_size,
			      2 * block_size, (int64_t) 0, &extents));

    EXPECT_EQ(extents.size(), 8u);
    for (auto& e : extents) {
      EXPECT_EQ(e.length, 2 * block_size);
    }
  }
}

TEST_P(AllocTest, test_alloc_failure)
{
  int64_t block_size = 1024;
  int64_t capacity = 4 * 1024 * block_size;

  {
    init_alloc(capacity, block_size);
    alloc->init_add_free(0, block_size * 256);
    alloc->init_add_free(block_size * 512, block_size * 256);

    PExtentVector extents;
    EXPECT_EQ(512 * block_size,
	      alloc->allocate(512 * (uint64_t)block_size,
			      (uint64_t) block_size * 256,
			      block_size * 256, (int64_t) 0, &extents));
    alloc->init_add_free(0, block_size * 256);
    alloc->init_add_free(block_size * 512, block_size * 256);
    extents.clear();
    EXPECT_EQ(-ENOSPC,
	      alloc->allocate(512 * (uint64_t)block_size,
			      (uint64_t) block_size * 512,
			      block_size * 512, (int64_t) 0, &extents));
  }
}

TEST_P(AllocTest, test_alloc_big)
{
  int64_t block_size = 4096;
  int64_t blocks = 104857600;
  int64_t mas = 4096;
  init_alloc(blocks*block_size, block_size);
  alloc->init_add_free(2*block_size, (blocks-2)*block_size);
  for (int64_t big = mas; big < 1048576*128; big*=2) {
    cout << big << std::endl;
    PExtentVector extents;
    EXPECT_EQ(big,
	      alloc->allocate(big, mas, 0, &extents));
  }
}

TEST_P(AllocTest, test_alloc_non_aligned_len)
{
  int64_t block_size = 1 << 12;
  int64_t blocks = (1 << 20) * 100;
  int64_t want_size = 1 << 22;
  int64_t alloc_unit = 1 << 20;
  
  init_alloc(blocks*block_size, block_size);
  alloc->init_add_free(0, 2097152);
  alloc->init_add_free(2097152, 1064960);
  alloc->init_add_free(3670016, 2097152);

  PExtentVector extents;
  EXPECT_EQ(want_size, alloc->allocate(want_size, alloc_unit, 0, &extents));
}

TEST_P(AllocTest, test_alloc_39334)
{
  uint64_t block = 0x4000;
  uint64_t size = 0x5d00000000;

  init_alloc(size, block);
  alloc->init_add_free(0x4000, 0x5cffffc000);
  EXPECT_EQ(size - block, alloc->get_free());
}

TEST_P(AllocTest, test_alloc_fragmentation)
{
  uint64_t capacity = 4 * 1024 * 1024;
  uint64_t alloc_unit = 4096;
  uint64_t want_size = alloc_unit;
  PExtentVector allocated, tmp;
  
  init_alloc(capacity, alloc_unit);
  alloc->init_add_free(0, capacity);
  bool bitmap_alloc = GetParam() == std::string("bitmap");
  
  EXPECT_EQ(0.0, alloc->get_fragmentation());

  for (size_t i = 0; i < capacity / alloc_unit; ++i)
  {
    tmp.clear();
    EXPECT_EQ(static_cast<int64_t>(want_size),
	      alloc->allocate(want_size, alloc_unit, 0, 0, &tmp));
    allocated.insert(allocated.end(), tmp.begin(), tmp.end());

    // bitmap fragmentation calculation doesn't provide such constant
    // estimate
    if (!bitmap_alloc) {
      EXPECT_EQ(0.0, alloc->get_fragmentation());
    }
  }
  tmp.clear();
  EXPECT_EQ(-ENOSPC, alloc->allocate(want_size, alloc_unit, 0, 0, &tmp));

  if (GetParam() == string("avl")) {
    // AVL allocator uses a different allocating strategy
    GTEST_SKIP() << "skipping for AVL allocator";
  } else if (GetParam() == string("hybrid")) {
    // AVL allocator uses a different allocating strategy
    GTEST_SKIP() << "skipping for Hybrid allocator";
  }

  for (size_t i = 0; i < allocated.size(); i += 2)
  {
    interval_set<uint64_t> release_set;
    release_set.insert(allocated[i].offset, allocated[i].length);
    alloc->release(release_set);
  }
  EXPECT_EQ(1.0, alloc->get_fragmentation());

  for (size_t i = 1; i < allocated.size() / 2; i += 2)
  {
    interval_set<uint64_t> release_set;
    release_set.insert(allocated[i].offset, allocated[i].length);
    alloc->release(release_set);
  }
  if (bitmap_alloc) {
    // fragmentation = one l1 slot is free + one l1 slot is partial
    EXPECT_EQ(50U, uint64_t(alloc->get_fragmentation() * 100));
  } else {
    // fragmentation approx = 257 intervals / 768 max intervals
    EXPECT_EQ(33u, uint64_t(alloc->get_fragmentation() * 100));
  }

  for (size_t i = allocated.size() / 2 + 1; i < allocated.size(); i += 2)
  {
    interval_set<uint64_t> release_set;
    release_set.insert(allocated[i].offset, allocated[i].length);
    alloc->release(release_set);
  }
  // doing some rounding trick as stupid allocator doesn't merge all the 
  // extents that causes some minor fragmentation (minor bug or by-design behavior?).
  // Hence leaving just two 
  // digits after decimal point due to this.
  EXPECT_EQ(0u, uint64_t(alloc->get_fragmentation() * 100));
}

TEST_P(AllocTest, test_fragmentation_score_0)
{
  uint64_t capacity = 16LL * 1024 * 1024 * 1024; //16 GB, very small
  uint64_t alloc_unit = 4096;

  init_alloc(capacity, alloc_unit);
  alloc->init_add_free(0, capacity);
  EXPECT_EQ(0, alloc->get_fragmentation_score());

  // alloc every 100M, should get very small score
  for (uint64_t pos = 0; pos < capacity; pos +=  100 * 1024 * 1024) {
    alloc->init_rm_free(pos, alloc_unit);
  }
  EXPECT_LT(alloc->get_fragmentation_score(), 0.0001); // frag < 0.01%
  for (uint64_t pos = 0; pos < capacity; pos +=  100 * 1024 * 1024) {
    // put back
    alloc->init_add_free(pos, alloc_unit);
  }

  // 10% space is trashed, rest is free, small score
  for (uint64_t pos = 0; pos < capacity / 10; pos +=  3 * alloc_unit) {
    alloc->init_rm_free(pos, alloc_unit);
  }
  EXPECT_LT(0.01, alloc->get_fragmentation_score()); // 1% < frag < 10%
  EXPECT_LT(alloc->get_fragmentation_score(), 0.1);
}

TEST_P(AllocTest, test_fragmentation_score_some)
{
  uint64_t capacity = 1024 * 1024 * 1024; //1 GB, very small
  uint64_t alloc_unit = 4096;

  init_alloc(capacity, alloc_unit);
  alloc->init_add_free(0, capacity);
  // half (in 16 chunks) is completely free,
  // other half completely fragmented, expect less than 50% fragmentation score
  for (uint64_t chunk = 0; chunk < capacity; chunk += capacity / 16) {
    for (uint64_t pos = 0; pos < capacity / 32; pos += alloc_unit * 3) {
      alloc->init_rm_free(chunk + pos, alloc_unit);
    }
  }
  EXPECT_LT(alloc->get_fragmentation_score(), 0.5); // f < 50%

  init_alloc(capacity, alloc_unit);
  alloc->init_add_free(0, capacity);
  // half (in 16 chunks) is completely full,
  // other half completely fragmented, expect really high fragmentation score
  for (uint64_t chunk = 0; chunk < capacity; chunk += capacity / 16) {
    alloc->init_rm_free(chunk + capacity / 32, capacity / 32);
    for (uint64_t pos = 0; pos < capacity / 32; pos += alloc_unit * 3) {
      alloc->init_rm_free(chunk + pos, alloc_unit);
    }
  }
  EXPECT_LT(0.9, alloc->get_fragmentation_score()); // 50% < f
}

TEST_P(AllocTest, test_fragmentation_score_1)
{
  uint64_t capacity = 1024 * 1024 * 1024; //1 GB, very small
  uint64_t alloc_unit = 4096;

  init_alloc(capacity, alloc_unit);
  alloc->init_add_free(0, capacity);
  // alloc every second AU, max fragmentation
  for (uint64_t pos = 0; pos < capacity; pos += alloc_unit * 2) {
    alloc->init_rm_free(pos, alloc_unit);
  }
  EXPECT_LT(0.99, alloc->get_fragmentation_score()); // 99% < f

  init_alloc(capacity, alloc_unit);
  alloc->init_add_free(0, capacity);
  // 1 allocated, 4 empty; expect very high score
  for (uint64_t pos = 0; pos < capacity; pos += alloc_unit * 5) {
    alloc->init_rm_free(pos, alloc_unit);
  }
  EXPECT_LT(0.90, alloc->get_fragmentation_score()); // 90% < f
}

TEST_P(AllocTest, test_dump_fragmentation_score)
{
  uint64_t capacity = 1024 * 1024 * 1024;
  uint64_t one_alloc_max = 2 * 1024 * 1024;
  uint64_t alloc_unit = 4096;
  uint64_t want_size = alloc_unit;
  uint64_t rounds = 10;
  uint64_t actions_per_round = 1000;
  PExtentVector allocated, tmp;
  gen_type rng;

  init_alloc(capacity, alloc_unit);
  alloc->init_add_free(0, capacity);

  EXPECT_EQ(0.0, alloc->get_fragmentation());
  EXPECT_EQ(0.0, alloc->get_fragmentation_score());

  uint64_t allocated_cnt = 0;
  for (size_t round = 0; round < rounds ; round++) {
    for (size_t j = 0; j < actions_per_round ; j++) {
      //free or allocate ?
      if ( rng() % capacity >= allocated_cnt ) {
	//allocate
	want_size = ( rng() % one_alloc_max ) / alloc_unit * alloc_unit + alloc_unit;
	tmp.clear();
        int64_t r = alloc->allocate(want_size, alloc_unit, 0, 0, &tmp);
        if (r > 0) {
          for (auto& t: tmp) {
            if (t.length > 0)
              allocated.push_back(t);
          }
          allocated_cnt += r;
        }
      } else {
	//free
	ceph_assert(allocated.size() > 0);
	size_t item = rng() % allocated.size();
	ceph_assert(allocated[item].length > 0);
	allocated_cnt -= allocated[item].length;
	interval_set<uint64_t> release_set;
	release_set.insert(allocated[item].offset, allocated[item].length);
	alloc->release(release_set);
	std::swap(allocated[item], allocated[allocated.size() - 1]);
	allocated.resize(allocated.size() - 1);
      }
    }

    size_t free_sum = 0;
    auto iterated_allocation = [&](size_t off, size_t len) {
      ceph_assert(len > 0);
      free_sum += len;
    };
    alloc->foreach(iterated_allocation);
    EXPECT_GT(1, alloc->get_fragmentation_score());
    EXPECT_EQ(capacity, free_sum + allocated_cnt);
  }

  for (size_t i = 0; i < allocated.size(); i ++)
  {
    interval_set<uint64_t> release_set;
    release_set.insert(allocated[i].offset, allocated[i].length);
    alloc->release(release_set);
  }
}

TEST_P(AllocTest, test_alloc_bug_24598)
{
  if (string(GetParam()) != "bitmap")
    return;
  
  uint64_t capacity = 0x2625a0000ull;
  uint64_t alloc_unit = 0x4000;
  uint64_t want_size = 0x200000;
  PExtentVector allocated, tmp;

  init_alloc(capacity, alloc_unit);

  alloc->init_add_free(0x4800000, 0x100000);
  alloc->init_add_free(0x4a00000, 0x100000);

  alloc->init_rm_free(0x4800000, 0x100000);
  alloc->init_rm_free(0x4a00000, 0x100000);

  alloc->init_add_free(0x3f00000, 0x500000);
  alloc->init_add_free(0x4500000, 0x100000);
  alloc->init_add_free(0x4700000, 0x100000);
  alloc->init_add_free(0x4900000, 0x100000);
  alloc->init_add_free(0x4b00000, 0x200000);

  EXPECT_EQ(static_cast<int64_t>(want_size),
	    alloc->allocate(want_size, 0x100000, 0, 0, &tmp));
  EXPECT_EQ(1u, tmp.size());
  EXPECT_EQ(0x4b00000u, tmp[0].offset);
  EXPECT_EQ(0x200000u, tmp[0].length);
}

//Verifies issue from
//http://tracker.ceph.com/issues/40703
//
TEST_P(AllocTest, test_alloc_big2)
{
  int64_t block_size = 4096;
  int64_t blocks = 1048576 * 2;
  int64_t mas = 1024*1024;
  init_alloc(blocks*block_size, block_size);
  alloc->init_add_free(0, blocks * block_size);

  PExtentVector extents;
  uint64_t need = block_size * blocks / 4; // 2GB
  EXPECT_EQ(need,
      alloc->allocate(need, mas, 0, &extents));
  need = block_size * blocks / 4; // 2GB
  extents.clear();
  EXPECT_EQ(need,
      alloc->allocate(need, mas, 0, &extents));
  EXPECT_TRUE(extents[0].length > 0);
}

//Verifies stuck 4GB chunk allocation
//in StupidAllocator
//
TEST_P(AllocTest, test_alloc_big3)
{
  int64_t block_size = 4096;
  int64_t blocks = 1048576 * 2;
  int64_t mas = 1024*1024;
  init_alloc(blocks*block_size, block_size);
  alloc->init_add_free(0, blocks * block_size);

  PExtentVector extents;
  uint64_t need = block_size * blocks / 2; // 4GB
  EXPECT_EQ(need,
      alloc->allocate(need, mas, 0, &extents));
  EXPECT_TRUE(extents[0].length > 0);
}

TEST_P(AllocTest, test_alloc_contiguous)
{
  int64_t block_size = 0x1000;
  int64_t capacity = block_size * 1024 * 1024;

  {
    init_alloc(capacity, block_size);

    alloc->init_add_free(0, capacity);
    PExtentVector extents;
    uint64_t need = 4 * block_size;
    EXPECT_EQ(need,
      alloc->allocate(need, need,
        0, (int64_t)0, &extents));
    EXPECT_EQ(1u, extents.size());
    EXPECT_EQ(extents[0].offset, 0);
    EXPECT_EQ(extents[0].length, 4 * block_size);

    extents.clear();
    EXPECT_EQ(need,
      alloc->allocate(need, need,
        0, (int64_t)0, &extents));
    EXPECT_EQ(1u, extents.size());
    EXPECT_EQ(extents[0].offset, 4 * block_size);
    EXPECT_EQ(extents[0].length, 4 * block_size);
  }

  alloc->shutdown();
}

TEST_P(AllocTest, test_alloc_47883)
{
  uint64_t block = 0x1000;
  uint64_t size = 1599858540544ul;

  init_alloc(size, block);

  alloc->init_add_free(0x1b970000, 0x26000);
  alloc->init_add_free(0x1747e9d5000, 0x493000);
  alloc->init_add_free(0x1747ee6a000, 0x196000);

  PExtentVector extents;
  auto need = 0x3f980000;
  auto got = alloc->allocate(need, 0x10000, 0, (int64_t)0, &extents);
  EXPECT_GE(got, 0x630000);
}

TEST_P(AllocTest, test_alloc_50656_best_fit)
{
  uint64_t block = 0x1000;
  uint64_t size = 0x3b9e400000;

  init_alloc(size, block);

  // too few free extents - causes best fit mode for avls
  for (size_t i = 0; i < 0x10; i++) {
    alloc->init_add_free(i * 2 * 0x100000, 0x100000);
  }

  alloc->init_add_free(0x1e1bd13000, 0x404000);

  PExtentVector extents;
  auto need = 0x400000;
  auto got = alloc->allocate(need, 0x10000, 0, (int64_t)0, &extents);
  EXPECT_GT(got, 0);
  EXPECT_EQ(got, 0x400000);
}

TEST_P(AllocTest, test_alloc_50656_first_fit)
{
  uint64_t block = 0x1000;
  uint64_t size = 0x3b9e400000;

  init_alloc(size, block);

  for (size_t i = 0; i < 0x10000; i += 2) {
    alloc->init_add_free(i * 0x100000, 0x100000);
  }

  alloc->init_add_free(0x1e1bd13000, 0x404000);

  PExtentVector extents;
  auto need = 0x400000;
  auto got = alloc->allocate(need, 0x10000, 0, (int64_t)0, &extents);
  EXPECT_GT(got, 0);
  EXPECT_EQ(got, 0x400000);
}

TEST_P(AllocTest, test_init_rm_free_unbound)
{
  int64_t block_size = 1024;
  int64_t capacity = 4 * 1024 * block_size;

  {
    init_alloc(capacity, block_size);

    alloc->init_add_free(0, block_size * 2);
    alloc->init_add_free(block_size * 3, block_size * 3);
    alloc->init_add_free(block_size * 7, block_size * 2);

    alloc->init_rm_free(block_size * 4, block_size);
    ASSERT_EQ(alloc->get_free(), block_size * 6);

    auto cb = [&](size_t off, size_t len) {
      cout << std::hex << "0x" << off << "~" << len << std::dec << std::endl;
    };
    alloc->foreach(cb);
  }
}

INSTANTIATE_TEST_SUITE_P(
  Allocator,
  AllocTest,
  ::testing::Values("stupid", "bitmap", "avl", "hybrid", "btree"));
