// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * In memory space allocator test cases.
 * Author: Ramesh Chander, Ramesh.Chander@sandisk.com
 */
#include "os/bluestore/Allocator.h"
#include "global/global_init.h"
#include <iostream>
#include "include/Context.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/errno.h"
#include "include/stringify.h"
#include <gtest/gtest.h>
#include <os/bluestore/BitAllocator.h>

#if GTEST_HAS_PARAM_TEST

class AllocTest : public ::testing::TestWithParam<const char*> {
public:
    boost::scoped_ptr<Allocator> alloc;
    AllocTest(): alloc(0) { }
    void init_alloc(int64_t size, uint64_t min_alloc_size) {
      std::cout << "Creating alloc type " << string(GetParam()) << " \n";
      alloc.reset(Allocator::create(string(GetParam()), size, min_alloc_size));
    }

    void init_close() {
      alloc.reset(0);
    }
};

TEST_P(AllocTest, test_alloc_init)
{
  int64_t blocks = BmapEntry::size();
  init_alloc(blocks, 1);
  ASSERT_EQ(0U, alloc->get_free());
  alloc->shutdown(); 
  blocks = BitMapZone::get_total_blocks() * 2 + 16;
  init_alloc(blocks, 1);
  ASSERT_EQ(0U, alloc->get_free());
  alloc->shutdown(); 
  blocks = BitMapZone::get_total_blocks() * 2;
  init_alloc(blocks, 1);
  ASSERT_EQ(alloc->get_free(), 0);
}

TEST_P(AllocTest, test_alloc_min_alloc)
{
  int64_t block_size = 1024;
  int64_t blocks = BitMapZone::get_total_blocks() * 2 * block_size;
  uint64_t offset = 0;
  uint32_t length = 0;
  int count = 0;

  init_alloc(blocks, block_size);
  alloc->init_add_free(block_size, block_size);
  EXPECT_EQ(alloc->reserve(block_size), 0);
  EXPECT_EQ(alloc->allocate(block_size, block_size, 0, &offset, &length), 0);

  /*
   * Allocate extent and make sure all comes in single extent.
   */   
  {
    alloc->init_add_free(0, block_size * 4);
    EXPECT_EQ(alloc->reserve(block_size * 4), 0);
    std::vector<AllocExtent> extents = std::vector<AllocExtent> 
                        (4, AllocExtent(0, 0));
  
    EXPECT_EQ(alloc->alloc_extents(4 * (uint64_t)block_size, (uint64_t) block_size, 
                                   0, (int64_t) 0, &extents, &count), 0);
    EXPECT_EQ(extents[0].length, 4 * block_size);
    EXPECT_EQ(0U, extents[1].length);
    EXPECT_EQ(count, 1);
  }

  /*
   * Allocate extent and make sure we get two different extents.
   */
  {
    alloc->init_add_free(0, block_size * 2);
    alloc->init_add_free(3 * block_size, block_size * 2);
    EXPECT_EQ(alloc->reserve(block_size * 4), 0);
    std::vector<AllocExtent> extents = std::vector<AllocExtent> 
                        (4, AllocExtent(0, 0));
  
    EXPECT_EQ(alloc->alloc_extents(4 * (uint64_t)block_size, (uint64_t) block_size, 
                                   0, (int64_t) 0, &extents, &count), 0);
    EXPECT_EQ(extents[0].length, 2 * block_size);
    EXPECT_EQ(extents[1].length, 2 * block_size);
    EXPECT_EQ(0U, extents[2].length);
    EXPECT_EQ(count, 2);
  }
  alloc->shutdown();
}

TEST_P(AllocTest, test_alloc_min_max_alloc)
{
  int64_t block_size = 1024;
  int64_t blocks = BitMapZone::get_total_blocks() * 2 * block_size;
  int count = 0;

  init_alloc(blocks, block_size);

  /*
   * Make sure we get all extents different when
   * min_alloc_size == max_alloc_size
   */
  {
    alloc->init_add_free(0, block_size * 4);
    EXPECT_EQ(alloc->reserve(block_size * 4), 0);
    std::vector<AllocExtent> extents = std::vector<AllocExtent> 
                        (4, AllocExtent(0, 0));
  
    EXPECT_EQ(alloc->alloc_extents(4 * (uint64_t)block_size, (uint64_t) block_size, 
                                   block_size, (int64_t) 0, &extents, &count), 0);
    for (int i = 0; i < 4; i++) {
      EXPECT_EQ(extents[i].length, block_size);
    }
    EXPECT_EQ(count, 4);
  }


  /*
   * Make sure we get extents of length max_alloc size
   * when max alloc size > min_alloc size
   */
  {
    alloc->init_add_free(0, block_size * 4);
    EXPECT_EQ(alloc->reserve(block_size * 4), 0);
    std::vector<AllocExtent> extents = std::vector<AllocExtent> 
                        (2, AllocExtent(0, 0));
  
    EXPECT_EQ(alloc->alloc_extents(4 * (uint64_t)block_size, (uint64_t) block_size, 
                                   2 * block_size, (int64_t) 0, &extents, &count), 0);
    for (int i = 0; i < 2; i++) {
      EXPECT_EQ(extents[i].length, block_size * 2);
    }
    EXPECT_EQ(count, 2);
  }

  /*
   * Allocate and free.
   */
  {
    alloc->init_add_free(0, block_size * 16);
    EXPECT_EQ(alloc->reserve(block_size * 16), 0);
    std::vector<AllocExtent> extents = std::vector<AllocExtent> 
                        (8, AllocExtent(0, 0));
  
    EXPECT_EQ(alloc->alloc_extents(16 * (uint64_t)block_size, (uint64_t) block_size, 
                                   2 * block_size, (int64_t) 0, &extents, &count), 0);

    EXPECT_EQ(count, 8);
    for (int i = 0; i < 8; i++) {
      EXPECT_EQ(extents[i].length, 2 * block_size);
    }
    EXPECT_EQ(alloc->release_extents(&extents, count), 0);
  }
}

TEST_P(AllocTest, test_alloc_failure)
{
  int64_t block_size = 1024;
  int64_t blocks = BitMapZone::get_total_blocks() * block_size;
  int count = 0;

  init_alloc(blocks, block_size);
  {
    alloc->init_add_free(0, block_size * 256);
    alloc->init_add_free(block_size * 512, block_size * 256);

    EXPECT_EQ(alloc->reserve(block_size * 512), 0);
    std::vector<AllocExtent> extents = std::vector<AllocExtent> 
                        (4, AllocExtent(0, 0));
  
    EXPECT_EQ(alloc->alloc_extents(512 * (uint64_t)block_size, (uint64_t) block_size * 256, 
                                   block_size * 256, (int64_t) 0, &extents, &count), 0);
    alloc->init_add_free(0, block_size * 256);
    alloc->init_add_free(block_size * 512, block_size * 256);
    EXPECT_EQ(alloc->reserve(block_size * 512), 0);
    EXPECT_EQ(alloc->alloc_extents(512 * (uint64_t)block_size, (uint64_t) block_size * 512,
                                   block_size * 512, (int64_t) 0, &extents, &count), -ENOSPC);
  }
}

INSTANTIATE_TEST_CASE_P(
  Allocator,
  AllocTest,
  ::testing::Values("stupid", "bitmap"));

#else

TEST(DummyTest, ValueParameterizedTestsAreNotSupportedOnThisPlatform) {}
#endif

int main(int argc, char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  env_to_vec(args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
