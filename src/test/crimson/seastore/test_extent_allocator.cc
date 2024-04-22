// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <random>

#include <boost/iterator/counting_iterator.hpp>

#include "test/crimson/gtest_seastar.h"
#include "crimson/os/seastore/random_block_manager.h"
#include "crimson/os/seastore/random_block_manager/extent_allocator.h"
#include "crimson/os/seastore/random_block_manager/avlallocator.h"
#include "include/interval_set.h"


using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;

namespace {
  [[maybe_unused]] seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }
}

struct allocator_test_t :
  public seastar_test_suite_t,
  ::testing::WithParamInterface<const char*> {
  std::random_device rd;
  std::mt19937 gen;
  ExtentAllocatorRef allocator;

  allocator_test_t()
    : gen(rd()) {}

  seastar::future<> set_up_fut() final {
    std::string a_type = GetParam();
    if (a_type == "avl") {
      allocator.reset(new AvlAllocator(false));
      return seastar::now();
    } 
    ceph_assert(0 == "no support");
  }
  seastar::future<> tear_down_fut() final {
    if (allocator) {
      allocator->close();
    }
    return seastar::now();
  }
  void init_alloc(uint64_t block_size, uint64_t total_size) {
    assert(allocator);
    allocator->init(0, total_size, block_size);
  }
  void close() {
    assert(allocator);
    allocator->close();
  }
  auto allocate(size_t size) {
    return allocator->alloc_extent(size);
  }
  auto allocates(size_t size) {
    return allocator->alloc_extents(size);
  }
  void free(uint64_t start, uint64_t length) {
    allocator->free_extent(start, length);
  }
  rbm_abs_addr get_random_addr(size_t block_size, size_t capacity) {
    return block_size *
      std::uniform_int_distribution<>(0, (capacity / block_size) - 1)(gen);
  }
};

TEST_P(allocator_test_t, test_alloc_init)
{
  init_alloc(4096, 4096 * 64);
  ASSERT_EQ((4096 * 64), allocator->get_available_size());
  close();
  init_alloc(8192, 8192 * 32);
  allocate(8192);
  ASSERT_EQ(8192 * 32 - 8192, allocator->get_available_size());
  close();
  init_alloc(4096, 4096 * 128);
  allocate(8192);
  ASSERT_EQ(4096 * 128 - 8192, allocator->get_available_size());
}

TEST_P(allocator_test_t, test_init_alloc_free)
{
  uint64_t block_size = 4096;
  uint64_t capacity = 4 * 1024 * block_size;

  {
    init_alloc(block_size, capacity);

    auto free_length = allocator->get_available_size();
    allocate(allocator->get_max_alloc_size());
    ASSERT_EQ(free_length - allocator->get_max_alloc_size(),
      allocator->get_available_size());

    free(0, allocator->get_max_alloc_size());
    ASSERT_EQ(free_length, allocator->get_available_size());
  }
}

TEST_P(allocator_test_t, test_scattered_alloc)
{
  uint64_t block_size = 8192;
  uint64_t capacity = 1024 * block_size;

  {
    init_alloc(block_size, capacity);
    allocator->mark_extent_used(0, block_size * 256);
    allocator->mark_extent_used(block_size * 512, block_size * 256);

    auto result = allocates(block_size * 512);
    ASSERT_EQ(true, result.has_value());

    free(0, block_size * 256);

    result = allocates(block_size * 512);
    ASSERT_EQ(false, result.has_value());
  }
}

TEST_P(allocator_test_t, test_random_alloc_verify)
{
  uint64_t block_size = 4096;
  uint64_t capacity = 64 * 1024 * block_size;
  uint64_t avail = capacity;
  interval_set<rbm_abs_addr> alloc_map;
  init_alloc(block_size, capacity);

  {
    for (int i = 0; i < 256; i++) {
      auto addr = get_random_addr(block_size, capacity);
      auto size = get_random_addr(block_size, capacity) % (4 << 20);
      if (addr + size > capacity || size == 0 ||
	  alloc_map.intersects(addr, size) ) continue;
      allocator->mark_extent_used(addr, size);
      alloc_map.insert(addr, size);
      avail -= size;
    }
    ASSERT_EQ(avail, allocator->get_available_size());

    for (auto p : alloc_map) {
      free(p.first, p.second);
      avail += p.second;
      ASSERT_EQ(avail, allocator->get_available_size());
    }
    alloc_map.clear();
    ASSERT_EQ(capacity, allocator->get_available_size());

    for (int i = 0; i < 100; i++) {
      auto addr = get_random_addr(block_size, capacity);
      auto size = get_random_addr(block_size, capacity) % (4 << 20);
      if (addr + size > capacity || size == 0 ||
	  alloc_map.intersects(addr, size) ) continue;
      allocator->mark_extent_used(addr, size);
      alloc_map.insert(addr, size);
      avail -= size;
    }

    for (int i = 0; i < 50; i++) {
      free((*alloc_map.begin()).first, (*alloc_map.begin()).second);
      avail += (*alloc_map.begin()).second;
      alloc_map.erase((*alloc_map.begin()).first, (*alloc_map.begin()).second);
      ASSERT_EQ(avail, allocator->get_available_size());

      auto addr = get_random_addr(block_size, capacity);
      auto size = get_random_addr(block_size, capacity) % (4 << 20);
      if (addr + size > capacity || size == 0 ||
	  alloc_map.intersects(addr, size) ) continue;
      allocator->mark_extent_used(addr, size);
      alloc_map.insert(addr, size);
      avail -= size;
    }
    ASSERT_EQ(avail, allocator->get_available_size());
  }
}

INSTANTIATE_TEST_SUITE_P(
  allocator_test,
  allocator_test_t,
  ::testing::Values("avl"));
