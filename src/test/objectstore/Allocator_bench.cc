// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * In memory space allocator benchmarks.
 * Author: Igor Fedotov, ifedotov@suse.com
 */
#include <iostream>
#include <boost/scoped_ptr.hpp>
#include <gtest/gtest.h>

#include "common/Cond.h"
#include "common/errno.h"
#include "include/stringify.h"
#include "include/Context.h"
#include "os/bluestore/Allocator.h"

#include <boost/random/uniform_int.hpp>
typedef boost::mt11213b gen_type;

#include "common/debug.h"
#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_

using namespace std;

class AllocTest : public ::testing::TestWithParam<const char*> {

public:
  boost::scoped_ptr<Allocator> alloc;
  AllocTest(): alloc(0) { }
  void init_alloc(int64_t size, uint64_t min_alloc_size) {
    std::cout << "Creating alloc type " << string(GetParam()) << " \n";
    alloc.reset(Allocator::create(g_ceph_context, GetParam(), size,
				  min_alloc_size));
  }

  void init_close() {
    alloc.reset(0);
  }
  void doOverwriteTest(uint64_t capacity, uint64_t prefill,
    uint64_t overwrite);
};

const uint64_t _1m = 1024 * 1024;

void dump_mempools()
{
  ostringstream ostr;
  Formatter* f = Formatter::create("json-pretty", "json-pretty", "json-pretty");
  ostr << "Mempools: ";
  f->open_object_section("mempools");
  mempool::dump(f);
  f->close_section();
  f->flush(ostr);
  delete f;
  ldout(g_ceph_context, 0) << ostr.str() << dendl;
}

class AllocTracker
{
  std::vector<uint64_t> allocations;
  uint64_t head = 0;
  uint64_t tail = 0;
  uint64_t size = 0;
  boost::uniform_int<> u1;

public:
  AllocTracker(uint64_t capacity, uint64_t alloc_unit)
    : u1(0, capacity)
  {
    ceph_assert(alloc_unit >= 0x100);
    ceph_assert(capacity <= (uint64_t(1) << 48)); // we use 5 octets (bytes 1 - 5) to store
				 // offset to save the required space.
				 // This supports capacity up to 281 TB

    allocations.resize(capacity / alloc_unit);
  }
  inline uint64_t get_head() const
  {
    return head;
  }

  inline uint64_t get_tail() const
  {
    return tail;
  }

  bool push(uint64_t offs, uint32_t len)
  {
    ceph_assert((len & 0xff) == 0);
    ceph_assert((offs & 0xff) == 0);
    ceph_assert((offs & 0xffff000000000000) == 0);

    if (head + 1 == tail)
      return false;
    uint64_t val = (offs << 16) | (len >> 8);
    allocations[head++] = val;
    head %= allocations.size();
    ++size;
    return true;
  }
  bool pop(uint64_t* offs, uint32_t* len)
  {
    if (size == 0)
      return false;
    uint64_t val = allocations[tail++];
    *len = uint64_t((val & 0xffffff) << 8);
    *offs = (val >> 16) & ~uint64_t(0xff);
    tail %= allocations.size();
    --size;
    return true;
  }
  bool pop_random(gen_type& rng, uint64_t* offs, uint32_t* len,
    uint32_t max_len = 0)
  {
    if (size == 0)
      return false;

    uint64_t pos = (u1(rng) % size) + tail;
    pos %= allocations.size();
    uint64_t val = allocations[pos];
    *len = uint64_t((val & 0xffffff) << 8);
    *offs = (val >> 16) & ~uint64_t(0xff);
    if (max_len && *len > max_len) {
      val = ((*offs + max_len) << 16) | ((*len - max_len) >> 8);
      allocations[pos] = val;
      *len = max_len;
    } else {
      allocations[pos] = allocations[tail++];
      tail %= allocations.size();
      --size;
    }
    return true;
  }
};

TEST_P(AllocTest, test_alloc_bench_seq)
{
  uint64_t capacity = uint64_t(1024) * 1024 * 1024 * 1024;
  uint64_t alloc_unit = 4096;
  uint64_t want_size = alloc_unit;
  PExtentVector allocated, tmp;

  init_alloc(capacity, alloc_unit);
  alloc->init_add_free(0, capacity);

  utime_t start = ceph_clock_now();
  for (uint64_t i = 0; i < capacity; i += want_size)
  {
    tmp.clear();
    EXPECT_EQ(static_cast<int64_t>(want_size),
	      alloc->allocate(want_size, alloc_unit, 0, 0, &tmp));
    if (0 == (i % (1 * 1024 * _1m))) {
      std::cout << "alloc " << i / 1024 / 1024 << " mb of "
        << capacity / 1024 / 1024 << std::endl;
    }
  }

  std::cout << "releasing..." << std::endl;
  for (size_t i = 0; i < capacity; i += want_size)
  {
    interval_set<uint64_t> release_set;
    release_set.insert(i, want_size);
    alloc->release(release_set);
    if (0 == (i % (1 * 1024 * _1m))) {
      std::cout << "release " << i / 1024 / 1024 << " mb of "
        << capacity / 1024 / 1024 << std::endl;
    }
  }
  std::cout<<"Executed in "<< ceph_clock_now() - start << std::endl;
  dump_mempools();
}

TEST_P(AllocTest, test_alloc_bench)
{
  uint64_t capacity = uint64_t(1024) * 1024 * 1024 * 1024;
  uint64_t alloc_unit = 4096;
  PExtentVector allocated, tmp;
  AllocTracker at(capacity, alloc_unit);

  init_alloc(capacity, alloc_unit);
  alloc->init_add_free(0, capacity);

  gen_type rng(time(NULL));
  boost::uniform_int<> u1(0, 9); // 4K-2M
  boost::uniform_int<> u2(0, 7); // 4K-512K

  utime_t start = ceph_clock_now();
  for (uint64_t i = 0; i < capacity * 2; )
  {
    uint32_t want = alloc_unit << u1(rng);

    tmp.clear();
    auto r = alloc->allocate(want, alloc_unit, 0, 0, &tmp);
    if (r < want) {
      break;
    }
    i += r;

    for(auto a : tmp) {
      bool full = !at.push(a.offset, a.length);
      EXPECT_EQ(full, false);
    }
    uint64_t want_release = alloc_unit << u2(rng);
    uint64_t released = 0;
    do {
      uint64_t o = 0;
      uint32_t l = 0;
      interval_set<uint64_t> release_set;
      if (!at.pop_random(rng, &o, &l, want_release - released)) {
	break;
      }
      release_set.insert(o, l);
      alloc->release(release_set);
      released += l;
    } while (released < want_release);

    if (0 == (i % (1 * 1024 * _1m))) {
      std::cout << "alloc " << i / 1024 / 1024 << " mb of "
        << capacity / 1024 / 1024 << std::endl;
    }
  }
  std::cout<<"Executed in "<< ceph_clock_now() - start << std::endl;
  std::cout<<"Avail "<< alloc->get_free() / _1m << " MB" << std::endl;
  dump_mempools();
}

void AllocTest::doOverwriteTest(uint64_t capacity, uint64_t prefill,
  uint64_t overwrite)
{
  uint64_t alloc_unit = 4096;
  PExtentVector allocated, tmp;
  AllocTracker at(capacity, alloc_unit);

  init_alloc(capacity, alloc_unit);
  alloc->init_add_free(0, capacity);

  gen_type rng(time(NULL));
  boost::uniform_int<> u1(0, 9); // 4K-2M
  boost::uniform_int<> u2(0, 9); // 4K-512K

  utime_t start = ceph_clock_now();
  // allocate 90% of the capacity
  auto cap = prefill;
  for (uint64_t i = 0; i < cap; )
  {
    uint32_t want = alloc_unit << u1(rng);
    tmp.clear();
    auto r = alloc->allocate(want, alloc_unit, 0, 0, &tmp);
    if (r < want) {
      break;
    }
    i += r;

    for(auto a : tmp) {
      bool full = !at.push(a.offset, a.length);
      EXPECT_EQ(full, false);
    }
    if (0 == (i % (1 * 1024 * _1m))) {
      std::cout << "alloc " << i / 1024 / 1024 << " mb of "
        << cap / 1024 / 1024 << std::endl;
    }
  }

  cap = overwrite;
  for (uint64_t i = 0; i < cap; )
  {
    uint64_t want_release = alloc_unit << u2(rng);
    uint64_t released = 0;
    do {
      uint64_t o = 0;
      uint32_t l = 0;
      interval_set<uint64_t> release_set;
      if (!at.pop_random(rng, &o, &l, want_release - released)) {
	break;
      }
      release_set.insert(o, l);
      alloc->release(release_set);
      released += l;
    } while (released < want_release);

    uint32_t want = alloc_unit << u1(rng);
    tmp.clear();
    auto r = alloc->allocate(want, alloc_unit, 0, 0, &tmp);
    if (r != want) {
      std::cout<<"Can't allocate more space, stopping."<< std::endl;
      break;
    }
    i += r;

    for(auto a : tmp) {
      bool full = !at.push(a.offset, a.length);
      EXPECT_EQ(full, false);
    }

    if (0 == (i % (1 * 1024 * _1m))) {
      std::cout << "reuse " << i / 1024 / 1024 << " mb of "
        << cap / 1024 / 1024 << std::endl;
    }
  }
  std::cout<<"Executed in "<< ceph_clock_now() - start << std::endl;
  std::cout<<"Avail "<< alloc->get_free() / _1m << " MB" << std::endl;

  dump_mempools();
}

TEST_P(AllocTest, test_alloc_bench_90_300)
{
  uint64_t capacity = uint64_t(1024) * 1024 * 1024 * 1024;
  auto prefill = capacity - capacity / 10;
  auto overwrite = capacity * 3;
  doOverwriteTest(capacity, prefill, overwrite);
}

TEST_P(AllocTest, test_alloc_bench_50_300)
{
  uint64_t capacity = uint64_t(1024) * 1024 * 1024 * 1024;
  auto prefill = capacity / 2;
  auto overwrite = capacity * 3;
  doOverwriteTest(capacity, prefill, overwrite);
}

TEST_P(AllocTest, test_alloc_bench_10_300)
{
  uint64_t capacity = uint64_t(1024) * 1024 * 1024 * 1024;
  auto prefill = capacity / 10;
  auto overwrite = capacity * 3;
  doOverwriteTest(capacity, prefill, overwrite);
}

TEST_P(AllocTest, mempoolAccounting)
{
  uint64_t bytes = mempool::bluestore_alloc::allocated_bytes();
  uint64_t items = mempool::bluestore_alloc::allocated_items();

  uint64_t alloc_size = 4 * 1024;
  uint64_t capacity = 512ll * 1024 * 1024 * 1024;
  Allocator* alloc = Allocator::create(g_ceph_context, GetParam(),
				       capacity, alloc_size);
  ASSERT_NE(alloc, nullptr);
  alloc->init_add_free(0, capacity);

  std::map<uint32_t, PExtentVector> all_allocs;
  for (size_t i = 0; i < 10000; i++) {
    PExtentVector tmp;
    alloc->allocate(alloc_size, alloc_size, 0, 0, &tmp);
    all_allocs[rand()] = tmp;
    tmp.clear();
    alloc->allocate(alloc_size, alloc_size, 0, 0, &tmp);
    all_allocs[rand()] = tmp;
    tmp.clear();

    auto it = all_allocs.upper_bound(rand());
    if (it != all_allocs.end()) {
      alloc->release(it->second);
      all_allocs.erase(it);
    }
  }

  delete(alloc);
  ASSERT_EQ(mempool::bluestore_alloc::allocated_bytes(), bytes);
  ASSERT_EQ(mempool::bluestore_alloc::allocated_items(), items);
}

INSTANTIATE_TEST_SUITE_P(
  Allocator,
  AllocTest,
  ::testing::Values("stupid", "bitmap", "avl", "hybrid", "btree"));
