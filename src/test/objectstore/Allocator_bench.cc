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
#include "os/bluestore/AllocatorBase.h"

#include <boost/random/mersenne_twister.hpp>
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
  void doOverwriteMPCTest(size_t thread_count,
    uint64_t capacity, uint64_t prefill,
    uint64_t overwrite);
  void doOverwriteMPC2Test(size_t thread_count,
    uint64_t capacity, uint64_t prefill,
    uint64_t overwrite,
    float extra = 0.05);
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
  boost::uniform_int<uint64_t> u1;

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

    size_t pos = (u1(rng) % size) + tail;
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
  std::cout << "Executed in " << ceph_clock_now() - start << std::endl;

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

TEST_P(AllocTest, test_alloc_bench_seq_interleaving)
{
  // by adjusting capacity and mempool dump output analysis one can
  // estimate max RAM usage for specific allocator's implementation with
  // real-life disk size, e.g. 8TB or 16 TB.
  // The current capacity is left pretty small to avoid huge RAM utilization
  // when using non-effective allocators (e.g. AVL) and hence let the test
  // case run perfectly on week H/W.
  uint64_t capacity = uint64_t(1024) * 1024 * 1024 * 128; //128GB
  uint64_t alloc_unit = 4096;
  uint64_t want_size = alloc_unit;
  PExtentVector allocated, tmp;

  init_alloc(capacity, alloc_unit);
  alloc->init_add_free(0, capacity);

  utime_t start = ceph_clock_now();
  alloc->init_rm_free(0, capacity);
  std::cout << "Executed in " << ceph_clock_now() - start << std::endl;

  std::cout << "releasing..." << std::endl;
  for (size_t i = 0; i < capacity; i += want_size * 2)
  {
    interval_set<uint64_t> release_set;
    release_set.insert(i, want_size);
    alloc->release(release_set);
    if (0 == (i % (1 * 1024 * _1m))) {
      std::cout << "release " << i / 1024 / 1024 << " mb of "
	<< capacity / 1024 / 1024 << std::endl;
    }
  }
  std::cout << "Executed in " << ceph_clock_now() - start << std::endl;
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

struct OverwriteTextContext : public Thread {
  size_t idx = 0;
  AllocTracker* tracker;
  Allocator* alloc = nullptr;
  size_t r_count = 0;
  size_t a_count = 0;
  uint64_t ae_count = 0;
  uint64_t re_count = 0;

  uint64_t how_many = 0;
  uint64_t alloc_unit = 0;
  timespan r_time;
  timespan a_time;

  OverwriteTextContext(size_t _idx,
    AllocTracker* at,
    Allocator* a,
    uint64_t want,
    uint64_t unit) :
    idx(_idx), tracker(at), alloc(a), how_many(want), alloc_unit(unit)
  {
  }

  void build_histogram() {
    const size_t num_buckets = 8;
    AllocatorBase::FreeStateHistogram hist(num_buckets);
    alloc->foreach(
      [&](size_t off, size_t len) {
	hist.record_extent(uint64_t(alloc_unit), off, len);
      });

    hist.foreach(
      [&](uint64_t max_len, uint64_t total, uint64_t aligned, uint64_t units) {
	uint64_t a_bytes = units * alloc_unit;
	std::cout << "<=" << max_len
	  << " -> " << total << "/" << aligned
	  << " a_bytes " << a_bytes
	  << " " << ((float)a_bytes / alloc->get_capacity() * 100) << "%"
	  << std::endl;
      });
  }

  void* entry() override {
    PExtentVector allocated, tmp;
    gen_type rng(time(NULL));
    boost::uniform_int<> u1(0, 9); // 4K-2M
    boost::uniform_int<> u2(0, 9); // 4K-2M

    r_time = ceph::make_timespan(0);
    a_time = ceph::make_timespan(0);

    for (uint64_t i = 0; i < how_many; )
    {
      uint64_t want_release = alloc_unit << u2(rng);
      uint64_t released = 0;
      interval_set<uint64_t> release_set;
      do {
	uint64_t o = 0;
	uint32_t l = 0;
	if (!tracker->pop_random(rng, &o, &l, want_release - released)) {
	  break;
	}
	release_set.insert(o, l);
	released += l;
      } while (released < want_release);

      uint32_t want = alloc_unit << u1(rng);
      tmp.clear();
      auto t0 = mono_clock::now();
      auto r = alloc->allocate(want, alloc_unit, 0, 0, &tmp);
      a_count++;
      ae_count += tmp.size();
      a_time += mono_clock::now() - t0;
      if (r != want) {
	std::cout << "Can't allocate more space, stopping." << std::endl;
	break;
      }
      i += r;

      for (auto a : tmp) {
	bool full = !tracker->push(a.offset, a.length);
	EXPECT_EQ(full, false);
      }
      {
	auto t0 = mono_clock::now();
	alloc->release(release_set);
	r_count++;
	r_time += mono_clock::now() - t0;
	re_count += release_set.num_intervals();
      }
      if (0 == (i % (1 * 1024 * _1m))) {
	std::cout << idx << ">> reuse " << i / 1024 / 1024 << " mb of "
	  << how_many / 1024 / 1024 << std::endl;
      }
    }
    return nullptr;
  }
};

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
  std::cout << "Executed prefill in " << ceph_clock_now() - start << std::endl;
  cap = overwrite;
  OverwriteTextContext ctx(0, &at, alloc.get(), cap, alloc_unit);

  start = ceph_clock_now();
  ctx.entry();

  std::cout << "Executed in " << ceph_clock_now() - start
	    << " alloc:" << ctx.a_count << "/" << ctx.ae_count << " in " << ctx.a_time
	    << " release:" << ctx.r_count << "/" << ctx.re_count << " in " << ctx.r_time
	    << std::endl;
  std::cout<<"Avail "<< alloc->get_free() / _1m << " MB" << std::endl;

  dump_mempools();
}

void AllocTest::doOverwriteMPCTest(size_t thread_count,
				   uint64_t capacity, uint64_t prefill,
				   uint64_t overwrite)
{
  uint64_t alloc_unit = 4096;
  PExtentVector tmp;
  std::vector<AllocTracker*> at;
  std::vector<OverwriteTextContext*> ctx;

  init_alloc(capacity, alloc_unit);
  alloc->init_add_free(0, capacity);

  at.resize(thread_count);
  ctx.resize(thread_count);
  for (size_t i = 0; i < thread_count; i++) {
    at[i] = new AllocTracker(capacity, alloc_unit);
    ctx[i] = new OverwriteTextContext(i, at[i], alloc.get(), overwrite, alloc_unit);
  }

  gen_type rng(time(NULL));
  boost::uniform_int<> u1(0, 9); // 4K-2M

  utime_t start = ceph_clock_now();
  // allocate %% + 10% of the capacity
  float extra = 0.1;
  auto cap = prefill * (1 + extra);

  uint64_t idx = 0;
  for (uint64_t i = 0; i < cap; )
  {
    uint32_t want = alloc_unit << u1(rng);
    tmp.clear();
    auto r = alloc->allocate(want, alloc_unit, 0, 0, &tmp);
    if (r < want) {
      break;
    }
    i += r;

    for (auto a : tmp) {
      bool full = !at[idx]->push(a.offset, a.length);
      EXPECT_EQ(full, false);
    }
    if (0 == (i % (1 * 1024 * _1m))) {
      std::cout << "alloc " << i / 1024 / 1024 << " mb of "
	<< cap / 1024 / 1024 << std::endl;
    }
    idx = (idx + 1) % thread_count;
  }

  // do release extra space to introduce some fragmentation
  cap = prefill * extra;
  idx = 0;
  for (uint64_t i = 0; i < cap; )
  {
    uint64_t want_release = alloc_unit << u1(rng);
    uint64_t released = 0;
    interval_set<uint64_t> release_set;
    do {
      uint64_t o = 0;
      uint32_t l = 0;
      if (!at[idx]->pop_random(rng, &o, &l, want_release - released)) {
	break;
      }
      release_set.insert(o, l);
      released += l;
    } while (released < want_release);
    alloc->release(release_set);
    i += released;
    if (0 == (i % (1 * 1024 * _1m))) {
      std::cout << "release " << i / 1024 / 1024 << " mb of "
	<< cap / 1024 / 1024 << std::endl;
    }
    idx = (idx + 1) % thread_count;
  }
  std::cout << "Executed prefill in " << ceph_clock_now() - start
	    << " Fragmentation:" << alloc->get_fragmentation_score()
            << std::endl;
  ctx[0]->build_histogram();

  start = ceph_clock_now();
  for (size_t i = 0; i < thread_count; i++) {
    ctx.at(i)->create(stringify(i).c_str());
  }

  for (size_t i = 0; i < thread_count; i++) {
    ctx.at(i)->join();
  }
  std::cout << "Executed in " << ceph_clock_now() - start
    << std::endl;
  std::cout << "Avail " << alloc->get_free() / _1m << " MB"
            << " Fragmentation:" << alloc->get_fragmentation_score()
            << std::endl;
  for (size_t i = 0; i < thread_count; i++) {
    std::cout << "alloc/release stats for " << i
      << " alloc:" << ctx.at(i)->a_count << "/" << ctx.at(i)->ae_count << " in " << ctx.at(i)->a_time
      << " release:" << ctx.at(i)->r_count << "/" << ctx.at(i)->re_count << " in " << ctx.at(i)->r_time
      << std::endl;
  }
  ctx[0]->build_histogram();
  dump_mempools();
  for (size_t i = 0; i < thread_count; i++) {
    delete at[i];
    delete ctx[i];
  }
}

struct OverwriteTextContext2 : public OverwriteTextContext {

  using OverwriteTextContext::OverwriteTextContext;
  void* entry() override {
    PExtentVector allocated, tmp;
    gen_type rng(time(NULL));
    boost::uniform_int<> u1(1, 16); // alloc_unit * u1 => 4K-64K

    r_time = ceph::make_timespan(0);
    a_time = ceph::make_timespan(0);
    uint64_t processed = 0;
    auto t00 = ceph_clock_now();
    for (uint64_t i = 0; i < how_many; )
    {
      int64_t want = alloc_unit * u1(rng);
      int64_t released = 0;
      interval_set<uint64_t> release_set;
      do {
	uint64_t o = 0;
	uint32_t l = 0;
	if (!tracker->pop_random(rng, &o, &l, want - released)) {
	  break;
	}
	release_set.insert(o, l);
	released += l;
      } while (released < want);
      tmp.clear();
      auto t0 = mono_clock::now();
      auto r = alloc->allocate(want, alloc_unit, 0, 0, &tmp);
      a_count++;
      ae_count += tmp.size();
      a_time += mono_clock::now() - t0;
      if (r != want) {
	std::cout << "Can't allocate more space, stopping." << std::endl;
	break;
      }
      i += r;

      for (auto a : tmp) {
	bool full = !tracker->push(a.offset, a.length);
	EXPECT_EQ(full, false);
      }
      {
	auto t0 = mono_clock::now();
	alloc->release(release_set);
	r_count++;
	r_time += mono_clock::now() - t0;
	re_count += release_set.num_intervals();
      }
      auto processed0 = processed;
      processed += want;
      auto _1g = 1024 * _1m;
      if (processed / _1g != processed0 / _1g) {
	std::cout << idx << ">> reuse " << i / 1024 / 1024 << " mb of "
	  << how_many / 1024 / 1024 << std::endl;

      }
      auto c = alloc->get_capacity();
      bool capacity_written = (processed / c) != (processed0 / c);
      if (capacity_written) {
	std::cout << "> Single iteration writing completed in " << (ceph_clock_now() - t00)
		  << " alloc/release stats for " << idx
		  << " alloc:" << a_count << "/" << ae_count << " in " << a_time
		  << " release:" << r_count << "/" << re_count << " in " << r_time
		  << std::endl;
	a_count = 0;
	ae_count = 0;
	r_count = 0;
	re_count = 0;
	r_time = ceph::make_timespan(0);
	a_time = ceph::make_timespan(0);
	if (idx == 0) {
	  std::cout << " Fragmentation: " << alloc->get_fragmentation_score()
		    << std::endl;
	  build_histogram();
	}
	t00 = ceph_clock_now();
      }
    }
    return nullptr;
  }
};

void AllocTest::doOverwriteMPC2Test(size_t thread_count,
  uint64_t capacity, uint64_t prefill,
  uint64_t overwrite,
  float extra)
{
  uint64_t alloc_unit = 4096;
  PExtentVector tmp;
  std::vector<AllocTracker*> at;
  std::vector<OverwriteTextContext2*> ctx;

  init_alloc(capacity, alloc_unit);
  alloc->init_add_free(0, capacity);

  at.resize(thread_count);
  ctx.resize(thread_count);
  for (size_t i = 0; i < thread_count; i++) {
    at[i] = new AllocTracker(capacity, alloc_unit);
    ctx[i] = new OverwriteTextContext2(i, at[i], alloc.get(), overwrite, alloc_unit);
  }

  gen_type rng(time(NULL));
  boost::uniform_int<> u1(8, 10); // 4096 << u1 => 1-4M chunks used for prefill
  boost::uniform_int<> u2(1, 512); // 4096 * u2 => 4K-2M chunks used for overwrite

  utime_t start = ceph_clock_now();
  // allocate %% + extra% of the capacity
  float cap = prefill + capacity * extra;

  uint64_t idx = 0;
  for (uint64_t i = 0; i < cap; )
  {
    uint32_t want = alloc_unit << u1(rng);
    tmp.clear();
    auto r = alloc->allocate(want, alloc_unit, 0, 0, &tmp);
    if (r < want) {
      break;
    }
    i += r;

    for (auto a : tmp) {
      bool full = !at[idx]->push(a.offset, a.length);
      EXPECT_EQ(full, false);
    }
    if (0 == (i % (1 * 1024 * _1m))) {
      std::cout << "alloc " << i / 1024 / 1024 << " mb of "
	<< cap / 1024 / 1024 << std::endl;
    }
    idx = (idx + 1) % thread_count;
  }
  // do release extra space to introduce some fragmentation
  cap = capacity * extra;
  idx = 0;
  for (uint64_t i = 0; i < (uint64_t)cap; )
  {
    uint64_t want_release = alloc_unit * u2(rng);
    uint64_t released = 0;
    interval_set<uint64_t> release_set;
    do {
      uint64_t o = 0;
      uint32_t l = 0;
      if (!at[idx]->pop_random(rng, &o, &l, want_release - released)) {
	break;
      }
      release_set.insert(o, l);
      released += l;
    } while (released < want_release);
    alloc->release(release_set);
    i += released;
    if (0 == (i % (1 * 1024 * _1m))) {
      std::cout << "release " << i / 1024 / 1024 << " mb of "
	<< cap / 1024 / 1024 << std::endl;
    }
    idx = (idx + 1) % thread_count;
  }

  std::cout << "Executed prefill in " << ceph_clock_now() - start
    << " Fragmentation:" << alloc->get_fragmentation_score()
    << std::endl;
  ctx[0]->build_histogram();

  start = ceph_clock_now();
  for (size_t i = 0; i < thread_count; i++) {
    ctx[i]->create(stringify(i).c_str());
  }

  for (size_t i = 0; i < thread_count; i++) {
    ctx.at(i)->join();
  }
  std::cout << "Executed in " << ceph_clock_now() - start
    << std::endl;
  std::cout << "Avail " << alloc->get_free() / _1m << " MB"
    << " Fragmentation:" << alloc->get_fragmentation_score()
    << std::endl;
  ctx[0]->build_histogram();

  dump_mempools();
  for (size_t i = 0; i < thread_count; i++) {
    delete at[i];
    delete ctx[i];
  }
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

TEST_P(AllocTest, test_alloc_bench_50_300_x2)
{
  // skipping for legacy and slow code
  if ((GetParam() == string("stupid"))) {
    GTEST_SKIP() << "skipping for specific allocators";
  }
  uint64_t capacity = uint64_t(1024) * 1024 * 1024 * 128;
  auto prefill = capacity / 2;
  auto overwrite = capacity * 3;
  doOverwriteMPCTest(2, capacity, prefill, overwrite);
}

/*
* The following benchmark test simulates small block overwrites over highly
*  utilized disk space prefilled with large extents. Overwrites are performed
* from two concurring threads accessing the same allocator.
* Detailed scenario:
* 1. Prefill (single threaded):
* 1.1. Fill 95% of the space with 1M-4M chunk allocations
* 1.2. Release 5% of the allcoated space using random extents of 4K-2M bytes
* 2. Random overwrite using 2 threads
* 2.1 Deallocate random sub-extent of size randomly selected within [4K-64K] range
* 2.2. Allocate random extent of size rwithin [4K-64K] range.
* 2.3. Repeat 2.1-2.2 until total newly allocated bytes are equal to 500% of
* the original disk space
*
* Pay attention to the resulting fragmentation score and histogram, time taken and
* bluestore_alloc mempool stats
*
*/
TEST_P(AllocTest, test_alloc_bench2_90_500_x2)
{
  // skipping for legacy and slow code
  if ((GetParam() == string("stupid"))) {
    GTEST_SKIP() << "skipping for specific allocators";
  }
  uint64_t capacity = uint64_t(1024) * 1024 * 1024 * 128;
  auto prefill = capacity * 9 / 10;
  auto overwrite = capacity * 5;
  doOverwriteMPC2Test(2, capacity, prefill, overwrite);
}

TEST_P(AllocTest, test_alloc_bench2_20_500_x2)
{
  // skipping for legacy and slow code
  if ((GetParam() == string("stupid"))) {
    GTEST_SKIP() << "skipping for specific allocators";
  }
  uint64_t capacity = uint64_t(1024) * 1024 * 1024 * 128;
  auto prefill = capacity * 2 / 10;
  auto overwrite = capacity * 5;
  doOverwriteMPC2Test(2, capacity, prefill, overwrite, 0.05);
}

TEST_P(AllocTest, test_alloc_bench2_50_500_x2)
{
  // skipping for legacy and slow code
  if ((GetParam() == string("stupid"))) {
    GTEST_SKIP() << "skipping for specific allocators";
  }
  uint64_t capacity = uint64_t(1024) * 1024 * 1024 * 128;
  auto prefill = capacity * 5 / 10;
  auto overwrite = capacity * 5;
  doOverwriteMPC2Test(2, capacity, prefill, overwrite, 0.05);
}

TEST_P(AllocTest, test_alloc_bench2_75_500_x2)
{
  // skipping for legacy and slow code
  if ((GetParam() == string("stupid"))) {
    GTEST_SKIP() << "skipping for specific allocators";
  }
  uint64_t capacity = uint64_t(1024) * 1024 * 1024 * 128;
  auto prefill = capacity * 75 / 100;
  auto overwrite = capacity * 15;
  doOverwriteMPC2Test(2, capacity, prefill, overwrite, 0.05);
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
  ::testing::Values("stupid", "bitmap", "avl", "hybrid", "btree", "hybrid_btree2"));
