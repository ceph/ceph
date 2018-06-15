// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Bitmap allocator fragmentation benchmarks.
 * Author: Adam Kupczyk, akupczyk@redhat.com
 */
#include <iostream>
#include <boost/scoped_ptr.hpp>
#include <gtest/gtest.h>
#include <boost/random/triangle_distribution.hpp>

#include "common/Mutex.h"
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

struct Scenario {
  uint64_t capacity;
  uint64_t alloc_unit;
  double high_mark;
  double low_mark;
  double leakness;
  uint32_t repeats;
};

void PrintTo(const Scenario& s, ::std::ostream* os)
{
  *os << "(capacity=" << s.capacity;
  *os << "G, alloc_unit=" << s.alloc_unit;
  *os << ", high_mark=" << s.high_mark;
  *os << ", low_mark=" << s.low_mark;
  *os << ", leakness=" << s.leakness;
  *os << ", repeats=" << s.repeats << ")";
}

#if GTEST_HAS_PARAM_TEST
class AllocTracker;
class AllocTest : /*public ::testing::Test,*/ public ::testing::TestWithParam<Scenario> {
protected:
  boost::scoped_ptr<AllocTracker> at;
  gen_type rng;
public:
  boost::scoped_ptr<Allocator> alloc;
  AllocTest(): alloc(nullptr) {}
  void init_alloc(int64_t size, uint64_t min_alloc_size);
  void init_close();
  void doAgeingTest(std::function<uint32_t()> size_generator,
		    uint64_t capacity, uint32_t alloc_unit,
		    uint64_t high_mark, uint64_t low_mark, uint32_t iterations, double leak_factor = 0);

  uint64_t capacity;
  uint32_t alloc_unit;

  uint64_t level = 0;
  uint64_t allocs = 0;
  uint64_t fragmented = 0;
  uint64_t fragments = 0;
  uint64_t total_fragments = 0;

  void do_fill(uint64_t high_mark, std::function<uint32_t()> size_generator, double leak_factor = 0);
  void do_free(uint64_t low_mark);

  uint32_t free_random();

};

const uint64_t _1m = 1024 * 1024;
const uint64_t _1G = 1024 * 1024 * 1024;

const uint64_t _2m = 2 * 1024 * 1024;

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
  std::vector<bluestore_pextent_t> allocations;
  uint64_t size = 0;

public:
  bool push(uint64_t offs, uint32_t len)
  {
    assert(len != 0);
    if (size + 1 > allocations.size())
      allocations.resize(size + 100);
    allocations[size++] = bluestore_pextent_t(offs, len);
    return true;
  }

  bool pop_random(gen_type& rng, uint64_t* offs, uint32_t* len,
    uint32_t max_len = 0)
  {
    if (size == 0)
      return false;
    uint64_t pos = rng() % size;
    *len = allocations[pos].length;
    *offs = allocations[pos].offset;

    if (max_len && *len > max_len) {
      allocations[pos].length = *len - max_len;
      allocations[pos].offset = *offs + max_len;
      *len = max_len;
    } else {
      allocations[pos] = allocations[size-1];
      --size;
    }
    return true;
  }
};


void AllocTest::init_alloc(int64_t size, uint64_t min_alloc_size) {
  this->capacity = size;
  this->alloc_unit = min_alloc_size;
  rng.seed(0);
  alloc.reset(Allocator::create(g_ceph_context, "bitmap", size,
				min_alloc_size));
  at.reset(new AllocTracker());
}

void AllocTest::init_close() {
    alloc.reset(0);
    at.reset(nullptr);
}

uint32_t AllocTest::free_random() {
  uint64_t o = 0;
  uint32_t l = 0;
  interval_set<uint64_t> release_set;
  if (!at->pop_random(rng, &o, &l)) {
    //empty?
    return 0;
  }
  release_set.insert(o, l);
  alloc->release(release_set);
  level -= l;
  return l;
}


void AllocTest::do_fill(uint64_t high_mark, std::function<uint32_t()> size_generator, double leak_factor) {
  assert (leak_factor >= 0);
  assert (leak_factor < 1);
  uint32_t leak_level = leak_factor * std::numeric_limits<uint32_t>::max();
  PExtentVector tmp;
  while (level < high_mark)
  {
    uint32_t want = size_generator();
    tmp.clear();
    utime_t start = ceph_clock_now();
    auto r = alloc->allocate(want, alloc_unit, 0, 0, &tmp);
    //std::cout << "want=" << want << " time=" << ceph_clock_now() - start << std::endl;
    if (r < want) {
      break;
    }
    level += r;
    for(auto a : tmp) {
      bool full = !at->push(a.offset, a.length);
      EXPECT_EQ(full, false);
    }
    allocs++;
    if (tmp.size() > 1) {
      fragmented ++;
      total_fragments += r;
      fragments += tmp.size();
    }
    if (leak_level > 0) {
      for (size_t i=0; i<tmp.size(); i++) {
	if (uint32_t(rng()) < leak_level) {
	  free_random();
	}
      }
    }
  }
}

void AllocTest::do_free(uint64_t low_mark) {
  while (level > low_mark)
  {
    if (free_random() == 0)
      break;
  }
}

void AllocTest::doAgeingTest(
    std::function<uint32_t()> size_generator,
    uint64_t capacity, uint32_t alloc_unit,
    uint64_t high_mark, uint64_t low_mark, uint32_t iterations, double leak_factor)
{
  assert(isp2(alloc_unit));
  g_ceph_context->_conf->bdev_block_size = alloc_unit;
  PExtentVector allocated, tmp;
  init_alloc(capacity, alloc_unit);
  alloc->init_add_free(0, capacity);

  utime_t start = ceph_clock_now();
  allocs = 0;
  fragmented = 0;
  fragments = 0;
  total_fragments = 0;
  std::cout << "INITIAL FILL" << std::endl;
  do_fill(high_mark, size_generator, leak_factor); //initial fill with data
  std::cout << "    fragmented allocs=" << 100.0 * fragmented / allocs << "%" <<
	" #frags=" << ( fragmented != 0 ? double(fragments) / fragmented : 0 )<<
	" time=" << (ceph_clock_now() - start) * 1000 << "ms" << std::endl;

  for (uint32_t i=0; i < iterations; i++)
  {
    allocs = 0;
    fragmented = 0;
    fragments = 0;
    total_fragments = 0;

    uint64_t level_previous = level;
    start = ceph_clock_now();
    std::cout << "ADDING CAPACITY " << i + 1 << std::endl;
    do_free(low_mark); //simulates adding new capacity to cluster
    std::cout << "    level change: " <<
	double(level_previous) / capacity << "% -> " <<
	double(level) / capacity << "% time=" <<
	(ceph_clock_now() - start) * 1000 << "ms" << std::endl;

    start = ceph_clock_now();
    std::cout << "APPENDING " << i + 1 << std::endl;
    do_fill(high_mark, size_generator, leak_factor); //only creating elements
    std::cout << "    fragmented allocs=" << 100.0 * fragmented / allocs << "%" <<
  	" #frags=" << ( fragmented != 0 ? double(fragments) / fragmented : 0 )<<
  	" time=" << (ceph_clock_now() - start) * 1000 << "ms" << std::endl;

  }
}

TEST_P(AllocTest, test_alloc_triangle_0_8M_16M)
{
  boost::triangle_distribution<double> D(1, (8 * 1024 * 1024) , (16 * 1024 * 1024) );

  Scenario s = GetParam();
  PrintTo(s, &std::cout);
  std::cout << std::endl;

  auto size_generator = [&]() -> uint32_t {
    return (uint32_t(D(rng)) + s.alloc_unit) & ~(s.alloc_unit - 1);
  };
  doAgeingTest(size_generator, s.capacity * _1G, s.alloc_unit,
	       s.high_mark * s.capacity * _1G,
	       s.low_mark * s.capacity * _1G,
	       s.repeats, s.leakness);
}

TEST_P(AllocTest, test_alloc_8M_and_64K)
{
  constexpr uint32_t max_chunk_size = 8*1024*1024;
  constexpr uint32_t min_chunk_size = 64*1024;
  Scenario s = GetParam();
  PrintTo(s, &std::cout);
  std::cout << std::endl;
  boost::uniform_int<> D(0, 1);

  auto size_generator = [&]() -> uint32_t {
    if (D(rng) == 0)
      return max_chunk_size;
    else
      return min_chunk_size;
  };
  doAgeingTest(size_generator, s.capacity * _1G, s.alloc_unit,
	       s.high_mark * s.capacity * _1G,
	       s.low_mark * s.capacity * _1G,
	       s.repeats, s.leakness);
}

TEST_P(AllocTest, test_alloc_fragmentation_max_chunk_8M)
{
  constexpr uint32_t max_object_size = 150*1000*1000;
  constexpr uint32_t max_chunk_size = 8*1024*1024;

  Scenario s = GetParam();
  PrintTo(s, &std::cout);
  std::cout << std::endl;
  boost::uniform_int<> D(1, max_object_size / s.alloc_unit);

  auto size_generator = [&]() -> uint32_t {
    static uint32_t object_size = 0;
    uint32_t c;
    if (object_size == 0)
      object_size = (uint32_t(D(rng))* s.alloc_unit);
    if (object_size > max_chunk_size)
      c = max_chunk_size;
    else
      c = object_size;
    object_size -= c;
    return c;
  };
  doAgeingTest(size_generator, s.capacity * _1G, s.alloc_unit,
	       s.high_mark * s.capacity * _1G,
	       s.low_mark * s.capacity * _1G,
	       s.repeats, s.leakness);
}

INSTANTIATE_TEST_CASE_P(
  Allocator,
  AllocTest,
  ::testing::Values(
      Scenario{1024, 65536, 0.8, 0.6, 0.1, 3},
      Scenario{1024, 65536, 0.9, 0.7, 0.0, 3},
      Scenario{1024, 65536, 0.9, 0.7, 0.1, 3},
      Scenario{1024, 65536, 0.8, 0.6, 0.5, 3},
      Scenario{1024, 65536, 0.9, 0.7, 0.5, 3},
      Scenario{1024*10, 65536, 0.8, 0.6, 0.1, 3},
      Scenario{1024*10, 65536, 0.9, 0.7, 0.0, 3},
      Scenario{1024*10, 65536, 0.9, 0.7, 0.1, 3},
      Scenario{1024*3, 65536, 0.8, 0.6, 0.3, 3},
      Scenario{1024*3, 65536, 0.9, 0.7, 0.0, 3},
      Scenario{1024*3, 65536, 0.9, 0.7, 0.3, 3}

  ));



#else

TEST(DummyTest, ValueParameterizedTestsAreNotSupportedOnThisPlatform) {}
#endif
