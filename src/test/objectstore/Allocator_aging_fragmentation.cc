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

#include "common/ceph_mutex.h"
#include "common/Cond.h"
#include "common/errno.h"
#include "global/global_init.h"
#include "include/stringify.h"
#include "include/Context.h"
#include "os/bluestore/Allocator.h"

#include <boost/random/uniform_int.hpp>

typedef boost::mt11213b gen_type;

#include "common/debug.h"
#define dout_context cct
#define dout_subsys ceph_subsys_

struct Scenario {
  uint64_t capacity;
  uint64_t alloc_unit;
  double high_mark;
  double low_mark;
  double leakness;
  uint32_t repeats;
};

std::vector<Scenario> scenarios{
      Scenario{512,    65536, 0.8, 0.6, 0.1, 3},
      Scenario{512,    65536, 0.9, 0.7, 0.0, 3},
      Scenario{512,    65536, 0.9, 0.7, 0.1, 3},
      Scenario{512,    65536, 0.8, 0.6, 0.5, 3},
      Scenario{512,    65536, 0.9, 0.7, 0.5, 3},
      Scenario{1024,   65536, 0.8, 0.6, 0.1, 3},
      Scenario{1024,   65536, 0.9, 0.7, 0.0, 3},
      Scenario{1024,   65536, 0.9, 0.7, 0.1, 3},
      Scenario{1024*2, 65536, 0.8, 0.6, 0.3, 3},
      Scenario{1024*2, 65536, 0.9, 0.7, 0.0, 3},
      Scenario{1024*2, 65536, 0.9, 0.7, 0.3, 3},
      Scenario{512,    65536/16, 0.8, 0.6, 0.1, 3},
      Scenario{512,    65536/16, 0.9, 0.7, 0.0, 3},
      Scenario{512,    65536/16, 0.9, 0.7, 0.1, 3},
      Scenario{512,    65536/16, 0.8, 0.6, 0.5, 3},
      Scenario{512,    65536/16, 0.9, 0.7, 0.5, 3},
      Scenario{1024,   65536/16, 0.8, 0.6, 0.1, 3},
      Scenario{1024,   65536/16, 0.9, 0.7, 0.0, 3},
      Scenario{1024,   65536/16, 0.9, 0.7, 0.1, 3},
      Scenario{1024*2, 65536/16, 0.8, 0.6, 0.3, 3},
      Scenario{1024*2, 65536/16, 0.9, 0.7, 0.0, 3},
      Scenario{1024*2, 65536/16, 0.9, 0.7, 0.3, 3}
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
bool verbose = getenv("VERBOSE") != nullptr;

class AllocTracker;
class AllocTest : public ::testing::TestWithParam<std::string> {
protected:
  boost::scoped_ptr<AllocTracker> at;
  gen_type rng;
  static boost::intrusive_ptr<CephContext> cct;

public:
  boost::scoped_ptr<Allocator> alloc;
  AllocTest(): alloc(nullptr) {}
  void init_alloc(const std::string& alloc_name, int64_t size, uint64_t min_alloc_size);
  void init_close();
  void doAgingTest(std::function<uint32_t()> size_generator,
		    const std::string& alloc_name, uint64_t capacity, uint32_t alloc_unit,
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

  void TearDown() final;
  static void SetUpTestSuite();
  static void TearDownTestSuite();
};

struct test_result {
  uint64_t tests_cnt = 0;
  double fragmented_percent = 0;
  double fragments_count = 0;
  double time = 0;
  double frag_score = 0;
};

std::map<std::string, test_result> results_per_allocator;

const uint64_t _1m = 1024 * 1024;
const uint64_t _1G = 1024 * 1024 * 1024;

const uint64_t _2m = 2 * 1024 * 1024;

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

boost::intrusive_ptr<CephContext> AllocTest::cct;

void AllocTest::init_alloc(const std::string& allocator_name, int64_t size, uint64_t min_alloc_size) {
  this->capacity = size;
  this->alloc_unit = min_alloc_size;
  rng.seed(0);
  alloc.reset(Allocator::create(cct.get(), allocator_name, size,
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
    auto r = alloc->allocate(want, alloc_unit, 0, 0, &tmp);
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

void AllocTest::doAgingTest(
    std::function<uint32_t()> size_generator,
    const std::string& allocator_name,
    uint64_t capacity, uint32_t alloc_unit,
    uint64_t high_mark, uint64_t low_mark, uint32_t iterations, double leak_factor)
{
  assert(isp2(alloc_unit));
  cct->_conf->bdev_block_size = alloc_unit;
  PExtentVector allocated, tmp;
  init_alloc(allocator_name, capacity, alloc_unit);
  alloc->init_add_free(0, capacity);

  utime_t start = ceph_clock_now();
  level = 0;
  allocs = 0;
  fragmented = 0;
  fragments = 0;
  total_fragments = 0;
  if (verbose) std::cout << "INITIAL FILL" << std::endl;
  do_fill(high_mark, size_generator, leak_factor); //initial fill with data
  if (verbose) std::cout << "    fragmented allocs=" << 100.0 * fragmented / allocs << "%" <<
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
    if (verbose) std::cout << "ADDING CAPACITY " << i + 1 << std::endl;
    do_free(low_mark); //simulates adding new capacity to cluster
    if (verbose) std::cout << "    level change: " <<
	double(level_previous) / capacity * 100 << "% -> " <<
	double(level) / capacity * 100 << "% time=" <<
	(ceph_clock_now() - start) * 1000 << "ms" << std::endl;

    start = ceph_clock_now();
    if (verbose) std::cout << "APPENDING " << i + 1 << std::endl;
    do_fill(high_mark, size_generator, leak_factor); //only creating elements
    if (verbose) std::cout << "    fragmented allocs=" << 100.0 * fragmented / allocs << "%" <<
        " #frags=" << ( fragmented != 0 ? double(fragments) / fragmented : 0 ) <<
        " time=" << (ceph_clock_now() - start) * 1000 << "ms" << std::endl;
  }
  double frag_score = alloc->get_fragmentation_score();
  do_free(0);
  double free_frag_score = alloc->get_fragmentation_score();
  ASSERT_EQ(alloc->get_free(), capacity);

  std::cout << "    fragmented allocs=" << 100.0 * fragmented / allocs << "%" <<
        " #frags=" << ( fragmented != 0 ? double(fragments) / fragmented : 0 ) <<
        " time=" << (ceph_clock_now() - start) * 1000 << "ms" <<
        " frag.score=" << frag_score << " after free frag.score=" << free_frag_score << std::endl;

  uint64_t sum = 0;
  uint64_t cnt = 0;
  auto list_free = [&](size_t off, size_t len) {
    cnt++;
    sum+=len;
  };
  alloc->dump(list_free);
  ASSERT_EQ(sum, capacity);
  if (verbose)
    std::cout << "free chunks sum=" << sum << " free chunks count=" << cnt << std::endl;

  //adding to totals
  test_result &r = results_per_allocator[allocator_name];
  r.tests_cnt ++;
  r.fragmented_percent += 100.0 * fragmented / allocs;
  r.fragments_count += ( fragmented != 0 ? double(fragments) / fragmented : 2 );
  r.time += ceph_clock_now() - start;
  r.frag_score += frag_score;
}

void AllocTest::SetUpTestSuite()
{
  vector<const char*> args;
  cct = global_init(NULL, args,
		    CEPH_ENTITY_TYPE_CLIENT,
		    CODE_ENVIRONMENT_UTILITY,
		    CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(cct.get());
}

void AllocTest::TearDown()
{
  at.reset();
  alloc.reset();
}

void AllocTest::TearDownTestSuite()
{
  cct.reset();

  std::cout << "Summary: " << std::endl;
  for (auto& r: results_per_allocator) {
    std::cout << r.first <<
        "    fragmented allocs=" << r.second.fragmented_percent / r.second.tests_cnt << "%" <<
        " #frags=" << r.second.fragments_count / r.second.tests_cnt <<
        " free_score=" << r.second.frag_score / r.second.tests_cnt <<
        " time=" << r.second.time * 1000 << "ms" << std::endl;
  }
}


TEST_P(AllocTest, test_alloc_triangle_0_8M_16M)
{
  std::string allocator_name = GetParam();
  boost::triangle_distribution<double> D(1, (8 * 1024 * 1024) , (16 * 1024 * 1024) );
  for (auto& s:scenarios) {
    std::cout << "Allocator: " << allocator_name << ", ";
    PrintTo(s, &std::cout);
    std::cout << std::endl;

    auto size_generator = [&]() -> uint32_t {
      return (uint32_t(D(rng)) + s.alloc_unit) & ~(s.alloc_unit - 1);
    };

    doAgingTest(size_generator, allocator_name, s.capacity * _1G, s.alloc_unit,
		 s.high_mark * s.capacity * _1G,
		 s.low_mark * s.capacity * _1G,
		 s.repeats, s.leakness);
  }
}

TEST_P(AllocTest, test_alloc_8M_and_64K)
{
  std::string allocator_name = GetParam();
  constexpr uint32_t max_chunk_size = 8*1024*1024;
  constexpr uint32_t min_chunk_size = 64*1024;
  for (auto& s:scenarios) {
    std::cout << "Allocator: " << allocator_name << ", ";
    PrintTo(s, &std::cout);
    std::cout << std::endl;
    boost::uniform_int<> D(0, 1);

    auto size_generator = [&]() -> uint32_t {
      if (D(rng) == 0)
	return max_chunk_size;
      else
	return min_chunk_size;
    };

    doAgingTest(size_generator, allocator_name, s.capacity * _1G, s.alloc_unit,
		 s.high_mark * s.capacity * _1G,
		 s.low_mark * s.capacity * _1G,
		 s.repeats, s.leakness);
  }
}

TEST_P(AllocTest, test_alloc_fragmentation_max_chunk_8M)
{
  std::string allocator_name = GetParam();
  constexpr uint32_t max_object_size = 150*1000*1000;
  constexpr uint32_t max_chunk_size = 8*1024*1024;
  for (auto& s:scenarios) {
    std::cout << "Allocator: " << allocator_name << ", ";
    PrintTo(s, &std::cout);
    std::cout << std::endl;
    boost::uniform_int<> D(1, max_object_size / s.alloc_unit);

    uint32_t object_size = 0;

    auto size_generator = [&]() -> uint32_t {
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

    doAgingTest(size_generator, allocator_name, s.capacity * _1G, s.alloc_unit,
		 s.high_mark * s.capacity * _1G,
		 s.low_mark * s.capacity * _1G,
		 s.repeats, s.leakness);
  }
}

TEST_P(AllocTest, test_bonus_empty_fragmented)
{
  uint64_t capacity = uint64_t(512) * 1024 * 1024 * 1024; //512 G
  uint64_t alloc_unit = 64 * 1024;
  std::string allocator_name = GetParam();
  std::cout << "Allocator: " << allocator_name << std::endl;
  init_alloc(allocator_name, capacity, alloc_unit);
  alloc->init_add_free(0, capacity);
  PExtentVector tmp;
  for (size_t i = 0; i < capacity / (1024 * 1024); i++) {
    tmp.clear();
    uint32_t want = 1024 * 1024;
    int r = alloc->allocate(want, alloc_unit, 0, 0, &tmp);
    ASSERT_EQ(r, want);
    if (tmp.size() > 1) {
      interval_set<uint64_t> release_set;
      for (auto& t: tmp) {
	release_set.insert(t.offset, t.length);
      }
      alloc->release(release_set);
    } else {
      interval_set<uint64_t> release_set;
      uint64_t offset = tmp[0].offset;
      uint64_t length = tmp[0].length;

      release_set.insert(offset + alloc_unit, length - 3 * alloc_unit);
      alloc->release(release_set);
      release_set.clear();

      release_set.insert(offset , alloc_unit);
      alloc->release(release_set);
      release_set.clear();

      release_set.insert(offset + length - 2 * alloc_unit, 2 * alloc_unit);
      alloc->release(release_set);
      release_set.clear();
    }
  }
  double frag_score = alloc->get_fragmentation_score();
  ASSERT_EQ(alloc->get_free(), capacity);
  std::cout << "    empty storage frag.score=" << frag_score << std::endl;
}

INSTANTIATE_TEST_CASE_P(
  Allocator,
  AllocTest,
  ::testing::Values("stupid", "bitmap", "avl", "btree"));
