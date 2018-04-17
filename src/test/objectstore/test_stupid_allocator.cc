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
