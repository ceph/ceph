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

  auto print = [](const std::string& name, const aset_t& S) {
    for (auto &s: S) {
      std::cout << name << s.first << "["; //std::endl;
      for (auto &i: s.second) {
        std::cout << i << ",";
      }
      std::cout << "]" << std::endl;
    }
  };

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
