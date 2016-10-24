// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2016 Western Digital Corporation
 *
 * Author: Allen Samuels <allen.samuels@sandisk.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#include <stdio.h>

#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "gtest/gtest.h"
#include "include/slab_containers.h"

template<typename A, typename B> void eq_elements(const A& a, const B& b) {
   auto lhs = a.begin();
   auto rhs = b.begin();
   while (lhs != a.end()) {
      EXPECT_EQ(*lhs,*rhs);
      lhs++;
      rhs++;
   }
   EXPECT_EQ(rhs,b.end());
}

template<typename A, typename B> void eq_pairs(const A& a, const B& b) {
   auto lhs = a.begin();
   auto rhs = b.begin();
   while (lhs != a.end()) {
      EXPECT_EQ(lhs->first,rhs->first);
      EXPECT_EQ(lhs->second,rhs->second);
      lhs++;
      rhs++;
   }
   EXPECT_EQ(rhs,b.end());
}

#define MAKE_INSERTER(inserter) \
template<typename A,typename B> void do_##inserter(A& a, B& b, size_t count, size_t base) { \
   for (size_t i = 0; i < count; ++i) { \
      a.inserter(base + i); \
      b.inserter(base + i); \
   } \
}

MAKE_INSERTER(push_back);
MAKE_INSERTER(insert);

template<typename A,typename B> void do_insert_key(A& a, B& b, size_t count, size_t base) { \
   for (size_t i = 0; i < count; ++i) {
      a.insert(make_pair(base+i,base+i));
      b.insert(make_pair(base+i,base+i));
   }
}

TEST(test_slab_containers, vector_context) {
   for (size_t i = 0; i < 10; ++i) {
      EXPECT_EQ(0u,mempool::unittest_1::allocated_bytes());
      EXPECT_EQ(0u,mempool::unittest_1::allocated_items());
      EXPECT_EQ(0u,mempool::unittest_1::free_bytes());
      EXPECT_EQ(0u,mempool::unittest_1::free_items());
      EXPECT_EQ(0u,mempool::unittest_1::containers());
      vector<int> a;
      mempool::unittest_1::slab_vector<int,4> b,c;
      EXPECT_EQ(2u,mempool::unittest_1::containers());
      EXPECT_EQ(2u,mempool::unittest_1::slabs());
      eq_elements(a,b);
      do_push_back(a,b,i,i);
      EXPECT_EQ(i > 4 ? 3u : 2u,mempool::unittest_1::slabs());
      eq_elements(a,b);
      c.swap(b);
      eq_elements(a,c);
      a.clear();
      b.clear();
      c.clear();
   }
}

TEST(test_slab_containers, list_context) {
   for (size_t i = 1; i < 10; ++i) {
      EXPECT_EQ(0u,mempool::unittest_1::allocated_bytes());
      EXPECT_EQ(0u,mempool::unittest_1::allocated_items());
      EXPECT_EQ(0u,mempool::unittest_1::free_bytes());
      EXPECT_EQ(0u,mempool::unittest_1::free_items());
      EXPECT_EQ(0u,mempool::unittest_1::containers());
      list<int> a;
      mempool::unittest_1::slab_list<int,4,4> b,c;
      EXPECT_EQ(2u,mempool::unittest_1::slabs());
      EXPECT_EQ(8u,mempool::unittest_1::allocated_items());
      EXPECT_EQ(8u,mempool::unittest_1::free_items());
      EXPECT_EQ(0u,mempool::unittest_1::inuse_items());
      eq_elements(a,b);
      do_push_back(a,b,i,i);
      EXPECT_EQ(i,mempool::unittest_1::inuse_items());
      EXPECT_EQ((i-1)/4 + 2,mempool::unittest_1::slabs());
      eq_elements(a,b);
      c.swap(b);
      eq_elements(a,c);
      a.erase(a.begin());
      c.erase(c.begin());
      eq_elements(a,c);
      a.clear();
      b.clear();
      c.clear();
      do_push_back(a,b,i,i);
      c.splice(c.begin(),b,b.begin(),b.end());
      eq_elements(a,c);
   }
   //
   // Now with reserve calls
   //
   for (int i = 1; i < 10; ++i) {
      list<int> a;
      mempool::unittest_1::slab_list<int,4> b,c;
      eq_elements(a,b);
      b.reserve(i);
      c.reserve(i);
      do_push_back(a,b,i,i);
      eq_elements(a,b);
      c.swap(b);
      eq_elements(a,c);
      a.erase(a.begin());
      c.erase(c.begin());
      eq_elements(a,c);
      a.clear();
      b.clear();
      c.clear();
      do_push_back(a,b,i,i);
      c.splice(c.begin(),b,b.begin(),b.end());
      eq_elements(a,c);
   }
}

TEST(test_slab_containers, set_context) {
   for (size_t i = 0; i < 10; ++i) {
      EXPECT_EQ(0u,mempool::unittest_1::allocated_bytes());
      EXPECT_EQ(0u,mempool::unittest_1::allocated_items());
      EXPECT_EQ(0u,mempool::unittest_1::free_bytes());
      EXPECT_EQ(0u,mempool::unittest_1::free_items());
      EXPECT_EQ(0u,mempool::unittest_1::containers());
      set<int> a;
      mempool::unittest_1::slab_set<int,4> b;
      EXPECT_EQ(1u,mempool::unittest_1::containers());
      EXPECT_EQ(1u,mempool::unittest_1::slabs());
      do_insert(a,b,i,i);
      EXPECT_EQ(i,mempool::unittest_1::inuse_items());
      eq_elements(a,b);
   }

   for (size_t i = 1; i < 10; ++i) {
      EXPECT_EQ(0u,mempool::unittest_1::allocated_bytes());
      EXPECT_EQ(0u,mempool::unittest_1::allocated_items());
      EXPECT_EQ(0u,mempool::unittest_1::free_bytes());
      EXPECT_EQ(0u,mempool::unittest_1::free_items());
      EXPECT_EQ(0u,mempool::unittest_1::containers());
      set<int> a;
      mempool::unittest_1::slab_set<int,4> b;
      do_insert(a,b,i,0);
      EXPECT_NE(a.find(i/2),a.end());
      EXPECT_NE(b.find(i/2),b.end());
      a.erase(a.find(i/2));
      b.erase(b.find(i/2));
      eq_elements(a,b);
   }
   for (size_t i = 1; i < 10; ++i) {
      EXPECT_EQ(0u,mempool::unittest_1::allocated_bytes());
      EXPECT_EQ(0u,mempool::unittest_1::allocated_items());
      EXPECT_EQ(0u,mempool::unittest_1::free_bytes());
      EXPECT_EQ(0u,mempool::unittest_1::free_items());
      EXPECT_EQ(0u,mempool::unittest_1::containers());
      set<int> a;
      mempool::unittest_1::slab_set<int,4> b;
      b.reserve(i);
      do_insert(a,b,i,0);
      EXPECT_NE(a.find(i/2),a.end());
      EXPECT_NE(b.find(i/2),b.end());
      a.erase(a.find(i/2));
      b.erase(b.find(i/2));
      eq_elements(a,b);
   }
}

TEST(test_slab_containers, documentation_test) {

  mempool::unittest_1::slab_set<int,2,4> x;

  EXPECT_EQ(2u,mempool::unittest_1::allocated_items()); // 2 items in stackSlab
  EXPECT_EQ(2u,mempool::unittest_1::free_items());      // both are free
  EXPECT_EQ(1u,mempool::unittest_1::containers());      // 1 container
  EXPECT_EQ(1u,mempool::unittest_1::slabs());           // 1 stackSlab
  EXPECT_EQ(0u,mempool::unittest_1::inuse_items());     // No items inuse

  x.insert(1); // satisfied by first internal slab element

  EXPECT_EQ(2u,mempool::unittest_1::allocated_items()); // still 2 in stackSlab
  EXPECT_EQ(1u,mempool::unittest_1::free_items());      // only 1 is free
  EXPECT_EQ(1u,mempool::unittest_1::containers());
  EXPECT_EQ(1u,mempool::unittest_1::slabs());
  EXPECT_EQ(1u,mempool::unittest_1::inuse_items());     // One item inuse

  x.insert(2); // Uses second internal slab element

  EXPECT_EQ(2u,mempool::unittest_1::allocated_items()); // still 2 in stackSlab
  EXPECT_EQ(0u,mempool::unittest_1::free_items());      // none are free
  EXPECT_EQ(1u,mempool::unittest_1::containers());
  EXPECT_EQ(1u,mempool::unittest_1::slabs());
  EXPECT_EQ(2u,mempool::unittest_1::inuse_items());

  x.insert(3); // Allocates a slab of 4

  EXPECT_EQ(6u,mempool::unittest_1::allocated_items()); // 2 + 4 (one slab)
  EXPECT_EQ(3u,mempool::unittest_1::free_items());      // 3 of the 4 are still free
  EXPECT_EQ(1u,mempool::unittest_1::containers());
  EXPECT_EQ(2u,mempool::unittest_1::slabs());
  EXPECT_EQ(3u,mempool::unittest_1::inuse_items());
}

int main(int argc, char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


/*
 * Local Variables:
 * compile-command: "cd ../../build ; make -j4 &&
 *   make unittest_slab_containers &&
 *   valgrind --tool=memcheck ./unittest_slab_containers --gtest_filter=*.*"
 * End:
 */
