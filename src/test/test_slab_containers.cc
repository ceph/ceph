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

#if 0
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
#endif

TEST(test_slab_containers, list_context) {
   for (size_t i = 1; i < 10; ++i) {
      list<int> a;
      ceph::slab::slab_list<int,4,4> b,c;
      eq_elements(a,b);
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
      //c.splice(c.begin(),b,b.begin(),b.end());
      //eq_elements(a,c);
   }
   for (int i = 1; i < 10; ++i) {
      list<int> a;
      ceph::slab::slab_list<int,4,2> b,c;
      eq_elements(a,b);
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
      //c.splice(c.begin(),b,b.begin(),b.end());
      //eq_elements(a,c);
   }
}

TEST(test_slab_containers, set_context) {
   for (size_t i = 0; i < 10; ++i) {
      set<int> a;
      ceph::slab::slab_set<int,4,2> b;
      do_insert(a,b,i,i);
      eq_elements(a,b);
   }

   for (size_t i = 1; i < 10; ++i) {
      set<int> a;
      ceph::slab::slab_set<int,4,2> b;
      do_insert(a,b,i,0);
      EXPECT_NE(a.find(i/2),a.end());
      EXPECT_NE(b.find(i/2),b.end());
      a.erase(a.find(i/2));
      b.erase(b.find(i/2));
      eq_elements(a,b);
   }
   for (size_t i = 1; i < 10; ++i) {
      set<int> a;
      ceph::slab::slab_set<int,4,2> b;
      do_insert(a,b,i,0);
      EXPECT_NE(a.find(i/2),a.end());
      EXPECT_NE(b.find(i/2),b.end());
      a.erase(a.find(i/2));
      b.erase(b.find(i/2));
      eq_elements(a,b);
   }
}

TEST(test_slab_containers, documentation_test) {

  ceph::slab::slab_set<int,2,4> x;
  x.insert(1); // satisfied by first internal slab element
  x.insert(2); // Uses second internal slab element
  x.insert(3); // Allocates a slab of 4
}


/*
 * Local Variables:
 * compile-command: "cd ../../build ; make -j4 &&
 *   make unittest_slab_containers &&
 *   valgrind --tool=memcheck ./unittest_slab_containers --gtest_filter=*.*"
 * End:
 */
