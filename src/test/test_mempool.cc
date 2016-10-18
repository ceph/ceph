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
#include "include/mempool.h"

void check_usage(mempool::pool_index_t ix)
{
  mempool::pool_t *pool = &mempool::get_pool(ix);
  mempool::stats_t total;
  map<std::string,mempool::stats_t> m;
  pool->get_stats(&total, &m);
  size_t usage = pool->allocated_bytes();
  size_t sum = 0;
  for (auto& p : m) {
    sum += p.second.bytes;
  }
  if (sum != usage) {
    ceph::TableFormatter jf;
    pool->dump(&jf);
    jf.flush(std::cout);
  }
  EXPECT_EQ(sum, usage);
}

template<typename A, typename B>
void eq_elements(const A& a, const B& b)
{
  auto lhs = a.begin();
  auto rhs = b.begin();
  while (lhs != a.end()) {
    EXPECT_EQ(*lhs,*rhs);
    lhs++;
    rhs++;
  }
  EXPECT_EQ(rhs,b.end());
}

template<typename A, typename B>
void eq_pairs(const A& a, const B& b)
{
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

#define MAKE_INSERTER(inserter)				\
  template<typename A,typename B>			\
void do_##inserter(A& a, B& b, int count, int base) {	\
  for (int i = 0; i < count; ++i) {			\
    a.inserter(base + i);				\
    b.inserter(base + i);				\
  }							\
}

MAKE_INSERTER(push_back);
MAKE_INSERTER(insert);

template<typename A,typename B>
void do_insert_key(A& a, B& b, int count, int base)
{
  for (int i = 0; i < count; ++i) {
    a.insert(make_pair(base+i,base+i));
    b.insert(make_pair(base+i,base+i));
    check_usage(mempool::unittest_1::id);
  }
}

TEST(mempool, vector_context)
{
  check_usage(mempool::unittest_1::id);
  EXPECT_EQ(mempool::unittest_1::allocated_bytes(), 0u);
  EXPECT_EQ(mempool::unittest_1::allocated_items(), 0u);
  for (unsigned i = 0; i < 10; ++i) {
    vector<int> a;
    mempool::unittest_1::vector<int> b,c;
    eq_elements(a,b);
    do_push_back(a,b,i,i);
    eq_elements(a,b);
    check_usage(mempool::unittest_1::id);

    mempool::stats_t total;
    map<std::string,mempool::stats_t> by_type;
    mempool::get_pool(mempool::unittest_1::id).get_stats(&total, &by_type);
    EXPECT_GE(mempool::unittest_1::allocated_bytes(), i * 4u);
    EXPECT_GE(mempool::unittest_1::allocated_items(), i);

    c.swap(b);
    eq_elements(a,c);
    check_usage(mempool::unittest_1::id);
    a.clear();
    b.clear();
    c.clear();
  }
}

TEST(mempool, list_context)
{
  for (unsigned i = 1; i < 10; ++i) {
    list<int> a;
    mempool::unittest_1::list<int> b,c;
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
    c.splice(c.begin(),b,b.begin(),b.end());

    mempool::stats_t total;
    map<std::string,mempool::stats_t> by_type;
    mempool::get_pool(mempool::unittest_1::id).get_stats(&total, &by_type);
    EXPECT_GE(mempool::unittest_1::allocated_bytes(), i * 4u);
    EXPECT_EQ(mempool::unittest_1::allocated_items(), i);

    eq_elements(a,c);
    check_usage(mempool::unittest_1::id);
  }
}

TEST(mempool, set_context)
{
  for (int i = 0; i < 10; ++i) {
    set<int> a;
    mempool::unittest_1::set<int> b;
    do_insert(a,b,i,i);
    eq_elements(a,b);
    check_usage(mempool::unittest_1::id);
  }

  for (int i = 1; i < 10; ++i) {
    set<int> a;
    mempool::unittest_1::set<int> b;
    do_insert(a,b,i,0);
    EXPECT_NE(a.find(i/2),a.end());
    EXPECT_NE(b.find(i/2),b.end());
    a.erase(a.find(i/2));
    b.erase(b.find(i/2));
    eq_elements(a,b);
    check_usage(mempool::unittest_1::id);
  }
}

struct obj {
  MEMPOOL_CLASS_HELPERS();
  int a;
  int b;
  obj() : a(1), b(1) {}
  obj(int _a) : a(_a), b(2) {}
  obj(int _a,int _b) : a(_a), b(_b) {}
  friend inline bool operator<(const obj& l, const obj& r) {
    return l.a < r.a;
  }
};
MEMPOOL_DEFINE_OBJECT_FACTORY(obj, obj, unittest_2);

TEST(mempool, test_factory)
{
   obj *o1 = new obj();
   obj *o2 = new obj(10);
   obj *o3 = new obj(20,30);
   check_usage(mempool::unittest_2::id);
   EXPECT_NE(o1,nullptr);
   EXPECT_EQ(o1->a,1);
   EXPECT_EQ(o1->b,1);
   EXPECT_EQ(o2->a,10);
   EXPECT_EQ(o2->b,2);
   EXPECT_EQ(o3->a,20);
   EXPECT_EQ(o3->b,30);

   delete o1;
   delete o2;
   delete o3;
   check_usage(mempool::unittest_2::id);
}

TEST(mempool, vector)
{
  {
    mempool::unittest_1::vector<int> v;
    v.push_back(1);
    v.push_back(2);
  }
  {
    mempool::unittest_2::vector<obj> v;
    v.push_back(obj());
    v.push_back(obj(1));
  }
}

TEST(mempool, set)
{
  mempool::unittest_1::set<int> set_int;
  set_int.insert(1);
  set_int.insert(2);
  mempool::unittest_2::set<obj> set_obj;
  set_obj.insert(obj());
  set_obj.insert(obj(1));
  set_obj.insert(obj(1, 2));
}

TEST(mempool, map)
{
  {
    mempool::unittest_1::map<int,int> v;
    v[1] = 2;
    v[3] = 4;
  }
  {
    mempool::unittest_2::map<int,obj> v;
    v[1] = obj();
    v[2] = obj(2);
    v[3] = obj(2, 3);
  }
}

TEST(mempool, list)
{
  {
    mempool::unittest_1::list<int> v;
    v.push_back(1);
    v.push_back(2);
  }
  {
    mempool::unittest_2::list<obj> v;
    v.push_back(obj());
    v.push_back(obj(1));
  }
}

TEST(mempool, unordered_map)
{
  mempool::unittest_2::unordered_map<int,obj> h;
  h[1] = obj();
  h[2] = obj(1);
}

int main(int argc, char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  // enable debug mode for the tests
  mempool::set_debug_mode(true);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


/*
 * Local Variables:
 * compile-command: "cd ../../build ; make -j4 &&
 *   make unittest_mempool &&
 *   valgrind --tool=memcheck ./unittest_mempool --gtest_filter=*.*"
 * End:
 */
