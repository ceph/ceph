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
#include "include/btree_map.h"
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
    check_usage(mempool::osd::id);
  }
}

TEST(mempool, vector_context)
{
  check_usage(mempool::osd::id);
  EXPECT_EQ(mempool::osd::allocated_bytes(), 0u);
  EXPECT_EQ(mempool::osd::allocated_items(), 0u);
  for (unsigned i = 0; i < 10; ++i) {
    vector<int> a;
    mempool::osd::vector<int> b,c;
    eq_elements(a,b);
    do_push_back(a,b,i,i);
    eq_elements(a,b);
    check_usage(mempool::osd::id);

    mempool::stats_t total;
    map<std::string,mempool::stats_t> by_type;
    mempool::get_pool(mempool::osd::id).get_stats(&total, &by_type);
    EXPECT_GE(mempool::osd::allocated_bytes(), i * 4u);
    EXPECT_GE(mempool::osd::allocated_items(), i);

    c.swap(b);
    eq_elements(a,c);
    check_usage(mempool::osd::id);
    a.clear();
    b.clear();
    c.clear();
  }
}

TEST(mempool, list_context)
{
  for (unsigned i = 1; i < 10; ++i) {
    list<int> a;
    mempool::osd::list<int> b,c;
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
    mempool::get_pool(mempool::osd::id).get_stats(&total, &by_type);
    EXPECT_GE(mempool::osd::allocated_bytes(), i * 4u);
    EXPECT_EQ(mempool::osd::allocated_items(), i);

    eq_elements(a,c);
    check_usage(mempool::osd::id);
  }
}

TEST(mempool, set_context)
{
  for (int i = 0; i < 10; ++i) {
    set<int> a;
    mempool::osd::set<int> b;
    do_insert(a,b,i,i);
    eq_elements(a,b);
    check_usage(mempool::osd::id);
  }

  for (int i = 1; i < 10; ++i) {
    set<int> a;
    mempool::osd::set<int> b;
    do_insert(a,b,i,0);
    EXPECT_NE(a.find(i/2),a.end());
    EXPECT_NE(b.find(i/2),b.end());
    a.erase(a.find(i/2));
    b.erase(b.find(i/2));
    eq_elements(a,b);
    check_usage(mempool::osd::id);
  }
}

struct obj {
  MEMPOOL_CLASS_HELPERS();
  int a;
  int b;
  obj() : a(1), b(1) {}
  explicit obj(int _a) : a(_a), b(2) {}
  obj(int _a,int _b) : a(_a), b(_b) {}
  friend inline bool operator<(const obj& l, const obj& r) {
    return l.a < r.a;
  }
};
MEMPOOL_DEFINE_OBJECT_FACTORY(obj, obj, osdmap);

TEST(mempool, test_factory)
{
   obj *o1 = new obj();
   obj *o2 = new obj(10);
   obj *o3 = new obj(20,30);
   check_usage(mempool::osdmap::id);
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
   check_usage(mempool::osdmap::id);
}

TEST(mempool, vector)
{
  {
    mempool::osd::vector<int> v;
    v.push_back(1);
    v.push_back(2);
  }
  {
    mempool::osdmap::vector<obj> v;
    v.push_back(obj());
    v.push_back(obj(1));
  }
}

TEST(mempool, set)
{
  mempool::osd::set<int> set_int;
  set_int.insert(1);
  set_int.insert(2);
  mempool::osdmap::set<obj> set_obj;
  set_obj.insert(obj());
  set_obj.insert(obj(1));
  set_obj.insert(obj(1, 2));
}

TEST(mempool, map)
{
  {
    mempool::osd::map<int,int> v;
    v[1] = 2;
    v[3] = 4;
  }
  {
    mempool::osdmap::map<int,obj> v;
    v[1] = obj();
    v[2] = obj(2);
    v[3] = obj(2, 3);
  }
}

TEST(mempool, list)
{
  {
    mempool::osd::list<int> v;
    v.push_back(1);
    v.push_back(2);
  }
  {
    mempool::osdmap::list<obj> v;
    v.push_back(obj());
    v.push_back(obj(1));
  }
 
}

TEST(mempool, dump)
{
  ostringstream ostr;

  Formatter* f = Formatter::create("xml-pretty", "xml-pretty", "xml-pretty");
  mempool::dump(f);
  f->flush(ostr);

  delete f;
  ASSERT_NE(ostr.str().find(mempool::get_pool_name((mempool::pool_index_t)0)),
    std::string::npos);

  ostr.str("");

  f = Formatter::create("html-pretty", "html-pretty", "html-pretty");
  mempool::dump(f);
  f->flush(ostr);

  delete f;
  ASSERT_NE(ostr.str().find(mempool::get_pool_name((mempool::pool_index_t)0)),
    std::string::npos);

  ostr.str("");
  f = Formatter::create("table", "table", "table");
  mempool::dump(f);
  f->flush(ostr);

  delete f;
  ASSERT_NE(ostr.str().find(mempool::get_pool_name((mempool::pool_index_t)0)),
    std::string::npos);

  ostr.str("");

  f = Formatter::create("json-pretty", "json-pretty", "json-pretty");
  mempool::dump(f);
  f->flush(ostr);
  delete f;

  ASSERT_NE(ostr.str().find(mempool::get_pool_name((mempool::pool_index_t)0)),
    std::string::npos);
}

TEST(mempool, unordered_map)
{
  mempool::osdmap::unordered_map<int,obj> h;
  h[1] = obj();
  h[2] = obj(1);
}

TEST(mempool, string_test)
{
  mempool::osdmap::string s;
  s.reserve(100);
  EXPECT_GE(mempool::osdmap::allocated_items(), s.capacity() + 1u); // +1 for zero-byte termination :
  for (size_t i = 0; i < 10; ++i) {
    s += '1';
    s.append(s);
    EXPECT_GE(mempool::osdmap::allocated_items(), s.capacity() + 1u);
  }
}

TEST(mempool, bufferlist)
{
  bufferlist bl;
  int len = 1048576;
  size_t before = mempool::buffer_anon::allocated_bytes();
  cout << "before " << before << std::endl;
  bl.append(buffer::create_aligned(len, 4096));
  size_t after = mempool::buffer_anon::allocated_bytes();
  cout << "after " << after << std::endl;
  ASSERT_GE(after, before + len);
}

TEST(mempool, bufferlist_reassign)
{
  bufferlist bl;
  size_t items_before = mempool::buffer_anon::allocated_items();
  size_t bytes_before = mempool::buffer_anon::allocated_bytes();
  bl.append("fooo");
  ASSERT_EQ(items_before + 1, mempool::buffer_anon::allocated_items());
  ASSERT_LT(bytes_before, mempool::buffer_anon::allocated_bytes());

  // move existing bl
  bl.reassign_to_mempool(mempool::mempool_osd);
  ASSERT_EQ(items_before, mempool::buffer_anon::allocated_items());
  ASSERT_EQ(bytes_before, mempool::buffer_anon::allocated_bytes());

  // additional appends should go to the same pool
  items_before = mempool::osd::allocated_items();
  bytes_before = mempool::osd::allocated_bytes();
  cout << "anon b " << mempool::buffer_anon::allocated_bytes() << std::endl;
  for (unsigned i = 0; i < 1000; ++i) {
    bl.append("asdfddddddddddddddddddddddasfdasdfasdfasdfasdfasdf");
  }
  cout << "anon a " << mempool::buffer_anon::allocated_bytes() << std::endl;
  ASSERT_LT(items_before, mempool::osd::allocated_items());
  ASSERT_LT(bytes_before, mempool::osd::allocated_bytes());

  // try_.. won't
  items_before = mempool::osd::allocated_items();
  bytes_before = mempool::osd::allocated_bytes();
  bl.try_assign_to_mempool(mempool::mempool_bloom_filter);
  ASSERT_EQ(items_before, mempool::osd::allocated_items());
  ASSERT_EQ(bytes_before, mempool::osd::allocated_bytes());
}

TEST(mempool, bufferlist_c_str)
{
  bufferlist bl;
  int len = 1048576;
  size_t before = mempool::osd::allocated_bytes();
  bl.append(buffer::create_aligned(len, 4096));
  bl.append(buffer::create_aligned(len, 4096));
  bl.reassign_to_mempool(mempool::mempool_osd);
  size_t after = mempool::osd::allocated_bytes();
  ASSERT_GE(after, before + len * 2);
  bl.c_str();
  size_t after_c_str = mempool::osd::allocated_bytes();
  ASSERT_EQ(after, after_c_str);
}

TEST(mempool, btree_map_test)
{
  typedef mempool::pool_allocator<mempool::mempool_osd,
    pair<const uint64_t,uint64_t>> allocator_t;
  typedef btree::btree_map<uint64_t,uint64_t,std::less<uint64_t>,allocator_t> btree_t;

  {
    btree_t btree;
    ASSERT_EQ(0, mempool::osd::allocated_items());
    ASSERT_EQ(0, mempool::osd::allocated_bytes());
    for (size_t i = 0; i < 1000; ++i) {
      btree[rand()] = rand();
    }
    ASSERT_LT(0, mempool::osd::allocated_items());
    ASSERT_LT(0, mempool::osd::allocated_bytes());
  }

  ASSERT_EQ(0, mempool::osd::allocated_items());
  ASSERT_EQ(0, mempool::osd::allocated_bytes());
}



int main(int argc, char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
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
