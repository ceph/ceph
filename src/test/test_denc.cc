// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2016 Red Hat
 *
 * Author: Sage Weil <sage@redhat.com>
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

#include "include/denc.h"

// test helpers

template<typename T>
void test_encode_decode(T v) {
  bufferlist bl;
  ::encode(v, bl);
  bufferlist::iterator p = bl.begin();
  T out;
  ::decode(out, p);
  ASSERT_EQ(v, out);
}

template<typename T>
void test_denc(T v) {
  // estimate
  size_t s = 0;
  denc(v, s);
  ASSERT_NE(s, 0u);

  // encode
  bufferlist bl;
  {
    auto a = bl.get_contiguous_appender(s);
    denc(v, a);
  }
  ASSERT_LE(bl.length(), s);

  // decode
  bl.rebuild();
  T out;
  auto bpi = bl.front().begin();
  denc(out, bpi);
  ASSERT_EQ(v, out);
  ASSERT_EQ(bpi.get_pos(), bl.c_str() + bl.length());

  // test glue
  test_encode_decode(v);
}

template<typename T>
void test_encode_decode_featured(T v) {
  bufferlist bl;
  ::encode(v, bl, 123);
  bufferlist::iterator p = bl.begin();
  T out;
  ::decode(out, p);
  ASSERT_EQ(v, out);
}

template<typename T>
void test_denc_featured(T v) {
  // estimate
  size_t s = 0;
  denc(v, s, 0);
  ASSERT_GT(s, 0u);

  // encode
  bufferlist bl;
  {
    auto a = bl.get_contiguous_appender(s);
    denc(v, a, 1);
  }
  ASSERT_LE(bl.length(), s);

  // decode
  bl.rebuild();
  T out;
  auto bpi = bl.front().begin();
  denc(out, bpi, 1);
  ASSERT_EQ(v, out);
  ASSERT_EQ(bpi.get_pos(), bl.c_str() + bl.length());

  // test glue
  test_encode_decode_featured(v);
}


// hooks to count bound calls

struct counts_t {
  int num_bound_encode = 0;
  int num_encode = 0;
  int num_decode = 0;
  void reset() {
    num_bound_encode = 0;
    num_encode = 0;
    num_decode = 0;
  }
} counts;

struct denc_counter_t {
  void bound_encode(size_t& p) const {
    ++counts.num_bound_encode;
    ++p;  // denc.h does not like 0-length objects
  }
  void encode(buffer::list::contiguous_appender& p) const {
    p.append("a", 1);
    ++counts.num_encode;
  }
  void decode(buffer::ptr::iterator &p) {
    p.advance(1);
    ++counts.num_decode;
  }
};
WRITE_CLASS_DENC(denc_counter_t)

struct denc_counter_bounded_t {
  void bound_encode(size_t& p) const {
    ++counts.num_bound_encode;
    ++p;  // denc.h does not like 0-length objects
  }
  void encode(buffer::list::contiguous_appender& p) const {
    p.append("a", 1);
    ++counts.num_encode;
  }
  void decode(buffer::ptr::iterator &p) {
    p.advance(1);
    ++counts.num_decode;
  }
};
WRITE_CLASS_DENC_BOUNDED(denc_counter_bounded_t)

TEST(denc, denc_counter)
{
  denc_counter_t single, single2;
  {
    bufferlist bl;
    ::encode(single, bl);
    ::decode(single2, bl);
  }
  ASSERT_EQ(counts.num_bound_encode, 1);
  ASSERT_EQ(counts.num_encode, 1);
  ASSERT_EQ(counts.num_decode, 1);
  counts.reset();
}

TEST(denc, simple)
{
  test_denc((uint8_t)4);
  test_denc((int8_t)-5);
  test_denc((uint16_t)6);
  test_denc((int16_t)-7);
  test_denc((uint32_t)8);
  test_denc((int32_t)-9);
  test_denc((uint64_t)10);
  test_denc((int64_t)-11);
}

TEST(denc, string)
{
  string a, b("hi"), c("multi\nline\n");
  test_denc(a);
  test_denc(b);
  test_denc(c);
}

struct legacy_t {
  int32_t a = 1;
  void encode(bufferlist& bl) const {
    ::encode(a, bl);
  }
  void decode(bufferlist::iterator& p) {
    ::decode(a, p);
  }
  legacy_t() {}
  legacy_t(int32_t i) : a(i) {}
  friend bool operator<(const legacy_t& l, const legacy_t& r) {
    return l.a < r.a;
  }
  friend bool operator==(const legacy_t& l, const legacy_t& r) {
    return l.a == r.a;
  }
};
WRITE_CLASS_ENCODER(legacy_t)

template<template<class> class C>
void test_common_veclist(const char* c) {
  {
    cout << c << "<std::string>" << std::endl;
    C<std::string> s;
    s.push_back("foo");
    s.push_back("bar");
    s.push_back("baz");
    counts.reset();
    test_denc(s);
  }
  {
    cout << c << "<int32_t>" << std::endl;
    C<int32_t> s;
    s.push_back(1);
    s.push_back(2);
    s.push_back(3);
    test_denc(s);
  }
  {
    cout << c << "<legacy_t>" << std::endl;
    C<legacy_t> s;
    s.push_back(legacy_t(1));
    s.push_back(legacy_t(2));
    test_encode_decode(s);
  }
}

// We only care about specializing the type, all other template
// parameters should have the default values. (Like first-class
// functions, first-class templates do not bring their defaults.)

template<typename T>
using default_vector = std::vector<T>;

TEST(denc, vector)
{
  test_common_veclist<default_vector>("std::vector");
  {
    counts.reset();
    vector<denc_counter_t> v, v2;
    v.resize(100);
    {
      bufferlist bl;
      ::encode(v, bl);
      ::decode(v2, bl);
    }
    ASSERT_EQ(counts.num_bound_encode, 100);
    ASSERT_EQ(counts.num_encode, 100);
    ASSERT_EQ(counts.num_decode, 100);
  }
  {
    counts.reset();
    vector<denc_counter_bounded_t> v, v2;
    v.resize(100);
    {
      bufferlist bl;
      ::encode(v, bl);
      ::decode(v2, bl);
    }
    ASSERT_EQ(counts.num_bound_encode, 1);
    ASSERT_EQ(counts.num_encode, 100);
    ASSERT_EQ(counts.num_decode, 100);
  }
}

template<typename T>
using default_list = std::list<T>;

TEST(denc, list)
{
  test_common_veclist<default_list>("std::list");
  {
    counts.reset();
    list<denc_counter_bounded_t> l, l2;
    for (unsigned i=0; i<100; ++i) {
      l.emplace_back(denc_counter_bounded_t());
    }
    {
      bufferlist bl;
      ::encode(l, bl);
      ::decode(l2, bl);
    }
    ASSERT_EQ(counts.num_bound_encode, 1);
    ASSERT_EQ(counts.num_encode, 100);
    ASSERT_EQ(counts.num_decode, 100);
  }
}

template<template<class> class C>
void test_setlike(const char* c) {
  {
    cout << c << "<std::string>" << std::endl;
    C<std::string> s;
    s.insert("foo");
    s.insert("bar");
    s.insert("baz");
    test_denc(s);
  }
  {
    cout << c << "<int32_t>" << std::endl;
    C<int32_t> s;
    s.insert(1);
    s.insert(2);
    s.insert(3);
    test_denc(s);
  }
  {
    cout << c << "<legacy_t>" << std::endl;
    C<legacy_t> s;
    s.insert(legacy_t(1));
    s.insert(legacy_t(2));
    test_encode_decode(s);
  }
}

template<typename T>
using default_set = std::set<T>;

TEST(denc, set)
{
  test_setlike<default_set>("std::set");
}

template<typename T>
using default_flat_set= boost::container::flat_set<T>;

TEST(denc, flat_set)
{
  test_setlike<default_flat_set>("std::set");
}

struct foo_t {
  int32_t a = 0;
  uint64_t b = 123;

  DENC(foo_t, v, p) {
    DENC_START(1, 1, p);
    ::denc(v.a, p);
    ::denc(v.b, p);
    DENC_FINISH(p);
  }

  friend bool operator==(const foo_t& l, const foo_t& r) {
    return l.a == r.a && l.b == r.b;
  }
};
WRITE_CLASS_DENC_BOUNDED(foo_t)

struct foo2_t {
  int32_t c = 0;
  uint64_t d = 123;

  DENC(foo2_t, v, p) {
    DENC_START(1, 1, p);
    ::denc(v.c, p);
    ::denc(v.d, p);
    DENC_FINISH(p);
  }

  friend bool operator==(const foo2_t& l, const foo2_t& r) {
    return l.c == r.c && l.d == r.d;
  }
};
WRITE_CLASS_DENC_BOUNDED(foo2_t)


struct bar_t {
  int32_t a = 0;
  uint64_t b = 123;

  DENC_FEATURED(bar_t, v, p, f) {
    ::denc(v.a, p, f);
    ::denc(v.b, p, f);
  }

  friend bool operator==(const bar_t& l, const bar_t& r) {
    return l.a == r.a && l.b == r.b;
  }
};
WRITE_CLASS_DENC_FEATURED_BOUNDED(bar_t)

TEST(denc, foo)
{
  foo_t a;
  test_denc(a);
  bufferlist bl;
  ::encode(a, bl);
  bl.hexdump(cout);
}

TEST(denc, bar)
{
  bar_t a;
  test_denc_featured(a);
}



TEST(denc, pair)
{
  pair<int32_t,std::string> p;
  bufferlist bl;
  {
    auto a = bl.get_contiguous_appender(1000);
    denc(p, a);
    ::encode(p, bl);
  }

  pair<int32_t,legacy_t> lp;
  ::encode(lp, bl);
}

template<template<class, class> class C>
void test_common_maplike(const char* c) {
  {
    cout << c << "<std::string, foo_t>" << std::endl;
    C<string, foo_t> s;
    s["foo"] = foo_t();
    s["bar"] = foo_t();
    s["baz"] = foo_t();
    test_denc(s);
  }
  {
    cout << c << "<std::string, bar_t>" << std::endl;
    C<string, bar_t> s;
    s["foo"] = bar_t();
    s["bar"] = bar_t();
    s["baz"] = bar_t();
    test_denc_featured(s);
  }
  {
    cout << c << "<std::string, legacy_t>" << std::endl;
    C<std::string, legacy_t> s;
    s["foo"] = legacy_t(1);
    s["bar"] = legacy_t(2);
    test_encode_decode(s);
  }
}

template<typename U, typename V>
using default_map = std::map<U, V>;

TEST(denc, map)
{
  test_common_maplike<default_map>("std::map");
}

template<typename U, typename V>
using default_flat_map = boost::container::flat_map<U, V>;

TEST(denc, flat_map)
{
  test_common_maplike<default_flat_map>("boost::container::flat_map");
}

TEST(denc, bufferptr_shallow_and_deep) {
  // shallow encode
  int32_t i = 1;
  bufferptr p1("foo", 3);
  bufferlist bl;
  {
    auto a = bl.get_contiguous_appender(100);
    denc(i, a);
    denc(p1, a);
    denc(i, a);
  }
  cout << "bl is " << bl << std::endl;
  bl.hexdump(cout);
  ASSERT_EQ(3u, bl.get_num_buffers());

  bufferlist bl2 = bl;
  bl.rebuild();
  bl2.rebuild();

  // shallow decode
  {
    cout << "bl is " << bl << std::endl;
    bl.hexdump(cout);
    auto p = bl.front().begin();
    bufferptr op;
    int32_t i;
    denc(i, p);
    denc(op, p);
    denc(i, p);
    ASSERT_EQ(3u, op.length());
    ASSERT_EQ('f', op[0]);
    memset(bl.c_str(), 0, bl.length());
    ASSERT_EQ(0, op[0]);
  }

  // deep decode
  {
    cout << "bl is " << bl2 << std::endl;
    bl2.hexdump(cout);
    auto p = bl2.front().begin_deep();
    bufferptr op;
    int32_t i;
    denc(i, p);
    denc(op, p);
    denc(i, p);
    ASSERT_EQ('f', op[0]);
    memset(bl2.c_str(), 1, bl2.length());
    ASSERT_EQ('f', op[0]);
  }
}

TEST(denc, array)
{
  {
    cout << "std::array<std::string, 3>" << std::endl;
    std::array<std::string, 3> s = { "foo", "bar", "baz" };
    counts.reset();
    test_denc(s);
  }
  {
    cout << "std::array<uint32_t, 3>" << std::endl;
    std::array<uint32_t, 3> s = { 1UL, 2UL, 3UL };
    test_denc(s);
  }
}

TEST(denc, tuple)
{
  {
    cout << "std::tuple<uint64_t, uint32_t>" << std::endl;
    std::tuple<uint64_t, uint32_t> s(100ULL, 97UL);
    counts.reset();
    test_denc(s);
  }
  {
    cout << "std::tuple<std::string, uint3_t>" << std::endl;
    std::tuple<std::string, uint32_t> s("foo", 97);
    test_denc(s);
  }
  {
    cout << "std::tuple<std::string, std::set<uint32_t>>" << std::endl;
    std::tuple<std::string, std::set<uint32_t>> s(
      "bar", std::set<uint32_t>{uint32_t(1), uint32_t(2), uint32_t(3)});
    test_denc(s);
  }
}

TEST(denc, optional)
{
  {
    cout << "boost::optional<uint64_t>" << std::endl;
    boost::optional<uint64_t> s = 97, t = boost::none;
    counts.reset();
    test_denc(s);
    test_denc(t);
  }
  {
    cout << "boost::optional<std::string>" << std::endl;
    boost::optional<std::string> s = std::string("Meow"), t = boost::none;
    counts.reset();
    test_denc(s);
    test_denc(t);
  }
  {
    size_t s = 0;
    denc(boost::none, s);
    ASSERT_NE(s, 0u);

    // encode
    bufferlist bl;
    {
      auto a = bl.get_contiguous_appender(s);
      denc(boost::none, a);
    }
    ASSERT_LE(bl.length(), s);

    bl.rebuild();
    boost::optional<uint32_t> out = 5;
    auto bpi = bl.front().begin();
    denc(out, bpi);
    ASSERT_FALSE(!!out);
    ASSERT_EQ(bpi.get_pos(), bl.c_str() + bl.length());
  }
}
