// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 eNovance SAS <licensing@enovance.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */
#include <iostream>
#include "global/global_init.h"
#include "common/ceph_json.h"
#include "common/Formatter.h"
#include "rgw/rgw_common.h"
#define GTEST
#ifdef GTEST
#include <gtest/gtest.h>
#else
#define TEST(x, y) void y()
#define ASSERT_EQ(v, s) if(v != s)cout << "Error at " << __LINE__ << "(" << #v << "!= " << #s << "\n"; \
                                else cout << "(" << #v << "==" << #s << ") PASSED\n";
#define EXPECT_EQ(v, s) ASSERT_EQ(v, s)
#define ASSERT_TRUE(c) if(c)cout << "Error at " << __LINE__ << "(" << #c << ")" << "\n"; \
                          else cout << "(" << #c << ") PASSED\n";
#define EXPECT_TRUE(c) ASSERT_TRUE(c) 
#endif
using namespace std;

static void init_bucket(rgw_bucket *bucket, const char *name)
{
  *bucket = rgw_bucket("", name, ".data-pool", ".index-pool", "marker", "bucket-id", NULL);
}

void check_parsed_correctly(rgw_obj& obj, const string& name, const string& ns, const string& instance)
{
  /* parse_raw_oid() */
  string parsed_name, parsed_ns, parsed_instance;
  ASSERT_EQ(true, rgw_obj::parse_raw_oid(obj.get_object(), &parsed_name, &parsed_instance, &parsed_ns));

  cout << "parsed: " << parsed_name << " ns=" << parsed_ns << " i=" << parsed_instance << std::endl;

  ASSERT_EQ(name, parsed_name);
  ASSERT_EQ(ns, parsed_ns);
  ASSERT_EQ(instance, parsed_instance);

  /* translate_raw_obj_to_obj_in_ns() */
  string tname = obj.get_object();
  string tns = ns + "foo";
  string tinstance;
  ASSERT_EQ(0, rgw_obj::translate_raw_obj_to_obj_in_ns(tname, tinstance, tns));
  ASSERT_EQ(name, tname);
  ASSERT_EQ(instance, tinstance);

  tname = obj.get_object();
  tns = ns;
  ASSERT_EQ(true, rgw_obj::translate_raw_obj_to_obj_in_ns(tname, tinstance, tns));

  cout << "parsed: " << parsed_name << " ns=" << parsed_ns << " i=" << parsed_instance << std::endl;

  ASSERT_EQ(name, tname);
  ASSERT_EQ(instance, tinstance);

  /* strip_namespace_from_object() */

  string strip_name = obj.get_object();
  string strip_ns, strip_instance;

  ASSERT_EQ(true, rgw_obj::strip_namespace_from_object(strip_name, strip_ns, strip_instance));

  cout << "stripped: " << strip_name << " ns=" << strip_ns << " i=" << strip_instance << std::endl;

  ASSERT_EQ(name, strip_name);
  ASSERT_EQ(ns, strip_ns);
  ASSERT_EQ(instance, strip_instance);
}

void test_obj(const string& name, const string& ns, const string& instance)
{
  rgw_bucket b;
  init_bucket(&b, "test");

  JSONFormatter *formatter = new JSONFormatter(true);

  formatter->open_object_section("test");
  rgw_obj o(b, name);
  rgw_obj obj1(o);

  if (!instance.empty()) {
    obj1.set_instance(instance);
  }
  if (!ns.empty()) {
    obj1.set_ns(ns);
  }
  
  check_parsed_correctly(obj1, name, ns, instance);
  encode_json("obj1", obj1, formatter);

  bufferlist bl;
  ::encode(obj1, bl);

  rgw_obj obj2;
  ::decode(obj2, bl);
  check_parsed_correctly(obj2, name, ns, instance);

  encode_json("obj2", obj2, formatter);

  rgw_obj obj3(o);
  bufferlist bl3;
  ::encode(obj3, bl3);
  ::decode(obj3, bl3);
  encode_json("obj3", obj3, formatter);

  if (!instance.empty()) {
    obj3.set_instance(instance);
  }
  if (!ns.empty()) {
    obj3.set_ns(ns);
  }
  check_parsed_correctly(obj3, name, ns, instance);

  encode_json("obj3-2", obj3, formatter);

  formatter->close_section();

  formatter->flush(cout);

  ASSERT_EQ(obj1, obj2);
  ASSERT_EQ(obj1, obj3);


  /* rgw_obj_key conversion */
  rgw_obj_key k;
  obj1.get_index_key(&k);

  rgw_obj new_obj(b, k);

  ASSERT_EQ(obj1, new_obj);

  delete formatter;
}

TEST(TestRGWObj, underscore) {
  test_obj("_obj", "", "");
  test_obj("_obj", "ns", "");
  test_obj("_obj", "", "v1");
  test_obj("_obj", "ns", "v1");
}

TEST(TestRGWObj, no_underscore) {
  test_obj("obj", "", "");
  test_obj("obj", "ns", "");
  test_obj("obj", "", "v1");
  test_obj("obj", "ns", "v1");
}

