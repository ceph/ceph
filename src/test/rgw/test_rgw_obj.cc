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
#include "rgw/rgw_rados.h"
#include "test_rgw_common.h"
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

void check_parsed_correctly(rgw_obj& obj, const string& name, const string& ns, const string& instance)
{
  /* parse_raw_oid() */
  rgw_obj_key parsed_key;
  ASSERT_EQ(true, rgw_obj_key::parse_raw_oid(obj.get_oid(), &parsed_key));

  cout << "parsed: " << parsed_key << std::endl;

  ASSERT_EQ(name, parsed_key.name);
  ASSERT_EQ(ns, parsed_key.ns);
  ASSERT_EQ(instance, parsed_key.instance);

  /* translate_raw_obj_to_obj_in_ns() */
  rgw_obj_key tkey = parsed_key;
  string tns = ns + "foo";
  ASSERT_EQ(0, rgw_obj_key::oid_to_key_in_ns(obj.get_oid(), &tkey, tns));

  tkey = rgw_obj_key();
  tns = ns;
  ASSERT_EQ(true, rgw_obj_key::oid_to_key_in_ns(obj.get_oid(), &tkey, tns));

  cout << "parsed: " << tkey << std::endl;

  ASSERT_EQ(obj.key, tkey);

  /* strip_namespace_from_object() */

  string strip_name = obj.get_oid();
  string strip_ns, strip_instance;

  ASSERT_EQ(true, rgw_obj_key::strip_namespace_from_name(strip_name, strip_ns, strip_instance));

  cout << "stripped: " << strip_name << " ns=" << strip_ns << " i=" << strip_instance << std::endl;

  ASSERT_EQ(name, strip_name);
  ASSERT_EQ(ns, strip_ns);
  ASSERT_EQ(instance, strip_instance);
}

void test_obj(const string& name, const string& ns, const string& instance)
{
  rgw_bucket b;
  test_rgw_init_bucket(&b, "test");

  JSONFormatter *formatter = new JSONFormatter(true);

  formatter->open_object_section("test");
  rgw_obj o(b, name);
  rgw_obj obj1(o);

  if (!instance.empty()) {
    obj1.key.instance = instance;
  }
  if (!ns.empty()) {
    obj1.key.ns = ns;
  }
  
  check_parsed_correctly(obj1, name, ns, instance);
  encode_json("obj1", obj1, formatter);

  bufferlist bl;
  encode(obj1, bl);

  rgw_obj obj2;
  decode(obj2, bl);
  check_parsed_correctly(obj2, name, ns, instance);

  encode_json("obj2", obj2, formatter);

  rgw_obj obj3(o);
  bufferlist bl3;
  encode(obj3, bl3);
  decode(obj3, bl3);
  encode_json("obj3", obj3, formatter);

  if (!instance.empty()) {
    obj3.key.instance = instance;
  }
  if (!ns.empty()) {
    obj3.key.ns = ns;
  }
  check_parsed_correctly(obj3, name, ns, instance);

  encode_json("obj3-2", obj3, formatter);

  formatter->close_section();

  formatter->flush(cout);

  ASSERT_EQ(obj1, obj2);
  ASSERT_EQ(obj1, obj3);


  /* rgw_obj_key conversion */
  rgw_obj_index_key k;
  obj1.key.get_index_key(&k);

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

template <class T>
void dump(JSONFormatter& f, const string& name, const T& entity)
{
  f.open_object_section(name.c_str());
  ::encode_json(name.c_str(), entity, &f);
  f.close_section();
  f.flush(cout);
}

static void test_obj_to_raw(test_rgw_env& env, const rgw_bucket& b,
                            const string& name, const string& instance, const string& ns,
                            const string& placement_id)
{
  JSONFormatter f(true);
  dump(f, "bucket", b);
  rgw_obj obj = test_rgw_create_obj(b, name, instance, ns);
  dump(f, "obj", obj);

  rgw_obj_select s(obj);
  rgw_raw_obj raw_obj = s.get_raw_obj(env.zonegroup, env.zone_params);
  dump(f, "raw_obj", raw_obj);

  if (!placement_id.empty()) {
    ASSERT_EQ(raw_obj.pool, env.get_placement(placement_id).data_pool);
  } else {
    ASSERT_EQ(raw_obj.pool, b.explicit_placement.data_pool);
  }
  ASSERT_EQ(raw_obj.oid, test_rgw_get_obj_oid(obj));

  rgw_obj new_obj;
  rgw_raw_obj_to_obj(b, raw_obj, &new_obj);

  dump(f, "new_obj", new_obj);

  ASSERT_EQ(obj, new_obj);

}

TEST(TestRGWObj, obj_to_raw) {
  test_rgw_env env;

  rgw_bucket b;
  test_rgw_init_bucket(&b, "test");

  rgw_bucket eb;
  test_rgw_init_explicit_placement_bucket(&eb, "ebtest");

  for (auto name : { "myobj", "_myobj", "_myobj_"}) {
    for (auto inst : { "", "inst"}) {
      for (auto ns : { "", "ns"}) {
        test_obj_to_raw(env, b, name, inst, ns, env.zonegroup.default_placement.name);
        test_obj_to_raw(env, eb, name, inst, ns, string());
      }
    }
  }
}

TEST(TestRGWObj, old_to_raw) {
  JSONFormatter f(true);
  test_rgw_env env;

  old_rgw_bucket eb;
  test_rgw_init_old_bucket(&eb, "ebtest");

  for (auto name : { "myobj", "_myobj", "_myobj_"}) {
    for (string inst : { "", "inst"}) {
      for (string ns : { "", "ns"}) {
        old_rgw_obj old(eb, name);
        if (!inst.empty()) {
          old.set_instance(inst);
        }
        if (!ns.empty()) {
          old.set_ns(ns);
        }

        bufferlist bl;

        encode(old, bl);

        rgw_obj new_obj;
        rgw_raw_obj raw_obj;

        try {
          auto iter = bl.cbegin();
          decode(new_obj, iter);

          iter = bl.begin();
          decode(raw_obj, iter);
        } catch (buffer::error& err) {
          ASSERT_TRUE(false);
        }

        bl.clear();

        rgw_obj new_obj2;
        rgw_raw_obj raw_obj2;

        encode(new_obj, bl);

        dump(f, "raw_obj", raw_obj);
        dump(f, "new_obj", new_obj);
        cout << "raw=" << raw_obj << std::endl;

        try {
          auto iter = bl.cbegin();
          decode(new_obj2, iter);

          /*
            can't decode raw obj here, because we didn't encode an old versioned
            object
           */

          bl.clear();
          encode(raw_obj, bl);
          iter = bl.begin();
          decode(raw_obj2, iter);
        } catch (buffer::error& err) {
          ASSERT_TRUE(false);
        }

        dump(f, "raw_obj2", raw_obj2);
        dump(f, "new_obj2", new_obj2);
        cout << "raw2=" << raw_obj2 << std::endl;

        ASSERT_EQ(new_obj, new_obj2);
        ASSERT_EQ(raw_obj, raw_obj2);
      }
    }
  }
}
