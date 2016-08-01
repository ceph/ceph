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
#include "rgw/rgw_common.h"
#include "rgw/rgw_rados.h"
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

static void init_bucket(rgw_bucket *bucket, const char *ten, const char *name)
{
  *bucket = rgw_bucket(ten, name, ".data-pool", ".index-pool", "marker.", "bucket-id", NULL);
}

void append_head(list<rgw_obj> *objs, rgw_obj& head)
{
  objs->push_back(head);
}

void append_stripes(list<rgw_obj> *objs, RGWObjManifest& manifest, uint64_t obj_size, uint64_t stripe_size)
{
  string prefix = manifest.get_prefix();
  rgw_bucket bucket = manifest.get_head().bucket;

  int i = 0;
  for (uint64_t ofs = manifest.get_max_head_size(); ofs < obj_size; ofs += stripe_size) {
    char buf[16];
    snprintf(buf, sizeof(buf), "%d", ++i);
    string oid = prefix + buf;
  cout << "oid=" << oid << std::endl;
    rgw_obj obj;
    obj.init_ns(bucket, oid, "shadow");
    objs->push_back(obj);
  }
}

static void gen_obj(uint64_t obj_size, uint64_t head_max_size, uint64_t stripe_size,
                    RGWObjManifest *manifest, rgw_bucket *bucket, rgw_obj *head, RGWObjManifest::generator *gen,
                    list<rgw_obj> *test_objs)
{
  manifest->set_trivial_rule(head_max_size, stripe_size);

  init_bucket(bucket, "", "buck");

  *head = rgw_obj(*bucket, "oid");
  gen->create_begin(g_ceph_context, manifest, *bucket, *head);

  append_head(test_objs, *head);
  cout << "test_objs.size()=" << test_objs->size() << std::endl;
  append_stripes(test_objs, *manifest, obj_size, stripe_size);

  cout << "test_objs.size()=" << test_objs->size() << std::endl;

  ASSERT_EQ((int)manifest->get_obj_size(), 0);
  ASSERT_EQ((int)manifest->get_head_size(), 0);
  ASSERT_EQ(manifest->has_tail(), false);

  uint64_t ofs = 0;
  list<rgw_obj>::iterator iter = test_objs->begin();

  while (ofs < obj_size) {
    rgw_obj obj = gen->get_cur_obj();
cout << "obj=" << obj << std::endl;
    ASSERT_TRUE(obj == *iter);

    ofs = MIN(ofs + gen->cur_stripe_max_size(), obj_size);
    gen->create_next(ofs);

  cout << "obj=" << obj << " *iter=" << *iter << std::endl;
  cout << "test_objs.size()=" << test_objs->size() << std::endl;
    ++iter;

  }

  if (manifest->has_tail()) {
    rgw_obj obj = gen->get_cur_obj();
    ASSERT_TRUE(obj == *iter);
    ++iter;
  }
  ASSERT_TRUE(iter == test_objs->end());
  ASSERT_EQ(manifest->get_obj_size(), obj_size);
  ASSERT_EQ(manifest->get_head_size(), MIN(obj_size, head_max_size));
  ASSERT_EQ(manifest->has_tail(), (obj_size > head_max_size));
}

TEST(TestRGWManifest, head_only_obj) {
  RGWObjManifest manifest;
  rgw_bucket bucket;
  rgw_obj head;
  RGWObjManifest::generator gen;

  int obj_size = 256 * 1024;

  list<rgw_obj> objs;

  gen_obj(obj_size, 512 * 1024, 4 * 1024 * 1024, &manifest, &bucket, &head, &gen, &objs);

  cout <<  " manifest.get_obj_size()=" << manifest.get_obj_size() << std::endl;
  cout <<  " manifest.get_head_size()=" << manifest.get_head_size() << std::endl;
  list<rgw_obj>::iterator liter;

  RGWObjManifest::obj_iterator iter;
  for (iter = manifest.obj_begin(), liter = objs.begin();
       iter != manifest.obj_end() && liter != objs.end();
       ++iter, ++liter) {
    ASSERT_TRUE(*liter == iter.get_location());
  }

  ASSERT_TRUE(iter == manifest.obj_end());
  ASSERT_TRUE(liter == objs.end());

  iter = manifest.obj_find(100 * 1024);
  ASSERT_TRUE(iter.get_location() == head);
  ASSERT_EQ((int)iter.get_stripe_size(), obj_size);
}

TEST(TestRGWManifest, obj_with_head_and_tail) {
  RGWObjManifest manifest;
  rgw_bucket bucket;
  rgw_obj head;
  RGWObjManifest::generator gen;

  list<rgw_obj> objs;

  int obj_size = 21 * 1024 * 1024 + 1000;
  int stripe_size = 4 * 1024 * 1024;
  int head_size = 512 * 1024;

  gen_obj(obj_size, head_size, stripe_size, &manifest, &bucket, &head, &gen, &objs);

  list<rgw_obj>::iterator liter;

  rgw_obj last_obj;

  RGWObjManifest::obj_iterator iter;
  for (iter = manifest.obj_begin(), liter = objs.begin();
       iter != manifest.obj_end() && liter != objs.end();
       ++iter, ++liter) {
    cout << "*liter=" << *liter << " iter.get_location()=" << iter.get_location() << std::endl;
    ASSERT_TRUE(*liter == iter.get_location());

    last_obj = iter.get_location();
  }

  ASSERT_TRUE(iter == manifest.obj_end());
  ASSERT_TRUE(liter == objs.end());

  iter = manifest.obj_find(100 * 1024);
  ASSERT_TRUE(iter.get_location() == head);
  ASSERT_EQ((int)iter.get_stripe_size(), head_size);

  uint64_t ofs = 20 * 1024 * 1024 + head_size;
  iter = manifest.obj_find(ofs + 100);

  ASSERT_TRUE(iter.get_location() == last_obj);
  ASSERT_EQ(iter.get_stripe_ofs(), ofs);
  ASSERT_EQ(iter.get_stripe_size(), obj_size - ofs);
}

TEST(TestRGWManifest, multipart) {
  int num_parts = 16;
  vector <RGWObjManifest> pm(num_parts);
  rgw_bucket bucket;
  uint64_t part_size = 10 * 1024 * 1024;
  uint64_t stripe_size = 4 * 1024 * 1024;

  string upload_id = "abc123";

  for (int i = 0; i < num_parts; ++i) {
    RGWObjManifest& manifest = pm[i];
    RGWObjManifest::generator gen;
    manifest.set_prefix(upload_id);

    manifest.set_multipart_part_rule(stripe_size, i + 1);

    uint64_t ofs;
    rgw_obj head;
    for (ofs = 0; ofs < part_size; ofs += stripe_size) {
      if (ofs == 0) {
        int r = gen.create_begin(g_ceph_context, &manifest, bucket, head);
        ASSERT_EQ(r, 0);
        continue;
      }
      gen.create_next(ofs);
    }

    if (ofs > part_size) {
      gen.create_next(part_size);
    }
  }

  RGWObjManifest m;

  for (int i = 0; i < num_parts; i++) {
    m.append(pm[i]);
  }
  RGWObjManifest::obj_iterator iter;
  for (iter = m.obj_begin(); iter != m.obj_end(); ++iter) {
    RGWObjManifest::obj_iterator fiter = m.obj_find(iter.get_ofs());
    ASSERT_TRUE(fiter.get_location() == iter.get_location());
  }

  ASSERT_EQ(m.get_obj_size(), num_parts * part_size);
}

