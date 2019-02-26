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
#include "common/ceph_argparse.h"
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
                          else cout << "(" << #c << ") PASSED\n";#define EXPECT_TRUE(c) ASSERT_TRUE(c) 
#define EXPECT_TRUE(c) ASSERT_TRUE(c) 
#endif
using namespace std;

struct OldObjManifestPart {
  old_rgw_obj loc;   /* the object where the data is located */
  uint64_t loc_ofs;  /* the offset at that object where the data is located */
  uint64_t size;     /* the part size */

  OldObjManifestPart() : loc_ofs(0), size(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 2, bl);
    encode(loc, bl);
    encode(loc_ofs, bl);
    encode(size, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
     DECODE_START_LEGACY_COMPAT_LEN_32(2, 2, 2, bl);
     decode(loc, bl);
     decode(loc_ofs, bl);
     decode(size, bl);
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(list<OldObjManifestPart*>& o);
};
WRITE_CLASS_ENCODER(OldObjManifestPart)

class OldObjManifest {
protected:
  map<uint64_t, OldObjManifestPart> objs;

  uint64_t obj_size;
public:

  OldObjManifest() : obj_size(0) {}
  OldObjManifest(const OldObjManifest& rhs) {
    *this = rhs;
  }
  OldObjManifest& operator=(const OldObjManifest& rhs) {
    objs = rhs.objs;
    obj_size = rhs.obj_size;
    return *this;
  }

  const map<uint64_t, OldObjManifestPart>& get_objs() {
    return objs;
  }

  void append(uint64_t ofs, const OldObjManifestPart& part) {
    objs[ofs] = part;
    obj_size = std::max(obj_size, ofs + part.size);
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 2, bl);
    encode(obj_size, bl);
    encode(objs, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN_32(6, 2, 2, bl);
    decode(obj_size, bl);
    decode(objs, bl);
    DECODE_FINISH(bl);
  }

  bool empty() {
    return objs.empty();
  }
};
WRITE_CLASS_ENCODER(OldObjManifest)

void append_head(list<rgw_obj> *objs, rgw_obj& head)
{
  objs->push_back(head);
}

void append_stripes(list<rgw_obj> *objs, RGWObjManifest& manifest, uint64_t obj_size, uint64_t stripe_size)
{
  string prefix = manifest.get_prefix();
  rgw_bucket bucket = manifest.get_obj().bucket;

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

static void gen_obj(test_rgw_env& env, uint64_t obj_size, uint64_t head_max_size, uint64_t stripe_size,
                    RGWObjManifest *manifest, const rgw_placement_rule& placement_rule, rgw_bucket *bucket, rgw_obj *head, RGWObjManifest::generator *gen,
                    list<rgw_obj> *test_objs)
{
  manifest->set_trivial_rule(head_max_size, stripe_size);

  test_rgw_init_bucket(bucket, "buck");

  *head = rgw_obj(*bucket, "oid");
  gen->create_begin(g_ceph_context, manifest, placement_rule, nullptr, *bucket, *head);

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
    rgw_raw_obj obj = gen->get_cur_obj(env.zonegroup, env.zone_params);
    cout << "obj=" << obj << std::endl;
    rgw_raw_obj test_raw = rgw_obj_select(*iter).get_raw_obj(env.zonegroup, env.zone_params);
    ASSERT_TRUE(obj == test_raw);

    ofs = std::min(ofs + gen->cur_stripe_max_size(), obj_size);
    gen->create_next(ofs);

  cout << "obj=" << obj << " *iter=" << *iter << std::endl;
  cout << "test_objs.size()=" << test_objs->size() << std::endl;
    ++iter;

  }

  if (manifest->has_tail()) {
    rgw_raw_obj obj = gen->get_cur_obj(env.zonegroup, env.zone_params);
    rgw_raw_obj test_raw = rgw_obj_select(*iter).get_raw_obj(env.zonegroup, env.zone_params);
    ASSERT_TRUE(obj == test_raw);
    ++iter;
  }
  ASSERT_TRUE(iter == test_objs->end());
  ASSERT_EQ(manifest->get_obj_size(), obj_size);
  ASSERT_EQ(manifest->get_head_size(), std::min(obj_size, head_max_size));
  ASSERT_EQ(manifest->has_tail(), (obj_size > head_max_size));
}

static void gen_old_obj(test_rgw_env& env, uint64_t obj_size, uint64_t head_max_size, uint64_t stripe_size,
                    OldObjManifest *manifest, old_rgw_bucket *bucket, old_rgw_obj *head,
                    list<old_rgw_obj> *test_objs)
{
  test_rgw_init_old_bucket(bucket, "buck");

  *head = old_rgw_obj(*bucket, "obj");

  OldObjManifestPart part;
  part.loc = *head;
  part.size = head_max_size;
  part.loc_ofs = 0;

  manifest->append(0, part);
  test_objs->push_back(part.loc);

  string prefix;
  append_rand_alpha(g_ceph_context, prefix, prefix, 16);

  int i = 0;
  for (uint64_t ofs = head_max_size; ofs < obj_size; ofs += stripe_size, i++) {
    char buf[32];
    snprintf(buf, sizeof(buf), "%s.%d", prefix.c_str(), i);
    old_rgw_obj loc(*bucket, buf);
    loc.set_ns("shadow");
    OldObjManifestPart part;
    part.loc = loc;
    part.size = min(stripe_size, obj_size - ofs);
    part.loc_ofs = 0;

    manifest->append(ofs, part);

    test_objs->push_back(loc);
  }
}

TEST(TestRGWManifest, head_only_obj) {
  test_rgw_env env;
  RGWObjManifest manifest;
  rgw_bucket bucket;
  rgw_obj head;
  RGWObjManifest::generator gen;

  int obj_size = 256 * 1024;

  list<rgw_obj> objs;

  gen_obj(env, obj_size, 512 * 1024, 4 * 1024 * 1024, &manifest, env.zonegroup.default_placement, &bucket, &head, &gen, &objs);

  cout <<  " manifest.get_obj_size()=" << manifest.get_obj_size() << std::endl;
  cout <<  " manifest.get_head_size()=" << manifest.get_head_size() << std::endl;
  list<rgw_obj>::iterator liter;

  RGWObjManifest::obj_iterator iter;
  for (iter = manifest.obj_begin(), liter = objs.begin();
       iter != manifest.obj_end() && liter != objs.end();
       ++iter, ++liter) {
    ASSERT_TRUE(env.get_raw(*liter) == env.get_raw(iter.get_location()));
  }

  ASSERT_TRUE(iter == manifest.obj_end());
  ASSERT_TRUE(liter == objs.end());

  rgw_raw_obj raw_head;

  iter = manifest.obj_find(100 * 1024);
  ASSERT_TRUE(env.get_raw(iter.get_location()) == env.get_raw(head));
  ASSERT_EQ((int)iter.get_stripe_size(), obj_size);
}

TEST(TestRGWManifest, obj_with_head_and_tail) {
  test_rgw_env env;
  RGWObjManifest manifest;
  rgw_bucket bucket;
  rgw_obj head;
  RGWObjManifest::generator gen;

  list<rgw_obj> objs;

  int obj_size = 21 * 1024 * 1024 + 1000;
  int stripe_size = 4 * 1024 * 1024;
  int head_size = 512 * 1024;

  gen_obj(env, obj_size, head_size, stripe_size, &manifest, env.zonegroup.default_placement, &bucket, &head, &gen, &objs);

  list<rgw_obj>::iterator liter;

  rgw_obj_select last_obj;

  RGWObjManifest::obj_iterator iter;
  for (iter = manifest.obj_begin(), liter = objs.begin();
       iter != manifest.obj_end() && liter != objs.end();
       ++iter, ++liter) {
    cout << "*liter=" << *liter << " iter.get_location()=" << env.get_raw(iter.get_location()) << std::endl;
    ASSERT_TRUE(env.get_raw(*liter) == env.get_raw(iter.get_location()));

    last_obj = iter.get_location();
  }

  ASSERT_TRUE(iter == manifest.obj_end());
  ASSERT_TRUE(liter == objs.end());

  iter = manifest.obj_find(100 * 1024);
  ASSERT_TRUE(env.get_raw(iter.get_location()) == env.get_raw(head));
  ASSERT_EQ((int)iter.get_stripe_size(), head_size);

  uint64_t ofs = 20 * 1024 * 1024 + head_size;
  iter = manifest.obj_find(ofs + 100);

  ASSERT_TRUE(env.get_raw(iter.get_location()) == env.get_raw(last_obj));
  ASSERT_EQ(iter.get_stripe_ofs(), ofs);
  ASSERT_EQ(iter.get_stripe_size(), obj_size - ofs);
}

TEST(TestRGWManifest, multipart) {
  test_rgw_env env;
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
        rgw_placement_rule rule(env.zonegroup.default_placement.name, RGW_STORAGE_CLASS_STANDARD);
        int r = gen.create_begin(g_ceph_context, &manifest, rule, nullptr, bucket, head);
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
    m.append(pm[i], env.zonegroup, env.zone_params);
  }
  RGWObjManifest::obj_iterator iter;
  for (iter = m.obj_begin(); iter != m.obj_end(); ++iter) {
    RGWObjManifest::obj_iterator fiter = m.obj_find(iter.get_ofs());
    ASSERT_TRUE(env.get_raw(fiter.get_location()) == env.get_raw(iter.get_location()));
  }

  ASSERT_EQ(m.get_obj_size(), num_parts * part_size);
}

TEST(TestRGWManifest, old_obj_manifest) {
  test_rgw_env env;
  OldObjManifest old_manifest;
  old_rgw_bucket old_bucket;
  old_rgw_obj old_head;

  int obj_size = 40 * 1024 * 1024;
  uint64_t stripe_size = 4 * 1024 * 1024;
  uint64_t head_size = 512 * 1024;

  list<old_rgw_obj> old_objs;

  gen_old_obj(env, obj_size, head_size, stripe_size, &old_manifest, &old_bucket, &old_head, &old_objs);

  ASSERT_EQ(old_objs.size(), 11u);


  bufferlist bl;
  encode(old_manifest , bl);

  RGWObjManifest manifest;

  try {
    auto iter = bl.cbegin();
    decode(manifest, iter);
  } catch (buffer::error& err) {
    ASSERT_TRUE(false);
  }

  rgw_raw_obj last_obj;

  RGWObjManifest::obj_iterator iter;
  auto liter = old_objs.begin();
  for (iter = manifest.obj_begin();
       iter != manifest.obj_end() && liter != old_objs.end();
       ++iter, ++liter) {
    rgw_pool old_pool(liter->bucket.data_pool);
    string old_oid;
    prepend_old_bucket_marker(old_bucket, liter->get_object(), old_oid);
    rgw_raw_obj raw_old(old_pool, old_oid);
    cout << "*liter=" << raw_old << " iter.get_location()=" << env.get_raw(iter.get_location()) << std::endl;
    ASSERT_EQ(raw_old, env.get_raw(iter.get_location()));

    last_obj = env.get_raw(iter.get_location());
  }

  ASSERT_TRUE(liter == old_objs.end());
  ASSERT_TRUE(iter == manifest.obj_end());

}


int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

