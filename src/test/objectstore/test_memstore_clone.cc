// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */
#include <boost/intrusive_ptr.hpp>
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "os/ObjectStore.h"
#include <gtest/gtest.h>
#include "include/assert.h"
#include "common/errno.h"
#include "store_test_fixture.h"

#define dout_context g_ceph_context

namespace {

const coll_t cid;

ghobject_t make_ghobject(const char *oid)
{
  return ghobject_t{hobject_t{oid, "", CEPH_NOSNAP, 0, 0, ""}};
}

} // anonymous namespace

class MemStoreClone : public StoreTestFixture {
public:
  MemStoreClone()
    : StoreTestFixture("memstore")
  {}
  void SetUp() override {
    StoreTestFixture::SetUp();
    if (HasFailure()) {
      return;
    }
    ObjectStore::Transaction t;
    ch = store->create_new_collection(cid);
    t.create_collection(cid, 4);
    unsigned r = store->queue_transaction(ch, std::move(t));
    if (r != 0) {
      derr << "failed to create collection with " << cpp_strerror(r) << dendl;
    }
    ASSERT_EQ(0U, r);
  }
  void TearDown() override {
    ch.reset();
    StoreTestFixture::TearDown();
  }
};

// src 11[11 11 11 11]11
// dst 22 22 22 22 22 22
// res 22 11 11 11 11 22
TEST_F(MemStoreClone, CloneRangeAllocated)
{
  ASSERT_TRUE(store);

  const auto src = make_ghobject("src1");
  const auto dst = make_ghobject("dst1");

  bufferlist srcbl, dstbl, result, expected;
  srcbl.append("111111111111");
  dstbl.append("222222222222");
  expected.append("221111111122");

  ObjectStore::Transaction t;
  t.write(cid, src, 0, 12, srcbl);
  t.write(cid, dst, 0, 12, dstbl);
  t.clone_range(cid, src, dst, 2, 8, 2);
  ASSERT_EQ(0u, store->queue_transaction(ch, std::move(t)));
  ASSERT_EQ(12, store->read(ch, dst, 0, 12, result));
  ASSERT_EQ(expected, result);
}

// src __[__ __ __ __]__ 11 11
// dst 22 22 22 22 22 22
// res 22 00 00 00 00 22
TEST_F(MemStoreClone, CloneRangeHole)
{
  ASSERT_TRUE(store);

  const auto src = make_ghobject("src2");
  const auto dst = make_ghobject("dst2");

  bufferlist srcbl, dstbl, result, expected;
  srcbl.append("1111");
  dstbl.append("222222222222");
  expected.append("22\000\000\000\000\000\000\000\00022", 12);

  ObjectStore::Transaction t;
  t.write(cid, src, 12, 4, srcbl);
  t.write(cid, dst, 0, 12, dstbl);
  t.clone_range(cid, src, dst, 2, 8, 2);
  ASSERT_EQ(0u, store->queue_transaction(ch, std::move(t)));
  ASSERT_EQ(12, store->read(ch, dst, 0, 12, result));
  ASSERT_EQ(expected, result);
}

// src __[__ __ __ 11]11
// dst 22 22 22 22 22 22
// res 22 00 00 00 11 22
TEST_F(MemStoreClone, CloneRangeHoleStart)
{
  ASSERT_TRUE(store);

  const auto src = make_ghobject("src3");
  const auto dst = make_ghobject("dst3");

  bufferlist srcbl, dstbl, result, expected;
  srcbl.append("1111");
  dstbl.append("222222222222");
  expected.append("22\000\000\000\000\000\0001122", 12);

  ObjectStore::Transaction t;
  t.write(cid, src, 8, 4, srcbl);
  t.write(cid, dst, 0, 12, dstbl);
  t.clone_range(cid, src, dst, 2, 8, 2);
  ASSERT_EQ(0u, store->queue_transaction(ch, std::move(t)));
  ASSERT_EQ(12, store->read(ch, dst, 0, 12, result));
  ASSERT_EQ(expected, result);
}

// src 11[11 __ __ 11]11
// dst 22 22 22 22 22 22
// res 22 11 00 00 11 22
TEST_F(MemStoreClone, CloneRangeHoleMiddle)
{
  ASSERT_TRUE(store);

  const auto src = make_ghobject("src4");
  const auto dst = make_ghobject("dst4");

  bufferlist srcbl, dstbl, result, expected;
  srcbl.append("1111");
  dstbl.append("222222222222");
  expected.append("2211\000\000\000\0001122", 12);

  ObjectStore::Transaction t;
  t.write(cid, src, 0, 4, srcbl);
  t.write(cid, src, 8, 4, srcbl);
  t.write(cid, dst, 0, 12, dstbl);
  t.clone_range(cid, src, dst, 2, 8, 2);
  ASSERT_EQ(0u, store->queue_transaction(ch, std::move(t)));
  ASSERT_EQ(12, store->read(ch, dst, 0, 12, result));
  ASSERT_EQ(expected, result);
}

// src 11[11 __ __ __]__ 11 11
// dst 22 22 22 22 22 22
// res 22 11 00 00 00 22
TEST_F(MemStoreClone, CloneRangeHoleEnd)
{
  ASSERT_TRUE(store);

  const auto src = make_ghobject("src5");
  const auto dst = make_ghobject("dst5");

  bufferlist srcbl, dstbl, result, expected;
  srcbl.append("1111");
  dstbl.append("222222222222");
  expected.append("2211\000\000\000\000\000\00022", 12);

  ObjectStore::Transaction t;
  t.write(cid, src, 0, 4, srcbl);
  t.write(cid, src, 12, 4, srcbl);
  t.write(cid, dst, 0, 12, dstbl);
  t.clone_range(cid, src, dst, 2, 8, 2);
  ASSERT_EQ(0u, store->queue_transaction(ch, std::move(t)));
  ASSERT_EQ(12, store->read(ch, dst, 0, 12, result));
  ASSERT_EQ(expected, result);
}

int main(int argc, char** argv)
{
  // default to memstore
  map<string,string> defaults = {
    { "osd_objectstore", "memstore" },
    { "osd_data", "msc.test_temp_dir" },
    { "memstore_page_size", "4" }
  };

  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  auto cct = global_init(&defaults, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
