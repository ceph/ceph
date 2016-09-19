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

namespace {

ObjectStore *g_store{nullptr};
const coll_t cid;

ghobject_t make_ghobject(const char *oid)
{
  return ghobject_t{hobject_t{oid, "", CEPH_NOSNAP, 0, 0, ""}};
}

} // anonymous namespace

// src 11[11 11 11 11]11
// dst 22 22 22 22 22 22
// res 22 11 11 11 11 22
TEST(MemStore, CloneRangeAllocated)
{
  ASSERT_TRUE(g_store);

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
  ASSERT_EQ(0u, g_store->apply_transaction(nullptr, std::move(t)));
  ASSERT_EQ(12, g_store->read(cid, dst, 0, 12, result));
  ASSERT_EQ(expected, result);
}

// src __[__ __ __ __]__ 11 11
// dst 22 22 22 22 22 22
// res 22 00 00 00 00 22
TEST(MemStore, CloneRangeHole)
{
  ASSERT_TRUE(g_store);

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
  ASSERT_EQ(0u, g_store->apply_transaction(nullptr, std::move(t)));
  ASSERT_EQ(12, g_store->read(cid, dst, 0, 12, result));
  ASSERT_EQ(expected, result);
}

// src __[__ __ __ 11]11
// dst 22 22 22 22 22 22
// res 22 00 00 00 11 22
TEST(MemStore, CloneRangeHoleStart)
{
  ASSERT_TRUE(g_store);

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
  ASSERT_EQ(0u, g_store->apply_transaction(nullptr, std::move(t)));
  ASSERT_EQ(12, g_store->read(cid, dst, 0, 12, result));
  ASSERT_EQ(expected, result);
}

// src 11[11 __ __ 11]11
// dst 22 22 22 22 22 22
// res 22 11 00 00 11 22
TEST(MemStore, CloneRangeHoleMiddle)
{
  ASSERT_TRUE(g_store);

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
  ASSERT_EQ(0u, g_store->apply_transaction(nullptr, std::move(t)));
  ASSERT_EQ(12, g_store->read(cid, dst, 0, 12, result));
  ASSERT_EQ(expected, result);
}

// src 11[11 __ __ __]__ 11 11
// dst 22 22 22 22 22 22
// res 22 11 00 00 00 22
TEST(MemStore, CloneRangeHoleEnd)
{
  ASSERT_TRUE(g_store);

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
  ASSERT_EQ(0u, g_store->apply_transaction(nullptr, std::move(t)));
  ASSERT_EQ(12, g_store->read(cid, dst, 0, 12, result));
  ASSERT_EQ(expected, result);
}

// enable boost::intrusive_ptr<CephContext>
void intrusive_ptr_add_ref(CephContext *cct) { cct->get(); }
void intrusive_ptr_release(CephContext *cct) { cct->put(); }

int main(int argc, char** argv)
{
  // default to memstore
  vector<const char*> defaults{
    "--osd_objectstore", "memstore",
    "--osd_data", "memstore_clone_temp_dir",
    "--memstore_page_size", "4",
  };

  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(&defaults, args, CEPH_ENTITY_TYPE_CLIENT,
              CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  // release g_ceph_context on exit
  boost::intrusive_ptr<CephContext> cct{g_ceph_context, false};

  // create and mount the objectstore
  std::unique_ptr<ObjectStore> store{ObjectStore::create(
      g_ceph_context,
      g_conf->osd_objectstore,
      g_conf->osd_data,
      g_conf->osd_journal,
      g_conf->osd_os_flags)};
  if (!store) {
    derr << "failed to create osd_objectstore=" << g_conf->osd_objectstore << dendl;
    return EXIT_FAILURE;
  }

  int r = store->mkfs();
  if (r < 0) {
    derr << "failed to mkfs with " << cpp_strerror(r) << dendl;
    return EXIT_FAILURE;
  }

  r = store->mount();
  if (r < 0) {
    derr << "failed to mount with " << cpp_strerror(r) << dendl;
    return EXIT_FAILURE;
  }
  g_store = store.get();

  ObjectStore::Transaction t;
  t.create_collection(cid, 4);
  r = store->apply_transaction(nullptr, std::move(t));
  if (r < 0) {
    derr << "failed to create collection with " << cpp_strerror(r) << dendl;
    return EXIT_FAILURE;
  }

  // unmount the store on exit
  auto umount = [] (ObjectStore *store) {
    int r = store->umount();
    if (r < 0) {
      derr << "failed to unmount with " << cpp_strerror(r) << dendl;
    }
    g_store = nullptr;
  };
  std::unique_ptr<ObjectStore, decltype(umount)> umounter{store.get(), umount};

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
