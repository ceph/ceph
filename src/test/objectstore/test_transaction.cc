// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Casey Bodley <cbodley@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "os/ObjectStore.h"
#include <gtest/gtest.h>
#include "common/Clock.h"
#include "include/utime.h"
#include <boost/tuple/tuple.hpp>

TEST(Transaction, MoveConstruct)
{
  auto a = ObjectStore::Transaction{};
  a.nop();
  ASSERT_FALSE(a.empty());

  // move-construct in b
  auto b = std::move(a);
  ASSERT_TRUE(a.empty());
  ASSERT_FALSE(b.empty());
}

TEST(Transaction, MoveAssign)
{
  auto a = ObjectStore::Transaction{};
  a.nop();
  ASSERT_FALSE(a.empty());

  auto b = ObjectStore::Transaction{};
  b = std::move(a); // move-assign to b
  ASSERT_TRUE(a.empty());
  ASSERT_FALSE(b.empty());
}

TEST(Transaction, CopyConstruct)
{
  auto a = ObjectStore::Transaction{};
  a.nop();
  ASSERT_FALSE(a.empty());

  auto b = a; // copy-construct in b
  ASSERT_FALSE(a.empty());
  ASSERT_FALSE(b.empty());
}

TEST(Transaction, CopyAssign)
{
  auto a = ObjectStore::Transaction{};
  a.nop();
  ASSERT_FALSE(a.empty());

  auto b = ObjectStore::Transaction{};
  b = a; // copy-assign to b
  ASSERT_FALSE(a.empty());
  ASSERT_FALSE(b.empty());
}

TEST(Transaction, Swap)
{
  auto a = ObjectStore::Transaction{};
  a.nop();
  ASSERT_FALSE(a.empty());

  auto b = ObjectStore::Transaction{};
  std::swap(a, b); // swap a and b
  ASSERT_TRUE(a.empty());
  ASSERT_FALSE(b.empty());
}

ObjectStore::Transaction generate_transaction()
{
  auto a = ObjectStore::Transaction{};
  a.nop();

  coll_t cid;
  object_t obj("test_name");
  snapid_t snap(0);
  hobject_t hoid(obj, "key", snap, 0, 0, "nspace");
  ghobject_t oid(hoid);

  coll_t acid;
  object_t aobj("another_test_name");
  snapid_t asnap(0);
  hobject_t ahoid(obj, "another_key", snap, 0, 0, "another_nspace");
  ghobject_t aoid(hoid);
  std::set<string> keys;
  keys.insert("any_1");
  keys.insert("any_2");
  keys.insert("any_3");

  bufferlist bl;
  bl.append_zero(4096);

  a.write(cid, oid, 1, 4096, bl, 0);

  a.omap_setkeys(acid, aoid, bl);

  a.omap_rmkeys(cid, aoid, keys);

  a.touch(acid, oid);

  return a;
}

TEST(Transaction, MoveRangesDelSrcObj)
{
  auto t = ObjectStore::Transaction{};
  t.nop();

  coll_t c(spg_t(pg_t(1,2), shard_id_t::NO_SHARD));

  ghobject_t o1(hobject_t("obj", "", 123, 456, -1, ""));
  ghobject_t o2(hobject_t("obj2", "", 123, 456, -1, ""));
  vector<std::pair<uint64_t, uint64_t>> move_info = {
    make_pair(1, 5),
    make_pair(10, 5)
  };

  t.touch(c, o1);
  bufferlist bl;
  bl.append("some data");
  t.write(c, o1, 1, bl.length(), bl);
  t.write(c, o1, 10, bl.length(), bl);

  t.clone(c, o1, o2);
  bl.append("some other data");
  t.write(c, o2, 1, bl.length(), bl);

  t.move_ranges_destroy_src(c, o1, o2, move_info);
}

TEST(Transaction, GetNumBytes)
{
  auto a = ObjectStore::Transaction{};
  a.nop();
  ASSERT_TRUE(a.get_encoded_bytes() == a.get_encoded_bytes_test());

  coll_t cid;
  object_t obj("test_name");
  snapid_t snap(0);
  hobject_t hoid(obj, "key", snap, 0, 0, "nspace");
  ghobject_t oid(hoid);

  coll_t acid;
  object_t aobj("another_test_name");
  snapid_t asnap(0);
  hobject_t ahoid(obj, "another_key", snap, 0, 0, "another_nspace");
  ghobject_t aoid(hoid);
  std::set<string> keys;
  keys.insert("any_1");
  keys.insert("any_2");
  keys.insert("any_3");

  bufferlist bl;
  bl.append_zero(4096);

  a.write(cid, oid, 1, 4096, bl, 0);
  ASSERT_TRUE(a.get_encoded_bytes() == a.get_encoded_bytes_test());

  a.omap_setkeys(acid, aoid, bl);
  ASSERT_TRUE(a.get_encoded_bytes() == a.get_encoded_bytes_test());

  a.omap_rmkeys(cid, aoid, keys);
  ASSERT_TRUE(a.get_encoded_bytes() == a.get_encoded_bytes_test());

  a.touch(acid, oid);
  ASSERT_TRUE(a.get_encoded_bytes() == a.get_encoded_bytes_test());
}

void bench_num_bytes(bool legacy)
{
  const int max = 2500000;
  auto a = generate_transaction();

  if (legacy) {
    cout << "get_encoded_bytes_test: ";
  } else {
    cout << "get_encoded_bytes: ";
  }

  utime_t start = ceph_clock_now(NULL);
  if (legacy) {
    for (int i = 0; i < max; ++i) {
      a.get_encoded_bytes_test();
    }
  } else {
    for (int i = 0; i < max; ++i) {
      a.get_encoded_bytes();
    }
  }

  utime_t end = ceph_clock_now(NULL);
  cout << max << " encodes in " << (end - start) << std::endl;

}

TEST(Transaction, GetNumBytesBenchLegacy)
{
   bench_num_bytes(true);
}

TEST(Transaction, GetNumBytesBenchCurrent)
{
   bench_num_bytes(false);
}
