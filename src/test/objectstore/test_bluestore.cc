// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include <fcntl.h>
#include <glob.h>
#include <stdio.h>
#include <string.h>
#include <iostream>
#include <memory>
#include <time.h>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <boost/random/binomial_distribution.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>

#include "global/global_context.h"
#include "os/bluestore/BlueStore_debug.h"

#include "include/Context.h"
#include "common/buffer_instrumentation.h"
#include "common/ceph_argparse.h"
#include "common/admin_socket.h"
#include "global/global_init.h"
#include "common/ceph_mutex.h"
#include "common/Cond.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/options.h" // for the size literals
#include "common/pretty_binary.h"
#include "include/stringify.h"
#include "include/coredumpctl.h"
#include "os/kv.h"
#include "store_test_fixture.h"



using namespace std;

#define dout_context g_ceph_context

class OnBlueStore : public StoreTestFixture {
public:
  OnBlueStore()
    : StoreTestFixture("bluestore") {}
  BlueStore& bs() {
    return *dynamic_cast<BlueStore*>(store.get());
  }
  BlueStore_debug& debug() {
    return bs().debug();
  }
};

template <typename T>
int queue_transaction(
  T &store,
  ObjectStore::CollectionHandle ch,
  ObjectStore::Transaction &&t) {
  if (rand() % 2) {
    ObjectStore::Transaction t2;
    t2.append(t);
    return store->queue_transaction(ch, std::move(t2));
  } else {
    return store->queue_transaction(ch, std::move(t));
  }
}

static bool bl_eq(bufferlist& expected, bufferlist& actual)
{
  if (expected.contents_equal(actual))
    return true;

  unsigned first = 0;
  if(expected.length() != actual.length()) {
    cout << "--- buffer lengths mismatch " << std::hex
         << "expected 0x" << expected.length() << " != actual 0x"
         << actual.length() << std::dec << std::endl;
    derr << "--- buffer lengths mismatch " << std::hex
         << "expected 0x" << expected.length() << " != actual 0x"
         << actual.length() << std::dec << dendl;
  }
  auto len = std::min(expected.length(), actual.length());
  while ( first<len && expected[first] == actual[first])
    ++first;
  unsigned last = len;
  while (last > 0 && expected[last-1] == actual[last-1])
    --last;
  if(len > 0) {
    cout << "--- buffer mismatch between offset 0x" << std::hex << first
         << " and 0x" << last << ", total 0x" << len << std::dec
         << std::endl;
    derr << "--- buffer mismatch between offset 0x" << std::hex << first
         << " and 0x" << last << ", total 0x" << len << std::dec
         << dendl;
    cout << "--- expected:\n";
    expected.hexdump(cout);
    cout << "--- actual:\n";
    actual.hexdump(cout);
  }
  return false;
}

TEST_F(OnBlueStore, test_debug_transaction)
{
  int r;
  coll_t cid;
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  BlueStore::TransContext* txc;
  txc = debug().create_TransContext(ch, nullptr);
  ghobject_t oid(hobject_t(sobject_t("an_object", CEPH_NOSNAP)));
  EXPECT_NE(txc, nullptr);
  {
    bufferlist bl;
    bl.append(std::string(0x2000, 'x'));
    ObjectStore::Transaction t;
    t.write(cid, oid, 0, 0x2000, bl);
    debug().add_Transaction(txc, &t);
  }
  debug().exec_TransContext(txc);
  
  C_SaferCond c;
  txc = debug().create_TransContext(ch, &c);
  ghobject_t oid_2(hobject_t(sobject_t("another_object", CEPH_NOSNAP)));
  EXPECT_NE(txc, nullptr);
  {
    bufferlist bl;
    bl.append(std::string(0x3000, 'y'));
    ObjectStore::Transaction t;
    t.write(cid, oid_2, 0, 0x3000, bl);
    debug().add_Transaction(txc, &t);
  }
  debug().exec_TransContext(txc);
  c.wait();

  bufferlist expected_data;
  expected_data.append(std::string(0x2000, 'x'));
  bufferlist read_data;
  r = store->read(ch, oid, 0x0, 0x2000 , read_data);
  EXPECT_EQ(r, 0x2000);
  EXPECT_TRUE(bl_eq(expected_data, read_data));

  bufferlist expected_data_2;
  expected_data_2.append(std::string(0x2000, 'x'));
  bufferlist read_data_2;
  r = store->read(ch, oid, 0x0, 0x2000 , read_data_2);
  EXPECT_EQ(r, 0x2000);
  EXPECT_TRUE(bl_eq(expected_data_2, read_data_2));

  {
    ObjectStore::Transaction t;
    t.remove(cid, oid);
    t.remove(cid, oid_2);
    t.remove_collection(cid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_F(OnBlueStore, test_write_no_read)
{
  int r;
  coll_t cid;
  ghobject_t oid(hobject_t(sobject_t("an_object", CEPH_NOSNAP)));
  bufferlist bl;
  bl.append(std::string(0x2000, 'a'));
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    t.touch(cid, oid);
    t.write(cid, oid, 0, 0x2000, bl);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    interval_set<uint64_t, std::map, false> disk_used;
    debug().get_used_disk(cid, oid, disk_used);
    size_t sum = 0;
    for (auto& it : disk_used) {
      sum += it.second;
    }
    EXPECT_EQ(sum, 0x2000);
  }

  BlueStore::TransContext* txc;
  txc = debug().create_TransContext(ch, nullptr);
  EXPECT_NE(txc, nullptr);

  bufferlist data;
  data.append(string(3096, 'x'));
  debug().write_no_read(txc, cid, oid, 0, 1000, data);
  debug().exec_TransContext(txc);

  bufferlist expected_data;
  expected_data.append(std::string(1000, 'a'));
  expected_data.append(std::string(3096, 'x'));
  expected_data.append(std::string(4096, 'a'));

  bufferlist read_data;
  r = store->read(ch, oid, 0, 8192 , read_data);
  EXPECT_EQ(r, 8192);
  EXPECT_TRUE(bl_eq(expected_data, read_data));

  {
    interval_set<uint64_t, std::map, false> disk_used;
    debug().get_used_disk(cid, oid, disk_used);
    size_t sum = 0;
    for (auto& it : disk_used) {
      sum += it.second;
    }
    EXPECT_EQ(sum, 0x3000);
  }

  {
    ObjectStore::Transaction t;
    t.remove(cid, oid);
    t.remove_collection(cid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_F(OnBlueStore, test_write_no_read_intense)
{
  typedef boost::mt11213b rand_t;
  rand_t rng(0);
  boost::uniform_int<> io_range(1, 20000);
  boost::uniform_int<> colors(0, 255);
  constexpr uint32_t canvas_size = 1024*1024;
  constexpr uint32_t paint_strokes = 500;
  unique_ptr<uint8_t> canvas(new uint8_t[canvas_size]);
  memset(canvas.get(), 0, canvas_size);

  int r;
  coll_t cid;
  ghobject_t oid(hobject_t(sobject_t("an_object", CEPH_NOSNAP)));
  bufferlist bl;
  bl.append(std::string(0x2000, 'a'));
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    t.touch(cid, oid);
    t.truncate(cid, oid, canvas_size);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  for (uint32_t s = 0; s < paint_strokes; s++) {
    BlueStore::TransContext *txc;
    txc = debug().create_TransContext(ch, nullptr);
    EXPECT_NE(txc, nullptr);

    uint32_t io_size = io_range(rng);
    uint32_t io_begin = boost::uniform_int<>(0, canvas_size - io_size)(rng);
    uint8_t color = colors(rng);
    bufferlist paint;
    paint.append(string(io_size, color));
    memset(canvas.get() + io_begin, color, io_size);
    debug().write_no_read(txc, cid, oid, 0, io_begin, paint);
    debug().exec_TransContext(txc);
  }

  bufferlist expected_data;
  expected_data.append((const char*)canvas.get(), canvas_size);
  bufferlist read_data;
  r = store->read(ch, oid, 0, canvas_size , read_data);
  EXPECT_EQ(r, canvas_size);
  EXPECT_TRUE(bl_eq(expected_data, read_data));

  ch.reset();
  EXPECT_EQ(store->umount(), 0);
  EXPECT_EQ(store->mount(), 0);
  ch = store->open_collection(cid);
  read_data.clear();
  r = store->read(ch, oid, 0, canvas_size , read_data);
  EXPECT_EQ(r, canvas_size);
  EXPECT_TRUE(bl_eq(expected_data, read_data));

  {
    ObjectStore::Transaction t;
    t.remove(cid, oid);
    t.remove_collection(cid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_F(OnBlueStore, test_write_on_corrupted)
{
  int r;
  coll_t cid;
  ghobject_t oid(hobject_t(sobject_t("a_corrupted_object", CEPH_NOSNAP)));
  auto ch = store->create_new_collection(cid);
  {
    C_SaferCond c;
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    t.touch(cid, oid);
    bufferlist bl;
    bl.append(std::string(0x4567, 'a'));
    t.write(cid, oid, 0, 0x4567, bl);
    t.register_on_commit(&c);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
    c.wait();
  }
  // corrupt data on disk
  interval_set<uint64_t, std::map, false> disk_used;
  debug().get_used_disk(cid, oid, disk_used);
  for (auto& it : disk_used) {
    auto bdev = bs().get_bdev();
    bufferlist bl;
    bl.append(std::string(it.second,'b'));
    bdev->write(it.first, bl, false);
  }

  ch.reset();
  EXPECT_EQ(store->umount(), 0);
  EXPECT_EQ(store->mount(), 0);
  ch = store->open_collection(cid);
  {
    bufferlist read_data;
    r = store->read(ch, oid, 0x1234, 0x1000, read_data);
    EXPECT_EQ(r, -EIO);
  }
  {
    C_SaferCond c;
    ObjectStore::Transaction t;
    bufferlist bl;
    bl.append(std::string(0x1000, 'c'));
    t.write(cid, oid, 0x1234, 0x1000, bl);
    t.register_on_commit(&c);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
    c.wait();
  }
  {
    bufferlist read_data;
    r = store->read(ch, oid, 0x1234, 0x1000, read_data);
    EXPECT_EQ(r, 0x1000);
    bufferlist expected_data;
    expected_data.append(std::string(0x1000, 'c'));
    EXPECT_TRUE(bl_eq(expected_data, read_data));
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, oid);
    t.remove_collection(cid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

int main(int argc, char **argv) {
  auto args = argv_to_vec(argc, argv);
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  // make sure we can adjust any config settings
  g_ceph_context->_conf._clear_safe_to_start_threads();

  g_ceph_context->_conf.set_val_or_die("bluestore_fsck_on_mkfs", "false");
  g_ceph_context->_conf.set_val_or_die("bluestore_fsck_on_mount", "false");
  g_ceph_context->_conf.set_val_or_die("bluestore_fsck_on_umount", "false");
  g_ceph_context->_conf.set_val_or_die("bluestore_debug_small_allocations", "4");
  g_ceph_context->_conf.set_val_or_die("bluestore_debug_freelist", "true");
  g_ceph_context->_conf.set_val_or_die("bluestore_clone_cow", "true");
  g_ceph_context->_conf.set_val_or_die("bluestore_max_alloc_size", "196608");
  // set small cache sizes so we see trimming during Synthetic tests
  g_ceph_context->_conf.set_val_or_die("bluestore_cache_size_hdd", "4000000");
  g_ceph_context->_conf.set_val_or_die("bluestore_cache_size_ssd", "4000000");
  g_ceph_context->_conf.set_val_or_die(
  "bluestore_debug_inject_allocation_from_file_failure", "0.66");

  // very short *_max prealloc so that we fall back to async submits
  g_ceph_context->_conf.set_val_or_die("bluestore_blobid_prealloc", "10");
  g_ceph_context->_conf.set_val_or_die("bluestore_nid_prealloc", "10");
  g_ceph_context->_conf.set_val_or_die("bluestore_debug_randomize_serial_transaction",
				 "10");

  g_ceph_context->_conf.set_val_or_die("bluefs_check_volume_selector_on_umount", "true");

  g_ceph_context->_conf.set_val_or_die("bdev_debug_aio", "true");
  g_ceph_context->_conf.set_val_or_die("log_max_recent", "10000");
  g_ceph_context->_conf.set_val_or_die("bluestore_write_v2", "true");

  g_ceph_context->_conf.set_val_or_die(
    "enable_experimental_unrecoverable_data_corrupting_features", "*");
  g_ceph_context->_conf.apply_changes(nullptr);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
