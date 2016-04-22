// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <stdio.h>
#include <string.h>
#include <iostream>
#include <time.h>
#include <sys/mount.h>
#include "kv/KeyValueDB.h"
#include "include/Context.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/errno.h"
#include "include/stringify.h"
#include <gtest/gtest.h>

#if GTEST_HAS_PARAM_TEST

class KVTest : public ::testing::TestWithParam<const char*> {
public:
  boost::scoped_ptr<KeyValueDB> db;

  KVTest() : db(0) {}

  string mydirname() { return "kv_test_temp_dir_" + string(GetParam()); }

  void init() {
    cout << "Creating " << string(GetParam()) << "\n";
    db.reset(KeyValueDB::create(g_ceph_context, string(GetParam()),
				mydirname()));
  }
  void fini() {
    db.reset(NULL);
  }

  virtual void SetUp() {
    int r = ::mkdir(mydirname().c_str(), 0777);
    if (r < 0 && errno != EEXIST) {
      r = -errno;
      cerr << __func__ << ": unable to create " << mydirname()
	   << ": " << cpp_strerror(r) << std::endl;
      return;
    }
    init();
  }
  virtual void TearDown() {
    fini();
  }
};

TEST_P(KVTest, OpenClose) {
  ASSERT_EQ(0, db->create_and_open(cout));
  fini();
}

TEST_P(KVTest, OpenCloseReopenClose) {
  ASSERT_EQ(0, db->create_and_open(cout));
  fini();
  init();
  ASSERT_EQ(0, db->open(cout));
  fini();
}

TEST_P(KVTest, PutReopen) {
  ASSERT_EQ(0, db->create_and_open(cout));
  {
    KeyValueDB::Transaction t = db->get_transaction();
    bufferlist value;
    value.append("value");
    t->set("prefix", "key", value);
    t->set("prefix", "key2", value);
    t->set("prefix", "key3", value);
    db->submit_transaction_sync(t);
  }
  fini();

  init();
  ASSERT_EQ(0, db->open(cout));
  {
    bufferlist v1, v2;
    ASSERT_EQ(0, db->get("prefix", "key", &v1));
    ASSERT_EQ(v1.length(), 5u);
    ASSERT_EQ(0, db->get("prefix", "key2", &v2));
    ASSERT_EQ(v2.length(), 5u);
  }
  {
    KeyValueDB::Transaction t = db->get_transaction();
    t->rmkey("prefix", "key");
    t->rmkey("prefix", "key3");
    db->submit_transaction_sync(t);
  }
  fini();

  init();
  ASSERT_EQ(0, db->open(cout));
  {
    bufferlist v1, v2, v3;
    ASSERT_EQ(-ENOENT, db->get("prefix", "key", &v1));
    ASSERT_EQ(0, db->get("prefix", "key2", &v2));
    ASSERT_EQ(v2.length(), 5u);
    ASSERT_EQ(-ENOENT, db->get("prefix", "key3", &v3));
  }
  fini();
}

TEST_P(KVTest, BenchCommit) {
  int n = 1024;
  ASSERT_EQ(0, db->create_and_open(cout));
  utime_t start = ceph_clock_now(NULL);
  {
    cout << "priming" << std::endl;
    // prime
    bufferlist big;
    bufferptr bp(1048576);
    bp.zero();
    big.append(bp);
    for (int i=0; i<30; ++i) {
      KeyValueDB::Transaction t = db->get_transaction();
      t->set("prefix", "big" + stringify(i), big);
      db->submit_transaction_sync(t);
    }
  }
  cout << "now doing small writes" << std::endl;
  bufferlist data;
  bufferptr bp(1024);
  bp.zero();
  data.append(bp);
  for (int i=0; i<n; ++i) {
    KeyValueDB::Transaction t = db->get_transaction();
    t->set("prefix", "key" + stringify(i), data);
    db->submit_transaction_sync(t);
  }
  utime_t end = ceph_clock_now(NULL);
  utime_t dur = end - start;
  cout << n << " commits in " << dur << ", avg latency " << (dur / (double)n)
       << std::endl;
  fini();
}


INSTANTIATE_TEST_CASE_P(
  KeyValueDB,
  KVTest,
  ::testing::Values("leveldb", "rocksdb"));

#else

// Google Test may not support value-parameterized tests with some
// compilers. If we use conditional compilation to compile out all
// code referring to the gtest_main library, MSVC linker will not link
// that library at all and consequently complain about missing entry
// point defined in that library (fatal error LNK1561: entry point
// must be defined). This dummy test keeps gtest_main linked in.
TEST(DummyTest, ValueParameterizedTestsAreNotSupportedOnThisPlatform) {}

#endif

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  env_to_vec(args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->set_val(
    "enable_experimental_unrecoverable_data_corrupting_features",
    "rocksdb");
  g_ceph_context->_conf->apply_changes(NULL);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
