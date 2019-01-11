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
#include "common/Cond.h"
#include "common/errno.h"
#include "include/stringify.h"
#include <gtest/gtest.h>

class KVTest : public ::testing::TestWithParam<const char*> {
public:
  boost::scoped_ptr<KeyValueDB> db;

  KVTest() : db(0) {}

  string _bl_to_str(bufferlist val) {
    string str(val.c_str(), val.length());
    return str;
  }

  void rm_r(string path) {
    string cmd = string("rm -r ") + path;
    cout << "==> " << cmd << std::endl;
    int r = ::system(cmd.c_str());
    if (r) {
      cerr << "failed with exit code " << r
	   << ", continuing anyway" << std::endl;
    }
  }

  void init() {
    cout << "Creating " << string(GetParam()) << "\n";
    db.reset(KeyValueDB::create(g_ceph_context, string(GetParam()),
				"kv_test_temp_dir"));
  }
  void fini() {
    db.reset(NULL);
  }

  void SetUp() override {
    int r = ::mkdir("kv_test_temp_dir", 0777);
    if (r < 0 && errno != EEXIST) {
      r = -errno;
      cerr << __func__ << ": unable to create kv_test_temp_dir: "
	   << cpp_strerror(r) << std::endl;
      return;
    }
    init();
  }
  void TearDown() override {
    fini();
    rm_r("kv_test_temp_dir");
  }
};

class KVSharded : public ::testing::Test {
public:
  KeyValueDB* db;
//  KeyValueDB* sharded_db;
  KVSharded()
  : db(nullptr)/*, sharded_db(nullptr)*/ {}

  string _bl_to_str(bufferlist val) {
    string str(val.c_str(), val.length());
    return str;
  }

  void rm_r(string path) {
    string cmd = string("rm -r ") + path;
    cout << "==> " << cmd << std::endl;
    int r = ::system(cmd.c_str());
    if (r) {
      cerr << "failed with exit code " << r
           << ", continuing anyway" << std::endl;
    }
  }

  void init() {
    cout << "Creating rocksdb\n";
    db = KeyValueDB::create(g_ceph_context,
                            "rocksdb",
                            "kv_test_temp_dir");
  }
  void fini() {
    delete(db);
  }

  void SetUp() override {
    int r = ::mkdir("kv_test_temp_dir", 0777);
    if (r < 0 && errno != EEXIST) {
      r = -errno;
      cerr << __func__ << ": unable to create kv_test_temp_dir: "
           << cpp_strerror(r) << std::endl;
      return;
    }
    init();
  }
  void TearDown() override {
    fini();
    rm_r("kv_test_temp_dir");
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

/*
 * Basic write and read test case in same database session.
 */
TEST_P(KVTest, OpenWriteRead) {
  ASSERT_EQ(0, db->create_and_open(cout));
  {
    KeyValueDB::Transaction t = db->get_transaction();
    bufferlist value;
    value.append("value");
    t->set("prefix", "key", value);
    value.clear();
    value.append("value2");
    t->set("prefix", "key2", value);
    value.clear();
    value.append("value3");
    t->set("prefix", "key3", value);
    db->submit_transaction_sync(t);

    bufferlist v1, v2;
    ASSERT_EQ(0, db->get("prefix", "key", &v1));
    ASSERT_EQ(v1.length(), 5u);
    (v1.c_str())[v1.length()] = 0x0;
    ASSERT_EQ(std::string(v1.c_str()), std::string("value"));
    ASSERT_EQ(0, db->get("prefix", "key2", &v2));
    ASSERT_EQ(v2.length(), 6u);
    (v2.c_str())[v2.length()] = 0x0;
    ASSERT_EQ(std::string(v2.c_str()), std::string("value2"));
  }
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
  utime_t start = ceph_clock_now();
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
  utime_t end = ceph_clock_now();
  utime_t dur = end - start;
  cout << n << " commits in " << dur << ", avg latency " << (dur / (double)n)
       << std::endl;
  fini();
}

struct AppendMOP : public KeyValueDB::MergeOperator {
  void merge_nonexistent(
    const char *rdata, size_t rlen, std::string *new_value) override {
    *new_value = "?" + std::string(rdata, rlen);
  }
  void merge(
    const char *ldata, size_t llen,
    const char *rdata, size_t rlen,
    std::string *new_value) override {
    *new_value = std::string(ldata, llen) + std::string(rdata, rlen);
  }
  // We use each operator name and each prefix to construct the
  // overall RocksDB operator name for consistency check at open time.
  const char *name() const override {
    return "Append";
  }
};

string tostr(bufferlist& b) {
  return string(b.c_str(),b.length());
}

TEST_P(KVTest, Merge) {
  shared_ptr<KeyValueDB::MergeOperator> p(new AppendMOP);
  int r = db->set_merge_operator("A",p);
  if (r < 0)
    return; // No merge operators for this database type
  ASSERT_EQ(0, db->create_and_open(cout));
  {
    KeyValueDB::Transaction t = db->get_transaction();
    bufferlist v1, v2, v3;
    v1.append(string("1"));
    v2.append(string("2"));
    v3.append(string("3"));
    t->set("P", "K1", v1);
    t->set("A", "A1", v2);
    t->rmkey("A", "A2");
    t->merge("A", "A2", v3);
    db->submit_transaction_sync(t);
  }
  {
    bufferlist v1, v2, v3;
    ASSERT_EQ(0, db->get("P", "K1", &v1));
    ASSERT_EQ(tostr(v1), "1");
    ASSERT_EQ(0, db->get("A", "A1", &v2));
    ASSERT_EQ(tostr(v2), "2");
    ASSERT_EQ(0, db->get("A", "A2", &v3));
    ASSERT_EQ(tostr(v3), "?3");
  }
  {
    KeyValueDB::Transaction t = db->get_transaction();
    bufferlist v1;
    v1.append(string("1"));
    t->merge("A", "A2", v1);
    db->submit_transaction_sync(t);
  }
  {
    bufferlist v;
    ASSERT_EQ(0, db->get("A", "A2", &v));
    ASSERT_EQ(tostr(v), "?31");
  }
  fini();
}

TEST_P(KVTest, RMRange) {
  ASSERT_EQ(0, db->create_and_open(cout));
  bufferlist value;
  value.append("value");
  {
    KeyValueDB::Transaction t = db->get_transaction();
    t->set("prefix", "key1", value);
    t->set("prefix", "key2", value);
    t->set("prefix", "key3", value);
    t->set("prefix", "key4", value);
    t->set("prefix", "key45", value);
    t->set("prefix", "key5", value);
    t->set("prefix", "key6", value);
    db->submit_transaction_sync(t);
  }

  {
    KeyValueDB::Transaction t = db->get_transaction();
    t->set("prefix", "key7", value);
    t->set("prefix", "key8", value);
    t->rm_range_keys("prefix", "key2", "key7");
    db->submit_transaction_sync(t);
    bufferlist v1, v2;
    ASSERT_EQ(0, db->get("prefix", "key1", &v1));
    v1.clear();
    ASSERT_EQ(-ENOENT, db->get("prefix", "key45", &v1));
    ASSERT_EQ(0, db->get("prefix", "key8", &v1));
    v1.clear();
    ASSERT_EQ(-ENOENT, db->get("prefix", "key2", &v1));
    ASSERT_EQ(0, db->get("prefix", "key7", &v2));
  }

  {
    KeyValueDB::Transaction t = db->get_transaction();
    t->rm_range_keys("prefix", "key", "key");
    db->submit_transaction_sync(t);
    bufferlist v1, v2;
    ASSERT_EQ(0, db->get("prefix", "key1", &v1));
    ASSERT_EQ(0, db->get("prefix", "key8", &v2));
  }

  {
    KeyValueDB::Transaction t = db->get_transaction();
    t->rm_range_keys("prefix", "key-", "key~");
    db->submit_transaction_sync(t);
    bufferlist v1, v2;
    ASSERT_EQ(-ENOENT, db->get("prefix", "key1", &v1));
    ASSERT_EQ(-ENOENT, db->get("prefix", "key8", &v2));
  }

  fini();
}

TEST_P(KVTest, RocksDBColumnFamilyTest) {
  if(string(GetParam()) != "rocksdb")
    return;

  std::vector<KeyValueDB::ColumnFamily> cfs;
  cfs.push_back(KeyValueDB::ColumnFamily("cf1", ""));
  cfs.push_back(KeyValueDB::ColumnFamily("cf2", ""));
  ASSERT_EQ(0, db->init(g_conf()->bluestore_rocksdb_options));
  cout << "creating two column families and opening them" << std::endl;
  ASSERT_EQ(0, db->create_and_open(cout, cfs));
  {
    KeyValueDB::Transaction t = db->get_transaction();
    bufferlist value;
    value.append("value");
    cout << "write a transaction includes three keys in different CFs" << std::endl;
    t->set("prefix", "key", value);
    t->set("cf1", "key", value);
    t->set("cf2", "key2", value);
    ASSERT_EQ(0, db->submit_transaction_sync(t));
  }
  fini();

  init();
  ASSERT_EQ(0, db->open(cout, cfs));
  {
    bufferlist v1, v2, v3;
    cout << "reopen db and read those keys" << std::endl;
    ASSERT_EQ(0, db->get("prefix", "key", &v1));
    ASSERT_EQ(0, _bl_to_str(v1) != "value");
    ASSERT_EQ(0, db->get("cf1", "key", &v2));
    ASSERT_EQ(0, _bl_to_str(v2) != "value");
    ASSERT_EQ(0, db->get("cf2", "key2", &v3));
    ASSERT_EQ(0, _bl_to_str(v2) != "value");
  }
  {
    cout << "delete two keys in CFs" << std::endl;
    KeyValueDB::Transaction t = db->get_transaction();
    t->rmkey("prefix", "key");
    t->rmkey("cf2", "key2");
    ASSERT_EQ(0, db->submit_transaction_sync(t));
  }
  fini();

  init();
  ASSERT_EQ(0, db->open(cout, cfs));
  {
    cout << "reopen db and read keys again." << std::endl;
    bufferlist v1, v2, v3;
    ASSERT_EQ(-ENOENT, db->get("prefix", "key", &v1));
    ASSERT_EQ(0, db->get("cf1", "key", &v2));
    ASSERT_EQ(0, _bl_to_str(v2) != "value");
    ASSERT_EQ(-ENOENT, db->get("cf2", "key2", &v3));
  }
  fini();
}

TEST_P(KVTest, RocksDBIteratorTest) {
  if(string(GetParam()) != "rocksdb")
    return;

  std::vector<KeyValueDB::ColumnFamily> cfs;
  cfs.push_back(KeyValueDB::ColumnFamily("cf1", ""));
  ASSERT_EQ(0, db->init(g_conf()->bluestore_rocksdb_options));
  cout << "creating one column family and opening it" << std::endl;
  ASSERT_EQ(0, db->create_and_open(cout, cfs));
  {
    KeyValueDB::Transaction t = db->get_transaction();
    bufferlist bl1;
    bl1.append("hello");
    bufferlist bl2;
    bl2.append("world");
    cout << "write some kv pairs into default and new CFs" << std::endl;
    t->set("prefix", "key1", bl1);
    t->set("prefix", "key2", bl2);
    KeyValueDB::ColumnFamilyHandle cf1h;
    ASSERT_NE(cf1h = db->column_family_handle("cf1"), nullptr);

    t->select(cf1h);
    t->set("cf1", "key1", bl1);
    t->set("cf1", "key2", bl2);
    ASSERT_EQ(0, db->submit_transaction_sync(t));
  }
  {
    cout << "iterating the default CF" << std::endl;
    KeyValueDB::Iterator iter = db->get_iterator("prefix");
    iter->seek_to_first();
    ASSERT_EQ(1, iter->valid());
    ASSERT_EQ("key1", iter->key());
    ASSERT_EQ("hello", _bl_to_str(iter->value()));
    ASSERT_EQ(0, iter->next());
    ASSERT_EQ(1, iter->valid());
    ASSERT_EQ("key2", iter->key());
    ASSERT_EQ("world", _bl_to_str(iter->value()));
  }
  {
    cout << "iterating the new CF" << std::endl;
    KeyValueDB::Iterator iter = db->get_iterator("cf1");
    iter->seek_to_first();
    ASSERT_EQ(1, iter->valid());
    ASSERT_EQ("key1", iter->key());
    ASSERT_EQ("hello", _bl_to_str(iter->value()));
    ASSERT_EQ(0, iter->next());
    ASSERT_EQ(1, iter->valid());
    ASSERT_EQ("key2", iter->key());
    ASSERT_EQ("world", _bl_to_str(iter->value()));
  }
  fini();
}

TEST_P(KVTest, RocksDBColumnFamilyHandle) {
  if(string(GetParam()) != "rocksdb")
    return;

  std::vector<KeyValueDB::ColumnFamily> cfs;
  cfs.push_back(KeyValueDB::ColumnFamily("cf1", ""));
  cfs.push_back(KeyValueDB::ColumnFamily("cf2", ""));
  ASSERT_EQ(0, db->init(g_conf()->bluestore_rocksdb_options));
  cout << "creating two column families and opening them" << std::endl;
  ASSERT_EQ(0, db->create_and_open(cout, cfs));

  KeyValueDB::ColumnFamilyHandle cf1h, cf2h, cf3h;
  ASSERT_NE(cf1h = db->column_family_handle("cf1"), nullptr);
  ASSERT_NE(cf2h = db->column_family_handle("cf2"), nullptr);
  ASSERT_EQ(cf3h = db->column_family_handle("cf3"), nullptr);

  {
    KeyValueDB::Transaction t = db->get_transaction();
    bufferlist value;
    value.append("value");
    cout << "write a transaction includes three keys in different CFs" << std::endl;
    t->set("prefix", "key", value);
    t->select(cf1h);
    t->set("cf1", "key", value);
    t->select(cf2h);
    t->set("cf2", "key2", value);
    ASSERT_EQ(0, db->submit_transaction_sync(t));
  }
  fini();

  init();
  ASSERT_EQ(0, db->open(cout, cfs));
  {
    ASSERT_NE(cf1h = db->column_family_handle("cf1"), nullptr);
    ASSERT_NE(cf2h = db->column_family_handle("cf2"), nullptr);
    bufferlist v1, v2, v3;
    cout << "reopen db and read those keys" << std::endl;
    ASSERT_EQ(0, db->get("prefix", "key", &v1));
    ASSERT_EQ(0, _bl_to_str(v1) != "value");
    ASSERT_EQ(0, db->get(cf1h, "cf1", "key", &v2));
    ASSERT_EQ(0, _bl_to_str(v2) != "value");
    ASSERT_EQ(0, db->get(cf2h, "cf2", "key2", &v3));
    ASSERT_EQ(0, _bl_to_str(v2) != "value");
  }
  {
    cout << "delete two keys in CFs" << std::endl;
    KeyValueDB::Transaction t = db->get_transaction();
    t->rmkey("prefix", "key");
    t->select(cf2h);
    t->rmkey("cf2", "key2");
    ASSERT_EQ(0, db->submit_transaction_sync(t));
  }
  fini();

  init();
  ASSERT_EQ(0, db->open(cout, cfs));
  {
    ASSERT_NE(cf1h = db->column_family_handle("cf1"), nullptr);
    ASSERT_NE(cf2h = db->column_family_handle("cf2"), nullptr);
    cout << "reopen db and read keys again." << std::endl;
    bufferlist v1, v2, v3;
    ASSERT_EQ(-ENOENT, db->get("prefix", "key", &v1));
    ASSERT_EQ(0, db->get(cf1h, "cf1", "key", &v2));
    ASSERT_EQ(0, _bl_to_str(v2) != "value");
    ASSERT_EQ(-ENOENT, db->get(cf2h, "cf2", "key2", &v3));
  }
  fini();
}

TEST_P(KVTest, RocksDBIteratorColumnFamiliesTest) {
  if(string(GetParam()) != "rocksdb")
    return;

  std::vector<KeyValueDB::ColumnFamily> cfs;
  cfs.push_back(KeyValueDB::ColumnFamily("cf1", ""));
  ASSERT_EQ(0, db->init(g_conf()->bluestore_rocksdb_options));
  cout << "creating one column family and opening it" << std::endl;
  ASSERT_EQ(0, db->create_and_open(cout, cfs));
  KeyValueDB::ColumnFamilyHandle cf1h;
  ASSERT_NE(cf1h = db->column_family_handle("cf1"), nullptr);
  {
    KeyValueDB::Transaction t = db->get_transaction();
    bufferlist bl1;
    bl1.append("hello");
    bufferlist bl2;
    bl2.append("world");
    cout << "write some kv pairs into default and new CFs" << std::endl;
    t->set("prefix", "key1", bl1);
    t->set("prefix", "key2", bl2);
    t->select(cf1h);
    t->set("cf1", "key1", bl1);
    t->set("cf1", "key2", bl2);
    ASSERT_EQ(0, db->submit_transaction_sync(t));
  }
  {
    cout << "iterating the default CF" << std::endl;
    KeyValueDB::Iterator iter = db->get_iterator("prefix");
    iter->seek_to_first();
    ASSERT_EQ(1, iter->valid());
    ASSERT_EQ("key1", iter->key());
    ASSERT_EQ("hello", _bl_to_str(iter->value()));
    ASSERT_EQ(0, iter->next());
    ASSERT_EQ(1, iter->valid());
    ASSERT_EQ("key2", iter->key());
    ASSERT_EQ("world", _bl_to_str(iter->value()));
  }
  {
    cout << "iterating the specialized CF" << std::endl;
    KeyValueDB::Iterator iter = db->get_iterator_cf(cf1h, "cf1");
    iter->seek_to_first();
    ASSERT_EQ(1, iter->valid());
    ASSERT_EQ("key1", iter->key());
    ASSERT_EQ("hello", _bl_to_str(iter->value()));
    auto a = iter->raw_key();
    ASSERT_EQ("cf1", a.first);
    ASSERT_EQ("key1", a.second);
    ASSERT_EQ(0, iter->next());
    ASSERT_EQ(1, iter->valid());
    ASSERT_EQ("key2", iter->key());
    ASSERT_EQ("world", _bl_to_str(iter->value()));
    a = iter->raw_key();
    ASSERT_EQ("cf1", a.first);
    ASSERT_EQ("key2", a.second);
  }
  {
    cout << "iterating the new CF" << std::endl;
    KeyValueDB::WholeSpaceIterator iter = db->get_wholespace_iterator_cf(cf1h);
    iter->seek_to_first();
    ASSERT_EQ(1, iter->valid());
    ASSERT_EQ("key1", iter->key());
    ASSERT_EQ("hello", _bl_to_str(iter->value()));
    ASSERT_EQ(0, iter->next());
    ASSERT_EQ(1, iter->valid());
    ASSERT_EQ("key2", iter->key());
    ASSERT_EQ("world", _bl_to_str(iter->value()));
  }
  fini();
}

TEST_P(KVTest, RocksDBCFMerge) {
  if(string(GetParam()) != "rocksdb")
    return;

  shared_ptr<KeyValueDB::MergeOperator> p(new AppendMOP);
  int r = db->set_merge_operator("cf1",p);
  if (r < 0)
    return; // No merge operators for this database type
  std::vector<KeyValueDB::ColumnFamily> cfs;
  cfs.push_back(KeyValueDB::ColumnFamily("cf1", ""));
  ASSERT_EQ(0, db->init(g_conf()->bluestore_rocksdb_options));
  cout << "creating one column family and opening it" << std::endl;
  ASSERT_EQ(0, db->create_and_open(cout, cfs));

  {
    KeyValueDB::Transaction t = db->get_transaction();
    bufferlist v1, v2, v3;
    v1.append(string("1"));
    v2.append(string("2"));
    v3.append(string("3"));
    t->set("P", "K1", v1);
    t->set("cf1", "A1", v2);
    t->rmkey("cf1", "A2");
    t->merge("cf1", "A2", v3);
    db->submit_transaction_sync(t);
  }
  {
    bufferlist v1, v2, v3;
    ASSERT_EQ(0, db->get("P", "K1", &v1));
    ASSERT_EQ(tostr(v1), "1");
    ASSERT_EQ(0, db->get("cf1", "A1", &v2));
    ASSERT_EQ(tostr(v2), "2");
    ASSERT_EQ(0, db->get("cf1", "A2", &v3));
    ASSERT_EQ(tostr(v3), "?3");
  }
  {
    KeyValueDB::Transaction t = db->get_transaction();
    bufferlist v1;
    v1.append(string("1"));
    t->merge("cf1", "A2", v1);
    db->submit_transaction_sync(t);
  }
  {
    bufferlist v;
    ASSERT_EQ(0, db->get("cf1", "A2", &v));
    ASSERT_EQ(tostr(v), "?31");
  }
  fini();
}

TEST_P(KVTest, RocksDB_estimate_size) {
  if(string(GetParam()) != "rocksdb")
    GTEST_SKIP();

  std::vector<KeyValueDB::ColumnFamily> cfs;
  cfs.push_back(KeyValueDB::ColumnFamily("cf1", ""));
  ASSERT_EQ(0, db->init(g_conf()->bluestore_rocksdb_options));
  cout << "creating one column family and opening it" << std::endl;
  ASSERT_EQ(0, db->create_and_open(cout));

  for(int test = 0; test < 20; test++)
  {
    KeyValueDB::Transaction t = db->get_transaction();
    bufferlist v1;
    v1.append(string(1000, '1'));
    for (int i = 0; i < 100; i++)
      t->set("A", to_string(i%10)+to_string(1000 * test + i), v1);
    db->submit_transaction_sync(t);
    db->compact();

    int64_t size_a = db->estimate_prefix_size("A","");
    ASSERT_GT(size_a, (test + 1) * 1000 * 100 * 0.8);
    ASSERT_LT(size_a, (test + 1) * 1000 * 100 * 1.2);
    int64_t size_a1 = db->estimate_prefix_size("A","1");
    ASSERT_GT(size_a1, (test + 1) * 1000 * 100 * 0.1 * 0.8);
    ASSERT_LT(size_a1, (test + 1) * 1000 * 100 * 0.1 * 1.2);
    int64_t size_b = db->estimate_prefix_size("B","");
    ASSERT_EQ(size_b, 0);
  }

  fini();
}

TEST_P(KVTest, RocksDB_estimate_size_column_family) {
  if(string(GetParam()) != "rocksdb")
    GTEST_SKIP();

  std::vector<KeyValueDB::ColumnFamily> cfs;
  cfs.push_back(KeyValueDB::ColumnFamily("cf1", ""));
  ASSERT_EQ(0, db->init(g_conf()->bluestore_rocksdb_options));
  cout << "creating one column family and opening it" << std::endl;
  ASSERT_EQ(0, db->create_and_open(cout, cfs));

  for(int test = 0; test < 20; test++)
  {
    KeyValueDB::Transaction t = db->get_transaction();
    bufferlist v1;
    v1.append(string(1000, '1'));
    for (int i = 0; i < 100; i++)
      t->set("cf1", to_string(i%10) + to_string(1000 * test + i), v1);
    db->submit_transaction_sync(t);
    db->compact();

    int64_t size_a = db->estimate_prefix_size("cf1","");
    ASSERT_GT(size_a, (test + 1) * 1000 * 100 * 0.8);
    ASSERT_LT(size_a, (test + 1) * 1000 * 100 * 1.2);
    int64_t size_a1 = db->estimate_prefix_size("cf1","1");
    ASSERT_GT(size_a1, (test + 1) * 1000 * 100 * 0.1 * 0.8);
    ASSERT_LT(size_a1, (test + 1) * 1000 * 100 * 0.1 * 1.2);
    int64_t size_b = db->estimate_prefix_size("B","");
    ASSERT_EQ(size_b, 0);
  }

  fini();
}

INSTANTIATE_TEST_SUITE_P(
  KeyValueDB,
  KVTest,
  ::testing::Values("leveldb", "rocksdb", "memdb"));

KeyValueDB* make_BlueStore_DB_Hash(KeyValueDB*, const std::map<std::string, size_t>& = {});

TEST_F(KVSharded, basic_creation) {
  std::vector<KeyValueDB::ColumnFamily> cfs;
  std::map<std::string, size_t> shards{{"A", 1}, {"C", 4}};
  db = make_BlueStore_DB_Hash(db, shards);
  ASSERT_EQ(0, db->init(g_conf()->bluestore_rocksdb_options));
  ASSERT_EQ(0, db->create_and_open(cout, cfs));

  KeyValueDB::Transaction t = db->get_transaction();
  bufferlist value;
  value.append("value");
  t->set("A", "key", value);
  t->set("C", "key", value);
  t->set("C", "key2", value);
  ASSERT_EQ(0, db->submit_transaction_sync(t));
  bufferlist b1, b2, b3;
  ASSERT_EQ(0, db->get("A", "key", &b1));
  ASSERT_EQ(tostr(b1), "value");
  ASSERT_EQ(0, db->get("C", "key", &b2));
  ASSERT_EQ(tostr(b2), "value");
  ASSERT_EQ(0, db->get("C", "key", &b3));
  ASSERT_EQ(tostr(b3), "value");
}

TEST_F(KVSharded, massive_creation) {
  std::vector<KeyValueDB::ColumnFamily> cfs;
  std::map<std::string, size_t> shards{{"A", 1}, {"B", 2}, {"C", 3}, {"D", 4}};
  db = make_BlueStore_DB_Hash(db, shards);
  ASSERT_EQ(0, db->init(g_conf()->bluestore_rocksdb_options));
  ASSERT_EQ(0, db->create_and_open(cout, cfs));

  const std::string prefixes[]={"A", "B", "C", "D"};
  for (int i = 0; i < 50; i++) {
    KeyValueDB::Transaction t = db->get_transaction();
    for (int j = 0; j < 10; j++) {
      int v = i * 10 + j;
      bufferlist value;
      value.append(to_string(v));
      t->set(prefixes[v % 4], to_string(v), value);
    }
    ASSERT_EQ(0, db->submit_transaction_sync(t));
  }

  for (int i = 0; i < 50; i++) {
    for (int j = 0; j < 10; j++) {
      int v = i * 10 + j;
      bufferlist value;
      ASSERT_EQ(0, db->get(prefixes[v % 4], to_string(v), &value));
      ASSERT_EQ(tostr(value), to_string(v));
    }
  }
}

TEST_F(KVSharded, iterator_basic) {
  std::map<std::string, std::string> k_v;
  while (k_v.size() < 1000) {
    std::string k, v;
    do {
      k.append(1, '0' + (rand() % ('z' - '0' + 1) ));
    } while (rand() % 9);
    do {
      v.append(1, '0' + (rand() % ('z' - '0' + 1) ));
    } while (rand() % 15);
    k_v.emplace(k,v);
  }

  //std::vector<KeyValueDB::ColumnFamily> cfs;
  std::map<std::string, size_t> shards{{"A", 7}};
  db = make_BlueStore_DB_Hash(db, shards);
  ASSERT_EQ(0, db->init(g_conf()->bluestore_rocksdb_options));
  ASSERT_EQ(0, db->create_and_open(cout/*, cfs*/));

  KeyValueDB::Transaction t = db->get_transaction();
  for (auto& it: k_v) {
    bufferlist value;
    value.append(it.second);
    t->set("A", it.first, value);
  }

  ASSERT_EQ(0, db->submit_transaction_sync(t));

  KeyValueDB::WholeSpaceIterator iter = db->get_wholespace_iterator();
  iter->seek_to_first();

  ASSERT_EQ(1, iter->valid());
  while (iter->valid()) {
    std::pair<std::string, std::string> k = iter->raw_key();
    std::string v = _bl_to_str(iter->value());
    ASSERT_EQ(k_v[k.second], v);
    k_v.erase(k.second);
    iter->next();
  }
  ASSERT_EQ(k_v.size(), 0);
}

TEST_F(KVSharded, iterator_multitable) {
  std::map<char, std::map<std::string, std::string>> prefix_k_v;
  char prefix;
  do {
    std::string k, v;
    prefix = "AIOPBb"[rand() % 6];
    do {
      k.append(1, '0' + (rand() % ('z' - '0' + 1) ));
    } while (rand() % 9);
    do {
      v.append(1, '0' + (rand() % ('z' - '0' + 1) ));
    } while (rand() % 15);
    prefix_k_v[prefix].emplace(k,v);
    //std::cout << ":" << std::string(1,prefix) << " " << k << " " << v << std::endl;
  } while (prefix_k_v[prefix].size() < 1000);

  //std::vector<KeyValueDB::ColumnFamily> cfs;
  std::map<std::string, size_t> shards{
    {"A", 7}, {"I", 5}, {"O", 2}, {"P", 1}, {"B", 3}, {"b", 1} };
  db = make_BlueStore_DB_Hash(db, shards);
  ASSERT_EQ(0, db->init(g_conf()->bluestore_rocksdb_options));
  ASSERT_EQ(0, db->create_and_open(cout/*, cfs*/));

  KeyValueDB::Transaction t = db->get_transaction();
  for (auto& pit: prefix_k_v) {
    for (auto& it: pit.second) {
      bufferlist value;
      value.append(it.second);
      t->set(std::string(1,pit.first), it.first, value);
    }
  }

  ASSERT_EQ(0, db->submit_transaction_sync(t));

  KeyValueDB::WholeSpaceIterator iter = db->get_wholespace_iterator();
  iter->seek_to_first();

  ASSERT_EQ(1, iter->valid());
  while (iter->valid()) {
    std::pair<std::string, std::string> k = iter->raw_key();
    std::string v = _bl_to_str(iter->value());
    //std::cout << k.first << " " << k.second << " " << v << std::endl;
    ASSERT_EQ(prefix_k_v[k.first[0]][k.second], v);
    prefix_k_v[k.first[0]].erase(k.second);
    iter->next();
  }
  for (auto& it: prefix_k_v) {
    ASSERT_EQ(it.second.size(), 0);
  }
}



int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf.set_val(
    "enable_experimental_unrecoverable_data_corrupting_features",
    "rocksdb, memdb");
  g_ceph_context->_conf.apply_changes(nullptr);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
