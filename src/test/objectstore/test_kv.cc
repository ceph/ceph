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
#include "kv/RocksDBStore.h"
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

TEST_P(KVTest, OpenClose) {
  ASSERT_EQ(0, db->create_and_open(cout));
  db->close();
  db->open(cout);
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

TEST_P(KVTest, ShardingRMRange) {
  if(string(GetParam()) != "rocksdb")
    return;
  std::string cfs("O(7)=");
  ASSERT_EQ(0, db->create_and_open(cout, cfs));
  {
    KeyValueDB::Transaction t = db->get_transaction();
    for (size_t i = 0; i < 1000; i++) {
      bufferlist value;
      char* a;
      ASSERT_EQ(asprintf(&a, "key%3.3ld", i), 6);
      value.append(a);
      t->set("O", a, value);
      free(a);
    }
    db->submit_transaction_sync(t);
  }

  {
    KeyValueDB::Transaction t = db->get_transaction();
    t->rm_range_keys("O", "key277", "key467");
    db->submit_transaction_sync(t);
  }

  for (size_t i = 0; i < 1000; i++) {
    char* key;
    ASSERT_EQ(asprintf(&key, "key%3.3ld", i), 6);
    bufferlist value;
    int r = db->get("O", key, &value);
    ASSERT_EQ(r, (i >= 277 && i < 467 ? -ENOENT : 0));
    free(key);
  }

  fini();
}


TEST_P(KVTest, RocksDBColumnFamilyTest) {
  if(string(GetParam()) != "rocksdb")
    return;

  std::string cfs("cf1 cf2");
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

  std::string cfs("cf1");
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

TEST_P(KVTest, RocksDBShardingIteratorTest) {
  if(string(GetParam()) != "rocksdb")
    return;

  std::string cfs("A(6)");
  ASSERT_EQ(0, db->init(g_conf()->bluestore_rocksdb_options));
  cout << "creating one column family and opening it" << std::endl;
  ASSERT_EQ(0, db->create_and_open(cout, cfs));
  {
    KeyValueDB::Transaction t = db->get_transaction();
    for (int v = 100; v <= 999; v++) {
      std::string str = to_string(v);
      bufferlist val;
      val.append(str);
      t->set("A", str, val);
    }
    ASSERT_EQ(0, db->submit_transaction_sync(t));
  }
  {
    KeyValueDB::Iterator it = db->get_iterator("A");
    int pos = 0;
    ASSERT_EQ(it->lower_bound(to_string(pos)), 0);
    for (pos = 100; pos <= 999; pos++) {
      ASSERT_EQ(it->valid(), true);
      ASSERT_EQ(it->key(), to_string(pos));
      ASSERT_EQ(it->value().to_str(), to_string(pos));
      it->next();
    }
    ASSERT_EQ(it->valid(), false);
    pos = 999;
    ASSERT_EQ(it->lower_bound(to_string(pos)), 0);
    for (pos = 999; pos >= 100; pos--) {
      ASSERT_EQ(it->valid(), true);
      ASSERT_EQ(it->key(), to_string(pos));
      ASSERT_EQ(it->value().to_str(), to_string(pos));
      it->prev();
    }
    ASSERT_EQ(it->valid(), false);
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
  std::string cfs("cf1");
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

  std::string cfs("cf1");
  ASSERT_EQ(0, db->init(g_conf()->bluestore_rocksdb_options));
  cout << "creating one column family and opening it" << std::endl;
  ASSERT_EQ(0, db->create_and_open(cout));

  for(int test = 0; test < 20; test++)
  {
    KeyValueDB::Transaction t = db->get_transaction();
    bufferlist v1;
    v1.append(string(1000, '1'));
    for (int i = 0; i < 100; i++)
      t->set("A", to_string(rand()%100000), v1);
    db->submit_transaction_sync(t);
    db->compact();

    int64_t size_a = db->estimate_prefix_size("A","");
    ASSERT_GT(size_a, (test + 1) * 1000 * 100 * 0.5);
    ASSERT_LT(size_a, (test + 1) * 1000 * 100 * 1.5);
    int64_t size_a1 = db->estimate_prefix_size("A","1");
    ASSERT_GT(size_a1, (test + 1) * 1000 * 100 * 0.1 * 0.5);
    ASSERT_LT(size_a1, (test + 1) * 1000 * 100 * 0.1 * 1.5);
    int64_t size_b = db->estimate_prefix_size("B","");
    ASSERT_EQ(size_b, 0);
  }

  fini();
}

TEST_P(KVTest, RocksDB_estimate_size_column_family) {
  if(string(GetParam()) != "rocksdb")
    GTEST_SKIP();

  std::string cfs("cf1");
  ASSERT_EQ(0, db->init(g_conf()->bluestore_rocksdb_options));
  cout << "creating one column family and opening it" << std::endl;
  ASSERT_EQ(0, db->create_and_open(cout, cfs));

  for(int test = 0; test < 20; test++)
  {
    KeyValueDB::Transaction t = db->get_transaction();
    bufferlist v1;
    v1.append(string(1000, '1'));
    for (int i = 0; i < 100; i++)
      t->set("cf1", to_string(rand()%100000), v1);
    db->submit_transaction_sync(t);
    db->compact();

    int64_t size_a = db->estimate_prefix_size("cf1","");
    ASSERT_GT(size_a, (test + 1) * 1000 * 100 * 0.5);
    ASSERT_LT(size_a, (test + 1) * 1000 * 100 * 1.5);
    int64_t size_a1 = db->estimate_prefix_size("cf1","1");
    ASSERT_GT(size_a1, (test + 1) * 1000 * 100 * 0.1 * 0.5);
    ASSERT_LT(size_a1, (test + 1) * 1000 * 100 * 0.1 * 1.5);
    int64_t size_b = db->estimate_prefix_size("B","");
    ASSERT_EQ(size_b, 0);
  }

  fini();
}

TEST_P(KVTest, RocksDB_parse_sharding_def) {
  if(string(GetParam()) != "rocksdb")
    GTEST_SKIP();

  bool result;
  std::vector<RocksDBStore::ColumnFamily> sharding_def;
  char const* error_position = nullptr;
  std::string error_msg;

  std::string_view text_def = "A(10,0-30) B(6)=option1,option2=aaaa C";
  result = RocksDBStore::parse_sharding_def(text_def,
					    sharding_def,
					    &error_position,
					    &error_msg);

  ASSERT_EQ(result, true);
  ASSERT_EQ(error_position, nullptr);
  ASSERT_EQ(error_msg, "");
  std::cout << text_def << std::endl;
  if (error_position) std::cout << std::string(error_position - text_def.begin(), ' ') << "^" << error_msg << std::endl;

  ASSERT_EQ(sharding_def.size(), 3);
  ASSERT_EQ(sharding_def[0].name, "A");
  ASSERT_EQ(sharding_def[0].shard_cnt, 10);
  ASSERT_EQ(sharding_def[0].hash_l, 0);
  ASSERT_EQ(sharding_def[0].hash_h, 30);

  ASSERT_EQ(sharding_def[1].name, "B");
  ASSERT_EQ(sharding_def[1].shard_cnt, 6);
  ASSERT_EQ(sharding_def[1].options, "option1,option2=aaaa");
  ASSERT_EQ(sharding_def[2].name, "C");
  ASSERT_EQ(sharding_def[2].shard_cnt, 1);


  text_def = "A(10 B(6)=option C";
  result = RocksDBStore::parse_sharding_def(text_def,
					    sharding_def,
					    &error_position,
					    &error_msg);
  std::cout << text_def << std::endl;
  if (error_position)
    std::cout << std::string(error_position - text_def.begin(), ' ') << "^" << error_msg << std::endl;
  ASSERT_EQ(result, false);
  ASSERT_NE(error_position, nullptr);
  ASSERT_NE(error_msg, "");

  text_def = "A(10,1) B(6)=option C";
  result = RocksDBStore::parse_sharding_def(text_def,
					    sharding_def,
					    &error_position,
					    &error_msg);
  std::cout << text_def << std::endl;
  std::cout << std::string(error_position - text_def.begin(), ' ') << "^" << error_msg << std::endl;
  ASSERT_EQ(result, false);
  ASSERT_NE(error_position, nullptr);
  ASSERT_NE(error_msg, "");
}



class RocksDBShardingTest : public ::testing::TestWithParam<const char*> {
public:
  boost::scoped_ptr<KeyValueDB> db;

  RocksDBShardingTest() : db(0) {}

  string _bl_to_str(bufferlist val) {
    string str(val.c_str(), val.length());
    return str;
  }

  void rm_r(string path) {
    string cmd = string("rm -r ") + path;
    if (verbose)
      cout << "==> " << cmd << std::endl;
    int r = ::system(cmd.c_str());
    if (r) {
      cerr << "failed with exit code " << r
	   << ", continuing anyway" << std::endl;
    }
  }

  void SetUp() override {
    verbose = getenv("VERBOSE") && strcmp(getenv("VERBOSE"), "1") == 0;

    int r = ::mkdir("kv_test_temp_dir", 0777);
    if (r < 0 && errno != EEXIST) {
      r = -errno;
      cerr << __func__ << ": unable to create kv_test_temp_dir: "
	   << cpp_strerror(r) << std::endl;
      return;
    }
    db.reset(KeyValueDB::create(g_ceph_context, "rocksdb",
				"kv_test_temp_dir"));
    ASSERT_EQ(0, db->init(g_conf()->bluestore_rocksdb_options));
    if (verbose)
      cout << "Creating database with sharding: " << GetParam() << std::endl;
    ASSERT_EQ(0, db->create_and_open(cout, GetParam()));
  }
  void TearDown() override {
    db.reset(nullptr);
    rm_r("kv_test_temp_dir");
  }

  /*
    A - main 0/1/20
    B - shard 1/3 x 0/1/20
    C - main 0/1/20
    D - shard 1/3 x 0/1/20
    E - main 0/1/20
  */
  bool verbose;
  std::vector<std::string> sharding_defs = {
    "Betelgeuse D",
    "Betelgeuse(3) D",
    "Betelgeuse D(3)",
    "Betelgeuse(3) D(3)"};
  std::vector<std::string> prefixes = {"Ad", "Betelgeuse", "C", "D", "Evade"};
  std::vector<std::string> randoms = {"0", "1", "2", "3", "4", "5",
				      "found", "brain", "fully", "pen", "worth", "race",
				      "stand", "nodded", "whenever", "surrounded", "industrial", "skin",
				      "this", "direction", "family", "beginning", "whenever", "held",
				      "metal", "year", "like", "valuable", "softly", "whistle",
				      "perfectly", "broken", "idea", "also", "coffee", "branch",
				      "tongue", "immediately", "bent", "partly", "burn", "include",
				      "certain", "burst", "final", "smoke", "positive", "perfectly"
  };
  int R = randoms.size();

  typedef int test_id[6];
  void zero(test_id& x) {
    k = 0;
    v = 0;
    for (auto& i:x)
      i = 0;
  }
  bool end(const test_id& x) {
    return x[5] != 0;
  }
  void next(test_id& x) {
    x[0]++;
    for (int i = 0; i < 5; i++) {
      if (x[i] == 3) {
	x[i] = 0;
	++x[i + 1];
      }
    }
  }

  std::map<std::string, std::string> data;
  int k = 0;
  int v = 0;

  void generate_data(const test_id& x) {
    data.clear();
    for (int i = 0; i < 5; i++) {
      if (verbose)
	std::cout << x[i] << "-";
      switch (x[i]) {
      case 0:
	break;
      case 1:
	data[RocksDBStore::combine_strings(prefixes[i], randoms[k++ % R])] = randoms[v++ % R];
	break;
      case 2:
	std::string base = randoms[k++ % R];
	for (int j = 0; j < 10; j++) {
	  data[RocksDBStore::combine_strings(prefixes[i], base + "." + randoms[k++ % R])] = randoms[v++ % R];
	}
	break;
      }
    }
  }

  void data_to_db() {
    KeyValueDB::Transaction t = db->get_transaction();
    for (auto &d : data) {
      bufferlist v1;
      v1.append(d.second);
      string prefix;
      string key;
      RocksDBStore::split_key(d.first, &prefix, &key);
      t->set(prefix, key, v1);
      if (verbose)
	std::cout << "SET " << prefix << " " << key << std::endl;
    }
    ASSERT_EQ(db->submit_transaction_sync(t), 0);
  }

  void clear_db() {
    KeyValueDB::Transaction t = db->get_transaction();
    for (auto &d : data) {
      string prefix;
      string key;
      RocksDBStore::split_key(d.first, &prefix, &key);
      t->rmkey(prefix, key);
    }
    ASSERT_EQ(db->submit_transaction_sync(t), 0);
    //paranoid, check if db empty
    KeyValueDB::WholeSpaceIterator it = db->get_wholespace_iterator();
    ASSERT_EQ(it->seek_to_first(), 0);
    ASSERT_EQ(it->valid(), false);
  }
};

TEST_P(RocksDBShardingTest, wholespace_next) {
  test_id X;
  zero(X);
  do {
    generate_data(X);
    data_to_db();

    KeyValueDB::WholeSpaceIterator it = db->get_wholespace_iterator();
    //move forward
    auto dit = data.begin();
    int r = it->seek_to_first();
    ASSERT_EQ(r, 0);
    ASSERT_EQ(it->valid(), (dit != data.end()));

    while (dit != data.end()) {
      ASSERT_EQ(it->valid(), true);
      string prefix;
      string key;
      RocksDBStore::split_key(dit->first, &prefix, &key);
      auto raw_key = it->raw_key();
      ASSERT_EQ(raw_key.first, prefix);
      ASSERT_EQ(raw_key.second, key);
      ASSERT_EQ(it->value().to_str(), dit->second);
      if (verbose)
	std::cout << "next " << prefix << " " << key << std::endl;
      ASSERT_EQ(it->next(), 0);
      ++dit;
    }
    ASSERT_EQ(it->valid(), false);

    clear_db();
    next(X);
  } while (!end(X));
}

TEST_P(RocksDBShardingTest, wholespace_prev) {
  test_id X;
  zero(X);
  do {
    generate_data(X);
    data_to_db();

    KeyValueDB::WholeSpaceIterator it = db->get_wholespace_iterator();
    auto dit = data.rbegin();
    int r = it->seek_to_last();
    ASSERT_EQ(r, 0);
    ASSERT_EQ(it->valid(), (dit != data.rend()));

    while (dit != data.rend()) {
      ASSERT_EQ(it->valid(), true);
      string prefix;
      string key;
      RocksDBStore::split_key(dit->first, &prefix, &key);
      auto raw_key = it->raw_key();
      ASSERT_EQ(raw_key.first, prefix);
      ASSERT_EQ(raw_key.second, key);
      ASSERT_EQ(it->value().to_str(), dit->second);
      if (verbose)
	std::cout << "prev " << prefix << " " << key << std::endl;
      ASSERT_EQ(it->prev(), 0);
      ++dit;
    }
    ASSERT_EQ(it->valid(), false);

    clear_db();
    next(X);
  } while (!end(X));
}

TEST_P(RocksDBShardingTest, wholespace_lower_bound) {
  test_id X;
  zero(X);
  do {
    generate_data(X);
    data_to_db();

    KeyValueDB::WholeSpaceIterator it = db->get_wholespace_iterator();
    auto dit = data.begin();
    int r = it->seek_to_first();
    ASSERT_EQ(r, 0);
    ASSERT_EQ(it->valid(), (dit != data.end()));

    while (dit != data.end()) {
      ASSERT_EQ(it->valid(), true);
      string prefix;
      string key;
      RocksDBStore::split_key(dit->first, &prefix, &key);
      KeyValueDB::WholeSpaceIterator it1 = db->get_wholespace_iterator();
      ASSERT_EQ(it1->lower_bound(prefix, key), 0);
      ASSERT_EQ(it1->valid(), true);
      auto raw_key = it1->raw_key();
      ASSERT_EQ(raw_key.first, prefix);
      ASSERT_EQ(raw_key.second, key);
      if (verbose)
	std::cout << "lower_bound " << prefix << " " << key << std::endl;
      ASSERT_EQ(it->next(), 0);
      ++dit;
    }
    ASSERT_EQ(it->valid(), false);

    clear_db();
    next(X);
  } while (!end(X));
}

TEST_P(RocksDBShardingTest, wholespace_upper_bound) {
  test_id X;
  zero(X);
  do {
    generate_data(X);
    data_to_db();

    KeyValueDB::WholeSpaceIterator it = db->get_wholespace_iterator();
    auto dit = data.begin();
    int r = it->seek_to_first();
    ASSERT_EQ(r, 0);
    ASSERT_EQ(it->valid(), (dit != data.end()));

    while (dit != data.end()) {
      ASSERT_EQ(it->valid(), true);
      string prefix;
      string key;
      string key_minus_1;
      RocksDBStore::split_key(dit->first, &prefix, &key);
      //decrement key minimally
      key_minus_1 = key.substr(0, key.length() - 1) + std::string(1, key[key.length() - 1] - 1);
      KeyValueDB::WholeSpaceIterator it1 = db->get_wholespace_iterator();
      ASSERT_EQ(it1->upper_bound(prefix, key_minus_1), 0);
      ASSERT_EQ(it1->valid(), true);
      auto raw_key = it1->raw_key();
      ASSERT_EQ(raw_key.first, prefix);
      ASSERT_EQ(raw_key.second, key);
      if (verbose)
	std::cout << "upper_bound " << prefix << " " << key_minus_1 << std::endl;
      ASSERT_EQ(it->next(), 0);
      ++dit;
    }
    ASSERT_EQ(it->valid(), false);

    clear_db();
    next(X);
  } while (!end(X));
}

TEST_P(RocksDBShardingTest, wholespace_lookup_limits) {
  test_id X;
  zero(X);
  do {
    generate_data(X);
    data_to_db();

    //lookup before first
    if (data.size() > 0) {
      auto dit = data.begin();
      string prefix;
      string key;
      RocksDBStore::split_key(dit->first, &prefix, &key);
      KeyValueDB::WholeSpaceIterator it1 = db->get_wholespace_iterator();
      ASSERT_EQ(it1->lower_bound(" ", " "), 0);
      ASSERT_EQ(it1->valid(), true);
      auto raw_key = it1->raw_key();
      ASSERT_EQ(raw_key.first, prefix);
      ASSERT_EQ(raw_key.second, key);
    }
    //lookup after last
    KeyValueDB::WholeSpaceIterator it1 = db->get_wholespace_iterator();
    ASSERT_EQ(it1->lower_bound("~", "~"), 0);
    ASSERT_EQ(it1->valid(), false);

    clear_db();
    next(X);
  } while (!end(X));
}



class RocksDBResharding : public ::testing::Test {
public:
  boost::scoped_ptr<RocksDBStore> db;

  RocksDBResharding() : db(0) {}

  string _bl_to_str(bufferlist val) {
    string str(val.c_str(), val.length());
    return str;
  }

  void rm_r(string path) {
    string cmd = string("rm -r ") + path;
    if (verbose)
      cout << "==> " << cmd << std::endl;
    int r = ::system(cmd.c_str());
    if (r) {
      cerr << "failed with exit code " << r
	   << ", continuing anyway" << std::endl;
    }
  }

  void SetUp() override {
    verbose = getenv("VERBOSE") && strcmp(getenv("VERBOSE"), "1") == 0;

    int r = ::mkdir("kv_test_temp_dir", 0777);
    if (r < 0 && errno != EEXIST) {
      r = -errno;
      cerr << __func__ << ": unable to create kv_test_temp_dir: "
	   << cpp_strerror(r) << std::endl;
      return;
    }

    KeyValueDB* db_kv = KeyValueDB::create(g_ceph_context, "rocksdb",
					 "kv_test_temp_dir");
    RocksDBStore* db_rocks = dynamic_cast<RocksDBStore*>(db_kv);
    ceph_assert(db_rocks);
    db.reset(db_rocks);
    ASSERT_EQ(0, db->init(g_conf()->bluestore_rocksdb_options));
  }
  void TearDown() override {
    db.reset(nullptr);
    rm_r("kv_test_temp_dir");
  }

  bool verbose;
  std::vector<std::string> prefixes = {"Ad", "Betelgeuse", "C", "D", "Evade"};
  std::vector<std::string> randoms = {"0", "1", "2", "3", "4", "5",
				      "found", "brain", "fully", "pen", "worth", "race",
				      "stand", "nodded", "whenever", "surrounded", "industrial", "skin",
				      "this", "direction", "family", "beginning", "whenever", "held",
				      "metal", "year", "like", "valuable", "softly", "whistle",
				      "perfectly", "broken", "idea", "also", "coffee", "branch",
				      "tongue", "immediately", "bent", "partly", "burn", "include",
				      "certain", "burst", "final", "smoke", "positive", "perfectly"
  };
  int R = randoms.size();
  int k = 0;
  std::map<std::string, std::string> data;

  void generate_data() {
    data.clear();
    for (size_t p = 0; p < prefixes.size(); p++) {
      size_t elem_count = 1 << (( p * 3 ) + 3);
      for (size_t i = 0; i < elem_count; i++) {
	std::string key;
	for (int x = 0; x < 5; x++) {
	  key = key + randoms[rand() % R];
	}
	std::string value;
	for (int x = 0; x < 3; x++) {
	  value = value + randoms[rand() % R];
	}
	data[RocksDBStore::combine_strings(prefixes[p], key)] = value;
      }
    }
  }

  void data_to_db() {
    KeyValueDB::Transaction t = db->get_transaction();
    size_t i = 0;
    for (auto& d: data) {
      bufferlist v1;
      v1.append(d.second);
      string prefix;
      string key;
      RocksDBStore::split_key(d.first, &prefix, &key);
      t->set(prefix, key, v1);
      if (verbose)
	std::cout << "SET " << prefix << " " << key << std::endl;
      i++;
      if ((i % 1000) == 0) {
	ASSERT_EQ(db->submit_transaction_sync(t), 0);
	t.reset();
	if (verbose)
	  std::cout << "writing key to DB" << std::endl;
	t = db->get_transaction();
      }
    }
    if (verbose)
      std::cout << "writing keys to DB" << std::endl;
    ASSERT_EQ(db->submit_transaction_sync(t), 0);
  }

  void clear_db() {
    KeyValueDB::Transaction t = db->get_transaction();
    for (auto &d : data) {
      string prefix;
      string key;
      RocksDBStore::split_key(d.first, &prefix, &key);
      t->rmkey(prefix, key);
    }
    ASSERT_EQ(db->submit_transaction_sync(t), 0);
    //paranoid, check if db empty
    KeyValueDB::WholeSpaceIterator it = db->get_wholespace_iterator();
    ASSERT_EQ(it->seek_to_first(), 0);
    ASSERT_EQ(it->valid(), false);
  }

  void check_db() {
    KeyValueDB::WholeSpaceIterator it = db->get_wholespace_iterator();
    //move forward
    auto dit = data.begin();
    int r = it->seek_to_first();
    ASSERT_EQ(r, 0);
    ASSERT_EQ(it->valid(), (dit != data.end()));

    while (dit != data.end()) {
      ASSERT_EQ(it->valid(), true);
      string prefix;
      string key;
      RocksDBStore::split_key(dit->first, &prefix, &key);
      auto raw_key = it->raw_key();
      ASSERT_EQ(raw_key.first, prefix);
      ASSERT_EQ(raw_key.second, key);
      ASSERT_EQ(it->value().to_str(), dit->second);
      if (verbose)
	std::cout << "next " << prefix << " " << key << std::endl;
      ASSERT_EQ(it->next(), 0);
      ++dit;
    }
    ASSERT_EQ(it->valid(), false);
  }
};

TEST_F(RocksDBResharding, basic) {
  ASSERT_EQ(0, db->create_and_open(cout, ""));
  generate_data();
  data_to_db();
  check_db();
  db->close();
  ASSERT_EQ(db->reshard("Evade(4)"), 0);
  ASSERT_EQ(db->open(cout), 0);
  check_db();
  db->close();
}

TEST_F(RocksDBResharding, all_to_shards) {
  ASSERT_EQ(0, db->create_and_open(cout, ""));
  generate_data();
  data_to_db();
  check_db();
  db->close();
  ASSERT_EQ(db->reshard("Ad(1) Betelgeuse(1) C(1) D(1) Evade(1)"), 0);
  ASSERT_EQ(db->open(cout), 0);
  check_db();
  db->close();
}

TEST_F(RocksDBResharding, all_to_shards_and_back_again) {
  ASSERT_EQ(0, db->create_and_open(cout, ""));
  generate_data();
  data_to_db();
  check_db();
  db->close();
  ASSERT_EQ(db->reshard("Ad(1) Betelgeuse(1) C(1) D(1) Evade(1)"), 0);
  ASSERT_EQ(db->open(cout), 0);
  check_db();
  db->close();
  ASSERT_EQ(db->reshard(""), 0);
  ASSERT_EQ(db->open(cout), 0);
  check_db();
  db->close();
}

TEST_F(RocksDBResharding, resume_interrupted_at_batch) {
  ASSERT_EQ(0, db->create_and_open(cout, ""));
  generate_data();
  data_to_db();
  check_db();
  db->close();
  RocksDBStore::resharding_ctrl ctrl;
  ctrl.unittest_fail_after_first_batch = true;
  ASSERT_EQ(db->reshard("Evade(4)", &ctrl), -1000);
  ASSERT_NE(db->open(cout), 0);
  ASSERT_EQ(db->reshard("Evade(4)"), 0);
  ASSERT_EQ(db->open(cout), 0);
  check_db();
  db->close();
}

TEST_F(RocksDBResharding, resume_interrupted_at_column) {
  ASSERT_EQ(0, db->create_and_open(cout, ""));
  generate_data();
  data_to_db();
  check_db();
  db->close();
  RocksDBStore::resharding_ctrl ctrl;
  ctrl.unittest_fail_after_processing_column = true;
  ASSERT_EQ(db->reshard("Evade(4)", &ctrl), -1001);
  ASSERT_NE(db->open(cout), 0);
  ASSERT_EQ(db->reshard("Evade(4)"), 0);
  ASSERT_EQ(db->open(cout), 0);
  check_db();
  db->close();
}

TEST_F(RocksDBResharding, resume_interrupted_before_commit) {
  ASSERT_EQ(0, db->create_and_open(cout, ""));
  generate_data();
  data_to_db();
  check_db();
  db->close();
  RocksDBStore::resharding_ctrl ctrl;
  ctrl.unittest_fail_after_successful_processing = true;
  ASSERT_EQ(db->reshard("Evade(4)", &ctrl), -1002);
  ASSERT_NE(db->open(cout), 0);
  ASSERT_EQ(db->reshard("Evade(4)"), 0);
  ASSERT_EQ(db->open(cout), 0);
  check_db();
  db->close();
}

TEST_F(RocksDBResharding, prevent_incomplete_hash_change) {
  ASSERT_EQ(0, db->create_and_open(cout, "Evade(4,0-3)"));
  generate_data();
  data_to_db();
  check_db();
  db->close();
  RocksDBStore::resharding_ctrl ctrl;
  ctrl.unittest_fail_after_successful_processing = true;
  ASSERT_EQ(db->reshard("Evade(4,0-8)", &ctrl), -1002);
  ASSERT_NE(db->open(cout), 0);
  ASSERT_EQ(db->reshard("Evade(4,0-8)"), 0);
  ASSERT_EQ(db->open(cout), 0);
  check_db();
  db->close();
}

TEST_F(RocksDBResharding, change_reshard) {
  ASSERT_EQ(0, db->create_and_open(cout, "Ad(4)"));
  generate_data();
  data_to_db();
  check_db();
  db->close();
  RocksDBStore::resharding_ctrl ctrl;
  ctrl.unittest_fail_after_first_batch = true;
  ASSERT_EQ(db->reshard("C(5) D(3)", &ctrl), -1000);
  ASSERT_NE(db->open(cout), 0);
  ctrl.unittest_fail_after_first_batch = false;
  ctrl.unittest_fail_after_processing_column = true;
  ASSERT_EQ(db->reshard("C(5) Evade(2)", &ctrl), -1001);
  ASSERT_NE(db->open(cout), 0);
  ctrl.unittest_fail_after_processing_column = false;
  ctrl.unittest_fail_after_successful_processing = true;
  ASSERT_EQ(db->reshard("Evade(2) D(3)", &ctrl), -1002);
  ASSERT_NE(db->open(cout), 0);
  ASSERT_EQ(db->reshard("Ad(1) Evade(5)"), 0);
  ASSERT_EQ(db->open(cout), 0);
  check_db();
  db->close();
}


INSTANTIATE_TEST_SUITE_P(
  KeyValueDB,
  KVTest,
  ::testing::Values("leveldb", "rocksdb", "memdb"));

INSTANTIATE_TEST_SUITE_P(
  KeyValueDB,
  RocksDBShardingTest,
  ::testing::Values("Betelgeuse D",
		    "Betelgeuse(3) D",
		    "Betelgeuse D(3)",
		    "Betelgeuse(3) D(3)"));

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
