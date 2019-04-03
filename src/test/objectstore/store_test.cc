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

#include <glob.h>
#include <stdio.h>
#include <string.h>
#include <iostream>
#include <time.h>
#include <sys/mount.h>
#include <boost/scoped_ptr.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <boost/random/binomial_distribution.hpp>
#include <gtest/gtest.h>

#include "os/ObjectStore.h"
#include "os/filestore/FileStore.h"
#if defined(WITH_BLUESTORE)
#include "os/bluestore/BlueStore.h"
#endif
#include "include/Context.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/errno.h"
#include "include/stringify.h"
#include "include/coredumpctl.h"

#include "include/unordered_map.h"
#include "store_test_fixture.h"

using namespace std::placeholders;

typedef boost::mt11213b gen_type;

const uint64_t DEF_STORE_TEST_BLOCKDEV_SIZE = 10240000000;
#define dout_context g_ceph_context

#if GTEST_HAS_PARAM_TEST

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


bool sorted(const vector<ghobject_t> &in) {
  ghobject_t start;
  for (vector<ghobject_t>::const_iterator i = in.begin();
       i != in.end();
       ++i) {
    if (start > *i) {
      cout << start << " should follow " << *i << std::endl;
      return false;
    }
    start = *i;
  }
  return true;
}

class StoreTest : public StoreTestFixture,
                  public ::testing::WithParamInterface<const char*> {
public:
  StoreTest()
    : StoreTestFixture(GetParam())
  {}
  void doCompressionTest();
  void doSyntheticTest(
    int num_ops,
    uint64_t max_obj, uint64_t max_wr, uint64_t align);
};

class StoreTestDeferredSetup : public StoreTest {
  void SetUp() override {
    //do nothing
  }

protected:
  void DeferredSetup() {
    StoreTest::SetUp();
  }

public:
};

class StoreTestSpecificAUSize : public StoreTestDeferredSetup {

public:
  typedef 
    std::function<void(
	   uint64_t num_ops,
	   uint64_t max_obj,
	   uint64_t max_wr,
    	   uint64_t align)> MatrixTest;

  void StartDeferred(size_t min_alloc_size) {
    SetVal(g_conf(), "bluestore_min_alloc_size", stringify(min_alloc_size).c_str());
    DeferredSetup();
  }

private:
  // bluestore matrix testing
  uint64_t max_write = 40 * 1024;
  uint64_t max_size = 400 * 1024;
  uint64_t alignment = 0;
  uint64_t num_ops = 10000;

protected:
  string matrix_get(const char *k) {
    if (string(k) == "max_write") {
      return stringify(max_write);
    } else if (string(k) == "max_size") {
      return stringify(max_size);
    } else if (string(k) == "alignment") {
      return stringify(alignment);
    } else if (string(k) == "num_ops") {
      return stringify(num_ops);
    } else {
      char *buf;
      g_conf().get_val(k, &buf, -1);
      string v = buf;
      free(buf);
      return v;
    }
  }

  void matrix_set(const char *k, const char *v) {
    if (string(k) == "max_write") {
      max_write = atoll(v);
    } else if (string(k) == "max_size") {
      max_size = atoll(v);
    } else if (string(k) == "alignment") {
      alignment = atoll(v);
    } else if (string(k) == "num_ops") {
      num_ops = atoll(v);
    } else {
      SetVal(g_conf(), k, v);
    }
  }

  void do_matrix_choose(const char *matrix[][10],
		        int i, int pos, int num,
                        MatrixTest fn) {
    if (matrix[i][0]) {
      int count;
      for (count = 0; matrix[i][count+1]; ++count) ;
      for (int j = 1; matrix[i][j]; ++j) {
        matrix_set(matrix[i][0], matrix[i][j]);
        do_matrix_choose(matrix,
                         i + 1,
                         pos * count + j - 1, 
                         num * count, 
                         fn);
      }
    } else {
      cout << "---------------------- " << (pos + 1) << " / " << num
	   << " ----------------------" << std::endl;
      for (unsigned k=0; matrix[k][0]; ++k) {
        cout << "  " << matrix[k][0] << " = " << matrix_get(matrix[k][0])
	     << std::endl;
      }
      g_ceph_context->_conf.apply_changes(nullptr);
      fn(num_ops, max_size, max_write, alignment);
    }
  }

  void do_matrix(const char *matrix[][10],
                 MatrixTest fn) {

    if (strcmp(matrix[0][0], "bluestore_min_alloc_size") == 0) {
      int count;
      for (count = 0; matrix[0][count+1]; ++count) ;
      for (size_t j = 1; matrix[0][j]; ++j) {
        if (j > 1) {
          TearDown();
        }
        StartDeferred(strtoll(matrix[0][j], NULL, 10));
        do_matrix_choose(matrix, 1, j - 1, count, fn);
      }
    } else {
      StartDeferred(0);
      do_matrix_choose(matrix, 0, 0, 1, fn);
    }
  }

};

TEST_P(StoreTest, collect_metadata) {
  map<string,string> pm;
  store->collect_metadata(&pm);
  if (GetParam() == string("filestore")) {
    ASSERT_NE(pm.count("filestore_backend"), 0u);
    ASSERT_NE(pm.count("filestore_f_type"), 0u);
    ASSERT_NE(pm.count("backend_filestore_partition_path"), 0u);
    ASSERT_NE(pm.count("backend_filestore_dev_node"), 0u);
  }
}

TEST_P(StoreTest, Trivial) {
}

TEST_P(StoreTest, TrivialRemount) {
  int r = store->umount();
  ASSERT_EQ(0, r);
  r = store->mount();
  ASSERT_EQ(0, r);
}

TEST_P(StoreTest, SimpleRemount) {
  coll_t cid;
  ghobject_t hoid(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  ghobject_t hoid2(hobject_t(sobject_t("Object 2", CEPH_NOSNAP)));
  bufferlist bl;
  bl.append("1234512345");
  int r;
  auto ch = store->create_new_collection(cid);
  {
    cerr << "create collection + write" << std::endl;
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    t.write(cid, hoid, 0, bl.length(), bl);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ch.reset();
  r = store->umount();
  ASSERT_EQ(0, r);
  r = store->mount();
  ASSERT_EQ(0, r);
  ch = store->open_collection(cid);
  {
    ObjectStore::Transaction t;
    t.write(cid, hoid2, 0, bl.length(), bl);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove(cid, hoid2);
    t.remove_collection(cid);
    cerr << "remove collection" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ch.reset();
  r = store->umount();
  ASSERT_EQ(0, r);
  r = store->mount();
  ASSERT_EQ(0, r);
  ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
    bool exists = store->exists(ch, hoid);
    ASSERT_TRUE(!exists);
  }
  {
    ObjectStore::Transaction t;
    t.remove_collection(cid);
    cerr << "remove collection" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, IORemount) {
  coll_t cid;
  bufferlist bl;
  bl.append("1234512345");
  int r;
  auto ch = store->create_new_collection(cid);
  {
    cerr << "create collection + objects" << std::endl;
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    for (int n=1; n<=100; ++n) {
      ghobject_t hoid(hobject_t(sobject_t("Object " + stringify(n), CEPH_NOSNAP)));
      t.write(cid, hoid, 0, bl.length(), bl);
    }
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  // overwrites
  {
    cout << "overwrites" << std::endl;
    for (int n=1; n<=100; ++n) {
      ObjectStore::Transaction t;
      ghobject_t hoid(hobject_t(sobject_t("Object " + stringify(n), CEPH_NOSNAP)));
      t.write(cid, hoid, 1, bl.length(), bl);
      r = queue_transaction(store, ch, std::move(t));
      ASSERT_EQ(r, 0);
    }
  }
  ch.reset();
  r = store->umount();
  ASSERT_EQ(0, r);
  r = store->mount();
  ASSERT_EQ(0, r);
  {
    ObjectStore::Transaction t;
    for (int n=1; n<=100; ++n) {
      ghobject_t hoid(hobject_t(sobject_t("Object " + stringify(n), CEPH_NOSNAP)));
      t.remove(cid, hoid);
    }
    t.remove_collection(cid);
    auto ch = store->open_collection(cid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, UnprintableCharsName) {
  coll_t cid;
  string name = "funnychars_";
  for (unsigned i = 0; i < 256; ++i) {
    name.push_back(i);
  }
  ghobject_t oid(hobject_t(sobject_t(name, CEPH_NOSNAP)));
  int r;
  auto ch = store->create_new_collection(cid);
  {
    cerr << "create collection + object" << std::endl;
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    t.touch(cid, oid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ch.reset();
  r = store->umount();
  ASSERT_EQ(0, r);
  r = store->mount();
  ASSERT_EQ(0, r);
  {
    cout << "removing" << std::endl;
    ObjectStore::Transaction t;
    t.remove(cid, oid);
    t.remove_collection(cid);
    auto ch = store->open_collection(cid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, FiemapEmpty) {
  coll_t cid;
  int r = 0;
  ghobject_t oid(hobject_t(sobject_t("fiemap_object", CEPH_NOSNAP)));
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    t.touch(cid, oid);
    t.truncate(cid, oid, 100000);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    bufferlist bl;
    store->fiemap(ch, oid, 0, 100000, bl);
    map<uint64_t,uint64_t> m, e;
    auto p = bl.cbegin();
    decode(m, p);
    cout << " got " << m << std::endl;
    e[0] = 100000;
    EXPECT_TRUE(m == e || m.empty());
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, oid);
    t.remove_collection(cid);
    cerr << "remove collection" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, FiemapHoles) {
  const uint64_t MAX_EXTENTS = 4000;
  const uint64_t SKIP_STEP = 65536;
  coll_t cid;
  int r = 0;
  ghobject_t oid(hobject_t(sobject_t("fiemap_object", CEPH_NOSNAP)));
  bufferlist bl;
  bl.append("foo");
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    t.touch(cid, oid);
    for (uint64_t i = 0; i < MAX_EXTENTS; i++)
      t.write(cid, oid, SKIP_STEP * i, 3, bl);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    //fiemap test from 0 to SKIP_STEP * (MAX_EXTENTS - 1) + 3
    bufferlist bl;
    store->fiemap(ch, oid, 0, SKIP_STEP * (MAX_EXTENTS - 1) + 3, bl);
    map<uint64_t,uint64_t> m, e;
    auto p = bl.cbegin();
    decode(m, p);
    cout << " got " << m << std::endl;
    ASSERT_TRUE(!m.empty());
    ASSERT_GE(m[0], 3u);
    auto last = m.crbegin();
    if (m.size() == 1) {
      ASSERT_EQ(0u, last->first);
    } else if (m.size() == MAX_EXTENTS) {
      for (uint64_t i = 0; i < MAX_EXTENTS; i++) {
        ASSERT_TRUE(m.count(SKIP_STEP * i));
      }
    }
    ASSERT_GT(last->first + last->second, SKIP_STEP * (MAX_EXTENTS - 1));
  }
  {
    // fiemap test from SKIP_STEP to SKIP_STEP * (MAX_EXTENTS - 2) + 3
    bufferlist bl;
    store->fiemap(ch, oid, SKIP_STEP, SKIP_STEP * (MAX_EXTENTS - 2) + 3, bl);
    map<uint64_t,uint64_t> m, e;
    auto p = bl.cbegin();
    decode(m, p);
    cout << " got " << m << std::endl;
    ASSERT_TRUE(!m.empty());
    // kstore always returns [0, object_size] regardless of offset and length
    // FIXME: if fiemap logic in kstore is refined
    if (string(GetParam()) != "kstore") {
      ASSERT_GE(m[SKIP_STEP], 3u);
      auto last = m.crbegin();
      if (m.size() == 1) {
        ASSERT_EQ(SKIP_STEP, last->first);
      } else if (m.size() == MAX_EXTENTS - 2) {
        for (uint64_t i = 1; i < MAX_EXTENTS - 1; i++) {
	  ASSERT_TRUE(m.count(SKIP_STEP*i));
	}
      }
      ASSERT_GT(last->first + last->second, SKIP_STEP * (MAX_EXTENTS - 1));
    }
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, oid);
    t.remove_collection(cid);
    cerr << "remove collection" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, SimpleMetaColTest) {
  coll_t cid;
  int r = 0;
  {
    auto ch = store->create_new_collection(cid);
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "create collection" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove_collection(cid);
    cerr << "remove collection" << std::endl;
    auto ch = store->open_collection(cid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    auto ch = store->create_new_collection(cid);
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "add collection" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove_collection(cid);
    cerr << "remove collection" << std::endl;
    auto ch = store->open_collection(cid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, SimplePGColTest) {
  coll_t cid(spg_t(pg_t(1,2), shard_id_t::NO_SHARD));
  int r = 0;
  {
    ObjectStore::Transaction t;
    auto ch = store->create_new_collection(cid);
    t.create_collection(cid, 4);
    cerr << "create collection" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove_collection(cid);
    cerr << "remove collection" << std::endl;
    auto ch = store->open_collection(cid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 4);
    cerr << "add collection" << std::endl;
    auto ch = store->create_new_collection(cid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove_collection(cid);
    cerr << "remove collection" << std::endl;
    auto ch = store->open_collection(cid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, SimpleColPreHashTest) {
  // Firstly we will need to revert the value making sure
  // collection hint actually works
  int merge_threshold = g_ceph_context->_conf->filestore_merge_threshold;
  std::ostringstream oss;
  if (merge_threshold > 0) {
    oss << "-" << merge_threshold;
    SetVal(g_conf(), "filestore_merge_threshold", oss.str().c_str());
  }

  uint32_t pg_num = 128;

  boost::uniform_int<> pg_id_range(0, pg_num);
  gen_type rng(time(NULL));
  int pg_id = pg_id_range(rng);

  int objs_per_folder = abs(merge_threshold) * 16 * g_ceph_context->_conf->filestore_split_multiple;
  boost::uniform_int<> folders_range(5, 256);
  uint64_t expected_num_objs = (uint64_t)objs_per_folder * (uint64_t)folders_range(rng);

  coll_t cid(spg_t(pg_t(pg_id, 15), shard_id_t::NO_SHARD));
  int r;
  auto ch = store->create_new_collection(cid);
  {
    // Create a collection along with a hint
    ObjectStore::Transaction t;
    t.create_collection(cid, 5);
    cerr << "create collection" << std::endl;
    bufferlist hint;
    encode(pg_num, hint);
    encode(expected_num_objs, hint);
    t.collection_hint(cid, ObjectStore::Transaction::COLL_HINT_EXPECTED_NUM_OBJECTS, hint);
    cerr << "collection hint" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    // Remove the collection
    ObjectStore::Transaction t;
    t.remove_collection(cid);
    cerr << "remove collection" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, SmallBlockWrites) {
  int r;
  coll_t cid;
  auto ch = store->create_new_collection(cid);
  ghobject_t hoid(hobject_t(sobject_t("foo", CEPH_NOSNAP)));
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  bufferlist a;
  bufferptr ap(0x1000);
  memset(ap.c_str(), 'a', 0x1000);
  a.append(ap);
  bufferlist b;
  bufferptr bp(0x1000);
  memset(bp.c_str(), 'b', 0x1000);
  b.append(bp);
  bufferlist c;
  bufferptr cp(0x1000);
  memset(cp.c_str(), 'c', 0x1000);
  c.append(cp);
  bufferptr zp(0x1000);
  zp.zero();
  bufferlist z;
  z.append(zp);
  {
    ObjectStore::Transaction t;
    t.write(cid, hoid, 0, 0x1000, a);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    bufferlist in, exp;
    r = store->read(ch, hoid, 0, 0x4000, in);
    ASSERT_EQ(0x1000, r);
    exp.append(a);
    ASSERT_TRUE(bl_eq(exp, in));
  }
  {
    ObjectStore::Transaction t;
    t.write(cid, hoid, 0x1000, 0x1000, b);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    bufferlist in, exp;
    r = store->read(ch, hoid, 0, 0x4000, in);
    ASSERT_EQ(0x2000, r);
    exp.append(a);
    exp.append(b);
    ASSERT_TRUE(bl_eq(exp, in));
  }
  {
    ObjectStore::Transaction t;
    t.write(cid, hoid, 0x3000, 0x1000, c);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    bufferlist in, exp;
    r = store->read(ch, hoid, 0, 0x4000, in);
    ASSERT_EQ(0x4000, r);
    exp.append(a);
    exp.append(b);
    exp.append(z);
    exp.append(c);
    ASSERT_TRUE(bl_eq(exp, in));
  }
  {
    ObjectStore::Transaction t;
    t.write(cid, hoid, 0x2000, 0x1000, a);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    bufferlist in, exp;
    r = store->read(ch, hoid, 0, 0x4000, in);
    ASSERT_EQ(0x4000, r);
    exp.append(a);
    exp.append(b);
    exp.append(a);
    exp.append(c);
    ASSERT_TRUE(bl_eq(exp, in));
  }
  {
    ObjectStore::Transaction t;
    t.write(cid, hoid, 0, 0x1000, c);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    bufferlist in, exp;
    r = store->read(ch, hoid, 0, 0x4000, in);
    ASSERT_EQ(0x4000, r);
    exp.append(c);
    exp.append(b);
    exp.append(a);
    exp.append(c);
    ASSERT_TRUE(bl_eq(exp, in));
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, BufferCacheReadTest) {
  int r;
  coll_t cid;
  ghobject_t hoid(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  {
    auto ch = store->open_collection(cid);
    ASSERT_FALSE(ch);
  }
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    bool exists = store->exists(ch, hoid);
    ASSERT_TRUE(!exists);

    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    cerr << "Creating object " << hoid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    exists = store->exists(ch, hoid);
    ASSERT_EQ(true, exists);
  }
  {
    ObjectStore::Transaction t;
    bufferlist bl, newdata;
    bl.append("abcde");
    t.write(cid, hoid, 0, 5, bl);
    t.write(cid, hoid, 10, 5, bl);
    cerr << "TwinWrite" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    r = store->read(ch, hoid, 0, 15, newdata);
    ASSERT_EQ(r, 15);
    {
      bufferlist expected;
      expected.append(bl);
      expected.append_zero(5);
      expected.append(bl);
      ASSERT_TRUE(bl_eq(expected, newdata));
    }
  }
  //overwrite over the same extents
  {
    ObjectStore::Transaction t;
    bufferlist bl, newdata;
    bl.append("edcba");
    t.write(cid, hoid, 0, 5, bl);
    t.write(cid, hoid, 10, 5, bl);
    cerr << "TwinWrite" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    r = store->read(ch, hoid, 0, 15, newdata);
    ASSERT_EQ(r, 15);
    {
      bufferlist expected;
      expected.append(bl);
      expected.append_zero(5);
      expected.append(bl);
      ASSERT_TRUE(bl_eq(expected, newdata));
    }
  }
  //additional write to an unused region of some blob
  {
    ObjectStore::Transaction t;
    bufferlist bl2, newdata;
    bl2.append("1234567890");

    t.write(cid, hoid, 20, bl2.length(), bl2);
    cerr << "Append" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    r = store->read(ch, hoid, 0, 30, newdata);
    ASSERT_EQ(r, 30);
    {
      bufferlist expected;
      expected.append("edcba");
      expected.append_zero(5);
      expected.append("edcba");
      expected.append_zero(5);
      expected.append(bl2);

      ASSERT_TRUE(bl_eq(expected, newdata));
    }
  }
  //additional write to an unused region of some blob and partial owerite over existing extents
  {
    ObjectStore::Transaction t;
    bufferlist bl, bl2, bl3, newdata;
    bl.append("DCB");
    bl2.append("1234567890");
    bl3.append("BA");

    t.write(cid, hoid, 30, bl2.length(), bl2);
    t.write(cid, hoid, 1, bl.length(), bl);
    t.write(cid, hoid, 13, bl3.length(), bl3);
    cerr << "TripleWrite" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    r = store->read(ch, hoid, 0, 40, newdata);
    ASSERT_EQ(r, 40);
    {
      bufferlist expected;
      expected.append("eDCBa");
      expected.append_zero(5);
      expected.append("edcBA");
      expected.append_zero(5);
      expected.append(bl2);
      expected.append(bl2);

      ASSERT_TRUE(bl_eq(expected, newdata));
    }
  }
}

void StoreTest::doCompressionTest()
{
  int r;
  coll_t cid;
  ghobject_t hoid(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  {
    auto ch = store->open_collection(cid);
    ASSERT_FALSE(ch);
  }
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    bool exists = store->exists(ch, hoid);
    ASSERT_TRUE(!exists);

    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    cerr << "Creating object " << hoid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    exists = store->exists(ch, hoid);
    ASSERT_EQ(true, exists);
  }
  std::string data;
  data.resize(0x10000 * 4);
  for(size_t i = 0;i < data.size(); i++)
    data[i] = i / 256;
  {
    ObjectStore::Transaction t;
    bufferlist bl, newdata;
    bl.append(data);
    t.write(cid, hoid, 0, bl.length(), bl);
    cerr << "CompressibleData (4xAU) Write" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    r = store->read(ch, hoid, 0, data.size() , newdata);

    ASSERT_EQ(r, (int)data.size());
    {
      bufferlist expected;
      expected.append(data);
      ASSERT_TRUE(bl_eq(expected, newdata));
    }
    newdata.clear();
    r = store->read(ch, hoid, 0, 711 , newdata);
    ASSERT_EQ(r, 711);
    {
      bufferlist expected;
      expected.append(data.substr(0,711));
      ASSERT_TRUE(bl_eq(expected, newdata));
    }
    newdata.clear();
    r = store->read(ch, hoid, 0xf00f, data.size(), newdata);
    ASSERT_EQ(r, int(data.size() - 0xf00f) );
    {
      bufferlist expected;
      expected.append(data.substr(0xf00f));
      ASSERT_TRUE(bl_eq(expected, newdata));
    }
    {
      struct store_statfs_t statfs;
      int r = store->statfs(&statfs);
      ASSERT_EQ(r, 0);
      ASSERT_EQ(statfs.data_stored, (unsigned)data.size());
      ASSERT_LE(statfs.data_compressed, (unsigned)data.size());
      ASSERT_EQ(statfs.data_compressed_original, (unsigned)data.size());
      ASSERT_LE(statfs.data_compressed_allocated, (unsigned)data.size());
    }
  }
  std::string data2;
  data2.resize(0x10000 * 4 - 0x9000);
  for(size_t i = 0;i < data2.size(); i++)
    data2[i] = (i+1) / 256;
  {
    ObjectStore::Transaction t;
    bufferlist bl, newdata;
    bl.append(data2);
    t.write(cid, hoid, 0x8000, bl.length(), bl);
    cerr << "CompressibleData partial overwrite" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    r = store->read(ch, hoid, 0, 0x10000, newdata);
    ASSERT_EQ(r, (int)0x10000);
    {
      bufferlist expected;
      expected.append(data.substr(0, 0x8000));
      expected.append(data2.substr(0, 0x8000));
      ASSERT_TRUE(bl_eq(expected, newdata));
    }
    newdata.clear();
    r = store->read(ch, hoid, 0x9000, 711 , newdata);
    ASSERT_EQ(r, 711);
    {
      bufferlist expected;
      expected.append(data2.substr(0x1000,711));
      ASSERT_TRUE(bl_eq(expected, newdata));
    }
    newdata.clear();
    r = store->read(ch, hoid, 0x0, 0x40000, newdata);
    ASSERT_EQ(r, int(0x40000) );
    {
      bufferlist expected;
      expected.append(data.substr(0, 0x8000));
      expected.append(data2.substr(0, 0x37000));
      expected.append(data.substr(0x3f000, 0x1000));
      ASSERT_TRUE(bl_eq(expected, newdata));
    }
  }
  data2.resize(0x3f000);
  for(size_t i = 0;i < data2.size(); i++)
    data2[i] = (i+2) / 256;
  {
    ObjectStore::Transaction t;
    bufferlist bl, newdata;
    bl.append(data2);
    t.write(cid, hoid, 0, bl.length(), bl);
    cerr << "CompressibleData partial overwrite, two extents overlapped, single one to be removed" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    r = store->read(ch, hoid, 0, 0x3e000 - 1, newdata);
    ASSERT_EQ(r, (int)0x3e000 - 1);
    {
      bufferlist expected;
      expected.append(data2.substr(0, 0x3e000 - 1));
      ASSERT_TRUE(bl_eq(expected, newdata));
    }
    newdata.clear();
    r = store->read(ch, hoid, 0x3e000-1, 0x2001, newdata);
    ASSERT_EQ(r, 0x2001);
    {
      bufferlist expected;
      expected.append(data2.substr(0x3e000-1, 0x1001));
      expected.append(data.substr(0x3f000, 0x1000));
      ASSERT_TRUE(bl_eq(expected, newdata));
    }
    newdata.clear();
    r = store->read(ch, hoid, 0x0, 0x40000, newdata);
    ASSERT_EQ(r, int(0x40000) );
    {
      bufferlist expected;
      expected.append(data2.substr(0, 0x3f000));
      expected.append(data.substr(0x3f000, 0x1000));
      ASSERT_TRUE(bl_eq(expected, newdata));
    }
  }
  data.resize(0x1001);
  for(size_t i = 0;i < data.size(); i++)
    data[i] = (i+3) / 256;
  {
    ObjectStore::Transaction t;
    bufferlist bl, newdata;
    bl.append(data);
    t.write(cid, hoid, 0x3f000-1, bl.length(), bl);
    cerr << "Small chunk partial overwrite, two extents overlapped, single one to be removed" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    r = store->read(ch, hoid, 0x3e000, 0x2000, newdata);
    ASSERT_EQ(r, (int)0x2000);
    {
      bufferlist expected;
      expected.append(data2.substr(0x3e000, 0x1000 - 1));
      expected.append(data.substr(0, 0x1001));
      ASSERT_TRUE(bl_eq(expected, newdata));
    }
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    cerr << "Cleaning object" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  //force fsck
  ch.reset();
  EXPECT_EQ(store->umount(), 0);
  ASSERT_EQ(store->fsck(false), 0); // do fsck explicitly
  EXPECT_EQ(store->mount(), 0);
  ch = store->open_collection(cid);
  auto settingsBookmark = BookmarkSettings();
  SetVal(g_conf(), "bluestore_compression_min_blob_size", "262144");
  g_ceph_context->_conf.apply_changes(nullptr);
  {
    data.resize(0x10000*6);

    for(size_t i = 0;i < data.size(); i++)
      data[i] = i / 256;
    ObjectStore::Transaction t;
    bufferlist bl, newdata;
    bl.append(data);
    t.write(cid, hoid, 0, bl.length(), bl);
    cerr << "CompressibleData large blob" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  //force fsck
  ch.reset();
  EXPECT_EQ(store->umount(), 0);
  ASSERT_EQ(store->fsck(false), 0); // do fsck explicitly
  EXPECT_EQ(store->mount(), 0);
  ch = store->open_collection(cid);
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, CompressionTest) {
  if (string(GetParam()) != "bluestore")
    return;

  SetVal(g_conf(), "bluestore_compression_algorithm", "snappy");
  SetVal(g_conf(), "bluestore_compression_mode", "force");
  g_ceph_context->_conf.apply_changes(nullptr);
  doCompressionTest();

  SetVal(g_conf(), "bluestore_compression_algorithm", "zlib");
  SetVal(g_conf(), "bluestore_compression_mode", "aggressive");
  g_ceph_context->_conf.apply_changes(nullptr);
  doCompressionTest();
}

TEST_P(StoreTest, SimpleObjectTest) {
  int r;
  coll_t cid;
  ghobject_t hoid(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  {
    auto ch = store->open_collection(cid);
    ASSERT_FALSE(ch);
  }
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    bool exists = store->exists(ch, hoid);
    ASSERT_TRUE(!exists);

    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    cerr << "Creating object " << hoid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    exists = store->exists(ch, hoid);
    ASSERT_EQ(true, exists);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.touch(cid, hoid);
    cerr << "Remove then create" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    bufferlist bl, orig;
    bl.append("abcde");
    orig = bl;
    t.remove(cid, hoid);
    t.write(cid, hoid, 0, 5, bl);
    cerr << "Remove then create" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    bufferlist in;
    r = store->read(ch, hoid, 0, 5, in);
    ASSERT_EQ(5, r);
    ASSERT_TRUE(bl_eq(orig, in));
  }
  {
    ObjectStore::Transaction t;
    bufferlist bl, exp;
    bl.append("abcde");
    exp = bl;
    exp.append(bl);
    t.write(cid, hoid, 5, 5, bl);
    cerr << "Append" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    bufferlist in;
    r = store->read(ch, hoid, 0, 10, in);
    ASSERT_EQ(10, r);
    ASSERT_TRUE(bl_eq(exp, in));
  }
  {
    ObjectStore::Transaction t;
    bufferlist bl, exp;
    bl.append("abcdeabcde");
    exp = bl;
    t.write(cid, hoid, 0, 10, bl);
    cerr << "Full overwrite" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    bufferlist in;
    r = store->read(ch, hoid, 0, 10, in);
    ASSERT_EQ(10, r);
    ASSERT_TRUE(bl_eq(exp, in));
  }
  {
    ObjectStore::Transaction t;
    bufferlist bl;
    bl.append("abcde");
    t.write(cid, hoid, 3, 5, bl);
    cerr << "Partial overwrite" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    bufferlist in, exp;
    exp.append("abcabcdede");
    r = store->read(ch, hoid, 0, 10, in);
    ASSERT_EQ(10, r);
    in.hexdump(cout);
    ASSERT_TRUE(bl_eq(exp, in));
  }
  {
    {
      ObjectStore::Transaction t;
      bufferlist bl;
      bl.append("fghij");
      t.truncate(cid, hoid, 0);
      t.write(cid, hoid, 5, 5, bl);
      cerr << "Truncate + hole" << std::endl;
      r = queue_transaction(store, ch, std::move(t));
      ASSERT_EQ(r, 0);
    }
    {
      ObjectStore::Transaction t;
      bufferlist bl;
      bl.append("abcde");
      t.write(cid, hoid, 0, 5, bl);
      cerr << "Reverse fill-in" << std::endl;
      r = queue_transaction(store, ch, std::move(t));
      ASSERT_EQ(r, 0);
    }

    bufferlist in, exp;
    exp.append("abcdefghij");
    r = store->read(ch, hoid, 0, 10, in);
    ASSERT_EQ(10, r);
    in.hexdump(cout);
    ASSERT_TRUE(bl_eq(exp, in));
  }
  {
    ObjectStore::Transaction t;
    bufferlist bl;
    bl.append("abcde01234012340123401234abcde01234012340123401234abcde01234012340123401234abcde01234012340123401234");
    t.write(cid, hoid, 0, bl.length(), bl);
    cerr << "larger overwrite" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    bufferlist in;
    r = store->read(ch, hoid, 0, bl.length(), in);
    ASSERT_EQ((int)bl.length(), r);
    in.hexdump(cout);
    ASSERT_TRUE(bl_eq(bl, in));
  }
  {
    bufferlist bl;
    bl.append("abcde01234012340123401234abcde01234012340123401234abcde01234012340123401234abcde01234012340123401234");

    //test: offset=len=0 mean read all data
    bufferlist in;
    r = store->read(ch, hoid, 0, 0, in);
    ASSERT_EQ((int)bl.length(), r);
    in.hexdump(cout);
    ASSERT_TRUE(bl_eq(bl, in));
  }
  {
    //verifying unaligned csums
    std::string s1("1"), s2(0x1000, '2'), s3("00");
    {
      ObjectStore::Transaction t;
      bufferlist bl;
      bl.append(s1);
      bl.append(s2);
      t.truncate(cid, hoid, 0);
      t.write(cid, hoid, 0x1000-1, bl.length(), bl);
      cerr << "Write unaligned csum, stage 1" << std::endl;
      r = queue_transaction(store, ch, std::move(t));
      ASSERT_EQ(r, 0);
    }

    bufferlist in, exp1, exp2, exp3;
    exp1.append(s1);
    exp2.append(s2);
    exp3.append(s3);
    r = store->read(ch, hoid, 0x1000-1, 1, in);
    ASSERT_EQ(1, r);
    ASSERT_TRUE(bl_eq(exp1, in));
    in.clear();
    r = store->read(ch, hoid, 0x1000, 0x1000, in);
    ASSERT_EQ(0x1000, r);
    ASSERT_TRUE(bl_eq(exp2, in));

    {
      ObjectStore::Transaction t;
      bufferlist bl;
      bl.append(s3);
      t.write(cid, hoid, 1, bl.length(), bl);
      cerr << "Write unaligned csum, stage 2" << std::endl;
      r = queue_transaction(store, ch, std::move(t));
      ASSERT_EQ(r, 0);
    }
    in.clear();
    r = store->read(ch, hoid, 1, 2, in);
    ASSERT_EQ(2, r);
    ASSERT_TRUE(bl_eq(exp3, in));
    in.clear();
    r = store->read(ch, hoid, 0x1000-1, 1, in);
    ASSERT_EQ(1, r);
    ASSERT_TRUE(bl_eq(exp1, in));
    in.clear();
    r = store->read(ch, hoid, 0x1000, 0x1000, in);
    ASSERT_EQ(0x1000, r);
    ASSERT_TRUE(bl_eq(exp2, in));

  }

  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

#if defined(WITH_BLUESTORE)

TEST_P(StoreTestSpecificAUSize, BluestoreStatFSTest) {
  if(string(GetParam()) != "bluestore")
    return;
  StartDeferred(65536);
  SetVal(g_conf(), "bluestore_compression_mode", "force");
  // just a big number to disble gc
  SetVal(g_conf(), "bluestore_gc_enable_total_threshold", "100000");
  SetVal(g_conf(), "bluestore_fsck_on_umount", "true");
  g_conf().apply_changes(nullptr);
  int r;

  int poolid = 4373;
  coll_t cid = coll_t(spg_t(pg_t(0, poolid), shard_id_t::NO_SHARD));
  ghobject_t hoid(hobject_t(sobject_t("Object 1", CEPH_NOSNAP),
                            string(),
			    0,
			    poolid,
			    string()));
  ghobject_t hoid2 = hoid;
  hoid2.hobj.snap = 1;
  {
    auto ch = store->open_collection(cid);
    ASSERT_FALSE(ch);
  }
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    bool exists = store->exists(ch, hoid);
    ASSERT_TRUE(!exists);

    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    cerr << "Creating object " << hoid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    exists = store->exists(ch, hoid);
    ASSERT_EQ(true, exists);
  }
  {
    struct store_statfs_t statfs;
    int r = store->statfs(&statfs);
    ASSERT_EQ(r, 0);
    ASSERT_EQ( 0u, statfs.allocated);
    ASSERT_EQ( 0u, statfs.data_stored);
    ASSERT_EQ(g_conf()->bluestore_block_size, statfs.total);
    ASSERT_TRUE(statfs.available > 0u && statfs.available < g_conf()->bluestore_block_size);

    struct store_statfs_t statfs_pool;
    r = store->pool_statfs(poolid, &statfs_pool);
    ASSERT_EQ(r, 0);
    ASSERT_EQ( 0u, statfs_pool.allocated);
    ASSERT_EQ( 0u, statfs_pool.data_stored);

    //force fsck
    ch.reset();
    EXPECT_EQ(store->umount(), 0);
    ASSERT_EQ(store->fsck(false), 0); // do fsck explicitly
    EXPECT_EQ(store->mount(), 0);
    ch = store->open_collection(cid);
  }
  {
    ObjectStore::Transaction t;
    bufferlist bl;
    bl.append("abcde");
    t.write(cid, hoid, 0, 5, bl);
    cerr << "Append 5 bytes" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    struct store_statfs_t statfs;
    int r = store->statfs(&statfs);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(5, statfs.data_stored);
    ASSERT_EQ(0x10000, statfs.allocated);
    ASSERT_EQ(0, statfs.data_compressed);
    ASSERT_EQ(0, statfs.data_compressed_original);
    ASSERT_EQ(0, statfs.data_compressed_allocated);

    struct store_statfs_t statfs_pool;
    r = store->pool_statfs(poolid, &statfs_pool);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(5, statfs_pool.data_stored);
    ASSERT_EQ(0x10000, statfs_pool.allocated);
    ASSERT_EQ(0, statfs_pool.data_compressed);
    ASSERT_EQ(0, statfs_pool.data_compressed_original);
    ASSERT_EQ(0, statfs_pool.data_compressed_allocated);

    // accessing unknown pool
    r = store->pool_statfs(poolid + 1, &statfs_pool);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(0, statfs_pool.data_stored);
    ASSERT_EQ(0, statfs_pool.allocated);
    ASSERT_EQ(0, statfs_pool.data_compressed);
    ASSERT_EQ(0, statfs_pool.data_compressed_original);
    ASSERT_EQ(0, statfs_pool.data_compressed_allocated);

    //force fsck
    ch.reset();
    EXPECT_EQ(store->umount(), 0);
    ASSERT_EQ(store->fsck(false), 0); // do fsck explicitly
    EXPECT_EQ(store->mount(), 0);
    ch = store->open_collection(cid);
  }
  {
    ObjectStore::Transaction t;
    std::string s(0x30000, 'a');
    bufferlist bl;
    bl.append(s);
    t.write(cid, hoid, 0x10000, bl.length(), bl);
    cerr << "Append 0x30000 compressible bytes" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    struct store_statfs_t statfs;
    int r = store->statfs(&statfs);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(0x30005, statfs.data_stored);
    ASSERT_EQ(0x30000, statfs.allocated);
    ASSERT_LE(statfs.data_compressed, 0x10000);
    ASSERT_EQ(0x20000, statfs.data_compressed_original);
    ASSERT_EQ(statfs.data_compressed_allocated, 0x10000);

    struct store_statfs_t statfs_pool;
    r = store->pool_statfs(poolid, &statfs_pool);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(0x30005, statfs_pool.data_stored);
    ASSERT_EQ(0x30000, statfs_pool.allocated);
    ASSERT_LE(statfs_pool.data_compressed, 0x10000);
    ASSERT_EQ(0x20000, statfs_pool.data_compressed_original);
    ASSERT_EQ(statfs_pool.data_compressed_allocated, 0x10000);
    //force fsck
    ch.reset();
    EXPECT_EQ(store->umount(), 0);
    ASSERT_EQ(store->fsck(false), 0); // do fsck explicitly
    EXPECT_EQ(store->mount(), 0);
    ch = store->open_collection(cid);
  }
  {
    ObjectStore::Transaction t;
    t.zero(cid, hoid, 1, 3);
    t.zero(cid, hoid, 0x20000, 9);
    cerr << "Punch hole at 1~3, 0x20000~9" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    struct store_statfs_t statfs;
    int r = store->statfs(&statfs);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(0x30005 - 3 - 9, statfs.data_stored);
    ASSERT_EQ(0x30000, statfs.allocated);
    ASSERT_LE(statfs.data_compressed, 0x10000);
    ASSERT_EQ(0x20000 - 9, statfs.data_compressed_original);
    ASSERT_EQ(statfs.data_compressed_allocated, 0x10000);

    struct store_statfs_t statfs_pool;
    r = store->pool_statfs(poolid, &statfs_pool);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(0x30005 - 3 - 9, statfs_pool.data_stored);
    ASSERT_EQ(0x30000, statfs_pool.allocated);
    ASSERT_LE(statfs_pool.data_compressed, 0x10000);
    ASSERT_EQ(0x20000 - 9, statfs_pool.data_compressed_original);
    ASSERT_EQ(statfs_pool.data_compressed_allocated, 0x10000);
    //force fsck
    ch.reset();
    EXPECT_EQ(store->umount(), 0);
    ASSERT_EQ(store->fsck(false), 0); // do fsck explicitly
    EXPECT_EQ(store->mount(), 0);
    ch = store->open_collection(cid);
  }
  {
    ObjectStore::Transaction t;
    std::string s(0x1000, 'b');
    bufferlist bl;
    bl.append(s);
    t.write(cid, hoid, 1, bl.length(), bl);
    t.write(cid, hoid, 0x10001, bl.length(), bl);
    cerr << "Overwrite first and second(compressible) extents" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    struct store_statfs_t statfs;
    int r = store->statfs(&statfs);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(0x30001 - 9 + 0x1000, statfs.data_stored);
    ASSERT_EQ(0x40000, statfs.allocated);
    ASSERT_LE(statfs.data_compressed, 0x10000);
    ASSERT_EQ(0x20000 - 9 - 0x1000, statfs.data_compressed_original);
    ASSERT_EQ(statfs.data_compressed_allocated, 0x10000);

    struct store_statfs_t statfs_pool;
    r = store->pool_statfs(poolid, &statfs_pool);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(0x30001 - 9 + 0x1000, statfs_pool.data_stored);
    ASSERT_EQ(0x40000, statfs_pool.allocated);
    ASSERT_LE(statfs_pool.data_compressed, 0x10000);
    ASSERT_EQ(0x20000 - 9 - 0x1000, statfs_pool.data_compressed_original);
    ASSERT_EQ(statfs_pool.data_compressed_allocated, 0x10000);
    //force fsck
    ch.reset();
    EXPECT_EQ(store->umount(), 0);
    ASSERT_EQ(store->fsck(false), 0); // do fsck explicitly
    EXPECT_EQ(store->mount(), 0);
    ch = store->open_collection(cid);
  }
  {
    ObjectStore::Transaction t;
    std::string s(0x10000, 'c');
    bufferlist bl;
    bl.append(s);
    t.write(cid, hoid, 0x10000, bl.length(), bl);
    t.write(cid, hoid, 0x20000, bl.length(), bl);
    t.write(cid, hoid, 0x30000, bl.length(), bl);
    cerr << "Overwrite compressed extent with 3 uncompressible ones" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    struct store_statfs_t statfs;
    int r = store->statfs(&statfs);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(0x30000 + 0x1001, statfs.data_stored);
    ASSERT_EQ(0x40000, statfs.allocated);
    ASSERT_LE(statfs.data_compressed, 0);
    ASSERT_EQ(0, statfs.data_compressed_original);
    ASSERT_EQ(0, statfs.data_compressed_allocated);

    struct store_statfs_t statfs_pool;
    r = store->pool_statfs(poolid, &statfs_pool);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(0x30000 + 0x1001, statfs_pool.data_stored);
    ASSERT_EQ(0x40000, statfs_pool.allocated);
    ASSERT_LE(statfs_pool.data_compressed, 0);
    ASSERT_EQ(0, statfs_pool.data_compressed_original);
    ASSERT_EQ(0, statfs_pool.data_compressed_allocated);
    //force fsck
    ch.reset();
    EXPECT_EQ(store->umount(), 0);
    ASSERT_EQ(store->fsck(false), 0); // do fsck explicitly
    EXPECT_EQ(store->mount(), 0);
    ch = store->open_collection(cid);
  }
  {
    ObjectStore::Transaction t;
    t.zero(cid, hoid, 0, 0x40000);
    cerr << "Zero object" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
    struct store_statfs_t statfs;
    int r = store->statfs(&statfs);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(0u, statfs.allocated);
    ASSERT_EQ(0u, statfs.data_stored);
    ASSERT_EQ(0u, statfs.data_compressed_original);
    ASSERT_EQ(0u, statfs.data_compressed);
    ASSERT_EQ(0u, statfs.data_compressed_allocated);

    struct store_statfs_t statfs_pool;
    r = store->pool_statfs(poolid, &statfs_pool);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(0u, statfs_pool.allocated);
    ASSERT_EQ(0u, statfs_pool.data_stored);
    ASSERT_EQ(0u, statfs_pool.data_compressed_original);
    ASSERT_EQ(0u, statfs_pool.data_compressed);
    ASSERT_EQ(0u, statfs_pool.data_compressed_allocated);
    //force fsck
    ch.reset();
    EXPECT_EQ(store->umount(), 0);
    ASSERT_EQ(store->fsck(false), 0); // do fsck explicitly
    EXPECT_EQ(store->mount(), 0);
    ch = store->open_collection(cid);
  }
  {
    ObjectStore::Transaction t;
    std::string s(0x10000, 'c');
    bufferlist bl;
    bl.append(s);
    bl.append(s);
    bl.append(s);
    bl.append(s.substr(0, 0x10000-2));
    t.write(cid, hoid, 0, bl.length(), bl);
    cerr << "Yet another compressible write" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
    struct store_statfs_t statfs;
    r = store->statfs(&statfs);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(0x40000 - 2, statfs.data_stored);
    ASSERT_EQ(0x30000, statfs.allocated);
    ASSERT_LE(statfs.data_compressed, 0x10000);
    ASSERT_EQ(0x20000, statfs.data_compressed_original);
    ASSERT_EQ(0x10000, statfs.data_compressed_allocated);

    struct store_statfs_t statfs_pool;
    r = store->pool_statfs(poolid, &statfs_pool);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(0x40000 - 2, statfs_pool.data_stored);
    ASSERT_EQ(0x30000, statfs_pool.allocated);
    ASSERT_LE(statfs_pool.data_compressed, 0x10000);
    ASSERT_EQ(0x20000, statfs_pool.data_compressed_original);
    ASSERT_EQ(0x10000, statfs_pool.data_compressed_allocated);
    //force fsck
    ch.reset();
    EXPECT_EQ(store->umount(), 0);
    ASSERT_EQ(store->fsck(false), 0); // do fsck explicitly
    EXPECT_EQ(store->mount(), 0);
    ch = store->open_collection(cid);
  }
  {
    struct store_statfs_t statfs;
    r = store->statfs(&statfs);
    ASSERT_EQ(r, 0);

    struct store_statfs_t statfs_pool;
    r = store->pool_statfs(poolid, &statfs_pool);
    ASSERT_EQ(r, 0);

    ObjectStore::Transaction t;
    t.clone(cid, hoid, hoid2);
    cerr << "Clone compressed objecte" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
    struct store_statfs_t statfs2;
    r = store->statfs(&statfs2);
    ASSERT_EQ(r, 0);
    ASSERT_GT(statfs2.data_stored, statfs.data_stored);
    ASSERT_EQ(statfs2.allocated, statfs.allocated);
    ASSERT_GT(statfs2.data_compressed, statfs.data_compressed);
    ASSERT_GT(statfs2.data_compressed_original, statfs.data_compressed_original);
    ASSERT_EQ(statfs2.data_compressed_allocated, statfs.data_compressed_allocated);

    struct store_statfs_t statfs2_pool;
    r = store->pool_statfs(poolid, &statfs2_pool);
    ASSERT_EQ(r, 0);
    ASSERT_GT(statfs2_pool.data_stored, statfs_pool.data_stored);
    ASSERT_EQ(statfs2_pool.allocated, statfs_pool.allocated);
    ASSERT_GT(statfs2_pool.data_compressed, statfs_pool.data_compressed);
    ASSERT_GT(statfs2_pool.data_compressed_original,
      statfs_pool.data_compressed_original);
    ASSERT_EQ(statfs2_pool.data_compressed_allocated,
      statfs_pool.data_compressed_allocated);
  }

  {
    // verify no
    auto poolid2 = poolid + 1;
    coll_t cid2 = coll_t(spg_t(pg_t(20, poolid2), shard_id_t::NO_SHARD));
    ghobject_t hoid(hobject_t(sobject_t("Object 2", CEPH_NOSNAP),
                              string(),
			      0,
			      poolid2,
			      string()));
    auto ch = store->create_new_collection(cid2);

    {

      struct store_statfs_t statfs1_pool;
      int r = store->pool_statfs(poolid, &statfs1_pool);
      ASSERT_EQ(r, 0);

      cerr << "Creating second collection " << cid2 << std::endl;
      ObjectStore::Transaction t;
      t.create_collection(cid2, 0);
      r = queue_transaction(store, ch, std::move(t));
      ASSERT_EQ(r, 0);

      t = ObjectStore::Transaction();
      bufferlist bl;
      bl.append("abcde");
      t.write(cid2, hoid, 0, 5, bl);
      r = queue_transaction(store, ch, std::move(t));
      ASSERT_EQ(r, 0);

      struct store_statfs_t statfs2_pool;
      r = store->pool_statfs(poolid2, &statfs2_pool);
      ASSERT_EQ(r, 0);
      ASSERT_EQ(5, statfs2_pool.data_stored);
      ASSERT_EQ(0x10000, statfs2_pool.allocated);
      ASSERT_EQ(0, statfs2_pool.data_compressed);
      ASSERT_EQ(0, statfs2_pool.data_compressed_original);
      ASSERT_EQ(0, statfs2_pool.data_compressed_allocated);

      struct store_statfs_t statfs1_pool_again;
      r = store->pool_statfs(poolid, &statfs1_pool_again);
      ASSERT_EQ(r, 0);
      // adjust 'available' since it has changed
      statfs1_pool_again.available = statfs1_pool.available;
      ASSERT_EQ(statfs1_pool_again, statfs1_pool);

      t = ObjectStore::Transaction();
      t.remove(cid2, hoid);
      t.remove_collection(cid2);
      cerr << "Cleaning" << std::endl;
      r = queue_transaction(store, ch, std::move(t));
      ASSERT_EQ(r, 0);
    }
  }

  {
    // verify ops on temporary object

    auto poolid3 = poolid + 2;
    coll_t cid3 = coll_t(spg_t(pg_t(20, poolid3), shard_id_t::NO_SHARD));
    ghobject_t hoid3(hobject_t(sobject_t("Object 3", CEPH_NOSNAP),
			       string(),
			       0,
			       poolid3,
			       string()));
    ghobject_t hoid3_temp;
    hoid3_temp.hobj = hoid3.hobj.make_temp_hobject("Object 3 temp");
    auto ch3 = store->create_new_collection(cid3);
    {
      struct store_statfs_t statfs1_pool;
      int r = store->pool_statfs(poolid, &statfs1_pool);
      ASSERT_EQ(r, 0);

      cerr << "Creating third collection " << cid3 << std::endl;
      ObjectStore::Transaction t;
      t.create_collection(cid3, 0);
      r = queue_transaction(store, ch3, std::move(t));
      ASSERT_EQ(r, 0);

      t = ObjectStore::Transaction();
      bufferlist bl;
      bl.append("abcde");
      t.write(cid3, hoid3_temp, 0, 5, bl);
      r = queue_transaction(store, ch3, std::move(t));
      ASSERT_EQ(r, 0);

      struct store_statfs_t statfs3_pool;
      r = store->pool_statfs(poolid3, &statfs3_pool);
      ASSERT_EQ(r, 0);
      ASSERT_EQ(5, statfs3_pool.data_stored);
      ASSERT_EQ(0x10000, statfs3_pool.allocated);
      ASSERT_EQ(0, statfs3_pool.data_compressed);
      ASSERT_EQ(0, statfs3_pool.data_compressed_original);
      ASSERT_EQ(0, statfs3_pool.data_compressed_allocated);

      struct store_statfs_t statfs1_pool_again;
      r = store->pool_statfs(poolid, &statfs1_pool_again);
      ASSERT_EQ(r, 0);
      // adjust 'available' since it has changed
      statfs1_pool_again.available = statfs1_pool.available;
      ASSERT_EQ(statfs1_pool_again, statfs1_pool);

      //force fsck
      ch.reset();
      ch3.reset();
      EXPECT_EQ(store->umount(), 0);
      EXPECT_EQ(store->mount(), 0);
      ch = store->open_collection(cid);
      ch3 = store->open_collection(cid3);

      t = ObjectStore::Transaction();
      t.collection_move_rename(
	cid3, hoid3_temp,
	cid3, hoid3);
      r = queue_transaction(store, ch3, std::move(t));
      ASSERT_EQ(r, 0);

      struct store_statfs_t statfs3_pool_again;
      r = store->pool_statfs(poolid3, &statfs3_pool_again);
      ASSERT_EQ(r, 0);
      ASSERT_EQ(statfs3_pool_again, statfs3_pool);

      //force fsck
      ch.reset();
      ch3.reset();
      EXPECT_EQ(store->umount(), 0);
      EXPECT_EQ(store->mount(), 0);
      ch = store->open_collection(cid);
      ch3 = store->open_collection(cid3);

      t = ObjectStore::Transaction();
      t.remove(cid3, hoid3);
      t.remove_collection(cid3);
      cerr << "Cleaning" << std::endl;
      r = queue_transaction(store, ch3, std::move(t));
      ASSERT_EQ(r, 0);
    }
  }

  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove(cid, hoid2);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    struct store_statfs_t statfs;
    r = store->statfs(&statfs);
    ASSERT_EQ(r, 0);
    ASSERT_EQ( 0u, statfs.allocated);
    ASSERT_EQ( 0u, statfs.data_stored);
    ASSERT_EQ( 0u, statfs.data_compressed_original);
    ASSERT_EQ( 0u, statfs.data_compressed);
    ASSERT_EQ( 0u, statfs.data_compressed_allocated);

    struct store_statfs_t statfs_pool;
    r = store->pool_statfs(poolid, &statfs_pool);
    ASSERT_EQ(r, 0);
    ASSERT_EQ( 0u, statfs_pool.allocated);
    ASSERT_EQ( 0u, statfs_pool.data_stored);
    ASSERT_EQ( 0u, statfs_pool.data_compressed_original);
    ASSERT_EQ( 0u, statfs_pool.data_compressed);
    ASSERT_EQ( 0u, statfs_pool.data_compressed_allocated);
  }
}

TEST_P(StoreTestSpecificAUSize, BluestoreFragmentedBlobTest) {
  if(string(GetParam()) != "bluestore")
    return;
  StartDeferred(0x10000);

  int r;
  coll_t cid;
  ghobject_t hoid(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    bool exists = store->exists(ch, hoid);
    ASSERT_TRUE(!exists);

    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    cerr << "Creating object " << hoid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    exists = store->exists(ch, hoid);
    ASSERT_EQ(true, exists);
  }
  {
    struct store_statfs_t statfs;
    int r = store->statfs(&statfs);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(g_conf()->bluestore_block_size, statfs.total);
    ASSERT_EQ(0u, statfs.allocated);
    ASSERT_EQ(0u, statfs.data_stored);
    ASSERT_TRUE(statfs.available > 0u && statfs.available < g_conf()->bluestore_block_size);
  }
  std::string data;
  data.resize(0x10000 * 3);
  {
    ObjectStore::Transaction t;
    for(size_t i = 0;i < data.size(); i++)
      data[i] = i / 256 + 1;
    bufferlist bl, newdata;
    bl.append(data);
    t.write(cid, hoid, 0, bl.length(), bl);
    t.zero(cid, hoid, 0x10000, 0x10000);
    cerr << "Append 3*0x10000 bytes and punch a hole 0x10000~10000" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    struct store_statfs_t statfs;
    int r = store->statfs(&statfs);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(0x20000, statfs.data_stored);
    ASSERT_EQ(0x20000, statfs.allocated);

    r = store->read(ch, hoid, 0, data.size(), newdata);
    ASSERT_EQ(r, (int)data.size());
    {
      bufferlist expected;
      expected.append(data.substr(0, 0x10000));
      expected.append(string(0x10000, 0));
      expected.append(data.substr(0x20000, 0x10000));
      ASSERT_TRUE(bl_eq(expected, newdata));
    }
    newdata.clear();

    r = store->read(ch, hoid, 1, data.size()-2, newdata);
    ASSERT_EQ(r, (int)data.size()-2);
    {
      bufferlist expected;
      expected.append(data.substr(1, 0x10000-1));
      expected.append(string(0x10000, 0));
      expected.append(data.substr(0x20000, 0x10000 - 1));
      ASSERT_TRUE(bl_eq(expected, newdata));
    }
    newdata.clear();
  }
  //force fsck
  ch.reset();
  EXPECT_EQ(store->umount(), 0);
  ASSERT_EQ(store->fsck(false), 0); // do fsck explicitly
  EXPECT_EQ(store->mount(), 0);
  ch = store->open_collection(cid);

  {
    ObjectStore::Transaction t;
    std::string data2(3, 'b');
    bufferlist bl, newdata;
    bl.append(data2);
    t.write(cid, hoid, 0x20000, bl.length(), bl);
    cerr << "Write 3 bytes after the hole" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    struct store_statfs_t statfs;
    int r = store->statfs(&statfs);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(0x20000, statfs.allocated);
    ASSERT_EQ(0x20000, statfs.data_stored);

    r = store->read(ch, hoid, 0x20000-1, 21, newdata);
    ASSERT_EQ(r, (int)21);
    {
      bufferlist expected;
      expected.append(string(0x1, 0));
      expected.append(string(data2));
      expected.append(data.substr(0x20003, 21-4));
      ASSERT_TRUE(bl_eq(expected, newdata));
    }
    newdata.clear();
  }
  //force fsck
  ch.reset();
  EXPECT_EQ(store->umount(), 0);
  ASSERT_EQ(store->fsck(false), 0); // do fsck explicitly
  EXPECT_EQ(store->mount(), 0);
  ch = store->open_collection(cid);

  {
    ObjectStore::Transaction t;
    std::string data2(3, 'a');
    bufferlist bl, newdata;
    bl.append(data2);
    t.write(cid, hoid, 0x10000+1, bl.length(), bl);
    cerr << "Write 3 bytes to the hole" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    struct store_statfs_t statfs;
    int r = store->statfs(&statfs);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(0x30000, statfs.allocated);
    ASSERT_EQ(0x20003, statfs.data_stored);

    r = store->read(ch, hoid, 0x10000-1, 0x10000+22, newdata);
    ASSERT_EQ(r, (int)0x10000+22);
    {
      bufferlist expected;
      expected.append(data.substr(0x10000-1, 1));
      expected.append(string(0x1, 0));
      expected.append(data2);
      expected.append(string(0x10000-4, 0));
      expected.append(string(0x3, 'b'));
      expected.append(data.substr(0x20004, 21-3));
      ASSERT_TRUE(bl_eq(expected, newdata));
    }
    newdata.clear();
  }
  {
    ObjectStore::Transaction t;
    bufferlist bl, newdata;
    bl.append(string(0x30000, 'c'));
    t.write(cid, hoid, 0, 0x30000, bl);
    t.zero(cid, hoid, 0, 0x10000);
    t.zero(cid, hoid, 0x20000, 0x10000);
    cerr << "Rewrite an object and create two holes at the beginning and the end" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    struct store_statfs_t statfs;
    int r = store->statfs(&statfs);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(0x10000, statfs.allocated);
    ASSERT_EQ(0x10000, statfs.data_stored);

    r = store->read(ch, hoid, 0, 0x30000, newdata);
    ASSERT_EQ(r, (int)0x30000);
    {
      bufferlist expected;
      expected.append(string(0x10000, 0));
      expected.append(string(0x10000, 'c'));
      expected.append(string(0x10000, 0));
      ASSERT_TRUE(bl_eq(expected, newdata));
    }
    newdata.clear();
  }

  //force fsck
  ch.reset();
  EXPECT_EQ(store->umount(), 0);
  ASSERT_EQ(store->fsck(false), 0); // do fsck explicitly
  EXPECT_EQ(store->mount(), 0);
  ch = store->open_collection(cid);

  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    struct store_statfs_t statfs;
    r = store->statfs(&statfs);
    ASSERT_EQ(r, 0);
    ASSERT_EQ( 0u, statfs.allocated);
    ASSERT_EQ( 0u, statfs.data_stored);
    ASSERT_EQ( 0u, statfs.data_compressed_original);
    ASSERT_EQ( 0u, statfs.data_compressed);
    ASSERT_EQ( 0u, statfs.data_compressed_allocated);
  }
}
#endif

TEST_P(StoreTest, ManySmallWrite) {
  int r;
  coll_t cid;
  ghobject_t a(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  ghobject_t b(hobject_t(sobject_t("Object 2", CEPH_NOSNAP)));
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  bufferlist bl;
  bufferptr bp(4096);
  bp.zero();
  bl.append(bp);
  for (int i=0; i<100; ++i) {
    ObjectStore::Transaction t;
    t.write(cid, a, i*4096, 4096, bl, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  for (int i=0; i<100; ++i) {
    ObjectStore::Transaction t;
    t.write(cid, b, (rand() % 1024)*4096, 4096, bl, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, a);
    t.remove(cid, b);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, MultiSmallWriteSameBlock) {
  int r;
  coll_t cid;
  ghobject_t a(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  bufferlist bl;
  bl.append("short");
  C_SaferCond c, d;
  // touch same block in both same transaction, tls, and pipelined txns
  {
    ObjectStore::Transaction t, u;
    t.write(cid, a, 0, 5, bl, 0);
    t.write(cid, a, 5, 5, bl, 0);
    t.write(cid, a, 4094, 5, bl, 0);
    t.write(cid, a, 9000, 5, bl, 0);
    u.write(cid, a, 10, 5, bl, 0);
    u.write(cid, a, 7000, 5, bl, 0);
    t.register_on_commit(&c);
    vector<ObjectStore::Transaction> v = {t, u};
    store->queue_transactions(ch, v);
  }
  {
    ObjectStore::Transaction t, u;
    t.write(cid, a, 40, 5, bl, 0);
    t.write(cid, a, 45, 5, bl, 0);
    t.write(cid, a, 4094, 5, bl, 0);
    t.write(cid, a, 6000, 5, bl, 0);
    u.write(cid, a, 610, 5, bl, 0);
    u.write(cid, a, 11000, 5, bl, 0);
    t.register_on_commit(&d);
    vector<ObjectStore::Transaction> v = {t, u};
    store->queue_transactions(ch, v);
  }
  c.wait();
  d.wait();
  {
    bufferlist bl2;
    r = store->read(ch, a, 0, 16000, bl2);
    ASSERT_GE(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, a);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, SmallSkipFront) {
  int r;
  coll_t cid;
  ghobject_t a(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.touch(cid, a);
    t.truncate(cid, a, 3000);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    bufferlist bl;
    bufferptr bp(4096);
    memset(bp.c_str(), 1, 4096);
    bl.append(bp);
    ObjectStore::Transaction t;
    t.write(cid, a, 4096, 4096, bl);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    bufferlist bl;
    ASSERT_EQ(8192, store->read(ch, a, 0, 8192, bl));
    for (unsigned i=0; i<4096; ++i)
      ASSERT_EQ(0, bl[i]);
    for (unsigned i=4096; i<8192; ++i)
      ASSERT_EQ(1, bl[i]);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, a);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, AppendDeferredVsTailCache) {
  int r;
  coll_t cid;
  ghobject_t a(hobject_t(sobject_t("fooo", CEPH_NOSNAP)));
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = store->queue_transaction(ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  unsigned min_alloc = g_conf()->bluestore_min_alloc_size;
  unsigned size = min_alloc / 3;
  bufferptr bpa(size);
  memset(bpa.c_str(), 1, bpa.length());
  bufferlist bla;
  bla.append(bpa);
  {
    ObjectStore::Transaction t;
    t.write(cid, a, 0, bla.length(), bla, 0);
    r = store->queue_transaction(ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  // force cached tail to clear ...
  {
    ch.reset();
    int r = store->umount();
    ASSERT_EQ(0, r);
    r = store->mount();
    ASSERT_EQ(0, r);
    ch = store->open_collection(cid);
  }

  bufferptr bpb(size);
  memset(bpb.c_str(), 2, bpb.length());
  bufferlist blb;
  blb.append(bpb);
  {
    ObjectStore::Transaction t;
    t.write(cid, a, bla.length(), blb.length(), blb, 0);
    r = store->queue_transaction(ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  bufferptr bpc(size);
  memset(bpc.c_str(), 3, bpc.length());
  bufferlist blc;
  blc.append(bpc);
  {
    ObjectStore::Transaction t;
    t.write(cid, a, bla.length() + blb.length(), blc.length(), blc, 0);
    r = store->queue_transaction(ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  bufferlist final;
  final.append(bla);
  final.append(blb);
  final.append(blc);
  bufferlist actual;
  {
    ASSERT_EQ((int)final.length(),
	      store->read(ch, a, 0, final.length(), actual));
    ASSERT_TRUE(bl_eq(final, actual));
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, a);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = store->queue_transaction(ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, AppendZeroTrailingSharedBlock) {
  int r;
  coll_t cid;
  ghobject_t a(hobject_t(sobject_t("fooo", CEPH_NOSNAP)));
  ghobject_t b = a;
  b.hobj.snap = 1;
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = store->queue_transaction(ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  unsigned min_alloc = g_conf()->bluestore_min_alloc_size;
  unsigned size = min_alloc / 3;
  bufferptr bpa(size);
  memset(bpa.c_str(), 1, bpa.length());
  bufferlist bla;
  bla.append(bpa);
  // make sure there is some trailing gunk in the last block
  {
    bufferlist bt;
    bt.append(bla);
    bt.append("BADBADBADBAD");
    ObjectStore::Transaction t;
    t.write(cid, a, 0, bt.length(), bt, 0);
    r = store->queue_transaction(ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.truncate(cid, a, size);
    r = store->queue_transaction(ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  // clone
  {
    ObjectStore::Transaction t;
    t.clone(cid, a, b);
    r = store->queue_transaction(ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  // append with implicit zeroing
  bufferptr bpb(size);
  memset(bpb.c_str(), 2, bpb.length());
  bufferlist blb;
  blb.append(bpb);
  {
    ObjectStore::Transaction t;
    t.write(cid, a, min_alloc * 3, blb.length(), blb, 0);
    r = store->queue_transaction(ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  bufferlist final;
  final.append(bla);
  bufferlist zeros;
  zeros.append_zero(min_alloc * 3 - size);
  final.append(zeros);
  final.append(blb);
  bufferlist actual;
  {
    ASSERT_EQ((int)final.length(),
	      store->read(ch, a, 0, final.length(), actual));
    final.hexdump(cout);
    actual.hexdump(cout);
    ASSERT_TRUE(bl_eq(final, actual));
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, a);
    t.remove(cid, b);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = store->queue_transaction(ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, SmallSequentialUnaligned) {
  int r;
  coll_t cid;
  ghobject_t a(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  bufferlist bl;
  int len = 1000;
  bufferptr bp(len);
  bp.zero();
  bl.append(bp);
  for (int i=0; i<1000; ++i) {
    ObjectStore::Transaction t;
    t.write(cid, a, i*len, len, bl, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, a);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, ManyBigWrite) {
  int r;
  coll_t cid;
  ghobject_t a(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  ghobject_t b(hobject_t(sobject_t("Object 2", CEPH_NOSNAP)));
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  bufferlist bl;
  bufferptr bp(4 * 1048576);
  bp.zero();
  bl.append(bp);
  for (int i=0; i<10; ++i) {
    ObjectStore::Transaction t;
    t.write(cid, a, i*4*1048586, 4*1048576, bl, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  // aligned
  for (int i=0; i<10; ++i) {
    ObjectStore::Transaction t;
    t.write(cid, b, (rand() % 256)*4*1048576, 4*1048576, bl, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  // unaligned
  for (int i=0; i<10; ++i) {
    ObjectStore::Transaction t;
    t.write(cid, b, (rand() % (256*4096))*1024, 4*1048576, bl, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  // do some zeros
  for (int i=0; i<10; ++i) {
    ObjectStore::Transaction t;
    t.zero(cid, b, (rand() % (256*4096))*1024, 16*1048576);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, a);
    t.remove(cid, b);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, BigWriteBigZero) {
  int r;
  coll_t cid;
  ghobject_t a(hobject_t(sobject_t("foo", CEPH_NOSNAP)));
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  bufferlist bl;
  bufferptr bp(1048576);
  memset(bp.c_str(), 'b', bp.length());
  bl.append(bp);
  bufferlist s;
  bufferptr sp(4096);
  memset(sp.c_str(), 's', sp.length());
  s.append(sp);
  {
    ObjectStore::Transaction t;
    t.write(cid, a, 0, bl.length(), bl);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.zero(cid, a, bl.length() / 4, bl.length() / 2);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.write(cid, a, bl.length() / 2, s.length(), s);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, a);
    t.remove_collection(cid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, MiscFragmentTests) {
  int r;
  coll_t cid;
  ghobject_t a(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  bufferlist bl;
  bufferptr bp(524288);
  bp.zero();
  bl.append(bp);
  {
    ObjectStore::Transaction t;
    t.write(cid, a, 0, 524288, bl, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.write(cid, a, 1048576, 524288, bl, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    bufferlist inbl;
    int r = store->read(ch, a, 524288 + 131072, 1024, inbl);
    ASSERT_EQ(r, 1024);
    ASSERT_EQ(inbl.length(), 1024u);
    ASSERT_TRUE(inbl.is_zero());
  }
  {
    ObjectStore::Transaction t;
    t.write(cid, a, 1048576 - 4096, 524288, bl, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, a);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

}

TEST_P(StoreTest, ZeroVsObjectSize) {
  int r;
  coll_t cid;
  struct stat stat;
  ghobject_t hoid(hobject_t(sobject_t("foo", CEPH_NOSNAP)));
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  bufferlist a;
  a.append("stuff");
  {
    ObjectStore::Transaction t;
    t.write(cid, hoid, 0, 5, a);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ASSERT_EQ(0, store->stat(ch, hoid, &stat));
  ASSERT_EQ(5, stat.st_size);
  {
    ObjectStore::Transaction t;
    t.zero(cid, hoid, 1, 2);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ASSERT_EQ(0, store->stat(ch, hoid, &stat));
  ASSERT_EQ(5, stat.st_size);
  {
    ObjectStore::Transaction t;
    t.zero(cid, hoid, 3, 200);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ASSERT_EQ(0, store->stat(ch, hoid, &stat));
  ASSERT_EQ(203, stat.st_size);
  {
    ObjectStore::Transaction t;
    t.zero(cid, hoid, 100000, 200);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ASSERT_EQ(0, store->stat(ch, hoid, &stat));
  ASSERT_EQ(100200, stat.st_size);
}

TEST_P(StoreTest, ZeroLengthWrite) {
  int r;
  coll_t cid;
  ghobject_t hoid(hobject_t(sobject_t("foo", CEPH_NOSNAP)));
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    t.touch(cid, hoid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    bufferlist empty;
    t.write(cid, hoid, 1048576, 0, empty);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  struct stat stat;
  r = store->stat(ch, hoid, &stat);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0, stat.st_size);

  bufferlist newdata;
  r = store->read(ch, hoid, 0, 1048576, newdata);
  ASSERT_EQ(0, r);
}

TEST_P(StoreTest, ZeroLengthZero) {
  int r;
  coll_t cid;
  ghobject_t hoid(hobject_t(sobject_t("foo", CEPH_NOSNAP)));
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    t.touch(cid, hoid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(0, r);
  }
  {
    ObjectStore::Transaction t;
    t.zero(cid, hoid, 1048576, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(0, r);
  }
  struct stat stat;
  r = store->stat(ch, hoid, &stat);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0, stat.st_size);

  bufferlist newdata;
  r = store->read(ch, hoid, 0, 1048576, newdata);
  ASSERT_EQ(0, r);
}

TEST_P(StoreTest, SimpleAttrTest) {
  int r;
  coll_t cid;
  ghobject_t hoid(hobject_t(sobject_t("attr object 1", CEPH_NOSNAP)));
  bufferlist val, val2;
  val.append("value");
  val.append("value2");
  {
    auto ch = store->open_collection(cid);
    ASSERT_FALSE(ch);
  }
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    bool empty;
    int r = store->collection_empty(ch, &empty);
    ASSERT_EQ(0, r);
    ASSERT_TRUE(empty);
  }
  {
    bufferptr bp;
    r = store->getattr(ch, hoid, "nofoo", bp);
    ASSERT_EQ(-ENOENT, r);
  }
  {
    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    t.setattr(cid, hoid, "foo", val);
    t.setattr(cid, hoid, "bar", val2);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    bool empty;
    int r = store->collection_empty(ch, &empty);
    ASSERT_EQ(0, r);
    ASSERT_TRUE(!empty);
  }
  {
    bufferptr bp;
    r = store->getattr(ch, hoid, "nofoo", bp);
    ASSERT_EQ(-ENODATA, r);

    r = store->getattr(ch, hoid, "foo", bp);
    ASSERT_EQ(0, r);
    bufferlist bl;
    bl.append(bp);
    ASSERT_TRUE(bl_eq(val, bl));

    map<string,bufferptr> bm;
    r = store->getattrs(ch, hoid, bm);
    ASSERT_EQ(0, r);

  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove_collection(cid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, SimpleListTest) {
  int r;
  coll_t cid(spg_t(pg_t(0, 1), shard_id_t(1)));
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  set<ghobject_t> all;
  {
    ObjectStore::Transaction t;
    for (int i=0; i<200; ++i) {
      string name("object_");
      name += stringify(i);
      ghobject_t hoid(hobject_t(sobject_t(name, CEPH_NOSNAP)),
		      ghobject_t::NO_GEN, shard_id_t(1));
      hoid.hobj.pool = 1;
      all.insert(hoid);
      t.touch(cid, hoid);
      cerr << "Creating object " << hoid << std::endl;
    }
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    set<ghobject_t> saw;
    vector<ghobject_t> objects;
    ghobject_t next, current;
    while (!next.is_max()) {
      int r = store->collection_list(ch, current, ghobject_t::get_max(),
				     50,
				     &objects, &next);
      ASSERT_EQ(r, 0);
      ASSERT_TRUE(sorted(objects));
      cout << " got " << objects.size() << " next " << next << std::endl;
      for (vector<ghobject_t>::iterator p = objects.begin(); p != objects.end();
	   ++p) {
	if (saw.count(*p)) {
	  cout << "got DUP " << *p << std::endl;
	} else {
	  //cout << "got new " << *p << std::endl;
	}
	saw.insert(*p);
      }
      objects.clear();
      current = next;
    }
    ASSERT_EQ(saw.size(), all.size());
    ASSERT_EQ(saw, all);
  }
  {
    ObjectStore::Transaction t;
    for (set<ghobject_t>::iterator p = all.begin(); p != all.end(); ++p)
      t.remove(cid, *p);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, ListEndTest) {
  int r;
  coll_t cid(spg_t(pg_t(0, 1), shard_id_t(1)));
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  set<ghobject_t> all;
  {
    ObjectStore::Transaction t;
    for (int i=0; i<200; ++i) {
      string name("object_");
      name += stringify(i);
      ghobject_t hoid(hobject_t(sobject_t(name, CEPH_NOSNAP)),
		      ghobject_t::NO_GEN, shard_id_t(1));
      hoid.hobj.pool = 1;
      all.insert(hoid);
      t.touch(cid, hoid);
      cerr << "Creating object " << hoid << std::endl;
    }
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ghobject_t end(hobject_t(sobject_t("object_100", CEPH_NOSNAP)),
		   ghobject_t::NO_GEN, shard_id_t(1));
    end.hobj.pool = 1;
    vector<ghobject_t> objects;
    ghobject_t next;
    int r = store->collection_list(ch, ghobject_t(), end, 500,
				   &objects, &next);
    ASSERT_EQ(r, 0);
    for (auto &p : objects) {
      ASSERT_NE(p, end);
    }
  }
  {
    ObjectStore::Transaction t;
    for (set<ghobject_t>::iterator p = all.begin(); p != all.end(); ++p)
      t.remove(cid, *p);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, Sort) {
  {
    hobject_t a(sobject_t("a", CEPH_NOSNAP));
    hobject_t b = a;
    ASSERT_EQ(a, b);
    b.oid.name = "b";
    ASSERT_NE(a, b);
    ASSERT_TRUE(a < b);
    a.pool = 1;
    b.pool = 2;
    ASSERT_TRUE(a < b);
    a.pool = 3;
    ASSERT_TRUE(a > b);
  }
  {
    ghobject_t a(hobject_t(sobject_t("a", CEPH_NOSNAP)));
    ghobject_t b(hobject_t(sobject_t("b", CEPH_NOSNAP)));
    a.hobj.pool = 1;
    b.hobj.pool = 1;
    ASSERT_TRUE(a < b);
    a.hobj.pool = -3;
    ASSERT_TRUE(a < b);
    a.hobj.pool = 1;
    b.hobj.pool = -3;
    ASSERT_TRUE(a > b);
  }
}

TEST_P(StoreTest, MultipoolListTest) {
  int r;
  int poolid = 4373;
  coll_t cid = coll_t(spg_t(pg_t(0, poolid), shard_id_t::NO_SHARD));
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  set<ghobject_t> all, saw;
  {
    ObjectStore::Transaction t;
    for (int i=0; i<200; ++i) {
      string name("object_");
      name += stringify(i);
      ghobject_t hoid(hobject_t(sobject_t(name, CEPH_NOSNAP)));
      if (rand() & 1)
	hoid.hobj.pool = -2 - poolid;
      else
	hoid.hobj.pool = poolid;
      all.insert(hoid);
      t.touch(cid, hoid);
      cerr << "Creating object " << hoid << std::endl;
    }
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    vector<ghobject_t> objects;
    ghobject_t next, current;
    while (!next.is_max()) {
      int r = store->collection_list(ch, current, ghobject_t::get_max(), 50,
				     &objects, &next);
      ASSERT_EQ(r, 0);
      cout << " got " << objects.size() << " next " << next << std::endl;
      for (vector<ghobject_t>::iterator p = objects.begin(); p != objects.end();
	   ++p) {
	saw.insert(*p);
      }
      objects.clear();
      current = next;
    }
    ASSERT_EQ(saw, all);
  }
  {
    ObjectStore::Transaction t;
    for (set<ghobject_t>::iterator p = all.begin(); p != all.end(); ++p)
      t.remove(cid, *p);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, SimpleCloneTest) {
  int r;
  coll_t cid;
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ghobject_t hoid(hobject_t(sobject_t("Object 1", CEPH_NOSNAP),
			    "key", 123, -1, ""));
  bufferlist small, large, xlarge, newdata, attr;
  small.append("small");
  large.append("large");
  xlarge.append("xlarge");
  {
    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    t.setattr(cid, hoid, "attr1", small);
    t.setattr(cid, hoid, "attr2", large);
    t.setattr(cid, hoid, "attr3", xlarge);
    t.write(cid, hoid, 0, small.length(), small);
    t.write(cid, hoid, 10, small.length(), small);
    cerr << "Creating object and set attr " << hoid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  ghobject_t hoid2(hobject_t(sobject_t("Object 2", CEPH_NOSNAP),
			     "key", 123, -1, ""));
  ghobject_t hoid3(hobject_t(sobject_t("Object 3", CEPH_NOSNAP)));
  {
    ObjectStore::Transaction t;
    t.clone(cid, hoid, hoid2);
    t.setattr(cid, hoid2, "attr2", small);
    t.rmattr(cid, hoid2, "attr1");
    t.write(cid, hoid, 10, large.length(), large);
    t.setattr(cid, hoid, "attr1", large);
    t.setattr(cid, hoid, "attr2", small);
    cerr << "Clone object and rm attr" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    r = store->read(ch, hoid, 10, 5, newdata);
    ASSERT_EQ(r, 5);
    ASSERT_TRUE(bl_eq(large, newdata));

    newdata.clear();
    r = store->read(ch, hoid, 0, 5, newdata);
    ASSERT_EQ(r, 5);
    ASSERT_TRUE(bl_eq(small, newdata));

    newdata.clear();
    r = store->read(ch, hoid2, 10, 5, newdata);
    ASSERT_EQ(r, 5);
    ASSERT_TRUE(bl_eq(small, newdata));

    r = store->getattr(ch, hoid2, "attr2", attr);
    ASSERT_EQ(r, 0);
    ASSERT_TRUE(bl_eq(small, attr));

    attr.clear();
    r = store->getattr(ch, hoid2, "attr3", attr);
    ASSERT_EQ(r, 0);
    ASSERT_TRUE(bl_eq(xlarge, attr));

    attr.clear();
    r = store->getattr(ch, hoid, "attr1", attr);
    ASSERT_EQ(r, 0);
    ASSERT_TRUE(bl_eq(large, attr));
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove(cid, hoid2);
    ASSERT_EQ(0, queue_transaction(store, ch, std::move(t)));
  }
  {
    bufferlist final;
    bufferptr p(16384);
    memset(p.c_str(), 1, p.length());
    bufferlist pl;
    pl.append(p);
    final.append(p);
    ObjectStore::Transaction t;
    t.write(cid, hoid, 0, pl.length(), pl);
    t.clone(cid, hoid, hoid2);
    bufferptr a(4096);
    memset(a.c_str(), 2, a.length());
    bufferlist al;
    al.append(a);
    final.append(a);
    t.write(cid, hoid, pl.length(), a.length(), al);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
    bufferlist rl;
    ASSERT_EQ((int)final.length(),
	      store->read(ch, hoid, 0, final.length(), rl));
    ASSERT_TRUE(bl_eq(rl, final));
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove(cid, hoid2);
    ASSERT_EQ(0, queue_transaction(store, ch, std::move(t)));
  }
  {
    bufferlist final;
    bufferptr p(16384);
    memset(p.c_str(), 111, p.length());
    bufferlist pl;
    pl.append(p);
    final.append(p);
    ObjectStore::Transaction t;
    t.write(cid, hoid, 0, pl.length(), pl);
    t.clone(cid, hoid, hoid2);
    bufferptr z(4096);
    z.zero();
    final.append(z);
    bufferptr a(4096);
    memset(a.c_str(), 112, a.length());
    bufferlist al;
    al.append(a);
    final.append(a);
    t.write(cid, hoid, pl.length() + z.length(), a.length(), al);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
    bufferlist rl;
    ASSERT_EQ((int)final.length(),
	      store->read(ch, hoid, 0, final.length(), rl));
    ASSERT_TRUE(bl_eq(rl, final));
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove(cid, hoid2);
    ASSERT_EQ(0, queue_transaction(store, ch, std::move(t)));
  }
  {
    bufferlist final;
    bufferptr p(16000);
    memset(p.c_str(), 5, p.length());
    bufferlist pl;
    pl.append(p);
    final.append(p);
    ObjectStore::Transaction t;
    t.write(cid, hoid, 0, pl.length(), pl);
    t.clone(cid, hoid, hoid2);
    bufferptr z(1000);
    z.zero();
    final.append(z);
    bufferptr a(8000);
    memset(a.c_str(), 6, a.length());
    bufferlist al;
    al.append(a);
    final.append(a);
    t.write(cid, hoid, 17000, a.length(), al);
    ASSERT_EQ(0, queue_transaction(store, ch, std::move(t)));
    bufferlist rl;
    ASSERT_EQ((int)final.length(),
	      store->read(ch, hoid, 0, final.length(), rl));
    /*cout << "expected:\n";
    final.hexdump(cout);
    cout << "got:\n";
    rl.hexdump(cout);*/
    ASSERT_TRUE(bl_eq(rl, final));
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove(cid, hoid2);
    ASSERT_EQ(0, queue_transaction(store, ch, std::move(t)));
  }
  {
    bufferptr p(1048576);
    memset(p.c_str(), 3, p.length());
    bufferlist pl;
    pl.append(p);
    ObjectStore::Transaction t;
    t.write(cid, hoid, 0, pl.length(), pl);
    t.clone(cid, hoid, hoid2);
    bufferptr a(65536);
    memset(a.c_str(), 4, a.length());
    bufferlist al;
    al.append(a);
    t.write(cid, hoid, a.length(), a.length(), al);
    ASSERT_EQ(0, queue_transaction(store, ch, std::move(t)));
    bufferlist rl;
    bufferlist final;
    final.substr_of(pl, 0, al.length());
    final.append(al);
    bufferlist end;
    end.substr_of(pl, al.length()*2, pl.length() - al.length()*2);
    final.append(end);
    ASSERT_EQ((int)final.length(),
	      store->read(ch, hoid, 0, final.length(), rl));
    /*cout << "expected:\n";
    final.hexdump(cout);
    cout << "got:\n";
    rl.hexdump(cout);*/
    ASSERT_TRUE(bl_eq(rl, final));
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove(cid, hoid2);
    ASSERT_EQ(0, queue_transaction(store, ch, std::move(t)));
  }
  {
    bufferptr p(65536);
    memset(p.c_str(), 7, p.length());
    bufferlist pl;
    pl.append(p);
    ObjectStore::Transaction t;
    t.write(cid, hoid, 0, pl.length(), pl);
    t.clone(cid, hoid, hoid2);
    bufferptr a(4096);
    memset(a.c_str(), 8, a.length());
    bufferlist al;
    al.append(a);
    t.write(cid, hoid, 32768, a.length(), al);
    ASSERT_EQ(0, queue_transaction(store, ch, std::move(t)));
    bufferlist rl;
    bufferlist final;
    final.substr_of(pl, 0, 32768);
    final.append(al);
    bufferlist end;
    end.substr_of(pl, final.length(), pl.length() - final.length());
    final.append(end);
    ASSERT_EQ((int)final.length(),
	      store->read(ch, hoid, 0, final.length(), rl));
    /*cout << "expected:\n";
    final.hexdump(cout);
    cout << "got:\n";
    rl.hexdump(cout);*/
    ASSERT_TRUE(bl_eq(rl, final));
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove(cid, hoid2);
    ASSERT_EQ(0, queue_transaction(store, ch, std::move(t)));
  }
  {
    bufferptr p(65536);
    memset(p.c_str(), 9, p.length());
    bufferlist pl;
    pl.append(p);
    ObjectStore::Transaction t;
    t.write(cid, hoid, 0, pl.length(), pl);
    t.clone(cid, hoid, hoid2);
    bufferptr a(4096);
    memset(a.c_str(), 10, a.length());
    bufferlist al;
    al.append(a);
    t.write(cid, hoid, 33768, a.length(), al);
    ASSERT_EQ(0, queue_transaction(store, ch, std::move(t)));
    bufferlist rl;
    bufferlist final;
    final.substr_of(pl, 0, 33768);
    final.append(al);
    bufferlist end;
    end.substr_of(pl, final.length(), pl.length() - final.length());
    final.append(end);
    ASSERT_EQ((int)final.length(),
	      store->read(ch, hoid, 0, final.length(), rl));
    /*cout << "expected:\n";
    final.hexdump(cout);
    cout << "got:\n";
    rl.hexdump(cout);*/
    ASSERT_TRUE(bl_eq(rl, final));
  }

  //Unfortunately we need a workaround for filestore since EXPECT_DEATH
  // macro has potential issues when using /in multithread environments. 
  //It works well for all stores but filestore for now. 
  //A fix setting gtest_death_test_style = "threadsafe" doesn't help as well - 
  //  test app clone asserts on store folder presence.
  //
  if (string(GetParam()) != "filestore") { 
    //verify if non-empty collection is properly handled after store reload
    ch.reset();
    r = store->umount();
    ASSERT_EQ(r, 0);
    r = store->mount();
    ASSERT_EQ(r, 0);
    ch = store->open_collection(cid);

    ObjectStore::Transaction t;
    t.remove_collection(cid);
    cerr << "Invalid rm coll" << std::endl;
    PrCtl unset_dumpable;
    EXPECT_DEATH(queue_transaction(store, ch, std::move(t)), "");
  }
  {
    ObjectStore::Transaction t;
    t.touch(cid, hoid3); //new record in db
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  //See comment above for "filestore" check explanation.
  if (string(GetParam()) != "filestore") {
    ObjectStore::Transaction t;
    //verify if non-empty collection is properly handled when there are some pending removes and live records in db
    cerr << "Invalid rm coll again" << std::endl;
    ch.reset();
    r = store->umount();
    ASSERT_EQ(r, 0);
    r = store->mount();
    ASSERT_EQ(r, 0);
    ch = store->open_collection(cid);

    t.remove(cid, hoid);
    t.remove(cid, hoid2);
    t.remove_collection(cid);
    PrCtl unset_dumpable;
    EXPECT_DEATH(queue_transaction(store, ch, std::move(t)), "");
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove(cid, hoid2);
    t.remove(cid, hoid3);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, OmapSimple) {
  int r;
  coll_t cid;
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ghobject_t hoid(hobject_t(sobject_t("omap_obj", CEPH_NOSNAP),
			    "key", 123, -1, ""));
  bufferlist small;
  small.append("small");
  map<string,bufferlist> km;
  km["foo"] = small;
  km["bar"].append("asdfjkasdkjdfsjkafskjsfdj");
  bufferlist header;
  header.append("this is a header");
  {
    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    t.omap_setkeys(cid, hoid, km);
    t.omap_setheader(cid, hoid, header);
    cerr << "Creating object and set omap " << hoid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  // get header, keys
  {
    bufferlist h;
    map<string,bufferlist> r;
    store->omap_get(ch, hoid, &h, &r);
    ASSERT_TRUE(bl_eq(header, h));
    ASSERT_EQ(r.size(), km.size());
    cout << "r: " << r << std::endl;
  }
  // test iterator with seek_to_first
  {
    map<string,bufferlist> r;
    ObjectMap::ObjectMapIterator iter = store->get_omap_iterator(ch, hoid);
    for (iter->seek_to_first(); iter->valid(); iter->next()) {
      r[iter->key()] = iter->value();
    }
    cout << "r: " << r << std::endl;
    ASSERT_EQ(r.size(), km.size());
  }
  // test iterator with initial lower_bound
  {
    map<string,bufferlist> r;
    ObjectMap::ObjectMapIterator iter = store->get_omap_iterator(ch, hoid);
    for (iter->lower_bound(string()); iter->valid(); iter->next()) {
      r[iter->key()] = iter->value();
    }
    cout << "r: " << r << std::endl;
    ASSERT_EQ(r.size(), km.size());
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, OmapCloneTest) {
  int r;
  coll_t cid;
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ghobject_t hoid(hobject_t(sobject_t("Object 1", CEPH_NOSNAP),
			    "key", 123, -1, ""));
  bufferlist small;
  small.append("small");
  map<string,bufferlist> km;
  km["foo"] = small;
  km["bar"].append("asdfjkasdkjdfsjkafskjsfdj");
  bufferlist header;
  header.append("this is a header");
  {
    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    t.omap_setkeys(cid, hoid, km);
    t.omap_setheader(cid, hoid, header);
    cerr << "Creating object and set omap " << hoid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ghobject_t hoid2(hobject_t(sobject_t("Object 2", CEPH_NOSNAP),
			     "key", 123, -1, ""));
  {
    ObjectStore::Transaction t;
    t.clone(cid, hoid, hoid2);
    cerr << "Clone object" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    map<string,bufferlist> r;
    bufferlist h;
    store->omap_get(ch, hoid2, &h, &r);
    ASSERT_TRUE(bl_eq(header, h));
    ASSERT_EQ(r.size(), km.size());
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove(cid, hoid2);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, SimpleCloneRangeTest) {
  int r;
  coll_t cid;
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ghobject_t hoid(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  hoid.hobj.pool = -1;
  bufferlist small, newdata;
  small.append("small");
  {
    ObjectStore::Transaction t;
    t.write(cid, hoid, 10, 5, small);
    cerr << "Creating object and write bl " << hoid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ghobject_t hoid2(hobject_t(sobject_t("Object 2", CEPH_NOSNAP)));
  hoid2.hobj.pool = -1;
  {
    ObjectStore::Transaction t;
    t.clone_range(cid, hoid, hoid2, 10, 5, 10);
    cerr << "Clone range object" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
    r = store->read(ch, hoid2, 10, 5, newdata);
    ASSERT_EQ(r, 5);
    ASSERT_TRUE(bl_eq(small, newdata));
  }
  {
    ObjectStore::Transaction t;
    t.truncate(cid, hoid, 1024*1024);
    t.clone_range(cid, hoid, hoid2, 0, 1024*1024, 0);
    cerr << "Clone range object" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
    struct stat stat, stat2;
    r = store->stat(ch, hoid, &stat);
    r = store->stat(ch, hoid2, &stat2);
    ASSERT_EQ(stat.st_size, stat2.st_size);
    ASSERT_EQ(1024*1024, stat2.st_size);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove(cid, hoid2);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}


TEST_P(StoreTest, SimpleObjectLongnameTest) {
  int r;
  coll_t cid;
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ghobject_t hoid(hobject_t(sobject_t("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaObjectaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 1", CEPH_NOSNAP)));
  {
    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    cerr << "Creating object " << hoid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

ghobject_t generate_long_name(unsigned i)
{
  stringstream name;
  name << "object id " << i << " ";
  for (unsigned j = 0; j < 500; ++j) name << 'a';
  ghobject_t hoid(hobject_t(sobject_t(name.str(), CEPH_NOSNAP)));
  hoid.hobj.set_hash(i % 2);
  return hoid;
}

TEST_P(StoreTest, LongnameSplitTest) {
  int r;
  coll_t cid;
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(0, r);
  }
  for (unsigned i = 0; i < 320; ++i) {
    ObjectStore::Transaction t;
    ghobject_t hoid = generate_long_name(i);
    t.touch(cid, hoid);
    cerr << "Creating object " << hoid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(0, r);
  }

  ghobject_t test_obj = generate_long_name(319);
  ghobject_t test_obj_2 = test_obj;
  test_obj_2.generation = 0;
  {
    ObjectStore::Transaction t;
    // should cause a split
    t.collection_move_rename(
      cid, test_obj,
      cid, test_obj_2);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(0, r);
  }

  for (unsigned i = 0; i < 319; ++i) {
    ObjectStore::Transaction t;
    ghobject_t hoid = generate_long_name(i);
    t.remove(cid, hoid);
    cerr << "Removing object " << hoid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(0, r);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, test_obj_2);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(0, r);
  }

}

TEST_P(StoreTest, ManyObjectTest) {
  int NUM_OBJS = 2000;
  int r = 0;
  coll_t cid;
  string base = "";
  for (int i = 0; i < 100; ++i) base.append("aaaaa");
  set<ghobject_t> created;
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  for (int i = 0; i < NUM_OBJS; ++i) {
    if (!(i % 5)) {
      cerr << "Object " << i << std::endl;
    }
    ObjectStore::Transaction t;
    char buf[100];
    snprintf(buf, sizeof(buf), "%d", i);
    ghobject_t hoid(hobject_t(sobject_t(string(buf) + base, CEPH_NOSNAP)));
    t.touch(cid, hoid);
    created.insert(hoid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  for (set<ghobject_t>::iterator i = created.begin();
       i != created.end();
       ++i) {
    struct stat buf;
    ASSERT_TRUE(!store->stat(ch, *i, &buf));
  }

  set<ghobject_t> listed, listed2;
  vector<ghobject_t> objects;
  r = store->collection_list(ch, ghobject_t(), ghobject_t::get_max(), INT_MAX, &objects, 0);
  ASSERT_EQ(r, 0);

  cerr << "objects.size() is " << objects.size() << std::endl;
  for (vector<ghobject_t> ::iterator i = objects.begin();
       i != objects.end();
       ++i) {
    listed.insert(*i);
    ASSERT_TRUE(created.count(*i));
  }
  ASSERT_TRUE(listed.size() == created.size());

  ghobject_t start, next;
  objects.clear();
  r = store->collection_list(
    ch,
    ghobject_t::get_max(),
    ghobject_t::get_max(),
    50,
    &objects,
    &next
    );
  ASSERT_EQ(r, 0);
  ASSERT_TRUE(objects.empty());

  objects.clear();
  listed.clear();
  ghobject_t start2, next2;
  while (1) {
    r = store->collection_list(ch, start, ghobject_t::get_max(),
			       50,
			       &objects,
			       &next);
    ASSERT_TRUE(sorted(objects));
    ASSERT_EQ(r, 0);
    listed.insert(objects.begin(), objects.end());
    if (objects.size() < 50) {
      ASSERT_TRUE(next.is_max());
      break;
    }
    objects.clear();

    start = next;
  }
  cerr << "listed.size() is " << listed.size() << std::endl;
  ASSERT_TRUE(listed.size() == created.size());
  if (listed2.size()) {
    ASSERT_EQ(listed.size(), listed2.size());
  }
  for (set<ghobject_t>::iterator i = listed.begin();
       i != listed.end();
       ++i) {
    ASSERT_TRUE(created.count(*i));
  }

  for (set<ghobject_t>::iterator i = created.begin();
       i != created.end();
       ++i) {
    ObjectStore::Transaction t;
    t.remove(cid, *i);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  cerr << "cleaning up" << std::endl;
  {
    ObjectStore::Transaction t;
    t.remove_collection(cid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}


class ObjectGenerator {
public:
  virtual ghobject_t create_object(gen_type *gen) = 0;
  virtual ~ObjectGenerator() {}
};

class MixedGenerator : public ObjectGenerator {
public:
  unsigned seq;
  int64_t poolid;
  explicit MixedGenerator(int64_t p) : seq(0), poolid(p) {}
  ghobject_t create_object(gen_type *gen) override {
    char buf[100];
    snprintf(buf, sizeof(buf), "OBJ_%u", seq);
    string name(buf);
    if (seq % 2) {
      for (unsigned i = 0; i < 300; ++i) {
	name.push_back('a');
      }
    }
    ++seq;
    return ghobject_t(
      hobject_t(
	name, string(), rand() & 2 ? CEPH_NOSNAP : rand(),
	(((seq / 1024) % 2) * 0xF00 ) +
	(seq & 0xFF),
	poolid, ""));
  }
};

class SyntheticWorkloadState {
  struct Object {
    bufferlist data;
    map<string, bufferlist> attrs;
  };
public:
  static const unsigned max_in_flight = 16;
  static const unsigned max_objects = 3000;
  static const unsigned max_attr_size = 5;
  static const unsigned max_attr_name_len = 100;
  static const unsigned max_attr_value_len = 1024 * 64;
  coll_t cid;
  unsigned write_alignment;
  unsigned max_object_len, max_write_len;
  unsigned in_flight;
  map<ghobject_t, Object> contents;
  set<ghobject_t> available_objects;
  set<ghobject_t> in_flight_objects;
  ObjectGenerator *object_gen;
  gen_type *rng;
  ObjectStore *store;
  ObjectStore::CollectionHandle ch;

  Mutex lock;
  Cond cond;

  struct EnterExit {
    const char *msg;
    explicit EnterExit(const char *m) : msg(m) {
      //cout << pthread_self() << " enter " << msg << std::endl;
    }
    ~EnterExit() {
      //cout << pthread_self() << " exit " << msg << std::endl;
    }
  };

  class C_SyntheticOnReadable : public Context {
  public:
    SyntheticWorkloadState *state;
    ghobject_t hoid;
    C_SyntheticOnReadable(SyntheticWorkloadState *state, ghobject_t hoid)
      : state(state), hoid(hoid) {}

    void finish(int r) override {
      Mutex::Locker locker(state->lock);
      EnterExit ee("onreadable finish");
      ASSERT_TRUE(state->in_flight_objects.count(hoid));
      ASSERT_EQ(r, 0);
      state->in_flight_objects.erase(hoid);
      if (state->contents.count(hoid))
        state->available_objects.insert(hoid);
      --(state->in_flight);
      state->cond.Signal();

      bufferlist r2;
      r = state->store->read(state->ch, hoid, 0, state->contents[hoid].data.length(), r2);
      ceph_assert(bl_eq(state->contents[hoid].data, r2));
      state->cond.Signal();
    }
  };

  class C_SyntheticOnStash : public Context {
  public:
    SyntheticWorkloadState *state;
    ghobject_t oid, noid;

    C_SyntheticOnStash(SyntheticWorkloadState *state,
		       ghobject_t oid, ghobject_t noid)
      : state(state), oid(oid), noid(noid) {}

    void finish(int r) override {
      Mutex::Locker locker(state->lock);
      EnterExit ee("stash finish");
      ASSERT_TRUE(state->in_flight_objects.count(oid));
      ASSERT_EQ(r, 0);
      state->in_flight_objects.erase(oid);
      if (state->contents.count(noid))
        state->available_objects.insert(noid);
      --(state->in_flight);
      bufferlist r2;
      r = state->store->read(
	state->ch, noid, 0,
	state->contents[noid].data.length(), r2);
      ceph_assert(bl_eq(state->contents[noid].data, r2));
      state->cond.Signal();
    }
  };

  class C_SyntheticOnClone : public Context {
  public:
    SyntheticWorkloadState *state;
    ghobject_t oid, noid;

    C_SyntheticOnClone(SyntheticWorkloadState *state,
                       ghobject_t oid, ghobject_t noid)
      : state(state), oid(oid), noid(noid) {}

    void finish(int r) override {
      Mutex::Locker locker(state->lock);
      EnterExit ee("clone finish");
      ASSERT_TRUE(state->in_flight_objects.count(oid));
      ASSERT_EQ(r, 0);
      state->in_flight_objects.erase(oid);
      if (state->contents.count(oid))
        state->available_objects.insert(oid);
      if (state->contents.count(noid))
        state->available_objects.insert(noid);
      --(state->in_flight);
      bufferlist r2;
      r = state->store->read(state->ch, noid, 0, state->contents[noid].data.length(), r2);
      ceph_assert(bl_eq(state->contents[noid].data, r2));
      state->cond.Signal();
    }
  };

  static void filled_byte_array(bufferlist& bl, size_t size)
  {
    static const char alphanum[] = "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";
    if (!size) {
      return;
    }
    bufferptr bp(size);
    for (unsigned int i = 0; i < size - 1; i++) {
      // severely limit entropy so we can compress...
      bp[i] = alphanum[rand() % 10]; //(sizeof(alphanum) - 1)];
    }
    bp[size - 1] = '\0';

    bl.append(bp);
  }
  
  SyntheticWorkloadState(ObjectStore *store,
			 ObjectGenerator *gen,
			 gen_type *rng,
			 coll_t cid,
			 unsigned max_size,
			 unsigned max_write,
			 unsigned alignment)
    : cid(cid), write_alignment(alignment), max_object_len(max_size),
      max_write_len(max_write), in_flight(0), object_gen(gen),
      rng(rng), store(store),
      lock("State lock") {}

  int init() {
    ObjectStore::Transaction t;
    ch = store->create_new_collection(cid);
    t.create_collection(cid, 0);
    return queue_transaction(store, ch, std::move(t));
  }
  void shutdown() {
    while (1) {
      vector<ghobject_t> objects;
      int r = store->collection_list(ch, ghobject_t(), ghobject_t::get_max(),
				     10, &objects, 0);
      ceph_assert(r >= 0);
      if (objects.empty())
	break;
      ObjectStore::Transaction t;
      for (vector<ghobject_t>::iterator p = objects.begin();
	   p != objects.end(); ++p) {
	t.remove(cid, *p);
      }
      queue_transaction(store, ch, std::move(t));
    }
    ObjectStore::Transaction t;
    t.remove_collection(cid);
    queue_transaction(store, ch, std::move(t));
  }
  void statfs(store_statfs_t& stat) {
    store->statfs(&stat);
  }

  ghobject_t get_uniform_random_object() {
    while (in_flight >= max_in_flight || available_objects.empty())
      cond.Wait(lock);
    boost::uniform_int<> choose(0, available_objects.size() - 1);
    int index = choose(*rng);
    set<ghobject_t>::iterator i = available_objects.begin();
    for ( ; index > 0; --index, ++i) ;
    ghobject_t ret = *i;
    return ret;
  }

  void wait_for_ready() {
    while (in_flight >= max_in_flight)
      cond.Wait(lock);
  }

  void wait_for_done() {
    Mutex::Locker locker(lock);
    while (in_flight)
      cond.Wait(lock);
  }

  bool can_create() {
    return (available_objects.size() + in_flight_objects.size()) < max_objects;
  }

  bool can_unlink() {
    return (available_objects.size() + in_flight_objects.size()) > 0;
  }

  unsigned get_random_alloc_hints() {
    unsigned f = 0;
    {
      boost::uniform_int<> u(0, 3);
      switch (u(*rng)) {
      case 1:
	f |= CEPH_OSD_ALLOC_HINT_FLAG_SEQUENTIAL_WRITE;
	break;
      case 2:
	f |= CEPH_OSD_ALLOC_HINT_FLAG_RANDOM_WRITE;
	break;
      }
    }
    {
      boost::uniform_int<> u(0, 3);
      switch (u(*rng)) {
      case 1:
	f |= CEPH_OSD_ALLOC_HINT_FLAG_SEQUENTIAL_READ;
	break;
      case 2:
	f |= CEPH_OSD_ALLOC_HINT_FLAG_RANDOM_READ;
	break;
      }
    }
    {
      // append_only, immutable
      boost::uniform_int<> u(0, 4);
      f |= u(*rng) << 4;
    }
    {
      boost::uniform_int<> u(0, 3);
      switch (u(*rng)) {
      case 1:
	f |= CEPH_OSD_ALLOC_HINT_FLAG_SHORTLIVED;
	break;
      case 2:
	f |= CEPH_OSD_ALLOC_HINT_FLAG_LONGLIVED;
	break;
      }
    }
    {
      boost::uniform_int<> u(0, 3);
      switch (u(*rng)) {
      case 1:
	f |= CEPH_OSD_ALLOC_HINT_FLAG_COMPRESSIBLE;
	break;
      case 2:
	f |= CEPH_OSD_ALLOC_HINT_FLAG_INCOMPRESSIBLE;
	break;
      }
    }
    return f;
  }

  int touch() {
    Mutex::Locker locker(lock);
    EnterExit ee("touch");
    if (!can_create())
      return -ENOSPC;
    wait_for_ready();
    ghobject_t new_obj = object_gen->create_object(rng);
    available_objects.erase(new_obj);
    ObjectStore::Transaction t;
    t.touch(cid, new_obj);
    boost::uniform_int<> u(17, 22);
    boost::uniform_int<> v(12, 17);
    t.set_alloc_hint(cid, new_obj,
		      1ull << u(*rng),
		      1ull << v(*rng),
		      get_random_alloc_hints());
    ++in_flight;
    in_flight_objects.insert(new_obj);
    if (!contents.count(new_obj))
      contents[new_obj] = Object();
    t.register_on_applied(new C_SyntheticOnReadable(this, new_obj));
    int status = store->queue_transaction(ch, std::move(t));
    return status;
  }

  int stash() {
    Mutex::Locker locker(lock);
    EnterExit ee("stash");
    if (!can_unlink())
      return -ENOENT;
    if (!can_create())
      return -ENOSPC;
    wait_for_ready();

    ghobject_t old_obj;
    int max = 20;
    do {
      old_obj = get_uniform_random_object();
    } while (--max && !contents[old_obj].data.length());
    available_objects.erase(old_obj);
    ghobject_t new_obj = old_obj;
    new_obj.generation++;
    available_objects.erase(new_obj);

    ObjectStore::Transaction t;
    t.collection_move_rename(cid, old_obj, cid, new_obj);
    ++in_flight;
    in_flight_objects.insert(old_obj);

    contents[new_obj].attrs = contents[old_obj].attrs;
    contents[new_obj].data = contents[old_obj].data;
    contents.erase(old_obj);
    t.register_on_applied(new C_SyntheticOnStash(this, old_obj, new_obj));
    int status = store->queue_transaction(ch, std::move(t));
    return status;
  }

  int clone() {
    Mutex::Locker locker(lock);
    EnterExit ee("clone");
    if (!can_unlink())
      return -ENOENT;
    if (!can_create())
      return -ENOSPC;
    wait_for_ready();

    ghobject_t old_obj;
    int max = 20;
    do {
      old_obj = get_uniform_random_object();
    } while (--max && !contents[old_obj].data.length());
    available_objects.erase(old_obj);
    ghobject_t new_obj = object_gen->create_object(rng);
    // make the hash match
    new_obj.hobj.set_hash(old_obj.hobj.get_hash());
    available_objects.erase(new_obj);

    ObjectStore::Transaction t;
    t.clone(cid, old_obj, new_obj);
    ++in_flight;
    in_flight_objects.insert(old_obj);

    contents[new_obj].attrs = contents[old_obj].attrs;
    contents[new_obj].data = contents[old_obj].data;

    t.register_on_applied(new C_SyntheticOnClone(this, old_obj, new_obj));
    int status = store->queue_transaction(ch, std::move(t));
    return status;
  }

  int clone_range() {
    Mutex::Locker locker(lock);
    EnterExit ee("clone_range");
    if (!can_unlink())
      return -ENOENT;
    if (!can_create())
      return -ENOSPC;
    wait_for_ready();

    ghobject_t old_obj;
    int max = 20;
    do {
      old_obj = get_uniform_random_object();
    } while (--max && !contents[old_obj].data.length());
    bufferlist &srcdata = contents[old_obj].data;
    if (srcdata.length() == 0) {
      return 0;
    }
    available_objects.erase(old_obj);
    ghobject_t new_obj = get_uniform_random_object();
    available_objects.erase(new_obj);

    boost::uniform_int<> u1(0, max_object_len - max_write_len);
    boost::uniform_int<> u2(0, max_write_len);
    uint64_t srcoff = u1(*rng);
    // make src and dst offsets match, since that's what the osd does
    uint64_t dstoff = srcoff; //u1(*rng);
    uint64_t len = u2(*rng);
    if (write_alignment) {
      srcoff = round_up_to(srcoff, write_alignment);
      dstoff = round_up_to(dstoff, write_alignment);
      len = round_up_to(len, write_alignment);
    }

    if (srcoff > srcdata.length() - 1) {
      srcoff = srcdata.length() - 1;
    }
    if (srcoff + len > srcdata.length()) {
      len = srcdata.length() - srcoff;
    }
    if (0)
      cout << __func__ << " from " << srcoff << "~" << len
	 << " (size " << srcdata.length() << ") to "
	 << dstoff << "~" << len << std::endl;

    ObjectStore::Transaction t;
    t.clone_range(cid, old_obj, new_obj, srcoff, len, dstoff);
    ++in_flight;
    in_flight_objects.insert(old_obj);

    bufferlist bl;
    if (srcoff < srcdata.length()) {
      if (srcoff + len > srcdata.length()) {
	bl.substr_of(srcdata, srcoff, srcdata.length() - srcoff);
      } else {
	bl.substr_of(srcdata, srcoff, len);
      }
    }

    bufferlist& dstdata = contents[new_obj].data;
    if (dstdata.length() <= dstoff) {
      if (bl.length() > 0) {
        dstdata.append_zero(dstoff - dstdata.length());
        dstdata.append(bl);
      }
    } else {
      bufferlist value;
      ceph_assert(dstdata.length() > dstoff);
      dstdata.copy(0, dstoff, value);
      value.append(bl);
      if (value.length() < dstdata.length())
        dstdata.copy(value.length(),
		     dstdata.length() - value.length(), value);
      value.swap(dstdata);
    }

    t.register_on_applied(new C_SyntheticOnClone(this, old_obj, new_obj));
    int status = store->queue_transaction(ch, std::move(t));
    return status;
  }


  int write() {
    Mutex::Locker locker(lock);
    EnterExit ee("write");
    if (!can_unlink())
      return -ENOENT;
    wait_for_ready();

    ghobject_t new_obj = get_uniform_random_object();
    available_objects.erase(new_obj);
    ObjectStore::Transaction t;

    boost::uniform_int<> u1(0, max_object_len - max_write_len);
    boost::uniform_int<> u2(0, max_write_len);
    uint64_t offset = u1(*rng);
    uint64_t len = u2(*rng);
    bufferlist bl;
    if (write_alignment) {
      offset = round_up_to(offset, write_alignment);
      len = round_up_to(len, write_alignment);
    }

    filled_byte_array(bl, len);

    bufferlist& data = contents[new_obj].data;
    if (data.length() <= offset) {
      if (len > 0) {
        data.append_zero(offset-data.length());
        data.append(bl);
      }
    } else {
      bufferlist value;
      ceph_assert(data.length() > offset);
      data.copy(0, offset, value);
      value.append(bl);
      if (value.length() < data.length())
        data.copy(value.length(),
		  data.length()-value.length(), value);
      value.swap(data);
    }

    t.write(cid, new_obj, offset, len, bl);
    ++in_flight;
    in_flight_objects.insert(new_obj);
    t.register_on_applied(new C_SyntheticOnReadable(this, new_obj));
    int status = store->queue_transaction(ch, std::move(t));
    return status;
  }

  int truncate() {
    Mutex::Locker locker(lock);
    EnterExit ee("truncate");
    if (!can_unlink())
      return -ENOENT;
    wait_for_ready();

    ghobject_t obj = get_uniform_random_object();
    available_objects.erase(obj);
    ObjectStore::Transaction t;

    boost::uniform_int<> choose(0, max_object_len);
    size_t len = choose(*rng);
    if (write_alignment) {
      len = round_up_to(len, write_alignment);
    }

    t.truncate(cid, obj, len);
    ++in_flight;
    in_flight_objects.insert(obj);
    bufferlist& data = contents[obj].data;
    if (data.length() <= len) {
      data.append_zero(len - data.length());
    } else {
      bufferlist bl;
      data.copy(0, len, bl);
      bl.swap(data);
    }

    t.register_on_applied(new C_SyntheticOnReadable(this, obj));
    int status = store->queue_transaction(ch, std::move(t));
    return status;
  }

  int zero() {
    Mutex::Locker locker(lock);
    EnterExit ee("zero");
    if (!can_unlink())
      return -ENOENT;
    wait_for_ready();

    ghobject_t new_obj = get_uniform_random_object();
    available_objects.erase(new_obj);
    ObjectStore::Transaction t;

    boost::uniform_int<> u1(0, max_object_len - max_write_len);
    boost::uniform_int<> u2(0, max_write_len);
    uint64_t offset = u1(*rng);
    uint64_t len = u2(*rng);
    if (write_alignment) {
      offset = round_up_to(offset, write_alignment);
      len = round_up_to(len, write_alignment);
    }

    if (len > 0) {
      auto& data = contents[new_obj].data;
      if (data.length() < offset + len) {
	data.append_zero(offset+len-data.length());
      }
      bufferlist n;
      n.substr_of(data, 0, offset);
      n.append_zero(len);
      if (data.length() > offset + len)
	data.copy(offset + len, data.length() - offset - len, n);
      data.swap(n);
    }

    t.zero(cid, new_obj, offset, len);
    ++in_flight;
    in_flight_objects.insert(new_obj);
    t.register_on_applied(new C_SyntheticOnReadable(this, new_obj));
    int status = store->queue_transaction(ch, std::move(t));
    return status;
  }

  void read() {
    EnterExit ee("read");
    boost::uniform_int<> u1(0, max_object_len/2);
    boost::uniform_int<> u2(0, max_object_len);
    uint64_t offset = u1(*rng);
    uint64_t len = u2(*rng);
    if (offset > len)
      swap(offset, len);

    ghobject_t obj;
    bufferlist expected;
    int r;
    {
      Mutex::Locker locker(lock);
      EnterExit ee("read locked");
      if (!can_unlink())
        return ;
      wait_for_ready();

      obj = get_uniform_random_object();
      expected = contents[obj].data;
    }
    bufferlist bl, result;
    if (0) cout << " obj " << obj
	 << " size " << expected.length()
	 << " offset " << offset
	 << " len " << len << std::endl;
    r = store->read(ch, obj, offset, len, result);
    if (offset >= expected.length()) {
      ASSERT_EQ(r, 0);
    } else {
      size_t max_len = expected.length() - offset;
      if (len > max_len)
        len = max_len;
      ceph_assert(len == result.length());
      ASSERT_EQ(len, result.length());
      expected.copy(offset, len, bl);
      ASSERT_EQ(r, (int)len);
      ASSERT_TRUE(bl_eq(bl, result));
    }
  }

  int setattrs() {
    Mutex::Locker locker(lock);
    EnterExit ee("setattrs");
    if (!can_unlink())
      return -ENOENT;
    wait_for_ready();

    ghobject_t obj = get_uniform_random_object();
    available_objects.erase(obj);
    ObjectStore::Transaction t;

    boost::uniform_int<> u0(1, max_attr_size);
    boost::uniform_int<> u1(4, max_attr_name_len);
    boost::uniform_int<> u2(4, max_attr_value_len);
    boost::uniform_int<> u3(0, 100);
    uint64_t size = u0(*rng);
    uint64_t name_len;
    map<string, bufferlist> attrs;
    set<string> keys;
    for (map<string, bufferlist>::iterator it = contents[obj].attrs.begin();
         it != contents[obj].attrs.end(); ++it)
      keys.insert(it->first);

    while (size--) {
      bufferlist name, value;
      uint64_t get_exist = u3(*rng);
      uint64_t value_len = u2(*rng);
      filled_byte_array(value, value_len);
      if (get_exist < 50 && keys.size()) {
        set<string>::iterator k = keys.begin();
        attrs[*k] = value;
        contents[obj].attrs[*k] = value;
        keys.erase(k);
      } else {
        name_len = u1(*rng);
        filled_byte_array(name, name_len);
        attrs[name.c_str()] = value;
        contents[obj].attrs[name.c_str()] = value;
      }
    }
    t.setattrs(cid, obj, attrs);
    ++in_flight;
    in_flight_objects.insert(obj);
    t.register_on_applied(new C_SyntheticOnReadable(this, obj));
    int status = store->queue_transaction(ch, std::move(t));
    return status;
  }

  void getattrs() {
    EnterExit ee("getattrs");
    ghobject_t obj;
    map<string, bufferlist> expected;
    {
      Mutex::Locker locker(lock);
      EnterExit ee("getattrs locked");
      if (!can_unlink())
        return ;
      wait_for_ready();

      int retry = 10;
      do {
        obj = get_uniform_random_object();
        if (!--retry)
          return ;
      } while (contents[obj].attrs.empty());
      expected = contents[obj].attrs;
    }
    map<string, bufferlist> attrs;
    int r = store->getattrs(ch, obj, attrs);
    ASSERT_TRUE(r == 0);
    ASSERT_TRUE(attrs.size() == expected.size());
    for (map<string, bufferlist>::iterator it = expected.begin();
         it != expected.end(); ++it) {
      ASSERT_TRUE(bl_eq(attrs[it->first], it->second));
    }
  }

  void getattr() {
    EnterExit ee("getattr");
    ghobject_t obj;
    int r;
    int retry;
    map<string, bufferlist> expected;
    {
      Mutex::Locker locker(lock);
      EnterExit ee("getattr locked");
      if (!can_unlink())
        return ;
      wait_for_ready();

      retry = 10;
      do {
        obj = get_uniform_random_object();
        if (!--retry)
          return ;
      } while (contents[obj].attrs.empty());
      expected = contents[obj].attrs;
    }
    boost::uniform_int<> u(0, expected.size()-1);
    retry = u(*rng);
    map<string, bufferlist>::iterator it = expected.begin();
    while (retry) {
      retry--;
      ++it;
    }

    bufferlist bl;
    r = store->getattr(ch, obj, it->first, bl);
    ASSERT_EQ(r, 0);
    ASSERT_TRUE(bl_eq(it->second, bl));
  }

  int rmattr() {
    Mutex::Locker locker(lock);
    EnterExit ee("rmattr");
    if (!can_unlink())
      return -ENOENT;
    wait_for_ready();

    ghobject_t obj;
    int retry = 10;
    do {
      obj = get_uniform_random_object();
      if (!--retry)
        return 0;
    } while (contents[obj].attrs.empty());

    boost::uniform_int<> u(0, contents[obj].attrs.size()-1);
    retry = u(*rng);
    map<string, bufferlist>::iterator it = contents[obj].attrs.begin();
    while (retry) {
      retry--;
      ++it;
    }

    available_objects.erase(obj);
    ObjectStore::Transaction t;
    t.rmattr(cid, obj, it->first);

    contents[obj].attrs.erase(it->first);
    ++in_flight;
    in_flight_objects.insert(obj);
    t.register_on_applied(new C_SyntheticOnReadable(this, obj));
    int status = store->queue_transaction(ch, std::move(t));
    return status;
  }

  void fsck(bool deep) {
    Mutex::Locker locker(lock);
    EnterExit ee("fsck");
    while (in_flight)
      cond.Wait(lock);
    ch.reset();
    store->umount();
    int r = store->fsck(deep);
    ceph_assert(r == 0 || r == -EOPNOTSUPP);
    store->mount();
    ch = store->open_collection(cid);
  }

  void scan() {
    Mutex::Locker locker(lock);
    EnterExit ee("scan");
    while (in_flight)
      cond.Wait(lock);
    vector<ghobject_t> objects;
    set<ghobject_t> objects_set, objects_set2;
    ghobject_t next, current;
    while (1) {
      //cerr << "scanning..." << std::endl;
      int r = store->collection_list(ch, current, ghobject_t::get_max(), 100,
				     &objects, &next);
      ASSERT_EQ(r, 0);
      ASSERT_TRUE(sorted(objects));
      objects_set.insert(objects.begin(), objects.end());
      objects.clear();
      if (next.is_max()) break;
      current = next;
    }
    if (objects_set.size() != available_objects.size()) {
      for (set<ghobject_t>::iterator p = objects_set.begin();
	   p != objects_set.end();
	   ++p)
	if (available_objects.count(*p) == 0) {
	  cerr << "+ " << *p << std::endl;
	  ceph_abort();
	}
      for (set<ghobject_t>::iterator p = available_objects.begin();
	   p != available_objects.end();
	   ++p)
	if (objects_set.count(*p) == 0)
	  cerr << "- " << *p << std::endl;
      //cerr << " objects_set: " << objects_set << std::endl;
      //cerr << " available_set: " << available_objects << std::endl;
      ceph_abort_msg("badness");
    }

    ASSERT_EQ(objects_set.size(), available_objects.size());
    for (set<ghobject_t>::iterator i = objects_set.begin();
	 i != objects_set.end();
	 ++i) {
      ASSERT_GT(available_objects.count(*i), (unsigned)0);
    }

    int r = store->collection_list(ch, ghobject_t(), ghobject_t::get_max(),
				   INT_MAX, &objects, 0);
    ASSERT_EQ(r, 0);
    objects_set2.insert(objects.begin(), objects.end());
    ASSERT_EQ(objects_set2.size(), available_objects.size());
    for (set<ghobject_t>::iterator i = objects_set2.begin();
	 i != objects_set2.end();
	 ++i) {
      ASSERT_GT(available_objects.count(*i), (unsigned)0);
      if (available_objects.count(*i) == 0) {
	cerr << "+ " << *i << std::endl;
      }
    }
  }

  void stat() {
    EnterExit ee("stat");
    ghobject_t hoid;
    uint64_t expected;
    {
      Mutex::Locker locker(lock);
      EnterExit ee("stat lock1");
      if (!can_unlink())
        return ;
      hoid = get_uniform_random_object();
      in_flight_objects.insert(hoid);
      available_objects.erase(hoid);
      ++in_flight;
      expected = contents[hoid].data.length();
    }
    struct stat buf;
    int r = store->stat(ch, hoid, &buf);
    ASSERT_EQ(0, r);
    ceph_assert((uint64_t)buf.st_size == expected);
    ASSERT_TRUE((uint64_t)buf.st_size == expected);
    {
      Mutex::Locker locker(lock);
      EnterExit ee("stat lock2");
      --in_flight;
      cond.Signal();
      in_flight_objects.erase(hoid);
      available_objects.insert(hoid);
    }
  }

  int unlink() {
    Mutex::Locker locker(lock);
    EnterExit ee("unlink");
    if (!can_unlink())
      return -ENOENT;
    ghobject_t to_remove = get_uniform_random_object();
    ObjectStore::Transaction t;
    t.remove(cid, to_remove);
    ++in_flight;
    available_objects.erase(to_remove);
    in_flight_objects.insert(to_remove);
    contents.erase(to_remove);
    t.register_on_applied(new C_SyntheticOnReadable(this, to_remove));
    int status = store->queue_transaction(ch, std::move(t));
    return status;
  }

  void print_internal_state() {
    Mutex::Locker locker(lock);
    cerr << "available_objects: " << available_objects.size()
	 << " in_flight_objects: " << in_flight_objects.size()
	 << " total objects: " << in_flight_objects.size() + available_objects.size()
	 << " in_flight " << in_flight << std::endl;
  }
};


void StoreTest::doSyntheticTest(
		     int num_ops,
		     uint64_t max_obj, uint64_t max_wr, uint64_t align)
{
  MixedGenerator gen(555);
  gen_type rng(time(NULL));
  coll_t cid(spg_t(pg_t(0,555), shard_id_t::NO_SHARD));

  SetVal(g_conf(), "bluestore_fsck_on_mount", "false");
  SetVal(g_conf(), "bluestore_fsck_on_umount", "false");
  g_ceph_context->_conf.apply_changes(nullptr);

  SyntheticWorkloadState test_obj(store.get(), &gen, &rng, cid,
				  max_obj, max_wr, align);
  test_obj.init();
  for (int i = 0; i < num_ops/10; ++i) {
    if (!(i % 500)) cerr << "seeding object " << i << std::endl;
    test_obj.touch();
  }
  for (int i = 0; i < num_ops; ++i) {
    if (!(i % 1000)) {
      cerr << "Op " << i << std::endl;
      test_obj.print_internal_state();
    }
    boost::uniform_int<> true_false(0, 999);
    int val = true_false(rng);
    if (val > 998) {
      test_obj.fsck(true);
    } else if (val > 997) {
      test_obj.fsck(false);
    } else if (val > 970) {
      test_obj.scan();
    } else if (val > 950) {
      test_obj.stat();
    } else if (val > 850) {
      test_obj.zero();
    } else if (val > 800) {
      test_obj.unlink();
    } else if (val > 550) {
      test_obj.write();
    } else if (val > 500) {
      test_obj.clone();
    } else if (val > 450) {
      test_obj.clone_range();
    } else if (val > 300) {
      test_obj.stash();
    } else if (val > 100) {
      test_obj.read();
    } else {
      test_obj.truncate();
    }
  }
  test_obj.wait_for_done();
  test_obj.shutdown();
}

TEST_P(StoreTest, Synthetic) {
  doSyntheticTest(10000, 400*1024, 40*1024, 0);
}

TEST_P(StoreTestSpecificAUSize, BlueFSExtenderTest) {
  if(string(GetParam()) != "bluestore")
    return;

  SetVal(g_conf(), "bluestore_block_db_size", "0");
  SetVal(g_conf(), "bluestore_block_wal_size", "0");
  SetVal(g_conf(), "bluestore_bluefs_min", "12582912");
  SetVal(g_conf(), "bluestore_bluefs_min_free", "4194304");
  SetVal(g_conf(), "bluestore_bluefs_gift_ratio", "0");
  SetVal(g_conf(), "bluestore_bluefs_min_ratio", "0");
  SetVal(g_conf(), "bluestore_bluefs_balance_interval", "100000");
  SetVal(g_conf(), "bluestore_bluefs_db_compatibility", "false");

  g_conf().apply_changes(nullptr);

  StartDeferred(4096);

  doSyntheticTest(10000, 400*1024, 40*1024, 0);

  BlueStore* bstore = NULL;
  EXPECT_NO_THROW(bstore = dynamic_cast<BlueStore*> (store.get()));

  // verify downgrades are broken and repair that
  bstore->umount();
  ASSERT_EQ(bstore->fsck(false), 0);

  SetVal(g_conf(), "bluestore_bluefs_db_compatibility", "true");
  g_conf().apply_changes(nullptr);

  ASSERT_EQ(bstore->fsck(false), 1);
  ASSERT_EQ(bstore->repair(false), 0);
  ASSERT_EQ(bstore->fsck(false), 0);
  bstore->mount();
}

#if defined(WITH_BLUESTORE)
TEST_P(StoreTestSpecificAUSize, SyntheticMatrixSharding) {
  if (string(GetParam()) != "bluestore")
    return;
  
  const char *m[][10] = {
    { "bluestore_min_alloc_size", "4096", 0 }, // must be the first!
    { "num_ops", "50000", 0 },
    { "max_write", "65536", 0 },
    { "max_size", "262144", 0 },
    { "alignment", "4096", 0 },
    { "bluestore_max_blob_size", "65536", 0 },
    { "bluestore_extent_map_shard_min_size", "60", 0 },
    { "bluestore_extent_map_shard_max_size", "300", 0 },
    { "bluestore_extent_map_shard_target_size", "150", 0 },
    { "bluestore_default_buffered_read", "true", 0 },
    { "bluestore_default_buffered_write", "true", 0 },
    { 0 },
  };
  do_matrix(m, std::bind(&StoreTest::doSyntheticTest, this, _1, _2, _3, _4));
}

TEST_P(StoreTestSpecificAUSize, ZipperPatternSharded) {
  if(string(GetParam()) != "bluestore")
    return;
  StartDeferred(4096);

  int r;
  coll_t cid;
  ghobject_t a(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  bufferlist bl;
  int len = 4096;
  bufferptr bp(len);
  bp.zero();
  bl.append(bp);
  for (int i=0; i<1000; ++i) {
    ObjectStore::Transaction t;
    t.write(cid, a, i*2*len, len, bl, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  for (int i=0; i<1000; ++i) {
    ObjectStore::Transaction t;
    t.write(cid, a, i*2*len + 1, len, bl, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, a);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTestSpecificAUSize, SyntheticMatrixCsumAlgorithm) {
  if (string(GetParam()) != "bluestore")
    return;

  const char *m[][10] = {
    { "bluestore_min_alloc_size", "65536", 0 }, // must be the first!
    { "max_write", "65536", 0 },
    { "max_size", "1048576", 0 },
    { "alignment", "16", 0 },
    { "bluestore_csum_type", "crc32c", "crc32c_16", "crc32c_8", "xxhash32",
      "xxhash64", "none", 0 },
    { "bluestore_default_buffered_write", "false", 0 },
    { 0 },
  };
  do_matrix(m, std::bind(&StoreTest::doSyntheticTest, this, _1, _2, _3, _4));
}

TEST_P(StoreTestSpecificAUSize, SyntheticMatrixCsumVsCompression) {
  if (string(GetParam()) != "bluestore")
    return;

  const char *m[][10] = {
    { "bluestore_min_alloc_size", "4096", "16384", 0 }, //to be the first!
    { "max_write", "131072", 0 },
    { "max_size", "262144", 0 },
    { "alignment", "512", 0 },
    { "bluestore_compression_mode", "force", 0},
    { "bluestore_compression_algorithm", "snappy", "zlib", 0 },
    { "bluestore_csum_type", "crc32c", 0 },
    { "bluestore_default_buffered_read", "true", "false", 0 },
    { "bluestore_default_buffered_write", "true", "false", 0 },
    { "bluestore_sync_submit_transaction", "false", 0 },
    { 0 },
  };
  do_matrix(m, std::bind(&StoreTest::doSyntheticTest, this, _1, _2, _3, _4));
}

TEST_P(StoreTestSpecificAUSize, SyntheticMatrixCompression) {
  if (string(GetParam()) != "bluestore")
    return;

  const char *m[][10] = {
    { "bluestore_min_alloc_size", "4096", "65536", 0 }, // to be the first!
    { "max_write", "1048576", 0 },
    { "max_size", "4194304", 0 },
    { "alignment", "65536", 0 },
    { "bluestore_compression_mode", "force", "aggressive", "passive", "none", 0},
    { "bluestore_default_buffered_write", "false", 0 },
    { "bluestore_sync_submit_transaction", "true", 0 },
    { 0 },
  };
  do_matrix(m, std::bind(&StoreTest::doSyntheticTest, this, _1, _2, _3, _4));
}

TEST_P(StoreTestSpecificAUSize, SyntheticMatrixCompressionAlgorithm) {
  if (string(GetParam()) != "bluestore")
    return;

  const char *m[][10] = {
    { "bluestore_min_alloc_size", "4096", "65536", 0 }, // to be the first!
    { "max_write", "1048576", 0 },
    { "max_size", "4194304", 0 },
    { "alignment", "65536", 0 },
    { "bluestore_compression_algorithm", "zlib", "snappy", 0 },
    { "bluestore_compression_mode", "force", 0 },
    { "bluestore_default_buffered_write", "false", 0 },
    { 0 },
  };
  do_matrix(m, std::bind(&StoreTest::doSyntheticTest, this, _1, _2, _3, _4));
}

TEST_P(StoreTestSpecificAUSize, SyntheticMatrixNoCsum) {
  if (string(GetParam()) != "bluestore")
    return;

  const char *m[][10] = {
    { "bluestore_min_alloc_size", "4096", "65536", 0 }, // to be the first!
    { "max_write", "65536", 0 },
    { "max_size", "1048576", 0 },
    { "alignment", "512", 0 },
    { "bluestore_max_blob_size", "262144", 0 },
    { "bluestore_compression_mode", "force", "none", 0},
    { "bluestore_csum_type", "none", 0},
    { "bluestore_default_buffered_read", "true", "false", 0 },
    { "bluestore_default_buffered_write", "true", 0 },
    { "bluestore_sync_submit_transaction", "true", "false", 0 },
    { 0 },
  };
  do_matrix(m, std::bind(&StoreTest::doSyntheticTest, this, _1, _2, _3, _4));
}

TEST_P(StoreTestSpecificAUSize, SyntheticMatrixPreferDeferred) {
  if (string(GetParam()) != "bluestore")
    return;

  const char *m[][10] = {
    { "bluestore_min_alloc_size", "4096", "65536", 0 }, // to be the first!
    { "max_write", "65536", 0 },
    { "max_size", "1048576", 0 },
    { "alignment", "512", 0 },
    { "bluestore_max_blob_size", "262144", 0 },
    { "bluestore_compression_mode", "force", "none", 0},
    { "bluestore_prefer_deferred_size", "32768", "0", 0},
    { 0 },
  };
  do_matrix(m, std::bind(&StoreTest::doSyntheticTest, this, _1, _2, _3, _4));
}
#endif // WITH_BLUESTORE

TEST_P(StoreTest, AttrSynthetic) {
  MixedGenerator gen(447);
  gen_type rng(time(NULL));
  coll_t cid(spg_t(pg_t(0,447),shard_id_t::NO_SHARD));

  SyntheticWorkloadState test_obj(store.get(), &gen, &rng, cid, 40*1024, 4*1024, 0);
  test_obj.init();
  for (int i = 0; i < 500; ++i) {
    if (!(i % 10)) cerr << "seeding object " << i << std::endl;
    test_obj.touch();
  }
  for (int i = 0; i < 1000; ++i) {
    if (!(i % 100)) {
      cerr << "Op " << i << std::endl;
      test_obj.print_internal_state();
    }
    boost::uniform_int<> true_false(0, 99);
    int val = true_false(rng);
    if (val > 97) {
      test_obj.scan();
    } else if (val > 93) {
      test_obj.stat();
    } else if (val > 75) {
      test_obj.rmattr();
    } else if (val > 47) {
      test_obj.setattrs();
    } else if (val > 45) {
      test_obj.clone();
    } else if (val > 37) {
      test_obj.stash();
    } else if (val > 30) {
      test_obj.getattrs();
    } else {
      test_obj.getattr();
    }
  }
  test_obj.wait_for_done();
  test_obj.shutdown();
}

TEST_P(StoreTest, HashCollisionTest) {
  int64_t poolid = 11;
  coll_t cid(spg_t(pg_t(0,poolid),shard_id_t::NO_SHARD));
  int r;
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  string base = "";
  for (int i = 0; i < 100; ++i) base.append("aaaaa");
  set<ghobject_t> created;
  for (int n = 0; n < 10; ++n) {
    char nbuf[100];
    sprintf(nbuf, "n%d", n);
  for (int i = 0; i < 1000; ++i) {
    char buf[100];
    sprintf(buf, "%d", i);
    if (!(i % 100)) {
      cerr << "Object n" << n << " "<< i << std::endl;
    }
    ghobject_t hoid(hobject_t(string(buf) + base, string(), CEPH_NOSNAP, 0, poolid, string(nbuf)));
    {
      ObjectStore::Transaction t;
      t.touch(cid, hoid);
      r = queue_transaction(store, ch, std::move(t));
      ASSERT_EQ(r, 0);
    }
    created.insert(hoid);
  }
  }
  vector<ghobject_t> objects;
  r = store->collection_list(ch, ghobject_t(), ghobject_t::get_max(), INT_MAX, &objects, 0);
  ASSERT_EQ(r, 0);
  set<ghobject_t> listed(objects.begin(), objects.end());
  cerr << "listed.size() is " << listed.size() << " and created.size() is " << created.size() << std::endl;
  ASSERT_TRUE(listed.size() == created.size());
  objects.clear();
  listed.clear();
  ghobject_t current, next;
  while (1) {
    r = store->collection_list(ch, current, ghobject_t::get_max(), 60,
			       &objects, &next);
    ASSERT_EQ(r, 0);
    ASSERT_TRUE(sorted(objects));
    for (vector<ghobject_t>::iterator i = objects.begin();
	 i != objects.end();
	 ++i) {
      if (listed.count(*i))
	cerr << *i << " repeated" << std::endl;
      listed.insert(*i);
    }
    if (objects.size() < 50) {
      ASSERT_TRUE(next.is_max());
      break;
    }
    objects.clear();
    current = next;
  }
  cerr << "listed.size() is " << listed.size() << std::endl;
  ASSERT_TRUE(listed.size() == created.size());
  for (set<ghobject_t>::iterator i = listed.begin();
       i != listed.end();
       ++i) {
    ASSERT_TRUE(created.count(*i));
  }

  for (set<ghobject_t>::iterator i = created.begin();
       i != created.end();
       ++i) {
    ObjectStore::Transaction t;
    t.remove(cid, *i);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ObjectStore::Transaction t;
  t.remove_collection(cid);
  r = queue_transaction(store, ch, std::move(t));
  ASSERT_EQ(r, 0);
}

TEST_P(StoreTest, ScrubTest) {
  int64_t poolid = 111;
  coll_t cid(spg_t(pg_t(0, poolid),shard_id_t(1)));
  int r;
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  string base = "aaaaa";
  set<ghobject_t> created;
  for (int i = 0; i < 1000; ++i) {
    char buf[100];
    sprintf(buf, "%d", i);
    if (!(i % 5)) {
      cerr << "Object " << i << std::endl;
    }
    ghobject_t hoid(hobject_t(string(buf) + base, string(), CEPH_NOSNAP, i,
			      poolid, ""),
		    ghobject_t::NO_GEN, shard_id_t(1));
    {
      ObjectStore::Transaction t;
      t.touch(cid, hoid);
      r = queue_transaction(store, ch, std::move(t));
      ASSERT_EQ(r, 0);
    }
    created.insert(hoid);
  }

  // Add same hobject_t but different generation
  {
    ghobject_t hoid1(hobject_t("same-object", string(), CEPH_NOSNAP, 0, poolid, ""),
		     ghobject_t::NO_GEN, shard_id_t(1));
    ghobject_t hoid2(hobject_t("same-object", string(), CEPH_NOSNAP, 0, poolid, ""), (gen_t)1, shard_id_t(1));
    ghobject_t hoid3(hobject_t("same-object", string(), CEPH_NOSNAP, 0, poolid, ""), (gen_t)2, shard_id_t(1));
    ObjectStore::Transaction t;
    t.touch(cid, hoid1);
    t.touch(cid, hoid2);
    t.touch(cid, hoid3);
    r = queue_transaction(store, ch, std::move(t));
    created.insert(hoid1);
    created.insert(hoid2);
    created.insert(hoid3);
    ASSERT_EQ(r, 0);
  }

  vector<ghobject_t> objects;
  r = store->collection_list(ch, ghobject_t(), ghobject_t::get_max(),
			     INT_MAX, &objects, 0);
  ASSERT_EQ(r, 0);
  set<ghobject_t> listed(objects.begin(), objects.end());
  cerr << "listed.size() is " << listed.size() << " and created.size() is " << created.size() << std::endl;
  ASSERT_TRUE(listed.size() == created.size());
  objects.clear();
  listed.clear();
  ghobject_t current, next;
  while (1) {
    r = store->collection_list(ch, current, ghobject_t::get_max(), 60,
			       &objects, &next);
    ASSERT_EQ(r, 0);
    ASSERT_TRUE(sorted(objects));
    for (vector<ghobject_t>::iterator i = objects.begin();
         i != objects.end(); ++i) {
      if (listed.count(*i))
        cerr << *i << " repeated" << std::endl;
      listed.insert(*i);
    }
    if (objects.size() < 50) {
      ASSERT_TRUE(next.is_max());
      break;
    }
    objects.clear();
    current = next.get_boundary();
  }
  cerr << "listed.size() is " << listed.size() << std::endl;
  ASSERT_TRUE(listed.size() == created.size());
  for (set<ghobject_t>::iterator i = listed.begin();
       i != listed.end();
       ++i) {
    ASSERT_TRUE(created.count(*i));
  }

  for (set<ghobject_t>::iterator i = created.begin();
       i != created.end();
       ++i) {
    ObjectStore::Transaction t;
    t.remove(cid, *i);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ObjectStore::Transaction t;
  t.remove_collection(cid);
  r = queue_transaction(store, ch, std::move(t));
  ASSERT_EQ(r, 0);
}


TEST_P(StoreTest, OMapTest) {
  coll_t cid;
  ghobject_t hoid(hobject_t("tesomap", "", CEPH_NOSNAP, 0, 0, ""));
  auto ch = store->create_new_collection(cid);
  int r;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  map<string, bufferlist> attrs;
  {
    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    t.omap_clear(cid, hoid);
    map<string, bufferlist> start_set;
    t.omap_setkeys(cid, hoid, start_set);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  for (int i = 0; i < 100; i++) {
    if (!(i%5)) {
      std::cout << "On iteration " << i << std::endl;
    }
    ObjectStore::Transaction t;
    bufferlist bl;
    map<string, bufferlist> cur_attrs;
    r = store->omap_get(ch, hoid, &bl, &cur_attrs);
    ASSERT_EQ(r, 0);
    for (map<string, bufferlist>::iterator j = attrs.begin();
	 j != attrs.end();
	 ++j) {
      bool correct = cur_attrs.count(j->first) && string(cur_attrs[j->first].c_str()) == string(j->second.c_str());
      if (!correct) {
	std::cout << j->first << " is present in cur_attrs " << cur_attrs.count(j->first) << " times " << std::endl;
	if (cur_attrs.count(j->first) > 0) {
	  std::cout << j->second.c_str() << " : " << cur_attrs[j->first].c_str() << std::endl;
	}
      }
      ASSERT_EQ(correct, true);
    }
    ASSERT_EQ(attrs.size(), cur_attrs.size());

    char buf[100];
    snprintf(buf, sizeof(buf), "%d", i);
    bl.clear();
    bufferptr bp(buf, strlen(buf) + 1);
    bl.append(bp);
    map<string, bufferlist> to_add;
    to_add.insert(pair<string, bufferlist>("key-" + string(buf), bl));
    attrs.insert(pair<string, bufferlist>("key-" + string(buf), bl));
    t.omap_setkeys(cid, hoid, to_add);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  int i = 0;
  while (attrs.size()) {
    if (!(i%5)) {
      std::cout << "removal: On iteration " << i << std::endl;
    }
    ObjectStore::Transaction t;
    bufferlist bl;
    map<string, bufferlist> cur_attrs;
    r = store->omap_get(ch, hoid, &bl, &cur_attrs);
    ASSERT_EQ(r, 0);
    for (map<string, bufferlist>::iterator j = attrs.begin();
	 j != attrs.end();
	 ++j) {
      bool correct = cur_attrs.count(j->first) && string(cur_attrs[j->first].c_str()) == string(j->second.c_str());
      if (!correct) {
	std::cout << j->first << " is present in cur_attrs " << cur_attrs.count(j->first) << " times " << std::endl;
	if (cur_attrs.count(j->first) > 0) {
	  std::cout << j->second.c_str() << " : " << cur_attrs[j->first].c_str() << std::endl;
	}
      }
      ASSERT_EQ(correct, true);
    }

    string to_remove = attrs.begin()->first;
    set<string> keys_to_remove;
    keys_to_remove.insert(to_remove);
    t.omap_rmkeys(cid, hoid, keys_to_remove);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    attrs.erase(to_remove);

    ++i;
  }

  {
    bufferlist bl1;
    bl1.append("omap_header");
    ObjectStore::Transaction t;
    t.omap_setheader(cid, hoid, bl1);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
    t = ObjectStore::Transaction();
 
    bufferlist bl2;
    bl2.append("value");
    map<string, bufferlist> to_add;
    to_add.insert(pair<string, bufferlist>("key", bl2));
    t.omap_setkeys(cid, hoid, to_add);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    bufferlist bl3;
    map<string, bufferlist> cur_attrs;
    r = store->omap_get(ch, hoid, &bl3, &cur_attrs);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(cur_attrs.size(), size_t(1));
    ASSERT_TRUE(bl_eq(bl1, bl3));
 
    set<string> keys;
    r = store->omap_get_keys(ch, hoid, &keys);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(keys.size(), size_t(1));
  }

  // test omap_clear, omap_rmkey_range
  {
    {
      map<string,bufferlist> to_set;
      for (int n=0; n<10; ++n) {
	to_set[stringify(n)].append("foo");
      }
      bufferlist h;
      h.append("header");
      ObjectStore::Transaction t;
      t.remove(cid, hoid);
      t.touch(cid, hoid);
      t.omap_setheader(cid, hoid, h);
      t.omap_setkeys(cid, hoid, to_set);
      r = queue_transaction(store, ch, std::move(t));
      ASSERT_EQ(r, 0);
    }
    {
      ObjectStore::Transaction t;
      t.omap_rmkeyrange(cid, hoid, "3", "7");
      r = queue_transaction(store, ch, std::move(t));
      ASSERT_EQ(r, 0);
    }
    {
      bufferlist hdr;
      map<string,bufferlist> m;
      store->omap_get(ch, hoid, &hdr, &m);
      ASSERT_EQ(6u, hdr.length());
      ASSERT_TRUE(m.count("2"));
      ASSERT_TRUE(!m.count("3"));
      ASSERT_TRUE(!m.count("6"));
      ASSERT_TRUE(m.count("7"));
      ASSERT_TRUE(m.count("8"));
      //cout << m << std::endl;
      ASSERT_EQ(6u, m.size());
    }
    {
      ObjectStore::Transaction t;
      t.omap_clear(cid, hoid);
      r = queue_transaction(store, ch, std::move(t));
      ASSERT_EQ(r, 0);
    }
    {
      bufferlist hdr;
      map<string,bufferlist> m;
      store->omap_get(ch, hoid, &hdr, &m);
      ASSERT_EQ(0u, hdr.length());
      ASSERT_EQ(0u, m.size());
    }
  }

  ObjectStore::Transaction t;
  t.remove(cid, hoid);
  t.remove_collection(cid);
  r = queue_transaction(store, ch, std::move(t));
  ASSERT_EQ(r, 0);
}

TEST_P(StoreTest, OMapIterator) {
  coll_t cid;
  ghobject_t hoid(hobject_t("tesomap", "", CEPH_NOSNAP, 0, 0, ""));
  int count = 0;
  auto ch = store->create_new_collection(cid);
  int r;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  map<string, bufferlist> attrs;
  {
    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    t.omap_clear(cid, hoid);
    map<string, bufferlist> start_set;
    t.omap_setkeys(cid, hoid, start_set);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ObjectMap::ObjectMapIterator iter;
  bool correct;
  //basic iteration
  for (int i = 0; i < 100; i++) {
    if (!(i%5)) {
      std::cout << "On iteration " << i << std::endl;
    }
    bufferlist bl;

    // FileStore may deadlock two active iterators over the same data
    iter = ObjectMap::ObjectMapIterator();

    iter = store->get_omap_iterator(ch, hoid);
    for (iter->seek_to_first(), count=0; iter->valid(); iter->next(), count++) {
      string key = iter->key();
      bufferlist value = iter->value();
      correct = attrs.count(key) && (string(value.c_str()) == string(attrs[key].c_str()));
      if (!correct) {
	if (attrs.count(key) > 0) {
	  std::cout << "key " << key << "in omap , " << value.c_str() << " : " << attrs[key].c_str() << std::endl;
	}
	else
	  std::cout << "key " << key << "should not exists in omap" << std::endl;
      }
      ASSERT_EQ(correct, true);
    }
    ASSERT_EQ((int)attrs.size(), count);

    // FileStore may deadlock an active iterator vs queue_transaction
    iter = ObjectMap::ObjectMapIterator();

    char buf[100];
    snprintf(buf, sizeof(buf), "%d", i);
    bl.clear();
    bufferptr bp(buf, strlen(buf) + 1);
    bl.append(bp);
    map<string, bufferlist> to_add;
    to_add.insert(pair<string, bufferlist>("key-" + string(buf), bl));
    attrs.insert(pair<string, bufferlist>("key-" + string(buf), bl));
    ObjectStore::Transaction t;
    t.omap_setkeys(cid, hoid, to_add);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  iter = store->get_omap_iterator(ch, hoid);
  //lower bound
  string bound_key = "key-5";
  iter->lower_bound(bound_key);
  correct = bound_key <= iter->key();
  if (!correct) {
    std::cout << "lower bound, bound key is " << bound_key << " < iter key is " << iter->key() << std::endl;
  }
  ASSERT_EQ(correct, true);
  //upper bound
  iter->upper_bound(bound_key);
  correct = iter->key() > bound_key;
  if (!correct) {
    std::cout << "upper bound, bound key is " << bound_key << " >= iter key is " << iter->key() << std::endl;
  }
  ASSERT_EQ(correct, true);

  // FileStore may deadlock an active iterator vs queue_transaction
  iter = ObjectMap::ObjectMapIterator();
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove_collection(cid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, XattrTest) {
  coll_t cid;
  ghobject_t hoid(hobject_t("tesomap", "", CEPH_NOSNAP, 0, 0, ""));
  bufferlist big;
  for (unsigned i = 0; i < 10000; ++i) {
    big.append('\0');
  }
  bufferlist small;
  for (unsigned i = 0; i < 10; ++i) {
    small.append('\0');
  }
  int r;
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    t.touch(cid, hoid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  map<string, bufferlist> attrs;
  {
    ObjectStore::Transaction t;
    t.setattr(cid, hoid, "attr1", small);
    attrs["attr1"] = small;
    t.setattr(cid, hoid, "attr2", big);
    attrs["attr2"] = big;
    t.setattr(cid, hoid, "attr3", small);
    attrs["attr3"] = small;
    t.setattr(cid, hoid, "attr1", small);
    attrs["attr1"] = small;
    t.setattr(cid, hoid, "attr4", big);
    attrs["attr4"] = big;
    t.setattr(cid, hoid, "attr3", big);
    attrs["attr3"] = big;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  map<string, bufferptr> aset;
  store->getattrs(ch, hoid, aset);
  ASSERT_EQ(aset.size(), attrs.size());
  for (map<string, bufferptr>::iterator i = aset.begin();
       i != aset.end();
       ++i) {
    bufferlist bl;
    bl.push_back(i->second);
    ASSERT_TRUE(attrs[i->first] == bl);
  }

  {
    ObjectStore::Transaction t;
    t.rmattr(cid, hoid, "attr2");
    attrs.erase("attr2");
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  aset.clear();
  store->getattrs(ch, hoid, aset);
  ASSERT_EQ(aset.size(), attrs.size());
  for (map<string, bufferptr>::iterator i = aset.begin();
       i != aset.end();
       ++i) {
    bufferlist bl;
    bl.push_back(i->second);
    ASSERT_TRUE(attrs[i->first] == bl);
  }

  bufferptr bp;
  r = store->getattr(ch, hoid, "attr2", bp);
  ASSERT_EQ(r, -ENODATA);

  r = store->getattr(ch, hoid, "attr3", bp);
  ASSERT_EQ(r, 0);
  bufferlist bl2;
  bl2.push_back(bp);
  ASSERT_TRUE(bl2 == attrs["attr3"]);

  ObjectStore::Transaction t;
  t.remove(cid, hoid);
  t.remove_collection(cid);
  r = queue_transaction(store, ch, std::move(t));
  ASSERT_EQ(r, 0);
}

void colsplittest(
  ObjectStore *store,
  unsigned num_objects,
  unsigned common_suffix_size,
  bool clones
  ) {
  coll_t cid(spg_t(pg_t(0,52),shard_id_t::NO_SHARD));
  coll_t tid(spg_t(pg_t(1<<common_suffix_size,52),shard_id_t::NO_SHARD));
  auto ch = store->create_new_collection(cid);
  auto tch = store->create_new_collection(tid);
  int r = 0;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, common_suffix_size);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  bufferlist small;
  small.append("small");
  {
    ObjectStore::Transaction t;
    for (uint32_t i = 0; i < (2 - (int)clones)*num_objects; ++i) {
      stringstream objname;
      objname << "obj" << i;
      ghobject_t a(hobject_t(
		     objname.str(),
		     "",
		     CEPH_NOSNAP,
		     i<<common_suffix_size,
		     52, ""));
      t.write(cid, a, 0, small.length(), small,
	      CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
      if (clones) {
	objname << "-clone";
	ghobject_t b(hobject_t(
		       objname.str(),
		       "",
		       CEPH_NOSNAP,
		       i<<common_suffix_size,
		       52, ""));
	t.clone(cid, a, b);
      }
      if (i % 100) {
	r = queue_transaction(store, ch, std::move(t));
	ASSERT_EQ(r, 0);
	t = ObjectStore::Transaction();
      }
    }
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.create_collection(tid, common_suffix_size + 1);
    t.split_collection(cid, common_suffix_size+1, 1<<common_suffix_size, tid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ch->flush();

  // check
  vector<ghobject_t> objects;
  r = store->collection_list(ch, ghobject_t(), ghobject_t::get_max(),
			     INT_MAX, &objects, 0);
  ASSERT_EQ(r, 0);
  ASSERT_EQ(objects.size(), num_objects);
  for (vector<ghobject_t>::iterator i = objects.begin();
       i != objects.end();
       ++i) {
    ASSERT_EQ(!!(i->hobj.get_hash() & (1<<common_suffix_size)), 0u);
  }

  objects.clear();
  r = store->collection_list(tch, ghobject_t(), ghobject_t::get_max(),
			     INT_MAX, &objects, 0);
  ASSERT_EQ(r, 0);
  ASSERT_EQ(objects.size(), num_objects);
  for (vector<ghobject_t>::iterator i = objects.begin();
       i != objects.end();
       ++i) {
    ASSERT_EQ(!(i->hobj.get_hash() & (1<<common_suffix_size)), 0u);
  }

  // merge them again!
  {
    ObjectStore::Transaction t;
    t.merge_collection(tid, cid, common_suffix_size);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  // check and clean up
  ObjectStore::Transaction t;
  {
    vector<ghobject_t> objects;
    r = store->collection_list(ch, ghobject_t(), ghobject_t::get_max(),
			       INT_MAX, &objects, 0);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(objects.size(), num_objects * 2); // both halves
    unsigned size = 0;
    for (vector<ghobject_t>::iterator i = objects.begin();
	 i != objects.end();
	 ++i) {
      t.remove(cid, *i);
      if (++size > 100) {
	size = 0;
	r = queue_transaction(store, ch, std::move(t));
	ASSERT_EQ(r, 0);
	t = ObjectStore::Transaction();
      }
    }
  }
  t.remove_collection(cid);
  r = queue_transaction(store, ch, std::move(t));
  ASSERT_EQ(r, 0);

  ch->flush();
  ASSERT_TRUE(!store->collection_exists(tid));
}

TEST_P(StoreTest, ColSplitTest0) {
  colsplittest(store.get(), 10, 5, false);
}
TEST_P(StoreTest, ColSplitTest1) {
  colsplittest(store.get(), 10000, 11, false);
}
TEST_P(StoreTest, ColSplitTest1Clones) {
  colsplittest(store.get(), 10000, 11, true);
}
TEST_P(StoreTest, ColSplitTest2) {
  colsplittest(store.get(), 100, 7, false);
}
TEST_P(StoreTest, ColSplitTest2Clones) {
  colsplittest(store.get(), 100, 7, true);
}

#if 0
TEST_P(StoreTest, ColSplitTest3) {
  colsplittest(store.get(), 100000, 25);
}
#endif

void test_merge_skewed(ObjectStore *store,
		       unsigned base, unsigned bits,
		       unsigned anum, unsigned bnum)
{
  cout << __func__ << " 0x" << std::hex << base << std::dec
       << " bits " << bits
       << " anum " << anum << " bnum " << bnum << std::endl;
  /*
    make merge source pgs have radically different # of objects in them,
    which should trigger different splitting in filestore, and verify that
    post-merge all objects are accessible.
    */
  int r;
  coll_t a(spg_t(pg_t(base, 0), shard_id_t::NO_SHARD));
  coll_t b(spg_t(pg_t(base | (1<<bits), 0), shard_id_t::NO_SHARD));

  auto cha = store->create_new_collection(a);
  auto chb = store->create_new_collection(b);
  {
    ObjectStore::Transaction t;
    t.create_collection(a, bits + 1);
    r = queue_transaction(store, cha, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.create_collection(b, bits + 1);
    r = queue_transaction(store, chb, std::move(t));
    ASSERT_EQ(r, 0);
  }

  bufferlist small;
  small.append("small");
  string suffix = "ooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooaaaaaaaaaa";
  set<ghobject_t> aobjects, bobjects;
  {
    // fill a
    ObjectStore::Transaction t;
    for (unsigned i = 0; i < 1000; ++i) {
      string objname = "a" + stringify(i) + suffix;
      ghobject_t o(hobject_t(
		     objname,
		     "",
		     CEPH_NOSNAP,
		     i<<(bits+1) | base,
		     52, ""));
      aobjects.insert(o);
      t.write(a, o, 0, small.length(), small, 0);
      if (i % 100) {
	r = queue_transaction(store, cha, std::move(t));
	ASSERT_EQ(r, 0);
	t = ObjectStore::Transaction();
      }
    }
    r = queue_transaction(store, cha, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    // fill b
    ObjectStore::Transaction t;
    for (unsigned i = 0; i < 10; ++i) {
      string objname = "b" + stringify(i) + suffix;
      ghobject_t o(hobject_t(
		     objname,
		     "",
		     CEPH_NOSNAP,
		     (i<<(base+1)) | base | (1<<bits),
		     52, ""));
      bobjects.insert(o);
      t.write(b, o, 0, small.length(), small, 0);
      if (i % 100) {
	r = queue_transaction(store, chb, std::move(t));
	ASSERT_EQ(r, 0);
	t = ObjectStore::Transaction();
      }
    }
    r = queue_transaction(store, chb, std::move(t));
    ASSERT_EQ(r, 0);
  }

  // merge b->a
  {
    ObjectStore::Transaction t;
    t.merge_collection(b, a, bits);
    r = queue_transaction(store, cha, std::move(t));
    ASSERT_EQ(r, 0);
  }

  // verify
  {
    vector<ghobject_t> got;
    store->collection_list(cha, ghobject_t(), ghobject_t::get_max(), INT_MAX,
			   &got, 0);
    set<ghobject_t> gotset;
    for (auto& o : got) {
      ASSERT_TRUE(aobjects.count(o) || bobjects.count(o));
      gotset.insert(o);
    }
    // check both listing and stat-ability (different code paths!)
    struct stat st;
    for (auto& o : aobjects) {
      ASSERT_TRUE(gotset.count(o));
      int r = store->stat(cha, o, &st, false);
      ASSERT_EQ(r, 0);
    }
    for (auto& o : bobjects) {
      ASSERT_TRUE(gotset.count(o));
      int r = store->stat(cha, o, &st, false);
      ASSERT_EQ(r, 0);
    }
  }

  // clean up
  {
    ObjectStore::Transaction t;
    for (auto &o : aobjects) {
      t.remove(a, o);
    }
    r = queue_transaction(store, cha, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    for (auto &o : bobjects) {
      t.remove(a, o);
    }
    t.remove_collection(a);
    r = queue_transaction(store, cha, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, MergeSkewed) {
  if (string(GetParam()) != "filestore")
    return;

  // this is sufficient to exercise merges with different hashing levels
  test_merge_skewed(store.get(), 0xf, 4, 10, 10000);
  test_merge_skewed(store.get(), 0xf, 4, 10000, 10);

  /*
  // this covers a zillion variations that all boil down to the same thing
  for (unsigned base = 3; base < 0x1000; base *= 5) {
    unsigned bits;
    unsigned t = base;
    for (bits = 0; t; t >>= 1) {
      ++bits;
    }
    for (unsigned b = bits; b < bits + 10; b += 3) {
      for (auto anum : { 10, 1000, 10000 }) {
	for (auto bnum : { 10, 1000, 10000 }) {
	  if (anum == bnum) {
	    continue;
	  }
	  test_merge_skewed(store.get(), base, b, anum, bnum);
	}
      }
    }
  }
  */
}


/**
 * This test tests adding two different groups
 * of objects, each with 1 common prefix and 1
 * different prefix.  We then remove half
 * in order to verify that the merging correctly
 * stops at the common prefix subdir.  See bug
 * #5273 */
TEST_P(StoreTest, TwoHash) {
  coll_t cid;
  int r;
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  std::cout << "Making objects" << std::endl;
  for (int i = 0; i < 360; ++i) {
    ObjectStore::Transaction t;
    ghobject_t o;
    o.hobj.pool = -1;
    if (i < 8) {
      o.hobj.set_hash((i << 16) | 0xA1);
      t.touch(cid, o);
    }
    o.hobj.set_hash((i << 16) | 0xB1);
    t.touch(cid, o);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  std::cout << "Removing half" << std::endl;
  for (int i = 1; i < 8; ++i) {
    ObjectStore::Transaction t;
    ghobject_t o;
    o.hobj.pool = -1;
    o.hobj.set_hash((i << 16) | 0xA1);
    t.remove(cid, o);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  std::cout << "Checking" << std::endl;
  for (int i = 1; i < 8; ++i) {
    ObjectStore::Transaction t;
    ghobject_t o;
    o.hobj.set_hash((i << 16) | 0xA1);
    o.hobj.pool = -1;
    bool exists = store->exists(ch, o);
    ASSERT_EQ(exists, false);
  }
  {
    ghobject_t o;
    o.hobj.set_hash(0xA1);
    o.hobj.pool = -1;
    bool exists = store->exists(ch, o);
    ASSERT_EQ(exists, true);
  }
  std::cout << "Cleanup" << std::endl;
  for (int i = 0; i < 360; ++i) {
    ObjectStore::Transaction t;
    ghobject_t o;
    o.hobj.set_hash((i << 16) | 0xA1);
    o.hobj.pool = -1;
    t.remove(cid, o);
    o.hobj.set_hash((i << 16) | 0xB1);
    t.remove(cid, o);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ObjectStore::Transaction t;
  t.remove_collection(cid);
  r = queue_transaction(store, ch, std::move(t));
  ASSERT_EQ(r, 0);
}

TEST_P(StoreTest, Rename) {
  coll_t cid(spg_t(pg_t(0, 2122),shard_id_t::NO_SHARD));
  ghobject_t srcoid(hobject_t("src_oid", "", CEPH_NOSNAP, 0, 0, ""));
  ghobject_t dstoid(hobject_t("dest_oid", "", CEPH_NOSNAP, 0, 0, ""));
  bufferlist a, b;
  a.append("foo");
  b.append("bar");
  auto ch = store->create_new_collection(cid);
  int r;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    t.write(cid, srcoid, 0, a.length(), a);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ASSERT_TRUE(store->exists(ch, srcoid));
  {
    ObjectStore::Transaction t;
    t.collection_move_rename(cid, srcoid, cid, dstoid);
    t.write(cid, srcoid, 0, b.length(), b);
    t.setattr(cid, srcoid, "attr", b);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ASSERT_TRUE(store->exists(ch, srcoid));
  ASSERT_TRUE(store->exists(ch, dstoid));
  {
    bufferlist bl;
    store->read(ch, srcoid, 0, 3, bl);
    ASSERT_TRUE(bl_eq(b, bl));
    store->read(ch, dstoid, 0, 3, bl);
    ASSERT_TRUE(bl_eq(a, bl));
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, dstoid);
    t.collection_move_rename(cid, srcoid, cid, dstoid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ASSERT_TRUE(store->exists(ch, dstoid));
  ASSERT_FALSE(store->exists(ch, srcoid));
  {
    bufferlist bl;
    store->read(ch, dstoid, 0, 3, bl);
    ASSERT_TRUE(bl_eq(b, bl));
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, dstoid);
    t.remove_collection(cid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, MoveRename) {
  coll_t cid(spg_t(pg_t(0, 212),shard_id_t::NO_SHARD));
  ghobject_t temp_oid(hobject_t("tmp_oid", "", CEPH_NOSNAP, 0, 0, ""));
  ghobject_t oid(hobject_t("dest_oid", "", CEPH_NOSNAP, 0, 0, ""));
  auto ch = store->create_new_collection(cid);
  int r;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    t.touch(cid, oid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ASSERT_TRUE(store->exists(ch, oid));
  bufferlist data, attr;
  map<string, bufferlist> omap;
  data.append("data payload");
  attr.append("attr value");
  omap["omap_key"].append("omap value");
  {
    ObjectStore::Transaction t;
    t.touch(cid, temp_oid);
    t.write(cid, temp_oid, 0, data.length(), data);
    t.setattr(cid, temp_oid, "attr", attr);
    t.omap_setkeys(cid, temp_oid, omap);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ASSERT_TRUE(store->exists(ch, temp_oid));
  {
    ObjectStore::Transaction t;
    t.remove(cid, oid);
    t.collection_move_rename(cid, temp_oid, cid, oid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ASSERT_TRUE(store->exists(ch, oid));
  ASSERT_FALSE(store->exists(ch, temp_oid));
  {
    bufferlist newdata;
    r = store->read(ch, oid, 0, 1000, newdata);
    ASSERT_GE(r, 0);
    ASSERT_TRUE(bl_eq(data, newdata));
    bufferlist newattr;
    r = store->getattr(ch, oid, "attr", newattr);
    ASSERT_EQ(r, 0);
    ASSERT_TRUE(bl_eq(attr, newattr));
    set<string> keys;
    keys.insert("omap_key");
    map<string, bufferlist> newomap;
    r = store->omap_get_values(ch, oid, keys, &newomap);
    ASSERT_GE(r, 0);
    ASSERT_EQ(1u, newomap.size());
    ASSERT_TRUE(newomap.count("omap_key"));
    ASSERT_TRUE(bl_eq(omap["omap_key"], newomap["omap_key"]));
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, oid);
    t.remove_collection(cid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, BigRGWObjectName) {
  coll_t cid(spg_t(pg_t(0,12),shard_id_t::NO_SHARD));
  ghobject_t oid(
    hobject_t(
      "default.4106.50_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "",
      CEPH_NOSNAP,
      0x81920472,
      12,
      ""),
    15,
    shard_id_t::NO_SHARD);
  ghobject_t oid2(oid);
  oid2.generation = 17;
  ghobject_t oidhead(oid);
  oidhead.generation = ghobject_t::NO_GEN;

  auto ch = store->create_new_collection(cid);

  int r;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    t.touch(cid, oidhead);
    t.collection_move_rename(cid, oidhead, cid, oid);
    t.touch(cid, oidhead);
    t.collection_move_rename(cid, oidhead, cid, oid2);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  {
    ObjectStore::Transaction t;
    t.remove(cid, oid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  {
    vector<ghobject_t> objects;
    r = store->collection_list(ch, ghobject_t(), ghobject_t::get_max(),
			       INT_MAX, &objects, 0);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(objects.size(), 1u);
    ASSERT_EQ(objects[0], oid2);
  }

  ASSERT_FALSE(store->exists(ch, oid));

  {
    ObjectStore::Transaction t;
    t.remove(cid, oid2);
    t.remove_collection(cid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

  }
}

TEST_P(StoreTest, SetAllocHint) {
  coll_t cid;
  ghobject_t hoid(hobject_t("test_hint", "", CEPH_NOSNAP, 0, 0, ""));
  auto ch = store->create_new_collection(cid);
  int r;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    t.touch(cid, hoid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.set_alloc_hint(cid, hoid, 4*1024*1024, 1024*4, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.set_alloc_hint(cid, hoid, 4*1024*1024, 1024*4, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove_collection(cid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, TryMoveRename) {
  coll_t cid;
  ghobject_t hoid(hobject_t("test_hint", "", CEPH_NOSNAP, 0, -1, ""));
  ghobject_t hoid2(hobject_t("test_hint2", "", CEPH_NOSNAP, 0, -1, ""));
  auto ch = store->create_new_collection(cid);
  int r;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.try_rename(cid, hoid, hoid2);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.try_rename(cid, hoid, hoid2);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  struct stat st;
  ASSERT_EQ(store->stat(ch, hoid, &st), -ENOENT);
  ASSERT_EQ(store->stat(ch, hoid2, &st), 0);
}

#if defined(WITH_BLUESTORE)
TEST_P(StoreTest, BluestoreOnOffCSumTest) {
  if (string(GetParam()) != "bluestore")
    return;
  SetVal(g_conf(), "bluestore_csum_type", "crc32c");
  g_conf().apply_changes(nullptr);

  int r;
  coll_t cid;
  ghobject_t hoid(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  {
    auto ch = store->open_collection(cid);
    ASSERT_FALSE(ch);
  }
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    //write with csum enabled followed by read with csum disabled
    size_t block_size = 64*1024;
    ObjectStore::Transaction t;
    bufferlist bl, orig;
    bl.append(std::string(block_size, 'a'));
    orig = bl;
    t.remove(cid, hoid);
    t.set_alloc_hint(cid, hoid, 4*1024*1024, 1024*8, 0);
    t.write(cid, hoid, 0, bl.length(), bl);
    cerr << "Remove then create" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    SetVal(g_conf(), "bluestore_csum_type", "none");
    g_conf().apply_changes(nullptr);

    bufferlist in;
    r = store->read(ch, hoid, 0, block_size, in);
    ASSERT_EQ((int)block_size, r);
    ASSERT_TRUE(bl_eq(orig, in));

  }
  {
    //write with csum disabled followed by read with csum enabled

    size_t block_size = 64*1024;
    ObjectStore::Transaction t;
    bufferlist bl, orig;
    bl.append(std::string(block_size, 'a'));
    orig = bl;
    t.remove(cid, hoid);
    t.set_alloc_hint(cid, hoid, 4*1024*1024, 1024*8, 0);
    t.write(cid, hoid, 0, bl.length(), bl);
    cerr << "Remove then create" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    SetVal(g_conf(), "bluestore_csum_type", "crc32c");
    g_conf().apply_changes(nullptr);

    bufferlist in;
    r = store->read(ch, hoid, 0, block_size, in);
    ASSERT_EQ((int)block_size, r);
    ASSERT_TRUE(bl_eq(orig, in));
  }
  {
    //'mixed' non-overlapping writes to the same blob 

    ObjectStore::Transaction t;
    bufferlist bl, orig;
    size_t block_size = 8000;
    bl.append(std::string(block_size, 'a'));
    orig = bl;
    t.remove(cid, hoid);
    t.write(cid, hoid, 0, bl.length(), bl);
    cerr << "Remove then create" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    SetVal(g_conf(), "bluestore_csum_type", "none");
    g_conf().apply_changes(nullptr);

    ObjectStore::Transaction t2;
    t2.write(cid, hoid, block_size*2, bl.length(), bl);
    cerr << "Append 'unprotected'" << std::endl;
    r = queue_transaction(store, ch, std::move(t2));
    ASSERT_EQ(r, 0);

    bufferlist in;
    r = store->read(ch, hoid, 0, block_size, in);
    ASSERT_EQ((int)block_size, r);
    ASSERT_TRUE(bl_eq(orig, in));
    in.clear();
    r = store->read(ch, hoid, block_size*2, block_size, in);
    ASSERT_EQ((int)block_size, r);
    ASSERT_TRUE(bl_eq(orig, in));

    SetVal(g_conf(), "bluestore_csum_type", "crc32c");
    g_conf().apply_changes(nullptr);
    in.clear();
    r = store->read(ch, hoid, 0, block_size, in);
    ASSERT_EQ((int)block_size, r);
    ASSERT_TRUE(bl_eq(orig, in));
    in.clear();
    r = store->read(ch, hoid, block_size*2, block_size, in);
    ASSERT_EQ((int)block_size, r);
    ASSERT_TRUE(bl_eq(orig, in));
  }
  {
    //partially blob overwrite under a different csum enablement mode

    ObjectStore::Transaction t;
    bufferlist bl, orig, orig2;
    size_t block_size0 = 0x10000;
    size_t block_size = 9000;
    size_t block_size2 = 5000;
    bl.append(std::string(block_size0, 'a'));
    t.remove(cid, hoid);
    t.set_alloc_hint(cid, hoid, 4*1024*1024, 1024*8, 0);
    t.write(cid, hoid, 0, bl.length(), bl);
    cerr << "Remove then create" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    SetVal(g_conf(), "bluestore_csum_type", "none");
    g_conf().apply_changes(nullptr);

    ObjectStore::Transaction t2;
    bl.clear();
    bl.append(std::string(block_size, 'b'));
    t2.write(cid, hoid, 0, bl.length(), bl);
    t2.write(cid, hoid, block_size0, bl.length(), bl);
    cerr << "Overwrite with unprotected data" << std::endl;
    r = queue_transaction(store, ch, std::move(t2));
    ASSERT_EQ(r, 0);

    orig = bl;
    orig2 = bl;
    orig.append( std::string(block_size0 - block_size, 'a'));

    bufferlist in;
    r = store->read(ch, hoid, 0, block_size0, in);
    ASSERT_EQ((int)block_size0, r);
    ASSERT_TRUE(bl_eq(orig, in));

    r = store->read(ch, hoid, block_size0, block_size, in);
    ASSERT_EQ((int)block_size, r);
    ASSERT_TRUE(bl_eq(orig2, in));

    SetVal(g_conf(), "bluestore_csum_type", "crc32c");
    g_conf().apply_changes(nullptr);

    ObjectStore::Transaction t3;
    bl.clear();
    bl.append(std::string(block_size2, 'c'));
    t3.write(cid, hoid, block_size0, bl.length(), bl);
    cerr << "Overwrite with protected data" << std::endl;
    r = queue_transaction(store, ch, std::move(t3));
    ASSERT_EQ(r, 0);

    in.clear();
    orig = bl;
    orig.append( std::string(block_size - block_size2, 'b'));
    r = store->read(ch, hoid, block_size0, block_size, in);
    ASSERT_EQ((int)block_size, r);
    ASSERT_TRUE(bl_eq(orig, in));
  }

  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}
#endif

INSTANTIATE_TEST_CASE_P(
  ObjectStore,
  StoreTest,
  ::testing::Values(
    "memstore",
    "filestore",
#if defined(WITH_BLUESTORE)
    "bluestore",
#endif
    "kstore"));

// Note: instantiate all stores to preserve store numbering order only
INSTANTIATE_TEST_CASE_P(
  ObjectStore,
  StoreTestSpecificAUSize,
  ::testing::Values(
    "memstore",
    "filestore",
#if defined(WITH_BLUESTORE)
    "bluestore",
#endif
    "kstore"));

#else

// Google Test may not support value-parameterized tests with some
// compilers. If we use conditional compilation to compile out all
// code referring to the gtest_main library, MSVC linker will not link
// that library at all and consequently complain about missing entry
// point defined in that library (fatal error LNK1561: entry point
// must be defined). This dummy test keeps gtest_main linked in.
TEST(DummyTest, ValueParameterizedTestsAreNotSupportedOnThisPlatform) {}

#endif

void doMany4KWritesTest(boost::scoped_ptr<ObjectStore>& store,
                        unsigned max_objects,
                        unsigned max_ops,
                        unsigned max_object_size,
                        unsigned max_write_size,
                        unsigned write_alignment)
{
  MixedGenerator gen(555);
  gen_type rng(time(NULL));
  coll_t cid(spg_t(pg_t(0,555), shard_id_t::NO_SHARD));
  store_statfs_t res_stat;

  SyntheticWorkloadState test_obj(store.get(),
                                  &gen,
                                  &rng,
                                  cid,
                                  max_object_size,
                                  max_write_size,
                                  write_alignment);
  test_obj.init();
  for (unsigned i = 0; i < max_objects; ++i) {
    if (!(i % 500)) cerr << "seeding object " << i << std::endl;
    test_obj.touch();
  }
  for (unsigned i = 0; i < max_ops; ++i) {
    if (!(i % 200)) {
      cerr << "Op " << i << std::endl;
      test_obj.print_internal_state();
    }
    test_obj.write();
  }
  test_obj.wait_for_done();
  test_obj.statfs(res_stat);
  if (!(res_stat.data_stored <= max_object_size) ||
      !(res_stat.allocated <= max_object_size)) {
    // this will provide more insight on the mismatch and
    // helps to avoid any races during stats collection
    test_obj.fsck(false);
    // retrieving stats once again and assert if still broken
    test_obj.statfs(res_stat);
    ASSERT_LE(res_stat.data_stored, max_object_size);
    ASSERT_LE(res_stat.allocated, max_object_size);
  }
  test_obj.shutdown();
}

TEST_P(StoreTestSpecificAUSize, Many4KWritesTest) {
  if (string(GetParam()) != "bluestore")
    return;

  StartDeferred(0x10000);

  const unsigned max_object = 4*1024*1024;
  doMany4KWritesTest(store, 1, 1000, max_object, 4*1024, 0);
}

TEST_P(StoreTestSpecificAUSize, Many4KWritesNoCSumTest) {
  if (string(GetParam()) != "bluestore")
    return;
  StartDeferred(0x10000);
  SetVal(g_conf(), "bluestore_csum_type", "none");
  g_ceph_context->_conf.apply_changes(nullptr);
  const unsigned max_object = 4*1024*1024;

  doMany4KWritesTest(store, 1, 1000, max_object, 4*1024, 0 );
}

TEST_P(StoreTestSpecificAUSize, TooManyBlobsTest) {
  if (string(GetParam()) != "bluestore")
    return;
  StartDeferred(0x10000);
  const unsigned max_object = 4*1024*1024;
  doMany4KWritesTest(store, 1, 1000, max_object, 4*1024, 0);
}

#if defined(WITH_BLUESTORE)
void get_mempool_stats(uint64_t* total_bytes, uint64_t* total_items)
{
  uint64_t onode_allocated = mempool::bluestore_cache_onode::allocated_bytes();
  uint64_t other_allocated = mempool::bluestore_cache_other::allocated_bytes();

  uint64_t onode_items = mempool::bluestore_cache_onode::allocated_items();
  uint64_t other_items = mempool::bluestore_cache_other::allocated_items();
  cout << "onode(" << onode_allocated << "/" << onode_items
       << ") other(" << other_allocated << "/" << other_items
       << ")" << std::endl;
  *total_bytes = onode_allocated + other_allocated;
  *total_items = onode_items;
}

TEST_P(StoreTestSpecificAUSize, OnodeSizeTracking) {

  if (string(GetParam()) != "bluestore")
    return;

  size_t block_size = 4096;
  StartDeferred(block_size);
  SetVal(g_conf(), "bluestore_compression_mode", "none");
  SetVal(g_conf(), "bluestore_csum_type", "none");
  SetVal(g_conf(), "bluestore_cache_size_hdd", "400000000");
  SetVal(g_conf(), "bluestore_cache_size_ssd", "400000000");
  g_conf().apply_changes(nullptr);

  int r;
  coll_t cid;
  ghobject_t hoid(hobject_t("test_hint", "", CEPH_NOSNAP, 0, -1, ""));
  size_t obj_size = 4 * 1024  * 1024;
  uint64_t total_bytes, total_bytes2;
  uint64_t total_onodes;
  get_mempool_stats(&total_bytes, &total_onodes);
  ASSERT_EQ(total_onodes, 0u);

  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    bufferlist bl, orig, orig2;
    
    bl.append(std::string(obj_size, 'a'));
    t.write(cid, hoid, 0, bl.length(), bl);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  get_mempool_stats(&total_bytes, &total_onodes);
  ASSERT_NE(total_bytes, 0u);
  ASSERT_EQ(total_onodes, 1u);

  {
    ObjectStore::Transaction t;
    t.truncate(cid, hoid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
 
  for(size_t i = 0; i < 1; ++i) {
    bufferlist bl;
    bl.append(std::string(block_size * (i+1), 'a'));
    for( size_t j = 0; j < obj_size; j+= bl.length()) {
      ObjectStore::Transaction t;
      t.write(cid, hoid, j, bl.length(), bl);
      r = queue_transaction(store, ch, std::move(t));
      ASSERT_EQ(r, 0);
    }
    get_mempool_stats(&total_bytes2, &total_onodes);
    ASSERT_NE(total_bytes2, 0u);
    ASSERT_EQ(total_onodes, 1u);
  }
  {
    cout <<" mempool dump:\n";
    JSONFormatter f(true);
    f.open_object_section("transaction");
    mempool::dump(&f);
    f.close_section();
    f.flush(cout);
    cout << std::endl;
  }
  {
    bufferlist bl;
    for (size_t i = 0; i < obj_size; i += 0x1000) {
      store->read(ch, hoid, i, 0x1000, bl);
    }
  }
  get_mempool_stats(&total_bytes, &total_onodes);
  ASSERT_NE(total_bytes, 0u);
  ASSERT_EQ(total_onodes, 1u);

  {
    cout <<" mempool dump:\n";
    JSONFormatter f(true);
    f.open_object_section("transaction");
    mempool::dump(&f);
    f.close_section();
    f.flush(cout);
    cout << std::endl;
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTestSpecificAUSize, BlobReuseOnOverwrite) {

  if (string(GetParam()) != "bluestore")
    return;

  size_t block_size = 4096;
  StartDeferred(block_size);
  SetVal(g_conf(), "bluestore_max_blob_size", "65536");
  g_conf().apply_changes(nullptr);

  int r;
  coll_t cid;
  ghobject_t hoid(hobject_t("test_hint", "", CEPH_NOSNAP, 0, -1, ""));

  const PerfCounters* logger = store->get_perf_counters();

  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    bufferlist bl;

    bl.append(std::string(block_size * 2, 'a'));
    t.write(cid, hoid, 0, bl.length(), bl, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    // overwrite at the beginning
    ObjectStore::Transaction t;
    bufferlist bl;

    bl.append(std::string(block_size, 'b'));
    t.write(cid, hoid, 0, bl.length(), bl, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    // append
    ObjectStore::Transaction t;
    bufferlist bl;

    bl.append(std::string(block_size * 2, 'c'));
    t.write(cid, hoid, block_size * 2, bl.length(), bl, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    // append with a gap
    ObjectStore::Transaction t;
    bufferlist bl;

    bl.append(std::string(block_size * 2, 'd'));
    t.write(cid, hoid, block_size * 5, bl.length(), bl, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    // We need to issue a read to trigger cache stat update that refresh
    // perf counters. additionally we need to wait some time for mempool
    // thread to update stats.
    sleep(1);
    bufferlist bl, expected;
    r = store->read(ch, hoid, 0, block_size, bl);
    ASSERT_EQ(r, (int)block_size);
    expected.append(string(block_size, 'b'));
    ASSERT_TRUE(bl_eq(expected, bl));
    ASSERT_EQ(logger->get(l_bluestore_blobs), 1u);
    ASSERT_EQ(logger->get(l_bluestore_extents), 2u);
  }
  {
    // overwrite at end
    ObjectStore::Transaction t;
    bufferlist bl;

    bl.append(std::string(block_size * 2, 'e'));

    // Currently we are unable to reuse blob when overwriting in a single step
    t.write(cid, hoid, block_size * 6, bl.length(), bl, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    // We need to issue a read to trigger cache stat update that refresh
    // perf counters. additionally we need to wait some time for mempool
    // thread to update stats.
    sleep(1);
    bufferlist bl, expected;
    r = store->read(ch, hoid, 0, block_size, bl);
    ASSERT_EQ(r, (int)block_size);
    expected.append(string(block_size, 'b'));
    ASSERT_TRUE(bl_eq(expected, bl));
    ASSERT_EQ(logger->get(l_bluestore_blobs), 1u);
    ASSERT_EQ(logger->get(l_bluestore_extents), 2u);
  }
  {
    // fill the gap
    ObjectStore::Transaction t;
    bufferlist bl;

    bl.append(std::string(block_size, 'f'));

    t.write(cid, hoid, block_size * 4, bl.length(), bl, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    // we need to wait some time for mempool
    // thread to update stats to be able to check blob/extent numbers from
    // perf counters.
    sleep(1);

    bufferlist bl, expected;
    r = store->read(ch, hoid, 0, block_size, bl);
    ASSERT_EQ(r, (int)block_size);
    expected.append(string(block_size, 'b'));
    ASSERT_TRUE(bl_eq(expected, bl));

    bl.clear();
    expected.clear();
    r = store->read(ch, hoid, block_size, block_size, bl);
    ASSERT_EQ(r, (int)block_size);
    expected.append(string(block_size, 'a'));
    ASSERT_TRUE(bl_eq(expected, bl));

    bl.clear();
    expected.clear();
    r = store->read(ch, hoid, block_size * 2, block_size * 2, bl);
    ASSERT_EQ(r, (int)block_size * 2);
    expected.append(string(block_size * 2, 'c'));
    ASSERT_TRUE(bl_eq(expected, bl));

    bl.clear();
    expected.clear();
    r = store->read(ch, hoid, block_size * 4, block_size, bl);
    ASSERT_EQ(r, (int)block_size);
    expected.append(string(block_size, 'f'));
    ASSERT_TRUE(bl_eq(expected, bl));

    bl.clear();
    expected.clear();
    r = store->read(ch, hoid, block_size * 5, block_size, bl);
    ASSERT_EQ(r, (int)block_size);
    expected.append(string(block_size, 'd'));
    ASSERT_TRUE(bl_eq(expected, bl));

    bl.clear();
    expected.clear();
    r = store->read(ch, hoid, block_size * 5, block_size * 3, bl);
    ASSERT_EQ(r, (int)block_size * 3);
    expected.append(string(block_size, 'd'));
    expected.append(string(block_size * 2, 'e'));
    ASSERT_TRUE(bl_eq(expected, bl));
  }
  ASSERT_EQ(logger->get(l_bluestore_blobs), 1u);
  ASSERT_EQ(logger->get(l_bluestore_extents), 1u);


  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTestSpecificAUSize, BlobReuseOnOverwriteReverse) {

  if (string(GetParam()) != "bluestore")
    return;

  size_t block_size = 4096;
  StartDeferred(block_size);
  SetVal(g_conf(), "bluestore_max_blob_size", "65536");
  g_conf().apply_changes(nullptr);

  int r;
  coll_t cid;
  ghobject_t hoid(hobject_t("test_hint", "", CEPH_NOSNAP, 0, -1, ""));

  auto ch = store->create_new_collection(cid);

  const PerfCounters* logger = store->get_perf_counters();
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    bufferlist bl;

    bl.append(std::string(block_size * 2, 'a'));
    t.write(cid, hoid, block_size * 10, bl.length(), bl, 
            CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    // prepend existing
    ObjectStore::Transaction t;
    bufferlist bl;

    bl.append(std::string(block_size, 'b'));
    t.write(cid, hoid, block_size * 9, bl.length(), bl,
            CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    // We need to issue a read to trigger cache stat update that refresh
    // perf counters. additionally we need to wait some time for mempool
    // thread to update stats.
    sleep(1);
    bufferlist bl, expected;
    r = store->read(ch, hoid, block_size * 9, block_size * 2, bl);
    ASSERT_EQ(r, (int)block_size * 2);
    expected.append(string(block_size, 'b'));
    expected.append(string(block_size, 'a'));
    ASSERT_TRUE(bl_eq(expected, bl));
    ASSERT_EQ(logger->get(l_bluestore_blobs), 1u);
    ASSERT_EQ(logger->get(l_bluestore_extents), 1u);
  }


  {
    // prepend existing with a gap
    ObjectStore::Transaction t;
    bufferlist bl;

    bl.append(std::string(block_size, 'c'));
    t.write(cid, hoid, block_size * 7, bl.length(), bl,
            CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    // We need to issue a read to trigger cache stat update that refresh
    // perf counters. additionally we need to wait some time for mempool
    // thread to update stats.
    sleep(1);
    bufferlist bl, expected;
    r = store->read(ch, hoid, block_size * 7, block_size * 3, bl);
    ASSERT_EQ(r, (int)block_size * 3);
    expected.append(string(block_size, 'c'));
    expected.append(string(block_size, 0));
    expected.append(string(block_size, 'b'));
    ASSERT_TRUE(bl_eq(expected, bl));
    ASSERT_EQ(logger->get(l_bluestore_blobs), 1u);
    ASSERT_EQ(logger->get(l_bluestore_extents), 2u);
  }

  {
    // append after existing with a gap
    ObjectStore::Transaction t;
    bufferlist bl;

    bl.append(std::string(block_size, 'd'));
    t.write(cid, hoid, block_size * 13, bl.length(), bl,
            CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    // We need to issue a read to trigger cache stat update that refresh
    // perf counters. additionally we need to wait some time for mempool
    // thread to update stats.
    sleep(1);
    bufferlist bl, expected;
    r = store->read(ch, hoid, block_size * 11, block_size * 3, bl);
    ASSERT_EQ(r, (int)block_size * 3);
    expected.append(string(block_size, 'a'));
    expected.append(string(block_size, 0));
    expected.append(string(block_size, 'd'));
    ASSERT_TRUE(bl_eq(expected, bl));
    ASSERT_EQ(logger->get(l_bluestore_blobs), 1u);
    ASSERT_EQ(logger->get(l_bluestore_extents), 3u);
  }

  {
    // append twice to the next max_blob slot
    ObjectStore::Transaction t;
    bufferlist bl;

    bl.append(std::string(block_size, 'e'));
    t.write(cid, hoid, block_size * 17, bl.length(), bl,
            CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
    t.write(cid, hoid, block_size * 19, bl.length(), bl,
            CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    // We need to issue a read to trigger cache stat update that refresh
    // perf counters. additionally we need to wait some time for mempool
    // thread to update stats.
    sleep(1);
    bufferlist bl, expected;
    r = store->read(ch, hoid, block_size * 17, block_size * 3, bl);
    ASSERT_EQ(r, (int)block_size * 3);
    expected.append(string(block_size, 'e'));
    expected.append(string(block_size, 0));
    expected.append(string(block_size, 'e'));
    ASSERT_TRUE(bl_eq(expected, bl));
    ASSERT_EQ(logger->get(l_bluestore_blobs), 2u);
    ASSERT_EQ(logger->get(l_bluestore_extents), 5u);
  }
  {
    // fill gaps at the second slot
    ObjectStore::Transaction t;
    bufferlist bl;

    bl.append(std::string(block_size, 'f'));
    t.write(cid, hoid, block_size * 16, bl.length(), bl,
            CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
    t.write(cid, hoid, block_size * 18, bl.length(), bl,
            CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    // We need to issue a read to trigger cache stat update that refresh
    // perf counters. additionally we need to wait some time for mempool
    // thread to update stats.
    sleep(1);
    bufferlist bl, expected;
    r = store->read(ch, hoid, block_size * 16, block_size * 4, bl);
    ASSERT_EQ(r, (int)block_size * 4);
    expected.append(string(block_size, 'f'));
    expected.append(string(block_size, 'e'));
    expected.append(string(block_size, 'f'));
    expected.append(string(block_size, 'e'));
    ASSERT_TRUE(bl_eq(expected, bl));
    ASSERT_EQ(logger->get(l_bluestore_blobs), 2u);
    ASSERT_EQ(logger->get(l_bluestore_extents), 4u);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTestSpecificAUSize, BlobReuseOnSmallOverwrite) {

  if (string(GetParam()) != "bluestore")
    return;

  size_t block_size = 4096;
  StartDeferred(block_size);
  SetVal(g_conf(), "bluestore_max_blob_size", "65536");
  g_conf().apply_changes(nullptr);

  int r;
  coll_t cid;
  ghobject_t hoid(hobject_t("test_hint", "", CEPH_NOSNAP, 0, -1, ""));

  const PerfCounters* logger = store->get_perf_counters();
  auto ch = store->create_new_collection(cid);

  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    bufferlist bl;

    bl.append(std::string(block_size, 'a'));
    t.write(cid, hoid, 0, bl.length(), bl, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
    t.write(cid, hoid, block_size * 2, bl.length(), bl,
      CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    // write small into the gap
    ObjectStore::Transaction t;
    bufferlist bl;

    bl.append(std::string(3, 'b'));
    t.write(cid, hoid, block_size + 1, bl.length(), bl,
      CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    // We need to issue a read to trigger cache stat update that refresh
    // perf counters. additionally we need to wait some time for mempool
    // thread to update stats.
    sleep(1);
    bufferlist bl, expected;
    r = store->read(ch, hoid, 0, block_size * 3, bl);
    ASSERT_EQ(r, (int)block_size * 3);
    expected.append(string(block_size, 'a'));
    expected.append(string(1, 0));
    expected.append(string(3, 'b'));
    expected.append(string(block_size - 4, 0));
    expected.append(string(block_size, 'a'));
    ASSERT_TRUE(bl_eq(expected, bl));

    ASSERT_EQ(logger->get(l_bluestore_blobs), 1u);
    ASSERT_EQ(logger->get(l_bluestore_extents), 3u);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

// The test case to reproduce an issue when write happens
// to a zero space between the extents sharing the same spanning blob
// with unloaded shard map.
// Second extent might be filled with zeros this way due to wrong result
// returned by has_any_extents() call in do_write_small. The latter is caused
// by incompletly loaded extent map.
TEST_P(StoreTestSpecificAUSize, SmallWriteOnShardedExtents) {
  if (string(GetParam()) != "bluestore")
    return;

  size_t block_size = 0x10000;
  StartDeferred(block_size);

  SetVal(g_conf(), "bluestore_csum_type", "xxhash64");
  SetVal(g_conf(), "bluestore_max_blob_size", "524288"); // for sure

  g_conf().apply_changes(nullptr);

  int r;
  coll_t cid;
  ghobject_t hoid1(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  auto ch = store->create_new_collection(cid);

  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
   //doing some tricks to have sharded extents/spanning objects
    ObjectStore::Transaction t;
    bufferlist bl, bl2;

    bl.append(std::string(0x80000, 'a'));
    t.write(cid, hoid1, 0, bl.length(), bl, 0);
    t.zero(cid, hoid1, 0x719e0, 0x75b0 );
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    bl2.append(std::string(0x70000, 'b'));
    t.write(cid, hoid1, 0, bl2.length(), bl2, 0);
    t.zero(cid, hoid1, 0, 0x50000);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

  }
  ch.reset();
  store->umount();
  store->mount();
  ch = store->open_collection(cid);

  {
    // do a write to zero space in between some extents sharing the same blob
    ObjectStore::Transaction t;
    bufferlist bl, bl2;

    bl.append(std::string(0x6520, 'c'));
    t.write(cid, hoid1, 0x71c00, bl.length(), bl, 0);

    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  {
    ObjectStore::Transaction t;
    bufferlist bl, expected;

    r = store->read(ch, hoid1, 0x70000, 0x9c00, bl);
    ASSERT_EQ(r, (int)0x9c00);
    expected.append(string(0x19e0, 'a'));
    expected.append(string(0x220, 0));
    expected.append(string(0x6520, 'c'));
    expected.append(string(0xe70, 0));
    expected.append(string(0xc70, 'a'));
    ASSERT_TRUE(bl_eq(expected, bl));
    bl.clear();

  }

  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid1);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTestSpecificAUSize, ExcessiveFragmentation) {
  if (string(GetParam()) != "bluestore")
    return;

  SetVal(g_conf(), "bluestore_block_size",
    stringify((uint64_t)2048 * 1024 * 1024).c_str());

  ASSERT_EQ(g_conf().get_val<Option::size_t>("bluefs_alloc_size"),
	    1024 * 1024U);

  size_t block_size = 0x10000;
  StartDeferred(block_size);

  int r;
  coll_t cid;
  ghobject_t hoid1(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  ghobject_t hoid2(hobject_t(sobject_t("Object 2", CEPH_NOSNAP)));
  auto ch = store->create_new_collection(cid);

  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    // create 2x400MB objects in a way that their pextents are interleaved
    ObjectStore::Transaction t;
    bufferlist bl;

    bl.append(std::string(block_size * 4, 'a')); // 256KB
    uint64_t offs = 0;
    while(offs < (uint64_t)400 * 1024 * 1024) {
      t.write(cid, hoid1, offs, bl.length(), bl, 0);
      t.write(cid, hoid2, offs, bl.length(), bl, 0);
      r = queue_transaction(store, ch, std::move(t));
      ASSERT_EQ(r, 0);
      offs += bl.length();
      if( (offs % (100 * 1024 * 1024)) == 0) {
       std::cout<<"written " << offs << std::endl;
      }
    }
  }
  std::cout<<"written 800MB"<<std::endl;
  {
    // Partially overwrite objects with 100MB each leaving space
    // fragmented and occuping still unfragmented space at the end
    // So we'll have enough free space but it'll lack long enough (e.g. 1MB)
    // contiguous pextents.
    ObjectStore::Transaction t;
    bufferlist bl;

    bl.append(std::string(block_size * 4, 'a'));
    uint64_t offs = 0;
    while(offs < 112 * 1024 * 1024) {
      t.write(cid, hoid1, offs, bl.length(), bl, 0);
      t.write(cid, hoid2, offs, bl.length(), bl, 0);
      r = queue_transaction(store, ch, std::move(t));
      ASSERT_EQ(r, 0);
      // this will produce high fragmentation if original allocations
      // were contiguous
      offs += bl.length();
      if( (offs % (10 * 1024 * 1024)) == 0) {
       std::cout<<"written " << offs << std::endl;
      }
    }
  }
  {
    // remove one of the object producing much free space
    // and hence triggering bluefs rebalance.
    // Which should fail as there is no long enough pextents.
    ObjectStore::Transaction t;
    t.remove(cid, hoid2);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  auto to_sleep = 5 *
    (int)g_conf().get_val<double>("bluestore_bluefs_balance_interval");
  std::cout<<"sleeping... " << std::endl;
  sleep(to_sleep);

  {
    // touch another object to triggerrebalance
    ObjectStore::Transaction t;
    t.touch(cid, hoid1);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid1);
    t.remove(cid, hoid2);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

#endif //#if defined(WITH_BLUESTORE)

TEST_P(StoreTest, KVDBHistogramTest) {
  if (string(GetParam()) != "bluestore")
    return;

  int NUM_OBJS = 200;
  int r = 0;
  coll_t cid;
  string base("testobj.");
  bufferlist a;
  bufferptr ap(0x1000);
  memset(ap.c_str(), 'a', 0x1000);
  a.append(ap);
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  for (int i = 0; i < NUM_OBJS; ++i) {
    ObjectStore::Transaction t;
    char buf[100];
    snprintf(buf, sizeof(buf), "%d", i);
    ghobject_t hoid(hobject_t(sobject_t(base + string(buf), CEPH_NOSNAP)));
    t.write(cid, hoid, 0, 0x1000, a);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  Formatter *f = Formatter::create("store_test", "json-pretty", "json-pretty");
  store->generate_db_histogram(f);
  f->flush(cout);
  cout << std::endl;
}

TEST_P(StoreTest, KVDBStatsTest) {
  if (string(GetParam()) != "bluestore")
    return;

  SetVal(g_conf(), "rocksdb_perf", "true");
  SetVal(g_conf(), "rocksdb_collect_compaction_stats", "true");
  SetVal(g_conf(), "rocksdb_collect_extended_stats","true");
  SetVal(g_conf(), "rocksdb_collect_memory_stats","true");
  g_ceph_context->_conf.apply_changes(nullptr);
  int r = store->umount();
  ASSERT_EQ(r, 0);
  r = store->mount(); //to force rocksdb stats
  ASSERT_EQ(r, 0);

  int NUM_OBJS = 200;
  coll_t cid;
  string base("testobj.");
  bufferlist a;
  bufferptr ap(0x1000);
  memset(ap.c_str(), 'a', 0x1000);
  a.append(ap);
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  for (int i = 0; i < NUM_OBJS; ++i) {
    ObjectStore::Transaction t;
    char buf[100];
    snprintf(buf, sizeof(buf), "%d", i);
    ghobject_t hoid(hobject_t(sobject_t(base + string(buf), CEPH_NOSNAP)));
    t.write(cid, hoid, 0, 0x1000, a);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  Formatter *f = Formatter::create("store_test", "json-pretty", "json-pretty");
  store->get_db_statistics(f);
  f->flush(cout);
  cout << std::endl;
}

#if defined(WITH_BLUESTORE)
TEST_P(StoreTestSpecificAUSize, garbageCollection) {
  int r;
  coll_t cid;
  int buf_len = 256 * 1024;
  int overlap_offset = 64 * 1024;
  int write_offset = buf_len;
  if (string(GetParam()) != "bluestore")
    return;

#define WRITE_AT(offset, _length) {\
      ObjectStore::Transaction t;\
      if ((uint64_t)_length != bl.length()) { \
        buffer::ptr p(bl.c_str(), _length);\
        bufferlist bl_tmp;\
        bl_tmp.push_back(p);\
        t.write(cid, hoid, offset, bl_tmp.length(), bl_tmp);\
      } else {\
        t.write(cid, hoid, offset, bl.length(), bl);\
      }\
      r = queue_transaction(store, ch, std::move(t));\
      ASSERT_EQ(r, 0);\
  }

  StartDeferred(65536);

  SetVal(g_conf(), "bluestore_compression_max_blob_size", "524288");
  SetVal(g_conf(), "bluestore_compression_min_blob_size", "262144");
  SetVal(g_conf(), "bluestore_max_blob_size", "524288");
  SetVal(g_conf(), "bluestore_compression_mode", "force");
  g_conf().apply_changes(nullptr);

  auto ch = store->create_new_collection(cid);

  ghobject_t hoid(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  {
    bufferlist in;
    r = store->read(ch, hoid, 0, 5, in);
    ASSERT_EQ(-ENOENT, r);
  }
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  std::string data;
  data.resize(buf_len);

  {
    { 
      bool exists = store->exists(ch, hoid);
      ASSERT_TRUE(!exists);

      ObjectStore::Transaction t;
      t.touch(cid, hoid);
      cerr << "Creating object " << hoid << std::endl;
      r = queue_transaction(store, ch, std::move(t));
      ASSERT_EQ(r, 0);

      exists = store->exists(ch, hoid);
      ASSERT_EQ(true, exists);
    } 
    bufferlist bl;

    for(size_t i = 0; i < data.size(); i++)
      data[i] = i % 256;

    bl.append(data);

    {
      struct store_statfs_t statfs;
      WRITE_AT(0, buf_len);
      int r = store->statfs(&statfs);
      ASSERT_EQ(r, 0);
      ASSERT_EQ(statfs.data_compressed_allocated, 0x10000);
    }
    {
      struct store_statfs_t statfs;
      WRITE_AT(write_offset - 2 * overlap_offset, buf_len);
      int r = store->statfs(&statfs);
      ASSERT_EQ(r, 0);
      ASSERT_EQ(statfs.data_compressed_allocated, 0x20000);
      const PerfCounters* counters = store->get_perf_counters();
      ASSERT_EQ(counters->get(l_bluestore_gc_merged), 0u);
    }

    {
      struct store_statfs_t statfs;
      WRITE_AT(write_offset - overlap_offset, buf_len);
      int r = store->statfs(&statfs);
      ASSERT_EQ(r, 0);
      ASSERT_EQ(statfs.data_compressed_allocated, 0x20000);
      const PerfCounters* counters = store->get_perf_counters();
      ASSERT_EQ(counters->get(l_bluestore_gc_merged), 0x10000u);
    }
    {
      struct store_statfs_t statfs;
      WRITE_AT(write_offset - 3 * overlap_offset, buf_len);
      int r = store->statfs(&statfs);
      ASSERT_EQ(r, 0);
      ASSERT_EQ(statfs.data_compressed_allocated, 0x20000);
      const PerfCounters* counters = store->get_perf_counters();
      ASSERT_EQ(counters->get(l_bluestore_gc_merged), 0x20000u);
    }
    {
      struct store_statfs_t statfs;
      WRITE_AT(write_offset + 1, overlap_offset-1);
      int r = store->statfs(&statfs);
      ASSERT_EQ(r, 0);
      ASSERT_EQ(statfs.data_compressed_allocated, 0x20000);
      const PerfCounters* counters = store->get_perf_counters();
      ASSERT_EQ(counters->get(l_bluestore_gc_merged), 0x20000u);
    }
    {
      struct store_statfs_t statfs;
      WRITE_AT(write_offset + 1, overlap_offset);
      int r = store->statfs(&statfs);
      ASSERT_EQ(r, 0);
      ASSERT_EQ(statfs.data_compressed_allocated, 0x10000);
      const PerfCounters* counters = store->get_perf_counters();
      ASSERT_EQ(counters->get(l_bluestore_gc_merged), 0x3ffffu);
    }
    {
      struct store_statfs_t statfs;
      WRITE_AT(0, buf_len-1);
      int r = store->statfs(&statfs);
      ASSERT_EQ(r, 0);
      ASSERT_EQ(statfs.data_compressed_allocated, 0x10000);
      const PerfCounters* counters = store->get_perf_counters();
      ASSERT_EQ(counters->get(l_bluestore_gc_merged), 0x40001u);
    }
    SetVal(g_conf(), "bluestore_gc_enable_total_threshold", "1"); //forbid GC when saving = 0
    {
      struct store_statfs_t statfs;
      WRITE_AT(1, overlap_offset-2);
      WRITE_AT(overlap_offset * 2 + 1, overlap_offset-2);
      int r = store->statfs(&statfs);
      ASSERT_EQ(r, 0);
      ASSERT_EQ(statfs.data_compressed_allocated, 0x10000);
      const PerfCounters* counters = store->get_perf_counters();
      ASSERT_EQ(counters->get(l_bluestore_gc_merged), 0x40001u);
    }
    {
      struct store_statfs_t statfs;
      WRITE_AT(overlap_offset + 1, overlap_offset-2);
      int r = store->statfs(&statfs);
      ASSERT_EQ(r, 0);
      ASSERT_EQ(statfs.data_compressed_allocated, 0x0);
      const PerfCounters* counters = store->get_perf_counters();
      ASSERT_EQ(counters->get(l_bluestore_gc_merged), 0x40007u);
    }
    {
      ObjectStore::Transaction t;
      t.remove(cid, hoid);
      cerr << "Cleaning" << std::endl;
      r = queue_transaction(store, ch, std::move(t));
      ASSERT_EQ(r, 0);
    }
  }
}

TEST_P(StoreTestSpecificAUSize, fsckOnUnalignedDevice) {
  if (string(GetParam()) != "bluestore")
    return;

  SetVal(g_conf(), "bluestore_block_size",
    stringify(0x280005000).c_str()); //10 Gb + 4K
  SetVal(g_conf(), "bluestore_fsck_on_mount", "false");
  SetVal(g_conf(), "bluestore_fsck_on_umount", "false");
  StartDeferred(0x4000);
  store->umount();
  ASSERT_EQ(store->fsck(false), 0); // do fsck explicitly
  store->mount();

}

TEST_P(StoreTestSpecificAUSize, fsckOnUnalignedDevice2) {
  if (string(GetParam()) != "bluestore")
    return;

  SetVal(g_conf(), "bluestore_block_size",
    stringify(0x280005000).c_str()); //10 Gb + 20K
  SetVal(g_conf(), "bluestore_fsck_on_mount", "false");
  SetVal(g_conf(), "bluestore_fsck_on_umount", "false");
  StartDeferred(0x1000);
  store->umount();
  ASSERT_EQ(store->fsck(false), 0); // do fsck explicitly
  store->mount();
}

namespace {
  ghobject_t make_object(const char* name, int64_t pool) {
    sobject_t soid{name, CEPH_NOSNAP};
    uint32_t hash = std::hash<sobject_t>{}(soid);
    return ghobject_t{hobject_t{soid, "", hash, pool, ""}};
  }
}

TEST_P(StoreTestSpecificAUSize, BluestoreRepairTest) {
  if (string(GetParam()) != "bluestore")
    return;
  const size_t offs_base = 65536 / 2;

  SetVal(g_conf(), "bluestore_fsck_on_mount", "false");
  SetVal(g_conf(), "bluestore_fsck_on_umount", "false");
  SetVal(g_conf(), "bluestore_max_blob_size", 
    stringify(2 * offs_base).c_str());
  SetVal(g_conf(), "bluestore_extent_map_shard_max_size", "12000");
  SetVal(g_conf(), "bluestore_no_per_pool_stats_tolerance", "enforce");

  StartDeferred(0x10000);

  BlueStore* bstore = dynamic_cast<BlueStore*> (store.get());

  // fill the store with some data
  const uint64_t pool = 555;
  coll_t cid(spg_t(pg_t(0, pool), shard_id_t::NO_SHARD));
  auto ch = store->create_new_collection(cid);

  ghobject_t hoid = make_object("Object 1", pool);
  ghobject_t hoid_dup = make_object("Object 1(dup)", pool);
  ghobject_t hoid2 = make_object("Object 2", pool);
  ghobject_t hoid_cloned = hoid2;
  hoid_cloned.hobj.snap = 1;
  ghobject_t hoid3 = make_object("Object 3", pool);
  ghobject_t hoid3_cloned = hoid3;
  hoid3_cloned.hobj.snap = 1;
  bufferlist bl;
  bl.append("1234512345");
  int r;
  const size_t repeats = 16;
  {
    auto ch = store->create_new_collection(cid);
    cerr << "create collection + write" << std::endl;
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    for( auto i = 0ul; i < repeats; ++i ) {
      t.write(cid, hoid, i * offs_base, bl.length(), bl);
      t.write(cid, hoid_dup, i * offs_base, bl.length(), bl);
    }
    for( auto i = 0ul; i < repeats; ++i ) {
      t.write(cid, hoid2, i * offs_base, bl.length(), bl);
    }
    t.clone(cid, hoid2, hoid_cloned);

    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  bstore->umount();
  //////////// leaked pextent fix ////////////
  cerr << "fix leaked pextents" << std::endl;
  ASSERT_EQ(bstore->fsck(false), 0);
  ASSERT_EQ(bstore->repair(false), 0);
  bstore->mount();
  bstore->inject_leaked(0x30000);
  bstore->umount();
  ASSERT_EQ(bstore->fsck(false), 1);
  ASSERT_EQ(bstore->repair(false), 0);
  ASSERT_EQ(bstore->fsck(false), 0);

  //////////// false free fix ////////////
  cerr << "fix false free pextents" << std::endl;
  bstore->mount();
  bstore->inject_false_free(cid, hoid);
  bstore->umount();
  ASSERT_EQ(bstore->fsck(false), 2);
  ASSERT_EQ(bstore->repair(false), 0);
  ASSERT_EQ(bstore->fsck(false), 0);

  //////////// verify invalid statfs ///////////
  cerr << "fix invalid statfs" << std::endl;
  store_statfs_t statfs0, statfs;
  bstore->mount();
  ASSERT_EQ(bstore->statfs(&statfs0), 0);
  statfs = statfs0;
  statfs.allocated += 0x10000;
  statfs.data_stored += 0x10000;
  ASSERT_FALSE(statfs0 == statfs);
  bstore->inject_statfs("bluestore_statfs", statfs);
  bstore->umount();

  ASSERT_EQ(bstore->fsck(false), 1);
  ASSERT_EQ(bstore->repair(false), 0);
  ASSERT_EQ(bstore->fsck(false), 0);
  ASSERT_EQ(bstore->mount(), 0);
  ASSERT_EQ(bstore->statfs(&statfs), 0);
  // adjust free space to success in comparison
  statfs0.available = statfs.available;
  ASSERT_EQ(statfs0, statfs);

  ///////// undecodable shared blob key / stray shared blob records ///////
  cerr << "undecodable shared blob key" << std::endl;
  bstore->inject_broken_shared_blob_key("undec1",
			    bufferlist());
  bstore->inject_broken_shared_blob_key("undecodable key 2",
			    bufferlist());
  bstore->inject_broken_shared_blob_key("undecodable key 3",
			    bufferlist());
  bstore->umount();
  ASSERT_EQ(bstore->fsck(false), 3);
  ASSERT_EQ(bstore->repair(false), 0);
  ASSERT_EQ(bstore->fsck(false), 0);

  cerr << "misreferencing" << std::endl;
  bstore->mount();
  bstore->inject_misreference(cid, hoid, cid, hoid_dup, 0);
  bstore->inject_misreference(cid, hoid, cid, hoid_dup, (offs_base * repeats) / 2);
  bstore->inject_misreference(cid, hoid, cid, hoid_dup, offs_base * (repeats -1) );
  
  bstore->umount();
  ASSERT_EQ(bstore->fsck(false), 6);
  ASSERT_EQ(bstore->repair(false), 0);

  ASSERT_EQ(bstore->fsck(true), 0);

  // reproducing issues #21040 & 20983
  SetVal(g_conf(), "bluestore_debug_inject_bug21040", "true");
  g_ceph_context->_conf.apply_changes(nullptr);
  bstore->mount();

  cerr << "repro bug #21040" << std::endl;
  {
    auto ch = store->open_collection(cid);
    {
      ObjectStore::Transaction t;
      bl.append("0123456789012345");
      t.write(cid, hoid3, offs_base, bl.length(), bl);
      bl.clear();
      bl.append('!');
      t.write(cid, hoid3, 0, bl.length(), bl);

      r = queue_transaction(store, ch, std::move(t));
      ASSERT_EQ(r, 0);
    }
    {
      ObjectStore::Transaction t;
      t.clone(cid, hoid3, hoid3_cloned);
      r = queue_transaction(store, ch, std::move(t));
      ASSERT_EQ(r, 0);
    }

    bstore->umount();
    ASSERT_EQ(bstore->fsck(false), 3);
    ASSERT_LE(bstore->repair(false), 0);
    ASSERT_EQ(bstore->fsck(false), 0);
  }

  // enable per-pool stats collection hence causing fsck to fail
  cerr << "per-pool statfs" << std::endl;
  SetVal(g_conf(), "bluestore_no_per_pool_stats_tolerance", "until_fsck");
  g_ceph_context->_conf.apply_changes(nullptr);

  ASSERT_EQ(bstore->fsck(false), 2);
  ASSERT_EQ(bstore->repair(false), 0);
  ASSERT_EQ(bstore->fsck(false), 0);

  cerr << "Completing" << std::endl;
  bstore->mount();

}

TEST_P(StoreTest, BluestoreStatistics) {
  if (string(GetParam()) != "bluestore")
    return;

  SetVal(g_conf(), "rocksdb_perf", "true");
  SetVal(g_conf(), "rocksdb_collect_compaction_stats", "true");
  SetVal(g_conf(), "rocksdb_collect_extended_stats","true");
  SetVal(g_conf(), "rocksdb_collect_memory_stats","true");

  // disable cache
  SetVal(g_conf(), "bluestore_cache_size_ssd", "0");
  SetVal(g_conf(), "bluestore_cache_size_hdd", "0");
  SetVal(g_conf(), "bluestore_cache_size", "0");
  g_ceph_context->_conf.apply_changes(nullptr);

  int r = store->umount();
  ASSERT_EQ(r, 0);
  r = store->mount();
  ASSERT_EQ(r, 0);

  BlueStore* bstore = NULL;
  EXPECT_NO_THROW(bstore = dynamic_cast<BlueStore*> (store.get()));

  coll_t cid;
  ghobject_t hoid(hobject_t("test_db_statistics", "", CEPH_NOSNAP, 0, 0, ""));
  auto ch = bstore->create_new_collection(cid);
  bufferlist bl;
  bl.append("0123456789abcdefghi");
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    t.touch(cid, hoid);
    t.write(cid, hoid, 0, bl.length(), bl);
    cerr << "Write object" << std::endl;
    r = queue_transaction(bstore, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    bufferlist readback;
    r = store->read(ch, hoid, 0, bl.length(), readback);
    ASSERT_EQ(static_cast<int>(bl.length()), r);
    ASSERT_TRUE(bl_eq(bl, readback));
  }
  Formatter *f = Formatter::create("store_test", "json-pretty", "json-pretty");
  EXPECT_NO_THROW(store->get_db_statistics(f));
  f->flush(cout);
  cout << std::endl;
}

TEST_P(StoreTestSpecificAUSize, BluestoreTinyDevFailure) {
  if (string(GetParam()) != "bluestore")
    return;
  // This caused superblock overwrite by bluefs, see
  // https://tracker.ceph.com/issues/24480
  SetVal(g_conf(), "bluestore_block_size",
    stringify(1024 * 1024 * 1024).c_str()); //1 Gb
  SetVal(g_conf(), "bluestore_block_db_size", "0");
  SetVal(g_conf(), "bluestore_block_db_create", "false");
  SetVal(g_conf(), "bluestore_bluefs_min",
    stringify(1024 * 1024 * 1024).c_str());
  StartDeferred(0x1000);
  store->umount();
  ASSERT_EQ(store->fsck(false), 0); // do fsck explicitly
  store->mount();
}

TEST_P(StoreTestSpecificAUSize, BluestoreTinyDevFailure2) {
  if (string(GetParam()) != "bluestore")
    return;

  // This caused assert in allocator as initial bluefs extent as slow device
  // overlaped with superblock
  // https://tracker.ceph.com/issues/24480
  SetVal(g_conf(), "bluestore_block_size",
    stringify(1024 * 1024 * 1024).c_str()); //1 Gb
  SetVal(g_conf(), "bluestore_block_db_size",
    stringify(1024 * 1024 * 1024).c_str()); //1 Gb
  SetVal(g_conf(), "bluestore_block_db_create", "true");
  SetVal(g_conf(), "bluestore_bluefs_min",
    stringify(1024 * 1024 * 1024).c_str());
  StartDeferred(0x1000);
  store->umount();
  ASSERT_EQ(store->fsck(false), 0); // do fsck explicitly
  store->mount();
}

TEST_P(StoreTest, SpuriousReadErrorTest) {
  if (string(GetParam()) != "bluestore")
    return;

  int r;
  auto logger = store->get_perf_counters();
  coll_t cid;
  auto ch = store->create_new_collection(cid);
  ghobject_t hoid(hobject_t(sobject_t("foo", CEPH_NOSNAP)));
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  bufferlist test_data;
  bufferptr ap(0x2000);
  memset(ap.c_str(), 'a', 0x2000);
  test_data.append(ap);
  {
    ObjectStore::Transaction t;
    t.write(cid, hoid, 0, 0x2000, test_data);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
    // force cache clear
    EXPECT_EQ(store->umount(), 0);
    EXPECT_EQ(store->mount(), 0);
  }

  cerr << "Injecting CRC error with no retry, expecting EIO" << std::endl;
  SetVal(g_conf(), "bluestore_retry_disk_reads", "0");
  SetVal(g_conf(), "bluestore_debug_inject_csum_err_probability", "1");
  g_ceph_context->_conf.apply_changes(nullptr);
  {
    bufferlist in;
    r = store->read(ch, hoid, 0, 0x2000, in, CEPH_OSD_OP_FLAG_FADVISE_NOCACHE);
    ASSERT_EQ(-EIO, r);
    ASSERT_EQ(logger->get(l_bluestore_read_eio), 1u);
    ASSERT_EQ(logger->get(l_bluestore_reads_with_retries), 0u);
  }

  cerr << "Injecting CRC error with retries, expecting success after several retries" << std::endl;
  SetVal(g_conf(), "bluestore_retry_disk_reads", "255");
  SetVal(g_conf(), "bluestore_debug_inject_csum_err_probability", "0.8");
  /**
   * Probabilistic test: 25 reads, each has a 80% chance of failing with 255 retries
   * Probability of at least one retried read: 1 - (0.2 ** 25) = 100% - 3e-18
   * Probability of a random test failure: 1 - ((1 - (0.8 ** 255)) ** 25) ~= 5e-24
   */
  g_ceph_context->_conf.apply_changes(nullptr);
  {
    for (int i = 0; i < 25; ++i) {
      bufferlist in;
      r = store->read(ch, hoid, 0, 0x2000, in, CEPH_OSD_OP_FLAG_FADVISE_NOCACHE);
      ASSERT_EQ(0x2000, r);
      ASSERT_TRUE(bl_eq(test_data, in));
    }
    ASSERT_GE(logger->get(l_bluestore_reads_with_retries), 1u);
  }
}

TEST_P(StoreTest, allocateBlueFSTest) {
  if (string(GetParam()) != "bluestore")
    return;

  BlueStore* bstore = NULL;
  EXPECT_NO_THROW(bstore = dynamic_cast<BlueStore*> (store.get()));

  struct store_statfs_t statfs;
  store->statfs(&statfs);

  uint64_t to_alloc = g_conf().get_val<Option::size_t>("bluefs_alloc_size");

  int r = bstore->allocate_bluefs_freespace(to_alloc, to_alloc, nullptr);
  ASSERT_EQ(r, 0);
  r = bstore->allocate_bluefs_freespace(statfs.total, statfs.total, nullptr);
  ASSERT_EQ(r, -ENOSPC);
  r = bstore->allocate_bluefs_freespace(to_alloc * 16, to_alloc * 16, nullptr);
  ASSERT_EQ(r, 0);
  store->umount();
  ASSERT_EQ(store->fsck(false), 0); // do fsck explicitly
  r = store->mount();
  ASSERT_EQ(r, 0);
}

TEST_P(StoreTest, mergeRegionTest) {
  if (string(GetParam()) != "bluestore")
    return;

  SetVal(g_conf(), "bluestore_fsck_on_mount", "true");
  SetVal(g_conf(), "bluestore_fsck_on_umount", "true");
  SetVal(g_conf(), "bdev_debug_inflight_ios", "true");
  g_ceph_context->_conf.apply_changes(nullptr);

  uint32_t chunk_size = g_ceph_context->_conf->bdev_block_size; 
  int r = -1;
  coll_t cid;
  ghobject_t hoid(hobject_t(sobject_t("Object", CEPH_NOSNAP)));
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    cerr << "Creating object " << hoid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  bufferlist bl5;
  bl5.append("abcde");
  uint64_t offset = 0;
  { // 1. same region
    ObjectStore::Transaction t;
    t.write(cid, hoid, offset, 5, bl5);
    t.write(cid, hoid, 0xa + offset, 5, bl5);
    t.write(cid, hoid, 0x14 + offset, 5, bl5);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  { // 2. adjacent regions
    ObjectStore::Transaction t;
    offset = chunk_size;
    t.write(cid, hoid, offset, 5, bl5);
    t.write(cid, hoid, offset + chunk_size + 3, 5, bl5);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  { // 3. front merge
    ObjectStore::Transaction t;
    offset = chunk_size * 2;
    t.write(cid, hoid, offset, 5, bl5);
    t.write(cid, hoid, offset + chunk_size - 2, 5, bl5);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  { // 4. back merge
    ObjectStore::Transaction t;
    bufferlist blc2;
    blc2.append_zero(chunk_size + 2);

    offset = chunk_size * 3;
    t.write(cid, hoid, offset, chunk_size + 2, blc2);
    t.write(cid, hoid, offset + chunk_size + 3, 5, bl5);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  { // 5. overlapping
    ObjectStore::Transaction t;
    uint64_t final_len = 0;
    offset = chunk_size * 10;
    bufferlist bl2c2;
    bl2c2.append_zero(chunk_size * 2);
    t.write(cid, hoid, offset + chunk_size * 3 - 3, chunk_size * 2, bl2c2);
    bl2c2.append_zero(2);
    t.write(cid, hoid, offset + chunk_size - 2, chunk_size * 2 + 2, bl2c2);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    final_len = (offset + chunk_size * 3 - 3) + (chunk_size * 2);
    bufferlist bl;
    r = store->read(ch, hoid, 0, final_len, bl);
    ASSERT_EQ(final_len, static_cast<uint64_t>(r));
  }
}
#endif  // WITH_BLUESTORE

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  // make sure we can adjust any config settings
  g_ceph_context->_conf._clear_safe_to_start_threads();

  g_ceph_context->_conf.set_val_or_die("osd_journal_size", "400");
  g_ceph_context->_conf.set_val_or_die("filestore_index_retry_probability", "0.5");
  g_ceph_context->_conf.set_val_or_die("filestore_op_thread_timeout", "1000");
  g_ceph_context->_conf.set_val_or_die("filestore_op_thread_suicide_timeout", "10000");
  //g_ceph_context->_conf.set_val_or_die("filestore_fiemap", "true");
  g_ceph_context->_conf.set_val_or_die("bluestore_fsck_on_mkfs", "false");
  g_ceph_context->_conf.set_val_or_die("bluestore_fsck_on_mount", "false");
  g_ceph_context->_conf.set_val_or_die("bluestore_fsck_on_umount", "false");
  g_ceph_context->_conf.set_val_or_die("bluestore_debug_misc", "true");
  g_ceph_context->_conf.set_val_or_die("bluestore_debug_small_allocations", "4");
  g_ceph_context->_conf.set_val_or_die("bluestore_debug_freelist", "true");
  g_ceph_context->_conf.set_val_or_die("bluestore_clone_cow", "true");
  g_ceph_context->_conf.set_val_or_die("bluestore_max_alloc_size", "196608");

  // set small cache sizes so we see trimming during Synthetic tests
  g_ceph_context->_conf.set_val_or_die("bluestore_cache_size_hdd", "4000000");
  g_ceph_context->_conf.set_val_or_die("bluestore_cache_size_ssd", "4000000");

  // very short *_max prealloc so that we fall back to async submits
  g_ceph_context->_conf.set_val_or_die("bluestore_blobid_prealloc", "10");
  g_ceph_context->_conf.set_val_or_die("bluestore_nid_prealloc", "10");
  g_ceph_context->_conf.set_val_or_die("bluestore_debug_randomize_serial_transaction",
				 "10");

  g_ceph_context->_conf.set_val_or_die("bdev_debug_aio", "true");

  // specify device size
  g_ceph_context->_conf.set_val_or_die("bluestore_block_size",
    stringify(DEF_STORE_TEST_BLOCKDEV_SIZE));

  g_ceph_context->_conf.set_val_or_die(
    "enable_experimental_unrecoverable_data_corrupting_features", "*");
  g_ceph_context->_conf.apply_changes(nullptr);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; make ceph_test_objectstore && 
 *    ./ceph_test_objectstore \
 *        --gtest_filter=*.collect_metadata* --log-to-stderr=true --debug-filestore=20
 *  "
 * End:
 */
