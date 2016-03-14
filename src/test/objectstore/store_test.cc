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
#include "os/ObjectStore.h"
#include "os/filestore/FileStore.h"
#include "include/Context.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/errno.h"
#include "include/stringify.h"
#include <boost/scoped_ptr.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <boost/random/binomial_distribution.hpp>
#include <gtest/gtest.h>

#include "include/unordered_map.h"
typedef boost::mt11213b gen_type;

#if GTEST_HAS_PARAM_TEST

class StoreTest : public ::testing::TestWithParam<const char*> {
public:
  boost::scoped_ptr<ObjectStore> store;

  StoreTest() : store(0) {}

  void rm_r(string path) {
    string cmd = string("rm -r ") + path;
    cout << "==> " << cmd << std::endl;
    int r = ::system(cmd.c_str());
    if (r) {
      cerr << "failed with exit code " << r
	   << ", continuing anyway" << std::endl;
    }
  }

  virtual void SetUp() {
    int r = ::mkdir("store_test_temp_dir", 0777);
    if (r < 0) {
      r = -errno;
      cerr << __func__ << ": unable to create store_test_temp_dir" << ": " << cpp_strerror(r) << std::endl;
      return;
    }

    ObjectStore *store_ = ObjectStore::create(g_ceph_context,
                                              string(GetParam()),
                                              string("store_test_temp_dir"),
                                              string("store_test_temp_journal"));
    if (!store_) {
      cerr << __func__ << ": objectstore type " << string(GetParam()) << " doesn't exist yet!" << std::endl;
      return;
    }
    EXPECT_EQ(store_->mkfs(), 0);
    EXPECT_EQ(store_->mount(), 0);
    store.reset(store_);
  }

  virtual void TearDown() {
    if (store) {
      store->umount();
      rm_r("store_test_temp_dir");
    }
  }
};

bool sorted(const vector<ghobject_t> &in, bool bitwise) {
  ghobject_t start;
  for (vector<ghobject_t>::const_iterator i = in.begin();
       i != in.end();
       ++i) {
    if (cmp(start, *i, bitwise) > 0) {
      cout << start << " should follow " << *i << std::endl;
      return false;
    }
    start = *i;
  }
  return true;
}

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
  store->umount();
  int r = store->mount();
  ASSERT_EQ(0, r);
}

TEST_P(StoreTest, SimpleRemount) {
  ObjectStore::Sequencer osr("test");
  coll_t cid;
  ghobject_t hoid(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  ghobject_t hoid2(hobject_t(sobject_t("Object 2", CEPH_NOSNAP)));
  bufferlist bl;
  bl.append("1234512345");
  int r;
  {
    cerr << "create collection + write" << std::endl;
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    t.write(cid, hoid, 0, bl.length(), bl);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  store->umount();
  r = store->mount();
  ASSERT_EQ(0, r);
  {
    ObjectStore::Transaction t;
    t.write(cid, hoid2, 0, bl.length(), bl);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove(cid, hoid2);
    t.remove_collection(cid);
    cerr << "remove collection" << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  store->umount();
  r = store->mount();
  ASSERT_EQ(0, r);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
    bool exists = store->exists(cid, hoid);
    ASSERT_TRUE(!exists);
  }
  {
    ObjectStore::Transaction t;
    t.remove_collection(cid);
    cerr << "remove collection" << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, IORemount) {
  ObjectStore::Sequencer osr("test");
  coll_t cid;
  bufferlist bl;
  bl.append("1234512345");
  int r;
  {
    cerr << "create collection + objects" << std::endl;
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    for (int n=1; n<=100; ++n) {
      ghobject_t hoid(hobject_t(sobject_t("Object " + stringify(n), CEPH_NOSNAP)));
      t.write(cid, hoid, 0, bl.length(), bl);
    }
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  // overwrites
  {
    cout << "overwrites" << std::endl;
    for (int n=1; n<=100; ++n) {
      ObjectStore::Transaction t;
      ghobject_t hoid(hobject_t(sobject_t("Object " + stringify(n), CEPH_NOSNAP)));
      t.write(cid, hoid, 1, bl.length(), bl);
      r = store->apply_transaction(&osr, std::move(t));
      ASSERT_EQ(r, 0);
    }
  }
  store->umount();
  r = store->mount();
  ASSERT_EQ(0, r);
  {
    ObjectStore::Transaction t;
    for (int n=1; n<=100; ++n) {
      ghobject_t hoid(hobject_t(sobject_t("Object " + stringify(n), CEPH_NOSNAP)));
      t.remove(cid, hoid);
    }
    t.remove_collection(cid);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, FiemapEmpty) {
  ObjectStore::Sequencer osr("test");
  coll_t cid;
  int r = 0;
  ghobject_t oid(hobject_t(sobject_t("fiemap_object", CEPH_NOSNAP)));
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    t.touch(cid, oid);
    t.truncate(cid, oid, 100000);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    bufferlist bl;
    store->fiemap(cid, oid, 0, 100000, bl);
    map<uint64_t,uint64_t> m, e;
    bufferlist::iterator p = bl.begin();
    ::decode(m, p);
    cout << " got " << m << std::endl;
    e[0] = 100000;
    EXPECT_TRUE(m == e || m.empty());
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, oid);
    t.remove_collection(cid);
    cerr << "remove collection" << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, FiemapHoles) {
  ObjectStore::Sequencer osr("test");
  coll_t cid;
  int r = 0;
  ghobject_t oid(hobject_t(sobject_t("fiemap_object", CEPH_NOSNAP)));
  bufferlist bl;
  bl.append("foo");
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    t.touch(cid, oid);
    t.write(cid, oid, 0, 3, bl);
    t.write(cid, oid, 1048576, 3, bl);
    t.write(cid, oid, 4194304, 3, bl);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    bufferlist bl;
    store->fiemap(cid, oid, 0, 4194307, bl);
    map<uint64_t,uint64_t> m, e;
    bufferlist::iterator p = bl.begin();
    ::decode(m, p);
    cout << " got " << m << std::endl;
    ASSERT_TRUE(!m.empty());
    ASSERT_GE(m[0], 3u);
    ASSERT_TRUE((m.size() == 1 &&
		 m[0] > 4194304u) ||
		(m.size() == 3 &&
		 m.count(0) &&
		 m.count(1048576) &&
		 m.count(4194304)));
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, oid);
    t.remove_collection(cid);
    cerr << "remove collection" << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, SimpleMetaColTest) {
  ObjectStore::Sequencer osr("test");
  coll_t cid;
  int r = 0;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "create collection" << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove_collection(cid);
    cerr << "remove collection" << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "add collection" << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove_collection(cid);
    cerr << "remove collection" << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, SimplePGColTest) {
  ObjectStore::Sequencer osr("test");
  coll_t cid(spg_t(pg_t(1,2), shard_id_t::NO_SHARD));
  int r = 0;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 4);
    cerr << "create collection" << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove_collection(cid);
    cerr << "remove collection" << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 4);
    cerr << "add collection" << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove_collection(cid);
    cerr << "remove collection" << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, SimpleColPreHashTest) {
  ObjectStore::Sequencer osr("test");
  // Firstly we will need to revert the value making sure
  // collection hint actually works
  int merge_threshold = g_ceph_context->_conf->filestore_merge_threshold;
  std::ostringstream oss;
  if (merge_threshold > 0) {
    oss << "-" << merge_threshold;
    g_ceph_context->_conf->set_val("filestore_merge_threshold", oss.str().c_str());
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
  {
    // Create a collection along with a hint
    ObjectStore::Transaction t;
    t.create_collection(cid, 5);
    cerr << "create collection" << std::endl;
    bufferlist hint;
    ::encode(pg_num, hint);
    ::encode(expected_num_objs, hint);
    t.collection_hint(cid, ObjectStore::Transaction::COLL_HINT_EXPECTED_NUM_OBJECTS, hint);
    cerr << "collection hint" << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    // Remove the collection
    ObjectStore::Transaction t;
    t.remove_collection(cid);
    cerr << "remove collection" << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  // Revert the config change so that it does not affect the split/merge tests
  if (merge_threshold > 0) {
    oss.str("");
    oss << merge_threshold;
    g_ceph_context->_conf->set_val("filestore_merge_threshold", oss.str().c_str());
  }
}

TEST_P(StoreTest, SimpleObjectTest) {
  ObjectStore::Sequencer osr("test");
  int r;
  coll_t cid;
  ghobject_t hoid(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  {
    bufferlist in;
    r = store->read(cid, hoid, 0, 5, in);
    ASSERT_EQ(-ENOENT, r);
  }
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    bool exists = store->exists(cid, hoid);
    ASSERT_TRUE(!exists);

    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    cerr << "Creating object " << hoid << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);

    exists = store->exists(cid, hoid);
    ASSERT_EQ(true, exists);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.touch(cid, hoid);
    cerr << "Remove then create" << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
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
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);

    bufferlist in;
    r = store->read(cid, hoid, 0, 5, in);
    ASSERT_EQ(5, r);
    ASSERT_TRUE(in.contents_equal(orig));
  }
  {
    ObjectStore::Transaction t;
    bufferlist bl, exp;
    bl.append("abcde");
    exp = bl;
    exp.append(bl);
    t.write(cid, hoid, 5, 5, bl);
    cerr << "Append" << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);

    bufferlist in;
    r = store->read(cid, hoid, 0, 10, in);
    ASSERT_EQ(10, r);
    ASSERT_TRUE(in.contents_equal(exp));
  }
  {
    ObjectStore::Transaction t;
    bufferlist bl, exp;
    bl.append("abcdeabcde");
    exp = bl;
    t.write(cid, hoid, 0, 10, bl);
    cerr << "Full overwrite" << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);

    bufferlist in;
    r = store->read(cid, hoid, 0, 10, in);
    ASSERT_EQ(10, r);
    ASSERT_TRUE(in.contents_equal(exp));
  }
  {
    ObjectStore::Transaction t;
    bufferlist bl;
    bl.append("abcde");
    t.write(cid, hoid, 3, 5, bl);
    cerr << "Partial overwrite" << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);

    bufferlist in, exp;
    exp.append("abcabcdede");
    r = store->read(cid, hoid, 0, 10, in);
    ASSERT_EQ(10, r);
    in.hexdump(cout);
    ASSERT_TRUE(in.contents_equal(exp));
  }
  {
    ObjectStore::Transaction t;
    bufferlist bl;
    bl.append("abcde01234012340123401234abcde01234012340123401234abcde01234012340123401234abcde01234012340123401234");
    t.write(cid, hoid, 0, bl.length(), bl);
    cerr << "larger overwrite" << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);

    bufferlist in;
    r = store->read(cid, hoid, 0, bl.length(), in);
    ASSERT_EQ((int)bl.length(), r);
    in.hexdump(cout);
    ASSERT_TRUE(in.contents_equal(bl));
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, ManySmallWrite) {
  ObjectStore::Sequencer osr("test");
  int r;
  coll_t cid;
  ghobject_t a(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  ghobject_t b(hobject_t(sobject_t("Object 2", CEPH_NOSNAP)));
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  bufferlist bl;
  bufferptr bp(4096);
  bp.zero();
  bl.append(bp);
  for (int i=0; i<100; ++i) {
    ObjectStore::Transaction t;
    t.write(cid, a, i*4096, 4096, bl, 0);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  for (int i=0; i<100; ++i) {
    ObjectStore::Transaction t;
    t.write(cid, b, (rand() % 1024)*4096, 4096, bl, 0);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, a);
    t.remove(cid, b);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, SmallSkipFront) {
  ObjectStore::Sequencer osr("test");
  int r;
  coll_t cid;
  ghobject_t a(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.touch(cid, a);
    t.truncate(cid, a, 3000);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    bufferlist bl;
    bufferptr bp(4096);
    memset(bp.c_str(), 1, 4096);
    bl.append(bp);
    ObjectStore::Transaction t;
    t.write(cid, a, 4096, 4096, bl);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    bufferlist bl;
    ASSERT_EQ(8192, store->read(cid, a, 0, 8192, bl));
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
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, SmallSequentialUnaligned) {
  ObjectStore::Sequencer osr("test");
  int r;
  coll_t cid;
  ghobject_t a(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
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
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, a);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, ManyBigWrite) {
  ObjectStore::Sequencer osr("test");
  int r;
  coll_t cid;
  ghobject_t a(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  ghobject_t b(hobject_t(sobject_t("Object 2", CEPH_NOSNAP)));
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  bufferlist bl;
  bufferptr bp(4 * 1048576);
  bp.zero();
  bl.append(bp);
  for (int i=0; i<10; ++i) {
    ObjectStore::Transaction t;
    t.write(cid, a, i*4*1048586, 4*1048576, bl, 0);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  // aligned
  for (int i=0; i<10; ++i) {
    ObjectStore::Transaction t;
    t.write(cid, b, (rand() % 256)*4*1048576, 4*1048576, bl, 0);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  // unaligned
  for (int i=0; i<10; ++i) {
    ObjectStore::Transaction t;
    t.write(cid, b, (rand() % (256*4096))*1024, 4*1048576, bl, 0);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  // do some zeros
  for (int i=0; i<10; ++i) {
    ObjectStore::Transaction t;
    t.zero(cid, b, (rand() % (256*4096))*1024, 16*1048576);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, a);
    t.remove(cid, b);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, MiscFragmentTests) {
  ObjectStore::Sequencer osr("test");
  int r;
  coll_t cid;
  ghobject_t a(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  bufferlist bl;
  bufferptr bp(524288);
  bp.zero();
  bl.append(bp);
  {
    ObjectStore::Transaction t;
    t.write(cid, a, 0, 524288, bl, 0);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.write(cid, a, 1048576, 524288, bl, 0);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    bufferlist inbl;
    int r = store->read(cid, a, 524288 + 131072, 1024, inbl);
    ASSERT_EQ(r, 1024);
    ASSERT_EQ(inbl.length(), 1024u);
    ASSERT_TRUE(inbl.is_zero());
  }
  {
    ObjectStore::Transaction t;
    t.write(cid, a, 1048576 - 4096, 524288, bl, 0);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, a);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }

}

TEST_P(StoreTest, SimpleAttrTest) {
  ObjectStore::Sequencer osr("test");
  int r;
  coll_t cid;
  ghobject_t hoid(hobject_t(sobject_t("attr object 1", CEPH_NOSNAP)));
  bufferlist val, val2;
  val.append("value");
  val.append("value2");
  {
    bufferptr bp;
    map<string,bufferptr> aset;
    r = store->getattr(cid, hoid, "nofoo", bp);
    ASSERT_EQ(-ENOENT, r);
    r = store->getattrs(cid, hoid, aset);
    ASSERT_EQ(-ENOENT, r);
  }
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    bool r = store->collection_empty(cid);
    ASSERT_TRUE(r);
  }
  {
    bufferptr bp;
    r = store->getattr(cid, hoid, "nofoo", bp);
    ASSERT_EQ(-ENOENT, r);
  }
  {
    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    t.setattr(cid, hoid, "foo", val);
    t.setattr(cid, hoid, "bar", val2);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    bool r = store->collection_empty(cid);
    ASSERT_TRUE(!r);
  }
  {
    bufferptr bp;
    r = store->getattr(cid, hoid, "nofoo", bp);
    ASSERT_EQ(-ENODATA, r);

    r = store->getattr(cid, hoid, "foo", bp);
    ASSERT_EQ(0, r);
    bufferlist bl;
    bl.append(bp);
    ASSERT_TRUE(bl.contents_equal(val));

    map<string,bufferptr> bm;
    r = store->getattrs(cid, hoid, bm);
    ASSERT_EQ(0, r);

  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove_collection(cid);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, SimpleListTest) {
  ObjectStore::Sequencer osr("test");
  int r;
  coll_t cid(spg_t(pg_t(0, 1), shard_id_t(1)));
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  set<ghobject_t, ghobject_t::BitwiseComparator> all;
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
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  for (int bitwise=0; bitwise<2; ++bitwise) {
    set<ghobject_t, ghobject_t::BitwiseComparator> saw;
    vector<ghobject_t> objects;
    ghobject_t next, current;
    while (!next.is_max()) {
      int r = store->collection_list(cid, current, ghobject_t::get_max(),
				     (bool)bitwise, 50,
				     &objects, &next);
      if (r == -EOPNOTSUPP) {
	++bitwise; // skip nibblewise test
	continue;
      }
      ASSERT_EQ(r, 0);
      ASSERT_TRUE(sorted(objects, (bool)bitwise));
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
    for (set<ghobject_t, ghobject_t::BitwiseComparator>::iterator p = all.begin(); p != all.end(); ++p)
      t.remove(cid, *p);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
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
    ASSERT_TRUE(cmp_bitwise(a, b) < 0);
    a.pool = 1;
    b.pool = 2;
    ASSERT_TRUE(cmp_bitwise(a, b) < 0);
    a.pool = 3;
    ASSERT_TRUE(cmp_bitwise(a, b) > 0);
  }
  {
    ghobject_t a(hobject_t(sobject_t("a", CEPH_NOSNAP)));
    ghobject_t b(hobject_t(sobject_t("b", CEPH_NOSNAP)));
    a.hobj.pool = 1;
    b.hobj.pool = 1;
    ASSERT_TRUE(cmp_bitwise(a, b) < 0);
    a.hobj.pool = -3;
    ASSERT_TRUE(cmp_bitwise(a, b) < 0);
    a.hobj.pool = 1;
    b.hobj.pool = -3;
    ASSERT_TRUE(cmp_bitwise(a, b) > 0);
  }
}

TEST_P(StoreTest, MultipoolListTest) {
  ObjectStore::Sequencer osr("test");
  int r;
  int poolid = 4373;
  coll_t cid = coll_t(spg_t(pg_t(0, poolid), shard_id_t::NO_SHARD));
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  set<ghobject_t, ghobject_t::BitwiseComparator> all, saw;
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
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    vector<ghobject_t> objects;
    ghobject_t next, current;
    while (!next.is_max()) {
      int r = store->collection_list(cid, current, ghobject_t::get_max(),
				     true, 50,
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
    for (set<ghobject_t, ghobject_t::BitwiseComparator>::iterator p = all.begin(); p != all.end(); ++p)
      t.remove(cid, *p);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, SimpleCloneTest) {
  ObjectStore::Sequencer osr("test");
  int r;
  coll_t cid;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
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
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ghobject_t hoid2(hobject_t(sobject_t("Object 2", CEPH_NOSNAP),
			     "key", 123, -1, ""));
  {
    ObjectStore::Transaction t;
    t.clone(cid, hoid, hoid2);
    t.setattr(cid, hoid2, "attr2", small);
    t.rmattr(cid, hoid2, "attr1");
    t.write(cid, hoid, 10, large.length(), large);
    t.setattr(cid, hoid, "attr1", large);
    t.setattr(cid, hoid, "attr2", small);
    cerr << "Clone object and rm attr" << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);

    r = store->read(cid, hoid, 10, 5, newdata);
    ASSERT_EQ(r, 5);
    ASSERT_TRUE(newdata.contents_equal(large));

    newdata.clear();
    r = store->read(cid, hoid, 0, 5, newdata);
    ASSERT_EQ(r, 5);
    ASSERT_TRUE(newdata.contents_equal(small));

    newdata.clear();
    r = store->read(cid, hoid2, 10, 5, newdata);
    ASSERT_EQ(r, 5);
    ASSERT_TRUE(newdata.contents_equal(small));

    r = store->getattr(cid, hoid2, "attr2", attr);
    ASSERT_EQ(r, 0);
    ASSERT_TRUE(attr.contents_equal(small));

    attr.clear();
    r = store->getattr(cid, hoid2, "attr3", attr);
    ASSERT_EQ(r, 0);
    ASSERT_TRUE(attr.contents_equal(xlarge));

    attr.clear();
    r = store->getattr(cid, hoid, "attr1", attr);
    ASSERT_EQ(r, 0);
    ASSERT_TRUE(attr.contents_equal(large));
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove(cid, hoid2);
    ASSERT_EQ(0u, store->apply_transaction(&osr, std::move(t)));
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
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
    bufferlist rl;
    ASSERT_EQ((int)final.length(),
	      store->read(cid, hoid, 0, final.length(), rl));
    ASSERT_TRUE(final.contents_equal(rl));
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove(cid, hoid2);
    ASSERT_EQ(0u, store->apply_transaction(&osr, std::move(t)));
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
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
    bufferlist rl;
    ASSERT_EQ((int)final.length(),
	      store->read(cid, hoid, 0, final.length(), rl));
    ASSERT_TRUE(final.contents_equal(rl));
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove(cid, hoid2);
    ASSERT_EQ(0u, store->apply_transaction(&osr, std::move(t)));
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
    ASSERT_EQ(0u, store->apply_transaction(&osr, std::move(t)));
    bufferlist rl;
    ASSERT_EQ((int)final.length(),
	      store->read(cid, hoid, 0, final.length(), rl));
    /*cout << "expected:\n";
    final.hexdump(cout);
    cout << "got:\n";
    rl.hexdump(cout);*/
    ASSERT_TRUE(final.contents_equal(rl));
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove(cid, hoid2);
    ASSERT_EQ(0u, store->apply_transaction(&osr, std::move(t)));
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
    ASSERT_EQ(0u, store->apply_transaction(&osr, std::move(t)));
    bufferlist rl;
    bufferlist final;
    final.substr_of(pl, 0, al.length());
    final.append(al);
    bufferlist end;
    end.substr_of(pl, al.length()*2, pl.length() - al.length()*2);
    final.append(end);
    ASSERT_EQ((int)final.length(),
	      store->read(cid, hoid, 0, final.length(), rl));
    /*cout << "expected:\n";
    final.hexdump(cout);
    cout << "got:\n";
    rl.hexdump(cout);*/
    ASSERT_TRUE(final.contents_equal(rl));
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove(cid, hoid2);
    ASSERT_EQ(0u, store->apply_transaction(&osr, std::move(t)));
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
    ASSERT_EQ(0u, store->apply_transaction(&osr, std::move(t)));
    bufferlist rl;
    bufferlist final;
    final.substr_of(pl, 0, 32768);
    final.append(al);
    bufferlist end;
    end.substr_of(pl, final.length(), pl.length() - final.length());
    final.append(end);
    ASSERT_EQ((int)final.length(),
	      store->read(cid, hoid, 0, final.length(), rl));
    /*cout << "expected:\n";
    final.hexdump(cout);
    cout << "got:\n";
    rl.hexdump(cout);*/
    ASSERT_TRUE(final.contents_equal(rl));
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove(cid, hoid2);
    ASSERT_EQ(0u, store->apply_transaction(&osr, std::move(t)));
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
    ASSERT_EQ(0u, store->apply_transaction(&osr, std::move(t)));
    bufferlist rl;
    bufferlist final;
    final.substr_of(pl, 0, 33768);
    final.append(al);
    bufferlist end;
    end.substr_of(pl, final.length(), pl.length() - final.length());
    final.append(end);
    ASSERT_EQ((int)final.length(),
	      store->read(cid, hoid, 0, final.length(), rl));
    /*cout << "expected:\n";
    final.hexdump(cout);
    cout << "got:\n";
    rl.hexdump(cout);*/
    ASSERT_TRUE(final.contents_equal(rl));
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove(cid, hoid2);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, OmapSimple) {
  ObjectStore::Sequencer osr("test");
  int r;
  coll_t cid;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
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
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  // get header, keys
  {
    bufferlist h;
    map<string,bufferlist> r;
    store->omap_get(cid, hoid, &h, &r);
    ASSERT_TRUE(h.contents_equal(header));
    ASSERT_EQ(r.size(), km.size());
    cout << "r: " << r << std::endl;
  }
  // test iterator with seek_to_first
  {
    map<string,bufferlist> r;
    ObjectMap::ObjectMapIterator iter = store->get_omap_iterator(cid, hoid);
    for (iter->seek_to_first(); iter->valid(); iter->next(false)) {
      r[iter->key()] = iter->value();
    }
    cout << "r: " << r << std::endl;
    ASSERT_EQ(r.size(), km.size());
  }
  // test iterator with initial lower_bound
  {
    map<string,bufferlist> r;
    ObjectMap::ObjectMapIterator iter = store->get_omap_iterator(cid, hoid);
    for (iter->lower_bound(string()); iter->valid(); iter->next(false)) {
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
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, OmapCloneTest) {
  ObjectStore::Sequencer osr("test");
  int r;
  coll_t cid;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
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
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ghobject_t hoid2(hobject_t(sobject_t("Object 2", CEPH_NOSNAP),
			     "key", 123, -1, ""));
  {
    ObjectStore::Transaction t;
    t.clone(cid, hoid, hoid2);
    cerr << "Clone object" << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    map<string,bufferlist> r;
    bufferlist h;
    store->omap_get(cid, hoid2, &h, &r);
    ASSERT_TRUE(h.contents_equal(header));
    ASSERT_EQ(r.size(), km.size());
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove(cid, hoid2);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, SimpleCloneRangeTest) {
  ObjectStore::Sequencer osr("test");
  int r;
  coll_t cid;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
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
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ghobject_t hoid2(hobject_t(sobject_t("Object 2", CEPH_NOSNAP)));
  hoid2.hobj.pool = -1;
  {
    ObjectStore::Transaction t;
    t.clone_range(cid, hoid, hoid2, 10, 5, 0);
    cerr << "Clone range object" << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
    r = store->read(cid, hoid2, 0, 5, newdata);
    ASSERT_EQ(r, 5);
    ASSERT_TRUE(newdata.contents_equal(small));
  }
  {
    ObjectStore::Transaction t;
    t.truncate(cid, hoid, 1024*1024);
    t.clone_range(cid, hoid, hoid2, 0, 1024*1024, 0);
    cerr << "Clone range object" << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
    struct stat stat, stat2;
    r = store->stat(cid, hoid, &stat);
    r = store->stat(cid, hoid2, &stat2);
    ASSERT_EQ(stat.st_size, stat2.st_size);
    ASSERT_EQ(1024*1024, stat2.st_size);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove(cid, hoid2);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
}


TEST_P(StoreTest, SimpleObjectLongnameTest) {
  ObjectStore::Sequencer osr("test");
  int r;
  coll_t cid;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ghobject_t hoid(hobject_t(sobject_t("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaObjectaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 1", CEPH_NOSNAP)));
  {
    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    cerr << "Creating object " << hoid << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, ManyObjectTest) {
  ObjectStore::Sequencer osr("test");
  int NUM_OBJS = 2000;
  int r = 0;
  coll_t cid;
  string base = "";
  for (int i = 0; i < 100; ++i) base.append("aaaaa");
  set<ghobject_t, ghobject_t::BitwiseComparator> created;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = store->apply_transaction(&osr, std::move(t));
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
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }

  for (set<ghobject_t, ghobject_t::BitwiseComparator>::iterator i = created.begin();
       i != created.end();
       ++i) {
    struct stat buf;
    ASSERT_TRUE(!store->stat(cid, *i, &buf));
  }

  set<ghobject_t, ghobject_t::BitwiseComparator> listed, listed2;
  vector<ghobject_t> objects;
  r = store->collection_list(cid, ghobject_t(), ghobject_t::get_max(), true, INT_MAX, &objects, 0);
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
    cid,
    ghobject_t::get_max(),
    ghobject_t::get_max(),
    true,
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
    // nibblewise
    r = store->collection_list(cid, start2, ghobject_t::get_max(), false,
			       50,
			       &objects,
			       &next2);
    if (r != -EOPNOTSUPP) {
      ASSERT_TRUE(sorted(objects, false));
      ASSERT_EQ(r, 0);
      listed2.insert(objects.begin(), objects.end());
      if (objects.size() < 50) {
	ASSERT_TRUE(next2.is_max());
      }
      objects.clear();
      start2 = next2;
    }

    // bitwise
    r = store->collection_list(cid, start, ghobject_t::get_max(), true,
			       50,
			       &objects,
			       &next);
    ASSERT_TRUE(sorted(objects, true));
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
  if (listed2.size())
    ASSERT_EQ(listed.size(), listed2.size());
  for (set<ghobject_t, ghobject_t::BitwiseComparator>::iterator i = listed.begin();
       i != listed.end();
       ++i) {
    ASSERT_TRUE(created.count(*i));
  }

  for (set<ghobject_t, ghobject_t::BitwiseComparator>::iterator i = created.begin();
       i != created.end();
       ++i) {
    ObjectStore::Transaction t;
    t.remove(cid, *i);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  cerr << "cleaning up" << std::endl;
  {
    ObjectStore::Transaction t;
    t.remove_collection(cid);
    r = store->apply_transaction(&osr, std::move(t));
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
  ghobject_t create_object(gen_type *gen) {
    char buf[100];
    snprintf(buf, sizeof(buf), "OBJ_%u", seq);
    string name(buf);
    ++seq;
    return ghobject_t(
      hobject_t(
	name, string(), rand() & 2 ? CEPH_NOSNAP : rand(),
	seq % 16, // use smaller set of hash values so clone can work
	poolid, ""));
  }
};

static void dump_bl_mismatch(bufferlist& expected, bufferlist& actual)
{
  cout << __func__ << std::endl;
  unsigned offset = 0;
  while (expected[offset] == actual[offset])
    ++offset;
  cout << "--- buffer mismatch at offset 0x" << std::hex << offset << std::dec
       << std::endl;
  cout << "--- expected:\n";
  expected.hexdump(cout);
  cout << "--- actual:\n";
  actual.hexdump(cout);
}

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
  static const unsigned max_attr_value_len = 1024 * 4;
  coll_t cid;
  unsigned max_object_len;
  unsigned in_flight;
  map<ghobject_t, Object, ghobject_t::BitwiseComparator> contents;
  set<ghobject_t, ghobject_t::BitwiseComparator> available_objects;
  set<ghobject_t, ghobject_t::BitwiseComparator> in_flight_objects;
  ObjectGenerator *object_gen;
  gen_type *rng;
  ObjectStore *store;
  ObjectStore::Sequencer *osr;

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
    ObjectStore::Transaction *t;
    ghobject_t hoid;
    C_SyntheticOnReadable(SyntheticWorkloadState *state,
                          ObjectStore::Transaction *t, ghobject_t hoid)
      : state(state), t(t), hoid(hoid) {}

    void finish(int r) {
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
      r = state->store->read(state->cid, hoid, 0, state->contents[hoid].data.length(), r2);
      if (!state->contents[hoid].data.contents_equal(r2)) {
	dump_bl_mismatch(state->contents[hoid].data, r2);
	assert(0 == "mismatch in OnReadable");
        ASSERT_TRUE(state->contents[hoid].data.contents_equal(r2));
      }
      state->cond.Signal();
    }
  };

  class C_SyntheticOnClone : public Context {
  public:
    SyntheticWorkloadState *state;
    ObjectStore::Transaction *t;
    ghobject_t oid, noid;

    C_SyntheticOnClone(SyntheticWorkloadState *state,
                          ObjectStore::Transaction *t, ghobject_t oid, ghobject_t noid)
      : state(state), t(t), oid(oid), noid(noid) {}

    void finish(int r) {
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
      r = state->store->read(state->cid, noid, 0, state->contents[noid].data.length(), r2);
      if (!state->contents[noid].data.contents_equal(r2)) {
	dump_bl_mismatch(state->contents[noid].data, r2);
	assert(0 == " mismatch after clone");
        ASSERT_TRUE(state->contents[noid].data.contents_equal(r2));
      }
      state->cond.Signal();
    }
  };

  static void filled_byte_array(bufferlist& bl, size_t size)
  {
    static const char alphanum[] = "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";

    bufferptr bp(size);
    for (unsigned int i = 0; i < size - 1; i++) {
      bp[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }
    bp[size - 1] = '\0';

    bl.append(bp);
  }
  
  SyntheticWorkloadState(ObjectStore *store,
			 ObjectGenerator *gen,
			 gen_type *rng,
			 ObjectStore::Sequencer *osr,
			 coll_t cid,
			 unsigned max_size)
    : cid(cid), max_object_len(max_size),
      in_flight(0), object_gen(gen), rng(rng), store(store), osr(osr),
      lock("State lock") {}

  int init() {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    return store->apply_transaction(osr, std::move(t));
  }
  void shutdown() {
    while (1) {
      vector<ghobject_t> objects;
      int r = store->collection_list(cid, ghobject_t(), ghobject_t::get_max(),
				     true, 10, &objects, 0);
      assert(r >= 0);
      if (objects.empty())
	break;
      ObjectStore::Transaction t;
      for (vector<ghobject_t>::iterator p = objects.begin();
	   p != objects.end(); ++p) {
	t.remove(cid, *p);
      }
      store->apply_transaction(osr, std::move(t));
    }
    ObjectStore::Transaction t;
    t.remove_collection(cid);
    store->apply_transaction(osr, std::move(t));
  }

  ghobject_t get_uniform_random_object() {
    while (in_flight >= max_in_flight || available_objects.empty())
      cond.Wait(lock);
    boost::uniform_int<> choose(0, available_objects.size() - 1);
    int index = choose(*rng);
    set<ghobject_t, ghobject_t::BitwiseComparator>::iterator i = available_objects.begin();
    for ( ; index > 0; --index, ++i) ;
    ghobject_t ret = *i;
    return ret;
  }

  void wait_for_ready() {
    while (in_flight >= max_in_flight)
      cond.Wait(lock);
  }

  void wait_for_done() {
    osr->flush();
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

  int touch() {
    Mutex::Locker locker(lock);
    EnterExit ee("touch");
    if (!can_create())
      return -ENOSPC;
    wait_for_ready();
    ghobject_t new_obj = object_gen->create_object(rng);
    available_objects.erase(new_obj);
    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    t->touch(cid, new_obj);
    ++in_flight;
    in_flight_objects.insert(new_obj);
    if (!contents.count(new_obj))
      contents[new_obj] = Object();
    int status = store->queue_transaction(osr, std::move(*t), new C_SyntheticOnReadable(this, t, new_obj));
    delete t;
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

    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    t->clone(cid, old_obj, new_obj);
    ++in_flight;
    in_flight_objects.insert(old_obj);

    // *copy* the data buffer, since we may modify it later.
    contents[new_obj].attrs = contents[old_obj].attrs;
    contents[new_obj].data.clear();
    contents[new_obj].data.append(contents[old_obj].data.c_str(),
				  contents[old_obj].data.length());
    int status = store->queue_transaction(osr, std::move(*t), new C_SyntheticOnClone(this, t, old_obj, new_obj));
    delete t;
    return status;
  }

  int setattrs() {
    Mutex::Locker locker(lock);
    EnterExit ee("setattrs");
    if (!can_unlink())
      return -ENOENT;
    wait_for_ready();

    ghobject_t obj = get_uniform_random_object();
    available_objects.erase(obj);
    ObjectStore::Transaction *t = new ObjectStore::Transaction;

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
    t->setattrs(cid, obj, attrs);
    ++in_flight;
    in_flight_objects.insert(obj);
    int status = store->queue_transaction(osr, std::move(*t), new C_SyntheticOnReadable(this, t, obj));
    delete t;
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
    int r = store->getattrs(cid, obj, attrs);
    ASSERT_TRUE(r == 0);
    ASSERT_TRUE(attrs.size() == expected.size());
    for (map<string, bufferlist>::iterator it = expected.begin();
         it != expected.end(); ++it) {
      ASSERT_TRUE(it->second.contents_equal(attrs[it->first]));
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
    r = store->getattr(cid, obj, it->first, bl);
    ASSERT_EQ(r, 0);
    ASSERT_TRUE(it->second.contents_equal(bl));
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
    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    t->rmattr(cid, obj, it->first);

    contents[obj].attrs.erase(it->first);
    ++in_flight;
    in_flight_objects.insert(obj);
    int status = store->queue_transaction(osr, std::move(*t), new C_SyntheticOnReadable(this, t, obj));
    delete t;
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
    ObjectStore::Transaction *t = new ObjectStore::Transaction;

    boost::uniform_int<> u1(0, max_object_len/2);
    boost::uniform_int<> u2(0, max_object_len/10);
    uint64_t offset = u1(*rng);
    uint64_t len = u2(*rng);
    bufferlist bl;
    if (offset > len)
      swap(offset, len);

    filled_byte_array(bl, len);

    bufferlist& data = contents[new_obj].data;
    if (data.length() <= offset) {
      data.append_zero(offset-data.length());
      data.append(bl);
    } else {
      bufferlist value;
      assert(data.length() > offset);
      data.copy(0, offset, value);
      value.append(bl);
      if (value.length() < data.length())
        data.copy(value.length(),
		  data.length()-value.length(), value);
      value.swap(data);
    }

    t->write(cid, new_obj, offset, len, bl);
    ++in_flight;
    in_flight_objects.insert(new_obj);
    int status = store->queue_transaction(osr, std::move(*t), new C_SyntheticOnReadable(this, t, new_obj));
    delete t;
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
    r = store->read(cid, obj, offset, len, result);
    if (offset >= expected.length()) {
      ASSERT_EQ(r, 0);
    } else {
      size_t max_len = expected.length() - offset;
      if (len > max_len)
        len = max_len;
      assert(len == result.length());
      ASSERT_EQ(len, result.length());
      expected.copy(offset, len, bl);
      ASSERT_EQ(r, (int)len);
      if (!result.contents_equal(bl)) {
	cout << " obj " << obj
	 << " size " << expected.length()
	 << " offset " << offset
	 << " len " << len << std::endl;
	dump_bl_mismatch(bl, result);
	assert(0 == "mismatch after read");
	ASSERT_TRUE(result.contents_equal(bl));
      }
    }
  }

  int truncate() {
    Mutex::Locker locker(lock);
    EnterExit ee("truncate");
    if (!can_unlink())
      return -ENOENT;
    wait_for_ready();

    ghobject_t obj = get_uniform_random_object();
    available_objects.erase(obj);
    ObjectStore::Transaction *t = new ObjectStore::Transaction;

    boost::uniform_int<> choose(0, max_object_len);
    size_t len = choose(*rng);
    bufferlist bl;

    t->truncate(cid, obj, len);
    ++in_flight;
    in_flight_objects.insert(obj);
    bufferlist& data = contents[obj].data;
    if (data.length() <= len)
      data.append_zero(len - data.length());
    else {
      data.copy(0, len, bl);
      bl.swap(data);
    }

    int status = store->queue_transaction(osr, std::move(*t), new C_SyntheticOnReadable(this, t, obj));
    delete t;
    return status;
  }

  void scan() {
    Mutex::Locker locker(lock);
    EnterExit ee("scan");
    while (in_flight)
      cond.Wait(lock);
    vector<ghobject_t> objects;
    set<ghobject_t, ghobject_t::BitwiseComparator> objects_set, objects_set2;
    ghobject_t next, current;
    while (1) {
      //cerr << "scanning..." << std::endl;
      int r = store->collection_list(cid, current, ghobject_t::get_max(),
				     true, 100,
				     &objects, &next);
      ASSERT_EQ(r, 0);
      ASSERT_TRUE(sorted(objects, true));
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
	  assert(0);
	}
      for (set<ghobject_t>::iterator p = available_objects.begin();
	   p != available_objects.end();
	   ++p)
	if (objects_set.count(*p) == 0)
	  cerr << "- " << *p << std::endl;
      //cerr << " objects_set: " << objects_set << std::endl;
      //cerr << " available_set: " << available_objects << std::endl;
      assert(0 == "badness");
    }

    ASSERT_EQ(objects_set.size(), available_objects.size());
    for (set<ghobject_t, ghobject_t::BitwiseComparator>::iterator i = objects_set.begin();
	 i != objects_set.end();
	 ++i) {
      ASSERT_GT(available_objects.count(*i), (unsigned)0);
    }

    int r = store->collection_list(cid, ghobject_t(), ghobject_t::get_max(), true, INT_MAX, &objects, 0);
    ASSERT_EQ(r, 0);
    objects_set2.insert(objects.begin(), objects.end());
    ASSERT_EQ(objects_set2.size(), available_objects.size());
    for (set<ghobject_t, ghobject_t::BitwiseComparator>::iterator i = objects_set2.begin();
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
    int r = store->stat(cid, hoid, &buf);
    ASSERT_EQ(0, r);
    assert((uint64_t)buf.st_size == expected);
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
    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    t->remove(cid, to_remove);
    ++in_flight;
    available_objects.erase(to_remove);
    in_flight_objects.insert(to_remove);
    contents.erase(to_remove);
    int status = store->queue_transaction(osr, std::move(*t), new C_SyntheticOnReadable(this, t, to_remove));
    delete t;
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
    ObjectStore::Transaction *t = new ObjectStore::Transaction;

    boost::uniform_int<> u1(0, max_object_len/2);
    boost::uniform_int<> u2(0, max_object_len/10);
    uint64_t offset = u1(*rng);
    uint64_t len = u2(*rng);
    if (offset > len)
      swap(offset, len);

    if (contents[new_obj].data.length() < offset + len) {
      contents[new_obj].data.append_zero(offset+len-contents[new_obj].data.length());
    }
    contents[new_obj].data.zero(offset, len);

    t->zero(cid, new_obj, offset, len);
    ++in_flight;
    in_flight_objects.insert(new_obj);
    int status = store->queue_transaction(osr, std::move(*t), new C_SyntheticOnReadable(this, t, new_obj));
    delete t;
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

TEST_P(StoreTest, Synthetic) {
  ObjectStore::Sequencer osr("test");
  MixedGenerator gen(555);
  gen_type rng(time(NULL));
  coll_t cid(spg_t(pg_t(0,555), shard_id_t::NO_SHARD));

  SyntheticWorkloadState test_obj(store.get(), &gen, &rng, &osr, cid, 400*1024);
  test_obj.init();
  for (int i = 0; i < 1000; ++i) {
    if (!(i % 500)) cerr << "seeding object " << i << std::endl;
    test_obj.touch();
  }
  for (int i = 0; i < 10000; ++i) {
    if (!(i % 1000)) {
      cerr << "Op " << i << std::endl;
      test_obj.print_internal_state();
    }
    boost::uniform_int<> true_false(0, 99);
    int val = true_false(rng);
    if (val > 97) {
      test_obj.scan();
    } else if (val > 95) {
      test_obj.stat();
    } else if (val > 85) {
      test_obj.zero();
    } else if (val > 80) {
      test_obj.unlink();
    } else if (val > 55) {
      test_obj.write();
    } else if (val > 50) {
      test_obj.clone();
    } else if (val > 10) {
      test_obj.read();
    } else {
      test_obj.truncate();
    }
  }
  test_obj.wait_for_done();
  test_obj.shutdown();
}

TEST_P(StoreTest, AttrSynthetic) {
  ObjectStore::Sequencer osr("test");
  MixedGenerator gen(447);
  gen_type rng(time(NULL));
  coll_t cid(spg_t(pg_t(0,447),shard_id_t::NO_SHARD));

  SyntheticWorkloadState test_obj(store.get(), &gen, &rng, &osr, cid, 40*1024);
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
  ObjectStore::Sequencer osr("test");
  int64_t poolid = 11;
  coll_t cid(spg_t(pg_t(0,poolid),shard_id_t::NO_SHARD));
  int r;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  string base = "";
  for (int i = 0; i < 100; ++i) base.append("aaaaa");
  set<ghobject_t, ghobject_t::BitwiseComparator> created;
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
      r = store->apply_transaction(&osr, std::move(t));
      ASSERT_EQ(r, 0);
    }
    created.insert(hoid);
  }
  }
  vector<ghobject_t> objects;
  r = store->collection_list(cid, ghobject_t(), ghobject_t::get_max(), true, INT_MAX, &objects, 0);
  ASSERT_EQ(r, 0);
  set<ghobject_t, ghobject_t::BitwiseComparator> listed(objects.begin(), objects.end());
  cerr << "listed.size() is " << listed.size() << " and created.size() is " << created.size() << std::endl;
  ASSERT_TRUE(listed.size() == created.size());
  objects.clear();
  listed.clear();
  ghobject_t current, next;
  while (1) {
    r = store->collection_list(cid, current, ghobject_t::get_max(), true, 60,
			       &objects, &next);
    ASSERT_EQ(r, 0);
    ASSERT_TRUE(sorted(objects, true));
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
  for (set<ghobject_t, ghobject_t::BitwiseComparator>::iterator i = listed.begin();
       i != listed.end();
       ++i) {
    ASSERT_TRUE(created.count(*i));
  }

  for (set<ghobject_t, ghobject_t::BitwiseComparator>::iterator i = created.begin();
       i != created.end();
       ++i) {
    ObjectStore::Transaction t;
    t.remove(cid, *i);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ObjectStore::Transaction t;
  t.remove_collection(cid);
  r = store->apply_transaction(&osr, std::move(t));
  ASSERT_EQ(r, 0);
}

TEST_P(StoreTest, ScrubTest) {
  ObjectStore::Sequencer osr("test");
  int64_t poolid = 111;
  coll_t cid(spg_t(pg_t(0, poolid),shard_id_t(1)));
  int r;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  string base = "aaaaa";
  set<ghobject_t, ghobject_t::BitwiseComparator> created;
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
      r = store->apply_transaction(&osr, std::move(t));
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
    r = store->apply_transaction(&osr, std::move(t));
    created.insert(hoid1);
    created.insert(hoid2);
    created.insert(hoid3);
    ASSERT_EQ(r, 0);
  }

  vector<ghobject_t> objects;
  r = store->collection_list(cid, ghobject_t(), ghobject_t::get_max(), true,
			     INT_MAX, &objects, 0);
  ASSERT_EQ(r, 0);
  set<ghobject_t, ghobject_t::BitwiseComparator> listed(objects.begin(), objects.end());
  cerr << "listed.size() is " << listed.size() << " and created.size() is " << created.size() << std::endl;
  ASSERT_TRUE(listed.size() == created.size());
  objects.clear();
  listed.clear();
  ghobject_t current, next;
  while (1) {
    r = store->collection_list(cid, current, ghobject_t::get_max(), true, 60,
			       &objects, &next);
    ASSERT_EQ(r, 0);
    ASSERT_TRUE(sorted(objects, true));
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
  for (set<ghobject_t, ghobject_t::BitwiseComparator>::iterator i = listed.begin();
       i != listed.end();
       ++i) {
    ASSERT_TRUE(created.count(*i));
  }

  for (set<ghobject_t, ghobject_t::BitwiseComparator>::iterator i = created.begin();
       i != created.end();
       ++i) {
    ObjectStore::Transaction t;
    t.remove(cid, *i);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ObjectStore::Transaction t;
  t.remove_collection(cid);
  r = store->apply_transaction(&osr, std::move(t));
  ASSERT_EQ(r, 0);
}


TEST_P(StoreTest, OMapTest) {
  ObjectStore::Sequencer osr("test");
  coll_t cid;
  ghobject_t hoid(hobject_t("tesomap", "", CEPH_NOSNAP, 0, 0, ""));
  int r;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }

  map<string, bufferlist> attrs;
  {
    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    t.omap_clear(cid, hoid);
    map<string, bufferlist> start_set;
    t.omap_setkeys(cid, hoid, start_set);
    store->apply_transaction(&osr, std::move(t));
  }

  for (int i = 0; i < 100; i++) {
    if (!(i%5)) {
      std::cout << "On iteration " << i << std::endl;
    }
    ObjectStore::Transaction t;
    bufferlist bl;
    map<string, bufferlist> cur_attrs;
    r = store->omap_get(cid, hoid, &bl, &cur_attrs);
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
    store->apply_transaction(&osr, std::move(t));
  }

  int i = 0;
  while (attrs.size()) {
    if (!(i%5)) {
      std::cout << "removal: On iteration " << i << std::endl;
    }
    ObjectStore::Transaction t;
    bufferlist bl;
    map<string, bufferlist> cur_attrs;
    r = store->omap_get(cid, hoid, &bl, &cur_attrs);
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
    store->apply_transaction(&osr, std::move(t));

    attrs.erase(to_remove);

    ++i;
  }

  {
    bufferlist bl1;
    bl1.append("omap_header");
    ObjectStore::Transaction t;
    t.omap_setheader(cid, hoid, bl1);
    store->apply_transaction(&osr, std::move(t));
    t = ObjectStore::Transaction();
 
    bufferlist bl2;
    bl2.append("value");
    map<string, bufferlist> to_add;
    to_add.insert(pair<string, bufferlist>("key", bl2));
    t.omap_setkeys(cid, hoid, to_add);
    store->apply_transaction(&osr, std::move(t));

    bufferlist bl3;
    map<string, bufferlist> cur_attrs;
    r = store->omap_get(cid, hoid, &bl3, &cur_attrs);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(cur_attrs.size(), size_t(1));
    ASSERT_TRUE(bl3.contents_equal(bl1));
 
    set<string> keys;
    r = store->omap_get_keys(cid, hoid, &keys);
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
      store->apply_transaction(&osr, std::move(t));
    }
    {
      ObjectStore::Transaction t;
      t.omap_rmkeyrange(cid, hoid, "3", "7");
      store->apply_transaction(&osr, std::move(t));
    }
    {
      bufferlist hdr;
      map<string,bufferlist> m;
      store->omap_get(cid, hoid, &hdr, &m);
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
      store->apply_transaction(&osr, std::move(t));
    }
    {
      bufferlist hdr;
      map<string,bufferlist> m;
      store->omap_get(cid, hoid, &hdr, &m);
      ASSERT_EQ(0u, hdr.length());
      ASSERT_EQ(0u, m.size());
    }
  }

  ObjectStore::Transaction t;
  t.remove(cid, hoid);
  t.remove_collection(cid);
  r = store->apply_transaction(&osr, std::move(t));
  ASSERT_EQ(r, 0);
}

TEST_P(StoreTest, OMapIterator) {
  ObjectStore::Sequencer osr("test");
  coll_t cid;
  ghobject_t hoid(hobject_t("tesomap", "", CEPH_NOSNAP, 0, 0, ""));
  int count = 0;
  int r;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }

  map<string, bufferlist> attrs;
  {
    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    t.omap_clear(cid, hoid);
    map<string, bufferlist> start_set;
    t.omap_setkeys(cid, hoid, start_set);
    store->apply_transaction(&osr, std::move(t));
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

    iter = store->get_omap_iterator(cid, hoid);
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

    // FileStore may deadlock an active iterator vs apply_transaction
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
    store->apply_transaction(&osr, std::move(t));
  }

  iter = store->get_omap_iterator(cid, hoid);
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

  // FileStore may deadlock an active iterator vs apply_transaction
  iter = ObjectMap::ObjectMapIterator();
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove_collection(cid);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, XattrTest) {
  ObjectStore::Sequencer osr("test");
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
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    t.touch(cid, hoid);
    r = store->apply_transaction(&osr, std::move(t));
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
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }

  map<string, bufferptr> aset;
  store->getattrs(cid, hoid, aset);
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
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }

  aset.clear();
  store->getattrs(cid, hoid, aset);
  ASSERT_EQ(aset.size(), attrs.size());
  for (map<string, bufferptr>::iterator i = aset.begin();
       i != aset.end();
       ++i) {
    bufferlist bl;
    bl.push_back(i->second);
    ASSERT_TRUE(attrs[i->first] == bl);
  }

  bufferptr bp;
  r = store->getattr(cid, hoid, "attr2", bp);
  ASSERT_EQ(r, -ENODATA);

  r = store->getattr(cid, hoid, "attr3", bp);
  ASSERT_EQ(r, 0);
  bufferlist bl2;
  bl2.push_back(bp);
  ASSERT_TRUE(bl2 == attrs["attr3"]);

  ObjectStore::Transaction t;
  t.remove(cid, hoid);
  t.remove_collection(cid);
  r = store->apply_transaction(&osr, std::move(t));
  ASSERT_EQ(r, 0);
}

void colsplittest(
  ObjectStore *store,
  unsigned num_objects,
  unsigned common_suffix_size
  ) {
  ObjectStore::Sequencer osr("test");
  coll_t cid(spg_t(pg_t(0,52),shard_id_t::NO_SHARD));
  coll_t tid(spg_t(pg_t(1<<common_suffix_size,52),shard_id_t::NO_SHARD));
  int r = 0;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, common_suffix_size);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    for (uint32_t i = 0; i < 2*num_objects; ++i) {
      stringstream objname;
      objname << "obj" << i;
      t.touch(cid, ghobject_t(hobject_t(
	  objname.str(),
	  "",
	  CEPH_NOSNAP,
	  i<<common_suffix_size,
	  52, "")));
    }
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.create_collection(tid, common_suffix_size + 1);
    t.split_collection(cid, common_suffix_size+1, 1<<common_suffix_size, tid);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }

  ObjectStore::Transaction t;
  vector<ghobject_t> objects;
  r = store->collection_list(cid, ghobject_t(), ghobject_t::get_max(), true,
			     INT_MAX, &objects, 0);
  ASSERT_EQ(r, 0);
  ASSERT_EQ(objects.size(), num_objects);
  for (vector<ghobject_t>::iterator i = objects.begin();
       i != objects.end();
       ++i) {
    ASSERT_EQ(!!(i->hobj.get_hash() & (1<<common_suffix_size)), 0u);
    t.remove(cid, *i);
  }

  objects.clear();
  r = store->collection_list(tid, ghobject_t(), ghobject_t::get_max(), true,
			     INT_MAX, &objects, 0);
  ASSERT_EQ(r, 0);
  ASSERT_EQ(objects.size(), num_objects);
  for (vector<ghobject_t>::iterator i = objects.begin();
       i != objects.end();
       ++i) {
    ASSERT_EQ(!(i->hobj.get_hash() & (1<<common_suffix_size)), 0u);
    t.remove(tid, *i);
  }

  t.remove_collection(cid);
  t.remove_collection(tid);
  r = store->apply_transaction(&osr, std::move(t));
  ASSERT_EQ(r, 0);
}

TEST_P(StoreTest, ColSplitTest1) {
  colsplittest(store.get(), 10000, 11);
}
TEST_P(StoreTest, ColSplitTest2) {
  colsplittest(store.get(), 100, 7);
}

#if 0
TEST_P(StoreTest, ColSplitTest3) {
  colsplittest(store.get(), 100000, 25);
}
#endif

/**
 * This test tests adding two different groups
 * of objects, each with 1 common prefix and 1
 * different prefix.  We then remove half
 * in order to verify that the merging correctly
 * stops at the common prefix subdir.  See bug
 * #5273 */
TEST_P(StoreTest, TwoHash) {
  ObjectStore::Sequencer osr("test");
  coll_t cid;
  int r;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = store->apply_transaction(&osr, std::move(t));
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
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  std::cout << "Removing half" << std::endl;
  for (int i = 1; i < 8; ++i) {
    ObjectStore::Transaction t;
    ghobject_t o;
    o.hobj.pool = -1;
    o.hobj.set_hash((i << 16) | 0xA1);
    t.remove(cid, o);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  std::cout << "Checking" << std::endl;
  for (int i = 1; i < 8; ++i) {
    ObjectStore::Transaction t;
    ghobject_t o;
    o.hobj.set_hash((i << 16) | 0xA1);
    o.hobj.pool = -1;
    bool exists = store->exists(cid, o);
    ASSERT_EQ(exists, false);
  }
  {
    ghobject_t o;
    o.hobj.set_hash(0xA1);
    o.hobj.pool = -1;
    bool exists = store->exists(cid, o);
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
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ObjectStore::Transaction t;
  t.remove_collection(cid);
  r = store->apply_transaction(&osr, std::move(t));
  ASSERT_EQ(r, 0);
}

TEST_P(StoreTest, Rename) {
  ObjectStore::Sequencer osr("test");
  coll_t cid(spg_t(pg_t(0, 2122),shard_id_t::NO_SHARD));
  ghobject_t srcoid(hobject_t("src_oid", "", CEPH_NOSNAP, 0, 0, ""));
  ghobject_t dstoid(hobject_t("dest_oid", "", CEPH_NOSNAP, 0, 0, ""));
  bufferlist a, b;
  a.append("foo");
  b.append("bar");
  int r;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    t.write(cid, srcoid, 0, a.length(), a);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ASSERT_TRUE(store->exists(cid, srcoid));
  {
    ObjectStore::Transaction t;
    t.collection_move_rename(cid, srcoid, cid, dstoid);
    t.remove(cid, srcoid);
    t.write(cid, srcoid, 0, b.length(), b);
    t.setattr(cid, srcoid, "attr", b);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ASSERT_TRUE(store->exists(cid, srcoid));
  ASSERT_TRUE(store->exists(cid, dstoid));
  {
    bufferlist bl;
    store->read(cid, srcoid, 0, 3, bl);
    ASSERT_TRUE(bl.contents_equal(b));
    store->read(cid, dstoid, 0, 3, bl);
    ASSERT_TRUE(bl.contents_equal(a));
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, dstoid);
    t.collection_move_rename(cid, srcoid, cid, dstoid);
    t.remove(cid, srcoid);
    t.setattr(cid, srcoid, "attr", a);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ASSERT_TRUE(store->exists(cid, dstoid));
  {
    bufferlist bl;
    store->read(cid, dstoid, 0, 3, bl);
    ASSERT_TRUE(bl.contents_equal(b));
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, dstoid);
    t.remove(cid, srcoid);
    t.remove_collection(cid);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, MoveRename) {
  ObjectStore::Sequencer osr("test");
  coll_t cid(spg_t(pg_t(0, 212),shard_id_t::NO_SHARD));
  ghobject_t temp_oid(hobject_t("tmp_oid", "", CEPH_NOSNAP, 0, 0, ""));
  ghobject_t oid(hobject_t("dest_oid", "", CEPH_NOSNAP, 0, 0, ""));
  int r;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    t.touch(cid, oid);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ASSERT_TRUE(store->exists(cid, oid));
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
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ASSERT_TRUE(store->exists(cid, temp_oid));
  {
    ObjectStore::Transaction t;
    t.remove(cid, oid);
    t.collection_move_rename(cid, temp_oid, cid, oid);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ASSERT_TRUE(store->exists(cid, oid));
  ASSERT_FALSE(store->exists(cid, temp_oid));
  {
    bufferlist newdata;
    r = store->read(cid, oid, 0, 1000, newdata);
    ASSERT_GE(r, 0);
    ASSERT_TRUE(newdata.contents_equal(data));
    bufferlist newattr;
    r = store->getattr(cid, oid, "attr", newattr);
    ASSERT_EQ(r, 0);
    ASSERT_TRUE(newattr.contents_equal(attr));
    set<string> keys;
    keys.insert("omap_key");
    map<string, bufferlist> newomap;
    r = store->omap_get_values(cid, oid, keys, &newomap);
    ASSERT_GE(r, 0);
    ASSERT_EQ(1u, newomap.size());
    ASSERT_TRUE(newomap.count("omap_key"));
    ASSERT_TRUE(newomap["omap_key"].contents_equal(omap["omap_key"]));
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, oid);
    t.remove_collection(cid);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, BigRGWObjectName) {
  ObjectStore::Sequencer osr("test");
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

  int r;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    t.touch(cid, oidhead);
    t.collection_move_rename(cid, oidhead, cid, oid);
    t.touch(cid, oidhead);
    t.collection_move_rename(cid, oidhead, cid, oid2);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }

  {
    ObjectStore::Transaction t;
    t.remove(cid, oid);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }

  {
    vector<ghobject_t> objects;
    r = store->collection_list(cid, ghobject_t(), ghobject_t::get_max(), true,
			       INT_MAX, &objects, 0);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(objects.size(), 1u);
    ASSERT_EQ(objects[0], oid2);
  }

  ASSERT_FALSE(store->exists(cid, oid));

  {
    ObjectStore::Transaction t;
    t.remove(cid, oid2);
    t.remove_collection(cid);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);

  }
}

TEST_P(StoreTest, SetAllocHint) {
  ObjectStore::Sequencer osr("test");
  coll_t cid;
  ghobject_t hoid(hobject_t("test_hint", "", CEPH_NOSNAP, 0, 0, ""));
  int r;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    t.touch(cid, hoid);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.set_alloc_hint(cid, hoid, 4*1024*1024, 1024*4);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.set_alloc_hint(cid, hoid, 4*1024*1024, 1024*4);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove_collection(cid);
    r = store->apply_transaction(&osr, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

INSTANTIATE_TEST_CASE_P(
  ObjectStore,
  StoreTest,
  ::testing::Values(
    "memstore",
    "filestore",
    "bluestore",
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

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  env_to_vec(args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->set_val("osd_journal_size", "400");
  g_ceph_context->_conf->set_val("filestore_index_retry_probability", "0.5");
  g_ceph_context->_conf->set_val("filestore_op_thread_timeout", "1000");
  g_ceph_context->_conf->set_val("filestore_op_thread_suicide_timeout", "10000");
  g_ceph_context->_conf->set_val("filestore_debug_disable_sharded_check", "true");
  g_ceph_context->_conf->set_val("filestore_fiemap", "true");
  g_ceph_context->_conf->set_val("bluestore_fsck_on_mount", "true");
  g_ceph_context->_conf->set_val("bluestore_fsck_on_umount", "true");
  g_ceph_context->_conf->set_val("bluestore_debug_misc", "true");
  g_ceph_context->_conf->set_val("bluestore_debug_small_allocations", "4");
  g_ceph_context->_conf->set_val("bluestore_debug_freelist", "true");
  g_ceph_context->_conf->set_val(
    "enable_experimental_unrecoverable_data_corrupting_features", "*");
  g_ceph_context->_conf->apply_changes(NULL);

  ::testing::InitGoogleTest(&argc, argv);
  int r = RUN_ALL_TESTS();
  g_ceph_context->put();
  return r;
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; make ceph_test_objectstore && 
 *    ./ceph_test_objectstore \
 *        --gtest_filter=*.collect_metadata* --log-to-stderr=true --debug-filestore=20
 *  "
 * End:
 */
