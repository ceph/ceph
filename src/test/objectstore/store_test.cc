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
#include "os/FileStore.h"
#include "os/KeyValueStore.h"
#include "include/Context.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/errno.h"
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
  virtual void SetUp() {
    int r = ::mkdir("store_test_temp_dir", 0777);
    if (r < 0 && errno != EEXIST) {
      r = -errno;
      cerr << __func__ << ": unable to create store_test_temp_dir" << ": " << cpp_strerror(r) << std::endl;
      return;
    }

    ObjectStore *store_ = ObjectStore::create(g_ceph_context,
                                              string(GetParam()),
                                              string("store_test_temp_dir"),
                                              string("store_test_temp_journal"));
    store.reset(store_);
    EXPECT_EQ(store->mkfs(), 0);
    EXPECT_EQ(store->mount(), 0);
  }

  virtual void TearDown() {
    store->umount();
  }
};

bool sorted(const vector<ghobject_t> &in) {
  ghobject_t start;
  for (vector<ghobject_t>::const_iterator i = in.begin();
       i != in.end();
       ++i) {
    if (start > *i) return false;
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
  }
}

TEST_P(StoreTest, SimpleColTest) {
  coll_t cid = coll_t("initial");
  int r = 0;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid);
    cerr << "create collection" << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove_collection(cid);
    cerr << "remove collection" << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.create_collection(cid);
    cerr << "add collection" << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove_collection(cid);
    cerr << "remove collection" << std::endl;
    r = store->apply_transaction(t);
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
    g_ceph_context->_conf->set_val("filestore_merge_threshold", oss.str().c_str());
  }

  uint32_t pg_num = 128;

  boost::uniform_int<> pg_id_range(0, pg_num);
  gen_type rng(time(NULL));
  int pg_id = pg_id_range(rng);

  int objs_per_folder = abs(merge_threshold) * 16 * g_ceph_context->_conf->filestore_split_multiple;
  boost::uniform_int<> folders_range(5, 256);
  uint64_t expected_num_objs = (uint64_t)objs_per_folder * (uint64_t)folders_range(rng);

  char buf[100];
  snprintf(buf, 100, "1.%x_head", pg_id);

  coll_t cid(buf);
  int r;
  {
    // Create a collection along with a hint
    ObjectStore::Transaction t;
    t.create_collection(cid);
    cerr << "create collection" << std::endl;
    bufferlist hint;
    ::encode(pg_num, hint);
    ::encode(expected_num_objs, hint);
    t.collection_hint(cid, ObjectStore::Transaction::COLL_HINT_EXPECTED_NUM_OBJECTS, hint);
    cerr << "collection hint" << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  {
    // Remove the collection
    ObjectStore::Transaction t;
    t.remove_collection(cid);
    cerr << "remove collection" << std::endl;
    r = store->apply_transaction(t);
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
  int r;
  coll_t cid = coll_t("coll");
  {
    ObjectStore::Transaction t;
    t.create_collection(cid);
    cerr << "Creating collection " << cid << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  ghobject_t hoid(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  {
    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    cerr << "Creating object " << hoid << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.touch(cid, hoid);
    cerr << "Remove then create" << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    bufferlist bl;
    bl.append("abcde");
    t.remove(cid, hoid);
    t.write(cid, hoid, 10, 5, bl);
    cerr << "Remove then create" << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, SimpleCloneTest) {
  int r;
  coll_t cid = coll_t("coll");
  {
    ObjectStore::Transaction t;
    t.create_collection(cid);
    cerr << "Creating collection " << cid << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  ghobject_t hoid(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  bufferlist small;
  small.append("small");
  {
    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    t.setattr(cid, hoid, "attr1", small);
    cerr << "Creating object and set attr " << hoid << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  ghobject_t hoid2(hobject_t(sobject_t("Object 2", CEPH_NOSNAP)));
  {
    ObjectStore::Transaction t;
    t.clone(cid, hoid, hoid2);
    t.rmattr(cid, hoid, "attr1");
    cerr << "Clone object and rm attr" << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove(cid, hoid2);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, SimpleCloneRangeTest) {
  int r;
  coll_t cid = coll_t("coll");
  {
    ObjectStore::Transaction t;
    t.create_collection(cid);
    cerr << "Creating collection " << cid << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  ghobject_t hoid(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  bufferlist small, newdata;
  small.append("small");
  {
    ObjectStore::Transaction t;
    t.write(cid, hoid, 10, 5, small);
    cerr << "Creating object and write bl " << hoid << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  ghobject_t hoid2(hobject_t(sobject_t("Object 2", CEPH_NOSNAP)));
  {
    ObjectStore::Transaction t;
    t.clone_range(cid, hoid, hoid2, 10, 5, 0);
    cerr << "Clone range object" << std::endl;
    r = store->apply_transaction(t);
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
    r = store->apply_transaction(t);
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
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
}


TEST_P(StoreTest, SimpleObjectLongnameTest) {
  int r;
  coll_t cid = coll_t("coll");
  {
    ObjectStore::Transaction t;
    t.create_collection(cid);
    cerr << "Creating collection " << cid << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  ghobject_t hoid(hobject_t(sobject_t("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaObjectaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 1", CEPH_NOSNAP)));
  {
    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    cerr << "Creating object " << hoid << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = store->apply_transaction(t);
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
  ObjectStore::Sequencer osr("test");
  int r;
  coll_t cid;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid);
    cerr << "Creating collection " << cid << std::endl;
    r = store->apply_transaction(&osr, t);
    ASSERT_EQ(r, 0);
  }
  for (unsigned i = 0; i < 320; ++i) {
    ObjectStore::Transaction t;
    ghobject_t hoid = generate_long_name(i);
    t.touch(cid, hoid);
    cerr << "Creating object " << hoid << std::endl;
    r = store->apply_transaction(&osr, t);
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
    r = store->apply_transaction(&osr, t);
  }

  for (unsigned i = 0; i < 319; ++i) {
    ObjectStore::Transaction t;
    ghobject_t hoid = generate_long_name(i);
    t.remove(cid, hoid);
    cerr << "Removing object " << hoid << std::endl;
    r = store->apply_transaction(&osr, t);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, test_obj_2);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = store->apply_transaction(&osr, t);
    ASSERT_EQ(r, 0);
  }

}

TEST_P(StoreTest, ManyObjectTest) {
  int NUM_OBJS = 2000;
  int r = 0;
  coll_t cid("blah");
  string base = "";
  for (int i = 0; i < 100; ++i) base.append("aaaaa");
  set<ghobject_t> created;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid);
    r = store->apply_transaction(t);
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
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }

  for (set<ghobject_t>::iterator i = created.begin();
       i != created.end();
       ++i) {
    struct stat buf;
    ASSERT_TRUE(!store->stat(cid, *i, &buf));
  }

  set<ghobject_t> listed;
  vector<ghobject_t> objects;
  r = store->collection_list(cid, objects);
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
  r = store->collection_list_partial(
    cid,
    ghobject_t::get_max(),
    50,
    60,
    0,
    &objects,
    &next
    );
  ASSERT_EQ(r, 0);
  ASSERT_TRUE(objects.empty());

  objects.clear();
  listed.clear();
  while (1) {
    r = store->collection_list_partial(cid, start,
				       50,
				       60,
				       0,
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
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  cerr << "cleaning up" << std::endl;
  {
    ObjectStore::Transaction t;
    t.remove_collection(cid);
    r = store->apply_transaction(t);
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
  MixedGenerator() : seq(0) {}
  ghobject_t create_object(gen_type *gen) {
    char buf[100];
    snprintf(buf, sizeof(buf), "%u", seq);

    boost::uniform_int<> true_false(0, 1);
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
	0, ""));
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
  static const unsigned max_object_len = 1024 * 40;
  static const unsigned max_attr_size = 5;
  static const unsigned max_attr_name_len = 100;
  static const unsigned max_attr_value_len = 1024 * 4;
  coll_t cid;
  unsigned in_flight;
  map<ghobject_t, Object> contents;
  set<ghobject_t> available_objects;
  set<ghobject_t> in_flight_objects;
  ObjectGenerator *object_gen;
  gen_type *rng;
  ObjectStore *store;
  ObjectStore::Sequencer *osr;

  Mutex lock;
  Cond cond;

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
      ASSERT_TRUE(state->in_flight_objects.count(hoid));
      ASSERT_EQ(r, 0);
      state->in_flight_objects.erase(hoid);
      if (state->contents.count(hoid))
        state->available_objects.insert(hoid);
      --(state->in_flight);
      state->cond.Signal();
    }
  };

  class C_SyntheticOnStash : public Context {
  public:
    SyntheticWorkloadState *state;
    ObjectStore::Transaction *t;
    ghobject_t oid, noid;

    C_SyntheticOnStash(SyntheticWorkloadState *state,
		       ObjectStore::Transaction *t, ghobject_t oid,
		       ghobject_t noid)
      : state(state), t(t), oid(oid), noid(noid) {}

    void finish(int r) {
      Mutex::Locker locker(state->lock);
      ASSERT_TRUE(state->in_flight_objects.count(oid));
      ASSERT_EQ(r, 0);
      state->in_flight_objects.erase(oid);
      if (state->contents.count(noid))
        state->available_objects.insert(noid);
      --(state->in_flight);
      bufferlist r2;
      r = state->store->read(
	state->cid, noid, 0,
	state->contents[noid].data.length(), r2);
      if (!state->contents[noid].data.contents_equal(r2)) {
	dump_bl_mismatch(state->contents[noid].data, r2);
	assert(0 == " mismatch after clone");
        ASSERT_TRUE(state->contents[noid].data.contents_equal(r2));
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
			 coll_t cid)
    : cid(cid), in_flight(0), object_gen(gen), rng(rng), store(store), osr(osr),
      lock("State lock") {}

  int init() {
    ObjectStore::Transaction t;
    t.create_collection(cid);
    return store->apply_transaction(t);
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

  int touch() {
    Mutex::Locker locker(lock);
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
    return store->queue_transaction(osr, t, new C_SyntheticOnReadable(this, t, new_obj));
  }

  int stash() {
    Mutex::Locker locker(lock);
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

    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    t->collection_move_rename(cid, old_obj, cid, new_obj);
    ++in_flight;
    in_flight_objects.insert(old_obj);

    // *copy* the data buffer, since we may modify it later.
    contents[new_obj].attrs = contents[old_obj].attrs;
    contents[new_obj].data.clear();
    contents[new_obj].data.append(contents[old_obj].data.c_str(),
				  contents[old_obj].data.length());
    contents.erase(old_obj);
    int status = store->queue_transaction(
      osr, t,
      new C_SyntheticOnStash(this, t, old_obj, new_obj));
    delete t;
    return status;
  }

  int clone() {
    Mutex::Locker locker(lock);
    if (!can_unlink())
      return -ENOENT;
    if (!can_create())
      return -ENOSPC;
    wait_for_ready();

    ghobject_t old_obj;
    do {
      old_obj = get_uniform_random_object();
    } while (contents[old_obj].data.length());
    available_objects.erase(old_obj);
    ghobject_t new_obj = object_gen->create_object(rng);
    available_objects.erase(new_obj);

    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    t->clone(cid, old_obj, new_obj);
    ++in_flight;
    in_flight_objects.insert(old_obj);

    contents[new_obj] = contents[old_obj];
    return store->queue_transaction(osr, t, new C_SyntheticOnClone(this, t, old_obj, new_obj));
  }

  int setattrs() {
    Mutex::Locker locker(lock);
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
    return store->queue_transaction(osr, t, new C_SyntheticOnReadable(this, t, obj));
  }

  void getattrs() {
    ghobject_t obj;
    map<string, bufferlist> expected;
    int retry;
    {
      Mutex::Locker locker(lock);
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
    ghobject_t obj;
    int r;
    int retry;
    map<string, bufferlist> expected;
    {
      Mutex::Locker locker(lock);
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
    ASSERT_TRUE(r >= 0);
    ASSERT_TRUE(it->second.contents_equal(bl));
  }

  int rmattr() {
    Mutex::Locker locker(lock);
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
    return store->queue_transaction(osr, t, new C_SyntheticOnReadable(this, t, obj));
  }

  int write() {
    Mutex::Locker locker(lock);
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

    if (contents[new_obj].data.length() <= offset) {
      contents[new_obj].data.append_zero(offset-contents[new_obj].data.length());
      contents[new_obj].data.append(bl);
    } else {
      bufferlist value;
      contents[new_obj].data.copy(0, offset, value);
      value.append(bl);
      if (value.length() < contents[new_obj].data.length())
        contents[new_obj].data.copy(value.length(),
                                    contents[new_obj].data.length()-value.length(), value);
      value.swap(contents[new_obj].data);
    }

    t->write(cid, new_obj, offset, len, bl);
    ++in_flight;
    in_flight_objects.insert(new_obj);
    return store->queue_transaction(osr, t, new C_SyntheticOnReadable(this, t, new_obj));
  }

  void read() {
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
      if (!can_unlink())
        return ;
      wait_for_ready();

      obj = get_uniform_random_object();
      expected = contents[obj].data;
    }
    bufferlist bl, result;
    r = store->read(cid, obj, offset, len, result);
    if (offset >= expected.length()) {
      ASSERT_EQ(r, 0);
    } else {
      size_t max_len = expected.length() - offset;
      if (len > max_len)
        len = max_len;
      ASSERT_EQ(len, result.length());
      expected.copy(offset, len, bl);
      ASSERT_EQ(r, (int)len);
      ASSERT_TRUE(result.contents_equal(bl));
    }
  }

  int truncate() {
    Mutex::Locker locker(lock);
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
    if (contents[obj].data.length() <= len)
      contents[obj].data.append_zero(len - contents[obj].data.length());
    else {
      contents[obj].data.copy(0, len, bl);
      bl.swap(contents[obj].data);
    }

    return store->queue_transaction(osr, t, new C_SyntheticOnReadable(this, t, obj));
  }

  void scan() {
    Mutex::Locker locker(lock);
    while (in_flight)
      cond.Wait(lock);
    vector<ghobject_t> objects;
    set<ghobject_t> objects_set, objects_set2;
    ghobject_t next, current;
    while (1) {
      cerr << "scanning..." << std::endl;
      int r = store->collection_list_partial(cid, current, 50, 100,
					     0, &objects, &next);
      ASSERT_EQ(r, 0);
      ASSERT_TRUE(sorted(objects));
      objects_set.insert(objects.begin(), objects.end());
      objects.clear();
      if (next.is_max()) break;
      current = next;
    }
    ASSERT_EQ(objects_set.size(), available_objects.size());
    for (set<ghobject_t>::iterator i = objects_set.begin();
	 i != objects_set.end();
	 ++i) {
      ASSERT_GT(available_objects.count(*i), (unsigned)0);
    }

    int r = store->collection_list(cid, objects);
    ASSERT_EQ(r, 0);
    objects_set2.insert(objects.begin(), objects.end());
    ASSERT_EQ(objects_set2.size(), available_objects.size());
    for (set<ghobject_t>::iterator i = objects_set2.begin();
	 i != objects_set2.end();
	 ++i) {
      ASSERT_GT(available_objects.count(*i), (unsigned)0);
    }
  }

  void stat() {
    ghobject_t hoid;
    uint64_t expected;
    {
      Mutex::Locker locker(lock);
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
      --in_flight;
      cond.Signal();
      in_flight_objects.erase(hoid);
      available_objects.insert(hoid);
    }
  }

  int unlink() {
    Mutex::Locker locker(lock);
    if (!can_unlink())
      return -ENOENT;
    ghobject_t to_remove = get_uniform_random_object();
    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    t->remove(cid, to_remove);
    ++in_flight;
    available_objects.erase(to_remove);
    in_flight_objects.insert(to_remove);
    contents.erase(to_remove);
    return store->queue_transaction(osr, t, new C_SyntheticOnReadable(this, t, to_remove));
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
  MixedGenerator gen;
  gen_type rng(time(NULL));
  coll_t cid("synthetic_1");

  SyntheticWorkloadState test_obj(store.get(), &gen, &rng, &osr, cid);
  test_obj.init();
  for (int i = 0; i < 1000; ++i) {
    if (!(i % 10)) cerr << "seeding object " << i << std::endl;
    test_obj.touch();
  }
  for (int i = 0; i < 10000; ++i) {
    if (!(i % 10)) {
      cerr << "Op " << i << std::endl;
      test_obj.print_internal_state();
    }
    boost::uniform_int<> true_false(0, 99);
    int val = true_false(rng);
    if (val > 97) {
      test_obj.scan();
    } else if (val > 90) {
      test_obj.stat();
    } else if (val > 85) {
      test_obj.unlink();
    } else if (val > 55) {
      test_obj.write();
    } else if (val > 50) {
      test_obj.clone();
    } else if (val > 30) {
      test_obj.stash();
    } else if (val > 10) {
      test_obj.read();
    } else {
      test_obj.truncate();
    }
  }
  test_obj.wait_for_done();
}

TEST_P(StoreTest, AttrSynthetic) {
  ObjectStore::Sequencer osr("test");
  MixedGenerator gen;
  gen_type rng(time(NULL));
  coll_t cid("synthetic_2");

  SyntheticWorkloadState test_obj(store.get(), &gen, &rng, &osr, cid);
  test_obj.init();
  for (int i = 0; i < 500; ++i) {
    if (!(i % 10)) cerr << "seeding object " << i << std::endl;
    test_obj.touch();
  }
  for (int i = 0; i < 10000; ++i) {
    if (!(i % 10)) {
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
}

TEST_P(StoreTest, HashCollisionTest) {
  coll_t cid("blah");
  int r;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid);
    r = store->apply_transaction(t);
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
    if (!(i % 5)) {
      cerr << "Object n" << n << " "<< i << std::endl;
    }
    ghobject_t hoid(hobject_t(string(buf) + base, string(), CEPH_NOSNAP, 0, 0, string(nbuf)));
    {
      ObjectStore::Transaction t;
      t.touch(cid, hoid);
      r = store->apply_transaction(t);
      ASSERT_EQ(r, 0);
    }
    created.insert(hoid);
  }
  }
  vector<ghobject_t> objects;
  r = store->collection_list(cid, objects);
  ASSERT_EQ(r, 0);
  set<ghobject_t> listed(objects.begin(), objects.end());
  cerr << "listed.size() is " << listed.size() << " and created.size() is " << created.size() << std::endl;
  ASSERT_TRUE(listed.size() == created.size());
  objects.clear();
  listed.clear();
  ghobject_t current, next;
  while (1) {
    r = store->collection_list_partial(cid, current, 50, 60,
				       0, &objects, &next);
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
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  ObjectStore::Transaction t;
  t.remove_collection(cid);
  r = store->apply_transaction(t);
  ASSERT_EQ(r, 0);
}

TEST_P(StoreTest, ScrubTest) {
  coll_t cid("blah");
  int r;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid);
    r = store->apply_transaction(t);
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
    ghobject_t hoid(hobject_t(string(buf) + base, string(), CEPH_NOSNAP, i, 0, ""));
    {
      ObjectStore::Transaction t;
      t.touch(cid, hoid);
      r = store->apply_transaction(t);
      ASSERT_EQ(r, 0);
    }
    created.insert(hoid);
  }

  // Add same hobject_t but different generation or shard_id
  {
    ghobject_t hoid1(hobject_t("same-object", string(), CEPH_NOSNAP, 0, 0, ""));
    ghobject_t hoid2(hobject_t("same-object", string(), CEPH_NOSNAP, 0, 0, ""), (gen_t)1, (shard_id_t)0);
    ghobject_t hoid3(hobject_t("same-object", string(), CEPH_NOSNAP, 0, 0, ""), (gen_t)2, (shard_id_t)0);
    ObjectStore::Transaction t;
    t.touch(cid, hoid1);
    t.touch(cid, hoid2);
    t.touch(cid, hoid3);
    r = store->apply_transaction(t);
    created.insert(hoid1);
    created.insert(hoid2);
    created.insert(hoid3);
    ASSERT_EQ(r, 0);
  }

  vector<ghobject_t> objects;
  r = store->collection_list(cid, objects);
  ASSERT_EQ(r, 0);
  set<ghobject_t> listed(objects.begin(), objects.end());
  cerr << "listed.size() is " << listed.size() << " and created.size() is " << created.size() << std::endl;
  ASSERT_TRUE(listed.size() == created.size());
  objects.clear();
  listed.clear();
  ghobject_t current, next;
  while (1) {
    r = store->collection_list_partial(cid, current, 50, 60,
                                       0, &objects, &next);
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
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  ObjectStore::Transaction t;
  t.remove_collection(cid);
  r = store->apply_transaction(t);
  ASSERT_EQ(r, 0);
}


TEST_P(StoreTest, OMapTest) {
  coll_t cid("blah");
  ghobject_t hoid(hobject_t("tesomap", "", CEPH_NOSNAP, 0, 0, ""));
  int r;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid);
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }

  map<string, bufferlist> attrs;
  {
    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    t.omap_clear(cid, hoid);
    map<string, bufferlist> start_set;
    t.omap_setkeys(cid, hoid, start_set);
    store->apply_transaction(t);
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
    store->apply_transaction(t);
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
    store->apply_transaction(t);

    attrs.erase(to_remove);

    ++i;
  }

  {
    bufferlist bl1;
    bl1.append("omap_header");
    ObjectStore::Transaction t;
    t.omap_setheader(cid, hoid, bl1);
    store->apply_transaction(t);

    bufferlist bl2;
    bl2.append("value");
    map<string, bufferlist> to_add;
    to_add.insert(pair<string, bufferlist>("key", bl2));
    t.omap_setkeys(cid, hoid, to_add);
    store->apply_transaction(t);

    bufferlist bl3;
    map<string, bufferlist> cur_attrs;
    r = store->omap_get(cid, hoid, &bl3, &cur_attrs);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(cur_attrs.size(), size_t(1));
    ASSERT_TRUE(bl3.contents_equal(bl1));
  }

  ObjectStore::Transaction t;
  t.remove(cid, hoid);
  t.remove_collection(cid);
  r = store->apply_transaction(t);
  ASSERT_EQ(r, 0);
}

TEST_P(StoreTest, XattrTest) {
  coll_t cid("blah");
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
    t.create_collection(cid);
    t.touch(cid, hoid);
    r = store->apply_transaction(t);
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
    r = store->apply_transaction(t);
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
    r = store->apply_transaction(t);
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
  ASSERT_GE(r, 0);
  bufferlist bl2;
  bl2.push_back(bp);
  ASSERT_TRUE(bl2 == attrs["attr3"]);

  ObjectStore::Transaction t;
  t.remove(cid, hoid);
  t.remove_collection(cid);
  r = store->apply_transaction(t);
  ASSERT_EQ(r, 0);
}

void colsplittest(
  ObjectStore *store,
  unsigned num_objects,
  unsigned common_suffix_size
  ) {
  coll_t cid("from");
  coll_t tid("to");
  int r = 0;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid);
    r = store->apply_transaction(t);
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
	  0, "")));
    }
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.create_collection(tid);
    t.split_collection(cid, common_suffix_size+1, 0, tid);
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }

  ObjectStore::Transaction t;
  vector<ghobject_t> objects;
  r = store->collection_list(cid, objects);
  ASSERT_EQ(r, 0);
  ASSERT_EQ(objects.size(), num_objects);
  for (vector<ghobject_t>::iterator i = objects.begin();
       i != objects.end();
       ++i) {
    ASSERT_EQ(!(i->hobj.get_hash() & (1<<common_suffix_size)), 0u);
    t.remove(cid, *i);
  }

  objects.clear();
  r = store->collection_list(tid, objects);
  ASSERT_EQ(r, 0);
  ASSERT_EQ(objects.size(), num_objects);
  for (vector<ghobject_t>::iterator i = objects.begin();
       i != objects.end();
       ++i) {
    ASSERT_EQ(i->hobj.get_hash() & (1<<common_suffix_size), 0u);
    t.remove(tid, *i);
  }

  t.remove_collection(cid);
  t.remove_collection(tid);
  r = store->apply_transaction(t);
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
  coll_t cid("asdf");
  int r;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid);
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  std::cout << "Making objects" << std::endl;
  for (int i = 0; i < 360; ++i) {
    ObjectStore::Transaction t;
    ghobject_t o;
    if (i < 8) {
      o.hobj.set_hash((i << 16) | 0xA1);
      t.touch(cid, o);
    }
    o.hobj.set_hash((i << 16) | 0xB1);
    t.touch(cid, o);
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  std::cout << "Removing half" << std::endl;
  for (int i = 1; i < 8; ++i) {
    ObjectStore::Transaction t;
    ghobject_t o;
    o.hobj.set_hash((i << 16) | 0xA1);
    t.remove(cid, o);
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  std::cout << "Checking" << std::endl;
  for (int i = 1; i < 8; ++i) {
    ObjectStore::Transaction t;
    ghobject_t o;
    o.hobj.set_hash((i << 16) | 0xA1);
    bool exists = store->exists(cid, o);
    ASSERT_EQ(exists, false);
  }
  {
    ghobject_t o;
    o.hobj.set_hash(0xA1);
    bool exists = store->exists(cid, o);
    ASSERT_EQ(exists, true);
  }
  std::cout << "Cleanup" << std::endl;
  for (int i = 0; i < 360; ++i) {
    ObjectStore::Transaction t;
    ghobject_t o;
    o.hobj.set_hash((i << 16) | 0xA1);
    t.remove(cid, o);
    o.hobj.set_hash((i << 16) | 0xB1);
    t.remove(cid, o);
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  ObjectStore::Transaction t;
  t.remove_collection(cid);
  r = store->apply_transaction(t);
  ASSERT_EQ(r, 0);
}

TEST_P(StoreTest, MoveRename) {
  coll_t temp_cid("mytemp");
  hobject_t temp_oid("tmp_oid", "", CEPH_NOSNAP, 0, 0, "");
  coll_t cid("dest");
  hobject_t oid("dest_oid", "", CEPH_NOSNAP, 0, 0, "");
  int r;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid);
    t.touch(cid, oid);
    r = store->apply_transaction(t);
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
    t.create_collection(temp_cid);
    t.touch(temp_cid, temp_oid);
    t.write(temp_cid, temp_oid, 0, data.length(), data);
    t.setattr(temp_cid, temp_oid, "attr", attr);
    t.omap_setkeys(temp_cid, temp_oid, omap);
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  ASSERT_TRUE(store->exists(temp_cid, temp_oid));
  {
    ObjectStore::Transaction t;
    t.remove(cid, oid);
    t.collection_move_rename(temp_cid, temp_oid, cid, oid);
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  ASSERT_TRUE(store->exists(cid, oid));
  ASSERT_FALSE(store->exists(temp_cid, temp_oid));
  {
    bufferlist newdata;
    r = store->read(cid, oid, 0, 1000, newdata);
    ASSERT_GE(r, 0);
    ASSERT_TRUE(newdata.contents_equal(data));
    bufferlist newattr;
    r = store->getattr(cid, oid, "attr", newattr);
    ASSERT_GE(r, 0);
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
    t.remove_collection(temp_cid);
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, BigRGWObjectName) {
  store->set_allow_sharded_objects();
  store->sync_and_flush();
  coll_t temp_cid("mytemp");
  hobject_t temp_oid("tmp_oid", "", CEPH_NOSNAP, 0, 0, "");
  coll_t cid("dest");
  ghobject_t oid(
    hobject_t(
      "default.4106.50_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "",
      CEPH_NOSNAP,
      0x81920472,
      3,
      ""),
    15,
    shard_id_t(1));
  ghobject_t oid2(oid);
  oid2.generation = 17;
  ghobject_t oidhead(oid);
  oidhead.generation = ghobject_t::NO_GEN;

  int r;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid);
    t.touch(cid, oidhead);
    t.collection_move_rename(cid, oidhead, cid, oid);
    t.touch(cid, oidhead);
    t.collection_move_rename(cid, oidhead, cid, oid2);
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }

  {
    ObjectStore::Transaction t;
    t.remove(cid, oid);
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }

  {
    vector<ghobject_t> objects;
    r = store->collection_list(cid, objects);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(objects.size(), 1u);
    ASSERT_EQ(objects[0], oid2);
  }

  ASSERT_FALSE(store->exists(cid, oid));

  {
    ObjectStore::Transaction t;
    t.remove(cid, oid2);
    t.remove_collection(cid);
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);

  }
}

TEST_P(StoreTest, SetAllocHint) {
  coll_t cid("alloc_hint");
  ghobject_t hoid(hobject_t("test_hint", "", CEPH_NOSNAP, 0, 0, ""));
  int r;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid);
    t.touch(cid, hoid);
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.set_alloc_hint(cid, hoid, 4*1024*1024, 1024*4);
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.set_alloc_hint(cid, hoid, 4*1024*1024, 1024*4);
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove_collection(cid);
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
}

INSTANTIATE_TEST_CASE_P(
  ObjectStore,
  StoreTest,
  ::testing::Values("memstore", "filestore", "keyvaluestore"));

#else

// Google Test may not support value-parameterized tests with some
// compilers. If we use conditional compilation to compile out all
// code referring to the gtest_main library, MSVC linker will not link
// that library at all and consequently complain about missing entry
// point defined in that library (fatal error LNK1561: entry point
// must be defined). This dummy test keeps gtest_main linked in.
TEST(DummyTest, ValueParameterizedTestsAreNotSupportedOnThisPlatform) {}

#endif


//
// support tests for qa/workunits/filestore/filestore.sh
//
TEST(EXT4StoreTest, _detect_fs) {
  if (::getenv("DISK") == NULL || ::getenv("MOUNTPOINT") == NULL) {
    cerr << "SKIP because DISK and MOUNTPOINT environment variables are not set. It is meant to run from qa/workunits/filestore/filestore.sh " << std::endl;
    return;
  }
  const string disk(::getenv("DISK"));
  EXPECT_LT((unsigned)0, disk.size());
  const string mnt(::getenv("MOUNTPOINT"));
  EXPECT_LT((unsigned)0, mnt.size());
  ::umount(mnt.c_str());

  const string dir("store_test_temp_dir");
  const string journal("store_test_temp_journal");

  //
  // without user_xattr, ext4 fails
  //
  {
    g_ceph_context->_conf->set_val("filestore_xattr_use_omap", "true");
    EXPECT_EQ(::system((string("mount -o loop,nouser_xattr ") + disk + " " + mnt).c_str()), 0);
    EXPECT_EQ(::chdir(mnt.c_str()), 0);
    EXPECT_EQ(::mkdir(dir.c_str(), 0755), 0);
    FileStore store(dir, journal);
    EXPECT_EQ(store._detect_fs(), -ENOTSUP);
    EXPECT_EQ(::chdir(".."), 0);
    EXPECT_EQ(::umount(mnt.c_str()), 0);
  }
  //
  // mounted with user_xattr, ext4 fails if filestore_xattr_use_omap is false
  //
  {
    g_ceph_context->_conf->set_val("filestore_xattr_use_omap", "false");
    EXPECT_EQ(::system((string("mount -o loop,user_xattr ") + disk + " " + mnt).c_str()), 0);
    EXPECT_EQ(::chdir(mnt.c_str()), 0);
    FileStore store(dir, journal);
    EXPECT_EQ(store._detect_fs(), -ENOTSUP);
    EXPECT_EQ(::chdir(".."), 0);
    EXPECT_EQ(::umount(mnt.c_str()), 0);
  }
  //
  // mounted with user_xattr, ext4 succeeds if filestore_xattr_use_omap is true
  //
  {
    g_ceph_context->_conf->set_val("filestore_xattr_use_omap", "true");
    EXPECT_EQ(::system((string("mount -o loop,user_xattr ") + disk + " " + mnt).c_str()), 0);
    EXPECT_EQ(::chdir(mnt.c_str()), 0);
    FileStore store(dir, journal);
    EXPECT_EQ(store._detect_fs(), 0);
    EXPECT_EQ(::chdir(".."), 0);
    EXPECT_EQ(::umount(mnt.c_str()), 0);
  }
}


int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->set_val("osd_journal_size", "400");
  g_ceph_context->_conf->set_val("filestore_index_retry_probability", "0.5");
  g_ceph_context->_conf->set_val("filestore_op_thread_timeout", "1000");
  g_ceph_context->_conf->set_val("filestore_op_thread_suicide_timeout", "10000");
  g_ceph_context->_conf->set_val("filestore_debug_disable_sharded_check", "true");
  g_ceph_context->_conf->set_val("filestore_fiemap", "true");
  g_ceph_context->_conf->set_val(
    "enable_experimental_unrecoverable_data_corrupting_features",
    "keyvaluestore");
  g_ceph_context->_conf->apply_changes(NULL);

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
