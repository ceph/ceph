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

#include <fcntl.h>
#include <glob.h>
#include <stdio.h>
#include <string.h>
#include <iostream>
#include <memory>
#include <time.h>
#include <sys/mount.h>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <boost/random/binomial_distribution.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>

#include "global/global_context.h"
#include "os/ObjectStore.h"
#include "os/bluestore/BlueStore.h"
#include "os/bluestore/BlueFS.h"
#include "include/Context.h"
#include "common/buffer_instrumentation.h"
#include "common/ceph_argparse.h"
#include "common/admin_socket.h"
#include "global/global_init.h"
#include "common/ceph_mutex.h"
#include "common/Cond.h"
#include "common/errno.h"
#include "common/options.h" // for the size literals
#include "common/pretty_binary.h"
#include "include/stringify.h"
#include "include/coredumpctl.h"
#include "include/unordered_map.h"
#include "os/kv.h"
#include "store_test_fixture.h"


using namespace std;
using namespace std::placeholders;

typedef boost::mt11213b gen_type;

const uint64_t DEF_STORE_TEST_BLOCKDEV_SIZE = 102400000000;
#define dout_context g_ceph_context

static uint64_t get_testing_seed(const char* function) {
  char* random_seed = getenv("TEST_RANDOM_SEED");
  uint64_t testing_seed;
  if (random_seed) {
    testing_seed = atoi(random_seed);
  } else {
    testing_seed = time(NULL);
  }
  cout << "seed for " << function << " is " << testing_seed << std::endl;
  return testing_seed;
}

#define TEST_RANDOM_SEED get_testing_seed(__func__)


string key_by_index(uint32_t key_id)
{
  string res;
  uint32_t range = 'z' - 'a' + 1;
  while (key_id > 0) {
    res.push_back(key_id % range + 'a');
    key_id = key_id / range;
  }
  return res;
}

string gen_key(uint32_t key_id)
{
  uint32_t extra_size = 10 - (key_id % 10);
  string key = key_by_index(key_id);
  uint32_t x = key_id;
  uint32_t range = 'z' - 'a' + 1;
  while (extra_size > 0) {
    x = x * 117 + key_id;
    key.push_back(x % range + 'a');
    extra_size--;
  }
  return key;
}

string gen_val(uint32_t val_id)
{
  uint32_t size = 100 - ((val_id * val_id) % 90);
  string val;
  uint32_t x = val_id;
  uint32_t range = 'z' - 'a' + 1;
  while (size > 0) {
    x = x * 1117 + val_id;
    val.push_back(x % range + 'a');
    size--;
  }
  return val;
}

pair<string, string> gen_element(
  uint32_t dur_0,
  uint32_t dur_kv,
  uint32_t cnt_kv,
  uint32_t elem_id,
  uint32_t gen /* 0 ... inf */)
{
  //map<string, string> res;
  uint32_t cycle = dur_0 + dur_kv * cnt_kv;
  uint32_t base = elem_id;
  uint32_t pos = (gen + base) % cycle;
  if (pos < dur_0) {
    return make_pair(string(), string());
  }
  uint32_t in_batch = (pos - dur_0) / dur_kv;
  uint32_t key_index = (gen / cycle) * cnt_kv + in_batch;
  gen_type rng(key_index * 0x5632fec3 + elem_id);
  uint32_t key_id = boost::uniform_int<>(0, (1LL<<31) - 1)(rng);
  uint32_t val_id = boost::uniform_int<>(0, (1LL<<31) - 1)(rng);
  //std::cout << "kid=" << key_id << " vid=" << val_id << std::endl;
  return make_pair(gen_key(key_id), gen_val(val_id));
}

// params to n0... nN-1 set i=0..N-1
// - duration of no element slope a+bi
// - duration of an element slope c+di
// - count of elements            i%A_CONSTANT
// - offset to hash               ?
// - starting position            hash(i)

map<string, string> gen_set(
  uint32_t set_id,
  uint32_t N, // set size
  uint32_t gen /* 0 ... inf */)
{
  map<string, string> result;
  gen_type rng(set_id * set_id * set_id + 117 * gen);
  for (uint32_t i=0; i<N; i++) {
    uint32_t dur_0 = boost::uniform_int<>(5, 15)(rng);
    uint32_t dur_kv = boost::uniform_int<>(5, 15)(rng);
    uint32_t cnt_kv = boost::uniform_int<>(1, 10)(rng);
    uint32_t elem_id = boost::uniform_int<>(0, (1LL << 31) - 1)(rng);

    auto elem = gen_element(dur_0, dur_kv, cnt_kv, elem_id, gen);
    if (!elem.first.empty()) {
      result[elem.first] = elem.second;
    }
  }
  return result;
}




class OmapBench : public StoreTestFixture,
                  public ::testing::WithParamInterface<const char*> {
public:
  OmapBench()
    : StoreTestFixture(GetParam())
     {}

  void SetUp() override
  {
    StoreTestFixture::SetUp();
    poolid = 4373;
    cid = coll_t(spg_t(pg_t(0, poolid), shard_id_t::NO_SHARD));
    ch = store->create_new_collection(cid);
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    int r = store->queue_transaction(ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  void TearDown() override
  {
    print_iter();
    ObjectStore::Transaction t;
    t.remove_collection(cid);
    int r = store->queue_transaction(ch, std::move(t));
    ASSERT_EQ(r, 0);
    StoreTestFixture::TearDown();
  }

  void print_iter()
  {
    double d;
    uint32_t i;
    d = (double)std::chrono::duration<double>(iterator_creation).count();
    i = iterator_creation_cnt;
    std::cout << fmt::format("iter create        {:0.7f}/{:3d}={:0.7f}", d, i, d / i) << std::endl;

    d = (double)std::chrono::duration<double>(iterator_seek_to_first).count();
    i = iterator_seek_to_first_cnt;
    std::cout << fmt::format("iter seek_to_first {:0.7f}/{:3d}={:0.7f}", d, i, d / i) << std::endl;

    d = (double)std::chrono::duration<double>(iterator_lower_bound).count();
    i = iterator_lower_bound_cnt;
    std::cout << fmt::format("iter lower_bound   {:0.7f}/{:3d}={:0.7f}", d, i, d / i) << std::endl;

    d = (double)std::chrono::duration<double>(iterator_upper_bound).count();
    i = iterator_upper_bound_cnt;
    std::cout << fmt::format("iter upper_bound   {:0.7f}/{:3d}={:0.7f}", d, i, d / i) << std::endl;
  }

  void print_and_clear()
  {
    print_iter();
    iterator_creation = signedspan(0);
    iterator_creation_cnt = 0;
    iterator_seek_to_first = signedspan(0);
    iterator_seek_to_first_cnt = 0;
    iterator_lower_bound = signedspan(0);
    iterator_lower_bound_cnt = 0;
    iterator_upper_bound = signedspan(0);
    iterator_upper_bound_cnt = 0;
    //print_iter();
  }

  signedspan iterator_creation = signedspan(0);
  uint32_t   iterator_creation_cnt = 0;
  signedspan iterator_seek_to_first = signedspan(0);
  uint32_t   iterator_seek_to_first_cnt = 0;
  signedspan iterator_lower_bound = signedspan(0);
  uint32_t   iterator_lower_bound_cnt = 0;
  signedspan iterator_upper_bound = signedspan(0);
  uint32_t   iterator_upper_bound_cnt = 0;

  int poolid;
  coll_t cid;
  gen_type rng;

  void apply_diff_to_omap(
    OmapBench* test,
    const map<string, string>& before,
    const map<string, string>& current,
    const coll_t& cid,
    const ghobject_t& hoid
  )
  {
    ObjectStore::Transaction t;
    auto setkeys = [&](const string& k, const string& v) {
      map<string, bufferlist> new_keys;
      bufferlist bl;
      bl.append(v);
      new_keys.emplace(k, bl);
      t.omap_setkeys(cid, hoid, new_keys);
    };
    auto b = before.begin();
    auto c = current.begin();
    while (b != before.end() || c != current.end()) {
      bool remove_before = false;
      bool add_current = false;
      if (b != before.end() && c != current.end()) {
        if (b->first > c->first) {
          remove_before = true;
        } else if (b->first < c->first) {
          add_current = true;
        } else if (b->second != c->second) {
          add_current = true;
        } else {
          ++b;
          ++c;
          continue;
        }
      }
      if (c == current.end() || remove_before) {
        t.omap_rmkey(cid, hoid, b->first);
        ++b;
        continue;
      }
      if (b == before.end() || add_current) {
        // add
        setkeys(c->first, c->second);
        ++c;
        continue;
      }
    }
    int r = test->store->queue_transaction(test->ch, std::move(t));
    ASSERT_EQ(r, 0);
  }



  void print_omap_set(map<string, uint32_t> &omap_set)
  {
    for (auto pos = omap_set.begin(); pos != omap_set.end(); ++pos) {
      std::cout << pos->first << " " << pos->second << std::endl;
    }
  };
  struct Iter2 {
    OmapBench* test = nullptr;
    ObjectMap::ObjectMapIterator iter;// = store->get_omap_iterator(ch, hoid);
    bool seeked = false;
    map<string, string> snap;// = current_omap_set;
    map<string, string>::iterator snap_it;
    Iter2() {}
    Iter2( OmapBench* test,
          const map<string, string>& source,
          ObjectStore::CollectionHandle ch,
          ghobject_t hoid)
    : test(test) {
      //std::cout << "new iterator" << std::endl;
      auto t = mono_clock::now();
      iter = test->store->get_omap_iterator(ch, hoid);
      test->iterator_creation += (mono_clock::now() - t);
      test->iterator_creation_cnt++;
      snap = source;
      seeked = false;
    };
    void seek_to_first() {
      //std::cout << "seek to first" << std::endl;
      auto t = mono_clock::now();
      iter->seek_to_first();
      test->iterator_seek_to_first += (mono_clock::now() - t);
      test->iterator_seek_to_first_cnt++;
      snap_it = snap.begin();
      seeked = true;
    };
    void upper_bound() {
      // jump to after (upper_bound)
      //std::cout << "upper_bound" << std::endl;
      snap_it = snap.begin();
      boost::uniform_int<> go_forward_generator(0, snap.size() - 1);
      uint32_t steps = go_forward_generator(test->rng);
      while (steps > 0) {
        ceph_assert(snap_it != snap.end());
        ++snap_it;
        --steps;
      }
      auto t = mono_clock::now();
      iter->upper_bound(snap_it->first);
      test->iterator_upper_bound += (mono_clock::now() - t);
      test->iterator_upper_bound_cnt++;
      ++snap_it;
      seeked = true;
    };
    void lower_bound() {
      // jump to exactly the element (lower_bound)
      //std::cout << "lower_bound" << std::endl;
      boost::uniform_int<> go_forward_generator(0, snap.size() - 1);
      uint32_t steps = go_forward_generator(test->rng);
      snap_it = snap.begin();
      while (steps > 0) {
        ceph_assert(snap_it != snap.end());
        ++snap_it;
        --steps;
      }
      auto t = mono_clock::now();
      iter->lower_bound(snap_it->first);
      test->iterator_lower_bound += (mono_clock::now() - t);
      test->iterator_lower_bound_cnt++;
      seeked = true;
    };
    void check() {
      // check if iterator gives same as data set
      //std::cout << "check" << std::endl;
      if (seeked) {
        for (int i = 0; i < 10; i++) {
          ASSERT_EQ(snap_it != snap.end(), iter->valid());
          if (!iter->valid()) break;
          ASSERT_EQ(snap_it->first,iter->key());
          ASSERT_EQ(snap_it->second, iter->value().to_str());
          iter->next();
          ++snap_it;
        }
      };
    };
  };
  void do_iter_action(
    Iter2& iter,
    uint32_t seek_to_first,
    uint32_t upper_bound,
    uint32_t lower_bound,
    uint32_t check)
  {
    // sometimes snapshot iterator
    // sometimes check some elements in the iterator
    // sometimes jump to beginning
    // sometimes jump to after some element (upper_bound)
    // sometimes jump exactly to an element (lower_bound)
    boost::uniform_int<> dataset_action_generator(0,
      seek_to_first + upper_bound + lower_bound + check - 1);
    uint32_t action = dataset_action_generator(rng);
    if (action < seek_to_first) {
      iter.seek_to_first();
    } else if (action < seek_to_first + upper_bound) {
      iter.upper_bound();
    } else if (action < seek_to_first + upper_bound + lower_bound) {
      iter.lower_bound();
    } else {
      iter.check();
    }
  }

  void print_omap_size()
  {
    BlueStore* bs = dynamic_cast<BlueStore*>(store.get());
    if (bs) {
      uint64_t omap_size = bs->get_kv()->estimate_prefix_size("p","");
      std::cout << "omap_size = " << omap_size << std::endl;
    } else {
      std::cout << "!not bluestore!" << std::endl;
    }
  }
};





TEST_P(OmapBench, lightOMAP_10M_onode_100_omap_1_iter)
{
  uint32_t OBJECTS = 0;
  rng = gen_type(TEST_RANDOM_SEED);
  std::vector<uint32_t> object_gen;
  auto hobj = [&](uint32_t id) -> ghobject_t {
    string name = "test-"+to_string(id);
    return ghobject_t(hobject_t(name, "", CEPH_NOSNAP, id, 0, ""));
  };
  auto more_objects = [&](uint32_t more_cnt)
  {
    ObjectStore::Transaction t;
    for (uint32_t i = 0; i < more_cnt; i++) {
      uint32_t pos = object_gen.size();
      ghobject_t hh = hobj(pos);
      t.touch(cid, hh);
      int r = store->queue_transaction(ch, std::move(t));
      ASSERT_EQ(r, 0);
      apply_diff_to_omap(this,
        map<string, string>(), gen_set(pos, 100, 0),
        cid, hh);
      object_gen.push_back(0);
      OBJECTS++;
    }
  };

  boost::uniform_int<> dataset_action_generator(0, 2 + 10 + 1000 - 1);
  Iter2 iter;
  for (int m = 0; m < 1000; m++) {
    auto t0 = mono_clock::now();
    more_objects(10000);

    boost::uniform_int<> onode_selector(0, OBJECTS - 1);
    for (int j = 0; j < 10000; j++) {
      uint32_t o = onode_selector(rng);
      uint32_t action = dataset_action_generator(rng);
      if (action < 2) {
        // change iterator
        iter = Iter2(this, gen_set(o, 100, object_gen[o]), ch, hobj(o));
      } else if (action < 10) {
        if (iter.test)
        do_iter_action(iter, 10, 10, 10, 10);
      } else {
        auto prev = gen_set(o, 100, object_gen[o]);
        object_gen[o]++;
        auto now = gen_set(o, 100, object_gen[o]);
        apply_diff_to_omap(this, prev, now, cid, hobj(o));
      }
    }
    auto t1 = mono_clock::now();
    std::cout << "objects=" << OBJECTS << " time=" << (t1 - t0) << std::endl;
    print_omap_size();
    print_and_clear();
  }

  ObjectStore::Transaction t;
  for (uint32_t i = 0; i < OBJECTS; i++) {
    t.remove(cid, hobj(i));
    if ((i % 10000) == 0) {
      std::cout << "del " << i << std::endl;
      int r = store->queue_transaction(ch, std::move(t));
      ASSERT_EQ(r, 0);
      t = ObjectStore::Transaction();
    }
  }
}


TEST_P(OmapBench, lightOMAP_100M_onode_10_omap_1_iter)
{
  uint32_t OBJECTS = 0;
  rng = gen_type(TEST_RANDOM_SEED);
  std::vector<uint32_t> object_gen;
  auto hobj = [&](uint32_t id) -> ghobject_t {
    string name = "test-"+to_string(id);
    return ghobject_t(hobject_t(name, "", CEPH_NOSNAP, id, 0, ""));
  };
  auto more_objects = [&](uint32_t more_cnt)
  {
    ObjectStore::Transaction t;
    for (uint32_t i = 0; i < more_cnt; i++) {
      uint32_t pos = object_gen.size();
      ghobject_t hh = hobj(pos);
      t.touch(cid, hh);
      int r = store->queue_transaction(ch, std::move(t));
      ASSERT_EQ(r, 0);
      apply_diff_to_omap(this,
        map<string, string>(), gen_set(pos, 10, 0),
        cid, hh);
      object_gen.push_back(0);
      OBJECTS++;
    }
  };

  boost::uniform_int<> dataset_action_generator(0, 2 + 10 + 1000 - 1);
  Iter2 iter;
  for (int m = 0; m < 1000; m++) {
    auto t0 = mono_clock::now();
    more_objects(100000);

    boost::uniform_int<> onode_selector(0, OBJECTS - 1);
    for (int j = 0; j < 10000; j++) {
      uint32_t o = onode_selector(rng);
      uint32_t action = dataset_action_generator(rng);
      if (action < 2) {
        // change iterator
        iter = Iter2(this, gen_set(o, 10, object_gen[o]), ch, hobj(o));
      } else if (action < 10) {
        if (iter.test)
        do_iter_action(iter, 10, 10, 10, 10);
      } else {
        auto prev = gen_set(o, 10, object_gen[o]);
        object_gen[o]++;
        auto now = gen_set(o, 10, object_gen[o]);
        apply_diff_to_omap(this, prev, now, cid, hobj(o));
      }
    }
    auto t1 = mono_clock::now();
    std::cout << "objects=" << OBJECTS << " time=" << (t1 - t0) << std::endl;
    print_omap_size();
    print_and_clear();
  }

  ObjectStore::Transaction t;
  for (uint32_t i = 0; i < OBJECTS; i++) {
    t.remove(cid, hobj(i));
    if ((i % 10000) == 0) {
      std::cout << "del " << i << std::endl;
      int r = store->queue_transaction(ch, std::move(t));
      ASSERT_EQ(r, 0);
      t = ObjectStore::Transaction();
    }
  }
}

TEST_P(OmapBench, lightOMAP_1M_onode_1000_omap_1_iter)
{
  uint32_t OBJECTS = 0;
  rng = gen_type(TEST_RANDOM_SEED);
  std::vector<uint32_t> object_gen;
  auto hobj = [&](uint32_t id) -> ghobject_t {
    string name = "test-"+to_string(id);
    return ghobject_t(hobject_t(name, "", CEPH_NOSNAP, id, poolid, ""));
  };
  auto more_objects = [&](uint32_t more_cnt)
  {
    ObjectStore::Transaction t;
    for (uint32_t i = 0; i < more_cnt; i++) {
      uint32_t pos = object_gen.size();
      ghobject_t hh = hobj(pos);
      t.touch(cid, hh);
      int r = store->queue_transaction(ch, std::move(t));
      ASSERT_EQ(r, 0);
      apply_diff_to_omap(this,
        map<string, string>(), gen_set(pos, 1000, 0),
        cid, hh);
      object_gen.push_back(0);
      OBJECTS++;
    }
  };

  boost::uniform_int<> dataset_action_generator(0, 2 + 10 + 100 - 1);
  Iter2 iter;
  for (int m = 0; m < 1000; m++) {
    auto t0 = mono_clock::now();
    more_objects(1000);

    boost::uniform_int<> onode_selector(0, OBJECTS - 1);
    for (int j = 0; j < 1000; j++) {
      uint32_t o = onode_selector(rng);
      uint32_t action = dataset_action_generator(rng);
      if (action < 2) {
        // change iterator
        iter = Iter2(this, gen_set(o, 1000, object_gen[o]), ch, hobj(o));
      } else if (action < 10) {
        if (iter.test)
        do_iter_action(iter, 10, 10, 10, 10);
      } else {
        auto prev = gen_set(o, 1000, object_gen[o]);
        object_gen[o]++;
        auto now = gen_set(o, 1000, object_gen[o]);
        apply_diff_to_omap(this, prev, now, cid, hobj(o));
      }
    }
    auto t1 = mono_clock::now();
    std::cout << "objects=" << OBJECTS << " time=" << (t1 - t0) << std::endl;
    print_omap_size();
    print_and_clear();
  }

  ObjectStore::Transaction t;
  for (uint32_t i = 0; i < OBJECTS; i++) {
    t.remove(cid, hobj(i));
    if ((i % 10000) == 0) {
      std::cout << "del " << i << std::endl;
      int r = store->queue_transaction(ch, std::move(t));
      ASSERT_EQ(r, 0);
      t = ObjectStore::Transaction();
    }
  }
}



INSTANTIATE_TEST_SUITE_P(
  ObjectStore,
  OmapBench,
  ::testing::Values(
    "memstore",
    "bluestore",
    "kstore"));

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
  g_ceph_context->_conf.set_val_or_die("bluestore_max_alloc_size", "196608");
  // set small cache sizes so we see trimming during Synthetic tests
  g_ceph_context->_conf.set_val_or_die("bluestore_cache_size_hdd", "4000000");
  g_ceph_context->_conf.set_val_or_die("bluestore_cache_size_ssd", "4000000");
  g_ceph_context->_conf.set_val_or_die(
  "bluestore_debug_inject_allocation_from_file_failure", "0.66");

  // specify device size
  g_ceph_context->_conf.set_val_or_die("bluestore_block_size",
    stringify(DEF_STORE_TEST_BLOCKDEV_SIZE));

  g_ceph_context->_conf.set_val_or_die(
    "enable_experimental_unrecoverable_data_corrupting_features", "*");
  g_ceph_context->_conf.apply_changes(nullptr);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

