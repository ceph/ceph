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
#include <string>
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

const uint64_t DEF_STORE_TEST_BLOCKDEV_SIZE = 1024000000000;
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

string gen_key(uint32_t key_id, uint32_t key_num)
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
  return key + to_string(key_num);
}

string gen_val(
  uint32_t val_id,
  uint32_t min_size,
  uint32_t max_size)
{
  uint32_t size = max_size - ((val_id * val_id) % (max_size - min_size));
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

struct element_t {
  uint32_t key_id;
  uint32_t val_id;
  bool valid() {
    return (key_id != 0) && (val_id != 0);
  }
};

element_t gen_element_spec(
  uint32_t dur_0,
  uint32_t dur_kv,
  uint32_t cnt_kv,
  uint32_t elem_id,
  uint32_t gen) /* 0 ... inf */
{
  uint32_t cycle = dur_0 + dur_kv * cnt_kv;
  uint32_t base = elem_id;
  uint32_t pos = (gen + base) % cycle;
  if (pos < dur_0) {
    return {0, 0};
  }
  uint32_t in_batch = (pos - dur_0) / dur_kv;
  uint32_t key_index = (gen / cycle) * cnt_kv + in_batch;
  uint32_t key_id = key_index * 0x5632fec3 + elem_id;
  uint32_t val_id = key_index * 0x25173c61 + elem_id*elem_id;
  return {key_id, val_id};
}


// params to n0... nN-1 set i=0..N-1
// - duration of no element slope a+bi
// - duration of an element slope c+di
// - count of elements            i%A_CONSTANT
// - offset to hash               ?
// - starting position            hash(i)

gen_type set_init(
  uint32_t set_id,
  uint32_t gen)
{
  return gen_type(set_id * set_id * set_id + 117 * gen);
}

element_t set_get_next(
  gen_type& rng,
  uint32_t gen)
{
  uint32_t dur_0 = boost::uniform_int<>(5, 15)(rng);
  uint32_t dur_kv = boost::uniform_int<>(5, 15)(rng);
  uint32_t cnt_kv = boost::uniform_int<>(1, 10)(rng);
  uint32_t elem_id = boost::uniform_int<>(0, (1LL << 31) - 1)(rng);
  element_t e = gen_element_spec(dur_0, dur_kv, cnt_kv,
    elem_id, gen);
  return e;
}

map<string, string> gen_set(
  uint32_t set_id,
  uint32_t N, // set size
  uint32_t gen, /* 0 ... inf */
  uint32_t val_min_size,
  uint32_t val_max_size)
{
  map<string, string> result;
  gen_type rng = set_init(set_id, gen);

  for (uint32_t i=0; i<N; i++) {
    element_t e = set_get_next(rng, gen);
    if (e.valid()) {
      result[gen_key(e.key_id, i)] = gen_val(e.val_id, val_min_size, val_max_size);
    }
  }
  return result;
}

struct omap_bench_test_t {
  const char* store_name;
  uint32_t target_object_count;
  uint32_t avg_omap_per_object;
  uint32_t add_batch_size = 1000;
  uint32_t modify_ops = 1000;
  uint32_t iterator_ops = 50;
  uint32_t val_min_size = 100;
  uint32_t val_max_size = 1000;
};
void PrintTo(const omap_bench_test_t& t, ::std::ostream* os)
{
  *os << t.store_name << "/total_obj=" << t.target_object_count
      << "/omap_per_obj=" << t.avg_omap_per_object
      << "/batch_add=" << t.add_batch_size
      << "/mod_ops=" << t.modify_ops
      << "/val=" << t.val_min_size << ".." << t.val_max_size;
}

class OmapBench : public StoreTestFixture,
                  public ::testing::WithParamInterface<omap_bench_test_t> {
public:
  OmapBench()
    : StoreTestFixture(GetParam().store_name)
     {}

  void SetUp() override
  {
    StoreTestFixture::SetUp();
    poolid = 4373;
    pgs = 64;
    for (uint32_t i = 0; i < pgs; i++) {
      cid.push_back(coll_t(spg_t(pg_t(i, poolid), shard_id_t::NO_SHARD)));
      ch.push_back(store->create_new_collection(cid.back()));
      ObjectStore::Transaction t;
      t.create_collection(cid.back(), 6);
      int r = store->queue_transaction(ch.back(), std::move(t));
      ASSERT_EQ(r, 0);
    }
  }

  void TearDown() override
  {
    for (uint32_t i = 0; i < pgs; i++) {
      ObjectStore::Transaction t;
      t.remove_collection(cid[i]);
      int r = store->queue_transaction(ch[i], std::move(t));
      ASSERT_EQ(r, 0);
    }
    StoreTestFixture::TearDown();
  }

  class Transaction_Waiter : public Context {
  protected:
    ceph::mutex lock;              ///< Mutex to take
    ceph::condition_variable cond; ///< Cond to signal
    std::atomic_uint32_t in_flight = 0;

  public:
    Transaction_Waiter() : Transaction_Waiter("C_SaferCond") {}
    explicit Transaction_Waiter(const std::string &name)
        : lock(ceph::make_mutex(name)) {}

    Transaction_Waiter* start() {
      ++in_flight;
      return this;
    }
    void finish(int r) override { complete(r); }

    /// We overload complete in order to not delete the context
    void complete(int r) override {
      if (--in_flight == 0) {
        std::lock_guard l(lock);
        cond.notify_all();
      }
    }

    /// Returns rval once the Context is called
    void wait() {
      std::unique_lock l{lock};
      cond.wait(l, [this] { return in_flight != 0; });
      return;
    }

    /// Wait until the \c secs expires or \c complete() is called
    bool wait_for(double secs) { return wait_for(ceph::make_timespan(secs)); }

    bool wait_for(ceph::timespan secs) {
      if (in_flight == 0) {
        return true;
      }
      std::unique_lock l{lock};
      if (cond.wait_for(l, secs, [this] { return in_flight.load() != 0; })) {
        return true;
      } else {
        return false; // ETIMEDOUT;
      }
    }
  };


  void print_iter()
  {
    double d;
    uint32_t i;
    d = (double)std::chrono::duration<double>(iterator_creation).count();
    i = iterator_creation_cnt;
    std::cout << fmt::format("iter create          {:0.7f}/{:3d}={:0.7f}", d, i, d / i) << std::endl;

    d = (double)std::chrono::duration<double>(iterator_seek_to_first).count();
    i = iterator_seek_to_first_cnt;
    std::cout << fmt::format("iter seek_first      {:0.7f}/{:3d}={:0.7f}", d, i, d / i) << std::endl;

    d = (double)std::chrono::duration<double>(iterator_seek_to_first_2nd).count();
    i = iterator_seek_to_first_2nd_cnt;
    std::cout << fmt::format("iter seek_first_2nd  {:0.7f}/{:3d}={:0.7f}", d, i, d / i) << std::endl;

    d = (double)std::chrono::duration<double>(iterator_lower_bound).count();
    i = iterator_lower_bound_cnt;
    std::cout << fmt::format("iter lower_bound     {:0.7f}/{:3d}={:0.7f}", d, i, d / i) << std::endl;

    d = (double)std::chrono::duration<double>(iterator_lower_bound_2nd).count();
    i = iterator_lower_bound_2nd_cnt;
    std::cout << fmt::format("iter lower_bound_2nd {:0.7f}/{:3d}={:0.7f}", d, i, d / i) << std::endl;

    d = (double)std::chrono::duration<double>(iterator_upper_bound).count();
    i = iterator_upper_bound_cnt;
    std::cout << fmt::format("iter upper_bound     {:0.7f}/{:3d}={:0.7f}", d, i, d / i) << std::endl;

    d = (double)std::chrono::duration<double>(iterator_upper_bound_2nd).count();
    i = iterator_upper_bound_2nd_cnt;
    std::cout << fmt::format("iter upper_bound_2nd {:0.7f}/{:3d}={:0.7f}", d, i, d / i) << std::endl;

    d = (double)std::chrono::duration<double>(iterator_next).count();
    i = iterator_next_cnt;
    std::cout << fmt::format("iter next            {:0.7f}/{:3d}={:0.7f}", d, i, d / i) << std::endl;
  }

  void print_and_clear()
  {
    print_iter();
    iterator_creation = signedspan(0);
    iterator_creation_cnt = 0;
    iterator_seek_to_first = signedspan(0);
    iterator_seek_to_first_cnt = 0;
    iterator_seek_to_first_2nd = signedspan(0);
    iterator_seek_to_first_2nd_cnt = 0;
    iterator_lower_bound = signedspan(0);
    iterator_lower_bound_cnt = 0;
    iterator_lower_bound_2nd = signedspan(0);
    iterator_lower_bound_2nd_cnt = 0;
    iterator_upper_bound = signedspan(0);
    iterator_upper_bound_cnt = 0;
    iterator_upper_bound_2nd = signedspan(0);
    iterator_upper_bound_2nd_cnt = 0;
    iterator_next = signedspan(0);
    iterator_next_cnt = 0;
  }

  signedspan iterator_creation = signedspan(0);
  uint32_t   iterator_creation_cnt = 0;
  signedspan iterator_seek_to_first = signedspan(0);
  uint32_t   iterator_seek_to_first_cnt = 0;
  signedspan iterator_seek_to_first_2nd = signedspan(0);
  uint32_t   iterator_seek_to_first_2nd_cnt = 0;
  signedspan iterator_lower_bound = signedspan(0);
  uint32_t   iterator_lower_bound_cnt = 0;
  signedspan iterator_lower_bound_2nd = signedspan(0);
  uint32_t   iterator_lower_bound_2nd_cnt = 0;
  signedspan iterator_upper_bound_2nd = signedspan(0);
  uint32_t   iterator_upper_bound_2nd_cnt = 0;
  signedspan iterator_upper_bound = signedspan(0);
  uint32_t   iterator_upper_bound_cnt = 0;
  signedspan iterator_next = signedspan(0);
  uint32_t   iterator_next_cnt = 0;

  uint32_t poolid;
  uint32_t pgs = 64;
  std::vector<coll_t> cid;
  std::vector<ObjectStore::CollectionHandle> ch;
  gen_type rng;
  Transaction_Waiter waiter;

  void apply_diff_to_omap(
    OmapBench* test,
    const map<string, string>& before,
    const map<string, string>& current,
    const coll_t& cid,
    ObjectStore::CollectionHandle ch,
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
    t.register_on_commit(waiter.start());
    int r = test->store->queue_transaction(ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  void apply_advance_to_omap(
    OmapBench* test,
    uint32_t o_id,
    uint32_t o_gen,
    uint32_t N,
    uint32_t val_min_size,
    uint32_t val_max_size,
    const coll_t& cid,
    ObjectStore::CollectionHandle ch,
    const ghobject_t& hoid)
  {
    ObjectStore::Transaction t;
    auto setkeys = [&](const string& k, const string& v) {
      map<string, bufferlist> new_keys;
      bufferlist bl;
      bl.append(v);
      new_keys.emplace(k, bl);
      t.omap_setkeys(cid, hoid, new_keys);
    };
    gen_type prev = set_init(o_id, o_gen);
    gen_type next = set_init(o_id, o_gen + 1);
    for (uint32_t i = 0; i < N; i++) {
      element_t p = set_get_next(prev, o_gen);
      element_t n = set_get_next(next, o_gen + 1);
      if (p.valid()) {
        if (n.valid()) {
          if (p.key_id != n.key_id) {
            t.omap_rmkey(cid, hoid, gen_key(p.key_id, i));
          }
          if (p.val_id != n.val_id) {
            setkeys(gen_key(n.key_id, i), gen_val(n.val_id, val_min_size, val_max_size));
          }
        } else {
          t.omap_rmkey(cid, hoid, gen_key(p.key_id, i));
        }
      } else {
        if (n.valid()) {
          setkeys(gen_key(n.key_id, i), gen_val(n.val_id, val_min_size, val_max_size));
        }
      }
    }
    t.register_on_commit(waiter.start());
    int r = test->store->queue_transaction(ch, std::move(t));
    ASSERT_EQ(r, 0);
  }



  void print_omap_set(map<string, uint32_t> &omap_set)
  {
    for (auto pos = omap_set.begin(); pos != omap_set.end(); ++pos) {
      std::cout << pos->first << " " << pos->second << std::endl;
    }
  };

  struct Iter {
    OmapBench* test = nullptr;
    ObjectMap::ObjectMapIterator iter;
    bool seeked = false;
    map<string, string> snap;
    map<string, string>::iterator snap_it;
    Iter() {}
    Iter( OmapBench* test,
          const map<string, string>& source,
          ObjectStore::CollectionHandle ch,
          ghobject_t hoid)
    : test(test) {
      auto t = mono_clock::now();
      iter = test->store->get_omap_iterator(ch, hoid);
      test->iterator_creation += (mono_clock::now() - t);
      test->iterator_creation_cnt++;
      snap = source;
      seeked = false;
    };
    void seek_to_first() {
      auto t = mono_clock::now();
      iter->seek_to_first();
      if (!seeked) {
        test->iterator_seek_to_first += (mono_clock::now() - t);
        test->iterator_seek_to_first_cnt++;
      } else {
        test->iterator_seek_to_first_2nd += (mono_clock::now() - t);
        test->iterator_seek_to_first_2nd_cnt++;
      }
      snap_it = snap.begin();
      seeked = true;
    };
    void upper_bound() {
      // jump to after (upper_bound)
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
      if (!seeked) {
        test->iterator_upper_bound += (mono_clock::now() - t);
        test->iterator_upper_bound_cnt++;
      } else {
        test->iterator_upper_bound_2nd += (mono_clock::now() - t);
        test->iterator_upper_bound_2nd_cnt++;
      }
      ++snap_it;
      seeked = true;
    };
    void lower_bound() {
      // jump to exactly the element (lower_bound)
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
      if (!seeked) {
        test->iterator_lower_bound += (mono_clock::now() - t);
        test->iterator_lower_bound_cnt++;
      } else {
        test->iterator_lower_bound_2nd += (mono_clock::now() - t);
        test->iterator_lower_bound_2nd_cnt++;
      }
      seeked = true;
    };
    void check() {
      // check if iterator gives same as data set
      if (seeked) {
        for (int i = 0; i < 10; i++) {
          ASSERT_EQ(snap_it != snap.end(), iter->valid());
          if (!iter->valid()) break;
          ASSERT_EQ(snap_it->first,iter->key());
          ASSERT_EQ(snap_it->second, iter->value().to_str());
          auto t = mono_clock::now();
          iter->next();
          test->iterator_next += (mono_clock::now() - t);
          test->iterator_next_cnt++;
          ++snap_it;
        }
      };
    };
  };
  void do_iter_action(
    Iter& iter,
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


TEST_P(OmapBench, expanding_and_modifying)
{
  rng = gen_type(TEST_RANDOM_SEED);
  PrintTo(GetParam(), &std::cout);
  std::cout << std::endl;
  uint32_t objects = 0;
  uint32_t target_objects = GetParam().target_object_count;
  uint32_t avg_omap_per_object = GetParam().avg_omap_per_object;
  uint32_t add_batch_size = GetParam().add_batch_size;
  uint32_t modify_ops = GetParam().modify_ops;
  uint32_t iterator_ops = GetParam().iterator_ops;
  uint32_t val_min_size = GetParam().val_min_size;
  uint32_t val_max_size = GetParam().val_max_size;

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
      t.touch(cid[pos % pgs], hh);
      t.register_on_commit(waiter.start());
      int r = store->queue_transaction(ch[pos % pgs], std::move(t));
      ASSERT_EQ(r, 0);
      map<string,string> new_set = gen_set(pos, avg_omap_per_object, 0, val_min_size, val_max_size);
      apply_diff_to_omap(this, map<string, string>(), new_set,
        cid[pos % pgs], ch[pos % pgs], hh);
      object_gen.push_back(0);
      objects++;
    }
    if (!waiter.wait_for(10)) {
      std::cout << "waiting timeout!" << std::endl;
    }
  };

  Iter iter;
  while (objects < target_objects) {
    auto t0 = mono_clock::now();
    more_objects(add_batch_size);

    boost::uniform_int<> onode_selector(0, objects - 1);
    for (uint32_t j = 0; j < modify_ops + iterator_ops; j++) {
      uint32_t o = onode_selector(rng);
      if (j % (modify_ops / iterator_ops) == 0) {
        if (!waiter.wait_for(10)) {
          std::cout << "waiting timeout!" << std::endl;
        }
        boost::uniform_int<> iterator_action_generator(0, 10);
        uint32_t action = iterator_action_generator(rng);
        if (action < 2 || !iter.test) {
        // change iterator
          iter = Iter(this,
            gen_set(o, avg_omap_per_object, object_gen[o], val_min_size, val_max_size),
            ch[o % pgs], hobj(o));
        }
        if (action >= 2) {
          do_iter_action(iter, 10, 10, 10, 10);
        }
      } else {
        apply_advance_to_omap(this, o, object_gen[o], avg_omap_per_object,
          val_min_size, val_max_size, cid[o % pgs], ch[o % pgs], hobj(o));
        object_gen[o]++;
      }
    }
    auto t1 = mono_clock::now();
    std::cout << "objects=" << objects << " time=" << (t1 - t0) << std::endl;
    print_omap_size();
    print_and_clear();
  }

  {
    std::vector<ObjectStore::Transaction> t;
    t.resize(100);
    for (uint32_t i = 0; i < objects; i++) {
      t[i % pgs].remove(cid[i % pgs], hobj(i));
      if ((i % 10000) == 0 || i == objects - 1) {
        std::cout << "del " << i << std::endl;
        for (uint32_t j = 0; j < pgs; ++j) {
          t[j].register_on_commit(waiter.start());
          int r = store->queue_transaction(ch[j], std::move(t[j]));
          ASSERT_EQ(r, 0);
          t[j] = ObjectStore::Transaction();
        }
        if (!waiter.wait_for(10)) {
          std::cout << "waiting timeout!" << std::endl;
        }
      }
    }
  }

}

INSTANTIATE_TEST_SUITE_P(
  ObjectStore,
  OmapBench,
  ::testing::Values(
    omap_bench_test_t{"bluestore", 1 * 1000 * 1000, 1000, 1000,  1000,  100, 10,  100},
    omap_bench_test_t{"bluestore", 10 * 1000 * 1000, 100, 3000,  3000,  100, 10,  100},
    omap_bench_test_t{"bluestore", 100 * 1000 * 1000, 10, 10000, 10000, 100, 10,  100},
    omap_bench_test_t{"bluestore", 1 * 1000 * 1000, 1000, 1000,  1000,  100, 100, 900},
    omap_bench_test_t{"bluestore", 10 * 1000 * 1000, 100, 3000,  3000,  100, 100, 900},
    omap_bench_test_t{"bluestore", 100 * 1000 * 1000, 10, 10000, 10000, 100, 100, 900}
  ));

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

  // specify device size
  g_ceph_context->_conf.set_val_or_die("bluestore_block_size",
    stringify(DEF_STORE_TEST_BLOCKDEV_SIZE));

  g_ceph_context->_conf.set_val_or_die(
    "enable_experimental_unrecoverable_data_corrupting_features", "*");
  g_ceph_context->_conf.apply_changes(nullptr);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

