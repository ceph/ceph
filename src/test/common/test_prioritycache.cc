// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "common/PriorityCache.h"
#include "global/global_context.h"
#include "gtest/gtest.h"

#include <memory>
#include <string>

// The autotuner (PriorityCache::Manager) hands a pool of memory to a set of
// caches: at each priority it serves what the caches request, and at the last
// priority it splits whatever is left by ratio. These tests drive the real
// Manager with fake caches that ask for a fixed number of bytes per priority.

namespace {

constexpr uint64_t MiB = 1ull << 20;

struct FakeCache : public PriorityCache::PriCache {
  std::string name;
  double ratio;
  int64_t want[PriorityCache::Priority::LAST + 1] = {0};
  int64_t bytes[PriorityCache::Priority::LAST + 1] = {0};
  int64_t committed = 0;
  FakeCache(std::string n, double r) : name(n), ratio(r) {}
  int64_t request_cache_bytes(PriorityCache::Priority pri, uint64_t) const override {
    return want[pri] > bytes[pri] ? want[pri] - bytes[pri] : 0;
  }
  int64_t get_cache_bytes(PriorityCache::Priority pri) const override { return bytes[pri]; }
  int64_t get_cache_bytes() const override {
    int64_t t = 0;
    for (auto b : bytes) t += b;
    return t;
  }
  void set_cache_bytes(PriorityCache::Priority pri, int64_t b) override { bytes[pri] = b; }
  void add_cache_bytes(PriorityCache::Priority pri, int64_t b) override { bytes[pri] += b; }
  int64_t commit_cache_size(uint64_t total) override {
    return committed = PriorityCache::get_chunk(get_cache_bytes(), total);
  }
  int64_t get_committed_size() const override { return committed; }
  double get_cache_ratio() const override { return ratio; }
  void set_cache_ratio(double r) override { ratio = r; }
  std::string get_cache_name() const override { return name; }
  void shift_bins() override {}
  void import_bins(const std::vector<uint64_t>&) override {}
  void set_bins(PriorityCache::Priority, uint64_t) override {}
  uint64_t get_bins(PriorityCache::Priority) const override { return 0; }
};

// rbd-mirror's cache: it owns one or two priorities and asserts on
// add_cache_bytes() at any other, and it runs with a ratio of 0
struct StrictCache : public FakeCache {
  PriorityCache::Priority owned;
  StrictCache(std::string n, PriorityCache::Priority p) : FakeCache(n, 0), owned(p) {}
  void add_cache_bytes(PriorityCache::Priority pri, int64_t b) override {
    ASSERT_EQ(owned, pri) << name << " was handed " << b << " bytes at priority " << pri;
    FakeCache::add_cache_bytes(pri, b);
  }
};

// BlueStore's four caches under a 2 GiB osd_memory_target: 256 MiB base,
// 15 % fragmentation allowance, so a 1484.8 MiB pool; meta and kv ratios
// 0.30, kv_onode 0.04, data the rest.
struct Tuner {
  static constexpr uint64_t POOL = 1484 * MiB + 819 * 1024;
  static int instances;
  // reserve_extra true, as BlueStore constructs it; a distinct perf counter
  // name per instance, since one test may hold two tuners
  PriorityCache::Manager pcm{g_ceph_context, POOL, POOL, 64 * POOL, true,
                             "test-pricache-" + std::to_string(instances++)};
  std::shared_ptr<FakeCache> data{new FakeCache("data", 0.36)};
  std::shared_ptr<FakeCache> meta{new FakeCache("meta", 0.30)};
  std::shared_ptr<FakeCache> kv{new FakeCache("kv", 0.30)};
  std::shared_ptr<FakeCache> kv_onode{new FakeCache("kv_onode", 0.04)};
  Tuner() {
    // with perf counters: balance() commits only the caches that have them
    pcm.insert("data", data, true);
    pcm.insert("meta", meta, true);
    pcm.insert("kv", kv, true);
    pcm.insert("kv_onode", kv_onode, true);
    pcm.tune_memory();   // min == max: the pool, whatever this process maps
    kv->want[PriorityCache::Priority::PRI0] = 64 * MiB;        // RocksDB always
    kv_onode->want[PriorityCache::Priority::PRI0] = 64 * MiB;  // asks a chunk at PRI0
  }
  // what balance() has to hand out: the pool less a chunk per cache
  static int64_t avail() {
    return POOL - 4 * PriorityCache::get_chunk(1, POOL);
  }
  int64_t granted() const {
    return data->get_cache_bytes() + meta->get_cache_bytes() +
           kv->get_cache_bytes() + kv_onode->get_cache_bytes();
  }
};
int Tuner::instances = 0;

} // anonymous namespace

TEST(PriorityCache, pool_is_handed_out_in_full) {
  // nothing asked at LAST: the whole remainder is split by ratio, and the
  // pool is handed out in full (within the integer truncation of the shares)
  Tuner t;
  t.meta->want[PriorityCache::Priority::PRI2] = 50 * MiB;
  t.data->want[PriorityCache::Priority::PRI3] = 200 * MiB;
  t.pcm.balance();
  EXPECT_EQ(Tuner::POOL, t.pcm.get_tuned_mem());
  EXPECT_EQ(50 * MiB, t.meta->bytes[PriorityCache::Priority::PRI2]);
  EXPECT_EQ(200 * MiB, t.data->bytes[PriorityCache::Priority::PRI3]);
  EXPECT_EQ(64 * MiB, t.kv->bytes[PriorityCache::Priority::PRI0]);
  EXPECT_EQ(64 * MiB, t.kv_onode->bytes[PriorityCache::Priority::PRI0]);
  int64_t rest = Tuner::avail() - (50 + 200 + 64 + 64) * MiB;
  EXPECT_EQ(static_cast<int64_t>(rest * 0.36), t.data->bytes[PriorityCache::Priority::LAST]);
  EXPECT_EQ(static_cast<int64_t>(rest * 0.30), t.meta->bytes[PriorityCache::Priority::LAST]);
  EXPECT_EQ(static_cast<int64_t>(rest * 0.30), t.kv->bytes[PriorityCache::Priority::LAST]);
  EXPECT_EQ(static_cast<int64_t>(rest * 0.04), t.kv_onode->bytes[PriorityCache::Priority::LAST]);
  EXPECT_NEAR(Tuner::avail(), t.granted(), 4096);
}

TEST(PriorityCache, request_at_last_is_credited) {
  // a cache with content older than its last age bin asks for it at LAST
  // (BlueStore's onode and buffer caches do). The grant must stay with the
  // cache, the ratio split of the remainder comes on top of it, and the
  // pool is still handed out in full.
  Tuner t;
  t.meta->want[PriorityCache::Priority::PRI2] = 50 * MiB;
  t.meta->want[PriorityCache::Priority::LAST] = 300 * MiB;
  t.data->want[PriorityCache::Priority::PRI2] = 100 * MiB;
  t.pcm.balance();
  EXPECT_EQ(50 * MiB, t.meta->bytes[PriorityCache::Priority::PRI2]);
  EXPECT_EQ(100 * MiB, t.data->bytes[PriorityCache::Priority::PRI2]);
  // served in full in the LAST loop, then the ratio share of what was left
  int64_t rest = Tuner::avail() - (50 + 300 + 100 + 64 + 64) * MiB;
  EXPECT_EQ(300 * MiB + static_cast<int64_t>(rest * 0.30),
            t.meta->bytes[PriorityCache::Priority::LAST]);
  EXPECT_EQ(static_cast<int64_t>(rest * 0.36), t.data->bytes[PriorityCache::Priority::LAST]);
  EXPECT_NEAR(Tuner::avail(), t.granted(), 4096);
  // and the commit funds it: 350 MiB granted, rounded up to a chunk
  EXPECT_GE(t.meta->committed, 350 * MiB);
}

TEST(PriorityCache, requests_at_last_share_by_ratio) {
  // two caches ask at LAST for more than is left: each gets its ratio share
  // of the remainder, and nothing is lost between the loop and the split
  Tuner t;
  t.meta->want[PriorityCache::Priority::LAST] = 2000 * MiB;
  t.data->want[PriorityCache::Priority::LAST] = 2000 * MiB;
  t.pcm.balance();
  int64_t rest = Tuner::avail() - (64 + 64) * MiB;
  // 0.36 : 0.30 of the remainder, over the loop's rounds (the ratio-0 caches
  // that asked for nothing drop out after the first round)
  EXPECT_NEAR(rest * 0.36 / 0.66, t.data->bytes[PriorityCache::Priority::LAST], 4096);
  EXPECT_NEAR(rest * 0.30 / 0.66, t.meta->bytes[PriorityCache::Priority::LAST], 4096);
  EXPECT_LE(t.kv->bytes[PriorityCache::Priority::LAST], 4096);
  EXPECT_LE(t.kv_onode->bytes[PriorityCache::Priority::LAST], 4096);
  EXPECT_NEAR(Tuner::avail(), t.granted(), 4096);
}

TEST(PriorityCache, zero_ratio_cache_sees_no_grant_at_last) {
  // rbd-mirror's caches: ratio 0, so the ratio split owes them nothing, and
  // their add_cache_bytes() asserts at a priority they did not ask at
  PriorityCache::Manager pcm(g_ceph_context, 512 * MiB, 512 * MiB, 512 * MiB, false,
                             "test-pricache-strict");
  std::shared_ptr<StrictCache> a{new StrictCache("a", PriorityCache::Priority::PRI10)};
  std::shared_ptr<StrictCache> b{new StrictCache("b", PriorityCache::Priority::PRI9)};
  a->want[PriorityCache::Priority::PRI10] = 100 * MiB;
  b->want[PriorityCache::Priority::PRI9] = 100 * MiB;
  pcm.insert("a", a, false);
  pcm.insert("b", b, false);
  pcm.tune_memory();
  pcm.balance();
  EXPECT_EQ(100 * MiB, a->bytes[PriorityCache::Priority::PRI10]);
  EXPECT_EQ(100 * MiB, b->bytes[PriorityCache::Priority::PRI9]);
  EXPECT_EQ(0, a->bytes[PriorityCache::Priority::LAST]);
  EXPECT_EQ(0, b->bytes[PriorityCache::Priority::LAST]);
}
