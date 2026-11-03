// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Unit tests for the probation segment of the binned LRU block cache:
 * admission on the second later use, the bound on probation, which entry
 * eviction takes, and the list pointers after removals.
 */

#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "global/global_context.h"
#include "kv/rocksdb_cache/BinnedLRUCache.h"

using rocksdb_cache::BinnedLRUCacheShard;
using rocksdb_cache::l_probation;
using rocksdb_cache::l_elems;

namespace {

/// One shard driven directly, so the test does not depend on the Cache API
/// of the RocksDB version in use. Values are never dereferenced.
struct Shard {
  std::unique_ptr<BinnedLRUCacheShard> s;

  Shard(size_t capacity, double probation_ratio) {
    s = std::make_unique<BinnedLRUCacheShard>(g_ceph_context, capacity,
                                              false, 0.0);
    s->SetProbationRatio(probation_ratio);
  }
  static uint32_t hash(const std::string& key) {
    return (uint32_t)std::hash<std::string>{}(key);
  }
  void insert(const std::string& key, size_t charge,
              rocksdb::Cache::Priority pri = rocksdb::Cache::Priority::LOW) {
    rocksdb::Status st = s->Insert(key, hash(key), nullptr, nullptr, charge,
                                   nullptr, pri);
    ASSERT_TRUE(st.ok()) << st.ToString();
  }
  /// look the entry up and release it: one use
  bool hit(const std::string& key) {
    rocksdb::Cache::Handle* h = s->Lookup(key, hash(key));
    if (!h) {
      return false;
    }
    s->Release(h);
    return true;
  }
  void erase(const std::string& key) {
    s->Erase(key, hash(key));
  }
  size_t probation() {
    return s->GetStats()[l_probation];
  }
  size_t usage() {
    return s->GetUsage();
  }
  /// a use in each of two later bins: the admission rule
  void promote(const std::string& key) {
    for (int b = 0; b < 2; b++) {
      s->shift_bins();
      ASSERT_TRUE(hit(key)) << key;
    }
  }
  /// insert into the main pool: through probation, promoted at once, since
  /// probation holds only its share and would evict earlier newcomers
  void insert_main(const std::string& key, size_t charge) {
    insert(key, charge);
    promote(key);
  }
};

} // anonymous namespace

TEST(BinnedLRUCacheProbation, promoted_after_uses_in_two_later_bins) {
  Shard c(1000, 0.5);
  c.insert("a", 100);
  EXPECT_EQ(100u, c.probation()) << "a new block is on probation";
  EXPECT_EQ(100u, c.usage());

  EXPECT_TRUE(c.hit("a"));
  EXPECT_EQ(100u, c.probation()) << "a use in the same bin does not count";

  c.s->shift_bins();
  EXPECT_TRUE(c.hit("a"));
  EXPECT_EQ(100u, c.probation()) << "one later use is not enough";
  EXPECT_TRUE(c.hit("a"));
  EXPECT_EQ(100u, c.probation()) << "nor a second use in that same bin";

  c.s->shift_bins();
  EXPECT_TRUE(c.hit("a"));
  EXPECT_EQ(0u, c.probation()) << "a use in a second later bin promotes";
  EXPECT_EQ(100u, c.usage());
}

// RocksDB inserts a block with a handle and reads it before releasing; a
// second reader may look the block up meanwhile. The block is still new to
// the cache and must start on probation.
TEST(BinnedLRUCacheProbation, block_shared_before_first_release_starts_on_probation) {
  Shard c(1000, 0.5);
  rocksdb::Cache::Handle* inserter = nullptr;
  ASSERT_TRUE(c.s->Insert("a", Shard::hash("a"), nullptr, nullptr, 100, &inserter,
                          rocksdb::Cache::Priority::LOW).ok());
  rocksdb::Cache::Handle* reader = c.s->Lookup("a", Shard::hash("a"));
  ASSERT_TRUE(reader);
  c.s->Release(reader);
  c.s->Release(inserter);  // the last reference: the block enters the list
  EXPECT_EQ(100u, c.probation());
  EXPECT_EQ(100u, c.usage());
}

TEST(BinnedLRUCacheProbation, ratio_zero_admits_at_once) {
  Shard c(1000, 0);
  c.insert("a", 100);
  EXPECT_EQ(0u, c.probation());
  EXPECT_EQ(100u, c.usage());
}

TEST(BinnedLRUCacheProbation, trimmed_to_its_share_under_capacity) {
  Shard c(1000, 0.1);  // probation may hold 100 of a cache that holds 1000
  c.insert("p1", 50);
  c.insert("p2", 50);
  EXPECT_EQ(100u, c.probation());
  c.insert("p3", 50);
  EXPECT_EQ(100u, c.probation()) << "beyond its share the oldest goes";
  EXPECT_EQ(100u, c.usage()) << "although the cache is far from full";
  EXPECT_FALSE(c.hit("p1"));
  EXPECT_TRUE(c.hit("p2"));
  EXPECT_TRUE(c.hit("p3"));
}

// With the main pool filling the cache, eviction takes the main pool's
// oldest entry while probation is below half its share, so that new blocks
// keep getting their chance; at or above half, probation's oldest goes.
TEST(BinnedLRUCacheProbation, eviction_spares_probation_below_half_its_share) {
  Shard c(1000, 0.2);  // probation share 200, half of it 100
  for (int i = 1; i <= 9; i++) {
    c.insert_main("m" + std::to_string(i), 100);
  }
  ASSERT_EQ(0u, c.probation());
  ASSERT_EQ(900u, c.usage());

  c.insert("p1", 90);  // probation 90: below half its share
  ASSERT_EQ(90u, c.probation());
  c.insert("p2", 100); // 1090 > 1000: something must go
  EXPECT_EQ(190u, c.probation());
  EXPECT_EQ(990u, c.usage());
  EXPECT_FALSE(c.hit("m1")) << "the main pool's oldest entry went";
  for (int i = 2; i <= 9; i++) {
    EXPECT_TRUE(c.hit("m" + std::to_string(i))) << "m" << i;
  }
  EXPECT_TRUE(c.hit("p1"));
  EXPECT_TRUE(c.hit("p2"));
}

TEST(BinnedLRUCacheProbation, eviction_takes_probation_at_or_above_half) {
  Shard c(1000, 0.2);
  for (int i = 1; i <= 8; i++) {
    c.insert_main("m" + std::to_string(i), 100);
  }
  c.insert("p1", 100);
  c.insert("p2", 100);  // probation 200: at its share, usage 1000
  ASSERT_EQ(200u, c.probation());
  ASSERT_EQ(1000u, c.usage());

  c.insert("p3", 100);  // 1100 > 1000: probation's oldest goes
  EXPECT_FALSE(c.hit("p1"));
  EXPECT_TRUE(c.hit("m1")) << "the main pool is untouched";
  EXPECT_TRUE(c.hit("p2"));
  EXPECT_TRUE(c.hit("p3"));
  EXPECT_EQ(200u, c.probation());
  EXPECT_EQ(1000u, c.usage());
}

// The two list pointers, newest probation entry and newest low-priority
// entry, after removing exactly those entries: later inserts and evictions
// must still land in the right place.
TEST(BinnedLRUCacheProbation, list_pointers_after_removing_the_newest_entries) {
  Shard c(1000, 0.2);
  c.insert_main("m1", 100);
  c.insert_main("m2", 100);
  c.insert("p1", 100);
  c.insert("p2", 100);
  ASSERT_EQ(200u, c.probation());
  ASSERT_EQ(400u, c.usage());

  c.erase("p2");  // the newest probation entry
  EXPECT_EQ(100u, c.probation());
  c.insert("p3", 100);  // goes above p1, below the main pool
  EXPECT_EQ(200u, c.probation());
  c.erase("m2");  // the newest low-priority entry
  EXPECT_EQ(300u, c.usage());

  // p3 is promoted: it must be inserted above m1, the newest main entry now
  c.promote("p3");
  EXPECT_EQ(100u, c.probation());

  // oldest first: p1 (probation), m1, p3
  c.s->SetCapacity(250);
  EXPECT_FALSE(c.hit("p1"));
  EXPECT_TRUE(c.hit("m1"));
  EXPECT_TRUE(c.hit("p3"));
  EXPECT_EQ(0u, c.probation());
  c.s->SetCapacity(150);
  EXPECT_FALSE(c.hit("m1"));
  EXPECT_TRUE(c.hit("p3"));
  EXPECT_EQ(100u, c.usage());
}

TEST(BinnedLRUCacheProbation, erase_unref_entries_keeps_the_element_count) {
  Shard c(1000, 0.5);
  c.insert("a", 100);
  c.insert("b", 100);
  EXPECT_EQ(2u, c.s->GetStats()[l_elems]);
  c.s->EraseUnRefEntries();
  EXPECT_EQ(0u, c.s->GetStats()[l_elems]);
  EXPECT_EQ(0u, c.usage());
  EXPECT_EQ(0u, c.probation());
}

// Index and filter blocks come in with high priority and are used by every
// read; they are admitted at once whether or not a high-priority pool is set.
TEST(BinnedLRUCacheProbation, high_priority_block_is_admitted_at_once) {
  Shard c(1000, 0.5);  // high_pri_pool_ratio 0
  c.insert("filter", 100, rocksdb::Cache::Priority::HIGH);
  EXPECT_EQ(0u, c.probation());
  EXPECT_EQ(100u, c.usage());
  EXPECT_TRUE(c.hit("filter"));
}

// A block larger than probation's share could never prove itself there: it
// would be trimmed on the insert that brought it in. It goes to the main pool.
TEST(BinnedLRUCacheProbation, block_larger_than_the_share_is_admitted_at_once) {
  Shard c(1000, 0.5);  // probation share 500
  c.insert("big", 600);
  EXPECT_EQ(0u, c.probation());
  EXPECT_EQ(600u, c.usage());
  EXPECT_TRUE(c.hit("big"));
}

// A shard starts with probation off: only a cache whose age bins are rotated
// (BlueStore's) turns it on, through the cache's SetProbationRatio().
TEST(BinnedLRUCacheProbation, off_until_the_cache_sets_a_ratio) {
  BinnedLRUCacheShard raw(g_ceph_context, 1000, false, 0.0);
  rocksdb::Status st = raw.Insert("a", Shard::hash("a"), nullptr, nullptr, 100,
                                  nullptr, rocksdb::Cache::Priority::LOW);
  ASSERT_TRUE(st.ok());
  EXPECT_EQ(0u, raw.GetStats()[l_probation]);
  rocksdb_cache::BinnedLRUCache cache(g_ceph_context, "t", 1000, 0, false, 0.0);
  cache.SetProbationRatio(0.5);
  auto* sh = static_cast<BinnedLRUCacheShard*>(cache.GetShard(0));
  st = sh->Insert("a", Shard::hash("a"), nullptr, nullptr, 100, nullptr,
                  rocksdb::Cache::Priority::LOW);
  ASSERT_TRUE(st.ok());
  EXPECT_EQ(100u, sh->GetStats()[l_probation]);
}


// Lowering the share at run time (or turning probation off) must not throw
// away the blocks on probation: they join the main pool as its oldest.
TEST(BinnedLRUCacheProbation, lowering_the_ratio_keeps_probation_blocks) {
  Shard c(1000, 0.5);
  for (int i = 1; i <= 4; i++) {
    c.insert("p" + std::to_string(i), 100);
  }
  ASSERT_EQ(400u, c.probation());
  c.s->SetProbationRatio(0.1);  // share 100
  EXPECT_EQ(400u, c.usage());
  EXPECT_LE(c.probation(), 100u);
  for (int i = 1; i <= 4; i++) {
    EXPECT_TRUE(c.hit("p" + std::to_string(i))) << "p" << i;
  }
  c.s->SetProbationRatio(0);
  EXPECT_EQ(0u, c.probation());
  EXPECT_EQ(400u, c.usage());
}

// A block on probation that is held while the cache shrinks under its size
// cannot go back on probation: it would be trimmed by the release that
// brought it back. It joins the main pool instead.
TEST(BinnedLRUCacheProbation, held_block_returns_to_the_main_pool_when_too_big) {
  Shard c(1000, 0.5);  // probation share 500
  c.insert("b", 400);
  ASSERT_EQ(400u, c.probation());
  rocksdb::Cache::Handle* h = c.s->Lookup("b", Shard::hash("b"));
  ASSERT_TRUE(h);
  c.s->SetCapacity(700);  // the block still fits the cache (400 of 700),
                          // not probation's share (350)
  c.s->Release(h);
  EXPECT_TRUE(c.hit("b")) << "still cached";
  EXPECT_EQ(0u, c.probation());
  EXPECT_EQ(400u, c.usage());
}

// The below-half rule spares probation only when the new block will fit its
// share; otherwise the oldest probation block goes, not a main-pool one, and
// the insert does not evict twice.
TEST(BinnedLRUCacheProbation, no_double_eviction_when_the_new_block_overflows_the_share) {
  Shard c(1000, 0.1);  // share 100, half of it 50
  for (int i = 1; i <= 9; i++) {
    c.insert_main("m" + std::to_string(i), 100);
  }
  c.insert("p1", 40);   // probation 40: below half
  ASSERT_EQ(940u, c.usage());
  c.insert("p2", 90);   // 40 + 90 > 100: p1 goes, not m1
  EXPECT_TRUE(c.hit("m1"));
  EXPECT_FALSE(c.hit("p1"));
  EXPECT_TRUE(c.hit("p2"));
  EXPECT_EQ(990u, c.usage());
  EXPECT_EQ(90u, c.probation());
}

// A capacity cut evicts the oldest blocks, probation's first: the rule that
// spares a nearly empty probation applies to inserts, not to shrinking.
TEST(BinnedLRUCacheProbation, capacity_cut_takes_probation_first) {
  Shard c(1000, 0.2);  // share 200, half 100
  for (int i = 1; i <= 9; i++) {
    c.insert_main("m" + std::to_string(i), 100);
  }
  c.insert("p1", 40);  // below half the share
  ASSERT_EQ(940u, c.usage());
  c.s->SetCapacity(900);
  EXPECT_FALSE(c.hit("p1")) << "the once-used block went";
  EXPECT_TRUE(c.hit("m1")) << "the proven one stayed";
  EXPECT_EQ(900u, c.usage());
}

// The below-half rule spares probation for a block that will go there; a
// high-priority or oversized block goes to the main pool, so making room for
// it must take probation's oldest, not a proven block.
TEST(BinnedLRUCacheProbation, no_main_pool_eviction_for_a_block_that_skips_probation) {
  Shard c(1000, 0.2);  // share 200, half 100
  for (int i = 1; i <= 9; i++) {
    c.insert_main("m" + std::to_string(i), 100);
  }
  c.insert("p1", 40);  // below half the share
  ASSERT_EQ(940u, c.usage());
  c.insert("filter", 100, rocksdb::Cache::Priority::HIGH);  // 1040 > 1000
  EXPECT_FALSE(c.hit("p1")) << "probation's oldest went";
  EXPECT_TRUE(c.hit("m1")) << "the proven block stayed";
  EXPECT_TRUE(c.hit("filter"));
  EXPECT_EQ(1000u, c.usage());
}
