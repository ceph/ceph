// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "test/crimson/gtest_seastar.h"

#include <vector>

#include "crimson/common/config_proxy.h"
#include "crimson/osd/pg_map.h"

using crimson::core_id_t;
using crimson::NULL_CORE;
using crimson::NULL_STORE_INDEX;
using crimson::osd::PGShardMapping;

namespace {

constexpr unsigned NUM_CORES = 4;
constexpr const char *REQUIRE_MATCH_OPT =
  "seastore_require_partition_count_match_reactor_count";

spg_t make_pgid(uint32_t seed)
{
  return spg_t(pg_t(seed, 1));
}

// Must run inside a seastar thread, on shard 0.
struct mapping_t {
  seastar::sharded<PGShardMapping> sharded;

  explicit mapping_t(uint32_t store_shard_nums) {
    sharded.start(core_id_t{0}, core_id_t{NUM_CORES}, store_shard_nums).get();
  }
  ~mapping_t() {
    sharded.stop().get();
  }

  core_id_t add(uint32_t seed, bool is_primary) {
    return sharded.local().get_or_create_pg_mapping(
      make_pgid(seed), NULL_CORE, NULL_STORE_INDEX, is_primary).get().first;
  }
  // pin a pg to core, as load_pgs() does
  void add_on(uint32_t seed, core_id_t core, bool is_primary) {
    sharded.local().get_or_create_pg_mapping(
      make_pgid(seed), core, 0, is_primary).get();
  }
  void set_primary(uint32_t seed, bool is_primary) {
    sharded.local().set_pg_primary(make_pgid(seed), is_primary).get();
  }
  void remove(uint32_t seed) {
    sharded.local().remove_pg_mapping(make_pgid(seed)).get();
  }
};

} // anonymous namespace

struct pg_shard_mapping_test_t : public seastar_test_suite_t {};

// Replicas are piled on cores 1-3 so a placement by total pg count would
// put every primary on core 0. Primaries must spread out instead.
TEST_F(pg_shard_mapping_test_t, primaries_spread_across_cores)
{
  run_async([] {
    if (seastar::smp::shard_count() != NUM_CORES) {
      GTEST_SKIP() << "needs --smp " << NUM_CORES;
    }
    mapping_t m(NUM_CORES);
    uint32_t seed = 0;
    for (core_id_t core = 1; core < NUM_CORES; ++core) {
      for (int i = 0; i < 6; ++i) {
        m.add_on(seed++, core, false);
      }
    }
    std::vector<unsigned> primaries(NUM_CORES, 0);
    for (unsigned i = 0; i < 2 * NUM_CORES; ++i) {
      ++primaries[m.add(seed++, true)];
    }
    for (auto count : primaries) {
      EXPECT_EQ(count, 2u);
    }
  });
}

// A pg promoted after it was mapped counts, so new primaries avoid its core.
TEST_F(pg_shard_mapping_test_t, promoted_pg_is_counted)
{
  run_async([] {
    if (seastar::smp::shard_count() != NUM_CORES) {
      GTEST_SKIP() << "needs --smp " << NUM_CORES;
    }
    mapping_t m(NUM_CORES);
    core_id_t promoted_core = NULL_CORE;
    for (uint32_t seed = 0; seed < NUM_CORES; ++seed) {
      auto core = m.add(seed, false);
      if (seed == 2) {
        promoted_core = core;
      }
    }
    m.set_primary(2, true);
    std::set<core_id_t> cores;
    for (uint32_t seed = 10; seed < 10 + NUM_CORES - 1; ++seed) {
      cores.insert(m.add(seed, true));
    }
    EXPECT_EQ(cores.size(), NUM_CORES - 1);
    EXPECT_EQ(cores.count(promoted_core), 0u);
  });
}

// A pg demoted, or removed, stops counting, so its core is used again.
TEST_F(pg_shard_mapping_test_t, demoted_and_removed_pg_are_not_counted)
{
  run_async([] {
    if (seastar::smp::shard_count() != NUM_CORES) {
      GTEST_SKIP() << "needs --smp " << NUM_CORES;
    }
    mapping_t m(NUM_CORES);
    std::vector<core_id_t> cores;
    for (uint32_t seed = 0; seed < NUM_CORES; ++seed) {
      cores.push_back(m.add(seed, true));
    }
    m.set_primary(1, false);
    EXPECT_EQ(m.add(10, true), cores[1]);
    m.remove(3);
    EXPECT_EQ(m.add(11, true), cores[3]);
  });
}

// With more cores than store shards, a pg is remapped to an alien core, which
// is counted under the core it came from. Demoting such a pg must lower
// that core's count: after demoting pg 3, its core is the least loaded.
TEST_F(pg_shard_mapping_test_t, alien_core_primaries_are_counted)
{
  run_async([] {
    if (seastar::smp::shard_count() != NUM_CORES) {
      GTEST_SKIP() << "needs --smp " << NUM_CORES;
    }
    constexpr uint32_t store_shard_nums = NUM_CORES / 2;
    // the test main() only starts the sharded conf; make its config
    // visible on the other shards so that set_val() can propagate
    crimson::common::sharded_conf().invoke_on(
      0, &crimson::common::ConfigProxy::start).get();
    crimson::common::local_conf().set_val(REQUIRE_MATCH_OPT, "false").get();
    {
      mapping_t m(store_shard_nums);
      std::vector<core_id_t> cores;
      for (uint32_t seed = 0; seed < NUM_CORES; ++seed) {
        cores.push_back(m.add(seed, true));
      }
      // 2 pgs per store shard, the second one of each on an alien core
      ASSERT_GE(cores[2], store_shard_nums);
      ASSERT_GE(cores[3], store_shard_nums);
      m.set_primary(3, false);
      EXPECT_EQ(m.add(10, true) % store_shard_nums,
                cores[3] % store_shard_nums);
    }
    crimson::common::local_conf().set_val(REQUIRE_MATCH_OPT, "true").get();
  });
}
