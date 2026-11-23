// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <map>
#include <algorithm>

#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>

#include "include/types.h"
#include "crimson/common/type_helpers.h"
#include "crimson/common/smp_helpers.h"
#include "crimson/osd/osd_operation.h"
#include "osd/osd_types.h"

namespace crimson::osd {
class PG;

/**
 * PGShardMapping
 *
 * Maintains a mapping from spg_t to the core containing that PG.  Internally, each
 * core has a local copy of the mapping to enable core-local lookups.  Updates
 * are proxied to core 0, and the back out to all other cores -- see get_or_create_pg_mapping.
 */
class PGShardMapping : public seastar::peering_sharded_service<PGShardMapping> {
public:
  /// Returns mapping if present, NULL_CORE otherwise
  core_id_t get_pg_mapping(spg_t pgid) {
    auto iter = pg_to_core.find(pgid);
    ceph_assert_always(iter == pg_to_core.end() || iter->second.first != NULL_CORE);
    return iter == pg_to_core.end() ? NULL_CORE : iter->second.first;
  }

  /**
   * Returns mapping for pgid, creates new one if it doesn't already exist
   *
   * core_expected, store_index: where the PG's data already is (e.g. a
   * PG loaded at startup, or a split child), or NULL_CORE and
   * NULL_STORE_INDEX for a new PG.
   *
   * A new PG without an expected core goes to the core with the fewest
   * PGs of its role: client ops are served by the primaries only, so
   * the primaries are spread over the cores evenly, and so are the
   * other PGs. 'primary': whether this OSD is the PG's (up) primary.
   * It is also counted for a PG with an expected core (e.g. loaded at
   * startup), so that the PGs created later balance against it.
   */
  seastar::future<std::pair<core_id_t, store_index_t>> get_or_create_pg_mapping(
    spg_t pgid,
    core_id_t core_expected,
    store_index_t store_index,
    bool primary);

  /// Remove pgid mapping
  seastar::future<> remove_pg_mapping(spg_t pgid);

  size_t get_num_pgs() const { return pg_to_core.size(); }

  seastar::future<> dump_store_shards(Formatter *f) const;

  /// Map to cores in [min_core_mapping, core_mapping_limit).
  /// require_partition_count_match: the store's (cached)
  /// seastore_require_partition_count_match_reactor_count
  PGShardMapping(core_id_t min_core_mapping, core_id_t core_mapping_limit, uint32_t store_shard_nums,
                 bool require_partition_count_match)
    : store_shard_nums(store_shard_nums),
      require_partition_count_match(require_partition_count_match) {
    ceph_assert_always(min_core_mapping < core_mapping_limit);
    auto max_core_mapping = std::min(min_core_mapping + store_shard_nums, core_mapping_limit);
    auto num_shard_services = (store_shard_nums + seastar::this_smp_shard_count() - 1 ) / seastar::this_smp_shard_count();
    auto num_alien_cores = (seastar::this_smp_shard_count() + store_shard_nums -1 ) / store_shard_nums;

    for (auto i = min_core_mapping; i != max_core_mapping; ++i) {
      for (unsigned int j = 0; j < num_shard_services; ++j) {
        if (i - min_core_mapping + j * seastar::this_smp_shard_count() < store_shard_nums) {
          core_shard_to_num_pgs[i].emplace(j, 0);
        }
      }
      core_to_num_pgs.emplace(i, 0);
      core_to_num_primaries.emplace(i, 0);
      for (unsigned int j = 0; j < num_alien_cores; ++j) {
        if (store_shard_nums * j + i < core_mapping_limit) {
          core_alien_to_num_pgs[i].emplace(store_shard_nums * j + i, 0);
        }
      }
    }
  }

  template <typename F>
  void for_each_pgid(F &&f) const {
    for (const auto &i: pg_to_core) {
      std::invoke(f, i.first);
    }
  }

private:

  uint32_t store_shard_nums;
  const bool require_partition_count_match;
  // only in shard 0
  //<core_id, num_pgs>
  std::map<core_id_t, unsigned> core_to_num_pgs;
  //<core_id, num_pgs this OSD was the primary of when mapped>
  std::map<core_id_t, unsigned> core_to_num_primaries;
  //<pg, the core_to_num_primaries entry it was counted in>
  std::map<spg_t, core_id_t> primary_pgs;
  //<core_id, <shard_index, num_pgs>>  // when smp < store_shard_nums, each core more than one store shard
  std::map<core_id_t, std::map<unsigned, unsigned>> core_shard_to_num_pgs;
  //<core_id, <alien_core_id, num_pgs>> // when smp > store_shard_nums, more than one core share store shard
  std::map<core_id_t, std::map<core_id_t, unsigned>> core_alien_to_num_pgs;
  // per-shard, updated by shard 0
  //<pg, <core_id, store_index>>
  std::map<spg_t, std::pair<core_id_t, store_index_t>> pg_to_core;

};

/**
 * PGMap
 *
 * Maps spg_t to PG instance within a shard.  Handles dealing with waiting
 * on pg creation.
 */
class PGMap {
  struct PGCreationState : BlockerT<PGCreationState> {
    static constexpr const char * type_name = "PGCreation";

    void dump_detail(Formatter *f) const final;

    spg_t pgid;
    seastar::shared_promise<Ref<PG>> promise;
    bool creating = false;
    PGCreationState(spg_t pgid);

    PGCreationState(const PGCreationState &) = delete;
    PGCreationState(PGCreationState &&) = delete;
    PGCreationState &operator=(const PGCreationState &) = delete;
    PGCreationState &operator=(PGCreationState &&) = delete;

    ~PGCreationState();
  };

  std::map<spg_t, PGCreationState> pgs_creating;
  using pgs_t = std::map<spg_t, Ref<PG>>;
  pgs_t pgs;

public:
  using PGCreationBlocker = PGCreationState;
  using PGCreationBlockingEvent = PGCreationBlocker::BlockingEvent;
  /**
   * Get future for pg with a bool indicating whether it's already being
   * created.
   */
  using wait_for_pg_ertr = crimson::errorator<
    crimson::ct_error::ecanceled>;
  using wait_for_pg_fut = wait_for_pg_ertr::future<Ref<PG>>;
  using wait_for_pg_ret = std::pair<wait_for_pg_fut, bool>;
  wait_for_pg_ret wait_for_pg(PGCreationBlockingEvent::TriggerI&&, spg_t pgid);

  /**
   * get PG in non-blocking manner
   */
  Ref<PG> get_pg(spg_t pgid);

  /**
   * Set creating
   */
  void set_creating(spg_t pgid);

  /**
   * Set newly created pg
   */
  void pg_created(spg_t pgid, Ref<PG> pg);

  /**
   * Add newly loaded pg
   */
  void pg_loaded(spg_t pgid, Ref<PG> pg);

  /**
   * Cancel pending creation of pgid.
   */
  void pg_creation_canceled(spg_t pgid);

  void remove_pg(spg_t pgid);

  pgs_t& get_pgs() { return pgs; }
  const pgs_t& get_pgs() const { return pgs; }
  auto get_pg_count() const { return pgs.size(); }
  PGMap() = default;
  ~PGMap();
};

}
