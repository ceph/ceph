// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

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
    ceph_assert_always(iter == pg_to_core.end() || iter->second != NULL_CORE);
    return iter == pg_to_core.end() ? NULL_CORE : iter->second;
  }

  /// Returns mapping for pgid, creates new one if it doesn't already exist
  seastar::future<core_id_t> get_or_create_pg_mapping(
    spg_t pgid,
    core_id_t core_expected = NULL_CORE);

  /// Remove pgid mapping
  seastar::future<> remove_pg_mapping(spg_t pgid);

  size_t get_num_pgs() const { return pg_to_core.size(); }

  /// Map to cores in [min_core_mapping, core_mapping_limit)
  PGShardMapping(core_id_t min_core_mapping, core_id_t core_mapping_limit) {
    ceph_assert_always(min_core_mapping < core_mapping_limit);
    for (auto i = min_core_mapping; i != core_mapping_limit; ++i) {
      core_to_num_pgs.emplace(i, 0);
    }
  }

  template <typename F>
  void for_each_pgid(F &&f) const {
    for (const auto &i: pg_to_core) {
      std::invoke(f, i.first);
    }
  }

private:
  // only in shard 0
  std::map<core_id_t, unsigned> core_to_num_pgs;
  // per-shard, updated by shard 0
  std::map<spg_t, core_id_t> pg_to_core;
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
