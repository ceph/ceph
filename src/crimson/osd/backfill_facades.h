// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/osd/backfill_state.h"
#include "crimson/osd/pg.h"
#include "osd/PeeringState.h"

namespace crimson::osd {

// PeeringFacade -- main implementation of the BackfillState::PeeringFacade
// interface. We have the abstraction to decuple BackfillState from Peering
// State, and thus cut depedencies in unit testing. The second implemention
// is BackfillFixture::PeeringFacade and sits in test_backfill.cc.
struct PeeringFacade final : BackfillState::PeeringFacade {
  PeeringState& peering_state;

  hobject_t earliest_backfill() const override {
    return peering_state.earliest_backfill();
  }

  const std::set<pg_shard_t>& get_backfill_targets() const override {
    return peering_state.get_backfill_targets();
  }

  const hobject_t& get_peer_last_backfill(pg_shard_t peer) const override {
    return peering_state.get_peer_info(peer).last_backfill;
  }

  eversion_t get_pg_committed_to() const override {
    return peering_state.get_pg_committed_to();
  }

  const eversion_t& get_log_tail() const override {
    return peering_state.get_info().log_tail;
  }

  const PGLog& get_pg_log() const override {
    return peering_state.get_pg_log();
  }

  void scan_log_after(eversion_t v, scan_log_func_t f) const override {
    peering_state.get_pg_log().get_log().scan_log_after(v, std::move(f));
  }

  bool is_backfill_target(pg_shard_t peer) const override {
    return peering_state.is_backfill_target(peer);
  }
  void update_complete_backfill_object_stats(const hobject_t &hoid,
                                             const pg_stat_t &stats) override {
    peering_state.update_complete_backfill_object_stats(hoid, stats);
  }

  bool is_backfilling() const override {
    return peering_state.is_backfilling();
  }

  void prepare_backfill_for_missing(
    const hobject_t &soid,
    const eversion_t &v,
    const std::vector<pg_shard_t> &peers) override {
    return peering_state.prepare_backfill_for_missing(soid, v, peers);
  }
  PeeringFacade(PeeringState& peering_state)
    : peering_state(peering_state) {
  }
};

// PGFacade -- a facade (in the GoF-defined meaning) simplifying the huge
// interface of crimson's PG class. The motivation is to have an inventory
// of behaviour that must be provided by a unit test's mock.
struct PGFacade final : BackfillState::PGFacade {
  PG& pg;

  const eversion_t& get_projected_last_update() const override {
    return pg.projected_last_update;
  }

  const PGLog::IndexedLog& get_projected_log() const override {
    return pg.projected_log;
  }

  PGFacade(PG& pg) : pg(pg) {}
  std::ostream &print(std::ostream &out) const override {
    return out << pg;
  }
};

} // namespace crimson::osd
