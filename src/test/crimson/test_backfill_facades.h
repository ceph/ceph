// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/osd/backfill_state.h"

namespace crimson::osd {

// PeeringFacade -- a facade (in the GoF-defined meaning) simplifying
// the interface of PeeringState. The motivation is to have an inventory
// of behaviour that must be provided by a unit test's mock.
struct BackfillState::PeeringFacade {
  virtual hobject_t earliest_backfill() const = 0;
  virtual std::set<pg_shard_t> get_backfill_targets() const = 0;
  virtual const hobject_t& get_peer_last_backfill(pg_shard_t peer) const = 0;
  virtual const eversion_t& get_last_update() const = 0;
  virtual const eversion_t& get_log_tail() const = 0;

  template <class... Args>
  void scan_log_after(Args&&... args) const {
  }

  virtual bool is_backfill_target(pg_shard_t peer) const = 0;
  virtual void update_complete_backfill_object_stats(const hobject_t &hoid,
                                             const pg_stat_t &stats) = 0;
  virtual bool is_backfilling() const = 0;
  virtual ~PeeringFacade() {}
};

// PGFacade -- a facade (in the GoF-defined meaning) simplifying the huge
// interface of crimson's PG class. The motivation is to have an inventory
// of behaviour that must be provided by a unit test's mock.
struct BackfillState::PGFacade {
  virtual const eversion_t& get_projected_last_update() const = 0;
  virtual ~PGFacade() {}
};

} // namespace crimson::osd
