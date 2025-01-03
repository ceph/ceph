// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/future.hh>

#include "common/hobject.h"
#include "include/types.h"
#include "osd/osd_types.h"

namespace crimson::osd {
  class ShardServices;
  class PglogBasedRecovery;
};

class RecoveryBackend;
class PGRecovery;

class PGRecoveryListener {
public:
  virtual crimson::osd::ShardServices& get_shard_services() = 0;
  virtual PGRecovery* get_recovery_handler() = 0;
  virtual epoch_t get_osdmap_epoch() const = 0;
  virtual bool is_primary() const = 0;
  virtual bool is_peered() const = 0;
  virtual bool is_recovering() const = 0;
  virtual bool is_backfilling() const = 0;
  virtual PeeringState& get_peering_state() = 0;
  virtual const pg_shard_t& get_pg_whoami() const = 0;
  virtual const spg_t& get_pgid() const = 0;
  virtual RecoveryBackend* get_recovery_backend() = 0;
  virtual bool is_unreadable_object(const hobject_t&, eversion_t* v = 0) const = 0;
  virtual bool has_reset_since(epoch_t) const = 0;
  virtual std::vector<pg_shard_t> get_replica_recovery_order() const = 0;
  virtual epoch_t get_last_peering_reset() const = 0;
  virtual const pg_info_t& get_info() const= 0;
  virtual seastar::future<> stop() = 0;
  virtual void publish_stats_to_osd() = 0;
  virtual OSDriver &get_osdriver() = 0;
  virtual SnapMapper &get_snap_mapper() = 0;
  virtual void set_pglog_based_recovery_op(
    crimson::osd::PglogBasedRecovery *op) = 0;
  virtual void reset_pglog_based_recovery_op() = 0;
};
