// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive_ptr.hpp>
#include <seastar/core/future.hh>
#include <seastar/core/weak_ptr.hh>
#include "messages/MOSDPGPCT.h"
#include "include/buffer_fwd.h"
#include "osd/osd_types.h"

#include "acked_peers.h"
#include "pg_backend.h"

namespace crimson::osd {
  class ShardServices;
  class PG;
}

class ReplicatedBackend : public PGBackend
{
public:
  using interruptor = ::crimson::interruptible::interruptor<
    ::crimson::osd::IOInterruptCondition>;
  template <typename T = void>
  using interruptible_future =
    ::crimson::interruptible::interruptible_future<
      ::crimson::osd::IOInterruptCondition, T>;
  ReplicatedBackend(pg_t pgid, pg_shard_t whoami,
		    crimson::osd::PG& pg,
		    CollectionRef coll,
		    crimson::osd::ShardServices& shard_services,
		    DoutPrefixProvider &dpp);
  void got_rep_op_reply(const MOSDRepOpReply& reply) final;
  seastar::future<> stop() final;
  void on_actingset_changed(bool same_primary) final;
private:
  ll_read_ierrorator::future<ceph::bufferlist>
    _read(const hobject_t& hoid, uint64_t off,
	  uint64_t len, uint32_t flags) override;
  rep_op_fut_t submit_transaction(
    const std::set<pg_shard_t> &pg_shards,
    const hobject_t& hoid,
    crimson::osd::ObjectContextRef&& new_clone,
    ceph::os::Transaction&& txn,
    osd_op_params_t&& osd_op_p,
    epoch_t min_epoch, epoch_t max_epoch,
    std::vector<pg_log_entry_t>&& log_entries) final;
  const pg_t pgid;
  const pg_shard_t whoami;
  class pending_on_t : public seastar::weakly_referencable<pending_on_t> {
  public:
    pending_on_t(
      size_t pending,
      const eversion_t& at_version,
      const eversion_t& last_complete)
      : pending{static_cast<unsigned>(pending)},
	at_version(at_version),
	last_complete(last_complete)
    {}
    unsigned pending;
    // The order of pending_txns' at_version must be the same as their
    // corresponding ceph_tid_t, as we rely on this condition for checking
    // whether a client request is already completed. To put it another
    // way, client requests at_version must be updated synchorously/simultaneously
    // with ceph_tid_t.
    const eversion_t at_version;
    const eversion_t last_complete;
    crimson::osd::acked_peers_t acked_peers;
    seastar::shared_promise<> all_committed;
  };
  using pending_transactions_t = std::map<ceph_tid_t, pending_on_t>;
  pending_transactions_t pending_trans;
  crimson::osd::PG& pg;

  MURef<MOSDRepOp> new_repop_msg(
    const pg_shard_t &pg_shard,
    const hobject_t &hoid,
    bufferlist &encoded_txn_p_bl,
    bufferlist &encoded_txn_d_bl,
    const osd_op_params_t &osd_op_p,
    epoch_t min_epoch,
    epoch_t map_epoch,
    const std::vector<pg_log_entry_t> &log_entries,
    bool send_op,
    ceph_tid_t tid);

  seastar::future<> request_committed(
    const osd_reqid_t& reqid, const eversion_t& at_version) final;

  seastar::timer<seastar::lowres_clock> pct_timer;

  /// Invoked by pct_timer to update PCT after a pause in IO
  interruptible_future<> send_pct_update();

  /// Kick pct timer if repop_queue is empty
  void maybe_kick_pct_update();

  /// Cancel pct timer if scheduled
  void cancel_pct_update();

public:
  /// Handle MOSDPGPCT message
  void do_pct(const MOSDPGPCT &m);
};
