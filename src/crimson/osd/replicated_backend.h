// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive_ptr.hpp>
#include <seastar/core/future.hh>
#include <seastar/core/weak_ptr.hh>
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
  rep_op_fut_t _submit_transaction(std::set<pg_shard_t>&& pg_shards,
    crimson::osd::ObjectContextRef &&obc,
    ceph::os::Transaction&& txn,
    osd_op_params_t&& osd_op_p,
    epoch_t min_epoch, epoch_t max_epoch,
    std::vector<pg_log_entry_t>&& log_entries) final;
  const pg_t pgid;
  class pending_on_t : public seastar::weakly_referencable<pending_on_t> {
  public:
    pending_on_t(size_t pending, const eversion_t& at_version)
      : pending{static_cast<unsigned>(pending)}, at_version(at_version)
    {}
    unsigned pending;
    // The order of pending_txns' at_version must be the same as their
    // corresponding ceph_tid_t, as we rely on this condition for checking
    // whether a client request is already completed. To put it another
    // way, client requests at_version must be updated synchorously/simultaneously
    // with ceph_tid_t.
    const eversion_t at_version;
    crimson::osd::acked_peers_t acked_peers;
    seastar::shared_promise<> all_committed;
  };
  using pending_transactions_t = std::map<ceph_tid_t, pending_on_t>;
  pending_transactions_t pending_trans;
  crimson::osd::PG& pg;

  seastar::future<> request_committed(
    const osd_reqid_t& reqid, const eversion_t& at_version) final;
};
