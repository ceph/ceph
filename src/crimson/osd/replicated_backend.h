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
}

class ReplicatedBackend : public PGBackend
{
public:
  ReplicatedBackend(pg_t pgid, pg_shard_t whoami,
		    CollectionRef coll,
		    crimson::osd::ShardServices& shard_services);
  void got_rep_op_reply(const MOSDRepOpReply& reply) final;
  seastar::future<> stop() final;
  void on_actingset_changed(peering_info_t pi) final;
private:
  ll_read_errorator::future<ceph::bufferlist> _read(const hobject_t& hoid,
					            uint64_t off,
					            uint64_t len,
					            uint32_t flags) override;
  seastar::future<crimson::osd::acked_peers_t>
  _submit_transaction(std::set<pg_shard_t>&& pg_shards,
		      const hobject_t& hoid,
		      ceph::os::Transaction&& txn,
		      const osd_op_params_t& osd_op_p,
		      epoch_t min_epoch, epoch_t max_epoch,
		      std::vector<pg_log_entry_t>&& log_entries) final;
  const pg_t pgid;
  const pg_shard_t whoami;
  crimson::osd::ShardServices& shard_services;
  ceph_tid_t next_txn_id = 0;
  class pending_on_t : public seastar::weakly_referencable<pending_on_t> {
  public:
    pending_on_t(size_t pending)
      : pending{static_cast<unsigned>(pending)}
    {}
    unsigned pending;
    crimson::osd::acked_peers_t acked_peers;
    seastar::promise<> all_committed;
  };
  using pending_transactions_t = std::map<ceph_tid_t, pending_on_t>;
  pending_transactions_t pending_trans;
};
