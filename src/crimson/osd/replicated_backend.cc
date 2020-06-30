// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "replicated_backend.h"

#include "messages/MOSDRepOpReply.h"

#include "crimson/common/exception.h"
#include "crimson/common/log.h"
#include "crimson/os/futurized_store.h"
#include "crimson/osd/shard_services.h"
#include "osd/PeeringState.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

ReplicatedBackend::ReplicatedBackend(pg_t pgid,
                                     pg_shard_t whoami,
                                     ReplicatedBackend::CollectionRef coll,
                                     crimson::osd::ShardServices& shard_services)
  : PGBackend{whoami.shard, coll, &shard_services.get_store()},
    pgid{pgid},
    whoami{whoami},
    shard_services{shard_services}
{}

ReplicatedBackend::ll_read_errorator::future<ceph::bufferlist>
ReplicatedBackend::_read(const hobject_t& hoid,
                         const uint64_t off,
                         const uint64_t len,
                         const uint32_t flags)
{
  if (__builtin_expect(stopping, false)) {
    throw crimson::common::system_shutdown_exception();
  }
  return store->read(coll, ghobject_t{hoid}, off, len, flags);
}

seastar::future<crimson::osd::acked_peers_t>
ReplicatedBackend::_submit_transaction(std::set<pg_shard_t>&& pg_shards,
                                       const hobject_t& hoid,
                                       ceph::os::Transaction&& txn,
                                       const osd_op_params_t& osd_op_p,
                                       epoch_t min_epoch, epoch_t map_epoch,
				       std::vector<pg_log_entry_t>&& log_entries)
{
  if (__builtin_expect(stopping, false)) {
    throw crimson::common::system_shutdown_exception();
  }
  if (__builtin_expect((bool)peering, false)) {
    throw crimson::common::actingset_changed(peering->is_primary);
  }

  const ceph_tid_t tid = next_txn_id++;
  auto req_id = osd_op_p.req->get_reqid();
  auto pending_txn =
    pending_trans.emplace(tid, pg_shards.size()).first;
  bufferlist encoded_txn;
  encode(txn, encoded_txn);

  return seastar::parallel_for_each(std::move(pg_shards),
    [=, encoded_txn=std::move(encoded_txn), txn=std::move(txn)]
    (auto pg_shard) mutable {
      if (pg_shard == whoami) {
        return shard_services.get_store().do_transaction(coll,std::move(txn));
      } else {
        auto m = make_message<MOSDRepOp>(req_id, whoami,
                                         spg_t{pgid, pg_shard.shard}, hoid,
                                         CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK,
                                         map_epoch, min_epoch,
                                         tid, osd_op_p.at_version);
        m->set_data(encoded_txn);
        pending_txn->second.acked_peers.push_back({pg_shard, eversion_t{}});
	encode(log_entries, m->logbl);
	m->pg_trim_to = osd_op_p.pg_trim_to;
	m->min_last_complete_ondisk = osd_op_p.min_last_complete_ondisk;
	m->set_rollback_to(osd_op_p.at_version);
        // TODO: set more stuff. e.g., pg_states
        return shard_services.send_to_osd(pg_shard.osd, std::move(m), map_epoch);
      }
    }).then([this, peers=pending_txn->second.weak_from_this()] {
      if (!peers) {
	// for now, only actingset_changed can cause peers
	// to be nullptr
	assert(peering);
	throw crimson::common::actingset_changed(peering->is_primary);
      }
      if (--peers->pending == 0) {
        peers->all_committed.set_value();
      }
      return peers->all_committed.get_future();
    }).then([pending_txn, this] {
      pending_txn->second.all_committed = {};
      auto acked_peers = std::move(pending_txn->second.acked_peers);
      pending_trans.erase(pending_txn);
      return seastar::make_ready_future<crimson::osd::acked_peers_t>(std::move(acked_peers));
    });
}

void ReplicatedBackend::on_actingset_changed(peering_info_t pi)
{
  peering.emplace(pi);
  crimson::common::actingset_changed e_actingset_changed{peering->is_primary};
  for (auto& [tid, pending_txn] : pending_trans) {
    pending_txn.all_committed.set_exception(e_actingset_changed);
  }
}

void ReplicatedBackend::got_rep_op_reply(const MOSDRepOpReply& reply)
{
  auto found = pending_trans.find(reply.get_tid());
  if (found == pending_trans.end()) {
    logger().warn("{}: no matched pending rep op: {}", __func__, reply);
    return;
  }
  auto& peers = found->second;
  for (auto& peer : peers.acked_peers) {
    if (peer.shard == reply.from) {
      peer.last_complete_ondisk = reply.get_last_complete_ondisk();
      if (--peers.pending == 0) {
        peers.all_committed.set_value();    
      }
      return;
    }
  }
}

seastar::future<> ReplicatedBackend::stop()
{
  logger().info("ReplicatedBackend::stop {}", coll->get_cid());
  stopping = true;
  for (auto& [tid, pending_on] : pending_trans) {
    pending_on.all_committed.set_exception(
	crimson::common::system_shutdown_exception());
  }
  pending_trans.clear();
  return seastar::now();
}
