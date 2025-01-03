// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "replicated_backend.h"

#include "messages/MOSDRepOpReply.h"

#include "crimson/common/coroutine.h"
#include "crimson/common/exception.h"
#include "crimson/common/log.h"
#include "crimson/os/futurized_store.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/shard_services.h"
#include "osd/PeeringState.h"

SET_SUBSYS(osd);

ReplicatedBackend::ReplicatedBackend(pg_t pgid,
                                     pg_shard_t whoami,
				     crimson::osd::PG& pg,
                                     ReplicatedBackend::CollectionRef coll,
                                     crimson::osd::ShardServices& shard_services,
				     DoutPrefixProvider &dpp)
  : PGBackend{whoami.shard, coll, shard_services, dpp},
    pgid{pgid},
    whoami{whoami},
    pg(pg)
{}

ReplicatedBackend::ll_read_ierrorator::future<ceph::bufferlist>
ReplicatedBackend::_read(const hobject_t& hoid,
                         const uint64_t off,
                         const uint64_t len,
                         const uint32_t flags)
{
  return store->read(coll, ghobject_t{hoid}, off, len, flags);
}

MURef<MOSDRepOp> ReplicatedBackend::new_repop_msg(
  const pg_shard_t &pg_shard,
  const hobject_t &hoid,
  const bufferlist &encoded_txn,
  const osd_op_params_t &osd_op_p,
  epoch_t min_epoch,
  epoch_t map_epoch,
  const std::vector<pg_log_entry_t> &log_entries,
  bool send_op,
  ceph_tid_t tid)
{
  ceph_assert(pg_shard != whoami);
  auto m = crimson::make_message<MOSDRepOp>(
    osd_op_p.req_id,
    whoami,
    spg_t{pgid, pg_shard.shard},
    hoid,
    CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK,
    map_epoch,
    min_epoch,
    tid,
    osd_op_p.at_version);
  if (send_op) {
    m->set_data(encoded_txn);
  } else {
    ceph::os::Transaction t;
    bufferlist bl;
    encode(t, bl);
    m->set_data(bl);
  }
  encode(log_entries, m->logbl);
  m->pg_trim_to = osd_op_p.pg_trim_to;
  m->pg_committed_to = osd_op_p.pg_committed_to;
  m->pg_stats = pg.get_info().stats;
  return m;
}

ReplicatedBackend::rep_op_fut_t
ReplicatedBackend::submit_transaction(
  const std::set<pg_shard_t> &pg_shards,
  const hobject_t& hoid,
  crimson::osd::ObjectContextRef &&new_clone,
  ceph::os::Transaction&& t,
  osd_op_params_t&& opp,
  epoch_t min_epoch, epoch_t map_epoch,
  std::vector<pg_log_entry_t>&& logv)
{
  LOG_PREFIX(ReplicatedBackend::submit_transaction);
  DEBUGDPP("object {}", dpp, hoid);
  auto log_entries = std::move(logv);
  auto txn = std::move(t);
  auto osd_op_p = std::move(opp);
  auto _new_clone = std::move(new_clone);

  const ceph_tid_t tid = shard_services.get_tid();
  auto pending_txn =
    pending_trans.try_emplace(tid, pg_shards.size(), osd_op_p.at_version).first;
  bufferlist encoded_txn;
  encode(txn, encoded_txn);

  for (auto &le : log_entries) {
    le.mark_unrollbackable();
  }

  std::vector<pg_shard_t> to_push_clone;
  auto sends = std::make_unique<std::vector<seastar::future<>>>();
  for (auto &pg_shard : pg_shards) {
    if (pg_shard == whoami) {
      continue;
    }
    MURef<MOSDRepOp> m;
    if (pg.should_send_op(pg_shard, hoid)) {
      m = new_repop_msg(
	pg_shard, hoid, encoded_txn, osd_op_p,
	min_epoch, map_epoch, log_entries, true, tid);
    } else {
      m = new_repop_msg(
	pg_shard, hoid, encoded_txn, osd_op_p,
	min_epoch, map_epoch, log_entries, false, tid);
      if (_new_clone && pg.is_missing_on_peer(pg_shard, hoid)) {
	// The head is in the push queue but hasn't been pushed yet.
	// We need to ensure that the newly created clone will be 
	// pushed as well, otherwise we might skip it.
	// See: https://tracker.ceph.com/issues/68808
	to_push_clone.push_back(pg_shard);
      }
    }
    pending_txn->second.acked_peers.push_back({pg_shard, eversion_t{}});
    // TODO: set more stuff. e.g., pg_states
    sends->emplace_back(
      shard_services.send_to_osd(
	pg_shard.osd, std::move(m), map_epoch));
  }

  co_await pg.update_snap_map(log_entries, txn);

  pg.log_operation(
    std::move(log_entries),
    osd_op_p.pg_trim_to,
    osd_op_p.at_version,
    osd_op_p.pg_committed_to,
    true,
    txn,
    false);

  auto all_completed = interruptor::make_interruptible(
      shard_services.get_store().do_transaction(coll, std::move(txn))
   ).then_interruptible([FNAME, this,
			peers=pending_txn->second.weak_from_this()] {
    if (!peers) {
      // for now, only actingset_changed can cause peers
      // to be nullptr
      ERRORDPP("peers is null, this should be impossible", dpp);
      assert(0 == "impossible");
    }
    if (--peers->pending == 0) {
      peers->all_committed.set_value();
      peers->all_committed = {};
      return seastar::now();
    }
    return peers->all_committed.get_shared_future();
  }).then_interruptible([pending_txn, this, _new_clone,
			to_push_clone=std::move(to_push_clone)] {
    auto acked_peers = std::move(pending_txn->second.acked_peers);
    pending_trans.erase(pending_txn);
    if (_new_clone && !to_push_clone.empty()) {
      pg.enqueue_push_for_backfill(
	_new_clone->obs.oi.soid,
	_new_clone->obs.oi.version,
	to_push_clone);
    }
    return seastar::make_ready_future<
      crimson::osd::acked_peers_t>(std::move(acked_peers));
  });

  auto sends_complete = seastar::when_all_succeed(
    sends->begin(), sends->end()
  ).finally([sends=std::move(sends)] {});
  co_return std::make_tuple(std::move(sends_complete), std::move(all_completed));
}

void ReplicatedBackend::on_actingset_changed(bool same_primary)
{
  crimson::common::actingset_changed e_actingset_changed{same_primary};
  for (auto& [tid, pending_txn] : pending_trans) {
    pending_txn.all_committed.set_exception(e_actingset_changed);
  }
  pending_trans.clear();
}

void ReplicatedBackend::got_rep_op_reply(const MOSDRepOpReply& reply)
{
  LOG_PREFIX(ReplicatedBackend::got_rep_op_reply);
  auto found = pending_trans.find(reply.get_tid());
  if (found == pending_trans.end()) {
    WARNDPP("cannot find rep op for message {}", dpp, reply);
    return;
  }
  auto& peers = found->second;
  for (auto& peer : peers.acked_peers) {
    if (peer.shard == reply.from) {
      peer.last_complete_ondisk = reply.get_last_complete_ondisk();
      if (--peers.pending == 0) {
        peers.all_committed.set_value();
        peers.all_committed = {};
      }
      return;
    }
  }
}

seastar::future<> ReplicatedBackend::stop()
{
  LOG_PREFIX(ReplicatedBackend::stop);
  INFODPP("cid {}", coll->get_cid());
  for (auto& [tid, pending_on] : pending_trans) {
    pending_on.all_committed.set_exception(
	crimson::common::system_shutdown_exception());
  }
  pending_trans.clear();
  return seastar::now();
}

seastar::future<>
ReplicatedBackend::request_committed(const osd_reqid_t& reqid,
				    const eversion_t& at_version)
{
  if (std::empty(pending_trans)) {
    return seastar::now();
  }
  auto iter = pending_trans.begin();
  auto& pending_txn = iter->second;
  if (pending_txn.at_version > at_version) {
    return seastar::now();
  }
  for (; iter->second.at_version < at_version; ++iter);
  // As for now, the previous client_request with the same reqid
  // mustn't have finished, as that would mean later client_requests
  // has finished before earlier ones.
  //
  // The following line of code should be "assert(pending_txn.at_version == at_version)",
  // as there can be only one transaction at any time in pending_trans due to
  // PG::request_pg_pipeline. But there's a high possibility that we will
  // improve the parallelism here in the future, which means there may be multiple
  // client requests in flight, so we loosed the restriction to as follows. Correct
  // me if I'm wrong:-)
  assert(iter != pending_trans.end() && iter->second.at_version == at_version);
  if (iter->second.pending) {
    return iter->second.all_committed.get_shared_future();
  } else {
    return seastar::now();
  }
}
