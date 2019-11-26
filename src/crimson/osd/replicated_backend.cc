#include "replicated_backend.h"

#include "messages/MOSDRepOpReply.h"

#include "crimson/common/log.h"
#include "crimson/os/cyanstore/cyan_object.h"
#include "crimson/os/futurized_store.h"
#include "crimson/osd/shard_services.h"

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
  return store->read(coll, ghobject_t{hoid}, off, len, flags);
}

seastar::future<crimson::osd::acked_peers_t>
ReplicatedBackend::_submit_transaction(std::set<pg_shard_t>&& pg_shards,
                                       const hobject_t& hoid,
                                       ceph::os::Transaction&& txn,
                                       osd_reqid_t req_id,
                                       epoch_t min_epoch, epoch_t map_epoch,
                                       eversion_t ver)
{
  const ceph_tid_t tid = next_txn_id++;
  auto pending_txn =
    pending_trans.emplace(tid, pending_on_t{pg_shards.size()}).first;
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
                                         tid, ver);
        m->set_data(encoded_txn);
        pending_txn->second.acked_peers.push_back({pg_shard, eversion_t{}});
        // TODO: set more stuff. e.g., pg_states
        return shard_services.send_to_osd(pg_shard.osd, std::move(m), map_epoch);
      }
    }).then([&peers=pending_txn->second] {
      if (--peers.pending == 0) {
        peers.all_committed.set_value();
      }
      return peers.all_committed.get_future();
    }).then([tid, pending_txn, this] {
      pending_txn->second.all_committed = {};
      auto acked_peers = std::move(pending_txn->second.acked_peers);
      pending_trans.erase(pending_txn);
      return seastar::make_ready_future<crimson::osd::acked_peers_t>(std::move(acked_peers));
    });
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
