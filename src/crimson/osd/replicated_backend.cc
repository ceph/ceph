// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <ranges>

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
  : PGBackend{whoami.shard, coll, shard_services},
    pgid{pgid},
    whoami{whoami}
{}

ReplicatedBackend::ll_read_ierrorator::future<ceph::bufferlist>
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


/*
 * High level transaction.
 *
 * Due to how clones, renames (and EC) are handled in RADOS, crimson
 * needs something similar to PGTransaction. Unlike it, HLTransaction
 * is just an extension (also from the memory arrangement's POV) to
 * the low-level os::Transaction. These jettisonable memory areas
 * helps to reorder operations.
 */
namespace crimson::osd {
class HLTransaction {
public:
  using Op = ceph::os::Transaction::Op;

  HLTransaction(ceph::os::Transaction& txn)
    : txn(txn) {
    if (!this->txn.empty()) {
      // doing this in the transition period. The goal is to have
      // full os:Transaction-like interface and make the data loc
      // info when e.g. `clone()` or `write()` are called.
      scan_data_locators();
    }
  }

  auto* begin() {
    return reinterpret_cast<Op*>(txn.op_bl.c_str());
  }

  auto* end() {
    return begin() + txn.get_num_ops();
  }

  decltype(auto) get_object_index() const {
    return txn.get_object_index();
  }

  template <class IteratorT>
  void reorder_back(IteratorT it) {
    auto idx = std::distance(begin(), &*it);
    logger().trace("{}: txn op={} buff={}", __func__, it->op, op_data_locators[idx]);
    tmp_vec.emplace_back(*it);
    tmp_bl.claim_append(op_data_locators[idx]);
  }

  ceph::os::Transaction& get_lltransaction() && {
    // TODO: this can happen in-place
    logger().debug("{}: txn.get_num_ops()={}, tmp_vec.size()={}",
                   __func__, txn.get_num_ops(), tmp_vec.size());
    ceph_assert_always(static_cast<size_t>(txn.get_num_ops()) == std::size(tmp_vec));
    std::move(std::begin(tmp_vec), std::end(tmp_vec), std::begin(*this));
    txn.data_bl = std::move(tmp_bl);
    return txn;
  }

private:
  // TODO: reference for now, after converting other places this
  // 'aggregation' becomes 'composition'.
  ceph::os::Transaction& txn;

  // to allow efficient reordering we need to know where op's data
  // resides in the big `os::Transaction::data_bl` buffer.
  std::vector<ceph::bufferlist> op_data_locators;

  void scan_data_locators();

  // TODO: kill these
  std::vector<Op> tmp_vec;
  ceph::bufferlist tmp_bl;
};

void HLTransaction::scan_data_locators()
{
  op_data_locators.reserve(txn.get_num_ops());
  auto i = txn.begin();
  bool stop_looping = false;
  using Transaction = ceph::os::Transaction;
  while (i.have_op() && !stop_looping) {
    const auto* op = i.decode_op();
    const auto op_data_offset = i.get_data_offset();
    switch (op->op) {
    case Transaction::OP_NOP:
    case Transaction::OP_CREATE:
    case Transaction::OP_TOUCH:
    case Transaction::OP_ZERO:
    case Transaction::OP_TRIMCACHE:
    case Transaction::OP_TRUNCATE:
    case Transaction::OP_REMOVE:
    case Transaction::OP_RMATTRS:
    case Transaction::OP_CLONE:
    case Transaction::OP_CLONERANGE:
    case Transaction::OP_CLONERANGE2:
    case Transaction::OP_MKCOLL:
    case Transaction::OP_COLL_SET_BITS:
    case Transaction::OP_RMCOLL:
    case Transaction::OP_COLL_ADD:
    case Transaction::OP_COLL_REMOVE:
    case Transaction::OP_COLL_MOVE:
    case Transaction::OP_SPLIT_COLLECTION:
    case Transaction::OP_SPLIT_COLLECTION2:
    case Transaction::OP_MERGE_COLLECTION:
    case Transaction::OP_COLL_MOVE_RENAME:
    case Transaction::OP_TRY_RENAME:
    case Transaction::OP_SETALLOCHINT:
    case Transaction::OP_COLL_RENAME:
    case Transaction::OP_OMAP_CLEAR:
      break;
    case Transaction::OP_WRITE:
    case Transaction::OP_COLL_HINT:
    case Transaction::OP_OMAP_SETHEADER:
      {
        // TODO: it's actually unnecessary to decode data, it would be
        // enough to decode just size.
	ceph::bufferlist bl;
	i.decode_bl(bl);
      }
      break;
    case Transaction::OP_SETATTR:
    case Transaction::OP_COLL_SETATTR:
      {
        std::string name = i.decode_string();
	ceph::bufferlist bl;
	i.decode_bl(bl);
      }
      break;
    case Transaction::OP_SETATTRS:
    case Transaction::OP_OMAP_SETKEYS:
      {
	std::map<std::string, ceph::bufferptr> aset;
	i.decode_attrset(aset);
      }
      break;
    case Transaction::OP_RMATTR:
    case Transaction::OP_COLL_RMATTR:
      {
        std::string name = i.decode_string();
      }
      break;
    case Transaction::OP_OMAP_RMKEYRANGE:
      {
        std::string first, last;
        first = i.decode_string();
        last = i.decode_string();
      }
      break;
    case Transaction::OP_OMAP_RMKEYS:
      {
	std::set<std::string> keys;
	i.decode_keyset(keys);
      }
      break;
    default:
      stop_looping = true;
      break;
    }
    const auto op_data_size = i.get_data_offset() - op_data_offset;
    op_data_locators.emplace_back();
    op_data_locators.back().substr_of(
      txn.data_bl, op_data_offset, op_data_size);
  }
}
} // namespace crimson::osd

static bool has_source(const crimson::osd::HLTransaction::Op& op)
{
  switch (op.op) {
    case ceph::os::Transaction::OP_CLONE:
    case ceph::os::Transaction::OP_CLONERANGE2:
    case ceph::os::Transaction::OP_COLL_MOVE_RENAME:
    case ceph::os::Transaction::OP_TRY_RENAME:
      return true;
    default:
      return false;
  }
}

static uint32_t get_target_oid(const crimson::osd::HLTransaction::Op& op)
{
  // that's how `PGTransaction` interacted with `os::Transaction` in
  // the classical OSD. See `generate_transaction()`.
  if (has_source(op)) {
    return op.dest_oid;
  } else {
    return op.oid;
  }
}

static bool is_source_for_op(uint32_t idx, const crimson::osd::HLTransaction::Op& op)
{
  if (has_source(op)) {
    return op.oid == idx;
  } else {
    return false;
  }
}

static bool is_source_for(
  uint32_t src_oid,
  uint32_t dst_oid,
  crimson::osd::HLTransaction& txn)
{
  return std::any_of(std::begin(txn), std::end(txn),
    [src_oid, dst_oid] (const crimson::osd::HLTransaction::Op& op) {
      return is_source_for_op(src_oid, op) && get_target_oid(op) == dst_oid;
    });
}

static bool is_source_for_any(uint32_t maybe_root_idx, auto& map, crimson::osd::HLTransaction& txn)
{
  for (const auto& [_, idx] : map) {
    if (is_source_for(idx, maybe_root_idx, txn)) {
      return true;
    }
  }
  return false;
}

//  const std::map<ghobject_t, uint32_t>& get_object_index() const {
template <class F>
void process_tree(crimson::osd::HLTransaction& txn, uint32_t parent_idx, F&& f)
{
  for (const auto& [obj, idx] : txn.get_object_index()) {
    if (parent_idx == idx) {
      continue;
    } else if (is_source_for(parent_idx, idx, txn)) {
      // parent (e.g. head) is source for idx (cloned-to-#5)
      process_tree(txn, idx, f);
    }
  }
  auto rng = txn | std::views::filter([parent_idx] (auto& op) {
    return get_target_oid(op) == parent_idx;
  });
  f(std::move(rng));
}


// This piece takes `os::Transaction` and scans it for multi-object
// operations to call the passed lambda in the same order as in OSD
// is happens thankfully to `PGTransaction::safe_create_traverse()`.
template <class F>
static void safe_create_traverse(crimson::osd::HLTransaction& txn, F&& f)
{
  for (const auto& [obj, idx] : txn.get_object_index()) {
    // the `is_source_for` releationship between the operations in txn
    // forms a graph. we're interested only in forrest of trees and we
    // process these trees from their roots. if the graph contains any
    // cycle, this selection drops it.
    logger().debug("{}: obj={} idx={}", __func__, obj, idx);
    if (!is_source_for_any(idx, txn.get_object_index(), txn)) {
      logger().debug("{}: obj={} is root!", __func__, obj);
      process_tree(txn, idx, f);
    }
  }
}

static std::string dump_txn(ceph::os::Transaction& txn)
{
  std::unique_ptr<Formatter> f{Formatter::create("json-pretty", "json-pretty", "json-pretty")};
  std::stringstream ss;
  try {
    txn.dump(f.get());
  } catch (const ceph::buffer::end_of_buffer& e) {
    ss << "GOT EXCEPTION! e=" << e << std::endl;
  }
  f->flush(ss);
  return ss.str();
}

ReplicatedBackend::rep_op_fut_t
ReplicatedBackend::_submit_transaction(std::set<pg_shard_t>&& pg_shards,
                                       const hobject_t& hoid,
                                       ceph::os::Transaction&& _txn,
                                       osd_op_params_t&& osd_op_p,
                                       epoch_t min_epoch, epoch_t map_epoch,
				       std::vector<pg_log_entry_t>&& log_entries)
{
  logger().debug("{}: txn={}", __func__, dump_txn(_txn));
  if (__builtin_expect(stopping, false)) {
    throw crimson::common::system_shutdown_exception();
  }
  if (__builtin_expect((bool)peering, false)) {
    throw crimson::common::actingset_changed(peering->is_primary);
  }

  crimson::osd::HLTransaction hltxn(_txn);
  safe_create_traverse(hltxn, [&hltxn](auto rng) {
    logger().debug("_submit_transaction: subrange");
#if 0
    std::ranges::stable_sort(rng, [](const auto& lhs, const auto& rhs) {
      if (lhs.op == ceph::os::Transaction::OP_CREATE) {
        if (rhs.op != ceph::os::Transaction::OP_CREATE) {
          return true;
        }
      } else if (lhs.op == ceph::os::Transaction::OP_CLONE) {
        if (rhs.op == ceph::os::Transaction::OP_CREATE) {
          return false;
        } else if (rhs.op == ceph::os::Transaction::OP_CLONE) {
          return false;
        } else {
          return true;
        }
      }
      return false;
    });
#else
    for (auto it = std::ranges::begin(rng); it != std::ranges::end(rng); ++it) {
      hltxn.reorder_back(it);
    }
#endif
  });
  logger().debug("{}: txn_after={}", __func__, dump_txn(_txn));

  const ceph_tid_t tid = shard_services.get_tid();
  auto pending_txn =
    pending_trans.try_emplace(tid, pg_shards.size(), osd_op_p.at_version).first;
  auto& txn = std::move(hltxn).get_lltransaction();
  bufferlist encoded_txn;
  encode(txn, encoded_txn);

  logger().debug("ReplicatedBackend::_submit_transaction: do_transaction...");
  auto all_completed = interruptor::make_interruptible(
    shard_services.get_store().do_transaction(coll, std::move(txn))
  ).then_interruptible([this, peers=pending_txn->second.weak_from_this()] {
    if (!peers) {
      // for now, only actingset_changed can cause peers
      // to be nullptr
      assert(peering);
      throw crimson::common::actingset_changed(peering->is_primary);
    }
    if (--peers->pending == 0) {
      peers->all_committed.set_value();
      peers->all_committed = {};
      return seastar::now();
    }
    return peers->all_committed.get_shared_future();
  }).then_interruptible([pending_txn, this] {
    auto acked_peers = std::move(pending_txn->second.acked_peers);
    pending_trans.erase(pending_txn);
    return seastar::make_ready_future<crimson::osd::acked_peers_t>(std::move(acked_peers));
  });

  for (auto pg_shard : pg_shards) {
    if (pg_shard != whoami) {
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
      m->set_data(encoded_txn);
      pending_txn->second.acked_peers.push_back({pg_shard, eversion_t{}});
      encode(log_entries, m->logbl);
      m->pg_trim_to = osd_op_p.pg_trim_to;
      m->min_last_complete_ondisk = osd_op_p.min_last_complete_ondisk;
      m->set_rollback_to(osd_op_p.at_version);
      // TODO: set more stuff. e.g., pg_states
      (void) shard_services.send_to_osd(pg_shard.osd, std::move(m), map_epoch);
    }
  }
  return {seastar::now(), std::move(all_completed)};
}

void ReplicatedBackend::on_actingset_changed(peering_info_t pi)
{
  peering.emplace(pi);
  crimson::common::actingset_changed e_actingset_changed{peering->is_primary};
  for (auto& [tid, pending_txn] : pending_trans) {
    pending_txn.all_committed.set_exception(e_actingset_changed);
  }
  pending_trans.clear();
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
        peers.all_committed = {};
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
  // PG::client_request_pg_pipeline. But there's a high possibility that we will
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
