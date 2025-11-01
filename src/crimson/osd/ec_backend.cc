#include <boost/iterator/counting_iterator.hpp>

#include "crimson/common/log.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/shard_services.h"
#include "ec_backend.h"
#include "include/Context.h"

#include "osd/PGTransaction.h"
#include "osd/ECTransaction.h"

SET_SUBSYS(osd);

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

ceph::ErasureCodeInterfaceRef ECBackend::create_ec_impl(
  const ec_profile_t& ec_profile)
{
  using crimson::common::local_conf;
  ceph::ErasureCodeInterfaceRef ec_impl;
  std::stringstream ss;
  ceph::ErasureCodePluginRegistry::instance().factory(
    ec_profile.find("plugin")->second,
    local_conf()->erasure_code_dir,
    const_cast<ec_profile_t&>(ec_profile),
    &ec_impl,
    &ss);
  return ec_impl;
}

ECBackend::ECBackend(pg_shard_t whoami,
                     ECBackend::CollectionRef coll,
                     crimson::osd::ShardServices& shard_services,
                     const ec_profile_t& ec_profile,
                     uint64_t stripe_width,
                     bool fast_read,
                     bool allows_ecoverwrites,
		     DoutPrefixProvider &dpp,
		     ECListener &eclistener)
  : PGBackend{whoami, coll, shard_services, dpp},
    ec_impl{create_ec_impl(ec_profile)},
    sinfo(ec_impl, &(eclistener.get_pool()), stripe_width),
    fast_read{fast_read},
    allows_ecoverwrites{allows_ecoverwrites},
    read_pipeline{shard_services.get_cct(), ec_impl, sinfo, &eclistener, *this},
    rmw_pipeline{shard_services.get_cct(), ec_impl, sinfo, &eclistener, *this, shard_services.lookup_ec_extent_cache_lru()}
{
}

ECBackend::ll_read_ierrorator::future<ceph::bufferlist>
ECBackend::_read(const hobject_t& hoid,
                 const uint64_t object_size,
                 const uint64_t off,
                 const uint64_t len,
                 const uint32_t flags)
{
  LOG_PREFIX(ECBackend::_read);
  const auto [aligned_off, aligned_len] =
    sinfo.ro_offset_len_to_stripe_ro_offset_len(off, len);
  std::map<hobject_t, std::list<ec_align_t>> reads;
  reads[hoid].emplace_back(
    ec_align_t{aligned_off, aligned_len, flags});
  seastar::promise<ceph::bufferlist> promise;
  auto ret = promise.get_future();
  objects_read_and_reconstruct(
    reads,
    fast_read,
    object_size,
    make_gen_lambda_context<ec_extents_t &&>(
      [hoid, off, len, promise=std::move(promise), FNAME](auto&& results) mutable {
        ceph_assert(results.size() == 1);
        ceph_assert(results.count(hoid) == 1);
        auto& got = results.at(hoid);
        if (got.err < 0) {
          ceph_abort_msg("implement error handling");
          return;
        }
	auto range = got.emap.get_containing_range(off, len);
	ceph_assert(range.first != range.second);
	ceph_assert(range.first.get_off() <= off);
        DEBUG("offset: {}", off);
        DEBUG("range offset: {}", range.first.get_off());
        DEBUG("length: {}", len);
        DEBUG("range length: {}", range.first.get_len());
	ceph_assert(
	  (off + len) <=
	  (range.first.get_off() + range.first.get_len()));
        ceph::bufferlist clients_bl;
	clients_bl.substr_of(
	  range.first.get_val(),
	  off - range.first.get_off(),
	  len);
        promise.set_value(std::move(clients_bl));
      }));
  return ret;
}

struct ECCrimsonOp : ECCommon::RMWPipeline::Op {
  PGTransactionUPtr t;

  static PGTransactionUPtr transate_transaction(
    ceph::os::Transaction&& t,
    crimson::osd::ObjectContextRef &&obc)
  {
    auto t_pg = std::make_unique<PGTransaction>();
    t_pg->add_obc(std::move(obc));
    auto i = std::begin(t);
    while (i.have_op()) {
      auto* op = i.decode_op();
      logger().debug("ECBackend::{} decoded op={} oid={} dest_oid={}",
		     __func__,
		     static_cast<uint32_t>(op->op),
		     i.get_oid(op->oid),
		     i.get_oid(op->dest_oid));
      switch (op->op) {
      case ceph::os::Transaction::OP_NOP:
	// please notice PG::Transaction nop() takes hoid but
	// the os::Transaction::nop() is parameterless.
	continue;
      case ceph::os::Transaction::OP_CREATE:
	t_pg->create(i.get_oid(op->oid).hobj);
	break;
      case ceph::os::Transaction::OP_TOUCH:
	t_pg->create(i.get_oid(op->oid).hobj);
	break;
      case ceph::os::Transaction::OP_REMOVE:
	t_pg->remove(i.get_oid(op->oid).hobj);
	break;
      case ceph::os::Transaction::OP_SETATTR:
	{
	auto name = i.decode_string();
	ceph::bufferlist bl;
        i.decode_bl(bl);
	t_pg->setattr(i.get_oid(op->oid).hobj,
		      name,
		      bl);
	}
	break;
      case ceph::os::Transaction::OP_SETATTRS:
	{
	std::map<std::string, ceph::bufferlist, std::less<>> aset;
        i.decode_attrset(aset);
	t_pg->setattrs(i.get_oid(op->oid).hobj, aset);
	}
	break;
      case ceph::os::Transaction::OP_RMATTR:
	t_pg->rmattr(i.get_oid(op->oid).hobj, i.decode_string());
	break;
      case ceph::os::Transaction::OP_RMATTRS:
	ceph_abort_msg("Not present in PGTransaction");
	break;
      case ceph::os::Transaction::OP_OMAP_CLEAR:
	t_pg->omap_clear(i.get_oid(op->oid).hobj);
      case ceph::os::Transaction::OP_OMAP_SETKEYS:
	{
	std::map<std::string, ceph::bufferlist> aset;
	i.decode_attrset(aset);
	t_pg->omap_setkeys(i.get_oid(op->oid).hobj, aset);
	}
	break;
      case ceph::os::Transaction::OP_OMAP_RMKEYS:
	{
	bufferlist keys_bl;
        i.decode_keyset_bl(&keys_bl);
	t_pg->omap_rmkeys(i.get_oid(op->oid).hobj, keys_bl);
	}
	break;
      case ceph::os::Transaction::OP_OMAP_RMKEYRANGE:
      case ceph::os::Transaction::OP_OMAP_SETHEADER:
	{
	ceph::bufferlist bl;
	i.decode_bl(bl);
	t_pg->omap_setheader(i.get_oid(op->oid).hobj, bl);
	}
	break;
      case ceph::os::Transaction::OP_WRITE:
	{
	ceph::bufferlist bl;
	i.decode_bl(bl);
	t_pg->write(i.get_oid(op->oid).hobj,
		    op->off,
		    op->len,
		    bl,
		    i.get_fadvise_flags());
	}
	break;
      case ceph::os::Transaction::OP_ZERO:
	t_pg->zero(i.get_oid(op->oid).hobj, op->off, op->len);
	break;
      case ceph::os::Transaction::OP_TRUNCATE:
	t_pg->truncate(i.get_oid(op->oid).hobj, op->off);
	break;
      case ceph::os::Transaction::OP_SETALLOCHINT:
	t_pg->set_alloc_hint(i.get_oid(op->oid).hobj,
			     op->expected_object_size,
			     op->expected_write_size,
			     op->hint);
        break;
      case ceph::os::Transaction::OP_CLONERANGE2:
	t_pg->clone_range(i.get_oid(op->oid).hobj,
			  i.get_oid(op->dest_oid).hobj,
			  op->off,
			  op->len,
			  op->dest_off);
      case ceph::os::Transaction::OP_CLONE:
	t_pg->clone(i.get_oid(op->dest_oid).hobj, i.get_oid(op->oid).hobj);
        break;
      case ceph::os::Transaction::OP_MKCOLL:
      case ceph::os::Transaction::OP_RMCOLL:
      case ceph::os::Transaction::OP_COLL_REMOVE:
      case ceph::os::Transaction::OP_COLL_SETATTR:
      case ceph::os::Transaction::OP_COLL_RMATTR:
      case ceph::os::Transaction::OP_COLL_SETATTRS:
      case ceph::os::Transaction::OP_COLL_HINT:
      case ceph::os::Transaction::OP_COLL_SET_BITS:
      case ceph::os::Transaction::OP_COLL_ADD:
      case ceph::os::Transaction::OP_COLL_MOVE_RENAME:
      case ceph::os::Transaction::OP_TRY_RENAME:
      case ceph::os::Transaction::OP_SPLIT_COLLECTION2:
      case ceph::os::Transaction::OP_MERGE_COLLECTION:
        ceph_abort_msg("Coll-related ops shouldn't be associated with PGTransaction");
      default:
        ceph_abort_msg("Unknown OP");
      }
    }
    return t_pg;
  }

  ECCrimsonOp(ceph::os::Transaction&& t,
              crimson::osd::ObjectContextRef &&obc,
	      ECCommon::RMWPipeline& rmw_pipeline)
    : Op(rmw_pipeline),
      t(transate_transaction(std::move(t), std::move(obc))) {
  }

  void generate_transactions(
      ceph::ErasureCodeInterfaceRef &ec_impl,
      pg_t pgid,
      const ECUtil::stripe_info_t &sinfo,
      std::map<hobject_t, ECUtil::shard_extent_map_t> *written,
      shard_id_map<ceph::os::Transaction> *transactions,
      DoutPrefixProvider *dpp,
      const OSDMapRef &osdmap) final
  {
    assert(t);
    ECTransaction::generate_transactions(
      t.get(),
      plan,
      ec_impl,
      pgid,
      sinfo,
      remote_shard_extent_map,
      log_entries,
      written,
      transactions,
      &temp_added,
      &temp_cleared,
      dpp,
      osdmap);
  }

  bool skip_transaction(
      std::set<shard_id_t> &pending_roll_forward,
      shard_id_t shard,
      ceph::os::Transaction &transaction) final {
    if (transaction.empty()) {
      return true;
    }
    pending_roll_forward.insert(shard);
    return false;
  }
};

class C_AllSubWritesCommited : public Context {
  seastar::promise<> on_complete;
public:
  C_AllSubWritesCommited() = default;

  void finish(int) override {
    on_complete.set_value();
  }

  PGBackend::interruptible_future<> get_future() {
    return on_complete.get_future();
  }
};

ECBackend::rep_op_fut_t
ECBackend::submit_transaction(const std::set<pg_shard_t> &pg_shards,
                              crimson::osd::ObjectContextRef&& obc,
			      crimson::osd::ObjectContextRef&& new_clone,
                              ceph::os::Transaction&& txn,
                              osd_op_params_t&& osd_op_p,
                              epoch_t min_epoch, epoch_t max_epoch,
			      std::vector<pg_log_entry_t>&& log_entries)
{
  const hobject_t& hoid = obc->obs.oi.soid;
  logger().debug("ECBackend::{} hoid={}", __func__, hoid);
  logger().warn("ECBackend::{} LINE {} obc->attr_cache {}", "_submit_transaction", __LINE__, obc->attr_cache);
  auto op = std::make_unique<ECCrimsonOp>(std::move(txn), std::move(obc), rmw_pipeline);
  logger().debug("ECBackend::{} LINE {}", "_submit_transaction", __LINE__);
  op->hoid = hoid;
  //op->delta_stats = delta_stats;
  op->version = osd_op_p.at_version;
  op->trim_to = osd_op_p.pg_trim_to;
  op->pg_committed_to =
    std::max(osd_op_p.pg_committed_to, rmw_pipeline.committed_to);
  op->log_entries = std::move(log_entries);
  //std::swap(op->updated_hit_set_history, hset_history);
  // TODO: promsie future here
  auto on_all_commit = new C_AllSubWritesCommited;
  op->on_all_commit = on_all_commit;
  op->tid = shard_services.get_tid();
  op->reqid = osd_op_p.req_id;
  op->client_op = nullptr; //client_op;
  //if (client_op) {
  //  op->trace = client_op->pg_trace;
  //}
  op->plan = ECCommon::get_write_plan(
    sinfo,
    *(op->t),
    read_pipeline,
    rmw_pipeline,
    &dpp);
  logger().info("{}: op {} starting", "_submit_transaction", ""); //*op);
  rmw_pipeline.start_rmw(std::move(op));
  logger().debug("ECBackend::{} started ec op", "_submit_transaction");
  logger().debug("ECBackend::{} LINE {}", "_submit_transaction", __LINE__);
  logger().debug("ECBackend::{} LINE {}", "_submit_transaction", __LINE__);
  return make_ready_future<rep_op_ret_t>(
    seastar::now(),
    on_all_commit->get_future().then_interruptible([] {
      logger().debug("DONE!!! ECBackend::{} LINE {}", "_submit_transaction", __LINE__);
      return seastar::now();
    })
  );
}

ECBackend::write_iertr::future<>
ECBackend::handle_sub_write(
  pg_shard_t from,
  ECSubWrite &&op,
  ECListener& pg)
{
  LOG_PREFIX(ECBackend::handle_sub_write);
  logger().info("{} from {}", __func__, from);
  if (!op.temp_added.empty()) {
    add_temp_obj(std::begin(op.temp_added), std::end(op.temp_added));
  }
  ceph::os::Transaction txn;
  if (op.backfill_or_async_recovery) {
    for (const auto& obj : op.temp_removed) {
      logger().info("{}: removing object {} since we won't get the transaction",
		    __func__, obj);
      txn.remove(coll->get_cid(),
		 ghobject_t{obj, ghobject_t::NO_GEN, get_shard()});
    }
  }
  clear_temp_objs(op.temp_removed);
  logger().debug("{}: missing before {}", __func__, "");

  // flag set to true during async recovery
  bool async = false;
  if (pg.is_missing_object(op.soid)) {
    async = true;
    logger().debug("{}: {} is missing", __func__, op.soid);
    for (const auto& e: op.log_entries) {
      logger().debug("{}: add_next_event entry {}, is_delete {}",
		      __func__, e, e.is_delete());
      pg.add_local_next_event(e);
    }
  }
  pg.log_operation(
    std::move(op.log_entries),
    op.updated_hit_set_history,
    op.trim_to,
    op.pg_committed_to,
    op.pg_committed_to,
    !op.backfill_or_async_recovery,
    txn,
    async);
  txn.append(op.t); // hack warn
  logger().debug("{}:{}", __func__, __LINE__);
  if (op.at_version != eversion_t()) {
    // dummy rollforward transaction doesn't get at_version
    // (and doesn't advance it)
    pg.op_applied(op.at_version);
  }
  logger().debug("{}:{}", __func__, __LINE__);
  return store->do_transaction(coll, std::move(txn)).then([FNAME] {
    DEBUG("transaction commited!");
    return write_iertr::now();
  });
}

void ECBackend::handle_sub_read_n_reply(
  pg_shard_t from,
  ECSubRead &op,
  const ZTracer::Trace &)
{
  std::ignore = seastar::do_with(std::move(op), [this](auto&& op) {
    return handle_rep_read_op(op).si_then([this](auto&& reply) {
      return this->handle_rep_read_reply(reply);
    });
  });
}

void ECBackend::handle_sub_write(
  pg_shard_t from,
  OpRequestRef msg,
  ECSubWrite &op,
  const ZTracer::Trace &trace,
  ECListener& eclistener)
{
  LOG_PREFIX(ECBackend::handle_sub_write);
  const auto tid = op.tid;
  DEBUG("tid {}", tid);
  std::ignore = handle_sub_write(
    from, std::move(op), eclistener
  ).si_then([tid, &eclistener, this] {
    assert(eclistener.pgb_is_primary());
    ECSubWriteReply reply;
    reply.tid = tid;
    //reply.last_complete = last_complete;
    reply.committed = true;
    reply.applied = true;
    reply.from = eclistener.whoami_shard();
    logger().debug("ECBackend::{} from {}",
		    "handle_sub_write::reply", reply.from);
    return handle_rep_write_reply(std::move(reply));
  }, crimson::ct_error::assert_all{});
}

ECBackend::write_iertr::future<>
ECBackend::handle_rep_write_op(
  Ref<MOSDECSubOpWrite> m,
  crimson::osd::PG& pg)
{
  LOG_PREFIX(ECBackend::handle_rep_write_op);
  const auto tid = m->op.tid;
  DEBUG("tid {} from {}", tid, m->op.from);
  return handle_sub_write(
    m->op.from, std::move(m->op), pg
  ).si_then([&pg] {
    assert(!pg.pgb_is_primary());
    return write_iertr::now();
  }, crimson::ct_error::assert_all{});
}

ECBackend::write_iertr::future<>
ECBackend::handle_rep_write_reply(ECSubWriteReply&& op)
{
  LOG_PREFIX(ECBackend::handle_rep_write_reply);
  DEBUG("handling reply from osd.{}, tid {}",  op.from.osd, op.tid);
  assert(rmw_pipeline.tid_to_op_map.contains(op.tid));
  const auto& from = op.from;
  auto& wop = *rmw_pipeline.tid_to_op_map.at(op.tid);
  if (op.committed) {
    // TODO: trace.event("sub write committed");
    logger().debug("ECBackend::{} from {} pending_commits {}",
		    __func__, from, wop.pending_commits);
    ceph_assert(wop.pending_commits > 0);
    --wop.pending_commits;
    // update_peer_last_complete_ondisk() is called by the higher
    // layer handler: PG::handle_rep_write_reply().
  }
  if (wop.pending_commits == 0) {
    rmw_pipeline.try_finish_rmw();
  }
  return write_iertr::now();
}

bool ECBackend::is_single_chunk(const hobject_t& obj, const ECSubRead& op)
{
  return (op.subchunks.find(obj)->second.size() == 1) &&
    (op.subchunks.find(obj)->second.front().second ==
      ec_impl->get_sub_chunk_count());
}

ECBackend::ll_read_errorator::future<ceph::bufferlist>
ECBackend::maybe_chunked_read(
  const hobject_t& obj,
  const ECSubRead& op,
  const uint64_t off,
  const uint64_t size,
  const uint32_t flags)
{
  LOG_PREFIX(ECBackend::maybe_chunked_read);
  DEBUG("obj {} off {} size {} flags {}", obj, off, size, flags);
  DEBUG("oid is: {}", ghobject_t{obj, ghobject_t::NO_GEN, get_shard()});
  if (is_single_chunk(obj, op)) {
    return store->read(
      coll, ghobject_t{obj, ghobject_t::NO_GEN, get_shard()}, off, size, flags);
  } else {
    return seastar::do_with(ceph::bufferlist{}, [=, this] (auto&& result_bl) {
      const int subchunk_size =
        sinfo.get_chunk_size() / ec_impl->get_sub_chunk_count();
      return crimson::do_for_each(
        boost::make_counting_iterator(0UL),
        boost::make_counting_iterator(1 + (size-1) / sinfo.get_chunk_size()),
        [off, flags, subchunk_size, &obj, &op, &result_bl, this] (const auto m) {
          const auto& sub_spec = op.subchunks.find(obj)->second;
          return crimson::do_for_each(
            std::begin(sub_spec),
            std::end(sub_spec),
            [&obj, off, flags, subchunk_size, m, &result_bl, this] (const auto& subchunk) {
              const auto [sub_off_count, sub_size_count] = subchunk;
              return store->read(
                coll,
                ghobject_t{obj, ghobject_t::NO_GEN, get_shard()},
                off + m*sinfo.get_chunk_size() + sub_off_count*subchunk_size,
                sub_size_count * subchunk_size,
                flags
              ).safe_then([&result_bl] (auto&& sub_bl) {
		result_bl.claim_append(sub_bl);
                return ll_read_errorator::now();
              });
            }
          );
        }
      ).safe_then([&result_bl] {
        return ll_read_errorator::make_ready_future<ceph::bufferlist>(
          std::move(result_bl));
      });
    });
  }
}

void ECBackend::objects_read_and_reconstruct(
  const std::map<hobject_t, std::list<ec_align_t>> &reads,
  bool fast_read,
  uint64_t object_size,
  GenContextURef<ec_extents_t &&> &&func)
{
  return read_pipeline.objects_read_and_reconstruct(
    reads, fast_read, object_size, std::move(func));
}

void ECBackend::objects_read_and_reconstruct_for_rmw(
  std::map<hobject_t, ECCommon::read_request_t> &&to_read,
  GenContextURef<ECCommon::ec_extents_t&&> &&func)
{
  return read_pipeline.objects_read_and_reconstruct_for_rmw(
    std::move(to_read), std::move(func));
}

ECBackend::ll_read_ierrorator::future<ECSubReadReply>
ECBackend::handle_rep_read_op(Ref<MOSDECSubOpRead> m)
{
  return handle_rep_read_op(m->op).finally([m=std::move(m)] {});
}

ECBackend::ll_read_ierrorator::future<ECSubReadReply>
ECBackend::handle_rep_read_op(ECSubRead& op)
{
  LOG_PREFIX(ECBackend::handle_rep_read_op);
  return seastar::do_with(ECSubReadReply{},
		          [&op, FNAME, this] (auto&& reply) {
    reply.from = whoami;
    reply.tid = op.tid;
    using read_ertr = crimson::os::FuturizedStore::Shard::read_errorator;
    DEBUG("op_list {}", op.to_read);
    return interruptor::do_for_each(op.to_read, [FNAME, &op, &reply, this] (auto read_item) {
      const auto& [obj, op_list] = read_item;
      // `obj=obj` is workaround for Clang's bug:
      // https://www.reddit.com/r/LLVM/comments/s0ykcj/why_does_clang_fail_with_error_reference_to_local/?rdt=36162
      return interruptor::do_for_each(op_list, [FNAME, &op, &reply, obj=obj, this] (auto op_spec) {
        const auto& [off, size, flags] = op_spec;
        return maybe_chunked_read(
          obj, op, off, size, flags
        ).safe_then([&reply, obj, off=off, size=size, FNAME] (auto&& result_bl) {
	  DEBUG("read requested={} len={}", size, result_bl.length());
	  reply.buffers_read[obj].emplace_back(off, std::move(result_bl));
	  return read_ertr::now();
	}).handle_error(read_ertr::all_same_way([&reply, obj, FNAME, this] (const auto& e) {
          assert(e.value() > 0);
	  if (e.value() == ENOENT && fast_read) {
	    INFO("ENOENT reading {}, fast, read, probably ok", obj);
	  } else {
	    ERROR("Error {} reading {}", e.value(), obj);
	    // TODO: clog error logging
            reply.buffers_read.erase(obj);
            reply.errors[obj] = -e.value();
	  }
          return read_ertr::now();
        }));
      });
    }).si_then([&op, &reply, FNAME, this] {
      DEBUG("attrs_to_read {}", op.attrs_to_read.size());
      return interruptor::do_for_each(op.attrs_to_read,
		                      [&reply, FNAME, this] (auto obj_attr) {
	DEBUG("fulfilling attr request on obj {}", obj_attr);
	if (reply.errors.count(obj_attr)) {
          return read_ertr::now();
	}
        return store->get_attrs(
          coll, ghobject_t{obj_attr, ghobject_t::NO_GEN, get_shard()}
	).safe_then([&reply, obj_attr] (auto&& attrs) {
	  reply.attrs_read[obj_attr] = std::move(attrs);
          return read_ertr::now();
        }, read_ertr::all_same_way([&reply, obj_attr] (const auto& e) {
          assert(e.value() > 0);
          reply.attrs_read.erase(obj_attr);
          reply.buffers_read.erase(obj_attr);
          reply.errors[obj_attr] = -e.value();
          return read_ertr::now();
	}));
      });
    }).si_then([&reply] {
      return read_ertr::make_ready_future<ECSubReadReply>(std::move(reply));
    });
  });
}

ECBackend::ll_read_ierrorator::future<>
ECBackend::handle_rep_read_reply(Ref<MOSDECSubOpReadReply> m)
{
  return handle_rep_read_reply(m->op).finally([m=std::move(m)] {});
}

ECBackend::ll_read_ierrorator::future<>
ECBackend::handle_rep_read_reply(ECSubReadReply& mop)
{
  const auto& from = mop.from;
  logger().debug("{}: reply {} from {}", __func__, mop, from);
  if (!read_pipeline.tid_to_read_map.contains(mop.tid)) {
    //canceled
    logger().debug("{}: canceled", __func__);
    return ll_read_ierrorator::now();
  }
  auto& rop = read_pipeline.tid_to_read_map.at(mop.tid);

  // 1. data
  for (auto& [obj, offset_buffer_map] : mop.buffers_read) {
    // if attribute error we better not have sent a buffer
    assert(!mop.errors.contains(obj));
    if (!rop.to_read.contains(obj)) {
      // We canceled this read! @see filter_read_op
      logger().debug("{}: to_read skipping", __func__);
      continue;
    }
    if (!rop.complete.contains(obj)) {
      rop.complete.emplace(obj, &sinfo);
    }
    auto &buffers_read = rop.complete.at(obj).buffers_read;
    for (auto &&[offset, buffer_list]: offset_buffer_map) {
      buffers_read.insert_in_shard(from.shard, offset, buffer_list);
    }
  }
  for (auto &&[hoid, req]: rop.to_read) {
    if (!rop.complete.contains(hoid)) {
      rop.complete.emplace(hoid, &sinfo);
    }
    auto &complete = rop.complete.at(hoid);
    for (auto &&[shard, read]: std::as_const(req.shard_reads)) {
      if (complete.errors.contains(read.pg_shard)) continue;

      complete.processed_read_requests[shard].union_of(read.extents);

      if (!rop.complete.contains(hoid) ||
        !complete.buffers_read.contains(shard)) {
        if (!read.extents.empty()) continue; // Complete the actual read first.

        // If we are first here, populate the completion.
        if (!rop.complete.contains(hoid)) {
          rop.complete.emplace(hoid, read_result_t(&sinfo));
        }
      }
    }
  }
  // 2. attrs
  for (auto &&[hoid, attr]: mop.attrs_read) {
    assert(!mop.errors.count(hoid));
    // if read error better not have sent an attribute
    if (!rop.to_read.count(hoid)) {
      // we canceled this read! @see filter_read_op
      logger().debug("{}: to_read skipping", __func__);
      continue;
    }
    if (!rop.complete.contains(hoid)) {
      rop.complete.emplace(hoid, &sinfo);
    }
    rop.complete.at(hoid).attrs.emplace();
    (*(rop.complete.at(hoid).attrs)).swap(attr);
  }
  // 3. errors
  for (auto &&[hoid, err]: mop.errors) {
    if (!rop.complete.contains(hoid)) {
      rop.complete.emplace(hoid, &sinfo);
    }
    auto &complete = rop.complete.at(hoid);
    complete.errors.emplace(from, err);
    complete.buffers_read.erase_shard(from.shard);
    complete.processed_read_requests.erase(from.shard);
    logger().debug("{} shard={} error={}", __func__, from, err);
  }
  {
    auto it = read_pipeline.shard_to_read_map.find(from);
    assert(it != std::end(read_pipeline.shard_to_read_map));
    // second is a set of all ongoing requests
    auto& txc_registry = it->second;
    assert(txc_registry.contains(mop.tid));
    txc_registry.erase(mop.tid);
  }
  assert(rop.in_progress.contains(from));
  rop.in_progress.erase(from);
  // For redundant reads check for completion as each shard comes in,
  // or in a non-recovery read check for completion once all the shards read.
  unsigned is_complete = 0;
  bool need_resend = false;
  if (rop.do_redundant_reads || rop.in_progress.empty()) {
    for (auto &&[oid, read_result]: rop.complete) {
      shard_id_set have;
      read_result.processed_read_requests.populate_shard_id_set(have);
      shard_id_set dummy_minimum;
      shard_id_set want_to_read;
      rop.to_read.at(oid).shard_want_to_read.
          populate_shard_id_set(want_to_read);

      int err = ec_impl->minimum_to_decode(want_to_read, have, dummy_minimum,
                                            nullptr);
      if (err) {
	logger().debug("{} minimum_to_decode failed {}", __func__, err);
        if (rop.in_progress.empty()) {
          // If we don't have enough copies, try other pg_shard_ts if available.
          // During recovery there may be multiple osds with copies of the same shard,
          // so getting EIO from one may result in multiple passes through this code path.
          if (!rop.do_redundant_reads) {
            int r = read_pipeline.send_all_remaining_reads(oid, rop);
            if (r == 0) {
              // We found that new reads are required to do a decode.
              need_resend = true;
              continue;
            } else if (r >  0) {
              // No new reads were requested. This means that some parity
              // shards can be assumed to be zeros.
              err = 0;
            }
            // else insufficient shards are available, keep the errors.
          }
          // Couldn't read any additional shards so handle as completed with errors
          // We don't want to confuse clients / RBD with objectstore error
          // values in particular ENOENT.  We may have different error returns
          // from different shards, so we'll return minimum_to_decode() error
          // (usually EIO) to reader.  It is likely an error here is due to a
          // damaged pg.
          rop.complete.at(oid).r = err;
          ++is_complete;
        }
      }

      if (!err) {
        ceph_assert(rop.complete.at(oid).r == 0);
        if (!rop.complete.at(oid).errors.empty()) {
	  using crimson::common::local_conf;
	  if (local_conf()->osd_read_ec_check_for_errors) {
	    logger().info("{}: not ignoring errors, use one shard err={}",
			  __func__, err);
            err = rop.complete.at(oid).errors.begin()->second;
            rop.complete.at(oid).r = err;
	  } else {
	    logger().info("{}: error(s) ignored for {} enough copies available",
			  __func__, oid);
            rop.complete.at(oid).errors.clear();
	  }
        }
        // avoid re-read for completed object as we may send remaining reads for uncopmpleted objects
        rop.to_read.at(oid).shard_reads.clear();
        rop.to_read.at(oid).want_attrs = false;
        ++is_complete;
      }
    }
  }
  if (need_resend) {
    read_pipeline.do_read_op(rop);
  } else if (rop.in_progress.empty() ||
             is_complete == rop.complete.size()) {
    logger().debug("{}: complete {}", __func__, rop);
    read_pipeline.complete_read_op(std::move(rop));
  } else {
    logger().info("{}: readop not completed yet: {}", __func__, rop);
  }
  return ll_read_ierrorator::now();
}

} // namespace crimson::osd
