#include <boost/iterator/counting_iterator.hpp>

#include "crimson/common/log.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/shard_services.h"
#include "ec_backend.h"

#include "osd/PGTransaction.h"
#include "osd/ECTransaction.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

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

ECBackend::ECBackend(pg_shard_t p_shard,
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
    sinfo{ec_impl->get_data_chunk_count(), stripe_width},
    fast_read{fast_read},
    allows_ecoverwrites{allows_ecoverwrites},
    read_pipeline{shard_services.get_cct(), ec_impl, sinfo, &eclistener},
    rmw_pipeline{shard_services.get_cct(), ec_impl, sinfo, &eclistener, *this}
{
}

ECBackend::ll_read_ierrorator::future<ceph::bufferlist>
ECBackend::_read(const hobject_t& hoid,
                 const uint64_t off,
                 const uint64_t len,
                 const uint32_t flags)
{
  // todo
  return seastar::make_ready_future<bufferlist>();
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
		     op->op,
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
              crimson::osd::ObjectContextRef &&obc)
    : t(transate_transaction(std::move(t), std::move(obc))) {
  }

  void generate_transactions(
      ceph::ErasureCodeInterfaceRef &ecimpl,
      pg_t pgid,
      const ECUtil::stripe_info_t &sinfo,
      std::map<hobject_t,extent_map> *written,
      std::map<shard_id_t, ObjectStore::Transaction> *transactions,
      DoutPrefixProvider *dpp,
      const ceph_release_t require_osd_release) final
  {
    assert(t);
#if 1
    ECTransaction::generate_transactions(
      t.get(),
      plan,
      ecimpl,
      pgid,
      sinfo,
      remote_read_result,
      log_entries,
      written,
      transactions,
      &temp_added,
      &temp_cleared,
      dpp,
      require_osd_release);
#endif
  }

  template <typename F>
  static ECTransaction::WritePlan get_write_plan(
    const ECUtil::stripe_info_t &sinfo,
    PGTransaction& t,
    F &&get_hinfo,
    DoutPrefixProvider *dpp)
  {
#if 1
    return ECTransaction::get_write_plan(
      sinfo,
      t,
      std::forward<F>(get_hinfo),
      dpp);
#endif
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
ECBackend::_submit_transaction(std::set<pg_shard_t>&& pg_shards,
                               crimson::osd::ObjectContextRef &&obc,
                               ceph::os::Transaction&& txn,
                               osd_op_params_t&& osd_op_p,
                               epoch_t min_epoch,
			       epoch_t max_epoch,
			       std::vector<pg_log_entry_t>&& log_entries)
{
  const hobject_t& hoid = obc->obs.oi.soid;
  logger().debug("ECBackend::{} hoid={}", __func__, hoid);
  return {
  seastar::now(),
  PGBackend::interruptor::async([=, this,
				 txn=std::move(txn),
				 osd_op_p=std::move(osd_op_p)]() mutable {
    logger().debug("ECBackend::{} LINE {}", "_submit_transaction", __LINE__);
    auto op = std::make_unique<ECCrimsonOp>(std::move(txn), std::move(obc));
    logger().debug("ECBackend::{} LINE {}", "_submit_transaction", __LINE__);
    op->hoid = hoid;
    //op->delta_stats = delta_stats;
    op->version = osd_op_p.at_version;
    op->trim_to = osd_op_p.pg_trim_to;
    op->roll_forward_to =
      std::max(osd_op_p.min_last_complete_ondisk, rmw_pipeline.committed_to);
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
    op->plan = op->get_write_plan(
      sinfo,
      *(op->t),
      [this](const hobject_t &i) {
        ECUtil::HashInfoRef ref =
          get_hash_info(i, true).handle_error_interruptible(
	  crimson::ct_error::assert_all{}
	).get();
    logger().debug("ECBackend::{} LINE {}", "_submit_transaction", __LINE__);
        ceph_assert_always(ref);
        return ref;
      },
      &dpp);
    logger().info("{}: op {} starting", "_submit_transaction", ""); //*op);
    rmw_pipeline.start_rmw(std::move(op));
    logger().debug("ECBackend::{} started ec op", "_submit_transaction");
    on_all_commit->get_future().get();
    logger().debug("ECBackend::{} LINE {}", "_submit_transaction", __LINE__);
  }).then_interruptible([] {
    logger().debug("ECBackend::{} LINE {}", "_submit_transaction", __LINE__);
    return seastar::make_ready_future<crimson::osd::acked_peers_t>();
  })};
}

ECBackend::load_hashinfo_iertr::future<ECUtil::HashInfoRef>
ECBackend::get_hash_info(
  const hobject_t &hoid,
  bool create,
  const std::map<std::string, ceph::buffer::ptr, std::less<>> *attr)
{
  logger().debug("{}: getting hashinfo attr on {}", __func__, hoid);
  if (auto ref = unstable_hashinfo_registry.lookup(hoid); ref) {
    return load_hashinfo_ertr::make_ready_future<ECUtil::HashInfoRef>(
      std::move(ref));
  }
  logger().info("{}: not in cache {}", __func__, hoid);

  using get_attr_ertr = crimson::os::FuturizedStore::Shard::get_attr_errorator;
  return store->get_attr(
    coll,
    ghobject_t{hoid, ghobject_t::NO_GEN, get_shard()},
    ECUtil::get_hinfo_key()
  ).safe_then([create, hoid, this] (auto&& valbl) {
    logger().debug("{}: found obj+attr on disk {}", "get_hash_info", hoid);
    if (!valbl.length()) {
      // If empty object and no hinfo, create it
      return load_hashinfo_ertr::make_ready_future<ECUtil::HashInfoRef>();
    }
    ECUtil::HashInfo hinfo(ec_impl->get_chunk_count());
    auto bp = valbl.cbegin();
    try {
      decode(hinfo, bp);
    } catch(...) {
      ceph_abort_msg("can't decode hinfo");
    }
    return load_hashinfo_ertr::make_ready_future<ECUtil::HashInfoRef>(
      unstable_hashinfo_registry.lookup_or_create(hoid, hinfo));
  }, crimson::ct_error::enoent::handle([create, hoid, this] {
    // no object
    logger().error("{}: the object {} doesn't exist. {}",
		   "get_hash_info", hoid, create ? "creating" : "");
    if (create) {
      ECUtil::HashInfo hinfo(ec_impl->get_chunk_count());
      return load_hashinfo_ertr::make_ready_future<ECUtil::HashInfoRef>(
        unstable_hashinfo_registry.lookup_or_create(hoid, hinfo)
      );
    } else {
      return load_hashinfo_ertr::make_ready_future<ECUtil::HashInfoRef>();
    }
  }), crimson::ct_error::enodata::handle([hoid] {
    // object exists but attr doesn't
    logger().error("{}: object {} exists but it lacks the attribute",
		   "get_hash_info", hoid);
    return load_hashinfo_ertr::make_ready_future<ECUtil::HashInfoRef>();
  }));
}

ECBackend::write_iertr::future<>
ECBackend::handle_sub_write(
  pg_shard_t from,
  ECSubWrite &&op,
  ECListener& pg)
{
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
    op.roll_forward_to,
    op.roll_forward_to,
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
  return store->do_transaction(coll, std::move(txn)).then([] {
    logger().debug("{}:{}", __func__, __LINE__);
    return write_iertr::now();
  });
}

void ECBackend::handle_sub_write(
  pg_shard_t from,
  OpRequestRef msg,
  ECSubWrite &op,
  const ZTracer::Trace &trace,
  ECListener& eclistener)
{
  std::ignore = handle_sub_write(from, std::move(op), eclistener);
}

ECBackend::write_iertr::future<>
ECBackend::handle_rep_write_op(
  Ref<MOSDECSubOpWrite> m,
  crimson::osd::PG& pg)
{
  return handle_sub_write(m->op.from, std::move(m->op), pg);
}

ECBackend::write_iertr::future<>
ECBackend::handle_rep_write_reply(ECSubWriteReply&& op)
{
  logger().debug("ECBackend::{}", __func__);
  assert(rmw_pipeline.tid_to_op_map.contains(op.tid));
  const auto& from = op.from;
  auto& wop = *rmw_pipeline.tid_to_op_map.at(op.tid);
  if (op.committed) {
    // TODO: trace.event("sub write committed");
    logger().debug("ECBackend::{} from {} pending_commit {}",
		    __func__, from, wop.pending_commit);
    ceph_assert(wop.pending_commit.count(from));
    wop.pending_commit.erase(from);
  }
  if (op.applied) {
    // TODO: trace.event("sub write applied");
    ceph_assert(wop.pending_apply.count(from));
    wop.pending_apply.erase(from);
  }

  if (wop.pending_commit.empty() &&
      wop.on_all_commit &&
      // also wait for apply, to preserve ordering with luminous peers.
      wop.pending_apply.empty()) {
    logger().info("ECBackend::{}: calling on_all_commit on {}", __func__, wop);
    wop.on_all_commit->complete(0);
    wop.on_all_commit = 0;
    // TODO: wop.trace.event("ec write all committed");
  }
  rmw_pipeline.check_ops();
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
        [off, size, flags, subchunk_size, &obj, &op, &result_bl, this] (const auto m) {
          const auto& sub_spec = op.subchunks.find(obj)->second;
          return crimson::do_for_each(
            std::begin(sub_spec),
            std::end(sub_spec),
            [&obj, off, size, flags, subchunk_size, m, &result_bl, this] (const auto& subchunk) {
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
  const std::map<hobject_t, std::list<boost::tuple<uint64_t, uint64_t, uint32_t> >
  > &reads,
  bool fast_read,
  GenContextURef<std::map<hobject_t,std::pair<int, extent_map> > &&> &&func)
{
  return read_pipeline.objects_read_and_reconstruct(
    reads, fast_read, std::move(func));
}

ECBackend::ll_read_ierrorator::future<>
ECBackend::handle_rep_read_op(Ref<MOSDECSubOpRead> m)
{
  return seastar::do_with(ECSubReadReply{},
		          [m=std::move(m), this] (auto&& reply) {
    const ECSubRead &op = m->op;
    reply.from = whoami;
    reply.tid = op.tid;
    using read_ertr = crimson::os::FuturizedStore::Shard::read_errorator;
    return interruptor::do_for_each(op.to_read, [&op, &reply, this] (auto read_item) {
      const auto& [obj, op_list] = read_item;
      return interruptor::do_for_each(op_list, [&op, &reply, obj, this] (auto op_spec) {
        const auto& [off, size, flags] = op_spec;
        return maybe_chunked_read(
          obj, op, off, size, flags
        ).safe_then([&reply, obj, off, size] (auto&& result_bl) {
          logger().debug("{}: read requested={} len={}",
			 "handle_rep_read_op", size, result_bl.length());
	  reply.buffers_read[obj].emplace_back(off, std::move(result_bl));
          return read_ertr::now();
	}).handle_error(read_ertr::all_same_way([&reply, obj, this] (const auto& e) {
          assert(e.value() > 0);
	  if (e.value() == ENOENT && fast_read) {
	    logger().info("{}: ENOENT reading {}, fast, read, probably ok",
			  "handle_rep_read_op", obj);
	  } else {
	    logger().error("{}: Error {} reading {}",
			   "handle_rep_read_op", e.value(), obj);
	    // TODO: clog error logging
            reply.buffers_read.erase(obj);
            reply.errors[obj] = -e.value();
	  }
          return read_ertr::now();
        }));
      });
    }).si_then([&op, &reply, this] {
      // return handle_rep_read_attrs();
      return interruptor::do_for_each(op.attrs_to_read,
		                      [&op, &reply, this] (auto obj_attr) {
	logger().debug("{}: fulfilling attr request on obj {}",
		       "handle_rep_read_op", obj_attr);
	if (reply.errors.count(obj_attr)) {
          return read_ertr::now();
	}
        return store->get_attrs(
          coll, ghobject_t{obj_attr, ghobject_t::NO_GEN, get_shard()}
	).safe_then([&reply, obj_attr] (auto&& attrs) {
	  reply.attrs_read[obj_attr] = std::move(attrs);
          return read_ertr::now();
        }, read_ertr::all_same_way([&reply, obj_attr, this] (const auto& e) {
          assert(e.value() > 0);
          reply.attrs_read.erase(obj_attr);
          reply.buffers_read.erase(obj_attr);
          reply.errors[obj_attr] = -e.value();
          return read_ertr::now();
	}));
      });
    });
  });
}

ECBackend::ll_read_ierrorator::future<>
ECBackend::handle_rep_read_reply(Ref<MOSDECSubOpReadReply> m)
{
  const auto& from = m->op.from;
  auto& mop = m->op;
  logger().debug("{}: reply {} from {}", __func__, mop, from);
  if (!read_pipeline.tid_to_read_map.contains(mop.tid)) {
    //canceled
    logger().debug("{}: canceled", __func__);
    return ll_read_ierrorator::now();
  }
  auto& rop = read_pipeline.tid_to_read_map.at(mop.tid);

  // 1. data
  for (auto& [obj, buffers] : mop.buffers_read) {
    // if attribute error we better not have sent a buffer
    assert(!mop.errors.contains(obj));
    if (!rop.to_read.contains(obj)) {
      // We canceled this read! @see filter_read_op
      logger().debug("{}: to_read skipping", __func__);
      continue;
    }
    // would be cool to have the C++23's view::zip
    auto req_iter = std::begin(rop.to_read.find(obj)->second.to_read);
    auto ret_iter = std::begin(rop.complete[obj].returned);
    for (auto& [len, buffer] : buffers) {
      // bolierplate
      assert(req_iter != std::end(rop.to_read.find(obj)->second.to_read));
      assert(ret_iter != std::end(rop.complete[obj].returned));

      const auto adjusted =
	sinfo.aligned_offset_len_to_chunk(
	  std::make_pair(req_iter->get<0>(), req_iter->get<1>()));
      assert(adjusted.first == len);

      ret_iter->get<2>()[from] = std::move(buffer);
      // bolierplate
      ++req_iter;
      ++ret_iter;
    }
  }
  // 2. attrs
  for (auto& [obj, attrs] : mop.attrs_read) {
    assert(!mop.errors.contains(obj));	// if read error better not have sent an attribute
    if (!rop.to_read.contains(obj)) {
      // we canceled this read! @see filter_read_op
      logger().debug("{}: to_read skipping", __func__);
      continue;
    }
    rop.complete[obj].attrs.emplace();
    (*(rop.complete[obj].attrs)).swap(attrs);
  }
  // 3. errors
  for (auto& [obj, errcode] : mop.errors) {
    rop.complete[obj].errors.emplace(from, errcode);
    logger().debug("{} shard={} error={}", __func__, from, errcode);
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
    for (const auto& [obj, read_result] : rop.complete) {
      std::set<int> have;
      for (const auto& [shard, bl] : read_result.returned.front().get<2>()) {
        have.emplace(shard.shard);
	logger().debug("{} have shard={}", __func__, shard);
      }
      std::map<int, std::vector<std::pair<int, int>>> dummy_minimum;
      int err;
      if ((err = ec_impl->minimum_to_decode(rop.want_to_read[obj], have, &dummy_minimum)) < 0) {
	logger().debug("{} minimum_to_decode failed {}", __func__, err);
        if (rop.in_progress.empty()) {
	  // If we don't have enough copies, try other pg_shard_ts if available.
	  // During recovery there may be multiple osds with copies of the same shard,
	  // so getting EIO from one may result in multiple passes through this code path.
	  if (!rop.do_redundant_reads) {
	    // TODO:
	    int r = 0;// read_pipeline.send_all_remaining_reads(obj, rop);
	    if (r == 0) {
	      // We changed the rop's to_read and not incrementing is_complete
	      need_resend = true;
	      continue;
	    }
	    // Couldn't read any additional shards so handle as completed with errors
	  }
	  // We don't want to confuse clients / RBD with objectstore error
	  // values in particular ENOENT.  We may have different error returns
	  // from different shards, so we'll return minimum_to_decode() error
	  // (usually EIO) to reader.  It is likely an error here is due to a
	  // damaged pg.
	  rop.complete[obj].r = err;
	  ++is_complete;
	}
      } else {
        assert(rop.complete[obj].r == 0);
	if (!rop.complete[obj].errors.empty()) {
	  using crimson::common::local_conf;
	  if (local_conf()->osd_read_ec_check_for_errors) {
	    logger().info("{}: not ignoring errors, use one shard err={}",
			  __func__, err);
	    err = rop.complete[obj].errors.begin()->second;
            rop.complete[obj].r = err;
	  } else {
	    logger().info("{}: error(s) ignored for {} enough copies available",
			  __func__, obj);
	    rop.complete[obj].errors.clear();
	  }
	}
	// avoid re-read for completed object as we may send remaining reads for uncopmpleted objects
	rop.to_read.at(obj).need.clear();
	rop.to_read.at(obj).want_attrs = false;
	++is_complete;
      }
    }
  }
  if (need_resend) {
    read_pipeline.do_read_op(rop);
  } else if (rop.in_progress.empty() ||
             is_complete == rop.complete.size()) {
    logger().debug("{}: complete {}", __func__, rop);
    read_pipeline.complete_read_op(rop);
  } else {
    logger().info("{}: readop not completed: {}", __func__, rop);
  }
  return ll_read_ierrorator::now();
}
