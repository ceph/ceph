#include <boost/iterator/counting_iterator.hpp>

#include "crimson/common/log.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/shard_services.h"
#include "ec_backend.h"

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

ECBackend::rep_op_fut_t
ECBackend::_submit_transaction(std::set<pg_shard_t>&& pg_shards,
                               const hobject_t& hoid,
                               ceph::os::Transaction&& txn,
                               osd_op_params_t&& osd_op_p,
                               epoch_t min_epoch, epoch_t max_epoch,
			       std::vector<pg_log_entry_t>&& log_entries)
{
  // todo
  return {seastar::now(),
	  seastar::make_ready_future<crimson::osd::acked_peers_t>()};
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
ECBackend::handle_rep_write_reply(Ref<MOSDECSubOpWriteReply>)
{
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
  // TODO XXX FIXME
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
  //logger().debug("{}: reply {}", __func__, mop);
  logger().debug("{}: reply {} from {}", __func__, "", from);
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
    //logger().debug("{}: complete {}", __func__, rop);
    read_pipeline.complete_read_op(rop);
  } else {
    //logger().info("{}: readop not completed: {}", __func__, rop);
  }
  return ll_read_ierrorator::now();
}
