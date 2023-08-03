#include <boost/iterator/counting_iterator.hpp>

#include "crimson/common/log.h"
#include "crimson/osd/shard_services.h"
#include "ec_backend.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

ECBackend::ECBackend(shard_id_t shard,
                     ECBackend::CollectionRef coll,
                     crimson::osd::ShardServices& shard_services,
                     const ec_profile_t&,
                     uint64_t stripe_width,
                     bool fast_read,
                     bool allows_ecoverwrites,
		     DoutPrefixProvider &dpp)
  : PGBackend{shard, coll, shard_services, dpp},
    sinfo{ec_impl->get_data_chunk_count(), stripe_width},
    fast_read{fast_read},
    allows_ecoverwrites{allows_ecoverwrites}
{
  // FIXME: ec_impl
  // todo
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
ECBackend::submit_transaction(const std::set<pg_shard_t> &pg_shards,
                              const hobject_t& hoid,
			      crimson::osd::ObjectContextRef&& new_clone,
                              ceph::os::Transaction&& txn,
                              osd_op_params_t&& osd_op_p,
                              epoch_t min_epoch, epoch_t max_epoch,
			      std::vector<pg_log_entry_t>&& log_entries)
{
  // todo
  return make_ready_future<rep_op_ret_t>(seastar::now(), seastar::now());
}

ECBackend::write_iertr::future<>
ECBackend::handle_rep_write_op(Ref<MOSDECSubOpWrite>)
{
  return write_iertr::now();
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
    });
  });
}

ECBackend::ll_read_ierrorator::future<>
ECBackend::handle_rep_read_reply(Ref<MOSDECSubOpReadReply>)
{
  return ll_read_ierrorator::now();
}

} // namespace crimson::osd
