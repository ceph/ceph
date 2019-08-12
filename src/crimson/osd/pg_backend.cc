// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "pg_backend.h"

#include <optional>
#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <fmt/ostream.h>
#include <seastar/core/print.hh>

#include "messages/MOSDOp.h"

#include "crimson/os/cyan_collection.h"
#include "crimson/os/cyan_object.h"
#include "crimson/os/futurized_store.h"
#include "replicated_backend.h"
#include "ec_backend.h"
#include "exceptions.h"

namespace {
  seastar::logger& logger() {
    return ceph::get_logger(ceph_subsys_osd);
  }
}

using ceph::common::local_conf;

std::unique_ptr<PGBackend> PGBackend::create(pg_t pgid,
					     const pg_shard_t pg_shard,
                                             const pg_pool_t& pool,
					     ceph::os::CollectionRef coll,
					     ceph::osd::ShardServices& shard_services,
                                             const ec_profile_t& ec_profile)
{
  switch (pool.type) {
  case pg_pool_t::TYPE_REPLICATED:
    return std::make_unique<ReplicatedBackend>(pgid, pg_shard,
					       coll, shard_services);
  case pg_pool_t::TYPE_ERASURE:
    return std::make_unique<ECBackend>(pg_shard.shard, coll, shard_services,
                                       std::move(ec_profile),
                                       pool.stripe_width);
  default:
    throw runtime_error(seastar::format("unsupported pool type '{}'",
                                        pool.type));
  }
}

PGBackend::PGBackend(shard_id_t shard,
                     CollectionRef coll,
                     ceph::os::FuturizedStore* store)
  : shard{shard},
    coll{coll},
    store{store}
{}

seastar::future<PGBackend::cached_os_t>
PGBackend::get_object_state(const hobject_t& oid)
{
  // want the head?
  if (oid.snap == CEPH_NOSNAP) {
    logger().trace("find_object: {}@HEAD", oid);
    return _load_os(oid);
  } else {
    // we want a snap
    return _load_ss(oid).then([oid,this](cached_ss_t ss) {
      // head?
      if (oid.snap > ss->seq) {
        return _load_os(oid.get_head());
      } else {
        // which clone would it be?
        auto clone = std::upper_bound(begin(ss->clones), end(ss->clones),
                                      oid.snap);
        if (clone == end(ss->clones)) {
          return seastar::make_exception_future<PGBackend::cached_os_t>(
            ceph::osd::object_not_found{});
        }
        // clone
        auto soid = oid;
        soid.snap = *clone;
        return _load_ss(soid).then([soid,this](cached_ss_t ss) {
          auto clone_snap = ss->clone_snaps.find(soid.snap);
          assert(clone_snap != end(ss->clone_snaps));
          if (clone_snap->second.empty()) {
            logger().trace("find_object: {}@[] -- DNE", soid);
            return seastar::make_exception_future<PGBackend::cached_os_t>(
              ceph::osd::object_not_found{});
          }
          auto first = clone_snap->second.back();
          auto last = clone_snap->second.front();
          if (first > soid.snap) {
            logger().trace("find_object: {}@[{},{}] -- DNE",
                           soid, first, last);
            return seastar::make_exception_future<PGBackend::cached_os_t>(
              ceph::osd::object_not_found{});
          }
          logger().trace("find_object: {}@[{},{}] -- HIT",
                         soid, first, last);
          return _load_os(soid);
        });
      }
    });
  }
}

seastar::future<PGBackend::cached_os_t>
PGBackend::_load_os(const hobject_t& oid)
{
  if (auto found = os_cache.find(oid); found) {
    return seastar::make_ready_future<cached_os_t>(std::move(found));
  }
  return store->get_attr(coll,
                         ghobject_t{oid, ghobject_t::NO_GEN, shard},
                         OI_ATTR).then_wrapped([oid, this](auto fut) {
    if (fut.failed()) {
      auto ep = std::move(fut).get_exception();
      if (!ceph::os::FuturizedStore::EnoentException::is_class_of(ep)) {
        std::rethrow_exception(ep);
      }
      return seastar::make_ready_future<cached_os_t>(
        os_cache.insert(oid,
          std::make_unique<ObjectState>(object_info_t{oid}, false)));
    } else {
      // decode existing OI_ATTR's value
      ceph::bufferlist bl;
      bl.push_back(std::move(fut).get0());
      return seastar::make_ready_future<cached_os_t>(
        os_cache.insert(oid,
          std::make_unique<ObjectState>(object_info_t{bl}, true /* exists */)));
    }
  });
}

seastar::future<PGBackend::cached_ss_t>
PGBackend::_load_ss(const hobject_t& oid)
{
  if (auto found = ss_cache.find(oid); found) {
    return seastar::make_ready_future<cached_ss_t>(std::move(found));
  }
  return store->get_attr(coll,
                         ghobject_t{oid, ghobject_t::NO_GEN, shard},
                         SS_ATTR).then_wrapped([oid, this](auto fut) {
    std::unique_ptr<SnapSet> snapset;
    if (fut.failed()) {
      auto ep = std::move(fut).get_exception();
      if (!ceph::os::FuturizedStore::EnoentException::is_class_of(ep)) {
        std::rethrow_exception(ep);
      } else {
        snapset = std::make_unique<SnapSet>();
      }
    } else {
      // decode existing SS_ATTR's value
      ceph::bufferlist bl;
      bl.push_back(std::move(fut).get0());
      snapset = std::make_unique<SnapSet>(bl);
    }
    return seastar::make_ready_future<cached_ss_t>(
      ss_cache.insert(oid, std::move(snapset)));
  });
}

seastar::future<ceph::osd::acked_peers_t>
PGBackend::mutate_object(
  std::set<pg_shard_t> pg_shards,
  cached_os_t&& os,
  ceph::os::Transaction&& txn,
  const MOSDOp& m,
  epoch_t min_epoch,
  epoch_t map_epoch,
  eversion_t ver)
{
  logger().trace("mutate_object: num_ops={}", txn.get_num_ops());
  if (os->exists) {
#if 0
    os.oi.version = ctx->at_version;
    os.oi.prior_version = ctx->obs->oi.version;
#endif

    os->oi.last_reqid = m.get_reqid();
    os->oi.mtime = m.get_mtime();
    os->oi.local_mtime = ceph_clock_now();

    // object_info_t
    {
      ceph::bufferlist osv;
      encode(os->oi, osv, 0);
      // TODO: get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));
      txn.setattr(coll->cid, ghobject_t{os->oi.soid}, OI_ATTR, osv);
    }
  } else {
    // reset cached ObjectState without enforcing eviction
    os->oi = object_info_t(os->oi.soid);
  }
  return _submit_transaction(std::move(pg_shards), os->oi.soid, std::move(txn),
			     m.get_reqid(), min_epoch, map_epoch, ver);
}

seastar::future<>
PGBackend::evict_object_state(const hobject_t& oid)
{
  os_cache.erase(oid);
  return seastar::now();
}

seastar::future<bufferlist> PGBackend::read(const object_info_t& oi,
                                            size_t offset,
                                            size_t length,
                                            size_t truncate_size,
                                            uint32_t truncate_seq,
                                            uint32_t flags)
{
  logger().trace("read: {} {}~{}", oi.soid, offset, length);
  // are we beyond truncate_size?
  size_t size = oi.size;
  if ((truncate_seq > oi.truncate_seq) &&
      (truncate_size < offset + length) &&
      (truncate_size < size)) {
    size = truncate_size;
  }
  if (!length) {
    // read the whole object if length is 0
    length = size;
  }
  if (offset >= size) {
    // read size was trimmed to zero and it is expected to do nothing,
    return seastar::make_ready_future<bufferlist>();
  }
  std::optional<uint32_t> maybe_crc;
  if (oi.is_data_digest() && offset == 0 && length >= oi.size) {
    maybe_crc = oi.data_digest;
  }
  return _read(oi.soid, offset, length, flags).then(
    [maybe_crc, soid=oi.soid, size=oi.size](auto bl) {
      // whole object?  can we verify the checksum?
      if (maybe_crc && bl.length() == size) {
        if (auto crc = bl.crc32c(-1); crc != *maybe_crc) {
          logger().error("full-object read crc {} != expected {} on {}",
            crc, *maybe_crc, soid);
          // todo: mark soid missing, perform recovery, and retry
          throw ceph::osd::object_corrupted{};
        }
      }
      return seastar::make_ready_future<bufferlist>(std::move(bl));
    });
}

seastar::future<> PGBackend::stat(
  const ObjectState& os,
  OSDOp& osd_op)
{
  if (os.exists/* TODO: && !os.is_whiteout() */) {
    logger().debug("stat os.oi.size={}, os.oi.mtime={}", os.oi.size, os.oi.mtime);
    encode(os.oi.size, osd_op.outdata);
    encode(os.oi.mtime, osd_op.outdata);
  } else {
    logger().debug("stat object does not exist");
    throw ceph::osd::object_not_found{};
  }
  return seastar::now();
  // TODO: ctx->delta_stats.num_rd++;
}

bool PGBackend::maybe_create_new_object(
  ObjectState& os,
  ceph::os::Transaction& txn)
{
  if (!os.exists) {
    ceph_assert(!os.oi.is_whiteout());
    os.exists = true;
    os.oi.new_object();

    txn.touch(coll->cid, ghobject_t{os.oi.soid});
    // TODO: delta_stats.num_objects++
    return false;
  } else if (os.oi.is_whiteout()) {
    os.oi.clear_flag(object_info_t::FLAG_WHITEOUT);
    // TODO: delta_stats.num_whiteouts--
  }
  return true;
}

seastar::future<> PGBackend::write(
    ObjectState& os,
    const OSDOp& osd_op,
    ceph::os::Transaction& txn)
{
  const ceph_osd_op& op = osd_op.op;
  uint64_t offset = op.extent.offset;
  uint64_t length = op.extent.length;
  bufferlist buf = osd_op.indata;
  if (auto seq = os.oi.truncate_seq;
      seq != 0 && op.extent.truncate_seq < seq) {
    // old write, arrived after trimtrunc
    if (offset + length > os.oi.size) {
      // no-op
      if (offset > os.oi.size) {
	length = 0;
	buf.clear();
      } else {
	// truncate
	auto len = os.oi.size - offset;
	buf.splice(len, length);
	length = len;
      }
    }
  } else if (op.extent.truncate_seq > seq) {
    // write arrives before trimtrunc
    if (os.exists && !os.oi.is_whiteout()) {
      txn.truncate(coll->cid, ghobject_t{os.oi.soid}, op.extent.truncate_size);
    }
    os.oi.truncate_seq = op.extent.truncate_seq;
    os.oi.truncate_size = op.extent.truncate_size;
  }
  maybe_create_new_object(os, txn);
  if (length == 0) {
    if (offset > os.oi.size) {
      txn.truncate(coll->cid, ghobject_t{os.oi.soid}, op.extent.offset);
    } else {
      txn.nop();
    }
  } else {
    txn.write(coll->cid, ghobject_t{os.oi.soid},
	      offset, length, std::move(buf), op.flags);
  }
  return seastar::now();
}

seastar::future<> PGBackend::writefull(
  ObjectState& os,
  const OSDOp& osd_op,
  ceph::os::Transaction& txn)
{
  const ceph_osd_op& op = osd_op.op;
  if (op.extent.length != osd_op.indata.length()) {
    throw ceph::osd::invalid_argument();
  }

  const bool existing = maybe_create_new_object(os, txn);
  if (existing && op.extent.length < os.oi.size) {
    txn.truncate(coll->cid, ghobject_t{os.oi.soid}, op.extent.length);
  }
  if (op.extent.length) {
    txn.write(coll->cid, ghobject_t{os.oi.soid}, 0, op.extent.length,
              osd_op.indata, op.flags);
    os.oi.size = op.extent.length;
  }
  return seastar::now();
}

seastar::future<> PGBackend::remove(ObjectState& os,
                                    ceph::os::Transaction& txn)
{
  // todo: snapset
  txn.remove(coll->cid, ghobject_t{os.oi.soid, ghobject_t::NO_GEN, shard});
  os.oi.size = 0;
  os.oi.new_object();
  os.exists = false;
  // todo: update watchers
  if (os.oi.is_whiteout()) {
    os.oi.clear_flag(object_info_t::FLAG_WHITEOUT);
  }
  return seastar::now();
}

seastar::future<std::vector<hobject_t>, hobject_t>
PGBackend::list_objects(const hobject_t& start, uint64_t limit) const
{
  auto gstart = start.is_min() ? ghobject_t{} : ghobject_t{start, 0, shard};
  return store->list_objects(coll,
                             gstart,
                             ghobject_t::get_max(),
                             limit)
    .then([this](std::vector<ghobject_t> gobjects, ghobject_t next) {
      std::vector<hobject_t> objects;
      boost::copy(gobjects |
        boost::adaptors::filtered([](const ghobject_t& o) {
          if (o.is_pgmeta()) {
            return false;
          } else if (o.hobj.is_temp()) {
            return false;
          } else {
            return o.is_no_gen();
          }
        }) |
        boost::adaptors::transformed([](const ghobject_t& o) {
          return o.hobj;
        }),
        std::back_inserter(objects));
      return seastar::make_ready_future<std::vector<hobject_t>, hobject_t>(
        objects, next.hobj);
    });
}

seastar::future<> PGBackend::setxattr(
  ObjectState& os,
  const OSDOp& osd_op,
  ceph::os::Transaction& txn)
{
  if (local_conf()->osd_max_attr_size > 0 &&
      osd_op.op.xattr.value_len > local_conf()->osd_max_attr_size) {
    throw ceph::osd::make_error(-EFBIG);
  }

  const auto max_name_len = std::min<uint64_t>(
    store->get_max_attr_name_length(), local_conf()->osd_max_attr_name_len);
  if (osd_op.op.xattr.name_len > max_name_len) {
    throw ceph::osd::make_error(-ENAMETOOLONG);
  }

  maybe_create_new_object(os, txn);

  std::string name;
  ceph::bufferlist val;
  {
    auto bp = osd_op.indata.cbegin();
    std::string aname;
    bp.copy(osd_op.op.xattr.name_len, aname);
    name = "_" + aname;
    bp.copy(osd_op.op.xattr.value_len, val);
  }

  txn.setattr(coll->cid, ghobject_t{os.oi.soid}, name, val);
  return seastar::now();
  //ctx->delta_stats.num_wr++;
}

seastar::future<> PGBackend::getxattr(
  const ObjectState& os,
  OSDOp& osd_op)
{
  std::string name;
  ceph::bufferlist val;
  {
    auto bp = osd_op.indata.cbegin();
    std::string aname;
    bp.copy(osd_op.op.xattr.name_len, aname);
    name = "_" + aname;
  }
  return getxattr(os.oi.soid, name).then([&osd_op] (ceph::bufferptr val) {
    osd_op.outdata.clear();
    osd_op.outdata.push_back(std::move(val));
    osd_op.op.xattr.value_len = osd_op.outdata.length();
    //ctx->delta_stats.num_rd_kb += shift_round_up(osd_op.outdata.length(), 10);
  }).handle_exception_type(
    [] (ceph::os::FuturizedStore::EnoentException& e) {
      return seastar::make_exception_future<>(ceph::osd::object_not_found{});
  });
  //ctx->delta_stats.num_rd++;
}

seastar::future<ceph::bufferptr> PGBackend::getxattr(
  const hobject_t& soid,
  std::string_view key)
{
  return store->get_attr(coll, ghobject_t{soid}, key);
}
