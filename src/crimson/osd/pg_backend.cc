// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "pg_backend.h"

#include <optional>
#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <seastar/core/print.hh>

#include "messages/MOSDOp.h"
#include "os/Transaction.h"
#include "common/Checksummer.h"
#include "common/Clock.h"

#include "crimson/common/exception.h"
#include "crimson/os/futurized_collection.h"
#include "crimson/os/futurized_store.h"
#include "crimson/osd/osd_operation.h"
#include "replicated_backend.h"
#include "replicated_recovery_backend.h"
#include "ec_backend.h"
#include "exceptions.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

using crimson::common::local_conf;

std::unique_ptr<PGBackend>
PGBackend::create(pg_t pgid,
		  const pg_shard_t pg_shard,
		  const pg_pool_t& pool,
		  crimson::os::CollectionRef coll,
		  crimson::osd::ShardServices& shard_services,
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
                     crimson::os::FuturizedStore* store)
  : shard{shard},
    coll{coll},
    store{store}
{}

PGBackend::load_metadata_ertr::future<PGBackend::loaded_object_md_t::ref>
PGBackend::load_metadata(const hobject_t& oid)
{
  if (__builtin_expect(stopping, false)) {
    throw crimson::common::system_shutdown_exception();
  }

  return store->get_attrs(
    coll,
    ghobject_t{oid, ghobject_t::NO_GEN, shard}).safe_then(
      [oid](auto &&attrs) -> load_metadata_ertr::future<loaded_object_md_t::ref>{
	loaded_object_md_t::ref ret(new loaded_object_md_t());
	if (auto oiiter = attrs.find(OI_ATTR); oiiter != attrs.end()) {
	  bufferlist bl;
	  bl.push_back(std::move(oiiter->second));
	  ret->os = ObjectState(
	    object_info_t(bl),
	    true);
	} else {
	  logger().error(
	    "load_metadata: object {} present but missing object info",
	    oid);
	  return crimson::ct_error::object_corrupted::make();
	}
	
	if (oid.is_head()) {
	  if (auto ssiter = attrs.find(SS_ATTR); ssiter != attrs.end()) {
	    bufferlist bl;
	    bl.push_back(std::move(ssiter->second));
	    ret->ss = SnapSet(bl);
	  } else {
	    /* TODO: add support for writing out snapsets
	    logger().error(
	      "load_metadata: object {} present but missing snapset",
	      oid);
	    //return crimson::ct_error::object_corrupted::make();
	    */
	    ret->ss = SnapSet();
	  }
	}

	return load_metadata_ertr::make_ready_future<loaded_object_md_t::ref>(
	  std::move(ret));
      }, crimson::ct_error::enoent::handle([oid] {
	logger().debug(
	  "load_metadata: object {} doesn't exist, returning empty metadata",
	  oid);
	return load_metadata_ertr::make_ready_future<loaded_object_md_t::ref>(
	  new loaded_object_md_t{
	    ObjectState(
	      object_info_t(oid),
	      false),
	    oid.is_head() ? std::optional<SnapSet>(SnapSet()) : std::nullopt
	  });
      }));
}

seastar::future<crimson::osd::acked_peers_t>
PGBackend::mutate_object(
  std::set<pg_shard_t> pg_shards,
  crimson::osd::ObjectContextRef &&obc,
  ceph::os::Transaction&& txn,
  const osd_op_params_t& osd_op_p,
  epoch_t min_epoch,
  epoch_t map_epoch,
  std::vector<pg_log_entry_t>&& log_entries)
{
  if (__builtin_expect((bool)peering, false)) {
    throw crimson::common::actingset_changed(peering->is_primary);
  }
  logger().trace("mutate_object: num_ops={}", txn.get_num_ops());
  if (obc->obs.exists) {
#if 0
    obc->obs.oi.version = ctx->at_version;
    obc->obs.oi.prior_version = ctx->obs->oi.version;
#endif

    auto& m = osd_op_p.req;
    obc->obs.oi.prior_version = obc->obs.oi.version;
    obc->obs.oi.version = osd_op_p.at_version;
    if (osd_op_p.user_at_version > obc->obs.oi.user_version)
      obc->obs.oi.user_version = osd_op_p.user_at_version;
    obc->obs.oi.last_reqid = m->get_reqid();
    obc->obs.oi.mtime = m->get_mtime();
    obc->obs.oi.local_mtime = ceph_clock_now();

    // object_info_t
    {
      ceph::bufferlist osv;
      encode(obc->obs.oi, osv, 0);
      // TODO: get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));
      txn.setattr(coll->get_cid(), ghobject_t{obc->obs.oi.soid}, OI_ATTR, osv);
    }
  } else {
    // reset cached ObjectState without enforcing eviction
    obc->obs.oi = object_info_t(obc->obs.oi.soid);
  }
  return _submit_transaction(
    std::move(pg_shards), obc->obs.oi.soid, std::move(txn),
    std::move(osd_op_p), min_epoch, map_epoch, std::move(log_entries));
}

static inline bool _read_verify_data(
  const object_info_t& oi,
  const ceph::bufferlist& data)
{
  if (oi.is_data_digest() && oi.size == data.length()) {
    // whole object?  can we verify the checksum?
    if (auto crc = data.crc32c(-1); crc != oi.data_digest) {
      logger().error("full-object read crc {} != expected {} on {}",
                     crc, oi.data_digest, oi.soid);
      // todo: mark soid missing, perform recovery, and retry
      return false;
    }
  }
  return true;
}

PGBackend::read_errorator::future<ceph::bufferlist>
PGBackend::read(const object_info_t& oi,
                const size_t offset,
                size_t length,
                const size_t truncate_size,
                const uint32_t truncate_seq,
                const uint32_t flags)
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
    return read_errorator::make_ready_future<bufferlist>();
  }
  return _read(oi.soid, offset, length, flags).safe_then(
    [&oi](auto&& bl) -> read_errorator::future<ceph::bufferlist> {
      if (const bool is_fine = _read_verify_data(oi, bl); is_fine) {
	logger().debug("read: data length: {}", bl.length());
        return read_errorator::make_ready_future<bufferlist>(std::move(bl));
      } else {
        return crimson::ct_error::object_corrupted::make();
      }
    });
}

PGBackend::read_errorator::future<>
PGBackend::sparse_read(const ObjectState& os, OSDOp& osd_op)
{
  const auto& op = osd_op.op;
  logger().trace("sparse_read: {} {}~{}",
                 os.oi.soid, op.extent.offset, op.extent.length);
  return store->fiemap(coll, ghobject_t{os.oi.soid},
		       op.extent.offset,
		       op.extent.length).then([&os, &osd_op, this](auto&& m) {
    return seastar::do_with(interval_set<uint64_t>{std::move(m)},
			    [&os, &osd_op, this](auto&& extents) {
      return store->readv(coll, ghobject_t{os.oi.soid},
                          extents, osd_op.op.flags).safe_then(
        [&os, &osd_op, &extents, this](auto&& bl) -> read_errorator::future<> {
        if (_read_verify_data(os.oi, bl)) {
          osd_op.op.extent.length = bl.length();
          // re-encode since it might be modified
          ceph::encode(extents, osd_op.outdata);
          encode_destructively(bl, osd_op.outdata);
          logger().trace("sparse_read got {} bytes from object {}",
                         osd_op.op.extent.length, os.oi.soid);
          return read_errorator::make_ready_future<>();
        } else {
          // TODO: repair it if crc mismatches
          return crimson::ct_error::object_corrupted::make();
        }
      });
    });
  });
}

namespace {

  template<class CSum>
  PGBackend::checksum_errorator::future<>
  do_checksum(ceph::bufferlist& init_value_bl,
	      size_t chunk_size,
	      const ceph::bufferlist& buf,
	      ceph::bufferlist& result)
  {
    typename CSum::init_value_t init_value;
    auto init_value_p = init_value_bl.cbegin();
    try {
      decode(init_value, init_value_p);
      // chop off the consumed part
      init_value_bl.splice(0, init_value_p.get_off());
    } catch (const ceph::buffer::end_of_buffer&) {
      logger().warn("{}: init value not provided", __func__);
      return crimson::ct_error::invarg::make();
    }
    const uint32_t chunk_count = buf.length() / chunk_size;
    ceph::bufferptr csum_data{
      ceph::buffer::create(sizeof(typename CSum::value_t) * chunk_count)};
    Checksummer::calculate<CSum>(
      init_value, chunk_size, 0, buf.length(), buf, &csum_data);
    encode(chunk_count, result);
    result.append(std::move(csum_data));
    return PGBackend::checksum_errorator::now();
  }
}

PGBackend::checksum_errorator::future<>
PGBackend::checksum(const ObjectState& os, OSDOp& osd_op)
{
  // sanity tests and normalize the argments
  auto& checksum = osd_op.op.checksum;
  if (checksum.offset == 0 && checksum.length == 0) {
    // zeroed offset+length implies checksum whole object
    checksum.length = os.oi.size;
  } else if (checksum.offset >= os.oi.size) {
    // read size was trimmed to zero, do nothing,
    // see PGBackend::read()
    return checksum_errorator::now();
  }
  if (checksum.chunk_size > 0) {
    if (checksum.length == 0) {
      logger().warn("{}: length required when chunk size provided", __func__);
      return crimson::ct_error::invarg::make();
    }
    if (checksum.length % checksum.chunk_size != 0) {
      logger().warn("{}: length not aligned to chunk size", __func__);
      return crimson::ct_error::invarg::make();
    }
  } else {
    checksum.chunk_size = checksum.length;
  }
  if (checksum.length == 0) {
    uint32_t count = 0;
    encode(count, osd_op.outdata);
    return checksum_errorator::now();
  }

  // read the chunk to be checksum'ed
  return _read(os.oi.soid, checksum.offset, checksum.length, osd_op.op.flags).safe_then(
    [&osd_op](auto&& read_bl) mutable -> checksum_errorator::future<> {
    auto& checksum = osd_op.op.checksum;
    if (read_bl.length() != checksum.length) {
      logger().warn("checksum: bytes read {} != {}",
                        read_bl.length(), checksum.length);
      return crimson::ct_error::invarg::make();
    }
    // calculate its checksum and put the result in outdata
    switch (checksum.type) {
    case CEPH_OSD_CHECKSUM_OP_TYPE_XXHASH32:
      return do_checksum<Checksummer::xxhash32>(osd_op.indata,
                                                checksum.chunk_size,
                                                read_bl,
                                                osd_op.outdata);
    case CEPH_OSD_CHECKSUM_OP_TYPE_XXHASH64:
      return do_checksum<Checksummer::xxhash64>(osd_op.indata,
                                                checksum.chunk_size,
                                                read_bl,
                                                osd_op.outdata);
    case CEPH_OSD_CHECKSUM_OP_TYPE_CRC32C:
      return do_checksum<Checksummer::crc32c>(osd_op.indata,
                                              checksum.chunk_size,
                                              read_bl,
                                              osd_op.outdata);
    default:
      logger().warn("checksum: unknown crc type ({})",
		    static_cast<uint32_t>(checksum.type));
      return crimson::ct_error::invarg::make();
    }
  });
}

PGBackend::stat_errorator::future<> PGBackend::stat(
  const ObjectState& os,
  OSDOp& osd_op)
{
  if (os.exists/* TODO: && !os.is_whiteout() */) {
    logger().debug("stat os.oi.size={}, os.oi.mtime={}", os.oi.size, os.oi.mtime);
    encode(os.oi.size, osd_op.outdata);
    encode(os.oi.mtime, osd_op.outdata);
  } else {
    logger().debug("stat object does not exist");
    return crimson::ct_error::enoent::make();
  }
  return stat_errorator::now();
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

    txn.touch(coll->get_cid(), ghobject_t{os.oi.soid});
    // TODO: delta_stats.num_objects++
    return false;
  } else if (os.oi.is_whiteout()) {
    os.oi.clear_flag(object_info_t::FLAG_WHITEOUT);
    // TODO: delta_stats.num_whiteouts--
  }
  return true;
}

static bool is_offset_and_length_valid(
  const std::uint64_t offset,
  const std::uint64_t length)
{
  if (const std::uint64_t max = local_conf()->osd_max_object_size;
      offset >= max || length > max || offset + length > max) {
    logger().debug("{} osd_max_object_size: {}, offset: {}, len: {}; "
                   "Hard limit of object size is 4GB",
                   __func__, max, offset, length);
    return false;
  } else {
    return true;
  }
}

seastar::future<> PGBackend::write(
    ObjectState& os,
    const OSDOp& osd_op,
    ceph::os::Transaction& txn,
    osd_op_params_t& osd_op_params)
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
      txn.truncate(coll->get_cid(),
                   ghobject_t{os.oi.soid}, op.extent.truncate_size);
      if (op.extent.truncate_size != os.oi.size) {
        os.oi.size = length;
        // TODO: truncate_update_size_and_usage()
	if (op.extent.truncate_size > os.oi.size) {
	  osd_op_params.clean_regions.mark_data_region_dirty(os.oi.size,
	      op.extent.truncate_size - os.oi.size);
	} else {
	  osd_op_params.clean_regions.mark_data_region_dirty(op.extent.truncate_size,
	      os.oi.size - op.extent.truncate_size);
	}
      }
    }
    os.oi.truncate_seq = op.extent.truncate_seq;
    os.oi.truncate_size = op.extent.truncate_size;
  }
  maybe_create_new_object(os, txn);
  if (length == 0) {
    if (offset > os.oi.size) {
      txn.truncate(coll->get_cid(), ghobject_t{os.oi.soid}, op.extent.offset);
    } else {
      txn.nop();
    }
  } else {
    txn.write(coll->get_cid(), ghobject_t{os.oi.soid},
	      offset, length, std::move(buf), op.flags);
    os.oi.size = std::max(offset + length, os.oi.size);
  }
  osd_op_params.clean_regions.mark_data_region_dirty(op.extent.offset,
						     op.extent.length);

  return seastar::now();
}

seastar::future<> PGBackend::writefull(
  ObjectState& os,
  const OSDOp& osd_op,
  ceph::os::Transaction& txn,
  osd_op_params_t& osd_op_params)
{
  const ceph_osd_op& op = osd_op.op;
  if (op.extent.length != osd_op.indata.length()) {
    throw crimson::osd::invalid_argument();
  }

  const bool existing = maybe_create_new_object(os, txn);
  if (existing && op.extent.length < os.oi.size) {
    txn.truncate(coll->get_cid(), ghobject_t{os.oi.soid}, op.extent.length);
    osd_op_params.clean_regions.mark_data_region_dirty(op.extent.length,
	os.oi.size - op.extent.length);
  }
  if (op.extent.length) {
    txn.write(coll->get_cid(), ghobject_t{os.oi.soid}, 0, op.extent.length,
              osd_op.indata, op.flags);
    os.oi.size = op.extent.length;
    osd_op_params.clean_regions.mark_data_region_dirty(0,
	std::max((uint64_t) op.extent.length, os.oi.size));
  }
  return seastar::now();
}

PGBackend::write_ertr::future<> PGBackend::truncate(
  ObjectState& os,
  const OSDOp& osd_op,
  ceph::os::Transaction& txn,
  osd_op_params_t& osd_op_params)
{
  if (!os.exists || os.oi.is_whiteout()) {
    logger().debug("{} object dne, truncate is a no-op", __func__);
    return write_ertr::now();
  }
  const ceph_osd_op& op = osd_op.op;
  if (!is_offset_and_length_valid(op.extent.offset, op.extent.length)) {
    return crimson::ct_error::file_too_large::make();
  }
  if (op.extent.truncate_seq) {
    assert(op.extent.offset == op.extent.truncate_size);
    if (op.extent.truncate_seq <= os.oi.truncate_seq) {
      logger().debug("{} truncate seq {} <= current {}, no-op",
                     __func__, op.extent.truncate_seq, os.oi.truncate_seq);
      return write_ertr::make_ready_future<>();
    } else {
      logger().debug("{} truncate seq {} > current {}, truncating",
                     __func__, op.extent.truncate_seq, os.oi.truncate_seq);
      os.oi.truncate_seq = op.extent.truncate_seq;
      os.oi.truncate_size = op.extent.truncate_size;
    }
  }
  maybe_create_new_object(os, txn);
  txn.truncate(coll->get_cid(),
               ghobject_t{os.oi.soid}, op.extent.offset);
  if (os.oi.size > op.extent.offset) {
    // TODO: modified_ranges.union_of(trim);
    osd_op_params.clean_regions.mark_data_region_dirty(op.extent.offset,
        os.oi.size - op.extent.offset);
  } else if (os.oi.size < op.extent.offset) {
    osd_op_params.clean_regions.mark_data_region_dirty(os.oi.size,
        op.extent.offset - os.oi.size);
  }
  // TODO: truncate_update_size_and_usage()
  // TODO: ctx->delta_stats.num_wr++;
  // ----
  // do no set exists, or we will break above DELETE -> TRUNCATE munging.
  os.oi.clear_data_digest();
  return write_ertr::now();
}

PGBackend::write_ertr::future<> PGBackend::zero(
  ObjectState& os,
  const OSDOp& osd_op,
  ceph::os::Transaction& txn,
  osd_op_params_t& osd_op_params)
{
  if (!os.exists || os.oi.is_whiteout()) {
    logger().debug("{} object dne, zero is a no-op", __func__);
    return write_ertr::now();
  }
  const ceph_osd_op& op = osd_op.op;
  if (!is_offset_and_length_valid(op.extent.offset, op.extent.length)) {
    return crimson::ct_error::file_too_large::make();
  }
  assert(op.extent.length);
  txn.zero(coll->get_cid(),
           ghobject_t{os.oi.soid},
           op.extent.offset,
           op.extent.length);
  // TODO: modified_ranges.union_of(zeroed);
  osd_op_params.clean_regions.mark_data_region_dirty(op.extent.offset,
						     op.extent.length);
  // TODO: ctx->delta_stats.num_wr++;
  os.oi.clear_data_digest();
  return write_ertr::now();
}

seastar::future<> PGBackend::create(
  ObjectState& os,
  const OSDOp& osd_op,
  ceph::os::Transaction& txn)
{
  if (os.exists && !os.oi.is_whiteout() &&
      (osd_op.op.flags & CEPH_OSD_OP_FLAG_EXCL)) {
    // this is an exclusive create
    throw crimson::osd::make_error(-EEXIST);
  }

  if (osd_op.indata.length()) {
    // handle the legacy. `category` is no longer implemented.
    try {
      auto p = osd_op.indata.cbegin();
      std::string category;
      decode(category, p);
    } catch (buffer::error&) {
      throw crimson::osd::invalid_argument();
    }
  }
  maybe_create_new_object(os, txn);
  txn.nop();
  return seastar::now();
}

seastar::future<> PGBackend::remove(ObjectState& os,
                                    ceph::os::Transaction& txn)
{
  // todo: snapset
  txn.remove(coll->get_cid(),
	     ghobject_t{os.oi.soid, ghobject_t::NO_GEN, shard});
  os.oi.size = 0;
  os.oi.new_object();
  os.exists = false;
  // todo: update watchers
  if (os.oi.is_whiteout()) {
    os.oi.clear_flag(object_info_t::FLAG_WHITEOUT);
  }
  return seastar::now();
}

seastar::future<std::tuple<std::vector<hobject_t>, hobject_t>>
PGBackend::list_objects(const hobject_t& start, uint64_t limit) const
{
  if (__builtin_expect(stopping, false)) {
    throw crimson::common::system_shutdown_exception();
  }

  auto gstart = start.is_min() ? ghobject_t{} : ghobject_t{start, 0, shard};
  return store->list_objects(coll,
                             gstart,
                             ghobject_t::get_max(),
                             limit)
    .then([](auto ret) {
      auto& [gobjects, next] = ret;
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
      return seastar::make_ready_future<std::tuple<std::vector<hobject_t>, hobject_t>>(
        std::make_tuple(objects, next.hobj));
    });
}

seastar::future<> PGBackend::setxattr(
  ObjectState& os,
  OSDOp& osd_op,
  ceph::os::Transaction& txn)
{
  if (local_conf()->osd_max_attr_size > 0 &&
      osd_op.op.xattr.value_len > local_conf()->osd_max_attr_size) {
    throw crimson::osd::make_error(-EFBIG);
  }

  const auto max_name_len = std::min<uint64_t>(
    store->get_max_attr_name_length(), local_conf()->osd_max_attr_name_len);
  if (osd_op.op.xattr.name_len > max_name_len) {
    throw crimson::osd::make_error(-ENAMETOOLONG);
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
    osd_op.indata.splice(0, bp.get_off());
  }
  logger().debug("setxattr on obj={} for attr={}", os.oi.soid, name);

  txn.setattr(coll->get_cid(), ghobject_t{os.oi.soid}, name, val);
  return seastar::now();
  //ctx->delta_stats.num_wr++;
}

PGBackend::get_attr_errorator::future<> PGBackend::getxattr(
  const ObjectState& os,
  OSDOp& osd_op) const
{
  std::string name;
  ceph::bufferlist val;
  {
    auto bp = osd_op.indata.cbegin();
    std::string aname;
    bp.copy(osd_op.op.xattr.name_len, aname);
    name = "_" + aname;
    osd_op.indata.splice(0, bp.get_off());
  }
  logger().debug("getxattr on obj={} for attr={}", os.oi.soid, name);
  return getxattr(os.oi.soid, name).safe_then([&osd_op] (ceph::bufferptr val) {
    osd_op.outdata.clear();
    osd_op.outdata.push_back(std::move(val));
    osd_op.op.xattr.value_len = osd_op.outdata.length();
    return get_attr_errorator::now();
    //ctx->delta_stats.num_rd_kb += shift_round_up(osd_op.outdata.length(), 10);
  });
  //ctx->delta_stats.num_rd++;
}

PGBackend::get_attr_errorator::future<ceph::bufferptr> PGBackend::getxattr(
  const hobject_t& soid,
  std::string_view key) const
{
  if (__builtin_expect(stopping, false)) {
    throw crimson::common::system_shutdown_exception();
  }

  return store->get_attr(coll, ghobject_t{soid}, key);
}

PGBackend::get_attr_errorator::future<> PGBackend::get_xattrs(
  const ObjectState& os,
  OSDOp& osd_op) const
{
  if (__builtin_expect(stopping, false)) {
    throw crimson::common::system_shutdown_exception();
  }
  return store->get_attrs(coll, ghobject_t{os.oi.soid}).safe_then(
    [&osd_op](auto&& attrs) {
    std::vector<std::pair<std::string, bufferlist>> user_xattrs;
    for (auto& [key, val] : attrs) {
      if (key.size() > 1 && key[0] == '_') {
	ceph::bufferlist bl;
	bl.append(std::move(val));
	user_xattrs.emplace_back(key.substr(1), std::move(bl));
      }
    }
    ceph::encode(user_xattrs, osd_op.outdata);
    return get_attr_errorator::now();
  });
}

static seastar::future<crimson::os::FuturizedStore::omap_values_t>
maybe_get_omap_vals_by_keys(
  crimson::os::FuturizedStore* store,
  const crimson::os::CollectionRef& coll,
  const object_info_t& oi,
  const std::set<std::string>& keys_to_get)
{
  if (oi.is_omap()) {
    return store->omap_get_values(coll, ghobject_t{oi.soid}, keys_to_get);
  } else {
    return seastar::make_ready_future<crimson::os::FuturizedStore::omap_values_t>(
      crimson::os::FuturizedStore::omap_values_t{});
  }
}

static seastar::future<std::tuple<bool, crimson::os::FuturizedStore::omap_values_t>>
maybe_get_omap_vals(
  crimson::os::FuturizedStore* store,
  const crimson::os::CollectionRef& coll,
  const object_info_t& oi,
  const std::string& start_after)
{
  if (oi.is_omap()) {
    return store->omap_get_values(coll, ghobject_t{oi.soid}, start_after);
  } else {
    return seastar::make_ready_future<std::tuple<bool, crimson::os::FuturizedStore::omap_values_t>>(
      std::make_tuple(true, crimson::os::FuturizedStore::omap_values_t{}));
  }
}

seastar::future<ceph::bufferlist> PGBackend::omap_get_header(
  const crimson::os::CollectionRef& c,
  const ghobject_t& oid) const
{
  return store->omap_get_header(c, oid);
}

seastar::future<> PGBackend::omap_get_header(
  const ObjectState& os,
  OSDOp& osd_op) const
{
  return omap_get_header(coll, ghobject_t{os.oi.soid}).then(
    [&osd_op] (ceph::bufferlist&& header) {
      osd_op.outdata = std::move(header);
      return seastar::now();
    });
}

seastar::future<> PGBackend::omap_get_keys(
  const ObjectState& os,
  OSDOp& osd_op) const
{
  if (__builtin_expect(stopping, false)) {
    throw crimson::common::system_shutdown_exception();
  }

  std::string start_after;
  uint64_t max_return;
  try {
    auto p = osd_op.indata.cbegin();
    decode(start_after, p);
    decode(max_return, p);
    osd_op.indata.splice(0, p.get_off());
  } catch (buffer::error&) {
    throw crimson::osd::invalid_argument{};
  }
  max_return =
    std::min(max_return, local_conf()->osd_max_omap_entries_per_request);

  // TODO: truly chunk the reading
  return maybe_get_omap_vals(store, coll, os.oi, start_after).then(
    [=, &osd_op] (auto ret) {
      ceph::bufferlist result;
      bool truncated = false;
      uint32_t num = 0;
      for (auto& [key, val] : std::get<1>(ret)) {
        if (num >= max_return ||
            result.length() >= local_conf()->osd_max_omap_bytes_per_request) {
          truncated = true;
          break;
        }
        encode(key, result);
        ++num;
      }
      encode(num, osd_op.outdata);
      osd_op.outdata.claim_append(result);
      encode(truncated, osd_op.outdata);
      return seastar::now();
    });

  // TODO:
  //ctx->delta_stats.num_rd_kb += shift_round_up(osd_op.outdata.length(), 10);
  //ctx->delta_stats.num_rd++;
}

seastar::future<> PGBackend::omap_get_vals(
  const ObjectState& os,
  OSDOp& osd_op) const
{
  if (__builtin_expect(stopping, false)) {
    throw crimson::common::system_shutdown_exception();
  }

  std::string start_after;
  uint64_t max_return;
  std::string filter_prefix;
  try {
    auto p = osd_op.indata.cbegin();
    decode(start_after, p);
    decode(max_return, p);
    decode(filter_prefix, p);
    osd_op.indata.splice(0, p.get_off());
  } catch (buffer::error&) {
    throw crimson::osd::invalid_argument{};
  }

  max_return = \
    std::min(max_return, local_conf()->osd_max_omap_entries_per_request);

  // TODO: truly chunk the reading
  return maybe_get_omap_vals(store, coll, os.oi, start_after).then(
    [=, &osd_op] (auto&& ret) {
      auto [done, vals] = std::move(ret);
      assert(done);
      ceph::bufferlist result;
      bool truncated = false;
      uint32_t num = 0;
      auto iter = filter_prefix > start_after ? vals.lower_bound(filter_prefix)
                                              : std::begin(vals);
      for (; iter != std::end(vals); ++iter) {
        const auto& [key, value] = *iter;
        if (key.substr(0, filter_prefix.size()) != filter_prefix) {
          break;
        } else if (num >= max_return ||
            result.length() >= local_conf()->osd_max_omap_bytes_per_request) {
          truncated = true;
          break;
        }
        encode(key, result);
        encode(value, result);
        ++num;
      }
      encode(num, osd_op.outdata);
      osd_op.outdata.claim_append(result);
      encode(truncated, osd_op.outdata);
      return seastar::now();
    });

  // TODO:
  //ctx->delta_stats.num_rd_kb += shift_round_up(osd_op.outdata.length(), 10);
  //ctx->delta_stats.num_rd++;
}
seastar::future<> PGBackend::omap_get_vals_by_keys(
  const ObjectState& os,
  OSDOp& osd_op) const
{
  if (__builtin_expect(stopping, false)) {
    throw crimson::common::system_shutdown_exception();
  }

  std::set<std::string> keys_to_get;
  try {
    auto p = osd_op.indata.cbegin();
    decode(keys_to_get, p);
    osd_op.indata.splice(0, p.get_off());
  } catch (buffer::error&) {
    throw crimson::osd::invalid_argument();
  }

  return maybe_get_omap_vals_by_keys(store, coll, os.oi, keys_to_get).then(
    [&osd_op] (crimson::os::FuturizedStore::omap_values_t vals) {
      encode(vals, osd_op.outdata);
      return seastar::now();
    });

  // TODO:
  //ctx->delta_stats.num_rd_kb += shift_round_up(osd_op.outdata.length(), 10);
  //ctx->delta_stats.num_rd++;
}

seastar::future<> PGBackend::omap_set_vals(
  ObjectState& os,
  OSDOp& osd_op,
  ceph::os::Transaction& txn,
  osd_op_params_t& osd_op_params)
{
  maybe_create_new_object(os, txn);

  ceph::bufferlist to_set_bl;
  try {
    auto p = osd_op.indata.cbegin();
    decode_str_str_map_to_bl(p, &to_set_bl);
    osd_op.indata.splice(0, p.get_off());
  } catch (buffer::error&) {
    throw crimson::osd::invalid_argument{};
  }

  txn.omap_setkeys(coll->get_cid(), ghobject_t{os.oi.soid}, to_set_bl);

  // TODO:
  //ctx->clean_regions.mark_omap_dirty();

  // TODO:
  //ctx->delta_stats.num_wr++;
  //ctx->delta_stats.num_wr_kb += shift_round_up(to_set_bl.length(), 10);
  os.oi.set_flag(object_info_t::FLAG_OMAP);
  os.oi.clear_omap_digest();
  osd_op_params.clean_regions.mark_omap_dirty();
  return seastar::now();
}

seastar::future<> PGBackend::omap_set_header(
  ObjectState& os,
  const OSDOp& osd_op,
  ceph::os::Transaction& txn)
{
  maybe_create_new_object(os, txn);
  txn.omap_setheader(coll->get_cid(), ghobject_t{os.oi.soid}, osd_op.indata);
  //TODO:
  //ctx->clean_regions.mark_omap_dirty();
  //ctx->delta_stats.num_wr++;
  os.oi.set_flag(object_info_t::FLAG_OMAP);
  os.oi.clear_omap_digest();
  return seastar::now();
}

seastar::future<> PGBackend::omap_remove_range(
  ObjectState& os,
  OSDOp& osd_op,
  ceph::os::Transaction& txn)
{
  std::string key_begin, key_end;
  try {
    auto p = osd_op.indata.cbegin();
    decode(key_begin, p);
    decode(key_end, p);
    osd_op.indata.splice(0, p.get_off());
  } catch (buffer::error& e) {
    throw crimson::osd::invalid_argument{};
  }
  txn.omap_rmkeyrange(coll->get_cid(), ghobject_t{os.oi.soid}, key_begin, key_end);
  //TODO:
  //ctx->delta_stats.num_wr++;
  os.oi.clear_omap_digest();
  return seastar::now();
}

seastar::future<struct stat> PGBackend::stat(
  CollectionRef c,
  const ghobject_t& oid) const
{
  return store->stat(c, oid);
}

seastar::future<std::map<uint64_t, uint64_t>>
PGBackend::fiemap(
  CollectionRef c,
  const ghobject_t& oid,
  uint64_t off,
  uint64_t len)
{
  return store->fiemap(c, oid, off, len);
}

void PGBackend::on_activate_complete() {
  peering.reset();
}

