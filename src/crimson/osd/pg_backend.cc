// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "pg_backend.h"

#include <charconv>
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
#include "crimson/common/tmap_helpers.h"
#include "crimson/os/futurized_collection.h"
#include "crimson/os/futurized_store.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/osd/object_context_loader.h"
#include "crimson/osd/pg.h"
#include "replicated_backend.h"
#include "replicated_recovery_backend.h"
#include "ec_backend.h"
#include "exceptions.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

using std::runtime_error;
using std::string;
using std::string_view;
using crimson::common::local_conf;

std::unique_ptr<PGBackend>
PGBackend::create(pg_t pgid,
		  const pg_shard_t pg_shard,
		  const pg_pool_t& pool,
		  crimson::osd::PG& pg,
		  crimson::os::CollectionRef coll,
		  crimson::osd::ShardServices& shard_services,
		  const ec_profile_t& ec_profile,
		  DoutPrefixProvider &dpp)
{
  switch (pool.type) {
  case pg_pool_t::TYPE_REPLICATED:
    return std::make_unique<ReplicatedBackend>(pgid, pg_shard, pg,
					       coll, shard_services,
					       dpp);
  case pg_pool_t::TYPE_ERASURE:
    return std::make_unique<ECBackend>(pg_shard.shard, coll, shard_services,
                                       std::move(ec_profile),
                                       pool.stripe_width,
				       dpp);
  default:
    throw runtime_error(seastar::format("unsupported pool type '{}'",
                                        pool.type));
  }
}

PGBackend::PGBackend(shard_id_t shard,
                     CollectionRef coll,
                     crimson::osd::ShardServices &shard_services,
		     DoutPrefixProvider &dpp)
  : shard{shard},
    coll{coll},
    shard_services{shard_services},
    dpp{dpp},
    store{&shard_services.get_store()}
{}

PGBackend::load_metadata_iertr::future
  <PGBackend::loaded_object_md_t::ref>
PGBackend::load_metadata(const hobject_t& oid)
{
  return interruptor::make_interruptible(store->get_attrs(
    coll,
    ghobject_t{oid, ghobject_t::NO_GEN, shard})).safe_then_interruptible(
      [oid](auto &&attrs) -> load_metadata_ertr::future<loaded_object_md_t::ref>{
        loaded_object_md_t::ref ret(new loaded_object_md_t());
        if (auto oiiter = attrs.find(OI_ATTR); oiiter != attrs.end()) {
          bufferlist bl = std::move(oiiter->second);
          try {
            ret->os = ObjectState(
              object_info_t(bl, oid),
              true);
          } catch (const buffer::error&) {
            logger().warn("unable to decode ObjectState");
            throw crimson::osd::invalid_argument();
          }
        } else {
          logger().error(
            "load_metadata: object {} present but missing object info",
            oid);
          return crimson::ct_error::object_corrupted::make();
        }

        if (oid.is_head()) {
          // Returning object_corrupted when the object exsits and the
          // Snapset is either not found or empty.
          bool object_corrupted = true;
          if (auto ssiter = attrs.find(SS_ATTR); ssiter != attrs.end()) {
            object_corrupted = false;
            bufferlist bl = std::move(ssiter->second);
            if (bl.length()) {
              ret->ssc = new crimson::osd::SnapSetContext(oid.get_snapdir());
              try {
                ret->ssc->snapset = SnapSet(bl);
                ret->ssc->exists = true;
                logger().debug(
                  "load_metadata: object {} and snapset {} present",
                   oid, ret->ssc->snapset);
              } catch (const buffer::error&) {
                logger().warn("unable to decode SnapSet");
                throw crimson::osd::invalid_argument();
              }
            } else {
              object_corrupted = true;
            }
          }
          if (object_corrupted) {
            logger().error(
              "load_metadata: object {} present but missing snapset",
              oid);
            return crimson::ct_error::object_corrupted::make();
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
            oid.is_head() ? (new crimson::osd::SnapSetContext(oid)) : nullptr
          });
      }));
}

PGBackend::rep_op_fut_t
PGBackend::mutate_object(
  std::set<pg_shard_t> pg_shards,
  crimson::osd::ObjectContextRef &&obc,
  ceph::os::Transaction&& txn,
  osd_op_params_t&& osd_op_p,
  epoch_t min_epoch,
  epoch_t map_epoch,
  std::vector<pg_log_entry_t>&& log_entries)
{
  logger().trace("mutate_object: num_ops={}", txn.get_num_ops());
  if (obc->obs.exists) {
#if 0
    obc->obs.oi.version = ctx->at_version;
    obc->obs.oi.prior_version = ctx->obs->oi.version;
#endif

    obc->obs.oi.prior_version = obc->obs.oi.version;
    obc->obs.oi.version = osd_op_p.at_version;
    if (osd_op_p.user_at_version > obc->obs.oi.user_version)
      obc->obs.oi.user_version = osd_op_p.user_at_version;
    obc->obs.oi.last_reqid = osd_op_p.req_id;
    obc->obs.oi.mtime = osd_op_p.mtime;
    obc->obs.oi.local_mtime = ceph_clock_now();

    // object_info_t
    {
      ceph::bufferlist osv;
      obc->obs.oi.encode(osv, CEPH_FEATURES_ALL);
      // TODO: get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));
      txn.setattr(coll->get_cid(), ghobject_t{obc->obs.oi.soid}, OI_ATTR, osv);
    }

    // snapset
    if (obc->obs.oi.soid.snap == CEPH_NOSNAP) {
      logger().debug("final snapset {} in {}",
        obc->ssc->snapset, obc->obs.oi.soid);
      ceph::bufferlist bss;
      encode(obc->ssc->snapset, bss);
      txn.setattr(coll->get_cid(), ghobject_t{obc->obs.oi.soid}, SS_ATTR, bss);
      obc->ssc->exists = true;
    } else {
      logger().debug("no snapset (this is a clone)");
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

PGBackend::read_ierrorator::future<>
PGBackend::read(const ObjectState& os, OSDOp& osd_op,
                object_stat_sum_t& delta_stats)
{
  const auto& oi = os.oi;
  const ceph_osd_op& op = osd_op.op;
  const uint64_t offset = op.extent.offset;
  uint64_t length = op.extent.length;
  logger().trace("read: {} {}~{}", oi.soid, offset, length);

  if (!os.exists || os.oi.is_whiteout()) {
    logger().debug("{}: {} DNE", __func__, os.oi.soid);
    return crimson::ct_error::enoent::make();
  }
  // are we beyond truncate_size?
  size_t size = oi.size;
  if ((op.extent.truncate_seq > oi.truncate_seq) &&
      (op.extent.truncate_size < offset + length) &&
      (op.extent.truncate_size < size)) {
    size = op.extent.truncate_size;
  }
  if (offset >= size) {
    // read size was trimmed to zero and it is expected to do nothing,
    return read_errorator::now();
  }
  if (!length) {
    // read the whole object if length is 0
    length = size;
  }
  return _read(oi.soid, offset, length, op.flags).safe_then_interruptible_tuple(
    [&delta_stats, &oi, &osd_op](auto&& bl) -> read_errorator::future<> {
    if (!_read_verify_data(oi, bl)) {
      // crc mismatches
      return crimson::ct_error::object_corrupted::make();
    }
    logger().debug("read: data length: {}", bl.length());
    osd_op.op.extent.length = bl.length();
    osd_op.rval = 0;
    delta_stats.num_rd++;
    delta_stats.num_rd_kb += shift_round_up(bl.length(), 10);
    osd_op.outdata = std::move(bl);
    return read_errorator::now();
  }, crimson::ct_error::input_output_error::handle([] {
    return read_errorator::future<>{crimson::ct_error::object_corrupted::make()};
  }),
  read_errorator::pass_further{});
}

PGBackend::read_ierrorator::future<>
PGBackend::sparse_read(const ObjectState& os, OSDOp& osd_op,
                object_stat_sum_t& delta_stats)
{
  if (!os.exists || os.oi.is_whiteout()) {
    logger().debug("{}: {} DNE", __func__, os.oi.soid);
    return crimson::ct_error::enoent::make();
  }

  const auto& op = osd_op.op;
  /* clients (particularly cephfs) may send truncate operations out of order
   * w.r.t. reads.  op.extent.truncate_seq and op.extent.truncate_size allow
   * the OSD to determine whether the client submitted read needs to be
   * adjusted to compensate for a truncate the OSD hasn't seen yet.
   */
  uint64_t adjusted_size = os.oi.size;
  const uint64_t offset = op.extent.offset;
  uint64_t adjusted_length = op.extent.length;
  if ((os.oi.truncate_seq < op.extent.truncate_seq) &&
       (op.extent.offset + op.extent.length > op.extent.truncate_size) &&
       (adjusted_size > op.extent.truncate_size)) {
    adjusted_size = op.extent.truncate_size;
  }
  if (offset > adjusted_size) {
    adjusted_length = 0;
  } else if (offset + adjusted_length > adjusted_size) {
    adjusted_length = adjusted_size - offset;
  }
  logger().trace("sparse_read: {} {}~{}",
                 os.oi.soid, op.extent.offset, op.extent.length);
  return interruptor::make_interruptible(store->fiemap(coll, ghobject_t{os.oi.soid},
    offset, adjusted_length)).safe_then_interruptible(
    [&delta_stats, &os, &osd_op, this](auto&& m) {
    return seastar::do_with(interval_set<uint64_t>{std::move(m)},
			    [&delta_stats, &os, &osd_op, this](auto&& extents) {
      return interruptor::make_interruptible(store->readv(coll, ghobject_t{os.oi.soid},
                          extents, osd_op.op.flags)).safe_then_interruptible_tuple(
        [&delta_stats, &os, &osd_op, &extents](auto&& bl) -> read_errorator::future<> {
        if (_read_verify_data(os.oi, bl)) {
          osd_op.op.extent.length = bl.length();
          // re-encode since it might be modified
          ceph::encode(extents, osd_op.outdata);
          encode_destructively(bl, osd_op.outdata);
          logger().trace("sparse_read got {} bytes from object {}",
                         osd_op.op.extent.length, os.oi.soid);
         delta_stats.num_rd++;
         delta_stats.num_rd_kb += shift_round_up(osd_op.op.extent.length, 10);
          return read_errorator::make_ready_future<>();
        } else {
          // crc mismatches
          return crimson::ct_error::object_corrupted::make();
        }
      }, crimson::ct_error::input_output_error::handle([] {
        return read_errorator::future<>{crimson::ct_error::object_corrupted::make()};
      }),
      read_errorator::pass_further{});
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

PGBackend::checksum_ierrorator::future<>
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
  return _read(os.oi.soid, checksum.offset, checksum.length, osd_op.op.flags)
  .safe_then_interruptible(
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

PGBackend::cmp_ext_ierrorator::future<>
PGBackend::cmp_ext(const ObjectState& os, OSDOp& osd_op)
{
  const ceph_osd_op& op = osd_op.op;
  uint64_t obj_size = os.oi.size;
  if (os.oi.truncate_seq < op.extent.truncate_seq &&
      op.extent.offset + op.extent.length > op.extent.truncate_size) {
    obj_size = op.extent.truncate_size;
  }
  uint64_t ext_len;
  if (op.extent.offset >= obj_size) {
    ext_len = 0;
  } else if (op.extent.offset + op.extent.length > obj_size) {
    ext_len = obj_size - op.extent.offset;
  } else {
    ext_len = op.extent.length;
  }
  auto read_ext = ll_read_ierrorator::make_ready_future<ceph::bufferlist>();
  if (ext_len == 0) {
    logger().debug("{}: zero length extent", __func__);
  } else if (!os.exists || os.oi.is_whiteout()) {
    logger().debug("{}: {} DNE", __func__, os.oi.soid);
  } else {
    read_ext = _read(os.oi.soid, op.extent.offset, ext_len, 0);
  }
  return read_ext.safe_then_interruptible([&osd_op](auto&& read_bl)
    -> cmp_ext_errorator::future<> {
    for (unsigned index = 0; index < osd_op.indata.length(); index++) {
      char byte_in_op = osd_op.indata[index];
      char byte_from_disk = (index < read_bl.length() ? read_bl[index] : 0);
      if (byte_in_op != byte_from_disk) {
        logger().debug("cmp_ext: mismatch at {}", index);
        // Unlike other ops, we set osd_op.rval here and return a different
        // error code via ct_error::cmp_fail.
        osd_op.rval = -MAX_ERRNO - index;
        return crimson::ct_error::cmp_fail::make();
      }
    }
    osd_op.rval = 0;
    return cmp_ext_errorator::make_ready_future<>();
  });
}

PGBackend::stat_ierrorator::future<>
PGBackend::stat(
  const ObjectState& os,
  OSDOp& osd_op,
  object_stat_sum_t& delta_stats)
{
  if (os.exists/* TODO: && !os.is_whiteout() */) {
    logger().debug("stat os.oi.size={}, os.oi.mtime={}", os.oi.size, os.oi.mtime);
    encode(os.oi.size, osd_op.outdata);
    encode(os.oi.mtime, osd_op.outdata);
  } else {
    logger().debug("stat object does not exist");
    return crimson::ct_error::enoent::make();
  }
  delta_stats.num_rd++;
  return stat_errorator::now();
}

PGBackend::write_iertr::future<> PGBackend::_writefull(
  ObjectState& os,
  off_t truncate_size,
  const bufferlist& bl,
  ceph::os::Transaction& txn,
  osd_op_params_t& osd_op_params,
  object_stat_sum_t& delta_stats,
  unsigned flags)
{
  const bool existing = maybe_create_new_object(os, txn, delta_stats);
  if (existing && bl.length() < os.oi.size) {

    txn.truncate(coll->get_cid(), ghobject_t{os.oi.soid}, bl.length());
    truncate_update_size_and_usage(delta_stats, os.oi, truncate_size);

    osd_op_params.clean_regions.mark_data_region_dirty(
      bl.length(),
      os.oi.size - bl.length());
  }
  if (bl.length()) {
    txn.write(
      coll->get_cid(), ghobject_t{os.oi.soid}, 0, bl.length(),
      bl, flags);
    update_size_and_usage(
      delta_stats,
      osd_op_params.modified_ranges,
      os.oi, 0,
      bl.length(), true);
    osd_op_params.clean_regions.mark_data_region_dirty(
      0,
      std::max((uint64_t)bl.length(), os.oi.size));
  }
  return seastar::now();
}

PGBackend::write_iertr::future<> PGBackend::_truncate(
  ObjectState& os,
  ceph::os::Transaction& txn,
  osd_op_params_t& osd_op_params,
  object_stat_sum_t& delta_stats,
  size_t offset,
  size_t truncate_size,
  uint32_t truncate_seq)
{
  if (truncate_seq) {
    assert(offset == truncate_size);
    if (truncate_seq <= os.oi.truncate_seq) {
      logger().debug("{} truncate seq {} <= current {}, no-op",
                     __func__, truncate_seq, os.oi.truncate_seq);
      return write_ertr::make_ready_future<>();
    } else {
      logger().debug("{} truncate seq {} > current {}, truncating",
                     __func__, truncate_seq, os.oi.truncate_seq);
      os.oi.truncate_seq = truncate_seq;
      os.oi.truncate_size = truncate_size;
    }
  }
  maybe_create_new_object(os, txn, delta_stats);
  if (os.oi.size != offset) {
    txn.truncate(
      coll->get_cid(),
      ghobject_t{os.oi.soid}, offset);
    if (os.oi.size > offset) {
      interval_set<uint64_t> trim;
      trim.insert(offset, os.oi.size - offset);
      osd_op_params.modified_ranges.union_of(trim);
      osd_op_params.clean_regions.mark_data_region_dirty(
        offset,
	os.oi.size - offset);
    } else {
      // os.oi.size < offset
      osd_op_params.clean_regions.mark_data_region_dirty(
        os.oi.size,
        offset - os.oi.size);
    }
    truncate_update_size_and_usage(delta_stats, os.oi, offset);
    os.oi.clear_data_digest();
  }
  delta_stats.num_wr++;
  return write_ertr::now();
}

bool PGBackend::maybe_create_new_object(
  ObjectState& os,
  ceph::os::Transaction& txn,
  object_stat_sum_t& delta_stats)
{
  if (!os.exists) {
    ceph_assert(!os.oi.is_whiteout());
    os.exists = true;
    os.oi.new_object();

    txn.touch(coll->get_cid(), ghobject_t{os.oi.soid});
    delta_stats.num_objects++;
    return false;
  } else if (os.oi.is_whiteout()) {
    os.oi.clear_flag(object_info_t::FLAG_WHITEOUT);
    delta_stats.num_whiteouts--;
  }
  return true;
}

void PGBackend::update_size_and_usage(object_stat_sum_t& delta_stats,
  interval_set<uint64_t>& modified,
  object_info_t& oi, uint64_t offset,
  uint64_t length, bool write_full)
{
  interval_set<uint64_t> ch;
  if (write_full) {
    if (oi.size) {
      ch.insert(0, oi.size);
    } else if (length) {
      ch.insert(offset, length);
    }
    modified.union_of(ch);
  }
  if (write_full ||
      (offset + length > oi.size && length)) {
    uint64_t new_size = offset + length;
    delta_stats.num_bytes -= oi.size;
    delta_stats.num_bytes += new_size;
    oi.size = new_size;
  }
  delta_stats.num_wr++;
  delta_stats.num_wr_kb += shift_round_up(length, 10);
}

void PGBackend::truncate_update_size_and_usage(object_stat_sum_t& delta_stats,
  object_info_t& oi,
  uint64_t truncate_size)
{
  if (oi.size != truncate_size) {
    delta_stats.num_bytes -= oi.size;
    delta_stats.num_bytes += truncate_size;
    oi.size = truncate_size;
  }
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

PGBackend::interruptible_future<> PGBackend::set_allochint(
  ObjectState& os,
  const OSDOp& osd_op,
  ceph::os::Transaction& txn,
  object_stat_sum_t& delta_stats)
{
  maybe_create_new_object(os, txn, delta_stats);

  os.oi.expected_object_size = osd_op.op.alloc_hint.expected_object_size;
  os.oi.expected_write_size = osd_op.op.alloc_hint.expected_write_size;
  os.oi.alloc_hint_flags = osd_op.op.alloc_hint.flags;
  txn.set_alloc_hint(coll->get_cid(),
                     ghobject_t{os.oi.soid},
                     os.oi.expected_object_size,
                     os.oi.expected_write_size,
                     os.oi.alloc_hint_flags);
  return seastar::now();
}

PGBackend::write_iertr::future<> PGBackend::write(
    ObjectState& os,
    const OSDOp& osd_op,
    ceph::os::Transaction& txn,
    osd_op_params_t& osd_op_params,
    object_stat_sum_t& delta_stats)
{
  const ceph_osd_op& op = osd_op.op;
  uint64_t offset = op.extent.offset;
  uint64_t length = op.extent.length;
  bufferlist buf = osd_op.indata;
  if (op.extent.length != osd_op.indata.length()) {
    return crimson::ct_error::invarg::make();
  }

  if (!is_offset_and_length_valid(op.extent.offset, op.extent.length)) {
    return crimson::ct_error::file_too_large::make();
  }

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
        if (op.extent.truncate_size < os.oi.size) {
          interval_set<uint64_t> trim;
          trim.insert(op.extent.truncate_size,
            os.oi.size - op.extent.truncate_size);
          osd_op_params.modified_ranges.union_of(trim);
          osd_op_params.clean_regions.mark_data_region_dirty(
            op.extent.truncate_size, os.oi.size - op.extent.truncate_size);
          os.oi.clear_data_digest();
        }
      }
      truncate_update_size_and_usage(delta_stats, os.oi, op.extent.truncate_size);
    }
    os.oi.truncate_seq = op.extent.truncate_seq;
    os.oi.truncate_size = op.extent.truncate_size;
  }
  maybe_create_new_object(os, txn, delta_stats);
  if (length == 0) {
    if (offset > os.oi.size) {
      txn.truncate(coll->get_cid(), ghobject_t{os.oi.soid}, op.extent.offset);
      truncate_update_size_and_usage(delta_stats, os.oi, op.extent.offset);
    } else {
      txn.nop();
    }
  } else {
    txn.write(coll->get_cid(), ghobject_t{os.oi.soid},
	      offset, length, std::move(buf), op.flags);
    update_size_and_usage(delta_stats, osd_op_params.modified_ranges,
                          os.oi, offset, length);
  }
  osd_op_params.clean_regions.mark_data_region_dirty(op.extent.offset,
						     op.extent.length);
  logger().debug("{} clean_regions modified", __func__);

  return seastar::now();
}

PGBackend::interruptible_future<> PGBackend::write_same(
  ObjectState& os,
  const OSDOp& osd_op,
  ceph::os::Transaction& txn,
  osd_op_params_t& osd_op_params,
  object_stat_sum_t& delta_stats)
{
  const ceph_osd_op& op = osd_op.op;
  const uint64_t len = op.writesame.length;
  if (len == 0) {
    return seastar::now();
  }
  if (op.writesame.data_length == 0 ||
      len % op.writesame.data_length != 0 ||
      op.writesame.data_length != osd_op.indata.length()) {
    throw crimson::osd::invalid_argument();
  }
  ceph::bufferlist repeated_indata;
  for (uint64_t size = 0; size < len; size += op.writesame.data_length) {
    repeated_indata.append(osd_op.indata);
  }
  maybe_create_new_object(os, txn, delta_stats);
  txn.write(coll->get_cid(), ghobject_t{os.oi.soid},
            op.writesame.offset, len,
            std::move(repeated_indata), op.flags);
  update_size_and_usage(delta_stats, osd_op_params.modified_ranges,
                        os.oi, op.writesame.offset, len);
  osd_op_params.clean_regions.mark_data_region_dirty(op.writesame.offset, len);
  return seastar::now();
}

PGBackend::write_iertr::future<> PGBackend::writefull(
  ObjectState& os,
  const OSDOp& osd_op,
  ceph::os::Transaction& txn,
  osd_op_params_t& osd_op_params,
  object_stat_sum_t& delta_stats)
{
  const ceph_osd_op& op = osd_op.op;
  if (op.extent.length != osd_op.indata.length()) {
    return crimson::ct_error::invarg::make();
  }
  if (!is_offset_and_length_valid(op.extent.offset, op.extent.length)) {
    return crimson::ct_error::file_too_large::make();
  }

  return _writefull(
    os,
    op.extent.truncate_size,
    osd_op.indata,
    txn,
    osd_op_params,
    delta_stats,
    op.flags);
}

PGBackend::rollback_iertr::future<> PGBackend::rollback(
  ObjectState& os,
  const SnapSet &ss,
  const OSDOp& osd_op,
  ceph::os::Transaction& txn,
  osd_op_params_t& osd_op_params,
  object_stat_sum_t& delta_stats,
  crimson::osd::ObjectContextRef head,
  crimson::osd::ObjectContextLoader& obc_loader,
  const SnapContext &snapc)
{
  const ceph_osd_op& op = osd_op.op;
  snapid_t snapid = (uint64_t)op.snap.snapid;
  assert(os.oi.soid.is_head());
  logger().debug("{} deleting {} and rolling back to old snap {}",
                  __func__, os.oi.soid ,snapid);
  hobject_t target_coid = os.oi.soid;
  target_coid.snap = snapid;
  return obc_loader.with_clone_obc_only<RWState::RWWRITE>(
    head, target_coid,
    [this, &os, &txn, &delta_stats, &osd_op_params, snapid]
    (auto head_obc, auto resolved_obc) {
    if (resolved_obc->obs.oi.soid.is_head()) {
      // no-op: The resolved oid returned the head object
      logger().debug("PGBackend::rollback: loaded head_obc: {}"
                     " do nothing",
                     resolved_obc->obs.oi.soid);
      return rollback_iertr::now();
    }
    /* TODO: https://tracker.ceph.com/issues/59114 This implementation will not
     * behave correctly for a rados operation consisting of a mutation followed
     * by a rollback to a snapshot since the last mutation of the object.
     * The correct behavior would be for the rollback to undo the mutation
     * earlier in the operation by resolving to the clone created at the start
     * of the operation (see resolve_oid).
     * Instead, it will select HEAD leaving that mutation intact since the SnapSet won't
     * yet contain that clone. This behavior exists in classic as well.
     */
    logger().debug("PGBackend::rollback: loaded clone_obc: {}",
                   resolved_obc->obs.oi.soid);
    // 1) Delete current head
    if (os.exists) {
      txn.remove(coll->get_cid(), ghobject_t{os.oi.soid,
                                  ghobject_t::NO_GEN, shard});
    }
    // 2) Clone correct snapshot into head
    txn.clone(coll->get_cid(), ghobject_t{resolved_obc->obs.oi.soid},
                               ghobject_t{os.oi.soid});
    //    Copy clone obc.os.oi to os.oi
    os.oi.clear_flag(object_info_t::FLAG_WHITEOUT);
    os.oi.copy_user_bits(resolved_obc->obs.oi);
    delta_stats.num_bytes -= os.oi.size;
    delta_stats.num_bytes += resolved_obc->obs.oi.size;
    osd_op_params.clean_regions.mark_data_region_dirty(0,
      std::max(os.oi.size, resolved_obc->obs.oi.size));
    osd_op_params.clean_regions.mark_omap_dirty();

    // 3) Calculate clone_overlaps by following overlaps
    const auto& clone_overlap =
      head_obc->ssc->snapset.clone_overlap;
    auto iter = clone_overlap.lower_bound(snapid);
    ceph_assert(iter != clone_overlap.end());
    interval_set<uint64_t> overlaps = iter->second;
    for (const auto&i: clone_overlap) {
      overlaps.intersection_of(i.second);
    }

    if (os.oi.size > 0) {
      interval_set<uint64_t> modified;
      modified.insert(0, os.oi.size);
      overlaps.intersection_of(modified);
      modified.subtract(overlaps);
      osd_op_params.modified_ranges.union_of(modified);
    }
    return rollback_iertr::now();
  }).safe_then_interruptible([] {
    logger().debug("PGBackend::rollback succefully");
    return rollback_iertr::now();
  },// there's no snapshot here, or there's no object.
    // if there's no snapshot, we delete the object;
    // otherwise, do nothing.
    crimson::ct_error::enoent::handle(
    [this, &os, snapid, &txn, &delta_stats, &snapc, &ss, &osd_op_params] {
      logger().debug("PGBackend::rollback: deleting head on {}"
                     " with snap_id of {}"
                     " because got ENOENT|whiteout on obc lookup",
                     os.oi.soid, snapid);
      return remove(os, txn, osd_op_params, delta_stats,
                    should_whiteout(ss, snapc), os.oi.size);
    }),
    rollback_ertr::pass_further{},
    crimson::ct_error::assert_all{"unexpected error in rollback"}
  );
}

PGBackend::append_ierrorator::future<> PGBackend::append(
  ObjectState& os,
  OSDOp& osd_op,
  ceph::os::Transaction& txn,
  osd_op_params_t& osd_op_params,
  object_stat_sum_t& delta_stats)
{
  const ceph_osd_op& op = osd_op.op;
  if (op.extent.length != osd_op.indata.length()) {
    return crimson::ct_error::invarg::make();
  }
  maybe_create_new_object(os, txn, delta_stats);
  if (op.extent.length) {
    txn.write(coll->get_cid(), ghobject_t{os.oi.soid},
              os.oi.size /* offset */, op.extent.length,
              std::move(osd_op.indata), op.flags);
    update_size_and_usage(delta_stats,
                          osd_op_params.modified_ranges,
                          os.oi, os.oi.size, op.extent.length);
    osd_op_params.clean_regions.mark_data_region_dirty(os.oi.size,
                                                       op.extent.length);
  }
  return seastar::now();
}

PGBackend::write_iertr::future<> PGBackend::truncate(
  ObjectState& os,
  const OSDOp& osd_op,
  ceph::os::Transaction& txn,
  osd_op_params_t& osd_op_params,
  object_stat_sum_t& delta_stats)
{
  if (!os.exists || os.oi.is_whiteout()) {
    logger().debug("{} object dne, truncate is a no-op", __func__);
    return write_ertr::now();
  }
  const ceph_osd_op& op = osd_op.op;
  if (!is_offset_and_length_valid(op.extent.offset, op.extent.length)) {
    return crimson::ct_error::file_too_large::make();
  }
  return _truncate(
    os, txn, osd_op_params, delta_stats,
    op.extent.offset, op.extent.truncate_size, op.extent.truncate_seq);
}

PGBackend::write_iertr::future<> PGBackend::zero(
  ObjectState& os,
  const OSDOp& osd_op,
  ceph::os::Transaction& txn,
  osd_op_params_t& osd_op_params,
  object_stat_sum_t& delta_stats)
{
  if (!os.exists || os.oi.is_whiteout()) {
    logger().debug("{} object dne, zero is a no-op", __func__);
    return write_ertr::now();
  }
  const ceph_osd_op& op = osd_op.op;
  if (!is_offset_and_length_valid(op.extent.offset, op.extent.length)) {
    return crimson::ct_error::file_too_large::make();
  }

  if (op.extent.offset >= os.oi.size || op.extent.length == 0) {
    return write_iertr::now(); // noop
  }

  if (op.extent.offset + op.extent.length >= os.oi.size) {
    return _truncate(
      os, txn, osd_op_params, delta_stats,
      op.extent.offset, op.extent.truncate_size, op.extent.truncate_seq);
  }

  txn.zero(coll->get_cid(),
           ghobject_t{os.oi.soid},
           op.extent.offset,
           op.extent.length);
  interval_set<uint64_t> ch;
  ch.insert(op.extent.offset, op.extent.length);
  osd_op_params.modified_ranges.union_of(ch);
  osd_op_params.clean_regions.mark_data_region_dirty(op.extent.offset,
						     op.extent.length);
  delta_stats.num_wr++;
  os.oi.clear_data_digest();
  return write_ertr::now();
}

PGBackend::create_iertr::future<> PGBackend::create(
  ObjectState& os,
  const OSDOp& osd_op,
  ceph::os::Transaction& txn,
  object_stat_sum_t& delta_stats)
{
  if (os.exists && !os.oi.is_whiteout() &&
      (osd_op.op.flags & CEPH_OSD_OP_FLAG_EXCL)) {
    // this is an exclusive create
    return crimson::ct_error::eexist::make();
  }

  if (osd_op.indata.length()) {
    // handle the legacy. `category` is no longer implemented.
    try {
      auto p = osd_op.indata.cbegin();
      std::string category;
      decode(category, p);
    } catch (buffer::error&) {
      return crimson::ct_error::invarg::make();
    }
  }
  maybe_create_new_object(os, txn, delta_stats);
  txn.create(coll->get_cid(),
             ghobject_t{os.oi.soid, ghobject_t::NO_GEN, shard});
  return seastar::now();
}

PGBackend::interruptible_future<>
PGBackend::remove(ObjectState& os, ceph::os::Transaction& txn)
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

PGBackend::remove_iertr::future<>
PGBackend::remove(ObjectState& os, ceph::os::Transaction& txn,
  osd_op_params_t& osd_op_params,
  object_stat_sum_t& delta_stats,
  bool whiteout,
  int num_bytes)
{
  if (!os.exists) {
    return crimson::ct_error::enoent::make();
  }

  if (!os.exists) {
    logger().debug("{} {} does not exist",__func__, os.oi.soid);
    return seastar::now();
  }
  if (whiteout && os.oi.is_whiteout()) {
    logger().debug("{} whiteout set on {} ",__func__, os.oi.soid);
    return seastar::now();
  }
  txn.remove(coll->get_cid(),
	     ghobject_t{os.oi.soid, ghobject_t::NO_GEN, shard});

  if (os.oi.is_omap()) {
    os.oi.clear_flag(object_info_t::FLAG_OMAP);
    delta_stats.num_objects_omap--;
  }

  if (os.oi.size > 0) {
    interval_set<uint64_t> ch;
    ch.insert(0, os.oi.size);
    osd_op_params.modified_ranges.union_of(ch);
    osd_op_params.clean_regions.mark_data_region_dirty(0, os.oi.size);
  }

  osd_op_params.clean_regions.mark_omap_dirty();
  delta_stats.num_wr++;
  // num_bytes of the removed clone or head object
  delta_stats.num_bytes -= num_bytes;
  os.oi.size = 0;
  os.oi.new_object();

  // todo: update watchers

  if (whiteout) {
    logger().debug("{} setting whiteout on {} ",__func__, os.oi.soid);
    os.oi.set_flag(object_info_t::FLAG_WHITEOUT);
    delta_stats.num_whiteouts++;
    txn.create(coll->get_cid(),
               ghobject_t{os.oi.soid, ghobject_t::NO_GEN, shard});
    return seastar::now();
  }

  // delete the head
  delta_stats.num_objects--;
  if (os.oi.soid.is_snap()) {
    delta_stats.num_object_clones--;
  }
  if (os.oi.is_whiteout()) {
    logger().debug("{} deleting whiteout on {}", __func__, os.oi.soid);
    os.oi.clear_flag(object_info_t::FLAG_WHITEOUT);
    delta_stats.num_whiteouts--;
  }
  os.exists = false;
  return seastar::now();
}

PGBackend::interruptible_future<std::tuple<std::vector<hobject_t>, hobject_t>>
PGBackend::list_objects(const hobject_t& start, uint64_t limit) const
{
  auto gstart = start.is_min() ? ghobject_t{} : ghobject_t{start, 0, shard};
  return interruptor::make_interruptible(store->list_objects(coll,
					 gstart,
					 ghobject_t::get_max(),
					 limit))
    .then_interruptible([](auto ret) {
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

PGBackend::setxattr_ierrorator::future<> PGBackend::setxattr(
  ObjectState& os,
  const OSDOp& osd_op,
  ceph::os::Transaction& txn,
  object_stat_sum_t& delta_stats)
{
  if (local_conf()->osd_max_attr_size > 0 &&
      osd_op.op.xattr.value_len > local_conf()->osd_max_attr_size) {
    return crimson::ct_error::file_too_large::make();
  }

  const auto max_name_len = std::min<uint64_t>(
    store->get_max_attr_name_length(), local_conf()->osd_max_attr_name_len);
  if (osd_op.op.xattr.name_len > max_name_len) {
    return crimson::ct_error::enametoolong::make();
  }

  maybe_create_new_object(os, txn, delta_stats);

  std::string name{"_"};
  ceph::bufferlist val;
  {
    auto bp = osd_op.indata.cbegin();
    bp.copy(osd_op.op.xattr.name_len, name);
    bp.copy(osd_op.op.xattr.value_len, val);
  }
  logger().debug("setxattr on obj={} for attr={}", os.oi.soid, name);
  txn.setattr(coll->get_cid(), ghobject_t{os.oi.soid}, name, val);
  delta_stats.num_wr++;
  return seastar::now();
}

PGBackend::get_attr_ierrorator::future<> PGBackend::getxattr(
  const ObjectState& os,
  OSDOp& osd_op,
  object_stat_sum_t& delta_stats) const
{
  std::string name;
  ceph::bufferlist val;
  {
    auto bp = osd_op.indata.cbegin();
    std::string aname;
    bp.copy(osd_op.op.xattr.name_len, aname);
    name = "_" + aname;
  }
  logger().debug("getxattr on obj={} for attr={}", os.oi.soid, name);
  return getxattr(os.oi.soid, std::move(name)).safe_then_interruptible(
    [&delta_stats, &osd_op] (ceph::bufferlist&& val) {
    osd_op.outdata = std::move(val);
    osd_op.op.xattr.value_len = osd_op.outdata.length();
    delta_stats.num_rd++;
    delta_stats.num_rd_kb += shift_round_up(osd_op.outdata.length(), 10);
    return get_attr_errorator::now();
  });
}

PGBackend::get_attr_ierrorator::future<ceph::bufferlist>
PGBackend::getxattr(
  const hobject_t& soid,
  std::string_view key) const
{
  return store->get_attr(coll, ghobject_t{soid}, key);
}

PGBackend::get_attr_ierrorator::future<ceph::bufferlist>
PGBackend::getxattr(
  const hobject_t& soid,
  std::string&& key) const
{
  return seastar::do_with(key, [this, &soid](auto &key) {
    return store->get_attr(coll, ghobject_t{soid}, key);
  });
}

PGBackend::get_attr_ierrorator::future<> PGBackend::get_xattrs(
  const ObjectState& os,
  OSDOp& osd_op,
  object_stat_sum_t& delta_stats) const
{
  return store->get_attrs(coll, ghobject_t{os.oi.soid}).safe_then(
    [&delta_stats, &osd_op](auto&& attrs) {
    std::vector<std::pair<std::string, bufferlist>> user_xattrs;
    ceph::bufferlist bl;
    for (auto& [key, val] : attrs) {
      if (key.size() > 1 && key[0] == '_') {
	bl.append(std::move(val));
	user_xattrs.emplace_back(key.substr(1), std::move(bl));
      }
    }
    ceph::encode(user_xattrs, osd_op.outdata);
    delta_stats.num_rd++;
    delta_stats.num_rd_kb += shift_round_up(bl.length(), 10);
    return get_attr_errorator::now();
  });
}

namespace {

template<typename U, typename V>
int do_cmp_xattr(int op, const U& lhs, const V& rhs)
{
  switch (op) {
  case CEPH_OSD_CMPXATTR_OP_EQ:
    return lhs == rhs;
  case CEPH_OSD_CMPXATTR_OP_NE:
    return lhs != rhs;
  case CEPH_OSD_CMPXATTR_OP_GT:
    return lhs > rhs;
  case CEPH_OSD_CMPXATTR_OP_GTE:
    return lhs >= rhs;
  case CEPH_OSD_CMPXATTR_OP_LT:
    return lhs < rhs;
  case CEPH_OSD_CMPXATTR_OP_LTE:
    return lhs <= rhs;
  default:
    return -EINVAL;
  }
}

} // anonymous namespace

static int do_xattr_cmp_u64(int op, uint64_t lhs, bufferlist& rhs_xattr)
{
  uint64_t rhs;

  if (rhs_xattr.length() > 0) {
    const char* first = rhs_xattr.c_str();
    if (auto [p, ec] = std::from_chars(first, first + rhs_xattr.length(), rhs);
	ec != std::errc()) {
      return -EINVAL;
    }
  } else {
    rhs = 0;
  }
  logger().debug("do_xattr_cmp_u64 '{}' vs '{}' op {}", lhs, rhs, op);
  return do_cmp_xattr(op, lhs, rhs);
}

PGBackend::cmp_xattr_ierrorator::future<> PGBackend::cmp_xattr(
  const ObjectState& os,
  OSDOp& osd_op,
  object_stat_sum_t& delta_stats) const
{
  std::string name{"_"};
  auto bp = osd_op.indata.cbegin();
  bp.copy(osd_op.op.xattr.name_len, name);
 
  logger().debug("cmpxattr on obj={} for attr={}", os.oi.soid, name);
  return getxattr(os.oi.soid, std::move(name)).safe_then_interruptible(
    [&delta_stats, &osd_op] (auto &&xattr) -> cmp_xattr_ierrorator::future<> {
    delta_stats.num_rd++;
    delta_stats.num_rd_kb += shift_round_up(osd_op.op.xattr.value_len, 10);

    int result = 0;
    auto bp = osd_op.indata.cbegin();
    bp += osd_op.op.xattr.name_len;

    switch (osd_op.op.xattr.cmp_mode) {
    case CEPH_OSD_CMPXATTR_MODE_STRING:
      {
        string lhs;
        bp.copy(osd_op.op.xattr.value_len, lhs);
        string_view rhs(xattr.c_str(), xattr.length());
        result = do_cmp_xattr(osd_op.op.xattr.cmp_op, lhs, rhs);
        logger().debug("cmpxattr lhs={}, rhs={}", lhs, rhs);
      }
    break;
    case CEPH_OSD_CMPXATTR_MODE_U64:
      {
        uint64_t lhs;
        try {
          decode(lhs, bp);
	} catch (ceph::buffer::error& e) {
          logger().info("cmp_xattr: buffer error expection");
          result = -EINVAL;
          break;
	}
        result = do_xattr_cmp_u64(osd_op.op.xattr.cmp_op, lhs, xattr);
      }
    break;
    default:
      logger().info("bad cmp mode {}", osd_op.op.xattr.cmp_mode);
      result = -EINVAL;
    }
    if (result == 0) {
      logger().info("cmp_xattr: comparison returned false");
      return crimson::ct_error::ecanceled::make();
    } else if (result == -EINVAL) {
      return crimson::ct_error::invarg::make();
    } else {
      osd_op.rval = 1;
      return cmp_xattr_ierrorator::now();
    }
  }).handle_error_interruptible(
    crimson::ct_error::enodata::handle([&delta_stats, &osd_op] ()
      ->cmp_xattr_errorator::future<> {
      delta_stats.num_rd++;
      delta_stats.num_rd_kb += shift_round_up(osd_op.op.xattr.value_len, 10);
      return crimson::ct_error::ecanceled::make();
    }),
    cmp_xattr_errorator::pass_further{}
  );
}

PGBackend::rm_xattr_iertr::future<>
PGBackend::rm_xattr(
  ObjectState& os,
  const OSDOp& osd_op,
  ceph::os::Transaction& txn)
{
  if (!os.exists || os.oi.is_whiteout()) {
    logger().debug("{}: {} DNE", __func__, os.oi.soid);
    return crimson::ct_error::enoent::make();
  }
  auto bp = osd_op.indata.cbegin();
  string attr_name{"_"};
  bp.copy(osd_op.op.xattr.name_len, attr_name);
  txn.rmattr(coll->get_cid(), ghobject_t{os.oi.soid}, attr_name);
  return rm_xattr_iertr::now();
}

void PGBackend::clone(
  /* const */object_info_t& snap_oi,
  const ObjectState& os,
  const ObjectState& d_os,
  ceph::os::Transaction& txn)
{
  // See OpsExecutor::execute_clone documentation
  txn.clone(coll->get_cid(), ghobject_t{os.oi.soid}, ghobject_t{d_os.oi.soid});
  {
    ceph::bufferlist bv;
    snap_oi.encode(bv, CEPH_FEATURES_ALL);
    txn.setattr(coll->get_cid(), ghobject_t{d_os.oi.soid}, OI_ATTR, bv);
  }
  txn.rmattr(coll->get_cid(), ghobject_t{d_os.oi.soid}, SS_ATTR);
}

using get_omap_ertr =
  crimson::os::FuturizedStore::Shard::read_errorator::extend<
    crimson::ct_error::enodata>;
using get_omap_iertr =
  ::crimson::interruptible::interruptible_errorator<
    ::crimson::osd::IOInterruptCondition,
    get_omap_ertr>;
static
get_omap_iertr::future<
  crimson::os::FuturizedStore::Shard::omap_values_t>
maybe_get_omap_vals_by_keys(
  crimson::os::FuturizedStore::Shard* store,
  const crimson::os::CollectionRef& coll,
  const object_info_t& oi,
  const std::set<std::string>& keys_to_get)
{
  if (oi.is_omap()) {
    return store->omap_get_values(coll, ghobject_t{oi.soid}, keys_to_get);
  } else {
    return crimson::ct_error::enodata::make();
  }
}

static
get_omap_iertr::future<
  std::tuple<bool, crimson::os::FuturizedStore::Shard::omap_values_t>>
maybe_get_omap_vals(
  crimson::os::FuturizedStore::Shard* store,
  const crimson::os::CollectionRef& coll,
  const object_info_t& oi,
  const std::string& start_after)
{
  if (oi.is_omap()) {
    return store->omap_get_values(coll, ghobject_t{oi.soid}, start_after);
  } else {
    return crimson::ct_error::enodata::make();
  }
}

PGBackend::ll_read_ierrorator::future<ceph::bufferlist>
PGBackend::omap_get_header(
  const crimson::os::CollectionRef& c,
  const ghobject_t& oid) const
{
  return store->omap_get_header(c, oid)
    .handle_error(
      crimson::ct_error::enodata::handle([] {
	return seastar::make_ready_future<bufferlist>();
      }),
      ll_read_errorator::pass_further{}
    );
}

PGBackend::ll_read_ierrorator::future<>
PGBackend::omap_get_header(
  const ObjectState& os,
  OSDOp& osd_op,
  object_stat_sum_t& delta_stats) const
{
  if (os.oi.is_omap()) {
    return omap_get_header(coll, ghobject_t{os.oi.soid}).safe_then_interruptible(
      [&delta_stats, &osd_op] (ceph::bufferlist&& header) {
        osd_op.outdata = std::move(header);
        delta_stats.num_rd_kb += shift_round_up(osd_op.outdata.length(), 10);
        delta_stats.num_rd++;
        return seastar::now();
      });
  } else {
    // no omap? return empty data but not ENOENT. This is imporant for
    // the case when the object is being creating due to to may_write().
    return seastar::now();
  }
}

PGBackend::ll_read_ierrorator::future<>
PGBackend::omap_get_keys(
  const ObjectState& os,
  OSDOp& osd_op,
  object_stat_sum_t& delta_stats) const
{
  if (!os.exists || os.oi.is_whiteout()) {
    logger().debug("{}: object does not exist: {}", os.oi.soid);
    return crimson::ct_error::enoent::make();
  }
  std::string start_after;
  uint64_t max_return;
  try {
    auto p = osd_op.indata.cbegin();
    decode(start_after, p);
    decode(max_return, p);
  } catch (buffer::error&) {
    throw crimson::osd::invalid_argument{};
  }
  max_return =
    std::min(max_return, local_conf()->osd_max_omap_entries_per_request);


  // TODO: truly chunk the reading
  return maybe_get_omap_vals(store, coll, os.oi, start_after).safe_then_interruptible(
    [=,&delta_stats, &osd_op](auto ret) {
      ceph::bufferlist result;
      bool truncated = false;
      uint32_t num = 0;
      for (auto &[key, val] : std::get<1>(ret)) {
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
      delta_stats.num_rd_kb += shift_round_up(osd_op.outdata.length(), 10);
      delta_stats.num_rd++;
      return seastar::now();
    }).handle_error_interruptible(
      crimson::ct_error::enodata::handle([&osd_op] {
        uint32_t num = 0;
	bool truncated = false;
	encode(num, osd_op.outdata);
	encode(truncated, osd_op.outdata);
	osd_op.rval = 0;
	return seastar::now();
      }),
      ll_read_errorator::pass_further{}
    );
}
static
PGBackend::omap_cmp_ertr::future<> do_omap_val_cmp(
  std::map<std::string, bufferlist, std::less<>> out,
  std::map<std::string, std::pair<bufferlist, int>> assertions)
{
  bufferlist empty;
  for (const auto &[akey, avalue] : assertions) {
    const auto [abl, aflag] = avalue;
    auto out_entry = out.find(akey);
    bufferlist &bl = (out_entry != out.end()) ? out_entry->second : empty;
    switch (aflag) {
      case CEPH_OSD_CMPXATTR_OP_EQ:
        if (!(bl == abl)) {
          return crimson::ct_error::ecanceled::make();
        }
      break;
      case CEPH_OSD_CMPXATTR_OP_LT:
        if (!(bl < abl)) {
          return crimson::ct_error::ecanceled::make();
        }
      break;
      case CEPH_OSD_CMPXATTR_OP_GT:
        if (!(bl > abl)) {
          return crimson::ct_error::ecanceled::make();
        }
      break;
      default:
        return crimson::ct_error::invarg::make();
    }
  }
  return PGBackend::omap_cmp_ertr::now();
}
PGBackend::omap_cmp_iertr::future<>
PGBackend::omap_cmp(
  const ObjectState& os,
  OSDOp& osd_op,
  object_stat_sum_t& delta_stats) const
{
  if (!os.exists || os.oi.is_whiteout()) {
    logger().debug("{}: object does not exist: {}", os.oi.soid);
    return crimson::ct_error::enoent::make();
  }

  auto bp = osd_op.indata.cbegin();
  std::map<std::string, std::pair<bufferlist, int> > assertions;
  try {
    decode(assertions, bp);
  } catch (buffer::error&) {
    return crimson::ct_error::invarg::make();
  }

  delta_stats.num_rd++;
  if (os.oi.is_omap()) {
    std::set<std::string> to_get;
    for (auto &i: assertions) {
      to_get.insert(i.first);
    }
    return store->omap_get_values(coll, ghobject_t{os.oi.soid}, to_get)
      .safe_then([=, &osd_op] (auto&& out) -> omap_cmp_iertr::future<> {
      osd_op.rval = 0;
      return  do_omap_val_cmp(out, assertions);
    });
  } else {
    return crimson::ct_error::ecanceled::make();
  }
}
PGBackend::ll_read_ierrorator::future<>
PGBackend::omap_get_vals(
  const ObjectState& os,
  OSDOp& osd_op,
  object_stat_sum_t& delta_stats) const
{
  if (!os.exists || os.oi.is_whiteout()) {
    logger().debug("{}: object does not exist: {}", os.oi.soid);
    return crimson::ct_error::enoent::make();
  }
  std::string start_after;
  uint64_t max_return;
  std::string filter_prefix;
  try {
    auto p = osd_op.indata.cbegin();
    decode(start_after, p);
    decode(max_return, p);
    decode(filter_prefix, p);
  } catch (buffer::error&) {
    throw crimson::osd::invalid_argument{};
  }

  max_return = \
    std::min(max_return, local_conf()->osd_max_omap_entries_per_request);
  delta_stats.num_rd_kb += shift_round_up(osd_op.outdata.length(), 10);
  delta_stats.num_rd++;

  // TODO: truly chunk the reading
  return maybe_get_omap_vals(store, coll, os.oi, start_after)
  .safe_then_interruptible(
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
      return ll_read_errorator::now();
    }).handle_error_interruptible(
      crimson::ct_error::enodata::handle([&osd_op] {
        encode(uint32_t{0} /* num */, osd_op.outdata);
        encode(bool{false} /* truncated */, osd_op.outdata);
        osd_op.rval = 0;
        return ll_read_errorator::now();
      }),
      ll_read_errorator::pass_further{}
    );
}

PGBackend::ll_read_ierrorator::future<>
PGBackend::omap_get_vals_by_keys(
  const ObjectState& os,
  OSDOp& osd_op,
  object_stat_sum_t& delta_stats) const
{
  if (!os.exists || os.oi.is_whiteout()) {
    logger().debug("{}: object does not exist: {}", __func__, os.oi.soid);
    return crimson::ct_error::enoent::make();
  }

  std::set<std::string> keys_to_get;
  try {
    auto p = osd_op.indata.cbegin();
    decode(keys_to_get, p);
  } catch (buffer::error&) {
    throw crimson::osd::invalid_argument();
  }
  delta_stats.num_rd_kb += shift_round_up(osd_op.outdata.length(), 10);
  delta_stats.num_rd++;
  return maybe_get_omap_vals_by_keys(store, coll, os.oi, keys_to_get)
  .safe_then_interruptible(
    [&osd_op] (crimson::os::FuturizedStore::Shard::omap_values_t&& vals) {
      encode(vals, osd_op.outdata);
      return ll_read_errorator::now();
    }).handle_error_interruptible(
      crimson::ct_error::enodata::handle([&osd_op] {
        uint32_t num = 0;
        encode(num, osd_op.outdata);
        osd_op.rval = 0;
        return ll_read_errorator::now();
      }),
      ll_read_errorator::pass_further{}
    );
}

PGBackend::interruptible_future<>
PGBackend::omap_set_vals(
  ObjectState& os,
  const OSDOp& osd_op,
  ceph::os::Transaction& txn,
  osd_op_params_t& osd_op_params,
  object_stat_sum_t& delta_stats)
{
  maybe_create_new_object(os, txn, delta_stats);

  ceph::bufferlist to_set_bl;
  try {
    auto p = osd_op.indata.cbegin();
    decode_str_str_map_to_bl(p, &to_set_bl);
  } catch (buffer::error&) {
    throw crimson::osd::invalid_argument{};
  }

  txn.omap_setkeys(coll->get_cid(), ghobject_t{os.oi.soid}, to_set_bl);
  osd_op_params.clean_regions.mark_omap_dirty();
  delta_stats.num_wr++;
  delta_stats.num_wr_kb += shift_round_up(to_set_bl.length(), 10);
  if (!os.oi.is_omap()) {
    os.oi.set_flag(object_info_t::FLAG_OMAP);
    delta_stats.num_objects_omap++;
  }
  os.oi.clear_omap_digest();
  return seastar::now();
}

PGBackend::interruptible_future<>
PGBackend::omap_set_header(
  ObjectState& os,
  const OSDOp& osd_op,
  ceph::os::Transaction& txn,
  osd_op_params_t& osd_op_params,
  object_stat_sum_t& delta_stats)
{
  maybe_create_new_object(os, txn, delta_stats);
  txn.omap_setheader(coll->get_cid(), ghobject_t{os.oi.soid}, osd_op.indata);
  osd_op_params.clean_regions.mark_omap_dirty();
  delta_stats.num_wr++;
  if (!os.oi.is_omap()) {
    os.oi.set_flag(object_info_t::FLAG_OMAP);
    delta_stats.num_objects_omap++;
  }
  os.oi.clear_omap_digest();
  return seastar::now();
}

PGBackend::interruptible_future<> PGBackend::omap_remove_range(
  ObjectState& os,
  const OSDOp& osd_op,
  ceph::os::Transaction& txn,
  object_stat_sum_t& delta_stats)
{
  std::string key_begin, key_end;
  try {
    auto p = osd_op.indata.cbegin();
    decode(key_begin, p);
    decode(key_end, p);
  } catch (buffer::error& e) {
    throw crimson::osd::invalid_argument{};
  }
  txn.omap_rmkeyrange(coll->get_cid(), ghobject_t{os.oi.soid}, key_begin, key_end);
  delta_stats.num_wr++;
  os.oi.clear_omap_digest();
  return seastar::now();
}

PGBackend::interruptible_future<> PGBackend::omap_remove_key(
  ObjectState& os,
  const OSDOp& osd_op,
  ceph::os::Transaction& txn)
{
  ceph::bufferlist to_rm_bl;
  try {
    auto p = osd_op.indata.cbegin();
    decode_str_set_to_bl(p, &to_rm_bl);
  } catch (buffer::error& e) {
    throw crimson::osd::invalid_argument{};
  }
  txn.omap_rmkeys(coll->get_cid(), ghobject_t{os.oi.soid}, to_rm_bl);
  // TODO:
  // ctx->clean_regions.mark_omap_dirty();
  // ctx->delta_stats.num_wr++;
  os.oi.clear_omap_digest();
  return seastar::now();
}

PGBackend::omap_clear_iertr::future<>
PGBackend::omap_clear(
  ObjectState& os,
  OSDOp& osd_op,
  ceph::os::Transaction& txn,
  osd_op_params_t& osd_op_params,
  object_stat_sum_t& delta_stats)
{
  if (!os.exists || os.oi.is_whiteout()) {
    logger().debug("{}: object does not exist: {}", os.oi.soid);
    return crimson::ct_error::enoent::make();
  }
  if (!os.oi.is_omap()) {
    return omap_clear_ertr::now();
  }
  txn.omap_clear(coll->get_cid(), ghobject_t{os.oi.soid});
  osd_op_params.clean_regions.mark_omap_dirty();
  delta_stats.num_wr++;
  os.oi.clear_omap_digest();
  os.oi.clear_flag(object_info_t::FLAG_OMAP);
  return omap_clear_ertr::now();
}

PGBackend::interruptible_future<struct stat>
PGBackend::stat(
  CollectionRef c,
  const ghobject_t& oid) const
{
  return store->stat(c, oid);
}

PGBackend::read_errorator::future<std::map<uint64_t, uint64_t>>
PGBackend::fiemap(
  CollectionRef c,
  const ghobject_t& oid,
  uint64_t off,
  uint64_t len)
{
  return store->fiemap(c, oid, off, len);
}

PGBackend::write_iertr::future<> PGBackend::tmapput(
  ObjectState& os,
  const OSDOp& osd_op,
  ceph::os::Transaction& txn,
  object_stat_sum_t& delta_stats,
  osd_op_params_t& osd_op_params)
{
  logger().debug("PGBackend::tmapput: {}", os.oi.soid);
  auto ret = crimson::common::do_tmap_put(osd_op.indata.cbegin());
  if (!ret.has_value()) {
    logger().debug("PGBackend::tmapup: {}, ret={}", os.oi.soid, ret.error());
    ceph_assert(ret.error() == -EINVAL);
    return crimson::ct_error::invarg::make();
  } else {
    auto bl = std::move(ret.value());
    return _writefull(
      os,
      bl.length(),
      std::move(bl),
      txn,
      osd_op_params,
      delta_stats,
      0);
  }
}

PGBackend::tmapup_iertr::future<> PGBackend::tmapup(
  ObjectState& os,
  const OSDOp& osd_op,
  ceph::os::Transaction& txn,
  object_stat_sum_t& delta_stats,
  osd_op_params_t& osd_op_params)
{
  logger().debug("PGBackend::tmapup: {}", os.oi.soid);
  return PGBackend::write_iertr::now(
  ).si_then([this, &os] {
    return _read(os.oi.soid, 0, os.oi.size, 0);
  }).handle_error_interruptible(
    crimson::ct_error::enoent::handle([](auto &) {
      return seastar::make_ready_future<bufferlist>();
    }),
    PGBackend::write_iertr::pass_further{},
    crimson::ct_error::assert_all{"read error in mutate_object_contents"}
  ).si_then([this, &os, &osd_op, &txn,
	     &delta_stats, &osd_op_params]
	    (auto &&bl) mutable -> PGBackend::tmapup_iertr::future<> {
    auto result = crimson::common::do_tmap_up(
      osd_op.indata.cbegin(),
      std::move(bl));
    if (!result.has_value()) {
      int ret = result.error();
      logger().debug("PGBackend::tmapup: {}, ret={}", os.oi.soid, ret);
      switch (ret) {
      case -EEXIST:
	return crimson::ct_error::eexist::make();
      case -ENOENT:
	return crimson::ct_error::enoent::make();
      case -EINVAL:
	return crimson::ct_error::invarg::make();
      default:
	ceph_assert(0 == "impossible error");
	return crimson::ct_error::invarg::make();
      }
    }

    logger().debug(
      "PGBackend::tmapup: {}, result.value.length()={}, ret=0",
      os.oi.soid, result.value().length());
    return _writefull(
      os,
      result.value().length(),
      result.value(),
      txn,
      osd_op_params,
      delta_stats,
      0);
  });
}

PGBackend::read_ierrorator::future<> PGBackend::tmapget(
  const ObjectState& os,
  OSDOp& osd_op,
  object_stat_sum_t& delta_stats)
{
  logger().debug("PGBackend::tmapget: {}", os.oi.soid);
  const auto& oi = os.oi;
  logger().debug("PGBackend::tmapget: read {} 0~{}", oi.soid, oi.size);
  if (!os.exists || os.oi.is_whiteout()) {
    logger().debug("PGBackend::tmapget: {} DNE", os.oi.soid);
    return crimson::ct_error::enoent::make();
  }

  return _read(oi.soid, 0, oi.size, 0).safe_then_interruptible_tuple(
    [&delta_stats, &osd_op](auto&& bl) -> read_errorator::future<> {
      logger().debug("PGBackend::tmapget: data length: {}", bl.length());
      osd_op.op.extent.length = bl.length();
      osd_op.rval = 0;
      delta_stats.num_rd++;
      delta_stats.num_rd_kb += shift_round_up(bl.length(), 10);
      osd_op.outdata = std::move(bl);
      return read_errorator::now();
    }, crimson::ct_error::input_output_error::handle([] {
      return read_errorator::future<>{crimson::ct_error::object_corrupted::make()};
    }),
    read_errorator::pass_further{});
}

