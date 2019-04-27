#include "pg_backend.h"

#include <optional>
#include <fmt/ostream.h>
#include <seastar/core/print.hh>

#include "crimson/os/cyan_collection.h"
#include "crimson/os/cyan_object.h"
#include "crimson/os/cyan_store.h"
#include "replicated_backend.h"
#include "ec_backend.h"
#include "exceptions.h"

namespace {
  seastar::logger& logger() {
    return ceph::get_logger(ceph_subsys_osd);
  }
}

std::unique_ptr<PGBackend> PGBackend::create(const spg_t pgid,
                                             const pg_pool_t& pool,
                                             ceph::os::CyanStore* store,
                                             const ec_profile_t& ec_profile)
{
  auto coll = store->open_collection(coll_t{pgid});
  switch (pool.type) {
  case pg_pool_t::TYPE_REPLICATED:
    return std::make_unique<ReplicatedBackend>(pgid.shard, coll, store);
  case pg_pool_t::TYPE_ERASURE:
    return std::make_unique<ECBackend>(pgid.shard, coll, store,
                                       std::move(ec_profile),
                                       pool.stripe_width);
  default:
    throw runtime_error(seastar::format("unsupported pool type '{}'",
                                        pool.type));
  }
}

PGBackend::PGBackend(shard_id_t shard,
                     CollectionRef coll,
                     ceph::os::CyanStore* store)
  : shard{shard},
    coll{coll},
    store{store}
{}

seastar::future<PGBackend::cached_oi_t>
PGBackend::get_object(const hobject_t& oid)
{
  // want the head?
  if (oid.snap == CEPH_NOSNAP) {
    logger().trace("find_object: {}@HEAD", oid);
    return _load_oi(oid);
  } else {
    // we want a snap
    return _load_ss(oid).then([oid,this](cached_ss_t ss) {
      // head?
      if (oid.snap > ss->seq) {
        return _load_oi(oid.get_head());
      } else {
        // which clone would it be?
        auto clone = std::upper_bound(begin(ss->clones), end(ss->clones),
                                      oid.snap);
        if (clone == end(ss->clones)) {
          throw object_not_found{};
        }
        // clone
        auto soid = oid;
        soid.snap = *clone;
        return _load_ss(soid).then([soid,this](cached_ss_t ss) {
          auto clone_snap = ss->clone_snaps.find(soid.snap);
          assert(clone_snap != end(ss->clone_snaps));
          if (clone_snap->second.empty()) {
            logger().trace("find_object: {}@[] -- DNE", soid);
            throw object_not_found{};
          }
          auto first = clone_snap->second.back();
          auto last = clone_snap->second.front();
          if (first > soid.snap) {
            logger().trace("find_object: {}@[{},{}] -- DNE",
                           soid, first, last);
            throw object_not_found{};
          }
          logger().trace("find_object: {}@[{},{}] -- HIT",
                         soid, first, last);
          return _load_oi(soid);
        });
      }
    });
  }
}

seastar::future<PGBackend::cached_oi_t>
PGBackend::_load_oi(const hobject_t& oid)
{
  if (auto found = oi_cache.find(oid); found) {
    return seastar::make_ready_future<cached_oi_t>(std::move(found));
  }
  return store->get_attr(coll,
                         ghobject_t{oid, ghobject_t::NO_GEN, shard},
                         OI_ATTR).then([oid, this](auto bp) {
    auto oi = std::make_unique<object_info_t>();
    bufferlist bl;
    bl.push_back(std::move(bp));
    oi->decode(bl);
    return seastar::make_ready_future<cached_oi_t>(
      oi_cache.insert(oid, std::move(oi)));
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
                         SS_ATTR).then([oid, this](auto bp) {
    bufferlist bl;
    bl.push_back(std::move(bp));
    auto snapset = std::make_unique<SnapSet>(bl);
    return seastar::make_ready_future<cached_ss_t>(
      ss_cache.insert(oid, std::move(snapset)));
  });
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
          throw object_corrupted{};
        }
      }
      return seastar::make_ready_future<bufferlist>(std::move(bl));
    });
}
