// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/spawn.hpp>

#include "include/neorados/RADOS.hpp"
#include "include/buffer.h"

#include "common/async/yield_context.h"
#include "common/containers.h"

#include "cls/rgw/cls_rgw_ops.h"

#include "neorados/cls/fifo.h"

#include "rgw_log_backing.h"

class RGWSI_BucketIndex_RADOS;

namespace asio = boost::asio;
namespace fifo = neorados::cls::fifo;
using ceph::containers::tiny_vector;

/// FIFO OID for a given index shard OID "<shard_oid>.bilog"
/// e.g. ".dir.{bucket_id}.{gen}.{shard_id}.bilog"
inline std::string bilog_fifo_oid(std::string_view shard_oid)
{
  return std::string{shard_oid} + ".bilog";
}

/// per-bucket FIFO bilog: one LazyFIFO per index shard, stored in the same
/// RADOS pool as the bucket index, with OID = <shard_oid>.bilog.
///
/// callers obtain shard_oids from open_bucket_index() and pass them to the
/// constructor. The shard count and gen are recorded in bucket_log_layout
/// (field 'in_index') so that trim/list can reconstruct the OIDs later.
class RGWBILogFIFO {
  tiny_vector<LazyFIFO> fifos;

public:
  /// fifos[i] will use OID bilog_fifo_oid(shard_oids[i]).
  RGWBILogFIFO(neorados::RADOS rados,
               const neorados::IOContext& loc,
               std::span<const std::string> shard_oids);

  int num_shards() const {
    return static_cast<int>(fifos.size());
  }

  /// append a single bilog entry to shard_id (awaitable).
  asio::awaitable<void> push(const DoutPrefixProvider* dpp,
                             int shard_id,
                             ceph::buffer::list entry);

  /// append a single bilog entry to shard_id (yield_context).
  void push(const DoutPrefixProvider* dpp,
            int shard_id,
            ceph::buffer::list entry,
            asio::yield_context y);

  /// list up to entries.size() entries from shard_id starting after marker.
  asio::awaitable<std::tuple<std::span<fifo::entry>, std::optional<std::string>>>
  list(const DoutPrefixProvider* dpp,
       int shard_id,
       std::string marker,
       std::span<fifo::entry> entries);

  std::tuple<std::span<fifo::entry>, std::optional<std::string>>
  list(const DoutPrefixProvider* dpp,
       int shard_id,
       std::string marker,
       std::span<fifo::entry> entries,
       asio::yield_context y);

  /// trim shard_id up to and including marker.
  /// exclusive=true leaves the entry at marker; exclusive=false removes it.
  asio::awaitable<void> trim(const DoutPrefixProvider* dpp,
                             int shard_id,
                             std::string marker,
                             bool exclusive);

  void trim(const DoutPrefixProvider* dpp,
            int shard_id,
            std::string marker,
            bool exclusive,
            asio::yield_context y);

  static std::string_view max_marker();
};

/// per-shard FIFO bilog batch writer for FIFO-backed buckets.
/// entries are staged internally and flushed to each shard's LazyFIFO either
/// when the batch reaches max_batch_size or via an explicit flush() call.
/// the destructor flushes any remaining staged entries.
class RGWBILogUpdateBatch {
  const DoutPrefixProvider* dpp;
  neorados::RADOS rados_;
  tiny_vector<LazyFIFO> fifos;   // one per index shard
  int num_shards;
  struct Pending {
    int shard;
    rgw_bi_log_entry entry;
  };
  std::vector<Pending> pending;

  /// returns the shard index for 'key' using the same hash as the bucket index.
  int shard_of(const cls_rgw_obj_key& key) const;
  /// append an entry to the pending list.
  void stage(int shard, rgw_bi_log_entry entry);
  /// encode and push all pending entries to their respective FIFO shards.
  void do_flush(asio::yield_context y);
  void do_flush();

public:
  /// shard_oids must be ordered by shard ID (as returned by open_bucket_index).
  RGWBILogUpdateBatch(const DoutPrefixProvider* dpp,
                      neorados::RADOS r,
                      neorados::IOContext loc,
                      std::span<const std::string> shard_oids);

  /// generic complete/cancel/add/del ops — built from an OpIssuer's base fields.
  void add_maybe_flush(uint64_t olh_epoch,
                       ceph::real_time mtime,
                       const cls_rgw_bi_log_related_op& op_info);

  /// OLH link entry — built during apply_olh_log replay (no OpIssuer available).
  void add_maybe_flush(uint64_t olh_epoch,
                       const cls_rgw_obj_key& key,
                       const std::string& op_tag,
                       bool delete_marker,
                       ceph::real_time mtime,
                       const rgw_zone_set& zones_trace);

  /// disk-state correction entry — built during check_disk_state reconciliation.
  void add_maybe_flush(RGWModifyOp op,
                       const rgw_bucket_dir_entry& list_state,
                       rgw_zone_set zones_trace);

  void flush(asio::yield_context y);
  void flush();

  ~RGWBILogUpdateBatch();
};
