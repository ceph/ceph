// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <algorithm>
#include <cmath>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/spawn.hpp>

#include "include/neorados/RADOS.hpp"
#include "include/buffer.h"

#include "common/async/yield_context.h"
#include "common/containers.h"

#include "cls/rgw/cls_rgw_ops.h"

#include "neorados/cls/fifo.h"

#include "rgw_log_backing.h"
#include "rgw_bucket_layout.h"

class RGWSI_BucketIndex_RADOS;

namespace asio = boost::asio;
namespace fifo = neorados::cls::fifo;
using ceph::containers::tiny_vector;

// build a bilog FIFO OID with format: "{bucket_id}.{log_gen}.{shard_id}.bilog".
inline std::string bilog_fifo_oid(std::string_view bucket_id,
                                   uint64_t log_gen,
                                   int shard_id)
{
  return std::string{bucket_id} + '.' +
         std::to_string(log_gen) + '.' +
         std::to_string(shard_id) + ".bilog";
}

// compute an appropriate number of bilog shards for a given index shard count
// upon reshard. uses logarithmic formula to keep the bilog shard count much lower,
// growing slowly every time index shards double, bilog gets 2 more shards.
// typically ranges from 7 to 21 across the default index max shards 11 to 1999 shards.
inline uint32_t bilog_shards_for_index(uint32_t index_shards,
                                       uint32_t min_shards = 7,
                                       uint32_t max_shards = 0)
{
  if (index_shards == 0) {
    return min_shards;
  }
  auto v = static_cast<uint32_t>(std::log2(index_shards) * 2.0);
  v = std::max(v, min_shards);
  if (max_shards > 0) {
    v = std::min(v, max_shards);
  }
  return v;
}

// per-bucket FIFO bilog: one LazyFIFO per bilog shard, stored in the log pool.

// shard count is recorded in bucket_log_layout_generation::layout::fifo::num_shards.
class RGWBILogFIFO {
  tiny_vector<LazyFIFO> fifos;

public:
  RGWBILogFIFO(neorados::RADOS rados,
               const neorados::IOContext& log_pool,
               std::string_view bucket_id,
               uint64_t log_gen,
               uint32_t num_shards);

  int num_shards() const {
    return static_cast<int>(fifos.size());
  }

  // append a single bilog entry to shard_id (awaitable).
  asio::awaitable<void> push(const DoutPrefixProvider* dpp,
                             int shard_id,
                             ceph::buffer::list entry);

  // append a single bilog entry to shard_id (yield_context).
  void push(const DoutPrefixProvider* dpp,
            int shard_id,
            ceph::buffer::list entry,
            asio::yield_context y);

  // list up to entries.size() entries from shard_id starting after marker.
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

  // trim shard_id up to and including marker.
  // exclusive=true leaves the entry at marker; exclusive=false removes it.
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

// per-shard FIFO bilog batch writer for FIFO-backed buckets.
// entries are staged internally and flushed to each shard's LazyFIFO either
// via an explicit flush() call or on destruction.
class RGWBILogUpdateBatch {
  const DoutPrefixProvider* dpp;
  neorados::RADOS rados_;
  std::shared_ptr<RGWBILogFIFO> fifo_;
  struct Pending {
    int shard;
    rgw_bi_log_entry entry;
  };
  std::vector<Pending> pending;

  // returns the shard index for 'key' using the same hash as the bucket index.
  int shard_of(const cls_rgw_obj_key& key) const;
  // append an entry to the pending list.
  void stage(int shard, rgw_bi_log_entry entry);
  // encode and push all pending entries to their respective FIFO shards.
  int do_flush(asio::yield_context y);
  int do_flush();

public:
  // fifo may be null (error-path batch — silently drops all entries).
  RGWBILogUpdateBatch(const DoutPrefixProvider* dpp,
                      neorados::RADOS r,
                      std::shared_ptr<RGWBILogFIFO> fifo);

  // generic complete/cancel/add/del ops — built from an OpIssuer's base fields.
  void add_maybe_flush(uint64_t olh_epoch,
                       ceph::real_time mtime,
                       const cls_rgw_bi_log_related_op& op_info);

  // OLH link entry — built during apply_olh_log replay (no OpIssuer available).
  void add_maybe_flush(uint64_t olh_epoch,
                       const cls_rgw_obj_key& key,
                       const std::string& op_tag,
                       bool delete_marker,
                       ceph::real_time mtime,
                       const rgw_zone_set& zones_trace);

  // disk-state correction entry — built during check_disk_state reconciliation.
  void add_maybe_flush(RGWModifyOp op,
                       const rgw_bucket_dir_entry& list_state,
                       rgw_zone_set zones_trace);

  int flush(optional_yield y);
  int flush();
  // awaitable variant — use from coroutine contexts (avoids use_blocked).
  asio::awaitable<int> co_flush();

  ~RGWBILogUpdateBatch();
};
