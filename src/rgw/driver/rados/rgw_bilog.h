// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <optional>
#include <span>
#include <string>
#include <string_view>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/spawn.hpp>

#include "include/neorados/RADOS.hpp"
#include "include/buffer.h"

#include "common/async/yield_context.h"
#include "common/containers.h"

#include "neorados/cls/fifo.h"

#include "rgw_log_backing.h"

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

  /// trim shard_id up to and including marker?
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

  /// returns the absolute maximum marker string for the FIFO implementation
  static std::string_view max_marker();
};
