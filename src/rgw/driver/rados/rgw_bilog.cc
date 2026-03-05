// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_bilog.h"

#include <span>
#include <string>

#include "common/dout.h"

#define dout_subsys ceph_subsys_rgw

using ceph::containers::tiny_vector;

RGWBILogFIFO::RGWBILogFIFO(neorados::RADOS rados,
                             const neorados::IOContext& loc,
                             std::span<const std::string> shard_oids)
  : fifos(shard_oids.size(),
          [&rados, &loc, &shard_oids](std::size_t i, auto emplacer) {
            emplacer.emplace(rados, bilog_fifo_oid(shard_oids[i]), loc);
          })
{}

asio::awaitable<void>
RGWBILogFIFO::push(const DoutPrefixProvider* dpp,
                   int shard_id,
                   ceph::buffer::list entry)
{
  co_return co_await fifos[shard_id].push(dpp, std::move(entry));
}

void RGWBILogFIFO::push(const DoutPrefixProvider* dpp,
                   int shard_id,
                   ceph::buffer::list entry,
                   asio::yield_context y)
{
  fifos[shard_id].push(dpp, std::move(entry), y);
}

asio::awaitable<std::tuple<std::span<fifo::entry>, std::optional<std::string>>>
RGWBILogFIFO::list(const DoutPrefixProvider* dpp,
                   int shard_id,
                   std::string marker,
                   std::span<fifo::entry> entries)
{
  co_return co_await fifos[shard_id].list(dpp, std::move(marker), entries);
}

std::tuple<std::span<fifo::entry>, std::optional<std::string>>
RGWBILogFIFO::list(const DoutPrefixProvider* dpp,
                   int shard_id,
                   std::string marker,
                   std::span<fifo::entry> entries,
                   asio::yield_context y)
{
  return fifos[shard_id].list(dpp, std::move(marker), entries, y);
}

asio::awaitable<void>
RGWBILogFIFO::trim(const DoutPrefixProvider* dpp,
                   int shard_id,
                   std::string marker,
                   bool exclusive)
{
  co_return co_await fifos[shard_id].trim(dpp, std::move(marker), exclusive);
}

void RGWBILogFIFO::trim(const DoutPrefixProvider* dpp,
                   int shard_id,
                   std::string marker,
                   bool exclusive,
                   asio::yield_context y)
{
  fifos[shard_id].trim(dpp, std::move(marker), exclusive, y);
}

std::string_view RGWBILogFIFO::max_marker()
{
  static const std::string s = fifo::FIFO::max_marker();
  return std::string_view(s);
}
