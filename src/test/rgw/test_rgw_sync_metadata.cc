// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

//#define BOOST_ASIO_ENABLE_HANDLER_TRACKING
#include "sync/metadata.h"

#include <utility>
#include <vector>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/co_spawn.hpp>
#include <gtest/gtest.h>
#include "common/async/co_waiter.h"
#include "common/iso_8601.h"
#include "test/neorados/common_tests.h"

namespace rgw::sync::metadata {

using namespace std::chrono_literals;

using mdlog_map = std::map<std::string, rgw_mdlog_entry, std::less<>>;

auto generate_entries(uint32_t count,
                      ceph::real_time timestamp = ceph::real_clock::zero(),
                      ceph::timespan interval = 1s)
{
  mdlog_map entries;
  for (uint32_t i = 0; i < count; i++) {
    rgw_mdlog_entry entry;
    entry.timestamp = timestamp;
    entries.emplace(ceph::to_iso_8601(timestamp), entry);
    timestamp += interval;
  }
  return entries;
}

void mdlog_list(const mdlog_map& entries,
                std::string_view marker, uint32_t max_entries,
                rgw_mdlog_shard_data& result)
{
  auto entry = entries.lower_bound(marker);
  for (uint32_t i = 0;
       entry != entries.end() && i < max_entries;
       ++entry, ++i) {
    result.entries.push_back(entry->second);
  }
  result.truncated = (entry != entries.end());
  if (result.truncated) {
    result.marker = entry->first;
  } else {
    result.marker.clear();
  }
}

struct MockRemoteLogShard : RemoteLog {
  rgw_mdlog_info log;
  RGWMetadataLogInfo shard;
  mdlog_map entries;

  auto info(rgw_mdlog_info& result)
      -> boost::asio::awaitable<void> override
  {
    result = log;
    co_return;
  }

  auto shard_info(std::string_view period, uint32_t shard,
                  RGWMetadataLogInfo& result)
      -> boost::asio::awaitable<void> override
  {
    std::ignore = period;
    std::ignore = shard;
    result = this->shard;
    co_return;
  }

  auto list(std::string_view period, uint32_t shard,
            std::string_view marker, uint32_t max_entries,
            rgw_mdlog_shard_data& result)
      -> boost::asio::awaitable<void> override
  {
    std::ignore = period;
    std::ignore = shard;
    mdlog_list(entries, marker, max_entries, result);
    co_return;
  }
};

struct MockLocalLogShard : LocalLog {
  mdlog_map entries;

  auto list(std::string_view period, uint32_t shard,
            std::string_view marker, uint32_t max_entries,
            rgw_mdlog_shard_data& result)
      -> boost::asio::awaitable<void>
  {
    std::ignore = period;
    std::ignore = shard;
    mdlog_list(entries, marker, max_entries, result);
    co_return;
  }

  auto write(std::string_view period, uint32_t shard,
             std::span<const rgw_mdlog_entry> entries)
      -> boost::asio::awaitable<void>
  {
    std::ignore = period;
    std::ignore = shard;
    for (const auto& entry : entries) {
      this->entries.emplace(ceph::to_iso_8601(entry.timestamp), entry);
    }
    co_return;
  }
};

CORO_TEST(metadata_sync, clone_log)
{
  constexpr std::string_view period;
  constexpr uint32_t shard = 0;

  MockRemoteLogShard peer_log;
  peer_log.entries = generate_entries(100);

  {
    MockLocalLogShard local_log;
    std::string marker;
    marker = co_await clone_log(nullptr, period, shard,
                                marker, 1000, peer_log, local_log);
    EXPECT_EQ(100, local_log.entries.size());
    EXPECT_TRUE(marker.empty());
  }
  {
    MockLocalLogShard local_log;
    std::string marker;
    marker = co_await clone_log(nullptr, period, shard,
                                marker, 99, peer_log, local_log);
    EXPECT_EQ(99, local_log.entries.size());
    EXPECT_FALSE(marker.empty());

    marker = co_await clone_log(nullptr, period, shard,
                                marker, 1000, peer_log, local_log);
    EXPECT_EQ(100, local_log.entries.size());
    EXPECT_TRUE(marker.empty());
  }
}

} // namespace rgw::sync::metadata
