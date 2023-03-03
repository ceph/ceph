// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/async/co_throttle.h"
#include "common/async/parallel_for_each.h"
#include "lease.h"
#include "metadata.h"

#include "rgw_sync.h"

namespace rgw::sync::metadata {

auto sync_init(const DoutPrefixProvider* dpp,
               rgw_meta_sync_info& sync_info,
               RemoteLog& peer_log)
    -> awaitable<void>
{
  mdlog_info log_info;
  try {
    co_await peer_log.info(log_info);
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0) << "failed to read remote mdlog info: "
        << e.what() << dendl;
    throw;
  }

  sync_info.num_shards = log_info.num_shards;
  sync_info.period = log_info.period;
  sync_info.realm_epoch = log_info.realm_epoch;
}

auto build_section_index(const DoutPrefixProvider* dpp,
                         uint32_t shards, std::string_view section,
                         RemoteMetadata& peer_meta,
                         LocalFullSyncIndex& local_index)
    -> awaitable<void>
{
  constexpr size_t max_entries = 1000;
  std::array<std::string, max_entries> keys;

  RemoteMetadata::list_result listing;
  do {
    listing = co_await peer_meta.list(section, listing.marker, keys);
    uint32_t shard = 0; // TODO: shard placement, per-shard batching
    co_await local_index.write(shard, listing.entries);
  } while (!listing.marker.empty());
}

auto build_index(const DoutPrefixProvider* dpp,
                 uint32_t shards, RemoteMetadata& peer_meta,
                 LocalFullSyncIndex& local_index)
    -> awaitable<void>
{
  std::vector<std::string> sections;
  co_await peer_meta.list_sections(sections);

  for (std::string_view section : sections) {
    co_await build_section_index(dpp, shards, section, peer_meta, local_index);
  }
}

auto try_sync(const DoutPrefixProvider* dpp,
              RemoteLog& peer_log, LocalLog& local_log,
              RemoteMetadata& peer_meta, LocalMetadata& local_meta,
              LocalFullSyncIndex& local_index,
              Status& status, LogStatus& log_status)
    -> awaitable<void>
{
  // read the global metadata sync status
  rgw_meta_sync_info sync_info;
  RGWObjVersionTracker objv;
  try {
    co_await status.read(objv, sync_info);
  } catch (const boost::system::system_error& e) {
    // treat ENOENT as StateInit
    if (e.code() != boost::system::errc::no_such_file_or_directory) {
      ldpp_dout(dpp, 0) << "failed to read metadata sync status: "
          << e.what() << dendl;
      throw;
    }
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0) << "failed to read metadata sync status: "
        << e.what() << dendl;
    throw;
  }

  if (sync_info.state == rgw_meta_sync_info::SyncState::StateInit) {
    try {
      co_await sync_init(dpp, sync_info, peer_log);
    } catch (const std::exception& e) {
      ldpp_dout(dpp, 0) << "metadata sync init failed: " << e.what() << dendl;
      throw;
    }
    sync_info.state = rgw_meta_sync_info::SyncState::StateBuildingFullSyncMaps;
    try {
      co_await status.write(objv, sync_info);
    } catch (const std::exception& e) {
      ldpp_dout(dpp, 0) << "failed to write metadata sync status: "
          << e.what() << dendl;
      throw;
    }
  }

  if (sync_info.state == rgw_meta_sync_info::SyncState::StateBuildingFullSyncMaps) {
    try {
      co_await build_index(dpp, sync_info.num_shards, peer_meta, local_index);
    } catch (const std::exception& e) {
      ldpp_dout(dpp, 0) << "metadata sync failed to build index: "
          << e.what() << dendl;
      throw;
    }
    sync_info.state = rgw_meta_sync_info::SyncState::StateSync;
    try {
      co_await status.write(objv, sync_info);
    } catch (const std::exception& e) {
      ldpp_dout(dpp, 0) << "failed to write metadata sync status: "
          << e.what() << dendl;
      throw;
    }
  }

  // TODO: for-loop over periods
  try {
    co_await sync_period(dpp, sync_info.num_shards, sync_info.period,
                         peer_log, local_log,
                         peer_meta, local_meta, log_status);
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0) << "metadata sync on period " << sync_info.period
        << " failed: " << e.what() << dendl;
    throw;
  }

  // TODO: update sync info for next period
}

class SyncPrefix : public DoutPrefixPipe {
 public:
  SyncPrefix(const DoutPrefixProvider& dpp) : DoutPrefixPipe(dpp) {}
  void add_prefix(std::ostream& out) const override {
    out << "metadata sync ";
  }
};

auto sync(const DoutPrefixProvider* dpp,
          RemoteLog& peer_log, LocalLog& local_log,
          RemoteMetadata& peer_meta, LocalMetadata& local_meta,
          LocalFullSyncIndex& local_index,
          Status& status, LogStatus& log_status)
    -> awaitable<void>
{
  auto prefix = SyncPrefix{*dpp};
  dpp = &prefix;

  for (;;) {
    try {
      co_await try_sync(dpp, peer_log, local_log,
                        peer_meta, local_meta,
                        local_index, status, log_status);
    } catch (const boost::system::system_error& e) {
      if (e.code() != boost::system::errc::operation_canceled) {
        ldpp_dout(dpp, 0) << "failed: " << e.what() << dendl;
        throw;
      }
      // retry on ECANCELED
      ldpp_dout(dpp, 20) << "raced to update sync status, retrying" << dendl;
    } catch (const std::exception& e) {
      ldpp_dout(dpp, 0) << "failed: " << e.what() << dendl;
      throw;
    }
  }
}

class PeriodPrefix : public DoutPrefixPipe {
  std::string_view period;
 public:
  PeriodPrefix(const DoutPrefixProvider& dpp, std::string_view period)
    : DoutPrefixPipe(dpp), period(period) {}
  void add_prefix(std::ostream& out) const override {
    out << "period:" << period << ' ';
  }
};

auto sync_period(const DoutPrefixProvider* dpp, uint32_t shards,
                 std::string_view period,
                 RemoteLog& peer_log, LocalLog& local_log,
                 RemoteMetadata& peer_meta, LocalMetadata& local_meta,
                 LogStatus& log_status)
    -> awaitable<void>
{
  auto prefix = PeriodPrefix{*dpp, period};
  dpp = &prefix;

  co_await ceph::async::parallel_for_each(
      std::views::iota(0u, shards), // XXX: iota is broken on clang
      [&] (uint32_t shard) -> awaitable<void> {
        co_await sync_shard(dpp, period, shard, peer_log, local_log,
                            peer_meta, local_meta, log_status);
      });
}

auto sync_shard_locked(const DoutPrefixProvider* dpp,
                       std::string_view period, uint32_t shard,
                       RemoteLog& peer_log, LocalLog& local_log,
                       RemoteMetadata& peer_meta, LocalMetadata& local_meta,
                       LogStatus& log_status)
    -> awaitable<void>
{
  RGWObjVersionTracker objv;
  rgw_meta_sync_marker shard_status;
  co_await log_status.read(shard, objv, shard_status);

  if (shard_status.state == rgw_meta_sync_marker::SyncState::FullSync) {
  }

  if (shard_status.state == rgw_meta_sync_marker::SyncState::IncrementalSync) {
  }
}

// satisfies LockClient with calls to LogStatus::lock/unlock
struct ShardLockClient : LockClient {
  LogStatus& status;
  uint32_t shard;
  std::string_view cookie;

  ShardLockClient(LogStatus& status, uint32_t shard, std::string_view cookie)
    : status(status), shard(shard), cookie(cookie) {}

  awaitable<void> acquire(ceph::timespan duration) override {
    constexpr bool renew = false;
    co_await status.lock(shard, cookie, duration, renew);
  }
  awaitable<void> renew(ceph::timespan duration) override {
    constexpr bool renew = true;
    co_await status.lock(shard, cookie, duration, renew);
  }
  awaitable<void> release() override {
    co_await status.unlock(shard, cookie);
  }
};

class ShardPrefix : public DoutPrefixPipe {
  const uint32_t shard;
 public:
  ShardPrefix(const DoutPrefixProvider& dpp, uint32_t shard)
    : DoutPrefixPipe(dpp), shard(shard) {}
  void add_prefix(std::ostream& out) const override {
    out << '[' << shard << "] ";
  }
};

auto sync_shard(const DoutPrefixProvider* dpp,
                std::string_view period, uint32_t shard,
                RemoteLog& peer_log, LocalLog& local_log,
                RemoteMetadata& peer_meta, LocalMetadata& local_meta,
                LogStatus& log_status)
    -> awaitable<void>
{
  auto prefix = ShardPrefix{*dpp, shard};
  dpp = &prefix;

  using namespace std::chrono_literals;
  constexpr ceph::timespan duration = 120s; // TODO
  std::string cookie = "TODO";
  auto lock = ShardLockClient{log_status, shard, cookie};

  // retry on lock contention
  for (;;) {
    try {
      co_await with_lease(lock, duration,
          sync_shard_locked(dpp, period, shard, peer_log, local_log,
                            peer_meta, local_meta, log_status));
    } catch (const boost::system::system_error& e) {
      if (e.code() == boost::system::errc::timed_out) {
        ldpp_dout(dpp, 0) << "WARNING: aborted sync after lock renewal "
            "timed out" << dendl;
      } else if (e.code() == boost::system::errc::device_or_resource_busy) {
        ldpp_dout(dpp, 20) << "failed to lock sync status shard: "
            << e.code().message() << dendl;
      } else {
        throw;
      }
    }
    // TODO: sleep/backoff
  }
}

auto clone_log(const DoutPrefixProvider* dpp, std::string_view period,
               uint32_t shard, std::string_view marker,
               RemoteLog& peer_log, LocalLog& local_log)
    -> awaitable<std::string>
{
  mdlog_shard_data result;
  co_await peer_log.list(period, shard, marker, result);
  if (!result.entries.empty()) {
    co_await local_log.write(period, shard, result.entries);
  }
  co_return result.marker;
}

} // namespace rgw::sync::metadata
