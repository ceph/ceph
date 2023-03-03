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

#include "metadata.h"

#include <boost/asio/basic_waitable_timer.hpp>
#include <boost/intrusive/set.hpp>
#include "include/scope_guard.h"
#include "common/async/co_throttle.h"
#include "common/async/parallel_for_each.h"
#include "common/ceph_time.h"
#include "lease.h"

namespace rgw::sync::metadata {

using ceph::async::cancel_on_error;
using ceph::async::co_throttle;

static auto sync_init(const DoutPrefixProvider* dpp,
                      rgw_meta_sync_info& sync_info,
                      RemoteLog& peer_log)
    -> boost::asio::awaitable<void>
{
  rgw_mdlog_info log_info;
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

static auto build_section_index(const DoutPrefixProvider* dpp,
                                uint32_t shards, std::string_view section,
                                RemoteMetadata& peer_meta,
                                LocalFullSyncIndex& local_index)
    -> boost::asio::awaitable<void>
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

static auto build_index(const DoutPrefixProvider* dpp,
                        uint32_t shards, RemoteMetadata& peer_meta,
                        LocalFullSyncIndex& local_index)
    -> boost::asio::awaitable<void>
{
  std::vector<std::string> sections;
  co_await peer_meta.list_sections(sections);

  for (std::string_view section : sections) {
    co_await build_section_index(dpp, shards, section, peer_meta, local_index);
  }
}

static auto try_sync(const DoutPrefixProvider* dpp,
                     RemoteMetadata& peer_meta, LocalMetadata& local_meta,
                     LocalFullSyncIndex& local_index,
                     RemoteLog& peer_log, LocalLog& local_log,
                     Status& status, LogStatus& log_status)
    -> boost::asio::awaitable<void>
{
  // read the global metadata sync status
  rgw_meta_sync_info sync_info;
  RGWObjVersionTracker objv;
  try {
    co_await status.read(objv, sync_info);
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
                         peer_meta, local_meta, local_index,
                         peer_log, local_log, log_status);
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
    -> boost::asio::awaitable<void>
{
  auto prefix = SyncPrefix{*dpp};
  dpp = &prefix;

  for (;;) {
    try {
      co_await try_sync(dpp, peer_meta, local_meta, local_index,
                        peer_log, local_log, status, log_status);
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
                 std::string_view period, RemoteMetadata& peer_meta,
                 LocalMetadata& local_meta, LocalFullSyncIndex& local_index,
                 RemoteLog& peer_log, LocalLog& local_log,
                 LogStatus& log_status)
    -> boost::asio::awaitable<void>
{
  auto prefix = PeriodPrefix{*dpp, period};
  dpp = &prefix;

  co_await ceph::async::parallel_for_each(
      std::views::iota(0u, shards), // XXX: iota is broken on clang
      [&] (uint32_t shard) -> boost::asio::awaitable<void> {
        co_await sync_shard(dpp, period, shard, peer_meta, local_meta,
                            local_index, peer_log, local_log, log_status);
      });
}

static auto sync_single(const DoutPrefixProvider* dpp,
                        std::string_view section,
                        std::string_view key,
                        RemoteMetadata& peer_meta,
                        LocalMetadata& local_meta)
    -> boost::asio::awaitable<void>
{
  bufferlist bl;
  try {
    try {
      co_await peer_meta.read(section, key, bl);
    } catch (const std::exception& e) {
      ldpp_dout(dpp, 4) << "failed to read remote metadata: "
          << e.what() << dendl;
      throw;
    }
  } catch (const boost::system::system_error& e) {
    if (e.code() == boost::system::errc::no_such_file_or_directory) {
      // on ENOENT, leave bl empty and remove local metadata below
    } else {
      throw;
    }
  }

  if (bl.length()) {
    // copy the local metadata object
    try {
      co_await local_meta.write(section, key, bl);
    } catch (const std::exception& e) {
      ldpp_dout(dpp, 4) << "failed to write local metadata: "
          << e.what() << dendl;
      throw;
    }
  } else {
    // delete the local metadata object
    try {
      try {
        co_await local_meta.remove(section, key);
      } catch (const std::exception& e) {
        ldpp_dout(dpp, 4) << "failed to remove local metadata: "
            << e.what() << dendl;
        throw;
      }
    } catch (const boost::system::system_error& e) {
      if (e.code() == boost::system::errc::no_such_file_or_directory) {
        // treat ENOENT as success
      } else {
        throw;
      }
    }
  }
} // sync_single

class FullSyncIndexCursor {
  uint32_t shard;
  std::string marker;

  using storage_type = std::vector<std::string>;
  storage_type storage;
  storage_type::iterator pos;
  storage_type::iterator end;
 public:
  FullSyncIndexCursor(uint32_t shard, std::string marker, size_t batch_size)
    : shard(shard), marker(std::move(marker)), storage(batch_size),
      pos(storage.begin()), end(storage.begin())
  {}

  auto next(const DoutPrefixProvider* dpp, LocalFullSyncIndex& index)
      -> boost::asio::awaitable<std::string>
  {
    if (pos == end) {
      try {
        auto result = co_await index.list(shard, marker, storage);
        pos = storage.begin();
        end = std::next(pos, result.entries.size());
        marker = std::move(result.marker);
      } catch (const std::exception& e) {
        ldpp_dout(dpp, 4) << "failed to list full sync index: "
            << e.what() << dendl;
        throw;
      }
      if (pos == end) {
        co_return std::string{}; // end of listing
      }
    }

    co_return *pos++;
  }
};

// track outstanding full sync operations
namespace full_sync_tracker {

struct entry : boost::intrusive::set_base_hook<> {
  std::string key;
  uint64_t pos;

  entry(std::string key, uint64_t pos) : key(std::move(key)), pos(pos) {}
};
struct key_less {
  bool operator()(std::string_view lhs, std::string_view rhs) const {
    return lhs < rhs;
  }
  bool operator()(const entry& lhs, const entry& rhs) const {
    return lhs.key < rhs.key;
  }
};
struct entry_key {
  using type = std::string_view;
  type operator()(const entry& m) const { return m.key; }
};
using set = boost::intrusive::set<entry,
      boost::intrusive::compare<key_less>,
      boost::intrusive::key_of_value<entry_key>>;

} // namespace full_sync_tracker

class KeyPrefix : public DoutPrefixPipe {
  std::string_view key;
 public:
  KeyPrefix(const DoutPrefixProvider& dpp, std::string_view key)
    : DoutPrefixPipe(dpp), key(key) {}
  void add_prefix(std::ostream& out) const override { out << key << ' '; }
};

struct as_full {
  const rgw_meta_sync_marker& status;
};
static std::ostream& operator<<(std::ostream& out, const as_full& f) {
  return out << "full:" << f.status.marker
      << ' ' << f.status.pos
      << '/' << f.status.total_entries;
}

struct as_incremental {
  const rgw_meta_sync_marker& status;
};
static std::ostream& operator<<(std::ostream& out, const as_incremental& i) {
  return out << "inc:" << i.status.marker
      << ' ' << i.status.timestamp;
}

static auto full_sync_single(const DoutPrefixProvider* dpp,
                             RemoteMetadata& peer_meta,
                             LocalMetadata& local_meta,
                             full_sync_tracker::entry entry,
                             full_sync_tracker::set& outstanding,
                             rgw_meta_sync_marker& status)
    -> boost::asio::awaitable<void>
{
  const std::string_view raw_key = entry.key;
  auto prefix = KeyPrefix{*dpp, raw_key};
  dpp = &prefix;

  // register as outstanding so subsequent calls won't advance the sync status
  const auto [iter, inserted] = outstanding.insert(entry);
  if (!inserted) {
    ldpp_dout(dpp, 4) << "full_sync_single skipping duplicate key" << dendl;
    co_return;
  }
  // erase on scope exit because the caller holds a reference to our stack
  auto eraser = make_scope_guard([&] { outstanding.erase(iter); });

  // parse the metadata section:key
  const auto colon = raw_key.find(':');
  const std::string_view section = raw_key.substr(0, colon);
  const std::string_view key = raw_key.substr(colon + 1);

  if (section.empty() || key.empty()) {
    // skip invalid metadata keys
    ldpp_dout(dpp, 4) << "full_sync_single skipping invalid key" << dendl;
  } else {
    ldpp_dout(dpp, 20) << "full_sync_single starting (outstanding -> "
       << outstanding.size() << ')' << dendl;
    try {
      co_await sync_single(dpp, section, key, peer_meta, local_meta);
    } catch (const std::exception& e) {
      ldpp_dout(dpp, 4) << "full_sync_single failed with " << e.what()
          << " (outstanding -> " << outstanding.size() - 1 << ')' << dendl;
      throw;
    }
  }

  // update status if all prior work completed
  if (iter == outstanding.begin()) {
    status.marker = iter->key;
    status.pos = iter->pos;
    ldpp_dout(dpp, 10) << "advancing sync status: " << as_full{status} << dendl;
  }

  ldpp_dout(dpp, 20) << "full_sync_single finishing (outstanding -> "
      << outstanding.size() - 1 << ')' << dendl;
} // full_sync_single

struct FullPrefix : DoutPrefixPipe {
  using DoutPrefixPipe::DoutPrefixPipe;
  void add_prefix(std::ostream& out) const override { out << "full "; }
};

static auto sync_shard_full(const DoutPrefixProvider* dpp, uint32_t shard,
                            RemoteMetadata& peer_meta,
                            LocalMetadata& local_meta,
                            LocalFullSyncIndex& local_index,
                            LogStatus& log_status,
                            rgw_meta_sync_marker& persisted_status,
                            RGWObjVersionTracker& objv)
    -> boost::asio::awaitable<void>
{
  auto prefix = FullPrefix{*dpp};
  dpp = &prefix;

  // use co_throttle to spawn a child coroutine for each full_sync_single()
  constexpr size_t spawn_window = 20; // TODO: rgw_meta_sync_spawn_window
  // on errors, cancel sync on later keys but let earlier ones finish so we
  // can update the sync status accordingly
  constexpr auto on_error = cancel_on_error::after;
  auto throttle = co_throttle{co_await boost::asio::this_coro::executor,
                              spawn_window, on_error};

  // track outstanding markers for sync status updates
  full_sync_tracker::set outstanding;
  uint64_t spawned_count = persisted_status.pos;

  // write sync status updates regularly to limit the amount of uncommitted
  // work, measured both by time and number of uncommitted entries
  using clock_type = ceph::coarse_mono_clock;
  using namespace std::chrono_literals;
  constexpr clock_type::duration max_uncommitted_interval = 1s;
  constexpr uint64_t max_uncommitted_count = 200;
  clock_type::time_point persisted_status_at = clock_type::now();

  // track in-memory updates separately
  rgw_meta_sync_marker current_status = persisted_status;
  std::exception_ptr eptr;

  // list the index in batches but loop over one key at a time
  auto cursor = FullSyncIndexCursor{shard, persisted_status.marker, 256};
  for (std::string key = co_await cursor.next(dpp, local_index);
       !key.empty(); key = co_await cursor.next(dpp, local_index)) {
    try {
      auto entry = full_sync_tracker::entry{std::move(key), ++spawned_count};
      // spawn sync on the single key. may throw exceptions from previous keys
      co_await throttle.spawn(
          full_sync_single(dpp, peer_meta, local_meta, std::move(entry),
                           outstanding, current_status));
    } catch (const std::exception&) {
      eptr = std::current_exception();
      break; // stop spawning more
    }

    if (current_status.marker == persisted_status.marker) {
      continue; // no marker updates yet
    }
    if (spawned_count < persisted_status.pos + spawn_window) {
      continue; // let the throttle fill up between status updates
    }
    if ((current_status.pos < persisted_status.pos + max_uncommitted_count) &&
        (clock_type::now() < persisted_status_at + max_uncommitted_interval)) {
      continue; // we've updated recently enough
    }

    // write a snapshot of the current status
    rgw_meta_sync_marker snapshot = current_status;
    ldpp_dout(dpp, 10) << "writing sync status: " << as_full{snapshot} << dendl;
    co_await log_status.write(shard, objv, snapshot); // exceptions propagate

    persisted_status = std::move(snapshot);
    persisted_status_at = clock_type::now();
  }

  try {
    // drain the remaining/uncanceled children
    co_await throttle.wait();
  } catch (const std::exception&) {
    eptr = std::current_exception();
  }

  if (eptr) {
    // on spawn() or wait() failure, record any progress before rethrowing
    if (current_status.marker != persisted_status.marker) try {
      ldpp_dout(dpp, 10) << "writing sync status: "
          << as_full{current_status} << dendl;
      co_await log_status.write(shard, objv, current_status);
      persisted_status = std::move(current_status);
    } catch (const std::exception&) {} // ignore and rethrow original

    std::rethrow_exception(eptr);
  }

  // on success, transition to incremental sync
  current_status.state = rgw_meta_sync_marker::SyncState::IncrementalSync;
  current_status.marker = std::move(current_status.next_step_marker);
  current_status.next_step_marker.clear();

  ldpp_dout(dpp, 10) << "writing sync status: "
      << as_incremental{current_status} << dendl;
  try {
    co_await log_status.write(shard, objv, current_status);
    persisted_status = std::move(current_status);
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 4) << "failed to write sync status with "
        << e.what() << dendl;
    throw;
  }
} // sync_shard_full

// track outstanding incremental sync operations
namespace inc_sync_tracker {

struct entry : boost::intrusive::set_base_hook<> {
  std::string key;
  ceph::real_time at;

  entry(std::string key, ceph::real_time at) : key(std::move(key)), at(at) {}
};
struct key_less {
  bool operator()(std::string_view lhs, std::string_view rhs) const {
    return lhs < rhs;
  }
  bool operator()(const entry& lhs, const entry& rhs) const {
    return lhs.key < rhs.key;
  }
};
struct entry_key {
  using type = std::string_view;
  type operator()(const entry& m) const { return m.key; }
};
using set = boost::intrusive::set<entry,
      boost::intrusive::compare<key_less>,
      boost::intrusive::key_of_value<entry_key>>;

} // namespace inc_sync_tracker

static auto inc_sync_single(const DoutPrefixProvider* dpp,
                            RemoteMetadata& peer_meta,
                            LocalMetadata& local_meta,
                            inc_sync_tracker::entry entry,
                            inc_sync_tracker::set& outstanding,
                            rgw_meta_sync_marker& status)
    -> boost::asio::awaitable<void>
{
  const std::string_view raw_key = entry.key;
  auto prefix = KeyPrefix{*dpp, raw_key};
  dpp = &prefix;

  // register as outstanding so subsequent calls won't advance the sync status
  const auto [iter, inserted] = outstanding.insert(entry);
  if (!inserted) {
    ldpp_dout(dpp, 4) << "inc_sync_single skipping duplicate key" << dendl;
    co_return;
  }
  // erase on scope exit because the caller holds a reference to our stack
  auto eraser = make_scope_guard([&] { outstanding.erase(iter); });

  // parse the metadata section:key
  const auto colon = raw_key.find(':');
  const std::string_view section = raw_key.substr(0, colon);
  const std::string_view key = raw_key.substr(colon + 1);

  if (section.empty() || key.empty()) {
    ldpp_dout(dpp, 4) << "inc_sync_single skipping invalid key" << dendl;
  } else {
    ldpp_dout(dpp, 20) << "inc_sync_single starting (outstanding -> "
       << outstanding.size() << ')' << dendl;
    try {
      co_await sync_single(dpp, section, key, peer_meta, local_meta);
    } catch (const std::exception& e) {
      ldpp_dout(dpp, 4) << "inc_sync_single failed with " << e.what()
          << " (outstanding -> " << outstanding.size() - 1 << ')' << dendl;
      throw;
    }
  }

  // update status if all prior work completed
  if (iter == outstanding.begin()) {
    status.marker = iter->key;
    status.timestamp = iter->at;
    ldpp_dout(dpp, 10) << "advancing sync status: "
        << as_incremental{status} << dendl;
  }

  ldpp_dout(dpp, 20) << "inc_sync_single finishing (outstanding -> "
      << outstanding.size() - 1 << ')' << dendl;
} // inc_sync_single

struct IncrementalPrefix : DoutPrefixPipe {
  using DoutPrefixPipe::DoutPrefixPipe;
  void add_prefix(std::ostream& out) const override { out << "inc "; }
};

static auto sync_shard_incremental(const DoutPrefixProvider* dpp,
                                   std::string_view period, uint32_t shard,
                                   RemoteMetadata& peer_meta,
                                   LocalMetadata& local_meta,
                                   RemoteLog& peer_log, LocalLog& local_log,
                                   LogStatus& log_status)
    -> boost::asio::awaitable<void>
{
  auto prefix = IncrementalPrefix{*dpp};
  dpp = &prefix;

  co_return;
}

static auto sync_shard_locked(const DoutPrefixProvider* dpp,
                              std::string_view period, uint32_t shard,
                              RemoteMetadata& peer_meta,
                              LocalMetadata& local_meta,
                              LocalFullSyncIndex& local_index,
                              RemoteLog& peer_log, LocalLog& local_log,
                              LogStatus& log_status, bool& reset_backoff)
    -> boost::asio::awaitable<void>
{
  reset_backoff = true;

  RGWObjVersionTracker objv;
  rgw_meta_sync_marker shard_status;
  co_await log_status.read(shard, objv, shard_status);

  if (shard_status.state == rgw_meta_sync_marker::SyncState::FullSync) {
    co_await sync_shard_full(dpp, shard, peer_meta, local_meta, local_index,
                             log_status, shard_status, objv);
  }

  if (shard_status.state == rgw_meta_sync_marker::SyncState::IncrementalSync) {
    co_await sync_shard_incremental(dpp, period, shard, peer_meta, local_meta,
                                    peer_log, local_log, log_status);
  }
}

// satisfies LockClient with calls to LogStatus::lock/unlock
struct ShardLockClient : LockClient {
  LogStatus& status;
  uint32_t shard;
  std::string_view cookie;

  ShardLockClient(LogStatus& status, uint32_t shard, std::string_view cookie)
    : status(status), shard(shard), cookie(cookie) {}

  boost::asio::awaitable<void> acquire(ceph::timespan duration) override {
    constexpr bool renew = false;
    co_await status.lock(shard, cookie, duration, renew);
  }
  boost::asio::awaitable<void> renew(ceph::timespan duration) override {
    constexpr bool renew = true;
    co_await status.lock(shard, cookie, duration, renew);
  }
  boost::asio::awaitable<void> release() override {
    co_await status.unlock(shard, cookie);
  }
};

static auto try_sync_shard(const DoutPrefixProvider* dpp,
                           std::string_view period, uint32_t shard,
                           RemoteMetadata& peer_meta,
                           LocalMetadata& local_meta,
                           LocalFullSyncIndex& local_index,
                           RemoteLog& peer_log, LocalLog& local_log,
                           LogStatus& log_status, bool& reset_backoff)
    -> boost::asio::awaitable<void>
{
  using namespace std::chrono_literals;
  constexpr ceph::timespan duration = 120s; // TODO
  std::string cookie = "TODO";
  auto lock = ShardLockClient{log_status, shard, cookie};

  try {
    co_await with_lease(lock, duration,
        sync_shard_locked(dpp, period, shard, peer_meta, local_meta,
                          local_index, peer_log, local_log,
                          log_status, reset_backoff));
  } catch (const lease_aborted& e) {
    ldpp_dout(dpp, 0) << "WARNING: aborted sync on lock renewal failure" << dendl;
    std::rethrow_exception(e.original_exception());
  }
}

class ShardPrefix : public DoutPrefixPipe {
  const uint32_t shard;
 public:
  ShardPrefix(const DoutPrefixProvider& dpp, uint32_t shard)
    : DoutPrefixPipe(dpp), shard(shard) {}
  void add_prefix(std::ostream& out) const override {
    out << "shard:" << shard << ' ';
  }
};

auto sync_shard(const DoutPrefixProvider* dpp,
                std::string_view period, uint32_t shard,
                RemoteMetadata& peer_meta, LocalMetadata& local_meta,
                LocalFullSyncIndex& local_index,
                RemoteLog& peer_log, LocalLog& local_log,
                LogStatus& log_status)
    -> boost::asio::awaitable<void>
{
  auto prefix = ShardPrefix{*dpp, shard};
  dpp = &prefix;

  using namespace std::chrono_literals;
  constexpr ceph::timespan max_retry_interval = 30s;
  constexpr ceph::timespan min_retry_interval = 1s;
  ceph::timespan retry_interval = min_retry_interval;

  using timer_type = boost::asio::basic_waitable_timer<ceph::coarse_mono_clock>;
  auto retry_timer = timer_type{co_await boost::asio::this_coro::executor};

  // retry on lock contention
  for (;;) {
    bool reset_backoff = false;
    try {
      co_await try_sync_shard(dpp, period, shard, peer_meta, local_meta,
                              local_index, peer_log, local_log,
                              log_status, reset_backoff);
    } catch (const boost::system::system_error& e) {
      if (e.code() == boost::system::errc::timed_out ||
          e.code() == boost::system::errc::device_or_resource_busy) {
        ldpp_dout(dpp, 4) << "failed to lock sync status shard: "
            << e.code().message() << dendl;
      } else {
        throw; // rethrow other errors without retry
      }
    }

    if (reset_backoff) {
      retry_interval = min_retry_interval;
    }
    retry_timer.expires_after(retry_interval);
    co_await retry_timer.async_wait(boost::asio::use_awaitable);
    retry_interval = std::min(retry_interval * 2, max_retry_interval);
  }
}

auto clone_log(const DoutPrefixProvider* dpp, std::string_view period,
               uint32_t shard, std::string_view marker, uint32_t max_entries,
               RemoteLog& peer_log, LocalLog& local_log)
    -> boost::asio::awaitable<std::string>
{
  rgw_mdlog_shard_data result;
  co_await peer_log.list(period, shard, marker, max_entries, result);
  if (!result.entries.empty()) {
    co_await local_log.write(period, shard, result.entries);
  }
  co_return result.marker;
}

} // namespace rgw::sync::metadata
