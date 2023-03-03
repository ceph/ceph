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

#pragma once

#include <span>
#include "common/ceph_time.h"
#include "rgw_common.h"
#include "rgw_meta_sync_status.h"
#include "common.h"

struct rgw_mdlog_entry;
struct rgw_mdlog_info;
struct rgw_mdlog_shard_data;

namespace rgw::sync::metadata {

struct meta_list_result; // nested in RGWFetchAllMetaCR
using mdlog_shard_data = rgw_mdlog_shard_data;
using mdlog_entry = rgw_mdlog_entry;
using mdlog_info = rgw_mdlog_info;
struct mdlog_shard_info; // RGWMetadataLogInfo

class RemoteMetadata {
 public:
  virtual ~RemoteMetadata() {}

  virtual auto list_sections(std::vector<std::string>& sections)
      -> awaitable<void> = 0;

  struct list_result {
    std::span<std::string> entries;
    std::string marker;
  };

  virtual auto list(std::string_view section,
                    std::string_view marker,
                    std::span<std::string> entries)
      -> awaitable<list_result> = 0;

  virtual auto read(std::string_view section,
                    std::string_view key,
                    bufferlist& bl)
      -> awaitable<void> = 0;
};

class LocalMetadata {
 public:
  virtual ~LocalMetadata() {}

  virtual auto write(std::string_view section,
                     RGWObjVersionTracker& objv,
                     std::string_view key,
                     const bufferlist& bl)
      -> awaitable<void> = 0;

  virtual auto remove(std::string_view section,
                      RGWObjVersionTracker& objv,
                      std::string_view key)
      -> awaitable<void> = 0;
};

class RemoteLog {
 public:
  virtual ~RemoteLog() {}

  virtual auto info(mdlog_info& result)
      -> awaitable<void> = 0;

  virtual auto shard_info(std::string_view period, uint32_t shard,
                          mdlog_shard_info& result)
      -> awaitable<void> = 0;

  virtual auto list(std::string_view period, uint32_t shard,
                    std::string_view marker,
                    mdlog_shard_data& result)
      -> awaitable<void> = 0;
};

class LocalLog {
 public:
  virtual ~LocalLog() {}

  virtual auto list(std::string_view period, uint32_t shard,
                    std::string_view marker,
                    mdlog_shard_data& result)
      -> awaitable<void> = 0;

  virtual auto write(std::string_view period, uint32_t shard,
                     std::span<const mdlog_entry> entries)
      -> awaitable<void> = 0;
};

class LocalFullSyncIndex {
 public:
  virtual ~LocalFullSyncIndex() {}

  struct list_result {
    std::span<std::string> entries;
    std::string marker;
  };

  virtual auto list(uint32_t shard, std::string_view marker,
                    std::span<std::string> entries)
      -> awaitable<list_result> = 0;

  virtual auto write(uint32_t shard, std::span<const std::string> entries)
      -> awaitable<void> = 0;
};

class Status {
 public:
  virtual ~Status() {}

  virtual auto read(RGWObjVersionTracker& objv,
                    rgw_meta_sync_info& status)
      -> awaitable<void> = 0;

  virtual auto write(RGWObjVersionTracker& objv,
                     const rgw_meta_sync_info& status)
      -> awaitable<void> = 0;
};

class LogStatus {
 public:
  virtual ~LogStatus() {}

  virtual auto read(uint32_t shard, RGWObjVersionTracker& objv,
                    rgw_meta_sync_marker& status)
      -> awaitable<void> = 0;

  virtual auto write(uint32_t shard, RGWObjVersionTracker& objv,
                     const rgw_meta_sync_marker& status)
      -> awaitable<void> = 0;

  virtual auto lock(uint32_t shard, std::string_view cookie,
                    ceph::timespan duration, bool renew)
      -> awaitable<void> = 0;

  virtual auto unlock(uint32_t shard, std::string_view cookie)
      -> awaitable<void> = 0;
};

// start global metadata sync
auto sync(const DoutPrefixProvider* dpp, uint32_t shards,
          RemoteLog& peer_log, LocalLog& local_log,
          RemoteMetadata& peer_meta, LocalMetadata& local_meta,
          LocalFullSyncIndex& local_index,
          Status& status, LogStatus& log_status)
    -> awaitable<void>;

// run incremental sync on the given period
auto sync_period(const DoutPrefixProvider* dpp, uint32_t shards,
                 std::string_view period,
                 RemoteLog& peer_log, LocalLog& local_log,
                 RemoteMetadata& peer_meta, LocalMetadata& local_meta,
                 LogStatus& log_status)
    -> awaitable<void>;

// run incremental sync on the given mdlog shard
auto sync_shard(const DoutPrefixProvider* dpp,
                std::string_view period, uint32_t shard,
                RemoteLog& peer_log, LocalLog& local_log,
                RemoteMetadata& peer_meta, LocalMetadata& local_meta,
                LogStatus& log_status)
    -> awaitable<void>;

// copy remote log entries to the local log, starting from the given marker.
// returns a marker for the next call
auto clone_log(const DoutPrefixProvider* dpp, uint32_t shard,
               std::string_view period, std::string_view marker,
               RemoteLog& peer_log, LocalLog& local_log)
    -> awaitable<std::string>;

} // namespace rgw::sync::metadata
