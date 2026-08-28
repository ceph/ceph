// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include "common/ceph_time.h"

namespace rgw::sal {
  class Driver;
}
class DoutPrefixProvider;
struct LanceDBSession;

namespace rgw::s3vector {
  bool init(const DoutPrefixProvider* dpp, rgw::sal::Driver* driver);
  void shutdown();
  void pause();
  void resume(const DoutPrefixProvider* dpp, rgw::sal::Driver* driver);
  // update whenever vectors are added to an index (row_count = number of rows mutated)
  bool notify_index_update(const DoutPrefixProvider* dpp, const std::string& bucket_name, const std::string& index_name, uint64_t row_count);
  // update whenever vectors are deleted from an index (row_count = number of keys deleted)
  bool notify_index_delete(const DoutPrefixProvider* dpp, const std::string& bucket_name, const std::string& index_name, uint64_t row_count);
  // update whenever a index is removed
  bool notify_index_remove(const DoutPrefixProvider* dpp, const std::string& bucket_name, const std::string& index_name);
  // get LanceDB session for a bucket, returns nullptr if session doesn't exist or manager is not initialized
  std::shared_ptr<const LanceDBSession> get_session(const DoutPrefixProvider* dpp, const std::string& bucket_name);
  // notify manager for session creation
  bool notify_session_create(const DoutPrefixProvider* dpp, const std::string& bucket_name);
  // notify manager for session deletion
  bool notify_session_delete(const DoutPrefixProvider* dpp, const std::string& bucket_name);

  // ==========================================================================
  // Background rebuild status/observability (consumed by the admin REST API).
  //
  // This layer is per-RGW-instance: it reports what THIS daemon is doing. The
  // returned structs are tagged with the daemon's identity (instance_id /
  // host_id) so the scope is unambiguous. See the design doc for the rationale
  // and the deferred cluster-wide aggregation plan.
  // ==========================================================================

  // Snapshot of an in-progress rebuild on this instance.
  struct active_build_info_t {
    std::string bucket;
    std::string index;
    ceph::coarse_real_time start_time;
    int lock_refreshes = 0;
  };

  // Global (process-wide) rebuild status + aggregate counters for this instance.
  struct background_status_t {
    // instance identity — makes the per-instance scope explicit in every report.
    std::string instance_id;
    std::string host_id;
    int active_rebuilds = 0;
    int max_concurrent_rebuilds = 0;
    int num_workers = 0;
    int tables_tracked = 0;
    // counters
    uint64_t total_rebuilds_started = 0;
    uint64_t total_rebuilds_completed = 0;
    uint64_t total_rebuilds_failed = 0;
    int peak_active_rebuilds = 0;
    uint64_t limit_reached_count = 0;
    uint64_t lock_refresh_count = 0;
    uint64_t lock_lost_count = 0;
    uint64_t lock_refresh_fail_count = 0;
    // active builds
    std::vector<active_build_info_t> active_builds_list;
  };

  // Returns the current status snapshot, or a default-constructed value (empty
  // instance_id) if the manager is not initialized.
  background_status_t get_background_status();

  // A single recorded event from the in-memory ring buffer.
  struct rebuild_event_info_t {
    std::string type;
    ceph::coarse_real_time timestamp;
    std::string bucket;
    std::string index;
    int active_rebuilds = 0;
    int max_concurrent = 0;
    int duration_ms = 0;
    std::string result;
  };

  // Returns recorded events, optionally filtered by timestamp (epoch seconds,
  // only events with timestamp >= since_epoch) and by bucket name.
  std::vector<rebuild_event_info_t> get_rebuild_events(
    uint64_t since_epoch = 0,
    const std::string& bucket_filter = "");
}

