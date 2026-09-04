// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_registration.hh>
#include "common/Formatter.h"

namespace crimson::osd::scrub {

/**
 * ScrubMetrics
 *
 * Seastar metrics for scrub operations in Crimson OSD.
 * Replaces the classic OSD perf counters with Seastar's native metrics system.
 */
class ScrubMetrics {
public:
  // Scrub I/O metrics (common to all pool types)
  uint64_t omapgetheader_cnt = 0;
  uint64_t omapgetheader_bytes = 0;
  uint64_t omapget_cnt = 0;
  uint64_t omapget_bytes = 0;

  // Pool-specific I/O metrics
  uint64_t getattr_cnt = 0;
  uint64_t stats_cnt = 0;
  uint64_t read_cnt = 0;
  uint64_t read_bytes = 0;

  // Scrub session metrics
  uint64_t started_cnt = 0;
  uint64_t active_started_cnt = 0;
  uint64_t successful_cnt = 0;
  uint64_t successful_elapsed = 0;
  uint64_t failed_cnt = 0;
  uint64_t failed_elapsed = 0;
  uint64_t write_intersects = 0;
  uint64_t write_blocked = 0;

  // Reservation process metrics
  uint64_t rsv_successful_cnt = 0;
  uint64_t rsv_successful_elapsed = 0;
  uint64_t rsv_aborted_cnt = 0;
  uint64_t rsv_rejected_cnt = 0;
  uint64_t rsv_skipped_cnt = 0;
  uint64_t rsv_failed_elapsed = 0;
  uint64_t rsv_secondaries_num = 0;

  ScrubMetrics() = default;

  void register_metrics(
    const std::string& pool_type,
    const std::string& scrub_level,
    const std::string& pg_id);

  bool is_registered() const { return metrics_registered; }

  void inc_started() { ++started_cnt; }
  void inc_active_started() { ++active_started_cnt; }
  void inc_successful(uint64_t elapsed) {
    ++successful_cnt;
    successful_elapsed += elapsed;
  }
  void inc_failed(uint64_t elapsed) {
    ++failed_cnt;
    failed_elapsed += elapsed;
  }
  void inc_write_intersects() { ++write_intersects; }
  void inc_write_blocked() { ++write_blocked; }

  void inc_getattr() { ++getattr_cnt; }
  void inc_stats() { ++stats_cnt; }
  void inc_read(uint64_t bytes) {
    ++read_cnt;
    read_bytes += bytes;
  }
  void inc_omapgetheader(uint64_t bytes) {
    ++omapgetheader_cnt;
    omapgetheader_bytes += bytes;
  }
  void inc_omapget(uint64_t bytes) {
    ++omapget_cnt;
    omapget_bytes += bytes;
  }

  void inc_rsv_successful(uint64_t elapsed) {
    ++rsv_successful_cnt;
    rsv_successful_elapsed += elapsed;
  }
  void inc_rsv_aborted() { ++rsv_aborted_cnt; }
  void inc_rsv_rejected() { ++rsv_rejected_cnt; }
  void inc_rsv_skipped() { ++rsv_skipped_cnt; }
  void inc_rsv_failed(uint64_t elapsed) {
    rsv_failed_elapsed += elapsed;
  }
  void set_rsv_secondaries_num(uint64_t num) {
    rsv_secondaries_num = num;
  }

  // Dump metrics for debugging/monitoring
  void dump(ceph::Formatter* f) const;

private:
  seastar::metrics::metric_groups metrics;
  bool metrics_registered = false;
};

} // namespace crimson::osd::scrub
