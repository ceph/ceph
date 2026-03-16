// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "osd/osd_types.h"

#include "osd/scrubber/scrub_resources.h"
#include "scrub_queue.h"

namespace crimson::osd {
class ShardServices;

static constexpr int SCRUB_TICK_INTERVAL = 5; // seconds between scrub scheduler ticks

class ScrubScheduler {
  ShardServices &shard_services;
  /// resource reservation management
  scrub::ScrubResources m_resource_bookkeeper;
  /// the queue of PGs waiting to be scrubbed
  scrub::ScrubQueue m_queue;

  scrub::OSDRestrictions restrictions_on_scrubbing(
    bool is_recovery_active,
    utime_t scrub_clock_now) const;
  scrub::schedule_result_t initiate_a_scrub(
    const scrub::SchedEntry& candidate,
    scrub::OSDRestrictions restrictions);
  bool scrub_random_backoff() const;
  bool scrub_time_permit(utime_t now) const;
  bool scrub_load_below_threshold() const;

public:
  ScrubScheduler(ShardServices &shard_services)
    : shard_services(shard_services),
      m_resource_bookkeeper() {}
  ~ScrubScheduler() = default;

  seastar::future<bool> is_recovery_active();

  void initiate_scrub(bool is_recovery_active);
  void enqueue_scrub_job(const scrub::ScrubJob& sjob);
  void enqueue_target(const scrub::SchedTarget& trgt);
  void dequeue_target(spg_t pgid, scrub_level_t s_or_d);
  void remove_from_osd_queue(spg_t pgid);

  static bool is_sched_target_eligible(
    const scrub::SchedEntry& e,
    const scrub::OSDRestrictions& r,
    utime_t time_now);
    // updating the resource counters
  std::unique_ptr<scrub::LocalResourceWrapper> inc_scrubs_local(
      bool is_high_priority);
  void dec_scrubs_local();
    
  scrub::ScrubQueue& get_queue() {
    return m_queue;
  }
};
} // namespace crimson::osd
