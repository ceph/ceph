// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "include/utime_fmt.h"
#include "osd/osd_types.h"
#include "osd/scrubber_common.h"

#include "utime.h"

namespace Scrub {
class ScrubSchedListener;
class SchedEntry;

/**
 *  the interface used by ScrubJob (a component of the PgScrubber) to access
 *  the scrub scheduling functionality.
 *  Separated from the actual implementation mostly due to cyclic dependencies.
 */
struct ScrubQueueOps {

  // a mockable ceph_clock_now(), to allow unit-testing of the scrub scheduling
  virtual utime_t scrub_clock_now() const = 0;

  virtual sched_conf_t populate_config_params(
      const pool_opts_t& pool_conf) const = 0;

  virtual void remove_entry(spg_t pgid, scrub_level_t s_or_d) = 0;

  /**
   * add both targets to the queue (but only if urgency>off)
   * Note: modifies the entries (setting 'is_valid') before queuing them.
   * \retval false if the targets were disabled (and were not added to
   * the queue)
   * \todo when implementing a queue w/o the need for white-out support -
   * restore to const&.
   */
  virtual bool
  queue_entries(spg_t pgid, SchedEntry shallow, SchedEntry deep) = 0;

  virtual void cp_and_queue_target(SchedEntry t) = 0;

  virtual ~ScrubQueueOps() = default;
};

}  // namespace Scrub
