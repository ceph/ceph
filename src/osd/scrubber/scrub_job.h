// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <vector>

#include "common/RefCountedObj.h"
#include "common/ceph_atomic.h"
#include "include/utime_fmt.h"
#include "osd/osd_types.h"
#include "osd/osd_types_fmt.h"
#include "osd/scrubber_common.h"

/**
 * The ID used to name a candidate to scrub:
 * - in this version: a PG is identified by its spg_t
 * - in the (near) future: a PG + a scrub type (shallow/deep)
 */
using ScrubTargetId = spg_t;


namespace Scrub {

enum class must_scrub_t { not_mandatory, mandatory };

enum class qu_state_t {
  not_registered,  // not a primary, thus not considered for scrubbing by this
		   // OSD (also the temporary state when just created)
  registered,	   // in either of the two queues ('to_scrub' or 'penalized')
  unregistering	   // in the process of being unregistered. Will be finalized
		   // under lock
};

struct scrub_schedule_t {
  utime_t scheduled_at{};
  utime_t deadline{0, 0};
};

struct sched_params_t {
  utime_t proposed_time{};
  double min_interval{0.0};
  double max_interval{0.0};
  must_scrub_t is_must{must_scrub_t::not_mandatory};
};

class ScrubJob final : public RefCountedObject {
 public:
  /**
   * a time scheduled for scrub, and a deadline: The scrub could be delayed
   * if system load is too high (but not if after the deadline),or if trying
   * to scrub out of scrub hours.
   */
  scrub_schedule_t schedule;

  /// pg to be scrubbed
  const spg_t pgid;

  /// the OSD id (for the log)
  const int whoami;

  ceph::atomic<qu_state_t> state{qu_state_t::not_registered};

  /**
   * the old 'is_registered'. Set whenever the job is registered with the OSD,
   * i.e. is in either the 'to_scrub' or the 'penalized' vectors.
   */
  std::atomic_bool in_queues{false};

  /// last scrub attempt failed to secure replica resources
  bool resources_failure{false};

  /**
   * 'updated' is a temporary flag, used to create a barrier after
   * 'sched_time' and 'deadline' (or any other job entry) were modified by
   * different task.
   * 'updated' also signals the need to move a job back from the penalized
   * queue to the regular one.
   */
  std::atomic_bool updated{false};

  /**
    * the scrubber is waiting for locked objects to be unlocked.
    * Set after a grace period has passed.
    */
  bool blocked{false};
  utime_t blocked_since{};

  utime_t penalty_timeout{0, 0};

  CephContext* cct;

  ScrubJob(CephContext* cct, const spg_t& pg, int node_id);

  utime_t get_sched_time() const { return schedule.scheduled_at; }

  static std::string_view qu_state_text(qu_state_t st);

  /**
   * relatively low-cost(*) access to the scrub job's state, to be used in
   * logging.
   *  (*) not a low-cost access on x64 architecture
   */
  std::string_view state_desc() const
  {
    return qu_state_text(state.load(std::memory_order_relaxed));
  }

  void update_schedule(const scrub_schedule_t& adjusted);

  void dump(ceph::Formatter* f) const;

  /*
   * as the atomic 'in_queues' appears in many log prints, accessing it for
   * display-only should be made less expensive (on ARM. On x86 the _relaxed
   * produces the same code as '_cs')
   */
  std::string_view registration_state() const
  {
    return in_queues.load(std::memory_order_relaxed) ? "in-queue"
						     : "not-queued";
  }

  /**
   * access the 'state' directly, for when a distinction between 'registered'
   * and 'unregistering' is needed (both have in_queues() == true)
   */
  bool is_state_registered() const { return state == qu_state_t::registered; }

  /**
   * a text description of the "scheduling intentions" of this PG:
   * are we already scheduled for a scrub/deep scrub? when?
   */
  std::string scheduling_state(utime_t now_is, bool is_deep_expected) const;

  std::ostream& gen_prefix(std::ostream& out, std::string_view fn) const;
  const std::string log_msg_prefix;
};

using ScrubJobRef = ceph::ref_t<ScrubJob>;
using ScrubQContainer = std::vector<ScrubJobRef>;
}  // namespace Scrub

namespace std {
std::ostream& operator<<(std::ostream& out, const Scrub::ScrubJob& pg);
}  // namespace std

namespace fmt {
template <>
struct formatter<Scrub::qu_state_t> : formatter<std::string_view> {
  template <typename FormatContext>
  auto format(const Scrub::qu_state_t& s, FormatContext& ctx)
  {
    auto out = ctx.out();
    out = fmt::formatter<string_view>::format(
	std::string{Scrub::ScrubJob::qu_state_text(s)}, ctx);
    return out;
  }
};

template <>
struct formatter<Scrub::ScrubJob> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const Scrub::ScrubJob& sjob, FormatContext& ctx)
  {
    return fmt::format_to(
	ctx.out(),
	"pg[{}] @ {:s} (dl:{:s}) - <{}> / failure: {} / pen. t.o.: {:s} / "
	"queue "
	"state: {:.7}",
	sjob.pgid, sjob.schedule.scheduled_at, sjob.schedule.deadline,
	sjob.registration_state(), sjob.resources_failure, sjob.penalty_timeout,
	sjob.state.load(std::memory_order_relaxed));
  }
};
}  // namespace fmt
