// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <atomic>
#include <chrono>
#include <compare>
#include <iostream>
#include <memory>
#include <vector>

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

struct scrub_schedule_t {
  utime_t scheduled_at{};
  utime_t deadline{0, 0};
  utime_t not_before{utime_t::max()};
  // when compared - the 'not_before' is ignored, assuming
  // we never compare jobs with different eligibility status.
  std::partial_ordering operator<=>(const scrub_schedule_t& rhs) const
  {
    auto cmp1 = scheduled_at <=> rhs.scheduled_at;
    if (cmp1 != 0) {
      return cmp1;
    }
    return deadline <=> rhs.deadline;
  };
  bool operator==(const scrub_schedule_t& rhs) const = default;
};

struct sched_params_t {
  utime_t proposed_time{};
  double min_interval{0.0};
  double max_interval{0.0};
  must_scrub_t is_must{must_scrub_t::not_mandatory};
};

/**
 *  A collection of the configuration parameters (pool & OSD) that affect
 *  scrub scheduling.
 */
struct sched_conf_t {
  /// the desired interval between shallow scrubs
  double shallow_interval{0.0};

  /// the desired interval between deep scrubs
  double deep_interval{0.0};

  /**
   * the maximum interval between shallow scrubs, as determined by either the
   * OSD or the pool configuration. Empty if no limit is configured.
   */
  std::optional<double> max_shallow;

  /**
   * the maximum interval between deep scrubs.
   * For deep scrubs - there is no equivalent of scrub_max_interval. Per the
   * documentation, once deep_scrub_interval has passed, we are already
   * "overdue", at least as far as the "ignore allowed load" window is
   * concerned. \todo based on users complaints (and the fact that the
   * interaction between the configuration parameters is clear to no one),
   * this will be revised shortly.
   */
  double max_deep{0.0};

  /**
   * interval_randomize_ratio
   *
   * We add an extra random duration to the configured times when doing
   * scheduling. An event configured with an interval of <interval> will
   * actually be scheduled at a time selected uniformly from
   * [<interval>, (1+<interval_randomize_ratio>) * <interval>)
   */
  double interval_randomize_ratio{0.0};

  /**
   * a randomization factor aimed at preventing 'thundering herd' problems
   * upon deep-scrubs common intervals. If polling a random number smaller
   * than that percentage, the next shallow scrub is upgraded to deep.
   */
  double deep_randomize_ratio{0.0};

  /**
   * must we schedule a scrub with high urgency if we do not have a valid
   * last scrub stamp?
   */
  bool mandatory_on_invalid{true};
};


class ScrubJob {
 public:
  /**
   * a time scheduled for scrub, and a deadline: The scrub could be delayed
   * if system load is too high (but not if after the deadline),or if trying
   * to scrub out of scrub hours.
   */
  scrub_schedule_t schedule;

  /// pg to be scrubbed
  spg_t pgid;

  /// the OSD id (for the log)
  int whoami;

  /**
   * Set whenever the PG scrubs are managed by the OSD (i.e. - from becoming
   * an active Primary till the end of the interval).
   */
  bool registered{false};

  /**
   * there is a scrub target for this PG in the queue.
   * \attn: temporary. Will be replaced with a pair of flags in the
   * two level-specific scheduling targets.
   */
  bool target_queued{false};

  /// how the last attempt to scrub this PG ended
  delay_cause_t last_issue{delay_cause_t::none};

  /**
    * the scrubber is waiting for locked objects to be unlocked.
    * Set after a grace period has passed.
    */
  bool blocked{false};
  utime_t blocked_since{};

  CephContext* cct;

  bool high_priority{false};

  ScrubJob(CephContext* cct, const spg_t& pg, int node_id);

  utime_t get_sched_time() const { return schedule.not_before; }

  std::string_view state_desc() const
  {
    return registered ? (target_queued ? "queued" : "registered")
		      : "not-registered";
  }

  /**
   * 'reset_failure_penalty' is used to reset the 'not_before' jo attribute to
   * the updated 'scheduled_at' time. This is used whenever the scrub-job
   * schedule is updated, and the update is not a result of a scrub attempt
   * failure.
   */
  void update_schedule(
      const scrub_schedule_t& adjusted,
      bool reset_failure_penalty);

  /**
   * If the scrub job was not explicitly requested, we postpone it by some
   * random length of time.
   * And if delaying the scrub - we calculate, based on pool parameters, a
   * deadline we should scrub before.
   */
  Scrub::scrub_schedule_t adjust_target_time(
    const Scrub::sched_params_t& recomputed_params) const;

  /**
   * push the 'not_before' time out by 'delay' seconds, so that this scrub target
   * would not be retried before 'delay' seconds have passed.
   */
  void delay_on_failure(
      std::chrono::seconds delay,
      delay_cause_t delay_cause,
      utime_t scrub_clock_now);

  /**
   * initial setting of the scheduling parameters of a newly registered
   * PG. The scrub targets (in this stage of the refactoring - the whole
   * scrub job) is initialized as for a regular periodic scrub.
   */
  void init_targets(
      const sched_params_t& suggested,
      const pg_info_t& info,
      const Scrub::sched_conf_t& aconf,
      utime_t scrub_clock_now);

 /**
   * recalculate the scheduling parameters for the periodic scrub targets.
   * Used whenever the "external state" of the PG changes, e.g. when made
   * primary - or indeed when the configuration changes.
   *
   * Does not modify ripe targets.
   * (why? for example, a 'scrub pg' command following a 'deepscrub pg'
   * would otherwise push the deep scrub to the future).
   */
  void on_periods_change(
      const sched_params_t& suggested,
      const Scrub::sched_conf_t& aconf,
      utime_t scrub_clock_now) {}

  void dump(ceph::Formatter* f) const;


  bool is_registered() const { return registered; }

  /**
   * is this a high priority scrub job?
   * High priority - (usually) a scrub that was initiated by the operator
   */
  bool is_high_priority() const { return high_priority; }

  /**
   * a text description of the "scheduling intentions" of this PG:
   * are we already scheduled for a scrub/deep scrub? when?
   */
  std::string scheduling_state(utime_t now_is, bool is_deep_expected) const;

  std::ostream& gen_prefix(std::ostream& out, std::string_view fn) const;
  std::string log_msg_prefix;

  // the comparison operator is used to sort the scrub jobs in the queue.
  // Note that it would not be needed in the next iteration of this code, as
  // the queue would *not* hold the full ScrubJob objects, but rather -
  // SchedTarget(s).
  std::partial_ordering operator<=>(const ScrubJob& rhs) const
  {
    return schedule <=> rhs.schedule;
  };
};

using ScrubQContainer = std::vector<std::unique_ptr<ScrubJob>>;

}  // namespace Scrub

namespace std {
std::ostream& operator<<(std::ostream& out, const Scrub::ScrubJob& pg);
}  // namespace std

namespace fmt {

template <>
struct formatter<Scrub::sched_params_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const Scrub::sched_params_t& pm, FormatContext& ctx)
  {
    return fmt::format_to(
	ctx.out(), "(proposed:{:s} min/max:{:.3f}/{:.3f} must:{:2s})",
        utime_t{pm.proposed_time}, pm.min_interval, pm.max_interval,
        pm.is_must == Scrub::must_scrub_t::mandatory ? "true" : "false");
  }
};


template <>
struct formatter<Scrub::ScrubJob> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const Scrub::ScrubJob& sjob, FormatContext& ctx)
  {
    return fmt::format_to(
	ctx.out(), "pg[{}] @ nb:{:s} ({:s}) (dl:{:s}) - <{}>",
	sjob.pgid, sjob.schedule.not_before, sjob.schedule.scheduled_at,
	sjob.schedule.deadline, sjob.state_desc());
  }
};

template <>
struct formatter<Scrub::sched_conf_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const Scrub::sched_conf_t& cf, FormatContext& ctx)
  {
    return fmt::format_to(
	ctx.out(),
	"periods:s:{}/{},d:{}/{},iv-ratio:{},deep-rand:{},on-inv:{}",
	cf.shallow_interval, cf.max_shallow.value_or(-1.0), cf.deep_interval,
	cf.max_deep, cf.interval_randomize_ratio, cf.deep_randomize_ratio,
	cf.mandatory_on_invalid);
  }
};
}  // namespace fmt
