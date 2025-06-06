// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <chrono>
#include <compare>
#include <iostream>
#include <memory>
#include <random>
#include <vector>

#include "common/ceph_atomic.h"
#include "common/fmt_common.h"
#include "include/utime_fmt.h"
#include "osd/osd_types.h"
#include "osd/osd_types_fmt.h"
#include "osd/scrubber_common.h"
#include "scrub_queue_entry.h"

namespace Scrub {

enum class must_scrub_t { not_mandatory, mandatory };

struct sched_params_t {
  utime_t proposed_time{};
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
   * upon deep-scrubs common intervals. The actual deep scrub interval will
   * be selected with a normal distribution around the configured interval,
   * with a standard deviation of <deep_randomize_ratio> * <interval>.
   */
  double deep_randomize_ratio{0.0};

  /**
   * must we schedule a scrub with high urgency if we do not have a valid
   * last scrub stamp?
   */
  bool mandatory_on_invalid{true};
};


/**
 * a wrapper around a Scrub::SchedEntry, adding some state flags
 * to be used only by the Scrubber. Note that the SchedEntry itself is known to
 * multiple objects (and must be kept small in size).
*/
struct SchedTarget {
  constexpr explicit SchedTarget(spg_t pg_id, scrub_level_t scrub_level)
      : sched_info{pg_id, scrub_level}
  {}

  /// our ID and scheduling parameters
  SchedEntry sched_info;

  /**
   * is this target (meaning - a copy of this specific combination of
   * PG and scrub type) currently in the queue?
   */
  bool queued{false};

  // some helper functions

  /// resets to the after-construction state
  void reset();

  /// set the urgency to the max of the current and the provided urgency
  void up_urgency_to(urgency_t u);

  /// access that part of the SchedTarget that is queued in the scrub queue
  const SchedEntry& queued_element() const { return sched_info; }

  bool is_deep() const { return sched_info.level == scrub_level_t::deep; }

  bool is_shallow() const { return sched_info.level == scrub_level_t::shallow; }

  scrub_level_t level() const { return sched_info.level; }

  urgency_t urgency() const { return sched_info.urgency; }

  /**
   * a loose definition of 'high priority' scrubs. Can only be used for
   * logs and user messages. Actual scheduling decisions should be based
   * on the 'urgency' attribute and its fine-grained characteristics.
   */
  bool is_high_priority() const
  {
    return urgency() != urgency_t::periodic_regular;
  }

  bool was_delayed() const { return sched_info.last_issue != delay_cause_t::none; }

  /// provides r/w access to the scheduling sub-object
  SchedEntry& sched_info_ref() { return sched_info; }
};



class ScrubJob {
 public:
  /// pg to be scrubbed
  spg_t pgid;

  /// the OSD id (for the log)
  int whoami;

  /*
   * the schedule for the next scrub at the specific level. Also - the
   * urgency and characteristics of the scrub (e.g. - high priority,
   * must-repair, ...)
   */
  SchedTarget shallow_target;
  SchedTarget deep_target;

  /**
   * Set whenever the PG scrubs are managed by the OSD (i.e. - from becoming
   * an active Primary till the end of the interval).
   */
  bool registered{false};

  /// how the last attempt to scrub this PG ended
  delay_cause_t last_issue{delay_cause_t::none};

  /**
   * the scrubber is waiting for locked objects to be unlocked.
   * Set after a grace period has passed.
   */
  bool blocked{false};
  utime_t blocked_since{};

  CephContext* cct;

  /// random generator for the randomization of the scrub times
  /// \todo consider using one common generator in the OSD service
  std::random_device random_dev;
  std::mt19937 random_gen;

  ScrubJob(CephContext* cct, const spg_t& pg, int node_id);

  /**
   * returns a possible reference to the earliest target that is eligible. If
   * both the shallow and the deep targets have their n.b. in the future,
   * nullopt is returned.
   */
  std::optional<std::reference_wrapper<SchedTarget>> earliest_eligible(
      utime_t scrub_clock_now);
  std::optional<std::reference_wrapper<const SchedTarget>> earliest_eligible(
      utime_t scrub_clock_now) const;

  /**
   * the target with the earliest 'not-before' time (i.e. - assuming
   * both targets are in the future).
   * \attn: might return the wrong answer if both targets are eligible.
   * If a need arises, a version that accepts the current time as a parameter
   * should be added. Then - a correct determination can be made for
   * all cases.
   */
  const SchedTarget& earliest_target() const;
  SchedTarget& earliest_target();

  /**
   * the target that will be scrubbed first. Basically - used
   * cmp_entries() to determine the order of the two targets.
   * Which means: if only one of the targets is eligible, it will be returned.
   * If both - the one with the highest priority -> level -> target time.
   * Otherwise - the one with the earliest not-before.
   */
  const SchedTarget& earliest_target(utime_t scrub_clock_now) const;
  SchedTarget& earliest_target(utime_t scrub_clock_now);

  /// the not-before of our earliest target (either shallow or deep)
  utime_t get_sched_time() const;

  std::string_view state_desc() const
  {
    return registered ? (is_queued() ? "queued" : "registered")
		      : "not-registered";
  }

  SchedTarget& get_target(scrub_level_t s_or_d);

  /**
   * Given a proposed time for the next scrub, and the relevant
   * configuration, adjust_schedule() determines the actual target time
   * and the 'not_before' time for the scrub.
   * The new values are updated into the scrub-job.
   *
   * Specifically:
   * - for high-priority scrubs: the 'not_before' is set to the
   *   (untouched) proposed target time.
   * - for regular scrubs: the proposed time is adjusted (delayed) based
   *   on the configuration; the n.b. is reset to the target.
   */
  void adjust_shallow_schedule(
    utime_t last_scrub,
    const Scrub::sched_conf_t& app_conf,
    utime_t scrub_clock_now);

  void adjust_deep_schedule(
    utime_t last_deep,
    const Scrub::sched_conf_t& app_conf,
    utime_t scrub_clock_now);

  /**
   * For the level specified, set the 'not-before' time to 'now+delay',
   * so that this scrub target would not be retried before the required
   * delay seconds have passed.
   * The delay is determined based on the 'cause' parameter.
   * The 'last_issue' is updated to the cause of the delay.
   * \returns a reference to the target that was modified.
   */
  [[maybe_unused]] SchedTarget& delay_on_failure(
      scrub_level_t level,
      delay_cause_t delay_cause,
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

  /**
   * the operator requested a scrub (shallow, deep or repair).
   * Set the selected target to the requested urgency, adjusting scheduling
   * parameters.
   */
  void operator_forced(scrub_level_t s_or_d, scrub_type_t scrub_type);

  /**
   * calculate a time offset large enough, so that once the relevant
   * last-scrub timestamp is forced back by this amount, the PG is
   * eligible for a periodic scrub of the specified level.
   * Used by the scrubber upon receiving a 'fake a scheduled scrub' request
   * from the operator.
   */
  double guaranteed_offset(
      scrub_level_t s_or_d,
      const Scrub::sched_conf_t& app_conf);

  void dump(ceph::Formatter* f) const;

  bool is_registered() const { return registered; }

  /// are any of our two SchedTargets queued in the scrub queue?
  bool is_queued() const;

  /// mark both targets as queued / not queued
  void clear_both_targets_queued();
  void set_both_targets_queued();

  /**
   * a text description of the "scheduling intentions" of this PG:
   * are we already scheduled for a scrub/deep scrub? when?
   */
  std::string scheduling_state(utime_t now_is) const;

  std::ostream& gen_prefix(std::ostream& out, std::string_view fn) const;
  std::string log_msg_prefix;

  // the comparison operator is used to sort the scrub jobs in the queue.
  // Note that it would not be needed in the next iteration of this code, as
  // the queue would *not* hold the full ScrubJob objects, but rather -
  // SchedTarget(s).
  std::partial_ordering operator<=>(const ScrubJob& rhs) const
  {
    return cmp_entries(
      ceph_clock_now(), shallow_target.queued_element(),
      deep_target.queued_element());
  };


 /*
 * Restrictions and limitations that apply to each urgency level:
 * -------------------------------------------------------------
 * Some types of scrubs are exempt from some or all of the preconditions and
 * limitations that apply to regular scrubs. The following table
 * details the specific set of exemptions per 'urgency' level:
 * (note: regular scrubs that are overdue are also allowed a specific
 * set of exemptions. Those will be covered elsewhere).
 *
 * The relevant limitations are:
 * - reservation: the scrub must reserve replicas;
 * - dow/time: the scrub must adhere to the allowed days-of-week/hours;
 * - ext-sleep: if initiated during allowed hours, the scrub is penalized
 *   if continued into the forbidden times, by having a longer sleep time;
 *   (note that this is only applicable to the wq scheduler).
 * - load: the scrub must not be initiated if the OSD is under heavy CPU load;
 * - noscrub: the scrub is aborted if the 'noscrub' flag (or the
 *  'nodeep-scrub' flag for deep scrubs) is set;
 * - randomization: the scrub's target time is extended by a random
 *   duration. This only applies to periodic scrubs.
 * - configuration changes: the target time may be modified following
 *   a change in the configuration. This only applies to periodic scrubs.
 * - max-scrubs: the scrub must not be initiated if the OSD is already
 *   scrubbing too many PGs (the 'osd_max_scrubs' limit).
 * - backoff: the scrub must not be initiated this tick if a dice roll
 *   failed.
 * - recovery: the scrub must not be initiated if the OSD is currently
 *   recovering PGs.
 *
 * The following table summarizes the limitations in effect per urgency level:
 *
 *  +------------+---------+--------------+---------+----------+-------------+
 *  | limitation |  must-  | after-repair |repairing| operator | must-repair |
 *  |            |  scrub  |(aft recovery)|(errors) | request  |             |
 *  +------------+---------+--------------+---------+----------+-------------+
 *  | reservation|    yes! |      no      |    no?  |     no   |      no     |
 *  | dow/time   |    yes  |     yes      |    no   |     no   |      no     |
 *  | ext-sleep  |    no   |      no      |    no   |     no   |      no     |
 *  | load       |    yes  |      no      |    no   |     no   |      no     |
 *  | noscrub    |    yes  |      no      |    Yes  |     no   |      no     |
 *  | max-scrubs |    yes  |      yes     |    Yes  |     no   |      no     |
 *  | backoff    |    yes  |      no      |    no   |     no   |      no     |
 *  | recovery   |    yes  |      yes     |    Yes  |     no   |      no     |
 *  +------------+---------+--------------+---------+----------+-------------+
 */

  // a set of helper functions for determining, for each urgency level, what
  // restrictions and limitations apply to that level.

  static bool observes_noscrub_flags(urgency_t urgency);

  static bool observes_allowed_hours(urgency_t urgency);

  static bool observes_extended_sleep(urgency_t urgency);

  static bool observes_load_limit(urgency_t urgency);

  static bool requires_reservation(urgency_t urgency);

  static bool requires_randomization(urgency_t urgency);

  static bool observes_max_concurrency(urgency_t urgency);

  static bool observes_random_backoff(urgency_t urgency);

  static bool observes_recovery(urgency_t urgency);

  // translating the 'urgency' into scrub behavior traits

  static bool has_high_queue_priority(urgency_t urgency);

  static bool is_repair_implied(urgency_t urgency);

  static bool is_autorepair_allowed(urgency_t urgency);
};
}  // namespace Scrub

namespace std {
std::ostream& operator<<(std::ostream& out, const Scrub::ScrubJob& pg);
}  // namespace std

namespace fmt {

template <>
struct formatter<Scrub::sched_params_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const Scrub::sched_params_t& pm, FormatContext& ctx) const
  {
    return fmt::format_to(
	ctx.out(), "proposed:{:s},must:{:c}", pm.proposed_time,
	pm.is_must == Scrub::must_scrub_t::mandatory ? 'y' : 'n');
  }
};

template <>
struct formatter<Scrub::SchedTarget> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const Scrub::SchedTarget& st, FormatContext& ctx) const
  {
     return fmt::format_to(
 	ctx.out(), "{},q:{:c},issue:{}", st.sched_info,
 	st.queued ? '+' : '-', st.sched_info.last_issue);
  }
};

template <>
struct formatter<Scrub::ScrubJob> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const Scrub::ScrubJob& sjob, FormatContext& ctx) const
  {
    return fmt::format_to(
	ctx.out(), "pg[{}]:sh:{}/dp:{}<{}>",
	sjob.pgid, sjob.shallow_target, sjob.deep_target, sjob.state_desc());
  }
};

template <>
struct formatter<Scrub::sched_conf_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const Scrub::sched_conf_t& cf, FormatContext& ctx) const
  {
    return fmt::format_to(
	ctx.out(),
	"periods:s:{},d:{},iv-ratio:{},deep-rand:{},on-inv:{}",
	cf.shallow_interval, cf.deep_interval,
	cf.interval_randomize_ratio, cf.deep_randomize_ratio,
	cf.mandatory_on_invalid);
  }
};
}  // namespace fmt
