// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once
// clang-format off
/*

        PgScrubber
        ┌───────────────────────────────────┐
        │       ScrubJob                    │
        │    ┌──────────────────────────────┤
        │    │          S target            │
        │    │        ┌────────────────────┐│
        │    │        │                    ││
        │    │        │     ┌───────────── ││
        │    │        │     │ sched info   ││
        │    │        │     │              ││
        │    │        └─────┴──────────────┘│
        │    │                              │
        │    │          D target            │
        │    │        ┌────────────────────┐│
        │    │        │                    ││
        │    │        │     ┌───────────── ││
        │    │        │     │ sched info   ││
        │    │        │     │              ││
        │    │        └─────┴──────────────┘│
        │    │                              │
        └────┴──────────────────────────────┘

 (some of the possible) Scenarios:

   * PG instance becomes Primary:
     - S & D targets are initialized based on configuration & PG info;
     - copies of their 'sched info' sub-objects are pushed into the scrub queue;

   * A specific SchedEntry (the sched-info object as appearing the scrub queue)
     is selected for scrubbing:
     - the entry is deleted from the queue;
     - the PgScrubber performs some preliminary checks;
       - failing those - the entry is updated (pushed backwards in time) and
         returned to the queue;
     - a scrub session is initiated;
     - the 2'nd queue entry corresponding to this PG is also removed from the
        queue;
     - the ScrubJob's S & D targets are "decoupled" from the OSD's scrub queue,
       and can be updated, if needed(*), with the new scheduling information;
       (*) mostly following operator requests

   * A scrub session is completed:
     - 'last-' timestamps are updated;
     - both targets are updated, combining possible new scheduling information
       in them with the schedule suggested by the new timestamps;
     - the sched-info objects are pushed to the queue;
 */
// clang-format on

#include <atomic>
#include <chrono>
#include <compare>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "common/ceph_atomic.h"
#include "include/utime_fmt.h"
#include "osd/osd_types.h"
#include "osd/osd_types_fmt.h"
#include "osd/scrubber_common.h"

#include "utime.h"

class PG;
class PgScrubber;
class OSDService;
template <>
struct fmt::formatter<Scrub::SchedTarget>;

namespace Scrub {

using namespace ::std::literals;

/**
 * Possible urgency levels for a specific scheduling target (shallow or deep):
 *
 * 'off' - the target is not scheduled for scrubbing. This is the initial state,
 * 	and is also the state of targets for a PG that cannot be scheduled to be
 *      scrubbed by this OSD (e.g. not a primary or not active).
 *
 * periodic scrubs:
 * ---------------
 *
 * 'periodic_regular' - the "standard" shallow/deep scrub performed
 *      periodically on each PG.
 *
 * 'overdue' - the target is eligible for periodic scrubbing, but has not been
 *      scrubbed for a while, and the time now is past its deadline.
 *      Overdue scrubs are allowed to run even if the OSD is overloaded, and in
 *      the wrong time or day.
 *      Also - their target time is not modified upon a configuration change.
 *
 *
 * priority scrubs (termed 'required' or 'must' in the legacy code):
 * ---------------------------------------------------------------
 * Priority scrubs:
 * - are not subject to times/days/load limitations;
 * - cannot be aborted by 'noscrub'/'nodeep-scrub' flags;
 * - do not have random delays added to their target time;
 * - never have their target time modified by a configuration change;
 * - never subject to 'extended sleep time' (see scrub_sleep_time());
 *
 * 'operator_requested' - the target was manually requested for scrubbing by
 *      an administrator.
 *
 * 'must' - the target is required to be scrubbed, as:
 *      - the scrub was initiated by a message specifying 'do_repair'; or
 *      - the PG info is not valid (i.e. we do not have a valid 'last-scrub' stamp)
 *   or - a deep scrub is required after the previous scrub ended with errors.
 *      'must' scrubs are similar to 'operator_requested', but have a higher
 *      priority (and have a repair flag set).
 *
 * 'after_repair' - triggered immediately after a recovery process
 *      ('m_after_repair_scrub_required' was set). The highest urgency assigned
 *      in this case assure we are not racing against any other type of scrub
 *      (and helps clarifying the PG/scrub status in the logs).
 *      This type of scrub is always deep.
 */
enum class urgency_t {
  off,
  periodic_regular,
  overdue,
  operator_requested,
  must,
  after_repair,
};

/**
 * the result of the last attempt to schedule a scrub for a specific PG.
 * The enum value itself is mostly used for logging purposes.
 * For a discussion of the handling of scrub initiation issues and scrub
 * aborts - see for example ScrubJob::delay_on*() & ScrubJob::on_abort()
 */
enum class delay_cause_t {
  none,		    ///< scrub attempt was successful
  replicas,	    ///< failed to reserve replicas
  flags,	    ///< noscrub or nodeep-scrub
  pg_state,	    ///< e.g. snap-trimming
  time,		    ///< time restrictions or busy CPU
  local_resources,  ///< too many scrubbing PGs
  aborted,	    ///< scrub was aborted on no(deep)-scrub
  backend_error,    ///< data access failure reported by the backend
  interval,          ///< the interval had ended mid-scrub
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

/**
 * SchedEntry holds the scheduling details for scrubbing a specific PG at
 * a specific scrub level. Namely - it identifies the pg X level combination,
 * the 'urgency' attribute of the scheduled scrub (which determines most of
 * its behavior and scheduling decisions) and the actual time attributes
 * for scheduling (target, deadline, not_before).
 */
struct SchedEntry {
  constexpr SchedEntry(spg_t pgid, scrub_level_t level)
      : pgid{pgid}
      , level{level}
  {}

  SchedEntry(const SchedEntry&) = default;
  SchedEntry(SchedEntry&&) = default;
  SchedEntry& operator=(const SchedEntry&) = default;
  SchedEntry& operator=(SchedEntry&&) = default;

  spg_t pgid;
  scrub_level_t level;

  urgency_t urgency{urgency_t::off};

  /**
   * the time at which we are allowed to start the scrub. Never
   * decreasing after 'target' is set.
   */
  utime_t not_before{utime_t::max()};

  /**
   * the 'deadline' is the time by which we expect the periodic scrub to
   * complete. It is determined by the SCRUB_MAX_INTERVAL pool configuration
   * and by osd_scrub_max_interval;
   * Once passed, the scrub will be allowed to run even if the OSD is overloaded
   * or during no-scrub hours.
   */
  utime_t deadline{utime_t::max()};

  /**
   * the 'target' is the time at which we intended the scrub to be scheduled.
   * For periodic (regular) scrubs, it is set to the time of the last scrub
   * plus the scrub interval (plus some randomization). Priority scrubs
   * have their own specific rules for the target time:
   * - for operator-initiated scrubs: 'target' is set to 'now';
   * - same for re-scrubbing (deep scrub after a shallow scrub that ended with
   *   errors;
   * - when requesting a scrub after a repair (the highest priority scrub):
   *   the target is set to '0' (beginning of time);
   */
  utime_t target{utime_t::max()};

  /**
   * a SchedEntry is 'ripe' for scrubbing if the current time is past its
   * 'not_before' time (which guarantees it is also past its 'target').
   * It also must not be 'inactive'. i.e. must not have urgency 'off'.
   */
  bool is_ripe(utime_t now_is) const;

  void dump(std::string_view sect_name, ceph::Formatter* f) const;
};

std::weak_ordering cmp_ripe_entries(
    const Scrub::SchedEntry& l,
    const Scrub::SchedEntry& r);

std::weak_ordering cmp_future_entries(
    const Scrub::SchedEntry& l,
    const Scrub::SchedEntry& r);


// the interface required by 'not_before_queue_t':

const utime_t& project_not_before(const Scrub::SchedEntry&);
const spg_t& project_removal_class(const Scrub::SchedEntry&);

/**
 *  SchedTarget is a wrapper around SchedEntry, adding those attributes
 *  and methods that are relevant to the PgScrubber (through its ScrubJob).
 *  Note: a ScrubJob holds two SchedTarget objects, one for each scrub level.
 */
class SchedTarget {
 public:
  friend class ScrubJob;
  friend struct fmt::formatter<Scrub::SchedTarget>;

  SchedTarget(
      spg_t pg_id,
      scrub_level_t scrub_level,
      int osd_num,
      CephContext* cct);

  std::ostream& gen_prefix(std::ostream& out) const;

  utime_t sched_time() const;

  /// access that part of the SchedTarget that is queued in the scrub queue
  const SchedEntry& queued_element() const { return sched_info; }

  bool is_deep() const { return sched_info.level == scrub_level_t::deep; }

  bool is_shallow() const { return sched_info.level == scrub_level_t::shallow; }

  scrub_level_t level() const { return sched_info.level; }

  /**
   * periodic scrubs are those with urgency of either periodic_regular or
   * overdue
   */
  bool is_periodic() const;

  /// 'required' is the legacy term for high-priority (non-periodic) scrubs
  bool is_required() const { return sched_info.urgency > urgency_t::overdue; }

  /**
   * urgency==off is only expected for SchedTarget objects belonging to
   * PGs that are not eligible for scrubbing (not Primaries, not clean, not
   * active)
   */
  bool is_off() const { return sched_info.urgency == urgency_t::off; }

  bool is_queued() const { return in_queue; }

  delay_cause_t delay_cause() const { return last_issue; }

  bool was_delayed() const { return last_issue != delay_cause_t::none; }

  /**
   * a SchedTarget is 'ripe' for scrubbing if the current time is past its
   * 'not_before' time (which guarantees it is also past its 'target').
   * And - it must not be 'inactive'. i.e. must not have urgency 'off'.
   */
  bool is_ripe(utime_t now_is) const { return sched_info.is_ripe(now_is); }

  bool over_deadline(utime_t now_is) const;

  urgency_t urgency() const { return sched_info.urgency; }

  // following scrub-initiation failures:

  /// sets 'not-before' to 'now+delay'; updates 'last_issue'
  void push_nb_out(
      std::chrono::seconds delay,
      delay_cause_t delay_cause,
      utime_t scrub_clock_now);

  /// push_nb_out() w/ delay=osd_scrub_retry_pg_state
  void delay_on_pg_state(utime_t scrub_clock_now);

  /// push_nb_out() w/ delay=osd_scrub_retry_delay
  void delay_on_level_not_allowed(utime_t scrub_clock_now);

  /// push_nb_out() w/ delay=osd_scrub_retry_wrong_time
  void delay_on_wrong_time(utime_t scrub_clock_now);

  /// push_nb_out() w/ delay=osd_scrub_retry_delay
  void delay_on_deep_errors(utime_t scrub_clock_now);

  void dump(std::string_view sect_name, ceph::Formatter* f) const;

  void clear_queued() { in_queue = false; }
  void set_queued() { in_queue = true; }

  // scrub flags
  bool get_auto_repair() const { return auto_repairing; }
  bool get_do_repair() const { return do_repair; }

 private:
  /// our ID and scheduling parameters
  SchedEntry sched_info;

  /**
   * is this target (meaning - a copy of his specific combination of
   * PG and scrub type) currently in the queue?
   */
  bool in_queue{false};

  /// the reason for the latest failure/delay (for logging/reporting purposes)
  delay_cause_t last_issue{delay_cause_t::none};

  // the flags affecting the scrub that will result from this target

  /**
   * (deep-scrub entries only:)
   * Supporting the equivalent of 'need-auto', which translated into:
   * - performing a deep scrub (taken care of by raising the priority of the
   *   deep target);
   * - marking that scrub as 'do_repair' (the next flag here);
   */
  bool auto_repairing{false};

  /**
   * (deep-scrub entries only:)
   * Set for scrub_requested() scrubs with the 'repair' flag set.
   * Translated (in set_op_parameters()) into a deep scrub with
   * m_is_repair & PG_REPAIR_SCRUB.
   */
  bool do_repair{false};

  // -----       logging support

  CephContext* cct;

  int whoami;  ///< the OSD id

 private:
  /// resets to the after-construction state
  void reset();

  void disable() { sched_info.urgency = urgency_t::off; }

  void set_oper_deep_target(scrub_type_t rpr, utime_t scrub_clock_now);
  void set_oper_shallow_target(scrub_type_t rpr, utime_t scrub_clock_now);

  void up_urgency_to(urgency_t u);

  // updating periodic targets:

  void update_as_shallow(
      const pg_info_t& info,
      const sched_conf_t& aconf,
      utime_t now_is);

  void update_as_deep(
      const pg_info_t& info,
      const sched_conf_t& aconf,
      utime_t now_is);

  std::string m_log_prefix;
};

// Queue-manipulation by a PG:
struct ScrubQueueOps;


// ////////////////////////////////////////////////////////////////////////// //
// ScrubJob -- scrub scheduling & parameters for a specific PG (PgScrubber)

class ScrubJob {
 public:
  ScrubJob(
      ScrubQueueOps& osd_queue,
      CephContext* cct,
      const spg_t& pg,
      int node_id);

  /// switch between the two scrub types
  static scrub_level_t the_other_level(scrub_level_t l);

  /// RRR was: dequeue and disable both shallow and deep targets
  /// dequeue both shallow and deep targets
  void remove_from_osd_queue();

  /// disable both shallow and deep targets
  /// RRR add comment explaining the 'why'
  void reset_schedule();

  /// push a target back to the queue (after having it modified)
  void requeue_entry(scrub_level_t tid);

  /**
   * returns a copy of the named target, and resets the 'left behind'
   * copy (which is either 'shallow_target' or 'deep_target')
   */
  SchedTarget get_moved_target(scrub_level_t s_or_d);

  void dequeue_entry(scrub_level_t s_or_d);

  bool in_queue() const;

  void on_abort(SchedTarget&& aborted_target, delay_cause_t issue, utime_t now_is);

  void on_reservation_failure(SchedTarget&& aborted_target, utime_t now_is);

  void mark_for_after_repair();

  SchedTarget& closest_target(utime_t scrub_clock_now);

  const SchedTarget& closest_target(utime_t scrub_clock_now) const;

  // return a concise description of the scheduling state of this PG
  pg_scrubbing_status_t get_schedule(utime_t now_is) const;


 public:
  /// pg to be scrubbed
  spg_t pgid;

  /// the OSD id (for the log)
  int whoami;

  CephContext* cct;

  ScrubQueueOps& scrub_queue;

  SchedTarget shallow_target;
  SchedTarget deep_target;

  SchedTarget& get_target(scrub_level_t lvl);

  /**
   * this PG (this ScrubJob) is being scrubbed now.
   * Set to true immediately after set_op_parameters() committed us
   * to a scrub.
   */
  bool scrubbing{false};

  // failures/issues/aborts-related information

  /**
   * the scrubber is waiting for locked objects to be unlocked.
   * Set after a grace period has passed.
   */
  bool blocked{false};
  utime_t blocked_since{};

  /**
   * the more consecutive failures - the longer we will delay before
   * retrying the scrub job
   */
  int consec_aborts{0};

  utime_t get_sched_time(utime_t scrub_clock_now) const;

  /**
   * the operator faked the timestamp. Reschedule the
   * relevant target.
   */
  void operator_periodic_targets(
      scrub_level_t level,
      utime_t upd_stamp,
      const pg_info_t& pg_info,
      const sched_conf_t& sched_configs,
      utime_t now_is);

  /**
   * the operator instructed us to scrub. The urgency is set to (at least)
   * 'operator_requested', or (if the request is for a repair-scrub) - to
   * 'must'
   */
  void operator_forced_targets(
      scrub_level_t level,
      scrub_type_t scrub_type,
      utime_t now_is);

  // deep scrub is marked for the next scrub cycle for this PG
  // The equivalent of must_scrub & must_deep_scrub
  void mark_for_rescrubbing();

  void init_and_queue_targets(
      const pg_info_t& info,
      const sched_conf_t& aconf,
      utime_t now_is);

  void at_scrub_completion(
      const pg_info_t& info,
      const sched_conf_t& aconf,
      utime_t now_is);

  /**
   * Following a change in the 'scrub period' parameters -
   * recomputing the targets.
   */
  void on_periods_change(
      const pg_info_t& info,
      const sched_conf_t& aconf,
      utime_t now_is);

  void merge_active_back(SchedTarget&& aborted_target,
    delay_cause_t issue,
    utime_t now_is);

  void dump(ceph::Formatter* f) const;

  std::string_view registration_state() const;

  /**
   * a text description of the "scheduling intentions" of this PG:
   * are we already scheduled for a scrub/deep scrub? when?
   */
  std::string scheduling_state() const;

  friend std::ostream& operator<<(std::ostream& out, const ScrubJob& pg);
  std::ostream& gen_prefix(std::ostream& out) const;
  std::string m_log_msg_prefix;

 protected:  // made public in u-tests

  /**
   * dequeues both targets, marking them as 'not in queue'.
   * returns the number of targets that were previously marked as "in queue".
   */
  int dequeue_targets();

  /**
   * dequeues the named target, marking it as 'not in queue'.
   * returns a ref to the modified target.
   */
  SchedTarget& dequeue_target(scrub_level_t lvl);


  /// giving a proper name to an internal flag affecting
  /// merge_delay_requeue() operation
  enum class delay_both_targets_t { no, yes };

  void merge_delay_requeue(SchedTarget&& aborted_target,
    delay_cause_t issue,
    std::chrono::seconds delay,
    delay_both_targets_t delay_both_targets,
    utime_t now_is);

  void merge_and_delay(SchedTarget&& aborted_target,
    delay_cause_t issue,
    std::chrono::seconds delay,
    delay_both_targets_t delay_both_targets,
    utime_t now_is);
};
}  // namespace Scrub


class PGLockWrapper;

namespace Scrub {

class ScrubSchedListener {
 public:
  virtual int get_nodeid() const = 0;  // returns the OSD number ('whoami')

  virtual std::optional<PGLockWrapper> get_locked_pg(spg_t pgid) = 0;

  virtual void queue_for_scrub_initiation(
      spg_t pg,
      scrub_level_t scrub_level,
      utime_t loop_id,
      Scrub::OSDRestrictions env_conditions) = 0;

  virtual ~ScrubSchedListener() {}
};

}  // namespace Scrub

// clang-format off
template <>
struct fmt::formatter<Scrub::urgency_t>
    : fmt::formatter<std::string_view> {
  template <typename FormatContext>
  auto format(Scrub::urgency_t urg, FormatContext& ctx)
  {
    using enum Scrub::urgency_t;
    std::string_view desc;
    switch (urg) {
      case after_repair:        desc = "after-repair"; break;
      case must:                desc = "must"; break;
      case operator_requested:  desc = "operator-requested"; break;
      case overdue:             desc = "overdue"; break;
      case periodic_regular:    desc = "periodic-regular"; break;
      case off:                 desc = "off"; break;
      // better to not have a default case, so that the compiler will warn
    }
    return formatter<string_view>::format(desc, ctx);
  }
};
// clang-format on

// clang-format off
template <>
struct fmt::formatter<Scrub::delay_cause_t> : fmt::formatter<std::string_view> {
  template <typename FormatContext>
  auto format(Scrub::delay_cause_t cause, FormatContext& ctx)
  {
    using enum Scrub::delay_cause_t;
    std::string_view desc;
    switch (cause) {
      case none:        desc = "ok"; break;
      case replicas:    desc = "replicas"; break;
      case flags:       desc = "flags"; break;	 // no-scrub etc'
      case pg_state:    desc = "pg-state"; break;
      case time:        desc = "time"; break;
      case local_resources: desc = "local-cnt"; break;
      case aborted:     desc = "aborted"; break;
      case backend_error: desc = "backend-error"; break;
      case interval:    desc = "interval"; break;
      // better to not have a default case, so that the compiler will warn
    }
    return formatter<string_view>::format(desc, ctx);
  }
};
// clang-format on

template <>
struct fmt::formatter<Scrub::SchedEntry> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const Scrub::SchedEntry& st, FormatContext& ctx)
  {
    return format_to(
	ctx.out(), "{}/{},nb:{:s},({},tr:{:s},dl:{:s})", st.pgid,
	(st.level == scrub_level_t::deep ? "dp" : "sh"), st.not_before,
	st.urgency, st.target, st.deadline);
  }
};

template <>
struct fmt::formatter<Scrub::SchedTarget> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const Scrub::SchedTarget& st, FormatContext& ctx)
  {
    return format_to(
	ctx.out(), "{},q:{},ar:{},issue:{}", st.sched_info,
	st.in_queue ? "+" : "-", st.auto_repairing ? "+" : "-", st.last_issue);
  }
};

template <>
struct fmt::formatter<Scrub::ScrubJob> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx)
  {
    auto it = ctx.begin();
    if (it != ctx.end() && *it == 's') {
      shortened = true;
      ++it;
    }
    return it;
  }

  template <typename FormatContext>
  auto format(const Scrub::ScrubJob& sjob, FormatContext& ctx)
  {
    if (shortened) {
      return fmt::format_to(
	  ctx.out(), "pg[{}]:reg:{}", sjob.pgid, sjob.registration_state());
    }
    return fmt::format_to(
	ctx.out(), "pg[{}]:[t/s:{},t/d:{}],reg:{}", sjob.pgid,
	sjob.shallow_target, sjob.deep_target, sjob.registration_state());
  }
  bool shortened{false};  ///< true = no 'nearest target' info
};

template <>
struct fmt::formatter<Scrub::sched_conf_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const Scrub::sched_conf_t& cf, FormatContext& ctx)
  {
    return format_to(
	ctx.out(),
	"periods: s:{}/{} d:{}/{} iv-ratio:{} deep-rand:{} on-inv:{}",
	cf.shallow_interval, cf.max_shallow.value_or(-1.0), cf.deep_interval,
	cf.max_deep, cf.interval_randomize_ratio, cf.deep_randomize_ratio,
	cf.mandatory_on_invalid);
  }
};
