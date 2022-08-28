// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once
// clang-format off
/*
┌───────────────────────┐
│ OSD                   │
│ OSDService           ─┼───┐
│                       │   │
│                       │   │
└───────────────────────┘   │   Ownes & uses the following
                            │   ScrubQueue interfaces:
                            │
                            │
                            │   - resource management (*1)
                            │
                            │   - environment conditions (*2)
                            │
                            │   - scrub scheduling (*3)
                            │
                            │
                            │
                            │
                            │
                            │
 ScrubQueue                 │
┌───────────────────────────▼────────────┐
│                                        │
│                                        │
│  ScrubQContainer    to_scrub <>────────┼────────┐
│  ScrubQContainer    penalized          │        │
│                                        │        │
│                                        │        │
│  OSD_wide resource counters            │        │
│                                        │        │
│                                        │        │
│  "env scrub conditions" monitoring     │        │
│                                        │        │
│                                        │        │
│                                        │        │
│                                        │        │
└─▲──────────────────────────────────────┘        │
  │                                               │
  │                                               │
  │uses interface <4>                             │
  │                                               │
  │                                               │
  │            ┌──────────────────────────────────┘
  │            │                 shared ownership of jobs
  │            │
  │      ┌─────▼──────┐
  │      │ScrubJob    │
  │      │            ├┐
  │      │            ││
  │      │            │┼┐
  │      │            │┼│
  └──────┤            │┼┤◄──────┐
         │            │┼│       │
         │            │┼│       │
         │            │┼│       │
         └┬───────────┼┼│       │shared ownership
          └─┼┼┼┼┼┼┼┼┼┼┼┼│       │
            └───────────┘       │
                                │
                                │
                                │
                                │
┌───────────────────────────────┼─┐
│                               <>│
│PgScrubber                       │
│                                 │
│                                 │
│                                 │
│                                 │
│                                 │
└─────────────────────────────────┘


SqrubQueue interfaces (main functions):

<1> - OSD/PG resources management:

  - can_inc_scrubs()
  - {inc/dec}_scrubs_{local/remote}()
  - dump_scrub_reservations()
  - {set/clear/is}_reserving_now()

<2> - environment conditions:

  - update_loadavg()

  - scrub_load_below_threshold()
  - scrub_time_permit()

<3> - scheduling scrubs:

  - select_pg_and_scrub()
  - dump_scrubs()

<4> - manipulating a job's state:

  - register_with_osd()
  - remove_from_osd_queue()
  - update_job()

 */
// clang-format on

#include <atomic>
#include <chrono>
#include <memory>
#include <optional>
#include <vector>

#include "common/RefCountedObj.h"
#include "common/ceph_atomic.h"
#include "osd/osd_types.h"
#include "osd/scrubber_common.h"
#include "include/utime_fmt.h"
#include "osd/osd_types_fmt.h"
#include "utime.h"

class PG;

namespace Scrub {

using namespace ::std::literals;

// possible outcome when trying to select a PG and scrub it
enum class schedule_result_t {
  scrub_initiated,     // successfully started a scrub
  none_ready,	       // no pg to scrub
  no_local_resources,  // failure to secure local OSD scrub resource
  already_started,     // failed, as already started scrubbing this pg
  no_such_pg,	       // can't find this pg
  bad_pg_state,	       // pg state (clean, active, etc.)
  preconditions	       // time, configuration, etc.
};

// the OSD services provided to the scrub scheduler
class ScrubSchedListener {
 public:
  virtual int get_nodeid() const = 0;  // returns the OSD number ('whoami')

  /**
   * A callback used by the ScrubQueue object to initiate a scrub on a specific
   * PG.
   *
   * The request might fail for multiple reasons, as ScrubQueue cannot by its
   * own check some of the PG-specific preconditions and those are checked here.
   * See attempt_t definition.
   *
   * @return a Scrub::attempt_t detailing either a success, or the failure
   * reason.
   */
  virtual schedule_result_t initiate_a_scrub(
    spg_t pgid,
    bool allow_requested_repair_only) = 0;

  virtual ~ScrubSchedListener() {}
};

}  // namespace Scrub

/**
 * the queue of PGs waiting to be scrubbed.
 * Main operations are scheduling/unscheduling a PG to be scrubbed at a certain
 * time.
 *
 * A "penalty" queue maintains those PGs that have failed to reserve the
 * resources of their replicas. The PGs in this list will be reinstated into the
 * scrub queue when all eligible PGs were already handled, or after a timeout
 * (or if their deadline has passed [[disabled at this time]]).
 */
class ScrubQueue {
 public:
  enum class must_scrub_t { not_mandatory, mandatory };

  enum class qu_state_t {
    not_registered,  // not a primary, thus not considered for scrubbing by this
		     // OSD (also the temporary state when just created)
    registered,	     // in either of the two queues ('to_scrub' or 'penalized')
    unregistering    // in the process of being unregistered. Will be finalized
		     // under lock
  };

  ScrubQueue(CephContext* cct, Scrub::ScrubSchedListener& osds);
  virtual ~ScrubQueue() = default;

  struct scrub_schedule_t {
    utime_t scheduled_at{};
    utime_t deadline{0, 0};
  };

  struct sched_params_t {
    utime_t proposed_time{};
    double min_interval{0.0};
    double max_interval{0.0};
    must_scrub_t is_must{ScrubQueue::must_scrub_t::not_mandatory};
  };

  struct ScrubJob final : public RefCountedObject {

    /**
     *  a time scheduled for scrub, and a deadline: The scrub could be delayed
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
     *  'updated' is a temporary flag, used to create a barrier after
     *  'sched_time' and 'deadline' (or any other job entry) were modified by
     *  different task.
     *  'updated' also signals the need to move a job back from the penalized
     *  queue to the regular one.
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

    /**
     * relatively low-cost(*) access to the scrub job's state, to be used in
     * logging.
     *  (*) not a low-cost access on x64 architecture
     */
    std::string_view state_desc() const
    {
      return ScrubQueue::qu_state_text(state.load(std::memory_order_relaxed));
    }

    void update_schedule(const ScrubQueue::scrub_schedule_t& adjusted);

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
     * a text description of the "scheduling intentions" of this PG:
     * are we already scheduled for a scrub/deep scrub? when?
     */
    std::string scheduling_state(utime_t now_is, bool is_deep_expected) const;

    friend std::ostream& operator<<(std::ostream& out, const ScrubJob& pg);
  };

  friend class TestOSDScrub;
  friend class ScrubSchedTestWrapper; ///< unit-tests structure

  using ScrubJobRef = ceph::ref_t<ScrubJob>;
  using ScrubQContainer = std::vector<ScrubJobRef>;

  static std::string_view qu_state_text(qu_state_t st);

  /**
   * called periodically by the OSD to select the first scrub-eligible PG
   * and scrub it.
   *
   * Selection is affected by:
   * - time of day: scheduled scrubbing might be configured to only happen
   *   during certain hours;
   * - same for days of the week, and for the system load;
   *
   * @param preconds: what types of scrub are allowed, given system status &
   *                  config. Some of the preconditions are calculated here.
   * @return Scrub::attempt_t::scrubbing if a scrub session was successfully
   *         initiated. Otherwise - the failure cause.
   *
   * locking: locks jobs_lock
   */
  Scrub::schedule_result_t select_pg_and_scrub(Scrub::ScrubPreconds& preconds);

  /**
   * Translate attempt_ values into readable text
   */
  static std::string_view attempt_res_text(Scrub::schedule_result_t v);

  /**
   * remove the pg from set of PGs to be scanned for scrubbing.
   * To be used if we are no longer the PG's primary, or if the PG is removed.
   */
  void remove_from_osd_queue(ScrubJobRef sjob);

  /**
   * @return the list (not std::set!) of all scrub jobs registered
   *   (apart from PGs in the process of being removed)
   */
  ScrubQContainer list_registered_jobs() const;

  /**
   * Add the scrub job to the list of jobs (i.e. list of PGs) to be periodically
   * scrubbed by the OSD.
   * The registration is active as long as the PG exists and the OSD is its
   * primary.
   *
   * See update_job() for the handling of the 'suggested' parameter.
   *
   * locking: might lock jobs_lock
   */
  void register_with_osd(ScrubJobRef sjob, const sched_params_t& suggested);

  /**
   * modify a scrub-job's schduled time and deadline
   *
   * There are 3 argument combinations to consider:
   * - 'must' is asserted, and the suggested time is 'scrub_must_stamp':
   *   the registration will be with "beginning of time" target, making the
   *   scrub-job eligible to immediate scrub (given that external conditions
   *   do not prevent scrubbing)
   *
   * - 'must' is asserted, and the suggested time is 'now':
   *   This happens if our stats are unknown. The results are similar to the
   *   previous scenario.
   *
   * - not a 'must': we take the suggested time as a basis, and add to it some
   *   configuration / random delays.
   *
   *  ('must' is sched_params_t.is_must)
   *
   *  locking: not using the jobs_lock
   */
  void update_job(ScrubJobRef sjob, const sched_params_t& suggested);

  sched_params_t determine_scrub_time(const requested_scrub_t& request_flags,
				      const pg_info_t& pg_info,
				      const pool_opts_t& pool_conf) const;

 public:
  void dump_scrubs(ceph::Formatter* f) const;

  /**
   * No new scrub session will start while a scrub was initiated on a PG,
   * and that PG is trying to acquire replica resources.
   */
  void set_reserving_now() { a_pg_is_reserving = true; }
  void clear_reserving_now() { a_pg_is_reserving = false; }
  bool is_reserving_now() const { return a_pg_is_reserving; }

  bool can_inc_scrubs() const;
  bool inc_scrubs_local();
  void dec_scrubs_local();
  bool inc_scrubs_remote();
  void dec_scrubs_remote();
  void dump_scrub_reservations(ceph::Formatter* f) const;

  /// counting the number of PGs stuck while scrubbing, waiting for objects
  void mark_pg_scrub_blocked(spg_t blocked_pg);
  void clear_pg_scrub_blocked(spg_t blocked_pg);
  int get_blocked_pgs_count() const;

  /**
   * Pacing the scrub operation by inserting delays (mostly between chunks)
   *
   * Special handling for regular scrubs that continued into "no scrub" times.
   * Scrubbing will continue, but the delays will be controlled by a separate
   * (read - with higher value) configuration element
   * (osd_scrub_extended_sleep).
   */
  double scrub_sleep_time(bool must_scrub) const;  /// \todo (future) return
						   /// milliseconds

  /**
   *  called every heartbeat to update the "daily" load average
   *
   *  @returns a load value for the logger
   */
  [[nodiscard]] std::optional<double> update_load_average();

 private:
  CephContext* cct;
  Scrub::ScrubSchedListener& osd_service;

#ifdef WITH_SEASTAR
  auto& conf() const { return local_conf(); }
#else
  auto& conf() const { return cct->_conf; }
#endif

  /**
   *  jobs_lock protects the job containers and the relevant scrub-jobs state
   *  variables. Specifically, the following are guaranteed:
   *  - 'in_queues' is asserted only if the job is in one of the queues;
   *  - a job will only be in state 'registered' if in one of the queues;
   *  - no job will be in the two queues simultaneously;
   *
   *  Note that PG locks should not be acquired while holding jobs_lock.
   */
  mutable ceph::mutex jobs_lock = ceph::make_mutex("ScrubQueue::jobs_lock");

  ScrubQContainer to_scrub;   ///< scrub jobs (i.e. PGs) to scrub
  ScrubQContainer penalized;  ///< those that failed to reserve remote resources
  bool restore_penalized{false};

  double daily_loadavg{0.0};

  static inline constexpr auto registered_job = [](const auto& jobref) -> bool {
    return jobref->state == qu_state_t::registered;
  };

  static inline constexpr auto invalid_state = [](const auto& jobref) -> bool {
    return jobref->state == qu_state_t::not_registered;
  };

  /**
   * Are there scrub jobs that should be reinstated?
   */
  void scan_penalized(bool forgive_all, utime_t time_now);

  /**
   * clear dead entries (unregistered, or belonging to removed PGs) from a
   * queue. Job state is changed to match new status.
   */
  void rm_unregistered_jobs(ScrubQContainer& group);

  /**
   * the set of all scrub jobs in 'group' which are ready to be scrubbed
   * (ready = their scheduled time has passed).
   * The scrub jobs in the new collection are sorted according to
   * their scheduled time.
   *
   * Note that the returned container holds independent refs to the
   * scrub jobs.
   */
  ScrubQContainer collect_ripe_jobs(ScrubQContainer& group, utime_t time_now);


  /// scrub resources management lock (guarding scrubs_local & scrubs_remote)
  mutable ceph::mutex resource_lock =
    ceph::make_mutex("ScrubQueue::resource_lock");

  // the counters used to manage scrub activity parallelism:
  int scrubs_local{0};
  int scrubs_remote{0};

  /**
   * The scrubbing of PGs might be delayed if the scrubbed chunk of objects is
   * locked by some other operation. A bug might cause this to be an infinite
   * delay. If that happens, the OSDs "scrub resources" (i.e. the
   * counters that limit the number of concurrent scrub operations) might
   * be exhausted.
   * We do issue a cluster-log warning in such occasions, but that message is
   * easy to miss. The 'some pg is blocked' global flag is used to note the
   * existence of such a situation in the scrub-queue log messages.
   */
  std::atomic_int_fast16_t blocked_scrubs_cnt{0};

  std::atomic_bool a_pg_is_reserving{false};

  [[nodiscard]] bool scrub_load_below_threshold() const;
  [[nodiscard]] bool scrub_time_permit(utime_t now) const;

  /**
   * If the scrub job was not explicitly requested, we postpone it by some
   * random length of time.
   * And if delaying the scrub - we calculate, based on pool parameters, a
   * deadline we should scrub before.
   *
   * @return a pair of values: the determined scrub time, and the deadline
   */
  scrub_schedule_t adjust_target_time(
    const sched_params_t& recomputed_params) const;

  /**
   * Look for scrub jobs that have their 'resources_failure' set. These jobs
   * have failed to acquire remote resources last time we've initiated a scrub
   * session on them. They are now moved from the 'to_scrub' queue to the
   * 'penalized' set.
   *
   * locking: called with job_lock held
   */
  void move_failed_pgs(utime_t now_is);

  Scrub::schedule_result_t select_from_group(
    ScrubQContainer& group,
    const Scrub::ScrubPreconds& preconds,
    utime_t now_is);

protected: // used by the unit-tests
  /**
   * unit-tests will override this function to return a mock time
   */
  virtual utime_t time_now() const { return ceph_clock_now(); }
};

template <>
struct fmt::formatter<ScrubQueue::qu_state_t>
    : fmt::formatter<std::string_view> {
  template <typename FormatContext>
  auto format(const ScrubQueue::qu_state_t& s, FormatContext& ctx)
  {
    auto out = ctx.out();
    out = fmt::formatter<string_view>::format(
      std::string{ScrubQueue::qu_state_text(s)}, ctx);
    return out;
  }
};

template <>
struct fmt::formatter<ScrubQueue::ScrubJob> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const ScrubQueue::ScrubJob& sjob, FormatContext& ctx)
  {
    return fmt::format_to(
      ctx.out(),
      "pg[{}] @ {:s} (dl:{:s}) - <{}> / failure: {} / pen. t.o.: {:s} / queue "
      "state: {:.7}",
      sjob.pgid, sjob.schedule.scheduled_at, sjob.schedule.deadline,
      sjob.registration_state(), sjob.resources_failure, sjob.penalty_timeout,
      sjob.state.load(std::memory_order_relaxed));
  }
};
