// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once
#include <string_view>

#include "osd/osd_types_fmt.h"
#include "osd/osd_perf_counters.h"
#include "osd/scrubber/osd_scrub_sched.h"
#include "osd/scrubber/scrub_resources.h"
#include "osd/scrubber_common.h"

/**
 *  Off-loading scrubbing initiation logic from the OSD.
 *  Also here: CPU load as pertaining to scrubs (TBD), and the scrub
 *  resource counters.
 *
 *  Locking:
 *  (as of this first step in the scheduler refactoring)
 *  - No protected data is maintained directly by the OsdScrub object
 *    (as it is not yet protected by any single OSDservice lock).
 */
class OsdScrub {
 public:
  OsdScrub(
      CephContext* cct,
      Scrub::ScrubSchedListener& osd_svc,
      const ceph::common::ConfigProxy& config);

  ~OsdScrub();

  // note: public, as accessed by the dout macros
  std::ostream& gen_prefix(std::ostream& out, std::string_view fn) const;

  /**
   * called periodically by the OSD to select the first scrub-eligible PG
   * and scrub it.
   */
  void initiate_scrub(bool active_recovery);

  /**
   * logs a string at log level 20, using OsdScrub's prefix.
   * An aux function to be used by sub-objects.
   */
  void log_fwd(std::string_view text);

  const Scrub::ScrubResources& resource_bookkeeper() const
  {
    return m_resource_bookkeeper;
  }

  void dump_scrubs(ceph::Formatter* f) const;  ///< fwd to the queue

  void dump_scrub_reservations(ceph::Formatter* f) const;

  /**
   * on_config_change() (the refactored "OSD::sched_all_scrubs()")
   *
   * for each PG registered with the OSD (i.e. - for which we are the primary):
   * lock that PG, and call its on_scrub_schedule_input_change() method
   * to handle a possible change in one of the configuration parameters
   * that affect scrub scheduling.
   */
  void on_config_change();


  // implementing the PGs interface to the scrub scheduling objects
  // ---------------------------------------------------------------

  // updating the resource counters
  std::unique_ptr<Scrub::LocalResourceWrapper> inc_scrubs_local(
      bool is_high_priority);
  void dec_scrubs_local();

  // counting the number of PGs stuck while scrubbing, waiting for objects
  void mark_pg_scrub_blocked(spg_t blocked_pg);
  void clear_pg_scrub_blocked(spg_t blocked_pg);

  /**
   * Add the scrub job to the list of jobs (i.e. list of PGs) to be periodically
   * scrubbed by the OSD.
   */
  void enqueue_scrub_job(const Scrub::ScrubJob& sjob);

  /**
   * copy the scheduling element (the SchedEntry sub-object) part of
   * the SchedTarget to the queue.
   */
  void enqueue_target(const Scrub::SchedTarget& trgt);

  /**
   * remove the specified scheduling target from the OSD scrub queue
   */
  void dequeue_target(spg_t pgid, scrub_level_t s_or_d);

  /**
   * remove the pg from set of PGs to be scanned for scrubbing.
   * To be used if we are no longer the PG's primary, or if the PG is removed.
   */
  void remove_from_osd_queue(spg_t pgid);

  /**
   * \returns std::chrono::milliseconds indicating how long to wait between
   * chunks.
   *
   * Implementation Note: Returned value is either osd_scrub_sleep or
   * osd_scrub_extended_sleep:
   * - if scrubs are allowed at this point in time - osd_scrub_sleep; otherwise
   *   (i.e. - the current time is outside of the allowed scrubbing hours/days,
   *   but the scrub started earlier):
   * - if the scrub observes "extended sleep" (i.e. - it's a low urgency
   *   scrub) - osd_scrub_extended_sleep.
   */
  std::chrono::milliseconds scrub_sleep_time(
      utime_t t_now,
      bool scrub_respects_ext_sleep) const;


  /**
   * \returns true if the current time is within the scrub time window
   */
  [[nodiscard]] bool scrub_time_permit(utime_t t) const;

  /**
   * Fetch the 1-minute load average. Used by
   * the OSD heartbeat handler to update a performance counter.
   * Also updates the number of CPUs, required internally by the
   * scrub queue.
   *
   * \returns the 1-minute element of getloadavg() or nullopt
   *          if the load is not available.
   */
  std::optional<double> update_load_average();

   // the scrub performance counters collections
   // ---------------------------------------------------------------
  PerfCounters* get_perf_counters(int pool_type, scrub_level_t level);

 private:
  CephContext* cct;
  Scrub::ScrubSchedListener& m_osd_svc;
  const ceph::common::ConfigProxy& conf;

  /**
   * check the OSD-wide environment conditions (scrub resources, time, etc.).
   * These may restrict the type of scrubs we are allowed to start, maybe
   * down to allowing only high-priority scrubs. See comments in scrub_job.h
   * detailing which condiitions may prevent what types of scrubs.
   *
   * The following possible limiting conditions are checked:
   * - high local OSD concurrency (i.e. too many scrubs on this OSD);
   * - a "dice roll" says we will not scrub in this tick (note: this
   *   specific condition is only checked if the "high concurrency" condition
   *   above is not detected);
   * - the CPU load is high (i.e. above osd_scrub_cpu_load_threshold);
   * - the OSD is performing a recovery & osd_scrub_during_recovery is 'false';
   * - the current time is outside of the allowed scrubbing hours/days
   */
  Scrub::OSDRestrictions restrictions_on_scrubbing(
      bool is_recovery_active,
      utime_t scrub_clock_now) const;

  static bool is_sched_target_eligible(
      const Scrub::SchedEntry& e,
      const Scrub::OSDRestrictions& r,
      utime_t time_now);

  /**
   * initiate a scrub on a specific PG
   * The PG is locked, enabling us to query its state. Specifically, we
   * verify that the PG is not already scrubbing, and that
   * a possible 'allow requested repair only' condition is not in conflict.
   *
   * \returns a schedule_result_t object, indicating whether the scrub was
   *          initiated, and if not - why.
   */
  Scrub::schedule_result_t initiate_a_scrub(
      const Scrub::SchedEntry& candidate,
      Scrub::OSDRestrictions restrictions);

  /// resource reservation management
  Scrub::ScrubResources m_resource_bookkeeper;

  /// the queue of PGs waiting to be scrubbed
  ScrubQueue m_queue;

  const std::string m_log_prefix{};

  /// list all scrub queue entries
  void debug_log_all_jobs() const;

  /// number of PGs stuck while scrubbing, waiting for objects
  int get_blocked_pgs_count() const;

  /**
   * roll a dice to determine whether we should skip this tick, not trying to
   * schedule a new scrub.
   * \returns true with probability of osd_scrub_backoff_ratio.
   */
  bool scrub_random_backoff() const;

  // tracking the CPU load
  // ---------------------------------------------------------------

  /*
   * tracking the average load on the CPU. Used both by the OSD performance
   * counters logger, and by the scrub queue (as no periodic scrubbing is
   * allowed if the load is too high).
   */

  /// the number of CPUs
  long loadavg_cpu_count{1};

  /// true if the load average (the 1-minute system average divided by
  /// the number of CPUs) is below the configured threshold
  bool scrub_load_below_threshold() const;


  // the scrub performance counters collections
  // ---------------------------------------------------------------

  // indexed by scrub level & pool type

  using pc_index_t = std::pair<scrub_level_t, int /*pool type*/>;
  // easy way to loop over the counter sets. Order must match the
  // perf_labels vector
  static inline std::array<pc_index_t, 4> perf_counters_indices = {
      pc_index_t{scrub_level_t::shallow, pg_pool_t::TYPE_REPLICATED},
      pc_index_t{scrub_level_t::deep, pg_pool_t::TYPE_REPLICATED},
      pc_index_t{scrub_level_t::shallow, pg_pool_t::TYPE_ERASURE},
      pc_index_t{scrub_level_t::deep, pg_pool_t::TYPE_ERASURE}};

  std::map<pc_index_t, ceph::common::PerfCounters*> m_perf_counters;

  // the labels matrix is: <shallow/deep>  X  <replicated/EC>
  static inline std::vector<std::string> perf_labels = {
      ceph::perf_counters::key_create(
	  "osd_scrub_sh_repl",
	  {{"level", "shallow"}, {"pooltype", "replicated"}}),
      ceph::perf_counters::key_create(
	  "osd_scrub_dp_repl",
	  {{"level", "deep"}, {"pooltype", "replicated"}}),
      ceph::perf_counters::key_create(
	  "osd_scrub_sh_ec",
	  {{"level", "shallow"}, {"pooltype", "ec"}}),
      ceph::perf_counters::key_create(
	  "osd_scrub_dp_ec",
	  {{"level", "deep"}, {"pooltype", "ec"}})};

  /**
   * create 4 sets of performance counters (for shallow vs. deep,
   * replicated vs. erasure pools). Add them to the cct, but also maintain
   * a separate map of the counters, indexed by the pool type and scrub level.
   */
  void create_scrub_perf_counters();

  // 'remove' the counters from the cct, and delete them
  void destroy_scrub_perf_counters();
};
