// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once
#include <string_view>

#include "osd/osd_types_fmt.h"
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

  ~OsdScrub() = default;

  // temporary friendship - only required in this transitory commit
  friend class OSD;

  // note: public, as accessed by the dout macros
  std::ostream& gen_prefix(std::ostream& out, std::string_view fn) const;

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


  // implementing the PGs interface to the scrub scheduling objects
  // ---------------------------------------------------------------

  // updating the resource counters
  bool inc_scrubs_local();
  void dec_scrubs_local();
  bool inc_scrubs_remote();
  void dec_scrubs_remote();

  // counting the number of PGs stuck while scrubbing, waiting for objects
  void mark_pg_scrub_blocked(spg_t blocked_pg);
  void clear_pg_scrub_blocked(spg_t blocked_pg);

  // updating scheduling information for a specific PG
  Scrub::sched_params_t determine_scrub_time(
      const requested_scrub_t& request_flags,
      const pg_info_t& pg_info,
      const pool_opts_t& pool_conf) const;

  /**
   * modify a scrub-job's scheduled time and deadline
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
   *  ('must' is Scrub::sched_params_t.is_must)
   *
   *  locking: not using the jobs_lock
   */
  void update_job(
      Scrub::ScrubJobRef sjob,
      const Scrub::sched_params_t& suggested);

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
  void register_with_osd(
      Scrub::ScrubJobRef sjob,
      const Scrub::sched_params_t& suggested);

  /**
   * remove the pg from set of PGs to be scanned for scrubbing.
   * To be used if we are no longer the PG's primary, or if the PG is removed.
   */
  void remove_from_osd_queue(Scrub::ScrubJobRef sjob);

  /**
   * \returns std::chrono::milliseconds indicating how long to wait between
   * chunks.
   *
   * Implementation Note: Returned value is either osd_scrub_sleep or
   * osd_scrub_extended_sleep, depending on must_scrub_param and time
   * of day (see configs osd_scrub_begin*)
   */
  std::chrono::milliseconds scrub_sleep_time(bool high_priority_scrub) const;

  /**
   * No new scrub session will start while a scrub was initiated on a PG,
   * and that PG is trying to acquire replica resources.
   * \retval false if the flag was already set (due to a race)
   */
  bool set_reserving_now();

  void clear_reserving_now();

  /**
   * \returns true if the current time is within the scrub time window
   */
  [[nodiscard]] bool scrub_time_permit(utime_t t) const;

  /**
   * An external interface into the LoadTracker object. Used by
   * the OSD tick to update the load data in the logger.
   *
   * \returns 100*(the decaying (running) average of the CPU load
   *          over the last 24 hours) or nullopt if the load is not
   *          available.
   * Note that the multiplication by 100 is required by the logger interface
   */
  std::optional<double> update_load_average();

 private:
  CephContext* cct;
  Scrub::ScrubSchedListener& m_osd_svc;
  const ceph::common::ConfigProxy& conf;

  /// resource reservation management
  Scrub::ScrubResources m_resource_bookkeeper;

  /// the queue of PGs waiting to be scrubbed
  ScrubQueue m_queue;

 public:
  // for this transitory commit only - to be removed
  bool can_inc_scrubs() { return m_resource_bookkeeper.can_inc_scrubs(); }

  // for this transitory commit only - to be removed
  Scrub::schedule_result_t select_pg_and_scrub(
      Scrub::OSDRestrictions& preconds);

  // for this transitory commit only - to be moved elsewhere
  /**
   * @return the list (not std::set!) of all scrub jobs registered
   *   (apart from PGs in the process of being removed)
   */
  Scrub::ScrubQContainer list_registered_jobs() const;

  /// one of this OSD's PGs is trying to acquire replica resources
  bool is_reserving_now() const;

 private:
  const std::string m_log_prefix{};

  /// number of PGs stuck while scrubbing, waiting for objects
  int get_blocked_pgs_count() const;
};
