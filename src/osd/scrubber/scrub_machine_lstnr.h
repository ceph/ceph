// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once
/**
 * \file the PgScrubber interface used by the scrub FSM
 */
#include "common/version.h"
#include "include/Context.h"
#include "osd/osd_types.h"

struct ScrubMachineListener;

namespace Scrub {

enum class PreemptionNoted { no_preemption, preempted };

/// the interface exposed by the PgScrubber into its internal
/// preemption_data object
struct preemption_t {

  virtual ~preemption_t() = default;

  preemption_t() = default;
  preemption_t(const preemption_t&) = delete;
  preemption_t(preemption_t&&) = delete;

  [[nodiscard]] virtual bool is_preemptable() const = 0;

  [[nodiscard]] virtual bool was_preempted() const = 0;

  virtual void adjust_parameters() = 0;

  /**
   *  Try to preempt the scrub.
   *  'true' (i.e. - preempted) if:
   *   preemptable && not already preempted
   */
  virtual bool do_preempt() = 0;

  /**
   *  disables preemptions.
   *  Returns 'true' if we were already preempted
   */
  virtual bool disable_and_test() = 0;
};

/// an aux used when blocking on a busy object.
/// Issues a log warning if still blocked after 'waittime'.
struct blocked_range_t {
  blocked_range_t(OSDService* osds,
		  ceph::timespan waittime,
		  ScrubMachineListener& scrubber);
  ~blocked_range_t();

  OSDService* m_osds;
  ScrubMachineListener& m_scrubber;

  Context* m_callbk;

  // once timed-out, we flag the OSD's scrub-queue as having
  // a problem. 'm_warning_issued' signals the need to clear
  // that OSD-wide flag.
  bool m_warning_issued{false};
};

using BlockedRangeWarning = std::unique_ptr<blocked_range_t>;

}  // namespace Scrub

struct ScrubMachineListener {

  struct MsgAndEpoch {
    MessageRef m_msg;
    epoch_t m_epoch;
  };

  virtual ~ScrubMachineListener() = default;

  /// set the string we'd use in logs to convey the current state-machine
  /// state.
  virtual void set_state_name(const char* name) = 0;

  [[nodiscard]] virtual bool is_primary() const = 0;

  virtual void select_range_n_notify() = 0;

  virtual Scrub::BlockedRangeWarning acquire_blocked_alarm() = 0;

  /// walk the log to find the latest update that affects our chunk
  virtual eversion_t search_log_for_updates() const = 0;

  virtual eversion_t get_last_update_applied() const = 0;

  virtual int pending_active_pushes() const = 0;

  virtual int build_primary_map_chunk() = 0;

  virtual int build_replica_map_chunk() = 0;

  virtual void on_init() = 0;

  virtual void on_replica_init() = 0;

  virtual void replica_handling_done() = 0;

  /// the version of 'scrub_clear_state()' that does not try to invoke FSM
  /// services (thus can be called from FSM reactions)
  virtual void clear_pgscrub_state() = 0;

  /*
   * Send an 'InternalSchedScrub' FSM event either immediately, or - if
   * 'm_need_sleep' is asserted - after a configuration-dependent timeout.
   */
  virtual void add_delayed_scheduling() = 0;

  /**
   * Ask all replicas for their scrub maps for the current chunk.
   */
  virtual void get_replicas_maps(bool replica_can_preempt) = 0;

  virtual void on_digest_updates() = 0;

  /// the part that actually finalizes a scrub
  virtual void scrub_finish() = 0;

  /**
   * Prepare a MOSDRepScrubMap message carrying the requested scrub map
   * @param was_preempted - were we preempted?
   * @return the message, and the current value of 'm_replica_min_epoch' (which
   * is used when sending the message, but will be overwritten before that).
   */
  [[nodiscard]] virtual MsgAndEpoch prep_replica_map_msg(
    Scrub::PreemptionNoted was_preempted) = 0;

  /**
   * Send to the primary the pre-prepared message containing the requested map
   */
  virtual void send_replica_map(const MsgAndEpoch& preprepared) = 0;

  /**
   * Let the primary know that we were preempted while trying to build the
   * requested map.
   */
  virtual void send_preempted_replica() = 0;

  [[nodiscard]] virtual bool has_pg_marked_new_updates() const = 0;

  virtual void set_subset_last_update(eversion_t e) = 0;

  [[nodiscard]] virtual bool was_epoch_changed() const = 0;

  virtual Scrub::preemption_t& get_preemptor() = 0;

  /**
   *  a "technical" collection of the steps performed once all
   *  rep maps are available:
   *  - the maps are compared
   *  - the scrub region markers (start_ & end_) are advanced
   *  - callbacks and ops that were pending are allowed to run
   */
  virtual void maps_compare_n_cleanup() = 0;

  /**
   * order the PgScrubber to initiate the process of reserving replicas' scrub
   * resources.
   */
  virtual void reserve_replicas() = 0;

  virtual void unreserve_replicas() = 0;

  virtual void set_scrub_begin_time() = 0;

  virtual void set_scrub_duration() = 0;

  /**
   * No new scrub session will start while a scrub was initiate on a PG,
   * and that PG is trying to acquire replica resources.
   * set_reserving_now()/clear_reserving_now() let's the OSD scrub-queue know
   * we are busy reserving.
   */
  virtual void set_reserving_now() = 0;
  virtual void clear_reserving_now() = 0;

  /**
   * Manipulate the 'I am being scrubbed now' Scrubber's flag
   */
  virtual void set_queued_or_active() = 0;
  virtual void clear_queued_or_active() = 0;

  /**
   * Our scrubbing is blocked, waiting for an excessive length of time for
   * our target chunk to be unlocked. We will set the corresponding flags,
   * both in the OSD_wide scrub-queue object, and in our own scrub-job object.
   * Both flags are used to report the unhealthy state in the log and in
   * response to scrub-queue queries.
   */
  virtual void set_scrub_blocked(utime_t since) = 0;
  virtual void clear_scrub_blocked() = 0;

  /**
   * the FSM interface into the "are we waiting for maps, either our own or from
   * replicas" state.
   * The FSM can only:
   * - mark the local map as available, and
   * - query status
   */
  virtual void mark_local_map_ready() = 0;

  [[nodiscard]] virtual bool are_all_maps_available() const = 0;

  /// a log/debug interface
  virtual std::string dump_awaited_maps() const = 0;

  /// exposed to be used by the scrub_machine logger
  virtual std::ostream& gen_prefix(std::ostream& out) const = 0;

  /// sending cluster-log warnings
  virtual void log_cluster_warning(const std::string& msg) const = 0;

  /// get a counting-ref to the PG itself.
  /// (used mainly when a sub-object of the Scrubber creates callbacks)
  virtual PGRef get_pgref() = 0;
};
