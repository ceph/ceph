// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once
/**
 * \file the PgScrubber interface used by the scrub FSM
 */
#include "common/LogClient.h"
#include "common/version.h"
#include "include/Context.h"
#include "osd/osd_types.h"
#include "osd/scrubber_common.h"

class PG;

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

}  // namespace Scrub

struct ScrubMachineListener {
  virtual CephContext *get_pg_cct() const = 0;
  virtual LogChannelRef &get_clog() const = 0;
  virtual int get_whoami() const = 0;
  virtual spg_t get_spgid() const = 0;
  virtual PG* get_pg() const = 0;

  /**
   * access the set of performance counters relevant to this scrub
   * (one of the four sets of counters maintained by the OSD)
   */
  virtual PerfCounters& get_counters_set() const = 0;

  using scrubber_callback_t = std::function<void(void)>;
  using scrubber_callback_cancel_token_t = Context*;

  /**
   * schedule_callback_after
   *
   * cb will be invoked after least duration time has elapsed.
   * Interface implementation is responsible for maintaining and locking
   * a PG reference.  cb will be silently discarded if the interval has changed
   * between the call to schedule_callback_after and when the pg is locked.
   *
   * Returns an associated token to be used in cancel_callback below.
   */
  virtual scrubber_callback_cancel_token_t schedule_callback_after(
    ceph::timespan duration, scrubber_callback_t &&cb) = 0;

  /**
   * cancel_callback
   *
   * Attempts to cancel the callback to which the passed token is associated.
   * cancel_callback is best effort, the callback may still fire.
   * cancel_callback guarantees that exactly one of the two things will happen:
   * - the callback is destroyed and will not be invoked
   * - the callback will be invoked
   */
  virtual void cancel_callback(scrubber_callback_cancel_token_t) = 0;

  virtual ceph::timespan get_range_blocked_grace() = 0;

  struct MsgAndEpoch {
    MessageRef m_msg;
    epoch_t m_epoch;
  };

  virtual ~ScrubMachineListener() = default;

  /// set the string we'd use in logs to convey the current state-machine
  /// state.
  virtual void set_state_name(const char* name) = 0;

  /// access the text specifying scrub level and whether it is a repair
  virtual std::string_view get_op_mode_text() const = 0;

  [[nodiscard]] virtual bool is_primary() const = 0;

  /// dequeue this PG from the OSD's scrub-queue
  virtual void rm_from_osd_scrubbing() = 0;

  /**
   * the FSM has entered the PrimaryActive state. That happens when
   * peered as a Primary, and achieving the 'active' state.
   */
  virtual void schedule_scrub_with_osd() = 0;

  virtual void select_range_n_notify() = 0;

  /// walk the log to find the latest update that affects our chunk
  virtual eversion_t search_log_for_updates() const = 0;

  virtual eversion_t get_last_update_applied() const = 0;

  virtual int pending_active_pushes() const = 0;

  virtual int build_primary_map_chunk() = 0;

  virtual int build_replica_map_chunk() = 0;

  virtual void on_init() = 0;

  virtual void on_replica_init() = 0;

  virtual void replica_handling_done() = 0;

  /**
   * clears both internal scrub state, and some PG-visible flags:
   * - the two scrubbing PG state flags;
   * - primary/replica scrub position (chunk boundaries);
   * - primary/replica interaction state;
   * - the backend state;
   * Also runs pending callbacks, and clears the active flags.
   * Does not try to invoke FSM events.
   */
  virtual void clear_pgscrub_state() = 0;

  /// Get time to sleep before next scrub
  virtual std::chrono::milliseconds get_scrub_sleep_time() const = 0;

  /// Queues InternalSchedScrub for later
  virtual void queue_for_scrub_resched(Scrub::scrub_prio_t prio) = 0;

  /**
   * Ask all replicas for their scrub maps for the current chunk.
   */
  virtual void get_replicas_maps(bool replica_can_preempt) = 0;

  virtual void on_digest_updates() = 0;

  /// the part that actually finalizes a scrub
  virtual void scrub_finish() = 0;

  /// notify the scrubber about a scrub failure
  /// (note: temporary implementation)
  virtual void penalize_next_scrub(Scrub::delay_cause_t cause) = 0;

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

  virtual void set_scrub_duration(std::chrono::milliseconds duration) = 0;

  /**
   * Manipulate the 'I am being scrubbed now' Scrubber's flag
   */
  virtual void set_queued_or_active() = 0;
  virtual void clear_queued_or_active() = 0;

  /// note the epoch when the scrub session started
  virtual void reset_epoch() = 0;

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

  /// delay next retry of this PG after a replica reservation failure
  virtual void flag_reservations_failure() = 0;

  /// is this scrub more than just regular periodic scrub?
  [[nodiscard]] virtual bool is_high_priority() const = 0;
};
