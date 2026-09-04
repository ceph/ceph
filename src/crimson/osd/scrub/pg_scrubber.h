// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab expandtab

#pragma once

#include <fmt/format.h>

#include <seastar/core/shared_future.hh>

#include "crimson/common/operation.h"
#include "crimson/common/interruptible_future.h"
#include "crimson/osd/pg_interval_interrupt_condition.h"
#include "msg/Message.h"
#include "osd/scrubber_common.h"
#include "scrub_machine.h"
#include "osd/scrubber/scrub_job.h"
#include "scrub_queue.h"
#include "osd/scrubber/scrub_resources.h"

namespace crimson::osd {
class PG;
class ScrubScan;
class ScrubFindRange;
class ScrubReserveRange;
class ScrubSleep;
}

namespace crimson::osd::scrub {

class ScrubMetrics;

struct blocked_range_t {
  hobject_t begin;
  hobject_t end;
  seastar::shared_promise<> p;
};

/**
 * the scrub operation flags. Primary only.
 * Set at scrub start. Checked in multiple locations - mostly
 * at finish.
 */
struct scrub_flags_t {

  unsigned int priority{0};

  /**
   * set by set_op_parameters() for deep scrubs, if the hardware
   * supports auto repairing and osd_scrub_auto_repair is enabled.
   */
  bool auto_repair{false};

  /// this flag indicates that we are scrubbing post repair to verify everything
  /// is fixed (otherwise - PG_STATE_FAILED_REPAIR will be asserted.)
  /// Update (July 2024): now reflects an 'after-repair' urgency.
  bool check_repair{false};

  /// checked at the end of the scrub, to possibly initiate a deep-scrub
  bool deep_scrub_on_error{false};

  /// set when an operator-requested shallow scrub discards the stored deep-scrub
  /// error details; used to also zero the pg-stat error counters at scrub end
  bool deep_errors_cleared{false};
};

class PGScrubber : public crimson::BlockerT<PGScrubber>, ScrubContext {
  friend class ::crimson::osd::ScrubScan;
  friend class ::crimson::osd::ScrubFindRange;
  friend class ::crimson::osd::ScrubReserveRange;
  friend class ::crimson::osd::ScrubSleep;

  using interruptor = ::crimson::interruptible::interruptor<
    ::crimson::osd::IOInterruptCondition>;
  template <typename T = void>
  using ifut =
    ::crimson::interruptible::interruptible_future<
      ::crimson::osd::IOInterruptCondition, T>;

  PG &pg;

  /// PG alias for logging in header functions
  DoutPrefixProvider &dpp;

  ScrubMachine machine;

  std::optional<blocked_range_t> blocked;

  std::optional<eversion_t> waiting_for_update;

  /// the sub-object that manages this PG's scheduling parameters.
  /// An Optional instead of a regular member, as we wish to directly
  /// control the order of construction/destruction.
  std::optional<ScrubJob> m_scrub_job;
  /**
   * once we acquire the local OSD resource, this is set to a wrapper that
   * guarantees that the resource will be released when the scrub is done
   */
  std::unique_ptr<LocalResourceWrapper> m_local_osd_resource;
  bool m_queued_or_active{false};
  std::optional<SchedTarget> m_active_target;
  epoch_t m_epoch_start{0};  ///< the actual epoch when scrubbing started
  epoch_t m_last_aborted{0}; ///< epoch of last abort to avoid duplicate resets
  scrub_flags_t m_flags;
  bool m_is_deep{false};
  bool m_is_repair{false};
  bool m_after_repair_scrub_required{false}; ///< schedule after_repair scrub after recovery
  enum class delay_both_targets_t { no, yes };

  template <typename E>
  void handle_event(E &&e)
  {
    LOG_PREFIX(PGScrubber::handle_event);
    SUBDEBUGDPP(osd, "handle_event: {}", dpp, e);
    machine.process_event(std::forward<E>(e));
  }

public:
  static constexpr const char *type_name = "PGScrubber";
  using Blocker = PGScrubber;
  void dump_detail(Formatter *f) const;
  spg_t get_pg_id() const;
  PG& get_pg() { return pg; }

  static inline bool is_scrub_message(Message &m) {
    switch (m.get_type()) {
    case MSG_OSD_REP_SCRUB:
    case MSG_OSD_REP_SCRUBMAP:
    case MSG_OSD_SCRUB_RESERVE:
      return true;
    default:
      return false;
    }
    return false;
  }

  static utime_t scrub_must_stamp() { return utime_t(1, 1); }
  PGScrubber(PG &pg);
  virtual ~PGScrubber();

  /// setup scrub machine state
  void initiate() { machine.initiate(); }

  /// notify machine on primary that PG is active+clean
  void on_primary_active_clean();

  /// notify machine on replica that PG is active
  void on_replica_activate();

  /// notify machine of interval change
  void on_interval_change();

  /// notify machine that PG has committed up to versino v
  void on_log_update(eversion_t v);

  /// notify scrubber that recovery has completed
  void recovery_completed();

  seastar::future<schedule_result_t> start_scrub(
    scrub_level_t s_or_d,
    OSDRestrictions osd_restrictions,
    ScrubPGPreconds pg_cond);

  /// handle scrub request (called by start_scrub when scheduler picks up a job)
  void handle_scrub_requested(bool deep);

  /// enqueue a manually requested scrub (called by admin command)
  void enqueue_scrub_requested(bool deep, bool repair = false);

  /// handle schedule-scrub command (test/debug only)
  void handle_schedule_scrub(bool deep, int64_t offset);

  /// is this scrub's urgency high enough, or must it reserve its replicas?
  [[nodiscard]] bool is_reservation_required() const;


  /// handle other scrub message
  void handle_scrub_message(Message &m);

  /// notify machine of a mutation of on_object resulting in delta_stats
  void handle_op_stats(
    const hobject_t &on_object,
    object_stat_sum_t delta_stats);

  /// async scrub reservation granted by singleton-side reserver callback
  void send_granted_by_reserver(const AsyncScrubResData& res_data);

  /// delay next retry of this PG after a replica reservation failure
  void flag_reservations_failure();

  /// maybe block an op trying to mutate hoid until chunk is complete
  ifut<> wait_scrub(
    PGScrubber::BlockingEvent::TriggerI&& trigger,
    const hobject_t &hoid);

  /// Update scrub job scheduling (called when config changes or pool info changes)
  void update_scrub_job();
  /// Request a deep scrub to repair errors found in shallow scrub
  void request_rescrubbing();

  /// Get scrub store errors for SCRUBLS operation
  bool get_store_errors(const scrub_ls_arg_t& arg,
                        scrub_ls_result_t& res_inout) const;

  /// Check if scrub is queued or actively running
  bool is_queued_or_active() const {
    return m_queued_or_active;
  }

  /// Check if scrub is currently reserving replicas
  bool is_reserving_replicas() const;

  /// Dump scrub metrics (if scrubbing is active)
  void dump_scrub_metrics(ceph::Formatter* f);

  /// Access to metrics for scrub machine states
  ScrubMetrics* get_scrub_metrics() {
    return m_last_scrub_metrics.get();
  }

  /// Get scrub sleep time in milliseconds (like classic OSD)
  std::chrono::milliseconds get_scrub_sleep_time() const;

  bool has_pending_digest_updates() const {
    return m_digest_updates_pending != 0;
  }

  void on_digest_update_complete(uint64_t generation);

  /// Check if scrub should abort due to noscrub/nodeep-scrub flags
  bool should_abort() const;

  /// Verify if scrub should continue or abort, handling epoch tracking
  bool verify_against_abort(epoch_t epoch_to_verify);

  /// Handle mid-scrub abort by re-enqueuing the job
  void on_mid_scrub_abort(delay_cause_t issue);

  /**
   * scan_snaps
   *
   * Validate the local SnapMapper against the authoritative snapset stored
   * in each head object's SS_ATTR, and repair any discrepancies.
   * Called on the primary after each local scrub map is built.
   *
   * @param map  The local shard's scrub map for the current chunk
   */
  void scan_snaps(const ScrubMap &map);

  /// Metrics for the last or current scrub session
  /// Persists across state transitions so it can be queried after scrub completes
  std::unique_ptr<ScrubMetrics> m_last_scrub_metrics;

  /// Track scrub start time to calculate duration
  std::optional<ScrubTimePoint> m_scrub_start_time;

  /// Track the total number of objects scrubbed across all chunks
  int64_t m_objects_scrubbed_in_chunk{0};

  unsigned m_digest_updates_pending{0};
  uint64_t m_digest_updates_generation{0};

  /// Track the number of object copies fixed during repair scrub
  int m_fixed_count{0};

  /// Accumulated missing/inconsistent/error counts across all chunks.
  /// Emitted once in emit_scrub_result to match classic OSD's scrub_finish() summary.
  int m_total_missing_count{0};
  int m_total_inconsistent_count{0};
  int m_total_error_count{0};

  /// Store scrub results for retrieval by rados list-inconsistent-obj
  epoch_t m_scrub_epoch{0};
  // Dual-store approach matching classic OSD's shallow_db and deep_db
  // shallow_errors: cleared on every scrub, stores filtered shallow-only errors
  // deep_errors: cleared only on deep scrub, stores all errors
  std::vector<inconsistent_obj_wrapper> m_shallow_errors;
  std::vector<inconsistent_obj_wrapper> m_deep_errors;
  std::vector<inconsistent_snapset_wrapper> m_stored_snapset_errors;
  // Track the type of the last completed scrub for proper retrieval
  bool m_last_scrub_was_deep{false};

  /// Start sleep operation between chunks
  void start_chunk_sleep();

  void set_queued_or_active() {
    m_queued_or_active = true;
  }
  void clear_queued_or_active()
  {
    if (m_queued_or_active) {
      m_queued_or_active = false;
    }
  }

private:
  DoutPrefixProvider &get_dpp() final { return dpp; }

  void schedule_scrub_with_osd() final;
  void rm_from_osd_scrubbing() final;
  void clear_pgscrub_state() final;

  void notify_scrub_start(bool deep) final;
  void requeue_penalized(
      scrub_level_t s_or_d,
      delay_both_targets_t delay_both,
      delay_cause_t cause,
      utime_t scrub_clock_now);
  seastar::future<bool> reserve_local(const SchedTarget& trgt);

  const std::set<pg_shard_t> &get_ids_to_scrub() const final;

  chunk_validation_policy_t get_policy() const final;

  void request_range(const hobject_t &start) final;
  void reserve_range(const hobject_t &start, const hobject_t &end) final;
  void release_range() final;
  void scan_range(
    pg_shard_t target,
    eversion_t version,
    bool deep,
    const hobject_t &start,
    const hobject_t &end) final;
  bool await_update(const eversion_t &version) final;
  void generate_and_submit_chunk_result(
    const hobject_t &begin,
    const hobject_t &end,
    bool deep) final;
  void emit_chunk_result(
    const request_range_result_t &range,
    chunk_result_t &&result) final;
  void emit_scrub_result(
    bool deep,
    object_stat_sum_t scrub_stats) final;

  /**
   * log_object_errors
   *
   * Log detailed error messages for an inconsistent object to the cluster log.
   * This matches the classic OSD behavior where individual object errors
   * are logged with specific details about what's wrong.
   *
   * @param obj_error The inconsistent object with error details
   * @param hoid      Full hobject_t (with hash) for the object — used to
   *                  produce the canonical oid string in log messages.
   */
  void log_object_errors(const inconsistent_obj_wrapper& obj_error,
                         const hobject_t& hoid);

  /**
   * log_snapset_errors
   *
   * Log detailed error messages for an inconsistent snapset to the cluster log.
   * This matches the classic OSD behavior where snapset errors are logged
   * with specific details about what's wrong (missing clones, unexpected clones, etc).
   *
   * @param snapset_error The inconsistent snapset with error details
   */
  void log_snapset_errors(const inconsistent_snapset_wrapper& snapset_error);

  /**
   * scrub_process_inconsistent
   *
   * Process inconsistent objects found during scrub and initiate repairs.
   * Similar to classic OSD's ScrubBackend::scrub_process_inconsistent().
   * Spawns async repair operations in the background.
   *
   * @param object_errors Vector of inconsistent objects with error details
   * @param object_hoids Map from full hobject_t to hobject_t with correct hash
   * @return Number of object copies being repaired
   */
  int scrub_process_inconsistent(
    const std::vector<inconsistent_obj_wrapper>& object_errors,
    const std::map<hobject_t, hobject_t>& object_hoids);

  /**
   * repair_object
   *
   * Repair a single object by marking it as missing on bad shards,
   * then triggering recovery if the primary has a bad copy.
   * Follows the classic OSD pattern from ScrubBackend::repair_object()
   * but uses Crimson's async PG::repair_object() to trigger recovery.
   *
   * @param soid Object to repair
   * @param auth_shard Authoritative shard with good copy
   * @param bad_shards Set of shards with bad/missing copies
   * @param version Object version for repair
   * @return Future that completes when repair is initiated
   */
  ::crimson::interruptible::interruptible_future<
    ::crimson::osd::IOInterruptCondition, void> repair_object(
    const hobject_t& soid,
    pg_shard_t auth_shard,
    const std::set<pg_shard_t>& bad_shards,
    uint64_t version);

  sched_conf_t populate_config_params() const;
  void update_targets(utime_t scrub_clock_now);
  void set_op_parameters(ScrubPGPreconds pg_cond);
  void cleanup_on_finish();
  void reset_internal_state();
  std::string_view registration_state() const;
  bool should_drop_message(Message &m) const;
  void handle_scrub_reserve_msgs(Message &m);

  /**
   * a text description of the current scrub mode (repair/deep-scrub/scrub)
   *
   * Note: based on PG_STATE_REPAIR, and not on m_is_repair. I.e. for
   * auto_repair will show as "deep-scrub" and not as "repair" (until the first
   * error is detected).
   */
  std::string_view m_mode_desc;

  void update_op_mode_text();

  std::string_view get_op_mode_text() const;

};

} // namespace crimson::osd::scrub

namespace fmt {

template <>
struct formatter<crimson::osd::scrub::blocked_range_t> {
  constexpr auto parse(format_parse_context& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const crimson::osd::scrub::blocked_range_t& r,
              FormatContext& ctx) const {
    return format_to(
        ctx.out(),
        "blocked_range[{} -> {}]",
        r.begin,
        r.end);
  }
};

} // namespace fmt
