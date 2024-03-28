// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

// clang-format off
/*

Main Scrubber interfaces:

┌──────────────────────────────────────────────┬────┐
│                                              │    │
│                                              │    │
│       PG                                     │    │
│                                              │    │
│                                              │    │
├──────────────────────────────────────────────┘    │
│                                                   │
│       PrimaryLogPG                                │
└────────────────────────────────┬──────────────────┘
                                 │
                                 │
                                 │ owns & uses
                                 │
                                 │
                                 │
┌────────────────────────────────▼──────────────────┐
│               <<ScrubPgIF>>                       │
└───────────────────────────▲───────────────────────┘
                            │
                            │
                            │implements
                            │
                            │
                            │
┌───────────────────────────┴───────────────┬───────┐
│                                           │       │
│         PgScrubber                        │       │
│                                           │       │
│                                           │       ├───────┐
├───────────────────────────────────────────┘       │       │
│                                                   │       │
│         PrimaryLogScrub                           │       │
└─────┬───────────────────┬─────────────────────────┘       │
      │                   │                         implements
      │    owns & uses    │                                 │
      │                   │       ┌─────────────────────────▼──────┐
      │                   │       │    <<ScrubMachineListener>>    │
      │                   │       └─────────▲──────────────────────┘
      │                   │                 │
      │                   │                 │
      │                   ▼                 │
      │    ┌────────────────────────────────┴───────┐
      │    │                                        │
      │    │        ScrubMachine                    │
      │    │                                        │
      │    └────────────────────────────────────────┘
      │
  ┌───▼─────────────────────────────────┐
  │                                     │
  │       ScrubStore                    │
  │                                     │
  └─────────────────────────────────────┘

*/
// clang-format on


#include <cassert>
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "osd/PG.h"
#include "osd/scrubber_common.h"

#include "ScrubStore.h"
#include "osd_scrub_sched.h"
#include "scrub_backend.h"
#include "scrub_machine_lstnr.h"
#include "scrub_reservations.h"

namespace Scrub {
class ScrubMachine;
struct BuildMap;
class LocalResourceWrapper;


/**
 * Once all replicas' scrub maps are received, we go on to compare the maps.
 * That is - unless we we have not yet completed building our own scrub map.
 * MapsCollectionStatus combines the status of waiting for both the local map
 * and the replicas, without resorting to adding dummy entries into a list.
 */
class MapsCollectionStatus {

  bool m_local_map_ready{false};
  std::vector<pg_shard_t> m_maps_awaited_for;

 public:
  [[nodiscard]] bool are_all_maps_available() const
  {
    return m_local_map_ready && m_maps_awaited_for.empty();
  }

  void mark_local_map_ready() { m_local_map_ready = true; }

  void mark_replica_map_request(pg_shard_t from_whom)
  {
    m_maps_awaited_for.push_back(from_whom);
  }

  /// @returns true if indeed waiting for this one. Otherwise: an error string
  auto mark_arriving_map(pg_shard_t from) -> std::tuple<bool, std::string_view>;

  [[nodiscard]] std::vector<pg_shard_t> get_awaited() const
  {
    return m_maps_awaited_for;
  }

  void reset();

  std::string dump() const;

  friend ostream& operator<<(ostream& out, const MapsCollectionStatus& sf);
};


}  // namespace Scrub


/**
 * the scrub operation flags. Primary only.
 * Set at scrub start. Checked in multiple locations - mostly
 * at finish.
 */
struct scrub_flags_t {

  unsigned int priority{0};

  /**
   * set by queue_scrub() if either planned_scrub.auto_repair or
   * need_auto were set.
   * Tested at scrub end.
   */
  bool auto_repair{false};

  /// this flag indicates that we are scrubbing post repair to verify everything
  /// is fixed
  bool check_repair{false};

  /// checked at the end of the scrub, to possibly initiate a deep-scrub
  bool deep_scrub_on_error{false};

  /**
   * scrub must not be aborted.
   * Set for explicitly requested scrubs, and for scrubs originated by the
   * pairing process with the 'repair' flag set (in the RequestScrub event).
   */
  bool required{false};
};

ostream& operator<<(ostream& out, const scrub_flags_t& sf);


/**
 * The part of PG-scrubbing code that isn't state-machine wiring.
 *
 * Why the separation? I wish to move to a different FSM implementation. Thus I
 * am forced to strongly decouple the state-machine implementation details from
 * the actual scrubbing code.
 */
class PgScrubber : public ScrubPgIF,
                   public ScrubMachineListener,
                   public ScrubBeListener {
 public:
  explicit PgScrubber(PG* pg);

  friend class ScrubBackend;  // will be replaced by a limited interface

  //  ------------------  the I/F exposed to the PG (ScrubPgIF) -------------

  /// are we waiting for resource reservation grants form our replicas?
  [[nodiscard]] bool is_reserving() const final;

  void initiate_regular_scrub(epoch_t epoch_queued) final;

  void initiate_scrub_after_repair(epoch_t epoch_queued) final;

  void send_scrub_resched(epoch_t epoch_queued) final;

  void active_pushes_notification(epoch_t epoch_queued) final;

  void update_applied_notification(epoch_t epoch_queued) final;

  void send_scrub_unblock(epoch_t epoch_queued) final;

  void digest_update_notification(epoch_t epoch_queued) final;

  void send_replica_maps_ready(epoch_t epoch_queued) final;

  void send_start_replica(epoch_t epoch_queued, Scrub::act_token_t token) final;

  void send_sched_replica(epoch_t epoch_queued, Scrub::act_token_t token) final;

  void send_replica_pushes_upd(epoch_t epoch_queued) final;
  /**
   * The PG has updated its 'applied version'. It might be that we are waiting
   * for this information: after selecting a range of objects to scrub, we've
   * marked the latest version of these objects in m_subset_last_update. We will
   * not start the map building before we know that the PG has reached this
   * version.
   */
  void on_applied_when_primary(const eversion_t& applied_version) final;

  void send_chunk_free(epoch_t epoch_queued) final;

  void send_chunk_busy(epoch_t epoch_queued) final;

  void send_local_map_done(epoch_t epoch_queued) final;

  void send_get_next_chunk(epoch_t epoch_queued) final;

  void send_scrub_is_finished(epoch_t epoch_queued) final;

  void send_granted_by_reserver(const AsyncScrubResData& req) final;

  /**
   *  we allow some number of preemptions of the scrub, which mean we do
   *  not block.  Then we start to block.  Once we start blocking, we do
   *  not stop until the scrub range is completed.
   */
  bool write_blocked_by_scrub(const hobject_t& soid) final;

  /// true if the given range intersects the scrub interval in any way
  bool range_intersects_scrub(const hobject_t& start,
			      const hobject_t& end) final;

  /**
   * route incoming replica-reservations requests/responses to the
   * appropriate handler.
   * As the ReplicaReservations object is to be owned by the ScrubMachine, we
   * send all relevant messages to the ScrubMachine.
   */
  void handle_scrub_reserve_msgs(OpRequestRef op) final;

  // managing scrub op registration

  void update_scrub_job(const requested_scrub_t& request_flags) final;

  void rm_from_osd_scrubbing() final;

  void schedule_scrub_with_osd() final;

  scrub_level_t scrub_requested(
      scrub_level_t scrub_level,
      scrub_type_t scrub_type,
      requested_scrub_t& req_flags) final;

  /**
   * Reserve local scrub resources (managed by the OSD)
   *
   * Fails if OSD's local-scrubs budget was exhausted
   * \returns were local resources reserved?
   */
  bool reserve_local() final;

  void handle_query_state(ceph::Formatter* f) final;

  pg_scrubbing_status_t get_schedule() const final;

  void on_operator_periodic_cmd(
    ceph::Formatter* f,
    scrub_level_t scrub_level,
    int64_t offset) final;

  void on_operator_forced_scrub(
    ceph::Formatter* f,
    scrub_level_t scrub_level,
    requested_scrub_t& request_flags) final;

  void dump_scrubber(ceph::Formatter* f,
		     const requested_scrub_t& request_flags) const final;

  // used if we are a replica

  void replica_scrub_op(OpRequestRef op) final;

  /// the op priority, taken from the primary's request message
  Scrub::scrub_prio_t replica_op_priority() const final
  {
    return m_replica_request_priority;
  };

  unsigned int scrub_requeue_priority(
    Scrub::scrub_prio_t with_priority,
    unsigned int suggested_priority) const final;
  /// the version that refers to m_flags.priority
  unsigned int scrub_requeue_priority(
    Scrub::scrub_prio_t with_priority) const final;

  void add_callback(Context* context) final { m_callbacks.push_back(context); }

  [[nodiscard]] bool are_callbacks_pending() const final  // used for an assert
							  // in PG.cc
  {
    return !m_callbacks.empty();
  }

  /// handle a message carrying a replica map
  void map_from_replica(OpRequestRef op) final;

  void on_new_interval() final;

  void on_primary_active_clean() final;

  void on_replica_activate() final;

  bool is_queued_or_active() const final;

  /**
   *  add to scrub statistics, but only if the soid is below the scrub start
   */
  void stats_of_handled_objects(const object_stat_sum_t& delta_stats,
				const hobject_t& soid) override
  {
    ceph_assert(false);
  }

  /**
   * finalize the parameters of the initiated scrubbing session:
   *
   * The "current scrub" flags (m_flags) are set from the 'planned_scrub'
   * flag-set; PG_STATE_SCRUBBING, and possibly PG_STATE_DEEP_SCRUB &
   * PG_STATE_REPAIR are set.
   */
  void set_op_parameters(const requested_scrub_t& request) final;

  void cleanup_store(ObjectStore::Transaction* t) final;

  bool get_store_errors(const scrub_ls_arg_t& arg,
			scrub_ls_result_t& res_inout) const override
  {
    return false;
  }

  void update_scrub_stats(ceph::coarse_real_clock::time_point now_is) final;

  int asok_debug(std::string_view cmd,
		 std::string param,
		 Formatter* f,
		 std::stringstream& ss) override;

  int m_debug_blockrange{0};

  // --------------------------------------------------------------------------
  // the I/F used by the state-machine (i.e. the implementation of
  // ScrubMachineListener)

  LogChannelRef &get_clog() const final;
  int get_whoami() const final;
  spg_t get_spgid() const final { return m_pg->get_pgid(); }
  PG* get_pg() const final { return m_pg; }
  PerfCounters& get_counters_set() const final;

  /// delay next retry of this PG after a replica reservation failure
  void flag_reservations_failure();

  scrubber_callback_cancel_token_t schedule_callback_after(
    ceph::timespan duration, scrubber_callback_t &&cb);

  void cancel_callback(scrubber_callback_cancel_token_t);

  ceph::timespan get_range_blocked_grace() {
    int grace = get_pg_cct()->_conf->osd_blocked_scrub_grace_period;
    if (grace == 0) {
      return ceph::timespan{};
    }
    ceph::timespan grace_period{
      m_debug_blockrange ?
      std::chrono::seconds(4) :
      std::chrono::seconds{grace}};
    return grace_period;
  }

  [[nodiscard]] bool is_primary() const final
  {
    return m_pg->recovery_state.is_primary();
  }

  /// is this scrub more than just regular periodic scrub?
  [[nodiscard]] bool is_high_priority() const final;

  void set_state_name(const char* name) final
  {
    m_fsm_state_name = name;
  }

  void select_range_n_notify() final;

  void set_scrub_blocked(utime_t since) final;
  void clear_scrub_blocked() final;


  /// walk the log to find the latest update that affects our chunk
  eversion_t search_log_for_updates() const final;

  eversion_t get_last_update_applied() const final
  {
    return m_pg->recovery_state.get_last_update_applied();
  }

  int pending_active_pushes() const final { return m_pg->active_pushes; }

  void on_init() final;
  void on_replica_init() final;
  void replica_handling_done() final;

  void clear_pgscrub_state() final;

  std::chrono::milliseconds get_scrub_sleep_time() const final;
  void queue_for_scrub_resched(Scrub::scrub_prio_t prio) final;

  void get_replicas_maps(bool replica_can_preempt) final;

  void on_digest_updates() final;

  void scrub_finish() final;

  void penalize_next_scrub(Scrub::delay_cause_t cause) final;

  ScrubMachineListener::MsgAndEpoch prep_replica_map_msg(
    Scrub::PreemptionNoted was_preempted) final;

  void send_replica_map(
    const ScrubMachineListener::MsgAndEpoch& preprepared) final;

  void send_preempted_replica() final;

  /**
   *  does the PG have newer updates than what we (the scrubber) know?
   */
  [[nodiscard]] bool has_pg_marked_new_updates() const final;

  void set_subset_last_update(eversion_t e) final;

  void maps_compare_n_cleanup() final;

  Scrub::preemption_t& get_preemptor() final;

  int build_primary_map_chunk() final;

  int build_replica_map_chunk() final;

  bool set_reserving_now() final;
  void clear_reserving_now() final;

  [[nodiscard]] bool was_epoch_changed() const final;

  void set_queued_or_active() final;
  /// Clears `m_queued_or_active` and restarts snap-trimming
  void clear_queued_or_active() final;

  void mark_local_map_ready() final;

  [[nodiscard]] bool are_all_maps_available() const final;

  std::string dump_awaited_maps() const final;

  void set_scrub_duration(std::chrono::milliseconds duration) final;

  std::ostream& gen_prefix(std::ostream& out) const final;

  /// facilitate scrub-backend access to SnapMapper mappings
  Scrub::SnapMapReaderI& get_snap_mapper_accessor()
  {
    return m_pg->snap_mapper;
  }

  void log_cluster_warning(const std::string& warning) const final;

 protected:
  bool state_test(uint64_t m) const { return m_pg->state_test(m); }
  void state_set(uint64_t m) { m_pg->state_set(m); }
  void state_clear(uint64_t m) { m_pg->state_clear(m); }

  [[nodiscard]] bool is_scrub_registered() const;

  /// the 'is-in-scheduling-queue' status, using relaxed-semantics access to the
  /// status
  std::string_view registration_state() const;

  virtual void _scrub_clear_state() {}

  utime_t m_scrub_reg_stamp;		///< stamp we registered for
  Scrub::ScrubJobRef m_scrub_job;	///< the scrub-job used by the OSD to
					///< schedule us

  ostream& show(ostream& out) const override;

 public:
  //  ------------------  the I/F used by the ScrubBackend (ScrubBeListener)

  // note: the reason we must have these forwarders, is because of the
  //  artificial PG vs. PrimaryLogPG distinction. Some of the services used
  //  by the scrubber backend are PrimaryLog-specific.

  void add_to_stats(const object_stat_sum_t& stat) override
  {
    ceph_assert(0 && "expecting a PrimaryLogScrub object");
  }

  void submit_digest_fixes(const digests_fixes_t& fixes) override
  {
    ceph_assert(0 && "expecting a PrimaryLogScrub object");
  }

  CephContext* get_pg_cct() const final { return m_pg->cct; }
 
  LoggerSinkSet& get_logger() const final;

  spg_t get_pgid() const final { return m_pg->get_pgid(); }

  /// Returns reference to current osdmap
  const OSDMapRef& get_osdmap() const final;


  // ---------------------------------------------------------------------------

  friend ostream& operator<<(ostream& out, const PgScrubber& scrubber);

  static utime_t scrub_must_stamp() { return utime_t(1, 1); }

  virtual ~PgScrubber();  // must be defined separately, in the .cc file

  [[nodiscard]] bool is_scrub_active() const final { return m_active; }

 private:
  void reset_internal_state();

  bool is_token_current(Scrub::act_token_t received_token);

  void requeue_waiting() const { m_pg->requeue_ops(m_pg->waiting_for_scrub); }

  /// Modify the token identifying the current replica scrub operation
  void advance_token();

  /**
   *  mark down some parameters of the initiated scrub:
   *  - the epoch when started;
   *  - the depth of the scrub requested (from the PG_STATE variable)
   */
  void reset_epoch() final;

  void run_callbacks();

  // 'query' command data for an active scrub
  void dump_active_scrubber(ceph::Formatter* f, bool is_deep) const;

  // -----     methods used to verify the relevance of incoming events:

  /**
   * should_drop_message
   *
   * Returns false if message was sent in the current epoch.  Otherwise,
   * returns true and logs a debug message.
   */
  bool should_drop_message(OpRequestRef &op) const;

  /**
   *  is the incoming event still relevant and should be forwarded to the FSM?
   *
   *  It isn't if:
   *  - (1) we are no longer 'actively scrubbing'; or
   *  - (2) the message is from an epoch prior to when we started the current
   * scrub session; or
   *  - (3) the message epoch is from a previous interval; or
   *  - (4) the 'abort' configuration flags were set.
   *
   *  For (1) & (2) - the incoming message is discarded, w/o further action.
   *
   *  For (3): (see check_interval() for a full description) if we have not
   *  reacted yet to this specific new interval, we do now:
   *   - replica reservations are silently discarded (we count on the replicas to
   *     notice the interval change and un-reserve themselves);
   *  - the scrubbing is halted.
   *
   *  For (4): the message will be discarded, but also:
   *    if this is the first time we've noticed the 'abort' request, we perform
   *    the abort.
   *
   *  \returns should the incoming event be processed?
   */
  bool is_message_relevant(epoch_t epoch_to_verify);

  /**
   * check the 'no scrub' configuration options.
   */
  [[nodiscard]] bool should_abort() const;

  /**
   * Check the 'no scrub' configuration flags.
   *
   * Reset everything if the abort was not handled before.
   * @returns false if the message was discarded due to abort flag.
   */
  [[nodiscard]] bool verify_against_abort(epoch_t epoch_to_verify);

  [[nodiscard]] bool check_interval(epoch_t epoch_to_verify) const;

  epoch_t m_last_aborted{};  // last time we've noticed a request to abort

  /**
   * once we acquire the local OSD resource, this is set to a wrapper that
   * guarantees that the resource will be released when the scrub is done
   */
  std::unique_ptr<Scrub::LocalResourceWrapper> m_local_osd_resource;

  /**
   * clearing the scrubber state & the PG's scrub-related flags
   * (calls clear_pgscrub_state()).
   * Also - publishes the PG stats.
   */
  void cleanup_on_finish();

 protected:
  PG* const m_pg;

  /**
   * the derivative-specific scrub-finishing touches:
   */
  virtual void _scrub_finish() {}

  // common code used by build_primary_map_chunk() and
  // build_replica_map_chunk():
  int build_scrub_map_chunk(ScrubMap& map,  // primary or replica?
			    ScrubMapBuilder& pos,
			    hobject_t start,
			    hobject_t end,
			    bool deep);

  std::unique_ptr<Scrub::ScrubMachine> m_fsm;
  /// the FSM state, as a string for logging
  const char* m_fsm_state_name{nullptr};
  const spg_t m_pg_id;	///< a local copy of m_pg->pg_id
  OSDService* const m_osds;
  const pg_shard_t m_pg_whoami;	 ///< a local copy of m_pg->pg_whoami;

  epoch_t m_interval_start{0};	///< interval's 'from' of when scrubbing was
				///< first scheduled

  void repair_oinfo_oid(ScrubMap& smap);

  /*
   * the exact epoch when the scrubbing actually started (started here - cleared
   * checks for no-scrub conf). Incoming events are verified against this, with
   * stale events discarded.
   */
  epoch_t m_epoch_start{0};  ///< the actual epoch when scrubbing started

  /**
   * (replica) a tag identifying a specific replica operation, i.e. the
   * creation of the replica scrub map for a single chunk.
   *
   * Background: the backend is asynchronous, and the specific
   * operations are size-limited. While the scrubber handles a specific
   * request, it is continuously triggered to poll the backend for the
   * full results for the chunk handled.
   * Once the chunk request becomes obsolete, either following an interval
   * change or if a new request was received, we must not send the stale
   * data to the primary. The polling of the obsolete chunk request must
   * stop, and the stale backend response should be discarded.
   * In other words - the token should be read as saying "the primary has
   * lost interest in the results of all operations identified by mismatched
   * token values".
   */
  Scrub::act_token_t m_current_token{1};

  /**
   * (primary/replica) a test aid. A counter that is incremented whenever a
   * scrub starts, and again when it terminates. Exposed as part of the 'pg
   * query' command, to be used by test scripts.
   *
   * @ATTN: not guaranteed to be accurate. To be only used for tests. This is
   * why it is initialized to a meaningless number;
   */
  int32_t m_sessions_counter{
    (int32_t)((int64_t)(this) & 0x0000'0000'00ff'fff0)};
  bool m_publish_sessions{false};  //< will the counter be part of 'query'
				   //output?

  scrub_flags_t m_flags;

  /// a reference to the details of the next scrub (as requested and managed by
  /// the PG)
  requested_scrub_t& m_planned_scrub;

  bool m_active{false};

  /**
   * a flag designed to prevent the initiation of a second scrub on a PG for
   * which scrubbing has been initiated.
   *
   * set once scrubbing was initiated (i.e. - even before the FSM event that
   * will trigger a state-change out of Inactive was handled), and only reset
   * once the FSM is back in Inactive.
   * In other words - its ON period encompasses:
   *   - the time period covered today by 'queued', and
   *   - the time when m_active is set, and
   *   - all the time from scrub_finish() calling update_stats() till the
   *     FSM handles the 'finished' event
   *
   * Compared with 'm_active', this flag is asserted earlier and remains ON for
   * longer.
   */
  bool m_queued_or_active{false};

  eversion_t m_subset_last_update{};

  std::unique_ptr<Scrub::Store> m_store;

  int num_digest_updates_pending{0};
  hobject_t m_start, m_end;  ///< note: half-closed: [start,end)

  /// Returns epoch of current osdmap
  epoch_t get_osdmap_epoch() const { return get_osdmap()->get_epoch(); }

  uint64_t get_scrub_cost(uint64_t num_chunk_objects);

  // collected statistics
  int m_shallow_errors{0};
  int m_deep_errors{0};
  int m_fixed_count{0};

 protected:
  /**
   * 'm_is_deep' - is the running scrub a deep one?
   *
   * Note that most of the code directly checks PG_STATE_DEEP_SCRUB, which is
   * primary-only (and is set earlier - when scheduling the scrub). 'm_is_deep'
   * is meaningful both for the primary and the replicas, and is used as a
   * parameter when building the scrub maps.
   */
  bool m_is_deep{false};

  /**
   * If set: affects the backend & scrubber-backend functions called after all
   * scrub maps are available.
   *
   * Replaces code that directly checks PG_STATE_REPAIR (which was meant to be
   * a "user facing" status display only).
   */
  bool m_is_repair{false};

  /**
   * User-readable summary of the scrubber's current mode of operation. Used for
   * both osd.*.log and the cluster log.
   * One of:
   *    "repair"
   *    "deep-scrub",
   *    "scrub
   *
   * Note: based on PG_STATE_REPAIR, and not on m_is_repair. I.e. for
   * auto_repair will show as "deep-scrub" and not as "repair" (until the first
   * error is detected).
   */
  std::string_view m_mode_desc;

  void update_op_mode_text();

  std::string_view get_op_mode_text() const final;

 private:
  /**
   * initiate a deep-scrub after the current scrub ended with errors.
   */
  void request_rescrubbing(requested_scrub_t& req_flags);

  /**
   * combine cluster & pool configuration options into a single struct
   * of scrub-related parameters.
   */
  Scrub::sched_conf_t populate_config_params() const;

  /**
   * determine the time when the next scrub should be scheduled
   *
   * based on the planned scrub's flags, time of last scrub, and
   * the pool's scrub configuration.
   */
  Scrub::sched_params_t determine_scrub_time(
      const pool_opts_t& pool_conf) const;

  /*
   * Select a range of objects to scrub.
   *
   * By:
   * - setting tentative range based on conf and divisor
   * - requesting a partial list of elements from the backend;
   * - handling some head/clones issues
   *
   * The selected range is set directly into 'm_start' and 'm_end'
   *
   * Returns std::nullopt if the range is busy otherwise returns the
   * number of objects in the range.
   */
  std::optional<uint64_t> select_range();

  std::list<Context*> m_callbacks;

  hobject_t m_max_end;	///< Largest end that may have been sent to replicas
  ScrubMapBuilder m_primary_scrubmap_pos;

  void _request_scrub_map(pg_shard_t replica,
			  eversion_t version,
			  hobject_t start,
			  hobject_t end,
			  bool deep,
			  bool allow_preemption);


  Scrub::MapsCollectionStatus m_maps_status;

  void persist_scrub_results(inconsistent_objs_t&& all_errors);
  void apply_snap_mapper_fixes(
    const std::vector<Scrub::snap_mapper_fix_t>& fix_list);

  // our latest periodic 'publish_stats_to_osd()'. Required frequency depends on
  // scrub state.
  ceph::coarse_real_clock::time_point m_last_stat_upd{};

  // ------------ members used if we are a replica

  epoch_t m_replica_min_epoch;	///< the min epoch needed to handle this message

  ScrubMapBuilder replica_scrubmap_pos;
  ScrubMap replica_scrubmap;

  // the backend, handling the details of comparing maps & fixing objects
  std::unique_ptr<ScrubBackend> m_be;

  /**
   * we mark the request priority as it arrived. It influences the queuing
   * priority when we wait for local updates
   */
  Scrub::scrub_prio_t m_replica_request_priority;

  /**
   * the 'preemption' "state-machine".
   * Note: I was considering an orthogonal sub-machine implementation, but as
   * the state diagram is extremely simple, the added complexity wasn't
   * justified.
   */
  class preemption_data_t : public Scrub::preemption_t {
   public:
    explicit preemption_data_t(PG* pg);	 // the PG access is used for conf
					 // access (and logs)

    [[nodiscard]] bool is_preemptable() const final { return m_preemptable; }

    preemption_data_t(const preemption_data_t&) = delete;
    preemption_data_t(preemption_data_t&&) = delete;

    bool do_preempt() final
    {
      if (m_preempted || !m_preemptable)
	return false;

      std::lock_guard<ceph::mutex> lk{m_preemption_lock};
      if (!m_preemptable)
	return false;

      m_preempted = true;
      return true;
    }

    /// same as 'do_preempt()' but w/o checks (as once a replica
    /// was preempted, we cannot continue)
    void replica_preempted() { m_preempted = true; }

    void enable_preemption()
    {
      std::lock_guard<ceph::mutex> lk{m_preemption_lock};
      if (are_preemptions_left() && !m_preempted) {
	m_preemptable = true;
      }
    }

    /// used by a replica to set preemptability state according to the Primary's
    /// request
    void force_preemptability(bool is_allowed)
    {
      // note: no need to lock for a replica
      m_preempted = false;
      m_preemptable = is_allowed;
    }

    bool disable_and_test() final
    {
      std::lock_guard<ceph::mutex> lk{m_preemption_lock};
      m_preemptable = false;
      return m_preempted;
    }

    [[nodiscard]] bool was_preempted() const { return m_preempted; }

    [[nodiscard]] size_t chunk_divisor() const { return m_size_divisor; }

    void reset();

    void adjust_parameters() final
    {
      std::lock_guard<ceph::mutex> lk{m_preemption_lock};

      if (m_preempted) {
	m_preempted = false;
	m_preemptable = adjust_left();
      } else {
	m_preemptable = are_preemptions_left();
      }
    }

   private:
    PG* m_pg;
    mutable ceph::mutex m_preemption_lock = ceph::make_mutex("preemption_lock");
    bool m_preemptable{false};
    bool m_preempted{false};
    int m_left;
    size_t m_size_divisor{1};
    bool are_preemptions_left() const { return m_left > 0; }

    bool adjust_left()
    {
      if (m_left > 0) {
	--m_left;
	m_size_divisor *= 2;
      }
      return m_left > 0;
    }
  };

  preemption_data_t preemption_data;
};
