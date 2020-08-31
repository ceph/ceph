// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "PG.h"
#include "ScrubStore.h"
#include "scrub_machine_lstnr.h"
#include "scrubber_common.h"

class Callback;


namespace Scrub {
class ScrubMachine;
struct BuildMap;
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


  // this flag indicates that we are scrubbing post repair to verify everything is fixed
  bool check_repair{false};

  /**
   * checked at the end of the scrub, to possibly initiate a deep-scrub
   */
  bool deep_scrub_on_error{false};  // RRR \todo handle the initialization of this one


  /**
   * the scrub session was originally marked 'must_scrub'. 'marked_must' is used
   * when determining sleep time for the scrubbing process. RRR not in original code
   */
  bool marked_must{false};
};

ostream& operator<<(ostream& out, const scrub_flags_t& sf);


/**
 * The part of PG-scrubbing code that's not a state-machine wiring.
 *
 * Why the separation? I wish to move to a different FSM implementation. Thus I
 * am forced to strongly decouple the state-machine implementation details from
 * the actual scrubbing code.
 */
class PgScrubber : public ScrubPgIF, public ScrubMachineListener {

 public:
  PgScrubber(PG* pg);

  friend struct Scrub::BuildMap;

  //
  //  ------------------------  the I/F exposed to the PG (ScrubPgIF)
  //

  void send_start_scrub() final;

  void send_sched_scrub() final;

  void send_scrub_resched() final;

  void active_pushes_notification() final;

  void update_applied_notification() final;

  void send_scrub_unblock() final;

  void digest_update_notification() final;

  void send_epoch_changed();

  void send_replica_maps_ready() final;

  /** we allow some number of preemptions of the scrub, which mean we do
   *  not block.  Then we start to block.  Once we start blocking, we do
   *  not stop until the scrub range is completed.
   */
  bool write_blocked_by_scrub(const hobject_t& soid) final;

  /// true if the given range intersects the scrub interval in any way
  bool range_intersects_scrub(const hobject_t& start, const hobject_t& end) final;


  void handle_scrub_reserve_request(OpRequestRef op) final;
  void handle_scrub_reserve_grant(OpRequestRef op, pg_shard_t from) final;
  void handle_scrub_reserve_reject(OpRequestRef op, pg_shard_t from) final;
  void handle_scrub_reserve_release(OpRequestRef op) final;
  void clear_scrub_reserved() final;  // PG::clear... fwds to here
  void scrub_unreserve_replicas() final;

  // managing scrub op registration
  bool is_scrub_registered() const final;

  // used if we are a replica

  void replica_scrub_op(OpRequestRef op) final;
  void replica_scrub(epoch_t epoch_queued) final;
  void replica_scrub_resched(epoch_t epoch_queued) final;

  /// the op priority, taken from the primary's request message
  Scrub::scrub_prio_t replica_op_priority() const final
  {
    return replica_request_priority_;
  };

  // unsigned int ongoing_priority() const final { return priority_; } const ;
  unsigned int scrub_requeue_priority(Scrub::scrub_prio_t with_priority) const final;
  // unsigned int scrub_requeue_priority(bool is_high_priority) const final;

  void queue_pushes_update(bool is_high_priority) final;
  void queue_pushes_update(Scrub::scrub_prio_t with_priority) final;


  //
  // ------------------------ the I/F used by scheduled OP items ------------------------
  //

  // void send_pushes_update([[maybe_unused]] ThreadPool::TPHandle &handle);


  // -------------------------------------------------------------------------------------------
  // the I/F used by the state-machine (i.e. the implementation of ScrubMachineListener)

  bool select_range() override;

  // walk the log to find the latest update that affects our chunk
  eversion_t search_log_for_updates() const override;

  eversion_t get_last_update_applied() const override
  {
    return pg_->recovery_state.get_last_update_applied();
  }

  void requeue_waiting() const override { pg_->requeue_ops(pg_->waiting_for_scrub); }

  int pending_active_pushes() const override { return pg_->active_pushes; }

  void scrub_compare_maps() override;

  void on_init() override;
  void on_replica_init() override;
  void replica_handling_done() override;

  void add_delayed_scheduling() final;

  /// \retval 'true' if a request was sent to at least one replica
  bool get_replicas_maps(bool replica_can_preempt) override;

  Scrub::FsmNext on_digest_updates() override;

  void send_replica_map(bool was_preempted) override;

  /**
   *  does the PG have newer updates than what we (the scrubber) know?
   */
  bool has_pg_marked_new_updates() const final;

  void set_subset_last_update(eversion_t e) final;

  void replica_update_start_epoch() override;

  void done_comparing_maps() final;

  Scrub::preemption_t* get_preemptor() final;

  int build_primary_map_chunk() override;  // -- fsm i/f --

  int build_replica_map_chunk(bool qu_priority) override;

 protected:
  // common code used by build_primary_map_chunk() and build_replica_map_chunk():
  int build_scrub_map_chunk(ScrubMap& map,  // primary or replica?
			    ScrubMapBuilder& pos,
			    hobject_t start,
			    hobject_t end,
			    bool deep);

 public:
  // -------------------------------------------------------------------------------------------


  void handle_query_state(Formatter* f);  // -- used by PG and derivatives --

  /**
   *  should we requeue blocked ops?
   *  Applicable to the PrimaryLogScrub derived class.
   */
  virtual bool should_requeue_blocked_ops(eversion_t last_recovery_applied)
  {
    return false;
  }

  friend ostream& operator<<(ostream& out, const PgScrubber& scrubber);


  // managing scrub op registration
  void reg_next_scrub(requested_scrub_t& request_flags, bool is_explicit);
  void unreg_next_scrub();
  void scrub_requested(bool deep,
		       bool repair,
		       bool need_auto,
		       requested_scrub_t& req_flags);

  /**
   * Reserve local scrub resources. Request reservations at replicas.
   *
   * Fail if OSD's local-scrubs budget was exhausted
   * \retval 'true' if local resources reserved.
   */
  bool reserve_local_n_remotes();

  bool state_test(uint64_t m) const { return pg_->state_test(m); }  // -- protected --
  void state_set(uint64_t m) { pg_->state_set(m); }
  void state_clear(uint64_t m) { pg_->state_clear(m); }

  void cleanup_on_finish();  // scrub_clear_state() as called for a Primary when
			     // Active->NotActive // -- private --
  void scrub_clear_state(bool has_error = false);
  virtual void _scrub_clear_state() {}

  /**
   *  add to scrub statistics, but only if the soid is below the scrub start
   */
  virtual void add_stats_if_lower(const object_stat_sum_t& delta_stats,
				  const hobject_t& soid)
  {
    ceph_assert(false);
  }  // -- used by the pglog --

  static utime_t scrub_must_stamp() { return utime_t(1, 1); }  // -- work needed --
  utime_t scrub_reg_stamp;   ///< stamp we registered for
  bool needs_sleep_{true};   ///< should we sleep before being rescheduled? always
			     ///< 'true', unless we just got out of a sleep period
  utime_t sleep_started_at;  // -- private --

  void reset_epoch(epoch_t epoch_queued);

  virtual ~PgScrubber();  // must be defined separately, in the .cc file

  bool was_epoch_changed();

  std::chrono::milliseconds fetch_sleep_needed();

  void set_op_parameters(requested_scrub_t& request);

  /*
   */
  void cleanup()
  {
    // RRR TBD
  }

  void reset();

  void _scan_snaps(ScrubMap& smap);  // private // note that the (non-standard for a
				     // non-virtual) name of the function is searched for
				     // by the QA standalone tests. Do not modify.

  void clean_meta_map(ScrubMap& for_meta_scrub);  // -- private --
  void run_callbacks();

  void cleanup_store(ObjectStore::Transaction* t);  // -- public - used by the Primlog --


  bool is_primary() const { return pg_->recovery_state.is_primary(); }

 private:
  bool is_event_relevant(epoch_t queued);
  bool scrub_process_inconsistent();


 public:
  /// handle a message carrying a replica map
  void map_from_replica(OpRequestRef op);


  // the part that actually finalizes a scrub
  void scrub_finish();


  // and a derivative-specific finishing touches:
  virtual void _scrub_finish() {}


  //  protected: but dev code needs access for now
  PG* const pg_;

 protected:
  /*
   * Validate consistency of the object info and snap sets.
   */
  virtual void scrub_snapshot_metadata(ScrubMap& map, const missing_map_t& missing_digest)
  {}

  std::unique_ptr<Scrub::ScrubMachine> fsm_;
  const spg_t pg_id_;  ///< a local copy of pg_->pg_id
  OSDService* const osds_;
  const pg_shard_t pg_whoami_;	///< a local copy of pg_->pg_whoami;

  epoch_t epoch_start_;	 ///< epoch when scrubbing was first scheduled
  epoch_t epoch_queued_;
  scrub_flags_t flags_;	 // RRR verify that only used by the primary

  bool active_{false};

 public:
  /*!
   * 'is_deep_' - is the running scrub a deep one?
   *
   * Note that most of the code directly checks PG_STATE_DEEP_SCRUB, which is primary-only
   * (and is set earlier - when scheduling the scrub). 'is_deep_' is meaningful both for
   * the primary and the replicas, and is used as a parameter when building the scrub
   * maps.
   */
  bool is_deep_{false};	 // -- fsm i/f --, but only for debugging

  bool is_scrub_active() const override;  // -- public --
  bool is_chunky_scrub_active() const override;

  eversion_t subset_last_update;  // -- used by the PG and by the FSM

  // Priority to use for scrub scheduling RRR
  // unsigned int priority_{0};
  bool is_must_{false};

  bool saved_req_scrub{false};	//  "We remember whether req_scrub was set when
				//  scrub_after_recovery set to true"
  bool req_scrub{false};

 private:
  inline static int fake_count{2};

  std::list<Context*> callbacks_;

  void message_all_replicas(int32_t opcode, std::string_view op_text);	// p
  void scrub_reserve_replicas();

 public:
  void add_callback(Context* context) { callbacks_.push_back(context); }

  bool are_callbacks_pending() const  // used for an assert in PG.cc
  {
    return !callbacks_.empty();
  }

 protected:
  std::unique_ptr<Scrub::Store> store_;
  int num_digest_updates_pending{0};  // -- protected --
  hobject_t start_, end_;	      ///< note:  [start,end)

 private:
  hobject_t max_end;  ///< Largest end that may have been sent to replicas
  ScrubMap primary_scrubmap;
  ScrubMapBuilder primary_scrubmap_pos;

  std::map<pg_shard_t, ScrubMap> received_maps;

  /// Cleaned std::map pending snap metadata scrub
  ScrubMap cleaned_meta_map;

  void _request_scrub_map(pg_shard_t replica,
			  eversion_t version,
			  hobject_t start,
			  hobject_t end,
			  bool deep,
			  bool allow_preemption);


 protected:
  /// Returns reference to current osdmap
  const OSDMapRef& get_osdmap() const;

  /// Returns epoch of current osdmap
  epoch_t get_osdmap_epoch() const { return get_osdmap()->get_epoch(); }

  CephContext* get_pg_cct() { return pg_->cct; }

  void send_start_replica();

  void send_sched_replica();



 public:
  omap_stat_t omap_stats = (const struct omap_stat_t){0};  // -- used by the PGBackend

  // note: the reservation state will be 'privatized' once the reservation process is
  // moved from the PG/OSD to the PgScrubber FSM
  std::set<pg_shard_t> reserved_peers;
  bool local_reserved{false};	// used by PG::sched_scrub()
  bool remote_reserved{false};	// protected
  bool reserve_failed{false};	// used by PG::sched_scrub()

  // collected statistics
  int shallow_errors{0};
  int deep_errors{0};
  int fixed_count{0};

  std::set<pg_shard_t> waiting_on_whom;

  /// Maps from objects with errors to missing peers
  std::map<hobject_t, std::set<pg_shard_t>> missing;

  /// Maps from objects with errors to inconsistent peers
  std::map<hobject_t, std::set<pg_shard_t>>
    inconsistent;  // -- used by the backend via the PG --

  /// Maps from object with errors to good peers
  std::map<hobject_t, std::list<std::pair<ScrubMap::object, pg_shard_t>>> authoritative_;

 private:
  // ------------ members used if we are a replica

  epoch_t replica_epoch_start_;	 // -- private --
  epoch_t replica_min_epoch_;	 ///< the min epoch needed to handle this message

  ScrubMapBuilder replica_scrubmap_pos;	 // RRR to document
  ScrubMap replica_scrubmap;		 // RRR to document
  /**
   * we mark the request priority as it arrived. It influences the queuing priority
   * when we wait for local updates
   */
  Scrub::scrub_prio_t replica_request_priority_;

  /**
   *  Queue a XX event to be sent to the replica, to trigger a re-check of the
   * availability of the scrub map prepared by the backend.
   */
  void requeue_replica(Scrub::scrub_prio_t is_high_priority);

 private:
  /*
   * the 'preemption' "state-machine".
   * Note: originally implemented as an orthogonal sub-machine. As the states diagram is
   * extremely simple, the added complexity was not justified.
   */
  class preemption_data_t : public Scrub::preemption_t {
   public:
    preemption_data_t(PG* pg);	// the PG access is used for conf access (and logs)

    bool is_preemptable() const final { return preemptable_; }

    bool do_preempt() final
    {
      if (preempted_ || !preemptable_)
	return false;

      std::lock_guard<std::mutex> lk{preemption_lock_};
      if (!preemptable_)
	return false;

      preempted_ = true;
      return true;
    }

    // same as 'do_preempt()' but w/o checks
    void replica_preempted() { preempted_ = true; }

    void enable_preemption()
    {
      if (are_preemptions_left()) {
	// std::lock_guard<std::mutex> lk{preemption_lock_};
	if (!preempted_)
	  preemptable_ = true;
      }
    }

    // used by a replica to set preemptability state according to the Primary's request
    void force_preemptability(bool is_allowed)
    {
      // no need to lock for a replica: std::lock_guard<std::mutex> lk{preemption_lock_};
      preempted_ = false;
      preemptable_ = is_allowed;
    }

    bool disable_and_test() final
    {
      std::lock_guard<std::mutex> lk{preemption_lock_};
      preemptable_ = false;
      return preempted_;
    }

    bool was_preempted() const { return preempted_; }

    size_t chunk_divisor() const { return size_divisor_; }

    void reset();

    void adjust_parameters() final
    {
      std::lock_guard<std::mutex> lk{preemption_lock_};

      if (preempted_) {
	preempted_ = false;
	preemptable_ = adjust_left();
      } else {
	preemptable_ = are_preemptions_left();
      }
    }

   private:
    PG* pg_;
    mutable std::mutex preemption_lock_;
    bool preemptable_{false};
    bool preempted_{false};
    int left_;
    size_t size_divisor_{1};
    bool are_preemptions_left() const { return left_ > 0; }

    bool adjust_left()
    {
      if (left_ > 0) {
	--left_;
	size_divisor_ *= 2;
      }
      return left_ > 0;
    }
  };

  preemption_data_t preemption_data;
};
