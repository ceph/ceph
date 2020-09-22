// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "include/types.h"
#include "os/ObjectStore.h"

#include "OpRequest.h"

namespace ceph {
class Formatter;
}

namespace Scrub {

/// high/low OP priority
enum class scrub_prio_t : bool { low_priority = false, high_priority = true };

///

}  // namespace Scrub


// we hold two of these flag collections: one
// for the next scrub, and one frozen at initiation (pg::queue_scrub())

struct requested_scrub_t {

  // flags to indicate explicitly requested scrubs (by admin)
  // bool must_scrub, must_deep_scrub, must_repair, need_auto;

  /*
   * 'must_scrub' is set by an admin command (or - by need_auto).
   *  Affects the priority of the scrubbing, and the sleep periods
   *  during the scrub.
   */
  bool must_scrub{false};

  /*
   * Set from:
   *  - scrub_requested() with need_auto param set, which only happens in
   *  - scrub_finish() - if deep_scrub_on_error is set, aand we have errors
   *
   * If set, will prevent the OSD from casually postponing our scrub. When scrubbing
   * starts, will cause must_scrub, must_deep_scrub and auto_repair to be set.
   */
  bool need_auto{false};

  bool must_deep_scrub{false};	// used also here. Not just in scrubber.flags_

  /*
   * (An intermediary flag used by pg::sched_scrub() on the first time
   * a planned scrub has all its resources)
   *
   * Set by is_time_for_deep(). Determines whether the next repair/scrub will
   * be 'deep'.
   */
  bool time_for_deep{false};

  bool deep_scrub_on_error{false};

  /*
   * If set, we should see must_deep_scrub and must_repair set, too
   *
   * - 'must_repair' is checked by the OSD when scheduling the scrubs.
   * - also checked & cleared at pg::queue_scrub()
   * - in pg::sched_scrub(), on 1'st call:
   */
  bool must_repair{false};

  /*
   * the value of auto_repair is determined in sched_scrub() (once per scrub. previous
   * value is not remembered). Set if
   * - (previous value is not remembered)
   * - allowed by configuration and backend, and
   * - must_scrub is not set (i.e. - this is a periodic scrub),
   * - time_for_deep was just set
   */
  bool auto_repair{false};

  // unsigned int priority;

  /// indicating that we are scrubbing post repair to verify everything is fixed
  bool check_repair{false};

  /*
   * scrub_after_recovery is only set at the final stages of a scrub, if:
   * - errors were found, and
   * - not all of those errors were fixed during the scrub, and
   * - PG_STATE_REPAIR is set (i.e. - RRR
   * - the errors are not all unfixable
   *
   * the flag is checked at:
   * - repair end - causing a 'queue_scrub'
   * - RRR I do not understand the check at PrimaryLogPG::recover_missing()
   */
  // should really be in pg itself: bool scrub_after_recovery{false};
};

ostream& operator<<(ostream& out, const requested_scrub_t& sf);

class ObjectStore;

/**
 *  The interface used by the PG when requesting scrub-related info or services
 */
struct ScrubPgIF {

  virtual ~ScrubPgIF(){};

  friend ostream& operator<<(ostream& out, const ScrubPgIF& s) { return s.show(out); }

  virtual ostream& show(ostream& out) const = 0;

  // --------------- triggering state-machine events:

  virtual void send_start_scrub() = 0;

  virtual void send_start_after_rec() = 0;

  virtual void send_sched_scrub() = 0;

  virtual void send_scrub_resched() = 0;

  virtual void replica_scrub_resched(epoch_t epoch_queued) = 0;

  virtual void active_pushes_notification() = 0;

  virtual void update_applied_notification() = 0;

  virtual void digest_update_notification() = 0;

  virtual void send_scrub_unblock() = 0;

  virtual void send_replica_maps_ready() = 0;


  // --------------------------------------------------

  virtual void reset_epoch(epoch_t epoch_queued) = 0;

  virtual bool are_callbacks_pending() const = 0;  // currently used only for an assert

  virtual bool is_scrub_active() const = 0;  // RRR must doc

  /// are we waiting for resource reservation grants form our replicas?
  virtual bool is_reserving() const = 0;

  /// handle a message carrying a replica map
  virtual void map_from_replica(OpRequestRef op) = 0;

  virtual void replica_scrub_op(OpRequestRef op) = 0;

  virtual void replica_scrub(epoch_t epoch_queued) = 0;

  virtual void set_op_parameters(requested_scrub_t&) = 0;

  virtual void scrub_clear_state(bool keep_repair_state = false) = 0;

  virtual void handle_query_state(ceph::Formatter* f) = 0;

  /**
   * we allow some number of preemptions of the scrub, which mean we do
   *  not block.  Then we start to block.  Once we start blocking, we do
   *  not stop until the scrub range is completed.
   */
  virtual bool write_blocked_by_scrub(const hobject_t& soid) = 0;

  /// true if the given range intersects the scrub interval in any way
  virtual bool range_intersects_scrub(const hobject_t& start, const hobject_t& end) = 0;

  /// the op priority, taken from the primary's request message
  virtual Scrub::scrub_prio_t replica_op_priority() const = 0;

  /// the priority of the on-going scrub (used when requeuing events)
  virtual unsigned int scrub_requeue_priority(
    Scrub::scrub_prio_t with_priority) const = 0;
  virtual unsigned int scrub_requeue_priority(Scrub::scrub_prio_t with_priority,
					      unsigned int suggested_priority) const = 0;

  virtual void queue_pushes_update(bool is_high_priority) = 0;
  virtual void queue_pushes_update(Scrub::scrub_prio_t with_priority) = 0;

  virtual void add_callback(Context* context) = 0;

  /// should we requeue blocked ops?
  virtual bool should_requeue_blocked_ops(eversion_t last_recovery_applied) = 0;


  // --------------- until after we move the reservations flow into the FSM:

  /**
   *  message all replicas with a request to "unreserve" scrub
   */
  virtual void unreserve_replicas() = 0;

  /**
   * clear both local and OSD-managed resource reservation flags
   * (note: no replica res/unres messages are involved!)
   */
  virtual void clear_scrub_reservations() = 0;

  /**
   * Reserve local scrub resources (managed by the OSD)
   *
   * Fail if OSD's local-scrubs budget was exhausted
   * \retval 'true' if local resources reserved.
   */
  virtual bool reserve_local() = 0;

  // on the replica:
  virtual void handle_scrub_reserve_request(OpRequestRef op) = 0;
  virtual void handle_scrub_reserve_release(OpRequestRef op) = 0;

  // and on the primary:
  virtual void handle_scrub_reserve_grant(OpRequestRef op, pg_shard_t from) = 0;
  virtual void handle_scrub_reserve_reject(OpRequestRef op, pg_shard_t from) = 0;


  // --------------- until after we move the scrub OP registration flow into the FSM:

  virtual bool is_scrub_registered() const = 0;
  virtual void reg_next_scrub(requested_scrub_t& request_flags, bool is_explicit) = 0;
  virtual void unreg_next_scrub() = 0;
  virtual void scrub_requested(bool deep,
			       bool repair,
			       bool need_auto,
			       requested_scrub_t& req_flags) = 0;

  // -------------------------------------------------------


  virtual bool is_chunky_scrub_active() const = 0;

  /// add to scrub statistics, but only if the soid is below the scrub start
  virtual void add_stats_if_lower(const object_stat_sum_t& delta_stats,
				  const hobject_t& soid) = 0;

  /// the version of 'scrub_clear_state()' that does not try to invoke FSM services
  /// (thus can be called from FSM reactions)
  virtual void clear_pgscrub_state(bool keep_repair_state) = 0;

  /**
   *  triggers the 'RemotesReserved' (all replicas granted scrub resources)
   *  state-machine event
   */
  virtual void send_remotes_reserved() = 0;

  /**
   *  triggers the 'ReservationFailure' (at least one replica denied us the requested
   * resources) state-machine event
   */
  virtual void send_reservation_failure() = 0;

  virtual void cleanup_store(ObjectStore::Transaction* t) = 0;

  /*
   require work:

  - access to saved_req_scrub - will be reworked as part of the 'abort' integration
  - access to req_scrub - will be reworked as part of the 'abort' integration
  - clear_scrub_reservations()
  */
};
