// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include "common/scrub_types.h"
#include "include/types.h"
#include "os/ObjectStore.h"

#include "OpRequest.h"

namespace ceph {
class Formatter;
}

namespace Scrub {

/// high/low OP priority
enum class scrub_prio_t : bool { low_priority = false, high_priority = true };

}  // namespace Scrub


/**
 * Flags affecting the scheduling and behaviour of the *next* scrub.
 *
 * we hold two of these flag collections: one
 * for the next scrub, and one frozen at initiation (i.e. in pg::queue_scrub())
 */
struct requested_scrub_t {

  // flags to indicate explicitly requested scrubs (by admin):
  // bool must_scrub, must_deep_scrub, must_repair, need_auto;

  /**
   * 'must_scrub' is set by an admin command (or by need_auto).
   *  Affects the priority of the scrubbing, and the sleep periods
   *  during the scrub.
   */
  bool must_scrub{false};

  /**
   * scrub must not be aborted.
   * Set for explicitly requested scrubs, and for scrubs originated by the pairing
   * process with the 'repair' flag set (in the RequestScrub event).
   *
   * Will be copied into the 'required' scrub flag upon scrub start.
   */
  bool req_scrub{false};

  /**
   * Set from:
   *  - scrub_requested() with need_auto param set, which only happens in
   *  - scrub_finish() - if deep_scrub_on_error is set, and we have errors
   *
   * If set, will prevent the OSD from casually postponing our scrub. When scrubbing
   * starts, will cause must_scrub, must_deep_scrub and auto_repair to be set.
   */
  bool need_auto{false};

  /**
   * Set for scrub-after-recovery just before we initiate the recovery deep scrub,
   * or if scrub_requested() was called with either need_auto ot repair.
   * Affects PG_STATE_DEEP_SCRUB.
   */
  bool must_deep_scrub{false};

  /**
   * (An intermediary flag used by pg::sched_scrub() on the first time
   * a planned scrub has all its resources). Determines whether the next
   * repair/scrub will be 'deep'.
   *
   * Note: 'dumped' by PgScrubber::dump() and such. In reality, being a
   * temporary that is set and reset by the same operation, will never
   * appear externally to be set
   */
  bool time_for_deep{false};

  bool deep_scrub_on_error{false};

  /**
   * If set, we should see must_deep_scrub and must_repair set, too
   *
   * - 'must_repair' is checked by the OSD when scheduling the scrubs.
   * - also checked & cleared at pg::queue_scrub()
   */
  bool must_repair{false};

  /*
   * the value of auto_repair is determined in sched_scrub() (once per scrub. previous
   * value is not remembered). Set if
   * - allowed by configuration and backend, and
   * - must_scrub is not set (i.e. - this is a periodic scrub),
   * - time_for_deep was just set
   */
  bool auto_repair{false};

  /**
   * indicating that we are scrubbing post repair to verify everything is fixed.
   * Otherwise - PG_STATE_FAILED_REPAIR will be asserted.
   */
  bool check_repair{false};
};

ostream& operator<<(ostream& out, const requested_scrub_t& sf);

/**
 *  The interface used by the PG when requesting scrub-related info or services
 */
struct ScrubPgIF {

  virtual ~ScrubPgIF() = default;

  friend ostream& operator<<(ostream& out, const ScrubPgIF& s) { return s.show(out); }

  virtual ostream& show(ostream& out) const = 0;

  // --------------- triggering state-machine events:

  virtual void initiate_regular_scrub(epoch_t epoch_queued) = 0;

  virtual void initiate_scrub_after_repair(epoch_t epoch_queued) = 0;

  virtual void send_scrub_resched(epoch_t epoch_queued) = 0;

  virtual void active_pushes_notification(epoch_t epoch_queued) = 0;

  virtual void update_applied_notification(epoch_t epoch_queued) = 0;

  virtual void digest_update_notification(epoch_t epoch_queued) = 0;

  virtual void send_scrub_unblock(epoch_t epoch_queued) = 0;

  virtual void send_replica_maps_ready(epoch_t epoch_queued) = 0;

  virtual void send_replica_pushes_upd(epoch_t epoch_queued) = 0;

  virtual void send_start_replica(epoch_t epoch_queued) = 0;

  virtual void send_sched_replica(epoch_t epoch_queued) = 0;

  virtual void send_full_reset(epoch_t epoch_queued) = 0;

  virtual void send_chunk_free(epoch_t epoch_queued) = 0;

  virtual void send_chunk_busy(epoch_t epoch_queued) = 0;

  virtual void send_local_map_done(epoch_t epoch_queued) = 0;

  virtual void send_get_next_chunk(epoch_t epoch_queued) = 0;

  virtual void send_scrub_is_finished(epoch_t epoch_queued) = 0;

  virtual void send_maps_compared(epoch_t epoch_queued) = 0;

  // --------------------------------------------------

  [[nodiscard]] virtual bool are_callbacks_pending()
    const = 0;	// currently only used for an assert

  /**
   * the scrubber is marked 'active':
   * - for the primary: when all replica OSDs grant us the requested resources
   * - for replicas: upon receiving the scrub request from the primary
   */
  [[nodiscard]] virtual bool is_scrub_active() const = 0;

  /// are we waiting for resource reservation grants form our replicas?
  [[nodiscard]] virtual bool is_reserving() const = 0;

  /// handle a message carrying a replica map
  virtual void map_from_replica(OpRequestRef op) = 0;

  virtual void replica_scrub_op(OpRequestRef op) = 0;

  virtual void set_op_parameters(requested_scrub_t&) = 0;

  virtual void scrub_clear_state() = 0;

  virtual void handle_query_state(ceph::Formatter* f) = 0;

  virtual void dump(ceph::Formatter* f) const = 0;

  /**
   * Return true if soid is currently being scrubbed and pending IOs should block.
   * May have a side effect of preempting an in-progress scrub -- will return false
   * in that case.
   *
   * @param soid object to check for ongoing scrub
   * @return boolean whether a request on soid should block until scrub completion
   */
  virtual bool write_blocked_by_scrub(const hobject_t& soid) = 0;

  /// Returns whether any objects in the range [begin, end] are being scrubbed
  virtual bool range_intersects_scrub(const hobject_t& start, const hobject_t& end) = 0;

  /// the op priority, taken from the primary's request message
  virtual Scrub::scrub_prio_t replica_op_priority() const = 0;

  /// the priority of the on-going scrub (used when requeuing events)
  virtual unsigned int scrub_requeue_priority(
    Scrub::scrub_prio_t with_priority) const = 0;
  virtual unsigned int scrub_requeue_priority(Scrub::scrub_prio_t with_priority,
					      unsigned int suggested_priority) const = 0;

  virtual void add_callback(Context* context) = 0;

  /// should we requeue blocked ops?
  [[nodiscard]] virtual bool should_requeue_blocked_ops(
    eversion_t last_recovery_applied) const = 0;

  /// add to scrub statistics, but only if the soid is below the scrub start
  virtual void stats_of_handled_objects(const object_stat_sum_t& delta_stats,
					const hobject_t& soid) = 0;

  /**
   * the version of 'scrub_clear_state()' that does not try to invoke FSM services
   * (thus can be called from FSM reactions)
   */
  virtual void clear_pgscrub_state() = 0;

  /**
   *  triggers the 'RemotesReserved' (all replicas granted scrub resources)
   *  state-machine event
   */
  virtual void send_remotes_reserved(epoch_t epoch_queued) = 0;

  /**
   * triggers the 'ReservationFailure' (at least one replica denied us the requested
   * resources) state-machine event
   */
  virtual void send_reservation_failure(epoch_t epoch_queued) = 0;

  virtual void cleanup_store(ObjectStore::Transaction* t) = 0;

  virtual bool get_store_errors(const scrub_ls_arg_t& arg,
				scrub_ls_result_t& res_inout) const = 0;

  // --------------- reservations -----------------------------------

  /**
   *  message all replicas with a request to "unreserve" scrub
   */
  virtual void unreserve_replicas() = 0;

  /**
   *  "forget" all replica reservations. No messages are sent to the
   *  previously-reserved.
   *
   *  Used upon interval change. The replicas' state is guaranteed to
   *  be reset separately by the interval-change event.
   */
  virtual void discard_replica_reservations() = 0;

  /**
   * clear both local and OSD-managed resource reservation flags
   */
  virtual void clear_scrub_reservations() = 0;

  /**
   * Reserve local scrub resources (managed by the OSD)
   *
   * Fails if OSD's local-scrubs budget was exhausted
   * \returns were local resources reserved?
   */
  virtual bool reserve_local() = 0;

  // on the replica:
  virtual void handle_scrub_reserve_request(OpRequestRef op) = 0;
  virtual void handle_scrub_reserve_release(OpRequestRef op) = 0;

  // and on the primary:
  virtual void handle_scrub_reserve_grant(OpRequestRef op, pg_shard_t from) = 0;
  virtual void handle_scrub_reserve_reject(OpRequestRef op, pg_shard_t from) = 0;

  virtual void reg_next_scrub(const requested_scrub_t& request_flags) = 0;
  virtual void unreg_next_scrub() = 0;
  virtual void scrub_requested(scrub_level_t scrub_level,
			       scrub_type_t scrub_type,
			       requested_scrub_t& req_flags) = 0;

  // --------------- debugging via the asok ------------------------------

  virtual int asok_debug(std::string_view cmd,
			 std::string param,
			 Formatter* f,
			 stringstream& ss) = 0;
};
