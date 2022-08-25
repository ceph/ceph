// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <fmt/ranges.h>

#include "common/scrub_types.h"
#include "include/types.h"
#include "os/ObjectStore.h"

#include "OpRequest.h"

namespace ceph {
class Formatter;
}

struct PGPool;

namespace Scrub {
  class ReplicaReservations;
}

/// Facilitating scrub-realated object access to private PG data
class ScrubberPasskey {
private:
  friend class Scrub::ReplicaReservations;
  friend class PrimaryLogScrub;
  friend class PgScrubber;
  friend class ScrubBackend;
  ScrubberPasskey() {}
  ScrubberPasskey(const ScrubberPasskey&) = default;
  ScrubberPasskey& operator=(const ScrubberPasskey&) = delete;
};

namespace Scrub {

/// high/low OP priority
enum class scrub_prio_t : bool { low_priority = false, high_priority = true };

/// Identifies a specific scrub activation within an interval,
/// see ScrubPGgIF::m_current_token
using act_token_t = uint32_t;

/// "environment" preconditions affecting which PGs are eligible for scrubbing
struct ScrubPreconds {
  bool allow_requested_repair_only{false};
  bool load_is_low{true};
  bool time_permit{true};
  bool only_deadlined{false};
};

/// PG services used by the scrubber backend
struct PgScrubBeListener {
  virtual ~PgScrubBeListener() = default;

  virtual const PGPool& get_pgpool() const = 0;
  virtual pg_shard_t get_primary() const = 0;
  virtual void force_object_missing(ScrubberPasskey,
                                    const std::set<pg_shard_t>& peer,
                                    const hobject_t& oid,
                                    eversion_t version) = 0;
  virtual const pg_info_t& get_pg_info(ScrubberPasskey) const = 0;

  // query the PG backend for the on-disk size of an object
  virtual uint64_t logical_to_ondisk_size(uint64_t logical_size) const = 0;

  // used to verify our "cleaness" before scrubbing
  virtual bool is_waiting_for_unreadable_object() const = 0;
};

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
   * Set for explicitly requested scrubs, and for scrubs originated by the
   * pairing process with the 'repair' flag set (in the RequestScrub event).
   *
   * Will be copied into the 'required' scrub flag upon scrub start.
   */
  bool req_scrub{false};

  /**
   * Set from:
   *  - scrub_requested() with need_auto param set, which only happens in
   *  - scrub_finish() - if deep_scrub_on_error is set, and we have errors
   *
   * If set, will prevent the OSD from casually postponing our scrub. When
   * scrubbing starts, will cause must_scrub, must_deep_scrub and auto_repair to
   * be set.
   */
  bool need_auto{false};

  /**
   * Set for scrub-after-recovery just before we initiate the recovery deep
   * scrub, or if scrub_requested() was called with either need_auto ot repair.
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
   * If set, we should see must_deep_scrub & must_scrub, too
   *
   * - 'must_repair' is checked by the OSD when scheduling the scrubs.
   * - also checked & cleared at pg::queue_scrub()
   */
  bool must_repair{false};

  /*
   * the value of auto_repair is determined in sched_scrub() (once per scrub.
   * previous value is not remembered). Set if
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

  /**
   * Used to indicate, both in client-facing listings and internally, that
   * the planned scrub will be a deep one.
   */
  bool calculated_to_deep{false};
};

std::ostream& operator<<(std::ostream& out, const requested_scrub_t& sf);

template <>
struct fmt::formatter<requested_scrub_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const requested_scrub_t& rs, FormatContext& ctx)
  {
    return fmt::format_to(ctx.out(),
                          "(plnd:{}{}{}{}{}{}{}{}{}{})",
                          rs.must_repair ? " must_repair" : "",
                          rs.auto_repair ? " auto_repair" : "",
                          rs.check_repair ? " check_repair" : "",
                          rs.deep_scrub_on_error ? " deep_scrub_on_error" : "",
                          rs.must_deep_scrub ? " must_deep_scrub" : "",
                          rs.must_scrub ? " must_scrub" : "",
                          rs.time_for_deep ? " time_for_deep" : "",
                          rs.need_auto ? " need_auto" : "",
                          rs.req_scrub ? " req_scrub" : "",
                          rs.calculated_to_deep ? " deep" : "");
  }
};

/**
 *  The interface used by the PG when requesting scrub-related info or services
 */
struct ScrubPgIF {

  virtual ~ScrubPgIF() = default;

  friend std::ostream& operator<<(std::ostream& out, const ScrubPgIF& s)
  {
    return s.show(out);
  }

  virtual std::ostream& show(std::ostream& out) const = 0;

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

  virtual void send_start_replica(epoch_t epoch_queued,
				  Scrub::act_token_t token) = 0;

  virtual void send_sched_replica(epoch_t epoch_queued,
				  Scrub::act_token_t token) = 0;

  virtual void send_full_reset(epoch_t epoch_queued) = 0;

  virtual void send_chunk_free(epoch_t epoch_queued) = 0;

  virtual void send_chunk_busy(epoch_t epoch_queued) = 0;

  virtual void send_local_map_done(epoch_t epoch_queued) = 0;

  virtual void send_get_next_chunk(epoch_t epoch_queued) = 0;

  virtual void send_scrub_is_finished(epoch_t epoch_queued) = 0;

  virtual void send_maps_compared(epoch_t epoch_queued) = 0;

  virtual void on_applied_when_primary(const eversion_t& applied_version) = 0;

  // --------------------------------------------------

  [[nodiscard]] virtual bool are_callbacks_pending() const = 0;	 // currently
								 // only used
								 // for an
								 // assert

  /**
   * the scrubber is marked 'active':
   * - for the primary: when all replica OSDs grant us the requested resources
   * - for replicas: upon receiving the scrub request from the primary
   */
  [[nodiscard]] virtual bool is_scrub_active() const = 0;

  /**
   * 'true' until after the FSM processes the 'scrub-finished' event,
   * and scrubbing is completely cleaned-up.
   *
   * In other words - holds longer than is_scrub_active(), thus preventing
   * a rescrubbing of the same PG while the previous scrub has not fully
   * terminated.
   */
  [[nodiscard]] virtual bool is_queued_or_active() const = 0;

  /**
   * Manipulate the 'scrubbing request has been queued, or - we are
   * actually scrubbing' Scrubber's flag
   *
   * clear_queued_or_active() will also restart any blocked snaptrimming.
   */
  virtual void set_queued_or_active() = 0;
  virtual void clear_queued_or_active() = 0;

  /// are we waiting for resource reservation grants form our replicas?
  [[nodiscard]] virtual bool is_reserving() const = 0;

  /// handle a message carrying a replica map
  virtual void map_from_replica(OpRequestRef op) = 0;

  virtual void replica_scrub_op(OpRequestRef op) = 0;

  virtual void set_op_parameters(const requested_scrub_t&) = 0;

  virtual void scrub_clear_state() = 0;

  virtual void handle_query_state(ceph::Formatter* f) = 0;

  virtual pg_scrubbing_status_t get_schedule() const = 0;

  virtual void dump_scrubber(ceph::Formatter* f,
			     const requested_scrub_t& request_flags) const = 0;

  /**
   * Return true if soid is currently being scrubbed and pending IOs should
   * block. May have a side effect of preempting an in-progress scrub -- will
   * return false in that case.
   *
   * @param soid object to check for ongoing scrub
   * @return boolean whether a request on soid should block until scrub
   * completion
   */
  virtual bool write_blocked_by_scrub(const hobject_t& soid) = 0;

  /// Returns whether any objects in the range [begin, end] are being scrubbed
  virtual bool range_intersects_scrub(const hobject_t& start,
				      const hobject_t& end) = 0;

  /// the op priority, taken from the primary's request message
  virtual Scrub::scrub_prio_t replica_op_priority() const = 0;

  /// the priority of the on-going scrub (used when requeuing events)
  virtual unsigned int scrub_requeue_priority(
    Scrub::scrub_prio_t with_priority) const = 0;
  virtual unsigned int scrub_requeue_priority(
    Scrub::scrub_prio_t with_priority,
    unsigned int suggested_priority) const = 0;

  virtual void add_callback(Context* context) = 0;

  /// add to scrub statistics, but only if the soid is below the scrub start
  virtual void stats_of_handled_objects(const object_stat_sum_t& delta_stats,
					const hobject_t& soid) = 0;

  /**
   * the version of 'scrub_clear_state()' that does not try to invoke FSM
   * services (thus can be called from FSM reactions)
   */
  virtual void clear_pgscrub_state() = 0;

  /**
   *  triggers the 'RemotesReserved' (all replicas granted scrub resources)
   *  state-machine event
   */
  virtual void send_remotes_reserved(epoch_t epoch_queued) = 0;

  /**
   * triggers the 'ReservationFailure' (at least one replica denied us the
   * requested resources) state-machine event
   */
  virtual void send_reservation_failure(epoch_t epoch_queued) = 0;

  virtual void cleanup_store(ObjectStore::Transaction* t) = 0;

  virtual bool get_store_errors(const scrub_ls_arg_t& arg,
				scrub_ls_result_t& res_inout) const = 0;

  /**
   * force a periodic 'publish_stats_to_osd()' call, to update scrub-related
   * counters and statistics.
   */
  virtual void update_scrub_stats(
    ceph::coarse_real_clock::time_point now_is) = 0;

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

  /**
   * Register/de-register with the OSD scrub queue
   *
   * Following our status as Primary or replica.
   */
  virtual void on_primary_change(const requested_scrub_t& request_flags) = 0;

  /**
   * Recalculate the required scrub time.
   *
   * This function assumes that the queue registration status is up-to-date,
   * i.e. the OSD "knows our name" if-f we are the Primary.
   */
  virtual void update_scrub_job(const requested_scrub_t& request_flags) = 0;

  virtual void on_maybe_registration_change(
    const requested_scrub_t& request_flags) = 0;

  // on the replica:
  virtual void handle_scrub_reserve_request(OpRequestRef op) = 0;
  virtual void handle_scrub_reserve_release(OpRequestRef op) = 0;

  // and on the primary:
  virtual void handle_scrub_reserve_grant(OpRequestRef op, pg_shard_t from) = 0;
  virtual void handle_scrub_reserve_reject(OpRequestRef op,
					   pg_shard_t from) = 0;

  virtual void rm_from_osd_scrubbing() = 0;

  virtual void scrub_requested(scrub_level_t scrub_level,
			       scrub_type_t scrub_type,
			       requested_scrub_t& req_flags) = 0;

  // --------------- debugging via the asok ------------------------------

  virtual int asok_debug(std::string_view cmd,
			 std::string param,
			 Formatter* f,
			 std::stringstream& ss) = 0;
};
