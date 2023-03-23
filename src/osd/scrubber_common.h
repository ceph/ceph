// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <fmt/ranges.h>

#include "common/scrub_types.h"
#include "include/expected.hpp"
#include "include/types.h"
#include "os/ObjectStore.h"

#include "OpRequest.h"

namespace ceph {
class Formatter;
}

struct PGPool;

namespace Scrub {
class ReplicaReservations;

// possible outcome when trying to select a PG and scrub it
enum class schedule_result_t {
  /// an internal, temporary, state:
  ok_thus_far,

  /// for a failure of a specific combination of PG & level:
  target_failure,

  /// for failure modes that are not related to a specific scrub target
  failure,
};

class SchedTarget;
struct SchedEntry;
}  // namespace Scrub

/// Facilitating scrub-related object access to private PG data
class ScrubberPasskey {
private:
  friend class Scrub::ReplicaReservations;
  friend class PrimaryLogScrub;
  friend class PgScrubber;
  friend class ScrubBackend;
  friend class ScrubQueue;
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

/*
 * Identifying an instance of a 'scrub scheduling loop' - the
 * OSD tick-initiated traversing the scrub queue, trying to start a scrub
 * session on each one in turn. Note that a common failure mode is the
 * failure to secure replicas reservations. The failure in that case is
 * asynchronous. For the 'scheduling loop' to continue, the failing PG
 * must notify the ScrubQueue. The token is used to identify the specific
 * scheduling loop instance, but is also used (in the ScrubQueue code,
 * where it is not 'opaque') to note that start time of the loop.
 */
using loop_token_t = utime_t;

/// "environment" preconditions affecting which PGs are eligible for scrubbing
struct OSDRestrictions {
  bool allow_requested_repair_only{false};
  bool load_is_low{true};
  bool time_permit{true};
  bool only_deadlined{false};
};

/// concise passing of PG state re scrubbing to the
/// scrubber at initiation of a scrub
struct ScrubPGPreconds {
  bool allow_shallow{true};
  bool allow_deep{true};
  bool has_deep_errors{false};
  bool can_autorepair{false};
};

/// a 'shorthand' for comparing a random number against a
/// probability threshold
static inline bool random_bool_with_probability(double probability)
{
  return (ceph::util::generate_random_number<double>(0.0, 1.0) < probability);
}

}  // namespace Scrub

template <>
struct fmt::formatter<Scrub::OSDRestrictions> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const Scrub::OSDRestrictions& conds, FormatContext& ctx)
  {
    return fmt::format_to(
      ctx.out(),
      "overdue-only:{} load:{} time:{} repair-only:{}",
        conds.only_deadlined,
        conds.load_is_low ? "ok" : "high",
        conds.time_permit ? "ok" : "no",
        conds.allow_requested_repair_only);
  }
};

template <>
struct fmt::formatter<Scrub::ScrubPGPreconds> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const Scrub::ScrubPGPreconds& conds, FormatContext& ctx)
  {
    return fmt::format_to(
      ctx.out(),
      "allowed:{}/{} err:{} autorp:{}",
        conds.allow_shallow ? "+" : "-",
        conds.allow_deep ? "+" : "-",
        conds.has_deep_errors ? "+" : "-",
        conds.can_autorepair ? "+" : "-");
  }
};


namespace Scrub {

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

  // used to verify our "cleanliness" before scrubbing
  virtual bool is_waiting_for_unreadable_object() const = 0;
};

}  // namespace Scrub


/**
 *  The interface used by the PG when requesting scrub-related info or services
 */
struct ScrubPgIF {

  virtual ~ScrubPgIF() = default;

  friend std::ostream& operator<<(std::ostream& out, const ScrubPgIF& s)
  {
    return s.show_concise(out);
  }

  virtual std::ostream& show_concise(std::ostream& out) const = 0;

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

  virtual void send_chunk_free(epoch_t epoch_queued) = 0;

  virtual void send_chunk_busy(epoch_t epoch_queued) = 0;

  virtual void send_local_map_done(epoch_t epoch_queued) = 0;

  virtual void send_get_next_chunk(epoch_t epoch_queued) = 0;

  virtual void send_scrub_is_finished(epoch_t epoch_queued) = 0;

  virtual void on_applied_when_primary(const eversion_t& applied_version) = 0;

  // --------------------------------------------------

  /**
   * Start a scrub operation.
   *
   * @param [in] level Level of scrub (deep or shallow)
   * @param [in] pg_cond conveying the PG state re scrubbing
   * @param [in] preconds a set of flags determined based on environment
   *       conditions (time & load) that might restrict scrub's level/urgency
   * @returns either 'scrub_initiated' or 'failure'
   */
  virtual void start_scrubbing(
      scrub_level_t lvl,
      Scrub::loop_token_t loop_id,
      Scrub::OSDRestrictions preconds,
      Scrub::ScrubPGPreconds pg_cond) = 0;

  /**
   * let the scrubber know that a recovery operation has completed.
   * This might trigger an 'after repair' scrub.
   */
  virtual void recovery_completed() = 0;

  virtual bool is_after_repair_required() const = 0;

  // currently only used for an assertion:
  [[nodiscard]] virtual bool are_callbacks_pending() const = 0;	

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
   * a re-scrubbing of the same PG while the previous scrub has not fully
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

  /// stop any active scrubbing (on interval end) and unregister from
  /// the OSD scrub queue
  virtual void on_new_interval() = 0;

  virtual void scrub_clear_state() = 0;

  virtual pg_scrubbing_status_t get_schedule() const = 0;

  //// perform 'scrub'/'deep_scrub' asok commands
  virtual void on_operator_periodic_cmd(
    ceph::Formatter* f,
    scrub_level_t scrub_level,
    int64_t offset) = 0;

  virtual void on_operator_forced_scrub(
    ceph::Formatter* f,
    scrub_level_t scrub_level
    ) = 0;

  virtual void dump_scrubber(ceph::Formatter* f) const = 0;

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

  /**
   * scrub scheduling configuration has changed. Update our scrub-queue
   * entries accordingly.
   */
  virtual void recalc_schedule() = 0;

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
   * if activated as a Primary - register the scrub job with the OSD
   * scrub queue
   */
  virtual void on_pg_activate() = 0;

  /**
   * Recalculate the required scrub time.
   *
   * This function assumes that the queue registration status is up-to-date,
   * i.e. the OSD "knows our name" if-f we are the Primary.
   */
  //virtual void update_scrub_job() = 0;

  // on the replica:
  virtual void handle_scrub_reserve_request(OpRequestRef op) = 0;
  virtual void handle_scrub_reserve_release(OpRequestRef op) = 0;

  // and on the primary:
  virtual void handle_scrub_reserve_grant(OpRequestRef op, pg_shard_t from) = 0;
  virtual void handle_scrub_reserve_reject(OpRequestRef op,
					   pg_shard_t from) = 0;

  virtual void rm_from_osd_scrubbing() = 0;

  /// returns the requested scrub's level (which is shallow
  /// only if scrub_level is shallow *and* no repair is requested)
  virtual scrub_level_t scrub_requested(
      scrub_level_t scrub_level,
      scrub_type_t scrub_type) = 0;

  // --------------- debugging via the asok ------------------------------

  virtual int asok_debug(
      std::string_view prefix,
      std::string_view cmd,
      std::string_view param,
      Formatter* f,
      std::stringstream& ss) = 0;
};
