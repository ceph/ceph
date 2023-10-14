// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <cassert>
#include <chrono>
#include <optional>
#include <string_view>
#include <vector>

#include "osd/scrubber_common.h"

#include "osd_scrub_sched.h"
#include "scrub_machine_lstnr.h"

namespace Scrub {

/**
 * Reserving/freeing scrub resources at the replicas.
 *
 * When constructed - sends reservation requests to the acting_set OSDs, one
 * by one.
 * Once a replica's OSD replies with a 'grant'ed reservation, we send a
 * reservation request to the next replica.
 * A rejection triggers a "couldn't acquire the replicas' scrub resources"
 * event. All granted reservations are released.
 *
 * Reserved replicas should be released at the end of the scrub session. The
 * one exception is if the scrub terminates upon an interval change. In that
 * scenario - the replicas discard their reservations on their own accord
 * when noticing the change in interval, and there is no need (and no
 * guaranteed way) to send them the release message.
 *
 * Timeouts:
 *
 *  Slow-Secondary Warning:
 *  Warn if a replica takes more than <conf> milliseconds to reply to a
 *  reservation request. Only one warning is issued per session.
 *
 *  Reservation Timeout:
 *  We limit the total time we wait for the replicas to respond to the
 *  reservation request. If the reservation back-and-forth does not complete
 *  within <conf> milliseconds, we give up and release all the reservations
 *  that have been acquired until that moment.
 *  (Why? because we have encountered instances where a reservation request was
 *  lost - either due to a bug or due to a network issue.)
 */
class ReplicaReservations {
  using clock = ceph::coarse_real_clock;

  ScrubMachineListener& m_scrubber;
  PG* m_pg;

  /// shorthand for m_scrubber.get_spgid().pgid
  const pg_t m_pgid;

  /// for dout && when queueing messages to the FSM
  OSDService* m_osds;

  /// the acting set (not including myself), sorted by pg_shard_t
  std::vector<pg_shard_t> m_sorted_secondaries;

  /// the next replica to which we will send a reservation request
  std::vector<pg_shard_t>::const_iterator m_next_to_request;

  /// for logs, and for detecting slow peers
  clock::time_point m_last_request_sent_at;

  /// the 'slow response' timeout (in milliseconds) - as configured.
  /// Doubles as a 'do once' flag for the warning.
  std::chrono::milliseconds m_slow_response_warn_timeout;

 public:
  ReplicaReservations(ScrubMachineListener& scrubber);

  ~ReplicaReservations();

  /**
   * The OK received from the replica (after verifying that it is indeed
   * the replica we are expecting a reply from) is noted, and triggers
   * one of two: either sending a reservation request to the next replica,
   * or notifying the scrubber that we have reserved them all.
   *
   * \returns true if there are no more replicas to send reservation requests
   * (i.e., the scrubber should proceed to the next phase), false otherwise.
   */
  bool handle_reserve_grant(OpRequestRef op, pg_shard_t from);

  /**
   * Verify that the sender of the received rejection is the replica we
   * were expecting a reply from.
   * If this is so - just mark the fact that the specific peer need not
   * be released.
   *
   * Note - the actual handling of scrub session termination and of
   * releasing the reserved replicas is done by the caller (the FSM).
   */
  void verify_rejections_source(OpRequestRef op, pg_shard_t from);

  /**
   * Notifies implementation that it is no longer responsible for releasing
   * tracked remote reservations.
   *
   * The intended usage is upon interval change.  In general, replicas are
   * responsible for releasing their own resources upon interval change without
   * coordination from the primary.
   *
   * Sends no messages.
   */
  void discard_remote_reservations();

  /// the only replica we are expecting a reply from
  std::optional<pg_shard_t> get_last_sent() const;

  // note: 'public', as accessed via the 'standard' dout_prefix() macro
  std::ostream& gen_prefix(std::ostream& out, std::string fn) const;

 private:
  /// send 'release' messages to all replicas we have managed to reserve
  void release_all();

  /// The number of requests that have been sent (and not rejected) so far.
  size_t active_requests_cnt() const;

  /**
   * Send a reservation request to the next replica.
   * - if there are no more replicas to send requests to, return true
   */
  bool send_next_reservation_or_complete();
};

} // namespace Scrub
