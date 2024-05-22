// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <cassert>
#include <chrono>
#include <optional>
#include <string_view>
#include <vector>

#include "messages/MOSDScrubReserve.h"
#include "osd/scrubber_common.h"

#include "osd_scrub_sched.h"
#include "scrub_machine_lstnr.h"

namespace Scrub {

using reservation_nonce_t = MOSDScrubReserve::reservation_nonce_t;

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
 *
 * Keeping primary & replica in sync:
 *
 * Reservation requests may be canceled by the primary independently of the
 * replica's response. Depending on timing, a cancellation by the primary might
 * or might not be processed by a replica prior to sending a response (either
 * rejection or success).  Thus, we associate each reservation request with a
 * nonce incremented with each reservation during an interval and drop any
 * responses that do not match our current nonce.
 * This check occurs after rejecting any messages from prior intervals, so
 * reusing nonces between intervals is not a problem.  Note that epoch would
 * not suffice as it is possible for this sequence to occur several times
 * without a new map epoch.
 * Note - 'release' messages, which are not replied to by the replica,
 * do not need or use that field.
 */
class ReplicaReservations {
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
  ScrubTimePoint m_last_request_sent_at;

  /**
   * A ref to PrimaryActive::last_request_sent_nonce.
   * Identifies a specific request sent, to verify against grant/deny
   * responses.
   * See PrimaryActive::last_request_sent_nonce for details.
   */
  reservation_nonce_t& m_last_request_sent_nonce;

  /// access to the performance counters container relevant to this scrub
  /// parameters
  PerfCounters& m_perf_set;

  /// used only for the 'duration of the reservation process' perf counter.
  /// discarded once the success or failure are recorded
  std::optional<ScrubTimePoint> m_process_started_at;

 public:
  ReplicaReservations(
      ScrubMachineListener& scrubber,
      reservation_nonce_t& nonce,
      PerfCounters& pc);

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
  bool handle_reserve_grant(const MOSDScrubReserve& msg, pg_shard_t from);

  /**
   * React to an incoming reservation rejection.
   *
   * Verify that the sender of the received rejection is the replica we
   * were expecting a reply from, and that the message isn't stale (see
   * m_last_request_sent_nonce for details).
   * If a valid rejection: log it, and mark the fact that the specific peer
   * need not be released.
   *
   * Note - the actual handling of scrub session termination and of
   * releasing the reserved replicas is done by the caller (the FSM).
   *
   * Returns true if the rejection is valid, false otherwise.
   */
  bool handle_reserve_rejection(const MOSDScrubReserve& msg, pg_shard_t from);

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

  /**
   * if the start time is still set, i.e. we have not yet marked
   * this as a success or a failure - log its duration as that of a failure.
   */
  void log_failure_and_duration(int failure_cause_counter);

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

  /**
   * is this is a reply to our last request?
   * Checks response once against m_last_request_sent_nonce. See
   * m_last_request_sent_nonce for details.
   */
  bool is_reservation_response_relevant(reservation_nonce_t msg_nonce) const;

  /**
   * is this reply coming from the expected replica?
   * Now that we check the nonce before checking the sender - this
   * check should never fail.
   */
  bool is_msg_source_correct(pg_shard_t from) const;

  // ---   perf counters helpers

  /**
   * log the duration of the reservation process as that of a success.
   */
  void log_success_and_duration();
};

} // namespace Scrub
