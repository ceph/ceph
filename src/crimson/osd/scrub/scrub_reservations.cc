// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "./scrub_reservations.h"

#include <span>

#include "common/ceph_time.h"
#include "common/debug.h"
#include "crimson/common/log.h"
#include "crimson/osd/scrub/pg_scrubber.h"
#include "crimson/osd/osd.h"
#include "crimson/osd/pg.h"
#include "osd/osd_types_fmt.h"


using namespace crimson::osd::scrub;
SET_SUBSYS(osd);
#define CRIMSON_LOG_PREFIX(...) LOG_PREFIX(__VA_ARGS__)
#define CRIMSON_DEBUG(...) DEBUG(__VA_ARGS__)
#define CRIMSON_INFO(...) INFO(__VA_ARGS__)
#define CRIMSON_CLOG_ERROR() m_pg->get_clog_error()
#define CRIMSON_CLOG_WARN() m_pg->get_clog_warn()
using namespace std::chrono;
using namespace std::chrono_literals;

namespace crimson::osd::scrub {
ReplicaReservations::ReplicaReservations(
  PGScrubber& scrubber,
  reservation_nonce_t& nonce,
  const ScrubMetrics& sm)
  : m_scrubber{scrubber}
  , m_pg{&m_scrubber.get_pg()}
  , m_pgid{m_scrubber.get_pg_id().pgid}
  , m_last_request_sent_nonce{nonce}
  , m_metrics{sm}
{
  CRIMSON_LOG_PREFIX("ReplicaReservations::ReplicaReservations");
  auto acting = m_pg->get_acting_shards();
  m_sorted_secondaries.reserve(acting.size());
  std::copy_if(
    acting.cbegin(), acting.cend(), std::back_inserter(m_sorted_secondaries),
    [whoami = m_pg->get_pg_whoami()](const pg_shard_t& shard) {
      return shard != whoami;
    });
  // Set the number of secondaries in metrics
  const_cast<ScrubMetrics&>(m_metrics).set_rsv_secondaries_num(m_sorted_secondaries.size());

  m_next_to_request = m_sorted_secondaries.cbegin();
  if (m_scrubber.is_reservation_required()) {
    m_process_started_at = ScrubClock::now();
    send_next_reservation_or_complete();
  } else {
    CRIMSON_DEBUG("high-priority scrub - no reservations needed");
    const_cast<ScrubMetrics&>(m_metrics).inc_rsv_skipped();
  }
}
void ReplicaReservations::release_all()
{
  CRIMSON_LOG_PREFIX("ReplicaReservations::release_all");
  std::span<const pg_shard_t> replicas{
      m_sorted_secondaries.cbegin(), m_next_to_request};
  CRIMSON_DEBUG("releasing {}", replicas);
  epoch_t epoch = m_pg->get_osdmap_epoch();

  // send 'release' messages to all replicas we have managed to reserve
  for (const auto& peer : replicas) {
    auto m = make_message<MOSDScrubReserve>(
      spg_t{m_pgid, peer.shard}, epoch, MOSDScrubReserve::RELEASE,
      m_pg->get_pg_whoami(), 0);
    std::ignore = m_pg->get_shard_services().send_to_osd(peer.osd, std::move(m), epoch);
  }

  m_sorted_secondaries.clear();
  m_next_to_request = m_sorted_secondaries.cbegin();
}

void ReplicaReservations::discard_remote_reservations()
{
  CRIMSON_LOG_PREFIX("ReplicaReservations::discard_remote_reservations");
  CRIMSON_DEBUG("reset w/o issuing messages");
  m_sorted_secondaries.clear();
  m_next_to_request = m_sorted_secondaries.cbegin();
}

void ReplicaReservations::log_success_and_duration()
{
  ceph_assert(m_process_started_at.has_value());
  auto logged_duration = ScrubClock::now() - m_process_started_at.value();
  // For Crimson, use metrics instead of perf counters
  // inc_rsv_successful handles both count and elapsed time
  auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(logged_duration).count();
  const_cast<ScrubMetrics&>(m_metrics).inc_rsv_successful(elapsed_ms);
  // Note: Seastar metrics don't use histograms like classic OSD perf counters
  m_process_started_at.reset();
}

void ReplicaReservations::log_failure_and_duration(int failure_cause_counter)
{
  if (!m_process_started_at.has_value()) {
    // outcome (success/failure) already logged
    return;
  }
  auto logged_duration = ScrubClock::now() - m_process_started_at.value();
  // For Crimson, use metrics instead of perf counters
  auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(logged_duration).count();
  const_cast<ScrubMetrics&>(m_metrics).inc_rsv_failed(elapsed_ms);
  // failure_cause_counter is handled separately by caller
  m_process_started_at.reset();
}

ReplicaReservations::~ReplicaReservations()
{
  release_all();
  // For Crimson, track aborted count separately
  if (m_process_started_at.has_value()) {
    log_failure_and_duration(0);  // log duration
    const_cast<ScrubMetrics&>(m_metrics).inc_rsv_aborted();
  }
}

bool ReplicaReservations::is_reservation_response_relevant(
    reservation_nonce_t msg_nonce) const
{
  return (msg_nonce == 0) || (msg_nonce == m_last_request_sent_nonce);
}

bool ReplicaReservations::is_msg_source_correct(pg_shard_t from) const
{
  const auto exp_source = get_last_sent();
  return exp_source && from == *exp_source;
}

bool ReplicaReservations::handle_reserve_grant(
    const MOSDScrubReserve& msg,
    pg_shard_t from)
{
  CRIMSON_LOG_PREFIX("ReplicaReservations::handle_reserve_grant");
  if (!is_reservation_response_relevant(msg.reservation_nonce)) {
    // this is a stale response to a previous request (e.g. one that
    // timed-out). See m_last_request_sent_nonce for details.
    CRIMSON_INFO("stale reservation response from {} with nonce {} vs. "
     "expected {} (e:{})",
     from, msg.reservation_nonce, m_last_request_sent_nonce,
     msg.map_epoch);

    return false;
  }

  // verify that the grant is from the peer we expected. If not?
  // for now - abort the OSD. There is no known scenario in which a
  // grant message with a correct nonce can arrive from the wrong peer.
  // (we would not abort for arriving messages with nonce 0, as those
  // are legacy messages, for which the nonce was not verified).
  if (!is_msg_source_correct(from)) {
    const auto error_text = fmt::format(
      "unexpected reservation grant from {} vs. the expected {} (e:{} "
      "message nonce:{})",
      from, get_last_sent().value_or(pg_shard_t{}), msg.map_epoch,
      msg.reservation_nonce);
    CRIMSON_INFO("{}", error_text.c_str());
    if (msg.reservation_nonce != 0) {
      CRIMSON_CLOG_ERROR() << error_text;
      ceph_abort_msg(error_text.c_str());
    }
    return false;
  }

  auto elapsed = ScrubClock::now() - m_last_request_sent_at;
  CRIMSON_DEBUG(
		  "(e:{} nonce:{}) granted by {} ({} of {}) in {}ms",
		  msg.map_epoch, msg.reservation_nonce, from,
		  active_requests_cnt(), m_sorted_secondaries.size(),
		  duration_cast<milliseconds>(elapsed).count());

  return send_next_reservation_or_complete();
}

bool ReplicaReservations::send_next_reservation_or_complete()
{
  CRIMSON_LOG_PREFIX("ReplicaReservations::send_next_reservation_or_complete");
  if (m_next_to_request == m_sorted_secondaries.cend()) {
    // granted by all replicas
    CRIMSON_DEBUG("remote reservation complete");
    log_success_and_duration();
    return true;  // done
  }

  // send the next reservation request
  const auto peer = *m_next_to_request;
  const auto epoch = m_pg->get_osdmap_epoch();
  m_last_request_sent_nonce++;
  auto m = make_message<MOSDScrubReserve>(
      spg_t{m_pgid, peer.shard}, epoch, MOSDScrubReserve::REQUEST, m_pg->get_pg_whoami(),
      m_last_request_sent_nonce);
  std::ignore = m_pg->get_shard_services().send_to_osd(peer.osd, std::move(m), epoch);
  m_last_request_sent_at = ScrubClock::now();
  CRIMSON_DEBUG(
		  "reserving {} (the {} of {} replicas) e:{} nonce:{}",
		  *m_next_to_request, active_requests_cnt() + 1,
		  m_sorted_secondaries.size(), epoch, m_last_request_sent_nonce);

  m_next_to_request++;
  return false;
}

bool ReplicaReservations::handle_reserve_rejection(
    const MOSDScrubReserve& msg,
    pg_shard_t from)
{
  CRIMSON_LOG_PREFIX("ReplicaReservations::handle_reserve_rejection");
  // a convenient log message for the reservation process conclusion
  // (matches the one in send_next_reservation_or_complete())
  CRIMSON_DEBUG(
		  "remote reservation failure. Rejected by {} ({})", from, msg);

  if (!is_reservation_response_relevant(msg.reservation_nonce)) {
    // this is a stale response to a previous request (e.g. one that
    // timed-out). See m_last_request_sent_nonce for details.
    CRIMSON_DEBUG(
		    "stale reservation response from {} with reservation_nonce "
		    "{} vs. expected {} (e:{})",
		    from, msg.reservation_nonce, m_last_request_sent_nonce,
		    msg.map_epoch);

    return false;
  }

  log_failure_and_duration(0);  // log duration
  const_cast<ScrubMetrics&>(m_metrics).inc_rsv_rejected();

  // we should never see a rejection carrying a valid
  // reservation nonce - arriving while we have no pending requests
  ceph_assert(get_last_sent().has_value() || msg.reservation_nonce == 0);

  // verify that the denial is from the peer we expected. If not?
  // There is no known scenario in which this can happen, but if it does -
  // we should treat it as though the *correct* peer has rejected the request,
  // but remember to release that peer, too.
  if (is_msg_source_correct(from)) {
    m_next_to_request--;  // no need to release this one
  } else {
    CRIMSON_CLOG_WARN() << fmt::format(
	"unexpected reservation denial from {} vs the expected {} (e:{} "
	"message reservation_nonce:{})",
	from, get_last_sent().value_or(pg_shard_t{}), msg.map_epoch,
	msg.reservation_nonce);
  }
  return true;
}

std::optional<pg_shard_t> ReplicaReservations::get_last_sent() const
{
  if (m_next_to_request == m_sorted_secondaries.cbegin()) {
    return std::nullopt;
  }
  return *(m_next_to_request - 1);
}

size_t ReplicaReservations::active_requests_cnt() const
{
  return m_next_to_request - m_sorted_secondaries.cbegin();
}
} // namespace Scrub
