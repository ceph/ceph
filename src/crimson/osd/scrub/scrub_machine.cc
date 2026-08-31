// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "include/ceph_assert.h"

#include "crimson/osd/pg.h"
#include "pg_scrubber.h"

namespace crimson::osd::scrub {

void ReplicaActive::RtReservationCB::finish(int)
{
  pg.scrubber.send_granted_by_reserver(res_data);
}

#define DECLARE_LOCALS                                           \
  auto& machine = context<ScrubMachine>();			 \
  std::ignore = machine;					 \
  PGScrubber* m_scrbr = machine.m_scrbr;		 \
  std::ignore = m_scrbr;                                           \
  auto pg_id = m_scrbr->get_pg_id();					 \
  std::ignore = pg_id;                                      \
  auto &pg = m_scrbr->get_pg();					 \
  std::ignore = pg;


WaitUpdate::WaitUpdate(my_context ctx) : ScrubState(ctx)
{
  DECLARE_LOCALS;

  // Check for abort before reserving range
  if (!m_scrbr->verify_against_abort(pg.get_osdmap_epoch())) {
    // Abort detected, post abort event to transition to AwaitScrub
    post_event(events::abort_t{});
    return;
  }

  auto &cs = context<ChunkState>();
  cs.range_reserved = true;
  assert(cs.range);
  get_scrub_context().reserve_range(cs.range->start, cs.range->end);
}

sc::result WaitUpdate::react(const ScrubContext::reserve_range_complete_t &e)
{
  DECLARE_LOCALS;

  // Check if scrub should abort before transitioning to ScanRange
  if (!m_scrbr->verify_against_abort(pg.get_osdmap_epoch())) {
    // Abort detected, post abort event to transition to AwaitScrub
    post_event(events::abort_t{});
    return discard_event();
  }

  context<ChunkState>().version = e.value;
  return transit<ScanRange>();
}

ScanRange::ScanRange(my_context ctx) : ScrubState(ctx)
{
  DECLARE_LOCALS;

  // Check for abort before scanning range
  if (!m_scrbr->verify_against_abort(pg.get_osdmap_epoch())) {
    // Abort detected, post abort event to transition to AwaitScrub
    post_event(events::abort_t{});
    return;
  }

  ceph_assert(context<ChunkState>().range);
  const auto &cs = context<ChunkState>();
  const auto &range = cs.range.value();
  get_scrub_context(
  ).foreach_id_to_scrub([this, &range, &cs](const auto &id) {
    get_scrub_context().scan_range(
      id, cs.version,
      context<Scrubbing>().deep,
      range.start, range.end);
    waiting_on++;
  });
}

sc::result ScanRange::react(const ScrubContext::scan_range_complete_t &event)
{
  DECLARE_LOCALS;

  auto [_, inserted] = maps.insert(event.value.to_pair());
  ceph_assert(inserted);
  ceph_assert(waiting_on > 0);
  --waiting_on;

  if (waiting_on > 0) {
    return discard_event();
  } else {
    // Check if scrub should abort after completing a chunk
    if (!m_scrbr->verify_against_abort(pg.get_osdmap_epoch())) {
      // Abort detected, post abort event to transition to AwaitScrub
      post_event(events::abort_t{});
      return discard_event();
    }

    ceph_assert(context<ChunkState>().range);
    {
      auto results = validate_chunk(
        get_scrub_context().get_dpp(),
        context<Scrubbing>().policy,
        maps);
      context<Scrubbing>().stats.add(results.stats);
      get_scrub_context().emit_chunk_result(
        *(context<ChunkState>().range),
        std::move(results));
    }
    return transit<WaitDigestUpdate>();
  }
}

WaitDigestUpdate::WaitDigestUpdate(my_context ctx) : ScrubState(ctx)
{
  DECLARE_LOCALS;

  if (!m_scrbr->has_pending_digest_updates()) {
    post_event(ScrubContext::digest_updates_complete_t{});
  }
}

sc::result WaitDigestUpdate::react(
  const ScrubContext::digest_updates_complete_t &)
{
  DECLARE_LOCALS;

  ceph_assert(context<ChunkState>().range);
  LOG_PREFIX(WaitDigestUpdate::react);
  bool is_last = context<ChunkState>().range->end.is_max();
  SUBDEBUGDPP(osd, "digest updates complete, is_last_chunk={}", dpp, is_last);
  if (is_last) {
    SUBDEBUGDPP(osd, "last chunk, completing scrub", dpp);
    auto& scrubbing = context<Scrubbing>();
    get_scrub_context().emit_scrub_result(
      scrubbing.deep,
      scrubbing.stats);
    if (auto* metrics = scrubbing.get_metrics()) {
      auto duration = ScrubClock::now() - scrubbing.scrub_start_time;
      auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        duration).count();
      metrics->inc_successful(elapsed_ms);
    }
    return transit<PrimaryActive>();
  }

  auto range_end = context<ChunkState>().range->end;
  SUBDEBUGDPP(osd, "chunk complete at range end {}, advancing to next chunk", dpp, range_end);
  context<Scrubbing>().advance_current(range_end);
  return transit<PendingTimer>();
}

Scrubbing::Scrubbing(my_context ctx)
  : ScrubState(ctx), policy(get_scrub_context().get_policy())
{
  DECLARE_LOCALS;

  // Record scrub start time for elapsed time calculation (using ScrubClock like classic)
  scrub_start_time = ScrubClock::now();

  // Increment started counter (metrics already registered in PGScrubber constructor)
  if (m_scrbr->m_last_scrub_metrics) {
    m_scrbr->m_last_scrub_metrics->inc_started();
  }
}

ScrubMetrics* Scrubbing::get_metrics()
{
  DECLARE_LOCALS;
  return m_scrbr->get_scrub_metrics();
}

ReservingReplicas::ReservingReplicas(my_context ctx) : ScrubState(ctx)
{
  LOG_PREFIX(ReservingReplicas::ReservingReplicas);
  SUBDEBUGDPP(osd, "entering ReservingReplicas state", dpp);
  DECLARE_LOCALS;
  auto &scrubbing = context<Scrubbing>();

  scrubbing.m_reservations.emplace(
      *m_scrbr, context<PrimaryActive>().last_request_sent_nonce, *scrubbing.get_metrics());

  if (!scrubbing.m_reservations->get_last_sent()) {
    // no replicas to reserve
    SUBDEBUGDPP(osd, "no replicas to reserve, transitioning immediately", dpp);
    // can't transit directly from here
    post_event(events::remotes_reserved_t{});
  }
}

sc::result ReservingReplicas::react(const events::replica_grant_t &event)
{
  LOG_PREFIX(ReservingReplicas::react(replica_grant_t));
  SUBDEBUGDPP(osd, "received grant from {}", dpp, event.m_from);
  DECLARE_LOCALS;
  auto &m = *const_cast<MOSDScrubReserve*>(static_cast<const MOSDScrubReserve*>(&event.m));

  auto &scrubbing = context<Scrubbing>();
  ceph_assert(scrubbing.m_reservations);
  if (scrubbing.m_reservations->handle_reserve_grant(m, event.m_from)) {
    // we are done with the reservation process
    // Note: elapsed time tracking is handled by ReplicaReservations::log_success_and_duration()
    if (auto* metrics = scrubbing.get_metrics()) {
      // Count the number of secondaries (replicas) we reserved
      // This is the size of the acting set minus the primary
      auto num_secondaries = get_scrub_context().get_ids_to_scrub().size() - 1;
      metrics->set_rsv_secondaries_num(num_secondaries);
      // Now that reservations are complete, scrubbing is "active"
      metrics->inc_active_started();
    }
    LOG_PREFIX(ReservingReplicas::react);
    SUBDEBUGDPP(osd, "reservations complete, transitioning to PendingTimer", dpp);
    // Transition to PendingTimer which will sleep before first chunk
    return transit<PendingTimer>();
  }
  return discard_event();

}

sc::result ReservingReplicas::react(const events::replica_reject_t &event)
{
  LOG_PREFIX(ReservingReplicas::react(replica_reject_t));
  SUBWARNDPP(osd, "received rejection from {}", dpp, event.m_from);
  DECLARE_LOCALS;

  auto& scrubbing = context<Scrubbing>();
  ceph_assert(scrubbing.m_reservations);
  auto &m = *const_cast<MOSDScrubReserve*>(static_cast<const MOSDScrubReserve*>(&event.m));

    // Verify that the message is from the replica we were expecting a reply from,
  // and that the message is not stale. If all is well - this is a real rejection:
  // - log required details;
  // - manipulate the 'next to reserve' iterator to exclude
  //   the rejecting replica from the set of replicas requiring release
  if (!scrubbing.m_reservations->handle_reserve_rejection(m, event.m_from)) {
    // stale or unexpected
    return discard_event();
  }

  // The rejection was carrying the correct reservation_nonce. It was
  // logged by handle_reserve_rejection().
  // Set 'reservation failure' as the scrub termination cause (affecting
  // the rescheduling of this PG)
  m_scrbr->flag_reservations_failure();

  if (auto* metrics = scrubbing.get_metrics()) {
    metrics->inc_rsv_rejected();
  }

  return transit<AwaitScrub>();
}

sc::result ReservingReplicas::react(const events::remotes_reserved_t &)
{
  DECLARE_LOCALS;
  LOG_PREFIX(ReservingReplicas::react(remotes_reserved_t));
  SUBDEBUGDPP(osd, "no replicas to reserve, transitioning to PendingTimer", dpp);

  // Increment active_started counter since we're about to start scrubbing
  auto &scrubbing = context<Scrubbing>();
  if (auto* metrics = scrubbing.get_metrics()) {
    metrics->inc_active_started();
  }

  // Transition to PendingTimer which will sleep before first chunk
  return transit<PendingTimer>();
}

// -------- for replicas -----------------------------------------------------

// ----------------------- ReplicaActive --------------------------------

ReplicaActive::~ReplicaActive()
{
}
sc::result ReplicaActive::react(const events::replica_reserve_request_t &event)
{
  LOG_PREFIX(ReplicaActive::react(replica_reserve_request_t));
  SUBDEBUGDPP(osd, "received reservation request from {}", dpp, event.m_from);
  DECLARE_LOCALS;
  auto &m = *const_cast<MOSDScrubReserve*>(static_cast<const MOSDScrubReserve*>(&event.m));

  if (m_reservation_status != reservation_status_t::unreserved) {
    // we are not expected to be in this state when a new request arrives.
    // Clear the existing reservation - be it granted or pending.
    SUBDEBUGDPP(
      osd,
      "unexpected request. discarding existing reservation "
      "(was granted?:{}). incoming request: {}",
      dpp,
      reservation_granted,
      m);

    clear_remote_reservation(true);
  }

  handle_reservation_request(event);
  return discard_event();
}

void ReplicaActive::handle_reservation_request(const events::replica_reserve_request_t& event)
{
  LOG_PREFIX(ReplicaActive::handle_reservation_request);
  DECLARE_LOCALS;
  auto &m = *const_cast<MOSDScrubReserve*>(static_cast<const MOSDScrubReserve*>(&event.m));

  const auto async_disabled = crimson::common::local_conf().get_val<bool>(
    "osd_scrub_disable_reservation_queuing");
  const bool async_request = !async_disabled && m.wait_for_resources;

  SUBDEBUGDPP(
    osd,
    "handling reservation request. async_request: {}, async_disabled: {}, "
    "m.wait_for_resources: {}",
    dpp, async_request, async_disabled, m.wait_for_resources);

  if (async_request) {
    AsyncScrubResData request_details(
      pg_id, event.m_from, m.map_epoch, m.reservation_nonce);

    SUBDEBUGDPP(
      osd,
      "queuing async reservation request:{} with details: {}",
      dpp, event, request_details);

    pending_reservation_nonce = m.reservation_nonce;
    auto *reservation_cb = new RtReservationCB(pg, request_details);

    std::ignore = pg.get_shard_services().scrub_local_request_reservation(
      pg_id,
      reservation_cb,
      /*prio=*/0,
      nullptr);

    m_reservation_status = reservation_status_t::requested_or_granted;
    return;
  }

  auto map_epoch = m.map_epoch;
  auto reservation_nonce = m.reservation_nonce;
  auto pg_whoami = pg.get_pg_whoami();
  auto primary_shard = pg.get_primary().shard;
  std::ignore = pg.get_shard_services().scrub_local_request_reservation_or_fail(
    pg_id
  ).then([this, &pg, pg_id, pg_whoami, primary_shard, from=event.m_from, map_epoch,
          reservation_nonce](bool granted) {
    LOG_PREFIX(ReplicaActive::handle_reservation_request);
    reservation_granted = granted;

    MessageURef reply;
    if (granted) {
      SUBDEBUGDPP(
        osd,
        "immediately granting reservation request from {}",
        dpp,
        from);
      m_reservation_status = reservation_status_t::requested_or_granted;
      reply = make_message<MOSDScrubReserve>(
        spg_t(pg_id.pgid, primary_shard),
        map_epoch,
        MOSDScrubReserve::GRANT,
        pg_whoami,
        reservation_nonce);
    } else {
      SUBDEBUGDPP(
        osd,
        "immediately rejecting reservation request from {}",
        dpp,
        from);
      m_reservation_status = reservation_status_t::unreserved;
      reply = make_message<MOSDScrubReserve>(
        spg_t(pg_id.pgid, primary_shard),
        map_epoch,
        MOSDScrubReserve::REJECT,
        pg_whoami,
        reservation_nonce);
    }

    std::ignore = pg.get_shard_services().send_to_osd(from.osd, std::move(reply), map_epoch);
    return;
  });
}

sc::result ReplicaActive::react(const events::reserver_granted_t &event)
{
  LOG_PREFIX(ReplicaActive::react(reserver_granted_t));
  const auto &reservation = event.value;
  SUBDEBUGDPP(osd, "reservation granted: {}", dpp, reservation);

  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases

  if (reservation.nonce != pending_reservation_nonce) {
    SUBDEBUGDPP(
      osd,
      "reservation_nonce mismatch: {} != {}",
      dpp,
      reservation.nonce,
      pending_reservation_nonce);
    return discard_event();
  }

  reservation_granted = true;
  pending_reservation_nonce = 0;

  // notify the primary
  auto grant_msg = make_message<MOSDScrubReserve>(
      spg_t(pg_id.pgid, pg.get_primary().shard), reservation.request_epoch,
            MOSDScrubReserve::GRANT, pg.get_pg_whoami(), reservation.nonce);
  std::ignore = pg.get_shard_services().send_to_osd(reservation.from.osd, std::move(grant_msg), reservation.request_epoch);
  return discard_event();
}

void ReplicaActive::clear_remote_reservation(bool warn_if_no_reservation)
{
  DECLARE_LOCALS;
  LOG_PREFIX(ReplicaActive::clear_remote_reservation);
  SUBDEBUGDPP(
    osd,
    "pending_reservation_nonce {}, reservation_granted {}",
    dpp,
    pending_reservation_nonce,
    reservation_granted);

  if (reservation_granted || pending_reservation_nonce) {
    std::ignore = pg.get_shard_services().scrub_local_cancel_reservation(
      pg_id
    ).then([this] {
      reservation_granted = false;
      pending_reservation_nonce = 0;
      ceph_assert(m_reservation_status != reservation_status_t::unreserved);
      m_reservation_status = reservation_status_t::unreserved;
    });
  } else if (warn_if_no_reservation) {
    SUBDEBUGDPP(osd, "not reserved!", dpp);
  }
}

sc::result ReplicaActive::react(const events::replica_release_t &event)
{
  LOG_PREFIX(ReplicaActive::react(replica_release_t));
  SUBDEBUGDPP(osd, "received release from {}", dpp, event.m_from);
  clear_remote_reservation(true);
  return discard_event();
}

sc::result ReplicaIdle::react(const events::replica_scan_t &event)
{
    LOG_PREFIX(ScrubState::ReplicaIdle::react(events::replica_scan_t));
    SUBDEBUGDPP(osd, "event.value: {}", get_scrub_context().get_dpp(), event.value);
    DECLARE_LOCALS;

  // if we are waiting for a reservation grant from the reserver (an
  // illegal scenario!), that reservation must be cleared.
  if (context<ReplicaActive>().pending_reservation_nonce) {
    SUBDEBUGDPP(osd, "osd.{} pg[{}]: new chunk request while still waiting for reservation",
      dpp, pg.get_pg_whoami(), pg.get_pgid());

    context<ReplicaActive>().clear_remote_reservation(true);
  }
  post_event(event);
  return transit<ReplicaChunkState>();
}

sc::result ReplicaChunkState::react(const events::replica_scan_t &event) {
    LOG_PREFIX(ScrubState::ReplicaChunkState::react(events::replica_scan_t));
    SUBDEBUGDPP(osd, "event.value: {}", get_scrub_context().get_dpp(), event.value);
    to_scan = event.value;
    if (get_scrub_context().await_update(event.value.version)) {
      post_event(ScrubContext::await_update_complete_t{});
    }
    return discard_event();
}
sc::result ReplicaChunkState::react(const events::replica_release_t& event)
{
  LOG_PREFIX(ScrubState::ReplicaChunkState::react(events::replica_release_t));
  SUBDEBUGDPP(osd, "received release from {}", get_scrub_context().get_dpp(), event.m_from);
  return transit<ReplicaActive>();
}

ReplicaScanChunk::ReplicaScanChunk(my_context ctx) : ScrubState(ctx)
{
  auto &to_scan = context<ReplicaChunkState>().to_scan;
  get_scrub_context().generate_and_submit_chunk_result(
    to_scan.start,
    to_scan.end,
    to_scan.deep);
}
// ----------------------- PendingTimer -----------------------------------

PendingTimer::PendingTimer(my_context ctx) : ScrubState(ctx) {
  DECLARE_LOCALS;
  LOG_PREFIX(PendingTimer::PendingTimer);
  SUBDEBUGDPP(osd, "entering PendingTimer state", dpp);

  // Start the sleep operation which will post internal_sched_scrub_t when done
  m_scrbr->start_chunk_sleep();
}



};
