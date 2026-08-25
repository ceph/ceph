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
  auto &cs = context<ChunkState>();
  cs.range_reserved = true;
  assert(cs.range);
  get_scrub_context().reserve_range(cs.range->start, cs.range->end);
}

ScanRange::ScanRange(my_context ctx) : ScrubState(ctx)
{
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
  auto [_, inserted] = maps.insert(event.value.to_pair());
  ceph_assert(inserted);
  ceph_assert(waiting_on > 0);
  --waiting_on;

  if (waiting_on > 0) {
    return discard_event();
  } else {
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
    if (context<ChunkState>().range->end.is_max()) {
      get_scrub_context().emit_scrub_result(
	context<Scrubbing>().deep,
	context<Scrubbing>().stats);
      return transit<PrimaryActive>();
    } else {
      context<Scrubbing>().advance_current(
	context<ChunkState>().range->end);
      return transit<ChunkState>();
    }
  }
}

ReservingReplicas::ReservingReplicas(my_context ctx) : ScrubState(ctx)
{
  LOG_PREFIX(ReservingReplicas::ReservingReplicas);
  SUBDEBUGDPP(osd, "entering ReservingReplicas state", dpp);
  DECLARE_LOCALS;
  auto &scrubbing = context<Scrubbing>();

  scrubbing.m_reservations.emplace(
      *m_scrbr, context<PrimaryActive>().last_request_sent_nonce,
      *scrubbing.m_counters_idx);

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
    return transit<ChunkState>();
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

  return transit<AwaitScrub>();
}

sc::result ReservingReplicas::react(const events::remotes_reserved_t &)
{
  LOG_PREFIX(ReservingReplicas::react(remotes_reserved_t));
  SUBDEBUGDPP(osd, "no replicas to reserve, proceeding to ChunkState", dpp);
  return transit<ChunkState>();
}

// -------- for replicas -----------------------------------------------------

// ----------------------- ReplicaActive --------------------------------

ReplicaActive::~ReplicaActive()
{
  clear_remote_reservation(false);
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


};
