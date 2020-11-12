// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "pg_scrubber.h"

#include <iostream>
#include <vector>

#include "debug.h"

#include "common/errno.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDRepScrub.h"
#include "messages/MOSDRepScrubMap.h"
#include "messages/MOSDScrub.h"
#include "messages/MOSDScrubReserve.h"

#include "OSD.h"
#include "ScrubStore.h"
#include "scrub_machine.h"

using namespace Scrub;
using namespace std::chrono;
using namespace std::chrono_literals;


#define dout_context (m_pg->cct)
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout, this->m_pg)

template <class T> static ostream& _prefix(std::ostream* _dout, T* t)
{
  return t->gen_prefix(*_dout) << " scrubber pg(" << t->pg_id << ") ";
}

ostream& operator<<(ostream& out, const requested_scrub_t& sf)
{
  if (sf.must_repair)
    out << " MUST_REPAIR";
  if (sf.auto_repair)
    out << " planned AUTO_REPAIR";
  if (sf.check_repair)
    out << " planned CHECK_REPAIR";
  if (sf.deep_scrub_on_error)
    out << " planned DEEP_SCRUB_ON_ERROR";
  if (sf.must_deep_scrub)
    out << " MUST_DEEP_SCRUB";
  if (sf.must_scrub)
    out << " MUST_SCRUB";
  if (sf.time_for_deep)
    out << " TIME_FOR_DEEP";
  if (sf.need_auto)
    out << " NEED_AUTO";
  if (sf.req_scrub)
    out << " planned REQ_SCRUB";

  return out;
}

// ///////////////////// ReplicaReservations //////////////////////////////////
namespace Scrub {

void ReplicaReservations::release_replica(pg_shard_t peer, epoch_t epoch)
{
  dout(15) << __func__ << " <ReplicaReservations> release-> " << peer << dendl;

  auto m = new MOSDScrubReserve(spg_t(m_pg->info.pgid.pgid, peer.shard), epoch,
				    MOSDScrubReserve::RELEASE, m_pg->pg_whoami);
  m_osds->send_message_osd_cluster(peer.osd, m, epoch);
}

ReplicaReservations::ReplicaReservations(PG* pg, pg_shard_t whoami)
    : m_pg{pg}
    , m_acting_set{pg->get_actingset()}
    , m_osds{m_pg->osd}
    , m_pending{static_cast<int>(m_acting_set.size()) - 1}
{
  epoch_t epoch = m_pg->get_osdmap_epoch();

  // handle the special case of no replicas
  if (m_pending <= 0) {
    // just signal the scrub state-machine to continue
    send_all_done();

  } else {

    for (auto p : m_acting_set) {
      if (p == whoami)
	continue;
      auto m = new MOSDScrubReserve(spg_t(m_pg->info.pgid.pgid, p.shard), epoch,
					MOSDScrubReserve::REQUEST, m_pg->pg_whoami);
      m_osds->send_message_osd_cluster(p.osd, m, epoch);
      m_waited_for_peers.push_back(p);
      dout(10) << __func__ << " <ReplicaReservations> reserve<-> " << p.osd << dendl;
    }
  }
}

void ReplicaReservations::send_all_done()
{
  m_osds->queue_for_scrub_granted(m_pg, scrub_prio_t::low_priority);
}

void ReplicaReservations::send_reject()
{
  m_osds->queue_for_scrub_denied(m_pg, scrub_prio_t::low_priority);
}

void ReplicaReservations::release_all()
{
  dout(10) << __func__ << " " << m_reserved_peers << dendl;

  m_had_rejections = true;  // preventing late-coming responses from triggering events
  epoch_t epoch = m_pg->get_osdmap_epoch();

  for (auto p : m_reserved_peers) {
    release_replica(p, epoch);
  }
  m_reserved_peers.clear();

  // note: the release will follow on the heels of the request. When tried otherwise,
  // grants that followed a reject arrived after the whole scrub machine-state was
  // reset, causing leaked reservations.
  if (m_pending) {
    for (auto p : m_waited_for_peers) {
      release_replica(p, epoch);
    }
  }
  m_waited_for_peers.clear();
}

ReplicaReservations::~ReplicaReservations()
{
  m_had_rejections = true;  // preventing late-coming responses from triggering events

  // send un-reserve messages to all reserved replicas. We do not wait for answer (there
  // wouldn't be one). Other incoming messages will be discarded on the way, by our
  // owner.
  release_all();
}

/**
 *  @ATTN we would not reach here if the ReplicaReservation object managed by the
 * scrubber was reset.
 */
void ReplicaReservations::handle_reserve_grant(OpRequestRef op, pg_shard_t from)
{
  dout(10) << __func__ << " <ReplicaReservations> granted-> " << from << dendl;
  op->mark_started();

  {
    // reduce the amount of extra release messages. Not a must, but the log is cleaner
    auto w = find(m_waited_for_peers.begin(), m_waited_for_peers.end(), from);
    if (w != m_waited_for_peers.end())
      m_waited_for_peers.erase(w);
  }

  // are we forced to reject the reservation?
  if (m_had_rejections) {

    dout(10) << " rejecting late-coming reservation from " << from << dendl;
    release_replica(from, m_pg->get_osdmap_epoch());

  } else if (std::find(m_reserved_peers.begin(), m_reserved_peers.end(), from) !=
	     m_reserved_peers.end()) {

    dout(10) << " already had osd." << from << " reserved" << dendl;

  } else {

    dout(10) << " osd." << from << " scrub reserve = success" << dendl;
    m_reserved_peers.push_back(from);
    if (--m_pending == 0) {
      send_all_done();
    }
  }
}

void ReplicaReservations::handle_reserve_reject(OpRequestRef op, pg_shard_t from)
{
  dout(10) << __func__ << " <ReplicaReservations> rejected-> " << from << dendl;
  dout(10) << __func__ << " " << *op->get_req() << dendl;
  op->mark_started();

  {
    // reduce the amount of extra release messages. Not a must, but the log is cleaner
    auto w = find(m_waited_for_peers.begin(), m_waited_for_peers.end(), from);
    if (w != m_waited_for_peers.end())
      m_waited_for_peers.erase(w);
  }

  if (m_had_rejections) {

    // our failure was already handled when the first rejection arrived
    dout(15) << " ignoring late-coming rejection from " << from << dendl;

  } else if (std::find(m_reserved_peers.begin(), m_reserved_peers.end(), from) !=
	     m_reserved_peers.end()) {

    dout(15) << " already had osd." << from << " reserved" << dendl;

  } else {

    dout(10) << " osd." << from << " scrub reserve = fail" << dendl;
    m_had_rejections = true;  // preventing any additional notifications
    --m_pending;	      // not sure we need this bookkeeping anymore
    send_reject();
  }
}

// ///////////////////// LocalReservation //////////////////////////////////

LocalReservation::LocalReservation(PG* pg, OSDService* osds)
    : m_pg{pg}	// holding the "whole PG" for dout() sake
    , m_osds{osds}
{
  if (!m_osds->inc_scrubs_local()) {
    dout(10) << __func__ << ": failed to reserve locally " << dendl;
    // the failure is signalled by not having m_holding_local_reservation set
    return;
  }

  dout(20) << __func__ << ": local OSD scrub resources reserved" << dendl;
  m_holding_local_reservation = true;
}

void LocalReservation::early_release()
{
  if (m_holding_local_reservation) {
    m_holding_local_reservation = false;
    m_osds->dec_scrubs_local();
    dout(20) << __func__ << ": local OSD scrub resources freed" << dendl;
  }
}

LocalReservation::~LocalReservation()
{
  early_release();
}


// ///////////////////// ReservedByRemotePrimary ///////////////////////////////

ReservedByRemotePrimary::ReservedByRemotePrimary(PG* pg, OSDService* osds)
    : m_pg{pg}	// holding the "whole PG" for dout() sake
    , m_osds{osds}
{
  if (!m_osds->inc_scrubs_remote()) {
    dout(10) << __func__ << ": failed to reserve at Primary request" << dendl;
    // the failure is signalled by not having m_reserved_by_remote_primary set
    return;
  }

  dout(20) << __func__ << ": scrub resources reserved at Primary request" << dendl;
  m_reserved_by_remote_primary = true;
}

void ReservedByRemotePrimary::early_release()
{
  dout(20) << "ReservedByRemotePrimary::" << __func__ << ": "
	   << m_reserved_by_remote_primary << dendl;
  if (m_reserved_by_remote_primary) {
    m_reserved_by_remote_primary = false;
    m_osds->dec_scrubs_remote();
    dout(20) << __func__ << ": scrub resources held for Primary were freed" << dendl;
  }
}

ReservedByRemotePrimary::~ReservedByRemotePrimary()
{
  early_release();
}

// ///////////////////// MapsCollectionStatus ////////////////////////////////

auto MapsCollectionStatus::mark_arriving_map(pg_shard_t from)
  -> std::tuple<bool, std::string_view>
{
  auto fe = std::find(m_maps_awaited_for.begin(), m_maps_awaited_for.end(), from);
  if (fe != m_maps_awaited_for.end()) {
    // we are indeed waiting for a map from this replica
    m_maps_awaited_for.erase(fe);
    return std::tuple{true, ""sv};
  } else {
    return std::tuple{false, "unsolicited scrub-map"sv};
  }
}

void MapsCollectionStatus::reset()
{
  *this = MapsCollectionStatus{};
}

std::string MapsCollectionStatus::dump() const
{
  std::string all;
  for (const auto& rp : m_maps_awaited_for) {
    all.append(rp.get_osd() + " "s);
  }
  return all;
}

ostream& operator<<(ostream& out, const MapsCollectionStatus& sf)
{
  out << " [ ";
  for (const auto& rp : sf.m_maps_awaited_for) {
    out << rp.get_osd() << " ";
  }
  if (!sf.m_local_map_ready) {
    out << " local ";
  }
  return out << " ] ";
}

}  // namespace Scrub
