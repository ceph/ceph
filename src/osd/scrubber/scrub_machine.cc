// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <chrono>
#include <typeinfo>

#include <boost/core/demangle.hpp>

#include "osd/OSD.h"
#include "osd/OpRequest.h"

#include "ScrubStore.h"
#include "scrub_machine.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << " scrubberFSM "

using namespace std::chrono;
using namespace std::chrono_literals;

#define DECLARE_LOCALS                                           \
  auto& machine = context<ScrubMachine>();			 \
  std::ignore = machine;					 \
  ScrubMachineListener* scrbr = machine.m_scrbr;		 \
  std::ignore = scrbr;                                           \
  auto pg_id = machine.m_pg_id;					 \
  std::ignore = pg_id;

NamedSimply::NamedSimply(ScrubMachineListener* scrubber, const char* name)
{
  scrubber->set_state_name(name);
}

namespace Scrub {

// --------- trace/debug auxiliaries -------------------------------

void on_event_creation(std::string_view nm)
{
  dout(20) << " event: --vvvv---- " << nm << dendl;
}

void on_event_discard(std::string_view nm)
{
  dout(20) << " event: --^^^^---- " << nm << dendl;
}

void ScrubMachine::assert_not_in_session() const
{
  ceph_assert(!state_cast<const Session*>());
}

bool ScrubMachine::is_reserving() const
{
  return state_cast<const ReservingReplicas*>();
}

bool ScrubMachine::is_primary_idle() const
{
  return state_cast<const PrimaryIdle*>();
}

bool ScrubMachine::is_accepting_updates() const
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  ceph_assert(scrbr->is_primary());

  return state_cast<const WaitLastUpdate*>();
}

// for the rest of the code in this file - we know what PG we are dealing with:
#undef dout_prefix
#define dout_prefix _prefix(_dout, this->context<ScrubMachine>())

template <class T>
static ostream& _prefix(std::ostream* _dout, T& t)
{
  return t.gen_prefix(*_dout);
}

std::ostream& ScrubMachine::gen_prefix(std::ostream& out) const
{
  return m_scrbr->gen_prefix(out) << "FSM: ";
}

ceph::timespan ScrubMachine::get_time_scrubbing() const
{
  // note: the state_cast does not work in the Session ctor
  auto session = state_cast<const Session*>();
  if (!session) {
    dout(20) << fmt::format("{}: not in session", __func__) << dendl;
    return ceph::timespan{};
  }

  if (session && session->m_session_started_at != ScrubTimePoint{}) {
    dout(20) << fmt::format(
		    "{}: session_started_at: {} d:{}", __func__,
		    session->m_session_started_at,
		    ScrubClock::now() - session->m_session_started_at)
	     << dendl;
    return ScrubClock::now() - session->m_session_started_at;
  }
  dout(30) << fmt::format("{}: no session_start time", __func__) << dendl;
  return ceph::timespan{};
}

// ////////////// the actual actions

// ----------------------- NotActive -----------------------------------------

NotActive::NotActive(my_context ctx)
    : my_base(ctx)
    , NamedSimply(context<ScrubMachine>().m_scrbr, "NotActive")
{
  dout(10) << "-- state -->> NotActive" << dendl;
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  scrbr->clear_queued_or_active();
}


// ----------------------- PrimaryActive --------------------------------

PrimaryActive::PrimaryActive(my_context ctx)
    : my_base(ctx)
    , NamedSimply(context<ScrubMachine>().m_scrbr, "PrimaryActive")
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "-- state -->> PrimaryActive" << dendl;
  // insert this PG into the OSD scrub queue. Calculate initial schedule
  scrbr->schedule_scrub_with_osd();
}

PrimaryActive::~PrimaryActive()
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  // we may have set some PG state flags without reaching Session.
  // And we may be holding a 'local resource'.
  scrbr->clear_pgscrub_state();
  scrbr->rm_from_osd_scrubbing();
}


// ---------------- PrimaryActive/PrimaryIdle ---------------------------

PrimaryIdle::PrimaryIdle(my_context ctx)
    : my_base(ctx)
    , NamedSimply(context<ScrubMachine>().m_scrbr, "PrimaryActive/PrimaryIdle")
{
  dout(10) << "-- state -->> PrimaryActive/PrimaryIdle" << dendl;
}

sc::result PrimaryIdle::react(const StartScrub&)
{
  dout(10) << "PrimaryIdle::react(const StartScrub&)" << dendl;
  DECLARE_LOCALS;
  scrbr->reset_epoch();
  return transit<ReservingReplicas>();
}

sc::result PrimaryIdle::react(const AfterRepairScrub&)
{
  dout(10) << "PrimaryIdle::react(const AfterRepairScrub&)" << dendl;
  DECLARE_LOCALS;
  scrbr->reset_epoch();
  return transit<ReservingReplicas>();
}

void PrimaryIdle::clear_state(const FullReset&) {
  dout(10) << "PrimaryIdle::react(const FullReset&): clearing state flags"
           << dendl;
  DECLARE_LOCALS;
  scrbr->clear_pgscrub_state();
}

// ----------------------- Session -----------------------------------------

Session::Session(my_context ctx)
    : my_base(ctx)
    , NamedSimply(context<ScrubMachine>().m_scrbr, "PrimaryActive/Session")
{
  dout(10) << "-- state -->> PrimaryActive/Session" << dendl;
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases

  // while we've checked the 'someone is reserving' flag before queueing
  // the start-scrub event, it's possible that the flag was set in the meantime.
  // Handling this case here requires adding a new sub-state, and the
  // complication of reporting a failure to the caller in a new failure
  // path. On the other hand - ignoring an ongoing reservation on rare
  // occasions will cause no harm.
  // We choose ignorance.
  std::ignore = scrbr->set_reserving_now();

  m_perf_set = &scrbr->get_counters_set();
  m_perf_set->inc(scrbcnt_started);
}

Session::~Session()
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  m_reservations.reset();

  // note the interaction between clearing the 'queued' flag and two
  // other states: the snap-mapper and the scrubber internal state.
  // All of these must be cleared in the correct order, and the snap mapper
  // (re-triggered by resetting the 'queued' flag) must not resume before
  // the scrubber is reset.
  scrbr->clear_pgscrub_state();
}

sc::result Session::react(const IntervalChanged&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "Session::react(const IntervalChanged&)" << dendl;

  m_reservations->discard_remote_reservations();
  return transit<NotActive>();
}


// ----------------------- ReservingReplicas ---------------------------------

ReservingReplicas::ReservingReplicas(my_context ctx)
    : my_base(ctx)
    , NamedSimply(context<ScrubMachine>().m_scrbr, "Session/ReservingReplicas")
{
  dout(10) << "-- state -->> ReservingReplicas" << dendl;
  auto& session = context<Session>();
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases

  // initiate the reservation process
  session.m_reservations.emplace(
      *scrbr, context<PrimaryActive>().last_request_sent_nonce,
      *session.m_perf_set);

  if (session.m_reservations->get_last_sent()) {
    // the 1'st reservation request was sent

    auto timeout = scrbr->get_pg_cct()->_conf.get_val<milliseconds>(
	"osd_scrub_reservation_timeout");
    if (timeout.count() > 0) {
      // Start a timer to handle case where the replicas take a long time to
      // ack the reservation.  See ReservationTimeout handler below.
      m_timeout_token =
	  machine.schedule_timer_event_after<ReservationTimeout>(timeout);
    }
  } else {
    // no replicas to reserve
    dout(10) << "no replicas to reserve" << dendl;
    // can't transit directly from here
    post_event(RemotesReserved{});
  }
}

ReservingReplicas::~ReservingReplicas()
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  // it's OK to try and clear the flag even if we don't hold it
  // (the flag remembers the actual holder)
  scrbr->clear_reserving_now();
}

sc::result ReservingReplicas::react(const ReplicaGrant& ev)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "ReservingReplicas::react(const ReplicaGrant&)" << dendl;
  const auto& m = ev.m_op->get_req<MOSDScrubReserve>();

  if (context<Session>().m_reservations->handle_reserve_grant(*m, ev.m_from)) {
    // we are done with the reservation process
    return transit<ActiveScrubbing>();
  }
  return discard_event();
}

sc::result ReservingReplicas::react(const ReplicaReject& ev)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  auto& session = context<Session>();
  dout(10) << "ReservingReplicas::react(const ReplicaReject&)" << dendl;
  const auto m = ev.m_op->get_req<MOSDScrubReserve>();

  // Verify that the message is from the replica we were expecting a reply from,
  // and that the message is not stale. If all is well - this is a real rejection:
  // - log required details;
  // - manipulate the 'next to reserve' iterator to exclude
  //   the rejecting replica from the set of replicas requiring release
  if (!session.m_reservations->handle_reserve_rejection(*m, ev.m_from)) {
    // stale or unexpected
    return discard_event();
  }

  // The rejection was carrying the correct reservation_nonce. It was
  // logged by handle_reserve_rejection().
  // Set 'reservation failure' as the scrub termination cause (affecting
  // the rescheduling of this PG)
  scrbr->flag_reservations_failure();

  // 'Session' state dtor stops the scrubber
  return transit<PrimaryIdle>();
}

sc::result ReservingReplicas::react(const ReservationTimeout&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  auto& session = context<Session>();
  dout(10) << "ReservingReplicas::react(const ReservationTimeout&)" << dendl;
  session.m_reservations->log_failure_and_duration(scrbcnt_resrv_timed_out);

  const auto msg = fmt::format(
      "osd.{} PgScrubber: {} timeout on reserving replicas (since {})",
      scrbr->get_whoami(), scrbr->get_spgid(), entered_at);
  dout(1) << msg << dendl;
  scrbr->get_clog()->warn() << msg;

  // cause the scrubber to stop the scrub session, marking 'reservation
  // failure' as the cause (affecting future scheduling)
  scrbr->flag_reservations_failure();
  return transit<PrimaryIdle>();
}

// ----------------------- ActiveScrubbing -----------------------------------

ActiveScrubbing::ActiveScrubbing(my_context ctx)
    : my_base(ctx)
    , NamedSimply(context<ScrubMachine>().m_scrbr, "Session/ActiveScrubbing")
{
  dout(10) << "-- state -->> ActiveScrubbing" << dendl;
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  auto& session = context<Session>();

  session.m_perf_set->inc(scrbcnt_active_started);
  scrbr->get_clog()->debug()
      << fmt::format("{} {} starts", pg_id, scrbr->get_op_mode_text());

  scrbr->on_init();
}

/**
 *  upon exiting the Active state
 */
ActiveScrubbing::~ActiveScrubbing()
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  auto& session = context<Session>();
  dout(15) << __func__ << dendl;

  // if the begin-time stamp was not set 'off' (as done if the scrubbing
  // completed successfully), we use it now to set the 'failed scrub' duration.
  if (session.m_session_started_at != ScrubTimePoint{}) {
    // delay the next invocation of the scrubber on this target
    scrbr->penalize_next_scrub(Scrub::delay_cause_t::aborted);

    auto logged_duration = ScrubClock::now() - session.m_session_started_at;
    session.m_perf_set->tinc(scrbcnt_failed_elapsed, logged_duration);
    session.m_perf_set->inc(scrbcnt_failed);
  }
}

// ----------------------- RangeBlocked -----------------------------------

/*
 * Blocked. Will be released by kick_object_context_blocked() (or upon
 * an abort)
 *
 * Note: we are never expected to be waiting for long for a blocked object.
 * Unfortunately we know from experience that a bug elsewhere might result
 * in an indefinite wait in this state, for an object that is never released.
 * If that happens, all we can do is to issue a warning message to help
 * with the debugging.
 */
RangeBlocked::RangeBlocked(my_context ctx)
    : my_base(ctx)
    , NamedSimply(context<ScrubMachine>().m_scrbr, "Session/Act/RangeBlocked")
{
  dout(10) << "-- state -->> Session/Act/RangeBlocked" << dendl;
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases

  auto grace = scrbr->get_range_blocked_grace();
  if (grace == ceph::timespan{}) {
    // we will not be sending any alarms re the blocked object
    dout(10)
      << __func__
      << ": blocked-alarm disabled ('osd_blocked_scrub_grace_period' set to 0)"
      << dendl;
  } else {
    // Schedule an event to warn that the pg has been blocked for longer than
    // the timeout, see RangeBlockedAlarm handler below
    dout(20) << fmt::format(": timeout:{}",
			    std::chrono::duration_cast<seconds>(grace))
	     << dendl;

    m_timeout_token = machine.schedule_timer_event_after<RangeBlockedAlarm>(
      grace);
  }
  context<Session>().m_perf_set->inc(scrbcnt_blocked);
}

sc::result RangeBlocked::react(const RangeBlockedAlarm&)
{
  DECLARE_LOCALS;
  char buf[50];
  std::time_t now_c = ScrubClock::to_time_t(entered_at);
  strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", std::localtime(&now_c));
  dout(10)
    << "PgScrubber: " << scrbr->get_spgid()
    << " blocked on an object for too long (since " << buf << ")" << dendl;
  scrbr->get_clog()->warn()
    << "osd." << scrbr->get_whoami()
    << " PgScrubber: " << scrbr->get_spgid()
    << " blocked on an object for too long (since " << buf
    << ")";

  scrbr->set_scrub_blocked(utime_t{now_c, 0});
  return discard_event();
}

// ----------------------- PendingTimer -----------------------------------

/**
 *  Sleeping till timer reactivation - or just requeuing
 */
PendingTimer::PendingTimer(my_context ctx)
    : my_base(ctx)
    , NamedSimply(context<ScrubMachine>().m_scrbr, "Session/Act/PendingTimer")
{
  dout(10) << "-- state -->> Session/Act/PendingTimer" << dendl;
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases

  auto sleep_time = scrbr->get_scrub_sleep_time();
  if (sleep_time.count()) {
    // the following log line is used by osd-scrub-test.sh
    dout(20) << __func__ << " scrub state is PendingTimer, sleeping" << dendl;

    dout(20) << "PgScrubber: " << scrbr->get_spgid()
	     << " sleeping for " << sleep_time << dendl;
    m_sleep_timer = machine.schedule_timer_event_after<SleepComplete>(
      sleep_time);
  } else {
    scrbr->queue_for_scrub_resched(Scrub::scrub_prio_t::high_priority);
  }
}

sc::result PendingTimer::react(const SleepComplete&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "PendingTimer::react(const SleepComplete&)" << dendl;

  auto slept_for = ScrubClock::now() - entered_at;
  dout(20) << "PgScrubber: " << scrbr->get_spgid()
	   << " slept for " << slept_for << dendl;

  scrbr->queue_for_scrub_resched(Scrub::scrub_prio_t::low_priority);
  return discard_event();
}

// ----------------------- NewChunk -----------------------------------

/**
 *  Preconditions:
 *  - preemption data was set
 *  - epoch start was updated
 */
NewChunk::NewChunk(my_context ctx)
    : my_base(ctx)
    , NamedSimply(context<ScrubMachine>().m_scrbr, "Session/Act/NewChunk")
{
  dout(10) << "-- state -->> Session/Act/NewChunk" << dendl;
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases

  scrbr->get_preemptor().adjust_parameters();

  //  choose range to work on
  //  select_range_n_notify() will signal either SelectedChunkFree or
  //  ChunkIsBusy. If 'busy', we transition to Blocked, and wait for the
  //  range to become available.
  scrbr->select_range_n_notify();
}

sc::result NewChunk::react(const SelectedChunkFree&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "NewChunk::react(const SelectedChunkFree&)" << dendl;

  scrbr->set_subset_last_update(scrbr->search_log_for_updates());
  return transit<WaitPushes>();
}

// ----------------------- WaitPushes -----------------------------------

WaitPushes::WaitPushes(my_context ctx)
    : my_base(ctx)
    , NamedSimply(context<ScrubMachine>().m_scrbr, "Session/Act/WaitPushes")
{
  dout(10) << " -- state -->> Session/Act/WaitPushes" << dendl;
  post_event(ActivePushesUpd{});
}

/*
 * Triggered externally, by the entity that had an update re pushes
 */
sc::result WaitPushes::react(const ActivePushesUpd&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10)
    << "WaitPushes::react(const ActivePushesUpd&) pending_active_pushes: "
    << scrbr->pending_active_pushes() << dendl;

  if (!scrbr->pending_active_pushes()) {
    // done waiting
    return transit<WaitLastUpdate>();
  }

  return discard_event();
}

// ----------------------- WaitLastUpdate -----------------------------------

WaitLastUpdate::WaitLastUpdate(my_context ctx)
    : my_base(ctx)
    , NamedSimply(context<ScrubMachine>().m_scrbr, "Session/Act/WaitLastUpdate")
{
  dout(10) << " -- state -->> Session/Act/WaitLastUpdate" << dendl;
  post_event(UpdatesApplied{});
}

/**
 *  Note:
 *  Updates are locally readable immediately. Thus, on the replicas we do need
 *  to wait for the update notifications before scrubbing. For the Primary it's
 *  a bit different: on EC (and only there) rmw operations have an additional
 *  read roundtrip. That means that on the Primary we need to wait for
 *  last_update_applied (the replica side, even on EC, is still safe
 *  since the actual transaction will already be readable by commit time.
 */
void WaitLastUpdate::on_new_updates(const UpdatesApplied&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "WaitLastUpdate::on_new_updates(const UpdatesApplied&)" << dendl;

  if (scrbr->has_pg_marked_new_updates()) {
    post_event(InternalAllUpdates{});
  } else {
    // will be requeued by op_applied
    dout(10) << "wait for EC read/modify/writes to queue" << dendl;
  }
}

/*
 *  request maps from the replicas in the acting set
 */
sc::result WaitLastUpdate::react(const InternalAllUpdates&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "WaitLastUpdate::react(const InternalAllUpdates&)" << dendl;

  scrbr->get_replicas_maps(scrbr->get_preemptor().is_preemptable());
  return transit<BuildMap>();
}

// ----------------------- BuildMap -----------------------------------

BuildMap::BuildMap(my_context ctx)
    : my_base(ctx)
    , NamedSimply(context<ScrubMachine>().m_scrbr, "Session/Act/BuildMap")
{
  dout(10) << " -- state -->> Session/Act/BuildMap" << dendl;
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  auto& session = context<Session>();

  // no need to check for an epoch change, as all possible flows that brought
  // us here have a check_interval() verification of their final event.

  if (scrbr->get_preemptor().was_preempted()) {

    // we were preempted, either directly or by a replica
    dout(10) << __func__ << " preempted!!!" << dendl;
    scrbr->mark_local_map_ready();
    post_event(IntBmPreempted{});
    session.m_perf_set->inc(scrbcnt_preempted);

  } else {

    // note that build_primary_map_chunk() may return -EINPROGRESS, but no
    // other error value (as those errors would cause it to crash the OSD).
    if (scrbr->build_primary_map_chunk() == -EINPROGRESS) {
      // must wait for the backend to finish. No specific event provided.
      // build_primary_map_chunk() has already requeued us.
      dout(20) << "waiting for the backend..." << dendl;

    } else {

      // the local map was created
      post_event(IntLocalMapDone{});
    }
  }
}

sc::result BuildMap::react(const IntLocalMapDone&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "BuildMap::react(const IntLocalMapDone&)" << dendl;

  scrbr->mark_local_map_ready();
  return transit<WaitReplicas>();
}

// ----------------------- DrainReplMaps -----------------------------------

DrainReplMaps::DrainReplMaps(my_context ctx)
    : my_base(ctx)
    , NamedSimply(context<ScrubMachine>().m_scrbr, "Session/Act/DrainReplMaps")
{
  dout(10) << "-- state -->> Session/Act/DrainReplMaps" << dendl;
  // we may have got all maps already. Send the event that will make us check.
  post_event(GotReplicas{});
}

sc::result DrainReplMaps::react(const GotReplicas&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "DrainReplMaps::react(const GotReplicas&)" << dendl;

  if (scrbr->are_all_maps_available()) {
    // NewChunk will handle the preemption that brought us to this state
    return transit<PendingTimer>();
  }

  dout(15) << "DrainReplMaps::react(const GotReplicas&): still draining "
	      "incoming maps: "
	   << scrbr->dump_awaited_maps() << dendl;
  return discard_event();
}

// ----------------------- WaitReplicas -----------------------------------

WaitReplicas::WaitReplicas(my_context ctx)
    : my_base(ctx)
    , NamedSimply(context<ScrubMachine>().m_scrbr, "Session/Act/WaitReplicas")
{
  dout(10) << "-- state -->> Session/Act/WaitReplicas" << dendl;
  post_event(GotReplicas{});
}

/**
 * note: now that maps_compare_n_cleanup() is "futurized"(*), and we remain in
 * this state for a while even after we got all our maps, we must prevent
 * are_all_maps_available() (actually - the code after the if()) from being
 * called more than once.
 * This is basically a separate state, but it's too transitory and artificial
 * to justify the cost of a separate state.

 * (*) "futurized" - in Crimson, the call to maps_compare_n_cleanup() returns
 * immediately after initiating the process. The actual termination of the
 * maps comparing etc' is signalled via an event. As we share the code with
 * "classic" OSD, here too maps_compare_n_cleanup() is responsible for
 * signalling the completion of the processing.
 */
sc::result WaitReplicas::react(const GotReplicas&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "WaitReplicas::react(const GotReplicas&)" << dendl;

  if (!all_maps_already_called && scrbr->are_all_maps_available()) {
    dout(10) << "WaitReplicas::react(const GotReplicas&) got all" << dendl;

    all_maps_already_called = true;

    // were we preempted?
    if (scrbr->get_preemptor().disable_and_test()) {  // a test&set


      dout(10) << "WaitReplicas::react(const GotReplicas&) PREEMPTED!" << dendl;
      return transit<PendingTimer>();

    } else {
      scrbr->maps_compare_n_cleanup();
      return transit<WaitDigestUpdate>();
    }
  } else {
    return discard_event();
  }
}

sc::result WaitReplicas::react(const DigestUpdate&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  auto warn_msg =
    "WaitReplicas::react(const DigestUpdate&): Unexpected DigestUpdate event"s;
  dout(10) << warn_msg << dendl;
  scrbr->log_cluster_warning(warn_msg);
  return discard_event();
}

// ----------------------- WaitDigestUpdate -----------------------------------

WaitDigestUpdate::WaitDigestUpdate(my_context ctx)
    : my_base(ctx)
    , NamedSimply(context<ScrubMachine>().m_scrbr, "Session/Act/WaitDigestUpdate")
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "-- state -->> Session/Act/WaitDigestUpdate" << dendl;

  // perform an initial check: maybe we already
  // have all the updates we need:
  // (note that DigestUpdate is usually an external event)
  post_event(DigestUpdate{});
}

sc::result WaitDigestUpdate::react(const DigestUpdate&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "WaitDigestUpdate::react(const DigestUpdate&)" << dendl;

  // on_digest_updates() will either:
  // - do nothing - if we are still waiting for updates, or
  // - finish the scrubbing of the current chunk, and:
  //  - send NextChunk, or
  //  - send ScrubFinished
  scrbr->on_digest_updates();
  return discard_event();
}

sc::result WaitDigestUpdate::react(const ScrubFinished&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "WaitDigestUpdate::react(const ScrubFinished&)" << dendl;
  auto& session = context<Session>();

  session.m_perf_set->inc(scrbcnt_successful);

  // set the 'scrub duration'
  auto duration = machine.get_time_scrubbing();
  session.m_perf_set->tinc(scrbcnt_successful_elapsed, duration);
  scrbr->set_scrub_duration(duration_cast<milliseconds>(duration));
  session.m_session_started_at = ScrubTimePoint{};

  scrbr->scrub_finish();
  return transit<PrimaryIdle>();
}

ScrubMachine::ScrubMachine(PG* pg, ScrubMachineListener* pg_scrub)
    : m_pg_id{pg->pg_id}
    , m_scrbr{pg_scrub}
{}

ScrubMachine::~ScrubMachine() = default;


// -------- for replicas -----------------------------------------------------

// ----------------------- ReplicaActive --------------------------------

ReplicaActive::ReplicaActive(my_context ctx)
    : my_base(ctx)
    , NamedSimply(context<ScrubMachine>().m_scrbr, "ReplicaActive")
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "-- state -->> ReplicaActive" << dendl;
  m_pg = scrbr->get_pg();
  m_osds = m_pg->get_pg_osd(ScrubberPasskey());
}

ReplicaActive::~ReplicaActive()
{
  clear_remote_reservation(false);
}

/*
 * Note: we are expected to be in the initial internal state (Idle) when
 * receiving any registration request. Our other internal states, the
 * active ones, have their own handler for this event, and will treat it
 * as an abort request.
 *
 * Process:
 * - if already reserved: clear existing reservation, then continue
 * - for async requests:
 *   - enqueue the request with reserver;
 *   - move to the ReplicaWaitingReservation state
 *   - no reply is expected by the caller
 * - for legacy requests:
 *   - ask the OSD for the "reservation resource"
 *   - if granted: move to ReplicaReserved and notify the Primary.
 *   - otherwise: just notify the requesting primary.
 *
 * implementation note: sc::result objects cannot be copied or moved. Thus,
 * we've resorted to returning a code indicating the next action.
 */
ReplicaReactCode ReplicaActive::on_reserve_request(
    const ReplicaReserveReq& ev,
    bool async_request)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  const auto m = ev.m_op->get_req<MOSDScrubReserve>();
  dout(10) << fmt::format(
		  "ReplicaActive::on_reserve_req() request:{} async_request:{} "
		  "reservation_nonce:{}",
		  ev, async_request, m->reservation_nonce)
	   << dendl;

  ceph_assert(!reservation_granted);
  ceph_assert(!pending_reservation_nonce);
  ReplicaReactCode next_action{ReplicaReactCode::discard};
  AsyncScrubResData request_details{
      pg_id, ev.m_from, ev.m_op->sent_epoch, m->reservation_nonce};
  auto& reserver = m_osds->get_scrub_reserver();

  if (async_request) {
    // the request is to be handled asynchronously
    dout(20) << fmt::format(
		    "{}: async request: {} details:{}", __func__, ev,
		    request_details)
	     << dendl;
    pending_reservation_nonce = m->reservation_nonce;
    const auto reservation_cb = new RtReservationCB(m_pg, request_details);
    reserver.request_reservation(pg_id, reservation_cb, 0, nullptr);
    next_action = ReplicaReactCode::goto_waiting_reservation;

  } else {
    // an immediate yes/no is required
    Message* reply{nullptr};
    reservation_granted = reserver.request_reservation_or_fail(pg_id);
    if (reservation_granted) {
      dout(10) << fmt::format("{}: reserved? yes", __func__) << dendl;
      reply = new MOSDScrubReserve(
	  spg_t(pg_id.pgid, m_pg->get_primary().shard), ev.m_op->sent_epoch,
	  MOSDScrubReserve::GRANT, m_pg->pg_whoami, m->reservation_nonce);
      next_action = ReplicaReactCode::goto_replica_reserved;

    } else {
      dout(10) << fmt::format("{}: reserved? no", __func__) << dendl;
      reply = new MOSDScrubReserve(
	  spg_t(pg_id.pgid, m_pg->get_primary().shard), ev.m_op->sent_epoch,
	  MOSDScrubReserve::REJECT, m_pg->pg_whoami, m->reservation_nonce);
      // the event is discarded
      next_action = ReplicaReactCode::discard;
    }

    m_osds->send_message_osd_cluster(
	reply, ev.m_op->get_req()->get_connection());
  }

  return next_action;
}

bool ReplicaActive::granted_by_reserver(const AsyncScrubResData& reservation)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << fmt::format("{}: reservation granted: {}", __func__, reservation)
	   << dendl;

  /// verify that the granted reservation is the one we were waiting for
  if (reservation.nonce != pending_reservation_nonce) {
    dout(5) << fmt::format(
	"{}: reservation_nonce mismatch: {} != {}", __func__, reservation.nonce,
	pending_reservation_nonce) << dendl;
    return false;
  }

  reservation_granted = true;
  pending_reservation_nonce = 0;  // no longer pending

  // notify the primary
  auto grant_msg = make_message<MOSDScrubReserve>(
      spg_t(pg_id.pgid, m_pg->get_primary().shard), reservation.request_epoch,
      MOSDScrubReserve::GRANT, m_pg->pg_whoami, pending_reservation_nonce);
  m_pg->send_cluster_message(
      m_pg->get_primary().osd, grant_msg, reservation.request_epoch, false);
  return true;
}

void ReplicaActive::on_release(const ReplicaRelease& ev)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << fmt::format("ReplicaActive::on_release() from {}", ev.m_from)
	   << dendl;
  clear_remote_reservation(true);
}

void ReplicaActive::clear_remote_reservation(bool warn_if_no_reservation)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << fmt::format(
		  "ReplicaActive::clear_remote_reservation(): "
		  "pending_reservation_nonce {}, reservation_granted {}",
		  pending_reservation_nonce, reservation_granted)
	   << dendl;
  if (reservation_granted || pending_reservation_nonce) {
    m_osds->get_scrub_reserver().cancel_reservation(pg_id);
    reservation_granted = false;
    pending_reservation_nonce = 0;
  } else if (warn_if_no_reservation) {
    const auto msg =
	"ReplicaActive::clear_remote_reservation(): "
	"not reserved!";
    dout(5) << msg << dendl;
    scrbr->get_clog()->warn() << msg;
  }
}

void ReplicaActive::ignore_unhandled_grant(const ReserverGranted&)
{
  dout(10) << "ReplicaActive::react(const ReserverGranted&): ignored"
	   << dendl;
}


// ---------------- ReplicaActive/ReplicaIdle ---------------------------

ReplicaIdle::ReplicaIdle(my_context ctx)
    : my_base(ctx)
    , NamedSimply(context<ScrubMachine>().m_scrbr, "ReplicaActive/ReplicaIdle")
{
  dout(10) << "-- state -->> ReplicaActive/ReplicaIdle" << dendl;
}

void ReplicaIdle::reset_ignored(const FullReset&)
{
  dout(10) << "ReplicaIdle::react(const FullReset&): FullReset ignored"
	   << dendl;
}


// ---------------- ReplicaIdle/ReplicaUnreserved ---------------------------

ReplicaUnreserved::ReplicaUnreserved(my_context ctx)
    : my_base(ctx)
    , NamedSimply(
	  context<ScrubMachine>().m_scrbr,
	  "ReplicaActive/ReplicaIdle/ReplicaUnreserved")
{
  dout(10) << "-- state -->> ReplicaActive/ReplicaIdle/ReplicaUnreserved"
	   << dendl;
}

sc::result ReplicaUnreserved::react(const ReplicaReserveReq& ev)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "ReplicaUnreserved::react(const ReplicaReserveReq&)" << dendl;

  const auto& m = *(ev.m_op->get_req<MOSDScrubReserve>());
  const auto async_disabled = scrbr->get_pg_cct()->_conf.get_val<bool>(
      "osd_scrub_disable_reservation_queuing");
  const bool async_request = !async_disabled && m.wait_for_resources;
  dout(15) << fmt::format(
		  "ReplicaUnreserved::react(const ReplicaReserveReq&): "
		  "request:{} disabled?:{} -> async? {}", m.wait_for_resources,
		  async_disabled, async_request)
	   << dendl;

  switch (context<ReplicaActive>().on_reserve_request(ev, async_request)) {
    case ReplicaReactCode::discard:
      return discard_event();
    case ReplicaReactCode::goto_waiting_reservation:
      return transit<ReplicaWaitingReservation>();
    case ReplicaReactCode::goto_replica_reserved:
      return transit<ReplicaReserved>();
    default:
      ceph_abort_msg("unexpected return value");
  }
  // can't happen, but some compilers complain:
  return transit<ReplicaReserved>();
}

sc::result ReplicaUnreserved::react(const StartReplica& ev)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "ReplicaUnreserved::react(const StartReplica&)" << dendl;
  post_event(ReplicaPushesUpd{});
  return transit<ReplicaActiveOp>();
}

sc::result ReplicaUnreserved::react(const ReplicaRelease&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "ReplicaUnreserved::react(const ReplicaRelease&)" << dendl;
  // this is a bug. We should never receive a release request unless we
  // are reserved or have a pending reservation.
  scrbr->get_clog()->error() << fmt::format(
      "osd.{} pg[{}]: reservation released while not reserved",
      scrbr->get_whoami(), scrbr->get_spgid());
  return discard_event();
}

sc::result ReplicaUnreserved::react(const ReserverGranted&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "ReplicaUnreserved::react(const ReserverGranted&)" << dendl;
  // shouldn't happen. Might be a result of a cancelled reservation
  // that was still delivered.
  // must unreserve
  dout(5) << "ReplicaUnreserved::react(const ReserverGranted&): reservation "
	     "granted while not being waited for"
	  << dendl;
  context<ReplicaActive>().clear_remote_reservation(false);
  return discard_event();
}


// ---------------- ReplicaIdle/ReplicaWaitingReservation ---------------------------

ReplicaWaitingReservation::ReplicaWaitingReservation(my_context ctx)
    : my_base(ctx)
    , NamedSimply(
	  context<ScrubMachine>().m_scrbr,
	  "ReplicaActive/ReplicaIdle/ReplicaWaitingReservation")
{
  dout(10)
      << "-- state -->> ReplicaActive/ReplicaIdle/ReplicaWaitingReservation"
      << dendl;
}

sc::result ReplicaWaitingReservation::react(const ReserverGranted& ev)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << fmt::format(
		  "ReplicaWaitingReservation::react(const ReserverGranted&): "
		  "event:{}",
		  ev)
	   << dendl;
  if (context<ReplicaActive>().granted_by_reserver(ev.value)) {
    return transit<ReplicaReserved>();
  }
  return discard_event();
}

sc::result ReplicaWaitingReservation::react(const ReplicaRelease& ev)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "ReplicaWaitingReservation::react(const ReplicaRelease&)"
	   << dendl;
  // must cancel the queued reservation request
  context<ReplicaActive>().on_release(ev);
  return transit<ReplicaUnreserved>();
}

sc::result ReplicaWaitingReservation::react(const StartReplica& ev)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "ReplicaWaitingReservation::react(const StartReplica&)" << dendl;

  // this shouldn't happen. We will handle it, but will also log an error.
  scrbr->get_clog()->error() << fmt::format(
      "osd.{} pg[{}]: new chunk request while still waiting for "
      "reservation",
      scrbr->get_whoami(), scrbr->get_spgid());
  context<ReplicaActive>().clear_remote_reservation(true);
  post_event(ReplicaPushesUpd{});
  return transit<ReplicaActiveOp>();
}

sc::result ReplicaWaitingReservation::react(const ReplicaReserveReq& ev)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "ReplicaWaitingReservation::react(const ReplicaReserveReq&)"
	   << dendl;
  // this shouldn't happen. We will handle it, but will also log an error.
  scrbr->get_clog()->error() << fmt::format(
      "osd.{} pg[{}]: reservation requested while previous is pending",
      scrbr->get_whoami(), scrbr->get_spgid());
  // cancel the existing reservation, and re-request
  context<ReplicaActive>().clear_remote_reservation(true);
  post_event(ev);
  return transit<ReplicaUnreserved>();
}


// ---------------- ReplicaIdle/ReplicaReserved ---------------------------

ReplicaReserved::ReplicaReserved(my_context ctx)
    : my_base(ctx)
    , NamedSimply(
	  context<ScrubMachine>().m_scrbr,
	  "ReplicaActive/ReplicaIdle/ReplicaReserved")
{
  dout(10) << "-- state -->> ReplicaActive/ReplicaIdle/ReplicaReserved"
	   << dendl;
}

sc::result ReplicaReserved::react(const ReplicaRelease& ev)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "ReplicaReserved::react(const ReplicaRelease&)" << dendl;
  context<ReplicaActive>().on_release(ev);
  return transit<ReplicaUnreserved>();
}

sc::result ReplicaReserved::react(const ReplicaReserveReq& ev)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "ReplicaReserved::react(const ReplicaReserveReq&)" << dendl;
  scrbr->get_clog()->error() << fmt::format(
      "osd.{} pg[{}]: reservation requested while still reserved",
      scrbr->get_whoami(), scrbr->get_spgid());
  // This is a bug. We should never receive a new request unless the
  // previous one was cancelled - either by the primary, or on interval
  // change.
  // cancel the existing reservation, and re-request
  context<ReplicaActive>().clear_remote_reservation(true);
  post_event(ev);
  return transit<ReplicaUnreserved>();
}

sc::result ReplicaReserved::react(const StartReplica& ev)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "ReplicaReserved::react(const StartReplica&)" << dendl;
  post_event(ReplicaPushesUpd{});
  return transit<ReplicaActiveOp>();
}


// ------------- ReplicaActive/ReplicaActiveOp --------------------------

ReplicaActiveOp::ReplicaActiveOp(my_context ctx)
    : my_base(ctx)
    , NamedSimply(context<ScrubMachine>().m_scrbr, "ReplicaActiveOp")
{
  dout(10) << "-- state -->> ReplicaActive/ReplicaActiveOp" << dendl;
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  scrbr->on_replica_init();
}


ReplicaActiveOp::~ReplicaActiveOp()
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << __func__ << dendl;
  scrbr->replica_handling_done();
}

sc::result ReplicaActiveOp::react(const StartReplica&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "ReplicaActiveOp::react(const StartReplica&)" << dendl;

  const auto msg = fmt::format(
      "osd.{} pg[{}]: new chunk request while still handling the previous one",
      scrbr->get_whoami(), scrbr->get_spgid());
  dout(1) << msg << dendl;
  scrbr->get_clog()->warn() << msg;

  // exit & re-enter the state
  post_event(ReplicaPushesUpd{});
  return transit<ReplicaActiveOp>();
}

sc::result ReplicaActiveOp::react(const ReplicaRelease& ev)
{
  dout(10) << "ReplicaActiveOp::react(const ReplicaRelease&)" << dendl;
  post_event(ev);
  return transit<ReplicaReserved>();
}


// ------------- ReplicaActive/ReplicaWaitUpdates ------------------------

ReplicaWaitUpdates::ReplicaWaitUpdates(my_context ctx)
    : my_base(ctx)
    , NamedSimply(
	  context<ScrubMachine>().m_scrbr,
	  "ReplicaActive/ReplicaActiveOp/ReplicaWaitUpdates")
{
  dout(10) << "-- state -->> ReplicaActive/ReplicaActiveOp/ReplicaWaitUpdates"
	   << dendl;
}


/*
 * Triggered externally, by the entity that had an update re pushes
 */
sc::result ReplicaWaitUpdates::react(const ReplicaPushesUpd&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "ReplicaWaitUpdates::react(const ReplicaPushesUpd&): "
	   << scrbr->pending_active_pushes() << dendl;

  if (scrbr->pending_active_pushes() == 0) {
    // done waiting
    return transit<ReplicaBuildingMap>();
  }

  return discard_event();
}


// ----------------------- ReplicaBuildingMap -----------------------------------

ReplicaBuildingMap::ReplicaBuildingMap(my_context ctx)
    : my_base(ctx)
    , NamedSimply(
	  context<ScrubMachine>().m_scrbr,
	  "ReplicaActive/ReplicaActiveOp/ReplicaBuildingMap")
{
  dout(10) << "-- state -->> ReplicaActive/ReplicaActiveOp/ReplicaBuildingMap"
	   << dendl;
  post_event(SchedReplica{});
}


sc::result ReplicaBuildingMap::react(const SchedReplica&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "ReplicaBuildingMap::react(const SchedReplica&). is_preemptable? "
	   << scrbr->get_preemptor().is_preemptable() << dendl;

  if (scrbr->get_preemptor().was_preempted()) {
    dout(10) << "replica scrub job preempted" << dendl;

    scrbr->send_preempted_replica();
    return transit<ReplicaReserved>();
  }

  // start or check progress of build_replica_map_chunk()
  auto ret_init = scrbr->build_replica_map_chunk();
  if (ret_init != -EINPROGRESS) {
    dout(10) << "ReplicaBuildingMap::react(const SchedReplica&): back to idle"
	     << dendl;
    return transit<ReplicaReserved>();
  }

  dout(20) << "ReplicaBuildingMap::react(const SchedReplica&): discarded"
	   << dendl;
  return discard_event();
}

}  // namespace Scrub
