// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "scrub_machine.h"

#include <chrono>

#include "OSD.h"
#include "OpRequest.h"
#include "ScrubStore.h"
#include "scrub_machine_lstnr.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << " scrubberFSM "


using namespace std::chrono;
using namespace std::chrono_literals;
namespace sc = boost::statechart;

#define DECLARE_LOCALS                                           \
  ScrubMachineListener* scrbr = context<ScrubMachine>().m_scrbr; \
  std::ignore = scrbr;                                           \
  auto pg_id = context<ScrubMachine>().m_pg_id;                  \
  std::ignore = pg_id;

namespace Scrub {

// --------- trace/debug auxiliaries -------------------------------

void on_event_creation(std::string_view nm)
{
  dout(20) << " scrubberFSM event: --vvvv---- " << nm << dendl;
}

void on_event_discard(std::string_view nm)
{
  dout(20) << " scrubberFSM event: --^^^^---- " << nm << dendl;
}

void ScrubMachine::my_states() const
{
  for (auto si = state_begin(); si != state_end(); ++si) {
    const auto& siw{*si};  // prevents a warning re side-effects
    dout(20) << __func__ << " : scrub-states : " << typeid(siw).name() << dendl;
  }
}

void ScrubMachine::assert_not_active() const
{
  ceph_assert(state_cast<const NotActive*>());
}

bool ScrubMachine::is_reserving() const
{
  return state_cast<const ReservingReplicas*>();
}

// for the rest of the code in this file - we know what PG we are dealing with:
#undef dout_prefix
#define dout_prefix _prefix(_dout, this->context<ScrubMachine>().m_pg)
template <class T> static ostream& _prefix(std::ostream* _dout, T* t)
{
  return t->gen_prefix(*_dout) << " scrubberFSM pg(" << t->pg_id << ") ";
}

// ////////////// the actual actions

// ----------------------- NotActive -----------------------------------------

NotActive::NotActive(my_context ctx) : my_base(ctx)
{
  dout(10) << "-- state -->> NotActive" << dendl;
}

sc::result NotActive::react(const IntervalChanged&)
{
  dout(15) << "NotActive::react(const IntervalChanged&)" << dendl;
  return discard_event();
}

// ----------------------- ReservingReplicas ---------------------------------

ReservingReplicas::ReservingReplicas(my_context ctx) : my_base(ctx)
{
  dout(10) << "-- state -->> ReservingReplicas" << dendl;
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  scrbr->reserve_replicas();
}

sc::result ReservingReplicas::react(const ReservationFailure&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "ReservingReplicas::react(const ReservationFailure&)" << dendl;

  // the Scrubber must release all resources and abort the scrubbing
  scrbr->clear_pgscrub_state();
  return transit<NotActive>();
}

/**
 * note: the event poster is handling the scrubber reset
 */
sc::result ReservingReplicas::react(const FullReset&)
{
  dout(10) << "ReservingReplicas::react(const FullReset&)" << dendl;
  return transit<NotActive>();
}

// ----------------------- ActiveScrubbing -----------------------------------

ActiveScrubbing::ActiveScrubbing(my_context ctx) : my_base(ctx)
{
  dout(10) << "-- state -->> ActiveScrubbing" << dendl;
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  scrbr->on_init();
}

/**
 *  upon exiting the Active state
 */
ActiveScrubbing::~ActiveScrubbing()
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(15) << __func__ << dendl;
  scrbr->unreserve_replicas();
}

/*
 * The only source of an InternalError event as of now is the BuildMap state,
 * when encountering a backend error.
 * We kill the scrub and reset the FSM.
 */
sc::result ActiveScrubbing::react(const InternalError&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << __func__ << dendl;
  scrbr->clear_pgscrub_state();
  return transit<NotActive>();
}

sc::result ActiveScrubbing::react(const FullReset&)
{
  dout(10) << "ActiveScrubbing::react(const FullReset&)" << dendl;
  // caller takes care of clearing the scrubber & FSM states
  return transit<NotActive>();
}

// ----------------------- RangeBlocked -----------------------------------

/*
 * Blocked. Will be released by kick_object_context_blocked() (or upon
 * an abort)
 */
RangeBlocked::RangeBlocked(my_context ctx) : my_base(ctx)
{
  dout(10) << "-- state -->> Act/RangeBlocked" << dendl;
}

// ----------------------- PendingTimer -----------------------------------

/**
 *  Sleeping till timer reactivation - or just requeuing
 */
PendingTimer::PendingTimer(my_context ctx) : my_base(ctx)
{
  dout(10) << "-- state -->> Act/PendingTimer" << dendl;
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases

  scrbr->add_delayed_scheduling();
}

// ----------------------- NewChunk -----------------------------------

/**
 *  Preconditions:
 *  - preemption data was set
 *  - epoch start was updated
 */
NewChunk::NewChunk(my_context ctx) : my_base(ctx)
{
  dout(10) << "-- state -->> Act/NewChunk" << dendl;
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases

  scrbr->get_preemptor().adjust_parameters();

  //  choose range to work on
  bool got_a_chunk = scrbr->select_range();
  if (got_a_chunk) {
    dout(15) << __func__ << " selection OK" << dendl;
    post_event(SelectedChunkFree{});
  } else {
    dout(10) << __func__ << " selected chunk is busy" << dendl;
    // wait until we are available (transitioning to Blocked)
    post_event(ChunkIsBusy{});
  }
}

sc::result NewChunk::react(const SelectedChunkFree&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "NewChunk::react(const SelectedChunkFree&)" << dendl;

  scrbr->set_subset_last_update(scrbr->search_log_for_updates());
  return transit<WaitPushes>();
}

// ----------------------- WaitPushes -----------------------------------

WaitPushes::WaitPushes(my_context ctx) : my_base(ctx)
{
  dout(10) << " -- state -->> Act/WaitPushes" << dendl;
  post_event(ActivePushesUpd{});
}

/*
 * Triggered externally, by the entity that had an update re pushes
 */
sc::result WaitPushes::react(const ActivePushesUpd&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "WaitPushes::react(const ActivePushesUpd&) pending_active_pushes: "
	   << scrbr->pending_active_pushes() << dendl;

  if (!scrbr->pending_active_pushes()) {
    // done waiting
    return transit<WaitLastUpdate>();
  }

  return discard_event();
}

// ----------------------- WaitLastUpdate -----------------------------------

WaitLastUpdate::WaitLastUpdate(my_context ctx) : my_base(ctx)
{
  dout(10) << " -- state -->> Act/WaitLastUpdate" << dendl;
  post_event(UpdatesApplied{});
}

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

BuildMap::BuildMap(my_context ctx) : my_base(ctx)
{
  dout(10) << " -- state -->> Act/BuildMap" << dendl;
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases

  // no need to check for an epoch change, as all possible flows that brought us here have
  // an check_interval() verification of their final event.

  if (scrbr->get_preemptor().was_preempted()) {

    // we were preempted, either directly or by a replica
    dout(10) << __func__ << " preempted!!!" << dendl;
    scrbr->mark_local_map_ready();
    post_event(IntBmPreempted{});

  } else {

    auto ret = scrbr->build_primary_map_chunk();

    if (ret == -EINPROGRESS) {
      // must wait for the backend to finish. No specific event provided.
      // build_primary_map_chunk() has already requeued us.
      dout(20) << "waiting for the backend..." << dendl;

    } else if (ret < 0) {

      dout(10) << "BuildMap::BuildMap() Error! Aborting. Ret: " << ret << dendl;
      // scrbr->mark_local_map_ready();
      post_event(InternalError{});

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

DrainReplMaps::DrainReplMaps(my_context ctx) : my_base(ctx)
{
  dout(10) << "-- state -->> Act/DrainReplMaps" << dendl;
  // we may have received all maps already. Send the event that will make us check.
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

  dout(15) << "DrainReplMaps::react(const GotReplicas&): still draining incoming maps: "
	   << scrbr->dump_awaited_maps() << dendl;
  return discard_event();
}

// ----------------------- WaitReplicas -----------------------------------

WaitReplicas::WaitReplicas(my_context ctx) : my_base(ctx)
{
  dout(10) << "-- state -->> Act/WaitReplicas" << dendl;
  post_event(GotReplicas{});
}

sc::result WaitReplicas::react(const GotReplicas&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "WaitReplicas::react(const GotReplicas&)" << dendl;

  if (scrbr->are_all_maps_available()) {
    dout(10) << "WaitReplicas::react(const GotReplicas&) got all" << dendl;

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

// ----------------------- WaitDigestUpdate -----------------------------------

WaitDigestUpdate::WaitDigestUpdate(my_context ctx) : my_base(ctx)
{
  dout(10) << "-- state -->> Act/WaitDigestUpdate" << dendl;
  // perform an initial check: maybe we already
  // have all the updates we need:
  // (note that DigestUpdate is usually an external event)
  post_event(DigestUpdate{});
}

sc::result WaitDigestUpdate::react(const DigestUpdate&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "WaitDigestUpdate::react(const DigestUpdate&)" << dendl;

  switch (scrbr->on_digest_updates()) {

    case Scrub::FsmNext::goto_notactive:
      // scrubbing is done
      return transit<NotActive>();

    case Scrub::FsmNext::next_chunk:
      // go get the next chunk
      return transit<PendingTimer>();

    case Scrub::FsmNext::do_discard:
      // still waiting for more updates
      return discard_event();
  }
  __builtin_unreachable();  // Prevent a gcc warning.
			    // Adding a phony 'default:' above is wrong: (a) prevents a
			    // warning if FsmNext is extended, and (b) elicits a correct
			    // warning from Clang
}

ScrubMachine::ScrubMachine(PG* pg, ScrubMachineListener* pg_scrub)
    : m_pg{pg}, m_pg_id{pg->pg_id}, m_scrbr{pg_scrub}
{
  dout(15) << "ScrubMachine created " << m_pg_id << dendl;
}

ScrubMachine::~ScrubMachine()
{
  dout(20) << "~ScrubMachine " << m_pg_id << dendl;
}

// -------- for replicas -----------------------------------------------------

// ----------------------- ReplicaWaitUpdates --------------------------------

ReplicaWaitUpdates::ReplicaWaitUpdates(my_context ctx) : my_base(ctx)
{
  dout(10) << "-- state -->> ReplicaWaitUpdates" << dendl;
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  scrbr->on_replica_init();
}

sc::result ReplicaWaitUpdates::react(const IntervalChanged&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "ReplicaWaitUpdates::react(const IntervalChanged&)" << dendl;

  // note: the master's reservation of us was just discarded by our caller
  scrbr->replica_handling_done();
  return transit<NotActive>();
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
    return transit<ActiveReplica>();
  }

  return discard_event();
}

/**
 * the event poster is handling the scrubber reset
 */
sc::result ReplicaWaitUpdates::react(const FullReset&)
{
  dout(10) << "ReplicaWaitUpdates::react(const FullReset&)" << dendl;
  return transit<NotActive>();
}

// ----------------------- ActiveReplica -----------------------------------

ActiveReplica::ActiveReplica(my_context ctx) : my_base(ctx)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "-- state -->> ActiveReplica" << dendl;
  scrbr->on_replica_init();  // as we might have skipped ReplicaWaitUpdates
  post_event(SchedReplica{});
}

sc::result ActiveReplica::react(const SchedReplica&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "ActiveReplica::react(const SchedReplica&). is_preemptable? "
	   << scrbr->get_preemptor().is_preemptable() << dendl;

  if (scrbr->get_preemptor().was_preempted()) {
    dout(10) << "replica scrub job preempted" << dendl;

    scrbr->send_replica_map(PreemptionNoted::preempted);
    scrbr->replica_handling_done();
    return transit<NotActive>();
  }

  // start or check progress of build_replica_map_chunk()

  auto ret = scrbr->build_replica_map_chunk();
  dout(15) << "ActiveReplica::react(const SchedReplica&) Ret: " << ret << dendl;

  if (ret == -EINPROGRESS) {
    // must wait for the backend to finish. No external event source.
    // build_replica_map_chunk() has already requeued a SchedReplica
    // event.

    dout(20) << "waiting for the backend..." << dendl;
    return discard_event();
  }

  if (ret < 0) {
    //  the existing code ignores this option, treating an error
    //  report as a success.
    dout(1) << "Error! Aborting. ActiveReplica::react(SchedReplica) Ret: " << ret
	    << dendl;
    scrbr->replica_handling_done();
    return transit<NotActive>();
  }


  // the local map was created. Send it to the primary.
  scrbr->send_replica_map(PreemptionNoted::no_preemption);
  scrbr->replica_handling_done();
  return transit<NotActive>();
}

sc::result ActiveReplica::react(const IntervalChanged&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "ActiveReplica::react(const IntervalChanged&) " << dendl;

  scrbr->send_replica_map(PreemptionNoted::no_preemption);
  scrbr->replica_handling_done();
  return transit<NotActive>();
}

/**
 * the event poster is handling the scrubber reset
 */
sc::result ActiveReplica::react(const FullReset&)
{
  dout(10) << "ActiveReplica::react(const FullReset&)" << dendl;
  return transit<NotActive>();
}

}  // namespace Scrub
