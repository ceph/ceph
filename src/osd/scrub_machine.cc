// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "scrub_machine.h"

#include <chrono>

#include "OSD.h"
#include "OpRequest.h"
#include "ScrubStore.h"
#include "pg_scrubber.h"  // RRR try to remove this one
#include "scrub_machine_lstnr.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "scrbrFSM."


using namespace std::chrono;
using namespace std::chrono_literals;

namespace sc = boost::statechart;


// clang-format off

#define DECLARE_LOCALS                                         \
  PgScrubber* scrbr = context< ScrubMachine >().scrbr_;        \
  std::ignore = scrbr;                                         \
  PG* pg = context< ScrubMachine >().scrbr_->pg_;              \
  std::ignore = pg;

// clang-format on

namespace Scrub {

//
//  trace/debug auxiliaries
//

state_logger_t::state_logger_t(std::string nm) : name_{nm}
{
  dout(7) << " ------ state -->> " << name_ << dendl;
}

void on_event_creation(std::string nm)
{
  dout(7) << " event: -->>>>---------- " << nm << " ------vvvvvv-------------------------"
	  << dendl;
}

void on_event_discard(std::string nm)
{
  dout(7) << " event: --XXXX---------- " << nm << " ------^^^^^^-------------------------"
	  << dendl;
}

void ScrubMachine::my_states()
{
  for (auto si = state_begin(); si != state_end(); ++si) {
    const auto& siw{*si};  // prevents a warning
    dout(7) << __func__ << " : RRRRR : " << typeid(siw).name() << dendl;
  }
}

void ScrubMachine::assert_not_active()
{
  //ceph_assert(state_cast<const NotActive&>(), /* will throw anyway... */ true );
  ceph_assert(state_cast<const NotActive*>());
  auto&& something = state_cast<const NotActive&>();
  dout(20) << __func__ << " : RRRRR : " << typeid(something).name() << dendl;
}

#undef dout_prefix
#define dout_prefix \
  *_dout << "scrbrFSM.pg(" << context<ScrubMachine>().scrbr_->pg_->pg_id << ") "



// //////////////////////////////////////////////////////
// ////////////// the actual actions
// //////////////////////////////////////////////////////

// next phase states:
// sc::result ExtInactive::react(const EpochChanged&) { return transit<ExtInactive>(); }
// sc::result ExtInactive::react(const SchedScrub&) { return transit<ReservationsMade>();
// }

sc::result NotActive::react(const EpochChanged&)
{
  DECLARE_LOCALS;  // aliases for 'scrbr', 'pg' pointers
  dout(7) << " NotActive::react(const EpochChanged&)" << dendl;
  return discard_event();
}

// next phase states:
// sc::result ReservationsMade::react(const EpochChanged&) { return
// transit<ExtInactive>(); }
// sc::result ReservationsMade::react(const TmpEvent_1&) { return
// transit<ActiveScrubbing>(); }


sc::result NotActive::react(const StartScrub&)
{
  dout(7) << " NotActive::react(const StartScrub&)" << dendl;
  return transit<ActiveScrubbing>();
}

sc::result NotActive::react(const StartReplica&)
{
  dout(7) << " NotActive::react(const StartReplica&)" << dendl;
  return transit<ReplicaWaitUpdates>();
}

ActiveScrubbing::ActiveScrubbing(my_context ctx)
    : my_base(ctx), state_logger_t{"ActiveScrubbing"s}
{
  DECLARE_LOCALS;  // aliases for 'scrbr', 'pg' pointers

  dout(7) << "  " << __func__ << dendl;
  scrbr->on_init();
}

/**
 *  upon existing the Active state
 */
void ScrubMachine::down_from_active(const EpochChanged&)
{
  DECLARE_LOCALS;  // aliases for 'scrbr', 'pg' pointers
  dout(7) << "  " << __func__ << dendl;
}

void ScrubMachine::on_epoch_changed(const EpochChanged&)
{
  dout(7) << "  " << __func__ << dendl;
  // scrubber_->scrub_clear_state();
  // scrubber_->scrub_unreserve_replicas();
}


/**
 *  done scrubbing
 */
sc::result ActiveScrubbing::react(const AllChunksDone&)
{
  dout(7) << " ActiveScrubbing::react(const AllChunksDone&)" << dendl;

  return transit<NotActive>();
}

sc::result ActiveScrubbing::react(const FullReset&)
{
  dout(7) << " ActiveScrubbing::react(const FullReset&)" << dendl;

  // do what's needed to be done in the PG

  // RRR

  return transit<NotActive>();
}

/*
 * Sleeping till scheduled-in
 */
ActvNQueued::ActvNQueued(my_context ctx)
    : my_base(ctx), state_logger_t{"Act/ActvNQueued"s}
{
  DECLARE_LOCALS;  // aliases for 'scrbr', 'pg' pointers

  dout(7) << "  " << __func__ << dendl;

  // RRR 16/8/20: bad if arrived here after an unblock: scrbr->requeue();
}

sc::result ActvNQueued::react(const EpochChanged&)
{
  dout(7) << " ActvNQueued::react(const EpochChanged&)" << dendl;
  // fix:
  return forward_event();
}

/*
 * Blocked. Will be released by kick_object_context_blocked() (or upon
 * an abort)
 */
RangeBlocked::RangeBlocked(my_context ctx)
    : my_base(ctx), state_logger_t{"Act/RangeBlocked"s}
{
  DECLARE_LOCALS;  // aliases for 'scrbr', 'pg' pointers

  dout(7) << "  " << __func__ << dendl;
}

sc::result RangeBlocked::react(const EpochChanged&)
{
  dout(7) << " RangeBlocked::react(const EpochChanged&)" << dendl;
  // fix:
  return forward_event();
}

/**
 *  Sleeping till timer reactivation - or just requeuing
 */
PendingTimer::PendingTimer(my_context ctx)
    : my_base(ctx), state_logger_t{"Act/PendingTimer"s}
{
  DECLARE_LOCALS;  // aliases for 'scrbr', 'pg' pointers

  // schedule a transition some sleep_time ms in the future, or (if
  // no sleep is required) - just a trip thru the OSD op-queue

  scrbr->add_delayed_scheduling();
}

sc::result PendingTimer::react(const EpochChanged&)
{
  // fix:
  dout(7) << "  PendingTimer::react(const EpochChanged&)" << dendl;
  return forward_event();
}

/**
 *  Preconds:
 *  - preemption data was set
 *  - epoch start was updated ***??
 */
NewChunk::NewChunk(my_context ctx) : my_base(ctx), state_logger_t{"Act/NewChunk"s}
{
  DECLARE_LOCALS;  // aliases for 'scrbr', 'pg' pointers

  dout(7) << " " << __func__ << dendl;
  scrbr->get_preemptor()->adjust_parameters();

  //  choose range to work on
  bool got_a_chunk = scrbr->select_range();
  dout(10) << __func__ << " selection " << (got_a_chunk ? "(OK)" : "(busy)") << dendl;
  if (got_a_chunk) {
    post_event(SelectedChunkFree{});
  } else {
    dout(7) << __func__ << " selected chunk is busy\n" << dendl;
    // wait until we are available (transitioning to Blocked)
    post_event(ChunkIsBusy{});
  }
}

sc::result NewChunk::react(const SelectedChunkFree&)
{
  DECLARE_LOCALS;  // aliases for 'scrbr', 'pg' pointers
  dout(7) << " NewChunk::react(const SelectedChunkFree&)" << dendl;

  scrbr->set_subset_last_update(scrbr->search_log_for_updates());
  return transit<WaitPushes>();
}

sc::result NewChunk::react(const EpochChanged&)
{
  dout(7) << "   NewChunk::react(const EpochChanged&)" << dendl;
  DECLARE_LOCALS;  // aliases for 'scrbr', 'pg' pointers

  scrbr->cleanup();  // RRR not done
  return transit<NotActive>();
}

WaitPushes::WaitPushes(my_context ctx) : my_base(ctx), state_logger_t{"Act/WaitPushes"s}
{
  post_event(ActivePushesUpd{});
}

sc::result WaitPushes::react(const EpochChanged&)
{
  dout(7) << " WaitPushes::react(const EpochChanged&)" << dendl;

  return transit<NotActive>();
}

/*
 * Triggered externally, by the entity that had an update re pushes
 */
sc::result WaitPushes::react(const ActivePushesUpd&)
{
  DECLARE_LOCALS;  // aliases for 'scrbr', 'pg' pointers
  dout(7) << " WaitPushes::react(const ActivePushesUpd&)"
	  << " actp: " << scrbr->pending_active_pushes() << dendl;

  if (scrbr->pending_active_pushes() == 0) {
    // done waiting
    return transit<WaitLastUpdate>();
  }

  return discard_event();
}

WaitLastUpdate::WaitLastUpdate(my_context ctx)
    : my_base(ctx), state_logger_t{"Act/WaitLastUpdate"s}
{
  post_event(UpdatesApplied{});
}

void WaitLastUpdate::on_new_updates(const UpdatesApplied&)
{
  DECLARE_LOCALS;  // aliases for 'scrbr', 'pg' pointers
  dout(8) << " WaitLastUpdate::on_new_updates(const UpdatesApplied&)" << dendl;

  if (scrbr->has_pg_marked_new_updates()) {
    post_event(InternalAllUpdates{});
  } else {
    // will be requeued by op_applied
    dout(7) << "wait for EC read/modify/writes to queue" << dendl;
  }
}

/*
 *  request maps from the replicas in the acting set
 */
sc::result WaitLastUpdate::react(const InternalAllUpdates&)
{
  DECLARE_LOCALS;  // aliases for 'scrbr', 'pg' pointers

  dout(7) << " WaitLastUpdate::react(const InternalAllUpdates&)" << dendl;

  if (scrbr->was_epoch_changed()) {
    dout(7) << "   epochchanged! WLU:IAU" << dendl;
    post_event(EpochChanged{});
    return discard_event();
  }

  dout(7) << " WaitLastUpdate::react(const InternalAllUpdates&) "
	  << scrbr->get_preemptor()->is_preemptable() << dendl;
  scrbr->get_replicas_maps(scrbr->get_preemptor()->is_preemptable());
  return transit<BuildMap>();
}


BuildMap::BuildMap(my_context ctx) : my_base(ctx), state_logger_t{"Act/BuildMap"s}
{
  DECLARE_LOCALS;  // aliases for 'scrbr', 'pg' pointers

  dout(7) << "  BMp  epochchanged? " << (scrbr->was_epoch_changed() ? "yes" : "no")
	  << dendl;
  dout(7) << "  BMp  preempted    ? "
	  << (scrbr->get_preemptor()->was_preempted() ? "yes" : "no") << dendl;

  // verify epoch stayed the same
  if (scrbr->was_epoch_changed()) {

    post_event(EpochChanged{});

  } else if (scrbr->get_preemptor()->was_preempted()) {

    // we were preempted, either directly or by a replica
    dout(10) << __func__ << " preempted!!!" << dendl;
    scrbr->waiting_on_whom.erase(scrbr->pg_whoami_);
    post_event(IntBmPreempted{});

  } else {

    auto ret = scrbr->build_primary_map_chunk();
    dout(7) << "   bpmc ret (115=inprogress) " << ret << dendl;

    if (ret == -EINPROGRESS) {
      // must wait for the backend to finish. No specific event provided.
      // build_primary_map_chunk() has already requeued us.

      dout(7) << " waiting for the backend..." << dendl;

      // unit-test code: fake preemption
      if (false && pg->pg_id.pgid.m_seed == 1 && fake_preemption_ &&
	  scrbr->get_preemptor()->is_preemptable()) {
	dout(7) << __func__ << " generating fake preemption" << dendl;
	fake_preemption_ = false;
	scrbr->preemption_data.do_preempt();
      }

    } else if (ret < 0) {

      dout(7) << "BuildMap::BuildMap() Error! Aborting. Ret: " << ret << dendl;
      scrbr->cleanup();
      post_event(InternalError{});

    } else {

      // the local map was created
      post_event(IntLocalMapDone{});
    }
  }
}

sc::result BuildMap::react(const IntLocalMapDone&)
{
  DECLARE_LOCALS;  // aliases for 'scrbr', 'pg' pointers
  dout(7) << " BuildMap::react(const IntLocalMapDone&)" << dendl;

  scrbr->waiting_on_whom.erase(scrbr->pg_whoami_);

  // ?? anything else?
  return transit<WaitReplicas>();
}

// //

DrainReplMaps::DrainReplMaps(my_context ctx)
    : my_base(ctx), state_logger_t{"Act/DrainReplMaps"s}
{
  // we may have received all maps already. Send the event that will make us check.
  post_event(GotReplicas{});
}

sc::result DrainReplMaps::react(const GotReplicas&)
{
  DECLARE_LOCALS;  // aliases for 'scrbr', 'pg' pointers
  dout(7) << " DrainReplMaps::react(const GotReplicas&)" << dendl;

  if (scrbr->waiting_on_whom.empty()) {

    // got all of them. Anything to do here?
    return transit<NewChunk>();
  }

  dout(10) << "DrainReplMaps::react(const GotReplicas&): still draining incoming maps: "
	   << scrbr->waiting_on_whom << dendl;
  return discard_event();
}

// //

WaitReplicas::WaitReplicas(my_context ctx)
    : my_base(ctx), state_logger_t{"Act/WaitReplicas"s}
{
  DECLARE_LOCALS;  // aliases for 'scrbr', 'pg' pointers
  dout(7) << " WaitReplicas::WaitReplicas() waiting for "
	  << scrbr->waiting_on_whom.size() << " replicas" << dendl;

  post_event(GotReplicas{});
}

sc::result WaitReplicas::react(const GotReplicas&)
{
  DECLARE_LOCALS;  // aliases for 'scrbr', 'pg' pointers
  dout(7) << "  WaitReplicas::react(const GotReplicas&)" << dendl;

  // RRR understand why I've decided that the original implementation just not check for
  //  preemption before waiting for everyone

  if (scrbr->waiting_on_whom.empty()) {

    dout(7) << "  WaitReplicas::react(const GotReplicas&) got all" << dendl;

    // were we preempted?
    if (scrbr->get_preemptor()->disable_and_test()) {  // a test&set


      dout(7) << "  WaitReplicas::react(const GotReplicas&) PREEMPTED!" << dendl;

      // RRR anything to do here?
      return transit<PendingTimer>();

    } else {

      dout(8) << "got the replicas!" << dendl;

      scrbr->done_comparing_maps();

      return transit<WaitDigestUpdate>();
    }
  } else {

    return discard_event();
  }
}

WaitDigestUpdate::WaitDigestUpdate(my_context ctx)
    : my_base(ctx), state_logger_t{"Act/WaitDigestUpdate"s}
{
  // perform an initial check: maybe we already
  // have all the updates we need:
  // (note that DigestUpdate is usually an external event)
  post_event(DigestUpdate{});
}

sc::result WaitDigestUpdate::react(const DigestUpdate&)
{
  DECLARE_LOCALS;  // aliases for 'scrbr', 'pg' pointers
  dout(7) << "  WaitDigestUpdate::react(const DigestUpdate&)" << dendl;


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

ScrubMachine::ScrubMachine(PG* pg, PgScrubber* pg_scrub) : pg_{pg}, scrbr_{pg_scrub}
{
  dout(10) << "ScrubMachine created " << pg_->pg_id << ". I am "
	   << (pg_->is_primary() ? "Primary" : "a replica") << dendl;
  dout(11) << __func__ << " ID: " << reinterpret_cast<uint64_t>(this) << dendl;
}



ScrubMachine::~ScrubMachine()
{
  dout(11) << "  ~ScrubMachine " << pg_->pg_id << dendl;
  dout(11) << __func__ << " ID: " << reinterpret_cast<uint64_t>(this) << dendl;
}


// ////////////////////////////////////////////////////////////////////////////////
//  for replicas
// ////////////////////////////////////////////////////////////////////////////////

ReplicaWaitUpdates::ReplicaWaitUpdates(my_context ctx)
    : my_base(ctx), state_logger_t{"ReplicaWaitUpdates"s}
{
  DECLARE_LOCALS;  // aliases for 'scrbr', 'pg' pointers

  dout(7) << "  " << __func__ << dendl;
  scrbr->on_replica_init();
  post_event(ActivePushesUpd{});
}


sc::result ReplicaWaitUpdates::react(const EpochChanged&)
{
  dout(7) << "   ReplicaWaitUpdates::react(const EpochChanged&)" << dendl;
  return transit<NotActive>();
}

/*
 * Triggered externally, by the entity that had an update re pushes
 */
sc::result ReplicaWaitUpdates::react(const ActivePushesUpd&)
{
  DECLARE_LOCALS;  // aliases for 'scrbr', 'pg' pointers

  dout(7) << "   replica actp: " << scrbr->pending_active_pushes() << dendl;
  dout(7) << "   is_deep_: " << scrbr->is_deep_ << dendl;
  dout(8) << " Was epoch c: " << scrbr->was_epoch_changed() << dendl;

  //  check for epoch-change
  if (scrbr->was_epoch_changed()) {

    post_event(EpochChanged{});

  } else if (scrbr->pending_active_pushes() == 0) {
    // done waiting

    scrbr->replica_update_start_epoch();
    return transit<ActiveReplica>();
  }

  return discard_event();
}

// //


ActiveReplica::ActiveReplica(my_context ctx)
    : my_base(ctx), state_logger_t{"ActiveReplica"s}
{
  DECLARE_LOCALS;  // aliases for 'scrbr', 'pg' pointers

  dout(7) << " RRRRRRRRRRRR" << __func__ << dendl;

  post_event(SchedReplica{});
}

ActiveReplica::~ActiveReplica()
{
  dout(7) << " RRRRRRRRRRRR" << __func__ << dendl;

  // should we clear 'is_active_' here, instead of in reset()? RRR
}

/*!
 * create the return message. Here, or in the internals_t?
 *
 *
 */
sc::result ActiveReplica::react(const SchedReplica&)
{
  DECLARE_LOCALS;  // aliases for 'scrbr', 'pg' pointers

  dout(7) << "  ActiveReplica::react(const SchedReplica&) shard:" << pg->pg_id
	  << " is-p? " << scrbr->get_preemptor()->is_preemptable() << " force? "
	  << fake_preemption_ << dendl;

  //  testing: fake preemption sometimes
  // if (pg->pg_id.pgid.m_seed == 2 && fake_preemption_ && scrbr->is_preemptable()) {
  //  /* for qa tests, 28/7/20:*/ fake_preemption_ = false;
  //  /* for qa tests, 28/7/20:*/ //scrbr->preemption_data.replica_preempted();
  // }

  // how do we check for epoch change for the replica?
  // RRR

  if (scrbr->was_epoch_changed()) {

    dout(7) << "  epoch changed" << dendl;

    scrbr->send_replica_map(false);
    post_event(EpochChanged{});

  } else if (scrbr->get_preemptor()->was_preempted()) {

    dout(7) << " replica scrub job preempted" << dendl;

    scrbr->send_replica_map(true);
    post_event(IntLocalMapDone{});

  } else {

    // start or check progress of build_replica_map_chunk()

    auto ret =
      scrbr->build_replica_map_chunk(false);  // low priority in the original code, too
    dout(7) << " ActiveReplica::react(const SchedReplica&) Ret: " << ret << dendl;
    if (ret == -EINPROGRESS) {

      // must wait for the backend to finish. No specific event provided.
      // build_replica_map_chunk() has already requeued us.

      dout(7) << "waiting for the backend..." << dendl;

    } else if (ret < 0) {

      //  the existing code ignores this option, treating an error
      //  report as a success.
      //  \todo what should we do here?

      dout(1) << "  Error! Aborting. ActiveReplica::react(const "
		 "SchedReplica&) Ret: "
	      << ret << dendl;
      post_event(IntLocalMapDone{});

    } else {

      // the local map was created. Send it to the primary.

      scrbr->send_replica_map(false);  // 'false' == not preempted
      // scrbr->cleanup();
      post_event(IntLocalMapDone{});
    }
  }
  return discard_event();
}

sc::result ActiveReplica::react(const IntLocalMapDone&)
{
  dout(10) << "  ActiveReplica::react(const IntLocalMapDone&)" << dendl;
  DECLARE_LOCALS;  // aliases for 'scrbr', 'pg' pointers

  scrbr->replica_handling_done();
  return transit<NotActive>();
}

sc::result ActiveReplica::react(const InternalError&)
{
  DECLARE_LOCALS;  // aliases for 'scrbr', 'pg' pointers
  dout(1) << "  Error! Aborting."
	  << " ActiveReplica::react(const InternalError&) " << dendl;

  scrbr->replica_handling_done();
  return transit<NotActive>();
}

sc::result ActiveReplica::react(const EpochChanged&)
{
  DECLARE_LOCALS;  // aliases for 'scrbr', 'pg' pointers
  dout(10) << " ActiveReplica::react(const EpochChanged&) " << dendl;

  scrbr->replica_handling_done();
  return transit<NotActive>();
}

// sc::result ActiveReplica::react(const FullReset&)
// {
//   dout(7) << "Probable bug: ActiveReplica::react(const FullReset&)" << dendl;
//   // nothing for now to do here, as (for now) we are counting on the PgScrubber code to
//   // release held resources
//   return transit<NotActive>();
// }


}  // namespace Scrub
