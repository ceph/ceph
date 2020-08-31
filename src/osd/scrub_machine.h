// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>

#include <boost/statechart/custom_reaction.hpp>
#include <boost/statechart/deferral.hpp>
#include <boost/statechart/event.hpp>
#include <boost/statechart/event_base.hpp>
#include <boost/statechart/in_state_reaction.hpp>
#include <boost/statechart/simple_state.hpp>
#include <boost/statechart/state.hpp>
#include <boost/statechart/state_machine.hpp>
#include <boost/statechart/transition.hpp>

#include "common/version.h"
#include "include/Context.h"

#include "scrubber_common.h"

using namespace std::string_literals;

class PG;  // holding a pointer to that one - just for testing
class PgScrubber;
namespace Scrub {

namespace sc = ::boost::statechart;
namespace mpl = ::boost::mpl;

struct state_logger_t {
  state_logger_t(std::string nm);
  ~state_logger_t()
  { /*logger().debug( "\t<<<-- /{}\n", name_);*/
  }
  std::string name_;
};


//
//  EVENTS
//

void on_event_creation(std::string nm);
void on_event_discard(std::string nm);

#define MEV(E)                 \
  struct E : sc::event<E> {    \
    inline static int actv{0}; \
    E()                        \
    {                          \
      if (!actv++)             \
	on_event_creation(#E); \
    }                          \
    ~E()                       \
    {                          \
      if (!--actv)             \
	on_event_discard(#E);  \
    }                          \
  };

MEV(EpochChanged)  ///< ... from what it was when this chunk started
MEV(StartScrub)
MEV(SchedScrub)
MEV(Unblocked)	///< triggered when the PG unblocked an object that was marked for
		///< scrubbing. Via the PGScrubUnblocked op
MEV(InternalSchedScrub)
MEV(SelectedChunkFree)
MEV(ChunkIsBusy)
MEV(ActivePushesUpd)	 ///< Update to active_pushes. 'active_pushes' represents recovery
			 ///< that is in-flight to the local Objectstore
MEV(UpdatesApplied)	 // external
MEV(InternalAllUpdates)	 ///< the internal counterpart of UpdatesApplied
MEV(GotReplicas)	 ///< got a map from a replica

MEV(IntBmPreempted)  ///< internal - BuildMap preempted. Required, as detected within the ctor
MEV(InternalError)

MEV(IntLocalMapDone)

MEV(DigestUpdate)  ///< external. called upon success of a MODIFY op. See
		   ///< scrub_snapshot_metadata()
MEV(AllChunksDone)

MEV(StartReplica)  ///< initiating replica scrub. replica_scrub_op() -> OSD Q -> replica_scrub()
MEV(SchedReplica)

MEV(FullReset)	///< guarantee that the FSM is in a quiescent state (currently - NotActive)


struct NotActive;	    ///< the quiescent state. No active scrubbing. (will be modified in
			    ///< phase 2)
struct ActiveScrubbing;	    ///< the active state for a Primary. A sub-machine.
struct ReplicaWaitUpdates;  ///< an active state for a replica. Waiting for all active
			    ///< operations to finish.
struct ActiveReplica;	    ///< an active state for a replica.


class ScrubMachine : public sc::state_machine<ScrubMachine, NotActive> {
 public:
  friend class PgScrubber;

 public:
  explicit ScrubMachine(PG* pg, PgScrubber* pg_scrub);
  ~ScrubMachine();

  PG* const pg_;  // used only for testing

  PgScrubber* const scrbr_;

  void down_from_active(const EpochChanged&);

  void on_epoch_changed(const EpochChanged&);

  void my_states();

  void assert_not_active();
};

//  states postponed for step 2:

// struct ExtInactive : sc::state<ExtInactive, ScrubMachine>,
// state_logger_t {
//  explicit ExtInactive(my_context ctx) : my_base(ctx), state_logger_t{ "ExtInactive"s }
//  {}
//
//  using reactions = mpl::list<sc::custom_reaction<EpochChanged>,
//  sc::custom_reaction<SchedScrub>
//			   // sc::custom_reaction< NullEvt >,
//			   // sc::transition< sc::event_base,
// ExtInactive >
//			   >
//    ;
//
//  sc::result react(const EpochChanged&);
//  sc::result react(const SchedScrub&);
//  // sc::result react(const MLogRec&);
//  sc::result react(const sc::event_base&) { return
//  discard_event(); }
//};
//
// struct ReservationsMade : sc::state<ReservationsMade, ScrubMachine>,
// state_logger_t {
//  explicit ReservationsMade(my_context ctx) : my_base(ctx), state_logger_t{
//  "ReservationsMade"s } {}
//
//  using reactions = mpl::list<sc::custom_reaction<EpochChanged>,
//				     sc::custom_reaction<TmpEvent_1>,
//				     // sc::custom_reaction< NullEvt >,
//				     sc::transition<sc::event_base,
// ExtInactive>>;
//
//  sc::result react(const EpochChanged&);
//  sc::result react(const TmpEvent_1&);
//  sc::result react(const sc::event_base&)
//  {
//    return discard_event();
//  }
//};


/**
 *  \ATTN the role of this state will be changed once the "reservations" states are
 *        incorporated into the FSM.
 *
 *  The base state - until a 'start-sched' event (if we are a Primary) or a
 *  'start-replica' is processed. The former will be issued by PG::scrub(), following a
 *  queued "PGScrub" op, and the latter: following an incoming RRR
 */
struct NotActive : sc::state<NotActive, ScrubMachine>, state_logger_t {
  explicit NotActive(my_context ctx) : my_base(ctx), state_logger_t{"NotActive"s} {}

  using reactions = mpl::list<sc::custom_reaction<EpochChanged>,
			      sc::custom_reaction<StartScrub>,
			      sc::custom_reaction<StartReplica>,
			      sc::custom_reaction<sc::event_base>>;

  sc::result react(const EpochChanged&);
  sc::result react(const StartScrub&);
  sc::result react(const StartReplica&);

  sc::result react(const sc::event_base&)  // in the future: assert here
  {
    return discard_event();
  }
};

// the "active" sub-states

struct ActvNQueued;   ///< scrub_active is set, but we may have to sleep before NewChunk
struct RangeBlocked;  ///< the objects range is blocked
struct PendingTimer;  ///< either delaying the scrub by some time and requeuing, or just
		      ///< requeue
struct NewChunk;      ///< select a chunk to scrub, and verify its availability
struct WaitPushes;
struct WaitLastUpdate;
struct BuildMap;
struct DrainReplMaps;  ///< a problem during BuildMap. Wait for all replicas to report,
		       ///< then restart.
struct WaitReplicas;   ///< wait for all replicas to report

struct ActiveScrubbing : sc::state<ActiveScrubbing, ScrubMachine, ActvNQueued>, state_logger_t {

  explicit ActiveScrubbing(my_context ctx);

  using reactions = mpl::list<
    sc::transition<EpochChanged, NotActive, ScrubMachine, &ScrubMachine::down_from_active>,
    sc::custom_reaction<AllChunksDone>,
    sc::custom_reaction<FullReset>>;

  sc::result react(const AllChunksDone&);
  sc::result react(const FullReset&);
};

struct ActvNQueued : sc::state<ActvNQueued, ActiveScrubbing>, state_logger_t {
  explicit ActvNQueued(my_context ctx);
  using reactions = mpl::list<sc::custom_reaction<EpochChanged>,
			      sc::transition<SchedScrub, PendingTimer>>	 // external trigger
    ;

  sc::result react(const EpochChanged&);
};

struct RangeBlocked : sc::state<RangeBlocked, ActiveScrubbing>, state_logger_t {
  explicit RangeBlocked(my_context ctx);
  using reactions = mpl::list<sc::custom_reaction<EpochChanged>,
			      sc::transition<Unblocked, PendingTimer>>	// external trigger
    ;

  sc::result react(const EpochChanged&);
};


struct PendingTimer : sc::state<PendingTimer, ActiveScrubbing>, state_logger_t {

  explicit PendingTimer(my_context ctx);

  using reactions = mpl::list<sc::custom_reaction<EpochChanged>,
			      sc::transition<InternalSchedScrub, NewChunk>,
			      sc::transition<SchedScrub, NewChunk>  // shouldn't
			      >;

  sc::result react(const EpochChanged&);
};

struct NewChunk : sc::state<NewChunk, ActiveScrubbing>, state_logger_t {
  explicit NewChunk(my_context ctx);
  using reactions = mpl::list<sc::custom_reaction<EpochChanged>,
			      sc::transition<ChunkIsBusy, RangeBlocked>,
			      sc::custom_reaction<SelectedChunkFree>,
			      sc::deferral<sc::event_base>>;

  sc::result react(const EpochChanged&);
  sc::result react(const SelectedChunkFree&);
};

/**
 * initiate the update process for this chunk
 *
 * Wait fo 'active_pushes' to clear.
 * 'active_pushes' represents recovery that is in-flight to the local Objectstore, hence
 * scrub waits until the correct data is readable (in-flight data to the Objectstore is
 * not readable until written to disk, termed 'applied' here)
 */
struct WaitPushes : sc::state<WaitPushes, ActiveScrubbing>, state_logger_t {

  explicit WaitPushes(my_context ctx);

  using reactions =
    mpl::list<sc::custom_reaction<EpochChanged>, sc::custom_reaction<ActivePushesUpd>>;

  sc::result react(const EpochChanged&);
  sc::result react(const ActivePushesUpd&);
};


struct WaitLastUpdate : sc::state<WaitLastUpdate, ActiveScrubbing>, state_logger_t {

  explicit WaitLastUpdate(my_context ctx);

  void on_new_updates(const UpdatesApplied&);

  using reactions = mpl::list<
    sc::custom_reaction<InternalAllUpdates>,
    sc::in_state_reaction<UpdatesApplied, WaitLastUpdate, &WaitLastUpdate::on_new_updates>>;

  sc::result react(const InternalAllUpdates&);
};

struct BuildMap : sc::state<BuildMap, ActiveScrubbing>, state_logger_t {
  explicit BuildMap(my_context ctx);

  using reactions = mpl::list<sc::transition<IntBmPreempted, DrainReplMaps>,
			      sc::transition<SchedScrub, BuildMap>,  // looping, waiting for the
								     // backend to finish. Obsolete.
								     // Rm when sure
			      sc::transition<InternalSchedScrub, BuildMap>,  // looping, waiting
									     // for the backend to
									     // finish
			      sc::custom_reaction<IntLocalMapDone>,
			      sc::transition<InternalError, NotActive>>;  // to fix RRR

  sc::result react(const IntLocalMapDone&);

  //  testing aids (to be removed)
  inline static bool fake_preemption_{true};
};

/*
 *  "drain" scrub-maps responses from replicas
 */
struct DrainReplMaps : sc::state<DrainReplMaps, ActiveScrubbing>, state_logger_t {
  explicit DrainReplMaps(my_context ctx);

  using reactions = mpl::list<sc::custom_reaction<GotReplicas>,
			      sc::transition<SchedScrub, DrainReplMaps>	 // RRR new addition.
									 // Verify
			      >;

  sc::result react(const GotReplicas&);
};

struct WaitReplicas : sc::state<WaitReplicas, ActiveScrubbing>, state_logger_t {

  explicit WaitReplicas(my_context ctx);

  using reactions = mpl::list<sc::custom_reaction<GotReplicas>,
			      sc::deferral<DigestUpdate>,
			      sc::transition<SchedScrub, WaitReplicas>,
			      sc::transition<InternalError, NotActive>>	 // to fix RRR
    ;

  sc::result react(const GotReplicas&);
};

struct WaitDigestUpdate : sc::state<WaitDigestUpdate, ActiveScrubbing>, state_logger_t {
  explicit WaitDigestUpdate(my_context ctx);

  using reactions = mpl::list<
    // sc::custom_reaction< EpochChanged >,
    sc::custom_reaction<DigestUpdate>>;

  sc::result react(const DigestUpdate&);
};


// ----------------------------- the "replica active" states -----------------------

/*
 * Waiting for 'active_pushes' to complete
 *
 * When in this state:
 * - the details of the Primary's request were internalized by PgScrubber;
 * -
 */
struct ReplicaWaitUpdates : sc::state<ReplicaWaitUpdates, ScrubMachine>, state_logger_t {

  explicit ReplicaWaitUpdates(my_context ctx);
  using reactions =
    mpl::list<sc::custom_reaction<ActivePushesUpd>, sc::custom_reaction<EpochChanged>>;

  sc::result react(const ActivePushesUpd&);
  sc::result react(const EpochChanged&);
};


struct ActiveReplica : sc::state<ActiveReplica, ScrubMachine>, state_logger_t {

  explicit ActiveReplica(my_context ctx);

  ~ActiveReplica();

  using reactions = mpl::list<sc::custom_reaction<EpochChanged>,
			      sc::custom_reaction<SchedReplica>,
			      sc::custom_reaction<IntLocalMapDone>,
			      // sc::custom_reaction<FullReset>,
			      sc::custom_reaction<InternalError>>;

  sc::result react(const SchedReplica&);
  sc::result react(const EpochChanged&);
  sc::result react(const IntLocalMapDone&);
  // sc::result react(const FullReset&);
  sc::result react(const InternalError&);

  //  testing aids (to be removed)
  inline static bool fake_preemption_{true};
};

}  // namespace Scrub
