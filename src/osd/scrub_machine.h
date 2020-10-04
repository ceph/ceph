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

#include "scrub_machine_lstnr.h"
#include "scrubber_common.h"

using namespace std::string_literals;

class PG;  // holding a pointer to that one - just for testing
class PgScrubber;
namespace Scrub {

namespace sc = ::boost::statechart;
namespace mpl = ::boost::mpl;

struct state_logger_t {
  state_logger_t(std::string_view nm);
  std::string_view m_name;
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

MEV(RemotesReserved)	 ///< all replicas have granted our reserve request
MEV(ReservationFailure)	 ///< a reservation request has failed
MEV(EpochChanged)	 ///< ... from what it was when this chunk started
MEV(StartScrub)	 ///< initiate a new scrubbing session (relevant if we are a Primary)
MEV(AfterRecoveryScrub)	 ///< initiate a new scrubbing session. Only triggered at Recovery
			 ///< completion.
MEV(Unblocked)	///< triggered when the PG unblocked an object that was marked for
		///< scrubbing. Via the PGScrubUnblocked op
MEV(InternalSchedScrub)
MEV(SelectedChunkFree)
MEV(ChunkIsBusy)
MEV(ActivePushesUpd)	 ///< Update to active_pushes. 'active_pushes' represents recovery
			 ///< that is in-flight to the local ObjectStore
MEV(UpdatesApplied)	 // external
MEV(InternalAllUpdates)	 ///< the internal counterpart of UpdatesApplied
MEV(GotReplicas)	 ///< got a map from a replica

MEV(IntBmPreempted)  ///< internal - BuildMap preempted. Required, as detected within the
		     ///< ctor
MEV(InternalError)

MEV(IntLocalMapDone)

MEV(DigestUpdate)  ///< external. called upon success of a MODIFY op. See
		   ///< scrub_snapshot_metadata()
MEV(AllChunksDone)

MEV(StartReplica)  ///< initiating replica scrub. replica_scrub_op() -> OSD Q ->
		   ///< replica_scrub()
MEV(SchedReplica)
MEV(ReplicaPushesUpd)  ///< Update to active_pushes. 'active_pushes' represents recovery
		       ///< that is in-flight to the local ObjectStore

MEV(FullReset)	///< guarantee that the FSM is in the quiescent state (i.e. NotActive)


struct NotActive;	    ///< the quiescent state. No active scrubbing.
struct ReservingReplicas;   ///< securing scrub resources from replicas' OSDs
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

  PG* m_pg;  // used only for testing
  spg_t m_pg_id;

  ScrubMachineListener* const m_scrbr;

  void down_on_epoch_change(const EpochChanged&);

  void on_epoch_changed(const EpochChanged&);

  void my_states() const;

  void assert_not_active() const;

  [[nodiscard]] bool is_reserving() const;
};

/**
 *  The Scrubber's base (quiescent) state.
 *  Scrubbing is triggered by one of the following events:
 *  - (standard scenario for a Primary): 'StartScrub'. Initiates the OSDs resources
 *    reservation process. Will be issued by PG::scrub(), following a
 *    queued "PGScrub" op.
 *  - a special end-of-recovery Primary scrub event ('AfterRecoveryScrub') that is
 *    not required to reserve resources.
 *  - (for a replica) 'StartReplica', triggered by an incoming MOSDRepScrub msg.
 */
struct NotActive : sc::state<NotActive, ScrubMachine>, state_logger_t {
  explicit NotActive(my_context ctx);

  using reactions = mpl::list<sc::custom_reaction<EpochChanged>,
			      sc::transition<StartScrub, ReservingReplicas>,
			      // a scrubbing that was initiated at recovery completion,
			      // and requires no resource reservations:
			      sc::transition<AfterRecoveryScrub, ActiveScrubbing>,
			      sc::transition<StartReplica, ReplicaWaitUpdates>,
			      sc::custom_reaction<sc::event_base>>;

  sc::result react(const EpochChanged&);
  sc::result react(const sc::event_base&)  // in the future: assert here
  {
    return discard_event();
  }
};

struct ReservingReplicas : sc::state<ReservingReplicas, ScrubMachine>, state_logger_t {

  explicit ReservingReplicas(my_context ctx);
  using reactions = mpl::list<sc::custom_reaction<EpochChanged>,
			      // all replicas granted our resources request
			      sc::transition<RemotesReserved, ActiveScrubbing>,
			      sc::custom_reaction<FullReset>,
			      sc::custom_reaction<ReservationFailure>>;

  sc::result react(const EpochChanged&);
  sc::result react(const FullReset&);
  sc::result react(const ReservationFailure&);
};


// the "active" sub-states

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

struct ActiveScrubbing : sc::state<ActiveScrubbing, ScrubMachine, PendingTimer>,
			 state_logger_t {

  explicit ActiveScrubbing(my_context ctx);
  ~ActiveScrubbing();

  using reactions = mpl::list<
    // done scrubbing
    sc::transition<AllChunksDone, NotActive>,

    sc::transition<EpochChanged,
		   NotActive,
		   ScrubMachine,
		   &ScrubMachine::down_on_epoch_change>,
    sc::custom_reaction<FullReset>>;

  sc::result react(const AllChunksDone&);
  sc::result react(const FullReset&);
};

struct RangeBlocked : sc::state<RangeBlocked, ActiveScrubbing>, state_logger_t {
  explicit RangeBlocked(my_context ctx);
  using reactions = mpl::list<sc::transition<Unblocked, PendingTimer>>;
};

struct PendingTimer : sc::state<PendingTimer, ActiveScrubbing>, state_logger_t {

  explicit PendingTimer(my_context ctx);

  using reactions = mpl::list<sc::transition<InternalSchedScrub, NewChunk>>;
};

struct NewChunk : sc::state<NewChunk, ActiveScrubbing>, state_logger_t {

  explicit NewChunk(my_context ctx);

  using reactions = mpl::list<sc::transition<ChunkIsBusy, RangeBlocked>,
			      sc::custom_reaction<SelectedChunkFree>>;

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

  using reactions = mpl::list<sc::custom_reaction<ActivePushesUpd>>;

  sc::result react(const ActivePushesUpd&);
};

struct WaitLastUpdate : sc::state<WaitLastUpdate, ActiveScrubbing>, state_logger_t {

  explicit WaitLastUpdate(my_context ctx);

  void on_new_updates(const UpdatesApplied&);

  using reactions = mpl::list<sc::custom_reaction<InternalAllUpdates>,
			      sc::in_state_reaction<UpdatesApplied,
						    WaitLastUpdate,
						    &WaitLastUpdate::on_new_updates>>;

  sc::result react(const InternalAllUpdates&);
};

struct BuildMap : sc::state<BuildMap, ActiveScrubbing>, state_logger_t {
  explicit BuildMap(my_context ctx);

  using reactions =
    mpl::list<sc::transition<IntBmPreempted, DrainReplMaps>,
	      sc::transition<InternalSchedScrub, BuildMap>,  // looping, waiting
							     // for the backend to
							     // finish
	      sc::custom_reaction<IntLocalMapDone>,
	      sc::transition<InternalError, NotActive>>;  // to discuss RRR

  sc::result react(const IntLocalMapDone&);

  //  testing aids (to be removed)
  inline static bool m_fake_preemption{true};
};

/*
 *  "drain" scrub-maps responses from replicas
 */
struct DrainReplMaps : sc::state<DrainReplMaps, ActiveScrubbing>, state_logger_t {
  explicit DrainReplMaps(my_context ctx);

  using reactions =
    mpl::list<sc::custom_reaction<GotReplicas>	// all replicas are accounted for
	      >;

  sc::result react(const GotReplicas&);
};

struct WaitReplicas : sc::state<WaitReplicas, ActiveScrubbing>, state_logger_t {

  explicit WaitReplicas(my_context ctx);

  using reactions =
    mpl::list<sc::custom_reaction<GotReplicas>, sc::deferral<DigestUpdate>>;

  sc::result react(const GotReplicas&);
};

struct WaitDigestUpdate : sc::state<WaitDigestUpdate, ActiveScrubbing>, state_logger_t {
  explicit WaitDigestUpdate(my_context ctx);

  using reactions = mpl::list<sc::custom_reaction<DigestUpdate>>;
  sc::result react(const DigestUpdate&);
};


// ----------------------------- the "replica active" states -----------------------

/*
 * Waiting for 'active_pushes' to complete
 *
 * When in this state:
 * - the details of the Primary's request were internalized by PgScrubber;
 * - 'active' scrubbing is set
 */
struct ReplicaWaitUpdates : sc::state<ReplicaWaitUpdates, ScrubMachine>, state_logger_t {

  explicit ReplicaWaitUpdates(my_context ctx);
  using reactions =
    mpl::list<sc::custom_reaction<ReplicaPushesUpd>, sc::custom_reaction<EpochChanged>>;

  sc::result react(const ReplicaPushesUpd&);
  sc::result react(const EpochChanged&);
};


struct ActiveReplica : sc::state<ActiveReplica, ScrubMachine>, state_logger_t {

  explicit ActiveReplica(my_context ctx);
  using reactions = mpl::list<sc::custom_reaction<EpochChanged>,
			      sc::custom_reaction<SchedReplica>,
			      sc::custom_reaction<IntLocalMapDone>,
			      sc::custom_reaction<FullReset>,
			      sc::custom_reaction<InternalError>>;

  sc::result react(const SchedReplica&);
  sc::result react(const EpochChanged&);
  sc::result react(const IntLocalMapDone&);
  sc::result react(const FullReset&);
  sc::result react(const InternalError&);

  //  testing aids (to be removed)
  inline static bool m_fake_preemption{true};
};

}  // namespace Scrub
