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

//
//  EVENTS
//

void on_event_creation(std::string_view nm);
void on_event_discard(std::string_view nm);

#define MEV(E)                                          \
  struct E : sc::event<E> {                             \
    inline static int actv{0};                          \
    E()                                                 \
    {                                                   \
      if (!actv++)                                      \
	on_event_creation(#E);                          \
    }                                                   \
    ~E()                                                \
    {                                                   \
      if (!--actv)                                      \
	on_event_discard(#E);                           \
    }                                                   \
    void print(std::ostream* out) const { *out << #E; } \
    std::string_view print() const { return #E; }       \
  };

MEV(RemotesReserved)  ///< all replicas have granted our reserve request

MEV(ReservationFailure)	 ///< a reservation request has failed

MEV(StartScrub)	 ///< initiate a new scrubbing session (relevant if we are a Primary)

MEV(AfterRepairScrub)  ///< initiate a new scrubbing session. Only triggered at Recovery
		       ///< completion.

MEV(Unblocked)	///< triggered when the PG unblocked an object that was marked for
		///< scrubbing. Via the PGScrubUnblocked op

MEV(InternalSchedScrub)

MEV(SelectedChunkFree)

MEV(ChunkIsBusy)

MEV(ActivePushesUpd)	 ///< Update to active_pushes. 'active_pushes' represents recovery
			 ///< that is in-flight to the local ObjectStore
MEV(UpdatesApplied)	 ///< (Primary only) all updates are committed

MEV(InternalAllUpdates)	 ///< the internal counterpart of UpdatesApplied

MEV(GotReplicas)  ///< got a map from a replica

MEV(IntBmPreempted)  ///< internal - BuildMap preempted. Required, as detected within the
		     ///< ctor

MEV(InternalError)

MEV(IntLocalMapDone)

MEV(DigestUpdate)  ///< external. called upon success of a MODIFY op. See
		   ///< scrub_snapshot_metadata()

MEV(MapsCompared)  ///< (Crimson) maps_compare_n_cleanup() transactions are done

MEV(StartReplica)  ///< initiating replica scrub.

MEV(StartReplicaNoWait)	 ///< 'start replica' when there are no pending updates

MEV(SchedReplica)

MEV(ReplicaPushesUpd)  ///< Update to active_pushes. 'active_pushes' represents recovery
		       ///< that is in-flight to the local ObjectStore

MEV(FullReset)	///< guarantee that the FSM is in the quiescent state (i.e. NotActive)

MEV(NextChunk)	///< finished handling this chunk. Go get the next one

MEV(ScrubFinished)  ///< all chunks handled


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
  explicit ScrubMachine(PG* pg, ScrubMachineListener* pg_scrub);
  ~ScrubMachine();

  PG* m_pg;  // only used for dout messages
  spg_t m_pg_id;
  ScrubMachineListener* m_scrbr;

  void my_states() const;
  void assert_not_active() const;
  [[nodiscard]] bool is_reserving() const;
  [[nodiscard]] bool is_accepting_updates() const;
};

/**
 *  The Scrubber's base (quiescent) state.
 *  Scrubbing is triggered by one of the following events:
 *  - (standard scenario for a Primary): 'StartScrub'. Initiates the OSDs resources
 *    reservation process. Will be issued by PG::scrub(), following a
 *    queued "PGScrub" op.
 *  - a special end-of-recovery Primary scrub event ('AfterRepairScrub') that is
 *    not required to reserve resources.
 *  - (for a replica) 'StartReplica' or 'StartReplicaNoWait', triggered by an incoming
 *    MOSDRepScrub message.
 *
 *  note (20.8.21): originally, AfterRepairScrub was triggering a scrub without waiting
 *   for replica resources to be acquired. But once replicas started using the
 *   resource-request to identify and tag the scrub session, this bypass cannot be
 *   supported anymore.
 */
struct NotActive : sc::state<NotActive, ScrubMachine> {
  explicit NotActive(my_context ctx);

  using reactions = mpl::list<sc::transition<StartScrub, ReservingReplicas>,
			      // a scrubbing that was initiated at recovery completion,
			      // and requires no resource reservations:
			      sc::transition<AfterRepairScrub, ReservingReplicas>,
			      sc::transition<StartReplica, ReplicaWaitUpdates>,
			      sc::transition<StartReplicaNoWait, ActiveReplica>>;
};

struct ReservingReplicas : sc::state<ReservingReplicas, ScrubMachine> {

  explicit ReservingReplicas(my_context ctx);
  using reactions = mpl::list<sc::custom_reaction<FullReset>,
			      // all replicas granted our resources request
			      sc::transition<RemotesReserved, ActiveScrubbing>,
			      sc::custom_reaction<ReservationFailure>>;

  sc::result react(const FullReset&);

  /// at least one replica denied us the scrub resources we've requested
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
struct WaitDigestUpdate;

struct ActiveScrubbing : sc::state<ActiveScrubbing, ScrubMachine, PendingTimer> {

  explicit ActiveScrubbing(my_context ctx);
  ~ActiveScrubbing();

  using reactions = mpl::list<
    sc::custom_reaction<InternalError>,
    sc::custom_reaction<FullReset>>;

  sc::result react(const FullReset&);
  sc::result react(const InternalError&);
};

struct RangeBlocked : sc::state<RangeBlocked, ActiveScrubbing> {
  explicit RangeBlocked(my_context ctx);
  using reactions = mpl::list<sc::transition<Unblocked, PendingTimer>>;
};

struct PendingTimer : sc::state<PendingTimer, ActiveScrubbing> {

  explicit PendingTimer(my_context ctx);

  using reactions = mpl::list<sc::transition<InternalSchedScrub, NewChunk>>;
};

struct NewChunk : sc::state<NewChunk, ActiveScrubbing> {

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
struct WaitPushes : sc::state<WaitPushes, ActiveScrubbing> {

  explicit WaitPushes(my_context ctx);

  using reactions = mpl::list<sc::custom_reaction<ActivePushesUpd>>;

  sc::result react(const ActivePushesUpd&);
};

struct WaitLastUpdate : sc::state<WaitLastUpdate, ActiveScrubbing> {

  explicit WaitLastUpdate(my_context ctx);

  void on_new_updates(const UpdatesApplied&);

  using reactions = mpl::list<sc::custom_reaction<InternalAllUpdates>,
			      sc::in_state_reaction<UpdatesApplied,
						    WaitLastUpdate,
						    &WaitLastUpdate::on_new_updates>>;

  sc::result react(const InternalAllUpdates&);
};

struct BuildMap : sc::state<BuildMap, ActiveScrubbing> {
  explicit BuildMap(my_context ctx);

  // possible error scenarios:
  // - an error reported by the backend will trigger an 'InternalError' event,
  //   handled by our parent state;
  // - if preempted, we switch to DrainReplMaps, where we will wait for all
  //   replicas to send their maps before acknowledging the preemption;
  // - an interval change will be handled by the relevant 'send-event' functions,
  //   and will translated into a 'FullReset' event.
  using reactions =
    mpl::list<sc::transition<IntBmPreempted, DrainReplMaps>,
	      sc::transition<InternalSchedScrub, BuildMap>,  // looping, waiting
							     // for the backend to
							     // finish
	      sc::custom_reaction<IntLocalMapDone>>;

  sc::result react(const IntLocalMapDone&);
};

/*
 *  "drain" scrub-maps responses from replicas
 */
struct DrainReplMaps : sc::state<DrainReplMaps, ActiveScrubbing> {
  explicit DrainReplMaps(my_context ctx);

  using reactions =
    mpl::list<sc::custom_reaction<GotReplicas>	// all replicas are accounted for
	      >;

  sc::result react(const GotReplicas&);
};

struct WaitReplicas : sc::state<WaitReplicas, ActiveScrubbing> {
  explicit WaitReplicas(my_context ctx);

  using reactions =
    mpl::list<sc::custom_reaction<GotReplicas>,	 // all replicas are accounted for
	      sc::transition<MapsCompared, WaitDigestUpdate>,
	      sc::deferral<DigestUpdate>  // might arrive before we've reached WDU
	      >;

  sc::result react(const GotReplicas&);

  bool all_maps_already_called{false};	// see comment in react code
};

struct WaitDigestUpdate : sc::state<WaitDigestUpdate, ActiveScrubbing> {
  explicit WaitDigestUpdate(my_context ctx);

  using reactions = mpl::list<sc::custom_reaction<DigestUpdate>,
			      sc::transition<NextChunk, PendingTimer>,
			      sc::transition<ScrubFinished, NotActive>>;
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
struct ReplicaWaitUpdates : sc::state<ReplicaWaitUpdates, ScrubMachine> {
  explicit ReplicaWaitUpdates(my_context ctx);
  using reactions =
    mpl::list<sc::custom_reaction<ReplicaPushesUpd>, sc::custom_reaction<FullReset>>;

  sc::result react(const ReplicaPushesUpd&);
  sc::result react(const FullReset&);
};


struct ActiveReplica : sc::state<ActiveReplica, ScrubMachine> {
  explicit ActiveReplica(my_context ctx);
  using reactions = mpl::list<sc::custom_reaction<SchedReplica>,
			      sc::custom_reaction<FullReset>,
			      sc::transition<ScrubFinished, NotActive>>;

  sc::result react(const SchedReplica&);
  sc::result react(const FullReset&);
};

}  // namespace Scrub
