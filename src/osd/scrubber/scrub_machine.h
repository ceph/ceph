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
#include "osd/scrubber_common.h"

#include "scrub_machine_lstnr.h"

/// a wrapper that sets the FSM state description used by the
/// PgScrubber
/// \todo consider using the full NamedState as in Peering
struct NamedSimply {
  explicit NamedSimply(ScrubMachineListener* scrubber, const char* name);
};

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

/// all replicas have granted our reserve request
MEV(RemotesReserved)

/// a reservation request has failed
MEV(ReservationFailure)

/// initiate a new scrubbing session (relevant if we are a Primary)
MEV(StartScrub)

/// initiate a new scrubbing session. Only triggered at Recovery completion
MEV(AfterRepairScrub)

/// triggered when the PG unblocked an object that was marked for scrubbing.
/// Via the PGScrubUnblocked op
MEV(Unblocked)

MEV(InternalSchedScrub)

MEV(SelectedChunkFree)

MEV(ChunkIsBusy)

/// Update to active_pushes. 'active_pushes' represents recovery that
/// is in-flight to the local ObjectStore
MEV(ActivePushesUpd)

/// (Primary only) all updates are committed
MEV(UpdatesApplied)

/// the internal counterpart of UpdatesApplied
MEV(InternalAllUpdates)

/// got a map from a replica
MEV(GotReplicas)

/// internal - BuildMap preempted. Required, as detected within the ctor
MEV(IntBmPreempted)

MEV(InternalError)

MEV(IntLocalMapDone)

/// external. called upon success of a MODIFY op. See
/// scrub_snapshot_metadata()
MEV(DigestUpdate)

/// maps_compare_n_cleanup() transactions are done
MEV(MapsCompared)

/// initiating replica scrub
MEV(StartReplica)

/// 'start replica' when there are no pending updates
MEV(StartReplicaNoWait)

MEV(SchedReplica)

/// Update to active_pushes. 'active_pushes' represents recovery
/// that is in-flight to the local ObjectStore
MEV(ReplicaPushesUpd)

/// guarantee that the FSM is in the quiescent state (i.e. NotActive)
MEV(FullReset)

/// finished handling this chunk. Go get the next one
MEV(NextChunk)

/// all chunks handled
MEV(ScrubFinished)

//
//  STATES
//

struct NotActive;	    ///< the quiescent state. No active scrubbing.
struct ReservingReplicas;   ///< securing scrub resources from replicas' OSDs
struct ActiveScrubbing;	    ///< the active state for a Primary. A sub-machine.
struct ReplicaWaitUpdates;  ///< an active state for a replica. Waiting for all
			    ///< active operations to finish.
struct ActiveReplica;	    ///< an active state for a replica.


class ScrubMachine : public sc::state_machine<ScrubMachine, NotActive> {
 public:
  friend class PgScrubber;

 public:
  explicit ScrubMachine(PG* pg, ScrubMachineListener* pg_scrub);
  ~ScrubMachine();

  spg_t m_pg_id;
  ScrubMachineListener* m_scrbr;
  std::ostream& gen_prefix(std::ostream& out) const;

  void assert_not_active() const;
  [[nodiscard]] bool is_reserving() const;
  [[nodiscard]] bool is_accepting_updates() const;
};

/**
 *  The Scrubber's base (quiescent) state.
 *  Scrubbing is triggered by one of the following events:
 *
 *  - (standard scenario for a Primary): 'StartScrub'. Initiates the OSDs
 *    resources reservation process. Will be issued by PG::scrub(), following a
 *    queued "PGScrub" op.
 *
 *  - a special end-of-recovery Primary scrub event ('AfterRepairScrub').
 *
 *  - (for a replica) 'StartReplica' or 'StartReplicaNoWait', triggered by
 *    an incoming MOSDRepScrub message.
 *
 *  note (20.8.21): originally, AfterRepairScrub was triggering a scrub without
 *  waiting for replica resources to be acquired. But once replicas started
 *  using the resource-request to identify and tag the scrub session, this
 *  bypass cannot be supported anymore.
 */
struct NotActive : sc::state<NotActive, ScrubMachine>, NamedSimply {
  explicit NotActive(my_context ctx);

  using reactions =
    mpl::list<sc::custom_reaction<StartScrub>,
	      // a scrubbing that was initiated at recovery completion:
	      sc::custom_reaction<AfterRepairScrub>,
	      sc::transition<StartReplica, ReplicaWaitUpdates>,
	      sc::transition<StartReplicaNoWait, ActiveReplica>>;
  sc::result react(const StartScrub&);
  sc::result react(const AfterRepairScrub&);
};

struct ReservingReplicas : sc::state<ReservingReplicas, ScrubMachine>,
			   NamedSimply {

  explicit ReservingReplicas(my_context ctx);
  ~ReservingReplicas();
  using reactions = mpl::list<sc::custom_reaction<FullReset>,
			      // all replicas granted our resources request
			      sc::transition<RemotesReserved, ActiveScrubbing>,
			      sc::custom_reaction<ReservationFailure>>;

  sc::result react(const FullReset&);

  /// at least one replica denied us the scrub resources we've requested
  sc::result react(const ReservationFailure&);
};


// the "active" sub-states

/// the objects range is blocked
struct RangeBlocked;

/// either delaying the scrub by some time and requeuing, or just requeue
struct PendingTimer;

/// select a chunk to scrub, and verify its availability
struct NewChunk;

struct WaitPushes;
struct WaitLastUpdate;
struct BuildMap;

/// a problem during BuildMap. Wait for all replicas to report, then restart.
struct DrainReplMaps;

/// wait for all replicas to report
struct WaitReplicas;

struct WaitDigestUpdate;

struct ActiveScrubbing
    : sc::state<ActiveScrubbing, ScrubMachine, PendingTimer>, NamedSimply {

  explicit ActiveScrubbing(my_context ctx);
  ~ActiveScrubbing();

  using reactions = mpl::list<sc::custom_reaction<InternalError>,
			      sc::custom_reaction<FullReset>>;

  sc::result react(const FullReset&);
  sc::result react(const InternalError&);
};

struct RangeBlocked : sc::state<RangeBlocked, ActiveScrubbing>, NamedSimply {
  explicit RangeBlocked(my_context ctx);
  using reactions = mpl::list<sc::transition<Unblocked, PendingTimer>>;

  Scrub::BlockedRangeWarning m_timeout;
};

struct PendingTimer : sc::state<PendingTimer, ActiveScrubbing>, NamedSimply {

  explicit PendingTimer(my_context ctx);

  using reactions = mpl::list<sc::transition<InternalSchedScrub, NewChunk>>;
};

struct NewChunk : sc::state<NewChunk, ActiveScrubbing>, NamedSimply {

  explicit NewChunk(my_context ctx);

  using reactions = mpl::list<sc::transition<ChunkIsBusy, RangeBlocked>,
			      sc::custom_reaction<SelectedChunkFree>>;

  sc::result react(const SelectedChunkFree&);
};

/**
 * initiate the update process for this chunk
 *
 * Wait fo 'active_pushes' to clear.
 * 'active_pushes' represents recovery that is in-flight to the local
 * Objectstore, hence scrub waits until the correct data is readable
 * (in-flight data to the Objectstore is not readable until written to
 * disk, termed 'applied' here)
 */
struct WaitPushes : sc::state<WaitPushes, ActiveScrubbing>, NamedSimply {

  explicit WaitPushes(my_context ctx);

  using reactions = mpl::list<sc::custom_reaction<ActivePushesUpd>>;

  sc::result react(const ActivePushesUpd&);
};

struct WaitLastUpdate : sc::state<WaitLastUpdate, ActiveScrubbing>,
			NamedSimply {

  explicit WaitLastUpdate(my_context ctx);

  void on_new_updates(const UpdatesApplied&);

  using reactions =
    mpl::list<sc::custom_reaction<InternalAllUpdates>,
	      sc::in_state_reaction<UpdatesApplied,
				    WaitLastUpdate,
				    &WaitLastUpdate::on_new_updates>>;

  sc::result react(const InternalAllUpdates&);
};

struct BuildMap : sc::state<BuildMap, ActiveScrubbing>, NamedSimply {
  explicit BuildMap(my_context ctx);

  // possible error scenarios:
  // - an error reported by the backend will trigger an 'InternalError' event,
  //   handled by our parent state;
  // - if preempted, we switch to DrainReplMaps, where we will wait for all
  //   replicas to send their maps before acknowledging the preemption;
  // - an interval change will be handled by the relevant 'send-event'
  //   functions, and will translated into a 'FullReset' event.
  using reactions = mpl::list<sc::transition<IntBmPreempted, DrainReplMaps>,
			      // looping, waiting for the backend to finish:
			      sc::transition<InternalSchedScrub, BuildMap>,
			      sc::custom_reaction<IntLocalMapDone>>;

  sc::result react(const IntLocalMapDone&);
};

/*
 *  "drain" scrub-maps responses from replicas
 */
struct DrainReplMaps : sc::state<DrainReplMaps, ActiveScrubbing>, NamedSimply {
  explicit DrainReplMaps(my_context ctx);

  using reactions =
    // all replicas are accounted for:
    mpl::list<sc::custom_reaction<GotReplicas>>;

  sc::result react(const GotReplicas&);
};

struct WaitReplicas : sc::state<WaitReplicas, ActiveScrubbing>, NamedSimply {
  explicit WaitReplicas(my_context ctx);

  using reactions = mpl::list<
    // all replicas are accounted for:
    sc::custom_reaction<GotReplicas>,
    sc::transition<MapsCompared, WaitDigestUpdate>,
    sc::custom_reaction<DigestUpdate>>;

  sc::result react(const GotReplicas&);
  sc::result react(const DigestUpdate&);
  bool all_maps_already_called{false};	// see comment in react code
};

struct WaitDigestUpdate : sc::state<WaitDigestUpdate, ActiveScrubbing>,
			  NamedSimply {
  explicit WaitDigestUpdate(my_context ctx);

  using reactions = mpl::list<sc::custom_reaction<DigestUpdate>,
			      sc::custom_reaction<ScrubFinished>,
			      sc::transition<NextChunk, PendingTimer>>;
  sc::result react(const DigestUpdate&);
  sc::result react(const ScrubFinished&);
};

// ----------------------------- the "replica active" states

/*
 * Waiting for 'active_pushes' to complete
 *
 * When in this state:
 * - the details of the Primary's request were internalized by PgScrubber;
 * - 'active' scrubbing is set
 */
struct ReplicaWaitUpdates : sc::state<ReplicaWaitUpdates, ScrubMachine>,
			    NamedSimply {
  explicit ReplicaWaitUpdates(my_context ctx);
  using reactions = mpl::list<sc::custom_reaction<ReplicaPushesUpd>,
			      sc::custom_reaction<FullReset>>;

  sc::result react(const ReplicaPushesUpd&);
  sc::result react(const FullReset&);
};


struct ActiveReplica : sc::state<ActiveReplica, ScrubMachine>, NamedSimply {
  explicit ActiveReplica(my_context ctx);
  using reactions = mpl::list<sc::custom_reaction<SchedReplica>,
			      sc::custom_reaction<FullReset>>;

  sc::result react(const SchedReplica&);
  sc::result react(const FullReset&);
};

}  // namespace Scrub
