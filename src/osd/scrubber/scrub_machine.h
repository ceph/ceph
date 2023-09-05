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

/// reservations have timed out
MEV(ReservationTimeout)

/// initiate a new scrubbing session (relevant if we are a Primary)
MEV(StartScrub)

/// initiate a new scrubbing session. Only triggered at Recovery completion
MEV(AfterRepairScrub)

/// triggered when the PG unblocked an object that was marked for scrubbing.
/// Via the PGScrubUnblocked op
MEV(Unblocked)

MEV(InternalSchedScrub)

MEV(RangeBlockedAlarm)

MEV(SleepComplete)

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

/// event emitted when the replica grants a reservation to the primary
MEV(ReplicaGrantReservation)

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
struct ReplicaIdle;         ///< Initial reserved replica state
struct ReplicaBuildingMap;	    ///< an active state for a replica.


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

private:
  /**
   * scheduled_event_state_t
   *
   * Heap allocated, ref-counted state shared between scheduled event callback
   * and timer_event_token_t.  Ensures that callback and timer_event_token_t
   * can be safetly destroyed in either order while still allowing for
   * cancellation.
   */
  struct scheduled_event_state_t {
    bool canceled = false;
    ScrubMachineListener::scrubber_callback_cancel_token_t cb_token = nullptr;

    operator bool() const {
      return nullptr != cb_token;
    }

    ~scheduled_event_state_t() {
      /* For the moment, this assert encodes an assumption that we always
       * retain the token until the event either fires or is canceled.
       * If a user needs/wants to relaxt that requirement, this assert can
       * be removed */
      assert(!cb_token);
    }
  };
public:
  /**
   * timer_event_token_t
   *
   * Represents in-flight timer event.  Destroying the object or invoking
   * release() directly will cancel the in-flight timer event preventing it
   * from being delivered.  The intended usage is to invoke
   * schedule_timer_event_after in the constructor of the state machine state
   * intended to handle the event and assign the returned timer_event_token_t
   * to a member of that state. That way, exiting the state will implicitely
   * cancel the event.  See RangedBlocked::m_timeout_token and
   * RangeBlockedAlarm for an example usage.
   */
  class timer_event_token_t {
    friend ScrubMachine;

    // invariant: (bool)parent == (bool)event_state
    ScrubMachine *parent = nullptr;
    std::shared_ptr<scheduled_event_state_t> event_state;

    timer_event_token_t(
      ScrubMachine *parent,
      std::shared_ptr<scheduled_event_state_t> event_state)
      :  parent(parent), event_state(event_state) {
      assert(*this);
    }

    void swap(timer_event_token_t &rhs) {
      std::swap(parent, rhs.parent);
      std::swap(event_state, rhs.event_state);
    }

  public:
    timer_event_token_t() = default;
    timer_event_token_t(timer_event_token_t &&rhs) {
      swap(rhs);
      assert(static_cast<bool>(parent) == static_cast<bool>(event_state));
    }

    timer_event_token_t &operator=(timer_event_token_t &&rhs) {
      swap(rhs);
      assert(static_cast<bool>(parent) == static_cast<bool>(event_state));
      return *this;
    }

    operator bool() const {
      assert(static_cast<bool>(parent) == static_cast<bool>(event_state));
      return parent;
    }

    void release() {
      if (*this) {
	if (*event_state) {
	  parent->m_scrbr->cancel_callback(event_state->cb_token);
	  event_state->canceled = true;
	  event_state->cb_token = nullptr;
	}
	event_state.reset();
	parent = nullptr;
      }
    }

    ~timer_event_token_t() {
      release();
    }
  };

  /**
   * schedule_timer_event_after
   *
   * Schedules event EventT{Args...} to be delivered duration in the future.
   * The implementation implicitely drops the event on interval change.  The
   * returned timer_event_token_t can be used to cancel the event prior to
   * its delivery -- it should generally be embedded as a member in the state
   * intended to handle the event.  See the comment on timer_event_token_t
   * for further information.
   */
  template <typename EventT, typename... Args>
  timer_event_token_t schedule_timer_event_after(
    ceph::timespan duration, Args&&... args) {
    auto token = std::make_shared<scheduled_event_state_t>();
    token->cb_token = m_scrbr->schedule_callback_after(
      duration,
      [this, token, event=EventT(std::forward<Args>(args)...)] {
	if (!token->canceled) {
	  token->cb_token = nullptr;
	  process_event(std::move(event));
	} else {
	  assert(nullptr == token->cb_token);
	}
      }
    );
    return timer_event_token_t{this, token};
  }
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
	      sc::transition<ReplicaGrantReservation, ReplicaIdle>>;
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
			      sc::custom_reaction<ReservationTimeout>,
			      sc::custom_reaction<ReservationFailure>>;

  ceph::coarse_real_clock::time_point entered_at =
    ceph::coarse_real_clock::now();
  ScrubMachine::timer_event_token_t m_timeout_token;

  sc::result react(const FullReset&);

  sc::result react(const ReservationTimeout&);

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
  using reactions = mpl::list<
    sc::custom_reaction<RangeBlockedAlarm>,
    sc::transition<Unblocked, PendingTimer>>;

  ceph::coarse_real_clock::time_point entered_at =
    ceph::coarse_real_clock::now();
  ScrubMachine::timer_event_token_t m_timeout_token;
  sc::result react(const RangeBlockedAlarm &);
};

/**
 * PendingTimer
 *
 * Represents period between chunks.  Waits get_scrub_sleep_time() (if non-zero)
 * by scheduling a SleepComplete event and then queues an InternalSchedScrub
 * to start the next chunk.
 */
struct PendingTimer : sc::state<PendingTimer, ActiveScrubbing>, NamedSimply {

  explicit PendingTimer(my_context ctx);

  using reactions = mpl::list<
    sc::transition<InternalSchedScrub, NewChunk>,
    sc::custom_reaction<SleepComplete>>;

  ceph::coarse_real_clock::time_point entered_at =
    ceph::coarse_real_clock::now();
  ScrubMachine::timer_event_token_t m_sleep_timer;
  sc::result react(const SleepComplete&);
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

/**
 * ReservedReplica
 *
 * Parent state for replica states,  Controls lifecycle for
 * PgScrubber::m_reservations.
 */
struct ReservedReplica : sc::state<ReservedReplica, ScrubMachine, ReplicaIdle>,
			 NamedSimply {
  explicit ReservedReplica(my_context ctx);
  ~ReservedReplica();

  using reactions = mpl::list<sc::transition<FullReset, NotActive>>;
};

struct ReplicaWaitUpdates;

/**
 * ReplicaIdle
 *
 * Replica is waiting for a map request.
 */
struct ReplicaIdle : sc::state<ReplicaIdle, ReservedReplica>,
		     NamedSimply {
  explicit ReplicaIdle(my_context ctx);
  ~ReplicaIdle();

  using reactions = mpl::list<
    sc::transition<StartReplica, ReplicaWaitUpdates>,
    sc::transition<StartReplicaNoWait, ReplicaBuildingMap>>;
};

/**
 * ReservedActiveOp
 *
 * Lifetime matches handling for a single map request op
 */
struct ReplicaActiveOp
  : sc::state<ReplicaActiveOp, ReservedReplica, ReplicaWaitUpdates>,
    NamedSimply {
  explicit ReplicaActiveOp(my_context ctx);
  ~ReplicaActiveOp();
};

/*
 * Waiting for 'active_pushes' to complete
 *
 * When in this state:
 * - the details of the Primary's request were internalized by PgScrubber;
 * - 'active' scrubbing is set
 */
struct ReplicaWaitUpdates : sc::state<ReplicaWaitUpdates, ReservedReplica>,
			    NamedSimply {
  explicit ReplicaWaitUpdates(my_context ctx);
  using reactions = mpl::list<sc::custom_reaction<ReplicaPushesUpd>>;

  sc::result react(const ReplicaPushesUpd&);
};


struct ReplicaBuildingMap : sc::state<ReplicaBuildingMap, ReservedReplica>
			  , NamedSimply {
  explicit ReplicaBuildingMap(my_context ctx);
  using reactions = mpl::list<sc::custom_reaction<SchedReplica>>;

  sc::result react(const SchedReplica&);
};

}  // namespace Scrub
