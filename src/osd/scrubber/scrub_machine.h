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

#include "common/fmt_common.h"
#include "include/Context.h"
#include "common/version.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDRepScrub.h"
#include "messages/MOSDRepScrubMap.h"
#include "osd/scrubber_common.h"

#include "scrub_machine_lstnr.h"
#include "scrub_reservations.h"

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

enum class reservation_status_t {
  unreserved,
  requested_or_granted ///< i.e. must be released
};

//
//  EVENTS
//

void on_event_creation(std::string_view nm);
void on_event_discard(std::string_view nm);


template <typename EV>
struct OpCarryingEvent : sc::event<EV> {
  static constexpr const char* event_name = "<>";
  const OpRequestRef m_op;
  const pg_shard_t m_from;
  OpCarryingEvent(OpRequestRef op, pg_shard_t from) : m_op{op}, m_from{from}
  {
    on_event_creation(static_cast<EV*>(this)->event_name);
  }

  OpCarryingEvent(const OpCarryingEvent&) = default;
  OpCarryingEvent(OpCarryingEvent&&) = default;
  OpCarryingEvent& operator=(const OpCarryingEvent&) = default;
  OpCarryingEvent& operator=(OpCarryingEvent&&) = default;

  void print(std::ostream* out) const
  {
    *out << fmt::format("{} (from: {})", EV::event_name, m_from);
  }
  std::string fmt_print() const
  {
    return fmt::format("{} (from: {})", EV::event_name, m_from);
  }
  std::string_view print() const { return EV::event_name; }
  ~OpCarryingEvent() { on_event_discard(EV::event_name); }
};

#define OP_EV(T)                                                     \
  struct T : OpCarryingEvent<T> {                                    \
    static constexpr const char* event_name = #T;                    \
    template <typename... Args>                                      \
    T(Args&&... args) : OpCarryingEvent(std::forward<Args>(args)...) \
    {                                                                \
    }                                                                \
  }


// reservation events carry peer's request/response data:

/// a replica has granted our reservation request
OP_EV(ReplicaGrant);

/// a replica has denied our reservation request
OP_EV(ReplicaReject);

/// received Primary request for scrub reservation
OP_EV(ReplicaReserveReq);

/// explicit release request from the Primary
OP_EV(ReplicaRelease);

template <typename T, has_formatter V>
struct value_event_t : sc::event<T> {
  const V value;

  template <typename... Args>
  value_event_t(Args&&... args) : value(std::forward<Args>(args)...)
  {
    on_event_creation(T::event_name);
  }

  value_event_t(const value_event_t&) = default;
  value_event_t(value_event_t&&) = default;
  value_event_t& operator=(const value_event_t&) = default;
  value_event_t& operator=(value_event_t&&) = default;
  ~value_event_t() { on_event_discard(T::event_name); }

  template <typename FormatContext>
  auto fmt_print_ctx(FormatContext& ctx) const
  {
    return fmt::format_to(ctx.out(), "{}({})", T::event_name, value);
  }
};

#define VALUE_EVENT(T, V)                                          \
  struct T : value_event_t<T, V> {                                 \
    static constexpr const char* event_name = #T;                  \
    template <typename... Args>                                    \
    T(Args&&... args) : value_event_t(std::forward<Args>(args)...) \
    {                                                              \
    }                                                              \
  };


/// the async-reserver granted our reservation request
VALUE_EVENT(ReserverGranted, AsyncScrubResData);

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

MEV(IntLocalMapDone)

/// external. called upon success of a MODIFY op. See
/// scrub_snapshot_metadata()
MEV(DigestUpdate)

/// peered as Primary - and clean
MEV(PrimaryActivate)

/// we are a replica for this PG
MEV(ReplicaActivate)

/// initiating replica scrub
MEV(StartReplica)

MEV(SchedReplica)

/// Update to active_pushes. 'active_pushes' represents recovery
/// that is in-flight to the local ObjectStore
MEV(ReplicaPushesUpd)

/**
 * IntervalChanged
 * The only path from PrimaryActive or ReplicaActive down to NotActive.
 *
 * Note re reserved replicas:
 * This event notifies the ScrubMachine that it is no longer responsible for
 * releasing replica state.  It will generally be submitted upon a PG interval
 * change.
 *
 * This event is distinct from FullReset because replicas are always responsible
 * for releasing any interval specific state (including but certainly not limited to
 * scrub reservations) upon interval change, without coordination from the
 * Primary.  This event notifies the ScrubMachine that it can forget about
 * such remote state.
 */
MEV(IntervalChanged)

/**
 * stops the scrubbing session, and resets the scrubber.
 * For a replica - aborts the handling of the current request.
 * In both cases - a transition to the peering mode quiescent state (i.e.
 * PrimaryIdle or ReplicaIdle).
 */
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

// the states for a Primary:
// note: PrimaryActive <==> in the OSD scrub queue
struct PrimaryActive;	   ///< base state for a Primary
struct PrimaryIdle;	   ///< ready for a new scrub request
struct Session;            ///< either reserving or actively scrubbing

// the Replica states:
struct ReplicaActive;  ///< base state for when peered as a replica

/// Inactive replica state
struct ReplicaIdle;

// and when handling a single chunk scrub request op:
struct ReplicaActiveOp;
// its sub-states:
struct ReplicaWaitUpdates;
struct ReplicaBuildingMap;


class ScrubMachine : public sc::state_machine<ScrubMachine, NotActive> {
 public:
  friend class PgScrubber;

 public:
  explicit ScrubMachine(PG* pg, ScrubMachineListener* pg_scrub);
  ~ScrubMachine();

  spg_t m_pg_id;
  ScrubMachineListener* m_scrbr;
  std::ostream& gen_prefix(std::ostream& out) const;

  void assert_not_in_session() const;
  [[nodiscard]] bool is_reserving() const;
  [[nodiscard]] bool is_accepting_updates() const;
  [[nodiscard]] bool is_primary_idle() const;

  // elapsed time for the currently active scrub.session
  ceph::timespan get_time_scrubbing() const;

// ///////////////// aux declarations & functions //////////////////////// //


private:
  /**
   * scheduled_event_state_t
   *
   * Heap allocated, ref-counted state shared between scheduled event callback
   * and timer_event_token_t.  Ensures that callback and timer_event_token_t
   * can be safely destroyed in either order while still allowing for
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
       * If a user needs/wants to relax that requirement, this assert can
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
   * to a member of that state. That way, exiting the state will implicitly
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
   * The implementation implicitly drops the event on interval change.  The
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


// ///////////////// the states //////////////////////// //

/*
 * When not scrubbing, the FSM is in one of three states:
 *
 * <> PrimaryActive - we are a Primary and active. The PG
 * is queued for some future scrubs in the OSD's scrub queue.
 *
 * <> ReplicaActive - we are a replica. In this state, we are
 * expecting either a replica reservation request from the Primary, or a
 * scrubbing request for a specific chunk.
 *
 * <> NotActive - the quiescent state. No active scrubbing.
 * We are neither an active Primary nor a replica.
 */
struct NotActive : sc::state<NotActive, ScrubMachine>, NamedSimply {
  explicit NotActive(my_context ctx);

  using reactions = mpl::list<
      // peering done, and we are a replica
      sc::transition<ReplicaActivate, ReplicaActive>,
      // peering done, and we are a Primary
      sc::transition<PrimaryActivate, PrimaryActive>>;
};

// ----------------------- when Primary --------------------------------------
// ---------------------------------------------------------------------------


/*
 *  The primary states:
 *
 *  PrimaryActive - starts when peering ends with us as a primary,
 *     and we are active and clean.
 *   - when in this state - we (our scrub targets) are queued in the
 *     OSD's scrub queue.
 *
 *  Sub-states:
 *     - PrimaryIdle - ready for a new scrub request
 *          * initial state of PrimaryActive
 *
 *     - Session - handling a single scrub session
 */

struct PrimaryIdle;

/**
 *  PrimaryActive
 *
 *  The basic state for an active Primary. Ready to accept a new scrub request.
 *  State managed here: being in the OSD's scrub queue (unless when scrubbing).
 *
 *  Scrubbing is triggered by one of the following events:
 *  - (standard scenario for a Primary): 'StartScrub'. Initiates the OSDs
 *    resources reservation process. Will be issued by PG::scrub(), following a
 *    queued "PGScrub" op.
 *  - a special end-of-recovery Primary scrub event ('AfterRepairScrub').
 */
struct PrimaryActive : sc::state<PrimaryActive, ScrubMachine, PrimaryIdle>,
			 NamedSimply {
  explicit PrimaryActive(my_context ctx);
  ~PrimaryActive();

  using reactions = mpl::list<
      // when the interval ends - we may not be a primary anymore
      sc::transition<IntervalChanged, NotActive>>;

 /**
  * Identifies a specific reservation request.
  * The primary is permitted to cancel outstanding reservation requests without
  * waiting for the pending response from the replica.  Thus, we may, in general,
  * see responses from prior reservation attempts that we need to ignore.  Each
  * reservation request is therefore associated with a nonce incremented within
  * an interval with each reservation request.  Any response with a non-matching
  * nonce must be from a reservation request we canceled.  Note that this check
  * occurs after validating that the message is from the current interval, so
  * reusing nonces between intervals is safe.
  *
  * 0 is a special value used to indicate that the sender did not include a nonce due
  * to not being a sufficiently recent version.
  */
  reservation_nonce_t last_request_sent_nonce{1};
};

/**
 * \ATTN: set_op_parameters() is called while we are still in this state (waiting
 * for a queued OSD message to trigger the transition into Session). Thus,
 * even in this 'idle' state - there is some state we must take care to reset.
 * Specifically - the PG state flags we were playing with in set_op_parameters().
 */
struct PrimaryIdle : sc::state<PrimaryIdle, PrimaryActive>, NamedSimply {
  explicit PrimaryIdle(my_context ctx);
  ~PrimaryIdle() = default;
  void clear_state(const FullReset&);

  using reactions = mpl::list<
      sc::custom_reaction<StartScrub>,
      // a scrubbing that was initiated at recovery completion:
      sc::custom_reaction<AfterRepairScrub>,
      // undoing set_op_params(), if aborted before starting the scrub:
      sc::in_state_reaction<FullReset, PrimaryIdle, &PrimaryIdle::clear_state>>;

  sc::result react(const StartScrub&);
  sc::result react(const AfterRepairScrub&);
};

/**
 *  Session
 *
 *  This state encompasses the two main "active" states: ReservingReplicas and
 *  ActiveScrubbing.
 *  'Session' is the owner of all the resources that are allocated for a
 *  scrub session performed as a Primary.
 *
 *  Exit from this state is either following an interval change, or with
 *  'FullReset' (that would cover all other completion/termination paths).
 *  Note that if terminating the session following an interval change - no
 *  reservations are released. This is because we know that the replicas are
 *  also resetting their reservations.
 */
struct Session : sc::state<Session, PrimaryActive, ReservingReplicas>,
                 NamedSimply {
  explicit Session(my_context ctx);
  ~Session();

  using reactions = mpl::list<sc::transition<FullReset, PrimaryIdle>,
                              sc::custom_reaction<IntervalChanged>>;

  sc::result react(const IntervalChanged&);

  /// managing the scrub session's reservations (optional, as
  /// it's an RAII wrapper around the state of 'holding reservations')
  std::optional<ReplicaReservations> m_reservations{std::nullopt};

  /// the relevant set of performance counters for this session
  /// (relevant, i.e. for this pool type X scrub level)
  PerfCounters* m_perf_set{nullptr};

  /// the time when the session was initiated
  ScrubTimePoint m_session_started_at{ScrubClock::now()};
};

struct ReservingReplicas : sc::state<ReservingReplicas, Session>, NamedSimply {
  explicit ReservingReplicas(my_context ctx);
  ~ReservingReplicas() = default;
  using reactions = mpl::list<
      sc::custom_reaction<ReplicaGrant>,
      sc::custom_reaction<ReplicaReject>,
      sc::transition<RemotesReserved, ActiveScrubbing>>;

  ScrubTimePoint entered_at = ScrubClock::now();

  /// a "raw" event carrying a peer's grant response
  sc::result react(const ReplicaGrant&);

  /// a "raw" event carrying a peer's denial response
  sc::result react(const ReplicaReject&);
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
    : sc::state<ActiveScrubbing, Session, PendingTimer>, NamedSimply {

  explicit ActiveScrubbing(my_context ctx);
  ~ActiveScrubbing();
};

struct RangeBlocked : sc::state<RangeBlocked, ActiveScrubbing>, NamedSimply {
  explicit RangeBlocked(my_context ctx);
  using reactions = mpl::list<
    sc::custom_reaction<RangeBlockedAlarm>,
    sc::transition<Unblocked, PendingTimer>>;

  ScrubTimePoint entered_at = ScrubClock::now();
  ScrubMachine::timer_event_token_t m_timeout_token;
  sc::result react(const RangeBlockedAlarm&);
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

  ScrubTimePoint entered_at = ScrubClock::now();
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
  // - an error reported by the backend will cause the scrubber to
  //   ceph_abort() the OSD. No need to handle it here.
  // - if preempted, we switch to DrainReplMaps, where we will wait for all
  //   replicas to send their maps before acknowledging the preemption;
  // - an interval change will be handled by the relevant 'send-event'
  //   functions, translated into an IntervalChanged event (handled by
  //   the 'Session' state).
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


// ---------------------------------------------------------------------------
// ----------------------------- the "replica active" states -----------------

/*
 *  The replica states:
 *
 *  ReplicaActive - starts after being peered as a replica. Ends on interval.
 *   - maintain the "I am reserved by a primary" state;
 *   - handles reservation requests
 *
 *  - ReplicaIdle - ready for a new scrub request
 *
 *    - initial state of ReplicaActive
 *    - No scrubbing is performed in this state, but reservation-related
 *      events are handled.
 *
 *  - ReplicaActiveOp - handling a single map request op
 *      * ReplicaWaitUpdates
 *      * ReplicaBuildingMap
 */
/*
 * AsyncReserver for scrub 'remote' reservations
 * -----------------------------------------------
 *
 * Unless disabled by 'osd_scrub_disable_reservation_queuing' (*), scrub
 * reservation requests are handled by an async reserver: they are queued,
 * until the number of concurrent scrubs is below the configured limit.

 * (*) Note: the 'osd_scrub_disable_reservation_queuing' option is a temporary
 * debug measure, and will be removed without deprecation in a future release.
 *
 * On the replica side, all reservations are treated as having the same priority.
 * Note that 'high priority' scrubs, e.g. user-initiated scrubs, do not perform
 * reservations on replicas at all.
 *
 * A queued scrub reservation request is cancelled by any of the following events:
 *
 * - a new interval: in this case, we do not expect to see a cancellation request
 *   from the primary, and we can simply remove the request from the queue;
 *
 * - a cancellation request from the primary: probably a result of timing out on
 *   the reservation process. Here, we can simply remove the request from the queue.
 *
 * - a new reservation request for the same PG: this is a bug. We had missed the
 *   previous cancellation request, which could never happen.
 *   We cancel the previous request, and replace
 *   it with the new one. We would also issue an error log message.
 *
 * Primary/Replica with differing versions:
 *
 * The updated version of MOSDScrubReserve contains a new 'wait_for_resources'
 * field. For legacy Primary OSDs, this field is decoded as 'false', and the
 * replica responds immediately, with grant/rejection.
*/


struct ReplicaIdle;

struct ReplicaActive : sc::state<ReplicaActive, ScrubMachine, ReplicaIdle>,
		       NamedSimply {
  explicit ReplicaActive(my_context ctx);
  ~ReplicaActive();
  void exit();

  /**
   * cancel a granted or pending reservation
   *
   * warn_if_no_reservation is set to true if the call is in response to a
   * cancellation from the primary.  In that event, we *must* find a
   * a granted or pending reservation and failing to do so warrants
   * a warning to clog as it is a bug.
   */
  void clear_remote_reservation(bool warn_if_no_reservation);

  void reset_ignored(const FullReset&);

  using reactions = mpl::list<
      sc::transition<IntervalChanged, NotActive>,
      sc::custom_reaction<ReserverGranted>,
      sc::custom_reaction<ReplicaReserveReq>,
      sc::custom_reaction<ReplicaRelease>,
      sc::in_state_reaction<
	  FullReset,
	  ReplicaActive,
	  &ReplicaActive::reset_ignored>>;

  /// handle a reservation request from a primary
  sc::result react(const ReplicaReserveReq& ev);

  /*
   * the Primary released the reservation.
   * Note: The ActiveReplicaOp state will handle this event as well.
   */
  sc::result react(const ReplicaRelease&);

  /**
   * the queued reservation request was granted by the async reserver.
   * Notify the Primary.
   */
  sc::result react(const ReserverGranted&);

  /**
   * a reservation request with this nonce is queued at the scrub_reserver,
   * and was not yet granted.
   */
  MOSDScrubReserve::reservation_nonce_t pending_reservation_nonce{0};

 private:
  PG* m_pg;
  OSDService* m_osds;

  // --- remote reservation machinery

  /*
   * 'reservation_granted' is set to 'true' when we have grant confirmation
   *  to the primary, and the reservation has not yet been canceled (either
   *  by the primary or following an interval change).
   *
   * Note the interaction with 'pending_reservation_nonce': the combination
   * of these two variables is used to track the state of the reservation
   * with the scrub_reserver. The possible combinations:
   * - pending_reservation_nonce == 0 && !reservation_granted -- no reservation
   *    was granted, and none is pending;
   * - pending_reservation_nonce != 0 && !reservation_granted -- we have a
   *   pending cb in the AsyncReserver for a request with nonce
   *   'pending_reservation_nonce'
   * - pending_reservation_nonce == 0 && reservation_granted -- we have sent
   *   a response to the primary granting the reservation
   * (invariant: !((pending_reservation_nonce != 0) && reservation_granted)
   *
   * Note that in the event that the primary is too old to support asynchronous
   * reservation, MOSDScrubReserve::wait_for_resources will be set to false by
   * the decoder and we bypass the 2'nd case above.
   */
  bool reservation_granted{false};

  reservation_status_t m_reservation_status{reservation_status_t::unreserved};

  /**
   * React to the reservation request.
   * Called after any existing pending/granted request was released.
   *
   * Async requests are sent to the reserver.
   * For old-style synchronous requests, the reserver is queried using
   * its 'immediate' interface, and the response is sent back to the primary.
   */
  void handle_reservation_request(const ReplicaReserveReq& ev);

  // clang-format off
  struct RtReservationCB : public Context {
    PGRef pg;
    AsyncScrubResData res_data;

    explicit RtReservationCB(PGRef pg, AsyncScrubResData request_details)
	: pg{pg}
	, res_data{request_details}
    {}

    void finish(int) override {
      pg->lock();
      pg->m_scrubber->send_granted_by_reserver(res_data);
      pg->unlock();
    }
  };
  // clang-format on
};


struct ReplicaIdle : sc::state<ReplicaIdle, ReplicaActive>, NamedSimply {
  explicit ReplicaIdle(my_context ctx);
  ~ReplicaIdle() = default;
  using reactions = mpl::list<sc::custom_reaction<StartReplica>>;

  sc::result react(const StartReplica& ev);
};


/**
 * ReplicaActiveOp
 *
 * Lifetime matches handling for a single map request op.
 */
struct ReplicaActiveOp
    : sc::state<ReplicaActiveOp, ReplicaActive, ReplicaWaitUpdates>,
      NamedSimply {
  explicit ReplicaActiveOp(my_context ctx);
  ~ReplicaActiveOp();

  using reactions = mpl::list<
      sc::custom_reaction<StartReplica>,
      sc::custom_reaction<ReplicaRelease>>;

  /**
   * Handling the unexpected (read - caused by a bug) case of receiving a
   * new chunk request while still handling the previous one.
   * To note:
   * - the primary is evidently no longer waiting for the results of the
   *   previous request. On the other hand
   * - we must respond to the new request, as the primary would wait for
   *   it "forever"`,
   * - and we should log this unexpected scenario clearly in the cluster log.
   */
  sc::result react(const StartReplica&);

  /**
   * a 'release' was send by the primary. Possible scenario: 'no-scrub'
   * abort. We abort the current chunk handling and re-enter ReplicaActive,
   * releasing the reservation on the way.
   */
  sc::result react(const ReplicaRelease&);
};

/*
 * Waiting for 'active_pushes' to complete
 *
 * When in this state:
 * - the details of the Primary's request were internalized by PgScrubber;
 * - 'active' scrubbing is set
 */
struct ReplicaWaitUpdates : sc::state<ReplicaWaitUpdates, ReplicaActiveOp>,
			    NamedSimply {
  explicit ReplicaWaitUpdates(my_context ctx);
  using reactions = mpl::list<sc::custom_reaction<ReplicaPushesUpd>>;

  sc::result react(const ReplicaPushesUpd&);
};

struct ReplicaBuildingMap : sc::state<ReplicaBuildingMap, ReplicaActiveOp>,
			    NamedSimply {
  explicit ReplicaBuildingMap(my_context ctx);
  using reactions = mpl::list<sc::custom_reaction<SchedReplica>>;

  sc::result react(const SchedReplica&);
};

}  // namespace Scrub
