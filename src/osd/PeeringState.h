// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/statechart/custom_reaction.hpp>
#include <boost/statechart/event.hpp>
#include <boost/statechart/simple_state.hpp>
#include <boost/statechart/state.hpp>
#include <boost/statechart/state_machine.hpp>
#include <boost/statechart/transition.hpp>
#include <boost/statechart/event_base.hpp>

#include "PGPeeringEvent.h"
#include "os/ObjectStore.h"
#include "OSDMap.h"

class PG;

  /* Encapsulates PG recovery process */
class PeeringState {
public:
  struct NamedState {
    const char *state_name;
    utime_t enter_time;
    PG* pg;
    const char *get_state_name() { return state_name; }
    NamedState(PG *pg_, const char *state_name_);
    virtual ~NamedState();
  };

  // [primary only] content recovery state
  struct BufferedRecoveryMessages {
    map<int, map<spg_t, pg_query_t> > query_map;
    map<int, vector<pair<pg_notify_t, PastIntervals> > > info_map;
    map<int, vector<pair<pg_notify_t, PastIntervals> > > notify_list;
  };

  struct PeeringCtx {
    utime_t start_time;
    map<int, map<spg_t, pg_query_t> > *query_map;
    map<int, vector<pair<pg_notify_t, PastIntervals> > > *info_map;
    map<int, vector<pair<pg_notify_t, PastIntervals> > > *notify_list;
    ObjectStore::Transaction *transaction;
    ThreadPool::TPHandle* handle;
    PeeringCtx(map<int, map<spg_t, pg_query_t> > *query_map,
		map<int,
		    vector<pair<pg_notify_t, PastIntervals> > > *info_map,
		map<int,
		    vector<pair<pg_notify_t, PastIntervals> > > *notify_list,
		ObjectStore::Transaction *transaction)
      : query_map(query_map), info_map(info_map),
	notify_list(notify_list),
	transaction(transaction),
        handle(NULL) {}

    PeeringCtx(BufferedRecoveryMessages &buf, PeeringCtx &rctx)
      : query_map(&(buf.query_map)),
	info_map(&(buf.info_map)),
	notify_list(&(buf.notify_list)),
	transaction(rctx.transaction),
        handle(rctx.handle) {}

    void accept_buffered_messages(BufferedRecoveryMessages &m) {
      ceph_assert(query_map);
      ceph_assert(info_map);
      ceph_assert(notify_list);
      for (map<int, map<spg_t, pg_query_t> >::iterator i = m.query_map.begin();
	   i != m.query_map.end();
	   ++i) {
	map<spg_t, pg_query_t> &omap = (*query_map)[i->first];
	for (map<spg_t, pg_query_t>::iterator j = i->second.begin();
	     j != i->second.end();
	     ++j) {
	  omap[j->first] = j->second;
	}
      }
      for (map<int, vector<pair<pg_notify_t, PastIntervals> > >::iterator i
	     = m.info_map.begin();
	   i != m.info_map.end();
	   ++i) {
	vector<pair<pg_notify_t, PastIntervals> > &ovec =
	  (*info_map)[i->first];
	ovec.reserve(ovec.size() + i->second.size());
	ovec.insert(ovec.end(), i->second.begin(), i->second.end());
      }
      for (map<int, vector<pair<pg_notify_t, PastIntervals> > >::iterator i
	     = m.notify_list.begin();
	   i != m.notify_list.end();
	   ++i) {
	vector<pair<pg_notify_t, PastIntervals> > &ovec =
	  (*notify_list)[i->first];
	ovec.reserve(ovec.size() + i->second.size());
	ovec.insert(ovec.end(), i->second.begin(), i->second.end());
      }
    }

    void send_notify(pg_shard_t to,
		     const pg_notify_t &info, const PastIntervals &pi) {
      ceph_assert(notify_list);
      (*notify_list)[to.osd].emplace_back(info, pi);
    }
  };

  struct QueryState : boost::statechart::event< QueryState > {
    Formatter *f;
    explicit QueryState(Formatter *f) : f(f) {}
    void print(std::ostream *out) const {
      *out << "Query";
    }
  };

  struct AdvMap : boost::statechart::event< AdvMap > {
    OSDMapRef osdmap;
    OSDMapRef lastmap;
    vector<int> newup, newacting;
    int up_primary, acting_primary;
    AdvMap(
      OSDMapRef osdmap, OSDMapRef lastmap,
      vector<int>& newup, int up_primary,
      vector<int>& newacting, int acting_primary):
      osdmap(osdmap), lastmap(lastmap),
      newup(newup),
      newacting(newacting),
      up_primary(up_primary),
      acting_primary(acting_primary) {}
    void print(std::ostream *out) const {
      *out << "AdvMap";
    }
  };

  struct ActMap : boost::statechart::event< ActMap > {
    ActMap() : boost::statechart::event< ActMap >() {}
    void print(std::ostream *out) const {
      *out << "ActMap";
    }
  };
  struct Activate : boost::statechart::event< Activate > {
    epoch_t activation_epoch;
    explicit Activate(epoch_t q) : boost::statechart::event< Activate >(),
			  activation_epoch(q) {}
    void print(std::ostream *out) const {
      *out << "Activate from " << activation_epoch;
    }
  };
public:
  struct UnfoundBackfill : boost::statechart::event<UnfoundBackfill> {
    explicit UnfoundBackfill() {}
    void print(std::ostream *out) const {
      *out << "UnfoundBackfill";
    }
  };
  struct UnfoundRecovery : boost::statechart::event<UnfoundRecovery> {
    explicit UnfoundRecovery() {}
    void print(std::ostream *out) const {
      *out << "UnfoundRecovery";
    }
  };

  struct RequestScrub : boost::statechart::event<RequestScrub> {
    bool deep;
    bool repair;
    explicit RequestScrub(bool d, bool r) : deep(d), repair(r) {}
    void print(std::ostream *out) const {
      *out << "RequestScrub(" << (deep ? "deep" : "shallow")
	   << (repair ? " repair" : "");
    }
  };

  TrivialEvent(Initialize)
  TrivialEvent(GotInfo)
  TrivialEvent(NeedUpThru)
  TrivialEvent(Backfilled)
  TrivialEvent(LocalBackfillReserved)
  TrivialEvent(RejectRemoteReservation)
  TrivialEvent(RequestBackfill)
  TrivialEvent(RemoteRecoveryPreempted)
  TrivialEvent(RemoteBackfillPreempted)
  TrivialEvent(BackfillTooFull)
  TrivialEvent(RecoveryTooFull)

  TrivialEvent(MakePrimary)
  TrivialEvent(MakeStray)
  TrivialEvent(NeedActingChange)
  TrivialEvent(IsIncomplete)
  TrivialEvent(IsDown)

  TrivialEvent(AllReplicasRecovered)
  TrivialEvent(DoRecovery)
  TrivialEvent(LocalRecoveryReserved)
  TrivialEvent(AllRemotesReserved)
  TrivialEvent(AllBackfillsReserved)
  TrivialEvent(GoClean)

  TrivialEvent(AllReplicasActivated)

  TrivialEvent(IntervalFlush)

  TrivialEvent(DeleteStart)
  TrivialEvent(DeleteSome)

  TrivialEvent(SetForceRecovery)
  TrivialEvent(UnsetForceRecovery)
  TrivialEvent(SetForceBackfill)
  TrivialEvent(UnsetForceBackfill)

  TrivialEvent(DeleteReserved)
  TrivialEvent(DeleteInterrupted)

  void start_handle(PeeringCtx *new_ctx);
  void end_handle();
  void begin_block_outgoing();
  void end_block_outgoing();
  void clear_blocked_outgoing();
 private:

  /* States */
  struct Initial;
  class PeeringMachine : public boost::statechart::state_machine< PeeringMachine, Initial > {
    PeeringState *state;
  public:
    CephContext *cct;
    spg_t spgid;
    PG *pg;

    utime_t event_time;
    uint64_t event_count;

    void clear_event_counters() {
      event_time = utime_t();
      event_count = 0;
    }

    void log_enter(const char *state_name);
    void log_exit(const char *state_name, utime_t duration);

    PeeringMachine(PeeringState *state, CephContext *cct, spg_t spgid, PG *pg) :
      state(state), cct(cct), spgid(spgid), pg(pg), event_count(0) {}

    /* Accessor functions for state methods */
    ObjectStore::Transaction* get_cur_transaction() {
      ceph_assert(state->rctx);
      ceph_assert(state->rctx->transaction);
      return state->rctx->transaction;
    }

    void send_query(pg_shard_t to, const pg_query_t &query);

    map<int, map<spg_t, pg_query_t> > *get_query_map() {
      ceph_assert(state->rctx);
      ceph_assert(state->rctx->query_map);
      return state->rctx->query_map;
    }

    map<int, vector<pair<pg_notify_t, PastIntervals> > > *get_info_map() {
      ceph_assert(state->rctx);
      ceph_assert(state->rctx->info_map);
      return state->rctx->info_map;
    }

    PeeringCtx *get_recovery_ctx() { return &*(state->rctx); }

    void send_notify(pg_shard_t to,
	       const pg_notify_t &info, const PastIntervals &pi) {
      ceph_assert(state->rctx);
      state->rctx->send_notify(to, info, pi);
    }
  };
  friend class PeeringMachine;

  /* States */
  // Initial
  // Reset
  // Start
  //   Started
  //     Primary
  //       WaitActingChange
  //       Peering
  //         GetInfo
  //         GetLog
  //         GetMissing
  //         WaitUpThru
  //         Incomplete
  //       Active
  //         Activating
  //         Clean
  //         Recovered
  //         Backfilling
  //         WaitRemoteBackfillReserved
  //         WaitLocalBackfillReserved
  //         NotBackfilling
  //         NotRecovering
  //         Recovering
  //         WaitRemoteRecoveryReserved
  //         WaitLocalRecoveryReserved
  //     ReplicaActive
  //       RepNotRecovering
  //       RepRecovering
  //       RepWaitBackfillReserved
  //       RepWaitRecoveryReserved
  //     Stray
  //     ToDelete
  //       WaitDeleteReserved
  //       Deleting
  // Crashed

  struct Crashed : boost::statechart::state< Crashed, PeeringMachine >, NamedState {
    explicit Crashed(my_context ctx);
  };

  struct Reset;

  struct Initial : boost::statechart::state< Initial, PeeringMachine >, NamedState {
    explicit Initial(my_context ctx);
    void exit();

    typedef boost::mpl::list <
      boost::statechart::transition< Initialize, Reset >,
      boost::statechart::custom_reaction< NullEvt >,
      boost::statechart::transition< boost::statechart::event_base, Crashed >
      > reactions;

    boost::statechart::result react(const MNotifyRec&);
    boost::statechart::result react(const MInfoRec&);
    boost::statechart::result react(const MLogRec&);
    boost::statechart::result react(const boost::statechart::event_base&) {
      return discard_event();
    }
  };

  struct Reset : boost::statechart::state< Reset, PeeringMachine >, NamedState {
    explicit Reset(my_context ctx);
    void exit();

    typedef boost::mpl::list <
      boost::statechart::custom_reaction< QueryState >,
      boost::statechart::custom_reaction< AdvMap >,
      boost::statechart::custom_reaction< ActMap >,
      boost::statechart::custom_reaction< NullEvt >,
      boost::statechart::custom_reaction< IntervalFlush >,
      boost::statechart::transition< boost::statechart::event_base, Crashed >
      > reactions;
    boost::statechart::result react(const QueryState& q);
    boost::statechart::result react(const AdvMap&);
    boost::statechart::result react(const ActMap&);
    boost::statechart::result react(const IntervalFlush&);
    boost::statechart::result react(const boost::statechart::event_base&) {
      return discard_event();
    }
  };

  struct Start;

  struct Started : boost::statechart::state< Started, PeeringMachine, Start >, NamedState {
    explicit Started(my_context ctx);
    void exit();

    typedef boost::mpl::list <
      boost::statechart::custom_reaction< QueryState >,
      boost::statechart::custom_reaction< AdvMap >,
      boost::statechart::custom_reaction< IntervalFlush >,
      // ignored
      boost::statechart::custom_reaction< NullEvt >,
      boost::statechart::custom_reaction<SetForceRecovery>,
      boost::statechart::custom_reaction<UnsetForceRecovery>,
      boost::statechart::custom_reaction<SetForceBackfill>,
      boost::statechart::custom_reaction<UnsetForceBackfill>,
      boost::statechart::custom_reaction<RequestScrub>,
      // crash
      boost::statechart::transition< boost::statechart::event_base, Crashed >
      > reactions;
    boost::statechart::result react(const QueryState& q);
    boost::statechart::result react(const AdvMap&);
    boost::statechart::result react(const IntervalFlush&);
    boost::statechart::result react(const boost::statechart::event_base&) {
      return discard_event();
    }
  };

  struct Primary;
  struct Stray;

  struct Start : boost::statechart::state< Start, Started >, NamedState {
    explicit Start(my_context ctx);
    void exit();

    typedef boost::mpl::list <
      boost::statechart::transition< MakePrimary, Primary >,
      boost::statechart::transition< MakeStray, Stray >
      > reactions;
  };

  struct Peering;
  struct WaitActingChange;
  struct Incomplete;
  struct Down;

  struct Primary : boost::statechart::state< Primary, Started, Peering >, NamedState {
    explicit Primary(my_context ctx);
    void exit();

    typedef boost::mpl::list <
      boost::statechart::custom_reaction< ActMap >,
      boost::statechart::custom_reaction< MNotifyRec >,
      boost::statechart::transition< NeedActingChange, WaitActingChange >,
      boost::statechart::custom_reaction<SetForceRecovery>,
      boost::statechart::custom_reaction<UnsetForceRecovery>,
      boost::statechart::custom_reaction<SetForceBackfill>,
      boost::statechart::custom_reaction<UnsetForceBackfill>,
      boost::statechart::custom_reaction<RequestScrub>
      > reactions;
    boost::statechart::result react(const ActMap&);
    boost::statechart::result react(const MNotifyRec&);
    boost::statechart::result react(const SetForceRecovery&);
    boost::statechart::result react(const UnsetForceRecovery&);
    boost::statechart::result react(const SetForceBackfill&);
    boost::statechart::result react(const UnsetForceBackfill&);
    boost::statechart::result react(const RequestScrub&);
  };

  struct WaitActingChange : boost::statechart::state< WaitActingChange, Primary>,
		      NamedState {
    typedef boost::mpl::list <
      boost::statechart::custom_reaction< QueryState >,
      boost::statechart::custom_reaction< AdvMap >,
      boost::statechart::custom_reaction< MLogRec >,
      boost::statechart::custom_reaction< MInfoRec >,
      boost::statechart::custom_reaction< MNotifyRec >
      > reactions;
    explicit WaitActingChange(my_context ctx);
    boost::statechart::result react(const QueryState& q);
    boost::statechart::result react(const AdvMap&);
    boost::statechart::result react(const MLogRec&);
    boost::statechart::result react(const MInfoRec&);
    boost::statechart::result react(const MNotifyRec&);
    void exit();
  };

  struct GetInfo;
  struct Active;

  struct Peering : boost::statechart::state< Peering, Primary, GetInfo >, NamedState {
    PastIntervals::PriorSet prior_set;
    bool history_les_bound;  //< need osd_find_best_info_ignore_history_les

    explicit Peering(my_context ctx);
    void exit();

    typedef boost::mpl::list <
      boost::statechart::custom_reaction< QueryState >,
      boost::statechart::transition< Activate, Active >,
      boost::statechart::custom_reaction< AdvMap >
      > reactions;
    boost::statechart::result react(const QueryState& q);
    boost::statechart::result react(const AdvMap &advmap);
  };

  struct WaitLocalRecoveryReserved;
  struct Activating;
  struct Active : boost::statechart::state< Active, Primary, Activating >, NamedState {
    explicit Active(my_context ctx);
    void exit();

    const set<pg_shard_t> remote_shards_to_reserve_recovery;
    const set<pg_shard_t> remote_shards_to_reserve_backfill;
    bool all_replicas_activated;

    typedef boost::mpl::list <
      boost::statechart::custom_reaction< QueryState >,
      boost::statechart::custom_reaction< ActMap >,
      boost::statechart::custom_reaction< AdvMap >,
      boost::statechart::custom_reaction< MInfoRec >,
      boost::statechart::custom_reaction< MNotifyRec >,
      boost::statechart::custom_reaction< MLogRec >,
      boost::statechart::custom_reaction< MTrim >,
      boost::statechart::custom_reaction< Backfilled >,
      boost::statechart::custom_reaction< AllReplicasActivated >,
      boost::statechart::custom_reaction< DeferRecovery >,
      boost::statechart::custom_reaction< DeferBackfill >,
      boost::statechart::custom_reaction< UnfoundRecovery >,
      boost::statechart::custom_reaction< UnfoundBackfill >,
      boost::statechart::custom_reaction< RemoteReservationRevokedTooFull>,
      boost::statechart::custom_reaction< RemoteReservationRevoked>,
      boost::statechart::custom_reaction< DoRecovery>
      > reactions;
    boost::statechart::result react(const QueryState& q);
    boost::statechart::result react(const ActMap&);
    boost::statechart::result react(const AdvMap&);
    boost::statechart::result react(const MInfoRec& infoevt);
    boost::statechart::result react(const MNotifyRec& notevt);
    boost::statechart::result react(const MLogRec& logevt);
    boost::statechart::result react(const MTrim& trimevt);
    boost::statechart::result react(const Backfilled&) {
      return discard_event();
    }
    boost::statechart::result react(const AllReplicasActivated&);
    boost::statechart::result react(const DeferRecovery& evt) {
      return discard_event();
    }
    boost::statechart::result react(const DeferBackfill& evt) {
      return discard_event();
    }
    boost::statechart::result react(const UnfoundRecovery& evt) {
      return discard_event();
    }
    boost::statechart::result react(const UnfoundBackfill& evt) {
      return discard_event();
    }
    boost::statechart::result react(const RemoteReservationRevokedTooFull&) {
      return discard_event();
    }
    boost::statechart::result react(const RemoteReservationRevoked&) {
      return discard_event();
    }
    boost::statechart::result react(const DoRecovery&) {
      return discard_event();
    }
  };

  struct Clean : boost::statechart::state< Clean, Active >, NamedState {
    typedef boost::mpl::list<
      boost::statechart::transition< DoRecovery, WaitLocalRecoveryReserved >,
      boost::statechart::custom_reaction<SetForceRecovery>,
      boost::statechart::custom_reaction<SetForceBackfill>
    > reactions;
    explicit Clean(my_context ctx);
    void exit();
    boost::statechart::result react(const boost::statechart::event_base&) {
      return discard_event();
    }
  };

  struct Recovered : boost::statechart::state< Recovered, Active >, NamedState {
    typedef boost::mpl::list<
      boost::statechart::transition< GoClean, Clean >,
      boost::statechart::transition< DoRecovery, WaitLocalRecoveryReserved >,
      boost::statechart::custom_reaction< AllReplicasActivated >
    > reactions;
    explicit Recovered(my_context ctx);
    void exit();
    boost::statechart::result react(const AllReplicasActivated&) {
      post_event(GoClean());
      return forward_event();
    }
  };

  struct Backfilling : boost::statechart::state< Backfilling, Active >, NamedState {
    typedef boost::mpl::list<
      boost::statechart::custom_reaction< Backfilled >,
      boost::statechart::custom_reaction< DeferBackfill >,
      boost::statechart::custom_reaction< UnfoundBackfill >,
      boost::statechart::custom_reaction< RemoteReservationRejected >,
      boost::statechart::custom_reaction< RemoteReservationRevokedTooFull>,
      boost::statechart::custom_reaction< RemoteReservationRevoked>
      > reactions;
    explicit Backfilling(my_context ctx);
    boost::statechart::result react(const RemoteReservationRejected& evt) {
      // for compat with old peers
      post_event(RemoteReservationRevokedTooFull());
      return discard_event();
    }
    void backfill_release_reservations();
    boost::statechart::result react(const Backfilled& evt);
    boost::statechart::result react(const RemoteReservationRevokedTooFull& evt);
    boost::statechart::result react(const RemoteReservationRevoked& evt);
    boost::statechart::result react(const DeferBackfill& evt);
    boost::statechart::result react(const UnfoundBackfill& evt);
    void cancel_backfill();
    void exit();
  };

  struct WaitRemoteBackfillReserved : boost::statechart::state< WaitRemoteBackfillReserved, Active >, NamedState {
    typedef boost::mpl::list<
      boost::statechart::custom_reaction< RemoteBackfillReserved >,
      boost::statechart::custom_reaction< RemoteReservationRejected >,
      boost::statechart::custom_reaction< RemoteReservationRevoked >,
      boost::statechart::transition< AllBackfillsReserved, Backfilling >
      > reactions;
    set<pg_shard_t>::const_iterator backfill_osd_it;
    explicit WaitRemoteBackfillReserved(my_context ctx);
    void retry();
    void exit();
    boost::statechart::result react(const RemoteBackfillReserved& evt);
    boost::statechart::result react(const RemoteReservationRejected& evt);
    boost::statechart::result react(const RemoteReservationRevoked& evt);
  };

  struct WaitLocalBackfillReserved : boost::statechart::state< WaitLocalBackfillReserved, Active >, NamedState {
    typedef boost::mpl::list<
      boost::statechart::transition< LocalBackfillReserved, WaitRemoteBackfillReserved >
      > reactions;
    explicit WaitLocalBackfillReserved(my_context ctx);
    void exit();
  };

  struct NotBackfilling : boost::statechart::state< NotBackfilling, Active>, NamedState {
    typedef boost::mpl::list<
      boost::statechart::transition< RequestBackfill, WaitLocalBackfillReserved>,
      boost::statechart::custom_reaction< RemoteBackfillReserved >,
      boost::statechart::custom_reaction< RemoteReservationRejected >
      > reactions;
    explicit NotBackfilling(my_context ctx);
    void exit();
    boost::statechart::result react(const RemoteBackfillReserved& evt);
    boost::statechart::result react(const RemoteReservationRejected& evt);
  };

  struct NotRecovering : boost::statechart::state< NotRecovering, Active>, NamedState {
    typedef boost::mpl::list<
      boost::statechart::transition< DoRecovery, WaitLocalRecoveryReserved >,
      boost::statechart::custom_reaction< DeferRecovery >,
      boost::statechart::custom_reaction< UnfoundRecovery >
      > reactions;
    explicit NotRecovering(my_context ctx);
    boost::statechart::result react(const DeferRecovery& evt) {
      /* no-op */
      return discard_event();
    }
    boost::statechart::result react(const UnfoundRecovery& evt) {
      /* no-op */
      return discard_event();
    }
    void exit();
  };

  struct ToDelete;
  struct RepNotRecovering;
  struct ReplicaActive : boost::statechart::state< ReplicaActive, Started, RepNotRecovering >, NamedState {
    explicit ReplicaActive(my_context ctx);
    void exit();

    typedef boost::mpl::list <
      boost::statechart::custom_reaction< QueryState >,
      boost::statechart::custom_reaction< ActMap >,
      boost::statechart::custom_reaction< MQuery >,
      boost::statechart::custom_reaction< MInfoRec >,
      boost::statechart::custom_reaction< MLogRec >,
      boost::statechart::custom_reaction< MTrim >,
      boost::statechart::custom_reaction< Activate >,
      boost::statechart::custom_reaction< DeferRecovery >,
      boost::statechart::custom_reaction< DeferBackfill >,
      boost::statechart::custom_reaction< UnfoundRecovery >,
      boost::statechart::custom_reaction< UnfoundBackfill >,
      boost::statechart::custom_reaction< RemoteBackfillPreempted >,
      boost::statechart::custom_reaction< RemoteRecoveryPreempted >,
      boost::statechart::custom_reaction< RecoveryDone >,
      boost::statechart::transition<DeleteStart, ToDelete>
      > reactions;
    boost::statechart::result react(const QueryState& q);
    boost::statechart::result react(const MInfoRec& infoevt);
    boost::statechart::result react(const MLogRec& logevt);
    boost::statechart::result react(const MTrim& trimevt);
    boost::statechart::result react(const ActMap&);
    boost::statechart::result react(const MQuery&);
    boost::statechart::result react(const Activate&);
    boost::statechart::result react(const RecoveryDone&) {
      return discard_event();
    }
    boost::statechart::result react(const DeferRecovery& evt) {
      return discard_event();
    }
    boost::statechart::result react(const DeferBackfill& evt) {
      return discard_event();
    }
    boost::statechart::result react(const UnfoundRecovery& evt) {
      return discard_event();
    }
    boost::statechart::result react(const UnfoundBackfill& evt) {
      return discard_event();
    }
    boost::statechart::result react(const RemoteBackfillPreempted& evt) {
      return discard_event();
    }
    boost::statechart::result react(const RemoteRecoveryPreempted& evt) {
      return discard_event();
    }
  };

  struct RepRecovering : boost::statechart::state< RepRecovering, ReplicaActive >, NamedState {
    typedef boost::mpl::list<
      boost::statechart::transition< RecoveryDone, RepNotRecovering >,
      // for compat with old peers
      boost::statechart::transition< RemoteReservationRejected, RepNotRecovering >,
      boost::statechart::transition< RemoteReservationCanceled, RepNotRecovering >,
      boost::statechart::custom_reaction< BackfillTooFull >,
      boost::statechart::custom_reaction< RemoteRecoveryPreempted >,
      boost::statechart::custom_reaction< RemoteBackfillPreempted >
      > reactions;
    explicit RepRecovering(my_context ctx);
    boost::statechart::result react(const RemoteRecoveryPreempted &evt);
    boost::statechart::result react(const BackfillTooFull &evt);
    boost::statechart::result react(const RemoteBackfillPreempted &evt);
    void exit();
  };

  struct RepWaitBackfillReserved : boost::statechart::state< RepWaitBackfillReserved, ReplicaActive >, NamedState {
    typedef boost::mpl::list<
      boost::statechart::custom_reaction< RemoteBackfillReserved >,
      boost::statechart::custom_reaction< RejectRemoteReservation >,
      boost::statechart::custom_reaction< RemoteReservationRejected >,
      boost::statechart::custom_reaction< RemoteReservationCanceled >
      > reactions;
    explicit RepWaitBackfillReserved(my_context ctx);
    void exit();
    boost::statechart::result react(const RemoteBackfillReserved &evt);
    boost::statechart::result react(const RejectRemoteReservation &evt);
    boost::statechart::result react(const RemoteReservationRejected &evt);
    boost::statechart::result react(const RemoteReservationCanceled &evt);
  };

  struct RepWaitRecoveryReserved : boost::statechart::state< RepWaitRecoveryReserved, ReplicaActive >, NamedState {
    typedef boost::mpl::list<
      boost::statechart::custom_reaction< RemoteRecoveryReserved >,
      // for compat with old peers
      boost::statechart::custom_reaction< RemoteReservationRejected >,
      boost::statechart::custom_reaction< RemoteReservationCanceled >
      > reactions;
    explicit RepWaitRecoveryReserved(my_context ctx);
    void exit();
    boost::statechart::result react(const RemoteRecoveryReserved &evt);
    boost::statechart::result react(const RemoteReservationRejected &evt) {
      // for compat with old peers
      post_event(RemoteReservationCanceled());
      return discard_event();
    }
    boost::statechart::result react(const RemoteReservationCanceled &evt);
  };

  struct RepNotRecovering : boost::statechart::state< RepNotRecovering, ReplicaActive>, NamedState {
    typedef boost::mpl::list<
      boost::statechart::custom_reaction< RequestRecoveryPrio >,
      boost::statechart::custom_reaction< RequestBackfillPrio >,
      boost::statechart::custom_reaction< RejectRemoteReservation >,
      boost::statechart::transition< RemoteReservationRejected, RepNotRecovering >,
      boost::statechart::transition< RemoteReservationCanceled, RepNotRecovering >,
      boost::statechart::custom_reaction< RemoteRecoveryReserved >,
      boost::statechart::custom_reaction< RemoteBackfillReserved >,
      boost::statechart::transition< RecoveryDone, RepNotRecovering >  // for compat with pre-reservation peers
      > reactions;
    explicit RepNotRecovering(my_context ctx);
    boost::statechart::result react(const RequestRecoveryPrio &evt);
    boost::statechart::result react(const RequestBackfillPrio &evt);
    boost::statechart::result react(const RemoteBackfillReserved &evt) {
      // my reservation completion raced with a RELEASE from primary
      return discard_event();
    }
    boost::statechart::result react(const RemoteRecoveryReserved &evt) {
      // my reservation completion raced with a RELEASE from primary
      return discard_event();
    }
    boost::statechart::result react(const RejectRemoteReservation &evt);
    void exit();
  };

  struct Recovering : boost::statechart::state< Recovering, Active >, NamedState {
    typedef boost::mpl::list <
      boost::statechart::custom_reaction< AllReplicasRecovered >,
      boost::statechart::custom_reaction< DeferRecovery >,
      boost::statechart::custom_reaction< UnfoundRecovery >,
      boost::statechart::custom_reaction< RequestBackfill >
      > reactions;
    explicit Recovering(my_context ctx);
    void exit();
    void release_reservations(bool cancel = false);
    boost::statechart::result react(const AllReplicasRecovered &evt);
    boost::statechart::result react(const DeferRecovery& evt);
    boost::statechart::result react(const UnfoundRecovery& evt);
    boost::statechart::result react(const RequestBackfill &evt);
  };

  struct WaitRemoteRecoveryReserved : boost::statechart::state< WaitRemoteRecoveryReserved, Active >, NamedState {
    typedef boost::mpl::list <
      boost::statechart::custom_reaction< RemoteRecoveryReserved >,
      boost::statechart::transition< AllRemotesReserved, Recovering >
      > reactions;
    set<pg_shard_t>::const_iterator remote_recovery_reservation_it;
    explicit WaitRemoteRecoveryReserved(my_context ctx);
    boost::statechart::result react(const RemoteRecoveryReserved &evt);
    void exit();
  };

  struct WaitLocalRecoveryReserved : boost::statechart::state< WaitLocalRecoveryReserved, Active >, NamedState {
    typedef boost::mpl::list <
      boost::statechart::transition< LocalRecoveryReserved, WaitRemoteRecoveryReserved >,
      boost::statechart::custom_reaction< RecoveryTooFull >
      > reactions;
    explicit WaitLocalRecoveryReserved(my_context ctx);
    void exit();
    boost::statechart::result react(const RecoveryTooFull &evt);
  };

  struct Activating : boost::statechart::state< Activating, Active >, NamedState {
    typedef boost::mpl::list <
      boost::statechart::transition< AllReplicasRecovered, Recovered >,
      boost::statechart::transition< DoRecovery, WaitLocalRecoveryReserved >,
      boost::statechart::transition< RequestBackfill, WaitLocalBackfillReserved >
      > reactions;
    explicit Activating(my_context ctx);
    void exit();
  };

  struct Stray : boost::statechart::state< Stray, Started >,
            NamedState {
    explicit Stray(my_context ctx);
    void exit();

    typedef boost::mpl::list <
      boost::statechart::custom_reaction< MQuery >,
      boost::statechart::custom_reaction< MLogRec >,
      boost::statechart::custom_reaction< MInfoRec >,
      boost::statechart::custom_reaction< ActMap >,
      boost::statechart::custom_reaction< RecoveryDone >,
      boost::statechart::transition<DeleteStart, ToDelete>
      > reactions;
    boost::statechart::result react(const MQuery& query);
    boost::statechart::result react(const MLogRec& logevt);
    boost::statechart::result react(const MInfoRec& infoevt);
    boost::statechart::result react(const ActMap&);
    boost::statechart::result react(const RecoveryDone&) {
      return discard_event();
    }
  };

  struct WaitDeleteReserved;
  struct ToDelete : boost::statechart::state<ToDelete, Started, WaitDeleteReserved>, NamedState {
    unsigned priority = 0;
    typedef boost::mpl::list <
      boost::statechart::custom_reaction< ActMap >,
      boost::statechart::custom_reaction< DeleteSome >
      > reactions;
    explicit ToDelete(my_context ctx);
    boost::statechart::result react(const ActMap &evt);
    boost::statechart::result react(const DeleteSome &evt) {
      // happens if we drop out of Deleting due to reprioritization etc.
      return discard_event();
    }
    void exit();
  };

  struct Deleting;
  struct WaitDeleteReserved : boost::statechart::state<WaitDeleteReserved,
						 ToDelete>, NamedState {
    typedef boost::mpl::list <
      boost::statechart::transition<DeleteReserved, Deleting>
      > reactions;
    explicit WaitDeleteReserved(my_context ctx);
    void exit();
  };

  struct Deleting : boost::statechart::state<Deleting,
				       ToDelete>, NamedState {
    typedef boost::mpl::list <
      boost::statechart::custom_reaction< DeleteSome >,
      boost::statechart::transition<DeleteInterrupted, WaitDeleteReserved>
      > reactions;
    explicit Deleting(my_context ctx);
    boost::statechart::result react(const DeleteSome &evt);
    void exit();
  };

  struct GetLog;

  struct GetInfo : boost::statechart::state< GetInfo, Peering >, NamedState {
    set<pg_shard_t> peer_info_requested;

    explicit GetInfo(my_context ctx);
    void exit();
    void get_infos();

    typedef boost::mpl::list <
      boost::statechart::custom_reaction< QueryState >,
      boost::statechart::transition< GotInfo, GetLog >,
      boost::statechart::custom_reaction< MNotifyRec >,
      boost::statechart::transition< IsDown, Down >
      > reactions;
    boost::statechart::result react(const QueryState& q);
    boost::statechart::result react(const MNotifyRec& infoevt);
  };

  struct GotLog : boost::statechart::event< GotLog > {
    GotLog() : boost::statechart::event< GotLog >() {}
  };

  struct GetLog : boost::statechart::state< GetLog, Peering >, NamedState {
    pg_shard_t auth_log_shard;
    boost::intrusive_ptr<MOSDPGLog> msg;

    explicit GetLog(my_context ctx);
    void exit();

    typedef boost::mpl::list <
      boost::statechart::custom_reaction< QueryState >,
      boost::statechart::custom_reaction< MLogRec >,
      boost::statechart::custom_reaction< GotLog >,
      boost::statechart::custom_reaction< AdvMap >,
      boost::statechart::transition< IsIncomplete, Incomplete >
      > reactions;
    boost::statechart::result react(const AdvMap&);
    boost::statechart::result react(const QueryState& q);
    boost::statechart::result react(const MLogRec& logevt);
    boost::statechart::result react(const GotLog&);
  };

  struct WaitUpThru;

  struct GetMissing : boost::statechart::state< GetMissing, Peering >, NamedState {
    set<pg_shard_t> peer_missing_requested;

    explicit GetMissing(my_context ctx);
    void exit();

    typedef boost::mpl::list <
      boost::statechart::custom_reaction< QueryState >,
      boost::statechart::custom_reaction< MLogRec >,
      boost::statechart::transition< NeedUpThru, WaitUpThru >
      > reactions;
    boost::statechart::result react(const QueryState& q);
    boost::statechart::result react(const MLogRec& logevt);
  };

  struct WaitUpThru : boost::statechart::state< WaitUpThru, Peering >, NamedState {
    explicit WaitUpThru(my_context ctx);
    void exit();

    typedef boost::mpl::list <
      boost::statechart::custom_reaction< QueryState >,
      boost::statechart::custom_reaction< ActMap >,
      boost::statechart::custom_reaction< MLogRec >
      > reactions;
    boost::statechart::result react(const QueryState& q);
    boost::statechart::result react(const ActMap& am);
    boost::statechart::result react(const MLogRec& logrec);
  };

  struct Down : boost::statechart::state< Down, Peering>, NamedState {
    explicit Down(my_context ctx);
    typedef boost::mpl::list <
      boost::statechart::custom_reaction< QueryState >,
      boost::statechart::custom_reaction< MNotifyRec >
      > reactions;
    boost::statechart::result react(const QueryState& q);
    boost::statechart::result react(const MNotifyRec& infoevt);
    void exit();
  };

  struct Incomplete : boost::statechart::state< Incomplete, Peering>, NamedState {
    typedef boost::mpl::list <
      boost::statechart::custom_reaction< AdvMap >,
      boost::statechart::custom_reaction< MNotifyRec >,
      boost::statechart::custom_reaction< QueryState >
      > reactions;
    explicit Incomplete(my_context ctx);
    boost::statechart::result react(const AdvMap &advmap);
    boost::statechart::result react(const MNotifyRec& infoevt);
    boost::statechart::result react(const QueryState& q);
    void exit();
  };

  PeeringMachine machine;
  CephContext* cct;
  spg_t spgid;
  PG *pg;

  /// context passed in by state machine caller
  PeeringCtx *orig_ctx;

  /// populated if we are buffering messages pending a flush
  boost::optional<BufferedRecoveryMessages> messages_pending_flush;

  /**
   * populated between start_handle() and end_handle(), points into
   * the message lists for messages_pending_flush while blocking messages
   * or into orig_ctx otherwise
   */
  boost::optional<PeeringCtx> rctx;

public:
  explicit PeeringState(CephContext *cct, spg_t spgid, PG *pg)
    : machine(this, cct, spgid, pg), cct(cct), spgid(spgid), pg(pg), orig_ctx(0) {
    machine.initiate();
  }

  void handle_event(const boost::statechart::event_base &evt,
	      PeeringCtx *rctx) {
    start_handle(rctx);
    machine.process_event(evt);
    end_handle();
  }

  void handle_event(PGPeeringEventRef evt,
	      PeeringCtx *rctx) {
    start_handle(rctx);
    machine.process_event(evt->get_event());
    end_handle();
  }

};
