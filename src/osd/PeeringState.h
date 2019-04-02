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
#include <string>
#include <atomic>

#include "PGLog.h"
#include "PGStateUtils.h"
#include "PGPeeringEvent.h"
#include "osd_types.h"
#include "os/ObjectStore.h"
#include "OSDMap.h"
#include "MissingLoc.h"
#include "common/LogClient.h"

class PG;

struct PGPool {
  CephContext* cct;
  epoch_t cached_epoch;
  int64_t id;
  string name;

  pg_pool_t info;
  SnapContext snapc;   // the default pool snapc, ready to go.

  // these two sets are for < mimic only
  interval_set<snapid_t> cached_removed_snaps;      // current removed_snaps set
  interval_set<snapid_t> newly_removed_snaps;  // newly removed in the last epoch

  PGPool(CephContext* cct, OSDMapRef map, int64_t i, const pg_pool_t& info,
	 const string& name)
    : cct(cct),
      cached_epoch(map->get_epoch()),
      id(i),
      name(name),
      info(info) {
    snapc = info.get_snap_context();
    if (map->require_osd_release < CEPH_RELEASE_MIMIC) {
      info.build_removed_snaps(cached_removed_snaps);
    }
  }

  void update(CephContext *cct, OSDMapRef map);
};

  /* Encapsulates PG recovery process */
class PeeringState : public MissingLoc::MappingInfo {
public:
  struct PeeringListener : public EpochSource {
    virtual void prepare_write(
      pg_info_t &info,
      PGLog &pglog,
      bool dirty_info,
      bool dirty_big_info,
      bool need_write_epoch,
      ObjectStore::Transaction &t) = 0;
    virtual void update_heartbeat_peers(set<int> peers) = 0;

    virtual void on_info_history_change() = 0;
    virtual void scrub_requested(bool deep, bool repair) = 0;

    virtual void send_cluster_message(int osd, Message *m, epoch_t epoch) = 0;

    // Flush state
    virtual bool try_flush_or_schedule_async() = 0;
    virtual void start_flush_on_transaction(
      ObjectStore::Transaction *t) = 0;
    virtual void on_flushed() = 0;

    // Recovery
    virtual void schedule_event_after(
      PGPeeringEventRef event,
      float delay) = 0;
    virtual void request_local_background_io_reservation(
      unsigned priority,
      PGPeeringEventRef on_grant,
      PGPeeringEventRef on_preempt) = 0;
    virtual void update_local_background_io_priority(
      unsigned priority) = 0;
    virtual void cancel_local_background_io_reservation() = 0;

    virtual void request_remote_recovery_reservation(
      unsigned priority,
      PGPeeringEventRef on_grant,
      PGPeeringEventRef on_preempt) = 0;
    virtual void cancel_remote_recovery_reservation() = 0;

    virtual void schedule_event_on_commit(
      ObjectStore::Transaction &t,
      PGPeeringEventRef on_commit) = 0;

    // HB
    virtual void set_probe_targets(const set<pg_shard_t> &probe_set) = 0;
    virtual void clear_probe_targets() = 0;

    virtual PerfCounters &get_peering_perf() = 0;

    virtual void clear_ready_to_merge() = 0;

    virtual void queue_want_pg_temp(const vector<int> &wanted) = 0;
    virtual void clear_want_pg_temp() = 0;

    virtual void publish_stats_to_osd() = 0;
    virtual void clear_publish_stats() = 0;

    virtual void check_recovery_sources(const OSDMapRef& newmap) = 0;
    virtual void check_blacklisted_watchers() = 0;
    virtual void clear_primary_state() = 0;

    // Event notification
    virtual void on_pool_change() = 0;
    virtual void on_role_change() = 0;
    virtual void on_change(ObjectStore::Transaction *t) = 0;
    virtual void on_activate(interval_set<snapid_t> to_trim) = 0;
    virtual void on_activate_complete() = 0;
    virtual void on_new_interval() = 0;
    virtual Context *on_clean() = 0;
    virtual void on_activate_committed() = 0;

    virtual void on_active_exit() = 0;

    // active map notifications
    virtual void on_active_actmap() = 0;
    virtual void on_active_advmap(const OSDMapRef &osdmap) = 0;

    // recovery reservation notifications
    virtual void on_backfill_reserved() = 0;
    virtual void on_backfill_canceled() = 0;
    virtual void on_recovery_reserved() = 0;

    // recovery space accounting
    virtual bool try_reserve_recovery_space(
      int64_t primary_num_bytes, int64_t local_num_bytes) = 0;
    virtual void unreserve_recovery_space() = 0;

    // Peering log events
    virtual PGLog::LogEntryHandlerRef get_log_handler(
      ObjectStore::Transaction *t) = 0;

    virtual epoch_t oldest_stored_osdmap() = 0;
    virtual LogChannel &get_clog() = 0;

    virtual ~PeeringListener() {}
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
  struct ActivateCommitted : boost::statechart::event< ActivateCommitted > {
    epoch_t epoch;
    epoch_t activation_epoch;
    explicit ActivateCommitted(epoch_t e, epoch_t ae)
      : boost::statechart::event< ActivateCommitted >(),
	activation_epoch(ae) {}
    void print(std::ostream *out) const {
      *out << "ActivateCommitted from " << activation_epoch
	   << " processed at " << epoch;
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
  public:
    PeeringState *state;
    PGStateHistory *state_history;
    CephContext *cct;
    spg_t spgid;
    DoutPrefixProvider *dpp;
    PeeringListener *pl;
    PG *pg;

    utime_t event_time;
    uint64_t event_count;

    void clear_event_counters() {
      event_time = utime_t();
      event_count = 0;
    }

    void log_enter(const char *state_name);
    void log_exit(const char *state_name, utime_t duration);

    PeeringMachine(
      PeeringState *state, CephContext *cct,
      spg_t spgid,
      DoutPrefixProvider *dpp,
      PeeringListener *pl,
      PG *pg,
      PGStateHistory *state_history) :
      state(state),
      state_history(state_history),
      cct(cct), spgid(spgid),
      dpp(dpp), pl(pl),
      pg(pg), event_count(0) {}

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
      boost::statechart::custom_reaction< ActivateCommitted >,
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
    boost::statechart::result react(const ActivateCommitted&);
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
    void all_activated_and_committed();
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
      boost::statechart::custom_reaction< ActivateCommitted >,
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
    boost::statechart::result react(const ActivateCommitted&);
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

  PGStateHistory state_history;
  PeeringMachine machine;
  CephContext* cct;
  spg_t spgid;
  DoutPrefixProvider *dpp;
  PeeringListener *pl;
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
  /**
   * OSDMap state
   */
  OSDMapRef osdmap_ref;              ///< Reference to current OSDMap
  PGPool pool;                       ///< Current pool state
  epoch_t last_persisted_osdmap = 0; ///< Last osdmap epoch persisted


  /**
   * Peering state information
   */
  int role = -1;             ///< 0 = primary, 1 = replica, -1=none.
  uint64_t state = 0;        ///< PG_STATE_*

  pg_shard_t primary;        ///< id/shard of primary
  pg_shard_t pg_whoami;      ///< my id/shard
  pg_shard_t up_primary;     ///< id/shard of primary of up set
  vector<int> up;            ///< crush mapping without temp pgs
  set<pg_shard_t> upset;     ///< up in set form
  vector<int> acting;        ///< actual acting set for the current interval
  set<pg_shard_t> actingset; ///< acting in set form

  /// union of acting, recovery, and backfill targets
  set<pg_shard_t> acting_recovery_backfill;

  bool send_notify = false; ///< True if a notify needs to be sent to the primary

  bool dirty_info = false;          ///< small info structu on disk out of date
  bool dirty_big_info = false;      ///< big info structure on disk out of date

  pg_info_t info;                   ///< current pg info
  pg_info_t last_written_info;      ///< last written info
  PastIntervals past_intervals;     ///< information about prior pg mappings
  PGLog  pg_log;                    ///< pg log

  epoch_t last_peering_reset = 0;   ///< epoch of last peering reset

  /// last_update that has committed; ONLY DEFINED WHEN is_active()
  eversion_t  last_update_ondisk;
  eversion_t  last_complete_ondisk; ///< last_complete that has committed.
  eversion_t  last_update_applied;  ///< last_update readable
  /// last version to which rollback_info trimming has been applied
  eversion_t  last_rollback_info_trimmed_to_applied;

  /// Counter to determine when pending flushes have completed
  unsigned flushes_in_progress = 0;

  /**
   * Primary state
   */
  set<pg_shard_t>    stray_set; ///< non-acting osds that have PG data.
  map<pg_shard_t, pg_info_t>    peer_info; ///< info from peers (stray or prior)
  map<pg_shard_t, int64_t>    peer_bytes; ///< Peer's num_bytes from peer_info
  set<pg_shard_t> peer_purged; ///< peers purged
  map<pg_shard_t, pg_missing_t> peer_missing; ///< peer missing sets
  set<pg_shard_t> peer_log_requested; ///< logs i've requested (and start stamps)
  set<pg_shard_t> peer_missing_requested; ///< missing sets requested

  /// features supported by all peers
  uint64_t peer_features = CEPH_FEATURES_SUPPORTED_DEFAULT;
  /// features supported by acting set
  uint64_t acting_features = CEPH_FEATURES_SUPPORTED_DEFAULT;
  /// features supported by up and acting
  uint64_t upacting_features = CEPH_FEATURES_SUPPORTED_DEFAULT;

  /// most recently consumed osdmap's require_osd_version
  unsigned last_require_osd_release = 0;

  vector<int> want_acting; ///< non-empty while peering needs a new acting set

  // acting_recovery_backfill contains shards that are acting,
  // async recovery targets, or backfill targets.
  map<pg_shard_t,eversion_t> peer_last_complete_ondisk;

  /// up: min over last_complete_ondisk, peer_last_complete_ondisk
  eversion_t  min_last_complete_ondisk;
  /// point to which the log should be trimmed
  eversion_t  pg_trim_to;

  set<int> blocked_by; ///< osds we are blocked by (for pg stats)

  bool need_up_thru = false; ///< true if osdmap with updated up_thru needed

  /// I deleted these strays; ignore racing PGInfo from them
  set<pg_shard_t> peer_activated;

  set<pg_shard_t> backfill_targets;       ///< osds to be backfilled
  set<pg_shard_t> async_recovery_targets; ///< osds to be async recovered

  /// osds which might have objects on them which are unfound on the primary
  set<pg_shard_t> might_have_unfound;

  bool deleting = false;  /// true while in removing or OSD is shutting down
  atomic<bool> deleted = {false}; /// true once deletion complete

  MissingLoc missing_loc; ///< information about missing objects

  bool backfill_reserved = false;
  bool backfill_reserving = false;

  void update_osdmap_ref(OSDMapRef newmap) {
    osdmap_ref = std::move(newmap);
  }

  void write_if_dirty(ObjectStore::Transaction& t);

  void update_heartbeat_peers();
  bool proc_replica_info(
    pg_shard_t from, const pg_info_t &oinfo, epoch_t send_epoch);
  void remove_down_peer_info(const OSDMapRef &osdmap);
  void purge_strays();
  void update_history(const pg_history_t& new_history);
  void check_recovery_sources(const OSDMapRef& map);
  void set_last_peering_reset();
  void check_full_transition(OSDMapRef lastmap, OSDMapRef osdmap);
  bool should_restart_peering(
    int newupprimary,
    int newactingprimary,
    const vector<int>& newup,
    const vector<int>& newacting,
    OSDMapRef lastmap,
    OSDMapRef osdmap);
  void start_peering_interval(
    const OSDMapRef lastmap,
    const vector<int>& newup, int up_primary,
    const vector<int>& newacting, int acting_primary,
    ObjectStore::Transaction *t);
  void on_new_interval();
  void init_primary_up_acting(
    const vector<int> &newup,
    const vector<int> &newacting,
    int new_up_primary,
    int new_acting_primary);
  void clear_primary_state();
  void check_past_interval_bounds() const;
  bool set_force_recovery(bool b);
  bool set_force_backfill(bool b);

  /// clip calculated priority to reasonable range
  inline int clamp_recovery_priority(int priority);
  /// get log recovery reservation priority
  unsigned get_recovery_priority();
  /// get backfill reservation priority
  unsigned get_backfill_priority();
  /// get priority for pg deletion
  unsigned get_delete_priority();

  bool adjust_need_up_thru(const OSDMapRef osdmap);
  PastIntervals::PriorSet build_prior();

  void reject_reservation();

  // acting set
  map<pg_shard_t, pg_info_t>::const_iterator find_best_info(
    const map<pg_shard_t, pg_info_t> &infos,
    bool restrict_to_up_acting,
    bool *history_les_bound) const;
  static void calc_ec_acting(
    map<pg_shard_t, pg_info_t>::const_iterator auth_log_shard,
    unsigned size,
    const vector<int> &acting,
    const vector<int> &up,
    const map<pg_shard_t, pg_info_t> &all_info,
    bool restrict_to_up_acting,
    vector<int> *want,
    set<pg_shard_t> *backfill,
    set<pg_shard_t> *acting_backfill,
    ostream &ss);
  static void calc_replicated_acting(
    map<pg_shard_t, pg_info_t>::const_iterator auth_log_shard,
    uint64_t force_auth_primary_missing_objects,
    unsigned size,
    const vector<int> &acting,
    const vector<int> &up,
    pg_shard_t up_primary,
    const map<pg_shard_t, pg_info_t> &all_info,
    bool restrict_to_up_acting,
    vector<int> *want,
    set<pg_shard_t> *backfill,
    set<pg_shard_t> *acting_backfill,
    const OSDMapRef osdmap,
    ostream &ss);
  void choose_async_recovery_ec(const map<pg_shard_t, pg_info_t> &all_info,
                                const pg_info_t &auth_info,
                                vector<int> *want,
                                set<pg_shard_t> *async_recovery) const;
  void choose_async_recovery_replicated(const map<pg_shard_t, pg_info_t> &all_info,
                                        const pg_info_t &auth_info,
                                        vector<int> *want,
                                        set<pg_shard_t> *async_recovery) const;

  bool recoverable_and_ge_min_size(const vector<int> &want) const;
  bool choose_acting(pg_shard_t &auth_log_shard,
		     bool restrict_to_up_acting,
		     bool *history_les_bound);

  bool search_for_missing(
    const pg_info_t &oinfo, const pg_missing_t &omissing,
    pg_shard_t fromosd,
    PeeringCtx*);
  void discover_all_missing(std::map<int, map<spg_t,pg_query_t> > &query_map);
  void build_might_have_unfound();
  void log_weirdness();
  void activate(
    ObjectStore::Transaction& t,
    epoch_t activation_epoch,
    map<int, map<spg_t,pg_query_t> >& query_map,
    map<int, vector<pair<pg_notify_t, PastIntervals> > > *activator_map,
    PeeringCtx *ctx);
  void share_pg_info();

  void rewind_divergent_log(ObjectStore::Transaction& t, eversion_t newhead);
  void merge_log(
    ObjectStore::Transaction& t, pg_info_t &oinfo,
    pg_log_t &olog, pg_shard_t from);

  void proc_primary_info(ObjectStore::Transaction &t, const pg_info_t &info);
  void proc_master_log(ObjectStore::Transaction& t, pg_info_t &oinfo,
		       pg_log_t &olog, pg_missing_t& omissing,
		       pg_shard_t from);
  void proc_replica_log(pg_info_t &oinfo, const pg_log_t &olog,
			pg_missing_t& omissing, pg_shard_t from);

public:
  PeeringState(
    CephContext *cct,
    spg_t spgid,
    const PGPool &pool,
    OSDMapRef curmap,
    DoutPrefixProvider *dpp,
    PeeringListener *pl,
    PG *pg);

  void set_backend_predicates(
    IsPGReadablePredicate *is_readable,
    IsPGRecoverablePredicate *is_recoverable) {
    missing_loc.set_backend_predicates(is_readable, is_recoverable);
  }

  // MissingLoc::MappingInfo
  const set<pg_shard_t> &get_upset() const override {
    return upset;
  }

  bool is_ec_pg() const override {
    return pool.info.is_erasure();
  }

  int get_pg_size() const override {
    return pool.info.size;
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

  void dump_history(Formatter *f) const {
    state_history.dump(f);
  }

  const char *get_current_state() const {
    return state_history.get_current_state();
  }

  epoch_t get_last_peering_reset() const {
    return last_peering_reset;
  }

  /// Returns stable reference to internal pool structure
  const PGPool &get_pool() const {
    return pool;
  }

  /// Returns reference to current osdmap
  const OSDMapRef &get_osdmap() const {
    ceph_assert(osdmap_ref);
    return osdmap_ref;
  }

  /// Returns epoch of current osdmap
  epoch_t get_osdmap_epoch() const {
    return get_osdmap()->get_epoch();
  }

  /// Updates peering state with new map
  void advance_map(
    OSDMapRef osdmap,       ///< [in] new osdmap
    OSDMapRef lastmap,      ///< [in] prev osdmap
    vector<int>& newup,     ///< [in] new up set
    int up_primary,         ///< [in] new up primary
    vector<int>& newacting, ///< [in] new acting
    int acting_primary,     ///< [in] new acting primary
    PeeringCtx *rctx        ///< [out] recovery context
    );

  /// Activates most recently updated map
  void activate_map(
    PeeringCtx *rctx        ///< [out] recovery context
    );

  /// resets last_persisted_osdmap
  void reset_last_persisted() {
    last_persisted_osdmap = 0;
  }

  template <typename Func>
  void adjust_purged_snaps(Func f) {
    f(info.purged_snaps);
    dirty_info = true;
    dirty_big_info = true;
  }

  bool is_deleting() const {
    return deleting;
  }
  bool is_deleted() const {
    return deleted;
  }
  bool is_acting_recovery_backfill(pg_shard_t osd) const {
    return acting_recovery_backfill.count(osd);
  }
  bool is_acting(pg_shard_t osd) const {
    return has_shard(pool.info.is_erasure(), acting, osd);
  }
  bool is_up(pg_shard_t osd) const {
    return has_shard(pool.info.is_erasure(), up, osd);
  }
  static bool has_shard(bool ec, const vector<int>& v, pg_shard_t osd) {
    if (ec) {
      return v.size() > (unsigned)osd.shard && v[osd.shard] == osd.osd;
    } else {
      return std::find(v.begin(), v.end(), osd.osd) != v.end();
    }
  }
  const PastIntervals& get_past_intervals() const {
    return past_intervals;
  }
  bool is_replica() const {
    return role > 0;
  }
  bool is_primary() const {
    return pg_whoami == primary;
  }
  bool pg_has_reset_since(epoch_t e) {
    return deleted || e < get_last_peering_reset();
  }

  void set_role(int r) {
    role = r;
  }
  int get_role() const {
    return role;
  }
  const vector<int> get_acting() const {
    return acting;
  }
  int get_acting_primary() const {
    return primary.osd;
  }
  pg_shard_t get_primary() const {
    return primary;
  }
  const vector<int> &get_up() const {
    return up;
  }
  int get_up_primary() const {
    return up_primary.osd;
  }

  bool state_test(uint64_t m) const { return (state & m) != 0; }
  void state_set(uint64_t m) { state |= m; }
  void state_clear(uint64_t m) { state &= ~m; }

  bool is_complete() const { return info.last_complete == info.last_update; }
  bool should_send_notify() const { return send_notify; }

  int get_state() const { return state; }
  bool is_active() const { return state_test(PG_STATE_ACTIVE); }
  bool is_activating() const { return state_test(PG_STATE_ACTIVATING); }
  bool is_peering() const { return state_test(PG_STATE_PEERING); }
  bool is_down() const { return state_test(PG_STATE_DOWN); }
  bool is_incomplete() const { return state_test(PG_STATE_INCOMPLETE); }
  bool is_clean() const { return state_test(PG_STATE_CLEAN); }
  bool is_degraded() const { return state_test(PG_STATE_DEGRADED); }
  bool is_undersized() const { return state_test(PG_STATE_UNDERSIZED); }
  bool is_remapped() const { return state_test(PG_STATE_REMAPPED); }
  bool is_peered() const {
    return state_test(PG_STATE_ACTIVE) || state_test(PG_STATE_PEERED);
  }
  bool is_premerge() const { return state_test(PG_STATE_PREMERGE); }
  bool is_repair() const { return state_test(PG_STATE_REPAIR); }
  bool is_empty() const { return info.last_update == eversion_t(0,0); }

  bool get_need_up_thru() const {
    return need_up_thru;
  }

  bool is_forced_recovery_or_backfill() const {
    return get_state() & (PG_STATE_FORCED_RECOVERY | PG_STATE_FORCED_BACKFILL);
  }

  bool is_backfill_reserved() const {
    return backfill_reserved;
  }

  bool is_backfill_reserving() const {
    return backfill_reserving;
  }

  unsigned get_last_require_osd_release() const {
    return last_require_osd_release;
  }

  const pg_info_t &get_info() const {
    return info;
  }

  bool needs_recovery() const;
  bool needs_backfill() const;
  bool all_unfound_are_queried_or_lost(const OSDMapRef osdmap) const;
  bool all_missing_unfound() const {
    const auto& missing = pg_log.get_missing();
    if (!missing.have_missing())
      return false;
    for (auto& m : missing.get_items()) {
      if (!missing_loc.is_unfound(m.first))
        return false;
    }
    return true;
  }

  bool perform_deletes_during_peering() const {
    return !(get_osdmap()->test_flag(CEPH_OSDMAP_RECOVERY_DELETES));
  }


  bool have_unfound() const {
    return missing_loc.have_unfound();
  }
  uint64_t get_num_unfound() const {
    return missing_loc.num_unfound();
  }

  // Flush control interface
private:
  void start_flush(ObjectStore::Transaction *t) {
    flushes_in_progress++;
    pl->start_flush_on_transaction(t);
  }
public:
  bool needs_flush() const {
    return flushes_in_progress > 0;
  }
  void complete_flush();
};
