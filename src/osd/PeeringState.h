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

#include "include/ceph_assert.h"

#include "PGLog.h"
#include "PGStateUtils.h"
#include "PGPeeringEvent.h"
#include "osd_types.h"
#include "os/ObjectStore.h"
#include "OSDMap.h"
#include "MissingLoc.h"
#include "osd/osd_perf_counters.h"
#include "common/ostream_temp.h"

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
  }

  void update(CephContext *cct, OSDMapRef map);
};

class PeeringCtx;

// [primary only] content recovery state
struct BufferedRecoveryMessages {
  map<int, map<spg_t, pg_query_t> > query_map;
  map<int, vector<pair<pg_notify_t, PastIntervals> > > info_map;
  map<int, vector<pair<pg_notify_t, PastIntervals> > > notify_list;

  BufferedRecoveryMessages() = default;
  BufferedRecoveryMessages(PeeringCtx &);

  void accept_buffered_messages(BufferedRecoveryMessages &m) {
    for (auto &[target, qmap] : m.query_map) {
      auto &omap = query_map[target];
      for (auto &[pg, query] : qmap) {
	omap[pg] = query;
      }
    }
    for (auto &[target, ivec] : m.info_map) {
      auto &ovec = info_map[target];
      ovec.reserve(ovec.size() + ivec.size());
      ovec.insert(ovec.end(), ivec.begin(), ivec.end());
    }
    for (auto &[target, nlist] : m.notify_list) {
      auto &ovec = notify_list[target];
      ovec.reserve(ovec.size() + nlist.size());
      ovec.insert(ovec.end(), nlist.begin(), nlist.end());
    }
  }
};

struct PeeringCtx : BufferedRecoveryMessages {
  ObjectStore::Transaction transaction;
  HBHandle* handle = nullptr;

  PeeringCtx() = default;

  PeeringCtx(const PeeringCtx &) = delete;
  PeeringCtx &operator=(const PeeringCtx &) = delete;

  PeeringCtx(PeeringCtx &&) = default;
  PeeringCtx &operator=(PeeringCtx &&) = default;

  void reset_transaction() {
    transaction = ObjectStore::Transaction();
  }
};

  /* Encapsulates PG recovery process */
class PeeringState : public MissingLoc::MappingInfo {
public:
  struct PeeringListener : public EpochSource {
    /// Prepare t with written information
    virtual void prepare_write(
      pg_info_t &info,
      pg_info_t &last_written_info,
      PastIntervals &past_intervals,
      PGLog &pglog,
      bool dirty_info,
      bool dirty_big_info,
      bool need_write_epoch,
      ObjectStore::Transaction &t) = 0;

    /// Notify that info/history changed (generally to update scrub registration)
    virtual void on_info_history_change() = 0;
    /// Notify that a scrub has been requested
    virtual void scrub_requested(bool deep, bool repair) = 0;

    /// Return current snap_trimq size
    virtual uint64_t get_snap_trimq_size() const = 0;

    /// Send cluster message to osd
    virtual void send_cluster_message(
      int osd, Message *m, epoch_t epoch, bool share_map_update=false) = 0;
    /// Send pg_created to mon
    virtual void send_pg_created(pg_t pgid) = 0;

    // ============ Flush state ==================
    /**
     * try_flush_or_schedule_async()
     *
     * If true, caller may assume all past operations on this pg
     * have been flushed.  Else, caller will receive an on_flushed()
     * call once the flush has completed.
     */
    virtual bool try_flush_or_schedule_async() = 0;
    /// Arranges for a commit on t to call on_flushed() once flushed.
    virtual void start_flush_on_transaction(
      ObjectStore::Transaction &t) = 0;
    /// Notification that all outstanding flushes for interval have completed
    virtual void on_flushed() = 0;

    //============= Recovery ====================
    /// Arrange for even to be queued after delay
    virtual void schedule_event_after(
      PGPeeringEventRef event,
      float delay) = 0;
    /**
     * request_local_background_io_reservation
     *
     * Request reservation at priority with on_grant queued on grant
     * and on_preempt on preempt
     */
    virtual void request_local_background_io_reservation(
      unsigned priority,
      PGPeeringEventRef on_grant,
      PGPeeringEventRef on_preempt) = 0;
    /// Modify pending local background reservation request priority
    virtual void update_local_background_io_priority(
      unsigned priority) = 0;
    /// Cancel pending local background reservation request
    virtual void cancel_local_background_io_reservation() = 0;

    /**
     * request_remote_background_io_reservation
     *
     * Request reservation at priority with on_grant queued on grant
     * and on_preempt on preempt
     */
    virtual void request_remote_recovery_reservation(
      unsigned priority,
      PGPeeringEventRef on_grant,
      PGPeeringEventRef on_preempt) = 0;
    /// Cancel pending remote background reservation request
    virtual void cancel_remote_recovery_reservation() = 0;

    /// Arrange for on_commit to be queued upon commit of t
    virtual void schedule_event_on_commit(
      ObjectStore::Transaction &t,
      PGPeeringEventRef on_commit) = 0;

    //============================ HB =============================
    /// Update hb set to peers
    virtual void update_heartbeat_peers(set<int> peers) = 0;

    /// Set targets being probed in this interval
    virtual void set_probe_targets(const set<pg_shard_t> &probe_set) = 0;
    /// Clear targets being probed in this interval
    virtual void clear_probe_targets() = 0;

    /// Queue for a pg_temp of wanted
    virtual void queue_want_pg_temp(const vector<int> &wanted) = 0;
    /// Clear queue for a pg_temp of wanted
    virtual void clear_want_pg_temp() = 0;

    /// Arrange for stats to be shipped to mon to be updated for this pg
    virtual void publish_stats_to_osd() = 0;
    /// Clear stats to be shipped to mon for this pg
    virtual void clear_publish_stats() = 0;

    /// Notification to check outstanding operation targets
    virtual void check_recovery_sources(const OSDMapRef& newmap) = 0;
    /// Notification to check outstanding blacklist
    virtual void check_blacklisted_watchers() = 0;
    /// Notification to clear state associated with primary
    virtual void clear_primary_state() = 0;

    // =================== Event notification ====================
    virtual void on_pool_change() = 0;
    virtual void on_role_change() = 0;
    virtual void on_change(ObjectStore::Transaction &t) = 0;
    virtual void on_activate(interval_set<snapid_t> to_trim) = 0;
    virtual void on_activate_complete() = 0;
    virtual void on_new_interval() = 0;
    virtual Context *on_clean() = 0;
    virtual void on_activate_committed() = 0;
    virtual void on_active_exit() = 0;

    // ====================== PG deletion =======================
    /// Notification of removal complete, t must be populated to complete removal
    virtual void on_removal(ObjectStore::Transaction &t) = 0;
    /// Perform incremental removal work
    virtual void do_delete_work(ObjectStore::Transaction &t) = 0;

    // ======================= PG Merge =========================
    virtual void clear_ready_to_merge() = 0;
    virtual void set_not_ready_to_merge_target(pg_t pgid, pg_t src) = 0;
    virtual void set_not_ready_to_merge_source(pg_t pgid) = 0;
    virtual void set_ready_to_merge_target(eversion_t lu, epoch_t les, epoch_t lec) = 0;
    virtual void set_ready_to_merge_source(eversion_t lu) = 0;

    // ==================== Map notifications ===================
    virtual void on_active_actmap() = 0;
    virtual void on_active_advmap(const OSDMapRef &osdmap) = 0;
    virtual epoch_t oldest_stored_osdmap() = 0;

    // ============ recovery reservation notifications ==========
    virtual void on_backfill_reserved() = 0;
    virtual void on_backfill_canceled() = 0;
    virtual void on_recovery_reserved() = 0;

    // ================recovery space accounting ================
    virtual bool try_reserve_recovery_space(
      int64_t primary_num_bytes, int64_t local_num_bytes) = 0;
    virtual void unreserve_recovery_space() = 0;

    // ================== Peering log events ====================
    /// Get handler for rolling forward/back log entries
    virtual PGLog::LogEntryHandlerRef get_log_handler(
      ObjectStore::Transaction &t) = 0;

    // ============ On disk representation changes ==============
    virtual void rebuild_missing_set_with_deletes(PGLog &pglog) = 0;

    // ======================= Logging ==========================
    virtual PerfCounters &get_peering_perf() = 0;
    virtual PerfCounters &get_perf_logger() = 0;
    virtual void log_state_enter(const char *state) = 0;
    virtual void log_state_exit(
      const char *state_name, utime_t enter_time,
      uint64_t events, utime_t event_dur) = 0;
    virtual void dump_recovery_info(Formatter *f) const = 0;

    virtual OstreamTemp get_clog_info() = 0;
    virtual OstreamTemp get_clog_error() = 0;
    virtual OstreamTemp get_clog_debug() = 0;

    virtual ~PeeringListener() {}
  };

private:
  /**
   * Wraps PeeringCtx to hide the difference between buffering messages to
   * be sent after flush or immediately.
   */
  struct PeeringCtxWrapper {
    utime_t start_time;
    map<int, map<spg_t, pg_query_t> > &query_map;
    map<int, vector<pair<pg_notify_t, PastIntervals> > > &info_map;
    map<int, vector<pair<pg_notify_t, PastIntervals> > > &notify_list;
    ObjectStore::Transaction &transaction;
    HBHandle * const handle = nullptr;

    PeeringCtxWrapper(PeeringCtx &wrapped) :
      query_map(wrapped.query_map),
      info_map(wrapped.info_map),
      notify_list(wrapped.notify_list),
      transaction(wrapped.transaction),
      handle(wrapped.handle) {}

    PeeringCtxWrapper(BufferedRecoveryMessages &buf, PeeringCtx &wrapped)
      : query_map(buf.query_map),
	info_map(buf.info_map),
	notify_list(buf.notify_list),
	transaction(wrapped.transaction),
        handle(wrapped.handle) {}

    PeeringCtxWrapper(PeeringCtxWrapper &&ctx) = default;

    void send_notify(pg_shard_t to,
		     const pg_notify_t &info, const PastIntervals &pi) {
      notify_list[to.osd].emplace_back(info, pi);
    }
  };
public:

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
	epoch(e),
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
      PGStateHistory *state_history) :
      state(state),
      state_history(state_history),
      cct(cct), spgid(spgid),
      dpp(dpp), pl(pl),
      event_count(0) {}

    /* Accessor functions for state methods */
    ObjectStore::Transaction& get_cur_transaction() {
      ceph_assert(state->rctx);
      return state->rctx->transaction;
    }

    void send_query(pg_shard_t to, const pg_query_t &query);

    map<int, map<spg_t, pg_query_t> > &get_query_map() {
      ceph_assert(state->rctx);
      return state->rctx->query_map;
    }

    map<int, vector<pair<pg_notify_t, PastIntervals> > > &get_info_map() {
      ceph_assert(state->rctx);
      return state->rctx->info_map;
    }

    PeeringCtxWrapper &get_recovery_ctx() {
      assert(state->rctx);
      return *(state->rctx);
    }

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
      boost::statechart::custom_reaction< ActivateCommitted >,
      boost::statechart::custom_reaction< DeleteSome >
      > reactions;
    explicit ToDelete(my_context ctx);
    boost::statechart::result react(const ActMap &evt);
    boost::statechart::result react(const DeleteSome &evt) {
      // happens if we drop out of Deleting due to reprioritization etc.
      return discard_event();
    }
    boost::statechart::result react(const ActivateCommitted&) {
      // Can happens if we were activated as a stray but not actually pulled
      // from prior to the pg going clean and sending a delete.
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
  CephContext* cct;
  spg_t spgid;
  DoutPrefixProvider *dpp;
  PeeringListener *pl;

  /// context passed in by state machine caller
  PeeringCtx *orig_ctx;

  /// populated if we are buffering messages pending a flush
  std::optional<BufferedRecoveryMessages> messages_pending_flush;

  /**
   * populated between start_handle() and end_handle(), points into
   * the message lists for messages_pending_flush while blocking messages
   * or into orig_ctx otherwise
   */
  std::optional<PeeringCtxWrapper> rctx;

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
  ceph_release_t last_require_osd_release = ceph_release_t::unknown;

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

  PeeringMachine machine;

  void update_osdmap_ref(OSDMapRef newmap) {
    osdmap_ref = std::move(newmap);
  }

  void update_heartbeat_peers();
  bool proc_replica_info(
    pg_shard_t from, const pg_info_t &oinfo, epoch_t send_epoch);
  void remove_down_peer_info(const OSDMapRef &osdmap);
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
    ObjectStore::Transaction &t);
  void on_new_interval();
  void clear_recovery_state();
  void clear_primary_state();
  void check_past_interval_bounds() const;
  bool set_force_recovery(bool b);
  bool set_force_backfill(bool b);

  /// clip calculated priority to reasonable range
  int clamp_recovery_priority(int prio, int pool_recovery_prio, int max);
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
  void choose_async_recovery_ec(
    const map<pg_shard_t, pg_info_t> &all_info,
    const pg_info_t &auth_info,
    vector<int> *want,
    set<pg_shard_t> *async_recovery,
    const OSDMapRef osdmap) const;
  void choose_async_recovery_replicated(
    const map<pg_shard_t, pg_info_t> &all_info,
    const pg_info_t &auth_info,
    vector<int> *want,
    set<pg_shard_t> *async_recovery,
    const OSDMapRef osdmap) const;

  bool recoverable(const vector<int> &want) const;
  bool choose_acting(pg_shard_t &auth_log_shard,
		     bool restrict_to_up_acting,
		     bool *history_les_bound);

  bool search_for_missing(
    const pg_info_t &oinfo, const pg_missing_t &omissing,
    pg_shard_t fromosd,
    PeeringCtxWrapper &rctx);
  void build_might_have_unfound();
  void log_weirdness();
  void activate(
    ObjectStore::Transaction& t,
    epoch_t activation_epoch,
    map<int, map<spg_t,pg_query_t> >& query_map,
    map<int, vector<pair<pg_notify_t, PastIntervals> > > *activator_map,
    PeeringCtxWrapper &ctx);

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

  void calc_min_last_complete_ondisk() {
    eversion_t min = last_complete_ondisk;
    ceph_assert(!acting_recovery_backfill.empty());
    for (set<pg_shard_t>::iterator i = acting_recovery_backfill.begin();
	 i != acting_recovery_backfill.end();
	 ++i) {
      if (*i == get_primary()) continue;
      if (peer_last_complete_ondisk.count(*i) == 0)
	return;   // we don't have complete info
      eversion_t a = peer_last_complete_ondisk[*i];
      if (a < min)
	min = a;
    }
    if (min == min_last_complete_ondisk)
      return;
    min_last_complete_ondisk = min;
    return;
  }

  void fulfill_info(
    pg_shard_t from, const pg_query_t &query,
    pair<pg_shard_t, pg_info_t> &notify_info);
  void fulfill_log(
    pg_shard_t from, const pg_query_t &query, epoch_t query_epoch);
  void fulfill_query(const MQuery& q, PeeringCtxWrapper &rctx);

  void try_mark_clean();

  void update_blocked_by();
  void update_calc_stats();

  void add_log_entry(const pg_log_entry_t& e, bool applied);

  void calc_trim_to();
  void calc_trim_to_aggressive();

public:
  PeeringState(
    CephContext *cct,
    pg_shard_t pg_whoami,
    spg_t spgid,
    const PGPool &pool,
    OSDMapRef curmap,
    DoutPrefixProvider *dpp,
    PeeringListener *pl);

  /// Process evt
  void handle_event(const boost::statechart::event_base &evt,
		    PeeringCtx *rctx) {
    start_handle(rctx);
    machine.process_event(evt);
    end_handle();
  }

  /// Process evt
  void handle_event(PGPeeringEventRef evt,
		    PeeringCtx *rctx) {
    start_handle(rctx);
    machine.process_event(evt->get_event());
    end_handle();
  }

  /// Init fresh instance of PG
  void init(
    int role,
    const vector<int>& newup, int new_up_primary,
    const vector<int>& newacting, int new_acting_primary,
    const pg_history_t& history,
    const PastIntervals& pi,
    bool backfill,
    ObjectStore::Transaction &t);

  /// Init pg instance from disk state
  template <typename F>
  auto init_from_disk_state(
    pg_info_t &&info_from_disk,
    PastIntervals &&past_intervals_from_disk,
    F &&pg_log_init) {
    info = std::move(info_from_disk);
    last_written_info = info;
    past_intervals = std::move(past_intervals_from_disk);
    auto ret = pg_log_init(pg_log);
    log_weirdness();
    return ret;
  }

  /// Set initial primary/acting
  void init_primary_up_acting(
    const vector<int> &newup,
    const vector<int> &newacting,
    int new_up_primary,
    int new_acting_primary);

  /// Set initial role
  void set_role(int r) {
    role = r;
  }

  /// Set predicates used for determining readable and recoverable
  void set_backend_predicates(
    IsPGReadablePredicate *is_readable,
    IsPGRecoverablePredicate *is_recoverable) {
    missing_loc.set_backend_predicates(is_readable, is_recoverable);
  }

  /// Send current pg_info to peers
  void share_pg_info();

  /// Get stats for child pgs
  void start_split_stats(
    const set<spg_t>& childpgs, vector<object_stat_sum_t> *out);

  /// Update new child with stats
  void finish_split_stats(
    const object_stat_sum_t& stats, ObjectStore::Transaction &t);

  /// Split state for child_pgid into *child
  void split_into(
    pg_t child_pgid, PeeringState *child, unsigned split_bits);

  /// Merge state from sources
  void merge_from(
    map<spg_t,PeeringState *>& sources,
    PeeringCtx &rctx,
    unsigned split_bits,
    const pg_merge_meta_t& last_pg_merge_meta);

  /// Permit stray replicas to purge now unnecessary state
  void purge_strays();

  /**
   * update_stats
   *
   * Mechanism for updating stats and/or history.  Pass t to mark
   * dirty and write out.  Return true if stats should be published
   * to the osd.
   */
  void update_stats(
    std::function<bool(pg_history_t &, pg_stat_t &)> f,
    ObjectStore::Transaction *t = nullptr);

  /**
   * adjust_purged_snaps
   *
   * Mechanism for updating purged_snaps.  Marks dirty_info, big_dirty_info.
   */
  void adjust_purged_snaps(
    std::function<void(interval_set<snapid_t> &snaps)> f);

  /// Updates info.hit_set to hset_history, does not dirty
  void update_hset(const pg_hit_set_history_t &hset_history);

  /**
   * update_history
   *
   * Merges new_history into info.history clearing past_intervals and
   * dirtying as needed.
   *
   * Calls PeeringListener::on_info_history_change()
   */
  void update_history(const pg_history_t& new_history);

  /**
   * prepare_stats_for_publish
   *
   * Returns updated pg_stat_t if stats have changed since
   * pg_stats_publish adding in unstable_stats.
   */
  std::optional<pg_stat_t> prepare_stats_for_publish(
    bool pg_stats_publish_valid,
    const pg_stat_t &pg_stats_publish,
    const object_stat_collection_t &unstable_stats);

  /**
   * Merge entries updating missing as necessary on all
   * acting_recovery_backfill logs and missings (also missing_loc)
   */
  bool append_log_entries_update_missing(
    const mempool::osd_pglog::list<pg_log_entry_t> &entries,
    ObjectStore::Transaction &t,
    std::optional<eversion_t> trim_to,
    std::optional<eversion_t> roll_forward_to);

  /**
   * Updates local log to reflect new write from primary.
   */
  void append_log(
    const vector<pg_log_entry_t>& logv,
    eversion_t trim_to,
    eversion_t roll_forward_to,
    ObjectStore::Transaction &t,
    bool transaction_applied,
    bool async);

  /**
   * Updates local log/missing to reflect new oob log update from primary
   */
  void merge_new_log_entries(
    const mempool::osd_pglog::list<pg_log_entry_t> &entries,
    ObjectStore::Transaction &t,
    std::optional<eversion_t> trim_to,
    std::optional<eversion_t> roll_forward_to);

  /// Update missing set to reflect e (TODOSAM: not sure why this is needed)
  void add_local_next_event(const pg_log_entry_t& e) {
    pg_log.missing_add_next_entry(e);
  }

  /// Update log trim boundary
  void update_trim_to() {
    bool hard_limit = (get_osdmap()->test_flag(CEPH_OSDMAP_PGLOG_HARDLIMIT));
    if (hard_limit)
      calc_trim_to_aggressive();
    else
      calc_trim_to();
  }

  /// Pre-process pending update on hoid represented by logv
  void pre_submit_op(
    const hobject_t &hoid,
    const vector<pg_log_entry_t>& logv,
    eversion_t at_version);

  /// Signal that oid has been locally recovered to version v
  void recover_got(
    const hobject_t &oid, eversion_t v,
    bool is_delete,
    ObjectStore::Transaction &t);

  /// Signal that oid has been recovered on peer to version
  void on_peer_recover(
    pg_shard_t peer,
    const hobject_t &soid,
    const eversion_t &version);

  /// Notify that soid is being recovered on peer
  void begin_peer_recover(
    pg_shard_t peer,
    const hobject_t soid);

  /// Pull missing sets from all candidate peers
  void discover_all_missing(std::map<int, map<spg_t,pg_query_t> > &query_map);

  /// Notify that hoid has been fully recocovered
  void object_recovered(
    const hobject_t &hoid,
    const object_stat_sum_t &stat_diff) {
    info.stats.stats.sum.add(stat_diff);
    missing_loc.recovered(hoid);
  }

  /// Update info/stats to reflect backfill progress
  void update_backfill_progress(
    const hobject_t &updated_backfill,
    const pg_stat_t &updated_stats,
    bool preserve_local_num_bytes,
    ObjectStore::Transaction &t);

  /// Update info/stats to reflect completed backfill on hoid
  void update_complete_backfill_object_stats(
    const hobject_t &hoid,
    const pg_stat_t &stats);

  /// Update last_backfill for peer to new_last_backfill
  void update_peer_last_backfill(
    pg_shard_t peer,
    const hobject_t &new_last_backfill);

  /// Update info.stats with delta_stats for operation on soid
  void apply_op_stats(
    const hobject_t &soid,
    const object_stat_sum_t &delta_stats);

  /**
   * force_object_missing
   *
   * Force oid on peer to be missing at version.  If the object does not
   * currently need recovery, either candidates if provided or the remainder
   * of the acting set will be deemed to have the object.
   */
  void force_object_missing(
    const pg_shard_t &peer,
    const hobject_t &oid,
    eversion_t version) {
    force_object_missing(set<pg_shard_t>{peer}, oid, version);
  }
  void force_object_missing(
    const set<pg_shard_t> &peer,
    const hobject_t &oid,
    eversion_t version);

  /// Update state prior to backfilling soid on targets
  void prepare_backfill_for_missing(
    const hobject_t &soid,
    const eversion_t &version,
    const vector<pg_shard_t> &targets);

  /// Set targets with the right version for revert (see recover_primary)
  void set_revert_with_targets(
    const hobject_t &soid,
    const set<pg_shard_t> &good_peers);

  /// Update lcod for fromosd
  void update_peer_last_complete_ondisk(
    pg_shard_t fromosd,
    eversion_t lcod) {
    peer_last_complete_ondisk[fromosd] = lcod;
  }

  /// Update lcod
  void update_last_complete_ondisk(
    eversion_t lcod) {
    last_complete_ondisk = lcod;
  }

  /// Update state to reflect recovery up to version
  void recovery_committed_to(eversion_t version);

  /// Mark recovery complete
  void local_recovery_complete() {
    info.last_complete = info.last_update;
  }

  /// Update last_requested pointer to v
  void set_last_requested(version_t v) {
    pg_log.set_last_requested(v);
  }

  /// Write dirty state to t
  void write_if_dirty(ObjectStore::Transaction& t);

  /// Mark write completed to v with persisted lc
  void complete_write(eversion_t v, eversion_t lc);

  /// Update local write applied pointer
  void local_write_applied(eversion_t v) {
    last_update_applied = v;
  }

  /// Updates peering state with new map
  void advance_map(
    OSDMapRef osdmap,       ///< [in] new osdmap
    OSDMapRef lastmap,      ///< [in] prev osdmap
    vector<int>& newup,     ///< [in] new up set
    int up_primary,         ///< [in] new up primary
    vector<int>& newacting, ///< [in] new acting
    int acting_primary,     ///< [in] new acting primary
    PeeringCtx &rctx        ///< [out] recovery context
    );

  /// Activates most recently updated map
  void activate_map(
    PeeringCtx &rctx        ///< [out] recovery context
    );

  /// resets last_persisted_osdmap
  void reset_last_persisted() {
    last_persisted_osdmap = 0;
    dirty_info = true;
    dirty_big_info = true;
  }

  /// Signal shutdown beginning
  void shutdown() {
    deleting = true;
  }

  /// Signal shutdown complete
  void set_delete_complete() {
    deleted = true;
  }

  /// Dirty info and write out
  void force_write_state(ObjectStore::Transaction &t) {
    dirty_info = true;
    dirty_big_info = true;
    write_if_dirty(t);
  }

  //============================ const helpers ================================
  const char *get_current_state() const {
    return state_history.get_current_state();
  }
  epoch_t get_last_peering_reset() const {
    return last_peering_reset;
  }
  eversion_t get_last_rollback_info_trimmed_to_applied() const {
    return last_rollback_info_trimmed_to_applied;
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

  bool is_ec_pg() const override {
    return pool.info.is_erasure();
  }
  int get_pg_size() const override {
    return pool.info.size;
  }
  bool is_deleting() const {
    return deleting;
  }
  bool is_deleted() const {
    return deleted;
  }
  const set<pg_shard_t> &get_upset() const override {
    return upset;
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
  bool pg_has_reset_since(epoch_t e) const {
    return deleted || e < get_last_peering_reset();
  }

  int get_role() const {
    return role;
  }
  const vector<int> &get_acting() const {
    return acting;
  }
  const set<pg_shard_t> &get_actingset() const {
    return actingset;
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

  bool is_backfill_target(pg_shard_t osd) const {
    return backfill_targets.count(osd);
  }
  const set<pg_shard_t> &get_backfill_targets() const {
    return backfill_targets;
  }
  bool is_async_recovery_target(pg_shard_t peer) const {
    return async_recovery_targets.count(peer);
  }
  const set<pg_shard_t> &get_async_recovery_targets() const {
    return async_recovery_targets;
  }
  const set<pg_shard_t> &get_acting_recovery_backfill() const {
    return acting_recovery_backfill;
  }

  const PGLog &get_pg_log() const {
    return pg_log;
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
  bool is_recovery_unfound() const {
    return state_test(PG_STATE_RECOVERY_UNFOUND);
  }
  bool is_backfill_unfound() const {
    return state_test(PG_STATE_BACKFILL_UNFOUND);
  }
  bool is_incomplete() const { return state_test(PG_STATE_INCOMPLETE); }
  bool is_clean() const { return state_test(PG_STATE_CLEAN); }
  bool is_degraded() const { return state_test(PG_STATE_DEGRADED); }
  bool is_undersized() const { return state_test(PG_STATE_UNDERSIZED); }
  bool is_remapped() const { return state_test(PG_STATE_REMAPPED); }
  bool is_peered() const {
    return state_test(PG_STATE_ACTIVE) || state_test(PG_STATE_PEERED);
  }
  bool is_recovering() const { return state_test(PG_STATE_RECOVERING); }
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

  ceph_release_t get_last_require_osd_release() const {
    return last_require_osd_release;
  }

  const pg_info_t &get_info() const {
    return info;
  }

  const decltype(peer_info) &get_peer_info() const {
    return peer_info;
  }
  const decltype(peer_missing) &get_peer_missing() const {
    return peer_missing;
  }
  const pg_missing_const_i &get_peer_missing(const pg_shard_t &peer) const {
    if (peer == pg_whoami) {
      return pg_log.get_missing();
    } else {
      assert(peer_missing.count(peer));
      return peer_missing.find(peer)->second;
    }
  }
  const pg_info_t&get_peer_info(pg_shard_t peer) const {
    assert(peer_info.count(peer));
    return peer_info.find(peer)->second;
  }
  bool has_peer_info(pg_shard_t peer) const {
    return peer_info.count(peer);
  }

  bool needs_recovery() const;
  bool needs_backfill() const;

  /**
   * Returns whether all peers which might have unfound objects have been
   * queried or marked lost.
   */
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

  bool have_missing() const {
    return pg_log.get_missing().num_missing() > 0;
  }
  bool get_num_missing() const {
    return pg_log.get_missing().num_missing() > 0;
  }

  const MissingLoc &get_missing_loc() const {
    return missing_loc;
  }

  const MissingLoc::missing_by_count_t &get_missing_by_count() const {
    return missing_loc.get_missing_by_count();
  }

  eversion_t get_min_last_complete_ondisk() const {
    return min_last_complete_ondisk;
  }

  eversion_t get_pg_trim_to() const {
    return pg_trim_to;
  }

  eversion_t get_last_update_applied() const {
    return last_update_applied;
  }

  bool debug_has_dirty_state() const {
    return dirty_info || dirty_big_info;
  }

  std::string get_pg_state_string() const {
    return pg_state_string(state);
  }

  /// Dump representation of past_intervals to out
  void print_past_intervals(ostream &out) const {
    out << "[" << past_intervals.get_bounds()
	<< ")/" << past_intervals.size();
  }

  void dump_history(Formatter *f) const {
    state_history.dump(f);
  }

  /// Dump formatted peering status
  void dump_peering_state(Formatter *f);

private:
  /// Mask feature vector with feature set from new peer
  void apply_peer_features(uint64_t f) { peer_features &= f; }

  /// Reset feature vector to default
  void reset_min_peer_features() {
    peer_features = CEPH_FEATURES_SUPPORTED_DEFAULT;
  }
public:
  /// Get feature vector common to all known peers with this pg
  uint64_t get_min_peer_features() const { return peer_features; }

  /// Get feature vector common to acting set
  uint64_t get_min_acting_features() const { return acting_features; }

  /// Get feature vector common to up/acting set
  uint64_t get_min_upacting_features() const { return upacting_features; }


  // Flush control interface
private:
  /**
   * Start additional flush (blocks needs_flush/activation until
   * complete_flush is called once for each start_flush call as
   * required by start_flush_on_transaction).
   */
  void start_flush(ObjectStore::Transaction &t) {
    flushes_in_progress++;
    pl->start_flush_on_transaction(t);
  }
public:
  /// True if there are outstanding flushes
  bool needs_flush() const {
    return flushes_in_progress > 0;
  }
  /// Must be called once per start_flush
  void complete_flush();

  friend ostream &operator<<(ostream &out, const PeeringState &ps);
};

ostream &operator<<(ostream &out, const PeeringState &ps);
