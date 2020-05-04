// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <memory>
#include <optional>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <boost/smart_ptr/local_shared_ptr.hpp>
#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/sleep.hh>

#include "common/dout.h"
#include "crimson/net/Fwd.h"
#include "messages/MOSDRepOpReply.h"
#include "messages/MOSDOpReply.h"
#include "os/Transaction.h"
#include "osd/osd_types.h"
#include "crimson/osd/object_context.h"
#include "osd/PeeringState.h"

#include "crimson/common/type_helpers.h"
#include "crimson/os/futurized_collection.h"
#include "crimson/osd/backfill_state.h"
#include "crimson/osd/osd_operations/client_request.h"
#include "crimson/osd/osd_operations/peering_event.h"
#include "crimson/osd/osd_operations/replicated_request.h"
#include "crimson/osd/osd_operations/background_recovery.h"
#include "crimson/osd/shard_services.h"
#include "crimson/osd/osdmap_gate.h"
#include "crimson/osd/pg_recovery.h"
#include "crimson/osd/pg_recovery_listener.h"
#include "crimson/osd/recovery_backend.h"

class OSDMap;
class MQuery;
class PGBackend;
class PGPeeringEvent;
class osd_op_params_t;

namespace recovery {
  class Context;
}

namespace crimson::net {
  class Messenger;
}

namespace crimson::os {
  class FuturizedStore;
}

namespace crimson::osd {
class ClientRequest;

class PG : public boost::intrusive_ref_counter<
  PG,
  boost::thread_unsafe_counter>,
  public PGRecoveryListener,
  PeeringState::PeeringListener,
  DoutPrefixProvider
{
  using ec_profile_t = std::map<std::string,std::string>;
  using cached_map_t = boost::local_shared_ptr<const OSDMap>;

  ClientRequest::PGPipeline client_request_pg_pipeline;
  PeeringEvent::PGPipeline peering_request_pg_pipeline;
  RepRequest::PGPipeline replicated_request_pg_pipeline;

  spg_t pgid;
  pg_shard_t pg_whoami;
  crimson::os::CollectionRef coll_ref;
  ghobject_t pgmeta_oid;

  seastar::timer<seastar::lowres_clock> check_readable_timer;
  seastar::timer<seastar::lowres_clock> renew_lease_timer;

public:
  PG(spg_t pgid,
     pg_shard_t pg_shard,
     crimson::os::CollectionRef coll_ref,
     pg_pool_t&& pool,
     std::string&& name,
     cached_map_t osdmap,
     ShardServices &shard_services,
     ec_profile_t profile);

  ~PG();

  const pg_shard_t& get_pg_whoami() const final {
    return pg_whoami;
  }

  const spg_t& get_pgid() const final {
    return pgid;
  }

  PGBackend& get_backend() {
    return *backend;
  }
  const PGBackend& get_backend() const {
    return *backend;
  }
  // EpochSource
  epoch_t get_osdmap_epoch() const final {
    return peering_state.get_osdmap_epoch();
  }

  eversion_t get_pg_trim_to() const {
    return peering_state.get_pg_trim_to();
  }

  eversion_t get_min_last_complete_ondisk() const {
    return peering_state.get_min_last_complete_ondisk();
  }

  const pg_info_t& get_info() const {
    return peering_state.get_info();
  }

  // DoutPrefixProvider
  std::ostream& gen_prefix(std::ostream& out) const final {
    return out << *this;
  }
  crimson::common::CephContext *get_cct() const final {
    return shard_services.get_cct();
  }
  unsigned get_subsys() const final {
    return ceph_subsys_osd;
  }

  crimson::os::CollectionRef get_collection_ref() {
    return coll_ref;
  }

  // PeeringListener
  void prepare_write(
    pg_info_t &info,
    pg_info_t &last_written_info,
    PastIntervals &past_intervals,
    PGLog &pglog,
    bool dirty_info,
    bool dirty_big_info,
    bool need_write_epoch,
    ceph::os::Transaction &t) final;

  void on_info_history_change() final {
    // Not needed yet -- mainly for scrub scheduling
  }

  void scrub_requested(bool deep, bool repair, bool need_auto = false) final {
    ceph_assert(0 == "Not implemented");
  }

  uint64_t get_snap_trimq_size() const final {
    return 0;
  }

  void send_cluster_message(
    int osd, Message *m,
    epoch_t epoch, bool share_map_update=false) final {
    (void)shard_services.send_to_osd(osd, m, epoch);
  }

  void send_pg_created(pg_t pgid) final {
    (void)shard_services.send_pg_created(pgid);
  }

  bool try_flush_or_schedule_async() final;

  void start_flush_on_transaction(
    ceph::os::Transaction &t) final {
    t.register_on_commit(
      new LambdaContext([this](int r){
	peering_state.complete_flush();
    }));
  }

  void on_flushed() final {
    // will be needed for unblocking IO operations/peering
  }

  template <typename T>
  void start_peering_event_operation(T &&evt, float delay = 0) {
    (void) shard_services.start_operation<LocalPeeringEvent>(
      this,
      shard_services,
      pg_whoami,
      pgid,
      delay,
      std::forward<T>(evt));
  }

  void schedule_event_after(
    PGPeeringEventRef event,
    float delay) final {
    start_peering_event_operation(std::move(*event), delay);
  }
  std::vector<pg_shard_t> get_replica_recovery_order() const final {
    return peering_state.get_replica_recovery_order();
  }
  void request_local_background_io_reservation(
    unsigned priority,
    PGPeeringEventURef on_grant,
    PGPeeringEventURef on_preempt) final {
    shard_services.local_reserver.request_reservation(
      pgid,
      on_grant ? make_lambda_context([this, on_grant=std::move(on_grant)] (int) {
	start_peering_event_operation(std::move(*on_grant));
      }) : nullptr,
      priority,
      on_preempt ? make_lambda_context(
	[this, on_preempt=std::move(on_preempt)] (int) {
	start_peering_event_operation(std::move(*on_preempt));
      }) : nullptr);
  }

  void update_local_background_io_priority(
    unsigned priority) final {
    shard_services.local_reserver.update_priority(
      pgid,
      priority);
  }

  void cancel_local_background_io_reservation() final {
    shard_services.local_reserver.cancel_reservation(
      pgid);
  }

  void request_remote_recovery_reservation(
    unsigned priority,
    PGPeeringEventURef on_grant,
    PGPeeringEventURef on_preempt) final {
    shard_services.remote_reserver.request_reservation(
      pgid,
      on_grant ? make_lambda_context([this, on_grant=std::move(on_grant)] (int) {
	start_peering_event_operation(std::move(*on_grant));
      }) : nullptr,
      priority,
      on_preempt ? make_lambda_context(
	[this, on_preempt=std::move(on_preempt)] (int) {
	start_peering_event_operation(std::move(*on_preempt));
      }) : nullptr);
  }

  void cancel_remote_recovery_reservation() final {
    shard_services.remote_reserver.cancel_reservation(
      pgid);
  }

  void schedule_event_on_commit(
    ceph::os::Transaction &t,
    PGPeeringEventRef on_commit) final {
    t.register_on_commit(
      make_lambda_context(
	[this, on_commit=std::move(on_commit)](int) {
	  start_peering_event_operation(std::move(*on_commit));
	}));
  }

  void update_heartbeat_peers(set<int> peers) final {
    // Not needed yet
  }
  void set_probe_targets(const set<pg_shard_t> &probe_set) final {
    // Not needed yet
  }
  void clear_probe_targets() final {
    // Not needed yet
  }
  void queue_want_pg_temp(const std::vector<int> &wanted) final {
    shard_services.queue_want_pg_temp(pgid.pgid, wanted);
  }
  void clear_want_pg_temp() final {
    shard_services.remove_want_pg_temp(pgid.pgid);
  }
  void publish_stats_to_osd() final {
    // Not needed yet
  }
  void clear_publish_stats() final {
    // Not needed yet
  }
  void check_recovery_sources(const OSDMapRef& newmap) final {
    // Not needed yet
  }
  void check_blacklisted_watchers() final {
    // Not needed yet
  }
  void clear_primary_state() final {
    // Not needed yet
  }

  void queue_check_readable(epoch_t last_peering_reset,
			    ceph::timespan delay) final;
  void recheck_readable() final;

  unsigned get_target_pg_log_entries() const final;

  void on_pool_change() final {
    // Not needed yet
  }
  void on_role_change() final {
    // Not needed yet
  }
  void on_change(ceph::os::Transaction &t) final;
  void on_activate(interval_set<snapid_t> to_trim) final;
  void on_activate_complete() final;
  void on_new_interval() final {
    // Not needed yet
  }
  Context *on_clean() final {
    // Not needed yet (will be needed for IO unblocking)
    return nullptr;
  }
  void on_activate_committed() final {
    // Not needed yet (will be needed for IO unblocking)
  }
  void on_active_exit() final {
    // Not needed yet
  }

  void on_removal(ceph::os::Transaction &t) final {
    // TODO
  }
  void do_delete_work(ceph::os::Transaction &t) final;

  // merge/split not ready
  void clear_ready_to_merge() final {}
  void set_not_ready_to_merge_target(pg_t pgid, pg_t src) final {}
  void set_not_ready_to_merge_source(pg_t pgid) final {}
  void set_ready_to_merge_target(eversion_t lu, epoch_t les, epoch_t lec) final {}
  void set_ready_to_merge_source(eversion_t lu) final {}

  void on_active_actmap() final {
    // Not needed yet
  }
  void on_active_advmap(const OSDMapRef &osdmap) final {
    // Not needed yet
  }
  epoch_t oldest_stored_osdmap() final {
    // TODO
    return 0;
  }

  void on_backfill_reserved() final {
    ceph_assert(0 == "Not implemented");
  }
  void on_backfill_canceled() final {
    ceph_assert(0 == "Not implemented");
  }
  void on_recovery_reserved() final {
    recovery_handler->start_pglogbased_recovery();
  }


  bool try_reserve_recovery_space(
    int64_t primary_num_bytes, int64_t local_num_bytes) final {
    return true;
  }
  void unreserve_recovery_space() final {}

  struct PGLogEntryHandler : public PGLog::LogEntryHandler {
    PG *pg;
    ceph::os::Transaction *t;
    PGLogEntryHandler(PG *pg, ceph::os::Transaction *t) : pg(pg), t(t) {}

    // LogEntryHandler
    void remove(const hobject_t &hoid) override {
      // TODO
    }
    void try_stash(const hobject_t &hoid, version_t v) override {
      // TODO
    }
    void rollback(const pg_log_entry_t &entry) override {
      // TODO
    }
    void rollforward(const pg_log_entry_t &entry) override {
      // TODO
    }
    void trim(const pg_log_entry_t &entry) override {
      // TODO
    }
  };
  PGLog::LogEntryHandlerRef get_log_handler(
    ceph::os::Transaction &t) final {
    return std::make_unique<PG::PGLogEntryHandler>(this, &t);
  }

  void rebuild_missing_set_with_deletes(PGLog &pglog) final {
    ceph_assert(0 == "Impossible for crimson");
  }

  PerfCounters &get_peering_perf() final {
    return shard_services.get_recoverystate_perf_logger();
  }
  PerfCounters &get_perf_logger() final {
    return shard_services.get_perf_logger();
  }

  void log_state_enter(const char *state) final;
  void log_state_exit(
    const char *state_name, utime_t enter_time,
    uint64_t events, utime_t event_dur) final;

  void dump_recovery_info(Formatter *f) const final {
  }

  OstreamTemp get_clog_info() final {
    // not needed yet: replace with not a stub (needs to be wired up to monc)
    return OstreamTemp(CLOG_INFO, nullptr);
  }
  OstreamTemp get_clog_debug() final {
    // not needed yet: replace with not a stub (needs to be wired up to monc)
    return OstreamTemp(CLOG_DEBUG, nullptr);
  }
  OstreamTemp get_clog_error() final {
    // not needed yet: replace with not a stub (needs to be wired up to monc)
    return OstreamTemp(CLOG_ERROR, nullptr);
  }

  ceph::signedspan get_mnow() final;
  HeartbeatStampsRef get_hb_stamps(int peer) final;
  void schedule_renew_lease(epoch_t plr, ceph::timespan delay) final;


  // Utility
  bool is_primary() const final {
    return peering_state.is_primary();
  }
  bool is_peered() const final {
    return peering_state.is_peered();
  }
  bool is_recovering() const final {
    return peering_state.is_recovering();
  }
  bool is_backfilling() const final {
    return peering_state.is_backfilling();
  }
  pg_stat_t get_stats() {
    auto stats = peering_state.prepare_stats_for_publish(
      false,
      pg_stat_t(),
      object_stat_collection_t());
    ceph_assert(stats);
    return *stats;
  }
  bool get_need_up_thru() const {
    return peering_state.get_need_up_thru();
  }
  epoch_t get_same_interval_since() const {
    return get_info().history.same_interval_since;
  }

  const auto& get_pool() const {
    return peering_state.get_pool();
  }
  pg_shard_t get_primary() const {
    return peering_state.get_primary();
  }

  /// initialize created PG
  void init(
    int role,
    const std::vector<int>& up,
    int up_primary,
    const std::vector<int>& acting,
    int acting_primary,
    const pg_history_t& history,
    const PastIntervals& pim,
    bool backfill,
    ceph::os::Transaction &t);

  seastar::future<> read_state(crimson::os::FuturizedStore* store);

  void do_peering_event(
    PGPeeringEvent& evt, PeeringCtx &rctx);

  void handle_advance_map(cached_map_t next_map, PeeringCtx &rctx);
  void handle_activate_map(PeeringCtx &rctx);
  void handle_initialize(PeeringCtx &rctx);

  static std::pair<hobject_t, RWState::State> get_oid_and_lock(
    const MOSDOp &m,
    const OpInfo &op_info);
  static std::optional<hobject_t> resolve_oid(
    const SnapSet &snapset,
    const hobject_t &oid);

  using load_obc_ertr = crimson::errorator<
    crimson::ct_error::object_corrupted>;
  load_obc_ertr::future<
    std::pair<crimson::osd::ObjectContextRef, bool>>
  get_or_load_clone_obc(
    hobject_t oid, crimson::osd::ObjectContextRef head_obc);

  load_obc_ertr::future<
    std::pair<crimson::osd::ObjectContextRef, bool>>
  get_or_load_head_obc(hobject_t oid);

  load_obc_ertr::future<ObjectContextRef> get_locked_obc(
    Operation *op,
    const hobject_t &oid,
    RWState::State type);
public:
  template <typename F>
  auto with_locked_obc(
    Ref<MOSDOp> &m,
    const OpInfo &op_info,
    Operation *op,
    F &&f) {
    if (__builtin_expect(stopping, false)) {
      throw crimson::common::system_shutdown_exception();
    }
    auto [oid, type] = get_oid_and_lock(*m, op_info);
    return get_locked_obc(op, oid, type)
      .safe_then([f=std::forward<F>(f), type=type](auto obc) {
	return f(obc).finally([obc, type=type] {
	  obc->put_lock_type(type);
	  return load_obc_ertr::now();
	});
      });
  }

  seastar::future<> handle_rep_op(Ref<MOSDRepOp> m);
  void handle_rep_op_reply(crimson::net::Connection* conn,
			   const MOSDRepOpReply& m);

  void print(std::ostream& os) const;

private:
  void do_peering_event(
    const boost::statechart::event_base &evt,
    PeeringCtx &rctx);
  seastar::future<Ref<MOSDOpReply>> do_osd_ops(
    Ref<MOSDOp> m,
    ObjectContextRef obc,
    const OpInfo &op_info);
  seastar::future<Ref<MOSDOpReply>> do_pg_ops(Ref<MOSDOp> m);
  seastar::future<> do_osd_op(
    ObjectState& os,
    OSDOp& op,
    ceph::os::Transaction& txn);
  seastar::future<ceph::bufferlist> do_pgnls(ceph::bufferlist& indata,
					     const std::string& nspace,
					     uint64_t limit);
  seastar::future<> submit_transaction(const OpInfo& op_info,
				       const std::vector<OSDOp>& ops,
				       ObjectContextRef&& obc,
				       ceph::os::Transaction&& txn,
				       const osd_op_params_t& oop);

private:
  OSDMapGate osdmap_gate;
  ShardServices &shard_services;

  cached_map_t osdmap;

public:
  cached_map_t get_osdmap() { return osdmap; }
  eversion_t next_version() {
    return eversion_t(get_osdmap_epoch(),
		      ++projected_last_update.version);
  }
  ShardServices& get_shard_services() final {
    return shard_services;
  }
  seastar::future<> stop();

private:
  std::unique_ptr<PGBackend> backend;
  std::unique_ptr<RecoveryBackend> recovery_backend;
  std::unique_ptr<PGRecovery> recovery_handler;

  PeeringState peering_state;
  eversion_t projected_last_update;
public:
  RecoveryBackend* get_recovery_backend() final {
    return recovery_backend.get();
  }
  PGRecovery* get_recovery_handler() final {
    return recovery_handler.get();
  }
  PeeringState& get_peering_state() final {
    return peering_state;
  }
  bool has_reset_since(epoch_t epoch) const final {
    return peering_state.pg_has_reset_since(epoch);
  }

  const pg_missing_tracker_t& get_local_missing() const {
    return peering_state.get_pg_log().get_missing();
  }
  epoch_t get_last_peering_reset() const {
    return peering_state.get_last_peering_reset();
  }
  const set<pg_shard_t> &get_acting_recovery_backfill() const {
    return peering_state.get_acting_recovery_backfill();
  }
  void begin_peer_recover(pg_shard_t peer, const hobject_t oid) {
    peering_state.begin_peer_recover(peer, oid);
  }
  uint64_t min_peer_features() const {
    return peering_state.get_min_peer_features();
  }
  const map<hobject_t, set<pg_shard_t>>&
  get_missing_loc_shards() const {
    return peering_state.get_missing_loc().get_missing_locs();
  }
  const map<pg_shard_t, pg_missing_t> &get_shard_missing() const {
    return peering_state.get_peer_missing();
  }
  const pg_missing_const_i* get_shard_missing(pg_shard_t shard) const {
    if (shard == pg_whoami)
      return &get_local_missing();
    else {
      auto it = peering_state.get_peer_missing().find(shard);
      if (it == peering_state.get_peer_missing().end())
	return nullptr;
      else
	return &it->second;
    }
  }
  int get_recovery_op_priority() const {
    int64_t pri = 0;
    get_pool().info.opts.get(pool_opts_t::RECOVERY_OP_PRIORITY, &pri);
    return  pri > 0 ? pri : crimson::common::local_conf()->osd_recovery_op_priority;
  }

private:
  // instead of seastar::gate, we use a boolean flag to indicate
  // whether the system is shutting down, as we don't need to track
  // continuations here.
  bool stopping = false;

  class WaitForActiveBlocker : public BlockerT<WaitForActiveBlocker> {
    PG *pg;

    const spg_t pgid;
    seastar::shared_promise<> p;

  protected:
    void dump_detail(Formatter *f) const;

  public:
    static constexpr const char *type_name = "WaitForActiveBlocker";

    WaitForActiveBlocker(PG *pg) : pg(pg) {}
    void on_active();
    blocking_future<> wait();
    seastar::future<> stop();
  } wait_for_active_blocker;

  friend std::ostream& operator<<(std::ostream&, const PG& pg);
  friend class ClientRequest;
  friend class PGAdvanceMap;
  friend class PeeringEvent;
  friend class RepRequest;
  friend struct BackfillState::PGFacade;
private:
  seastar::future<bool> find_unfound() {
    return seastar::make_ready_future<bool>(true);
  }

  bool is_missing_object(const hobject_t& soid) const {
    return peering_state.get_pg_log().get_missing().get_items().count(soid);
  }
  bool is_unreadable_object(const hobject_t &oid,
			    eversion_t* v = 0) const final {
    return is_missing_object(oid) ||
      !peering_state.get_missing_loc().readable_with_acting(
	oid, get_actingset(), v);
  }
  const set<pg_shard_t> &get_actingset() const {
    return peering_state.get_actingset();
  }
};

std::ostream& operator<<(std::ostream&, const PG& pg);

}
