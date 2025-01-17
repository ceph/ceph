// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include <memory>
#include <optional>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>

#include "common/dout.h"
#include "include/interval_set.h"
#include "crimson/net/Fwd.h"
#include "messages/MOSDRepOpReply.h"
#include "messages/MOSDOpReply.h"
#include "os/Transaction.h"
#include "osd/osd_types.h"
#include "osd/osd_types_fmt.h"
#include "crimson/osd/object_context.h"
#include "osd/PeeringState.h"
#include "osd/SnapMapper.h"

#include "crimson/common/interruptible_future.h"
#include "crimson/common/log.h"
#include "crimson/common/type_helpers.h"
#include "crimson/os/futurized_collection.h"
#include "crimson/osd/backfill_state.h"
#include "crimson/osd/pg_interval_interrupt_condition.h"
#include "crimson/osd/ops_executer.h"
#include "crimson/osd/osd_operations/client_request.h"
#include "crimson/osd/osd_operations/logmissing_request.h"
#include "crimson/osd/osd_operations/logmissing_request_reply.h"
#include "crimson/osd/osd_operations/peering_event.h"
#include "crimson/osd/osd_operations/replicated_request.h"
#include "crimson/osd/shard_services.h"
#include "crimson/osd/osdmap_gate.h"
#include "crimson/osd/pg_activation_blocker.h"
#include "crimson/osd/pg_recovery.h"
#include "crimson/osd/pg_recovery_listener.h"
#include "crimson/osd/recovery_backend.h"
#include "crimson/osd/object_context_loader.h"
#include "crimson/osd/scrub/pg_scrubber.h"

class MQuery;
class OSDMap;
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
class OpsExecuter;
class BackfillRecovery;
class SnapTrimEvent;
class PglogBasedRecovery;

class PG : public boost::intrusive_ref_counter<
  PG,
  boost::thread_unsafe_counter>,
  public PGRecoveryListener,
  PeeringState::PeeringListener,
  DoutPrefixProvider
{
  using ec_profile_t = std::map<std::string,std::string>;
  using cached_map_t = OSDMapService::cached_map_t;

  ClientRequest::PGPipeline request_pg_pipeline;
  PGPeeringPipeline peering_request_pg_pipeline;

  ClientRequest::Orderer client_request_orderer;

  spg_t pgid;
  pg_shard_t pg_whoami;
  crimson::os::CollectionRef coll_ref;
  ghobject_t pgmeta_oid;

  seastar::timer<seastar::lowres_clock> check_readable_timer;
  seastar::timer<seastar::lowres_clock> renew_lease_timer;

public:
  template <typename T = void>
  using interruptible_future =
    ::crimson::interruptible::interruptible_future<
      ::crimson::osd::IOInterruptCondition, T>;

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

  const pg_info_t& get_info() const final {
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

  uint64_t get_snap_trimq_size() const final {
    return std::size(snap_trimq);
  }

  /**
   * complete_rctx
   *
   * complete_rctx is responsible for submitting writes and messages
   * resulting from processing a PeeringState event as well as resolving
   * any asyncronous actions initiated by the PeeringState::Listener
   * callbacks below.  The caller is responsible for calling complete_rctx
   * and waiting for the future to resolve before exiting the
   * PGPeeringPipeline::process stage (see osd_operations/peering_event.h).
   *
   * orderer below ensures that operations submitted on the OSD-wide
   * OSDSingleton instance are completed in the order initiated.  This is
   * specifically important for operations on the local and remote async
   * reserver instances, as well as setting and clearing pg_temp mapping
   * requests.
   */
  ShardServices::singleton_orderer_t orderer;
  seastar::future<> complete_rctx(PeeringCtx &&rctx) {
    shard_services.send_pg_temp(orderer);
    if (get_need_up_thru()) {
      shard_services.send_alive(orderer, get_same_interval_since());
    }

    ShardServices::singleton_orderer_t o;
    std::swap(o, orderer);
    return seastar::when_all(
      shard_services.dispatch_context(
	get_collection_ref(),
	std::move(rctx)),
      shard_services.run_orderer(std::move(o))
    ).then([](auto) {});
  }

  void send_cluster_message(
    int osd, MessageURef m,
    epoch_t epoch, bool share_map_update=false) final {
    LOG_PREFIX(PG::send_cluster_message);
    SUBDEBUGDPP(
      osd, "message {} to {} share_map_update {}",
      *this, *m, osd, share_map_update);
    /* We don't bother to queue this one in the orderer because capturing the
     * message ref in std::function is problematic as it isn't copyable.  This
     * is solvable, but it's not quite worth the effort at the moment as we
     * aren't worried about ordering of message send events except between
     * messages to the same target within an interval, which doesn't really
     * happen while processing a single event.  It'll probably be worth
     * generalizing the orderer structure to fix this in the future, probably
     * by using std::move_only_function once widely available. */
    std::ignore = shard_services.send_to_osd(osd, std::move(m), epoch);
  }

  void send_pg_created(pg_t pgid) final {
    LOG_PREFIX(PG::send_pg_created);
    SUBDEBUGDPP(osd, "pgid {}", *this, pgid);
    shard_services.send_pg_created(orderer, pgid);
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
    LOG_PREFIX(PG::start_peering_event_operations);
    SUBDEBUGDPP(osd, "event {} delay {}", *this, evt.get_desc(), delay);
    (void) shard_services.start_operation<LocalPeeringEvent>(
      this,
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
    LOG_PREFIX(PG::request_local_background_io_reservation);
    SUBDEBUGDPP(
      osd, "priority {} on_grant {} on_preempt {}",
      *this, priority, on_grant->get_desc(), on_preempt->get_desc());
    shard_services.local_request_reservation(
      orderer,
      pgid,
      on_grant ? make_lambda_context([this, on_grant=std::move(on_grant)] (int) {
	start_peering_event_operation(std::move(*on_grant));
      }) : nullptr,
      priority,
      on_preempt ? make_lambda_context(
	[this, on_preempt=std::move(on_preempt)] (int) {
	  start_peering_event_operation(std::move(*on_preempt));
	}) : nullptr
    );
  }

  void update_local_background_io_priority(
    unsigned priority) final {
    LOG_PREFIX(PG::update_local_background_io_priority);
    SUBDEBUGDPP(osd, "priority {}", *this, priority);
    shard_services.local_update_priority(
      orderer,
      pgid,
      priority);
  }

  void cancel_local_background_io_reservation() final {
    LOG_PREFIX(PG::cancel_local_background_io_reservation);
    SUBDEBUGDPP(osd, "", *this);
    shard_services.local_cancel_reservation(
      orderer,
      pgid);
  }

  void request_remote_recovery_reservation(
    unsigned priority,
    PGPeeringEventURef on_grant,
    PGPeeringEventURef on_preempt) final {
    LOG_PREFIX(PG::request_remote_recovery_reservation);
    SUBDEBUGDPP(
      osd, "priority {} on_grant {} on_preempt {}",
      *this, on_grant->get_desc(), on_preempt->get_desc());
    shard_services.remote_request_reservation(
      orderer,
      pgid,
      on_grant ? make_lambda_context([this, on_grant=std::move(on_grant)] (int) {
	start_peering_event_operation(std::move(*on_grant));
      }) : nullptr,
      priority,
      on_preempt ? make_lambda_context(
	[this, on_preempt=std::move(on_preempt)] (int) {
	  start_peering_event_operation(std::move(*on_preempt));
      }) : nullptr
    );
  }

  void cancel_remote_recovery_reservation() final {
    LOG_PREFIX(PG::cancel_remote_recovery_reservation);
    SUBDEBUGDPP(osd, "", *this);
    shard_services.remote_cancel_reservation(orderer, pgid);
  }

  void schedule_event_on_commit(
    ceph::os::Transaction &t,
    PGPeeringEventRef on_commit) final {
    LOG_PREFIX(PG::schedule_event_on_commit);
    SUBDEBUGDPP(osd, "on_commit {}", *this, on_commit->get_desc());
    t.register_on_commit(
      make_lambda_context(
	[this, on_commit=std::move(on_commit)](int) {
	  start_peering_event_operation(std::move(*on_commit));
	}));
  }

  void update_heartbeat_peers(std::set<int> peers) final {
    // Not needed yet
  }
  void set_probe_targets(const std::set<pg_shard_t> &probe_set) final {
    // Not needed yet
  }
  void clear_probe_targets() final {
    // Not needed yet
  }
  void queue_want_pg_temp(const std::vector<int> &wanted) final {
    LOG_PREFIX(PG::queue_want_pg_temp);
    SUBDEBUGDPP(osd, "wanted {}", *this, wanted);
    shard_services.queue_want_pg_temp(orderer, pgid.pgid, wanted);
  }
  void clear_want_pg_temp() final {
    LOG_PREFIX(PG::clear_want_pg_temp);
    SUBDEBUGDPP(osd, "", *this);
    shard_services.remove_want_pg_temp(orderer, pgid.pgid);
  }
  void check_recovery_sources(const OSDMapRef& newmap) final {
    LOG_PREFIX(PG::check_recovery_sources);
    recovery_backend->for_each_recovery_waiter(
      [newmap, FNAME, this](auto &, auto &waiter) {
        if (waiter->is_pulling() &&
            newmap->is_down(waiter->pull_info->from.osd)) {
          SUBDEBUGDPP(
            osd,
            " repeating pulling for {}, due to osd {} down",
            *this,
            waiter->pull_info->soid,
            waiter->pull_info->from.osd);
          waiter->repeat_pull();
        }
      });
  }
  void check_blocklisted_watchers() final;
  void clear_primary_state() final {
    recovery_finisher = nullptr;
  }

  void queue_check_readable(epoch_t last_peering_reset,
			    ceph::timespan delay) final;
  void recheck_readable() final;

  unsigned get_target_pg_log_entries() const final;

  void init_collection_pool_opts();
  void on_pool_change();
  void on_role_change() final {
    // Not needed yet
  }
  void on_change(ceph::os::Transaction &t) final;
  void on_activate(interval_set<snapid_t> to_trim) final;
  void on_replica_activate() final;
  void on_activate_complete() final;
  void on_new_interval() final {
    recovery_finisher = nullptr;
  }
  Context *on_clean() final;
  void on_activate_committed() final {
    if (!is_primary()) {
      wait_for_active_blocker.unblock();
    }
  }
  void on_active_exit() final {
    // Not needed yet
  }

  void on_removal(ceph::os::Transaction &t) final;

  void clear_log_entry_maps();

  std::pair<ghobject_t, bool>
  do_delete_work(ceph::os::Transaction &t, ghobject_t _next) final;

  // merge/split not ready
  void clear_ready_to_merge() final {}
  void set_not_ready_to_merge_target(pg_t pgid, pg_t src) final {}
  void set_not_ready_to_merge_source(pg_t pgid) final {}
  void set_ready_to_merge_target(eversion_t lu, epoch_t les, epoch_t lec) final {}
  void set_ready_to_merge_source(eversion_t lu) final {}

  void on_active_actmap() final;
  void on_active_advmap(const OSDMapRef &osdmap) final;

  epoch_t cluster_osdmap_trim_lower_bound() final {
    return shard_services.get_osdmap_tlb();
  }

  void on_backfill_reserved() final {
    recovery_handler->on_backfill_reserved();
  }
  void on_backfill_canceled() final {
    recovery_handler->backfill_cancelled();
  }

  void on_recovery_cancelled() final {
    cancel_pglog_based_recovery_op();
  }

  void on_recovery_reserved() final {
    recovery_handler->start_pglogbased_recovery();
  }


  bool try_reserve_recovery_space(
    int64_t primary_num_bytes, int64_t local_num_bytes) final {
    // TODO
    return true;
  }
  void unreserve_recovery_space() final {}

  void remove_maybe_snapmapped_object(
    ceph::os::Transaction &t,
    const hobject_t &soid);

  struct PGLogEntryHandler : public PGLog::LogEntryHandler {
    PG *pg;
    ceph::os::Transaction *t;
    PGLogEntryHandler(PG *pg, ceph::os::Transaction *t) : pg(pg), t(t) {}

    // LogEntryHandler
    void remove(const hobject_t &soid) override;
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

  ceph::signedspan get_mnow() const final;
  HeartbeatStampsRef get_hb_stamps(int peer) final;
  void schedule_renew_lease(epoch_t plr, ceph::timespan delay) final;


  // Utility
  bool is_active_clean() const {
    return peering_state.is_active() && peering_state.is_clean();
  }
  bool is_primary() const final {
    return peering_state.is_primary();
  }
  bool is_nonprimary() const {
    return peering_state.is_nonprimary();
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
  uint64_t get_last_user_version() const {
    return get_info().last_user_version;
  }
  bool get_need_up_thru() const {
    return peering_state.get_need_up_thru();
  }
  bool should_send_op(pg_shard_t peer, const hobject_t &hoid) const;
  epoch_t get_same_interval_since() const {
    return get_info().history.same_interval_since;
  }

  const auto& get_pgpool() const {
    return peering_state.get_pgpool();
  }
  pg_shard_t get_primary() const {
    return peering_state.get_primary();
  }

  /// initialize created PG
  seastar::future<> init(
    int role,
    const std::vector<int>& up,
    int up_primary,
    const std::vector<int>& acting,
    int acting_primary,
    const pg_history_t& history,
    const PastIntervals& pim,
    ceph::os::Transaction &t);

  seastar::future<> read_state(crimson::os::FuturizedStore::Shard* store);

  void do_peering_event(PGPeeringEvent& evt, PeeringCtx &rctx);

  void handle_advance_map(cached_map_t next_map, PeeringCtx &rctx);
  void handle_activate_map(PeeringCtx &rctx);
  void handle_initialize(PeeringCtx &rctx);

  static hobject_t get_oid(const hobject_t& hobj);
  static RWState::State get_lock_type(const OpInfo &op_info);

  using load_obc_ertr = crimson::errorator<
    crimson::ct_error::enoent,
    crimson::ct_error::object_corrupted>;
  using load_obc_iertr =
    ::crimson::interruptible::interruptible_errorator<
      ::crimson::osd::IOInterruptCondition,
      load_obc_ertr>;
  using interruptor = ::crimson::interruptible::interruptor<
    ::crimson::osd::IOInterruptCondition>;

public:
  using with_obc_func_t =
    std::function<load_obc_iertr::future<> (ObjectContextRef, ObjectContextRef)>;

  load_obc_iertr::future<> with_locked_obc(
    const hobject_t &hobj,
    const OpInfo &op_info,
    with_obc_func_t&& f);

  interruptible_future<> handle_rep_op(Ref<MOSDRepOp> m);
  void update_stats(const pg_stat_t &stat);
  interruptible_future<> update_snap_map(
    const std::vector<pg_log_entry_t> &log_entries,
    ObjectStore::Transaction& t);
  void log_operation(
    std::vector<pg_log_entry_t>&& logv,
    const eversion_t &trim_to,
    const eversion_t &roll_forward_to,
    const eversion_t &min_last_complete_ondisk,
    bool transaction_applied,
    ObjectStore::Transaction &txn,
    bool async = false);
  void replica_clear_repop_obc(
    const std::vector<pg_log_entry_t> &logv);
  void handle_rep_op_reply(const MOSDRepOpReply& m);
  interruptible_future<> do_update_log_missing(
    Ref<MOSDPGUpdateLogMissing> m,
    crimson::net::ConnectionXcoreRef conn);
  interruptible_future<> do_update_log_missing_reply(
                         Ref<MOSDPGUpdateLogMissingReply> m);


  void print(std::ostream& os) const;
  void dump_primary(Formatter*);
  interruptible_future<> complete_error_log(const ceph_tid_t& rep_tid,
                                       const eversion_t& version);
  interruptible_future<eversion_t> submit_error_log(
    Ref<MOSDOp> m,
    const OpInfo &op_info,
    ObjectContextRef obc,
    const std::error_code e,
    ceph_tid_t rep_tid);
  seastar::future<> clear_temp_objects();

private:

  struct BackgroundProcessLock {
    struct Wait : OrderedConcurrentPhaseT<Wait> {
      static constexpr auto type_name = "PG::BackgroundProcessLock::wait";
    } wait;
    seastar::shared_mutex mutex;

    interruptible_future<> lock_with_op(SnapTrimEvent &st_event) noexcept;
    interruptible_future<> lock() noexcept;

    void unlock() noexcept {
      mutex.unlock();
    }
  } background_process_lock;

  using run_executer_ertr = crimson::compound_errorator_t<
    OpsExecuter::osd_op_errorator,
    crimson::errorator<
      crimson::ct_error::edquot,
      crimson::ct_error::eagain,
      crimson::ct_error::enospc
      >
    >;
  using run_executer_iertr = crimson::interruptible::interruptible_errorator<
    ::crimson::osd::IOInterruptCondition,
    run_executer_ertr>;
  using run_executer_fut = run_executer_iertr::future<>;
  run_executer_fut run_executer(
    OpsExecuter &ox,
    ObjectContextRef obc,
    const OpInfo &op_info,
    std::vector<OSDOp>& ops);

  using submit_executer_ret = std::tuple<
    interruptible_future<>,
    interruptible_future<>>;
  using submit_executer_fut = interruptible_future<
    submit_executer_ret>;
  submit_executer_fut submit_executer(
    OpsExecuter &&ox,
    const std::vector<OSDOp>& ops);

  struct do_osd_ops_params_t;

  interruptible_future<MURef<MOSDOpReply>> do_pg_ops(Ref<MOSDOp> m);
  interruptible_future<
    std::tuple<interruptible_future<>, interruptible_future<>>>
  submit_transaction(
    ObjectContextRef&& obc,
    ceph::os::Transaction&& txn,
    osd_op_params_t&& oop,
    std::vector<pg_log_entry_t>&& log_entries);
  interruptible_future<> repair_object(
    const hobject_t& oid,
    eversion_t& v);
  void check_blocklisted_obc_watchers(ObjectContextRef &obc);
  interruptible_future<seastar::stop_iteration> trim_snap(
    snapid_t to_trim,
    bool needs_pause);

private:
  PG_OSDMapGate osdmap_gate;
  ShardServices &shard_services;


public:
  cached_map_t get_osdmap() { return peering_state.get_osdmap(); }
  eversion_t get_next_version() {
    return eversion_t(get_osdmap_epoch(),
		      projected_last_update.version + 1);
  }
  ShardServices& get_shard_services() final {
    return shard_services;
  }
  seastar::future<> stop();
private:
  class C_PG_FinishRecovery : public Context {
  public:
    explicit C_PG_FinishRecovery(PG &pg) : pg(pg) {}
    void finish(int r) override;
  private:
    PG& pg;
  };
  std::unique_ptr<PGBackend> backend;
  std::unique_ptr<RecoveryBackend> recovery_backend;
  std::unique_ptr<PGRecovery> recovery_handler;
  C_PG_FinishRecovery *recovery_finisher;

  PeeringState peering_state;
  eversion_t projected_last_update;

public:
  // scrub state

  friend class ScrubScan;
  friend class ScrubFindRange;
  friend class ScrubReserveRange;
  friend class scrub::PGScrubber;
  template <typename T> friend class RemoteScrubEventBaseT;

  scrub::PGScrubber scrubber;

  void scrub_requested(scrub_level_t scrub_level, scrub_type_t scrub_type) final;

  ObjectContextRegistry obc_registry;
  ObjectContextLoader obc_loader;

private:
  OSDriver osdriver;
  SnapMapper snap_mapper;
public:
  // PeeringListener
  void publish_stats_to_osd() final;
  void clear_publish_stats() final;
  pg_stat_t get_stats() const;
  void apply_stats(
    const hobject_t &soid,
    const object_stat_sum_t &delta_stats);

private:
  std::optional<pg_stat_t> pg_stats;

public:
  OSDriver &get_osdriver() final {
    return osdriver;
  }
  SnapMapper &get_snap_mapper() final {
    return snap_mapper;
  }
  RecoveryBackend* get_recovery_backend() final {
    return recovery_backend.get();
  }
  PGRecovery* get_recovery_handler() final {
    return recovery_handler.get();
  }
  PeeringState& get_peering_state() final {
    return peering_state;
  }
  bool has_backfill_state() const {
    return (bool)(recovery_handler->backfill_state);
  }
  const BackfillState& get_backfill_state() const {
    return *recovery_handler->backfill_state;
  }
  hobject_t get_last_backfill_started() const {
    return get_backfill_state().get_last_backfill_started();
  }
  bool has_reset_since(epoch_t epoch) const final {
    return peering_state.pg_has_reset_since(epoch);
  }

  const pg_missing_tracker_t& get_local_missing() const {
    return peering_state.get_pg_log().get_missing();
  }
  epoch_t get_last_peering_reset() const final {
    return peering_state.get_last_peering_reset();
  }
  const std::set<pg_shard_t> &get_acting_recovery_backfill() const {
    return peering_state.get_acting_recovery_backfill();
  }
  bool is_backfill_target(pg_shard_t osd) const {
    return peering_state.is_backfill_target(osd);
  }
  void begin_peer_recover(pg_shard_t peer, const hobject_t oid) {
    peering_state.begin_peer_recover(peer, oid);
  }
  uint64_t min_peer_features() const {
    return peering_state.get_min_peer_features();
  }
  const std::map<hobject_t, std::set<pg_shard_t>>&
  get_missing_loc_shards() const {
    return peering_state.get_missing_loc().get_missing_locs();
  }
  const std::map<pg_shard_t, pg_missing_t> &get_shard_missing() const {
    return peering_state.get_peer_missing();
  }
  epoch_t get_interval_start_epoch() const {
    return get_info().history.same_interval_since;
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

  struct complete_op_t {
    const version_t user_version;
    const eversion_t version;
    const int err;
  };
  interruptible_future<std::optional<complete_op_t>>
  already_complete(const osd_reqid_t& reqid);
  int get_recovery_op_priority() const {
    int64_t pri = 0;
    get_pgpool().info.opts.get(pool_opts_t::RECOVERY_OP_PRIORITY, &pri);
    return  pri > 0 ? pri : crimson::common::local_conf()->osd_recovery_op_priority;
  }
  seastar::future<> mark_unfound_lost(int) {
    // TODO: see PrimaryLogPG::mark_all_unfound_lost()
    return seastar::now();
  }
  interruptible_future<> find_unfound(epoch_t epoch_started);
  bool have_unfound() const {
    return peering_state.have_unfound();
  }

  bool old_peering_msg(epoch_t reply_epoch, epoch_t query_epoch) const;

  template <typename MsgType>
  bool can_discard_replica_op(const MsgType& m) const {
    return can_discard_replica_op(m, m.get_map_epoch());
  }

  void set_pglog_based_recovery_op(PglogBasedRecovery *op) final;
  void reset_pglog_based_recovery_op() final;
  void cancel_pglog_based_recovery_op();

private:
  // instead of seastar::gate, we use a boolean flag to indicate
  // whether the system is shutting down, as we don't need to track
  // continuations here.
  bool stopping = false;

  PGActivationBlocker wait_for_active_blocker;
  PglogBasedRecovery* pglog_based_recovery_op = nullptr;

  friend std::ostream& operator<<(std::ostream&, const PG& pg);
  friend class ClientRequest;
  friend struct CommonClientRequest;
  friend class PGAdvanceMap;
  template <class T>
  friend class PeeringEvent;
  friend class RepRequest;
  friend class LogMissingRequest;
  friend class LogMissingRequestReply;
  friend class BackfillRecovery;
  friend struct PGFacade;
  friend class InternalClientRequest;
  friend class WatchTimeoutRequest;
  friend class SnapTrimEvent;
  friend class SnapTrimObjSubEvent;
private:

  void mutate_object(
    ObjectContextRef& obc,
    ceph::os::Transaction& txn,
    osd_op_params_t& osd_op_p);
  bool can_discard_replica_op(const Message& m, epoch_t m_map_epoch) const;
  bool can_discard_op(const MOSDOp& m) const;
  void context_registry_on_change();
  bool is_missing_object(const hobject_t& soid) const {
    return peering_state.get_pg_log().get_missing().get_items().count(soid);
  }
  bool is_unreadable_object(const hobject_t &oid,
			    eversion_t* v = 0) const final {
    return is_missing_object(oid) ||
      !peering_state.get_missing_loc().readable_with_acting(
	oid, get_actingset(), v);
  }
  bool is_degraded_or_backfilling_object(const hobject_t& soid) const;
  const std::set<pg_shard_t> &get_actingset() const {
    return peering_state.get_actingset();
  }

private:
  friend class IOInterruptCondition;
  struct log_update_t {
    std::set<pg_shard_t> waiting_on;
    seastar::shared_promise<> all_committed;
  };

  std::map<ceph_tid_t, log_update_t> log_entry_update_waiting_on;
  // snap trimming
  interval_set<snapid_t> snap_trimq;
};

struct PG::do_osd_ops_params_t {
  crimson::net::ConnectionXcoreRef &get_connection() const {
    return conn;
  }
  osd_reqid_t get_reqid() const {
    return reqid;
  }
  utime_t get_mtime() const {
    return mtime;
  };
  epoch_t get_map_epoch() const {
    return map_epoch;
  }
  entity_inst_t get_orig_source_inst() const {
    return orig_source_inst;
  }
  uint64_t get_features() const {
    return features;
  }
  // Only used by InternalClientRequest, no op flags
  bool has_flag(uint32_t flag) const {
    return false;
  }

  // Only used by ExecutableMessagePimpl
  entity_name_t get_source() const {
    return orig_source_inst.name;
  }

  snapid_t get_snapid() const {
    return snapid;
  }

  crimson::net::ConnectionXcoreRef &conn;
  osd_reqid_t reqid;
  utime_t mtime;
  epoch_t map_epoch;
  entity_inst_t orig_source_inst;
  uint64_t features;
  snapid_t snapid;
};

std::ostream& operator<<(std::ostream&, const PG& pg);

}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::osd::PG> : fmt::ostream_formatter {};
#endif
