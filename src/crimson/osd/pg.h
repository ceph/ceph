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

#include "common/dout.h"
#include "crimson/net/Fwd.h"
#include "os/Transaction.h"
#include "osd/osd_types.h"
#include "osd/osd_internal_types.h"
#include "osd/PeeringState.h"

#include "crimson/common/type_helpers.h"
#include "crimson/osd/osd_operations/client_request.h"
#include "crimson/osd/osd_operations/peering_event.h"
#include "crimson/osd/shard_services.h"
#include "crimson/osd/osdmap_gate.h"

class OSDMap;
class MQuery;
class PGBackend;
class PGPeeringEvent;
namespace recovery {
  class Context;
}

namespace ceph::net {
  class Messenger;
}

namespace ceph::os {
  class FuturizedStore;
}

namespace ceph::osd {
class ClientRequest;

class PG : public boost::intrusive_ref_counter<
  PG,
  boost::thread_unsafe_counter>,
  PeeringState::PeeringListener,
  DoutPrefixProvider
{
  using ec_profile_t = std::map<std::string,std::string>;
  using cached_map_t = boost::local_shared_ptr<const OSDMap>;

  ClientRequest::PGPipeline client_request_pg_pipeline;
  PeeringEvent::PGPipeline peering_request_pg_pipeline;

  spg_t pgid;
  pg_shard_t pg_whoami;
  coll_t coll;
  ceph::os::CollectionRef coll_ref;
  ghobject_t pgmeta_oid;
public:
  PG(spg_t pgid,
     pg_shard_t pg_shard,
     pg_pool_t&& pool,
     std::string&& name,
     cached_map_t osdmap,
     ShardServices &shard_services,
     ec_profile_t profile);

  ~PG();

  const pg_shard_t& get_pg_whoami() const {
    return pg_whoami;
  }

  const spg_t& get_pgid() const {
    return pgid;
  }

  // EpochSource
  epoch_t get_osdmap_epoch() const final {
    return peering_state.get_osdmap_epoch();
  }

  // DoutPrefixProvider
  std::ostream& gen_prefix(std::ostream& out) const final {
    return out << *this;
  }
  CephContext *get_cct() const final {
    return shard_services.get_cct();
  }
  unsigned get_subsys() const final {
    return ceph_subsys_osd;
  }

  ceph::os::CollectionRef get_collection_ref() {
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
    ObjectStore::Transaction &t) final {
    std::map<string,bufferlist> km;
    if (dirty_big_info || dirty_info) {
      int ret = prepare_info_keymap(
	shard_services.get_cct(),
	&km,
	get_osdmap_epoch(),
	info,
	last_written_info,
	past_intervals,
	dirty_big_info,
	need_write_epoch,
	true,
	nullptr,
	this);
      ceph_assert(ret == 0);
    }
    pglog.write_log_and_missing(
      t, &km, coll, pgmeta_oid,
      peering_state.get_pool().info.require_rollback());
    if (!km.empty())
      t.omap_setkeys(coll, pgmeta_oid, km);
  }

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
    shard_services.send_to_osd(osd, m, epoch);
  }

  void send_pg_created(pg_t pgid) final {
    shard_services.send_pg_created(pgid);
  }

  bool try_flush_or_schedule_async() final;

  void start_flush_on_transaction(
    ObjectStore::Transaction &t) final {
    t.register_on_commit(
      new LambdaContext([this](){
	peering_state.complete_flush();
    }));
  }

  void on_flushed() final {
    // will be needed for unblocking IO operations/peering
  }

  void schedule_event_after(
    PGPeeringEventRef event,
    float delay) final {
    ceph_assert(0 == "Not implemented yet");
  }

  void request_local_background_io_reservation(
    unsigned priority,
    PGPeeringEventRef on_grant,
    PGPeeringEventRef on_preempt) final {
    ceph_assert(0 == "Not implemented yet");
  }

  void update_local_background_io_priority(
    unsigned priority) final {
    ceph_assert(0 == "Not implemented yet");
  }

  void cancel_local_background_io_reservation() final {
    // Not implemented yet, but gets called on exit() from some states
  }

  void request_remote_recovery_reservation(
    unsigned priority,
    PGPeeringEventRef on_grant,
    PGPeeringEventRef on_preempt) final {
    ceph_assert(0 == "Not implemented yet");
  }

  void cancel_remote_recovery_reservation() final {
    // Not implemented yet, but gets called on exit() from some states
  }

  void schedule_event_on_commit(
    ObjectStore::Transaction &t,
    PGPeeringEventRef on_commit) final {
    t.register_on_commit(
      new LambdaContext(
	[this, on_commit=std::move(on_commit)] {
	  shard_services.start_operation<LocalPeeringEvent>(
	    this,
	    shard_services,
	    pg_whoami,
	    pgid,
	    std::move(*on_commit));
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


  void on_pool_change() final {
    // Not needed yet
  }
  void on_role_change() final {
    // Not needed yet
  }
  void on_change(ObjectStore::Transaction &t) final {
    // Not needed yet
  }
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

  void on_removal(ObjectStore::Transaction &t) final {
    // TODO
  }
  void do_delete_work(ObjectStore::Transaction &t) final {
    // TODO
  }

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
    ceph_assert(0 == "Not implemented");
  }


  bool try_reserve_recovery_space(
    int64_t primary_num_bytes, int64_t local_num_bytes) final {
    return true;
  }
  void unreserve_recovery_space() final {}

  struct PGLogEntryHandler : public PGLog::LogEntryHandler {
    PG *pg;
    ObjectStore::Transaction *t;
    PGLogEntryHandler(PG *pg, ObjectStore::Transaction *t) : pg(pg), t(t) {}

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
    ObjectStore::Transaction &t) final {
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

  // Utility
  bool is_primary() const {
    return peering_state.is_primary();
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

  /// initialize created PG
  void init(
    ceph::os::CollectionRef coll_ref,
    int role,
    const std::vector<int>& up,
    int up_primary,
    const std::vector<int>& acting,
    int acting_primary,
    const pg_history_t& history,
    const PastIntervals& pim,
    bool backfill,
    ObjectStore::Transaction &t);

  seastar::future<> read_state(ceph::os::FuturizedStore* store);

  void do_peering_event(
    PGPeeringEvent& evt, PeeringCtx &rctx);

  void handle_advance_map(cached_map_t next_map, PeeringCtx &rctx);
  void handle_activate_map(PeeringCtx &rctx);
  void handle_initialize(PeeringCtx &rctx);
  seastar::future<> handle_op(ceph::net::Connection* conn,
			      Ref<MOSDOp> m);
  void handle_rep_op_reply(ceph::net::Connection* conn,
			   const MOSDRepOpReply& m);

  void print(std::ostream& os) const;

private:
  void do_peering_event(
    const boost::statechart::event_base &evt,
    PeeringCtx &rctx);
  seastar::future<Ref<MOSDOpReply>> do_osd_ops(Ref<MOSDOp> m);
  seastar::future<> do_osd_op(
    ObjectState& os,
    OSDOp& op,
    ceph::os::Transaction& txn);
  seastar::future<ceph::bufferlist> do_pgnls(ceph::bufferlist& indata,
					     const std::string& nspace,
					     uint64_t limit);
  seastar::future<> submit_transaction(boost::local_shared_ptr<ObjectState>&& os,
				       ceph::os::Transaction&& txn,
				       const MOSDOp& req);

private:
  OSDMapGate osdmap_gate;
  ShardServices &shard_services;

  cached_map_t osdmap;
  std::unique_ptr<PGBackend> backend;

  PeeringState peering_state;
  eversion_t projected_last_update;

  seastar::shared_promise<> active_promise;
  seastar::future<> wait_for_active();

  friend std::ostream& operator<<(std::ostream&, const PG& pg);
  friend class ClientRequest;
  friend class PeeringEvent;
  friend class PGAdvanceMap;
};

std::ostream& operator<<(std::ostream&, const PG& pg);

}
