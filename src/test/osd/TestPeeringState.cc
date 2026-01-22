// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library Public License as published by
 * the Free Software Foundation; either version 2, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library Public License for more details.
 *
 */

#include <memory>
#include <gtest/gtest.h>
#include "osd/PeeringState.h"
#include "osd/PGBackend.h"
#include "osd/ReplicatedBackend.h"
#include "osd/ECBackend.h"
#include "osd/OSDMap.h"
#include "osd/osd_types.h"
#include "osd/osd_perf_counters.h"
#include "common/ceph_context.h"
#include "common/ostream_temp.h"
#include "common/perf_counters_collection.h"
#include "common/WorkQueue.h"
#include "common/intrusive_timer.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "messages/MOSDPeeringOp.h"
#include "msg/Connection.h"
#include "os/ObjectStore.h"

// dout using global context and OSD subsystem
// main set OSD subsystem debug level
#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_osd

using namespace std;

//MockConnection - simple stub. Required because PeeringState needs
//to know the features of the peer OSD which sent a peering message
class MockConnection : public Connection {
 public:
  MockConnection() : Connection(g_ceph_context, nullptr) {
    set_features(CEPH_FEATURES_ALL);
  }

  bool is_connected() override {
    return true;
  }

  int send_message(Message *m) override {
    m->put();
    return 0;
  }

  void send_keepalive() override {
  }

  void mark_down() override {
  }

  void mark_disposable() override {
  }

  entity_addr_t get_peer_socket_addr() const override {
    return entity_addr_t();
  }
};

//MockLog - simple stub
class MockLog : public LoggerSinkSet {
 public:
  void debug(std::stringstream& s) final
  {
    std::cout << "\n<<debug>> " << s.str() << std::endl;
  }

  void info(std::stringstream& s) final
  {
    std::cout << "\n<<info>> " << s.str() << std::endl;
  }

  void sec(std::stringstream& s) final
  {
    std::cout << "\n<<sec>> " << s.str() << std::endl;
  }

  void warn(std::stringstream& s) final
  {
    std::cout << "\n<<warn>> " << s.str() << std::endl;
  }

  void error(std::stringstream& s) final
  {
    err_count++;
    std::cout << "\n<<error>> " << s.str() << std::endl;
  }

  OstreamTemp info() final { return OstreamTemp(CLOG_INFO, this); }
  OstreamTemp warn() final { return OstreamTemp(CLOG_WARN, this); }
  OstreamTemp error() final { return OstreamTemp(CLOG_ERROR, this); }
  OstreamTemp sec() final { return OstreamTemp(CLOG_ERROR, this); }
  OstreamTemp debug() final { return OstreamTemp(CLOG_DEBUG, this); }

  void do_log(clog_type prio, std::stringstream& ss) final
  {
    switch (prio) {
      case CLOG_DEBUG:
        debug(ss);
        break;
      case CLOG_INFO:
        info(ss);
        break;
      case CLOG_SEC:
        sec(ss);
        break;
      case CLOG_WARN:
        warn(ss);
        break;
      case CLOG_ERROR:
      default:
        error(ss);
        break;
    }
  }

  void do_log(clog_type prio, const std::string& ss) final
  {
    switch (prio) {
      case CLOG_DEBUG:
        debug() << ss;
        break;
      case CLOG_INFO:
        info() << ss;
        break;
      case CLOG_SEC:
        sec() << ss;
        break;
      case CLOG_WARN:
        warn() << ss;
        break;
      case CLOG_ERROR:
      default:
        error() << ss;
        break;
    }
  }

  virtual ~MockLog() {}

  int err_count{0};
  int expected_err_count{0};
  void set_expected_err_count(int c) { expected_err_count = c; }
};

// MockECRecPred - simple stub for IsPGRecoverablePredicate
class MockECRecPred : public IsPGRecoverablePredicate {
 public:
  MockECRecPred() {}

  bool operator()(const std::set<pg_shard_t> &_have) const override {
    return true;
  }
};

IsPGRecoverablePredicate *get_is_recoverable_predicate() {
  return new MockECRecPred();
}

// MockECReadPred - simple stub for IsPGReadablePredicate
class MockECReadPred : public IsPGReadablePredicate {
 public:
  MockECReadPred() {}
  bool operator()(const std::set<pg_shard_t> &_have) const override {
    return true;
  }
};

IsPGReadablePredicate *get_is_readable_predicate() {
  return new MockECReadPred();
}

// MockPGBackendListener - simple stub for PGBackend::Listener
class MockPGBackendListener : public PGBackend::Listener {
public:
  pg_info_t info;
  OSDMapRef osdmap;
  const pg_pool_t pool;
  PGLog log;
  DoutPrefixProvider *dpp;
  pg_shard_t pg_whoami;
  std::set<pg_shard_t> shardset;
  std::map<pg_shard_t, pg_info_t> shard_info;
  std::map<pg_shard_t, pg_missing_t> shard_missing;
  std::map<hobject_t, std::set<pg_shard_t>> missing_loc_shards;
  pg_missing_tracker_t local_missing;

  MockPGBackendListener(OSDMapRef osdmap, const pg_pool_t pi, DoutPrefixProvider *dpp, pg_shard_t pg_whoami) :
    osdmap(osdmap), pool(pi), log(g_ceph_context), dpp(dpp), pg_whoami(pg_whoami) {}

  // Debugging
  DoutPrefixProvider *get_dpp() override {
    return dpp;
  }

  // Recovery callbacks
  void on_local_recover(
    const hobject_t &oid,
    const ObjectRecoveryInfo &recovery_info,
    ObjectContextRef obc,
    bool is_delete,
    ObjectStore::Transaction *t) override {
  }

  void on_global_recover(
    const hobject_t &oid,
    const object_stat_sum_t &stat_diff,
    bool is_delete) override {
  }

  void on_peer_recover(
    pg_shard_t peer,
    const hobject_t &oid,
    const ObjectRecoveryInfo &recovery_info) override {
  }

  void begin_peer_recover(
    pg_shard_t peer,
    const hobject_t oid) override {
  }

  void apply_stats(
    const hobject_t &soid,
    const object_stat_sum_t &delta_stats) override {
  }

  void on_failed_pull(
    const std::set<pg_shard_t> &from,
    const hobject_t &soid,
    const eversion_t &v) override {
  }

  void cancel_pull(const hobject_t &soid) override {
  }

  void remove_missing_object(
    const hobject_t &oid,
    eversion_t v,
    Context *on_complete) override {
  }

  // Locking
  void pg_lock() override {}
  void pg_unlock() override {}
  void pg_add_ref() override {}
  void pg_dec_ref() override {}

  // Context wrapping
  Context *bless_context(Context *c) override {
    return c;
  }

  GenContext<ThreadPool::TPHandle&> *bless_gencontext(
    GenContext<ThreadPool::TPHandle&> *c) override {
    return c;
  }

  GenContext<ThreadPool::TPHandle&> *bless_unlocked_gencontext(
    GenContext<ThreadPool::TPHandle&> *c) override {
    return c;
  }

  // Messaging
  void send_message(int to_osd, Message *m) override {
  }

  void queue_transaction(
    ObjectStore::Transaction&& t,
    OpRequestRef op = OpRequestRef()) override {
  }

  void queue_transactions(
    std::vector<ObjectStore::Transaction>& tls,
    OpRequestRef op = OpRequestRef()) override {
  }

  epoch_t get_interval_start_epoch() const override {
    return 1;
  }

  epoch_t get_last_peering_reset_epoch() const override {
    return 1;
  }

  // Shard information
  const std::set<pg_shard_t> &get_acting_recovery_backfill_shards() const override {
    return shardset;
  }

  const std::set<pg_shard_t> &get_acting_shards() const override {
    return shardset;
  }

  const std::set<pg_shard_t> &get_backfill_shards() const override {
    return shardset;
  }

  std::ostream& gen_dbg_prefix(std::ostream& out) const override {
    return out << "MockPGBackend ";
  }

  const std::map<hobject_t, std::set<pg_shard_t>> &get_missing_loc_shards() const override {
    return missing_loc_shards;
  }

  const pg_missing_tracker_t &get_local_missing() const override {
    return local_missing;
  }

  void add_local_next_event(const pg_log_entry_t& e) override {
  }

  const std::map<pg_shard_t, pg_missing_t> &get_shard_missing() const override {
    return shard_missing;
  }

  const pg_missing_const_i &get_shard_missing(pg_shard_t peer) const override {
    return local_missing;
  }

  const std::map<pg_shard_t, pg_info_t> &get_shard_info() const override {
    return shard_info;
  }

  const PGLog &get_log() const override {
    return log;
  }

  bool pgb_is_primary() const override {
    return true;
  }

  const OSDMapRef& pgb_get_osdmap() const override {
    return osdmap;
  }

  epoch_t pgb_get_osdmap_epoch() const override {
    return osdmap->get_epoch();
  }

  const pg_info_t &get_info() const override {
    return info;
  }

  const pg_pool_t &get_pool() const override {
    return pool;
  }

  eversion_t get_pg_committed_to() const override {
    return eversion_t();
  }

  ObjectContextRef get_obc(
    const hobject_t &hoid,
    const std::map<std::string, ceph::buffer::list, std::less<>> &attrs) override {
    return ObjectContextRef();
  }

  bool try_lock_for_read(
    const hobject_t &hoid,
    ObcLockManager &manager) override {
    return true;
  }

  void release_locks(ObcLockManager &manager) override {
  }

  void op_applied(const eversion_t &applied_version) override {
  }

  bool should_send_op(pg_shard_t peer, const hobject_t &hoid) override {
    return true;
  }

  bool pg_is_undersized() const override {
    return false;
  }

  bool pg_is_repair() const override {
    return false;
  }

  void update_migration_watermark(const hobject_t &watermark) override {
  }

  std::optional<hobject_t> consider_updating_migration_watermark(
    std::set<hobject_t> &deleted) override {
    return std::nullopt;
  }

  void log_operation(
    std::vector<pg_log_entry_t>&& logv,
    const std::optional<pg_hit_set_history_t> &hset_history,
    const eversion_t &trim_to,
    const eversion_t &roll_forward_to,
    const eversion_t &pg_committed_to,
    bool transaction_applied,
    ObjectStore::Transaction &t,
    bool async = false) override {
  }

  void pgb_set_object_snap_mapping(
    const hobject_t &soid,
    const std::set<snapid_t> &snaps,
    ObjectStore::Transaction *t) override {
  }

  void pgb_clear_object_snap_mapping(
    const hobject_t &soid,
    ObjectStore::Transaction *t) override {
  }

  void update_peer_last_complete_ondisk(
    pg_shard_t fromosd,
    eversion_t lcod) override {
  }

  void update_last_complete_ondisk(eversion_t lcod) override {
  }

  void update_pct(eversion_t pct) override {
  }

  void update_stats(const pg_stat_t &stat) override {
  }

  void schedule_recovery_work(
    GenContext<ThreadPool::TPHandle&> *c,
    uint64_t cost) override {
  }

  common::intrusive_timer &get_pg_timer() override {
    ceph_abort("Not supported");
  }

  pg_shard_t whoami_shard() const override {
    return pg_whoami;
  }

  spg_t primary_spg_t() const override {
    return spg_t();
  }

  pg_shard_t primary_shard() const override {
    return pg_shard_t();
  }

  uint64_t min_peer_features() const override {
    return CEPH_FEATURES_ALL;
  }

  uint64_t min_upacting_features() const override {
    return CEPH_FEATURES_ALL;
  }

  pg_feature_vec_t get_pg_acting_features() const override {
    return pg_feature_vec_t();
  }

  hobject_t get_temp_recovery_object(
    const hobject_t& target,
    eversion_t version) override {
    return hobject_t();
  }

  void send_message_osd_cluster(
    int peer, Message *m, epoch_t from_epoch) override {
  }

  void send_message_osd_cluster(
    std::vector<std::pair<int, Message*>>& messages, epoch_t from_epoch) override {
  }

  void send_message_osd_cluster(MessageRef, Connection *con) override {
  }

  void send_message_osd_cluster(Message *m, const ConnectionRef& con) override {
  }

  void start_mon_command(
    const std::vector<std::string>& cmd, const bufferlist& inbl,
    bufferlist *outbl, std::string *outs,
    Context *onfinish) override {
  }

  ConnectionRef get_con_osd_cluster(int peer, epoch_t from_epoch) override {
    return nullptr;
  }

  entity_name_t get_cluster_msgr_name() override {
    return entity_name_t();
  }

  PerfCounters *get_logger() override {
    return nullptr;
  }

  ceph_tid_t get_tid() override {
    return 0;
  }

  OstreamTemp clog_error() override {
    return OstreamTemp(CLOG_ERROR, nullptr);
  }

  OstreamTemp clog_warn() override {
    return OstreamTemp(CLOG_WARN, nullptr);
  }

  bool check_failsafe_full() override {
    return false;
  }

  void inc_osd_stat_repaired() override {
  }

  bool pg_is_remote_backfilling() override {
    return false;
  }

  void pg_add_local_num_bytes(int64_t num_bytes) override {
  }

  void pg_sub_local_num_bytes(int64_t num_bytes) override {
  }

  void pg_add_num_bytes(int64_t num_bytes) override {
  }

  void pg_sub_num_bytes(int64_t num_bytes) override {
  }

  bool maybe_preempt_replica_scrub(const hobject_t& oid) override {
    return false;
  }

  struct ECListener *get_eclistener() override {
    return nullptr;
  }
};

// MockPGBackend - simple stub for PGBackend
class MockPGBackend : public PGBackend {
public:
  MockPGBackend(CephContext* cct, Listener *l, ObjectStore *store,
                const coll_t &coll, ObjectStore::CollectionHandle &ch)
    : PGBackend(cct, l, store, coll, ch) {}

  // Recovery operations
  RecoveryHandle *open_recovery_op() override {
    return nullptr;
  }

  void run_recovery_op(RecoveryHandle *h, int priority) override {
  }

  int recover_object(
    const hobject_t &hoid,
    eversion_t v,
    ObjectContextRef head,
    ObjectContextRef obc,
    RecoveryHandle *h) override {
    return 0;
  }

  // Message handling
  bool can_handle_while_inactive(OpRequestRef op) override {
    return false;
  }

  bool _handle_message(OpRequestRef op) override {
    return false;
  }

  void check_recovery_sources(const OSDMapRef& osdmap) override {
  }

  // State management
  void on_change() override {
  }

  void clear_recovery_state() override {
  }

  // Predicates
  IsPGRecoverablePredicate *get_is_recoverable_predicate() const override {
    return nullptr;
  }

  IsPGReadablePredicate *get_is_readable_predicate() const override {
    return nullptr;
  }

  bool get_ec_supports_crc_encode_decode() const override {
    return false;
  }

  void dump_recovery_info(ceph::Formatter *f) const override {
  }

  bool ec_can_decode(const shard_id_set &available_shards) const override {
    return false;
  }

  shard_id_map<bufferlist> ec_encode_acting_set(
    const bufferlist &in_bl) const override {
    return {0};
  }

  shard_id_map<bufferlist> ec_decode_acting_set(
    const shard_id_map<bufferlist> &shard_map, int chunk_size) const override {
    return {0};
  }

  ECUtil::stripe_info_t ec_get_sinfo() const override {
    return {0, 0, 0};
  }

  // Transaction submission
  void submit_transaction(
    const hobject_t &hoid,
    const object_stat_sum_t &delta_stats,
    const eversion_t &at_version,
    PGTransactionUPtr &&t,
    const eversion_t &trim_to,
    const eversion_t &pg_committed_to,
    std::vector<pg_log_entry_t>&& log_entries,
    std::optional<pg_hit_set_history_t> &hset_history,
    Context *on_all_commit,
    ceph_tid_t tid,
    osd_reqid_t reqid,
    OpRequestRef op) override {
  }

  void call_write_ordered(std::function<void(void)> &&cb) override {
    cb();
  }

  // Object operations
  int objects_read_sync(
    const hobject_t &hoid,
    uint64_t off,
    uint64_t len,
    uint32_t op_flags,
    ceph::buffer::list *bl) override {
    return 0;
  }

  void objects_read_async(
    const hobject_t &hoid,
    uint64_t object_size,
    const std::list<std::pair<ec_align_t,
      std::pair<ceph::buffer::list*, Context*>>> &to_read,
    Context *on_complete, bool fast_read = false) override {
  }

  bool auto_repair_supported() const override {
    return false;
  }

  uint64_t be_get_ondisk_size(uint64_t logical_size,
                              shard_id_t shard_id) const override {
    return logical_size;
  }

  int be_deep_scrub(
    const Scrub::ScrubCounterSet& io_counters,
    const hobject_t &oid,
    ScrubMap &map,
    ScrubMapBuilder &pos,
    ScrubMap::object &o) override {
    return 0;
  }
};

// MockPGLogEntryHandler
//
// This is a fully functional implementation of the PGLog::LogEntryHandler
// interface. It calls code in PGBackend to perform the requested operations
// although some of the stubs in MockPGBackend return questionable information
// about the object size so the generated ObjectStore::Transaction is probably
// not correct. The main purpose is to use partial_write to update the PWLC
// information when appending entries to the log
class MockPGLogEntryHandler : public PGLog::LogEntryHandler {
 public:
  MockPGBackend *backend;
  ObjectStore::Transaction *t;
  MockPGLogEntryHandler(MockPGBackend *backend, ObjectStore::Transaction *t) : backend(backend), t(t) {}

  // LogEntryHandler
  void remove(const hobject_t &hoid) override {
    dout(0) << "MockPGLogEntryHandler::remove " << hoid << dendl;
    backend->remove(hoid, t);
  }
  void try_stash(const hobject_t &hoid, version_t v) override {
    dout(0) << "MockPGLogEntryHandler::try_stash " << hoid << " " << v << dendl;
    backend->try_stash(hoid, v, t);
  }
  void rollback(const pg_log_entry_t &entry) override {
    dout(0) << "MockPGLogEntryHandler::rollback " << entry << dendl;
    ceph_assert(entry.can_rollback());
    backend->rollback(entry, t);
  }
  void rollforward(const pg_log_entry_t &entry) override {
    dout(0) << "MockPGLogEntryHandler::rollforward " << entry << dendl;
    backend->rollforward(entry, t);
  }
  void trim(const pg_log_entry_t &entry) override {
    dout(0) << "MockPGLogEntryHandler::trim " << entry << dendl;
    backend->trim(entry, t);
  }
  void partial_write(pg_info_t *info, eversion_t previous_version,
                      const pg_log_entry_t &entry
    ) override {
    dout(0) << "MockPGLogEntryHandler::partial_write " << entry << dendl;
    backend->partial_write(info, previous_version, entry);
  }
};

// Mock PeeringListener - stub of PeeringState::PeeringListener
// to help with testing of PeeringState. Keep track of calls
// from PeeringState and emulate PrimaryLogPG/PG functionality
// for testing purposes.
class MockPeeringListener : public PeeringState::PeeringListener {
 public:
  MockLog logger;
  PeeringState *ps;
  unique_ptr<MockPGBackendListener> backend_listener;
  coll_t coll;
  ObjectStore::CollectionHandle ch;
  unique_ptr<MockPGBackend> backend;
  PerfCounters* recoverystate_perf;
  PerfCounters* logger_perf;
  std::vector<int> next_acting;

#ifdef WITH_CRIMSON
  // Per OSD state
  std::map<int,std::list<MessageURef>> messages;
#else
  // Per OSD state
  std::map<int,std::list<MessageRef>> messages;
#endif
  std::vector<HeartbeatStampsRef> hb_stamps;
  std::list<PGPeeringEventRef> events;
  std::list<PGPeeringEventRef> stalled_events;

  // By default MockPeeringListener will add events to the event queue immediately
  // simulating the responses that PrimaryLogPG normally generates. These inject
  // booleans can change the behavior to test other code paths

  // If inject_event_stall is true then events are added to the stalled_events list
  // and the test case must manually dispatch the event
  bool inject_event_stall = false;

  // If inject_keep_preempt is true then the preempt event for a local/remote
  // reservation is added to the stalled_events list so the test case can later
  // dispatch this event to test a preempted reservation
  bool inject_keep_preempt = false;

  MockPeeringListener(OSDMapRef osdmap, const pg_pool_t pi, DoutPrefixProvider *dpp, pg_shard_t pg_whoami) {
    backend_listener = make_unique<MockPGBackendListener>(osdmap, pi, dpp, pg_whoami);
    backend = make_unique<MockPGBackend>(g_ceph_context, backend_listener.get(), nullptr, coll, ch);
    recoverystate_perf = build_recoverystate_perf(g_ceph_context);
    g_ceph_context->get_perfcounters_collection()->add(recoverystate_perf);
    logger_perf = build_osd_logger(g_ceph_context);
    g_ceph_context->get_perfcounters_collection()->add(logger_perf);
  }

  // EpochSource interface
  epoch_t get_osdmap_epoch() const override {
    return current_epoch;
  }

  // PeeringListener interface
  void prepare_write(
    pg_info_t &info,
    pg_info_t &last_written_info,
    PastIntervals &past_intervals,
    PGLog &pglog,
    bool dirty_info,
    bool dirty_big_info,
    bool need_write_epoch,
    ObjectStore::Transaction &t) override {
    prepare_write_called = true;
  }

  void scrub_requested(scrub_level_t scrub_level, scrub_type_t scrub_type) override {
    scrub_requested_called = true;
  }

  uint64_t get_snap_trimq_size() const override {
    return snap_trimq_size;
  }

#ifdef WITH_CRIMSON
  void send_cluster_message(
    int osd, MessageURef m, epoch_t epoch, bool share_map_update=false) override {
    dout(0) << "send_cluster_message to " << osd << " " << m << dendl;
    messages[osd].push_back(m);
    messages_sent++;
  }
#else
  void send_cluster_message(
    int osd, MessageRef m, epoch_t epoch, bool share_map_update=false) override {
    dout(0) << "send_cluster_message to " << osd << " " << m << dendl;
    messages[osd].push_back(m);
    messages_sent++;
  }
#endif

  void send_pg_created(pg_t pgid) override {
    pg_created_sent = true;
  }
  ceph::signedspan get_mnow() const override {
    return ceph::signedspan::zero();
  }

  HeartbeatStampsRef get_hb_stamps(int peer) override {
    if (peer >= (int)hb_stamps.size()) {
      hb_stamps.resize(peer + 1);
    }
    if (!hb_stamps[peer]) {
      hb_stamps[peer] = ceph::make_ref<HeartbeatStamps>(peer);
    }
    return hb_stamps[peer];
  }

  void schedule_renew_lease(epoch_t plr, ceph::timespan delay) override {
    renew_lease_scheduled = true;
  }

  void queue_check_readable(epoch_t lpr, ceph::timespan delay) override {
    check_readable_queued = true;
  }

  void recheck_readable() override {
    readable_rechecked = true;
  }

  unsigned get_target_pg_log_entries() const override {
    return target_pg_log_entries;
  }


  bool try_flush_or_schedule_async() override {
    return true;
  }

  void start_flush_on_transaction(ObjectStore::Transaction &t) override {
    flush_started = true;
  }

  void on_flushed() override {
    flushed = true;
  }

  void schedule_event_after(
    PGPeeringEventRef event,
    float delay) override {
    if (inject_event_stall) {
      stalled_events.push_back(std::move(event));
    } else {
      events.push_back(std::move(event));
    }
    events_scheduled++;
  }

  void request_local_background_io_reservation(
    unsigned priority,
    PGPeeringEventURef on_grant,
    PGPeeringEventURef on_preempt) override {
    if (inject_event_stall) {
      stalled_events.push_back(std::move(on_grant));
    } else {
      events.push_back(std::move(on_grant));
    }
    if (inject_keep_preempt) {
      stalled_events.push_back(std::move(on_preempt));
    }
    io_reservations_requested++;
  }

  void update_local_background_io_priority(
    unsigned priority) override {
    io_priority_updated = true;
  }

  void cancel_local_background_io_reservation() override {
    io_reservation_cancelled = true;
  }

  void request_remote_recovery_reservation(
    unsigned priority,
    PGPeeringEventURef on_grant,
    PGPeeringEventURef on_preempt) override {
    if (inject_event_stall) {
      stalled_events.push_back(std::move(on_grant));
    } else {
      events.push_back(std::move(on_grant));
    }
    if (inject_keep_preempt) {
      stalled_events.push_back(std::move(on_preempt));
    }
    remote_recovery_reservations_requested++;
  }

  void cancel_remote_recovery_reservation() override {
    remote_recovery_reservation_cancelled = true;
  }

  void schedule_event_on_commit(
    ObjectStore::Transaction &t,
    PGPeeringEventRef on_commit) override {
    if (inject_event_stall) {
      stalled_events.push_back(std::move(on_commit));
    } else {
      events.push_back(std::move(on_commit));
    }
    events_on_commit_scheduled++;
  }

  void update_heartbeat_peers(std::set<int> peers) override {
    heartbeat_peers_updated = true;
  }

  void set_probe_targets(const std::set<pg_shard_t> &probe_set) override {
    probe_targets_set = true;
  }

  void clear_probe_targets() override {
    probe_targets_cleared = true;
  }

  void queue_want_pg_temp(const std::vector<int> &wanted) override {
    pg_temp_wanted = true;
    next_acting = wanted;
  }

  void clear_want_pg_temp() override {
    pg_temp_cleared = true;
  }

  void send_pg_migrated_pool() override {
    pg_migrated_pool_sent = true;
  }

  void publish_stats_to_osd() override {
    stats_published = true;
  }

  void clear_publish_stats() override {
    stats_cleared = true;
  }

  void check_recovery_sources(const OSDMapRef& newmap) override {
    recovery_sources_checked = true;
  }

  void check_blocklisted_watchers() override {
    blocklisted_watchers_checked = true;
  }

  void clear_primary_state() override {
    primary_state_cleared = true;
  }

  void on_active_exit() override {
    active_exited = true;
  }

  void on_active_actmap() override {
    active_actmap_called = true;
  }

  void on_active_advmap(const OSDMapRef &osdmap) override {
    active_advmap_called = true;
  }

  void on_backfill_reserved() override {
    backfill_reserved = true;
  }

  void on_recovery_reserved() override {
    recovery_reserved = true;
  }

  Context *on_clean() override {
    clean_called = true;
    return nullptr;
  }

  void on_activate(interval_set<snapid_t> snaps) override {
    activate_called = true;
  }

  void on_change(ObjectStore::Transaction &t) override {
    change_called = true;
  }

  std::pair<ghobject_t, bool> do_delete_work(
    ObjectStore::Transaction &t, ghobject_t _next) override {
    delete_work_done = true;
    return std::make_pair(ghobject_t(), true);
  }

  void clear_ready_to_merge() override {
    ready_to_merge_cleared = true;
  }

  void set_not_ready_to_merge_target(pg_t pgid, pg_t src) override {
    not_ready_to_merge_target_set = true;
  }

  void set_not_ready_to_merge_source(pg_t pgid) override {
    not_ready_to_merge_source_set = true;
  }

  void set_ready_to_merge_target(eversion_t lu, epoch_t les, epoch_t lec) override {
    ready_to_merge_target_set = true;
  }

  void set_ready_to_merge_source(eversion_t lu) override {
    ready_to_merge_source_set = true;
  }

  epoch_t cluster_osdmap_trim_lower_bound() override {
    return 1;
  }

  void on_backfill_suspended() override {
    backfill_suspended = true;
  }

  void on_recovery_cancelled() override {
    recovery_cancelled = true;
  }

  void on_pool_migration_reserved() override {
    pool_migration_reserved = true;
  }

  void on_pool_migration_suspended() override {
    pool_migration_suspended = true;
  }

  bool try_reserve_recovery_space(
    int64_t primary_num_bytes,
    int64_t local_num_bytes,
    int64_t num_objects = 0) override {
    recovery_space_reserved = true;
    return true;
  }

  void unreserve_recovery_space() override {
    recovery_space_unreserved = true;
  }

  PGLog::LogEntryHandlerRef get_log_handler(
    ObjectStore::Transaction &t) override {
    return std::make_unique<MockPGLogEntryHandler>(backend.get(), &t);
  }

  void rebuild_missing_set_with_deletes(PGLog &pglog) override {
    missing_set_rebuilt = true;
  }

  PerfCounters &get_peering_perf() override {
    return *recoverystate_perf;
  }

  PerfCounters &get_perf_logger() override {
    return *logger_perf;
  }

  void log_state_enter(const char *state) override {
    state_entered = true;
  }

  void log_state_exit(
    const char *state_name, utime_t enter_time,
    uint64_t events, utime_t event_dur) override {
    state_exited = true;
  }

  void dump_recovery_info(ceph::Formatter *f) const override {
    recovery_info_dumped = true;
  }

  OstreamTemp get_clog_info() override {
    return logger.info();
  }

  OstreamTemp get_clog_error() override {
    return logger.error();
  }

  OstreamTemp get_clog_debug() override {
    return logger.debug();
  }

  void on_activate_complete(HBHandle *handle) override {
    dout(0) << __func__ << dendl;
    std::list<PGPeeringEventRef> *event_queue;
    if (inject_event_stall) {
      event_queue = &stalled_events;
    } else {
      event_queue = &events;
    }

    if (ps->needs_recovery()) {
      dout(10) << "activate not all replicas are up-to-date, queueing recovery" << dendl;
      event_queue->push_back(std::move(
          std::make_shared<PGPeeringEvent>(
            get_osdmap_epoch(),
            get_osdmap_epoch(),
            PeeringState::DoRecovery())));
    } else if (ps->needs_backfill()) {
      dout(10) << "activate queueing backfill" << dendl;
      event_queue->push_back(std::move(
          std::make_shared<PGPeeringEvent>(
            get_osdmap_epoch(),
            get_osdmap_epoch(),
            PeeringState::RequestBackfill())));
    } else if (ps->needs_pool_migration()) {
      dout(10) << "activate queueing pool migration" << dendl;
      event_queue->push_back(std::move(
          std::make_shared<PGPeeringEvent>(
            get_osdmap_epoch(),
            get_osdmap_epoch(),
            PeeringState::DoPoolMigration())));
    } else {
      dout(10) << "activate all replicas clean, no recovery" << dendl;
      event_queue->push_back(std::move(
          std::make_shared<PGPeeringEvent>(
            get_osdmap_epoch(),
            get_osdmap_epoch(),
            PeeringState::AllReplicasRecovered())));
    }
    activate_complete_called = true;
  }

  void on_activate_committed(HBHandle *handle) override {
    activate_committed_called = true;
  }

  void on_new_interval() override {
    new_interval_called = true;
  }

  void on_pool_change() override {
    pool_changed = true;
  }

  void on_role_change() override {
    role_changed = true;
  }

  void on_removal(ObjectStore::Transaction &t) override {
    removal_called = true;
  }

  // Test state tracking
  unsigned target_pg_log_entries = 100;
  bool renew_lease_scheduled = false;
  bool check_readable_queued = false;
  bool readable_rechecked = false;
  bool heartbeat_peers_updated = false;
  bool probe_targets_set = false;
  bool probe_targets_cleared = false;
  bool pg_temp_wanted = false;
  bool pg_temp_cleared = false;
  bool pg_migrated_pool_sent = false;
  bool stats_published = false;
  bool stats_cleared = false;
  bool recovery_sources_checked = false;
  bool blocklisted_watchers_checked = false;
  bool primary_state_cleared = false;
  bool delete_work_done = false;
  bool ready_to_merge_cleared = false;
  bool not_ready_to_merge_target_set = false;
  bool not_ready_to_merge_source_set = false;
  bool ready_to_merge_target_set = false;
  bool ready_to_merge_source_set = false;
  bool backfill_suspended = false;
  bool recovery_cancelled = false;
  bool pool_migration_reserved = false;
  bool pool_migration_suspended = false;
  bool recovery_space_reserved = false;
  bool recovery_space_unreserved = false;
  bool missing_set_rebuilt = false;
  bool state_entered = false;
  bool state_exited = false;
  mutable bool recovery_info_dumped = false;
  epoch_t current_epoch = 1;
  uint64_t snap_trimq_size = 0;
  bool prepare_write_called = false;
  bool scrub_requested_called = false;
  bool pg_created_sent = false;
  bool flush_started = false;
  bool flushed = false;
  bool io_priority_updated = false;
  bool io_reservation_cancelled = false;
  bool remote_recovery_reservation_cancelled = false;
  bool active_exited = false;
  bool active_actmap_called = false;
  bool active_advmap_called = false;
  bool backfill_reserved = false;
  bool backfill_cancelled = false;
  bool recovery_reserved = false;
  bool clean_called = false;
  bool activate_called = false;
  bool activate_complete_called = false;
  bool change_called = false;
  bool activate_committed_called = false;
  bool new_interval_called = false;
  bool primary_status_changed = false;
  bool pool_changed = false;
  bool role_changed = false;
  bool removal_called = false;
  bool shutdown_called = false;
  int messages_sent = 0;
  int events_scheduled = 0;
  int io_reservations_requested = 0;
  int remote_recovery_reservations_requested = 0;
  int events_on_commit_scheduled = 0;
};

// Test fixture for PeeringState tests
class PeeringStateTest : public ::testing::Test {
protected:
  std::shared_ptr<OSDMap> osdmap;
  std::vector<int> up;
  std::vector<int> acting;
  std::vector<int> up_acting;
  int up_primary;
  int acting_primary;
  uint64_t pool_id;
  int pool_size;
  osd_reqid_t reqid;

  // Per OSD state
  std::map<int,unique_ptr<PeeringState>> osd_peeringstate;
  std::map<int,unique_ptr<PeeringCtx>> osd_peeringctx;
  std::map<int,unique_ptr<MockPeeringListener>> listeners;

  // Dpp helper
  class DppHelper : public NoDoutPrefix {
    public:
    PeeringStateTest *t;
    int osd;
    int shard;
    DppHelper(CephContext *cct, unsigned subsys, PeeringStateTest *t, int osd, int shard)
    : NoDoutPrefix(cct, subsys), t(t), osd(osd), shard(shard) {}

    std::ostream& gen_prefix(std::ostream& out) const override
    {
      out << "osd " << osd << "(" << shard << "): ";
      if (t->osd_peeringstate.contains(osd)) {
        PeeringState *ps = t->osd_peeringstate[osd].get();
        out << *ps << " ";
      }
      return out;
    }
  };
  std::map<int,unique_ptr<DppHelper>> dpp;

  DoutPrefixProvider *get_dpp(int osd)
  {
    return dpp[osd].get();
  }

  PeeringState *get_ps(int osd)
  {
    return osd_peeringstate[osd].get();
  }

  bool has_ps(int osd)
  {
    return osd_peeringstate.contains(osd);
  }

  PeeringCtx *get_ctx(int osd)
  {
    return osd_peeringctx[osd].get();
  }

  MockPeeringListener *get_listener(int osd)
  {
    return listeners[osd].get();
  }

  // ============================================================================
  // Helpers for OSDMap, Pools, Epochs and Acting Set
  // ============================================================================

  // Helper to create OSDMap
  std::shared_ptr<OSDMap> setup_osdmap(int num_osds)
  {
    dout(0) << "setup_osdmap" << dendl;
    auto osdmap = std::make_shared<OSDMap>();
    uuid_d fsid;
    osdmap->build_simple(g_ceph_context, 0, fsid, num_osds);
    OSDMap::Incremental pending_inc(osdmap->get_epoch() + 1);
    pending_inc.fsid = osdmap->get_fsid();
    entity_addrvec_t sample_addrs;
    sample_addrs.v.push_back(entity_addr_t());
    uuid_d sample_uuid;
    for (int i = 0; i < num_osds; ++i) {
      sample_uuid.generate_random();
      sample_addrs.v[0].nonce = i;
      pending_inc.new_state[i] = CEPH_OSD_EXISTS | CEPH_OSD_NEW;
      pending_inc.new_up_client[i] = sample_addrs;
      pending_inc.new_up_cluster[i] = sample_addrs;
      pending_inc.new_hb_back_up[i] = sample_addrs;
      pending_inc.new_hb_front_up[i] = sample_addrs;
      pending_inc.new_weight[i] = CEPH_OSD_IN;
      pending_inc.new_uuid[i] = sample_uuid;
      // Setup osd_xinfo as PeeringState checks features
      osd_xinfo_t xi;
      xi.features = CEPH_FEATURES_ALL;
      pending_inc.new_xinfo[i] = xi;
    }
    osdmap->apply_incremental(pending_inc);
    return osdmap;
  }

  void apply_incremental(OSDMap::Incremental inc)
  {
    osdmap->apply_incremental(inc);
    for (const auto &[osd, l] : listeners) {
      l->current_epoch = osdmap->get_epoch();
    }
  }

  // Helper to create new OSDMap epoch and process up_thru and pg_temp updates
  // if_required - if set to true only generates a new epoch if up_thru or pg_temp change required
  // returns true if a new epoch was created
  bool new_epoch(bool if_required = false)
  {
    bool did_work = false;
    epoch_t e = osdmap->get_epoch();
    OSDMap::Incremental pending_inc(e + 1);
    pending_inc.fsid = osdmap->get_fsid();
    for (auto osd: up_acting) {
      if (has_ps(osd)) {
        if (get_ps(osd)->get_need_up_thru()) {
          dout(0) << "new_epoch updating up_thru for osd " << osd << dendl;
          pending_inc.new_up_thru[osd] = e;
          did_work = true;
        }
      }
      if (osd == acting_primary && get_listener(osd)) {
        MockPeeringListener *listener = get_listener(osd);
        if (listener->pg_temp_wanted) {
          dout(0) << "new_epoch updating acting set to " << listener->next_acting << dendl;
          acting = listener->next_acting;
          if (acting.empty()) {
            acting = up;
          }
          listener->pg_temp_wanted = false;
          did_work = true;
        }
      }
    }
    if (!did_work && !if_required) {
      // No need for a new epoch
      return false;
    }
    apply_incremental(pending_inc);
    return true;
  }

  // Helper to create an EC Pool
  void create_ec_pool(int k = 2, int m = 2, bool fast_ec = true)
  {
    // Create a EC pool
    pool_size = k + m;
    OSDMap::Incremental new_pool_inc(osdmap->get_epoch() + 1);
    new_pool_inc.new_pool_max = osdmap->get_pool_max();
    new_pool_inc.fsid = osdmap->get_fsid();
    pool_id = ++new_pool_inc.new_pool_max;
    pg_pool_t empty;
    auto p = new_pool_inc.get_new_pool(pool_id, &empty);
    p->size = pool_size;
    p->min_size = k; // lower than normal to allow more error-injects
    p->set_pg_num(1);
    p->set_pgp_num(1);
    p->type = pg_pool_t::TYPE_ERASURE;
    int r = osdmap->crush->add_simple_rule(
        "erasure", "default", "osd", "",
        "indep", pg_pool_t::TYPE_ERASURE,
        &cerr);
    p->crush_rule = r;
    p->set_flag(pg_pool_t::FLAG_HASHPSPOOL);
    new_pool_inc.new_pool_names[pool_id] = "pool";
    std::map<std::string, std::string> erasure_code_profile =
      {{"plugin", "isa"},
       {"technique", "reed_sol_van"},
       {"k", fmt::format("{}", k)},
       {"m", fmt::format("{}", m)},
       {"stripe_unit", "16384"}};
    osdmap->set_erasure_code_profile(
        "default",
        erasure_code_profile);
    p->erasure_code_profile =
        "default";
    p->set_flag(pg_pool_t::FLAG_EC_OVERWRITES);
    if (fast_ec) {
      p->nonprimary_shards.clear();
      for (int i = 1; i < k; i++) {
        p->nonprimary_shards.insert(shard_id_t(i));
      }
      p->set_flag(pg_pool_t::FLAG_EC_OPTIMIZATIONS);
    }
    apply_incremental(new_pool_inc);
  }

  // Create a 2nd pool as an EC pool and set up migration from the old pool
  void migrate_to_ec_pool(int k = 2, int m = 2, bool fast_ec = true)
  {
    uint64_t old_pool_id = pool_id;
    int old_pool_size = pool_size;
    create_ec_pool(k, m, fast_ec);
    OSDMap::Incremental pending_inc(osdmap->get_epoch() + 1);
    const pg_pool_t *sp = osdmap->get_pg_pool(old_pool_id);
    pg_pool_t *spi = pending_inc.get_new_pool(old_pool_id, sp);
    const pg_pool_t *tp = osdmap->get_pg_pool(pool_id);
    pg_pool_t *tpi = pending_inc.get_new_pool(pool_id, tp);
    spi->migration_src.reset();
    spi->migration_target = pool_id;
    tpi->migration_src = old_pool_id;
    tpi->migration_target.reset();
    spi->migrating_pgs.emplace(pg_t(0, old_pool_id));
    spi->lowest_migrated_pg = 1;
    apply_incremental(pending_inc);
    // Reset to the source pool
    pool_id = old_pool_id;
    pool_size = old_pool_size;
  }

  // Note: Currently the acting and up set are being maintained separatly from
  // the OSDMap which gives the test harness a little more control over choice
  // of OSDs and shards than using CRUSH. It would probably be better to use
  // pg_upmap and pg_temp in the OSDMap to control this
  // Unlike OSDMap, the test harness requires both up and acting to be set
  // even if they are the same.

  // Helper to configure up set and acting set
  void setup_up_acting()
  {
    // Simple configuration - up set = acting set = {0, 1, 2, ... }
    up.clear();
    acting.clear();
    for (int osd = 0; osd < pool_size; osd++) {
      up.push_back(osd);
      acting.push_back(osd);
      up_acting.push_back(osd);
    }
    up_primary = 0;
    acting_primary = 0;
  }

  // Helper to swap an OSD in the up and acting set
  void modify_up_acting(int offset, int osd)
  {
    up[offset] = osd;
    acting[offset] = osd;
    up_acting.push_back(osd);
    new_epoch();
  }

  // Helper - take OSD down by removing it from up/acting set
  void osd_down(int offset, int osd) {
    dout(0) << "= osd." << osd << "(" << offset << ") set down+out =" << dendl;
    ceph_assert(up[offset] == osd);
    up[offset] = pg_pool_t::pg_CRUSH_ITEM_NONE;
    ceph_assert(acting[offset] == osd);
    acting[offset] = pg_pool_t::pg_CRUSH_ITEM_NONE;
    up_acting.erase(remove(up_acting.begin(), up_acting.end(), osd), up_acting.end());
    // Mark the OSD down+out in the OSDMap
    OSDMap::Incremental pending_inc(osdmap->get_epoch() + 1);
    pending_inc.pending_osd_state_set(osd, CEPH_OSD_UP); // XORed
    pending_inc.new_weight[osd] = CEPH_OSD_OUT;
    apply_incremental(pending_inc);
  }

  // Helper - bring OSD up by adding it to up/acting set
  void osd_up(int offset, int osd) {
    dout(0) << "= osd." << osd << "(" << offset << ") set up+in =" << dendl;
    ceph_assert(up[offset] == pg_pool_t::pg_CRUSH_ITEM_NONE);
    up[offset] = osd;
    ceph_assert(acting[offset] == pg_pool_t::pg_CRUSH_ITEM_NONE);
    acting[offset] = osd;
    up_acting.push_back(osd);
    // Mark the OSD up+in in the OSD Map
    OSDMap::Incremental pending_inc(osdmap->get_epoch() + 1);
    pending_inc.pending_osd_state_set(osd, CEPH_OSD_UP);
    pending_inc.new_weight[osd] = CEPH_OSD_IN;
    apply_incremental(pending_inc);
  }

  // ============================================================================
  // Dispatchers - Send work (peering messages) to other OSDs and process queued
  // peering events. These functions emulate Messenger and the OSD Scheduler.
  // Normally dispatch_all() is called to process all the queues until they are
  // all empty, but there are more granular versions which can be used to test
  // state or inject changes after each step
  // ============================================================================

  // Dispatcher - send peering messages to other OSDs from a given OSD. Peering
  // messages are queued in message_map in PeeringCtx
  // toosd - if specified only send messages to the specified OSD
  // num_messages - if specified only send up to the specified number of messages
  bool dispatch_peering_messages( int fromosd, int toosd = -1, int num_messages = -1 )
  {
    PeeringCtx *ctx = get_ctx(fromosd);
    bool did_work = false;
    for (auto& [osd, ls] : ctx->message_map) {
      if (!osdmap->is_up(osd)) {
        dout(0) << __func__ << " skipping down osd." << osd << dendl;
        continue;
      }
      if ( toosd >= 0 && osd != toosd) {
        continue;
      }
      for (auto it = ls.begin(); it != ls.end();) {
        auto m = *it;
        it = ls.erase(it);
        MOSDPeeringOp *pm = static_cast<MOSDPeeringOp*>(m.detach());
        dout(0) << __func__ << " sending from osd." << fromosd << " to osd." << osd << " " << *pm << dendl;
        ceph_msg_header h = pm->get_header();
        h.src.num = fromosd;
        pm->set_header(h);
#if WITH_CRIMSON
        pm->set_features(CEPH_FEATURES_ALL);
#else
        ConnectionRef c = new MockConnection();
        pm->set_connection(c);
#endif
        get_ps(osd)->handle_event(PGPeeringEventRef(pm->get_event()), get_ctx(osd));
        did_work = true;
        if (num_messages > 0 && --num_messages == 0) {
          return did_work;
        }
      }
    }
    return did_work;
  }

  // Dispatch peering messages to all OSDs until all queues empty
  bool dispatch_all_peering_messages()
  {
    bool rc = false;
    bool did_work;
    do {
      did_work = false;
      for (auto osd: up_acting) {
        did_work |= dispatch_peering_messages(osd);
      }
      rc |= did_work;
    } while (did_work);
    return rc;
  }

  // Dispatcher - send clustering messages to other OSDs from a given OSD. Cluster
  // messages are sent by send_cluster_message and are queued by MockPeeringListener
  // toosd - if specified only send messages to the specified OSD
  // num_messages - if specified only send up to the specified number of messages
  bool dispatch_cluster_messages( int fromosd, int toosd = -1, int num_messages = -1 )
  {
    bool did_work = false;
    for (auto& [osd, ls] : get_listener(fromosd)->messages) {
      if (!osdmap->is_up(osd)) {
        dout(0) << __func__ << " skipping down osd." << osd << dendl;
        continue;
      }
      if ( toosd >= 0 && osd != toosd) {
        continue;
      }
      for (auto it = ls.begin(); it != ls.end();) {
        auto mr = *it;
        auto m = mr.detach();
        it = ls.erase(it);
        // TODO : Should handle messages other than MOSDPeeringOp events, however
        // for now this seems to be sufficient
        dout(0) << __func__ << " message type = " << m->get_type() << dendl;
        MOSDPeeringOp *pm = static_cast<MOSDPeeringOp*>(m);
        dout(0) << __func__ << " sending from osd." << fromosd << " to osd." << osd << " " << *pm << dendl;
        ceph_msg_header h = pm->get_header();
        h.src.num = fromosd;
        pm->set_header(h);
#if WITH_CRIMSON
        pm->set_features(CEPH_FEATURES_ALL);
#else
        ConnectionRef c = new MockConnection();
        pm->set_connection(c);
#endif
        get_ps(osd)->handle_event(PGPeeringEventRef(pm->get_event()), get_ctx(osd));
        did_work = true;
        if (num_messages > 0 && --num_messages == 0) {
          return did_work;
        }
      }
    }
    return did_work;
  }

  // Dispatch cluster messages to all OSDs until all queues empty
  bool dispatch_all_cluster_messages()
  {
    bool rc = false;
    bool did_work;
    do {
      did_work = false;
      for (auto osd: up_acting) {
        did_work |= dispatch_cluster_messages(osd);
      }
      rc |= did_work;
    } while (did_work);
    return rc;
  }

  // Dispatcher - dispatch events. Events are generated by MockPeeringListener in
  // response to some of the calls from PeeringState, this emulates the behavior
  // of PrimaryLogPg and PG
  // stalled - true if dispatching stalled events (see inject_event_stall)
  // num_events - if specified only send up to the specified number of events
  bool dispatch_events( int fromosd, bool stalled = false, int num_events = -1 )
  {
    bool did_work = false;
    auto &ls = stalled ? get_listener(fromosd)->stalled_events :
                         get_listener(fromosd)->events;
    for (auto it = ls.begin(); it != ls.end();) {
      auto evt = *it;
      it = ls.erase(it);
      dout(0) << __func__ << " event to osd." << fromosd << " " << evt->get_desc() << dendl;
      get_ps(fromosd)->handle_event(evt, get_ctx(fromosd));
      did_work = true;
      if (num_events > 0 && --num_events == 0) {
        return did_work;
      }
    }
    return did_work;
  }

  // Dispatch events to all OSDs until all queues empty
  // stalled - true if dispatching stalled events (see inject_event_stall)
  bool dispatch_all_events(bool stalled = false)
  {
    bool rc = false;
    bool did_work;
    do {
      did_work = false;
      for (auto osd: up_acting) {
        did_work |= dispatch_events(osd, stalled);
      }
      rc |= did_work;
    } while (did_work);
    return rc;
  }

  // Dispatch all types of queued work repeatedly until queues are empty
  bool dispatch_all()
  {
    bool rc = false;
    bool did_work;
    do {
      did_work = dispatch_all_peering_messages();
      did_work |= dispatch_all_cluster_messages();
      did_work |= dispatch_all_events();
      rc |= did_work;
    } while (did_work);
    return rc;
  }

  // ============================================================================
  // Factory to create PeeringState instance for a given osd and shard
  // ============================================================================

  // Helper to create a single PeeringState instance
  PeeringState *create_peering_state(int osd, int shard)
  {
    dout(0) << "= create_peering_state osd." << osd << "(" << shard <<") =" << dendl;
    pg_shard_t pg_whoami(osd, shard_id_t(shard));
    const pg_pool_t pi = *osdmap->get_pg_pool(pool_id);
    PGPool pool(osdmap, pool_id, pi, osdmap->get_pool_name(pool_id));
    dpp[osd] = make_unique<DppHelper>(g_ceph_context, dout_subsys, this, osd, shard);
    spg_t spgid = spg_t(pg_t(0, pool_id), shard_id_t(shard));
    listeners[osd] = make_unique<MockPeeringListener>(osdmap, pi, get_dpp(osd), pg_whoami);
    get_listener(osd)->current_epoch = osdmap->get_epoch();
    unique_ptr<PeeringState> ps = make_unique<PeeringState>(
      g_ceph_context,
      pg_whoami,
      spgid,
      pool,
      osdmap,
      PG_FEATURE_CLASSIC_ALL,
      get_dpp(osd),
      get_listener(osd));
    listeners[osd]->ps = ps.get();
    ps->set_backend_predicates(
      get_is_readable_predicate(),
      get_is_recoverable_predicate());
    osd_peeringstate[osd] = move(ps);
    osd_peeringctx[osd] = make_unique<PeeringCtx>();
    return get_ps(osd);
  }

  // ============================================================================
  // Helpers to test PeeringState interfaces
  // ============================================================================

  // Helper - create peering state for all osds
  void test_create_peering_state(int toosd = -1, int shard = -1)
  {
    dout(0) << "= test_create_peering_state =" << dendl;
    for (auto osd : up_acting ) {
      if (toosd != -1 && toosd != osd) {
        continue;
      }
      create_peering_state(osd, (shard == -1) ? osd : shard);
    }
  }

  // Helper - init for all osds
  void test_init(int toosd = -1, bool dne = false)
  {
    dout(0) << "= test_init =" << dendl;
    pg_history_t history;
    history.same_interval_since = osdmap->get_epoch();
    history.epoch_pool_created = osdmap->get_epoch();
    history.last_epoch_clean = osdmap->get_epoch();
    if (!dne) {
      history.epoch_created = osdmap->get_epoch();
    }
    PastIntervals past_intervals;
    ObjectStore::Transaction t;

    for (auto osd : up_acting ) {
      if (toosd != -1 && toosd != osd) {
        continue;
      }
      get_ps(osd)->init(
        (osd == acting_primary) ? 0 /* role: primary */ : 1 /* role: replica */,
        up,
        up_primary,
        acting,
        acting_primary,
        history,
        past_intervals,
        t);
    }
  }

  // Helper - init from disk for all osds
  void test_init_from_disk(int toosd = -1)
  {
    dout(0) << "= test_init_from_disk =" << dendl;

    for (auto osd : up_acting ) {
      if (toosd != -1 && toosd != osd) {
        continue;
      }
      pg_info_t info;
      spg_t spgid = spg_t(pg_t(0, pool_id), shard_id_t(0));
      info.pgid = spgid;
      PastIntervals past_intervals;

      get_ps(osd)->init_from_disk_state(
        std::move(info),
        std::move(past_intervals),
        [](PGLog &log) { return 0; });
    }
  }

  // Helper - initialize event for all osds
  void test_event_initialize(int toosd = -1)
  {
    dout(0) << "= test_event_initialize =" << dendl;
    for (auto osd : up_acting ) {
      if (toosd != -1 && toosd != osd) {
        continue;
      }
      auto evt = std::make_shared<PGPeeringEvent>(
        osdmap->get_epoch(),
        osdmap->get_epoch(),
        PeeringState::Initialize());

      get_ps(osd)->handle_event(evt, get_ctx(osd));
    }
  }

  // Helper - advance map for all osds
  void test_event_advance_map(int toosd = -1)
  {
    dout(0) << "= test_event_advance_map =" << dendl;
    for (auto osd : up_acting ) {
      if (toosd != -1 && toosd != osd) {
        continue;
      }
      get_ps(osd)->advance_map(osdmap, osdmap, up, 0, acting, 0, *(get_ctx(osd)));
      get_ps(osd)->activate_map(*(get_ctx(osd)));
    }
  }

  // Helper - activate map event for all osds
  void test_event_activate_map(int toosd = -1)
  {
    dout(0) << "= test_event_activate_map =" << dendl;
    for (auto osd : up_acting ) {
      if (toosd != -1 && toosd != osd) {
        continue;
      }
      get_ps(osd)->activate_map(*(get_ctx(osd)));
    }
  }

  // Helper - construct a pg_log_entry and call append_log for specified osds
  // By default create a full write and append the log entry to all the OSDs
  // written - if provided then create a partial write to the specified OSDs
  // osds - if provided then only append the log entry to the specified OSDs
  // update_only - if true then only update the log, do not complete it
  // do_trim - if true then trim earlier log entries (usefull for testing backfill)
  // returns eversion of the new log entry
  eversion_t test_append_log_entry(
    shard_id_set written = shard_id_set(),
    shard_id_set osds = shard_id_set(),
    bool update_only = false,
    bool do_trim = false)
  {
    if (osds.empty()) {
      int shard = 0;
      for (auto osd : acting) {
        if (osd != pg_pool_t::pg_CRUSH_ITEM_NONE) {
          osds.insert(shard_id_t(shard));
        }
        shard++;
      }
    }
    dout(0) << "= test_append_log_entry written=" << written << " osds=" << osds << " =" << dendl;

    object_t oid("foo");
    hobject_t soid(oid, oid.name, 0, 1234, pool_id, "");
    ++reqid.tid;
    eversion_t v, pv;
    version_t uv;
    v.epoch = osdmap->get_epoch();
    v.version = reqid.tid;
    if (reqid.tid > 1) {
      pv.epoch = v.epoch;
      pv.version = reqid.tid - 1;
    } else {
      pv.epoch = 0;
      pv.version = 0;
    }
    uv = v.version;
    pg_log_entry_t entry(pg_log_entry_t::MODIFY, soid, v, pv, uv, reqid, utime_t(), 0);
    entry.written_shards = written;

    for (auto osd : osds) {
      // Skips OSDs that are not written
      if (!written.empty() && !written.contains(osd)) {
        continue;
      }
      std::vector<pg_log_entry_t> entries;
      ObjectStore::Transaction t;
      entries.push_back(entry);

      eversion_t trim;
      if (do_trim) {
        trim = pv;
      }
      get_ps(int(osd))->append_log(
        std::move(entries),
        trim, // trim_to
        trim, // roll_forward_to
        trim, // pg_committed_to
        t,
        true,
        false);
      if (!update_only) {
        std::vector<pg_log_entry_t> empty;
        get_ps(int(osd))->append_log(
          std::move(empty),
          trim, // trim_to
          v, // roll_forward_to
          v, // pg_committed_to
          t,
          true,
          false);
      }
    }
    return v;
  }

  // Helper - call begin_peer_recover
  void test_begin_peer_recover(int osd, int shard)
  {
    dout(0) << "= test_begin_peer_recover " << osd << "(" << shard << ") =" << dendl;
    object_t oid("foo");
    hobject_t soid(oid, oid.name, 0, 1234, pool_id, "");
    get_ps(acting_primary)->begin_peer_recover(pg_shard_t(osd, shard_id_t(shard)), soid);
  }

  // Helper - call on_peer_recover
  void test_on_peer_recover(int osd, int shard, eversion_t v)
  {
    dout(0) << "= test_on_peer_recover " << osd << "(" << shard << ")" << v << " =" << dendl;
    object_t oid("foo");
    hobject_t soid(oid, oid.name, 0, 1234, pool_id, "");
    ObjectStore::Transaction t;
    get_ps(acting_primary)->on_peer_recover(pg_shard_t(osd, shard_id_t(shard)), soid, v);
  }

  // Helper - call recover got to indicate an object has been recovered
  void test_recover_got(int osd, eversion_t v)
  {
    dout(0) << "= test_recover_got " << osd << " " << v << " =" << dendl;
    object_t oid("foo");
    hobject_t soid(oid, oid.name, 0, 1234, pool_id, "");
    ObjectStore::Transaction t;
    get_ps(osd)->recover_got(soid, v, false, t);
  }

  // Helper - call object_recovered
  void test_object_recovered()
  {
    dout(0) << "= test_object_recovered =" << dendl;
    object_t oid("foo");
    hobject_t soid(oid, oid.name, 0, 1234, pool_id, "");
    object_stat_sum_t stat_diff;
    get_ps(acting_primary)->object_recovered(soid, stat_diff);
  }

  void test_prepare_backfill_for_missing(int osd, int shard, eversion_t version)
  {
    dout(0) << "= test_prepare_backfill_for_missing " << osd << " " << shard << " " << version << " =" << dendl;
    object_t oid("foo");
    hobject_t soid(oid, oid.name, 0, 1234, pool_id, "");
    get_ps(acting_primary)->prepare_backfill_for_missing(soid, version, {pg_shard_t(osd,shard_id_t(shard))});
  }

  void test_update_peer_last_backfill(int osd, int shard, hobject_t last_backfill)
  {
    dout(0) << "= test_update_peer_last_backfill =" << dendl;
    get_ps(acting_primary)->update_peer_last_backfill(pg_shard_t(osd, shard_id_t(shard)), last_backfill);
  }

  void test_update_backfill_progress(int osd, hobject_t last_backfill)
  {
    dout(0) << "= test_update_backfill_progress =" << dendl;
    pg_stat_t stats;
    ObjectStore::Transaction t;
    get_ps(osd)->update_backfill_progress(last_backfill, stats, false, t);
  }

  // Helper - all replicas recovered
  void test_event_all_replicas_recovered()
  {
    dout(0) << "= test_event_all_replicas_recovered =" << dendl;
    auto evt = std::make_shared<PGPeeringEvent>(
      osdmap->get_epoch(),
      osdmap->get_epoch(),
      PeeringState::AllReplicasRecovered());

    get_ps(acting_primary)->handle_event(evt, get_ctx(acting_primary));
  }

  // Helper - recovery done
  void test_event_recovery_done(int osd)
  {
    dout(0) << "= test_event_recovery_done =" << dendl;
    auto evt = std::make_shared<PGPeeringEvent>(
      osdmap->get_epoch(),
      osdmap->get_epoch(),
      RecoveryDone());

    get_ps(osd)->handle_event(evt, get_ctx(osd));
  }

  // Helper - backfilled
  void test_event_backfilled()
  {
    dout(0) << "= test_event_backfilled =" << dendl;
    auto evt = std::make_shared<PGPeeringEvent>(
      osdmap->get_epoch(),
      osdmap->get_epoch(),
      PeeringState::Backfilled());

    get_ps(acting_primary)->handle_event(evt, get_ctx(acting_primary));
  }

// Helper - migration done
  void test_event_migration_done()
  {
    dout(0) << "= test_event_migration_done =" << dendl;
    auto evt = std::make_shared<PGPeeringEvent>(
      osdmap->get_epoch(),
      osdmap->get_epoch(),
      PeeringState::PoolMigrationDone());

    get_ps(acting_primary)->handle_event(evt, get_ctx(acting_primary));
  }

  // Helper - run peering cycle with new epochs if required for upthru or pgtemp
  void test_peering()
  {
    // Full peering cycle
    while (true) {
      test_event_advance_map();
      test_event_activate_map();
      dispatch_all();
      bool did_work = new_epoch(false); // New epoch if required for upthru or pgtemp
      if (!did_work) {
        break;
      }
    }
  }

  // ============================================================================
  // Verification Helper Functions
  // ============================================================================

  // Helper - verify primary OSD is in active+clean state
  void verify_primary_active_clean(int osd)
  {
    dout(0) << "Verifying primary osd " << osd << " is active+clean" << dendl;
    auto ps = get_ps(osd);
    EXPECT_TRUE(get_listener(osd)->clean_called);
    EXPECT_TRUE(get_listener(osd)->activate_complete_called);
    EXPECT_TRUE(ps->is_clean());
    EXPECT_FALSE(ps->is_recovering());
    EXPECT_FALSE(ps->is_backfilling());
    EXPECT_FALSE(ps->is_migrating());
  }

  // Helper - verify primary OSD is in active+recovering state
  void verify_primary_active_recovering(int osd)
  {
    dout(0) << "Verifying primary osd " << osd << " is active+recovering" << dendl;
    auto ps = get_ps(osd);
    EXPECT_TRUE(get_listener(osd)->activate_complete_called);
    EXPECT_FALSE(ps->is_clean());
    EXPECT_TRUE(ps->is_recovering());
    EXPECT_FALSE(ps->is_backfilling());
    EXPECT_FALSE(ps->is_migrating());
  }

  // Helper - verify primary OSD is in active+backfilling state
  void verify_primary_active_backfilling(int osd)
  {
    dout(0) << "Verifying primary osd " << osd << " is active+backfilling" << dendl;
    auto ps = get_ps(osd);
    EXPECT_TRUE(get_listener(osd)->activate_complete_called);
    EXPECT_FALSE(ps->is_clean());
    EXPECT_FALSE(ps->is_recovering());
    EXPECT_TRUE(ps->is_backfilling());
    EXPECT_FALSE(ps->is_migrating());
  }

  // Helper - verify primary OSD is in active+migrating state
  void verify_primary_active_migrating(int osd)
  {
    dout(0) << "Verifying primary osd " << osd << " is active+migrating" << dendl;
    auto ps = get_ps(osd);
    EXPECT_TRUE(get_listener(osd)->activate_complete_called);
    EXPECT_FALSE(ps->is_clean());
    EXPECT_FALSE(ps->is_recovering());
    EXPECT_FALSE(ps->is_backfilling());
    EXPECT_TRUE(ps->is_migrating());
  }

  // Helper - verify replica OSD is activated
  void verify_replica_activated(int osd)
  {
    dout(0) << "Verifying replica osd " << osd << " is activated" << dendl;
    EXPECT_TRUE(get_listener(osd)->activate_committed_called);
  }

  // Helper - verify OSD is active and peered
  void verify_active_and_peered(int osd)
  {
    auto ps = get_ps(osd);
    EXPECT_TRUE(ps->is_active());
    EXPECT_TRUE(ps->is_peered());
  }

  // Helper - verify log state for an OSD
  void verify_log_state(int osd, const eversion_t& expected_update,
                        const eversion_t& expected_complete,
                        const eversion_t& expected_head,
                        const eversion_t& expected_tail)
  {
    auto ps = get_ps(osd);
    EXPECT_EQ(ps->get_info().last_update, expected_update);
    EXPECT_EQ(ps->get_info().last_complete, expected_complete);
    EXPECT_EQ(ps->get_pg_log().get_head(), expected_head);
    EXPECT_EQ(ps->get_pg_log().get_tail(), expected_tail);
  }

  // Helper - verify that there are no missing or unfound objects
  void verify_no_missing_or_unfound(int osd, int shard, bool check_missing_loc = true)
  {
    auto ps = get_ps(osd);
    EXPECT_FALSE(ps->have_missing());
    EXPECT_FALSE(ps->have_unfound());
    if (check_missing_loc) {
      EXPECT_EQ(ps->get_missing_loc().get_missing_locs().size(), 0);
    }
    // Primary shard peer missing state should agree with the shard
    if (osd != up_primary) {
      ps = get_ps(up_primary);
      EXPECT_EQ(ps->get_peer_missing(pg_shard_t(osd, shard_id_t(shard))).num_missing(), 0);
    }
  }

  // Helper - verify that there are missing objects but not unfound objects
  void verify_missing(int osd, int shard)
  {
    auto ps = get_ps(osd);
    EXPECT_TRUE(ps->have_missing());
    EXPECT_FALSE(ps->have_unfound());
    // Primary shard peer missing state should agree with the shard
    if (osd != up_primary) {
      auto pps = get_ps(up_primary);
      EXPECT_EQ(pps->get_peer_missing(pg_shard_t(osd, shard_id_t(shard))).num_missing(),
                ps->get_num_missing());
    }
  }

  // Helper - verify all OSDs in active+clean state with optional log checks
  void verify_all_active_clean(const eversion_t& expected_update = eversion_t(),
                                const eversion_t& expected_tail = eversion_t())
  {
    int shard = 0;
    for (auto osd: up) {
      dout(0) << "Verifying state of osd " << osd << dendl;
      if (osd == up_primary) {
        verify_primary_active_clean(osd);
      } else {
        verify_replica_activated(osd);
      }
      verify_no_missing_or_unfound(osd, shard);
      verify_active_and_peered(osd);
      verify_log_state(osd, expected_update, expected_update, expected_update, expected_tail);
      ++shard;
    }
  }

  // Helper - verify all OSDs in active+recovering state with optional log checks
  void verify_all_active_recovering(const eversion_t& expected_update,
                                     const eversion_t& expected_complete,
                                     const eversion_t& expected_complete_recovering,
                                     int recovering_osd,
                                     const eversion_t& expected_tail = eversion_t()) {
    int shard = 0;
    for (auto osd: up) {
      dout(0) << "Verifying state of osd " << osd << dendl;
      if (osd == up_primary) {
        verify_primary_active_recovering(osd);
      } else {
        verify_replica_activated(osd);
      }
      if (osd != recovering_osd) {
        verify_no_missing_or_unfound(osd, shard, osd != up_primary);
      } else {
        verify_missing(osd, shard);
      }
      verify_active_and_peered(osd);
      verify_log_state(
        osd,
        expected_update,
        (osd == recovering_osd) ? expected_complete_recovering : expected_complete,
        expected_update,
        expected_tail);
      ++shard;
    }
  }

  // Helper - verify all OSDs in active+backfilling state with log checks
  void verify_all_active_backfilling(const eversion_t& expected_update,
                                      const eversion_t& expected_tail = eversion_t())
  {
    int shard = 0;
    for (auto osd: up) {
      dout(0) << "Verifying state of osd " << osd << dendl;
      if (osd == up_primary) {
        verify_primary_active_backfilling(osd);
      } else {
        verify_replica_activated(osd);
      }
      verify_no_missing_or_unfound(osd, shard);
      verify_active_and_peered(osd);
      verify_log_state(osd, expected_update, expected_update, expected_update, expected_tail);
      ++shard;
    }
  }

  // Helper - verify all OSDs in active+migrating state with optional log checks
  void verify_all_active_migrating(const eversion_t& expected_update = eversion_t(),
                                const eversion_t& expected_tail = eversion_t())
  {
    int shard = 0;
    for (auto osd: up) {
      dout(0) << "Verifying state of osd " << osd << dendl;
      if (osd == up_primary) {
        verify_primary_active_migrating(osd);
      } else {
        verify_replica_activated(osd);
      }
      verify_no_missing_or_unfound(osd, shard);
      verify_active_and_peered(osd);
      verify_log_state(osd, expected_update, expected_update, expected_update, expected_tail);
      ++shard;
    }
  }

  // Helper - verify PWLC state
  void verify_pwlc(epoch_t e,  std::map<shard_id_t,std::pair<eversion_t, eversion_t>> pwlc, int toosd = -1)
  {
    for (auto osd: acting) {
      if (toosd != -1 && toosd != osd) {
        continue;
      }
      auto ps = get_ps(osd);
      auto info = ps->get_info();
      EXPECT_EQ(info.partial_writes_last_complete_epoch, e);
      EXPECT_EQ(info.partial_writes_last_complete, pwlc);
    }
  }

  // ============================================================================
  // GTest - Setup and Teardown
  // ============================================================================

  // GTest SetUp function - called before each test
  void SetUp() override
  {
    g_ceph_context->_log->set_max_new(0);
    dout(0) << "SetUp" << dendl;

    // Create a basic OSDMap
    osdmap = setup_osdmap(10);
    create_ec_pool();
    setup_up_acting();
  }

  // GTest TearDown function - called after each test
  void TearDown() override
  {
    osd_peeringstate.clear();
    osd_peeringctx.clear();
    listeners.clear();
    dpp.clear();
  }
};

// ============================================================================
// Test Stubs - Basic Initialization
// ============================================================================

TEST_F(PeeringStateTest, Construction) {
  dout(0) << "== Construction ==" << dendl;
  // Test basic construction of PeeringState
  test_create_peering_state(acting[0]);
  // Verify
  ASSERT_NE(get_ps(acting[0]), nullptr);
}

TEST_F(PeeringStateTest, InitFresh) {
  dout(0) << "== InitFresh ==" << dendl;
  // Test initialization of a fresh PG
  test_create_peering_state(acting[0]);
  test_init(acting[0]);
  // Verify
  EXPECT_EQ(get_ps(acting[0])->get_role(), 0);
  EXPECT_TRUE(get_ps(acting[0])->is_primary());
  EXPECT_TRUE(get_listener(acting[0])->new_interval_called);
}

TEST_F(PeeringStateTest, InitFromDisk) {
  dout(0) << "== InitFromDisk ==" << dendl;
  // Test initialization from disk state
  test_create_peering_state(acting[0]);
  test_init_from_disk(acting[0]);
  // Verify
  EXPECT_EQ(get_ps(acting[0])->get_info().pgid,
            spg_t(pg_t(0, pool_id), shard_id_t(0)));
}

// ============================================================================
// Test Stubs - State Machine Events
// ============================================================================

TEST_F(PeeringStateTest, HandleInitialize) {
  dout(0) << "== HandleInitialize ==" << dendl;
  // Test handling Initialize event
  test_create_peering_state(acting[0]);
  test_init(acting[0]);
  test_event_initialize(acting[0]);
  // Verify state transitions occurred
  EXPECT_TRUE(get_listener(acting[0])->new_interval_called);
}


TEST_F(PeeringStateTest, HandleAdvMap) {
  dout(0) << "== HandleAdvMap ==" << dendl;
  // Test handling ActMap event
  test_create_peering_state(acting[0]);
  test_init(acting[0]);
  test_event_initialize(acting[0]);
  test_event_advance_map(acting[0]);
}

TEST_F(PeeringStateTest, HandleActMap) {
  dout(0) << "== HandleActMap ==" << dendl;
  // Test handling ActMap event
  test_create_peering_state(acting[0]);
  test_init(acting[0]);
  test_event_initialize(acting[0]);
  test_event_advance_map(acting[0]);
  test_event_activate_map(acting[0]);
}

// ============================================================================
// Test Stubs - Peering Operations
// ============================================================================

TEST_F(PeeringStateTest, ChooseActing) {
  // Test choosing acting set
  test_create_peering_state(acting[0]);
  test_init(acting[0]);

  // Test that acting set is properly configured
  EXPECT_EQ(get_ps(acting[0])->get_acting().size(), acting.size());
  EXPECT_EQ(get_ps(acting[0])->get_acting_primary(), acting_primary);
}

// ============================================================================
// Test Stubs - Log Operations
// ============================================================================

TEST_F(PeeringStateTest, PGLogAccess) {
  // Test accessing PG log
  auto ps = create_peering_state(0, 0);

  const PGLog& log = ps->get_pg_log();
  EXPECT_EQ(log.get_head(), eversion_t());
}

TEST_F(PeeringStateTest, AppendLog) {
  // Test appending to PG log
  auto ps = create_peering_state(0, 0);

  std::vector<pg_log_entry_t> entries;
  ObjectStore::Transaction t;

  // Create a test log entry
  pg_log_entry_t entry;
  entry.version = eversion_t(1, 1);
  entry.op = pg_log_entry_t::MODIFY;
  entries.push_back(entry);

  ps->append_log(
    std::move(entries),
    eversion_t(),
    eversion_t(),
    eversion_t(),
    t,
    false,
    false);
}

TEST_F(PeeringStateTest, TrimLog) {
  // Test log trimming
  auto ps = create_peering_state(0, 0);

  ps->update_trim_to();
  eversion_t trim_to = ps->get_pg_trim_to();

  // Fresh PG should have no trim boundary
  EXPECT_EQ(trim_to, eversion_t());
}

// ============================================================================
// Test Stubs - Missing Objects
// ============================================================================

TEST_F(PeeringStateTest, MissingLocTracking) {
  // Test missing location tracking
  auto ps = create_peering_state(0, 0);

  const MissingLoc& missing_loc = ps->get_missing_loc();
  EXPECT_FALSE(missing_loc.have_unfound());
}

TEST_F(PeeringStateTest, RecoverGot) {
  // Test marking object as recovered
  auto ps = create_peering_state(0, 0);

  hobject_t oid;
  oid.pool = 1;
  oid.oid = "test_object";
  ObjectStore::Transaction t;
  ps->recover_got(oid, eversion_t(1, 1), false, t);
}

// ============================================================================
// Test Stubs - State Queries
// ============================================================================

TEST_F(PeeringStateTest, StateQueries) {
  // Test various state query methods
  auto ps = create_peering_state(0, 0);

  EXPECT_FALSE(ps->is_active());
  EXPECT_FALSE(ps->is_clean());
  EXPECT_FALSE(ps->is_peered());
  EXPECT_FALSE(ps->is_recovering());
  EXPECT_FALSE(ps->is_backfilling());
}

TEST_F(PeeringStateTest, RoleQueries) {
  // Test role query methods
  auto ps = create_peering_state(0, 0);

  std::vector<int> up = {0, 1, 2};
  std::vector<int> acting = {0, 1, 2};
  pg_history_t history;
  PastIntervals past_intervals;
  ObjectStore::Transaction t;

  ps->init(0, up, 0, acting, 0, history, past_intervals, t);

  EXPECT_TRUE(ps->is_primary());
  EXPECT_FALSE(ps->is_nonprimary());
  EXPECT_EQ(ps->get_role(), 0);
}

TEST_F(PeeringStateTest, ActingSetQueries) {
  // Test acting set queries
  auto ps = create_peering_state(0, 0);

  std::vector<int> up = {0, 1, 2};
  std::vector<int> acting = {0, 1, 2};
  pg_history_t history;
  PastIntervals past_intervals;
  ObjectStore::Transaction t;

  ps->init(0, up, 0, acting, 0, history, past_intervals, t);

  EXPECT_EQ(ps->get_acting().size(), 3u);
  EXPECT_EQ(ps->get_up().size(), 3u);
  EXPECT_TRUE(ps->is_acting(pg_shard_t(0, shard_id_t(0))));
}

// ============================================================================
// Test Stubs - OSDMap Operations
// ============================================================================

TEST_F(PeeringStateTest, OSDMapAccess) {
  // Test OSDMap access
  auto ps = create_peering_state(0, 0);

  const OSDMapRef& map = ps->get_osdmap();
  EXPECT_NE(map, nullptr);
  EXPECT_GT(map->get_epoch(), 0u);
}

TEST_F(PeeringStateTest, PoolInfo) {
  // Test pool information access
  auto ps = create_peering_state(0, 0);

  const PGPool& pool = ps->get_pgpool();
  EXPECT_EQ(pool.id, 1);
}

// ============================================================================
// Test Stubs - Feature Tracking
// ============================================================================

TEST_F(PeeringStateTest, FeatureTracking) {
  // Test feature vector tracking
  auto ps = create_peering_state(0, 0);

  uint64_t features = ps->get_min_peer_features();
  EXPECT_GT(features, 0u);

  uint64_t acting_features = ps->get_min_acting_features();
  EXPECT_GT(acting_features, 0u);
}

// ============================================================================
// Test Stubs - Statistics
// ============================================================================

TEST_F(PeeringStateTest, Statistics) {
  // Test statistics access
  auto ps = create_peering_state(0, 0);

  const pg_info_t& info = ps->get_info();
  EXPECT_EQ(info.stats.stats.sum.num_objects, 0u);
}

TEST_F(PeeringStateTest, UpdateStats) {
  // Test statistics updates
  auto ps = create_peering_state(0, 0);

  ps->update_stats(
    [](pg_history_t &history, pg_stat_t &stats) {
      stats.stats.sum.num_objects = 10;
      return true;
    });

  EXPECT_EQ(ps->get_info().stats.stats.sum.num_objects, 10u);
}

// ============================================================================
// Test Stubs - Deletion
// ============================================================================

TEST_F(PeeringStateTest, DeletionState) {
  // Test deletion state
  auto ps = create_peering_state(0, 0);

  EXPECT_FALSE(ps->is_deleting());
  EXPECT_FALSE(ps->is_deleted());
}

// ============================================================================
// Test Stubs - Flush Operations
// ============================================================================

TEST_F(PeeringStateTest, FlushOperations) {
  // Test flush tracking
  auto ps = create_peering_state(0, 0);

  EXPECT_FALSE(ps->needs_flush());

  ObjectStore::Transaction t;
  ps->complete_flush();
}

// ============================================================================
// Test Stubs - History and Past Intervals
// ============================================================================

TEST_F(PeeringStateTest, HistoryUpdate) {
  // Test history updates
  test_create_peering_state(acting[0]);

  pg_history_t new_history;
  new_history.epoch_created = 1;
  new_history.last_epoch_clean = 1;

  get_ps(acting[0])->update_history(new_history);
}

TEST_F(PeeringStateTest, PastIntervals) {
  // Test past intervals access
  auto ps = create_peering_state(0, 0);

  const PastIntervals& past_intervals = ps->get_past_intervals();
  EXPECT_EQ(past_intervals.size(), 0u);
}

// ============================================================================
// Multi OSD Peering Tests
// ============================================================================

// Multi-OSD test of peering for a new (empty PG) all the way to active+clean
TEST_F(PeeringStateTest, SimplePeeringToActiveClean) {
  dout(0) << "== SimplePeeringToActiveClean ==" << dendl;
  // Full peering cycle
  test_create_peering_state();
  test_init();
  test_event_initialize();
  test_event_advance_map();
  test_event_activate_map();
  dispatch_all();
  new_epoch(); // for wait_upthru
  test_event_advance_map();
  test_event_activate_map();
  dispatch_all();
  // Verify that we got to active+clean
  verify_all_active_clean();
}

// Multi-OSD test of peering for PG with 1 full write log entry all the way to active+clean
TEST_F(PeeringStateTest, FullWritePeeringToActiveClean) {
  dout(0) << "== FullWritePeeringToActiveClean ==" << dendl;
  // Init
  test_create_peering_state();
  test_init();
  test_event_initialize();
  // Append 1 log entry to all shards
  eversion_t expected = test_append_log_entry();
  // Full peering cycle
  test_event_advance_map();
  test_event_activate_map();
  dispatch_all();
  new_epoch(); // for wait_upthru
  test_event_advance_map();
  test_event_activate_map();
  dispatch_all();
  // Verify that we got to active+clean and that log entry was kept
  verify_all_active_clean(expected, eversion_t());
}

// Multi-OSD test of peering for PG with 2 log entries that are incomplete all the way to active+clean
TEST_F(PeeringStateTest, IncompleteWritePeeringToActiveClean) {
  dout(0) << "== IncompleteWritePeeringToActiveClean ==" << dendl;
  // Init
  test_create_peering_state();
  test_init();
  test_event_initialize();
  // Append 2 log entries one to just shard 0,1 and one to just shard 0
  shard_id_set written;
  shard_id_set osds1;
  osds1.insert(shard_id_t(0));
  osds1.insert(shard_id_t(1));
  test_append_log_entry(written, osds1);
  shard_id_set osds2;
  osds2.insert(shard_id_t(0));
  test_append_log_entry(written, osds2);
  // Full peering cycle
  test_event_advance_map();
  dispatch_all();
  test_event_activate_map();
  dispatch_all();
  new_epoch(); // for wait_upthru
  test_event_advance_map();
  dispatch_all();
  test_event_activate_map();
  dispatch_all();
  // Verify that we got to active+clean and that log entries got rolled backwards
  verify_all_active_clean(eversion_t(), eversion_t());
}

// Multi-OSD test of peering for PG with 1 partial write log entry all the way to active+clean
TEST_F(PeeringStateTest, PartialWritePeeringToActiveClean) {
  dout(0) << "== PartialWritePeeringToActiveClean ==" << dendl;
  // Init
  test_create_peering_state();
  test_init();
  test_event_initialize();
  // Append 1 log entry to a partial set of shards
  shard_id_set written;
  written.insert(shard_id_t(0));
  written.insert(shard_id_t(2));
  written.insert(shard_id_t(3));
  eversion_t expected = test_append_log_entry(written, written);
  // Verify that shard 1 has no PWLC and shards 0,2,3 have PWLC saying shard 1 missed writes 0'0-2'1
  {
    std::map<shard_id_t,std::pair<eversion_t, eversion_t>> expected_pwlc;
    verify_pwlc(epoch_t(), expected_pwlc, acting[1]);
    expected_pwlc[shard_id_t(1)] = std::pair(eversion_t(), expected);
    epoch_t expected_pwlc_epoch = osdmap->get_epoch();
    verify_pwlc(expected_pwlc_epoch, expected_pwlc, acting[0]);
    verify_pwlc(expected_pwlc_epoch, expected_pwlc, acting[2]);
    verify_pwlc(expected_pwlc_epoch, expected_pwlc, acting[3]);
  }
  for (int osd : acting) {
    dout(0) << osd << " PWLC=" << get_ps(osd)->get_info().partial_writes_last_complete << dendl;
  }
  // Full peering cycle
  test_event_advance_map();
  test_event_activate_map();
  dispatch_all();
  new_epoch(); // for wait_upthru
  test_event_advance_map();
  test_event_activate_map();
  dispatch_all();
  // Verify that we got to active+clean and that log entry was kept
  verify_all_active_clean(expected, eversion_t());
  // Verify all shards have PWLC saying shard 1 missed writes 0'0-2'1
  {
    std::map<shard_id_t,std::pair<eversion_t, eversion_t>> expected_pwlc;
    expected_pwlc[shard_id_t(1)] = std::pair(eversion_t(), expected);
    epoch_t expected_pwlc_epoch = osdmap->get_epoch();
    verify_pwlc(expected_pwlc_epoch, expected_pwlc);
  }
}

// Multi-OSD test of peering for PG with 1 partial write (not complete) log entry all the way to active+clean
TEST_F(PeeringStateTest, PartialWriteNotCompletePeeringToActiveClean) {
  dout(0) << "== PartialWritePeeringToActiveClean ==" << dendl;
  // Init
  test_create_peering_state();
  test_init();
  test_event_initialize();
  // Append 1 log entry to a partial set of shards but do not advance last_complete
  shard_id_set written;
  written.insert(shard_id_t(0));
  written.insert(shard_id_t(2));
  written.insert(shard_id_t(3));
  eversion_t expected = test_append_log_entry(written, written, true);
  // Verify no shards have PWLC as the write has not been completed
  {
    std::map<shard_id_t,std::pair<eversion_t, eversion_t>> expected_pwlc;
    verify_pwlc(epoch_t(0), expected_pwlc);
  }
  // Full peering cycle - the definitive shard will be OSD 1 that didn't see the partial write
  // but proc_master_log will determine that all the other shards saw the partial write and
  // will roll it forward
  test_event_advance_map();
  test_event_activate_map();
  dispatch_all();
  new_epoch(); // for wait_upthru
  test_event_advance_map();
  test_event_activate_map();
  dispatch_all();
  // Verify that we got to active+clean and that log entry was kept
  verify_all_active_clean(expected, eversion_t());
  // Verify all shards have PWLC saying shard 1 missed writes 0'0-2'1
  {
    std::map<shard_id_t,std::pair<eversion_t, eversion_t>> expected_pwlc;
    expected_pwlc[shard_id_t(1)] = std::pair(eversion_t(), expected);
    epoch_t expected_pwlc_epoch = osdmap->get_epoch();
    verify_pwlc(expected_pwlc_epoch, expected_pwlc);
  }
}

// Multi-OSD test of peering for PG with 3 full write log entries all the way to active+clean
// then change acting set and run async recovery, recover the object and all the way to active+clean
TEST_F(PeeringStateTest, AsyncRecovery) {
  dout(0) << "== AsyncRecovery ==" << dendl;
  // Init
  test_create_peering_state();
  test_init();
  test_event_initialize();
  // Append 3 log entries to all shards - enough to force async recovery
  // later on because main reduced osd_async_recovery_min_cost to 2
  test_append_log_entry();
  eversion_t previous = test_append_log_entry();
  eversion_t expected = test_append_log_entry();
  // Full peering cycle
  test_peering();
  // Verify that we got to active+clean and that log entries were kept
  verify_all_active_clean(expected, eversion_t());
  // Swap out OSD 1 for OSD 9
  modify_up_acting(1, 9);
  test_create_peering_state(9, 1);
  test_init(9);
  test_event_initialize(9);
  // Full peering cycle
  test_peering();
  // Verify that we got to active+recovering
  verify_all_active_recovering(expected, expected, previous, 9, eversion_t());
  // Now recover the object
  test_begin_peer_recover(9, 1);
  test_on_peer_recover(9, 1, expected);
  test_recover_got(9, expected);
  test_object_recovered();
  test_event_all_replicas_recovered();
  EXPECT_TRUE(new_epoch(true));
  test_peering();
  // Verify that we got to active+clean and that log entries were kept
  verify_all_active_clean(expected, eversion_t());
}

// Multi-OSD test of peering for PG with 1 full write log entry all the way to active+clean
// then change acting set and run sync recovery, recover the object and all the way to active+clean
TEST_F(PeeringStateTest, SyncRecovery) {
  dout(0) << "== SyncRecovery ==" << dendl;
  // Init
  test_create_peering_state();
  test_init();
  test_event_initialize();
  // Append 1 log entries to all shards
  eversion_t previous;
  eversion_t expected = test_append_log_entry();
  // Full peering cycle
  test_peering();
  // Verify that we got to active+clean and that log entry was kept
  verify_all_active_clean(expected, eversion_t());
  // Swap out OSD 1 for OSD 9
  modify_up_acting(1, 9);
  test_create_peering_state(9, 1);
  test_init(9);
  test_event_initialize(9);
  // Full peering cycle
  test_peering();
  // Verify that we got to active+recovering
  verify_all_active_recovering(expected, expected, previous, 9, eversion_t());
  // Now recover the object
  test_begin_peer_recover(9, 1);
  test_on_peer_recover(9, 1, expected);
  test_recover_got(9, expected);
  test_object_recovered();
  test_event_all_replicas_recovered();
  // Verify that we got to active+clean and that log entry was kept
  verify_all_active_clean(expected, eversion_t());
}

// Multi-OSD test of peering for PG with 3 full write log entries all the way to active+clean
// then change acting set and run backfill, recover the object and all the way to active+clean
TEST_F(PeeringStateTest, Backfill) {
  dout(0) << "== Backfill ==" << dendl;
  // Init
  test_create_peering_state();
  test_init();
  test_event_initialize();
  // Append 3 log entries to all shards - trim the log so that
  // backfill occurs later on
  test_append_log_entry();
  eversion_t expected_tail = test_append_log_entry();
  eversion_t expected = test_append_log_entry(shard_id_set(), shard_id_set(), false, true);
  // Full peering cycle
  test_peering();
  // Verify that we got to active+clean and that log entries were kept
  verify_all_active_clean(expected, expected_tail);
  // Swap out OSD 1 for OSD 9
  modify_up_acting(1, 9);
  test_create_peering_state(9, 1);
  test_init(9);
  test_event_initialize(9);
  // Full peering cycle
  test_peering();
  // Verify that we got to active+backfill
  verify_all_active_backfilling(expected, expected_tail);
  // Backfill the object
  test_prepare_backfill_for_missing(9, 1, expected);
  test_begin_peer_recover(9, 1);
  test_on_peer_recover(9, 1, expected);
  test_recover_got(9, expected);
  test_object_recovered();
  test_update_peer_last_backfill(9, 1, hobject_t::get_max());
  test_update_backfill_progress(9, hobject_t::get_max()); // MOSDPGBackfill::OP_BACKFILL_PROGRESS
  // Signal backfill has completed
  test_event_recovery_done(9); // MOSDPGBackfill::OP_BACKFILL_FINISH
  test_event_backfilled();
  dispatch_all();
  EXPECT_TRUE(new_epoch(true));
  test_peering();
  // Verify that we got to active+clean and that log entries were kept
  verify_all_active_clean(expected, expected_tail);
}

// Multi-OSD test of peering with pool migration all the way to active+clean
TEST_F(PeeringStateTest, PoolMigration) {
  dout(0) << "== PoolMigration ==" << dendl;
  // Configure pool migration to an EC pool
  migrate_to_ec_pool();
  // Init
  test_create_peering_state();
  test_init();
  test_event_initialize();
  // Full peering cycle
  test_peering();
  // Verify pool migration started
  verify_all_active_migrating(eversion_t(), eversion_t());
  // Signal migration complete
  test_event_migration_done();
  // Verify that we got to active+clean
  verify_all_active_clean(eversion_t(), eversion_t());
  EXPECT_TRUE(get_listener(acting[0])->pg_migrated_pool_sent);
}

// Multi-OSD test of peering for fix for https://tracker.ceph.com/issues/74218
TEST_F(PeeringStateTest, Issue74218) {
  // Scenario:
  // Partial write to OSDs 0,2,3 that has not been completed
  // OSD 2 down, peering runs and rolls-forward the partial write
  // OSD 1 down, peering runs
  // Partial write to OSDs 0,[2],3 that is completed
  // OSD 2 up, peering runs
  //   OSD 2 rolls forward the 1st write, without the fix this overwrites pwlc with a stale version
  // OSD 1 up, peering runs - make sure OSD 2 replys to GetInfo last
  //   OSD 2 provides its stale copy of pwlc
  //   primary gives stale pwlc to osd 1 which asserts
  dout(0) << "== Issue74218 ==" << dendl;
  // Init
  test_create_peering_state();
  test_init();
  test_event_initialize();
  // Partial write to OSDs 0,2,3 but do not advance last_complete
  shard_id_set written;
  written.insert(shard_id_t(0));
  written.insert(shard_id_t(2));
  written.insert(shard_id_t(3));
  eversion_t expected1 = test_append_log_entry(written, written, true);
  // Verify no shards have PWLC as the write has not been completed
  {
    std::map<shard_id_t,std::pair<eversion_t, eversion_t>> expected_pwlc;
    verify_pwlc(epoch_t(0), expected_pwlc);
  }
  // OSD 2 down, run peering
  osd_down(2, 2);
  test_peering();
  // OSD 1 down, run peering
  osd_down(1, 1);
  test_peering();
  // Partial write to OSDs 0,[2],3
  shard_id_set osds;
  osds.insert(shard_id_t(0));
  osds.insert(shard_id_t(3));
  eversion_t expected2 = test_append_log_entry(written, osds);
  // OSD 2 up, run peering
  osd_up(2, 2);
  test_peering();
  // OSD 1 up, run peering
  osd_up(1, 1);
  // Full peering cycle - but delay OSD 2's response to GetInfo
  test_event_advance_map();
  test_event_activate_map();
  dispatch_peering_messages(0); // Send query info to OSDs 1,2,3
  dispatch_peering_messages(1); // Send reply from OSD 1
  dispatch_peering_messages(3); // Send reply from OSD 3
  dispatch_peering_messages(2); // Send reply from OSD 2 - must be last
  // Run rest of peering
  dispatch_all();
  new_epoch(); // for wait_upthru
  test_event_advance_map();
  test_event_activate_map();
  dispatch_all();
  // Verify that we got to active+recovering
  verify_all_active_recovering(expected2, expected2, expected1, 2, eversion_t());
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char **argv)
{
  std::map<std::string, std::string> defaults = {
    // our map is flat, so just try and split across OSDs, not hosts or whatever
    {"osd_crush_chooseleaf_type", "0"},
    // debug level 20 for OSD so we get full logs
    {"debug_osd", "20"},
    // reduce async recovery cost to 2 to make it easier to test async recovery
    {"osd_async_recovery_min_cost","2"},
  };
  std::vector<const char*> args(argv, argv + argc);
  auto cct = global_init(&defaults,
                         args,
                         CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY,
                         CINIT_FLAG_NO_MON_CONFIG);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
