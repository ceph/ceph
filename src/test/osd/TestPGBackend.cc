// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 IBM
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

/* This is a test harness for PGBackend implementations such as
 * EC (both classic and fast) and ReplicatedBackend. It creates
 * multiple instances of PGBackend (one per simulated OSD) each
 * with its own object store (using MemStore). For EC it also
 * sets up an ISA-L plugin. The test harness passes messages between
 * the instances so that tests can inject read and write I/Os to test
 * the backends.
 */

#include <memory>
#include <gtest/gtest.h>
#include "osd/ECSwitch.h"
#include "osd/ECBackend.h"
#include "osd/ECBackendL.h"
#include "osd/PGBackend.h"
#include "osd/OSDMap.h"
#include "osd/osd_types.h"
#include "osd/ReplicatedBackend.h"
#include "common/ceph_context.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "erasure-code/ErasureCodeInterface.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "erasure-code/isa/ErasureCodeIsa.h"
#include "os/memstore/MemStore.h"

// dout using global context and OSD subsystem
#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_osd

using namespace std;

//MockConnection - simple stub
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

// MockListener - implementing both PGBackend::Listener and ECListener
class MockListener : public PGBackend::Listener, public ECListener {
public:
  pg_info_t info;
  OSDMapRef osdmap;
  pg_pool_t pool;
  PGLog log;
  DoutPrefixProvider *dpp;
  pg_shard_t pg_whoami;
  bool ec_optimizations_enabled = false;
  std::unique_ptr<MemStore> store;
  ObjectStore::CollectionHandle ch;
  coll_t coll;
  PerfCounters* logger_perf;
  string storename;

  // Per OSD message queue - stores messages sent to other OSDs
  std::map<int, std::list<MessageRef>> messages;
  int messages_sent = 0;
  // Per OSD context queue - stores completion contexts for OS transactions
  std::list<Context *> queued_contexts;

  MockListener(OSDMapRef osdmap, const pg_pool_t &pi,
               DoutPrefixProvider *dpp, pg_shard_t pg_whoami,
               bool ec_opt = false) :
    osdmap(osdmap), pool(pi), log(g_ceph_context), dpp(dpp),
    pg_whoami(pg_whoami), ec_optimizations_enabled(ec_opt),
    coll(spg_t(pg_t(), pg_whoami.shard)) {
    // Initialize MemStore for transaction handling
    storename = "store_"s + to_string(pg_whoami.osd) + "_"s + to_string(pg_whoami.shard.id);
    string rm = "rm -r "s + storename + " 2>/dev/null"s;
    system(rm.c_str());
    EXPECT_EQ(0, mkdir(storename.c_str(), 0777));
    store = std::make_unique<MemStore>(g_ceph_context, storename.c_str());
    EXPECT_EQ(0, store->mkfs());
    EXPECT_EQ(0, store->mount());
    ch = store->create_new_collection(coll);
    ObjectStore::Transaction t;
    t.create_collection(coll, 0);
    EXPECT_EQ(0, store->queue_transaction(ch, std::move(t)));

    logger_perf = build_osd_logger(g_ceph_context);
    g_ceph_context->get_perfcounters_collection()->add(logger_perf);
  }

  ~MockListener() {
    if (store) {
      EXPECT_EQ(0, store->umount());
      string rm = "rm -r "s + storename;
      EXPECT_EQ(0, system(rm.c_str()));
    }
  }

  // Debugging
  DoutPrefixProvider *get_dpp() override {
    return dpp;
  }

  // Recovery callbacks
  void on_local_recover(const hobject_t &oid,
                        const ObjectRecoveryInfo &recovery_info,
                        ObjectContextRef obc, bool is_delete,
                        ObjectStore::Transaction *t) override {}

  void on_global_recover(const hobject_t &oid,
                         const object_stat_sum_t &stat_diff,
                         bool is_delete) override {}

  void on_peer_recover(pg_shard_t peer, const hobject_t &oid,
                       const ObjectRecoveryInfo &recovery_info) override {}

  void begin_peer_recover(pg_shard_t peer, const hobject_t oid) override {}

  void apply_stats(const hobject_t &soid,
                   const object_stat_sum_t &delta_stats) override {}

  void on_failed_pull(const std::set<pg_shard_t> &from,
                      const hobject_t &soid, const eversion_t &v) override {}

  void cancel_pull(const hobject_t &soid) override {}

  void remove_missing_object(const hobject_t &oid, eversion_t v,
                             Context *on_complete) override {}

  // Locking
  void pg_lock() override {}
  void pg_unlock() override {}
  void pg_add_ref() override {}
  void pg_dec_ref() override {}

  // Context wrapping
  Context *bless_context(Context *c) override { return c; }
  GenContext<ThreadPool::TPHandle&> *bless_gencontext(
    GenContext<ThreadPool::TPHandle&> *c) override { return c; }
  GenContext<ThreadPool::TPHandle&> *bless_unlocked_gencontext(
    GenContext<ThreadPool::TPHandle&> *c) override { return c; }

  void queue_transaction(ObjectStore::Transaction&& t,
                         OpRequestRef op = OpRequestRef()) override {
    std::vector<ObjectStore::Transaction> tls;
    tls.push_back(std::move(t));
    queue_transactions(tls, op);
  }
  void queue_transactions(std::vector<ObjectStore::Transaction>& tls,
                          OpRequestRef op = OpRequestRef()) override {
    // Forward to MemStore for actual transaction handling
    if (store && ch) {
      // Steal the Context callbacks from the transactions before calling MemStore, this
      // allows the test harness to manage the context callbacks itself instead of using
      // a Finisher thread. This keeps the test harness single threaded and gives more
      // control for ordering async replies.
      Context *on_apply = NULL;
      Context *on_apply_sync = NULL;
      Context *on_commit = NULL;
      ObjectStore::Transaction::collect_contexts(tls, &on_apply, &on_commit, &on_apply_sync);
      store->queue_transactions(ch, tls, op);
      if (on_apply_sync) {
        on_apply_sync->complete(0);
      }
      if (on_apply) {
        queued_contexts.push_back(on_apply);
      }
      if (on_commit) {
        queued_contexts.push_back(on_commit);
      }
    }
  }

  //FIXME - initialize these properly
  std::set<pg_shard_t> acting_set {pg_shard_t(0, shard_id_t(0)), pg_shard_t(1, shard_id_t(1)), pg_shard_t(2, shard_id_t(2))};
  std::set<pg_shard_t> acting_recovering_backfilling_set {pg_shard_t(0, shard_id_t(0)), pg_shard_t(1, shard_id_t(1)), pg_shard_t(2, shard_id_t(2))};
  std::set<pg_shard_t> backfilling_set;

  epoch_t get_interval_start_epoch() const override { return 1; }
  epoch_t get_last_peering_reset_epoch() const override { return 1; }

  const std::set<pg_shard_t> &get_acting_recovery_backfill_shards() const override {
    return acting_recovering_backfilling_set;
  }

  const std::set<pg_shard_t> &get_acting_shards() const override {
    return acting_set;
  }

  const std::set<pg_shard_t> &get_backfill_shards() const override {
    return backfilling_set;
  }

  std::ostream& gen_dbg_prefix(std::ostream& out) const override {
    return out;
  }

  //FIXME
  const std::map<hobject_t, std::set<pg_shard_t>> &get_missing_loc_shards() const override {
    static std::map<hobject_t, std::set<pg_shard_t>> empty;
    return empty;
  }

  //FIXME
  const pg_missing_tracker_t &get_local_missing() const override {
    static pg_missing_tracker_t empty;
    return empty;
  }

  void add_local_next_event(const pg_log_entry_t& e) override {}

  //FIXME
  const std::map<pg_shard_t, pg_missing_t> &get_shard_missing() const override {
    static std::map<pg_shard_t, pg_missing_t> empty;
    return empty;
  }

  //FIXME
  const pg_missing_const_i &get_shard_missing(pg_shard_t peer) const override {
    static pg_missing_tracker_t empty;
    return empty;
  }

  //FIXME
  const std::map<pg_shard_t, pg_info_t> &get_shard_info() const override {
    static std::map<pg_shard_t, pg_info_t> empty;
    return empty;
  }

  const PGLog &get_log() const override { return log; }
  bool pgb_is_primary() const override { return pg_whoami.shard.id == 0; } // FIXME
  const OSDMapRef& pgb_get_osdmap() const override { return osdmap; }
  epoch_t pgb_get_osdmap_epoch() const override { return osdmap->get_epoch(); }
  const pg_info_t &get_info() const override { return info; }

  const pg_pool_t &get_pool() const override {
    // Modify pool to return ec_optimizations_enabled
    const_cast<pg_pool_t&>(pool).flags = ec_optimizations_enabled ?
      (pool.flags | pg_pool_t::FLAG_EC_OPTIMIZATIONS) :
      (pool.flags & ~pg_pool_t::FLAG_EC_OPTIMIZATIONS);
    return pool;
  }

  eversion_t get_pg_committed_to() const override { return eversion_t(); }

  // FIXME
  ObjectContextRef get_obc(const hobject_t &hoid,
    const std::map<std::string, ceph::buffer::list, std::less<>> &attrs) override {
    return ObjectContextRef();
  }

  bool try_lock_for_read(const hobject_t &hoid,
                         ObcLockManager &manager) override { return true; }
  void release_locks(ObcLockManager &manager) override {}
  void op_applied(const eversion_t &applied_version) override {}

  // FIXME - might limit test coverage
  bool should_send_op(pg_shard_t peer, const hobject_t &hoid) override { return true; }

  // FIXME
  bool pg_is_undersized() const override { return false; }

  // FIXME
  bool pg_is_repair() const override { return false; }

  void log_operation(std::vector<pg_log_entry_t>&& logv,
                     const std::optional<pg_hit_set_history_t> &hset_history,
                     const eversion_t &trim_to, const eversion_t &roll_forward_to,
                     const eversion_t &pg_committed_to, bool transaction_applied,
                     ObjectStore::Transaction &t, bool async = false) override {}

  void pgb_set_object_snap_mapping(const hobject_t &soid,
                                    const std::set<snapid_t> &snaps,
                                    ObjectStore::Transaction *t) override {}

  void pgb_clear_object_snap_mapping(const hobject_t &soid,
                                      ObjectStore::Transaction *t) override {}

  void update_peer_last_complete_ondisk(pg_shard_t fromosd, eversion_t lcod) override {}
  void update_last_complete_ondisk(eversion_t lcod) override {}
  void update_pct(eversion_t pct) override {}
  void update_stats(const pg_stat_t &stat) override {}
  void schedule_recovery_work(GenContext<ThreadPool::TPHandle&> *c, uint64_t cost) override {}

  common::intrusive_timer &get_pg_timer() override {
    // Used by Replicated Backend for PCT
    ceph_abort("Not supported");
  }

  pg_shard_t whoami_shard() const override { return pg_whoami; }

  spg_t primary_spg_t() const override { return spg_t(); } // FIXME

  pg_shard_t primary_shard() const override { return pg_shard_t(0, shard_id_t(0)); } // FIXME

  uint64_t min_peer_features() const override { return CEPH_FEATURES_ALL; }

  uint64_t min_upacting_features() const override { return CEPH_FEATURES_ALL; }

  pg_feature_vec_t get_pg_acting_features() const override { return pg_feature_vec_t(PG_FEATURE_CLASSIC_ALL); }

  hobject_t get_temp_recovery_object(const hobject_t& target,
                                     eversion_t version) override {
    ostringstream ss;
    ss << "temp_recovering_" << info.pgid  // (note this includes the shardid)
      << "_" << version
      << "_" << info.history.same_interval_since
      << "_" << target.snap;
    // pgid + version + interval + snapid is unique, and short
    hobject_t hoid = target.make_temp_hobject(ss.str());
    dout(20) << __func__ << " " << hoid << dendl;
    return hoid;
  }

  void send_message(int peer, Message *m) override {
    dout(0) << "send_message to osd." << peer << " " << *m << dendl;
    messages[peer].push_back(MessageRef(m, false));
    messages_sent++;
  }

  void send_message_osd_cluster(int peer, Message *m, epoch_t from_epoch) override {
    dout(0) << "send_message_osd_cluster to osd." << peer << " " << *m << dendl;
    messages[peer].push_back(MessageRef(m, false));
    messages_sent++;
  }

  void send_message_osd_cluster(std::vector<std::pair<int, Message*>>& msgs,
                                epoch_t from_epoch) override {
    for (auto& [peer, m] : msgs) {
      dout(0) << "send_message_osd_cluster to osd." << peer << " " << *m << dendl;
      messages[peer].push_back(MessageRef(m, false));
      messages_sent++;
    }
  }

  void send_message_osd_cluster(MessageRef m, Connection *con) override {
    dout(0) << "send_message_osd_cluster (MessageRef) " << *m << dendl;
    messages[con->get_peer_id()].push_back(m);
    messages_sent++;
  }

  void send_message_osd_cluster(Message *m, const ConnectionRef& con) override {
    dout(0) << "send_message_osd_cluster (ConnectionRef) " << *m << dendl;
    messages[con->get_peer_id()].push_back(MessageRef(m, false));
    messages_sent++;
  }

  void send_message_osd_cluster(int osd, MOSDPGPush* msg, epoch_t from_epoch) override {
    dout(0) << "send_message_osd_cluster (MOSDPGPush) to osd." << osd << " " << *msg << dendl;
    messages[osd].push_back(MessageRef(msg, false));
    messages_sent++;
  }

  ConnectionRef get_con_osd_cluster(int peer, epoch_t from_epoch) override { 
    // Return connection, will be passed back to send_message_osd_cluster
    ConnectionRef c = new MockConnection();
    c->peer_id = peer;
    return c;
  }

  void start_mon_command(std::vector<std::string>&& cmd, bufferlist&& inbl,
                         bufferlist *outbl, std::string *outs, Context *onfinish) override {}

  entity_name_t get_cluster_msgr_name() override { return entity_name_t(); }

  PerfCounters *get_logger() override { return logger_perf; }

  ceph_tid_t last_tid;
  ceph_tid_t get_tid() override {
    return last_tid++;
  }

  OstreamTemp clog_error() override { return OstreamTemp(CLOG_ERROR, nullptr); }
  OstreamTemp clog_warn() override { return OstreamTemp(CLOG_WARN, nullptr); }
  bool check_failsafe_full() override { return false; }
  void inc_osd_stat_repaired() override {}
  bool pg_is_remote_backfilling() override { return false; }
  void pg_add_local_num_bytes(int64_t num_bytes) override {}
  void pg_sub_local_num_bytes(int64_t num_bytes) override {}
  void pg_add_num_bytes(int64_t num_bytes) override {}
  void pg_sub_num_bytes(int64_t num_bytes) override {}
  bool maybe_preempt_replica_scrub(const hobject_t& oid) override { return false; }
  struct ECListener *get_eclistener() override { return this; }

  const shard_id_set &get_acting_recovery_backfill_shard_id_set() const override {
    static shard_id_set shards;
    for (auto &ps : acting_recovering_backfilling_set) {
      if (ps.shard != shard_id_t::NO_SHARD) {
        shards.insert(ps.shard);
      }
    }
    return shards;
  }

  //FIXME
  const pg_missing_const_i * maybe_get_shard_missing(pg_shard_t peer) const override {
    return nullptr;
  }
  //FIXME
  const pg_info_t &get_shard_info(pg_shard_t peer) const override {
    static pg_info_t empty;
    return empty;
  }
  //FIXME
  bool is_missing_object(const hobject_t& oid) const override { return false; }

  void add_temp_obj(const hobject_t &oid) override {}
  void clear_temp_obj(const hobject_t &oid) override {}
};

// Test fixture for Backend tests
class PGBackendTest : public ::testing::Test {
protected:
  // Per Cluster state
  OSDMapRef osdmap;
  pg_pool_t pool;

  // Per OSD state
  std::map<int,unique_ptr<PGBackend>> backend;
  std::map<int,unique_ptr<ECExtentCache::LRU>> lru;
  std::map<int,ceph::ErasureCodeInterfaceRef> plugin;
  std::map<int,unique_ptr<MockListener>> listener;
  std::map<int,unique_ptr<ErasureCodeIsaTableCache>> tcache;
  std::map<int,unique_ptr<OpTracker>> op_tracker;
  // Dpp helper
  // Generate log output that includes the OSD and shard (because tests
  // simulate multiple OSDs)
  class DppHelper : public NoDoutPrefix {
    public:
    int osd;
    int shard;
    DppHelper(CephContext *cct, unsigned subsys, int osd, int shard)
    : NoDoutPrefix(cct, subsys), osd(osd), shard(shard) {}

    std::ostream& gen_prefix(std::ostream& out) const override
    {
      out << "osd " << osd << "(" << shard << "): ";
      return out;
    }
  };
  std::map<int,unique_ptr<DppHelper>> dpp;

  DoutPrefixProvider *get_dpp(int osd)
  {
    return dpp[osd].get();
  }

  PGBackend *get_backend(int osd)
  {
    return backend[osd].get();
  }

  ECExtentCache::LRU *get_lru(int osd)
  {
    return lru[osd].get();
  }

  ceph::ErasureCodeInterfaceRef get_plugin(int osd)
  {
    return plugin[osd];
  }

  MockListener *get_listener(int osd)
  {
    return listener[osd].get();
  }

  ErasureCodeIsaTableCache *get_tcache(int osd)
  {
    return tcache[osd].get();
  }

  OpTracker *get_op_tracker(int osd)
  {
    return op_tracker[osd].get();
  }

  // Helper - create a simple OSDMap
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
      osd_xinfo_t xi;
      xi.features = CEPH_FEATURES_ALL;
      pending_inc.new_xinfo[i] = xi;
    }
    osdmap->apply_incremental(pending_inc);
    return osdmap;
  }

  // Helper - setup EC backends
  void setup_ec(int k, int m, bool ec_optimizations = false, int chunk_size = 0) {
    pool.type = pg_pool_t::TYPE_ERASURE;
    pool.size = k + m;
    pool.min_size = k; // lower than normal to allow more error-injects
    pool.crush_rule = 0;
    pool.set_pg_num(1);
    pool.set_pgp_num(1);

    if (!chunk_size) {
      // Use default chunk size
      chunk_size = ec_optimizations ? 16384 : 4096;
    }
    ErasureCodeProfile profile =
      {{"plugin", "isa"},
       {"technique", "reed_sol_van"},
       {"k", fmt::format("{}", k)},
       {"m", fmt::format("{}", m)},
       {"stripe_unit", fmt::format("{}", chunk_size)}};

    for (int osd = 0; osd < k + m; osd++) {
      int shard = osd;
      dpp[osd] = make_unique<DppHelper>(g_ceph_context, dout_subsys, osd, shard);
      op_tracker[osd] = make_unique<OpTracker>(g_ceph_context, true, 1);
      tcache[osd] = make_unique<ErasureCodeIsaTableCache>();
      plugin[osd] = std::make_shared<ErasureCodeIsaDefault>( *get_tcache(osd),
        "reed_sol_van", ErasureCodeIsa::kVandermonde);
      // Initialize plugin with profile
      std::ostringstream ss;
      int ret = get_plugin(osd)->init(profile, &ss);
      if (ret != 0) {
        std::cerr << "Failed to initialize ErasureCodeIsa: " << ss.str() << std::endl;
      }
      lru[osd] = make_unique<ECExtentCache::LRU>(1024 *1024);
      listener[osd] = make_unique<MockListener>(
        osdmap, pool, get_dpp(osd), pg_shard_t(osd, shard_id_t(shard)), ec_optimizations);
      backend[osd] = make_unique<ECSwitch>(
        get_listener(osd), get_listener(osd)->coll, get_listener(osd)->ch,
        get_listener(osd)->store.get(), g_ceph_context,
        get_plugin(osd), k * chunk_size, *get_lru(osd));
    }
  }

  //Helper - setup Replicated backends
  void setup_rep(int replicas) {
    pool.type = pg_pool_t::TYPE_REPLICATED;
    pool.size = replicas;
    pool.min_size = 1; // lower than normal to allow more error-injects
    pool.crush_rule = 0;
    pool.set_pg_num(1);
    pool.set_pgp_num(1);

    for (int osd = 0; osd < replicas; osd++) {
      int shard = osd;
      dpp[osd] = make_unique<DppHelper>(g_ceph_context, dout_subsys, osd, shard);
      op_tracker[osd] = make_unique<OpTracker>(g_ceph_context, true, 1);
      listener[osd] = make_unique<MockListener>(
        osdmap, pool, get_dpp(osd), pg_shard_t(osd, shard_id_t(shard)), false);
      backend[osd] = make_unique<ReplicatedBackend>(
        get_listener(osd), get_listener(osd)->coll, get_listener(osd)->ch, get_listener(osd)->store.get(), g_ceph_context);
    }
  }

  // Dispatcher - send cluster messages to other OSDs from the listener
  // This simulates message delivery between OSDs in the test environment
  // toosd - if specified only send messages to the specified OSD
  // num_messages - if specified only send up to the specified number of messages
  // Returns true if any messages were dispatched
  bool dispatch_cluster_messages(int fromosd, int toosd = -1, int num_messages = -1) {
    bool did_work = false;

    for (auto& [osd, ls] : get_listener(fromosd)->messages) {
      if (!osdmap->is_up(osd)) {
        dout(0) << __func__ << " skipping down osd." << osd << dendl;
        continue;
      }
      if (toosd >= 0 && osd != toosd) {
        continue;
      }
      for (auto it = ls.begin(); it != ls.end();) {
        auto mr = *it;
        auto m = mr.detach();
        it = ls.erase(it);

        dout(0) << __func__ << " dispatching message type " << m->get_type()
                << " from osd." << fromosd << " to osd." << osd << dendl;
        ConnectionRef c = new MockConnection();
        c->peer_id = fromosd;
        m->set_connection(c);
        OpRequestRef op = get_op_tracker(osd)->create_request<OpRequest, Message*>(m);
        if (!get_backend(osd)->_handle_message(op)) {
          dout(0) << __func__ << " failed to handle message" << dendl;
        }
        did_work = true;
        if (num_messages > 0 && --num_messages == 0) {
          return did_work;
        }
      }
    }
    return did_work;
  }

  // Dispatch all cluster messages until all queues are empty
  bool dispatch_all_cluster_messages() {
    bool rc = false;
    bool did_work;
    do {
      did_work = false;
      for (auto && [osd, backend] : backend) {
        did_work |= dispatch_cluster_messages(osd);
      }
      rc |= did_work;
    } while (did_work);
    return rc;
  }

  // Dispatch finisher completions
  bool dispatch_finisher(int osd, int num_completions = -1, int rc = 0) {
    auto ls = get_listener(osd);
    int dispatched = 0;
    while (!ls->queued_contexts.empty() && (num_completions == -1 || dispatched < num_completions)) {
      auto ctx = ls->queued_contexts.front();
      ls->queued_contexts.pop_front();
      dout(0) << __func__ << " dispatching finisher " << ctx << " to osd." << osd << dendl;
      ctx->complete(rc);
      dispatched++;
    }
    return (dispatched > 0);
  }

  // Dispatch all finishers until all queues are empty
  bool dispatch_all_finishers() {
    bool rc = false;
    bool did_work;
    do {
      did_work = false;
      for (auto && [osd, backend] : backend) {
        did_work |= dispatch_finisher(osd);
      }
      rc |= did_work;
    } while (did_work);
    return rc;
  }

  // Dispatch all queued work
  bool dispatch_all() {
    bool rc = false;
    bool did_work;
    do {
      did_work = dispatch_all_cluster_messages();
      did_work |= dispatch_all_finishers();
      rc |= did_work;
    } while (did_work);
    return rc;
  }

  // Helper - completion Context
  struct C_Done;
  struct IO {
    C_Done *ctx;
    bool finished;
    int result;

    IO();
  };
  struct C_Done : public Context {
    IO *io;
    C_Done(IO *io) : io(io) {}

    void finish(int r) override {
      dout(0) << "Completion callback r=" << r << dendl;
      io->finished = true;
      io->result = r;
    }
  };

  // Helper - create hobject
  hobject_t make_object(std::string name = "foo"s)
  {
    object_t oid(name);
    return hobject_t(oid, oid.name, 0, 1234, 0, "");
  }

  // GTest SetUp function - called before each test
  void SetUp() override {
    g_ceph_context->_log->set_max_new(0);

    // Create a simple OSDMap
    osdmap = setup_osdmap(16);
  }

  // GTest TearDown function - called after each test
  void TearDown() override
  {
    backend.clear();
    listener.clear();
    plugin.clear();
    lru.clear();
    tcache.clear();
    for (auto && [osd, tracker] : op_tracker) {
      tracker->on_shutdown();
    }
    op_tracker.clear();
    dpp.clear();
  }
};

PGBackendTest::IO::IO() {
  ctx = new C_Done(this);
}

// ============================================================================
// Tests
// ============================================================================

TEST_F(PGBackendTest, FastEcAsyncReadEIO) {
  dout(0) << "= FastECAsyncReadEIO =" << dendl;
  setup_ec(2, 1, true); // FastEC 2+1

  //FIXME: Write helper for read I/O
  dout(0) << "== Read IO ==" << dendl;
  auto io1 = std::make_unique<IO>();
  auto io2 = std::make_unique<IO>();
  bufferlist read_bl;
  list<pair<ec_align_t, pair<bufferlist*, Context*> > > parms;
  pair<bufferlist *, Context*> ctx_pair = {&read_bl, io1->ctx};
  parms.emplace_back(ec_align_t{ 0, 4096, 0}, std::move(ctx_pair));
  hobject_t soid = make_object();
  get_backend(0)->objects_read_async(soid, 4096, parms, io2->ctx, false);
  dispatch_all();
  EXPECT_TRUE(io1->finished);
  EXPECT_EQ(io1->result, -EIO);
  EXPECT_TRUE(io2->finished);
  EXPECT_EQ(io2->result, -EIO);
}

TEST_F(PGBackendTest, FastEcWrite) {
  dout(0) << "= FastECWrite =" << dendl;
  setup_ec(2, 1, true); // FastEC 2+1

  //FIXME: Write helper for write I/O
  // Create + write object
  dout(0) << "== Write IO ==" << dendl;
  auto io = std::make_unique<IO>();
  hobject_t soid = make_object();
  object_stat_sum_t stats;
  std::vector<pg_log_entry_t> log_entries;
  std::optional<pg_hit_set_history_t> hset;
  eversion_t version(1,1);
  PGTransactionUPtr t = std::make_unique<PGTransaction>();
  bufferlist write_bl;
  write_bl.append("ABCD", 4);
  write_bl.append_zero(4096 - 4);
  t->create(soid);
  t->write(soid, 0, 4096, write_bl);
  OpRequestRef op;
  osd_reqid_t req;
  ObjectContextRef obc = std::make_unique<ObjectContext>();
  obc->obs.oi = object_info_t(soid);
  obc->obs.exists = true;
  t->add_obc(obc);
  log_entries.push_back(pg_log_entry_t(pg_log_entry_t::MODIFY, soid, version, eversion_t(), 0, req, utime_t(), 0));
  get_backend(0)->submit_transaction(soid, stats, version, std::move(t),
                                     eversion_t(), eversion_t(), std::move(log_entries),
                                     hset, io->ctx, 0, req, op);
  dispatch_all();
  EXPECT_TRUE(io->finished);
  EXPECT_EQ(io->result, 0);

  // Read it back
  dout(0) << "== Read IO ==" << dendl;
  auto io1 = std::make_unique<IO>();
  auto io2 = std::make_unique<IO>();
  bufferlist read_bl;
  list<pair<ec_align_t, pair<bufferlist*, Context*> > > parms;
  pair<bufferlist *, Context*> ctx_pair = {&read_bl, io1->ctx};
  parms.emplace_back(ec_align_t{ 0, 4096, 0}, std::move(ctx_pair));
  get_backend(0)->objects_read_async(soid, 4096, parms, io2->ctx, false);
  dispatch_all();
  EXPECT_TRUE(io1->finished);
  EXPECT_EQ(io1->result, 4096);
  EXPECT_TRUE(io2->finished);
  EXPECT_EQ(io2->result, 0);
  EXPECT_EQ(write_bl, read_bl);
}

TEST_F(PGBackendTest, ClassicEcAsyncReadEIO) {
  dout(0) << "= ClassicECAsyncReadEIO =" << dendl;
  setup_ec(2, 1, false); // ClassicEC 2+1
  dout(0) << "== Read IO ==" << dendl;
  auto io1 = std::make_unique<IO>();
  auto io2 = std::make_unique<IO>();
  bufferlist read_bl;
  list<pair<ec_align_t, pair<bufferlist*, Context*> > > parms;
  pair<bufferlist *, Context*> ctx_pair = {&read_bl, io1->ctx};
  parms.emplace_back(ec_align_t{ 0, 4096, 0}, std::move(ctx_pair));
  hobject_t soid = make_object();
  get_backend(0)->objects_read_async(soid, 4096, parms, io2->ctx, false);
  dispatch_all();
  EXPECT_TRUE(io1->finished);
  EXPECT_EQ(io1->result, -EIO);
  EXPECT_TRUE(io2->finished);
  EXPECT_EQ(io2->result, -EIO);
}

TEST_F(PGBackendTest, RepSyncReadENOENT) {
  dout(0) << "= RepSyncReadENOENT =" << dendl;
  setup_rep(3); // Replica-3
  dout(0) << "== Read IO ==" << dendl;
  bufferlist read_bl;
  hobject_t soid = make_object();
  int r = get_backend(0)->objects_read_sync(soid, 0, 4096, 0, &read_bl);
  EXPECT_EQ(r, -ENOENT);
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char **argv)
{
  std::map<std::string, std::string> defaults = {
    {"osd_crush_chooseleaf_type", "0"},
    {"debug_osd", "20"},
    {"debug_memstore","20"}
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
