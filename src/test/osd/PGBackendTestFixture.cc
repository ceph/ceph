// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "test/osd/PGBackendTestFixture.h"
#include "common/errno.h"
#include "messages/MOSDECSubOpWrite.h"
#include "messages/MOSDECSubOpWriteReply.h"
#include "messages/MOSDECSubOpRead.h"
#include "messages/MOSDECSubOpReadReply.h"
#include "messages/MOSDRepOp.h"
#include "messages/MOSDRepOpReply.h"
#include "messages/MOSDPGPush.h"
#include "messages/MOSDPGPushReply.h"

void PGBackendTestFixture::initialize_scrub_infra()
{
  scrub_listener = TestScrubBackend::create_scrub_listener(spgid, osdmap);
  snap_reader = TestScrubBackend::create_snap_reader();
}

void PGBackendTestFixture::setup_ec_pool()
{
  CephContext *cct = g_ceph_context;

  int num_osds = (num_zones > 0) ? (num_zones * (k + m)) : (k + m);

  osdmap = std::make_shared<OSDMap>();
  osdmap->set_max_osd(num_osds);

  for (int i = 0; i < num_osds; i++) {
    osdmap->set_state(i, CEPH_OSD_EXISTS);
    osdmap->set_weight(i, CEPH_OSD_OUT);
    osdmap->crush->set_item_name(i, "osd." + std::to_string(i));
  }

  // Use incremental to set OSDs as up and with proper features
  OSDMap::Incremental inc(osdmap->get_epoch() + 1);
  inc.fsid = osdmap->get_fsid();

  for (int i = 0; i < num_osds; i++) {
    inc.new_state[i] = CEPH_OSD_UP;
    inc.new_weight[i] = CEPH_OSD_IN;

    // Set up_thru to a high value to avoid WaitUpThru state during initial peering
    // The OSDMap will go through several increments (adding pools, etc.) so we need
    // up_thru to be higher than the final epoch
    inc.new_up_thru[i] = 100;

    // Set OSD features to include NAUTILUS, OCTOPUS and QUINCY server features (required for peering)
    osd_xinfo_t xinfo;
    xinfo.features = CEPH_FEATUREMASK_SERVER_NAUTILUS | CEPH_FEATUREMASK_SERVER_OCTOPUS | CEPH_FEATUREMASK_SERVER_QUINCY;
    inc.new_xinfo[i] = xinfo;
  }

  // Apply the incremental to set state, weight, and features
  // This will properly calculate up_osd_features
  osdmap->apply_incremental(inc);

  // create_ec_pool expects num_zones >= 1, so default 0 to 1
  int zones = (num_zones > 0) ? num_zones : 1;
  pg_pool_t pool = OSDMapTestHelpers::create_ec_pool(k, m, stripe_unit * k, pool_flags, pool_id, zones);
  OSDMapTestHelpers::add_pool(osdmap, pool_id, pool);

  pgid = pg_t(0, pool_id);

  std::vector<int> acting;
  for (int i = 0; i < num_osds; i++) {
    acting.push_back(i);
  }
  OSDMapTestHelpers::set_pg_acting(osdmap, pgid, acting);

  // Finalize the CRUSH map to calculate working_size
  // This is required for crush_init_workspace() to work correctly
  osdmap->crush->finalize();

  if (ec_plugin == "mock") {
    ec_impl = std::make_shared<MockErasureCode>(k, k + m);
  } else {
    ErasureCodeProfile profile;
    profile["k"] = std::to_string(k);
    profile["m"] = std::to_string(m);
    profile["plugin"] = ec_plugin;

    if (!ec_technique.empty()) {
      profile["technique"] = ec_technique;
    }

    profile["stripe_unit"] = std::to_string(stripe_unit);

    std::stringstream ss;
    // Tests are run from the build directory, so "./lib" points to the
    // erasure code plugins in the build tree rather than /usr/local/lib64/ceph/erasure-code/
    int ret = ceph::ErasureCodePluginRegistry::instance().factory(
      ec_plugin,
      "./lib",
      profile,
      &ec_impl,
      &ss);

    if (ret != 0) {
      FAIL() << "Failed to create EC plugin '" << ec_plugin << "': " << ss.str();
      return;
    }
  }

  // Create per-OSD stores and fixtures (store + LRU only — no collection here,
  // because collections are per-PG not per-OSD and belong in TestPG).
  for (int i = 0; i < num_osds; i++) {
    auto osd_fixture = std::make_unique<OsdTestFixture>(i);
    osd_fixture->store = MockStore::create(g_ceph_context, i);
    ASSERT_TRUE(osd_fixture->store);
    osd_fixture->data_dir = "";
    osd_fixture->lru = std::make_unique<ECExtentCache::LRU>(1024 * 1024 * 100);
    osd_fixtures[i] = std::move(osd_fixture);
  }

  const pg_pool_t* pool_ptr = OSDMapTestHelpers::get_pool(osdmap, pool_id);
  ceph_assert(pool_ptr != nullptr);

  // Only create TestPGs upfront if the derived class wants it
  // (e.g., TestBackendBasics which doesn't use peering)
  // ECPeeringTestFixture overrides this to return false and creates
  // TestPGs lazily in response to OSD map publication
  if (should_create_test_pgs_upfront()) {
    for (int i = 0; i < num_osds; i++) {
      auto* osd_fixture = osd_fixtures[i].get();
      spg_t shard_spgid(pgid, shard_id_t(i));
      pg_shard_t pg_whoami(i, shard_id_t(i));

      // Create TestPG and its per-PG collection (spg_t = pgid + shard_id).
      TestPG* test_pg = osd_fixture->create_pg(shard_spgid, pg_whoami);
      coll_t shard_coll(shard_spgid);
      auto shard_ch = osd_fixture->store->create_new_collection(shard_coll);
      ObjectStore::Transaction ct;
      ct.create_collection(shard_coll, 0);
      ASSERT_EQ(osd_fixture->store->queue_transaction(shard_ch, std::move(ct)), 0);
      test_pg->coll = shard_coll;
      test_pg->ch = shard_ch;

      // Create backend listener
      auto shard_listener = std::make_unique<MockPGBackendListener>(
        osdmap, pool_id, dpp.get(), pg_whoami);

      // Initialize the listener's own info.pgid so OSDMap queries work
      shard_listener->info.pgid = shard_spgid;

      for (int j = 0; j < num_osds; j++) {
        shard_listener->shardset.insert(pg_shard_t(j, shard_id_t(j)));
        shard_listener->acting_recovery_backfill_shard_id_set.insert(shard_id_t(j));

        // Initialize shard_info for each shard - required by EC backend
        pg_info_t shard_pg_info;
        shard_pg_info.pgid = spg_t(pgid, shard_id_t(j));
        shard_listener->shard_info[pg_shard_t(j, shard_id_t(j))] = shard_pg_info;

        // Initialize shard_missing for each shard - required by EC backend
        pg_missing_t shard_missing;
        shard_listener->shard_missing[pg_shard_t(j, shard_id_t(j))] = shard_missing;
      }

      shard_listener->set_store(osd_fixture->store.get(), test_pg->ch);
      shard_listener->set_event_loop(event_loop.get());

      // Create EC backend using the TestPG's collection
      auto shard_ec_switch = std::make_unique<ECSwitch>(
        shard_listener.get(), test_pg->coll, test_pg->ch, osd_fixture->store.get(),
        cct, ec_impl, stripe_unit * k, *osd_fixture->lru);

      // Store in TestPG
      test_pg->backend_listener = std::move(shard_listener);
      test_pg->backend = std::move(shard_ec_switch);
    }
  }

  // Create MockMessenger and register a single handler that routes to backends
  messenger = std::make_unique<MockMessenger>(event_loop.get(), cct);
  
  // Set up epoch getter for MockMessenger to enable epoch-based message filtering
  messenger->set_epoch_getter([this](int osd) -> epoch_t {
    // Get the epoch from the OSD fixture's backend listener
    auto* osd_fixture = get_osd_fixture(osd);
    if (osd_fixture) {
      // FIXME: Assumes osd == shard in current test harness
      TestPG* test_pg = get_test_pg(osd, osd);
      if (test_pg && test_pg->has_backend()) {
        return test_pg->get_backend_listener()->pgb_get_osdmap_epoch();
      }
    }
    // If listener doesn't exist yet, use the test fixture's osdmap
    return osdmap->get_epoch();
  });
  
  // Set TestPG getter - MockMessenger extracts spg_t and calls this to look up TestPG
  messenger->set_test_pg_getter([this](int osd, spg_t spgid) -> TestPG* {
    return get_test_pg(osd, spgid);
  });
  
  // Create an OpTracker for wrapping messages in OpRequestRef
  // This is needed because PGBackend::_handle_message expects OpRequestRef
  // Store as member variable so it can be properly shut down in TearDown()
  op_tracker = std::make_shared<OpTracker>(cct, true, 1);
  
  // Helper lambda to create a typed handler that wraps messages and routes to
  // the correct OSD's backend.  Uses to_osd (not get_test_pg() which always
  // returns the primary) so that CRUSH-placed pools where shard N may live on
  // any OSD are handled correctly.
  auto make_backend_handler = [this]<typename MsgType>(int msg_type) {
    messenger->register_typed_handler<MsgType>(msg_type,
      [this](int from_osd, int to_osd, boost::intrusive_ptr<MsgType> m) -> bool {
        TestPG* test_pg = get_first_test_pg_for_osd(to_osd);
        ceph_assert(test_pg != nullptr && test_pg->has_backend());
        // OpRequest stores Message* and put()s in its destructor.  Use
        // m.detach() to transfer the +1 refcount to the raw pointer
        // OpRequest takes ownership of, so the lifetime balances without
        // any explicit refcount manipulation.
        OpRequestRef op =
            this->op_tracker->create_request<OpRequest, Message*>(m.detach());
        return test_pg->get_backend()->_handle_message(op);
      });
  };

  // Register typed handlers for all EC message types
  make_backend_handler.template operator()<MOSDECSubOpWrite>(MSG_OSD_EC_WRITE);
  make_backend_handler.template operator()<MOSDECSubOpWriteReply>(MSG_OSD_EC_WRITE_REPLY);
  make_backend_handler.template operator()<MOSDECSubOpRead>(MSG_OSD_EC_READ);
  make_backend_handler.template operator()<MOSDECSubOpReadReply>(MSG_OSD_EC_READ_REPLY);
  make_backend_handler.template operator()<MOSDPGPush>(MSG_OSD_PG_PUSH);
  make_backend_handler.template operator()<MOSDPGPushReply>(MSG_OSD_PG_PUSH_REPLY);

  // Set messenger on all TestPGs that were created upfront
  // (ECPeeringTestFixture will set messenger when TestPGs are created lazily)
  if (should_create_test_pgs_upfront()) {
    for (int i = 0; i < num_osds; i++) {
      TestPG* test_pg = get_first_test_pg_for_osd(i);
      if (test_pg && test_pg->has_backend()) {
        test_pg->get_backend_listener()->set_messenger(messenger.get());
      }
    }
  }
}

void PGBackendTestFixture::setup_replicated_pool()
{
  CephContext *cct = g_ceph_context;

  osdmap = std::make_shared<OSDMap>();
  osdmap->set_max_osd(num_replicas);
  osdmap->set_state(0, CEPH_OSD_EXISTS | CEPH_OSD_UP);

  pg_pool_t pool;
  pool.type = pg_pool_t::TYPE_REPLICATED;
  pool.size = num_replicas;
  pool.min_size = min_size;
  pool.crush_rule = 0;

  osdmap->inc_epoch();

  OSDMapTestHelpers::add_pool(osdmap, pool_id, pool);

  // Finalize the CRUSH map to calculate working_size
  // This is required for crush_init_workspace() to work correctly
  osdmap->crush->finalize();

  pgid = pg_t(0, pool_id);
  
  // Set up pg_temp to define the acting set with OSD 0 as primary
  std::vector<int> acting;
  for (int i = 0; i < num_replicas; i++) {
    acting.push_back(i);
  }
  OSDMapTestHelpers::set_pg_acting(osdmap, pgid, acting);
  OSDMapTestHelpers::set_pg_acting_primary(osdmap, pgid, 0);

  spg_t replica_spgid(pgid, shard_id_t::NO_SHARD);

  const pg_pool_t* pool_ptr = OSDMapTestHelpers::get_pool(osdmap, pool_id);
  ceph_assert(pool_ptr != nullptr);

  // Create per-OSD stores, fixtures, collections, and backends in one pass.
  for (int i = 0; i < num_replicas; i++) {
    auto osd_fixture = std::make_unique<OsdTestFixture>(i);
    osd_fixture->store = MockStore::create(g_ceph_context, i);
    ASSERT_TRUE(osd_fixture->store);
    osd_fixture->data_dir = "";
    osd_fixtures[i] = std::move(osd_fixture);

    auto* osd_fix = osd_fixtures[i].get();
    pg_shard_t pg_whoami(i, shard_id_t::NO_SHARD);

    // Create TestPG and its per-PG collection.
    TestPG* test_pg = osd_fix->create_pg(replica_spgid, pg_whoami);
    coll_t replica_coll(replica_spgid);
    auto replica_ch = osd_fix->store->create_new_collection(replica_coll);
    ObjectStore::Transaction ct;
    ct.create_collection(replica_coll, 0);
    ASSERT_EQ(osd_fix->store->queue_transaction(replica_ch, std::move(ct)), 0);
    test_pg->coll = replica_coll;
    test_pg->ch = replica_ch;

    auto replica_listener = std::make_unique<MockPGBackendListener>(
      osdmap, pool_id, dpp.get(), pg_whoami);

    // Initialize the listener's own info.pgid so OSDMap queries work
    replica_listener->info.pgid = replica_spgid;

    // For replicated pools, use NO_SHARD for all replicas
    for (int j = 0; j < num_replicas; j++) {
      replica_listener->shardset.insert(pg_shard_t(j, shard_id_t::NO_SHARD));

      // Initialize shard_info for each replica - required by backend
      pg_info_t replica_pg_info;
      replica_pg_info.pgid = replica_spgid;
      replica_listener->shard_info[pg_shard_t(j, shard_id_t::NO_SHARD)] = replica_pg_info;

      // Initialize shard_missing for each replica - required by backend
      pg_missing_t replica_missing;
      replica_listener->shard_missing[pg_shard_t(j, shard_id_t::NO_SHARD)] = replica_missing;
    }

    replica_listener->set_store(osd_fix->store.get(), test_pg->ch);
    replica_listener->set_event_loop(event_loop.get());

    auto replica_backend = std::make_unique<ReplicatedBackend>(
      replica_listener.get(), test_pg->coll, test_pg->ch, osd_fix->store.get(), cct);

    test_pg->backend_listener = std::move(replica_listener);
    test_pg->backend = std::move(replica_backend);
  }

  // Create MockMessenger and register a single handler that routes to backends
  messenger = std::make_unique<MockMessenger>(event_loop.get(), cct);
  
  // Set up epoch getter for MockMessenger to enable epoch-based message filtering
  messenger->set_epoch_getter([this](int osd) -> epoch_t {
    auto test_pg = get_test_pg();
    if (test_pg->has_backend()) {
      return test_pg->get_backend_listener()->pgb_get_osdmap_epoch();
    }
    // If listener doesn't exist yet, use the test fixture's osdmap
    return osdmap->get_epoch();
  });
  
  // Set TestPG getter - MockMessenger extracts spg_t and calls this to look up TestPG
  messenger->set_test_pg_getter([this](int osd, spg_t spgid) -> TestPG* {
    return get_test_pg(osd, spgid);
  });
  
  // Create an OpTracker for wrapping messages in OpRequestRef
  // This is needed because PGBackend::_handle_message expects OpRequestRef
  // Store as member variable so it can be properly shut down in TearDown()
  op_tracker = std::make_shared<OpTracker>(cct, true, 1);
  
  // Helper lambda to create a typed handler that wraps messages and routes to backends
  auto make_backend_handler = [this]<typename MsgType>(int msg_type) {
    messenger->register_typed_handler<MsgType>(msg_type,
      [this](int from_osd, int to_osd, boost::intrusive_ptr<MsgType> m) -> bool {
        TestPG* test_pg = get_test_pg();
        OpRequestRef op =
            this->op_tracker->create_request<OpRequest, Message*>(m.detach());
        return test_pg->get_backend()->_handle_message(op);
      });
  };

  // Register typed handlers for replicated backend message types
  make_backend_handler.template operator()<MOSDRepOp>(MSG_OSD_REPOP);
  make_backend_handler.template operator()<MOSDRepOpReply>(MSG_OSD_REPOPREPLY);
  
  for (int i = 0; i < num_replicas; i++) {
    TestPG* test_pg = get_first_test_pg_for_osd(i);
    if (test_pg && test_pg->has_backend()) {
      test_pg->get_backend_listener()->set_messenger(messenger.get());
    }
  }
}

void PGBackendTestFixture::do_transaction(
  const hobject_t& hoid,
  PGTransactionUPtr pg_t,
  const object_stat_sum_t& delta_stats,
  const eversion_t& at_version,
  std::vector<pg_log_entry_t> log_entries,
  std::function<void(int)> on_write_complete)
{
  eversion_t trim_to(0, 0);
  eversion_t pg_committed_to(0, 0);
  std::optional<pg_hit_set_history_t> hset_history;

  Context *on_complete = new LambdaContext([on_write_complete](int r) {
    // Call the write-specific completion lambda if provided
    if (on_write_complete) {
      on_write_complete(r);
    }
  });

  ceph_tid_t tid = next_tid++;
  osd_reqid_t reqid(entity_name_t::OSD(0), 0, tid);

  PGBackend* primary_backend = get_primary_backend();
  ceph_assert(primary_backend != nullptr);
  primary_backend->submit_transaction(
    hoid,
    delta_stats,
    at_version,
    std::move(pg_t),
    trim_to,
    pg_committed_to,
    std::move(log_entries),
    hset_history,
    on_complete,
    tid,
    reqid,
    OpRequestRef()
  );
}

// Helper function that performs the actual write logic
// Must be called within event loop context on the primary OSD
void PGBackendTestFixture::do_create_and_write_impl(
  const std::string& obj_name,
  const std::string& data,
  const eversion_t& at_version,
  bool& completed,
  int& result)
{
  
  hobject_t hoid = make_test_object(obj_name);
  PGTransactionUPtr pg_t = std::make_unique<PGTransaction>();
  pg_t->create(hoid);

  // Use persistent OBC so attr_cache is maintained across operations
  ObjectContextRef obc = get_object_context(hoid, true);
  pg_t->obc_map[hoid] = obc;

  // Note: We do NOT pre-seed attr_cache here. For a new object, attr_cache
  // should be empty. ECTransaction::attr_updates() will update attr_cache
  // with the new OI from PGTransaction::attr_updates during the transaction.

  // Track outstanding write
  get_test_pg()->outstanding_writes[hoid]++;

  bufferlist bl;
  bl.append(data);
  
  // Only perform write if data is non-empty (PGTransaction requires len > 0)
  if (bl.length() > 0) {
    pg_t->write(hoid, 0, bl.length(), bl);
  }

  object_stat_sum_t delta_stats;
  delta_stats.num_objects = 1;
  delta_stats.num_bytes = bl.length();

  // Build the NEW OI that finish_ctx() would produce
  object_info_t new_oi = obc->obs.oi;
  new_oi.version = at_version;
  new_oi.prior_version = obc->obs.oi.version;
  new_oi.size = bl.length();

  // Encode new OI and put into PGTransaction as an attr update.
  // This matches PrimaryLogPG::finish_ctx() lines 9127-9130,9142.
  {
    bufferlist oi_bl;
    new_oi.encode(oi_bl,
      osdmap->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));
    pg_t->setattr(hoid, OI_ATTR, oi_bl);
  }

  // snapset
  if (hoid.snap == CEPH_NOSNAP) {
    bufferlist bss;
    encode(SnapSet(), bss);
    pg_t->setattr(hoid, SS_ATTR, bss);
  }

  // Update OBC obs to new state BEFORE submitting the transaction.
  // This matches PrimaryLogPG::finish_ctx() line 9187: ctx->obc->obs = ctx->new_obs
  // At this point: obc->obs.oi has NEW state, obc->attr_cache[OI_ATTR] has OLD state.
  obc->obs.oi = new_oi;
  obc->obs.exists = true;

  std::vector<pg_log_entry_t> log_entries;
  pg_log_entry_t entry;
  entry.mark_unrollbackable();
  entry.op = pg_log_entry_t::MODIFY;
  entry.soid = hoid;
  entry.version = at_version;
  entry.prior_version = eversion_t(0, 0);
  log_entries.push_back(entry);

  // Capture TestPG pointer before creating lambda
  TestPG* test_pg = get_test_pg();
  
  // Create completion lambda for write-specific cleanup
  // Capture completed and result by reference from caller
  auto write_complete = [test_pg, hoid, obc, object_tracker=this->object_tracker, obj_name, data, at_version, &completed, &result](int r) {
    // Note: we do NOT update obc->obs after completion — it was already
    // updated above before submit, matching PrimaryLogPG behavior.
    // ECTransaction::attr_updates() will have updated attr_cache[OI_ATTR]
    // to the new encoded OI during the transaction.

    // Decrement outstanding writes counter
    if (test_pg->outstanding_writes[hoid] > 0) {
      test_pg->outstanding_writes[hoid]--;
      // Clean up the counter if it reaches 0, but don't clear attr_cache here.
      // The attr_cache will be cleared on on_change() events.
      if (test_pg->outstanding_writes[hoid] == 0) {
        test_pg->outstanding_writes.erase(hoid);
      }
    }

    if (r != 0 && r != -EINPROGRESS) {
      // Transaction failed — roll back OBC state.
      // In production this would be handled differently, but for tests
      // we just reset to a clean state.
      obc->obs.oi = object_info_t(hoid);
      obc->obs.exists = false;
      obc->attr_cache.clear();
      test_pg->outstanding_writes.erase(hoid);
    }
    
    if (r == 0 && object_tracker) {
      object_tracker->record_create(obj_name, data, at_version);
    }
    
    // Mark as completed and store result
    completed = true;
    result = r;
  };

  do_transaction(
    hoid, std::move(pg_t), delta_stats, at_version, std::move(log_entries), write_complete);
}

// Public interface that schedules the write on the primary OSD
int PGBackendTestFixture::create_and_write(
  const std::string& obj_name,
  const std::string& data)
{
  // Get the primary TestPG directly
  TestPG* primary_test_pg = get_primary_test_pg();
  if (!primary_test_pg) {
    return -EINVAL;
  }
  
  int primary_osd = primary_test_pg->pg_whoami.osd;
  
  bool completed = false;
  int result = -EINPROGRESS;
  eversion_t version = get_next_version();
  
  event_loop->run_in_pg(primary_osd, primary_test_pg, [this, &completed, &result, &version, obj_name, data]() {
    // Call the impl function which will set up the completion callback
    // The callback will update 'completed' and 'result' when it fires
    do_create_and_write_impl(obj_name, data, version, completed, result);
  });
  
  // Run the event loop to complete the transaction
  // The completion callback will update 'completed' and 'result'
  event_loop->run_until_idle();
  
  // Return the result from the completion callback
  return completed ? result : -EINPROGRESS;
}

void PGBackendTestFixture::set_object_context(
  const hobject_t& hoid,
  ObjectContextRef obc)
{
  TestPG* test_pg = get_test_pg();
  ceph_assert(test_pg != nullptr);
  test_pg->object_contexts[hoid] = obc;
}

void PGBackendTestFixture::clear_object_contexts()
{
  TestPG* test_pg = get_test_pg();
  ceph_assert(test_pg != nullptr);
  test_pg->object_contexts.clear();
}


ObjectContextRef PGBackendTestFixture::get_object_context(
  const hobject_t& hoid,
  bool can_create,
  const std::map<std::string, ceph::buffer::list, std::less<>> *attrs)
{
  TestPG* test_pg = get_test_pg();
  ceph_assert(test_pg != nullptr);
  
  // Check cache first (matches PrimaryLogPG::get_object_context line 11968)
  auto it = test_pg->object_contexts.find(hoid);
  if (it != test_pg->object_contexts.end()) {
    return it->second;
  }

  // Check disk for object info (matches PrimaryLogPG lines 11977-11982)
  bufferlist bv;
  if (attrs) {
    auto it_oi = attrs->find(OI_ATTR);
    ceph_assert(it_oi != attrs->end());
    bv = it_oi->second;
  } else {
    PGBackend* primary_backend = get_primary_backend();
    int r = primary_backend->objects_get_attr(hoid, OI_ATTR, &bv);
    
    if (r < 0) {
      if (!can_create) {
        // Object doesn't exist and can't create (matches PrimaryLogPG lines 11985-11989)
        return ObjectContextRef();
      }
      
      // Create new object context (matches PrimaryLogPG lines 11992-12004)
      object_info_t oi(hoid);
      ObjectContextRef obc = std::make_shared<ObjectContext>();
      obc->obs.oi = oi;
      obc->obs.exists = false;
      obc->ssc = nullptr;
      set_object_context(hoid, obc);
      return obc;
    }
  }

  // Decode object_info (matches PrimaryLogPG lines 12008-12015)
  object_info_t oi;
  try {
    bufferlist::const_iterator bliter = bv.begin();
    decode(oi, bliter);
  } catch (...) {
    return ObjectContextRef();
  }
  
  // Create OBC and populate from disk (matches PrimaryLogPG lines 12019-12022)
  ObjectContextRef obc = std::make_shared<ObjectContext>();
  obc->obs.oi = oi;
  obc->obs.exists = true;
  obc->ssc = nullptr;
  
  // For EC pools, load all attributes (matches PrimaryLogPG lines 12031-12040)
  if (pool_type == EC) {
    if (attrs) {
      obc->attr_cache = *attrs;
    } else {
      PGBackend* primary_backend = get_primary_backend();
      int r = primary_backend->objects_get_attrs(hoid, &obc->attr_cache);
      ceph_assert(r == 0);
    }
  }
  
  // Cache the OBC (matches PrimaryLogPG line 12019 lookup_or_create)
  set_object_context(hoid, obc);
  
  return obc;
}

// Helper function for write implementation
void PGBackendTestFixture::do_write_impl(
  const std::string& obj_name,
  uint64_t offset,
  const std::string& data,
  uint64_t object_size,
  const eversion_t& at_version,
  bool& completed,
  int& result)
{
  hobject_t hoid = make_test_object(obj_name);
  PGTransactionUPtr pg_t = std::make_unique<PGTransaction>();

  ObjectContextRef obc = get_object_context(hoid, false);
  pg_t->obc_map[hoid] = obc;

  // Track outstanding write
  get_test_pg()->outstanding_writes[hoid]++;

  bufferlist bl;
  bl.append(data);
  pg_t->write(hoid, offset, bl.length(), bl);

  uint64_t new_size = std::max(object_size, offset + bl.length());

  object_stat_sum_t delta_stats;
  if (new_size > object_size) {
    delta_stats.num_bytes = new_size - object_size;
  } else {
    delta_stats.num_bytes = 0;
  }

  // Prior version comes from the object's current version
  eversion_t prior_version = obc->obs.oi.version;

  // Build the NEW OI
  object_info_t new_oi = obc->obs.oi;
  new_oi.version = at_version;
  new_oi.prior_version = prior_version;
  new_oi.size = new_size;

  // Encode new OI into PGTransaction
  {
    bufferlist oi_bl;
    new_oi.encode(oi_bl,
      osdmap->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));
    pg_t->setattr(hoid, OI_ATTR, oi_bl);
  }

  // Update OBC obs to new state BEFORE submitting
  obc->obs.oi = new_oi;

  std::vector<pg_log_entry_t> log_entries;
  pg_log_entry_t entry;
  // Don't mark as unrollbackable - partial writes need rollback support
  entry.op = pg_log_entry_t::MODIFY;
  entry.soid = hoid;
  entry.version = at_version;
  entry.prior_version = prior_version;
  log_entries.push_back(entry);

  // Capture TestPG pointer before creating lambda
  TestPG* test_pg = get_test_pg();
  
  // Create completion lambda for write-specific cleanup
  // Capture completed and result by reference from caller
  auto write_complete = [test_pg, hoid, obc, prior_version, object_size, object_tracker=this->object_tracker, obj_name, offset, data, at_version, &completed, &result](int r) {
    // Decrement outstanding writes counter
    if (test_pg->outstanding_writes[hoid] > 0) {
      test_pg->outstanding_writes[hoid]--;
      // Clean up the counter if it reaches 0, but don't clear attr_cache here.
      // The attr_cache will be cleared on on_change() events.
      if (test_pg->outstanding_writes[hoid] == 0) {
        test_pg->outstanding_writes.erase(hoid);
      }
    }

    if (r != 0 && r != -EINPROGRESS) {
      // Roll back OBC on failure
      obc->obs.oi.version = prior_version;
      obc->obs.oi.size = object_size;
      obc->attr_cache.clear();
      test_pg->outstanding_writes.erase(hoid);
    }
    
    if (r == 0 && object_tracker) {
      object_tracker->record_data_write(obj_name, offset, data, at_version);
    }
    
    // Mark as completed and store result
    completed = true;
    result = r;
  };

  do_transaction(
    hoid, std::move(pg_t), delta_stats, at_version, std::move(log_entries), write_complete);
}

// Public interface that schedules the write on the primary OSD
int PGBackendTestFixture::write(
  const std::string& obj_name,
  uint64_t offset,
  const std::string& data,
  uint64_t object_size)
{
  // Get the primary TestPG directly
  TestPG* primary_test_pg = get_primary_test_pg();
  if (!primary_test_pg) {
    return -EINVAL;
  }
  
  int primary_osd = primary_test_pg->pg_whoami.osd;
  
  bool completed = false;
  int result = -EINPROGRESS;
  eversion_t version = get_next_version();
  
  event_loop->run_in_pg(primary_osd, primary_test_pg, [this, &completed, &result, &version, obj_name, offset, data, object_size]() {
    do_write_impl(obj_name, offset, data, object_size, version, completed, result);
  });
  
  // Run the event loop to complete the transaction
  // The completion callback will update 'completed' and 'result'
  event_loop->run_until_idle();
  
  return completed ? result : -EINPROGRESS;
}

int PGBackendTestFixture::delete_object(const std::string& obj_name)
{
  // Get the primary TestPG directly
  TestPG* primary_test_pg = get_primary_test_pg();
  if (!primary_test_pg) {
    return -EINVAL;
  }
  
  int primary_osd = primary_test_pg->pg_whoami.osd;
  
  bool completed = false;
  int result = -EINPROGRESS;
  eversion_t at_version = get_next_version();
  
  event_loop->run_in_pg(primary_osd, primary_test_pg, [this, &completed, &result, at_version, obj_name]() {
    hobject_t hoid = make_test_object(obj_name);
    
    // Get OBC
    ObjectContextRef obc = get_object_context(hoid, false);
    if (!obc) {
      completed = true;
      result = -ENOENT;
      return;
    }
    
    // Create PGTransaction for delete
    std::unique_ptr<PGTransaction> pg_t(new PGTransaction());
    pg_t->obc_map[hoid] = obc;  // Populate OBC map
    pg_t->remove(hoid);
    
    object_stat_sum_t delta_stats;
    delta_stats.num_objects = -1;
    delta_stats.num_bytes = -(int64_t)obc->obs.oi.size;
    
    eversion_t prior_version = obc->obs.oi.version;
    
    // Create log entry for delete
    std::vector<pg_log_entry_t> log_entries;
    pg_log_entry_t entry;
    entry.op = pg_log_entry_t::DELETE;
    entry.soid = hoid;
    entry.version = at_version;
    entry.prior_version = prior_version;
    log_entries.push_back(entry);
    
    TestPG* test_pg = get_test_pg();
    
    // Create completion lambda
    auto delete_complete = [test_pg, hoid, object_tracker=this->object_tracker, obj_name, at_version, &completed, &result](int r) {
      // Clean up outstanding writes and attr_cache
      test_pg->outstanding_writes.erase(hoid);

      if (r == 0 && object_tracker) {
        object_tracker->record_delete(obj_name, at_version);
      }

      // Mark as completed and store result
      completed = true;
      result = r;
    };
    
    do_transaction(
      hoid, std::move(pg_t), delta_stats, at_version, std::move(log_entries), delete_complete);
  });
  
  // Run the event loop to complete the transaction
  event_loop->run_until_idle();
  
  return completed ? result : -EINPROGRESS;
}

int PGBackendTestFixture::read_object(
  const std::string& obj_name,
  uint64_t offset,
  uint64_t length,
  bufferlist& out_data,
  uint64_t object_size)
{
  hobject_t hoid = make_test_object(obj_name);

  if (pool_type == EC) {
    bool completed = false;
    int completion_result = -1;

    std::list<std::pair<ec_align_t, std::pair<bufferlist*, Context*>>> to_read;

    ec_align_t align(offset, length, 0);

    Context *read_complete = new LambdaContext([&completed, &completion_result](int r) {
      completed = true;
      completion_result = r;
    });

    to_read.push_back(std::make_pair(align, std::make_pair(&out_data, read_complete)));

    Context *on_complete = new LambdaContext([](int r) {
    });

    PGBackend* primary_backend = get_primary_backend();
    ceph_assert(primary_backend != nullptr);
    ECSwitch* ec_switch = dynamic_cast<ECSwitch*>(primary_backend);
    ceph_assert(ec_switch != nullptr);

    ec_switch->objects_read_async(
      hoid,
      object_size,
      to_read,
      on_complete,
      false
    );

    event_loop->run_until_idle();

    ceph_assert(completed);

    return completion_result;
  } else {
    PGBackend* primary_backend = get_primary_backend();
    ceph_assert(primary_backend != nullptr);
    ReplicatedBackend* rep_backend = dynamic_cast<ReplicatedBackend*>(primary_backend);
    ceph_assert(rep_backend != nullptr);

    int result = rep_backend->objects_read_sync(
      hoid,
      offset,
      length,
      0,
      &out_data
    );

    return result;
  }
}

int PGBackendTestFixture::read_attribute(
  const std::string& obj_name,
  const std::string& attr_name,
  bufferlist& out_value)
{
  hobject_t hoid = make_test_object(obj_name);
  PGBackend* primary_backend = get_primary_backend();
  if (!primary_backend) {
    return -EINVAL;
  }
  
  return primary_backend->objects_get_attr(hoid, attr_name, &out_value);
}

void PGBackendTestFixture::verify_attribute(
  const std::string& obj_name,
  const std::string& attr_name)
{
  ceph_assert(object_tracker);
  auto expected_value = object_tracker->get_expected_attribute(obj_name, attr_name);
  ASSERT_TRUE(expected_value.has_value())
    << "ObjectTracker has no record of attribute " << attr_name << " for " << obj_name;

  bufferlist attr_bl;
  int result = read_attribute(obj_name, attr_name, attr_bl);
  ASSERT_EQ(result, 0) << "Failed to read attribute " << attr_name << " from " << obj_name;

  std::string actual_value(attr_bl.c_str(), attr_bl.length());
  ASSERT_EQ(actual_value, *expected_value)
    << "Attribute " << attr_name << " on " << obj_name << " doesn't match expected value";
}

void PGBackendTestFixture::verify_object(const std::string& obj_name)
{
  ceph_assert(object_tracker);
  
  // Get the expected size from the object tracker
  uint64_t object_size = object_tracker->get_expected_size(obj_name);
  
  // Get the expected data for the entire object (offset 0, full size)
  std::string expected_data =
    object_tracker->get_expected_data(obj_name, 0, object_size);

  bufferlist read_data;
  int read_result = read_object(obj_name, 0, expected_data.length(), read_data, object_size);

  ASSERT_GE(read_result, 0) << "Read should complete successfully for " << obj_name;

  std::string read_string(read_data.c_str(), read_data.length());
  ASSERT_EQ(read_string, expected_data) << "Data should match for " << obj_name;

  const auto* tracked_object = object_tracker->get_object_state(obj_name);
  ASSERT_NE(tracked_object, nullptr)
    << "ObjectTracker has no record of object " << obj_name;

  // Verify expected attributes exist with correct values
  for (const auto& [attr_name, _] : tracked_object->attributes) {
    verify_attribute(obj_name, attr_name);
  }
  
  // List all actual attributes and verify no unexpected ones exist
  std::map<std::string, ceph::buffer::list, std::less<>> actual_attrs;
  int list_result = list_attributes(obj_name, actual_attrs);
  ASSERT_EQ(0, list_result) << "Failed to list attributes for " << obj_name;
  
  // Check for unexpected attributes (excluding system attributes)
  for (const auto& [attr_name, attr_bl] : actual_attrs) {
    // Skip system attributes (start with _ or are snapset)
    if (attr_name[0] == '_' || attr_name == "snapset") {
      continue;
    }
    
    // This is a user attribute - verify it's in the tracker
    ASSERT_TRUE(tracked_object->attributes.count(attr_name) > 0)
      << "Unexpected attribute '" << attr_name << "' found on object " << obj_name
      << " - this attribute should not exist (may indicate failed rollback)";
  }
}

void PGBackendTestFixture::create_and_write_verify(
  const std::string& obj_name,
  const std::string& data)
{
  int result = create_and_write(obj_name, data);

  ASSERT_GE(result, 0) << "Write should complete successfully";
  // Always verify - tests should only use this helper when success is expected
  verify_object(obj_name);
}

void PGBackendTestFixture::write_verify(
  const std::string& obj_name,
  size_t offset,
  const std::string& data,
  size_t object_size,
  const std::string& context_msg)
{
  int result = write(obj_name, offset, data, object_size);

  std::string msg_suffix = context_msg.empty() ? "" : " (" + context_msg + ")";
  ASSERT_GE(result, 0) << "Write should complete successfully" << msg_suffix;

  verify_object(obj_name);
}

// ---------------------------------------------------------------------------
// NOTE: update_osdmap() intentionally does NOT reconcile listener acting sets
//
// This method updates only:
//   - The fixture's osdmap pointer
//   - The osdmap reference in all listeners
//
// It does NOT update the following fields on any MockPGBackendListener:
//   - shardset
//   - acting_recovery_backfill_shard_id_set
//   - shard_info
//   - shard_missing
//
// This is intentional: those fields describe the acting set as seen by each
// individual OSD, and their correct values depend on the specific failure
// scenario being simulated.  Updating them blindly here would hide bugs and
// make it impossible to test partial-failure cases.
//
// Callers that need to simulate an OSD failure MUST update those fields
// themselves before (or after) calling update_osdmap().
//
// See TestECFailover::simulate_osd_failure() for a worked example that
// removes the failed shard from shardset and
// acting_recovery_backfill_shard_id_set on every listener before delegating
// to update_osdmap().
// ---------------------------------------------------------------------------
void PGBackendTestFixture::update_osdmap(
  std::shared_ptr<OSDMap> new_osdmap,
  std::optional<pg_shard_t> new_primary)
{
  // Step 1: Update the osdmap reference first
  osdmap = new_osdmap;

  // Step 2: Update the osdmap and pool in all backend listeners
  // Use get_test_pg_by_instance() to access all OSDs, including those that
  // may have been removed from the acting set
  for (auto& [osd, osd_fixture] : osd_fixtures) {
    
    for (auto &[spg, test_pg] : osd_fixture->pgs)
    if (test_pg->has_backend()) {
      test_pg->get_backend_listener()->osdmap = new_osdmap;
      // Update the pool to point to the new OSDMap's pool object
      // This is critical because ECBackend's stripe_info_t holds a pointer to
      // the pg_pool_t inside MockPGBackendListener::pool.info. When we use
      // deepish_copy_from() to create a new OSDMap, pool objects are at new
      // addresses. Calling pool.update() copies the new pool info into the
      // existing pool.info member, keeping its address stable.
      test_pg->get_backend_listener()->pool.update(new_osdmap);
    }
  }

  // Step 3: Clear outstanding writes for all PGs
  // Note: outstanding_writes is now per-PG, so we clear it for each PG
  for (auto& [osd, osd_fixture] : osd_fixtures) {
    for (auto& [spgid, test_pg] : osd_fixture->pgs) {
      test_pg->outstanding_writes.clear();
    }
  }

  // Step 4: Schedule on_change() calls as event loop actions
  // This allows them to be delayed and processed after the new epoch
  // Iterate over all PGs in all OSDs to handle on_change() for each
  for (auto& [osd, osd_fixture] : osd_fixtures) {
    for (auto& [spgid, test_pg] : osd_fixture->pgs) {
      if (test_pg->has_backend()) {
        PGBackend* backend_ptr = test_pg->get_backend();
        // Schedule peering event with TestPG context so get_test_pg() works
        event_loop->schedule_peering_event(osd, test_pg.get(), [this, backend_ptr]() {
          backend_ptr->on_change();
          clear_object_contexts();
        });
      }
    }
  }
  event_loop->run_until_idle();
}


// Helper function for write_attribute implementation
void PGBackendTestFixture::do_write_attribute_impl(
  const std::string& obj_name,
  const std::string& attr_name,
  const std::string& attr_value,
  bool force_all_shards,
  bool& completed,
  int& result)
{
  hobject_t hoid = make_test_object(obj_name);
  PGTransactionUPtr pg_t = std::make_unique<PGTransaction>();
  
  ObjectContextRef obc = get_object_context(hoid, false);
  pg_t->obc_map[hoid] = obc;
  
  get_test_pg()->outstanding_writes[hoid]++;
  
  eversion_t prior_version = obc->obs.oi.version;
  eversion_t at_version = get_next_version();
  
  object_info_t new_oi = obc->obs.oi;
  new_oi.version = at_version;
  new_oi.prior_version = prior_version;
  
  {
    bufferlist oi_bl;
    new_oi.encode(oi_bl, osdmap->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));
    pg_t->setattr(hoid, OI_ATTR, oi_bl);
  }
  
  {
    bufferlist attr_bl;
    attr_bl.append(attr_value);
    pg_t->setattr(hoid, attr_name, attr_bl);
  }
  
  obc->obs.oi = new_oi;
  
  std::vector<pg_log_entry_t> log_entries;
  pg_log_entry_t entry;
  entry.op = pg_log_entry_t::MODIFY;
  entry.soid = hoid;
  entry.version = at_version;
  entry.prior_version = prior_version;
  log_entries.push_back(entry);
  
  object_stat_sum_t delta_stats;
  
  // Capture TestPG pointer before creating lambda
  TestPG* test_pg = get_test_pg();
  
  // Capture completed and result by reference from caller
  auto write_complete = [test_pg, hoid, obc, prior_version, object_tracker=this->object_tracker, obj_name, attr_name, attr_value, at_version, &completed, &result](int r) {
    if (test_pg->outstanding_writes[hoid] > 0) {
      test_pg->outstanding_writes[hoid]--;
      if (test_pg->outstanding_writes[hoid] == 0) {
        test_pg->outstanding_writes.erase(hoid);
      }
    }
    
    if (r != 0 && r != -EINPROGRESS) {
      obc->obs.oi.version = prior_version;
      obc->attr_cache.clear();
      test_pg->outstanding_writes.erase(hoid);
    }
    
    // Record in object tracker after successful write (excluding OI_ATTR as requested)
    // Only record if object_tracker is still valid (may be null during teardown)
    if (r == 0 && attr_name != OI_ATTR && object_tracker) {
      object_tracker->record_attribute_write(obj_name, attr_name, attr_value, at_version);
    }
    
    // Mark as completed and store result
    completed = true;
    result = r;
  };
  
  // Control first_write_in_interval to simulate different write patterns
  if (force_all_shards && pool_type == EC) {
    PGBackend* primary_backend = get_primary_backend();
    if (primary_backend) {
      primary_backend->on_change();
    }
  }
  
  do_transaction(
    hoid, std::move(pg_t), delta_stats, at_version, std::move(log_entries), write_complete);
}

// Public interface that schedules the write on the primary OSD
int PGBackendTestFixture::write_attribute(
  const std::string& obj_name,
  const std::string& attr_name,
  const std::string& attr_value,
  bool force_all_shards)
{
  // Get the primary TestPG directly
  TestPG* primary_test_pg = get_primary_test_pg();
  if (!primary_test_pg) {
    return -EINVAL;
  }
  
  int primary_osd = primary_test_pg->pg_whoami.osd;
  
  bool completed = false;
  int result = -EINPROGRESS;
  
  event_loop->run_in_pg(primary_osd, primary_test_pg, [this, &completed, &result, obj_name, attr_name, attr_value, force_all_shards]() {
    do_write_attribute_impl(obj_name, attr_name, attr_value, force_all_shards, completed, result);
  });
  
  // Run the event loop to complete the transaction
  // The completion callback will update 'completed' and 'result'
  event_loop->run_until_idle();
  
  return completed ? result : -EINPROGRESS;
}

int PGBackendTestFixture::list_attributes(
  const std::string& obj_name,
  std::map<std::string, ceph::buffer::list, std::less<>>& attrs)
{
  hobject_t hoid = make_test_object(obj_name);
  
  PGBackend* primary_backend = get_primary_backend();
  if (!primary_backend) {
    return -EINVAL;
  }
  
  int r = primary_backend->objects_get_attrs(hoid, &attrs);
  return r;
}

object_info_t PGBackendTestFixture::read_shard_object_info(
  const std::string& obj_name,
  int shard)
{
  hobject_t hoid = make_test_object(obj_name);
  ghobject_t ghoid(hoid, ghobject_t::NO_GEN, shard_id_t(shard));
  
  TestPG* shard_pg = get_test_pg_by_shard(shard);
  OsdTestFixture* osd_fixture = shard_pg ? get_osd_fixture(shard_pg->pg_whoami.osd) : nullptr;
  if (!shard_pg || !osd_fixture || !shard_pg->ch || !osd_fixture->store) {
    std::cerr << "ERROR: No collection handle or store for shard " << shard << std::endl;
    return object_info_t(hoid);
  }

  ceph::buffer::ptr value_ptr;
  int r = osd_fixture->store->getattr(shard_pg->ch, ghoid, OI_ATTR, value_ptr);
  if (r < 0) {
    std::cerr << "ERROR: Failed to read OI_ATTR from shard " << shard
              << ": " << cpp_strerror(r) << std::endl;
    return object_info_t(hoid);
  }
  
  bufferlist bl;
  bl.append(value_ptr);
  auto p = bl.cbegin();
  object_info_t oi;
  oi.decode(p);
  return oi;
}


void PGBackendTestFixture::scrub_all_objects()
{
  if (!scrub_listener || !snap_reader) {
    initialize_scrub_infra();
  }

  // Collect all objects from the primary OSD's store
  std::set<std::string> all_object_names;
  
  TestPG* primary_pg = get_primary_test_pg();
  if (!primary_pg) {
    std::cerr << "WARNING: No primary PG found during teardown scrub" << std::endl;
    return;
  }
  
  int primary_osd = primary_pg->pg_whoami.osd;
  OsdTestFixture* primary_fixture = get_osd_fixture(primary_osd);
  if (!primary_fixture || !primary_pg->ch) {
    std::cerr << "WARNING: No primary fixture or collection handle during teardown scrub" << std::endl;
    return;
  }

  // List all objects in the collection
  ghobject_t next;
  while (true) {
    std::vector<ghobject_t> objects;
    int r = primary_fixture->store->collection_list(
      primary_pg->ch,
      next,
      ghobject_t::get_max(),
      primary_fixture->store->get_ideal_list_max(),
      &objects,
      &next);
    
    if (r < 0) {
      std::cerr << "WARNING: collection_list failed during teardown scrub: "
                << cpp_strerror(r) << std::endl;
      break;
    }
    
    if (objects.empty()) {
      break;
    }
    
    // Extract object names from hobject_t
    for (const auto& ghobj : objects) {
      const hobject_t& hobj = ghobj.hobj;
      // Only scrub objects in our pool
      if (hobj.pool == pool_id && !hobj.is_max()) {
        all_object_names.insert(hobj.oid.name);
      }
    }
  }

  // Scrub each object found (skip deleted objects if ObjectTracker is enabled)
  for (const auto& obj_name : all_object_names) {
    // Skip objects that have been deleted (if ObjectTracker is tracking them)
    if (object_tracker) {
      const auto* obj_state = object_tracker->get_object_state(obj_name);
      if (obj_state && !obj_state->exists) {
        // Object was deleted, skip scrubbing it
        continue;
      }
    }
    
    bool corrupted = scrub_object(obj_name);
    EXPECT_FALSE(corrupted)
      << "Object '" << obj_name << "' found to be corrupted during teardown scrub";
  }

}

bool PGBackendTestFixture::scrub_object(const std::string& obj_name, bool skip_verify)
{
  hobject_t hoid = make_test_object(obj_name);

  // Get the acting set from the OSDMap to know which OSDs to scrub
  std::vector<int> acting_osds;
  int acting_primary = -1;
  osdmap->pg_to_acting_osds(pgid, &acting_osds, &acting_primary);
  
  std::map<pg_shard_t, ScrubMap> scrub_maps;

  // Scan each OSD in the acting set
  for (size_t i = 0; i < acting_osds.size(); ++i) {
    int osd = acting_osds[i];
    shard_id_t shard_id(i);
    pg_shard_t pg_shard(osd, shard_id);
    
    TestPG* test_pg = get_test_pg(pg_shard);
    if (!test_pg) {
      std::cerr << "WARNING: TestPG for OSD " << osd << " (shard " << i << ") does not exist "
                << "(object may be left degraded, cannot scrub)" << std::endl;
      return false;
    }
    
    ScrubMap& smap = scrub_maps[pg_shard];
    
    // Execute be_scan_list within the PG context for this shard
    event_loop->run_in_pg(osd, test_pg, [&]() {
      PGBackend* backend = test_pg->get_backend();
      if (!backend) {
        std::cerr << "ERROR: Backend pointer for OSD " << osd << " (shard " << i << ") is null" << std::endl;
        return;
      }

      ScrubMapBuilder pos;
      pos.ls.push_back(hoid);
      pos.pos = 0;
      pos.deep = true;

      const Scrub::ScrubCounterSet& counters = Scrub::io_counters_ec;

      int r = backend->be_scan_list(counters, smap, pos);
      while (r == -EINPROGRESS) {
        r = backend->be_scan_list(counters, smap, pos);
      }

      if (r != 0) {
        std::cerr << "ERROR: be_scan_list failed for OSD " << osd << " (shard " << i << "): " << cpp_strerror(r) << std::endl;
        return;
      }

      if (!smap.objects.contains(hoid)) {
        std::cerr << "ERROR: Object not in scrub map for OSD " << osd << " (shard " << i << ")" << std::endl;
        return;
      }
    });
    
    // Run event loop to complete any pending operations for this shard
    event_loop->run_until_idle();
  }

  if (!scrub_listener || !snap_reader) {
    initialize_scrub_infra();
  }

  ceph_assert(scrub_listener);
  ceph_assert(snap_reader);

  MockPGBackendListener* primary_listener = get_primary_listener();
  if (!primary_listener) {
    std::cerr << "ERROR: No primary listener found" << std::endl;
    return true;
  }
  int primary_shard = primary_listener->pg_whoami.osd;

  PGBackend* primary_backend = get_primary_backend();
  if (!primary_backend) {
    std::cerr << "ERROR: No primary backend found" << std::endl;
    return true;
  }

  const pg_pool_t* pool_info = osdmap->get_pg_pool(pool_id);
  ceph_assert(pool_info != nullptr);

  MockPgScrubBeListener pg_scrub_listener(primary_backend);
  pg_scrub_listener.pool = std::make_shared<PGPool>(
    osdmap, pool_id, *pool_info, "test_pool");
  pg_scrub_listener.primary = primary_shard;
  pg_scrub_listener.info.pgid = spgid;

  // Get the acting set from the OSDMap (already retrieved earlier in the function)
  spg_t primary_spg;
  osdmap->get_primary_shard(pgid, &primary_spg);
  
  std::set<pg_shard_t> acting_set;
  for (size_t i = 0; i < acting_osds.size(); ++i) {
    acting_set.insert(pg_shard_t(acting_osds[i], shard_id_t(i)));
  }

  TestScrubBackend scrub_backend(*scrub_listener, pg_scrub_listener,
                                 pg_shard_t(acting_primary, shard_id_t(primary_spg.shard)),
                                 false, scrub_level_t::deep, acting_set);

  scrub_backend.new_chunk();

  for (const auto& [pg_shard, smap] : scrub_maps) {
    scrub_backend.insert_faked_smap(pg_shard, smap);
  }

  auto result = scrub_backend.scrub_compare_maps(false, *snap_reader);

  bool scrub_found_corruption = !result.inconsistent_objs.empty();
  
  // Verify attributes are consistent across all shards
  if (!scrub_found_corruption && !skip_verify) {
    // Get attributes from the primary shard as reference
    std::map<std::string, ceph::buffer::ptr, std::less<>> primary_attrs;
    int primary_shard_id = (int)primary_spg.shard;
    
    ghobject_t primary_ghoid(hoid, ghobject_t::NO_GEN, primary_spg.shard);
    TestPG* primary_pg = get_test_pg_by_shard(primary_shard_id);
    OsdTestFixture* primary_fixture = primary_pg ? get_osd_fixture(primary_pg->pg_whoami.osd) : nullptr;
    
    if (primary_pg && primary_fixture && primary_pg->ch && primary_fixture->store) {
      int r = primary_fixture->store->getattrs(primary_pg->ch, primary_ghoid, primary_attrs);
      if (r == 0) {
        // Compare attributes on all other shards
        for (size_t i = 0; i < acting_osds.size(); ++i) {
          if (static_cast<int>(i) == primary_shard_id) continue; // Skip primary, already have it
          
          shard_id_t shard_id(i);
          ghobject_t shard_ghoid(hoid, ghobject_t::NO_GEN, shard_id);
          
          TestPG* shard_pg = get_test_pg_by_shard(i);
          OsdTestFixture* shard_fixture = shard_pg ? get_osd_fixture(shard_pg->pg_whoami.osd) : nullptr;
          
          if (!shard_pg || !shard_fixture || !shard_pg->ch || !shard_fixture->store) {
            std::cerr << "WARNING: Cannot verify attributes for shard " << i << std::endl;
            continue;
          }
          
          std::map<std::string, ceph::buffer::ptr, std::less<>> shard_attrs;
          r = shard_fixture->store->getattrs(shard_pg->ch, shard_ghoid, shard_attrs);
          if (r < 0) {
            std::cerr << "WARNING: Failed to get attributes for shard " << i << ": "
                      << cpp_strerror(r) << std::endl;
            continue;
          }

          if (!pool_info->is_nonprimary_shard(shard_id))
          {
            // Compare attribute sets
            if (primary_attrs.size() != shard_attrs.size()) {
              std::cerr << "ERROR: Attribute count mismatch for object " << obj_name
                        << " - primary has " << primary_attrs.size()
                        << " attrs, shard " << i << " has " << shard_attrs.size() << std::endl;

              // List attributes on this shard to show what's different
              std::cerr << "  Shard " << i << " attributes: ";
              for (const auto& [attr_name, attr_ptr] : shard_attrs) {
                std::cerr << attr_name << " ";
              }
              std::cerr << std::endl;

              scrub_found_corruption = true;
            }

            // Compare each attribute
            for (const auto& [attr_name, primary_bl] : primary_attrs) {
              auto shard_it = shard_attrs.find(attr_name);
              if (shard_it == shard_attrs.end()) {
                std::cerr << "ERROR: Attribute '" << attr_name << "' missing on shard " << i
                          << " for object " << obj_name << std::endl;
                scrub_found_corruption = true;
                continue;
              }

              // Convert buffer::ptr to bufferlist for comparison
              bufferlist primary_bl_list, shard_bl_list;
              primary_bl_list.append(primary_bl);
              shard_bl_list.append(shard_it->second);

              if (!primary_bl_list.contents_equal(shard_bl_list)) {
                std::cerr << "ERROR: Attribute '" << attr_name << "' value mismatch on shard " << i
                          << " for object " << obj_name << std::endl;
                scrub_found_corruption = true;
              }
            }

            // Check for extra attributes on shard
            for (const auto& [attr_name, shard_bl] : shard_attrs) {
              if (primary_attrs.find(attr_name) == primary_attrs.end()) {
                std::cerr << "ERROR: Extra attribute '" << attr_name << "' on shard " << i
                          << " for object " << obj_name << std::endl;
                scrub_found_corruption = true;
              }
            }
          }
        }
      }
    }
  }
  
  if (!skip_verify) {
    verify_object(obj_name);
  }
  
  return scrub_found_corruption;
}


bufferlist PGBackendTestFixture::create_random_buffer(size_t size)
{
  bufferlist bl;

  if (size == 0) {
    return bl;
  }

  ceph::buffer::ptr bp(size);

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<unsigned char> dis(0, 255);

  for (size_t i = 0; i < size; ++i) {
    bp[i] = dis(gen);
  }

  bl.append(std::move(bp));
  return bl;
}

void PGBackendTestFixture::corrupt_shard_data(const hobject_t& obj, pg_shard_t shard)
{
  auto fixture_it = osd_fixtures.find(shard.osd);
  if (fixture_it == osd_fixtures.end()) {
    std::cerr << "ERROR: No fixture for shard " << shard.osd << std::endl;
    return;
  }
  
  auto& fixture = fixture_it->second;
  TestPG* shard_pg = get_test_pg(shard);
  if (!fixture->store || !shard_pg || !shard_pg->ch) {
    std::cerr << "ERROR: No store or collection handle for shard " << shard.osd << std::endl;
    return;
  }

  ghobject_t ghoid(obj, ghobject_t::NO_GEN, shard.shard);

  struct stat st;
  int r = fixture->store->stat(shard_pg->ch, ghoid, &st);
  if (r < 0) {
    std::cerr << "ERROR: Failed to stat object on shard " << shard.osd
              << ": " << cpp_strerror(r) << std::endl;
    return;
  }

  uint64_t size = st.st_size;
  if (size == 0) {
    std::cerr << "WARNING: Object has zero size on shard " << shard.osd << std::endl;
    return;
  }

  bufferlist zero_bl;
  zero_bl.append_zero(size);

  ObjectStore::Transaction t;
  t.write(shard_pg->ch->cid, ghoid, 0, size, zero_bl);

  r = fixture->store->queue_transaction(shard_pg->ch, std::move(t));
  if (r < 0) {
    std::cerr << "ERROR: Failed to corrupt object on shard " << shard.osd
              << ": " << cpp_strerror(r) << std::endl;
    return;
  }

  std::cout << "Corrupted shard " << shard.osd << " data for object " << obj
            << " (wrote " << size << " bytes of zeros)" << std::endl;
}
