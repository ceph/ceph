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

void PGBackendTestFixture::setup_ec_pool()
{
  CephContext *cct = g_ceph_context;

  int num_osds = k + m;

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

  pg_pool_t pool = OSDMapTestHelpers::create_ec_pool(k, m, stripe_unit * k, pool_flags);
  OSDMapTestHelpers::add_pool(osdmap, pool_id, pool);

  pgid = pg_t(0, pool_id);
  spgid = spg_t(pgid, shard_id_t(0));

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

  ObjectStore::Transaction t;
  for (int i = 0; i < num_osds; i++) {
    spg_t shard_spgid(pgid, shard_id_t(i));
    coll_t shard_coll(shard_spgid);
    auto shard_ch = store->create_new_collection(shard_coll);
    t.create_collection(shard_coll, 0);

    colls[i] = shard_coll;
    chs[i] = shard_ch;

    if (i == 0) {
      ch = shard_ch;
      coll = shard_coll;
    }
  }

  ASSERT_EQ(store->queue_transaction(ch, std::move(t)), 0);

  const pg_pool_t* pool_ptr = OSDMapTestHelpers::get_pool(osdmap, pool_id);
  ceph_assert(pool_ptr != nullptr);

  for (int i = 0; i < num_osds; i++) {
    auto shard_listener = std::make_unique<MockPGBackendListener>(
      osdmap, pool_id, dpp.get(), pg_shard_t(i, shard_id_t(i)));

    // Initialize the listener's own info.pgid so OSDMap queries work
    shard_listener->info.pgid = spg_t(pgid, shard_id_t(i));

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

    shard_listener->set_store(store.get(), chs[i]);
    shard_listener->set_event_loop(event_loop.get());

    auto shard_lru = std::make_unique<ECExtentCache::LRU>(1024 * 1024 * 100);
    auto shard_ec_switch = std::make_unique<ECSwitch>(
      shard_listener.get(), colls[i], chs[i], store.get(),
      cct, ec_impl, stripe_unit * k, *shard_lru);

    listeners[i] = std::move(shard_listener);
    lrus[i] = std::move(shard_lru);
    backends[i] = std::move(shard_ec_switch);
  }

  // Create MockMessenger and register a single handler that routes to backends
  messenger = std::make_unique<MockMessenger>(event_loop.get(), cct);
  
  // Set up epoch getter for MockMessenger to enable epoch-based message filtering
  messenger->set_epoch_getter([this](int osd) -> epoch_t {
    // Get the epoch from the listener's osdmap
    auto it = listeners.find(osd);
    if (it != listeners.end()) {
      return it->second->pgb_get_osdmap_epoch();
    }
    // If listener doesn't exist yet, use the test fixture's osdmap
    return osdmap->get_epoch();
  });
  
  // Create an OpTracker for wrapping messages in OpRequestRef
  // This is needed because PGBackend::_handle_message expects OpRequestRef
  // Store as member variable so it can be properly shut down in TearDown()
  op_tracker = std::make_shared<OpTracker>(cct, true, 1);
  
  // Helper lambda to create a typed handler that wraps messages and routes to backends
  auto make_backend_handler = [this]<typename MsgType>(int msg_type) {
    messenger->register_typed_handler<MsgType>(msg_type,
      [this](int from_osd, int to_osd, MsgType* m) -> bool {
        auto it = backends.find(to_osd);
        ceph_assert(it != backends.end());
        OpRequestRef op = this->op_tracker->create_request<OpRequest, Message*>(m);
        return it->second->_handle_message(op);
      });
  };
  
  // Register typed handlers for all EC message types
  make_backend_handler.template operator()<MOSDECSubOpWrite>(MSG_OSD_EC_WRITE);
  make_backend_handler.template operator()<MOSDECSubOpWriteReply>(MSG_OSD_EC_WRITE_REPLY);
  make_backend_handler.template operator()<MOSDECSubOpRead>(MSG_OSD_EC_READ);
  make_backend_handler.template operator()<MOSDECSubOpReadReply>(MSG_OSD_EC_READ_REPLY);

  for (int i = 0; i < num_osds; i++) {
    listeners[i]->set_messenger(messenger.get());
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
  spgid = spg_t(pgid, shard_id_t::NO_SHARD);
  
  // Set up pg_temp to define the acting set with OSD 0 as primary
  std::vector<int> acting;
  for (int i = 0; i < num_replicas; i++) {
    acting.push_back(i);
  }
  OSDMapTestHelpers::set_pg_acting(osdmap, pgid, acting);
  OSDMapTestHelpers::set_pg_acting_primary(osdmap, pgid, 0);

  ObjectStore::Transaction t;
  spg_t replica_spgid(pgid, shard_id_t::NO_SHARD);
  coll_t replica_coll(replica_spgid);
  auto replica_ch = store->create_new_collection(replica_coll);
  t.create_collection(replica_coll, 0);

  ASSERT_EQ(store->queue_transaction(replica_ch, std::move(t)), 0);

  // All replicas share the same collection
  for (int i = 0; i < num_replicas; i++) {
    colls[i] = replica_coll;
    chs[i] = replica_ch;
  }

  ch = replica_ch;
  coll = replica_coll;

  const pg_pool_t* pool_ptr = OSDMapTestHelpers::get_pool(osdmap, pool_id);
  ceph_assert(pool_ptr != nullptr);

  for (int i = 0; i < num_replicas; i++) {
    auto replica_listener = std::make_unique<MockPGBackendListener>(
      osdmap, pool_id, dpp.get(), pg_shard_t(i, shard_id_t::NO_SHARD));

    // Initialize the listener's own info.pgid so OSDMap queries work
    replica_listener->info.pgid = spg_t(pgid, shard_id_t::NO_SHARD);

    // For replicated pools, use NO_SHARD for all replicas
    for (int j = 0; j < num_replicas; j++) {
      replica_listener->shardset.insert(pg_shard_t(j, shard_id_t::NO_SHARD));

      // Initialize shard_info for each replica - required by backend
      pg_info_t replica_pg_info;
      replica_pg_info.pgid = spg_t(pgid, shard_id_t::NO_SHARD);
      replica_listener->shard_info[pg_shard_t(j, shard_id_t::NO_SHARD)] = replica_pg_info;

      // Initialize shard_missing for each replica - required by backend
      pg_missing_t replica_missing;
      replica_listener->shard_missing[pg_shard_t(j, shard_id_t::NO_SHARD)] = replica_missing;
    }

    replica_listener->set_store(store.get(), chs[i]);
    replica_listener->set_event_loop(event_loop.get());

    auto replica_backend = std::make_unique<ReplicatedBackend>(
      replica_listener.get(), colls[i], chs[i], store.get(), cct);

    listeners[i] = std::move(replica_listener);
    backends[i] = std::move(replica_backend);
  }

  // Create MockMessenger and register a single handler that routes to backends
  messenger = std::make_unique<MockMessenger>(event_loop.get(), cct);
  
  // Set up epoch getter for MockMessenger to enable epoch-based message filtering
  messenger->set_epoch_getter([this](int osd) -> epoch_t {
    // Get the epoch from the listener's osdmap
    auto it = listeners.find(osd);
    if (it != listeners.end()) {
      return it->second->pgb_get_osdmap_epoch();
    }
    // If listener doesn't exist yet, use the test fixture's osdmap
    return osdmap->get_epoch();
  });
  
  // Create an OpTracker for wrapping messages in OpRequestRef
  // This is needed because PGBackend::_handle_message expects OpRequestRef
  // Store as member variable so it can be properly shut down in TearDown()
  op_tracker = std::make_shared<OpTracker>(cct, true, 1);
  
  // Helper lambda to create a typed handler that wraps messages and routes to backends
  auto make_backend_handler = [this]<typename MsgType>(int msg_type) {
    messenger->register_typed_handler<MsgType>(msg_type,
      [this](int from_osd, int to_osd, MsgType* m) -> bool {
        auto it = backends.find(to_osd);
        ceph_assert(it != backends.end());
        OpRequestRef op = this->op_tracker->create_request<OpRequest, Message*>(m);
        return it->second->_handle_message(op);
      });
  };
  
  // Register typed handlers for replicated backend message types
  make_backend_handler.template operator()<MOSDRepOp>(MSG_OSD_REPOP);
  make_backend_handler.template operator()<MOSDRepOpReply>(MSG_OSD_REPOPREPLY);

  for (int i = 0; i < num_replicas; i++) {
    listeners[i]->set_messenger(messenger.get());
  }
}

int PGBackendTestFixture::do_transaction_and_complete(
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

  bool completed = false;
  int completion_result = -1;
  Context *on_complete = new LambdaContext([&completed, &completion_result, on_write_complete](int r) {
    completed = true;
    completion_result = r;
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

  event_loop->run_until_idle();

  if (!completed) {
    completion_result = -EINPROGRESS;
  }

  return completion_result;
}

int PGBackendTestFixture::create_and_write(
  const std::string& obj_name,
  const std::string& data)
{
  // Auto-generate version
  eversion_t at_version = get_next_version();
  
  hobject_t hoid = make_test_object(obj_name);
  PGTransactionUPtr pg_t = std::make_unique<PGTransaction>();
  pg_t->create(hoid);

  // Use persistent OBC so attr_cache is maintained across operations
  ObjectContextRef obc = get_or_create_obc(hoid, false, 0);
  pg_t->obc_map[hoid] = obc;

  // Note: We do NOT pre-seed attr_cache here. For a new object, attr_cache
  // should be empty. ECTransaction::attr_updates() will update attr_cache
  // with the new OI from PGTransaction::attr_updates during the transaction.

  // Track outstanding write
  outstanding_writes[hoid]++;

  bufferlist bl;
  bl.append(data);
  pg_t->write(hoid, 0, bl.length(), bl);

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

  // Create completion lambda for write-specific cleanup
  auto write_complete = [this, hoid, obc](int r) {
    // Note: we do NOT update obc->obs after completion — it was already
    // updated above before submit, matching PrimaryLogPG behavior.
    // ECTransaction::attr_updates() will have updated attr_cache[OI_ATTR]
    // to the new encoded OI during the transaction.

    // Decrement outstanding writes counter
    if (outstanding_writes[hoid] > 0) {
      outstanding_writes[hoid]--;
      // Clean up the counter if it reaches 0, but don't clear attr_cache here.
      // The attr_cache will be cleared on on_change() events.
      if (outstanding_writes[hoid] == 0) {
        outstanding_writes.erase(hoid);
      }
    }

    if (r != 0 && r != -EINPROGRESS) {
      // Transaction failed — roll back OBC state.
      // In production this would be handled differently, but for tests
      // we just reset to a clean state.
      obc->obs.oi = object_info_t(hoid);
      obc->obs.exists = false;
      obc->attr_cache.clear();
      outstanding_writes.erase(hoid);
    }
  };

  int result = do_transaction_and_complete(
    hoid, std::move(pg_t), delta_stats, at_version, std::move(log_entries), write_complete);

  return result;
}

ObjectContextRef PGBackendTestFixture::get_object_context(
  const hobject_t& hoid)
{
  PGBackend* primary_backend = get_primary_backend();
  ObjectContextRef obc = std::make_shared<ObjectContext>();
  obc->obs.oi = object_info_t(hoid);
  obc->obs.exists = false;
  obc->ssc = nullptr;
  
  // Try to read the ObjectInfo from the store
  ghobject_t ghoid(hoid, ghobject_t::NO_GEN, primary_backend->get_parent()->whoami_shard().shard);
  ceph::buffer::ptr value_ptr;
  int r = store->getattr(ch, ghoid, OI_ATTR, value_ptr);
  ceph_assert(r >= 0 && value_ptr.length() > 0);

  bufferlist bl;
  bl.append(value_ptr);
  auto p = bl.cbegin();
  obc->obs.oi.decode(p);
  obc->obs.exists = true;
  
  return obc;
}

int PGBackendTestFixture::write(
  const std::string& obj_name,
  uint64_t offset,
  const std::string& data,
  uint64_t object_size)
{
  hobject_t hoid = make_test_object(obj_name);
  PGTransactionUPtr pg_t = std::make_unique<PGTransaction>();

  ObjectContextRef obc = get_or_create_obc(hoid, true, object_size);
  pg_t->obc_map[hoid] = obc;

  // Track outstanding write
  outstanding_writes[hoid]++;

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
  eversion_t at_version = get_next_version();

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

  // Create completion lambda for write-specific cleanup
  auto write_complete = [this, hoid, obc, prior_version, object_size](int r) {
    // Decrement outstanding writes counter
    if (outstanding_writes[hoid] > 0) {
      outstanding_writes[hoid]--;
      // Clean up the counter if it reaches 0, but don't clear attr_cache here.
      // The attr_cache will be cleared on on_change() events.
      if (outstanding_writes[hoid] == 0) {
        outstanding_writes.erase(hoid);
      }
    }

    if (r != 0 && r != -EINPROGRESS) {
      // Roll back OBC on failure
      obc->obs.oi.version = prior_version;
      obc->obs.oi.size = object_size;
      obc->attr_cache.clear();
      outstanding_writes.erase(hoid);
    }
  };

  int result = do_transaction_and_complete(
    hoid, std::move(pg_t), delta_stats, at_version, std::move(log_entries), write_complete);

  return result;
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

void PGBackendTestFixture::verify_object(
  const std::string& obj_name,
  const std::string& expected_data,
  size_t offset,
  size_t object_size)
{
  bufferlist read_data;
  int read_result = read_object(obj_name, offset, expected_data.length(), read_data, object_size);

  EXPECT_GE(read_result, 0) << "Read should complete successfully";
  EXPECT_EQ(read_data.length(), expected_data.length()) << "Read data length should match";
  
  if (read_data.length() == expected_data.length()) {
    std::string read_string(read_data.c_str(), read_data.length());
    EXPECT_EQ(read_string, expected_data) << "Data should match";
  }
}

void PGBackendTestFixture::create_and_write_verify(
  const std::string& obj_name,
  const std::string& data)
{
  int result = create_and_write(obj_name, data);
  
  EXPECT_GE(result, 0) << "Write should complete successfully";
  
  // Always verify - tests should only use this helper when success is expected
  verify_object(obj_name, data, 0, data.length());
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
  EXPECT_GE(result, 0) << "Write should complete successfully" << msg_suffix;
  
  // Always verify - tests should only use this helper when success is expected
  bufferlist read_data;
  int read_result = read_object(obj_name, offset, data.length(), read_data,
                                 std::max(object_size, offset + data.length()));
  
  EXPECT_GE(read_result, 0) << "Read should complete successfully" << msg_suffix;
  EXPECT_EQ(read_data.length(), data.length()) << "Read data length should match" << msg_suffix;
  
  if (read_data.length() == data.length()) {
    std::string read_string(read_data.c_str(), read_data.length());
    EXPECT_EQ(read_string, data) << "Written data should match" << msg_suffix;
  }
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

  // Step 2: Update the osdmap in all listeners
  for (auto& [instance, list] : listeners) {
    if (list) {
      list->osdmap = new_osdmap;
    }
  }

  // Step 3: Clear all attr_caches before on_change()
  // The cached OI attributes may be stale after a peering event
  clear_all_attr_caches();

  // Step 4: Schedule on_change() calls as event loop actions
  // This allows them to be delayed and processed after the new epoch
  for (auto& [instance, be] : backends) {
    if (be) {
      PGBackend* backend_ptr = be.get();
      event_loop->schedule_peering_event(instance, [backend_ptr]() {
        backend_ptr->on_change();
      });
    }
  }
  event_loop->run_until_idle();
}

void PGBackendTestFixture::cleanup_data_dir()
{
  // Only clean up if the directory exists and hasn't been cleaned already
  if (!data_dir.empty() && std::filesystem::exists(data_dir)) {
    std::error_code ec;
    std::filesystem::remove_all(data_dir, ec);
    // Silently ignore errors during cleanup - we tried our best
  }
}

void PGBackendTestFixture::clear_all_attr_caches()
{
  // Clear attr_cache for all objects. This is called on on_change() to
  // invalidate cached attributes that might be stale after a peering event.
  for (auto& [hoid, obc] : object_contexts) {
    if (obc) {
      obc->attr_cache.clear();
    }
  }
}
