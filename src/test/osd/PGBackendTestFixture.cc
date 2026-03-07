// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "test/osd/PGBackendTestFixture.h"
#include "common/errno.h"

// ============================================================================
// PGBackendTestFixture - non-template method implementations
// ============================================================================

void PGBackendTestFixture::setup_ec_pool()
{
  CephContext *cct = g_ceph_context;

  // Create OSDMap with EC pool
  osdmap = std::make_shared<OSDMap>();
  osdmap->set_max_osd(k + m);

  // First, create OSD entries in the base map
  for (int i = 0; i < k + m; i++) {
    osdmap->set_state(i, CEPH_OSD_EXISTS);
    osdmap->set_weight(i, CEPH_OSD_OUT);
    osdmap->crush->set_item_name(i, "osd." + std::to_string(i));
  }

  // Now use incremental to set OSDs as up and with proper features
  OSDMap::Incremental inc(osdmap->get_epoch() + 1);
  inc.fsid = osdmap->get_fsid();

  for (int i = 0; i < k + m; i++) {
    // Mark OSD as up (EXISTS is already set above)
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

  // Create EC pool configuration and add it to OSDMap
  pg_pool_t pool = OSDMapTestHelpers::create_ec_pool(k, m, stripe_width, pool_id);
  OSDMapTestHelpers::add_pool(osdmap, pool_id, pool);

  // If ec_optimizations is false, clear FLAG_EC_OPTIMIZATIONS now, before
  // any ECSwitch backends are created.  This ensures that is_optimized_actual
  // (captured in the ECSwitch constructor) is consistent with the live pool
  // from the very start, avoiding the assertion in ECSwitch::is_optimized().
  if (!ec_optimizations) {
    OSDMapTestHelpers::clear_pool_flag(
      osdmap, pool_id, pg_pool_t::FLAG_EC_OPTIMIZATIONS);
  }

  pgid = pg_t(0, pool_id);
  spgid = spg_t(pgid, shard_id_t(0));

  // Set up acting set in OSDMap using pg_temp
  OSDMapTestHelpers::setup_ec_pg(osdmap, pgid, k, m, 0);

  // Create erasure code implementation based on plugin configuration
  if (ec_plugin == "mock") {
    // Use mock implementation for testing
    ec_impl = std::make_shared<MockErasureCode>(k, k + m);
  } else {
    // Use real EC plugin
    ErasureCodeProfile profile;
    profile["k"] = std::to_string(k);
    profile["m"] = std::to_string(m);
    profile["plugin"] = ec_plugin;

    // Add technique if specified
    if (!ec_technique.empty()) {
      profile["technique"] = ec_technique;
    }

    // Set stripe width in the profile
    profile["stripe_unit"] = std::to_string(stripe_width / k);

    std::stringstream ss;
    int ret = ceph::ErasureCodePluginRegistry::instance().factory(
      ec_plugin,
      g_conf().get_val<std::string>("erasure_code_dir"),
      profile,
      &ec_impl,
      &ss);

    if (ret != 0) {
      FAIL() << "Failed to create EC plugin '" << ec_plugin << "': " << ss.str();
      return;
    }
  }

  // Create collections for all shards - EC backend needs these for reads/writes
  ObjectStore::Transaction t;
  for (int i = 0; i < k + m; i++) {
    spg_t shard_spgid(pgid, shard_id_t(i));
    coll_t shard_coll(shard_spgid);
    auto shard_ch = store->create_new_collection(shard_coll);
    t.create_collection(shard_coll, 0);

    // Store collection info for this shard
    colls[i] = shard_coll;
    chs[i] = shard_ch;

    // Use shard 0's collection as the primary collection handle
    if (i == 0) {
      ch = shard_ch;
      coll = shard_coll;
    }
  }

  ASSERT_EQ(store->queue_transaction(ch, std::move(t)), 0);

  // Get pool from OSDMap
  const pg_pool_t* pool_ptr = OSDMapTestHelpers::get_pool(osdmap, pool_id);
  ceph_assert(pool_ptr != nullptr);

  // Create a listener, LRU, and ECSwitch for each shard
  for (int i = 0; i < k + m; i++) {
    // Create mock listener for this shard - use factory if provided, else default
    // Initially, shard 0 is the primary
    std::unique_ptr<MockPGBackendListener> shard_listener;
    if (listener_factory) {
      shard_listener = listener_factory(
        i,
        osdmap,
        pool_id,
        dpp.get(),
        pg_shard_t(i, shard_id_t(i)),
        pg_shard_t(0, shard_id_t(0)));
    } else {
      shard_listener = std::make_unique<MockPGBackendListener>(
        osdmap,
        pool_id,
        dpp.get(),
        pg_shard_t(i, shard_id_t(i)),
        pg_shard_t(0, shard_id_t(0))  // primary is shard 0
      );
    }

    // Set up acting set - populate all shard-related structures
    for (int j = 0; j < k + m; j++) {
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

    // Set the store, event loop, and op tracker on the listener
    shard_listener->set_store(store.get(), chs[i]);
    shard_listener->set_event_loop(event_loop.get());
    shard_listener->set_op_tracker(op_tracker.get());

    // Initialize ECSwitch infrastructure for this shard
    auto shard_lru = std::make_unique<ECExtentCache::LRU>(1024 * 1024 * 100);
    auto shard_ec_switch = std::make_unique<ECSwitch>(
      shard_listener.get(), colls[i], chs[i], store.get(),
      cct, ec_impl, stripe_width, *shard_lru);

    // Store the instances
    listeners[i] = std::move(shard_listener);
    lrus[i] = std::move(shard_lru);
    backends[i] = std::move(shard_ec_switch);
  }

  // Now that all shards are created, set up message routing
  for (int i = 0; i < k + m; i++) {
    message_router[i] = [this, i](OpRequestRef op) -> bool {
      return backends[i]->_handle_message(op);
    };
  }

  // Configure all listeners to use the message router
  for (int i = 0; i < k + m; i++) {
    listeners[i]->set_message_router(&message_router);
    listeners[i]->set_handle_message_callback(
      [this, i](OpRequestRef op) -> bool {
        return backends[i]->_handle_message(op);
      });
  }

  // Set up convenience pointers to primary shard (shard 0)
  listener = listeners[0].get();
  backend = backends[0].get();
}

void PGBackendTestFixture::setup_replicated_pool()
{
  CephContext *cct = g_ceph_context;

  // Create OSDMap with replicated pool
  osdmap = std::make_shared<OSDMap>();
  osdmap->set_max_osd(num_replicas);
  osdmap->set_state(0, CEPH_OSD_EXISTS | CEPH_OSD_UP);

  pg_pool_t pool;
  pool.type = pg_pool_t::TYPE_REPLICATED;
  pool.size = num_replicas;
  pool.min_size = min_size;
  pool.crush_rule = 0;

  osdmap->inc_epoch();

  // Add the replicated pool to OSDMap (authoritative source)
  OSDMapTestHelpers::add_pool(osdmap, pool_id, pool);

  pgid = pg_t(0, pool_id);
  spgid = spg_t(pgid, shard_id_t::NO_SHARD);

  // Create a single collection for replicated pool (all replicas share it)
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

  // Set primary collection handle
  ch = replica_ch;
  coll = replica_coll;

  // Get pool from OSDMap (authoritative source)
  const pg_pool_t* pool_ptr = OSDMapTestHelpers::get_pool(osdmap, pool_id);
  ceph_assert(pool_ptr != nullptr);

  // Create a listener and ReplicatedBackend for each replica
  for (int i = 0; i < num_replicas; i++) {
    // Create mock listener for this replica - use factory if provided, else default
    // Initially, replica 0 is the primary
    std::unique_ptr<MockPGBackendListener> replica_listener;
    if (listener_factory) {
      replica_listener = listener_factory(
        i,
        osdmap,
        pool_id,
        dpp.get(),
        pg_shard_t(i, shard_id_t::NO_SHARD),
        pg_shard_t(0, shard_id_t::NO_SHARD));
    } else {
      replica_listener = std::make_unique<MockPGBackendListener>(
        osdmap,
        pool_id,
        dpp.get(),
        pg_shard_t(i, shard_id_t::NO_SHARD),
        pg_shard_t(0, shard_id_t::NO_SHARD)  // primary is replica 0
      );
    }

    // Set up acting set - populate all replica-related structures
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

    // Set the store, event loop, and op tracker on the listener
    replica_listener->set_store(store.get(), chs[i]);
    replica_listener->set_event_loop(event_loop.get());
    replica_listener->set_op_tracker(op_tracker.get());

    // Create ReplicatedBackend for this replica
    auto replica_backend = std::make_unique<ReplicatedBackend>(
      replica_listener.get(), colls[i], chs[i], store.get(), cct);

    // Store the instances
    listeners[i] = std::move(replica_listener);
    backends[i] = std::move(replica_backend);
  }

  // Now that all replicas are created, set up message routing
  for (int i = 0; i < num_replicas; i++) {
    message_router[i] = [this, i](OpRequestRef op) -> bool {
      return backends[i]->_handle_message(op);
    };
  }

  // Configure all listeners to use the message router
  for (int i = 0; i < num_replicas; i++) {
    listeners[i]->set_message_router(&message_router);
    listeners[i]->set_handle_message_callback(
      [this, i](OpRequestRef op) -> bool {
        return backends[i]->_handle_message(op);
      });
  }

  // Set up convenience pointers to primary replica (replica 0)
  listener = listeners[0].get();
  backend = backends[0].get();
}

int PGBackendTestFixture::do_transaction_and_complete(
  const hobject_t& hoid,
  PGTransactionUPtr pg_t,
  const object_stat_sum_t& delta_stats,
  const eversion_t& at_version,
  std::vector<pg_log_entry_t> log_entries)
{
  eversion_t trim_to(0, 0);
  eversion_t pg_committed_to(0, 0);
  std::optional<pg_hit_set_history_t> hset_history;

  bool completed = false;
  int completion_result = -1;
  Context *on_complete = new LambdaContext([&completed, &completion_result](int r) {
    completed = true;
    completion_result = r;
  });

  ceph_tid_t tid = 1;
  osd_reqid_t reqid(entity_name_t::OSD(0), 0, tid);

  // Submit the transaction to the primary instance (instance 0) - polymorphic call
  backend->submit_transaction(
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

  // Run the unified event loop to process all scheduled events
  event_loop->run_until_idle(10000);  // Max 10000 events to prevent infinite loops

  if (!completed) {
    throw std::runtime_error("Transaction did not complete within timeout");
  }

  return completion_result;
}

int PGBackendTestFixture::create_and_write(
  const std::string& obj_name,
  const std::string& data,
  const eversion_t& at_version)
{
  // Create object and transaction
  hobject_t hoid = make_test_object(obj_name);
  PGTransactionUPtr pg_t = std::make_unique<PGTransaction>();
  pg_t->create(hoid);

  // Add ObjectContext - initially object doesn't exist
  ObjectContextRef obc = make_object_context(hoid, false, 0);
  pg_t->obc_map[hoid] = obc;

  // Add write operation
  bufferlist bl;
  bl.append(data);
  pg_t->write(hoid, 0, bl.length(), bl);

  // Prepare stats
  object_stat_sum_t delta_stats;
  delta_stats.num_objects = 1;
  delta_stats.num_bytes = bl.length();

  // Prepare log entry
  std::vector<pg_log_entry_t> log_entries;
  pg_log_entry_t entry;
  entry.mark_unrollbackable();
  entry.op = pg_log_entry_t::MODIFY;
  entry.soid = hoid;
  entry.version = at_version;
  entry.prior_version = eversion_t(0, 0);
  log_entries.push_back(entry);

  // Call the hook before executing the transaction.
  // ECPeeringTestFixture overrides this to append log entries to PeeringState.
  pre_transaction_hook(hoid, log_entries, at_version);

  // Execute transaction
  int result = do_transaction_and_complete(
    hoid, std::move(pg_t), delta_stats, at_version, std::move(log_entries));

  // After successful write, update the ObjectContext to reflect the object exists
  if (result == 0) {
    obc->obs.exists = true;
    obc->obs.oi.size = bl.length();
    obc->obs.oi.version = at_version;
  }

  return result;
}

int PGBackendTestFixture::write(
  const std::string& obj_name,
  uint64_t offset,
  const std::string& data,
  const eversion_t& prior_version,
  const eversion_t& at_version,
  uint64_t object_size)
{
  // Create object and transaction
  hobject_t hoid = make_test_object(obj_name);
  PGTransactionUPtr pg_t = std::make_unique<PGTransaction>();

  // Object already exists, so we need ObjectContext with exists=true
  ObjectContextRef obc = make_object_context(hoid, true, object_size);
  obc->obs.oi.version = prior_version;
  pg_t->obc_map[hoid] = obc;

  // Add write operation at offset
  bufferlist bl;
  bl.append(data);
  pg_t->write(hoid, offset, bl.length(), bl);

  // Prepare stats for partial write
  object_stat_sum_t delta_stats;
  // Calculate size change - if write extends beyond current size
  uint64_t new_size = std::max(object_size, offset + bl.length());
  if (new_size > object_size) {
    delta_stats.num_bytes = new_size - object_size;
  } else {
    delta_stats.num_bytes = 0;  // Size doesn't change for partial write within existing size
  }

  // Prepare log entry
  std::vector<pg_log_entry_t> log_entries;
  pg_log_entry_t entry;
  // Don't mark as unrollbackable - partial writes need rollback support
  entry.op = pg_log_entry_t::MODIFY;
  entry.soid = hoid;
  entry.version = at_version;
  entry.prior_version = prior_version;
  log_entries.push_back(entry);

  // Execute transaction
  int result = do_transaction_and_complete(
    hoid, std::move(pg_t), delta_stats, at_version, std::move(log_entries));

  // After successful write, update the ObjectContext to reflect the new state
  if (result == 0) {
    obc->obs.oi.size = new_size;
    obc->obs.oi.version = at_version;
  }

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
    // EC: Use asynchronous read
    bool completed = false;
    int completion_result = -1;

    // Create the read list with alignment
    std::list<std::pair<ec_align_t, std::pair<bufferlist*, Context*>>> to_read;

    ec_align_t align(offset, length, 0);  // offset, size, flags

    Context *read_complete = new LambdaContext([&completed, &completion_result](int r) {
      completed = true;
      completion_result = r;
    });

    to_read.push_back(std::make_pair(align, std::make_pair(&out_data, read_complete)));

    // Create overall completion context
    Context *on_complete = new LambdaContext([](int r) {
      // Overall completion - individual read contexts handle the actual completion
    });

    // Submit the async read to the primary instance (instance 0)
    // Cast to ECSwitch to access objects_read_async
    ECSwitch* ec_switch = dynamic_cast<ECSwitch*>(backend);
    ceph_assert(ec_switch != nullptr);

    ec_switch->objects_read_async(
      hoid,
      object_size,
      to_read,
      on_complete,
      false  // fast_read
    );

    // Keep running event loop until read completes
    event_loop->run_until_idle(10000);

    if (!completed) {
      throw std::runtime_error("Read operation did not complete within timeout");
    }

    return completion_result;
  } else {
    // Replicated: Use synchronous read
    ReplicatedBackend* rep_backend = dynamic_cast<ReplicatedBackend*>(backend);
    ceph_assert(rep_backend != nullptr);

    int result = rep_backend->objects_read_sync(
      hoid,
      offset,
      length,
      0,  // op_flags
      &out_data
    );

    return result;
  }
}

void PGBackendTestFixture::update_osdmap(
  std::shared_ptr<OSDMap> new_osdmap,
  std::optional<pg_shard_t> new_primary)
{
  // Step 1: Call on_change() on all backends to clear in-flight operations
  for (auto& [instance, be] : backends) {
    if (be) {
      be->on_change();
    }
  }

  // Step 2: Update the osdmap reference
  osdmap = new_osdmap;

  // Step 3: Update the primary if specified
  if (new_primary.has_value()) {
    // Update all listeners with the new primary
    for (auto& [instance, list] : listeners) {
      if (list) {
        list->primary = new_primary.value();
      }
    }

    // Update convenience pointers if the primary changed
    int new_primary_instance = pool_type == EC ?
      new_primary.value().shard.id : new_primary.value().osd;

    if (new_primary_instance >= 0 &&
        new_primary_instance < static_cast<int>(listeners.size())) {
      listener = listeners[new_primary_instance].get();
      backend = backends[new_primary_instance].get();
    }
  }

  // Step 4: Update the osdmap in all listeners
  for (auto& [instance, list] : listeners) {
    if (list) {
      list->osdmap = new_osdmap;
    }
  }
}

// Made with Bob
