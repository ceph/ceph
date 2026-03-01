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

void PGBackendTestFixture::setup_ec_pool()
{
  CephContext *cct = g_ceph_context;

  osdmap = std::make_shared<OSDMap>();
  osdmap->set_max_osd(k + m);

  for (int i = 0; i < k + m; i++) {
    osdmap->set_state(i, CEPH_OSD_EXISTS);
    osdmap->set_weight(i, CEPH_OSD_OUT);
    osdmap->crush->set_item_name(i, "osd." + std::to_string(i));
  }

  // Use incremental to set OSDs as up and with proper features
  OSDMap::Incremental inc(osdmap->get_epoch() + 1);
  inc.fsid = osdmap->get_fsid();

  for (int i = 0; i < k + m; i++) {
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

  pg_pool_t pool = OSDMapTestHelpers::create_ec_pool(k, m, stripe_unit * k, pool_flags, pool_id);
  OSDMapTestHelpers::add_pool(osdmap, pool_id, pool);

  pgid = pg_t(0, pool_id);
  spgid = spg_t(pgid, shard_id_t(0));

  OSDMapTestHelpers::setup_ec_pg(osdmap, pgid, k, m, 0);

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
  for (int i = 0; i < k + m; i++) {
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

  for (int i = 0; i < k + m; i++) {
    std::unique_ptr<MockPGBackendListener> shard_listener;
    if (listener_factory) {
      shard_listener = listener_factory(
        i,
        osdmap,
        pool_id,
        dpp.get(),
        pg_shard_t(i, shard_id_t(i)));
    } else {
      shard_listener = std::make_unique<MockPGBackendListener>(
        osdmap,
        pool_id,
        dpp.get(),
        pg_shard_t(i, shard_id_t(i))
      );
    }

    // Initialize the listener's own info.pgid so OSDMap queries work
    shard_listener->info.pgid = spg_t(pgid, shard_id_t(i));

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

    shard_listener->set_store(store.get(), chs[i]);
    shard_listener->set_event_loop(event_loop.get());
    shard_listener->set_op_tracker(op_tracker.get());

    auto shard_lru = std::make_unique<ECExtentCache::LRU>(1024 * 1024 * 100);
    auto shard_ec_switch = std::make_unique<ECSwitch>(
      shard_listener.get(), colls[i], chs[i], store.get(),
      cct, ec_impl, stripe_unit * k, *shard_lru);

    listeners[i] = std::move(shard_listener);
    lrus[i] = std::move(shard_lru);
    backends[i] = std::move(shard_ec_switch);
  }

  for (int i = 0; i < k + m; i++) {
    message_router[i] = [this, i](OpRequestRef op) -> bool {
      return backends[i]->_handle_message(op);
    };
  }

  for (int i = 0; i < k + m; i++) {
    listeners[i]->set_message_router(&message_router);
    listeners[i]->set_handle_message_callback(
      [this, i](OpRequestRef op) -> bool {
        return backends[i]->_handle_message(op);
      });
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
    std::unique_ptr<MockPGBackendListener> replica_listener;
    if (listener_factory) {
      replica_listener = listener_factory(
        i,
        osdmap,
        pool_id,
        dpp.get(),
        pg_shard_t(i, shard_id_t::NO_SHARD));
    } else {
      replica_listener = std::make_unique<MockPGBackendListener>(
        osdmap,
        pool_id,
        dpp.get(),
        pg_shard_t(i, shard_id_t::NO_SHARD)
      );
    }

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
    replica_listener->set_op_tracker(op_tracker.get());

    auto replica_backend = std::make_unique<ReplicatedBackend>(
      replica_listener.get(), colls[i], chs[i], store.get(), cct);

    listeners[i] = std::move(replica_listener);
    backends[i] = std::move(replica_backend);
  }

  for (int i = 0; i < num_replicas; i++) {
    message_router[i] = [this, i](OpRequestRef op) -> bool {
      return backends[i]->_handle_message(op);
    };
  }

  for (int i = 0; i < num_replicas; i++) {
    listeners[i]->set_message_router(&message_router);
    listeners[i]->set_handle_message_callback(
      [this, i](OpRequestRef op) -> bool {
        return backends[i]->_handle_message(op);
      });
  }
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

  event_loop->run_until_idle(10000);

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
  hobject_t hoid = make_test_object(obj_name);
  PGTransactionUPtr pg_t = std::make_unique<PGTransaction>();
  pg_t->create(hoid);

  ObjectContextRef obc = make_object_context(hoid, false, 0);
  pg_t->obc_map[hoid] = obc;

  bufferlist bl;
  bl.append(data);
  pg_t->write(hoid, 0, bl.length(), bl);

  object_stat_sum_t delta_stats;
  delta_stats.num_objects = 1;
  delta_stats.num_bytes = bl.length();

  std::vector<pg_log_entry_t> log_entries;
  pg_log_entry_t entry;
  entry.mark_unrollbackable();
  entry.op = pg_log_entry_t::MODIFY;
  entry.soid = hoid;
  entry.version = at_version;
  entry.prior_version = eversion_t(0, 0);
  log_entries.push_back(entry);

  int result = do_transaction_and_complete(
    hoid, std::move(pg_t), delta_stats, at_version, std::move(log_entries));

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
  hobject_t hoid = make_test_object(obj_name);
  PGTransactionUPtr pg_t = std::make_unique<PGTransaction>();

  ObjectContextRef obc = make_object_context(hoid, true, object_size);
  obc->obs.oi.version = prior_version;
  pg_t->obc_map[hoid] = obc;

  bufferlist bl;
  bl.append(data);
  pg_t->write(hoid, offset, bl.length(), bl);

  object_stat_sum_t delta_stats;
  uint64_t new_size = std::max(object_size, offset + bl.length());
  if (new_size > object_size) {
    delta_stats.num_bytes = new_size - object_size;
  } else {
    delta_stats.num_bytes = 0;
  }

  std::vector<pg_log_entry_t> log_entries;
  pg_log_entry_t entry;
  // Don't mark as unrollbackable - partial writes need rollback support
  entry.op = pg_log_entry_t::MODIFY;
  entry.soid = hoid;
  entry.version = at_version;
  entry.prior_version = prior_version;
  log_entries.push_back(entry);

  int result = do_transaction_and_complete(
    hoid, std::move(pg_t), delta_stats, at_version, std::move(log_entries));

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

    event_loop->run_until_idle(10000);

    if (!completed) {
      throw std::runtime_error("Read operation did not complete within timeout");
    }

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
  // Step 1: Call on_change() on all backends to clear in-flight operations
  for (auto& [instance, be] : backends) {
    if (be) {
      be->on_change();
    }
  }

  // Step 2: Update the osdmap reference
  osdmap = new_osdmap;

  // Step 3: Update the osdmap in all listeners
  for (auto& [instance, list] : listeners) {
    if (list) {
      list->osdmap = new_osdmap;
    }
  }
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

