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

#include "test/osd/ECPeeringTestFixture.h"

PeeringState* ECPeeringTestFixture::create_peering_state(int shard)
{
  const pg_pool_t& pi = get_pool();
  pg_shard_t pg_whoami(shard, shard_id_t(shard));
  PGPool pool(osdmap, pool_id, pi, "test_pool");

  shard_dpps[shard] = std::make_unique<ShardDpp>(g_ceph_context, this, shard);

  shard_peering_listeners[shard] = std::make_unique<MockPeeringListener>(
    osdmap, pool_id, shard_dpps[shard].get(), pg_whoami);
  shard_peering_listeners[shard]->current_epoch = osdmap->get_epoch();

  shard_peering_listeners[shard]->queue_transaction_callback =
    [this, shard](ObjectStore::Transaction&& t) -> int {
      return queue_transaction_helper(shard, std::move(t));
    };

  // Transfer ownership of the backend listener from the base class listeners[]
  // map into the peering listener.  The factory (set in our constructor) already
  // recorded a raw pointer in backend_listeners[] so we know which entry to move.
  // After the move, listeners[shard] holds a null unique_ptr; TearDown() already
  // guards against that with "if (list)".
  shard_peering_listeners[shard]->backend_listener = std::move(listeners[shard]);
  shard_peering_listeners[shard]->coll = colls[shard];
  shard_peering_listeners[shard]->ch = chs[shard];

  // Recreate backend with the correct backend_listener pointer.
  // The MockPeeringListener constructor created backend with the temporary
  // backend_listener it allocated internally, but we just replaced backend_listener
  // with the one from the base class listeners[] map.  We must recreate backend
  // so its parent pointer points to the new backend_listener, not the destroyed one.
  shard_peering_listeners[shard]->backend = std::make_unique<MockPGBackend>(
    g_ceph_context,
    shard_peering_listeners[shard]->backend_listener.get(),
    nullptr,
    colls[shard],
    chs[shard]);

  spg_t spgid(pgid, shard_id_t(shard));
  auto ps = std::make_unique<PeeringState>(
    g_ceph_context,
    pg_whoami,
    spgid,
    pool,
    osdmap,
    PG_FEATURE_CLASSIC_ALL,
    shard_dpps[shard].get(),
    shard_peering_listeners[shard].get());

  shard_peering_listeners[shard]->ps = ps.get();
  
  ps->set_backend_predicates(
    get_is_readable_predicate(),
    get_is_recoverable_predicate());

  shard_peering_states[shard] = std::move(ps);
  shard_peering_listeners[shard]->backend_listener->set_peering_state(shard_peering_states[shard].get());
  shard_peering_ctxs[shard] = std::make_unique<PeeringCtx>();

  return shard_peering_states[shard].get();
}

void ECPeeringTestFixture::init_peering(bool dne)
{
  pg_history_t history;
  history.same_interval_since = osdmap->get_epoch();
  history.epoch_pool_created = osdmap->get_epoch();
  history.last_epoch_clean = osdmap->get_epoch();
  if (!dne) {
    history.epoch_created = osdmap->get_epoch();
  }
  PastIntervals past_intervals;

  // Get primary from OSDMap using base class pgid member
  std::vector<int> up_osds, acting_osds;
  int up_primary = -1, acting_primary = -1;
  osdmap->pg_to_up_acting_osds(this->pgid, &up_osds, &up_primary, &acting_osds, &acting_primary);

  for (int shard : acting_osds) {
    ObjectStore::Transaction t;
    get_peering_state(shard)->init(
      (shard == acting_primary) ? 0 : 1,  // role
      up_osds,
      up_primary,
      acting_osds,
      acting_primary,
      history,
      past_intervals,
      t);

    queue_transaction_helper(shard, std::move(t));
  }
}

void ECPeeringTestFixture::update_osdmap_with_peering(
  std::shared_ptr<OSDMap> new_osdmap,
  std::optional<pg_shard_t> new_primary)
{
  OSDMapRef old_osdmap = osdmap;

  update_osdmap(new_osdmap, new_primary);

  // Update peering listeners for ALL shards (even failed ones need epoch updates)
  for (auto& [shard, listener] : shard_peering_listeners) {
    listener->current_epoch = new_osdmap->get_epoch();
  }

  // Get primary from OSDMap for advance_map calls using base class pgid member
  std::vector<int> up_osds, acting_osds;
  int up_primary = -1, acting_primary = -1;
  osdmap->pg_to_up_acting_osds(this->pgid, &up_osds, &up_primary, &acting_osds, &acting_primary);

  // Call advance_map on ALL shards that have peering states, including failed ones
  // This ensures that failed OSDs are notified of map changes (e.g., primary failover)
  // Use the newly computed up_osds and acting_osds from the new OSDMap
  for (auto& [shard, ps] : shard_peering_states) {
    ps->advance_map(
      osdmap, old_osdmap, up_osds, up_primary, acting_osds, acting_primary,
      *get_peering_ctx(shard));
  }

  // Call activate_map on ALL shards that have peering states
  // This ensures failed OSDs properly transition state and notify their backends
  for (auto& [shard, ps] : shard_peering_states) {
    ps->activate_map(*get_peering_ctx(shard));
  }

  dispatch_all();

  // Handle up_thru requirements - keep creating new epochs until peering completes.
  // Note: For primary failover scenarios, full peering may not complete immediately.
  int max_iterations = 3;
  do {
    event_advance_map();
    event_activate_map();
  } while (new_epoch(true) && --max_iterations);
}

bool ECPeeringTestFixture::new_epoch(bool if_required)
{
  bool did_work = false;
  epoch_t e = osdmap->get_epoch();
  OSDMap::Incremental pending_inc(e + 1);
  pending_inc.fsid = osdmap->get_fsid();

  // Get acting set from OSDMap
  std::vector<int> acting_osds;
  int acting_primary = -1;
  osdmap->pg_to_acting_osds(this->pgid, &acting_osds, &acting_primary);

  for (int shard : acting_osds) {
    // Skip failed OSDs (marked as CRUSH_ITEM_NONE)
    if (shard == CRUSH_ITEM_NONE) {
      continue;
    }
    if (get_peering_state(shard)->get_need_up_thru()) {
      pending_inc.new_up_thru[shard] = e;
      did_work = true;
    }
  }

  if (acting_primary >= 0) {
    auto& listener = shard_peering_listeners[acting_primary];
    if (listener->pg_temp_wanted) {
      // Get up set from OSDMap
      std::vector<int> up_osds;
      int up_primary = -1;
      osdmap->pg_to_up_acting_osds(this->pgid, &up_osds, &up_primary, nullptr, nullptr);
      
      std::vector<int> acting_temp = listener->next_acting;
      if (acting_temp.empty()) {
        acting_temp = up_osds;
      }
      
      // Apply the pg_temp change that peering requested.
      // For EC pools with optimizations, transform to primaryfirst order
      // (this simulates what the monitor does in production).
      const pg_pool_t* pool = osdmap->get_pg_pool(this->pgid.pool());
      std::vector<int> pg_temp_acting = acting_temp;
      if (pool && pool->allows_ecoptimizations()) {
        pg_temp_acting = osdmap->pgtemp_primaryfirst(*pool, acting_temp);
      }
      
      pending_inc.new_pg_temp[this->pgid] =
        mempool::osdmap::vector<int>(pg_temp_acting.begin(), pg_temp_acting.end());
      
      listener->pg_temp_wanted = false;
      did_work = true;
    }
  }

  if (!did_work && if_required) {
    return false;
  }

  osdmap->apply_incremental(pending_inc);

  for (auto& [shard, listener] : shard_peering_listeners) {
    listener->current_epoch = osdmap->get_epoch();
  }

  return true;
}

void ECPeeringTestFixture::run_peering_cycle()
{
  init_peering();
  event_initialize();
  dispatch_all();
  event_advance_map();
  dispatch_all();
  event_activate_map();
  dispatch_all();

  // Handle up_thru requirements - keep creating new epochs until peering completes.
  int max_iterations = 10;
  for (int i = 0; i < max_iterations && !all_shards_active(); i++) {
    if (new_epoch(true)) {
      event_advance_map();
      dispatch_all();
      event_activate_map();
      dispatch_all();
    }
  }
}

int ECPeeringTestFixture::queue_transaction_helper(int shard, ObjectStore::Transaction&& t)
{
  if (t.empty()) {
    return 0;
  }

  // Note: Contexts are stolen by MockPGBackendListener::queue_transaction,
  // so we don't need to call execute_finishers here
  int result = store->queue_transaction(chs[shard], std::move(t));

  return result;
}

void ECPeeringTestFixture::mark_osd_down(int osd_id)
{
  // Create new OSDMap with the OSD marked as down
  // This emulates what the real monitor does: just mark the OSD down,
  // do NOT set pg_temp. Peering will detect the change and request pg_temp.
  auto new_osdmap = std::make_shared<OSDMap>();
  new_osdmap->deepish_copy_from(*osdmap);
  OSDMapTestHelpers::mark_osd_down(new_osdmap, osd_id);
  
  update_osdmap_with_peering(new_osdmap);
  dispatch_all();
  
  // Process any pg_temp requests from peering (emulates monitor processing MOSDPGTemp)
  // This will apply the primaryfirst transformation if needed
  if (new_epoch(false)) {
    event_advance_map();
    dispatch_all();
  }
}

void ECPeeringTestFixture::mark_osd_up(int osd_id)
{
  // Create new OSDMap with the OSD marked as up using OSDMapTestHelpers
  auto new_osdmap = std::make_shared<OSDMap>();
  new_osdmap->deepish_copy_from(*osdmap);
  OSDMapTestHelpers::mark_osd_up(new_osdmap, osd_id);
  
  update_osdmap_with_peering(new_osdmap);
  dispatch_all();
}

void ECPeeringTestFixture::mark_osds_down(const std::vector<int>& osd_ids)
{
  // Create new OSDMap with all OSDs marked as down using OSDMapTestHelpers
  auto new_osdmap = std::make_shared<OSDMap>();
  new_osdmap->deepish_copy_from(*osdmap);
  OSDMapTestHelpers::mark_osds_down(new_osdmap, osd_ids);
  
  update_osdmap_with_peering(new_osdmap);
  dispatch_all();
}

void ECPeeringTestFixture::advance_epoch()
{
  auto new_osdmap = std::make_shared<OSDMap>();
  new_osdmap->deepish_copy_from(*osdmap);
  OSDMapTestHelpers::advance_epoch(new_osdmap);
  
  update_osdmap_with_peering(new_osdmap);
  dispatch_all();
}

