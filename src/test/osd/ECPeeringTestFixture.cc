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

#include "test/osd/ECPeeringTestFixture.h"

// ============================================================================
// ECPeeringTestFixture - non-template method implementations
// ============================================================================

PeeringState* ECPeeringTestFixture::create_peering_state(int shard)
{
  const pg_pool_t& pi = get_pool();
  pg_shard_t pg_whoami(shard, shard_id_t(shard));
  PGPool pool(osdmap, pool_id, pi, "test_pool");

  // Create DPP for this shard
  shard_dpps[shard] = std::make_unique<ShardDpp>(g_ceph_context, this, shard);

  // Create peering listener for this shard
  shard_peering_listeners[shard] = std::make_unique<MockPeeringListener>(
    osdmap, pool_id, shard_dpps[shard].get(), pg_whoami);
  shard_peering_listeners[shard]->current_epoch = osdmap->get_epoch();

  // Set up transaction queuing callback
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

  // Create PeeringState
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

  // Set backend predicates
  ps->set_backend_predicates(
    get_is_readable_predicate(),
    get_is_recoverable_predicate());

  shard_peering_states[shard] = std::move(ps);
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

  for (int shard : up_acting) {
    ObjectStore::Transaction t;
    get_peering_state(shard)->init(
      (shard == acting_primary) ? 0 : 1,  // role
      up,
      up_primary,
      acting,
      acting_primary,
      history,
      past_intervals,
      t);

    // Queue the transaction to persist PG metadata
    queue_transaction_helper(shard, std::move(t));
  }
}

void ECPeeringTestFixture::update_osdmap_with_peering(
  std::shared_ptr<OSDMap> new_osdmap,
  std::optional<pg_shard_t> new_primary)
{
  // Save the old osdmap before updating
  OSDMapRef old_osdmap = osdmap;

  // First update the base EC infrastructure
  // This updates osdmap, up, acting, up_primary, acting_primary, up_acting
  update_osdmap(new_osdmap, new_primary);

  // Update peering listeners for ALL shards (even failed ones need epoch updates)
  for (auto& [shard, listener] : shard_peering_listeners) {
    listener->current_epoch = new_osdmap->get_epoch();
    // Update the primary in the backend listener if specified
    if (new_primary.has_value()) {
      listener->backend_listener->primary = new_primary.value();
    }
  }

  // Send AdvanceMap ONLY to shards that are still in the acting set
  // Failed shards should not receive events
  for (int shard : up_acting) {
    // Skip failed OSDs (marked as CRUSH_ITEM_NONE)
    if (shard == CRUSH_ITEM_NONE) {
      continue;
    }
    // Only send to shards that have PeeringState
    if (shard_peering_states.count(shard)) {
      get_peering_state(shard)->advance_map(
        osdmap, old_osdmap, up, up_primary, acting, acting_primary,
        *get_peering_ctx(shard));
    }
  }

  // Send ActivateMap to complete the peering cycle
  for (int shard : up_acting) {
    // Skip failed OSDs (marked as CRUSH_ITEM_NONE)
    if (shard == CRUSH_ITEM_NONE) {
      continue;
    }
    if (shard_peering_states.count(shard)) {
      get_peering_state(shard)->activate_map(*get_peering_ctx(shard));
    }
  }

  // Dispatch all peering events to allow GetInfo/GetLog exchanges
  dispatch_all();

  // Handle up_thru requirements - keep creating new epochs until peering completes
  // This simulates the monitor acknowledging OSDs are up
  // Note: For primary failover scenarios, full peering may not complete immediately
  int max_iterations = 3;  // Limit iterations to prevent hangs
  for (int i = 0; i < max_iterations; i++) {
    // Check if all shards are active
    if (all_shards_active()) {
      break;
    }

    // Dispatch any pending messages
    dispatch_all();

    if (new_epoch(true)) {  // Only create epoch if needed
      // Save old map again for the new advance_map call
      old_osdmap = osdmap;

      // Send AdvanceMap to notify all shards of the new epoch
      for (int shard : up_acting) {
        // Skip failed OSDs (marked as CRUSH_ITEM_NONE)
        if (shard == CRUSH_ITEM_NONE) {
          continue;
        }
        if (shard_peering_states.count(shard)) {
          get_peering_state(shard)->advance_map(
            osdmap, old_osdmap, up, up_primary, acting, acting_primary,
            *get_peering_ctx(shard));
        }
      }
      dispatch_all();
    }
  }
}

bool ECPeeringTestFixture::new_epoch(bool if_required)
{
  bool did_work = false;
  epoch_t e = osdmap->get_epoch();
  OSDMap::Incremental pending_inc(e + 1);
  pending_inc.fsid = osdmap->get_fsid();

  // Check for up_thru updates
  for (int shard : up_acting) {
    // Skip failed OSDs (marked as CRUSH_ITEM_NONE)
    if (shard == CRUSH_ITEM_NONE) {
      continue;
    }
    if (get_peering_state(shard)->get_need_up_thru()) {
      pending_inc.new_up_thru[shard] = e;
      did_work = true;
    }
  }

  // Check for pg_temp updates
  if (acting_primary >= 0) {
    auto& listener = shard_peering_listeners[acting_primary];
    if (listener->pg_temp_wanted) {
      acting = listener->next_acting;
      if (acting.empty()) {
        acting = up;
      }
      listener->pg_temp_wanted = false;
      did_work = true;
    }
  }

  if (!did_work && if_required) {
    return false;
  }

  osdmap->apply_incremental(pending_inc);

  // Update all listeners
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

  // Handle up_thru requirements - keep creating new epochs until peering completes
  // This simulates the monitor acknowledging OSDs are up
  int max_iterations = 10;  // Prevent infinite loops
  for (int i = 0; i < max_iterations && !all_shards_active(); i++) {
    if (new_epoch(true)) {  // Only create epoch if needed
      // Send AdvanceMap to notify all shards of the new epoch
      event_advance_map();
      dispatch_all();
    }
  }
}

void ECPeeringTestFixture::pre_transaction_hook(
  const hobject_t& /* hoid */,
  const std::vector<pg_log_entry_t>& log_entries,
  const eversion_t& /* at_version */)
{
  // Add the log entries to the primary's PeeringState PG log BEFORE executing
  // the transaction.  Only do this if the osdmap hasn't changed (to avoid
  // assertion failures in append_log).
  if (!log_entries.empty() && acting_primary >= 0) {
    auto* primary_ps = get_peering_state(acting_primary);
    if (primary_ps && primary_ps->get_osdmap()->get_epoch() == osdmap->get_epoch()) {
      // Create a transaction for the log entries
      ObjectStore::Transaction log_t;

      // Add entries to the PG log using append_log
      primary_ps->append_log(
        std::vector<pg_log_entry_t>(log_entries),  // Copy the entries
        eversion_t(),  // trim_to
        eversion_t(),  // roll_forward_to
        eversion_t(),  // pg_committed_to
        log_t,         // transaction
        false,         // transaction_applied
        false          // async
      );

      // Queue the transaction to persist the log entries
      if (!log_t.empty()) {
        queue_transaction_helper(acting_primary, std::move(log_t));
      }
    }
  }
}

int ECPeeringTestFixture::queue_transaction_helper(int shard, ObjectStore::Transaction&& t)
{
  // Skip empty transactions
  if (t.empty()) {
    return 0;
  }

  // Queue to the shard's collection
  // Note: Contexts are stolen by MockPGBackendListener::queue_transaction,
  // so we don't need to call execute_finishers here
  int result = store->queue_transaction(chs[shard], std::move(t));

  return result;
}
