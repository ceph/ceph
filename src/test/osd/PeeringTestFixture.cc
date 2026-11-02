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

#include "test/osd/PeeringTestFixture.h"
#include "crush/crush.h" // for CRUSH_ITEM_NONE

std::ostream& PeeringTestFixture::ShardDpp::gen_prefix(std::ostream& out) const {
  if (test_pg && test_pg->has_peering_state()) {
    PeeringState *ps = test_pg->get_peering_state();
    out << *ps;

    // Add missing stats like PG::operator<< does (mimics production code)
    out << " m=" << ps->get_num_missing();
    if (ps->is_primary()) {
      uint64_t unfound = ps->get_num_unfound();
      out << " u=" << unfound;
    }
    if (!ps->is_clean()) {
      out << " mbc=" << ps->get_missing_by_count();
    }

    out << " ";
  }
  return out;
}


IsPGRecoverablePredicate* PeeringTestFixture::get_is_recoverable_predicate(TestPG *test_pg) {
  return test_pg->backend->get_is_recoverable_predicate();
}

IsPGReadablePredicate* PeeringTestFixture::get_is_readable_predicate(TestPG *test_pg) {
  return test_pg->backend->get_is_readable_predicate();
}

void PeeringTestFixture::SetUp() {
  // Save the initial config state so we can restore it in TearDown()
  initial_config_values_ = g_ceph_context->_conf.get_config_values();

  PGBackendTestFixture::SetUp();

  // Install a pg_upmap so that shard N is always on OSD N.
  // Skipped when use_upmap() returns false (e.g. ECCrushTestFixture), which
  // lets CRUSH determine placement naturally.
  if (use_upmap()) {
    std::vector<int> initial_acting;
    for (int i = 0; i < get_instance_count(); ++i) {
      initial_acting.push_back(i);
    }
    OSDMap::Incremental inc(osdmap->get_epoch() + 1);
    inc.fsid = osdmap->get_fsid();
    inc.new_pg_upmap[pgid] =
      mempool::osdmap::vector<int32_t>(
        initial_acting.begin(),
        initial_acting.end());
    osdmap->apply_incremental(inc);
  }

  // NOTE: TestPGs and peering states are no longer created upfront here.
  // They will be created lazily in response to OSD map publication
  // via ensure_test_pg_exists() called from event_advance_map().
  
  // Override epoch getter to use peering listeners from OsdTestFixture.
  // Use the osd parameter to look up the TestPG, since EventLoop context
  // may not always be set when this is called (e.g. from MockPeeringListener
  // lambdas scheduled with a null TestPG context).
  messenger->set_epoch_getter([this](int osd) -> epoch_t {
    // Prefer the EventLoop context TestPG if available and on the right OSD
    TestPG* test_pg = EventLoop::get_current_test_pg();
    if (test_pg && test_pg->pg_whoami.osd == osd && test_pg->has_peering_state()) {
      return test_pg->get_peering_listener()->get_osdmap_epoch();
    }
    // Fall back to OSD fixture lookup: scan all PGs on this OSD
    auto* osd_fixture = get_osd_fixture(osd);
    if (osd_fixture) {
      for (auto& [spgid, pg] : osd_fixture->pgs) {
        if (pg && pg->has_peering_state()) {
          return pg->get_peering_listener()->get_osdmap_epoch();
        }
      }
    }
    // Fallback to test fixture's osdmap
    return osdmap->get_epoch();
  });
  
  // Set TestPG getter - MockMessenger extracts spg_t and calls this to look up TestPG
  messenger->set_test_pg_getter([this](int osd, spg_t spgid) -> TestPG* {
    return get_test_pg(osd, spgid);
  });

  // Register handlers for peering messages (MOSDPeeringOp)
  // All peering messages (Query, Notify, Info, Log) use the same handler pattern
  // since they all inherit from MOSDPeeringOp and use get_event()
  auto peering_handler = [this](int from_osd, int to_osd,
                                boost::intrusive_ptr<MOSDPeeringOp> op) -> bool {
    // Message is already correctly typed as MOSDPeeringOp.  The
    // intrusive_ptr keeps the message alive across this handler;
    // releasing happens automatically when `op` goes out of scope.
    ceph_assert(op);

    // Get the peering event from the message
    PGPeeringEventRef evt_ref(op->get_event());

    // Route to the correct PG shard using the spg_t from the message.
    spg_t key = op->get_spg();
    TestPG* dest_pg = get_test_pg(to_osd, key);
    ceph_assert(dest_pg != nullptr);
    PeeringCtx* ctx = dest_pg->get_peering_ctx();
    PeeringState* ps = dest_pg->get_peering_state();
    ps->handle_event(evt_ref, ctx);

    auto t = ctx->transaction.claim_and_reset();
    if (!t.empty()) {
      int r = queue_transaction_helper(dest_pg, std::move(t));
      ceph_assert(r >= 0);
    }
    return true;
  };
  
  // Register the same handler for all peering message types
  messenger->register_typed_handler<MOSDPeeringOp>(MSG_OSD_PG_QUERY2, peering_handler);
  messenger->register_typed_handler<MOSDPeeringOp>(MSG_OSD_PG_NOTIFY2, peering_handler);
  messenger->register_typed_handler<MOSDPeeringOp>(MSG_OSD_PG_INFO2, peering_handler);
  messenger->register_typed_handler<MOSDPeeringOp>(MSG_OSD_PG_LOG, peering_handler);
  messenger->register_typed_handler<MOSDPeeringOp>(MSG_OSD_PG_LEASE, peering_handler);
  messenger->register_typed_handler<MOSDPeeringOp>(MSG_OSD_PG_LEASE_ACK, peering_handler);
  messenger->register_typed_handler<MOSDPeeringOp>(MSG_OSD_RECOVERY_RESERVE, peering_handler);
  messenger->register_typed_handler<MOSDPeeringOp>(MSG_OSD_PG_TRIM, peering_handler);
  messenger->register_typed_handler<MOSDPeeringOp>(MSG_OSD_BACKFILL_RESERVE, peering_handler);

  // Register idle callback to check for buffered messages
  event_loop->register_idle_callback([this]() -> bool {
    bool found_messages = false;
    // Check all PeeringCtx objects for buffered messages
    for_each_peering_listener([&](int osd, TestPG* test_pg, MockPeeringListener* pl) {
      PeeringCtx* ctx = test_pg->get_peering_ctx();
      if (!ctx->message_map.empty()) {
        dispatch_buffered_messages(osd, ctx);
        found_messages = true;
      }
    });
    return found_messages;
  });

  // Allow derived fixtures to modify the OSDMap before the first peering
  // cycle runs (e.g. ECCrushTestFixture installs a proper CRUSH map here).
  pre_peering_hook();

  new_epoch_loop();
}

void PeeringTestFixture::TearDown() {
  // Restore the initial config state to undo any set_config() calls made during the test
  g_ceph_context->_conf.set_config_values(initial_config_values_);

  // OSD fixtures (which contain peering states, contexts, listeners, and dpps)
  // are cleared by the base class
  PGBackendTestFixture::TearDown();
}

void PeeringTestFixture::set_config(const std::string& option, const std::string& value) {
  g_ceph_context->_conf.set_val(option, value);
  g_ceph_context->_conf.apply_changes(nullptr);
}

void PeeringTestFixture::set_stall_recovery_reservations(bool v) {
  stall_recovery_reservations = v;
  for_each_peering_listener([&](int osd, TestPG* test_pg, MockPeeringListener* pl) {
    pl->inject_event_stall = v;
  });
}

void PeeringTestFixture::set_target_pg_log_entries(unsigned n) {
  for_each_peering_listener([&](int osd, TestPG* test_pg, MockPeeringListener* pl) {
    pl->target_pg_log_entries = n;
  });
}

eversion_t PeeringTestFixture::compute_submit_trim_to() {
  if (!enable_log_trimming) {
    return eversion_t(0, 0);
  }
  int primary = get_primary_shard_from_osdmap();
  if (primary < 0 || primary == CRUSH_ITEM_NONE) {
    return eversion_t(0, 0);
  }
  auto* ps = get_primary_test_pg()->get_peering_state();
  ps->update_trim_to();  // mirrors PrimaryLogPG pre-submit
  return ps->get_pg_trim_to();
}

eversion_t PeeringTestFixture::compute_submit_pg_committed_to() {
  if (!enable_log_trimming) {
    return eversion_t(0, 0);
  }
  int primary = get_primary_shard_from_osdmap();
  if (primary < 0 || primary == CRUSH_ITEM_NONE) {
    return eversion_t(0, 0);
  }
  return get_primary_test_pg()->get_peering_state()->get_pg_committed_to();
}

void PeeringTestFixture::on_primary_write_committed(const eversion_t& at_version) {
  if (!enable_log_trimming) {
    return;
  }
  int primary = get_primary_shard_from_osdmap();
  if (primary < 0 || primary == CRUSH_ITEM_NONE) {
    return;
  }
  auto* ps = get_primary_test_pg()->get_peering_state();
  ps->complete_write(at_version, at_version);  // mirrors PrimaryLogPG::repop_all_committed
}

PeeringState* PeeringTestFixture::get_peering_state_on_osd(int osd) {
  TestPG* test_pg = nullptr;
  for (auto& [spgid, pg] : get_osd_fixture(osd)->pgs) {
    if (spgid.pgid == pgid) {
      ceph_assert(test_pg == nullptr);
      test_pg = pg.get();
    }
  }
  ceph_assert(test_pg != nullptr);
  return test_pg->get_peering_state();
}

int PeeringTestFixture::get_primary_shard_from_osdmap() const {
  std::vector<int> acting_osds;
  int acting_primary = -1;
  osdmap->pg_to_acting_osds(this->pgid, &acting_osds, &acting_primary);
  return acting_primary;
}

MockPGBackendListener* PeeringTestFixture::get_primary_listener() {
  TestPG* test_pg = get_primary_test_pg();
  if (test_pg && test_pg->has_peering_state()) {
    MockPeeringListener* peering_listener = test_pg->get_peering_listener();
    if (peering_listener && peering_listener->backend_listener) {
      // Assert that the backend listener agrees it's primary
      ceph_assert(peering_listener->backend_listener->pgb_is_primary());
      return peering_listener->backend_listener.get();
    }
  }
  return nullptr;
}

PGBackend* PeeringTestFixture::get_primary_backend() {
  TestPG* test_pg = get_primary_test_pg();
  if (test_pg && test_pg->has_peering_state()) {
    MockPeeringListener* peering_listener = test_pg->get_peering_listener();
    if (peering_listener && peering_listener->backend_listener) {
      // Assert that the backend listener agrees it's primary
      ceph_assert(peering_listener->backend_listener->pgb_is_primary());

      // Return the backend from TestPG, which is connected to the event loop and message routers
      if (test_pg->has_backend()) {
        return test_pg->get_backend();
      }
    }
  }
  return nullptr;
}

void PeeringTestFixture::advance_map_impl()
{
  std::vector<int> up_osds, acting_osds;
  int up_primary = -1, acting_primary = -1;
  
  osdmap->pg_to_up_acting_osds(pgid, &up_osds, &up_primary, &acting_osds, &acting_primary);

  int osd = event_loop->get_current_executing_osd();

  for (size_t shard_pos = 0; shard_pos < up_osds.size(); ++shard_pos) {
    if (osd != up_osds.at(shard_pos)) {
      continue;
    }
    pg_shard_t pg_shard(osd, shard_at(shard_pos));
    
    // Ensure TestPG exists for this OSD/shard combination
    ensure_test_pg_exists(pg_shard);
  }

  auto osd_fixture = get_osd_fixture(osd);

  for (auto& [spgid, test_pg] : osd_fixture->pgs)
  {
    // Advance both the parent and, once split_pg() has created it, the
    // child PG.  split_pg() advances the child itself up to the split
    // epoch, but subsequent epochs (e.g. a later OSD failure) must still
    // reach the child through this path.
    if (spgid.pgid != pgid &&
        (child_pgid == pg_t() || spgid.pgid != child_pgid)) {
      continue;
    }

    TestPG* test_pg_ptr = test_pg.get();
    event_loop->schedule_peering_event(osd, test_pg_ptr, [this, test_pg_ptr]
    {
      std::vector<int> up_osds, acting_osds;
      int up_primary = -1, acting_primary = -1;

      osdmap->pg_to_up_acting_osds(test_pg_ptr->spgid.pgid, &up_osds, &up_primary,
        &acting_osds, &acting_primary);
      PeeringState* ps = get_peering_state();
      ps->advance_map(osdmap, ps->get_osdmap(),
        up_osds, up_primary, acting_osds, acting_primary, *get_peering_ctx());
    });
  }
}

void PeeringTestFixture::event_advance_map() {
  // First, ensure OSD fixtures exist for all UP OSDs in the OSDMap
  // This must be done BEFORE scheduling peering events
  std::set<int> all_osds;
  osdmap->get_all_osds(all_osds);

  for (int osd : all_osds) {
    ensure_osd_fixture_exists(osd);
  }

  // Now schedule advance_map events for all existing OSD fixtures
  for (auto& [osd, osd_fixture] : osd_fixtures) {
    event_loop->schedule_peering_event(osd, nullptr, [this]() {
      advance_map_impl();
    });
  }
  event_loop->run_until_idle();
}

void PeeringTestFixture::event_activate_map() {
  // Schedule activate_map events for each shard instead of running directly.
  // Activate both the parent and, once split_pg() has created it, the
  // child PG; see the matching comment in advance_map_impl().
  for_each_test_pg([&](int osd, TestPG* test_pg) {
    if (test_pg->spgid.pgid != pgid &&
        (child_pgid == pg_t() || test_pg->spgid.pgid != child_pgid)) {
      return;
    }
    event_loop->schedule_peering_event(osd, test_pg, [this]() {
      get_peering_state()->activate_map(*get_peering_ctx());
    });
  });
  event_loop->run_until_idle();
}

void PeeringTestFixture::dispatch_buffered_messages(int osd, PeeringCtx* ctx) {
  ceph_assert(messenger);
  ceph_assert(ctx);
  for (auto& [target_osd, msg_list] : ctx->message_map) {
    for (auto& msg : msg_list) {
      messenger->send_message(osd, target_osd, msg.get());
    }
    msg_list.clear();
  }
  ctx->message_map.clear();
}

bool PeeringTestFixture::all_shards_active() {
  // Get acting set from OSDMap
  std::vector<int> acting_osds;
  int acting_primary = -1;
  osdmap->pg_to_acting_osds(this->pgid, &acting_osds, &acting_primary);
  
  for (size_t shard_idx = 0; shard_idx < acting_osds.size(); ++shard_idx) {
    int osd = acting_osds[shard_idx];
    // Skip failed OSDs (marked as CRUSH_ITEM_NONE)
    if (osd == CRUSH_ITEM_NONE) {
      continue;
    }
    spg_t spg(this->pgid, shard_at(shard_idx));
    TestPG* test_pg = get_test_pg(osd, spg);
    if (!test_pg || !test_pg->get_peering_state()->is_active()) {
      return false;
    }
  }
  return true;
}

bool PeeringTestFixture::primary_is_clean() {
  // Get primary from OSDMap
  std::vector<int> acting_osds;
  int acting_primary = -1;
  osdmap->pg_to_acting_osds(this->pgid, &acting_osds, &acting_primary);
  
  if (acting_primary >= 0 && acting_primary != CRUSH_ITEM_NONE) {
    return get_primary_test_pg()->get_peering_state()->is_clean();
  }
  return false;
}

void PeeringTestFixture::suspend_osd(int osd) {
  if (event_loop) {
    event_loop->suspend_to_osd(osd);
  }
}

void PeeringTestFixture::unsuspend_osd(int osd) {
  if (event_loop) {
    event_loop->unsuspend_to_osd(osd);
  }
}

bool PeeringTestFixture::is_osd_suspended(int osd) {
  return event_loop && event_loop->is_to_osd_suspended(osd);
}

void PeeringTestFixture::suspend_primary_to_osd(int to_osd) {
  if (event_loop) {
    int primary = get_primary_shard_from_osdmap();
    if (primary >= 0) {
      event_loop->suspend_from_to_osd(primary, to_osd);
    }
  }
}

void PeeringTestFixture::unsuspend_primary_to_osd(int to_osd) {
  if (event_loop) {
    int primary = get_primary_shard_from_osdmap();
    if (primary >= 0) {
      event_loop->unsuspend_from_to_osd(primary, to_osd);
    }
  }
}

void PeeringTestFixture::ensure_osd_fixture_exists(int osd) {
  if (osd_fixtures.find(osd) != osd_fixtures.end()) {
    return;
  }

  // Create OsdTestFixture for this OSD (store + LRU only).
  // Collections are per-PG and created in ensure_test_pg_exists().
  auto osd_fixture = std::make_unique<OsdTestFixture>(osd);

  // Create a new store for this OSD
  osd_fixture->store = MockStore::create(g_ceph_context, osd);
  ceph_assert(osd_fixture->store);
  osd_fixture->data_dir = osd_fixture->store->get_data_dir();

  // Create extent cache LRU for this OSD
  osd_fixture->lru = std::make_unique<ECExtentCache::LRU>(1024 * 1024 * 100);

  osd_fixtures[osd] = std::move(osd_fixture);
}


// Shared tail of ensure_test_pg_exists() and create_child_peering_state():
// test_pg->peering_listener must already be set by the caller.  This method
// constructs the PeeringState, wires pl->ps, sets backend predicates, and
// stores the state, ctx, and ctx pointer into the TestPG.
void PeeringTestFixture::create_peering_state_common(
  TestPG* test_pg,
  spg_t spgid,
  pg_shard_t pg_whoami)
{
  auto& pl = test_pg->peering_listener;

  auto ps = std::make_unique<PeeringState>(
    g_ceph_context,
    pg_whoami,
    spgid,
    pl->backend_listener->pool,
    osdmap,
    PG_FEATURE_CLASSIC_ALL,
    test_pg->dpp.get(),
    pl.get());

  pl->ps = ps.get();

  ps->set_backend_predicates(
    get_is_readable_predicate(test_pg),
    get_is_recoverable_predicate(test_pg));

  test_pg->peering_state = std::move(ps);
  pl->backend_listener->set_peering_state(test_pg->peering_state.get());
  test_pg->peering_ctx = std::make_unique<PeeringCtx>();
  // Wire pl->ctx so MockPeeringListener lambdas route deferred events to the
  // correct PG (parent vs. split child) when both coexist after split_pg().
  pl->ctx = test_pg->peering_ctx.get();
}

void PeeringTestFixture::ensure_test_pg_exists(pg_shard_t pg_whoami) {
  int osd = pg_whoami.osd;
  ceph_assert(osd_fixtures.find(osd) != osd_fixtures.end());

  auto* osd_fixture = get_osd_fixture(osd);
  ceph_assert(osd_fixture != nullptr);

  spg_t shard_spgid(pgid, pg_whoami.shard);

  if (osd_fixture->has_pg(shard_spgid)) {
    return;
  }

  // Create the TestPG and its per-PG ObjectStore collection.
  // The collection is keyed by spg_t (pgid + shard_id), so it belongs in
  // TestPG, not in OsdTestFixture.  CRUSH may place shard N on any OSD, so
  // shard_id != osd_id in general.
  TestPG* test_pg = osd_fixture->create_pg(shard_spgid, pg_whoami);
{
    coll_t shard_coll(shard_spgid);
    auto shard_ch = osd_fixture->store->create_new_collection(shard_coll);
    ObjectStore::Transaction ct;
    ct.create_collection(shard_coll, 0);
    int r = osd_fixture->store->queue_transaction(shard_ch, std::move(ct));
    ceph_assert(r == 0);
    test_pg->coll = shard_coll;
    test_pg->ch = shard_ch;
  }
  const pg_pool_t* pool_ptr = OSDMapTestHelpers::get_pool(osdmap, pool_id);
  ceph_assert(pool_ptr != nullptr);

  // Create the per-PG ShardDpp first so both the backend listener and the
  // peering listener log with the same (per-shard) prefix.
  test_pg->dpp = std::make_unique<ShardDpp>(g_ceph_context, this, test_pg);

  // Create backend listener using the TestPG's collection
  auto shard_listener = std::make_unique<MockPGBackendListener>(
    osdmap, pool_id, test_pg->dpp.get(), pg_whoami);

  shard_listener->info.pgid = shard_spgid;
  shard_listener->set_store(osd_fixture->store.get(), test_pg->ch);
  shard_listener->set_event_loop(event_loop.get());
  shard_listener->set_messenger(messenger.get());

  test_pg->backend = create_backend(test_pg, shard_listener.get());
  test_pg->backend_listener = std::move(shard_listener);

  // Construct MockPeeringListener, transferring ownership of the backend
  // listener from TestPG. The backend listener was created just above, in
  // this function.
  auto peering_listener = std::make_unique<MockPeeringListener>(
    osdmap, pool_id, test_pg->dpp.get(), pg_whoami,
    std::move(test_pg->backend_listener),
    osd_fixture->store.get(), test_pg->coll, test_pg->ch);

  peering_listener->current_epoch = osdmap->get_epoch();
  peering_listener->set_messenger(messenger.get());
  peering_listener->set_event_loop(event_loop.get());
  peering_listener->backend_listener->set_messenger(messenger.get());
  peering_listener->pg_backend = test_pg->backend.get();

  peering_listener->queue_transaction_callback =
    [this, test_pg](ObjectStore::Transaction&& t) -> int {
      return queue_transaction_helper(test_pg, std::move(t));
    };

  // Wire the peering listener and construct PeeringState (common with
  // create_child_peering_state).
  test_pg->peering_listener = std::move(peering_listener);
  create_peering_state_common(test_pg, shard_spgid, pg_whoami);

  init_peering(test_pg);

  // Schedule Initialize event for this newly created TestPG
  auto evt = std::make_shared<PGPeeringEvent>(
    osdmap->get_epoch(),
    osdmap->get_epoch(),
    PeeringState::Initialize());

  event_loop->schedule_peering_event(osd, test_pg, [test_pg, evt]() {
    test_pg->get_peering_state()->handle_event(evt, test_pg->get_peering_ctx());
  });

}

void PeeringTestFixture::init_peering(TestPG *test_pg)
{
  pg_history_t history;
  history.same_interval_since = osdmap->get_epoch();
  history.epoch_pool_created = osdmap->get_epoch();
  history.last_epoch_clean = osdmap->get_epoch();
  history.epoch_created = osdmap->get_epoch();
  PastIntervals past_intervals;

  // Get primary from OSDMap using base class pgid member
  std::vector<int> up_osds, acting_osds;
  int up_primary = -1, acting_primary = -1;
  osdmap->pg_to_up_acting_osds(this->pgid, &up_osds, &up_primary, &acting_osds, &acting_primary);
  ObjectStore::Transaction t;
  test_pg->get_peering_state()->init(
    (test_pg->pg_whoami.osd == acting_primary) ? 0 : 1,  // role
    up_osds,
    up_primary,
    acting_osds,
    acting_primary,
    history,
    past_intervals,
    t);

  queue_transaction_helper(test_pg, std::move(t));
}

void PeeringTestFixture::update_osdmap_with_peering(
  std::shared_ptr<OSDMap> new_osdmap)
{
  OSDMapRef old_osdmap = osdmap;
  osdmap = new_osdmap;

  for_each_test_pg([&](int osd, TestPG* test_pg) {
    if (test_pg->has_peering_state()) {
      test_pg->get_peering_listener()->current_epoch = osdmap->get_epoch();
    }
    // Mirror PGBackendTestFixture::update_osdmap(): keep each backend
    // listener's osdmap and pool snapshot current. Without this,
    // pgb_get_osdmap()/pgb_get_osdmap_epoch() and get_pool() would keep
    // returning the SetUp-time map/pool forever on the peering path.
    if (test_pg->has_backend()) {
      MockPGBackendListener* bl = test_pg->get_backend_listener();
      if (bl) {
        bl->osdmap = new_osdmap;
        bl->pool.update(new_osdmap);
      }
    }
  });

  new_epoch_loop();
}

void PeeringTestFixture::new_epoch_loop() {
  int max = 10;
  do {
    ceph_assert(--max);
    event_advance_map();
    event_activate_map();
  } while (new_epoch(true));
}

bool PeeringTestFixture::new_epoch(bool if_required)
{
  return apply_new_epoch(this->pgid, if_required);
}

bool PeeringTestFixture::apply_new_epoch(pg_t which_pg, bool if_required)
{
  bool did_work = false;
  epoch_t e = osdmap->get_epoch();
  OSDMap::Incremental pending_inc(e + 1);
  pending_inc.fsid = osdmap->get_fsid();

  std::vector<int> acting_osds;
  int acting_primary = -1;
  osdmap->pg_to_acting_osds(which_pg, &acting_osds, &acting_primary);

  int s = 0;
  for (int osd : acting_osds) {
    shard_id_t shard = shard_at(s++);
    // Skip failed OSDs (marked as CRUSH_ITEM_NONE)
    if (osd == CRUSH_ITEM_NONE) {
      continue;
    }
    spg_t spg(which_pg, shard);
    // TestPG may not exist yet for OSDs that were just added to the acting
    // set but haven't been initialized by event_advance_map() yet.
    TestPG* test_pg = get_test_pg(osd, spg);
    if (!test_pg || !test_pg->has_peering_state()) {
      continue;
    }
    if (test_pg->get_peering_state()->get_need_up_thru()) {
      pending_inc.new_up_thru[osd] = e;
      did_work = true;
    }
  }

  if (acting_primary >= 0 && acting_primary != CRUSH_ITEM_NONE) {
    TestPG* test_pg = get_primary_test_pg(which_pg);
    if (test_pg && test_pg->has_peering_state()) {
      MockPeeringListener* listener = test_pg->get_peering_listener();
      if (listener->pg_temp_wanted) {
        std::vector<int> up_osds;
        int up_primary = -1;
        osdmap->pg_to_up_acting_osds(which_pg, &up_osds, &up_primary, nullptr, nullptr);

        std::vector<int> acting_temp = listener->next_acting;
        if (acting_temp.empty()) {
          acting_temp = up_osds;
        }

        // For EC pools with optimizations, transform to primaryfirst order before
        // storing in pg_temp.  This matches what the real monitor does and what
        // _get_temp_osds() expects when it calls pgtemp_undo_primaryfirst().
        const pg_pool_t* pool = osdmap->get_pg_pool(which_pg.pool());
        if (pool && pool->allows_ecoptimizations()) {
          acting_temp = osdmap->pgtemp_primaryfirst(*pool, acting_temp);
        }

        pending_inc.new_pg_temp[which_pg] =
        mempool::osdmap::vector<int32_t>(acting_temp.begin(), acting_temp.end());

        listener->pg_temp_wanted = false;
        did_work = true;
      }
    }
  }

  if (!did_work && if_required) {
    return false;
  }

  osdmap->apply_incremental(pending_inc);

  for_each_peering_listener([&](int osd, TestPG* test_pg, MockPeeringListener* pl) {
    pl->current_epoch = osdmap->get_epoch();
  });

  return true;
}

int PeeringTestFixture::queue_transaction_helper(TestPG* test_pg, ObjectStore::Transaction&& t)
{
  if (t.empty()) {
    return 0;
  }

  ceph_assert(test_pg != nullptr && test_pg->ch);
  OsdTestFixture* osd_fixture = get_osd_fixture(test_pg->pg_whoami.osd);
  ceph_assert(osd_fixture != nullptr && osd_fixture->store);

  // The collection is in TestPG, not OsdTestFixture.  The ch passed to
  // queue_transaction is used only as a sequencer key in MemStore; the actual
  // collection for each op comes from the transaction data itself.  Use this
  // TestPG's own ch, not some other PG's, so that after a split each child
  // is sequenced against its own collection rather than a sibling's.
  return osd_fixture->store->queue_transaction(test_pg->ch, std::move(t));
}

void PeeringTestFixture::mark_osd_down(int osd_id)
{
  // Create new OSDMap with the OSD marked as down
  // This emulates what the real monitor does: just mark the OSD down,
  // do NOT set pg_temp. Peering will detect the change and request pg_temp.
  auto new_osdmap = std::make_shared<OSDMap>();
  new_osdmap->deepish_copy_from(*osdmap);
  OSDMapTestHelpers::mark_osd_down(new_osdmap, osd_id);
  
  update_osdmap_with_peering(new_osdmap);
}

void PeeringTestFixture::mark_osd_up(int osd_id)
{
  // Create new OSDMap with the OSD marked as up using OSDMapTestHelpers
  auto new_osdmap = std::make_shared<OSDMap>();
  new_osdmap->deepish_copy_from(*osdmap);
  OSDMapTestHelpers::mark_osd_up(new_osdmap, osd_id);
  
  update_osdmap_with_peering(new_osdmap);
}

void PeeringTestFixture::mark_osds_down(const std::vector<int>& osd_ids)
{
  // Create new OSDMap with all OSDs marked as down using OSDMapTestHelpers
  auto new_osdmap = std::make_shared<OSDMap>();
  new_osdmap->deepish_copy_from(*osdmap);
  OSDMapTestHelpers::mark_osds_down(new_osdmap, osd_ids);
  
  update_osdmap_with_peering(new_osdmap);
}

void PeeringTestFixture::set_pool_min_size(unsigned new_min_size)
{
  auto new_osdmap = std::make_shared<OSDMap>();
  new_osdmap->deepish_copy_from(*osdmap);
  OSDMapTestHelpers::set_pool_min_size(new_osdmap, pool_id, new_min_size);

  update_osdmap_with_peering(new_osdmap);
}

void PeeringTestFixture::set_pg_upmap(const std::vector<int>& osds)
{
  auto new_osdmap = std::make_shared<OSDMap>();
  new_osdmap->deepish_copy_from(*osdmap);
  OSDMap::Incremental inc(new_osdmap->get_epoch() + 1);
  inc.fsid = new_osdmap->get_fsid();
  inc.new_pg_upmap[pgid] =
    mempool::osdmap::vector<int32_t>(osds.begin(), osds.end());
  new_osdmap->apply_incremental(inc);

  update_osdmap_with_peering(new_osdmap);
}

void PeeringTestFixture::advance_epoch()
{
  auto new_osdmap = std::make_shared<OSDMap>();
  new_osdmap->deepish_copy_from(*osdmap);
  OSDMapTestHelpers::advance_epoch(new_osdmap);
  
  update_osdmap_with_peering(new_osdmap);
}

// Helper to check recovery completion and queue appropriate events
// This mimics what PrimaryLogPG::start_recovery_ops() does when recovery completes
void PeeringTestFixture::check_recovery_completion_impl(int osd_id)
{
  // Find the parent PG (pgid, not child_pgid) for this OSD.
  // After a split an OSD hosts both the parent and child PGs; we must look up
  // the parent by iterating the acting set to find this OSD's shard position.
  TestPG* test_pg = nullptr;
  {
    std::vector<int> acting_osds;
    int acting_primary = -1;
    osdmap->pg_to_acting_osds(this->pgid, &acting_osds, &acting_primary);
    for (size_t shard_idx = 0; shard_idx < acting_osds.size(); ++shard_idx) {
      if (acting_osds[shard_idx] == osd_id) {
        spg_t spg(this->pgid, shard_at(shard_idx));
        test_pg = get_test_pg(osd_id, spg);
        break;
      }
    }
  }
  if (!test_pg) {
    return;
  }

  auto ps = test_pg->get_peering_state();
  if (!ps) {
    return;
  }

  // Check if we're in recovering state and recovery is complete
  if (ps->state_test(PG_STATE_RECOVERING) && !ps->needs_recovery()) {
    std::cout << "  OSD " << osd_id << ": Recovery complete, queuing completion event..." << std::endl;

    // Clear recovering state (mimics PrimaryLogPG.cc:13619)
    ps->state_clear(PG_STATE_RECOVERING);
    ps->state_clear(PG_STATE_FORCED_RECOVERY);

    // Check if backfill is needed
    if (ps->needs_backfill()) {
      std::cout << "  OSD " << osd_id << ": Queuing RequestBackfill event" << std::endl;
      PGPeeringEventRef evt = std::make_shared<PGPeeringEvent>(
        ps->get_osdmap_epoch(),
        ps->get_osdmap_epoch(),
        PeeringState::RequestBackfill());
      event_loop->schedule_peering_event(osd_id, test_pg, [evt, test_pg]() {
        test_pg->get_peering_state()->handle_event(evt, test_pg->get_peering_ctx());
      });
    } else {
      std::cout << "  OSD " << osd_id << ": Queuing AllReplicasRecovered event" << std::endl;
      ps->state_clear(PG_STATE_FORCED_BACKFILL);
      PGPeeringEventRef evt = std::make_shared<PGPeeringEvent>(
        ps->get_osdmap_epoch(),
        ps->get_osdmap_epoch(),
        PeeringState::AllReplicasRecovered());
      event_loop->schedule_peering_event(osd_id, test_pg, [evt, test_pg]() {
        test_pg->get_peering_state()->handle_event(evt, test_pg->get_peering_ctx());
      });
    }
  }
}

void PeeringTestFixture::run_on_primary(EventLoop::GenericEvent fn)
{
  // schedule_transaction() captures the current EventLoop TestPG context at
  // call time; called from plain test scope that context is whatever a
  // previous event happened to leave behind, so establish it explicitly.
  TestPG* primary_test_pg = get_primary_test_pg();
  ceph_assert(primary_test_pg != nullptr);
  int primary = primary_test_pg->pg_whoami.osd;
  event_loop->run_in_pg(primary, primary_test_pg, [&] {
    event_loop->schedule_transaction(primary, std::move(fn));
  });
  event_loop->run_until_idle();
}

void PeeringTestFixture::check_recovery_completion()
{
  // After recovery completes, check each OSD to see if recovery is done
  // and queue appropriate completion events (AllReplicasRecovered, RequestBackfill, etc.)
  // This mimics what PrimaryLogPG::start_recovery_ops() does when it detects completion
  std::vector<int> acting_osds;
  int acting_primary = -1;
  osdmap->pg_to_acting_osds(this->pgid, &acting_osds, &acting_primary);

  for (int osd : acting_osds) {
    if (osd == CRUSH_ITEM_NONE) {
      continue;
    }
    spg_t spgid;
    TestPG* test_pg = get_spg_for_osd(osd, &spgid) ? get_test_pg(osd, spgid) : nullptr;
    if (!test_pg) {
      continue;
    }
    event_loop->run_in_pg(osd, test_pg, [&] {
      event_loop->schedule_transaction(osd, [this, osd]() {
        check_recovery_completion_impl(osd);
      });
    });
  }
  event_loop->run_until_idle();
}
