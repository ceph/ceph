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
#include "test/osd/MockECRecPred.h"
#include "test/osd/MockECReadPred.h"
#include "crush/crush.h" // for CRUSH_ITEM_NONE

std::ostream& ECPeeringTestFixture::ShardDpp::gen_prefix(std::ostream& out) const {
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

IsPGRecoverablePredicate* ECPeeringTestFixture::get_is_recoverable_predicate() {
  return new MockECRecPred(k, m);
}

IsPGReadablePredicate* ECPeeringTestFixture::get_is_readable_predicate() {
  return new MockECReadPred(k, m);
}

ECPeeringTestFixture::ECPeeringTestFixture()
  : PGBackendTestFixture(PGBackendTestFixture::EC) {
}

void ECPeeringTestFixture::SetUp() {
  PGBackendTestFixture::SetUp();

  // The harness does not use CRUSH, so we must set an upmap.  Choose the upmap
  // to have shard == osd.
  {
    std::vector<int> initial_acting;
    for (int i = 0; i < k + m; ++i) {
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

  new_epoch_loop();
}

void ECPeeringTestFixture::TearDown() {
  // OSD fixtures (which contain peering states, contexts, listeners, and dpps)
  // are cleared by the base class
  PGBackendTestFixture::TearDown();
}

void ECPeeringTestFixture::set_config(const std::string& option, const std::string& value) {
  g_ceph_context->_conf.set_val(option, value);
  g_ceph_context->_conf.apply_changes(nullptr);
}

void ECPeeringTestFixture::set_stall_recovery_reservations(bool v) {
  stall_recovery_reservations = v;
  for_each_peering_listener([&](int osd, TestPG* test_pg, MockPeeringListener* pl) {
    pl->inject_event_stall = v;
  });
}

void ECPeeringTestFixture::set_target_pg_log_entries(unsigned n) {
  for_each_peering_listener([&](int osd, TestPG* test_pg, MockPeeringListener* pl) {
    pl->target_pg_log_entries = n;
  });
}

eversion_t ECPeeringTestFixture::compute_submit_trim_to() {
  if (!enable_log_trimming) {
    return eversion_t(0, 0);
  }
  int primary = get_primary_shard_from_osdmap();
  if (primary < 0 || primary == CRUSH_ITEM_NONE) {
    return eversion_t(0, 0);
  }
  auto* ps = get_peering_state(primary);
  ps->update_trim_to();  // mirrors PrimaryLogPG pre-submit
  return ps->get_pg_trim_to();
}

eversion_t ECPeeringTestFixture::compute_submit_pg_committed_to() {
  if (!enable_log_trimming) {
    return eversion_t(0, 0);
  }
  int primary = get_primary_shard_from_osdmap();
  if (primary < 0 || primary == CRUSH_ITEM_NONE) {
    return eversion_t(0, 0);
  }
  return get_peering_state(primary)->get_pg_committed_to();
}

void ECPeeringTestFixture::on_primary_write_committed(const eversion_t& at_version) {
  if (!enable_log_trimming) {
    return;
  }
  int primary = get_primary_shard_from_osdmap();
  if (primary < 0 || primary == CRUSH_ITEM_NONE) {
    return;
  }
  auto* ps = get_peering_state(primary);
  ps->complete_write(at_version, at_version);  // mirrors PrimaryLogPG::repop_all_committed
}

// Find the TestPG for a given shard by spg_t(pgid, shard_id_t(shard)).
// This handles the case where the OSD is down (not in acting set):
// we search all OSD fixtures for a TestPG that owns the given shard.
TestPG* ECPeeringTestFixture::find_test_pg_for_shard(int shard) {
  // First try the fast path: look up via acting set.
  TestPG* test_pg = get_test_pg_by_shard(shard);
  if (test_pg) {
    return test_pg;
  }
  // Slow path: shard's OSD may be down (CRUSH_ITEM_NONE).  Search all
  // OSD fixtures for the spg_t that matches (pgid, shard_id_t(shard)).
  spg_t spg(pgid, shard_id_t(shard));
  for (auto& [osd, osd_fixture] : osd_fixtures) {
    if (osd_fixture->has_pg(spg)) {
      return osd_fixture->get_pg(spg);
    }
  }
  return nullptr;
}

PeeringState* ECPeeringTestFixture::get_peering_state(int shard) {
  TestPG* test_pg = find_test_pg_for_shard(shard);
  ceph_assert(test_pg != nullptr);
  return test_pg->get_peering_state();
}

PeeringCtx* ECPeeringTestFixture::get_peering_ctx(int shard) {
  TestPG* test_pg = find_test_pg_for_shard(shard);
  ceph_assert(test_pg != nullptr);
  return test_pg->get_peering_ctx();
}

MockPeeringListener* ECPeeringTestFixture::get_peering_listener(int shard) {
  TestPG* test_pg = find_test_pg_for_shard(shard);
  ceph_assert(test_pg != nullptr);
  return test_pg->get_peering_listener();
}

int ECPeeringTestFixture::get_primary_shard_from_osdmap() const {
  std::vector<int> acting_osds;
  int acting_primary = -1;
  osdmap->pg_to_acting_osds(this->pgid, &acting_osds, &acting_primary);
  return acting_primary;
}

MockPGBackendListener* ECPeeringTestFixture::get_primary_listener() {
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

PGBackend* ECPeeringTestFixture::get_primary_backend() {
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

void ECPeeringTestFixture::advance_map_impl()
{
  std::vector<int> up_osds, acting_osds;
  int up_primary = -1, acting_primary = -1;
  
  osdmap->pg_to_up_acting_osds(pgid, &up_osds, &up_primary, &acting_osds, &acting_primary);

  int osd = event_loop->get_current_executing_osd();

  for (size_t shard_pos = 0; shard_pos < up_osds.size(); ++shard_pos) {
    if (osd != up_osds.at(shard_pos)) {
      continue;
    }
    pg_shard_t pg_shard(osd, shard_id_t((int)shard_pos));
    
    // Ensure TestPG exists for this OSD/shard combination
    ensure_test_pg_exists(pg_shard);
  }

  auto osd_fixture = get_osd_fixture(osd);

  for (auto& [spgid, test_pg] : osd_fixture->pgs)
  {
    // The child PG (post split_pg()) is advanced separately by split_pg()
    // itself; advancing it again here would double-apply the epoch.
    if (spgid.pgid != pgid) {
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

void ECPeeringTestFixture::event_advance_map() {
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

void ECPeeringTestFixture::event_activate_map() {
  // Schedule activate_map events for each shard instead of running directly.
  // The child PG (post split_pg()) is activated separately by split_pg()
  // itself; activating it again here would double-apply the map.
  for_each_test_pg([&](int osd, TestPG* test_pg) {
    if (test_pg->spgid.pgid != pgid) {
      return;
    }
    event_loop->schedule_peering_event(osd, test_pg, [this]() {
      get_peering_state()->activate_map(*get_peering_ctx());
    });
  });
  event_loop->run_until_idle();
}

void ECPeeringTestFixture::dispatch_buffered_messages(int osd, PeeringCtx* ctx) {
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

bool ECPeeringTestFixture::all_shards_active() {
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
    spg_t spg(this->pgid, shard_id_t(shard_idx));
    TestPG* test_pg = get_test_pg(osd, spg);
    if (!test_pg || !test_pg->get_peering_state()->is_active()) {
      return false;
    }
  }
  return true;
}

bool ECPeeringTestFixture::all_shards_clean() {
  // Get primary from OSDMap
  std::vector<int> acting_osds;
  int acting_primary = -1;
  osdmap->pg_to_acting_osds(this->pgid, &acting_osds, &acting_primary);
  
  if (acting_primary >= 0 && acting_primary != CRUSH_ITEM_NONE) {
    return get_primary_test_pg()->get_peering_state()->is_clean();
  }
  return false;
}

std::string ECPeeringTestFixture::get_state_name(int shard) {
  return get_test_pg_by_shard(shard)->get_peering_state()->get_current_state();
}

void ECPeeringTestFixture::suspend_osd(int osd) {
  if (event_loop) {
    event_loop->suspend_to_osd(osd);
  }
}

void ECPeeringTestFixture::unsuspend_osd(int osd) {
  if (event_loop) {
    event_loop->unsuspend_to_osd(osd);
  }
}

bool ECPeeringTestFixture::is_osd_suspended(int osd) {
  return event_loop && event_loop->is_to_osd_suspended(osd);
}

void ECPeeringTestFixture::suspend_primary_to_osd(int to_osd) {
  if (event_loop) {
    int primary = get_primary_shard_from_osdmap();
    if (primary >= 0) {
      event_loop->suspend_from_to_osd(primary, to_osd);
    }
  }
}

void ECPeeringTestFixture::unsuspend_primary_to_osd(int to_osd) {
  if (event_loop) {
    int primary = get_primary_shard_from_osdmap();
    if (primary >= 0) {
      event_loop->unsuspend_from_to_osd(primary, to_osd);
    }
  }
}

void ECPeeringTestFixture::inject_read_error_for_shard(const std::string& obj_name, int shard, int error_code)
{
  hobject_t hoid(object_t(obj_name), "", CEPH_NOSNAP, 0, pool_id, "");
  ghobject_t ghoid(hoid, ghobject_t::NO_GEN, shard_id_t(shard));

  OsdTestFixture* osd_fixture = get_osd_fixture(shard);
  ceph_assert(osd_fixture != nullptr && osd_fixture->store != nullptr);
  osd_fixture->store->inject_read_error(ghoid, error_code);
}

PeeringState* ECPeeringTestFixture::get_child_peering_state(int shard) {
  spg_t key(child_pgid, shard_id_t(shard));
  TestPG* test_pg = get_test_pg(shard, key);
  ceph_assert(test_pg != nullptr);
  return test_pg->get_peering_state();
}

// The child's log/missing/info are populated by the subsequent
// PeeringState::split_into() call.
PeeringState* ECPeeringTestFixture::create_child_peering_state(int shard,
                                                               unsigned split_bits)
{
  spg_t child_spgid(child_pgid, shard_id_t(shard));
  pg_shard_t pg_whoami(shard, shard_id_t(shard));

  // Ensure OSD fixture exists for this shard
  ensure_osd_fixture_exists(shard);
  auto* osd_fixture = get_osd_fixture(shard);
  ceph_assert(osd_fixture != nullptr);

  // Create child ObjectStore collection at the post-split bit depth
  coll_t child_coll(child_spgid);
  auto child_ch = osd_fixture->store->create_new_collection(child_coll);
  {
    ObjectStore::Transaction t;
    // Create at the post-split bit depth; split_collection() asserts dest bits == split_bits.
    t.create_collection(child_coll, split_bits);
    osd_fixture->store->queue_transaction(child_ch, std::move(t));
  }

  // Create child TestPG
  TestPG* child_test_pg = osd_fixture->create_pg(child_spgid, pg_whoami);
  child_test_pg->coll = child_coll;
  child_test_pg->ch = child_ch;

  // Create ShardDpp for child
  child_test_pg->dpp = std::make_unique<ShardDpp>(g_ceph_context, this, child_test_pg);

  // Create backend listener for child
  auto child_bl = std::make_unique<MockPGBackendListener>(
    osdmap, pool_id, child_test_pg->dpp.get(), pg_whoami);
  child_bl->info.pgid = child_spgid;
  for (int j = 0; j < k + m; j++) {
    child_bl->shardset.insert(pg_shard_t(j, shard_id_t(j)));
    child_bl->acting_recovery_backfill_shard_id_set.insert(shard_id_t(j));
    pg_info_t shard_pg_info;
    shard_pg_info.pgid = spg_t(child_pgid, shard_id_t(j));
    child_bl->shard_info[pg_shard_t(j, shard_id_t(j))] = shard_pg_info;
    child_bl->shard_missing[pg_shard_t(j, shard_id_t(j))] = pg_missing_t();
  }
  child_bl->set_store(osd_fixture->store.get(), child_ch);
  child_bl->set_event_loop(event_loop.get());
  child_bl->set_messenger(messenger.get());

  // Create peering listener for child
  child_test_pg->peering_listener = std::make_unique<MockPeeringListener>(
    osdmap, pool_id, child_test_pg->dpp.get(), pg_whoami,
    std::move(child_bl),
    osd_fixture->store.get(), child_coll, child_ch);

  auto& pl = child_test_pg->peering_listener;
  pl->current_epoch = osdmap->get_epoch();
  pl->inject_event_stall = stall_recovery_reservations;
  pl->set_messenger(messenger.get());
  pl->set_event_loop(event_loop.get());
  pl->backend_listener->set_messenger(messenger.get());
  pl->queue_transaction_callback =
    [this, child_test_pg](ObjectStore::Transaction&& t) -> int {
      return queue_transaction_helper(child_test_pg, std::move(t));
    };

  // Construct PeeringState and wire everything (common with ensure_test_pg_exists).
  create_peering_state_common(child_test_pg, child_spgid, pg_whoami);

  return child_test_pg->peering_state.get();
}

void ECPeeringTestFixture::ensure_osd_fixture_exists(int osd) {
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
void ECPeeringTestFixture::create_peering_state_common(
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
    get_is_readable_predicate(),
    get_is_recoverable_predicate());

  test_pg->peering_state = std::move(ps);
  pl->backend_listener->set_peering_state(test_pg->peering_state.get());
  test_pg->peering_ctx = std::make_unique<PeeringCtx>();
  // Wire pl->ctx so MockPeeringListener lambdas route deferred events to the
  // correct PG (parent vs. split child) when both coexist after split_pg().
  pl->ctx = test_pg->peering_ctx.get();
}

void ECPeeringTestFixture::ensure_test_pg_exists(pg_shard_t pg_whoami) {
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

  // Create EC backend using the TestPG's collection
  auto shard_ec_switch = std::make_unique<ECSwitch>(
    shard_listener.get(), test_pg->coll, test_pg->ch, osd_fixture->store.get(),
    g_ceph_context, ec_impl, stripe_unit * k, *osd_fixture->lru);

  // Store in TestPG
  test_pg->backend_listener = std::move(shard_listener);
  test_pg->backend = std::move(shard_ec_switch);

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

pg_t ECPeeringTestFixture::split_pg()
{
  // This harness supports exactly one 1->2 PG split.  The child PG identity
  // is stored in child_pgid and can only be set once; a second call would
  // silently overwrite it and orphan all existing child state.  A single
  // 1→2 split is sufficient to reproduce all known split-related bugs;
  // assert the pool is still at pg_num 1 to catch misuse.
  {
    const pg_pool_t* p = osdmap->get_pg_pool(pool_id);
    ceph_assert(p != nullptr);
    ceph_assert(p->get_pg_num() == 1 && "split_pg() supports only a single 1->2 split");
  }

  const unsigned new_pg_num = 2;
  const unsigned split_bits = pgid.get_split_bits(new_pg_num);
  child_pgid = pg_t(1, pool_id);  // seed 1 = child of seed 0 for pg_num 1 -> 2

  // Read the parent's current acting set from the osdmap so the new epoch
  // preserves whatever acting set the test built before the split (e.g. after
  // an OSD failure).  The child inherits the same set; a caller that needs a
  // different child up-map can advance the epoch after the split.
  std::vector<int> up_osds;
  {
    std::vector<int> acting_osds;
    int up_primary = -1, acting_primary = -1;
    osdmap->pg_to_up_acting_osds(pgid, &up_osds, &up_primary,
                                 &acting_osds, &acting_primary);
  }

  // 1. Bump pg_num to 2 and add upmaps for both parent and child.
  auto new_osdmap = std::make_shared<OSDMap>();
  new_osdmap->deepish_copy_from(*osdmap);
  {
    OSDMap::Incremental inc(new_osdmap->get_epoch() + 1);
    inc.fsid = new_osdmap->get_fsid();
    const pg_pool_t* cur = new_osdmap->get_pg_pool(pool_id);
    ceph_assert(cur != nullptr);
    pg_pool_t updated = *cur;
    updated.set_pg_num(new_pg_num);
    updated.set_pgp_num(new_pg_num);
    inc.new_pools[pool_id] = updated;
    inc.new_pg_upmap[pgid] =
      mempool::osdmap::vector<int32_t>(up_osds.begin(), up_osds.end());
    inc.new_pg_upmap[child_pgid] =
      mempool::osdmap::vector<int32_t>(up_osds.begin(), up_osds.end());
    new_osdmap->apply_incremental(inc);
  }

  // Advance each parent shard to the new osdmap so split_into() can resolve
  // child_pgid.  Use a throwaway PeeringCtx to discard the new-interval
  // peering queries — we must not re-peer the parent.
  osdmap = new_osdmap;
  for_each_test_pg([&](int osd, TestPG* test_pg) {
    if (test_pg->spgid.pgid != pgid) return;  // only advance parent shards
    if (!test_pg->has_peering_state()) return;
    test_pg->get_peering_listener()->current_epoch = osdmap->get_epoch();
  });
  {
    std::vector<int> acting_osds;
    int up_primary = -1, acting_primary = -1;
    osdmap->pg_to_up_acting_osds(pgid, &up_osds, &up_primary,
                                 &acting_osds, &acting_primary);
    for (int shard = 0; shard < k + m; shard++) {
      PeeringState* ps = get_peering_state(shard);
      OSDMapRef lastmap = ps->get_osdmap();
      PeeringCtx throwaway;
      ps->advance_map(osdmap, lastmap, up_osds, up_primary, acting_osds,
                      acting_primary, throwaway);
      (void)throwaway.transaction.claim_and_reset();
    }
  }

  // 2. Split each shard: create the child state, run production split_into(),
  //    then split the ObjectStore collection.
  for (int shard = 0; shard < k + m; shard++) {
    create_child_peering_state(shard, split_bits);
    auto* parent_ps = get_peering_state(shard);
    parent_ps->split_into(child_pgid, get_child_peering_state(shard), split_bits);

    // Split the ObjectStore collection
    auto* osd_fixture = get_osd_fixture(shard);
    ceph_assert(osd_fixture != nullptr);
    spg_t parent_spgid(pgid, shard_id_t(shard));
    TestPG* parent_test_pg = get_test_pg(shard, parent_spgid);
    ceph_assert(parent_test_pg != nullptr);
    ObjectStore::Transaction t;
    spg_t child_spgid(child_pgid, shard_id_t(shard));
    TestPG* child_test_pg = get_test_pg(shard, child_spgid);
    ceph_assert(child_test_pg != nullptr);
    t.split_collection(parent_test_pg->coll, split_bits, child_pgid.ps(),
                       child_test_pg->coll);
    osd_fixture->store->queue_transaction(parent_test_pg->ch, std::move(t));
  }

  // 3. Peer the child PG: advance_map/activate_map cycles, applying up_thru and
  //    pg_temp as the monitor would.  The child primary requests pg_temp for EC
  //    primaryfirst ordering and stalls in WaitActingChange without it.
  for (int shard = 0; shard < k + m; shard++) {
    auto evt = std::make_shared<PGPeeringEvent>(
      osdmap->get_epoch(), osdmap->get_epoch(), PeeringState::Initialize());
    spg_t child_spgid(child_pgid, shard_id_t(shard));
    TestPG* child_test_pg = get_test_pg(shard, child_spgid);
    ceph_assert(child_test_pg != nullptr);
    child_test_pg->get_peering_state()->handle_event(
      evt, child_test_pg->get_peering_ctx());
  }
  event_loop->run_until_idle();

  // Returns true if any up_thru or pg_temp was applied (more cycles needed).
  // Delegates to apply_new_epoch() which shares the implementation with new_epoch().
  auto child_apply_new_epoch = [this]() -> bool {
    return apply_new_epoch(child_pgid, /*if_required=*/true);
  };

  int max_cycles = 10;
  bool more = true;
  while (more && --max_cycles) {
    for (int shard = 0; shard < k + m; shard++) {
      PeeringState* ps = get_child_peering_state(shard);
      OSDMapRef lastmap = ps->get_osdmap();
      if (lastmap->get_epoch() == osdmap->get_epoch()) {
        continue;
      }
      std::vector<int> c_up_osds, c_acting_osds;
      int c_up_primary = -1, c_acting_primary = -1;
      osdmap->pg_to_up_acting_osds(child_pgid, &c_up_osds, &c_up_primary,
                                   &c_acting_osds, &c_acting_primary);
      spg_t child_spgid(child_pgid, shard_id_t(shard));
      TestPG* child_test_pg = get_test_pg(shard, child_spgid);
      ceph_assert(child_test_pg != nullptr);
      ps->advance_map(osdmap, lastmap, c_up_osds, c_up_primary, c_acting_osds,
                      c_acting_primary, *child_test_pg->get_peering_ctx());
    }
    event_loop->run_until_idle();
    for (int shard = 0; shard < k + m; shard++) {
      spg_t child_spgid(child_pgid, shard_id_t(shard));
      TestPG* child_test_pg = get_test_pg(shard, child_spgid);
      ceph_assert(child_test_pg != nullptr);
      get_child_peering_state(shard)->activate_map(*child_test_pg->get_peering_ctx());
    }
    event_loop->run_until_idle();

    more = child_apply_new_epoch();
  }

  return child_pgid;
}

void ECPeeringTestFixture::init_peering(TestPG *test_pg)
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

void ECPeeringTestFixture::update_osdmap_with_peering(
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

void ECPeeringTestFixture::new_epoch_loop() {
  int max = 10;
  do {
    ceph_assert(--max);
    event_advance_map();
    event_activate_map();
  } while (new_epoch(true));
}

bool ECPeeringTestFixture::new_epoch(bool if_required)
{
  return apply_new_epoch(this->pgid, if_required);
}

bool ECPeeringTestFixture::apply_new_epoch(pg_t which_pg, bool if_required)
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
    shard_id_t shard(s++);
    // Skip failed OSDs (marked as CRUSH_ITEM_NONE)
    if (osd == CRUSH_ITEM_NONE) {
      continue;
    }
    spg_t spg(which_pg, shard_id_t(shard));
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

int ECPeeringTestFixture::queue_transaction_helper(TestPG* test_pg, ObjectStore::Transaction&& t)
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

void ECPeeringTestFixture::mark_osd_down(int osd_id)
{
  // Create new OSDMap with the OSD marked as down
  // This emulates what the real monitor does: just mark the OSD down,
  // do NOT set pg_temp. Peering will detect the change and request pg_temp.
  auto new_osdmap = std::make_shared<OSDMap>();
  new_osdmap->deepish_copy_from(*osdmap);
  OSDMapTestHelpers::mark_osd_down(new_osdmap, osd_id);
  
  update_osdmap_with_peering(new_osdmap);
}

void ECPeeringTestFixture::mark_osd_up(int osd_id)
{
  // Create new OSDMap with the OSD marked as up using OSDMapTestHelpers
  auto new_osdmap = std::make_shared<OSDMap>();
  new_osdmap->deepish_copy_from(*osdmap);
  OSDMapTestHelpers::mark_osd_up(new_osdmap, osd_id);
  
  update_osdmap_with_peering(new_osdmap);
}

void ECPeeringTestFixture::mark_osds_down(const std::vector<int>& osd_ids)
{
  // Create new OSDMap with all OSDs marked as down using OSDMapTestHelpers
  auto new_osdmap = std::make_shared<OSDMap>();
  new_osdmap->deepish_copy_from(*osdmap);
  OSDMapTestHelpers::mark_osds_down(new_osdmap, osd_ids);
  
  update_osdmap_with_peering(new_osdmap);
}

void ECPeeringTestFixture::advance_epoch()
{
  auto new_osdmap = std::make_shared<OSDMap>();
  new_osdmap->deepish_copy_from(*osdmap);
  OSDMapTestHelpers::advance_epoch(new_osdmap);
  
  update_osdmap_with_peering(new_osdmap);
}

void ECPeeringTestFixture::run_recovery_and_verify_callbacks(
  const std::string& obj_name,
  int removed_osd,
  const std::string& expected_data)
{
  // Delegate to the parallel version with a single object
  run_parallel_recovery_and_verify_callbacks(
    {obj_name},
    removed_osd,
    {expected_data});
}

// Helper function that performs the actual recovery logic
// Must be called within event loop context on the primary OSD
void ECPeeringTestFixture::do_run_parallel_recovery_and_verify_callbacks_impl(
  const std::vector<std::string>& obj_names,
  int target_osd,
  const std::vector<std::string>& expected_data,
  int primary_shard)
{
  auto primary_ps = get_peering_state(primary_shard);
  pg_shard_t target_shard(target_osd, shard_id_t(target_osd));

  std::cout << "\n=== Starting Parallel Recovery for " << obj_names.size()
            << " objects ===" << std::endl;

  // Step 1: Verify all objects are in the missing set and prepare recovery
  std::vector<hobject_t> hoids;
  std::vector<ObjectContextRef> obcs;
  std::vector<pg_missing_item> missing_items;

  for (size_t i = 0; i < obj_names.size(); ++i) {
    hobject_t hoid = make_test_object(obj_names[i]);
    hoids.push_back(hoid);

    pg_missing_item missing_item;

    // Check if the target OSD is the current primary
    // If so, check the primary's own missing set; otherwise check peer_missing
    if (target_osd == primary_shard) {
      // The target OSD became primary again after coming back up
      // Check the primary's own missing set
      const pg_missing_t& primary_missing = primary_ps->get_pg_log().get_missing();
      ASSERT_TRUE(primary_missing.have_missing())
        << "Primary OSD " << target_osd << " should have missing objects after coming back up";

      ASSERT_TRUE(primary_missing.is_missing(hoid, &missing_item))
        << "Object " << obj_names[i] << " should be in primary " << target_osd << "'s missing set";

      std::cout << "  OSD " << target_osd << " is the primary and has object " << obj_names[i] << " in its own missing set" << std::endl;
      obcs.push_back(ObjectContextRef());
    } else {

      // The target OSD is a peer, check peer_missing
      const auto& peer_missing_map = primary_ps->get_peer_missing();
      auto peer_missing_it = peer_missing_map.find(target_shard);
      ASSERT_NE(peer_missing_it, peer_missing_map.end())
        << "Primary should have peer_missing entry for OSD " << target_osd;

      const pg_missing_t& peer_missing = peer_missing_it->second;
      ASSERT_TRUE(peer_missing.have_missing())
        << "Peer OSD " << target_osd << " should have missing objects after coming back up";

      ASSERT_TRUE(peer_missing.is_missing(hoid, &missing_item))
        << "Object " << obj_names[i] << " should be in peer " << target_osd << "'s missing set";

      auto target_ps = get_peering_state(target_osd);
      const pg_missing_t& target_missing = target_ps->get_pg_log().get_missing();
      ASSERT_TRUE(target_missing.have_missing())
        << "Target OSD " << target_osd << " should have missing objects after coming back up";

      pg_missing_item target_missing_item;
      ASSERT_TRUE(target_missing.is_missing(hoid, &target_missing_item))
        << "Object " << obj_names[i] << " should be in peer " << target_osd << "'s missing set";

      ASSERT_EQ(target_missing_item, missing_item) << "Missing on shard and primary should match";

      // Read the OI directly from the primary's store to get the authoritative version
      // This avoids relying on potentially stale cached data in the OBC
      TestPG* primary_test_pg = get_test_pg_by_shard(primary_shard);
      ASSERT_TRUE(primary_test_pg && primary_test_pg->ch)
        << "Primary shard " << primary_shard << " must have a valid collection handle";
      OsdTestFixture* primary_fixture = get_osd_fixture(primary_test_pg->pg_whoami.osd);
      ASSERT_TRUE(primary_fixture && primary_fixture->store)
        << "Primary shard " << primary_shard << " must have a store";
      
      ghobject_t primary_ghoid(hoid, ghobject_t::NO_GEN, shard_id_t(primary_shard));
      ceph::buffer::ptr oi_ptr;
      int r = primary_fixture->store->getattr(primary_test_pg->ch, primary_ghoid, OI_ATTR, oi_ptr);
      ASSERT_GE(r, 0) << "Failed to read OI_ATTR from primary store for " << obj_names[i];
      
      bufferlist oi_bl;
      oi_bl.append(oi_ptr);
      object_info_t oi;
      auto p = oi_bl.cbegin();
      oi.decode(p);
      
      std::cout << "  OSD " << target_osd << " is a peer and has object " << obj_names[i]
                << " in peer_missing (OI version from primary store: " << oi.version << ")" << std::endl;
      
      // Verify the missing item's need version matches what we read from the store
      ASSERT_EQ(missing_item.need, oi.version)
        << "Missing item need version should match OI version from primary store for " << obj_names[i];
      
      // Get OBC for this object - matches PrimaryLogPG::prep_object_replica_pushes behavior
      // which calls get_object_context(soid, false) and handles null response
      // Pass can_create=false to ensure we reload from disk with all attributes
      ObjectContextRef obc = get_object_context(hoid, false);
      ASSERT_TRUE(obc) << "Failed to load OBC from disk for " << obj_names[i];
      ASSERT_FALSE(obc->attr_cache.empty())
        << "OBC attr_cache must be populated for recovery of " << obj_names[i];
      obcs.push_back(obc);
    }

    missing_items.push_back(missing_item);
  }

  // Reset recovery callback tracker before starting recovery
  auto* primary_listener = get_primary_listener();
  primary_listener->recovery_tracker.reset();

  // Step 2: Open a single recovery operation handle
  std::cout << "\n  Opening single recovery operation for all objects..." << std::endl;
  PGBackend::RecoveryHandle *h = get_primary_backend()->open_recovery_op();

  // Step 3: Queue ALL objects for recovery in this single operation
  // This is the key difference - all objects share the same recovery operation
  std::cout << "  Queuing all " << obj_names.size() << " objects for parallel recovery..." << std::endl;
  for (size_t i = 0; i < obj_names.size(); ++i) {
    std::cout << "    Queuing object " << obj_names[i] << " (hoid: " << hoids[i] << ")" << std::endl;
    int r = get_primary_backend()->recover_object(
      hoids[i],
      missing_items[i].need,
      ObjectContextRef(),
      obcs[i],
      h);
    ASSERT_EQ(0, r) << "recover_object should successfully queue " << obj_names[i];
  }

  // Step 4: Run the recovery operation ONCE for all objects
  // This processes all queued recoveries together in a single operation
  std::cout << "\n  Running single recovery operation for all queued objects..." << std::endl;
  std::cout << "  (This is where Bug 75432 would trigger if present)" << std::endl;
  get_primary_backend()->run_recovery_op(h, 10);  // priority = 10
  event_loop->run_until_idle();

  // Step 5: Verify recovery callbacks and data for all objects
  std::cout << "\n  === Recovery Callback Verification ===" << std::endl;
  std::cout << "  on_local_recover calls: " << primary_listener->recovery_tracker.on_local_recover_calls << std::endl;
  std::cout << "  on_peer_recover calls: " << primary_listener->recovery_tracker.on_peer_recover_calls.size() << " peers" << std::endl;
  std::cout << "  on_global_recover calls: " << primary_listener->recovery_tracker.on_global_recover_calls << std::endl;

  for (size_t i = 0; i < obj_names.size(); ++i) {
    std::cout << "\n  Verifying object " << obj_names[i] << "..." << std::endl;

    // Verify recovery callback was called for this object
    bool callback_found = false;
    if (target_osd == primary_shard) {
      // Local recovery
      for (const auto& obj : primary_listener->recovery_tracker.on_local_recover_objects) {
        if (obj == hoids[i]) {
          callback_found = true;
          break;
        }
      }
      EXPECT_TRUE(callback_found)
        << "on_local_recover should be called for " << obj_names[i];
    } else {
      // Peer recovery
      for (const auto& [peer, obj] : primary_listener->recovery_tracker.on_peer_recover_objects) {
        if (peer == target_shard && obj == hoids[i]) {
          callback_found = true;
          break;
        }
      }
      EXPECT_TRUE(callback_found)
        << "on_peer_recover should be called for " << obj_names[i];
    }

    // Verify the recovered data
    bufferlist read_bl;
    if (expected_data[i].size() > 0)
    {
      int r = read_object(obj_names[i], 0, expected_data[i].length(),
                         read_bl, expected_data[i].length());
      EXPECT_EQ(r, (int)expected_data[i].length())
        << "Should read full object " << obj_names[i];

      std::string read_data(read_bl.c_str(), read_bl.length());
      EXPECT_EQ(read_data, expected_data[i])
        << "Recovered data should match for " << obj_names[i];
    }

    std::cout << "  ✓ Object " << obj_names[i] << " recovered successfully" << std::endl;
  }

  // Verify on_global_recover was called for all objects
  EXPECT_EQ((int)obj_names.size(), primary_listener->recovery_tracker.on_global_recover_calls)
    << "on_global_recover should be called once for each object";

  std::cout << "\n  === All parallel recovery callbacks and data verified successfully ===" << std::endl;
}

// Public interface that schedules the recovery on the primary OSD
void ECPeeringTestFixture::run_parallel_recovery_and_verify_callbacks(
  const std::vector<std::string>& obj_names,
  int target_osd,
  const std::vector<std::string>& expected_data)
{
  // Verify we have matching sizes
  ASSERT_EQ(obj_names.size(), expected_data.size())
    << "obj_names and expected_data must have the same size";

  // Get the actual primary from the OSDMap
  int primary_shard = get_primary_shard_from_osdmap();
  if (primary_shard < 0 || primary_shard == CRUSH_ITEM_NONE) {
    // No valid primary, cannot run recovery
    return;
  }
  
  // Schedule the recovery operation on the primary OSD
  event_loop->schedule_transaction(primary_shard, [this, obj_names, target_osd, expected_data, primary_shard]() {
    do_run_parallel_recovery_and_verify_callbacks_impl(obj_names, target_osd, expected_data, primary_shard);
  });
  event_loop->run_until_idle();
}
