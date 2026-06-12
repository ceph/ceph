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

// ShardDpp implementation
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
  
  // Override epoch getter to use peering listeners from OsdTestFixture
  // Use the osd parameter to look up the TestPG, since EventLoop context
  // may not be set when this is called
  messenger->set_epoch_getter([this](int osd) -> epoch_t {
    TestPG* test_pg = get_test_pg();
    if (test_pg && test_pg->has_peering_state()) {
      return test_pg->get_peering_listener()->get_osdmap_epoch();
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
    
    // Handle the event on the destination shard's peering state
    PeeringCtx* ctx = get_test_pg()->get_peering_ctx();
    auto ps = get_test_pg()->get_peering_state();
    ps->handle_event(evt_ref, ctx);

    auto t = ctx->transaction.claim_and_reset();
    int r = queue_transaction_helper(to_osd, std::move(t));
    ceph_assert( r >= 0 );
    return true;  // Message handled
  };
  
  // Register the same handler for all peering message types
  messenger->register_typed_handler<MOSDPeeringOp>(MSG_OSD_PG_QUERY2, peering_handler);
  messenger->register_typed_handler<MOSDPeeringOp>(MSG_OSD_PG_NOTIFY2, peering_handler);
  messenger->register_typed_handler<MOSDPeeringOp>(MSG_OSD_PG_INFO2, peering_handler);
  messenger->register_typed_handler<MOSDPeeringOp>(MSG_OSD_PG_LOG, peering_handler);
  messenger->register_typed_handler<MOSDPeeringOp>(MSG_OSD_PG_LEASE, peering_handler);
  messenger->register_typed_handler<MOSDPeeringOp>(MSG_OSD_PG_LEASE_ACK, peering_handler);
  messenger->register_typed_handler<MOSDPeeringOp>(MSG_OSD_RECOVERY_RESERVE, peering_handler);

  // Register idle callback to check for buffered messages
  event_loop->register_idle_callback([this]() -> bool {
    bool found_messages = false;
    // Check all PeeringCtx objects for buffered messages
    for (auto& [osd, osd_fixture] : osd_fixtures) {
      for (auto& [spgid, test_pg] : osd_fixture->pgs) {
        if (test_pg && test_pg->has_peering_state()) {
          PeeringCtx* ctx = test_pg->get_peering_ctx();
          if (!ctx->message_map.empty()) {
            dispatch_buffered_messages(osd, ctx);
            found_messages = true;
          }
        }
      }
    }
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

void ECPeeringTestFixture::ensure_osd_fixture_exists(int osd) {
  if (osd_fixtures.find(osd) != osd_fixtures.end()) {
    return;
  }
  
  // Create OsdTestFixture for this OSD
  auto osd_fixture = std::make_unique<OsdTestFixture>(osd);
  
  // Create a new store for this OSD
  osd_fixture->store = MockStore::create(g_ceph_context, osd);
  ceph_assert(osd_fixture->store);
  osd_fixture->data_dir = osd_fixture->store->get_path();
  
  // Create collection for this shard
  spg_t shard_spgid(pgid, shard_id_t(osd));
  coll_t shard_coll(shard_spgid);
  auto shard_ch = osd_fixture->store->create_new_collection(shard_coll);
  
  ObjectStore::Transaction t;
  t.create_collection(shard_coll, 0);
  int r = osd_fixture->store->queue_transaction(shard_ch, std::move(t));
  ceph_assert(r == 0);

  osd_fixture->coll = shard_coll;
  osd_fixture->ch = shard_ch;
  
  // Create extent cache LRU for this OSD
  osd_fixture->lru = std::make_unique<ECExtentCache::LRU>(1024 * 1024 * 100);
  
  osd_fixtures[osd] = std::move(osd_fixture);
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
  
  TestPG* test_pg = osd_fixture->create_pg(shard_spgid, pg_whoami);
  
  const pg_pool_t* pool_ptr = OSDMapTestHelpers::get_pool(osdmap, pool_id);
  ceph_assert(pool_ptr != nullptr);
  
  // Create backend listener
  auto shard_listener = std::make_unique<MockPGBackendListener>(
    osdmap, pool_id, dpp.get(), pg_whoami);

  // Initialize the listener's own info.pgid so OSDMap queries work
  shard_listener->info.pgid = shard_spgid;
  shard_listener->set_store(osd_fixture->store.get(), osd_fixture->ch);
  shard_listener->set_event_loop(event_loop.get());
  shard_listener->set_messenger(messenger.get());

  // Create EC backend
  auto shard_ec_switch = std::make_unique<ECSwitch>(
    shard_listener.get(), osd_fixture->coll, osd_fixture->ch, osd_fixture->store.get(),
    g_ceph_context, ec_impl, stripe_unit * k, *osd_fixture->lru);

  // Store in TestPG
  test_pg->backend_listener = std::move(shard_listener);
  test_pg->backend = std::move(shard_ec_switch);

  // Create ShardDpp and store in TestPG
  test_pg->dpp = std::make_unique<ShardDpp>(g_ceph_context, this, test_pg);

  // Construct MockPeeringListener, transferring ownership of the backend
  // listener from TestPG. The backend listener was created by setup_ec_pool().
  auto peering_listener = std::make_unique<MockPeeringListener>(
    osdmap, pool_id, test_pg->dpp.get(), pg_whoami,
    std::move(test_pg->backend_listener),
    osd_fixture->store.get(), osd_fixture->coll, osd_fixture->ch);

  peering_listener->current_epoch = osdmap->get_epoch();
  peering_listener->set_messenger(messenger.get());
  peering_listener->set_event_loop(event_loop.get());
  peering_listener->set_fixture(this);
  peering_listener->backend_listener->set_messenger(messenger.get());

  peering_listener->queue_transaction_callback =
    [this, pg_whoami](ObjectStore::Transaction&& t) -> int {
      return queue_transaction_helper((int)pg_whoami.shard, std::move(t));
  };

  // Pass the PGPool from the backend_listener to PeeringState
  // The listener's pool.info will be updated in place when the OSDMap changes
  auto ps = std::make_unique<PeeringState>(
    g_ceph_context,
    pg_whoami,
    shard_spgid,
    peering_listener->backend_listener->pool,
    osdmap,
    PG_FEATURE_CLASSIC_ALL,
    test_pg->dpp.get(),
    peering_listener.get());

  peering_listener->ps = ps.get();

  ps->set_backend_predicates(
    get_is_readable_predicate(),
    get_is_recoverable_predicate());

  // Store everything in TestPG
  test_pg->peering_state = std::move(ps);
  peering_listener->backend_listener->set_peering_state(test_pg->peering_state.get());
  test_pg->peering_listener = std::move(peering_listener);
  test_pg->peering_ctx = std::make_unique<PeeringCtx>();

  init_peering(test_pg);
  
  // Schedule Initialize event for this newly created TestPG
  auto evt = std::make_shared<PGPeeringEvent>(
    osdmap->get_epoch(),
    osdmap->get_epoch(),
    PeeringState::Initialize());
  
  event_loop->schedule_peering_event(osd, test_pg, [this, evt]() {
    auto test_pg = get_test_pg();
    test_pg->get_peering_state()->handle_event(evt, test_pg->get_peering_ctx());
  });

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
    event_loop->schedule_peering_event(osd, test_pg.get(), [this]
    {
      std::vector<int> up_osds, acting_osds;
      int up_primary = -1, acting_primary = -1;

      osdmap->pg_to_up_acting_osds(pgid, &up_osds, &up_primary, &acting_osds, &acting_primary);
      PeeringState* ps = get_peering_state();
        get_peering_state()->advance_map(osdmap, ps->get_osdmap(),
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
  // Schedule activate_map events for each shard instead of running directly
  for (auto& [osd, osd_fixture] : osd_fixtures) {
    for (auto& [spgid, test_pg] : osd_fixture->pgs) {
      event_loop->schedule_peering_event(osd, test_pg.get(), [this]() {
        get_peering_state()->activate_map(*get_peering_ctx());
      });
    }
  }
  event_loop->run_until_idle();
}

void ECPeeringTestFixture::dispatch_buffered_messages(int from_shard, PeeringCtx* ctx) {
  ceph_assert(messenger);
  ceph_assert(ctx);

  // Check if there are any buffered messages in the context
  for (auto& [target_osd, msg_list] : ctx->message_map) {
    for (auto& msg : msg_list) {
      // Route the message through the messenger
      // msg is a MessageRef (boost::intrusive_ptr<Message>), need to get raw pointer
      // MockMessenger will set the connection when it processes the message
      messenger->send_message(from_shard, target_osd, msg.get());
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
  
  for (int osd : acting_osds) {
    // Skip failed OSDs (marked as CRUSH_ITEM_NONE)
    if (osd == CRUSH_ITEM_NONE) {
      continue;
    }
    if (!get_first_test_pg_for_osd(osd)->get_peering_state()->is_active()) {
      return false;
    }
  }
  return true;
}

bool ECPeeringTestFixture::primary_is_clean() {
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
  ceph_assert(osd_fixture != nullptr && osd_fixture->store);
  osd_fixture->store->inject_read_error(ghoid, error_code);
}

void ECPeeringTestFixture::init_peering(TestPG *test_pg)
{
  pg_history_t history;
  history.same_interval_since = osdmap->get_epoch();
  history.epoch_pool_created = osdmap->get_epoch();
  history.last_epoch_clean = osdmap->get_epoch();
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

  queue_transaction_helper(test_pg->pg_whoami.osd, std::move(t));
}

void ECPeeringTestFixture::update_osdmap_with_peering(
  std::shared_ptr<OSDMap> new_osdmap,
  std::optional<pg_shard_t> new_primary)
{
  OSDMapRef old_osdmap = osdmap;

  update_osdmap(new_osdmap, new_primary);
  new_epoch(false);
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
  bool did_work = false;
  epoch_t e = osdmap->get_epoch();
  OSDMap::Incremental pending_inc(e + 1);
  pending_inc.fsid = osdmap->get_fsid();

  // Get acting set from OSDMap
  std::vector<int> acting_osds;
  int acting_primary = -1;
  osdmap->pg_to_acting_osds(this->pgid, &acting_osds, &acting_primary);

  int s = 0;
  for (int osd : acting_osds) {
    shard_id_t shard(s++);
    // Skip failed OSDs (marked as CRUSH_ITEM_NONE)
    if (osd == CRUSH_ITEM_NONE) {
      continue;
    }
    spg_t spg(pgid, shard_id_t(shard));
    if (get_test_pg(osd, spg)->get_peering_state()->get_need_up_thru()) {
      pending_inc.new_up_thru[osd] = e;
      did_work = true;
    }
  }

  if (acting_primary >= 0) {
    TestPG* test_pg = get_primary_test_pg();
    if (test_pg && test_pg->has_peering_state()) {
      MockPeeringListener* listener = test_pg->get_peering_listener();
      if (listener->pg_temp_wanted) {
        std::vector<int> up_osds;
        int up_primary = -1;
        osdmap->pg_to_up_acting_osds(this->pgid, &up_osds, &up_primary, nullptr, nullptr);

        std::vector<int> acting_temp = listener->next_acting;
        if (acting_temp.empty()) {
          acting_temp = up_osds;
        }

        // For EC pools with optimizations, transform to primaryfirst order before
        // storing in pg_temp. This matches what the real monitor does and what
        // _get_temp_osds() expects when it calls pgtemp_undo_primaryfirst().
        const pg_pool_t* pool = osdmap->get_pg_pool(this->pgid.pool());
        if (pool && pool->allows_ecoptimizations()) {
          acting_temp = osdmap->pgtemp_primaryfirst(*pool, acting_temp);
        }

        pending_inc.new_pg_temp[this->pgid] =
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

  for (auto& [osd, osd_fixture] : osd_fixtures)
  {
    for (auto &[shard, test_pg] : osd_fixture->pgs) {
      if (test_pg->has_peering_state()) {
        test_pg->get_peering_listener()->current_epoch = osdmap->get_epoch();
      }
    }
  }

  return true;
}

int ECPeeringTestFixture::queue_transaction_helper(int shard, ObjectStore::Transaction&& t)
{
  if (t.empty()) {
    return 0;
  }

  // Note: Contexts are stolen by MockPGBackendListener::queue_transaction,
  // so we don't need to call execute_finishers here
  OsdTestFixture* osd_fixture = get_osd_fixture(shard);
  ceph_assert(osd_fixture != nullptr && osd_fixture->store);
  int result = osd_fixture->store->queue_transaction(osd_fixture->ch, std::move(t));

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

void ECPeeringTestFixture::set_pool_min_size(unsigned new_min_size)
{
  auto new_osdmap = std::make_shared<OSDMap>();
  new_osdmap->deepish_copy_from(*osdmap);
  OSDMapTestHelpers::set_pool_min_size(new_osdmap, pool_id, new_min_size);

  update_osdmap_with_peering(new_osdmap);
}

void ECPeeringTestFixture::advance_epoch()
{
  auto new_osdmap = std::make_shared<OSDMap>();
  new_osdmap->deepish_copy_from(*osdmap);
  OSDMapTestHelpers::advance_epoch(new_osdmap);
  
  update_osdmap_with_peering(new_osdmap);
}

void ECPeeringTestFixture::run_recovery(
  const std::string& obj_name,
  bool recover_primary,
  const std::string& expected_data)
{
  // Delegate to the parallel version with a single object
  run_parallel_recovery(
    {obj_name},
    recover_primary,
    {expected_data});
}

// Helper function that performs the actual recovery logic
// Must be called within event loop context on the primary OSD
void ECPeeringTestFixture::do_run_parallel_recovery_impl(
  const std::vector<std::string>& obj_names,
  bool recover_primary,
  const std::vector<std::string>& expected_data,
  int primary_shard)
{
  auto primary_ps = get_primary_test_pg()->get_peering_state();

  std::cout << "\n=== Starting Parallel Recovery for " << obj_names.size()
            << " objects (recover_primary=" << recover_primary << ") ===" << std::endl;

  // Step 1: Verify consistency of missing sets and prepare recovery
  std::vector<hobject_t> hoids;
  std::vector<ObjectContextRef> obcs;
  std::vector<pg_missing_item> missing_items;

  for (size_t i = 0; i < obj_names.size(); ++i) {
    hobject_t hoid = make_test_object(obj_names[i]);
    hoids.push_back(hoid);

    pg_missing_item missing_item;

    if (recover_primary) {
      // Recovering to primary - check the primary's own missing set
      const pg_missing_t& primary_missing = primary_ps->get_pg_log().get_missing();
      ASSERT_TRUE(primary_missing.have_missing())
        << "Primary should have missing objects";

      ASSERT_TRUE(primary_missing.is_missing(hoid, &missing_item))
        << "Object " << obj_names[i] << " should be in primary's missing set";

      std::cout << "  Object " << obj_names[i] << " is in primary's missing set" << std::endl;
      obcs.push_back(ObjectContextRef());
    } else {
      // Recovering to peers - verify consistency between peer_missing_map and actual peer missing sets
      const auto& peer_missing_map = primary_ps->get_peer_missing();
      
      std::cout << "  Verifying missing set consistency for object " << obj_names[i] << std::endl;
      std::cout << "    peer_missing_map has " << peer_missing_map.size() << " entries" << std::endl;

      // For each peer in peer_missing_map, verify it matches the peer's actual missing set
      for (const auto& [peer_shard, peer_missing_from_map] : peer_missing_map) {
        std::cout << "    Checking peer " << peer_shard << std::endl;
        
        // Get the actual missing set from the peer's PeeringState
        auto peer_ps = get_test_pg(peer_shard)->get_peering_state();
        const pg_missing_t& peer_actual_missing = peer_ps->get_pg_log().get_missing();
        
        // Check if this object is in the peer_missing_map for this peer
        pg_missing_item map_missing_item;
        bool in_map = peer_missing_from_map.is_missing(hoid, &map_missing_item);
        
        // Check if this object is in the peer's actual missing set
        pg_missing_item actual_missing_item;
        bool in_actual = peer_actual_missing.is_missing(hoid, &actual_missing_item);
        
        // They should match
        ASSERT_EQ(in_map, in_actual)
          << "Mismatch for object " << obj_names[i] << " on peer " << peer_shard
          << ": in peer_missing_map=" << in_map << ", in actual missing=" << in_actual;
        
        if (in_map && in_actual) {
          ASSERT_EQ(map_missing_item, actual_missing_item)
            << "Missing item mismatch for object " << obj_names[i] << " on peer " << peer_shard;
          std::cout << "      ✓ Object " << obj_names[i] << " missing set consistent for peer " << peer_shard << std::endl;
          
          // Use the first peer's missing_item for recovery
          if (missing_items.size() == i) {
            missing_item = map_missing_item;
          }
        }
      }
      
      // Get OBC for this object
      ObjectContextRef obc = get_object_context(hoid, false);
      ASSERT_TRUE(obc) << "Failed to load OBC from disk for " << obj_names[i];
      ASSERT_FALSE(obc->attr_cache.empty())
        << "OBC attr_cache must be populated for recovery of " << obj_names[i];
      obcs.push_back(obc);
    }

    missing_items.push_back(missing_item);
  }

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
  get_primary_backend()->run_recovery_op(h, 10);  // priority = 10
}

// Helper to check recovery completion and queue appropriate events
// This mimics what PrimaryLogPG::start_recovery_ops() does when recovery completes
void ECPeeringTestFixture::check_recovery_completion_impl(int osd_id)
{
  auto test_pg = get_first_test_pg_for_osd(osd_id);
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

// Public interface that schedules the recovery on the primary OSD
void ECPeeringTestFixture::run_parallel_recovery(
  const std::vector<std::string>& obj_names,
  bool recover_primary,
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
  event_loop->schedule_transaction(primary_shard, [this, obj_names, recover_primary, expected_data, primary_shard]() {
    do_run_parallel_recovery_impl(obj_names, recover_primary, expected_data, primary_shard);
  });
  event_loop->run_until_idle();
  
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
    event_loop->schedule_transaction(osd, [this, osd]() {
      check_recovery_completion_impl(osd);
    });
  }
  event_loop->run_until_idle();
}

