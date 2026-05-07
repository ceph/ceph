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

 /* This is a test harness for testing PeeringState (with PGLog
  * and MissingLoc) and the peering process. Because the main
  * purpose of peering is to reconcile the state of a PG across
  * the cluster the test harness simulates multiple OSDs each with
  * their own instance of PeeringState and emulates messenger and
  * the OSD scheduler passing peering messages between them. This
  * allows both unit tests of individual functions and tests of
  * the whole peering cycle. PeeringState coordinates recovery
  * and backfill, the test harness emulates enough of PG and
  * PrimaryLogPG to allow this to be tested.
  */

#include <memory>
#include <gtest/gtest.h>
#include "test/osd/MockConnection.h"
#include "test/osd/MockECRecPred.h"
#include "test/osd/MockECReadPred.h"
#include "test/osd/MockPeeringListener.h"
#include "global/global_init.h"
#include "messages/MOSDPeeringOp.h"
#include "msg/Connection.h"
#include "os/ObjectStore.h"

// dout using global context and OSD subsystem
// main sets OSD subsystem debug level
#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_osd

using namespace std;

IsPGRecoverablePredicate *get_is_recoverable_predicate() {
  return new MockECRecPred();
}

IsPGReadablePredicate *get_is_readable_predicate() {
  return new MockECReadPred();
}

// Test fixture for PeeringState tests
class PeeringStateTest : public ::testing::Test {
protected:
  std::shared_ptr<OSDMap> osdmap;
  std::vector<int> up;
  std::vector<int> acting;
  std::vector<int> up_acting;
  int up_primary;
  int acting_primary;
  uint64_t pool_id;
  int pool_size;
  osd_reqid_t reqid;

  // Per OSD state
  std::map<int,unique_ptr<PeeringState>> osd_peeringstate;
  std::map<int,unique_ptr<PeeringCtx>> osd_peeringctx;
  std::map<int,unique_ptr<MockPeeringListener>> listeners;

  // Dpp helper
  // Generate log output that includes the OSD and shard (because tests
  // simulate multiple OSDs) and the PeeringState
  class DppHelper : public NoDoutPrefix {
    public:
    PeeringStateTest *t;
    int osd;
    int shard;
    DppHelper(CephContext *cct, unsigned subsys, PeeringStateTest *t, int osd, int shard)
    : NoDoutPrefix(cct, subsys), t(t), osd(osd), shard(shard) {}

    std::ostream& gen_prefix(std::ostream& out) const override
    {
      out << "osd " << osd << "(" << shard << "): ";
      if (t->osd_peeringstate.contains(osd)) {
        PeeringState *ps = t->osd_peeringstate[osd].get();
        out << *ps << " ";
      }
      return out;
    }
  };
  std::map<int,unique_ptr<DppHelper>> dpp;

  DoutPrefixProvider *get_dpp(int osd)
  {
    return dpp[osd].get();
  }

  PeeringState *get_ps(int osd)
  {
    return osd_peeringstate[osd].get();
  }

  bool has_ps(int osd)
  {
    return osd_peeringstate.contains(osd);
  }

  PeeringCtx *get_ctx(int osd)
  {
    return osd_peeringctx[osd].get();
  }

  MockPeeringListener *get_listener(int osd)
  {
    return listeners[osd].get();
  }

  // ============================================================================
  // Helpers for OSDMap, Pools, Epochs and Acting Set
  // ============================================================================

  // Helper to create OSDMap
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
      // Setup osd_xinfo as PeeringState checks features
      osd_xinfo_t xi;
      xi.features = CEPH_FEATURES_ALL;
      pending_inc.new_xinfo[i] = xi;
    }
    osdmap->apply_incremental(pending_inc);
    return osdmap;
  }

  // Helper to update OSDMap and the epoch number in each listener
  void apply_incremental(OSDMap::Incremental inc)
  {
    osdmap->apply_incremental(inc);
    for (const auto &[osd, l] : listeners) {
      l->current_epoch = osdmap->get_epoch();
    }
  }

  // Helper to create new OSDMap epoch and process up_thru and pg_temp updates
  // if_required - if set to true only generates a new epoch if up_thru or pg_temp change required
  // returns true if a new epoch was created
  bool new_epoch(bool if_required = false)
  {
    bool did_work = false;
    epoch_t e = osdmap->get_epoch();
    OSDMap::Incremental pending_inc(e + 1);
    pending_inc.fsid = osdmap->get_fsid();
    for (auto osd: up_acting) {
      if (has_ps(osd)) {
        if (get_ps(osd)->get_need_up_thru()) {
          dout(0) << "new_epoch updating up_thru for osd " << osd << dendl;
          pending_inc.new_up_thru[osd] = e;
          did_work = true;
        }
      }
      if (osd == acting_primary && get_listener(osd)) {
        MockPeeringListener *listener = get_listener(osd);
        if (listener->pg_temp_wanted) {
          dout(0) << "new_epoch updating acting set to " << listener->next_acting << dendl;
          acting = listener->next_acting;
          if (acting.empty()) {
            acting = up;
          }
          listener->pg_temp_wanted = false;
          did_work = true;
        }
      }
    }
    if (!did_work && !if_required) {
      // No need for a new epoch
      return false;
    }
    apply_incremental(pending_inc);
    return true;
  }

  // Note: Currently the acting and up set are being maintained separatly from
  // the OSDMap which gives the test harness a little more control over choice
  // of OSDs and shards than using CRUSH. It would probably be better to use
  // pg_upmap and pg_temp in the OSDMap to control this
  // Unlike OSDMap, the test harness requires both up and acting to be set
  // even if they are the same.

  // Helper to configure up set and acting set
  void setup_up_acting()
  {
    // Simple configuration - up set = acting set = {0, 1, 2, ... }
    up.clear();
    acting.clear();
    up_acting.clear();
    for (int osd = 0; osd < pool_size; osd++) {
      up.push_back(osd);
      acting.push_back(osd);
      up_acting.push_back(osd);
    }
    up_primary = 0;
    acting_primary = 0;
  }

  // Helper to create an EC Pool
  void create_ec_pool(int k = 2, int m = 2, bool fast_ec = true)
  {
    pool_size = k + m;
    OSDMap::Incremental new_pool_inc(osdmap->get_epoch() + 1);
    new_pool_inc.new_pool_max = osdmap->get_pool_max();
    new_pool_inc.fsid = osdmap->get_fsid();
    pool_id = ++new_pool_inc.new_pool_max;
    pg_pool_t empty;
    auto p = new_pool_inc.get_new_pool(pool_id, &empty);
    p->size = pool_size;
    p->min_size = k; // lower than normal to allow more error-injects
    p->set_pg_num(1);
    p->set_pgp_num(1);
    p->type = pg_pool_t::TYPE_ERASURE;
    int r = osdmap->crush->add_simple_rule(
        "erasure", "default", "osd", "",
        "indep", pg_pool_t::TYPE_ERASURE,
        &cerr);
    p->crush_rule = r;
    p->set_flag(pg_pool_t::FLAG_HASHPSPOOL);
    // Warning - name not unique
    new_pool_inc.new_pool_names[pool_id] = "pool";
    std::map<std::string, std::string> erasure_code_profile =
      {{"plugin", "isa"},
       {"technique", "reed_sol_van"},
       {"k", fmt::format("{}", k)},
       {"m", fmt::format("{}", m)},
       {"stripe_unit", "16384"}};
    // Warning - profile not unique
    osdmap->set_erasure_code_profile(
        "default",
        erasure_code_profile);
    p->erasure_code_profile =
        "default";
    p->set_flag(pg_pool_t::FLAG_EC_OVERWRITES);
    if (fast_ec) {
      p->nonprimary_shards.clear();
      for (int i = 1; i < k; i++) {
        p->nonprimary_shards.insert(shard_id_t(i));
      }
      p->set_flag(pg_pool_t::FLAG_EC_OPTIMIZATIONS);
    }
    apply_incremental(new_pool_inc);
    setup_up_acting();
  }

  // Helper to create a Replica Pool
  void create_rep_pool(int n = 3)
  {
    // Create a replica pool
    pool_size = n;
    OSDMap::Incremental new_pool_inc(osdmap->get_epoch() + 1);
    new_pool_inc.new_pool_max = osdmap->get_pool_max();
    new_pool_inc.fsid = osdmap->get_fsid();
    pool_id = ++new_pool_inc.new_pool_max;
    pg_pool_t empty;
    auto p = new_pool_inc.get_new_pool(pool_id, &empty);
    p->size = pool_size;
    p->min_size = 1; // lower than normal to allow more error-injects
    p->set_pg_num(1);
    p->set_pgp_num(1);
    p->type = pg_pool_t::TYPE_REPLICATED;
    p->crush_rule = 0;
    p->set_flag(pg_pool_t::FLAG_HASHPSPOOL);
    // Warning - name not unique
    new_pool_inc.new_pool_names[pool_id] = "pool";
    apply_incremental(new_pool_inc);
    setup_up_acting();
  }

#if POOL_MIGRATION
  // Create a 2nd pool as an EC pool and set up migration from the old pool
  void migrate_to_ec_pool(int k = 2, int m = 2, bool fast_ec = true)
  {
    uint64_t old_pool_id = pool_id;
    int old_pool_size = pool_size;
    create_ec_pool(k, m, fast_ec);
    OSDMap::Incremental pending_inc(osdmap->get_epoch() + 1);
    const pg_pool_t *sp = osdmap->get_pg_pool(old_pool_id);
    pg_pool_t *spi = pending_inc.get_new_pool(old_pool_id, sp);
    const pg_pool_t *tp = osdmap->get_pg_pool(pool_id);
    pg_pool_t *tpi = pending_inc.get_new_pool(pool_id, tp);
    spi->migration_src.reset();
    spi->migration_target = pool_id;
    tpi->migration_src = old_pool_id;
    tpi->migration_target.reset();
    spi->migrating_pgs.emplace(pg_t(0, old_pool_id));
    spi->lowest_migrated_pg = 1;
    apply_incremental(pending_inc);
    // Reset to the source pool
    pool_id = old_pool_id;
    pool_size = old_pool_size;
    setup_up_acting();
  }
#endif

  // Helper to swap an OSD in the up and acting set
  void modify_up_acting(int offset, int osd)
  {
    up[offset] = osd;
    acting[offset] = osd;
    up_acting.push_back(osd);
    new_epoch();
  }

  // Helper - take OSD down by removing it from up/acting set
  void osd_down(int offset, int osd) {
    dout(0) << "= osd." << osd << "(" << offset << ") set down+out =" << dendl;
    ceph_assert(up[offset] == osd);
    up[offset] = pg_pool_t::pg_CRUSH_ITEM_NONE;
    ceph_assert(acting[offset] == osd);
    acting[offset] = pg_pool_t::pg_CRUSH_ITEM_NONE;
    up_acting.erase(remove(up_acting.begin(), up_acting.end(), osd), up_acting.end());
    // Mark the OSD down+out in the OSDMap
    OSDMap::Incremental pending_inc(osdmap->get_epoch() + 1);
    pending_inc.pending_osd_state_set(osd, CEPH_OSD_UP); // XORed
    pending_inc.new_weight[osd] = CEPH_OSD_OUT;
    apply_incremental(pending_inc);
  }

  // Helper - bring OSD up by adding it to up/acting set
  void osd_up(int offset, int osd) {
    dout(0) << "= osd." << osd << "(" << offset << ") set up+in =" << dendl;
    ceph_assert(up[offset] == pg_pool_t::pg_CRUSH_ITEM_NONE);
    up[offset] = osd;
    ceph_assert(acting[offset] == pg_pool_t::pg_CRUSH_ITEM_NONE);
    acting[offset] = osd;
    up_acting.push_back(osd);
    // Mark the OSD up+in in the OSD Map
    OSDMap::Incremental pending_inc(osdmap->get_epoch() + 1);
    pending_inc.pending_osd_state_set(osd, CEPH_OSD_UP);
    pending_inc.new_weight[osd] = CEPH_OSD_IN;
    apply_incremental(pending_inc);
  }

  // ============================================================================
  // Dispatchers - Send work (peering messages) to other OSDs and process queued
  // peering events. These functions emulate Messenger and the OSD Scheduler.
  // Normally dispatch_all() is called to process all the queues until they are
  // all empty, but there are more granular versions which can be used to test
  // state or inject changes after each step
  // ============================================================================

  // Dispatcher - send peering messages to other OSDs from a given OSD. Peering
  // messages are queued in message_map in PeeringCtx
  // toosd - if specified only send messages to the specified OSD
  // num_messages - if specified only send up to the specified number of messages
  bool dispatch_peering_messages( int fromosd, int toosd = -1, int num_messages = -1 )
  {
    PeeringCtx *ctx = get_ctx(fromosd);
    bool did_work = false;
    for (auto& [osd, ls] : ctx->message_map) {
      if (!osdmap->is_up(osd)) {
        dout(0) << __func__ << " skipping down osd." << osd << dendl;
        continue;
      }
      if ( toosd >= 0 && osd != toosd) {
        continue;
      }
      for (auto it = ls.begin(); it != ls.end();) {
        MessageRef m = *it;
        it = ls.erase(it);
        MOSDPeeringOp *pm = static_cast<MOSDPeeringOp*>(m.get());
        dout(0) << __func__ << " sending from osd." << fromosd << " to osd." << osd << " " << *pm << dendl;
        ceph_msg_header h = pm->get_header();
        h.src.num = fromosd;
        pm->set_header(h);
#if WITH_CRIMSON
        pm->set_features(CEPH_FEATURES_ALL);
#else
        ConnectionRef c = ceph::make_ref<MockConnection>();
        pm->set_connection(c);
#endif
        get_ps(osd)->handle_event(PGPeeringEventRef(pm->get_event()), get_ctx(osd));
        // MessageRef m goes out of scope here, automatically releasing the reference
        did_work = true;
        if (num_messages > 0 && --num_messages == 0) {
          return did_work;
        }
      }
    }
    return did_work;
  }

  // Dispatch peering messages to all OSDs until all queues empty
  bool dispatch_all_peering_messages()
  {
    bool rc = false;
    bool did_work;
    do {
      did_work = false;
      for (auto osd: up_acting) {
        did_work |= dispatch_peering_messages(osd);
      }
      rc |= did_work;
    } while (did_work);
    return rc;
  }

  // Dispatcher - send clustering messages to other OSDs from a given OSD. Cluster
  // messages are sent by send_cluster_message and are queued by MockPeeringListener
  // toosd - if specified only send messages to the specified OSD
  // num_messages - if specified only send up to the specified number of messages
  bool dispatch_cluster_messages( int fromosd, int toosd = -1, int num_messages = -1 )
  {
    bool did_work = false;
    for (auto& [osd, ls] : get_listener(fromosd)->messages) {
      if (!osdmap->is_up(osd)) {
        dout(0) << __func__ << " skipping down osd." << osd << dendl;
        continue;
      }
      if ( toosd >= 0 && osd != toosd) {
        continue;
      }
      for (auto it = ls.begin(); it != ls.end();) {
        MessageRef m = *it;
        it = ls.erase(it);
        // NOTE: This dispatcher only handles MOSDPeeringOp-derived messages (MOSDPGLog,
        // MOSDPGNotify2, MOSDPGInfo2, MOSDPGLease, MOSDPGLeaseAck, MOSDPGQuery2, MOSDPGTrim).
        // Non-peering messages like MOSDPGRemove and MRecoveryReserve are sent via
        // send_cluster_message() but are not dispatched through this function - they are
        // handled by other test mechanisms or are not relevant to peering state transitions.
        // This is sufficient for testing PeeringState behavior as all peering-related
        // messages derive from MOSDPeeringOp and provide get_event() for state machine events.
        // Future enhancement: If testing non-peering cluster messages becomes necessary,
        // add type checking and appropriate handling for Message-derived (non-MOSDPeeringOp) types.
        dout(0) << __func__ << " message type = " << m->get_type() << dendl;
        MOSDPeeringOp *pm = static_cast<MOSDPeeringOp*>(m.get());
        dout(0) << __func__ << " sending from osd." << fromosd << " to osd." << osd << " " << *pm << dendl;
        ceph_msg_header h = pm->get_header();
        h.src.num = fromosd;
        pm->set_header(h);
#if WITH_CRIMSON
        pm->set_features(CEPH_FEATURES_ALL);
#else
        ConnectionRef c = ceph::make_ref<MockConnection>();
        pm->set_connection(c);
#endif
        get_ps(osd)->handle_event(PGPeeringEventRef(pm->get_event()), get_ctx(osd));
        // MessageRef m goes out of scope here, automatically releasing the reference
        did_work = true;
        if (num_messages > 0 && --num_messages == 0) {
          return did_work;
        }
      }
    }
    return did_work;
  }

  // Dispatch cluster messages to all OSDs until all queues empty
  bool dispatch_all_cluster_messages()
  {
    bool rc = false;
    bool did_work;
    do {
      did_work = false;
      for (auto osd: up_acting) {
        did_work |= dispatch_cluster_messages(osd);
      }
      rc |= did_work;
    } while (did_work);
    return rc;
  }

  // Dispatcher - dispatch events. Events are generated by MockPeeringListener in
  // response to some of the calls from PeeringState, this emulates the behavior
  // of PrimaryLogPg and PG
  // stalled - true if dispatching stalled events (see inject_event_stall)
  // num_events - if specified only send up to the specified number of events
  bool dispatch_events( int fromosd, bool stalled = false, int num_events = -1 )
  {
    bool did_work = false;
    auto &ls = stalled ? get_listener(fromosd)->stalled_events :
                         get_listener(fromosd)->events;
    for (auto it = ls.begin(); it != ls.end();) {
      auto evt = *it;
      it = ls.erase(it);
      dout(0) << __func__ << " event to osd." << fromosd << " " << evt->get_desc() << dendl;
      get_ps(fromosd)->handle_event(evt, get_ctx(fromosd));
      did_work = true;
      if (num_events > 0 && --num_events == 0) {
        return did_work;
      }
    }
    return did_work;
  }

  // Dispatch events to all OSDs until all queues empty
  // stalled - true if dispatching stalled events (see inject_event_stall)
  bool dispatch_all_events(bool stalled = false)
  {
    bool rc = false;
    bool did_work;
    do {
      did_work = false;
      for (auto osd: up_acting) {
        did_work |= dispatch_events(osd, stalled);
      }
      rc |= did_work;
    } while (did_work);
    return rc;
  }

  // Dispatch all types of queued work repeatedly until queues are empty
  bool dispatch_all()
  {
    bool rc = false;
    bool did_work;
    do {
      did_work = dispatch_all_peering_messages();
      did_work |= dispatch_all_cluster_messages();
      did_work |= dispatch_all_events();
      rc |= did_work;
    } while (did_work);
    return rc;
  }

  // ============================================================================
  // Factory to create PeeringState instance for a given osd and shard
  // ============================================================================

  // Helper to create a single PeeringState instance
  PeeringState *create_peering_state(int osd, int shard)
  {
    dout(0) << "= create_peering_state osd." << osd << "(" << shard <<") =" << dendl;
    const pg_pool_t pi = *osdmap->get_pg_pool(pool_id);
    pg_shard_t pg_whoami(osd, pi.is_erasure() ? shard_id_t(shard) : shard_id_t::NO_SHARD);
    PGPool pool(osdmap, pool_id, pi, osdmap->get_pool_name(pool_id));
    dpp[osd] = make_unique<DppHelper>(g_ceph_context, dout_subsys, this, osd, shard);
    spg_t spgid = spg_t(pg_t(0, pool_id), pg_whoami.shard);
    listeners[osd] = make_unique<MockPeeringListener>(osdmap, pool_id, get_dpp(osd), pg_whoami);
    get_listener(osd)->current_epoch = osdmap->get_epoch();
    unique_ptr<PeeringState> ps = make_unique<PeeringState>(
      g_ceph_context,
      pg_whoami,
      spgid,
      pool,
      osdmap,
      PG_FEATURE_CLASSIC_ALL,
      get_dpp(osd),
      get_listener(osd));
    listeners[osd]->ps = ps.get();
    ps->set_backend_predicates(
      get_is_readable_predicate(),
      get_is_recoverable_predicate());
    osd_peeringstate[osd] = std::move(ps);
    osd_peeringctx[osd] = make_unique<PeeringCtx>();
    return get_ps(osd);
  }

  // ============================================================================
  // Helpers to test PeeringState interfaces
  // ============================================================================

  // Helper - create peering state for all osds
  void test_create_peering_state(int toosd = -1, int shard = -1)
  {
    dout(0) << "= test_create_peering_state =" << dendl;
    for (auto osd : up_acting ) {
      if (toosd != -1 && toosd != osd) {
        continue;
      }
      create_peering_state(osd, (shard == -1) ? osd : shard);
    }
  }

  // Helper - init for all osds
  void test_init(int toosd = -1, bool dne = false)
  {
    dout(0) << "= test_init =" << dendl;
    pg_history_t history;
    history.same_interval_since = osdmap->get_epoch();
    history.epoch_pool_created = osdmap->get_epoch();
    history.last_epoch_clean = osdmap->get_epoch();
    if (!dne) {
      history.epoch_created = osdmap->get_epoch();
    }
    PastIntervals past_intervals;
    ObjectStore::Transaction t;

    for (auto osd : up_acting ) {
      if (toosd != -1 && toosd != osd) {
        continue;
      }
      get_ps(osd)->init(
        (osd == acting_primary) ? 0 /* role: primary */ : 1 /* role: replica */,
        up,
        up_primary,
        acting,
        acting_primary,
        history,
        past_intervals,
        t);
    }
  }

  // Helper - init from disk for all osds
  void test_init_from_disk(int toosd = -1)
  {
    dout(0) << "= test_init_from_disk =" << dendl;

    for (auto osd : up_acting ) {
      if (toosd != -1 && toosd != osd) {
        continue;
      }
      pg_info_t info;
      spg_t spgid = spg_t(pg_t(0, pool_id), shard_id_t(0));
      info.pgid = spgid;
      PastIntervals past_intervals;

      get_ps(osd)->init_from_disk_state(
        std::move(info),
        std::move(past_intervals),
        [](PGLog &log) { return 0; });
    }
  }

  // Helper - initialize event for all osds
  void test_event_initialize(int toosd = -1)
  {
    dout(0) << "= test_event_initialize =" << dendl;
    for (auto osd : up_acting ) {
      if (toosd != -1 && toosd != osd) {
        continue;
      }
      auto evt = std::make_shared<PGPeeringEvent>(
        osdmap->get_epoch(),
        osdmap->get_epoch(),
        PeeringState::Initialize());

      get_ps(osd)->handle_event(evt, get_ctx(osd));
    }
  }

  // Helper - advance map for all osds
  void test_event_advance_map(int toosd = -1)
  {
    dout(0) << "= test_event_advance_map =" << dendl;
    for (auto osd : up_acting ) {
      if (toosd != -1 && toosd != osd) {
        continue;
      }
      get_ps(osd)->advance_map(osdmap, osdmap, up, up_primary, acting, acting_primary, *(get_ctx(osd)));
    }
  }

  // Helper - activate map event for all osds
  void test_event_activate_map(int toosd = -1)
  {
    dout(0) << "= test_event_activate_map =" << dendl;
    for (auto osd : up_acting ) {
      if (toosd != -1 && toosd != osd) {
        continue;
      }
      get_ps(osd)->activate_map(*(get_ctx(osd)));
    }
  }

  // Helper - construct a shard_id_set from a vector of integers
  shard_id_set ss(vector<int> v) {
    shard_id_set s;
    for (auto e : v) {
      s.insert(shard_id_t(e));
    }
    return s;
  }
  const shard_id_set ss_all;

  // Helper - construct a pg_log_entry and call append_log for specified osds
  // By default create a full write and append the log entry to all the OSDs
  // written - if provided then create a partial write to the specified OSDs
  // osds - if provided then only append the log entry to the specified OSDs
  // update_only - if true then only update the log, do not complete it
  // do_trim - if true then trim earlier log entries (usefull for testing backfill)
  // returns eversion of the new log entry
  eversion_t test_append_log_entry(
    shard_id_set written = shard_id_set(),
    shard_id_set osds = shard_id_set(),
    bool update_only = false,
    bool do_trim = false)
  {
    if (osds.empty()) {
      int shard = 0;
      for (auto osd : acting) {
        if (osd != pg_pool_t::pg_CRUSH_ITEM_NONE) {
          osds.insert(shard_id_t(shard));
        }
        shard++;
      }
    }
    dout(0) << "= test_append_log_entry written=" << written << " osds=" << osds << " =" << dendl;
    if (get_listener(acting_primary)->first_write_in_interval) {
      // Fix for issue 73891
      if (!written.empty()) {
        dout(0) << "First write in new interval is promoted to a full write" << dendl;
        written.clear();
        get_listener(acting_primary)->first_write_in_interval = false;
      }
    }
    object_t oid("foo");
    hobject_t soid(oid, oid.name, 0, 1234, pool_id, "");
    ++reqid.tid;
    eversion_t v, pv;
    version_t uv;
    v.epoch = osdmap->get_epoch();
    v.version = reqid.tid;
    if (reqid.tid > 1) {
      pv.epoch = v.epoch;
      pv.version = reqid.tid - 1;
    } else {
      pv.epoch = 0;
      pv.version = 0;
    }
    uv = v.version;
    pg_log_entry_t entry(pg_log_entry_t::MODIFY, soid, v, pv, uv, reqid, utime_t(), 0);
    entry.written_shards = written;

    for (auto osd : osds) {
      // Skips OSDs that are not written
      if (!written.empty() && !written.contains(osd)) {
        continue;
      }
      std::vector<pg_log_entry_t> entries;
      ObjectStore::Transaction t;
      entries.push_back(entry);

      eversion_t trim;
      if (do_trim) {
        trim = pv;
      }
      get_ps(int(osd))->append_log(
        std::move(entries),
        trim, // trim_to
        trim, // roll_forward_to
        trim, // pg_committed_to
        t,
        true,
        false);
      if (!update_only) {
        std::vector<pg_log_entry_t> empty;
        get_ps(int(osd))->append_log(
          std::move(empty),
          trim, // trim_to
          v, // roll_forward_to
          v, // pg_committed_to
          t,
          true,
          false);
      }
    }
    return v;
  }

  // Helper - call begin_peer_recover
  void test_begin_peer_recover(int osd, int shard)
  {
    dout(0) << "= test_begin_peer_recover " << osd << "(" << shard << ") =" << dendl;
    object_t oid("foo");
    hobject_t soid(oid, oid.name, 0, 1234, pool_id, "");
    get_ps(acting_primary)->begin_peer_recover(pg_shard_t(osd, shard_id_t(shard)), soid);
  }

  // Helper - call on_peer_recover
  void test_on_peer_recover(int osd, int shard, eversion_t v)
  {
    dout(0) << "= test_on_peer_recover " << osd << "(" << shard << ")" << v << " =" << dendl;
    object_t oid("foo");
    hobject_t soid(oid, oid.name, 0, 1234, pool_id, "");
    ObjectStore::Transaction t;
    get_ps(acting_primary)->on_peer_recover(pg_shard_t(osd, shard_id_t(shard)), soid, v);
  }

  // Helper - call recover got to indicate an object has been recovered
  void test_recover_got(int osd, eversion_t v)
  {
    dout(0) << "= test_recover_got " << osd << " " << v << " =" << dendl;
    object_t oid("foo");
    hobject_t soid(oid, oid.name, 0, 1234, pool_id, "");
    ObjectStore::Transaction t;
    get_ps(osd)->recover_got(soid, v, false, t);
  }

  // Helper - call object_recovered
  void test_object_recovered()
  {
    dout(0) << "= test_object_recovered =" << dendl;
    object_t oid("foo");
    hobject_t soid(oid, oid.name, 0, 1234, pool_id, "");
    object_stat_sum_t stat_diff;
    get_ps(acting_primary)->object_recovered(soid, stat_diff);
  }

  void test_prepare_backfill_for_missing(int osd, int shard, eversion_t version)
  {
    dout(0) << "= test_prepare_backfill_for_missing " << osd << " " << shard << " " << version << " =" << dendl;
    object_t oid("foo");
    hobject_t soid(oid, oid.name, 0, 1234, pool_id, "");
    get_ps(acting_primary)->prepare_backfill_for_missing(soid, version, {pg_shard_t(osd,shard_id_t(shard))});
  }

  void test_update_peer_last_backfill(int osd, int shard, hobject_t last_backfill)
  {
    dout(0) << "= test_update_peer_last_backfill =" << dendl;
    get_ps(acting_primary)->update_peer_last_backfill(pg_shard_t(osd, shard_id_t(shard)), last_backfill);
  }

  void test_update_backfill_progress(int osd, hobject_t last_backfill)
  {
    dout(0) << "= test_update_backfill_progress =" << dendl;
    pg_stat_t stats;
    ObjectStore::Transaction t;
    get_ps(osd)->update_backfill_progress(last_backfill, stats, false, t);
  }

  // Helper - all replicas recovered
  void test_event_all_replicas_recovered()
  {
    dout(0) << "= test_event_all_replicas_recovered =" << dendl;
    auto evt = std::make_shared<PGPeeringEvent>(
      osdmap->get_epoch(),
      osdmap->get_epoch(),
      PeeringState::AllReplicasRecovered());

    get_ps(acting_primary)->handle_event(evt, get_ctx(acting_primary));
  }

  // Helper - recovery done
  void test_event_recovery_done(int osd)
  {
    dout(0) << "= test_event_recovery_done =" << dendl;
    auto evt = std::make_shared<PGPeeringEvent>(
      osdmap->get_epoch(),
      osdmap->get_epoch(),
      RecoveryDone());

    get_ps(osd)->handle_event(evt, get_ctx(osd));
  }

  // Helper - backfilled
  void test_event_backfilled()
  {
    dout(0) << "= test_event_backfilled =" << dendl;
    auto evt = std::make_shared<PGPeeringEvent>(
      osdmap->get_epoch(),
      osdmap->get_epoch(),
      PeeringState::Backfilled());

    get_ps(acting_primary)->handle_event(evt, get_ctx(acting_primary));
  }

#if POOL_MIGRATION
// Helper - migration done
  void test_event_migration_done()
  {
    dout(0) << "= test_event_migration_done =" << dendl;
    auto evt = std::make_shared<PGPeeringEvent>(
      osdmap->get_epoch(),
      osdmap->get_epoch(),
      PeeringState::PoolMigrationDone());

    get_ps(acting_primary)->handle_event(evt, get_ctx(acting_primary));
  }
#endif

  // Helper - run peering cycle with new epochs if required for upthru or pgtemp
  void test_peering()
  {
    // Full peering cycle
    while (true) {
      test_event_advance_map();
      test_event_activate_map();
      dispatch_all();
      bool did_work = new_epoch(false); // New epoch if required for upthru or pgtemp
      if (!did_work) {
        break;
      }
    }
  }

  // ============================================================================
  // Verification Helper Functions
  // ============================================================================

  // Helper - verify primary OSD is in active+clean state
  void verify_primary_active_clean(int osd)
  {
    dout(0) << "Verifying primary osd " << osd << " is active+clean" << dendl;
    auto ps = get_ps(osd);
    EXPECT_TRUE(get_listener(osd)->clean_called);
    EXPECT_TRUE(get_listener(osd)->activate_complete_called);
    EXPECT_TRUE(ps->is_clean());
    EXPECT_FALSE(ps->is_recovering());
    EXPECT_FALSE(ps->is_backfilling());
#if POOL_MIGRATION
    EXPECT_FALSE(ps->is_migrating());
#endif
  }

  // Helper - verify primary OSD is in active+recovering state
  void verify_primary_active_recovering(int osd)
  {
    dout(0) << "Verifying primary osd " << osd << " is active+recovering" << dendl;
    auto ps = get_ps(osd);
    EXPECT_TRUE(get_listener(osd)->activate_complete_called);
    EXPECT_FALSE(ps->is_clean());
    EXPECT_TRUE(ps->is_recovering());
    EXPECT_FALSE(ps->is_backfilling());
#if POOL_MIGRATION
    EXPECT_FALSE(ps->is_migrating());
#endif
  }

  // Helper - verify primary OSD is in active+backfilling state
  void verify_primary_active_backfilling(int osd)
  {
    dout(0) << "Verifying primary osd " << osd << " is active+backfilling" << dendl;
    auto ps = get_ps(osd);
    EXPECT_TRUE(get_listener(osd)->activate_complete_called);
    EXPECT_FALSE(ps->is_clean());
    EXPECT_FALSE(ps->is_recovering());
    EXPECT_TRUE(ps->is_backfilling());
#if POOL_MIGRATION
    EXPECT_FALSE(ps->is_migrating());
#endif
  }

  // Helper - verify primary OSD is in active+migrating state
  void verify_primary_active_migrating(int osd)
  {
    dout(0) << "Verifying primary osd " << osd << " is active+migrating" << dendl;
    auto ps = get_ps(osd);
    EXPECT_TRUE(get_listener(osd)->activate_complete_called);
    EXPECT_FALSE(ps->is_clean());
    EXPECT_FALSE(ps->is_recovering());
    EXPECT_FALSE(ps->is_backfilling());
#if POOL_MIGRATION
    EXPECT_TRUE(ps->is_migrating());
#endif
  }

  // Helper - verify replica OSD is activated
  void verify_replica_activated(int osd)
  {
    dout(0) << "Verifying replica osd " << osd << " is activated" << dendl;
    EXPECT_TRUE(get_listener(osd)->activate_committed_called);
  }

  // Helper - verify OSD is active and peered
  void verify_active_and_peered(int osd)
  {
    auto ps = get_ps(osd);
    EXPECT_TRUE(ps->is_active());
    EXPECT_TRUE(ps->is_peered());
  }

  // Helper - verify log state for an OSD
  void verify_log_state(int osd, const eversion_t& expected_update,
                        const eversion_t& expected_complete,
                        const eversion_t& expected_head,
                        const eversion_t& expected_tail)
  {
    auto ps = get_ps(osd);
    EXPECT_EQ(ps->get_info().last_update, expected_update);
    EXPECT_EQ(ps->get_info().last_complete, expected_complete);
    EXPECT_EQ(ps->get_pg_log().get_head(), expected_head);
    EXPECT_EQ(ps->get_pg_log().get_tail(), expected_tail);
  }

  // Helper - verify logs
  // identical - if identical is true, all logs should be identical
  //            if identical is false, all logs should be equivalent
  //            with differences only because of partial writes
  void verify_logs(bool identical = false)
  {
    auto logp = get_ps(acting_primary)->get_pg_log();
    int shard = 0;
    for (auto osd: acting) {
      if (osd == acting_primary || osd == pg_pool_t::pg_CRUSH_ITEM_NONE) {
        ++shard;
        continue;
      }
      dout(0) << "Verifying log for osd " << osd << dendl;
      auto log = get_ps(osd)->get_pg_log();
      // Head and tail should match
      EXPECT_EQ(log.get_head(), logp.get_head());
      EXPECT_EQ(log.get_tail(), logp.get_tail());
      auto pi = logp.get_log().log.begin();
      auto pe = logp.get_log().log.end();
      auto i = log.get_log().log.begin();
      auto e = log.get_log().log.end();
      while (pi != pe && i != e) {
        if (pi->version < i->version && !pi->is_written_shard(shard_id_t(shard))) {
          // Primary may have partial write log entries that this
          // shard does not have because it was not written to
          ++pi;
          continue;
        }
        // Log entries should match (not a perfect check, but good enough for peering)
        EXPECT_EQ(pi->version, i->version);
        EXPECT_EQ(pi->soid, i->soid);
        EXPECT_EQ(pi->op, i->op);
        ++pi;
        ++i;
      }
      while (pi != pe && !pi->is_written_shard(shard_id_t(shard))) {
        // Primary may have partial write log entries that this
        // shard does not have because it was not written to
        ++pi;
      }
      // No extra entries in either log
      EXPECT_EQ(pi, pe);
      EXPECT_EQ(i, e);
      ++shard;
    }
  }

  // Helper - verify that there are no missing or unfound objects
  void verify_no_missing_or_unfound(int osd, int shard, bool check_missing_loc = true)
  {
    auto ps = get_ps(osd);
    EXPECT_FALSE(ps->have_missing());
    EXPECT_FALSE(ps->have_unfound());
    if (check_missing_loc) {
      EXPECT_EQ(ps->get_missing_loc().get_missing_locs().size(), 0);
    }
    // Primary shard peer missing state should agree with the shard
    if (osd != up_primary) {
      pg_shard_t pg_whoami = get_listener(osd)->pg_whoami;
      ps = get_ps(up_primary);
      EXPECT_EQ(ps->get_peer_missing(pg_whoami).num_missing(), 0);
    }
  }

  // Helper - verify that there are missing objects but not unfound objects
  void verify_missing(int osd, int shard)
  {
    auto ps = get_ps(osd);
    EXPECT_TRUE(ps->have_missing());
    EXPECT_FALSE(ps->have_unfound());
    // Primary shard peer missing state should agree with the shard
    if (osd != up_primary) {
      pg_shard_t pg_whoami = get_listener(osd)->pg_whoami;
      auto pps = get_ps(up_primary);
      EXPECT_EQ(pps->get_peer_missing(pg_whoami).num_missing(),
                ps->get_num_missing());
    }
  }

  // Helper - verify all OSDs in active+clean state with log checks
  void verify_all_active_clean(const eversion_t& expected_update = eversion_t(),
                                const eversion_t& expected_tail = eversion_t())
  {
    int shard = 0;
    for (auto osd: up) {
      dout(0) << "Verifying state of osd " << osd << dendl;
      if (osd == up_primary) {
        verify_primary_active_clean(osd);
      } else {
        verify_replica_activated(osd);
      }
      verify_no_missing_or_unfound(osd, shard);
      verify_active_and_peered(osd);
      verify_log_state(osd, expected_update, expected_update, expected_update, expected_tail);
      ++shard;
    }
    verify_logs();
  }

  // Helper - verify all OSDs in active+recovering state with log checks
  void verify_all_active_recovering(const eversion_t& expected_update,
                                     const eversion_t& expected_complete,
                                     const eversion_t& expected_complete_recovering,
                                     int recovering_osd,
                                     const eversion_t& expected_tail = eversion_t()) {
    int shard = 0;
    for (auto osd: up) {
      dout(0) << "Verifying state of osd " << osd << dendl;
      if (osd == up_primary) {
        verify_primary_active_recovering(osd);
      } else {
        verify_replica_activated(osd);
      }
      if (osd != recovering_osd) {
        verify_no_missing_or_unfound(osd, shard, osd != up_primary);
      } else {
        verify_missing(osd, shard);
      }
      verify_active_and_peered(osd);
      verify_log_state(
        osd,
        expected_update,
        (osd == recovering_osd) ? expected_complete_recovering : expected_complete,
        expected_update,
        expected_tail);
      ++shard;
    }
    verify_logs();
  }

  // Helper - verify all OSDs in active+backfilling state with log checks
  void verify_all_active_backfilling(const eversion_t& expected_update,
                                      const eversion_t& expected_tail = eversion_t())
  {
    int shard = 0;
    for (auto osd: up) {
      dout(0) << "Verifying state of osd " << osd << dendl;
      if (osd == up_primary) {
        verify_primary_active_backfilling(osd);
      } else {
        verify_replica_activated(osd);
      }
      verify_no_missing_or_unfound(osd, shard);
      verify_active_and_peered(osd);
      verify_log_state(osd, expected_update, expected_update, expected_update, expected_tail);
      ++shard;
    }
    verify_logs();
  }

#if POOL_MIGRATION
  // Helper - verify all OSDs in active+migrating state with log checks
  void verify_all_active_migrating(const eversion_t& expected_update = eversion_t(),
                                const eversion_t& expected_tail = eversion_t())
  {
    int shard = 0;
    for (auto osd: up) {
      dout(0) << "Verifying state of osd " << osd << dendl;
      if (osd == up_primary) {
        verify_primary_active_migrating(osd);
      } else {
        verify_replica_activated(osd);
      }
      verify_no_missing_or_unfound(osd, shard);
      verify_active_and_peered(osd);
      verify_log_state(osd, expected_update, expected_update, expected_update, expected_tail);
      ++shard;
    }
    verify_logs();
  }
#endif

  // Helper - verify PWLC state
  void verify_pwlc(epoch_t e,  std::map<shard_id_t,std::pair<eversion_t, eversion_t>> pwlc, int toosd = -1)
  {
    for (auto osd: acting) {
      if (toosd != -1 && toosd != osd) {
        continue;
      }
      dout(0) << "Verifying PWLC for osd " << osd << dendl;
      auto ps = get_ps(osd);
      auto info = ps->get_info();
      EXPECT_EQ(info.partial_writes_last_complete_epoch, e);
      EXPECT_EQ(info.partial_writes_last_complete, pwlc);
    }
  }

  // ============================================================================
  // GTest - Setup and Teardown
  // ============================================================================

  // GTest SetUp function - called before each test
  void SetUp() override
  {
    g_ceph_context->_log->set_max_new(0);
    dout(0) << "SetUp" << dendl;

    // Create a basic OSDMap
    osdmap = setup_osdmap(10);
    // By default tests use a fast EC 2+2 pool
    create_ec_pool();
  }

  // GTest TearDown function - called after each test
  void TearDown() override
  {
    osd_peeringstate.clear();
    // Clear any undispatched messages in PeeringCtx to prevent leaks
    for (auto& [osd, ctx] : osd_peeringctx) {
      ctx->message_map.clear();
    }
    osd_peeringctx.clear();
    // Clear any undispatched messages and events to prevent leaks
    for (auto& [osd, listener] : listeners) {
      listener->messages.clear();
      listener->events.clear();
      listener->stalled_events.clear();
    }
    listeners.clear();
    dpp.clear();
  }
};

// ============================================================================
// Test Stubs - Basic Initialization
// ============================================================================

TEST_F(PeeringStateTest, Construction) {
  dout(0) << "== Construction ==" << dendl;
  // Test basic construction of PeeringState
  test_create_peering_state(acting[0]);
  // Verify
  ASSERT_NE(get_ps(acting[0]), nullptr);
}

TEST_F(PeeringStateTest, InitFresh) {
  dout(0) << "== InitFresh ==" << dendl;
  // Test initialization of a fresh PG
  test_create_peering_state(acting[0]);
  test_init(acting[0]);
  // Verify
  EXPECT_EQ(get_ps(acting[0])->get_role(), 0);
  EXPECT_TRUE(get_ps(acting[0])->is_primary());
  EXPECT_TRUE(get_listener(acting[0])->new_interval_called);
}

TEST_F(PeeringStateTest, InitFromDisk) {
  dout(0) << "== InitFromDisk ==" << dendl;
  // Test initialization from disk state
  test_create_peering_state(acting[0]);
  test_init_from_disk(acting[0]);
  // Verify
  EXPECT_EQ(get_ps(acting[0])->get_info().pgid,
            spg_t(pg_t(0, pool_id), shard_id_t(0)));
}

// ============================================================================
// Test Stubs - State Machine Events
// ============================================================================

TEST_F(PeeringStateTest, HandleInitialize) {
  dout(0) << "== HandleInitialize ==" << dendl;
  // Test handling Initialize event
  test_create_peering_state(acting[0]);
  test_init(acting[0]);
  test_event_initialize(acting[0]);
  // Verify state transitions occurred
  EXPECT_TRUE(get_listener(acting[0])->new_interval_called);
}


TEST_F(PeeringStateTest, HandleAdvMap) {
  dout(0) << "== HandleAdvMap ==" << dendl;
  // Test handling ActMap event
  test_create_peering_state(acting[0]);
  test_init(acting[0]);
  test_event_initialize(acting[0]);
  test_event_advance_map(acting[0]);
}

TEST_F(PeeringStateTest, HandleActMap) {
  dout(0) << "== HandleActMap ==" << dendl;
  // Test handling ActMap event
  test_create_peering_state(acting[0]);
  test_init(acting[0]);
  test_event_initialize(acting[0]);
  test_event_advance_map(acting[0]);
  test_event_activate_map(acting[0]);
}

// ============================================================================
// Test Stubs - Peering Operations
// ============================================================================

TEST_F(PeeringStateTest, ChooseActing) {
  // Test choosing acting set
  test_create_peering_state(acting[0]);
  test_init(acting[0]);

  // Test that acting set is properly configured
  EXPECT_EQ(get_ps(acting[0])->get_acting().size(), acting.size());
  EXPECT_EQ(get_ps(acting[0])->get_acting_primary(), acting_primary);
}

// ============================================================================
// Test Stubs - Log Operations
// ============================================================================

TEST_F(PeeringStateTest, PGLogAccess) {
  // Test accessing PG log
  auto ps = create_peering_state(0, 0);

  const PGLog& log = ps->get_pg_log();
  EXPECT_EQ(log.get_head(), eversion_t());
}

TEST_F(PeeringStateTest, AppendLog) {
  // Test appending to PG log
  auto ps = create_peering_state(0, 0);

  std::vector<pg_log_entry_t> entries;
  ObjectStore::Transaction t;

  // Create a test log entry
  pg_log_entry_t entry;
  entry.version = eversion_t(1, 1);
  entry.op = pg_log_entry_t::MODIFY;
  entries.push_back(entry);

  ps->append_log(
    std::move(entries),
    eversion_t(),
    eversion_t(),
    eversion_t(),
    t,
    false,
    false);
}

TEST_F(PeeringStateTest, TrimLog) {
  // Test log trimming
  auto ps = create_peering_state(0, 0);

  ps->update_trim_to();
  eversion_t trim_to = ps->get_pg_trim_to();

  // Fresh PG should have no trim boundary
  EXPECT_EQ(trim_to, eversion_t());
}

// ============================================================================
// Test Stubs - Missing Objects
// ============================================================================

TEST_F(PeeringStateTest, MissingLocTracking) {
  // Test missing location tracking
  auto ps = create_peering_state(0, 0);

  const MissingLoc& missing_loc = ps->get_missing_loc();
  EXPECT_FALSE(missing_loc.have_unfound());
}

TEST_F(PeeringStateTest, RecoverGot) {
  // Test marking object as recovered
  auto ps = create_peering_state(0, 0);

  hobject_t oid;
  oid.pool = 1;
  oid.oid = "test_object";
  ObjectStore::Transaction t;
  ps->recover_got(oid, eversion_t(1, 1), false, t);
}

// ============================================================================
// Test Stubs - State Queries
// ============================================================================

TEST_F(PeeringStateTest, StateQueries) {
  // Test various state query methods
  auto ps = create_peering_state(0, 0);

  EXPECT_FALSE(ps->is_active());
  EXPECT_FALSE(ps->is_clean());
  EXPECT_FALSE(ps->is_peered());
  EXPECT_FALSE(ps->is_recovering());
  EXPECT_FALSE(ps->is_backfilling());
}

TEST_F(PeeringStateTest, RoleQueries) {
  // Test role query methods
  auto ps = create_peering_state(0, 0);

  std::vector<int> up = {0, 1, 2};
  std::vector<int> acting = {0, 1, 2};
  pg_history_t history;
  PastIntervals past_intervals;
  ObjectStore::Transaction t;

  ps->init(0, up, 0, acting, 0, history, past_intervals, t);

  EXPECT_TRUE(ps->is_primary());
  EXPECT_FALSE(ps->is_nonprimary());
  EXPECT_EQ(ps->get_role(), 0);
}

TEST_F(PeeringStateTest, ActingSetQueries) {
  // Test acting set queries
  auto ps = create_peering_state(0, 0);

  std::vector<int> up = {0, 1, 2};
  std::vector<int> acting = {0, 1, 2};
  pg_history_t history;
  PastIntervals past_intervals;
  ObjectStore::Transaction t;

  ps->init(0, up, 0, acting, 0, history, past_intervals, t);

  EXPECT_EQ(ps->get_acting().size(), 3u);
  EXPECT_EQ(ps->get_up().size(), 3u);
  EXPECT_TRUE(ps->is_acting(pg_shard_t(0, shard_id_t(0))));
}

// ============================================================================
// Test Stubs - OSDMap Operations
// ============================================================================

TEST_F(PeeringStateTest, OSDMapAccess) {
  // Test OSDMap access
  auto ps = create_peering_state(0, 0);

  const OSDMapRef& map = ps->get_osdmap();
  EXPECT_NE(map, nullptr);
  EXPECT_GT(map->get_epoch(), 0u);
}

TEST_F(PeeringStateTest, PoolInfo) {
  // Test pool information access
  auto ps = create_peering_state(0, 0);

  const PGPool& pool = ps->get_pgpool();
  EXPECT_EQ(pool.id, 1);
}

// ============================================================================
// Test Stubs - Feature Tracking
// ============================================================================

TEST_F(PeeringStateTest, FeatureTracking) {
  // Test feature vector tracking
  auto ps = create_peering_state(0, 0);

  uint64_t features = ps->get_min_peer_features();
  EXPECT_GT(features, 0u);

  uint64_t acting_features = ps->get_min_acting_features();
  EXPECT_GT(acting_features, 0u);
}

// ============================================================================
// Test Stubs - Statistics
// ============================================================================

TEST_F(PeeringStateTest, Statistics) {
  // Test statistics access
  auto ps = create_peering_state(0, 0);

  const pg_info_t& info = ps->get_info();
  EXPECT_EQ(info.stats.stats.sum.num_objects, 0u);
}

TEST_F(PeeringStateTest, UpdateStats) {
  // Test statistics updates
  auto ps = create_peering_state(0, 0);

  ps->update_stats(
    [](pg_history_t &history, pg_stat_t &stats) {
      stats.stats.sum.num_objects = 10;
      return true;
    });

  EXPECT_EQ(ps->get_info().stats.stats.sum.num_objects, 10u);
}

// ============================================================================
// Test Stubs - Deletion
// ============================================================================

TEST_F(PeeringStateTest, DeletionState) {
  // Test deletion state
  auto ps = create_peering_state(0, 0);

  EXPECT_FALSE(ps->is_deleting());
  EXPECT_FALSE(ps->is_deleted());
}

// ============================================================================
// Test Stubs - Flush Operations
// ============================================================================

TEST_F(PeeringStateTest, FlushOperations) {
  // Test flush tracking
  auto ps = create_peering_state(0, 0);

  EXPECT_FALSE(ps->needs_flush());

  ObjectStore::Transaction t;
  ps->complete_flush();
}

// ============================================================================
// Test Stubs - History and Past Intervals
// ============================================================================

TEST_F(PeeringStateTest, HistoryUpdate) {
  // Test history updates
  test_create_peering_state(acting[0]);

  pg_history_t new_history;
  new_history.epoch_created = 1;
  new_history.last_epoch_clean = 1;

  get_ps(acting[0])->update_history(new_history);
}

TEST_F(PeeringStateTest, PastIntervals) {
  // Test past intervals access
  auto ps = create_peering_state(0, 0);

  const PastIntervals& past_intervals = ps->get_past_intervals();
  EXPECT_EQ(past_intervals.size(), 0u);
}

// ============================================================================
// Multi OSD Peering Tests
// ============================================================================

// Multi-OSD test of peering for a new (empty PG) all the way to active+clean
TEST_F(PeeringStateTest, SimplePeeringToActiveClean) {
  dout(0) << "== SimplePeeringToActiveClean ==" << dendl;
  // Full peering cycle
  test_create_peering_state();
  test_init();
  test_event_initialize();
  test_event_advance_map();
  test_event_activate_map();
  dispatch_all();
  new_epoch(); // for wait_upthru
  test_event_advance_map();
  test_event_activate_map();
  dispatch_all();
  // Verify that we got to active+clean
  verify_all_active_clean();
}

// Multi-OSD test of peering for PG with 1 full write log entry all the way to active+clean
TEST_F(PeeringStateTest, FullWritePeeringToActiveClean) {
  dout(0) << "== FullWritePeeringToActiveClean ==" << dendl;
  // Init
  test_create_peering_state();
  test_init();
  test_event_initialize();
  // Append 1 log entry to all shards
  eversion_t expected = test_append_log_entry();
  // Full peering cycle
  test_event_advance_map();
  test_event_activate_map();
  dispatch_all();
  new_epoch(); // for wait_upthru
  test_event_advance_map();
  test_event_activate_map();
  dispatch_all();
  // Verify that we got to active+clean and that log entry was kept
  verify_all_active_clean(expected, eversion_t());
}

// Multi-OSD test of peering for PG with 2 log entries that are incomplete all the way to active+clean
TEST_F(PeeringStateTest, IncompleteWritePeeringToActiveClean) {
  dout(0) << "== IncompleteWritePeeringToActiveClean ==" << dendl;
  // Init
  test_create_peering_state();
  test_init();
  test_event_initialize();
  // Append 2 log entries one to just shard 0,1 and one to just shard 0
  test_append_log_entry(ss_all, ss({0, 1}));
  test_append_log_entry(ss_all, ss({0}));
  // Full peering cycle
  test_event_advance_map();
  dispatch_all();
  test_event_activate_map();
  dispatch_all();
  new_epoch(); // for wait_upthru
  test_event_advance_map();
  dispatch_all();
  test_event_activate_map();
  dispatch_all();
  // Verify that we got to active+clean and that log entries got rolled backwards
  verify_all_active_clean(eversion_t(), eversion_t());
}

// Multi-OSD test of a replica pool peering for PG with 2 log entries that are incomplete
// all the way to active+recovering. Unlike EC pools the log entries get rolled forward
TEST_F(PeeringStateTest, RepIncompleteWritePeeringToActiveRecovering) {
  dout(0) << "== RepIncompleteWritePeeringToActiveRecovering ==" << dendl;
  // Init
  create_rep_pool();
  test_create_peering_state();
  test_init();
  test_event_initialize();
  // Append 2 log entries to shard 0,1
  eversion_t previous = test_append_log_entry(ss_all, ss({0, 1}));
  eversion_t expected = test_append_log_entry(ss_all, ss({0 ,1}));
  // Full peering cycle
  test_event_advance_map();
  dispatch_all();
  test_event_activate_map();
  dispatch_all();
  new_epoch(); // for wait_upthru
  test_event_advance_map();
  dispatch_all();
  test_event_activate_map();
  dispatch_all();
  // Verify that we got to active+recovering
  verify_all_active_recovering(expected, expected, previous, 2, eversion_t());
}

// Multi-OSD test of peering for PG with 1 partial write log entry all the way to active+clean
TEST_F(PeeringStateTest, PartialWritePeeringToActiveClean) {
  dout(0) << "== PartialWritePeeringToActiveClean ==" << dendl;
  // Init
  test_create_peering_state();
  test_init();
  test_event_initialize();
  // Append 1 log entry to a partial set of shards
  eversion_t expected = test_append_log_entry(ss({0, 2, 3}), ss({0, 2, 3}));
  // Verify that shard 1 has no PWLC and shards 0,2,3 have PWLC saying shard 1 missed writes 0'0-2'1
  {
    std::map<shard_id_t,std::pair<eversion_t, eversion_t>> expected_pwlc;
    verify_pwlc(epoch_t(), expected_pwlc, acting[1]);
    expected_pwlc[shard_id_t(1)] = std::pair(eversion_t(), expected);
    epoch_t expected_pwlc_epoch = osdmap->get_epoch();
    verify_pwlc(expected_pwlc_epoch, expected_pwlc, acting[0]);
    verify_pwlc(expected_pwlc_epoch, expected_pwlc, acting[2]);
    verify_pwlc(expected_pwlc_epoch, expected_pwlc, acting[3]);
  }
  for (int osd : acting) {
    dout(0) << osd << " PWLC=" << get_ps(osd)->get_info().partial_writes_last_complete << dendl;
  }
  // Full peering cycle
  test_event_advance_map();
  test_event_activate_map();
  dispatch_all();
  new_epoch(); // for wait_upthru
  test_event_advance_map();
  test_event_activate_map();
  dispatch_all();
  // Verify that we got to active+clean and that log entry was kept
  verify_all_active_clean(expected, eversion_t());
  // Verify all shards have PWLC saying shard 1 missed writes 0'0-2'1
  {
    std::map<shard_id_t,std::pair<eversion_t, eversion_t>> expected_pwlc;
    expected_pwlc[shard_id_t(1)] = std::pair(eversion_t(), expected);
    epoch_t expected_pwlc_epoch = osdmap->get_epoch();
    verify_pwlc(expected_pwlc_epoch, expected_pwlc);
  }
}

// Multi-OSD test of peering for PG with 1 partial write (not complete) log entry all the way to active+clean
TEST_F(PeeringStateTest, PartialWriteNotCompletePeeringToActiveClean) {
  dout(0) << "== PartialWritePeeringToActiveClean ==" << dendl;
  // Init
  test_create_peering_state();
  test_init();
  test_event_initialize();
  // Append 1 log entry to a partial set of shards but do not advance last_complete
  eversion_t expected = test_append_log_entry(ss({0, 2, 3}), ss({0, 2, 3}), true);
  // Verify no shards have PWLC as the write has not been completed
  {
    std::map<shard_id_t,std::pair<eversion_t, eversion_t>> expected_pwlc;
    verify_pwlc(epoch_t(0), expected_pwlc);
  }
  // Full peering cycle - the definitive shard will be OSD 1 that didn't see the partial write
  // but proc_master_log will determine that all the other shards saw the partial write and
  // will roll it forward
  test_event_advance_map();
  test_event_activate_map();
  dispatch_all();
  new_epoch(); // for wait_upthru
  test_event_advance_map();
  test_event_activate_map();
  dispatch_all();
  // Verify that we got to active+clean and that log entry was kept
  verify_all_active_clean(expected, eversion_t());
  // Verify all shards have PWLC saying shard 1 missed writes 0'0-2'1
  {
    std::map<shard_id_t,std::pair<eversion_t, eversion_t>> expected_pwlc;
    expected_pwlc[shard_id_t(1)] = std::pair(eversion_t(), expected);
    epoch_t expected_pwlc_epoch = osdmap->get_epoch();
    verify_pwlc(expected_pwlc_epoch, expected_pwlc);
  }
}

// Multi-OSD test of peering for PG with 3 full write log entries all the way to active+clean
// then change acting set and run async recovery, recover the object and all the way to active+clean
TEST_F(PeeringStateTest, AsyncRecovery) {
  dout(0) << "== AsyncRecovery ==" << dendl;
  // Init
  test_create_peering_state();
  test_init();
  test_event_initialize();
  // Append 3 log entries to all shards - enough to force async recovery
  // later on because main reduced osd_async_recovery_min_cost to 2
  test_append_log_entry();
  eversion_t previous = test_append_log_entry();
  eversion_t expected = test_append_log_entry();
  // Full peering cycle
  test_peering();
  // Verify that we got to active+clean and that log entries were kept
  verify_all_active_clean(expected, eversion_t());
  // Swap out OSD 1 for OSD 9
  modify_up_acting(1, 9);
  test_create_peering_state(9, 1);
  test_init(9);
  test_event_initialize(9);
  // Full peering cycle
  test_peering();
  // Verify that we got to active+recovering
  verify_all_active_recovering(expected, expected, previous, 9, eversion_t());
  // Now recover the object
  test_begin_peer_recover(9, 1);
  test_on_peer_recover(9, 1, expected);
  test_recover_got(9, expected);
  test_object_recovered();
  test_event_all_replicas_recovered();
  EXPECT_TRUE(new_epoch(true));
  test_peering();
  // Verify that we got to active+clean and that log entries were kept
  verify_all_active_clean(expected, eversion_t());
}

// Multi-OSD test of peering for PG with 1 full write log entry all the way to active+clean
// then change acting set and run sync recovery, recover the object and all the way to active+clean
TEST_F(PeeringStateTest, SyncRecovery) {
  dout(0) << "== SyncRecovery ==" << dendl;
  // Init
  test_create_peering_state();
  test_init();
  test_event_initialize();
  // Append 1 log entries to all shards
  eversion_t previous;
  eversion_t expected = test_append_log_entry();
  // Full peering cycle
  test_peering();
  // Verify that we got to active+clean and that log entry was kept
  verify_all_active_clean(expected, eversion_t());
  // Swap out OSD 1 for OSD 9
  modify_up_acting(1, 9);
  test_create_peering_state(9, 1);
  test_init(9);
  test_event_initialize(9);
  // Full peering cycle
  test_peering();
  // Verify that we got to active+recovering
  verify_all_active_recovering(expected, expected, previous, 9, eversion_t());
  // Now recover the object
  test_begin_peer_recover(9, 1);
  test_on_peer_recover(9, 1, expected);
  test_recover_got(9, expected);
  test_object_recovered();
  test_event_all_replicas_recovered();
  // Verify that we got to active+clean and that log entry was kept
  verify_all_active_clean(expected, eversion_t());
}

// Multi-OSD test of peering for PG with 3 full write log entries all the way to active+clean
// then change acting set and run backfill, recover the object and all the way to active+clean
TEST_F(PeeringStateTest, Backfill) {
  dout(0) << "== Backfill ==" << dendl;
  // Init
  test_create_peering_state();
  test_init();
  test_event_initialize();
  // Append 3 log entries to all shards - trim the log so that
  // backfill occurs later on
  test_append_log_entry();
  eversion_t expected_tail = test_append_log_entry();
  eversion_t expected = test_append_log_entry(shard_id_set(), shard_id_set(), false, true);
  // Full peering cycle
  test_peering();
  // Verify that we got to active+clean and that log entries were kept
  verify_all_active_clean(expected, expected_tail);
  // Swap out OSD 1 for OSD 9
  modify_up_acting(1, 9);
  test_create_peering_state(9, 1);
  test_init(9);
  test_event_initialize(9);
  // Full peering cycle
  test_peering();
  // Verify that we got to active+backfill
  verify_all_active_backfilling(expected, expected_tail);
  // Backfill the object
  test_prepare_backfill_for_missing(9, 1, expected);
  test_begin_peer_recover(9, 1);
  test_on_peer_recover(9, 1, expected);
  test_recover_got(9, expected);
  test_object_recovered();
  test_update_peer_last_backfill(9, 1, hobject_t::get_max());
  test_update_backfill_progress(9, hobject_t::get_max()); // MOSDPGBackfill::OP_BACKFILL_PROGRESS
  // Signal backfill has completed
  test_event_recovery_done(9); // MOSDPGBackfill::OP_BACKFILL_FINISH
  test_event_backfilled();
  dispatch_all();
  EXPECT_TRUE(new_epoch(true));
  test_peering();
  // Verify that we got to active+clean and that log entries were kept
  verify_all_active_clean(expected, expected_tail);
}

#if POOL_MIGRATION
// Multi-OSD test of peering with pool migration all the way to active+clean
TEST_F(PeeringStateTest, PoolMigration) {
  dout(0) << "== PoolMigration ==" << dendl;
  // Configure pool migration to an EC pool
  migrate_to_ec_pool();
  // Init
  test_create_peering_state();
  test_init();
  test_event_initialize();
  // Full peering cycle
  test_peering();
  // Verify pool migration started
  verify_all_active_migrating(eversion_t(), eversion_t());
  // Signal migration complete
  test_event_migration_done();
  // Verify that we got to active+clean
  verify_all_active_clean(eversion_t(), eversion_t());
  EXPECT_TRUE(get_listener(acting[0])->pg_migrated_pool_sent);
}
#endif

#if POOL_MIGRATION
// Multi-OSD test of peering with pool migration too full
TEST_F(PeeringStateTest, PoolMigrationTooFull) {
  dout(0) << "== PoolMigration ==" << dendl;
  // Configure pool migration to an EC pool
  migrate_to_ec_pool();
  // Init
  test_create_peering_state();
  test_init();
  test_event_initialize();
  // Fail reservation on OSD 1
  get_listener(acting[1])->inject_fail_reserve_recovery_space = true;
  // Full peering cycle
  test_peering();
  // Verify that pool migration is stalled with too full
  EXPECT_EQ(get_listener(acting[0])->events_scheduled, 1);
  EXPECT_TRUE(get_ps(acting[0])->state_test(PG_STATE_MIGRATION_TOOFULL));
  EXPECT_EQ(get_listener(acting[0])->last_state_entered, "Started/Primary/Active/NotMigrating");
  EXPECT_EQ(get_listener(acting[1])->last_state_entered, "Started/ReplicaActive/RepNotRecovering");
  EXPECT_EQ(get_listener(acting[2])->last_state_entered, "Started/ReplicaActive/RepNotRecovering");
  EXPECT_EQ(get_listener(acting[3])->last_state_entered, "Started/ReplicaActive/RepNotRecovering");

  // Fail reservation on OSD 2 as well
  get_listener(acting[2])->inject_fail_reserve_recovery_space = true;
  dispatch_all_events(true); // run stalled schedule_after event to retry migration
  dispatch_all();
  // Verify that pool migration is stalled with too full
  EXPECT_EQ(get_listener(acting[0])->events_scheduled, 2);
  EXPECT_TRUE(get_ps(acting[0])->state_test(PG_STATE_MIGRATION_TOOFULL));
  EXPECT_EQ(get_listener(acting[0])->last_state_entered, "Started/Primary/Active/NotMigrating");
  EXPECT_EQ(get_listener(acting[1])->last_state_entered, "Started/ReplicaActive/RepNotRecovering");
  EXPECT_EQ(get_listener(acting[2])->last_state_entered, "Started/ReplicaActive/RepNotRecovering");
  EXPECT_EQ(get_listener(acting[3])->last_state_entered, "Started/ReplicaActive/RepNotRecovering");

  // Clear injects
  get_listener(acting[1])->inject_fail_reserve_recovery_space = false;
  get_listener(acting[2])->inject_fail_reserve_recovery_space = false;
  dispatch_all_events(true);  // run stalled schedule_after event to retry migration
  dispatch_all();
  // Verify pool migration started
  verify_all_active_migrating(eversion_t(), eversion_t());
  // Signal migration complete
  test_event_migration_done();
  // Verify that we got to active+clean
  verify_all_active_clean(eversion_t(), eversion_t());
  EXPECT_TRUE(get_listener(acting[0])->pg_migrated_pool_sent);
}
#endif

#if POOL_MIGRATION
// Multi-OSD test of peering with pool migration reservtion preempt
TEST_F(PeeringStateTest, PoolMigrationPrempt) {
  dout(0) << "== PoolMigration ==" << dendl;
  // Configure pool migration to an EC pool
  migrate_to_ec_pool();
  // Init
  test_create_peering_state();
  test_init();
  test_event_initialize();
  // Keep preempt reservation event on OSD 1
  get_listener(acting[1])->inject_keep_preempt = true;
  // Full peering cycle
  test_peering();
  // Verify pool migration started
  verify_all_active_migrating(eversion_t(), eversion_t());
  EXPECT_EQ(get_listener(acting[0])->io_reservations_requested, 1);
  EXPECT_EQ(get_listener(acting[1])->remote_recovery_reservations_requested, 1);
  EXPECT_EQ(get_listener(acting[2])->remote_recovery_reservations_requested, 1);
  EXPECT_EQ(get_listener(acting[3])->remote_recovery_reservations_requested, 1);
  EXPECT_EQ(get_listener(acting[0])->stalled_events.size(), 0);
  EXPECT_EQ(get_listener(acting[1])->stalled_events.size(), 1); // captured preempt event
  EXPECT_EQ(get_listener(acting[2])->stalled_events.size(), 0);
  EXPECT_EQ(get_listener(acting[3])->stalled_events.size(), 0);
  EXPECT_EQ(get_listener(acting[0])->last_state_entered, "Started/Primary/Active/MigratingSource");
  EXPECT_EQ(get_listener(acting[1])->last_state_entered, "Started/ReplicaActive/RepRecovering");
  EXPECT_EQ(get_listener(acting[2])->last_state_entered, "Started/ReplicaActive/RepRecovering");
  EXPECT_EQ(get_listener(acting[3])->last_state_entered, "Started/ReplicaActive/RepRecovering");
  dout(0) << "= PoolMigration prempt reservation osd 1 =" << dendl;
  dispatch_all_events(true); // preempt reservation on OSD 1
  EXPECT_EQ(get_listener(acting[0])->last_state_entered, "Started/Primary/Active/MigratingSource");
  EXPECT_EQ(get_listener(acting[1])->last_state_entered, "Started/ReplicaActive/RepNotRecovering");
  EXPECT_EQ(get_listener(acting[2])->last_state_entered, "Started/ReplicaActive/RepRecovering");
  EXPECT_EQ(get_listener(acting[3])->last_state_entered, "Started/ReplicaActive/RepRecovering");
  // Keep preempt reservation event on OSD 2 as well
  get_listener(acting[2])->inject_keep_preempt = true;
  dispatch_all();
  verify_all_active_migrating(eversion_t(), eversion_t());
  EXPECT_EQ(get_listener(acting[0])->io_reservations_requested, 2);
  EXPECT_EQ(get_listener(acting[1])->remote_recovery_reservations_requested, 2);
  EXPECT_EQ(get_listener(acting[2])->remote_recovery_reservations_requested, 2);
  EXPECT_EQ(get_listener(acting[3])->remote_recovery_reservations_requested, 2);
  EXPECT_EQ(get_listener(acting[0])->stalled_events.size(), 0);
  EXPECT_EQ(get_listener(acting[1])->stalled_events.size(), 1); // captured preempt event
  EXPECT_EQ(get_listener(acting[2])->stalled_events.size(), 1); // captured preempt event
  EXPECT_EQ(get_listener(acting[3])->stalled_events.size(), 0);
  EXPECT_EQ(get_listener(acting[0])->last_state_entered, "Started/Primary/Active/MigratingSource");
  EXPECT_EQ(get_listener(acting[1])->last_state_entered, "Started/ReplicaActive/RepRecovering");
  EXPECT_EQ(get_listener(acting[2])->last_state_entered, "Started/ReplicaActive/RepRecovering");
  EXPECT_EQ(get_listener(acting[3])->last_state_entered, "Started/ReplicaActive/RepRecovering");
  dout(0) << "= PoolMigration prempt reservation osd 1 and 2 =" << dendl;
  dispatch_all_events(true); // preempt reservation on OSD 1 and 2
  EXPECT_EQ(get_listener(acting[0])->last_state_entered, "Started/Primary/Active/MigratingSource");
  EXPECT_EQ(get_listener(acting[1])->last_state_entered, "Started/ReplicaActive/RepNotRecovering");
  EXPECT_EQ(get_listener(acting[2])->last_state_entered, "Started/ReplicaActive/RepNotRecovering");
  EXPECT_EQ(get_listener(acting[3])->last_state_entered, "Started/ReplicaActive/RepRecovering");
  // Keep preempt reservation event on OSD 0 (primary) as well
  get_listener(acting[0])->inject_keep_preempt = true;
  dispatch_all();
  verify_all_active_migrating(eversion_t(), eversion_t());
  EXPECT_EQ(get_listener(acting[0])->io_reservations_requested, 3);
  EXPECT_EQ(get_listener(acting[1])->remote_recovery_reservations_requested, 3);
  EXPECT_EQ(get_listener(acting[2])->remote_recovery_reservations_requested, 3);
  EXPECT_EQ(get_listener(acting[3])->remote_recovery_reservations_requested, 3);
  EXPECT_EQ(get_listener(acting[0])->stalled_events.size(), 1); // captured preempt event
  EXPECT_EQ(get_listener(acting[1])->stalled_events.size(), 1); // captured preempt event
  EXPECT_EQ(get_listener(acting[2])->stalled_events.size(), 1); // captured preempt event
  EXPECT_EQ(get_listener(acting[3])->stalled_events.size(), 0);
  EXPECT_EQ(get_listener(acting[0])->last_state_entered, "Started/Primary/Active/MigratingSource");
  EXPECT_EQ(get_listener(acting[1])->last_state_entered, "Started/ReplicaActive/RepRecovering");
  EXPECT_EQ(get_listener(acting[2])->last_state_entered, "Started/ReplicaActive/RepRecovering");
  EXPECT_EQ(get_listener(acting[3])->last_state_entered, "Started/ReplicaActive/RepRecovering");
  dout(0) << "= PoolMigration prempt reservation osd 0, 1 and 2 =" << dendl;
  // 0 Sends RELEASE first, then 1,2 send REVOKE
  dispatch_events(acting[0], true , 1);
  dispatch_events(acting[1], true , 1);
  dispatch_events(acting[2], true , 1);
  dispatch_all();
  dispatch_events(acting[0], true , 1);
  EXPECT_EQ(get_listener(acting[0])->last_state_entered, "Started/Primary/Active/WaitLocalPoolMigrationReserved");
  EXPECT_EQ(get_listener(acting[1])->last_state_entered, "Started/ReplicaActive/RepNotRecovering");
  EXPECT_EQ(get_listener(acting[2])->last_state_entered, "Started/ReplicaActive/RepNotRecovering");
  EXPECT_EQ(get_listener(acting[3])->last_state_entered, "Started/ReplicaActive/RepNotRecovering");
  dispatch_all();
  // Verify pool migration started
  verify_all_active_migrating(eversion_t(), eversion_t());
  EXPECT_EQ(get_listener(acting[0])->io_reservations_requested, 4);
  EXPECT_EQ(get_listener(acting[1])->remote_recovery_reservations_requested, 4);
  EXPECT_EQ(get_listener(acting[2])->remote_recovery_reservations_requested, 4);
  EXPECT_EQ(get_listener(acting[3])->remote_recovery_reservations_requested, 4);
  EXPECT_EQ(get_listener(acting[0])->stalled_events.size(), 1); // captured preempt event
  EXPECT_EQ(get_listener(acting[1])->stalled_events.size(), 1); // captured preempt event
  EXPECT_EQ(get_listener(acting[2])->stalled_events.size(), 1); // captured preempt event
  EXPECT_EQ(get_listener(acting[3])->stalled_events.size(), 0);
  EXPECT_EQ(get_listener(acting[0])->last_state_entered, "Started/Primary/Active/MigratingSource");
  EXPECT_EQ(get_listener(acting[1])->last_state_entered, "Started/ReplicaActive/RepRecovering");
  EXPECT_EQ(get_listener(acting[2])->last_state_entered, "Started/ReplicaActive/RepRecovering");
  EXPECT_EQ(get_listener(acting[3])->last_state_entered, "Started/ReplicaActive/RepRecovering");
  dout(0) << "= PoolMigration prempt reservation osd 0, 1 and 2 race hazard =" << dendl;
  // 1,2 send REVOKE first, then 0 sends RELEASE
  get_listener(acting[0])->inject_keep_preempt = false;
  get_listener(acting[1])->inject_keep_preempt = false;
  get_listener(acting[2])->inject_keep_preempt = false;
  // 1 and 2 prempt reservation and send messages to osd 0
  dispatch_events(acting[1], true);
  dispatch_events(acting[2], true);
  dispatch_cluster_messages(1);
  dispatch_cluster_messages(2);
  // 0 prempt reservation is slow
  dispatch_events(acting[0], true);
  dispatch_all();
  // Verify pool migration started
  verify_all_active_migrating(eversion_t(), eversion_t());
  // Signal migration complete
  test_event_migration_done();
  // Verify that we got to active+clean
  verify_all_active_clean(eversion_t(), eversion_t());
  EXPECT_TRUE(get_listener(acting[0])->pg_migrated_pool_sent);
}
#endif

// ============================================================================
// Tests for bug fixes
// ============================================================================

// Multi-OSD test of peering for fix for https://tracker.ceph.com/issues/71493
TEST_F(PeeringStateTest, Issue71493) {
  // Scenario: Backfill gets stopped by a remote reservastion being preempted
  // but this races with MOSDPGBackfill::OP_BACKFILL_FINISH event. The
  // probelmatic sequence is:
  //
  // Primary calls update_pee_last_backfill to set last_backfill to hobject_t::MAX
  // Primary sends a MOSDPGBackfill::OP_BACKFILL_FINISH message to the backfilling shard
  // Backfilling shard preempts the remote reservation and sends Revoked message
  // Backfilling shard processes the MOSDPGBackfill::OP_BACKFILL_FINISH message
  // Primary processes RemoteReservation revoked message from the backfilling OSD
  //   Sets BACKFILL_WAIT and suspends backfill (fix only does this if needs_backfill() returns true)
  //   Discards the event because needs_backfill() returns false
  // Primary sends and processes Backfilled event
  //   Leaves BACKFILL_WAIT set
  dout(0) << "== Issue71493 ==" << dendl;
  // Init
  test_create_peering_state();
  test_init();
  test_event_initialize();
  // Append 3 log entries to all shards - trim the log so that
  // backfill occurs later on
  test_append_log_entry();
  eversion_t expected_tail = test_append_log_entry();
  eversion_t expected = test_append_log_entry(shard_id_set(), shard_id_set(), false, true);
  // Full peering cycle
  test_peering();
  // Verify that we got to active+clean and that log entries were kept
  verify_all_active_clean(expected, expected_tail);
  // Swap out OSD 1 for OSD 9
  modify_up_acting(1, 9);
  test_create_peering_state(9, 1);
  test_init(9);
  test_event_initialize(9);
  // Keep preempt reservation event on OSD 9 for later
  get_listener(acting[1])->inject_keep_preempt = true;
  // Full peering cycle
  test_peering();
  // Verify that we got to active+backfill
  verify_all_active_backfilling(expected, expected_tail);
  // Backfill the object
  test_prepare_backfill_for_missing(9, 1, expected);
  test_begin_peer_recover(9, 1);
  test_on_peer_recover(9, 1, expected);
  test_recover_got(9, expected);
  test_object_recovered();
  test_update_peer_last_backfill(9, 1, hobject_t::get_max());
  test_update_backfill_progress(9, hobject_t::get_max()); // MOSDPGBackfill::OP_BACKFILL_PROGRESS
  dout(0) << "= revoking remote reservation =" << dendl;
  // Preempt remote reservation ahead of MOSDPGBackfill::OP_BACKFILL_FINISH
  dispatch_all_events(true);
  dispatch_all();
  // Signal backfill has completed
  test_event_recovery_done(9); // MOSDPGBackfill::OP_BACKFILL_FINISH
  dispatch_all();
  test_event_backfilled();
  dispatch_all();
  // Without the fix BACKFILL_WAIT is still set on the primary
#ifdef ISSUE_71493
  EXPECT_FALSE(get_ps(acting[0])->state_test(PG_STATE_BACKFILL_WAIT));
#endif
  // Even without the fix the new interval caused by the acting set changing
  // after the backfill has completed will clean up the PG state
  EXPECT_TRUE(new_epoch(true));
  test_peering();
  // Verify that we got to active+clean and that log entries were kept
  verify_all_active_clean(expected, expected_tail);
}

// Multi-OSD test of peering for fix for https://tracker.ceph.com/issues/73891
TEST_F(PeeringStateTest, Issue73891) {
  // Scenario: 3+2 EC pool [0,1,2,3,4]
  // Partial write A to OSDs 0,3,4 completes
  // Partial write B to OSDs 0,1,3,4 only updates 0, 1 before interuption
  // OSD 1 down, peering runs and rolls-backward the partial write B
  // Partial write C to OSDs 0,3,4 that is completed
  // OSD 1 up, peering runs
  //   Without fix OSD 3 doesn't roll back write B
  dout(0) << "== Issue73891 ==" << dendl;
  // Init
  create_ec_pool(3,2);
  test_create_peering_state();
  test_init();
  test_event_initialize();
  // Partial Write A to OSDs 0,3,4
  test_append_log_entry(ss({0, 3, 4}), ss({0, 3, 4}));
  // Partial Write B to OSDs 0,1,3,4 only updates 0, 1
  test_append_log_entry(ss({0, 1, 3, 4}), ss({0, 1}), true);
  // OSD 1 down, run peering
  osd_down(1, 1);
  test_peering();
  // Partial Write C to 0,3,4
  test_append_log_entry(ss({0, 3, 4}), ss({0, 3, 4}));
  // OSD 1 up, run peering
  osd_up(1, 1);
  test_peering();
  verify_logs(); // Without fix will fail
}

// Multi-OSD test of peering for fix for https://tracker.ceph.com/issues/74218
TEST_F(PeeringStateTest, Issue74218) {
  // Scenario:
  // Partial write to OSDs 0,2,3 that has not been completed
  // OSD 2 down, peering runs and rolls-forward the partial write
  // OSD 1 down, peering runs
  // Partial write to OSDs 0,[2],3 that is completed
  // OSD 2 up, peering runs
  //   OSD 2 rolls forward the 1st write, without the fix this overwrites pwlc with a stale version
  // OSD 1 up, peering runs - make sure OSD 2 replys to GetInfo last
  //   OSD 2 provides its stale copy of pwlc
  //   primary gives stale pwlc to osd 1 which asserts
  dout(0) << "== Issue74218 ==" << dendl;
  // Init
  test_create_peering_state();
  test_init();
  test_event_initialize();
  // Partial write to OSDs 0,2,3 but do not advance last_complete
  eversion_t expected1 = test_append_log_entry(ss({0, 2, 3}), ss({0, 2, 3}), true);
  // Verify no shards have PWLC as the write has not been completed
  {
    std::map<shard_id_t,std::pair<eversion_t, eversion_t>> expected_pwlc;
    verify_pwlc(epoch_t(0), expected_pwlc);
  }
  // OSD 2 down, run peering
  osd_down(2, 2);
  test_peering();
  // OSD 1 down, run peering
  osd_down(1, 1);
  test_peering();
  // Partial write to OSDs 0,[2],3
  eversion_t expected2 = test_append_log_entry(ss({0, 2, 3}), ss({0, 3}));
  // OSD 2 up, run peering
  osd_up(2, 2);
  test_peering();
  // OSD 1 up, run peering
  osd_up(1, 1);
  // Full peering cycle - but delay OSD 2's response to GetInfo
  test_event_advance_map();
  test_event_activate_map();
  dispatch_peering_messages(0); // Send query info to OSDs 1,2,3
  dispatch_peering_messages(1); // Send reply from OSD 1
  dispatch_peering_messages(3); // Send reply from OSD 3
  dispatch_peering_messages(2); // Send reply from OSD 2 - must be last
  // Run rest of peering
  dispatch_all();
  new_epoch(); // for wait_upthru
  test_event_advance_map();
  test_event_activate_map();
  dispatch_all();
  // Verify that we got to active+recovering
  // Prior to fix for issue 73891 the last write would not modify OSD 1 so it would
  // not need recovery. With this fix the write becomes a full-write and OSD 1
  // ends up with a missing object
  // verify_all_active_recovering(expected2, expected2, expected1, 2, eversion_t());
  verify_primary_active_recovering(acting[0]);
  verify_no_missing_or_unfound(acting[0], 0, false);
  verify_active_and_peered(acting[0]);
  verify_log_state(acting[0], expected2, expected2, expected2, eversion_t());
  verify_replica_activated(acting[1]);
  verify_missing(acting[1], 1);
  verify_log_state(acting[1], expected2, expected1, expected2, eversion_t());
  verify_replica_activated(acting[2]);
  verify_missing(acting[2], 2);
  verify_log_state(acting[2], expected2, expected1, expected2, eversion_t());
  verify_replica_activated(acting[3]);
  verify_no_missing_or_unfound(acting[3], 3, true);
  verify_log_state(acting[3], expected2, expected2, expected2, eversion_t());
  verify_logs();
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char **argv)
{
  std::map<std::string, std::string> defaults = {
    // our map is flat, so just try and split across OSDs, not hosts or whatever
    {"osd_crush_chooseleaf_type", "0"},
    // debug level 20 for OSD so we get full logs
    {"debug_osd", "20"},
    // reduce async recovery cost to 2 to make it easier to test async recovery
    {"osd_async_recovery_min_cost","2"},
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
