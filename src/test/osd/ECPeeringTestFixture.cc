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

IsPGRecoverablePredicate* ECPeeringTestFixture::get_is_recoverable_predicate(TestPG *test_pg) {
  return new MockECRecPred(k, m);
}

IsPGReadablePredicate* ECPeeringTestFixture::get_is_readable_predicate(TestPG *test_pg) {
  return new MockECReadPred(k, m);
}

ECPeeringTestFixture::ECPeeringTestFixture()
  : PeeringTestFixture(EC) {
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

std::string ECPeeringTestFixture::get_state_name(int shard) {
  return get_test_pg_by_shard(shard)->get_peering_state()->get_current_state();
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
    for (int shard = 0; shard < get_instance_count(); shard++) {
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
  for (int shard = 0; shard < get_instance_count(); shard++) {
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
  for (int shard = 0; shard < get_instance_count(); shard++) {
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
    for (int shard = 0; shard < get_instance_count(); shard++) {
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
    for (int shard = 0; shard < get_instance_count(); shard++) {
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

  // Step 1: Verify all objects are in the missing set and prepare recovery
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
      bool found_missing_peer = false;
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

          // Use the first matching peer's missing_item for recovery. (Not
          // "missing_items.size() == i": that compares against the *count*
          // pushed so far, which never changes within this inner loop, so it
          // was true on every matching peer and ended up using the *last*
          // match instead of the first.)
          if (!found_missing_peer) {
            missing_item = map_missing_item;
            found_missing_peer = true;
          }
        }
      }
      ASSERT_TRUE(found_missing_peer)
        << "Object " << obj_names[i] << " should be missing on at least one peer";

      // Get OBC for this object
      ObjectContextRef obc = get_object_context(hoid, false);
      ASSERT_TRUE(obc) << "Failed to load OBC from disk for " << obj_names[i];
      ASSERT_FALSE(obc->attr_cache.empty())
        << "OBC attr_cache must be populated for recovery of " << obj_names[i];
      obcs.push_back(obc);
    }

    missing_items.push_back(missing_item);
  }

  // Reset recovery callback tracker before starting recovery, so the
  // verification below (Step 5) reflects only this call's recoveries.
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
  get_primary_backend()->run_recovery_op(h, 10);  // priority = 10

  // Recovery callback/data verification (the old step 5) is done by the
  // caller (run_parallel_recovery()) after event_loop->run_until_idle() has
  // fully drained the queue, not here: read_object() internally calls
  // run_until_idle() itself, and calling that reentrantly while still inside
  // an event being processed by the outer run_until_idle() call (this
  // function runs inside a scheduled TRANSACTION event) can interleave with
  // still-in-flight recovery messages and duplicate an EC read tid.
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
  
  run_on_primary([this, obj_names, recover_primary, expected_data, primary_shard]() {
    do_run_parallel_recovery_impl(obj_names, recover_primary, expected_data, primary_shard);
  });

  // Verify recovery callbacks and the recovered data for each object, now
  // that the queue is fully drained. (Not done inside
  // do_run_parallel_recovery_impl(): read_object() calls run_until_idle()
  // itself, and that function runs inside a scheduled TRANSACTION event
  // being processed by the run_until_idle() call just above, so calling it
  // reentrantly there can interleave with still-in-flight recovery messages
  // and duplicate an EC read tid. See review item C4/E1 — this restores the
  // verification the old pre-branch run_parallel_recovery_and_verify_callbacks()
  // did, which was dropped when this function split off of it.)
  {
    auto* primary_listener = get_primary_listener();
    for (size_t i = 0; i < obj_names.size(); ++i) {
      hobject_t hoid = make_test_object(obj_names[i]);
      bool callback_found = false;
      if (recover_primary) {
        for (const auto& obj : primary_listener->recovery_tracker.on_local_recover_objects) {
          if (obj == hoid) {
            callback_found = true;
            break;
          }
        }
        EXPECT_TRUE(callback_found)
          << "on_local_recover should be called for " << obj_names[i];
      } else {
        for (const auto& [peer, obj] : primary_listener->recovery_tracker.on_peer_recover_objects) {
          if (obj == hoid) {
            callback_found = true;
            break;
          }
        }
        EXPECT_TRUE(callback_found)
          << "on_peer_recover should be called for " << obj_names[i];
      }

      if (!expected_data[i].empty()) {
        bufferlist read_bl;
        int r = read_object(obj_names[i], 0, expected_data[i].length(), read_bl,
                             expected_data[i].length());
        EXPECT_EQ(r, (int)expected_data[i].length())
          << "Should read full object " << obj_names[i];
        if (r == (int)expected_data[i].length()) {
          std::string read_data(read_bl.c_str(), read_bl.length());
          EXPECT_EQ(read_data, expected_data[i])
            << "Recovered data should match for " << obj_names[i];
        }
      }
    }

    EXPECT_EQ((int)obj_names.size(), primary_listener->recovery_tracker.on_global_recover_calls)
      << "on_global_recover should be called once for each object";
  }

  check_recovery_completion();
}
