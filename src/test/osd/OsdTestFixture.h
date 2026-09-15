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

#pragma once

#include <memory>
#include <map>
#include "osd/PeeringState.h"
#include "osd/ECExtentCache.h"
#include "osd/PGBackend.h"
#include "test/osd/MockPeeringListener.h"
#include "os/ObjectStore.h"
#include "test/osd/MockStore.h"

// Forward declarations
class PGBackend;
class MockPGBackendListener;

/**
 * TestPG - Per-PG test data structure
 *
 * This class does NOT inherit from anything. It's a simple container for
 * all the per-PG data needed by tests. This avoids the complexity and
 * boilerplate of trying to mock the real PG class.
 *
 * Contains:
 * - PeeringState (optional, for tests that use peering)
 * - PeeringCtx (optional, for tests that use peering)
 * - MockPeeringListener (optional, for tests that use peering)
 * - ShardDpp (optional, for tests that use peering)
 * - PGBackend (optional, for tests that use backends)
 * - MockPGBackendListener (optional, for tests that use backends)
 */
class TestPG {
public:
  // PG identity
  spg_t spgid;
  pg_shard_t pg_whoami;

  // Per-PG ObjectStore collection.  A collection is keyed by spg_t (pgid +
  // shard_id), so it belongs here rather than on OsdTestFixture which is
  // keyed only by OSD number.
  coll_t coll;
  ObjectStore::CollectionHandle ch;

  // Peering-related components (optional)
  // IMPORTANT: Member destruction order matters!
  // Members are destroyed in REVERSE order of declaration.
  // - dpp must be declared before peering_state (peering_state holds raw pointer to dpp)
  // - peering_listener must be declared before peering_state (peering_state's PGStateHistory
  //   holds a reference to peering_listener as an EpochSource)
  std::unique_ptr<DoutPrefixProvider> dpp;
  std::unique_ptr<MockPeeringListener> peering_listener;
  std::unique_ptr<PeeringState> peering_state;
  std::unique_ptr<PeeringCtx> peering_ctx;

  // Backend-related components (optional)
  std::unique_ptr<PGBackend> backend;
  std::unique_ptr<MockPGBackendListener> backend_listener;
  
  /// Persistent OBC storage - emulates PrimaryLogPG's object_contexts LRU.
  /// Keyed by hobject_t, values are shared_ptr so the same OBC is reused
  /// across sequential operations on the same object. This is critical for
  /// EC attr_cache continuity.
  std::map<hobject_t, ObjectContextRef> object_contexts;
  
  /// Track outstanding writes per object. When this reaches 0, we can safely
  /// clear attr_cache (as there are no in-flight writes that might have stale
  /// cached OI data).
  std::map<hobject_t, int> outstanding_writes;
  
  TestPG(const spg_t& spgid_, const pg_shard_t& pg_whoami_)
    : spgid(spgid_), pg_whoami(pg_whoami_) {}
  
  // Accessors with assertions for safety
  PeeringState* get_peering_state() const {
    ceph_assert(peering_state != nullptr);
    return peering_state.get();
  }
  
  PeeringCtx* get_peering_ctx() const {
    ceph_assert(peering_ctx != nullptr);
    return peering_ctx.get();
  }
  
  MockPeeringListener* get_peering_listener() const {
    ceph_assert(peering_listener != nullptr);
    return peering_listener.get();
  }
  
  PGBackend* get_backend() const {
    ceph_assert(backend != nullptr);
    return backend.get();
  }
  
  MockPGBackendListener* get_backend_listener() const {
    // The backend_listener is owned by peering_listener after create_peering_state()
    if (peering_listener && peering_listener->backend_listener) {
      return peering_listener->backend_listener.get();
    }
    // Fallback to direct backend_listener (used before peering is set up)
    if (backend_listener) {
      return backend_listener.get();
    }
    return nullptr;
  }
  
  DoutPrefixProvider* get_dpp() const {
    ceph_assert(dpp != nullptr);
    return dpp.get();
  }
  
  // Check if components are initialized
  bool has_peering_state() const { return peering_state != nullptr; }
  bool has_backend() const { return backend != nullptr; }
};

/**
 * OsdTestFixture - Per-OSD test data structure
 *
 * Contains all per-OSD data:
 * - Extent cache LRU (for EC pools)
 * - Map of spg_t -> TestPG (supports multiple PGs per OSD)
 * - Collection handles (for ObjectStore operations)
 */
class OsdTestFixture {
public:
  // OSD identity
  int osd_id;
  
  // Data directory for this OSD
  std::string data_dir;
  
  // ObjectStore for this OSD
  // Using shared_ptr to allow sharing the same store across multiple OSDs in replicated pools
  std::shared_ptr<MockStore> store;
  
  // Extent cache LRU (for EC pools only)
  std::unique_ptr<ECExtentCache::LRU> lru;
  
  // Map of PGs on this OSD
  std::map<spg_t, std::unique_ptr<TestPG>> pgs;
  
  explicit OsdTestFixture(int osd_id_) : osd_id(osd_id_) {}
  
  ~OsdTestFixture() {
    // Clean up in proper order: clear PGs first, then unmount store
    pgs.clear();
    if (store) {
      // Only unmount if this is the last reference to the store
      if (store.use_count() == 1) {
        store->umount();
      }
      store.reset();
    }
  }
  
  // Get or create a TestPG for a given spg_t
  TestPG* create_pg(const spg_t& spgid, const pg_shard_t& pg_whoami) {
    ceph_assert(!pgs.contains(spgid));

    auto pg = std::make_unique<TestPG>(spgid, pg_whoami);
    TestPG* pg_ptr = pg.get();
    pgs[spgid] = std::move(pg);
    return pg_ptr;
  }
  
  // Get an existing TestPG (asserts if not found)
  TestPG* get_pg(const spg_t& spgid) const {
    auto it = pgs.find(spgid);
    ceph_assert(it != pgs.end());
    return it->second.get();
  }
  
  // Check if a PG exists
  bool has_pg(const spg_t& spgid) const {
    return pgs.find(spgid) != pgs.end();
  }
};

// Made with Bob
