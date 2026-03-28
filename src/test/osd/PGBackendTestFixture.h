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

#include <filesystem>
#include <memory>
#include <random>
#include <sstream>
#include <iomanip>
#include <gtest/gtest.h>
#include "common/errno.h"
#include "test/osd/MockErasureCode.h"
#include "test/osd/MockPGBackendListener.h"
#include "test/osd/EventLoop.h"
#include "common/TrackedOp.h"
#include "os/memstore/MemStore.h"
#include "osd/ECSwitch.h"
#include "osd/ECExtentCache.h"
#include "osd/ReplicatedBackend.h"
#include "osd/PGBackend.h"
#include "osd/OSDMap.h"
#include "osd/osd_types.h"
#include "osd/PGTransaction.h"
#include "common/ceph_context.h"
#include "os/ObjectStore.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "test/osd/OSDMapTestHelpers.h"

// Unified test fixture for EC and Replicated backend tests with ObjectStore.
// Uses PoolType to branch between EC (ECSwitch) and Replicated (ReplicatedBackend).
class PGBackendTestFixture : public ::testing::Test {
public:
  enum PoolType {
    EC,
    REPLICATED
  };

protected:
  PoolType pool_type;

  // Pool flags to set on the EC pool (e.g., FLAG_EC_OVERWRITES, FLAG_EC_OPTIMIZATIONS).
  // Derived classes can set this before SetUp() to configure the pool flags.
  // setup_ec_pool() uses this value when creating the pool.
  // Default includes both OVERWRITES and OPTIMIZATIONS flags.
  uint64_t pool_flags = pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS;
  
  std::unique_ptr<MemStore> store;
  std::string data_dir;
  ObjectStore::CollectionHandle ch;
  coll_t coll;
  
  std::shared_ptr<OSDMap> osdmap;
  std::unique_ptr<OpTracker> op_tracker;
  std::unique_ptr<EventLoop> event_loop;
  std::map<int, std::function<bool(OpRequestRef)>> message_router;
  
  std::map<int, std::unique_ptr<MockPGBackendListener>> listeners;
  std::map<int, std::unique_ptr<PGBackend>> backends;
  std::map<int, coll_t> colls;
  std::map<int, ObjectStore::CollectionHandle> chs;
  
  /**
   * Optional listener factory callback.
   *
   * If set, setup_ec_pool() and setup_replicated_pool() will call this
   * factory instead of constructing MockPGBackendListener directly.
   * The factory receives the instance index and the parameters needed to
   * construct the listener, and must return a unique_ptr to the new
   * MockPGBackendListener.  The returned object is stored in listeners[i]
   * as usual, so ownership stays with the base class.
   *
   * Derived classes (e.g. ECPeeringTestFixture) can set this in their
   * constructor to gain direct access to the created listeners without
   * needing to steal ownership via release_listener().
   */
  std::function<std::unique_ptr<MockPGBackendListener>(
    int instance,
    std::shared_ptr<OSDMap> osdmap,
    int64_t pool_id,
    DoutPrefixProvider* dpp,
    pg_shard_t whoami)> listener_factory;

  ceph::ErasureCodeInterfaceRef ec_impl;
  std::map<int, std::unique_ptr<ECExtentCache::LRU>> lrus;
  int k = 4;  // data chunks
  int m = 2;  // coding chunks
  uint64_t stripe_unit = 4096;  // aka chunk_size
  std::string ec_plugin = "isa";
  std::string ec_technique = "reed_sol_van";
  
  int num_replicas = 3;
  int min_size = 2;
  
  int64_t pool_id = 0;
  pg_t pgid;
  spg_t spgid;
  
  class TestDpp : public NoDoutPrefix {
  public:
    TestDpp(CephContext *cct) : NoDoutPrefix(cct, ceph_subsys_osd) {}
    
    std::ostream& gen_prefix(std::ostream& out) const override {
      out << "PGBackendTest: ";
      return out;
    }
  };
  std::unique_ptr<TestDpp> dpp;

public:
  explicit PGBackendTestFixture(PoolType type = EC) : pool_type(type)
  {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dis;
    uint64_t random_num = dis(gen);
    
    std::ostringstream oss;
    oss << "memstore_test_" << std::hex << std::setfill('0') << std::setw(16) << random_num;
    data_dir = oss.str();
    
    ceph_assert(stripe_unit % 4096 == 0);
    ceph_assert(stripe_unit != 0);
  }
  
  ~PGBackendTestFixture() {
    // Ensure cleanup happens even if TearDown() wasn't called or failed
    cleanup_data_dir();
  }
  
  void SetUp() override {
    int r = ::mkdir(data_dir.c_str(), 0777);
    if (r < 0) {
      r = -errno;
      std::cerr << __func__ << ": unable to create " << data_dir << ": " << cpp_strerror(r) << std::endl;
    }
    ASSERT_EQ(0, r);
    
    // Create MemStore - contexts are stolen by MockPGBackendListener, so we don't need manual_finisher
    store.reset(new MemStore(g_ceph_context, data_dir));
    ASSERT_TRUE(store);
    ASSERT_EQ(0, store->mkfs());
    ASSERT_EQ(0, store->mount());
    
    g_conf().set_safe_to_start_threads();
    
    CephContext *cct = g_ceph_context;
    dpp = std::make_unique<TestDpp>(cct);
    event_loop = std::make_unique<EventLoop>(false);
    op_tracker = std::make_unique<OpTracker>(cct, false, 1);
    
    if (pool_type == EC) {
      setup_ec_pool();
    } else {
      setup_replicated_pool();
    }
  }
  
  void TearDown() override {
    // 0. Process any remaining events in the EventLoop.
    // If the test passed, orphaned events indicate a bug - warn and skip draining
    // so the test fails loudly.  If the test already failed, drain silently to
    // allow the rest of TearDown to complete without cascading errors.
    if (event_loop) {
      if (event_loop->has_events()) {
        if (!HasFailure()) {
          ADD_FAILURE() << "TearDown: " << event_loop->queued_event_count()
                        << " orphaned events remain after a passing test";
        }
        event_loop->run_until_idle(1000);
      }
    }
    
    // 1. Clean up all backend instances (polymorphic cleanup)
    //    Note: We skip calling on_change() during teardown as it may access
    //    invalid state. The backends will be destroyed anyway.
    backends.clear();
    
    // 2. Clean up EC-specific resources
    if (pool_type == EC) {
      lrus.clear();
      ec_impl.reset();
    }
    
    // 3. Clean up listeners
    listeners.clear();
    
    // 4. Reset op tracker (call on_shutdown first)
    if (op_tracker) {
      op_tracker->on_shutdown();
      op_tracker.reset();
    }
    
    // 5. Reset all collection handles
    chs.clear();
    colls.clear();
    
    if (ch) {
      ch.reset();
    }
    
    // 6. Unmount and destroy the store
    if (store) {
      store->umount();
      store.reset();
    }
    
    // 7. Clean up the test directory
    cleanup_data_dir();
  }
  
private:
  void setup_ec_pool();
  void setup_replicated_pool();
  void cleanup_data_dir();

public:
  const pg_pool_t& get_pool() const {
    const pg_pool_t* pool = OSDMapTestHelpers::get_pool(osdmap, pool_id);
    ceph_assert(pool != nullptr);
    return *pool;
  }
  
  int get_instance_count() const {
    return pool_type == EC ? (k + m) : num_replicas;
  }
  
  int get_data_chunk_count() const {
    return k;
  }
  
  int get_coding_chunk_count() const {
    return m;
  }
  
  uint64_t get_stripe_width() const {
    return stripe_unit * k;
  }
  
  int get_min_size() const {
    return min_size;
  }
  
  // Get the primary listener and backend by checking which listener reports itself as primary
  virtual MockPGBackendListener* get_primary_listener() {
    for (auto& [instance, listener] : listeners) {
      if (listener && listener->pgb_is_primary()) {
        return listener.get();
      }
    }
    return nullptr;
  }
  
  virtual PGBackend* get_primary_backend() {
    for (auto& [instance, listener] : listeners) {
      if (listener && listener->pgb_is_primary()) {
        auto it = backends.find(instance);
        return (it != backends.end()) ? it->second.get() : nullptr;
      }
    }
    return nullptr;
  }
  
  hobject_t make_test_object(const std::string& name) const {
    return hobject_t(object_t(name), "", CEPH_NOSNAP, 0, pool_id, "");
  }
  
  ObjectContextRef make_object_context(
    const hobject_t& hoid,
    bool exists = false,
    uint64_t size = 0) const
  {
    ObjectContextRef obc = std::make_shared<ObjectContext>();
    obc->obs.oi = object_info_t(hoid);
    obc->obs.oi.size = size;
    obc->obs.exists = exists;
    obc->ssc = nullptr;
    return obc;
  }
  
  int do_transaction_and_complete(
    const hobject_t& hoid,
    PGTransactionUPtr pg_t,
    const object_stat_sum_t& delta_stats,
    const eversion_t& at_version,
    std::vector<pg_log_entry_t> log_entries);
  
  virtual int create_and_write(
    const std::string& obj_name,
    const std::string& data,
    const eversion_t& at_version = eversion_t(1, 1));

public:
  
  int write(
    const std::string& obj_name,
    uint64_t offset,
    const std::string& data,
    const eversion_t& prior_version,
    const eversion_t& at_version,
    uint64_t object_size);

  int read_object(
    const std::string& obj_name,
    uint64_t offset,
    uint64_t length,
    bufferlist& out_data,
    uint64_t object_size);

  /**
   * Update the OSDMap and trigger backend cleanup.
   *
   * Calls on_change() on all backends, then updates the osdmap reference in
   * the fixture and all listeners.  Optionally updates the primary field on
   * every MockPGBackendListener and the convenience pointers (listener, backend).
   *
   * Does NOT update acting-set fields (shardset,
   * acting_recovery_backfill_shard_id_set, shard_info, shard_missing) on any
   * listener — those depend on the specific failure scenario being simulated
   * and must be updated by the caller.  See TestECFailover::simulate_osd_failure()
   * for a worked example.
   */
  virtual void update_osdmap(
    std::shared_ptr<OSDMap> new_osdmap,
    std::optional<pg_shard_t> new_primary = std::nullopt);

};

