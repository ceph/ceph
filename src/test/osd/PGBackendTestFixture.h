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
#include "test/osd/OSDMapTestHelpers.h"
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

/**
 * PGBackendTestFixture - Unified test fixture for EC and Replicated backend tests with ObjectStore
 *
 * This fixture consolidates ECStoreTestFixture and ReplicatedStoreTestFixture into a single
 * unified implementation, eliminating ~700 lines of duplicated code. It uses polymorphism
 * through the PGBackend interface and conditional logic based on pool_type to support both
 * EC and Replicated backends.
 *
 * Key design decisions:
 * - Uses enum PoolType { EC, REPLICATED } to branch behavior
 * - Stores backends polymorphically in std::map<int, std::unique_ptr<PGBackend>>
 * - Keeps ALL EC-specific members (ec_impl, lrus, k, m, stripe_width, etc.)
 * - Keeps ALL Replicated-specific members (num_replicas, min_size)
 * - Uses unified naming: "instance" instead of "shard" or "replica"
 * - Provides convenience pointers: backend (points to backends[0]), listener
 *
 * It provides:
 * - Real ObjectStore (memstore by default, configurable)
 * - Mock PG backend listener with store integration
 * - Mock operation tracker
 * - OSDMap with EC or replicated pool configuration
 * - PG and instance configuration
 * - Collection setup for EC or replicated operations
 *
 * Usage:
 *   class MyECTest : public PGBackendTestFixture {
 *   protected:
 *     MyECTest() : PGBackendTestFixture(PoolType::EC) {}
 *     void SetUp() override {
 *       PGBackendTestFixture::SetUp();
 *       // Your additional setup
 *     }
 *   };
 *
 *   class MyReplicatedTest : public PGBackendTestFixture {
 *   protected:
 *     MyReplicatedTest() : PGBackendTestFixture(PoolType::REPLICATED) {}
 *     void SetUp() override {
 *       PGBackendTestFixture::SetUp();
 *       // Your additional setup
 *     }
 *   };
 */
class PGBackendTestFixture : public ::testing::Test {
public:
  // Pool type enum for branching behavior
  enum PoolType {
    EC,
    REPLICATED
  };

protected:
  // Pool type - determines backend creation and behavior
  PoolType pool_type;

  // Whether EC optimizations are enabled (FLAG_EC_OPTIMIZATIONS).
  // Derived classes can set this to false before SetUp() to create a pool
  // without EC optimizations.  setup_ec_pool() reads this flag and clears
  // FLAG_EC_OPTIMIZATIONS from the OSDMap pool entry before creating the
  // ECSwitch backends, so that is_optimized_actual is consistent with the
  // live pool from the start.
  bool ec_optimizations = true;
  
  // MemStore - always use manual finisher mode for single-threaded testing
  std::unique_ptr<MemStore> store;
  std::string data_dir;
  ObjectStore::CollectionHandle ch;
  coll_t coll;
  
  // Common members
  std::shared_ptr<OSDMap> osdmap;
  std::unique_ptr<OpTracker> op_tracker;
  
  // EventLoop for unified event processing
  std::unique_ptr<EventLoop> event_loop;
  
  // Message router - maps OSD ID to message handler
  std::map<int, std::function<bool(OpRequestRef)>> message_router;
  
  // Multi-instance support - polymorphic backend storage
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
    pg_shard_t whoami,
    pg_shard_t primary)> listener_factory;

  // Primary instance (instance 0) - convenience accessors
  MockPGBackendListener* listener = nullptr;
  PGBackend* backend = nullptr;
  
  // EC-specific members (only used when pool_type == EC)
  ceph::ErasureCodeInterfaceRef ec_impl;
  std::map<int, std::unique_ptr<ECExtentCache::LRU>> lrus;
  int k = 4;  // data chunks
  int m = 2;  // coding chunks
  uint64_t stripe_width = 4096 * 4;  // Will be updated to 4096 * k in SetUp
  std::string ec_plugin = "isa";  // EC plugin to use: "isa", "jerasure", etc.
  std::string ec_technique = "reed_sol_van";  // EC technique
  
  // Replicated-specific members (only used when pool_type == REPLICATED)
  int num_replicas = 3;  // Total number of replicas (size)
  int min_size = 2;      // Minimum replicas for writes
  
  // Pool and PG configuration
  int64_t pool_id = 0;  // Use pool 0 to match backend's default
  pg_t pgid;
  spg_t spgid;
  
  // DoutPrefixProvider for logging
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
  /**
   * Constructor - always uses MemStore with manual finisher
   * 
   * @param type Pool type (EC or REPLICATED)
   */
  explicit PGBackendTestFixture(PoolType type = EC) : pool_type(type)
  {
    // Generate random directory name for test isolation
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dis;
    uint64_t random_num = dis(gen);
    
    std::ostringstream oss;
    oss << "memstore_test_" << std::hex << std::setfill('0') << std::setw(16) << random_num;
    data_dir = oss.str();
    
    // Update stripe_width based on k value
    ceph_assert((stripe_width / k) % 4096 == 0);
    ceph_assert(stripe_width != 0);
  }
  
  void SetUp() override {
    // Create directory for MemStore
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
    
    // Set config to safe mode
    g_conf().set_safe_to_start_threads();
    
    // Set up common infrastructure
    CephContext *cct = g_ceph_context;
    dpp = std::make_unique<TestDpp>(cct);
    
    // Create EventLoop for unified event processing
    event_loop = std::make_unique<EventLoop>(false);  // verbose=false by default
    
    // Create OpTracker for operation tracking (tracking disabled for tests)
    op_tracker = std::make_unique<OpTracker>(cct, false, 1);
    
    // Branch on pool type for setup
    if (pool_type == EC) {
      setup_ec_pool();
    } else {
      setup_replicated_pool();
    }
  }
  
  void TearDown() override {
    // Clean up in proper order to handle pending operations
    
    // 0. Process any remaining events in the EventLoop
    if (event_loop) {
      event_loop->run_until_idle(1000);  // Max 1000 events to prevent infinite loops
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
    
    // Clear convenience pointers
    listener = nullptr;
    backend = nullptr;
    
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
    std::filesystem::remove_all(data_dir);
  }
  
private:
  /**
   * Setup EC pool and backends
   */
  void setup_ec_pool();

  /**
   * Setup replicated pool and backends
   */
  void setup_replicated_pool();

public:
  // Helper methods
  
  /**
   * Get the pool configuration from OSDMap
   */
  const pg_pool_t& get_pool() const {
    const pg_pool_t* pool = OSDMapTestHelpers::get_pool(osdmap, pool_id);
    ceph_assert(pool != nullptr);
    return *pool;
  }
  
  /**
   * Get total instance count (k+m for EC, num_replicas for Replicated)
   */
  int get_instance_count() const {
    return pool_type == EC ? (k + m) : num_replicas;
  }
  
  /**
   * Get the number of data chunks (EC only)
   */
  int get_data_chunk_count() const {
    return k;
  }
  
  /**
   * Get the number of coding chunks (EC only)
   */
  int get_coding_chunk_count() const {
    return m;
  }
  
  /**
   * Get the stripe width (EC only)
   */
  uint64_t get_stripe_width() const {
    return stripe_width;
  }
  
  /**
   * Get minimum size (Replicated only)
   */
  int get_min_size() const {
    return min_size;
  }
  
  /**
   * Create a test object hobject_t
   * 
   * @param name Object name
   * @return hobject_t for the test object
   */
  hobject_t make_test_object(const std::string& name) const {
    return hobject_t(object_t(name), "", CEPH_NOSNAP, 0, pool_id, "");
  }
  
  /**
   * Create an ObjectContext for a test object
   *
   * @param hoid The object
   * @param exists Whether the object exists
   * @param size Object size
   * @return ObjectContextRef
   */
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
  
  /**
   * Generic helper: Submit a transaction and wait for completion
   *
   * This method handles the full lifecycle of a transaction:
   * - Submits the transaction to the backend (polymorphic)
   * - Runs the event loop to process messages and transactions across all instances
   * - Waits for completion with timeout handling
   *
   * @param hoid The object being modified
   * @param pg_t The PGTransaction to submit
   * @param delta_stats Statistics delta for this transaction
   * @param at_version Version for this transaction
   * @param log_entries PG log entries for this transaction
   * @return Completion result code (0 on success)
   * @throws std::runtime_error if transaction times out
   */
  int do_transaction_and_complete(
    const hobject_t& hoid,
    PGTransactionUPtr pg_t,
    const object_stat_sum_t& delta_stats,
    const eversion_t& at_version,
    std::vector<pg_log_entry_t> log_entries);
  
  /**
   * Wrapper: Create an object and write data to it
   *
   * This is a high-level helper that combines object creation and writing
   * into a single operation. It handles all the boilerplate setup including:
   * - Creating the PGTransaction
   * - Setting up ObjectContext
   * - Preparing statistics and log entries
   * - Executing the transaction
   * - Updating ObjectContext to reflect the object now exists
   *
   * @param obj_name Name for the test object
   * @param data Data to write to the object
   * @param at_version Version for this transaction (default: 1.1)
   * @return Completion result code (0 on success)
   */
  virtual int create_and_write(
    const std::string& obj_name,
    const std::string& data,
    const eversion_t& at_version = eversion_t(1, 1));

protected:
  /**
   * Hook called inside create_and_write() just before do_transaction_and_complete().
   *
   * The default implementation is a no-op.  ECPeeringTestFixture overrides this
   * to append the PG log entries to the primary's PeeringState before the
   * transaction is submitted, so that the PG log stays consistent with the
   * object store.
   *
   * @param hoid        The object being created/written
   * @param log_entries The log entries that will be submitted with the transaction
   * @param at_version  The version for this transaction
   */
  virtual void pre_transaction_hook(
      const hobject_t& /* hoid */,
      const std::vector<pg_log_entry_t>& /* log_entries */,
      const eversion_t& /* at_version */) {
    // Default: no-op. ECPeeringTestFixture overrides to append PG log entries.
  }

public:
  
  /**
   * Wrapper: Write data to an existing object
   *
   * This is a high-level helper that writes data to an existing object
   * at a specified offset. It handles all the boilerplate setup including:
   * - Creating the PGTransaction
   * - Setting up ObjectContext with existing object state
   * - Preparing statistics and log entries
   * - Executing the transaction
   * - Updating ObjectContext to reflect the new state
   *
   * @param obj_name Name of the existing object
   * @param offset Offset to write at
   * @param data Data to write to the object
   * @param prior_version Previous version of the object
   * @param at_version Version for this transaction
   * @param object_size Current size of the object
   * @return Completion result code (0 on success)
   */
  int write(
    const std::string& obj_name,
    uint64_t offset,
    const std::string& data,
    const eversion_t& prior_version,
    const eversion_t& at_version,
    uint64_t object_size);

  /**
   * Wrapper: Read data from an object
   *
   * This is a high-level helper that reads data from an existing object.
   * For EC backends, it uses the asynchronous read interface.
   * For Replicated backends, it uses the synchronous read interface.
   *
   * @param obj_name Name of the object to read
   * @param offset Offset to read from
   * @param length Length to read
   * @param out_data Buffer to store the read data
   * @param object_size Size of the object (for EC alignment)
   * @return Completion result code (0 on success, or bytes read for Replicated)
   */
  int read_object(
    const std::string& obj_name,
    uint64_t offset,
    uint64_t length,
    bufferlist& out_data,
    uint64_t object_size);

  /**
   * Update the OSDMap and trigger backend cleanup
   *
   * This helper simulates an OSDMap change (e.g., during failover scenarios).
   * It properly handles the backend state transitions by:
   * 1. Calling on_change() on all backends to clear in-flight operations
   * 2. Updating the osdmap reference in the fixture and all listeners
   * 3. Optionally updating the primary instance
   *
   * @param new_osdmap The new OSDMap to use
   * @param new_primary Optional new primary instance (default: no change)
   */
  virtual void update_osdmap(
    std::shared_ptr<OSDMap> new_osdmap,
    std::optional<pg_shard_t> new_primary = std::nullopt);

};

