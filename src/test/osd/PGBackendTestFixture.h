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
#include "test/osd/MockMessenger.h"
#include "common/TrackedOp.h"
#include "os/memstore/MemStore.h"
#include "test/osd/MockStore.h"
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
  
  std::unique_ptr<MockStore> store;
  std::string data_dir;
  ObjectStore::CollectionHandle ch;
  coll_t coll;
  
  std::shared_ptr<OSDMap> osdmap;
  std::unique_ptr<EventLoop> event_loop;
  std::unique_ptr<MockMessenger> messenger;
  
  std::map<int, std::unique_ptr<MockPGBackendListener>> listeners;
  std::map<int, std::unique_ptr<PGBackend>> backends;
  std::map<int, coll_t> colls;
  std::map<int, ObjectStore::CollectionHandle> chs;
  
  /// Persistent OBC storage - emulates PrimaryLogPG's object_contexts LRU.
  /// Keyed by hobject_t, values are shared_ptr so the same OBC is reused
  /// across sequential operations on the same object. This is critical for
  /// EC attr_cache continuity.
  std::map<hobject_t, ObjectContextRef> object_contexts;
  
  /// Track outstanding writes per object. When this reaches 0, we can safely
  /// clear attr_cache (as there are no in-flight writes that might have stale
  /// cached OI data).
  std::map<hobject_t, int> outstanding_writes;
  
  // OpTracker for wrapping messages in OpRequestRef
  std::shared_ptr<OpTracker> op_tracker;
  
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
  
  // Transaction ID counter - increments with each transaction
  ceph_tid_t next_tid = 1;
  
  // Version counter for auto-generating versions in write* functions
  // The epoch comes from osdmap, this tracks the second version number
  uint64_t next_version = 1;
  
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
    
    // Create MockMemStore - contexts are stolen by MockPGBackendListener, so we don't need manual_finisher
    store.reset(new MockStore(g_ceph_context, data_dir));
    ASSERT_TRUE(store);
    ASSERT_EQ(0, store->mkfs());
    ASSERT_EQ(0, store->mount());
    
    g_conf().set_safe_to_start_threads();
    
    CephContext *cct = g_ceph_context;
    dpp = std::make_unique<TestDpp>(cct);
    event_loop = std::make_unique<EventLoop>(false);
    
    if (pool_type == EC) {
      setup_ec_pool();
    } else {
      setup_replicated_pool();
    }
  }
  
  void TearDown() override {

    if (event_loop) {
      if (event_loop->has_events()) {
        if (!HasFailure()) {
          ADD_FAILURE() << "TearDown: " << event_loop->queued_event_count()
                        << " orphaned events remain after a passing test";
        }
        event_loop->run_until_idle();
      }
    }

    if (op_tracker) {
      op_tracker->on_shutdown();
      op_tracker.reset();
    }

    backends.clear();
    object_contexts.clear();
    outstanding_writes.clear();
    
    if (pool_type == EC) {
      lrus.clear();
      ec_impl.reset();
    }

    listeners.clear();
    chs.clear();
    colls.clear();
    
    if (ch) {
      ch.reset();
    }

    if (store) {
      store->umount();
      store.reset();
    }

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
  
  /// Get an existing OBC or create a new one.
  /// Unlike make_object_context(), this method reuses OBCs for the same
  /// object across operations, which is essential for attr_cache continuity
  /// in EC pools.
  /// @param primary_shard The shard ID to read attributes from (for EC pools)
  ObjectContextRef get_or_create_obc(
    const hobject_t& hoid,
    bool exists = false,
    uint64_t size = 0,
    int primary_shard = 0)
  {
    auto it = object_contexts.find(hoid);
    ObjectContextRef obc;

    if (it != object_contexts.end()) {
      obc = it->second;
    } else {
      obc = make_object_context(hoid, exists, size);
      object_contexts[hoid] = obc;
    }

    // If the object exists and this is an EC pool, populate attr_cache with
    // ALL attributes from disk if not already populated. This matches production
    // behavior where the OBC is loaded with all xattrs from the object store.
    // In EC, attributes are stored per-shard, so we must read from the specified shard.
    if (exists && pool_type == EC && store && !chs.empty() && obc->attr_cache.empty()) {
      auto writes_it = outstanding_writes.find(hoid);
      bool has_outstanding_writes = (writes_it != outstanding_writes.end() && writes_it->second > 0);

      // Cannot read from disk if there are outstanding writes - test bug
      ceph_assert(!has_outstanding_writes);

      // For EC pools, attributes are stored with the shard ID in the ghobject_t
      ceph_assert(primary_shard >= 0 && primary_shard < (int)chs.size());
      ObjectStore::CollectionHandle ch = chs[primary_shard];
      if (ch) {
        ghobject_t ghoid(hoid, ghobject_t::NO_GEN, shard_id_t(primary_shard));
        std::map<std::string, ceph::buffer::ptr, std::less<>> attrs;
        int r = store->getattrs(ch, ghoid, attrs);

        if (r >= 0) {
          // Successfully read all attributes from disk - populate the cache
          for (auto& [key, value_ptr] : attrs) {
            bufferlist bl;
            bl.append(value_ptr);
            obc->attr_cache[key] = std::move(bl);
          }
        }
      }
    }

    return obc;
  }
  
  /**
   * Set the next version number for auto-generation.
   * This can be used by tests after rollback to set the version to a specific value.
   * The epoch will still come from the osdmap.
   */
  void set_next_version(uint64_t version) {
    next_version = version;
  }
  
  /**
   * Get the next version as an eversion_t with epoch from osdmap.
   * This auto-increments the version counter.
   */
  eversion_t get_next_version() {
    epoch_t epoch = osdmap->get_epoch();
    return eversion_t(epoch, next_version++);
  }
  
  /**
   * Read ObjectInfo from the store for an existing object.
   * Returns an ObjectContext with the decoded ObjectInfo, or a new
   * ObjectContext with default values if the object doesn't exist.
   */
  ObjectContextRef get_object_context(
    const hobject_t& hoid);
  
  int do_transaction_and_complete(
    const hobject_t& hoid,
    PGTransactionUPtr pg_t,
    const object_stat_sum_t& delta_stats,
    const eversion_t& at_version,
    std::vector<pg_log_entry_t> log_entries,
    std::function<void(int)> on_write_complete = nullptr);
  
  virtual int create_and_write(
    const std::string& obj_name,
    const std::string& data);

public:
  
  int write(
    const std::string& obj_name,
    uint64_t offset,
    const std::string& data,
    uint64_t object_size);

  int read_object(
    const std::string& obj_name,
    uint64_t offset,
    uint64_t length,
    bufferlist& out_data,
    uint64_t object_size);

  /**
   * Read an object and verify that its contents match expected data.
   *
   * This helper function combines read_object with assertions to verify:
   * 1. The read operation completes successfully (result >= 0)
   * 2. The read data length matches expected length
   * 3. The read data content matches expected content
   *
   * @param obj_name Name of the object to read
   * @param expected_data Expected data content
   * @param offset Offset to read from (default: 0)
   * @param context_msg Optional context message to append to assertion messages
   */
  void verify_object(
    const std::string& obj_name,
    const std::string& expected_data,
    size_t offset,
    size_t object_size);

  /**
   * Create and write an object, then verify it was written correctly.
   *
   * This helper function combines create_and_write with verify_object to:
   * 1. Create and write the object
   * 2. Verify the write completed successfully (result == 0)
   * 3. Read back and verify the data matches
   *
   * @param obj_name Name of the object to create and write
   * @param data Data to write
   * @param context_msg Optional context message to append to assertion messages
   */
  void create_and_write_verify(
    const std::string& obj_name,
    const std::string& data);

  /**
   * Write to an object (potentially with offset), then verify the write succeeded.
   *
   * This helper function combines write with verification to:
   * 1. Write data at the specified offset
   * 2. Verify the write completed successfully (result == 0)
   * 3. Read back and verify the written data matches
   *
   * @param obj_name Name of the object to write
   * @param offset Offset to write at
   * @param data Data to write
   * @param object_size Current size of the object
   * @param context_msg Optional context message to append to assertion messages
   * @return The result code from the write operation
   */
  void write_verify(
    const std::string& obj_name,
    size_t offset,
    const std::string& data,
    size_t object_size,
    const std::string& context_msg = "");

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

  /**
   * Clear attr_cache for all objects.
   * Called on on_change() to invalidate cached attributes that might be stale
   * after a peering event or OSDMap change.
   */
  void clear_all_attr_caches();

  /**
   * Write attributes to an object with control over first_write_in_interval.
   *
   * This simulates different types of writes in EC pools:
   * - force_all_shards=true: Simulates first_write_in_interval=true, causing
   *   all_shards_written() which updates ALL shards (data + parity)
   * - force_all_shards=false: Simulates first_write_in_interval=false, causing
   *   only PRIMARY shards (shard 0 + parity shards) to be updated
   *
   * This is useful for testing EC rollback scenarios where version mismatches
   * can occur between primary and non-primary shards.
   *
   * @param obj_name Name of the object
   * @param attr_name Name of the attribute to write
   * @param attr_value Value of the attribute
   * @param force_all_shards If true, forces all shards to be written
   * @return Result code (0 on success, -EINPROGRESS if blocked, negative on error)
   */
  int write_attribute(
    const std::string& obj_name,
    const std::string& attr_name,
    const std::string& attr_value,
    bool force_all_shards);

  /**
   * Read object_info_t directly from the ObjectStore for a specific shard.
   *
   * This bypasses the OBC cache and reads the actual on-disk state,
   * which is useful for verifying version consistency across shards
   * after rollback or peering events.
   *
   * @param obj_name Name of the object
   * @param shard Shard ID to read from
   * @return The object_info_t decoded from the shard's OI_ATTR
   */
  object_info_t read_shard_object_info(
    const std::string& obj_name,
    int shard);

};

