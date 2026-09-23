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
#include <utility>
#include <string>
#include <gtest/gtest.h>
#include "common/errno.h"
#include "test/osd/MockErasureCode.h"
#include "test/osd/MockPGBackendListener.h"
#include "test/osd/EventLoop.h"
#include "test/osd/MockMessenger.h"
#include "test/osd/OsdTestFixture.h"
#include "test/osd/MockStore.h"
#include "test/osd/ObjectTracker.h"
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
#include "test/osd/ScrubTestFixture.h"
#include "osd/scrubber/scrub_backend.h"
#include "osd/scrubber/pg_scrubber.h"

/**
 * RAII helper that saves a ceph config option on construction and restores
 * it on destruction.  Use this in tests instead of a bare set_config() call
 * whenever the config change must be undone even if the test exits early
 * via ASSERT_* or an exception.
 *
 * Usage:
 *   {
 *     ScopedConfig guard("osd_pg_log_trim_max", "1000");
 *     // ... test body that relies on trim_max == 1000 ...
 *   }  // original value is restored here regardless of how the block exits
 *
 * The guard reads the current live value from g_ceph_context->_conf at
 * construction time, so it is correct even when the ceph default differs
 * from the value this test file previously hardcoded as the "default".
 */
class ScopedConfig {
public:
  ScopedConfig(const std::string& key, const std::string& value)
    : key_(key)
  {
    // Save the current value before we overwrite it.
    g_ceph_context->_conf.get_val(key, &saved_);
    g_ceph_context->_conf.set_val(key, value);
    g_ceph_context->_conf.apply_changes(nullptr);
  }

  ~ScopedConfig() {
    g_ceph_context->_conf.set_val(key_, saved_);
    g_ceph_context->_conf.apply_changes(nullptr);
  }

  // Non-copyable, non-movable.
  ScopedConfig(const ScopedConfig&) = delete;
  ScopedConfig& operator=(const ScopedConfig&) = delete;

private:
  std::string key_;
  std::string saved_;
};

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

  std::shared_ptr<OSDMap> osdmap;
  std::unique_ptr<EventLoop> event_loop;
  std::unique_ptr<MockMessenger> messenger;
  
  // Per-OSD test fixtures
  std::map<int, std::unique_ptr<OsdTestFixture>> osd_fixtures;
  // OpTracker for wrapping messages in OpRequestRef
  std::shared_ptr<OpTracker> op_tracker;
// Scrub infrastructure - initialized once and reused across scrub operations
  std::unique_ptr<MockScrubBeListener> scrub_listener;
  std::unique_ptr<MockSnapMapReader> snap_reader;
  ceph::ErasureCodeInterfaceRef ec_impl;
  int k = 4;  // data chunks
  int m = 2;  // coding chunks
  uint64_t stripe_unit = 4096;  // aka chunk_size
  std::string ec_plugin = "isa";
  std::string ec_technique = "reed_sol_van";
  int num_zones = 1;

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

// Object tracker for monitoring operations
  // Using shared_ptr to allow safe capture in async completion lambdas
  std::shared_ptr<ObjectTracker> object_tracker;

  std::unique_ptr<NoDoutPrefix> dpp;

public:
  ceph_tid_t get_tid() {
    return next_tid++;
  }

  explicit PGBackendTestFixture(PoolType type = EC) : pool_type(type)
  {
    ceph_assert(stripe_unit % 4096 == 0);
    ceph_assert(stripe_unit != 0);
  }
  
  ~PGBackendTestFixture() = default;
  
  void SetUp() override {
    ceph::logging::Log::set_prefix_hook(&EventLoop::get_log_prefix);
    
    g_conf().set_safe_to_start_threads();
    
    CephContext *cct = g_ceph_context;

    // Make dout statements flush immediately - we don't care about performance in tests
    if (cct->_log) {
      cct->_log->set_max_new(1);
    }

    dpp = std::make_unique<NoDoutPrefix>(cct, ceph_subsys_osd);
    event_loop = std::make_unique<EventLoop>(dpp.get());
    
    // Enable object tracking for all tests
    enable_object_tracking();

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

    // Scrub all objects before shutting down infrastructure (optimized EC pools only)
    // This verifies that all objects remain consistent throughout the test
    // Skip legacy EC pools (without FLAG_EC_OPTIMIZATIONS) as they have different behavior
    if (pool_type == EC &&
        (pool_flags & pg_pool_t::FLAG_EC_OPTIMIZATIONS) &&
        !osd_fixtures.empty() &&
        !HasFailure()) {
      scrub_all_objects();
    }

    if (op_tracker) {
      op_tracker->on_shutdown();
      op_tracker.reset();
    }

    // Clear OSD fixtures (which contain backends, listeners, LRUs, collections, stores, etc.)
    // Note: object_contexts and outstanding_writes are now per-PG and will be cleaned up when TestPG is destroyed
    osd_fixtures.clear();
    if (pool_type == EC) {
      ec_impl.reset();
    }

    ceph::logging::Log::set_prefix_hook(nullptr);
  }

private:
  void setup_ec_pool();
  void setup_replicated_pool();

  // Shared messenger/op-tracker wiring used by both setup_ec_pool() and
  // setup_replicated_pool(); message-type-specific handlers are registered
  // separately via register_backend_handler() after this returns.
  void setup_messenger();

  // Registers a handler that routes MsgType to the TestPG identified by the
  // message's spg_t on its destination OSD. Both pool-setup paths route the
  // same way so that a PG split (multiple PGs per OSD) is handled correctly
  // rather than assuming a single PG per OSD.
  template<typename MsgType>
  void register_backend_handler(int msg_type);

  // Sets the messenger on every TestPG that was created upfront during
  // setup. A no-op for fixtures that create TestPGs lazily (see
  // should_create_test_pgs_upfront()); those set the messenger themselves
  // when each TestPG is created.
  void set_messenger_on_all_pgs();

protected:
  /**
   * Should TestPGs be created upfront during setup?
   *
   * Returns true for base class (TestBackendBasics) which doesn't use peering.
   * Returns false for ECPeeringTestFixture which creates TestPGs lazily
   * in response to OSD map publication via event_advance_map().
   */
  virtual bool should_create_test_pgs_upfront() const { return true; }
  void initialize_scrub_infra();

public:
  const pg_pool_t& get_pool() const {
    const pg_pool_t* pool = OSDMapTestHelpers::get_pool(osdmap, pool_id);
    ceph_assert(pool != nullptr);
    return *pool;
  }
  
  int get_instance_count() const {
    return pool_type == EC ? (num_zones * (k + m)) : num_replicas;
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
  
  // Helper methods to access OsdTestFixture data
  OsdTestFixture* get_osd_fixture(int osd) {
    auto it = osd_fixtures.find(osd);
    if (it != osd_fixtures.end()) {
      return it->second.get();
    }
    return nullptr;
  }

  /**
   * Get the TestPG from the current EventLoop context.
   * This is the preferred method for most code paths as it uses the
   * context automatically set by MockMessenger when routing messages.
   *
   * @return Pointer to TestPG from EventLoop context, or nullptr if not set
   */
  TestPG* get_test_pg() {
    auto rc = EventLoop::get_current_test_pg();
    ceph_assert(rc);
    return rc;
  }

  /**
   * Get the TestPG for a given spg_t on the current OSD.
   * Uses the OSD from EventLoop context.
   *
   * @param spgid The spg_t identifying the PG
   * @return Pointer to TestPG, or nullptr if not found or no current OSD
   */
  TestPG* get_test_pg(const spg_t& spgid) {
    int osd = EventLoop::get_current_executing_osd();
    if (osd < 0) {
      return nullptr;
    }
    return get_test_pg(osd, spgid);
  }

  /**
   * Get the TestPG for a given OSD and spg_t.
   * This is the most general accessor that can retrieve any TestPG.
   *
   * @param osd The OSD number
   * @param spgid The spg_t identifying the PG
   * @return Pointer to TestPG, or nullptr if not found
   */
  TestPG* get_test_pg(int osd, const spg_t& spgid) {
    auto* osd_fixture = get_osd_fixture(osd);
    if (osd_fixture && osd_fixture->has_pg(spgid)) {
      return osd_fixture->get_pg(spgid);
    }
    return nullptr;
  }

  /**
   * Get the TestPG for a given OSD and shard number.
   * This is a convenience overload that constructs the spg_t from the shard.
   *
   * @param osd The OSD number
   * @param shard The shard number
   * @return Pointer to TestPG, or nullptr if not found
   */
  TestPG* get_test_pg(int osd, int shard) {
    spg_t spgid(pgid, shard_id_t(shard));
    return get_test_pg(osd, spgid);
  }

  /**
   * Get the TestPG for a given pg_shard_t.
   * This is a convenience overload that extracts OSD and shard from pg_shard_t.
   *
   * @param pg_shard The pg_shard_t containing OSD and shard
   * @return Pointer to TestPG, or nullptr if not found
   */
  TestPG* get_test_pg(const pg_shard_t& pg_shard) {
    spg_t spgid(pgid, pg_shard.shard);
    return get_test_pg(pg_shard.osd, spgid);
  }

  /**
   * Run a lambda with a specific OSD and TestPG context.
   * This is a convenience wrapper around EventLoop::run_in_pg() that
   * automatically looks up the TestPG from the OSD and spg_t.
   *
   * @param osd The OSD number to set as current
   * @param spgid The spg_t identifying the PG
   * @param callback The lambda to execute with the context set
   */
  template<typename Func>
  void run_in_pg(int osd, const spg_t& spgid, Func&& callback) {
    TestPG* test_pg = get_test_pg(osd, spgid);
    ceph_assert(test_pg != nullptr);
    event_loop->run_in_pg(osd, test_pg, std::forward<Func>(callback));
  }

  /**
   * Get the primary TestPG for a given pg_t, using OSDMap to determine the
   * primary OSD and shard. This properly handles the mapping from OSD to
   * shard for EC pools.
   *
   * @param which_pg The pg_t to look up the primary for
   * @return Pointer to primary TestPG, or nullptr if not found
   */
  TestPG* get_primary_test_pg(pg_t which_pg) {
    int primary_osd;
    spg_t primary_spgid;
    if (!osdmap->get_primary_shard(which_pg, &primary_osd, &primary_spgid)) {
      return nullptr;
    }
    return get_test_pg(primary_osd, primary_spgid);
  }

  /**
   * Get the primary TestPG for this fixture's current pgid. See the pg_t
   * overload above.
   *
   * @return Pointer to primary TestPG, or nullptr if not found
   */
  TestPG* get_primary_test_pg() {
    return get_primary_test_pg(pgid);
  }

  /**
   * Get the spg_t for a given OSD by looking up its position in the acting set.
   * This properly maps OSD number to shard for EC pools.
   *
   * @param osd The OSD number
   * @param out_spgid Output parameter for the spg_t
   * @return true if the OSD is in the acting set, false otherwise
   */
  bool get_spg_for_osd(int osd, spg_t *out_spgid) const {
    std::vector<int> acting;
    int primary;
    osdmap->pg_to_acting_osds(pgid, &acting, &primary);

    if (pool_type == EC) {
      // For EC pools, find the OSD's position in the acting set (that's the shard)
      for (size_t i = 0; i < acting.size(); ++i) {
        if (acting[i] == osd) {
          *out_spgid = spg_t(pgid, shard_id_t(i));
          return true;
        }
      }
      return false;
    } else {
      // For replicated pools, all OSDs use NO_SHARD
      *out_spgid = spg_t(pgid, shard_id_t::NO_SHARD);
      return true;
    }
  }

  /**
   * Get the sole TestPG on a given OSD.
   * NOTE: This only works if the OSD has exactly one PG (e.g. no split has
   * happened yet); it asserts otherwise. For an OSD with multiple PGs, look
   * up the specific spg_t you need instead.
   *
   * @param osd The OSD number
   * @return Pointer to the OSD's one TestPG
   */
  TestPG* get_first_test_pg_for_osd(int osd) {
    spg_t spgid;
    auto fixture = get_osd_fixture(osd);
    ceph_assert(fixture);
    // If this assert fails it means this test is doing more complex things
    // than this function can cope with.  Find a different method which does
    // not assume that each OSD has a single PG.
    ceph_assert(fixture->pgs.size() == 1);
    return fixture->pgs.begin()->second.get();
  }

  TestPG* get_test_pg_by_shard(int shard) {
    std::vector<int> acting;
    int acting_primary; // ignored
    osdmap->pg_to_acting_osds(pgid, &acting, &acting_primary);
    ceph_assert(shard >= 0);
    ceph_assert(std::cmp_less(shard, acting.size()));
    int osd = acting.at(shard);
    if (osd == CRUSH_ITEM_NONE) {
      return nullptr;
    }
    spg_t spg(pgid, shard_id_t(shard));
    return get_test_pg(osd, spg);
  }

  /**
   * Invoke f(osd, test_pg) for every TestPG on every OSD fixture. The
   * shared primitive behind for_each_peering_listener()/
   * for_each_backend_listener() and any caller that needs to iterate every
   * TestPG without a peering/backend filter (e.g. to apply a pgid filter of
   * its own, such as skipping a split's child or parent shards).
   */
  template<typename F>
  void for_each_test_pg(F&& f) {
    for (auto& [osd, osd_fixture] : osd_fixtures) {
      for (auto& [spgid, test_pg] : osd_fixture->pgs) {
        f(osd, test_pg.get());
      }
    }
  }

  /**
   * Invoke f(osd, test_pg, peering_listener) for every TestPG that has a
   * peering state.
   */
  template<typename F>
  void for_each_peering_listener(F&& f) {
    for_each_test_pg([&](int osd, TestPG* test_pg) {
      if (test_pg->has_peering_state()) {
        f(osd, test_pg, test_pg->get_peering_listener());
      }
    });
  }

  /**
   * Invoke f(osd, test_pg, backend_listener) for every TestPG that has a
   * backend.
   */
  template<typename F>
  void for_each_backend_listener(F&& f) {
    for_each_test_pg([&](int osd, TestPG* test_pg) {
      if (test_pg->has_backend()) {
        f(osd, test_pg, test_pg->get_backend_listener());
      }
    });
  }

  // Remove a shard from every backend listener's shardset and
  // acting_recovery_backfill_shard_id_set, e.g. after simulating its OSD
  // failing.
  void remove_shard_from_all_listeners(pg_shard_t shard) {
    for_each_backend_listener([&](int osd, TestPG* test_pg,
                                   MockPGBackendListener* listener) {
      listener->shardset.erase(shard);
      listener->acting_recovery_backfill_shard_id_set.erase(shard.shard);
    });
  }

  // Get the primary listener and backend by checking which listener reports itself as primary
  virtual MockPGBackendListener* get_primary_listener() {
    TestPG* test_pg = get_primary_test_pg();
    ceph_assert(test_pg);
    return test_pg->get_backend_listener();
  }

  virtual PGBackend* get_primary_backend() {
    TestPG* test_pg = get_primary_test_pg();
    ceph_assert(test_pg);
    return test_pg->get_backend();
  }
  
  // Default hash 0 (all objects map to PG seed 0).  Override to steer an
  // object into a specific child PG after a split.
  std::map<std::string, uint32_t> object_hash_overrides;

  void set_object_hash(const std::string& name, uint32_t hash) {
    object_hash_overrides[name] = hash;
  }

  hobject_t make_test_object(const std::string& name) const {
    uint32_t hash = 0;
    auto it = object_hash_overrides.find(name);
    if (it != object_hash_overrides.end()) {
      hash = it->second;
    }
    return hobject_t(object_t(name), "", CEPH_NOSNAP, hash, pool_id, "");
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


  void set_next_version(uint64_t version) {
    next_version = version;
  }
  
  eversion_t get_next_version() {
    epoch_t epoch = osdmap->get_epoch();
    return eversion_t(epoch, next_version++);
  }
  
  void set_object_context(
    const hobject_t& hoid,
    ObjectContextRef obc);

  void clear_object_contexts();

  ObjectContextRef get_object_context(
    const hobject_t& hoid,
    bool can_create,
    const std::map<std::string, ceph::buffer::list, std::less<>> *attrs = nullptr);
  
  void do_transaction(
    const hobject_t& hoid,
    PGTransactionUPtr pg_t,
    const object_stat_sum_t& delta_stats,
    const eversion_t& at_version,
    std::vector<pg_log_entry_t> log_entries,
    std::function<void(int)> on_write_complete = nullptr);

  // Opt-in log trimming.  Default hooks return (0,0) so existing tests are
  // unaffected.  ECPeeringTestFixture overrides these to mirror PrimaryLogPG.
  bool enable_log_trimming = false;
  virtual eversion_t compute_submit_trim_to() { return eversion_t(0, 0); }
  virtual eversion_t compute_submit_pg_committed_to() { return eversion_t(0, 0); }
  virtual void on_primary_write_committed(const eversion_t& at_version) {}

  /**
   * Schedule `body` to run on the primary OSD/PG and drain the event loop.
   *
   * This is the shared "run an op on the primary" wrapper used by every
   * write-shaped public entry point (create_and_write, write,
   * truncate_and_write, create_snapshot, rollback, delete_object,
   * write_attribute): look up the primary TestPG (returning -EINVAL if
   * there isn't one), allocate the heap-backed result cell that `body`
   * writes into via its completion, schedule `body` on the primary OSD,
   * optionally drain the event loop, and return the outcome.
   *
   * `result` is heap-allocated (not a stack reference) because the
   * completion may still be queued (e.g. -EINPROGRESS, or a suspended
   * shard) when this function returns; see make_write_completion().
   */
  int run_primary_op(
    std::function<void(std::shared_ptr<int> result)> body,
    bool run = true);

  /**
   * Build the completion lambda shared by every write-shaped transaction:
   * decrement/erase the object's outstanding_writes counter, roll back the
   * OBC via `on_error` if the transaction failed (and it wasn't just
   * -EINPROGRESS), otherwise invoke `on_success` (e.g. to record the write
   * in the ObjectTracker), and finally store the completion result in
   * `result`. `on_error` and `on_success` may be nullptr.
   */
  std::function<void(int)> make_write_completion(
    TestPG* test_pg,
    const hobject_t& hoid,
    std::shared_ptr<int> result,
    std::function<void()> on_error,
    std::function<void(int)> on_success);

  /**
   * Look up (creating if necessary) the OBCs for `hoid` and its snap=1
   * clone `snap_hoid`, marking either as existing with size `size` if it
   * was just created, and register both in `pg_t`'s obc_map. Shared by
   * create_snapshot() and rollback().
   */
  void prepare_obc_pair(
    const hobject_t& hoid,
    const hobject_t& snap_hoid,
    uint64_t size,
    PGTransaction* pg_t,
    ObjectContextRef& obc,
    ObjectContextRef& snap_obc);

  void do_create_and_write_impl(
    const std::string& obj_name,
    const std::string& data,
    const eversion_t& at_version,
    std::shared_ptr<int> result);

  void do_write_impl(
    const std::string& obj_name,
    uint64_t offset,
    const std::string& data,
    uint64_t object_size,
    const eversion_t& at_version,
    std::shared_ptr<int> result);

  void do_truncate_and_write_impl(
    const std::string& obj_name,
    uint64_t object_size,
    std::optional<uint64_t> truncate_size,
    const std::vector<std::pair<uint64_t, std::string>>& writes,
    std::shared_ptr<int> result);

  void do_write_attribute_impl(
    const std::string& obj_name,
    const std::string& attr_name,
    const std::string& attr_value,
    bool force_all_shards,
    std::shared_ptr<int> result);

  virtual int create_and_write(
    const std::string& obj_name,
    const std::string& data);

public:
  
  int write(
    const std::string& obj_name,
    uint64_t offset,
    const std::string& data,
    uint64_t object_size,
    bool run = true);

  /**
   * Write operation with optional truncate and multiple writes in a single transaction.
   *
   * @param obj_name Name of the object
   * @param object_size Current size of the object
   * @param truncate_size Optional truncate size (nullopt means no truncate)
   * @param writes Vector of {offset, data} pairs to write
   * @param run If true (default) call run_until_idle
   * @return Result code (0 on success, negative on error)
   */
  int truncate_and_write(
    const std::string& obj_name,
    uint64_t object_size,
    std::optional<uint64_t> truncate_size,
    const std::vector<std::pair<uint64_t, std::string>>& writes,
    bool run = true);

  /**
   * Create a snapshot of an existing object (head → snap=1).
   *
   * @param obj_name  Name of the already-written head object
   * @param snap_size Size of the object at snapshot time
   */
  int create_snapshot(
    const std::string& obj_name,
    uint64_t snap_size,
    bool run = true);

  int rollback(
    const std::string& obj_name,
    uint64_t snap_size,
    bool run = true);

  int read_object(
    const std::string& obj_name,
    uint64_t offset,
    uint64_t length,
    bufferlist& out_data,
    uint64_t object_size);

  int delete_object(const std::string& obj_name);

  /**
   * Read an attribute from an object.
   *
   * @param obj_name Name of the object
   * @param attr_name Name of the attribute to read
   * @param out_value Output buffer for the attribute value
   * @return 0 on success, negative error code on failure
   */
  int read_attribute(
    const std::string& obj_name,
    const std::string& attr_name,
    bufferlist& out_value);

  /**
   * Verify an attribute matches the value tracked by ObjectTracker.
   *
   * Reads the attribute from the store and asserts it equals the value that
   * was recorded when the attribute was written.  ObjectTracker must be
   * enabled when this is called.
   *
   * @param obj_name Name of the object
   * @param attr_name Name of the attribute
   */
  void verify_attribute(
    const std::string& obj_name,
    const std::string& attr_name);

  /**
   * Read an object and verify that its contents match tracked data.
   *
   * This helper function combines read_object with assertions to verify:
   * 1. The read operation completes successfully (result >= 0)
   * 2. The read data content matches ObjectTracker's expected content
   * 3. All tracked attributes match ObjectTracker's expected values
   *
   * @param obj_name Name of the object to read
   */
  /**
   * Visualize data miscompare with hex+ASCII dump and line compression.
   *
   * @param obj_name Name of the object being compared
   * @param expected_buf Expected data buffer
   * @param read_buf Actual read data buffer
   * @param size Size of both buffers
   * @param phase Description of when the comparison occurred (e.g., "After shard 1 failure")
   */
  void visualize_miscompare(
    const std::string& obj_name,
    const char* expected_buf,
    const char* read_buf,
    size_t size,
    const std::string& phase);

  void verify_object(const std::string& obj_name);

  /**
   * Read an object and verify that its contents match the explicitly provided data.
   *
   * Use this overload when the expected data was built by the test itself (e.g.
   * after a truncate+write sequence that is not tracked by ObjectTracker).
   *
   * @param obj_name   Name of the object to read
   * @param expected   Expected object contents
   * @param offset     Offset at which to start reading
   * @param size       Number of bytes to read and compare
   */
  void verify_object(
    const std::string& obj_name,
    const std::string& expected,
    uint64_t offset,
    size_t size);

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
   * the fixture and all listeners.
   *
   * Does NOT update acting-set fields (shardset,
   * acting_recovery_backfill_shard_id_set, shard_info, shard_missing) on any
   * listener — those depend on the specific failure scenario being simulated
   * and must be updated by the caller.  See TestECFailover::simulate_osd_failure()
   * for a worked example.
   */
  virtual void update_osdmap(std::shared_ptr<OSDMap> new_osdmap);

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
   * List all attributes on an object.
   *
   * @param obj_name Name of the object
   * @param attrs Output map of attribute name to bufferlist
   * @return 0 on success, negative on error
   */
  int list_attributes(
    const std::string& obj_name,
    std::map<std::string, ceph::buffer::list, std::less<>>& attrs);

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

  /**
   * Scrub all objects in the collection during teardown.
   *
   * This utility method:
   * 1. Enumerates all objects in the primary OSD's collection
   * 2. Scrubs each object found
   * 3. Reports any corruption detected
   *
   * This is called automatically during TearDown to verify that all
   * objects remain consistent throughout the test.
   */
  void scrub_all_objects();

  /**
   * Scrub an object and verify it has no corruption.
   *
   * This utility method:
   * 1. Builds scrub maps for all shards using be_scan_list()
   * 2. Creates a ScrubBackend with mock listeners
   * 3. Calls scrub_compare_maps() to check for inconsistencies
   * 4. Returns true if corruption was detected, false otherwise
   *
   * The scrub infrastructure (mock listeners) is initialized once in SetUp()
   * and reused across all scrub operations for efficiency.
   *
   * @param obj_name Name of the object to scrub
   * @return true if corruption detected, false if object is consistent
   */
  bool scrub_object(const std::string& obj_name, bool skip_verify = false);

  /**
   * Counts calls to scrub_object() made since this fixture instance was
   * constructed (i.e. since the start of the current TEST_P). Lets a test
   * assert that it actually invoked an inline scrub at a given point in its
   * own body, as distinct from the unconditional consistency scrub TearDown()
   * runs afterwards (see scrub_all_objects()), which checks only final state
   * and would not catch a claimed mid-test scrub that never happened.
   */
  int scrub_object_call_count = 0;

  /**
   * Corrupt the data for a specific shard of an object.
   *
   * This utility method directly writes zeros to the stored data for a given
   * shard, simulating data corruption at the storage level. This is useful
   * for testing scrub detection of corrupted data.
   *
   * @param obj The hobject_t identifying the object to corrupt
   * @param shard The pg_shard_t identifying which shard to corrupt
   */
  void corrupt_shard_data(const hobject_t& obj, pg_shard_t shard);

  /**
   * Create a bufferlist filled with random data.
   *
   * This utility method generates a buffer of the specified size filled with
   * random bytes. Useful for testing scenarios where random data is needed
   * to avoid patterns that might mask bugs (e.g., XOR patterns in EC).
   *
   * @param size Size of the buffer to create in bytes
   * @return A bufferlist containing random data
   */
  bufferlist create_random_buffer(size_t size);

  /**
   * Get the object tracker instance.
   *
   * @return Pointer to the object tracker, or nullptr if not enabled
   */
  ObjectTracker* get_object_tracker() {
    return object_tracker.get();
  }

  /**
   * Enable object tracking.
   *
   * This creates an object tracker instance that will monitor all write operations.
   * Should be called in SetUp() or at the start of a test.
   */
  void enable_object_tracking() {
    object_tracker = std::make_shared<ObjectTracker>();
  }

  /**
   * Disable object tracking and clear tracked state.
   */
  void disable_object_tracking() {
    object_tracker.reset();
  }

};

