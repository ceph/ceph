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
#include <vector>
#include "test/osd/PGBackendTestFixture.h"
#include "test/osd/MockPeeringListener.h"
#include "osd/PeeringState.h"
#include "messages/MOSDPGNotify2.h"
#include "test/osd/MockMessenger.h"

// Forward declaration
class ECPeeringTestFixture;

/**
 * ECPeeringTestFixture - EC test fixture with full peering infrastructure
 *
 * This fixture extends PGBackendTestFixture to add full PeeringState support
 * for each shard, enabling comprehensive testing of EC peering, recovery,
 * and failover scenarios. It combines the principles from TestPeeringState
 * with the EC backend infrastructure from PGBackendTestFixture.
 */
class ECPeeringTestFixture : public PGBackendTestFixture {
protected:
  std::map<int, std::unique_ptr<PeeringState>> shard_peering_states;
  std::map<int, std::unique_ptr<PeeringCtx>> shard_peering_ctxs;
  std::map<int, std::unique_ptr<MockPeeringListener>> shard_peering_listeners;
  
  class ShardDpp : public NoDoutPrefix {
  public:
    ECPeeringTestFixture *fixture;
    int shard;
    
    ShardDpp(CephContext *cct, ECPeeringTestFixture *f, int s)
      : NoDoutPrefix(cct, ceph_subsys_osd), fixture(f), shard(s) {}
    
    std::ostream& gen_prefix(std::ostream& out) const override;
  };
  std::map<int, std::unique_ptr<ShardDpp>> shard_dpps;
  
  IsPGRecoverablePredicate *get_is_recoverable_predicate();
  IsPGReadablePredicate *get_is_readable_predicate();

public:
  ECPeeringTestFixture();

  int queue_transaction_helper(int shard, ObjectStore::Transaction&& t);
  
  void SetUp() override;
  void TearDown() override;
  
  PeeringState* create_peering_state(int shard);
  
  PeeringState* get_peering_state(int shard);
  PeeringCtx* get_peering_ctx(int shard);
  MockPeeringListener* get_peering_listener(int shard);
  
  /**
   * Query the OSDMap to determine which shard is the primary.
   * This is the authoritative source of truth for primary determination.
   *
   * @return The shard ID of the primary, or -1 if no primary exists
   */
  int get_primary_shard_from_osdmap() const;
  
  // Override base class methods to work with peering fixture's structure
  MockPGBackendListener* get_primary_listener() override;
  PGBackend* get_primary_backend() override;
  
  void init_peering(bool dne = false);
  void event_initialize();
  void event_advance_map();
  void event_activate_map();
  
  /**
   * set_config - Set a configuration option for testing
   *
   * @param option The configuration option name
   * @param value The value to set
   */
  void set_config(const std::string& option, const std::string& value);
  
private:
  /**
   * dispatch_buffered_messages - Check for and dispatch any buffered messages
   *
   * After handling a peering event, PeeringState may have buffered messages
   * in the PeeringCtx that need to be dispatched. This function checks for
   * such messages and routes them through the messenger.
   */
  void dispatch_buffered_messages(int from_shard, PeeringCtx* ctx);

public:

  // IMPORTANT: For EC pools, shard positions in acting array must be preserved.
  // Failed OSDs should be replaced with CRUSH_ITEM_NONE, not removed.
  void update_osdmap_with_peering(
    std::shared_ptr<OSDMap> new_osdmap,
    std::optional<pg_shard_t> new_primary = std::nullopt);

  void new_epoch_loop();
  bool new_epoch(bool if_required = false);

  void run_first_peering();
  
  // OSDMap manipulation helpers - these create a new epoch and trigger peering
  
  /**
   * Mark an OSD as down (exists but not UP).
   * Creates a new OSDMap epoch and triggers peering.
   */
  void mark_osd_down(int osd_id);
  
  /**
   * Mark an OSD as up.
   * Creates a new OSDMap epoch and triggers peering.
   */
  void mark_osd_up(int osd_id);
  
  /**
   * Mark multiple OSDs as down.
   * Creates a new OSDMap epoch and triggers peering.
   */
  void mark_osds_down(const std::vector<int>& osd_ids);
  
  /**
   * Advance to a new epoch without changing OSD states.
   * Useful for testing re-peering scenarios.
   */
  void advance_epoch();
  
  bool all_shards_active();
  
  // In EC pools, only the primary tracks PG_STATE_CLEAN.
  bool all_shards_clean();
  
  std::string get_state_name(int shard);

  /**
   * Suspend an OSD - queues events for this OSD without executing them.
   * This simulates an OSD being temporarily unavailable.
   * Events remain queued and will be processed when the OSD is unsuspended.
   *
   * @param osd The OSD number to suspend
   */
  void suspend_osd(int osd);

  /**
   * Unsuspend a previously suspended OSD.
   * Queued events for this OSD will be processed on the next event loop iteration.
   *
   * @param osd The OSD number to unsuspend
   */
  void unsuspend_osd(int osd);

  /**
   * Check if an OSD is currently suspended.
   *
   * @param osd The OSD number to check
   * @return true if the OSD is suspended, false otherwise
   */
  bool is_osd_suspended(int osd);

  /**
   * Suspend messages from the primary to a specific OSD.
   * This blocks communication from the primary to the target OSD while
   * allowing other communication to proceed normally.
   *
   * @param to_osd The OSD number to block messages to (from the primary)
   */
  void suspend_primary_to_osd(int to_osd);

  /**
   * Unsuspend messages from the primary to a specific OSD.
   * Queued messages will be processed on the next event loop iteration.
   *
   * @param to_osd The OSD number to unblock messages to (from the primary)
   */
  void unsuspend_primary_to_osd(int to_osd);

  /**
   * Inject a read error for a specific object on a specific shard's store.
   * The error will be returned on the next read() call for this object,
   * then automatically cleared.
   *
   * @param obj_name The name of the object to inject an error for
   * @param shard The shard number whose store should return the error
   * @param error_code The error code to return (should be negative, e.g., -EIO)
   */
  void inject_read_error_for_shard(const std::string& obj_name, int shard, int error_code);

  /**
   * run_recovery_and_verify_callbacks - Run recovery for an object and verify callbacks
   *
   * This helper function encapsulates the complete EC recovery flow:
   * 1. Verifies the object is in the peer's missing set
   * 2. Runs the recovery operation
   * 3. Verifies all recovery callbacks were invoked correctly
   * 4. Verifies PeeringState was updated correctly
   *
   * @param obj_name The name of the object to recover
   * @param removed_osd The OSD that was down and needs recovery
   * @param expected_data The expected data content after recovery
   */
  void run_recovery_and_verify_callbacks(
    const std::string& obj_name,
    int removed_osd,
    const std::string& expected_data);

  /**
   * run_parallel_recovery_and_verify_callbacks - Run parallel recovery for multiple objects
   *
   * This helper function recovers multiple objects in parallel within a single recovery
   * operation. This is the key difference from run_recovery_and_verify_callbacks which
   * recovers objects sequentially (one at a time).
   *
   * The parallel recovery flow:
   * 1. Calls recover_object() for ALL objects first (queues them)
   * 2. Calls run_recovery_op() ONCE to process all queued recoveries together
   * 3. Verifies callbacks and data for all objects
   *
   * This reproduces Bug 75432 where multiple objects in a single operation can cause
   * assertion failures when some complete while others need resend.
   *
   * @param obj_names Vector of object names to recover in parallel
   * @param target_osd The OSD that was down and needs recovery
   * @param expected_data Vector of expected data content (must match obj_names size)
   */
  void run_parallel_recovery_and_verify_callbacks(
    const std::vector<std::string>& obj_names,
    int target_osd,
    const std::vector<std::string>& expected_data);
};

