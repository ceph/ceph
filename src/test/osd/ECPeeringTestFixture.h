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

class ECPeeringTestFixture;

class ECPeeringTestFixture : public PGBackendTestFixture {
protected:
  class ShardDpp : public NoDoutPrefix {
  public:
    ECPeeringTestFixture *fixture;
    TestPG *test_pg;  // Direct pointer to the TestPG this DPP belongs to
    ShardDpp(CephContext *cct, ECPeeringTestFixture *f, TestPG *tp)
      : NoDoutPrefix(cct, ceph_subsys_osd), fixture(f), test_pg(tp) {}
    
    std::ostream& gen_prefix(std::ostream& out) const override;
  };

  // Park recovery reservation grants so peering completes without launching
  // recovery (a grant delivered into a later interval hits Reset and aborts).
  bool stall_recovery_reservations = false;

  // Child-PG identity set by split_pg().  Zero until a split is performed.
  pg_t child_pgid;

  IsPGRecoverablePredicate *get_is_recoverable_predicate();
  IsPGReadablePredicate *get_is_readable_predicate();

public:
  ECPeeringTestFixture();

  int queue_transaction_helper(TestPG* test_pg, ObjectStore::Transaction&& t);
  
  void SetUp() override;
  void TearDown() override;

  // No-arg variants delegate to the current EventLoop TestPG context
  PeeringState* get_peering_state() {
    return get_test_pg()->get_peering_state();
  }
  
  PeeringCtx* get_peering_ctx() {
    return get_test_pg()->get_peering_ctx();
  }
  
  MockPeeringListener* get_peering_listener() {
    return get_test_pg()->get_peering_listener();
  }

  // Shard-indexed accessor overloads
  TestPG* find_test_pg_for_shard(int shard);
  PeeringState* get_peering_state(int shard);
  PeeringCtx* get_peering_ctx(int shard);
  MockPeeringListener* get_peering_listener(int shard);

  int get_primary_shard_from_osdmap() const;

  MockPGBackendListener* get_primary_listener() override;
  PGBackend* get_primary_backend() override;
  
  void init_peering(TestPG *test_pg);
  void advance_map_impl();
  void event_advance_map();
  void event_activate_map();
  
  void set_config(const std::string& option, const std::string& value);

  // Set pg log target length on all listeners to drive log trimming.
  // Combine with enable_log_trimming = true.
  void set_target_pg_log_entries(unsigned n);

  // Park recovery reservation grants so peering completes (peer_missing is
  // populated) without launching recovery.
  void set_stall_recovery_reservations(bool v);

  eversion_t compute_submit_trim_to() override;
  eversion_t compute_submit_pg_committed_to() override;
  void on_primary_write_committed(const eversion_t& at_version) override;

  // Double pg_num and split the fixture PG into itself (parent, seed 0) and a
  // child (seed 1).  Returns the child pg_t.
  pg_t split_pg();
  PeeringState* create_child_peering_state(int shard, unsigned split_bits);
  PeeringState* get_child_peering_state(int shard);
  pg_t get_child_pgid() const { return child_pgid; }

  /**
   * ensure_osd_fixture_exists - Create OSD fixture if it doesn't exist
   *
   * This is called in response to OSDMap updates to create fixtures for
   * OSDs that are in the acting set but don't have fixtures yet.
   *
   * @param osd The OSD number to ensure exists
   */
  void ensure_osd_fixture_exists(int osd);
  
  /**
   * ensure_test_pg_exists - Create TestPG if it doesn't exist
   *
   * This is called in response to OSDMap updates to create TestPGs for
   * OSDs that are in the acting set but don't have TestPGs yet.
   *
   * @param osd The OSD number
   * @param shard The shard number
   */
  void ensure_test_pg_exists(pg_shard_t pg_whoami);

protected:
  /**
   * Override to defer TestPG creation until OSD map publication.
   * TestPGs will be created lazily in event_advance_map() via ensure_test_pg_exists().
   */
  bool should_create_test_pgs_upfront() const override { return false; }

private:
  void dispatch_buffered_messages(int osd, PeeringCtx* ctx);

  // Shared tail of ensure_test_pg_exists() and create_child_peering_state():
  // test_pg->peering_listener must already be set by the caller.  Constructs
  // the PeeringState, wires pl->ps / pl->ctx, and sets backend predicates.
  void create_peering_state_common(TestPG* test_pg, spg_t spgid, pg_shard_t pg_whoami);

  // Core of new_epoch(): checks up_thru/pg_temp for the given PG and, if any
  // work was found (or if_required is false), bumps the osdmap epoch and
  // updates all listener current_epochs.  Returns true iff a new epoch was
  // applied.  Shared between new_epoch() (parent PG) and the child-peering
  // loop inside split_pg() (child PG).
  bool apply_new_epoch(pg_t which_pg, bool if_required);

public:

  void update_osdmap_with_peering(std::shared_ptr<OSDMap> new_osdmap);

  void new_epoch_loop();
  bool new_epoch(bool if_required = false);

  // OSDMap manipulation helpers - these create a new epoch and trigger peering
  
  /**
   * Mark an OSD as down (exists but not UP).
   * Creates a new OSDMap epoch and triggers peering.
   */
  void mark_osd_down(int osd_id);
  void mark_osd_up(int osd_id);
  void mark_osds_down(const std::vector<int>& osd_ids);

  /**
   * Advance to a new epoch without changing OSD states.
   * Useful for testing re-peering scenarios.
   */
  void advance_epoch();

  bool all_shards_active();
  bool all_shards_clean();  // only the primary tracks PG_STATE_CLEAN in EC pools
  std::string get_state_name(int shard);

  void suspend_osd(int osd);
  void unsuspend_osd(int osd);
  bool is_osd_suspended(int osd);

  void suspend_primary_to_osd(int to_osd);
  void unsuspend_primary_to_osd(int to_osd);

  // Inject a one-shot read error on the given shard's store for this object.
  void inject_read_error_for_shard(const std::string& obj_name, int shard, int error_code);

  void run_recovery_and_verify_callbacks(
    const std::string& obj_name,
    int removed_osd,
    const std::string& expected_data);

  // Recover multiple objects in a single parallel operation (reproduces bug 75432).
  void run_parallel_recovery_and_verify_callbacks(
    const std::vector<std::string>& obj_names,
    int target_osd,
    const std::vector<std::string>& expected_data);

private:
  void do_run_parallel_recovery_and_verify_callbacks_impl(
    const std::vector<std::string>& obj_names,
    int target_osd,
    const std::vector<std::string>& expected_data,
    int instance);
};

