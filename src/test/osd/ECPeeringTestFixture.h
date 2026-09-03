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
  std::map<int, std::unique_ptr<PeeringState>> shard_peering_states;
  std::map<int, std::unique_ptr<PeeringCtx>> shard_peering_ctxs;
  std::map<int, std::unique_ptr<MockPeeringListener>> shard_peering_listeners;

  class ShardDpp : public NoDoutPrefix {
  public:
    ECPeeringTestFixture *fixture;
    int shard;
    bool is_child;

    ShardDpp(CephContext *cct, ECPeeringTestFixture *f, int s, bool child = false)
      : NoDoutPrefix(cct, ceph_subsys_osd), fixture(f), shard(s), is_child(child) {}

    std::ostream& gen_prefix(std::ostream& out) const override;
  };
  std::map<int, std::unique_ptr<ShardDpp>> shard_dpps;

  // Child-PG state populated by split_pg().  Empty until a split is performed.
  pg_t child_pgid;
  unsigned child_split_bits = 0;
  bool stall_recovery_reservations = false;
  std::map<int, std::unique_ptr<PeeringState>> child_peering_states;
  std::map<int, std::unique_ptr<PeeringCtx>> child_peering_ctxs;
  std::map<int, std::unique_ptr<MockPeeringListener>> child_peering_listeners;
  std::map<int, coll_t> child_colls;
  std::map<int, ObjectStore::CollectionHandle> child_chs;
  std::map<int, std::unique_ptr<ShardDpp>> child_dpps;

  IsPGRecoverablePredicate *get_is_recoverable_predicate();
  IsPGReadablePredicate *get_is_readable_predicate();

public:
  ECPeeringTestFixture();

  int queue_transaction_helper(int shard, ObjectStore::Transaction&& t);
  
  void SetUp() override;
  void TearDown() override;
  
  PeeringState* create_peering_state(int shard);
  PeeringState* create_child_peering_state(int shard);

  PeeringState* get_peering_state(int shard);
  PeeringCtx* get_peering_ctx(int shard);
  MockPeeringListener* get_peering_listener(int shard);
  
  int get_primary_shard_from_osdmap() const;

  MockPGBackendListener* get_primary_listener() override;
  PGBackend* get_primary_backend() override;
  
  void init_peering(bool dne = false);
  void event_initialize();
  void event_advance_map();
  void event_activate_map();
  
  void set_config(const std::string& option, const std::string& value);

  // Set pg log target length on all listeners to drive log trimming.
  // Combine with enable_log_trimming = true.
  void set_target_pg_log_entries(unsigned n);

  // Park recovery reservation grants so peering completes (peer_missing is
  // populated) without launching recovery.  A grant delivered across a later
  // interval change would hit a PeeringState in Reset and abort.
  void set_stall_recovery_reservations(bool v);

  eversion_t compute_submit_trim_to() override;
  eversion_t compute_submit_pg_committed_to() override;
  void on_primary_write_committed(const eversion_t& at_version) override;

  // Double pg_num and split the fixture PG into itself (parent, seed 0) and a
  // child (seed 1).  Objects route to parent or child by hash (set_object_hash()).
  // Returns the child pg_t.
  pg_t split_pg();

  PeeringState* get_child_peering_state(int shard);
  pg_t get_child_pgid() const { return child_pgid; }

private:
  void dispatch_buffered_messages(int from_shard, PeeringCtx* ctx);

  // Shared tail of create_peering_state() and create_child_peering_state():
  // constructs the PeeringState, wires pl->ps / pl->ctx, sets backend
  // predicates, and stores everything in the supplied maps.
  PeeringState* create_peering_state_common(
    int shard,
    spg_t spgid,
    std::map<int, std::unique_ptr<PeeringState>>& states,
    std::map<int, std::unique_ptr<PeeringCtx>>& ctxs,
    std::map<int, std::unique_ptr<MockPeeringListener>>& listeners_map,
    std::map<int, std::unique_ptr<ShardDpp>>& dpps);

public:

  void update_osdmap_with_peering(
    std::shared_ptr<OSDMap> new_osdmap,
    std::optional<pg_shard_t> new_primary = std::nullopt);

  void new_epoch_loop();
  bool new_epoch(bool if_required = false);

  void run_first_peering();
  
  void mark_osd_down(int osd_id);
  void mark_osd_up(int osd_id);
  void mark_osds_down(const std::vector<int>& osd_ids);
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

