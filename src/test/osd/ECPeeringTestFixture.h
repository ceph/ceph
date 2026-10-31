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

#include "test/osd/PeeringTestFixture.h"

class ECPeeringTestFixture : public PeeringTestFixture {
protected:
  IsPGRecoverablePredicate *get_is_recoverable_predicate(TestPG *test_pg) override;
  IsPGReadablePredicate *get_is_readable_predicate(TestPG *test_pg) override;

public:
  ECPeeringTestFixture();

  using PeeringTestFixture::get_peering_state;
  using PeeringTestFixture::get_peering_ctx;
  using PeeringTestFixture::get_peering_listener;

  // Shard-indexed accessor overloads
  TestPG* find_test_pg_for_shard(int shard);
  PeeringState* get_peering_state(int shard);
  PeeringCtx* get_peering_ctx(int shard);
  MockPeeringListener* get_peering_listener(int shard);

  // Double pg_num and split the fixture PG into itself (parent, seed 0) and a
  // child (seed 1).  Returns the child pg_t.
  pg_t split_pg();
  PeeringState* create_child_peering_state(int shard, unsigned split_bits);
  PeeringState* get_child_peering_state(int shard);
  pg_t get_child_pgid() const { return child_pgid; }

  std::string get_state_name(int shard);

  // Inject a one-shot read error on the given shard's store for this object.
  void inject_read_error_for_shard(const std::string& obj_name, int shard, int error_code);

  /**
   * run_recovery - Run recovery for an object
   *
   * This helper function encapsulates the complete EC recovery flow:
   * 1. Verifies consistency of missing sets
   * 2. Runs the recovery operation
   *
   * @param obj_name The name of the object to recover
   * @param recover_primary If true, recover to primary; if false, recover to peers
   * @param expected_data The expected data content after recovery
   */
  void run_recovery(
    const std::string& obj_name,
    bool recover_primary,
    const std::string& expected_data);

  /**
   * run_parallel_recovery - Run parallel recovery for multiple objects
   *
   * This helper function recovers multiple objects in parallel within a single recovery
   * operation. This is the key difference from run_recovery which recovers objects
   * sequentially (one at a time).
   *
   * The parallel recovery flow:
   * 1. Calls recover_object() for ALL objects first (queues them)
   * 2. Calls run_recovery_op() ONCE to process all queued recoveries together
   *
   * This reproduces Bug 75432 where multiple objects in a single operation can cause
   * assertion failures when some complete while others need resend.
   *
   * @param obj_names Vector of object names to recover in parallel
   * @param recover_primary If true, recover to primary; if false, recover to peers
   * @param expected_data Vector of expected data content (must match obj_names size)
   */
  void run_parallel_recovery(
    const std::vector<std::string>& obj_names,
    bool recover_primary,
    const std::vector<std::string>& expected_data);

private:
  void do_run_parallel_recovery_impl(
    const std::vector<std::string>& obj_names,
    bool recover_primary,
    const std::vector<std::string>& expected_data,
    int instance);
};
