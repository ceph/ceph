// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2024 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

// WI-17-e: Unit tests for SnapMapper::record_completed_rollbacks(),
// set_completed_rollback(), and is_completed_rollback().
//
// Uses a simple in-memory StoreDriver so no real ObjectStore is required.

#include "gtest/gtest.h"

#include "osd/SnapMapper.h"
#include "common/map_cacher.hpp"
#include "include/buffer.h"

#include <map>
#include <set>
#include <string>

using namespace std;

// ---------------------------------------------------------------------------
// SimpleMapDriver -- an in-memory MapCacher::StoreDriver<string,bufferlist>
// ---------------------------------------------------------------------------

class SimpleMapDriver : public MapCacher::StoreDriver<string, ceph::buffer::list> {
public:
  map<string, ceph::buffer::list> store;

  // Transaction that batches writes/removes until applied
  class Txn : public MapCacher::Transaction<string, ceph::buffer::list> {
  public:
    SimpleMapDriver *parent;
    explicit Txn(SimpleMapDriver *p) : parent(p) {}

    void set_keys(const map<string, ceph::buffer::list>& keys) override {
      for (auto& [k, v] : keys)
        parent->store[k] = v;
    }
    void remove_keys(const set<string>& keys) override {
      for (auto& k : keys)
        parent->store.erase(k);
    }
    void add_callback(Context *c) override {
      if (c) c->complete(0);
    }
  };

  Txn make_txn() { return Txn(this); }

  int get_keys(const set<string>& keys,
               map<string, ceph::buffer::list> *got) override {
    for (auto& k : keys) {
      auto it = store.find(k);
      if (it != store.end())
        (*got)[k] = it->second;
    }
    return 0;
  }
  int get_next(const string& key,
               pair<string, ceph::buffer::list> *next) override {
    auto it = store.upper_bound(key);
    if (it == store.end()) return -ENOENT;
    *next = *it;
    return 0;
  }
  int get_next_or_current(const string& key,
                          pair<string, ceph::buffer::list> *next) override {
    auto it = store.lower_bound(key);
    if (it == store.end()) return -ENOENT;
    *next = *it;
    return 0;
  }
};

// ---------------------------------------------------------------------------
// WI-17-e: Unit tests
// ---------------------------------------------------------------------------

TEST(SnapMapperCompletedRollbacks, SetAndIsCompletedRollback) {
  SimpleMapDriver driver;
  auto txn = driver.make_txn();

  // Set a rollback in pool 1, rollback_id 42
  SnapMapper::set_completed_rollback(driver, txn, 1, snapid_t(42));

  // It should now be found
  EXPECT_TRUE(SnapMapper::is_completed_rollback(driver, 1, snapid_t(42)));

  // An unrelated rollback should not be found
  EXPECT_FALSE(SnapMapper::is_completed_rollback(driver, 1, snapid_t(43)));
  EXPECT_FALSE(SnapMapper::is_completed_rollback(driver, 2, snapid_t(42)));
}

TEST(SnapMapperCompletedRollbacks, RecordAcrossEpochsAndPools) {
  SimpleMapDriver driver;

  // Build a two-epoch, two-pool completed_rollbacks map:
  // epoch 10: pool 0: rb_ids [5, 6), pool 1: rb_ids [10, 12)
  // epoch 11: pool 0: rb_ids [7, 8), pool 2: rb_ids [100, 101)
  map<epoch_t, map<int64_t, snap_interval_set_t>> completed;
  completed[10][0].insert(5, 1);   // rb_id 5
  completed[10][1].insert(10, 2);  // rb_ids 10, 11
  completed[11][0].insert(7, 1);   // rb_id 7
  completed[11][2].insert(100, 1); // rb_id 100

  {
    auto txn = driver.make_txn();
    SnapMapper::record_completed_rollbacks(
      g_ceph_context, driver, std::move(txn), completed);
  }

  // All recorded IDs must be found
  EXPECT_TRUE(SnapMapper::is_completed_rollback(driver, 0, snapid_t(5)));
  EXPECT_TRUE(SnapMapper::is_completed_rollback(driver, 1, snapid_t(10)));
  EXPECT_TRUE(SnapMapper::is_completed_rollback(driver, 1, snapid_t(11)));
  EXPECT_TRUE(SnapMapper::is_completed_rollback(driver, 0, snapid_t(7)));
  EXPECT_TRUE(SnapMapper::is_completed_rollback(driver, 2, snapid_t(100)));

  // Unrecorded IDs must not be found
  EXPECT_FALSE(SnapMapper::is_completed_rollback(driver, 0, snapid_t(6)));
  EXPECT_FALSE(SnapMapper::is_completed_rollback(driver, 0, snapid_t(8)));
  EXPECT_FALSE(SnapMapper::is_completed_rollback(driver, 1, snapid_t(9)));
  EXPECT_FALSE(SnapMapper::is_completed_rollback(driver, 1, snapid_t(12)));
  EXPECT_FALSE(SnapMapper::is_completed_rollback(driver, 2, snapid_t(99)));
  EXPECT_FALSE(SnapMapper::is_completed_rollback(driver, 2, snapid_t(101)));
  EXPECT_FALSE(SnapMapper::is_completed_rollback(driver, 3, snapid_t(5)));
}

TEST(SnapMapperCompletedRollbacks, IdempotentRecord) {
  SimpleMapDriver driver;

  map<epoch_t, map<int64_t, snap_interval_set_t>> cr;
  cr[10][0].insert(50, 1);  // rb_id 50

  {
    auto txn = driver.make_txn();
    SnapMapper::record_completed_rollbacks(
      g_ceph_context, driver, std::move(txn), cr);
  }

  EXPECT_TRUE(SnapMapper::is_completed_rollback(driver, 0, snapid_t(50)));

  // Recording again (same data) must not corrupt the state
  {
    auto txn = driver.make_txn();
    SnapMapper::record_completed_rollbacks(
      g_ceph_context, driver, std::move(txn), cr);
  }

  EXPECT_TRUE(SnapMapper::is_completed_rollback(driver, 0, snapid_t(50)));
}
