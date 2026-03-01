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

#include <set>
#include "osd/PGBackend.h"

/**
 * MockECRecPred - configurable stub for IsPGRecoverablePredicate.
 *
 * When constructed with default arguments (k=0, m=0), always returns true
 * (original behaviour, suitable for basic tests that don't need quorum
 * checking).
 *
 * When constructed with real k and m values, implements proper quorum
 * checking: the PG is recoverable if at least k shards are available (i.e.
 * we have enough shards to reconstruct all data, since any k-of-(k+m) EC
 * scheme can recover from up to m failures).
 *
 * This enables negative testing of scenarios where too many OSDs are down
 * and the PG should be marked Incomplete.
 */
class MockECRecPred : public IsPGRecoverablePredicate {
 public:
  /**
   * @param k  Number of data chunks (0 = always-true mode)
   * @param m  Number of coding chunks (0 = always-true mode)
   */
  explicit MockECRecPred(int k = 0, int m = 0) : k(k), m(m) {}

  bool operator()(const std::set<pg_shard_t> &have) const override {
    // When k==0 fall back to always-true (backward-compatible default)
    if (k == 0) {
      return true;
    }
    // Recoverable when we have at least k shards (can tolerate up to m failures)
    return static_cast<int>(have.size()) >= k;
  }

 private:
  int k;
  int m;
};

