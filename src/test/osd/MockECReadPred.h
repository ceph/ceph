// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 IBM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library Public License as published by
 * the Free Software Foundation; either version 2, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library Public License for more details.
 *
 */

#pragma once

#include <set>
#include "osd/PGBackend.h"

/**
 * MockECReadPred - configurable stub for IsPGReadablePredicate.
 *
 * When constructed with default arguments (k=0, m=0), always returns true
 * (original behaviour, suitable for basic tests that don't need quorum
 * checking).
 *
 * When constructed with real k and m values, implements proper quorum
 * checking: the PG is readable if at least k shards are available (i.e.
 * we have enough data shards to reconstruct the object without needing
 * any coding shards).
 *
 * This enables negative testing of scenarios where too many OSDs are down
 * and the PG should be unreadable.
 */
class MockECReadPred : public IsPGReadablePredicate {
 public:
  /**
   * @param k  Number of data chunks (0 = always-true mode)
   * @param m  Number of coding chunks (unused in read predicate, kept for
   *           symmetry with MockECRecPred)
   */
  explicit MockECReadPred(int k = 0, int m = 0) : k(k), m(m) {}

  bool operator()(const std::set<pg_shard_t> &have) const override {
    // When k==0 fall back to always-true (backward-compatible default)
    if (k == 0) {
      return true;
    }
    // Readable when we have at least k shards available
    return static_cast<int>(have.size()) >= k;
  }

 private:
  int k;
  int m;
};

