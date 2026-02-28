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

// MockECReadPred - simple stub for IsPGReadablePredicate
// Warning - this always returns true. This means we cannot test scenarios
// where there are too many OSDs down and the PG should be incomplete
class MockECReadPred : public IsPGReadablePredicate {
 public:
  MockECReadPred() {}
  bool operator()(const std::set<pg_shard_t> &_have) const override {
    return true;
  }
};

