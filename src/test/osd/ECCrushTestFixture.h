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

#include "test/osd/ECPeeringTestFixture.h"

/**
 * ECCrushTestFixture - EC test fixture with a proper CRUSH map and a real
 * EC indep rule attached to the pool.
 *
 * ECPeeringTestFixture uses a minimal CRUSH map (item names only, no bucket
 * hierarchy, crush_rule = 0 which is unset) and overrides placement with a
 * pg_upmap.  This fixture builds on top of that by replacing the CRUSH map
 * with a complete bucket hierarchy (root → rack → host → osds) plus a named
 * EC indep rule, and pointing the pool at that rule.
 *
 * The pg_upmap is kept intact so the shard == osd invariant is preserved
 * throughout.  Tests that inherit from this fixture exercise the real CRUSH
 * rule evaluation path while remaining simple to reason about.
 *
 * Topology:
 *   root "default"
 *     └─ rack "localrack"
 *          └─ host "localhost"
 *               └─ osd.0 … osd.(k+m-1)
 *   rule "ec_rule"  type erasure  mode indep  failure_domain osd
 */
class ECCrushTestFixture : public ECPeeringTestFixture {
public:
  ECCrushTestFixture() : ECPeeringTestFixture() {}

protected:
  bool use_upmap() const override { return false; }
  void pre_peering_hook() override;
};
