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
 * EC rule attached to the pool.
 *
 * ECPeeringTestFixture uses a minimal CRUSH map (item names only, no bucket
 * hierarchy, crush_rule = 0 which is unset) and overrides placement with a
 * pg_upmap. This fixture replaces that with a complete CRUSH hierarchy and
 * updates the pool to use the resulting rule.
 *
 * For `num_zones == 1`, the topology is:
 *   root "default"
 *     └─ rack "localrack"
 *          └─ host "localhost"
 *               └─ osd.0 … osd.(k+m-1)
 *   rule "ec_rule"  type erasure  mode indep  failure_domain osd
 *
 * For `num_zones > 1`, the topology is:
 *   root "default"
 *     ├─ datacenter "zone-0"  →  host "host-0"  →  local OSDs
 *     └─ datacenter "zone-1"  →  host "host-1"  →  remote OSDs
 *   rule "ec_stretch_rule"  type erasure  mode indep
 *
 * The pg_upmap is kept disabled so CRUSH determines placement directly.
 * Tests that inherit from this fixture exercise the real CRUSH rule
 * evaluation path while remaining simple to reason about.
 */
class ECCrushTestFixture : public ECPeeringTestFixture {
public:
  ECCrushTestFixture() : ECPeeringTestFixture() {}

protected:
  bool use_upmap() const override { return false; }
  void pre_peering_hook() override;
};
