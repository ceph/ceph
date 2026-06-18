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

#include "test/osd/ECCrushTestFixture.h"
#include "osd/OSDMap.h"
#include "crush/CrushWrapper.h"

void ECCrushTestFixture::pre_peering_hook()
{
  // ------------------------------------------------------------------
  // Replace the minimal CRUSH map that setup_ec_pool() builds with a
  // proper bucket hierarchy and a real EC indep rule, then remove the
  // pg_temp so CRUSH drives placement.
  //
  // build_simple_crush_map() creates:
  //   root "default"
  //     └─ rack "localrack"
  //          └─ host "localhost"
  //               └─ osd.0 … osd.(k+m-1)
  //   rule 0: "replicated_rule" (firstn, TYPE_REPLICATED)
  //
  // We add:
  //   rule 1: "ec_rule"  (indep, TYPE_ERASURE, failure_domain=osd)
  //
  // The pool's crush_rule is updated to the new EC rule.
  // The pg_temp from setup_ec_pool() is removed so OSDMap consults CRUSH.
  // ------------------------------------------------------------------

  CephContext* cct = g_ceph_context;

  // Build a fresh CRUSH map for k+m OSDs.
  CrushWrapper new_crush;
  {
    std::stringstream ss;
    int r = OSDMap::build_simple_crush_map(cct, new_crush, k + m, &ss);
    ceph_assert(r == 0);

    r = new_crush.add_simple_rule(
      "ec_rule", "default", "osd", "",
      "indep", pg_pool_t::TYPE_ERASURE, &ss);
    ceph_assert(r >= 0);
  }
  const int ec_rule_id = new_crush.get_rule_id("ec_rule");
  ceph_assert(ec_rule_id >= 0);
  new_crush.finalize();

  // Apply the new CRUSH map.
  {
    OSDMap::Incremental crush_inc(osdmap->get_epoch() + 1);
    crush_inc.fsid = osdmap->get_fsid();
    new_crush.encode(crush_inc.crush, CEPH_FEATURES_SUPPORTED_DEFAULT);
    osdmap->apply_incremental(crush_inc);
  }

  // Point the pool at the new EC rule.
  {
    const pg_pool_t* existing = osdmap->get_pg_pool(pool_id);
    ceph_assert(existing != nullptr);
    pg_pool_t updated = *existing;
    updated.crush_rule = ec_rule_id;

    OSDMap::Incremental pool_inc(osdmap->get_epoch() + 1);
    pool_inc.fsid = osdmap->get_fsid();
    pool_inc.new_pools[pool_id] = updated;
    osdmap->apply_incremental(pool_inc);
  }

  // Remove the pg_temp that setup_ec_pool() installed so that CRUSH
  // determines the acting set for this PG.
  {
    OSDMap::Incremental rm_inc(osdmap->get_epoch() + 1);
    rm_inc.fsid = osdmap->get_fsid();
    rm_inc.new_pg_temp[pgid] = mempool::osdmap::vector<int32_t>();
    osdmap->apply_incremental(rm_inc);
  }
}
