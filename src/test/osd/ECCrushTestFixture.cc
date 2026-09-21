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
#include "crush/crush.h" // for CRUSH_HASH_DEFAULT

void ECCrushTestFixture::pre_peering_hook()
{
  // ------------------------------------------------------------------
  // Replace the minimal CRUSH map that setup_ec_pool() builds with a
  // proper bucket hierarchy and a real EC indep rule, then remove the
  // pg_temp so CRUSH drives placement.
  //
  // Single-zone (num_zones == 1):
  //   build_simple_crush_map() gives us the standard flat hierarchy and
  //   we add:  rule "ec_rule"  (indep, TYPE_ERASURE, failure_domain=osd)
  //
  // Multi-zone (num_zones > 1):
  //   We use insert_item() with a location map (the same API that
  //   build_simple_crush_map itself uses internally) to build one
  //   single-OSD host per shard, since add_simple_stretch_rule()'s
  //   CHOOSELEAF step selects k+m distinct hosts within each zone:
  //     root "default"
  //       ├─ datacenter "zone-0"  →  host "host-0-0" … "host-0-(k+m-1)"
  //       │                            (osd.0 … osd.(k+m-1), one per host)
  //       └─ datacenter "zone-1"  →  host "host-1-0" … "host-1-(k+m-1)"
  //                                    (osd.(k+m) … osd.(2*(k+m)-1))
  //   and add:  rule "ec_stretch_rule"  via add_simple_stretch_rule()
  //
  // In both cases the pool's crush_rule is updated and the pg_temp from
  // setup_ec_pool() is removed so OSDMap consults CRUSH.
  // ------------------------------------------------------------------

  CephContext* cct = g_ceph_context;

  CrushWrapper new_crush;
  std::string rule_name;

  if (num_zones <= 1) {
    // Single-zone: reuse the standard helper.
    std::stringstream ss;
    int r = OSDMap::build_simple_crush_map(cct, new_crush, k + m, &ss);
    ceph_assert(r == 0);

    rule_name = "ec_rule";
    r = new_crush.add_simple_rule(
      rule_name, "default", "osd", "",
      "indep", pg_pool_t::TYPE_ERASURE, &ss);
    ceph_assert(r >= 0);
  } else {
    // Multi-zone: build the root/datacenter/host/osd hierarchy by hand
    // (build_simple_crush_map() only knows how to build a flat one), then
    // add an EC stretch rule that picks k+m OSDs spread across the zones.
    new_crush.create();
    new_crush.set_type_name(0, "osd");
    new_crush.set_type_name(1, "host");
    new_crush.set_type_name(8, "datacenter");
    new_crush.set_type_name(11, "root");

    int rootid = 0;
    int r = new_crush.add_bucket(0, 0, CRUSH_HASH_DEFAULT, 11, 0, NULL, NULL,
                                 &rootid);
    ceph_assert(r == 0);
    new_crush.set_item_name(rootid, "default");

    // add_simple_stretch_rule()'s CHOOSELEAF step picks num_replica_per_zone
    // (k+m) distinct buckets of the osd failure-domain type ("host") within
    // each zone, so each zone needs k+m separate one-OSD hosts rather than a
    // single host holding all of them.
    const int osds_per_zone = k + m;
    for (int zone = 0; zone < num_zones; ++zone) {
      const std::string zone_name = "zone-" + std::to_string(zone);
      for (int i = 0; i < osds_per_zone; ++i) {
        const int osd_id = zone * osds_per_zone + i;
        const std::string osd_name = "osd." + std::to_string(osd_id);
        const std::string host_name = "host-" + std::to_string(zone) + "-" +
                                      std::to_string(i);
        const std::map<std::string, std::string> loc{
          {"host", host_name},
          {"datacenter", zone_name},
          {"root", "default"}
        };
        r = new_crush.insert_item(cct, osd_id, 1.0, osd_name, loc);
        ceph_assert(r == 0);
      }
    }

    std::stringstream ss;
    rule_name = "ec_stretch_rule";
    r = new_crush.add_simple_stretch_rule(
      rule_name, "default", "datacenter", "host",
      num_zones, k + m, "", "indep", pg_pool_t::TYPE_ERASURE,
      /*force=*/false, &ss);
    ceph_assert(r >= 0);
  }

  const int ec_rule_id = new_crush.get_rule_id(rule_name);
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
