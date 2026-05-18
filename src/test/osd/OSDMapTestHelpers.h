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
#include <string>
#include <vector>
#include "osd/OSDMap.h"
#include "osd/osd_types.h"

// Utility functions for managing OSDMap state in tests.
// (Previously in OSDMapTestHelpers.h — embedded here as the sole user.)
class OSDMapTestHelpers {
public:
  // Add or update a pool in the OSDMap. Pass pool_id=-1 to auto-assign.
  static int64_t add_pool(
    OSDMap& osdmap,
    int64_t pool_id,
    const pg_pool_t& pool,
    const std::string& pool_name = "")
  {
    if (pool_id < 0) {
      pool_id = osdmap.get_pool_max() + 1;
    }
    
    std::string name = pool_name.empty() ?
      ("pool_" + std::to_string(pool_id)) : pool_name;
    
    // Use OSDMap::Incremental to properly add pool and pool name
    // This ensures both pools map and pool_name map are updated correctly
    OSDMap::Incremental inc(osdmap.get_epoch() + 1);
    inc.fsid = osdmap.get_fsid();
    inc.new_pools[pool_id] = pool;
    inc.new_pool_names[pool_id] = name;
    
    osdmap.apply_incremental(inc);
    
    return pool_id;
  }
  
  static int64_t add_pool(
    std::shared_ptr<OSDMap> osdmap,
    int64_t pool_id,
    const pg_pool_t& pool,
    const std::string& pool_name = "")
  {
    return add_pool(*osdmap, pool_id, pool, pool_name);
  }
  
  static const pg_pool_t* get_pool(
    const OSDMap& osdmap,
    int64_t pool_id)
  {
    return osdmap.get_pg_pool(pool_id);
  }
  
  static const pg_pool_t* get_pool(
    const std::shared_ptr<OSDMap>& osdmap,
    int64_t pool_id)
  {
    return get_pool(*osdmap, pool_id);
  }
  
  // Set acting set for a PG using pg_temp (standard Ceph mechanism for overriding CRUSH).
  // For EC pools with nonprimary_shards optimization, pg_temp must be stored in
  // "primaryfirst" order (primary-capable shards first). This simulates what the
  // monitor does in production when initially setting up pg_temp.
  static void set_pg_acting(
    OSDMap& osdmap,
    pg_t pgid,
    const std::vector<int>& acting)
  {
    OSDMap::Incremental inc(osdmap.get_epoch() + 1);
    inc.fsid = osdmap.get_fsid();
    
    if (acting.empty()) {
      // Empty acting set means remove pg_temp
      inc.new_pg_temp[pgid] = mempool::osdmap::vector<int32_t>();
    } else {
      // For EC pools with optimizations, transform to primaryfirst order.
      // This is used for initial setup. For dynamic changes during peering,
      // the test should let peering detect invalid primaries and request
      // corrections via queue_want_pg_temp().
      std::vector<int> transformed_acting = acting;
      const pg_pool_t* pool = osdmap.get_pg_pool(pgid.pool());
      if (pool && pool->allows_ecoptimizations()) {
        transformed_acting = osdmap.pgtemp_primaryfirst(*pool, acting);
      }
      
      mempool::osdmap::vector<int32_t> temp_acting;
      for (int osd : transformed_acting) {
        temp_acting.push_back(osd);
      }
      inc.new_pg_temp[pgid] = temp_acting;
    }
    
    osdmap.apply_incremental(inc);
  }
  
  static void set_pg_acting(
    std::shared_ptr<OSDMap> osdmap,
    pg_t pgid,
    const std::vector<int>& acting)
  {
    set_pg_acting(*osdmap, pgid, acting);
  }
  
  static bool get_pg_acting(
    const OSDMap& osdmap,
    pg_t pgid,
    std::vector<int>& acting)
  {
    acting.clear();
    int primary;
    osdmap.pg_to_acting_osds(pgid, &acting, &primary);
    return !acting.empty();
  }
  
  static bool get_pg_acting(
    const std::shared_ptr<OSDMap>& osdmap,
    pg_t pgid,
    std::vector<int>& acting)
  {
    return get_pg_acting(*osdmap, pgid, acting);
  }
  
  static void set_pg_acting_primary(
    OSDMap& osdmap,
    pg_t pgid,
    int primary)
  {
    OSDMap::Incremental inc(osdmap.get_epoch() + 1);
    inc.fsid = osdmap.get_fsid();
    inc.new_primary_temp[pgid] = primary;
    osdmap.apply_incremental(inc);
  }
  
  static void set_pg_acting_primary(
    std::shared_ptr<OSDMap> osdmap,
    pg_t pgid,
    int primary)
  {
    set_pg_acting_primary(*osdmap, pgid, primary);
  }
  
  static bool get_pg_acting_primary(
    const OSDMap& osdmap,
    pg_t pgid,
    int& primary)
  {
    std::vector<int> acting;
    osdmap.pg_to_acting_osds(pgid, &acting, &primary);
    return primary >= 0;
  }
  
  static bool get_pg_acting_primary(
    const std::shared_ptr<OSDMap>& osdmap,
    pg_t pgid,
    int& primary)
  {
    return get_pg_acting_primary(*osdmap, pgid, primary);
  }
  
  static pg_pool_t create_ec_pool(
    int k,
    int m,
    uint64_t stripe_width,
    uint64_t flags,
    int64_t pool_id = 0,
    int num_zones = 1)
  {
    ceph_assert(num_zones > 0);

    pg_pool_t pool;
    pool.type = pg_pool_t::TYPE_ERASURE;

    // size = num_zones * (k + m)
    pool.size = num_zones * (k + m);
    pool.opts.set(pool_opts_t::NUM_ZONES, num_zones);
    
    // For multi-zone configurations, set min_size to allow up to m failures
    // min_size = num_zones * (k+m) - m
    pool.min_size = num_zones * (k + m) - m;
    pool.crush_rule = 0;
    pool.erasure_code_profile = "default";
    pool.stripe_width = stripe_width;
    
    // Set flags as specified by caller
    pool.flags = flags;
    
    // Only set nonprimary_shards if OPTIMIZATIONS flag is set
    if (flags & pg_pool_t::FLAG_EC_OPTIMIZATIONS) {
      // Mark shards 1 to k-1 (inclusive) as nonprimary
      // Shard 0 can be primary, shards k to k+m-1 (coding shards) can be primary
      for (int i = 1; i < k; i++) {
        pool.nonprimary_shards.insert(shard_id_t(i));
      }
    }
    
    return pool;
  }
  
  static pg_pool_t create_replicated_pool(
    int size,
    int min_size,
    int64_t pool_id = 0)
  {
    pg_pool_t pool;
    pool.type = pg_pool_t::TYPE_REPLICATED;
    pool.size = size;
    pool.min_size = min_size;
    pool.crush_rule = 0;
    
    return pool;
  }
  
  static void setup_ec_pg(
    OSDMap& osdmap,
    pg_t pgid,
    int k,
    int m,
    int primary_shard = 0)
  {
    std::vector<int> acting;
    for (int i = 0; i < k + m; i++) {
      acting.push_back(i);
    }
    set_pg_acting(osdmap, pgid, acting);
    // Don't set primary_temp for EC pools - let OSDMap determine the primary
    // based on the pool's nonprimary_shards configuration
    // set_pg_acting_primary(osdmap, pgid, primary_shard);
  }
  
  static void setup_ec_pg(
    std::shared_ptr<OSDMap> osdmap,
    pg_t pgid,
    int k,
    int m,
    int primary_shard = 0)
  {
    setup_ec_pg(*osdmap, pgid, k, m, primary_shard);
  }

  // Copy the pool, unset the flag, then apply via incremental.
  static void clear_pool_flag(
    OSDMap& osdmap,
    int64_t pool_id,
    uint64_t flag)
  {
    const pg_pool_t* existing = osdmap.get_pg_pool(pool_id);
    ceph_assert(existing != nullptr);

    pg_pool_t updated = *existing;
    updated.unset_flag(flag);

    OSDMap::Incremental inc(osdmap.get_epoch() + 1);
    inc.fsid = osdmap.get_fsid();
    inc.new_pools[pool_id] = updated;
    osdmap.apply_incremental(inc);
  }

  static void clear_pool_flag(
    std::shared_ptr<OSDMap> osdmap,
    int64_t pool_id,
    uint64_t flag)
  {
    clear_pool_flag(*osdmap, pool_id, flag);
  }

  /**
   * Set the min_size for a pool.
   * Creates a new epoch.
   *
   * @param osdmap The OSDMap to modify
   * @param pool_id The pool ID
   * @param new_min_size The new min_size value
   */
  static void set_pool_min_size(OSDMap& osdmap, int64_t pool_id, unsigned new_min_size)
  {
    const pg_pool_t* existing = osdmap.get_pg_pool(pool_id);
    ceph_assert(existing != nullptr);

    pg_pool_t updated = *existing;
    updated.min_size = new_min_size;

    OSDMap::Incremental inc(osdmap.get_epoch() + 1);
    inc.fsid = osdmap.get_fsid();
    inc.new_pools[pool_id] = updated;
    osdmap.apply_incremental(inc);
  }

  static void set_pool_min_size(std::shared_ptr<OSDMap> osdmap, int64_t pool_id, unsigned new_min_size)
  {
    set_pool_min_size(*osdmap, pool_id, new_min_size);
  }

  // OSD state manipulation methods
  
  /**
   * Mark an OSD as down (exists but not UP) in the OSDMap.
   * Creates a new epoch.
   *
   * @param osdmap The OSDMap to modify
   * @param osd_id The OSD to mark as down
   */
  static void mark_osd_down(OSDMap& osdmap, int osd_id)
  {
    OSDMap::Incremental inc(osdmap.get_epoch() + 1);
    inc.fsid = osdmap.get_fsid();
    inc.new_state[osd_id] = CEPH_OSD_EXISTS;  // Mark as down (exists but not UP)

    // Preserve xinfo features when marking OSD down
    // This is critical for peering to work correctly with feature checks
    const osd_xinfo_t& existing_xinfo = osdmap.get_xinfo(osd_id);
    inc.new_xinfo[osd_id] = existing_xinfo;

    osdmap.apply_incremental(inc);
  }
  
  static void mark_osd_down(std::shared_ptr<OSDMap> osdmap, int osd_id)
  {
    mark_osd_down(*osdmap, osd_id);
  }
  
  /**
   * Mark an OSD as up in the OSDMap.
   * Creates a new epoch.
   *
   * @param osdmap The OSDMap to modify
   * @param osd_id The OSD to mark as up
   */
  static void mark_osd_up(OSDMap& osdmap, int osd_id)
  {
    OSDMap::Incremental inc(osdmap.get_epoch() + 1);
    inc.fsid = osdmap.get_fsid();
    inc.new_state[osd_id] = CEPH_OSD_EXISTS | CEPH_OSD_UP;

    // Preserve xinfo features when marking OSD up
    // This is critical for peering to work correctly with feature checks
    const osd_xinfo_t& existing_xinfo = osdmap.get_xinfo(osd_id);
    inc.new_xinfo[osd_id] = existing_xinfo;

    osdmap.apply_incremental(inc);
  }
  
  static void mark_osd_up(std::shared_ptr<OSDMap> osdmap, int osd_id)
  {
    mark_osd_up(*osdmap, osd_id);
  }
  
  /**
   * Mark multiple OSDs as down in the OSDMap.
   * Creates a new epoch.
   *
   * @param osdmap The OSDMap to modify
   * @param osd_ids The OSDs to mark as down
   */
  static void mark_osds_down(OSDMap& osdmap, const std::vector<int>& osd_ids)
  {
    OSDMap::Incremental inc(osdmap.get_epoch() + 1);
    inc.fsid = osdmap.get_fsid();
    for (int osd_id : osd_ids) {
      inc.new_state[osd_id] = CEPH_OSD_EXISTS;  // Mark as down (exists but not UP)

      // Preserve xinfo features when marking OSD down
      // This is critical for peering to work correctly with feature checks
      const osd_xinfo_t& existing_xinfo = osdmap.get_xinfo(osd_id);
      inc.new_xinfo[osd_id] = existing_xinfo;
    }
    osdmap.apply_incremental(inc);
  }
  
  static void mark_osds_down(std::shared_ptr<OSDMap> osdmap, const std::vector<int>& osd_ids)
  {
    mark_osds_down(*osdmap, osd_ids);
  }
  
  /**
   * Advance to a new epoch without changing OSD states.
   * Useful for testing re-peering scenarios.
   *
   * @param osdmap The OSDMap to modify
   */
  static void advance_epoch(OSDMap& osdmap)
  {
    OSDMap::Incremental inc(osdmap.get_epoch() + 1);
    inc.fsid = osdmap.get_fsid();
    osdmap.apply_incremental(inc);
  }
  
  static void advance_epoch(std::shared_ptr<OSDMap> osdmap)
  {
    advance_epoch(*osdmap);
  }
};
