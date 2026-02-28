// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include "osd/OSDMap.h"
#include "osd/osd_types.h"
#include <memory>
#include <vector>

/**
 * OSDMapTestHelpers - Utility functions for managing OSDMap in tests
 *
 * This class provides helper methods for test fixtures to properly manage
 * pg_pool_t configuration and acting/up sets through OSDMap. This centralizes
 * pool and PG metadata management, making tests cleaner and more maintainable.
 *
 * Key features:
 * - Add/update pools in OSDMap
 * - Set acting/up sets for PGs via pg_temp
 * - Retrieve pool configuration from OSDMap
 * - Query acting/up sets from OSDMap
 *
 * Design rationale:
 * - Pools should be stored in OSDMap, not separately in test fixtures
 * - Acting/up sets should be managed via pg_temp (standard Ceph mechanism)
 * - Helper methods provide a clean API that follows Ceph conventions
 * - All pool/PG metadata accessible through OSDMap for consistency
 */
class OSDMapTestHelpers {
public:
  /**
   * Add or update a pool in the OSDMap
   *
   * This method adds a new pool to the OSDMap or updates an existing one.
   * It properly manages the pool_max counter and pool name mapping.
   *
   * @param osdmap The OSDMap to modify
   * @param pool_id The pool ID (use -1 to auto-assign next available ID)
   * @param pool The pg_pool_t configuration
   * @param pool_name Optional pool name (defaults to "pool_<id>")
   * @return The pool ID that was assigned
   */
  static int64_t add_pool(
    OSDMap& osdmap,
    int64_t pool_id,
    const pg_pool_t& pool,
    const std::string& pool_name = "")
  {
    // Auto-assign pool ID if requested
    if (pool_id < 0) {
      pool_id = osdmap.get_pool_max() + 1;
    }
    
    // Generate pool name if not provided
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
  
  /**
   * Add or update a pool in a shared OSDMap
   *
   * Convenience overload for shared_ptr<OSDMap>
   */
  static int64_t add_pool(
    std::shared_ptr<OSDMap> osdmap,
    int64_t pool_id,
    const pg_pool_t& pool,
    const std::string& pool_name = "")
  {
    return add_pool(*osdmap, pool_id, pool, pool_name);
  }
  
  /**
   * Get pool configuration from OSDMap
   *
   * @param osdmap The OSDMap to query
   * @param pool_id The pool ID
   * @return Pointer to pg_pool_t, or nullptr if pool doesn't exist
   */
  static const pg_pool_t* get_pool(
    const OSDMap& osdmap,
    int64_t pool_id)
  {
    return osdmap.get_pg_pool(pool_id);
  }
  
  /**
   * Get pool configuration from shared OSDMap
   */
  static const pg_pool_t* get_pool(
    const std::shared_ptr<OSDMap>& osdmap,
    int64_t pool_id)
  {
    return get_pool(*osdmap, pool_id);
  }
  
  /**
   * Set acting set for a PG using pg_temp
   *
   * This method sets the acting set for a PG by configuring pg_temp.
   * In Ceph, pg_temp is the standard mechanism for overriding the
   * CRUSH-computed acting set.
   *
   * @param osdmap The OSDMap to modify
   * @param pgid The PG ID
   * @param acting The acting set (vector of OSD IDs)
   */
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
      // Convert to mempool vector and set pg_temp
      mempool::osdmap::vector<int32_t> temp_acting;
      for (int osd : acting) {
        temp_acting.push_back(osd);
      }
      inc.new_pg_temp[pgid] = temp_acting;
    }
    
    osdmap.apply_incremental(inc);
  }
  
  /**
   * Set acting set for a PG in shared OSDMap
   */
  static void set_pg_acting(
    std::shared_ptr<OSDMap> osdmap,
    pg_t pgid,
    const std::vector<int>& acting)
  {
    set_pg_acting(*osdmap, pgid, acting);
  }
  
  /**
   * Get acting set for a PG from pg_temp
   *
   * @param osdmap The OSDMap to query
   * @param pgid The PG ID
   * @param acting Output vector for acting set
   * @return true if pg_temp exists for this PG, false otherwise
   */
  static bool get_pg_acting(
    const OSDMap& osdmap,
    pg_t pgid,
    std::vector<int>& acting)
  {
    acting.clear();
    
    // Use pg_to_acting_osds which properly handles pg_temp
    int primary;
    osdmap.pg_to_acting_osds(pgid, &acting, &primary);
    
    return !acting.empty();
  }
  
  /**
   * Get acting set for a PG from shared OSDMap
   */
  static bool get_pg_acting(
    const std::shared_ptr<OSDMap>& osdmap,
    pg_t pgid,
    std::vector<int>& acting)
  {
    return get_pg_acting(*osdmap, pgid, acting);
  }
  
  /**
   * Set primary for a PG using primary_temp
   *
   * @param osdmap The OSDMap to modify
   * @param pgid The PG ID
   * @param primary The primary OSD ID (-1 to remove primary_temp)
   */
  static void set_pg_primary(
    OSDMap& osdmap,
    pg_t pgid,
    int primary)
  {
    OSDMap::Incremental inc(osdmap.get_epoch() + 1);
    inc.fsid = osdmap.get_fsid();
    
    // Set primary_temp (-1 to remove)
    inc.new_primary_temp[pgid] = primary;
    
    osdmap.apply_incremental(inc);
  }
  
  /**
   * Set primary for a PG in shared OSDMap
   */
  static void set_pg_primary(
    std::shared_ptr<OSDMap> osdmap,
    pg_t pgid,
    int primary)
  {
    set_pg_primary(*osdmap, pgid, primary);
  }
  
  /**
   * Get primary for a PG from primary_temp
   *
   * @param osdmap The OSDMap to query
   * @param pgid The PG ID
   * @param primary Output for primary OSD ID
   * @return true if primary_temp exists for this PG, false otherwise
   */
  static bool get_pg_primary(
    const OSDMap& osdmap,
    pg_t pgid,
    int& primary)
  {
    // Use pg_to_acting_osds which properly handles primary_temp
    std::vector<int> acting;
    osdmap.pg_to_acting_osds(pgid, &acting, &primary);
    
    return primary >= 0;
  }
  
  /**
   * Get primary for a PG from shared OSDMap
   */
  static bool get_pg_primary(
    const std::shared_ptr<OSDMap>& osdmap,
    pg_t pgid,
    int& primary)
  {
    return get_pg_primary(*osdmap, pgid, primary);
  }
  
  /**
   * Create a basic EC pool configuration
   *
   * Helper to create a standard EC pool configuration for tests.
   *
   * @param k Number of data chunks
   * @param m Number of coding chunks
   * @param stripe_width Stripe width in bytes
   * @param pool_id Pool ID (for setting last_change epoch)
   * @return Configured pg_pool_t
   */
  static pg_pool_t create_ec_pool(
    int k,
    int m,
    uint64_t stripe_width,
    int64_t pool_id = 0)
  {
    pg_pool_t pool;
    pool.type = pg_pool_t::TYPE_ERASURE;
    pool.size = k + m;
    pool.min_size = k;
    pool.crush_rule = 0;
    pool.set_flag(pg_pool_t::FLAG_EC_OVERWRITES);
    pool.set_flag(pg_pool_t::FLAG_EC_OPTIMIZATIONS);
    pool.erasure_code_profile = "default";
    pool.stripe_width = stripe_width;
    
    return pool;
  }
  
  /**
   * Create a basic replicated pool configuration
   *
   * Helper to create a standard replicated pool configuration for tests.
   *
   * @param size Number of replicas
   * @param min_size Minimum number of replicas
   * @param pool_id Pool ID (for setting last_change epoch)
   * @return Configured pg_pool_t
   */
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
  
  /**
   * Set up a complete acting set for an EC PG
   *
   * This is a convenience method that sets up both the acting set
   * and primary for an EC PG in one call.
   *
   * @param osdmap The OSDMap to modify
   * @param pgid The PG ID
   * @param k Number of data chunks
   * @param m Number of coding chunks
   * @param primary_shard Which shard should be primary (default: 0)
   */
  static void setup_ec_pg(
    OSDMap& osdmap,
    pg_t pgid,
    int k,
    int m,
    int primary_shard = 0)
  {
    // Create acting set with all shards
    std::vector<int> acting;
    for (int i = 0; i < k + m; i++) {
      acting.push_back(i);
    }
    
    // Set acting set
    set_pg_acting(osdmap, pgid, acting);
    
    // Set primary
    set_pg_primary(osdmap, pgid, primary_shard);
  }
  
  /**
   * Set up a complete acting set for an EC PG in shared OSDMap
   */
  static void setup_ec_pg(
    std::shared_ptr<OSDMap> osdmap,
    pg_t pgid,
    int k,
    int m,
    int primary_shard = 0)
  {
    setup_ec_pg(*osdmap, pgid, k, m, primary_shard);
  }

  /**
   * Clear a flag on an existing pool in the OSDMap
   *
   * Retrieves the current pool configuration, clears the specified flag,
   * and applies the change via OSDMap::Incremental.
   *
   * @param osdmap The OSDMap to modify
   * @param pool_id The pool ID
   * @param flag The flag to clear (e.g. pg_pool_t::FLAG_EC_OPTIMIZATIONS)
   */
  static void clear_pool_flag(
    OSDMap& osdmap,
    int64_t pool_id,
    uint64_t flag)
  {
    const pg_pool_t* existing = osdmap.get_pg_pool(pool_id);
    ceph_assert(existing != nullptr);

    // Copy the pool, unset the flag, then apply via incremental
    pg_pool_t updated = *existing;
    updated.unset_flag(flag);

    OSDMap::Incremental inc(osdmap.get_epoch() + 1);
    inc.fsid = osdmap.get_fsid();
    inc.new_pools[pool_id] = updated;
    osdmap.apply_incremental(inc);
  }

  /**
   * Clear a flag on an existing pool in a shared OSDMap
   */
  static void clear_pool_flag(
    std::shared_ptr<OSDMap> osdmap,
    int64_t pool_id,
    uint64_t flag)
  {
    clear_pool_flag(*osdmap, pool_id, flag);
  }
};

