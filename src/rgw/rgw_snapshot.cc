/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 * Copyright 2013 Inktank
 */

#include <string>
#include <map>

#include "common/errno.h"
#include "common/ceph_json.h"
#include "rgw_rados.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h"

#include "include/types.h"
#include "rgw_snapshot.h"
#include "rgw_user.h"
#include "rgw_string.h"

// until everything is moved from rgw_common
#include "rgw_common.h"

#include "cls/user/cls_user_types.h"


/*
  RGW Snapshots are implemented using rados snapshots on all pools used
  by the zone.  This is easy to implement, but has a few complications:
  * RGW snapshots are not atomic.  It takes 10+ seconds to snapshot the zone
    pools sequentially.  I believe that by taking snapshots in the proper 
    order, we can avoid problems, but this needs some stress testing.
  * Mixing RGW snapshots and rados snapshots on the RGW pools is confusing.
    It's not recommended.
  * Because we're snapshotting all pools in a zone, some end user operations
    may not be intuitive.  For example, a user given access to a bucket will
    only be able to access snapshots created after access was granted.

*/



RGWSnapshot::RGWSnapshot( CephContext *_cct, RGWRados *_store, const string& _snap_name)
{
  cct = _cct;
  store = _store;
  snap_name = _snap_name;
}

int RGWSnapshot::get_snapshots( CephContext *cct, RGWRados *store,
    list<string> pools, list<RGWSnapshot>& snaps)
{
  map<string, RGWSnapshot> pool_snaps_map;

  bool firstpass = true;
  string first_pool_name;
  for( list<string>::iterator pool_name = pools.begin();
       pool_name != pools.end(); ++pool_name) {
    vector<librados::snap_t> pool_snaps_vec;
    map<string, int> missing_snaps;
    librados::IoCtx io_ctx;

    // Used to remove snapshots that don't exist on all pools
    if( !firstpass) {
      for( map<string, RGWSnapshot>::iterator mi = pool_snaps_map.begin();
           mi != pool_snaps_map.end(); ++mi) {
        missing_snaps[mi->first] = 0;
      }
    }

    int r = store->rados->ioctx_create(pool_name->c_str(), io_ctx);
    if (r < 0) {
      cerr << "can't create IoCtx for pool " << *pool_name << std::endl;
      return r;
    }

    io_ctx.snap_list(&pool_snaps_vec);
    for (vector<librados::snap_t>::iterator i = pool_snaps_vec.begin();
         i != pool_snaps_vec.end();
         ++i) {
      string s;
      time_t t;

      if (io_ctx.snap_get_name(*i, &s) < 0)
        continue;
      if (io_ctx.snap_get_stamp(*i, &t) < 0)
        continue;

      // We want to maintain a list of snapshots that exist on all
      // pools.  Initialize the map on the first pool, 
      if( firstpass) {
        RGWSnapshot pool_snap( cct, store, s);
        pool_snap.snap_created = t;
        pool_snap.snap_num = *i;

        pool_snaps_map.insert( std::pair<string,RGWSnapshot>(s,pool_snap));

        first_pool_name = *pool_name;
      } else {
        try {
          RGWSnapshot pool_snap = pool_snaps_map.at(s);
          long long time_delta = abs( (long long)pool_snap.snap_created - (long long)t);
          if( time_delta > 60) {
            cerr << "rados snapshot creation time on " << *i << " is more than 60s offset from the .rgw root pool.  Ingoring snapshot." << std::endl;
            pool_snaps_map.erase( s);
          }
           
          missing_snaps.erase(s);
        } catch( out_of_range e) {
          cerr << "Found rados snapshots that aren't RGW snapshots.  It's a bad idea to mix radosgw-admin mksnap and rados mksnap on the RGW pools." << std::endl;
          cerr << "To clean up this warning, manually remove snapshot " << s << " on pool " << *pool_name << std::endl;
          // If it's not in the pool_snaps_map, then ignore it.
        }
      }
    }

    if( !missing_snaps.empty()) {
      cerr << "Found rados snapshots that aren't RGW snapshots.  It's a bad idea to mix radosgw-admin mksnap and rados mksnap on the RGW pools." << std::endl;
      for( map<string, int>::iterator mi = missing_snaps.begin();
           mi != missing_snaps.end(); ++mi) {
        cerr << "To clean up this warning, manually remove snapshot " << mi->first << " on pool " << first_pool_name << std::endl;
        pool_snaps_map.erase( mi->first);
      }
    }

    firstpass = false;
  }

  for( map<string, RGWSnapshot>::iterator mi = pool_snaps_map.begin();
       mi != pool_snaps_map.end(); ++mi) {
    snaps.push_back( mi->second);
  }

  return( 0);
}

// Return a list of RADOS Pools used by RGW.
// The list is in order for creating snapshots.
// The pools listed here are not guarenteed to exist.  
// You probably want get_rgw_pools()
int RGWSnapshot::get_rados_pools( CephContext *cct, RGWRados *store, list<string>& pools)
{
  RGWRegion region;
  int ret = region.init(g_ceph_context, store);
  if (ret < 0) {
    cerr << "failed to init region: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  RGWZoneParams zone;
  ret = zone.init(g_ceph_context, store, region);
  if (ret < 0) {
    cerr << "unable to initialize zone: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  string placement_target = region.default_placement;

  pools.push_back( zone.domain_root.data_pool);
  pools.push_back( zone.control_pool.data_pool);
  pools.push_back( zone.gc_pool.data_pool);
  pools.push_back( zone.log_pool.data_pool);
  pools.push_back( zone.intent_log_pool.data_pool);
  pools.push_back( zone.usage_log_pool.data_pool);
  pools.push_back( zone.user_keys_pool.data_pool);
  pools.push_back( zone.user_email_pool.data_pool);
  pools.push_back( zone.user_swift_pool.data_pool);
  pools.push_back( zone.user_uid_pool.data_pool);
  pools.push_back( zone.placement_pools[placement_target].index_pool);
  pools.push_back( zone.placement_pools[placement_target].data_pool);

  return 0;
}

// Return a list of RADOS Pools used by RGW.
// The list is in order for creating snapshots.
// The pools returned all exist.
int RGWSnapshot::get_rgw_pools( CephContext *cct, RGWRados *store, list<string>& pools)
{
  list<string> rgw_pools;
  list<string> all_pools;
  librados::Rados rados;

  int ret = RGWSnapshot::get_rados_pools( cct, store, rgw_pools);
  if (ret < 0)
    return ret;

  ret = store->rados->pool_list( all_pools);
  if (ret < 0)
    return ret;

  // I know this is bad form,
  // but I don't know enough std::list to make it less stupid
  for( list<string>::iterator rgwp = rgw_pools.begin();
       rgwp != rgw_pools.end(); ++rgwp) {
    for( list<string>::iterator ap = all_pools.begin();
         ap != all_pools.end(); ++ap) {
      if( *rgwp == *ap) {
        pools.push_back( *rgwp);
        continue;
      }
    }
  }

  return( 0);
}


int RGWSnapshot::create()
{
  list<string> snap_pools;
  list<RGWSnapshot> snaps;

  int ret = RGWSnapshot::get_rgw_pools( cct, store, snap_pools);
  if (ret < 0)
    return ret;

  ret = RGWSnapshot::get_snapshots( cct, store, snap_pools, snaps);
  if (ret < 0)
    return ret;

  for( list<RGWSnapshot>::iterator siter = snaps.begin();
       siter != snaps.end(); ++siter) {
    if( snap_name == siter->snap_name) {
      cerr << "snapshot " << snap_name << " already exists" << std::endl;
      return -EEXIST;
    }
  }

  for( list<string>::iterator snap_pool = snap_pools.begin();
       snap_pool != snap_pools.end(); ++snap_pool) {
    librados::IoCtx io_ctx;

    int r = store->rados->ioctx_create(snap_pool->c_str(), io_ctx);
    if (r < 0) {
      cerr << "can't create IoCtx for pool " << *snap_pool << std::endl;
      return r;
    }

    r = io_ctx.snap_create(snap_name.c_str());
    if (r < 0) {
      cerr << "can't mksnap for pool " << *snap_pool << std::endl;
      if( r == -EEXIST) {
        cerr << "This probably means your using radosgw-admin mksnap and rados mksnap on RGW pools.  Don't do that." << std::endl;
      }
      return r;
    }
  }

  return( 0);
}
