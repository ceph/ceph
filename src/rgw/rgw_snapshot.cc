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




RGWSnapshot::RGWSnapshot( CephContext *_cct, RGWRados *_store, const string& _snap_name)
{
  cct = _cct;
  store = _store;
  snap_name = _snap_name;
}

int RGWSnapshot::get_snapshots( CephContext *cct, RGWRados *store,
    list<string> pools, list<RGWSnapshot>& snaps)
{
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


int RGWSnapshot::make()
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

  formatter->open_object_section("mksnap");
  encode_json("snap_pools", snap_pools, formatter);
  encode_json("snaps", snaps, formatter);
  formatter->close_section();
  formatter->flush(cout);
  cout << std::endl;

  for( list<string>::iterator snap_pool = snap_pools.begin();
       snap_pool != snap_pools.end(); ++snap_pool) {
    cerr << "snapshot " << *snap_pool << std::endl;

    librados::IoCtx io_ctx;

    int r = store->rados->ioctx_create(snap_pool->c_str(), io_ctx);
    if (r < 0) {
      cerr << "can't create IoCtx for pool " << *snap_pool << std::endl;
      return r;
    }

    r = io_ctx.snap_create(snap_name.c_str());
    if (r < 0) {
      cerr << "can't mksnap for pool " << *snap_pool << std::endl;
      return r;
    }
  }

  return( 0);
}

bool RGWSnapshot::exists()
{
  return( false);
}

void RGWSnapshot::set_formatter( Formatter *_formatter)
{
  formatter = _formatter;
}
