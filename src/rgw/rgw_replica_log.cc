/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 * Copyright 2013 Inktank
 */

#include "rgw_replica_log.h"
#include "cls/replica_log/cls_replica_log_client.h"
#include "rgw_rados.h"

RGWReplicaLogger::RGWReplicaLogger(RGWRados *_store) :
    cct(_store->cct), store(_store) {}

int RGWReplicaLogger::open_ioctx(librados::IoCtx& ctx, const string& pool)
{
  int r = store->rados->ioctx_create(pool.c_str(), ctx);
  if (r < 0) {
    lderr(cct) << "ERROR: could not open rados pool "
	       << pool << dendl;
  }
  return r;
}

int RGWReplicaLogger::update_bound(const string& oid, const string& pool,
                                   const string& daemon_id,
                                   const string& marker, const utime_t& time,
                                   const list<pair<string, utime_t> > *entries)
{
  cls_replica_log_progress_marker progress;
  cls_replica_log_prepare_marker(progress, daemon_id, marker, time,
                                 entries);

  librados::IoCtx ioctx;
  int r = open_ioctx(ioctx, pool);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation opw;
  cls_replica_log_update_bound(opw, progress);
  return ioctx.operate(oid, &opw);
}

int RGWReplicaLogger::delete_bound(const string& oid, const string& pool,
                                   const string& daemon_id)
{
  librados::IoCtx ioctx;
  int r = open_ioctx(ioctx, pool);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation opw;
  cls_replica_log_delete_bound(opw, daemon_id);
  return ioctx.operate(oid, &opw);
}

int RGWReplicaLogger::get_bounds(const string& oid, const string& pool,
                                 string& marker, utime_t& oldest_time,
                                 list<cls_replica_log_progress_marker>& markers)
{
  librados::IoCtx ioctx;
  int r = open_ioctx(ioctx, pool);
  if (r < 0) {
    return r;
  }

  return cls_replica_log_get_bounds(ioctx, oid, marker, oldest_time, markers);
}

void RGWReplicaLogger::get_bound_info(
    const cls_replica_log_progress_marker& progress,
    string& entity, string& marker,
    utime_t time,
    list<pair<string, utime_t> >& entries) {
  cls_replica_log_extract_marker(progress, entity, marker, time, entries);
}

RGWReplicaObjectLogger::
RGWReplicaObjectLogger(RGWRados *_store,
                       const string& _pool,
                       const string& _prefix) : RGWReplicaLogger(_store),
                       pool(_pool), prefix(_prefix) {
  if (pool.empty())
    store->get_log_pool_name(pool);
}

int RGWReplicaObjectLogger::create_log_objects(int shards)
{
  librados::IoCtx ioctx;
  int r = open_ioctx(ioctx, pool);
  if (r < 0) {
    return r;
  }
  for (int i = 0; i < shards; ++i) {
    string oid;
    get_shard_oid(i, oid);
    r = ioctx.create(oid, false);
    if (r < 0)
      return r;
  }
  return r;
}

RGWReplicaBucketLogger::RGWReplicaBucketLogger(RGWRados *_store) :
  RGWReplicaLogger(_store)
{
  store->get_log_pool_name(pool);
  prefix = _store->ctx()->_conf->rgw_replica_log_obj_prefix;
  prefix.append(".");
}
