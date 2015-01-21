/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 * Copyright 2013 Inktank
 */

#include "common/ceph_json.h"

#include "rgw_replica_log.h"
#include "cls/replica_log/cls_replica_log_client.h"
#include "rgw_rados.h"


void RGWReplicaBounds::dump(Formatter *f) const
{
  encode_json("marker", marker, f);
  encode_json("oldest_time", oldest_time, f);
  encode_json("markers", markers, f);
}

void RGWReplicaBounds::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("marker", marker, obj);
  JSONDecoder::decode_json("oldest_time", oldest_time, obj);
  JSONDecoder::decode_json("markers", markers, obj);
}

RGWReplicaLogger::RGWReplicaLogger(RGWRados *_store) :
    cct(_store->cct), store(_store) {}

int RGWReplicaLogger::open_ioctx(librados::IoCtx& ctx, const string& pool)
{
  int r = store->rados->ioctx_create(pool.c_str(), ctx);
  if (r == -ENOENT) {
    rgw_bucket p(pool.c_str());
    r = store->create_pool(p);
    if (r < 0)
      return r;

    // retry
    r = store->rados->ioctx_create(pool.c_str(), ctx);
  }
  if (r < 0) {
    lderr(cct) << "ERROR: could not open rados pool " << pool << dendl;
  }
  return r;
}

int RGWReplicaLogger::update_bound(const string& oid, const string& pool,
                                   const string& daemon_id,
                                   const string& marker, const utime_t& time,
                                   const list<RGWReplicaItemMarker> *entries,
                                   bool need_to_exist)
{
  cls_replica_log_progress_marker progress;
  progress.entity_id = daemon_id;
  progress.position_marker = marker;
  progress.position_time = time;
  progress.items = *entries;

  librados::IoCtx ioctx;
  int r = open_ioctx(ioctx, pool);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation opw;
  if (need_to_exist) {
    opw.assert_exists();
  }
  cls_replica_log_update_bound(opw, progress);
  return ioctx.operate(oid, &opw);
}

int RGWReplicaLogger::write_bounds(const string& oid, const string& pool,
                                   RGWReplicaBounds& bounds)
{
  librados::IoCtx ioctx;
  int r = open_ioctx(ioctx, pool);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation opw;
  list<RGWReplicaProgressMarker>::iterator iter = bounds.markers.begin();
  for (; iter != bounds.markers.end(); ++iter) {
    RGWReplicaProgressMarker& progress = *iter;
    cls_replica_log_update_bound(opw, progress);
  }

  r = ioctx.operate(oid, &opw);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWReplicaLogger::delete_bound(const string& oid, const string& pool,
                                   const string& daemon_id, bool purge_all,
                                   bool need_to_exist)
{
  librados::IoCtx ioctx;
  int r = open_ioctx(ioctx, pool);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation opw;
  if (need_to_exist) {
    opw.assert_exists();
  }
  if (purge_all) {
    opw.remove();
  } else {
    cls_replica_log_delete_bound(opw, daemon_id);
  }
  return ioctx.operate(oid, &opw);
}

int RGWReplicaLogger::get_bounds(const string& oid, const string& pool,
                                 RGWReplicaBounds& bounds)
{
  librados::IoCtx ioctx;
  int r = open_ioctx(ioctx, pool);
  if (r < 0) {
    return r;
  }

  return cls_replica_log_get_bounds(ioctx, oid, bounds.marker, bounds.oldest_time, bounds.markers);
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

int RGWReplicaBucketLogger::update_bound(const rgw_bucket& bucket, const string& daemon_id,
                                         const string& marker, const utime_t& time,
                                         const list<RGWReplicaItemMarker> *entries,
                                         bool index_by_instance)
{
  bool need_to_exist = index_by_instance; /* don't need to exist if not indexing by instance */
  int r = RGWReplicaLogger::update_bound(get_key(bucket, index_by_instance), pool,
                                         daemon_id, marker, time, entries, need_to_exist);
  if (r != -ENOENT) {
    return r;
  }
  /*
   * can only get here if need_to_exist == true,
   * entry is not found, let's convert old entry if exists
   */
  RGWReplicaBounds bounds;
  r = convert_old_bounds(bucket, bounds);
  if (r < 0 && r != -ENOENT) {
    return r;
  }

  return RGWReplicaLogger::update_bound(get_key(bucket, index_by_instance), pool,
                                        daemon_id, marker, time, entries, false);
}

int RGWReplicaBucketLogger::delete_bound(const rgw_bucket& bucket, const string& daemon_id, bool index_by_instance, bool purge_all)
{
  bool need_to_exist = index_by_instance; /* don't need to exist if not indexing by instance */
  int r = RGWReplicaLogger::delete_bound(get_key(bucket, index_by_instance), pool, daemon_id, purge_all, need_to_exist);
  if (r != -ENOENT) {
    return r;
  }
  /*
   * can only get here if need_to_exist == true,
   * entry is not found, let's convert old entry if exists
   */
  RGWReplicaBounds bounds;
  r = convert_old_bounds(bucket, bounds);
  if (r < 0 && r != -ENOENT) {
    return r;
  }
  return RGWReplicaLogger::delete_bound(get_key(bucket, index_by_instance), pool, daemon_id, purge_all, false);
}

int RGWReplicaBucketLogger::get_bounds(const rgw_bucket& bucket, RGWReplicaBounds& bounds, bool index_by_instance) {
  int r = RGWReplicaLogger::get_bounds(get_key(bucket, index_by_instance), pool, bounds);
  if (r != -ENOENT || !index_by_instance) {
    return r;
  }

  r = convert_old_bounds(bucket, bounds);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWReplicaBucketLogger::convert_old_bounds(const rgw_bucket& bucket, RGWReplicaBounds& bounds) {
  string old_key = get_key(bucket, false);
  string new_key = get_key(bucket, true);

  /* couldn't find when indexed by instance, retry with old key by bucket name only */
  int r = RGWReplicaLogger::get_bounds(old_key, pool, bounds);
  if (r < 0) {
    return r;
  }
  /* convert to new keys */
  r = RGWReplicaLogger::write_bounds(new_key, pool, bounds);
  if (r < 0) {
    return r;
  }

  string daemon_id;
  r = RGWReplicaLogger::delete_bound(old_key, pool, daemon_id, true, false); /* purge all */
  if (r < 0) {
    return r;
  }
  return 0;
}
