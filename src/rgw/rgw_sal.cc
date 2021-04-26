// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include <errno.h>
#include <stdlib.h>
#include <system_error>
#include <unistd.h>
#include <sstream>

#include "common/errno.h"

#include "rgw_sal.h"
#include "rgw_lc.h"
#include "rgw_bucket.h"
#include "rgw_multi.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::sal {

int RGWRadosUser::list_buckets(const string& marker, const string& end_marker,
			       uint64_t max, bool need_stats, RGWBucketList &buckets)
{
  RGWUserBuckets ulist;
  bool is_truncated = false;
  int ret;
  buckets.clear();

  ret = store->ctl()->user->list_buckets(info.user_id, marker, end_marker, max,
					 need_stats, &ulist, &is_truncated);
  if (ret < 0)
    return ret;

  buckets.set_truncated(is_truncated);
  for (const auto& ent : ulist.get_buckets()) {
    RGWRadosBucket *rb = new RGWRadosBucket(this->store, *this, ent.second);
    buckets.add(rb);
  }

  return 0;
}

RGWBucketList::~RGWBucketList()
{
  for (auto itr = buckets.begin(); itr != buckets.end(); itr++) {
    delete itr->second;
  }
  buckets.clear();
}

RGWBucket* RGWRadosUser::add_bucket(rgw_bucket& bucket,
				       ceph::real_time creation_time)
{
  return NULL;
}

int RGWRadosUser::get_by_id(rgw_user id, optional_yield y)

{
    return store->ctl()->user->get_info_by_uid(id, &info, y);
}

RGWObject *RGWRadosBucket::create_object(const rgw_obj_key &key)
{
  if (!object) {
    object = new RGWRadosObject(store, key);
  }

  return object;
}

int RGWRadosBucket::remove_bucket(bool delete_children, optional_yield y)
{
  int ret;
  map<RGWObjCategory, RGWStorageStats> stats;
  std::vector<rgw_bucket_dir_entry> objs;
  map<string, bool> common_prefixes;
  string bucket_ver, master_ver;

  ret = get_bucket_info(y);
  if (ret < 0)
    return ret;

  ret = get_bucket_stats(info, RGW_NO_SHARD, &bucket_ver, &master_ver, stats);
  if (ret < 0)
    return ret;

  RGWRados::Bucket target(store->getRados(), info);
  RGWRados::Bucket::List list_op(&target);
  int max = 1000;

  list_op.params.list_versions = true;
  list_op.params.allow_unordered = true;

  bool is_truncated = false;
  do {
    objs.clear();

    ret = list_op.list_objects(max, &objs, &common_prefixes, &is_truncated, null_yield);
    if (ret < 0)
      return ret;

    if (!objs.empty() && !delete_children) {
      lderr(store->ctx()) << "ERROR: could not remove non-empty bucket " << ent.bucket.name << dendl;
      return -ENOTEMPTY;
    }

    for (const auto& obj : objs) {
      rgw_obj_key key(obj.key);
      /* xxx dang */
      ret = rgw_remove_object(store, info, ent.bucket, key);
      if (ret < 0 && ret != -ENOENT) {
        return ret;
      }
    }
  } while(is_truncated);

  string prefix, delimiter;

  ret = abort_bucket_multiparts(store, store->ctx(), info, prefix, delimiter);
  if (ret < 0) {
    return ret;
  }

  // remove lifecycle config, if any (XXX note could be made generic)
  (void) store->getRados()->get_lc()->remove_bucket_config(
          get_info(), get_attrs());

  ret = store->ctl()->bucket->sync_user_stats(info.owner, info);
  if ( ret < 0) {
     ldout(store->ctx(), 1) << "WARNING: failed sync user stats before bucket delete. ret=" <<  ret << dendl;
  }

  RGWObjVersionTracker objv_tracker;

  // if we deleted children above we will force delete, as any that
  // remain is detrius from a prior bug
  ret = store->getRados()->delete_bucket(info, objv_tracker, null_yield, !delete_children);
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: could not remove bucket " <<
      ent.bucket.name << dendl;
    return ret;
  }

  ret = store->ctl()->bucket->unlink_bucket(info.owner, ent.bucket, null_yield, false);
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: unable to remove user bucket information" << dendl;
  }

  return ret;
}

int RGWRadosBucket::get_bucket_info(optional_yield y)
{
  return store->getRados()->get_bucket_info(store->svc(), ent.bucket.tenant, ent.bucket.name, info,
					    NULL, y, &attrs);
}

int RGWRadosBucket::get_bucket_stats(RGWBucketInfo& bucket_info, int shard_id,
				     std::string *bucket_ver, std::string *master_ver,
				     std::map<RGWObjCategory, RGWStorageStats>& stats,
				     std::string *max_marker, bool *syncstopped)
{
  return store->getRados()->get_bucket_stats(bucket_info, shard_id, bucket_ver, master_ver, stats, max_marker, syncstopped);
}

int RGWRadosBucket::read_bucket_stats(optional_yield y)
{
      return store->ctl()->bucket->read_bucket_stats(ent.bucket, &ent, y);
}

int RGWRadosBucket::sync_user_stats()
{
      return store->ctl()->bucket->sync_user_stats(user.info.user_id, info);
}

int RGWRadosBucket::update_container_stats(void)
{
  int ret;
  map<std::string, RGWBucketEnt> m;

  m[ent.bucket.name] = ent;
  ret = store->getRados()->update_containers_stats(m);
  if (!ret)
    return -EEXIST;
  if (ret < 0)
    return ret;

  map<string, RGWBucketEnt>::iterator iter = m.find(ent.bucket.name);
  if (iter == m.end())
    return -EINVAL;

  ent.count = iter->second.count;
  ent.size = iter->second.size;
  ent.size_rounded = iter->second.size_rounded;
  ent.placement_rule = std::move(iter->second.placement_rule);

  return 0;
}

int RGWRadosBucket::check_bucket_shards(void)
{
      return store->getRados()->check_bucket_shards(info, ent.bucket, get_count());
}

int RGWRadosBucket::link(RGWUser* new_user, optional_yield y)
{
  RGWBucketEntryPoint ep;
  ep.bucket = ent.bucket;
  ep.owner = new_user->get_user();
  ep.creation_time = get_creation_time();
  ep.linked = true;
  map<string, bufferlist> ep_attrs;
  rgw_ep_info ep_data{ep, ep_attrs};

  return store->ctl()->bucket->link_bucket(new_user->get_user(), info.bucket,
					   ceph::real_time(), y, true, &ep_data);
}

int RGWRadosBucket::unlink(RGWUser* new_user, optional_yield y)
{
  return -1;
}

int RGWRadosBucket::chown(RGWUser* new_user, RGWUser* old_user, optional_yield y)
{
  string obj_marker;

  return store->ctl()->bucket->chown(store, info, new_user->get_user(),
			   old_user->get_display_name(), obj_marker, y);
}

bool RGWRadosBucket::is_owner(RGWUser* user)
{
  get_bucket_info(null_yield);

  return (info.owner.compare(user->get_user()) == 0);
}

int RGWRadosBucket::set_acl(RGWAccessControlPolicy &acl, optional_yield y)
{
  bufferlist aclbl;

  acls = acl;
  acl.encode(aclbl);

  return store->ctl()->bucket->set_acl(acl.get_owner(), ent.bucket, info, aclbl, null_yield);
}

RGWUser *RGWRadosStore::get_user(const rgw_user &u)
{
  return new RGWRadosUser(this, u);
}

//RGWBucket *RGWRadosStore::create_bucket(RGWUser &u, const rgw_bucket &b)
//{
  //if (!bucket) {
    //bucket = new RGWRadosBucket(this, u, b);
  //}
//
  //return bucket;
//}
//
void RGWRadosStore::finalize(void) {
  if (rados)
    rados->finalize();
}

int RGWRadosStore::get_bucket(RGWUser& u, const rgw_bucket& b, RGWBucket** bucket)
{
  int ret;
  RGWBucket* bp;

  *bucket = nullptr;

  bp = new RGWRadosBucket(this, u, b);
  if (!bp) {
    return -ENOMEM;
  }
  ret = bp->get_bucket_info(null_yield);
  if (ret < 0) {
    delete bp;
    return ret;
  }

  *bucket = bp;
  return 0;
}

} // namespace rgw::sal

rgw::sal::RGWRadosStore *RGWStoreManager::init_storage_provider(CephContext *cct, bool use_gc_thread, bool use_lc_thread, bool quota_threads, bool run_sync_thread, bool run_reshard_thread, bool use_cache, bool use_gc)
{
  RGWRados *rados = new RGWRados;
  rgw::sal::RGWRadosStore *store = new rgw::sal::RGWRadosStore();

  store->setRados(rados);
  rados->set_store(store);

  if ((*rados).set_use_cache(use_cache)
              .set_use_gc(use_gc)
              .set_run_gc_thread(use_gc_thread)
              .set_run_lc_thread(use_lc_thread)
              .set_run_quota_threads(quota_threads)
              .set_run_sync_thread(run_sync_thread)
              .set_run_reshard_thread(run_reshard_thread)
              .initialize(cct) < 0) {
    delete store;
    return NULL;
  }

  return store;
}

rgw::sal::RGWRadosStore *RGWStoreManager::init_raw_storage_provider(CephContext *cct)
{
  RGWRados *rados = new RGWRados;
  rgw::sal::RGWRadosStore *store = new rgw::sal::RGWRadosStore();

  store->setRados(rados);
  rados->set_store(store);

  rados->set_context(cct);

  int ret = rados->init_svc(true);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed to init services (ret=" << cpp_strerror(-ret) << ")" << dendl;
    delete store;
    return nullptr;
  }

  if (rados->init_rados() < 0) {
    delete store;
    return nullptr;
  }

  return store;
}

void RGWStoreManager::close_storage(rgw::sal::RGWRadosStore *store)
{
  if (!store)
    return;

  store->finalize();

  delete store;
}
